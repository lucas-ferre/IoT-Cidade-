import streamlit as st
import socket
import os
import re
import json
import time
import struct
import uuid
import datetime
import threading
import pandas as pd
import requests
import asks
import trio
import plotly.express as px
import redis
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from google.protobuf.message import DecodeError

# Importa as classes do Protobuf geradas no momento do build
import messages_pb2 # pyright: ignore[reportMissingImports]

# ====================================================================
# CONFIGURAÇÕES DE REDE E TRANSPORTE TCP
# ====================================================================

GATEWAY_HOST = os.getenv("GATEWAY_HOST", "gateway")
GATEWAY_PORT = int(os.getenv("GATEWAY_PORT", "5001"))

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


def get_secret(name: str, default: str = "") -> str:
    """Lê <NAME>_FILE (Docker secret) e cai para a env var <NAME>.

    Permite injetar a senha do dashboard via Docker secret em vez de texto puro.
    """
    file_path = os.getenv(name + "_FILE")
    if file_path:
        try:
            with open(file_path, "r", encoding="utf-8") as fh:
                return fh.read().strip()
        except OSError:
            pass
    return os.getenv(name, default)


DASHBOARD_PASSWORD = get_secret("DASHBOARD_PASSWORD", "")

# TTL do cache de status do gateway (segundos)
# Evita probe TCP bloqueante a cada rerender do Streamlit.
_GW_STATUS_TTL = 10.0
_TCP_CONNECT_TIMEOUT = 2.0
_TCP_IO_TIMEOUT = 5.0
_MAX_TCP_FRAME_BYTES = 1024 * 1024
_REQUEST_WORKERS = 4
_OS_ERROR_CODE_RE = re.compile(r"\[(?:Errno|WinError)\s*-?\d+\]\s*|\bErrno\s*-?\d+\b:?\s*", re.IGNORECASE)
_GATEWAY_UNAVAILABLE_CODES = {
    "GATEWAY_DNS_ERROR",
    "CONNECT_TIMEOUT",
    "CONNECTION_REFUSED",
    "CONNECTION_CLOSED",
    "IO_TIMEOUT",
    "SOCKET_ERROR",
}


def _clean_os_error_text(exc: BaseException) -> str:
    message = ""
    if isinstance(exc, OSError):
        message = getattr(exc, "strerror", "") or ""
        if not message and len(exc.args) > 1 and isinstance(exc.args[1], str):
            message = exc.args[1]

    if not message:
        message = str(exc)

    message = _OS_ERROR_CODE_RE.sub("", message).strip()
    return message or exc.__class__.__name__


def _format_transport_error(
    result: "TcpRequestResult",
    action: str,
    *,
    requires_gateway_db: bool = False,
) -> str:
    detail = result.error_message or "Falha de transporte sem detalhe adicional."

    if result.error_code in _GATEWAY_UNAVAILABLE_CODES:
        db_note = ""
        if requires_gateway_db:
            db_note = (
                " As consultas dependem do banco de dados servido pelo Gateway, "
                "então nenhum dado histórico pode ser recuperado enquanto ele estiver offline."
            )

        return (
            f"Gateway indisponível na rede. Não foi possível {action}."
            f"{db_note} Verifique se `{GATEWAY_HOST}:{GATEWAY_PORT}` está online e tente novamente. "
            f"Detalhe técnico: {detail}"
        )

    if result.error_code:
        return f"Não foi possível {action}. {result.error_code}: {detail}"

    return f"Não foi possível {action}. {detail}"

@dataclass
class TcpRequestResult:
    response: messages_pb2.ClientResponse | None = None
    error_code: str = ""
    error_message: str = ""

    @property
    def transport_ok(self) -> bool:
        return self.error_code == ""


class GatewayConnectionClosed(RuntimeError):
    pass


class GatewayTcpClient:
    """Cliente TCP persistente para reduzir handshakes e classificar falhas de rede."""

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self._sock: socket.socket | None = None
        self._lock = threading.Lock()

    @property
    def is_busy(self) -> bool:
        return self._lock.locked()

    def close(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    def _connect_locked(self) -> None:
        if self._sock is not None:
            return

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(_TCP_CONNECT_TIMEOUT)
            sock.connect((self.host, self.port))
            sock.settimeout(_TCP_IO_TIMEOUT)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self._sock = sock
        except (socket.timeout, ConnectionRefusedError, OSError):
            sock.close()
            raise

    def ensure_connected(self) -> TcpRequestResult:
        acquired = self._lock.acquire(blocking=False)
        if not acquired:
            return TcpRequestResult(
                error_code="BUSY",
                error_message="Canal TCP ocupado por outra operação em andamento.",
            )

        try:
            try:
                self._connect_locked()
                return TcpRequestResult()
            except socket.timeout:
                self.close()
                return TcpRequestResult(
                    error_code="CONNECT_TIMEOUT",
                    error_message=f"Tempo limite ao abrir conexão TCP com {self.host}:{self.port}.",
                )
            except socket.gaierror as exc:
                self.close()
                return TcpRequestResult(
                    error_code="GATEWAY_DNS_ERROR",
                    error_message=(
                        f"Não foi possível resolver o host '{self.host}'. "
                        f"Detalhe: {_clean_os_error_text(exc)}."
                    ),
                )
            except ConnectionRefusedError:
                self.close()
                return TcpRequestResult(
                    error_code="CONNECTION_REFUSED",
                    error_message=f"Conexão TCP recusada em {self.host}:{self.port}.",
                )
            except OSError as exc:
                self.close()
                return TcpRequestResult(
                    error_code="SOCKET_ERROR",
                    error_message=(
                        f"Falha de socket ao conectar a {self.host}:{self.port}. "
                        f"Detalhe: {_clean_os_error_text(exc)}."
                    ),
                )
        finally:
            self._lock.release()

    def _recv_exact_locked(self, size: int) -> bytes:
        assert self._sock is not None, "Socket não inicializado em _recv_exact_locked"
        data = bytearray()
        while len(data) < size:
            chunk = self._sock.recv(size - len(data))
            if not chunk:
                raise GatewayConnectionClosed("Gateway encerrou a conexão TCP.")
            data.extend(chunk)
        return bytes(data)

    def request(self, request: messages_pb2.ClientRequest) -> TcpRequestResult:
        msg_data = request.SerializeToString()
        frame = struct.pack(">I", len(msg_data)) + msg_data

        with self._lock:
            for attempt in range(2):
                try:
                    self._connect_locked()
                    assert self._sock is not None, "Socket não inicializado após _connect_locked"
                    self._sock.sendall(frame)

                    header = self._recv_exact_locked(4)
                    resp_len = struct.unpack(">I", header)[0]
                    if resp_len <= 0 or resp_len > _MAX_TCP_FRAME_BYTES:
                        self.close()
                        return TcpRequestResult(
                            error_code="INVALID_FRAME_SIZE",
                            error_message=f"Gateway retornou frame inválido: {resp_len} bytes.",
                        )

                    data = self._recv_exact_locked(resp_len)
                    response = messages_pb2.ClientResponse()
                    response.ParseFromString(data)
                    return TcpRequestResult(response=response)

                except (GatewayConnectionClosed, ConnectionResetError, ConnectionAbortedError, BrokenPipeError) as exc:
                    self.close()
                    if attempt == 0:
                        continue
                    return TcpRequestResult(
                        error_code="CONNECTION_CLOSED",
                        error_message=(
                            "Conexão TCP encerrada durante a requisição. "
                            f"Detalhe: {_clean_os_error_text(exc)}."
                        ),
                    )
                except socket.timeout:
                    self.close()
                    return TcpRequestResult(
                        error_code="IO_TIMEOUT",
                        error_message="Gateway não respondeu dentro da janela de timeout do socket.",
                    )
                except socket.gaierror as exc:
                    self.close()
                    return TcpRequestResult(
                        error_code="GATEWAY_DNS_ERROR",
                        error_message=(
                            f"Não foi possível resolver o host '{self.host}'. "
                            f"Detalhe: {_clean_os_error_text(exc)}."
                        ),
                    )
                except ConnectionRefusedError:
                    self.close()
                    return TcpRequestResult(
                        error_code="CONNECTION_REFUSED",
                        error_message=f"Conexão TCP recusada em {self.host}:{self.port}.",
                    )
                except struct.error as exc:
                    self.close()
                    return TcpRequestResult(
                        error_code="FRAME_HEADER_ERROR",
                        error_message=f"Cabeçalho de frame TCP corrompido: {exc}",
                    )
                except DecodeError as exc:
                    self.close()
                    return TcpRequestResult(
                        error_code="PROTOBUF_DECODE_ERROR",
                        error_message=f"Resposta Protobuf inválida do Gateway: {exc}",
                    )
                except OSError as exc:
                    self.close()
                    return TcpRequestResult(
                        error_code="SOCKET_ERROR",
                        error_message=f"Falha de socket no fluxo TCP: {_clean_os_error_text(exc)}.",
                    )

        return TcpRequestResult(
            error_code="REQUEST_FAILED",
            error_message="Falha não classificada no fluxo TCP.",
        )


def get_gateway_client() -> GatewayTcpClient:
    if "gateway_tcp_client" not in st.session_state:
        st.session_state.gateway_tcp_client = GatewayTcpClient(GATEWAY_HOST, GATEWAY_PORT)
    return st.session_state.gateway_tcp_client


def get_request_executor() -> ThreadPoolExecutor:
    if "tcp_request_executor" not in st.session_state:
        st.session_state.tcp_request_executor = ThreadPoolExecutor(max_workers=_REQUEST_WORKERS)
    return st.session_state.tcp_request_executor


def submit_tcp_request(task_key: str, request: messages_pb2.ClientRequest, context: dict) -> None:
    client = get_gateway_client()
    executor = get_request_executor()
    st.session_state[task_key] = {
        "future": executor.submit(client.request, request),
        "context": context,
        "started_at": time.time(),
    }


def is_tcp_task_pending(task_key: str) -> bool:
    task = st.session_state.get(task_key)
    return bool(task and not task["future"].done())


def remember_gateway_transport_state(result: TcpRequestResult) -> None:
    if result.response is not None:
        st.session_state.gw_status = True
        st.session_state.gw_last_check = time.time()
    elif result.error_code in _GATEWAY_UNAVAILABLE_CODES:
        st.session_state.gw_status = False
        st.session_state.gw_last_check = time.time()


def consume_tcp_task(task_key: str, label: str) -> tuple[TcpRequestResult, dict] | None:
    task = st.session_state.get(task_key)
    if not task:
        return None

    future = task["future"]
    if not future.done():
        elapsed = time.time() - task["started_at"]
        st.info(f"{label} em andamento há {elapsed:.1f}s. A interface segue disponível.")
        if st.button(f"Verificar {label}", key=f"{task_key}_poll"):
            st.rerun()
        return None

    result = future.result()
    remember_gateway_transport_state(result)
    context = task["context"]
    del st.session_state[task_key]
    return result, context


def send_tcp_request_result(request: messages_pb2.ClientRequest) -> TcpRequestResult:
    return get_gateway_client().request(request)


def send_tcp_request(request: messages_pb2.ClientRequest) -> messages_pb2.ClientResponse | None:
    """Compatibilidade para chamadas síncronas existentes, agora com erro classificado."""
    result = send_tcp_request_result(request)
    if result.response is not None:
        return result.response

    if result.error_message:
        st.error(_format_transport_error(result, "enviar requisição ao Gateway"))
    return None


def _device_to_dict(d) -> dict:
    """ Converte DeviceInfo protobuf para dict Python simples.
    Armazenar dicts em session_state evita mutação de objetos protobuf e elimina
    dependência de ciclo de vida do GC sobre mensagens filho de `resp`.
    """
    return {
        "device_id":          d.device_id,
        "type":               int(d.type),
        "status":             int(d.status),
        "ip_address":         d.ip_address,
        "control_port":       int(d.control_port),
        "aggregator_id":      d.aggregator_id,
        "is_controllable":    bool(d.is_controllable),
        "last_seen_timestamp": int(d.last_seen_timestamp),
        "coord_x":            int(d.coord_x),
        "coord_y":            int(d.coord_y),
    }

def check_gateway_status() -> bool:
    """Atesta a vitalidade do Gateway reaproveitando o canal TCP persistente."""
    client = get_gateway_client()
    if client.is_busy:
        return st.session_state.get("gw_status", False)

    result = client.ensure_connected()
    return result.transport_ok

def infer_sector_from_device_id(device_id: str) -> str:
    """Realiza análise em substring para mapear IDs heterogêneos aos setores físicos."""
    sector_map = {
        "pici": "Pici",
        "benfica": "Benfica",
        "porangabussu": "Porangabussu",
        "labomar": "Labomar",
    }
    
    # Busca relaxada para não quebrar com nomes como "CameraPici01"
    for slug, label in sector_map.items():
        if slug in device_id.lower():
            return label
    return "N/A"

def get_metric_reference_range(metric_key: str) -> dict:
    """Retorna dicionário com topologia de faixas críticas para a UI."""
    ranges = {
        "temperature": {
            "safe": (18, 26), "warning": (15, 32), "critical": (-float('inf'), 15),
            "description": "Faixa recomendada: 18-26°C"
        },
        "humidity": {
            "safe": (40, 60), "warning": (30, 70),
            "description": "Faixa recomendada: 40-60%"
        },
        "co2": {
            "safe": (0, 800), "warning": (800, 1200), "critical": (1200, float('inf')),
            "description": "Nível seguro: < 800 ppm"
        },
        "pm25": {
            "safe": (0, 12), "warning": (12, 35), "critical": (35, float('inf')),
            "description": "Limite seguro: ≤ 12 µg/m³"
        },
        "pm10": {
            "safe": (0, 54), "warning": (54, 154), "critical": (154, float('inf')),
            "description": "Limite seguro: ≤ 54 µg/m³"
        },
        "luminosity": {
            "safe": (30, 80), "warning": (20, 100),
            "description": "Nível recomendado: 30-80%"
        },
        "power_consumption": {
            "safe": (0, 100), "warning": (100, 200), "critical": (200, float('inf')),
            "description": "Consumo normal: 0-100W"
        },
        "queue_length": {
            "safe": (0, 20), "warning": (20, 35), "critical": (35, float('inf')),
            "description": "Fila tolerável: até 35 veículos"
        }
    }
    return ranges.get(metric_key, {
        "description": f"Métrica contínua: {metric_key}"
    })

def aqi_category(v: float) -> str:
    """Traduz valor numérico do AQI para categoria de impacto ambiental."""
    if v <= 50:   return "🟢 Bom"
    if v <= 100:  return "🟡 Moderado"
    if v <= 150:  return "🟠 Insalubre (sensíveis)"
    if v <= 200:  return "🔴 Insalubre"
    if v <= 300:  return "🟣 Muito Insalubre"
    return "⚫ Perigoso"

# ====================================================================
# MAPAS DE CONFIGURAÇÃO DE DOMÍNIO
# ====================================================================

TYPE_MAP = {
    messages_pb2.DEVICE_TYPE_TRAFFIC_LIGHT: "🚦 Semáforo",
    messages_pb2.DEVICE_TYPE_LAMP_POST: "💡 Poste Inteligente",
    messages_pb2.DEVICE_TYPE_WEATHER_STATION: "🌦️ Estação Met.",
    messages_pb2.DEVICE_TYPE_CAMERA: "📹 Câmera de Tráfego",
    messages_pb2.DEVICE_TYPE_AIR_QUALITY: "💨 Qualidade do Ar",
    messages_pb2.DEVICE_TYPE_FLOOD: "🌊 Sensor de Enchente",
    messages_pb2.DEVICE_TYPE_NOISE: "🔊 Sensor de Ruído",
}

STATUS_MAP = {
    messages_pb2.STATUS_ON: "🟢 ONLINE",
    messages_pb2.STATUS_OFF: "⚪ OFFLINE",
    messages_pb2.STATUS_ERROR: "🔴 FALHA"
}

METRIC_ICONS = {
    "temperature": "🌡️", "humidity": "💧", "co2": "🌿",
    "pm25": "🌫️", "pm10": "💨", "aqi": "🏭",
    "luminosity": "💡", "power_consumption": "⚡", "state": "🚦",
    "vehicles_count": "🚗", "infractions": "📸", "queue_length": "🚥",
    "water_level": "🌊", "flow_rate": "🚰", "noise_db": "🔊", "peak_db": "📢",
}

METRIC_UNITS = {
    "temperature": "°C", "humidity": "%", "co2": "ppm",
    "pm25": "µg/m³", "pm10": "µg/m³", "aqi": "",
    "luminosity": "%", "power_consumption": "W", "state": "",
    "vehicles_count": "veh/min", "infractions": "count", "queue_length": "vehicles",
    "water_level": "cm", "flow_rate": "L/s", "noise_db": "dB", "peak_db": "dB",
}

DEVICE_METRICS_MAP = {
    messages_pb2.DEVICE_TYPE_WEATHER_STATION: ["temperature", "humidity", "co2", "pm25", "pm10", "aqi"],
    messages_pb2.DEVICE_TYPE_AIR_QUALITY: ["temperature", "humidity", "co2", "pm25", "pm10", "aqi"],
    messages_pb2.DEVICE_TYPE_LAMP_POST: ["luminosity", "power_consumption"],
    messages_pb2.DEVICE_TYPE_TRAFFIC_LIGHT: ["state", "queue_length"],
    messages_pb2.DEVICE_TYPE_CAMERA: ["vehicles_count", "infractions"],
    messages_pb2.DEVICE_TYPE_FLOOD: ["water_level", "flow_rate"],
    messages_pb2.DEVICE_TYPE_NOISE: ["noise_db", "peak_db"],
}

# Métricas alertáveis/automatizáveis (usadas nas abas de Alertas e Automação).
_AUTOMATION_METRICS = [
    "temperature", "humidity", "co2", "pm25", "pm10", "aqi",
    "luminosity", "power_consumption", "queue_length",
    "vehicles_count", "infractions",
    "water_level", "flow_rate", "noise_db", "peak_db",
]

# ====================================================================
# INICIALIZAÇÃO DA INTERFACE STREAMLIT
# ====================================================================

st.set_page_config(
    page_title="Smart City Analytics Client",
    page_icon="🏙️",
    layout="wide",
    initial_sidebar_state="expanded"
)


def require_login() -> None:
    """Portão de senha simples. Se DASHBOARD_PASSWORD não estiver configurada,
    o acesso permanece aberto (com aviso), preservando compatibilidade."""
    if not DASHBOARD_PASSWORD:
        return  # auth desabilitada — comportamento legado

    if st.session_state.get("auth_ok"):
        return

    st.title("🔒 Acesso Restrito — Smart City")
    st.caption("Informe a senha para acessar o Centro de Controle.")
    with st.form("login_form"):
        entered = st.text_input("Senha", type="password")
        submitted = st.form_submit_button("Entrar", type="primary")
    if submitted:
        if entered == DASHBOARD_PASSWORD:
            st.session_state.auth_ok = True
            st.rerun()
        else:
            st.error("Senha incorreta.")
    st.stop()


require_login()

st.title("🏙️ Centro de Controle Analítico - Smart City")
st.markdown("Monitoramento distribuído, controle operacional e agregação estatística via Sockets TCP/Protobuf.")

# ====================================================================
# GERENCIAMENTO DE ESTADO EM MEMÓRIA
# ====================================================================

if 'device_history' not in st.session_state:
    st.session_state.device_history = []
if 'command_history' not in st.session_state:
    st.session_state.command_history = []
if 'last_cmd_result' not in st.session_state:
    st.session_state.last_cmd_result = None
if 'selected_device_id' not in st.session_state:
    st.session_state.selected_device_id = None
# Cache de status do gateway com TTL
if 'gw_last_check' not in st.session_state:
    st.session_state.gw_last_check = 0.0
if 'gw_status' not in st.session_state:
    st.session_state.gw_status = False
#  Mensagem da última sincronização de topologia (Aba 1)
if 'last_list_msg' not in st.session_state:
    st.session_state.last_list_msg = None
#  Resultado da última query OLAP (Aba 3)
if 'olap_result' not in st.session_state:
    st.session_state.olap_result = None
if 'olap_context' not in st.session_state:
    st.session_state.olap_context = None
if 'olap_error' not in st.session_state:
    st.session_state.olap_error = None
#  Resultado da última inspeção individual (Aba 4)
if 'inspection_result' not in st.session_state:
    st.session_state.inspection_result = None
if 'inspection_context' not in st.session_state:
    st.session_state.inspection_context = None
if 'inspection_error' not in st.session_state:
    st.session_state.inspection_error = None
if "control_task" not in st.session_state:
    st.session_state.control_task = None
if "last_backup_time" not in st.session_state:
    st.session_state.last_backup_time = ""

async def _async_get_backup_status() -> str:
    try:
        resp = await asks.get("http://backup_go:8080/backup/status", timeout=2)
        if resp.status_code == 200:
            return resp.json().get("last_backup_time", "")
    except Exception:
        pass
    return ""

def get_backup_status() -> str:
    return trio.run(_async_get_backup_status)

async def _async_trigger_backup() -> tuple[int, dict, str]:
    try:
        resp = await asks.post("http://backup_go:8080/backup/trigger", timeout=5)
        return resp.status_code, resp.json(), ""
    except Exception as e:
        return 500, {}, str(e)

def trigger_manual_backup():
    status_code, data, err = trio.run(_async_trigger_backup)
    if status_code == 200:
        st.toast("Backup manual concluído com sucesso!", icon="✅")
        st.session_state.last_backup_time = data.get("last_backup_time", "")
    else:
        msg = err if err else f"Erro no backup: {status_code}"
        st.toast(f"Falha ao contatar serviço de backup: {msg}", icon="❌")

# ====================================================================
# SIDEBAR
# ====================================================================

with st.sidebar:
    st.subheader("📊 Status do Sistema")
    col1, col2, col3 = st.columns(3)

    # Probe TCP só executado quando o cache expirar (TTL = 10s).
    if time.time() - st.session_state.gw_last_check > _GW_STATUS_TTL:
        st.session_state.gw_status = check_gateway_status()
        st.session_state.gw_last_check = time.time()

    gateway_status = "🟢 Ativo" if st.session_state.gw_status else "🔴 Inativo"
    col1.metric("Gateway", gateway_status)

    sensor_count = len(st.session_state.device_history) if st.session_state.device_history else "N/A"
    col2.metric("Sensores", sensor_count, help="Atualizar na aba Descoberta")
    col3.metric("Hora UTC", datetime.datetime.now(datetime.timezone.utc).strftime('%H:%M'))
    st.markdown("---")
    
    st.subheader("💾 Backup Analítico (Go)")
    if st.button("Realizar Backup Manual", use_container_width=True):
        trigger_manual_backup()
    
    st.markdown("---")
    
    # Enriquecimento com requests (Open-Meteo)
    st.subheader("🌤️ Clima Local (Fortaleza)")
    try:
        # Fortaleza coords: -3.71722, -38.54306
        weather_resp = requests.get(
            "https://api.open-meteo.com/v1/forecast?latitude=-3.7172&longitude=-38.5431&current=temperature_2m,relative_humidity_2m&timezone=auto",
            timeout=3
        )
        if weather_resp.status_code == 200:
            w_data = weather_resp.json().get("current", {})
            temp = w_data.get("temperature_2m", "--")
            hum = w_data.get("relative_humidity_2m", "--")
            st.metric("Temperatura Externa", f"{temp} °C")
            st.metric("Umidade Relativa", f"{hum} %")
        else:
            st.error("Falha ao consultar API externa.")
    except Exception as e:
        st.error(f"Erro de rede: {e}")

    st.markdown("---")
    st.info("💡 Selecione uma aba para iniciar operações na rede.")

st.divider()

tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9 = st.tabs([
    "📡 Fontes de Dados (Descoberta)",
    "⚙️ Painel de Atuação (Controle)",
    "📊 Consultas Analíticas (OLAP)",
    "🔍 Inspeção Individual (Sensor)",
    "🗺️ Mapa Interativo (Matriz)",
    "🧾 Auditoria de Comandos",
    "🚨 Alertas em Tempo Real",
    "⚡ Automação (Regras)",
    "📈 Observabilidade (Sistema)"
])

# --------------------------------------------------------------------
# ABA 1: Topologia de Descoberta
# --------------------------------------------------------------------
with tab1:
    st.subheader("📡 Nós Operacionais Registrados no Gateway")

    # Consome resultado de sincronização enviada em background.
    # O padrão submit→rerun→consume mantém a UI responsiva durante o I/O TCP.
    completed_list = consume_tcp_task("list_devices_task", "Sincronização de topologia")
    if completed_list:
        result, _ = completed_list
        resp = result.response
        if resp and resp.success:
            # Armazena como dicts Python — elimina mutação de objetos
            # protobuf e dependência do ciclo de vida de `resp` no GC.
            st.session_state.device_history = [_device_to_dict(d) for d in resp.devices]
            if not resp.devices:
                st.session_state.last_list_msg = ("info", "✓ Nenhum dispositivo descoberto pelo Gateway até o momento.")
            else:
                st.session_state.last_list_msg = ("success", f"✓ {len(resp.devices)} nó(s) identificado(s) com sucesso.")
        elif resp:
            st.session_state.last_list_msg = ("warning", f"⚠️ Alerta do Barramento: {resp.message}")
        else:
            st.session_state.last_list_msg = (
                "error",
                f"❌ {_format_transport_error(result, 'sincronizar a topologia')}",
            )
        st.rerun()

    if st.button("Atualizar Topologia de Rede", type="primary", use_container_width=True,
                 disabled=is_tcp_task_pending("list_devices_task")):
        
        # Check automatic backup
        current_backup = get_backup_status()
        if current_backup and current_backup != st.session_state.last_backup_time:
            if st.session_state.last_backup_time != "":
                st.info(f"💾 Um novo backup foi gerado automaticamente às: {current_backup}")
            st.session_state.last_backup_time = current_backup

        req = messages_pb2.ClientRequest()
        req.type = messages_pb2.REQUEST_TYPE_LIST_DEVICES
        submit_tcp_request("list_devices_task", req, {})
        st.session_state.last_list_msg = None
        st.rerun()

    if st.session_state.last_list_msg:
        kind, msg = st.session_state.last_list_msg
        {"success": st.success, "warning": st.warning,
         "error": st.error, "info": st.info}[kind](msg)

    if st.session_state.device_history:
        device_data = []
        for d in st.session_state.device_history:
            #  Acesso via dict — os dados são agora dicts Python simples.
            device_data.append({
                "ID": d["device_id"],
                "Agregador": d.get("aggregator_id", "N/A"),
                "Setor Geográfico": infer_sector_from_device_id(d["device_id"]),
                "Classe do Dispositivo": TYPE_MAP.get(d["type"], "Desconhecido"),
                "Status Atual": STATUS_MAP.get(d["status"], "Desconhecido"),
                "Ponto de Entrada": (
                    f"{d['ip_address']}:{d['control_port']}" if d["is_controllable"]
                    else f"{d['ip_address']} (Somente UDP)"
                ),
                "Permite Controle": "✓ Sim" if d["is_controllable"] else "✗ Não",
                "Último ACK": datetime.datetime.fromtimestamp(d["last_seen_timestamp"]).strftime('%H:%M:%S')
            })

        df = pd.DataFrame(device_data)
        st.dataframe(df, use_container_width=True, hide_index=True)

        st.divider()
        col_graph, col_stats = st.columns([2, 1])

        with col_graph:
            status_counts = defaultdict(int)
            for d in st.session_state.device_history:
                status_label = STATUS_MAP.get(d["status"], "Desconhecido")
                status_counts[status_label] += 1

            if status_counts:
                st.bar_chart(pd.DataFrame([
                    {"Status da Frota": status, "Nós": count}
                    for status, count in status_counts.items()
                ]).set_index("Status da Frota"))

        with col_stats:
            st.metric("Total de Nós Indexados", len(st.session_state.device_history))
            online_count = sum(1 for d in st.session_state.device_history if d["status"] == messages_pb2.STATUS_ON)
            st.metric("Instâncias Operacionais (ONLINE)", online_count)
            controllable_count = sum(1 for d in st.session_state.device_history if d["is_controllable"])
            st.metric("Interfaces de Atuação Disponíveis", controllable_count)

# --------------------------------------------------------------------
# ABA 2: Comandos de Controle (RPC)
# --------------------------------------------------------------------
with tab2:
    st.subheader("⚙️ Console de Atuação Contextual")
    
    if not st.session_state.device_history:
        st.info("Topologia desconhecida. Sincronize a rede na aba 'Fontes de Dados'.")
    else:
        controllable_devices = [d for d in st.session_state.device_history if d["is_controllable"]]
        device_ids = [d["device_id"] for d in controllable_devices]
        
        if not device_ids:
            st.warning("Nenhum dispositivo controlável disponível. Sincronize a topologia primeiro.")
        else:
            # ── Controle em Massa (por Setor / por Tipo) ─────────────────
            with st.expander("🛠️ Controle em Massa (por Setor / por Tipo)", expanded=False):
                st.caption("Aplica um comando a TODOS os dispositivos controláveis que casam com o filtro.")
                mc_scope = st.radio("Escopo", ["Por Setor", "Por Tipo"], horizontal=True, key="mc_scope")

                if mc_scope == "Por Setor":
                    sectors = sorted({infer_sector_from_device_id(d["device_id"]) for d in controllable_devices})
                    mc_value = st.selectbox("Setor alvo", sectors, key="mc_sector")
                    mass_targets = [d for d in controllable_devices
                                    if infer_sector_from_device_id(d["device_id"]) == mc_value]
                else:
                    types_present = sorted({d["type"] for d in controllable_devices})
                    mc_value = st.selectbox("Tipo alvo", types_present,
                                            format_func=lambda t: TYPE_MAP.get(t, f"Tipo {t}"), key="mc_type")
                    mass_targets = [d for d in controllable_devices if d["type"] == mc_value]

                st.write(f"**{len(mass_targets)}** dispositivo(s) atingido(s): "
                         + (", ".join(d["device_id"] for d in mass_targets) if mass_targets else "—"))

                mcc1, mcc2 = st.columns(2)
                with mcc1:
                    mc_alt_status = st.checkbox("Alterar status", key="mc_alt_status")
                    mc_status_label = st.radio(
                        "Novo estado", ["Ligar (ON)", "Desligar (OFF)", "Falha (ERROR)"],
                        disabled=not mc_alt_status, key="mc_status_label")
                with mcc2:
                    mc_alt_freq = st.checkbox("Alterar frequência", key="mc_alt_freq")
                    mc_freq = st.slider("Frequência (s)", 1, 60, 5,
                                        disabled=not mc_alt_freq, key="mc_freq")

                if st.button("Aplicar em Massa", type="primary", key="mc_apply", use_container_width=True):
                    if not mass_targets:
                        st.warning("Nenhum dispositivo no filtro selecionado.")
                    elif not mc_alt_status and not mc_alt_freq:
                        st.warning("Selecione ao menos um parâmetro (status e/ou frequência).")
                    else:
                        client = get_gateway_client()
                        mass_results = []
                        progress = st.progress(0.0)
                        for i, d in enumerate(mass_targets):
                            req = messages_pb2.ClientRequest()
                            req.type = messages_pb2.REQUEST_TYPE_SEND_COMMAND
                            req.target_device_id = d["device_id"]
                            cmd = req.command_payload
                            cmd.command_id = f"MASS-{uuid.uuid4().hex[:6].upper()}"
                            if mc_alt_status:
                                cmd.update_status = True
                                cmd.target_status = (
                                    messages_pb2.STATUS_ON if "Ligar" in mc_status_label
                                    else messages_pb2.STATUS_OFF if "Desligar" in mc_status_label
                                    else messages_pb2.STATUS_ERROR)
                            if mc_alt_freq:
                                cmd.update_frequency = True
                                cmd.new_frequency_secs = int(mc_freq)
                            res = client.request(req)
                            ok = res.response is not None and res.response.success
                            mass_results.append({
                                "Dispositivo": d["device_id"],
                                "Resultado": "✓ OK" if ok else "✗ Falha",
                                "Detalhe": (res.response.message if res.response else res.error_message),
                            })
                            progress.progress((i + 1) / len(mass_targets))
                        ok_n = sum(1 for r in mass_results if r["Resultado"].startswith("✓"))
                        st.success(f"Comando aplicado: {ok_n}/{len(mass_results)} com sucesso.")
                        st.dataframe(pd.DataFrame(mass_results), use_container_width=True, hide_index=True)

            st.divider()

            col_sel, col_det = st.columns([1, 1])
            
            with col_sel:
                if st.session_state.selected_device_id not in device_ids and device_ids:
                    st.session_state.selected_device_id = device_ids[0]
                
                target_id = st.selectbox(
                    "Selecione o Nó Alvo de Atuação", 
                    options=device_ids,
                    key="selected_device_id"
                )
                
                selected_device = next((d for d in controllable_devices if d["device_id"] == target_id), None)
            
            if selected_device:
                with col_det:
                    st.markdown(f"""
                    **Tabela de Assinatura do Nó:**
                    - **Classificação:** `{TYPE_MAP.get(selected_device["type"], "Desconhecido")}`
                    - **Socket Escuta:** `{selected_device["ip_address"]}:{selected_device["control_port"]}`
                    - **Estado Local:** `{STATUS_MAP.get(selected_device["status"], "Desconhecido")}`
                    """)

                st.divider()
                
                if st.session_state.last_cmd_result:
                    c_msg, c_clr = st.columns([9, 1])
                    with c_msg:
                        res_type = st.session_state.last_cmd_result["type"]
                        msg = st.session_state.last_cmd_result["message"]
                        if res_type == "success": st.success(f"**Confirmação Positiva:** {msg}")
                        elif res_type == "error": st.error(f"**Falha de I/O:** {msg}")
                        elif res_type == "warning": st.warning(f"⚠️ {msg}")
                        elif res_type == "info": st.info(msg)
                    with c_clr:
                        if st.button("✕", key="clear_msg"):
                            st.session_state.last_cmd_result = None
                
                st.write(f"### Parâmetros de Intervenção: `{target_id}`")
                
                c1, c2 = st.columns(2)
                
                with c1:
                    alterar_status = st.checkbox("Engatilhar Mutação de Estado", value=False)
                    
                    if selected_device["type"] == messages_pb2.DEVICE_TYPE_TRAFFIC_LIGHT:
                        status_options = ["Ligar (VERDE)", "Desligar (OFF)", "Emergência (PISCANTE)"]
                    elif selected_device["type"] == messages_pb2.DEVICE_TYPE_LAMP_POST:
                        status_options = ["Acender Relé (ON)", "Cortar Relé (OFF)", "Modo Manutenção (ERR)"]
                    elif selected_device["type"] == messages_pb2.DEVICE_TYPE_CAMERA:
                        status_options = ["Gravar Stream (ON)", "Pausar Stream (OFF)", "Diagnóstico Binário (ERR)"]
                    else:
                        status_options = ["Ativar", "Desativar", "Provocar Falha"]
                        
                    novo_status_label = st.radio("Seletor de Estado Desejado", status_options, disabled=not alterar_status)
                
                with c2:
                    alterar_freq = st.checkbox("Substituir Relógio de Telemetria", value=False)
                    nova_freq = st.slider("Duty Cycle (Segundos/Datagrama)", 1, 60, 5, 
                                          help="Altera a agressividade com que o nó dispara pacotes UDP.", 
                                          disabled=not alterar_freq)

                    st.markdown("---")

                completed_command = consume_tcp_task("command_task", "Comando TCP")
                if completed_command:
                    result, context = completed_command
                    resp = result.response
                    ts = datetime.datetime.now().strftime('%H:%M:%S')

                    if resp:
                        st.session_state.command_history.append({
                            "timestamp": ts,
                            "device": context["target_id"],
                            "command_id": context["command_id"],
                            "status": "✓ Aprovado" if resp.success else "✗ Recusado",
                            "message": resp.message,
                        })

                        if resp.success:

                            for device in st.session_state.device_history:
                                if device["device_id"] == context["target_id"]:
                                    if context["update_status"]:
                                        device["status"] = context["target_status"]
                                    device["last_seen_timestamp"] = int(time.time())
                                    break

                            st.session_state.last_cmd_result = {"type": "success", "message": resp.message}
                        else:
                            st.session_state.last_cmd_result = {"type": "error", "message": resp.message}
                    else:
                        st.session_state.command_history.append({
                            "timestamp": ts,
                            "device": context["target_id"],
                            "command_id": context["command_id"],
                            "status": f"⚠️ {result.error_code}",
                            "message": result.error_message,
                        })
                        st.session_state.last_cmd_result = {
                            "type": "error",
                            "message": _format_transport_error(result, "transmitir o comando TCP"),
                        }

                    st.rerun()

                command_pending = is_tcp_task_pending("command_task")
                if st.button(
                    "Transmitir Payload de Controle (TCP)",
                    type="primary",
                    use_container_width=True,
                    disabled=command_pending,
                ):
                    if not alterar_status and not alterar_freq:
                        st.session_state.last_cmd_result = {"type": "warning", "message": "Nenhum parâmetro selecionado para sobreposição."}
                        st.rerun()
                    else:
                        req = messages_pb2.ClientRequest()
                        req.type = messages_pb2.REQUEST_TYPE_SEND_COMMAND
                        req.target_device_id = target_id
                        
                        cmd = req.command_payload
                        cmd.command_id = f"CMD-{uuid.uuid4().hex[:6].upper()}"
                        
                        if alterar_status:
                            cmd.update_status = True
                            if any(x in novo_status_label for x in ["Ligar", "Acender", "Gravar", "Ativar"]):
                                cmd.target_status = messages_pb2.STATUS_ON
                            elif any(x in novo_status_label for x in ["Desligar", "Cortar", "Pausar", "Desativar"]):
                                cmd.target_status = messages_pb2.STATUS_OFF
                            else:
                                cmd.target_status = messages_pb2.STATUS_ERROR
                                
                        if alterar_freq:
                            cmd.update_frequency = True
                            cmd.new_frequency_secs = int(nova_freq)

                        submit_tcp_request(
                            "command_task",
                            req,
                            {
                                "target_id": target_id,
                                "command_id": cmd.command_id,
                                "update_status": bool(cmd.update_status),
                                "target_status": cmd.target_status,
                            },
                        )
                        st.session_state.last_cmd_result = {
                            "type": "info",
                            "message": "Comando enviado em segundo plano; aguardando ACK do Gateway.",
                        }

                        st.rerun()
                
                st.divider()
                st.subheader("📜 Auditoria de Atuação Recente")
                
                if st.session_state.command_history:
                    history_df = pd.DataFrame(st.session_state.command_history[-10:])
                    history_df = history_df[['timestamp', 'device', 'command_id', 'status', 'message']]
                    history_df.columns = ['Ocorrência', 'Endereço Lógico', 'Hash do Comando', 'Status Execução', 'Retorno I/O']
                    st.dataframe(history_df.iloc[::-1], use_container_width=True, hide_index=True)
                else:
                    st.info("📋 Tabela de auditoria vazia. Os comandos executados nesta sessão aparecerão aqui.")

# --------------------------------------------------------------------
# ABA 3: Análise Multidimensional (OLAP)
# --------------------------------------------------------------------
with tab3:
    st.subheader("📊 Agregação Analítica Servidor-Lado (SQLite WAL)")

    c_op, c_met, c_time = st.columns(3)

    with c_op:
        operacao = st.selectbox("Função de Avaliação Numérica", [
            ("📈 Média Aritmética Amostral", messages_pb2.OP_AVERAGE),
            ("📊 Desvio Padrão Populacional", messages_pb2.OP_STD_DEV),
            ("🔀 Cálculo de Maior Variação Geográfica", messages_pb2.OP_MAX_VARIATION),
        ], format_func=lambda x: x[0])

    with c_met:
        metrica_alvo = st.selectbox("Vetor de Telemetria", [
            ("🌡️ Temperatura (°C)",              "temperature"),
            ("💧 Umidade Relativa (%)",            "humidity"),
            ("🌿 CO₂ (ppm)",                      "co2"),
            ("🌫️ PM2.5 — Partículas Finas",      "pm25"),
            ("💨 PM10 — Partículas Grossas",     "pm10"),
            ("🏭 AQI — Índice Base EPA",         "aqi"),
            ("💡 Luxmetria Resultante (%)",      "luminosity"),
            ("⚡ Drenagem Energética (W)",       "power_consumption"),
            ("🚗 Fluxo Veicular Direto",         "vehicles_count"),
            ("📸 Taxa de Infrações Corrente",    "infractions"),
            ("🚥 Fila Semafórica (veículos)",    "queue_length"),
            ("🌊 Nível d'Água (cm)",             "water_level"),
            ("🚰 Vazão (L/s)",                   "flow_rate"),
            ("🔊 Ruído (dB)",                    "noise_db"),
        ], format_func=lambda x: x[0])

    with c_time:
        janela_horas = st.slider("Fatia Temporal Histórica (Horas passadas)", min_value=1, max_value=24, value=1)

    completed_olap = consume_tcp_task("olap_task", "Query OLAP")
    if completed_olap:
        result, context = completed_olap
        st.session_state.olap_context = context
        if result.response is not None:
            st.session_state.olap_result = result.response
            st.session_state.olap_error = None
        else:
            st.session_state.olap_result = None
            st.session_state.olap_error = _format_transport_error(
                result,
                "executar a consulta OLAP",
                requires_gateway_db=True,
            )
        st.rerun()

    if st.button("Disparar Query ao Gateway", type="primary", use_container_width=True,
                 disabled=is_tcp_task_pending("olap_task")):
        req = messages_pb2.ClientRequest()
        req.type = messages_pb2.REQUEST_TYPE_ANALYTICS_QUERY
        req.query_op = operacao[1]
        req.query_metric = metrica_alvo[1]
        agora = int(time.time())
        req.end_timestamp = agora
        req.start_timestamp = agora - (janela_horas * 3600)
        submit_tcp_request("olap_task", req, {
            "operacao": operacao,
            "metrica_alvo": metrica_alvo,
            "janela_horas": janela_horas,
        })
        st.session_state.olap_result = None
        st.session_state.olap_error = None
        st.rerun()

    # Renderiza resultado armazenado (persiste entre rerenders)
    resp = st.session_state.olap_result
    ctx  = st.session_state.olap_context
    if st.session_state.olap_error:
        st.error(st.session_state.olap_error)
    elif resp and ctx:
        op_ctx      = ctx["operacao"]
        metrica_ctx = ctx["metrica_alvo"]
        janela_ctx  = ctx["janela_horas"]

        if resp.success:
            st.divider()

            col_result, col_metadata = st.columns([2, 1])

            with col_result:
                icon  = METRIC_ICONS.get(metrica_ctx[1], "📊")
                unit  = METRIC_UNITS.get(metrica_ctx[1], "")
                value = resp.analytics_result

                label_desc = op_ctx[0].split(" ")[1] if "Maior" not in op_ctx[0] else "Variação"
                st.metric(label=f"{icon} {label_desc} Agregada — {metrica_ctx[0]}",
                          value=f"{value:.2f} {unit}".strip())

                # Contextualização Paramétrica
                ref_range = get_metric_reference_range(metrica_ctx[1])
                if metrica_ctx[1] == "aqi":
                    st.info(f"**Detecção Automática AQI:** {aqi_category(value)}  \n"
                            "(Base EPA): 0-50 Bom · 51-100 Moderado · 101-150 Sensíveis · 151-200 Insalubre")
                elif "safe" in ref_range and "warning" in ref_range:
                    safe_min, safe_max = ref_range["safe"]
                    warn_min, warn_max = ref_range["warning"]

                    if safe_min <= value <= safe_max:
                        st.success(f"**🟢 Estável: Parâmetro operando no envelope seguro.**\n\n{ref_range.get('description', '')}")
                    elif warn_min <= value <= warn_max:
                        st.warning(f"**🟡 Atenção: Desvio tolerável do ideal.**\n\n{ref_range.get('description', '')}")
                    else:
                        st.error(f"**🔴 Crítico: Integridade térmica/física comprometida.**\n\n{ref_range.get('description', '')}")

                # Desenho da Linha Temporal Nativa (Isolado de sessões fantasma)
                st.subheader(f"📈 Extração do Histórico Contínuo ({janela_ctx}h)")

                if len(resp.graph_points) > 0:
                    chart_data = pd.DataFrame([
                        {
                            "Eixo X": datetime.datetime.fromtimestamp(pt.timestamp).strftime("%H:%M:%S"),
                            "Grandeza Física": pt.value,
                            "MAC Address / ID": pt.device_id
                        }
                        for pt in resp.graph_points
                    ])

                    # Pivotagem protegida contra colisões atômicas do log UDP (agrupamento aritmético do milissegundo)
                    chart_pivot = pd.pivot_table(
                        chart_data,
                        index="Eixo X",
                        columns="MAC Address / ID",
                        values="Grandeza Física",
                        aggfunc="mean"
                    )

                    st.line_chart(chart_pivot, use_container_width=True, height=360)
                else:
                    st.info("📋 Base de dados desprovida de medições brutas no intervalo de amostragem requisitado.")

            with col_metadata:
                st.info(f"📋 **Estatística da Extração SQL:**\n\n{resp.result_metadata}\n\n**Escopo Analítico:**\n{janela_ctx} hora(s)")
                st.caption(f"**Token de Assinatura (ID):** {resp.message_id}")
                st.caption(f"**Geração do Report:** {datetime.datetime.fromtimestamp(resp.timestamp).strftime('%H:%M:%S')}")
        else:
            st.warning(f"⚠️ Restrição Computacional do Gateway: {resp.message}")
    elif resp is not None:
        st.error(_format_transport_error(
            TcpRequestResult(error_code="REQUEST_FAILED"),
            "executar a consulta OLAP",
            requires_gateway_db=True,
        ))

    # Informações sobre o processamento
    st.divider()
    with st.expander("Como funciona o OLAP no Gateway?"):
        st.markdown("""
        O Gateway implementa **Online Analytical Processing (OLAP)** para análises rápidas:

        - **Média Aritmética**: média simples dos valores da série temporal
        - **Desvio Padrão**: variação dos dados em relação à média
        - **Maior Variação por Dispositivo**: identifica o sensor com maior amplitude (máx − mín) na janela selecionada

        Os dados são agregados no servidor e apenas o escalar resultante é transmitido ao cliente.

        **Métricas disponíveis por sensor:**
        | Sensor | Métricas |
        |--------|----------|
        | 🌡️ Estação Ambiental (C) | `temperature` · `humidity` · `co2` · `pm25` · `pm10` · `aqi` |
        | 💡 Poste Inteligente (Lua) | `luminosity` · `power_consumption` |
        | 🚦 Semáforo (Java) | `state` · `queue_length` |
        | 📹 Câmera de Tráfego (Python) | `vehicles_count` · `infractions` |

        **Referência de qualidade do ar (AQI — EPA):**
        `0–50` Bom · `51–100` Moderado · `101–150` Insalubre (sensíveis) ·
        `151–200` Insalubre · `201–300` Muito insalubre · `>300` Perigoso
        """)

# --------------------------------------------------------------------
# ABA 4: Inspeção Individual (Diagnóstico Local Vetorizado)
# --------------------------------------------------------------------
with tab4:
    st.subheader("🔍 Inspeção Individual e Diagnóstico de Telemetria")

    if not st.session_state.device_history:
        st.info("Topologia desconhecida. Sincronize a rede na aba 'Fontes de Dados'.")
    else:
        all_devices  = st.session_state.device_history
        # Indexação por device_id usando dicts.
        device_lookup = {d["device_id"]: d for d in all_devices}

        c_dev, c_metric, c_time = st.columns([2, 2, 1])

        with c_dev:
            target_inspec_id = st.selectbox(
                "Nó Analisado",
                options=list(device_lookup.keys()),
                key="tab4_selected_device_id",
            )

        selected_inspec_device = device_lookup[target_inspec_id]
        available_metrics = DEVICE_METRICS_MAP.get(selected_inspec_device["type"], ["state"])

        with c_metric:
            metrica_inspec_alvo = st.selectbox(
                "Métrica Operacional",
                options=available_metrics,
                format_func=lambda x: f"{METRIC_ICONS.get(x, '📊')} {x.replace('_', ' ').title()}",
            )

        with c_time:
            janela_inspec = st.slider(
                "Janela (Horas)",
                min_value=1,
                max_value=24,
                value=1,
                key="tab4_slider",
            )

        # Consome resultado de inspeção enviado em background.
        completed_inspec = consume_tcp_task("inspection_task", "Varredura do sensor")
        if completed_inspec:
            result, context = completed_inspec
            st.session_state.inspection_context = context
            if result.response is not None:
                st.session_state.inspection_result = result.response
                st.session_state.inspection_error = None
            else:
                st.session_state.inspection_result = None
                st.session_state.inspection_error = _format_transport_error(
                    result,
                    "executar a inspeção individual",
                    requires_gateway_db=True,
                )
            st.rerun()

        if st.button("Executar Varredura do Sensor", type="primary", use_container_width=True,
                     disabled=is_tcp_task_pending("inspection_task")):
            req = messages_pb2.ClientRequest()
            req.type            = messages_pb2.REQUEST_TYPE_ANALYTICS_QUERY
            req.query_op        = messages_pb2.OP_AVERAGE
            req.query_metric    = metrica_inspec_alvo
            req.target_device_id = target_inspec_id
            agora = int(time.time())
            req.end_timestamp   = agora
            req.start_timestamp = agora - (janela_inspec * 3600)
            submit_tcp_request("inspection_task", req, {
                "target_id":    target_inspec_id,
                "metrica":      metrica_inspec_alvo,
                "janela":       janela_inspec,
            })
            st.session_state.inspection_result  = None
            st.session_state.inspection_context = None
            st.session_state.inspection_error = None
            st.rerun()

        # Renderiza resultado armazenado (persiste entre rerenders)
        resp = st.session_state.inspection_result
        ctx  = st.session_state.inspection_context
        if st.session_state.inspection_error:
            st.error(st.session_state.inspection_error)
        elif resp and ctx:
            if resp.success:
                filtered_points = [
                    pt for pt in resp.graph_points
                    if pt.device_id == ctx["target_id"]
                ]

                if not filtered_points:
                    st.warning(
                        f"Não há pontos de `{ctx['metrica']}` emitidos por "
                        f"`{ctx['target_id']}` na janela solicitada."
                    )
                else:
                    df_inspec = pd.DataFrame([
                        {"timestamp": pt.timestamp, "value": pt.value}
                        for pt in filtered_points
                    ]).sort_values(by="timestamp").reset_index(drop=True)

                    df_inspec["delta_t"] = df_inspec["timestamp"].diff()
                    mean_interval = df_inspec["delta_t"].mean()
                    same_second_samples = int((df_inspec["delta_t"] == 0.0).sum())
                    same_second_rate = (
                        (same_second_samples / len(df_inspec)) * 100
                        if len(df_inspec) > 0 else 0.0
                    )

                    st.divider()
                    st.markdown(f"### Saúde Operacional: `{ctx['target_id']}`")

                    col_kpi1, col_kpi2, col_kpi3 = st.columns(3)
                    col_kpi1.metric("Amostras Extraídas", len(df_inspec))

                    interval_label = (
                        "Amostra insuficiente"
                        if pd.isna(mean_interval)
                        else f"{mean_interval:.2f}s"
                    )
                    col_kpi2.metric(
                        "Intervalo Médio Entre Amostras",
                        interval_label,
                        help="Média do intervalo entre timestamps consecutivos enviados pelo sensor.",
                    )
                    col_kpi3.metric(
                        "Eventos no Mesmo Segundo",
                        f"{same_second_rate:.1f}%",
                        f"{same_second_samples} ocorrência(s)",
                        delta_color="off",
                        help=(
                            "Taxa de eventos com timestamps idênticos. Comportamento esperado em rajadas "
                            "ou quando limiares são detectados (telemetria periódica + evento disparado no mesmo segundo). "
                            "O gateway deduplicou por (timestamp, message_id), então todos os eventos são legítimos."
                        ),
                    )

                    st.markdown("---")
                    col_graph, col_table = st.columns([2, 1])

                    with col_graph:
                        st.write(f"#### Comportamento do Sinal: **{ctx['metrica']}**")
                        df_inspec["Horário"] = pd.to_datetime(
                            df_inspec["timestamp"], unit="s"
                        ).dt.strftime("%H:%M:%S")
                        st.line_chart(
                            df_inspec.set_index("Horário")["value"],
                            use_container_width=True,
                            height=350,
                        )

                    with col_table:
                        unit = METRIC_UNITS.get(ctx["metrica"], "")
                        value_label = (
                            f"Medição ({unit})" if unit else "Medição"
                        )
                        st.write("#### Registros do Sensor")
                        st.dataframe(
                            df_inspec[["Horário", "value"]].rename(
                                columns={"value": value_label}
                            ),
                            use_container_width=True,
                            hide_index=True,
                        )
            elif resp:
                st.error(f"Falha na extração de dados do nó: {resp.message}")
        elif resp is not None:
            st.error(_format_transport_error(
                TcpRequestResult(error_code="REQUEST_FAILED"),
                "executar a inspeção individual",
                requires_gateway_db=True,
            ))
# --------------------------------------------------------------------
# ABA 5: Mapa Interativo (Matriz)
# --------------------------------------------------------------------
with tab5:
    st.subheader("🗺️ Mapa Interativo da Universidade (Matriz 100x100)")
    
    if "device_history" not in st.session_state or not st.session_state.device_history:
        st.info("Nenhum dispositivo encontrado. Vá para a aba 'Fontes de Dados' e atualize a topologia.")
    else:
        df_map = pd.DataFrame(st.session_state.device_history)
        
        # Filtros
        col_filters1, col_filters2 = st.columns(2)
        with col_filters1:
            map_mode = st.radio(
                "Modo de Visualização",
                ["Gráfico de Dispersão (Scatter)", "Mapa de Calor (Heatmap)",
                 "Mini-mapas por Setor", "Replay Temporal"],
                horizontal=True,
            )
        with col_filters2:
            device_types = df_map["type"].unique()
            selected_types = st.multiselect("Filtrar por Tipo de Sensor", 
                options=device_types, 
                default=device_types,
                format_func=lambda x: {
                    messages_pb2.DEVICE_TYPE_CAMERA: "Câmera (Python)",
                    messages_pb2.DEVICE_TYPE_TRAFFIC_LIGHT: "Semáforo (Java)",
                    messages_pb2.DEVICE_TYPE_LAMP_POST: "Poste Inteligente (Lua)",
                    messages_pb2.DEVICE_TYPE_WEATHER_STATION: "Estação Ambiental (C)",
                    messages_pb2.DEVICE_TYPE_FLOOD: "Enchente (Node.js)",
                    messages_pb2.DEVICE_TYPE_NOISE: "Ruído (Ruby)"
                }.get(x, f"Desconhecido ({x})")
            )
        
        if selected_types:
            df_filtered = df_map[df_map["type"].isin(selected_types)].copy()
            
            # Map type to string for hover
            df_filtered["type_str"] = df_filtered["type"].map(lambda x: {
                    messages_pb2.DEVICE_TYPE_CAMERA: "Câmera",
                    messages_pb2.DEVICE_TYPE_TRAFFIC_LIGHT: "Semáforo",
                    messages_pb2.DEVICE_TYPE_LAMP_POST: "Poste Inteligente",
                    messages_pb2.DEVICE_TYPE_WEATHER_STATION: "Estação Ambiental",
                    messages_pb2.DEVICE_TYPE_FLOOD: "Enchente",
                    messages_pb2.DEVICE_TYPE_NOISE: "Ruído"
                }.get(x, "Desconhecido")
            )
            df_filtered["status_str"] = df_filtered["status"].map(lambda x: {
                    messages_pb2.STATUS_ON: "ON",
                    messages_pb2.STATUS_OFF: "OFF",
                    messages_pb2.STATUS_ERROR: "ERROR"
                }.get(x, "UNKNOWN")
            )

            if df_filtered.empty:
                st.warning("Nenhum dispositivo corresponde aos filtros.")
            elif map_mode in ("Gráfico de Dispersão (Scatter)", "Mapa de Calor (Heatmap)"):
                if map_mode == "Gráfico de Dispersão (Scatter)":
                    fig = px.scatter(
                        df_filtered,
                        x="coord_x",
                        y="coord_y",
                        color="type_str",
                        hover_name="device_id",
                        # custom_data carrega o device_id em cada ponto. Como px.scatter
                        # cria uma série (trace) por type_str, o pointIndex da seleção é
                        # relativo à série, não ao DataFrame — usar iloc[pointIndex]
                        # retornaria o nó errado. Lendo customdata recuperamos o ID exato.
                        custom_data=["device_id"],
                        hover_data={
                            "type_str": True,
                            "status_str": True,
                            "coord_x": True,
                            "coord_y": True,
                            "ip_address": True,
                            "aggregator_id": True
                        },
                        title="Localização dos Sensores (Matriz 100x100)",
                        labels={"coord_x": "Coordenada X", "coord_y": "Coordenada Y", "type_str": "Tipo"},
                        range_x=[0, 100],
                        range_y=[0, 100],
                        height=600
                    )
                    fig.update_traces(marker=dict(size=12, line=dict(width=2, color='DarkSlateGrey')))
                else:
                    fig = px.density_heatmap(
                        df_filtered,
                        x="coord_x",
                        y="coord_y",
                        title="Densidade de Sensores (Heatmap)",
                        labels={"coord_x": "Coordenada X", "coord_y": "Coordenada Y"},
                        range_x=[0, 100],
                        range_y=[0, 100],
                        nbinsx=20,
                        nbinsy=20,
                        height=600
                    )

                # Make interactive via st.plotly_chart
                event = st.plotly_chart(fig, use_container_width=True, on_select="rerun")

                if map_mode == "Gráfico de Dispersão (Scatter)" and event and event.get("selection") and event["selection"]["points"]:
                    selected_point = event["selection"]["points"][0]
                    # Recupera o device_id a partir do customdata do ponto selecionado.
                    # Isso é robusto a múltiplas séries (uma por tipo) no gráfico.
                    selected_device_id = None
                    customdata = selected_point.get("customdata")
                    if customdata:
                        selected_device_id = customdata[0]

                    matches = df_filtered[df_filtered["device_id"] == selected_device_id]
                    if selected_device_id is not None and not matches.empty:
                        device_info = matches.iloc[0]

                        st.markdown("### Detalhes do Sensor Selecionado")
                        st.json({
                            "ID do Dispositivo": device_info["device_id"],
                            "Tipo": device_info["type_str"],
                            "Status": device_info["status_str"],
                            "Coordenadas": f"X:{device_info['coord_x']}, Y:{device_info['coord_y']}",
                            "IP": device_info["ip_address"],
                            "Agregador": device_info["aggregator_id"],
                            "Última Vez Visto": datetime.datetime.fromtimestamp(device_info["last_seen_timestamp"]).strftime('%Y-%m-%d %H:%M:%S') if device_info["last_seen_timestamp"] else "Desconhecido"
                        })

            # ── Modo: Mini-mapas por Setor ───────────────────────────
            elif map_mode == "Mini-mapas por Setor":
                st.markdown("#### Mini-mapas por Setor")
                df_filtered["setor"] = df_filtered["device_id"].map(infer_sector_from_device_id)
                sectors = sorted(s for s in df_filtered["setor"].unique() if s)
                if not sectors:
                    st.info("Sem setores identificáveis nos dispositivos filtrados.")
                else:
                    cols = st.columns(2)
                    for i, sector in enumerate(sectors):
                        sub = df_filtered[df_filtered["setor"] == sector]
                        online = int((sub["status"] == messages_pb2.STATUS_ON).sum())
                        with cols[i % 2]:
                            st.markdown(f"**{sector}** — {len(sub)} nó(s) · {online} ON")
                            fig_s = px.scatter(
                                sub, x="coord_x", y="coord_y", color="type_str",
                                hover_name="device_id", range_x=[0, 100], range_y=[0, 100], height=320,
                            )
                            fig_s.update_traces(marker=dict(size=11, line=dict(width=1, color='DarkSlateGrey')))
                            fig_s.update_layout(showlegend=False, margin=dict(l=8, r=8, t=10, b=8))
                            st.plotly_chart(fig_s, use_container_width=True, key=f"minimap_{sector}")

            # ── Modo: Replay Temporal (time-travel) ──────────────────
            elif map_mode == "Replay Temporal":
                st.markdown("#### ⏯️ Replay Temporal (time-travel)")
                st.caption("Reconstrói a evolução de uma métrica no mapa. Use ▶ play / o slider do gráfico; baixe os quadros em CSV.")
                rc1, rc2, rc3 = st.columns([2, 1, 1])
                with rc1:
                    replay_metric = st.selectbox("Métrica", _AUTOMATION_METRICS, key="replay_metric")
                with rc2:
                    replay_hours = st.slider("Janela (h)", 1, 24, 1, key="replay_hours")
                with rc3:
                    replay_buckets = st.slider("Quadros", 6, 60, 20, key="replay_buckets")

                if st.button("▶️ Carregar Replay", key="replay_load", use_container_width=True):
                    req = messages_pb2.ClientRequest()
                    req.type = messages_pb2.REQUEST_TYPE_ANALYTICS_QUERY
                    req.query_op = messages_pb2.OP_AVERAGE
                    req.query_metric = replay_metric
                    agora = int(time.time())
                    req.end_timestamp = agora
                    req.start_timestamp = agora - replay_hours * 3600
                    result = get_gateway_client().request(req)
                    if result.response is None or not result.response.success or not result.response.graph_points:
                        st.session_state.pop("replay_data", None)
                        st.warning("Sem dados para o replay nessa janela/métrica.")
                    else:
                        st.session_state["replay_data"] = {
                            "metric": replay_metric,
                            "buckets": int(replay_buckets),
                            "points": [(int(p.timestamp), float(p.value), p.device_id) for p in result.response.graph_points],
                        }

                rdata = st.session_state.get("replay_data")
                if rdata and rdata.get("metric") == replay_metric and rdata.get("points"):
                    coords = {d["device_id"]: (d["coord_x"], d["coord_y"]) for d in st.session_state.device_history}
                    pts = rdata["points"]
                    tmin = min(p[0] for p in pts)
                    tmax = max(p[0] for p in pts)
                    span = max(1, tmax - tmin)
                    nb = max(2, rdata["buckets"])
                    rows = []
                    for ts, val, dev in pts:
                        if dev not in coords:
                            continue
                        bucket = int((ts - tmin) / span * (nb - 1))
                        b_ts = tmin + int(bucket * span / (nb - 1))
                        frame = f"{bucket:02d} · {datetime.datetime.fromtimestamp(b_ts).strftime('%H:%M:%S')}"
                        cx, cy = coords[dev]
                        rows.append({"frame": frame, "device_id": dev, "coord_x": cx, "coord_y": cy, "value": val})
                    if not rows:
                        st.info("Os pontos retornados não casam com dispositivos da topologia atual.")
                    else:
                        df_replay = (
                            pd.DataFrame(rows)
                            .groupby(["frame", "device_id", "coord_x", "coord_y"], as_index=False)["value"].mean()
                            .sort_values("frame")
                        )
                        fig_r = px.scatter(
                            df_replay, x="coord_x", y="coord_y",
                            animation_frame="frame", color="value", hover_name="device_id",
                            range_x=[0, 100], range_y=[0, 100], height=600,
                            color_continuous_scale="Turbo",
                            title=f"Replay de {replay_metric} ({replay_hours}h, {nb} quadros)",
                        )
                        fig_r.update_traces(marker=dict(size=14, line=dict(width=1, color='DarkSlateGrey')))
                        st.plotly_chart(fig_r, use_container_width=True)
                        st.download_button(
                            "⬇️ Exportar quadros (CSV)",
                            df_replay.to_csv(index=False).encode("utf-8"),
                            file_name=f"replay_{replay_metric}.csv", mime="text/csv",
                        )
                else:
                    st.info("Escolha a métrica/janela e clique em **Carregar Replay**.")

# --------------------------------------------------------------------
# ABA 6: Auditoria de Comandos (stream Redis 'audit')
# --------------------------------------------------------------------
@st.cache_resource
def get_redis_client() -> "redis.Redis":
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True,
                       socket_timeout=2.0, socket_connect_timeout=2.0)


def fetch_audit_entries(limit: int = 100):
    """Lê os últimos comandos auditados do stream Redis 'audit' (mais recentes primeiro)."""
    client = get_redis_client()
    return client.xrevrange("audit", count=limit)


with tab6:
    st.subheader("🧾 Auditoria de Comandos de Atuação")
    st.caption(
        "Registro append-only (stream Redis `audit`) de todos os comandos de "
        "controle processados pelo Gateway — origem, alvo, ação e resultado."
    )

    col_n, col_btn = st.columns([1, 1])
    with col_n:
        audit_limit = st.slider("Quantidade de registros", 10, 500, 100, step=10, key="audit_limit")
    with col_btn:
        st.write("")
        st.write("")
        refresh = st.button("🔄 Atualizar Auditoria", use_container_width=True)

    try:
        entries = fetch_audit_entries(audit_limit)
        if not entries:
            st.info("📋 Nenhum comando auditado ainda. Os comandos de atuação aparecerão aqui.")
        else:
            rows = []
            for entry_id, fields in entries:
                ts_raw = fields.get("ts", "")
                try:
                    ts_fmt = datetime.datetime.fromtimestamp(int(ts_raw)).strftime("%Y-%m-%d %H:%M:%S")
                except (ValueError, TypeError):
                    ts_fmt = ts_raw
                rows.append({
                    "Quando": ts_fmt,
                    "Origem (IP)": fields.get("peer", "?"),
                    "Dispositivo Alvo": fields.get("target", ""),
                    "Ação": fields.get("action", ""),
                    "Comando": fields.get("command_id", ""),
                    "Resultado": "✓ OK" if fields.get("success") == "1" else "✗ Falha",
                    "Mensagem": fields.get("message", ""),
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            st.caption(f"Exibindo {len(rows)} registro(s) mais recente(s).")
    except redis.RedisError as exc:
        st.error(
            f"Não foi possível ler a auditoria do Redis (`{REDIS_HOST}:{REDIS_PORT}`). "
            f"Verifique se o broker está online. Detalhe: {exc}"
        )


# --------------------------------------------------------------------
# ABA 7: Alertas em Tempo Real (stream Redis 'alerts')
# --------------------------------------------------------------------
_SEVERITY_BADGE = {"critical": "🔴 Crítico", "warning": "🟡 Alerta", "info": "🔵 Info"}


def fetch_alert_entries(limit: int = 100):
    """Lê os alertas mais recentes do stream Redis 'alerts'."""
    client = get_redis_client()
    return client.xrevrange("alerts", count=limit)


with tab7:
    st.subheader("🚨 Alertas em Tempo Real")
    st.caption(
        "Eventos publicados pelo Gateway (stream Redis `alerts`) quando uma métrica "
        "rompe seu limiar. Também são enviados a um webhook externo, se configurado."
    )

    ca1, ca2, ca3 = st.columns([1, 1, 1])
    with ca1:
        alert_limit = st.slider("Quantidade", 10, 500, 100, step=10, key="alert_limit")
    with ca2:
        sev_filter = st.multiselect(
            "Severidade", ["critical", "warning", "info"],
            default=["critical", "warning", "info"],
            format_func=lambda s: _SEVERITY_BADGE.get(s, s), key="alert_sev")
    with ca3:
        st.write("")
        st.write("")
        st.button("🔄 Atualizar Alertas", use_container_width=True)

    try:
        entries = fetch_alert_entries(alert_limit)
        rows = []
        for entry_id, fields in entries:
            sev = fields.get("severity", "info")
            if sev_filter and sev not in sev_filter:
                continue
            ts_raw = fields.get("ts", "")
            try:
                ts_fmt = datetime.datetime.fromtimestamp(int(ts_raw)).strftime("%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError):
                ts_fmt = ts_raw
            rows.append({
                "Quando": ts_fmt,
                "Severidade": _SEVERITY_BADGE.get(sev, sev),
                "Dispositivo": fields.get("device_id", ""),
                "Métrica": fields.get("metric", ""),
                "Valor": fields.get("value", ""),
                "Limiar": f"{fields.get('op', '')} {fields.get('threshold', '')}".strip(),
                "Mensagem": fields.get("message", ""),
            })

        crit_n = sum(1 for r in rows if "Crítico" in r["Severidade"])
        warn_n = sum(1 for r in rows if "Alerta" in r["Severidade"])
        k1, k2, k3 = st.columns(3)
        k1.metric("Total exibido", len(rows))
        k2.metric("🔴 Críticos", crit_n)
        k3.metric("🟡 Alertas", warn_n)

        if not rows:
            st.success("✓ Nenhum alerta no filtro atual. Sistema dentro dos limiares.")
        else:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

            with st.expander("📈 Análise dos alertas exibidos"):
                df_a = pd.DataFrame(rows)
                an1, an2 = st.columns(2)
                with an1:
                    st.caption("Alertas por dispositivo")
                    st.bar_chart(df_a["Dispositivo"].value_counts())
                with an2:
                    st.caption("Alertas por métrica")
                    st.bar_chart(df_a["Métrica"].value_counts())
    except redis.RedisError as exc:
        st.error(
            f"Não foi possível ler os alertas do Redis (`{REDIS_HOST}:{REDIS_PORT}`). "
            f"Verifique se o broker está online. Detalhe: {exc}"
        )

    # ── Silenciar / Reconhecer (ack) ─────────────────────────────────
    st.divider()
    with st.expander("🔕 Silenciar / Reconhecer Alertas (ack)"):
        st.caption(
            "Silencia um par dispositivo+métrica por um período — o Gateway para "
            "de gerar esse alerta até expirar."
        )
        dev_opts = [d["device_id"] for d in st.session_state.get("device_history", [])]
        s1, s2, s3 = st.columns([2, 2, 1])
        with s1:
            sil_device = st.selectbox(
                "Dispositivo", dev_opts or ["(sincronize a topologia na aba 1)"], key="sil_dev")
        with s2:
            sil_metric = st.selectbox("Métrica", _AUTOMATION_METRICS, key="sil_metric")
        with s3:
            sil_minutes = st.number_input("Minutos", min_value=1, value=15, step=5, key="sil_min")

        if st.button("🔕 Silenciar", key="sil_apply"):
            if not dev_opts:
                st.warning("Sincronize a topologia primeiro (aba 'Fontes de Dados').")
            else:
                try:
                    expiry = int(time.time()) + int(sil_minutes) * 60
                    get_redis_client().hset("alert_silences", f"{sil_device}|{sil_metric}", str(expiry))
                    st.success(f"Silenciado: {sil_device} / {sil_metric} por {sil_minutes} min.")
                except redis.RedisError as exc:
                    st.error(f"Falha ao silenciar: {exc}")

        # Silenciamentos ativos
        try:
            raw_sil = get_redis_client().hgetall("alert_silences")
            now_ts = int(time.time())
            active = []
            for field, value in (raw_sil or {}).items():
                try:
                    if int(value) > now_ts:
                        dev, _, met = field.partition("|")
                        active.append((field, dev, met, int(value) - now_ts))
                except (TypeError, ValueError):
                    continue
            if active:
                st.markdown("**Silenciamentos ativos:**")
                for field, dev, met, remaining in active:
                    cc1, cc2 = st.columns([5, 1])
                    cc1.markdown(f"• `{dev}` / `{met}` — expira em ~{remaining // 60}min{remaining % 60:02d}s")
                    if cc2.button("Remover", key=f"unsil_{field}"):
                        get_redis_client().hdel("alert_silences", field)
                        st.rerun()
        except redis.RedisError:
            pass


# --------------------------------------------------------------------
# ABA 8: Automação (Regras IFTTT — editáveis, persistidas no Redis)
# --------------------------------------------------------------------
_AUTOMATION_OPS = [">=", "<=", ">", "<"]
_AUTOMATION_STATUS = {"Ligar (ON)": "STATUS_ON", "Desligar (OFF)": "STATUS_OFF", "Falha (ERROR)": "STATUS_ERROR"}


def load_automation_rules() -> list:
    try:
        raw = get_redis_client().get("automation_rules")
        return json.loads(raw) if raw else []
    except (redis.RedisError, json.JSONDecodeError, TypeError):
        return []


def save_automation_rules(rules: list) -> None:
    get_redis_client().set("automation_rules", json.dumps(rules))


with tab8:
    st.subheader("⚡ Motor de Automação (IFTTT)")
    st.caption(
        "Regras `SE métrica <op> limiar ENTÃO comando`, avaliadas no Gateway sobre "
        "a telemetria. As ações reusam o canal de atuação (com cripto/anti-replay se ligado)."
    )

    try:
        rules = load_automation_rules()
    except Exception as exc:  # noqa: BLE001
        rules = []
        st.error(f"Falha ao carregar regras do Redis: {exc}")

    # ── Regras existentes ────────────────────────────────────────────
    st.markdown("#### Regras Ativas")
    if not rules:
        st.info("Nenhuma regra cadastrada ainda. Crie uma abaixo.")
    else:
        for idx, rule in enumerate(rules):
            tgt = rule.get("target_device_id") or "(dispositivo que disparou)"
            acts = []
            if rule.get("action_status"):
                acts.append(f"status={rule.get('status')}")
            if rule.get("action_freq"):
                acts.append(f"freq={rule.get('frequency_secs')}s")
            label = (
                f"**{rule.get('name', rule.get('id'))}** — "
                f"SE `{rule.get('metric')} {rule.get('op')} {rule.get('threshold')}` "
                f"ENTÃO `{', '.join(acts) or '—'}` em `{tgt}` "
                f"(cooldown {rule.get('cooldown_secs', 60)}s)"
            )
            c_txt, c_tog, c_del = st.columns([6, 1, 1])
            with c_txt:
                status_icon = "🟢" if rule.get("enabled", True) else "⚪"
                st.markdown(f"{status_icon} {label}")
            with c_tog:
                if st.button("On/Off", key=f"rule_tog_{idx}"):
                    rules[idx]["enabled"] = not rules[idx].get("enabled", True)
                    save_automation_rules(rules)
                    st.rerun()
            with c_del:
                if st.button("🗑️", key=f"rule_del_{idx}"):
                    rules.pop(idx)
                    save_automation_rules(rules)
                    st.rerun()

    st.divider()

    # ── Nova regra ───────────────────────────────────────────────────
    st.markdown("#### Adicionar Regra")

    controllable = [d for d in st.session_state.get("device_history", []) if d.get("is_controllable")]
    target_options = ["(dispositivo que disparou)"] + [d["device_id"] for d in controllable]

    with st.form("new_rule_form"):
        r1, r2, r3, r4 = st.columns([2, 1, 1, 1])
        with r1:
            rule_name = st.text_input("Nome da regra", placeholder="ex.: AQI alto desliga câmera")
        with r2:
            rule_metric = st.selectbox("Métrica", _AUTOMATION_METRICS)
        with r3:
            rule_op = st.selectbox("Operador", _AUTOMATION_OPS)
        with r4:
            rule_threshold = st.number_input("Limiar", value=100.0, step=1.0)

        r5, r6 = st.columns(2)
        with r5:
            rule_target = st.selectbox("Dispositivo alvo da ação", target_options)
            rule_cooldown = st.number_input("Cooldown (s)", min_value=1, value=60, step=5)
        with r6:
            rule_act_status = st.checkbox("Ação: alterar status")
            rule_status_label = st.selectbox("Novo estado", list(_AUTOMATION_STATUS.keys()),
                                             disabled=not rule_act_status)
            rule_act_freq = st.checkbox("Ação: alterar frequência")
            rule_freq = st.slider("Frequência (s)", 1, 60, 5, disabled=not rule_act_freq)

        submitted = st.form_submit_button("➕ Criar Regra", type="primary", use_container_width=True)
        if submitted:
            if not rule_name.strip():
                st.warning("Dê um nome à regra.")
            elif not rule_act_status and not rule_act_freq:
                st.warning("Selecione ao menos uma ação (status e/ou frequência).")
            else:
                new_rule = {
                    "id": f"rule_{uuid.uuid4().hex[:8]}",
                    "name": rule_name.strip(),
                    "enabled": True,
                    "metric": rule_metric,
                    "op": rule_op,
                    "threshold": float(rule_threshold),
                    "target_device_id": "" if rule_target.startswith("(") else rule_target,
                    "action_status": bool(rule_act_status),
                    "status": _AUTOMATION_STATUS[rule_status_label] if rule_act_status else "",
                    "action_freq": bool(rule_act_freq),
                    "frequency_secs": int(rule_freq) if rule_act_freq else 0,
                    "cooldown_secs": int(rule_cooldown),
                }
                rules.append(new_rule)
                try:
                    save_automation_rules(rules)
                    st.success(f"Regra '{new_rule['name']}' criada.")
                    st.rerun()
                except redis.RedisError as exc:
                    st.error(f"Falha ao salvar no Redis: {exc}")


# --------------------------------------------------------------------
# ABA 9: Observabilidade do Sistema (métricas internas via Redis 'gw_metrics')
# --------------------------------------------------------------------
def fetch_gw_metrics() -> dict | None:
    try:
        raw = get_redis_client().get("gw_metrics")
        return json.loads(raw) if raw else None
    except (redis.RedisError, json.JSONDecodeError, TypeError):
        return None


with tab9:
    st.subheader("📈 Observabilidade do Sistema")
    st.caption(
        "Saúde interna da plataforma, publicada pelo Gateway (Redis `gw_metrics`) "
        "e exposta também em formato Prometheus em `gateway:9100/metrics`."
    )

    if st.button("🔄 Atualizar Métricas", key="obs_refresh"):
        st.rerun()

    snap = fetch_gw_metrics()
    if not snap:
        st.info(
            "Sem métricas ainda. O Gateway publica a cada ~10s; verifique se ele e o "
            "Redis estão online."
        )
    else:
        age = int(time.time()) - int(snap.get("ts", 0))
        st.caption(f"Snapshot de ~{age}s atrás.")

        gauges = snap.get("gauges", {})
        counters = snap.get("counters", {})
        aggs = snap.get("aggregators", {})

        # KPIs principais
        g1, g2, g3, g4 = st.columns(4)
        q = gauges.get("telemetry_queue_size", 0)
        qmax = gauges.get("telemetry_queue_max", 1) or 1
        g1.metric("Fila de Telemetria", f"{q}/{qmax}", f"{(q / qmax) * 100:.0f}% cheia", delta_color="off")
        g2.metric("Dispositivos ON", f"{gauges.get('devices_online', 0)}/{gauges.get('devices_total', 0)}")
        g3.metric("Portas UDP Livres", f"{gauges.get('available_ports', 0)}/{gauges.get('max_ports', 0)}")
        g4.metric("Canal Seguro", "🔒 ON" if gauges.get("control_secure") else "🔓 OFF")

        # Contadores acumulados
        st.markdown("#### Contadores acumulados")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Payloads de Telemetria", counters.get("telemetry_payloads_total", 0))
        c2.metric("Métricas Persistidas", counters.get("metrics_persisted_total", 0))
        c3.metric("Alertas Gerados", counters.get("alerts_total", 0))
        cmd_total = counters.get("commands_total", 0)
        cmd_fail = counters.get("commands_failed_total", 0)
        c4.metric("Comandos (falhas)", cmd_total, f"{cmd_fail} falha(s)", delta_color="inverse")

        # Saúde dos agregadores (health two-way ACK)
        st.markdown("#### Saúde dos Agregadores (heartbeat ACK)")
        if not aggs:
            st.info("Nenhum agregador conhecido ainda.")
        else:
            rows = []
            for name, info in aggs.items():
                rows.append({
                    "Agregador": name,
                    "Estado": "🟢 ATIVO" if info.get("up") else "🔴 INATIVO",
                    "Silêncio (s)": info.get("silent_for", -1),
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
