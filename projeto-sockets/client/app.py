import streamlit as st
import os
import time
import uuid
import datetime
import pandas as pd
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor


# Importa as classes do Protobuf geradas no momento do build
import messages_pb2 # pyright: ignore[reportMissingImports]
from dashboard_helpers import assess_analytics_value, build_history_chart, build_history_frame
from gateway_transport import GatewayTcpClient, TcpRequestResult, _GATEWAY_UNAVAILABLE_CODES
from metric_catalog import DEVICE_METRICS_MAP, METRIC_ICONS, METRIC_OPTIONS, METRIC_UNITS

# ====================================================================
# CONFIGURAÇÕES DE REDE E TRANSPORTE TCP
# ====================================================================

GATEWAY_HOST = os.getenv("GATEWAY_HOST", "gateway")
GATEWAY_PORT = int(os.getenv("GATEWAY_PORT", "5001"))

# TTL do cache de status do gateway (segundos)
# Evita probe TCP bloqueante a cada rerender do Streamlit.
_GW_STATUS_TTL = 10.0
_REQUEST_WORKERS = 4


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

def get_gateway_client() -> GatewayTcpClient:
    if "gateway_tcp_client" not in st.session_state:
        st.session_state.gateway_tcp_client = GatewayTcpClient(GATEWAY_HOST, GATEWAY_PORT)
    return st.session_state.gateway_tcp_client


def get_request_executor() -> ThreadPoolExecutor:
    if "tcp_request_executor" not in st.session_state:
        st.session_state.tcp_request_executor = ThreadPoolExecutor(max_workers=_REQUEST_WORKERS)
    return st.session_state.tcp_request_executor


def prepare_request_metadata(request: messages_pb2.ClientRequest) -> None:
    """Preenche o envelope na thread do Streamlit antes de delegar o socket."""
    now = int(time.time())
    if not request.message_id:
        request.message_id = f"REQ-{uuid.uuid4().hex}"
    if request.timestamp <= 0:
        request.timestamp = now

    if request.type == messages_pb2.REQUEST_TYPE_SEND_COMMAND:
        command = request.command_payload
        if command.timestamp <= 0:
            command.timestamp = now
        if not command.target_device_id:
            command.target_device_id = request.target_device_id


def submit_tcp_request(task_key: str, request: messages_pb2.ClientRequest, context: dict) -> None:
    prepare_request_metadata(request)
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
    """Completa metadados obrigatórios antes de atravessar a fronteira TCP."""
    prepare_request_metadata(request)
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
        "is_controllable":    bool(d.is_controllable),
        "last_seen_timestamp": int(d.last_seen_timestamp),
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
        "centro": "Centro",
        "campus": "Campus",
        "hospital": "Hospital",
    }
    
    # Busca relaxada para não quebrar com nomes como "CameraPici01"
    for slug, label in sector_map.items():
        if slug in device_id.lower():
            return label
    return "N/A"

# ====================================================================
# MAPAS DE CONFIGURAÇÃO DE DOMÍNIO
# ====================================================================

TYPE_MAP = {
    messages_pb2.DEVICE_TYPE_TRAFFIC_LIGHT: "🚦 Semáforo",
    messages_pb2.DEVICE_TYPE_LAMP_POST: "💡 Poste Inteligente",
    messages_pb2.DEVICE_TYPE_WEATHER_STATION: "🌦️ Estação Met.",
    messages_pb2.DEVICE_TYPE_CAMERA: "📹 Câmera de Tráfego",
    messages_pb2.DEVICE_TYPE_AIR_QUALITY: "💨 Qualidade do Ar",
    messages_pb2.DEVICE_TYPE_PARKING_SENSOR: "🅿️ Estacionamento Inteligente",
    messages_pb2.DEVICE_TYPE_WATER_SENSOR: "🚰 Água e Saneamento (Rust)",
    messages_pb2.DEVICE_TYPE_WASTE_SENSOR: "🗑️ Lixeira Inteligente (TypeScript)",
}

STATUS_MAP = {
    messages_pb2.STATUS_ON: "🟢 ONLINE",
    messages_pb2.STATUS_OFF: "⚪ OFFLINE",
    messages_pb2.STATUS_ERROR: "🔴 FALHA"
}

# ====================================================================
# INICIALIZAÇÃO DA INTERFACE STREAMLIT
# ====================================================================

st.set_page_config(
    page_title="Smart City Analytics Client",
    page_icon="🏙️",
    layout="wide",
    initial_sidebar_state="expanded"
)

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
    col1.metric("Hub de acesso", gateway_status,
                help="Disponibilidade do canal TCP; as requisições verificam o gateway interno.")

    sensor_count = len(st.session_state.device_history) if st.session_state.device_history else "N/A"
    col2.metric("Sensores", sensor_count, help="Atualizar na aba Descoberta")
    col3.metric("Hora UTC", datetime.datetime.now(datetime.timezone.utc).strftime('%H:%M:%S'))
    st.markdown("---")
    st.info("💡 Selecione uma aba para iniciar operações na rede.")

st.divider()

tab1, tab2, tab3, tab4 = st.tabs([
    "📡 Fontes de Dados (Descoberta)", 
    "⚙️ Painel de Atuação (Controle)", 
    "📊 Consultas Analíticas (OLAP)",
    "🔍 Inspeção Individual (Sensor)"
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
                        status_options = {
                            "Ativar controlador (ON)": messages_pb2.STATUS_ON,
                            "Desligar controlador (OFF)": messages_pb2.STATUS_OFF,
                        }
                    elif selected_device["type"] == messages_pb2.DEVICE_TYPE_LAMP_POST:
                        status_options = {
                            "Acender relé (ON)": messages_pb2.STATUS_ON,
                            "Cortar relé (OFF)": messages_pb2.STATUS_OFF,
                        }
                    elif selected_device["type"] == messages_pb2.DEVICE_TYPE_CAMERA:
                        status_options = {
                            "Gravar stream (ON)": messages_pb2.STATUS_ON,
                            "Pausar stream (OFF)": messages_pb2.STATUS_OFF,
                        }
                    elif selected_device["type"] == messages_pb2.DEVICE_TYPE_PARKING_SENSOR:
                        status_options = {
                            "Monitorar vagas (ON)": messages_pb2.STATUS_ON,
                            "Suspender monitoramento (OFF)": messages_pb2.STATUS_OFF,
                        }
                    else:
                        status_options = {
                            "Ativar dispositivo (ON)": messages_pb2.STATUS_ON,
                            "Desativar dispositivo (OFF)": messages_pb2.STATUS_OFF,
                        }

                    novo_status_label = st.radio(
                        "Seletor de Estado Desejado",
                        list(status_options),
                        disabled=not alterar_status,
                    )
                
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
                        cmd.timestamp = int(time.time())
                        cmd.target_device_id = target_id
                        
                        if alterar_status:
                            cmd.update_status = True
                            cmd.target_status = status_options[novo_status_label]
                                
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
            ("📊 Desvio Padrão Amostral", messages_pb2.OP_STD_DEV),
            ("🔀 Cálculo de Maior Variação Geográfica", messages_pb2.OP_MAX_VARIATION),
        ], format_func=lambda x: x[0])

    with c_met:
        metrica_alvo = st.selectbox("Vetor de Telemetria", METRIC_OPTIONS,
                                    format_func=lambda x: x[0])

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

                label_desc = {
                    messages_pb2.OP_AVERAGE: "Média",
                    messages_pb2.OP_STD_DEV: "Desvio padrão amostral",
                    messages_pb2.OP_MAX_VARIATION: "Maior variação por dispositivo",
                }.get(op_ctx[1], "Resultado")
                st.metric(label=f"{icon} {label_desc} — {metrica_ctx[0]}",
                          value=f"{value:.2f} {unit}".strip())

                assessment = assess_analytics_value(
                    metrica_ctx[1], value, is_average=op_ctx[1] == messages_pb2.OP_AVERAGE
                )
                if assessment:
                    kind, message = assessment
                    {"success": st.success, "warning": st.warning,
                     "error": st.error, "info": st.info}[kind](message)
                elif op_ctx[1] != messages_pb2.OP_AVERAGE:
                    st.caption("Desvio padrão e amplitude descrevem dispersão; as faixas de nível se aplicam à média.")

                # Desenho da Linha Temporal Nativa (Isolado de sessões fantasma)
                st.subheader(f"📈 Extração do Histórico Contínuo ({janela_ctx}h)")

                if len(resp.graph_points) > 0:
                    chart_pivot = build_history_chart(resp.graph_points)
                    st.line_chart(chart_pivot, use_container_width=True, height=360)
                    st.caption("Horários em UTC; amostras do mesmo sensor e instante são agrupadas pela média.")
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
        - **Desvio Padrão Amostral**: dispersão dos dados em relação à média, com divisor n − 1
        - **Maior Variação por Dispositivo**: identifica o sensor com maior amplitude (máx − mín) na janela selecionada

        O servidor transmite o resultado agregado e uma série de pontos para desenhar o histórico.

        **Métricas disponíveis por sensor:**
        | Sensor | Métricas |
        |--------|----------|
        | 🌡️ Estação Ambiental (C) | 13: clima, ar, vento, pressão, chuva, ruído, visibilidade e UV |
        | 💡 Poste Inteligente (Lua) | 8: luminosidade, potência, energia, tensão, corrente e estado do LED |
        | 🚦 Semáforo (Java) | 8: fase, fila, espera, velocidade, ocupação, ciclo, pedestres e tempo de verde |
        | 📹 Câmera de Tráfego (Python) | 8: fluxo, infrações, velocidade, ocupação, acidentes, pedestres, confiança e veículos pesados |
        | 🅿️ Estacionamento (Go) | 11: vagas, ocupação, rotatividade, chegadas/saídas, espera, receita e recarga elétrica |
        | 🚰 Água (Rust) | 8: nível, vazão, pressão, pH, turbidez, temperatura, condutividade e vazamento |
        | 🗑️ Lixeira (TypeScript) | 9: ocupação, peso, temperatura, umidade, bateria, sinal, coletas, inclinação e aquecimento |

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
            if st.session_state.get("tab4_selected_device_id") not in device_lookup:
                st.session_state.tab4_selected_device_id = next(iter(device_lookup))
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
                    df_inspec = build_history_frame(filtered_points)

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
                        st.line_chart(
                            build_history_chart(filtered_points),
                            use_container_width=True,
                            height=350,
                        )
                        st.caption("Horários em UTC; amostras do mesmo instante são agrupadas pela média.")

                    with col_table:
                        unit = METRIC_UNITS.get(ctx["metrica"], "")
                        value_label = (
                            f"Medição ({unit})" if unit else "Medição"
                        )
                        st.write("#### Registros do Sensor")
                        st.dataframe(
                            df_inspec[["Horário UTC", "value"]].rename(
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
