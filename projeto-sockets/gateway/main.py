import asyncio
import json
import logging
import math
import sqlite3
import time
import struct
import os
import socket
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Tuple

# Biblioteca assíncrona para I/O não-bloqueante no SQLite
import aiosqlite
import redis.asyncio as redis
from google.protobuf.message import DecodeError
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

def get_secret(name: str, default: str = "") -> str:
    """Lê um segredo de <NAME>_FILE (Docker secret) e cai para a env var <NAME>.

    Permite tirar segredos (chave AES, licença) do docker-compose em texto puro,
    apontando, por exemplo, AES_SECRET_KEY_FILE=/run/secrets/aes_secret_key.
    Mantém compatibilidade total: se *_FILE não existir, usa a env var de sempre.
    """
    file_path = os.getenv(name + "_FILE")
    if file_path:
        try:
            with open(file_path, "r", encoding="utf-8") as fh:
                return fh.read().strip()
        except OSError as exc:
            log_msg = f"[Config] Falha ao ler {name}_FILE='{file_path}': {exc}"
            print(log_msg)
    return os.getenv(name, default)


_raw_key = get_secret("AES_SECRET_KEY", "SmartCityKey1234").encode("utf-8")
AES_KEY = _raw_key.ljust(16, b'\x00')[:16]  # 16 bytes (AES-128)

def decrypt_payload(raw_payload: bytes) -> bytes:
    """Descriptografa o payload AES-128-GCM vindo do Redis."""
    if len(raw_payload) < 28: # 12 nonce + pelo menos 1 byte + 16 tag
        raise ValueError("Payload criptografado muito curto")
    nonce = raw_payload[:12]
    ciphertext = raw_payload[12:]
    aesgcm = AESGCM(AES_KEY)
    return aesgcm.decrypt(nonce, ciphertext, None)


def encrypt_control(plaintext: bytes) -> bytes:
    """Cifra um frame do canal de controle (nonce[12] || ciphertext+tag).

    Mesmo esquema do pipeline de telemetria; usado quando CONTROL_SECURE=1 para
    proteger o canal gateway↔sensor (confidencialidade + integridade via tag GCM).
    """
    nonce = os.urandom(12)
    return nonce + AESGCM(AES_KEY).encrypt(nonce, plaintext, None)


# Cifra/anti-replay do canal de controle (deve casar com o flag nos sensores).
CONTROL_SECURE = os.getenv("CONTROL_SECURE", "0").strip().lower() in ("1", "true", "yes", "on")

# ====================================================================
# [M5] LOGGING CONFIGURÁVEL VIA ENV VAR
#   LOG_LEVEL=DEBUG   → telemetria e probes (verboso)
#   LOG_LEVEL=INFO    → descoberta, comandos, eventos (padrão)
#   LOG_LEVEL=WARNING → apenas alertas e erros
# ====================================================================

_LOG_LEVEL_STR = os.getenv("LOG_LEVEL", "INFO").upper()
_LOG_LEVEL     = getattr(logging, _LOG_LEVEL_STR, logging.INFO)

logging.basicConfig(
    level=_LOG_LEVEL,
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("Gateway")

# ====================================================================
# CONFIGURAÇÕES DE BANCO DE DADOS
# ====================================================================

DB_DIR = "db"
if not os.path.exists(DB_DIR):
    os.makedirs(DB_DIR)

DB_FILE            = os.path.join(DB_DIR, "smartcity_gateway.db")
DB_POOL_SIZE       = max(1, int(os.getenv("DB_POOL_SIZE", "4")))
DB_BUSY_TIMEOUT_MS = 10000

# Intervalo do WAL checkpoint em segundos (padrão 5 min, mínimo 60 s)
WAL_CHECKPOINT_INTERVAL_SECS = max(60.0, float(os.getenv("WAL_CHECKPOINT_INTERVAL_SECS", "300")))

# [OLAP] Gestão de volume e aceleração de consultas
TELEMETRY_QUEUE_MAXSIZE = max(100, int(os.getenv("TELEMETRY_QUEUE_MAXSIZE", "10000")))
TELEMETRY_BATCH_MAX_PAYLOADS = max(1, int(os.getenv("TELEMETRY_BATCH_MAX_PAYLOADS", "100")))
TELEMETRY_BATCH_MAX_ROWS = max(1, int(os.getenv("TELEMETRY_BATCH_MAX_ROWS", "500")))
TELEMETRY_BATCH_FLUSH_INTERVAL_SECS = max(
    0.05,
    float(os.getenv("TELEMETRY_BATCH_FLUSH_INTERVAL_SECS", "1.0")),
)

METRICS_RAW_RETENTION_SECS = max(0, int(os.getenv("METRICS_RAW_RETENTION_SECS", str(7 * 24 * 3600))))
ROLLUP_1M_RETENTION_SECS = max(0, int(os.getenv("ROLLUP_1M_RETENTION_SECS", str(30 * 24 * 3600))))
ROLLUP_5M_RETENTION_SECS = max(0, int(os.getenv("ROLLUP_5M_RETENTION_SECS", str(180 * 24 * 3600))))
ROLLUP_1H_RETENTION_SECS = max(0, int(os.getenv("ROLLUP_1H_RETENTION_SECS", str(365 * 24 * 3600))))
METRICS_RETENTION_INTERVAL_SECS = max(60.0, float(os.getenv("METRICS_RETENTION_INTERVAL_SECS", "3600")))
ROLLUP_BACKFILL_ON_STARTUP = os.getenv("ROLLUP_BACKFILL_ON_STARTUP", "1").lower() not in {"0", "false", "no"}

OLAP_RAW_MAX_WINDOW_SECS = max(60, int(os.getenv("OLAP_RAW_MAX_WINDOW_SECS", "3600")))
OLAP_1M_MAX_WINDOW_SECS = max(OLAP_RAW_MAX_WINDOW_SECS, int(os.getenv("OLAP_1M_MAX_WINDOW_SECS", str(24 * 3600))))
OLAP_5M_MAX_WINDOW_SECS = max(OLAP_1M_MAX_WINDOW_SECS, int(os.getenv("OLAP_5M_MAX_WINDOW_SECS", str(7 * 24 * 3600))))

ROLLUP_TABLES = (
    ("metrics_rollup_1m", 60, ROLLUP_1M_RETENTION_SECS),
    ("metrics_rollup_5m", 300, ROLLUP_5M_RETENTION_SECS),
    ("metrics_rollup_1h", 3600, ROLLUP_1H_RETENTION_SECS),
)

DB_POOL = None
TELEMETRY_QUEUE: asyncio.Queue | None = None

# Cliente Redis persistente para publicação (alertas / automação) — Fase B.
REDIS_PUB = None

# [Fase E] Observabilidade — contadores internos + endpoint Prometheus.
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "9100"))
GW_METRICS_PUBLISH_SECS = max(2, int(os.getenv("GW_METRICS_PUBLISH_SECS", "10")))
_METRICS = {
    "telemetry_payloads_total": 0,
    "metrics_persisted_total": 0,
    "alerts_total": 0,
    "commands_total": 0,
    "commands_failed_total": 0,
}

# Referências fortes para tasks UDP criadas via asyncio.create_task.
# O event loop mantém apenas referências fracas; sem este set, o GC pode coletar
# a task antes de ela concluir, descartando telemetria/descoberta silenciosamente.
_BACKGROUND_TASKS: set[asyncio.Task] = set()

# Importa as classes do Protobuf geradas dinamicamente
import messages_pb2  # pyright: ignore[reportMissingImports]

# ====================================================================
# CONFIGURAÇÕES DE REDE
# ====================================================================

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

TCP_PORT           = 5001
AUTH_PORT          = 5007

GATEWAY_LICENSE_KEY = get_secret("GATEWAY_LICENSE_KEY", "SMARTCITY-V1-FULL-LICENSE")
MAX_AVAILABLE_PORTS = max(1, int(os.getenv("MAX_AVAILABLE_PORTS", "100")))
MIN_DYNAMIC_PORT = 6000

class InvalidLicenseKeyException(Exception): pass
class InvalidHexServiceCodeException(Exception): pass
class NoPortsAvailableException(Exception): pass

import random
_AVAILABLE_PORTS = set(range(MIN_DYNAMIC_PORT, MIN_DYNAMIC_PORT + MAX_AVAILABLE_PORTS))
_LAST_ASSIGNED_PORT = 0

# Conjunto de agregadores conhecidos. A porta dinâmica de um sensor é aberta em
# TODOS eles, não apenas no que repassou o discovery — assim, se o load-balancer
# do sensor migrar para outro agregador depois do auth, a telemetria continua
# sendo recebida (a porta já está aberta lá). Semeado por env e enriquecido em
# tempo de execução a partir do campo `aggregator` do discovery_stream.
_KNOWN_AGGREGATORS: set[str] = {
    agg.strip()
    for agg in os.getenv("KNOWN_AGGREGATORS", "rust_tokio_1,java_netty_1").split(",")
    if agg.strip()
}

# [Fase B+] Saúde de agregadores — última atividade (relay p/ Redis) por agregador.
# Detecta agregador que parou de repassar dados e gera alerta de SISTEMA.
AGG_HEALTH_TIMEOUT_SECS = max(5, int(os.getenv("AGG_HEALTH_TIMEOUT_SECS", "30")))
AGG_HEALTH_CHECK_SECS = max(2, int(os.getenv("AGG_HEALTH_CHECK_SECS", "10")))
_aggregator_last_seen: dict[str, int] = {}
_aggregator_down: set[str] = set()
_HEX_SERVICE_CODES = {
    messages_pb2.DEVICE_TYPE_TRAFFIC_LIGHT: "0A",
    messages_pb2.DEVICE_TYPE_LAMP_POST: "0B",
    messages_pb2.DEVICE_TYPE_WEATHER_STATION: "0C",
    messages_pb2.DEVICE_TYPE_CAMERA: "0D",
    messages_pb2.DEVICE_TYPE_FLOOD: "0E",
    messages_pb2.DEVICE_TYPE_NOISE: "0F",
}

def allocate_dynamic_port():
    global _LAST_ASSIGNED_PORT
    if not _AVAILABLE_PORTS:
        raise NoPortsAvailableException("Nao ha portas disponiveis no Gateway")
    candidates = list(_AVAILABLE_PORTS - {_LAST_ASSIGNED_PORT})
    if not candidates:
        if _LAST_ASSIGNED_PORT in _AVAILABLE_PORTS:
            candidates = [_LAST_ASSIGNED_PORT]
        else:
            raise NoPortsAvailableException("Nao ha portas disponiveis no Gateway")
    port = random.choice(candidates)
    _AVAILABLE_PORTS.remove(port)
    _LAST_ASSIGNED_PORT = port
    return port

# Timeout para leitura de cabeçalho e payload TCP do cliente (configurável)
TCP_CLIENT_READ_TIMEOUT = max(5.0, float(os.getenv("TCP_CLIENT_READ_TIMEOUT", "10")))
TCP_CLIENT_IDLE_TIMEOUT = max(
    TCP_CLIENT_READ_TIMEOUT,
    float(os.getenv("TCP_CLIENT_IDLE_TIMEOUT", "60")),
)
TCP_MAX_FRAME_BYTES = max(1024, int(os.getenv("TCP_MAX_FRAME_BYTES", str(1024 * 1024))))

MULTICAST_GROUP          = "239.0.0.1"
MULTICAST_PORT           = 5005
DISCOVERY_PROBE_PAYLOAD  = b"SMARTCITY_DISCOVERY_PROBE"
DISCOVERY_PROBE_INTERVAL_SECS = max(1.0, float(os.getenv("DISCOVERY_PROBE_INTERVAL_SECS", "15")))
MULTICAST_TTL            = max(1, int(os.getenv("MULTICAST_TTL", "1")))
DEVICE_OFFLINE_TIMEOUT_SECS = max(1.0, float(os.getenv("DEVICE_OFFLINE_TIMEOUT_SECS", "45")))
DEVICE_OFFLINE_CHECK_INTERVAL_SECS = max(1.0, float(os.getenv("DEVICE_OFFLINE_CHECK_INTERVAL_SECS", "5")))

# ====================================================================
# INICIALIZAÇÃO DE BANCO DE DADOS
# ====================================================================

@dataclass(slots=True)
class MetricSample:
    device_id: str
    timestamp: int
    metric_name: str
    value: float
    unit: str


@dataclass(slots=True)
class TelemetryEnvelope:
    device_id: str
    ip: str
    timestamp: int
    status: int
    metrics: list[MetricSample]


class SQLiteConnectionPool:
    """Pool simples para reutilizar conexões aiosqlite entre requisições."""

    def __init__(self, db_file: str, size: int, timeout: float = 10.0):
        self.db_file = db_file
        self.size    = size
        self.timeout = timeout
        self._queue       = asyncio.Queue(maxsize=size)
        self._connections = []
        self._started     = False

    async def start(self):
        if self._started:
            return

        for _ in range(self.size):
            db = await aiosqlite.connect(self.db_file, timeout=self.timeout)
            await db.execute(f"PRAGMA busy_timeout = {DB_BUSY_TIMEOUT_MS};")
            await db.execute("PRAGMA foreign_keys = ON;")
            await db.commit()
            self._connections.append(db)
            await self._queue.put(db)

        self._started = True
        log.info("Pool SQLite inicializado com %d conexões.", self.size)

    @asynccontextmanager
    async def connection(self):
        if not self._started:
            raise RuntimeError("Pool SQLite ainda não foi inicializado.")

        db = await self._queue.get()
        try:
            yield db
        except Exception:
           
            try:
                await db.rollback()
            except Exception as rollback_exc:
                log.warning("Rollback SQLite falhou (conexão possivelmente inválida): %s", rollback_exc)
            raise
        finally:
            self._queue.put_nowait(db)

    async def close(self):
        while not self._queue.empty():
            self._queue.get_nowait()

        for db in self._connections:
            await db.close()

        self._connections.clear()
        self._started = False
        log.info("Pool SQLite encerrado.")


def get_db_pool() -> SQLiteConnectionPool:
    if DB_POOL is None:
        raise RuntimeError("Pool SQLite não inicializado.")
    return DB_POOL


def get_telemetry_queue() -> asyncio.Queue:
    if TELEMETRY_QUEUE is None:
        raise RuntimeError("Fila de telemetria não inicializada.")
    return TELEMETRY_QUEUE


def ensure_metrics_index(cursor: sqlite3.Cursor):
    """Garante índices compatíveis com ingestão, retenção e consultas OLAP."""

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_metrics_metric_time_device
        ON metrics (metric_name, timestamp, device_id)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_metrics_device_metric_time
        ON metrics (device_id, metric_name, timestamp)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_metrics_timestamp
        ON metrics (timestamp)
    """)


def ensure_rollup_tables(cursor: sqlite3.Cursor):
    """Cria tabelas agregadas para reduzir varreduras OLAP em janelas grandes."""
    for table_name, _, _ in ROLLUP_TABLES:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                bucket_start INTEGER NOT NULL,
                device_id    TEXT    NOT NULL,
                metric_name  TEXT    NOT NULL,
                unit         TEXT,
                sample_count INTEGER NOT NULL,
                value_sum    REAL    NOT NULL,
                value_sum_sq REAL    NOT NULL,
                value_min    REAL    NOT NULL,
                value_max    REAL    NOT NULL,
                PRIMARY KEY (bucket_start, device_id, metric_name)
            )
        """)
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{table_name}_metric_bucket_device
            ON {table_name} (metric_name, bucket_start, device_id)
        """)
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{table_name}_device_metric_bucket
            ON {table_name} (device_id, metric_name, bucket_start)
        """)


def backfill_rollup_tables(cursor: sqlite3.Cursor):
    """Popula rollups a partir de métricas brutas já existentes no banco."""
    if not ROLLUP_BACKFILL_ON_STARTUP:
        return

    for table_name, bucket_size, _ in ROLLUP_TABLES:
        cursor.execute(f"""
            INSERT OR IGNORE INTO {table_name}
            (bucket_start, device_id, metric_name, unit, sample_count,
             value_sum, value_sum_sq, value_min, value_max)
            SELECT
                CAST(timestamp / ? AS INTEGER) * ? AS bucket_start,
                device_id,
                metric_name,
                MAX(unit) AS unit,
                COUNT(*) AS sample_count,
                SUM(value) AS value_sum,
                SUM(value * value) AS value_sum_sq,
                MIN(value) AS value_min,
                MAX(value) AS value_max
            FROM metrics
            WHERE timestamp IS NOT NULL
              AND metric_name IS NOT NULL
              AND device_id IS NOT NULL
            GROUP BY bucket_start, device_id, metric_name
        """, (bucket_size, bucket_size))


def init_db():
    """Inicialização síncrona executada apenas no boot do Gateway."""
    conn   = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Ativa o modo WAL (Write-Ahead Logging) para otimizar concorrência de leitura/escrita
    cursor.execute("PRAGMA journal_mode=WAL;")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS devices (
            device_id    TEXT PRIMARY KEY,
            type         INTEGER,
            status       INTEGER,
            ip_address   TEXT,
            control_port INTEGER,
            is_controllable INTEGER,
            last_seen    INTEGER,
            aggregator_id TEXT,
            coord_x      INTEGER,
            coord_y      INTEGER
        )
    """)
    # Migration if table already exists
    try:
        cursor.execute("ALTER TABLE devices ADD COLUMN aggregator_id TEXT")
    except sqlite3.OperationalError:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE devices ADD COLUMN coord_x INTEGER")
        cursor.execute("ALTER TABLE devices ADD COLUMN coord_y INTEGER")
    except sqlite3.OperationalError:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE devices ADD COLUMN telemetry_port INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass  # Column already exists

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_devices_last_seen
        ON devices (last_seen)
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS metrics (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id   TEXT,
            timestamp   INTEGER,
            metric_name TEXT,
            value       REAL,
            unit        TEXT
        )
    """)
    # [Fase B+] Histórico durável de alertas (além do stream Redis efêmero).
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS alerts_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp   INTEGER,
            device_id   TEXT,
            metric      TEXT,
            value       REAL,
            threshold   REAL,
            op          TEXT,
            severity    TEXT,
            message     TEXT
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_alerts_time ON alerts_history (timestamp)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_alerts_device ON alerts_history (device_id, timestamp)
    """)

    ensure_metrics_index(cursor)
    ensure_rollup_tables(cursor)
    backfill_rollup_tables(cursor)
    conn.commit()
    conn.close()

# ====================================================================
# WORKERS ASSÍNCRONOS DE PERSISTÊNCIA
# ====================================================================

def build_telemetry_envelope(payload: messages_pb2.DataPayload, ip: str) -> TelemetryEnvelope:
    return TelemetryEnvelope(
        device_id=payload.device_id,
        ip=ip,
        timestamp=int(payload.timestamp),
        status=int(payload.current_status),
        metrics=[
            MetricSample(
                device_id=payload.device_id,
                timestamp=int(payload.timestamp),
                metric_name=m.name,
                value=float(m.value),
                unit=m.unit,
            )
            for m in payload.metrics
        ],
    )


def build_rollup_rows(metric_rows: list[tuple[str, int, str, float, str]]):
    rollup_rows: dict[str, list[tuple[int, str, str, str, int, float, float, float, float]]] = {}

    for table_name, bucket_size, _ in ROLLUP_TABLES:
        aggregated: dict[tuple[int, str, str], dict[str, float | int | str]] = {}

        for device_id, timestamp, metric_name, value, unit in metric_rows:
            bucket_start = (int(timestamp) // bucket_size) * bucket_size
            key = (bucket_start, device_id, metric_name)
            current = aggregated.get(key)

            if current is None:
                aggregated[key] = {
                    "unit": unit,
                    "sample_count": 1,
                    "value_sum": value,
                    "value_sum_sq": value * value,
                    "value_min": value,
                    "value_max": value,
                }
            else:
                current["unit"] = unit or current["unit"]
                current["sample_count"] = int(current["sample_count"]) + 1
                current["value_sum"] = float(current["value_sum"]) + value
                current["value_sum_sq"] = float(current["value_sum_sq"]) + (value * value)
                current["value_min"] = min(float(current["value_min"]), value)
                current["value_max"] = max(float(current["value_max"]), value)

        rollup_rows[table_name] = [
            (
                bucket_start,
                device_id,
                metric_name,
                str(values["unit"]),
                int(values["sample_count"]),
                float(values["value_sum"]),
                float(values["value_sum_sq"]),
                float(values["value_min"]),
                float(values["value_max"]),
            )
            for (bucket_start, device_id, metric_name), values in aggregated.items()
        ]

    return rollup_rows


async def persist_telemetry_batch(batch: list[TelemetryEnvelope]):
    if not batch:
        return

    now = int(time.time())
    device_updates = {
        # control_port=0 incluído explicitamente para evitar NULL quando
        # a telemetria chega antes da mensagem de descoberta (race condition de rede).
        # INSERT OR IGNORE garante que uma descoberta posterior sobrescreva via
        # process_discovery (ON CONFLICT DO UPDATE com o valor real da porta).
        envelope.device_id: (envelope.device_id, 0, envelope.status, envelope.ip, 0, 0, now)
        for envelope in batch
    }
    device_update_rows = [(now, row[2], row[0]) for row in device_updates.values()]
    metric_rows = [
        (
            sample.device_id,
            sample.timestamp,
            sample.metric_name,
            sample.value,
            sample.unit,
        )
        for envelope in batch
        for sample in envelope.metrics
    ]

    async with get_db_pool().connection() as db:
        await db.executemany("""
            INSERT OR IGNORE INTO devices (device_id, type, status, ip_address, control_port, is_controllable, last_seen)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, list(device_updates.values()))

        await db.executemany("""
            UPDATE devices SET last_seen = ?, status = ? WHERE device_id = ?
        """, device_update_rows)

        if metric_rows:
            await db.executemany("""
                INSERT INTO metrics (device_id, timestamp, metric_name, value, unit)
                VALUES (?, ?, ?, ?, ?)
            """, metric_rows)

            for table_name, rows in build_rollup_rows(metric_rows).items():
                if not rows:
                    continue
                await db.executemany(f"""
                    INSERT INTO {table_name}
                    (bucket_start, device_id, metric_name, unit, sample_count,
                     value_sum, value_sum_sq, value_min, value_max)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(bucket_start, device_id, metric_name) DO UPDATE SET
                        unit         = excluded.unit,
                        sample_count = {table_name}.sample_count + excluded.sample_count,
                        value_sum    = {table_name}.value_sum + excluded.value_sum,
                        value_sum_sq = {table_name}.value_sum_sq + excluded.value_sum_sq,
                        value_min    = MIN({table_name}.value_min, excluded.value_min),
                        value_max    = MAX({table_name}.value_max, excluded.value_max)
                """, rows)

        await db.commit()

    _METRICS["telemetry_payloads_total"] += len(batch)
    _METRICS["metrics_persisted_total"] += len(metric_rows)

    log.debug(
        "Batch de telemetria persistido: %d payload(s), %d métrica(s).",
        len(batch), len(metric_rows),
    )


async def process_telemetry(payload: messages_pb2.DataPayload, ip: str):
    """Enfileira telemetria para persistência em lote sem travar o Event Loop."""
    envelope = build_telemetry_envelope(payload, ip)
    await get_telemetry_queue().put(envelope)


# ====================================================================
# [FASE B] ALERTAS EM TEMPO REAL
#   Avalia cada métrica contra limiares; ao romper (com cooldown por
#   device+métrica), publica no stream Redis 'alerts' e dispara webhook.
# ====================================================================

ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "").strip()
ALERT_COOLDOWN_SECS = max(1, int(os.getenv("ALERT_COOLDOWN_SECS", "60")))

# (métrica, operador, limiar, severidade)
ALERT_THRESHOLDS = (
    ("temperature",       ">=",   32.0, "warning"),
    ("pm25",              ">=",   35.0, "warning"),
    ("aqi",               ">=",  100.0, "warning"),
    ("co2",               ">=", 1200.0, "warning"),
    ("queue_length",      ">=",   35.0, "warning"),
    ("power_consumption", ">=",   32.0, "warning"),
    ("luminosity",        "<=",   80.0, "info"),
    ("vehicles_count",    ">=",   80.0, "warning"),
    ("infractions",       ">=",    3.0, "critical"),
    ("water_level",       ">=",  150.0, "critical"),   # enchente (cm)
    ("noise_db",          ">=",   85.0, "warning"),    # ruído (dB)
)

_alert_last_fired: dict[tuple[str, str], int] = {}

# Silenciamentos ativos: "device_id|metric" -> epoch de expiração.
# Recarregados do hash Redis 'alert_silences' (editável pela UI / ack de alertas).
ALERT_SILENCES: dict[str, int] = {}


def _threshold_breached(op: str, value: float, threshold: float) -> bool:
    if op == ">=":
        return value >= threshold
    if op == "<=":
        return value <= threshold
    if op == ">":
        return value > threshold
    if op == "<":
        return value < threshold
    return False


def _post_webhook_blocking(url: str, payload: dict):
    import urllib.request
    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5).close()
    except Exception as exc:  # noqa: BLE001 — webhook é best-effort
        log.debug("Webhook de alerta falhou: %s", exc)


def dispatch_alert_webhook(alert: dict):
    if not ALERT_WEBHOOK_URL:
        return
    text = f"[{alert['severity'].upper()}] {alert['device_id']}: {alert['message']}"
    # 'content' (Discord) e 'text' (Slack) p/ compatibilidade ampla.
    payload = {"content": text, "text": text, **alert}
    asyncio.get_running_loop().run_in_executor(None, _post_webhook_blocking, ALERT_WEBHOOK_URL, payload)


async def _publish_alert(alert: dict):
    """Publica um alerta no stream Redis e dispara o webhook (best-effort)."""
    if REDIS_PUB is not None:
        try:
            await REDIS_PUB.xadd("alerts", alert, maxlen=5000, approximate=True)
        except Exception as exc:
            log.debug("Falha ao publicar alerta no Redis: %s", exc)
    dispatch_alert_webhook(alert)


async def insert_alert_history(alerts: list[dict]):
    """Persiste alertas na tabela durável alerts_history (para análise futura)."""
    if not alerts:
        return
    rows = []
    for a in alerts:
        try:
            rows.append((
                int(a.get("ts", 0) or 0), a.get("device_id", ""), a.get("metric", ""),
                float(a.get("value", 0) or 0), float(a.get("threshold", 0) or 0),
                a.get("op", ""), a.get("severity", ""), a.get("message", ""),
            ))
        except (TypeError, ValueError):
            continue
    try:
        async with get_db_pool().connection() as db:
            await db.executemany(
                "INSERT INTO alerts_history "
                "(timestamp, device_id, metric, value, threshold, op, severity, message) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows,
            )
            await db.commit()
    except Exception as exc:
        log.debug("Falha ao gravar histórico de alertas: %s", exc)


def _is_silenced(device_id: str, metric: str, now: int) -> bool:
    return ALERT_SILENCES.get(f"{device_id}|{metric}", 0) > now


async def evaluate_alerts(batch: list[TelemetryEnvelope]):
    """Verifica limiares de um batch e publica alertas (stream + webhook + histórico)."""
    if REDIS_PUB is None:
        return
    now = int(time.time())
    fired: list[dict] = []
    for envelope in batch:
        for sample in envelope.metrics:
            for metric, op, threshold, severity in ALERT_THRESHOLDS:
                if sample.metric_name != metric:
                    continue
                if not _threshold_breached(op, sample.value, threshold):
                    continue
                if _is_silenced(sample.device_id, metric, now):
                    continue
                key = (sample.device_id, metric)
                if now - _alert_last_fired.get(key, 0) < ALERT_COOLDOWN_SECS:
                    continue
                _alert_last_fired[key] = now
                alert = {
                    "ts": str(now),
                    "device_id": sample.device_id,
                    "metric": metric,
                    "value": f"{sample.value:.2f}",
                    "threshold": str(threshold),
                    "op": op,
                    "severity": severity,
                    "message": f"{metric}={sample.value:.1f} {op} {threshold:g}",
                }
                await _publish_alert(alert)
                fired.append(alert)
                log.info("ALERTA [%s] %s: %s", severity, sample.device_id, alert["message"])
    _METRICS["alerts_total"] += len(fired)
    await insert_alert_history(fired)


async def telemetry_batch_worker_loop():
    """Consome a fila UDP e grava métricas brutas + rollups em batches."""
    queue = get_telemetry_queue()

    while True:
        batch: list[TelemetryEnvelope] = []
        try:
            first = await queue.get()
            batch.append(first)
            metric_rows = len(first.metrics)
            deadline = asyncio.get_running_loop().time() + TELEMETRY_BATCH_FLUSH_INTERVAL_SECS

            while (
                len(batch) < TELEMETRY_BATCH_MAX_PAYLOADS
                and metric_rows < TELEMETRY_BATCH_MAX_ROWS
            ):
                timeout = deadline - asyncio.get_running_loop().time()
                if timeout <= 0:
                    break

                try:
                    item = await asyncio.wait_for(queue.get(), timeout=timeout)
                except asyncio.TimeoutError:
                    break

                batch.append(item)
                metric_rows += len(item.metrics)

            await persist_telemetry_batch(batch)
            await evaluate_alerts(batch)
            await evaluate_automation(batch)
        except asyncio.CancelledError:
            if batch:
                await persist_telemetry_batch(batch)
                for _ in batch:
                    queue.task_done()
            raise
        except Exception as exc:
            log.warning("Falha no worker de batch de telemetria: %s", exc)
            for _ in batch:
                queue.task_done()
        else:
            for _ in batch:
                queue.task_done()


async def process_discovery(disc: messages_pb2.DiscoveryResponse, ip: str):
    """Registra ou renova a presença de nós operacionais assincronamente."""
    now = int(time.time())

    announced = disc.ip_address.strip() if disc.ip_address else ""
    effective_ip = announced if announced else ip

    async with get_db_pool().connection() as db:
        async with db.execute(
            "SELECT 1 FROM devices WHERE device_id = ?",
            (disc.device_id,),
        ) as cursor:
            existing_device = await cursor.fetchone()

        await db.execute("""
            INSERT INTO devices
            (device_id, type, status, ip_address, control_port, is_controllable, last_seen, aggregator_id, coord_x, coord_y)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(device_id) DO UPDATE SET
                type            = excluded.type,
                status          = excluded.status,
                ip_address      = excluded.ip_address,
                control_port    = excluded.control_port,
                is_controllable = excluded.is_controllable,
                last_seen       = excluded.last_seen,
                aggregator_id   = excluded.aggregator_id,
                coord_x         = excluded.coord_x,
                coord_y         = excluded.coord_y
        """, (disc.device_id, disc.type, disc.initial_status, effective_ip,
              disc.control_port, int(disc.is_controllable), now, disc.aggregator_id, disc.coord_x, disc.coord_y))
        await db.commit()

    if existing_device is None:
        log.info(
            "Nó registrado: '%s' — rede=%s anunciado='%s' porta=%d (controlável=%s).",
            disc.device_id, ip, announced or "<não informado>",
            disc.control_port, disc.is_controllable,
        )
    else:
        log.debug(
            "Heartbeat/topologia renovado: '%s' — rede=%s anunciado='%s' porta=%d.",
            disc.device_id, ip, announced or "<não informado>", disc.control_port,
        )


async def redis_telemetry_loop():
    """Consome a stream de telemetria do Redis de forma contínua."""
    log.info("Iniciando consumo de telemetria via Redis Stream...")
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    # "$" = apenas mensagens novas. Evita reprocessar todo o histórico a cada
    # reinício do Gateway (o que duplicaria métricas, pois o INSERT em `metrics`
    # não é idempotente e os rollups somam via ON CONFLICT).
    last_id = "$"
    try:
        while True:
            try:
                events = await client.xread({"telemetry_stream": last_id}, count=100, block=1000)
                for stream, messages in events:
                    for msg_id, data in messages:
                        last_id = msg_id
                        raw_payload = data[b'payload']
                        agg = data.get(b'aggregator', b'').decode('utf-8')
                        if agg:
                            _aggregator_last_seen[agg] = int(time.time())
                            _KNOWN_AGGREGATORS.add(agg)
                        try:
                            decrypted = decrypt_payload(raw_payload)
                            payload = messages_pb2.DataPayload()
                            payload.ParseFromString(decrypted)
                            task = asyncio.create_task(process_telemetry(payload, "0.0.0.0"))
                            _BACKGROUND_TASKS.add(task)
                            task.add_done_callback(_BACKGROUND_TASKS.discard)
                        except Exception as e:
                            log.debug("Datagrama de telemetria inválido do Redis: %s", e)
            except Exception as e:
                log.error("Erro ao ler telemetry_stream: %s", e)
                await asyncio.sleep(2)
    except asyncio.CancelledError:
        await client.close()
        raise


async def redis_discovery_loop():
    """Consome a stream de descoberta do Redis."""
    log.info("Iniciando consumo de discovery via Redis Stream...")
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    # "$" = apenas mensagens novas. Reprocessar descobertas antigas no restart
    # marcaria nós já desligados como recém-vistos (last_seen = agora).
    last_id = "$"
    try:
        while True:
            try:
                events = await client.xread({"discovery_stream": last_id}, count=100, block=1000)
                for stream, messages in events:
                    for msg_id, data in messages:
                        last_id = msg_id
                        raw_payload = data[b'payload']
                        aggregator_id = data.get(b'aggregator', b'').decode('utf-8')
                        try:
                            decrypted = decrypt_payload(raw_payload)
                            disc = messages_pb2.DiscoveryResponse()
                            disc.ParseFromString(decrypted)
                            if aggregator_id:
                                disc.aggregator_id = aggregator_id
                                _KNOWN_AGGREGATORS.add(aggregator_id)
                                _aggregator_last_seen[aggregator_id] = int(time.time())
                            if disc.device_id:
                                task = asyncio.create_task(process_discovery(disc, "0.0.0.0"))
                                _BACKGROUND_TASKS.add(task)
                                task.add_done_callback(_BACKGROUND_TASKS.discard)
                        except Exception as e:
                            log.debug("Datagrama de descoberta inválido do Redis: %s", e)
            except Exception as e:
                log.error("Erro ao ler discovery_stream: %s", e)
                await asyncio.sleep(2)
    except asyncio.CancelledError:
        await client.close()
        raise


async def multicast_discovery_probe_loop():
    """Solicita periodicamente que sensores reanunciem a própria topologia."""
    await asyncio.sleep(2.0)

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, MULTICAST_TTL)

            while True:
                try:
                    probe = messages_pb2.AggregatorLoad()
                    probe.aggregator_id = "GATEWAY_PROBE"
                    sock.sendto(probe.SerializeToString(), (MULTICAST_GROUP, MULTICAST_PORT))
                    log.debug(
                        "Probe de descoberta enviado para %s:%d.",
                        MULTICAST_GROUP, MULTICAST_PORT,
                    )
                except OSError as exc:
                    log.warning("Falha ao enviar probe de descoberta: %s", exc)

                await asyncio.sleep(DISCOVERY_PROBE_INTERVAL_SECS)
    except asyncio.CancelledError:
        raise
    except OSError as exc:
        log.error("Loop de probes indisponível: %s", exc)


# ====================================================================
# [M3] MANUTENÇÃO DO BANCO: WAL CHECKPOINT PERIÓDICO
# ====================================================================

async def wal_checkpoint_loop():
    """Emite PRAGMA wal_checkpoint(PASSIVE) periodicamente para limitar crescimento do WAL."""
    await asyncio.sleep(WAL_CHECKPOINT_INTERVAL_SECS)

    while True:
        try:
            async with get_db_pool().connection() as db:
                await db.execute("PRAGMA wal_checkpoint(PASSIVE);")
                await db.commit()
            log.debug("WAL checkpoint executado (intervalo=%ds).", WAL_CHECKPOINT_INTERVAL_SECS)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("Falha no WAL checkpoint: %s", exc)

        await asyncio.sleep(WAL_CHECKPOINT_INTERVAL_SECS)


async def device_offline_monitor_loop():
    """Marca dispositivos como offline quando deixam de renovar last_seen."""
    await asyncio.sleep(DEVICE_OFFLINE_CHECK_INTERVAL_SECS)

    while True:
        try:
            cutoff = int(time.time() - DEVICE_OFFLINE_TIMEOUT_SECS)
            # rowcount capturado dentro do bloco async with, enquanto a
            # conexão ainda está checada para uso. Acessá-lo após o bloco é frágil
            # pois a conexão pode ser reutilizada por outra corrotina.
            marked_offline = 0
            offline_devices = []
            async with get_db_pool().connection() as db:
                cursor = await db.execute("""
                    SELECT aggregator_id, telemetry_port FROM devices
                    WHERE last_seen < ? AND status != ? AND telemetry_port > 0
                """, (cutoff, messages_pb2.STATUS_OFF))
                offline_devices = await cursor.fetchall()
                
                cursor = await db.execute("""
                    UPDATE devices
                    SET status = ?, telemetry_port = 0
                    WHERE last_seen < ? AND status != ?
                """, (messages_pb2.STATUS_OFF, cutoff, messages_pb2.STATUS_OFF))
                await db.commit()
                marked_offline = cursor.rowcount

            if offline_devices:
                try:
                    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
                    for agg_id, port in offline_devices:
                        if MIN_DYNAMIC_PORT <= port < (MIN_DYNAMIC_PORT + MAX_AVAILABLE_PORTS):
                            _AVAILABLE_PORTS.add(port)
                            # A porta foi aberta em todos os agregadores no auth;
                            # fecha em todos eles (∪ o do device) para não vazar sockets.
                            for agg in set(_KNOWN_AGGREGATORS) | ({agg_id} if agg_id else set()):
                                await client.lpush(f"agg_control_{agg}", f"FECHAR_PORTA_UDP: {port}")
                    await client.close()
                    log.info("Liberadas %d porta(s) UDP de dispositivos offline.", len(offline_devices))
                except Exception as e:
                    log.error("Erro ao publicar FECHAR_PORTA_UDP no Redis: %s", e)

            if marked_offline > 0:
                log.warning(
                    "%d dispositivo(s) sem heartbeat por mais de %.1fs marcados como offline.",
                    marked_offline, DEVICE_OFFLINE_TIMEOUT_SECS,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("Falha no monitor de presença dos dispositivos: %s", exc)

        await asyncio.sleep(DEVICE_OFFLINE_CHECK_INTERVAL_SECS)


async def aggregator_health_loop():
    """Auto-alerta de saúde: emite alerta de SISTEMA quando um agregador para de
    repassar dados ao Redis (e um alerta de recuperação quando volta).

    Sinal baseado na atividade de relay (precursor do ACK two-way da Fase E);
    um agregador up porém ocioso pode aparecer como inativo — por isso só alerta
    em transições e com timeout generoso.
    """
    await asyncio.sleep(AGG_HEALTH_CHECK_SECS)
    while True:
        try:
            now = int(time.time())
            for agg in sorted(_KNOWN_AGGREGATORS):
                last = _aggregator_last_seen.get(agg, 0)
                if last == 0:
                    continue  # nunca repassou nada ainda — não alarmar no boot
                silent_for = now - last
                if silent_for > AGG_HEALTH_TIMEOUT_SECS and agg not in _aggregator_down:
                    _aggregator_down.add(agg)
                    alert = {
                        "ts": str(now), "device_id": f"agg:{agg}", "metric": "aggregator_health",
                        "value": str(silent_for), "threshold": str(AGG_HEALTH_TIMEOUT_SECS),
                        "op": ">", "severity": "critical",
                        "message": f"Agregador '{agg}' sem repassar dados há {silent_for}s.",
                    }
                    await _publish_alert(alert)
                    await insert_alert_history([alert])
                    log.warning("SAÚDE: agregador '%s' inativo (%ds sem relay).", agg, silent_for)
                elif silent_for <= AGG_HEALTH_TIMEOUT_SECS and agg in _aggregator_down:
                    _aggregator_down.discard(agg)
                    alert = {
                        "ts": str(now), "device_id": f"agg:{agg}", "metric": "aggregator_health",
                        "value": "0", "threshold": str(AGG_HEALTH_TIMEOUT_SECS),
                        "op": "<=", "severity": "info",
                        "message": f"Agregador '{agg}' voltou a repassar dados.",
                    }
                    await _publish_alert(alert)
                    await insert_alert_history([alert])
                    log.info("SAÚDE: agregador '%s' recuperado.", agg)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.debug("Falha no monitor de saúde de agregadores: %s", exc)

        await asyncio.sleep(AGG_HEALTH_CHECK_SECS)


async def metrics_retention_loop():
    """Remove dados antigos conforme políticas de retenção configuradas."""
    await asyncio.sleep(METRICS_RETENTION_INTERVAL_SECS)

    while True:
        try:
            now = int(time.time())
            deleted_parts: list[str] = []

            async with get_db_pool().connection() as db:
                if METRICS_RAW_RETENTION_SECS > 0:
                    cursor = await db.execute(
                        "DELETE FROM metrics WHERE timestamp < ?",
                        (now - METRICS_RAW_RETENTION_SECS,),
                    )
                    deleted_parts.append(f"raw={cursor.rowcount}")

                for table_name, _, retention_secs in ROLLUP_TABLES:
                    if retention_secs <= 0:
                        continue
                    cursor = await db.execute(
                        f"DELETE FROM {table_name} WHERE bucket_start < ?",
                        (now - retention_secs,),
                    )
                    deleted_parts.append(f"{table_name}={cursor.rowcount}")

                await db.commit()

            log.debug("Retenção de métricas executada (%s).", ", ".join(deleted_parts) or "sem limites")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("Falha na retenção de métricas: %s", exc)

        await asyncio.sleep(METRICS_RETENTION_INTERVAL_SECS)


# ====================================================================
# CAMADA DE REDE: ATENDIMENTO AO CLIENTE (TCP COM FRAMING)
# ====================================================================

def new_client_response(success: bool = True) -> messages_pb2.ClientResponse:
    resp = messages_pb2.ClientResponse(success=success)
    resp.message_id = f"GW-RESP-{uuid.uuid4().hex[:8]}"
    resp.timestamp  = int(time.time())
    return resp

async def handle_auth_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peer = writer.get_extra_info('peername')
    log.info("Nova conexao de Autenticacao de %s", peer)
    try:
        raw_len = await asyncio.wait_for(reader.readexactly(4), timeout=5.0)
        msg_len = struct.unpack("!I", raw_len)[0]
        payload = await asyncio.wait_for(reader.readexactly(msg_len), timeout=5.0)
        
        req = messages_pb2.AuthRequest()
        req.ParseFromString(payload)
        
        expected_hex = _HEX_SERVICE_CODES.get(req.type)
        if not expected_hex or expected_hex != req.hex_service_code:
            raise InvalidHexServiceCodeException(f"Hex invalido: esperado {expected_hex}, recebido {req.hex_service_code}")
            
        if f"SMARTCITY-{req.license_key_part}-LICENSE" != GATEWAY_LICENSE_KEY:
            raise InvalidLicenseKeyException(f"Chave de licenca invalida")
            
        aggregator_id = None
        existing_port = 0
        for _ in range(3):
            async with get_db_pool().connection() as db:
                async with db.execute(
                    "SELECT aggregator_id, telemetry_port FROM devices WHERE device_id = ?",
                    (req.device_id,),
                ) as cursor:
                    row = await cursor.fetchone()
                    if row and row[0]:
                        aggregator_id = row[0]
                        existing_port = int(row[1] or 0)
                        break
            await asyncio.sleep(1.0)

        if not aggregator_id:
            raise Exception("Dispositivo nao encontrado via Discovery ainda. Agregador desconhecido.")

        # Reutiliza a porta já atribuída a este dispositivo em uma autenticação
        # anterior (ex.: sensor reiniciou). Sem isso, cada re-auth alocava uma
        # nova porta e a antiga vazava permanentemente de _AVAILABLE_PORTS
        # (a porta antiga sumia da coluna telemetry_port e nunca era reciclada),
        # esgotando o pool após sucessivos reinícios.
        if (
            MIN_DYNAMIC_PORT <= existing_port < (MIN_DYNAMIC_PORT + MAX_AVAILABLE_PORTS)
            and existing_port not in _AVAILABLE_PORTS
        ):
            port = existing_port
        else:
            port = allocate_dynamic_port()

        async with get_db_pool().connection() as db:
            await db.execute("UPDATE devices SET telemetry_port = ? WHERE device_id = ?", (port, req.device_id))
            await db.commit()
        
        # Abre a porta em todos os agregadores conhecidos (inclui o do device),
        # tornando a telemetria resiliente a trocas de rota do load-balancer.
        target_aggregators = set(_KNOWN_AGGREGATORS) | {aggregator_id}
        client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        for agg in target_aggregators:
            await client.lpush(f"agg_control_{agg}", f"ABRIR_PORTA_UDP: {port}")
        await client.close()

        resp = messages_pb2.AuthResponse()
        resp.success = True
        resp.message = "OK"
        resp.assigned_port = port
        
        resp_payload = resp.SerializeToString()
        writer.write(struct.pack("!I", len(resp_payload)))
        writer.write(resp_payload)
        await writer.drain()
        
        log.info("Auth SUCCESS: %s porta alocada %d no agregador %s", req.device_id, port, aggregator_id)
        
    except Exception as e:
        log.error("Auth FAIL para %s: %s", peer, e)
        resp = messages_pb2.AuthResponse()
        resp.success = False
        resp.message = str(e)
        resp.assigned_port = 0
        resp_payload = resp.SerializeToString()
        writer.write(struct.pack("!I", len(resp_payload)))
        writer.write(resp_payload)
        try:
            await writer.drain()
        except:
            pass
    finally:
        writer.close()
        await writer.wait_closed()



@dataclass(slots=True)
class OlapSource:
    name: str
    table_name: str
    bucket_size: int
    is_rollup: bool


@dataclass(slots=True)
class OlapQueryResult:
    success: bool
    message: str
    analytics_result: float
    result_metadata: str
    graph_rows: list[tuple[int, float, str]]
    sample_count: int


def choose_olap_source(start_timestamp: int, end_timestamp: int) -> OlapSource:
    window_secs = max(0, end_timestamp - start_timestamp)
    if window_secs <= OLAP_RAW_MAX_WINDOW_SECS:
        return OlapSource("raw", "metrics", 0, False)
    if window_secs <= OLAP_1M_MAX_WINDOW_SECS:
        return OlapSource("rollup_1m", "metrics_rollup_1m", 60, True)
    if window_secs <= OLAP_5M_MAX_WINDOW_SECS:
        return OlapSource("rollup_5m", "metrics_rollup_5m", 300, True)
    return OlapSource("rollup_1h", "metrics_rollup_1h", 3600, True)


def olap_time_range(source: OlapSource, start_timestamp: int, end_timestamp: int) -> tuple[int, int]:
    if not source.is_rollup:
        return start_timestamp, end_timestamp

    return (
        (start_timestamp // source.bucket_size) * source.bucket_size,
        (end_timestamp // source.bucket_size) * source.bucket_size,
    )


def sample_stddev(sample_count: int, value_sum: float, value_sum_sq: float) -> float:
    if sample_count <= 1:
        return 0.0

    variance = (value_sum_sq - ((value_sum * value_sum) / sample_count)) / (sample_count - 1)
    return math.sqrt(max(0.0, variance))


async def fetch_graph_rows(
    db,
    source: OlapSource,
    metric_name: str,
    start_timestamp: int,
    end_timestamp: int,
    target_device_id: str = "",
) -> list[tuple[int, float, str]]:
    # Query construída dinamicamente com base em (is_rollup × target_device_id).
    query_start, query_end = olap_time_range(source, start_timestamp, end_timestamp)

    params: list = [metric_name, query_start, query_end]
    device_filter = ""
    if target_device_id:
        device_filter = "AND device_id = ?"
        params.append(target_device_id)

    if source.is_rollup:
        query = f"""
            SELECT bucket_start,
                   value_sum / sample_count AS bucket_avg,
                   device_id
            FROM {source.table_name}
            WHERE metric_name = ? AND bucket_start BETWEEN ? AND ? {device_filter}
            ORDER BY bucket_start ASC, device_id ASC
        """
    else:
        query = f"""
            SELECT timestamp, value, device_id
            FROM metrics
            WHERE metric_name = ? AND timestamp BETWEEN ? AND ? {device_filter}
            ORDER BY timestamp ASC
        """

    async with db.execute(query, params) as cursor:
        return [
            (int(ts), float(value), str(device_id))
            for ts, value, device_id in await cursor.fetchall()
        ]


async def execute_olap_from_source(
    db,
    req: messages_pb2.ClientRequest,
    source: OlapSource,
) -> OlapQueryResult:
    # Queries construídas dinamicamente com base em (is_rollup × target_device_id).
    query_start, query_end = olap_time_range(source, req.start_timestamp, req.end_timestamp)

    # Fragmentos que diferem entre rollup e raw
    if source.is_rollup:
        time_col    = "bucket_start"
        count_col   = "SUM(sample_count)"
        sum_col     = "SUM(value_sum)"
        sumsq_col   = "SUM(value_sum_sq)"
        min_col     = "MIN(value_min)"
        max_col     = "MAX(value_max)"
    else:
        time_col    = "timestamp"
        count_col   = "COUNT(*)"
        sum_col     = "COALESCE(SUM(value), 0.0)"
        sumsq_col   = "COALESCE(SUM(value * value), 0.0)"
        min_col     = "MIN(value)"
        max_col     = "MAX(value)"

    # Filtro de dispositivo opcional
    params_base: list = [req.query_metric, query_start, query_end]
    device_filter = ""
    if req.target_device_id:
        device_filter = "AND device_id = ?"
        params_base.append(req.target_device_id)

    if req.query_op in (messages_pb2.OP_AVERAGE, messages_pb2.OP_STD_DEV):
        async with db.execute(f"""
            SELECT COALESCE({count_col}, 0),
                   COALESCE({sum_col}, 0.0),
                   COALESCE({sumsq_col}, 0.0)
            FROM {source.table_name}
            WHERE metric_name = ? AND {time_col} BETWEEN ? AND ? {device_filter}
        """, params_base) as cursor:
            row = await cursor.fetchone()

        sample_count = int(row[0] or 0)
        value_sum    = float(row[1] or 0.0)
        value_sum_sq = float(row[2] or 0.0)

        if sample_count == 0:
            return OlapQueryResult(
                success=False,
                message="Janela de dados insuficiente para computação estatística.",
                analytics_result=0.0,
                result_metadata="",
                graph_rows=[],
                sample_count=0,
            )

        if req.query_op == messages_pb2.OP_AVERAGE:
            analytics_result = value_sum / sample_count
        else:
            analytics_result = sample_stddev(sample_count, value_sum, value_sum_sq)

        graph_rows = await fetch_graph_rows(
            db, source, req.query_metric, req.start_timestamp, req.end_timestamp, req.target_device_id,
        )
        bucket_label = f", bucket={source.bucket_size}s" if source.is_rollup else ""
        return OlapQueryResult(
            success=True,
            message="",
            analytics_result=analytics_result,
            result_metadata=(
                f"Fonte OLAP: {source.name}{bucket_label}. "
                f"Amostras processadas: {sample_count}. Pontos retornados: {len(graph_rows)}."
            ),
            graph_rows=graph_rows,
            sample_count=sample_count,
        )

    if req.query_op == messages_pb2.OP_MAX_VARIATION:
        async with db.execute(f"""
            SELECT device_id,
                   {count_col} AS total_count,
                   {min_col}   AS min_value,
                   {max_col}   AS max_value
            FROM {source.table_name}
            WHERE metric_name = ? AND {time_col} BETWEEN ? AND ? {device_filter}
            GROUP BY device_id
            HAVING {count_col} > 1
        """, params_base) as cursor:
            variation_rows = await cursor.fetchall()

        if not variation_rows:
            return OlapQueryResult(
                success=False,
                message="Dados insuficientes para calcular variação por dispositivo.",
                analytics_result=0.0,
                result_metadata="",
                graph_rows=[],
                sample_count=0,
            )

        best_dev, total_count, min_value, max_value = max(
            variation_rows,
            key=lambda row: float(row[3]) - float(row[2]),
        )
        max_variation = float(max_value) - float(min_value)
        sample_count  = sum(int(row[1]) for row in variation_rows)
        graph_rows    = await fetch_graph_rows(
            db, source, req.query_metric, req.start_timestamp, req.end_timestamp, req.target_device_id,
        )
        bucket_label = f", bucket={source.bucket_size}s" if source.is_rollup else ""
        return OlapQueryResult(
            success=True,
            message="",
            analytics_result=max_variation,
            result_metadata=(
                f"Fonte OLAP: {source.name}{bucket_label}. "
                f"Maior variação: dispositivo {best_dev} — "
                f"{len(variation_rows)} nós avaliados, {sample_count} amostras."
            ),
            graph_rows=graph_rows,
            sample_count=sample_count,
        )

    if req.query_op in (messages_pb2.OP_ANOMALY_DETECTION, messages_pb2.OP_PERCENTILE_95, messages_pb2.OP_LINEAR_TREND):
        graph_rows = await fetch_graph_rows(
            db, source, req.query_metric, req.start_timestamp, req.end_timestamp, req.target_device_id,
        )
        bucket_label = f", bucket={source.bucket_size}s" if source.is_rollup else ""

        if not graph_rows:
            return OlapQueryResult(
                success=False,
                message="Dados insuficientes para análise avançada.",
                analytics_result=0.0,
                result_metadata="",
                graph_rows=[],
                sample_count=0,
            )

        values = [r[1] for r in graph_rows]
        sample_count = len(values)

        if req.query_op == messages_pb2.OP_ANOMALY_DETECTION:
            mean = sum(values) / sample_count
            variance = sum((v - mean) ** 2 for v in values) / sample_count
            std_dev = variance ** 0.5
            threshold = 3 * std_dev
            
            anomalies = [r for r in graph_rows if abs(r[1] - mean) > threshold]
            anomaly_count = len(anomalies)
            
            return OlapQueryResult(
                success=True,
                message="",
                analytics_result=float(anomaly_count),
                result_metadata=(
                    f"Fonte OLAP: {source.name}{bucket_label}. "
                    f"Anomalias (Z-Score > 3): {anomaly_count} de {sample_count} amostras analisadas."
                ),
                graph_rows=anomalies,
                sample_count=sample_count,
            )

        if req.query_op == messages_pb2.OP_PERCENTILE_95:
            sorted_values = sorted(values)
            idx = int(0.95 * sample_count)
            if idx >= sample_count: idx = sample_count - 1
            p95 = sorted_values[idx]
            
            return OlapQueryResult(
                success=True,
                message="",
                analytics_result=p95,
                result_metadata=(
                    f"Fonte OLAP: {source.name}{bucket_label}. "
                    f"Percentil 95 calculado sobre {sample_count} pontos agregados."
                ),
                graph_rows=graph_rows,
                sample_count=sample_count,
            )

        if req.query_op == messages_pb2.OP_LINEAR_TREND:
            if sample_count < 2:
                return OlapQueryResult(
                    success=False,
                    message="Dados insuficientes para calcular tendência (mínimo 2 pontos).",
                    analytics_result=0.0,
                    result_metadata="",
                    graph_rows=graph_rows,
                    sample_count=sample_count,
                )
                
            x_values = [r[0] for r in graph_rows]
            x_min = min(x_values)
            x_norm = [x - x_min for x in x_values]
            
            sum_x = sum(x_norm)
            sum_y = sum(values)
            sum_xy = sum(x * y for x, y in zip(x_norm, values))
            sum_xx = sum(x * x for x in x_norm)
            
            denominator = (sample_count * sum_xx) - (sum_x * sum_x)
            slope = 0.0
            if denominator != 0:
                slope = ((sample_count * sum_xy) - (sum_x * sum_y)) / denominator
                
            return OlapQueryResult(
                success=True,
                message="",
                analytics_result=slope,
                result_metadata=(
                    f"Fonte OLAP: {source.name}{bucket_label}. "
                    f"Tendência Linear (Slope): {slope:.6f} unidades/segundo."
                ),
                graph_rows=graph_rows,
                sample_count=sample_count,
            )

    return OlapQueryResult(
        success=False,
        message=f"Operação analítica desconhecida: {req.query_op}.",
        analytics_result=0.0,
        result_metadata="",
        graph_rows=[],
        sample_count=0,
    )


async def execute_adaptive_olap(db, req: messages_pb2.ClientRequest) -> OlapQueryResult:
    source = choose_olap_source(req.start_timestamp, req.end_timestamp)
    result = await execute_olap_from_source(db, req, source)

    if result.success or not source.is_rollup:
        return result

    raw_source = OlapSource("raw-fallback", "metrics", 0, False)
    fallback = await execute_olap_from_source(db, req, raw_source)
    if fallback.success:
        fallback.result_metadata = (
            fallback.result_metadata
            + " Rollup escolhido sem dados suficientes; consulta bruta usada como fallback."
        )
    return fallback


async def audit_command(
    peer_ip: str,
    target_device_id: str,
    command_id: str,
    action: str,
    success: bool,
    message: str,
):
    """Registra um comando de atuação no stream Redis 'audit' (append-only).

    Tolerante a falhas: um erro de auditoria nunca deve abortar o comando.
    """
    try:
        client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        await client.xadd(
            "audit",
            {
                "ts": str(int(time.time())),
                "peer": peer_ip or "?",
                "target": target_device_id or "",
                "command_id": command_id or "",
                "action": action or "",
                "success": "1" if success else "0",
                "message": (message or "")[:200],
            },
            maxlen=10000,
            approximate=True,
        )
        await client.close()
    except Exception as exc:
        log.debug("Falha ao gravar auditoria no Redis: %s", exc)


def _describe_command_action(cmd: messages_pb2.ConfigCommand) -> str:
    parts = []
    if cmd.update_status:
        parts.append(f"status={messages_pb2.DeviceStatus.Name(cmd.target_status)}")
    if cmd.update_frequency:
        parts.append(f"freq={cmd.new_frequency_secs}s")
    return ", ".join(parts) if parts else "leitura/no-op"


async def dispatch_command_to_device(
    cmd: messages_pb2.ConfigCommand,
    source_label: str,
) -> tuple[bool, str]:
    """Encaminha um ConfigCommand a um nó atuador (framing + cripto opcional) e audita.

    Reutilizado pelo proxy do cliente (SEND_COMMAND) e pelo motor de automação.
    Retorna (sucesso, mensagem). `cmd.target_device_id` deve estar preenchido.
    """
    target_device_id = cmd.target_device_id
    async with get_db_pool().connection() as db:
        async with db.execute(
            "SELECT ip_address, control_port FROM devices WHERE device_id = ?",
            (target_device_id,),
        ) as cursor:
            target = await cursor.fetchone()

    if not target or target[1] <= 0:
        success, message = False, "Nó não encontrado ou desprovido de porta de controle."
        await audit_command(source_label, target_device_id, cmd.command_id,
                            _describe_command_action(cmd), success, message)
        return success, message

    success, message = False, ""
    s_w = None
    try:
        s_r, s_w = await asyncio.wait_for(
            asyncio.open_connection(target[0], target[1]), timeout=5.0,
        )
        # Carimbo de tempo do gateway (emissor confiável) p/ janela anti-replay.
        cmd.timestamp = int(time.time())
        cmd_bytes = cmd.SerializeToString()
        if CONTROL_SECURE:
            cmd_bytes = encrypt_control(cmd_bytes)
        s_w.write(struct.pack(">I", len(cmd_bytes)) + cmd_bytes)
        await s_w.drain()

        s_header = await asyncio.wait_for(s_r.readexactly(4), timeout=5.0)
        s_len = struct.unpack(">I", s_header)[0]
        if s_len <= 0 or s_len > TCP_MAX_FRAME_BYTES:
            raise ValueError(f"frame de resposta inválido do nó alvo: {s_len} bytes")
        s_body = await asyncio.wait_for(s_r.readexactly(s_len), timeout=5.0)
        if CONTROL_SECURE:
            s_body = decrypt_payload(s_body)
        node_resp = messages_pb2.ConfigResponse()
        node_resp.ParseFromString(s_body)
        success, message = node_resp.success, node_resp.message
        log.info("CMD '%s' → '%s' (%s): sucesso=%s.",
                 cmd.command_id, target_device_id, source_label, success)
    except asyncio.TimeoutError:
        success, message = False, "Timeout de I/O com o nó alvo durante atuação remota."
        log.warning("Timeout ao encaminhar comando para '%s'.", target_device_id)
    except ConnectionRefusedError as exc:
        success, message = False, f"Nó alvo recusou a conexão TCP: {exc}"
        log.warning("Conexão recusada por '%s': %s", target_device_id, exc)
    except (asyncio.IncompleteReadError, struct.error, DecodeError, ValueError) as exc:
        success, message = False, f"Resposta TCP/Protobuf inválida do nó alvo: {exc}"
        log.warning("Frame inválido de '%s': %s", target_device_id, exc)
    except OSError as exc:
        success, message = False, f"Falha de socket com o nó alvo: {exc}"
        log.warning("Falha de socket ao encaminhar comando para '%s': %s", target_device_id, exc)
    finally:
        if s_w is not None:
            s_w.close()
            try:
                await s_w.wait_closed()
            except Exception:
                pass

    await audit_command(source_label, target_device_id, cmd.command_id,
                        _describe_command_action(cmd), success, message)
    _METRICS["commands_total"] += 1
    if not success:
        _METRICS["commands_failed_total"] += 1
    return success, message


# ====================================================================
# [FASE B] MOTOR DE AUTOMAÇÃO (IFTTT) — regras editáveis pela UI
#   Regras vivem no Redis (chave 'automation_rules', JSON), são recarregadas
#   periodicamente e avaliadas sobre a telemetria; ao romper, disparam um
#   comando de atuação via dispatch_command_to_device (com cooldown por regra).
# ====================================================================

AUTOMATION_REFRESH_SECS = max(1, int(os.getenv("AUTOMATION_REFRESH_SECS", "5")))
AUTOMATION_RULES: list[dict] = []
_automation_last_fired: dict[tuple[str, str], int] = {}


def _status_to_int(value) -> int:
    """Aceita status como int (1/2/3) ou nome ('STATUS_OFF')."""
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        pass
    try:
        return messages_pb2.DeviceStatus.Value(str(value))
    except ValueError:
        return messages_pb2.STATUS_OFF


async def automation_rules_refresh_loop():
    """Recarrega periodicamente regras de automação e silenciamentos de alerta do Redis."""
    global AUTOMATION_RULES, ALERT_SILENCES
    while True:
        try:
            if REDIS_PUB is not None:
                raw = await REDIS_PUB.get("automation_rules")
                AUTOMATION_RULES = json.loads(raw) if raw else []

                # Silenciamentos: hash 'alert_silences' {device|metric: expiry_epoch}.
                # Descarta os já expirados (limpeza preguiçosa).
                now = int(time.time())
                raw_sil = await REDIS_PUB.hgetall("alert_silences")
                silences = {}
                for field, value in (raw_sil or {}).items():
                    try:
                        key = field.decode() if isinstance(field, bytes) else field
                        # REDIS_PUB não usa decode_responses → value vem em bytes.
                        val = value.decode() if isinstance(value, bytes) else value
                        expiry = int(val)
                        if expiry > now:
                            silences[key] = expiry
                    except (TypeError, ValueError):
                        continue
                ALERT_SILENCES = silences
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.debug("Falha ao carregar config (regras/silenciamentos): %s", exc)
        await asyncio.sleep(AUTOMATION_REFRESH_SECS)


async def evaluate_automation(batch: list[TelemetryEnvelope]):
    """Avalia as regras de automação sobre um batch e dispara as ações."""
    rules = AUTOMATION_RULES
    if not rules:
        return
    now = int(time.time())
    for envelope in batch:
        for sample in envelope.metrics:
            for rule in rules:
                if not rule.get("enabled", True):
                    continue
                if rule.get("metric") != sample.metric_name:
                    continue
                try:
                    threshold = float(rule["threshold"])
                except (KeyError, TypeError, ValueError):
                    continue
                if not _threshold_breached(rule.get("op", ">="), sample.value, threshold):
                    continue

                # Alvo: device fixo da regra ou o próprio device que rompeu.
                target = rule.get("target_device_id") or sample.device_id
                cooldown = max(1, int(rule.get("cooldown_secs", 60)))
                key = (str(rule.get("id", "")), target)
                if now - _automation_last_fired.get(key, 0) < cooldown:
                    continue
                _automation_last_fired[key] = now

                cmd = messages_pb2.ConfigCommand()
                cmd.command_id = f"AUTO-{str(rule.get('id', 'rule'))[:8]}-{uuid.uuid4().hex[:4]}"
                cmd.target_device_id = target
                if rule.get("action_status"):
                    cmd.update_status = True
                    cmd.target_status = _status_to_int(rule.get("status"))
                if rule.get("action_freq"):
                    cmd.update_frequency = True
                    cmd.new_frequency_secs = max(1, int(rule.get("frequency_secs", 5)))

                if not cmd.update_status and not cmd.update_frequency:
                    continue  # regra sem ação efetiva

                task = asyncio.create_task(
                    dispatch_command_to_device(cmd, f"automation:{rule.get('id', '')}")
                )
                _BACKGROUND_TASKS.add(task)
                task.add_done_callback(_BACKGROUND_TASKS.discard)
                log.info(
                    "Automação '%s' disparada por %s=%.1f → %s",
                    rule.get("name", rule.get("id", "?")), sample.metric_name, sample.value, target,
                )


async def build_client_response(
    req: messages_pb2.ClientRequest,
    peer,
) -> messages_pb2.ClientResponse:
    """Processa uma requisição já desserializada e preserva o contrato ClientResponse."""
    resp = new_client_response(success=True)

    # ── Rota: Sincronização de Inventário ────────────────────────────
    if req.type == messages_pb2.REQUEST_TYPE_LIST_DEVICES:
        async with get_db_pool().connection() as db:
            # [R1] Colunas explícitas — resistente a mudanças futuras de schema
            async with db.execute("""
                SELECT device_id, type, status, ip_address,
                       control_port, is_controllable, last_seen, aggregator_id, coord_x, coord_y
                FROM devices
            """) as cursor:
                async for row in cursor:
                    device_id, dtype, status, ip, ctrl_port, is_ctrl, last_seen, agg_id, cx, cy = row
                    d = resp.devices.add()
                    d.device_id           = device_id
                    d.type                = dtype
                    d.status              = status
                    d.ip_address          = ip
                    d.control_port        = ctrl_port
                    d.is_controllable     = bool(is_ctrl)
                    d.last_seen_timestamp = last_seen
                    d.coord_x = cx if cx is not None else 0
                    d.coord_y = cy if cy is not None else 0
                    if agg_id:
                        d.aggregator_id = agg_id

        resp.message = "Sincronização de topologia extraída via pool aiosqlite."
        log.info("LIST_DEVICES → %d nós retornados para %s.", len(resp.devices), peer)
        return resp

    # ── Rota: Proxy de Atuação Remota ────────────────────────────────
    if req.type == messages_pb2.REQUEST_TYPE_SEND_COMMAND:
        req.command_payload.target_device_id = req.target_device_id
        peer_ip = peer[0] if peer else "?"
        resp.success, resp.message = await dispatch_command_to_device(
            req.command_payload, peer_ip,
        )
        return resp

    # ── Rota: Agregações Estatísticas (OLAP) ─────────────────────────
    if req.type == messages_pb2.REQUEST_TYPE_ANALYTICS_QUERY:
        async with get_db_pool().connection() as db:
            olap_result = await execute_adaptive_olap(db, req)

        resp.success = olap_result.success
        resp.message = olap_result.message
        resp.analytics_result = olap_result.analytics_result
        resp.result_metadata = olap_result.result_metadata

        if resp.success:
            for timestamp, value, device_id in olap_result.graph_rows:
                pt           = resp.graph_points.add()
                pt.timestamp = timestamp
                pt.value     = value
                pt.device_id = device_id

        log.info(
            "ANALYTICS_QUERY metric='%s' op=%d → %d amostras/%d pontos (sucesso=%s).",
            req.query_metric, req.query_op,
            olap_result.sample_count, len(olap_result.graph_rows), resp.success,
        )

        return resp

    resp.success = False
    resp.message = f"Tipo de requisição desconhecido: {req.type}."
    return resp


async def send_client_response(
    writer: asyncio.StreamWriter,
    resp: messages_pb2.ClientResponse,
):
    resp_bytes = resp.SerializeToString()
    writer.write(struct.pack(">I", len(resp_bytes)) + resp_bytes)
    await writer.drain()


async def handle_client_request(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Pipeline TCP assíncrono com Length-Prefix Framing e conexão persistente."""
    peer = writer.get_extra_info("peername")
    try:
        while True:
            try:
                header = await asyncio.wait_for(
                    reader.readexactly(4),
                    timeout=TCP_CLIENT_IDLE_TIMEOUT,
                )
            except asyncio.TimeoutError:
                log.debug("Conexão TCP ociosa com %s encerrada.", peer)
                break
            except asyncio.IncompleteReadError:
                log.debug("Cliente TCP %s encerrou a conexão.", peer)
                break

            try:
                msg_len = struct.unpack(">I", header)[0]
            except struct.error as exc:
                log.warning("Cabeçalho TCP inválido de %s: %s", peer, exc)
                break

            if msg_len <= 0 or msg_len > TCP_MAX_FRAME_BYTES:
                log.warning("Frame TCP inválido de %s: %d bytes.", peer, msg_len)
                resp = new_client_response(success=False)
                resp.message = f"Frame TCP inválido: {msg_len} bytes."
                await send_client_response(writer, resp)
                break

            try:
                data = await asyncio.wait_for(
                    reader.readexactly(msg_len),
                    timeout=TCP_CLIENT_READ_TIMEOUT,
                )
            except asyncio.TimeoutError:
                log.warning(
                    "Timeout lendo payload TCP de %s (%d bytes) — conexão encerrada.",
                    peer, msg_len,
                )
                break
            except asyncio.IncompleteReadError:
                log.warning("Payload TCP incompleto de %s — conexão encerrada.", peer)
                break

            req = messages_pb2.ClientRequest()
            try:
                req.ParseFromString(data)
            except DecodeError as exc:
                resp = new_client_response(success=False)
                resp.message = f"Payload Protobuf inválido: {exc}"
                await send_client_response(writer, resp)
                continue

            try:
                resp = await build_client_response(req, peer)
            except sqlite3.Error as exc:
                log.warning("Falha SQLite processando requisição de %s: %s", peer, exc)
                resp = new_client_response(success=False)
                resp.message = f"Falha SQLite no Gateway: {exc}"
            except Exception as exc:
                # Captura exceções inesperadas (ex.: AttributeError, TypeError)
                # que escapariam para o handler de conexão e encerrariam o canal TCP
                # sem enviar resposta ao cliente.
                log.error(
                    "Erro inesperado processando requisição de %s: %s",
                    peer, exc, exc_info=True,
                )
                resp = new_client_response(success=False)
                resp.message = "Erro interno no Gateway — consulte os logs do servidor."
            await send_client_response(writer, resp)

    except (ConnectionResetError, BrokenPipeError) as exc:
        log.warning("Conexão TCP interrompida por %s: %s", peer, exc)
    except OSError as exc:
        log.warning("Erro de socket no handler TCP (%s): %s", peer, exc)
    finally:
        writer.close()
        await writer.wait_closed()


# ====================================================================
# [FASE E] OBSERVABILIDADE — snapshot, Prometheus e heartbeats de agregadores
# ====================================================================

async def build_metrics_snapshot() -> dict:
    """Coleta contadores + gauges + saúde dos agregadores num dicionário."""
    queue_size = TELEMETRY_QUEUE.qsize() if TELEMETRY_QUEUE is not None else 0
    devices_total = devices_online = 0
    try:
        async with get_db_pool().connection() as db:
            async with db.execute(
                "SELECT COUNT(*), COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0) FROM devices",
                (messages_pb2.STATUS_ON,),
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    devices_total = int(row[0] or 0)
                    devices_online = int(row[1] or 0)
    except Exception:
        pass

    now = int(time.time())
    aggregators = {}
    for agg in sorted(_KNOWN_AGGREGATORS):
        last = _aggregator_last_seen.get(agg, 0)
        up = 1 if (last > 0 and now - last <= AGG_HEALTH_TIMEOUT_SECS) else 0
        aggregators[agg] = {"up": up, "last_seen": last, "silent_for": (now - last) if last else -1}

    return {
        "ts": now,
        "counters": dict(_METRICS),
        "gauges": {
            "telemetry_queue_size": queue_size,
            "telemetry_queue_max": TELEMETRY_QUEUE_MAXSIZE,
            "available_ports": len(_AVAILABLE_PORTS),
            "max_ports": MAX_AVAILABLE_PORTS,
            "known_aggregators": len(_KNOWN_AGGREGATORS),
            "devices_total": devices_total,
            "devices_online": devices_online,
            "control_secure": 1 if CONTROL_SECURE else 0,
        },
        "aggregators": aggregators,
    }


async def build_prometheus_text() -> str:
    """Renderiza o snapshot no formato de exposição do Prometheus."""
    snap = await build_metrics_snapshot()
    out: list[str] = []

    def emit(name, value, help_text, typ):
        out.append(f"# HELP {name} {help_text}")
        out.append(f"# TYPE {name} {typ}")
        out.append(f"{name} {value}")

    for key, value in snap["counters"].items():
        emit(f"gw_{key}", value, key, "counter")
    for key, value in snap["gauges"].items():
        emit(f"gw_{key}", value, key, "gauge")

    out.append("# HELP gw_aggregator_up Agregador ativo (1) ou inativo (0)")
    out.append("# TYPE gw_aggregator_up gauge")
    for agg, info in snap["aggregators"].items():
        out.append(f'gw_aggregator_up{{aggregator="{agg}"}} {info["up"]}')
    return "\n".join(out) + "\n"


async def handle_prometheus(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Servidor HTTP mínimo que expõe /metrics no formato Prometheus."""
    try:
        request_line = await asyncio.wait_for(reader.readline(), timeout=3.0)
        # Drena os cabeçalhos (best-effort) até a linha em branco.
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=3.0)
            if line in (b"\r\n", b"\n", b""):
                break

        path = b""
        parts = request_line.split(b" ")
        if len(parts) >= 2:
            path = parts[1]

        if path.startswith(b"/metrics"):
            body = (await build_prometheus_text()).encode("utf-8")
            status = "200 OK"
            ctype = "text/plain; version=0.0.4; charset=utf-8"
        else:
            body = b"Smart City Gateway - /metrics\n"
            status = "200 OK"
            ctype = "text/plain; charset=utf-8"

        header = (
            f"HTTP/1.1 {status}\r\n"
            f"Content-Type: {ctype}\r\n"
            f"Content-Length: {len(body)}\r\n"
            "Connection: close\r\n\r\n"
        ).encode("utf-8")
        writer.write(header + body)
        await writer.drain()
    except Exception:
        pass
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def metrics_publish_loop():
    """Lê heartbeats dos agregadores (health ACK) e publica o snapshot no Redis."""
    await asyncio.sleep(3.0)
    while True:
        try:
            if REDIS_PUB is not None:
                # Health two-way: agregadores escrevem agg_heartbeat:<id> (com TTL).
                # Mais confiável que a atividade de relay (não dá falso-positivo
                # quando o agregador está vivo porém ocioso).
                for agg in list(_KNOWN_AGGREGATORS):
                    hb = await REDIS_PUB.get(f"agg_heartbeat:{agg}")
                    if hb:
                        try:
                            ts = int(hb.decode() if isinstance(hb, bytes) else hb)
                            if ts > _aggregator_last_seen.get(agg, 0):
                                _aggregator_last_seen[agg] = ts
                        except (TypeError, ValueError):
                            pass
                snapshot = await build_metrics_snapshot()
                await REDIS_PUB.set("gw_metrics", json.dumps(snapshot))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.debug("Falha ao publicar métricas: %s", exc)
        await asyncio.sleep(GW_METRICS_PUBLISH_SECS)


# ====================================================================
# INICIALIZAÇÃO DO LOOP DE EVENTOS E KERNEL DE BORDAS
# ====================================================================

async def main():
    global DB_POOL, TELEMETRY_QUEUE, REDIS_PUB

    log.info("============================================================")
    log.info("Inicializando nó central (aiosqlite / Framing Distribuído)...")
    log.info("LOG_LEVEL=%s | TCP_READ_TIMEOUT=%.1fs | TCP_IDLE_TIMEOUT=%.1fs | WAL_CHECKPOINT=%ds",
             _LOG_LEVEL_STR, TCP_CLIENT_READ_TIMEOUT, TCP_CLIENT_IDLE_TIMEOUT,
             WAL_CHECKPOINT_INTERVAL_SECS)
    log.info("OFFLINE_TIMEOUT=%.1fs | OFFLINE_CHECK=%.1fs",
             DEVICE_OFFLINE_TIMEOUT_SECS, DEVICE_OFFLINE_CHECK_INTERVAL_SECS)
    log.info(
        "OLAP raw<=%ds | 1m<=%ds | 5m<=%ds | raw_retention=%ds | batch=%d payloads/%d rows",
        OLAP_RAW_MAX_WINDOW_SECS, OLAP_1M_MAX_WINDOW_SECS, OLAP_5M_MAX_WINDOW_SECS,
        METRICS_RAW_RETENTION_SECS, TELEMETRY_BATCH_MAX_PAYLOADS, TELEMETRY_BATCH_MAX_ROWS,
    )
    log.info("============================================================")

    init_db()
    DB_POOL = SQLiteConnectionPool(DB_FILE, DB_POOL_SIZE)
    await DB_POOL.start()
    TELEMETRY_QUEUE = asyncio.Queue(maxsize=TELEMETRY_QUEUE_MAXSIZE)
    REDIS_PUB = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)  # publicação de alertas (Fase B)

    loop = asyncio.get_running_loop()

    # Tarefas de leitura do Redis (Substitui datagram endpoints)
    redis_tel_task  = asyncio.create_task(redis_telemetry_loop())
    redis_disc_task = asyncio.create_task(redis_discovery_loop())

    # Servidor de Controle TCP
    server = await asyncio.start_server(handle_client_request, "0.0.0.0", TCP_PORT)
    
    # Servidor de Autenticação TCP
    auth_server = await asyncio.start_server(handle_auth_client, "0.0.0.0", AUTH_PORT)

    # Servidor HTTP de métricas Prometheus (/metrics) — [Fase E]
    prom_server = await asyncio.start_server(handle_prometheus, "0.0.0.0", PROMETHEUS_PORT)

    # Tasks de background
    probe_task      = asyncio.create_task(multicast_discovery_probe_loop())
    checkpoint_task = asyncio.create_task(wal_checkpoint_loop())  # [M3]
    offline_task    = asyncio.create_task(device_offline_monitor_loop())
    telemetry_task  = asyncio.create_task(telemetry_batch_worker_loop())
    retention_task  = asyncio.create_task(metrics_retention_loop())
    automation_task = asyncio.create_task(automation_rules_refresh_loop())  # [Fase B]
    agg_health_task = asyncio.create_task(aggregator_health_loop())          # [Fase B+]
    metrics_task    = asyncio.create_task(metrics_publish_loop())            # [Fase E]

    log.info(
        "Hub pronto. TCP:%d | Lendo dados do Redis (%s:%d)",
        TCP_PORT, REDIS_HOST, REDIS_PORT,
    )

    try:
        async with server, auth_server, prom_server:
            await asyncio.gather(
                server.serve_forever(),
                auth_server.serve_forever(),
                prom_server.serve_forever(),
            )
    finally:
        # Cancela tasks UDP em voo (telemetria/descoberta) antes de fechar
        # os transportes e o pool. Sem isso, tasks que chegaram no último instante
        # podem tentar acessar o pool já encerrado.
        for task in list(_BACKGROUND_TASKS):
            task.cancel()
        if _BACKGROUND_TASKS:
            await asyncio.gather(*_BACKGROUND_TASKS, return_exceptions=True)

        probe_task.cancel()
        checkpoint_task.cancel()
        offline_task.cancel()
        telemetry_task.cancel()
        retention_task.cancel()
        automation_task.cancel()
        agg_health_task.cancel()
        metrics_task.cancel()
        redis_tel_task.cancel()
        redis_disc_task.cancel()
        await asyncio.gather(
            probe_task, checkpoint_task, offline_task, telemetry_task, retention_task,
            automation_task, agg_health_task, metrics_task, redis_tel_task, redis_disc_task,
            return_exceptions=True,
        )
        await DB_POOL.close()
        if REDIS_PUB is not None:
            await REDIS_PUB.close()
            REDIS_PUB = None
        DB_POOL = None
        TELEMETRY_QUEUE = None
        log.info("Gateway encerrado.")


if __name__ == "__main__":
    asyncio.run(main())