import asyncio
import logging
import math
import sqlite3
import time
import struct
import os
import socket
import uuid
import signal
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Tuple

# Biblioteca assíncrona para I/O não-bloqueante no SQLite
import aiosqlite
from google.protobuf.message import DecodeError

from analytics import (
    OlapSource,
    build_rollup_rows,
    choose_retained_olap_source as select_olap_source,
    graph_sampling_stride,
    olap_time_range,
    sample_stddev,
    validate_time_window,
)

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
TELEMETRY_SHUTDOWN_TIMEOUT_SECS = max(
    0.1, float(os.getenv("TELEMETRY_SHUTDOWN_TIMEOUT_SECS", "20"))
)
DISCOVERY_MAX_IN_FLIGHT = max(1, int(os.getenv("DISCOVERY_MAX_IN_FLIGHT", "256")))

METRICS_RAW_RETENTION_SECS = max(0, int(os.getenv("METRICS_RAW_RETENTION_SECS", str(7 * 24 * 3600))))
ROLLUP_1M_RETENTION_SECS = max(0, int(os.getenv("ROLLUP_1M_RETENTION_SECS", str(30 * 24 * 3600))))
ROLLUP_5M_RETENTION_SECS = max(0, int(os.getenv("ROLLUP_5M_RETENTION_SECS", str(180 * 24 * 3600))))
ROLLUP_1H_RETENTION_SECS = max(0, int(os.getenv("ROLLUP_1H_RETENTION_SECS", str(365 * 24 * 3600))))
METRICS_RETENTION_INTERVAL_SECS = max(60.0, float(os.getenv("METRICS_RETENTION_INTERVAL_SECS", "3600")))
ROLLUP_BACKFILL_ON_STARTUP = os.getenv("ROLLUP_BACKFILL_ON_STARTUP", "1").lower() not in {"0", "false", "no"}

OLAP_RAW_MAX_WINDOW_SECS = max(60, int(os.getenv("OLAP_RAW_MAX_WINDOW_SECS", "3600")))
OLAP_1M_MAX_WINDOW_SECS = max(OLAP_RAW_MAX_WINDOW_SECS, int(os.getenv("OLAP_1M_MAX_WINDOW_SECS", str(24 * 3600))))
OLAP_5M_MAX_WINDOW_SECS = max(OLAP_1M_MAX_WINDOW_SECS, int(os.getenv("OLAP_5M_MAX_WINDOW_SECS", str(7 * 24 * 3600))))
OLAP_MAX_QUERY_WINDOW_SECS = max(60, int(os.getenv("OLAP_MAX_QUERY_WINDOW_SECS", str(30 * 24 * 3600))))
OLAP_MAX_GRAPH_POINTS = max(2, int(os.getenv("OLAP_MAX_GRAPH_POINTS", "2000")))

ROLLUP_TABLES = (
    ("metrics_rollup_1m", 60, ROLLUP_1M_RETENTION_SECS),
    ("metrics_rollup_5m", 300, ROLLUP_5M_RETENTION_SECS),
    ("metrics_rollup_1h", 3600, ROLLUP_1H_RETENTION_SECS),
)

DB_POOL = None
TELEMETRY_QUEUE: asyncio.Queue | None = None

# Referências fortes para tasks assíncronas de descoberta, com limite explícito.
_BACKGROUND_TASKS: set[asyncio.Task] = set()
_CLIENT_TASKS: set[asyncio.Task] = set()
_TELEMETRY_DROPPED = 0
_DISCOVERY_DROPPED = 0

# Importa as classes do Protobuf geradas dinamicamente
import messages_pb2  # pyright: ignore[reportMissingImports]

# ====================================================================
# CONFIGURAÇÕES DE REDE
# ====================================================================

UDP_TELEMETRY_PORT = 5000   # Porta dedicada exclusivamente à ingestão de dados
UDP_DISCOVERY_PORT = 5002   # Porta dedicada exclusivamente aos handshakes de topologia
TCP_PORT           = 5001

# Timeout para leitura de cabeçalho e payload TCP do cliente (configurável)
TCP_CLIENT_READ_TIMEOUT = max(5.0, float(os.getenv("TCP_CLIENT_READ_TIMEOUT", "10")))
TCP_CLIENT_IDLE_TIMEOUT = max(
    TCP_CLIENT_READ_TIMEOUT,
    float(os.getenv("TCP_CLIENT_IDLE_TIMEOUT", "60")),
)
TCP_MAX_FRAME_BYTES = max(1024, int(os.getenv("TCP_MAX_FRAME_BYTES", str(1024 * 1024))))

# Limites das fronteiras não autenticadas. São deliberadamente conservadores:
# todos os payloads legítimos do simulador ficam muito abaixo destes valores.
UDP_MAX_DATAGRAM_BYTES = min(
    65507,
    max(512, int(os.getenv("UDP_MAX_DATAGRAM_BYTES", str(16 * 1024)))),
)
MAX_DEVICE_ID_LENGTH = max(16, int(os.getenv("MAX_DEVICE_ID_LENGTH", "128")))
MAX_MESSAGE_ID_LENGTH = max(16, int(os.getenv("MAX_MESSAGE_ID_LENGTH", "128")))
MAX_METRIC_NAME_LENGTH = max(8, int(os.getenv("MAX_METRIC_NAME_LENGTH", "128")))
MAX_METRIC_UNIT_LENGTH = max(4, int(os.getenv("MAX_METRIC_UNIT_LENGTH", "32")))
MAX_METRICS_PER_PAYLOAD = max(1, int(os.getenv("MAX_METRICS_PER_PAYLOAD", "64")))
MESSAGE_MAX_AGE_SECS = max(0, int(os.getenv("MESSAGE_MAX_AGE_SECS", str(24 * 3600))))
MESSAGE_MAX_FUTURE_SKEW_SECS = max(
    0,
    int(os.getenv("MESSAGE_MAX_FUTURE_SKEW_SECS", "300")),
)

VALID_DEVICE_TYPES = frozenset(
    {
        messages_pb2.DEVICE_TYPE_TRAFFIC_LIGHT,
        messages_pb2.DEVICE_TYPE_LAMP_POST,
        messages_pb2.DEVICE_TYPE_WEATHER_STATION,
        messages_pb2.DEVICE_TYPE_CAMERA,
        messages_pb2.DEVICE_TYPE_AIR_QUALITY,
        messages_pb2.DEVICE_TYPE_PARKING_SENSOR,
    }
)
VALID_DEVICE_STATUSES = frozenset(
    {
        messages_pb2.STATUS_ON,
        messages_pb2.STATUS_OFF,
        messages_pb2.STATUS_ERROR,
    }
)
VALID_COMMAND_STATUSES = frozenset(
    {messages_pb2.STATUS_ON, messages_pb2.STATUS_OFF}
)
VALID_REQUEST_TYPES = frozenset(
    {
        messages_pb2.REQUEST_TYPE_LIST_DEVICES,
        messages_pb2.REQUEST_TYPE_SEND_COMMAND,
        messages_pb2.REQUEST_TYPE_ANALYTICS_QUERY,
    }
)
VALID_QUERY_OPS = frozenset(
    {
        messages_pb2.OP_AVERAGE,
        messages_pb2.OP_STD_DEV,
        messages_pb2.OP_MAX_VARIATION,
    }
)

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
    message_id: str


def validate_text_field(
    value: str,
    field_name: str,
    max_length: int,
    *,
    required: bool = True,
) -> str | None:
    """Retorna uma descrição do erro ou ``None`` para texto canônico e limitado."""

    stripped = value.strip()
    if required and not stripped:
        return f"{field_name} é obrigatório"
    if len(stripped) > max_length:
        return f"{field_name} excede {max_length} caracteres"
    if value != stripped:
        return f"{field_name} não pode começar ou terminar com espaços"
    if any(ord(char) < 32 or ord(char) == 127 for char in stripped):
        return f"{field_name} contém caracteres de controle"
    return None


def validate_ingress_timestamp(
    timestamp: int,
    field_name: str,
    *,
    now: int | None = None,
) -> str | None:
    """Valida timestamps recebidos contra os limites de replay e relógio futuro."""

    current_time = int(time.time()) if now is None else int(now)
    value = int(timestamp)
    if value <= 0:
        return f"{field_name} deve ser um Unix timestamp positivo"
    if value > current_time + MESSAGE_MAX_FUTURE_SKEW_SECS:
        return (
            f"{field_name} está mais de {MESSAGE_MAX_FUTURE_SKEW_SECS}s no futuro"
        )
    if MESSAGE_MAX_AGE_SECS and value < current_time - MESSAGE_MAX_AGE_SECS:
        return f"{field_name} excede a idade máxima de {MESSAGE_MAX_AGE_SECS}s"
    return None


def validate_telemetry_payload(
    payload: messages_pb2.DataPayload,
    *,
    now: int | None = None,
) -> str | None:
    """Valida completamente um DataPayload antes de alocar trabalho assíncrono."""

    error = validate_text_field(
        payload.message_id, "message_id", MAX_MESSAGE_ID_LENGTH
    )
    if error:
        return error
    error = validate_text_field(
        payload.device_id, "device_id", MAX_DEVICE_ID_LENGTH
    )
    if error:
        return error
    error = validate_ingress_timestamp(payload.timestamp, "timestamp", now=now)
    if error:
        return error
    if int(payload.current_status) not in VALID_DEVICE_STATUSES:
        return f"current_status inválido: {int(payload.current_status)}"
    if len(payload.metrics) > MAX_METRICS_PER_PAYLOAD:
        return (
            f"payload contém {len(payload.metrics)} métricas; "
            f"máximo permitido: {MAX_METRICS_PER_PAYLOAD}"
        )

    for index, metric in enumerate(payload.metrics):
        error = validate_text_field(
            metric.name,
            f"metrics[{index}].name",
            MAX_METRIC_NAME_LENGTH,
        )
        if error:
            return error
        error = validate_text_field(
            metric.unit,
            f"metrics[{index}].unit",
            MAX_METRIC_UNIT_LENGTH,
            required=False,
        )
        if error:
            return error
        if not math.isfinite(float(metric.value)):
            return f"metrics[{index}].value deve ser finito"

    return None


def validate_discovery_payload(
    discovery: messages_pb2.DiscoveryResponse,
    *,
    now: int | None = None,
) -> str | None:
    """Valida uma mensagem de descoberta antes de criar sua task de persistência."""

    error = validate_text_field(
        discovery.message_id, "message_id", MAX_MESSAGE_ID_LENGTH
    )
    if error:
        return error
    error = validate_text_field(
        discovery.device_id, "device_id", MAX_DEVICE_ID_LENGTH
    )
    if error:
        return error
    error = validate_ingress_timestamp(discovery.timestamp, "timestamp", now=now)
    if error:
        return error
    if int(discovery.type) not in VALID_DEVICE_TYPES:
        return f"type inválido: {int(discovery.type)}"
    if int(discovery.initial_status) not in VALID_DEVICE_STATUSES:
        return f"initial_status inválido: {int(discovery.initial_status)}"
    error = validate_text_field(
        discovery.ip_address,
        "ip_address",
        255,
        required=False,
    )
    if error:
        return error
    if discovery.is_controllable and not 1 <= discovery.control_port <= 65535:
        return f"control_port inválida: {discovery.control_port}"
    if not discovery.is_controllable and discovery.control_port not in (0,):
        return "control_port deve ser zero para um dispositivo não controlável"
    return None


def validate_analytics_request(req: messages_pb2.ClientRequest) -> str | None:
    error = validate_text_field(
        req.query_metric, "query_metric", MAX_METRIC_NAME_LENGTH
    )
    if error:
        return error
    error = validate_text_field(
        req.target_device_id,
        "target_device_id",
        MAX_DEVICE_ID_LENGTH,
        required=False,
    )
    if error:
        return error
    if int(req.query_op) not in VALID_QUERY_OPS:
        return f"query_op inválida: {int(req.query_op)}"
    if req.start_timestamp <= 0 or req.end_timestamp <= 0:
        return "start_timestamp e end_timestamp devem ser Unix timestamps positivos"
    try:
        validate_time_window(
            int(req.start_timestamp),
            int(req.end_timestamp),
            OLAP_MAX_QUERY_WINDOW_SECS,
        )
    except ValueError as exc:
        return str(exc)
    return None


def validate_command_request(req: messages_pb2.ClientRequest) -> str | None:
    error = validate_text_field(
        req.target_device_id, "target_device_id", MAX_DEVICE_ID_LENGTH
    )
    if error:
        return error
    if not req.HasField("command_payload"):
        return "command_payload é obrigatório"

    command = req.command_payload
    error = validate_text_field(
        command.command_id, "command_id", MAX_MESSAGE_ID_LENGTH
    )
    if error:
        return error
    error = validate_ingress_timestamp(command.timestamp, "command.timestamp")
    if error:
        return error
    error = validate_text_field(
        command.target_device_id,
        "command.target_device_id",
        MAX_DEVICE_ID_LENGTH,
    )
    if error:
        return error
    if command.target_device_id != req.target_device_id:
        return "command.target_device_id diverge de target_device_id"
    if not command.update_status and not command.update_frequency:
        return "o comando deve solicitar ao menos uma alteração"
    if command.update_status and int(command.target_status) not in VALID_COMMAND_STATUSES:
        return f"target_status inválido: {int(command.target_status)}"
    if command.update_frequency and not 1 <= command.new_frequency_secs <= 60:
        return "new_frequency_secs deve estar entre 1 e 60"
    return None


def validate_client_request(req: messages_pb2.ClientRequest) -> str | None:
    """Valida o envelope TCP e delega os campos específicos de cada rota."""

    error = validate_text_field(req.message_id, "message_id", MAX_MESSAGE_ID_LENGTH)
    if error:
        return error
    error = validate_ingress_timestamp(req.timestamp, "timestamp")
    if error:
        return error
    if int(req.type) not in VALID_REQUEST_TYPES:
        return f"type inválido: {int(req.type)}"
    if req.type == messages_pb2.REQUEST_TYPE_SEND_COMMAND:
        return validate_command_request(req)
    if req.type == messages_pb2.REQUEST_TYPE_ANALYTICS_QUERY:
        return validate_analytics_request(req)
    return None


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
        except BaseException:

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
    os.makedirs(DB_DIR, exist_ok=True)
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
            last_seen    INTEGER
        )
    """)
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
    ensure_metrics_index(cursor)
    ensure_rollup_tables(cursor)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS telemetry_messages (
            device_id TEXT NOT NULL,
            message_id TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            PRIMARY KEY (device_id, message_id)
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_telemetry_messages_timestamp
        ON telemetry_messages (timestamp)
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS telemetry_state (
            device_id TEXT PRIMARY KEY,
            last_timestamp INTEGER NOT NULL
        )
    """)
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
        message_id=payload.message_id,
    )


async def persist_telemetry_batch(batch: list[TelemetryEnvelope]):
    if not batch:
        return

    now = int(time.time())
    async with get_db_pool().connection() as db:
        # A identidade e os dados precisam ser confirmados juntos. BEGIN IMMEDIATE
        # também impede que duas conexões aceitem a mesma mensagem simultaneamente.
        await db.execute("BEGIN IMMEDIATE")
        accepted = []
        for envelope in batch:
            async with db.execute("""
                INSERT OR IGNORE INTO telemetry_messages (device_id, message_id, timestamp)
                SELECT ?, ?, ?
                WHERE ? >= COALESCE(
                    (SELECT last_timestamp FROM telemetry_state WHERE device_id = ?), 0
                )
            """, (envelope.device_id, envelope.message_id, envelope.timestamp,
                  envelope.timestamp, envelope.device_id)) as cursor:
                inserted = cursor.rowcount
            if not inserted:
                continue
            accepted.append(envelope)
            await db.execute("""
                INSERT INTO telemetry_state (device_id, last_timestamp) VALUES (?, ?)
                ON CONFLICT(device_id) DO UPDATE SET last_timestamp = excluded.last_timestamp
            """, (envelope.device_id, envelope.timestamp))

        # A telemetria pode preceder a descoberta. O registro inicial não apaga
        # capacidades de controle já anunciadas por um dispositivo existente.
        device_updates = {
            envelope.device_id: (envelope.device_id, 0, envelope.status, envelope.ip, 0, 0, now)
            for envelope in accepted
        }
        device_update_rows = [(now, row[2], row[0]) for row in device_updates.values()]
        metric_rows = [
            (sample.device_id, sample.timestamp, sample.metric_name, sample.value, sample.unit)
            for envelope in accepted
            for sample in envelope.metrics
        ]
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

            for table_name, rows in build_rollup_rows(metric_rows, ROLLUP_TABLES).items():
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

    log.debug(
        "Batch de telemetria persistido: %d payload(s), %d métrica(s).",
        len(batch), len(metric_rows),
    )


def enqueue_telemetry(payload: messages_pb2.DataPayload, ip: str) -> bool:
    """Enfileira sem bloquear e aplica backpressure descartando sobrecarga."""
    global _TELEMETRY_DROPPED

    envelope = build_telemetry_envelope(payload, ip)
    queue = get_telemetry_queue()

    try:
        queue.put_nowait(envelope)
    except asyncio.QueueFull:
        _TELEMETRY_DROPPED += 1
        if _TELEMETRY_DROPPED == 1 or _TELEMETRY_DROPPED % 100 == 0:
            log.warning(
                "Fila de telemetria saturada; %d pacote(s) descartado(s) "
                "para preservar a memória (limite=%d).",
                _TELEMETRY_DROPPED,
                TELEMETRY_QUEUE_MAXSIZE,
            )
        return False

    return True


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

            retry_delay = 0.25
            while True:
                try:
                    await persist_telemetry_batch(batch)
                    break
                except Exception as exc:
                    # Manter o lote local evita perder pacotes admitidos durante
                    # falhas transitórias; a fila continua limitada por backpressure.
                    log.warning("Falha ao persistir telemetria; repetindo em %.2fs: %s",
                                retry_delay, exc)
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(5.0, retry_delay * 2)
        except asyncio.CancelledError:
            if batch:
                await persist_telemetry_batch(batch)
            raise
        finally:
            for _ in batch:
                queue.task_done()


async def process_discovery(disc: messages_pb2.DiscoveryResponse, ip: str):
    """Registra ou renova a presença de nós operacionais assincronamente."""
    now = int(time.time())

    device_id = disc.device_id.strip()
    announced = disc.ip_address.strip()[:255] if disc.ip_address else ""

    if not device_id or len(device_id) > 128:
        log.warning("Descoberta rejeitada de %s: device_id ausente ou longo demais.", ip)
        return

    if disc.is_controllable and not 1 <= disc.control_port <= 65535:
        log.warning(
            "Descoberta rejeitada de %s para '%s': porta de controle inválida (%d).",
            ip, device_id, disc.control_port,
        )
        return

    # O endereço declarado é dado não confiável. Usar a origem do datagrama evita
    # que uma descoberta induza o gateway a abrir conexão para um terceiro host.
    effective_ip = ip
    control_port = disc.control_port if disc.is_controllable else 0

    async with get_db_pool().connection() as db:
        async with db.execute(
            "SELECT 1 FROM devices WHERE device_id = ?",
            (device_id,),
        ) as cursor:
            existing_device = await cursor.fetchone()

        await db.execute("""
            INSERT INTO devices
            (device_id, type, status, ip_address, control_port, is_controllable, last_seen)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(device_id) DO UPDATE SET
                type            = excluded.type,
                status          = excluded.status,
                ip_address      = excluded.ip_address,
                control_port    = excluded.control_port,
                is_controllable = excluded.is_controllable,
                last_seen       = excluded.last_seen
        """, (device_id, disc.type, disc.initial_status, effective_ip,
              control_port, int(disc.is_controllable), now))
        await db.commit()

    if existing_device is None:
        log.info(
            "Nó registrado: '%s' — rede=%s anunciado='%s' porta=%d (controlável=%s).",
            device_id, ip, announced or "<não informado>",
            control_port, disc.is_controllable,
        )
    else:
        log.debug(
            "Heartbeat/topologia renovado: '%s' — rede=%s anunciado='%s' porta=%d.",
            device_id, ip, announced or "<não informado>", control_port,
        )


# ====================================================================
# CAMADA DE REDE: INGESTÃO E DESCOBERTA (MULTIPLEXAÇÃO FÍSICA UDP)
# ====================================================================

class TelemetryUDPProtocol(asyncio.DatagramProtocol):
    """Protocolo de transporte focado estritamente na ingestão contínua (Porta 5000)."""

    def connection_made(self, transport):
        self.transport = transport
        log.info("Interface de Telemetria ativa na porta %d.", UDP_TELEMETRY_PORT)

    def datagram_received(self, data: bytes, addr: Tuple[str, int]):
        """Decodifica estritamente fluxos operacionais DataPayload."""

        if not data or len(data) > UDP_MAX_DATAGRAM_BYTES:
            log.warning(
                "Datagrama de telemetria rejeitado de %s: %d bytes (limite=%d).",
                addr,
                len(data),
                UDP_MAX_DATAGRAM_BYTES,
            )
            return

        # Tenta extrair datagramas de Telemetria (Métricas Físicas)
        try:
            payload = messages_pb2.DataPayload()
            payload.ParseFromString(data)

            validation_error = validate_telemetry_payload(payload)
            if validation_error:
                log.debug(
                    "Telemetria rejeitada de %s: %s.", addr, validation_error
                )
                return

            # A fila limitada é a fronteira de backpressure. Não criamos uma
            # task por datagrama, pois tasks bloqueadas também consumiriam
            # memória quando o SQLite não acompanhasse a taxa de entrada.
            if not enqueue_telemetry(payload, addr[0]):
                return

            # A deduplicação acontece na transação de persistência, inclusive
            # após reinicialização e depois de uma falha de escrita.
            log.debug(
                "Pacote ID [%s] de '%s' — atraso %ds.",
                payload.message_id, payload.device_id,
                int(time.time()) - payload.timestamp,
            )
            return
        except Exception as exc:
            log.debug("Datagrama de telemetria inválido de %s: %s", addr, exc)


class DiscoveryUDPProtocol(asyncio.DatagramProtocol):
    """Protocolo de transporte focado no registro de topologia (Porta 5002)."""

    def connection_made(self, transport):
        self.transport = transport
        log.info("Interface de Descoberta ativa na porta %d.", UDP_DISCOVERY_PORT)

    def datagram_received(self, data: bytes, addr: Tuple[str, int]):
        """Decodifica estritamente fluxos de handshake e heartbeat."""
        global _DISCOVERY_DROPPED

        if not data or len(data) > UDP_MAX_DATAGRAM_BYTES:
            log.warning(
                "Datagrama de descoberta rejeitado de %s: %d bytes (limite=%d).",
                addr,
                len(data),
                UDP_MAX_DATAGRAM_BYTES,
            )
            return

        try:
            disc = messages_pb2.DiscoveryResponse()
            disc.ParseFromString(data)

            validation_error = validate_discovery_payload(disc)
            if validation_error:
                log.debug(
                    "Descoberta rejeitada de %s: %s.", addr, validation_error
                )
                return

            if len(_BACKGROUND_TASKS) >= DISCOVERY_MAX_IN_FLIGHT:
                _DISCOVERY_DROPPED += 1
                if _DISCOVERY_DROPPED == 1 or _DISCOVERY_DROPPED % 100 == 0:
                    log.warning(
                        "Limite de descobertas concorrentes atingido; "
                        "%d pacote(s) descartado(s) (limite=%d).",
                        _DISCOVERY_DROPPED,
                        DISCOVERY_MAX_IN_FLIGHT,
                    )
                return

            task = asyncio.create_task(process_discovery(disc, addr[0]))
            _BACKGROUND_TASKS.add(task)
            task.add_done_callback(_BACKGROUND_TASKS.discard)
        except Exception as e:
            log.error("Falha na decodificação de datagrama de Descoberta %s: %s", addr, e)


async def multicast_discovery_probe_loop():
    """Solicita periodicamente que sensores reanunciem a própria topologia."""
    await asyncio.sleep(2.0)

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, MULTICAST_TTL)

            while True:
                try:
                    sock.sendto(DISCOVERY_PROBE_PAYLOAD, (MULTICAST_GROUP, MULTICAST_PORT))
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
            async with get_db_pool().connection() as db:
                cursor = await db.execute("""
                    UPDATE devices
                    SET status = ?
                    WHERE last_seen < ? AND status != ?
                """, (messages_pb2.STATUS_OFF, cutoff, messages_pb2.STATUS_OFF))
                await db.commit()
                marked_offline = cursor.rowcount

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

                # Uma mensagem expirada já é recusada na entrada. O checkpoint
                # de ordem por dispositivo permanece mesmo após limpar os IDs.
                if MESSAGE_MAX_AGE_SECS > 0:
                    cursor = await db.execute(
                        "DELETE FROM telemetry_messages WHERE timestamp < ?",
                        (now - MESSAGE_MAX_AGE_SECS,),
                    )
                    deleted_parts.append(f"message_ids={cursor.rowcount}")

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


@dataclass(slots=True)
class OlapQueryResult:
    success: bool
    message: str
    analytics_result: float
    result_metadata: str
    graph_rows: list[tuple[int, float, str]]
    sample_count: int


def choose_olap_source(start_timestamp: int, end_timestamp: int) -> OlapSource:
    return select_olap_source(
        start_timestamp,
        end_timestamp,
        OLAP_RAW_MAX_WINDOW_SECS,
        OLAP_1M_MAX_WINDOW_SECS,
        OLAP_5M_MAX_WINDOW_SECS,
        now_timestamp=int(time.time()),
        raw_retention_secs=METRICS_RAW_RETENTION_SECS,
        rollup_1m_retention_secs=ROLLUP_1M_RETENTION_SECS,
        rollup_5m_retention_secs=ROLLUP_5M_RETENTION_SECS,
        rollup_1h_retention_secs=ROLLUP_1H_RETENTION_SECS,
    )


async def fetch_graph_rows(
    db,
    source: OlapSource,
    metric_name: str,
    start_timestamp: int,
    end_timestamp: int,
    target_device_id: str = "",
) -> tuple[list[tuple[int, float, str]], int]:
    """Retorna uma série limitada e o total de pontos disponíveis.

    Quando a consulta excede ``OLAP_MAX_GRAPH_POINTS``, o banco calcula uma
    posição estável por ``timestamp/device/id`` e devolve uma amostra uniforme
    que sempre preserva os extremos. Assim, a memória e o frame TCP ficam
    limitados sem carregar toda a série em Python.
    """

    query_start, query_end = olap_time_range(source, start_timestamp, end_timestamp)

    params: list = [metric_name, query_start, query_end]
    device_filter = ""
    if target_device_id:
        device_filter = "AND device_id = ?"
        params.append(target_device_id)

    if source.is_rollup:
        time_expression = "bucket_start"
        value_expression = "value_sum / sample_count"
        order_expression = "bucket_start ASC, device_id ASC"
    else:
        time_expression = "timestamp"
        value_expression = "value"
        # ``id`` desempata leituras do mesmo dispositivo no mesmo segundo.
        order_expression = "timestamp ASC, device_id ASC, id ASC"

    where_clause = (
        f"metric_name = ? AND {time_expression} BETWEEN ? AND ? {device_filter}"
    )
    async with db.execute(
        f"SELECT COUNT(*) FROM {source.table_name} WHERE {where_clause}",
        params,
    ) as cursor:
        count_row = await cursor.fetchone()

    total_points = int(count_row[0] or 0)
    if total_points == 0:
        return [], 0

    if total_points <= OLAP_MAX_GRAPH_POINTS:
        query = f"""
            SELECT {time_expression}, {value_expression}, device_id
            FROM {source.table_name}
            WHERE {where_clause}
            ORDER BY {order_expression}
            LIMIT ?
        """
        query_params = [*params, OLAP_MAX_GRAPH_POINTS]
    else:
        stride = graph_sampling_stride(total_points, OLAP_MAX_GRAPH_POINTS)
        last_index = total_points - 1
        query = f"""
            WITH ordered_points AS (
                SELECT
                    {time_expression} AS point_timestamp,
                    {value_expression} AS point_value,
                    device_id,
                    ROW_NUMBER() OVER (ORDER BY {order_expression}) - 1 AS sample_index
                FROM {source.table_name}
                WHERE {where_clause}
            )
            SELECT point_timestamp, point_value, device_id
            FROM ordered_points
            WHERE
                (sample_index < ? AND sample_index % ? = 0)
                OR sample_index = ?
            ORDER BY sample_index ASC
            LIMIT ?
        """
        query_params = [
            *params,
            last_index,
            stride,
            last_index,
            OLAP_MAX_GRAPH_POINTS,
        ]

    async with db.execute(query, query_params) as cursor:
        rows = [
            (int(ts), float(value), str(device_id))
            for ts, value, device_id in await cursor.fetchall()
        ]
    return rows, total_points


async def execute_olap_from_source(
    db,
    req: messages_pb2.ClientRequest,
    source: OlapSource,
) -> OlapQueryResult:
    # Queries construídas dinamicamente com base em (is_rollup × target_device_id).
    query_start, query_end = olap_time_range(source, req.start_timestamp, req.end_timestamp)
    window_label = ""
    if source.is_rollup:
        effective_end = query_end + source.bucket_size - 1
        if query_start != req.start_timestamp or effective_end != req.end_timestamp:
            try:
                start_label = datetime.fromtimestamp(query_start, timezone.utc).isoformat()
                end_label = datetime.fromtimestamp(effective_end, timezone.utc).isoformat()
            except (ValueError, OverflowError, OSError):
                start_label, end_label = str(query_start), str(effective_end)
            window_label = (
                f" Janela agregada: {start_label} até {end_label}. "
                "As agregações das bordas podem incluir amostras fora do período solicitado."
            )

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

        graph_rows, graph_total_points = await fetch_graph_rows(
            db, source, req.query_metric, req.start_timestamp, req.end_timestamp, req.target_device_id,
        )
        bucket_label = f", bucket={source.bucket_size}s" if source.is_rollup else ""
        graph_label = (
            f"Pontos retornados: {len(graph_rows)} de {graph_total_points} (amostrados)."
            if graph_total_points > len(graph_rows)
            else f"Pontos retornados: {len(graph_rows)}."
        )
        return OlapQueryResult(
            success=True,
            message="",
            analytics_result=analytics_result,
            result_metadata=(
                f"Fonte OLAP: {source.name}{bucket_label}. "
                f"Amostras processadas: {sample_count}. {graph_label}{window_label}"
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
        graph_rows, graph_total_points = await fetch_graph_rows(
            db, source, req.query_metric, req.start_timestamp, req.end_timestamp, req.target_device_id,
        )
        bucket_label = f", bucket={source.bucket_size}s" if source.is_rollup else ""
        graph_label = (
            f"{len(graph_rows)} de {graph_total_points} pontos de gráfico amostrados."
            if graph_total_points > len(graph_rows)
            else f"{len(graph_rows)} pontos de gráfico retornados."
        )
        return OlapQueryResult(
            success=True,
            message="",
            analytics_result=max_variation,
            result_metadata=(
                f"Fonte OLAP: {source.name}{bucket_label}. "
                f"Maior variação: dispositivo {best_dev} — "
                f"{len(variation_rows)} nós avaliados, {sample_count} amostras. "
                f"{graph_label}{window_label}"
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


async def build_client_response(
    req: messages_pb2.ClientRequest,
    peer,
) -> messages_pb2.ClientResponse:
    """Processa uma requisição já desserializada e preserva o contrato ClientResponse."""
    resp = new_client_response(success=True)
    resp.message_id = req.message_id

    validation_error = validate_client_request(req)
    if validation_error:
        resp.success = False
        resp.message = f"Requisição rejeitada: {validation_error}."
        log.warning("Requisição TCP inválida de %s: %s.", peer, validation_error)
        return resp

    # ── Rota: Sincronização de Inventário ────────────────────────────
    if req.type == messages_pb2.REQUEST_TYPE_LIST_DEVICES:
        async with get_db_pool().connection() as db:
            # [R1] Colunas explícitas — resistente a mudanças futuras de schema
            async with db.execute("""
                SELECT device_id, type, status, ip_address,
                       control_port, is_controllable, last_seen
                FROM devices
            """) as cursor:
                async for row in cursor:
                    device_id, dtype, status, ip, ctrl_port, is_ctrl, last_seen = row
                    d = resp.devices.add()
                    d.device_id           = device_id
                    d.type                = dtype
                    d.status              = status
                    d.ip_address          = ip
                    d.control_port        = ctrl_port
                    d.is_controllable     = bool(is_ctrl)
                    d.last_seen_timestamp = last_seen

        resp.message = "Sincronização de topologia extraída via pool aiosqlite."
        log.info("LIST_DEVICES → %d nós retornados para %s.", len(resp.devices), peer)
        return resp

    # ── Rota: Proxy de Atuação Remota ────────────────────────────────
    if req.type == messages_pb2.REQUEST_TYPE_SEND_COMMAND:
        async with get_db_pool().connection() as db:
            async with db.execute(
                "SELECT ip_address, control_port FROM devices WHERE device_id = ?",
                (req.target_device_id,),
            ) as cursor:
                target = await cursor.fetchone()

        if not target or target[1] <= 0:
            resp.success = False
            resp.message = "Nó não encontrado ou desprovido de porta de controle."
            return resp

        s_w = None
        try:
            s_r, s_w = await asyncio.wait_for(
                asyncio.open_connection(target[0], target[1]),
                timeout=5.0,
            )

            # Aplica Framing no envio do comando para os Atuadores (Lua/Java/Python)
            req.command_payload.target_device_id = req.target_device_id
            cmd_bytes = req.command_payload.SerializeToString()
            s_w.write(struct.pack(">I", len(cmd_bytes)) + cmd_bytes)
            await s_w.drain()

            # Lê a resposta binária do nó alvo respeitando a janela de Framing
            s_header = await asyncio.wait_for(s_r.readexactly(4), timeout=5.0)
            s_len    = struct.unpack(">I", s_header)[0]
            if s_len <= 0 or s_len > TCP_MAX_FRAME_BYTES:
                raise ValueError(f"frame de resposta inválido do nó alvo: {s_len} bytes")
            s_body   = await asyncio.wait_for(s_r.readexactly(s_len), timeout=5.0)

            node_resp = messages_pb2.ConfigResponse()
            node_resp.ParseFromString(s_body)
            if node_resp.command_id != req.command_payload.command_id:
                raise ValueError("command_id da resposta não corresponde ao comando enviado")
            response_error = validate_ingress_timestamp(node_resp.timestamp, "response.timestamp")
            if response_error:
                raise ValueError(response_error)
            if node_resp.success and (
                int(node_resp.updated_status) not in VALID_DEVICE_STATUSES
                or not 1 <= node_resp.updated_frequency_secs <= 60
            ):
                raise ValueError("estado ou frequência inválidos na confirmação do comando")
            if node_resp.success and (
                (req.command_payload.update_status
                 and node_resp.updated_status != req.command_payload.target_status)
                or (req.command_payload.update_frequency
                    and node_resp.updated_frequency_secs != req.command_payload.new_frequency_secs)
            ):
                raise ValueError("confirmação do nó não contém as alterações solicitadas")

            resp.success, resp.message = node_resp.success, node_resp.message
            log.info(
                "SEND_COMMAND '%s' → '%s': sucesso=%s.",
                req.command_payload.command_id, req.target_device_id, resp.success,
            )
        except asyncio.TimeoutError:
            resp.success = False
            resp.message = "Timeout de I/O com o nó alvo durante atuação remota."
            log.warning("Timeout ao encaminhar comando para '%s'.", req.target_device_id)
        except ConnectionRefusedError as exc:
            resp.success = False
            resp.message = f"Nó alvo recusou a conexão TCP: {exc}"
            log.warning("Conexão recusada por '%s': %s", req.target_device_id, exc)
        except (asyncio.IncompleteReadError, struct.error, DecodeError, ValueError) as exc:
            resp.success = False
            resp.message = f"Resposta TCP/Protobuf inválida do nó alvo: {exc}"
            log.warning("Frame inválido de '%s': %s", req.target_device_id, exc)
        except OSError as exc:
            resp.success = False
            resp.message = f"Falha de socket com o nó alvo: {exc}"
            log.warning("Falha de socket ao encaminhar comando para '%s': %s", req.target_device_id, exc)
        finally:
            if s_w is not None:
                s_w.close()
                with suppress(OSError):
                    await s_w.wait_closed()

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
    task = asyncio.current_task()
    _CLIENT_TASKS.add(task)
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
            resp.message_id = req.message_id
            await send_client_response(writer, resp)

    except (ConnectionResetError, BrokenPipeError) as exc:
        log.warning("Conexão TCP interrompida por %s: %s", peer, exc)
    except OSError as exc:
        log.warning("Erro de socket no handler TCP (%s): %s", peer, exc)
    finally:
        _CLIENT_TASKS.discard(task)
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()


# ====================================================================
# INICIALIZAÇÃO DO LOOP DE EVENTOS E KERNEL DE BORDAS
# ====================================================================

async def stop_telemetry_worker(task: asyncio.Task):
    """Drena os pacotes já admitidos antes de encerrar o consumidor."""
    drained = True
    try:
        await asyncio.wait_for(get_telemetry_queue().join(), TELEMETRY_SHUTDOWN_TIMEOUT_SECS)
    except asyncio.TimeoutError:
        drained = False
        log.error("Tempo limite de %.1fs ao drenar telemetria; há pacotes sem confirmação de gravação.",
                  TELEMETRY_SHUTDOWN_TIMEOUT_SECS)
    task.cancel()
    results = await asyncio.gather(task, return_exceptions=True)
    for result in results:
        if isinstance(result, Exception):
            drained = False
            log.error("Falha na última gravação de telemetria durante encerramento: %s", result)
    return drained


async def main():
    global DB_POOL, TELEMETRY_QUEUE

    log.info("============================================================")
    log.info("Inicializando nó central (aiosqlite / Framing Distribuído)...")
    log.info("LOG_LEVEL=%s | TCP_READ_TIMEOUT=%.1fs | TCP_IDLE_TIMEOUT=%.1fs | WAL_CHECKPOINT=%ds",
             _LOG_LEVEL_STR, TCP_CLIENT_READ_TIMEOUT, TCP_CLIENT_IDLE_TIMEOUT,
             WAL_CHECKPOINT_INTERVAL_SECS)
    log.info("OFFLINE_TIMEOUT=%.1fs | OFFLINE_CHECK=%.1fs",
             DEVICE_OFFLINE_TIMEOUT_SECS, DEVICE_OFFLINE_CHECK_INTERVAL_SECS)
    log.info(
        "OLAP raw<=%ds | 1m<=%ds | 5m<=%ds | query<=%ds | graph<=%d pontos",
        OLAP_RAW_MAX_WINDOW_SECS, OLAP_1M_MAX_WINDOW_SECS, OLAP_5M_MAX_WINDOW_SECS,
        OLAP_MAX_QUERY_WINDOW_SECS, OLAP_MAX_GRAPH_POINTS,
    )
    log.info(
        "INGRESS UDP<=%d bytes | métricas<=%d | idade<=%ds | futuro<=%ds | batch=%d payloads/%d rows",
        UDP_MAX_DATAGRAM_BYTES, MAX_METRICS_PER_PAYLOAD, MESSAGE_MAX_AGE_SECS,
        MESSAGE_MAX_FUTURE_SKEW_SECS, TELEMETRY_BATCH_MAX_PAYLOADS,
        TELEMETRY_BATCH_MAX_ROWS,
    )
    log.info("============================================================")

    init_db()
    DB_POOL = SQLiteConnectionPool(DB_FILE, DB_POOL_SIZE)
    await DB_POOL.start()
    TELEMETRY_QUEUE = asyncio.Queue(maxsize=TELEMETRY_QUEUE_MAXSIZE)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(shutdown_signal, stop_event.set)
        except NotImplementedError:
            # O loop Proactor do Windows não implementa add_signal_handler.
            signal.signal(shutdown_signal,
                          lambda *_: loop.call_soon_threadsafe(stop_event.set))

    # Provisionando as instâncias de transporte baseadas na segregação de portas
    telemetry_transport, _ = await loop.create_datagram_endpoint(
        TelemetryUDPProtocol, local_addr=("0.0.0.0", UDP_TELEMETRY_PORT)
    )
    discovery_transport, _ = await loop.create_datagram_endpoint(
        DiscoveryUDPProtocol, local_addr=("0.0.0.0", UDP_DISCOVERY_PORT)
    )

    # Servidor de Controle TCP
    server = await asyncio.start_server(handle_client_request, "0.0.0.0", TCP_PORT)

    # Tasks de background
    probe_task      = asyncio.create_task(multicast_discovery_probe_loop())
    checkpoint_task = asyncio.create_task(wal_checkpoint_loop())  # [M3]
    offline_task    = asyncio.create_task(device_offline_monitor_loop())
    telemetry_task  = asyncio.create_task(telemetry_batch_worker_loop())
    retention_task  = asyncio.create_task(metrics_retention_loop())

    log.info(
        "Hub pronto. TCP:%d | UDP(Telem):%d | UDP(Disc):%d",
        TCP_PORT, UDP_TELEMETRY_PORT, UDP_DISCOVERY_PORT,
    )

    try:
        await stop_event.wait()
    finally:
        # Interromper a admissão primeiro permite que join() alcance a fila vazia.
        server.close()
        telemetry_transport.close()
        discovery_transport.close()
        active_tasks = list(_BACKGROUND_TASKS | _CLIENT_TASKS)
        for task in active_tasks:
            task.cancel()
        if active_tasks:
            await asyncio.gather(*active_tasks, return_exceptions=True)
        await server.wait_closed()

        probe_task.cancel()
        checkpoint_task.cancel()
        offline_task.cancel()
        retention_task.cancel()
        await asyncio.gather(
            probe_task, checkpoint_task, offline_task, retention_task,
            return_exceptions=True,
        )
        await stop_telemetry_worker(telemetry_task)
        await DB_POOL.close()
        DB_POOL = None
        TELEMETRY_QUEUE = None
        log.info("Gateway encerrado.")


if __name__ == "__main__":
    asyncio.run(main())
