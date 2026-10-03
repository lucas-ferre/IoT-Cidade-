"""Validação e transporte independentes do banco e do processo gateway."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from dataclasses import dataclass
import ipaddress
import json
import math
import socket
import struct
import time

import messages_pb2 as pb


@dataclass(frozen=True)
class Limits:
    max_frame: int = 1048576
    max_udp: int = 16384
    message_age: int = 86400
    future_skew: int = 300
    request_timeout: float = 10.0
    max_clients: int = 128
    rate: float = 80.0
    burst: float = 200.0
    device_ttl: float = 120.0
    max_devices: int = 2048


def text_error(value: str, field: str, limit: int = 128, required: bool = True) -> str | None:
    if (required and not value) or value != value.strip() or len(value) > limit:
        return f"invalid_{field}"
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        return f"invalid_{field}"
    return None


def timestamp_error(timestamp: int, limits: Limits, *, command: bool = False) -> str | None:
    now = int(time.time())
    age = min(limits.message_age, 300) if command else limits.message_age
    skew = min(limits.future_skew, 30) if command else limits.future_skew
    if timestamp <= 0 or timestamp > now + skew or (age and timestamp < now - age):
        return "invalid_timestamp"
    return None


def envelope_error(message, limits: Limits) -> str | None:
    return text_error(message.message_id, "message_id") or timestamp_error(message.timestamp, limits)


def telemetry_error(message, limits: Limits) -> str | None:
    error = envelope_error(message, limits) or text_error(message.device_id, "device_id")
    if error:
        return error
    if message.current_status not in (1, 2, 3):
        return "invalid_status"
    if len(message.metrics) > 64:
        return "too_many_metrics"
    names = set()
    for metric in message.metrics:
        error = text_error(metric.name, "metric_name") or text_error(metric.unit, "metric_unit", 32, False)
        if error:
            return error
        if not math.isfinite(metric.value):
            return "non_finite_metric"
        if metric.name in names:
            return "duplicate_metric"
        names.add(metric.name)
    return None


def discovery_error(message, limits: Limits) -> str | None:
    error = envelope_error(message, limits) or text_error(message.device_id, "device_id")
    if error:
        return error
    if message.initial_status not in (1, 2, 3):
        return "invalid_status"
    return text_error(message.ip_address, "ip_address", 255, False)


def command_error(command, limits: Limits) -> str | None:
    error = text_error(command.command_id, "command_id") or text_error(command.target_device_id, "target_device_id")
    if error:
        return error
    error = timestamp_error(command.timestamp, limits, command=True)
    if error:
        return error
    if not (command.update_status or command.update_frequency):
        return "empty_command"
    if command.update_status and command.target_status not in (1, 2):
        return "invalid_target_status"
    if command.update_frequency and not 1 <= command.new_frequency_secs <= 60:
        return "invalid_frequency"
    return None


def request_error(request, limits: Limits) -> str | None:
    error = envelope_error(request, limits)
    if error:
        return error
    if request.type not in (1, 2, 3):
        return "invalid_request_type"
    if request.type == 2:
        error = text_error(request.target_device_id, "target_device_id")
        if error:
            return error
        if not request.HasField("command_payload"):
            return "missing_command"
        if request.target_device_id != request.command_payload.target_device_id:
            return "target_mismatch"
        return command_error(request.command_payload, limits)
    if request.type == 3:
        error = text_error(request.query_metric, "query_metric") or text_error(request.target_device_id, "target_device_id", required=False)
        if error:
            return error
        if request.query_op not in (1, 2, 3):
            return "invalid_query_op"
        if request.start_timestamp <= 0 or request.end_timestamp < request.start_timestamp:
            return "invalid_query_window"
        if request.end_timestamp - request.start_timestamp > 2592000:
            return "query_window_too_large"
    return None


async def read_frame(reader: asyncio.StreamReader, limits: Limits) -> bytes | None:
    try:
        header = await asyncio.wait_for(reader.readexactly(4), limits.request_timeout)
    except asyncio.IncompleteReadError as exc:
        if not exc.partial:
            return None
        raise ValueError("truncated_frame") from exc
    size = struct.unpack("!I", header)[0]
    if not 0 < size <= limits.max_frame:
        raise ValueError("invalid_frame_size")
    try:
        return await asyncio.wait_for(reader.readexactly(size), limits.request_timeout)
    except asyncio.IncompleteReadError as exc:
        raise ValueError("truncated_frame") from exc


async def write_frame(writer: asyncio.StreamWriter, message, limits: Limits) -> None:
    data = message if isinstance(message, bytes) else message.SerializeToString()
    if not 0 < len(data) <= limits.max_frame:
        raise ValueError("invalid_frame_size")
    writer.write(struct.pack("!I", len(data)) + data)
    await asyncio.wait_for(writer.drain(), limits.request_timeout)


async def close_writer(writer: asyncio.StreamWriter) -> None:
    writer.close()
    try:
        await asyncio.wait_for(writer.wait_closed(), 1.0)
    except (OSError, asyncio.TimeoutError):
        pass


class RateLimiter:
    """Token buckets com estado limitado, independentemente dos IPs recebidos."""

    def __init__(self, rate: float, burst: float, max_sources: int = 512):
        self.rate = rate
        self.burst = burst
        self.max_sources = max_sources
        self.sources = OrderedDict()

    def allow(self, source: str) -> bool:
        now = time.monotonic()
        tokens, previous = self.sources.pop(source, (self.burst, now))
        tokens = min(self.burst, tokens + max(0, now - previous) * self.rate)
        allowed = tokens >= 1.0
        self.sources[source] = (tokens - 1.0 if allowed else tokens, now)
        while len(self.sources) > self.max_sources:
            self.sources.popitem(last=False)
        return allowed


class HostDirectory:
    """Resolve somente hosts de configuração; jamais resolve um endereço do pacote."""

    def __init__(self, hosts, *, fixed: dict[str, set[str]] | None = None, refresh: float = 15.0):
        self.hosts = tuple(dict.fromkeys(hosts))
        self.fixed = fixed
        self.refresh = refresh
        self.addresses: dict[str, set[str]] = {}
        self.updated: dict[str, float] = {}

    async def update(self):
        if self.fixed is not None:
            self.addresses = {host: set(addresses) for host, addresses in self.fixed.items()}
            self.updated = {host: time.monotonic() for host in self.addresses}
            return
        loop = asyncio.get_running_loop()
        for host in self.hosts:
            try:
                address = str(ipaddress.IPv4Address(host))
                results = {address}
            except ipaddress.AddressValueError:
                try:
                    infos = await asyncio.wait_for(loop.getaddrinfo(host, None, family=socket.AF_INET, type=socket.SOCK_STREAM), 2.0)
                    results = {info[4][0] for info in infos}
                except (OSError, asyncio.TimeoutError):
                    # Nunca prolonga indefinidamente uma autorização DNS antiga.
                    if time.monotonic() - self.updated.get(host, 0) > self.refresh * 3:
                        self.addresses.pop(host, None)
                    continue
            self.addresses[host] = results
            self.updated[host] = time.monotonic()

    def permits(self, host: str, source: str) -> bool:
        return source in self.addresses.get(host, ())

    def address(self, host: str) -> str:
        addresses = self.addresses.get(host)
        if not addresses:
            raise ConnectionError("configured_host_unavailable")
        return sorted(addresses)[0]


class Audit:
    def __init__(self, hub_id: str, sink=None):
        self.hub_id = hub_id
        self.sink = sink or (lambda entry: print(json.dumps(entry, ensure_ascii=False), flush=True))
        self.counts: dict[str, int] = {}

    def record(self, event: str, reason: str, source: str = "", device_id: str = ""):
        key = f"{event}:{reason}"
        self.counts[key] = self.counts.get(key, 0) + 1
        # Evita que uma rajada de pacotes inválidos transforme stdout no gargalo.
        # Os contadores continuam exatos; as primeiras ocorrências e amostras
        # seguintes preservam a evidência para auditoria.
        count = self.counts[key]
        if count > 5 and count % 100:
            return
        self.sink({"timestamp": int(time.time()), "hub_id": self.hub_id, "event": event,
                   "reason": reason, "source_ip": source, "device_id": device_id[:128],
                   "count": count})
