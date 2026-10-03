"""Dois hubs que filtram a entrada antes de encaminhá-la ao gateway isolado."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import os
import re
import signal
import socket
import sys
import time
import uuid

from google.protobuf.message import DecodeError

from .common import (Audit, HostDirectory, Limits, RateLimiter, close_writer, command_error,
                     discovery_error, pb, read_frame, request_error, telemetry_error,
                     text_error, timestamp_error, write_frame)


@dataclass(frozen=True)
class SensorProfile:
    prefix: str
    host: str
    device_type: int
    control_port: int


DEFAULT_PROFILES = (
    SensorProfile("estacao_", "sensor_clima", 3, 0),
    SensorProfile("poste_", "sensor_posto", 2, 5006),
    SensorProfile("semaforo_", "sensor_java", 1, 5003),
    SensorProfile("camera_", "sensor_camera", 4, 5004),
    SensorProfile("parking_", "sensor_estacionamento", 6, 5007),
    SensorProfile("water_", "sensor_agua", 7, 5008),
    SensorProfile("waste_", "sensor_lixeiras", 8, 0),
)


@dataclass
class DeviceRoute:
    source_ip: str
    control_port: int
    host: str
    expires_at: float


class DatagramIngress(asyncio.DatagramProtocol):
    def __init__(self, hub, kind):
        self.hub = hub
        self.kind = kind

    def datagram_received(self, data, address):
        self.hub.receive_datagram(self.kind, data, address)

    def error_received(self, error):
        self.hub.audit.record("error", "udp_transport_error")


class HubBase:
    def __init__(self, hub_id, *, gateway_host="gateway", directory=None,
                 limits=None, bind_host="0.0.0.0", audit_sink=None):
        self.gateway_host = gateway_host
        self.directory = directory
        self.limits = limits or Limits()
        self.bind_host = bind_host
        self.audit = Audit(hub_id, audit_sink)
        self.limiter = RateLimiter(self.limits.rate, self.limits.burst)
        self.servers = []
        self.transports = []
        self.tasks = set()
        self.clients = set()
        self.active_clients = 0
        self.closing = False

    def spawn(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def maintenance(self):
        while True:
            await asyncio.sleep(self.directory.refresh)
            await self.directory.update()
            self.audit.sink({"timestamp": int(time.time()), "hub_id": self.audit.hub_id,
                             "event": "counters", "counters": dict(self.audit.counts),
                             "active_clients": self.active_clients})

    async def close(self):
        self.closing = True
        for server in self.servers:
            server.close()
        for transport in self.transports:
            transport.close()
        pending = list(self.tasks)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for writer in list(self.clients):
            await close_writer(writer)
        # Python 3.12+ espera também o encerramento das conexões existentes.
        # Portanto os handlers e escritores precisam terminar antes deste wait.
        await asyncio.gather(*(server.wait_closed() for server in self.servers))

    def source_ip(self, writer):
        address = writer.get_extra_info("peername")
        return address[0] if address else ""

    def admit(self, writer, source_allowed):
        source = self.source_ip(writer)
        if self.closing:
            return False
        if not source_allowed:
            self.audit.record("rejected", "unauthorized_source", source)
            return False
        if self.active_clients >= self.limits.max_clients:
            self.audit.record("rejected", "client_capacity", source)
            return False
        if not self.limiter.allow("tcp:" + source):
            self.audit.record("rejected", "rate_limit", source)
            return False
        self.active_clients += 1
        self.clients.add(writer)
        return True

    def release(self, writer):
        self.active_clients -= 1
        self.clients.discard(writer)


class SensorHub(HubBase):
    def __init__(self, *, profiles=DEFAULT_PROFILES, telemetry_port=5000,
                 discovery_port=5002, control_port=5010, gateway_telemetry_port=5000,
                 gateway_discovery_port=5002, **kwargs):
        super().__init__("hub_sensores", **kwargs)
        self.profiles = tuple(profiles)
        self.directory = self.directory or HostDirectory([self.gateway_host] + [profile.host for profile in profiles])
        self.telemetry_port = telemetry_port
        self.discovery_port = discovery_port
        self.control_port = control_port
        self.gateway_telemetry_port = gateway_telemetry_port
        self.gateway_discovery_port = gateway_discovery_port
        self.routes: dict[str, DeviceRoute] = {}
        self.outgoing = None

    def profile_for(self, device_id):
        if not re.fullmatch(r"[a-z][a-z0-9_]{1,127}", device_id):
            return None
        return next((profile for profile in self.profiles if device_id.startswith(profile.prefix)), None)

    def receive_datagram(self, kind, data, address):
        source = address[0]
        if not self.limiter.allow("udp:" + source):
            self.audit.record("rejected", "rate_limit", source)
            return
        if not 0 < len(data) <= self.limits.max_udp:
            self.audit.record("rejected", "invalid_datagram_size", source)
            return
        message = pb.DiscoveryResponse() if kind == "discovery" else pb.DataPayload()
        try:
            message.ParseFromString(data)
        except DecodeError:
            self.audit.record("rejected", "malformed_protobuf", source)
            return
        error = discovery_error(message, self.limits) if kind == "discovery" else telemetry_error(message, self.limits)
        if error:
            self.audit.record("rejected", error, source, message.device_id)
            return
        profile = self.profile_for(message.device_id)
        if profile is None:
            self.audit.record("rejected", "unknown_device", source, message.device_id)
            return
        if not self.directory.permits(profile.host, source):
            self.audit.record("rejected", "source_identity_mismatch", source, message.device_id)
            return
        now = time.monotonic()
        if kind == "discovery":
            if (message.type != profile.device_type or message.control_port != profile.control_port
                    or message.is_controllable != bool(profile.control_port)):
                self.audit.record("rejected", "profile_mismatch", source, message.device_id)
                return
            self.routes = {device_id: route for device_id, route in self.routes.items() if route.expires_at > now}
            if message.device_id not in self.routes and len(self.routes) >= self.limits.max_devices:
                self.audit.record("rejected", "device_capacity", source, message.device_id)
                return
            route = DeviceRoute(source, profile.control_port, profile.host, now + self.limits.device_ttl)
            # O endereço anunciado é informativo. O endpoint é sempre a origem autorizada.
            message.ip_address = source
            if message.is_controllable:
                message.control_port = self.control_port
            payload = message.SerializeToString()
            destination_port = self.gateway_discovery_port
        else:
            route = self.routes.get(message.device_id)
            if route is None or route.expires_at <= now:
                self.audit.record("rejected", "device_not_discovered", source, message.device_id)
                return
            if route.source_ip != source:
                self.audit.record("rejected", "source_identity_mismatch", source, message.device_id)
                return
            payload = data
            destination_port = self.gateway_telemetry_port
        try:
            self.outgoing.sendto(payload, (self.directory.address(self.gateway_host), destination_port))
        except (OSError, ConnectionError, RuntimeError):
            self.audit.record("error", "gateway_unavailable", source, message.device_id)
            return
        if kind == "discovery":
            self.routes[message.device_id] = route
        self.audit.record("forwarded", kind, source, message.device_id)

    async def start(self):
        await self.directory.update()
        loop = asyncio.get_running_loop()
        self.outgoing, _ = await loop.create_datagram_endpoint(asyncio.DatagramProtocol, local_addr=(self.bind_host, 0))
        self.transports.append(self.outgoing)
        for kind, port_name in (("telemetry", "telemetry_port"), ("discovery", "discovery_port")):
            transport, _ = await loop.create_datagram_endpoint(lambda name=kind: DatagramIngress(self, name), local_addr=(self.bind_host, getattr(self, port_name)))
            self.transports.append(transport)
            setattr(self, port_name, transport.get_extra_info("sockname")[1])
        server = await asyncio.start_server(self.handle_control, self.bind_host, self.control_port, limit=self.limits.max_frame + 4)
        self.servers.append(server)
        self.control_port = server.sockets[0].getsockname()[1]
        self.spawn(self.maintenance())
        self.audit.record("started", "listeners_ready")

    async def control_failure(self, writer, command_id, reason):
        response = pb.ConfigResponse(message_id=uuid.uuid4().hex, command_id=command_id,
                                     timestamp=int(time.time()), success=False,
                                     message=f"hub_sensores: {reason}")
        await write_frame(writer, response, self.limits)

    async def handle_control(self, reader, writer):
        source = self.source_ip(writer)
        if not self.admit(writer, self.directory.permits(self.gateway_host, source)):
            await close_writer(writer)
            return
        task = asyncio.current_task()
        self.tasks.add(task)
        upstream = None
        command = pb.ConfigCommand()
        try:
            data = await read_frame(reader, self.limits)
            if data is None:
                return
            command.ParseFromString(data)
            error = command_error(command, self.limits)
            route = self.routes.get(command.target_device_id)
            if not error and (route is None or route.expires_at <= time.monotonic()):
                error = "unknown_or_expired_device"
            if not error and (not route.control_port or not self.directory.permits(route.host, route.source_ip)):
                error = "unavailable_control_endpoint"
            if error:
                self.audit.record("rejected", error, source, command.target_device_id)
                await self.control_failure(writer, command.command_id, error)
                return
            upstream_reader, upstream = await asyncio.wait_for(asyncio.open_connection(route.source_ip, route.control_port, limit=self.limits.max_frame + 4), self.limits.request_timeout)
            await write_frame(upstream, data, self.limits)
            response_data = await read_frame(upstream_reader, self.limits)
            if response_data is None:
                raise ConnectionError("missing_ack")
            response = pb.ConfigResponse()
            response.ParseFromString(response_data)
            if (response.command_id != command.command_id or text_error(response.message_id, "message_id")
                    or timestamp_error(response.timestamp, self.limits, command=True)):
                raise ValueError("ack_mismatch")
            if response.success:
                if command.update_status and response.updated_status != command.target_status:
                    raise ValueError("ack_status_mismatch")
                if command.update_frequency and response.updated_frequency_secs != command.new_frequency_secs:
                    raise ValueError("ack_frequency_mismatch")
            await write_frame(writer, response_data, self.limits)
            self.audit.record("forwarded", "control_ack", source, command.target_device_id)
        except (DecodeError, ValueError, OSError, ConnectionError, asyncio.TimeoutError) as error:
            reason = str(error) if isinstance(error, ValueError) and not isinstance(error, DecodeError) else "invalid_or_unavailable_control"
            self.audit.record("rejected", reason, source, command.target_device_id)
            try:
                await self.control_failure(writer, command.command_id[:128], reason)
            except (OSError, ConnectionError, ValueError, asyncio.TimeoutError):
                pass
        finally:
            try:
                if upstream:
                    await close_writer(upstream)
            finally:
                try:
                    await close_writer(writer)
                finally:
                    self.release(writer)
                    self.tasks.discard(task)


class AccessHub(HubBase):
    def __init__(self, *, allowed_hosts=("dashboard",), access_port=5001,
                 gateway_access_port=5001, **kwargs):
        super().__init__("hub_acesso", **kwargs)
        self.allowed_hosts = tuple(allowed_hosts)
        self.directory = self.directory or HostDirectory([self.gateway_host] + list(self.allowed_hosts))
        self.access_port = access_port
        self.gateway_access_port = gateway_access_port

    async def start(self):
        await self.directory.update()
        server = await asyncio.start_server(self.handle_client, self.bind_host, self.access_port, limit=self.limits.max_frame + 4)
        self.servers.append(server)
        self.access_port = server.sockets[0].getsockname()[1]
        self.spawn(self.maintenance())
        self.audit.record("started", "listener_ready")

    async def failure(self, writer, message_id, reason):
        response = pb.ClientResponse(message_id=message_id, timestamp=int(time.time()),
                                     success=False, message=f"hub_acesso: {reason}")
        await write_frame(writer, response, self.limits)

    async def handle_client(self, reader, writer):
        source = self.source_ip(writer)
        if not self.admit(writer, any(self.directory.permits(host, source) for host in self.allowed_hosts)):
            await close_writer(writer)
            return
        task = asyncio.current_task()
        self.tasks.add(task)
        upstream = None
        request = pb.ClientRequest()
        try:
            while True:
                request = pb.ClientRequest()
                data = await read_frame(reader, self.limits)
                if data is None:
                    break
                request.ParseFromString(data)
                if not self.limiter.allow("request:" + source):
                    self.audit.record("rejected", "rate_limit", source)
                    await self.failure(writer, request.message_id[:128], "rate_limit")
                    break
                error = request_error(request, self.limits)
                if error:
                    self.audit.record("rejected", error, source, request.target_device_id)
                    await self.failure(writer, request.message_id[:128], error)
                    continue
                if upstream is None:
                    upstream_reader, upstream = await asyncio.wait_for(asyncio.open_connection(self.directory.address(self.gateway_host), self.gateway_access_port, limit=self.limits.max_frame + 4), self.limits.request_timeout)
                await write_frame(upstream, data, self.limits)
                response_data = await read_frame(upstream_reader, self.limits)
                if response_data is None:
                    raise ConnectionError("gateway_closed")
                response = pb.ClientResponse()
                response.ParseFromString(response_data)
                if response.message_id != request.message_id:
                    raise ValueError("response_id_mismatch")
                await write_frame(writer, response_data, self.limits)
                self.audit.record("forwarded", "client_request", source, request.target_device_id)
        except (DecodeError, ValueError, OSError, ConnectionError, asyncio.TimeoutError) as error:
            reason = str(error) if isinstance(error, ValueError) and not isinstance(error, DecodeError) else "invalid_or_unavailable_request"
            self.audit.record("rejected", reason, source, request.target_device_id)
            try:
                await self.failure(writer, request.message_id[:128], reason)
            except (OSError, ConnectionError, ValueError, asyncio.TimeoutError):
                pass
        finally:
            try:
                if upstream:
                    await close_writer(upstream)
            finally:
                try:
                    await close_writer(writer)
                finally:
                    self.release(writer)
                    self.tasks.discard(task)


def number(name, default, minimum, maximum, converter=float):
    value = converter(os.getenv(name, str(default)))
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} fora do intervalo {minimum}..{maximum}")
    return value


def limits_from_env():
    return Limits(
        max_frame=number("HUB_TCP_MAX_FRAME_BYTES", 1048576, 64, 4194304, int),
        max_udp=number("HUB_UDP_MAX_BYTES", 16384, 64, 65507, int),
        message_age=number("HUB_MESSAGE_MAX_AGE_SECS", 86400, 1, 604800, int),
        future_skew=number("HUB_MESSAGE_MAX_FUTURE_SKEW_SECS", 300, 0, 600, int),
        request_timeout=number("HUB_REQUEST_TIMEOUT_SECS", 10, 0.1, 120),
        max_clients=number("HUB_MAX_CLIENTS", 128, 1, 1024, int),
        rate=number("HUB_RATE_PER_SECOND", 80, 0.1, 10000),
        burst=number("HUB_RATE_BURST", 200, 1, 20000),
        device_ttl=number("HUB_DEVICE_TTL_SECS", 120, 15, 3600),
        max_devices=number("HUB_MAX_DEVICES", 2048, 1, 10000, int),
    )


async def run(mode):
    options = {"gateway_host": os.getenv("GATEWAY_HOST", "gateway"),
               "bind_host": os.getenv("HUB_BIND_HOST", "0.0.0.0"), "limits": limits_from_env()}
    if mode == "sensores":
        hub = SensorHub(**options)
    elif mode == "acesso":
        allowed = tuple(host.strip() for host in os.getenv("ACCESS_ALLOWED_HOSTS", "dashboard").split(",") if host.strip())
        if not allowed:
            raise ValueError("ACCESS_ALLOWED_HOSTS deve informar ao menos um host autorizado")
        hub = AccessHub(allowed_hosts=allowed, **options)
    else:
        raise ValueError("modo deve ser sensores ou acesso")
    hub.directory.refresh = number("HUB_DNS_REFRESH_SECS", 15, 1, 60)
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    try:
        await hub.start()
        await stop.wait()
    finally:
        await hub.close()


def healthcheck(mode):
    port = 5010 if mode == "sensores" else 5001
    with socket.create_connection(("127.0.0.1", port), timeout=1):
        pass


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "healthcheck":
        healthcheck(sys.argv[2])
    else:
        asyncio.run(run(sys.argv[1] if len(sys.argv) > 1 else os.getenv("HUB_MODE", "sensores")))
