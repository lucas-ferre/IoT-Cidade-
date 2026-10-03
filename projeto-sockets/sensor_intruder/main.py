"""Teste opt-in: emite tráfego inválido somente aos dois hubs configurados."""

from __future__ import annotations

from dataclasses import dataclass
import ipaddress
import json
import math
import os
import socket
import struct
import time
import uuid

import messages_pb2 as pb


@dataclass(frozen=True)
class Scenario:
    name: str
    channel: str
    payload: bytes


def build_scenarios(*, now=None, spoof_device_id="camera_pici_01", max_udp=16384):
    now = int(time.time()) if now is None else now
    suffix = uuid.uuid4().hex[:12]
    device_id = f"intruder_{suffix}"

    def payload(identity=device_id, timestamp=now, value=25.0):
        message = pb.DataPayload(message_id=f"intruder-{uuid.uuid4().hex}", timestamp=timestamp,
                                 device_id=identity, current_status=pb.STATUS_ON)
        message.metrics.add(name="temperature", value=value, unit="C")
        return message.SerializeToString()

    discovery = pb.DiscoveryResponse(message_id=f"intruder-disc-{suffix}", timestamp=now,
                                     device_id=device_id, type=pb.DEVICE_TYPE_CAMERA,
                                     ip_address="sensor_camera", control_port=5004,
                                     initial_status=pb.STATUS_ON, is_controllable=True)
    spoof_discovery = pb.DiscoveryResponse()
    spoof_discovery.CopyFrom(discovery)
    spoof_discovery.device_id = spoof_device_id
    invalid = pb.ClientRequest(message_id=f"intruder-request-{suffix}", timestamp=now, type=0)
    command = pb.ClientRequest(message_id=f"intruder-command-{suffix}", timestamp=now,
                              type=pb.REQUEST_TYPE_SEND_COMMAND, target_device_id=spoof_device_id)
    command.command_payload.CopyFrom(pb.ConfigCommand(command_id=f"intruder-cmd-{suffix}", timestamp=now,
                                                      target_device_id=spoof_device_id,
                                                      update_frequency=True, new_frequency_secs=0))
    return (
        Scenario("unknown_discovery", "discovery", discovery.SerializeToString()),
        Scenario("unknown_identity", "telemetry", payload()),
        Scenario("spoof_discovery", "discovery", spoof_discovery.SerializeToString()),
        Scenario("spoof_telemetry", "telemetry", payload(spoof_device_id)),
        Scenario("malformed_protobuf", "telemetry", b"\x0a\xff\xff\xff"),
        Scenario("expired_timestamp", "telemetry", payload(timestamp=now - 604801)),
        Scenario("future_timestamp", "telemetry", payload(timestamp=now + 3600)),
        Scenario("non_finite_metric", "telemetry", payload(value=math.nan)),
        Scenario("oversized_datagram", "telemetry", b"X" * (max_udp + 1)),
        Scenario("invalid_request_type", "access", invalid.SerializeToString()),
        Scenario("invalid_frequency", "access", command.SerializeToString()),
        Scenario("malformed_tcp_protobuf", "access", b"\x0a\xff\xff\xff"),
    )


def resolve_lab_host(host):
    addresses = sorted({item[4][0] for item in socket.getaddrinfo(host, None, socket.AF_INET, socket.SOCK_STREAM)})
    if not addresses:
        raise ValueError("destino configurado sem endereço IPv4")
    for address in addresses:
        parsed = ipaddress.IPv4Address(address)
        if not (parsed.is_private or parsed.is_loopback) or parsed.is_multicast or parsed.is_unspecified:
            raise ValueError("o sensor de teste aceita somente destinos privados ou loopback")
    return addresses[0]


def receive_exact(sock, count):
    chunks = bytearray()
    while len(chunks) < count:
        chunk = sock.recv(count - len(chunks))
        if not chunk:
            raise ConnectionError("hub fechou a conexão")
        chunks.extend(chunk)
    return bytes(chunks)


def send_access_scenario(address, port, data):
    # Um único pedido por conexão; nunca envia comandos válidos aos sensores.
    with socket.create_connection((address, port), timeout=2.0) as sock:
        sock.settimeout(2.0)
        try:
            sock.sendall(struct.pack("!I", len(data)) + data)
            size = struct.unpack("!I", receive_exact(sock, 4))[0]
            if not 0 < size <= 1048576:
                return "invalid_response_frame"
            response = pb.ClientResponse()
            response.ParseFromString(receive_exact(sock, size))
            return "request_rejected" if not response.success else "unexpected_success"
        except (ConnectionError, ConnectionResetError, BrokenPipeError):
            return "connection_closed_by_hub"


def bounded_number(name, default, minimum, maximum, converter=float):
    value = converter(os.getenv(name, str(default)))
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} deve estar entre {minimum} e {maximum}")
    return value


def emit(event, **fields):
    print(json.dumps({"timestamp": int(time.time()), "sensor_id": "sensor_invasor",
                      "event": event, **fields}, ensure_ascii=False), flush=True)


def main():
    if os.getenv("INTRUDER_ENABLED", "0") != "1":
        emit("disabled", reason="execute explicitamente com INTRUDER_ENABLED=1 ou perfil security-test")
        return 0
    rounds = bounded_number("INTRUDER_ROUNDS", 1, 1, 10, int)
    interval = bounded_number("INTRUDER_INTERVAL_SECS", 0.15, 0.02, 5)
    telemetry_port = bounded_number("INTRUDER_TELEMETRY_PORT", 5000, 1, 65535, int)
    discovery_port = bounded_number("INTRUDER_DISCOVERY_PORT", 5002, 1, 65535, int)
    access_port = bounded_number("INTRUDER_ACCESS_PORT", 5001, 1, 65535, int)
    max_udp = bounded_number("INTRUDER_UDP_MAX_BYTES", 16384, 64, 65000, int)
    spoof_id = os.getenv("INTRUDER_SPOOF_DEVICE_ID", "camera_pici_01")
    if len(spoof_id) > 128:
        raise ValueError("INTRUDER_SPOOF_DEVICE_ID excede 128 caracteres")
    sensor_address = resolve_lab_host(os.getenv("INTRUDER_SENSOR_HUB_HOST", "hub_sensores"))
    access_address = resolve_lab_host(os.getenv("INTRUDER_ACCESS_HUB_HOST", "hub_acesso"))
    counts = {"udp_sent": 0, "tcp_rejected": 0, "errors": 0, "unexpected_success": 0}
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp:
        udp.settimeout(2.0)
        for round_number in range(1, rounds + 1):
            for scenario in build_scenarios(spoof_device_id=spoof_id, max_udp=max_udp):
                try:
                    if scenario.channel == "access":
                        result = send_access_scenario(access_address, access_port, scenario.payload)
                        if result in ("request_rejected", "connection_closed_by_hub"):
                            counts["tcp_rejected"] += 1
                        else:
                            counts["unexpected_success"] += 1
                    else:
                        port = discovery_port if scenario.channel == "discovery" else telemetry_port
                        udp.sendto(scenario.payload, (sensor_address, port))
                        counts["udp_sent"] += 1
                        result = "sent_not_acknowledged"
                    emit("scenario", scenario=scenario.name, channel=scenario.channel,
                         round=round_number, result=result)
                except OSError as error:
                    counts["errors"] += 1
                    emit("transport_error", scenario=scenario.name, reason=type(error).__name__)
                time.sleep(interval)
    emit("completed", rounds=rounds, counts=counts,
         udp_verification="consulte auditoria JSON hub_sensores; UDP não possui confirmação")
    return 1 if counts["errors"] or counts["unexpected_success"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
