"""Verifica inventário, telemetria e controle pela mesma conexão do dashboard."""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
import uuid

import messages_pb2 as pb
from gateway_transport import GatewayTcpClient


FAMILIES = {
    pb.DEVICE_TYPE_WEATHER_STATION: ("C", "temperature"),
    pb.DEVICE_TYPE_LAMP_POST: ("Lua", "luminosity"),
    pb.DEVICE_TYPE_TRAFFIC_LIGHT: ("Java", "state"),
    pb.DEVICE_TYPE_CAMERA: ("Python", "vehicles_count"),
    pb.DEVICE_TYPE_PARKING_SENSOR: ("Go", "total_spaces"),
    pb.DEVICE_TYPE_WATER_SENSOR: ("Rust", "water_level"),
    pb.DEVICE_TYPE_WASTE_SENSOR: ("TypeScript", "waste_fill_level"),
}


def request(client, request_type, **fields):
    message = pb.ClientRequest(message_id=f"smoke-{uuid.uuid4().hex}",
                               timestamp=int(time.time()), type=request_type, **fields)
    result = client.request(message)
    if result.response is None:
        raise RuntimeError(f"{result.error_code}: {result.error_message}")
    return result.response


def run_checks(client, *, expected_devices=66, timeout=90.0):
    deadline = time.monotonic() + timeout
    last_error = "inventário ainda incompleto"
    devices = []
    metric_checks = {}
    while time.monotonic() < deadline:
        try:
            inventory = request(client, pb.REQUEST_TYPE_LIST_DEVICES)
            if not inventory.success:
                raise RuntimeError(inventory.message)
            devices = list(inventory.devices)
            if any(device.device_id.startswith("intruder_") for device in devices):
                raise RuntimeError("dispositivo invasor apareceu no inventário")
            if len(devices) != expected_devices:
                raise RuntimeError(f"inventário: {len(devices)} de {expected_devices} dispositivos")
            types = {device.type for device in devices}
            if not set(FAMILIES).issubset(types):
                raise RuntimeError("o inventário ainda não contém as sete famílias")
            if any(device.is_controllable and device.control_port != 5010 for device in devices):
                raise RuntimeError("controle descoberto não aponta para o proxy do hub de sensores")
            # Cada métrica principal é exclusiva de uma família; a consulta global
            # permite que dispositivos desligados continuem no inventário.
            metric_checks = {}
            for device_type, (language, metric) in FAMILIES.items():
                now = int(time.time())
                response = request(client, pb.REQUEST_TYPE_ANALYTICS_QUERY,
                                   query_metric=metric, query_op=pb.OP_AVERAGE,
                                   start_timestamp=now - 300, end_timestamp=now)
                if not response.success or not response.graph_points:
                    raise RuntimeError(f"telemetria {language}/{metric}: {response.message}")
                family_ids = {device.device_id for device in devices if device.type == device_type}
                if any(point.device_id not in family_ids or not math.isfinite(point.value)
                       for point in response.graph_points) or not math.isfinite(response.analytics_result):
                    raise RuntimeError(f"série inválida para {language}/{metric}")
                metric_checks[language] = {"metric": metric, "points": len(response.graph_points)}
            break
        except RuntimeError as error:
            last_error = str(error)
            time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
    else:
        raise RuntimeError(f"verificação não concluiu em {timeout:g}s: {last_error}")

    water = next(device for device in devices if device.type == pb.DEVICE_TYPE_WATER_SENSOR
                 and device.is_controllable)
    # O intervalo padrão é 5s. Reaplicá-lo testa o ACK e o caminho de controle
    # dashboard -> hub_acesso -> gateway -> hub_sensores -> sensor Rust.
    command = pb.ConfigCommand(command_id=f"smoke-command-{uuid.uuid4().hex}",
                               timestamp=int(time.time()), target_device_id=water.device_id,
                               update_frequency=True, new_frequency_secs=5)
    response = request(client, pb.REQUEST_TYPE_SEND_COMMAND, target_device_id=water.device_id,
                       command_payload=command)
    if not response.success:
        raise RuntimeError(f"controle Rust recusado: {response.message}")
    return {"devices": len(devices), "families": len(FAMILIES), "telemetry": metric_checks,
            "rust_control": {"device_id": water.device_id, "frequency_secs": 5, "success": True}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-devices", type=int, default=66)
    parser.add_argument("--timeout", type=float, default=90.0)
    args = parser.parse_args()
    if args.expected_devices < len(FAMILIES) or not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("informe ao menos sete dispositivos e um timeout positivo e finito")
    client = GatewayTcpClient(os.getenv("GATEWAY_HOST", "hub_acesso"),
                              int(os.getenv("GATEWAY_PORT", "5001")))
    try:
        print(json.dumps(run_checks(client, expected_devices=args.expected_devices,
                                    timeout=args.timeout), ensure_ascii=False))
        return 0
    except (RuntimeError, StopIteration) as error:
        print(json.dumps({"success": False, "error": str(error)}, ensure_ascii=False), file=sys.stderr)
        return 1
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
