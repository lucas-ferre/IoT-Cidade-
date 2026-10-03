"""Sistema real em loopback: dois hubs, gateway, SQLite e controle do sensor.

As portas são efêmeras e as identidades de rede são fixadas na configuração,
sem Docker, DNS externo, Streamlit ou envio para equipamentos de terceiros.
"""
from __future__ import annotations

import asyncio
from contextlib import suppress
import importlib.util
from pathlib import Path
import sys
import tempfile
import time
import unittest
import uuid
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "gateway"))
sys.path.insert(0, str(ROOT))
sys.path.append(str(ROOT / "client"))
_spec = importlib.util.spec_from_file_location("gateway_system", ROOT / "gateway" / "main.py")
gateway = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = gateway
_spec.loader.exec_module(gateway)
_smoke_spec = importlib.util.spec_from_file_location("client_smoke_system", ROOT / "client" / "smoke.py")
smoke = importlib.util.module_from_spec(_smoke_spec)
sys.modules[_smoke_spec.name] = smoke
_smoke_spec.loader.exec_module(smoke)

from hubs.common import HostDirectory, Limits, close_writer, pb, read_frame, write_frame
from hubs.main import AccessHub, SensorHub, SensorProfile
from sensor_intruder.main import build_scenarios


class SystemFixture:
    """Recursos compartilhados, sem herdar os testes unitários de outros arquivos."""

    def __init__(self, control_port=0):
        self.temp = tempfile.TemporaryDirectory()
        self.transports = []
        self.servers = []
        self.writers = []
        self.sensor_tasks = set()
        self.audit = []
        self.commands = []
        self.acknowledgements = []
        self.sensor_status = pb.STATUS_ON
        self.sensor_frequency = 5
        self.worker = None
        self.sensor_hub = None
        self.access_hub = None
        self.limits = Limits(request_timeout=0.5, rate=1000, burst=1000)
        self.control_port = control_port
        self.config = patch.multiple(
            gateway, DB_DIR=self.temp.name, DB_FILE=str(Path(self.temp.name) / "system.sqlite"),
            TELEMETRY_QUEUE=asyncio.Queue(maxsize=100), TELEMETRY_BATCH_MAX_PAYLOADS=4,
            TELEMETRY_BATCH_FLUSH_INTERVAL_SECS=0.005,
            TELEMETRY_SHUTDOWN_TIMEOUT_SECS=1.0,
            _BACKGROUND_TASKS=set(), _CLIENT_TASKS=set(),
        )

    async def start(self):
        self.config.start()
        gateway.init_db()
        gateway.DB_POOL = gateway.SQLiteConnectionPool(gateway.DB_FILE, 2)
        await gateway.DB_POOL.start()
        self.worker = asyncio.create_task(gateway.telemetry_batch_worker_loop())
        loop = asyncio.get_running_loop()
        ports = {}
        for name, factory in (("telemetry", gateway.TelemetryUDPProtocol),
                              ("discovery", gateway.DiscoveryUDPProtocol)):
            transport, _ = await loop.create_datagram_endpoint(factory, local_addr=("127.0.0.1", 0))
            self.transports.append(transport)
            ports[name] = transport.get_extra_info("sockname")[1]
        sensor = await asyncio.start_server(self.sensor_control, "127.0.0.1", 0)
        backend = await asyncio.start_server(gateway.handle_client_request, "127.0.0.1", 0)
        self.servers.extend((sensor, backend))
        self.sensor_port = sensor.sockets[0].getsockname()[1]
        fixed = {name: {"127.0.0.1"} for name in (
            "gateway", "dashboard", "sensor_camera", "sensor_agua", "sensor_lixeiras",
            "sensor_clima", "sensor_posto", "sensor_java", "sensor_estacionamento",
        )}
        self.sensor_hub = SensorHub(
            profiles=(SensorProfile("camera_", "sensor_camera", pb.DEVICE_TYPE_CAMERA, self.sensor_port),
                      SensorProfile("estacao_", "sensor_clima", pb.DEVICE_TYPE_WEATHER_STATION, 0),
                      SensorProfile("poste_", "sensor_posto", pb.DEVICE_TYPE_LAMP_POST, self.sensor_port),
                      SensorProfile("semaforo_", "sensor_java", pb.DEVICE_TYPE_TRAFFIC_LIGHT, self.sensor_port),
                      SensorProfile("parking_", "sensor_estacionamento", pb.DEVICE_TYPE_PARKING_SENSOR, self.sensor_port),
                      SensorProfile("water_", "sensor_agua", pb.DEVICE_TYPE_WATER_SENSOR, self.sensor_port),
                      SensorProfile("waste_", "sensor_lixeiras", pb.DEVICE_TYPE_WASTE_SENSOR, 0)),
            telemetry_port=0, discovery_port=0, control_port=self.control_port,
            gateway_telemetry_port=ports["telemetry"], gateway_discovery_port=ports["discovery"],
            directory=HostDirectory([], fixed=fixed), limits=self.limits,
            bind_host="127.0.0.1", audit_sink=self.audit.append,
        )
        self.access_hub = AccessHub(
            access_port=0, gateway_access_port=backend.sockets[0].getsockname()[1],
            directory=HostDirectory([], fixed=fixed), limits=self.limits,
            bind_host="127.0.0.1", audit_sink=self.audit.append,
        )
        await self.sensor_hub.start()
        await self.access_hub.start()
        self.sender, _ = await loop.create_datagram_endpoint(
            asyncio.DatagramProtocol, local_addr=("127.0.0.1", 0)
        )
        self.intruder, _ = await loop.create_datagram_endpoint(
            asyncio.DatagramProtocol, local_addr=("127.0.0.2", 0)
        )
        self.transports.extend((self.sender, self.intruder))

    async def close(self):
        for server in self.servers:
            server.close()
        for writer in self.writers:
            await close_writer(writer)
        for hub in (self.access_hub, self.sensor_hub):
            if hub is not None:
                await asyncio.wait_for(hub.close(), 1.0)
        for transport in self.transports:
            transport.close()
        # Fechar os clientes e drenar as tasks admitidas antes de devolver o pool.
        clients = list(gateway._CLIENT_TASKS)
        for task in clients:
            task.cancel()
        await asyncio.gather(*clients, return_exceptions=True)
        await asyncio.gather(*list(gateway._BACKGROUND_TASKS), return_exceptions=True)
        if self.worker is not None:
            await gateway.stop_telemetry_worker(self.worker)
        for task in list(self.sensor_tasks):
            task.cancel()
        await asyncio.gather(*list(self.sensor_tasks), return_exceptions=True)
        for server in self.servers:
            await server.wait_closed()
        if gateway.DB_POOL is not None:
            await gateway.DB_POOL.close()
            gateway.DB_POOL = None
        self.config.stop()
        self.temp.cleanup()

    async def sensor_control(self, reader, writer):
        task = asyncio.current_task()
        self.sensor_tasks.add(task)
        try:
            command = pb.ConfigCommand.FromString(await read_frame(reader, self.limits))
            self.commands.append(command)
            if command.update_status:
                self.sensor_status = command.target_status
            if command.update_frequency:
                self.sensor_frequency = command.new_frequency_secs
            response = pb.ConfigResponse(
                message_id=f"ACK-{uuid.uuid4().hex}", timestamp=int(time.time()),
                command_id=command.command_id, success=True, message=f"applied:{command.command_id}",
                updated_status=self.sensor_status, updated_frequency_secs=self.sensor_frequency,
            )
            self.acknowledgements.append(response)
            await write_frame(writer, response, self.limits)
        finally:
            await close_writer(writer)
            self.sensor_tasks.discard(task)

    async def rows(self, query, parameters=()):
        async with gateway.DB_POOL.connection() as db:
            async with db.execute(query, parameters) as cursor:
                return await cursor.fetchall()

    async def eventually(self, predicate, timeout=1.0):
        async def check():
            while not await predicate():
                await asyncio.sleep(0.005)
        await asyncio.wait_for(check(), timeout)

    async def discover(self, device_id="camera_pici_01", device_type=pb.DEVICE_TYPE_CAMERA,
                       controllable=True):
        message = pb.DiscoveryResponse(
            message_id=f"DISC-{uuid.uuid4().hex}", timestamp=int(time.time()), device_id=device_id,
            type=device_type, ip_address="untrusted.example", initial_status=pb.STATUS_ON,
            control_port=self.sensor_port if controllable else 0, is_controllable=controllable,
        )
        self.sender.sendto(message.SerializeToString(), ("127.0.0.1", self.sensor_hub.discovery_port))
        async def registered():
            return bool(await self.rows("SELECT 1 FROM devices WHERE device_id=?", (device_id,)))
        await self.eventually(registered)

    async def emit(self, metric_values, device_id="camera_pici_01"):
        message = pb.DataPayload(message_id=uuid.uuid4().hex, timestamp=int(time.time()),
                                 device_id=device_id, current_status=pb.STATUS_ON)
        for name, value, unit in metric_values:
            message.metrics.add(name=name, value=value, unit=unit)
        self.sender.sendto(message.SerializeToString(), ("127.0.0.1", self.sensor_hub.telemetry_port))
        async def persisted():
            return bool(await self.rows("SELECT 1 FROM telemetry_messages WHERE message_id=?", (message.message_id,)))
        await self.eventually(persisted)
        return message

    async def connect(self, source="127.0.0.1"):
        reader, writer = await asyncio.wait_for(asyncio.open_connection(
            "127.0.0.1", self.access_hub.access_port, local_addr=(source, 0)
        ), 1.0)
        self.writers.append(writer)
        return reader, writer

    async def exchange(self, connection, request):
        reader, writer = connection
        await write_frame(writer, request, self.limits)
        data = await read_frame(reader, self.limits)
        return pb.ClientResponse.FromString(data) if data is not None else None

    def request(self, request_type=pb.REQUEST_TYPE_LIST_DEVICES, **fields):
        return pb.ClientRequest(message_id=uuid.uuid4().hex, timestamp=int(time.time()),
                                type=request_type, **fields)

    async def snapshot(self):
        return {table: await self.rows(f"SELECT * FROM {table} ORDER BY 1, 2") for table in (
            "devices", "metrics", "telemetry_messages", "telemetry_state",
            "metrics_rollup_1m", "metrics_rollup_5m", "metrics_rollup_1h",
        )}


class SystemIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # O smoke de produção verifica explicitamente o endpoint 5010. Os outros
        # cenários continuam livres de portas fixas e executam com portas efêmeras.
        control_port = 5010 if self._testMethodName == "test_production_smoke_checks_seven_families_over_one_persistent_connection" else 0
        self.system = SystemFixture(control_port=control_port)
        self.addAsyncCleanup(self.system.close)
        await self.system.start()

    async def test_discovery_telemetry_database_inventory_analytics_and_control_cross_both_hubs(self):
        system = self.system
        await system.discover()
        await system.emit((('vehicles_count', 20, 'veh/min'), ('average_speed', 35, 'km/h')))
        await system.emit((('vehicles_count', 40, 'veh/min'), ('average_speed', 25, 'km/h')))
        self.assertEqual(await system.rows("SELECT COUNT(*) FROM metrics"), [(4,)])
        connection = await system.connect()
        request = system.request()
        inventory = await system.exchange(connection, request)
        self.assertTrue(inventory.success, inventory.message)
        self.assertEqual(inventory.message_id, request.message_id)
        self.assertEqual(len(inventory.devices), 1)
        device = inventory.devices[0]
        self.assertEqual(device.device_id, "camera_pici_01")
        self.assertEqual(device.ip_address, "127.0.0.1")
        self.assertEqual(device.control_port, system.sensor_hub.control_port)
        self.assertNotEqual(device.control_port, system.sensor_port)
        self.assertTrue(device.is_controllable)
        route = system.sensor_hub.routes[device.device_id]
        self.assertEqual((route.source_ip, route.control_port), ("127.0.0.1", system.sensor_port))
        now = int(time.time())
        query = system.request(pb.REQUEST_TYPE_ANALYTICS_QUERY, target_device_id=device.device_id,
                               query_metric="vehicles_count", query_op=pb.OP_AVERAGE,
                               start_timestamp=now - 60, end_timestamp=now + 1)
        result = await system.exchange(connection, query)
        self.assertTrue(result.success, result.message)
        self.assertEqual(result.message_id, query.message_id)
        self.assertAlmostEqual(result.analytics_result, 30.0)
        self.assertEqual(sorted(point.value for point in result.graph_points), [20, 40])
        command = system.request(pb.REQUEST_TYPE_SEND_COMMAND, target_device_id=device.device_id)
        command.command_payload.CopyFrom(pb.ConfigCommand(
            command_id="CMD-SYSTEM-01", timestamp=int(time.time()), target_device_id=device.device_id,
            update_status=True, target_status=pb.STATUS_OFF,
            update_frequency=True, new_frequency_secs=7,
        ))
        response = await system.exchange(connection, command)
        self.assertTrue(response.success, response.message)
        self.assertEqual(response.message_id, command.message_id)
        self.assertEqual(response.message, "applied:CMD-SYSTEM-01")
        self.assertEqual(len(system.commands), 1)
        self.assertEqual(system.commands[0].command_id, "CMD-SYSTEM-01")
        self.assertEqual(system.acknowledgements[0].command_id, "CMD-SYSTEM-01")
        self.assertEqual((system.sensor_status, system.sensor_frequency), (pb.STATUS_OFF, 7))
        self.assertTrue(any(entry.get("reason") == "control_ack" for entry in system.audit))

    async def test_twelve_intruder_scenarios_do_not_change_inventory_metrics_or_rollups(self):
        system = self.system
        await system.discover()
        await system.emit((('vehicles_count', 30, 'veh/min'),))
        before = await system.snapshot()
        route = system.sensor_hub.routes["camera_pici_01"]
        baseline_rejections = sum(entry.get("event") == "rejected" for entry in system.audit)
        scenarios = build_scenarios(max_udp=system.limits.max_udp)
        self.assertEqual(len(scenarios), 12)
        for scenario in scenarios:
            if scenario.channel == "access":
                connection = await system.connect(source="127.0.0.2")
                # O filtro de origem pode encerrar antes de ler o pedido.
                with suppress(OSError, ConnectionError):
                    await write_frame(connection[1], scenario.payload, system.limits)
                try:
                    data = await asyncio.wait_for(connection[0].read(), 0.5)
                except ConnectionResetError:
                    data = b""  # Windows sinaliza reset quando há bytes ainda não lidos.
                self.assertEqual(data, b"")
            else:
                port = system.sensor_hub.discovery_port if scenario.channel == "discovery" else system.sensor_hub.telemetry_port
                system.intruder.sendto(scenario.payload, ("127.0.0.1", port))
        async def all_audited():
            return sum(entry.get("event") == "rejected" for entry in system.audit) >= baseline_rejections + 12
        await system.eventually(all_audited)
        # Esperar auditoria + workers elimina a falsa confirmação por sendto UDP.
        await asyncio.sleep(0.02)
        await asyncio.wait_for(gateway.get_telemetry_queue().join(), 0.5)
        self.assertEqual(await system.snapshot(), before)
        self.assertIs(system.sensor_hub.routes["camera_pici_01"], route)
        self.assertFalse(system.commands)
        self.assertFalse(await system.rows("SELECT device_id FROM devices WHERE device_id LIKE 'intruder_%'"))
        reasons = {entry.get("reason") for entry in system.audit if entry.get("event") == "rejected"}
        self.assertTrue({"unknown_device", "source_identity_mismatch", "malformed_protobuf",
                         "invalid_timestamp", "non_finite_metric", "invalid_datagram_size",
                         "unauthorized_source"}.issubset(reasons), reasons)
        self.assertTrue((await system.exchange(await system.connect(), system.request())).success)

    async def test_new_rust_and_typescript_types_are_preserved_through_gateway_and_analytics(self):
        system = self.system
        devices = (("water_centro_01", pb.DEVICE_TYPE_WATER_SENSOR, True, "water_flow", 12.5, "L/min"),
                   ("waste_centro_01", pb.DEVICE_TYPE_WASTE_SENSOR, False, "waste_fill_level", 42.5, "%"))
        for identity, device_type, controllable, metric, value, unit in devices:
            await system.discover(identity, device_type, controllable)
            await system.emit(((metric, value, unit),), identity)
        connection = await system.connect()
        inventory = await system.exchange(connection, system.request())
        self.assertTrue(inventory.success, inventory.message)
        indexed = {device.device_id: device for device in inventory.devices}
        self.assertEqual(set(indexed), {device[0] for device in devices})
        for identity, device_type, controllable, metric, value, unit in devices:
            self.assertEqual(indexed[identity].type, device_type)
            self.assertEqual(indexed[identity].is_controllable, controllable)
            self.assertEqual(indexed[identity].control_port, system.sensor_hub.control_port if controllable else 0)
            now = int(time.time())
            query = system.request(pb.REQUEST_TYPE_ANALYTICS_QUERY, target_device_id=identity,
                                   query_metric=metric, query_op=pb.OP_AVERAGE,
                                   start_timestamp=now - 60, end_timestamp=now + 1)
            response = await system.exchange(connection, query)
            self.assertTrue(response.success, response.message)
            self.assertAlmostEqual(response.analytics_result, value)
            self.assertEqual(await system.rows("SELECT unit FROM metrics WHERE device_id=?", (identity,)), [(unit,)])

    async def test_production_smoke_checks_seven_families_over_one_persistent_connection(self):
        system = self.system
        principal_metrics = (
            ("estacao_pici_01", pb.DEVICE_TYPE_WEATHER_STATION, False, "temperature", 29.0, "C"),
            ("poste_pici_01", pb.DEVICE_TYPE_LAMP_POST, True, "luminosity", 86.0, "%"),
            ("semaforo_pici_01", pb.DEVICE_TYPE_TRAFFIC_LIGHT, True, "state", 3.0, "code"),
            ("camera_pici_01", pb.DEVICE_TYPE_CAMERA, True, "vehicles_count", 36.0, "veh/min"),
            ("parking_centro_01", pb.DEVICE_TYPE_PARKING_SENSOR, True, "total_spaces", 120.0, "spaces"),
            ("water_centro_01", pb.DEVICE_TYPE_WATER_SENSOR, True, "water_level", 62.0, "%"),
            ("waste_centro_01", pb.DEVICE_TYPE_WASTE_SENSOR, False, "waste_fill_level", 42.5, "%"),
        )
        for identity, device_type, controllable, metric, value, unit in principal_metrics:
            await system.discover(identity, device_type, controllable)
            await system.emit(((metric, value, unit),), identity)
        client = smoke.GatewayTcpClient("127.0.0.1", system.access_hub.access_port)
        calls = []
        real_request = client.request
        def recording_request(message):
            result = real_request(message)
            calls.append((message, result, client._sock))
            return result
        client.request = recording_request
        try:
            # O cliente síncrono precisa sair do loop que serve gateway e hubs.
            result = await asyncio.wait_for(asyncio.to_thread(
                smoke.run_checks, client, expected_devices=7, timeout=2.0
            ), 3.0)
            self.assertEqual(result["devices"], 7)
            self.assertEqual(result["families"], 7)
            self.assertEqual(set(result["telemetry"]), {"C", "Lua", "Java", "Python", "Go", "Rust", "TypeScript"})
            self.assertTrue(all(check["points"] == 1 for check in result["telemetry"].values()))
            self.assertEqual(result["rust_control"], {
                "device_id": "water_centro_01", "frequency_secs": 5, "success": True,
            })
            self.assertEqual(len(calls), 9, 'inventário, sete consultas e um comando')
            self.assertEqual(len({id(sock) for _, _, sock in calls}), 1)
            self.assertTrue(all(sock is not None for _, _, sock in calls))
            for message, reply, _ in calls:
                self.assertTrue(reply.transport_ok)
                self.assertTrue(reply.response.success, reply.response.message)
                self.assertEqual(reply.response.message_id, message.message_id)
            self.assertEqual(system.access_hub.active_clients, 1)
            self.assertEqual(len(gateway._CLIENT_TASKS), 1)
            self.assertEqual(len(system.commands), 1)
            command = system.commands[0]
            self.assertEqual(command.target_device_id, "water_centro_01")
            self.assertTrue(command.update_frequency)
            self.assertEqual(command.new_frequency_secs, 5)
            self.assertEqual(system.acknowledgements[0].command_id, command.command_id)
        finally:
            client.close()


if __name__ == "__main__":
    unittest.main()
