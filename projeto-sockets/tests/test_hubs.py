"""Integração dos hubs com TCP/UDP reais em loopback e portas efêmeras."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
import struct
import sys
import time
import unittest
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.append(str(ROOT / "gateway"))

from hubs.common import Audit, HostDirectory, Limits, RateLimiter, pb, read_frame, write_frame
from hubs.main import AccessHub, SensorHub, SensorProfile


class DatagramCollector(asyncio.DatagramProtocol):
    def __init__(self):
        self.queue = asyncio.Queue()

    def datagram_received(self, data, address):
        self.queue.put_nowait((data, address))


class SensorHubTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.audit = []
        self.transports = []
        self.servers = []
        self.writers = []
        self.commands = []
        self.ack_mismatch = False
        self.limits = Limits(request_timeout=0.5)
        loop = asyncio.get_running_loop()
        for name in ("telemetry", "discovery"):
            collector = DatagramCollector()
            transport, _ = await loop.create_datagram_endpoint(lambda collector=collector: collector, local_addr=("127.0.0.1", 0))
            self.transports.append(transport)
            setattr(self, name + "_collector", collector)
            setattr(self, name + "_port", transport.get_extra_info("sockname")[1])
        sensor = await asyncio.start_server(self.sensor_control, "127.0.0.1", 0)
        self.servers.append(sensor)
        self.sensor_port = sensor.sockets[0].getsockname()[1]
        directory = HostDirectory([], fixed={"gateway": {"127.0.0.1"}, "sensor_camera": {"127.0.0.1"}})
        self.hub = SensorHub(profiles=(SensorProfile("camera_", "sensor_camera", 4, self.sensor_port),),
                             telemetry_port=0, discovery_port=0, control_port=0,
                             gateway_telemetry_port=self.telemetry_port, gateway_discovery_port=self.discovery_port,
                             directory=directory, limits=self.limits, bind_host="127.0.0.1", audit_sink=self.audit.append)
        await self.hub.start()
        self.sender, _ = await loop.create_datagram_endpoint(asyncio.DatagramProtocol, local_addr=("127.0.0.1", 0))
        self.transports.append(self.sender)

    async def asyncTearDown(self):
        for writer in self.writers:
            writer.close()
            await writer.wait_closed()
        await self.hub.close()
        for server in self.servers:
            server.close()
            await server.wait_closed()
        for transport in self.transports:
            transport.close()

    async def sensor_control(self, reader, writer):
        try:
            data = await read_frame(reader, self.limits)
            command = pb.ConfigCommand()
            command.ParseFromString(data)
            self.commands.append(command)
            response = pb.ConfigResponse(message_id="ack-id", command_id="wrong" if self.ack_mismatch else command.command_id,
                                         timestamp=int(time.time()), success=True,
                                         updated_status=command.target_status,
                                         updated_frequency_secs=command.new_frequency_secs)
            await write_frame(writer, response, self.limits)
        finally:
            writer.close()
            await writer.wait_closed()

    def discovery(self, **kwargs):
        fields = dict(message_id="disc-1", timestamp=int(time.time()), device_id="camera_pici_01",
                      type=4, ip_address="attacker-address-is-not-an-endpoint", control_port=self.sensor_port,
                      initial_status=1, is_controllable=True)
        fields.update(kwargs)
        return pb.DiscoveryResponse(**fields)

    def payload(self, **kwargs):
        fields = dict(message_id="data-1", timestamp=int(time.time()), device_id="camera_pici_01", current_status=1)
        fields.update(kwargs)
        message = pb.DataPayload(**fields)
        message.metrics.add(name="vehicles_count", value=40, unit="veh/min")
        return message

    async def discover(self, message=None):
        self.sender.sendto((message or self.discovery()).SerializeToString(), ("127.0.0.1", self.hub.discovery_port))
        data, address = await asyncio.wait_for(self.discovery_collector.queue.get(), 1.0)
        forwarded = pb.DiscoveryResponse()
        forwarded.ParseFromString(data)
        return forwarded, address

    async def rejected_udp(self, message, kind, reason):
        port = self.hub.discovery_port if kind == "discovery" else self.hub.telemetry_port
        data = message if isinstance(message, bytes) else message.SerializeToString()
        self.sender.sendto(data, ("127.0.0.1", port))
        collector = self.discovery_collector if kind == "discovery" else self.telemetry_collector
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(collector.queue.get(), 0.06)
        self.assertTrue(any(entry.get("reason") == reason for entry in self.audit), self.audit)

    async def command(self, device="camera_pici_01"):
        reader, writer = await asyncio.open_connection("127.0.0.1", self.hub.control_port)
        self.writers.append(writer)
        command = pb.ConfigCommand(command_id="cmd-1", timestamp=int(time.time()), target_device_id=device,
                                   update_status=True, target_status=2, update_frequency=True, new_frequency_secs=7)
        await write_frame(writer, command, self.limits)
        response = pb.ConfigResponse()
        response.ParseFromString(await read_frame(reader, self.limits))
        return response

    async def test_discovery_rewrites_to_hub_and_pins_observed_source(self):
        forwarded, address = await self.discover()
        self.assertEqual(forwarded.device_id, "camera_pici_01")
        self.assertEqual(forwarded.control_port, self.hub.control_port)
        self.assertEqual(forwarded.ip_address, "127.0.0.1")
        self.assertEqual(address[0], "127.0.0.1")
        self.assertEqual(self.hub.routes[forwarded.device_id].control_port, self.sensor_port)
        self.assertEqual(self.hub.routes[forwarded.device_id].source_ip, "127.0.0.1")

    async def test_telemetry_forwarding_preserves_exact_payload(self):
        await self.discover()
        payload = self.payload().SerializeToString()
        self.sender.sendto(payload, ("127.0.0.1", self.hub.telemetry_port))
        data, _ = await asyncio.wait_for(self.telemetry_collector.queue.get(), 1.0)
        self.assertEqual(data, payload)

    async def test_unknown_identity_and_unregistered_telemetry_are_dropped(self):
        await self.rejected_udp(self.discovery(device_id="intruder_01"), "discovery", "unknown_device")
        await self.rejected_udp(self.payload(), "telemetry", "device_not_discovered")

    async def test_spoofed_source_cannot_register_or_replace_device(self):
        await self.discover()
        existing = self.hub.routes["camera_pici_01"]
        self.hub.directory.addresses["sensor_camera"] = {"127.0.0.2"}
        await self.rejected_udp(self.discovery(), "discovery", "source_identity_mismatch")
        await self.rejected_udp(self.payload(), "telemetry", "source_identity_mismatch")
        self.assertIs(self.hub.routes["camera_pici_01"], existing)

    async def test_profile_type_and_control_port_cannot_be_forged(self):
        for fields in ({"type": 1}, {"control_port": 22}, {"is_controllable": False}):
            await self.rejected_udp(self.discovery(**fields), "discovery", "profile_mismatch")
        self.assertFalse(self.hub.routes)

    async def test_malformed_timestamp_nan_duplicate_and_oversized_datagrams(self):
        await self.rejected_udp(b"\x0a\xff\xff\xff", "telemetry", "malformed_protobuf")
        await self.rejected_udp(self.payload(timestamp=1), "telemetry", "invalid_timestamp")
        await self.rejected_udp(self.payload(timestamp=int(time.time()) + 3600), "telemetry", "invalid_timestamp")
        payload = self.payload()
        payload.metrics[0].value = float("nan")
        await self.rejected_udp(payload, "telemetry", "non_finite_metric")
        payload = self.payload()
        payload.metrics.add(name="vehicles_count", value=2, unit="veh/min")
        await self.rejected_udp(payload, "telemetry", "duplicate_metric")
        await self.rejected_udp(b"x" * (self.limits.max_udp + 1), "telemetry", "invalid_datagram_size")

    async def test_control_relays_ack_and_requested_changes(self):
        await self.discover()
        response = await self.command()
        self.assertTrue(response.success)
        self.assertEqual(response.command_id, "cmd-1")
        self.assertEqual(response.updated_status, 2)
        self.assertEqual(response.updated_frequency_secs, 7)
        self.assertEqual(len(self.commands), 1)

    async def test_unknown_target_does_not_open_sensor_connection(self):
        response = await self.command("intruder_unknown")
        self.assertFalse(response.success)
        self.assertEqual(response.command_id, "cmd-1")
        self.assertIn("unknown_or_expired_device", response.message)
        self.assertFalse(self.commands)

    async def test_expired_registry_drops_telemetry_and_commands(self):
        await self.discover()
        self.hub.routes["camera_pici_01"].expires_at = time.monotonic() - 1
        await self.rejected_udp(self.payload(), "telemetry", "device_not_discovered")
        self.assertFalse((await self.command()).success)
        self.assertFalse(self.commands)

    async def test_ack_with_wrong_command_id_is_not_forwarded_as_success(self):
        await self.discover()
        self.ack_mismatch = True
        response = await self.command()
        self.assertFalse(response.success)
        self.assertEqual(response.command_id, "cmd-1")
        self.assertIn("ack_mismatch", response.message)

    async def test_control_source_must_be_gateway(self):
        await self.discover()
        self.hub.directory.addresses["gateway"] = {"127.0.0.2"}
        reader, writer = await asyncio.open_connection("127.0.0.1", self.hub.control_port)
        self.writers.append(writer)
        self.assertEqual(await reader.read(), b"")
        self.assertFalse(self.commands)
        self.assertTrue(any(entry.get("reason") == "unauthorized_source" for entry in self.audit))

    async def test_device_capacity_rejects_new_registry_entry(self):
        self.hub.limits = replace(self.limits, max_devices=1)
        await self.discover()
        await self.rejected_udp(self.discovery(device_id="camera_benfica_01"), "discovery", "device_capacity")
        self.assertEqual(len(self.hub.routes), 1)

    async def test_shutdown_cancels_idle_control_connection_immediately(self):
        self.hub.limits = replace(self.limits, request_timeout=60)
        reader, writer = await asyncio.open_connection("127.0.0.1", self.hub.control_port)
        self.writers.append(writer)
        # Uma troca vazia de scheduling permite que o handler admita o cliente.
        await asyncio.sleep(0)
        await asyncio.wait_for(self.hub.close(), 1)
        self.assertEqual(await reader.read(), b"")
        self.assertEqual(self.hub.active_clients, 0)


class AccessHubTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.audit = []
        self.requests = []
        self.connections = 0
        self.bad_response_id = False
        self.writers = []
        self.limits = Limits(request_timeout=0.2)
        self.server = await asyncio.start_server(self.fake_gateway, "127.0.0.1", 0)
        self.hub = AccessHub(allowed_hosts=("dashboard",), access_port=0,
                             gateway_access_port=self.server.sockets[0].getsockname()[1],
                             directory=HostDirectory([], fixed={"gateway": {"127.0.0.1"}, "dashboard": {"127.0.0.1"}}),
                             limits=self.limits, bind_host="127.0.0.1", audit_sink=self.audit.append)
        await self.hub.start()

    async def asyncTearDown(self):
        for writer in self.writers:
            writer.close()
            await writer.wait_closed()
        await self.hub.close()
        self.server.close()
        await self.server.wait_closed()

    async def fake_gateway(self, reader, writer):
        self.connections += 1
        try:
            while (data := await read_frame(reader, self.limits)) is not None:
                request = pb.ClientRequest()
                request.ParseFromString(data)
                self.requests.append(request)
                response = pb.ClientResponse(message_id="wrong" if self.bad_response_id else request.message_id,
                                             timestamp=int(time.time()), success=True, message="ok")
                await write_frame(writer, response, self.limits)
        except (OSError, ValueError, asyncio.TimeoutError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    async def connect(self):
        reader, writer = await asyncio.open_connection("127.0.0.1", self.hub.access_port)
        self.writers.append(writer)
        return reader, writer

    async def exchange(self, reader, writer, request):
        await write_frame(writer, request, self.limits)
        response = pb.ClientResponse()
        response.ParseFromString(await read_frame(reader, self.limits))
        return response

    def request(self, message_id="req-1", **kwargs):
        fields = dict(message_id=message_id, timestamp=int(time.time()), type=1)
        fields.update(kwargs)
        return pb.ClientRequest(**fields)

    async def test_multiple_requests_use_one_upstream_connection_and_preserve_ids(self):
        reader, writer = await self.connect()
        for index in range(3):
            response = await self.exchange(reader, writer, self.request(f"req-{index}"))
            self.assertTrue(response.success)
            self.assertEqual(response.message_id, f"req-{index}")
        self.assertEqual(self.connections, 1)
        self.assertEqual(len(self.requests), 3)

    async def test_invalid_command_is_rejected_before_gateway_connection(self):
        reader, writer = await self.connect()
        request = self.request(type=2, target_device_id="camera_pici_01")
        request.command_payload.CopyFrom(pb.ConfigCommand(command_id="cmd-1", timestamp=int(time.time()),
                                                          target_device_id="camera_pici_01", update_frequency=True,
                                                          new_frequency_secs=0))
        response = await self.exchange(reader, writer, request)
        self.assertFalse(response.success)
        self.assertIn("invalid_frequency", response.message)
        self.assertEqual(response.message_id, request.message_id)
        self.assertEqual(self.connections, 0)
        # O mesmo cliente pode corrigir a requisição sem reabrir a conexão.
        self.assertTrue((await self.exchange(reader, writer, self.request("req-fixed"))).success)

    async def test_invalid_query_window_and_request_type_are_rejected(self):
        reader, writer = await self.connect()
        queries = (self.request(type=0), self.request(type=3, query_metric="temperature", query_op=1,
                                                     start_timestamp=100, end_timestamp=99))
        for request in queries:
            self.assertFalse((await self.exchange(reader, writer, request)).success)
        self.assertEqual(self.connections, 0)

    async def test_foreign_source_cannot_open_upstream_connection(self):
        self.hub.directory.addresses["dashboard"] = {"127.0.0.2"}
        reader, writer = await self.connect()
        self.assertEqual(await reader.read(), b"")
        self.assertEqual(self.connections, 0)
        self.assertTrue(any(entry.get("reason") == "unauthorized_source" for entry in self.audit))

    async def test_oversized_frame_is_rejected_without_allocating_body(self):
        reader, writer = await self.connect()
        writer.write(struct.pack("!I", self.limits.max_frame + 1))
        await writer.drain()
        response = pb.ClientResponse()
        response.ParseFromString(await read_frame(reader, self.limits))
        self.assertFalse(response.success)
        self.assertIn("invalid_frame_size", response.message)
        self.assertEqual(self.connections, 0)

    async def test_truncated_body_and_timeout_close_connection(self):
        reader, writer = await self.connect()
        writer.write(struct.pack("!I", 20) + b"small")
        await writer.drain()
        response = pb.ClientResponse()
        # A leitura do hub expira e libera a capacidade sem aguardar o resto do corpo.
        data = await asyncio.wait_for(reader.readexactly(4), 1)
        size = struct.unpack("!I", data)[0]
        response.ParseFromString(await reader.readexactly(size))
        self.assertFalse(response.success)
        self.assertEqual(await reader.read(), b"")
        self.assertEqual(self.connections, 0)

    async def test_mismatched_gateway_response_id_is_rejected(self):
        self.bad_response_id = True
        reader, writer = await self.connect()
        response = await self.exchange(reader, writer, self.request("original-id"))
        self.assertFalse(response.success)
        self.assertEqual(response.message_id, "original-id")
        self.assertIn("response_id_mismatch", response.message)

    async def test_concurrency_capacity_is_enforced(self):
        self.hub.limits = replace(self.limits, max_clients=1, request_timeout=1)
        reader1, writer1 = await self.connect()
        reader2, writer2 = await self.connect()
        self.assertEqual(await reader2.read(), b"")
        self.assertEqual(self.hub.active_clients, 1)
        self.assertTrue(any(entry.get("reason") == "client_capacity" for entry in self.audit))

    async def test_shutdown_cancels_persistent_idle_connection_immediately(self):
        self.hub.limits = replace(self.limits, request_timeout=60)
        reader, writer = await self.connect()
        self.assertTrue((await self.exchange(reader, writer, self.request())).success)
        await asyncio.wait_for(self.hub.close(), 1)
        self.assertEqual(await reader.read(), b"")
        self.assertEqual(self.hub.active_clients, 0)


class HubUtilityTests(unittest.TestCase):
    def test_audit_counts_every_packet_but_bounds_repeated_logs(self):
        entries = []
        audit = Audit("test-hub", entries.append)
        for index in range(1000):
            audit.record("rejected", "rate_limit", "127.0.0.1")
        self.assertEqual(audit.counts["rejected:rate_limit"], 1000)
        self.assertEqual(len(entries), 15)
        self.assertEqual(entries[-1]["count"], 1000)

    def test_limiter_rejects_burst_and_bounds_source_state(self):
        limiter = RateLimiter(rate=0.1, burst=1, max_sources=2)
        self.assertTrue(limiter.allow("source1"))
        self.assertFalse(limiter.allow("source1"))
        self.assertTrue(limiter.allow("source2"))
        self.assertTrue(limiter.allow("source3"))
        self.assertEqual(len(limiter.sources), 2)

    def test_all_sensor_families_have_unique_dns_profile(self):
        from hubs.main import DEFAULT_PROFILES
        profiles = {profile.prefix: profile for profile in DEFAULT_PROFILES}
        self.assertEqual(len(profiles), 7)
        self.assertEqual(profiles["water_"].device_type, 7)
        self.assertEqual(profiles["waste_"].device_type, 8)
        self.assertEqual(profiles["waste_"].control_port, 0)


class HostDirectoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_dns_refresh_eventually_revokes_old_authorization(self):
        directory = HostDirectory(("sensor_camera",), refresh=1)
        directory.addresses["sensor_camera"] = {"127.0.0.2"}
        directory.updated["sensor_camera"] = time.monotonic() - 4
        loop = asyncio.get_running_loop()
        with patch.object(loop, "getaddrinfo", new=AsyncMock(side_effect=OSError("dns unavailable"))):
            await directory.update()
        self.assertFalse(directory.permits("sensor_camera", "127.0.0.2"))
        with self.assertRaises(ConnectionError):
            directory.address("sensor_camera")

    async def test_literal_authorized_address_does_not_require_dns(self):
        directory = HostDirectory(("127.0.0.1",))
        await directory.update()
        self.assertTrue(directory.permits("127.0.0.1", "127.0.0.1"))
        self.assertFalse(directory.permits("127.0.0.1", "127.0.0.2"))


if __name__ == "__main__":
    unittest.main()
