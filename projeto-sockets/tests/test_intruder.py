"""Cenários do invasor e prova de que o hub não os repassa ao gateway."""

from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.append(str(ROOT / "gateway"))

from hubs.common import HostDirectory, Limits
from hubs.main import AccessHub, SensorHub
from sensor_intruder import main as intruder


class DatagramCollector(asyncio.DatagramProtocol):
    def __init__(self):
        self.queue = asyncio.Queue()

    def datagram_received(self, data, address):
        self.queue.put_nowait((data, address))


class IntruderTests(unittest.TestCase):
    def test_generator_is_finite_and_has_all_security_scenarios(self):
        scenarios = intruder.build_scenarios(now=1000000)
        self.assertEqual(len(scenarios), 12)
        self.assertEqual(len({scenario.name for scenario in scenarios}), 12)
        self.assertEqual(sum(scenario.channel == "access" for scenario in scenarios), 3)
        self.assertEqual(max(len(scenario.payload) for scenario in scenarios), 16385)

    def test_main_does_nothing_without_explicit_enable(self):
        with patch.dict("os.environ", {"INTRUDER_ENABLED": "0"}), patch.object(intruder, "resolve_lab_host") as resolve, patch.object(intruder, "emit"):
            self.assertEqual(intruder.main(), 0)
            resolve.assert_not_called()

    def test_public_destination_and_multicast_are_rejected(self):
        for address in ("8.8.8.8", "239.0.0.1", "0.0.0.0"):
            with patch.object(intruder.socket, "getaddrinfo", return_value=[(2, 1, 6, "", (address, 0))]):
                with self.assertRaises(ValueError):
                    intruder.resolve_lab_host("configured-host")

    def test_loopback_destination_is_accepted(self):
        with patch.object(intruder.socket, "getaddrinfo", return_value=[(2, 1, 6, "", ("127.0.0.1", 0))]):
            self.assertEqual(intruder.resolve_lab_host("localhost"), "127.0.0.1")

    def test_configuration_cannot_create_unbounded_rounds_or_rate(self):
        for name, value, default, minimum, maximum in (
            ("INTRUDER_ROUNDS", "11", 1, 1, 10),
            ("INTRUDER_INTERVAL_SECS", "0", 0.15, 0.02, 5),
        ):
            with patch.dict("os.environ", {name: value}):
                with self.assertRaises(ValueError):
                    intruder.bounded_number(name, default, minimum, maximum)


class IntruderIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_every_intruder_scenario_stays_out_of_gateway(self):
        audit = []
        loop = asyncio.get_running_loop()
        collector = DatagramCollector()
        backend, _ = await loop.create_datagram_endpoint(lambda: collector, local_addr=("127.0.0.1", 0))
        backend_port = backend.get_extra_info("sockname")[1]
        hosts = {"gateway": {"127.0.0.1"}, "sensor_camera": {"127.0.0.2"}, "dashboard": {"127.0.0.2"}}
        sensor_hub = SensorHub(telemetry_port=0, discovery_port=0, control_port=0,
                               gateway_telemetry_port=backend_port, gateway_discovery_port=backend_port,
                               directory=HostDirectory([], fixed=hosts), bind_host="127.0.0.1", audit_sink=audit.append)
        access_hub = AccessHub(access_port=0, directory=HostDirectory([], fixed=hosts),
                              bind_host="127.0.0.1", audit_sink=audit.append,
                              limits=Limits(request_timeout=0.5))
        sender, _ = await loop.create_datagram_endpoint(asyncio.DatagramProtocol, local_addr=("127.0.0.1", 0))
        try:
            await sensor_hub.start()
            await access_hub.start()
            for scenario in intruder.build_scenarios():
                if scenario.channel == "access":
                    result = await asyncio.to_thread(intruder.send_access_scenario, "127.0.0.1", access_hub.access_port, scenario.payload)
                    self.assertEqual(result, "connection_closed_by_hub")
                else:
                    port = sensor_hub.discovery_port if scenario.channel == "discovery" else sensor_hub.telemetry_port
                    sender.sendto(scenario.payload, ("127.0.0.1", port))
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(collector.queue.get(), 0.1)
            self.assertFalse(sensor_hub.routes)
            self.assertFalse(any(entry.get("event") == "forwarded" for entry in audit))
            reasons = {entry.get("reason") for entry in audit}
            self.assertTrue({"unknown_device", "source_identity_mismatch", "malformed_protobuf",
                             "invalid_timestamp", "non_finite_metric", "invalid_datagram_size",
                             "unauthorized_source"}.issubset(reasons), reasons)
        finally:
            await sensor_hub.close()
            await access_hub.close()
            sender.close()
            backend.close()


if __name__ == "__main__":
    unittest.main()
