import concurrent.futures
import math
import os
import socket
import struct
import threading
import time
import unittest
from unittest.mock import patch

import sensor


class CameraTelemetryTests(unittest.TestCase):
    def test_fleet_distributes_nine_unique_cameras_across_sectors(self):
        devices = sensor.build_device_fleet(9)
        self.assertEqual(len(devices), 9)
        for slug in ("pici", "benfica", "porangabussu"):
            for ordinal in range(1, 4):
                self.assertIn(f"camera_{slug}_{ordinal:02d}", devices)

    def test_traffic_readings_remain_physically_consistent(self):
        expected_units = {
            "vehicles_count": "veh/min", "infractions": "count",
            "average_speed": "km/h", "road_occupancy": "%",
            "accidents": "count", "pedestrians_count": "pedestrians/min",
            "detection_confidence": "%", "heavy_vehicles_count": "veh/min",
        }
        for _ in range(500):
            metrics = sensor.build_traffic_metrics()
            self.assertEqual({m.name: m.unit for m in metrics}, expected_units)
            self.assertEqual(len(metrics), len(expected_units))
            values = {m.name: m.value for m in metrics}
            self.assertTrue(all(math.isfinite(v) and v >= 0 for v in values.values()))
            self.assertLessEqual(values["infractions"], values["vehicles_count"])
            self.assertLessEqual(values["heavy_vehicles_count"], values["vehicles_count"])
            self.assertGreaterEqual(values["average_speed"], 8.0)
            self.assertLessEqual(values["average_speed"], 70.0)
            self.assertGreaterEqual(values["road_occupancy"], 0.0)
            self.assertLessEqual(values["road_occupancy"], 100.0)
            self.assertGreaterEqual(values["detection_confidence"], 85.0)
            self.assertLessEqual(values["detection_confidence"], 99.8)


class CameraControlTests(unittest.TestCase):
    def setUp(self):
        self.devices = sensor.build_device_fleet(3)
        self.fleet_patch = patch.object(sensor, "DEVICES", self.devices)
        self.cache_patch = patch.object(sensor, "_recent_commands", {})
        self.fleet_patch.start()
        self.cache_patch.start()
        self.addCleanup(self.fleet_patch.stop)
        self.addCleanup(self.cache_patch.stop)

    def command(self, **overrides):
        values = dict(
            command_id="CMD-TEST-001",
            timestamp=int(time.time()),
            target_device_id="camera_pici_01",
            update_status=True,
            target_status=sensor.messages_pb2.STATUS_OFF,
            update_frequency=True,
            new_frequency_secs=15,
        )
        values.update(overrides)
        return sensor.messages_pb2.ConfigCommand(**values)

    def exchange(self, command):
        client, server = socket.socketpair()
        client.settimeout(5)
        with patch.object(sensor, "send_discovery_response") as discovery:
            worker = threading.Thread(target=sensor.handle_control_client, args=(server, ("local", 0)))
            worker.start()
            try:
                payload = command.SerializeToString()
                frame = struct.pack(">I", len(payload)) + payload
                # Os prefixos/corpos não são garantidos em um único recv TCP.
                for offset in range(0, len(frame), 3):
                    client.sendall(frame[offset:offset + 3])
                size = struct.unpack(">I", sensor.recv_exact(client, 4))[0]
                response = sensor.messages_pb2.ConfigResponse()
                response.ParseFromString(sensor.recv_exact(client, size))
            finally:
                client.close()
                worker.join(5)
            self.assertFalse(worker.is_alive(), "handler TCP não encerrou")
            return response, discovery.call_count

    def test_tcp_applies_to_target_and_returns_correlated_response(self):
        response, discoveries = self.exchange(self.command())
        self.assertTrue(response.success)
        self.assertEqual(response.command_id, "CMD-TEST-001")
        self.assertEqual(response.updated_status, sensor.messages_pb2.STATUS_OFF)
        self.assertEqual(response.updated_frequency_secs, 15)
        self.assertEqual(discoveries, 1)
        self.assertEqual(self.devices["camera_benfica_01"]["status"], sensor.messages_pb2.STATUS_ON)

    def test_replay_cannot_reverse_previously_applied_command(self):
        first, _ = self.exchange(self.command())
        replay, discoveries = self.exchange(self.command(
            target_status=sensor.messages_pb2.STATUS_ON, new_frequency_secs=1))
        self.assertTrue(first.success)
        self.assertFalse(replay.success)
        self.assertIn("ja processado", replay.message)
        self.assertEqual(replay.updated_status, sensor.messages_pb2.STATUS_OFF)
        self.assertEqual(replay.updated_frequency_secs, 15)
        self.assertEqual(discoveries, 0)

    def test_replay_is_atomic_for_concurrent_connections(self):
        command = self.command()
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(lambda _: sensor.apply_control_command(command), range(24)))
        self.assertEqual(sum(error is None for _, error in results), 1)
        self.assertEqual(self.devices[command.target_device_id]["frequency_secs"], 15)

    def test_invalid_command_does_not_change_state_or_consume_id(self):
        response, discoveries = self.exchange(self.command(new_frequency_secs=61))
        self.assertFalse(response.success)
        self.assertEqual(response.updated_status, sensor.messages_pb2.STATUS_ON)
        self.assertEqual(response.updated_frequency_secs, 5)
        self.assertEqual(discoveries, 0)
        _, error = sensor.apply_control_command(self.command())
        self.assertIsNone(error)

    def test_cache_keeps_replays_until_command_timestamp_expires(self):
        with patch.object(sensor.time, "monotonic", return_value=100.0):
            _, error = sensor.apply_control_command(self.command())
        self.assertIsNone(error)
        with patch.object(sensor.time, "monotonic", return_value=100.0 + sensor.COMMAND_REPLAY_WINDOW_SECS):
            _, error = sensor.apply_control_command(self.command(command_id="CMD-NEW"))
        self.assertIsNone(error)
        self.assertNotIn("CMD-TEST-001", sensor._recent_commands)

    def test_full_cache_rejects_new_command_without_evicting_replay_guard(self):
        sensor._recent_commands.update({f"CMD-{i}": time.monotonic() for i in range(sensor.MAX_RECENT_COMMANDS)})
        _, error = sensor.apply_control_command(self.command())
        self.assertIn("limite", error)
        self.assertEqual(self.devices["camera_pici_01"]["status"], sensor.messages_pb2.STATUS_ON)
        self.assertEqual(len(sensor._recent_commands), sensor.MAX_RECENT_COMMANDS)

    def test_manual_status_survives_scheduled_telemetry(self):
        sensor.apply_control_command(self.command())
        with patch.object(sensor, "random_device_status", return_value=sensor.messages_pb2.STATUS_ON):
            snapshot = sensor.prepare_device_for_telemetry("camera_pici_01")
        self.assertEqual(snapshot[2], sensor.messages_pb2.STATUS_OFF)

    def test_nonfinite_heartbeat_config_uses_finite_default(self):
        for raw in ("nan", "inf", "-inf"):
            with self.subTest(raw=raw), patch.dict(os.environ, SENSOR_HEARTBEAT_INTERVAL_SECS=raw):
                self.assertEqual(sensor.env_float("SENSOR_HEARTBEAT_INTERVAL_SECS", 10.0, 1.0), 10.0)


if __name__ == "__main__":
    unittest.main()
