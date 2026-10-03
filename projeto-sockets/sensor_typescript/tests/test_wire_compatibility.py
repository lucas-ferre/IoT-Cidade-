"""Independent wire oracle: parse the TypeScript encoder using generated Protobuf."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


class TypeScriptWireCompatibilityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sensor_root = Path(__file__).resolve().parents[1]
        cls.temporary = tempfile.TemporaryDirectory()
        common = cls.sensor_root.parent / "common"
        compiler = shutil.which("protoc")
        command = [compiler] if compiler else [sys.executable, "-m", "grpc_tools.protoc"]
        subprocess.run(command + [
            f"--proto_path={common}", f"--python_out={cls.temporary.name}", str(common / "messages.proto"),
        ], check=True, capture_output=True, text=True, timeout=30)
        specification = importlib.util.spec_from_file_location(
            "wire_messages_pb2", Path(cls.temporary.name) / "messages_pb2.py"
        )
        cls.messages = importlib.util.module_from_spec(specification)
        specification.loader.exec_module(cls.messages)
        node = os.environ.get("NODE_BINARY") or shutil.which("node")
        if not node:
            raise RuntimeError("Node >=22.18 required for wire compatibility test")
        output = subprocess.check_output([
            node, "--experimental-strip-types", "tests/wire_fixture.ts"
        ], cwd=cls.sensor_root, text=True, timeout=30)
        cls.fixture = json.loads(output)

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    def test_discovery_all_fields_and_large_int64_timestamp(self):
        message = self.messages.DiscoveryResponse.FromString(bytes.fromhex(self.fixture["discovery"]))
        self.assertEqual(message.message_id, "fixture-discovery")
        self.assertEqual(message.timestamp, 1099511627776)
        self.assertEqual(message.device_id, "waste_centro_01")
        self.assertEqual(message.type, self.messages.DEVICE_TYPE_WASTE_SENSOR)
        self.assertEqual(message.ip_address, "sensor_lixeiras")
        self.assertEqual(message.initial_status, self.messages.STATUS_ON)
        self.assertFalse(message.is_controllable)
        self.assertEqual(message.control_port, 0)

    def test_metrics_fixed64_utf8_negative_values_and_field_order(self):
        message = self.messages.DataPayload.FromString(bytes.fromhex(self.fixture["telemetry"]))
        self.assertEqual(message.message_id, "fixture-telemetry")
        self.assertEqual(message.timestamp, 1099511627776)
        self.assertEqual(message.device_id, "waste_centro_01")
        self.assertEqual(message.current_status, self.messages.STATUS_ON)
        self.assertEqual([(metric.name, metric.value, metric.unit) for metric in message.metrics], [
            ("waste_fill_level", 42.5, "%"), ("signal_strength", -72.25, "dBm"),
            ("unicode_á", 0.125, "µg/m³"),
        ])


if __name__ == "__main__":
    unittest.main()
