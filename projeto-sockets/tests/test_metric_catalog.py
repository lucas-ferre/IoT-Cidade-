"""O catálogo da UI deve permitir consultar todas as famílias e unidades."""

import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "client"))
import messages_pb2 as pb
from metric_catalog import DEVICE_METRICS_MAP, METRICS, METRIC_OPTIONS


class MetricCatalogTests(unittest.TestCase):
    def test_all_device_metrics_are_queryable_and_have_units(self):
        options = {key for _, key in METRIC_OPTIONS}
        for device_type, names in DEVICE_METRICS_MAP.items():
            with self.subTest(device_type=device_type):
                self.assertGreaterEqual(len(names), 8)
                self.assertEqual(len(names), len(set(names)))
                for name in names:
                    self.assertIn(name, options)
                    self.assertTrue(METRICS[name][1])

    def test_new_languages_and_distinct_wait_units(self):
        self.assertIn("water_ph", DEVICE_METRICS_MAP[pb.DEVICE_TYPE_WATER_SENSOR])
        self.assertIn("waste_weight", DEVICE_METRICS_MAP[pb.DEVICE_TYPE_WASTE_SENSOR])
        self.assertEqual(METRICS["average_wait"][1], "seconds")
        self.assertEqual(METRICS["parking_wait_time"][1], "min")
        self.assertNotIn("average_wait", DEVICE_METRICS_MAP[pb.DEVICE_TYPE_PARKING_SENSOR])

    def test_options_cover_catalog_without_duplicates(self):
        self.assertEqual(len(METRIC_OPTIONS), len(METRICS))
        self.assertEqual(len({key for _, key in METRIC_OPTIONS}), len(METRIC_OPTIONS))


if __name__ == "__main__":
    unittest.main()
