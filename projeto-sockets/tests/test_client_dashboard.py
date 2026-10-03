"""Regressões da transformação e interpretação dos dados no dashboard."""

from __future__ import annotations

import datetime
import math
import sys
import unittest
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


CLIENT_DIR = Path(__file__).resolve().parents[1] / "client"
sys.path.insert(0, str(CLIENT_DIR))

from dashboard_helpers import (  # noqa: E402
    assess_analytics_value,
    build_history_chart,
    build_history_frame,
)


@dataclass
class Point:
    timestamp: int
    value: float
    device_id: str = "sensor-a"


class HistoryChartTests(unittest.TestCase):
    def test_orders_points_across_midnight_using_complete_utc_dates(self) -> None:
        midnight = int(datetime.datetime(2026, 10, 3, tzinfo=datetime.timezone.utc).timestamp())
        result = build_history_chart([
            Point(midnight + 5, 20.0),
            Point(midnight - 5, 10.0),
        ])

        self.assertEqual(result["sensor-a"].tolist(), [10.0, 20.0])
        self.assertEqual(result.index[0], pd.Timestamp("2026-10-02T23:59:55Z"))
        self.assertEqual(result.index[1], pd.Timestamp("2026-10-03T00:00:05Z"))

    def test_same_clock_time_on_different_days_is_not_merged(self) -> None:
        result = build_history_chart([Point(100, 10.0), Point(100 + 86_400, 30.0)])

        self.assertEqual(len(result), 2)
        self.assertEqual(result["sensor-a"].tolist(), [10.0, 30.0])

    def test_groups_only_identical_instant_and_device(self) -> None:
        result = build_history_chart([
            Point(100, 10.0), Point(100, 14.0), Point(100, 50.0, "sensor-b")
        ])

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["sensor-a"], 12.0)
        self.assertEqual(result.iloc[0]["sensor-b"], 50.0)

    def test_inspection_keeps_duplicate_events_and_stable_order(self) -> None:
        result = build_history_frame([Point(101, 3.0), Point(100, 1.0), Point(100, 2.0)])

        self.assertEqual(result["timestamp"].tolist(), [100, 100, 101])
        self.assertEqual(result["value"].tolist(), [1.0, 2.0, 3.0])
        self.assertEqual(result["timestamp"].diff().tolist()[1:], [0.0, 1.0])

    def test_empty_history_remains_a_valid_empty_chart(self) -> None:
        self.assertTrue(build_history_frame([]).empty)
        self.assertTrue(build_history_chart([]).empty)


class AnalyticsAssessmentTests(unittest.TestCase):
    def test_small_temperature_deviation_does_not_create_critical_temperature_alert(self) -> None:
        self.assertIsNone(assess_analytics_value("temperature", 2.0, is_average=False))

    def test_aqi_variation_does_not_create_air_quality_category(self) -> None:
        self.assertIsNone(assess_analytics_value("aqi", 20.0, is_average=False))

    def test_mean_still_receives_reference_classification(self) -> None:
        self.assertEqual(assess_analytics_value("temperature", 22.0, is_average=True)[0], "success")
        self.assertEqual(assess_analytics_value("temperature", 30.0, is_average=True)[0], "warning")
        self.assertEqual(assess_analytics_value("temperature", 40.0, is_average=True)[0], "error")
        self.assertIn("Moderado", assess_analytics_value("aqi", 70.0, is_average=True)[1])

    def test_non_finite_result_is_not_reported_as_safe_or_critical(self) -> None:
        for value in (math.nan, math.inf, -math.inf):
            with self.subTest(value=value):
                self.assertEqual(assess_analytics_value("aqi", value, is_average=True)[0], "warning")

    def test_unknown_metric_has_no_invented_range(self) -> None:
        self.assertIsNone(assess_analytics_value("available_spaces", 20.0, is_average=True))


if __name__ == "__main__":
    unittest.main()
