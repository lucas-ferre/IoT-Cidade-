"""Testes unitários das funções analíticas puras do gateway."""

from __future__ import annotations

import math
import sys
import unittest
from pathlib import Path


GATEWAY_DIR = Path(__file__).resolve().parents[1] / "gateway"
sys.path.insert(0, str(GATEWAY_DIR))

from analytics import (  # noqa: E402
    OlapSource,
    build_rollup_rows,
    choose_olap_source,
    choose_retained_olap_source,
    downsample_graph_rows,
    olap_time_range,
    sample_stddev,
    validate_time_window,
)


class OlapSourceTests(unittest.TestCase):
    def test_selects_source_at_each_window_boundary(self) -> None:
        start = 1_000_000

        self.assertEqual(
            choose_olap_source(start, start + 3_600, 3_600, 86_400, 604_800).name,
            "raw",
        )
        self.assertEqual(
            choose_olap_source(start, start + 3_601, 3_600, 86_400, 604_800).name,
            "rollup_1m",
        )
        self.assertEqual(
            choose_olap_source(start, start + 86_401, 3_600, 86_400, 604_800).name,
            "rollup_5m",
        )
        self.assertEqual(
            choose_olap_source(start, start + 604_801, 3_600, 86_400, 604_800).name,
            "rollup_1h",
        )

    def test_rejects_reversed_window_and_non_monotonic_limits(self) -> None:
        with self.assertRaisesRegex(ValueError, "start_timestamp"):
            choose_olap_source(20, 10, 60, 300, 3_600)
        with self.assertRaisesRegex(ValueError, "crescentes"):
            choose_olap_source(10, 20, 300, 60, 3_600)

    def test_enforces_explicit_maximum_window(self) -> None:
        self.assertEqual(validate_time_window(100, 160, 60), 60)
        with self.assertRaisesRegex(ValueError, "excede o máximo"):
            validate_time_window(100, 161, 60)


class RetainedOlapSourceTests(unittest.TestCase):
    DAY = 24 * 3_600
    NOW = 400 * DAY

    def choose(self, start: int, window: int, **overrides: int) -> OlapSource:
        retention = {
            "now_timestamp": self.NOW,
            "raw_retention_secs": 7 * self.DAY,
            "rollup_1m_retention_secs": 30 * self.DAY,
            "rollup_5m_retention_secs": 180 * self.DAY,
            "rollup_1h_retention_secs": 365 * self.DAY,
        }
        retention.update(overrides)
        return choose_retained_olap_source(
            start, start + window, 3_600, self.DAY, 7 * self.DAY, **retention,
        )

    def test_unlimited_retention_preserves_duration_selection(self) -> None:
        start = self.NOW - 20 * self.DAY
        for window in (3_600, 3_601, self.DAY, self.DAY + 1, 7 * self.DAY + 1):
            with self.subTest(window=window):
                actual = self.choose(
                    start, window,
                    raw_retention_secs=0,
                    rollup_1m_retention_secs=0,
                    rollup_5m_retention_secs=0,
                    rollup_1h_retention_secs=0,
                )
                expected = choose_olap_source(
                    start, start + window, 3_600, self.DAY, 7 * self.DAY,
                )
                self.assertEqual(actual, expected)

    def test_expired_finer_sources_use_retained_history(self) -> None:
        cases = (
            (8, 1_800, "rollup_1m"),
            (35, 7_200, "rollup_5m"),
            (181, 7_200, "rollup_1h"),
        )
        for age_days, window, expected in cases:
            with self.subTest(age_days=age_days):
                source = self.choose(self.NOW - age_days * self.DAY, window)
                self.assertEqual(source.name, expected)

    def test_retention_must_cover_start_instead_of_only_window_end(self) -> None:
        # The end is inside raw retention, but the first half hour has expired.
        start = self.NOW - 7 * self.DAY - 1_800
        self.assertEqual(self.choose(start, 3_600).name, "rollup_1m")

    def test_raw_retention_cutoff_is_inclusive(self) -> None:
        cutoff = self.NOW - 7 * self.DAY
        self.assertEqual(self.choose(cutoff, 60).name, "raw")
        self.assertEqual(self.choose(cutoff - 1, 60).name, "rollup_1m")

    def test_rollup_coverage_respects_bucket_retention_cutoff(self) -> None:
        # bucket_start=9000 survives cutoff 9000, but expires at cutoff 9001.
        options = {
            "now_timestamp": 10_000,
            "raw_retention_secs": 100,
            "rollup_5m_retention_secs": 0,
        }
        self.assertEqual(
            self.choose(9_001, 1, rollup_1m_retention_secs=1_000, **options).name,
            "rollup_1m",
        )
        self.assertEqual(
            self.choose(9_001, 1, rollup_1m_retention_secs=999, **options).name,
            "rollup_5m",
        )

    def test_zero_retention_keeps_each_source_available_indefinitely(self) -> None:
        start = self.NOW - 399 * self.DAY
        cases = (
            (1_800, "raw_retention_secs", "raw"),
            (7_200, "rollup_1m_retention_secs", "rollup_1m"),
            (self.DAY + 1, "rollup_5m_retention_secs", "rollup_5m"),
            (7 * self.DAY + 1, "rollup_1h_retention_secs", "rollup_1h"),
        )
        for window, retention_field, expected in cases:
            with self.subTest(source=expected):
                self.assertEqual(
                    self.choose(start, window, **{retention_field: 0}).name,
                    expected,
                )

    def test_no_retained_source_falls_back_to_one_hour(self) -> None:
        self.assertEqual(self.choose(self.NOW - 366 * self.DAY, 60).name, "rollup_1h")

    def test_invalid_retention_clock_and_window_are_rejected(self) -> None:
        for field in (
            "raw_retention_secs",
            "rollup_1m_retention_secs",
            "rollup_5m_retention_secs",
            "rollup_1h_retention_secs",
        ):
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, "retenções"):
                    self.choose(self.NOW - self.DAY, 60, **{field: -1})
        with self.assertRaisesRegex(ValueError, "now_timestamp"):
            self.choose(0, 60, now_timestamp=-1)
        with self.assertRaisesRegex(ValueError, "start_timestamp"):
            self.choose(self.NOW, -1)


class BucketTests(unittest.TestCase):
    def test_raw_range_is_unchanged(self) -> None:
        source = OlapSource("raw", "metrics", 0, False)
        self.assertEqual(olap_time_range(source, 61, 179), (61, 179))

    def test_rollup_range_is_aligned_to_bucket_start(self) -> None:
        source = OlapSource("rollup_1m", "metrics_rollup_1m", 60, True)
        self.assertEqual(olap_time_range(source, 61, 179), (60, 120))

    def test_rollup_requires_positive_bucket(self) -> None:
        source = OlapSource("invalid", "metrics_rollup", 0, True)
        with self.assertRaisesRegex(ValueError, "bucket_size"):
            olap_time_range(source, 60, 120)


class StatisticsTests(unittest.TestCase):
    def test_sample_standard_deviation_from_aggregated_moments(self) -> None:
        # Amostra clássica: 2, 4, 4, 4, 5, 5, 7, 9.
        result = sample_stddev(sample_count=8, value_sum=40.0, value_sum_sq=232.0)
        self.assertAlmostEqual(result, math.sqrt(32.0 / 7.0), places=12)

    def test_single_sample_has_zero_deviation(self) -> None:
        self.assertEqual(sample_stddev(1, 5.0, 25.0), 0.0)

    def test_non_finite_moments_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "finitos"):
            sample_stddev(2, math.inf, math.inf)


class RollupAggregationTests(unittest.TestCase):
    def test_aggregates_by_bucket_device_and_metric(self) -> None:
        rows = [
            ("dev-a", 61, "temperature", 10.0, "C"),
            ("dev-a", 119, "temperature", 14.0, ""),
            ("dev-a", 120, "temperature", 20.0, "C"),
            ("dev-b", 119, "temperature", 8.0, "C"),
        ]
        specs = (
            ("metrics_rollup_1m", 60, 86_400),
            ("metrics_rollup_5m", 300, 86_400),
        )

        result = build_rollup_rows(rows, specs)

        self.assertEqual(
            result["metrics_rollup_1m"],
            [
                (60, "dev-a", "temperature", "C", 2, 24.0, 296.0, 10.0, 14.0),
                (60, "dev-b", "temperature", "C", 1, 8.0, 64.0, 8.0, 8.0),
                (120, "dev-a", "temperature", "C", 1, 20.0, 400.0, 20.0, 20.0),
            ],
        )
        self.assertEqual(
            result["metrics_rollup_5m"],
            [
                (0, "dev-a", "temperature", "C", 3, 44.0, 696.0, 10.0, 20.0),
                (0, "dev-b", "temperature", "C", 1, 8.0, 64.0, 8.0, 8.0),
            ],
        )

    def test_rejects_non_finite_metric(self) -> None:
        with self.assertRaisesRegex(ValueError, "finitos"):
            build_rollup_rows(
                [("dev-a", 60, "temperature", math.nan, "C")],
                (("metrics_rollup_1m", 60, 86_400),),
            )


class GraphSamplingTests(unittest.TestCase):
    def test_sampling_is_deterministic_and_preserves_extremes(self) -> None:
        rows = [(timestamp, float(timestamp), "dev-a") for timestamp in range(10)]
        expected = [rows[0], rows[3], rows[6], rows[9]]

        self.assertEqual(downsample_graph_rows(rows, 4), expected)
        self.assertEqual(downsample_graph_rows(rows, 4), expected)

    def test_short_series_is_not_changed(self) -> None:
        rows = [(1, 1.0, "dev-a"), (2, 2.0, "dev-a")]
        self.assertEqual(downsample_graph_rows(rows, 10), rows)


if __name__ == "__main__":
    unittest.main()
