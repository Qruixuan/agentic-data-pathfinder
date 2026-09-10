from __future__ import annotations

import unittest

from pathfinder.simulator.container_calibration import (
    ContainerCalibrationError,
    _fit_storage_model,
    _p95,
)


class ContainerStorageFitTest(unittest.TestCase):
    def test_recovers_nonnegative_latency_and_throughput(self) -> None:
        rows = []
        for size in (1_000_000, 5_000_000, 10_000_000, 20_000_000):
            for noise in (-0.1, 0.1):
                rows.append({
                    "logical_bytes": size,
                    "service_time_ms": (
                        2.0 + size / 100_000_000 * 1000.0 + noise
                    ),
                })
        result = _fit_storage_model(rows)
        self.assertEqual("FITTED", result["status"])
        self.assertAlmostEqual(2.0, result["base_latency_ms"], places=9)
        self.assertAlmostEqual(
            100_000_000,
            result["throughput_bytes_per_second"],
            places=2,
        )

    def test_refuses_a_narrow_byte_span(self) -> None:
        result = _fit_storage_model([
            {"logical_bytes": size, "service_time_ms": service}
            for size, service in (
                (100, 1.0),
                (100, 1.1),
                (200, 1.2),
                (200, 1.3),
            )
        ])
        self.assertEqual("NOT_FITTED", result["status"])
        self.assertEqual("byte_size_span_below_fourfold", result["reason"])

    def test_refuses_nonpositive_scaling(self) -> None:
        result = _fit_storage_model([
            {"logical_bytes": size, "service_time_ms": service}
            for size, service in (
                (100, 4.0),
                (100, 4.1),
                (1_000, 3.0),
                (1_000, 3.1),
            )
        ])
        self.assertEqual("NOT_FITTED", result["status"])
        self.assertEqual("non_positive_size_service_slope", result["reason"])

    def test_p95_is_nearest_rank(self) -> None:
        self.assertEqual(18.0, _p95([float(value) for value in range(20)]))
        with self.assertRaises(ContainerCalibrationError):
            _p95([])


if __name__ == "__main__":
    unittest.main()
