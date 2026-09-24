"""Focused guards for the seven-question token-cost export."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
import sqlite3
import tempfile
import unittest

from experiments.multiq_pilot_20260924.audit_24route_cost import _qwen
from experiments.multiq_pilot_20260924.export_n6_usage_numeric import export
from experiments.multiq_pilot_20260924.replay_28_baselines import _choice


def _database(path: Path, *, cached: int) -> None:
    database = path
    connection = sqlite3.connect(database)
    try:
        connection.execute("""
            CREATE TABLE n6_provider_usage (
                result_sha256 TEXT PRIMARY KEY, request_sha256 TEXT,
                input_units INTEGER, cached_input_units INTEGER,
                output_units INTEGER, total_units INTEGER
            )
        """)
        connection.execute(
            "INSERT INTO n6_provider_usage VALUES (?, ?, ?, ?, ?, ?)",
            ("a" * 64, "b" * 64, 100, cached, 20, 120),
        )
        connection.commit()
    finally:
        connection.close()


class SealedCostTests(unittest.TestCase):
    def test_first_video_question_uses_raw_then_index(self) -> None:
        self.assertEqual(_choice("first-R-then-I", 0), "R")
        self.assertEqual(_choice("first-R-then-I", 1), "I")
        self.assertEqual(_choice("always-DC", 8), "DC")

    def test_qwen_list_price_separates_cached_input_from_output(self) -> None:
        self.assertEqual(
            _qwen(1_000_000, 250_000, 100_000), Decimal("0.700"),
        )

    def test_n6_export_contains_only_numeric_digest_bound_usage(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            database = Path(root) / "usage.sqlite3"
            _database(database, cached=40)
            report = export(database)
        self.assertEqual(report["record_count"], 1)
        self.assertEqual(report["rows"], [{
            "result_sha256": "a" * 64,
            "request_sha256": "b" * 64,
            "input_units": 100,
            "cached_input_units": 40,
            "output_units": 20,
            "total_units": 120,
        }])
        self.assertIs(report["credentials_recorded"], False)

    def test_n6_export_rejects_invalid_cache_count(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            database = Path(root) / "usage.sqlite3"
            _database(database, cached=101)
            with self.assertRaisesRegex(ValueError, "invalid"):
                export(database)
