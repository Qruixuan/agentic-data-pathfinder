"""Focused guards for the seven-question token-cost export."""

from __future__ import annotations

from decimal import Decimal
import sqlite3

import pytest

from experiments.multiq_pilot_20260924.audit_24route_cost import _qwen
from experiments.multiq_pilot_20260924.export_n6_usage_numeric import export


def test_qwen_list_price_separates_cached_input_from_output() -> None:
    assert _qwen(1_000_000, 250_000, 100_000) == Decimal("0.700")


def test_n6_export_contains_only_digest_bound_numeric_usage(tmp_path) -> None:
    database = tmp_path / "usage.sqlite3"
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
            ("a" * 64, "b" * 64, 100, 40, 20, 120),
        )
        connection.commit()
    finally:
        connection.close()
    report = export(database)
    assert report["record_count"] == 1
    assert report["rows"] == [{
        "result_sha256": "a" * 64,
        "request_sha256": "b" * 64,
        "input_units": 100,
        "cached_input_units": 40,
        "output_units": 20,
        "total_units": 120,
    }]
    assert report["credentials_recorded"] is False


def test_n6_export_rejects_invalid_cache_count(tmp_path) -> None:
    database = tmp_path / "usage.sqlite3"
    connection = sqlite3.connect(database)
    try:
        connection.execute("""
            CREATE TABLE n6_provider_usage (
                result_sha256 TEXT, request_sha256 TEXT,
                input_units INTEGER, cached_input_units INTEGER,
                output_units INTEGER, total_units INTEGER
            )
        """)
        connection.execute(
            "INSERT INTO n6_provider_usage VALUES (?, ?, ?, ?, ?, ?)",
            ("a" * 64, "b" * 64, 100, 101, 20, 120),
        )
        connection.commit()
    finally:
        connection.close()
    with pytest.raises(ValueError, match="invalid"):
        export(database)
