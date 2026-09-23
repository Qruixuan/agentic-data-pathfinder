"""Fail-closed file and publication checks for the 24-route input binding."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pathfinder.simulator.interleaved_multiq_route_bindings import (
    InterleavedRouteBindingsError,
    freeze_interleaved_route_bindings,
    verify_interleaved_route_bindings,
)


class InterleavedRouteBindingTests(unittest.TestCase):
    @staticmethod
    def _expected(**_sources):
        return ({
            "schema_version": "fixture",
            "route_count": 24,
            "data_agent_binding_count": 42,
            "manifest_sha256": "a" * 64,
        }, [{"trial_key": f"trial-{index}"} for index in range(24)])

    def test_freeze_verify_and_reused_target_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "routes"
            with patch(
                "pathfinder.simulator.interleaved_multiq_route_bindings._expected",
                side_effect=self._expected,
            ):
                report = freeze_interleaved_route_bindings(output_dir=output)
                self.assertEqual(report["route_count"], 24)
                self.assertFalse(report["runtime_admission_created"])
                self.assertFalse(report["workflow_submitted"])
                with self.assertRaises(InterleavedRouteBindingsError):
                    freeze_interleaved_route_bindings(output_dir=output)
                rows = (output / "route-inputs.jsonl").read_text(
                    encoding="utf-8"
                ).splitlines()
                self.assertEqual(len(rows), 24)
                self.assertEqual(json.loads(rows[0])["trial_key"], "trial-0")

    def test_tampered_content_and_extra_file_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "routes"
            with patch(
                "pathfinder.simulator.interleaved_multiq_route_bindings._expected",
                side_effect=self._expected,
            ):
                freeze_interleaved_route_bindings(output_dir=output)
                route_file = output / "route-inputs.jsonl"
                original = route_file.read_bytes()
                route_file.write_bytes(original + b"{}\n")
                with self.assertRaises(InterleavedRouteBindingsError):
                    verify_interleaved_route_bindings(output)
                route_file.write_bytes(original)
                (output / "unexpected").write_bytes(b"x")
                with self.assertRaises(InterleavedRouteBindingsError):
                    verify_interleaved_route_bindings(output)


if __name__ == "__main__":
    unittest.main()
