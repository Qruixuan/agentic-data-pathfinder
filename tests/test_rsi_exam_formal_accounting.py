from __future__ import annotations

import json
import unittest

from pathfinder.rsi_exam.formal_accounting import (
    _accounting_document,
    _public_rows,
)


def _measurement(component: str, metric: str, value: float) -> dict:
    return {
        "component_id": component,
        "metric_id": metric,
        "value": value,
    }


def _row(
    case_id: str,
    design_id: str,
    family: str,
    node: str,
    *,
    origin_bytes: int,
    cache_bytes: int = 0,
    cache: bool = False,
) -> dict:
    measurements = []
    if origin_bytes:
        component = (
            "route-access-derived" if family != "raw" else "access-raw-artifact"
        )
        measurements += [
            _measurement(component, "bytes-read", origin_bytes),
            _measurement(component, "service-time", 1.0),
        ]
    if cache:
        measurements += [
            _measurement("cache-lookup", "bytes-read", cache_bytes),
            _measurement("cache-lookup", "service-time", 0.5),
        ]
    if family == "indexed-raw":
        measurements += [
            _measurement("query-index", "bytes-read", 11),
            _measurement("query-index", "bytes-sent", 7),
            _measurement("query-index", "service-time", 0.25),
        ]
    measurements += [
        _measurement("prepare-model-input", "service-time", 2.0),
        _measurement("infer", "service-time", 3.0),
        _measurement("score-hidden-answer", "service-time", 0.75),
    ]
    return {
        "case_id": case_id,
        "trial_key": f"workload|smoke-temporal|{design_id}|r0000",
        "result": {
            "status": "COMPLETE",
            "measurements": measurements,
            "semantic_route_evidence": {
                "design_id": design_id,
                "route": {
                    "route_family": family,
                    "executor_node_id": node,
                },
                "model_input": {
                    "semantic_input_profile_id": f"profile-{family}",
                    "payload_size_bytes": 321,
                },
                "scoring": {
                    "task_success": True,
                    "answer": "must-not-be-copied",
                },
                "n1_exactly_once_authenticated_score_verified": True,
                "prompt": "must-not-be-copied",
            },
        },
    }


def _ten_rows() -> list[dict]:
    return [
        _row("n7-raw", "D0", "raw", "N7", origin_bytes=1000),
        _row("n7-indexed", "D1", "indexed-raw", "N7", origin_bytes=200),
        _row("n7-derived", "D2", "remote-derived", "N7", origin_bytes=50),
        _row(
            "n7-cache-miss",
            "D3",
            "local-cache-derived",
            "N7",
            origin_bytes=50,
            cache=True,
        ),
        _row(
            "n7-cache-hit",
            "D3",
            "local-cache-derived",
            "N7",
            origin_bytes=0,
            cache_bytes=50,
            cache=True,
        ),
        _row("n8-raw", "D4", "raw", "N8", origin_bytes=1000),
        _row("n8-indexed", "D5", "indexed-raw", "N8", origin_bytes=200),
        _row("n8-derived", "D6", "remote-derived", "N8", origin_bytes=50),
        _row(
            "n8-cache-miss",
            "D7",
            "local-cache-derived",
            "N8",
            origin_bytes=50,
            cache=True,
        ),
        _row(
            "n8-cache-hit",
            "D7",
            "local-cache-derived",
            "N8",
            origin_bytes=0,
            cache_bytes=50,
            cache=True,
        ),
    ]


class FormalAccountingTest(unittest.TestCase):
    def test_converter_keeps_public_metrics_and_drops_model_text(self) -> None:
        rows = _public_rows(_ten_rows())

        self.assertEqual(10, len(rows))
        self.assertEqual(1000, rows[0]["origin_bytes_read"])
        self.assertEqual("miss", rows[3]["cache_branch"])
        self.assertEqual("hit", rows[4]["cache_branch"])
        self.assertEqual(50, rows[4]["cache_bytes_read"])
        serialized = json.dumps(rows, sort_keys=True)
        self.assertNotIn("must-not-be-copied", serialized)
        self.assertNotIn("prompt", serialized)
        self.assertNotIn("answer", serialized)

    def test_accounting_binds_runtime_build_and_route_bytes(self) -> None:
        rows = _public_rows(_ten_rows())
        unit = {
            "case_id": "object-1",
            "object_id": "object-1",
            "run_id": "formal-object-1-r0000",
            "evidence_receipt_sha256": "a" * 64,
        }
        runtime = {
            "object_id": "object-1",
            "credentials_recorded": False,
            "original_object_bytes_read": 1000,
            "frame_bundle_size_bytes": 200,
            "original_object_read_mode": (
                "complete-object-read-then-source-side-decode"
            ),
            "partial_mp4_byte_range_claimed": False,
            "reduced_source_storage_io_claimed": False,
            "manifest_sha256": "b" * 64,
        }

        accounting = _accounting_document(
            unit, rows, runtime, collection_id="formal-collection"
        )

        self.assertEqual(1000, accounting["one_time_build"][
            "this_object_source_bytes_read"
        ])
        self.assertEqual(200, accounting["one_time_build"][
            "this_object_projection_bytes"
        ])
        self.assertFalse(accounting["credentials_recorded"])
        self.assertFalse(accounting["hidden_label_values_included"])

    def test_converter_rejects_an_incomplete_route(self) -> None:
        rows = _ten_rows()
        rows[0]["result"]["status"] = "FAILED"

        with self.assertRaisesRegex(ValueError, "route is not complete"):
            _public_rows(rows)

    def test_accounting_rejects_runtime_projection_byte_drift(self) -> None:
        rows = _public_rows(_ten_rows())
        unit = {
            "case_id": "object-1",
            "object_id": "object-1",
            "run_id": "formal-object-1-r0000",
            "evidence_receipt_sha256": "a" * 64,
        }
        runtime = {
            "object_id": "object-1",
            "credentials_recorded": False,
            "original_object_bytes_read": 1000,
            "frame_bundle_size_bytes": 201,
            "original_object_read_mode": (
                "complete-object-read-then-source-side-decode"
            ),
            "partial_mp4_byte_range_claimed": False,
            "reduced_source_storage_io_claimed": False,
            "manifest_sha256": "b" * 64,
        }

        with self.assertRaisesRegex(ValueError, "indexed route bytes differ"):
            _accounting_document(
                unit, rows, runtime, collection_id="formal-collection"
            )


if __name__ == "__main__":
    unittest.main()
