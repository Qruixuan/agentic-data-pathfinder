"""Focused guards for measured provider-use reconstruction."""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from pathfinder.rsi_exam.materialization_cost_evidence import (
    _caption_request_rows,
    _embedding_request_rows,
)


def _raw(path: Path, *, request: str, response: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "window_id": "nextqa-val-1#win00",
        "window_descriptor_sha256": "w" * 64,
        "request_input_sha256": request,
        "response_sha256": response,
        "credentials_recorded": False,
        "raw_content": "do not copy this model output",
        "usage": {
            "prompt_tokens": 7,
            "completion_tokens": 3,
            "total_tokens": 10,
        },
    }), encoding="utf-8")


def _caption() -> dict[str, object]:
    return {
        "object_id": "nextqa-val-1",
        "ordinal": 0,
        "window_descriptor_sha256": "w" * 64,
        "request_input_sha256": "r" * 64,
        "response_sha256": "s" * 64,
    }


class MaterializationCostEvidenceTests(unittest.TestCase):
    def test_reconstructs_paid_retry_without_copying_content(self) -> None:
        with TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            directory = root / "raw" / "nextqa-val-1"
            _raw(directory / "00.attempt-01.json", request="r" * 64,
                 response="f" * 64)
            _raw(directory / "00.attempt-02.json", request="r" * 64,
                 response="s" * 64)
            rows = _caption_request_rows([_caption()], root)
        self.assertEqual(len(rows), 2)
        self.assertEqual(sum(row["total_units"] for row in rows), 20)
        self.assertEqual(
            [row["selected_caption_response"] for row in rows],
            [False, True],
        )
        self.assertNotIn("do not copy", json.dumps(rows))
        self.assertTrue(all("raw_content" not in row for row in rows))

    def test_excludes_old_request_for_same_window(self) -> None:
        with TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            directory = root / "raw" / "nextqa-val-1"
            _raw(directory / "00.attempt-01.json", request="x" * 64,
                 response="f" * 64)
            _raw(directory / "00.attempt-02.json", request="r" * 64,
                 response="s" * 64)
            rows = _caption_request_rows([_caption()], root)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["attempt"], 2)

    def test_rejects_missing_selected_response(self) -> None:
        with TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            _raw(root / "raw" / "nextqa-val-1" / "00.attempt-01.json",
                 request="r" * 64, response="other")
            with self.assertRaisesRegex(ValueError, "not unique"):
                _caption_request_rows([_caption()], root)

    def test_embedding_usage_requires_request_and_input_coverage(self) -> None:
        index = {
            "embedding_request_count": 1,
            "embedding_input_count": 2,
            "embedding_receipts": [{
                "input_count": 2,
                "request_sha256": "a" * 64,
                "response_sha256": "b" * 64,
                "service_time_seconds": 0.3,
                "usage": {"prompt_tokens": 17, "total_tokens": 17},
            }],
        }
        self.assertEqual(_embedding_request_rows(index)[0]["input_units"], 17)
        index["embedding_input_count"] = 3
        with self.assertRaisesRegex(ValueError, "input count differs"):
            _embedding_request_rows(index)
