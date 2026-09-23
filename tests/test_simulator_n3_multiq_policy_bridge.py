"""Public temporal query selections become source-bound N3 policies."""

from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pathfinder.rsi_exam.interleaved_multiq_plan import freeze_interleaved_plan
from pathfinder.simulator.n3_multiq_indexed_data_plane import (
    N3MultiQuestionPackageError,
    derive_n3_multiq_question_policies,
)
from pathfinder.simulator.raw_cold_data_plane import PACKAGE_MANIFEST_NAME
from tests.test_rsi_exam_interleaved_multiq_plan import _questions


class MultiQuestionPolicyBridgeTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.questions = _questions(2)
        self.source_sha = hashlib.sha256(b"public-source").hexdigest()
        self.plan = self.root / "plan"
        freeze_interleaved_plan(
            self.questions, seed="multiq-policy-test-seed",
            experiment_id="multiq-policy-test",
            public_source_sha256=self.source_sha, output_dir=self.plan,
        )
        self.raw = self.root / "raw"
        self.raw.mkdir()
        self.prep = self.root / "prep"
        self.prep.mkdir()
        self.query = self.root / "query"
        self.query.mkdir()
        objects = sorted({row["object_id"] for row in self.questions})
        raw_rows = []
        prep_rows = []
        for object_id in objects:
            raw_rows.append({
                "object_id": object_id,
                "artifact_sha256": "a" * 64,
                "artifact_size_bytes": 100,
            })
            prep_rows.append({
                "object_id": object_id,
                "source_video_sha256": "a" * 64,
                "source_video_size_bytes": 100,
                "duration_seconds": 10.0,
            })
        (self.raw / PACKAGE_MANIFEST_NAME).write_bytes(
            (json.dumps({"objects": raw_rows}, sort_keys=True, indent=2)
             + "\n").encode("utf-8")
        )
        (self.prep / "temporal-index-preparation.json").write_bytes(
            (json.dumps({"objects": prep_rows}, sort_keys=True, indent=2)
             + "\n").encode("utf-8")
        )
        self.selections = []
        for index, question in enumerate(self.questions):
            intervals = ([[2.0, 3.0], [4.0, 8.0]] if index == 0
                         else [[2.0, 8.0]])
            self.selections.append({
                "question_id": question["question_id"],
                "object_id": question["object_id"],
                "question_sha256": hashlib.sha256(
                    question["question"].encode("utf-8")
                ).hexdigest(),
                "selection": {
                    "action_id": "semantic-temporal-index-v2-subject-aware-topk",
                    "anchor_top_k": 2,
                    "anchor_window_ordinals": [1, 2],
                    "expansion_basis": "timestamp",
                    "fallback_used": False,
                    "max_selected_windows": 4,
                    "merged_intervals_seconds": intervals,
                    "relation": "following",
                    "selected_window_ordinals": [1, 2, 3],
                    "selected_span_seconds": [2.0, 8.0],
                },
            })
        self._write_selections()

    def _write_selections(self) -> None:
        (self.query / "temporal-query-selections.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in self.selections),
            encoding="utf-8",
        )

    def _derive(self) -> list[dict]:
        with patch(
            "pathfinder.simulator.n3_multiq_indexed_data_plane."
            "verify_temporal_query_batch",
            return_value={"package_sha256": "b" * 64},
        ), patch(
            "pathfinder.simulator.n3_multiq_indexed_data_plane."
            "verify_formal_temporal_index_preparation"
        ), patch(
            "pathfinder.simulator.n3_multiq_indexed_data_plane."
            "verify_raw_cold_data_plane_package"
        ):
            return derive_n3_multiq_question_policies(
                plan_dir=self.plan, public_questions=self.questions,
                public_source_sha256=self.source_sha,
                query_dir=self.query, video_index_dir=self.root / "video",
                preparation_dir=self.prep, caption_dir=self.root / "caption",
                raw_package_dir=self.raw,
            )

    def test_six_policies_bind_task_and_preserve_disjoint_provenance(self) -> None:
        policies = self._derive()
        self.assertEqual(6, len(policies))
        self.assertEqual(6, len({x["task_binding_sha256"] for x in policies}))
        first = next(x for x in policies
                     if x["question_id"] == self.questions[0]["question_id"])
        policy = first["selection_policy"]
        self.assertEqual([2.0, 3.0],
                         policy.selection_provenance[
                             "merged_intervals_seconds"][0])
        self.assertEqual(0.2, policy.temporal_start_fraction)
        self.assertEqual(0.8, policy.temporal_end_fraction)
        self.assertEqual("b" * 64, policy.selection_provenance[
            "temporal_index_package_sha256"])

    def test_question_digest_drift_is_rejected(self) -> None:
        self.selections[0]["question_sha256"] = "0" * 64
        self._write_selections()
        with self.assertRaisesRegex(
            N3MultiQuestionPackageError,
            "does not bind the N3 source and public question",
        ):
            self._derive()


if __name__ == "__main__":
    unittest.main()
