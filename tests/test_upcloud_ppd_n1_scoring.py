"""Offline checks for the separate, authenticated PPD score step."""

from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from experiments.upcloud_ppd_20260925.score_engineering_session import (
    _public_task,
    prepare_score,
    score_once,
)
from pathfinder.distributed.scoring import (
    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
)
from pathfinder.simulator.hidden_oracle import build_n1_public_task_binding


class N1ScoringPreparationTests(unittest.TestCase):
    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        self.receipt = root / "receipt.json"
        self.task_path = root / "task.json"
        self.db = root / "gateway.sqlite3"
        self.task = build_n1_public_task_binding(
            workload_id="public-ppd-q5",
            object_id="nextqa-val-11584566583",
            task_class_id="video_qa",
            question="How many people are speaking on the microphone?",
            answer_options=[
                {"option_id": option, "text": text}
                for option, text in zip(
                    "ABCDE", ("one", "five", "nine", "six", "three")
                )
            ],
            success_scoring_rule=MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
        )
        self.task_path.write_text(json.dumps(self.task), encoding="utf-8")
        self.source_receipt = {
            "status": "DONE", "closure_verified": True,
            "task_success_evaluated": False,
            "answer_has_explicit_final_option": True,
            "eligible_for_scientific_claims": False,
            "session_id": "public-ppd-run-1", "workflow_id": "wfl-1",
            "task_id": "tsk-1", "design_id": "PPD_REMOTE_DIGEST",
            "answer_format": "bare-option", "extracted_option_id": "A",
        }
        self._save_receipt()
        prompt = (
            "Answer this video question by choosing one option ID only.\n\n"
            "How many people are speaking on the microphone?\n"
            "A. one\nB. five\nC. nine\nD. six\nE. three\n"
        )
        connection = sqlite3.connect(self.db)
        try:
            connection.executescript(
                "CREATE TABLE gateway_sessions (session_id TEXT, trial_id TEXT, "
                "question TEXT, design_id TEXT, task_class_id TEXT, status TEXT, "
                "flowmesh_workflow_id TEXT, flowmesh_task_id TEXT, "
                "final_answer TEXT, object_id TEXT);"
                "CREATE TABLE gateway_access_events "
                "(session_id TEXT, accepted INTEGER);"
            )
            connection.execute(
                "INSERT INTO gateway_sessions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("public-ppd-run-1", "public-ppd-trial-1", prompt,
                 "PPD_REMOTE_DIGEST", "video_qa", "DONE", "wfl-1", "tsk-1",
                 "A", "nextqa-val-11584566583"),
            )
            connection.execute(
                "INSERT INTO gateway_access_events VALUES (?, 1)",
                ("public-ppd-run-1",),
            )
            connection.commit()
        finally:
            connection.close()

    def _save_receipt(self) -> None:
        self.receipt.write_text(json.dumps(self.source_receipt), encoding="utf-8")

    def _prepare(self):
        return prepare_score(
            receipt_path=self.receipt,
            state_db=self.db,
            public_task_path=self.task_path,
            oracle_id="public-ppd-oracle-1",
            public_task_set_sha256="1" * 64,
        )

    def test_prepares_one_bound_request_without_a_model_call(self) -> None:
        prepared = self._prepare()
        self.assertEqual(prepared.request["predicted_answer"], "A")
        self.assertEqual(
            prepared.request["task_binding_sha256"], self.task["task_binding_sha256"]
        )
        self.assertEqual(prepared.request["run_id"], "public-ppd-run-1")
        self.assertEqual(prepared.request["trial_id"], "public-ppd-trial-1")

    def test_rejects_receipt_or_gateway_tampering(self) -> None:
        self.source_receipt["extracted_option_id"] = "B"
        self._save_receipt()
        with self.assertRaisesRegex(ValueError, "receipt option differs"):
            self._prepare()
        self.source_receipt["extracted_option_id"] = "A"
        self.source_receipt["closure_verified"] = False
        self._save_receipt()
        with self.assertRaisesRegex(ValueError, "unscored completed closure"):
            self._prepare()

    def test_rejects_wrong_public_task(self) -> None:
        task = dict(self.task)
        task["question"] = "A different question?"
        self.task_path.write_text(json.dumps(task), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "not canonical"):
            self._prepare()

    def test_repository_public_task_is_canonical(self) -> None:
        path = (
            Path(__file__).resolve().parents[1]
            / "experiments/upcloud_ppd_20260925/public-engineering-q5-task.json"
        )
        task = _public_task(path)
        self.assertEqual(task["object_id"], "nextqa-val-11584566583")
        self.assertEqual(
            task["task_binding_sha256"],
            "63460126d2df5a8661eca1d175d4978d1caa5932f379f7c56439510bd53dd85d",
        )

    def test_ambiguous_gateway_answer_cannot_be_scored(self) -> None:
        connection = sqlite3.connect(self.db)
        try:
            connection.execute(
                "UPDATE gateway_sessions SET final_answer = ?",
                ("A or B",),
            )
            connection.commit()
        finally:
            connection.close()
        with self.assertRaisesRegex(ValueError, "receipt option differs"):
            self._prepare()

    def test_public_score_receipt_excludes_prediction_and_hidden_label(self) -> None:
        prepared = self._prepare()

        class FakeScorer:
            def score_once_and_verify(self, request):
                self_request = request
                self_id = self_request["score_request_id"]
                return SimpleNamespace(
                    authentication_verified=True,
                    verification_sha256="2" * 64,
                    result={
                        "score_request_id": self_id,
                        "public_task_set_sha256": "1" * 64,
                        "hidden_answer_returned": False,
                        "result_content_sha256": "3" * 64,
                        "score_evidence_hmac_sha256": "4" * 64,
                        "success_scoring_rule": (
                            MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE
                        ),
                        "correct": True,
                        "score": 1.0,
                    },
                )

        output = score_once(prepared, FakeScorer())
        self.assertTrue(output["task_success"])
        self.assertTrue(output["task_success_evaluated"])
        self.assertNotIn("predicted_answer", output)
        self.assertNotIn("correct_answer_id", output)
        self.assertFalse(output["eligible_for_scientific_claims"])


if __name__ == "__main__":
    unittest.main()
