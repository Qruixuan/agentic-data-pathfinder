"""A screening failure must never discard already-paid LLM-bearing results."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from pathfinder.simulator.full_flow_visible_screening import (
    RECEIPT_NAME,
    RESULTS_NAME,
    freeze_visible_screening_plan,
    run_visible_screening,
    verify_checksums,
    verify_visible_screening_run,
)

CANONICAL = "multiple-choice-option-id-canonical-match-v1"


def _trial(workload: str, design: str, *, direct: bool) -> dict:
    return {
        "trial_key": f"scenario|{workload}|{design}|r0000",
        "workload_id": workload,
        "executor_node_id": "N7",
        "route_family": "raw" if direct else "remote-derived",
        "semantic_input_profile": {
            "profile_id": "raw-direct-video-v1" if direct else "derived-sparse-frames-4-v1",
            "direct_video_input": direct,
        },
    }


def _task(workload: str, obj: str) -> dict:
    return {
        "workload_id": workload,
        "object_id": obj,
        "task_class_id": "video_qa",
        "question": f"q {workload}",
        "answer_options": [{"option_id": "A", "text": "a"}],
        "success_scoring_rule": CANONICAL,
    }


def _result(success: bool, answer: str = "C") -> dict[str, Any]:
    return {
        "execution_transport": "flowmesh",
        "n1_score_authenticity_verified": True,
        "llm_called": True,
        "route_evidence_sha256": "0" * 64,
        "semantic_route_evidence": {
            "n1_score_request": {"predicted_answer": answer},
            "scoring": {"task_success": success},
            "model_input": {
                "semantic_input_profile_id": "raw-direct-video-v1",
                "mode": "direct-video",
                "direct_video_input": True,
                "payload_size_bytes": 123,
                "frame_count": 0,
            },
        },
    }


class ScreeningPersistenceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        workloads = ("smoke-alpha", "smoke-beta")
        trials = []
        for w in workloads:
            trials.append(_trial(w, "D0", direct=True))
            trials.append(_trial(w, "D2", direct=False))
        tasks = [_task(w, f"obj-{w}") for w in workloads]
        admission = {
            "admission_sha256": "a" * 64,
            "worker_pin": {"kind": "worker_alias", "value": "alias"},
        }
        task_path = self.root / "public-tasks.json"
        task_path.write_text(json.dumps({"tasks": tasks}), encoding="utf-8")
        self.inputs = mock.Mock(admission=admission, bound_trials=trials)
        self.patcher = mock.patch(
            "pathfinder.simulator.full_flow_visible_screening."
            "load_full_flow_local_semantic_execution_inputs",
            return_value=self.inputs,
        )
        self.patcher.start()
        self.addCleanup(self.patcher.stop)
        self.plan_dir = self.root / "plan"
        freeze_visible_screening_plan(
            local_semantic_admission_dir=self.root,
            public_task_set=task_path,
            screening_id="screening-test",
            seed="seed",
            git_commit="abc123",
            screening_actions=[
                {"action_id": "direct-video", "design_id": "D0", "repetition": "r0000"},
                {"action_id": "remote-derived", "design_id": "D2", "repetition": "r0000"},
            ],
            stratum_by_workload={"smoke-alpha": "a", "smoke-beta": "b"},
            selection_rule={
                "primary_action_id": "direct-video",
                "cheap_action_id": "remote-derived",
            },
            max_candidates=6,
            max_workflows=12,
            exclude_workload_ids=[],
            output_dir=self.plan_dir,
        )

    def test_failure_persists_every_completed_action(self) -> None:
        calls: list[str] = []

        def execute(candidate, action, key):
            calls.append(action["action_id"])
            # Fail only on the third action, after two have been paid for.
            if len(calls) == 3:
                raise RuntimeError("flowmesh-workflow-terminal-failure")
            return _result(success=True)

        out = self.root / "run"
        with self.assertRaisesRegex(RuntimeError, "terminal-failure"):
            run_visible_screening(
                self.plan_dir,
                self.root,
                run_id="run-1",
                execute_action=execute,
                output_dir=out,
            )

        # The two successful, already-paid results survive the failure.
        self.assertTrue(out.is_dir())
        verify_checksums(out)
        rows = [
            json.loads(line)
            for line in (out / RESULTS_NAME).read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        self.assertEqual(2, len(rows))
        receipt = json.loads((out / RECEIPT_NAME).read_text(encoding="utf-8"))
        self.assertEqual("INCOMPLETE_VISIBLE_SCREENING", receipt["status"])
        self.assertEqual(3, receipt["workflows_submitted"])
        self.assertIsNotNone(receipt["failure"])
        # The third action is the second candidate's first action, matching
        # the real failure shape this regression was written for.
        self.assertEqual("direct-video", receipt["failure"]["action_id"])
        self.assertEqual("smoke-beta", receipt["failure"]["workload_id"])

    def test_incomplete_receipt_still_verifies(self) -> None:
        def execute(candidate, action, key):
            if action["action_id"] == "remote-derived":
                raise RuntimeError("boom")
            return _result(success=True)

        out = self.root / "run2"
        with self.assertRaises(RuntimeError):
            run_visible_screening(
                self.plan_dir,
                self.root,
                run_id="run-2",
                execute_action=execute,
                output_dir=out,
            )
        report = verify_visible_screening_run(out, plan_dir=self.plan_dir)
        self.assertEqual("VERIFIED", report["status"])
        self.assertFalse(report["eligible_for_scientific_claims"])

    def test_completed_screening_stops_at_first_primary_hit(self) -> None:
        def execute(candidate, action, key):
            # Primary rule: direct video true, derived false.
            return _result(success=action["action_id"] == "direct-video")

        out = self.root / "run3"
        receipt = run_visible_screening(
            self.plan_dir,
            self.root,
            run_id="run-3",
            execute_action=execute,
            output_dir=out,
        )
        self.assertEqual("COMPLETED_VISIBLE_SCREENING", receipt["status"])
        self.assertEqual("primary", receipt["selection_rule_satisfied"])
        # Stopped after the first candidate: two workflows, not four.
        self.assertEqual(2, receipt["workflows_submitted"])
        self.assertEqual(1, receipt["candidates_screened"])
        verify_visible_screening_run(out, plan_dir=self.plan_dir)


if __name__ == "__main__":
    unittest.main()
