"""No-network proof that only source-admitted candidate runs can execute."""

import json
from pathlib import Path
import tempfile
import unittest

from pathfinder.integrations.flowmesh.route_action_candidate_schedule import (
    freeze_route_action_candidates,
)
from pathfinder.integrations.flowmesh.route_action_runtime_admission import (
    RouteActionRuntimeAdmissionError,
    freeze_route_action_runtime_admission,
    verify_route_action_runtime_admission,
)


ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "artifacts/nextqa-atphard-8x5-prep-input-20260925-v1/plan"
QUOTE = ROOT / "artifacts/route-action-quotes-t60-dev-20260927-v2-draft"


@unittest.skipUnless(PLAN.is_dir() and QUOTE.is_dir(),
                     "public development fixtures are absent")
class RouteActionRuntimeAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.candidates = self.root / "candidates"
        self.admission = self.root / "admission"
        question = json.loads((
            PLAN / "ten-route-multiq-schedule.jsonl"
        ).read_bytes().splitlines()[0])["question_id"]
        freeze_route_action_candidates(
            output_dir=self.candidates, plan_dir=PLAN, quote_dir=QUOTE,
            question_ids=[question], execution_namespace="runtime-unit",
            order_seed="precommitted-order",
        )
        rows = [json.loads(line) for line in (
            self.candidates / "route-action-candidates.jsonl"
        ).read_bytes().splitlines()]
        families = {
            "R": "raw", "I": "indexed-raw", "D": "remote-derived",
            "DC": "local-cache-derived",
        }
        self.trials = list({row["trial_key"]: {
            "trial_key": row["trial_key"],
            "flowmesh_submission_authorized": True,
            "required_runtime_adapter_ids": [],
            "design_id": row["action_id"],
            "executor_node_id": row["executor_node_id"],
            "route_family": families[row["arm_id"]],
            "artifact_object_id": row["object_id"],
            "public_task_binding_sha256": row["public_task_sha256"],
            "repetition": 1 if row["cache_state"] == "hit" else 0,
        } for row in rows}.values())
        self.source_sha = "a" * 64

    def freeze(self):
        return freeze_route_action_runtime_admission(
            output_dir=self.admission, candidate_dir=self.candidates,
            plan_dir=PLAN, quote_dir=QUOTE,
            admitted_trials=self.trials,
            admission_sha256=self.source_sha,
        )

    def test_frozen_runs_bind_both_nodes_and_cache_episodes(self):
        report = self.freeze()
        self.assertEqual("VERIFIED_ROUTE_ACTION_RUNTIME_ADMISSION",
                         report["status"])
        self.assertEqual(28, report["run_count"])
        self.assertEqual(14, len(report["bindings"]["N7"]))
        self.assertEqual(14, len(report["bindings"]["N8"]))
        self.assertEqual(8, sum(value is not None for node in
                                report["bindings"].values()
                                for value in node.values()))

    def test_changed_admission_or_trial_rejected(self):
        self.freeze()
        with self.assertRaisesRegex(RouteActionRuntimeAdmissionError,
                                    "differs"):
            verify_route_action_runtime_admission(
                self.admission, candidate_dir=self.candidates,
                plan_dir=PLAN, quote_dir=QUOTE,
                admitted_trials=self.trials,
                admission_sha256="b" * 64,
            )
        altered = [dict(row) for row in self.trials]
        altered[0]["artifact_object_id"] = "wrong-object"
        with self.assertRaises(Exception):
            verify_route_action_runtime_admission(
                self.admission, candidate_dir=self.candidates,
                plan_dir=PLAN, quote_dir=QUOTE,
                admitted_trials=altered,
                admission_sha256=self.source_sha,
            )

    def test_tampering_and_output_reuse_rejected(self):
        self.freeze()
        with self.assertRaisesRegex(RouteActionRuntimeAdmissionError,
                                    "already exists"):
            self.freeze()
        rows = self.admission / "route-action-runtime-runs.jsonl"
        rows.write_bytes(rows.read_bytes() + b"{}\n")
        with self.assertRaisesRegex(RouteActionRuntimeAdmissionError,
                                    "differs"):
            verify_route_action_runtime_admission(
                self.admission, candidate_dir=self.candidates,
                plan_dir=PLAN, quote_dir=QUOTE,
                admitted_trials=self.trials,
                admission_sha256=self.source_sha,
            )
