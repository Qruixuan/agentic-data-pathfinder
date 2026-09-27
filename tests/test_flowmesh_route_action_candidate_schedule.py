"""No-provider checks for precommitted PPD session/action identities."""

import json
from pathlib import Path
import tempfile
import unittest

from pathfinder.integrations.flowmesh.route_action_candidate_schedule import (
    RouteActionCandidateScheduleError,
    candidate_run_bindings,
    freeze_route_action_candidates,
    verify_route_action_candidates,
)


ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "artifacts/nextqa-atphard-8x5-prep-input-20260925-v1/plan"
QUOTE = ROOT / "artifacts/route-action-quotes-t60-dev-20260927-v2-draft"


@unittest.skipUnless(PLAN.is_dir() and QUOTE.is_dir(),
                     "public development fixtures are absent")
class RouteActionCandidateScheduleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.output = Path(self.temp.name) / "candidates"
        schedule = [json.loads(line) for line in (
            PLAN / "ten-route-multiq-schedule.jsonl"
        ).read_bytes().splitlines()]
        first = schedule[0]["object_id"]
        self.questions = [row["question_id"] for row in schedule
                          if row["object_id"] == first][:2]
        self.old_run_ids = {
            slot["run_id"] for row in schedule
            for slot in row["route_slots"]
        }

    def freeze(self):
        return freeze_route_action_candidates(
            output_dir=self.output, plan_dir=PLAN, quote_dir=QUOTE,
            question_ids=self.questions,
            execution_namespace="fresh-unit-two-questions",
            order_seed="outcome-blind-seed",
        )

    def rows(self):
        return [json.loads(line) for line in (
            self.output / "route-action-candidates.jsonl"
        ).read_bytes().splitlines()]

    def test_all_potential_actions_are_bound_before_choice(self):
        report = self.freeze()
        self.assertEqual("VERIFIED_CANDIDATES_NOT_RUNTIME_ADMISSION",
                         report["status"])
        self.assertEqual(2, report["question_count"])
        self.assertEqual(8, report["session_count"])
        self.assertEqual(56, report["candidate_count"])
        rows = self.rows()
        self.assertFalse({row["run_id"] for row in rows} & self.old_run_ids)
        self.assertEqual({"miss", "hit"}, {
            row["cache_state"] for row in rows
            if row["action_id"] == "D3"
        })
        for session_id in {row["session_id"] for row in rows
                           if row["action_id"] == "D3"}:
            self.assertEqual(1, len({
                row["run_id"] for row in rows
                if row["session_id"] == session_id
                and row["action_id"] == "D3"
            }))
        self.assertEqual(1, len({
            row["cache_episode_id"] for row in rows
            if row["action_id"] == "D3"
        }))
        self.assertEqual(1, len({
            row["cache_episode_id"] for row in rows
            if row["action_id"] == "D7"
        }))
        self.assertNotEqual(
            next(row["cache_episode_id"] for row in rows
                 if row["action_id"] == "D3"),
            next(row["cache_episode_id"] for row in rows
                 if row["action_id"] == "D7"),
        )
        self.assertFalse(any(row["cache_episode_id"] is not None
                             for row in rows if row["arm_id"] != "DC"))
        self.assertFalse(any("answer" in row or "task_success" in row
                             for row in rows))

    def test_design_order_is_frozen_and_counterbalanced(self):
        self.freeze()
        rows = self.rows()
        for question in self.questions:
            design_order = {
                row["physical_design_id"]: row["design_order"]
                for row in rows if row["question_id"] == question
            }
            self.assertEqual(set(range(4)), set(design_order.values()))
        self.assertEqual(
            verify_route_action_candidates(
                self.output, plan_dir=PLAN, quote_dir=QUOTE,
            )["candidate_count"], 56,
        )

    def test_tampering_or_reusing_output_fails_closed(self):
        self.freeze()
        with self.assertRaisesRegex(RouteActionCandidateScheduleError,
                                    "already exists"):
            self.freeze()
        rows = self.output / "route-action-candidates.jsonl"
        rows.write_bytes(rows.read_bytes() + b"{}\n")
        with self.assertRaisesRegex(RouteActionCandidateScheduleError,
                                    "differs"):
            verify_route_action_candidates(
                self.output, plan_dir=PLAN, quote_dir=QUOTE,
            )

    def test_unknown_public_question_is_rejected(self):
        with self.assertRaisesRegex(RouteActionCandidateScheduleError,
                                    "absent"):
            freeze_route_action_candidates(
                output_dir=self.output, plan_dir=PLAN, quote_dir=QUOTE,
                question_ids=["not-a-public-question"],
                execution_namespace="fresh-unit", order_seed="seed",
            )

    def test_frozen_sources_cannot_be_output_targets(self):
        with self.assertRaisesRegex(RouteActionCandidateScheduleError,
                                    "frozen source"):
            freeze_route_action_candidates(
                output_dir=PLAN / "never-created-candidate-package",
                plan_dir=PLAN, quote_dir=QUOTE,
                question_ids=self.questions,
                execution_namespace="fresh-unit", order_seed="seed",
            )

    def test_historical_forty_question_fixture_scales_without_submission(self):
        question_ids = [json.loads(line)["question_id"] for line in (
            PLAN / "ten-route-multiq-schedule.jsonl"
        ).read_bytes().splitlines()]
        report = freeze_route_action_candidates(
            output_dir=self.output, plan_dir=PLAN, quote_dir=QUOTE,
            question_ids=question_ids,
            execution_namespace="fixture-only-not-for-submission",
            order_seed="outcome-blind-seed",
        )
        self.assertEqual(40, report["question_count"])
        self.assertEqual(160, report["session_count"])
        self.assertEqual(1120, report["candidate_count"])
        self.assertIs(report["workflow_submitted"], False)

    def test_candidate_runs_cross_check_source_bound_trial_fields(self):
        self.freeze()
        families = {
            "R": "raw", "I": "indexed-raw", "D": "remote-derived",
            "DC": "local-cache-derived",
        }
        trials = {}
        for row in self.rows():
            trials[row["trial_key"]] = {
                "trial_key": row["trial_key"],
                "flowmesh_submission_authorized": True,
                "required_runtime_adapter_ids": [],
                "design_id": row["action_id"],
                "executor_node_id": row["executor_node_id"],
                "route_family": families[row["arm_id"]],
                "artifact_object_id": row["object_id"],
                "public_task_binding_sha256": row["public_task_sha256"],
                "repetition": 1 if row["cache_state"] == "hit" else 0,
            }
        admitted = list(trials.values())
        bindings = candidate_run_bindings(
            self.output, plan_dir=PLAN, quote_dir=QUOTE,
            admitted_trials=admitted,
        )
        self.assertEqual(56, sum(map(len, bindings.values())))
        self.assertEqual({"N7", "N8"}, set(bindings))
        self.assertEqual(4, len({
            (run_id, trial_key) for node in bindings.values()
            for (run_id, trial_key), episode in node.items()
            if episode is not None and trial_key.endswith("|D3|r0000")
        }))
        admitted[0]["artifact_object_id"] = "wrong-object"
        with self.assertRaisesRegex(
            RouteActionCandidateScheduleError,
            "differs from its verified admitted trial",
        ):
            candidate_run_bindings(
                self.output, plan_dir=PLAN, quote_dir=QUOTE,
                admitted_trials=admitted,
            )


if __name__ == "__main__":
    unittest.main()
