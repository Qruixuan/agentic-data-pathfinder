"""Focused checks for the ten-route branch of the shared batch runner."""

from __future__ import annotations

import copy
import json
from pathlib import Path
import unittest

from experiments import interleaved_batch as batch
from pathfinder.rsi_exam.ten_route_multiq_plan import (
    load_verified_multiq_plan,
    ten_route_trial_key,
)


ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "artifacts/ten-route-multiq-sealed-20260925-v1-plan-20260924t171621z"


class TenRouteMultiQuestionBatchTests(unittest.TestCase):
    def _config(self) -> dict:
        return {
            "schema_version": batch.TEN_MULTIQ_SCHEMA,
            "admission_dir": "new-admission",
            "baseline_spec_dir": None,
            "source_dirs": {key: "public-" + key for key in batch.SOURCE_KEYS},
            "coordinator_base_urls": {
                "N7": "http://10.70.0.17:19087",
                "N8": "http://10.70.0.18:19088",
            },
            "worker_alias": "pathfinder_costaware_20260815a",
            "worker_node_alias": "pathfinder-n7",
            "task_timeout_seconds": 900,
            "expected_plan_sha256": "a" * 64,
            "expected_question_count": 6,
            "expected_route_count": 60,
        }

    def test_config_requires_both_private_origins_and_exact_count(self):
        config = self._config()
        self.assertEqual(batch._validate_config(config), config)
        for node, bad in (("N7", "http://127.0.0.1:19087"),
                          ("N8", "http://10.70.0.17:19088")):
            changed = copy.deepcopy(config)
            changed["coordinator_base_urls"][node] = bad
            with self.subTest(node=node), self.assertRaises(ValueError):
                batch._validate_config(changed)
        changed = copy.deepcopy(config)
        changed["expected_route_count"] = 59
        with self.assertRaisesRegex(ValueError, "cardinality"):
            batch._validate_config(changed)

    def test_real_frozen_plan_orders_all_sixty_unique_trials(self):
        manifest, questions, report = load_verified_multiq_plan(PLAN)
        self.assertEqual(report["route_observation_count"], 60)
        self.assertEqual(len(questions), 6)
        schedule = [json.loads(line) for line in (
            PLAN / "ten-route-multiq-schedule.jsonl"
        ).read_bytes().splitlines()]
        trials, routes = [], {}
        for question in schedule:
            for slot in question["route_slots"]:
                key = ten_route_trial_key(
                    manifest["experiment_id"], question["question_id"],
                    slot["design_id"], slot["repetition"],
                )
                trials.append({
                    "trial_key": key,
                    "design_id": slot["design_id"],
                    "executor_node_id": slot["executor_node_id"],
                })
                routes[key] = {
                    "run_id": slot["run_id"],
                    "object_id": question["object_id"],
                    "cache_episode_id": slot["cache_episode_id"],
                }
        ordered = batch.schedule_trials(
            list(reversed(trials)), schedule, routes,
            experiment_id=manifest["experiment_id"],
        )
        self.assertEqual(len(ordered), 60)
        self.assertEqual(len({trial["trial_key"] for trial in ordered}), 60)
        self.assertEqual(
            [(row["design_id"], row["executor_node_id"])
             for row in ordered[:10]],
            [(row["design_id"], row["executor_node_id"])
             for row in schedule[0]["route_slots"]],
        )
        broken = copy.deepcopy(schedule)
        broken[0]["route_slots"][4]["run_id"] = "wrong"
        with self.assertRaisesRegex(ValueError, "slot identity"):
            batch.schedule_trials(
                trials, broken, routes,
                experiment_id=manifest["experiment_id"],
            )


if __name__ == "__main__":
    unittest.main()
