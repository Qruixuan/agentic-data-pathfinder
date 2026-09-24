"""Focused checks for the ten-route branch of the shared batch runner."""

from __future__ import annotations

import copy
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from experiments import interleaved_batch as batch
from pathfinder.rsi_exam.ten_route_multiq_plan import (
    freeze_ten_route_multiq_plan, load_verified_multiq_plan,
    ten_route_trial_key,
)
from tests.test_rsi_exam_ten_route_multiq_plan import public_questions


class TenRouteMultiQuestionBatchTests(unittest.TestCase):
    @staticmethod
    def _plan(root: Path) -> Path:
        omitted = {"video-a": "causal", "video-b": "temporal",
                   "video-c": "descriptive"}
        selected = [row for row in public_questions()
                    if row["object_id"] in omitted
                    and row["stratum"] != omitted[row["object_id"]]]
        plan = root / "plan"
        freeze_ten_route_multiq_plan(
            selected, seed="batch-ten-route-test-seed",
            experiment_id="batch-ten-route-test",
            public_source_sha256="a" * 64,
            exposure_inventory_sha256="b" * 64,
            output_dir=plan,
        )
        return plan

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

        larger = copy.deepcopy(config)
        larger["expected_question_count"] = 40
        larger["expected_route_count"] = 400
        self.assertEqual(batch._validate_config(larger), larger)
        larger["expected_route_count"] = 399
        with self.assertRaisesRegex(ValueError, "cardinality"):
            batch._validate_config(larger)

    def test_frozen_plan_orders_all_sixty_unique_trials(self):
        with tempfile.TemporaryDirectory() as directory:
            plan = self._plan(Path(directory))
            manifest, questions, report = load_verified_multiq_plan(plan)
            self.assertEqual(report["route_observation_count"], 60)
            self.assertEqual(len(questions), 6)
            schedule = [json.loads(line) for line in (
                plan / "ten-route-multiq-schedule.jsonl"
            ).read_bytes().splitlines()]
            trials, routes = [], {}
            for question in schedule:
                for slot in question["route_slots"]:
                    key = ten_route_trial_key(
                        manifest["experiment_id"],
                        question["question_id"], slot["design_id"],
                        slot["repetition"],
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

    def test_load_inputs_binds_both_nodes_and_all_cache_pairs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = self._config()
            plan_path = self._plan(root)
            shutil.copytree(
                plan_path, root / config["source_dirs"]["plan_dir"],
            )
            manifest, _, _ = load_verified_multiq_plan(plan_path)
            config["expected_plan_sha256"] = manifest["plan_sha256"]
            schedule = [json.loads(line) for line in (
                plan_path / "ten-route-multiq-schedule.jsonl"
            ).read_bytes().splitlines()]
            routes, trials, episodes = [], [], []
            for question in schedule:
                for slot in question["route_slots"]:
                    key = ten_route_trial_key(
                        manifest["experiment_id"],
                        question["question_id"], slot["design_id"],
                        slot["repetition"],
                    )
                    node = slot["executor_node_id"]
                    routes.append({
                        "trial_key": key,
                        "run_id": slot["run_id"],
                        "object_id": question["object_id"],
                        "cache_episode_id": slot["cache_episode_id"],
                    })
                    trials.append({
                        "trial_key": key,
                        "workload_id": question["question_id"],
                        "design_id": slot["design_id"],
                        "repetition": slot["repetition"],
                        "executor_node_id": node,
                        "route_family": (
                            "local-cache-derived"
                            if slot["arm_id"] == "DC" else "raw"
                        ),
                        "worker_alias": config["worker_alias"],
                        "order_index": len(trials),
                        "route_coordinator_binding": {
                            "base_url": config["coordinator_base_urls"][node],
                        },
                    })
                    if slot["cache_episode_id"] is not None:
                        episodes.append({
                            "trial_key": key, "run_id": slot["run_id"],
                            "cache_episode_id": slot["cache_episode_id"],
                        })
            binding = root / config["source_dirs"]["binding_dir"]
            admission = root / config["admission_dir"]
            binding.mkdir()
            admission.mkdir()

            def write_rows(path: Path, rows: list[dict]) -> None:
                path.write_bytes(b"".join(
                    batch._canonical(row) + b"\n" for row in rows
                ))

            write_rows(binding / "route-inputs.jsonl", routes)
            write_rows(admission / "admitted-trials.jsonl", trials)
            write_rows(admission / "admitted-stages.jsonl", [{}])
            write_rows(admission / "cache-episode-bindings.jsonl", episodes)
            report = {
                "status": "VERIFIED_TEN_ROUTE_MULTIQ_ADMISSION_NOT_DEPLOYED",
                "trial_count": 60, "stage_count": 1,
                "index_query_plan_count": 12,
                "data_agent_plan_binding_count": 96,
                "cache_episode_binding_count": 24,
                "admission_sha256": "c" * 64,
            }
            with patch.object(batch, "verify_interleaved_runtime_admission",
                              return_value=report):
                loaded = batch.load_inputs(config, root)
            self.assertEqual(len(loaded["trials"]), 60)
            self.assertEqual(len(loaded["episodes"]), 24)
            self.assertEqual({row["executor_node_id"]
                              for row in loaded["trials"]}, {"N7", "N8"})
            bad = copy.deepcopy(config)
            bad["coordinator_base_urls"]["N8"] = (
                "http://10.70.0.18:19089"
            )
            with patch.object(batch, "verify_interleaved_runtime_admission",
                              return_value=report):
                with self.assertRaisesRegex(ValueError, "origin binding"):
                    batch.load_inputs(bad, root)


if __name__ == "__main__":
    unittest.main()
