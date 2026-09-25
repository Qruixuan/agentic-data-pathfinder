"""Small public ten-observation multi-question plan regressions."""

import json
from pathlib import Path
import tempfile
import unittest

from pathfinder.distributed.scoring import (
    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
)
from pathfinder.rsi_exam.ten_route_multiq_plan import (
    TenRouteMultiQuestionPlanError, freeze_ten_route_multiq_plan,
    verify_ten_route_multiq_plan,
)
from pathfinder.simulator.hidden_oracle import build_n1_public_task_binding


def public_questions():
    options = [{"option_id": chr(65 + i), "text": f"choice {i}"}
               for i in range(5)]
    rows = []
    for video in ("video-a", "video-b", "video-c", "video-d"):
        for stratum in ("causal", "temporal", "descriptive"):
            question_id = f"{video}-{stratum}"
            question = f"What happened in {video} ({stratum})?"
            binding = build_n1_public_task_binding(
                workload_id=question_id, object_id=video,
                task_class_id=stratum, question=question,
                answer_options=options,
                success_scoring_rule=(
                    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE
                ),
            )
            rows.append({
                "question_id": question_id,
                "object_id": video,
                "stratum": stratum,
                "question": question,
                "answer_options": options,
                "public_task_sha256": binding["task_binding_sha256"],
            })
    return rows


class TenRouteMultiQuestionPlanTests(unittest.TestCase):
    def cohort(self):
        omitted = {"video-a": "causal", "video-b": "temporal",
                   "video-c": "descriptive"}
        return [row for row in public_questions()
                if row["object_id"] in omitted
                and row["stratum"] != omitted[row["object_id"]]]

    def test_freeze_verifies_ten_slots_and_state_isolation(self):
        selected = self.cohort()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "new-plan"
            report = freeze_ten_route_multiq_plan(
                selected, seed="seed-1", experiment_id="episode-1",
                public_source_sha256="a" * 64,
                exposure_inventory_sha256="b" * 64,
                output_dir=root,
            )
            self.assertEqual(report["route_observation_count"], 60)
            self.assertEqual(report, verify_ten_route_multiq_plan(
                root, selected, public_source_sha256="a" * 64,
                exposure_inventory_sha256="b" * 64,
            ))
            rows = [json.loads(line) for line in
                    (root / "ten-route-multiq-schedule.jsonl")
                    .read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(rows), 6)
            self.assertEqual(len({slot["run_id"] for row in rows
                                  for slot in row["route_slots"]}), 60)
            self.assertEqual(
                {row["object_id"] for row in rows[:3]},
                {"video-a", "video-b", "video-c"},
            )
            self.assertTrue(all(a["object_id"] != b["object_id"]
                                for a, b in zip(rows, rows[1:])))
            positions = {oid: [i for i, row in enumerate(rows)
                               if row["object_id"] == oid]
                         for oid in ("video-a", "video-b", "video-c")}
            self.assertGreater(len({p[1] - p[0] for p in positions.values()}),
                               1)
            for row in rows:
                self.assertEqual(len(row["route_slots"]), 10)
                for node in ("N7", "N8"):
                    cache = [slot for slot in row["route_slots"]
                             if slot["executor_node_id"] == node
                             and slot["arm_id"] == "DC"]
                    self.assertEqual([s["cache_expectation"] for s in cache],
                                     ["miss", "hit"])
                    self.assertEqual(cache[0]["cache_episode_id"],
                                     cache[1]["cache_episode_id"])
                    self.assertEqual(cache[0]["repetition"], 0)
                    self.assertEqual(cache[1]["repetition"], 1)
            with self.assertRaisesRegex(TenRouteMultiQuestionPlanError,
                                        "plan differs"):
                verify_ten_route_multiq_plan(
                    root, selected, public_source_sha256="a" * 64,
                    exposure_inventory_sha256="c" * 64,
                )

    def test_labels_and_outcomes_are_not_public_plan_inputs(self):
        candidates = self.cohort()
        candidates[0]["answer"] = "A"
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(ValueError, "fields differ"):
                freeze_ten_route_multiq_plan(
                    candidates, seed="seed-1", experiment_id="episode-1",
                    public_source_sha256="a" * 64,
                    exposure_inventory_sha256="b" * 64,
                    output_dir=Path(temp) / "rejected",
                )

    def test_eight_video_five_question_schedule_is_interleaved(self):
        options = [{"option_id": chr(65 + i), "text": f"choice {i}"}
                   for i in range(5)]
        selected = []
        for video_index in range(8):
            object_id = f"video-{video_index}"
            for question_index in range(5):
                stratum = "causal" if question_index == 0 else "temporal"
                question_id = f"{object_id}-q{question_index}"
                question = f"What happened in {object_id}?"
                binding = build_n1_public_task_binding(
                    workload_id=question_id, object_id=object_id,
                    task_class_id=stratum, question=question,
                    answer_options=options,
                    success_scoring_rule=(
                        MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE
                    ),
                )
                selected.append({
                    "question_id": question_id,
                    "object_id": object_id,
                    "stratum": stratum,
                    "question": question,
                    "answer_options": options,
                    "public_task_sha256": binding["task_binding_sha256"],
                })
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "new-plan"
            report = freeze_ten_route_multiq_plan(
                selected, seed="seed-8x5", experiment_id="episode-8x5",
                public_source_sha256="a" * 64,
                exposure_inventory_sha256="b" * 64,
                output_dir=root,
            )
            self.assertEqual(report["object_count"], 8)
            self.assertEqual(report["question_count"], 40)
            self.assertEqual(report["route_observation_count"], 400)
            self.assertEqual(report, verify_ten_route_multiq_plan(
                root, selected, public_source_sha256="a" * 64,
                exposure_inventory_sha256="b" * 64,
            ))
            schedule = [json.loads(line) for line in (
                root / "ten-route-multiq-schedule.jsonl"
            ).read_bytes().splitlines()]
            self.assertEqual(len(schedule), 40)
            for round_index in range(5):
                block = schedule[round_index * 8:(round_index + 1) * 8]
                self.assertEqual({row["object_id"] for row in block},
                                 {f"video-{i}" for i in range(8)})
                self.assertTrue(all(row["round"] == round_index
                                    for row in block))
            self.assertTrue(all(a["object_id"] != b["object_id"]
                                for a, b in zip(schedule, schedule[1:])))
            self.assertEqual(len({slot["run_id"] for row in schedule
                                  for slot in row["route_slots"]}), 400)

    def test_light_derived_supplement_has_only_six_frame_only_slots(self):
        selected = self.cohort()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "light-plan"
            report = freeze_ten_route_multiq_plan(
                selected, seed="seed-light", experiment_id="light-episode",
                public_source_sha256="a" * 64,
                exposure_inventory_sha256="b" * 64,
                output_dir=root, derived_profile="frame-only",
            )
            self.assertEqual(report["route_observation_count"], 36)
            manifest = json.loads((root / "ten-route-multiq-plan.json").read_bytes())
            self.assertEqual(manifest["derived_representation_ids"],
                             ["sampled_frame_bundle"])
            schedule = [json.loads(line) for line in (
                root / "ten-route-multiq-schedule.jsonl"
            ).read_bytes().splitlines()]
            for row in schedule:
                self.assertEqual(len(row["route_slots"]), 6)
                self.assertEqual(
                    {slot["design_id"] for slot in row["route_slots"]},
                    {"D2", "D3", "D6", "D7"},
                )
                for node in ("N7", "N8"):
                    cache = [slot for slot in row["route_slots"]
                             if slot["executor_node_id"] == node
                             and slot["arm_id"] == "DC"]
                    self.assertEqual([slot["cache_expectation"]
                                      for slot in cache], ["miss", "hit"])
            self.assertEqual(report, verify_ten_route_multiq_plan(
                root, selected, public_source_sha256="a" * 64,
                exposure_inventory_sha256="b" * 64,
            ))

    def test_single_summary_fusion_preserves_six_slots_and_two_artifacts(self):
        selected = self.cohort()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "summary-plan"
            report = freeze_ten_route_multiq_plan(
                selected, seed="seed-summary", experiment_id="summary-episode",
                public_source_sha256="a" * 64,
                exposure_inventory_sha256="b" * 64,
                output_dir=root, derived_profile="single-summary-fusion",
            )
            self.assertEqual(report["route_observation_count"], 36)
            manifest = json.loads((root / "ten-route-multiq-plan.json").read_bytes())
            self.assertEqual(manifest["derived_representation_ids"],
                             ["multimodal_digest", "sampled_frame_bundle"])
            self.assertEqual(manifest["profile"],
                             "light-derived-fusion-supplement")
            self.assertEqual(report, verify_ten_route_multiq_plan(
                root, selected, public_source_sha256="a" * 64,
                exposure_inventory_sha256="b" * 64,
            ))


if __name__ == "__main__":
    unittest.main()
