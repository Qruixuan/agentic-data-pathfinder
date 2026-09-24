from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from pathfinder.distributed.scoring import (
    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
)
from pathfinder.rsi_exam.interleaved_multiq_plan import (
    ARM_CONTRACTS,
    InterleavedPlanError,
    freeze_interleaved_plan,
    interleaved_cache_episode_bindings,
    select_outcome_blind_cohort,
    verify_interleaved_plan,
)
from pathfinder.simulator.hidden_oracle import build_n1_public_task_binding


SOURCE_SHA256 = hashlib.sha256(b"public-task-source").hexdigest()


def _questions(video_count: int = 4) -> list[dict[str, object]]:
    rows = []
    for video in range(video_count):
        for stratum in ("causal", "temporal", "descriptive"):
            question_id = f"video-{video}-{stratum}"
            object_id = f"video-{video}"
            question = f"What happened in {question_id}?"
            options = [
                {"option_id": chr(ord("A") + index),
                 "text": f"option {index}"}
                for index in range(5)
            ]
            task = build_n1_public_task_binding(
                workload_id=question_id,
                object_id=object_id,
                task_class_id=stratum,
                question=question,
                answer_options=options,
                success_scoring_rule=(
                    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE
                ),
            )
            rows.append({
                "question_id": question_id,
                "object_id": object_id,
                "stratum": stratum,
                "question": question,
                "answer_options": options,
                "public_task_sha256": task["task_binding_sha256"],
            })
    return rows


class InterleavedMultiQuestionPlanTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def test_six_cache_trials_share_only_the_frozen_episode(self) -> None:
        questions = _questions(2)
        output = self.root / "plan"
        freeze_interleaved_plan(
            questions, seed="multiq-cache-seed-v1",
            experiment_id="multiq-cache-unique-v1",
            public_source_sha256=SOURCE_SHA256, output_dir=output,
        )
        bindings = interleaved_cache_episode_bindings(
            output, questions, public_source_sha256=SOURCE_SHA256,
        )
        self.assertEqual(6, len(bindings))
        self.assertEqual(
            {"multiq-cache-unique-v1-dc"}, set(bindings.values())
        )
        self.assertTrue(all(key[1].endswith("|DC") for key in bindings))
        self.assertEqual(6, len({key[0] for key in bindings}))

    def test_seventh_public_question_yields_28_interleaved_routes(self) -> None:
        import json

        source = _questions(2)
        extra = dict(source[0])
        extra["question_id"] = "video-0-extra-temporal"
        extra["stratum"] = "temporal"
        extra["question"] = "What happened after the second event?"
        extra["public_task_sha256"] = build_n1_public_task_binding(
            workload_id=extra["question_id"],
            object_id=extra["object_id"],
            task_class_id=extra["stratum"],
            question=extra["question"],
            answer_options=extra["answer_options"],
            success_scoring_rule=MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
        )["task_binding_sha256"]
        source.append(extra)
        output = self.root / "seven"
        report = freeze_interleaved_plan(
            source, seed="seven-seed", experiment_id="seven-test",
            public_source_sha256=SOURCE_SHA256, output_dir=output,
        )
        self.assertEqual(7, report["question_count"])
        self.assertEqual(28, report["route_count"])
        rows = [json.loads(line) for line in (
            output / "interleaved-schedule.jsonl"
        ).read_text(encoding="utf-8").splitlines()]
        self.assertEqual(list(range(7)), [row["ordinal"] for row in rows])
        self.assertTrue(all(a["object_id"] != b["object_id"]
                            for a, b in zip(rows, rows[1:])))
        self.assertEqual(28, len({slot["run_id"] for row in rows
                                  for slot in row["route_slots"]}))
        self.assertEqual(7, len(interleaved_cache_episode_bindings(
            output, source, public_source_sha256=SOURCE_SHA256,
        )))
        self.assertEqual(report, verify_interleaved_plan(
            output, list(reversed(source)),
            public_source_sha256=SOURCE_SHA256,
        ))

    def test_four_video_schedule_is_reproducible_and_interleaved(self) -> None:
        import json

        source = _questions()
        output = self.root / "plan"
        report = freeze_interleaved_plan(
            source, seed="multiq-pilot-seed-v1",
            experiment_id="multiq-pilot-unique-v1",
            public_source_sha256=SOURCE_SHA256, output_dir=output
        )
        self.assertEqual(4, report["object_count"])
        self.assertEqual(12, report["question_count"])
        self.assertEqual(48, report["route_count"])
        self.assertEqual(
            {"R": "raw", "D": "remote-derived",
             "DC": "local-cache-derived", "I": "indexed-derived"},
            {arm: contract["route_family"]
             for arm, contract in ARM_CONTRACTS.items()},
        )
        self.assertEqual(
            report,
            verify_interleaved_plan(
                output, list(reversed(source)),
                public_source_sha256=SOURCE_SHA256,
            ),
        )
        rows = [json.loads(line) for line in
                (output / "interleaved-schedule.jsonl").read_text(
                    encoding="utf-8"
                ).splitlines()]
        self.assertEqual(12, len(rows))
        self.assertTrue(all(a["object_id"] != b["object_id"]
                            for a, b in zip(rows, rows[1:])))
        self.assertTrue(all(len({row["object_id"] for row in rows[i:i + 4]})
                            == 4 for i in (0, 4, 8)))
        self.assertTrue(all(set(row["arm_order"]) == {"R", "D", "DC", "I"}
                            for row in rows))
        self.assertEqual(48, len({slot["run_id"] for row in rows
                                  for slot in row["route_slots"]}))
        self.assertEqual(1, len({slot["cache_episode_id"] for row in rows
                                 for slot in row["route_slots"]
                                 if slot["arm_id"] == "DC"}))
        self.assertTrue(all(slot["cache_episode_id"] is None
                            for row in rows for slot in row["route_slots"]
                            if slot["arm_id"] != "DC"))

    def test_two_video_gate_and_source_binding(self) -> None:
        source = _questions(2)
        output = self.root / "gate"
        report = freeze_interleaved_plan(
            source, seed="multiq-gate-seed-v1",
            experiment_id="multiq-gate-unique-v1",
            public_source_sha256=SOURCE_SHA256, output_dir=output
        )
        self.assertEqual(24, report["route_count"])
        changed = [dict(row) for row in source]
        changed[0]["question"] = "Changed public question"
        with self.assertRaisesRegex(
            InterleavedPlanError, "differs from its canonical N1 task"
        ):
            verify_interleaved_plan(
                output, changed, public_source_sha256=SOURCE_SHA256
            )
        with self.assertRaisesRegex(
            InterleavedPlanError, "differs from public tasks"
        ):
            verify_interleaved_plan(
                output, source,
                public_source_sha256=hashlib.sha256(b"changed").hexdigest(),
            )
        with self.assertRaisesRegex(
            InterleavedPlanError, "output already exists"
        ):
            freeze_interleaved_plan(
                source, seed="multiq-gate-seed-v1",
                experiment_id="multiq-gate-unique-v1",
                public_source_sha256=SOURCE_SHA256, output_dir=output
            )

    def test_labels_and_duplicate_question_ids_are_refused(self) -> None:
        source = _questions(2)
        with self.assertRaisesRegex(InterleavedPlanError, "labels/outcomes"):
            freeze_interleaved_plan(
                [{**source[0], "correct_answer_id": "A"}, *source[1:]],
                seed="safe-seed", experiment_id="test-forbidden",
                public_source_sha256=SOURCE_SHA256,
                output_dir=self.root / "forbidden",
            )
        with self.assertRaisesRegex(InterleavedPlanError, "IDs repeat"):
            freeze_interleaved_plan(
                [source[0], dict(source[0]),
                 *source[2:]],
                seed="safe-seed", experiment_id="test-duplicates",
                public_source_sha256=SOURCE_SHA256,
                output_dir=self.root / "duplicates",
            )
        self.assertFalse((self.root / "forbidden").exists())
        self.assertFalse((self.root / "duplicates").exists())

    def test_options_and_schedule_are_source_bound(self) -> None:
        source = _questions(2)
        output = self.root / "bound"
        freeze_interleaved_plan(
            source, seed="bound-seed", experiment_id="bound-episode",
            public_source_sha256=SOURCE_SHA256, output_dir=output,
        )
        altered = [dict(row) for row in source]
        altered[0]["answer_options"] = [
            dict(option) for option in source[0]["answer_options"]
        ]
        altered[0]["answer_options"][0]["text"] = "altered option"
        with self.assertRaisesRegex(
            InterleavedPlanError, "differs from its canonical N1 task"
        ):
            verify_interleaved_plan(
                output, altered, public_source_sha256=SOURCE_SHA256
            )
        schedule = output / "interleaved-schedule.jsonl"
        schedule.write_bytes(schedule.read_bytes() + b"\n")
        with self.assertRaisesRegex(
            InterleavedPlanError, "checksums differ"
        ):
            verify_interleaved_plan(
                output, source, public_source_sha256=SOURCE_SHA256
            )

    def test_fresh_cohort_is_outcome_blind_and_complete(self) -> None:
        source = _questions(7)
        extra_id = "video-0-extra-causal"
        source.append({
            **source[0],
            "question_id": extra_id,
            "public_task_sha256": build_n1_public_task_binding(
                workload_id=extra_id,
                object_id=source[0]["object_id"],
                task_class_id=source[0]["stratum"],
                question=source[0]["question"],
                answer_options=source[0]["answer_options"],
                success_scoring_rule=(
                    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE
                ),
            )["task_binding_sha256"],
        })
        selected = select_outcome_blind_cohort(
            source, seed="fresh-seed", excluded_object_ids=["video-0"],
        )
        self.assertEqual(12, len(selected))
        self.assertEqual(4, len({row["object_id"] for row in selected}))
        self.assertNotIn("video-0", {row["object_id"] for row in selected})
        self.assertEqual(
            selected,
            select_outcome_blind_cohort(
                list(reversed(source)), seed="fresh-seed",
                excluded_object_ids=["video-0"],
            ),
        )
        with self.assertRaisesRegex(
            InterleavedPlanError, "cannot fill"
        ):
            select_outcome_blind_cohort(
                source, seed="fresh-seed",
                excluded_object_ids=[f"video-{index}" for index in range(4)],
                object_count=4,
            )


if __name__ == "__main__":
    unittest.main()
