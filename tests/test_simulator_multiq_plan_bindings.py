"""Source-bound Data Agent plan IDs for the interleaved four-arm pilot."""

from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from pathfinder.distributed.scoring import (
    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
)
from pathfinder.rsi_exam.interleaved_multiq_plan import (
    freeze_interleaved_plan,
    interleaved_trial_key,
)
from pathfinder.simulator.full_flow_multiq_exact_selection import (
    MultiQuestionSelectionError,
    interleaved_data_agent_plan_bindings,
)
from pathfinder.simulator.hidden_oracle import build_n1_public_task_binding
from pathfinder.simulator.n3_multiq_indexed_data_plane import (
    build_n3_multiq_indexed_package,
)
from pathfinder.simulator.raw_cold_data_plane import (
    RawColdObjectBinding,
    build_raw_cold_data_plane_package,
)
from tests.test_simulator_n3_indexed_data_plane import _Sampler, _mp4, _sha256
from tests.test_simulator_n3_multiq_indexed_data_plane import _question


SOURCE_SHA = hashlib.sha256(b"public-question-source").hexdigest()


class MultiQuestionPlanBindingTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        video = self.root / "video.mp4"
        payload = _mp4()
        video.write_bytes(payload)
        self.objects = ("nextqa-val-a", "nextqa-val-b")
        self.raw = self.root / "raw"
        build_raw_cold_data_plane_package(
            [RawColdObjectBinding(
                object_id=object_id,
                artifact_path=video,
                catalog_version="multiq-catalog-v1",
                plan_ids=("legacy-plan",),
                dataset_id="nextqa",
                dataset_revision="multiq-test",
                source_object_id=object_id,
                artifact_sha256=_sha256(payload),
                artifact_size_bytes=len(payload),
            ) for object_id in self.objects],
            output_dir=self.raw,
            package_id="n3-raw-multiq-bindings-test-v1",
        )
        self.questions = []
        self.policies = []
        index = 0
        for object_id in self.objects:
            for stratum in ("causal", "temporal", "descriptive"):
                question_id = f"{object_id}-{stratum}"
                question = f"What happened in {question_id}?"
                options = [
                    {"option_id": chr(ord("A") + option),
                     "text": f"option {option}"}
                    for option in range(5)
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
                self.questions.append({
                    "question_id": question_id,
                    "object_id": object_id,
                    "stratum": stratum,
                    "question": question,
                    "answer_options": options,
                    "public_task_sha256": task["task_binding_sha256"],
                })
                policy = _question(object_id, index)["selection_policy"]
                policy = replace(
                    policy,
                    selection_provenance={
                        **policy.selection_provenance,
                        "public_question_sha256": hashlib.sha256(
                            question.encode("utf-8")
                        ).hexdigest(),
                    },
                )
                self.policies.append({
                    "question_id": question_id,
                    "object_id": object_id,
                    "task_binding_sha256": task["task_binding_sha256"],
                    "public_question_sha256": hashlib.sha256(
                        question.encode("utf-8")
                    ).hexdigest(),
                    "selection_policy": policy,
                })
                index += 1
        self.plan = self.root / "plan"
        freeze_interleaved_plan(
            self.questions,
            seed="multiq-plan-binding-test-seed",
            experiment_id="multiq-plan-binding-test",
            public_source_sha256=SOURCE_SHA,
            output_dir=self.plan,
        )
        self.n3 = self.root / "n3-multiq"
        self.sampler = _Sampler()
        build_n3_multiq_indexed_package(
            self.raw, output_dir=self.n3,
            package_id="n3-multiq-plan-binding-test-v1",
            question_policies=self.policies, sampler=self.sampler,
        )
        self.n4 = self.root / "n4"
        self.n4.mkdir()
        (self.n4 / "n4-derived-data-package.json").write_text(
            json.dumps({"objects": [
                {"object_id": object_id,
                 "representation_id": representation,
                 "plan_ids": ["D2", "D3"]}
                for object_id in self.objects
                for representation in (
                    "multimodal_digest", "sampled_frame_bundle"
                )
            ]}), encoding="utf-8",
        )

    def _bind(self) -> dict[tuple[str, str, str, str], str]:
        with patch(
            "pathfinder.simulator.full_flow_multiq_exact_selection."
            "verify_n4_derived_data_package",
            return_value={"status": "VERIFIED"},
        ):
            return interleaved_data_agent_plan_bindings(
                plan_dir=self.plan,
                public_questions=self.questions,
                public_source_sha256=SOURCE_SHA,
                n3_package_dir=self.n3,
                raw_package_dir=self.raw,
                question_policies=self.policies,
                n4_package_dir=self.n4,
                sampler=self.sampler,
            )

    def test_six_questions_bind_24_routes_and_distinct_n3_plans(self) -> None:
        bindings = self._bind()
        self.assertEqual(42, len(bindings))
        for question in self.questions:
            object_id = question["object_id"]
            plans = []
            for arm in ("R", "D", "DC", "I"):
                key = interleaved_trial_key(
                    "multiq-plan-binding-test", question["question_id"], arm,
                )
                if arm in {"R", "I"}:
                    representation = (
                        "raw_video" if arm == "R"
                        else "indexed_temporal_frame_bundle"
                    )
                    plans.append(bindings[
                        (key, "N3", object_id, representation)
                    ])
                else:
                    self.assertEqual(
                        "D2" if arm == "D" else "D3",
                        bindings[(key, "N4", object_id,
                                  "multimodal_digest")],
                    )
            self.assertEqual(plans[0], plans[1])
        indexed_plans = {
            plan for (trial, node, _object, representation), plan
            in bindings.items()
            if node == "N3"
            and representation == "indexed_temporal_frame_bundle"
        }
        self.assertEqual(6, len(indexed_plans))

    def test_question_or_n4_binding_drift_is_rejected(self) -> None:
        changed = [dict(row) for row in self.questions]
        changed[0]["public_task_sha256"] = "0" * 64
        with self.assertRaisesRegex(Exception, "canonical N1 task"):
            with patch(
                "pathfinder.simulator.full_flow_multiq_exact_selection."
                "verify_n4_derived_data_package",
                return_value={"status": "VERIFIED"},
            ):
                interleaved_data_agent_plan_bindings(
                    plan_dir=self.plan, public_questions=changed,
                    public_source_sha256=SOURCE_SHA,
                    n3_package_dir=self.n3, raw_package_dir=self.raw,
                    question_policies=self.policies,
                    n4_package_dir=self.n4, sampler=self.sampler,
                )
        path = self.n4 / "n4-derived-data-package.json"
        document = json.loads(path.read_text(encoding="utf-8"))
        document["objects"][0]["plan_ids"] = ["D3"]
        path.write_text(json.dumps(document), encoding="utf-8")
        with self.assertRaisesRegex(
            MultiQuestionSelectionError,
            "N4 lacks the frozen arm artifact plan binding",
        ):
            self._bind()

    def test_selection_bound_to_other_question_text_is_rejected(self) -> None:
        wrong_digest = hashlib.sha256(b"different public question").hexdigest()
        altered = [dict(row) for row in self.policies]
        altered[0]["public_question_sha256"] = wrong_digest
        policy = altered[0]["selection_policy"]
        altered[0]["selection_policy"] = replace(
            policy,
            selection_provenance={
                **policy.selection_provenance,
                "public_question_sha256": wrong_digest,
            },
        )
        self.n3 = self.root / "n3-multiq-wrong-question"
        self.policies = altered
        build_n3_multiq_indexed_package(
            self.raw, output_dir=self.n3,
            package_id="n3-multiq-wrong-question-v1",
            question_policies=altered, sampler=self.sampler,
        )
        with self.assertRaisesRegex(
            MultiQuestionSelectionError,
            "does not bind the frozen question text",
        ):
            self._bind()


if __name__ == "__main__":
    unittest.main()
