"""The four public arms form executable DAG shapes without authorizing runs."""

from __future__ import annotations

import unittest
import tempfile
from pathlib import Path
from unittest.mock import patch

from pathfinder.distributed.scoring import (
    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
)
from pathfinder.simulator.hidden_oracle import build_n1_public_task_binding
from pathfinder.simulator.interleaved_multiq_trial_dag import (
    InterleavedTrialDagError,
    build_interleaved_trial_dag,
    freeze_interleaved_trial_dags,
    verify_interleaved_trial_dags,
)


OBJECT_ID = "nextqa-val-123"
QUESTION_ID = "nextqa-val-123-q2"


def _question() -> dict:
    options = [
        {"option_id": option, "text": f"choice {option}"}
        for option in "ABCDE"
    ]
    task = build_n1_public_task_binding(
        workload_id=QUESTION_ID, object_id=OBJECT_ID,
        task_class_id="temporal", question="What happened next?",
        answer_options=options,
        success_scoring_rule=MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
    )
    return {
        "question_id": QUESTION_ID,
        "object_id": OBJECT_ID,
        "stratum": "temporal",
        "question": task["question"],
        "answer_options": options,
        "public_task_sha256": task["task_binding_sha256"],
    }


def _artifact(representation: str, character: str) -> dict:
    return {
        "object_id": OBJECT_ID,
        "representation_id": representation,
        "artifact_sha256": character * 64,
        "artifact_size_bytes": 100,
    }


def _build(arm: str, *, question: dict | None = None,
           executor_node_id: str = "N7", indexed_raw: bool = False,
           design_id: str | None = None, repetition: int = 0):
    question = question or _question()
    route = {
        "arm_id": arm,
        "object_id": OBJECT_ID,
        "public_task_sha256": question["public_task_sha256"],
        "trial_key": f"multiq|{QUESTION_ID}|{arm}",
        "ordinal": 1,
    }
    if design_id is not None:
        route.update({"design_id": design_id, "repetition": repetition,
                      "order_index": 13})
    return build_interleaved_trial_dag(
        route, question,
        raw_artifact=_artifact("raw_video", "a"),
        derived_artifacts={
            "multimodal_digest": _artifact("multimodal_digest", "b"),
            "sampled_frame_bundle": _artifact("sampled_frame_bundle", "c"),
        },
        n3_catalog_version="n3-test",
        n4_catalog_version="n4-test",
        selected_policy={
            "frame_count": 4,
            "temporal_window_fraction": [0.25, 0.75],
        } if arm == "I" else None,
        executor_node_id=executor_node_id,
        indexed_raw=indexed_raw,
    )


class InterleavedTrialDagTests(unittest.TestCase):
    def test_all_four_arms_validate_without_submission_authority(self) -> None:
        expected = {
            "R": ("raw", "direct-video"),
            "D": ("remote-derived", "digest+frames-fusion"),
            "DC": ("local-cache-derived", "digest+frames-fusion"),
            "I": ("indexed-derived", "digest+indexed-frames-fusion"),
        }
        for arm, (family, mode) in expected.items():
            with self.subTest(arm=arm):
                trial, stages = _build(arm)
                self.assertEqual(trial["route_family"], family)
                self.assertEqual(trial["semantic_input_profile"]["input_mode"], mode)
                self.assertEqual(len(stages), len(trial["semantic_stage_keys"]))
                self.assertFalse(trial["flowmesh_submission_authorized"])
                self.assertEqual(
                    trial["required_runtime_adapter_ids"],
                    ["multiq-runtime-admission-pending"],
                )

    def test_derived_and_cache_have_same_model_input_profile(self) -> None:
        direct, _ = _build("D")
        cached, stages = _build("DC")
        self.assertEqual(direct["semantic_input_profile"],
                         cached["semantic_input_profile"])
        self.assertEqual(
            {row["object_representation_identity"]["representation_id"]
             for row in stages if row["action"] == "lookup"},
            {"multimodal_digest", "sampled_frame_bundle"},
        )
        self.assertEqual(
            {row["condition"]["equals"] for row in stages
             if row["condition"] is not None},
            {"hit", "miss"},
        )

    def test_indexed_fusion_binds_query_aware_raw_and_digest(self) -> None:
        trial, stages = _build("I")
        self.assertEqual(
            {row["representation_id"] for row in
             trial["representation_identities"]},
            {"raw_video", "multimodal_digest"},
        )
        self.assertEqual(
            trial["semantic_input_profile"]["frame_selection"]
            ["temporal_window_fraction"],
            [0.25, 0.75],
        )
        self.assertTrue(any(row["action"] == "query-index" for row in stages))
        self.assertFalse(any(row["action"] == "lookup" for row in stages))

    def test_ten_route_indexed_raw_reuses_dag_on_both_nodes(self) -> None:
        for node in ("N7", "N8"):
            with self.subTest(node=node):
                trial, stages = _build(
                    "I", executor_node_id=node, indexed_raw=True,
                    design_id="D1" if node == "N7" else "D5",
                )
                self.assertEqual(trial["executor_node_id"], node)
                self.assertEqual(trial["design_id"],
                                 "D1" if node == "N7" else "D5")
                self.assertEqual(trial["route_family"], "indexed-raw")
                self.assertEqual(
                    {row["representation_id"] for row in
                     trial["representation_identities"]}, {"raw_video"},
                )
                self.assertFalse(any(row["stage_key"].endswith("read-digest")
                                     for row in stages))
                self.assertTrue(any(
                    row["action"] == "transfer-bytes"
                    and row["logical_node_ids"] == ["N3", node]
                    for row in stages
                ))

    def test_ten_route_cache_and_bypass_keep_identical_model_content(self) -> None:
        for node in ("N7", "N8"):
            direct, _ = _build("D", executor_node_id=node)
            cached, stages = _build("DC", executor_node_id=node)
            self.assertEqual(direct["semantic_input_profile"],
                             cached["semantic_input_profile"])
            self.assertEqual({row["logical_node_ids"][0] for row in stages
                              if row["action"] == "lookup"}, {node})

    def test_missing_query_policy_and_wrong_task_fail_closed(self) -> None:
        question = _question()
        route = {
            "arm_id": "I", "object_id": OBJECT_ID,
            "public_task_sha256": question["public_task_sha256"],
            "trial_key": "multiq|wrong", "ordinal": 0,
        }
        with self.assertRaises(InterleavedTrialDagError):
            build_interleaved_trial_dag(
                route, question,
                raw_artifact=_artifact("raw_video", "a"),
                derived_artifacts={
                    "multimodal_digest": _artifact("multimodal_digest", "b"),
                    "sampled_frame_bundle": _artifact("sampled_frame_bundle", "c"),
                },
                n3_catalog_version="n3-test", n4_catalog_version="n4-test",
            )
        altered = dict(question, public_task_sha256="0" * 64)
        with self.assertRaises(InterleavedTrialDagError):
            _build("R", question=altered)

    def test_frozen_dag_package_is_immutable_and_not_admitted(self) -> None:
        documents = {
            "interleaved-trial-dags.json": (
                b'{"trial_count":24,"stage_count":276,'
                b'"package_sha256":"' + b"a" * 64 + b'"}'
            ),
            "interleaved-trials.jsonl": b'{"trial_key":"first"}\n',
            "interleaved-stages.jsonl": b'{"stage_key":"first|schedule"}\n',
        }
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "dag"
            with patch(
                "pathfinder.simulator.interleaved_multiq_trial_dag."
                "_package_contents", return_value=documents,
            ):
                report = freeze_interleaved_trial_dags(output_dir=output)
                self.assertEqual(report["trial_count"], 24)
                self.assertFalse(report["runtime_admission_created"])
                self.assertFalse(report["workflow_submitted"])
                (output / "interleaved-trials.jsonl").write_bytes(b"changed")
                with self.assertRaises(InterleavedTrialDagError):
                    verify_interleaved_trial_dags(output)


if __name__ == "__main__":
    unittest.main()
