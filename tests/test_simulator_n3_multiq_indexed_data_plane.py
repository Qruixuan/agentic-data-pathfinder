from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from pathfinder.data_agent_manifest import load_data_agent_manifest
from pathfinder.simulator.n3_indexed_data_plane import (
    INDEXED_REPRESENTATION_ID,
    N3TemporalSelectionPolicy,
    TEMPORAL_INDEX_SELECTED_SAMPLING_METHOD,
)
from pathfinder.simulator.full_flow_multiq_exact_selection import (
    MultiQuestionExactSelectionCatalog,
    MultiQuestionSelectionError,
)
from pathfinder.simulator.full_flow_semantic_route_runtime import ArtifactIdentity
from pathfinder.simulator.n3_multiq_indexed_data_plane import (
    N3MultiQuestionPackageError,
    build_n3_multiq_indexed_package,
    verify_n3_multiq_indexed_package,
)
from pathfinder.simulator.raw_cold_data_plane import (
    DATA_AGENT_MANIFEST_PATH,
    PACKAGE_MANIFEST_NAME,
    RawColdObjectBinding,
    build_raw_cold_data_plane_package,
)
from tests.test_simulator_n3_indexed_data_plane import _Sampler, _mp4, _sha256


def _question(object_id: str, ordinal: int) -> dict:
    question_sha = hashlib.sha256(f"question-{ordinal}".encode()).hexdigest()
    policy = N3TemporalSelectionPolicy(
        frame_count=2 + ordinal,
        temporal_start_fraction=0.1,
        temporal_end_fraction=0.4,
        sampling_method=TEMPORAL_INDEX_SELECTED_SAMPLING_METHOD,
        selection_provenance={
            "action_id": "temporal-index-multiq-v1",
            "anchor_top_k": 2,
            "anchor_window_ordinals": [1, 2],
            "expansion_basis": "timestamp",
            "fallback_used": False,
            "max_selected_windows": 4,
            "merged_intervals_seconds": [[4.0, 16.0]],
            "public_question_sha256": question_sha,
            "relation": "none",
            "selected_window_ordinals": [1, 2],
            "temporal_index_package_sha256": "2" * 64,
        },
    )
    return {
        "question_id": f"question-{ordinal}",
        "object_id": object_id,
        "task_binding_sha256": hashlib.sha256(
            f"task-{ordinal}".encode()
        ).hexdigest(),
        "public_question_sha256": question_sha,
        "selection_policy": policy,
    }


class N3MultiQuestionIndexedPackageTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.object_id = "nextqa-val-multiq-test"
        source = self.root / "source.mp4"
        payload = _mp4()
        source.write_bytes(payload)
        self.raw = self.root / "raw"
        build_raw_cold_data_plane_package(
            [RawColdObjectBinding(
                object_id=self.object_id,
                artifact_path=source,
                catalog_version="multiq-catalog-v1",
                plan_ids=("legacy-plan",),
                dataset_id="nextqa",
                dataset_revision="multiq-test",
                source_object_id="multiq-test",
                artifact_sha256=_sha256(payload),
                artifact_size_bytes=len(payload),
            )],
            output_dir=self.raw,
            package_id="n3-raw-multiq-test-v1",
        )
        self.questions = [_question(self.object_id, index) for index in (0, 1)]
        self.sampler = _Sampler()

    def test_two_questions_resolve_different_bundles_for_one_video(self) -> None:
        package = self.root / "multiq"
        result = build_n3_multiq_indexed_package(
            self.raw, output_dir=package, package_id="n3-multiq-test-v1",
            question_policies=self.questions, sampler=self.sampler,
        )
        self.assertEqual(1, result["object_count"])
        self.assertEqual(2, result["question_count"])
        self.assertEqual(
            result,
            verify_n3_multiq_indexed_package(
                package, raw_package_dir=self.raw,
                question_policies=list(reversed(self.questions)),
                sampler=self.sampler,
            ),
        )
        report = json.loads((package / PACKAGE_MANIFEST_NAME).read_bytes())
        catalog = json.loads((package / "config/object-catalog.json").read_bytes())
        default_path = catalog["objects"][self.object_id]["representations"][
            INDEXED_REPRESENTATION_ID
        ]["path"]
        self.assertFalse((package / "config" / default_path).exists())
        manifest = load_data_agent_manifest(package / DATA_AGENT_MANIFEST_PATH)
        resolved = [manifest.resolve(
            plan_id=row["plan_id"], object_id=self.object_id,
            representation_id=INDEXED_REPRESENTATION_ID,
            requested_location="origin-cold",
        ).path.read_bytes() for row in report["question_selections"]]
        self.assertNotEqual(resolved[0], resolved[1])
        raw_row = report["raw_objects"][0]
        identity = ArtifactIdentity(
            object_id=self.object_id, representation_id="raw_video",
            artifact_sha256=raw_row["artifact_sha256"],
            artifact_size_bytes=raw_row["artifact_size_bytes"],
            object_catalog_version=raw_row["catalog_version"],
        )
        resolver = MultiQuestionExactSelectionCatalog(
            package, raw_package_dir=self.raw,
            question_policies=self.questions, sampler=self.sampler,
        )
        selections = [resolver.resolve_for_task(
            identity, task_binding_sha256=question["task_binding_sha256"]
        ) for question in self.questions]
        self.assertNotEqual(
            selections[0].selected_artifact_sha256,
            selections[1].selected_artifact_sha256,
        )
        with self.assertRaisesRegex(
            MultiQuestionSelectionError, "binds this video and task"
        ):
            resolver.resolve_for_task(
                identity, task_binding_sha256="0" * 64
            )

    def test_label_field_and_tampering_fail_closed(self) -> None:
        with self.assertRaisesRegex(
            N3MultiQuestionPackageError, "unsafe or missing"
        ):
            build_n3_multiq_indexed_package(
                self.raw, output_dir=self.root / "forbidden",
                package_id="forbidden", sampler=self.sampler,
                question_policies=[
                    {**self.questions[0], "correct_answer_id": "A"},
                ],
            )
        package = self.root / "valid"
        build_n3_multiq_indexed_package(
            self.raw, output_dir=package, package_id="n3-multiq-valid",
            question_policies=self.questions, sampler=self.sampler,
        )
        report = json.loads((package / PACKAGE_MANIFEST_NAME).read_bytes())
        selected = report["question_selections"][0]
        artifact = package / selected["artifact_package_path"]
        artifact.write_bytes(artifact.read_bytes() + b"tampered")
        with self.assertRaisesRegex(
            N3MultiQuestionPackageError, "checksums differ"
        ):
            verify_n3_multiq_indexed_package(
                package, raw_package_dir=self.raw,
                question_policies=self.questions, sampler=self.sampler,
            )

    def test_question_plan_cannot_fetch_another_videos_projection(self) -> None:
        other_id = "nextqa-val-multiq-other"
        other_source = self.root / "other.mp4"
        payload = _mp4()
        other_source.write_bytes(payload)
        combined = self.root / "combined-raw"
        bindings = [
            RawColdObjectBinding(
                object_id=object_id,
                artifact_path=source,
                catalog_version="multiq-catalog-v2",
                plan_ids=("legacy-plan",),
                dataset_id="nextqa",
                dataset_revision="multiq-test",
                source_object_id=object_id,
                artifact_sha256=_sha256(payload),
                artifact_size_bytes=len(payload),
            )
            for object_id, source in (
                (self.object_id, self.root / "source.mp4"),
                (other_id, other_source),
            )
        ]
        build_raw_cold_data_plane_package(
            bindings, output_dir=combined, package_id="n3-raw-multiq-two",
        )
        questions = [_question(self.object_id, 0), _question(other_id, 1)]
        package = self.root / "multiq-two"
        build_n3_multiq_indexed_package(
            combined, output_dir=package, package_id="n3-multiq-two",
            question_policies=questions, sampler=self.sampler,
        )
        manifest = load_data_agent_manifest(package / DATA_AGENT_MANIFEST_PATH)
        report = json.loads((package / PACKAGE_MANIFEST_NAME).read_bytes())
        for row in report["question_selections"]:
            other = other_id if row["object_id"] == self.object_id else self.object_id
            unrelated = manifest.resolve(
                plan_id=row["plan_id"], object_id=other,
                representation_id=INDEXED_REPRESENTATION_ID,
                requested_location="origin-cold",
            )
            self.assertFalse(unrelated.path.is_file())


if __name__ == "__main__":
    unittest.main()
