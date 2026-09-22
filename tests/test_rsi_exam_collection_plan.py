from __future__ import annotations

import hashlib
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from pathfinder.rsi_exam.collection_plan import (
    CASES_NAME,
    CHECKSUMS_NAME,
    MANIFEST_NAME,
    OPERATIONS_NAME,
    OfflineReplayError,
    audit_collection_candidates,
    freeze_collection_plan,
    verify_collection_plan,
)


BUILDER_COMMIT = "7" * 40


def _write_json(path: Path, value: object) -> None:
    path.write_bytes(
        (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode(
            "utf-8"
        )
    )


def _task(object_id: str, workload_id: str, ordinal: int) -> dict:
    binding = hashlib.sha256(
        f"{object_id}:{workload_id}:{ordinal}".encode("utf-8")
    ).hexdigest()
    return {
        "answer_options": [
            {"option_id": "A", "text": "first"},
            {"option_id": "B", "text": "second"},
        ],
        "credentials_recorded": False,
        "object_id": object_id,
        "question": f"public question {ordinal}",
        "schema_version": "pathfinder.n1-public-task-binding/v1alpha1",
        "success_scoring_rule": "multiple-choice-option-id-canonical-match-v1",
        "task_binding_sha256": binding,
        "task_class_id": "video_qa",
        "workload_id": workload_id,
    }


def _task_set() -> dict:
    tasks = []
    ordinal = 0
    for stratum in ("causal", "descriptive", "temporal"):
        for item in range(5):
            tasks.append(_task(f"video-{stratum}-{item}", stratum, ordinal))
            ordinal += 1
    return {
        "credentials_recorded": False,
        "label_values_included": False,
        "schema_version": "pathfinder.public-task-set/v1alpha1",
        "task_plane_id": "public-cohort-test-v1",
        "tasks": tasks,
    }


def _spec() -> dict:
    return {
        "cohort_id": "collection-test-v1",
        "collection_repetitions": 2,
        "schema_version": "pathfinder.rsi-exam-cohort-spec/v1alpha1",
        "selection_seed": "fixed-public-seed-v1",
        "split_stratum_targets": {
            "train": {"causal": 2, "descriptive": 2, "temporal": 2},
            "development": {"causal": 1, "descriptive": 1, "temporal": 1},
            "test": {"causal": 1, "descriptive": 1, "temporal": 1},
        },
        "stratum_by_workload": {
            "causal": "causal",
            "descriptive": "descriptive",
            "temporal": "temporal",
        },
    }


def _inputs(root: Path) -> tuple[Path, Path]:
    task_path = root / "public-tasks.json"
    spec_path = root / "cohort-spec.json"
    _write_json(task_path, _task_set())
    _write_json(spec_path, _spec())
    return task_path, spec_path


def _raw_bindings(root: Path, sizes: dict[str, int]) -> Path:
    path = root / "raw-bindings.json"
    _write_json(path, {
        "catalog_version": "fixture-catalog-v1",
        "credentials_recorded": False,
        "dataset_id": "fixture",
        "dataset_revision": "fixture-v1",
        "objects": [
            {
                "artifact_path": f"/fixture/{object_id}.mp4",
                "artifact_sha256": hashlib.sha256(
                    object_id.encode("utf-8")
                ).hexdigest(),
                "artifact_size_bytes": size,
                "object_id": object_id,
                "source_object_id": object_id,
            }
            for object_id, size in sorted(sizes.items())
        ],
        "package_id": "fixture-raw-candidates-v1",
        "plan_ids": ["D0", "D1", "D2", "D3", "D4", "D5", "D6", "D7"],
        "schema_version": "pathfinder.simulator-raw-cold-bindings/v1alpha1",
    })
    return path


class CollectionPlanTest(unittest.TestCase):
    def test_v2_filters_oversized_raw_video_before_seeded_selection(self) -> None:
        with TemporaryDirectory() as name:
            root = Path(name)
            task_path, spec_path = _inputs(root)
            tasks = _task_set()
            tasks["tasks"].extend([
                _task("video-causal-replacement", "causal", 90),
                _task("video-temporal-replacement", "temporal", 91),
            ])
            _write_json(task_path, tasks)
            spec = _spec()
            spec["schema_version"] = "pathfinder.rsi-exam-cohort-spec/v1alpha2"
            spec["max_direct_video_bytes"] = 7_000_000
            _write_json(spec_path, spec)
            sizes = {
                task["object_id"]: 1_000_000 for task in tasks["tasks"]
            }
            oversized = "video-causal-0"
            sizes[oversized] = 9_000_000
            bindings = _raw_bindings(root, sizes)

            audit = audit_collection_candidates(
                task_path,
                spec_path,
                bindings,
            )
            self.assertEqual("READY_FOR_OUTCOME_BLIND_SELECTION", audit["status"])
            self.assertEqual(7_000_000, audit["max_direct_video_bytes"])
            self.assertRegex(audit["raw_candidate_bindings_sha256"], r"^[0-9a-f]{64}$")

            plan = root / "plan-v2"
            freeze_collection_plan(
                task_path,
                spec_path,
                builder_commit=BUILDER_COMMIT,
                output_dir=plan,
                raw_candidate_bindings=bindings,
            )
            verified = verify_collection_plan(
                plan,
                public_task_set=task_path,
                cohort_spec=spec_path,
                builder_commit=BUILDER_COMMIT,
                raw_candidate_bindings=bindings,
            )
            self.assertTrue(verified["source_binding_checked"])
            manifest = json.loads((plan / MANIFEST_NAME).read_text("utf-8"))
            self.assertEqual(
                "pathfinder.rsi-exam-trace-collection-plan/v1alpha2",
                manifest["schema_version"],
            )
            cases = [
                json.loads(line)
                for line in (plan / CASES_NAME).read_text("utf-8").splitlines()
            ]
            self.assertNotIn(oversized, {row["object_id"] for row in cases})
            self.assertTrue(
                all(row["raw_video_size_bytes"] <= 7_000_000 for row in cases)
            )

    def test_freeze_is_outcome_blind_video_disjoint_and_source_bound(self) -> None:
        with TemporaryDirectory() as name:
            root = Path(name)
            task_path, spec_path = _inputs(root)
            plan = root / "plan"
            receipt = freeze_collection_plan(
                task_path,
                spec_path,
                builder_commit=BUILDER_COMMIT,
                output_dir=plan,
            )
            self.assertEqual(
                "FROZEN_OUTCOME_BLIND_COLLECTION_PLAN",
                receipt["status"],
            )
            self.assertEqual(12, receipt["case_count"])
            self.assertEqual(240, receipt["operation_count"])
            verified = verify_collection_plan(
                plan,
                public_task_set=task_path,
                cohort_spec=spec_path,
                builder_commit=BUILDER_COMMIT,
            )
            self.assertEqual(
                "VERIFIED_OUTCOME_BLIND_COLLECTION_PLAN",
                verified["status"],
            )
            self.assertTrue(verified["source_binding_checked"])
            self.assertTrue(verified["video_disjoint"])
            for path in plan.iterdir():
                self.assertNotIn(b"\r", path.read_bytes(), path.name)
            cases = [
                json.loads(line)
                for line in (plan / CASES_NAME).read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(12, len({row["object_id"] for row in cases}))
            self.assertTrue(all("question" not in row for row in cases))
            self.assertTrue(all("answer_options" not in row for row in cases))

    def test_same_seed_is_byte_deterministic(self) -> None:
        with TemporaryDirectory() as name:
            root = Path(name)
            task_path, spec_path = _inputs(root)
            first = root / "first"
            second = root / "second"
            freeze_collection_plan(
                task_path,
                spec_path,
                builder_commit=BUILDER_COMMIT,
                output_dir=first,
            )
            freeze_collection_plan(
                task_path,
                spec_path,
                builder_commit=BUILDER_COMMIT,
                output_dir=second,
            )
            self.assertEqual(
                (first / CHECKSUMS_NAME).read_bytes(),
                (second / CHECKSUMS_NAME).read_bytes(),
            )
            for filename in (
                MANIFEST_NAME,
                CASES_NAME,
                OPERATIONS_NAME,
                CHECKSUMS_NAME,
            ):
                self.assertEqual(
                    (first / filename).read_bytes(),
                    (second / filename).read_bytes(),
                )

    def test_candidate_audit_reports_shortfall_without_freezing(self) -> None:
        with TemporaryDirectory() as name:
            root = Path(name)
            task_path, spec_path = _inputs(root)
            tasks = _task_set()
            tasks["tasks"] = tasks["tasks"][:3]
            _write_json(task_path, tasks)
            audit = audit_collection_candidates(task_path, spec_path)
            self.assertEqual(
                "BLOCKED_INSUFFICIENT_PUBLIC_CANDIDATES",
                audit["status"],
            )
            self.assertFalse(audit["minimum_count_gate_satisfied"])
            self.assertFalse(audit["task_outcomes_read"])
            with self.assertRaisesRegex(
                OfflineReplayError,
                "minimum stratum counts",
            ):
                freeze_collection_plan(
                    task_path,
                    spec_path,
                    builder_commit=BUILDER_COMMIT,
                    output_dir=root / "plan",
                )
            self.assertFalse((root / "plan").exists())

    def test_outcome_bearing_public_task_is_rejected(self) -> None:
        with TemporaryDirectory() as name:
            root = Path(name)
            task_path, spec_path = _inputs(root)
            value = _task_set()
            value["tasks"][0]["task_success"] = True
            _write_json(task_path, value)
            with self.assertRaisesRegex(
                OfflineReplayError,
                "unsupported fields",
            ):
                audit_collection_candidates(task_path, spec_path)

    def test_matching_keeps_one_object_in_only_one_split(self) -> None:
        with TemporaryDirectory() as name:
            root = Path(name)
            task_path, spec_path = _inputs(root)
            value = _task_set()
            value["tasks"].extend([
                _task("shared-video", "causal", 90),
                _task("shared-video", "temporal", 91),
            ])
            _write_json(task_path, value)
            plan = root / "plan"
            freeze_collection_plan(
                task_path,
                spec_path,
                builder_commit=BUILDER_COMMIT,
                output_dir=plan,
            )
            cases = [
                json.loads(line)
                for line in (plan / CASES_NAME).read_text(encoding="utf-8").splitlines()
            ]
            objects = [row["object_id"] for row in cases]
            self.assertEqual(len(objects), len(set(objects)))

    def test_audit_rejects_counts_that_cannot_form_disjoint_assignment(self) -> None:
        with TemporaryDirectory() as name:
            root = Path(name)
            task_path = root / "public-tasks.json"
            spec_path = root / "cohort-spec.json"
            value = {
                "credentials_recorded": False,
                "label_values_included": False,
                "schema_version": "pathfinder.public-task-set/v1alpha1",
                "task_plane_id": "overlap-only-v1",
                "tasks": [
                    _task("shared-video", "causal", 90),
                    _task("shared-video", "temporal", 91),
                ],
            }
            spec = {
                "cohort_id": "overlap-only-v1",
                "collection_repetitions": 1,
                "schema_version": "pathfinder.rsi-exam-cohort-spec/v1alpha1",
                "selection_seed": "overlap-only-seed",
                "split_stratum_targets": {
                    "train": {"causal": 1, "temporal": 1},
                },
                "stratum_by_workload": {
                    "causal": "causal",
                    "temporal": "temporal",
                },
            }
            _write_json(task_path, value)
            _write_json(spec_path, spec)
            audit = audit_collection_candidates(task_path, spec_path)
            self.assertTrue(audit["minimum_count_gate_satisfied"])
            self.assertFalse(audit["video_disjoint_assignment_satisfied"])
            self.assertEqual(
                "BLOCKED_VIDEO_DISJOINT_ASSIGNMENT",
                audit["status"],
            )

    def test_tampered_operation_is_rejected(self) -> None:
        with TemporaryDirectory() as name:
            root = Path(name)
            task_path, spec_path = _inputs(root)
            plan = root / "plan"
            freeze_collection_plan(
                task_path,
                spec_path,
                builder_commit=BUILDER_COMMIT,
                output_dir=plan,
            )
            (plan / OPERATIONS_NAME).write_bytes(
                (plan / OPERATIONS_NAME).read_bytes() + b"{}\n"
            )
            with self.assertRaisesRegex(OfflineReplayError, "mismatch"):
                verify_collection_plan(plan)


if __name__ == "__main__":
    unittest.main()
