from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from pathfinder.simulator.container_contract import plan_container_backend
from pathfinder.simulator.full_flow_logical_routes import (
    compile_full_flow_logical_routes,
)
from pathfinder.simulator.full_flow_semantic_matrix import (
    ARTIFACT_BINDINGS_NAME,
    ARTIFACT_BINDING_SET_SCHEMA_VERSION,
    CHECKSUMS_NAME,
    PLAN_NAME,
    PUBLIC_TASKS_NAME,
    SERVICE_CATALOG_NAME,
    STAGES_NAME,
    TRIALS_NAME,
    FullFlowSemanticMatrixError,
    compile_full_flow_semantic_matrix,
    verify_full_flow_semantic_matrix,
)
from pathfinder.simulator.hidden_oracle import (
    MULTIPLE_CHOICE_EXACT_SCORING_RULE,
    build_n1_public_task_binding,
)
from pathfinder.simulator.portable import build_portable_execution_plan


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = ROOT / "configs" / "flowmesh_infra_simulator_4x8_smoke.json"
CONTAINER_SPEC = (
    ROOT / "configs" / "flowmesh_infra_container_4x8_contract.json"
)

WORKLOADS = {
    "smoke-descriptive": ("video-descriptive", "video_qa_descriptive"),
    "smoke-temporal": ("video-temporal", "video_qa_temporal"),
    "smoke-causal": ("video-causal", "video_qa_causal"),
    "smoke-retrieval": ("video-retrieval-target", "video_retrieval"),
}


def _artifact_object_id(logical_object_id: str) -> str:
    return f"real-{logical_object_id}-20260915"


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False)
        + "\n"
    ).encode("utf-8")


def _jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _keys(value: object) -> set[str]:
    if isinstance(value, dict):
        return set(value) | set().union(*(_keys(item) for item in value.values()))
    if isinstance(value, list):
        return set().union(*(_keys(item) for item in value)) if value else set()
    return set()


def _public_task_document() -> dict:
    tasks = []
    for workload_id, (object_id, task_class) in sorted(WORKLOADS.items()):
        tasks.append(build_n1_public_task_binding(
            workload_id=workload_id,
            object_id=_artifact_object_id(object_id),
            task_class_id=task_class,
            question=f"Answer the public task for {workload_id}.",
            answer_options=[
                {"option_id": "A", "text": "First public option."},
                {"option_id": "B", "text": "Second public option."},
            ],
            success_scoring_rule=MULTIPLE_CHOICE_EXACT_SCORING_RULE,
        ))
    return {
        "schema_version": "pathfinder.public-task-set/v1alpha1",
        "task_plane_id": "semantic-matrix-public-tasks-v1",
        "tasks": tasks,
        "label_values_included": False,
        "credentials_recorded": False,
    }


def _artifact_binding_document() -> dict:
    used_representations = {
        "video-descriptive": {"multimodal_digest", "raw_video"},
        "video-temporal": {"raw_video", "sampled_frame_bundle"},
        "video-causal": {
            "multimodal_digest",
            "raw_video",
            "sampled_frame_bundle",
        },
        "video-retrieval-target": {
            "multimodal_digest",
            "raw_video",
            "sampled_frame_bundle",
        },
    }
    objects = []
    for logical_object_id in sorted({item[0] for item in WORKLOADS.values()}):
        representations = []
        for representation_id, size in (
            ("multimodal_digest", 32000),
            ("raw_video", 1234567),
            ("sampled_frame_bundle", 456789),
        ):
            if representation_id not in used_representations[logical_object_id]:
                continue
            representations.append({
                "representation_id": representation_id,
                "artifact_sha256": _sha256(
                    f"{logical_object_id}|{representation_id}".encode("utf-8")
                ),
                "artifact_size_bytes": size,
                "object_catalog_version": "real-artifact-catalog-v1",
            })
        objects.append({
            "logical_object_id": logical_object_id,
            "artifact_object_id": _artifact_object_id(logical_object_id),
            "representations": representations,
        })
    return {
        "schema_version": ARTIFACT_BINDING_SET_SCHEMA_VERSION,
        "binding_set_id": "real-artifact-bindings-v1",
        "objects": objects,
        "credentials_recorded": False,
    }


def _restamp(root: Path) -> None:
    plan_path = root / PLAN_NAME
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    plan["output_sha256"] = {
        name: _sha256((root / name).read_bytes())
        for name in (
            ARTIFACT_BINDINGS_NAME,
            PUBLIC_TASKS_NAME,
            SERVICE_CATALOG_NAME,
            STAGES_NAME,
            TRIALS_NAME,
        )
    }
    plan.pop("plan_sha256", None)
    plan["plan_sha256"] = _sha256(_canonical(plan))
    plan_path.write_bytes(_json_bytes(plan))
    names = sorted(
        (
            ARTIFACT_BINDINGS_NAME,
            PLAN_NAME,
            PUBLIC_TASKS_NAME,
            SERVICE_CATALOG_NAME,
            STAGES_NAME,
            TRIALS_NAME,
        )
    )
    (root / CHECKSUMS_NAME).write_text(
        "".join(
            f"{_sha256((root / name).read_bytes())}  {name}\n"
            for name in names
        ),
        encoding="utf-8",
    )


class FullFlowSemanticMatrixTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temporary.name)
        cls.portable = cls.root / "portable"
        cls.container = cls.root / "container"
        cls.logical = cls.root / "logical"
        cls.public_tasks = cls.root / "public-tasks.json"
        cls.artifact_bindings = cls.root / "artifact-bindings.json"
        build_portable_execution_plan(SCENARIO, output_dir=cls.portable)
        plan_container_backend(
            SCENARIO,
            cls.portable,
            CONTAINER_SPEC,
            output_dir=cls.container,
        )
        compile_full_flow_logical_routes(
            SCENARIO,
            cls.container,
            output_dir=cls.logical,
        )
        cls.public_tasks.write_bytes(_json_bytes(_public_task_document()))
        cls.artifact_bindings.write_bytes(
            _json_bytes(_artifact_binding_document())
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()

    def _compile(
        self,
        name: str,
        public_tasks: Path | None = None,
        artifact_bindings: Path | None = None,
    ) -> Path:
        output = self.root / name
        if output.exists():
            shutil.rmtree(output)
        compile_full_flow_semantic_matrix(
            self.logical,
            SCENARIO,
            self.container,
            public_tasks or self.public_tasks,
            artifact_bindings or self.artifact_bindings,
            output_dir=output,
        )
        return output

    def test_freezes_all_64_trials_and_all_route_families(self) -> None:
        output = self._compile("complete")
        plan = json.loads((output / PLAN_NAME).read_text(encoding="utf-8"))
        self.assertEqual(64, plan["matrix_dimensions"]["trial_count"])
        self.assertEqual(32, plan["matrix_dimensions"]["matrix_cell_count"])
        self.assertEqual(658, plan["coverage_summary"]["semantic_stage_count"])
        self.assertEqual(
            {
                "indexed-raw": 12,
                "local-cache-derived": 16,
                "raw": 20,
                "remote-derived": 16,
            },
            plan["coverage_summary"]["route_family_trial_counts"],
        )
        trials = _jsonl(output / TRIALS_NAME)
        self.assertEqual(
            {(f"W{w}", f"D{d}") for w in range(1, 5) for d in range(8)},
            {(row["workload_class"], row["design_id"]) for row in trials},
        )

    def test_binds_each_trial_to_exact_public_task_and_representations(self) -> None:
        output = self._compile("bindings")
        public = json.loads(
            (output / PUBLIC_TASKS_NAME).read_text(encoding="utf-8")
        )
        tasks = {row["workload_id"]: row for row in public["tasks"]}
        for trial in _jsonl(output / TRIALS_NAME):
            task = tasks[trial["workload_id"]]
            self.assertEqual(task["object_id"], trial["artifact_object_id"])
            self.assertNotEqual(
                trial["logical_object_id"],
                trial["artifact_object_id"],
            )
            self.assertEqual(
                task["task_binding_sha256"],
                trial["public_task_binding_sha256"],
            )
            self.assertEqual(
                [
                    {
                        "logical_object_id": trial["logical_object_id"],
                        "artifact_object_id": trial["artifact_object_id"],
                        "representation_id": representation_id,
                        "representation_binding": next(
                            binding
                            for binding in next(
                                item
                                for item in _artifact_binding_document()["objects"]
                                if item["logical_object_id"]
                                == trial["logical_object_id"]
                            )["representations"]
                            if binding["representation_id"] == representation_id
                        ),
                    }
                    for representation_id in trial["logical_trial"][
                        "representation_ids"
                    ]
                ],
                trial["representation_identities"],
            )

    def test_preserves_logical_dag_and_cache_conditions_exactly(self) -> None:
        output = self._compile("dag")
        logical = _jsonl(self.logical / "logical-route-stages.jsonl")
        semantic = _jsonl(output / STAGES_NAME)
        self.assertEqual(len(logical), len(semantic))
        self.assertEqual(
            logical,
            [row["logical_stage"] for row in semantic],
        )
        conditions = [row["condition"] for row in logical if row["condition"]]
        self.assertEqual(80, len(conditions))
        self.assertEqual({"hit", "miss"}, {row["equals"] for row in conditions})

    def test_capability_boundary_rejects_synthetic_semantic_outcomes(self) -> None:
        output = self._compile("capabilities")
        plan = json.loads((output / PLAN_NAME).read_text(encoding="utf-8"))
        boundary = plan["execution_boundary"]
        self.assertFalse(boundary["synthetic_task_success_by_design_consumed"])
        self.assertFalse(boundary["semantic_outcome_values_included"])
        self.assertFalse(boundary["semantic_execution_performed"])
        self.assertFalse(boundary["semantic_quality_evaluated"])
        self.assertIn(
            "semantic-task-execution",
            boundary["unsupported_capabilities"],
        )
        self.assertNotIn(
            "task_success_by_design",
            _keys(_jsonl(output / TRIALS_NAME)),
        )

    def test_is_endpoint_free_hidden_label_free_and_checksum_bound(self) -> None:
        output = self._compile("safe")
        combined = b"".join(
            path.read_bytes()
            for path in output.iterdir()
            if path.name != CHECKSUMS_NAME
        ).decode("utf-8").lower()
        self.assertNotIn("http://", combined)
        self.assertNotIn("https://", combined)
        self.assertNotIn("correct_answer_id", combined)
        self.assertNotIn("api_key", combined)
        self.assertNotIn("bearer_token", combined)
        all_rows = (
            _jsonl(output / STAGES_NAME)
            + _jsonl(output / TRIALS_NAME)
            + [json.loads((output / PLAN_NAME).read_text(encoding="utf-8"))]
        )
        keys = _keys(all_rows)
        self.assertNotIn("task_success", keys)
        self.assertNotIn("score", keys)
        self.assertEqual(
            sorted(
                (
                    CHECKSUMS_NAME,
                    ARTIFACT_BINDINGS_NAME,
                    PLAN_NAME,
                    PUBLIC_TASKS_NAME,
                    SERVICE_CATALOG_NAME,
                    STAGES_NAME,
                    TRIALS_NAME,
                )
            ),
            sorted(path.name for path in output.iterdir()),
        )

    def test_output_is_deterministic_and_source_bound(self) -> None:
        first = self._compile("deterministic-a")
        second = self._compile("deterministic-b")
        self.assertEqual(
            {path.name: path.read_bytes() for path in first.iterdir()},
            {path.name: path.read_bytes() for path in second.iterdir()},
        )
        report = verify_full_flow_semantic_matrix(
            first,
            self.logical,
            SCENARIO,
            self.container,
            self.public_tasks,
            self.artifact_bindings,
        )
        self.assertEqual("VERIFIED", report["status"])
        self.assertEqual(64, report["trial_count"])
        self.assertFalse(report["semantic_execution_performed"])

    def test_missing_task_and_wrong_object_fail_closed(self) -> None:
        document = _public_task_document()
        document["tasks"].pop()
        missing = self.root / "missing-public.json"
        missing.write_bytes(_json_bytes(document))
        with self.assertRaisesRegex(
            FullFlowSemanticMatrixError,
            "missing one or more logical workloads",
        ):
            self._compile("missing-output", missing)

        document = _public_task_document()
        item = document["tasks"][0]
        replacement = build_n1_public_task_binding(
            workload_id=item["workload_id"],
            object_id="different-object",
            task_class_id=item["task_class_id"],
            question=item["question"],
            answer_options=item["answer_options"],
            success_scoring_rule=item["success_scoring_rule"],
        )
        document["tasks"][0] = replacement
        wrong = self.root / "wrong-object-public.json"
        wrong.write_bytes(_json_bytes(document))
        with self.assertRaisesRegex(
            FullFlowSemanticMatrixError,
            "artifact object does not match logical workload",
        ):
            self._compile("wrong-object-output", wrong)

    def test_hidden_label_and_runtime_address_fail_closed(self) -> None:
        document = _public_task_document()
        document["tasks"][0]["correct_answer_id"] = "A"
        hidden = self.root / "hidden-public.json"
        hidden.write_bytes(_json_bytes(document))
        with self.assertRaisesRegex(
            FullFlowSemanticMatrixError,
            "hidden oracle field entered public payload",
        ):
            self._compile("hidden-output", hidden)

        document = _public_task_document()
        document["tasks"][0]["question"] = "https://private.invalid/task"
        item = document["tasks"][0]
        document["tasks"][0] = build_n1_public_task_binding(
            workload_id=item["workload_id"],
            object_id=item["object_id"],
            task_class_id=item["task_class_id"],
            question=item["question"],
            answer_options=item["answer_options"],
            success_scoring_rule=item["success_scoring_rule"],
        )
        addressed = self.root / "address-public.json"
        addressed.write_bytes(_json_bytes(document))
        with self.assertRaisesRegex(
            FullFlowSemanticMatrixError,
            "runtime address",
        ):
            self._compile("address-output", addressed)

    def test_missing_or_inexact_representation_binding_fails_closed(self) -> None:
        document = _artifact_binding_document()
        document["objects"][0]["representations"].pop()
        missing = self.root / "missing-representation.json"
        missing.write_bytes(_json_bytes(document))
        with self.assertRaisesRegex(
            FullFlowSemanticMatrixError,
            "representations do not match logical routes",
        ):
            self._compile("missing-representation-output", artifact_bindings=missing)

        document = _artifact_binding_document()
        document["objects"][0]["representations"][0][
            "artifact_sha256"
        ] = "not-a-digest"
        invalid = self.root / "invalid-representation.json"
        invalid.write_bytes(_json_bytes(document))
        with self.assertRaisesRegex(
            FullFlowSemanticMatrixError,
            "artifact_sha256 is invalid",
        ):
            self._compile("invalid-representation-output", artifact_bindings=invalid)

    def test_checksum_and_restamped_action_tampering_fail_closed(self) -> None:
        output = self._compile("tamper")
        with (output / TRIALS_NAME).open("ab") as handle:
            handle.write(b" ")
        with self.assertRaisesRegex(
            FullFlowSemanticMatrixError,
            "checksum mismatch",
        ):
            verify_full_flow_semantic_matrix(
                output,
                self.logical,
                SCENARIO,
                self.container,
                self.public_tasks,
                self.artifact_bindings,
            )

        output = self._compile("action-tamper")
        stages = _jsonl(output / STAGES_NAME)
        stages[0]["logical_stage"]["action"] = "unregistered-action"
        stages[0]["source_logical_stage_sha256"] = _sha256(
            _canonical(stages[0]["logical_stage"])
        )
        (output / STAGES_NAME).write_text(
            "".join(
                json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n"
                for row in stages
            ),
            encoding="utf-8",
        )
        _restamp(output)
        with self.assertRaisesRegex(
            FullFlowSemanticMatrixError,
            "unsupported service action",
        ):
            verify_full_flow_semantic_matrix(
                output,
                self.logical,
                SCENARIO,
                self.container,
                self.public_tasks,
                self.artifact_bindings,
            )


if __name__ == "__main__":
    unittest.main()
