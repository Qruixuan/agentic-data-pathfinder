from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from pathfinder.simulator.container_contract import plan_container_backend
from pathfinder.simulator.full_flow_deployment import (
    DEPLOYMENT_SOURCE_SCHEMA_VERSION,
    build_full_flow_deployment_binding,
)
from pathfinder.simulator.full_flow_logical_routes import (
    compile_full_flow_logical_routes,
)
from pathfinder.simulator.full_flow_semantic_execution_admission import (
    ADMISSION_NAME,
    CHECKSUMS_NAME,
    GAPS_NAME,
    SMOKES_NAME,
    STAGES_NAME,
    TRIALS_NAME,
    FullFlowSemanticExecutionAdmissionError,
    freeze_full_flow_semantic_execution_admission,
    verify_full_flow_semantic_execution_admission,
)
from pathfinder.simulator.full_flow_semantic_matrix import (
    ARTIFACT_BINDING_SET_SCHEMA_VERSION,
    compile_full_flow_semantic_matrix,
)
from pathfinder.simulator.hidden_oracle import (
    MULTIPLE_CHOICE_EXACT_SCORING_RULE,
    N1_LABEL_SOURCE_SCHEMA_VERSION,
    build_n1_hidden_label_record,
    build_n1_oracle_package,
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
_PERSISTENT = {
    "immutable-hidden-oracle",
    "durable-trial-identity",
    "immutable-content-addressed-artifacts",
    "frozen-index-snapshot",
    "idempotent-content-addressed-output",
    "persistent-with-explicit-cache-scope",
}


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _write_json(path: Path, value: object) -> Path:
    path.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    return path


def _read_jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _artifact_object_id(logical_object_id: str) -> str:
    return f"real-{logical_object_id}-20260915"


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
        "task_plane_id": "semantic-execution-public-tasks-v1",
        "tasks": tasks,
        "label_values_included": False,
        "credentials_recorded": False,
    }


def _artifact_binding_document() -> dict:
    used = {
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
    for logical_id in sorted(used):
        representations = []
        for representation_id, size in (
            ("multimodal_digest", 32000),
            ("raw_video", 1234567),
            ("sampled_frame_bundle", 456789),
        ):
            if representation_id in used[logical_id]:
                representations.append({
                    "representation_id": representation_id,
                    "artifact_sha256": _sha256(
                        f"{logical_id}|{representation_id}".encode("utf-8")
                    ),
                    "artifact_size_bytes": size,
                    "object_catalog_version": "real-artifact-catalog-v1",
                })
        objects.append({
            "logical_object_id": logical_id,
            "artifact_object_id": _artifact_object_id(logical_id),
            "representations": representations,
        })
    return {
        "schema_version": ARTIFACT_BINDING_SET_SCHEMA_VERSION,
        "binding_set_id": "real-artifact-bindings-v1",
        "objects": objects,
        "credentials_recorded": False,
    }


class FullFlowSemanticExecutionAdmissionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temporary.name)
        cls.portable = cls.root / "portable"
        cls.container = cls.root / "container"
        cls.logical = cls.root / "logical"
        cls.semantic = cls.root / "semantic"
        cls.deployment = cls.root / "deployment"
        cls.oracle = cls.root / "oracle"
        cls.public_tasks = cls.root / "public-tasks.json"
        cls.artifacts = cls.root / "artifacts.json"
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
        public = _public_task_document()
        _write_json(cls.public_tasks, public)
        _write_json(cls.artifacts, _artifact_binding_document())
        compile_full_flow_semantic_matrix(
            cls.logical,
            SCENARIO,
            cls.container,
            cls.public_tasks,
            cls.artifacts,
            output_dir=cls.semantic,
        )
        catalog = json.loads(
            (cls.logical / "logical-service-contracts.json").read_text(
                encoding="utf-8"
            )
        )
        source = cls._deployment_source(catalog)
        source_path = _write_json(cls.root / "deployment-source.json", source)
        build_full_flow_deployment_binding(
            cls.logical,
            SCENARIO,
            cls.container,
            source_path,
            output_dir=cls.deployment,
        )
        labels = [
            build_n1_hidden_label_record(task, correct_answer_id="B")
            for task in public["tasks"]
        ]
        labels.sort(
            key=lambda row: (row["object_id"], row["task_binding_sha256"])
        )
        label_source = {
            "schema_version": N1_LABEL_SOURCE_SCHEMA_VERSION,
            "logical_node_id": "N1",
            "oracle_id": "semantic-execution-oracle-v1",
            "labels": labels,
            "credentials_recorded": False,
        }
        label_path = _write_json(cls.root / "hidden-labels.source.json", label_source)
        build_n1_oracle_package(label_path, output_dir=cls.oracle)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()

    @staticmethod
    def _deployment_source(catalog: dict) -> dict:
        bindings = []
        for contract in catalog["service_contracts"]:
            contract_id = contract["service_contract_id"]
            network = contract["role"] == "logical-byte-transfer"
            nodes = sorted(contract["logical_node_ids"])
            if network:
                credentials = []
            elif contract_id == "N1.hidden-score":
                credentials = [
                    "PATHFINDER_N1_ORACLE_EVIDENCE_SECRET",
                    "PATHFINDER_N1_ORACLE_TOKEN",
                ]
            elif contract_id == "N6.semantic-inference":
                credentials = ["UTU_LLM_API_KEY"]
            else:
                credentials = ["PATHFINDER_SERVICE_TOKEN"]
            bindings.append({
                "service_contract_id": contract_id,
                "adapter_id": "test-service-adapter-v1",
                "logical_node_ids": nodes,
                "actions": sorted(contract["actions"]),
                "representation_ids": [
                    "multimodal_digest",
                    "raw_video",
                    "sampled_frame_bundle",
                ],
                "base_url": (
                    None
                    if network
                    else f"http://127.0.0.1:{19000 + int(nodes[0][1:])}"
                ),
                "credential_env_names": credentials,
                "persistent_state": (
                    contract["state_semantics"] in _PERSISTENT
                ),
            })
        return {
            "schema_version": DEPLOYMENT_SOURCE_SCHEMA_VERSION,
            "deployment_id": "local-eight-node-semantic-v1",
            "backend": "single-host-compose",
            "service_bindings": bindings,
            "network_binding": {
                "adapter_id": "application-rate-rtt-shaper-v1",
                "mode": "application-shaped-single-host",
                "measurement_class": "configured-shaping-conformance",
                "parameters_fitted": False,
            },
            "trusted_private_http_hosts": [],
            "credentials_recorded": False,
        }

    def _freeze(self, name: str, **overrides: object) -> Path:
        output = self.root / name
        if output.exists():
            shutil.rmtree(output)
        arguments = {
            "semantic_matrix_dir": self.semantic,
            "deployment_binding_dir": self.deployment,
            "logical_route_dir": self.logical,
            "scenario_path": SCENARIO,
            "container_plan_dir": self.container,
            "public_task_set_path": self.public_tasks,
            "artifact_binding_path": self.artifacts,
            "n1_oracle_package_dir": self.oracle,
            "worker_alias": "pathfinder-semantic-worker",
            "output_dir": output,
            "admission_id": "semantic-execution-admission-test-v1",
        }
        arguments.update(overrides)
        freeze_full_flow_semantic_execution_admission(**arguments)
        return output

    def _verify(self, output: Path) -> dict:
        return verify_full_flow_semantic_execution_admission(
            output,
            self.semantic,
            self.deployment,
            self.logical,
            SCENARIO,
            self.container,
            self.public_tasks,
            self.artifacts,
            self.oracle,
        )

    def test_freezes_all_trials_as_blocked_not_submittable(self) -> None:
        output = self._freeze("complete")
        report = self._verify(output)
        self.assertEqual("VERIFIED_BLOCKED", report["status"])
        self.assertEqual(64, report["trial_count"])
        self.assertEqual(10, report["representative_smoke_count"])
        admission = json.loads((output / ADMISSION_NAME).read_text())
        self.assertEqual(
            "BLOCKED_MISSING_ROUTE_RUNTIME_ADAPTERS",
            admission["status"],
        )
        self.assertFalse(
            admission["execution_admission"][
                "flowmesh_submission_authorized"
            ]
        )
        self.assertFalse(
            admission["execution_admission"][
                "flowmesh_workflow_templates_included"
            ]
        )

    def test_preserves_artifacts_tasks_dags_conditions_and_deployment(self) -> None:
        output = self._freeze("bindings")
        source_trials = _read_jsonl(
            self.semantic / "semantic-matrix-trials.jsonl"
        )
        bound_trials = _read_jsonl(output / TRIALS_NAME)
        source = source_trials[0]
        bound = bound_trials[0]
        self.assertEqual(
            source["public_task_binding_sha256"],
            bound["public_task_binding_sha256"],
        )
        self.assertEqual(
            bound["public_task_binding_sha256"],
            bound["public_task_binding"]["task_binding_sha256"],
        )
        self.assertEqual(
            source["representation_identities"],
            bound["representation_identities"],
        )
        self.assertEqual(
            source["semantic_stage_keys"],
            bound["semantic_stage_keys"],
        )
        self.assertEqual(
            "N7.execution-compute",
            bound["route_coordinator_binding"]["service_contract_id"],
        )
        self.assertEqual(
            "http://127.0.0.1:19007",
            bound["route_coordinator_binding"]["base_url"],
        )
        stages = _read_jsonl(output / STAGES_NAME)
        conditional = [row for row in stages if row["condition"] is not None]
        self.assertGreater(len(conditional), 0)
        self.assertEqual({"hit", "miss"}, {
            row["condition"]["equals"] for row in conditional
        })
        service = next(
            row for row in stages if row["service_base_url"] is not None
        )
        self.assertTrue(service["service_base_url"].startswith("http://"))
        self.assertEqual(
            "route-coordinator-required",
            service["stage_result_handoff_mode"],
        )

    def test_representative_smokes_cover_families_and_cache_order(
        self,
    ) -> None:
        output = self._freeze("smokes")
        rows = {row["case_id"]: row for row in _read_jsonl(output / SMOKES_NAME)}
        self.assertEqual(10, len(rows))
        for node in ("n7", "n8"):
            expected_executor = node.upper()
            self.assertEqual("raw", rows[f"{node}-raw"]["route_family"])
            self.assertEqual(
                "indexed-raw",
                rows[f"{node}-indexed-raw"]["route_family"],
            )
            self.assertEqual(
                "remote-derived",
                rows[f"{node}-remote-derived"]["route_family"],
            )
            miss = rows[f"{node}-cache-miss"]
            hit = rows[f"{node}-cache-hit"]
            self.assertEqual("miss", miss["expected_cache_branch"])
            self.assertEqual("hit", hit["expected_cache_branch"])
            self.assertEqual(
                "empty-cache-scope-for-representation",
                miss["cache_precondition"],
            )
            self.assertEqual(
                "prerequisite-insert-same-runtime-epoch",
                hit["cache_precondition"],
            )
            self.assertEqual(miss["trial_key"], hit["prerequisite_trial_key"])
            self.assertTrue(all(
                rows[f"{node}-{case}"]["expected_executor_node_id"]
                == expected_executor
                for case in (
                    "raw",
                    "indexed-raw",
                    "remote-derived",
                    "cache-miss",
                    "cache-hit",
                )
            ))
        self.assertTrue(all(
            row["flowmesh_submission_authorized"] is False
            for row in rows.values()
        ))

    def test_runtime_gaps_are_exact_and_non_upcloud(self) -> None:
        output = self._freeze("gaps")
        document = json.loads((output / GAPS_NAME).read_text())
        gaps = {row["adapter_id"]: row for row in document["required_adapters"]}
        for required in (
            "raw-route-coordinator-v1",
            "indexed-raw-route-coordinator-v1",
            "remote-derived-route-coordinator-v1",
            "conditional-cache-derived-route-coordinator-v1",
            "cache-state-lifecycle-attestation-v1",
            "n1-authenticated-score-handoff-v2",
            "semantic-artifact-availability-preflight-v1",
        ):
            self.assertIn(required, gaps)
        self.assertTrue(all(row["affected_trial_count"] > 0 for row in gaps.values()))
        self.assertTrue(all(row["requires_upcloud"] is False for row in gaps.values()))
        self.assertFalse(document["all_required_adapters_implemented"])

    def test_n1_authentication_is_runtime_only_and_values_do_not_leak(self) -> None:
        output = self._freeze("authentication")
        text = "\n".join(
            path.read_text(encoding="utf-8")
            for path in output.iterdir()
            if path.name != CHECKSUMS_NAME
        )
        self.assertIn("PATHFINDER_N1_ORACLE_EVIDENCE_SECRET", text)
        self.assertIn("PATHFINDER_N1_ORACLE_TOKEN", text)
        self.assertNotIn("actual-oracle-secret-value", text)
        admission = json.loads((output / ADMISSION_NAME).read_text())
        authentication = admission["hidden_score_authentication"]
        self.assertTrue(authentication["hmac_verification_required"])
        self.assertTrue(
            authentication["one_request_per_run_trial_identity_required"]
        )
        self.assertFalse(authentication["hidden_label_content_included"])

    def test_two_freezes_are_byte_identical(self) -> None:
        first = self._freeze("deterministic-a")
        second = self._freeze("deterministic-b")
        self.assertEqual(
            {path.name: path.read_bytes() for path in first.iterdir()},
            {path.name: path.read_bytes() for path in second.iterdir()},
        )

    def test_restamped_trial_tampering_fails_source_recompilation(self) -> None:
        output = self._freeze("tampered")
        rows = _read_jsonl(output / TRIALS_NAME)
        rows[0]["worker_alias"] = "attacker-worker"
        (output / TRIALS_NAME).write_bytes(
            "".join(
                json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n"
                for row in rows
            ).encode("utf-8"),
        )
        admission = json.loads((output / ADMISSION_NAME).read_text())
        admission["output_sha256"][TRIALS_NAME] = _sha256(
            (output / TRIALS_NAME).read_bytes()
        )
        admission.pop("admission_sha256")
        admission["admission_sha256"] = _sha256(_canonical(admission))
        (output / ADMISSION_NAME).write_bytes(
            (
                json.dumps(
                    admission,
                    ensure_ascii=False,
                    sort_keys=True,
                    indent=2,
                )
                + "\n"
            ).encode("utf-8")
        )
        (output / CHECKSUMS_NAME).write_bytes(
            "".join(
                f"{_sha256((output / name).read_bytes())}  {name}\n"
                for name in sorted({
                    ADMISSION_NAME,
                    TRIALS_NAME,
                    STAGES_NAME,
                    GAPS_NAME,
                    SMOKES_NAME,
                })
            ).encode("utf-8"),
        )
        with self.assertRaisesRegex(
            FullFlowSemanticExecutionAdmissionError,
            "does not match inputs",
        ):
            self._verify(output)

    def test_missing_n1_authentication_env_names_fails_closed(self) -> None:
        catalog = json.loads(
            (self.logical / "logical-service-contracts.json").read_text()
        )
        source = self._deployment_source(catalog)
        for row in source["service_bindings"]:
            if row["service_contract_id"] == "N1.hidden-score":
                row["credential_env_names"] = ["PATHFINDER_SERVICE_TOKEN"]
        source_path = _write_json(self.root / "weak-auth-source.json", source)
        weak_binding = self.root / "weak-auth-binding"
        if weak_binding.exists():
            shutil.rmtree(weak_binding)
        build_full_flow_deployment_binding(
            self.logical,
            SCENARIO,
            self.container,
            source_path,
            output_dir=weak_binding,
        )
        with self.assertRaisesRegex(
            FullFlowSemanticExecutionAdmissionError,
            "lacks runtime authentication env names",
        ):
            self._freeze(
                "weak-auth-admission",
                deployment_binding_dir=weak_binding,
            )

    def test_different_oracle_public_binding_fails_closed(self) -> None:
        public = _public_task_document()
        labels = [
            build_n1_hidden_label_record(public["tasks"][0], correct_answer_id="A")
        ]
        source = {
            "schema_version": N1_LABEL_SOURCE_SCHEMA_VERSION,
            "logical_node_id": "N1",
            "oracle_id": "wrong-oracle-v1",
            "labels": labels,
            "credentials_recorded": False,
        }
        source_path = _write_json(self.root / "wrong-label-source.json", source)
        wrong = self.root / "wrong-oracle"
        if wrong.exists():
            shutil.rmtree(wrong)
        build_n1_oracle_package(source_path, output_dir=wrong)
        with self.assertRaisesRegex(
            FullFlowSemanticExecutionAdmissionError,
            "does not bind the semantic public task set",
        ):
            self._freeze(
                "wrong-oracle-admission",
                n1_oracle_package_dir=wrong,
            )

    def test_existing_output_is_not_overwritten(self) -> None:
        output = self._freeze("immutable-output")
        before = {path.name: path.read_bytes() for path in output.iterdir()}
        with self.assertRaisesRegex(
            FullFlowSemanticExecutionAdmissionError,
            "already exists",
        ):
            freeze_full_flow_semantic_execution_admission(
                self.semantic,
                self.deployment,
                self.logical,
                SCENARIO,
                self.container,
                self.public_tasks,
                self.artifacts,
                self.oracle,
                worker_alias="pathfinder-semantic-worker",
                output_dir=output,
            )
        self.assertEqual(
            before,
            {path.name: path.read_bytes() for path in output.iterdir()},
        )


if __name__ == "__main__":
    unittest.main()
