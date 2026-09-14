from __future__ import annotations

import json
import tempfile
import unittest
import uuid
from hashlib import sha256
from pathlib import Path
from unittest.mock import patch

from pathfinder.distributed.registry import build_endpoint_registry
from pathfinder.distributed.scoring import (
    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
    load_workload_scoring_contract,
    render_workload_question,
)
from pathfinder.integrations.flowmesh.pathfinder_evidence_bridge import (
    FlowMeshPathfinderEvidenceError,
    PATHFINDER_EVIDENCE_SPEC_SCHEMA_VERSION,
    build_flowmesh_pathfinder_evidence,
    verify_flowmesh_pathfinder_evidence,
)
from pathfinder.simulator.container_node import ContainerNodeRuntime
from pathfinder.simulator.data_agent_semantic_vertical import (
    CONTAINER_NODE_SEMANTIC_RESULT_SCHEMA_VERSION_V2,
    DATA_AGENT_SEMANTIC_MANIFEST_SCHEMA_VERSION,
    DATA_AGENT_SEMANTIC_RECORD_SCHEMA_VERSION,
    DATA_AGENT_SEMANTIC_SPEC_SCHEMA_VERSION,
    _semantic_request_id,
)


PLAN_SHA = "1" * 64
RUN_SHA = "2" * 64
ARTIFACT_SHA = "3" * 64
MANIFEST_SHA = "4" * 64
FINAL_ANSWER = "[B]"
ANSWER_SHA = sha256(FINAL_ANSWER.encode("utf-8")).hexdigest()
TRIAL_KEY = "matrix-v1|W1|D0|r0000"
SEMANTIC_RUN_ID = "semantic-run-001"


def _write_json(path: Path, value: object) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_jsonl(path: Path, values: list[dict[str, object]]) -> None:
    path.write_text(
        "".join(
            json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"
            for value in values
        ),
        encoding="utf-8",
    )


def _restamp_output(root: Path) -> None:
    names = (
        "pathfinder-evidence-manifest.json",
        "pathfinder-evidence-records.jsonl",
    )
    (root / "SHA256SUMS").write_text(
        "".join(
            f"{sha256((root / name).read_bytes()).hexdigest()}  {name}\n"
            for name in names
        ),
        encoding="utf-8",
    )


class PathfinderEvidenceBridgeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.plan = self.root / "matrix-plan"
        self.run = self.root / "matrix-run"
        self.semantic = self.root / "semantic"
        self.output = self.root / "evidence"
        self.binding_spec = self.root / "binding-spec.json"
        self.semantic_spec = self.root / "semantic-spec.json"
        self.registry = self._registry()
        self.plan.mkdir()
        self.run.mkdir()
        self.semantic.mkdir()
        self._write_sources()

    @staticmethod
    def _registry():
        return build_endpoint_registry(
            {
                "schema_version": (
                    "pathfinder.data-agent-endpoint-registry/v1alpha1"
                ),
                "registry_id": "registry-v1",
                "execution_node_id": "pathfinder-execution-luyao3",
                "endpoints": [
                    {
                        "endpoint_id": "real-origin-endpoint",
                        "node_id": "real-data-agent-node",
                        "location": "real-origin",
                        "base_url_env": "TEST_DATA_AGENT_URL",
                        "token_env": "TEST_DATA_AGENT_TOKEN",
                        "timeout_seconds": 5.0,
                        "max_retries": 0,
                        "telemetry_capabilities": [
                            "access_telemetry",
                            "transfer_bytes",
                            "quiescence_wait",
                        ],
                        "network_transport": "remote",
                    }
                ],
                "placement": [
                    {
                        "design_id": "D_origin_remote",
                        "representation_id": "sampled_frame_bundle",
                        "endpoint_id": "real-origin-endpoint",
                    }
                ],
            },
            source_sha256="7" * 64,
        )

    def _write_sources(self) -> None:
        _write_json(
            self.plan / "flowmesh-container-matrix-plan.json",
            {"matrix_id": "matrix-v1", "plan_sha256": PLAN_SHA},
        )
        _write_jsonl(
            self.plan / "flowmesh-container-matrix-trials.jsonl",
            [
                {
                    "trial_key": TRIAL_KEY,
                    "trial_id": "trial-001",
                    "workload_id": "visible-workload-1",
                    "workload_class": "W1",
                    "design_id": "D0",
                    "repetition": 0,
                    "object_id": "synthetic-video-1",
                    "executor_node_id": "N7",
                }
            ],
        )
        _write_jsonl(
            self.plan / "flowmesh-container-matrix-operations.jsonl",
            [
                {
                    "trial_key": TRIAL_KEY,
                    "operation_key": f"{TRIAL_KEY}|read",
                    "operation_kind": "storage_read",
                    "object_id": "synthetic-video-1",
                    "representation_id": "synthetic-raw",
                    "execution_node_id": "N3",
                },
                {
                    "trial_key": TRIAL_KEY,
                    "operation_key": f"{TRIAL_KEY}|infer",
                    "operation_kind": "compute",
                    "object_id": "synthetic-video-1",
                    "representation_id": "synthetic-raw",
                    "execution_node_id": "N6",
                },
            ],
        )
        _write_json(
            self.run / "flowmesh-container-matrix-run.json",
            {
                "run_id": "matrix-run-v1",
                "run_sha256": RUN_SHA,
                "matrix_plan_sha256": PLAN_SHA,
            },
        )
        telemetry = {
            "logical_bytes_sum": 2048,
            "physical_bytes_sum": 2048,
            "service_time_ms_sum": 12.5,
            "service_time_ms_sum_by_operation_kind": {
                "compute": 2.5,
                "storage_read": 10.0,
            },
        }
        _write_jsonl(
            self.run / "flowmesh-container-matrix-trial-results.jsonl",
            [
                {
                    "status": "COMPLETE",
                    "sequence_index": 0,
                    "trial_key": TRIAL_KEY,
                    "trial_id": "trial-001",
                    "workload_id": "visible-workload-1",
                    "workload_class": "W1",
                    "design_id": "D0",
                    "repetition": 0,
                    "executor_node_id": "N7",
                    "telemetry": telemetry,
                    "service_time_sum_is_end_to_end_latency": False,
                    "queue_time_measured": False,
                    "semantic_task_quality_evaluated": False,
                    "credentials_recorded": False,
                }
            ],
        )
        _write_jsonl(
            self.run / "flowmesh-container-matrix-operation-results.jsonl",
            [
                {
                    "trial_key": TRIAL_KEY,
                    "operation_key": f"{TRIAL_KEY}|read",
                    "executed": True,
                    "telemetry_complete": True,
                    "semantic_task_quality_evaluated": False,
                },
                {
                    "trial_key": TRIAL_KEY,
                    "operation_key": f"{TRIAL_KEY}|infer",
                    "executed": True,
                    "telemetry_complete": True,
                    "semantic_task_quality_evaluated": False,
                },
            ],
        )

        answer_options = [
            {"option_id": "A", "text": "A vehicle crosses a river."},
            {"option_id": "B", "text": "Two musicians perform."},
            {"option_id": "C", "text": "A person cooks food."},
        ]
        question = "Which option describes the main action?"
        semantic_spec = {
            "schema_version": DATA_AGENT_SEMANTIC_SPEC_SCHEMA_VERSION,
            "semantic_run_id": SEMANTIC_RUN_ID,
            "trial_key": TRIAL_KEY,
            "semantic_executor_node_id": "N6",
            "representation_id": "sampled_frame_bundle",
            "expected_model": "qwen/qwen3.8-27b",
            "data_agent_route_design_id": "D_origin_remote",
            "data_agent_plan_id": "D_origin_remote",
            "data_agent_plan_epoch": 0,
            "workload_id": "visible-workload-1",
            "task_class_id": "video_qa",
            "artifact_object_id": "nextqa-val-1",
            "artifact_sha256": ARTIFACT_SHA,
            "artifact_size_bytes": 4096,
            "object_catalog_version": "catalog-v1",
            "question": question,
            "success_scoring_rule": (
                MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE
            ),
            "answer_options": answer_options,
            "correct_answer_id": "B",
            "credentials_recorded": False,
        }
        _write_json(self.semantic_spec, semantic_spec)
        self.semantic_spec_sha256 = sha256(
            self.semantic_spec.read_bytes()
        ).hexdigest()
        scoring_contract = load_workload_scoring_contract(
            {
                "object_id": "nextqa-val-1",
                "question": question,
                "answer_options": answer_options,
                "correct_answer_id": "B",
            },
            MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
        )
        rendered_question = render_workload_question(
            semantic_spec,
            scoring_contract,
        )
        semantic_prompt = ContainerNodeRuntime.build_semantic_vision_prompt(
            "sampled_frame_bundle",
            1,
            rendered_question,
        )
        access_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                (
                    "pathfinder-data-agent-semantic-access:"
                    f"{SEMANTIC_RUN_ID}:{TRIAL_KEY}:nextqa-val-1:"
                    "sampled_frame_bundle:0"
                ),
            )
        )

        artifact = {
            "access_id": access_id,
            "media_type": "application/x-tar",
            "size_bytes": 4096,
            "sha256": ARTIFACT_SHA,
            "object_id": "nextqa-val-1",
            "object_catalog_version": "catalog-v1",
            "location": "real-origin",
        }
        frame_bundle = {
            "representation_id": "sampled_frame_bundle",
            "representation_sha256": ARTIFACT_SHA,
            "artifact_media_type": "application/x-tar",
            "artifact_size_bytes": 4096,
            "artifact_sha256": ARTIFACT_SHA,
            "manifest_sha256": MANIFEST_SHA,
            "frame_count": 1,
            "total_jpeg_bytes": 100,
            "frame_sequence_sha256": "5" * 64,
            "frames": [
                {
                    "frame_index": 0,
                    "timestamp_seconds": 0.0,
                    "width": 16,
                    "height": 16,
                    "jpeg_size_bytes": 100,
                    "jpeg_sha256": "6" * 64,
                    "media_type": "image/jpeg",
                }
            ],
        }
        delivery = {
            "telemetry_supported": True,
            "telemetry_complete": True,
            "in_flight_request_count": 0,
            "download_request_count": 1,
            "completed_request_count": 1,
            "full_download_count": 1,
            "bytes_sent": 4096,
            "artifact_size_bytes": 4096,
            "server_reported_transfer_latency_ms": 4.0,
            "exactly_one_full_download": True,
            "bytes_sent_equals_artifact_size": True,
            "telemetry_object_id": "nextqa-val-1",
            "telemetry_object_catalog_version": "catalog-v1",
            "expected_object_catalog_version": "catalog-v1",
        }
        record = {
            "schema_version": DATA_AGENT_SEMANTIC_RECORD_SCHEMA_VERSION,
            "status": "COMPLETE",
            "semantic_run_id": SEMANTIC_RUN_ID,
            "semantic_request_id": _semantic_request_id(
                semantic_run_id=SEMANTIC_RUN_ID,
                trial_key=TRIAL_KEY,
                execution_node_id="N6",
                representation_id="sampled_frame_bundle",
                representation_sha256=ARTIFACT_SHA,
                question_sha256=sha256(
                    rendered_question.encode("utf-8")
                ).hexdigest(),
                prompt_sha256=sha256(
                    semantic_prompt.encode("utf-8")
                ).hexdigest(),
                frame_sequence_sha256="5" * 64,
            ),
            "event_index": 0,
            "matrix_id": "matrix-v1",
            "matrix_plan_sha256": PLAN_SHA,
            "trial_key": TRIAL_KEY,
            "trial_id": "trial-001",
            "workload_id": "visible-workload-1",
            "workload_class": "W1",
            "matrix_design_id": "D0",
            "repetition": 0,
            "matrix_object_id": "synthetic-video-1",
            "artifact_object_id": "nextqa-val-1",
            "matrix_executor_node_id": "N7",
            "semantic_executor_node_id": "N6",
            "representation_id": "sampled_frame_bundle",
            "endpoint_registry_id": "registry-v1",
            "endpoint_registry_sha256": "7" * 64,
            "semantic_spec_sha256": self.semantic_spec_sha256,
            "route": self.registry.route(
                design_id="D_origin_remote",
                representation_id="sampled_frame_bundle",
            ).to_public_dict(),
            "data_agent_route_design_id": "D_origin_remote",
            "data_agent_plan_id": "D_origin_remote",
            "data_agent_plan_epoch": 0,
            "data_agent_access_id": access_id,
            "artifact": artifact,
            "frame_bundle": frame_bundle,
            "delivery": delivery,
            "latency_ms": {
                "data_agent_service": 3.0,
                "client_access_round_trip": 5.0,
                "artifact_download_elapsed": 2.0,
                "server_reported_transfer": 4.0,
            },
            "question_sha256": sha256(
                rendered_question.encode("utf-8")
            ).hexdigest(),
            "prompt_sha256": sha256(
                semantic_prompt.encode("utf-8")
            ).hexdigest(),
            "frame_sequence_sha256": "5" * 64,
            "container_request_sha256": "8" * 64,
            "container_result_schema_version": (
                CONTAINER_NODE_SEMANTIC_RESULT_SCHEMA_VERSION_V2
            ),
            "semantic_input_kind": "ordered-jpeg-frames",
            "representation_delivery_bytes": 100,
            "success_scoring_rule": (
                MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE
            ),
            "answer_option_ids": ["A", "B", "C"],
            "correct_answer_id": "B",
            "final_answer": FINAL_ANSWER,
            "final_answer_sha256": ANSWER_SHA,
            "task_success": True,
            "model": "qwen/qwen3.8-27b",
            "expected_model": "qwen/qwen3.8-27b",
            "semantic_service_time_ms": 20.0,
            "semantic_runtime_epoch": "9" * 32,
            "semantic_health_verified": True,
            "idempotent_replay": False,
            "container_data_plane_artifact_delivery_verified": False,
            "semantic_frame_payload_integrity_verified": True,
            "data_agent_artifact_delivery_verified": True,
            "container_semantic_response_consistency_verified": True,
            "container_runtime_code_provenance_verified": False,
            "scoring_verified": True,
            "execution_and_semantic_route_unified": False,
            "llm_called": True,
            "semantic_telemetry_complete": True,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }
        record_path = self.semantic / "data-agent-frame-bundle-semantic-record.json"
        _write_json(record_path, record)
        _write_json(
            self.semantic / "data-agent-frame-bundle-semantic-manifest.json",
            {
                "schema_version": DATA_AGENT_SEMANTIC_MANIFEST_SCHEMA_VERSION,
                "status": "COMPLETE",
                "semantic_run_id": SEMANTIC_RUN_ID,
                "event_index": 0,
                "matrix_id": "matrix-v1",
                "matrix_plan_sha256": PLAN_SHA,
                "matrix_plan_file_sha256": sha256(
                    (
                        self.plan / "flowmesh-container-matrix-plan.json"
                    ).read_bytes()
                ).hexdigest(),
                "trial_key": TRIAL_KEY,
                "endpoint_registry_id": "registry-v1",
                "endpoint_registry_sha256": "7" * 64,
                "semantic_spec_sha256": self.semantic_spec_sha256,
                "record_count": 1,
                "record_sha256": sha256(record_path.read_bytes()).hexdigest(),
                "artifact_size_bytes": 4096,
                "artifact_sha256": ARTIFACT_SHA,
                "frame_count": 1,
                "task_success_count": 1,
                "models": ["qwen/qwen3.8-27b"],
                "expected_model": "qwen/qwen3.8-27b",
                "semantic_runtime_epoch": "9" * 32,
                "semantic_health_verified": True,
                "data_agent_artifact_delivery_verified": True,
                "container_semantic_response_consistency_verified": True,
                "container_runtime_code_provenance_verified": False,
                "scoring_verified": True,
                "execution_and_semantic_route_unified": False,
                "llm_called": True,
                "credentials_recorded": False,
                "eligible_for_scientific_claims": False,
                "limitations": [
                    "The matrix object and semantic artifact are distinct.",
                    "The two routes are associated, not unified.",
                    "This is integration evidence, not a cost result.",
                ],
            },
        )
        semantic_names = (
            "data-agent-frame-bundle-semantic-manifest.json",
            "data-agent-frame-bundle-semantic-record.json",
        )
        (self.semantic / "SHA256SUMS").write_text(
            "".join(
                f"{sha256((self.semantic / name).read_bytes()).hexdigest()}  {name}\n"
                for name in semantic_names
            ),
            encoding="utf-8",
        )
        self._write_spec()

    def _write_spec(self, **binding_overrides: object) -> None:
        binding = {
            "semantic_run_id": SEMANTIC_RUN_ID,
            "semantic_spec_sha256": self.semantic_spec_sha256,
            "event_index": 0,
            "expected_model": "qwen/qwen3.8-27b",
            "trial_key": TRIAL_KEY,
            "workload_id": "visible-workload-1",
            "workload_class": "W1",
            "matrix_design_id": "D0",
            "data_agent_route_design_id": "D_origin_remote",
            "repetition": 0,
            "matrix_object_id": "synthetic-video-1",
            "artifact_object_id": "nextqa-val-1",
            "representation_id": "sampled_frame_bundle",
        }
        binding.update(binding_overrides)
        _write_json(
            self.binding_spec,
            {
                "schema_version": PATHFINDER_EVIDENCE_SPEC_SCHEMA_VERSION,
                "evidence_bundle_id": "pathfinder-bridge-test-v1",
                "matrix_id": "matrix-v1",
                "matrix_plan_sha256": PLAN_SHA,
                "matrix_run_id": "matrix-run-v1",
                "bindings": [binding],
                "execution_and_semantic_route_unified": False,
                "cost_basis": "unavailable",
                "credentials_recorded": False,
                "eligible_for_awm_oed": False,
                "eligible_for_scientific_claims": False,
            },
        )

    def _patch_verifiers(self):
        return (
            patch(
                "pathfinder.integrations.flowmesh.pathfinder_evidence_bridge."
                "verify_flowmesh_container_matrix_plan",
                return_value={"status": "VERIFIED"},
            ),
            patch(
                "pathfinder.integrations.flowmesh.pathfinder_evidence_bridge."
                "verify_flowmesh_container_matrix_run",
                return_value={"status": "VERIFIED"},
            ),
            patch(
                "pathfinder.integrations.flowmesh.pathfinder_evidence_bridge."
                "_verify_semantic_source",
                return_value={"status": "VERIFIED"},
            ),
        )

    def _build(self) -> dict[str, object]:
        verifiers = self._patch_verifiers()
        with verifiers[0], verifiers[1], verifiers[2]:
            return build_flowmesh_pathfinder_evidence(
                binding_spec=self.binding_spec,
                matrix_plan_dir=self.plan,
                matrix_run_dir=self.run,
                endpoint_registry=self.registry,
                data_agent_semantic_dirs=[self.semantic],
                data_agent_semantic_specs=[self.semantic_spec],
                output_dir=self.output,
            )

    def _verify(self) -> dict[str, object]:
        verifiers = self._patch_verifiers()
        with verifiers[0], verifiers[1], verifiers[2]:
            return verify_flowmesh_pathfinder_evidence(
                evidence_dir=self.output,
                binding_spec=self.binding_spec,
                matrix_plan_dir=self.plan,
                matrix_run_dir=self.run,
                endpoint_registry=self.registry,
                data_agent_semantic_dirs=[self.semantic],
                data_agent_semantic_specs=[self.semantic_spec],
            )

    def test_builds_and_offline_verifies_one_explicit_association(self) -> None:
        result = self._build()
        self.assertEqual("VERIFIED", result["status"])
        self.assertEqual(
            "cross-layer-posthoc-association-conformance",
            result["evidence_class"],
        )
        self.assertEqual(1, result["record_count"])
        self.assertEqual(
            {
                "SHA256SUMS",
                "pathfinder-evidence-manifest.json",
                "pathfinder-evidence-records.jsonl",
            },
            {path.name for path in self.output.iterdir()},
        )
        self.assertEqual("VERIFIED", self._verify()["status"])

    def test_calls_native_data_agent_semantic_verifier_with_frozen_spec(
        self,
    ) -> None:
        matrix_plan_result = {
            "status": "VERIFIED",
            "plan_sha256": PLAN_SHA,
        }
        with (
            patch(
                "pathfinder.integrations.flowmesh.pathfinder_evidence_bridge."
                "verify_flowmesh_container_matrix_plan",
                return_value=matrix_plan_result,
            ),
            patch(
                "pathfinder.integrations.flowmesh.pathfinder_evidence_bridge."
                "verify_flowmesh_container_matrix_run",
                return_value={"status": "VERIFIED"},
            ),
            patch(
                "pathfinder.simulator.data_agent_semantic_vertical."
                "verify_flowmesh_container_matrix_plan",
                return_value=matrix_plan_result,
            ),
        ):
            result = build_flowmesh_pathfinder_evidence(
                binding_spec=self.binding_spec,
                matrix_plan_dir=self.plan,
                matrix_run_dir=self.run,
                endpoint_registry=self.registry,
                data_agent_semantic_dirs=[self.semantic],
                data_agent_semantic_specs=[self.semantic_spec],
                output_dir=self.output,
            )

        self.assertEqual("VERIFIED", result["status"])
        record = json.loads(
            (self.output / "pathfinder-evidence-records.jsonl")
            .read_text(encoding="utf-8")
            .strip()
        )
        self.assertEqual("D0", record["matrix_design_id"])
        self.assertEqual(
            "D_origin_remote",
            record["data_agent_route_design_id"],
        )
        self.assertEqual(
            self.semantic_spec_sha256,
            record["association"]["semantic_spec_sha256"],
        )

    def test_keeps_matrix_and_artifact_objects_separate(self) -> None:
        self._build()
        record = json.loads(
            (self.output / "pathfinder-evidence-records.jsonl")
            .read_text(encoding="utf-8")
            .strip()
        )
        self.assertEqual("synthetic-video-1", record["matrix_object_id"])
        self.assertEqual("nextqa-val-1", record["artifact_object_id"])
        self.assertFalse(
            record["association"]["execution_and_semantic_route_unified"]
        )
        self.assertTrue(
            record["association"]["exact_trial_key_association_verified"]
        )
        self.assertFalse(
            record["association"]["flowmesh_semantic_execution_verified"]
        )
        self.assertEqual(
            "real-data-agent-node",
            record["data_agent"]["route"]["source_node_id"],
        )
        self.assertNotEqual("N3", record["data_agent"]["route"]["source_node_id"])
        self.assertEqual(
            "pathfinder-execution-luyao3",
            record["data_agent"]["destination_host_node_id"],
        )
        self.assertEqual("N6", record["semantic"]["container_node_id"])
        self.assertTrue(record["semantic"]["response_consistency_verified"])
        self.assertFalse(
            record["semantic"][
                "container_runtime_code_provenance_verified"
            ]
        )
        self.assertNotEqual(
            record["data_agent"]["destination_host_node_id"],
            record["semantic"]["container_node_id"],
        )
        self.assertFalse(
            record["association"][
                "host_to_container_ownership_mapping_verified"
            ]
        )
        manifest = json.loads(
            (self.output / "pathfinder-evidence-manifest.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertTrue(
            manifest["container_semantic_response_consistency_verified"]
        )
        self.assertFalse(
            manifest["container_runtime_code_provenance_verified"]
        )

    def test_emits_no_cost_or_awm_claim(self) -> None:
        self._build()
        record = json.loads(
            (self.output / "pathfinder-evidence-records.jsonl")
            .read_text(encoding="utf-8")
            .strip()
        )
        self.assertEqual("unavailable", record["cost"]["cost_basis"])
        self.assertIsNone(record["cost"]["total_cost"])
        self.assertFalse(record["cost"]["monetary_cost_measured"])
        self.assertFalse(record["eligible_for_awm_oed"])

    def test_does_not_copy_answer_or_binary_content(self) -> None:
        self._build()
        raw = (self.output / "pathfinder-evidence-records.jsonl").read_text(
            encoding="utf-8"
        )
        self.assertNotIn('"final_answer"', raw)
        self.assertNotIn("jpeg_base64", raw)
        self.assertNotIn("representation_path", raw)
        self.assertNotIn("api_key", raw.casefold())

    def test_binding_mismatch_fails_without_partial_output(self) -> None:
        self._write_spec(artifact_object_id="different-object")
        with self.assertRaisesRegex(
            FlowMeshPathfinderEvidenceError,
            "binding artifact_object_id differs",
        ):
            self._build()
        self.assertFalse(self.output.exists())

    def test_semantic_spec_digest_is_an_explicit_binding(self) -> None:
        self._write_spec(semantic_spec_sha256="a" * 64)
        with self.assertRaisesRegex(
            FlowMeshPathfinderEvidenceError,
            "binding semantic_spec_sha256 differs",
        ):
            self._build()
        self.assertFalse(self.output.exists())

    def test_semantic_directory_and_spec_counts_must_match(self) -> None:
        verifiers = self._patch_verifiers()
        with verifiers[0], verifiers[1], verifiers[2]:
            with self.assertRaisesRegex(
                FlowMeshPathfinderEvidenceError,
                "must have equal counts",
            ):
                build_flowmesh_pathfinder_evidence(
                    binding_spec=self.binding_spec,
                    matrix_plan_dir=self.plan,
                    matrix_run_dir=self.run,
                    endpoint_registry=self.registry,
                    data_agent_semantic_dirs=[self.semantic],
                    data_agent_semantic_specs=[],
                    output_dir=self.output,
                )

    def test_matrix_object_is_checked_against_plan_not_artifact(self) -> None:
        self._write_spec(matrix_object_id="different-synthetic-object")
        with self.assertRaises(FlowMeshPathfinderEvidenceError):
            self._build()
        self.assertFalse(self.output.exists())

    def test_incomplete_data_agent_delivery_is_refused(self) -> None:
        path = self.semantic / "data-agent-frame-bundle-semantic-record.json"
        record = json.loads(path.read_text(encoding="utf-8"))
        record["delivery"]["telemetry_complete"] = False
        _write_json(path, record)
        manifest_path = self.semantic / "data-agent-frame-bundle-semantic-manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["record_sha256"] = sha256(path.read_bytes()).hexdigest()
        _write_json(manifest_path, manifest)
        with self.assertRaisesRegex(
            FlowMeshPathfinderEvidenceError,
            "delivery telemetry_complete changed",
        ):
            self._build()

    def test_restamped_output_tamper_is_rederived_from_sources(self) -> None:
        self._build()
        path = self.output / "pathfinder-evidence-records.jsonl"
        record = json.loads(path.read_text(encoding="utf-8").strip())
        record["cost"]["total_cost"] = 1.0
        _write_jsonl(path, [record])
        _restamp_output(self.output)
        with self.assertRaisesRegex(
            FlowMeshPathfinderEvidenceError,
            "does not match verified sources",
        ):
            self._verify()

    def test_existing_output_is_never_overwritten(self) -> None:
        self.output.mkdir()
        marker = self.output / "owned-by-user.txt"
        marker.write_text("preserve", encoding="utf-8")
        with self.assertRaisesRegex(
            FlowMeshPathfinderEvidenceError,
            "already exists",
        ):
            self._build()
        self.assertEqual("preserve", marker.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
