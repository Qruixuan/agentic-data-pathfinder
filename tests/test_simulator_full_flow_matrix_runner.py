from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pathfinder.simulator.container_contract import plan_container_backend
from pathfinder.simulator.full_flow_deployment import (
    DEPLOYMENT_SOURCE_SCHEMA_VERSION,
    build_full_flow_deployment_binding,
)
from pathfinder.simulator.full_flow_logical_routes import (
    compile_full_flow_logical_routes,
)
from pathfinder.simulator.full_flow_matrix_runner import (
    CHECKPOINT_DIR_NAME,
    CHECKSUMS_NAME,
    EVIDENCE_NAME,
    JOURNAL_NAME,
    PUBLIC_ROUTE_EVIDENCE_SCHEMA_VERSION,
    REPORT_NAME,
    ROUTE_EVIDENCE_NAME,
    TRIAL_RESULT_SCHEMA_VERSION,
    FullFlowSemanticMatrixRunnerError,
    SemanticMatrixRunFailed,
    SemanticTrialExecutionError,
    load_full_flow_semantic_matrix_route_evidence,
    run_full_flow_semantic_matrix,
    validate_semantic_trial_result,
    verify_full_flow_semantic_matrix_run,
)
from pathfinder.simulator.full_flow_semantic_matrix import (
    ARTIFACT_BINDING_SET_SCHEMA_VERSION,
    TRIALS_NAME as SEMANTIC_TRIALS_NAME,
    compile_full_flow_semantic_matrix,
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
_PERSISTENT = {
    "immutable-hidden-oracle",
    "durable-trial-identity",
    "immutable-content-addressed-artifacts",
    "frozen-index-snapshot",
    "idempotent-content-addressed-output",
    "persistent-with-explicit-cache-scope",
}


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _write_json(path: Path, value: object) -> Path:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def _artifact_object_id(logical_object_id: str) -> str:
    return f"bound-{logical_object_id}-v1"


def _public_tasks() -> dict:
    return {
        "schema_version": "pathfinder.public-task-set/v1alpha1",
        "task_plane_id": "runner-public-tasks-v1",
        "tasks": [
            build_n1_public_task_binding(
                workload_id=workload_id,
                object_id=_artifact_object_id(object_id),
                task_class_id=task_class,
                question=f"Public question for {workload_id}?",
                answer_options=[
                    {"option_id": "A", "text": "First option."},
                    {"option_id": "B", "text": "Second option."},
                ],
                success_scoring_rule=MULTIPLE_CHOICE_EXACT_SCORING_RULE,
            )
            for workload_id, (object_id, task_class) in sorted(WORKLOADS.items())
        ],
        "label_values_included": False,
        "credentials_recorded": False,
    }


def _artifact_bindings() -> dict:
    needed = {
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
    for logical_object_id in sorted(needed):
        representations = []
        for representation_id, size in (
            ("multimodal_digest", 32000),
            ("raw_video", 1234567),
            ("sampled_frame_bundle", 456789),
        ):
            if representation_id in needed[logical_object_id]:
                representations.append({
                    "representation_id": representation_id,
                    "artifact_sha256": _sha256(
                        f"{logical_object_id}|{representation_id}".encode()
                    ),
                    "artifact_size_bytes": size,
                    "object_catalog_version": "runner-artifacts-v1",
                })
        objects.append({
            "logical_object_id": logical_object_id,
            "artifact_object_id": _artifact_object_id(logical_object_id),
            "representations": representations,
        })
    return {
        "schema_version": ARTIFACT_BINDING_SET_SCHEMA_VERSION,
        "binding_set_id": "runner-artifact-bindings-v1",
        "objects": objects,
        "credentials_recorded": False,
    }


def _public_route_evidence(trial: dict) -> dict:
    task_success = trial["order_index"] % 3 != 0
    score = 1.0 if task_success else 0.0
    executor_node_id = (
        "N7" if trial["design_id"] in {"D0", "D1", "D2", "D3"} else "N8"
    )
    score_request = {
        "schema_version": "pathfinder.n1-score-request/v1alpha2",
        "score_request_id": _sha256(
            f"score-request|{trial['trial_key']}".encode()
        ),
        "evaluation_unit_id": _sha256(
            f"evaluation-unit|{trial['trial_key']}".encode()
        ),
        "oracle_id": "runner-oracle-v1",
        "run_id": "runner-public-evidence-v1",
        "trial_id": trial["trial_key"],
        "object_id": trial["artifact_object_id"],
        "task_binding_sha256": trial["public_task_binding_sha256"],
        "predicted_answer": "A",
        "credentials_recorded": False,
    }
    score_result = {
        "schema_version": "pathfinder.n1-score-result/v1alpha2",
        "status": "SCORED",
        "score_request_id": score_request["score_request_id"],
        "evaluation_unit_id": score_request["evaluation_unit_id"],
        "oracle_id": score_request["oracle_id"],
        "node_id": "N1",
        "run_id": score_request["run_id"],
        "trial_id": trial["trial_key"],
        "object_id": trial["artifact_object_id"],
        "task_binding_sha256": trial["public_task_binding_sha256"],
        "request_sha256": _sha256(
            json.dumps(
                score_request,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
        ),
        "prediction_sha256": _sha256(b"A"),
        "success_scoring_rule": "multiple-choice-exact-option-id-v1",
        "correct": task_success,
        "score": score,
        "public_task_set_sha256": _sha256(b"runner-public-task-set"),
        "oracle_instance_hmac_sha256": _sha256(b"runner-oracle-hmac"),
        "score_evidence_hmac_sha256": _sha256(b"runner-score-hmac"),
        "idempotent_replay": False,
        "hidden_answer_returned": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    score_result["result_content_sha256"] = _sha256(
        json.dumps(
            score_result,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    )
    authentication_verification_sha256 = _sha256(
        json.dumps(
            {
                "domain": "pathfinder.authenticated-n1-score-verification/v1",
                "request_sha256": score_result["request_sha256"],
                "result_content_sha256": score_result[
                    "result_content_sha256"
                ],
                "score_evidence_hmac_sha256": score_result[
                    "score_evidence_hmac_sha256"
                ],
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    )
    evidence = {
        "schema_version": PUBLIC_ROUTE_EVIDENCE_SCHEMA_VERSION,
        "status": "COMPLETE",
        "execution_id": _sha256(f"execution|{trial['trial_key']}".encode()),
        "request_sha256": _sha256(f"request|{trial['trial_key']}".encode()),
        "run_id": score_request["run_id"],
        "trial_id": trial["trial_key"],
        "trial_key": trial["trial_key"],
        "trial_sha256": _sha256(
            json.dumps(trial, separators=(",", ":"), sort_keys=True).encode()
        ),
        "stage_dag_sha256": _sha256(b"runner-stage-dag"),
        "workload_id": trial["workload_id"],
        "workload_class": trial["workload_class"],
        "design_id": trial["design_id"],
        "repetition": trial["repetition"],
        "artifact_object_id": trial["artifact_object_id"],
        "public_task_binding_sha256": trial["public_task_binding_sha256"],
        "route": {
            "route_family": trial["route_family"],
            "executor_node_id": executor_node_id,
            "inference_node_id": "N6",
            "score_node_id": "N1",
        },
        "artifact_identities": [],
        "provisioning_references": [],
        "stage_results": [],
        "cache_branches": [],
        "model_input": {
            "mode": "digest",
            "payload_sha256": _sha256(b"runner-model-input"),
            "payload_size_bytes": 1,
            "component_identity_sha256": [],
            "preparation_sha256": _sha256(b"runner-preparation"),
        },
        "semantic": {
            "model": "runner-model",
            "input_sha256": _sha256(b"runner-model-input"),
            "request_sha256": _sha256(b"runner-semantic-request"),
            "result_sha256": _sha256(b"runner-semantic-result"),
            "final_answer_sha256": _sha256(b"A"),
            "service_time_ms": 1.0,
        },
        "scoring": {
            "oracle_id": score_result["oracle_id"],
            "score_request_id": score_result["score_request_id"],
            "evaluation_unit_id": score_result["evaluation_unit_id"],
            "task_binding_sha256": score_result["task_binding_sha256"],
            "task_success": task_success,
            "score": score,
            "score_evidence_hmac_sha256": score_result[
                "score_evidence_hmac_sha256"
            ],
            "result_content_sha256": score_result["result_content_sha256"],
            "authentication_verification_sha256": (
                authentication_verification_sha256
            ),
            "authenticated_n1_v1alpha2": True,
        },
        "n1_score_request": score_request,
        "n1_score_result": score_result,
        "neutral_observation_candidate": {
            "schema_version": (
                "pathfinder.simulator-neutral-observation-candidate/v1alpha1"
            ),
            "trial_key": trial["trial_key"],
            "order_index": trial["order_index"],
            "workload_id": trial["workload_id"],
            "workload_class": trial["workload_class"],
            "design_id": trial["design_id"],
            "repetition": trial["repetition"],
            "object_id": trial["artifact_object_id"],
            "route_family": trial["route_family"],
            "executor_node_id": executor_node_id,
            "task_success": task_success,
            "score": score,
            "score_authenticity_verified": True,
            "score_authentication": "n1-hmac-verified",
            "component_service_time_ms": {},
            "byte_measurements": {
                "adapter_bytes_read": 0,
                "adapter_bytes_sent": 0,
                "semantic_input_bytes": 1,
            },
            "monetary_measurement_available": False,
            "monetary_values_included": False,
            "synthetic_monetary_inputs_consumed": False,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        },
        "all_frozen_stages_accounted_for": True,
        "exclusive_cache_branches_verified": True,
        "n2_exact_range_required_for_indexed_raw": True,
        "n3_n4_artifact_identity_verified": True,
        "n5_provisioning_references_verified": True,
        "n6_input_mode_verified": True,
        "n1_exactly_once_authenticated_score_verified": True,
        "idempotent_replay": False,
        "endpoint_values_included": False,
        "credential_values_included": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    evidence["evidence_sha256"] = _sha256(
        json.dumps(evidence, separators=(",", ":"), sort_keys=True).encode()
    )
    return evidence


class RecordingExecutor:
    def __init__(
        self,
        failures: dict[int, list[Exception]] | None = None,
        *,
        invalid_at: int | None = None,
    ) -> None:
        self.failures = failures or {}
        self.invalid_at = invalid_at
        self.calls: list[tuple[int, str, str]] = []

    def execute(self, *, trial: dict, idempotency_key: str) -> dict:
        order = trial["order_index"]
        self.calls.append((order, trial["trial_key"], idempotency_key))
        failures = self.failures.get(order, [])
        if failures:
            raise failures.pop(0)
        result = {
            "schema_version": TRIAL_RESULT_SCHEMA_VERSION,
            "status": "COMPLETE",
            "trial_key": trial["trial_key"],
            "idempotency_key": idempotency_key,
            "task_success": order % 3 != 0,
            "semantic_answer_sha256": _sha256(
                f"prediction|{trial['trial_key']}".encode()
            ),
            "n1_score_evidence_sha256": _sha256(
                f"n1-score|{trial['trial_key']}".encode()
            ),
            "n1_score_authenticity_verified": True,
            "route_evidence_sha256": _sha256(
                f"route|{trial['trial_key']}".encode()
            ),
            "semantic_route_evidence": None,
            "artifact_binding_evidence_sha256": _sha256(
                f"artifact|{trial['trial_key']}".encode()
            ),
            "measurements": [
                {
                    "component_id": "N3",
                    "metric_id": "logical_bytes",
                    "value": 1024 + order,
                    "unit": "bytes",
                    "measurement_class": "measured",
                },
                {
                    "component_id": "N6",
                    "metric_id": "service_time",
                    "value": 1.5 + order,
                    "unit": "milliseconds",
                    "measurement_class": "measured",
                },
            ],
            "execution_transport": "test-double",
            "flowmesh_workflow_evidence_sha256": None,
            "llm_called": True,
            "telemetry_complete": True,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }
        if self.invalid_at == order:
            result["correct_answer_id"] = "A"
        return result


class FlowMeshRecordingExecutor(RecordingExecutor):
    def execute(self, *, trial: dict, idempotency_key: str) -> dict:
        result = super().execute(
            trial=trial,
            idempotency_key=idempotency_key,
        )
        evidence = _public_route_evidence(trial)
        result["route_evidence_sha256"] = evidence["evidence_sha256"]
        result["semantic_route_evidence"] = evidence
        result["execution_transport"] = "flowmesh"
        result["flowmesh_workflow_evidence_sha256"] = _sha256(
            f"flowmesh|{trial['trial_key']}".encode()
        )
        return result


class FullFlowSemanticMatrixRunnerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temporary.name)
        cls.portable = cls.root / "portable"
        cls.container = cls.root / "container"
        cls.logical = cls.root / "logical"
        cls.public_tasks = _write_json(
            cls.root / "public-tasks.json",
            _public_tasks(),
        )
        cls.artifact_bindings = _write_json(
            cls.root / "artifact-bindings.json",
            _artifact_bindings(),
        )
        cls.semantic = cls.root / "semantic"
        cls.deployment = cls.root / "deployment"
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
        compile_full_flow_semantic_matrix(
            cls.logical,
            SCENARIO,
            cls.container,
            cls.public_tasks,
            cls.artifact_bindings,
            output_dir=cls.semantic,
        )
        catalog = json.loads(
            (cls.logical / "logical-service-contracts.json").read_text()
        )
        bindings = []
        for contract in catalog["service_contracts"]:
            network = contract["role"] == "logical-byte-transfer"
            nodes = sorted(contract["logical_node_ids"])
            bindings.append({
                "service_contract_id": contract["service_contract_id"],
                "adapter_id": "runner-test-adapter-v1",
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
                "credential_env_names": (
                    [] if network else ["PATHFINDER_TEST_TOKEN"]
                ),
                "persistent_state": contract["state_semantics"] in _PERSISTENT,
            })
        source = _write_json(
            cls.root / "deployment-source.json",
            {
                "schema_version": DEPLOYMENT_SOURCE_SCHEMA_VERSION,
                "deployment_id": "runner-local-eight-node-v1",
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
            },
        )
        build_full_flow_deployment_binding(
            cls.logical,
            SCENARIO,
            cls.container,
            source,
            output_dir=cls.deployment,
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()

    def setUp(self) -> None:
        self.case = Path(tempfile.mkdtemp(dir=self.root))

    def tearDown(self) -> None:
        import shutil

        shutil.rmtree(self.case)

    def _arguments(self, output: Path) -> dict:
        return {
            "semantic_matrix_dir": self.semantic,
            "deployment_binding_dir": self.deployment,
            "logical_route_dir": self.logical,
            "scenario_path": SCENARIO,
            "container_plan_dir": self.container,
            "public_task_set_path": self.public_tasks,
            "artifact_binding_path": self.artifact_bindings,
            "run_id": "runner-test-v1",
            "output_dir": output,
        }

    def _verification_arguments(self, output: Path) -> dict:
        arguments = self._arguments(output)
        arguments.pop("run_id")
        return arguments

    def test_executes_all_64_in_frozen_order_and_freezes_evidence(self) -> None:
        output = self.case / "complete"
        executor = RecordingExecutor()
        result = run_full_flow_semantic_matrix(
            **self._arguments(output),
            executor=executor,
        )
        self.assertEqual("VERIFIED", result["status"])
        self.assertEqual(64, result["completed_trial_count"])
        self.assertEqual(list(range(64)), [call[0] for call in executor.calls])
        self.assertEqual(64, len({call[2] for call in executor.calls}))
        evidence = [
            json.loads(line)
            for line in (output / EVIDENCE_NAME).read_text().splitlines()
        ]
        self.assertEqual(64, len(evidence))
        self.assertEqual(list(range(64)), [row["order_index"] for row in evidence])
        self.assertEqual(64, sum(row["llm_called"] for row in evidence))
        self.assertTrue(all(row["n1_score_authenticity_verified"] for row in evidence))
        text = (output / EVIDENCE_NAME).read_text().casefold()
        self.assertNotIn("correct_answer", text)
        self.assertNotIn("http://", text)
        self.assertEqual(b"", (output / ROUTE_EVIDENCE_NAME).read_bytes())
        self.assertTrue((output / CHECKSUMS_NAME).is_file())
        self.assertEqual(
            64,
            len(list((output / CHECKPOINT_DIR_NAME).iterdir())),
        )

    def test_flowmesh_route_evidence_is_durable_and_strictly_loadable(
        self,
    ) -> None:
        output = self.case / "flowmesh-route-evidence"
        result = run_full_flow_semantic_matrix(
            **self._arguments(output),
            executor=FlowMeshRecordingExecutor(),
        )
        self.assertEqual(64, result["semantic_route_evidence_count"])
        self.assertTrue(
            result["semantic_route_evidence_complete_for_flowmesh_trials"]
        )
        loaded = load_full_flow_semantic_matrix_route_evidence(output)
        self.assertEqual("VERIFIED_PUBLIC_ROUTE_EVIDENCE", loaded["status"])
        self.assertEqual(64, loaded["route_evidence_count"])
        self.assertEqual(
            [
                json.loads(line)["trial_key"]
                for line in (
                    self.semantic / SEMANTIC_TRIALS_NAME
                ).read_text().splitlines()
            ],
            [
                row["trial_key"]
                for row in loaded["evidence_records"]
            ],
        )

    def test_route_evidence_loader_fails_closed_on_sidecar_tampering(
        self,
    ) -> None:
        output = self.case / "route-sidecar-tamper"
        run_full_flow_semantic_matrix(
            **self._arguments(output),
            executor=FlowMeshRecordingExecutor(),
        )
        with (output / ROUTE_EVIDENCE_NAME).open("ab") as handle:
            handle.write(b"\n")
        with self.assertRaisesRegex(
            FullFlowSemanticMatrixRunnerError,
            "checksum mismatch",
        ):
            load_full_flow_semantic_matrix_route_evidence(output)

    def test_legitimate_route_replay_retains_original_commitment(self) -> None:
        trial = json.loads(
            (self.semantic / SEMANTIC_TRIALS_NAME).read_text().splitlines()[0]
        )
        idempotency_key = _sha256(b"runner-route-replay")
        result = FlowMeshRecordingExecutor().execute(
            trial=trial,
            idempotency_key=idempotency_key,
        )
        original = result["route_evidence_sha256"]
        result["semantic_route_evidence"]["idempotent_replay"] = True
        validated = validate_semantic_trial_result(
            result,
            trial=trial,
            idempotency_key=idempotency_key,
        )
        self.assertTrue(validated["semantic_route_evidence"]["idempotent_replay"])
        self.assertEqual(original, validated["route_evidence_sha256"])

    def test_route_evidence_rejects_private_fields_after_restamping(self) -> None:
        trial = json.loads(
            (self.semantic / SEMANTIC_TRIALS_NAME).read_text().splitlines()[0]
        )
        for private_key, private_value in (
            ("api_key", "private"),
            ("access_token", "private"),
            ("hidden_labels", ["A"]),
            ("relevance_values", [trial["artifact_object_id"]]),
        ):
            with self.subTest(private_key=private_key):
                idempotency_key = _sha256(
                    f"private|{private_key}".encode()
                )
                result = FlowMeshRecordingExecutor().execute(
                    trial=trial,
                    idempotency_key=idempotency_key,
                )
                route = result["semantic_route_evidence"]
                route["semantic"][private_key] = private_value
                core = dict(route)
                core.pop("evidence_sha256")
                route["evidence_sha256"] = _sha256(
                    json.dumps(
                        core,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    ).encode()
                )
                result["route_evidence_sha256"] = route["evidence_sha256"]
                with self.assertRaisesRegex(
                    FullFlowSemanticMatrixRunnerError,
                    "private field",
                ):
                    validate_semantic_trial_result(
                        result,
                        trial=trial,
                        idempotency_key=idempotency_key,
                    )

    def test_public_prediction_is_allowed_inside_exact_n1_request(self) -> None:
        trial = json.loads(
            (self.semantic / SEMANTIC_TRIALS_NAME).read_text().splitlines()[0]
        )
        idempotency_key = _sha256(b"public-prediction")
        result = FlowMeshRecordingExecutor().execute(
            trial=trial,
            idempotency_key=idempotency_key,
        )
        route = result["semantic_route_evidence"]
        prediction = "https://public.example/prediction"
        request = route["n1_score_request"]
        score_result = route["n1_score_result"]
        request["predicted_answer"] = prediction
        score_result["request_sha256"] = _sha256(
            json.dumps(
                request,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
        )
        score_result["prediction_sha256"] = _sha256(prediction.encode())
        score_result_core = dict(score_result)
        score_result_core.pop("result_content_sha256")
        score_result["result_content_sha256"] = _sha256(
            json.dumps(
                score_result_core,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
        )
        route["semantic"]["final_answer_sha256"] = score_result[
            "prediction_sha256"
        ]
        route["scoring"]["result_content_sha256"] = score_result[
            "result_content_sha256"
        ]
        route["scoring"]["authentication_verification_sha256"] = _sha256(
            json.dumps(
                {
                    "domain": (
                        "pathfinder.authenticated-n1-score-verification/v1"
                    ),
                    "request_sha256": score_result["request_sha256"],
                    "result_content_sha256": score_result[
                        "result_content_sha256"
                    ],
                    "score_evidence_hmac_sha256": score_result[
                        "score_evidence_hmac_sha256"
                    ],
                },
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
        )
        core = dict(route)
        core.pop("evidence_sha256")
        route["evidence_sha256"] = _sha256(
            json.dumps(
                core,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
        )
        result["route_evidence_sha256"] = route["evidence_sha256"]
        validated = validate_semantic_trial_result(
            result,
            trial=trial,
            idempotency_key=idempotency_key,
        )
        self.assertEqual(
            "https://public.example/prediction",
            validated["semantic_route_evidence"]["n1_score_request"][
                "predicted_answer"
            ],
        )

    def test_infrastructure_failure_requires_exact_ack_and_resumes_prefix(self) -> None:
        output = self.case / "infra-resume"
        executor = RecordingExecutor({3: [
            SemanticTrialExecutionError("infrastructure", "idp-unavailable")
        ]})
        with self.assertRaises(SemanticMatrixRunFailed) as caught:
            run_full_flow_semantic_matrix(
                **self._arguments(output),
                executor=executor,
            )
        failed_digest = caught.exception.failed_entry_sha256
        self.assertEqual([0, 1, 2, 3], [call[0] for call in executor.calls])
        no_call = RecordingExecutor()
        with self.assertRaises(SemanticMatrixRunFailed):
            run_full_flow_semantic_matrix(
                **self._arguments(output),
                executor=no_call,
            )
        self.assertEqual([], no_call.calls)
        with self.assertRaisesRegex(
            FullFlowSemanticMatrixRunnerError,
            "does not match latest",
        ):
            run_full_flow_semantic_matrix(
                **self._arguments(output),
                executor=no_call,
                acknowledge_failed_entry_sha256="0" * 64,
            )
        resumed = RecordingExecutor()
        result = run_full_flow_semantic_matrix(
            **self._arguments(output),
            executor=resumed,
            acknowledge_failed_entry_sha256=failed_digest,
        )
        self.assertEqual("VERIFIED", result["status"])
        self.assertEqual(list(range(3, 64)), [call[0] for call in resumed.calls])
        self.assertEqual(executor.calls[-1][2], resumed.calls[0][2])
        report = json.loads((output / REPORT_NAME).read_text())
        self.assertEqual(1, report["historic_infrastructure_failure_count"])
        self.assertEqual(0, report["historic_semantic_failure_count"])
        self.assertEqual(1, report["failure_acknowledgement_count"])

    def test_semantic_failure_is_distinguished(self) -> None:
        output = self.case / "semantic-failure"
        executor = RecordingExecutor({0: [
            SemanticTrialExecutionError("semantic", "n1-score-invalid")
        ]})
        with self.assertRaises(SemanticMatrixRunFailed) as caught:
            run_full_flow_semantic_matrix(
                **self._arguments(output),
                executor=executor,
            )
        self.assertEqual("semantic", caught.exception.failure_class)
        row = json.loads((output / JOURNAL_NAME).read_text().splitlines()[-1])
        self.assertEqual("semantic", row["failure_class"])
        self.assertNotIn("message", row)

    def test_ambiguous_intent_requires_a_new_explicit_failure_ack(self) -> None:
        output = self.case / "ambiguous-intent"

        class InterruptedExecutor:
            def execute(self, *, trial: dict, idempotency_key: str) -> dict:
                del trial, idempotency_key
                raise KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            run_full_flow_semantic_matrix(
                **self._arguments(output),
                executor=InterruptedExecutor(),
            )
        rows = [
            json.loads(line)
            for line in (output / JOURNAL_NAME).read_text().splitlines()
        ]
        self.assertEqual(["TRIAL_INTENT"], [row["state"] for row in rows])

        no_call = RecordingExecutor()
        with self.assertRaises(SemanticMatrixRunFailed) as caught:
            run_full_flow_semantic_matrix(
                **self._arguments(output),
                executor=no_call,
            )
        self.assertEqual("ambiguous-execution-outcome", caught.exception.failure_code)
        self.assertEqual([], no_call.calls)

        resumed = RecordingExecutor()
        result = run_full_flow_semantic_matrix(
            **self._arguments(output),
            executor=resumed,
            acknowledge_failed_entry_sha256=(
                caught.exception.failed_entry_sha256
            ),
        )
        self.assertEqual("VERIFIED", result["status"])
        self.assertEqual(list(range(64)), [call[0] for call in resumed.calls])

    def test_invalid_executor_result_becomes_sanitized_semantic_failure(self) -> None:
        output = self.case / "invalid-result"
        executor = RecordingExecutor(invalid_at=0)
        with self.assertRaises(SemanticMatrixRunFailed) as caught:
            run_full_flow_semantic_matrix(
                **self._arguments(output),
                executor=executor,
            )
        self.assertEqual("semantic", caught.exception.failure_class)
        self.assertEqual("invalid-executor-result", caught.exception.failure_code)
        self.assertNotIn("correct_answer_id", (output / JOURNAL_NAME).read_text())

    def test_completed_run_is_read_only_and_never_calls_executor_again(self) -> None:
        output = self.case / "complete-rerun"
        run_full_flow_semantic_matrix(
            **self._arguments(output),
            executor=RecordingExecutor(),
        )
        before = {
            path.relative_to(output).as_posix(): path.read_bytes()
            for path in output.rglob("*")
            if path.is_file()
        }
        executor = RecordingExecutor()
        result = run_full_flow_semantic_matrix(
            **self._arguments(output),
            executor=executor,
        )
        after = {
            path.relative_to(output).as_posix(): path.read_bytes()
            for path in output.rglob("*")
            if path.is_file()
        }
        self.assertEqual("VERIFIED", result["status"])
        self.assertEqual([], executor.calls)
        self.assertEqual(before, after)

    def test_result_checkpoint_crash_window_recovers_without_executor_call(
        self,
    ) -> None:
        output = self.case / "result-crash"
        from pathfinder.simulator import full_flow_matrix_runner as module

        original = module._write_atomic_new
        raised = False

        def fail_first_checkpoint(path: Path, payload: bytes) -> None:
            nonlocal raised
            if path.parent.name == CHECKPOINT_DIR_NAME and not raised:
                raised = True
                raise OSError("simulated crash")
            original(path, payload)

        first = RecordingExecutor()
        with patch.object(
            module,
            "_write_atomic_new",
            side_effect=fail_first_checkpoint,
        ):
            with self.assertRaises(OSError):
                run_full_flow_semantic_matrix(
                    **self._arguments(output),
                    executor=first,
                )
        self.assertEqual([0], [call[0] for call in first.calls])
        resumed = RecordingExecutor()
        result = run_full_flow_semantic_matrix(
            **self._arguments(output),
            executor=resumed,
        )
        self.assertEqual("VERIFIED", result["status"])
        self.assertEqual(list(range(1, 64)), [call[0] for call in resumed.calls])
        completions = [
            json.loads(line)
            for line in (output / JOURNAL_NAME).read_text().splitlines()
            if json.loads(line)["state"] == "TRIAL_COMPLETED"
        ]
        self.assertTrue(completions[0]["recovered_without_executor_call"])

    def test_finalization_crash_reuses_matching_evidence_without_reexecution(
        self,
    ) -> None:
        output = self.case / "finalization-crash"
        from pathfinder.simulator import full_flow_matrix_runner as module

        original = module._write_atomic_new
        raised = False

        def fail_report(path: Path, payload: bytes) -> None:
            nonlocal raised
            if path.name == REPORT_NAME and not raised:
                raised = True
                raise OSError("simulated finalization crash")
            original(path, payload)

        first = RecordingExecutor()
        with patch.object(module, "_write_atomic_new", side_effect=fail_report):
            with self.assertRaises(OSError):
                run_full_flow_semantic_matrix(
                    **self._arguments(output),
                    executor=first,
                )
        self.assertEqual(64, len(first.calls))
        self.assertTrue((output / EVIDENCE_NAME).is_file())
        self.assertFalse((output / REPORT_NAME).exists())
        resumed = RecordingExecutor()
        result = run_full_flow_semantic_matrix(
            **self._arguments(output),
            executor=resumed,
        )
        self.assertEqual("VERIFIED", result["status"])
        self.assertEqual([], resumed.calls)

    def test_output_inside_frozen_source_is_refused_before_execution(self) -> None:
        executor = RecordingExecutor()
        with self.assertRaisesRegex(
            FullFlowSemanticMatrixRunnerError,
            "inside a frozen source",
        ):
            run_full_flow_semantic_matrix(
                **self._arguments(self.semantic / "unsafe-run"),
                executor=executor,
            )
        self.assertEqual([], executor.calls)
        self.assertFalse((self.semantic / "unsafe-run").exists())

    def test_tampered_checkpoint_fails_verification(self) -> None:
        output = self.case / "tamper"
        run_full_flow_semantic_matrix(
            **self._arguments(output),
            executor=RecordingExecutor(),
        )
        checkpoint = sorted((output / CHECKPOINT_DIR_NAME).iterdir())[0]
        checkpoint.write_bytes(checkpoint.read_bytes() + b" ")
        with self.assertRaisesRegex(
            FullFlowSemanticMatrixRunnerError,
            "checkpoint digest mismatch|valid JSON|checksum mismatch",
        ):
            verify_full_flow_semantic_matrix_run(
                **self._verification_arguments(output)
            )

    def test_flowmesh_transport_requires_workflow_evidence_digest(self) -> None:
        output = self.case / "flowmesh-claim"
        executor = RecordingExecutor()
        original = executor.execute

        def invalid_flowmesh(*, trial: dict, idempotency_key: str) -> dict:
            result = original(trial=trial, idempotency_key=idempotency_key)
            result["execution_transport"] = "flowmesh"
            return result

        executor.execute = invalid_flowmesh  # type: ignore[method-assign]
        with self.assertRaises(SemanticMatrixRunFailed) as caught:
            run_full_flow_semantic_matrix(
                **self._arguments(output),
                executor=executor,
            )
        self.assertEqual("invalid-executor-result", caught.exception.failure_code)


if __name__ == "__main__":
    unittest.main()
