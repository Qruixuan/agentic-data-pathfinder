from __future__ import annotations

import hashlib
import json
import unittest
from typing import Any, Mapping

from pathfinder.integrations.flowmesh.contracts import (
    FlowMeshSettings,
    FlowMeshWorkerIdentity,
    SubmittedWorkflow,
    TerminalWorkflow,
    WorkflowValidation,
)
from pathfinder.integrations.flowmesh.semantic_matrix_trial import (
    FlowMeshSemanticTrialError,
    FlowMeshSemanticTrialExecutor,
    _model_input_frontier,
    _verify_route_evidence,
    GenericSemanticRouteRequestHandler,
    SEMANTIC_ROUTE_ENDPOINT_PATH,
    build_semantic_route_request,
    full_flow_hmac_header_provider,
    validate_semantic_route_request,
)
from pathfinder.simulator.container_node import (
    FULL_FLOW_INGRESS_SIGNATURE_HEADER,
    full_flow_request_hmac_sha256,
)
from pathfinder.simulator.full_flow_matrix_runner import (
    TRIAL_RESULT_SCHEMA_VERSION,
    SemanticTrialExecutionError,
    _validated_result,
)
from pathfinder.simulator.full_flow_semantic_execution_admission import (
    BOUND_STAGE_SCHEMA_VERSION,
    BOUND_TRIAL_SCHEMA_VERSION,
)
from pathfinder.simulator.full_flow_semantic_route_runtime import (
    SEMANTIC_ROUTE_EVIDENCE_SCHEMA_VERSION,
)
from pathfinder.simulator.full_flow_semantic_input_profiles import (
    build_semantic_input_profile,
    profile_sha256,
)


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _fixtures(executor_node_id: str = "N8") -> tuple[dict, dict, list[dict]]:
    source = {
        "schema_version": "pathfinder.full-flow-semantic-matrix-trial/v1alpha1",
        "trial_key": "scenario|semantic-workload|D2|r0000",
        "order_index": 0,
        "workload_id": "semantic-workload",
        "workload_class": "W1",
        "design_id": "D2",
        "repetition": 0,
        "route_family": "remote-derived",
    }
    stage_actions = (
        ("admit", "admit-trial", "N1"),
        ("infer", "infer", "N6"),
        ("score", "score-hidden-answer", "N1"),
    )
    stages = []
    previous = None
    for index, (suffix, action, node) in enumerate(stage_actions):
        key = f"{source['trial_key']}|{suffix}"
        stages.append({
            "schema_version": BOUND_STAGE_SCHEMA_VERSION,
            "stage_key": key,
            "trial_key": source["trial_key"],
            "stage_index": index,
            "phase": "evaluation" if action == "score-hidden-answer" else "execution",
            "action": action,
            "condition": None,
            "dependency_stage_keys": [] if previous is None else [previous],
            "service_contract_id": f"{node}.service",
            "logical_node_ids": [node],
            "deployment_adapter_id": "test-adapter-v1",
            "service_base_url": f"http://127.0.0.1:19{index + 1:03d}",
            "credential_env_names": [],
            "network_binding": None,
            "public_task_binding_sha256": _sha(b"public-task"),
            "object_representation_identity": {
                "logical_object_id": "logical-video",
                "artifact_object_id": "real-video",
                "representation_id": "multimodal_digest",
                "representation_binding": {
                    "representation_id": "multimodal_digest",
                    "artifact_sha256": _sha(b"digest"),
                    "artifact_size_bytes": 6,
                    "object_catalog_version": "catalog-v1",
                },
            },
            "source_semantic_stage_sha256": _sha(f"source-{index}".encode()),
            "stage_result_handoff_mode": "route-coordinator-required",
            "direct_flowmesh_api_task_ready": False,
            "credential_values_included": False,
        })
        previous = key
    identity = {
        "logical_object_id": "logical-video",
        "artifact_object_id": "real-video",
        "representation_id": "multimodal_digest",
        "representation_binding": {
            "representation_id": "multimodal_digest",
            "artifact_sha256": _sha(b"digest"),
            "artifact_size_bytes": 6,
            "object_catalog_version": "catalog-v1",
        },
    }
    bound = {
        "schema_version": BOUND_TRIAL_SCHEMA_VERSION,
        "trial_key": source["trial_key"],
        "order_index": 0,
        "workload_id": "semantic-workload",
        "workload_class": "W1",
        "design_id": "D2",
        "repetition": 0,
        "route_family": "remote-derived",
        "executor_node_id": executor_node_id,
        "public_task_binding_sha256": _sha(b"public-task"),
        "public_task_binding": {
            "schema_version": "pathfinder.n1-public-task/v1alpha1",
            "workload_id": "semantic-workload",
            "object_id": "real-video",
            "task_class_id": "video_qa_descriptive",
            "question": "What is visible?",
            "answer_options": [
                {"option_id": "A", "text": "One action."},
                {"option_id": "B", "text": "Another action."},
            ],
            "success_scoring_rule": "multiple-choice-exact-option-id-v1",
            "task_binding_sha256": _sha(b"public-task"),
            "hidden_label_included": False,
        },
        "artifact_object_id": "real-video",
        "representation_identities": [identity],
        "semantic_stage_keys": [row["stage_key"] for row in stages],
        "bound_stage_sha256": [_sha(_canonical(row)) for row in stages],
        "required_provisioning_chain_ids": [],
        "source_semantic_trial_sha256": _sha(_canonical(source)),
        "worker_alias": "semantic-worker",
        "flowmesh_execution_shape": "one-api-task-to-route-coordinator",
        "route_coordinator_binding": {
            "service_contract_id": f"{executor_node_id}.execution-compute",
            "base_url": "http://127.0.0.1:19088"
            if executor_node_id == "N8"
            else "http://127.0.0.1:19087",
            "adapter_id": "semantic-route-coordinator-v1",
            "credential_env_names": ["PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET"],
        },
        "required_runtime_adapter_ids": [],
        "flowmesh_submission_authorized": True,
        "semantic_execution_performed": False,
        "credentials_recorded": False,
    }
    return source, bound, stages


def _route_evidence(run_id: str, trial: Mapping[str, Any], stages: list[dict]) -> dict:
    stage_results = []
    for index, stage in enumerate(stages):
        stage_results.append({
            "stage_key": stage["stage_key"],
            "stage_index": index,
            "action": stage["action"],
            "condition": None,
            "state": "EXECUTED",
            "outcome_kind": "test",
            "outcome_sha256": _sha(f"outcome-{index}".encode()),
            "service_time_ms": float(index + 1),
            "bytes_read": index * 10,
            "bytes_sent": index * 5,
        })
    identity_core = {
        "object_id": "real-video",
        "representation_id": "multimodal_digest",
        "artifact_sha256": _sha(b"digest"),
        "artifact_size_bytes": 6,
        "object_catalog_version": "catalog-v1",
    }
    score_request = {
        "schema_version": "pathfinder.n1-score-request/v1alpha2",
        "score_request_id": _sha(b"score-request"),
        "evaluation_unit_id": _sha(b"unit"),
        "oracle_id": "oracle-v1",
        "run_id": run_id,
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
        "run_id": run_id,
        "trial_id": trial["trial_key"],
        "object_id": trial["artifact_object_id"],
        "task_binding_sha256": trial["public_task_binding_sha256"],
        "request_sha256": _sha(_canonical(score_request)),
        "prediction_sha256": _sha(b"A"),
        "success_scoring_rule": "multiple-choice-exact-option-id-v1",
        "correct": True,
        "score": 1.0,
        "public_task_set_sha256": _sha(b"public-task-set"),
        "oracle_instance_hmac_sha256": _sha(b"oracle-hmac"),
        "score_evidence_hmac_sha256": _sha(b"score-hmac"),
        "idempotent_replay": False,
        "hidden_answer_returned": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    score_result["result_content_sha256"] = _sha(_canonical(score_result))
    authentication_verification_sha256 = _sha(_canonical({
        "domain": "pathfinder.authenticated-n1-score-verification/v1",
        "request_sha256": score_result["request_sha256"],
        "result_content_sha256": score_result["result_content_sha256"],
        "score_evidence_hmac_sha256": score_result[
            "score_evidence_hmac_sha256"
        ],
    }))
    evidence = {
        "schema_version": SEMANTIC_ROUTE_EVIDENCE_SCHEMA_VERSION,
        "status": "COMPLETE",
        "execution_id": _sha(b"execution"),
        "request_sha256": _sha(b"route-request"),
        "run_id": run_id,
        "trial_id": trial["trial_key"],
        "trial_key": trial["trial_key"],
        "trial_sha256": _sha(_canonical(trial)),
        "stage_dag_sha256": _sha(_canonical(stages)),
        "workload_id": trial["workload_id"],
        "workload_class": trial["workload_class"],
        "design_id": trial["design_id"],
        "repetition": trial["repetition"],
        "artifact_object_id": trial["artifact_object_id"],
        "public_task_binding_sha256": trial["public_task_binding_sha256"],
        "route": {
            "route_family": trial["route_family"],
            "executor_node_id": trial["executor_node_id"],
            "inference_node_id": "N6",
            "score_node_id": "N1",
        },
        "artifact_identities": [{
            "logical_object_id": "logical-video",
            **identity_core,
            "identity_sha256": _sha(_canonical(identity_core)),
        }],
        "provisioning_references": [],
        "stage_results": stage_results,
        "cache_branches": [],
        "model_input": {
            "mode": "digest",
            "payload_sha256": _sha(b"model-input"),
            "payload_size_bytes": 12,
            "component_identity_sha256": [_sha(_canonical(identity_core))],
            "preparation_sha256": _sha(b"preparation"),
        },
        "semantic": {
            "model": "vision-model",
            "input_sha256": _sha(b"model-input"),
            "request_sha256": _sha(b"semantic-request"),
            "result_sha256": _sha(b"semantic-result"),
            "final_answer_sha256": _sha(b"A"),
            "service_time_ms": 2.0,
        },
        "scoring": {
            "oracle_id": "oracle-v1",
            "score_request_id": _sha(b"score-request"),
            "evaluation_unit_id": _sha(b"unit"),
            "task_binding_sha256": trial["public_task_binding_sha256"],
            "task_success": True,
            "score": 1.0,
            "score_evidence_hmac_sha256": _sha(b"score-hmac"),
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
            "executor_node_id": trial["executor_node_id"],
            "task_success": True,
            "score": 1.0,
            "score_authenticity_verified": True,
            "score_authentication": "n1-hmac-verified",
            "component_service_time_ms": {"test": 1.0},
            "byte_measurements": {
                "adapter_bytes_read": 0,
                "adapter_bytes_sent": 0,
                "semantic_input_bytes": 12,
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
    evidence["evidence_sha256"] = _sha(_canonical(evidence))
    return evidence


class FakeFlowMeshClient:
    def __init__(self, result: Mapping[str, Any], *, wrapped: bool = False) -> None:
        self.result = dict(result)
        self.wrapped = wrapped
        self.workflow: dict | None = None
        self.validation = WorkflowValidation(ok=True)
        self.assigned_worker = "wkr-semantic"
        self.submit_count = 0

    def describe_current_worker(self, *, worker_id=None, alias=None):
        self.requested_alias = alias
        return FlowMeshWorkerIdentity(
            worker_id="wkr-semantic",
            alias="semantic-worker",
            status="IDLE",
        )

    def validate(self, workflow):
        self.workflow = json.loads(json.dumps(workflow))
        return self.validation

    def submit(self, workflow):
        self.submit_count += 1
        self.workflow = json.loads(json.dumps(workflow))
        return SubmittedWorkflow("wfl-semantic", ("tsk-semantic",))

    def wait(self, workflow_id, poll_interval_seconds):
        return TerminalWorkflow(workflow_id=workflow_id, status="DONE")

    def retrieve_result(self, task_id):
        api = {
            "executor": "api",
            "ok": True,
            "status_code": 200,
            "text": json.dumps(self.result, sort_keys=True),
        }
        return {"result": api} if self.wrapped else api

    def describe_task_failure(self, task_id):
        return {"status": "DONE", "assigned_worker": self.assigned_worker}


class FakeRouteCoordinator:
    def __init__(self) -> None:
        self.call: dict | None = None

    def execute(self, *, run_id, bound_trial, bound_stages):
        self.call = {
            "run_id": run_id,
            "bound_trial": bound_trial,
            "bound_stages": bound_stages,
        }
        return {"status": "COMPLETE", "trial_key": bound_trial["trial_key"]}


class FlowMeshSemanticTrialTest(unittest.TestCase):
    def _executor(
        self,
        node: str = "N8",
        *,
        wrapped: bool = False,
        runtime_header_provider=None,
    ):
        source, bound, stages = _fixtures(node)
        evidence = _route_evidence("semantic-run-v1", bound, stages)
        client = FakeFlowMeshClient(evidence, wrapped=wrapped)
        if runtime_header_provider is None:
            runtime_header_provider = lambda _request: {
                FULL_FLOW_INGRESS_SIGNATURE_HEADER: "a" * 64
            }
        executor = FlowMeshSemanticTrialExecutor(
            client=client,
            settings=FlowMeshSettings(
                worker_alias="semantic-worker",
                owner="pathfinder",
                task_timeout_seconds=900,
            ),
            run_id="semantic-run-v1",
            bound_trials=[bound],
            bound_stages=stages,
            runtime_header_provider=runtime_header_provider,
        )
        return source, bound, stages, client, executor

    def test_n8_direct_api_result_produces_strict_neutral_result(self) -> None:
        source, _bound, _stages, client, executor = self._executor("N8")
        idem = _sha(b"idempotency")
        result = executor.execute(trial=source, idempotency_key=idem)
        self.assertEqual(TRIAL_RESULT_SCHEMA_VERSION, result["schema_version"])
        self.assertEqual("flowmesh", result["execution_transport"])
        self.assertTrue(result["task_success"])
        self.assertTrue(result["n1_score_authenticity_verified"])
        self.assertEqual(client.result, result["semantic_route_evidence"])
        self.assertEqual(
            client.result["evidence_sha256"],
            result["route_evidence_sha256"],
        )
        self.assertEqual(9, len(result["measurements"]))
        self.assertEqual(
            result,
            _validated_result(result, trial=source, idempotency_key=idem),
        )
        self.assertEqual("wkr-semantic", (
            client.workflow["metadata"]["annotations"]["schedule_hint"]
            ["selected_worker"]
        ))
        task = client.workflow["spec"]["graph"]["nodes"][0]["spec"]
        self.assertEqual(
            "http://127.0.0.1:19088" + SEMANTIC_ROUTE_ENDPOINT_PATH,
            task["api"]["url"],
        )
        self.assertEqual(idem, task["api"]["body"]["idempotency_key"])
        self.assertNotIn("url", result)
        self.assertNotIn('"final_answer":', json.dumps(result).lower())
        self.assertEqual(
            "A",
            result["semantic_route_evidence"]["n1_score_request"][
                "predicted_answer"
            ],
        )
        serialized = json.dumps(result).casefold()
        self.assertNotIn("correct_answer", serialized)
        self.assertNotIn("hidden_label", serialized)

    def test_verified_bound_trial_can_drive_representative_smoke(self) -> None:
        _source, bound, _stages, client, executor = self._executor("N8")
        idem = _sha(b"promoted-bound-smoke")
        result = executor.execute(trial=bound, idempotency_key=idem)
        self.assertEqual("COMPLETE", result["status"])
        self.assertEqual(bound["trial_key"], result["trial_key"])
        self.assertEqual(1, client.submit_count)

    def test_n7_wrapped_api_result_is_supported(self) -> None:
        source, _bound, _stages, client, executor = self._executor(
            "N7", wrapped=True
        )
        result = executor.execute(
            trial=source,
            idempotency_key=_sha(b"wrapped"),
        )
        task = client.workflow["spec"]["graph"]["nodes"][0]["spec"]
        self.assertEqual(
            "http://127.0.0.1:19087" + SEMANTIC_ROUTE_ENDPOINT_PATH,
            task["api"]["url"],
        )
        self.assertTrue(result["llm_called"])

    def test_runtime_hmac_header_is_submitted_but_never_returned(self) -> None:
        fake_secret = "unit-test-only-secret-0123456789abcdef"
        source, _bound, _stages, client, executor = self._executor(
            runtime_header_provider=full_flow_hmac_header_provider(fake_secret)
        )
        result = executor.execute(
            trial=source,
            idempotency_key=_sha(b"runtime-header"),
        )
        api = client.workflow["spec"]["graph"]["nodes"][0]["spec"]["api"]
        self.assertEqual(
            full_flow_request_hmac_sha256(api["body"], fake_secret),
            api["headers"][FULL_FLOW_INGRESS_SIGNATURE_HEADER],
        )
        serialized = json.dumps(result, sort_keys=True)
        self.assertNotIn(fake_secret, serialized)
        self.assertNotIn(
            api["headers"][FULL_FLOW_INGRESS_SIGNATURE_HEADER], serialized
        )

    def test_request_digest_and_stage_binding_fail_closed(self) -> None:
        _source, bound, stages = _fixtures()
        request = build_semantic_route_request(
            run_id="semantic-run-v1",
            idempotency_key=_sha(b"request"),
            bound_trial=bound,
            bound_stages=stages,
        )
        request["bound_stages"][0]["action"] = "changed"
        with self.assertRaisesRegex(ValueError, "bound stage content changed"):
            validate_semantic_route_request(request)

    def test_request_handler_bridges_to_generic_route_coordinator(self) -> None:
        _source, bound, stages = _fixtures()
        request = build_semantic_route_request(
            run_id="semantic-run-v1",
            idempotency_key=_sha(b"handler"),
            bound_trial=bound,
            bound_stages=stages,
        )
        coordinator = FakeRouteCoordinator()
        handler = GenericSemanticRouteRequestHandler(coordinator)
        result = handler.execute(request)
        self.assertEqual("COMPLETE", result["status"])
        self.assertEqual("semantic-run-v1", coordinator.call["run_id"])
        self.assertEqual(bound, coordinator.call["bound_trial"])
        self.assertEqual(stages, coordinator.call["bound_stages"])

    def test_unauthorized_bound_trial_never_calls_flowmesh(self) -> None:
        source, bound, stages = _fixtures()
        bound["flowmesh_submission_authorized"] = False
        evidence = _route_evidence("semantic-run-v1", bound, stages)
        client = FakeFlowMeshClient(evidence)
        executor = FlowMeshSemanticTrialExecutor(
            client=client,
            settings=FlowMeshSettings(worker_alias="semantic-worker"),
            run_id="semantic-run-v1",
            bound_trials=[bound],
            bound_stages=stages,
            runtime_header_provider=lambda _request: {
                FULL_FLOW_INGRESS_SIGNATURE_HEADER: "a" * 64
            },
        )
        with self.assertRaises(SemanticTrialExecutionError) as context:
            executor.execute(
                trial=source,
                idempotency_key=_sha(b"unauthorized"),
            )
        self.assertEqual("semantic", context.exception.failure_class)
        self.assertEqual(0, client.submit_count)

    def test_wrong_completed_worker_fails_with_stable_code(self) -> None:
        source, _bound, _stages, client, executor = self._executor()
        client.assigned_worker = "wkr-other"
        with self.assertRaises(SemanticTrialExecutionError) as context:
            executor.execute(
                trial=source,
                idempotency_key=_sha(b"wrong-worker"),
            )
        self.assertEqual("infrastructure", context.exception.failure_class)
        self.assertEqual(
            "flowmesh-worker-assignment-mismatch",
            context.exception.failure_code,
        )

    def test_invalid_route_evidence_is_semantic_failure(self) -> None:
        source, _bound, _stages, client, executor = self._executor()
        client.result["trial_key"] = "different-trial"
        with self.assertRaises(SemanticTrialExecutionError) as context:
            executor.execute(
                trial=source,
                idempotency_key=_sha(b"bad-evidence"),
            )
        self.assertEqual("semantic", context.exception.failure_class)
        self.assertEqual(
            "invalid-semantic-route-evidence",
            context.exception.failure_code,
        )

    def test_validation_rejection_prevents_submission(self) -> None:
        source, _bound, _stages, client, executor = self._executor()
        client.validation = WorkflowValidation(ok=False, errors=("bad",))
        with self.assertRaises(SemanticTrialExecutionError) as context:
            executor.execute(
                trial=source,
                idempotency_key=_sha(b"validation"),
            )
        self.assertEqual("semantic", context.exception.failure_class)
        self.assertEqual(0, client.submit_count)


if __name__ == "__main__":
    unittest.main()


def _identity(representation_id: str, size: int) -> dict:
    return {
        "logical_object_id": "logical-video",
        "artifact_object_id": "real-video",
        "representation_id": representation_id,
        "representation_binding": {
            "representation_id": representation_id,
            "artifact_sha256": _sha(representation_id.encode()),
            "artifact_size_bytes": size,
            "object_catalog_version": "catalog-v1",
        },
    }


def _core(identity: Mapping[str, Any]) -> dict:
    binding = identity["representation_binding"]
    return {
        "object_id": identity["artifact_object_id"],
        "representation_id": identity["representation_id"],
        "artifact_sha256": binding["artifact_sha256"],
        "artifact_size_bytes": binding["artifact_size_bytes"],
        "object_catalog_version": binding["object_catalog_version"],
    }


def _identity_digest(identity: Mapping[str, Any]) -> str:
    return _sha(_canonical(_core(identity)))


#: Frozen DAG shapes as (suffix, action, dependency suffixes, representation).
#: ``None`` marks a control stage carrying the unbound identity placeholder.
_ROUTE_SHAPES: dict[str, tuple[tuple[str, str, tuple[str, ...], str | None], ...]] = {
    # raw: the raw video itself is the N6 input
    "raw": (
        ("admit", "admit-trial", (), None),
        ("scan-raw", "access-raw-artifact", ("admit",), "raw_video"),
        ("send-model-input", "transfer-bytes", ("scan-raw",), "raw_video"),
        ("infer", "infer", ("send-model-input",), None),
    ),
    # indexed raw: the selected exact range carries the raw source identity
    "indexed-raw": (
        ("admit", "admit-trial", (), None),
        ("query-index", "query-index", ("admit",), None),
        ("read-range", "access-raw-artifact", ("query-index",), "raw_video"),
        ("send-model-input", "transfer-bytes", ("read-range",), "raw_video"),
        ("infer", "infer", ("send-model-input",), None),
    ),
    # remote digest: the digest is the N6 input
    "remote-digest": (
        ("admit", "admit-trial", (), None),
        ("read-digest", "access-derived-artifact", ("admit",), "multimodal_digest"),
        ("send-model-input", "transfer-bytes", ("read-digest",), "multimodal_digest"),
        ("infer", "infer", ("send-model-input",), None),
    ),
    # remote frames: the frame bundle is the N6 input
    "remote-frames": (
        ("admit", "admit-trial", (), None),
        ("read-frames", "access-derived-artifact", ("admit",), "sampled_frame_bundle"),
        ("send-model-input", "transfer-bytes", ("read-frames",), "sampled_frame_bundle"),
        ("infer", "infer", ("send-model-input",), None),
    ),
    # remote combined: both representations are sent to N6 through a join
    "remote-combined": (
        ("admit", "admit-trial", (), None),
        ("read-digest", "access-derived-artifact", ("admit",), "multimodal_digest"),
        ("read-frames", "access-derived-artifact", ("admit",), "sampled_frame_bundle"),
        ("join", "branch-join", ("read-digest", "read-frames"), None),
        ("send-model-input", "transfer-bytes", ("join",), None),
        ("infer", "infer", ("send-model-input",), None),
    ),
    # remote retrieval (the deployed D2): the digest selects candidates
    # upstream, and only the frame bundle reaches N6.
    "remote-retrieval": (
        ("admit", "admit-trial", (), None),
        ("query-index", "query-index", ("admit",), None),
        ("read-digests", "access-derived-artifact", ("query-index",), "multimodal_digest"),
        ("transfer-digests", "transfer-bytes", ("read-digests",), "multimodal_digest"),
        ("read-frames", "access-derived-artifact", ("transfer-digests",), "sampled_frame_bundle"),
        ("send-model-input", "transfer-bytes", ("read-frames",), "sampled_frame_bundle"),
        ("infer", "infer", ("send-model-input",), None),
    ),
    # local cache: hit and miss branches join, and both reach the same
    # artifact, so the frontier must not count it twice.
    "local-cache": (
        ("admit", "admit-trial", (), None),
        ("cache-hit", "cache-lookup", ("admit",), "sampled_frame_bundle"),
        ("cache-miss", "access-derived-artifact", ("admit",), "sampled_frame_bundle"),
        ("join", "branch-join", ("cache-hit", "cache-miss"), None),
        ("send-model-input", "transfer-bytes", ("join",), None),
        ("infer", "infer", ("send-model-input",), None),
    ),
}

_REPRESENTATION_SIZES = {
    "raw_video": 481280,
    "sampled_frame_bundle": 481280,
    "multimodal_digest": 827,
}


class ModelInputFrontierTest(unittest.TestCase):
    """The N6 model input binds the frozen input frontier, not every artifact.

    A retrieval intermediate is routed and must appear in the route's artifact
    identities, but it is never sent to N6. Requiring the model input to name
    every routed artifact rejected the frozen remote-retrieval workload.
    """

    def _build(
        self,
        shape: str,
        executor_node_id: str = "N8",
    ) -> tuple[dict, list[dict], dict]:
        _source, trial, base_stages = _fixtures(executor_node_id)
        template = base_stages[0]
        trial_key = trial["trial_key"]
        rows = _ROUTE_SHAPES[shape]
        used: dict[str, dict] = {}
        stages: list[dict] = []
        for index, (suffix, action, deps, representation) in enumerate(rows):
            if representation is None:
                identity = {
                    "logical_object_id": "logical-video",
                    "artifact_object_id": "real-video",
                    "representation_id": None,
                    "representation_binding": {},
                }
            else:
                identity = _identity(
                    representation,
                    _REPRESENTATION_SIZES[representation],
                )
                used[representation] = identity
            stage = dict(template)
            stage.update({
                "stage_key": f"{trial_key}|{suffix}",
                "stage_index": index,
                "action": action,
                "condition": None,
                "dependency_stage_keys": [
                    f"{trial_key}|{name}" for name in deps
                ],
                "object_representation_identity": identity,
                "source_semantic_stage_sha256": _sha(f"src-{index}".encode()),
            })
            stages.append(stage)
        identities = [used[name] for name in sorted(used)]
        trial = dict(trial)
        trial["representation_identities"] = identities
        trial["semantic_stage_keys"] = [row["stage_key"] for row in stages]
        trial["bound_stage_sha256"] = [_sha(_canonical(row)) for row in stages]
        evidence = _route_evidence("run-frontier", trial, stages)
        evidence["artifact_identities"] = [
            {
                "logical_object_id": identity["logical_object_id"],
                **_core(identity),
                "identity_sha256": _identity_digest(identity),
            }
            for identity in identities
        ]
        evidence["trial_sha256"] = _sha(_canonical(trial))
        evidence["stage_dag_sha256"] = _sha(_canonical(stages))
        return trial, stages, evidence

    def _seal(self, evidence: dict) -> dict:
        """Recompute the evidence self-digest after a fixture mutation."""

        core = dict(evidence)
        core.pop("evidence_sha256", None)
        evidence["evidence_sha256"] = _sha(_canonical(core))
        return evidence

    def _verify(self, trial: dict, stages: list[dict], evidence: dict) -> dict:
        return _verify_route_evidence(
            self._seal(evidence),
            run_id="run-frontier",
            bound_trial=trial,
            bound_stages=stages,
        )

    def _set_model_input(self, evidence: dict, names: list[str]) -> None:
        evidence["model_input"] = dict(evidence["model_input"])
        evidence["model_input"]["component_identity_sha256"] = [
            _identity_digest(_identity(name, _REPRESENTATION_SIZES[name]))
            for name in names
        ]

    def test_remote_retrieval_binds_only_the_frame_bundle(self) -> None:
        trial, stages, evidence = self._build("remote-retrieval")
        self._set_model_input(evidence, ["sampled_frame_bundle"])
        verified = self._verify(trial, stages, evidence)
        self.assertEqual(trial["trial_key"], verified["trial_key"])
        # the retrieval intermediate stays in the full route artifact set
        self.assertEqual(
            ["multimodal_digest", "sampled_frame_bundle"],
            sorted(
                row["representation_id"]
                for row in evidence["artifact_identities"]
            ),
        )

    def test_remote_retrieval_rejects_missing_frame_bundle(self) -> None:
        trial, stages, evidence = self._build("remote-retrieval")
        self._set_model_input(evidence, [])
        with self.assertRaises(FlowMeshSemanticTrialError) as caught:
            self._verify(trial, stages, evidence)
        self.assertIn("model input", str(caught.exception).lower())

    def test_remote_retrieval_rejects_added_retrieval_digest(self) -> None:
        trial, stages, evidence = self._build("remote-retrieval")
        self._set_model_input(
            evidence,
            ["multimodal_digest", "sampled_frame_bundle"],
        )
        with self.assertRaises(FlowMeshSemanticTrialError):
            self._verify(trial, stages, evidence)

    def test_remote_combined_requires_both_identities(self) -> None:
        trial, stages, evidence = self._build("remote-combined")
        self._set_model_input(
            evidence,
            ["multimodal_digest", "sampled_frame_bundle"],
        )
        self._verify(trial, stages, evidence)
        for partial in (["multimodal_digest"], ["sampled_frame_bundle"]):
            with self.subTest(model_input=partial):
                trial, stages, evidence = self._build("remote-combined")
                self._set_model_input(evidence, partial)
                with self.assertRaises(FlowMeshSemanticTrialError):
                    self._verify(trial, stages, evidence)

    def test_remote_digest_and_frames_shapes(self) -> None:
        for shape, expected in (
            ("remote-digest", "multimodal_digest"),
            ("remote-frames", "sampled_frame_bundle"),
        ):
            with self.subTest(shape=shape):
                trial, stages, evidence = self._build(shape)
                self._set_model_input(evidence, [expected])
                self._verify(trial, stages, evidence)

    def test_raw_and_indexed_raw_require_the_raw_identity(self) -> None:
        for shape in ("raw", "indexed-raw"):
            with self.subTest(shape=shape):
                trial, stages, evidence = self._build(shape)
                self._set_model_input(evidence, ["raw_video"])
                self._verify(trial, stages, evidence)
                trial, stages, evidence = self._build(shape)
                self._set_model_input(evidence, [])
                with self.assertRaises(FlowMeshSemanticTrialError):
                    self._verify(trial, stages, evidence)

    def test_frozen_semantic_input_profile_is_verified(self) -> None:
        trial, stages, evidence = self._build("raw")
        trial["route_family"] = "raw"
        profile = build_semantic_input_profile(
            route_family="raw",
            model_input_representation_ids=["raw_video"],
        )
        trial["semantic_input_profile"] = profile
        evidence["route"]["route_family"] = "raw"
        evidence["trial_sha256"] = _sha(_canonical(trial))
        self._set_model_input(evidence, ["raw_video"])
        evidence["semantic_input_profile_verified"] = True
        evidence["model_input"].update({
            "mode": "raw-prepared-frames",
            "semantic_input_profile_id": profile["profile_id"],
            "semantic_input_profile_sha256": profile_sha256(profile),
            "semantic_input_profile_verified": True,
            "semantic_content_sha256": _sha(b"semantic-content"),
            "frame_count": 24,
            "frame_timestamps_seconds": [float(index) for index in range(24)],
            "frame_dimensions": [
                {"width": 2, "height": 2} for _ in range(24)
            ],
            "frame_payload_bytes": 240,
            "frame_sequence_sha256": _sha(b"frame-sequence"),
            "digest_input_sha256": None,
            "temporal_window_fraction": [0.0, 1.0],
            "direct_video_input": False,
        })
        self._verify(trial, stages, evidence)
        evidence["model_input"]["temporal_window_fraction"] = [0.1, 1.0]
        with self.assertRaisesRegex(
            FlowMeshSemanticTrialError,
            "frozen profile",
        ):
            self._verify(trial, stages, evidence)

    def test_local_cache_join_counts_the_artifact_once(self) -> None:
        trial, stages, evidence = self._build("local-cache")
        self._set_model_input(evidence, ["sampled_frame_bundle"])
        self._verify(trial, stages, evidence)
        frontier = _model_input_frontier(
            bound_stages=stages,
            frozen_identity_digests={
                _identity_digest(identity): _core(identity)
                for identity in trial["representation_identities"]
            },
        )
        self.assertEqual(1, len(frontier))

    def test_identity_absent_from_the_frozen_trial_is_rejected(self) -> None:
        trial, stages, evidence = self._build("remote-retrieval")
        trial = dict(trial)
        trial["representation_identities"] = [
            identity
            for identity in trial["representation_identities"]
            if identity["representation_id"] != "sampled_frame_bundle"
        ]
        evidence["trial_sha256"] = _sha(_canonical(trial))
        self._set_model_input(evidence, ["sampled_frame_bundle"])
        with self.assertRaises(FlowMeshSemanticTrialError):
            self._verify(trial, stages, evidence)

    def test_artifact_identities_must_list_every_frozen_artifact(self) -> None:
        trial, stages, evidence = self._build("remote-retrieval")
        self._set_model_input(evidence, ["sampled_frame_bundle"])
        evidence["artifact_identities"] = [
            row
            for row in evidence["artifact_identities"]
            if row["representation_id"] != "multimodal_digest"
        ]
        with self.assertRaises(FlowMeshSemanticTrialError) as caught:
            self._verify(trial, stages, evidence)
        self.assertIn("artifact identities", str(caught.exception))

    def test_component_order_does_not_change_the_comparison(self) -> None:
        trial, stages, evidence = self._build("remote-combined")
        self._set_model_input(
            evidence,
            ["multimodal_digest", "sampled_frame_bundle"],
        )
        forward = list(evidence["model_input"]["component_identity_sha256"])
        self._verify(trial, stages, evidence)
        evidence["model_input"]["component_identity_sha256"] = forward[::-1]
        self._verify(trial, stages, evidence)

    def test_duplicate_component_identities_are_rejected(self) -> None:
        trial, stages, evidence = self._build("remote-frames")
        self._set_model_input(evidence, ["sampled_frame_bundle"])
        components = evidence["model_input"]["component_identity_sha256"]
        evidence["model_input"]["component_identity_sha256"] = components * 2
        with self.assertRaises(FlowMeshSemanticTrialError):
            self._verify(trial, stages, evidence)

    def test_missing_dependency_stage_fails_closed(self) -> None:
        # remote-combined walks through the join, so a pruned branch stage is
        # genuinely unreachable rather than simply never visited.
        trial, stages, evidence = self._build("remote-combined")
        pruned = [
            stage
            for stage in stages
            if not stage["stage_key"].endswith("|read-frames")
        ]
        with self.assertRaises(FlowMeshSemanticTrialError):
            _model_input_frontier(
                bound_stages=pruned,
                frozen_identity_digests={
                    _identity_digest(identity): _core(identity)
                    for identity in trial["representation_identities"]
                },
            )

    def test_dependency_cycle_fails_closed(self) -> None:
        trial, stages, evidence = self._build("remote-combined")
        trial_key = trial["trial_key"]
        for stage in stages:
            if stage["stage_key"].endswith("|join"):
                stage["dependency_stage_keys"] = [f"{trial_key}|send-model-input"]
        with self.assertRaises(FlowMeshSemanticTrialError) as caught:
            _model_input_frontier(
                bound_stages=stages,
                frozen_identity_digests={
                    _identity_digest(identity): _core(identity)
                    for identity in trial["representation_identities"]
                },
            )
        self.assertIn("cycle", str(caught.exception))

    def test_empty_frontier_fails_closed(self) -> None:
        trial, stages, evidence = self._build("remote-frames")
        for stage in stages:
            if stage["action"] != "infer":
                stage["object_representation_identity"] = {
                    "logical_object_id": "logical-video",
                    "artifact_object_id": "real-video",
                    "representation_id": None,
                    "representation_binding": {},
                }
        with self.assertRaises(FlowMeshSemanticTrialError):
            _model_input_frontier(
                bound_stages=stages,
                frozen_identity_digests={
                    _identity_digest(identity): _core(identity)
                    for identity in trial["representation_identities"]
                },
            )

    def test_malformed_identity_binding_fails_closed(self) -> None:
        trial, stages, evidence = self._build("remote-frames")
        for stage in stages:
            if stage["stage_key"].endswith("|send-model-input"):
                stage["object_representation_identity"] = {
                    "logical_object_id": "logical-video",
                    "artifact_object_id": "real-video",
                    "representation_id": "sampled_frame_bundle",
                    "representation_binding": {"artifact_sha256": "not-a-digest"},
                }
        with self.assertRaises(FlowMeshSemanticTrialError):
            _model_input_frontier(
                bound_stages=stages,
                frozen_identity_digests={
                    _identity_digest(identity): _core(identity)
                    for identity in trial["representation_identities"]
                },
            )

    def test_exactly_one_infer_stage_is_required(self) -> None:
        trial, stages, evidence = self._build("remote-frames")
        frozen = {
            _identity_digest(identity): _core(identity)
            for identity in trial["representation_identities"]
        }
        without_infer = [row for row in stages if row["action"] != "infer"]
        with self.assertRaises(FlowMeshSemanticTrialError):
            _model_input_frontier(
                bound_stages=without_infer,
                frozen_identity_digests=frozen,
            )
        extra = dict(stages[-1])
        extra["stage_key"] = stages[-1]["stage_key"] + "-second"
        with self.assertRaises(FlowMeshSemanticTrialError):
            _model_input_frontier(
                bound_stages=[*stages, extra],
                frozen_identity_digests=frozen,
            )
