"""Offline tests for the one-task FlowMesh full-flow integration."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from typing import Any, Callable, Mapping
from unittest import mock

from pathfinder.distributed.scoring import (
    MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE,
    load_workload_scoring_contract,
    render_workload_question,
)
from pathfinder.frame_bundle_ingest import FRAME_BUNDLE_MEDIA_TYPE
from pathfinder.integrations.flowmesh.contracts import (
    FlowMeshSettings,
    FlowMeshWorkerIdentity,
    SubmittedWorkflow,
    TerminalWorkflow,
    WorkflowValidation,
)
from pathfinder.integrations.flowmesh.full_flow_trial import (
    FULL_FLOW_DEPLOYMENT_BINDING_SCHEMA_VERSION,
    FlowMeshFullFlowTrialError,
    build_flowmesh_full_flow_trial_workflow,
    build_full_flow_deployment_binding,
    plan_flowmesh_full_flow_trial,
    run_flowmesh_full_flow_trial,
    verify_flowmesh_full_flow_trial_plan,
    verify_flowmesh_full_flow_trial_run,
)
from pathfinder.simulator.data_agent_semantic_vertical import (
    DATA_AGENT_SEMANTIC_SPEC_SCHEMA_VERSION,
)
from pathfinder.simulator.container_node import (
    FULL_FLOW_INGRESS_SIGNATURE_HEADER,
    ContainerNodeRuntime,
)
from pathfinder.simulator.full_flow_data_plane import (
    FullFlowArtifactBinding,
    build_full_flow_data_plane_package,
)
from pathfinder.simulator.full_flow_runtime import (
    FULL_FLOW_EVIDENCE_SCHEMA_VERSION,
    FULL_FLOW_REQUEST_SCHEMA_VERSION,
    FullFlowRouteConfig,
    FullFlowTrialRuntime,
)
from tests.test_data_agent_semantic_vertical import _bundle_bytes
from tests.test_simulator_full_flow_runtime import (
    FakeDataAgentClient,
    FakeSemanticAdapter,
)


TRIAL_KEY = "scenario-v1|smoke-descriptive|D2|r0000"
OBJECT_ID = "nextqa-val-0000000001"
CATALOG_VERSION = "catalog-origin-n4-v1"
MODEL = "qwen3.8-27b"
WORKER_ALIAS = "pathfinder-cost-aware"
INGRESS_SECRET = "test-only-full-flow-ingress-secret"


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _write_json(path: Path, value: Any) -> Path:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def _semantic_spec(raw: bytes, **overrides: Any) -> dict[str, Any]:
    value: dict[str, Any] = {
        "schema_version": DATA_AGENT_SEMANTIC_SPEC_SCHEMA_VERSION,
        "semantic_run_id": "full-flow-semantic-smoke-v1",
        "trial_key": TRIAL_KEY,
        "semantic_executor_node_id": "N6",
        "representation_id": "sampled_frame_bundle",
        "workload_id": "smoke-descriptive",
        "task_class_id": "video_qa",
        "artifact_object_id": OBJECT_ID,
        "question": "Which option describes the main action?",
        "answer_options": [
            {"option_id": "A", "text": "A person cooks food."},
            {"option_id": "B", "text": "Two musicians perform."},
            {"option_id": "C", "text": "A vehicle crosses a river."},
        ],
        "correct_answer_id": "B",
        "expected_model": MODEL,
        "success_scoring_rule": (
            MULTIPLE_CHOICE_CANONICAL_OPTION_SCORING_RULE
        ),
        "data_agent_route_design_id": "D_origin_remote",
        "data_agent_plan_id": "D_origin_remote",
        "data_agent_plan_epoch": 7,
        "artifact_sha256": _sha256(raw),
        "artifact_size_bytes": len(raw),
        "object_catalog_version": CATALOG_VERSION,
        "credentials_recorded": False,
    }
    value.update(overrides)
    return value


def _prepare_inputs(
    root: Path,
    *,
    semantic_overrides: Mapping[str, Any] | None = None,
    deployment_url: str = "http://127.0.0.1:19087",
) -> tuple[Path, Path, Path]:
    raw = _bundle_bytes()
    artifact = root / "sampled-frame-bundle.tar"
    artifact.write_bytes(raw)
    spec = _write_json(
        root / "semantic.json",
        _semantic_spec(raw, **dict(semantic_overrides or {})),
    )
    data_plane = root / "data-plane"
    build_full_flow_data_plane_package(
        [
            FullFlowArtifactBinding(
                object_id=OBJECT_ID,
                artifact_path=artifact,
                catalog_version=CATALOG_VERSION,
                plan_ids=("D_origin_remote",),
                semantic_spec_paths=(spec,),
            )
        ],
        output_dir=data_plane,
        package_id="full-flow-data-plane-test-v1",
    )
    deployment = root / "deployment.json"
    build_full_flow_deployment_binding(
        deployment_binding_id="local-eight-node-v1",
        coordinator_api_url=deployment_url,
        worker_alias=WORKER_ALIAS,
        api_task_timeout_seconds=900,
        output_path=deployment,
    )
    return spec, data_plane, deployment


def _plan(root: Path) -> Path:
    spec, data_plane, deployment = _prepare_inputs(root)
    plan_flowmesh_full_flow_trial(
        semantic_spec=spec,
        data_plane_package=data_plane,
        deployment_binding=deployment,
        output_dir=root / "plan",
        owner="pathfinder",
    )
    return root / "plan"


def _plan_documents(plan: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    logical = json.loads(
        (plan / "flowmesh-full-flow-logical-plan.json").read_text(
            encoding="utf-8"
        )
    )
    deployment = json.loads(
        (plan / "flowmesh-full-flow-deployment-binding.json").read_text(
            encoding="utf-8"
        )
    )
    return logical, deployment


def _evidence(
    request: Mapping[str, Any],
    expected_artifact: Mapping[str, Any],
) -> dict[str, Any]:
    route = FullFlowRouteConfig(
        route_id=request["route_id"],
        requested_location="origin-warm",
        data_agent_plan_id="D_origin_remote",
        data_agent_plan_epoch=7,
    )
    final_answer = "B"
    contract = load_workload_scoring_contract(
        {
            "object_id": request["object_id"],
            "question": request["question"],
            "answer_options": request["answer_options"],
            "correct_answer_id": request["correct_answer_id"],
        },
        request["success_scoring_rule"],
        name="fake full-flow evidence",
    )
    rendered_question = render_workload_question(
        {"question": request["question"]},
        contract,
    )
    question_sha256 = _sha256(rendered_question.encode("utf-8"))
    prompt = ContainerNodeRuntime.build_semantic_vision_prompt(
        request["representation_id"],
        expected_artifact["frame_count"],
        rendered_question,
    )
    prompt_sha256 = _sha256(prompt.encode("utf-8"))
    frame_sequence_sha256 = "5" * 64
    semantic_request_id = _sha256(_canonical_bytes({
        "full_flow_request_id": request["full_flow_request_id"],
        "trial_key": request["trial_key"],
        "execution_node_id": "N6",
        "representation_id": request["representation_id"],
        "representation_sha256": request["artifact_sha256"],
        "question_sha256": question_sha256,
        "prompt_sha256": prompt_sha256,
        "frame_sequence_sha256": frame_sequence_sha256,
    }))
    total_jpeg_bytes = 100
    frame_count = expected_artifact["frame_count"]
    frame_sizes = [total_jpeg_bytes // frame_count] * frame_count
    frame_sizes[-1] += total_jpeg_bytes - sum(frame_sizes)
    frame_metadata = [
        {
            "frame_index": index,
            "timestamp_seconds": float(index),
            "width": 2,
            "height": 2,
            "jpeg_size_bytes": frame_sizes[index],
            "jpeg_sha256": f"{index + 1:x}" * 64,
        }
        for index in range(frame_count)
    ]
    return {
        "schema_version": FULL_FLOW_EVIDENCE_SCHEMA_VERSION,
        "status": "COMPLETE",
        "full_flow_request_id": request["full_flow_request_id"],
        "request_sha256": _sha256(_canonical_bytes(request)),
        "frozen_binding_sha256": request["frozen_binding_sha256"],
        "route_config_sha256": route.sha256,
        "idempotent_replay": False,
        "run_id": request["run_id"],
        "trial_id": request["trial_id"],
        "trial_key": request["trial_key"],
        "workload_id": request["workload_id"],
        "task_class_id": request["task_class_id"],
        "object_id": request["object_id"],
        "representation_id": request["representation_id"],
        "route": {
            "route_id": route.route_id,
            "source_node_id": "N4",
            "executor_node_id": "N7",
            "inference_node_id": "N6",
            "requested_location": "origin-warm",
        },
        "data_agent": {
            "access_id": "full-flow-access-test-v1",
            "source_node_id": "N4",
            "source_identity_basis": "data-agent-health-before-and-after",
            "source_identity_verified": True,
            "health_sha256_before": "7" * 64,
            "health_sha256_after": "7" * 64,
            "plan_id": "D_origin_remote",
            "plan_epoch": 7,
            "object_catalog_version": request["object_catalog_version"],
            "artifact_media_type": FRAME_BUNDLE_MEDIA_TYPE,
            "artifact_size_bytes": request["artifact_size_bytes"],
            "artifact_sha256": request["artifact_sha256"],
            "manifest_sha256": expected_artifact["manifest_sha256"],
            "frame_count": expected_artifact["frame_count"],
            "total_jpeg_bytes": total_jpeg_bytes,
            "delivery": {
                "telemetry_supported": True,
                "telemetry_complete": True,
                "in_flight_request_count": 0,
                "download_request_count": 1,
                "completed_request_count": 1,
                "full_download_count": 1,
                "bytes_sent": request["artifact_size_bytes"],
                "artifact_size_bytes": request["artifact_size_bytes"],
                # A completed transfer may lack a server-side latency sample;
                # exact byte/download accounting remains mandatory.
                "server_reported_transfer_latency_ms": None,
                "exactly_one_full_download": True,
                "bytes_sent_equals_artifact_size": True,
                "telemetry_object_id": request["object_id"],
                "telemetry_object_catalog_version": request[
                    "object_catalog_version"
                ],
                "expected_object_catalog_version": request[
                    "object_catalog_version"
                ],
            },
            "latency_ms": {
                "data_agent_service": 2.0,
                "client_access_round_trip": 3.0,
                "artifact_download_elapsed": 1.0,
                "server_reported_transfer": 1.0,
            },
        },
        "semantic": {
            "semantic_request_id": semantic_request_id,
            "request_sha256": "2" * 64,
            "question_sha256": question_sha256,
            "prompt_sha256": prompt_sha256,
            "frame_sequence_sha256": frame_sequence_sha256,
            "frame_metadata": frame_metadata,
            "representation_delivery_bytes": total_jpeg_bytes,
            "model": request["expected_model"],
            "runtime_epoch": "6" * 32,
            "service_time_ms": 10.0,
            "adapter_idempotent_replay": False,
        },
        "scoring": {
            "success_scoring_rule": request["success_scoring_rule"],
            "answer_option_ids": [
                item["option_id"] for item in request["answer_options"]
            ],
            "answer_options_sha256": _sha256(
                _canonical_bytes(request["answer_options"])
            ),
            "correct_answer_id": request["correct_answer_id"],
            "final_answer": final_answer,
            "final_answer_sha256": _sha256(final_answer.encode("utf-8")),
            "task_success": True,
        },
        "real_object_identity_verified": True,
        "data_agent_source_identity_verified": True,
        "data_agent_artifact_delivery_verified": True,
        "semantic_frame_payload_integrity_verified": True,
        "container_semantic_response_consistency_verified": True,
        "semantic_health_verified": True,
        "scoring_verified": True,
        "route_unified": True,
        "llm_called": True,
        "telemetry_complete": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


class FakeFlowMeshClient:
    def __init__(
        self,
        expected_artifact: Mapping[str, Any],
        *,
        mutate_result: Callable[[dict[str, Any]], None] | None = None,
        assigned_worker: str = "wkr-42",
        full_flow_runtime: FullFlowTrialRuntime | None = None,
    ) -> None:
        self.expected_artifact = dict(expected_artifact)
        self.mutate_result = mutate_result
        self.assigned_worker = assigned_worker
        self.full_flow_runtime = full_flow_runtime
        self.describe_current_worker_calls = 0
        self.validated: list[dict[str, Any]] = []
        self.submitted: dict[str, Any] | None = None
        self.result: dict[str, Any] | None = None

    def describe_current_worker(
        self,
        *,
        worker_id: str | None = None,
        alias: str | None = None,
    ) -> FlowMeshWorkerIdentity:
        self.describe_current_worker_calls += 1
        if worker_id is not None or alias != WORKER_ALIAS:
            raise RuntimeError("unexpected worker selector")
        return FlowMeshWorkerIdentity(
            worker_id="wkr-42",
            alias=alias,
            status="IDLE",
            namespace="test",
            cluster="test",
            node_alias="test-node",
        )

    def validate(self, workflow: Mapping[str, Any]) -> WorkflowValidation:
        self.validated.append(json.loads(json.dumps(workflow)))
        return WorkflowValidation(ok=True)

    def submit(self, workflow: Mapping[str, Any]) -> SubmittedWorkflow:
        self.submitted = json.loads(json.dumps(workflow))
        nodes = self.submitted["spec"]["graph"]["nodes"]
        if len(nodes) != 1:
            raise AssertionError("full-flow workflow must have one task")
        request = nodes[0]["spec"]["api"]["body"]
        result = (
            dict(self.full_flow_runtime.execute(request))
            if self.full_flow_runtime is not None
            else _evidence(request, self.expected_artifact)
        )
        if self.mutate_result is not None:
            self.mutate_result(result)
        self.result = {
            "executor": "api",
            "ok": True,
            "status_code": 200,
            "text": json.dumps(result, sort_keys=True),
        }
        return SubmittedWorkflow("wfl-full-flow", ("tsk-full-flow",))

    def wait(
        self,
        workflow_id: str,
        poll_interval_seconds: float,
    ) -> TerminalWorkflow:
        return TerminalWorkflow(workflow_id, "DONE")

    def retrieve_result(self, task_id: str) -> dict[str, Any]:
        if task_id != "tsk-full-flow" or self.result is None:
            raise AssertionError("unexpected result request")
        return self.result

    def describe_task_failure(self, task_id: str) -> dict[str, Any]:
        return {
            "task_status": "DONE",
            "assigned_worker": self.assigned_worker,
        }


class FullFlowPlanTest(unittest.TestCase):
    def test_builds_deterministic_dedicated_deployment_binding(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "binding.json"
            document = build_full_flow_deployment_binding(
                deployment_binding_id="deployment-v1",
                coordinator_api_url="http://127.0.0.1:19087/",
                worker_alias=WORKER_ALIAS,
                api_task_timeout_seconds=900,
                output_path=path,
            )
            self.assertEqual(
                FULL_FLOW_DEPLOYMENT_BINDING_SCHEMA_VERSION,
                document["schema_version"],
            )
            self.assertEqual("http://127.0.0.1:19087", document[
                "coordinator_api_url"
            ])
            self.assertEqual(document, json.loads(path.read_text()))

    def test_logical_plan_is_endpoint_free_and_uses_runtime_request(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            plan = _plan(root)
            verified = verify_flowmesh_full_flow_trial_plan(plan)
            self.assertEqual("VERIFIED", verified["status"])
            self.assertEqual(OBJECT_ID, verified["object_id"])
            self.assertEqual("N4", verified["source_node_id"])
            self.assertEqual("N7", verified["execution_node_id"])
            self.assertEqual("N6", verified["semantic_executor_node_id"])
            logical, deployment = _plan_documents(plan)
            logical_text = json.dumps(logical)
            self.assertEqual(
                FULL_FLOW_REQUEST_SCHEMA_VERSION,
                logical["request_body"]["schema_version"],
            )
            self.assertNotIn("http://", logical_text)
            self.assertNotIn("https://", logical_text)
            self.assertNotIn(WORKER_ALIAS, logical_text)
            self.assertEqual(WORKER_ALIAS, deployment["worker_alias"])

    def test_plan_binds_canonical_packaged_spec_not_host_encoding(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            spec, data_plane, deployment = _prepare_inputs(root)
            source_document = json.loads(spec.read_text(encoding="utf-8"))
            spec.write_bytes(
                json.dumps(
                    source_document,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")
            )
            output = root / "encoded-plan"
            plan_flowmesh_full_flow_trial(
                semantic_spec=spec,
                data_plane_package=data_plane,
                deployment_binding=deployment,
                output_dir=output,
            )
            logical, _ = _plan_documents(output)
            package = json.loads(
                (data_plane / "full-flow-data-plane.json").read_text(
                    encoding="utf-8"
                )
            )
            packaged_spec_sha256 = package["objects"][0][
                "semantic_specs"
            ][0]["sha256"]
            self.assertEqual(
                packaged_spec_sha256,
                logical["semantic_spec_sha256"],
            )
            self.assertNotEqual(_sha256(spec.read_bytes()), packaged_spec_sha256)

    def test_template_is_one_static_non_submittable_n7_api_task(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            plan = _plan(Path(temporary))
            template = json.loads(
                (
                    plan / "flowmesh-full-flow-workflow-template.json"
                ).read_text(encoding="utf-8")
            )
            nodes = template["spec"]["graph"]["nodes"]
            self.assertEqual(1, len(nodes))
            api = nodes[0]["spec"]["api"]
            self.assertEqual(
                "http://127.0.0.1:19087/v1/pathfinder/trials/execute",
                api["url"],
            )
            self.assertEqual(900, api["timeout_sec"])
            self.assertEqual(
                {"Content-Type": "application/json"},
                api["headers"],
            )
            self.assertEqual(
                FULL_FLOW_REQUEST_SCHEMA_VERSION,
                api["body"]["schema_version"],
            )
            annotations = template["metadata"]["annotations"]
            self.assertNotIn("schedule_hint", annotations)
            self.assertTrue(annotations["custom"][
                "pathfinder_workflow_is_not_submittable"
            ])

    def test_runtime_builder_pins_worker_and_preserves_body(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            plan = _plan(Path(temporary))
            logical, deployment = _plan_documents(plan)
            workflow = build_flowmesh_full_flow_trial_workflow(
                logical,
                deployment,
                selected_worker_id="wkr-42",
            )
            self.assertEqual(
                {"selected_worker": "wkr-42"},
                workflow["metadata"]["annotations"]["schedule_hint"],
            )
            body = workflow["spec"]["graph"]["nodes"][0]["spec"][
                "api"
            ]["body"]
            self.assertEqual(logical["request_body"], body)
            self.assertNotIn(
                FULL_FLOW_INGRESS_SIGNATURE_HEADER,
                workflow["spec"]["graph"]["nodes"][0]["spec"]["api"][
                    "headers"
                ],
            )

    def test_unbound_semantic_spec_and_plain_remote_http_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            spec, data_plane, deployment = _prepare_inputs(root)
            changed = json.loads(spec.read_text())
            changed["semantic_run_id"] = "different-semantic-run"
            changed_spec = _write_json(root / "changed.json", changed)
            with self.assertRaisesRegex(
                FlowMeshFullFlowTrialError,
                "not uniquely bound",
            ):
                plan_flowmesh_full_flow_trial(
                    semantic_spec=changed_spec,
                    data_plane_package=data_plane,
                    deployment_binding=deployment,
                    output_dir=root / "bad-plan",
                )
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(
                FlowMeshFullFlowTrialError,
                "must use HTTPS",
            ):
                _prepare_inputs(
                    Path(temporary),
                    deployment_url="http://n7.internal:8080",
                )

    def test_plan_checksum_tampering_is_detected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            plan = _plan(Path(temporary))
            logical = plan / "flowmesh-full-flow-logical-plan.json"
            logical.write_bytes(logical.read_bytes() + b" ")
            with self.assertRaisesRegex(
                FlowMeshFullFlowTrialError,
                "checksum mismatch",
            ):
                verify_flowmesh_full_flow_trial_plan(plan)


class FullFlowRunTest(unittest.TestCase):
    @staticmethod
    def _settings() -> FlowMeshSettings:
        return FlowMeshSettings(
            base_url="https://root.test",
            worker_alias=WORKER_ALIAS,
            validate_before_submit=True,
            poll_interval_seconds=0.01,
        )

    @staticmethod
    def _client(
        plan: Path,
        *,
        mutate_result: Callable[[dict[str, Any]], None] | None = None,
        assigned_worker: str = "wkr-42",
    ) -> FakeFlowMeshClient:
        logical, _ = _plan_documents(plan)
        return FakeFlowMeshClient(
            logical["expected_artifact"],
            mutate_result=mutate_result,
            assigned_worker=assigned_worker,
        )

    def test_fake_run_and_offline_verifier_cover_the_real_object_route(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            plan = _plan(root)
            client = self._client(plan)
            result = run_flowmesh_full_flow_trial(
                plan_dir=plan,
                output_dir=root / "run",
                client=client,
                settings=self._settings(),
                full_flow_ingress_hmac_secret=INGRESS_SECRET,
            )
            self.assertEqual("COMPLETE", result["status"])
            self.assertEqual(1, len(client.validated))
            self.assertEqual(
                1,
                len(client.submitted["spec"]["graph"]["nodes"]),
            )
            submitted_headers = client.submitted["spec"]["graph"]["nodes"][0][
                "spec"
            ]["api"]["headers"]
            self.assertRegex(
                submitted_headers[FULL_FLOW_INGRESS_SIGNATURE_HEADER],
                r"^[0-9a-f]{64}$",
            )
            self.assertNotIn(INGRESS_SECRET, json.dumps(client.submitted))
            self.assertEqual(OBJECT_ID, result["object_id"])
            self.assertTrue(result["route_unified"])
            self.assertFalse(result["host_artifact_materialized"])
            verified = verify_flowmesh_full_flow_trial_run(
                root / "run",
                plan_dir=plan,
            )
            self.assertEqual("VERIFIED", verified["status"])
            self.assertTrue(verified["plan_binding_checked"])
            self.assertEqual(OBJECT_ID, verified["object_id"])
            self.assertFalse(verified["host_artifact_materialized"])

            durable = "".join(
                path.read_text(encoding="utf-8")
                for path in (root / "run").glob("*.json")
            )
            for forbidden in (
                "http://",
                "https://",
                "jpeg_base64",
                "frame_bytes",
                "artifact_payload",
                "artifact_path",
            ):
                self.assertNotIn(forbidden, durable)

    def test_missing_runtime_hmac_fails_before_any_flowmesh_call(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            plan = _plan(root)
            client = self._client(plan)
            environment = dict(os.environ)
            environment.pop(
                "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET",
                None,
            )
            with (
                mock.patch.dict(os.environ, environment, clear=True),
                self.assertRaisesRegex(
                    FlowMeshFullFlowTrialError,
                    "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET is required",
                ),
            ):
                run_flowmesh_full_flow_trial(
                    plan_dir=plan,
                    output_dir=root / "missing-auth-run",
                    client=client,
                    settings=self._settings(),
                )
            self.assertEqual(0, client.describe_current_worker_calls)
            self.assertEqual([], client.validated)
            self.assertIsNone(client.submitted)
            self.assertFalse((root / "missing-auth-run").exists())

    def test_current_n7_runtime_evidence_contract_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            plan = _plan(root)
            raw = _bundle_bytes()
            runtime = FullFlowTrialRuntime(
                route_config=FullFlowRouteConfig(
                    route_id="D_origin_remote",
                    requested_location="origin-warm",
                    data_agent_plan_id="D_origin_remote",
                    data_agent_plan_epoch=7,
                ),
                data_agent_client=FakeDataAgentClient(
                    raw,
                    catalog_version=CATALOG_VERSION,
                ),
                semantic_adapter=FakeSemanticAdapter(model=MODEL),
            )
            logical, _ = _plan_documents(plan)
            client = FakeFlowMeshClient(
                logical["expected_artifact"],
                full_flow_runtime=runtime,
            )
            completed = run_flowmesh_full_flow_trial(
                plan_dir=plan,
                output_dir=root / "run",
                client=client,
                settings=self._settings(),
                full_flow_ingress_hmac_secret=INGRESS_SECRET,
            )
            self.assertEqual("COMPLETE", completed["status"])
            self.assertTrue(completed["route_unified"])
            self.assertFalse(completed["host_artifact_materialized"])

    def test_false_required_claim_fails_before_output(self) -> None:
        for field in (
            "real_object_identity_verified",
            "data_agent_source_identity_verified",
            "data_agent_artifact_delivery_verified",
            "semantic_health_verified",
            "scoring_verified",
            "route_unified",
            "llm_called",
            "telemetry_complete",
        ):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                plan = _plan(root)
                client = self._client(
                    plan,
                    mutate_result=lambda value, key=field: value.__setitem__(
                        key, False
                    ),
                )
                with self.assertRaisesRegex(
                    FlowMeshFullFlowTrialError,
                    field,
                ):
                    run_flowmesh_full_flow_trial(
                        plan_dir=plan,
                        output_dir=root / "run",
                        client=client,
                        settings=self._settings(),
                        full_flow_ingress_hmac_secret=INGRESS_SECRET,
                    )
                self.assertFalse((root / "run").exists())

    def test_wrong_object_model_and_score_are_rejected(self) -> None:
        mutations: tuple[Callable[[dict[str, Any]], None], ...] = (
            lambda value: value.__setitem__("object_id", "wrong-object"),
            lambda value: value["data_agent"].__setitem__(
                "source_node_id", "N3"
            ),
            lambda value: value["data_agent"].__setitem__(
                "source_identity_verified", False
            ),
            lambda value: value["semantic"].__setitem__(
                "model", "different-model"
            ),
            lambda value: value["scoring"].__setitem__(
                "task_success", False
            ),
        )
        for mutation in mutations:
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                plan = _plan(root)
                with self.assertRaises(FlowMeshFullFlowTrialError):
                    run_flowmesh_full_flow_trial(
                        plan_dir=plan,
                        output_dir=root / "run",
                        client=self._client(plan, mutate_result=mutation),
                        settings=self._settings(),
                        full_flow_ingress_hmac_secret=INGRESS_SECRET,
                    )

    def test_payload_field_and_worker_mismatch_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            plan = _plan(root)
            with self.assertRaisesRegex(
                FlowMeshFullFlowTrialError,
                "fields changed",
            ):
                run_flowmesh_full_flow_trial(
                    plan_dir=plan,
                    output_dir=root / "run-payload",
                    client=self._client(
                        plan,
                        mutate_result=lambda value: value.__setitem__(
                            "artifact_payload", "forbidden"
                        ),
                    ),
                    settings=self._settings(),
                    full_flow_ingress_hmac_secret=INGRESS_SECRET,
                )
            with self.assertRaisesRegex(
                FlowMeshFullFlowTrialError,
                "pinned worker",
            ):
                run_flowmesh_full_flow_trial(
                    plan_dir=plan,
                    output_dir=root / "run-worker",
                    client=self._client(
                        plan,
                        assigned_worker="wkr-other",
                    ),
                    settings=self._settings(),
                    full_flow_ingress_hmac_secret=INGRESS_SECRET,
                )

    def test_run_checksum_tampering_is_detected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            plan = _plan(root)
            run_flowmesh_full_flow_trial(
                plan_dir=plan,
                output_dir=root / "run",
                client=self._client(plan),
                settings=self._settings(),
                full_flow_ingress_hmac_secret=INGRESS_SECRET,
            )
            report = root / "run" / "flowmesh-full-flow-run.json"
            report.write_bytes(report.read_bytes() + b" ")
            with self.assertRaisesRegex(
                FlowMeshFullFlowTrialError,
                "checksum mismatch",
            ):
                verify_flowmesh_full_flow_trial_run(
                    root / "run",
                    plan_dir=plan,
                )


if __name__ == "__main__":
    unittest.main()
