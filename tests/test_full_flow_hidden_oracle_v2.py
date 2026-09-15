"""Label-free N1 scoring integration for the full-flow v2 boundary."""

from __future__ import annotations

import http.client
import json
import os
import tempfile
import unittest
from hashlib import sha256
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest import mock

from pathfinder.data_agent_server import (
    DataAgentServerSettings,
    create_data_agent_http_server,
)
from pathfinder.distributed.scoring import MULTIPLE_CHOICE_EXACT_SCORING_RULE
from pathfinder.integrations.flowmesh.full_flow_trial import (
    FlowMeshFullFlowTrialError,
    build_flowmesh_full_flow_trial_v2_workflow,
    build_full_flow_deployment_binding,
    plan_flowmesh_full_flow_trial_v2,
    run_flowmesh_full_flow_trial_v2,
    verify_flowmesh_full_flow_trial_v2_plan,
    verify_flowmesh_full_flow_trial_v2_run,
)
from pathfinder.integrations.flowmesh.contracts import FlowMeshSettings
from pathfinder.simulator.container_node import (
    FULL_FLOW_INGRESS_SIGNATURE_HEADER,
    ContainerNodeError,
    _full_flow_runtime_from_environment,
    create_container_node_server,
    full_flow_request_hmac_sha256,
)
from pathfinder.simulator.full_flow_data_plane import (
    DATA_AGENT_MANIFEST_PATH,
    FullFlowArtifactBinding,
    build_full_flow_data_plane_package,
)
from pathfinder.simulator.full_flow_runtime import (
    FULL_FLOW_EVIDENCE_V2_SCHEMA_VERSION,
    FULL_FLOW_REQUEST_V2_SCHEMA_VERSION,
    FullFlowHttpConfig,
    FullFlowRouteConfig,
    FullFlowRuntimeError,
    FullFlowTrialRuntime,
    build_full_flow_trial_request_v2,
    build_http_full_flow_runtime,
)
from pathfinder.simulator.hidden_oracle import (
    N1_LABEL_SOURCE_SCHEMA_VERSION,
    N1HiddenOracleService,
    assert_hidden_oracle_fields_absent,
    build_n1_oracle_package,
    build_n1_public_task_binding,
    create_n1_oracle_http_server,
)
from tests.test_simulator_full_flow_http_integration import (
    _VisionLLMHandler,
    _serving,
)
from tests.test_simulator_full_flow_runtime import _bundle_bytes
from tests.test_simulator_full_flow_runtime import (
    FakeDataAgentClient,
    FakeSemanticAdapter,
)
from tests.test_flowmesh_full_flow_trial import FakeFlowMeshClient


OBJECT_ID = "nextqa-val-0000000001"
CATALOG_VERSION = "hidden-oracle-full-flow-v2"
PLAN_ID = "D_origin_remote"
MODEL = "local-vision-model"
ORACLE_ID = "nextqa-hidden-oracle-v2"
DATA_AGENT_TOKEN = "test-only-data-agent-token"
ARTIFACT_SECRET = "test-only-artifact-secret"
ORACLE_TOKEN = "test-only-oracle-token"
ORACLE_EVIDENCE_SECRET = b"test-only-n1-evidence-secret-at-least-32-bytes"
LLM_API_KEY = "test-only-llm-key"
SEMANTIC_TOKEN = "test-only-semantic-bearer-token"
INGRESS_SECRET = "test-only-full-flow-ingress-secret"
QUESTION = "Which option describes the main action?"
OPTIONS = [
    {"option_id": "A", "text": "A person cooks."},
    {"option_id": "B", "text": "Musicians perform."},
    {"option_id": "C", "text": "A vehicle moves."},
]


def _public_task() -> dict[str, object]:
    return build_n1_public_task_binding(
        workload_id="visible-video-qa-v2",
        object_id=OBJECT_ID,
        task_class_id="video_qa",
        question=QUESTION,
        answer_options=OPTIONS,
        success_scoring_rule=MULTIPLE_CHOICE_EXACT_SCORING_RULE,
    )


def _request(
    route: FullFlowRouteConfig,
    raw_bundle: bytes,
) -> dict[str, object]:
    public = _public_task()
    return build_full_flow_trial_request_v2(
        route_config=route,
        full_flow_request_id="full-flow-hidden-oracle-request-v2",
        run_id="full-flow-hidden-oracle-run-v2",
        trial_id="full-flow-hidden-oracle-trial-v2",
        trial_key="scenario-v2|W1|D2|r0000",
        workload_id=str(public["workload_id"]),
        task_class_id=str(public["task_class_id"]),
        object_id=OBJECT_ID,
        artifact_sha256=sha256(raw_bundle).hexdigest(),
        artifact_size_bytes=len(raw_bundle),
        object_catalog_version=CATALOG_VERSION,
        expected_model=MODEL,
        question=QUESTION,
        answer_options=OPTIONS,
        success_scoring_rule=MULTIPLE_CHOICE_EXACT_SCORING_RULE,
        oracle_id=ORACLE_ID,
        task_binding_sha256=str(public["task_binding_sha256"]),
    )


def _freeze_oracle(root: Path) -> tuple[Path, dict[str, object]]:
    public = _public_task()
    label_source = root / "hidden-label-source.json"
    label_source.write_text(
        json.dumps(
            {
                "schema_version": N1_LABEL_SOURCE_SCHEMA_VERSION,
                "oracle_id": ORACLE_ID,
                "logical_node_id": "N1",
                "labels": [
                    {
                        "object_id": OBJECT_ID,
                        "task_binding_sha256": public[
                            "task_binding_sha256"
                        ],
                        "success_scoring_rule": (
                            MULTIPLE_CHOICE_EXACT_SCORING_RULE
                        ),
                        "answer_option_ids": ["A", "B", "C"],
                        "correct_answer_id": "B",
                    }
                ],
                "credentials_recorded": False,
            },
            sort_keys=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    package = root / "oracle-package"
    result = build_n1_oracle_package(label_source, output_dir=package)
    return package, result


class FullFlowHiddenOracleV2Test(unittest.TestCase):
    def setUp(self) -> None:
        self.route = FullFlowRouteConfig(
            route_id=PLAN_ID,
            requested_location="origin-warm",
            data_agent_plan_id=PLAN_ID,
            data_agent_plan_epoch=0,
        )

    def test_public_request_and_flowmesh_body_have_no_hidden_label(self) -> None:
        request = _request(self.route, _bundle_bytes())
        self.assertEqual(
            FULL_FLOW_REQUEST_V2_SCHEMA_VERSION,
            request["schema_version"],
        )
        self.assertNotIn("correct_answer_id", request)
        deployment = build_full_flow_deployment_binding(
            deployment_binding_id="hidden-oracle-v2-deployment",
            coordinator_api_url="http://127.0.0.1:19087",
            worker_alias="pathfinder-test-worker",
            api_task_timeout_seconds=300,
        )
        workflow = build_flowmesh_full_flow_trial_v2_workflow(
            request,
            self.route,
            deployment,
            selected_worker_id="wkr-test-v2",
        )
        assert_hidden_oracle_fields_absent(workflow)
        serialized = json.dumps(workflow, sort_keys=True)
        self.assertNotIn("correct_answer_id", serialized)
        body = workflow["spec"]["graph"]["nodes"][0]["spec"]["api"]["body"]
        self.assertEqual(request, body)

    def test_wrong_task_binding_and_hidden_field_fail_before_workflow(self) -> None:
        raw = _bundle_bytes()
        public = _public_task()
        with self.assertRaisesRegex(FullFlowRuntimeError, "task_binding"):
            build_full_flow_trial_request_v2(
                route_config=self.route,
                full_flow_request_id="invalid-binding-request-v2",
                run_id="invalid-binding-run-v2",
                trial_id="invalid-binding-trial-v2",
                trial_key="scenario-v2|W1|D2|r0001",
                workload_id=str(public["workload_id"]),
                object_id=OBJECT_ID,
                artifact_sha256=sha256(raw).hexdigest(),
                artifact_size_bytes=len(raw),
                object_catalog_version=CATALOG_VERSION,
                expected_model=MODEL,
                question=QUESTION,
                answer_options=OPTIONS,
                success_scoring_rule=MULTIPLE_CHOICE_EXACT_SCORING_RULE,
                oracle_id=ORACLE_ID,
                task_binding_sha256="0" * 64,
            )
        request = _request(self.route, raw)
        request["correct_answer_id"] = "B"
        deployment = build_full_flow_deployment_binding(
            deployment_binding_id="hidden-oracle-v2-deployment",
            coordinator_api_url="http://127.0.0.1:19087",
            worker_alias="pathfinder-test-worker",
            api_task_timeout_seconds=300,
        )
        with self.assertRaises((FlowMeshFullFlowTrialError, FullFlowRuntimeError)):
            build_flowmesh_full_flow_trial_v2_workflow(
                request,
                self.route,
                deployment,
                selected_worker_id="wkr-test-v2",
            )

    def test_environment_binds_n1_only_as_ephemeral_runtime_config(self) -> None:
        environment = {
            "PATHFINDER_FULL_FLOW_ENABLED": "1",
            "PATHFINDER_FULL_FLOW_SOURCE_NODE_ID": "N4",
            "PATHFINDER_FULL_FLOW_EXECUTOR_NODE_ID": "N7",
            "PATHFINDER_FULL_FLOW_INFERENCE_NODE_ID": "N6",
            "PATHFINDER_FULL_FLOW_SCORING_NODE_ID": "N1",
            "PATHFINDER_FULL_FLOW_ROUTE_ID": PLAN_ID,
            "PATHFINDER_FULL_FLOW_REQUESTED_LOCATION": "origin-warm",
            "PATHFINDER_FULL_FLOW_DATA_AGENT_PLAN_ID": PLAN_ID,
            "PATHFINDER_FULL_FLOW_DATA_AGENT_BASE_URL": (
                "http://pathfinder-sim-n4-data-agent:8780"
            ),
            "PATHFINDER_FULL_FLOW_SEMANTIC_BASE_URL": (
                "http://pathfinder-sim-n6-inference:9080"
            ),
            "PATHFINDER_FULL_FLOW_ORACLE_BASE_URL": (
                "http://pathfinder-sim-n1-control:9080"
            ),
            "PATHFINDER_FULL_FLOW_ORACLE_ID": ORACLE_ID,
            "PATHFINDER_FULL_FLOW_ORACLE_PUBLIC_TASK_SET_SHA256": "a" * 64,
            "PATHFINDER_FULL_FLOW_ORACLE_TOKEN": ORACLE_TOKEN,
            "PATHFINDER_FULL_FLOW_SIMULATOR_PRIVATE_HOSTS": (
                "pathfinder-sim-n1-control,"
                "pathfinder-sim-n4-data-agent,"
                "pathfinder-sim-n6-inference"
            ),
            "PATHFINDER_DATA_AGENT_TOKEN": DATA_AGENT_TOKEN,
            "PATHFINDER_CONTAINER_NODE_TOKEN": SEMANTIC_TOKEN,
        }
        sentinel = object()
        with (
            mock.patch.dict(os.environ, environment, clear=True),
            mock.patch(
                "pathfinder.simulator.full_flow_runtime."
                "build_http_full_flow_runtime",
                return_value=sentinel,
            ) as build,
        ):
            result = _full_flow_runtime_from_environment("N7")
        self.assertIs(sentinel, result)
        http = build.call_args.kwargs["http_config"]
        self.assertEqual(ORACLE_ID, http.oracle_id)
        self.assertEqual("a" * 64, http.oracle_public_task_set_sha256)
        self.assertNotIn(ORACLE_TOKEN, repr(http))
        self.assertNotIn(DATA_AGENT_TOKEN, repr(http))

    def test_environment_refuses_partial_n1_runtime_binding(self) -> None:
        environment = {
            "PATHFINDER_FULL_FLOW_ENABLED": "1",
            "PATHFINDER_FULL_FLOW_SOURCE_NODE_ID": "N4",
            "PATHFINDER_FULL_FLOW_EXECUTOR_NODE_ID": "N7",
            "PATHFINDER_FULL_FLOW_INFERENCE_NODE_ID": "N6",
            "PATHFINDER_FULL_FLOW_SCORING_NODE_ID": "N1",
            "PATHFINDER_FULL_FLOW_ROUTE_ID": PLAN_ID,
            "PATHFINDER_FULL_FLOW_REQUESTED_LOCATION": "origin-warm",
            "PATHFINDER_FULL_FLOW_DATA_AGENT_PLAN_ID": PLAN_ID,
            "PATHFINDER_FULL_FLOW_DATA_AGENT_BASE_URL": (
                "http://pathfinder-sim-n4-data-agent:8780"
            ),
            "PATHFINDER_FULL_FLOW_SEMANTIC_BASE_URL": (
                "http://pathfinder-sim-n6-inference:9080"
            ),
            "PATHFINDER_FULL_FLOW_ORACLE_BASE_URL": (
                "http://pathfinder-sim-n1-control:9080"
            ),
            "PATHFINDER_FULL_FLOW_SIMULATOR_PRIVATE_HOSTS": (
                "pathfinder-sim-n1-control,"
                "pathfinder-sim-n4-data-agent,"
                "pathfinder-sim-n6-inference"
            ),
            "PATHFINDER_DATA_AGENT_TOKEN": DATA_AGENT_TOKEN,
        }
        with mock.patch.dict(os.environ, environment, clear=True):
            with self.assertRaisesRegex(
                ContainerNodeError,
                "PATHFINDER_FULL_FLOW_ORACLE_ID is required",
            ):
                _full_flow_runtime_from_environment("N7")

    def test_durable_v2_plan_run_and_offline_verifier_are_label_free(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = _bundle_bytes()
            artifact = root / "sampled-frame-bundle.tar"
            artifact.write_bytes(raw)
            data_plane = root / "data-plane"
            build_full_flow_data_plane_package(
                [
                    FullFlowArtifactBinding(
                        object_id=OBJECT_ID,
                        artifact_path=artifact,
                        catalog_version=CATALOG_VERSION,
                        plan_ids=(PLAN_ID,),
                    )
                ],
                output_dir=data_plane,
                package_id="hidden-oracle-durable-data-v2",
            )
            oracle_package, _ = _freeze_oracle(root)
            oracle = N1HiddenOracleService(
                oracle_package,
                state_db=root / "n1-durable.sqlite3",
                evidence_secret=ORACLE_EVIDENCE_SECRET,
            )
            deployment = build_full_flow_deployment_binding(
                deployment_binding_id="hidden-oracle-durable-v2",
                coordinator_api_url="http://127.0.0.1:19087",
                worker_alias="pathfinder-cost-aware",
                api_task_timeout_seconds=300,
            )
            request = _request(self.route, raw)
            plan_dir = root / "v2-plan"
            frozen = plan_flowmesh_full_flow_trial_v2(
                public_request=request,
                route_config=self.route,
                data_plane_package=data_plane,
                deployment_binding=deployment,
                output_dir=plan_dir,
            )
            self.assertEqual("N1", frozen["scoring_node_id"])
            verified_plan = verify_flowmesh_full_flow_trial_v2_plan(plan_dir)
            self.assertEqual("VERIFIED", verified_plan["status"])
            self.assertEqual(ORACLE_ID, verified_plan["oracle_id"])
            runtime = FullFlowTrialRuntime(
                route_config=self.route,
                data_agent_client=FakeDataAgentClient(
                    raw,
                    catalog_version=CATALOG_VERSION,
                ),
                semantic_adapter=FakeSemanticAdapter(model=MODEL),
                oracle_client=oracle,
            )
            logical = json.loads(
                (plan_dir / "flowmesh-full-flow-v2-logical-plan.json")
                .read_text(encoding="utf-8")
            )
            client = FakeFlowMeshClient(
                logical["expected_artifact"],
                full_flow_runtime=runtime,
            )
            settings = FlowMeshSettings(
                base_url="https://root.test",
                worker_alias="pathfinder-cost-aware",
                validate_before_submit=True,
                poll_interval_seconds=0.01,
            )
            run_dir = root / "v2-run"
            completed = run_flowmesh_full_flow_trial_v2(
                plan_dir=plan_dir,
                output_dir=run_dir,
                client=client,
                settings=settings,
                full_flow_ingress_hmac_secret=INGRESS_SECRET,
            )
            self.assertEqual("COMPLETE", completed["status"])
            self.assertTrue(completed["task_success"])
            verified_run = verify_flowmesh_full_flow_trial_v2_run(
                run_dir,
                plan_dir=plan_dir,
                n1_oracle_package_dir=oracle_package,
                n1_evidence_secret=ORACLE_EVIDENCE_SECRET,
            )
            self.assertEqual("VERIFIED", verified_run["status"])
            self.assertTrue(verified_run["oracle_hmac_verified"])
            self.assertEqual(ORACLE_ID, verified_run["oracle_id"])
            structural = verify_flowmesh_full_flow_trial_v2_run(
                run_dir,
                plan_dir=plan_dir,
            )
            self.assertEqual("STRUCTURALLY_VERIFIED", structural["status"])
            self.assertFalse(structural["oracle_hmac_verified"])
            self.assertIsNone(structural["task_success"])
            for directory in (plan_dir, run_dir):
                durable = b"".join(
                    path.read_bytes()
                    for path in directory.iterdir()
                    if path.is_file()
                )
                self.assertNotIn(b"correct_answer_id", durable)
                self.assertNotIn(ORACLE_TOKEN.encode("utf-8"), durable)

            record_path = run_dir / "flowmesh-full-flow-v2-task-record.json"
            record = json.loads(record_path.read_text(encoding="utf-8"))
            record["service_result"]["scoring"]["oracle_result"][
                "correct_answer_id"
            ] = "B"
            record["service_result_sha256"] = sha256(
                json.dumps(
                    record["service_result"],
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
            record_path.write_text(
                json.dumps(record, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )
            run_checksums = run_dir / "SHA256SUMS"
            run_lines = run_checksums.read_text(encoding="utf-8").splitlines()
            run_checksums.write_text(
                "\n".join(
                    (
                        f"{sha256(record_path.read_bytes()).hexdigest()}  "
                        f"{record_path.name}"
                        if line.endswith("  " + record_path.name)
                        else line
                    )
                    for line in run_lines
                )
                + "\n",
                encoding="utf-8",
            )
            with self.assertRaises(FlowMeshFullFlowTrialError):
                verify_flowmesh_full_flow_trial_v2_run(
                    run_dir,
                    plan_dir=plan_dir,
                )

            logical["request_body"]["correct_answer_id"] = "B"
            logical_path = plan_dir / "flowmesh-full-flow-v2-logical-plan.json"
            logical_path.write_text(
                json.dumps(logical, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )
            checksum_path = plan_dir / "SHA256SUMS"
            checksum_lines = checksum_path.read_text(encoding="utf-8").splitlines()
            checksum_path.write_text(
                "\n".join(
                    (
                        f"{sha256(logical_path.read_bytes()).hexdigest()}  "
                        f"{logical_path.name}"
                        if line.endswith("  " + logical_path.name)
                        else line
                    )
                    for line in checksum_lines
                )
                + "\n",
                encoding="utf-8",
            )
            with self.assertRaises((FlowMeshFullFlowTrialError, FullFlowRuntimeError)):
                verify_flowmesh_full_flow_trial_v2_plan(plan_dir)

    def test_real_loopback_n4_n7_n6_and_n1_scoring(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw_bundle = _bundle_bytes()
            bundle_path = root / "source-frame-bundle.tar"
            bundle_path.write_bytes(raw_bundle)
            data_plane = root / "data-plane"
            build_full_flow_data_plane_package(
                [
                    FullFlowArtifactBinding(
                        object_id=OBJECT_ID,
                        artifact_path=bundle_path,
                        catalog_version=CATALOG_VERSION,
                        plan_ids=(PLAN_ID,),
                    )
                ],
                output_dir=data_plane,
                package_id="hidden-oracle-full-flow-data-v2",
            )
            data_agent = create_data_agent_http_server(
                manifest_path=data_plane / DATA_AGENT_MANIFEST_PATH,
                operation_db=root / "n4-operations.sqlite3",
                settings=DataAgentServerSettings(
                    host="127.0.0.1",
                    port=0,
                    token=DATA_AGENT_TOKEN,
                    artifact_secret=ARTIFACT_SECRET,
                ),
            )
            public = _public_task()
            label_source = root / "hidden-label-source.json"
            label_source.write_text(
                json.dumps(
                    {
                        "schema_version": N1_LABEL_SOURCE_SCHEMA_VERSION,
                        "oracle_id": ORACLE_ID,
                        "logical_node_id": "N1",
                        "labels": [
                            {
                                "object_id": OBJECT_ID,
                                "task_binding_sha256": public[
                                    "task_binding_sha256"
                                ],
                                "success_scoring_rule": (
                                    MULTIPLE_CHOICE_EXACT_SCORING_RULE
                                ),
                                "answer_option_ids": ["A", "B", "C"],
                                "correct_answer_id": "B",
                            }
                        ],
                        "credentials_recorded": False,
                    },
                    sort_keys=True,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            oracle_package = root / "oracle-package"
            package_result = build_n1_oracle_package(
                label_source,
                output_dir=oracle_package,
            )
            oracle = create_n1_oracle_http_server(
                oracle_package,
                state_db=root / "n1-oracle.sqlite3",
                bearer_token=ORACLE_TOKEN,
                evidence_secret=ORACLE_EVIDENCE_SECRET,
                host="127.0.0.1",
                port=0,
            )
            llm = ThreadingHTTPServer(("127.0.0.1", 0), _VisionLLMHandler)
            llm.requests = []  # type: ignore[attr-defined]
            semantic_environment = {
                "PATHFINDER_SEMANTIC_LLM_BASE_URL": (
                    f"http://127.0.0.1:{llm.server_address[1]}"
                ),
                "PATHFINDER_SEMANTIC_LLM_MODEL": MODEL,
                "PATHFINDER_SEMANTIC_LLM_API_KEY": LLM_API_KEY,
                "PATHFINDER_SEMANTIC_LLM_TIMEOUT_SECONDS": "10",
            }
            with (
                _serving(data_agent),
                _serving(oracle),
                _serving(llm),
                mock.patch.dict(os.environ, semantic_environment, clear=False),
            ):
                n6 = create_container_node_server(
                    "N6",
                    root / "n6-state",
                    enable_semantic_llm=True,
                    semantic_bearer_token=SEMANTIC_TOKEN,
                )
                n7_runtime = build_http_full_flow_runtime(
                    route_config=self.route,
                    http_config=FullFlowHttpConfig(
                        data_agent_base_url=(
                            f"http://127.0.0.1:{data_agent.server_address[1]}"
                        ),
                        semantic_base_url=(
                            f"http://127.0.0.1:{n6.server_address[1]}"
                        ),
                        data_agent_token=DATA_AGENT_TOKEN,
                        semantic_bearer_token=SEMANTIC_TOKEN,
                        oracle_base_url=(
                            f"http://127.0.0.1:{oracle.server_address[1]}"
                        ),
                        oracle_id=ORACLE_ID,
                        oracle_public_task_set_sha256=package_result[
                            "public_task_set_sha256"
                        ],
                        oracle_token=ORACLE_TOKEN,
                        max_retries=0,
                        data_agent_timeout_seconds=10,
                        semantic_timeout_seconds=10,
                        oracle_timeout_seconds=10,
                    ),
                )
                n7 = create_container_node_server(
                    "N7",
                    root / "n7-state",
                    full_flow_runtime=n7_runtime,
                    full_flow_hmac_secret=INGRESS_SECRET,
                )
                with _serving(n6), _serving(n7):
                    trial_request = _request(self.route, raw_bundle)
                    body = json.dumps(
                        trial_request,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                    connection = http.client.HTTPConnection(
                        "127.0.0.1",
                        n7.server_address[1],
                        timeout=15,
                    )
                    try:
                        connection.request(
                            "POST",
                            "/v1/pathfinder/trials/execute",
                            body=body,
                            headers={
                                "Content-Type": "application/json",
                                FULL_FLOW_INGRESS_SIGNATURE_HEADER: (
                                    full_flow_request_hmac_sha256(
                                        trial_request,
                                        INGRESS_SECRET,
                                    )
                                ),
                            },
                        )
                        response = connection.getresponse()
                        response_body = response.read()
                    finally:
                        connection.close()

            self.assertEqual(200, response.status, response_body.decode())
            evidence = json.loads(response_body)
            self.assertEqual(
                FULL_FLOW_EVIDENCE_V2_SCHEMA_VERSION,
                evidence["schema_version"],
            )
            self.assertTrue(evidence["scoring"]["task_success"])
            self.assertEqual(1.0, evidence["scoring"]["score"])
            self.assertEqual("N1", evidence["scoring"]["oracle_result"]["node_id"])
            self.assertEqual(
                public["task_binding_sha256"],
                evidence["scoring"]["task_binding_sha256"],
            )
            self.assertFalse(
                evidence["scoring"]["oracle_result"]["hidden_answer_returned"]
            )
            self.assertEqual(1, oracle.service.score_count())
            assert_hidden_oracle_fields_absent(evidence)
            durable = json.dumps(evidence, sort_keys=True)
            for forbidden in (
                "correct_answer_id",
                DATA_AGENT_TOKEN,
                ARTIFACT_SECRET,
                ORACLE_TOKEN,
                ORACLE_EVIDENCE_SECRET.decode("utf-8"),
                LLM_API_KEY,
                "http://",
            ):
                self.assertNotIn(forbidden, durable)


if __name__ == "__main__":
    unittest.main()
