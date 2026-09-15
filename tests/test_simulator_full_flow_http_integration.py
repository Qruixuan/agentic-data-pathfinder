"""Loopback HTTP integration for the complete N4 -> N7 -> N6 trial path.

This module deliberately starts only in-process test servers.  It exercises
the production Data Agent server and clients, both real container-node HTTP
endpoints, and the real vision request adapter without contacting FlowMesh,
Docker, or an external LLM provider.
"""

from __future__ import annotations

import http.client
import json
import os
import tempfile
import threading
import unittest
from contextlib import contextmanager
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterator
from unittest import mock

from pathfinder.data_agent_server import (
    DataAgentServerSettings,
    create_data_agent_http_server,
)
from pathfinder.distributed.scoring import (
    MULTIPLE_CHOICE_EXACT_SCORING_RULE,
)
from pathfinder.frame_bundle_ingest import (
    FRAME_BUNDLE_MEDIA_TYPE,
    validate_frame_bundle_bytes,
)
from pathfinder.simulator.container_node import (
    FULL_FLOW_INGRESS_SIGNATURE_HEADER,
    create_container_node_server,
    full_flow_request_hmac_sha256,
)
from pathfinder.simulator.full_flow_data_plane import (
    DATA_AGENT_MANIFEST_PATH,
    FullFlowArtifactBinding,
    build_full_flow_data_plane_package,
)
from pathfinder.simulator.full_flow_runtime import (
    FULL_FLOW_EVIDENCE_SCHEMA_VERSION,
    FullFlowHttpConfig,
    FullFlowRouteConfig,
    build_full_flow_trial_request,
    build_http_full_flow_runtime,
)
from tests.test_simulator_full_flow_runtime import _bundle_bytes


OBJECT_ID = "nextqa-val-0000000001"
CATALOG_VERSION = "full-flow-http-catalog-v1"
PLAN_ID = "D_origin_remote"
MODEL = "local-vision-model"
DATA_AGENT_TOKEN = "integration-data-token-must-not-persist"
ARTIFACT_SECRET = "integration-artifact-secret-must-not-persist"
LLM_API_KEY = "integration-llm-key-must-not-persist"
SEMANTIC_TOKEN = "integration-semantic-token-must-not-persist"
INGRESS_SECRET = "integration-ingress-secret-must-not-persist"


@contextmanager
def _serving(server: ThreadingHTTPServer) -> Iterator[None]:
    thread = threading.Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": 0.01},
        daemon=True,
    )
    thread.start()
    try:
        yield
    finally:
        try:
            server.shutdown()
        finally:
            server.server_close()
            thread.join(timeout=5.0)
        if thread.is_alive():
            raise AssertionError("loopback test server did not stop")


class _VisionLLMHandler(BaseHTTPRequestHandler):
    """Minimal OpenAI-compatible vision completion endpoint."""

    server: ThreadingHTTPServer

    def log_message(self, format: str, *args: object) -> None:
        del format, args

    def do_POST(self) -> None:
        if self.path != "/chat/completions":
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        request = json.loads(self.rfile.read(length).decode("utf-8"))
        requests = getattr(self.server, "requests")
        requests.append({
            "authorization": self.headers.get("Authorization"),
            "content_type": self.headers.get("Content-Type"),
            "request": request,
        })
        response = json.dumps({
            "model": request["model"],
            "choices": [{"message": {"content": "B"}}],
        }).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)


class FullFlowHTTPIntegrationTest(unittest.TestCase):
    def test_real_loopback_services_complete_one_exact_scored_trial(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw_bundle = _bundle_bytes()
            bundle_path = root / "source-frame-bundle.tar"
            bundle_path.write_bytes(raw_bundle)
            bundle = validate_frame_bundle_bytes(
                raw_bundle,
                expected_object_id=OBJECT_ID,
                expected_sha256=sha256(raw_bundle).hexdigest(),
                expected_size_bytes=len(raw_bundle),
                artifact_media_type=FRAME_BUNDLE_MEDIA_TYPE,
            )

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
                package_id="full-flow-http-integration-v1",
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
            data_agent_base = (
                f"http://127.0.0.1:{data_agent.server_address[1]}"
            )

            llm = ThreadingHTTPServer(
                ("127.0.0.1", 0),
                _VisionLLMHandler,
            )
            llm.requests = []  # type: ignore[attr-defined]
            llm_base = f"http://127.0.0.1:{llm.server_address[1]}"
            semantic_environment = {
                "PATHFINDER_SEMANTIC_LLM_BASE_URL": llm_base,
                "PATHFINDER_SEMANTIC_LLM_MODEL": MODEL,
                "PATHFINDER_SEMANTIC_LLM_API_KEY": LLM_API_KEY,
                "PATHFINDER_SEMANTIC_LLM_TIMEOUT_SECONDS": "10",
            }

            with (
                _serving(data_agent),
                _serving(llm),
                mock.patch.dict(
                    os.environ,
                    semantic_environment,
                    clear=False,
                ),
            ):
                n6 = create_container_node_server(
                    "N6",
                    root / "n6-state",
                    enable_semantic_llm=True,
                    semantic_bearer_token=SEMANTIC_TOKEN,
                )
                n6_base = f"http://127.0.0.1:{n6.server_address[1]}"
                route = FullFlowRouteConfig(
                    route_id=PLAN_ID,
                    requested_location="origin-warm",
                    data_agent_plan_id=PLAN_ID,
                    data_agent_plan_epoch=0,
                )
                n7_full_flow = build_http_full_flow_runtime(
                    route_config=route,
                    http_config=FullFlowHttpConfig(
                        data_agent_base_url=data_agent_base,
                        semantic_base_url=n6_base,
                        data_agent_token=DATA_AGENT_TOKEN,
                        semantic_bearer_token=SEMANTIC_TOKEN,
                        data_agent_timeout_seconds=10.0,
                        semantic_timeout_seconds=10.0,
                        max_retries=0,
                    ),
                )
                n7 = create_container_node_server(
                    "N7",
                    root / "n7-state",
                    full_flow_runtime=n7_full_flow,
                    full_flow_hmac_secret=INGRESS_SECRET,
                )

                with _serving(n6), _serving(n7):
                    request = build_full_flow_trial_request(
                        route_config=route,
                        full_flow_request_id="full-flow-http-request-v1",
                        run_id="full-flow-http-run-v1",
                        trial_id="full-flow-http-trial-v1",
                        trial_key="scenario-v1|W1|D2|r0000",
                        workload_id="visible-video-qa-v1",
                        task_class_id="video_qa",
                        object_id=OBJECT_ID,
                        artifact_sha256=sha256(raw_bundle).hexdigest(),
                        artifact_size_bytes=len(raw_bundle),
                        object_catalog_version=CATALOG_VERSION,
                        expected_model=MODEL,
                        question="Which option describes the main action?",
                        answer_options=[
                            {"option_id": "A", "text": "A person cooks."},
                            {"option_id": "B", "text": "Musicians perform."},
                            {"option_id": "C", "text": "A vehicle moves."},
                        ],
                        correct_answer_id="B",
                        success_scoring_rule=(
                            MULTIPLE_CHOICE_EXACT_SCORING_RULE
                        ),
                    )
                    body = json.dumps(
                        request,
                        separators=(",", ":"),
                        sort_keys=True,
                    ).encode("utf-8")
                    connection = http.client.HTTPConnection(
                        "127.0.0.1",
                        n7.server_address[1],
                        timeout=15.0,
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
                                        request,
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
                FULL_FLOW_EVIDENCE_SCHEMA_VERSION,
                evidence["schema_version"],
            )
            self.assertEqual("COMPLETE", evidence["status"])
            self.assertEqual(OBJECT_ID, evidence["object_id"])
            self.assertEqual(
                {
                    "source_node_id": "N4",
                    "executor_node_id": "N7",
                    "inference_node_id": "N6",
                },
                {
                    key: evidence["route"][key]
                    for key in (
                        "source_node_id",
                        "executor_node_id",
                        "inference_node_id",
                    )
                },
            )
            self.assertTrue(evidence["route_unified"])
            self.assertTrue(evidence["real_object_identity_verified"])
            self.assertTrue(evidence["data_agent_source_identity_verified"])
            self.assertTrue(evidence["data_agent_artifact_delivery_verified"])
            self.assertTrue(evidence["semantic_health_verified"])
            self.assertTrue(evidence["scoring_verified"])
            self.assertEqual("B", evidence["scoring"]["final_answer"])
            self.assertTrue(evidence["scoring"]["task_success"])
            self.assertEqual(MODEL, evidence["semantic"]["model"])
            self.assertEqual(
                sha256(raw_bundle).hexdigest(),
                evidence["data_agent"]["artifact_sha256"],
            )
            self.assertEqual(
                bundle.manifest_sha256,
                evidence["data_agent"]["manifest_sha256"],
            )
            self.assertEqual(
                bundle.frame_count,
                evidence["data_agent"]["frame_count"],
            )

            access_id = evidence["data_agent"]["access_id"]
            telemetry = data_agent.service.access_telemetry(access_id)
            downloads = telemetry["artifact_download"]
            self.assertEqual(1, downloads["download_request_count"])
            self.assertEqual(1, downloads["completed_request_count"])
            self.assertEqual(1, downloads["full_download_count"])
            self.assertEqual(len(raw_bundle), downloads["bytes_sent"])

            llm_requests = llm.requests  # type: ignore[attr-defined]
            self.assertEqual(1, len(llm_requests))
            self.assertEqual(
                "Bearer " + LLM_API_KEY,
                llm_requests[0]["authorization"],
            )
            llm_content = llm_requests[0]["request"]["messages"][0][
                "content"
            ]
            self.assertEqual(bundle.frame_count + 1, len(llm_content))
            self.assertTrue(
                all(
                    item["image_url"]["url"].startswith(
                        "data:image/jpeg;base64,"
                    )
                    for item in llm_content[1:]
                )
            )

            durable_text = json.dumps(evidence, sort_keys=True)
            for forbidden in (
                DATA_AGENT_TOKEN,
                ARTIFACT_SECRET,
                LLM_API_KEY,
                "http://",
                "https://",
                "authorization",
                "jpeg_base64",
            ):
                self.assertNotIn(forbidden, durable_text.casefold())
            self.assertFalse(evidence["credentials_recorded"])
            self.assertFalse(evidence["eligible_for_scientific_claims"])


if __name__ == "__main__":
    unittest.main()
