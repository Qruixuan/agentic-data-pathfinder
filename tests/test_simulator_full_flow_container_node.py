"""Container-node integration tests for the native full-flow endpoint."""

from __future__ import annotations

import http.client
import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from pathfinder.simulator.container_node import (
    FULL_FLOW_INGRESS_SIGNATURE_HEADER,
    SEMANTIC_ROUTE_ENDPOINT_PATH,
    ContainerNodeError,
    ContainerNodeRuntime,
    _full_flow_runtime_from_environment,
    create_container_node_server,
    full_flow_request_hmac_sha256,
)


SEMANTIC_TOKEN = "container-node-test-bearer"
INGRESS_SECRET = "container-node-test-hmac-secret-32"


class _FakeFullFlowRuntime:
    def __init__(self) -> None:
        self.requests: list[dict[str, object]] = []

    def execute(self, request: dict[str, object]) -> dict[str, object]:
        self.requests.append(dict(request))
        return {
            "schema_version": "pathfinder.test-full-flow-evidence/v1",
            "status": "completed",
            "route_unified": True,
            "credentials_recorded": False,
        }


class _FakeSemanticRouteHandler:
    def __init__(self) -> None:
        self.requests: list[dict[str, object]] = []

    def execute(self, request: dict[str, object]) -> dict[str, object]:
        self.requests.append(dict(request))
        return {
            "schema_version": "pathfinder.test-semantic-route-evidence/v1",
            "status": "COMPLETE",
            "credentials_recorded": False,
        }


class FullFlowContainerNodeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_runtime_is_enabled_only_on_n7_and_reported_safely(self) -> None:
        full_flow = _FakeFullFlowRuntime()
        runtime = ContainerNodeRuntime(
            "N7",
            self.root / "n7",
            full_flow_runtime=full_flow,
        )

        health = runtime.health()
        self.assertTrue(health["full_flow_enabled"])
        self.assertEqual("N4", health["full_flow_source_node_id"])
        self.assertEqual("N7", health["full_flow_executor_node_id"])
        self.assertEqual("N6", health["full_flow_inference_node_id"])
        self.assertNotIn("url", json.dumps(health).casefold())
        self.assertNotIn("token", json.dumps(health).casefold())

        with self.assertRaisesRegex(ContainerNodeError, "only on N7"):
            ContainerNodeRuntime(
                "N6",
                self.root / "n6",
                full_flow_runtime=full_flow,
            )

    def test_http_endpoint_delegates_one_json_request_to_n7(self) -> None:
        full_flow = _FakeFullFlowRuntime()
        server = create_container_node_server(
            "N7",
            self.root / "state",
            full_flow_runtime=full_flow,
            full_flow_hmac_secret=INGRESS_SECRET,
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 2.0)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)

        request = {"full_flow_request_id": "request-1"}
        body = json.dumps(request).encode("utf-8")
        connection = http.client.HTTPConnection(
            "127.0.0.1",
            server.server_address[1],
            timeout=5.0,
        )
        self.addCleanup(connection.close)
        connection.request(
            "POST",
            "/v1/pathfinder/trials/execute",
            body=body,
            headers={
                "Content-Type": "application/json",
                FULL_FLOW_INGRESS_SIGNATURE_HEADER: (
                    full_flow_request_hmac_sha256(request, INGRESS_SECRET)
                ),
            },
        )
        response = connection.getresponse()
        result = json.loads(response.read())

        self.assertEqual(200, response.status)
        self.assertTrue(result["route_unified"])
        self.assertEqual([request], full_flow.requests)

    def test_semantic_route_endpoint_is_authenticated_on_n7_and_n8(self) -> None:
        for node_id in ("N7", "N8"):
            with self.subTest(node_id=node_id):
                handler = _FakeSemanticRouteHandler()
                server = create_container_node_server(
                    node_id,
                    self.root / node_id.casefold(),
                    semantic_route_handler=handler,
                    full_flow_hmac_secret=INGRESS_SECRET,
                )
                thread = threading.Thread(
                    target=server.serve_forever,
                    daemon=True,
                )
                thread.start()
                self.addCleanup(thread.join, 2.0)
                self.addCleanup(server.server_close)
                self.addCleanup(server.shutdown)

                request = {
                    "schema_version": (
                        "pathfinder.flowmesh-semantic-route-request/v1alpha1"
                    ),
                    "request_sha256": "a" * 64,
                }
                body = json.dumps(request).encode("utf-8")
                connection = http.client.HTTPConnection(
                    "127.0.0.1",
                    server.server_address[1],
                    timeout=5.0,
                )
                self.addCleanup(connection.close)
                connection.request(
                    "POST",
                    SEMANTIC_ROUTE_ENDPOINT_PATH,
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
                result = json.loads(response.read())

                self.assertEqual(200, response.status)
                self.assertEqual("COMPLETE", result["status"])
                self.assertEqual([request], handler.requests)
                health = server.runtime.health()
                self.assertTrue(health["semantic_route_coordinator_enabled"])
                self.assertEqual(
                    node_id,
                    health["semantic_route_coordinator_node_id"],
                )
                self.assertEqual(
                    SEMANTIC_ROUTE_ENDPOINT_PATH,
                    health["semantic_route_endpoint_path"],
                )

    def test_semantic_route_handler_is_restricted_to_execution_nodes(self) -> None:
        with self.assertRaisesRegex(ContainerNodeError, "N7 or N8"):
            ContainerNodeRuntime(
                "N6",
                self.root / "invalid-semantic-route-node",
                semantic_route_handler=_FakeSemanticRouteHandler(),
            )

    def test_full_flow_endpoint_rejects_missing_invalid_and_duplicate_hmac(self) -> None:
        full_flow = _FakeFullFlowRuntime()
        server = create_container_node_server(
            "N7",
            self.root / "authenticated-state",
            full_flow_runtime=full_flow,
            full_flow_hmac_secret=INGRESS_SECRET,
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 2.0)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        request = {"full_flow_request_id": "request-auth"}
        body = json.dumps(request).encode("utf-8")

        for signature in (None, "0" * 64):
            with self.subTest(signature=signature):
                connection = http.client.HTTPConnection(
                    "127.0.0.1", server.server_port, timeout=5.0
                )
                headers = {"Content-Type": "application/json"}
                if signature is not None:
                    headers[FULL_FLOW_INGRESS_SIGNATURE_HEADER] = signature
                connection.request(
                    "POST",
                    "/v1/pathfinder/trials/execute",
                    body=body,
                    headers=headers,
                )
                response = connection.getresponse()
                payload = json.loads(response.read())
                connection.close()
                self.assertEqual(401, response.status)
                self.assertEqual("unauthorized", payload["message"])
                self.assertEqual(
                    "Pathfinder-HMAC",
                    response.getheader("WWW-Authenticate"),
                )

        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_port, timeout=5.0
        )
        connection.putrequest("POST", "/v1/pathfinder/trials/execute")
        connection.putheader("Content-Type", "application/json")
        connection.putheader("Content-Length", str(len(body)))
        signature = full_flow_request_hmac_sha256(request, INGRESS_SECRET)
        connection.putheader(FULL_FLOW_INGRESS_SIGNATURE_HEADER, signature)
        connection.putheader(FULL_FLOW_INGRESS_SIGNATURE_HEADER, signature)
        connection.endheaders(body)
        response = connection.getresponse()
        response.read()
        connection.close()
        self.assertEqual(401, response.status)
        self.assertEqual([], full_flow.requests)

    def test_full_flow_hmac_is_bound_to_the_exact_request(self) -> None:
        full_flow = _FakeFullFlowRuntime()
        server = create_container_node_server(
            "N7",
            self.root / "request-binding-state",
            full_flow_runtime=full_flow,
            full_flow_hmac_secret=INGRESS_SECRET,
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 2.0)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        signed = {"full_flow_request_id": "request-a", "route_id": "route-a"}
        submitted = {
            "full_flow_request_id": "request-a",
            "route_id": "route-b",
        }
        body = json.dumps(submitted).encode("utf-8")
        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_port, timeout=5.0
        )
        connection.request(
            "POST",
            "/v1/pathfinder/trials/execute",
            body=body,
            headers={
                "Content-Type": "application/json",
                FULL_FLOW_INGRESS_SIGNATURE_HEADER: (
                    full_flow_request_hmac_sha256(signed, INGRESS_SECRET)
                ),
            },
        )
        response = connection.getresponse()
        response.read()
        connection.close()
        self.assertEqual(401, response.status)
        self.assertEqual([], full_flow.requests)

    def test_public_health_does_not_require_full_flow_hmac(self) -> None:
        server = create_container_node_server(
            "N7",
            self.root / "public-health-state",
            full_flow_runtime=_FakeFullFlowRuntime(),
            full_flow_hmac_secret=INGRESS_SECRET,
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 2.0)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_port, timeout=5.0
        )
        connection.request("GET", "/healthz")
        response = connection.getresponse()
        health = json.loads(response.read())
        connection.close()
        self.assertEqual(200, response.status)
        self.assertEqual("ok", health["status"])
        self.assertNotIn(INGRESS_SECRET, json.dumps(health))

    def test_disabled_full_flow_endpoint_fails_closed(self) -> None:
        runtime = ContainerNodeRuntime("N7", self.root / "state")
        with self.assertRaisesRegex(ContainerNodeError, "disabled"):
            runtime.execute_full_flow_trial({"request": "value"})

    def test_semantic_artifact_endpoint_uses_exact_single_bearer(self) -> None:
        artifacts = self.root / "artifacts"
        artifacts.mkdir()
        (artifacts / "digest.txt").write_text("bounded", encoding="utf-8")
        server = create_container_node_server(
            "N4",
            self.root / "semantic-artifact-state",
            semantic_artifact_root=artifacts,
            semantic_bearer_token=SEMANTIC_TOKEN,
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 2.0)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        target = "/v1/semantic/representation/read?path=digest.txt"

        for authorization, expected_status in (
            (None, 401),
            ("Bearer wrong", 401),
            ("Bearer " + SEMANTIC_TOKEN, 200),
        ):
            with self.subTest(authorization=authorization):
                connection = http.client.HTTPConnection(
                    "127.0.0.1", server.server_port, timeout=5.0
                )
                headers = {}
                if authorization is not None:
                    headers["Authorization"] = authorization
                connection.request("GET", target, headers=headers)
                response = connection.getresponse()
                payload = response.read()
                connection.close()
                self.assertEqual(expected_status, response.status)
                if expected_status == 200:
                    self.assertEqual(b"bounded", payload)
                else:
                    self.assertEqual("Bearer", response.getheader("WWW-Authenticate"))

        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_port, timeout=5.0
        )
        connection.putrequest("GET", target)
        connection.putheader("Authorization", "Bearer " + SEMANTIC_TOKEN)
        connection.putheader("Authorization", "Bearer " + SEMANTIC_TOKEN)
        connection.endheaders()
        response = connection.getresponse()
        response.read()
        connection.close()
        self.assertEqual(401, response.status)

    def test_semantic_invoke_uses_exact_single_bearer(self) -> None:
        server = create_container_node_server(
            "N6",
            self.root / "semantic-invoke-state",
            enable_semantic_llm=True,
            semantic_bearer_token=SEMANTIC_TOKEN,
        )
        semantic_complete = mock.Mock(
            return_value={"status": "complete", "credentials_recorded": False}
        )
        server.runtime.semantic_complete = semantic_complete
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 2.0)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        body = b"{}"

        for authorization, expected_status in (
            (None, 401),
            ("Bearer wrong", 401),
            ("Bearer " + SEMANTIC_TOKEN, 200),
        ):
            with self.subTest(authorization=authorization):
                connection = http.client.HTTPConnection(
                    "127.0.0.1", server.server_port, timeout=5.0
                )
                headers = {"Content-Type": "application/json"}
                if authorization is not None:
                    headers["Authorization"] = authorization
                connection.request(
                    "POST",
                    "/v1/semantic/chat-completions",
                    body=body,
                    headers=headers,
                )
                response = connection.getresponse()
                response.read()
                connection.close()
                self.assertEqual(expected_status, response.status)

        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_port, timeout=5.0
        )
        connection.putrequest("POST", "/v1/semantic/chat-completions")
        connection.putheader("Content-Type", "application/json")
        connection.putheader("Content-Length", str(len(body)))
        connection.putheader("Authorization", "Bearer " + SEMANTIC_TOKEN)
        connection.putheader("Authorization", "Bearer " + SEMANTIC_TOKEN)
        connection.endheaders(body)
        response = connection.getresponse()
        response.read()
        connection.close()
        self.assertEqual(401, response.status)
        self.assertEqual(1, semantic_complete.call_count)

    def test_server_refuses_protected_endpoints_without_runtime_secrets(self) -> None:
        artifacts = self.root / "missing-secret-artifacts"
        artifacts.mkdir()
        with self.assertRaisesRegex(ContainerNodeError, "semantic bearer token"):
            create_container_node_server(
                "N4",
                self.root / "missing-semantic-secret-state",
                semantic_artifact_root=artifacts,
            )
        with self.assertRaisesRegex(ContainerNodeError, "HMAC secret"):
            create_container_node_server(
                "N7",
                self.root / "missing-full-flow-secret-state",
                full_flow_runtime=_FakeFullFlowRuntime(),
            )
        with self.assertRaisesRegex(ContainerNodeError, "at least 32 bytes"):
            create_container_node_server(
                "N7",
                self.root / "short-full-flow-secret-state",
                full_flow_runtime=_FakeFullFlowRuntime(),
                full_flow_hmac_secret="too-short",
            )

    def test_environment_builds_ephemeral_route_and_http_binding(self) -> None:
        environment = {
            "PATHFINDER_FULL_FLOW_ENABLED": "1",
            "PATHFINDER_FULL_FLOW_SOURCE_NODE_ID": "N4",
            "PATHFINDER_FULL_FLOW_EXECUTOR_NODE_ID": "N7",
            "PATHFINDER_FULL_FLOW_INFERENCE_NODE_ID": "N6",
            "PATHFINDER_FULL_FLOW_ROUTE_ID": "n4-n7-n6-v1",
            "PATHFINDER_FULL_FLOW_REQUESTED_LOCATION": "origin-warm",
            "PATHFINDER_FULL_FLOW_DATA_AGENT_PLAN_ID": "D2",
            "PATHFINDER_FULL_FLOW_DATA_AGENT_PLAN_EPOCH": "4",
            "PATHFINDER_FULL_FLOW_DATA_AGENT_BASE_URL": (
                "http://pathfinder-sim-n4-data-agent:8780"
            ),
            "PATHFINDER_FULL_FLOW_SEMANTIC_BASE_URL": (
                "http://pathfinder-sim-n6-inference:9080"
            ),
            "PATHFINDER_FULL_FLOW_SIMULATOR_PRIVATE_HOSTS": (
                "pathfinder-sim-n4-data-agent,"
                "pathfinder-sim-n6-inference"
            ),
            "PATHFINDER_DATA_AGENT_TOKEN": "runtime-only-test-token",
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
        route = build.call_args.kwargs["route_config"]
        http = build.call_args.kwargs["http_config"]
        self.assertEqual("D2", route.data_agent_plan_id)
        self.assertEqual(4, route.data_agent_plan_epoch)
        self.assertEqual("origin-warm", route.requested_location)
        self.assertEqual(
            (
                "pathfinder-sim-n4-data-agent",
                "pathfinder-sim-n6-inference",
            ),
            http.simulator_private_http_hosts,
        )
        self.assertNotIn("runtime-only-test-token", repr(http))
        self.assertNotIn(SEMANTIC_TOKEN, repr(http))
        self.assertEqual(SEMANTIC_TOKEN, http.semantic_bearer_token)

    def test_environment_refuses_full_flow_on_another_node(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"PATHFINDER_FULL_FLOW_ENABLED": "1"},
            clear=True,
        ):
            with self.assertRaisesRegex(ContainerNodeError, "only on N7"):
                _full_flow_runtime_from_environment("N6")

    def test_environment_requires_data_agent_authentication(self) -> None:
        environment = {
            "PATHFINDER_FULL_FLOW_ENABLED": "1",
            "PATHFINDER_FULL_FLOW_SOURCE_NODE_ID": "N4",
            "PATHFINDER_FULL_FLOW_EXECUTOR_NODE_ID": "N7",
            "PATHFINDER_FULL_FLOW_INFERENCE_NODE_ID": "N6",
            "PATHFINDER_FULL_FLOW_ROUTE_ID": "n4-n7-n6-v1",
            "PATHFINDER_FULL_FLOW_REQUESTED_LOCATION": "origin-warm",
            "PATHFINDER_FULL_FLOW_DATA_AGENT_PLAN_ID": "D2",
            "PATHFINDER_FULL_FLOW_DATA_AGENT_BASE_URL": (
                "http://pathfinder-sim-n4-data-agent:8780"
            ),
            "PATHFINDER_FULL_FLOW_SEMANTIC_BASE_URL": (
                "http://pathfinder-sim-n6-inference:9080"
            ),
            "PATHFINDER_FULL_FLOW_SIMULATOR_PRIVATE_HOSTS": (
                "pathfinder-sim-n4-data-agent,"
                "pathfinder-sim-n6-inference"
            ),
        }
        with mock.patch.dict(os.environ, environment, clear=True):
            with self.assertRaisesRegex(
                ContainerNodeError,
                "PATHFINDER_DATA_AGENT_TOKEN is required",
            ):
                _full_flow_runtime_from_environment("N7")


if __name__ == "__main__":
    unittest.main()
