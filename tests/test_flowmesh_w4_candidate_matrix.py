from __future__ import annotations

import json
import io
import threading
import unittest
import urllib.error
import urllib.request
from contextlib import redirect_stdout
from pathlib import Path

from pathfinder.integrations.flowmesh.w4_candidate_matrix import (
    REQUESTS_NAME,
    FlowMeshW4CandidateMatrixError,
    W4_COORDINATOR_ENDPOINT_PATH,
    build_flowmesh_w4_candidate_matrix_workflow,
    full_flow_w4_hmac_header_provider,
    plan_flowmesh_w4_candidate_matrix,
    run_flowmesh_w4_candidate_matrix,
    validate_flowmesh_w4_trial_response,
    verify_flowmesh_w4_candidate_matrix_plan,
    verify_flowmesh_w4_candidate_matrix_run,
)
from pathfinder.integrations.flowmesh.contracts import (
    FlowMeshSettings,
    FlowMeshWorkerIdentity,
    SubmittedWorkflow,
    TerminalWorkflow,
    WorkflowValidation,
)
from pathfinder.cli import main as cli_main
from pathfinder.simulator.container_node import (
    FULL_FLOW_INGRESS_SIGNATURE_HEADER,
)
from pathfinder.simulator.full_flow_w4_flowmesh_service import (
    FullFlowW4FlowMeshCoordinator,
    create_full_flow_w4_flowmesh_http_server,
)
from tests import test_simulator_full_flow_w4_live_executor as live_fixture


SECRET = "test-only-w4-flowmesh-ingress-secret-123456789"


def _requests(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]


class _FakeFlowMeshClient:
    def __init__(self, responses: list[dict]) -> None:
        self._task_ids = tuple(f"tsk-w4-{index:02d}" for index in range(16))
        self._responses = {
            task_id: response
            for task_id, response in zip(
                self._task_ids,
                reversed(responses),
                strict=True,
            )
        }
        self.workflow = None

    def describe_current_worker(self, *, worker_id=None, alias=None):
        return FlowMeshWorkerIdentity(
            worker_id="wkr-w4-test",
            alias=alias,
            status="IDLE",
            namespace="test",
            cluster="test",
            node_alias="local-test",
        )

    def validate(self, workflow):
        self.workflow = json.loads(json.dumps(workflow))
        return WorkflowValidation(ok=True)

    def submit(self, workflow):
        self.workflow = json.loads(json.dumps(workflow))
        return SubmittedWorkflow("wfl-w4-test", self._task_ids)

    def wait(self, workflow_id, poll_interval_seconds):
        return TerminalWorkflow(workflow_id=workflow_id, status="DONE")

    def retrieve_result(self, task_id):
        return {
            "executor": "api",
            "ok": True,
            "status_code": 200,
            "text": json.dumps(self._responses[task_id], sort_keys=True),
        }

    def describe_task_failure(self, task_id):
        return {"status": "DONE", "assigned_worker": "wkr-w4-test"}


class FlowMeshW4CandidateMatrixTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = live_fixture.FullFlowW4LiveExecutorTest(
            "test_executes_all_sixteen_routes_through_strict_components"
        )
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.root = self.fixture.root
        self.plan = self.root / "flowmesh-plan"
        plan_flowmesh_w4_candidate_matrix(
            route_package_dir=self.fixture.route,
            run_id="w4-flowmesh-test-v1",
            worker_alias="pathfinder-test-worker",
            output_dir=self.plan,
        )
        self.requests = _requests(self.plan / REQUESTS_NAME)

    def _coordinators(self):
        executor, _artifacts, _transport, _ranker = self.fixture.executor()

        def health(node_id):
            cache_adapter = executor._components.caches[node_id]
            return cache_adapter.cache.health()

        coordinators = {
            node_id: FullFlowW4FlowMeshCoordinator(
                coordinator_node_id=node_id,
                route_package_dir=self.fixture.route,
                executor=executor,
                cache_health=lambda node_id=node_id: health(node_id),
                state_db=self.root / f"{node_id.lower()}-coordinator.sqlite3",
            )
            for node_id in ("N7", "N8")
        }
        return executor, coordinators

    def test_plan_is_endpoint_free_and_recompiles_exactly(self) -> None:
        result = verify_flowmesh_w4_candidate_matrix_plan(
            self.plan,
            route_package_dir=self.fixture.route,
        )
        self.assertEqual("VERIFIED", result["status"])
        self.assertEqual(16, result["flowmesh_api_task_count"])
        text = "\n".join(
            path.read_text(encoding="utf-8")
            for path in self.plan.iterdir()
            if path.is_file()
        )
        self.assertNotIn("http://", text)
        self.assertNotIn("https://", text)
        self.assertNotIn(SECRET, text)

    def test_cli_freezes_and_verifies_the_endpoint_free_plan(self) -> None:
        output = self.root / "cli-flowmesh-plan"
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            status = cli_main([
                "freeze-simulator-full-flow-w4-flowmesh-plan",
                "--route-package-dir",
                str(self.fixture.route),
                "--run-id",
                "w4-flowmesh-cli-v1",
                "--worker-alias",
                "pathfinder-test-worker",
                "--output-dir",
                str(output),
                "--compact",
            ])
        self.assertEqual(0, status, stdout.getvalue())
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            status = cli_main([
                "verify-simulator-full-flow-w4-flowmesh-plan",
                "--plan-dir",
                str(output),
                "--route-package-dir",
                str(self.fixture.route),
                "--compact",
            ])
        self.assertEqual(0, status, stdout.getvalue())
        self.assertEqual("VERIFIED", json.loads(stdout.getvalue())["status"])

    def test_workflow_has_one_serial_worker_pinned_task_per_trial(self) -> None:
        workflow = build_flowmesh_w4_candidate_matrix_workflow(
            self.requests,
            coordinator_base_urls={
                "N7": "http://127.0.0.1:19087",
                "N8": "http://127.0.0.1:19088",
            },
            selected_worker_id="wkr-test",
            owner="pathfinder",
            api_task_timeout_seconds=900,
            runtime_header_provider=full_flow_w4_hmac_header_provider(SECRET),
        )
        nodes = workflow["spec"]["graph"]["nodes"]
        self.assertEqual(16, len(nodes))
        self.assertEqual("wkr-test", workflow["metadata"]["annotations"][
            "schedule_hint"
        ]["selected_worker"])
        for index, node in enumerate(nodes):
            self.assertEqual(f"w4-trial-{index:04d}", node["name"])
            self.assertEqual(
                [] if index == 0 else [f"w4-trial-{index - 1:04d}"],
                node.get("dependsOn", []),
            )
            api = node["spec"]["api"]
            self.assertTrue(api["url"].endswith(W4_COORDINATOR_ENDPOINT_PATH))
            self.assertEqual(
                {"Content-Type", FULL_FLOW_INGRESS_SIGNATURE_HEADER},
                set(api["headers"]),
            )
        duplicate = [dict(self.requests[0]) for _ in range(16)]
        with self.assertRaisesRegex(
            FlowMeshW4CandidateMatrixError,
            "complete frozen matrix",
        ):
            build_flowmesh_w4_candidate_matrix_workflow(
                duplicate,
                coordinator_base_urls={
                    "N7": "http://127.0.0.1:19087",
                    "N8": "http://127.0.0.1:19088",
                },
                selected_worker_id="wkr-test",
                owner="pathfinder",
                api_task_timeout_seconds=900,
            )

    def test_services_execute_all_trials_with_independent_miss_then_hit(self) -> None:
        _executor, coordinators = self._coordinators()
        responses = []
        for request in self.requests:
            response = coordinators[request["coordinator_node_id"]].execute(request)
            responses.append(
                validate_flowmesh_w4_trial_response(response, request=request)
            )
        self.assertEqual(16, len(responses))
        for design, node in (("D3", "N7"), ("D7", "N8")):
            outcomes = {
                response["trial_result"]["repetition"]: {
                    row["cache_outcome"]
                    for row in response["operation_evidence"]
                    if row["action"] == "lookup"
                    and row["execution_status"] == "COMPLETED"
                }
                for response in responses
                if response["trial_result"]["design_id"] == design
            }
            self.assertEqual({"miss"}, outcomes[0])
            self.assertEqual({"hit"}, outcomes[1])
            health = coordinators[node].health()
            self.assertEqual("ok", health["status"])
            self.assertEqual(node, health["node_id"])
            self.assertEqual(node, health["coordinator_node_id"])
            self.assertEqual(
                f"{node}.w4-candidate-coordinator",
                health["runtime_service_contract_id"],
            )
            self.assertTrue(health["cache_health_verified"])
            self.assertEqual(8, health["completed_trial_count"])
            self.assertGreater(health["cache_entry_count"], 0)
        replay = coordinators["N7"].execute(self.requests[0])
        self.assertEqual(responses[0], replay)

    def test_http_surface_is_distinct_authenticated_and_content_bound(self) -> None:
        _executor, coordinators = self._coordinators()
        server = create_full_flow_w4_flowmesh_http_server(
            coordinators["N7"],
            host="127.0.0.1",
            port=0,
            hmac_secret=SECRET,
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        origin = f"http://127.0.0.1:{server.server_port}"
        request = self.requests[0]
        body = json.dumps(
            request,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

        with urllib.request.urlopen(origin + "/healthz", timeout=5) as response:
            health = json.load(response)
            self.assertEqual(200, response.status)
        self.assertEqual("ok", health["status"])
        self.assertEqual("N7", health["node_id"])
        self.assertEqual("N7", health["coordinator_node_id"])
        self.assertEqual(
            "N7.w4-candidate-coordinator",
            health["runtime_service_contract_id"],
        )
        self.assertTrue(health["cache_health_verified"])
        self.assertTrue(health["cache_state_consistent"])

        generic = urllib.request.Request(
            origin + "/v1/full-flow/execute",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with self.assertRaises(urllib.error.HTTPError) as generic_error:
            urllib.request.urlopen(generic, timeout=5)
        self.assertEqual(400, generic_error.exception.code)

        unsigned = urllib.request.Request(
            origin + W4_COORDINATOR_ENDPOINT_PATH,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with self.assertRaises(urllib.error.HTTPError) as auth_error:
            urllib.request.urlopen(unsigned, timeout=5)
        self.assertEqual(401, auth_error.exception.code)

        signed_headers = {
            "Content-Type": "application/json",
            **full_flow_w4_hmac_header_provider(SECRET)(request),
        }
        signed = urllib.request.Request(
            origin + W4_COORDINATOR_ENDPOINT_PATH,
            data=body,
            headers=signed_headers,
            method="POST",
        )
        with urllib.request.urlopen(signed, timeout=5) as response:
            result = json.load(response)
        self.assertEqual("COMPLETE", result["status"])
        self.assertEqual(request["request_sha256"], result["request_sha256"])

    def test_fresh_cache_and_frozen_order_fail_closed(self) -> None:
        executor, _artifacts, _transport, _ranker = self.fixture.executor()
        bad_health = lambda: {
            "status": "ok",
            "node_id": "N7",
            "entry_count": 1,
            "used_bytes": 10,
            "credentials_recorded": False,
        }
        with self.assertRaisesRegex(
            RuntimeError,
            "cache namespace is not fresh",
        ):
            FullFlowW4FlowMeshCoordinator(
                coordinator_node_id="N7",
                route_package_dir=self.fixture.route,
                executor=executor,
                cache_health=bad_health,
                state_db=self.root / "stale.sqlite3",
            )
        _executor, coordinators = self._coordinators()
        with self.assertRaisesRegex(RuntimeError, "frozen cache-preserving order"):
            coordinators["N7"].execute(self.requests[1])

    def test_restart_replays_durably_and_external_cache_drift_blocks_health(
        self,
    ) -> None:
        executor, coordinators = self._coordinators()
        node = "N7"
        requests = [
            request
            for request in self.requests
            if request["coordinator_node_id"] == node
        ]
        responses = [coordinators[node].execute(request) for request in requests]
        cache = executor._components.caches[node].cache
        state_db = self.root / "n7-coordinator.sqlite3"

        restarted = FullFlowW4FlowMeshCoordinator(
            coordinator_node_id=node,
            route_package_dir=self.fixture.route,
            executor=executor,
            cache_health=cache.health,
            state_db=state_db,
        )
        self.assertEqual(responses[0], restarted.execute(requests[0]))
        self.assertEqual("ok", restarted.health()["status"])
        self.assertEqual(8, restarted.health()["completed_trial_count"])

        cache.put(
            request_id="external-w4-cache-mutation",
            object_id="external-object",
            representation_id="external-representation",
            payload=b"externally-mutated-cache-entry",
        )
        health = restarted.health()
        self.assertEqual("blocked", health["status"])
        self.assertTrue(health["cache_health_verified"])
        self.assertFalse(health["cache_state_consistent"])
        self.assertEqual(node, health["node_id"])
        self.assertEqual(node, health["coordinator_node_id"])
        self.assertEqual(
            "N7.w4-candidate-coordinator",
            health["runtime_service_contract_id"],
        )

        server = create_full_flow_w4_flowmesh_http_server(
            restarted,
            host="127.0.0.1",
            port=0,
            hmac_secret=SECRET,
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        url = f"http://127.0.0.1:{server.server_port}/healthz"
        with self.assertRaises(urllib.error.HTTPError) as error:
            urllib.request.urlopen(url, timeout=5)
        self.assertEqual(503, error.exception.code)
        payload = json.loads(error.exception.read())
        self.assertEqual("blocked", payload["status"])
        self.assertNotIn(SECRET, json.dumps(payload, sort_keys=True))

    def test_flowmesh_run_is_source_bound_and_offline_verifiable(self) -> None:
        _executor, coordinators = self._coordinators()
        responses = [
            coordinators[request["coordinator_node_id"]].execute(request)
            for request in self.requests
        ]
        client = _FakeFlowMeshClient(responses)
        output = self.root / "flowmesh-run"
        result = run_flowmesh_w4_candidate_matrix(
            plan_dir=self.plan,
            route_package_dir=self.fixture.route,
            crosswalk_dir=self.fixture.crosswalk,
            index_package_dir=self.fixture.fixture.index,
            output_dir=output,
            coordinator_base_urls={
                "N7": "http://127.0.0.1:19087",
                "N8": "http://127.0.0.1:19088",
            },
            runtime_header_provider=full_flow_w4_hmac_header_provider(SECRET),
            client=client,
            settings=FlowMeshSettings(
                worker_alias="pathfinder-test-worker",
                owner="pathfinder",
            ),
        )
        self.assertEqual("COMPLETE", result["status"])
        verified = verify_flowmesh_w4_candidate_matrix_run(
            output,
            plan_dir=self.plan,
            route_package_dir=self.fixture.route,
            crosswalk_dir=self.fixture.crosswalk,
            index_package_dir=self.fixture.fixture.index,
        )
        self.assertEqual("VERIFIED", verified["status"])
        self.assertEqual(16, verified["completed_trial_count"])
        self.assertEqual(16, verified["flowmesh_api_task_count"])
        self.assertEqual(16, len(client.workflow["spec"]["graph"]["nodes"]))
        durable_text = "\n".join(
            path.read_text(encoding="utf-8", errors="ignore")
            for path in output.rglob("*")
            if path.is_file()
        )
        self.assertNotIn("http://127.0.0.1", durable_text)
        self.assertNotIn(SECRET, durable_text)


if __name__ == "__main__":
    unittest.main()
