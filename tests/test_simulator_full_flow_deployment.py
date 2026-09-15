from __future__ import annotations

import json
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from pathfinder.simulator.container_contract import plan_container_backend
from pathfinder.simulator.full_flow_deployment import (
    DEPLOYMENT_SOURCE_SCHEMA_VERSION,
    DEPLOYMENT_SOURCE_SCHEMA_VERSION_V1ALPHA2,
    FullFlowDeploymentError,
    W4_COORDINATOR_HEALTH_SCHEMA_VERSION,
    build_full_flow_deployment_binding,
    full_flow_w4_runtime_service_binding_requirements,
    preflight_full_flow_deployment,
    verify_full_flow_deployment_binding,
)
from pathfinder.simulator.full_flow_logical_routes import (
    compile_full_flow_logical_routes,
)
from pathfinder.simulator.portable import build_portable_execution_plan


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = ROOT / "configs" / "flowmesh_infra_simulator_4x8_smoke.json"
CONTAINER_SPEC = (
    ROOT / "configs" / "flowmesh_infra_container_4x8_contract.json"
)


def _write_json(path: Path, value: object) -> Path:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


_PERSISTENT = {
    "immutable-hidden-oracle",
    "durable-trial-identity",
    "immutable-content-addressed-artifacts",
    "frozen-index-snapshot",
    "idempotent-content-addressed-output",
    "persistent-with-explicit-cache-scope",
}


class _HealthHandler(BaseHTTPRequestHandler):
    def log_message(self, *_args: object) -> None:
        return None

    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/healthz":
            self.send_error(404)
            return
        health = {
                "status": "ok",
                "node_id": self.server.node_id,
                "credentials_recorded": False,
            }
        runtime_id = getattr(
            self.server,
            "runtime_service_contract_id",
            None,
        )
        if runtime_id is not None:
            health.update({
                "schema_version": W4_COORDINATOR_HEALTH_SCHEMA_VERSION,
                "coordinator_node_id": self.server.node_id,
                "runtime_service_contract_id": runtime_id,
            })
        payload = json.dumps(
            health,
            sort_keys=True,
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


class FullFlowDeploymentTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temporary.name)
        cls.portable = cls.root / "portable"
        cls.container = cls.root / "container"
        cls.logical = cls.root / "logical"
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
        cls.catalog = json.loads(
            (cls.logical / "logical-service-contracts.json").read_text()
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()

    def _source(self, origins: dict[str, str] | None = None) -> dict:
        if origins is None:
            origins = {
                f"N{index}": f"http://127.0.0.1:{19000 + index}"
                for index in range(1, 9)
            }
        bindings = []
        for contract in self.catalog["service_contracts"]:
            network = contract["role"] == "logical-byte-transfer"
            nodes = sorted(contract["logical_node_ids"])
            bindings.append(
                {
                    "service_contract_id": contract["service_contract_id"],
                    "adapter_id": "test-adapter-v1",
                    "logical_node_ids": nodes,
                    "actions": sorted(contract["actions"]),
                    "representation_ids": [
                        "multimodal_digest",
                        "raw_video",
                        "sampled_frame_bundle",
                    ],
                    "base_url": None if network else origins[nodes[0]],
                    "credential_env_names": (
                        [] if network else ["PATHFINDER_TEST_TOKEN"]
                    ),
                    "persistent_state": (
                        contract["state_semantics"] in _PERSISTENT
                    ),
                }
            )
        return {
            "schema_version": DEPLOYMENT_SOURCE_SCHEMA_VERSION,
            "deployment_id": "local-eight-node-v1",
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

    def _build(self, name: str, source: dict) -> Path:
        source_path = _write_json(self.root / f"{name}.source.json", source)
        output = self.root / name
        build_full_flow_deployment_binding(
            self.logical,
            SCENARIO,
            self.container,
            source_path,
            output_dir=output,
        )
        return output

    def _health_server(
        self,
        node_id: str,
        runtime_service_contract_id: str | None = None,
    ) -> tuple[ThreadingHTTPServer, threading.Thread, str]:
        server = ThreadingHTTPServer(("127.0.0.1", 0), _HealthHandler)
        server.node_id = node_id
        server.runtime_service_contract_id = runtime_service_contract_id
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        return server, thread, f"http://{host}:{port}"

    def _v2_source(
        self,
        *,
        origins: dict[str, str] | None = None,
        runtime_origins: dict[str, str] | None = None,
    ) -> dict:
        source = self._source(origins)
        source["schema_version"] = DEPLOYMENT_SOURCE_SCHEMA_VERSION_V1ALPHA2
        if runtime_origins is None:
            runtime_origins = {
                "N7": "http://127.0.0.1:19107",
                "N8": "http://127.0.0.1:19108",
            }
        source["runtime_service_bindings"] = [
            {**row, "base_url": runtime_origins[row["logical_node_id"]]}
            for row in full_flow_w4_runtime_service_binding_requirements()
        ]
        return source

    def test_freezes_complete_capability_checked_binding(self) -> None:
        output = self._build("complete-binding", self._source())
        result = verify_full_flow_deployment_binding(
            output,
            logical_plan_dir=self.logical,
            scenario_path=SCENARIO,
            container_plan_dir=self.container,
        )
        self.assertEqual("VERIFIED", result["status"])
        self.assertTrue(result["capability_coverage_complete"])
        self.assertGreater(result["service_contract_count"], 10)
        text = (output / "full-flow-deployment-binding.json").read_text()
        self.assertNotIn("token-present", text)
        self.assertIn("PATHFINDER_TEST_TOKEN", text)
        self.assertTrue(result["legacy_schema"])
        self.assertFalse(result["w4_runtime_bindings_complete"])
        self.assertFalse(result["pre_upcloud_deployment_schema_ready"])

    def test_v2_freezes_exact_w4_runtime_bindings(self) -> None:
        output = self._build("v2-complete-binding", self._v2_source())
        result = verify_full_flow_deployment_binding(
            output,
            logical_plan_dir=self.logical,
            scenario_path=SCENARIO,
            container_plan_dir=self.container,
        )
        self.assertFalse(result["legacy_schema"])
        self.assertEqual(2, result["runtime_service_binding_count"])
        self.assertTrue(result["w4_runtime_bindings_complete"])
        self.assertTrue(result["pre_upcloud_deployment_schema_ready"])
        self.assertEqual(
            [
                "N7.w4-candidate-coordinator",
                "N8.w4-candidate-coordinator",
            ],
            [
                row["runtime_service_contract_id"]
                for row in result["runtime_service_bindings"]
            ],
        )

    def test_v2_runtime_binding_identity_fails_closed(self) -> None:
        mutations = {
            "missing": lambda source: source["runtime_service_bindings"].pop(),
            "duplicate": lambda source: source[
                "runtime_service_bindings"
            ].__setitem__(1, dict(source["runtime_service_bindings"][0])),
            "id": lambda source: source[
                "runtime_service_bindings"
            ][0].__setitem__(
                "runtime_service_contract_id",
                "N7.wrong-coordinator",
            ),
            "node": lambda source: source["runtime_service_bindings"][0].__setitem__(
                "logical_node_id", "N8"
            ),
            "parent": lambda source: source["runtime_service_bindings"][0].__setitem__(
                "parent_service_contract_id", "N8.execution-compute"
            ),
            "persistence": lambda source: source[
                "runtime_service_bindings"
            ][0].__setitem__("persistent_state", False),
            "credential": lambda source: source["runtime_service_bindings"][0][
                "credential_env_names"
            ].pop(),
            "base-url": lambda source: source[
                "runtime_service_bindings"
            ][0].__setitem__("base_url", "http://127.0.0.1:19007"),
            "health": lambda source: source["runtime_service_bindings"][0].__setitem__(
                "health_schema_version", "wrong-v1"
            ),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                source = self._v2_source()
                mutate(source)
                with self.assertRaises(FullFlowDeploymentError):
                    self._build(f"bad-runtime-{name}", source)

    def test_missing_digest_materialization_capability_fails_closed(self) -> None:
        source = self._source()
        for row in source["service_bindings"]:
            if row["service_contract_id"] == "N5.materializer":
                row["representation_ids"] = ["sampled_frame_bundle"]
        with self.assertRaisesRegex(
            FullFlowDeploymentError,
            "N5.materializer lacks representations.*multimodal_digest",
        ):
            self._build("missing-digest", source)

    def test_multi_host_binding_refuses_loopback(self) -> None:
        source = self._source()
        source["backend"] = "multi-host-private-network"
        source["network_binding"]["mode"] = "physical-private-network"
        with self.assertRaisesRegex(FullFlowDeploymentError, "loopback"):
            self._build("bad-multi-host", source)

    def test_service_origin_cannot_impersonate_multiple_nodes(self) -> None:
        source = self._source({
            f"N{index}": "http://127.0.0.1:19001"
            for index in range(1, 9)
        })
        with self.assertRaisesRegex(
            FullFlowDeploymentError,
            "exactly one logical node",
        ):
            self._build("ambiguous-origin", source)

    def test_https_origin_requires_explicit_host_trust(self) -> None:
        origins = {
            f"N{index}": f"https://node-{index}.private.test"
            for index in range(1, 9)
        }
        source = self._source(origins)
        with self.assertRaisesRegex(
            FullFlowDeploymentError,
            "loopback or explicitly trusted",
        ):
            self._build("untrusted-https", source)

    def test_read_only_preflight_deduplicates_service_origin(self) -> None:
        servers = []
        threads = []
        try:
            origins = {}
            for index in range(1, 9):
                server = ThreadingHTTPServer(
                    ("127.0.0.1", 0),
                    _HealthHandler,
                )
                server.node_id = f"N{index}"
                thread = threading.Thread(
                    target=server.serve_forever,
                    daemon=True,
                )
                thread.start()
                host, port = server.server_address
                origins[f"N{index}"] = f"http://{host}:{port}"
                servers.append(server)
                threads.append(thread)
            output = self._build(
                "preflight-binding",
                self._source(origins),
            )
            result = preflight_full_flow_deployment(
                output,
                logical_plan_dir=self.logical,
                scenario_path=SCENARIO,
                container_plan_dir=self.container,
            )
        finally:
            for server in servers:
                server.shutdown()
                server.server_close()
            for thread in threads:
                thread.join(timeout=5)
        self.assertEqual("READY", result["status"])
        self.assertEqual(8, result["distinct_service_origin_count"])
        self.assertTrue(result["read_only_probe"])
        self.assertFalse(result["workflow_submitted"])

    def test_v2_preflight_requires_exact_runtime_health_identity(self) -> None:
        servers: list[ThreadingHTTPServer] = []
        threads: list[threading.Thread] = []
        try:
            origins: dict[str, str] = {}
            for index in range(1, 9):
                server, thread, origin = self._health_server(f"N{index}")
                servers.append(server)
                threads.append(thread)
                origins[f"N{index}"] = origin
            runtime_origins: dict[str, str] = {}
            for node in ("N7", "N8"):
                runtime_id = f"{node}.w4-candidate-coordinator"
                server, thread, origin = self._health_server(node, runtime_id)
                servers.append(server)
                threads.append(thread)
                runtime_origins[node] = origin
            output = self._build(
                "v2-preflight-binding",
                self._v2_source(
                    origins=origins,
                    runtime_origins=runtime_origins,
                ),
            )
            result = preflight_full_flow_deployment(
                output,
                logical_plan_dir=self.logical,
                scenario_path=SCENARIO,
                container_plan_dir=self.container,
            )
        finally:
            for server in servers:
                server.shutdown()
                server.server_close()
            for thread in threads:
                thread.join(timeout=5)
        self.assertEqual("READY", result["status"])
        self.assertEqual(10, result["distinct_service_origin_count"])
        self.assertEqual(2, result["runtime_service_origin_count"])
        self.assertTrue(result["w4_runtime_preflight_complete"])

    def test_v2_preflight_rejects_wrong_runtime_service_identity(self) -> None:
        servers: list[ThreadingHTTPServer] = []
        threads: list[threading.Thread] = []
        try:
            origins: dict[str, str] = {}
            for index in range(1, 9):
                server, thread, origin = self._health_server(f"N{index}")
                servers.append(server)
                threads.append(thread)
                origins[f"N{index}"] = origin
            runtime_origins: dict[str, str] = {}
            for node in ("N7", "N8"):
                runtime_id = (
                    "N8.w4-candidate-coordinator"
                    if node == "N7"
                    else "N8.w4-candidate-coordinator"
                )
                server, thread, origin = self._health_server(node, runtime_id)
                servers.append(server)
                threads.append(thread)
                runtime_origins[node] = origin
            output = self._build(
                "v2-bad-health-binding",
                self._v2_source(
                    origins=origins,
                    runtime_origins=runtime_origins,
                ),
            )
            with self.assertRaisesRegex(
                FullFlowDeploymentError,
                "wrong runtime service",
            ):
                preflight_full_flow_deployment(
                    output,
                    logical_plan_dir=self.logical,
                    scenario_path=SCENARIO,
                    container_plan_dir=self.container,
                )
        finally:
            for server in servers:
                server.shutdown()
                server.server_close()
            for thread in threads:
                thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
