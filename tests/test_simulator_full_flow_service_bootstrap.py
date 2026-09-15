from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path

from pathfinder.simulator.container_contract import plan_container_backend
from pathfinder.simulator.full_flow_logical_routes import (
    compile_full_flow_logical_routes,
)
from pathfinder.simulator.full_flow_service_bootstrap import (
    BOOTSTRAP_NAME,
    LAUNCHERS_NAME,
    FullFlowServiceBootstrapError,
    freeze_full_flow_local_service_bootstrap,
    verify_full_flow_local_service_bootstrap,
)
from pathfinder.simulator.portable import build_portable_execution_plan


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = ROOT / "configs" / "flowmesh_infra_simulator_4x8_smoke.json"
CONTAINER_SPEC = (
    ROOT / "configs" / "flowmesh_infra_container_4x8_contract.json"
)


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]


class FullFlowServiceBootstrapTest(unittest.TestCase):
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

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()

    def setUp(self) -> None:
        self.case_root = Path(tempfile.mkdtemp(dir=self.root))

    def tearDown(self) -> None:
        shutil.rmtree(self.case_root)

    def _freeze(self, output: Path) -> dict:
        return freeze_full_flow_local_service_bootstrap(
            self.logical,
            SCENARIO,
            self.container,
            bootstrap_id="eight-node-local-bootstrap-v1",
            output_dir=output,
        )

    def test_freezes_exact_endpoint_free_startup_contracts(self) -> None:
        output = self.case_root / "bootstrap"
        report = self._freeze(output)
        document = _json(output / BOOTSTRAP_NAME)
        rows = _jsonl(output / LAUNCHERS_NAME)

        self.assertEqual(report["status"], "VERIFIED_LOCAL_SERVICE_BOOTSTRAP")
        self.assertEqual(report["logical_node_count"], 8)
        self.assertEqual(len(rows), document["service_contract_count"])
        self.assertGreater(document["direct_process_contract_count"], 0)
        self.assertGreater(document["embedded_contract_count"], 0)
        self.assertEqual(document["upcloud_required_blocker_count"], 0)
        self.assertTrue(document["full_api_isomorphic_topology_ready"])
        self.assertEqual(2, document["w4_flowmesh_coordinator_count"])
        self.assertEqual(
            ["N7", "N8"], document["w4_flowmesh_coordinator_nodes"]
        )
        self.assertTrue(
            document["w4_flowmesh_coordinators_startable_without_upcloud"]
        )
        self.assertEqual(2, document["w4_dedicated_cache_count"])
        self.assertTrue(document["w4_cache_namespaces_exclusive"])
        self.assertFalse(document["concrete_endpoints_included"])
        self.assertFalse(document["credential_values_included"])

        serialized = output.read_bytes() if output.is_file() else b"".join(
            path.read_bytes() for path in sorted(output.iterdir())
        )
        self.assertNotIn(b"://", serialized)
        self.assertNotIn(b"sk-", serialized)
        self.assertNotIn(b"Bearer ", serialized)

    def test_maps_real_process_and_embedded_contracts(self) -> None:
        output = self.case_root / "bootstrap"
        self._freeze(output)
        by_id = {
            row["service_contract_id"]: row
            for row in _jsonl(output / LAUNCHERS_NAME)
        }

        direct_entrypoints = {
            "N1.hidden-score": "serve-simulator-n1-hidden-oracle",
            "N2.global-index": "serve-simulator-n2-index",
            "N3.raw-data-agent": "serve-data-agent",
            "N4.derived-data-agent": "serve-data-agent",
            "N5.materializer": "serve-simulator-n5-materializer",
            "N6.semantic-inference": "serve-container-node",
            "N7.execution-compute": (
                "serve-simulator-full-flow-semantic-route"
            ),
            "N7.persistent-cache": "serve-simulator-full-flow-cache",
            "N8.execution-compute": (
                "serve-simulator-full-flow-semantic-route"
            ),
            "N8.persistent-cache": "serve-simulator-full-flow-cache",
        }
        for contract_id, command in direct_entrypoints.items():
            row = by_id[contract_id]
            self.assertTrue(row["independently_startable"])
            self.assertIn(command, row["entrypoint_argv_template"])
            self.assertEqual(row["health_route"], "/healthz")

        n1_companions = by_id["N1.hidden-score"]["companion_processes"]
        self.assertEqual(1, len(n1_companions))
        self.assertIn(
            "serve-simulator-n1-remote-verifier",
            n1_companions[0]["entrypoint_argv_template"],
        )

        n6 = by_id["N6.semantic-inference"]
        self.assertEqual(
            ["PATHFINDER_CONTAINER_NODE_TOKEN", "PATHFINDER_SEMANTIC_LLM_API_KEY"],
            n6["credential_env_names"],
        )
        self.assertIn(
            "PATHFINDER_SEMANTIC_LLM_BASE_URL",
            n6["configuration_env_names"],
        )
        self.assertIn(
            "PATHFINDER_SEMANTIC_LLM_MODEL",
            n6["configuration_env_names"],
        )
        self.assertFalse(any(
            name.startswith("UTU_LLM_")
            for name in (
                n6["credential_env_names"] + n6["configuration_env_names"]
            )
        ))

        for contract_id, node_id in (
            ("N2.global-index", "N2"),
            ("N7.local-index", "N7"),
            ("N8.local-index", "N8"),
        ):
            argv = by_id[contract_id]["entrypoint_argv_template"]
            node_flag = argv.index("--node-id")
            self.assertEqual(node_id, argv[node_flag + 1])

        self.assertEqual(
            by_id["N1.trial-control"]["implementation_kind"],
            "flowmesh-runner-embedded",
        )
        self.assertEqual(
            by_id["N7.branch-join"]["implementation_kind"],
            "flowmesh-runner-embedded",
        )
        for node_id in ("N7", "N8"):
            row = by_id[f"{node_id}.execution-compute"]
            self.assertTrue(row["independently_startable"])
            self.assertIn(
                "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET",
                row["credential_env_names"],
            )
            self.assertIn(
                "PATHFINDER_N1_PUBLIC_COMMITMENT_DIR",
                row["artifact_binding_env_names"],
            )
            self.assertNotIn(
                "PATHFINDER_N1_PACKAGE_DIR",
                row["artifact_binding_env_names"],
            )
            self.assertFalse(any(
                "TASK_PLANE" in name
                for name in row["artifact_binding_env_names"]
            ))
        transports = [
            row for key, row in by_id.items() if key.startswith("transport.")
        ]
        self.assertTrue(transports)
        self.assertTrue(all(not row["independently_startable"] for row in transports))

    def test_required_multi_process_roles_use_explicit_companions(self) -> None:
        output = self.case_root / "bootstrap"
        self._freeze(output)
        report = _json(output / BOOTSTRAP_NAME)
        by_id = {
            row["service_contract_id"]: row
            for row in _jsonl(output / LAUNCHERS_NAME)
        }
        self.assertEqual(report["local_code_blockers"], [])
        self.assertEqual(report["incomplete_contract_count"], 0)
        self.assertTrue(report["full_api_isomorphic_topology_ready"])
        self.assertEqual(
            by_id["N4.derived-data-agent"]["companion_processes"][0][
                "implementation_id"
            ],
            "pathfinder.simulator.n4_publication_http",
        )
        self.assertEqual(
            by_id["N5.materializer"]["companion_processes"][0][
                "implementation_id"
            ],
            "pathfinder.simulator.n5_digest_http",
        )
        for node_id in ("N7", "N8"):
            launcher = by_id[f"{node_id}.execution-compute"]
            companions = launcher["companion_processes"]
            self.assertEqual(2, len(companions))
            coordinator = next(
                row
                for row in companions
                if row["implementation_id"]
                == "pathfinder.simulator.full_flow_w4_flowmesh_service"
            )
            cache = next(
                row
                for row in companions
                if row["runtime_service_contract_id"]
                == f"{node_id}.w4-candidate-cache"
            )
            self.assertEqual(
                "exclusive-w4-cache-namespace", cache["state_semantics"]
            )
            self.assertTrue(cache["persistent_state_required"])
            self.assertEqual(
                [
                    "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
                    f"PATHFINDER_{node_id}_W4_CACHE_TOKEN",
                ],
                cache["credential_env_names"],
            )
            self.assertIn(
                f"PATHFINDER_{node_id}_W4_CACHE_MAX_ARTIFACT_BYTES",
                cache["configuration_env_names"],
            )
            self.assertEqual(
                "pathfinder.simulator.full_flow_w4_flowmesh_service",
                coordinator["implementation_id"],
            )
            self.assertEqual(
                f"{node_id}.w4-candidate-coordinator",
                coordinator["runtime_service_contract_id"],
            )
            self.assertIn(
                "serve-simulator-full-flow-w4-flowmesh-coordinator",
                coordinator["entrypoint_argv_template"],
            )
            self.assertEqual("/healthz", coordinator["health_route"])
            self.assertTrue(coordinator["persistent_state_required"])
            self.assertEqual(
                sorted([
                    "N2.global-index",
                    "N3.raw-data-agent",
                    "N4.derived-data-agent",
                    "N6.semantic-inference",
                    "N7.local-index",
                    "N8.local-index",
                    f"{node_id}.w4-candidate-cache",
                ]),
                coordinator["depends_on_runtime_service_contract_ids"],
            )
            self.assertIn(
                f"PATHFINDER_{node_id}_W4_CACHE_BASE_URL",
                coordinator["configuration_env_names"],
            )
            self.assertEqual(
                [
                    "PATHFINDER_CONTAINER_NODE_TOKEN",
                    "PATHFINDER_DATA_AGENT_TOKEN",
                    "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
                    "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET",
                    "PATHFINDER_N2_INDEX_TOKEN",
                    "PATHFINDER_N3_DATA_AGENT_TOKEN",
                    "PATHFINDER_N4_DATA_AGENT_TOKEN",
                    "PATHFINDER_N7_FULL_FLOW_CACHE_TOKEN",
                    "PATHFINDER_N7_INDEX_TOKEN",
                    "PATHFINDER_N7_W4_CACHE_TOKEN",
                    "PATHFINDER_N8_FULL_FLOW_CACHE_TOKEN",
                    "PATHFINDER_N8_INDEX_TOKEN",
                    "PATHFINDER_N8_W4_CACHE_TOKEN",
                ],
                coordinator["credential_env_names"],
            )
            self.assertEqual(
                {
                    "PATHFINDER_FULL_FLOW_W4_CROSSWALK_DIR",
                    "PATHFINDER_FULL_FLOW_W4_ROUTE_PACKAGE_DIR",
                    "PATHFINDER_N2_PACKAGE_DIR",
                    "PATHFINDER_N7_PACKAGE_DIR",
                    "PATHFINDER_N8_PACKAGE_DIR",
                },
                set(coordinator["artifact_binding_env_names"]),
            )
            self.assertIn(
                f"PATHFINDER_{node_id}_W4_COORDINATOR_STATE_DB",
                coordinator["configuration_env_names"],
            )
            self.assertIn(
                f"PATHFINDER_{node_id}_W4_RAW_SAMPLER_SCRATCH_DIR",
                coordinator["configuration_env_names"],
            )
            self.assertEqual(
                [f"PATHFINDER_{node_id}_W4_RAW_SAMPLER_SCRATCH_DIR"],
                coordinator["ephemeral_state_env_names"],
            )
        self.assertTrue(by_id["N4.derived-data-agent"]["contract_complete"])
        self.assertTrue(by_id["N5.materializer"]["contract_complete"])

    def test_freeze_is_byte_deterministic(self) -> None:
        first = self.case_root / "first"
        second = self.case_root / "second"
        self._freeze(first)
        self._freeze(second)
        self.assertEqual(
            {path.name: path.read_bytes() for path in first.iterdir()},
            {path.name: path.read_bytes() for path in second.iterdir()},
        )

    def test_verifier_rejects_tampering(self) -> None:
        output = self.case_root / "bootstrap"
        self._freeze(output)
        path = output / LAUNCHERS_NAME
        path.write_bytes(path.read_bytes().replace(b"N6", b"N9", 1))
        with self.assertRaisesRegex(
            FullFlowServiceBootstrapError,
            "checksum",
        ):
            verify_full_flow_local_service_bootstrap(
                output,
                logical_plan_dir=self.logical,
                scenario_path=SCENARIO,
                container_plan_dir=self.container,
            )

if __name__ == "__main__":
    unittest.main()
