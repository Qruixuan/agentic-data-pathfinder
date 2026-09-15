from __future__ import annotations

import hashlib
import json
import re
import shutil
import tempfile
import unittest
from pathlib import Path

from pathfinder.simulator.container_contract import plan_container_backend
from pathfinder.simulator import full_flow_compose_overlay as overlay_module
from pathfinder.simulator.full_flow_compose_overlay import (
    CHECKSUMS_NAME,
    COMPOSE_NAME,
    COMPOSE_OVERLAY_SCHEMA_VERSION,
    GATE_NAME,
    LEGACY_COMPOSE_OVERLAY_SCHEMA_VERSION,
    LEGACY_COMPOSE_OVERLAY_SCHEMA_VERSION_V1ALPHA3,
    MANIFEST_NAME,
    FullFlowComposeOverlayError,
    render_full_flow_local_compose_overlay,
    verify_full_flow_local_compose_overlay,
)
from pathfinder.simulator.full_flow_deployment import (
    DEPLOYMENT_SOURCE_SCHEMA_VERSION,
    DEPLOYMENT_SOURCE_SCHEMA_VERSION_V1ALPHA2,
    build_full_flow_deployment_binding,
    full_flow_w4_runtime_service_binding_requirements,
)
from pathfinder.simulator.full_flow_logical_routes import (
    compile_full_flow_logical_routes,
)
from pathfinder.simulator.full_flow_service_bootstrap import (
    freeze_full_flow_local_service_bootstrap,
)
from pathfinder.simulator.portable import build_portable_execution_plan


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = ROOT / "configs" / "flowmesh_infra_simulator_4x8_smoke.json"
CONTAINER_SPEC = (
    ROOT / "configs" / "flowmesh_infra_container_4x8_contract.json"
)

_PERSISTENT = {
    "immutable-hidden-oracle",
    "durable-trial-identity",
    "immutable-content-addressed-artifacts",
    "frozen-index-snapshot",
    "idempotent-content-addressed-output",
    "persistent-with-explicit-cache-scope",
}

_N1_VERIFIER_SERVICE = (
    "pathfinder-full-flow-n1-hidden-score-n1-remote-verification"
)
_W4_COORDINATOR_SERVICES = {
    "n7": (
        "pathfinder-full-flow-n7-execution-compute-w4-flowmesh-service"
    ),
    "n8": (
        "pathfinder-full-flow-n8-execution-compute-w4-flowmesh-service"
    ),
}


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, value: object) -> Path:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def _restamp(output: Path) -> None:
    documents = {
        path.name: path.read_bytes()
        for path in output.iterdir()
        if path.name != CHECKSUMS_NAME
    }
    (output / CHECKSUMS_NAME).write_bytes(b"".join(
        (
            hashlib.sha256(documents[name]).hexdigest()
            + "  "
            + name
            + "\n"
        ).encode("utf-8")
        for name in sorted(documents)
    ))


def _service_block(compose: str, service_name: str) -> str:
    marker = f"  {service_name}:\n"
    start = compose.index(marker)
    next_markers = [
        position
        for position in (
            compose.find("\n  pathfinder-full-flow-", start + len(marker)),
            compose.find("\nnetworks:\n", start + len(marker)),
        )
        if position >= 0
    ]
    return compose[start : min(next_markers)]


class FullFlowComposeOverlayTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temporary.name)
        cls.portable = cls.root / "portable"
        cls.container = cls.root / "container"
        cls.logical = cls.root / "logical"
        cls.bootstrap = cls.root / "bootstrap"
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
        freeze_full_flow_local_service_bootstrap(
            cls.logical,
            SCENARIO,
            cls.container,
            bootstrap_id="local-full-flow-bootstrap-v1",
            output_dir=cls.bootstrap,
        )
        cls.catalog = _json(cls.logical / "logical-service-contracts.json")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()

    def setUp(self) -> None:
        self.case_root = Path(tempfile.mkdtemp(dir=self.root))

    def tearDown(self) -> None:
        shutil.rmtree(self.case_root)

    def _source(self, *, backend: str = "single-host-compose") -> dict:
        bindings = []
        for contract in self.catalog["service_contracts"]:
            network = contract["role"] == "logical-byte-transfer"
            nodes = sorted(contract["logical_node_ids"])
            node = nodes[0].casefold()
            credentials: list[str]
            if network:
                credentials = []
            elif contract["service_contract_id"] == "N6.semantic-inference":
                credentials = ["PATHFINDER_CONTAINER_NODE_TOKEN"]
            elif contract["service_contract_id"] == "N7.execution-compute":
                credentials = ["PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET"]
            else:
                credentials = ["PATHFINDER_BINDING_TOKEN"]
            bindings.append({
                "service_contract_id": contract["service_contract_id"],
                "adapter_id": "compose-contract-http-v1",
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
                    else (
                        f"http://127.0.0.1:{19080 + int(nodes[0][1:])}"
                        if backend == "single-host-compose"
                        else f"https://{node}.private.example"
                    )
                ),
                "credential_env_names": credentials,
                "persistent_state": (
                    contract["state_semantics"] in _PERSISTENT
                ),
            })
        runtime_bindings = []
        for offset, requirement in enumerate(
            full_flow_w4_runtime_service_binding_requirements(),
            start=7,
        ):
            node = requirement["logical_node_id"].casefold()
            runtime_bindings.append({
                **requirement,
                "base_url": (
                    f"http://127.0.0.1:{19180 + offset}"
                    if backend == "single-host-compose"
                    else f"https://{node}.private.example:{19180 + offset}"
                ),
            })
        return {
            "schema_version": DEPLOYMENT_SOURCE_SCHEMA_VERSION_V1ALPHA2,
            "deployment_id": (
                "local-full-flow-v1"
                if backend == "single-host-compose"
                else "upcloud-full-flow-v1"
            ),
            "backend": backend,
            "service_bindings": bindings,
            "runtime_service_bindings": runtime_bindings,
            "network_binding": {
                "adapter_id": (
                    "application-rate-rtt-shaper-v1"
                    if backend == "single-host-compose"
                    else "private-network-measurement-v1"
                ),
                "mode": (
                    "application-shaped-single-host"
                    if backend == "single-host-compose"
                    else "physical-private-network"
                ),
                "measurement_class": (
                    "configured-shaping-conformance"
                    if backend == "single-host-compose"
                    else "measured-private-network"
                ),
                "parameters_fitted": False,
            },
            "trusted_private_http_hosts": (
                []
                if backend == "single-host-compose"
                else [f"n{index}.private.example" for index in range(1, 9)]
            ),
            "credentials_recorded": False,
        }

    def _binding(self, name: str, *, backend: str = "single-host-compose") -> Path:
        source = _write_json(
            self.case_root / f"{name}.json",
            self._source(backend=backend),
        )
        output = self.case_root / f"{name}-binding"
        build_full_flow_deployment_binding(
            self.logical,
            SCENARIO,
            self.container,
            source,
            output_dir=output,
        )
        return output

    def _render(self, output: Path, binding: Path | None = None) -> dict:
        if binding is None:
            binding = self._binding(output.name)
        return render_full_flow_local_compose_overlay(
            self.bootstrap,
            binding,
            logical_plan_dir=self.logical,
            scenario_path=SCENARIO,
            container_plan_dir=self.container,
            overlay_id="local-eight-node-full-flow-v1",
            output_dir=output,
        )

    def _verify(self, output: Path, binding: Path) -> dict:
        return verify_full_flow_local_compose_overlay(
            output,
            service_bootstrap_dir=self.bootstrap,
            deployment_binding_dir=binding,
            logical_plan_dir=self.logical,
            scenario_path=SCENARIO,
            container_plan_dir=self.container,
        )

    def test_renders_one_safe_full_flow_service_stack(self) -> None:
        binding = self._binding("complete")
        output = self.case_root / "overlay"
        report = self._render(output, binding)
        manifest = _json(output / MANIFEST_NAME)
        compose = (output / COMPOSE_NAME).read_text(encoding="utf-8")
        n3_service = "pathfinder-full-flow-n3-raw-data-agent"
        n4_service = "pathfinder-full-flow-n4-derived-data-agent"
        n3_fragment = (
            output / f"compose.service.{n3_service}.yaml"
        ).read_text(encoding="utf-8")
        n4_fragment = (
            output / f"compose.service.{n4_service}.yaml"
        ).read_text(encoding="utf-8")

        self.assertEqual(
            "VERIFIED_LOCAL_COMPOSE_OVERLAY_NOT_LAUNCHED",
            report["status"],
        )
        self.assertEqual(8, report["logical_node_count"])
        self.assertEqual(
            COMPOSE_OVERLAY_SCHEMA_VERSION, report["schema_version"]
        )
        self.assertTrue(report["compose_service_names_dns_safe"])
        self.assertEqual(
            63, report["compose_service_name_dns_label_limit"]
        )
        self.assertEqual(
            "preserve-or-remove-redundant-full-flow-prefix-or-"
            "sha256-suffixed-truncation-v1",
            report["compose_service_name_policy"],
        )
        self.assertEqual(19, report["compose_service_count"])
        self.assertEqual(12, report["primary_service_count"])
        self.assertEqual(7, report["companion_service_count"])
        self.assertEqual(2, report["w4_flowmesh_coordinator_service_count"])
        self.assertEqual(2, report["w4_runtime_service_binding_count"])
        self.assertTrue(report["w4_runtime_bindings_complete"])
        self.assertEqual(2, report["w4_coordinator_deployment_origin_count"])
        self.assertEqual(2, report["w4_dedicated_cache_service_count"])
        self.assertTrue(report["w4_cache_namespaces_exclusive"])
        self.assertEqual(
            ["N7", "N8"], report["w4_flowmesh_coordinator_nodes"]
        )
        self.assertEqual(10, report["persistent_volume_count"])
        self.assertEqual(
            report["persistent_volume_count"],
            len(manifest["persistent_volume_bindings"]),
        )
        self.assertEqual(
            {
                "N1.hidden-score",
                "N3.raw-data-agent",
                "N4.derived-data-agent",
                "N5.materializer",
                "N7.persistent-cache",
                "N7.w4-candidate-cache",
                "N7.w4-candidate-coordinator",
                "N8.persistent-cache",
                "N8.w4-candidate-cache",
                "N8.w4-candidate-coordinator",
            },
            {
                row["runtime_service_contract_id"]
                for row in manifest["persistent_volume_bindings"]
            },
        )
        self.assertEqual(
            [f"N{index}" for index in range(1, 9)],
            manifest["logical_node_ids"],
        )
        self.assertEqual(
            {f"N{index}" for index in range(1, 9)},
            set(manifest["logical_node_service_groups"]),
        )
        self.assertEqual(19, len(manifest["service_inventory"]))
        service_names = [
            row["service_name"] for row in manifest["service_inventory"]
        ]
        self.assertTrue(all(len(name) <= 63 for name in service_names))
        self.assertTrue(all(
            re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", name)
            for name in service_names
        ))
        self.assertIn(_N1_VERIFIER_SERVICE, service_names)
        self.assertTrue(
            set(_W4_COORDINATOR_SERVICES.values()) <= set(service_names)
        )
        self.assertTrue(all(
            row["logical_node_id"] in {f"N{index}" for index in range(1, 9)}
            for row in manifest["service_inventory"]
        ))
        self.assertEqual(
            7,
            sum(
                row["component_kind"] == "companion"
                for row in manifest["service_inventory"]
            ),
        )
        self.assertEqual(
            {
                "N7.w4-candidate-coordinator",
                "N8.w4-candidate-coordinator",
            },
            {
                row["runtime_service_contract_id"]
                for row in manifest["service_inventory"]
                if row["implementation_id"]
                == "pathfinder.simulator.full_flow_w4_flowmesh_service"
            },
        )
        coordinator_rows = {
            row["runtime_service_contract_id"]: row
            for row in manifest["service_inventory"]
            if row["implementation_id"]
            == "pathfinder.simulator.full_flow_w4_flowmesh_service"
        }
        self.assertEqual(
            {
                "N7.w4-candidate-coordinator": hashlib.sha256(
                    b"http://127.0.0.1:19187"
                ).hexdigest(),
                "N8.w4-candidate-coordinator": hashlib.sha256(
                    b"http://127.0.0.1:19188"
                ).hexdigest(),
            },
            {
                contract_id: row["deployment_origin_sha256"]
                for contract_id, row in coordinator_rows.items()
            },
        )
        self.assertTrue(all(
            row["deployment_origin_sha256"] is None
            for row in manifest["service_inventory"]
            if row["component_kind"] == "companion"
            and row["implementation_id"]
            != "pathfinder.simulator.full_flow_w4_flowmesh_service"
        ))
        self.assertTrue(manifest["flowmesh_owns_transport_and_control"])
        self.assertFalse(manifest["services_started"])
        self.assertFalse(manifest["docker_invoked"])
        self.assertEqual(19, manifest["selective_service_unit_count"])
        self.assertEqual(19, report["selective_service_unit_count"])
        self.assertEqual(
            2, manifest["data_agent_complete_package_mount_count"]
        )
        self.assertEqual(
            2, report["data_agent_complete_package_mount_count"]
        )
        self.assertEqual(23, len(list(output.iterdir())))

        self.assertEqual(19, compose.count("pathfinder.logical-node:"))
        self.assertEqual(19, compose.count("\n    entrypoint: []\n"))
        for service in (
            "pathfinder-full-flow-n1-hidden-score",
            _N1_VERIFIER_SERVICE,
            "pathfinder-full-flow-n2-global-index",
            "pathfinder-full-flow-n3-raw-data-agent",
            "pathfinder-full-flow-n4-derived-data-agent",
            "pathfinder-full-flow-n4-derived-data-agent-n4-publication-http",
            "pathfinder-full-flow-n5-materializer",
            "pathfinder-full-flow-n5-materializer-n5-digest-http",
            "pathfinder-full-flow-n6-semantic-inference",
            "pathfinder-full-flow-n7-local-index",
            "pathfinder-full-flow-n7-execution-compute",
            (
                "pathfinder-full-flow-n7-execution-compute-"
                "full-flow-cache"
            ),
            _W4_COORDINATOR_SERVICES["n7"],
            "pathfinder-full-flow-n7-persistent-cache",
            "pathfinder-full-flow-n8-local-index",
            "pathfinder-full-flow-n8-execution-compute",
            (
                "pathfinder-full-flow-n8-execution-compute-"
                "full-flow-cache"
            ),
            _W4_COORDINATOR_SERVICES["n8"],
            "pathfinder-full-flow-n8-persistent-cache",
        ):
            self.assertIn(f"  {service}:\n", compose)
        self.assertEqual(19, compose.count("\n    read_only: true\n"))
        self.assertEqual(19, compose.count('      - "ALL"'))
        self.assertEqual(19, compose.count('      - "no-new-privileges:true"'))
        self.assertEqual(19, compose.count("    healthcheck:"))
        self.assertEqual(19, compose.count("r.read(65537)"))
        self.assertEqual(19, compose.count("p.get('status') == 'ok'"))
        self.assertEqual(
            19,
            compose.count("p.get('credentials_recorded') is False"),
        )
        self.assertIn("PATHFINDER_FULL_FLOW_SERVICE_IMAGE:?set", compose)
        self.assertIn("PATHFINDER_FULL_FLOW_NETWORK_NAME:?set", compose)
        self.assertNotIn("PATHFINDER_N3_DATA_AGENT_MANIFEST", compose)
        self.assertNotIn("PATHFINDER_N4_DATA_AGENT_MANIFEST", compose)
        self.assertIn(
            "/opt/pathfinder/full-flow/n3-package/"
            "config/data-agent-manifest.json",
            compose,
        )
        self.assertIn(
            "${PATHFINDER_N3_PACKAGE_DIR:?set PATHFINDER_N3_PACKAGE_DIR}",
            compose,
        )
        for fragment, node, service in (
            (n3_fragment, "N3", n3_service),
            (n4_fragment, "N4", n4_service),
        ):
            self.assertEqual(1, fragment.count("pathfinder.logical-node:"))
            self.assertIn(f"  {service}:\n", fragment)
            self.assertIn(
                f"/opt/pathfinder/full-flow/{node.casefold()}-package/"
                "config/data-agent-manifest.json",
                fragment,
            )
            self.assertIn(
                f"${{PATHFINDER_{node}_PACKAGE_DIR:?set "
                f"PATHFINDER_{node}_PACKAGE_DIR}}",
                fragment,
            )
            self.assertIn("        read_only: true", fragment)
            self.assertIn("          create_host_path: false", fragment)
            self.assertIn("    entrypoint: []", fragment)
            self.assertNotIn(
                f"PATHFINDER_{node}_DATA_AGENT_MANIFEST", fragment
            )
            self.assertIn(
                "PATHFINDER_DATA_AGENT_TOKEN="
                f"${{PATHFINDER_{node}_DATA_AGENT_TOKEN:?set "
                f"PATHFINDER_{node}_DATA_AGENT_TOKEN}}",
                fragment,
            )
            for unrelated in (
                {f"N{index}" for index in range(1, 9)} - {node}
            ):
                self.assertNotIn(f"PATHFINDER_{unrelated}_", fragment)
        n2_fragment = (
            output / "compose.service.pathfinder-full-flow-n2-global-index.yaml"
        ).read_text(encoding="utf-8")
        self.assertNotIn("\nvolumes:\n", n2_fragment)

        units = {
            row["root_service_name"]: row
            for row in manifest["selective_service_units"]
        }
        for node, service in (("N3", n3_service), ("N4", n4_service)):
            required = units[service]["required_runtime_environment_names"]
            credentials = units[service]["credential_environment_names"]
            self.assertIn(f"PATHFINDER_{node}_DATA_AGENT_TOKEN", required)
            self.assertIn(f"PATHFINDER_{node}_DATA_AGENT_TOKEN", credentials)
            self.assertNotIn("PATHFINDER_DATA_AGENT_TOKEN", required)
            self.assertNotIn("PATHFINDER_DATA_AGENT_TOKEN", credentials)

        expected_fragments = {
            row["compose_file"] for row in manifest["selective_service_units"]
        }
        actual_fragments = {
            path.name for path in output.glob("compose.service.*.yaml")
        }
        self.assertEqual(expected_fragments, actual_fragments)
        for unit in manifest["selective_service_units"]:
            payload = (output / unit["compose_file"]).read_bytes()
            self.assertEqual(
                hashlib.sha256(payload).hexdigest(), unit["compose_sha256"]
            )
            text = payload.decode("utf-8")
            service_names = {
                line[2:-1]
                for line in text.splitlines()
                if line.startswith("  pathfinder-full-flow-")
                and line.endswith(":")
            }
            self.assertEqual(
                set(unit["included_service_names"]), service_names
            )
            placeholders = set(re.findall(
                r"\$\{([A-Z][A-Z0-9_]+):\?set ", text
            ))
            self.assertTrue(
                placeholders
                <= set(unit["required_runtime_environment_names"])
            )
            self.assertFalse(text.endswith("volumes:\n"))
        for node_id in ("n7", "n8"):
            route = _service_block(
                compose,
                f"pathfinder-full-flow-{node_id}-execution-compute",
            )
            self.assertIn(
                "serve-simulator-full-flow-semantic-route",
                route,
            )
            self.assertIn("PATHFINDER_N1_PUBLIC_COMMITMENT_DIR", route)
            self.assertNotIn("PATHFINDER_N1_PACKAGE_DIR", route)
            self.assertNotIn("TASK_PLANE", route)
            w4 = _service_block(
                compose,
                _W4_COORDINATOR_SERVICES[node_id],
            )
            self.assertIn(
                "serve-simulator-full-flow-w4-flowmesh-coordinator", w4
            )
            self.assertIn("PATHFINDER_FULL_FLOW_W4_ROUTE_PACKAGE_DIR", w4)
            self.assertIn("PATHFINDER_FULL_FLOW_W4_CROSSWALK_DIR", w4)
            self.assertIn("PATHFINDER_N7_PACKAGE_DIR", w4)
            self.assertIn("PATHFINDER_N8_PACKAGE_DIR", w4)
            self.assertIn("PATHFINDER_N2_INDEX_TOKEN", w4)
            self.assertIn("PATHFINDER_DATA_AGENT_TOKEN", w4)
            self.assertIn("PATHFINDER_FULL_FLOW_CACHE_TOKEN", w4)
            self.assertIn("PATHFINDER_N7_INDEX_TOKEN", w4)
            self.assertIn("PATHFINDER_N8_INDEX_TOKEN", w4)
            self.assertIn("PATHFINDER_N7_W4_CACHE_BASE_URL", w4)
            self.assertIn("PATHFINDER_N8_W4_CACHE_BASE_URL", w4)
            self.assertIn("PATHFINDER_N7_W4_CACHE_TOKEN", w4)
            self.assertIn("PATHFINDER_N8_W4_CACHE_TOKEN", w4)
            self.assertIn(
                f"PATHFINDER_{node_id.upper()}_W4_COORDINATOR_STATE_DB",
                w4,
            )
            self.assertIn(
                f"PATHFINDER_{node_id.upper()}_W4_RAW_SAMPLER_SCRATCH_DIR",
                w4,
            )
            self.assertIn(
                f"PATHFINDER_{node_id.upper()}_W4_RAW_SAMPLER_"
                "SCRATCH_DIR=/scratch",
                w4,
            )
            self.assertIn(
                "/scratch:rw,noexec,nosuid,nodev,size=2147483648",
                w4,
            )
            self.assertIn("full-flow-n", w4)
            self.assertIn("-w4-candidate-coordinator-state", w4)
            cache = _service_block(
                compose,
                (
                    f"pathfinder-full-flow-{node_id}-execution-compute-"
                    "full-flow-cache"
                ),
            )
            self.assertIn(
                f"PATHFINDER_{node_id.upper()}_W4_CACHE_STATE_DIR", cache
            )
            self.assertIn(
                f"PATHFINDER_{node_id.upper()}_W4_CACHE_TOKEN", cache
            )
            self.assertIn(
                f"PATHFINDER_{node_id.upper()}_W4_CACHE_MAX_ARTIFACT_BYTES",
                cache,
            )
            self.assertNotIn("PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET", cache)
            self.assertIn("PATHFINDER_FULL_FLOW_CACHE_TOKEN", cache)
            self.assertIn(
                f"pathfinder-full-flow-{node_id}-execution-compute-"
                "full-flow-cache:",
                w4,
            )
            self.assertIn('condition: "service_healthy"', w4)

    def test_records_auth_names_without_values_or_bound_origins(self) -> None:
        binding = self._binding("auth")
        output = self.case_root / "overlay"
        self._render(output, binding)
        manifest = _json(output / MANIFEST_NAME)
        compose = (output / COMPOSE_NAME).read_text(encoding="utf-8")

        self.assertEqual(
            "PATHFINDER_CONTAINER_NODE_TOKEN",
            manifest["n6_semantic_bearer_env_name"],
        )
        self.assertEqual(
            "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET",
            manifest["n7_ingress_hmac_env_name"],
        )
        self.assertIn(
            "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET",
            manifest["credential_environment_names"],
        )
        self.assertIn("PATHFINDER_CONTAINER_NODE_TOKEN", compose)
        self.assertIn("PATHFINDER_BINDING_TOKEN", compose)
        self.assertNotIn("PATHFINDER_BINDING_TOKEN=", compose)
        self.assertNotIn("http://127.0.0.1:19081", compose)
        self.assertFalse(manifest["runtime_environment_values_included"])
        self.assertFalse(manifest["credential_values_included"])
        self.assertFalse(manifest["concrete_deployment_endpoints_included"])

    def test_n4_publication_is_a_staged_fail_closed_gate(self) -> None:
        binding = self._binding("gate")
        output = self.case_root / "overlay"
        self._render(output, binding)
        gate = _json(output / GATE_NAME)
        compose = (output / COMPOSE_NAME).read_text(encoding="utf-8")

        self.assertEqual("OPERATOR_GATE_REQUIRED_NOT_SATISFIED", gate["status"])
        self.assertFalse(gate["gate_satisfied"])
        self.assertFalse(gate["compose_automatically_enforces_gate"])
        self.assertTrue(gate["serve_profile_must_not_be_selected_before_gate"])
        self.assertFalse(gate["data_agent_start_before_gate_allowed"])
        self.assertFalse(gate["publication_mutation_during_trials_allowed"])
        self.assertNotIn(
            gate["publication_service"],
            gate["serve_services"],
        )
        self.assertEqual(
            "PATHFINDER_N4_PACKAGE_DIR",
            gate["rebind_env_name"],
        )
        self.assertEqual(
            "PATHFINDER_N4_DATA_AGENT_MANIFEST",
            gate["rebind_manifest_contract_env_name"],
        )
        self.assertEqual(
            "config/data-agent-manifest.json",
            gate["rebind_manifest_relative_path"],
        )
        self.assertTrue(gate["complete_package_read_only_mount_required"])
        self.assertIn(
            "N4-publication-service-stopped",
            gate["required_evidence_before_rebind"],
        )
        self.assertIn('      - "provision-derived"', compose)
        self.assertIn('      - "serve-frozen"', compose)
        self.assertIn(
            'pathfinder.n4-publication-gate: "publish-first"',
            compose,
        )
        self.assertIn(
            'pathfinder.n4-publication-gate: "serve-only-after-rebind"',
            compose,
        )
        for node_id in ("n7", "n8"):
            route = _service_block(
                compose,
                f"pathfinder-full-flow-{node_id}-execution-compute",
            )
            self.assertIn('      - "serve-frozen"', route)
            self.assertNotIn('      - "provision-derived"', route)
        publication = _service_block(
            compose,
            "pathfinder-full-flow-n4-derived-data-agent-n4-publication-http",
        )
        data_agent = _service_block(
            compose,
            "pathfinder-full-flow-n4-derived-data-agent",
        )
        frame = _service_block(
            compose,
            "pathfinder-full-flow-n5-materializer",
        )
        digest = _service_block(
            compose,
            "pathfinder-full-flow-n5-materializer-n5-digest-http",
        )
        self.assertNotIn("PATHFINDER_N4_DATA_AGENT_MANIFEST", publication)
        self.assertNotIn("PATHFINDER_DATA_AGENT_ARTIFACT_SECRET", publication)
        self.assertNotIn("PATHFINDER_N4_PUBLICATION_TOKEN", data_agent)
        self.assertNotIn("PATHFINDER_N5_DIGEST_LLM_API_KEY", frame)
        self.assertIn("PATHFINDER_N5_DIGEST_LLM_API_KEY", digest)

    def test_repeated_render_is_byte_identical(self) -> None:
        binding = self._binding("deterministic")
        first = self.case_root / "first"
        second = self.case_root / "second"
        self._render(first, binding)
        self._render(second, binding)
        self.assertEqual(
            {path.name: path.read_bytes() for path in first.iterdir()},
            {path.name: path.read_bytes() for path in second.iterdir()},
        )

    def test_dns_safe_service_names_preserve_short_names_and_avoid_collisions(
        self,
    ) -> None:
        self.assertEqual(
            "pathfinder-full-flow-n3-raw-data-agent",
            overlay_module._service_name(
                "N3.raw-data-agent",
                "pathfinder.simulator.data_agent",
                True,
                dns_safe=True,
            ),
        )
        first = overlay_module._bounded_service_name(
            "pathfinder-full-flow-" + "a" * 80 + "-first"
        )
        second = overlay_module._bounded_service_name(
            "pathfinder-full-flow-" + "a" * 80 + "-second"
        )
        self.assertNotEqual(first, second)
        self.assertLessEqual(len(first), 63)
        self.assertLessEqual(len(second), 63)
        self.assertRegex(first, r"\A[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")
        self.assertRegex(second, r"\A[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")

    def test_w4_fragments_include_exact_dependency_closures(self) -> None:
        binding = self._binding("w4-closures")
        output = self.case_root / "overlay"
        self._render(output, binding)
        manifest = _json(output / MANIFEST_NAME)
        units = {
            row["root_service_name"]: row
            for row in manifest["selective_service_units"]
        }
        common = {
            "pathfinder-full-flow-n2-global-index",
            "pathfinder-full-flow-n3-raw-data-agent",
            "pathfinder-full-flow-n4-derived-data-agent",
            "pathfinder-full-flow-n6-semantic-inference",
            "pathfinder-full-flow-n7-local-index",
            "pathfinder-full-flow-n8-local-index",
        }
        for node in ("n7", "n8"):
            root = _W4_COORDINATOR_SERVICES[node]
            cache = (
                f"pathfinder-full-flow-{node}-execution-compute-"
                "full-flow-cache"
            )
            self.assertEqual(
                common | {root, cache},
                set(units[root]["included_service_names"]),
            )
            fragment = (output / units[root]["compose_file"]).read_text(
                encoding="utf-8"
            )
            self.assertNotIn("pathfinder-full-flow-n5-materializer:", fragment)
            self.assertNotIn("pathfinder-full-flow-n1-hidden-score:", fragment)

    def test_rechecksummed_extra_file_is_rejected_before_document_reads(
        self,
    ) -> None:
        binding = self._binding("extra-file")
        output = self.case_root / "overlay"
        self._render(output, binding)
        (output / "unexpected.bin").write_bytes(b"not part of the overlay")
        _restamp(output)
        with self.assertRaisesRegex(
            FullFlowComposeOverlayError,
            "file set changed",
        ):
            self._verify(output, binding)

    def test_selective_fragment_tampering_fails_rederivation(self) -> None:
        binding = self._binding("fragment-tamper")
        output = self.case_root / "overlay"
        self._render(output, binding)
        fragment = output / (
            "compose.service."
            "pathfinder-full-flow-n3-raw-data-agent.yaml"
        )
        fragment.write_bytes(
            fragment.read_bytes().replace(
                b"pids_limit: 128", b"pids_limit: 999", 1
            )
        )
        _restamp(output)
        with self.assertRaisesRegex(
            FullFlowComposeOverlayError,
            "does not match the verified source contracts",
        ):
            self._verify(output, binding)

    def test_missing_selective_fragment_fails_even_when_rechecksummed(
        self,
    ) -> None:
        binding = self._binding("fragment-missing")
        output = self.case_root / "overlay"
        self._render(output, binding)
        (output / (
            "compose.service."
            "pathfinder-full-flow-n3-raw-data-agent.yaml"
        )).unlink()
        _restamp(output)
        with self.assertRaisesRegex(
            FullFlowComposeOverlayError,
            "file set changed",
        ):
            self._verify(output, binding)

    def test_legacy_v1alpha2_overlay_remains_verifiable(self) -> None:
        binding = self._binding("legacy-overlay")
        output = self.case_root / "overlay"
        inputs = overlay_module._verified_inputs(
            self.bootstrap,
            binding,
            logical_plan_dir=self.logical,
            scenario_path=SCENARIO,
            container_plan_dir=self.container,
        )
        documents = overlay_module._documents(
            overlay_id="local-eight-node-full-flow-v1",
            bootstrap_root=inputs[0],
            bootstrap_report=inputs[2],
            deployment_report=inputs[3],
            launchers=inputs[4],
            deployment=inputs[5],
            schema_version=LEGACY_COMPOSE_OVERLAY_SCHEMA_VERSION,
        )
        documents[CHECKSUMS_NAME] = overlay_module._checksums(documents)
        output.mkdir()
        for name, payload in documents.items():
            (output / name).write_bytes(payload)

        report = self._verify(output, binding)
        manifest = _json(output / MANIFEST_NAME)
        gate = _json(output / GATE_NAME)
        compose = (output / COMPOSE_NAME).read_text(encoding="utf-8")

        self.assertEqual(
            LEGACY_COMPOSE_OVERLAY_SCHEMA_VERSION,
            report["schema_version"],
        )
        self.assertEqual(0, report["selective_service_unit_count"])
        self.assertEqual(4, len(list(output.iterdir())))
        self.assertNotIn("selective_service_units", manifest)
        self.assertIn("PATHFINDER_N3_DATA_AGENT_MANIFEST", compose)
        self.assertEqual(
            "pathfinder.full-flow-local-compose-stage-gate/v1alpha1",
            gate["schema_version"],
        )
        self.assertEqual(
            "PATHFINDER_N4_DATA_AGENT_MANIFEST", gate["rebind_env_name"]
        )
        self.assertNotIn(
            "complete_package_read_only_mount_required", gate
        )

    def test_legacy_v1alpha3_overlay_remains_byte_verifiable(self) -> None:
        binding = self._binding("legacy-v1alpha3-overlay")
        output = self.case_root / "overlay"
        inputs = overlay_module._verified_inputs(
            self.bootstrap,
            binding,
            logical_plan_dir=self.logical,
            scenario_path=SCENARIO,
            container_plan_dir=self.container,
        )
        documents = overlay_module._documents(
            overlay_id="local-eight-node-full-flow-v1",
            bootstrap_root=inputs[0],
            bootstrap_report=inputs[2],
            deployment_report=inputs[3],
            launchers=inputs[4],
            deployment=inputs[5],
            schema_version=(
                LEGACY_COMPOSE_OVERLAY_SCHEMA_VERSION_V1ALPHA3
            ),
        )
        documents[CHECKSUMS_NAME] = overlay_module._checksums(documents)
        output.mkdir()
        for name, payload in documents.items():
            (output / name).write_bytes(payload)

        report = self._verify(output, binding)
        manifest = _json(output / MANIFEST_NAME)
        compose = (output / COMPOSE_NAME).read_text(encoding="utf-8")

        self.assertEqual(
            LEGACY_COMPOSE_OVERLAY_SCHEMA_VERSION_V1ALPHA3,
            report["schema_version"],
        )
        self.assertEqual(19, report["selective_service_unit_count"])
        self.assertFalse(report["compose_service_names_dns_safe"])
        self.assertNotIn(
            "compose_service_name_policy",
            manifest,
        )
        self.assertIn(
            "pathfinder-full-flow-n1-hidden-score-"
            "full-flow-n1-remote-verification:",
            compose,
        )

    def test_rechecksummed_compose_tampering_fails_rederivation(self) -> None:
        binding = self._binding("tamper")
        output = self.case_root / "overlay"
        self._render(output, binding)
        compose_path = output / COMPOSE_NAME
        compose_path.write_bytes(
            compose_path.read_bytes().replace(b"pids_limit: 128", b"pids_limit: 999", 1)
        )
        _restamp(output)
        with self.assertRaisesRegex(
            FullFlowComposeOverlayError,
            "does not match the verified source contracts",
        ):
            self._verify(output, binding)

    def test_multi_host_binding_is_not_rendered_as_local_compose(self) -> None:
        binding = self._binding("multi-host", backend="multi-host-private-network")
        with self.assertRaisesRegex(
            FullFlowComposeOverlayError,
            "single-host-compose",
        ):
            self._render(self.case_root / "overlay", binding)

    def test_legacy_deployment_binding_is_not_w4_compose_ready(self) -> None:
        source_value = self._source()
        source_value["schema_version"] = DEPLOYMENT_SOURCE_SCHEMA_VERSION
        source_value.pop("runtime_service_bindings")
        source = _write_json(self.case_root / "legacy-source.json", source_value)
        binding = self.case_root / "legacy-binding"
        build_full_flow_deployment_binding(
            self.logical,
            SCENARIO,
            self.container,
            source,
            output_dir=binding,
        )

        with self.assertRaisesRegex(
            FullFlowComposeOverlayError,
            "v1alpha2 W4 runtime bindings",
        ):
            self._render(self.case_root / "legacy-overlay", binding)


if __name__ == "__main__":
    unittest.main()
