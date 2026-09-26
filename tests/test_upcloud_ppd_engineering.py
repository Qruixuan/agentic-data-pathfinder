"""No-network contract checks for the isolated PPD engineering freeze."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from experiments.upcloud_ppd_20260925.freeze_engineering import (
    PLANS,
    REPS,
    _config,
    _registry,
    _verify_access_bindings,
    _write_json,
)
from pathfinder.config import load_config
from pathfinder.data_agent_manifest import DataAgentBindingMismatchError
from pathfinder.distributed.registry import build_endpoint_registry


class PpdEngineeringFreezeTests(unittest.TestCase):
    def test_one_access_and_three_actual_representation_names(self) -> None:
        config = _config(dict.fromkeys(REPS, 1024))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "system.json"
            _write_json(path, config)
            parsed = load_config(path)
        self.assertEqual(parsed.task_classes["video_qa"].max_accesses, 1)
        self.assertEqual(set(parsed.representations), set(REPS))
        self.assertEqual(set(parsed.designs), set(PLANS))

    def test_every_design_representation_has_one_explicit_endpoint(self) -> None:
        registry = build_endpoint_registry(_registry(), source_sha256="0" * 64)
        self.assertEqual(len(registry.placement), len(PLANS) * len(REPS))
        for design in PLANS:
            for rep in REPS:
                route = registry.route(
                    design_id=design, representation_id=rep,
                )
                self.assertEqual(route.rule, "explicit-design-representation")
                self.assertTrue(
                    registry.endpoint(route.endpoint_id)
                    .private_http_service_name.startswith(
                        "pathfinder-full-flow-"
                    )
                )
        self.assertNotEqual(
            registry.route(
                design_id=PLANS[0], representation_id="multimodal_digest",
            ).endpoint_id,
            registry.route(
                design_id=PLANS[1], representation_id="multimodal_digest",
            ).endpoint_id,
        )
        self.assertIsNone(registry.default_endpoint_id)

    def test_all_six_access_bindings_resolve_without_network(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, registry, packages = self._access_fixture(root)
            _verify_access_bindings(
                config, registry, packages, "public-test-object",
            )

    def test_preflight_rejects_legacy_location_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, registry, packages = self._access_fixture(root)
            payload = json.loads(config.read_text(encoding="utf-8"))
            payload["physical_designs"][0]["paths"]["raw_video"]["location"] = (
                "n3-remote-origin"
            )
            _write_json(config, payload)
            with self.assertRaises(DataAgentBindingMismatchError):
                _verify_access_bindings(
                    config, registry, packages, "public-test-object",
                )

    @staticmethod
    def _access_fixture(root: Path) -> tuple[Path, Path, dict[str, Path]]:
        config = root / "system.json"
        registry = root / "endpoint-registry.json"
        _write_json(config, _config(dict.fromkeys(REPS, 1024)))
        _write_json(registry, _registry())
        packages = {}
        for endpoint_id, node, reps, location in (
            ("n3_raw", "N3", ("raw_video",), "origin-cold"),
            ("n4_remote", "N4", REPS[1:], "origin-warm"),
            ("n4_n7_replica", "N4", REPS[1:], "origin-warm"),
        ):
            package = root / endpoint_id
            manifest_dir = package / "config"
            manifest_dir.mkdir(parents=True)
            manifest = {
                "schema_version": "pathfinder.data-agent-manifest/v1alpha1",
                "node_id": node,
                "require_plan_binding": True,
                "representations": {
                    rep: {
                        "kind": "artifact_uri",
                        "media_type": "application/octet-stream",
                        "path": "artifact.bin",
                        "default_binding": {"location": location},
                        "plan_bindings": {
                            plan: {"location": location} for plan in PLANS
                        },
                    }
                    for rep in reps
                },
            }
            _write_json(manifest_dir / "data-agent-manifest.json", manifest)
            packages[endpoint_id] = package
        return config, registry, packages


if __name__ == "__main__":
    unittest.main()
