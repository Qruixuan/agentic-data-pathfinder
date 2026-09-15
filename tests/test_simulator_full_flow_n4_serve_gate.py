from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from pathfinder.simulator.full_flow_n4_serve_gate import (
    CHECKSUMS_NAME,
    GATE_NAME,
    FullFlowN4ServeGateError,
    freeze_full_flow_n4_preprovisioned_serve_gate,
    verify_full_flow_n4_preprovisioned_serve_gate,
)
from pathfinder.simulator.full_flow_provisioning_catalog import (
    CATALOG_NAME as PROVISIONING_CATALOG_NAME,
    build_full_flow_provisioning_catalog,
)
from tests import test_simulator_full_flow_artifact_bindings as artifact_fixture
from tests import test_simulator_full_flow_compose_overlay as compose_fixture


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _restamp(root: Path) -> None:
    payload = (root / GATE_NAME).read_bytes()
    (root / CHECKSUMS_NAME).write_text(
        f"{hashlib.sha256(payload).hexdigest()}  {GATE_NAME}\n",
        encoding="utf-8",
    )


class FullFlowN4ServeGateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        compose_fixture.FullFlowComposeOverlayTest.setUpClass()
        artifact_fixture.FullFlowArtifactBindingsTest.setUpClass()

        cls.compose_case = compose_fixture.FullFlowComposeOverlayTest(
            "test_renders_one_safe_full_flow_service_stack"
        )
        cls.compose_case.setUp()
        cls.binding = cls.compose_case._binding("n4-serve-gate")
        cls.overlay = cls.compose_case.case_root / "n4-serve-overlay"
        cls.compose_case._render(cls.overlay, cls.binding)

        cls.artifact_case = artifact_fixture.FullFlowArtifactBindingsTest(
            "test_builds_source_verified_binding_set_for_all_four_objects"
        )
        cls.bindings = cls.artifact_case.build("n4-serve-bindings")
        cls.n4 = artifact_fixture.FullFlowArtifactBindingsTest.n4

        cls.temporary = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temporary.name)
        cls.provisioning = cls.root / "provisioning"
        build_full_flow_provisioning_catalog(
            cls.bindings,
            cls.n4,
            catalog_id="n4-preprovisioned-serve-source-v1",
            output_dir=cls.provisioning,
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()
        cls.compose_case.tearDown()
        artifact_fixture.FullFlowArtifactBindingsTest.tearDownClass()
        compose_fixture.FullFlowComposeOverlayTest.tearDownClass()

    def setUp(self) -> None:
        self.case_root = Path(tempfile.mkdtemp(dir=self.root))

    def tearDown(self) -> None:
        shutil.rmtree(self.case_root)

    def _arguments(self) -> dict[str, Path]:
        return {
            "compose_overlay_dir": self.overlay,
            "service_bootstrap_dir": self.compose_case.bootstrap,
            "deployment_binding_dir": self.binding,
            "logical_plan_dir": self.compose_case.logical,
            "scenario_path": compose_fixture.SCENARIO,
            "container_plan_dir": self.compose_case.container,
            "provisioning_catalog_dir": self.provisioning,
            "artifact_binding_dir": self.bindings,
            "n4_package_dir": self.n4,
        }

    def _freeze(self, name: str) -> Path:
        output = self.case_root / name
        freeze_full_flow_n4_preprovisioned_serve_gate(
            **self._arguments(),
            gate_id="n4-preprovisioned-local-semantic-v1",
            output_dir=output,
        )
        return output

    def _verify(self, output: Path) -> dict:
        return verify_full_flow_n4_preprovisioned_serve_gate(
            output,
            **self._arguments(),
        )

    def test_freezes_exact_read_only_profile_authorization(self) -> None:
        output = self._freeze("complete")
        report = self._verify(output)
        gate = _json(output / GATE_NAME)

        self.assertEqual("VERIFIED", report["status"])
        self.assertEqual("serve-frozen", report["authorized_compose_profile"])
        self.assertEqual(
            "provision-derived", report["prohibited_concurrent_profile"]
        )
        self.assertTrue(report["preprovisioned_snapshot_used"])
        self.assertFalse(report["live_n5_materialization_executed"])
        self.assertTrue(gate["publication_companion_excluded_by_authorized_profile"])
        self.assertFalse(gate["publication_mutation_during_trials_allowed"])
        self.assertFalse(gate["performance_measured"])
        self.assertFalse(gate["monetary_cost_measured"])
        self.assertFalse(gate["upcloud_ready"])

    def test_is_deterministic_and_does_not_modify_sources(self) -> None:
        before = {
            str(path): path.read_bytes()
            for root in (self.overlay, self.provisioning, self.n4)
            for path in root.rglob("*")
            if path.is_file()
        }
        first = self._freeze("deterministic-a")
        second = self._freeze("deterministic-b")
        self.assertEqual(
            (first / GATE_NAME).read_bytes(),
            (second / GATE_NAME).read_bytes(),
        )
        after = {
            str(path): path.read_bytes()
            for root in (self.overlay, self.provisioning, self.n4)
            for path in root.rglob("*")
            if path.is_file()
        }
        self.assertEqual(before, after)

    def test_rejects_tampering_even_when_outer_checksum_is_rewritten(self) -> None:
        output = self._freeze("tampered")
        gate = _json(output / GATE_NAME)
        gate["live_n5_materialization_executed"] = True
        (output / GATE_NAME).write_text(
            json.dumps(gate, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        _restamp(output)
        with self.assertRaisesRegex(
            FullFlowN4ServeGateError,
            "digest failed|scope changed|does not match",
        ):
            self._verify(output)

    def test_rejects_source_drift(self) -> None:
        output = self._freeze("source-drift")
        replacement = self.case_root / "replacement-provisioning"
        shutil.copytree(self.provisioning, replacement)
        catalog_path = replacement / PROVISIONING_CATALOG_NAME
        catalog = _json(catalog_path)
        catalog["catalog_id"] = "different-but-restamped"
        unsigned = dict(catalog)
        unsigned.pop("catalog_sha256")
        catalog["catalog_sha256"] = hashlib.sha256(
            json.dumps(
                unsigned,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        catalog_path.write_text(
            json.dumps(catalog, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        (replacement / "SHA256SUMS").write_text(
            f"{hashlib.sha256(catalog_path.read_bytes()).hexdigest()}  "
            f"{PROVISIONING_CATALOG_NAME}\n",
            encoding="utf-8",
        )
        arguments = self._arguments()
        arguments["provisioning_catalog_dir"] = replacement
        with self.assertRaises(FullFlowN4ServeGateError):
            verify_full_flow_n4_preprovisioned_serve_gate(
                output,
                **arguments,
            )


if __name__ == "__main__":
    unittest.main()
