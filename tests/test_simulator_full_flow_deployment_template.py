from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path

from pathfinder.simulator.container_contract import plan_container_backend
from pathfinder.simulator.full_flow_deployment import (
    FullFlowDeploymentError,
    build_full_flow_deployment_binding,
)
from pathfinder.simulator.full_flow_deployment_template import (
    CHECKSUMS_NAME,
    REQUIREMENTS_NAME,
    SOURCE_TEMPLATE_NAME,
    FullFlowDeploymentTemplateError,
    generate_full_flow_deployment_source_template,
    validate_completed_full_flow_deployment_source,
    verify_full_flow_deployment_source_template,
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


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, value: object) -> Path:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


class FullFlowDeploymentTemplateTest(unittest.TestCase):
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

    def _generate(self, output: Path) -> dict:
        return generate_full_flow_deployment_source_template(
            self.logical,
            SCENARIO,
            self.container,
            template_id="portable-full-flow-v1",
            output_dir=output,
        )

    def _completed_source(
        self,
        template_dir: Path,
        *,
        backend: str,
    ) -> dict:
        source = _json(template_dir / SOURCE_TEMPLATE_NAME)
        source["deployment_id"] = (
            "compose-eight-node-v1"
            if backend == "single-host-compose"
            else "upcloud-eight-node-v1"
        )
        source["backend"] = backend
        source["trusted_private_http_hosts"] = (
            []
            if backend == "single-host-compose"
            else [
                *[f"n{index}.private.example" for index in range(1, 9)],
                "w4-n7.private.example",
                "w4-n8.private.example",
            ]
        )
        network = source["network_binding"]
        network["adapter_id"] = (
            "application-rate-rtt-shaper-v1"
            if backend == "single-host-compose"
            else "private-network-measurement-v1"
        )
        network["mode"] = (
            "application-shaped-single-host"
            if backend == "single-host-compose"
            else "physical-private-network"
        )
        network["measurement_class"] = (
            "configured-shaping-conformance"
            if backend == "single-host-compose"
            else "measured-private-network"
        )
        for row in source["service_bindings"]:
            row["adapter_id"] = "contract-http-adapter-v1"
            if row["base_url"] is not None:
                if backend == "single-host-compose":
                    node_number = int(row["logical_node_ids"][0][1:])
                    row["base_url"] = (
                        f"http://127.0.0.1:{19080 + node_number}"
                    )
                else:
                    node = row["logical_node_ids"][0].casefold()
                    row["base_url"] = f"https://{node}.private.example"
                row["credential_env_names"] = ["PATHFINDER_SERVICE_TOKEN"]
        for row in source["runtime_service_bindings"]:
            node = row["logical_node_id"]
            row["base_url"] = (
                f"http://127.0.0.1:{19100 + int(node[1:])}"
                if backend == "single-host-compose"
                else f"https://w4-{node.casefold()}.private.example"
            )
        return source

    def _validate(self, source_path: Path, template: Path) -> dict:
        return validate_completed_full_flow_deployment_source(
            source_path,
            template_dir=template,
            logical_plan_dir=self.logical,
            scenario_path=SCENARIO,
            container_plan_dir=self.container,
        )

    def test_template_is_complete_endpoint_free_and_deterministic(self) -> None:
        first = self.case_root / "template-a"
        second = self.case_root / "template-b"
        report = self._generate(first)
        self._generate(second)

        self.assertEqual("VERIFIED_OPERATOR_EDITABLE_TEMPLATE", report["status"])
        self.assertGreater(report["service_contract_count"], 10)
        self.assertGreater(report["unresolved_placeholder_count"], 10)
        self.assertFalse(report["final_binding_ready"])
        self.assertEqual(2, report["runtime_service_binding_count"])
        self.assertTrue(report["w4_runtime_bindings_required"])
        self.assertEqual(
            {CHECKSUMS_NAME, REQUIREMENTS_NAME, SOURCE_TEMPLATE_NAME},
            {path.name for path in first.iterdir()},
        )
        for name in (CHECKSUMS_NAME, REQUIREMENTS_NAME, SOURCE_TEMPLATE_NAME):
            self.assertEqual((first / name).read_bytes(), (second / name).read_bytes())
        text = "\n".join(
            (first / name).read_text(encoding="utf-8")
            for name in (REQUIREMENTS_NAME, SOURCE_TEMPLATE_NAME)
        )
        self.assertNotIn("://", text)
        self.assertNotIn("PATHFINDER_SERVICE_TOKEN", text)

    def test_requirements_list_every_contract_and_required_semantics(self) -> None:
        template = self.case_root / "template"
        self._generate(template)
        requirements = _json(template / REQUIREMENTS_NAME)
        source = _json(template / SOURCE_TEMPLATE_NAME)

        contracts = requirements["service_contracts"]
        bindings = source["service_bindings"]
        self.assertEqual(
            requirements["service_contract_count"],
            len(contracts),
        )
        self.assertEqual(
            {row["service_contract_id"] for row in contracts},
            {row["service_contract_id"] for row in bindings},
        )
        for row in contracts:
            self.assertTrue(row["logical_node_ids"])
            self.assertTrue(row["required_actions"])
            self.assertTrue(row["protocol_contracts"])
            self.assertTrue(row["state_semantics"])
        n5 = next(
            row
            for row in contracts
            if row["service_contract_id"] == "N5.materializer"
        )
        self.assertEqual(
            ["multimodal_digest", "sampled_frame_bundle"],
            n5["required_representation_ids"],
        )
        self.assertTrue(n5["persistent_state_required"])
        self.assertEqual(2, requirements["runtime_service_binding_count"])
        self.assertEqual(
            [
                "N7.w4-candidate-coordinator",
                "N8.w4-candidate-coordinator",
            ],
            [
                row["runtime_service_contract_id"]
                for row in requirements["runtime_service_bindings"]
            ],
        )
        self.assertTrue(
            all(
                row["exact_health_identity_required"] is True
                for row in requirements["runtime_service_bindings"]
            )
        )

    def test_template_and_unresolved_skeleton_fail_final_binding(self) -> None:
        template = self.case_root / "template"
        self._generate(template)
        with self.assertRaises(FullFlowDeploymentError):
            build_full_flow_deployment_binding(
                self.logical,
                SCENARIO,
                self.container,
                template / REQUIREMENTS_NAME,
                output_dir=self.case_root / "binding-a",
            )
        with self.assertRaises(FullFlowDeploymentError):
            build_full_flow_deployment_binding(
                self.logical,
                SCENARIO,
                self.container,
                template / SOURCE_TEMPLATE_NAME,
                output_dir=self.case_root / "binding-b",
            )
        with self.assertRaisesRegex(
            FullFlowDeploymentTemplateError,
            "unresolved placeholders",
        ):
            self._validate(template / SOURCE_TEMPLATE_NAME, template)

    def test_same_template_validates_compose_and_upcloud_bindings(self) -> None:
        template = self.case_root / "template"
        self._generate(template)
        for backend in (
            "single-host-compose",
            "multi-host-private-network",
        ):
            with self.subTest(backend=backend):
                path = _write_json(
                    self.case_root / f"{backend}.json",
                    self._completed_source(template, backend=backend),
                )
                report = self._validate(path, template)
                self.assertEqual(
                    "VALID_COMPLETED_DEPLOYMENT_SOURCE",
                    report["status"],
                )
                self.assertEqual(backend, report["backend"])
                self.assertTrue(report["capability_coverage_complete"])
                self.assertTrue(report["final_binding_ready"])
                self.assertEqual(2, report["runtime_service_binding_count"])
                self.assertTrue(report["w4_runtime_bindings_complete"])
                self.assertFalse(report["binding_retained"])
                self.assertFalse(report["cloud_calls_made"])

    def test_missing_contract_or_changed_logical_semantics_fails(self) -> None:
        template = self.case_root / "template"
        self._generate(template)
        missing = self._completed_source(
            template,
            backend="single-host-compose",
        )
        missing["service_bindings"].pop()
        changed = self._completed_source(
            template,
            backend="single-host-compose",
        )
        changed["service_bindings"][0]["actions"] = ["different-action"]
        for name, source in (("missing", missing), ("changed", changed)):
            with self.subTest(name=name):
                path = _write_json(self.case_root / f"{name}.json", source)
                with self.assertRaises(FullFlowDeploymentTemplateError):
                    self._validate(path, template)

    def test_missing_or_changed_runtime_service_fails_closed(self) -> None:
        template = self.case_root / "template"
        self._generate(template)
        missing = self._completed_source(
            template,
            backend="single-host-compose",
        )
        missing["runtime_service_bindings"].pop()
        changed = self._completed_source(
            template,
            backend="single-host-compose",
        )
        changed["runtime_service_bindings"][0][
            "parent_service_contract_id"
        ] = "N8.execution-compute"
        cases = (("missing-runtime", missing), ("changed-runtime", changed))
        for name, source in cases:
            with self.subTest(name=name):
                path = _write_json(self.case_root / f"{name}.json", source)
                with self.assertRaises(FullFlowDeploymentTemplateError):
                    self._validate(path, template)

    def test_placeholder_embedded_in_an_endpoint_is_still_unresolved(self) -> None:
        template = self.case_root / "template"
        self._generate(template)
        source = self._completed_source(
            template,
            backend="multi-host-private-network",
        )
        service = next(
            row
            for row in source["service_bindings"]
            if row["base_url"] is not None
        )
        service["base_url"] = "https://${PRIVATE_HOST}"
        path = _write_json(self.case_root / "embedded-placeholder.json", source)
        with self.assertRaisesRegex(
            FullFlowDeploymentTemplateError,
            "unresolved placeholders",
        ):
            self._validate(path, template)

    def test_credential_values_are_not_accepted_as_environment_names(self) -> None:
        template = self.case_root / "template"
        self._generate(template)
        source = self._completed_source(
            template,
            backend="single-host-compose",
        )
        service = next(
            row for row in source["service_bindings"]
            if row["base_url"] is not None
        )
        service["credential_env_names"] = ["actual-secret-value"]
        path = _write_json(self.case_root / "credential.json", source)
        with self.assertRaisesRegex(
            FullFlowDeploymentTemplateError,
            "environment variable name",
        ):
            self._validate(path, template)

    def test_tampered_template_fails_checksum_verification(self) -> None:
        template = self.case_root / "template"
        self._generate(template)
        with (template / SOURCE_TEMPLATE_NAME).open("ab") as handle:
            handle.write(b"\n")
        with self.assertRaisesRegex(
            FullFlowDeploymentTemplateError,
            "checksum failed",
        ):
            verify_full_flow_deployment_source_template(
                template,
                logical_plan_dir=self.logical,
                scenario_path=SCENARIO,
                container_plan_dir=self.container,
            )

    def test_existing_output_is_not_overwritten(self) -> None:
        template = self.case_root / "template"
        self._generate(template)
        with self.assertRaisesRegex(
            FullFlowDeploymentTemplateError,
            "already exists",
        ):
            self._generate(template)


if __name__ == "__main__":
    unittest.main()
