from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from pathfinder.cli import _parser, main as cli_main


class FullFlowCliTest(unittest.TestCase):
    def _invoke(self, arguments: list[str]) -> tuple[int, dict]:
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            status = cli_main([*arguments, "--compact"])
        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 1, stdout.getvalue())
        return status, json.loads(lines[0])

    def test_parser_exposes_all_offline_and_execution_commands(self) -> None:
        commands = {
            action.dest: set(action.choices or ())
            for action in _parser()._actions
            if action.dest == "command"
        }
        names = commands["command"]
        self.assertTrue({
            "build-full-flow-data-plane",
            "verify-full-flow-data-plane",
            "build-full-flow-compose-binding",
            "verify-full-flow-compose-binding",
            "build-full-flow-deployment-binding",
            "plan-flowmesh-full-flow-trial",
            "verify-flowmesh-full-flow-trial-plan",
            "run-flowmesh-full-flow-trial",
            "verify-flowmesh-full-flow-trial-run",
            "build-flowmesh-full-flow-public-request-v2",
            "plan-flowmesh-full-flow-trial-v2",
            "verify-flowmesh-full-flow-trial-v2-plan",
            "run-flowmesh-full-flow-trial-v2",
            "verify-flowmesh-full-flow-trial-v2-run",
            "build-simulator-n1-hidden-oracle",
            "verify-simulator-n1-hidden-oracle",
            "freeze-simulator-n1-oracle-preselection-commitment",
            "verify-simulator-n1-oracle-preselection-commitment",
            "serve-simulator-n1-hidden-oracle",
            "serve-simulator-n1-remote-verifier",
            "build-simulator-full-flow-task-plane",
            "verify-simulator-full-flow-task-plane",
            "compile-simulator-full-flow-logical-routes",
            "verify-simulator-full-flow-logical-routes",
            "build-simulator-full-flow-provisioning-catalog",
            "verify-simulator-full-flow-provisioning-catalog",
            "build-simulator-full-flow-exact-range-catalog",
            "verify-simulator-full-flow-exact-range-catalog",
            "compile-simulator-full-flow-semantic-matrix",
            "verify-simulator-full-flow-semantic-matrix",
            "build-simulator-full-flow-index-query-plan-catalog",
            "verify-simulator-full-flow-index-query-plan-catalog",
            "freeze-simulator-full-flow-n4-preprovisioned-serve-gate",
            "verify-simulator-full-flow-n4-preprovisioned-serve-gate",
            "freeze-simulator-full-flow-n4-live-serve-gate",
            "verify-simulator-full-flow-n4-live-serve-gate",
            "serve-simulator-full-flow-semantic-route",
            "promote-simulator-full-flow-local-semantic-execution-admission",
            "verify-simulator-full-flow-local-semantic-execution-admission",
            "verify-simulator-full-flow-local-semantic-runtime-package",
            "preflight-simulator-full-flow-semantic-artifacts",
            "verify-simulator-full-flow-semantic-artifact-preflight",
            "freeze-simulator-full-flow-w4-retrieval-contract",
            "verify-simulator-full-flow-w4-retrieval-contract",
            "evaluate-simulator-full-flow-w4-retrieval",
            "verify-simulator-full-flow-w4-retrieval-evaluation",
            "freeze-simulator-full-flow-w4-retrieval-runtime",
            "verify-simulator-full-flow-w4-retrieval-runtime",
            "run-simulator-full-flow-w4-lexical-ranker",
            "verify-simulator-full-flow-w4-ranker-run",
            "freeze-simulator-full-flow-w4-candidate-routes",
            "verify-simulator-full-flow-w4-candidate-routes",
            "run-simulator-full-flow-w4-candidate-conformance",
            "verify-simulator-full-flow-w4-candidate-conformance",
            "build-simulator-full-flow-deployment",
            "verify-simulator-full-flow-deployment",
            "preflight-simulator-full-flow-deployment",
            "generate-simulator-full-flow-deployment-template",
            "verify-simulator-full-flow-deployment-template",
            "validate-simulator-full-flow-deployment-source",
            "build-simulator-n2-index",
            "verify-simulator-n2-index",
            "serve-simulator-n2-index",
            "build-simulator-n3-raw-data-plane",
            "verify-simulator-n3-raw-data-plane",
            "build-simulator-n3-indexed-data-plane",
            "verify-simulator-n3-indexed-data-plane",
            "build-simulator-n4-derived-data-plane",
            "verify-simulator-n4-derived-data-plane",
            "serve-simulator-full-flow-cache",
            "serve-simulator-n5-materializer",
            "run-simulator-n5-n4-live-frame-bundle-smoke",
            "verify-simulator-n5-n4-live-frame-bundle-smoke",
            "run-simulator-n5-n4-live-digest-smoke",
            "verify-simulator-n5-n4-live-digest-smoke",
            "freeze-simulator-n5-digest-plan",
            "verify-simulator-n5-digest-plan",
            "run-simulator-n5-digest-materialization",
            "verify-simulator-n5-digest-materialization",
            "freeze-simulator-policy-routes",
            "verify-simulator-policy-routes",
            "freeze-simulator-oed-routes",
            "verify-simulator-oed-routes",
            "freeze-simulator-full-flow-observations",
            "verify-simulator-full-flow-observations",
        }.issubset(names))

    def test_index_service_node_identity_defaults_to_n2_and_accepts_local_nodes(
        self,
    ) -> None:
        default = _parser().parse_args([
            "serve-simulator-n2-index",
            "--package-dir",
            "index-package",
        ])
        self.assertEqual("N2", default.node_id)
        for node_id in ("N7", "N8"):
            parsed = _parser().parse_args([
                "serve-simulator-n2-index",
                "--package-dir",
                "index-package",
                "--node-id",
                node_id,
            ])
            self.assertEqual(node_id, parsed.node_id)

    def test_label_free_v2_request_plan_and_verifiers_are_wired(self) -> None:
        public = {
            "workload_id": "workload-v2",
            "task_class_id": "video_qa",
            "object_id": "object-v2",
            "question": "What happens?",
            "answer_options": [{"option_id": "A", "text": "Action"}],
            "success_scoring_rule": "multiple_choice_exact",
            "task_binding_sha256": "b" * 64,
        }
        frozen = {
            **public,
            "full_flow_request_id": "request-v2",
            "frozen_binding_sha256": "c" * 64,
            "credentials_recorded": False,
        }
        with TemporaryDirectory() as raw:
            root = Path(raw)
            public_path = root / "public-task.json"
            public_path.write_text(json.dumps(public), encoding="utf-8")
            request_path = root / "public-request.json"
            with mock.patch(
                "pathfinder.simulator.full_flow_runtime."
                "build_full_flow_trial_request_v2",
                return_value=frozen,
            ) as build:
                status, payload = self._invoke([
                    "build-flowmesh-full-flow-public-request-v2",
                    "--public-task-binding",
                    str(public_path),
                    "--oracle-id",
                    "oracle-v2",
                    "--full-flow-request-id",
                    "request-v2",
                    "--run-id",
                    "run-v2",
                    "--trial-id",
                    "trial-v2",
                    "--trial-key",
                    "scenario|W1|D2|r0000",
                    "--route-id",
                    "D2",
                    "--requested-location",
                    "origin-warm",
                    "--data-agent-plan-id",
                    "D2",
                    "--artifact-sha256",
                    "a" * 64,
                    "--artifact-size-bytes",
                    "481280",
                    "--object-catalog-version",
                    "catalog-v2",
                    "--expected-model",
                    "vision-model-v2",
                    "--output",
                    str(request_path),
                ])
            self.assertEqual(0, status)
            self.assertEqual(
                "FROZEN_PUBLIC_FULL_FLOW_REQUEST_V2",
                payload["status"],
            )
            self.assertEqual(frozen, json.loads(request_path.read_text()))
            self.assertEqual("oracle-v2", build.call_args.kwargs["oracle_id"])
            self.assertEqual("D2", build.call_args.kwargs["route_config"].route_id)

            with mock.patch(
                "pathfinder.integrations.flowmesh.full_flow_trial."
                "plan_flowmesh_full_flow_trial_v2",
                return_value={"status": "FROZEN_FULL_FLOW_TRIAL_V2"},
            ) as plan:
                status, payload = self._invoke([
                    "plan-flowmesh-full-flow-trial-v2",
                    "--public-request",
                    str(request_path),
                    "--data-plane-package",
                    "n4-package",
                    "--deployment-binding",
                    "deployment.json",
                    "--route-id",
                    "D2",
                    "--requested-location",
                    "origin-warm",
                    "--data-agent-plan-id",
                    "D2",
                    "--output-dir",
                    "v2-plan",
                ])
            self.assertEqual(0, status)
            self.assertEqual("FROZEN_FULL_FLOW_TRIAL_V2", payload["status"])
            self.assertEqual(frozen, plan.call_args.kwargs["public_request"])
            self.assertEqual(
                "D2", plan.call_args.kwargs["route_config"].route_id
            )

        with mock.patch(
            "pathfinder.integrations.flowmesh.full_flow_trial."
            "verify_flowmesh_full_flow_trial_v2_plan",
            return_value={"status": "VERIFIED"},
        ) as verify_plan:
            status, payload = self._invoke([
                "verify-flowmesh-full-flow-trial-v2-plan",
                "--plan-dir",
                "v2-plan",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        verify_plan.assert_called_once_with(Path("v2-plan"))

        with mock.patch(
            "pathfinder.integrations.flowmesh.full_flow_trial."
            "verify_flowmesh_full_flow_trial_v2_run",
            return_value={"status": "VERIFIED"},
        ) as verify_run:
            status, payload = self._invoke([
                "verify-flowmesh-full-flow-trial-v2-run",
                "--run-dir",
                "v2-run",
                "--plan-dir",
                "v2-plan",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        verify_run.assert_called_once_with(
            Path("v2-run"),
            plan_dir=Path("v2-plan"),
            n1_oracle_package_dir=None,
            n1_evidence_secret=None,
        )

    def test_n2_index_build_and_verify_are_wired_offline(self) -> None:
        with mock.patch(
            "pathfinder.simulator.index_service.build_n2_index_package",
            return_value={"status": "FROZEN_PORTABLE_INDEX"},
        ) as build:
            status, payload = self._invoke([
                "build-simulator-n2-index",
                "--source-manifest",
                "visible-index-source.json",
                "--output-dir",
                "n2-index",
            ])
        self.assertEqual(0, status)
        self.assertEqual("FROZEN_PORTABLE_INDEX", payload["status"])
        build.assert_called_once_with(
            Path("visible-index-source.json"),
            output_dir=Path("n2-index"),
        )

        with mock.patch(
            "pathfinder.simulator.index_service.verify_n2_index_package",
            return_value={"status": "VERIFIED"},
        ) as verify:
            status, payload = self._invoke([
                "verify-simulator-n2-index",
                "--output-dir",
                "n2-index",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        verify.assert_called_once_with(Path("n2-index"))

    def test_n1_oracle_build_and_verify_are_wired_offline(self) -> None:
        with mock.patch(
            "pathfinder.simulator.hidden_oracle.build_n1_oracle_package",
            return_value={"status": "FROZEN_HIDDEN_ORACLE"},
        ) as build:
            status, payload = self._invoke([
                "build-simulator-n1-hidden-oracle",
                "--label-source",
                "hidden-labels.source.json",
                "--output-dir",
                "n1-oracle",
            ])
        self.assertEqual(0, status)
        self.assertEqual("FROZEN_HIDDEN_ORACLE", payload["status"])
        build.assert_called_once_with(
            Path("hidden-labels.source.json"),
            output_dir=Path("n1-oracle"),
        )

        with mock.patch(
            "pathfinder.simulator.hidden_oracle.verify_n1_oracle_package",
            return_value={"status": "VERIFIED"},
        ) as verify:
            status, payload = self._invoke([
                "verify-simulator-n1-hidden-oracle",
                "--output-dir",
                "n1-oracle",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        verify.assert_called_once_with(Path("n1-oracle"))

    def test_n1_oracle_commitment_commands_are_wired_offline(self) -> None:
        with mock.patch(
            "pathfinder.simulator.hidden_oracle_commitment."
            "freeze_n1_oracle_preselection_commitment",
            return_value={
                "status": "FROZEN_PRESELECTION_ORACLE_COMMITMENT"
            },
        ) as freeze:
            status, payload = self._invoke([
                "freeze-simulator-n1-oracle-preselection-commitment",
                "--oracle-package-dir",
                "n1-oracle",
                "--commitment-id",
                "oracle-commitment-v1",
                "--output-dir",
                "oracle-commitment",
            ])
        self.assertEqual(0, status)
        self.assertEqual(
            "FROZEN_PRESELECTION_ORACLE_COMMITMENT",
            payload["status"],
        )
        freeze.assert_called_once_with(
            Path("n1-oracle"),
            commitment_id="oracle-commitment-v1",
            output_dir=Path("oracle-commitment"),
        )

        with mock.patch(
            "pathfinder.simulator.hidden_oracle_commitment."
            "verify_n1_oracle_preselection_commitment",
            return_value={"status": "VERIFIED"},
        ) as verify:
            status, payload = self._invoke([
                "verify-simulator-n1-oracle-preselection-commitment",
                "--commitment-dir",
                "oracle-commitment",
                "--oracle-package-dir",
                "n1-oracle",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        verify.assert_called_once_with(
            Path("oracle-commitment"),
            oracle_package_dir=Path("n1-oracle"),
        )

    def test_full_flow_task_plane_build_and_verify_are_wired(self) -> None:
        with mock.patch(
            "pathfinder.simulator.full_flow_tasks.build_full_flow_task_plane",
            return_value={"status": "FROZEN_PUBLIC_PRIVATE_TASK_PLANE"},
        ) as build:
            status, payload = self._invoke([
                "build-simulator-full-flow-task-plane",
                "--semantic-spec",
                "task-a.json",
                "--semantic-spec",
                "task-b.json",
                "--task-plane-id",
                "tasks-v1",
                "--oracle-id",
                "oracle-v1",
                "--output-dir",
                "task-plane",
            ])
        self.assertEqual(0, status)
        self.assertEqual("FROZEN_PUBLIC_PRIVATE_TASK_PLANE", payload["status"])
        build.assert_called_once_with(
            [Path("task-a.json"), Path("task-b.json")],
            task_plane_id="tasks-v1",
            oracle_id="oracle-v1",
            output_dir=Path("task-plane"),
        )

        with mock.patch(
            "pathfinder.simulator.full_flow_tasks.verify_full_flow_task_plane",
            return_value={"status": "VERIFIED"},
        ) as verify:
            status, payload = self._invoke([
                "verify-simulator-full-flow-task-plane",
                "--output-dir",
                "task-plane",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        verify.assert_called_once_with(Path("task-plane"))

    def test_artifact_binding_build_and_verify_are_wired(self) -> None:
        common = [
            "--logical-plan-dir",
            "logical-routes",
            "--scenario",
            "scenario.json",
            "--container-plan-dir",
            "container-plan",
            "--task-plane-dir",
            "task-plane",
            "--n3-package-dir",
            "n3-package",
            "--n4-package-dir",
            "n4-package",
        ]
        with mock.patch(
            "pathfinder.simulator.full_flow_artifact_bindings."
            "build_full_flow_artifact_bindings",
            return_value={"status": "FROZEN_VERIFIED_ARTIFACT_BINDINGS"},
        ) as build:
            status, payload = self._invoke([
                "build-simulator-full-flow-artifact-bindings",
                *common,
                "--binding-set-id",
                "bindings-v1",
                "--output-dir",
                "bindings",
            ])
        self.assertEqual(0, status)
        self.assertEqual(
            "FROZEN_VERIFIED_ARTIFACT_BINDINGS",
            payload["status"],
        )
        build.assert_called_once_with(
            Path("logical-routes"),
            Path("scenario.json"),
            Path("container-plan"),
            Path("task-plane"),
            Path("n3-package"),
            Path("n4-package"),
            binding_set_id="bindings-v1",
            output_dir=Path("bindings"),
        )

        with mock.patch(
            "pathfinder.simulator.full_flow_artifact_bindings."
            "verify_full_flow_artifact_bindings",
            return_value={"status": "VERIFIED"},
        ) as verify:
            status, payload = self._invoke([
                "verify-simulator-full-flow-artifact-bindings",
                "--output-dir",
                "bindings",
                *common,
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        verify.assert_called_once_with(
            Path("bindings"),
            Path("logical-routes"),
            Path("scenario.json"),
            Path("container-plan"),
            Path("task-plane"),
            Path("n3-package"),
            Path("n4-package"),
        )

    def test_exact_range_catalog_build_and_verify_are_wired(self) -> None:
        with mock.patch(
            "pathfinder.simulator.full_flow_exact_range_catalog."
            "build_full_flow_exact_range_catalog",
            return_value={"status": "FROZEN_EXACT_FULL_OBJECT_FALLBACK"},
        ) as build:
            status, payload = self._invoke([
                "build-simulator-full-flow-exact-range-catalog",
                "--n3-package-dir",
                "n3-package",
                "--catalog-id",
                "exact-ranges-v1",
                "--output-dir",
                "exact-ranges",
            ])
        self.assertEqual(0, status)
        self.assertEqual(
            "FROZEN_EXACT_FULL_OBJECT_FALLBACK",
            payload["status"],
        )
        build.assert_called_once_with(
            Path("n3-package"),
            catalog_id="exact-ranges-v1",
            output_dir=Path("exact-ranges"),
        )

        with mock.patch(
            "pathfinder.simulator.full_flow_exact_range_catalog."
            "verify_full_flow_exact_range_catalog",
            return_value={"status": "VERIFIED"},
        ) as verify:
            status, payload = self._invoke([
                "verify-simulator-full-flow-exact-range-catalog",
                "--catalog-dir",
                "exact-ranges",
                "--n3-package-dir",
                "n3-package",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        verify.assert_called_once_with(
            Path("exact-ranges"),
            Path("n3-package"),
        )

    def test_provisioning_catalog_build_and_verify_are_wired(self) -> None:
        with mock.patch(
            "pathfinder.simulator.full_flow_provisioning_catalog."
            "build_full_flow_provisioning_catalog",
            return_value={"status": "FROZEN_PREPROVISIONED_DERIVED_CATALOG"},
        ) as build:
            status, payload = self._invoke([
                "build-simulator-full-flow-provisioning-catalog",
                "--artifact-binding-dir",
                "artifact-bindings",
                "--n4-package-dir",
                "n4-package",
                "--catalog-id",
                "provisioning-v1",
                "--output-dir",
                "provisioning",
            ])
        self.assertEqual(0, status)
        self.assertEqual(
            "FROZEN_PREPROVISIONED_DERIVED_CATALOG",
            payload["status"],
        )
        build.assert_called_once_with(
            Path("artifact-bindings"),
            Path("n4-package"),
            catalog_id="provisioning-v1",
            output_dir=Path("provisioning"),
        )

        with mock.patch(
            "pathfinder.simulator.full_flow_provisioning_catalog."
            "verify_full_flow_provisioning_catalog",
            return_value={"status": "VERIFIED"},
        ) as verify:
            status, payload = self._invoke([
                "verify-simulator-full-flow-provisioning-catalog",
                "--catalog-dir",
                "provisioning",
                "--artifact-binding-dir",
                "artifact-bindings",
                "--n4-package-dir",
                "n4-package",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        verify.assert_called_once_with(
            Path("provisioning"),
            artifact_binding_dir=Path("artifact-bindings"),
            n4_package_dir=Path("n4-package"),
        )

    def test_n4_preprovisioned_serve_gate_freeze_and_verify_are_wired(
        self,
    ) -> None:
        common = [
            "--compose-overlay-dir",
            "overlay",
            "--service-bootstrap-dir",
            "bootstrap",
            "--deployment-binding-dir",
            "deployment",
            "--logical-plan-dir",
            "logical-routes",
            "--scenario",
            "scenario.json",
            "--container-plan-dir",
            "container-plan",
            "--provisioning-catalog-dir",
            "provisioning",
            "--artifact-binding-dir",
            "artifact-bindings",
            "--n4-package-dir",
            "n4-package",
        ]
        with mock.patch(
            "pathfinder.simulator.full_flow_n4_serve_gate."
            "freeze_full_flow_n4_preprovisioned_serve_gate",
            return_value={"status": "VERIFIED"},
        ) as freeze:
            status, payload = self._invoke([
                "freeze-simulator-full-flow-n4-preprovisioned-serve-gate",
                *common,
                "--gate-id",
                "n4-local-serve-v1",
                "--output-dir",
                "n4-gate",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        freeze.assert_called_once_with(
            Path("overlay"),
            Path("bootstrap"),
            Path("deployment"),
            Path("logical-routes"),
            Path("scenario.json"),
            Path("container-plan"),
            Path("provisioning"),
            Path("artifact-bindings"),
            Path("n4-package"),
            gate_id="n4-local-serve-v1",
            output_dir=Path("n4-gate"),
        )

        with mock.patch(
            "pathfinder.simulator.full_flow_n4_serve_gate."
            "verify_full_flow_n4_preprovisioned_serve_gate",
            return_value={"status": "VERIFIED"},
        ) as verify:
            status, payload = self._invoke([
                "verify-simulator-full-flow-n4-preprovisioned-serve-gate",
                "--gate-dir",
                "n4-gate",
                *common,
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        verify.assert_called_once_with(
            Path("n4-gate"),
            Path("overlay"),
            Path("bootstrap"),
            Path("deployment"),
            Path("logical-routes"),
            Path("scenario.json"),
            Path("container-plan"),
            Path("provisioning"),
            Path("artifact-bindings"),
            Path("n4-package"),
        )

    def test_full_flow_compose_overlay_render_and_verify_are_wired(self) -> None:
        common = [
            "--service-bootstrap-dir",
            "bootstrap",
            "--deployment-binding-dir",
            "deployment",
            "--logical-plan-dir",
            "logical-routes",
            "--scenario",
            "scenario.json",
            "--container-plan-dir",
            "container-plan",
        ]
        with mock.patch(
            "pathfinder.simulator.full_flow_compose_overlay."
            "render_full_flow_local_compose_overlay",
            return_value={
                "status": "VERIFIED_LOCAL_COMPOSE_OVERLAY_NOT_LAUNCHED"
            },
        ) as render:
            status, payload = self._invoke([
                "render-simulator-full-flow-compose-overlay",
                *common,
                "--overlay-id",
                "overlay-v1",
                "--output-dir",
                "overlay",
            ])
        self.assertEqual(0, status)
        self.assertEqual(
            "VERIFIED_LOCAL_COMPOSE_OVERLAY_NOT_LAUNCHED",
            payload["status"],
        )
        render.assert_called_once_with(
            Path("bootstrap"),
            Path("deployment"),
            logical_plan_dir=Path("logical-routes"),
            scenario_path=Path("scenario.json"),
            container_plan_dir=Path("container-plan"),
            overlay_id="overlay-v1",
            output_dir=Path("overlay"),
        )

        with mock.patch(
            "pathfinder.simulator.full_flow_compose_overlay."
            "verify_full_flow_local_compose_overlay",
            return_value={"status": "VERIFIED"},
        ) as verify:
            status, payload = self._invoke([
                "verify-simulator-full-flow-compose-overlay",
                "--overlay-dir",
                "overlay",
                *common,
            ])
        self.assertEqual(0, status)
        verify.assert_called_once_with(
            Path("overlay"),
            service_bootstrap_dir=Path("bootstrap"),
            deployment_binding_dir=Path("deployment"),
            logical_plan_dir=Path("logical-routes"),
            scenario_path=Path("scenario.json"),
            container_plan_dir=Path("container-plan"),
        )

    def test_semantic_matrix_compile_and_verify_are_wired(self) -> None:
        common = [
            "--logical-plan-dir",
            "logical-routes",
            "--scenario",
            "scenario.json",
            "--container-plan-dir",
            "container-plan",
            "--public-task-set",
            "public-tasks.json",
            "--artifact-bindings",
            "artifact-bindings.json",
        ]
        with mock.patch(
            "pathfinder.simulator.full_flow_semantic_matrix."
            "compile_full_flow_semantic_matrix",
            return_value={"status": "FROZEN_SEMANTIC_MATRIX"},
        ) as compile_matrix:
            status, payload = self._invoke([
                "compile-simulator-full-flow-semantic-matrix",
                *common,
                "--compiler-id",
                "semantic-compiler-v1",
                "--output-dir",
                "semantic-matrix",
            ])
        self.assertEqual(0, status)
        self.assertEqual("FROZEN_SEMANTIC_MATRIX", payload["status"])
        compile_matrix.assert_called_once_with(
            Path("logical-routes"),
            Path("scenario.json"),
            Path("container-plan"),
            Path("public-tasks.json"),
            Path("artifact-bindings.json"),
            output_dir=Path("semantic-matrix"),
            compiler_id="semantic-compiler-v1",
        )

        with mock.patch(
            "pathfinder.simulator.full_flow_semantic_matrix."
            "verify_full_flow_semantic_matrix",
            return_value={"status": "VERIFIED"},
        ) as verify_matrix:
            status, payload = self._invoke([
                "verify-simulator-full-flow-semantic-matrix",
                "--plan-dir",
                "semantic-matrix",
                *common,
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        verify_matrix.assert_called_once_with(
            Path("semantic-matrix"),
            Path("logical-routes"),
            Path("scenario.json"),
            Path("container-plan"),
            Path("public-tasks.json"),
            Path("artifact-bindings.json"),
        )

    def test_w4_retrieval_contract_and_evaluation_are_wired(self) -> None:
        module = "pathfinder.simulator.full_flow_w4_retrieval_contract."
        with mock.patch(
            module + "freeze_full_flow_w4_retrieval_contract",
            return_value={"status": "FROZEN_W4"},
        ) as freeze:
            status, payload = self._invoke([
                "freeze-simulator-full-flow-w4-retrieval-contract",
                "--semantic-matrix-dir",
                "semantic-matrix",
                "--retrieval-config",
                "retrieval.json",
                "--representation-manifest",
                "representations.json",
                "--selected-query-id",
                "query-test",
                "--contract-id",
                "w4-v1",
                "--output-dir",
                "w4-contract",
            ])
        self.assertEqual(0, status)
        self.assertEqual("FROZEN_W4", payload["status"])
        freeze.assert_called_once_with(
            Path("semantic-matrix"),
            Path("retrieval.json"),
            Path("representations.json"),
            selected_query_id="query-test",
            contract_id="w4-v1",
            output_dir=Path("w4-contract"),
        )

        with mock.patch(
            module + "verify_full_flow_w4_retrieval_contract",
            return_value={"status": "VERIFIED"},
        ) as verify_contract:
            status, payload = self._invoke([
                "verify-simulator-full-flow-w4-retrieval-contract",
                "--contract-dir",
                "w4-contract",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        verify_contract.assert_called_once_with(Path("w4-contract"))

        with mock.patch(
            module + "evaluate_full_flow_w4_retrieval",
            return_value={"status": "COMPLETE"},
        ) as evaluate:
            status, payload = self._invoke([
                "evaluate-simulator-full-flow-w4-retrieval",
                "--contract-dir",
                "w4-contract",
                "--observations",
                "w4-observations.json",
                "--output-dir",
                "w4-evaluation",
            ])
        self.assertEqual(0, status)
        self.assertEqual("COMPLETE", payload["status"])
        evaluate.assert_called_once_with(
            Path("w4-contract"),
            Path("w4-observations.json"),
            output_dir=Path("w4-evaluation"),
        )

        with mock.patch(
            module + "verify_full_flow_w4_retrieval_evaluation",
            return_value={"status": "VERIFIED_SOURCE_BOUND"},
        ) as verify_evaluation:
            status, payload = self._invoke([
                "verify-simulator-full-flow-w4-retrieval-evaluation",
                "--output-dir",
                "w4-evaluation",
                "--contract-dir",
                "w4-contract",
                "--observations",
                "w4-observations.json",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED_SOURCE_BOUND", payload["status"])
        verify_evaluation.assert_called_once_with(
            Path("w4-evaluation"),
            contract_dir=Path("w4-contract"),
            observations_path=Path("w4-observations.json"),
        )

    def test_semantic_execution_admission_freeze_and_verify_are_wired(
        self,
    ) -> None:
        common = [
            "--semantic-matrix-dir",
            "semantic-matrix",
            "--deployment-binding-dir",
            "deployment",
            "--logical-plan-dir",
            "logical-routes",
            "--scenario",
            "scenario.json",
            "--container-plan-dir",
            "container-plan",
            "--public-task-set",
            "public-tasks.json",
            "--artifact-bindings",
            "artifact-bindings.json",
            "--n1-oracle-package-dir",
            "oracle-package",
        ]
        with mock.patch(
            "pathfinder.simulator.full_flow_semantic_execution_admission."
            "freeze_full_flow_semantic_execution_admission",
            return_value={"status": "BLOCKED_MISSING_ROUTE_RUNTIME_ADAPTERS"},
        ) as freeze:
            status, payload = self._invoke([
                "freeze-simulator-full-flow-semantic-execution-admission",
                *common,
                "--worker-alias",
                "worker-v1",
                "--admission-id",
                "admission-v1",
                "--output-dir",
                "admission",
            ])
        self.assertEqual(0, status)
        self.assertEqual(
            "BLOCKED_MISSING_ROUTE_RUNTIME_ADAPTERS",
            payload["status"],
        )
        freeze.assert_called_once_with(
            Path("semantic-matrix"),
            Path("deployment"),
            Path("logical-routes"),
            Path("scenario.json"),
            Path("container-plan"),
            Path("public-tasks.json"),
            Path("artifact-bindings.json"),
            Path("oracle-package"),
            worker_alias="worker-v1",
            admission_id="admission-v1",
            output_dir=Path("admission"),
        )

        with mock.patch(
            "pathfinder.simulator.full_flow_semantic_execution_admission."
            "verify_full_flow_semantic_execution_admission",
            return_value={"status": "VERIFIED_BLOCKED"},
        ) as verify:
            status, payload = self._invoke([
                "verify-simulator-full-flow-semantic-execution-admission",
                "--admission-dir",
                "admission",
                *common,
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED_BLOCKED", payload["status"])
        verify.assert_called_once_with(
            Path("admission"),
            Path("semantic-matrix"),
            Path("deployment"),
            Path("logical-routes"),
            Path("scenario.json"),
            Path("container-plan"),
            Path("public-tasks.json"),
            Path("artifact-bindings.json"),
            Path("oracle-package"),
        )

    def test_http_artifact_preflight_commands_are_wired(self) -> None:
        module = "pathfinder.simulator.full_flow_artifact_preflight."
        source_arguments = [
            "--semantic-execution-admission-dir",
            "semantic-admission",
            "--n3-package-dir",
            "n3-package",
            "--n4-package-dir",
            "n4-package",
        ]
        with mock.patch.dict(
            "os.environ",
            {
                "PATHFINDER_N3_DATA_AGENT_TOKEN": "n3-runtime-token",
                "PATHFINDER_N4_DATA_AGENT_TOKEN": "n4-runtime-token",
            },
            clear=True,
        ), mock.patch(
            module + "preflight_full_flow_semantic_artifacts_over_http",
            return_value={"status": "VERIFIED"},
        ) as preflight:
            status, payload = self._invoke([
                "preflight-simulator-full-flow-semantic-artifacts",
                *source_arguments,
                "--n3-data-agent-url",
                "http://127.0.0.1:19083",
                "--n4-data-agent-url",
                "http://127.0.0.1:19084",
                "--preflight-id",
                "artifact-preflight-v1",
                "--timeout-seconds",
                "12",
                "--max-retries",
                "0",
                "--max-artifact-bytes",
                "123456",
                "--telemetry-quiescence-timeout-seconds",
                "4",
                "--simulator-private-http-host",
                "pathfinder-sim-n3-origin-cold",
                "--output-dir",
                "artifact-preflight",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        preflight.assert_called_once_with(
            Path("semantic-admission"),
            Path("n3-package"),
            Path("n4-package"),
            n3_base_url="http://127.0.0.1:19083",
            n4_base_url="http://127.0.0.1:19084",
            n3_token="n3-runtime-token",
            n4_token="n4-runtime-token",
            preflight_id="artifact-preflight-v1",
            output_dir=Path("artifact-preflight"),
            timeout_seconds=12.0,
            max_retries=0,
            max_artifact_bytes=123456,
            telemetry_quiescence_timeout_seconds=4.0,
            simulator_private_http_hosts=(
                "pathfinder-sim-n3-origin-cold",
            ),
        )

        with mock.patch(
            module + "verify_full_flow_semantic_artifact_preflight",
            return_value={"status": "VERIFIED"},
        ) as verify:
            status, payload = self._invoke([
                "verify-simulator-full-flow-semantic-artifact-preflight",
                "--preflight-dir",
                "artifact-preflight",
                *source_arguments,
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        verify.assert_called_once_with(
            Path("artifact-preflight"),
            semantic_execution_admission_dir=Path("semantic-admission"),
            n3_package_dir=Path("n3-package"),
            n4_package_dir=Path("n4-package"),
        )

    def test_http_artifact_preflight_requires_runtime_tokens(self) -> None:
        with mock.patch.dict(
            "os.environ",
            {
                "PATHFINDER_N3_DATA_AGENT_TOKEN": "",
                "PATHFINDER_N4_DATA_AGENT_TOKEN": "",
            },
            clear=True,
        ):
            status, payload = self._invoke([
                "preflight-simulator-full-flow-semantic-artifacts",
                "--semantic-execution-admission-dir",
                "semantic-admission",
                "--n3-package-dir",
                "n3-package",
                "--n4-package-dir",
                "n4-package",
                "--n3-data-agent-url",
                "http://127.0.0.1:19083",
                "--n4-data-agent-url",
                "http://127.0.0.1:19084",
                "--preflight-id",
                "artifact-preflight-v1",
                "--output-dir",
                "artifact-preflight",
            ])
        self.assertEqual(2, status)
        self.assertEqual("error", payload["status"])
        self.assertNotIn("runtime-token", json.dumps(payload))

    def test_local_semantic_admission_commands_are_wired(self) -> None:
        module = (
            "pathfinder.simulator.full_flow_local_semantic_admission."
        )
        sources = [
            "--legacy-admission-dir",
            "legacy-admission",
            "--semantic-matrix-dir",
            "semantic-matrix",
            "--deployment-binding-dir",
            "deployment",
            "--logical-plan-dir",
            "logical-routes",
            "--scenario",
            "scenario.json",
            "--container-plan-dir",
            "container-plan",
            "--public-task-set",
            "public-tasks.json",
            "--artifact-bindings",
            "artifact-bindings.json",
            "--n1-oracle-package-dir",
            "oracle-package",
            "--artifact-preflight-dir",
            "artifact-preflight",
            "--exact-range-catalog-dir",
            "exact-ranges",
            "--n3-package-dir",
            "n3-package",
            "--provisioning-catalog-dir",
            "provisioning",
            "--n4-package-dir",
            "n4-package",
        ]
        with mock.patch(
            module + "promote_full_flow_local_semantic_execution_admission",
            return_value={"status": "PROMOTED_LOCAL_SEMANTIC_EXECUTION"},
        ) as promote:
            status, payload = self._invoke([
                "promote-simulator-full-flow-local-semantic-execution-admission",
                *sources,
                "--semantics-mode",
                "legacy-mcq-local-conformance",
                "--promotion-id",
                "promotion-v1",
                "--output-dir",
                "local-admission",
            ])
        self.assertEqual(0, status)
        self.assertEqual(
            "PROMOTED_LOCAL_SEMANTIC_EXECUTION", payload["status"]
        )
        promote.assert_called_once_with(
            Path("legacy-admission"),
            Path("semantic-matrix"),
            Path("deployment"),
            Path("logical-routes"),
            Path("scenario.json"),
            Path("container-plan"),
            Path("public-tasks.json"),
            Path("artifact-bindings.json"),
            Path("oracle-package"),
            Path("artifact-preflight"),
            Path("exact-ranges"),
            Path("n3-package"),
            Path("provisioning"),
            Path("n4-package"),
            semantics_mode="legacy-mcq-local-conformance",
            promotion_id="promotion-v1",
            output_dir=Path("local-admission"),
        )

        with mock.patch(
            module + "verify_full_flow_local_semantic_execution_admission",
            return_value={
                "status": "VERIFIED_LOCAL_SEMANTIC_CONFORMANCE_INPUTS"
            },
        ) as verify:
            status, payload = self._invoke([
                "verify-simulator-full-flow-local-semantic-execution-admission",
                "--admission-dir",
                "local-admission",
                *sources,
            ])
        self.assertEqual(0, status)
        self.assertEqual(
            "VERIFIED_LOCAL_SEMANTIC_CONFORMANCE_INPUTS",
            payload["status"],
        )
        verify.assert_called_once_with(
            Path("local-admission"),
            Path("legacy-admission"),
            Path("semantic-matrix"),
            Path("deployment"),
            Path("logical-routes"),
            Path("scenario.json"),
            Path("container-plan"),
            Path("public-tasks.json"),
            Path("artifact-bindings.json"),
            Path("oracle-package"),
            Path("artifact-preflight"),
            Path("exact-ranges"),
            Path("n3-package"),
            Path("provisioning"),
            Path("n4-package"),
        )

        with mock.patch(
            module + "verify_full_flow_local_semantic_runtime_package",
            return_value={
                "status": "VERIFIED_PUBLIC_LOCAL_RUNTIME_INPUTS"
            },
        ) as verify_runtime:
            status, payload = self._invoke([
                "verify-simulator-full-flow-local-semantic-runtime-package",
                "--admission-dir",
                "local-admission",
            ])
        self.assertEqual(0, status)
        self.assertEqual(
            "VERIFIED_PUBLIC_LOCAL_RUNTIME_INPUTS", payload["status"]
        )
        verify_runtime.assert_called_once_with(Path("local-admission"))

    def test_index_query_plan_catalog_commands_are_wired(self) -> None:
        module = (
            "pathfinder.simulator.full_flow_index_query_plan_catalog."
        )
        sources = [
            "--local-semantic-admission-dir",
            "local-admission",
            "--n2-index-package-dir",
            "n2-index",
        ]
        with mock.patch(
            module + "build_full_flow_index_query_plan_catalog",
            return_value={"status": "VERIFIED"},
        ) as build:
            status, payload = self._invoke([
                "build-simulator-full-flow-index-query-plan-catalog",
                *sources,
                "--output-dir",
                "query-plans",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        build.assert_called_once_with(
            Path("local-admission"),
            Path("n2-index"),
            output_dir=Path("query-plans"),
        )

        with mock.patch(
            module + "verify_full_flow_index_query_plan_catalog",
            return_value={"status": "VERIFIED"},
        ) as verify:
            status, payload = self._invoke([
                "verify-simulator-full-flow-index-query-plan-catalog",
                "--catalog-dir",
                "query-plans",
                *sources,
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        verify.assert_called_once_with(
            Path("query-plans"),
            local_semantic_admission_dir=Path("local-admission"),
            n2_index_package_dir=Path("n2-index"),
        )

    def test_semantic_route_service_uses_public_sources_and_runtime_env(
        self,
    ) -> None:
        arguments = [
            "serve-simulator-full-flow-semantic-route",
            "--node-id",
            "N8",
            "--local-semantic-admission-dir",
            "local-admission",
            "--n1-public-commitment-dir",
            "n1-public",
            "--artifact-binding-dir",
            "artifact-bindings",
            "--n2-index-package-dir",
            "n2-index",
            "--n3-package-dir",
            "n3-package",
            "--n4-package-dir",
            "n4-package",
            "--exact-range-catalog-dir",
            "exact-ranges",
            "--provisioning-catalog-dir",
            "provisioning",
            "--index-query-plan-catalog-dir",
            "query-plans",
            "--state-dir",
            "route-state",
            "--n2-index-base-url",
            "http://pathfinder-sim-n2:9080",
            "--n7-index-base-url",
            "http://pathfinder-sim-n7:9080",
            "--n8-index-base-url",
            "http://pathfinder-sim-n8:9080",
            "--n3-data-agent-base-url",
            "http://pathfinder-sim-n3:9080",
            "--n4-data-agent-base-url",
            "http://pathfinder-sim-n4:9080",
            "--n7-cache-base-url",
            "http://pathfinder-full-flow-n7-persistent-cache:9080",
            "--n8-cache-base-url",
            "http://pathfinder-full-flow-n8-persistent-cache:9080",
            "--n7-cache-id",
            "cache-n7",
            "--n8-cache-id",
            "cache-n8",
            "--n7-node-health-base-url",
            "http://pathfinder-sim-n7:9080",
            "--n8-node-health-base-url",
            "http://pathfinder-sim-n8:9080",
            "--n6-base-url",
            "http://pathfinder-sim-n6:9080",
            "--n1-base-url",
            "http://pathfinder-full-flow-n1-hidden-score:9080",
            "--n1-verification-base-url",
            (
                "http://pathfinder-full-flow-n1-hidden-score-"
                "n1-remote-verification:9181"
            ),
            "--semantic-model",
            "qwen3.8-27b",
            "--simulator-private-http-hosts",
            "pathfinder-sim-n2,pathfinder-full-flow-n1-hidden-score",
            "--timeout-seconds",
            "45",
            "--max-artifact-bytes",
            "123456",
            "--host",
            "127.0.0.1",
            "--port",
            "19089",
        ]
        environment_names = (
            "PATHFINDER_N2_INDEX_TOKEN",
            "PATHFINDER_N7_INDEX_TOKEN",
            "PATHFINDER_N8_INDEX_TOKEN",
            "PATHFINDER_DATA_AGENT_TOKEN",
            "PATHFINDER_N3_DATA_AGENT_TOKEN",
            "PATHFINDER_N4_DATA_AGENT_TOKEN",
            "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
            "PATHFINDER_N7_FULL_FLOW_CACHE_TOKEN",
            "PATHFINDER_N8_FULL_FLOW_CACHE_TOKEN",
            "PATHFINDER_CONTAINER_NODE_TOKEN",
            "PATHFINDER_N1_ORACLE_TOKEN",
            "PATHFINDER_N1_VERIFICATION_TOKEN",
            "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET",
        )
        environment = {
            name: f"runtime-only-{index:02d}-secret"
            for index, name in enumerate(environment_names)
        }
        assembly = mock.Mock()
        assembly.handler = object()
        with (
            mock.patch.dict("os.environ", environment, clear=True),
            mock.patch(
                "pathfinder.simulator.full_flow_semantic_route_service_factory."
                "FrozenSemanticRouteServiceSources"
            ) as source_type,
            mock.patch(
                "pathfinder.simulator.full_flow_semantic_route_service_factory."
                "RuntimeSemanticServiceInputs"
            ) as runtime_type,
            mock.patch(
                "pathfinder.simulator.full_flow_semantic_route_service_factory."
                "assemble_full_flow_semantic_route_service",
                return_value=assembly,
            ) as assemble,
            mock.patch(
                "pathfinder.simulator.container_node.serve_container_node"
            ) as serve,
        ):
            status = cli_main(arguments)
        self.assertEqual(0, status)
        source_type.assert_called_once_with(
            local_admission_dir=Path("local-admission"),
            n1_public_commitment_dir=Path("n1-public"),
            artifact_binding_dir=Path("artifact-bindings"),
            n2_index_package_dir=Path("n2-index"),
            n3_package_dir=Path("n3-package"),
            n4_package_dir=Path("n4-package"),
            exact_range_catalog_dir=Path("exact-ranges"),
            provisioning_catalog_dir=Path("provisioning"),
            index_query_plan_catalog_dir=Path("query-plans"),
        )
        runtime = runtime_type.call_args.kwargs
        self.assertEqual("N8", runtime["logical_node_id"])
        self.assertEqual(
            {
                # every regular index authenticates with the N2 token
                "N2": environment["PATHFINDER_N2_INDEX_TOKEN"],
                "N7": environment["PATHFINDER_N2_INDEX_TOKEN"],
                "N8": environment["PATHFINDER_N2_INDEX_TOKEN"],
            },
            runtime["index_bearer_tokens"],
        )
        self.assertEqual(
            {
                "N3": environment["PATHFINDER_N3_DATA_AGENT_TOKEN"],
                "N4": environment["PATHFINDER_N4_DATA_AGENT_TOKEN"],
            },
            runtime["data_agent_bearer_tokens"],
        )
        # A node-specific token must never be shadowed by the shared one.
        self.assertNotEqual(
            runtime["data_agent_bearer_tokens"]["N3"],
            runtime["data_agent_bearer_tokens"]["N4"],
        )
        self.assertEqual(
            {
                "N7": environment["PATHFINDER_FULL_FLOW_CACHE_TOKEN"],
                "N8": environment["PATHFINDER_FULL_FLOW_CACHE_TOKEN"],
            },
            runtime["cache_bearer_tokens"],
        )
        self.assertEqual(
            environment["PATHFINDER_N1_VERIFICATION_TOKEN"],
            runtime["n1_verification_bearer_token"],
        )
        self.assertEqual(
            (
                "pathfinder-sim-n2",
                "pathfinder-full-flow-n1-hidden-score",
            ),
            runtime["simulator_private_http_hosts"],
        )
        self.assertEqual(45.0, runtime["timeout_seconds"])
        self.assertEqual(123456, runtime["max_artifact_bytes"])
        assemble.assert_called_once_with(
            source_type.return_value,
            runtime_type.return_value,
            state_dir=Path("route-state"),
        )
        assembly.require_ready.assert_called_once_with()
        serve.assert_called_once_with(
            "N8",
            Path("route-state"),
            host="127.0.0.1",
            port=19089,
            semantic_route_handler=assembly.handler,
        )

    def test_semantic_route_service_fails_before_assembly_without_secrets(
        self,
    ) -> None:
        value_flags = (
            "local-semantic-admission-dir",
            "n1-public-commitment-dir",
            "artifact-binding-dir",
            "n2-index-package-dir",
            "n3-package-dir",
            "n4-package-dir",
            "exact-range-catalog-dir",
            "provisioning-catalog-dir",
            "index-query-plan-catalog-dir",
            "state-dir",
            "n2-index-base-url",
            "n7-index-base-url",
            "n8-index-base-url",
            "n3-data-agent-base-url",
            "n4-data-agent-base-url",
            "n7-cache-base-url",
            "n8-cache-base-url",
            "n7-cache-id",
            "n8-cache-id",
            "n7-node-health-base-url",
            "n8-node-health-base-url",
            "n6-base-url",
            "n1-base-url",
            "n1-verification-base-url",
            "semantic-model",
        )
        arguments = [
            "serve-simulator-full-flow-semantic-route",
            "--node-id",
            "N7",
            *(
                value
                for name in value_flags
                for value in ("--" + name, name)
            ),
            "--port",
            "19087",
        ]
        parsed = _parser().parse_args(arguments)
        self.assertEqual("N7", parsed.node_id)
        stdout = io.StringIO()
        with mock.patch.dict("os.environ", {}, clear=True), redirect_stdout(
            stdout
        ):
            status = cli_main(arguments)
        self.assertEqual(2, status)
        payload = json.loads(stdout.getvalue())
        self.assertIn("runtime-only credential", payload["message"])

    def test_w4_flowmesh_coordinator_accepts_shared_runtime_secrets(
        self,
    ) -> None:
        arguments = [
            "serve-simulator-full-flow-w4-flowmesh-coordinator",
            "--coordinator-node-id",
            "N7",
            "--route-package-dir",
            "w4-routes",
            "--crosswalk-dir",
            "w4-crosswalk",
            "--n2-index-package-dir",
            "n2-index",
            "--n7-index-package-dir",
            "n7-index",
            "--n8-index-package-dir",
            "n8-index",
            "--n2-index-base-url",
            "http://n2.test",
            "--n7-index-base-url",
            "http://n7-index.test",
            "--n8-index-base-url",
            "http://n8-index.test",
            "--n3-data-agent-base-url",
            "http://n3.test",
            "--n4-data-agent-base-url",
            "http://n4.test",
            "--n7-cache-base-url",
            "http://n7-cache.test",
            "--n7-cache-id",
            "cache-n7",
            "--n8-cache-base-url",
            "http://n8-cache.test",
            "--n8-cache-id",
            "cache-n8",
            "--n6-base-url",
            "http://n6.test",
            "--semantic-model",
            "vision-model",
            "--raw-sampler-scratch-dir",
            "scratch",
            "--state-db",
            "state/coordinator.sqlite3",
            "--host",
            "127.0.0.1",
            "--port",
            "19097",
        ]
        environment = {
            "PATHFINDER_N2_INDEX_TOKEN": "shared-index-secret",
            "PATHFINDER_DATA_AGENT_TOKEN": "shared-data-secret",
            "PATHFINDER_FULL_FLOW_CACHE_TOKEN": "shared-cache-secret",
            "PATHFINDER_N7_W4_CACHE_TOKEN": "w4-cache-n7-secret",
            "PATHFINDER_N8_W4_CACHE_TOKEN": "w4-cache-n8-secret",
            "PATHFINDER_CONTAINER_NODE_TOKEN": "semantic-secret",
            "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET": "ingress-secret",
        }
        coordinator = mock.Mock()
        coordinator.health.return_value = {"status": "ok"}
        server = mock.Mock()
        with (
            mock.patch.dict("os.environ", environment, clear=True),
            mock.patch(
                "pathfinder.simulator.full_flow_w4_local_factory."
                "W4LocalRuntimeInputs"
            ) as runtime_type,
            mock.patch(
                "pathfinder.simulator.full_flow_w4_flowmesh_service."
                "build_local_full_flow_w4_flowmesh_coordinator",
                return_value=coordinator,
            ) as build,
            mock.patch(
                "pathfinder.simulator.full_flow_w4_flowmesh_service."
                "create_full_flow_w4_flowmesh_http_server",
                return_value=server,
            ) as create_server,
        ):
            status, payload = self._invoke(arguments)
        self.assertEqual(0, status)
        self.assertEqual("ok", payload["status"])
        runtime = runtime_type.call_args.kwargs
        self.assertEqual(
            {
                "N2": "shared-index-secret",
                "N7": "shared-index-secret",
                "N8": "shared-index-secret",
            },
            runtime["index_bearer_tokens"],
        )
        self.assertEqual(
            {"N3": "shared-data-secret", "N4": "shared-data-secret"},
            runtime["data_agent_bearer_tokens"],
        )
        self.assertEqual(
            {
                "N7": "w4-cache-n7-secret",
                "N8": "w4-cache-n8-secret",
            },
            runtime["cache_bearer_tokens"],
        )
        build.assert_called_once()
        create_server.assert_called_once_with(
            coordinator,
            host="127.0.0.1",
            port=19097,
            hmac_secret="ingress-secret",
        )
        server.serve_forever.assert_called_once_with()
        server.server_close.assert_called_once_with()

    def test_full_flow_logical_route_compile_and_verify_are_wired(self) -> None:
        with mock.patch(
            "pathfinder.simulator.full_flow_logical_routes."
            "compile_full_flow_logical_routes",
            return_value={"status": "FROZEN_ENDPOINT_FREE_LOGICAL_ROUTES"},
        ) as compile_routes:
            status, payload = self._invoke([
                "compile-simulator-full-flow-logical-routes",
                "--scenario",
                "scenario.json",
                "--container-plan-dir",
                "container-plan",
                "--compiler-id",
                "compiler-v2",
                "--output-dir",
                "logical-routes",
            ])
        self.assertEqual(0, status)
        self.assertEqual(
            "FROZEN_ENDPOINT_FREE_LOGICAL_ROUTES",
            payload["status"],
        )
        compile_routes.assert_called_once_with(
            Path("scenario.json"),
            Path("container-plan"),
            output_dir=Path("logical-routes"),
            compiler_id="compiler-v2",
        )

        with mock.patch(
            "pathfinder.simulator.full_flow_logical_routes."
            "verify_full_flow_logical_routes",
            return_value={"status": "VERIFIED"},
        ) as verify:
            status, payload = self._invoke([
                "verify-simulator-full-flow-logical-routes",
                "--plan-dir",
                "logical-routes",
                "--scenario",
                "scenario.json",
                "--container-plan-dir",
                "container-plan",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        verify.assert_called_once_with(
            Path("logical-routes"),
            Path("scenario.json"),
            Path("container-plan"),
        )

    def test_full_flow_deployment_build_verify_and_preflight_are_wired(self) -> None:
        common = [
            "--logical-plan-dir",
            "logical-routes",
            "--scenario",
            "scenario.json",
            "--container-plan-dir",
            "container-plan",
        ]
        with mock.patch(
            "pathfinder.simulator.full_flow_deployment."
            "build_full_flow_deployment_binding",
            return_value={"status": "FROZEN_FULL_FLOW_DEPLOYMENT_BINDING"},
        ) as build:
            status, payload = self._invoke([
                "build-simulator-full-flow-deployment",
                *common,
                "--deployment-source",
                "deployment.source.json",
                "--output-dir",
                "deployment",
            ])
        self.assertEqual(0, status)
        self.assertEqual(
            "FROZEN_FULL_FLOW_DEPLOYMENT_BINDING",
            payload["status"],
        )
        build.assert_called_once_with(
            Path("logical-routes"),
            Path("scenario.json"),
            Path("container-plan"),
            Path("deployment.source.json"),
            output_dir=Path("deployment"),
        )

        with mock.patch(
            "pathfinder.simulator.full_flow_deployment."
            "verify_full_flow_deployment_binding",
            return_value={"status": "VERIFIED"},
        ) as verify:
            status, payload = self._invoke([
                "verify-simulator-full-flow-deployment",
                "--binding-dir",
                "deployment",
                *common,
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        verify.assert_called_once_with(
            Path("deployment"),
            logical_plan_dir=Path("logical-routes"),
            scenario_path=Path("scenario.json"),
            container_plan_dir=Path("container-plan"),
        )

        with mock.patch(
            "pathfinder.simulator.full_flow_deployment."
            "preflight_full_flow_deployment",
            return_value={"status": "READY"},
        ) as preflight:
            status, payload = self._invoke([
                "preflight-simulator-full-flow-deployment",
                "--binding-dir",
                "deployment",
                *common,
                "--timeout-seconds",
                "7.5",
            ])
        self.assertEqual(0, status)
        self.assertEqual("READY", payload["status"])
        preflight.assert_called_once_with(
            Path("deployment"),
            logical_plan_dir=Path("logical-routes"),
            scenario_path=Path("scenario.json"),
            container_plan_dir=Path("container-plan"),
            timeout_seconds=7.5,
        )

    def test_deployment_template_commands_are_wired_offline(self) -> None:
        common = [
            "--logical-plan-dir",
            "logical-routes",
            "--scenario",
            "scenario.json",
            "--container-plan-dir",
            "container-plan",
        ]
        with mock.patch(
            "pathfinder.simulator.full_flow_deployment_template."
            "generate_full_flow_deployment_source_template",
            return_value={"status": "VERIFIED_OPERATOR_EDITABLE_TEMPLATE"},
        ) as generate:
            status, payload = self._invoke([
                "generate-simulator-full-flow-deployment-template",
                *common,
                "--template-id",
                "deployment-template-v1",
                "--output-dir",
                "deployment-template",
            ])
        self.assertEqual(0, status)
        self.assertEqual(
            "VERIFIED_OPERATOR_EDITABLE_TEMPLATE",
            payload["status"],
        )
        generate.assert_called_once_with(
            Path("logical-routes"),
            Path("scenario.json"),
            Path("container-plan"),
            template_id="deployment-template-v1",
            output_dir=Path("deployment-template"),
        )

        with mock.patch(
            "pathfinder.simulator.full_flow_deployment_template."
            "verify_full_flow_deployment_source_template",
            return_value={"status": "VERIFIED_OPERATOR_EDITABLE_TEMPLATE"},
        ) as verify:
            status, payload = self._invoke([
                "verify-simulator-full-flow-deployment-template",
                *common,
                "--template-dir",
                "deployment-template",
            ])
        self.assertEqual(0, status)
        self.assertEqual(
            "VERIFIED_OPERATOR_EDITABLE_TEMPLATE",
            payload["status"],
        )
        verify.assert_called_once_with(
            Path("deployment-template"),
            logical_plan_dir=Path("logical-routes"),
            scenario_path=Path("scenario.json"),
            container_plan_dir=Path("container-plan"),
        )

        with mock.patch(
            "pathfinder.simulator.full_flow_deployment_template."
            "validate_completed_full_flow_deployment_source",
            return_value={"status": "VALID_COMPLETED_DEPLOYMENT_SOURCE"},
        ) as validate:
            status, payload = self._invoke([
                "validate-simulator-full-flow-deployment-source",
                *common,
                "--template-dir",
                "deployment-template",
                "--deployment-source",
                "deployment.source.json",
            ])
        self.assertEqual(0, status)
        self.assertEqual(
            "VALID_COMPLETED_DEPLOYMENT_SOURCE",
            payload["status"],
        )
        validate.assert_called_once_with(
            Path("deployment.source.json"),
            template_dir=Path("deployment-template"),
            logical_plan_dir=Path("logical-routes"),
            scenario_path=Path("scenario.json"),
            container_plan_dir=Path("container-plan"),
        )

    def test_n3_raw_data_plane_build_and_verify_are_wired_offline(self) -> None:
        with mock.patch(
            "pathfinder.simulator.raw_cold_data_plane."
            "build_raw_cold_data_plane_package_from_manifest",
            return_value={"status": "FROZEN_RAW_COLD_DATA_PLANE"},
        ) as build:
            status, payload = self._invoke([
                "build-simulator-n3-raw-data-plane",
                "--binding-manifest",
                "raw-object-bindings.json",
                "--output-dir",
                "n3-raw-data",
            ])
        self.assertEqual(0, status)
        self.assertEqual("FROZEN_RAW_COLD_DATA_PLANE", payload["status"])
        build.assert_called_once_with(
            Path("raw-object-bindings.json"),
            output_dir=Path("n3-raw-data"),
        )

        with mock.patch(
            "pathfinder.simulator.raw_cold_data_plane."
            "verify_raw_cold_data_plane_package",
            return_value={"status": "VERIFIED_RAW_COLD_DATA_PLANE"},
        ) as verify:
            status, payload = self._invoke([
                "verify-simulator-n3-raw-data-plane",
                "--output-dir",
                "n3-raw-data",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED_RAW_COLD_DATA_PLANE", payload["status"])
        verify.assert_called_once_with(Path("n3-raw-data"))

    def test_n3_indexed_data_plane_build_and_verify_are_wired_offline(
        self,
    ) -> None:
        with mock.patch(
            "pathfinder.simulator.n3_indexed_data_plane."
            "build_n3_indexed_data_plane_package",
            return_value={"status": "FROZEN_N3_INDEXED_DATA_PLANE"},
        ) as build:
            status, payload = self._invoke([
                "build-simulator-n3-indexed-data-plane",
                "--source-raw-package-dir",
                "n3-raw-data",
                "--package-id",
                "n3-indexed-v1",
                "--frame-count",
                "8",
                "--jpeg-max-dimension",
                "640",
                "--temporal-start-fraction",
                "0.2",
                "--temporal-end-fraction",
                "0.8",
                "--output-dir",
                "n3-indexed-data",
            ])
        self.assertEqual(0, status)
        self.assertEqual("FROZEN_N3_INDEXED_DATA_PLANE", payload["status"])
        build.assert_called_once()
        positional, keywords = build.call_args
        self.assertEqual((Path("n3-raw-data"),), positional)
        self.assertEqual(Path("n3-indexed-data"), keywords["output_dir"])
        self.assertEqual("n3-indexed-v1", keywords["package_id"])
        policy = keywords["policy"]
        self.assertEqual(8, policy.frame_count)
        self.assertEqual(640, policy.jpeg_max_dimension)
        self.assertEqual(0.2, policy.temporal_start_fraction)
        self.assertEqual(0.8, policy.temporal_end_fraction)

        with mock.patch(
            "pathfinder.simulator.n3_indexed_data_plane."
            "verify_n3_indexed_data_plane_package",
            return_value={"status": "VERIFIED_N3_INDEXED_DATA_PLANE"},
        ) as verify:
            status, payload = self._invoke([
                "verify-simulator-n3-indexed-data-plane",
                "--output-dir",
                "n3-indexed-data",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED_N3_INDEXED_DATA_PLANE", payload["status"])
        verify.assert_called_once_with(Path("n3-indexed-data"))

    def test_service_bootstrap_freeze_and_verify_are_wired_offline(self) -> None:
        with mock.patch(
            "pathfinder.simulator.full_flow_service_bootstrap."
            "freeze_full_flow_local_service_bootstrap",
            return_value={"status": "VERIFIED_LOCAL_SERVICE_BOOTSTRAP"},
        ) as freeze:
            status, payload = self._invoke([
                "freeze-simulator-full-flow-service-bootstrap",
                "--logical-plan-dir",
                "logical-routes",
                "--scenario",
                "scenario.json",
                "--container-plan-dir",
                "container-plan",
                "--bootstrap-id",
                "local-services-v1",
                "--output-dir",
                "service-bootstrap",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED_LOCAL_SERVICE_BOOTSTRAP", payload["status"])
        freeze.assert_called_once_with(
            Path("logical-routes"),
            Path("scenario.json"),
            Path("container-plan"),
            bootstrap_id="local-services-v1",
            output_dir=Path("service-bootstrap"),
        )

        with mock.patch(
            "pathfinder.simulator.full_flow_service_bootstrap."
            "verify_full_flow_local_service_bootstrap",
            return_value={"status": "VERIFIED_LOCAL_SERVICE_BOOTSTRAP"},
        ) as verify:
            status, payload = self._invoke([
                "verify-simulator-full-flow-service-bootstrap",
                "--bootstrap-dir",
                "service-bootstrap",
                "--logical-plan-dir",
                "logical-routes",
                "--scenario",
                "scenario.json",
                "--container-plan-dir",
                "container-plan",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED_LOCAL_SERVICE_BOOTSTRAP", payload["status"])
        verify.assert_called_once_with(
            Path("service-bootstrap"),
            logical_plan_dir=Path("logical-routes"),
            scenario_path=Path("scenario.json"),
            container_plan_dir=Path("container-plan"),
        )

    def test_n4_derived_data_plane_build_and_verify_are_wired_offline(
        self,
    ) -> None:
        with mock.patch(
            "pathfinder.simulator.n4_derived_data_plane."
            "build_n4_derived_data_package_from_manifest",
            return_value={"status": "FROZEN_N4_DERIVED_DATA_PACKAGE"},
        ) as build:
            status, payload = self._invoke([
                "build-simulator-n4-derived-data-plane",
                "--binding-manifest",
                "n4-artifact-bindings.json",
                "--output-dir",
                "n4-derived-data",
            ])
        self.assertEqual(0, status)
        self.assertEqual(
            "FROZEN_N4_DERIVED_DATA_PACKAGE",
            payload["status"],
        )
        build.assert_called_once_with(
            Path("n4-artifact-bindings.json"),
            output_dir=Path("n4-derived-data"),
        )

        with mock.patch(
            "pathfinder.simulator.n4_derived_data_plane."
            "verify_n4_derived_data_package",
            return_value={"status": "VERIFIED_N4_DERIVED_DATA_PACKAGE"},
        ) as verify:
            status, payload = self._invoke([
                "verify-simulator-n4-derived-data-plane",
                "--output-dir",
                "n4-derived-data",
            ])
        self.assertEqual(0, status)
        self.assertEqual(
            "VERIFIED_N4_DERIVED_DATA_PACKAGE",
            payload["status"],
        )
        verify.assert_called_once_with(Path("n4-derived-data"))

    def test_cache_service_requires_runtime_secret(self) -> None:
        stdout = io.StringIO()
        with mock.patch.dict(
            "os.environ",
            {"PATHFINDER_FULL_FLOW_CACHE_TOKEN": ""},
        ), redirect_stdout(stdout):
            status = cli_main([
                    "serve-simulator-full-flow-cache",
                    "--node-id",
                    "N7",
                    "--cache-id",
                    "N7.derived-cache",
                    "--state-dir",
                    "cache-state",
                    "--capacity-bytes",
                    "1000",
                ])
        payload = json.loads(stdout.getvalue())
        self.assertEqual(2, status)
        self.assertIn("TOKEN is required", payload["message"])

    def test_cache_service_supports_dedicated_w4_runtime_secret(self) -> None:
        with (
            mock.patch.dict(
                "os.environ",
                {"PATHFINDER_N7_W4_CACHE_TOKEN": "dedicated-secret"},
                clear=True,
            ),
            mock.patch(
                "pathfinder.simulator.full_flow_cache.serve_full_flow_cache",
            ) as serve,
        ):
            status = cli_main([
                "serve-simulator-full-flow-cache",
                "--node-id",
                "N7",
                "--cache-id",
                "N7.w4-cache",
                "--state-dir",
                "cache-state",
                "--capacity-bytes",
                "1000",
                "--token-env-name",
                "PATHFINDER_N7_W4_CACHE_TOKEN",
            ])
        self.assertEqual(0, status)
        serve.assert_called_once_with(
            Path("cache-state"),
            node_id="N7",
            cache_id="N7.w4-cache",
            capacity_bytes=1000,
            token="dedicated-secret",
            host="0.0.0.0",
            port=9081,
            max_artifact_bytes=64 * 1024 * 1024,
        )

    def test_w4_cache_service_accepts_shared_runtime_secret_fallback(
        self,
    ) -> None:
        with (
            mock.patch.dict(
                "os.environ",
                {"PATHFINDER_FULL_FLOW_CACHE_TOKEN": "shared-secret"},
                clear=True,
            ),
            mock.patch(
                "pathfinder.simulator.full_flow_cache.serve_full_flow_cache",
            ) as serve,
        ):
            status = cli_main([
                "serve-simulator-full-flow-cache",
                "--node-id",
                "N8",
                "--cache-id",
                "N8.w4-cache",
                "--state-dir",
                "cache-state",
                "--capacity-bytes",
                "1000",
                "--token-env-name",
                "PATHFINDER_N8_W4_CACHE_TOKEN",
                "--fallback-token-env-name",
                "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
                "--max-artifact-bytes",
                str(2 * 1024 * 1024 * 1024),
            ])
        self.assertEqual(0, status)
        self.assertEqual("shared-secret", serve.call_args.kwargs["token"])
        self.assertEqual(
            2 * 1024 * 1024 * 1024,
            serve.call_args.kwargs["max_artifact_bytes"],
        )

    def test_n1_oracle_service_requires_both_runtime_secrets(self) -> None:
        stdout = io.StringIO()
        with mock.patch.dict(
            "os.environ",
            {
                "PATHFINDER_N1_ORACLE_TOKEN": "token-present",
                "PATHFINDER_N1_ORACLE_EVIDENCE_SECRET": "",
            },
        ), redirect_stdout(stdout):
            status = cli_main([
                "serve-simulator-n1-hidden-oracle",
                "--package-dir",
                "n1-oracle",
                "--state-db",
                "oracle.sqlite3",
            ])
        payload = json.loads(stdout.getvalue())
        self.assertEqual(2, status)
        self.assertIn("EVIDENCE_SECRET", payload["message"])

    def test_n5_materializer_service_requires_runtime_secret(self) -> None:
        stdout = io.StringIO()
        with mock.patch.dict(
            "os.environ",
            {"PATHFINDER_N5_MATERIALIZATION_TOKEN": ""},
        ), redirect_stdout(stdout):
            status = cli_main([
                "serve-simulator-n5-materializer",
                "--state-dir",
                "n5-state",
            ])
        payload = json.loads(stdout.getvalue())
        self.assertEqual(2, status)
        self.assertIn("N5_MATERIALIZATION_TOKEN", payload["message"])

    def test_n5_digest_offline_commands_and_runtime_adapter_are_wired(
        self,
    ) -> None:
        sampled = [object()]
        with (
            mock.patch(
                "pathfinder.video_prep.sample_video",
                return_value=(sampled, 12.5),
            ) as sample,
            mock.patch(
                "pathfinder.simulator.n5_digest_materialization."
                "freeze_n5_multimodal_digest_plan",
                return_value={"status": "VERIFIED"},
            ) as freeze,
        ):
            status, payload = self._invoke([
                "freeze-simulator-n5-digest-plan",
                "--source-video",
                "video.mp4",
                "--object-id",
                "object-1",
                "--model-id",
                "qwen3.8-27b",
                "--plan-id",
                "digest-plan-v1",
                "--frame-count",
                "4",
                "--seed",
                "17",
                "--output-dir",
                "digest-plan",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        sample.assert_called_once_with(
            Path("video.mp4"),
            frame_count=4,
            jpeg_max_dimension=768,
        )
        freeze.assert_called_once_with(
            Path("video.mp4"),
            sampled,
            source_duration_seconds=12.5,
            object_id="object-1",
            model_id="qwen3.8-27b",
            output_dir=Path("digest-plan"),
            plan_id="digest-plan-v1",
            jpeg_max_dimension=768,
            seed=17,
            maximum_digest_bytes=256 * 1024,
        )

        with mock.patch(
            "pathfinder.simulator.n5_digest_materialization."
            "verify_n5_multimodal_digest_plan",
            return_value={"status": "VERIFIED", "model_id": "qwen3.8-27b"},
        ) as verify_plan:
            status, payload = self._invoke([
                "verify-simulator-n5-digest-plan",
                "--plan-dir",
                "digest-plan",
                "--source-video",
                "video.mp4",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        verify_plan.assert_called_once_with(
            Path("digest-plan"),
            Path("video.mp4"),
        )

        environment = {
            "PATHFINDER_N5_DIGEST_LLM_BASE_URL": "https://vision.test/v1",
            "PATHFINDER_N5_DIGEST_LLM_MODEL": "qwen3.8-27b",
            "PATHFINDER_N5_DIGEST_LLM_API_KEY": "runtime-only-key",
        }
        with (
            mock.patch.dict("os.environ", environment, clear=False),
            mock.patch(
                "pathfinder.simulator.n5_digest_materialization."
                "verify_n5_multimodal_digest_plan",
                return_value={
                    "status": "VERIFIED",
                    "model_id": "qwen3.8-27b",
                },
            ),
            mock.patch(
                "pathfinder.simulator.n5_digest_materialization."
                "OpenAICompatibleVisionDigestAdapter",
            ) as adapter_type,
            mock.patch(
                "pathfinder.simulator.n5_digest_materialization."
                "materialize_n5_multimodal_digest",
                return_value={"status": "VERIFIED", "credentials_recorded": False},
            ) as materialize,
        ):
            status, payload = self._invoke([
                "run-simulator-n5-digest-materialization",
                "--plan-dir",
                "digest-plan",
                "--source-video",
                "video.mp4",
                "--output-dir",
                "digest-output",
            ])
        self.assertEqual(0, status)
        self.assertFalse(payload["credentials_recorded"])
        adapter_type.assert_called_once_with(
            base_url="https://vision.test/v1",
            api_key="runtime-only-key",
            model_id="qwen3.8-27b",
            allowed_http_simulator_hosts=[],
            timeout_seconds=180.0,
            max_attempts=3,
        )
        materialize.assert_called_once_with(
            Path("digest-plan"),
            Path("video.mp4"),
            output_dir=Path("digest-output"),
            vision_adapter=adapter_type.return_value,
        )

        with mock.patch(
            "pathfinder.simulator.n5_digest_materialization."
            "verify_n5_multimodal_digest_materialization",
            return_value={"status": "VERIFIED"},
        ) as verify_output:
            status, payload = self._invoke([
                "verify-simulator-n5-digest-materialization",
                "--output-dir",
                "digest-output",
                "--plan-dir",
                "digest-plan",
                "--source-video",
                "video.mp4",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        verify_output.assert_called_once_with(
            Path("digest-output"),
            Path("digest-plan"),
            Path("video.mp4"),
        )

    def test_policy_and_oed_route_bridge_commands_are_wired(self) -> None:
        common = [
            "--logical-plan-dir",
            "logical",
            "--scenario",
            "scenario.json",
            "--container-plan-dir",
            "container",
        ]
        with mock.patch(
            "pathfinder.simulator.policy_oed_bridge.freeze_policy_assignment",
            return_value={"status": "VERIFIED"},
        ) as freeze_policy:
            status, payload = self._invoke([
                "freeze-simulator-policy-routes",
                *common,
                "--policy-id",
                "policy-v1",
                "--awm-policy-sha256",
                "a" * 64,
                "--assignment",
                "W1=D0",
                "--assignment",
                "W2=D1,D2",
                "--assignment",
                "W3=D3",
                "--assignment",
                "W4=D4",
                "--output-dir",
                "policy-routes",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        freeze_policy.assert_called_once_with(
            logical_route_plan_dir=Path("logical"),
            scenario_path=Path("scenario.json"),
            container_plan_dir=Path("container"),
            policy_id="policy-v1",
            awm_policy_sha256="a" * 64,
            assignments={
                "W1": ["D0"],
                "W2": ["D1", "D2"],
                "W3": ["D3"],
                "W4": ["D4"],
            },
            output_dir=Path("policy-routes"),
        )

        with mock.patch(
            "pathfinder.simulator.policy_oed_bridge."
            "freeze_oed_prospective_selection",
            return_value={"status": "VERIFIED"},
        ) as freeze_oed:
            status, payload = self._invoke([
                "freeze-simulator-oed-routes",
                *common,
                "--oed-request-id",
                "oed-v1",
                "--oed-request-sha256",
                "b" * 64,
                "--trial-key",
                "scenario|workload|D0|r0000",
                "--output-dir",
                "oed-routes",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        freeze_oed.assert_called_once_with(
            logical_route_plan_dir=Path("logical"),
            scenario_path=Path("scenario.json"),
            container_plan_dir=Path("container"),
            oed_request_id="oed-v1",
            oed_request_sha256="b" * 64,
            requested_trial_keys=["scenario|workload|D0|r0000"],
            output_dir=Path("oed-routes"),
        )

    def test_neutral_observation_bridge_loads_evidence_without_costs(
        self,
    ) -> None:
        evidence = {"schema_version": "test-evidence"}
        with TemporaryDirectory() as directory:
            evidence_path = Path(directory) / "evidence.json"
            evidence_path.write_text(json.dumps(evidence), encoding="utf-8")
            with mock.patch(
                "pathfinder.simulator.policy_oed_bridge."
                "freeze_full_flow_observations",
                return_value={
                    "status": "VERIFIED",
                    "monetary_cost_included": False,
                },
            ) as freeze:
                status, payload = self._invoke([
                    "freeze-simulator-full-flow-observations",
                    "--logical-plan-dir",
                    "logical",
                    "--scenario",
                    "scenario.json",
                    "--container-plan-dir",
                    "container",
                    "--observation-set-id",
                    "observations-v1",
                    "--evidence-json",
                    str(evidence_path),
                    "--output-dir",
                    "observations",
                ])
        self.assertEqual(0, status)
        self.assertFalse(payload["monetary_cost_included"])
        freeze.assert_called_once_with(
            logical_route_plan_dir=Path("logical"),
            scenario_path=Path("scenario.json"),
            container_plan_dir=Path("container"),
            observation_set_id="observations-v1",
            evidence_records=[evidence],
            output_dir=Path("observations"),
            external_real_cost_manifest=None,
            n1_oracle_package_dir=None,
            n1_evidence_secret=None,
            semantic_execution_admission_dir=None,
            semantic_matrix_run_dir=None,
        )

    def test_neutral_observation_cli_rejects_duplicate_json_keys(self) -> None:
        with TemporaryDirectory() as directory:
            evidence_path = Path(directory) / "evidence.json"
            evidence_path.write_text(
                '{"schema_version":"one","schema_version":"two"}',
                encoding="utf-8",
            )
            status, payload = self._invoke([
                "freeze-simulator-full-flow-observations",
                "--logical-plan-dir",
                "logical",
                "--scenario",
                "scenario.json",
                "--container-plan-dir",
                "container",
                "--observation-set-id",
                "observations-v1",
                "--evidence-json",
                str(evidence_path),
                "--output-dir",
                "observations",
            ])
        self.assertEqual(2, status)
        self.assertEqual("error", payload["status"])
        self.assertIn("repeats JSON key", payload["message"])

    def test_neutral_observation_bridge_accepts_verified_matrix_run(
        self,
    ) -> None:
        with mock.patch(
            "pathfinder.simulator.policy_oed_bridge."
            "freeze_full_flow_observations",
            return_value={"status": "VERIFIED"},
        ) as freeze:
            status, payload = self._invoke([
                "freeze-simulator-full-flow-observations",
                "--logical-plan-dir",
                "logical",
                "--scenario",
                "scenario.json",
                "--container-plan-dir",
                "container",
                "--observation-set-id",
                "observations-v1",
                "--semantic-matrix-run-dir",
                "semantic-run",
                "--semantic-execution-admission-dir",
                "admission",
                "--output-dir",
                "observations",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        freeze.assert_called_once_with(
            logical_route_plan_dir=Path("logical"),
            scenario_path=Path("scenario.json"),
            container_plan_dir=Path("container"),
            observation_set_id="observations-v1",
            evidence_records=None,
            output_dir=Path("observations"),
            external_real_cost_manifest=None,
            n1_oracle_package_dir=None,
            n1_evidence_secret=None,
            semantic_execution_admission_dir=Path("admission"),
            semantic_matrix_run_dir=Path("semantic-run"),
        )

    def test_data_plane_build_wires_portable_inputs_only(self) -> None:
        payload = {"status": "FROZEN", "package_id": "portable-v1"}
        with mock.patch(
            "pathfinder.simulator.full_flow_data_plane."
            "build_full_flow_data_plane_package_from_semantic_specs",
            return_value=payload,
        ) as build:
            status, printed = self._invoke([
                "build-full-flow-data-plane",
                "--semantic-spec-artifact",
                "spec-a.json",
                "bundle-a.tar",
                "--semantic-spec-artifact",
                "spec-b.json",
                "bundle-b.tar",
                "--package-id",
                "portable-v1",
                "--output-dir",
                "portable-output",
            ])

        self.assertEqual(status, 0)
        self.assertEqual(printed, payload)
        build.assert_called_once_with(
            [
                (Path("spec-a.json"), Path("bundle-a.tar")),
                (Path("spec-b.json"), Path("bundle-b.tar")),
            ],
            output_dir=Path("portable-output"),
            package_id="portable-v1",
        )

    def test_compose_build_wires_only_deployment_route_metadata(self) -> None:
        payload = {"status": "GENERATED_NOT_LAUNCHED"}
        with mock.patch(
            "pathfinder.simulator.full_flow_compose."
            "build_full_flow_compose_binding",
            return_value=payload,
        ) as build:
            status, printed = self._invoke([
                "build-full-flow-compose-binding",
                "--base-compose-package",
                "compose-base",
                "--data-plane-package",
                "portable-data",
                "--route-id",
                "route-v1",
                "--data-agent-plan-id",
                "plan-v1",
                "--data-agent-plan-epoch",
                "7",
                "--output-dir",
                "compose-bound",
            ])

        self.assertEqual(status, 0)
        self.assertEqual(printed, payload)
        build.assert_called_once_with(
            Path("compose-base"),
            Path("portable-data"),
            output_dir=Path("compose-bound"),
            route_id="route-v1",
            data_agent_plan_id="plan-v1",
            data_agent_plan_epoch=7,
        )

    def test_deployment_binding_and_logical_plan_are_separate(self) -> None:
        binding_payload = {
            "schema_version": "pathfinder.full-flow-deployment-binding/v1alpha1"
        }
        with mock.patch(
            "pathfinder.integrations.flowmesh.full_flow_trial."
            "build_full_flow_deployment_binding",
            return_value=binding_payload,
        ) as build_binding:
            status, printed = self._invoke([
                "build-full-flow-deployment-binding",
                "--deployment-binding-id",
                "upcloud-binding-v1",
                "--coordinator-api-url",
                "https://n7.private.example",
                "--worker-alias",
                "worker-v1",
                "--api-task-timeout-seconds",
                "900",
                "--output",
                "deployment.json",
            ])

        self.assertEqual(status, 0)
        self.assertEqual(printed, binding_payload)
        build_binding.assert_called_once_with(
            deployment_binding_id="upcloud-binding-v1",
            coordinator_api_url="https://n7.private.example",
            worker_alias="worker-v1",
            api_task_timeout_seconds=900,
            output_path=Path("deployment.json"),
        )

        plan_payload = {"status": "FROZEN_LOGICAL_FULL_FLOW_TRIAL"}
        with mock.patch(
            "pathfinder.integrations.flowmesh.full_flow_trial."
            "plan_flowmesh_full_flow_trial",
            return_value=plan_payload,
        ) as plan:
            status, printed = self._invoke([
                "plan-flowmesh-full-flow-trial",
                "--semantic-spec",
                "semantic.json",
                "--data-plane-package",
                "portable-data",
                "--deployment-binding",
                "deployment.json",
                "--owner",
                "pathfinder-test",
                "--output-dir",
                "logical-plan",
            ])

        self.assertEqual(status, 0)
        self.assertEqual(printed, plan_payload)
        plan.assert_called_once_with(
            semantic_spec=Path("semantic.json"),
            data_plane_package=Path("portable-data"),
            deployment_binding=Path("deployment.json"),
            output_dir=Path("logical-plan"),
            owner="pathfinder-test",
        )

    def test_offline_verifiers_dispatch_without_services(self) -> None:
        cases = [
            (
                "verify-full-flow-data-plane",
                "pathfinder.simulator.full_flow_data_plane."
                "verify_full_flow_data_plane_package",
                ["--output-dir", "portable-data"],
                (Path("portable-data"),),
                {},
            ),
            (
                "verify-full-flow-compose-binding",
                "pathfinder.simulator.full_flow_compose."
                "verify_full_flow_compose_binding",
                [
                    "--base-compose-package",
                    "compose-base",
                    "--data-plane-package",
                    "portable-data",
                    "--output-dir",
                    "compose-bound",
                ],
                (Path("compose-bound"),),
                {
                    "base_compose_package": Path("compose-base"),
                    "data_plane_package": Path("portable-data"),
                },
            ),
            (
                "verify-flowmesh-full-flow-trial-plan",
                "pathfinder.integrations.flowmesh.full_flow_trial."
                "verify_flowmesh_full_flow_trial_plan",
                ["--plan-dir", "logical-plan"],
                (Path("logical-plan"),),
                {},
            ),
            (
                "verify-flowmesh-full-flow-trial-run",
                "pathfinder.integrations.flowmesh.full_flow_trial."
                "verify_flowmesh_full_flow_trial_run",
                [
                    "--run-dir",
                    "trial-run",
                    "--plan-dir",
                    "logical-plan",
                ],
                (Path("trial-run"),),
                {"plan_dir": Path("logical-plan")},
            ),
        ]
        for command, target, arguments, positional, keywords in cases:
            with self.subTest(command=command), mock.patch(
                target,
                return_value={"status": "VERIFIED", "command": command},
            ) as verify:
                status, payload = self._invoke([command, *arguments])
                self.assertEqual(status, 0)
                self.assertEqual(payload["status"], "VERIFIED")
                verify.assert_called_once_with(*positional, **keywords)


if __name__ == "__main__":
    unittest.main()
