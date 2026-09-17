from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import mock

from pathfinder.cli import (
    _local_semantic_flowmesh_executor,
    _parser,
    main as cli_main,
)
from pathfinder.cli_commands._common import (
    DATA_AGENT_CREDENTIALS,
    INDEX_CREDENTIALS,
    PERSISTENT_CACHE_CREDENTIALS,
    resolve_credentials,
    semantic_route_credential_contract,
    w4_credential_contract,
)
from pathfinder.config import ConfigError


class FullFlowLocalSemanticExecutionCliTest(unittest.TestCase):
    def _invoke(self, arguments: list[str]) -> tuple[int, dict]:
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            status = cli_main([*arguments, "--compact"])
        lines = stdout.getvalue().splitlines()
        self.assertEqual(1, len(lines), stdout.getvalue())
        return status, json.loads(lines[0])

    @staticmethod
    def _n4_sources() -> list[str]:
        return [
            "--n4-serve-gate-dir",
            "n4-gate",
            "--compose-overlay-dir",
            "compose-overlay",
            "--service-bootstrap-dir",
            "service-bootstrap",
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
            "artifact-binding-package",
            "--n4-package-dir",
            "n4-package",
        ]

    @classmethod
    def _matrix_sources(cls) -> list[str]:
        n4 = cls._n4_sources()
        # Shared N4 source flags are also the matrix's deployment sources.
        return [
            "--local-semantic-admission-dir",
            "local-admission",
            "--smoke-dir",
            "smokes",
            *n4,
            "--semantic-matrix-dir",
            "semantic-matrix",
            "--public-task-set",
            "public-tasks.json",
            "--artifact-bindings",
            "artifact-bindings.json",
        ]

    @staticmethod
    def _multi_host_smoke_sources(live_sources: Path) -> list[str]:
        return [
            "--local-semantic-admission-dir",
            "local-admission",
            "--n4-serve-gate-dir",
            "n4-live-gate",
            "--deployment-binding-dir",
            "multi-host-deployment",
            "--logical-plan-dir",
            "logical-routes",
            "--scenario",
            "scenario.json",
            "--container-plan-dir",
            "container-plan",
            "--artifact-binding-dir",
            "artifact-binding-package",
            "--n4-live-gate-sources",
            str(live_sources),
        ]

    @staticmethod
    def _write_live_gate_sources(root: Path, payload: dict | None = None) -> Path:
        document = payload or {
            "live_receipt_bindings": [{
                "kind": "frame_bundle",
                "receipt_dir": "receipts/frame",
                "n5_plan": {},
            }],
            "n4_publication_store_root": "n4-store",
            "rebound_artifact_binding_dir": "artifact-binding-package",
            "rebound_semantic_matrix_dir": "semantic-matrix",
            "rebound_admission_dir": "local-admission",
        }
        path = root / "live-gate-sources.json"
        path.write_text(json.dumps(document) + "\n", encoding="utf-8")
        return path

    def test_parser_exposes_live_smoke_and_gated_matrix_commands(self) -> None:
        commands = {
            action.dest: set(action.choices or ())
            for action in _parser()._actions
            if action.dest == "command"
        }["command"]
        self.assertTrue({
            "run-simulator-full-flow-local-semantic-smokes",
            "verify-simulator-full-flow-local-semantic-smokes",
            "run-simulator-full-flow-semantic-smokes",
            "verify-simulator-full-flow-semantic-smokes",
            "run-simulator-full-flow-local-semantic-matrix",
            "verify-simulator-full-flow-local-semantic-matrix",
        }.issubset(commands))

    def test_multi_host_smoke_commands_forward_only_runtime_sources(self) -> None:
        with TemporaryDirectory() as raw:
            descriptor = self._write_live_gate_sources(Path(raw))
            sources = self._multi_host_smoke_sources(descriptor)
            executor = object()
            context = mock.MagicMock()
            context.__enter__.return_value = executor
            context.__exit__.return_value = False
            with (
                mock.patch(
                    "pathfinder.cli._local_semantic_flowmesh_executor",
                    return_value=context,
                ) as executor_context,
                mock.patch(
                    "pathfinder.simulator.full_flow_local_semantic_smoke."
                    "run_full_flow_semantic_smokes",
                    return_value={"status": "VERIFIED"},
                ) as run,
            ):
                status, payload = self._invoke([
                    "run-simulator-full-flow-semantic-smokes",
                    *sources,
                    "--run-id",
                    "multi-host-smoke-v1",
                    "--output-dir",
                    "smokes",
                    "--flowmesh-base-url",
                    "https://root.test",
                ])
            self.assertEqual(0, status)
            self.assertEqual("VERIFIED", payload["status"])
            executor_context.assert_called_once_with(
                Path("local-admission"),
                run_id="multi-host-smoke-v1",
                flowmesh_base_url="https://root.test",
                task_timeout_seconds=900,
                poll_interval_seconds=2.0,
            )
            self.assertIs(executor, run.call_args.kwargs["executor"])
            self.assertEqual(
                Path("multi-host-deployment"), run.call_args.args[2]
            )
            self.assertNotIn("compose_overlay_dir", run.call_args.kwargs)

            with mock.patch(
                "pathfinder.simulator.full_flow_local_semantic_smoke."
                "verify_full_flow_semantic_smokes",
                return_value={"status": "VERIFIED"},
            ) as verify:
                status, _ = self._invoke([
                    "verify-simulator-full-flow-semantic-smokes",
                    "--smoke-dir",
                    "smokes",
                    *sources,
                ])
            self.assertEqual(0, status)
            self.assertEqual(
                Path("multi-host-deployment"),
                verify.call_args.kwargs["deployment_binding_dir"],
            )

    def test_live_smoke_refuses_a_bare_boolean_instead_of_n4_gate_sources(
        self,
    ) -> None:
        stderr = io.StringIO()
        with redirect_stderr(stderr), self.assertRaises(SystemExit):
            _parser().parse_args([
                "run-simulator-full-flow-local-semantic-smokes",
                "--local-semantic-admission-dir",
                "local-admission",
                "--run-id",
                "smoke-v1",
                "--output-dir",
                "smokes",
            ])
        self.assertIn("--n4-serve-gate-dir", stderr.getvalue())

    def test_smoke_run_and_verify_forward_every_n4_gate_source(self) -> None:
        executor = object()
        context = mock.MagicMock()
        context.__enter__.return_value = executor
        context.__exit__.return_value = False
        with (
            mock.patch(
                "pathfinder.cli._local_semantic_flowmesh_executor",
                return_value=context,
            ) as executor_context,
            mock.patch(
                "pathfinder.simulator.full_flow_local_semantic_smoke."
                "run_full_flow_local_semantic_smokes",
                return_value={"status": "VERIFIED"},
            ) as run,
        ):
            status, payload = self._invoke([
                "run-simulator-full-flow-local-semantic-smokes",
                "--local-semantic-admission-dir",
                "local-admission",
                *self._n4_sources(),
                "--run-id",
                "smoke-v1",
                "--output-dir",
                "smokes",
                "--flowmesh-base-url",
                "https://root.test",
                "--task-timeout",
                "901",
                "--poll-interval",
                "3",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        executor_context.assert_called_once_with(
            Path("local-admission"),
            run_id="smoke-v1",
            flowmesh_base_url="https://root.test",
            task_timeout_seconds=901,
            poll_interval_seconds=3.0,
        )
        self.assertIs(executor, run.call_args.kwargs["executor"])
        self.assertEqual(Path("n4-gate"), run.call_args.args[1])
        self.assertEqual(Path("n4-package"), run.call_args.args[10])

        with mock.patch(
            "pathfinder.simulator.full_flow_local_semantic_smoke."
            "verify_full_flow_local_semantic_smokes",
            return_value={"status": "VERIFIED"},
        ) as verify:
            status, _ = self._invoke([
                "verify-simulator-full-flow-local-semantic-smokes",
                "--smoke-dir",
                "smokes",
                "--local-semantic-admission-dir",
                "local-admission",
                *self._n4_sources(),
            ])
        self.assertEqual(0, status)
        self.assertEqual(
            Path("n4-gate"),
            verify.call_args.kwargs["n4_serve_gate_dir"],
        )
        self.assertEqual(
            Path("compose-overlay"),
            verify.call_args.kwargs["compose_overlay_dir"],
        )

    def test_matrix_run_forwards_smoke_gate_and_failure_acknowledgement(
        self,
    ) -> None:
        executor = object()
        context = mock.MagicMock()
        context.__enter__.return_value = executor
        context.__exit__.return_value = False
        with (
            mock.patch(
                "pathfinder.cli._local_semantic_flowmesh_executor",
                return_value=context,
            ),
            mock.patch(
                "pathfinder.simulator.full_flow_local_semantic_matrix_gate."
                "run_smoke_gated_full_flow_local_semantic_matrix",
                return_value={"status": "VERIFIED"},
            ) as run,
        ):
            status, payload = self._invoke([
                "run-simulator-full-flow-local-semantic-matrix",
                *self._matrix_sources(),
                "--run-id",
                "matrix-v1",
                "--output-dir",
                "matrix-run",
                "--acknowledge-failed-entry-sha256",
                "f" * 64,
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        self.assertEqual(Path("smokes"), run.call_args.args[1])
        self.assertEqual(Path("n4-gate"), run.call_args.args[2])
        self.assertIs(executor, run.call_args.kwargs["executor"])
        self.assertEqual(
            "f" * 64,
            run.call_args.kwargs["acknowledge_failed_entry_sha256"],
        )

        with mock.patch(
            "pathfinder.simulator.full_flow_local_semantic_matrix_gate."
            "verify_smoke_gated_full_flow_local_semantic_matrix_run",
            return_value={"status": "VERIFIED"},
        ) as verify:
            status, _ = self._invoke([
                "verify-simulator-full-flow-local-semantic-matrix",
                *self._matrix_sources(),
                "--output-dir",
                "matrix-run",
            ])
        self.assertEqual(0, status)
        self.assertEqual(Path("n4-gate"), verify.call_args.args[2])
        self.assertEqual(Path("matrix-run"), verify.call_args.kwargs["output_dir"])

    def test_all_four_commands_resolve_and_forward_live_gate_sources(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            descriptor = self._write_live_gate_sources(root)
            flag = ["--n4-live-gate-sources", str(descriptor)]
            expected = {
                "live_receipt_bindings": [{
                    "kind": "frame_bundle",
                    "receipt_dir": (root / "receipts/frame").resolve(),
                    "n5_plan": {},
                }],
                "n4_publication_store_root": (root / "n4-store").resolve(),
                "rebound_artifact_binding_dir": (
                    root / "artifact-binding-package"
                ).resolve(),
                "rebound_semantic_matrix_dir": (
                    root / "semantic-matrix"
                ).resolve(),
                "rebound_admission_dir": (root / "local-admission").resolve(),
            }
            executor = object()
            context = mock.MagicMock()
            context.__enter__.return_value = executor
            context.__exit__.return_value = False

            with (
                mock.patch(
                    "pathfinder.cli._local_semantic_flowmesh_executor",
                    return_value=context,
                ),
                mock.patch(
                    "pathfinder.simulator.full_flow_local_semantic_smoke."
                    "run_full_flow_local_semantic_smokes",
                    return_value={"status": "VERIFIED"},
                ) as smoke_run,
            ):
                status, _ = self._invoke([
                    "run-simulator-full-flow-local-semantic-smokes",
                    "--local-semantic-admission-dir",
                    "local-admission",
                    *self._n4_sources(),
                    *flag,
                    "--run-id",
                    "live-smoke-v1",
                    "--output-dir",
                    "smokes",
                ])
            self.assertEqual(0, status)
            self.assertEqual(
                expected,
                smoke_run.call_args.kwargs["n4_live_gate_sources"],
            )

            with mock.patch(
                "pathfinder.simulator.full_flow_local_semantic_smoke."
                "verify_full_flow_local_semantic_smokes",
                return_value={"status": "VERIFIED"},
            ) as smoke_verify:
                status, _ = self._invoke([
                    "verify-simulator-full-flow-local-semantic-smokes",
                    "--smoke-dir",
                    "smokes",
                    "--local-semantic-admission-dir",
                    "local-admission",
                    *self._n4_sources(),
                    *flag,
                ])
            self.assertEqual(0, status)
            self.assertEqual(
                expected,
                smoke_verify.call_args.kwargs["n4_live_gate_sources"],
            )

            with (
                mock.patch(
                    "pathfinder.cli._local_semantic_flowmesh_executor",
                    return_value=context,
                ),
                mock.patch(
                    "pathfinder.simulator."
                    "full_flow_local_semantic_matrix_gate."
                    "run_smoke_gated_full_flow_local_semantic_matrix",
                    return_value={"status": "VERIFIED"},
                ) as matrix_run,
            ):
                status, _ = self._invoke([
                    "run-simulator-full-flow-local-semantic-matrix",
                    *self._matrix_sources(),
                    *flag,
                    "--run-id",
                    "live-matrix-v1",
                    "--output-dir",
                    "matrix-run",
                ])
            self.assertEqual(0, status)
            self.assertEqual(
                expected,
                matrix_run.call_args.kwargs["n4_live_gate_sources"],
            )

            with mock.patch(
                "pathfinder.simulator.full_flow_local_semantic_matrix_gate."
                "verify_smoke_gated_full_flow_local_semantic_matrix_run",
                return_value={"status": "VERIFIED"},
            ) as matrix_verify:
                status, _ = self._invoke([
                    "verify-simulator-full-flow-local-semantic-matrix",
                    *self._matrix_sources(),
                    *flag,
                    "--output-dir",
                    "matrix-run",
                ])
            self.assertEqual(0, status)
            self.assertEqual(
                expected,
                matrix_verify.call_args.kwargs["n4_live_gate_sources"],
            )

    def test_live_gate_source_json_rejects_missing_extra_and_non_array(self) -> None:
        valid = {
            "live_receipt_bindings": [],
            "n4_publication_store_root": "n4-store",
            "rebound_artifact_binding_dir": "bindings",
            "rebound_semantic_matrix_dir": "semantic",
            "rebound_admission_dir": "admission",
        }
        variants = {
            "missing": {
                key: value
                for key, value in valid.items()
                if key != "rebound_admission_dir"
            },
            "extra": valid | {"unexpected": "value"},
            "non-array": valid | {"live_receipt_bindings": {}},
            "empty-array": valid,
        }
        with TemporaryDirectory() as raw:
            root = Path(raw)
            for name, value in variants.items():
                with self.subTest(name=name):
                    descriptor = self._write_live_gate_sources(
                        root,
                        value,
                    )
                    with mock.patch(
                        "pathfinder.simulator.full_flow_local_semantic_smoke."
                        "verify_full_flow_local_semantic_smokes"
                    ) as verify:
                        status, payload = self._invoke([
                            "verify-simulator-full-flow-local-semantic-smokes",
                            "--smoke-dir",
                            "smokes",
                            "--local-semantic-admission-dir",
                            "local-admission",
                            *self._n4_sources(),
                            "--n4-live-gate-sources",
                            str(descriptor),
                        ])
                    self.assertEqual(2, status)
                    self.assertEqual("error", payload["status"])
                    verify.assert_not_called()

    def test_executor_requires_runtime_secret_and_closes_client(self) -> None:
        inputs = SimpleNamespace(
            admission={
                "worker_pin": {
                    "kind": "worker_alias",
                    "value": "semantic-worker",
                }
            },
            bound_trials=(object(),),
            bound_stages=(object(),),
        )
        with (
            mock.patch.dict(
                "os.environ",
                {"PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET": ""},
                clear=False,
            ),
            mock.patch(
                "pathfinder.simulator.full_flow_local_semantic_admission."
                "load_full_flow_local_semantic_execution_inputs",
                return_value=inputs,
            ),
            self.assertRaises(ConfigError),
        ):
            with _local_semantic_flowmesh_executor(
                Path("local-admission"),
                run_id="run-v1",
                flowmesh_base_url=None,
                task_timeout_seconds=900,
                poll_interval_seconds=2.0,
            ):
                pass

        fake_client = mock.MagicMock()
        fake_executor = object()
        with (
            mock.patch.dict(
                "os.environ",
                {"PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET": "runtime-only"},
                clear=False,
            ),
            mock.patch(
                "pathfinder.simulator.full_flow_local_semantic_admission."
                "load_full_flow_local_semantic_execution_inputs",
                return_value=inputs,
            ),
            mock.patch(
                "pathfinder.integrations.flowmesh.SdkFlowMeshClient",
                return_value=fake_client,
            ),
            mock.patch(
                "pathfinder.integrations.flowmesh."
                "FlowMeshSemanticTrialExecutor",
                return_value=fake_executor,
            ),
            mock.patch(
                "pathfinder.integrations.flowmesh."
                "full_flow_hmac_header_provider",
                return_value=lambda _request: {},
            ) as signer,
        ):
            with _local_semantic_flowmesh_executor(
                Path("local-admission"),
                run_id="run-v1",
                flowmesh_base_url="https://root.test",
                task_timeout_seconds=900,
                poll_interval_seconds=2.0,
            ) as actual:
                self.assertIs(fake_executor, actual)
        signer.assert_called_once_with("runtime-only")
        fake_client.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()


class FullFlowCredentialContractTest(unittest.TestCase):
    """Each service family carries its own credential contract.

    One shared precedence table cannot serve them: the Data Agents
    authenticate per node, while the regular index and cache services
    authenticate with a single shared credential. Applying the Data Agent rule
    to the indexes selected a token the local index servers reject with 401.
    """

    def test_data_agent_credentials_are_node_specific_first(self) -> None:
        environment = {
            "PATHFINDER_DATA_AGENT_TOKEN": "shared-token",
            "PATHFINDER_N3_DATA_AGENT_TOKEN": "n3-token",
            "PATHFINDER_N4_DATA_AGENT_TOKEN": "n4-token",
        }
        resolved, missing = resolve_credentials(
            environment,
            {
                "N3 Data Agent": DATA_AGENT_CREDENTIALS["N3"],
                "N4 Data Agent": DATA_AGENT_CREDENTIALS["N4"],
            },
        )
        self.assertEqual([], missing)
        self.assertEqual("n3-token", resolved["N3 Data Agent"])
        self.assertEqual("n4-token", resolved["N4 Data Agent"])
        self.assertNotEqual(
            resolved["N3 Data Agent"],
            resolved["N4 Data Agent"],
        )

    def test_regular_index_clients_keep_the_n2_token(self) -> None:
        # The deployed N7/N8 local index servers authenticate with
        # PATHFINDER_N2_INDEX_TOKEN, so a node-specific variable must not win.
        environment = {
            "PATHFINDER_N2_INDEX_TOKEN": "n2-index",
            "PATHFINDER_N7_INDEX_TOKEN": "n7-index",
            "PATHFINDER_N8_INDEX_TOKEN": "n8-index",
        }
        contract = semantic_route_credential_contract()
        resolved, missing = resolve_credentials(environment, contract)
        self.assertEqual("n2-index", resolved["N2 index"])
        self.assertEqual("n2-index", resolved["N7 index"])
        self.assertEqual("n2-index", resolved["N8 index"])
        self.assertNotIn("N7 index", missing)

    def test_regular_cache_clients_keep_the_shared_cache_token(self) -> None:
        environment = {
            "PATHFINDER_FULL_FLOW_CACHE_TOKEN": "shared-cache",
            "PATHFINDER_N7_FULL_FLOW_CACHE_TOKEN": "n7-cache",
            "PATHFINDER_N8_FULL_FLOW_CACHE_TOKEN": "n8-cache",
        }
        resolved, _ = resolve_credentials(
            environment,
            {
                "N7 cache": PERSISTENT_CACHE_CREDENTIALS["N7"],
                "N8 cache": PERSISTENT_CACHE_CREDENTIALS["N8"],
            },
        )
        self.assertEqual("shared-cache", resolved["N7 cache"])
        self.assertEqual("shared-cache", resolved["N8 cache"])

    def test_node_specific_values_still_serve_as_a_fallback(self) -> None:
        resolved, missing = resolve_credentials(
            {
                "PATHFINDER_N7_INDEX_TOKEN": "n7-index",
                "PATHFINDER_N7_FULL_FLOW_CACHE_TOKEN": "n7-cache",
            },
            {
                "N7 index": INDEX_CREDENTIALS["N7"],
                "N7 cache": PERSISTENT_CACHE_CREDENTIALS["N7"],
            },
        )
        self.assertEqual([], missing)
        self.assertEqual("n7-index", resolved["N7 index"])
        self.assertEqual("n7-cache", resolved["N7 cache"])

    def test_shared_only_deployment_still_resolves(self) -> None:
        environment = {
            "PATHFINDER_N2_INDEX_TOKEN": "n2-index",
            "PATHFINDER_DATA_AGENT_TOKEN": "shared-data",
            "PATHFINDER_FULL_FLOW_CACHE_TOKEN": "shared-cache",
            "PATHFINDER_CONTAINER_NODE_TOKEN": "node",
            "PATHFINDER_N1_ORACLE_TOKEN": "oracle",
            "PATHFINDER_N1_VERIFICATION_TOKEN": "verify",
            "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET": "ingress",
        }
        resolved, missing = resolve_credentials(
            environment,
            semantic_route_credential_contract(),
        )
        self.assertEqual([], missing)
        self.assertEqual("shared-data", resolved["N3 Data Agent"])
        self.assertEqual("shared-data", resolved["N4 Data Agent"])
        self.assertEqual("shared-cache", resolved["N7 cache"])

    def test_w4_dedicated_cache_contract_is_unchanged(self) -> None:
        environment = {
            "PATHFINDER_N7_W4_CACHE_TOKEN": "n7-w4-cache",
            "PATHFINDER_N8_W4_CACHE_TOKEN": "n8-w4-cache",
            "PATHFINDER_FULL_FLOW_CACHE_TOKEN": "shared-cache",
        }
        dedicated = w4_credential_contract(dedicated_cache=True)
        resolved, _ = resolve_credentials(environment, dedicated)
        self.assertEqual("n7-w4-cache", resolved["N7 cache"])
        self.assertEqual("n8-w4-cache", resolved["N8 cache"])
        self.assertEqual(
            "PATHFINDER_N7_W4_CACHE_TOKEN",
            dedicated["N7 cache"][0],
        )
        # the regular W4 path must not pick up the dedicated token
        regular = w4_credential_contract(dedicated_cache=False)
        self.assertEqual(
            PERSISTENT_CACHE_CREDENTIALS["N7"],
            regular["N7 cache"],
        )
        resolved, _ = resolve_credentials(environment, regular)
        self.assertEqual("shared-cache", resolved["N7 cache"])

    def test_contracts_agree_with_the_frozen_service_bootstrap(self) -> None:
        """Each client selects what the frozen service contract declares."""

        declared = {
            "N2 index": "PATHFINDER_N2_INDEX_TOKEN",
            "N7 index": "PATHFINDER_N2_INDEX_TOKEN",
            "N8 index": "PATHFINDER_N2_INDEX_TOKEN",
            "N7 cache": "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
            "N8 cache": "PATHFINDER_FULL_FLOW_CACHE_TOKEN",
            "N6 semantic": "PATHFINDER_CONTAINER_NODE_TOKEN",
            "N1 score": "PATHFINDER_N1_ORACLE_TOKEN",
            "N1 verifier": "PATHFINDER_N1_VERIFICATION_TOKEN",
            "route ingress": "PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET",
        }
        contract = semantic_route_credential_contract()
        for service, variable in declared.items():
            with self.subTest(service=service):
                self.assertEqual(variable, contract[service][0])
        # the Data Agents are the one family that resolves per node first
        self.assertEqual(
            "PATHFINDER_N3_DATA_AGENT_TOKEN",
            contract["N3 Data Agent"][0],
        )
        self.assertEqual(
            "PATHFINDER_N4_DATA_AGENT_TOKEN",
            contract["N4 Data Agent"][0],
        )
        for service, candidates in contract.items():
            with self.subTest(service=service):
                self.assertTrue(candidates)
                self.assertTrue(all(
                    isinstance(name, str)
                    and name.startswith("PATHFINDER_")
                    for name in candidates
                ))

    def test_missing_credentials_fail_closed(self) -> None:
        resolved, missing = resolve_credentials(
            {},
            semantic_route_credential_contract(),
        )
        self.assertTrue(all(value is None for value in resolved.values()))
        self.assertTrue(missing)
        self.assertIn(
            "PATHFINDER_N2_INDEX_TOKEN",
            " ".join(missing),
        )

    def test_missing_report_names_variables_never_values(self) -> None:
        _, missing = resolve_credentials(
            {"PATHFINDER_N3_DATA_AGENT_TOKEN": "n3-secret-value"},
            {
                "N3 Data Agent": DATA_AGENT_CREDENTIALS["N3"],
                "N4 Data Agent": DATA_AGENT_CREDENTIALS["N4"],
            },
        )
        rendered = " ".join(missing)
        self.assertNotIn("n3-secret-value", rendered)
        self.assertIn("PATHFINDER_N4_DATA_AGENT_TOKEN", rendered)

    def test_empty_value_is_not_selected(self) -> None:
        resolved, missing = resolve_credentials(
            {
                "PATHFINDER_N4_DATA_AGENT_TOKEN": "",
                "PATHFINDER_DATA_AGENT_TOKEN": "shared-token",
            },
            {"N4 Data Agent": DATA_AGENT_CREDENTIALS["N4"]},
        )
        self.assertEqual("shared-token", resolved["N4 Data Agent"])
        self.assertEqual([], missing)
