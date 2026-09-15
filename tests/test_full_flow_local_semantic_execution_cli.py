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
            "run-simulator-full-flow-local-semantic-matrix",
            "verify-simulator-full-flow-local-semantic-matrix",
        }.issubset(commands))

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
