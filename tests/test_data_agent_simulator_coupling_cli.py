from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from pathfinder.cli import main as cli_main
from pathfinder.data_agent_client import DataAgentClientSettings


class DataAgentSimulatorCouplingCliTest(unittest.TestCase):
    def _invoke(self, arguments: list[str]) -> tuple[int, dict]:
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            status = cli_main([*arguments, "--compact"])
        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 1, stdout.getvalue())
        return status, json.loads(lines[0])

    def test_run_semantic_trial_uses_verified_endpoint_and_dispatches(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            compose_dir = root / "compose"
            compose_dir.mkdir()
            (compose_dir / "container_endpoints.json").write_text(
                json.dumps(
                    {
                        "endpoints": {
                            "N6": {
                                "host_semantic_url": (
                                    "https://attacker.invalid/"
                                    "v1/semantic/chat-completions"
                                ),
                                "host_health_url": (
                                    "https://attacker.invalid/healthz"
                                ),
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )

            registry = mock.Mock(name="endpoint_registry")
            route = SimpleNamespace(endpoint_id="origin_remote")
            registry.route.return_value = route
            endpoint = mock.Mock(name="endpoint")
            endpoint.client_settings.return_value = DataAgentClientSettings(
                base_url="https://data-agent.example",
                timeout_seconds=31.0,
                max_artifact_bytes=1024,
            )
            registry.endpoint.return_value = endpoint
            spec = SimpleNamespace(
                document={
                    "data_agent_route_design_id": "D_origin_remote",
                    "representation_id": "sampled_frame_bundle",
                    "semantic_executor_node_id": "N6",
                }
            )
            client = mock.Mock(name="data_agent_client")
            adapter = mock.Mock(name="semantic_adapter")
            payload = {"status": "COMPLETE", "semantic_run_id": "semantic-1"}

            with (
                mock.patch(
                    "pathfinder.distributed.registry.load_endpoint_registry",
                    return_value=registry,
                ) as load_registry,
                mock.patch(
                    "pathfinder.simulator.data_agent_semantic_vertical."
                    "load_data_agent_frame_bundle_semantic_spec",
                    return_value=spec,
                ) as load_spec,
                mock.patch(
                    "pathfinder.simulator.local_container."
                    "verify_local_container_compose",
                    return_value={
                        "semantic_quality_enabled": True,
                        "semantic_executor_node_id": "N6",
                        "verified_semantic_endpoint": {
                            "host_semantic_url": (
                                "http://127.0.0.1:19086/"
                                "v1/semantic/chat-completions"
                            ),
                            "host_health_url": (
                                "http://127.0.0.1:19086/healthz"
                            ),
                        },
                    },
                ) as verify_compose,
                mock.patch(
                    "pathfinder.data_agent_client.HttpDataAgentClient",
                    return_value=client,
                ) as client_constructor,
                mock.patch(
                    "pathfinder.simulator.data_agent_semantic_vertical."
                    "HttpContainerSemanticVisionAdapter",
                    return_value=adapter,
                ) as adapter_constructor,
                mock.patch(
                    "pathfinder.simulator.data_agent_semantic_vertical."
                    "execute_data_agent_frame_bundle_semantic_trial",
                    return_value=payload,
                ) as execute,
            ):
                status, printed = self._invoke(
                    [
                        "run-data-agent-frame-bundle-semantic-trial",
                        "--matrix-plan-dir",
                        str(root / "matrix-plan"),
                        "--semantic-spec",
                        str(root / "semantic-spec.json"),
                        "--endpoint-registry",
                        str(root / "endpoint-registry.json"),
                        "--compose-package-dir",
                        str(compose_dir),
                        "--output-dir",
                        str(root / "semantic-output"),
                        "--request-timeout",
                        "12.5",
                        "--telemetry-quiescence-timeout",
                        "7.25",
                        "--max-artifact-bytes",
                        "123456",
                        "--event-index",
                        "3",
                    ]
                )

            self.assertEqual(status, 0)
            self.assertEqual(printed, payload)
            load_registry.assert_called_once_with(root / "endpoint-registry.json")
            load_spec.assert_called_once_with(root / "semantic-spec.json")
            registry.route.assert_called_once_with(
                design_id="D_origin_remote",
                representation_id="sampled_frame_bundle",
            )
            registry.endpoint.assert_called_once_with("origin_remote")
            verify_compose.assert_called_once_with(compose_dir)

            client_constructor.assert_called_once()
            client_settings = client_constructor.call_args.args[0]
            self.assertEqual(client_settings.base_url, "https://data-agent.example")
            self.assertEqual(client_settings.timeout_seconds, 31.0)
            self.assertEqual(client_settings.max_artifact_bytes, 123456)
            adapter_constructor.assert_called_once_with(
                semantic_url=(
                    "http://127.0.0.1:19086/v1/semantic/chat-completions"
                ),
                health_url="http://127.0.0.1:19086/healthz",
                expected_execution_node_id="N6",
                timeout_seconds=12.5,
            )

            execute.assert_called_once()
            dispatched = execute.call_args.kwargs
            self.assertEqual(dispatched["matrix_plan_dir"], root / "matrix-plan")
            self.assertEqual(
                dispatched["semantic_spec"], root / "semantic-spec.json"
            )
            self.assertIs(dispatched["endpoint_registry"], registry)
            self.assertEqual(
                dispatched["clients_by_endpoint_id"],
                {"origin_remote": client},
            )
            self.assertIs(dispatched["adapter"], adapter)
            self.assertEqual(
                dispatched["output_dir"], root / "semantic-output"
            )
            self.assertEqual(dispatched["event_index"], 3)
            self.assertEqual(dispatched["limits"].max_artifact_bytes, 123456)
            self.assertEqual(dispatched["quiescence_timeout_seconds"], 7.25)

    def test_verify_semantic_trial_parses_and_dispatches(self) -> None:
        root = Path("contract-fixtures")
        registry = mock.Mock(name="endpoint_registry")
        payload = {"status": "VERIFIED", "checked_files": 4}

        with (
            mock.patch(
                "pathfinder.distributed.registry.load_endpoint_registry",
                return_value=registry,
            ) as load_registry,
            mock.patch(
                "pathfinder.simulator.data_agent_semantic_vertical."
                "verify_data_agent_frame_bundle_semantic_trial",
                return_value=payload,
            ) as verify,
        ):
            status, printed = self._invoke(
                [
                    "verify-data-agent-frame-bundle-semantic-trial",
                    "--output-dir",
                    str(root / "semantic-output"),
                    "--matrix-plan-dir",
                    str(root / "matrix-plan"),
                    "--semantic-spec",
                    str(root / "semantic-spec.json"),
                    "--endpoint-registry",
                    str(root / "endpoint-registry.json"),
                ]
            )

        self.assertEqual(status, 0)
        self.assertEqual(printed, payload)
        load_registry.assert_called_once_with(root / "endpoint-registry.json")
        verify.assert_called_once_with(
            output_dir=root / "semantic-output",
            matrix_plan_dir=root / "matrix-plan",
            endpoint_registry=registry,
            semantic_spec=root / "semantic-spec.json",
        )

    def test_build_evidence_parses_repeated_inputs_and_dispatches(self) -> None:
        root = Path("contract-fixtures")
        registry = mock.Mock(name="endpoint_registry")
        payload = {"status": "COMPLETE", "evidence_record_count": 2}

        with (
            mock.patch(
                "pathfinder.distributed.registry.load_endpoint_registry",
                return_value=registry,
            ) as load_registry,
            mock.patch(
                "pathfinder.integrations.flowmesh.pathfinder_evidence_bridge."
                "build_flowmesh_pathfinder_evidence",
                return_value=payload,
            ) as build,
        ):
            status, printed = self._invoke(
                [
                    "build-flowmesh-pathfinder-evidence",
                    "--binding-spec",
                    str(root / "binding-spec.json"),
                    "--matrix-plan-dir",
                    str(root / "matrix-plan"),
                    "--matrix-run-dir",
                    str(root / "matrix-run"),
                    "--endpoint-registry",
                    str(root / "endpoint-registry.json"),
                    "--data-agent-semantic-dir",
                    str(root / "semantic-run-a"),
                    "--data-agent-semantic-dir",
                    str(root / "semantic-run-b"),
                    "--data-agent-semantic-spec",
                    str(root / "semantic-spec-a.json"),
                    "--data-agent-semantic-spec",
                    str(root / "semantic-spec-b.json"),
                    "--output-dir",
                    str(root / "evidence"),
                ]
            )

        self.assertEqual(status, 0)
        self.assertEqual(printed, payload)
        load_registry.assert_called_once_with(root / "endpoint-registry.json")
        build.assert_called_once_with(
            binding_spec=root / "binding-spec.json",
            matrix_plan_dir=root / "matrix-plan",
            matrix_run_dir=root / "matrix-run",
            endpoint_registry=registry,
            data_agent_semantic_dirs=[
                root / "semantic-run-a",
                root / "semantic-run-b",
            ],
            data_agent_semantic_specs=[
                root / "semantic-spec-a.json",
                root / "semantic-spec-b.json",
            ],
            output_dir=root / "evidence",
        )

    def test_verify_evidence_parses_repeated_inputs_and_dispatches(self) -> None:
        root = Path("contract-fixtures")
        registry = mock.Mock(name="endpoint_registry")
        payload = {"status": "VERIFIED", "checked_files": 3}

        with (
            mock.patch(
                "pathfinder.distributed.registry.load_endpoint_registry",
                return_value=registry,
            ) as load_registry,
            mock.patch(
                "pathfinder.integrations.flowmesh.pathfinder_evidence_bridge."
                "verify_flowmesh_pathfinder_evidence",
                return_value=payload,
            ) as verify,
        ):
            status, printed = self._invoke(
                [
                    "verify-flowmesh-pathfinder-evidence",
                    "--evidence-dir",
                    str(root / "evidence"),
                    "--binding-spec",
                    str(root / "binding-spec.json"),
                    "--matrix-plan-dir",
                    str(root / "matrix-plan"),
                    "--matrix-run-dir",
                    str(root / "matrix-run"),
                    "--endpoint-registry",
                    str(root / "endpoint-registry.json"),
                    "--data-agent-semantic-dir",
                    str(root / "semantic-run-a"),
                    "--data-agent-semantic-dir",
                    str(root / "semantic-run-b"),
                    "--data-agent-semantic-spec",
                    str(root / "semantic-spec-a.json"),
                    "--data-agent-semantic-spec",
                    str(root / "semantic-spec-b.json"),
                ]
            )

        self.assertEqual(status, 0)
        self.assertEqual(printed, payload)
        load_registry.assert_called_once_with(root / "endpoint-registry.json")
        verify.assert_called_once_with(
            evidence_dir=root / "evidence",
            binding_spec=root / "binding-spec.json",
            matrix_plan_dir=root / "matrix-plan",
            matrix_run_dir=root / "matrix-run",
            endpoint_registry=registry,
            data_agent_semantic_dirs=[
                root / "semantic-run-a",
                root / "semantic-run-b",
            ],
            data_agent_semantic_specs=[
                root / "semantic-spec-a.json",
                root / "semantic-spec-b.json",
            ],
        )


if __name__ == "__main__":
    unittest.main()
