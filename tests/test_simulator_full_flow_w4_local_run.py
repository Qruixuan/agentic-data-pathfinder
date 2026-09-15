import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

import pathfinder.simulator as simulator_api
from pathfinder.cli import main as cli_main
from pathfinder.simulator.full_flow_w4_local_factory import (
    W4LocalRuntimeInputs,
    build_local_w4_live_components,
)
from pathfinder.simulator.full_flow_w4_local_run import (
    FullFlowW4LocalRunError,
    run_full_flow_w4_local_component_execution,
)


TOKEN = "runtime-only-test-token"
RUN_MODULE = "pathfinder.simulator.full_flow_w4_local_run."


def _runtime(root: Path) -> W4LocalRuntimeInputs:
    return W4LocalRuntimeInputs(
        index_base_urls={
            "N2": "http://127.0.0.1:19082",
            "N7": "http://127.0.0.1:19087",
            "N8": "http://127.0.0.1:19088",
        },
        index_bearer_tokens={node: TOKEN for node in ("N2", "N7", "N8")},
        index_package_dirs={
            node: root / f"index-{node}" for node in ("N2", "N7", "N8")
        },
        data_agent_base_urls={
            "N3": "http://127.0.0.1:19183",
            "N4": "http://127.0.0.1:19184",
        },
        data_agent_bearer_tokens={"N3": TOKEN, "N4": TOKEN},
        data_agent_locations={"N3": "origin-cold", "N4": "origin-warm"},
        cache_base_urls={
            "N7": "http://127.0.0.1:19287",
            "N8": "http://127.0.0.1:19288",
        },
        cache_bearer_tokens={"N7": TOKEN, "N8": TOKEN},
        cache_ids={"N7": "cache-n7", "N8": "cache-n8"},
        n6_base_url="http://127.0.0.1:19086",
        n6_bearer_token=TOKEN,
        semantic_model="qwen3.8-27b",
        raw_sampler_scratch_dir=root / "scratch",
    )


class FullFlowW4LocalRunTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_public_exports_include_factory_and_one_shot_runner(self):
        self.assertIs(
            build_local_w4_live_components,
            simulator_api.build_local_w4_live_components,
        )
        self.assertIs(
            run_full_flow_w4_local_component_execution,
            simulator_api.run_full_flow_w4_local_component_execution,
        )
        self.assertIs(W4LocalRuntimeInputs, simulator_api.W4LocalRuntimeInputs)

    def test_one_shot_runner_executes_sixteen_trials_and_verifies_receipt(self):
        runtime = _runtime(self.root)
        route = self.root / "routes"
        crosswalk = self.root / "crosswalk"
        output = self.root / "output"
        components = object()
        executor = object()
        coordinator = {
            "status": "COMPLETE",
            "run_id": "w4-local-run-v1",
            "trial_count": 16,
            "planned_operation_count": 126,
            "activated_operation_count": 98,
            "inactive_operation_count": 28,
        }
        verified = {
            "status": "VERIFIED",
            "evidence_class": "live-local-component-execution",
            "operation_count": 98,
            "llm_called": True,
            "flowmesh_workflow_submitted": False,
            "real_cloud_performance_measured": False,
            "eligible_for_scientific_claims": False,
        }
        with (
            mock.patch(
                RUN_MODULE + "build_local_w4_live_components",
                return_value=components,
            ) as build,
            mock.patch(
                RUN_MODULE + "LiveW4CandidateOperationExecutor",
                return_value=executor,
            ) as executor_type,
            mock.patch(
                RUN_MODULE + "run_full_flow_w4_candidate_coordinator",
                return_value=coordinator,
            ) as coordinate,
            mock.patch(
                RUN_MODULE + "freeze_full_flow_w4_component_execution_receipt",
                return_value={"status": "FROZEN"},
            ) as freeze,
            mock.patch(
                RUN_MODULE + "verify_full_flow_w4_component_execution_receipt",
                return_value=verified,
            ) as verify,
        ):
            result = run_full_flow_w4_local_component_execution(
                route_package_dir=route,
                crosswalk_dir=crosswalk,
                runtime=runtime,
                run_id="w4-local-run-v1",
                output_dir=output,
            )
        build.assert_called_once_with(runtime)
        executor_type.assert_called_once_with(
            route_package_dir=route,
            crosswalk_dir=crosswalk,
            canonical_index_package_dir=runtime.index_package_dirs["N2"],
            components=components,
            evidence_class="live-local-component-execution",
        )
        coordinate.assert_called_once_with(
            route,
            run_id="w4-local-run-v1",
            executor=executor,
            output_dir=output.resolve() / "coordinator-run",
        )
        self.assertIs(executor, freeze.call_args.kwargs["executor"])
        self.assertEqual(
            output.resolve() / "component-receipt",
            freeze.call_args.kwargs["output_dir"],
        )
        verify.assert_called_once()
        self.assertEqual(16, result["trial_count"])
        self.assertEqual(98, result["component_event_count"])
        self.assertFalse(result["flowmesh_workflow_submitted"])
        self.assertFalse(result["network_performance_measured"])
        self.assertNotIn(TOKEN, json.dumps(result))

    def test_one_shot_runner_refuses_overstated_receipt(self):
        runtime = _runtime(self.root)
        with (
            mock.patch(
                RUN_MODULE + "build_local_w4_live_components",
                return_value=object(),
            ),
            mock.patch(
                RUN_MODULE + "LiveW4CandidateOperationExecutor",
                return_value=object(),
            ),
            mock.patch(
                RUN_MODULE + "run_full_flow_w4_candidate_coordinator",
                return_value={
                    "status": "COMPLETE",
                    "run_id": "w4-local-run-v1",
                    "trial_count": 16,
                },
            ),
            mock.patch(
                RUN_MODULE + "freeze_full_flow_w4_component_execution_receipt",
                return_value={"status": "FROZEN"},
            ),
            mock.patch(
                RUN_MODULE + "verify_full_flow_w4_component_execution_receipt",
                return_value={
                    "status": "VERIFIED",
                    "evidence_class": "live-local-component-execution",
                    "flowmesh_workflow_submitted": True,
                    "real_cloud_performance_measured": False,
                    "eligible_for_scientific_claims": False,
                },
            ),
        ):
            with self.assertRaisesRegex(
                FullFlowW4LocalRunError,
                "overstates or misstates",
            ):
                run_full_flow_w4_local_component_execution(
                    route_package_dir=self.root / "routes",
                    crosswalk_dir=self.root / "crosswalk",
                    runtime=runtime,
                    run_id="w4-local-run-v1",
                    output_dir=self.root / "output",
                )


class FullFlowW4LocalRunCliTest(unittest.TestCase):
    def arguments(self) -> list[str]:
        return [
            "run-simulator-full-flow-w4-local-component-execution",
            "--route-package-dir", "routes",
            "--crosswalk-dir", "crosswalk",
            "--n2-index-package-dir", "index-n2",
            "--n2-index-base-url", "http://pathfinder-sim-n2:9080",
            "--n7-index-package-dir", "index-n7",
            "--n7-index-base-url", "http://pathfinder-sim-n7:9080",
            "--n8-index-package-dir", "index-n8",
            "--n8-index-base-url", "http://pathfinder-sim-n8:9080",
            "--n3-data-agent-base-url", "http://pathfinder-sim-n3:9080",
            "--n4-data-agent-base-url", "http://pathfinder-sim-n4:9080",
            "--n7-cache-base-url", "http://pathfinder-sim-n7:9180",
            "--n8-cache-base-url", "http://pathfinder-sim-n8:9180",
            "--n7-cache-id", "cache-n7",
            "--n8-cache-id", "cache-n8",
            "--n6-base-url", "http://pathfinder-sim-n6:9080",
            "--semantic-model", "qwen3.8-27b",
            "--raw-sampler-scratch-dir", "scratch",
            "--run-id", "w4-local-run-v1",
            "--output-dir", "output",
            "--timeout-seconds", "45",
            "--simulator-private-http-hosts",
            "pathfinder-sim-n2,pathfinder-sim-n6",
        ]

    @staticmethod
    def environment() -> dict[str, str]:
        names = (
            "PATHFINDER_N2_INDEX_TOKEN",
            "PATHFINDER_N7_INDEX_TOKEN",
            "PATHFINDER_N8_INDEX_TOKEN",
            "PATHFINDER_N3_DATA_AGENT_TOKEN",
            "PATHFINDER_N4_DATA_AGENT_TOKEN",
            "PATHFINDER_N7_FULL_FLOW_CACHE_TOKEN",
            "PATHFINDER_N8_FULL_FLOW_CACHE_TOKEN",
            "PATHFINDER_CONTAINER_NODE_TOKEN",
        )
        return {
            name: f"runtime-only-{index:02d}-secret"
            for index, name in enumerate(names)
        }

    def test_cli_prefers_service_canonical_secrets_and_runs(self):
        environment = self.environment()
        runtime = object()
        expected = {
            "status": "COMPLETE",
            "trial_count": 16,
            "flowmesh_workflow_submitted": False,
            "credentials_recorded": False,
        }
        stream = io.StringIO()
        with (
            mock.patch.dict("os.environ", environment, clear=True),
            mock.patch(
                "pathfinder.simulator.full_flow_w4_local_factory."
                "W4LocalRuntimeInputs",
                return_value=runtime,
            ) as runtime_type,
            mock.patch(
                "pathfinder.simulator.full_flow_w4_local_run."
                "run_full_flow_w4_local_component_execution",
                return_value=expected,
            ) as run,
            redirect_stdout(stream),
        ):
            status = cli_main(self.arguments())
        self.assertEqual(0, status)
        kwargs = runtime_type.call_args.kwargs
        self.assertEqual(
            {
                "N2": environment["PATHFINDER_N2_INDEX_TOKEN"],
                "N7": environment["PATHFINDER_N2_INDEX_TOKEN"],
                "N8": environment["PATHFINDER_N2_INDEX_TOKEN"],
            },
            kwargs["index_bearer_tokens"],
        )
        self.assertEqual(
            {
                "N3": environment["PATHFINDER_N3_DATA_AGENT_TOKEN"],
                "N4": environment["PATHFINDER_N4_DATA_AGENT_TOKEN"],
            },
            kwargs["data_agent_bearer_tokens"],
        )
        self.assertEqual(
            environment["PATHFINDER_CONTAINER_NODE_TOKEN"],
            kwargs["n6_bearer_token"],
        )
        self.assertEqual(
            ("pathfinder-sim-n2", "pathfinder-sim-n6"),
            kwargs["simulator_private_http_hosts"],
        )
        run.assert_called_once_with(
            route_package_dir=Path("routes"),
            crosswalk_dir=Path("crosswalk"),
            runtime=runtime,
            run_id="w4-local-run-v1",
            output_dir=Path("output"),
        )
        output = stream.getvalue()
        self.assertEqual(expected, json.loads(output))
        for secret in environment.values():
            self.assertNotIn(secret, output)

    def test_cli_accepts_documented_shared_secret_fallbacks(self):
        environment = {
            "PATHFINDER_N2_INDEX_TOKEN": "shared-index-secret",
            "PATHFINDER_DATA_AGENT_TOKEN": "shared-data-secret",
            "PATHFINDER_FULL_FLOW_CACHE_TOKEN": "shared-cache-secret",
            "PATHFINDER_CONTAINER_NODE_TOKEN": "semantic-secret",
        }
        with (
            mock.patch.dict("os.environ", environment, clear=True),
            mock.patch(
                "pathfinder.simulator.full_flow_w4_local_factory."
                "W4LocalRuntimeInputs",
                return_value=object(),
            ) as runtime_type,
            mock.patch(
                "pathfinder.simulator.full_flow_w4_local_run."
                "run_full_flow_w4_local_component_execution",
                return_value={"status": "COMPLETE"},
            ),
            redirect_stdout(io.StringIO()),
        ):
            status = cli_main(self.arguments())
        self.assertEqual(0, status)
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
            {"N7": "shared-cache-secret", "N8": "shared-cache-secret"},
            runtime["cache_bearer_tokens"],
        )

    def test_cli_missing_secret_fails_before_runtime_construction(self):
        stream = io.StringIO()
        with (
            mock.patch.dict("os.environ", {}, clear=True),
            mock.patch(
                "pathfinder.simulator.full_flow_w4_local_factory."
                "W4LocalRuntimeInputs",
            ) as runtime_type,
            mock.patch(
                "pathfinder.simulator.full_flow_w4_local_run."
                "run_full_flow_w4_local_component_execution",
            ) as run,
            redirect_stdout(stream),
        ):
            status = cli_main(self.arguments())
        self.assertEqual(2, status)
        payload = json.loads(stream.getvalue())
        self.assertEqual("error", payload["status"])
        self.assertIn("PATHFINDER_N3_DATA_AGENT_TOKEN", payload["message"])
        runtime_type.assert_not_called()
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
