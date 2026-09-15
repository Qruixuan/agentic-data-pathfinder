from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from pathfinder.cli import main as cli_main


class FullFlowCompletionCliTest(unittest.TestCase):
    def _invoke(self, arguments: list[str]) -> tuple[int, dict]:
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            status = cli_main([*arguments, "--compact"])
        lines = stdout.getvalue().splitlines()
        self.assertEqual(1, len(lines), stdout.getvalue())
        return status, json.loads(lines[0])

    def test_w4_runtime_freeze_and_verifiers_are_wired(self) -> None:
        with mock.patch(
            "pathfinder.simulator.full_flow_w4_retrieval_runtime."
            "freeze_full_flow_w4_retrieval_runtime_overlay",
            return_value={"status": "FROZEN"},
        ) as freeze:
            status, payload = self._invoke([
                "freeze-simulator-full-flow-w4-retrieval-runtime",
                "--contract-dir",
                "contract",
                "--local-semantic-admission-dir",
                "admission",
                "--runtime-overlay-id",
                "runtime-v1",
                "--output-dir",
                "runtime",
            ])
        self.assertEqual(0, status)
        self.assertEqual("FROZEN", payload["status"])
        freeze.assert_called_once_with(
            Path("contract"),
            Path("admission"),
            runtime_overlay_id="runtime-v1",
            output_dir=Path("runtime"),
        )

        with mock.patch(
            "pathfinder.simulator.full_flow_w4_retrieval_runtime."
            "verify_full_flow_w4_retrieval_runtime_overlay",
            return_value={"status": "VERIFIED"},
        ) as verify:
            status, _ = self._invoke([
                "verify-simulator-full-flow-w4-retrieval-runtime",
                "--output-dir",
                "runtime",
            ])
        self.assertEqual(0, status)
        verify.assert_called_once_with(Path("runtime"))

        with mock.patch(
            "pathfinder.simulator.full_flow_w4_retrieval_runtime."
            "verify_full_flow_w4_retrieval_ranker_run",
            return_value={"status": "VERIFIED"},
        ) as verify_run:
            status, _ = self._invoke([
                "verify-simulator-full-flow-w4-ranker-run",
                "--output-dir",
                "ranker-run",
                "--runtime-overlay-dir",
                "runtime",
            ])
        self.assertEqual(0, status)
        verify_run.assert_called_once_with(
            Path("ranker-run"),
            runtime_overlay_dir=Path("runtime"),
        )

    def test_live_n5_n4_smoke_uses_runtime_only_tokens(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            plan = root / "plan.json"
            video = root / "video.mp4"
            plan.write_text('{"plan_id":"plan-v1"}\n', encoding="utf-8")
            video.write_bytes(b"video")
            environment = {
                "PATHFINDER_N5_MATERIALIZATION_TOKEN": "n5-runtime-token",
                "PATHFINDER_N4_PUBLICATION_TOKEN": "n4-runtime-token",
            }
            with (
                mock.patch.dict("os.environ", environment, clear=False),
                mock.patch(
                    "pathfinder.simulator.full_flow_live_provisioning_smoke."
                    "run_n5_n4_live_frame_bundle_provisioning_smoke",
                    return_value={"status": "VERIFIED"},
                ) as run,
            ):
                status, payload = self._invoke([
                    "run-simulator-n5-n4-live-frame-bundle-smoke",
                    "--n5-plan",
                    str(plan),
                    "--source-video",
                    str(video),
                    "--n5-base-url",
                    "http://127.0.0.1:19085",
                    "--n4-base-url",
                    "http://127.0.0.1:19184",
                    "--smoke-id",
                    "live-smoke-v1",
                    "--publication-id",
                    "publication-v1",
                    "--package-id",
                    "package-v1",
                    "--catalog-version",
                    "catalog-v1",
                    "--output-dir",
                    str(root / "receipt"),
                ])
            self.assertEqual(0, status)
            self.assertEqual("VERIFIED", payload["status"])
            call = run.call_args
            self.assertEqual({"plan_id": "plan-v1"}, call.args[0])
            self.assertEqual(b"video", call.args[1])
            self.assertEqual(
                "n5-runtime-token", call.kwargs["n5_config"].bearer_token
            )
            self.assertEqual(
                "n4-runtime-token", call.kwargs["n4_config"].bearer_token
            )

    def test_w4_lexical_ranker_binds_three_runtime_index_clients(self) -> None:
        environment = {"PATHFINDER_N2_INDEX_TOKEN": "runtime-index-token"}
        executor = object()
        with (
            mock.patch.dict("os.environ", environment, clear=False),
            mock.patch(
                "pathfinder.simulator.index_service.verify_n2_index_package",
                return_value={
                    "status": "VERIFIED",
                    "index_id": "w4-index-v1",
                    "index_sha256": "a" * 64,
                    "source_manifest_sha256": "b" * 64,
                },
            ) as verify_index,
            mock.patch(
                "pathfinder.simulator.index_service.N2IndexHTTPClient",
                side_effect=lambda **kwargs: kwargs,
            ) as client,
            mock.patch(
                "pathfinder.simulator.full_flow_w4_retrieval_runtime."
                "W4LexicalIndexRankingExecutor",
                return_value=executor,
            ) as adapter,
            mock.patch(
                "pathfinder.simulator.full_flow_w4_retrieval_runtime."
                "run_full_flow_w4_retrieval_ranker",
                return_value={"status": "COMPLETE"},
            ) as run,
        ):
            status, payload = self._invoke([
                "run-simulator-full-flow-w4-lexical-ranker",
                "--runtime-overlay-dir",
                "runtime",
                "--n2-index-base-url",
                "http://pathfinder-full-flow-n2:9082",
                "--n7-index-base-url",
                "http://pathfinder-full-flow-n7-index:9082",
                "--n8-index-base-url",
                "http://pathfinder-full-flow-n8-index:9082",
                "--index-package-dir",
                "index-package",
                "--run-id",
                "w4-run-v1",
                "--allow-http-simulator-host",
                "pathfinder-full-flow-n2",
                "--output-dir",
                "ranker-run",
            ])
        self.assertEqual(0, status)
        self.assertEqual("COMPLETE", payload["status"])
        verify_index.assert_called_once_with(Path("index-package"))
        self.assertEqual(3, client.call_count)
        clients = adapter.call_args.kwargs["clients"]
        self.assertEqual({"N2", "N7", "N8"}, set(clients))
        self.assertTrue(all(
            value["bearer_token"] == "runtime-index-token"
            and value["expected_index_sha256"] == "a" * 64
            and value["expected_index_id"] == "w4-index-v1"
            for value in clients.values()
        ))
        self.assertEqual(
            {
                "clients": clients,
                "index_package_dir": Path("index-package"),
                "index_id": "w4-index-v1",
                "index_sha256": "a" * 64,
                "source_manifest_sha256": "b" * 64,
            },
            adapter.call_args.kwargs,
        )
        run.assert_called_once_with(
            Path("runtime"),
            run_id="w4-run-v1",
            executor=executor,
            output_dir=Path("ranker-run"),
        )

    def test_w4_candidate_route_blueprint_commands_are_wired(self) -> None:
        with mock.patch(
            "pathfinder.simulator.full_flow_w4_candidate_routes."
            "freeze_full_flow_w4_candidate_routes",
            return_value={"status": "FROZEN"},
        ) as freeze:
            status, _ = self._invoke([
                "freeze-simulator-full-flow-w4-candidate-routes",
                "--runtime-overlay-dir",
                "runtime",
                "--n3-package-dir",
                "n3",
                "--n4-package-dir",
                "n4",
                "--index-package-dir",
                "index",
                "--exact-range-catalog-dir",
                "ranges",
                "--physical-plan-id",
                "w4-physical-v1",
                "--output-dir",
                "blueprints",
            ])
        self.assertEqual(0, status)
        freeze.assert_called_once_with(
            Path("runtime"),
            Path("n3"),
            Path("n4"),
            Path("index"),
            Path("ranges"),
            physical_plan_id="w4-physical-v1",
            output_dir=Path("blueprints"),
        )

        with mock.patch(
            "pathfinder.simulator.full_flow_w4_candidate_routes."
            "verify_full_flow_w4_candidate_routes",
            return_value={"status": "VERIFIED"},
        ) as verify:
            status, _ = self._invoke([
                "verify-simulator-full-flow-w4-candidate-routes",
                "--output-dir",
                "blueprints",
            ])
        self.assertEqual(0, status)
        verify.assert_called_once_with(Path("blueprints"))

    def test_w4_candidate_conformance_commands_are_wired(self) -> None:
        executor = object()
        with (
            mock.patch(
                "pathfinder.simulator.full_flow_w4_candidate_coordinator."
                "DeterministicW4CandidateOperationExecutor",
                return_value=executor,
            ),
            mock.patch(
                "pathfinder.simulator.full_flow_w4_candidate_coordinator."
                "run_full_flow_w4_candidate_coordinator",
                return_value={"status": "COMPLETE"},
            ) as run,
        ):
            status, payload = self._invoke([
                "run-simulator-full-flow-w4-candidate-conformance",
                "--route-package-dir",
                "candidate-routes",
                "--run-id",
                "w4-candidate-run-v1",
                "--output-dir",
                "candidate-run",
            ])
        self.assertEqual(0, status)
        self.assertEqual("COMPLETE", payload["status"])
        run.assert_called_once_with(
            Path("candidate-routes"),
            run_id="w4-candidate-run-v1",
            executor=executor,
            output_dir=Path("candidate-run"),
        )

        with mock.patch(
            "pathfinder.simulator.full_flow_w4_candidate_coordinator."
            "verify_full_flow_w4_candidate_coordinator_run",
            return_value={"status": "VERIFIED"},
        ) as verify:
            status, payload = self._invoke([
                "verify-simulator-full-flow-w4-candidate-conformance",
                "--run-dir",
                "candidate-run",
                "--route-package-dir",
                "candidate-routes",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        verify.assert_called_once_with(
            Path("candidate-run"),
            route_package_dir=Path("candidate-routes"),
        )

    def test_live_n5_n4_receipt_verifier_is_wired(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            plan = root / "plan.json"
            plan.write_text('{"plan_id":"plan-v1"}\n', encoding="utf-8")
            with mock.patch(
                "pathfinder.simulator.full_flow_live_provisioning_smoke."
                "verify_n5_n4_live_frame_bundle_provisioning_smoke",
                return_value={"status": "VERIFIED"},
            ) as verify:
                status, _ = self._invoke([
                    "verify-simulator-n5-n4-live-frame-bundle-smoke",
                    "--output-dir",
                    str(root / "receipt"),
                    "--n5-plan",
                    str(plan),
                ])
            self.assertEqual(0, status)
            verify.assert_called_once_with(
                root / "receipt",
                n5_plan={"plan_id": "plan-v1"},
            )

    def test_live_n4_serve_gate_commands_are_wired(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            bindings_path = root / "bindings.json"
            bindings = [{
                "kind": "frame-bundle",
                "receipt_dir": "receipt",
                "n5_plan": "plan.json",
            }]
            bindings_path.write_text(json.dumps(bindings), encoding="utf-8")
            common = [
                "--live-receipt-bindings",
                str(bindings_path),
                "--n4-publication-store-root",
                "n4-store",
                "--rebound-artifact-binding-dir",
                "artifact-bindings",
                "--rebound-semantic-matrix-dir",
                "semantic-matrix",
                "--rebound-admission-dir",
                "admission",
            ]
            with mock.patch(
                "pathfinder.simulator.full_flow_n4_live_serve_gate."
                "freeze_full_flow_n4_live_serve_gate",
                return_value={"status": "VERIFIED"},
            ) as freeze:
                status, _ = self._invoke([
                    "freeze-simulator-full-flow-n4-live-serve-gate",
                    *common,
                    "--gate-id",
                    "live-gate-v1",
                    "--output-dir",
                    "live-gate",
                ])
            self.assertEqual(0, status)
            freeze.assert_called_once_with(
                bindings,
                Path("n4-store"),
                Path("artifact-bindings"),
                Path("semantic-matrix"),
                Path("admission"),
                gate_id="live-gate-v1",
                output_dir=Path("live-gate"),
            )

            with mock.patch(
                "pathfinder.simulator.full_flow_n4_live_serve_gate."
                "verify_full_flow_n4_live_serve_gate",
                return_value={"status": "VERIFIED"},
            ) as verify:
                status, _ = self._invoke([
                    "verify-simulator-full-flow-n4-live-serve-gate",
                    "--gate-dir",
                    "live-gate",
                    *common,
                ])
            self.assertEqual(0, status)
            verify.assert_called_once_with(
                Path("live-gate"),
                bindings,
                Path("n4-store"),
                Path("artifact-bindings"),
                Path("semantic-matrix"),
                Path("admission"),
            )

    def test_live_digest_smoke_uses_runtime_only_tokens(self) -> None:
        environment = {
            "PATHFINDER_N5_DIGEST_TOKEN": "n5-digest-runtime-token",
            "PATHFINDER_N4_PUBLICATION_TOKEN": "n4-runtime-token",
        }
        with (
            mock.patch.dict("os.environ", environment, clear=False),
            mock.patch(
                "pathfinder.simulator.full_flow_live_provisioning_smoke."
                "run_n5_n4_live_multimodal_digest_provisioning_smoke",
                return_value={"status": "VERIFIED"},
            ) as run,
        ):
            status, payload = self._invoke([
                "run-simulator-n5-n4-live-digest-smoke",
                "--n5-digest-plan-dir",
                "digest-plan",
                "--source-video",
                "video.mp4",
                "--n5-digest-base-url",
                "http://127.0.0.1:19185",
                "--n4-base-url",
                "http://127.0.0.1:19184",
                "--smoke-id",
                "digest-smoke-v1",
                "--request-id",
                "digest-request-v1",
                "--publication-id",
                "digest-publication-v1",
                "--package-id",
                "digest-package-v1",
                "--catalog-version",
                "digest-catalog-v1",
                "--output-dir",
                "digest-receipt",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        call = run.call_args
        self.assertEqual(Path("digest-plan"), call.args[0])
        self.assertEqual(Path("video.mp4"), call.args[1])
        self.assertEqual(
            "n4-runtime-token", call.kwargs["n4_config"].bearer_token
        )

    def test_live_digest_receipt_verifier_is_wired(self) -> None:
        with mock.patch(
            "pathfinder.simulator.full_flow_live_provisioning_smoke."
            "verify_n5_n4_live_multimodal_digest_provisioning_smoke",
            return_value={"status": "VERIFIED"},
        ) as verify:
            status, _ = self._invoke([
                "verify-simulator-n5-n4-live-digest-smoke",
                "--output-dir",
                "digest-receipt",
                "--n5-digest-plan-dir",
                "digest-plan",
                "--source-video",
                "video.mp4",
            ])
        self.assertEqual(0, status)
        verify.assert_called_once_with(
            Path("digest-receipt"),
            n5_digest_plan_dir=Path("digest-plan"),
            source_video_path=Path("video.mp4"),
        )


if __name__ == "__main__":
    unittest.main()
