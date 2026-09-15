from __future__ import annotations

import hashlib
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from pathfinder.simulator.full_flow_deployment import DEPLOYMENT_BINDING_NAME
from pathfinder.simulator.full_flow_local_semantic_admission import (
    ADMISSION_NAME,
    FrozenLocalSemanticExecutionInputs,
)
from pathfinder.simulator.full_flow_local_semantic_matrix_gate import (
    CHECKSUMS_NAME,
    FullFlowLocalSemanticMatrixGateError,
    GATE_CONTRACT_NAME,
    GATE_RECEIPT_NAME,
    MATRIX_RUN_DIR_NAME,
    run_smoke_gated_full_flow_local_semantic_matrix,
    verify_smoke_gated_full_flow_local_semantic_matrix_run,
)
from pathfinder.simulator.full_flow_local_semantic_smoke import (
    RECEIPT_NAME as SMOKE_RECEIPT_NAME,
)
from pathfinder.simulator.full_flow_matrix_runner import REPORT_NAME
from pathfinder.simulator.full_flow_semantic_matrix import TRIALS_NAME


def _canonical(value) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


class NeverExecutor:
    def execute(self, *, trial, idempotency_key):
        del trial, idempotency_key
        raise AssertionError("the mocked runner must own this effect boundary")


class LocalSemanticMatrixGateTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.admission = self.root / "admission"
        self.smoke = self.root / "smoke"
        self.semantic = self.root / "semantic"
        self.deployment = self.root / "deployment"
        for directory in (
            self.admission,
            self.smoke,
            self.semantic,
            self.deployment,
        ):
            directory.mkdir()

        self.source_trials = [
            {
                "trial_key": f"trial-{index:02d}",
                "order_index": index,
                "source_logical_trial_sha256": f"{index:064x}",
            }
            for index in range(64)
        ]
        (self.semantic / TRIALS_NAME).write_bytes(
            b"".join(_canonical(row) + b"\n" for row in self.source_trials)
        )
        (self.semantic / CHECKSUMS_NAME).write_text(
            "semantic-checksums\n", encoding="utf-8"
        )
        (self.deployment / DEPLOYMENT_BINDING_NAME).write_text(
            "{}\n", encoding="utf-8"
        )
        (self.admission / ADMISSION_NAME).write_text(
            "{}\n", encoding="utf-8"
        )
        (self.admission / CHECKSUMS_NAME).write_text(
            "admission-checksums\n", encoding="utf-8"
        )
        self.smoke_receipt = {
            "run_id": "smoke-run-v1",
            "receipt_sha256": "b" * 64,
        }
        (self.smoke / SMOKE_RECEIPT_NAME).write_text(
            json.dumps(self.smoke_receipt) + "\n", encoding="utf-8"
        )
        (self.smoke / CHECKSUMS_NAME).write_text(
            "smoke-checksums\n", encoding="utf-8"
        )

        legacy = {
            "semantic_matrix_plan_sha256": "c" * 64,
            "semantic_matrix_source_binding_sha256": "d" * 64,
            "semantic_matrix_checksums_sha256": _sha(
                (self.semantic / CHECKSUMS_NAME).read_bytes()
            ),
            "deployment_binding_sha256": "e" * 64,
            "deployment_binding_file_sha256": _sha(
                (self.deployment / DEPLOYMENT_BINDING_NAME).read_bytes()
            ),
        }
        self.inputs = FrozenLocalSemanticExecutionInputs(
            admission={
                "promotion_id": "promotion-v1",
                "semantics_mode": "legacy-mcq-local-conformance",
                "admission_sha256": "a" * 64,
                "worker_pin": {
                    "kind": "worker_alias",
                    "value": "semantic-worker",
                },
                "source_commitments": {
                    "legacy_original_source_bindings": legacy,
                },
            },
            bound_trials=tuple({
                "trial_key": row["trial_key"],
                "order_index": row["order_index"],
                "source_semantic_trial_sha256": _sha(_canonical(row)),
            } for row in self.source_trials),
            bound_stages=(),
            representative_smokes=(),
            adapter_inventory={},
        )
        self.common = {
            "n4_serve_gate_dir": self.root / "n4-gate",
            "compose_overlay_dir": self.root / "compose-overlay",
            "service_bootstrap_dir": self.root / "service-bootstrap",
            "provisioning_catalog_dir": self.root / "provisioning",
            "artifact_binding_dir": self.root / "artifact-binding-package",
            "n4_package_dir": self.root / "n4-package",
            "semantic_matrix_dir": self.semantic,
            "deployment_binding_dir": self.deployment,
            "logical_route_dir": self.root / "logical",
            "scenario_path": self.root / "scenario.json",
            "container_plan_dir": self.root / "container-plan",
            "public_task_set_path": self.root / "tasks.json",
            "artifact_binding_path": (
                self.root / "artifact-binding-package" / "artifacts.json"
            ),
        }
        self.live_gate_sources = {
            "live_receipt_bindings": ({
                "kind": "frame_bundle",
                "receipt_dir": self.root / "live-receipt",
                "n5_plan": {},
            },),
            "n4_publication_store_root": self.root / "n4-store",
            "rebound_artifact_binding_dir": self.common[
                "artifact_binding_dir"
            ],
            "rebound_semantic_matrix_dir": self.semantic,
            "rebound_admission_dir": self.admission,
        }
        self.patches = [
            mock.patch(
                "pathfinder.simulator.full_flow_local_semantic_matrix_gate."
                "load_full_flow_local_semantic_execution_inputs",
                return_value=self.inputs,
            ),
            mock.patch(
                "pathfinder.simulator.full_flow_local_semantic_matrix_gate."
                "verify_full_flow_local_semantic_smokes",
                return_value={
                    "status": "VERIFIED",
                    "run_id": "smoke-run-v1",
                    "receipt_sha256": "b" * 64,
                    "smoke_count": 10,
                    "full_matrix_runtime_gate_satisfied": True,
                    "full_matrix_submission_authorized": True,
                    "n4_serve_gate_sha256": "8" * 64,
                    "n4_serve_gate_kind": "preprovisioned-snapshot",
                    "n4_preprovisioned_snapshot_used": True,
                    "n4_live_materialization_executed": False,
                    "n4_rebound_inputs_verified": False,
                    "n4_source_binding_checked": True,
                    "n4_publication_companion_excluded": True,
                    "n4_authorized_compose_profile": "serve-frozen",
                },
            ),
            mock.patch(
                "pathfinder.simulator.full_flow_local_semantic_matrix_gate."
                "verify_full_flow_semantic_matrix",
                return_value={
                    "status": "VERIFIED",
                    "plan_sha256": "c" * 64,
                    "source_binding_sha256": "d" * 64,
                },
            ),
            mock.patch(
                "pathfinder.simulator.full_flow_local_semantic_matrix_gate."
                "verify_full_flow_deployment_binding",
                return_value={
                    "status": "VERIFIED",
                    "binding_sha256": "e" * 64,
                },
            ),
        ]
        for patcher in self.patches:
            patcher.start()
            self.addCleanup(patcher.stop)

    @staticmethod
    def _runner_result() -> dict:
        return {
            "status": "VERIFIED",
            "completed_trial_count": 64,
            "neutral_evidence_count": 64,
        }

    @staticmethod
    def _materialize_inner(output_dir: Path) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / REPORT_NAME).write_text("{}\n", encoding="utf-8")
        (output_dir / CHECKSUMS_NAME).write_text(
            "inner-checksums\n", encoding="utf-8"
        )

    def test_verified_smoke_authorizes_and_binds_complete_run(self) -> None:
        output = self.root / "run"

        def run_effect(**kwargs):
            self._materialize_inner(Path(kwargs["output_dir"]))
            return self._runner_result()

        with (
            mock.patch(
                "pathfinder.simulator.full_flow_local_semantic_matrix_gate."
                "run_full_flow_semantic_matrix",
                side_effect=run_effect,
            ) as run,
            mock.patch(
                "pathfinder.simulator.full_flow_local_semantic_matrix_gate."
                "verify_full_flow_semantic_matrix_run",
                return_value=self._runner_result(),
            ),
        ):
            report = run_smoke_gated_full_flow_local_semantic_matrix(
                self.admission,
                self.smoke,
                **self.common,
                run_id="matrix-run-v1",
                output_dir=output,
                executor=NeverExecutor(),
            )
            verified = verify_smoke_gated_full_flow_local_semantic_matrix_run(
                self.admission,
                self.smoke,
                **self.common,
                output_dir=output,
            )

        self.assertEqual(
            "VERIFIED_SMOKE_GATED_LOCAL_SEMANTIC_MATRIX",
            report["status"],
        )
        self.assertTrue(report["full_matrix_runtime_gate_satisfied"])
        self.assertEqual(report, verified)
        self.assertEqual(
            {GATE_CONTRACT_NAME, GATE_RECEIPT_NAME, CHECKSUMS_NAME},
            {path.name for path in output.iterdir() if path.is_file()},
        )
        self.assertTrue((output / MATRIX_RUN_DIR_NAME).is_dir())
        self.assertEqual(1, run.call_count)

    def test_missing_or_unverified_smoke_fails_before_runner_and_output(self) -> None:
        output = self.root / "blocked"
        with (
            mock.patch(
                "pathfinder.simulator.full_flow_local_semantic_matrix_gate."
                "verify_full_flow_local_semantic_smokes",
                side_effect=FullFlowLocalSemanticMatrixGateError(
                    "smoke receipt is incomplete"
                ),
            ),
            mock.patch(
                "pathfinder.simulator.full_flow_local_semantic_matrix_gate."
                "run_full_flow_semantic_matrix"
            ) as run,
        ):
            with self.assertRaises(FullFlowLocalSemanticMatrixGateError):
                run_smoke_gated_full_flow_local_semantic_matrix(
                    self.admission,
                    self.smoke,
                    **self.common,
                    run_id="blocked-run-v1",
                    output_dir=output,
                    executor=NeverExecutor(),
                )
        run.assert_not_called()
        self.assertFalse(output.exists())

    def test_live_n5_smoke_gate_is_reverified_and_bound_by_outer_gate(self) -> None:
        output = self.root / "live-run"
        live_smoke = {
            "status": "VERIFIED",
            "run_id": "smoke-run-v1",
            "receipt_sha256": "b" * 64,
            "smoke_count": 10,
            "full_matrix_runtime_gate_satisfied": True,
            "full_matrix_submission_authorized": True,
            "n4_serve_gate_sha256": "7" * 64,
            "n4_serve_gate_kind": "live-n5-publication",
            "n4_preprovisioned_snapshot_used": False,
            "n4_live_materialization_executed": True,
            "n4_rebound_inputs_verified": True,
            "n4_source_binding_checked": True,
            "n4_publication_companion_excluded": True,
            "n4_authorized_compose_profile": "serve-frozen",
        }

        def run_effect(**kwargs):
            self._materialize_inner(Path(kwargs["output_dir"]))
            return self._runner_result()

        with (
            mock.patch(
                "pathfinder.simulator.full_flow_local_semantic_matrix_gate."
                "verify_full_flow_local_semantic_smokes",
                return_value=live_smoke,
            ) as smoke_verify,
            mock.patch(
                "pathfinder.simulator.full_flow_local_semantic_matrix_gate."
                "run_full_flow_semantic_matrix",
                side_effect=run_effect,
            ),
            mock.patch(
                "pathfinder.simulator.full_flow_local_semantic_matrix_gate."
                "verify_full_flow_semantic_matrix_run",
                return_value=self._runner_result(),
            ),
        ):
            report = run_smoke_gated_full_flow_local_semantic_matrix(
                self.admission,
                self.smoke,
                **self.common,
                run_id="live-matrix-run-v1",
                output_dir=output,
                executor=NeverExecutor(),
                n4_live_gate_sources=self.live_gate_sources,
            )
            verified = verify_smoke_gated_full_flow_local_semantic_matrix_run(
                self.admission,
                self.smoke,
                **self.common,
                output_dir=output,
                n4_live_gate_sources=self.live_gate_sources,
            )

        self.assertEqual("live-n5-publication", report["n4_serve_gate_kind"])
        self.assertEqual(report, verified)
        self.assertGreaterEqual(smoke_verify.call_count, 3)
        self.assertTrue(all(
            call.kwargs.get("n4_live_gate_sources")
            is self.live_gate_sources
            for call in smoke_verify.call_args_list
        ))

    def test_explicit_live_mode_rejects_preprovisioned_smoke_receipt(self) -> None:
        output = self.root / "wrong-mode"
        with (
            mock.patch(
                "pathfinder.simulator.full_flow_local_semantic_matrix_gate."
                "run_full_flow_semantic_matrix"
            ) as run,
        ):
            with self.assertRaises(FullFlowLocalSemanticMatrixGateError):
                run_smoke_gated_full_flow_local_semantic_matrix(
                    self.admission,
                    self.smoke,
                    **self.common,
                    run_id="wrong-mode-v1",
                    output_dir=output,
                    executor=NeverExecutor(),
                    n4_live_gate_sources=self.live_gate_sources,
                )
        run.assert_not_called()
        self.assertFalse(output.exists())

    def test_source_mismatch_fails_closed_before_runner(self) -> None:
        self.inputs.admission["source_commitments"][
            "legacy_original_source_bindings"
        ]["semantic_matrix_plan_sha256"] = "f" * 64
        with mock.patch(
            "pathfinder.simulator.full_flow_local_semantic_matrix_gate."
            "run_full_flow_semantic_matrix"
        ) as run:
            with self.assertRaises(FullFlowLocalSemanticMatrixGateError):
                run_smoke_gated_full_flow_local_semantic_matrix(
                    self.admission,
                    self.smoke,
                    **self.common,
                    run_id="source-mismatch-v1",
                    output_dir=self.root / "source-mismatch",
                    executor=NeverExecutor(),
                )
        run.assert_not_called()

    def test_failure_acknowledgement_is_forwarded_only_to_inner_runner(self) -> None:
        output = self.root / "resume"
        output.mkdir()
        # Obtain the exact outer contract without reaching the runner.
        from pathfinder.simulator.full_flow_local_semantic_matrix_gate import (
            _gate_contract,
        )

        contract = _gate_contract(
            self.admission,
            self.smoke,
            **self.common,
            run_id="resume-run-v1",
        )
        (output / GATE_CONTRACT_NAME).write_bytes(_canonical(contract) + b"\n")

        def run_effect(**kwargs):
            self._materialize_inner(Path(kwargs["output_dir"]))
            return self._runner_result()

        with (
            mock.patch(
                "pathfinder.simulator.full_flow_local_semantic_matrix_gate."
                "run_full_flow_semantic_matrix",
                side_effect=run_effect,
            ) as run,
            mock.patch(
                "pathfinder.simulator.full_flow_local_semantic_matrix_gate."
                "verify_full_flow_semantic_matrix_run",
                return_value=self._runner_result(),
            ),
        ):
            run_smoke_gated_full_flow_local_semantic_matrix(
                self.admission,
                self.smoke,
                **self.common,
                run_id="resume-run-v1",
                output_dir=output,
                executor=NeverExecutor(),
                acknowledge_failed_entry_sha256="9" * 64,
            )
        self.assertEqual(
            "9" * 64,
            run.call_args.kwargs["acknowledge_failed_entry_sha256"],
        )


if __name__ == "__main__":
    unittest.main()
