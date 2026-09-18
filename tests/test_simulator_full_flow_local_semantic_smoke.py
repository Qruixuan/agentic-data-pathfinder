from __future__ import annotations

import hashlib
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from pathfinder.simulator.full_flow_local_semantic_admission import (
    FrozenLocalSemanticExecutionInputs,
)
from pathfinder.simulator.full_flow_local_semantic_smoke import (
    CHECKSUMS_NAME,
    FullFlowLocalSemanticSmokeError,
    FullFlowLocalSemanticSmokeExecutionError,
    RECEIPT_NAME,
    RESULTS_NAME,
    _semantic_input_invariants,
    run_full_flow_local_semantic_smokes,
    run_full_flow_semantic_smokes,
    verify_full_flow_local_semantic_smokes,
    verify_full_flow_semantic_smokes,
)
from pathfinder.simulator.full_flow_n4_serve_gate import (
    CHECKSUMS_NAME as N4_GATE_CHECKSUMS_NAME,
    GATE_NAME as N4_GATE_NAME,
)
from pathfinder.simulator.full_flow_n4_live_serve_gate import (
    CHECKSUMS_NAME as N4_LIVE_GATE_CHECKSUMS_NAME,
    GATE_NAME as N4_LIVE_GATE_NAME,
)
from pathfinder.simulator.full_flow_one_case import (
    CHECKSUMS_NAME as ONE_CASE_CHECKSUMS_NAME,
    PLAN_NAME as ONE_CASE_PLAN_NAME,
    TRIALS_NAME as ONE_CASE_TRIALS_NAME,
    FrozenFullFlowOneCasePlan,
)
from pathfinder.simulator.full_flow_matrix_runner import (
    PUBLIC_ROUTE_EVIDENCE_SCHEMA_VERSION,
    SemanticTrialExecutionError,
    TRIAL_RESULT_SCHEMA_VERSION,
)


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


CASES = (
    "n7-raw",
    "n7-indexed-raw",
    "n7-remote-derived",
    "n7-cache-miss",
    "n7-cache-hit",
    "n8-raw",
    "n8-indexed-raw",
    "n8-remote-derived",
    "n8-cache-miss",
    "n8-cache-hit",
)


class RecordingExecutor:
    def __init__(self, *, wrong_answer_case: str | None = None) -> None:
        self.calls: list[tuple[str, str]] = []
        self.wrong_answer_case = wrong_answer_case

    def execute(self, *, trial, idempotency_key):
        case_id = str(trial["trial_key"]).removeprefix("trial-")
        self.calls.append((case_id, idempotency_key))
        evidence = {
            "schema_version": PUBLIC_ROUTE_EVIDENCE_SCHEMA_VERSION,
            "status": "COMPLETE",
            "trial_key": trial["trial_key"],
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }
        evidence["evidence_sha256"] = _sha(json.dumps(
            evidence,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode())
        return {
            "schema_version": TRIAL_RESULT_SCHEMA_VERSION,
            "status": "COMPLETE",
            "trial_key": trial["trial_key"],
            "idempotency_key": idempotency_key,
            "task_success": case_id != self.wrong_answer_case,
            "semantic_answer_sha256": _sha(case_id.encode()),
            "n1_score_evidence_sha256": _sha(b"n1-" + case_id.encode()),
            "n1_score_authenticity_verified": True,
            "route_evidence_sha256": evidence["evidence_sha256"],
            "semantic_route_evidence": evidence,
            "artifact_binding_evidence_sha256": _sha(
                b"artifact-" + case_id.encode()
            ),
            "measurements": [],
            "execution_transport": "flowmesh",
            "flowmesh_workflow_evidence_sha256": _sha(
                b"flowmesh-" + case_id.encode()
            ),
            "llm_called": True,
            "telemetry_complete": True,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }


class FailingExecutor:
    def execute(self, *, trial, idempotency_key):
        del trial, idempotency_key
        raise SemanticTrialExecutionError(
            "infrastructure", "flowmesh-temporarily-unavailable"
        )


class LocalSemanticSmokeTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / "admission"
        self.source.mkdir()
        (self.source / "local-semantic-execution-admission.json").write_text(
            "{}\n", encoding="utf-8"
        )
        (self.source / "SHA256SUMS").write_text(
            "fixture\n", encoding="utf-8"
        )
        (self.source / "semantic-execution-smokes.jsonl").write_text(
            "fixture\n", encoding="utf-8"
        )
        self.n4_gate = self.root / "n4-gate"
        self.n4_gate.mkdir()
        (self.n4_gate / N4_GATE_NAME).write_text(
            "{}\n", encoding="utf-8"
        )
        (self.n4_gate / N4_GATE_CHECKSUMS_NAME).write_text(
            "fixture\n", encoding="utf-8"
        )
        self.live_n4_gate = self.root / "live-n4-gate"
        self.live_n4_gate.mkdir()
        (self.live_n4_gate / N4_LIVE_GATE_NAME).write_text(
            "{}\n", encoding="utf-8"
        )
        (self.live_n4_gate / N4_LIVE_GATE_CHECKSUMS_NAME).write_text(
            "fixture\n", encoding="utf-8"
        )
        self.n4_sources = (
            self.n4_gate,
            self.root / "compose-overlay",
            self.root / "service-bootstrap",
            self.root / "deployment",
            self.root / "logical-routes",
            self.root / "scenario.json",
            self.root / "container-plan",
            self.root / "provisioning",
            self.root / "artifact-bindings",
            self.root / "n4-package",
        )
        self.n4_source_keywords = {
            "n4_serve_gate_dir": self.n4_gate,
            "compose_overlay_dir": self.root / "compose-overlay",
            "service_bootstrap_dir": self.root / "service-bootstrap",
            "deployment_binding_dir": self.root / "deployment",
            "logical_route_dir": self.root / "logical-routes",
            "scenario_path": self.root / "scenario.json",
            "container_plan_dir": self.root / "container-plan",
            "provisioning_catalog_dir": self.root / "provisioning",
            "artifact_binding_dir": self.root / "artifact-bindings",
            "n4_package_dir": self.root / "n4-package",
        }
        self.live_gate_sources = {
            "live_receipt_bindings": ({
                "kind": "frame_bundle",
                "receipt_dir": self.root / "live-receipt",
                "n5_plan": {},
            },),
            "n4_publication_store_root": self.root / "n4-store",
            "rebound_artifact_binding_dir": self.root / "artifact-bindings",
            "rebound_semantic_matrix_dir": self.root / "rebound-semantic",
            "rebound_admission_dir": self.source,
        }
        trials = tuple({
            "trial_key": f"trial-{case_id}",
            "order_index": index,
            "executor_node_id": case_id[:2].upper(),
            "route_coordinator_binding": {
                "service_contract_id": (
                    f"{case_id[:2].upper()}.execution-compute"
                ),
                "base_url": (
                    "http://10.70.0.17:8780"
                    if case_id.startswith("n7-")
                    else "http://10.70.0.18:8780"
                ),
            },
        } for index, case_id in enumerate(CASES))
        smokes = []
        for case_id in CASES:
            row = {
                "case_id": case_id,
                "trial_key": f"trial-{case_id}",
                "expected_cache_branch": (
                    "miss" if case_id.endswith("cache-miss") else
                    "hit" if case_id.endswith("cache-hit") else None
                ),
                "prerequisite_trial_key": (
                    f"trial-{case_id.removesuffix('hit')}miss"
                    if case_id.endswith("cache-hit") else None
                ),
                "expected_executor_node_id": case_id[:2].upper(),
                "flowmesh_submission_authorized": True,
                "runtime_gate_state": "REQUIRED_NOT_EXECUTED",
                "semantic_execution_performed": False,
            }
            smokes.append(row)
        self.inputs = FrozenLocalSemanticExecutionInputs(
            admission={
                "promotion_id": "promotion-v1",
                "admission_sha256": "a" * 64,
                "deployment_id": "multi-host-v1",
                "source_commitments": {
                    "legacy_original_source_bindings": {
                        "deployment_binding_sha256": "8" * 64,
                    },
                },
            },
            bound_trials=trials,
            bound_stages=(),
            representative_smokes=tuple(smokes),
            adapter_inventory={},
        )
        self.loader = mock.patch(
            "pathfinder.simulator.full_flow_local_semantic_smoke."
            "load_full_flow_local_semantic_execution_inputs",
            return_value=self.inputs,
        )
        self.loader.start()
        self.addCleanup(self.loader.stop)
        self.n4_verifier = mock.patch(
            "pathfinder.simulator.full_flow_local_semantic_smoke."
            "verify_full_flow_n4_preprovisioned_serve_gate",
            return_value={
                "status": "VERIFIED",
                "gate_id": "n4-gate-v1",
                "gate_sha256": "9" * 64,
                "serve_profile_authorized": True,
                "authorized_compose_profile": "serve-frozen",
                "preprovisioned_snapshot_used": True,
                "live_n5_materialization_executed": False,
                "source_binding_checked": True,
            },
        )
        self.n4_verifier_mock = self.n4_verifier.start()
        self.addCleanup(self.n4_verifier.stop)
        self.result_validator = mock.patch(
            "pathfinder.simulator.full_flow_local_semantic_smoke."
            "validate_semantic_trial_result",
            side_effect=lambda value, **_: dict(value),
        )
        self.result_validator.start()
        self.addCleanup(self.result_validator.stop)

    def test_runs_ten_cases_in_dependency_order_and_verifies(self) -> None:
        executor = RecordingExecutor()
        output = self.root / "smoke"
        report = run_full_flow_local_semantic_smokes(
            self.source,
            *self.n4_sources,
            run_id="local-smoke-v1",
            executor=executor,
            output_dir=output,
        )
        self.assertEqual("VERIFIED", report["status"])
        self.assertTrue(report["full_matrix_submission_authorized"])
        self.assertEqual(list(CASES), [case for case, _ in executor.calls])
        self.assertEqual(
            {RECEIPT_NAME, RESULTS_NAME, CHECKSUMS_NAME},
            {path.name for path in output.iterdir()},
        )
        verify = verify_full_flow_local_semantic_smokes(
            output,
            local_semantic_admission_dir=self.source,
            **self.n4_source_keywords,
        )
        self.assertEqual(report["receipt_sha256"], verify["receipt_sha256"])

    def test_profiled_smoke_enforces_content_invariance_and_separation(self) -> None:
        profiled = FrozenLocalSemanticExecutionInputs(
            admission=self.inputs.admission,
            bound_trials=tuple({
                **trial,
                "semantic_input_profile": {"profile_id": "fixture"},
            } for trial in self.inputs.bound_trials),
            bound_stages=self.inputs.bound_stages,
            representative_smokes=self.inputs.representative_smokes,
            adapter_inventory=self.inputs.adapter_inventory,
        )
        content = {
            "n7-raw": "1" * 64,
            "n8-raw": "1" * 64,
            "n7-indexed-raw": "2" * 64,
            "n8-indexed-raw": "2" * 64,
            "n7-remote-derived": "3" * 64,
            "n8-remote-derived": "3" * 64,
            "n7-cache-miss": "3" * 64,
            "n7-cache-hit": "3" * 64,
            "n8-cache-miss": "3" * 64,
            "n8-cache-hit": "3" * 64,
        }
        profiles = {
            case_id: (
                "4" * 64 if case_id.endswith("raw")
                and "indexed" not in case_id else
                "5" * 64 if "indexed" in case_id else
                "6" * 64
            )
            for case_id in CASES
        }

        def rows() -> list[dict]:
            return [{
                "case_id": case_id,
                "result": {
                    "semantic_route_evidence": {
                        "semantic_input_profile_verified": True,
                        "model_input": {
                            "semantic_input_profile_verified": True,
                            "semantic_content_sha256": content[case_id],
                            "semantic_input_profile_sha256": profiles[case_id],
                        },
                    },
                },
            } for case_id in CASES]

        report = _semantic_input_invariants(profiled, rows())
        self.assertTrue(report["semantic_input_profiles_verified"])
        self.assertTrue(
            report["execution_location_semantic_invariance_verified"]
        )
        self.assertTrue(report["cache_state_semantic_invariance_verified"])
        self.assertTrue(report["route_family_semantic_separation_verified"])
        content["n8-raw"] = "7" * 64
        with self.assertRaisesRegex(
            FullFlowLocalSemanticSmokeError,
            "N7 and N8 changed semantic input content",
        ):
            _semantic_input_invariants(profiled, rows())

    def test_runs_source_bound_one_case_without_authorizing_matrix(self) -> None:
        design_ids = (
            "D0", "D1", "D2", "D3", "D3",
            "D4", "D5", "D6", "D7", "D7",
        )
        for trial, design_id in zip(
            self.inputs.bound_trials, design_ids, strict=True
        ):
            trial["flowmesh_submission_authorized"] = True
            trial["design_id"] = design_id
            trial["route_family"] = "one-case-route"
        selections = []
        for smoke, design_id in zip(
            self.inputs.representative_smokes, design_ids, strict=True
        ):
            selections.append({
                "case_id": smoke["case_id"],
                "trial_key": smoke["trial_key"],
                "executor_node_id": smoke["expected_executor_node_id"],
                "expected_cache_branch": smoke["expected_cache_branch"],
                "prerequisite_trial_key": smoke["prerequisite_trial_key"],
                "design_id": design_id,
                "representation_ids": ["raw_video"],
            })
        one_case = FrozenFullFlowOneCasePlan(
            plan={
                "plan_sha256": "1" * 64,
                "case_id": "causal-one-case-v1",
                "workload_id": "smoke-causal",
                "artifact_object_id": "video-causal",
                "safe_design_id": "D0",
            },
            trials=tuple(selections),
        )
        plan_root = self.root / "one-case-plan"
        plan_root.mkdir()
        for name in (
            ONE_CASE_PLAN_NAME,
            ONE_CASE_TRIALS_NAME,
            ONE_CASE_CHECKSUMS_NAME,
        ):
            (plan_root / name).write_text(name + "\n", encoding="utf-8")
        executor = RecordingExecutor()
        output = self.root / "one-case-run"
        with mock.patch(
            "pathfinder.simulator.full_flow_local_semantic_smoke."
            "load_full_flow_one_case_plan",
            return_value=one_case,
        ):
            report = run_full_flow_local_semantic_smokes(
                self.source,
                *self.n4_sources,
                run_id="causal-one-case-run-v1",
                executor=executor,
                output_dir=output,
                one_case_plan_dir=plan_root,
            )
        self.assertTrue(report["one_case_execution_complete"])
        self.assertEqual("smoke-causal", report["one_case_workload_id"])
        self.assertFalse(report["full_matrix_runtime_gate_satisfied"])
        self.assertFalse(report["full_matrix_submission_authorized"])
        receipt = json.loads((output / RECEIPT_NAME).read_text())
        self.assertEqual(
            "pathfinder.full-flow-one-case-run-receipt/v1alpha1",
            receipt["schema_version"],
        )
        self.assertEqual("1" * 64, receipt["one_case_plan_sha256"])
        self.assertEqual(list(CASES), [case for case, _ in executor.calls])

    def test_wrong_answer_does_not_select_model_or_block_gate(self) -> None:
        report = run_full_flow_local_semantic_smokes(
            self.source,
            *self.n4_sources,
            run_id="wrong-answer-is-observed-v1",
            executor=RecordingExecutor(wrong_answer_case="raw"),
            output_dir=self.root / "wrong-answer",
        )
        self.assertTrue(report["full_matrix_submission_authorized"])
        self.assertFalse(report["task_success_required_for_gate"])

    def test_executor_failure_is_sanitized_and_publishes_nothing(self) -> None:
        output = self.root / "failed"
        with self.assertRaises(FullFlowLocalSemanticSmokeExecutionError) as ctx:
            run_full_flow_local_semantic_smokes(
                self.source,
                *self.n4_sources,
                run_id="failed-smoke-v1",
                executor=FailingExecutor(),
                output_dir=output,
            )
        self.assertEqual("infrastructure", ctx.exception.failure_class)
        self.assertFalse(output.exists())

    def test_n4_serve_gate_failure_prevents_every_executor_call(self) -> None:
        output = self.root / "n4-blocked"
        executor = RecordingExecutor()
        with mock.patch(
            "pathfinder.simulator.full_flow_local_semantic_smoke."
            "verify_full_flow_n4_preprovisioned_serve_gate",
            side_effect=ValueError("wrong Compose profile"),
        ):
            with self.assertRaises(FullFlowLocalSemanticSmokeError):
                run_full_flow_local_semantic_smokes(
                    self.source,
                    *self.n4_sources,
                    run_id="n4-blocked-smoke-v1",
                    executor=executor,
                    output_dir=output,
                )
        self.assertEqual([], executor.calls)
        self.assertFalse(output.exists())

    def test_live_n5_gate_authorizes_smoke_and_is_bound_in_receipt(self) -> None:
        live_report = {
            "status": "VERIFIED",
            "gate_id": "live-n4-gate-v1",
            "gate_sha256": "7" * 64,
            "authorized_compose_profile": "serve-frozen",
            "live_n5_materialization_executed": True,
            "publication_companion_excluded": True,
            "n4_data_agent_rebind_inputs_verified": True,
            "n4_data_agent_runtime_rebind_executed": False,
            "source_binding_checked": True,
        }
        output = self.root / "live-smoke"
        with mock.patch(
            "pathfinder.simulator.full_flow_local_semantic_smoke."
            "verify_full_flow_n4_live_serve_gate",
            return_value=live_report,
        ) as verifier:
            report = run_full_flow_local_semantic_smokes(
                self.source,
                self.live_n4_gate,
                *self.n4_sources[1:],
                run_id="live-n5-smoke-v1",
                executor=RecordingExecutor(),
                output_dir=output,
                n4_live_gate_sources=self.live_gate_sources,
            )
            verified = verify_full_flow_local_semantic_smokes(
                output,
                local_semantic_admission_dir=self.source,
                **(
                    self.n4_source_keywords
                    | {"n4_serve_gate_dir": self.live_n4_gate}
                ),
                n4_live_gate_sources=self.live_gate_sources,
            )

        self.assertEqual("live-n5-publication", report["n4_serve_gate_kind"])
        self.assertTrue(report["n4_live_materialization_executed"])
        self.assertTrue(report["n4_rebound_inputs_verified"])
        self.assertFalse(report["n4_preprovisioned_snapshot_used"])
        self.assertEqual(report["receipt_sha256"], verified["receipt_sha256"])
        self.assertGreaterEqual(verifier.call_count, 3)
        self.n4_verifier_mock.assert_not_called()

    def test_multi_host_wrapper_reuses_the_ten_case_runner(self) -> None:
        deployment = self.root / "multi-host-deployment"
        deployment.mkdir()
        (deployment / "full-flow-deployment-binding.json").write_text(
            json.dumps({
                "deployment_id": "multi-host-v1",
                "network_binding": {"mode": "physical-private-network"},
                "service_bindings": [
                    {
                        "service_contract_id": "N7.execution-compute",
                        "base_url": "http://10.70.0.17:8780",
                    },
                    {
                        "service_contract_id": "N8.execution-compute",
                        "base_url": "http://10.70.0.18:8780",
                    },
                ],
            }) + "\n",
            encoding="utf-8",
        )
        live_report = {
            "status": "VERIFIED",
            "gate_id": "live-n4-gate-v1",
            "gate_sha256": "7" * 64,
            "authorized_compose_profile": "serve-frozen",
            "live_n5_materialization_executed": True,
            "publication_companion_excluded": True,
            "n4_data_agent_rebind_inputs_verified": True,
            "n4_data_agent_runtime_rebind_executed": False,
            "source_binding_checked": True,
        }
        output = self.root / "multi-host-smoke"
        executor = RecordingExecutor()
        with (
            mock.patch(
                "pathfinder.simulator.full_flow_local_semantic_smoke."
                "verify_full_flow_deployment_binding",
                return_value={
                    "status": "VERIFIED",
                    "backend": "multi-host-private-network",
                    "deployment_id": "multi-host-v1",
                    "binding_sha256": "8" * 64,
                },
            ),
            mock.patch(
                "pathfinder.simulator.full_flow_local_semantic_smoke."
                "verify_full_flow_n4_live_serve_gate",
                return_value=live_report,
            ),
        ):
            report = run_full_flow_semantic_smokes(
                self.source,
                self.live_n4_gate,
                deployment,
                self.root / "logical",
                self.root / "scenario.json",
                self.root / "container-plan",
                self.root / "artifact-bindings",
                run_id="multi-host-smoke-v1",
                executor=executor,
                output_dir=output,
                n4_live_gate_sources=self.live_gate_sources,
            )
            verified = verify_full_flow_semantic_smokes(
                output,
                local_semantic_admission_dir=self.source,
                n4_serve_gate_dir=self.live_n4_gate,
                deployment_binding_dir=deployment,
                logical_route_dir=self.root / "logical",
                scenario_path=self.root / "scenario.json",
                container_plan_dir=self.root / "container-plan",
                artifact_binding_dir=self.root / "artifact-bindings",
                n4_live_gate_sources=self.live_gate_sources,
            )

        self.assertEqual(list(CASES), [case for case, _ in executor.calls])
        self.assertEqual("multi-host-private-network", report[
            "runtime_environment"
        ])
        self.assertTrue(report["coordinator_origins_verified"])
        self.assertEqual(report["receipt_sha256"], verified["receipt_sha256"])

    def test_multi_host_wrapper_rejects_coordinator_drift(self) -> None:
        deployment = self.root / "drifted-deployment"
        deployment.mkdir()
        (deployment / "full-flow-deployment-binding.json").write_text(
            json.dumps({
                "deployment_id": "multi-host-v1",
                "network_binding": {"mode": "physical-private-network"},
                "service_bindings": [
                    {
                        "service_contract_id": "N7.execution-compute",
                        "base_url": "http://10.70.0.99:8780",
                    },
                    {
                        "service_contract_id": "N8.execution-compute",
                        "base_url": "http://10.70.0.18:8780",
                    },
                ],
            }) + "\n",
            encoding="utf-8",
        )
        executor = RecordingExecutor()
        with mock.patch(
            "pathfinder.simulator.full_flow_local_semantic_smoke."
            "verify_full_flow_deployment_binding",
            return_value={
                "status": "VERIFIED",
                "backend": "multi-host-private-network",
                "deployment_id": "multi-host-v1",
                "binding_sha256": "8" * 64,
            },
        ):
            with self.assertRaises(FullFlowLocalSemanticSmokeError):
                run_full_flow_semantic_smokes(
                    self.source,
                    self.live_n4_gate,
                    deployment,
                    self.root / "logical",
                    self.root / "scenario.json",
                    self.root / "container-plan",
                    self.root / "artifact-bindings",
                    run_id="drift-must-fail-v1",
                    executor=executor,
                    output_dir=self.root / "must-not-exist",
                    n4_live_gate_sources=self.live_gate_sources,
                )
        self.assertEqual([], executor.calls)

    def test_live_gate_wrong_mode_claim_fails_before_execution(self) -> None:
        output = self.root / "wrong-live-mode"
        executor = RecordingExecutor()
        with mock.patch(
            "pathfinder.simulator.full_flow_local_semantic_smoke."
            "verify_full_flow_n4_live_serve_gate",
            return_value={
                "status": "VERIFIED",
                "gate_id": "wrong-mode",
                "gate_sha256": "6" * 64,
                "authorized_compose_profile": "serve-frozen",
                "live_n5_materialization_executed": False,
                "publication_companion_excluded": True,
                "n4_data_agent_rebind_inputs_verified": True,
                "n4_data_agent_runtime_rebind_executed": False,
                "source_binding_checked": True,
            },
        ):
            with self.assertRaises(FullFlowLocalSemanticSmokeError):
                run_full_flow_local_semantic_smokes(
                    self.source,
                    self.live_n4_gate,
                    *self.n4_sources[1:],
                    run_id="wrong-live-mode-v1",
                    executor=executor,
                    output_dir=output,
                    n4_live_gate_sources=self.live_gate_sources,
                )
        self.assertEqual([], executor.calls)
        self.assertFalse(output.exists())

    def test_live_gate_kind_tamper_is_rejected_after_restamping(self) -> None:
        output = self.root / "live-kind-tamper"
        live_report = {
            "status": "VERIFIED",
            "gate_id": "live-n4-gate-v1",
            "gate_sha256": "7" * 64,
            "authorized_compose_profile": "serve-frozen",
            "live_n5_materialization_executed": True,
            "publication_companion_excluded": True,
            "n4_data_agent_rebind_inputs_verified": True,
            "n4_data_agent_runtime_rebind_executed": False,
            "source_binding_checked": True,
        }
        with mock.patch(
            "pathfinder.simulator.full_flow_local_semantic_smoke."
            "verify_full_flow_n4_live_serve_gate",
            return_value=live_report,
        ):
            run_full_flow_local_semantic_smokes(
                self.source,
                self.live_n4_gate,
                *self.n4_sources[1:],
                run_id="live-kind-tamper-v1",
                executor=RecordingExecutor(),
                output_dir=output,
                n4_live_gate_sources=self.live_gate_sources,
            )
            receipt = json.loads((output / RECEIPT_NAME).read_text())
            receipt["n4_serve_gate_kind"] = "preprovisioned-snapshot"
            unsigned = dict(receipt)
            del unsigned["receipt_sha256"]
            receipt["receipt_sha256"] = _sha(json.dumps(
                unsigned,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode())
            (output / RECEIPT_NAME).write_text(
                json.dumps(receipt, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            (output / CHECKSUMS_NAME).write_text(
                "".join(
                    f"{_sha((output / name).read_bytes())}  {name}\n"
                    for name in sorted((RECEIPT_NAME, RESULTS_NAME))
                ),
                encoding="utf-8",
            )
            with self.assertRaises(FullFlowLocalSemanticSmokeError):
                verify_full_flow_local_semantic_smokes(
                    output,
                    local_semantic_admission_dir=self.source,
                    **(
                        self.n4_source_keywords
                        | {"n4_serve_gate_dir": self.live_n4_gate}
                    ),
                    n4_live_gate_sources=self.live_gate_sources,
                )

    def test_tampered_result_is_rejected_even_with_new_checksums(self) -> None:
        output = self.root / "tampered"
        run_full_flow_local_semantic_smokes(
            self.source,
            *self.n4_sources,
            run_id="tamper-smoke-v1",
            executor=RecordingExecutor(),
            output_dir=output,
        )
        rows = [
            json.loads(line)
            for line in (output / RESULTS_NAME).read_text().splitlines()
        ]
        rows[0]["result"]["n1_score_authenticity_verified"] = False
        (output / RESULTS_NAME).write_text(
            "".join(
                json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n"
                for row in rows
            ),
            encoding="utf-8",
        )
        (output / CHECKSUMS_NAME).write_text(
            "".join(
                f"{_sha((output / name).read_bytes())}  {name}\n"
                for name in sorted((RECEIPT_NAME, RESULTS_NAME))
            ),
            encoding="utf-8",
        )
        with self.assertRaises(FullFlowLocalSemanticSmokeError):
            verify_full_flow_local_semantic_smokes(
                output,
                local_semantic_admission_dir=self.source,
                **self.n4_source_keywords,
            )


if __name__ == "__main__":
    unittest.main()
