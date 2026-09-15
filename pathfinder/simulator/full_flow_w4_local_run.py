"""One-shot local component execution for all sixteen frozen W4 trials.

Runtime endpoints and credentials are carried only by ``W4LocalRuntimeInputs``.
The persisted coordinator run and component receipt contain public operation
evidence, never those runtime values or artifact bytes.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .full_flow_w4_candidate_coordinator import (
    run_full_flow_w4_candidate_coordinator,
)
from .full_flow_w4_live_executor import (
    LiveW4CandidateOperationExecutor,
    freeze_full_flow_w4_component_execution_receipt,
    verify_full_flow_w4_component_execution_receipt,
)
from .full_flow_w4_local_factory import (
    W4LocalRuntimeInputs,
    build_local_w4_live_components,
)


class FullFlowW4LocalRunError(RuntimeError):
    """Raised when the one-shot local W4 execution cannot be verified."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowW4LocalRunError(message)


def run_full_flow_w4_local_component_execution(
    *,
    route_package_dir: str | Path,
    crosswalk_dir: str | Path,
    runtime: W4LocalRuntimeInputs,
    run_id: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Run, persist, freeze, and verify the complete local W4 matrix.

    This invokes real configured HTTP clients for index, Data Agent, cache,
    and N6 semantic work. N1 admission/return and inter-stage byte transport
    remain the explicitly non-network in-process adapters from the factory.
    """

    _require(isinstance(runtime, W4LocalRuntimeInputs), "runtime is invalid")
    requested_target = Path(output_dir)
    _require(
        not requested_target.exists() and not requested_target.is_symlink(),
        f"output directory already exists: {requested_target.resolve()}",
    )
    target = requested_target.resolve()
    coordinator_dir = target / "coordinator-run"
    receipt_dir = target / "component-receipt"
    canonical_index = runtime.index_package_dirs["N2"]
    components = build_local_w4_live_components(runtime)
    executor = LiveW4CandidateOperationExecutor(
        route_package_dir=route_package_dir,
        crosswalk_dir=crosswalk_dir,
        canonical_index_package_dir=canonical_index,
        components=components,
        evidence_class="live-local-component-execution",
    )
    coordinator = run_full_flow_w4_candidate_coordinator(
        route_package_dir,
        run_id=run_id,
        executor=executor,
        output_dir=coordinator_dir,
    )
    _require(
        coordinator.get("status") == "COMPLETE"
        and coordinator.get("trial_count") == 16,
        "local W4 coordinator did not complete all sixteen trials",
    )
    frozen = freeze_full_flow_w4_component_execution_receipt(
        coordinator_dir,
        route_package_dir=route_package_dir,
        crosswalk_dir=crosswalk_dir,
        index_package_dir=canonical_index,
        executor=executor,
        output_dir=receipt_dir,
    )
    _require(frozen.get("status") == "FROZEN", "component receipt not frozen")
    receipt = verify_full_flow_w4_component_execution_receipt(
        receipt_dir,
        coordinator_run_dir=coordinator_dir,
        route_package_dir=route_package_dir,
        crosswalk_dir=crosswalk_dir,
        index_package_dir=canonical_index,
    )
    _require(
        receipt.get("status") == "VERIFIED"
        and receipt.get("evidence_class")
        == "live-local-component-execution"
        and receipt.get("flowmesh_workflow_submitted") is False
        and receipt.get("real_cloud_performance_measured") is False
        and receipt.get("eligible_for_scientific_claims") is False,
        "local W4 receipt overstates or misstates its evidence",
    )
    return {
        "status": "COMPLETE",
        "run_id": coordinator["run_id"],
        "trial_count": coordinator["trial_count"],
        "planned_operation_count": coordinator["planned_operation_count"],
        "activated_operation_count": coordinator[
            "activated_operation_count"
        ],
        "inactive_operation_count": coordinator["inactive_operation_count"],
        "component_event_count": receipt["operation_count"],
        "llm_called": receipt["llm_called"],
        "flowmesh_workflow_submitted": False,
        "n1_admission_and_return": "in-process-public-control-only",
        "inter_stage_transport": "in-process-byte-preserving-only",
        "network_performance_measured": False,
        "real_cloud_performance_measured": False,
        "endpoints_recorded": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
        "coordinator_run_dir": str(coordinator_dir),
        "component_receipt_dir": str(receipt_dir),
        "output_dir": str(target),
    }


__all__ = [
    "FullFlowW4LocalRunError",
    "run_full_flow_w4_local_component_execution",
]
