"""Freeze a conservative trial-wrapper coordinator for the 4x8 matrix.

The matrix ledger deliberately contains more than the small linear smoke:
some trials are index-first, others have parallel independent operations, and
the D3/D7 cache trials have a branch that can only be chosen after a live
cache lookup.  A static FlowMesh graph cannot safely represent every one of
those cases by submitting individual operations.

This module is therefore a *coordinator admission* layer, not an executor.
It binds an already verified v2 matrix and formal infrastructure profile into
a deterministic, globally serial trial-wrapper schedule.  The emitted package
is deliberately non-submittable: it proves exactly what a future live
coordinator must admit, and explicitly marks conditional trials as requiring
the existing two-phase branch-aware runner.  It never starts a container,
contacts FlowMesh, reads credentials, or calls an LLM.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Mapping, Sequence

from .container_conditional_dag import resolve_conditional_container_trial
from .container_dag import (
    FlowMeshContainerDagError,
    _canonical_bytes,
    _checksums,
    _document_sha256,
    _json_bytes,
    _jsonl_bytes,
    _sha256_bytes,
    _text,
    _write_documents,
)
from .container_formal_profile import (
    FLOWMESH_CONTAINER_FORMAL_PROFILE_SCHEMA_VERSION,
    verify_flowmesh_container_formal_execution_profile,
)
from .container_matrix import (
    FLOWMESH_CONTAINER_MATRIX_PLAN_SCHEMA_VERSION,
    _read_plan as _read_matrix_plan,
    _verify_plan_contents as _verify_matrix_plan_contents,
)


FLOWMESH_CONTAINER_MATRIX_COORDINATOR_PLAN_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-matrix-coordinator-plan/v1alpha1"
)
FLOWMESH_CONTAINER_MATRIX_COORDINATOR_ADMISSION_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-matrix-coordinator-admission/v1alpha1"
)

_PLAN_FILES = {
    "flowmesh-container-matrix-coordinator-plan.json",
    "flowmesh-container-matrix-coordinator-trial-wrappers.jsonl",
    "flowmesh-container-matrix-coordinator-admission.json",
}
_COORDINATOR_ID = re.compile(r"[a-z0-9][a-z0-9._-]*")
_CONDITIONAL_DESIGNS = frozenset({"D3", "D7"})
_MEASUREMENT_SCOPE = {
    "evidence_class": "infrastructure-conformance-only",
    "semantic_task_quality_evaluated": False,
    "configured_cost_evaluated": False,
    "physical_monetary_cost_evaluated": False,
    "scientific_claims_eligible": False,
}


class FlowMeshContainerMatrixCoordinatorError(FlowMeshContainerDagError):
    """Raised when a matrix cannot form a conservative coordinator plan."""


def _coordinator_require(condition: bool, message: str) -> None:
    if not condition:
        raise FlowMeshContainerMatrixCoordinatorError(message)


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FlowMeshContainerMatrixCoordinatorError(
            f"cannot read valid {label}: {path.name}"
        ) from exc
    _coordinator_require(isinstance(value, dict), f"{label} must be an object")
    return value


def _read_jsonl(path: Path, label: str) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FlowMeshContainerMatrixCoordinatorError(
            f"cannot read {label}"
        ) from exc
    rows: list[dict[str, Any]] = []
    for number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise FlowMeshContainerMatrixCoordinatorError(
                f"{label} contains invalid JSON at line {number}"
            ) from exc
        _coordinator_require(
            isinstance(value, dict), f"{label} row must be an object"
        )
        rows.append(value)
    return rows


def _sha256_path(path: Path, label: str) -> str:
    try:
        return _sha256_bytes(path.read_bytes())
    except OSError as exc:
        raise FlowMeshContainerMatrixCoordinatorError(
            f"cannot read {label}"
        ) from exc


def _copy_mapping(value: Mapping[str, Any], label: str) -> dict[str, Any]:
    try:
        copied = json.loads(_canonical_bytes(value).decode("utf-8"))
    except (TypeError, ValueError, UnicodeError, json.JSONDecodeError) as exc:
        raise FlowMeshContainerMatrixCoordinatorError(
            f"{label} cannot be canonicalized"
        ) from exc
    _coordinator_require(isinstance(copied, dict), f"{label} must be an object")
    return copied


def _coordinator_id(value: Any) -> str:
    identifier = _text(value, "coordinator_id")
    _coordinator_require(
        _COORDINATOR_ID.fullmatch(identifier) is not None,
        "coordinator_id contains unsupported characters",
    )
    return identifier


def _read_matrix_input(
    matrix_plan_dir: str | Path,
) -> tuple[
    Path,
    dict[str, Any],
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, Any],
    dict[str, Any],
]:
    root = Path(matrix_plan_dir).resolve()
    try:
        root, matrix, trials, operations, admission = _read_matrix_plan(root)
        verified = _verify_matrix_plan_contents(
            root, matrix, trials, operations, admission
        )
    except Exception as exc:
        raise FlowMeshContainerMatrixCoordinatorError(
            "matrix plan does not verify: " + str(exc)
        ) from exc
    _coordinator_require(
        matrix.get("schema_version")
        == FLOWMESH_CONTAINER_MATRIX_PLAN_SCHEMA_VERSION,
        "matrix coordinator requires a v2 runtime-integrity matrix plan",
    )
    return root, matrix, trials, operations, admission, dict(verified)


def _read_profile_input(
    formal_execution_profile_dir: str | Path,
) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    root = Path(formal_execution_profile_dir).resolve()
    try:
        verified = verify_flowmesh_container_formal_execution_profile(root)
    except Exception as exc:
        raise FlowMeshContainerMatrixCoordinatorError(
            "formal execution profile does not verify: " + str(exc)
        ) from exc
    profile = _read_json(
        root / "flowmesh-container-formal-execution-profile.json",
        "formal execution profile",
    )
    _coordinator_require(
        profile.get("schema_version")
        == FLOWMESH_CONTAINER_FORMAL_PROFILE_SCHEMA_VERSION,
        "unsupported formal execution profile schema",
    )
    return root, profile, dict(verified)


def _validate_profile_binding(
    *,
    matrix_root: Path,
    matrix: Mapping[str, Any],
    profile_root: Path,
    profile: Mapping[str, Any],
) -> None:
    """Require an exact, non-secret profile-to-matrix relationship."""

    profile_matrix = profile.get("matrix")
    _coordinator_require(
        isinstance(profile_matrix, Mapping), "formal profile matrix binding is missing"
    )
    _coordinator_require(
        profile.get("execution_profile_id") == matrix.get("execution_profile_id"),
        "formal profile execution_profile_id does not match the matrix",
    )
    _coordinator_require(
        profile_matrix.get("matrix_plan_sha256") == matrix.get("plan_sha256"),
        "formal profile is bound to a different matrix plan",
    )
    _coordinator_require(
        profile_matrix.get("matrix_plan_file_sha256")
        == _sha256_path(
            matrix_root / "flowmesh-container-matrix-plan.json", "matrix plan"
        ),
        "formal profile matrix file binding does not match",
    )
    _coordinator_require(
        profile_matrix.get("matrix_schema_version")
        == FLOWMESH_CONTAINER_MATRIX_PLAN_SCHEMA_VERSION,
        "formal profile is not bound to a v2 matrix plan",
    )
    _coordinator_require(
        profile_matrix.get("source_git_revision")
        == matrix.get("source_git_revision"),
        "formal profile source revision does not match the matrix",
    )
    _coordinator_require(
        profile_matrix.get("worker_alias") == matrix.get("worker_alias"),
        "formal profile worker pin does not match the matrix",
    )
    _coordinator_require(
        profile_matrix.get("node_api_url_mapping_sha256")
        == _sha256_bytes(_canonical_bytes(matrix.get("node_api_urls"))),
        "formal profile node endpoint-map binding does not match the matrix",
    )
    _coordinator_require(
        profile.get("primary_trial_wrapper_max_concurrency") == 1,
        "formal profile must limit primary trial wrappers to concurrency 1",
    )
    _coordinator_require(
        profile.get("same_cache_lane_serial_execution_required") is True,
        "formal profile must require same-cache-lane serialization",
    )
    _coordinator_require(
        profile.get("cross_lane_parallelism_authorized") is False,
        "formal profile must not authorize cross-lane parallelism",
    )
    _coordinator_require(
        profile.get("trial_dispatch_unit") == "plan-bound-trial-wrapper",
        "formal profile dispatch unit is not a plan-bound trial wrapper",
    )
    _coordinator_require(
        profile.get("runtime_integrity") == matrix.get("runtime_integrity"),
        "formal profile runtime-integrity contract does not match the matrix",
    )
    _coordinator_require(
        profile.get("measurement_scope") == _MEASUREMENT_SCOPE,
        "formal profile measurement scope is not infrastructure-only",
    )
    _coordinator_require(
        profile.get("eligible_for_formal_infrastructure_execution") is True
        and profile.get("eligible_for_scientific_claims") is False,
        "formal profile eligibility boundary changed",
    )
    _coordinator_require(
        profile_root.is_dir(), "formal execution profile directory is unavailable"
    )


def _cache_lanes(operations: Sequence[Mapping[str, Any]]) -> list[dict[str, str]]:
    lanes: set[tuple[str, str]] = set()
    for operation in operations:
        cache = operation.get("cache_adapter")
        if cache is None:
            continue
        _coordinator_require(
            isinstance(cache, Mapping), "cache adapter must be an object"
        )
        lanes.add(
            (
                _text(cache.get("cache_id"), "cache_id"),
                _text(operation.get("cache_scope_id"), "cache_scope_id"),
            )
        )
    return [
        {"cache_id": cache_id, "cache_scope_id": cache_scope_id}
        for cache_id, cache_scope_id in sorted(lanes)
    ]


def _trial_wrappers(
    *,
    trials: Sequence[Mapping[str, Any]],
    operations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Derive one explicit serial wrapper per frozen trial.

    This routine is intentionally not a generic graph submitter.  For a
    conditional trial it invokes the branch resolver only to validate and
    record the required two-phase protocol; it never returns a flattened list
    of active operations as if a static graph could submit it safely.
    """

    by_trial: dict[str, list[dict[str, Any]]] = {}
    for raw in operations:
        _coordinator_require(
            isinstance(raw, Mapping), "matrix operation must be an object"
        )
        row = _copy_mapping(raw, "matrix operation")
        trial_key = _text(row.get("trial_key"), "matrix operation trial_key")
        by_trial.setdefault(trial_key, []).append(row)

    ordered_trials: list[dict[str, Any]] = []
    seen_trial_keys: set[str] = set()
    seen_order_indexes: set[int] = set()
    for raw in trials:
        _coordinator_require(isinstance(raw, Mapping), "matrix trial must be an object")
        trial = _copy_mapping(raw, "matrix trial")
        trial_key = _text(trial.get("trial_key"), "matrix trial_key")
        order_index = trial.get("order_index")
        _coordinator_require(
            type(order_index) is int and order_index >= 0,
            "matrix trial order_index must be a non-negative integer",
        )
        _coordinator_require(
            trial_key not in seen_trial_keys,
            "matrix trial keys are not unique",
        )
        _coordinator_require(
            order_index not in seen_order_indexes,
            "matrix trial order indexes are not unique",
        )
        seen_trial_keys.add(trial_key)
        seen_order_indexes.add(order_index)
        _coordinator_require(
            trial_key in by_trial and by_trial[trial_key],
            "matrix trial has no operations for coordinator admission",
        )
        ordered_trials.append(trial)
    _coordinator_require(
        len(ordered_trials) == 64,
        "formal coordinator requires exactly 64 matrix trial wrappers",
    )
    _coordinator_require(
        seen_order_indexes == set(range(64)),
        "matrix trial order indexes must be exactly 0 through 63",
    )
    _coordinator_require(
        set(by_trial) == seen_trial_keys,
        "matrix operations do not cover exactly the frozen trial ledger",
    )

    wrappers: list[dict[str, Any]] = []
    lane_predecessors: dict[tuple[str, str], str] = {}
    globally_previous: str | None = None
    for sequence_index, trial in enumerate(
        sorted(ordered_trials, key=lambda row: int(row["order_index"]))
    ):
        trial_key = str(trial["trial_key"])
        rows = by_trial[trial_key]
        keys = [
            _text(row.get("operation_key"), "matrix operation_key") for row in rows
        ]
        _coordinator_require(
            len(keys) == len(set(keys)),
            "matrix trial operation keys are not unique",
        )
        conditions = [row for row in rows if row.get("condition") is not None]
        lanes = _cache_lanes(rows)
        design_id = _text(trial.get("design_id"), "matrix design_id")
        expected_conditional = design_id in _CONDITIONAL_DESIGNS
        _coordinator_require(
            bool(conditions) == expected_conditional,
            "matrix conditional state is unsupported for design " + design_id,
        )
        _coordinator_require(
            trial.get("conditional_operation_count") == len(conditions),
            "matrix trial conditional operation count changed",
        )
        _coordinator_require(
            trial.get("operation_count") == len(rows),
            "matrix trial operation count changed",
        )
        _coordinator_require(
            trial.get("cache_scope_ids")
            == [row["cache_scope_id"] for row in lanes],
            "matrix trial cache scope summary changed",
        )

        lane_keys = [
            (row["cache_id"], row["cache_scope_id"]) for row in lanes
        ]
        same_lane_predecessors = sorted(
            {
                lane_predecessors[lane]
                for lane in lane_keys
                if lane in lane_predecessors
            }
        )
        conditional_protocol: dict[str, Any] | None = None
        if conditions:
            try:
                resolution = resolve_conditional_container_trial(
                    rows, trial_key=trial_key
                )
            except Exception as exc:
                raise FlowMeshContainerMatrixCoordinatorError(
                    "conditional matrix trial cannot form the required "
                    "two-phase protocol: " + str(exc)
                ) from exc
            conditional_protocol = {
                "strategy": "two-phase-live-cache-observation-then-frozen-branch",
                "operation_level_submission_permitted": False,
                "phase_a_operation_count": len(
                    resolution["phase_a_operation_keys"]
                ),
                "phase_b_operation_count": len(
                    resolution["phase_b_operation_keys"]
                ),
                "expected_cache_outcomes": resolution["cache_outcomes"],
                "resolution_sha256": _sha256_bytes(_canonical_bytes(resolution)),
            }
        else:
            conditional_protocol = {
                "strategy": "unconditional-plan-bound-trial-wrapper",
                "operation_level_submission_permitted": False,
            }

        wrappers.append(
            {
                "sequence_index": sequence_index,
                "trial_key": trial_key,
                "trial_id": _text(trial.get("trial_id"), "matrix trial_id"),
                "order_index": trial["order_index"],
                "workload_id": _text(trial.get("workload_id"), "matrix workload_id"),
                "workload_class": _text(
                    trial.get("workload_class"), "matrix workload_class"
                ),
                "design_id": design_id,
                "repetition": trial["repetition"],
                "executor_node_id": _text(
                    trial.get("executor_node_id"), "matrix executor_node_id"
                ),
                "operation_count": len(rows),
                "operation_keys_sha256": _sha256_bytes(
                    _canonical_bytes(keys)
                ),
                "conditional_operation_count": len(conditions),
                "cache_lanes": lanes,
                "global_serial_predecessor_trial_key": globally_previous,
                "same_cache_lane_predecessor_trial_keys": same_lane_predecessors,
                "primary_trial_wrapper_slot": 0,
                "maximum_concurrent_primary_trial_wrappers": 1,
                "conditional_protocol": conditional_protocol,
                "workflow_submitted": False,
            }
        )
        for lane in lane_keys:
            lane_predecessors[lane] = trial_key
        globally_previous = trial_key
    return wrappers


def _coordinator_admission(wrappers: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    _coordinator_require(
        len(wrappers) == 64, "coordinator admission must cover 64 wrappers"
    )
    conditional = [
        row for row in wrappers if row.get("conditional_operation_count", 0) > 0
    ]
    return {
        "schema_version": FLOWMESH_CONTAINER_MATRIX_COORDINATOR_ADMISSION_SCHEMA_VERSION,
        "primary_trial_wrapper_max_concurrency": 1,
        "global_serial_execution_required": True,
        "same_cache_lane_serial_execution_required": True,
        "cross_lane_parallelism_authorized": False,
        "flowmesh_dispatch_unit": "plan-bound-trial-wrapper",
        "operation_level_submission_permitted": False,
        "conditional_branch_protocol": "two-phase-live-cache-observation-then-frozen-branch",
        "trial_wrapper_count": len(wrappers),
        "conditional_trial_wrapper_count": len(conditional),
        "workflow_submitted": False,
        "services_started": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def _check_output_files(root: Path) -> None:
    expected_files = _PLAN_FILES | {"SHA256SUMS"}
    _coordinator_require(root.is_dir(), "matrix coordinator plan directory does not exist")
    actual_files = {path.name for path in root.iterdir() if path.is_file()}
    _coordinator_require(
        actual_files == expected_files,
        "matrix coordinator plan file set changed",
    )
    try:
        lines = (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FlowMeshContainerMatrixCoordinatorError(
            "matrix coordinator checksum file is unreadable"
        ) from exc
    observed: dict[str, str] = {}
    for line in lines:
        digest, separator, name = line.partition("  ")
        _coordinator_require(
            separator == "  " and name in _PLAN_FILES,
            "invalid matrix coordinator checksum row",
        )
        _coordinator_require(
            name not in observed, "duplicate matrix coordinator checksum"
        )
        _coordinator_require(
            _sha256_path(root / name, f"matrix coordinator {name}") == digest,
            f"matrix coordinator checksum mismatch: {name}",
        )
        observed[name] = digest
    _coordinator_require(
        set(observed) == _PLAN_FILES,
        "matrix coordinator checksums are incomplete",
    )


def _verify_plan_contents(
    root: Path,
    plan: Mapping[str, Any],
    wrappers: Sequence[Mapping[str, Any]],
    admission: Mapping[str, Any],
) -> None:
    _coordinator_require(
        plan.get("schema_version")
        == FLOWMESH_CONTAINER_MATRIX_COORDINATOR_PLAN_SCHEMA_VERSION,
        "unsupported matrix coordinator plan schema",
    )
    _coordinator_require(
        plan.get("status") == "FROZEN_DRY_RUN",
        "matrix coordinator plan is not a frozen dry-run",
    )
    _coordinator_require(
        plan.get("plan_sha256") == _document_sha256(plan, "plan_sha256"),
        "matrix coordinator plan digest mismatch",
    )
    _coordinator_id(plan.get("coordinator_id"))
    _coordinator_require(
        plan.get("trial_wrapper_count") == 64
        and len(wrappers) == 64,
        "matrix coordinator does not cover exactly 64 trial wrappers",
    )
    _coordinator_require(
        plan.get("primary_trial_wrapper_max_concurrency") == 1,
        "matrix coordinator primary concurrency changed",
    )
    _coordinator_require(
        plan.get("same_cache_lane_serial_execution_required") is True
        and plan.get("cross_lane_parallelism_authorized") is False,
        "matrix coordinator concurrency semantics changed",
    )
    _coordinator_require(
        plan.get("runtime_integrity") is not None,
        "matrix coordinator runtime-integrity contract is missing",
    )
    _coordinator_require(
        plan.get("measurement_scope") == _MEASUREMENT_SCOPE,
        "matrix coordinator measurement scope changed",
    )
    _coordinator_require(
        plan.get("trial_wrappers_sha256")
        == _sha256_path(
            root / "flowmesh-container-matrix-coordinator-trial-wrappers.jsonl",
            "matrix coordinator trial wrappers",
        ),
        "matrix coordinator wrapper ledger digest changed",
    )
    _coordinator_require(
        plan.get("admission_contract_sha256")
        == _sha256_path(
            root / "flowmesh-container-matrix-coordinator-admission.json",
            "matrix coordinator admission",
        ),
        "matrix coordinator admission contract digest changed",
    )
    _coordinator_require(
        admission == _coordinator_admission(wrappers),
        "matrix coordinator admission contract changed",
    )

    seen_keys: set[str] = set()
    expected_predecessor: str | None = None
    for index, raw in enumerate(wrappers):
        _coordinator_require(
            isinstance(raw, Mapping), "matrix coordinator wrapper must be an object"
        )
        row = raw
        _coordinator_require(
            row.get("sequence_index") == index,
            "matrix coordinator wrapper order is not globally serial",
        )
        trial_key = _text(row.get("trial_key"), "matrix coordinator trial_key")
        _coordinator_require(
            trial_key not in seen_keys,
            "matrix coordinator trial wrapper keys are not unique",
        )
        seen_keys.add(trial_key)
        _coordinator_require(
            row.get("global_serial_predecessor_trial_key") == expected_predecessor,
            "matrix coordinator global predecessor changed",
        )
        expected_predecessor = trial_key
        _coordinator_require(
            row.get("primary_trial_wrapper_slot") == 0
            and row.get("maximum_concurrent_primary_trial_wrappers") == 1,
            "matrix coordinator wrapper concurrency changed",
        )
        _coordinator_require(
            row.get("workflow_submitted") is False,
            "matrix coordinator dry-run records a workflow submission",
        )
        conditional_count = row.get("conditional_operation_count")
        _coordinator_require(
            type(conditional_count) is int and conditional_count >= 0,
            "matrix coordinator conditional operation count is invalid",
        )
        protocol = row.get("conditional_protocol")
        _coordinator_require(
            isinstance(protocol, Mapping), "matrix coordinator conditional protocol is missing"
        )
        _coordinator_require(
            protocol.get("operation_level_submission_permitted") is False,
            "matrix coordinator must not permit operation-level submission",
        )
        if conditional_count:
            _coordinator_require(
                row.get("design_id") in _CONDITIONAL_DESIGNS,
                "matrix coordinator conditional trial has an unsupported design",
            )
            _coordinator_require(
                protocol.get("strategy")
                == "two-phase-live-cache-observation-then-frozen-branch",
                "matrix coordinator conditional trial was flattened",
            )
            _coordinator_require(
                isinstance(protocol.get("expected_cache_outcomes"), Mapping)
                and bool(protocol.get("expected_cache_outcomes")),
                "matrix coordinator conditional outcome vector is missing",
            )
        else:
            _coordinator_require(
                row.get("design_id") not in _CONDITIONAL_DESIGNS,
                "matrix coordinator lost a conditional branch for D3 or D7",
            )
            _coordinator_require(
                protocol.get("strategy")
                == "unconditional-plan-bound-trial-wrapper",
                "matrix coordinator unconditional wrapper strategy changed",
            )
    _coordinator_require(
        len(seen_keys) == 64,
        "matrix coordinator wrapper coverage is incomplete",
    )
    _coordinator_require(
        plan.get("workflow_submitted") is False
        and plan.get("services_started") is False
        and plan.get("credentials_recorded") is False
        and plan.get("eligible_for_scientific_claims") is False,
        "matrix coordinator dry-run safety boundary changed",
    )


def plan_flowmesh_container_matrix_coordinator_dry_run(
    *,
    matrix_plan_dir: str | Path,
    formal_execution_profile_dir: str | Path,
    coordinator_id: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze a non-submittable, globally serial 64-trial coordinator plan.

    The plan is evidence that the full matrix has a conservative admission
    contract.  It is intentionally not a command to submit 64 workflows.
    A later live coordinator must consume this package and use the dedicated
    conditional runner for D3/D7 rather than treating both branches as active.
    """

    identifier = _coordinator_id(coordinator_id)
    target = Path(output_dir).resolve()
    _coordinator_require(
        not target.exists(), "matrix coordinator output directory already exists"
    )
    (
        matrix_root,
        matrix,
        trials,
        operations,
        _matrix_admission,
        matrix_verified,
    ) = _read_matrix_input(matrix_plan_dir)
    profile_root, profile, profile_verified = _read_profile_input(
        formal_execution_profile_dir
    )
    _validate_profile_binding(
        matrix_root=matrix_root,
        matrix=matrix,
        profile_root=profile_root,
        profile=profile,
    )
    wrappers = _trial_wrappers(trials=trials, operations=operations)
    admission = _coordinator_admission(wrappers)
    wrappers_bytes = _jsonl_bytes(wrappers)
    admission_bytes = _json_bytes(admission)
    matrix_file = matrix_root / "flowmesh-container-matrix-plan.json"
    profile_file = profile_root / "flowmesh-container-formal-execution-profile.json"
    plan: dict[str, Any] = {
        "schema_version": FLOWMESH_CONTAINER_MATRIX_COORDINATOR_PLAN_SCHEMA_VERSION,
        "status": "FROZEN_DRY_RUN",
        "coordinator_id": identifier,
        "execution_profile_id": profile["execution_profile_id"],
        "matrix": {
            "matrix_id": matrix["matrix_id"],
            "matrix_plan_sha256": matrix["plan_sha256"],
            "matrix_plan_file_sha256": _sha256_path(matrix_file, "matrix plan"),
            "matrix_operations_sha256": matrix["matrix_operations_sha256"],
            "matrix_schema_version": matrix["schema_version"],
            "matrix_verification_status": matrix_verified["status"],
        },
        "formal_execution_profile": {
            "profile_sha256": profile["profile_sha256"],
            "profile_file_sha256": _sha256_path(
                profile_file, "formal execution profile"
            ),
            "verification_status": profile_verified["status"],
        },
        "primary_trial_wrapper_max_concurrency": 1,
        "same_cache_lane_serial_execution_required": True,
        "cross_lane_parallelism_authorized": False,
        "runtime_integrity": _copy_mapping(
            matrix["runtime_integrity"], "matrix runtime-integrity contract"
        ),
        "measurement_scope": dict(_MEASUREMENT_SCOPE),
        "trial_wrapper_count": len(wrappers),
        "conditional_trial_wrapper_count": sum(
            1 for row in wrappers if row["conditional_operation_count"] > 0
        ),
        "operation_count": sum(int(row["operation_count"]) for row in wrappers),
        "trial_wrappers_sha256": _sha256_bytes(wrappers_bytes),
        "admission_contract_sha256": _sha256_bytes(admission_bytes),
        "dry_run_boundary": {
            "workflow_submission_permitted": False,
            "operation_level_submission_permitted": False,
            "containers_contacted": False,
            "flowmesh_contacted": False,
            "live_cache_branch_decision_performed": False,
            "reason": (
                "This is a frozen coordinator admission artifact. Conditional "
                "trials require a live two-phase branch-aware runner; no "
                "operation-level flattening is authorized."
            ),
        },
        "limitations": [
            "The package freezes coordinator admission only; it does not submit or execute a FlowMesh workflow.",
            "D3 and D7 remain two-phase live-cache trials. Their frozen snapshot outcomes are an admission contract, not permission to submit both branches.",
            "The first formal profile serializes all primary trial wrappers; it makes no cross-lane parallelism claim.",
            "The evidence is infrastructure-conformance-only and is not a semantic-quality, configured-cost, or physical-money result.",
        ],
        "workflow_submitted": False,
        "services_started": False,
        "llm_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    plan["plan_sha256"] = _document_sha256(plan, "plan_sha256")
    documents = {
        "flowmesh-container-matrix-coordinator-plan.json": _json_bytes(plan),
        "flowmesh-container-matrix-coordinator-trial-wrappers.jsonl": wrappers_bytes,
        "flowmesh-container-matrix-coordinator-admission.json": admission_bytes,
    }
    documents["SHA256SUMS"] = _checksums(documents)
    _write_documents(target, documents)
    verified = verify_flowmesh_container_matrix_coordinator_dry_run(target)
    return {
        "status": "FROZEN_MATRIX_COORDINATOR_DRY_RUN",
        "output_dir": str(target),
        "coordinator_id": identifier,
        "execution_profile_id": plan["execution_profile_id"],
        "matrix_plan_sha256": plan["matrix"]["matrix_plan_sha256"],
        "profile_sha256": plan["formal_execution_profile"]["profile_sha256"],
        "trial_wrapper_count": len(wrappers),
        "conditional_trial_wrapper_count": plan["conditional_trial_wrapper_count"],
        "primary_trial_wrapper_max_concurrency": 1,
        "workflow_submitted": False,
        "services_started": False,
        "eligible_for_scientific_claims": False,
        "verification_status": verified["status"],
    }


def verify_flowmesh_container_matrix_coordinator_dry_run(
    plan_dir: str | Path,
    *,
    matrix_plan_dir: str | Path | None = None,
    formal_execution_profile_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Verify a frozen dry-run coordinator package.

    Source directories are optional for portable checksum verification.  When
    both are supplied, their current verified contents must also match the
    package's frozen matrix/profile bindings; supplying only one is refused.
    """

    root = Path(plan_dir).resolve()
    _check_output_files(root)
    plan = _read_json(
        root / "flowmesh-container-matrix-coordinator-plan.json",
        "matrix coordinator plan",
    )
    wrappers = _read_jsonl(
        root / "flowmesh-container-matrix-coordinator-trial-wrappers.jsonl",
        "matrix coordinator trial wrappers",
    )
    admission = _read_json(
        root / "flowmesh-container-matrix-coordinator-admission.json",
        "matrix coordinator admission",
    )
    _verify_plan_contents(root, plan, wrappers, admission)
    _coordinator_require(
        (matrix_plan_dir is None) == (formal_execution_profile_dir is None),
        "matrix and formal profile source directories must be supplied together",
    )
    source_binding_checked = False
    if matrix_plan_dir is not None and formal_execution_profile_dir is not None:
        (
            matrix_root,
            matrix,
            trials,
            operations,
            _matrix_admission,
            _matrix_verified,
        ) = _read_matrix_input(matrix_plan_dir)
        profile_root, profile, _profile_verified = _read_profile_input(
            formal_execution_profile_dir
        )
        _validate_profile_binding(
            matrix_root=matrix_root,
            matrix=matrix,
            profile_root=profile_root,
            profile=profile,
        )
        expected_wrappers = _trial_wrappers(trials=trials, operations=operations)
        _coordinator_require(
            list(wrappers) == expected_wrappers,
            "matrix coordinator wrapper ledger does not match its sources",
        )
        _coordinator_require(
            plan["matrix"]
            == {
                "matrix_id": matrix["matrix_id"],
                "matrix_plan_sha256": matrix["plan_sha256"],
                "matrix_plan_file_sha256": _sha256_path(
                    matrix_root / "flowmesh-container-matrix-plan.json",
                    "matrix plan",
                ),
                "matrix_operations_sha256": matrix["matrix_operations_sha256"],
                "matrix_schema_version": matrix["schema_version"],
                "matrix_verification_status": "VERIFIED",
            },
            "matrix coordinator matrix binding does not match its source",
        )
        _coordinator_require(
            plan["formal_execution_profile"]
            == {
                "profile_sha256": profile["profile_sha256"],
                "profile_file_sha256": _sha256_path(
                    profile_root
                    / "flowmesh-container-formal-execution-profile.json",
                    "formal execution profile",
                ),
                "verification_status": "VERIFIED",
            },
            "matrix coordinator formal profile binding does not match its source",
        )
        source_binding_checked = True
    return {
        "status": "VERIFIED",
        "coordinator_id": plan["coordinator_id"],
        "execution_profile_id": plan["execution_profile_id"],
        "matrix_plan_sha256": plan["matrix"]["matrix_plan_sha256"],
        "profile_sha256": plan["formal_execution_profile"]["profile_sha256"],
        "trial_wrapper_count": plan["trial_wrapper_count"],
        "conditional_trial_wrapper_count": plan[
            "conditional_trial_wrapper_count"
        ],
        "primary_trial_wrapper_max_concurrency": 1,
        "source_binding_checked": source_binding_checked,
        "workflow_submitted": False,
        "services_started": False,
        "eligible_for_scientific_claims": False,
    }
