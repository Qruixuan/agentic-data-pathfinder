"""Source-bound smoke gate for the local 64-trial semantic matrix.

The representative ten-smoke receipt is an execution precondition, not a
comment in an operator runbook.  This module binds that receipt, the promoted
public runtime package, and the original semantic/deployment sources into an
outer run contract *before* the durable matrix runner may call its executor.

The inner runner keeps its existing crash/failure acknowledgement semantics.
An incomplete outer directory is therefore resumable, while a complete outer
receipt proves which smoke evidence authorized the submitted matrix.  Nothing
in this layer weakens the local-only claim boundary: W4 remains the historical
multiple-choice placeholder and neither performance nor monetary cost is
certified.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .full_flow_deployment import (
    CHECKSUMS_NAME as DEPLOYMENT_CHECKSUMS_NAME,
    DEPLOYMENT_BINDING_NAME,
    verify_full_flow_deployment_binding,
)
from .full_flow_local_semantic_admission import (
    ADMISSION_NAME,
    CHECKSUMS_NAME as ADMISSION_CHECKSUMS_NAME,
    FrozenLocalSemanticExecutionInputs,
    load_full_flow_local_semantic_execution_inputs,
)
from .full_flow_local_semantic_smoke import (
    CHECKSUMS_NAME as SMOKE_CHECKSUMS_NAME,
    N4LiveServeGateSources,
    RECEIPT_NAME as SMOKE_RECEIPT_NAME,
    verify_full_flow_local_semantic_smokes,
)
from .full_flow_matrix_runner import (
    REPORT_NAME as INNER_REPORT_NAME,
    SemanticTrialExecutor,
    run_full_flow_semantic_matrix,
    verify_full_flow_semantic_matrix_run,
)
from .full_flow_semantic_matrix import (
    CHECKSUMS_NAME as SEMANTIC_CHECKSUMS_NAME,
    TRIALS_NAME as SEMANTIC_TRIALS_NAME,
    verify_full_flow_semantic_matrix,
)


GATE_CONTRACT_SCHEMA_VERSION = (
    "pathfinder.full-flow-local-semantic-matrix-gate-contract/v1alpha2"
)
GATE_RECEIPT_SCHEMA_VERSION = (
    "pathfinder.full-flow-local-semantic-matrix-gate-receipt/v1alpha2"
)
GATE_CONTRACT_NAME = "local-semantic-matrix-gate-contract.json"
GATE_RECEIPT_NAME = "local-semantic-matrix-gate-receipt.json"
MATRIX_RUN_DIR_NAME = "matrix-run"
CHECKSUMS_NAME = "SHA256SUMS"

_FINAL_FILES = {GATE_CONTRACT_NAME, GATE_RECEIPT_NAME, CHECKSUMS_NAME}
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class FullFlowLocalSemanticMatrixGateError(ValueError):
    """Raised when the representative-smoke gate cannot authorize a run."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowLocalSemanticMatrixGateError(message)


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FullFlowLocalSemanticMatrixGateError(
            "matrix gate value is not canonical JSON"
        ) from exc


def _json_bytes(value: Any) -> bytes:
    return _canonical(value) + b"\n"


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _digest(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{name} is not lowercase SHA-256",
    )
    return str(value)


def _identifier(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
        f"{name} is invalid",
    )
    return str(value)


def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, child in pairs:
        _require(key not in value, f"matrix gate repeats key {key}")
        value[key] = child
    return value


def _read_json(path: Path, name: str) -> dict[str, Any]:
    _require(path.is_file() and not path.is_symlink(), f"{name} is missing")
    try:
        value = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_unique_pairs,
            parse_constant=lambda token: (_ for _ in ()).throw(
                FullFlowLocalSemanticMatrixGateError(
                    f"{name} contains invalid number {token}"
                )
            ),
        )
    except FullFlowLocalSemanticMatrixGateError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowLocalSemanticMatrixGateError(
            f"cannot read {name}"
        ) from exc
    _require(isinstance(value, dict), f"{name} must be an object")
    return value


def _read_jsonl(path: Path, name: str) -> list[dict[str, Any]]:
    _require(path.is_file() and not path.is_symlink(), f"{name} is missing")
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FullFlowLocalSemanticMatrixGateError(
            f"cannot read {name}"
        ) from exc
    _require(lines and all(line.strip() for line in lines), f"{name} is empty")
    rows: list[dict[str, Any]] = []
    for position, line in enumerate(lines, start=1):
        try:
            value = json.loads(line, object_pairs_hook=_unique_pairs)
        except (json.JSONDecodeError, FullFlowLocalSemanticMatrixGateError) as exc:
            raise FullFlowLocalSemanticMatrixGateError(
                f"{name} line {position} is invalid"
            ) from exc
        _require(isinstance(value, dict), f"{name} row is not an object")
        rows.append(value)
    return rows


def _source_arguments(
    semantic_matrix_dir: str | Path,
    deployment_binding_dir: str | Path,
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    public_task_set_path: str | Path,
    artifact_binding_path: str | Path,
) -> dict[str, str | Path]:
    return {
        "semantic_matrix_dir": semantic_matrix_dir,
        "deployment_binding_dir": deployment_binding_dir,
        "logical_route_dir": logical_route_dir,
        "scenario_path": scenario_path,
        "container_plan_dir": container_plan_dir,
        "public_task_set_path": public_task_set_path,
        "artifact_binding_path": artifact_binding_path,
    }


def _live_gate_source_paths(
    sources: Mapping[str, Any] | None,
) -> tuple[Path, ...]:
    if sources is None:
        return ()
    names = (
        "n4_publication_store_root",
        "rebound_artifact_binding_dir",
        "rebound_semantic_matrix_dir",
        "rebound_admission_dir",
    )
    return tuple(Path(sources[name]).resolve() for name in names)


def _verify_promoted_trials_bind_semantic_source(
    inputs: FrozenLocalSemanticExecutionInputs,
    semantic_root: Path,
) -> str:
    source_rows = _read_jsonl(
        semantic_root / SEMANTIC_TRIALS_NAME,
        "source semantic trials",
    )
    _require(len(source_rows) == 64, "source semantic matrix is not 64 trials")
    promoted = {str(row.get("trial_key")): row for row in inputs.bound_trials}
    _require(len(promoted) == 64, "promoted trial catalog is not 64 trials")
    ordered_commitments: list[dict[str, Any]] = []
    for position, source in enumerate(source_rows):
        trial_key = str(source.get("trial_key"))
        _require(trial_key in promoted, "promoted catalog omits a source trial")
        bound = promoted[trial_key]
        source_digest = _sha256(_canonical(source))
        _require(
            bound.get("source_semantic_trial_sha256") == source_digest
            and bound.get("order_index") == position,
            "promoted trial does not bind the supplied semantic source",
        )
        ordered_commitments.append({
            "order_index": position,
            "trial_key": trial_key,
            "source_semantic_trial_sha256": source_digest,
        })
    return _sha256(_canonical(ordered_commitments))


def _gate_contract(
    local_semantic_admission_dir: str | Path,
    smoke_dir: str | Path,
    n4_serve_gate_dir: str | Path,
    compose_overlay_dir: str | Path,
    service_bootstrap_dir: str | Path,
    provisioning_catalog_dir: str | Path,
    artifact_binding_dir: str | Path,
    n4_package_dir: str | Path,
    semantic_matrix_dir: str | Path,
    deployment_binding_dir: str | Path,
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    public_task_set_path: str | Path,
    artifact_binding_path: str | Path,
    *,
    run_id: str,
    n4_live_gate_sources: N4LiveServeGateSources | None = None,
) -> dict[str, Any]:
    admission_root = Path(local_semantic_admission_dir).resolve()
    smoke_root = Path(smoke_dir).resolve()
    semantic_root = Path(semantic_matrix_dir).resolve()
    deployment_root = Path(deployment_binding_dir).resolve()
    _require(
        Path(artifact_binding_path).resolve().parent
        == Path(artifact_binding_dir).resolve(),
        "N4 gate and semantic matrix use different artifact-binding roots",
    )
    inputs = load_full_flow_local_semantic_execution_inputs(admission_root)
    smoke = verify_full_flow_local_semantic_smokes(
        smoke_root,
        local_semantic_admission_dir=admission_root,
        n4_serve_gate_dir=n4_serve_gate_dir,
        compose_overlay_dir=compose_overlay_dir,
        service_bootstrap_dir=service_bootstrap_dir,
        deployment_binding_dir=deployment_binding_dir,
        logical_route_dir=logical_route_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
        provisioning_catalog_dir=provisioning_catalog_dir,
        artifact_binding_dir=artifact_binding_dir,
        n4_package_dir=n4_package_dir,
        n4_live_gate_sources=n4_live_gate_sources,
    )
    expected_gate_kind = (
        "live-n5-publication"
        if n4_live_gate_sources is not None
        else "preprovisioned-snapshot"
    )
    _require(
        smoke.get("status") == "VERIFIED"
        and smoke.get("smoke_count") == 10
        and smoke.get("full_matrix_runtime_gate_satisfied") is True
        and smoke.get("full_matrix_submission_authorized") is True
        and smoke.get("n4_serve_gate_kind") == expected_gate_kind
        and smoke.get("n4_preprovisioned_snapshot_used")
        is (n4_live_gate_sources is None)
        and smoke.get("n4_live_materialization_executed")
        is (n4_live_gate_sources is not None)
        and smoke.get("n4_rebound_inputs_verified")
        is (n4_live_gate_sources is not None)
        and smoke.get("n4_source_binding_checked") is True
        and smoke.get("n4_publication_companion_excluded") is True
        and smoke.get("n4_authorized_compose_profile") == "serve-frozen",
        "verified ten-smoke receipt did not authorize the full matrix",
    )
    if n4_live_gate_sources is not None:
        _require(
            Path(
                n4_live_gate_sources["rebound_semantic_matrix_dir"]
            ).resolve()
            == semantic_root,
            "live N4 gate binds another rebound semantic matrix",
        )
    semantic = verify_full_flow_semantic_matrix(
        semantic_root,
        logical_route_dir,
        scenario_path,
        container_plan_dir,
        public_task_set_path,
        artifact_binding_path,
    )
    deployment = verify_full_flow_deployment_binding(
        deployment_root,
        logical_plan_dir=logical_route_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    _require(
        semantic.get("status") == "VERIFIED"
        and deployment.get("status") == "VERIFIED",
        "semantic or deployment source verification failed",
    )
    commitments = inputs.admission.get("source_commitments")
    _require(isinstance(commitments, Mapping), "admission commitments are missing")
    legacy = commitments.get("legacy_original_source_bindings")
    _require(isinstance(legacy, Mapping), "legacy source binding is missing")
    _require(
        legacy.get("semantic_matrix_plan_sha256")
        == semantic.get("plan_sha256")
        and legacy.get("semantic_matrix_source_binding_sha256")
        == semantic.get("source_binding_sha256")
        and legacy.get("semantic_matrix_checksums_sha256")
        == _sha256((semantic_root / SEMANTIC_CHECKSUMS_NAME).read_bytes())
        and legacy.get("deployment_binding_sha256")
        == deployment.get("binding_sha256")
        and legacy.get("deployment_binding_file_sha256")
        == _sha256(
            (deployment_root / DEPLOYMENT_BINDING_NAME).read_bytes()
        ),
        "promoted admission binds different semantic/deployment sources",
    )
    trial_order_sha256 = _verify_promoted_trials_bind_semantic_source(
        inputs,
        semantic_root,
    )
    smoke_receipt = _read_json(
        smoke_root / SMOKE_RECEIPT_NAME,
        "representative smoke receipt",
    )
    _require(
        smoke_receipt.get("receipt_sha256") == smoke.get("receipt_sha256"),
        "smoke verifier and receipt disagree",
    )
    worker_pin = inputs.admission.get("worker_pin")
    _require(
        isinstance(worker_pin, Mapping)
        and worker_pin.get("kind") == "worker_alias"
        and isinstance(worker_pin.get("value"), str)
        and bool(str(worker_pin["value"]).strip()),
        "promoted admission has no worker-alias pin",
    )
    contract: dict[str, Any] = {
        "schema_version": GATE_CONTRACT_SCHEMA_VERSION,
        "status": "FROZEN_SMOKE_AUTHORIZED_LOCAL_SEMANTIC_MATRIX",
        "run_id": _identifier(run_id, "run_id"),
        "promotion_id": inputs.admission["promotion_id"],
        "semantics_mode": inputs.admission["semantics_mode"],
        "worker_pin": dict(worker_pin),
        "admission_sha256": _digest(
            inputs.admission.get("admission_sha256"),
            "admission_sha256",
        ),
        "admission_file_sha256": _sha256(
            (admission_root / ADMISSION_NAME).read_bytes()
        ),
        "admission_checksums_sha256": _sha256(
            (admission_root / ADMISSION_CHECKSUMS_NAME).read_bytes()
        ),
        "smoke_run_id": smoke["run_id"],
        "smoke_receipt_sha256": smoke["receipt_sha256"],
        "smoke_receipt_file_sha256": _sha256(
            (smoke_root / SMOKE_RECEIPT_NAME).read_bytes()
        ),
        "smoke_checksums_sha256": _sha256(
            (smoke_root / SMOKE_CHECKSUMS_NAME).read_bytes()
        ),
        "n4_serve_gate_sha256": smoke["n4_serve_gate_sha256"],
        "n4_serve_gate_kind": smoke["n4_serve_gate_kind"],
        "n4_preprovisioned_snapshot_used": smoke[
            "n4_preprovisioned_snapshot_used"
        ],
        "n4_live_materialization_executed": smoke[
            "n4_live_materialization_executed"
        ],
        "n4_rebound_inputs_verified": smoke[
            "n4_rebound_inputs_verified"
        ],
        "n4_authorized_compose_profile": "serve-frozen",
        "semantic_matrix_plan_sha256": semantic["plan_sha256"],
        "semantic_matrix_source_binding_sha256": semantic[
            "source_binding_sha256"
        ],
        "deployment_binding_sha256": deployment["binding_sha256"],
        "source_semantic_trial_order_sha256": trial_order_sha256,
        "representative_smoke_count": 10,
        "full_matrix_runtime_gate_satisfied": True,
        "full_matrix_submission_authorized": True,
        "task_success_required_for_gate": False,
        "w4_task_semantics": "multiple-choice-placeholder",
        "w4_retrieval_quality_evaluated": False,
        "performance_measured": False,
        "monetary_cost_measured": False,
        "upcloud_ready": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    contract["contract_sha256"] = _sha256(_canonical(contract))
    return contract


def _write_atomic_new(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = None
    temporary: Path | None = None
    try:
        descriptor, raw = tempfile.mkstemp(
            prefix=f".{path.name}.",
            dir=path.parent,
        )
        temporary = Path(raw)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = None
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        temporary = None
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _write_expected_or_new(path: Path, payload: bytes) -> None:
    if path.exists():
        _require(
            path.is_file()
            and not path.is_symlink()
            and path.read_bytes() == payload,
            f"existing {path.name} differs from deterministic gate evidence",
        )
        return
    _write_atomic_new(path, payload)


def _receipt(
    contract: Mapping[str, Any],
    runner_report: Mapping[str, Any],
    matrix_run_root: Path,
) -> dict[str, Any]:
    receipt: dict[str, Any] = {
        "schema_version": GATE_RECEIPT_SCHEMA_VERSION,
        "status": "COMPLETE",
        "run_id": contract["run_id"],
        "contract_sha256": contract["contract_sha256"],
        "admission_sha256": contract["admission_sha256"],
        "smoke_receipt_sha256": contract["smoke_receipt_sha256"],
        "n4_serve_gate_kind": contract["n4_serve_gate_kind"],
        "semantic_matrix_plan_sha256": contract[
            "semantic_matrix_plan_sha256"
        ],
        "deployment_binding_sha256": contract[
            "deployment_binding_sha256"
        ],
        "inner_run_report_sha256": _sha256(
            (matrix_run_root / INNER_REPORT_NAME).read_bytes()
        ),
        "inner_run_checksums_sha256": _sha256(
            (matrix_run_root / CHECKSUMS_NAME).read_bytes()
        ),
        "completed_trial_count": runner_report["completed_trial_count"],
        "neutral_evidence_count": runner_report["neutral_evidence_count"],
        "full_matrix_runtime_gate_satisfied": True,
        "flowmesh_semantic_execution_completed": True,
        "w4_retrieval_quality_evaluated": False,
        "performance_measured": False,
        "monetary_cost_measured": False,
        "upcloud_ready": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    receipt["receipt_sha256"] = _sha256(_canonical(receipt))
    return receipt


def _assert_output_separate(
    output: Path,
    *sources: str | Path,
) -> None:
    for source in sources:
        root = Path(source).resolve()
        _require(
            output != root and not output.is_relative_to(root),
            "gated run output cannot be inside a frozen source directory",
        )


def _verify_outer_files(root: Path) -> None:
    _require(root.is_dir() and not root.is_symlink(), "gated run is missing")
    _require(
        {path.name for path in root.iterdir() if path.is_file()} == _FINAL_FILES
        and {path.name for path in root.iterdir() if path.is_dir()}
        == {MATRIX_RUN_DIR_NAME}
        and all(not path.is_symlink() for path in root.iterdir()),
        "gated run file structure changed",
    )
    expected = b"".join(
        f"{_sha256((root / name).read_bytes())}  {name}\n".encode("utf-8")
        for name in sorted({GATE_CONTRACT_NAME, GATE_RECEIPT_NAME})
    )
    _require(
        (root / CHECKSUMS_NAME).read_bytes() == expected,
        "gated run checksums failed",
    )


def _verify_incomplete_outer_structure(root: Path) -> None:
    entries = list(root.iterdir())
    _require(
        all(not path.is_symlink() for path in entries),
        "incomplete gated run contains a symbolic link",
    )
    _require(
        {path.name for path in entries if path.is_file()}
        <= {GATE_CONTRACT_NAME, GATE_RECEIPT_NAME}
        and {path.name for path in entries if path.is_dir()}
        <= {MATRIX_RUN_DIR_NAME},
        "incomplete gated run contains an unexpected entry",
    )


def run_smoke_gated_full_flow_local_semantic_matrix(
    local_semantic_admission_dir: str | Path,
    smoke_dir: str | Path,
    n4_serve_gate_dir: str | Path,
    compose_overlay_dir: str | Path,
    service_bootstrap_dir: str | Path,
    provisioning_catalog_dir: str | Path,
    artifact_binding_dir: str | Path,
    n4_package_dir: str | Path,
    semantic_matrix_dir: str | Path,
    deployment_binding_dir: str | Path,
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    public_task_set_path: str | Path,
    artifact_binding_path: str | Path,
    *,
    run_id: str,
    output_dir: str | Path,
    executor: SemanticTrialExecutor,
    acknowledge_failed_entry_sha256: str | None = None,
    n4_live_gate_sources: N4LiveServeGateSources | None = None,
) -> dict[str, Any]:
    """Execute/resume the matrix only after verifying its ten-smoke gate."""

    sources = _source_arguments(
        semantic_matrix_dir,
        deployment_binding_dir,
        logical_route_dir,
        scenario_path,
        container_plan_dir,
        public_task_set_path,
        artifact_binding_path,
    )
    contract = _gate_contract(
        local_semantic_admission_dir,
        smoke_dir,
        n4_serve_gate_dir,
        compose_overlay_dir,
        service_bootstrap_dir,
        provisioning_catalog_dir,
        artifact_binding_dir,
        n4_package_dir,
        **sources,
        run_id=run_id,
        n4_live_gate_sources=n4_live_gate_sources,
    )
    root = Path(output_dir).resolve()
    _assert_output_separate(
        root,
        local_semantic_admission_dir,
        smoke_dir,
        n4_serve_gate_dir,
        compose_overlay_dir,
        service_bootstrap_dir,
        provisioning_catalog_dir,
        artifact_binding_dir,
        n4_package_dir,
        semantic_matrix_dir,
        deployment_binding_dir,
        logical_route_dir,
        container_plan_dir,
        *_live_gate_source_paths(n4_live_gate_sources),
    )
    if not root.exists():
        root.mkdir(parents=True)
        _write_atomic_new(root / GATE_CONTRACT_NAME, _json_bytes(contract))
    else:
        _require(root.is_dir() and not root.is_symlink(), "gated run is invalid")
        _verify_incomplete_outer_structure(root)
        _require(
            (root / GATE_CONTRACT_NAME).read_bytes() == _json_bytes(contract),
            "existing gated run contract differs from current smoke/sources",
        )
        if (root / CHECKSUMS_NAME).exists():
            return verify_smoke_gated_full_flow_local_semantic_matrix_run(
                local_semantic_admission_dir,
                smoke_dir,
                n4_serve_gate_dir,
                compose_overlay_dir,
                service_bootstrap_dir,
                provisioning_catalog_dir,
                artifact_binding_dir,
                n4_package_dir,
                **sources,
                output_dir=root,
                n4_live_gate_sources=n4_live_gate_sources,
            )

    matrix_root = root / MATRIX_RUN_DIR_NAME
    runner_report = run_full_flow_semantic_matrix(
        **sources,
        run_id=run_id,
        output_dir=matrix_root,
        executor=executor,
        acknowledge_failed_entry_sha256=acknowledge_failed_entry_sha256,
    )
    receipt = _receipt(contract, runner_report, matrix_root)
    _write_expected_or_new(root / GATE_RECEIPT_NAME, _json_bytes(receipt))
    checksums = b"".join(
        f"{_sha256((root / name).read_bytes())}  {name}\n".encode("utf-8")
        for name in sorted({GATE_CONTRACT_NAME, GATE_RECEIPT_NAME})
    )
    _write_expected_or_new(root / CHECKSUMS_NAME, checksums)
    return verify_smoke_gated_full_flow_local_semantic_matrix_run(
        local_semantic_admission_dir,
        smoke_dir,
        n4_serve_gate_dir,
        compose_overlay_dir,
        service_bootstrap_dir,
        provisioning_catalog_dir,
        artifact_binding_dir,
        n4_package_dir,
        **sources,
        output_dir=root,
        n4_live_gate_sources=n4_live_gate_sources,
    )


def verify_smoke_gated_full_flow_local_semantic_matrix_run(
    local_semantic_admission_dir: str | Path,
    smoke_dir: str | Path,
    n4_serve_gate_dir: str | Path,
    compose_overlay_dir: str | Path,
    service_bootstrap_dir: str | Path,
    provisioning_catalog_dir: str | Path,
    artifact_binding_dir: str | Path,
    n4_package_dir: str | Path,
    semantic_matrix_dir: str | Path,
    deployment_binding_dir: str | Path,
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    public_task_set_path: str | Path,
    artifact_binding_path: str | Path,
    *,
    output_dir: str | Path,
    n4_live_gate_sources: N4LiveServeGateSources | None = None,
) -> dict[str, Any]:
    """Verify the outer gate and the inner 64-trial durable run."""

    sources = _source_arguments(
        semantic_matrix_dir,
        deployment_binding_dir,
        logical_route_dir,
        scenario_path,
        container_plan_dir,
        public_task_set_path,
        artifact_binding_path,
    )
    root = Path(output_dir).resolve()
    _assert_output_separate(
        root,
        local_semantic_admission_dir,
        smoke_dir,
        n4_serve_gate_dir,
        compose_overlay_dir,
        service_bootstrap_dir,
        provisioning_catalog_dir,
        artifact_binding_dir,
        n4_package_dir,
        semantic_matrix_dir,
        deployment_binding_dir,
        logical_route_dir,
        container_plan_dir,
        *_live_gate_source_paths(n4_live_gate_sources),
    )
    _verify_outer_files(root)
    recorded_contract = _read_json(root / GATE_CONTRACT_NAME, "gate contract")
    expected_contract = _gate_contract(
        local_semantic_admission_dir,
        smoke_dir,
        n4_serve_gate_dir,
        compose_overlay_dir,
        service_bootstrap_dir,
        provisioning_catalog_dir,
        artifact_binding_dir,
        n4_package_dir,
        **sources,
        run_id=recorded_contract.get("run_id"),
        n4_live_gate_sources=n4_live_gate_sources,
    )
    _require(
        (root / GATE_CONTRACT_NAME).read_bytes()
        == _json_bytes(expected_contract),
        "gate contract no longer matches smoke evidence and sources",
    )
    runner = verify_full_flow_semantic_matrix_run(
        **sources,
        output_dir=root / MATRIX_RUN_DIR_NAME,
    )
    expected_receipt = _receipt(
        expected_contract,
        runner,
        root / MATRIX_RUN_DIR_NAME,
    )
    _require(
        (root / GATE_RECEIPT_NAME).read_bytes()
        == _json_bytes(expected_receipt),
        "gated run receipt changed",
    )
    receipt = _read_json(root / GATE_RECEIPT_NAME, "gate receipt")
    recorded_digest = _digest(receipt.pop("receipt_sha256", None), "receipt_sha256")
    _require(
        recorded_digest == _sha256(_canonical(receipt)),
        "gated run receipt digest failed",
    )
    return {
        "status": "VERIFIED_SMOKE_GATED_LOCAL_SEMANTIC_MATRIX",
        "run_id": expected_contract["run_id"],
        "promotion_id": expected_contract["promotion_id"],
        "contract_sha256": expected_contract["contract_sha256"],
        "smoke_receipt_sha256": expected_contract["smoke_receipt_sha256"],
        "n4_serve_gate_kind": expected_contract["n4_serve_gate_kind"],
        "planned_trial_count": 64,
        "completed_trial_count": runner["completed_trial_count"],
        "neutral_evidence_count": runner["neutral_evidence_count"],
        "full_matrix_runtime_gate_satisfied": True,
        "source_binding_checked": True,
        "w4_retrieval_quality_evaluated": False,
        "performance_measured": False,
        "monetary_cost_measured": False,
        "upcloud_ready": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "CHECKSUMS_NAME",
    "FullFlowLocalSemanticMatrixGateError",
    "GATE_CONTRACT_NAME",
    "GATE_CONTRACT_SCHEMA_VERSION",
    "GATE_RECEIPT_NAME",
    "GATE_RECEIPT_SCHEMA_VERSION",
    "MATRIX_RUN_DIR_NAME",
    "run_smoke_gated_full_flow_local_semantic_matrix",
    "verify_smoke_gated_full_flow_local_semantic_matrix_run",
]
