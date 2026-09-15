"""Legacy blocked full-flow aggregation contract.

This module is retained only to verify historical blocked artifacts.  New
work must use :mod:`full_flow_pre_upcloud_readiness`, the smoke-gated semantic
matrix contract, and the separate W4 FlowMesh contract.  It verifies
every package that can be prepared before a cloud deployment, records only
public identifiers and cryptographic commitments, and reproduces the result
from the original sources during verification.  In particular, it never
copies the N1 hidden labels, deployment endpoints, or credential values.

Its historical schema deliberately remains blocked and must not be promoted
now that the route adapters exist.  It is not an execution result and it is
not performance, cost, or scientific evidence.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any, Mapping, Sequence

from .config import load_simulator_scenario
from .data_agent_semantic_vertical import (
    load_data_agent_frame_bundle_semantic_spec,
)
from .full_flow_artifact_bindings import (
    ARTIFACT_BINDINGS_NAME,
    CHECKSUMS_NAME as ARTIFACT_CHECKSUMS_NAME,
    PROVENANCE_NAME as ARTIFACT_PROVENANCE_NAME,
    verify_full_flow_artifact_bindings,
)
from .full_flow_compose_overlay import (
    CHECKSUMS_NAME as OVERLAY_CHECKSUMS_NAME,
    GATE_NAME as OVERLAY_GATE_NAME,
    MANIFEST_NAME as OVERLAY_MANIFEST_NAME,
    verify_full_flow_local_compose_overlay,
)
from .full_flow_deployment import (
    CHECKSUMS_NAME as DEPLOYMENT_CHECKSUMS_NAME,
    DEPLOYMENT_BINDING_NAME,
    verify_full_flow_deployment_binding,
)
from .full_flow_semantic_execution_admission import (
    ADMISSION_NAME,
    CHECKSUMS_NAME as ADMISSION_CHECKSUMS_NAME,
    GAPS_NAME as ADMISSION_GAPS_NAME,
    verify_full_flow_semantic_execution_admission,
)
from .full_flow_semantic_matrix import (
    CHECKSUMS_NAME as SEMANTIC_CHECKSUMS_NAME,
    PLAN_NAME as SEMANTIC_PLAN_NAME,
    TRIALS_NAME as SEMANTIC_TRIALS_NAME,
    verify_full_flow_semantic_matrix,
)
from .full_flow_service_bootstrap import (
    BOOTSTRAP_NAME,
    CHECKSUMS_NAME as BOOTSTRAP_CHECKSUMS_NAME,
    LAUNCHERS_NAME as BOOTSTRAP_LAUNCHERS_NAME,
    verify_full_flow_local_service_bootstrap,
)
from .full_flow_tasks import (
    CHECKSUMS as TASK_PLANE_CHECKSUMS_NAME,
    ORACLE_PACKAGE as TASK_PLANE_ORACLE_PACKAGE,
    PUBLIC_TASK_SET as TASK_PLANE_PUBLIC_TASK_SET,
    TASK_PLANE_MANIFEST,
    verify_full_flow_task_plane,
)
from .hidden_oracle import verify_n1_oracle_package
from .hidden_oracle_commitment import (
    CHECKSUMS_NAME as ORACLE_COMMITMENT_CHECKSUMS_NAME,
    COMMITMENT_NAME as ORACLE_COMMITMENT_NAME,
    verify_n1_oracle_preselection_commitment,
)
from .n4_derived_data_plane import (
    CHECKSUMS_NAME as N4_CHECKSUMS_NAME,
    PACKAGE_MANIFEST_NAME as N4_MANIFEST_NAME,
)
from .policy_oed_bridge import (
    CHECKSUMS_NAME as POLICY_OED_CHECKSUMS_NAME,
    OED_MANIFEST_NAME,
    POLICY_MANIFEST_NAME,
    verify_oed_prospective_selection,
    verify_policy_assignment,
)
from .raw_cold_data_plane import (
    CHECKSUMS_NAME as N3_CHECKSUMS_NAME,
    PACKAGE_MANIFEST_NAME as N3_MANIFEST_NAME,
)


EXPERIMENT_FREEZE_SCHEMA_VERSION = (
    "pathfinder.full-flow-offline-experiment-freeze/v1alpha1"
)
EXPERIMENT_FREEZE_NAME = "full-flow-offline-experiment-freeze.json"
CHECKSUMS_NAME = "SHA256SUMS"

_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}\Z")
_MODEL_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+/-]{0,255}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_URL = re.compile(r"https?://", re.IGNORECASE)
_CONTENT_FILES = frozenset({EXPERIMENT_FREEZE_NAME})
_ALL_FILES = _CONTENT_FILES | {CHECKSUMS_NAME}


class FullFlowExperimentFreezeError(ValueError):
    """Raised when the aggregate pre-execution freeze is not reproducible."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowExperimentFreezeError(message)


def _identifier(value: Any, label: str) -> str:
    _require(
        isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
        f"{label} is invalid",
    )
    return str(value)


def _text(value: Any, label: str, *, max_bytes: int = 1024) -> str:
    _require(
        isinstance(value, str)
        and bool(value)
        and value == value.strip()
        and "\x00" not in value
        and len(value.encode("utf-8")) <= max_bytes,
        f"{label} is invalid",
    )
    return str(value)


def _model_id(value: Any) -> str:
    _require(
        isinstance(value, str) and _MODEL_ID.fullmatch(value) is not None,
        "semantic execution model is invalid",
    )
    return str(value)


def _digest(value: Any, label: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{label} is not a SHA-256 digest",
    )
    return str(value)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            indent=2,
        )
        + "\n"
    ).encode("utf-8")


def _strict_json(path: Path, label: str) -> dict[str, Any]:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            _require(key not in result, f"{label} repeats key {key}")
            result[key] = value
        return result

    _require(path.is_file() and not path.is_symlink(), f"{label} is missing")
    try:
        value = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=unique,
            parse_constant=lambda token: (_ for _ in ()).throw(
                FullFlowExperimentFreezeError(
                    f"{label} contains invalid constant {token}"
                )
            ),
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowExperimentFreezeError(f"cannot read {label}") from exc
    _require(isinstance(value, dict), f"{label} must be an object")
    return value


def _strict_jsonl(path: Path, label: str) -> list[dict[str, Any]]:
    _require(path.is_file() and not path.is_symlink(), f"{label} is missing")
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FullFlowExperimentFreezeError(f"cannot read {label}") from exc
    _require(bool(lines), f"{label} is empty")
    rows: list[dict[str, Any]] = []
    for index, line in enumerate(lines):
        _require(bool(line.strip()), f"{label} contains a blank row")
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise FullFlowExperimentFreezeError(
                f"cannot read {label} row {index}"
            ) from exc
        _require(isinstance(row, dict), f"{label} row {index} is not an object")
        rows.append(row)
    return rows


def _file_sha256(path: Path, label: str) -> str:
    _require(path.is_file() and not path.is_symlink(), f"{label} is missing")
    return _sha256(path.read_bytes())


def _checksum_commitment(root: Path, name: str, label: str) -> str:
    return _file_sha256(root / name, f"{label} checksum file")


def _assert_public_safe(value: Any, path: str = "freeze") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            _assert_public_safe(child, f"{path}.{key}")
        return
    if isinstance(value, list):
        for index, child in enumerate(value):
            _assert_public_safe(child, f"{path}[{index}]")
        return
    if isinstance(value, str):
        _require(_URL.search(value) is None, f"{path} contains an endpoint URL")
        _require(
            not value.casefold().startswith("bearer "),
            f"{path} contains a bearer credential",
        )


def _semantic_model_binding(
    semantic_spec_paths: Sequence[str | Path],
    task_plane_root: Path,
) -> tuple[str, list[str]]:
    _require(
        isinstance(semantic_spec_paths, Sequence)
        and not isinstance(semantic_spec_paths, (str, bytes))
        and bool(semantic_spec_paths),
        "semantic_spec_paths must be a non-empty sequence",
    )
    specs = [
        load_data_agent_frame_bundle_semantic_spec(path)
        for path in semantic_spec_paths
    ]
    hashes = sorted(spec.source_sha256 for spec in specs)
    _require(
        len(hashes) == len(set(hashes)),
        "semantic_spec_paths repeat a source document",
    )
    task_manifest = _strict_json(
        task_plane_root / TASK_PLANE_MANIFEST,
        "task-plane manifest",
    )
    expected_hashes = task_manifest.get("semantic_spec_source_sha256")
    _require(
        hashes == expected_hashes,
        "semantic specs do not exactly open the verified task-plane source hashes",
    )
    models = sorted({spec.document["expected_model"] for spec in specs})
    _require(
        len(models) == 1,
        "semantic task sources do not freeze exactly one execution model",
    )
    return _model_id(models[0]), hashes


def _optional_policy_binding(
    policy_assignment_dir: str | Path | None,
    *,
    logical_route_dir: Path,
    scenario_path: Path,
    container_plan_dir: Path,
) -> dict[str, Any]:
    if policy_assignment_dir is None:
        return {
            "included": False,
            "status": "NOT_SUPPLIED",
            "selection_sha256": None,
            "checksums_sha256": None,
        }
    root = Path(policy_assignment_dir).resolve()
    report = verify_policy_assignment(
        assignment_dir=root,
        logical_route_plan_dir=logical_route_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    return {
        "included": True,
        "status": report["status"],
        "policy_id": report["policy_id"],
        "selection_sha256": report["assignment_sha256"],
        "selected_trial_count": report["selected_trial_count"],
        "checksums_sha256": _checksum_commitment(
            root,
            POLICY_OED_CHECKSUMS_NAME,
            "policy assignment",
        ),
        "manifest_file_sha256": _file_sha256(
            root / POLICY_MANIFEST_NAME,
            "policy assignment manifest",
        ),
    }


def _optional_oed_binding(
    oed_selection_dir: str | Path | None,
    *,
    logical_route_dir: Path,
    scenario_path: Path,
    container_plan_dir: Path,
) -> dict[str, Any]:
    if oed_selection_dir is None:
        return {
            "included": False,
            "status": "NOT_SUPPLIED",
            "selection_sha256": None,
            "checksums_sha256": None,
        }
    root = Path(oed_selection_dir).resolve()
    report = verify_oed_prospective_selection(
        selection_dir=root,
        logical_route_plan_dir=logical_route_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    return {
        "included": True,
        "status": report["status"],
        "oed_request_id": report["oed_request_id"],
        "selection_sha256": report["prospective_plan_sha256"],
        "selected_trial_count": report["requested_trial_count"],
        "checksums_sha256": _checksum_commitment(
            root,
            POLICY_OED_CHECKSUMS_NAME,
            "OED selection",
        ),
        "manifest_file_sha256": _file_sha256(
            root / OED_MANIFEST_NAME,
            "OED selection manifest",
        ),
    }


def _documents(
    *,
    freeze_id: str,
    semantic_matrix_dir: Path,
    artifact_binding_package_dir: Path,
    deployment_binding_dir: Path,
    oracle_commitment_dir: Path,
    n1_oracle_package_dir: Path,
    service_bootstrap_dir: Path,
    compose_overlay_dir: Path,
    semantic_execution_admission_dir: Path,
    logical_route_dir: Path,
    scenario_path: Path,
    container_plan_dir: Path,
    task_plane_dir: Path,
    n3_package_dir: Path,
    n4_package_dir: Path,
    public_task_set_path: Path,
    artifact_binding_path: Path,
    semantic_spec_paths: Sequence[str | Path],
    policy_assignment_dir: str | Path | None,
    oed_selection_dir: str | Path | None,
) -> dict[str, bytes]:
    freeze_id = _identifier(freeze_id, "freeze_id")

    artifact_report = verify_full_flow_artifact_bindings(
        artifact_binding_package_dir,
        logical_route_dir,
        scenario_path,
        container_plan_dir,
        task_plane_dir,
        n3_package_dir,
        n4_package_dir,
    )
    task_report = verify_full_flow_task_plane(task_plane_dir)
    _require(
        public_task_set_path.read_bytes()
        == (task_plane_dir / TASK_PLANE_PUBLIC_TASK_SET).read_bytes(),
        "semantic matrix public tasks differ from the verified task plane",
    )
    _require(
        artifact_binding_path.read_bytes()
        == (artifact_binding_package_dir / ARTIFACT_BINDINGS_NAME).read_bytes(),
        "semantic matrix artifacts differ from the verified binding package",
    )
    semantic_report = verify_full_flow_semantic_matrix(
        semantic_matrix_dir,
        logical_route_dir,
        scenario_path,
        container_plan_dir,
        public_task_set_path,
        artifact_binding_path,
    )
    deployment_report = verify_full_flow_deployment_binding(
        deployment_binding_dir,
        logical_plan_dir=logical_route_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    oracle_report = verify_n1_oracle_package(n1_oracle_package_dir)
    nested_oracle = task_plane_dir / TASK_PLANE_ORACLE_PACKAGE
    nested_report = verify_n1_oracle_package(nested_oracle)
    _require(
        oracle_report["oracle_id"] == nested_report["oracle_id"]
        and (n1_oracle_package_dir / "SHA256SUMS").read_bytes()
        == (nested_oracle / "SHA256SUMS").read_bytes(),
        "N1 oracle package differs from the verified task-plane oracle",
    )
    commitment_report = verify_n1_oracle_preselection_commitment(
        oracle_commitment_dir,
        oracle_package_dir=n1_oracle_package_dir,
    )
    bootstrap_report = verify_full_flow_local_service_bootstrap(
        service_bootstrap_dir,
        logical_plan_dir=logical_route_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    overlay_report = verify_full_flow_local_compose_overlay(
        compose_overlay_dir,
        service_bootstrap_dir=service_bootstrap_dir,
        deployment_binding_dir=deployment_binding_dir,
        logical_plan_dir=logical_route_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    _require(
        overlay_report["n4_operator_gate_required"] is True
        and overlay_report["n4_gate_satisfied"] is False,
        "Compose operator gate is not fail-closed",
    )
    admission_report = verify_full_flow_semantic_execution_admission(
        semantic_execution_admission_dir,
        semantic_matrix_dir,
        deployment_binding_dir,
        logical_route_dir,
        scenario_path,
        container_plan_dir,
        public_task_set_path,
        artifact_binding_path,
        n1_oracle_package_dir,
    )
    _require(
        admission_report["status"] == "VERIFIED_BLOCKED"
        and admission_report["flowmesh_submission_authorized"] is False,
        "semantic execution admission is not fail-closed",
    )
    scenario = load_simulator_scenario(scenario_path)
    semantic_plan = _strict_json(
        semantic_matrix_dir / SEMANTIC_PLAN_NAME,
        "semantic matrix plan",
    )
    semantic_trials = _strict_jsonl(
        semantic_matrix_dir / SEMANTIC_TRIALS_NAME,
        "semantic matrix trials",
    )
    order_indices = [row.get("order_index") for row in semantic_trials]
    _require(
        order_indices == list(range(len(semantic_trials))),
        "semantic trial order is not contiguous and frozen",
    )
    trial_order = [
        _text(row.get("trial_key"), "semantic trial key")
        for row in semantic_trials
    ]
    repetitions = semantic_plan.get("matrix_dimensions", {}).get("repetitions")
    _require(
        repetitions == scenario.repetitions
        and len(semantic_trials) == scenario.planned_trial_count,
        "semantic matrix repetitions disagree with the verified scenario",
    )
    model_id, semantic_source_hashes = _semantic_model_binding(
        semantic_spec_paths,
        task_plane_dir,
    )

    admission = _strict_json(
        semantic_execution_admission_dir / ADMISSION_NAME,
        "semantic execution admission",
    )
    worker_pin = admission.get("worker_pin")
    _require(
        isinstance(worker_pin, dict) and worker_pin.get("kind") == "worker_alias",
        "semantic admission worker pin changed",
    )
    worker_alias = _identifier(worker_pin.get("value"), "worker alias")
    runtime_gaps = _strict_json(
        semantic_execution_admission_dir / ADMISSION_GAPS_NAME,
        "semantic execution runtime gaps",
    )
    _require(
        runtime_gaps.get("status") == "BLOCKING_RUNTIME_ADAPTERS_ENUMERATED"
        and runtime_gaps.get("all_required_adapters_implemented") is False,
        "route runtime gap classification was weakened",
    )
    required_adapters = runtime_gaps.get("required_adapters")
    _require(
        isinstance(required_adapters, list) and bool(required_adapters),
        "semantic admission contains no explicit runtime adapter gaps",
    )
    adapter_ids = sorted(
        _identifier(row.get("adapter_id"), "runtime adapter id")
        for row in required_adapters
        if isinstance(row, dict)
    )
    _require(
        len(adapter_ids) == len(required_adapters)
        and len(adapter_ids) == len(set(adapter_ids)),
        "runtime adapter gap identifiers are invalid",
    )

    policy = _optional_policy_binding(
        policy_assignment_dir,
        logical_route_dir=logical_route_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    oed = _optional_oed_binding(
        oed_selection_dir,
        logical_route_dir=logical_route_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )

    source_bindings = {
        "semantic_matrix": {
            "plan_sha256": semantic_report["plan_sha256"],
            "source_binding_sha256": semantic_report["source_binding_sha256"],
            "checksums_sha256": _checksum_commitment(
                semantic_matrix_dir,
                SEMANTIC_CHECKSUMS_NAME,
                "semantic matrix",
            ),
        },
        "artifact_binding_package": {
            "binding_set_id": artifact_report["binding_set_id"],
            "artifact_bindings_file_sha256": _file_sha256(
                artifact_binding_package_dir / ARTIFACT_BINDINGS_NAME,
                "artifact bindings",
            ),
            "provenance_file_sha256": _file_sha256(
                artifact_binding_package_dir / ARTIFACT_PROVENANCE_NAME,
                "artifact binding provenance",
            ),
            "checksums_sha256": _checksum_commitment(
                artifact_binding_package_dir,
                ARTIFACT_CHECKSUMS_NAME,
                "artifact binding package",
            ),
        },
        "task_plane": {
            "task_plane_id": task_report["task_plane_id"],
            "manifest_file_sha256": _file_sha256(
                task_plane_dir / TASK_PLANE_MANIFEST,
                "task-plane manifest",
            ),
            "checksums_sha256": _checksum_commitment(
                task_plane_dir,
                TASK_PLANE_CHECKSUMS_NAME,
                "task plane",
            ),
            "semantic_spec_source_sha256": semantic_source_hashes,
        },
        "n3_raw_cold_package": {
            "manifest_file_sha256": _file_sha256(
                n3_package_dir / N3_MANIFEST_NAME,
                "N3 package manifest",
            ),
            "checksums_sha256": _checksum_commitment(
                n3_package_dir,
                N3_CHECKSUMS_NAME,
                "N3 package",
            ),
        },
        "n4_derived_package": {
            "manifest_file_sha256": _file_sha256(
                n4_package_dir / N4_MANIFEST_NAME,
                "N4 package manifest",
            ),
            "checksums_sha256": _checksum_commitment(
                n4_package_dir,
                N4_CHECKSUMS_NAME,
                "N4 package",
            ),
        },
        "deployment_binding": {
            "deployment_id": deployment_report["deployment_id"],
            "backend": deployment_report["backend"],
            "binding_sha256": deployment_report["binding_sha256"],
            "binding_file_sha256": _file_sha256(
                deployment_binding_dir / DEPLOYMENT_BINDING_NAME,
                "deployment binding",
            ),
            "checksums_sha256": _checksum_commitment(
                deployment_binding_dir,
                DEPLOYMENT_CHECKSUMS_NAME,
                "deployment binding",
            ),
        },
        "n1_oracle_preselection_commitment": {
            "commitment_id": commitment_report["commitment_id"],
            "commitment_sha256": commitment_report["commitment_sha256"],
            "commitment_file_sha256": _file_sha256(
                oracle_commitment_dir / ORACLE_COMMITMENT_NAME,
                "N1 oracle commitment",
            ),
            "checksums_sha256": _checksum_commitment(
                oracle_commitment_dir,
                ORACLE_COMMITMENT_CHECKSUMS_NAME,
                "N1 oracle commitment",
            ),
        },
        "n1_private_oracle": {
            "content_included": False,
            "package_manifest_sha256": _file_sha256(
                n1_oracle_package_dir / "n1-oracle-package.json",
                "N1 oracle package manifest",
            ),
            "package_checksums_sha256": _checksum_commitment(
                n1_oracle_package_dir,
                "SHA256SUMS",
                "N1 oracle package",
            ),
            "public_task_set_sha256": oracle_report["public_task_set_sha256"],
        },
        "service_bootstrap": {
            "bootstrap_id": bootstrap_report["bootstrap_id"],
            "bootstrap_file_sha256": _file_sha256(
                service_bootstrap_dir / BOOTSTRAP_NAME,
                "service bootstrap",
            ),
            "launchers_file_sha256": _file_sha256(
                service_bootstrap_dir / BOOTSTRAP_LAUNCHERS_NAME,
                "service bootstrap launchers",
            ),
            "checksums_sha256": _checksum_commitment(
                service_bootstrap_dir,
                BOOTSTRAP_CHECKSUMS_NAME,
                "service bootstrap",
            ),
        },
        "local_compose_overlay": {
            "overlay_id": overlay_report["overlay_id"],
            "manifest_file_sha256": _file_sha256(
                compose_overlay_dir / OVERLAY_MANIFEST_NAME,
                "Compose overlay manifest",
            ),
            "stage_gate_file_sha256": _file_sha256(
                compose_overlay_dir / OVERLAY_GATE_NAME,
                "Compose overlay stage gate",
            ),
            "checksums_sha256": _checksum_commitment(
                compose_overlay_dir,
                OVERLAY_CHECKSUMS_NAME,
                "Compose overlay",
            ),
        },
        "semantic_execution_admission": {
            "admission_id": admission_report["admission_id"],
            "admission_sha256": admission_report["admission_sha256"],
            "runtime_gaps_file_sha256": _file_sha256(
                semantic_execution_admission_dir / ADMISSION_GAPS_NAME,
                "semantic execution runtime gaps",
            ),
            "checksums_sha256": _checksum_commitment(
                semantic_execution_admission_dir,
                ADMISSION_CHECKSUMS_NAME,
                "semantic execution admission",
            ),
        },
        "policy_assignment": policy,
        "oed_prospective_selection": oed,
    }
    execution_contract = {
        "worker_pin": {"kind": "worker_alias", "value": worker_alias},
        "semantic_model_id": model_id,
        "scenario_seed": scenario.seed,
        "repetitions": repetitions,
        "trial_count": len(trial_order),
        "trial_order": trial_order,
        "trial_order_sha256": _sha256(_canonical_bytes(trial_order)),
        "trial_order_source": "verified-semantic-matrix-order-index",
        "model_source": "verified-task-plane-semantic-specs",
    }
    document: dict[str, Any] = {
        "schema_version": EXPERIMENT_FREEZE_SCHEMA_VERSION,
        "status": "BLOCKED_MISSING_ROUTE_RUNTIME_ADAPTERS",
        "freeze_id": freeze_id,
        "scenario_id": semantic_report["scenario_id"],
        "execution_contract": execution_contract,
        "source_bindings": source_bindings,
        "source_bindings_sha256": _sha256(_canonical_bytes(source_bindings)),
        "preselection_integrity": {
            "oracle_content_hash_committed": True,
            "private_oracle_opening_verified": True,
            "label_values_included": False,
            "independent_timestamp_attested": False,
            "external_approval_attested": False,
        },
        "blocking_runtime_adapter_ids": adapter_ids,
        "blocking_runtime_adapter_count": len(adapter_ids),
        "route_runtime_adapters_implemented": False,
        "compose_operator_gate_satisfied": overlay_report[
            "n4_gate_satisfied"
        ],
        "flowmesh_submission_authorized": False,
        "current_blocker_requires_upcloud": False,
        "endpoint_values_included": False,
        "deployment_endpoints_bound_by_hash_only": True,
        "private_oracle_content_included": False,
        "credential_values_included": False,
        "services_started": False,
        "workflow_submitted": False,
        "semantic_execution_performed": False,
        "performance_measured": False,
        "cost_measured": False,
        "scientific_claim_made": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    document["freeze_sha256"] = _sha256(_canonical_bytes(document))
    _assert_public_safe(document)
    return {EXPERIMENT_FREEZE_NAME: _json_bytes(document)}


def _paths(
    *,
    semantic_matrix_dir: str | Path,
    artifact_binding_package_dir: str | Path,
    deployment_binding_dir: str | Path,
    oracle_commitment_dir: str | Path,
    n1_oracle_package_dir: str | Path,
    service_bootstrap_dir: str | Path,
    compose_overlay_dir: str | Path,
    semantic_execution_admission_dir: str | Path,
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    task_plane_dir: str | Path,
    n3_package_dir: str | Path,
    n4_package_dir: str | Path,
    public_task_set_path: str | Path,
    artifact_binding_path: str | Path,
) -> dict[str, Path]:
    return {
        "semantic_matrix_dir": Path(semantic_matrix_dir).resolve(),
        "artifact_binding_package_dir": Path(
            artifact_binding_package_dir
        ).resolve(),
        "deployment_binding_dir": Path(deployment_binding_dir).resolve(),
        "oracle_commitment_dir": Path(oracle_commitment_dir).resolve(),
        "n1_oracle_package_dir": Path(n1_oracle_package_dir).resolve(),
        "service_bootstrap_dir": Path(service_bootstrap_dir).resolve(),
        "compose_overlay_dir": Path(compose_overlay_dir).resolve(),
        "semantic_execution_admission_dir": Path(
            semantic_execution_admission_dir
        ).resolve(),
        "logical_route_dir": Path(logical_route_dir).resolve(),
        "scenario_path": Path(scenario_path).resolve(),
        "container_plan_dir": Path(container_plan_dir).resolve(),
        "task_plane_dir": Path(task_plane_dir).resolve(),
        "n3_package_dir": Path(n3_package_dir).resolve(),
        "n4_package_dir": Path(n4_package_dir).resolve(),
        "public_task_set_path": Path(public_task_set_path).resolve(),
        "artifact_binding_path": Path(artifact_binding_path).resolve(),
    }


def freeze_full_flow_offline_experiment(
    *,
    freeze_id: str,
    semantic_matrix_dir: str | Path,
    artifact_binding_package_dir: str | Path,
    deployment_binding_dir: str | Path,
    oracle_commitment_dir: str | Path,
    n1_oracle_package_dir: str | Path,
    service_bootstrap_dir: str | Path,
    compose_overlay_dir: str | Path,
    semantic_execution_admission_dir: str | Path,
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    task_plane_dir: str | Path,
    n3_package_dir: str | Path,
    n4_package_dir: str | Path,
    public_task_set_path: str | Path,
    artifact_binding_path: str | Path,
    semantic_spec_paths: Sequence[str | Path],
    output_dir: str | Path,
    policy_assignment_dir: str | Path | None = None,
    oed_selection_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Create one deterministic, public, fail-closed experiment freeze."""

    source_paths = _paths(
        semantic_matrix_dir=semantic_matrix_dir,
        artifact_binding_package_dir=artifact_binding_package_dir,
        deployment_binding_dir=deployment_binding_dir,
        oracle_commitment_dir=oracle_commitment_dir,
        n1_oracle_package_dir=n1_oracle_package_dir,
        service_bootstrap_dir=service_bootstrap_dir,
        compose_overlay_dir=compose_overlay_dir,
        semantic_execution_admission_dir=semantic_execution_admission_dir,
        logical_route_dir=logical_route_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
        task_plane_dir=task_plane_dir,
        n3_package_dir=n3_package_dir,
        n4_package_dir=n4_package_dir,
        public_task_set_path=public_task_set_path,
        artifact_binding_path=artifact_binding_path,
    )
    documents = _documents(
        freeze_id=freeze_id,
        semantic_spec_paths=semantic_spec_paths,
        policy_assignment_dir=policy_assignment_dir,
        oed_selection_dir=oed_selection_dir,
        **source_paths,
    )
    documents[CHECKSUMS_NAME] = b"".join(
        f"{_sha256(documents[name])}  {name}\n".encode("utf-8")
        for name in sorted(_CONTENT_FILES)
    )
    target = Path(output_dir).resolve()
    _require(not target.exists(), f"experiment freeze already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    parent = Path(tempfile.mkdtemp(prefix=".full-flow-freeze-", dir=target.parent))
    stage = parent / "freeze"
    try:
        stage.mkdir()
        for name, payload in documents.items():
            (stage / name).write_bytes(payload)
        verified = verify_full_flow_offline_experiment(
            freeze_dir=stage,
            semantic_spec_paths=semantic_spec_paths,
            policy_assignment_dir=policy_assignment_dir,
            oed_selection_dir=oed_selection_dir,
            **source_paths,
        )
        os.replace(stage, target)
    finally:
        shutil.rmtree(parent, ignore_errors=True)
    return {**verified, "output_dir": str(target)}


def verify_full_flow_offline_experiment(
    *,
    freeze_dir: str | Path,
    semantic_matrix_dir: str | Path,
    artifact_binding_package_dir: str | Path,
    deployment_binding_dir: str | Path,
    oracle_commitment_dir: str | Path,
    n1_oracle_package_dir: str | Path,
    service_bootstrap_dir: str | Path,
    compose_overlay_dir: str | Path,
    semantic_execution_admission_dir: str | Path,
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    task_plane_dir: str | Path,
    n3_package_dir: str | Path,
    n4_package_dir: str | Path,
    public_task_set_path: str | Path,
    artifact_binding_path: str | Path,
    semantic_spec_paths: Sequence[str | Path],
    policy_assignment_dir: str | Path | None = None,
    oed_selection_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Verify the public freeze and recompile it from every source package."""

    root = Path(freeze_dir).resolve()
    _require(root.is_dir(), "experiment freeze directory does not exist")
    entries = list(root.iterdir())
    _require(
        all(path.is_file() and not path.is_symlink() for path in entries),
        "experiment freeze must contain regular files only",
    )
    _require({path.name for path in entries} == _ALL_FILES, "freeze file set changed")
    document = _strict_json(root / EXPERIMENT_FREEZE_NAME, "experiment freeze")
    recorded = document.get("freeze_sha256")
    unsigned = dict(document)
    unsigned.pop("freeze_sha256", None)
    _require(
        isinstance(recorded, str)
        and recorded == _sha256(_canonical_bytes(unsigned)),
        "experiment freeze digest failed",
    )
    expected_checksum = (
        f"{_file_sha256(root / EXPERIMENT_FREEZE_NAME, 'experiment freeze')}  "
        f"{EXPERIMENT_FREEZE_NAME}\n"
    ).encode("utf-8")
    _require(
        (root / CHECKSUMS_NAME).read_bytes() == expected_checksum,
        "experiment freeze checksum failed",
    )
    _require(
        document.get("schema_version") == EXPERIMENT_FREEZE_SCHEMA_VERSION
        and document.get("status") == "BLOCKED_MISSING_ROUTE_RUNTIME_ADAPTERS",
        "experiment freeze schema or blocked status changed",
    )
    source_paths = _paths(
        semantic_matrix_dir=semantic_matrix_dir,
        artifact_binding_package_dir=artifact_binding_package_dir,
        deployment_binding_dir=deployment_binding_dir,
        oracle_commitment_dir=oracle_commitment_dir,
        n1_oracle_package_dir=n1_oracle_package_dir,
        service_bootstrap_dir=service_bootstrap_dir,
        compose_overlay_dir=compose_overlay_dir,
        semantic_execution_admission_dir=semantic_execution_admission_dir,
        logical_route_dir=logical_route_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
        task_plane_dir=task_plane_dir,
        n3_package_dir=n3_package_dir,
        n4_package_dir=n4_package_dir,
        public_task_set_path=public_task_set_path,
        artifact_binding_path=artifact_binding_path,
    )
    expected = _documents(
        freeze_id=document.get("freeze_id"),
        semantic_spec_paths=semantic_spec_paths,
        policy_assignment_dir=policy_assignment_dir,
        oed_selection_dir=oed_selection_dir,
        **source_paths,
    )
    _require(
        (root / EXPERIMENT_FREEZE_NAME).read_bytes()
        == expected[EXPERIMENT_FREEZE_NAME],
        "experiment freeze does not match deterministic source recompilation",
    )
    return {
        "status": "VERIFIED_BLOCKED",
        "freeze_id": document["freeze_id"],
        "freeze_sha256": document["freeze_sha256"],
        "worker_alias": document["execution_contract"]["worker_pin"]["value"],
        "semantic_model_id": document["execution_contract"][
            "semantic_model_id"
        ],
        "scenario_seed": document["execution_contract"]["scenario_seed"],
        "repetitions": document["execution_contract"]["repetitions"],
        "trial_count": document["execution_contract"]["trial_count"],
        "blocking_runtime_adapter_count": document[
            "blocking_runtime_adapter_count"
        ],
        "flowmesh_submission_authorized": False,
        "source_binding_checked": True,
        "endpoint_values_included": False,
        "private_oracle_content_included": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "CHECKSUMS_NAME",
    "EXPERIMENT_FREEZE_NAME",
    "EXPERIMENT_FREEZE_SCHEMA_VERSION",
    "FullFlowExperimentFreezeError",
    "freeze_full_flow_offline_experiment",
    "verify_full_flow_offline_experiment",
]
