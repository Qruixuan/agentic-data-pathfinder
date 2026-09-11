"""Freeze the conservative execution profile for a 4x8 container matrix.

The eight-node simulator currently supports an infrastructure-conformance
experiment, not semantic-quality or physical-money claims.  A matrix freeze
already binds topology, operations, cache scopes, and runtime-integrity
requirements, while the fast/slow audit establishes only that configured
application-shaping routes were exercised as configured.  This module joins
those two immutable inputs into one deliberately conservative profile.

It is not a scheduler and never contacts a container, FlowMesh, Docker, an
LLM, or an external endpoint.  In particular, it does not turn the
fast/slow audit into a physical-network calibration.  The profile admits
only a serial, plan-bound trial-wrapper execution until cross-lane
parallelism has its own evidence and contract.
"""

from __future__ import annotations

import json
import re
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping

from ...simulator.container_contract import CONTAINER_NODE_RESULT_SCHEMA_VERSION
from .container_dag import (
    TELEMETRY_PROVENANCE_VERSION,
    FlowMeshContainerDagError,
    _canonical_bytes,
    _checksums,
    _document_sha256,
    _json_bytes,
    _require,
    _sha256_bytes,
    _text,
    _write_documents,
)
from .container_full_chain_calibration import (
    FLOWMESH_CONTAINER_FULL_CHAIN_CALIBRATION_MANIFEST_SCHEMA_VERSION,
    FLOWMESH_CONTAINER_FULL_CHAIN_CALIBRATION_REPORT_SCHEMA_VERSION,
    verify_flowmesh_container_full_chain_calibration,
)
from .container_matrix import (
    FLOWMESH_CONTAINER_MATRIX_PLAN_SCHEMA_VERSION,
    verify_flowmesh_container_matrix_plan,
)


FLOWMESH_CONTAINER_FORMAL_PROFILE_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-formal-execution-profile/v1alpha1"
)

_OUTPUT_FILES = {"flowmesh-container-formal-execution-profile.json"}
_PROFILE_ID = re.compile(r"[a-z0-9][a-z0-9._-]*")
_EXPECTED_AUDIT_CLASS = (
    "posthoc-configured-application-shaping-conformance-only"
)


class FlowMeshContainerFormalProfileError(FlowMeshContainerDagError):
    """Raised when immutable 4x8 inputs cannot form a safe execution profile."""


def _profile_require(condition: bool, message: str) -> None:
    if not condition:
        raise FlowMeshContainerFormalProfileError(message)


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FlowMeshContainerFormalProfileError(
            f"cannot read valid {label}: {path.name}"
        ) from exc
    _profile_require(isinstance(value, dict), f"{label} must be an object")
    return value


def _sha256_path(path: Path, label: str) -> str:
    try:
        return _sha256_bytes(path.read_bytes())
    except OSError as exc:
        raise FlowMeshContainerFormalProfileError(f"cannot read {label}") from exc


def _profile_id(value: Any) -> str:
    identifier = _text(value, "execution_profile_id")
    _profile_require(
        _PROFILE_ID.fullmatch(identifier) is not None,
        "execution_profile_id contains unsupported characters",
    )
    return identifier


def _read_matrix_input(matrix_plan_dir: str | Path) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    root = Path(matrix_plan_dir).resolve()
    try:
        verified = verify_flowmesh_container_matrix_plan(root)
    except Exception as exc:
        raise FlowMeshContainerFormalProfileError(
            "matrix plan does not verify: " + str(exc)
        ) from exc
    matrix = _read_json(
        root / "flowmesh-container-matrix-plan.json", "matrix plan"
    )
    _profile_require(
        matrix.get("schema_version") == FLOWMESH_CONTAINER_MATRIX_PLAN_SCHEMA_VERSION,
        "formal execution profile requires a v2 runtime-integrity matrix plan",
    )
    return root, matrix, dict(verified)


def _read_calibration_input(
    calibration_audit_dir: str | Path,
) -> tuple[Path, dict[str, Any], dict[str, Any], dict[str, Any]]:
    root = Path(calibration_audit_dir).resolve()
    try:
        verified = verify_flowmesh_container_full_chain_calibration(root)
    except Exception as exc:
        raise FlowMeshContainerFormalProfileError(
            "fast/slow calibration audit does not verify: " + str(exc)
        ) from exc
    report = _read_json(
        root / "full-chain-calibration-report.json", "fast/slow calibration report"
    )
    manifest = _read_json(
        root / "full-chain-calibration-manifest.json", "fast/slow calibration manifest"
    )
    _profile_require(
        report.get("schema_version")
        == FLOWMESH_CONTAINER_FULL_CHAIN_CALIBRATION_REPORT_SCHEMA_VERSION,
        "unsupported fast/slow calibration report schema",
    )
    _profile_require(
        manifest.get("schema_version")
        == FLOWMESH_CONTAINER_FULL_CHAIN_CALIBRATION_MANIFEST_SCHEMA_VERSION,
        "unsupported fast/slow calibration manifest schema",
    )
    _profile_require(
        report.get("audit_class") == _EXPECTED_AUDIT_CLASS
        and manifest.get("audit_class") == _EXPECTED_AUDIT_CLASS,
        "calibration audit has an unsupported evidence class",
    )
    calibration = report.get("calibration")
    _profile_require(
        isinstance(calibration, Mapping), "calibration report is missing its boundary"
    )
    _profile_require(
        calibration.get("parameters_fitted") == 0
        and calibration.get("scenario_parameters_modified") is False
        and calibration.get("rate_card_modified") is False
        and calibration.get("physical_network_rate_inferred") is False
        and calibration.get("network_throughput_derived") is False,
        "fast/slow calibration audit no longer has descriptive-only semantics",
    )
    _profile_require(
        report.get("eligible_for_scientific_claims") is False
        and manifest.get("eligible_for_scientific_claims") is False,
        "post-hoc calibration audit unexpectedly permits scientific claims",
    )
    return root, report, manifest, dict(verified)


def _runtime_integrity_contract(matrix: Mapping[str, Any]) -> dict[str, Any]:
    contract = matrix.get("runtime_integrity")
    _profile_require(
        isinstance(contract, Mapping), "v2 matrix is missing runtime-integrity contract"
    )
    expected = {
        "container_operation_result_schema_version": (
            CONTAINER_NODE_RESULT_SCHEMA_VERSION
        ),
        "telemetry_provenance_version": TELEMETRY_PROVENANCE_VERSION,
        "runtime_epoch_binding_required": True,
        "pre_submit_health_required": True,
        "post_run_health_required": True,
        "all_referenced_runtime_nodes_must_be_healthy": True,
        "container_restart_during_trial_refuses_canonicalization": True,
        "legacy_v1_artifacts_not_runtime_integrity_eligible": True,
    }
    _profile_require(
        dict(contract) == expected,
        "matrix runtime-integrity contract is not the required v2 contract",
    )
    return expected


def _admission_contract(matrix_root: Path, matrix: Mapping[str, Any]) -> dict[str, Any]:
    admission = _read_json(
        matrix_root / "flowmesh-container-matrix-admission.json", "matrix admission"
    )
    _profile_require(
        admission.get("trial_admission") == matrix.get("trial_admission"),
        "matrix admission contract does not match its plan",
    )
    _profile_require(
        admission.get("same_cache_lane_serial_execution_required") is True,
        "formal execution profile requires same-cache-lane serialization",
    )
    _profile_require(
        admission.get("cross_lane_parallelism_not_yet_claimed") is True,
        "matrix unexpectedly asserts cross-lane parallelism",
    )
    _profile_require(
        admission.get("flowmesh_dispatch_unit") == "plan-bound-trial-wrapper",
        "formal execution profile requires plan-bound trial wrappers",
    )
    return admission


def _verify_profile_output(root: Path) -> dict[str, Any]:
    expected_files = _OUTPUT_FILES | {"SHA256SUMS"}
    _profile_require(root.is_dir(), "formal execution profile directory does not exist")
    actual_files = {path.name for path in root.iterdir() if path.is_file()}
    _profile_require(
        actual_files == expected_files,
        "formal execution profile file set changed",
    )
    try:
        checksum_lines = (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FlowMeshContainerFormalProfileError(
            "formal execution profile checksum file is unreadable"
        ) from exc
    checksums: dict[str, str] = {}
    for line in checksum_lines:
        digest, separator, name = line.partition("  ")
        _profile_require(
            separator == "  " and name in _OUTPUT_FILES,
            "invalid formal execution profile checksum row",
        )
        _profile_require(name not in checksums, "duplicate formal execution profile checksum")
        _profile_require(
            _sha256_path(root / name, f"formal execution profile {name}") == digest,
            f"formal execution profile checksum mismatch: {name}",
        )
        checksums[name] = digest
    _profile_require(
        set(checksums) == _OUTPUT_FILES,
        "formal execution profile checksums are incomplete",
    )
    profile = _read_json(
        root / "flowmesh-container-formal-execution-profile.json",
        "formal execution profile",
    )
    _profile_require(
        profile.get("schema_version")
        == FLOWMESH_CONTAINER_FORMAL_PROFILE_SCHEMA_VERSION,
        "unsupported formal execution profile schema",
    )
    _profile_require(profile.get("status") == "FROZEN", "formal execution profile is not frozen")
    _profile_require(
        profile.get("profile_sha256") == _document_sha256(profile, "profile_sha256"),
        "formal execution profile digest mismatch",
    )
    _profile_id(profile.get("execution_profile_id"))
    _profile_require(
        profile.get("primary_trial_wrapper_max_concurrency") == 1,
        "formal profile primary concurrency changed",
    )
    _profile_require(
        profile.get("same_cache_lane_serial_execution_required") is True
        and profile.get("cross_lane_parallelism_authorized") is False,
        "formal profile cache-lane concurrency semantics changed",
    )
    _profile_require(
        profile.get("runtime_integrity")
        == {
            "container_operation_result_schema_version": CONTAINER_NODE_RESULT_SCHEMA_VERSION,
            "telemetry_provenance_version": TELEMETRY_PROVENANCE_VERSION,
            "runtime_epoch_binding_required": True,
            "pre_submit_health_required": True,
            "post_run_health_required": True,
            "all_referenced_runtime_nodes_must_be_healthy": True,
            "container_restart_during_trial_refuses_canonicalization": True,
            "legacy_v1_artifacts_not_runtime_integrity_eligible": True,
        },
        "formal profile runtime-integrity contract changed",
    )
    measurement_scope = profile.get("measurement_scope")
    _profile_require(isinstance(measurement_scope, Mapping), "formal profile measurement scope is missing")
    _profile_require(
        measurement_scope
        == {
            "evidence_class": "infrastructure-conformance-only",
            "semantic_task_quality_evaluated": False,
            "configured_cost_evaluated": False,
            "physical_monetary_cost_evaluated": False,
            "scientific_claims_eligible": False,
        },
        "formal profile measurement scope changed",
    )
    _profile_require(
        profile.get("credentials_recorded") is False
        and profile.get("services_started") is False
        and profile.get("workflow_submitted") is False
        and profile.get("eligible_for_scientific_claims") is False,
        "formal profile safety boundary changed",
    )
    return profile


def freeze_flowmesh_container_formal_execution_profile(
    *,
    matrix_plan_dir: str | Path,
    calibration_audit_dir: str | Path,
    execution_profile_id: str,
    output_dir: str | Path,
    primary_trial_wrapper_max_concurrency: int = 1,
) -> dict[str, Any]:
    """Bind current v2 matrix and descriptive fast/slow audit into a profile.

    The first formal profile intentionally admits one trial wrapper at a time.
    That is a conservative execution policy, not a claim that the system's
    eventual optimal concurrency is one.
    """

    identifier = _profile_id(execution_profile_id)
    _profile_require(
        type(primary_trial_wrapper_max_concurrency) is int
        and primary_trial_wrapper_max_concurrency == 1,
        "the initial formal profile requires primary_trial_wrapper_max_concurrency=1",
    )
    target = Path(output_dir).resolve()
    _profile_require(
        not target.exists(), "formal execution profile output already exists"
    )
    matrix_root, matrix, matrix_verified = _read_matrix_input(matrix_plan_dir)
    _profile_require(
        matrix.get("execution_profile_id") == identifier,
        "matrix execution_profile_id does not match the requested profile",
    )
    _profile_require(
        matrix.get("flowmesh_execution_boundary", {}).get("coordinator_required")
        is True,
        "matrix does not require a plan-bound coordinator",
    )
    runtime_integrity = _runtime_integrity_contract(matrix)
    admission = _admission_contract(matrix_root, matrix)
    audit_root, audit_report, audit_manifest, audit_verified = _read_calibration_input(
        calibration_audit_dir
    )

    matrix_file = matrix_root / "flowmesh-container-matrix-plan.json"
    audit_report_file = audit_root / "full-chain-calibration-report.json"
    audit_manifest_file = audit_root / "full-chain-calibration-manifest.json"
    profile: dict[str, Any] = {
        "schema_version": FLOWMESH_CONTAINER_FORMAL_PROFILE_SCHEMA_VERSION,
        "status": "FROZEN",
        "execution_profile_id": identifier,
        "matrix": {
            "matrix_id": matrix.get("matrix_id"),
            "matrix_plan_sha256": matrix.get("plan_sha256"),
            "matrix_plan_file_sha256": _sha256_path(matrix_file, "matrix plan"),
            "matrix_schema_version": matrix.get("schema_version"),
            "source_git_revision": matrix.get("source_git_revision"),
            "worker_alias": matrix.get("worker_alias"),
            "node_api_url_mapping_sha256": _sha256_bytes(
                _canonical_bytes(matrix.get("node_api_urls"))
            ),
            "matrix_verification_status": matrix_verified.get("status"),
        },
        "fast_slow_path_audit": {
            "audit_class": audit_report.get("audit_class"),
            "report_sha256": _sha256_path(audit_report_file, "calibration report"),
            "manifest_sha256": _sha256_path(
                audit_manifest_file, "calibration manifest"
            ),
            "fast_plan_sha256": audit_manifest.get("fast_plan_sha256"),
            "slow_plan_sha256": audit_manifest.get("slow_plan_sha256"),
            "network_transfer_observation_count": audit_manifest.get(
                "network_transfer_observation_count"
            ),
            "parameters_fitted": 0,
            "physical_network_rate_inferred": False,
            "network_throughput_derived": False,
            "verification_status": audit_verified.get("status"),
            "role": "configured-application-shaping-conformance-only",
        },
        "primary_trial_wrapper_max_concurrency": 1,
        "same_cache_lane_serial_execution_required": True,
        "cross_lane_parallelism_authorized": False,
        "trial_dispatch_unit": admission.get("flowmesh_dispatch_unit"),
        "runtime_integrity": runtime_integrity,
        "measurement_scope": {
            "evidence_class": "infrastructure-conformance-only",
            "semantic_task_quality_evaluated": False,
            "configured_cost_evaluated": False,
            "physical_monetary_cost_evaluated": False,
            "scientific_claims_eligible": False,
        },
        "limitations": [
            "The fast/slow audit verifies configured application shaping only; it does not calibrate a physical network link.",
            "The initial profile deliberately serializes trial wrappers. Cross-lane parallelism requires separately frozen scheduling evidence.",
            "The profile is not an evaluation of semantic task quality, configured cost, or physical monetary cost.",
            "Runtime epochs detect container-instance changes but do not authenticate images or records.",
        ],
        "services_started": False,
        "workflow_submitted": False,
        "llm_called": False,
        "credentials_recorded": False,
        "eligible_for_formal_infrastructure_execution": True,
        "eligible_for_scientific_claims": False,
    }
    profile["profile_sha256"] = _document_sha256(profile, "profile_sha256")
    documents = {
        "flowmesh-container-formal-execution-profile.json": _json_bytes(profile),
    }
    documents["SHA256SUMS"] = _checksums(documents)
    _write_documents(target, documents)
    verified = verify_flowmesh_container_formal_execution_profile(target)
    return {
        "status": "FROZEN_FORMAL_INFRASTRUCTURE_PROFILE",
        "output_dir": str(target),
        "execution_profile_id": identifier,
        "profile_sha256": profile["profile_sha256"],
        "matrix_plan_sha256": profile["matrix"]["matrix_plan_sha256"],
        "primary_trial_wrapper_max_concurrency": 1,
        "cross_lane_parallelism_authorized": False,
        "parameters_fitted": 0,
        "eligible_for_formal_infrastructure_execution": True,
        "eligible_for_scientific_claims": False,
        "verification_status": verified["status"],
    }


def verify_flowmesh_container_formal_execution_profile(
    output_dir: str | Path,
) -> dict[str, Any]:
    """Verify a frozen formal execution profile without contacting services."""

    root = Path(output_dir).resolve()
    profile = _verify_profile_output(root)
    return {
        "status": "VERIFIED",
        "execution_profile_id": profile["execution_profile_id"],
        "profile_sha256": profile["profile_sha256"],
        "matrix_plan_sha256": profile["matrix"]["matrix_plan_sha256"],
        "primary_trial_wrapper_max_concurrency": profile[
            "primary_trial_wrapper_max_concurrency"
        ],
        "cross_lane_parallelism_authorized": False,
        "parameters_fitted": 0,
        "eligible_for_formal_infrastructure_execution": True,
        "eligible_for_scientific_claims": False,
    }
