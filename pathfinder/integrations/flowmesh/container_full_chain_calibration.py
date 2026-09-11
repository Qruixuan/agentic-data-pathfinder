"""Read-only fast/slow application-shaping audit for full-chain artifacts.

The FlowMesh full-chain smoke preserves a configured application-level
shaping target and a container-reported operation duration.  Those facts are
useful to check that an intentionally fast route and an intentionally slow
route were exercised as frozen.  They are *not* a physical-network
calibration: the target comes from the frozen link adapter and the duration
also contains container/application overhead.

This module therefore fits no parameters, changes no scenario, starts no
service, and never derives a bytes-per-second figure.  It reads two already
verified full-chain plan/run pairs and publishes a separate immutable audit.
"""

from __future__ import annotations

import json
import math
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping, Sequence

from .container_dag import (
    FlowMeshContainerDagError,
    _checksums,
    _json_bytes,
    _jsonl_bytes,
    _sha256_bytes,
    _write_documents,
)
from .container_full_chain import (
    _read_plan,
    _read_run,
    verify_flowmesh_container_full_physical_chain_plan,
    verify_flowmesh_container_full_physical_chain_run,
)


FLOWMESH_CONTAINER_FULL_CHAIN_CALIBRATION_REPORT_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-full-chain-calibration-report/v1alpha1"
)
FLOWMESH_CONTAINER_FULL_CHAIN_CALIBRATION_DIAGNOSTIC_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-full-chain-calibration-diagnostic/v1alpha1"
)
FLOWMESH_CONTAINER_FULL_CHAIN_CALIBRATION_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-full-chain-calibration-audit/v1alpha1"
)

_OUTPUT_FILES = {
    "full-chain-calibration-report.json",
    "full-chain-calibration-diagnostics.jsonl",
    "full-chain-calibration-manifest.json",
}


class FullChainCalibrationAuditError(FlowMeshContainerDagError):
    """Raised when two full-chain artifacts cannot support this audit."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise FullChainCalibrationAuditError(message)


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullChainCalibrationAuditError(
            f"cannot read valid {label}: {path.name}"
        ) from exc
    _require(isinstance(value, dict), f"{label} must be an object")
    return value


def _read_jsonl(path: Path, label: str) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FullChainCalibrationAuditError(
            f"cannot read {label}: {path.name}"
        ) from exc
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise FullChainCalibrationAuditError(
                f"invalid {label} JSON at line {line_number}"
            ) from exc
        _require(isinstance(row, dict), f"{label} row must be an object")
        rows.append(row)
    _require(bool(rows), f"{label} is empty")
    return rows


def _number(value: Any, field: str, *, positive: bool = False) -> float:
    _require(
        isinstance(value, (int, float)) and not isinstance(value, bool),
        f"{field} must be numeric",
    )
    number = float(value)
    _require(math.isfinite(number), f"{field} must be finite")
    _require(
        number > 0.0 if positive else number >= 0.0,
        f"{field} must be {'positive' if positive else 'non-negative'}",
    )
    return number


def _text(value: Any, field: str) -> str:
    _require(isinstance(value, str) and bool(value.strip()), f"{field} is missing")
    return value.strip()


def _fingerprint_directory(root: Path) -> dict[str, str]:
    """Return a path-free digest map for a closed source artifact directory."""

    _require(root.is_dir(), "calibration input directory does not exist")
    entries = sorted(path for path in root.iterdir() if path.is_file())
    _require(entries, "calibration input directory has no files")
    return {
        path.name: sha256(path.read_bytes()).hexdigest()
        for path in entries
    }


def _path_is_within(candidate: Path, parent: Path) -> bool:
    return candidate == parent or parent in candidate.parents


def _require_disjoint_paths(
    *,
    output_dir: Path,
    inputs: Mapping[str, Path],
) -> None:
    all_inputs = list(inputs.values())
    _require(
        len(set(all_inputs)) == len(all_inputs),
        "fast and slow calibration inputs must be four distinct directories",
    )
    for label, source in inputs.items():
        _require(
            not _path_is_within(output_dir, source)
            and not _path_is_within(source, output_dir),
            "calibration output must be outside every frozen input artifact "
            f"({label})",
        )


def _source_document_digests(
    *,
    plan_dir: Path,
    run_dir: Path,
) -> dict[str, str]:
    names = {
        "plan_file_sha256": plan_dir / "flowmesh-container-full-chain-plan.json",
        "workflow_template_file_sha256": (
            plan_dir / "flowmesh-container-full-chain-workflow-template.json"
        ),
        "run_file_sha256": run_dir / "flowmesh-container-full-chain-run.json",
        "submission_file_sha256": (
            run_dir / "flowmesh-container-full-chain-submission.json"
        ),
        "task_results_file_sha256": (
            run_dir / "flowmesh-container-full-chain-task-results.jsonl"
        ),
    }
    return {
        label: _sha256_bytes(path.read_bytes())
        for label, path in names.items()
    }


def _network_target_ms(operation: Mapping[str, Any]) -> float:
    """Recompute the configured application-shaping target from the plan.

    This intentionally uses only the frozen link adapter.  It is a
    configuration check, not a network-rate estimate from observed timing.
    """

    _require(
        operation.get("operation_kind") == "network_transfer",
        "network target requested for a non-network operation",
    )
    link = operation.get("link_adapter")
    _require(isinstance(link, Mapping), "network operation has no link adapter")
    logical_bytes = operation.get("logical_bytes")
    _require(
        type(logical_bytes) is int and logical_bytes >= 0,
        "network operation logical_bytes is invalid",
    )
    bandwidth = _number(
        link.get("bandwidth_bytes_per_second"),
        "link bandwidth_bytes_per_second",
        positive=True,
    )
    rtt_ms = _number(link.get("round_trip_time_ms"), "link round_trip_time_ms")
    return (float(logical_bytes) / bandwidth * 1000.0) + rtt_ms


def _operations_by_id(
    operations: Sequence[Mapping[str, Any]],
    *,
    label: str,
) -> dict[str, Mapping[str, Any]]:
    indexed: dict[str, Mapping[str, Any]] = {}
    for operation in operations:
        operation_id = _text(operation.get("operation_id"), f"{label} operation_id")
        _require(
            operation_id not in indexed,
            f"{label} full-chain operation IDs are not unique",
        )
        indexed[operation_id] = operation
    return indexed


def _validate_pair_shape(
    fast_plan: Mapping[str, Any],
    slow_plan: Mapping[str, Any],
) -> dict[str, Any]:
    """Ensure D0/D4 differ in path configuration, not workload shape."""

    fast_operations = fast_plan.get("operations")
    slow_operations = slow_plan.get("operations")
    _require(isinstance(fast_operations, list), "fast plan operations are missing")
    _require(isinstance(slow_operations, list), "slow plan operations are missing")
    _require(
        fast_plan.get("container_operations_source_sha256")
        == slow_plan.get("container_operations_source_sha256"),
        "fast and slow plans were not frozen from the same container operation ledger",
    )
    _require(
        fast_plan.get("worker_alias") == slow_plan.get("worker_alias"),
        "fast and slow plans pin different worker aliases",
    )
    fast_by_id = _operations_by_id(fast_operations, label="fast")
    slow_by_id = _operations_by_id(slow_operations, label="slow")
    _require(
        set(fast_by_id) == set(slow_by_id),
        "fast and slow plans do not contain the same operation IDs",
    )
    for operation_id in sorted(fast_by_id):
        fast = fast_by_id[operation_id]
        slow = slow_by_id[operation_id]
        for field in (
            "operation_kind",
            "object_id",
            "representation_id",
            "logical_bytes",
        ):
            _require(
                fast.get(field) == slow.get(field),
                "fast and slow plans differ in frozen workload shape at "
                f"operation {operation_id}: {field}",
            )
    return {
        "same_container_operations_source": True,
        "same_worker_alias": True,
        "same_operation_ids": True,
        "same_operation_kind_object_representation_and_bytes": True,
        "operation_count": len(fast_by_id),
    }


def _network_diagnostics(
    *,
    role: str,
    plan: Mapping[str, Any],
    run_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    operations = plan.get("operations")
    _require(isinstance(operations, list), f"{role} plan operations are missing")
    result_by_key = {
        _text(row.get("operation_key"), f"{role} result operation_key"): row
        for row in run_rows
    }
    diagnostics: list[dict[str, Any]] = []
    for operation in operations:
        if operation.get("operation_kind") != "network_transfer":
            continue
        operation_key = _text(operation.get("operation_key"), "network operation_key")
        result = result_by_key.get(operation_key)
        _require(result is not None, "run lacks a frozen network operation result")
        target = _network_target_ms(operation)
        recorded_target = _number(
            result.get("application_shaping_target_ms"),
            "recorded application_shaping_target_ms",
        )
        _require(
            math.isclose(recorded_target, target, rel_tol=0.0, abs_tol=1e-6),
            "recorded shaping target does not match its frozen link adapter "
            f"for {operation_key}",
        )
        service = _number(result.get("service_time_ms"), "container service_time_ms")
        _require(
            service + 1e-6 >= target,
            "container service duration is below the configured shaping target "
            f"for {operation_key}",
        )
        physical_bytes = result.get("physical_bytes")
        _require(
            physical_bytes == operation.get("logical_bytes"),
            "network operation did not report exact physical bytes",
        )
        link = operation.get("link_adapter")
        assert isinstance(link, Mapping)
        residual = service - target
        diagnostics.append(
            {
                "schema_version": (
                    FLOWMESH_CONTAINER_FULL_CHAIN_CALIBRATION_DIAGNOSTIC_SCHEMA_VERSION
                ),
                "path_role": role,
                "plan_sha256": plan["plan_sha256"],
                "trial_key": plan["trial_key"],
                "operation_key": operation_key,
                "operation_id": operation["operation_id"],
                "operation_kind": "network_transfer",
                "source_node_id": link.get("source_node_id"),
                "destination_node_id": link.get("destination_node_id"),
                "link_id": link.get("link_id"),
                "object_id": operation.get("object_id"),
                "representation_id": operation.get("representation_id"),
                "logical_bytes": operation["logical_bytes"],
                "physical_bytes": physical_bytes,
                "configured_bandwidth_bytes_per_second": link.get(
                    "bandwidth_bytes_per_second"
                ),
                "configured_round_trip_time_ms": link.get("round_trip_time_ms"),
                "recomputed_application_shaping_target_ms": round(target, 9),
                "recorded_application_shaping_target_ms": round(recorded_target, 9),
                "container_reported_service_time_ms": round(service, 9),
                "service_minus_configured_target_ms": round(residual, 9),
                "target_conformance": "service-at-or-above-configured-target",
                "network_throughput_derived": False,
                "physical_network_rate_inferred": False,
                "credentials_recorded": False,
            }
        )
    _require(diagnostics, f"{role} full chain has no network transfer")
    return diagnostics


def _primary_network_row(
    rows: Sequence[Mapping[str, Any]], *, role: str
) -> dict[str, Any]:
    """Choose one primary transfer solely by uniquely largest logical bytes."""

    largest = max(int(row["logical_bytes"]) for row in rows)
    candidates = [row for row in rows if int(row["logical_bytes"]) == largest]
    _require(
        len(candidates) == 1,
        f"{role} path has no unique largest network transfer for fast/slow comparison",
    )
    return dict(candidates[0])


def _public_path_summary(
    *,
    plan: Mapping[str, Any],
    summary: Mapping[str, Any],
    source_digests: Mapping[str, str],
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    primary = _primary_network_row(rows, role="path")
    worker = summary.get("selected_worker")
    _require(
        isinstance(worker, Mapping), "full-chain summary selected_worker is missing"
    )
    telemetry_provenance = summary.get("telemetry_provenance")
    _require(
        isinstance(telemetry_provenance, Mapping),
        "full-chain summary telemetry provenance is missing",
    )
    return {
        "source_plan_schema_version": plan["schema_version"],
        "source_run_schema_version": summary["schema_version"],
        "source_telemetry_provenance_version": _text(
            telemetry_provenance.get("version"), "telemetry provenance version"
        ),
        "trial_key": plan["trial_key"],
        "smoke_id": plan["smoke_id"],
        "plan_sha256": plan["plan_sha256"],
        "container_operations_source_sha256": plan[
            "container_operations_source_sha256"
        ],
        "worker_alias": plan["worker_alias"],
        "worker_id": _text(worker.get("worker_id"), "selected worker_id"),
        "physical_operation_count": plan["physical_operation_count"],
        "network_transfer_count": len(rows),
        "primary_network_transfer": {
            "selection_rule": "unique-largest-logical-bytes",
            "operation_key": primary["operation_key"],
            "logical_bytes": primary["logical_bytes"],
            "configured_application_shaping_target_ms": primary[
                "recomputed_application_shaping_target_ms"
            ],
            "container_reported_service_time_ms": primary[
                "container_reported_service_time_ms"
            ],
            "service_minus_configured_target_ms": primary[
                "service_minus_configured_target_ms"
            ],
        },
        "source_document_sha256": dict(sorted(source_digests.items())),
    }


def _require_source_claim_boundaries(
    *,
    plan: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> None:
    # The public full-chain verifiers above decide which historical schemas
    # are readable.  In particular, existing D0/D4 v1alpha1 run artifacts
    # remain auditable after a current writer moves to a later schema.  This
    # layer must never reinterpret a schema merely because it is current.
    _text(plan.get("schema_version"), "full-chain plan schema_version")
    _text(summary.get("schema_version"), "full-chain run schema_version")
    for field in (
        "llm_called",
        "semantic_task_quality_evaluated",
        "eligible_for_scientific_claims",
        "credentials_recorded",
    ):
        _require(
            plan.get(field) is False,
            f"full-chain plan violates calibration boundary: {field}",
        )
        _require(
            summary.get(field) is False,
            f"full-chain run violates calibration boundary: {field}",
        )
    telemetry = summary.get("telemetry")
    _require(isinstance(telemetry, Mapping), "full-chain run telemetry is missing")
    _require(
        telemetry.get("network_throughput_derived") is False,
        "source run claims derived network throughput",
    )
    _require(
        telemetry.get("queue_time_measured") is False,
        "source run claims measured queue time",
    )
    _require(
        telemetry.get("service_time_ms_sum_is_end_to_end_latency") is False,
        "source run claims end-to-end latency",
    )


def _verify_output(root: Path) -> dict[str, Any]:
    expected = _OUTPUT_FILES | {"SHA256SUMS"}
    actual = {path.name for path in root.iterdir() if path.is_file()}
    _require(actual == expected, "full-chain calibration output file set changed")
    checksums: dict[str, str] = {}
    try:
        checksum_lines = (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FullChainCalibrationAuditError(
            "full-chain calibration checksum file is unreadable"
        ) from exc
    for line in checksum_lines:
        digest, separator, name = line.partition("  ")
        _require(
            separator == "  " and name in _OUTPUT_FILES,
            "invalid full-chain calibration checksum row",
        )
        _require(name not in checksums, "duplicate full-chain calibration checksum")
        _require(
            _sha256_bytes((root / name).read_bytes()) == digest,
            f"full-chain calibration checksum mismatch: {name}",
        )
        checksums[name] = digest
    _require(
        set(checksums) == _OUTPUT_FILES,
        "full-chain calibration checksums are incomplete",
    )
    report = _read_json(
        root / "full-chain-calibration-report.json", "calibration report"
    )
    manifest = _read_json(
        root / "full-chain-calibration-manifest.json", "calibration manifest"
    )
    diagnostics = _read_jsonl(
        root / "full-chain-calibration-diagnostics.jsonl", "calibration diagnostics"
    )
    _require(
        report.get("schema_version")
        == FLOWMESH_CONTAINER_FULL_CHAIN_CALIBRATION_REPORT_SCHEMA_VERSION,
        "unsupported full-chain calibration report schema",
    )
    _require(report.get("status") == "COMPLETE", "full-chain calibration incomplete")
    _require(
        manifest.get("schema_version")
        == FLOWMESH_CONTAINER_FULL_CHAIN_CALIBRATION_MANIFEST_SCHEMA_VERSION,
        "unsupported full-chain calibration manifest schema",
    )
    _require(manifest.get("status") == "COMPLETE", "calibration manifest incomplete")
    _require(
        manifest.get("output_sha256")
        == {
            name: digest
            for name, digest in checksums.items()
            if name != "full-chain-calibration-manifest.json"
        },
        "calibration manifest output digests disagree",
    )
    _require(
        report.get("diagnostics_sha256")
        == checksums["full-chain-calibration-diagnostics.jsonl"],
        "calibration report diagnostics digest disagrees",
    )
    _require(
        report.get("network_transfer_observation_count") == len(diagnostics),
        "calibration report diagnostics count disagrees",
    )
    _require(
        all(
            row.get("schema_version")
            == FLOWMESH_CONTAINER_FULL_CHAIN_CALIBRATION_DIAGNOSTIC_SCHEMA_VERSION
            and row.get("network_throughput_derived") is False
            and row.get("physical_network_rate_inferred") is False
            for row in diagnostics
        ),
        "calibration diagnostic schema or measurement boundary changed",
    )
    _require(
        report.get("audit_class")
        == "posthoc-configured-application-shaping-conformance-only",
        "full-chain calibration audit class changed",
    )
    calibration = report.get("calibration")
    _require(isinstance(calibration, Mapping), "calibration boundary is missing")
    _require(
        calibration.get("parameters_fitted") == 0,
        "full-chain audit must not fit parameters",
    )
    for field in (
        "scenario_parameters_modified",
        "rate_card_modified",
        "physical_network_rate_inferred",
        "network_throughput_derived",
    ):
        _require(
            calibration.get(field) is False,
            f"full-chain calibration must not claim {field}",
        )
    _require(report.get("posthoc") is True, "full-chain calibration must be post-hoc")
    for field in (
        "source_artifacts_modified",
        "llm_called",
        "services_started",
        "workflow_submitted",
        "credentials_recorded",
        "eligible_for_scientific_claims",
    ):
        _require(
            report.get(field) is False,
            f"full-chain calibration boundary changed: {field}",
        )
    _require(
        manifest.get("audit_class") == report.get("audit_class"),
        "calibration manifest audit class disagrees",
    )
    _require(
        manifest.get("parameters_fitted") == 0,
        "calibration manifest must record zero fitted parameters",
    )
    for field in (
        "external_services_called",
        "credentials_recorded",
        "eligible_for_scientific_claims",
    ):
        _require(
            manifest.get(field) is False,
            f"calibration manifest boundary changed: {field}",
        )
    _require(
        report.get("eligible_for_scientific_claims") is False,
        "post-hoc full-chain calibration audit cannot be claim eligible",
    )
    return manifest


def audit_flowmesh_container_full_chain_calibration(
    *,
    fast_plan_dir: str | Path,
    fast_run_dir: str | Path,
    slow_plan_dir: str | Path,
    slow_run_dir: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Audit an existing configured-fast/configured-slow full-chain pair.

    Inputs are checked offline and remain untouched.  ``fast`` and ``slow``
    refer only to the frozen target for the uniquely largest transfer.  The
    audit refuses to fit a rate, infer physical throughput, or turn this
    post-hoc evidence into a scientific performance claim.
    """

    paths = {
        "fast_plan": Path(fast_plan_dir).resolve(),
        "fast_run": Path(fast_run_dir).resolve(),
        "slow_plan": Path(slow_plan_dir).resolve(),
        "slow_run": Path(slow_run_dir).resolve(),
    }
    target = Path(output_dir).resolve()
    _require_disjoint_paths(output_dir=target, inputs=paths)
    _require(not target.exists(), f"calibration output already exists: {target}")

    fingerprint_before = {
        label: _fingerprint_directory(path)
        for label, path in sorted(paths.items())
    }

    # Public verifiers supply the checksum, pinning, coverage, and telemetry
    # guarantees.  The private readers only expose their already-verified raw
    # documents for the descriptive residual audit below.
    verify_flowmesh_container_full_physical_chain_plan(paths["fast_plan"])
    verify_flowmesh_container_full_physical_chain_run(
        paths["fast_run"], plan_dir=paths["fast_plan"]
    )
    verify_flowmesh_container_full_physical_chain_plan(paths["slow_plan"])
    verify_flowmesh_container_full_physical_chain_run(
        paths["slow_run"], plan_dir=paths["slow_plan"]
    )
    fast_plan = _read_plan(paths["fast_plan"])
    fast_summary, fast_results, _ = _read_run(paths["fast_run"])
    slow_plan = _read_plan(paths["slow_plan"])
    slow_summary, slow_results, _ = _read_run(paths["slow_run"])
    _require_source_claim_boundaries(plan=fast_plan, summary=fast_summary)
    _require_source_claim_boundaries(plan=slow_plan, summary=slow_summary)

    pair_shape = _validate_pair_shape(fast_plan, slow_plan)
    fast_rows = _network_diagnostics(
        role="configured-fast", plan=fast_plan, run_rows=fast_results
    )
    slow_rows = _network_diagnostics(
        role="configured-slow", plan=slow_plan, run_rows=slow_results
    )
    fast_primary = _primary_network_row(fast_rows, role="configured-fast")
    slow_primary = _primary_network_row(slow_rows, role="configured-slow")
    _require(
        float(fast_primary["recomputed_application_shaping_target_ms"])
        < float(slow_primary["recomputed_application_shaping_target_ms"]),
        "configured-fast primary transfer is not faster than configured-slow "
        "primary transfer",
    )
    _require(
        _text(
            fast_summary.get("selected_worker", {}).get("worker_id"),
            "fast selected worker_id",
        )
        == _text(
            slow_summary.get("selected_worker", {}).get("worker_id"),
            "slow selected worker_id",
        ),
        "fast and slow runs used different worker IDs",
    )

    fingerprint_after_analysis = {
        label: _fingerprint_directory(path)
        for label, path in sorted(paths.items())
    }
    _require(
        fingerprint_before == fingerprint_after_analysis,
        "a supposedly read-only full-chain audit changed a source artifact",
    )
    fast_digests = _source_document_digests(
        plan_dir=paths["fast_plan"], run_dir=paths["fast_run"]
    )
    slow_digests = _source_document_digests(
        plan_dir=paths["slow_plan"], run_dir=paths["slow_run"]
    )
    diagnostics = [*fast_rows, *slow_rows]
    diagnostics_bytes = _jsonl_bytes(diagnostics)
    report: dict[str, Any] = {
        "schema_version": (
            FLOWMESH_CONTAINER_FULL_CHAIN_CALIBRATION_REPORT_SCHEMA_VERSION
        ),
        "status": "COMPLETE",
        "audit_class": "posthoc-configured-application-shaping-conformance-only",
        "fast_and_slow_meaning": (
            "fast and slow refer only to the configured target of the "
            "unique-largest transfer; they do not describe an independently "
            "measured physical network"
        ),
        "pair_shape": pair_shape,
        "paths": {
            "configured_fast": _public_path_summary(
                plan=fast_plan,
                summary=fast_summary,
                source_digests=fast_digests,
                rows=fast_rows,
            ),
            "configured_slow": _public_path_summary(
                plan=slow_plan,
                summary=slow_summary,
                source_digests=slow_digests,
                rows=slow_rows,
            ),
        },
        "primary_target_delta_ms": round(
            float(slow_primary["recomputed_application_shaping_target_ms"])
            - float(fast_primary["recomputed_application_shaping_target_ms"]),
            9,
        ),
        "network_transfer_observation_count": len(diagnostics),
        "diagnostics_sha256": _sha256_bytes(diagnostics_bytes),
        "source_schema_compatibility": {
            "configured_fast_run_is_legacy_v1alpha1": (
                str(fast_summary.get("schema_version", "")).endswith("/v1alpha1")
            ),
            "configured_slow_run_is_legacy_v1alpha1": (
                str(slow_summary.get("schema_version", "")).endswith("/v1alpha1")
            ),
            "rule": (
                "source schemas are accepted only through the existing "
                "offline full-chain verifier; this audit never rewrites "
                "or upgrades a historical artifact"
            ),
        },
        "calibration": {
            "parameters_fitted": 0,
            "scenario_parameters_modified": False,
            "rate_card_modified": False,
            "physical_network_rate_inferred": False,
            "network_throughput_derived": False,
            "purpose": (
                "check frozen application-shaping targets against the "
                "container-reported service durations only"
            ),
        },
        "source_artifact_fingerprint_before": fingerprint_before,
        "source_artifact_fingerprint_after_analysis": fingerprint_after_analysis,
        "source_artifacts_modified": False,
        "measurement_boundaries": [
            "The target is recomputed from the frozen application link adapter; "
            "it is a configured intervention, not an independent network "
            "measurement.",
            "No bytes-per-second value is computed from service duration or "
            "payload bytes, so this audit does not estimate network throughput.",
            "Service duration is preserved from the original container result "
            "and is used only as a descriptive target residual; it is not "
            "promoted to end-to-end latency.",
            "No queue, FlowMesh scheduling, cross-container clock, semantic "
            "quality, LLM, or monetary-cost measurement is included.",
            "The source telemetry provenance version is recorded per path; "
            "this audit does not rely on a claimed HTTP-transport decomposition.",
            "One completed run per path is post-hoc conformance evidence, not "
            "an uncertainty estimate or a scientific performance result.",
        ],
        "posthoc": True,
        "llm_called": False,
        "services_started": False,
        "workflow_submitted": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    report_bytes = _json_bytes(report)
    manifest = {
        "schema_version": (
            FLOWMESH_CONTAINER_FULL_CHAIN_CALIBRATION_MANIFEST_SCHEMA_VERSION
        ),
        "status": "COMPLETE",
        "audit_class": report["audit_class"],
        "fast_plan_sha256": fast_plan["plan_sha256"],
        "slow_plan_sha256": slow_plan["plan_sha256"],
        "network_transfer_observation_count": len(diagnostics),
        "parameters_fitted": 0,
        "external_services_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
        "output_sha256": {
            "full-chain-calibration-diagnostics.jsonl": _sha256_bytes(
                diagnostics_bytes
            ),
            "full-chain-calibration-report.json": _sha256_bytes(report_bytes),
        },
    }
    documents = {
        "full-chain-calibration-report.json": report_bytes,
        "full-chain-calibration-diagnostics.jsonl": diagnostics_bytes,
        "full-chain-calibration-manifest.json": _json_bytes(manifest),
    }
    documents["SHA256SUMS"] = _checksums(documents)
    _write_documents(target, documents)
    _verify_output(target)
    return {
        "status": "COMPLETE",
        "audit_class": report["audit_class"],
        "fast_plan_sha256": fast_plan["plan_sha256"],
        "slow_plan_sha256": slow_plan["plan_sha256"],
        "network_transfer_observation_count": len(diagnostics),
        "primary_target_delta_ms": report["primary_target_delta_ms"],
        "parameters_fitted": 0,
        "network_throughput_derived": False,
        "source_artifacts_modified": False,
        "output_dir": str(target),
        "eligible_for_scientific_claims": False,
    }


def verify_flowmesh_container_full_chain_calibration(
    output_dir: str | Path,
) -> dict[str, Any]:
    """Verify one immutable, source-independent calibration-audit package."""

    root = Path(output_dir).resolve()
    _require(root.is_dir(), f"full-chain calibration output does not exist: {root}")
    manifest = _verify_output(root)
    return {
        "status": "VERIFIED",
        "audit_class": manifest["audit_class"],
        "fast_plan_sha256": manifest["fast_plan_sha256"],
        "slow_plan_sha256": manifest["slow_plan_sha256"],
        "network_transfer_observation_count": manifest[
            "network_transfer_observation_count"
        ],
        "parameters_fitted": 0,
        "network_throughput_derived": False,
        "checked_files": len(_OUTPUT_FILES),
        "eligible_for_scientific_claims": False,
    }
