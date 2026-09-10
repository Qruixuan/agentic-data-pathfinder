"""Post-hoc calibration from a bound container-emulation execution.

Only fixture file reads identify a simulator performance parameter without
circularity. Network duration is application-shaped from the input plan,
while control, index, CPU, and GPU operations are deliberate no-ops. This
module fits effective storage latency/throughput only and retains everything
else as validation diagnostics.
"""

from __future__ import annotations

import json
import math
import os
import shutil
import tempfile
from collections import defaultdict
from hashlib import sha256
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable, Mapping

from .admission import TRIAL_ADMISSION_ALGORITHM, TRIAL_LATENCY_ORIGIN
from .config import load_simulator_scenario
from .container_execution import verify_container_execution
from .portable import verify_portable_execution_plan
from .runner import verify_simulator_run


CONTAINER_CALIBRATION_REPORT_SCHEMA_VERSION = (
    "pathfinder.container-backend-calibration-report/v1alpha1"
)
CONTAINER_CALIBRATION_DIAGNOSTIC_SCHEMA_VERSION = (
    "pathfinder.container-backend-calibration-operation/v1alpha1"
)
CONTAINER_CALIBRATION_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.container-backend-calibration-run/v1alpha1"
)


class ContainerCalibrationError(ValueError):
    """Raised when container observations cannot support the declared fit."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ContainerCalibrationError(message)


def _sha256_bytes(value: bytes) -> str:
    return sha256(value).hexdigest()


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _jsonl_bytes(values: Iterable[Mapping[str, Any]]) -> bytes:
    return "".join(
        json.dumps(
            value,
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
        for value in values
    ).encode("utf-8")


def _read_json(path: Path, name: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ContainerCalibrationError(f"cannot read valid {name}: {path}") from exc
    _require(isinstance(value, dict), f"{name} must be an object")
    return value


def _read_jsonl(path: Path, name: str) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise ContainerCalibrationError(f"cannot read {name}: {path}") from exc
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ContainerCalibrationError(
                f"invalid {name} at line {line_number}"
            ) from exc
        _require(isinstance(row, dict), f"{name} row must be an object")
        rows.append(row)
    _require(bool(rows), f"{name} is empty")
    return rows


def _p95(values: list[float]) -> float:
    _require(bool(values), "cannot calculate p95 of an empty sample")
    ordered = sorted(values)
    index = max(0, math.ceil(0.95 * len(ordered)) - 1)
    return ordered[index]


def _fit_storage_model(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    """Fit non-negative ``latency + bytes / throughput`` to size medians."""

    samples: list[tuple[int, float]] = []
    for row in rows:
        size = row.get("logical_bytes")
        service = row.get("service_time_ms")
        _require(type(size) is int and size > 0, "storage sample size is invalid")
        _require(
            type(service) in (int, float)
            and math.isfinite(float(service))
            and float(service) > 0.0,
            "storage sample service time is invalid",
        )
        samples.append((size, float(service)))

    by_size: dict[int, list[float]] = defaultdict(list)
    for size, service in samples:
        by_size[size].append(service)
    points = sorted((size, median(values)) for size, values in by_size.items())
    if len(samples) < 4:
        return {"status": "NOT_FITTED", "reason": "fewer_than_four_samples"}
    if len(points) < 2:
        return {"status": "NOT_FITTED", "reason": "no_size_variation"}
    if points[-1][0] / points[0][0] < 4.0:
        return {
            "status": "NOT_FITTED",
            "reason": "byte_size_span_below_fourfold",
        }

    x_center = mean(size for size, _ in points)
    y_center = mean(service for _, service in points)
    denominator = sum((size - x_center) ** 2 for size, _ in points)
    slope = sum(
        (size - x_center) * (service - y_center)
        for size, service in points
    ) / denominator
    intercept = y_center - slope * x_center
    if slope <= 0.0:
        return {
            "status": "NOT_FITTED",
            "reason": "non_positive_size_service_slope",
        }
    if intercept < 0.0:
        intercept = 0.0
        slope = sum(size * service for size, service in points) / sum(
            size * size for size, _ in points
        )
    _require(slope > 0.0, "storage fit has non-positive slope")

    predictions = [intercept + slope * size for size, _ in samples]
    residuals = [
        abs(service - predicted)
        for (_, service), predicted in zip(samples, predictions)
    ]
    relative = [
        residual / predicted if predicted > 0.0 else 0.0
        for residual, predicted in zip(residuals, predictions)
    ]
    return {
        "status": "FITTED",
        "sample_count": len(samples),
        "distinct_size_count": len(points),
        "minimum_size_bytes": points[0][0],
        "maximum_size_bytes": points[-1][0],
        "base_latency_ms": intercept,
        "throughput_bytes_per_second": 1000.0 / slope,
        "observed_p95_relative_residual": _p95(relative),
        "in_sample_mean_absolute_error_ms": mean(residuals),
        "in_sample_p95_absolute_error_ms": _p95(residuals),
        "size_median_points": [
            {"logical_bytes": size, "median_service_time_ms": service}
            for size, service in points
        ],
    }


def _resource_rows(scenario: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    resources: dict[str, dict[str, Any]] = {}
    nodes = scenario.get("nodes")
    _require(isinstance(nodes, list), "scenario.nodes must be an array")
    for node in nodes:
        _require(isinstance(node, dict), "scenario node must be an object")
        for resource in node.get("resources", []):
            _require(isinstance(resource, dict), "scenario resource must be an object")
            resource_id = resource.get("resource_id")
            _require(
                isinstance(resource_id, str) and resource_id not in resources,
                "scenario resource_id is missing or duplicated",
            )
            resources[resource_id] = resource
    return resources


def _operation_key(row: Mapping[str, Any]) -> str:
    key = row.get("operation_key")
    if isinstance(key, str) and key:
        return key
    trial = row.get("trial_key")
    operation = row.get("operation_id")
    _require(
        isinstance(trial, str) and isinstance(operation, str),
        "operation identity is incomplete",
    )
    return f"{trial}|{operation}"


def _verify_output(root: Path) -> dict[str, Any]:
    expected = {
        "calibrated_scenario.json",
        "calibration_report.json",
        "operation_diagnostics.jsonl",
        "calibration_manifest.json",
    }
    actual = {path.name for path in root.iterdir() if path.is_file()}
    _require(actual == expected | {"SHA256SUMS"}, "calibration output file set changed")
    checksums: dict[str, str] = {}
    for line in (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
        digest, separator, name = line.partition("  ")
        _require(separator == "  " and name in expected, "malformed SHA256SUMS")
        _require(name not in checksums, f"duplicate calibration checksum: {name}")
        _require(
            _sha256_bytes((root / name).read_bytes()) == digest,
            f"calibration checksum mismatch: {name}",
        )
        checksums[name] = digest
    _require(set(checksums) == expected, "calibration checksums are incomplete")
    manifest = _read_json(root / "calibration_manifest.json", "calibration manifest")
    _require(
        manifest.get("schema_version")
        == CONTAINER_CALIBRATION_MANIFEST_SCHEMA_VERSION,
        "unsupported calibration manifest schema_version",
    )
    _require(manifest.get("status") == "COMPLETE", "calibration is incomplete")
    _require(
        manifest.get("output_sha256")
        == {
            name: digest
            for name, digest in checksums.items()
            if name != "calibration_manifest.json"
        },
        "calibration manifest digests disagree",
    )
    scenario = load_simulator_scenario(root / "calibrated_scenario.json")
    _require(
        scenario.scenario_id == manifest.get("output_scenario_id"),
        "calibrated scenario identity changed",
    )
    report = _read_json(root / "calibration_report.json", "calibration report")
    _require(report.get("status") == "COMPLETE", "calibration report is incomplete")
    _require(
        report.get("eligible_for_scientific_claims") is False,
        "post-hoc calibration cannot be claim eligible",
    )
    return manifest


def calibrate_container_backend(
    scenario_path: str | Path,
    portable_plan_dir: str | Path,
    reference_run_dir: str | Path,
    container_run_dir: str | Path,
    *,
    output_scenario_id: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Fit identifiable fixture-storage parameters from one container run."""

    scenario_source = Path(scenario_path).resolve()
    scenario_model = load_simulator_scenario(scenario_source)
    scenario = _read_json(scenario_source, "scenario")
    portable_root = Path(portable_plan_dir).resolve()
    reference_root = Path(reference_run_dir).resolve()
    container_root = Path(container_run_dir).resolve()
    verify_portable_execution_plan(portable_root)
    verify_simulator_run(reference_root)
    verify_container_execution(container_root)

    portable_manifest = _read_json(
        portable_root / "portable_plan_manifest.json", "portable manifest"
    )
    reference_manifest = _read_json(
        reference_root / "run_manifest.json", "reference run manifest"
    )
    container_manifest = _read_json(
        container_root / "container_run_manifest.json", "container run manifest"
    )
    _require(
        portable_manifest.get("scenario_id") == scenario_model.scenario_id
        == reference_manifest.get("scenario_id")
        == container_manifest.get("scenario_id"),
        "scenario identity differs across calibration inputs",
    )
    _require(
        portable_manifest.get("scenario_sha256") == scenario_model.source_sha256
        == reference_manifest.get("scenario_sha256"),
        "scenario digest differs across calibration inputs",
    )
    _require(
        container_manifest.get("portable_plan_sha256")
        == portable_manifest.get("plan_sha256"),
        "container execution is not bound to the portable plan",
    )
    _require(
        container_manifest.get("status") == "COMPLETE_INFRASTRUCTURE_ONLY",
        "container calibration requires a complete infrastructure-only run",
    )
    _require(
        container_manifest.get("payload_mode")
        == "deterministic-size-preserving-fixture",
        "container calibration requires deterministic size-preserving fixtures",
    )
    new_id = output_scenario_id.strip()
    _require(bool(new_id), "output_scenario_id must be non-empty")
    _require(new_id != scenario_model.scenario_id, "output scenario_id must change")

    reference_events = _read_jsonl(reference_root / "events.jsonl", "reference events")
    container_events = _read_jsonl(
        container_root / "operation_results.jsonl", "container operations"
    )
    reference_by_key = {_operation_key(row): row for row in reference_events}
    container_by_key = {_operation_key(row): row for row in container_events}
    _require(
        len(reference_by_key) == len(reference_events)
        and len(container_by_key) == len(container_events),
        "duplicate operation key in calibration inputs",
    )
    _require(
        set(reference_by_key) == set(container_by_key),
        "reference and container operation sets differ",
    )

    storage_samples: dict[str, list[dict[str, Any]]] = defaultdict(list)
    diagnostics: list[dict[str, Any]] = []
    network_excess: list[float] = []
    for operation_key in sorted(reference_by_key):
        reference = reference_by_key[operation_key]
        candidate = container_by_key[operation_key]
        for field in (
            "trial_key",
            "operation_id",
            "operation_kind",
        ):
            _require(
                reference.get(field) == candidate.get(field),
                f"paired operation changed {field}: {operation_key}",
            )
        both_executed = (
            reference.get("executed") is True
            and candidate.get("executed") is True
        )
        if both_executed:
            _require(
                reference.get("logical_bytes") == candidate.get("logical_bytes"),
                f"paired operation changed logical_bytes: {operation_key}",
            )
        if both_executed and candidate.get("operation_kind") not in (
            "cache_lookup",
            "cache_insert",
            "barrier",
        ):
            for field in ("resource_id", "resource_kind"):
                _require(
                    reference.get(field) == candidate.get(field),
                    f"paired operation changed {field}: {operation_key}",
                )
        role = "validation-only"
        reason = (
            "cache_state_or_execution_path_differs_between_backends"
            if reference.get("executed") != candidate.get("executed")
            else "unbound_or_skipped_operation"
        )
        if candidate.get("executed") is True:
            kind = candidate.get("operation_kind")
            if kind in ("storage_read", "cache_read"):
                if (
                    candidate.get("resource_kind") == "storage"
                    and type(candidate.get("logical_bytes")) is int
                    and candidate["logical_bytes"] > 0
                    and candidate.get("physical_bytes")
                    == candidate.get("logical_bytes")
                ):
                    role = "direct-storage-fit-candidate"
                    reason = "fixture_file_read_identifies_effective_storage_service"
                    storage_samples[str(candidate["resource_id"])].append(candidate)
                else:
                    reason = "storage_operation_did_not_read_exact_fixture_bytes"
            elif kind == "network_transfer":
                reason = "application_shaped_from_input_plan_circular_for_fitting"
                target = candidate.get("application_shaping_target_ms")
                if type(target) in (int, float) and float(target) >= 0.0:
                    network_excess.append(
                        float(candidate["service_time_ms"]) - float(target)
                    )
            elif kind in ("control", "index_query", "compute"):
                reason = "container_operation_is_deliberate_noop_not_physical_work"
            elif kind in ("cache_lookup", "cache_insert", "barrier"):
                reason = "metadata_or_barrier_operation_does_not_identify_resource_rate"
            else:
                reason = "unsupported_direct_calibration_kind"
        diagnostics.append({
            "schema_version": CONTAINER_CALIBRATION_DIAGNOSTIC_SCHEMA_VERSION,
            "operation_key": operation_key,
            "trial_key": candidate.get("trial_key"),
            "operation_id": candidate.get("operation_id"),
            "operation_kind": candidate.get("operation_kind"),
            "resource_id": candidate.get("resource_id"),
            "logical_bytes": candidate.get("logical_bytes"),
            "reference_service_time_ms": reference.get("service_time_ms"),
            "container_service_time_ms": candidate.get("service_time_ms"),
            "reference_queue_time_ms": reference.get("queue_time_ms"),
            "container_queue_time_ms": candidate.get("queue_time_ms"),
            "calibration_role": role,
            "role_reason": reason,
        })

    resources = _resource_rows(scenario)
    fitted: list[dict[str, Any]] = []
    retained: list[dict[str, Any]] = []
    fitted_ids: set[str] = set()
    for resource_id, rows in sorted(storage_samples.items()):
        _require(resource_id in resources, f"unknown storage resource: {resource_id}")
        resource = resources[resource_id]
        _require(resource.get("kind") == "storage", f"{resource_id} is not storage")
        fit = _fit_storage_model(rows)
        if fit["status"] != "FITTED":
            retained.append({
                "resource_id": resource_id,
                "sample_count": len(rows),
                **fit,
            })
            continue
        previous = {
            "base_latency_ms": resource.get("base_latency_ms", 0.0),
            "throughput_bytes_per_second": resource.get(
                "throughput_bytes_per_second"
            ),
            "jitter_fraction": resource.get("jitter_fraction", 0.0),
        }
        previous_predictions = [
            float(previous["base_latency_ms"])
            + float(row["logical_bytes"])
            / float(previous["throughput_bytes_per_second"])
            * 1000.0
            for row in rows
        ]
        previous_errors = [
            abs(float(row["service_time_ms"]) - prediction)
            for row, prediction in zip(rows, previous_predictions)
        ]
        updated = {
            "base_latency_ms": fit["base_latency_ms"],
            "throughput_bytes_per_second": fit["throughput_bytes_per_second"],
        }
        resource.update(updated)
        fitted_ids.add(resource_id)
        fitted.append({
            "resource_id": resource_id,
            "measurement_scope": "cached-container-fixture-file-read",
            "previous_parameters": previous,
            "fitted_parameters": updated,
            "previous_model_mean_absolute_error_ms": mean(previous_errors),
            "previous_model_p95_absolute_error_ms": _p95(previous_errors),
            "jitter_fraction_retained_from_base_scenario": previous[
                "jitter_fraction"
            ],
            **fit,
        })

    _require(bool(fitted), "no storage resource met direct-fit identifiability rules")
    for row in diagnostics:
        if row["calibration_role"] == "direct-storage-fit-candidate":
            if row["resource_id"] in fitted_ids:
                row["calibration_role"] = "direct-storage-fit"
            else:
                row["calibration_role"] = "validation-only"
                row["role_reason"] = "insufficient_size_span_or_sample_count"

    scenario["scenario_id"] = new_id
    scenario["calibration_provenance"] = (
        "posthoc-container-fixture-storage-only;"
        f"source-scenario={scenario_model.scenario_id};"
        "network-shaping-and-noop-compute-excluded"
    )
    reference_queue = sum(float(row["queue_time_ms"]) for row in reference_events)
    container_queue = sum(float(row["queue_time_ms"]) for row in container_events)
    reference_records = _read_jsonl(
        reference_root / "canonical_records.jsonl", "reference records"
    )
    container_records = _read_jsonl(
        container_root / "infrastructure_records.jsonl", "container records"
    )
    admission_semantics_aligned = all(
        row.get("latency_origin") == TRIAL_LATENCY_ORIGIN
        and row.get("trial_admission_algorithm") == TRIAL_ADMISSION_ALGORITHM
        and type(row.get("trial_admission_slots")) is int
        and type(row.get("trial_admission_queue_ms")) in (int, float)
        for row in reference_records + container_records
    ) and {
        int(row["trial_admission_slots"])
        for row in reference_records + container_records
    } == {scenario_model.trial_admission_slots}
    reference_admission_queue = sum(
        float(row.get("trial_admission_queue_ms", 0.0))
        for row in reference_records
    )
    admission_queue = sum(
        float(
            row.get(
                "trial_admission_queue_ms",
                row.get("trial_dispatch_queue_ms", 0.0),
            )
        )
        for row in container_records
    )
    report = {
        "schema_version": CONTAINER_CALIBRATION_REPORT_SCHEMA_VERSION,
        "status": "COMPLETE",
        "calibration_class": "posthoc-container-fixture-storage-only",
        "base_scenario_id": scenario_model.scenario_id,
        "output_scenario_id": new_id,
        "paired_operation_count": len(diagnostics),
        "direct_fit_operation_count": sum(
            row["calibration_role"] == "direct-storage-fit" for row in diagnostics
        ),
        "fitted_storage_resource_count": len(fitted),
        "fitted_storage_resources": fitted,
        "storage_resources_retained_for_insufficient_identifiability": retained,
        "network_calibration": {
            "parameter_count_calibrated": 0,
            "observation_count": len(network_excess),
            "role": "validation-only-circular-application-shaping",
            "median_service_minus_shaping_target_ms": (
                median(network_excess) if network_excess else None
            ),
            "p95_absolute_service_minus_shaping_target_ms": (
                _p95([abs(value) for value in network_excess])
                if network_excess else None
            ),
        },
        "compute_control_index_calibration": {
            "parameter_count_calibrated": 0,
            "role": "excluded-deliberate-container-noops",
        },
        "queue_calibration": {
            "parameter_count_calibrated": 0,
            "reference_resource_queue_time_ms": reference_queue,
            "container_resource_queue_time_ms": container_queue,
            "reference_trial_admission_queue_time_ms": (
                reference_admission_queue
            ),
            "container_trial_admission_queue_time_ms": admission_queue,
            "shared_admission_semantics": admission_semantics_aligned,
            "role": (
                "comparison-only-shared-admission-contract"
                if admission_semantics_aligned
                else "diagnostic-only-admission-semantics-differ"
            ),
        },
        "in_sample_fit": True,
        "posthoc": True,
        "same_observations_are_not_independent_validation": True,
        "new_container_run_required_for_validation": True,
        "calibrates_representative_physical_hardware": False,
        "calibrates_local_container_fixture_io_only": True,
        "storage_jitter_calibrated": False,
        "resource_slots_calibrated": False,
        "rate_card_calibrated": False,
        "semantic_quality_evaluated": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    documents = {
        "calibrated_scenario.json": _json_bytes(scenario),
        "calibration_report.json": _json_bytes(report),
        "operation_diagnostics.jsonl": _jsonl_bytes(diagnostics),
    }
    manifest = {
        "schema_version": CONTAINER_CALIBRATION_MANIFEST_SCHEMA_VERSION,
        "status": "COMPLETE",
        "base_scenario_sha256": scenario_model.source_sha256,
        "portable_plan_sha256": portable_manifest["plan_sha256"],
        "reference_run_manifest_sha256": _sha256_bytes(
            (reference_root / "run_manifest.json").read_bytes()
        ),
        "container_run_manifest_sha256": _sha256_bytes(
            (container_root / "container_run_manifest.json").read_bytes()
        ),
        "output_scenario_id": new_id,
        "fitted_storage_resource_count": len(fitted),
        "external_services_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
        "output_sha256": {
            name: _sha256_bytes(content)
            for name, content in sorted(documents.items())
        },
    }
    documents["calibration_manifest.json"] = _json_bytes(manifest)
    documents["SHA256SUMS"] = "".join(
        f"{_sha256_bytes(content)}  {name}\n"
        for name, content in sorted(documents.items())
    ).encode("utf-8")

    target = Path(output_dir).resolve()
    _require(not target.exists(), f"calibration output already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging_parent = Path(tempfile.mkdtemp(prefix=".container-calibration-", dir=target.parent))
    staging = staging_parent / "output"
    try:
        staging.mkdir()
        for name, content in documents.items():
            (staging / name).write_bytes(content)
        _verify_output(staging)
        _require(not target.exists(), f"calibration output already exists: {target}")
        os.replace(staging, target)
    finally:
        shutil.rmtree(staging_parent, ignore_errors=True)
    return {
        "status": "COMPLETE",
        "calibration_class": report["calibration_class"],
        "output_scenario_id": new_id,
        "paired_operation_count": len(diagnostics),
        "direct_fit_operation_count": report["direct_fit_operation_count"],
        "fitted_storage_resource_count": len(fitted),
        "new_container_run_required_for_validation": True,
        "output_dir": str(target),
        "eligible_for_scientific_claims": False,
    }


def verify_container_backend_calibration(output_dir: str | Path) -> dict[str, Any]:
    """Verify a published container calibration without contacting services."""

    root = Path(output_dir).resolve()
    _require(root.is_dir(), f"calibration output does not exist: {root}")
    manifest = _verify_output(root)
    return {
        "status": "VERIFIED",
        "output_scenario_id": manifest["output_scenario_id"],
        "fitted_storage_resource_count": manifest[
            "fitted_storage_resource_count"
        ],
        "checked_files": 4,
        "eligible_for_scientific_claims": False,
    }
