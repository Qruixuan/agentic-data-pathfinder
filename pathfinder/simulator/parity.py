"""Descriptive parity evaluation across two portable-plan backends.

The evaluator deliberately refuses incomplete or differently identified trial
sets.  It computes paired diagnostics but cannot announce parity until a
separate threshold contract is preregistered.  Configured simulator tariffs
are excluded because they are not physical cost measurements.
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
from .portable import verify_portable_execution_plan


PARITY_PAIR_SCHEMA_VERSION = "pathfinder.backend-parity-pair/v1alpha1"
PARITY_REPORT_SCHEMA_VERSION = "pathfinder.backend-parity-report/v1alpha1"
PARITY_MANIFEST_SCHEMA_VERSION = "pathfinder.backend-parity-run/v1alpha1"
PARITY_COMPARISON_SCOPES = ("full", "infrastructure-only")

_IDENTITY_FIELDS = (
    "trial_key",
    "trial_id",
    "session_id",
    "order_index",
    "workload_id",
    "workload_class",
    "object_id",
    "task_type",
    "design_id",
    "repetition",
    "seed",
)
_BASE_SCALAR_METRICS = (
    "latency_ms",
    "logical_bytes",
    "physical_bytes",
    "network_bytes",
)
_ADMISSION_SCALAR_METRICS = (
    "trial_admission_queue_ms",
    "active_execution_latency_ms",
)
_MAP_METRICS = ("resource_service_ms", "resource_queue_ms")
_OUTPUT_FILES = {
    "parity_manifest.json",
    "parity_pairs.jsonl",
    "parity_report.json",
}


class BackendParityError(ValueError):
    """Raised when two backend outputs cannot be compared safely."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise BackendParityError(message)


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _invalid_number(value: str) -> None:
    raise BackendParityError(f"non-finite JSON number: {value}")


def _read_json(path: Path, name: str) -> tuple[bytes, Any]:
    try:
        raw = path.read_bytes()
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_keys,
            parse_constant=_invalid_number,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise BackendParityError(f"cannot read valid {name}: {path}") from exc
    return raw, value


def _read_jsonl(path: Path, name: str) -> tuple[bytes, list[dict[str, Any]]]:
    try:
        raw = path.read_bytes()
        lines = raw.decode("utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise BackendParityError(f"cannot read UTF-8 {name}: {path}") from exc
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(
                line,
                object_pairs_hook=_unique_keys,
                parse_constant=_invalid_number,
            )
        except (json.JSONDecodeError, BackendParityError) as exc:
            raise BackendParityError(
                f"invalid {name} at line {line_number}: {exc}"
            ) from exc
        _require(isinstance(value, dict), f"{name} row must be an object")
        rows.append(value)
    _require(bool(rows), f"{name} must not be empty")
    return raw, rows


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    _require(isinstance(value, Mapping), f"{name} must be an object")
    return value


def _text(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and bool(value.strip()),
        f"{name} must be a non-empty string",
    )
    return value.strip()


def _number(value: Any, name: str) -> float:
    _require(
        type(value) in (int, float)
        and math.isfinite(float(value))
        and float(value) >= 0.0,
        f"{name} must be a finite non-negative number",
    )
    return float(value)


def _sha256_bytes(value: bytes) -> str:
    return sha256(value).hexdigest()


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


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
    return b"".join(_canonical_bytes(value) + b"\n" for value in values)


def _records_by_trial(
    rows: list[dict[str, Any]],
    planned: Mapping[str, Mapping[str, Any]],
    label: str,
    comparison_scope: str,
    scalar_metrics: tuple[str, ...],
    admission_slots: int | None,
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = _text(row.get("trial_key"), f"{label}.trial_key")
        _require(key not in result, f"{label} repeats trial_key {key}")
        _require(key in planned, f"{label} has unplanned trial_key {key}")
        expected = planned[key]
        for field in _IDENTITY_FIELDS:
            _require(
                field in row and row[field] == expected[field],
                f"{label} trial {key} differs on identity field {field}",
            )
        _require(
            row.get("outcome_type") == "completed",
            f"{label} trial {key} is not completed",
        )
        _require(
            row.get("telemetry_complete") is True,
            f"{label} trial {key} telemetry is incomplete",
        )
        _require(
            row.get("artifact_delivery_complete") is True,
            f"{label} trial {key} artifact delivery is incomplete",
        )
        _require(
            row.get("credentials_recorded") is False,
            f"{label} trial {key} must record credentials_recorded=false",
        )
        task_success = row.get("task_success")
        if comparison_scope == "full":
            _require(
                type(task_success) is bool,
                f"{label} trial {key} task_success must be literal boolean",
            )
        else:
            _require(
                type(task_success) is bool or task_success is None,
                f"{label} trial {key} task_success must be boolean or null",
            )
            if task_success is None:
                _require(
                    row.get("semantic_task_quality_evaluated") is False,
                    f"{label} trial {key} null task_success requires explicit "
                    "semantic_task_quality_evaluated=false",
                )
        for metric in scalar_metrics:
            _number(row.get(metric), f"{label}.{key}.{metric}")
        if "trial_admission_queue_ms" in scalar_metrics:
            _require(
                row.get("latency_origin") == TRIAL_LATENCY_ORIGIN
                and row.get("trial_admission_algorithm")
                == TRIAL_ADMISSION_ALGORITHM,
                f"{label} trial {key} uses different admission semantics",
            )
            _require(
                row.get("trial_admission_slots") == admission_slots,
                f"{label} trial {key} uses different admission slots",
            )
            _require(
                abs(
                    float(row["latency_ms"])
                    - float(row["trial_admission_queue_ms"])
                    - float(row["active_execution_latency_ms"])
                )
                <= 1e-6,
                f"{label} trial {key} latency does not include admission wait",
            )
        for metric in _MAP_METRICS:
            values = _mapping(row.get(metric), f"{label}.{key}.{metric}")
            for resource, value in values.items():
                _text(resource, f"{label}.{key}.{metric} resource")
                _number(value, f"{label}.{key}.{metric}.{resource}")
        result[key] = row
    _require(
        set(result) == set(planned),
        f"{label} does not contain the exact frozen trial set",
    )
    return result


def _map_deltas(
    reference: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> dict[str, float]:
    keys = set(reference) | set(candidate)
    return {
        key: float(candidate.get(key, 0.0)) - float(reference.get(key, 0.0))
        for key in sorted(keys)
    }


def _pair(
    reference_label: str,
    candidate_label: str,
    reference: Mapping[str, Any],
    candidate: Mapping[str, Any],
    comparison_scope: str,
    scalar_metrics: tuple[str, ...],
) -> dict[str, Any]:
    scalar_deltas = {
        metric: float(candidate[metric]) - float(reference[metric])
        for metric in scalar_metrics
    }
    reference_latency = float(reference["latency_ms"])
    full = comparison_scope == "full"
    return {
        "schema_version": PARITY_PAIR_SCHEMA_VERSION,
        "comparison_scope": comparison_scope,
        "trial_key": reference["trial_key"],
        "trial_id": reference["trial_id"],
        "order_index": reference["order_index"],
        "workload_id": reference["workload_id"],
        "workload_class": reference["workload_class"],
        "design_id": reference["design_id"],
        "repetition": reference["repetition"],
        "reference_backend": reference_label,
        "candidate_backend": candidate_label,
        "reference_metrics": {
            metric: reference[metric] for metric in scalar_metrics
        },
        "candidate_metrics": {
            metric: candidate[metric] for metric in scalar_metrics
        },
        "scalar_deltas": scalar_deltas,
        "latency_absolute_relative_error": (
            abs(scalar_deltas["latency_ms"]) / reference_latency
            if reference_latency > 0.0
            else (0.0 if candidate["latency_ms"] == 0.0 else None)
        ),
        "resource_service_ms_delta": _map_deltas(
            reference["resource_service_ms"],
            candidate["resource_service_ms"],
        ),
        "resource_queue_ms_delta": _map_deltas(
            reference["resource_queue_ms"],
            candidate["resource_queue_ms"],
        ),
        "reference_task_success": reference["task_success"] if full else None,
        "candidate_task_success": candidate["task_success"] if full else None,
        "task_success_changed": (
            reference["task_success"] != candidate["task_success"]
            if full
            else None
        ),
        "task_success_comparison_status": (
            "COMPARED" if full else "EXCLUDED_BY_INFRASTRUCTURE_ONLY_SCOPE"
        ),
        "semantic_task_quality_compared": full,
    }


def _p95(values: list[float]) -> float:
    ordered = sorted(values)
    position = max(0, math.ceil(0.95 * len(ordered)) - 1)
    return ordered[position]


def _mean_map_delta(
    rows: list[dict[str, Any]],
    field: str,
) -> dict[str, float]:
    resources = {
        resource
        for row in rows
        for resource in row[field]
    }
    return {
        resource: mean(float(row[field].get(resource, 0.0)) for row in rows)
        for resource in sorted(resources)
    }


def _aggregate(
    rows: list[dict[str, Any]],
    scope: str,
    comparison_scope: str,
) -> dict[str, Any]:
    latency_deltas = [row["scalar_deltas"]["latency_ms"] for row in rows]
    relative = [
        row["latency_absolute_relative_error"] for row in rows
        if row["latency_absolute_relative_error"] is not None
    ]
    result = {
        "scope": scope,
        "paired_trial_count": len(rows),
        "mean_latency_delta_ms": mean(latency_deltas),
        "median_latency_delta_ms": median(latency_deltas),
        "p95_absolute_latency_delta_ms": _p95([
            abs(value) for value in latency_deltas
        ]),
        "mean_latency_absolute_relative_error": (
            mean(relative) if relative else None
        ),
        "mean_network_bytes_delta": mean(
            row["scalar_deltas"]["network_bytes"] for row in rows
        ),
        "mean_logical_bytes_delta": mean(
            row["scalar_deltas"]["logical_bytes"] for row in rows
        ),
        "mean_physical_bytes_delta": mean(
            row["scalar_deltas"]["physical_bytes"] for row in rows
        ),
        "mean_resource_service_ms_delta": _mean_map_delta(
            rows,
            "resource_service_ms_delta",
        ),
        "mean_resource_queue_ms_delta": _mean_map_delta(
            rows,
            "resource_queue_ms_delta",
        ),
        "task_success_change_count": (
            sum(row["task_success_changed"] for row in rows)
            if comparison_scope == "full"
            else None
        ),
        "semantic_task_quality_compared": comparison_scope == "full",
    }
    if "trial_admission_queue_ms" in rows[0]["scalar_deltas"]:
        result["mean_trial_admission_queue_delta_ms"] = mean(
            row["scalar_deltas"]["trial_admission_queue_ms"] for row in rows
        )
        result["mean_active_execution_latency_delta_ms"] = mean(
            row["scalar_deltas"]["active_execution_latency_ms"] for row in rows
        )
    return result


def _design_rank(
    rows: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    values: dict[str, list[float]] = defaultdict(list)
    for row in rows.values():
        values[row["design_id"]].append(float(row["latency_ms"]))
    return [
        design_id for design_id, _ in sorted(
            (
                (design_id, mean(latencies))
                for design_id, latencies in values.items()
            ),
            key=lambda item: (item[1], item[0]),
        )
    ]


def _verify_output(root: Path) -> dict[str, Any]:
    expected = _OUTPUT_FILES | {"SHA256SUMS"}
    actual = {path.name for path in root.iterdir() if path.is_file()}
    _require(actual == expected, "parity output file set changed")
    checksums: dict[str, str] = {}
    for line in (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
        digest, separator, name = line.partition("  ")
        _require(
            separator == "  " and name in _OUTPUT_FILES,
            "parity SHA256SUMS is malformed",
        )
        _require(name not in checksums, f"duplicate parity checksum: {name}")
        _require(
            _sha256_bytes((root / name).read_bytes()) == digest,
            f"parity checksum mismatch: {name}",
        )
        checksums[name] = digest
    _require(set(checksums) == _OUTPUT_FILES, "parity checksums are incomplete")
    _, value = _read_json(root / "parity_manifest.json", "parity manifest")
    manifest = _mapping(value, "parity manifest")
    _require(
        manifest.get("schema_version") == PARITY_MANIFEST_SCHEMA_VERSION,
        "unsupported parity manifest schema_version",
    )
    _require(manifest.get("status") == "COMPLETE", "parity run is incomplete")
    _require(
        manifest.get("output_sha256")
        == {
            name: digest
            for name, digest in checksums.items()
            if name != "parity_manifest.json"
        },
        "parity manifest output digests disagree",
    )
    return dict(manifest)


def evaluate_backend_parity(
    portable_plan_dir: str | Path,
    reference_records_path: str | Path,
    candidate_records_path: str | Path,
    *,
    reference_label: str,
    candidate_label: str,
    output_dir: str | Path,
    comparison_scope: str = "full",
) -> dict[str, Any]:
    """Compare two complete backend ledgers under one portable plan."""

    reference_name = _text(reference_label, "reference_label")
    candidate_name = _text(candidate_label, "candidate_label")
    scope_name = _text(comparison_scope, "comparison_scope")
    _require(
        scope_name in PARITY_COMPARISON_SCOPES,
        f"comparison_scope must be one of {PARITY_COMPARISON_SCOPES}",
    )
    _require(reference_name != candidate_name, "backend labels must differ")
    portable_root = Path(portable_plan_dir).resolve()
    portable = verify_portable_execution_plan(portable_root)
    admission_aware = portable.get("trial_admission") is not None
    admission_slots = (
        int(portable["trial_admission"]["slots"])
        if admission_aware
        else None
    )
    scalar_metrics = _BASE_SCALAR_METRICS + (
        _ADMISSION_SCALAR_METRICS if admission_aware else ()
    )
    trial_raw, trial_rows = _read_jsonl(
        portable_root / "trials.jsonl",
        "portable trials",
    )
    planned = {row["trial_key"]: row for row in trial_rows}
    _require(len(planned) == len(trial_rows), "portable plan repeats trial keys")
    reference_raw, reference_rows = _read_jsonl(
        Path(reference_records_path).resolve(),
        "reference records",
    )
    candidate_raw, candidate_rows = _read_jsonl(
        Path(candidate_records_path).resolve(),
        "candidate records",
    )
    reference = _records_by_trial(
        reference_rows,
        planned,
        reference_name,
        scope_name,
        scalar_metrics,
        admission_slots,
    )
    candidate = _records_by_trial(
        candidate_rows,
        planned,
        candidate_name,
        scope_name,
        scalar_metrics,
        admission_slots,
    )
    pairs = [
        _pair(
            reference_name,
            candidate_name,
            reference[key],
            candidate[key],
            scope_name,
            scalar_metrics,
        )
        for key in sorted(planned, key=lambda item: planned[item]["order_index"])
    ]
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in pairs:
        grouped[("design", row["design_id"])].append(row)
        grouped[("workload_class", row["workload_class"])].append(row)
    aggregates = [_aggregate(pairs, "overall", scope_name)]
    aggregates.extend(
        {
            **_aggregate(rows, f"{kind}:{value}", scope_name),
            "group_kind": kind,
            "group_value": value,
        }
        for (kind, value), rows in sorted(grouped.items())
    )
    reference_rank = _design_rank(reference)
    candidate_rank = _design_rank(candidate)
    report = {
        "schema_version": PARITY_REPORT_SCHEMA_VERSION,
        "status": "DESCRIPTIVE_ONLY_THRESHOLDS_UNSET",
        "comparison_scope": scope_name,
        "scenario_id": portable["scenario_id"],
        "portable_plan_sha256": portable["plan_sha256"],
        "reference_backend": reference_name,
        "candidate_backend": candidate_name,
        "paired_trial_count": len(pairs),
        "aggregates": aggregates,
        "reference_design_latency_rank": reference_rank,
        "candidate_design_latency_rank": candidate_rank,
        "design_latency_rank_exact_match": reference_rank == candidate_rank,
        "trial_admission_contract_compared": admission_aware,
        "compared_metrics": [
            *scalar_metrics,
            "resource_service_ms",
            "resource_queue_ms",
            *(["task_success"] if scope_name == "full" else []),
        ],
        "excluded_metrics": (
            ["configured_cost"]
            if scope_name == "full"
            else ["task_success", "configured_cost"]
        ),
        "semantic_task_quality_compared": scope_name == "full",
        "infrastructure_only_does_not_establish_task_quality": (
            scope_name == "infrastructure-only"
        ),
        "parity_thresholds": None,
        "parity_claim_made": False,
        "configured_cost_excluded": True,
        "configured_cost_exclusion_reason": (
            "scenario rate-card units are not physical monetary measurements"
        ),
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    pair_bytes = _jsonl_bytes(pairs)
    report_bytes = _json_bytes(report)
    documents = {
        "parity_pairs.jsonl": pair_bytes,
        "parity_report.json": report_bytes,
    }
    manifest = {
        "schema_version": PARITY_MANIFEST_SCHEMA_VERSION,
        "status": "COMPLETE",
        "comparison_scope": scope_name,
        "scenario_id": portable["scenario_id"],
        "portable_plan_sha256": portable["plan_sha256"],
        "portable_trials_sha256": _sha256_bytes(trial_raw),
        "reference_records_sha256": _sha256_bytes(reference_raw),
        "candidate_records_sha256": _sha256_bytes(candidate_raw),
        "paired_trial_count": len(pairs),
        "parity_claim_made": False,
        "semantic_task_quality_compared": scope_name == "full",
        "external_services_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
        "output_sha256": {
            name: _sha256_bytes(content)
            for name, content in sorted(documents.items())
        },
    }
    documents["parity_manifest.json"] = _json_bytes(manifest)
    documents["SHA256SUMS"] = b"".join(
        f"{_sha256_bytes(content)}  {name}\n".encode("utf-8")
        for name, content in sorted(documents.items())
    )
    target = Path(output_dir).resolve()
    _require(not target.exists(), f"parity output already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging_parent = Path(tempfile.mkdtemp(prefix=".parity-", dir=target.parent))
    staging = staging_parent / "output"
    try:
        staging.mkdir()
        for name, content in documents.items():
            path = staging / name
            with path.open("wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
        _verify_output(staging)
        os.replace(staging, target)
    finally:
        shutil.rmtree(staging_parent, ignore_errors=True)
    verified = _verify_output(target)
    return {
        **verified,
        "evaluation_status": report["status"],
        "comparison_scope": scope_name,
        "design_latency_rank_exact_match": report[
            "design_latency_rank_exact_match"
        ],
        "output_dir": str(target),
        "report_path": str(target / "parity_report.json"),
    }


def verify_backend_parity(output_dir: str | Path) -> dict[str, Any]:
    """Read-only verification of one descriptive parity evaluation."""

    root = Path(output_dir).resolve()
    _require(root.is_dir(), f"parity output does not exist: {root}")
    manifest = _verify_output(root)
    _, report_value = _read_json(root / "parity_report.json", "parity report")
    report = _mapping(report_value, "parity report")
    _require(
        report.get("schema_version") == PARITY_REPORT_SCHEMA_VERSION,
        "unsupported parity report schema_version",
    )
    comparison_scope = manifest.get("comparison_scope", "full")
    _require(
        comparison_scope in PARITY_COMPARISON_SCOPES,
        "unsupported parity comparison scope",
    )
    _require(
        report.get("comparison_scope", "full") == comparison_scope,
        "parity report and manifest comparison scopes differ",
    )
    _, pairs = _read_jsonl(root / "parity_pairs.jsonl", "parity pairs")
    _require(
        len(pairs) == manifest["paired_trial_count"]
        == report.get("paired_trial_count"),
        "parity paired trial count changed",
    )
    for pair in pairs:
        _require(
            pair.get("comparison_scope", "full") == comparison_scope,
            "parity pair comparison scope changed",
        )
        if comparison_scope == "infrastructure-only":
            _require(
                pair.get("semantic_task_quality_compared") is False
                and pair.get("task_success_changed") is None
                and pair.get("reference_task_success") is None
                and pair.get("candidate_task_success") is None
                and pair.get("task_success_comparison_status")
                == "EXCLUDED_BY_INFRASTRUCTURE_ONLY_SCOPE",
                "infrastructure-only pair promotes semantic task quality",
            )
    if comparison_scope == "infrastructure-only":
        _require(
            manifest.get("semantic_task_quality_compared") is False
            and report.get("semantic_task_quality_compared") is False
            and report.get("infrastructure_only_does_not_establish_task_quality")
            is True,
            "infrastructure-only provenance is incomplete",
        )
        _require(
            "task_success" in report.get("excluded_metrics", []),
            "infrastructure-only report does not exclude task_success",
        )
        for aggregate in report.get("aggregates", []):
            _require(
                aggregate.get("task_success_change_count") is None
                and aggregate.get("semantic_task_quality_compared") is False,
                "infrastructure-only aggregate promotes task quality",
            )
    _require(
        report.get("parity_claim_made") is False,
        "unregistered parity claim is forbidden",
    )
    return {
        "status": "VERIFIED",
        "scenario_id": manifest["scenario_id"],
        "paired_trial_count": manifest["paired_trial_count"],
        "comparison_scope": comparison_scope,
        "evaluation_status": report["status"],
        "semantic_task_quality_compared": comparison_scope == "full",
        "parity_claim_made": False,
        "checked_files": len(_OUTPUT_FILES),
        "eligible_for_scientific_claims": False,
    }
