"""Read-only import of frozen FlowMesh records into simulator observations.

This module is intentionally an evidence adapter, not a parameter fitter.  It
normalizes quantities that FlowMesh and the Data Agent actually recorded while
refusing to infer unobserved disk, network, or model-service components.  A
later calibration step can bind these observations to named simulator
resources without weakening their provenance.
"""

from __future__ import annotations

import json
import math
import os
import shutil
import tempfile
from collections import defaultdict
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from statistics import mean
from typing import Any, Iterable, Mapping


FLOWMESH_TRACE_IMPORT_SCHEMA_VERSION = (
    "pathfinder.flowmesh-infra-trace-import/v1alpha1"
)
FLOWMESH_TRIAL_OBSERVATION_SCHEMA_VERSION = (
    "pathfinder.flowmesh-trial-observation/v1alpha1"
)
FLOWMESH_ACCESS_OBSERVATION_SCHEMA_VERSION = (
    "pathfinder.flowmesh-access-observation/v1alpha1"
)
FLOWMESH_TRACE_SUMMARY_SCHEMA_VERSION = (
    "pathfinder.flowmesh-trace-summary/v1alpha1"
)

SUPPORTED_RECORD_SCHEMAS = (
    "pathfinder.distributed-pilot-record/v1alpha1",
    "pathfinder.flowmesh-pilot-record/v1alpha1",
)

LATENCY_FIELDS = (
    "felt_latency_ms",
    "data_agent_service_latency_ms",
    "data_agent_fetch_latency_ms",
    "data_agent_controlled_delay_ms",
    "artifact_transfer_latency_ms",
)


class FlowMeshTraceImportError(ValueError):
    """Raised when runtime evidence is ambiguous or unsafe to import."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise FlowMeshTraceImportError(message)


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _invalid_number(value: str) -> None:
    raise FlowMeshTraceImportError(f"non-finite JSON number: {value}")


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


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
    return "".join(_canonical_json(value) + "\n" for value in values).encode(
        "utf-8"
    )


def _sha256_bytes(value: bytes) -> str:
    return sha256(value).hexdigest()


def _text(value: Any, name: str, *, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    _require(
        isinstance(value, str) and bool(value.strip()),
        f"{name} must be a non-empty string",
    )
    return value.strip()


def _number(
    value: Any,
    name: str,
    *,
    optional: bool = False,
    integer: bool = False,
) -> float | int | None:
    if value is None and optional:
        return None
    expected = type(value) is int if integer else type(value) in (int, float)
    _require(expected, f"{name} must be a {'non-negative integer' if integer else 'number'}")
    result = float(value)
    _require(math.isfinite(result) and result >= 0.0, f"{name} is invalid")
    return int(value) if integer else result


def _read_jsonl(path: Path) -> tuple[bytes, list[dict[str, Any]]]:
    try:
        raw = path.read_bytes()
        text = raw.decode("utf-8")
    except (OSError, UnicodeError) as exc:
        raise FlowMeshTraceImportError(
            f"cannot read UTF-8 FlowMesh records: {path}"
        ) from exc
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(
                line,
                object_pairs_hook=_unique_keys,
                parse_constant=_invalid_number,
            )
        except (json.JSONDecodeError, FlowMeshTraceImportError) as exc:
            raise FlowMeshTraceImportError(
                f"invalid FlowMesh JSONL at {path.name}:{line_number}: {exc}"
            ) from exc
        _require(
            isinstance(value, dict),
            f"{path.name}:{line_number} must contain one JSON object",
        )
        records.append(value)
    _require(bool(records), f"FlowMesh records file is empty: {path}")
    return raw, records


def _duration_ms(record: Mapping[str, Any], name: str) -> float | None:
    if record.get("duration_seconds") is not None:
        seconds = _number(record["duration_seconds"], f"{name}.duration_seconds")
        assert isinstance(seconds, float)
        return seconds * 1000.0
    started = record.get("started_at")
    finished = record.get("finished_at")
    if started is None and finished is None:
        return None
    _require(
        isinstance(started, str) and isinstance(finished, str),
        f"{name} must provide both started_at and finished_at",
    )
    try:
        start = datetime.fromisoformat(started.replace("Z", "+00:00"))
        end = datetime.fromisoformat(finished.replace("Z", "+00:00"))
        _require(
            start.utcoffset() is not None and end.utcoffset() is not None,
            f"{name} timestamps must include UTC offsets",
        )
        seconds = (end - start).total_seconds()
    except (TypeError, ValueError) as exc:
        raise FlowMeshTraceImportError(
            f"{name} has invalid ISO-8601 timestamps"
        ) from exc
    _require(seconds >= 0.0, f"{name} finishes before it starts")
    return seconds * 1000.0


def _fingerprint(value: Any, name: str) -> str | None:
    if value is None:
        return None
    text = _text(value, name)
    assert isinstance(text, str)
    _require(
        len(text) == 64 and all(character in "0123456789abcdef" for character in text),
        f"{name} must be a lowercase SHA-256 digest",
    )
    return text


def _identity_fingerprint(value: Any, name: str) -> str | None:
    text = _text(value, name, optional=True)
    if text is None:
        return None
    return sha256(text.encode("utf-8")).hexdigest()


def _stats(values: Iterable[float | int | None]) -> dict[str, Any]:
    supplied = list(values)
    observed = sorted(float(value) for value in supplied if value is not None)

    def quantile(fraction: float) -> float | None:
        if not observed:
            return None
        position = (len(observed) - 1) * fraction
        lower = math.floor(position)
        upper = math.ceil(position)
        return observed[lower] + (
            observed[upper] - observed[lower]
        ) * (position - lower)

    return {
        "count": len(observed),
        "missing_count": sum(value is None for value in supplied),
        "minimum": observed[0] if observed else None,
        "mean": mean(observed) if observed else None,
        "median": quantile(0.5),
        "p95": quantile(0.95),
        "maximum": observed[-1] if observed else None,
    }


def _event_observation(
    event: Mapping[str, Any],
    *,
    source_sha256: str,
    source_row_index: int,
    source_event_index: int,
    record: Mapping[str, Any],
    require_complete_delivery: bool,
) -> dict[str, Any]:
    name = f"record[{source_row_index}].access_events[{source_event_index}]"
    _require("artifact_handle" not in event, f"{name} contains a raw artifact handle")
    accepted = event.get("accepted")
    _require(type(accepted) is bool, f"{name}.accepted must be a literal boolean")
    representation_id = _text(
        event.get("representation_id"), f"{name}.representation_id", optional=True
    )
    if accepted:
        _require(
            representation_id is not None,
            f"{name}.representation_id is required for an accepted access",
        )
    handle = _fingerprint(
        event.get("artifact_handle_sha256"), f"{name}.artifact_handle_sha256"
    )
    bytes_read = _number(
        event.get("bytes_read"), f"{name}.bytes_read", optional=True, integer=True
    )
    artifact_bytes = _number(
        event.get("artifact_bytes_sent"),
        f"{name}.artifact_bytes_sent",
        optional=True,
        integer=True,
    )
    requests = _number(
        event.get("artifact_download_request_count"),
        f"{name}.artifact_download_request_count",
        optional=True,
        integer=True,
    )
    full_downloads = _number(
        event.get("artifact_full_download_count"),
        f"{name}.artifact_full_download_count",
        optional=True,
        integer=True,
    )
    if accepted and handle is not None and require_complete_delivery:
        _require(
            requests is not None and requests >= 1,
            f"{name} selected an artifact without a download request",
        )
        _require(
            full_downloads is not None and full_downloads >= 1,
            f"{name} selected an artifact without a completed full download",
        )
        _require(
            artifact_bytes is not None and artifact_bytes > 0,
            f"{name} selected an artifact without transferred bytes",
        )
    if handle is None:
        _require(
            not any((artifact_bytes or 0, requests or 0, full_downloads or 0)),
            f"{name} has artifact counters without a handle fingerprint",
        )
    latencies = {
        field: _number(event.get(field), f"{name}.{field}", optional=True)
        for field in LATENCY_FIELDS
    }
    realized_cost = _number(
        event.get("realized_cost"), f"{name}.realized_cost", optional=True
    )
    payload_bytes = artifact_bytes if handle is not None else bytes_read
    identity = f"{source_sha256}:{source_row_index}:{source_event_index}"
    return {
        "schema_version": FLOWMESH_ACCESS_OBSERVATION_SCHEMA_VERSION,
        "observation_id": sha256(identity.encode("utf-8")).hexdigest(),
        "source_file_sha256": source_sha256,
        "source_row_index": source_row_index,
        "source_event_index": source_event_index,
        "experiment_id": _text(
            record.get("experiment_id"), f"{name}.experiment_id"
        ),
        "trial_key_sha256": _identity_fingerprint(
            record.get("trial_key"), f"{name}.trial_key"
        ),
        "workload_id": _text(
            record.get("workload_id"), f"{name}.workload_id"
        ),
        "design_id": _text(record.get("design_id"), f"{name}.design_id"),
        "task_class_id": _text(
            record.get("task_class_id"), f"{name}.task_class_id"
        ),
        "representation_id": representation_id,
        "endpoint_id": _text(
            event.get("endpoint_id"), f"{name}.endpoint_id", optional=True
        ),
        "source_node_id": _text(
            event.get("source_node_id"), f"{name}.source_node_id", optional=True
        ),
        "destination_execution_node_id": _text(
            event.get("destination_execution_node_id"),
            f"{name}.destination_execution_node_id",
            optional=True,
        ),
        "source_location": _text(
            event.get("source_location", event.get("location")),
            f"{name}.source_location",
            optional=True,
        ),
        "payload_bytes": payload_bytes,
        "bytes_read": bytes_read,
        "artifact_bytes_sent": artifact_bytes,
        **latencies,
        "runtime_reported_service_cost": realized_cost,
        "runtime_cost_is_not_a_physical_rate_calibration": True,
        "artifact_delivery_via_handle": handle is not None,
        "artifact_download_request_count": requests,
        "artifact_full_download_count": full_downloads,
        "route_identity_complete": all(
            value is not None
            for value in (
                event.get("endpoint_id"),
                event.get("source_node_id"),
                event.get("destination_execution_node_id"),
            )
        ),
        "accepted": accepted,
        "simulated": False,
        "input_origin": "operator-supplied-trace-not-independently-verified",
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def _normalize_records(
    records: list[dict[str, Any]],
    *,
    source_sha256: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    trials: list[dict[str, Any]] = []
    accesses: list[dict[str, Any]] = []
    identities: set[str] = set()
    for row_index, record in enumerate(records):
        name = f"record[{row_index}]"
        schema = _text(record.get("schema_version"), f"{name}.schema_version")
        _require(
            schema in SUPPORTED_RECORD_SCHEMAS,
            f"{name} has unsupported schema_version {schema!r}",
        )
        outcome = _text(record.get("outcome_type"), f"{name}.outcome_type")
        complete = outcome == "completed"
        trial_key = _text(record.get("trial_key"), f"{name}.trial_key")
        assert isinstance(trial_key, str)
        experiment_id = _text(
            record.get("experiment_id"), f"{name}.experiment_id"
        )
        workload_id = _text(record.get("workload_id"), f"{name}.workload_id")
        object_id = _text(record.get("object_id"), f"{name}.object_id")
        design_id = _text(record.get("design_id"), f"{name}.design_id")
        task_class_id = _text(
            record.get("task_class_id"), f"{name}.task_class_id"
        )
        repetition = _number(
            record.get("repetition"), f"{name}.repetition", integer=True
        )
        identity = f"{source_sha256}:{trial_key}"
        _require(identity not in identities, f"duplicate trial_key: {trial_key}")
        identities.add(identity)
        raw_events = record.get("access_events")
        _require(isinstance(raw_events, list), f"{name}.access_events must be an array")
        normalized_events = []
        for event_index, event in enumerate(raw_events):
            _require(
                isinstance(event, Mapping),
                f"{name}.access_events[{event_index}] must be an object",
            )
            normalized_events.append(_event_observation(
                event,
                source_sha256=source_sha256,
                source_row_index=row_index,
                source_event_index=event_index,
                record=record,
                require_complete_delivery=complete,
            ))
        accepted = [event for event in normalized_events if event["accepted"]]
        if record.get("access_event_count") is not None:
            declared = _number(
                record["access_event_count"],
                f"{name}.access_event_count",
                integer=True,
            )
            _require(declared == len(raw_events), f"{name}.access_event_count disagrees")
        if record.get("accepted_access_count") is not None:
            declared = _number(
                record["accepted_access_count"],
                f"{name}.accepted_access_count",
                integer=True,
            )
            _require(declared == len(accepted), f"{name}.accepted_access_count disagrees")

        if complete:
            _require(
                record.get("telemetry_complete") is True,
                f"{name}.telemetry_complete must be the literal True",
            )
            _require(
                record.get("artifact_delivery_complete") is True,
                f"{name}.artifact_delivery_complete must be the literal True",
            )
            _require(
                type(record.get("task_success")) is bool,
                f"{name}.task_success must be a literal boolean",
            )
            accesses.extend(accepted)

        trial_id = f"{source_sha256}:{row_index}:{trial_key}"
        trial_observation = {
            "schema_version": FLOWMESH_TRIAL_OBSERVATION_SCHEMA_VERSION,
            "observation_id": sha256(trial_id.encode("utf-8")).hexdigest(),
            "source_file_sha256": source_sha256,
            "source_row_index": row_index,
            "source_record_schema_version": schema,
            "experiment_id": experiment_id,
            "trial_key_sha256": sha256(trial_key.encode("utf-8")).hexdigest(),
            "trial_id_sha256": _identity_fingerprint(
                record.get("trial_id"), f"{name}.trial_id"
            ),
            "session_id_sha256": _identity_fingerprint(
                record.get("session_id"), f"{name}.session_id"
            ),
            "workflow_id_sha256": _identity_fingerprint(
                record.get("workflow_id"), f"{name}.workflow_id"
            ),
            "task_id_sha256": _identity_fingerprint(
                record.get("task_id"), f"{name}.task_id"
            ),
            "workload_id": workload_id,
            "object_id": object_id,
            "design_id": design_id,
            "task_class_id": task_class_id,
            "repetition": repetition,
            "outcome_type": outcome,
            "task_success": record.get("task_success") if complete else None,
            "duration_ms": _duration_ms(record, name),
            "access_event_count": len(normalized_events),
            "accepted_access_count": len(accepted),
            "calibration_access_count": len(accepted) if complete else 0,
            "included_in_calibration": complete,
            "calibration_exclusion_reason": (
                None if complete else f"non-completed outcome: {outcome}"
            ),
            "simulated": False,
            "input_origin": "operator-supplied-trace-not-independently-verified",
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }
        trials.append(trial_observation)
    return trials, accesses


def _summary(
    trials: list[dict[str, Any]],
    accesses: list[dict[str, Any]],
    *,
    source_sha256: str,
) -> dict[str, Any]:
    by_design: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trial in trials:
        by_design[str(trial.get("design_id") or "unrecorded")].append(trial)
    design_summaries = []
    for design_id, rows in sorted(by_design.items()):
        complete = [row for row in rows if row["included_in_calibration"]]
        design_summaries.append({
            "design_id": design_id,
            "records": len(rows),
            "completed_records": len(complete),
            "failed_records": len(rows) - len(complete),
            "task_success_rate": (
                sum(bool(row["task_success"]) for row in complete) / len(complete)
                if complete
                else None
            ),
            "duration_ms": _stats(row["duration_ms"] for row in complete),
        })

    route_groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    route_fields = (
        "design_id",
        "task_class_id",
        "representation_id",
        "endpoint_id",
        "source_node_id",
        "destination_execution_node_id",
        "source_location",
    )
    for event in accesses:
        route_groups[tuple(event.get(field) for field in route_fields)].append(event)
    route_summaries = []
    for key, rows in sorted(route_groups.items(), key=lambda item: repr(item[0])):
        route = dict(zip(route_fields, key))
        route_summaries.append({
            **route,
            "observations": len(rows),
            "complete_route_identity_observations": sum(
                bool(row["route_identity_complete"]) for row in rows
            ),
            "payload_bytes": _stats(row["payload_bytes"] for row in rows),
            **{
                field: _stats(row[field] for row in rows)
                for field in LATENCY_FIELDS
            },
            "runtime_reported_service_cost": _stats(
                row["runtime_reported_service_cost"] for row in rows
            ),
        })
    return {
        "schema_version": FLOWMESH_TRACE_SUMMARY_SCHEMA_VERSION,
        "status": "COMPLETE",
        "source_file_sha256": source_sha256,
        "record_count": len(trials),
        "completed_record_count": sum(
            trial["included_in_calibration"] for trial in trials
        ),
        "failed_record_count": sum(
            not trial["included_in_calibration"] for trial in trials
        ),
        "calibration_access_observation_count": len(accesses),
        "design_summaries": design_summaries,
        "route_summaries": route_summaries,
        "physical_cost_rate_calibrated": False,
        "unobserved_components_not_inferred": [
            "disk_service_time_when_not_separately_recorded",
            "network_queue_time_when_not_separately_recorded",
            "worker_cpu_time",
            "worker_gpu_time",
            "model_service_time",
        ],
        "simulated": False,
        "input_origin": "operator-supplied-trace-not-independently-verified",
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def _documents(
    *,
    source_path: Path,
    source_sha256: str,
    trials: list[dict[str, Any]],
    accesses: list[dict[str, Any]],
) -> dict[str, bytes]:
    summary = _summary(trials, accesses, source_sha256=source_sha256)
    documents = {
        "trial_observations.jsonl": _jsonl_bytes(trials),
        "access_observations.jsonl": _jsonl_bytes(accesses),
        "calibration_summary.json": _json_bytes(summary),
    }
    manifest = {
        "schema_version": FLOWMESH_TRACE_IMPORT_SCHEMA_VERSION,
        "status": "COMPLETE",
        "source_file_name": source_path.name,
        "source_file_sha256": source_sha256,
        "source_record_schemas": sorted({
            trial["source_record_schema_version"] for trial in trials
        }),
        "record_count": len(trials),
        "completed_record_count": summary["completed_record_count"],
        "failed_record_count": summary["failed_record_count"],
        "calibration_access_observation_count": len(accesses),
        "raw_questions_recorded": False,
        "raw_answers_recorded": False,
        "raw_artifact_handles_recorded": False,
        "physical_cost_rate_calibrated": False,
        "external_services_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
        "output_sha256": {
            name: _sha256_bytes(content)
            for name, content in sorted(documents.items())
        },
    }
    documents["import_manifest.json"] = _json_bytes(manifest)
    documents["SHA256SUMS"] = "".join(
        f"{_sha256_bytes(content)}  {name}\n"
        for name, content in sorted(documents.items())
    ).encode("utf-8")
    return documents


def _verify(root: Path) -> dict[str, Any]:
    checksum_path = root / "SHA256SUMS"
    _require(checksum_path.is_file(), "trace import has no SHA256SUMS")
    found: set[str] = set()
    for line in checksum_path.read_text(encoding="utf-8").splitlines():
        digest, separator, name = line.partition("  ")
        _require(
            separator == "  " and bool(name) and Path(name).name == name,
            "trace import SHA256SUMS is malformed",
        )
        _require(name not in found, f"duplicate checksum entry: {name}")
        found.add(name)
        path = root / name
        _require(path.is_file(), f"trace import output is missing: {name}")
        _require(
            _sha256_bytes(path.read_bytes()) == digest,
            f"trace import checksum mismatch: {name}",
        )
    expected = {
        "trial_observations.jsonl",
        "access_observations.jsonl",
        "calibration_summary.json",
        "import_manifest.json",
    }
    actual = {path.name for path in root.iterdir() if path.is_file()}
    _require(
        actual == expected | {"SHA256SUMS"},
        "trace import directory contains an unexpected file set",
    )
    _require(found == expected, "trace import checksums do not bind the exact output set")
    manifest = json.loads(
        (root / "import_manifest.json").read_text(encoding="utf-8"),
        object_pairs_hook=_unique_keys,
        parse_constant=_invalid_number,
    )
    _require(
        manifest.get("schema_version") == FLOWMESH_TRACE_IMPORT_SCHEMA_VERSION,
        "trace import manifest has an unsupported schema_version",
    )
    _require(manifest.get("status") == "COMPLETE", "trace import is not complete")
    manifest_digests = manifest.get("output_sha256")
    _require(
        isinstance(manifest_digests, dict)
        and manifest_digests == {
            name: _sha256_bytes((root / name).read_bytes())
            for name in sorted(expected - {"import_manifest.json"})
        },
        "trace import manifest output digests disagree with its files",
    )
    return manifest


def import_flowmesh_trace(
    records_path: str | Path,
    *,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Normalize one immutable FlowMesh JSONL record source atomically."""

    source = Path(records_path).resolve()
    raw, records = _read_jsonl(source)
    source_sha256 = _sha256_bytes(raw)
    trials, accesses = _normalize_records(records, source_sha256=source_sha256)
    documents = _documents(
        source_path=source,
        source_sha256=source_sha256,
        trials=trials,
        accesses=accesses,
    )
    target = Path(output_dir).resolve()
    if target.exists():
        raise FlowMeshTraceImportError(
            f"trace import output directory already exists: {target}"
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{target.name}.tmp-", dir=target.parent))
    try:
        for name, content in documents.items():
            path = temporary / name
            with path.open("wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
        _verify(temporary)
        temporary.replace(target)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    manifest = _verify(target)
    return {
        **manifest,
        "output_dir": str(target),
        "trial_observations_path": str(target / "trial_observations.jsonl"),
        "access_observations_path": str(target / "access_observations.jsonl"),
        "calibration_summary_path": str(target / "calibration_summary.json"),
        "checksums_path": str(target / "SHA256SUMS"),
    }


def verify_flowmesh_trace_import(output_dir: str | Path) -> dict[str, Any]:
    """Verify a published trace import without modifying it."""

    root = Path(output_dir).resolve()
    _require(root.is_dir(), f"trace import directory does not exist: {root}")
    manifest = _verify(root)
    return {
        "status": "VERIFIED",
        "source_file_sha256": manifest["source_file_sha256"],
        "record_count": manifest["record_count"],
        "calibration_access_observation_count": manifest[
            "calibration_access_observation_count"
        ],
        "checked_files": 4,
        "eligible_for_scientific_claims": False,
    }
