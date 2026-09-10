"""Infrastructure-only execution of a frozen container-emulation plan.

The driver talks to already-running node services.  It does not start Docker,
does not evaluate semantic task quality, and does not convert configured cost
rates into measured money.  The portable plan's global trial-admission width
is authoritative; smoke subsets may use fewer slots.  Concurrent execution
also enforces frozen resource and link slots without claiming to decompose
queues inside the container service, operating system, or Docker network.
"""

from __future__ import annotations

import errno
import json
import os
import threading
import time
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .admission import (
    TRIAL_ADMISSION_ALGORITHM,
    TRIAL_LATENCY_ORIGIN,
    TrialAdmissionError,
    validate_trial_admission_contract,
)
from .container_node import CONTAINER_NODE_RESULT_SCHEMA_VERSION
from .local_container import verify_local_container_compose
from .portable import verify_portable_execution_plan


CONTAINER_EXECUTION_EVENT_SCHEMA_VERSION = (
    "pathfinder.container-emulation-measured-event/v1alpha1"
)
CONTAINER_INFRASTRUCTURE_RECORD_SCHEMA_VERSION = (
    "pathfinder.container-emulation-infrastructure-record/v1alpha2"
)
LEGACY_CONTAINER_INFRASTRUCTURE_RECORD_SCHEMA_VERSION = (
    "pathfinder.container-emulation-infrastructure-record/v1alpha1"
)
CONTAINER_EXECUTION_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.container-emulation-execution-run/v1alpha3"
)
LEGACY_CONTAINER_EXECUTION_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.container-emulation-execution-run/v1alpha1"
)
CHECKPOINT_CONTAINER_EXECUTION_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.container-emulation-execution-run/v1alpha2"
)
CONTAINER_EXECUTION_CHECKPOINT_SCHEMA_VERSION = (
    "pathfinder.container-emulation-execution-checkpoint/v1alpha2"
)
LEGACY_CONTAINER_EXECUTION_CHECKPOINT_SCHEMA_VERSION = (
    "pathfinder.container-emulation-execution-checkpoint/v1alpha1"
)
CONTAINER_EXECUTION_CHECKPOINT_ENTRY_SCHEMA_VERSION = (
    "pathfinder.container-emulation-execution-checkpoint-entry/v1alpha2"
)
LEGACY_CONTAINER_EXECUTION_CHECKPOINT_ENTRY_SCHEMA_VERSION = (
    "pathfinder.container-emulation-execution-checkpoint-entry/v1alpha1"
)

_ATOMIC_REPLACE_MAX_ATTEMPTS = 20
_ATOMIC_REPLACE_INITIAL_BACKOFF_SECONDS = 0.01
_ATOMIC_REPLACE_MAX_BACKOFF_SECONDS = 0.25

_LEGACY_OUTPUT_FILES = {
    "container_run_manifest.json",
    "infrastructure_records.jsonl",
    "operation_results.jsonl",
}
_CHECKPOINT_FILES = {
    "container_run_checkpoint.json",
    "trial_checkpoint.jsonl",
}
_OUTPUT_FILES = _LEGACY_OUTPUT_FILES | _CHECKPOINT_FILES

_TRIAL_IDENTITY_FIELDS = (
    "trial_key",
    "trial_id",
    "workflow_id",
    "task_id",
    "session_id",
    "order_index",
    "workload_id",
    "workload_class",
    "object_id",
    "task_type",
    "design_id",
    "executor_node_id",
    "repetition",
    "seed",
    "arrival_time_ms",
)


class ContainerExecutionError(RuntimeError):
    """Raised when a live node violates the frozen execution contract."""


class _AdmissionGate:
    """One auditable capacity gate derived from a frozen portable binding."""

    def __init__(self, key: str, slots: int) -> None:
        self.key = key
        self.slots = slots
        self._semaphore = threading.BoundedSemaphore(slots)

    def acquire(self, ready_ns: int) -> tuple[int, bool]:
        if self._semaphore.acquire(blocking=False):
            return ready_ns, False
        self._semaphore.acquire()
        return time.perf_counter_ns(), True

    def release(self) -> None:
        self._semaphore.release()


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ContainerExecutionError(message)


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


def _sha256_bytes(value: bytes) -> str:
    return sha256(value).hexdigest()


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    try:
        return [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ContainerExecutionError(f"cannot read valid JSONL: {path}") from exc


def _atomic_write(path: Path, content: bytes) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        for attempt in range(_ATOMIC_REPLACE_MAX_ATTEMPTS):
            try:
                os.replace(temporary, path)
                break
            except PermissionError as exc:
                transient_windows_lock = (
                    getattr(exc, "winerror", None) in (5, 32)
                    or exc.errno in (errno.EACCES, errno.EBUSY, errno.EPERM)
                )
                if (
                    not transient_windows_lock
                    or attempt + 1 == _ATOMIC_REPLACE_MAX_ATTEMPTS
                ):
                    raise
                time.sleep(
                    min(
                        _ATOMIC_REPLACE_INITIAL_BACKOFF_SECONDS * (2**attempt),
                        _ATOMIC_REPLACE_MAX_BACKOFF_SECONDS,
                    )
                )
    finally:
        if temporary.exists():
            temporary.unlink()


def _document_sha256(document: Mapping[str, Any], digest_field: str) -> str:
    payload = dict(document)
    payload.pop(digest_field, None)
    return _sha256_bytes(_canonical_bytes(payload))


def _checkpoint_contract(
    *,
    scenario_id: str,
    backend_id: str,
    portable_plan_sha256: str,
    endpoint_binding_sha256: str,
    endpoint_binding_source: str,
    selected_trial_keys: list[str],
    requested_max_concurrency: int,
    frozen_trial_admission_slots: int,
    request_timeout_seconds: float,
    runtime_epochs: Mapping[str, str],
) -> dict[str, Any]:
    concurrent = requested_max_concurrency > 1 and len(selected_trial_keys) > 1
    contract = {
        "schema_version": CONTAINER_EXECUTION_CHECKPOINT_SCHEMA_VERSION,
        "scenario_id": scenario_id,
        "backend_id": backend_id,
        "portable_plan_sha256": portable_plan_sha256,
        "endpoint_binding_sha256": endpoint_binding_sha256,
        "endpoint_binding_source": endpoint_binding_source,
        "selected_trial_keys": selected_trial_keys,
        "selected_trial_keys_sha256": _sha256_bytes(
            _canonical_bytes(selected_trial_keys)
        ),
        "requested_max_concurrency": requested_max_concurrency,
        "frozen_trial_admission_slots": frozen_trial_admission_slots,
        "request_timeout_seconds": float(request_timeout_seconds),
        "execution_mode": "concurrent" if concurrent else "serial",
        "runtime_epochs": dict(sorted(runtime_epochs.items())),
        "runtime_epochs_sha256": _sha256_bytes(
            _canonical_bytes(dict(sorted(runtime_epochs.items())))
        ),
        "resume_requires_same_runtime_epoch": True,
        "partial_trials_are_not_canonical": True,
        "operation_idempotency_required": True,
        "credentials_recorded": False,
    }
    contract["checkpoint_sha256"] = _document_sha256(
        contract,
        "checkpoint_sha256",
    )
    return contract


def _validate_record_events(
    record: Mapping[str, Any],
    events: list[Mapping[str, Any]],
) -> None:
    record_schema = record.get("schema_version")
    _require(
        record_schema in (
            CONTAINER_INFRASTRUCTURE_RECORD_SCHEMA_VERSION,
            LEGACY_CONTAINER_INFRASTRUCTURE_RECORD_SCHEMA_VERSION,
        ),
        "unsupported infrastructure record schema_version",
    )
    _require(record.get("outcome_type") == "completed", "record is incomplete")
    _require(record.get("telemetry_complete") is True, "telemetry incomplete")
    _require(
        record.get("artifact_delivery_complete") is True,
        "artifact delivery incomplete",
    )
    _require(record.get("credentials_recorded") is False, "credentials recorded")
    _require(
        record.get("semantic_task_quality_evaluated") is False
        and record.get("task_success") is None,
        "infrastructure record promotes semantic task quality",
    )
    _require(record.get("event_count") == len(events), "record event count changed")
    _require(
        record.get("executed_event_count")
        == sum(row.get("executed") is True for row in events),
        "record executed event count changed",
    )
    _require(
        record.get("logical_bytes")
        == sum(int(row.get("logical_bytes", -1)) for row in events),
        "record logical byte count changed",
    )
    _require(
        record.get("physical_bytes")
        == sum(int(row.get("physical_bytes", -1)) for row in events),
        "record physical byte count changed",
    )
    _require(
        record.get("network_bytes")
        == sum(
            int(row.get("physical_bytes", -1))
            for row in events
            if row.get("executed") is True
            and row.get("operation_kind") == "network_transfer"
        ),
        "record network byte count changed",
    )
    if record_schema == CONTAINER_INFRASTRUCTURE_RECORD_SCHEMA_VERSION:
        admission_ms = record.get("trial_admission_queue_ms")
        active_ms = record.get("active_execution_latency_ms")
        latency_ms = record.get("latency_ms")
        _require(
            type(admission_ms) in (int, float) and admission_ms >= 0.0,
            "record trial admission queue time is invalid",
        )
        _require(
            type(active_ms) in (int, float) and active_ms >= 0.0,
            "record active execution latency is invalid",
        )
        _require(
            type(latency_ms) in (int, float)
            and abs(float(latency_ms) - float(admission_ms) - float(active_ms))
            <= 1e-6,
            "record latency does not include trial admission queue time",
        )
        _require(
            record.get("latency_origin") == TRIAL_LATENCY_ORIGIN
            and record.get("trial_admission_algorithm")
            == TRIAL_ADMISSION_ALGORITHM,
            "record trial admission semantics changed",
        )
        _require(
            type(record.get("trial_admission_slots")) is int
            and record["trial_admission_slots"] > 0,
            "record trial admission slots are invalid",
        )
        _require(
            record.get("trial_dispatch_queue_ms") == admission_ms,
            "deprecated dispatch queue alias differs from admission queue",
        )


def _load_checkpoint_entries(
    path: Path,
    *,
    selected_trials: list[Mapping[str, Any]],
    operations_by_trial: Mapping[str, list[dict[str, Any]]],
    event_index_by_operation: Mapping[str, int],
) -> list[dict[str, Any]]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ContainerExecutionError(f"cannot read checkpoint ledger: {path}") from exc
    _require(not raw or raw.endswith(b"\n"), "checkpoint ledger has a torn final row")
    try:
        entries = [
            json.loads(line.decode("utf-8"))
            for line in raw.splitlines()
            if line.strip()
        ]
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ContainerExecutionError("checkpoint ledger contains invalid JSON") from exc
    trial_by_key = {str(row["trial_key"]): row for row in selected_trials}
    seen: set[str] = set()
    for sequence, entry in enumerate(entries):
        _require(isinstance(entry, dict), "checkpoint entry must be an object")
        _require(
            entry.get("schema_version")
            == CONTAINER_EXECUTION_CHECKPOINT_ENTRY_SCHEMA_VERSION,
            "unsupported checkpoint entry schema_version",
        )
        _require(
            entry.get("entry_sha256")
            == _document_sha256(entry, "entry_sha256"),
            "checkpoint entry digest mismatch",
        )
        _require(
            entry.get("checkpoint_sequence") == sequence,
            "checkpoint sequence is not contiguous",
        )
        _require(
            type(entry.get("invocation_index")) is int
            and entry["invocation_index"] >= 0,
            "checkpoint invocation index is invalid",
        )
        _require(
            type(entry.get("invocation_elapsed_ms_at_checkpoint")) in (int, float)
            and entry["invocation_elapsed_ms_at_checkpoint"] >= 0.0,
            "checkpoint invocation elapsed time is invalid",
        )
        trial_key = entry.get("trial_key")
        _require(trial_key in trial_by_key, "checkpoint contains an unplanned trial")
        _require(trial_key not in seen, "checkpoint contains a duplicate trial")
        seen.add(str(trial_key))
        record = entry.get("record")
        events = entry.get("events")
        _require(isinstance(record, dict), "checkpoint record is missing")
        _require(isinstance(events, list), "checkpoint events are missing")
        _require(
            record.get("invocation_index") == entry["invocation_index"]
            and all(
                event.get("invocation_index") == entry["invocation_index"]
                for event in events
            ),
            "checkpoint invocation provenance changed",
        )
        expected_trial = trial_by_key[str(trial_key)]
        for field in _TRIAL_IDENTITY_FIELDS:
            _require(
                record.get(field) == expected_trial.get(field),
                f"checkpoint trial identity changed: {field}",
            )
        expected_operations = operations_by_trial[str(trial_key)]
        _require(
            len(events) == len(expected_operations),
            "checkpoint trial operation count changed",
        )
        for event, operation in zip(events, expected_operations, strict=True):
            _require(isinstance(event, dict), "checkpoint event must be an object")
            _require(
                event.get("schema_version")
                == CONTAINER_EXECUTION_EVENT_SCHEMA_VERSION,
                "unsupported checkpoint event schema_version",
            )
            for field in (
                "trial_key",
                "operation_key",
                "operation_id",
                "operation_kind",
            ):
                _require(
                    event.get(field) == operation.get(field),
                    f"checkpoint operation identity changed: {field}",
                )
            _require(
                event.get("event_index")
                == event_index_by_operation[operation["operation_key"]],
                "checkpoint event index changed",
            )
            _require(type(event.get("executed")) is bool, "event executed flag changed")
            _require(event.get("telemetry_complete") is True, "event telemetry incomplete")
            _require(event.get("credentials_recorded") is False, "event recorded credentials")
        _validate_record_events(record, events)
    return entries


def _append_checkpoint_entry(path: Path, entry: Mapping[str, Any]) -> None:
    try:
        existing = path.read_bytes()
    except OSError as exc:
        raise ContainerExecutionError("cannot read checkpoint ledger for update") from exc
    _require(
        not existing or existing.endswith(b"\n"),
        "checkpoint ledger has a torn final row",
    )
    # Whole-ledger replacement is intentional.  A process interruption during
    # the write leaves either the previous complete prefix or the new complete
    # prefix; it cannot expose a partially appended JSON row as canonical.
    _atomic_write(path, existing + _canonical_bytes(entry) + b"\n")


def _request_json(
    url: str,
    *,
    payload: Mapping[str, Any] | None,
    timeout_seconds: float,
) -> dict[str, Any]:
    body = None if payload is None else _canonical_bytes(payload)
    request = Request(
        url,
        data=body,
        method="GET" if body is None else "POST",
        headers=(
            {}
            if body is None
            else {"Content-Type": "application/json"}
        ),
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            raw = response.read(2 * 1024 * 1024 + 1)
            _require(len(raw) <= 2 * 1024 * 1024, "node response is too large")
            _require(response.status == 200, f"node returned HTTP {response.status}")
    except HTTPError as exc:
        detail = exc.read(2048).decode("utf-8", errors="replace")
        raise ContainerExecutionError(
            f"node returned HTTP {exc.code}: {detail}"
        ) from exc
    except URLError as exc:
        raise ContainerExecutionError(f"node is unavailable: {exc.reason}") from exc
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ContainerExecutionError("node returned invalid JSON") from exc
    _require(isinstance(value, dict), "node response must be an object")
    return value


def _endpoint_map(
    compose_root: Path,
    override: Mapping[str, Mapping[str, Any]] | None,
) -> tuple[dict[str, Mapping[str, Any]], str, str]:
    if override is None:
        raw = (compose_root / "container_endpoints.json").read_bytes()
        payload = json.loads(raw.decode("utf-8"))
        source = "frozen-compose-package"
    else:
        payload = {
            "endpoints": {key: dict(value) for key, value in override.items()}
        }
        raw = _canonical_bytes(payload)
        source = "explicit-test-override"
    rows = payload.get("endpoints")
    _require(isinstance(rows, dict) and len(rows) == 8, "exactly eight endpoints required")
    result: dict[str, Mapping[str, Any]] = {}
    for node_id, row in rows.items():
        _require(isinstance(row, Mapping), f"endpoint {node_id} must be an object")
        for field in (
            "container_name",
            "container_url",
            "host_health_url",
            "host_operation_url",
        ):
            _require(
                isinstance(row.get(field), str) and bool(row[field]),
                f"endpoint {node_id} lacks {field}",
            )
        result[str(node_id)] = row
    return result, _sha256_bytes(raw), source


def _binding(operation: Mapping[str, Any]) -> tuple[str | None, str | None]:
    resource = operation.get("resource_adapter")
    if isinstance(resource, Mapping):
        return str(resource["resource_id"]), str(resource["resource_kind"])
    link = operation.get("link_adapter")
    if isinstance(link, Mapping):
        return str(link["link_id"]), "network"
    cache = operation.get("cache_adapter")
    if isinstance(cache, Mapping):
        return str(cache["cache_id"]), "cache"
    return None, None


def _admission_spec(
    operation: Mapping[str, Any],
    portable_operation: Mapping[str, Any],
) -> tuple[str, int] | None:
    """Return the frozen capacity gate for one executable operation."""

    resource = operation.get("resource_adapter")
    if isinstance(resource, Mapping):
        binding = portable_operation.get("resource_binding")
        _require(isinstance(binding, Mapping), "portable resource binding missing")
        _require(
            binding.get("resource_id") == resource.get("resource_id"),
            "portable and container resource bindings differ",
        )
        slots = binding.get("slots")
        _require(type(slots) is int and slots > 0, "resource slots must be positive")
        return f"resource:{binding['resource_id']}", slots
    link = operation.get("link_adapter")
    if isinstance(link, Mapping):
        binding = portable_operation.get("link_binding")
        _require(isinstance(binding, Mapping), "portable link binding missing")
        _require(
            binding.get("link_id") == link.get("link_id"),
            "portable and container link bindings differ",
        )
        slots = binding.get("slots")
        _require(type(slots) is int and slots > 0, "link slots must be positive")
        return f"link:{binding['link_id']}", slots
    cache = operation.get("cache_adapter")
    if isinstance(cache, Mapping):
        binding = portable_operation.get("cache_binding")
        _require(isinstance(binding, Mapping), "portable cache binding missing")
        _require(
            binding.get("cache_id") == cache.get("cache_id"),
            "portable and container cache bindings differ",
        )
        # Cache metadata mutation is serialized by the node runtime.  The
        # portable cache contract has capacity bytes but no independent slot
        # count, so one metadata admission slot is explicit here.
        return f"cache:{binding['cache_id']}", 1
    return None


def _execute_operation(
    operation: Mapping[str, Any],
    endpoints: Mapping[str, Mapping[str, Any]],
    *,
    timeout_seconds: float,
    run_started_ns: int,
    event_index: int,
    ready_ns: int | None = None,
    admitted_ns: int | None = None,
    queue_time_measurement: str = "not-observed-serial-driver",
    contention_key: str | None = None,
    resource_capacity_slots: int | None = None,
    queue_observed: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    node_id = str(operation["execution_node_id"])
    _require(node_id in endpoints, f"no endpoint for execution node {node_id}")
    request_payload = dict(operation)
    if operation["operation_kind"] == "network_transfer":
        destination = str(operation["destination_node_id"])
        _require(destination in endpoints, f"no endpoint for destination {destination}")
        request_payload["destination_url"] = endpoints[destination]["container_url"]
    request_started_ns = time.perf_counter_ns()
    if ready_ns is None:
        ready_ns = request_started_ns
    if admitted_ns is None:
        admitted_ns = request_started_ns
    result = _request_json(
        str(endpoints[node_id]["host_operation_url"]),
        payload=request_payload,
        timeout_seconds=timeout_seconds,
    )
    request_finished_ns = time.perf_counter_ns()
    _require(
        result.get("schema_version") == CONTAINER_NODE_RESULT_SCHEMA_VERSION,
        "node returned an unsupported result schema",
    )
    _require(result.get("operation_key") == operation["operation_key"], "operation key changed")
    _require(result.get("execution_node_id") == node_id, "execution node changed")
    _require(result.get("outcome_type") == "completed", "operation did not complete")
    _require(result.get("telemetry_complete") is True, "operation telemetry is incomplete")
    _require(result.get("credentials_recorded") is False, "node recorded credentials")
    _require(
        result.get("semantic_task_quality_evaluated") is False,
        "infrastructure node unexpectedly reported semantic quality",
    )
    resource_id, resource_kind = _binding(operation)
    event = {
        "schema_version": CONTAINER_EXECUTION_EVENT_SCHEMA_VERSION,
        "event_index": event_index,
        "trial_key": operation["trial_key"],
        "operation_key": operation["operation_key"],
        "operation_id": operation["operation_id"],
        "operation_kind": operation["operation_kind"],
        "executed": True,
        "skip_reason": None,
        "execution_node_id": node_id,
        "destination_node_id": operation["destination_node_id"],
        "resource_id": resource_id,
        "resource_kind": resource_kind,
        "ready_time_ms": (ready_ns - run_started_ns) / 1_000_000.0,
        "start_time_ms": (admitted_ns - run_started_ns) / 1_000_000.0,
        "end_time_ms": (request_finished_ns - run_started_ns) / 1_000_000.0,
        "queue_time_ms": (admitted_ns - ready_ns) / 1_000_000.0,
        "queue_time_measurement": queue_time_measurement,
        "queue_observed": queue_observed,
        "contention_key": contention_key,
        "resource_capacity_slots": resource_capacity_slots,
        "service_time_ms": result["service_time_ms"],
        "host_round_trip_ms": (
            request_finished_ns - request_started_ns
        ) / 1_000_000.0,
        "logical_bytes": result["logical_bytes"],
        "physical_bytes": result["physical_bytes"],
        "cache_result": result.get("cache_result"),
        "cache_evictions": result.get("cache_evictions", []),
        "payload_sha256": result.get("payload_sha256"),
        "application_shaping_target_ms": result.get(
            "application_shaping_target_ms"
        ),
        "operation_result_replayed": result.get("idempotent_replay") is True,
        "telemetry_complete": True,
        "credentials_recorded": False,
    }
    return event, result


def _skip_event(
    operation: Mapping[str, Any],
    *,
    reason: str,
    run_started_ns: int,
    event_index: int,
) -> dict[str, Any]:
    now_ms = (time.perf_counter_ns() - run_started_ns) / 1_000_000.0
    return {
        "schema_version": CONTAINER_EXECUTION_EVENT_SCHEMA_VERSION,
        "event_index": event_index,
        "trial_key": operation["trial_key"],
        "operation_key": operation["operation_key"],
        "operation_id": operation["operation_id"],
        "operation_kind": operation["operation_kind"],
        "executed": False,
        "skip_reason": reason,
        "execution_node_id": None,
        "destination_node_id": None,
        "resource_id": None,
        "resource_kind": None,
        "ready_time_ms": now_ms,
        "start_time_ms": now_ms,
        "end_time_ms": now_ms,
        "queue_time_ms": 0.0,
        "queue_time_measurement": "not-applicable-skipped",
        "queue_observed": False,
        "contention_key": None,
        "resource_capacity_slots": None,
        "service_time_ms": 0.0,
        "host_round_trip_ms": 0.0,
        "logical_bytes": 0,
        "physical_bytes": 0,
        "cache_result": None,
        "cache_evictions": [],
        "payload_sha256": None,
        "application_shaping_target_ms": None,
        "operation_result_replayed": False,
        "telemetry_complete": True,
        "credentials_recorded": False,
    }


def _record(
    trial: Mapping[str, Any],
    events: list[Mapping[str, Any]],
    *,
    trial_started_ns: int,
    trial_finished_ns: int,
    trial_admission_queue_ms: float = 0.0,
    trial_admission_slots: int = 1,
    concurrent_execution: bool = False,
    observed_active_trials_at_start: int = 1,
) -> dict[str, Any]:
    service: dict[str, float] = defaultdict(float)
    queue: dict[str, float] = defaultdict(float)
    for event in events:
        if event["executed"] and event["resource_kind"] is not None:
            service[str(event["resource_kind"])] += float(event["service_time_ms"])
            queue[str(event["resource_kind"])] += float(event["queue_time_ms"])
    active_execution_latency_ms = (
        trial_finished_ns - trial_started_ns
    ) / 1_000_000.0
    return {
        "schema_version": CONTAINER_INFRASTRUCTURE_RECORD_SCHEMA_VERSION,
        **{
            field: trial[field]
            for field in (
                "trial_key",
                "trial_id",
                "workflow_id",
                "task_id",
                "session_id",
                "order_index",
                "workload_id",
                "workload_class",
                "object_id",
                "task_type",
                "design_id",
                "executor_node_id",
                "repetition",
                "seed",
                "arrival_time_ms",
            )
        },
        "outcome_type": "completed",
        "telemetry_complete": True,
        "artifact_delivery_complete": True,
        "latency_ms": (
            trial_admission_queue_ms + active_execution_latency_ms
        ),
        "trial_admission_queue_ms": trial_admission_queue_ms,
        "active_execution_latency_ms": active_execution_latency_ms,
        "latency_origin": TRIAL_LATENCY_ORIGIN,
        "trial_admission_algorithm": TRIAL_ADMISSION_ALGORITHM,
        "trial_admission_slots": trial_admission_slots,
        # Retained as an explicitly deprecated alias for audit readers of the
        # first container-run schema.  New comparisons use the canonical name.
        "trial_dispatch_queue_ms": trial_admission_queue_ms,
        "concurrent_execution": concurrent_execution,
        "observed_active_trials_at_start": observed_active_trials_at_start,
        "event_count": len(events),
        "executed_event_count": sum(bool(row["executed"]) for row in events),
        "logical_bytes": sum(int(row["logical_bytes"]) for row in events),
        "physical_bytes": sum(int(row["physical_bytes"]) for row in events),
        "network_bytes": sum(
            int(row["physical_bytes"])
            for row in events
            if row["executed"] and row["operation_kind"] == "network_transfer"
        ),
        "resource_service_ms": dict(sorted(service.items())),
        "resource_queue_ms": dict(sorted(queue.items())),
        "task_success": None,
        "semantic_task_quality_evaluated": False,
        "quality_provenance": "unavailable-in-infrastructure-only-container-run",
        "container_emulated": True,
        "flowmesh_deployed": False,
        "llm_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def execute_local_container_plan(
    compose_package_dir: str | Path,
    portable_plan_dir: str | Path,
    *,
    output_dir: str | Path,
    trial_limit: int | None = None,
    trial_key: str | Iterable[str] | None = None,
    max_concurrency: int | None = None,
    request_timeout_seconds: float = 900.0,
    endpoint_override: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Execute or auditably resume trials against already-running services.

    A completed trial is appended and fsynced as one digest-bound checkpoint
    entry.  A partially executed trial is never recorded.  Resume is permitted
    only while all node runtime epochs remain unchanged, which ensures the
    node-side operation-idempotency state still covers any lost response.
    """

    _require(
        trial_limit is None or (type(trial_limit) is int and trial_limit > 0),
        "trial_limit must be a positive integer",
    )
    requested_trial_keys: list[str] | None
    if trial_key is None:
        requested_trial_keys = None
    elif isinstance(trial_key, str):
        requested_trial_keys = [trial_key]
    else:
        requested_trial_keys = list(trial_key)
        _require(bool(requested_trial_keys), "trial_key selection cannot be empty")
        _require(
            all(isinstance(key, str) and bool(key) for key in requested_trial_keys),
            "trial_key values must be non-empty strings",
        )
    if requested_trial_keys is not None:
        _require(
            len(requested_trial_keys) == len(set(requested_trial_keys)),
            "trial_key selection contains duplicates",
        )
    _require(
        trial_limit is None or requested_trial_keys is None,
        "trial_limit and trial_key are mutually exclusive",
    )
    _require(
        max_concurrency is None
        or (type(max_concurrency) is int and max_concurrency > 0),
        "max_concurrency must be a positive integer when supplied",
    )
    _require(
        requested_trial_keys is None
        or len(requested_trial_keys) > 1
        or max_concurrency in (None, 1),
        "a single trial_key requires max_concurrency=1",
    )
    _require(request_timeout_seconds > 0.0, "request timeout must be positive")
    compose_root = Path(compose_package_dir).resolve()
    compose = verify_local_container_compose(compose_root)
    portable_root = Path(portable_plan_dir).resolve()
    portable = verify_portable_execution_plan(portable_root)
    try:
        frozen_admission_slots = validate_trial_admission_contract(
            portable.get("trial_admission"),
            planned_trial_count=int(portable["planned_trial_count"]),
        )
    except TrialAdmissionError as exc:
        raise ContainerExecutionError(
            "portable plan does not freeze the shared trial-admission contract: "
            f"{exc}"
        ) from exc
    _require(
        compose["scenario_id"] == portable["scenario_id"],
        "Compose package and portable plan scenario differ",
    )
    operations = _read_jsonl(compose_root / "container_operations.jsonl")
    trials = _read_jsonl(portable_root / "trials.jsonl")
    portable_operations = _read_jsonl(portable_root / "operations.jsonl")
    _require(
        {row["trial_key"] for row in operations}
        == {row["trial_key"] for row in trials},
        "operation and trial sets differ",
    )
    portable_by_operation = {
        row["operation_key"]: row for row in portable_operations
    }
    _require(
        len(portable_by_operation) == len(portable_operations),
        "portable operation keys are not unique",
    )
    _require(
        set(portable_by_operation)
        == {row["operation_key"] for row in operations},
        "portable and container operation sets differ",
    )
    if requested_trial_keys is not None:
        requested_key_set = set(requested_trial_keys)
        selected_trials = [
            row for row in trials if row["trial_key"] in requested_key_set
        ]
        missing_keys = sorted(
            requested_key_set - {row["trial_key"] for row in selected_trials}
        )
        _require(
            not missing_keys,
            f"unknown trial_key values: {missing_keys}",
        )
    else:
        selected_trials = trials if trial_limit is None else trials[:trial_limit]
    requested_max_concurrency = (
        min(frozen_admission_slots, len(selected_trials))
        if max_concurrency is None
        else max_concurrency
    )
    complete_selection = len(selected_trials) == len(trials)
    _require(
        not complete_selection
        or requested_max_concurrency == frozen_admission_slots,
        "complete execution requires max_concurrency to equal the frozen "
        "trial_admission.slots",
    )
    _require(
        requested_max_concurrency <= frozen_admission_slots,
        "max_concurrency cannot exceed frozen trial_admission.slots",
    )
    _require(
        requested_trial_keys is None
        or len(requested_trial_keys) > 1
        or requested_max_concurrency == 1,
        "a single trial_key requires max_concurrency=1",
    )
    selected_keys = {row["trial_key"] for row in selected_trials}
    operations_by_trial: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for operation in operations:
        if operation["trial_key"] in selected_keys:
            operations_by_trial[operation["trial_key"]].append(operation)
    event_index_by_operation: dict[str, int] = {}
    next_event_index = 0
    for trial in selected_trials:
        trial_operations = operations_by_trial[trial["trial_key"]]
        _require(bool(trial_operations), f"trial has no operations: {trial['trial_key']}")
        for operation in trial_operations:
            event_index_by_operation[operation["operation_key"]] = next_event_index
            next_event_index += 1
    _require(bool(selected_trials), "trial selection cannot be empty")
    concurrent = requested_max_concurrency > 1 and len(selected_trials) > 1
    arrival_schedule_enforced = len(selected_trials) > 1
    effective_max_concurrency = min(
        requested_max_concurrency,
        len(selected_trials),
    )
    admission_specs: dict[str, tuple[str, int] | None] = {}
    gates: dict[str, _AdmissionGate] = {}
    if concurrent:
        for operation in operations:
            if operation["trial_key"] not in selected_keys:
                continue
            spec = _admission_spec(
                operation,
                portable_by_operation[operation["operation_key"]],
            )
            admission_specs[operation["operation_key"]] = spec
            if spec is None:
                continue
            key, slots = spec
            existing = gates.get(key)
            if existing is None:
                gates[key] = _AdmissionGate(key, slots)
            else:
                _require(
                    existing.slots == slots,
                    f"frozen capacity changed for {key}",
                )
    endpoints, endpoint_sha256, endpoint_source = _endpoint_map(
        compose_root,
        endpoint_override,
    )
    target = Path(output_dir).resolve()
    checkpoint_path = target / "container_run_checkpoint.json"
    ledger_path = target / "trial_checkpoint.jsonl"
    selected_trial_keys = [str(row["trial_key"]) for row in selected_trials]

    existing_contract: dict[str, Any] | None = None
    if target.exists():
        _require(target.is_dir(), f"container execution output is not a directory: {target}")
        actual = {path.name for path in target.iterdir() if path.is_file()}
        _require(
            actual <= _OUTPUT_FILES | {"SHA256SUMS"},
            "incomplete execution directory contains unexpected files",
        )
        _require(checkpoint_path.is_file(), "execution checkpoint contract is missing")
        _require(ledger_path.is_file(), "execution checkpoint ledger is missing")
        try:
            existing_contract = json.loads(
                checkpoint_path.read_text(encoding="utf-8")
            )
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ContainerExecutionError("execution checkpoint contract is invalid") from exc
        _require(
            isinstance(existing_contract, dict)
            and existing_contract.get("schema_version")
            == CONTAINER_EXECUTION_CHECKPOINT_SCHEMA_VERSION,
            "unsupported execution checkpoint schema_version",
        )
        _require(
            existing_contract.get("checkpoint_sha256")
            == _document_sha256(existing_contract, "checkpoint_sha256"),
            "execution checkpoint contract digest mismatch",
        )
        expected_static = {
            "scenario_id": compose["scenario_id"],
            "backend_id": compose["backend_id"],
            "portable_plan_sha256": portable["plan_sha256"],
            "endpoint_binding_sha256": endpoint_sha256,
            "endpoint_binding_source": endpoint_source,
            "selected_trial_keys": selected_trial_keys,
            "selected_trial_keys_sha256": _sha256_bytes(
                _canonical_bytes(selected_trial_keys)
            ),
            "requested_max_concurrency": requested_max_concurrency,
            "frozen_trial_admission_slots": frozen_admission_slots,
            "request_timeout_seconds": float(request_timeout_seconds),
            "execution_mode": "concurrent" if concurrent else "serial",
        }
        for field, expected in expected_static.items():
            _require(
                existing_contract.get(field) == expected,
                f"resume contract changed: {field}",
            )
        if (target / "SHA256SUMS").is_file():
            verify_container_execution(target)
            manifest = json.loads(
                (target / "container_run_manifest.json").read_text(encoding="utf-8")
            )
            return {
                **manifest,
                "output_dir": str(target),
                "completed_output_reused": True,
                "executed_this_invocation": 0,
                "checkpoint_reused_trial_count": manifest[
                    "executed_trial_count"
                ],
                "resume_performed": True,
            }

    runtime_epochs: dict[str, str] = {}
    for node_id in sorted(endpoints):
        health = _request_json(
            str(endpoints[node_id]["host_health_url"]),
            payload=None,
            timeout_seconds=min(request_timeout_seconds, 10.0),
        )
        _require(health.get("status") == "ok", f"node {node_id} is unhealthy")
        _require(health.get("node_id") == node_id, f"node {node_id} identity changed")
        _require(
            health.get("semantic_quality_enabled") is False,
            f"node {node_id} unexpectedly enables semantic quality",
        )
        epoch = health.get("runtime_epoch")
        _require(
            isinstance(epoch, str) and bool(epoch),
            f"node {node_id} has no runtime epoch",
        )
        runtime_epochs[node_id] = epoch

    expected_contract = _checkpoint_contract(
        scenario_id=str(compose["scenario_id"]),
        backend_id=str(compose["backend_id"]),
        portable_plan_sha256=str(portable["plan_sha256"]),
        endpoint_binding_sha256=endpoint_sha256,
        endpoint_binding_source=endpoint_source,
        selected_trial_keys=selected_trial_keys,
        requested_max_concurrency=requested_max_concurrency,
        frozen_trial_admission_slots=frozen_admission_slots,
        request_timeout_seconds=request_timeout_seconds,
        runtime_epochs=runtime_epochs,
    )
    if existing_contract is None:
        _require(not target.exists(), f"container execution output exists: {target}")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.mkdir()
        _atomic_write(checkpoint_path, _json_bytes(expected_contract))
        _atomic_write(ledger_path, b"")
    else:
        _require(
            existing_contract == expected_contract,
            "node runtime epoch changed; start a new output directory",
        )

    checkpoint_entries = _load_checkpoint_entries(
        ledger_path,
        selected_trials=selected_trials,
        operations_by_trial=operations_by_trial,
        event_index_by_operation=event_index_by_operation,
    )
    completed_keys = {str(row["trial_key"]) for row in checkpoint_entries}
    pending_trials = [
        row for row in selected_trials if row["trial_key"] not in completed_keys
    ]
    reused_trial_count = len(checkpoint_entries)
    invocation_index = (
        max((int(row["invocation_index"]) for row in checkpoint_entries), default=-1)
        + 1
    )

    run_started_ns = time.perf_counter_ns()
    minimum_arrival_ms = min(
        (float(trial["arrival_time_ms"]) for trial in pending_trials),
        default=0.0,
    )
    activity_lock = threading.Lock()
    checkpoint_lock = threading.Lock()
    admission_order_condition = threading.Condition()
    pending_admission_position = {
        str(trial["trial_key"]): position
        for position, trial in enumerate(pending_trials)
    }
    next_admission_position = 0
    active_trials = 0
    peak_active_trials = 0

    def run_trial(
        trial: Mapping[str, Any],
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        nonlocal active_trials, peak_active_trials, next_admission_position
        arrival_target_ns = run_started_ns
        if arrival_schedule_enforced:
            arrival_target_ns += int(
                (float(trial["arrival_time_ms"]) - minimum_arrival_ms)
                * 1_000_000.0
            )
            remaining_ns = arrival_target_ns - time.perf_counter_ns()
            if remaining_ns > 0:
                time.sleep(remaining_ns / 1_000_000_000.0)
        if concurrent:
            position = pending_admission_position[str(trial["trial_key"])]
            with admission_order_condition:
                while position != next_admission_position:
                    admission_order_condition.wait()
                trial_started_ns = time.perf_counter_ns()
                next_admission_position += 1
                admission_order_condition.notify_all()
        else:
            trial_started_ns = time.perf_counter_ns()
        dispatch_queue_ms = max(
            0.0,
            (trial_started_ns - arrival_target_ns) / 1_000_000.0,
        ) if arrival_schedule_enforced else 0.0
        with activity_lock:
            active_trials += 1
            peak_active_trials = max(peak_active_trials, active_trials)
            observed_active_trials_at_start = active_trials
        trial_events: list[dict[str, Any]] = []
        result_by_operation: dict[str, dict[str, Any]] = {}
        completed_operations: set[str] = set()
        try:
            for operation in operations_by_trial[trial["trial_key"]]:
                for dependency in operation["dependency_operation_keys"]:
                    _require(
                        dependency in completed_operations,
                        f"operation dependency is incomplete: {dependency}",
                    )
                condition = operation.get("condition")
                execute = True
                skip_reason = None
                if condition is not None:
                    decision_key = condition["cache_operation_key"]
                    _require(
                        decision_key in result_by_operation,
                        f"condition result is unavailable: {decision_key}",
                    )
                    observed = result_by_operation[decision_key].get("cache_result")
                    expected = condition["equals"]
                    execute = observed == expected
                    if not execute:
                        skip_reason = (
                            f"condition {condition['cache_operation_id']}={expected} "
                            f"did not match observed {observed}"
                        )
                event_index = event_index_by_operation[operation["operation_key"]]
                if execute:
                    gate = None
                    ready_ns = None
                    admitted_ns = None
                    queue_observed = False
                    queue_measurement = "not-observed-serial-driver"
                    contention_key = None
                    capacity_slots = None
                    if concurrent:
                        ready_ns = time.perf_counter_ns()
                        spec = admission_specs[operation["operation_key"]]
                        if spec is None:
                            admitted_ns = ready_ns
                            queue_measurement = "not-applicable-unbound-operation"
                        else:
                            contention_key, capacity_slots = spec
                            gate = gates[contention_key]
                            admitted_ns, queue_observed = gate.acquire(ready_ns)
                            queue_measurement = "driver-enforced-frozen-slot-admission"
                    try:
                        event, result = _execute_operation(
                            operation,
                            endpoints,
                            timeout_seconds=request_timeout_seconds,
                            run_started_ns=run_started_ns,
                            event_index=event_index,
                            ready_ns=ready_ns,
                            admitted_ns=admitted_ns,
                            queue_time_measurement=queue_measurement,
                            contention_key=contention_key,
                            resource_capacity_slots=capacity_slots,
                            queue_observed=queue_observed,
                        )
                    finally:
                        if gate is not None:
                            gate.release()
                    result_by_operation[operation["operation_key"]] = result
                else:
                    event = _skip_event(
                        operation,
                        reason=str(skip_reason),
                        run_started_ns=run_started_ns,
                        event_index=event_index,
                    )
                    result_by_operation[operation["operation_key"]] = {
                        "outcome_type": "skipped",
                        "cache_result": None,
                    }
                trial_events.append(event)
                completed_operations.add(operation["operation_key"])
            trial_finished_ns = time.perf_counter_ns()
            for event in trial_events:
                event["invocation_index"] = invocation_index
            record = _record(
                trial,
                trial_events,
                trial_started_ns=trial_started_ns,
                trial_finished_ns=trial_finished_ns,
                trial_admission_queue_ms=dispatch_queue_ms,
                trial_admission_slots=frozen_admission_slots,
                concurrent_execution=concurrent,
                observed_active_trials_at_start=observed_active_trials_at_start,
            )
            record["invocation_index"] = invocation_index
            with checkpoint_lock:
                entry = {
                    "schema_version": (
                        CONTAINER_EXECUTION_CHECKPOINT_ENTRY_SCHEMA_VERSION
                    ),
                    "checkpoint_sequence": len(checkpoint_entries),
                    "invocation_index": invocation_index,
                    "invocation_elapsed_ms_at_checkpoint": (
                        time.perf_counter_ns() - run_started_ns
                    ) / 1_000_000.0,
                    "trial_key": trial["trial_key"],
                    "record": record,
                    "events": trial_events,
                }
                entry["entry_sha256"] = _document_sha256(
                    entry,
                    "entry_sha256",
                )
                _append_checkpoint_entry(ledger_path, entry)
                checkpoint_entries.append(entry)
            return record, trial_events
        finally:
            with activity_lock:
                active_trials -= 1

    if concurrent and pending_trials:
        with ThreadPoolExecutor(
            max_workers=min(effective_max_concurrency, len(pending_trials)),
            thread_name_prefix="pathfinder-container-trial",
        ) as executor:
            pending_iterator = iter(pending_trials)
            futures = {
                executor.submit(run_trial, trial)
                for trial in (
                    next(pending_iterator, None)
                    for _ in range(effective_max_concurrency)
                )
                if trial is not None
            }
            while futures:
                completed, futures = wait(
                    futures,
                    return_when=FIRST_COMPLETED,
                )
                for future in completed:
                    future.result()
                for _ in completed:
                    trial = next(pending_iterator, None)
                    if trial is not None:
                        futures.add(executor.submit(run_trial, trial))
    else:
        for trial in pending_trials:
            run_trial(trial)

    checkpoint_entries = _load_checkpoint_entries(
        ledger_path,
        selected_trials=selected_trials,
        operations_by_trial=operations_by_trial,
        event_index_by_operation=event_index_by_operation,
    )
    _require(
        len(checkpoint_entries) == len(selected_trials),
        "execution ended without checkpointing every selected trial",
    )
    entry_by_key = {str(row["trial_key"]): row for row in checkpoint_entries}
    records = [entry_by_key[key]["record"] for key in selected_trial_keys]
    events = sorted(
        (
            event
            for entry in checkpoint_entries
            for event in entry["events"]
        ),
        key=lambda event: int(event["event_index"]),
    )
    run_finished_ns = time.perf_counter_ns()
    complete = len(selected_trials) == len(trials)
    event_bytes = _jsonl_bytes(events)
    record_bytes = _jsonl_bytes(records)
    documents = {
        "operation_results.jsonl": event_bytes,
        "infrastructure_records.jsonl": record_bytes,
        "container_run_checkpoint.json": checkpoint_path.read_bytes(),
        "trial_checkpoint.jsonl": ledger_path.read_bytes(),
    }
    invocation_elapsed: dict[int, float] = defaultdict(float)
    for entry in checkpoint_entries:
        index = int(entry["invocation_index"])
        invocation_elapsed[index] = max(
            invocation_elapsed[index],
            float(entry["invocation_elapsed_ms_at_checkpoint"]),
        )
    cumulative_checkpoint_elapsed_ms = sum(invocation_elapsed.values())
    observed_peak = max(
        (int(record.get("observed_active_trials_at_start", 1)) for record in records),
        default=1,
    )
    manifest = {
        "schema_version": CONTAINER_EXECUTION_MANIFEST_SCHEMA_VERSION,
        "status": "COMPLETE_INFRASTRUCTURE_ONLY" if complete else "PARTIAL_SMOKE",
        "backend_id": compose["backend_id"],
        "scenario_id": compose["scenario_id"],
        "portable_plan_sha256": portable["plan_sha256"],
        "endpoint_binding_sha256": endpoint_sha256,
        "endpoint_binding_source": endpoint_source,
        "planned_trial_count": len(trials),
        "executed_trial_count": len(records),
        "operation_result_count": len(events),
        "execution_mode": "concurrent" if concurrent else "serial",
        "requested_max_concurrency": requested_max_concurrency,
        "effective_max_concurrency": effective_max_concurrency,
        "trial_admission": portable["trial_admission"],
        "trial_admission_queue_measured": True,
        "latency_origin": TRIAL_LATENCY_ORIGIN,
        "latency_includes_trial_admission_queue": True,
        "total_trial_admission_queue_ms": sum(
            float(row["trial_admission_queue_ms"]) for row in records
        ),
        "admission_timing_comparable_across_backends": (
            len(invocation_elapsed) == 1
        ),
        "peak_active_trials": observed_peak,
        "arrival_schedule_enforced": arrival_schedule_enforced,
        "serial_driver": not concurrent,
        "contention_measured": concurrent,
        "queue_time_measured": concurrent,
        "queue_measurement": (
            "driver-enforced-frozen-slot-admission"
            if concurrent
            else "not-observed-serial-driver"
        ),
        "queued_operation_count": sum(
            row.get("queue_observed") is True for row in events
        ),
        "total_queue_time_ms": sum(float(row["queue_time_ms"]) for row in events),
        "observed_contention": any(
            row.get("queue_observed") is True for row in events
        ),
        "payload_mode": "deterministic-size-preserving-fixture",
        "semantic_task_quality_evaluated": False,
        "configured_cost_evaluated": False,
        "elapsed_ms": cumulative_checkpoint_elapsed_ms,
        "elapsed_ms_measurement": (
            "sum-of-last-durable-checkpoint-elapsed-time-per-invocation"
        ),
        "finalization_elapsed_ms": (
            run_finished_ns - run_started_ns
        ) / 1_000_000.0,
        "checkpoint_schema_version": (
            CONTAINER_EXECUTION_CHECKPOINT_SCHEMA_VERSION
        ),
        "checkpoint_entry_schema_version": (
            CONTAINER_EXECUTION_CHECKPOINT_ENTRY_SCHEMA_VERSION
        ),
        "checkpoint_sha256": expected_contract["checkpoint_sha256"],
        "runtime_epochs_sha256": expected_contract["runtime_epochs_sha256"],
        "checkpoint_trial_count": len(checkpoint_entries),
        "checkpoint_reused_trial_count": reused_trial_count,
        "executed_this_invocation": len(pending_trials),
        "invocation_count": len(invocation_elapsed),
        "resume_performed": reused_trial_count > 0,
        "partial_trials_promoted": False,
        "operation_idempotency_required": True,
        "resume_requires_same_runtime_epoch": True,
        "docker_called_by_runner": False,
        "services_started_by_runner": False,
        "external_node_services_called": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
        "limitations": [
            (
                "queue time is driver-enforced admission against frozen resource "
                "and link slots; node-internal, operating-system, and Docker "
                "network queues are not separately decomposed"
                if concurrent
                else "serial driver does not measure resource contention or queueing"
            ),
            (
                "application shaping enforces an elapsed-time floor after real "
                "HTTP transfer; it is not packet-level shaping"
            ),
            "deterministic fixture bytes are not modality content",
            (
                "event time offsets are relative to each checkpoint invocation; "
                "invocation_index identifies that clock domain"
            ),
            "semantic task success and physical monetary cost are unavailable",
        ],
        "output_sha256": {
            name: _sha256_bytes(content)
            for name, content in sorted(documents.items())
        },
    }
    documents["container_run_manifest.json"] = _json_bytes(manifest)
    documents["SHA256SUMS"] = b"".join(
        f"{_sha256_bytes(content)}  {name}\n".encode("utf-8")
        for name, content in sorted(documents.items())
    )
    for name in sorted(_OUTPUT_FILES):
        if name in _CHECKPOINT_FILES:
            continue
        _atomic_write(target / name, documents[name])
    _atomic_write(target / "SHA256SUMS", documents["SHA256SUMS"])
    verify_container_execution(target)
    return {**manifest, "output_dir": str(target)}


def verify_container_execution(output_dir: str | Path) -> dict[str, Any]:
    """Verify a container execution output without contacting live nodes."""

    root = Path(output_dir).resolve()
    _require(root.is_dir(), f"container execution output is missing: {root}")
    try:
        manifest = json.loads(
            (root / "container_run_manifest.json").read_text(encoding="utf-8")
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ContainerExecutionError("container execution manifest is invalid") from exc
    schema_version = manifest.get("schema_version")
    _require(
        schema_version in (
            CONTAINER_EXECUTION_MANIFEST_SCHEMA_VERSION,
            CHECKPOINT_CONTAINER_EXECUTION_MANIFEST_SCHEMA_VERSION,
            LEGACY_CONTAINER_EXECUTION_MANIFEST_SCHEMA_VERSION,
        ),
        "unsupported container execution schema_version",
    )
    checkpoint_manifest = schema_version in (
        CONTAINER_EXECUTION_MANIFEST_SCHEMA_VERSION,
        CHECKPOINT_CONTAINER_EXECUTION_MANIFEST_SCHEMA_VERSION,
    )
    expected_files = _OUTPUT_FILES if checkpoint_manifest else _LEGACY_OUTPUT_FILES
    actual = {path.name for path in root.iterdir() if path.is_file()}
    _require(actual == expected_files | {"SHA256SUMS"}, "execution file set changed")
    checksums: dict[str, str] = {}
    for line in (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
        digest, separator, name = line.partition("  ")
        _require(separator == "  " and name in expected_files, "malformed checksum row")
        _require(name not in checksums, f"duplicate checksum: {name}")
        _require(
            _sha256_bytes((root / name).read_bytes()) == digest,
            f"container execution checksum mismatch: {name}",
        )
        checksums[name] = digest
    _require(set(checksums) == expected_files, "execution checksums are incomplete")
    _require(
        manifest.get("output_sha256")
        == {
            name: digest
            for name, digest in checksums.items()
            if name != "container_run_manifest.json"
        },
        "container execution manifest digests disagree",
    )
    records = _read_jsonl(root / "infrastructure_records.jsonl")
    events = _read_jsonl(root / "operation_results.jsonl")
    _require(
        len(records) == manifest.get("executed_trial_count"),
        "executed trial count changed",
    )
    _require(
        len(events) == manifest.get("operation_result_count"),
        "operation count changed",
    )
    _require(
        [row.get("event_index") for row in events] == list(range(len(events))),
        "operation event indexes are not contiguous",
    )
    if schema_version == CONTAINER_EXECUTION_MANIFEST_SCHEMA_VERSION:
        try:
            frozen_slots = validate_trial_admission_contract(
                manifest.get("trial_admission"),
                planned_trial_count=int(manifest.get("planned_trial_count", 0)),
            )
        except (TrialAdmissionError, TypeError, ValueError) as exc:
            raise ContainerExecutionError(str(exc)) from exc
        _require(
            manifest.get("latency_origin") == TRIAL_LATENCY_ORIGIN
            and manifest.get("latency_includes_trial_admission_queue") is True
            and manifest.get("trial_admission_queue_measured") is True,
            "container run does not use the shared latency/admission semantics",
        )
        _require(
            manifest.get("arrival_schedule_enforced")
            is (len(records) > 1),
            "trial arrival schedule enforcement changed",
        )
        _require(
            all(row.get("trial_admission_slots") == frozen_slots for row in records),
            "record trial admission slots differ from the frozen contract",
        )
        _require(
            manifest.get("total_trial_admission_queue_ms")
            == sum(float(row["trial_admission_queue_ms"]) for row in records),
            "total trial admission queue time changed",
        )
    if checkpoint_manifest:
        try:
            contract = json.loads(
                (root / "container_run_checkpoint.json").read_text(
                    encoding="utf-8"
                )
            )
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ContainerExecutionError("checkpoint contract is invalid") from exc
        expected_checkpoint_schema = (
            CONTAINER_EXECUTION_CHECKPOINT_SCHEMA_VERSION
            if schema_version == CONTAINER_EXECUTION_MANIFEST_SCHEMA_VERSION
            else LEGACY_CONTAINER_EXECUTION_CHECKPOINT_SCHEMA_VERSION
        )
        expected_entry_schema = (
            CONTAINER_EXECUTION_CHECKPOINT_ENTRY_SCHEMA_VERSION
            if schema_version == CONTAINER_EXECUTION_MANIFEST_SCHEMA_VERSION
            else LEGACY_CONTAINER_EXECUTION_CHECKPOINT_ENTRY_SCHEMA_VERSION
        )
        _require(
            isinstance(contract, dict)
            and contract.get("schema_version") == expected_checkpoint_schema,
            "unsupported checkpoint contract schema_version",
        )
        _require(
            contract.get("checkpoint_sha256")
            == _document_sha256(contract, "checkpoint_sha256"),
            "checkpoint contract digest mismatch",
        )
        _require(
            contract.get("selected_trial_keys_sha256")
            == _sha256_bytes(_canonical_bytes(contract.get("selected_trial_keys"))),
            "checkpoint trial selection digest mismatch",
        )
        _require(
            contract.get("runtime_epochs_sha256")
            == _sha256_bytes(_canonical_bytes(contract.get("runtime_epochs"))),
            "checkpoint runtime epoch digest mismatch",
        )
        _require(
            manifest.get("checkpoint_sha256") == contract["checkpoint_sha256"]
            and manifest.get("runtime_epochs_sha256")
            == contract["runtime_epochs_sha256"],
            "manifest checkpoint binding changed",
        )
        checkpoint_bytes = (root / "trial_checkpoint.jsonl").read_bytes()
        _require(
            not checkpoint_bytes or checkpoint_bytes.endswith(b"\n"),
            "checkpoint ledger has a torn final row",
        )
        try:
            checkpoint_entries = [
                json.loads(line.decode("utf-8"))
                for line in checkpoint_bytes.splitlines()
                if line.strip()
            ]
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ContainerExecutionError("checkpoint ledger contains invalid JSON") from exc
        seen: set[str] = set()
        for sequence, entry in enumerate(checkpoint_entries):
            _require(isinstance(entry, dict), "checkpoint entry must be an object")
            _require(
                entry.get("schema_version") == expected_entry_schema,
                "unsupported checkpoint entry schema_version",
            )
            _require(
                entry.get("entry_sha256")
                == _document_sha256(entry, "entry_sha256"),
                "checkpoint entry digest mismatch",
            )
            _require(
                entry.get("checkpoint_sequence") == sequence,
                "checkpoint sequence is not contiguous",
            )
            key = entry.get("trial_key")
            _require(
                isinstance(key, str) and key not in seen,
                "checkpoint trial key is invalid or duplicated",
            )
            seen.add(key)
            _require(
                isinstance(entry.get("record"), dict)
                and isinstance(entry.get("events"), list),
                "checkpoint entry payload is incomplete",
            )
            _require(
                entry["record"].get("trial_key") == key
                and all(event.get("trial_key") == key for event in entry["events"]),
                "checkpoint entry mixes trial identities",
            )
            _require(
                entry["record"].get("invocation_index")
                == entry.get("invocation_index")
                and all(
                    event.get("invocation_index")
                    == entry.get("invocation_index")
                    for event in entry["events"]
                ),
                "checkpoint invocation provenance changed",
            )
            _validate_record_events(entry["record"], entry["events"])
        selected_keys = contract.get("selected_trial_keys")
        _require(isinstance(selected_keys, list), "checkpoint trial selection is invalid")
        entry_by_key = {entry["trial_key"]: entry for entry in checkpoint_entries}
        _require(set(entry_by_key) == set(selected_keys), "checkpoint trial set is incomplete")
        _require(
            records == [entry_by_key[key]["record"] for key in selected_keys],
            "final records differ from durable checkpoints",
        )
        checkpoint_events = sorted(
            (
                event
                for entry in checkpoint_entries
                for event in entry["events"]
            ),
            key=lambda event: int(event["event_index"]),
        )
        _require(events == checkpoint_events, "final events differ from durable checkpoints")
        _require(
            manifest.get("checkpoint_trial_count") == len(checkpoint_entries),
            "checkpoint trial count changed",
        )
    execution_mode = manifest.get("execution_mode", "serial")
    _require(
        execution_mode in ("serial", "concurrent"),
        "unsupported container execution mode",
    )
    for event in events:
        _require(
            event.get("schema_version") == CONTAINER_EXECUTION_EVENT_SCHEMA_VERSION,
            "unsupported operation event schema_version",
        )
        _require(type(event.get("executed")) is bool, "event executed flag changed")
        queue_ms = event.get("queue_time_ms")
        _require(
            type(queue_ms) in (int, float) and queue_ms >= 0.0,
            "event queue time is invalid",
        )
        if event.get("queue_observed") is True:
            _require(event["executed"] is True, "skipped event claims queueing")
            _require(queue_ms > 0.0, "queued event has no measured wait")
            _require(
                isinstance(event.get("contention_key"), str)
                and bool(event["contention_key"]),
                "queued event has no contention key",
            )
    if execution_mode == "concurrent":
        _require(manifest.get("serial_driver") is False, "concurrent run is serial")
        _require(
            manifest.get("contention_measured") is True
            and manifest.get("queue_time_measured") is True,
            "concurrent queue measurement is not explicit",
        )
        _require(
            manifest.get("arrival_schedule_enforced") is True,
            "concurrent arrival schedule was not enforced",
        )
        effective = manifest.get("effective_max_concurrency")
        peak = manifest.get("peak_active_trials")
        _require(
            type(effective) is int and effective > 1,
            "concurrent effective_max_concurrency is invalid",
        )
        _require(
            type(peak) is int and 1 <= peak <= effective,
            "concurrent peak_active_trials is invalid",
        )
        queued_count = sum(row.get("queue_observed") is True for row in events)
        _require(
            manifest.get("queued_operation_count") == queued_count,
            "queued operation count changed",
        )
        queue_total = sum(float(row["queue_time_ms"]) for row in events)
        _require(
            manifest.get("total_queue_time_ms") == queue_total,
            "total queue time changed",
        )
        _require(
            manifest.get("observed_contention") is (queued_count > 0),
            "observed contention flag changed",
        )
    events_by_trial: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        events_by_trial[str(event.get("trial_key"))].append(event)
    for record in records:
        _validate_record_events(
            record,
            events_by_trial[str(record.get("trial_key"))],
        )
        if execution_mode == "concurrent":
            _require(
                record.get("concurrent_execution") is True,
                "concurrent record lost execution provenance",
            )
    return {
        "status": "VERIFIED",
        "run_status": manifest["status"],
        "scenario_id": manifest["scenario_id"],
        "executed_trial_count": manifest["executed_trial_count"],
        "operation_result_count": manifest["operation_result_count"],
        "semantic_task_quality_evaluated": False,
        "eligible_for_scientific_claims": False,
    }
