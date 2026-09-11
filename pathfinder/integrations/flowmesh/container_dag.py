"""FlowMesh orchestration for a small, physical container-operation DAG.

This module deliberately keeps the control plane and the physical data plane
separate.  FlowMesh schedules three API tasks with explicit dependencies;
each task calls an already-running Pathfinder container node.  The container
nodes perform bounded fixture storage, HTTP transfer, and CPU work.  No LLM
is called and no semantic task-quality conclusion is produced.

The implementation is intended for an integration smoke, not for the 64-trial
simulator experiment.  It selects an existing linear chain from the frozen
container-operation contract rather than inventing new operation attributes.
"""

from __future__ import annotations

import json
import math
import os
import re
import shutil
import tempfile
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from ...simulator.container_contract import (
    CONTAINER_NODE_RESULT_SCHEMA_VERSION,
    CONTAINER_OPERATION_LEGACY_SCHEMA_VERSION,
    CONTAINER_OPERATION_SCHEMA_VERSION,
)
from .adapter import FlowMeshRunError, extract_api_executor_result
from .contracts import (
    FlowMeshClientProtocol,
    FlowMeshSettings,
    SubmittedWorkflow,
    TerminalWorkflow,
)
from .preflight import describe_pinned_worker
from .redaction import redact_secrets


FLOWMESH_CONTAINER_DAG_PLAN_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-operation-dag-plan/v1alpha2"
)
#: v1alpha1 plans have no planner-controlled API timeout. They stay
#: verifiable under the fixed 120 s the planner emitted at the time; they are
#: never rewritten, and the legacy timeout is reported rather than assumed.
FLOWMESH_CONTAINER_DAG_PLAN_LEGACY_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-operation-dag-plan/v1alpha1"
)
#: The API timeout the v1alpha1 planner hard-coded into every task.
LEGACY_API_TASK_TIMEOUT_SECONDS = 120
DEFAULT_API_TASK_TIMEOUT_SECONDS = 120
FLOWMESH_CONTAINER_DAG_RUN_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-operation-dag-run/v1alpha3"
)
#: v1alpha2 records timing, but predates runtime-instance binding and the
#: corrected network timing provenance. It remains readable as historical
#: evidence and cannot be promoted to a v2 runtime-integrity result.
FLOWMESH_CONTAINER_DAG_RUN_TIMING_V1_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-operation-dag-run/v1alpha2"
)
#: v1alpha1 runs discarded every timing field. They stay readable and are
#: labelled timing-not-recorded rather than back-filled; a measurement that
#: was never preserved cannot be recovered later.
FLOWMESH_CONTAINER_DAG_RUN_LEGACY_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-operation-dag-run/v1alpha1"
)

TELEMETRY_PROVENANCE_LEGACY_VERSION = (
    "pathfinder.container-dag-telemetry/v1alpha1"
)
TELEMETRY_PROVENANCE_VERSION = "pathfinder.container-dag-telemetry/v1alpha2"

#: Exactly the timing fields that may be preserved. Payload digests, prompts,
#: and answers stay out of the artifact. Cache outcome fields are validated
#: separately and are preserved only for cache operations.
_TELEMETRY_TIMING_FIELDS = (
    "service_time_ms",
    "fixture_materialization_ms_excluded_from_storage_measurement",
    "application_shaping_target_ms",
)

#: What each preserved number actually is. Recorded in the artifact so a
#: later reader cannot mistake a configured target for a measurement.
TELEMETRY_FIELD_PROVENANCE_V1: dict[str, str] = {
    "service_time_ms": (
        "measured inside the container operation as the monotonic interval "
        "between the start and end of the operation body on the executing "
        "node; it excludes FlowMesh scheduling, queueing, and HTTP transport"
    ),
    "fixture_materialization_ms_excluded_from_storage_measurement": (
        "measured on the executing node while preparing the read fixture, "
        "before the timed operation body begins; it is deliberately EXCLUDED "
        "from service_time_ms and is reported separately so a storage "
        "measurement is never inflated by test-fixture setup"
    ),
    "application_shaping_target_ms": (
        "a CONFIGURED application-level shaping target derived from the "
        "frozen link adapter, not an independently measured network latency "
        "and not an observed round-trip time"
    ),
    "logical_bytes": (
        "the exact byte count declared by the frozen operation and confirmed "
        "by the container result"
    ),
    "physical_bytes": (
        "the exact byte count the container reported reading or transferring"
    ),
    "telemetry_complete": (
        "the container's own assertion that it reported a complete record; a "
        "false or missing value refuses the run artifact"
    ),
}

TELEMETRY_DISCLAIMERS_V1: tuple[str, ...] = (
    "No cross-container clock comparison is made: every timing value is a "
    "duration measured by one node against its own monotonic clock, and "
    "durations from different nodes are never subtracted or ordered.",
    "No queue time, scheduling delay, or end-to-end latency is claimed; "
    "those were not measured and are not derivable from these records.",
    "No network throughput is derived from bytes and service time: the "
    "transfer is application-shaped, so such a ratio would describe the "
    "shaper, not the link.",
    "This is infrastructure-conformance telemetry, not a performance result.",
)

#: v2 corrects the network timing scope and exposes the two node-side
#: components needed to audit it. The new fields are still deliberately
#: insufficient to claim end-to-end latency or physical-link throughput.
TELEMETRY_FIELD_PROVENANCE: dict[str, str] = {
    "service_time_ms": (
        "measured inside the container operation as the monotonic interval "
        "between the start and end of the operation body on the executing "
        "node; a network transfer includes source-side HTTP request/response "
        "work and any application-level shaping sleep, while fixture "
        "materialization remains excluded"
    ),
    "fixture_materialization_ms_excluded_from_storage_measurement": (
        "measured on the executing node while preparing the read fixture, "
        "before the timed operation body begins; it is deliberately EXCLUDED "
        "from service_time_ms and is reported separately so a storage "
        "measurement is never inflated by test-fixture setup"
    ),
    "application_shaping_target_ms": (
        "a CONFIGURED application-level shaping target derived from the "
        "frozen link adapter, not an independently measured network latency "
        "and not an observed round-trip time"
    ),
    "network_http_exchange_ms": (
        "for a network transfer only, the source node's observed HTTP "
        "connection, request, payload-send, response-read, and sink "
        "validation interval; it is not a physical-link latency measurement"
    ),
    "application_shaping_sleep_ms": (
        "for a network transfer only, the observed post-exchange sleep used "
        "to enforce the configured application-level target; it is not a "
        "network measurement"
    ),
    "runtime_epoch": (
        "the executing container runtime instance identifier, captured in "
        "the operation result and matched to pre- and post-run health probes"
    ),
    "destination_runtime_epoch": (
        "for a network transfer only, the sink container runtime instance "
        "identifier returned with the transfer acknowledgement and matched "
        "to the run health binding"
    ),
    "logical_bytes": (
        "the exact byte count declared by the frozen operation and confirmed "
        "by the container result"
    ),
    "physical_bytes": (
        "the exact byte count the container reported reading or transferring"
    ),
    "telemetry_complete": (
        "the container's own assertion that it reported a complete record; a "
        "false or missing value refuses the run artifact"
    ),
}

TELEMETRY_DISCLAIMERS: tuple[str, ...] = (
    "No cross-container clock comparison is made: every timing value is a "
    "duration measured by one node against its own monotonic clock, and "
    "durations from different nodes are never subtracted or ordered.",
    "No FlowMesh queue time, scheduling delay, worker-to-node request time, "
    "or end-to-end latency is claimed; those were not measured and are not "
    "derivable from these records.",
    "No network throughput is derived from bytes and service time: the "
    "transfer is application-shaped, so such a ratio would describe the "
    "shaper, not the link.",
    "Runtime epochs detect a container-instance change during this run; they "
    "do not authenticate a record or establish image provenance.",
    "This is infrastructure-conformance telemetry, not a performance result.",
)

_TELEMETRY_FIELD_PROVENANCE_BY_VERSION = {
    TELEMETRY_PROVENANCE_LEGACY_VERSION: TELEMETRY_FIELD_PROVENANCE_V1,
    TELEMETRY_PROVENANCE_VERSION: TELEMETRY_FIELD_PROVENANCE,
}
_TELEMETRY_DISCLAIMERS_BY_VERSION = {
    TELEMETRY_PROVENANCE_LEGACY_VERSION: TELEMETRY_DISCLAIMERS_V1,
    TELEMETRY_PROVENANCE_VERSION: TELEMETRY_DISCLAIMERS,
}
_RUNTIME_EPOCH = re.compile(r"[0-9a-f]{32}")

_PLAN_FILES = {
    "flowmesh-container-dag-plan.json",
    "flowmesh-container-dag-workflow-template.json",
}
_RUN_FILES = {
    "flowmesh-container-dag-run.json",
    "flowmesh-container-dag-submission.json",
    "flowmesh-container-dag-task-results.jsonl",
}
_STEP_NAMES = ("storage-read", "network-transfer", "compute")
_STEP_KINDS = ("storage_read", "network_transfer", "compute")
_PHYSICAL_IO_OPERATION_KINDS = frozenset(
    {"storage_read", "cache_read", "network_transfer"}
)
_CACHE_OPERATION_KINDS = frozenset(
    {"cache_lookup", "cache_read", "cache_insert"}
)


class FlowMeshContainerDagError(FlowMeshRunError):
    """Raised when a container-operation DAG cannot be safely run."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise FlowMeshContainerDagError(message)


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


def _document_sha256(value: Mapping[str, Any], field: str) -> str:
    copied = dict(value)
    copied.pop(field, None)
    return _sha256_bytes(_canonical_bytes(copied))


def _text(value: Any, field: str) -> str:
    _require(
        isinstance(value, str) and bool(value.strip()),
        f"{field} must be a non-empty string",
    )
    return value.strip()


def _runtime_epoch(value: Any, field: str) -> str:
    epoch = _text(value, field)
    _require(
        _RUNTIME_EPOCH.fullmatch(epoch) is not None,
        f"{field} must be a lowercase runtime epoch",
    )
    return epoch


def _telemetry_contract(
    provenance_version: str,
) -> tuple[Mapping[str, str], tuple[str, ...]]:
    fields = _TELEMETRY_FIELD_PROVENANCE_BY_VERSION.get(provenance_version)
    disclaimers = _TELEMETRY_DISCLAIMERS_BY_VERSION.get(provenance_version)
    _require(
        fields is not None and disclaimers is not None,
        "unsupported container telemetry provenance",
    )
    return fields, disclaimers


def _required_runtime_node_ids(
    operations: Sequence[Mapping[str, Any]],
) -> tuple[str, ...]:
    required = {
        _text(row.get("execution_node_id"), "execution_node_id")
        for row in operations
    }
    required.update(
        _text(row.get("destination_node_id"), "destination_node_id")
        for row in operations
        if row.get("operation_kind") == "network_transfer"
    )
    _require(bool(required), "runtime binding needs at least one node")
    return tuple(sorted(required))


def _health_url(base_url: str) -> str:
    base = _text(base_url, "node API URL").rstrip("/")
    parsed = urlsplit(base)
    _require(
        parsed.scheme == "http" and parsed.hostname is not None,
        "node API URL must be an absolute http URL",
    )
    _require(
        parsed.username is None and parsed.password is None,
        "node API URL must not contain credentials",
    )
    return base + "/healthz"


def _probe_container_runtime_epochs(
    node_api_urls: Mapping[str, str],
    operations: Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    """Read the v2 instance identity from every node a run can touch.

    This helper is called only by live runners, never by a planner or an
    offline verifier.  A task result independently carries the same epoch,
    preventing a host-side health probe from being mistaken for proof of the
    endpoint actually reached by a FlowMesh worker.
    """

    expected: dict[str, str] = {}
    for node_id in _required_runtime_node_ids(operations):
        _require(node_id in node_api_urls, f"no API URL was supplied for runtime node {node_id}")
        url = _health_url(_text(node_api_urls[node_id], f"node API URL for {node_id}"))
        try:
            with urlopen(Request(url, method="GET"), timeout=10.0) as response:
                raw = response.read(128 * 1024 + 1)
                _require(len(raw) <= 128 * 1024, "container health response is too large")
                _require(response.status == 200, f"container health returned HTTP {response.status}")
        except HTTPError as exc:
            raise FlowMeshContainerDagError(
                f"container node {node_id} health returned HTTP {exc.code}"
            ) from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise FlowMeshContainerDagError(
                "container node "
                f"{node_id} health is unavailable: {getattr(exc, 'reason', exc)}"
            ) from exc
        try:
            health = json.loads(raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise FlowMeshContainerDagError(
                f"container node {node_id} health is invalid JSON"
            ) from exc
        _require(isinstance(health, Mapping), "container health must be an object")
        _require(health.get("status") == "ok", f"container node {node_id} is unhealthy")
        _require(health.get("node_id") == node_id, f"container node {node_id} identity changed")
        _require(
            health.get("operation_result_schema_version")
            == CONTAINER_NODE_RESULT_SCHEMA_VERSION,
            f"container node {node_id} does not support the v2 result contract",
        )
        _require(
            health.get("semantic_quality_enabled") is False,
            f"container node {node_id} unexpectedly enables semantic quality",
        )
        expected[node_id] = _runtime_epoch(
            health.get("runtime_epoch"), f"container node {node_id} runtime_epoch"
        )
    return dict(sorted(expected.items()))


def _validate_runtime_epochs(
    value: Mapping[str, Any],
    operations: Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    required = _required_runtime_node_ids(operations)
    _require(
        set(value) == set(required),
        "runtime epoch binding node set changed",
    )
    return {
        node_id: _runtime_epoch(value[node_id], f"runtime epoch for {node_id}")
        for node_id in required
    }


def _runtime_epoch_binding(
    *,
    plan_sha256: str,
    node_api_urls: Mapping[str, str],
    operations: Sequence[Mapping[str, Any]],
    before: Mapping[str, Any],
    after: Mapping[str, Any],
) -> dict[str, Any]:
    before_epochs = _validate_runtime_epochs(before, operations)
    after_epochs = _validate_runtime_epochs(after, operations)
    _require(
        before_epochs == after_epochs,
        "container runtime epoch changed during the FlowMesh run",
    )
    urls = {
        node_id: _text(node_api_urls[node_id], f"node API URL for {node_id}")
        for node_id in _required_runtime_node_ids(operations)
    }
    return {
        "schema_version": "pathfinder.flowmesh-container-runtime-binding/v1alpha1",
        "plan_sha256": _text(plan_sha256, "plan_sha256"),
        "node_api_urls_sha256": _sha256_bytes(_canonical_bytes(dict(sorted(urls.items())))),
        "node_runtime_epochs_before": before_epochs,
        "node_runtime_epochs_after": after_epochs,
        "operation_result_schema_version": CONTAINER_NODE_RESULT_SCHEMA_VERSION,
        "runtime_epoch_binding_required": True,
        "all_runtime_epochs_stable": True,
        "credentials_recorded": False,
    }


def _verify_runtime_epoch_binding(
    binding: Any,
    *,
    plan_sha256: Any,
    node_api_urls: Mapping[str, Any],
    operations: Sequence[Mapping[str, Any]],
    rows: Sequence[Mapping[str, Any]],
) -> None:
    _require(isinstance(binding, Mapping), "container runtime epoch binding is missing")
    _require(
        binding.get("schema_version")
        == "pathfinder.flowmesh-container-runtime-binding/v1alpha1",
        "unsupported container runtime epoch binding schema",
    )
    _require(binding.get("plan_sha256") == plan_sha256, "runtime epoch binding plan changed")
    required = _required_runtime_node_ids(operations)
    urls = {
        node_id: _text(node_api_urls[node_id], f"node API URL for {node_id}")
        for node_id in required
    }
    _require(
        binding.get("node_api_urls_sha256")
        == _sha256_bytes(_canonical_bytes(dict(sorted(urls.items())))),
        "runtime epoch binding endpoint map changed",
    )
    before_value = binding.get("node_runtime_epochs_before")
    after_value = binding.get("node_runtime_epochs_after")
    _require(
        isinstance(before_value, Mapping) and isinstance(after_value, Mapping),
        "runtime epoch binding epoch maps are missing",
    )
    before = _validate_runtime_epochs(before_value, operations)
    after = _validate_runtime_epochs(after_value, operations)
    _require(before == after, "runtime epoch binding records a changed node epoch")
    _require(
        binding.get("operation_result_schema_version")
        == CONTAINER_NODE_RESULT_SCHEMA_VERSION,
        "runtime epoch binding result schema changed",
    )
    _require(binding.get("runtime_epoch_binding_required") is True, "runtime epoch binding is not required")
    _require(binding.get("all_runtime_epochs_stable") is True, "runtime epoch binding is not stable")
    _require(binding.get("credentials_recorded") is False, "runtime epoch binding records credentials")
    for row in rows:
        source = _text(row.get("execution_node_id"), "task result execution_node_id")
        _require(
            _runtime_epoch(row.get("runtime_epoch"), "task result runtime_epoch")
            == before[source],
            "task result runtime epoch does not match the run binding",
        )
        if row.get("operation_kind") == "network_transfer":
            destination = _text(
                row.get("destination_node_id"), "task result destination_node_id"
            )
            _require(
                _runtime_epoch(
                    row.get("destination_runtime_epoch"),
                    "task result destination_runtime_epoch",
                )
                == before[destination],
                "network sink runtime epoch does not match the run binding",
            )
        else:
            _require(
                row.get("destination_runtime_epoch") is None,
                "non-network task result names a destination runtime epoch",
            )


def _copy_operation(value: Mapping[str, Any]) -> dict[str, Any]:
    # JSON round-tripping prevents a caller from mutating a nested field after
    # validation and before workflow construction.
    try:
        copied = json.loads(_canonical_bytes(value).decode("utf-8"))
    except (TypeError, ValueError, UnicodeError, json.JSONDecodeError) as exc:
        raise FlowMeshContainerDagError(
            "container operation cannot be canonicalized"
        ) from exc
    _require(isinstance(copied, dict), "container operation must be an object")
    return copied


def _validate_operation(value: Mapping[str, Any]) -> dict[str, Any]:
    operation = _copy_operation(value)
    _require(
        operation.get("schema_version") in {
            CONTAINER_OPERATION_SCHEMA_VERSION,
            CONTAINER_OPERATION_LEGACY_SCHEMA_VERSION,
        },
        "unsupported container operation schema_version",
    )
    _text(operation.get("operation_key"), "operation_key")
    _text(operation.get("trial_key"), "trial_key")
    _text(operation.get("operation_id"), "operation_id")
    _text(operation.get("operation_kind"), "operation_kind")
    _text(operation.get("execution_node_id"), "execution_node_id")
    _require(
        type(operation.get("logical_bytes")) is int
        and operation["logical_bytes"] >= 0,
        "logical_bytes must be a non-negative integer",
    )
    dependencies = operation.get("dependency_operation_keys")
    _require(
        isinstance(dependencies, list)
        and all(isinstance(item, str) and item for item in dependencies),
        "dependency_operation_keys must be a list of non-empty strings",
    )
    condition = operation.get("condition")
    if condition is not None:
        # A conditional operation is a legitimate part of a frozen ledger --
        # it is one side of a cache hit/miss branch. It is validated here like
        # any other row; whether it may be *selected* into a smoke chain is a
        # separate question, answered in the selection functions below.
        _require(
            isinstance(condition, Mapping),
            "container operation condition must be an object or null",
        )
        _text(condition.get("cache_operation_key"), "condition.cache_operation_key")
        _text(condition.get("cache_operation_id"), "condition.cache_operation_id")
        _require(
            condition.get("equals") in ("hit", "miss"),
            "condition.equals must be hit or miss",
        )
    return operation


def _is_unconditional(operation: Mapping[str, Any]) -> bool:
    """Whether an operation runs unconditionally within its trial.

    The three-node smoke measures one deterministic physical path. An
    operation gated on a cache hit or miss may simply not run, so it can never
    be part of that path -- but its presence in the ledger is not an error.
    """
    return operation.get("condition") is None


def load_container_operations(path: str | Path) -> list[dict[str, Any]]:
    """Load and structurally validate a frozen ``container_operations.jsonl``.

    Every row is validated, including conditional cache-branch rows: a full
    frozen ledger legitimately contains them, so rejecting the file because it
    is complete would be wrong. The "must be unconditional" rule belongs to
    chain selection, not to loading.
    """

    source = Path(path).resolve()
    try:
        lines = source.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FlowMeshContainerDagError(
            f"cannot read container operations: {source}"
        ) from exc
    operations: list[dict[str, Any]] = []
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise FlowMeshContainerDagError(
                f"container operations contain invalid JSON at line {line_number}"
            ) from exc
        _require(
            isinstance(value, Mapping),
            f"container operation at line {line_number} is not an object",
        )
        operations.append(_validate_operation(value))
    _require(bool(operations), "container operations cannot be empty")
    keys = [row["operation_key"] for row in operations]
    _require(len(keys) == len(set(keys)), "container operation keys are not unique")
    return operations


def select_linear_container_operation_dag(
    operations: Sequence[Mapping[str, Any]],
    *,
    trial_key: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Select one exact ``storage_read -> network_transfer -> compute`` chain.

    The chain must be direct and unconditional within a single frozen trial.
    Refusing ambiguity is intentional: the smoke needs a small, inspectable
    dependency graph rather than silently choosing a path through a cache
    branch or a multi-parent operation.

    Conditional operations are skipped as chain members, not treated as an
    error in the ledger. They stay in ``by_key`` so dependency resolution
    still sees the complete graph: a chain must be refused because a real
    predecessor exists, never accepted because that predecessor was filtered
    out of view.
    """

    checked = [_validate_operation(row) for row in operations]
    by_key = {row["operation_key"]: row for row in checked}
    _require(len(by_key) == len(checked), "container operation keys are not unique")
    candidates: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = []
    for read in checked:
        if read["operation_kind"] != "storage_read":
            continue
        if not _is_unconditional(read):
            continue
        if trial_key is not None and read["trial_key"] != trial_key:
            continue
        # The three-node smoke intentionally omits non-physical scheduling
        # markers, but may not skip a real I/O/compute predecessor.  Without
        # this guard a W2 index path could be made to look like a complete
        # read-transfer-compute path while silently dropping the index return.
        read_predecessors = [
            by_key.get(key) for key in read["dependency_operation_keys"]
        ]
        _require(
            all(item is not None for item in read_predecessors),
            "storage read names a dependency outside the frozen operation set",
        )
        if any(
            item["operation_kind"] not in ("control", "barrier")
            for item in read_predecessors
            if item is not None
        ):
            continue
        # An omitted predecessor must also be unconditional. A conditional
        # scheduling marker means the chain itself is reachable only on one
        # branch, which is exactly the ambiguity this smoke refuses.
        if any(
            not _is_unconditional(item)
            for item in read_predecessors
            if item is not None
        ):
            continue
        for transfer in checked:
            if (
                transfer["operation_kind"] != "network_transfer"
                or not _is_unconditional(transfer)
                or transfer["trial_key"] != read["trial_key"]
                or transfer["dependency_operation_keys"] != [read["operation_key"]]
            ):
                continue
            for compute in checked:
                if (
                    compute["operation_kind"] != "compute"
                    or not _is_unconditional(compute)
                    or compute["trial_key"] != read["trial_key"]
                    or compute["dependency_operation_keys"]
                    != [transfer["operation_key"]]
                ):
                    continue
                # This ensures the direct dependency keys are not merely
                # strings that happen to appear in another trial.
                _require(
                    by_key[transfer["operation_key"]]["dependency_operation_keys"]
                    == [read["operation_key"]],
                    "transfer dependency changed during validation",
                )
                candidates.append((read, transfer, compute))
    _require(
        bool(candidates),
        "no unconditional storage_read -> network_transfer -> compute chain was found",
    )
    candidates.sort(
        key=lambda items: tuple(item["operation_key"] for item in items)
    )
    if trial_key is None and len(candidates) > 1:
        trial_keys = sorted({items[0]["trial_key"] for items in candidates})
        raise FlowMeshContainerDagError(
            "multiple linear container-operation chains are available; pass an "
            "exact trial_key (candidates: " + ", ".join(trial_keys) + ")"
        )
    if trial_key is not None and len(candidates) > 1:
        raise FlowMeshContainerDagError(
            "the selected trial contains multiple linear container-operation chains; "
            "choose a trial with exactly one chain"
        )
    return tuple(_copy_operation(item) for item in candidates[0])  # type: ignore[return-value]


def list_linear_container_operation_dag_candidates(
    operations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """List safe, direct physical DAG candidates without choosing one.

    The response is intentionally small enough to make an operator pin one
    workload/trial before submitting anything to FlowMesh.

    A trial whose only read/transfer/compute path runs behind a cache
    condition simply yields no candidate; it is not an error, and it does not
    prevent other trials in the same ledger from being listed.
    """

    checked = [_validate_operation(row) for row in operations]
    trial_keys = sorted({row["trial_key"] for row in checked})
    candidates: list[dict[str, Any]] = []
    for key in trial_keys:
        try:
            selected = select_linear_container_operation_dag(
                checked, trial_key=key
            )
        except FlowMeshContainerDagError:
            continue
        candidates.append({
            "trial_key": key,
            "operation_keys": [row["operation_key"] for row in selected],
            "operation_kinds": [row["operation_kind"] for row in selected],
            "execution_nodes": [row["execution_node_id"] for row in selected],
            "omitted_nonphysical_predecessor_operation_keys": list(
                selected[0]["dependency_operation_keys"]
            ),
        })
    return candidates


def _operation_lower_bound(operation: Mapping[str, Any]) -> dict[str, Any]:
    """Derive a conservative duration lower bound from the frozen operation.

    Only quantities the operation already carries are used. A network
    transfer with a link adapter has a real floor -- the payload cannot cross
    a rate-limited link faster than ``logical_bytes / bandwidth``, and at
    least one round trip is needed -- so that floor is computed. Storage and
    compute carry no rate in the frozen record, so their bound is 0.0: an
    honest "nothing derivable here", never an invented duration.

    The bound is a floor, not an estimate. Real execution is slower.
    """
    components: dict[str, float] = {}
    seconds = 0.0
    link = operation.get("link_adapter")
    if operation["operation_kind"] == "network_transfer" and isinstance(link, Mapping):
        bandwidth = link.get("bandwidth_bytes_per_second")
        _require(
            isinstance(bandwidth, (int, float))
            and not isinstance(bandwidth, bool)
            and bandwidth > 0,
            "link adapter bandwidth_bytes_per_second must be a positive number",
        )
        rtt_ms = link.get("round_trip_time_ms", 0.0)
        _require(
            isinstance(rtt_ms, (int, float))
            and not isinstance(rtt_ms, bool)
            and rtt_ms >= 0,
            "link adapter round_trip_time_ms must be a non-negative number",
        )
        transfer = operation["logical_bytes"] / float(bandwidth)
        latency = float(rtt_ms) / 1000.0
        components = {
            "transfer_seconds": round(transfer, 6),
            "round_trip_seconds": round(latency, 6),
        }
        seconds = transfer + latency
    basis = "link-rate-and-round-trip" if components else "not-derivable"
    return {
        "operation_key": operation["operation_key"],
        "operation_kind": operation["operation_kind"],
        "lower_bound_seconds": round(seconds, 6),
        "basis": basis,
        "components": components,
    }


def derive_operation_lower_bounds(
    operations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Per-operation conservative duration floors, in selection order."""
    return [_operation_lower_bound(row) for row in operations]


def _require_api_timeout_covers(
    bounds: Sequence[Mapping[str, Any]],
    api_task_timeout_seconds: int,
) -> None:
    """Refuse a timeout that provably cannot let an operation finish.

    A task killed mid-transfer produces a failure indistinguishable from a
    real one, so this is checked when the plan is frozen rather than
    discovered on a worker minutes into a run.
    """
    for bound in bounds:
        if api_task_timeout_seconds < bound["lower_bound_seconds"]:
            raise FlowMeshContainerDagError(
                "API task timeout is shorter than the derived lower bound for "
                f"operation {bound['operation_key']}: requested "
                f"{api_task_timeout_seconds}s, required at least "
                f"{bound['lower_bound_seconds']}s "
                f"(basis: {bound['basis']}); this floor excludes storage, "
                "compute, and scheduling overhead, so choose a larger value"
            )


def _validate_api_task_timeout(value: Any) -> int:
    _require(
        type(value) is int and value > 0,
        "api_task_timeout_seconds must be a positive integer",
    )
    return int(value)


def _validate_node_api_urls(
    operations: Sequence[Mapping[str, Any]],
    node_api_urls: Mapping[str, str],
) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for node_id, raw_url in node_api_urls.items():
        name = _text(node_id, "node API URL node_id")
        url = _text(raw_url, f"node API URL for {name}").rstrip("/")
        parsed = urlsplit(url)
        _require(
            parsed.scheme == "http" and parsed.hostname is not None,
            f"node API URL for {name} must be an absolute http URL",
        )
        _require(
            parsed.username is None and parsed.password is None,
            f"node API URL for {name} must not contain credentials",
        )
        _require(
            not parsed.query and not parsed.fragment,
            f"node API URL for {name} must not contain a query or fragment",
        )
        normalized[name] = url
    required = {str(row["execution_node_id"]) for row in operations}
    required.update(
        str(row["destination_node_id"])
        for row in operations
        if row.get("operation_kind") == "network_transfer"
    )
    missing = sorted(required - set(normalized))
    _require(
        not missing,
        "no API URL was supplied for execution node(s): " + ", ".join(missing),
    )
    return dict(sorted(normalized.items()))


def _task_spec(
    operation: Mapping[str, Any],
    url: str,
    api_task_timeout_seconds: int,
) -> dict[str, Any]:
    return {
        "taskType": "api",
        "api": {
            "url": url.rstrip("/") + "/v1/operations/execute",
            "method": "POST",
            "headers": {"Content-Type": "application/json"},
            "body": _copy_operation(operation),
            "timeout_sec": api_task_timeout_seconds,
            "response": {
                "parse_json": False,
                "return_body": True,
                "raise_for_status": True,
                "max_body_bytes": 131072,
            },
        },
        # FlowMesh result retrieval is Root-based.  ``http`` with no URL
        # directs a current FlowMesh worker to its own Root result endpoint
        # without embedding a bearer token in this workflow.
        "output": {"destination": {"type": "http"}, "artifacts": []},
    }


def build_flowmesh_container_operation_workflow(
    operations: Sequence[Mapping[str, Any]],
    *,
    node_api_urls: Mapping[str, str],
    selected_worker_id: str,
    smoke_id: str,
    owner: str = "pathfinder",
    api_task_timeout_seconds: int = LEGACY_API_TASK_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Build a worker-pinned FlowMesh API graph for an exact three-step DAG.

    ``api_task_timeout_seconds`` is applied verbatim to every API task. The
    default reproduces the value the v1alpha1 planner hard-coded, so a legacy
    plan still builds the workflow it was frozen against.
    """

    _require(len(operations) == 3, "container DAG must contain exactly three operations")
    copied = [_validate_operation(row) for row in operations]
    _require(
        tuple(row["operation_kind"] for row in copied) == _STEP_KINDS,
        "container DAG kinds must be storage_read, network_transfer, compute",
    )
    _require(
        len({row["trial_key"] for row in copied}) == 1,
        "all container DAG operations must belong to one trial",
    )
    _require(
        copied[1]["dependency_operation_keys"] == [copied[0]["operation_key"]]
        and copied[2]["dependency_operation_keys"] == [copied[1]["operation_key"]],
        "container DAG dependencies are not a direct three-step chain",
    )
    worker = _text(selected_worker_id, "selected_worker_id")
    run_name = _text(smoke_id, "smoke_id")
    resolved_urls = _validate_node_api_urls(copied, node_api_urls)
    timeout = _validate_api_task_timeout(api_task_timeout_seconds)
    _require_api_timeout_covers(derive_operation_lower_bounds(copied), timeout)
    nodes: list[dict[str, Any]] = []
    for index, (name, operation) in enumerate(zip(_STEP_NAMES, copied)):
        item: dict[str, Any] = {
            "name": name,
            "spec": _task_spec(
                operation,
                resolved_urls[operation["execution_node_id"]],
                timeout,
            ),
        }
        if index:
            item["dependsOn"] = [_STEP_NAMES[index - 1]]
        nodes.append(item)
    return {
        "apiVersion": "flowmesh/v1",
        "kind": "APITask",
        "metadata": {
            "name": f"pathfinder-container-{run_name[:36]}",
            "owner": _text(owner, "owner"),
            "annotations": {
                "schedule_hint": {"selected_worker": worker},
                "custom": {
                    "pathfinder_smoke_id": run_name,
                    "pathfinder_evidence_class": (
                        "orchestration-transport-compute-conformance"
                    ),
                    "pathfinder_trial_key": copied[0]["trial_key"],
                    "pathfinder_operation_keys": [
                        row["operation_key"] for row in copied
                    ],
                    "pathfinder_semantic_quality_evaluated": False,
                },
            },
        },
        "spec": {"graph": {"nodes": nodes}},
    }


def _workflow_template(
    operations: Sequence[Mapping[str, Any]],
    node_api_urls: Mapping[str, str],
    *,
    smoke_id: str,
    worker_alias: str,
    owner: str,
    api_task_timeout_seconds: int,
) -> dict[str, Any]:
    """Build a non-submittable template that contains no volatile worker ID."""

    urls = _validate_node_api_urls(operations, node_api_urls)
    timeout = _validate_api_task_timeout(api_task_timeout_seconds)
    nodes: list[dict[str, Any]] = []
    for index, (name, operation) in enumerate(zip(_STEP_NAMES, operations)):
        node: dict[str, Any] = {
            "name": name,
            "spec": _task_spec(
                operation,
                urls[str(operation["execution_node_id"])],
                timeout,
            ),
        }
        if index:
            node["dependsOn"] = [_STEP_NAMES[index - 1]]
        nodes.append(node)
    return {
        "apiVersion": "flowmesh/v1",
        "kind": "APITask",
        "metadata": {
            "name": f"pathfinder-container-{smoke_id[:36]}",
            "owner": owner,
            "annotations": {
                "custom": {
                    "pathfinder_smoke_id": smoke_id,
                    "pathfinder_worker_alias_to_resolve_at_submission": worker_alias,
                    "pathfinder_workflow_is_not_submittable": True,
                    "pathfinder_reason": (
                        "the current worker ID is deliberately resolved immediately "
                        "before submission"
                    ),
                }
            },
        },
        "spec": {"graph": {"nodes": nodes}},
    }


def _write_documents(target: Path, documents: Mapping[str, bytes]) -> None:
    _require(not target.exists(), f"output directory already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging_parent = Path(tempfile.mkdtemp(prefix=".flowmesh-container-dag-", dir=target.parent))
    staging = staging_parent / "output"
    try:
        staging.mkdir()
        for name, content in sorted(documents.items()):
            path = staging / name
            with path.open("wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
        os.replace(staging, target)
    finally:
        shutil.rmtree(staging_parent, ignore_errors=True)


def _checksums(documents: Mapping[str, bytes]) -> bytes:
    return b"".join(
        f"{_sha256_bytes(content)}  {name}\n".encode("utf-8")
        for name, content in sorted(documents.items())
    )


def _read_plan(plan_dir: str | Path) -> dict[str, Any]:
    root = Path(plan_dir).resolve()
    _require(root.is_dir(), f"container DAG plan directory does not exist: {root}")
    try:
        checksums = (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FlowMeshContainerDagError("container DAG plan checksum file is unreadable") from exc
    observed: dict[str, str] = {}
    for line in checksums:
        digest, separator, name = line.partition("  ")
        _require(separator == "  " and name in _PLAN_FILES, "invalid container DAG plan checksum row")
        _require(name not in observed, "duplicate container DAG plan checksum")
        observed[name] = digest
    _require(set(observed) == _PLAN_FILES, "container DAG plan checksum set is incomplete")
    for name, digest in observed.items():
        _require((root / name).is_file(), f"container DAG plan file is missing: {name}")
        _require(
            _sha256_bytes((root / name).read_bytes()) == digest,
            f"container DAG plan checksum mismatch: {name}",
        )
    try:
        plan = json.loads((root / "flowmesh-container-dag-plan.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FlowMeshContainerDagError("container DAG plan is invalid JSON") from exc
    _require(isinstance(plan, dict), "container DAG plan must be an object")
    schema = plan.get("schema_version")
    _require(
        schema
        in (
            FLOWMESH_CONTAINER_DAG_PLAN_SCHEMA_VERSION,
            FLOWMESH_CONTAINER_DAG_PLAN_LEGACY_SCHEMA_VERSION,
        ),
        "unsupported container DAG plan schema",
    )
    _require(plan.get("status") == "FROZEN", "container DAG plan is not frozen")
    _require(plan.get("plan_sha256") == _document_sha256(plan, "plan_sha256"), "container DAG plan digest mismatch")
    operations = plan.get("operations")
    _require(isinstance(operations, list), "container DAG plan operations are missing")
    selected = tuple(_validate_operation(row) for row in operations)
    _require(len(selected) == 3, "container DAG plan must contain exactly three operations")
    # Selection can no longer produce one, but a plan is a file on disk that
    # may have been written by an older or edited toolchain.
    _require(
        all(_is_unconditional(row) for row in selected),
        "container DAG plan contains a conditional operation",
    )
    _require(
        tuple(row["operation_kind"] for row in selected) == _STEP_KINDS,
        "container DAG plan operation kinds changed",
    )
    _require(
        selected[1]["dependency_operation_keys"] == [selected[0]["operation_key"]]
        and selected[2]["dependency_operation_keys"] == [selected[1]["operation_key"]],
        "container DAG plan dependencies changed",
    )
    _require(
        plan.get("omitted_nonphysical_predecessor_operation_keys")
        == selected[0]["dependency_operation_keys"],
        "container DAG plan omitted-predecessor record changed",
    )
    urls = plan.get("node_api_urls")
    _require(isinstance(urls, Mapping), "container DAG plan has no node API URLs")
    _validate_node_api_urls(selected, urls)
    _text(plan.get("worker_alias"), "worker_alias")
    if schema == FLOWMESH_CONTAINER_DAG_PLAN_LEGACY_SCHEMA_VERSION:
        # A legacy plan is read under the semantics it was frozen with and is
        # never rewritten. Its timeout is reported, not invented.
        _require(
            "api_task_timeout_seconds" not in plan,
            "a legacy container DAG plan must not declare an API task timeout",
        )
        plan["api_task_timeout_seconds"] = LEGACY_API_TASK_TIMEOUT_SECONDS
        plan["api_task_timeout_source"] = "legacy-fixed-default"
        plan["operation_lower_bound_seconds"] = derive_operation_lower_bounds(
            selected
        )
        plan["max_operation_lower_bound_seconds"] = max(
            (
                row["lower_bound_seconds"]
                for row in plan["operation_lower_bound_seconds"]
            ),
            default=0.0,
        )
        return plan
    timeout = _validate_api_task_timeout(plan.get("api_task_timeout_seconds"))
    # Re-derive from the stored operations rather than trusting the recorded
    # evidence. The plan digest already covers both, but a tampered plan can
    # be re-stamped; a bound recomputed from the operations themselves cannot
    # be edited without also editing the operation it comes from.
    recomputed = derive_operation_lower_bounds(selected)
    _require(
        plan.get("operation_lower_bound_seconds") == recomputed,
        "container DAG plan lower-bound record does not match its operations",
    )
    _require(
        plan.get("max_operation_lower_bound_seconds")
        == max((row["lower_bound_seconds"] for row in recomputed), default=0.0),
        "container DAG plan maximum lower bound does not match its operations",
    )
    _require_api_timeout_covers(recomputed, timeout)
    plan["api_task_timeout_source"] = "plan"
    return plan


def plan_flowmesh_container_operation_dag(
    *,
    container_operations_path: str | Path,
    node_api_urls: Mapping[str, str],
    worker_alias: str,
    smoke_id: str,
    output_dir: str | Path,
    trial_key: str | None = None,
    owner: str = "pathfinder",
    api_task_timeout_seconds: int = DEFAULT_API_TASK_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Freeze an exact three-operation FlowMesh container-DAG input package.

    The package has an alias, rather than a current worker ID.  A current ID
    is intentionally resolved by the run command just before submission.
    """

    alias = _text(worker_alias, "worker_alias")
    run_name = _text(smoke_id, "smoke_id")
    owner_name = _text(owner, "owner")
    source = Path(container_operations_path).resolve()
    selected = select_linear_container_operation_dag(
        load_container_operations(source), trial_key=trial_key
    )
    urls = _validate_node_api_urls(selected, node_api_urls)
    timeout = _validate_api_task_timeout(api_task_timeout_seconds)
    # Derive the floors before writing anything: a plan that cannot finish is
    # refused at freeze time rather than after a worker has burned the wall
    # clock on it.
    bounds = derive_operation_lower_bounds(selected)
    _require_api_timeout_covers(bounds, timeout)
    plan = {
        "schema_version": FLOWMESH_CONTAINER_DAG_PLAN_SCHEMA_VERSION,
        "status": "FROZEN",
        "smoke_id": run_name,
        "worker_alias": alias,
        "owner": owner_name,
        "trial_key": selected[0]["trial_key"],
        "container_operations_source_sha256": _sha256_bytes(source.read_bytes()),
        "operations": list(selected),
        "omitted_nonphysical_predecessor_operation_keys": list(
            selected[0]["dependency_operation_keys"]
        ),
        "node_api_urls": urls,
        "api_task_timeout_seconds": timeout,
        "operation_lower_bound_seconds": bounds,
        "max_operation_lower_bound_seconds": max(
            (row["lower_bound_seconds"] for row in bounds), default=0.0
        ),
        "evidence_class": "orchestration-transport-compute-conformance",
        "llm_called": False,
        "semantic_task_quality_evaluated": False,
        "eligible_for_scientific_claims": False,
        "credentials_recorded": False,
    }
    plan["plan_sha256"] = _document_sha256(plan, "plan_sha256")
    template = _workflow_template(
        selected,
        urls,
        smoke_id=run_name,
        worker_alias=alias,
        owner=owner_name,
        api_task_timeout_seconds=timeout,
    )
    documents = {
        "flowmesh-container-dag-plan.json": _json_bytes(plan),
        "flowmesh-container-dag-workflow-template.json": _json_bytes(template),
    }
    documents["SHA256SUMS"] = _checksums(documents)
    target = Path(output_dir).resolve()
    _write_documents(target, documents)
    return {
        "status": "FROZEN_WORKFLOW_INPUTS",
        "output_dir": str(target),
        "smoke_id": run_name,
        "trial_key": selected[0]["trial_key"],
        "operation_keys": [row["operation_key"] for row in selected],
        "execution_nodes": [row["execution_node_id"] for row in selected],
        "worker_alias": alias,
        "plan_sha256": plan["plan_sha256"],
        "api_task_timeout_seconds": timeout,
        "max_operation_lower_bound_seconds": plan[
            "max_operation_lower_bound_seconds"
        ],
        "workflow_submitted": False,
        "services_started": False,
        "llm_called": False,
        "eligible_for_scientific_claims": False,
    }


def verify_flowmesh_container_operation_dag_plan(
    plan_dir: str | Path,
) -> dict[str, Any]:
    """Offline verification for a frozen three-operation DAG package."""

    plan = _read_plan(plan_dir)
    return {
        "status": "VERIFIED",
        "smoke_id": plan["smoke_id"],
        "trial_key": plan["trial_key"],
        "operation_keys": [row["operation_key"] for row in plan["operations"]],
        "execution_nodes": [row["execution_node_id"] for row in plan["operations"]],
        "worker_alias": plan["worker_alias"],
        "plan_sha256": plan["plan_sha256"],
        "schema_version": plan["schema_version"],
        "api_task_timeout_seconds": plan["api_task_timeout_seconds"],
        "api_task_timeout_source": plan["api_task_timeout_source"],
        "operation_lower_bound_seconds": plan["operation_lower_bound_seconds"],
        "max_operation_lower_bound_seconds": plan[
            "max_operation_lower_bound_seconds"
        ],
        "workflow_submitted": False,
        "services_started": False,
        "eligible_for_scientific_claims": False,
    }


def _workflow_failure(
    terminal: TerminalWorkflow,
    submitted: SubmittedWorkflow,
    client: FlowMeshClientProtocol,
) -> FlowMeshContainerDagError:
    details: list[str] = [terminal.detail] if terminal.detail else []
    for task_id in submitted.task_ids:
        try:
            detail = client.describe_task_failure(task_id)
        except Exception as exc:  # diagnostic retrieval must not mask failure
            details.append("task detail lookup failed: " + redact_secrets(str(exc)))
            continue
        if detail:
            details.append(
                json.dumps(detail, sort_keys=True, ensure_ascii=False)
            )
    return FlowMeshContainerDagError(
        f"FlowMesh container DAG {submitted.workflow_id} ended with {terminal.status}: "
        + ("; ".join(details) or "no task detail was reported")
    )


def _finite_non_negative(value: Any, field: str) -> float:
    _require(
        isinstance(value, (int, float)) and not isinstance(value, bool),
        f"container telemetry {field} must be a number",
    )
    number = float(value)
    _require(
        math.isfinite(number),
        f"container telemetry {field} must be finite",
    )
    _require(number >= 0.0, f"container telemetry {field} must not be negative")
    return number


def _operation_telemetry(
    result: Mapping[str, Any],
    operation: Mapping[str, Any],
    *,
    provenance_version: str = TELEMETRY_PROVENANCE_VERSION,
) -> dict[str, Any]:
    """Extract and validate the preserved telemetry subset.

    Every field is required. A missing timing value is refused rather than
    defaulted, because a silent zero would be indistinguishable from a real
    measurement of zero.
    """
    _telemetry_contract(provenance_version)
    kind = operation["operation_kind"]
    telemetry: dict[str, Any] = {}
    for field in (
        "service_time_ms",
        "fixture_materialization_ms_excluded_from_storage_measurement",
    ):
        _require(field in result, f"container result is missing {field}")
        telemetry[field] = _finite_non_negative(result[field], field)

    _require(
        "application_shaping_target_ms" in result,
        "container result is missing application_shaping_target_ms",
    )
    shaping = result["application_shaping_target_ms"]
    if shaping is None:
        # Null is legitimate only where the runtime genuinely has no shaping
        # target: it is set exclusively on the network transfer path.
        _require(
            kind != "network_transfer",
            "a network transfer must report an application shaping target",
        )
        telemetry["application_shaping_target_ms"] = None
    else:
        _require(
            kind == "network_transfer",
            f"a {kind} operation must not report an application shaping "
            "target",
        )
        telemetry["application_shaping_target_ms"] = _finite_non_negative(
            shaping, "application_shaping_target_ms"
        )

    if kind not in ("storage_read", "cache_read"):
        # Only a fixture-backed storage or cache read materializes anything;
        # a non-zero value elsewhere means the record does not describe the
        # operation it claims to.
        _require(
            telemetry[
                "fixture_materialization_ms_excluded_from_storage_measurement"
            ]
            == 0.0,
            f"a {kind} operation must not report fixture materialization time",
        )

    if provenance_version == TELEMETRY_PROVENANCE_VERSION:
        for field in (
            "network_http_exchange_ms",
            "application_shaping_sleep_ms",
        ):
            _require(field in result, f"container result is missing {field}")
        if kind == "network_transfer":
            http_exchange_ms = _finite_non_negative(
                result["network_http_exchange_ms"],
                "network_http_exchange_ms",
            )
            shaping_sleep_ms = _finite_non_negative(
                result["application_shaping_sleep_ms"],
                "application_shaping_sleep_ms",
            )
            _require(
                http_exchange_ms + shaping_sleep_ms
                <= telemetry["service_time_ms"] + 0.1,
                "network timing components exceed service_time_ms",
            )
            telemetry["network_http_exchange_ms"] = http_exchange_ms
            telemetry["application_shaping_sleep_ms"] = shaping_sleep_ms
        else:
            _require(
                result["network_http_exchange_ms"] is None
                and result["application_shaping_sleep_ms"] is None,
                f"a {kind} operation must not report network timing components",
            )
            telemetry["network_http_exchange_ms"] = None
            telemetry["application_shaping_sleep_ms"] = None

    started = result.get("started_monotonic_ns")
    finished = result.get("finished_monotonic_ns")
    if isinstance(started, int) and isinstance(finished, int):
        # Same-node, same-clock consistency only. This is not a cross-
        # container comparison; it checks the record against itself.
        _require(
            finished >= started,
            "container telemetry finished before it started",
        )
        _require(
            abs(
                (finished - started) / 1_000_000.0
                - telemetry["service_time_ms"]
            )
            <= 1e-6,
            "container service_time_ms disagrees with its own monotonic "
            "interval",
        )
    return telemetry


def _operation_cache_result(
    result: Mapping[str, Any],
    operation: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate cache fields and preserve them only for cache operations."""

    kind = operation["operation_kind"]
    for field in ("cache_result", "cache_scope_id", "cache_evictions"):
        _require(field in result, f"container result is missing {field}")

    cache_result = result["cache_result"]
    cache_scope_id = result["cache_scope_id"]
    cache_evictions = result["cache_evictions"]
    _require(
        type(cache_evictions) is list,
        "container result cache_evictions must be an array",
    )
    _require(
        all(
            isinstance(value, str) and bool(value.strip())
            for value in cache_evictions
        ),
        "container result cache_evictions must contain non-empty strings",
    )
    _require(
        len(cache_evictions) == len(set(cache_evictions)),
        "container result cache_evictions must not contain duplicates",
    )

    if kind not in _CACHE_OPERATION_KINDS:
        _require(
            cache_result is None,
            f"a {kind} operation must report a neutral cache_result",
        )
        _require(
            cache_scope_id is None,
            f"a {kind} operation must report a neutral cache_scope_id",
        )
        _require(
            cache_evictions == [],
            f"a {kind} operation must report no cache evictions",
        )
        return {}

    expected_scope_id = _text(
        operation.get("cache_scope_id"),
        f"{kind} operation cache_scope_id",
    )
    actual_scope_id = _text(
        cache_scope_id,
        f"{kind} result cache_scope_id",
    )
    _require(
        actual_scope_id == expected_scope_id,
        f"{kind} result changed cache scope",
    )

    if kind == "cache_lookup":
        _require(
            cache_result in {"hit", "miss"},
            "cache lookup result must be the literal hit or miss outcome",
        )
        _require(
            cache_evictions == [],
            "cache lookup result must not report cache evictions",
        )
    elif kind == "cache_read":
        _require(
            cache_result == "hit",
            "cache read result must report a live cache hit",
        )
        _require(
            cache_evictions == [],
            "cache read result must not report cache evictions",
        )
    else:
        _require(
            cache_result is None,
            "cache insert result must report a neutral cache_result",
        )

    return {
        "cache_result": cache_result,
        "cache_scope_id": actual_scope_id,
        "cache_evictions": list(cache_evictions),
    }


def _operation_result(
    raw: Mapping[str, Any],
    operation: Mapping[str, Any],
    *,
    task_id: str,
    selected_worker_id: str,
    task_detail: Mapping[str, Any] | None,
    expected_runtime_epochs: Mapping[str, str],
) -> dict[str, Any]:
    api = extract_api_executor_result(raw)
    try:
        result = json.loads(api["text"])
    except json.JSONDecodeError as exc:
        raise FlowMeshContainerDagError(
            f"FlowMesh task {task_id} returned non-JSON container output"
        ) from exc
    _require(isinstance(result, Mapping), "container result must be an object")
    _require(
        result.get("schema_version") == CONTAINER_NODE_RESULT_SCHEMA_VERSION,
        "container result does not support the v2 result contract",
    )
    _require(result.get("status") == "completed", "container operation did not complete")
    _require(result.get("outcome_type") == "completed", "container outcome is not completed")
    _require(result.get("telemetry_complete") is True, "container telemetry is incomplete")
    _require(result.get("credentials_recorded") is False, "container result recorded credentials")
    _require(
        result.get("semantic_task_quality_evaluated") is False,
        "container result must report semantic_task_quality_evaluated=false",
    )
    _require(result.get("idempotent_replay") is False, "container operation was replayed")
    for field in ("operation_key", "operation_kind", "execution_node_id"):
        _require(
            result.get(field) == operation.get(field),
            f"container result changed {field}",
        )
    _require(
        result.get("logical_bytes") == operation.get("logical_bytes"),
        "container result changed logical byte count",
    )
    if operation["operation_kind"] in _PHYSICAL_IO_OPERATION_KINDS:
        _require(
            result.get("physical_bytes") == operation["logical_bytes"],
            "container I/O result did not report exact physical bytes",
        )
    else:
        _require(
            result.get("physical_bytes") == 0,
            "container non-I/O result must not report physical transfer bytes",
        )
    cache_fields = _operation_cache_result(result, operation)
    if task_detail is not None:
        assigned = task_detail.get("assigned_worker")
        _require(
            assigned == selected_worker_id,
            "FlowMesh assigned a container DAG task to a worker other than the pin",
        )
    source = _text(operation.get("execution_node_id"), "execution_node_id")
    _require(source in expected_runtime_epochs, "runtime binding lacks the execution node")
    runtime_epoch = _runtime_epoch(result.get("runtime_epoch"), "container runtime_epoch")
    _require(
        runtime_epoch == expected_runtime_epochs[source],
        "container result runtime epoch does not match the pre-submit health binding",
    )
    destination_runtime_epoch = result.get("destination_runtime_epoch")
    if operation["operation_kind"] == "network_transfer":
        destination = _text(
            operation.get("destination_node_id"), "destination_node_id"
        )
        _require(
            destination in expected_runtime_epochs,
            "runtime binding lacks the network destination node",
        )
        destination_runtime_epoch = _runtime_epoch(
            destination_runtime_epoch,
            "container destination_runtime_epoch",
        )
        _require(
            destination_runtime_epoch == expected_runtime_epochs[destination],
            "network sink runtime epoch does not match the pre-submit health binding",
        )
    else:
        _require(
            destination_runtime_epoch is None,
            "non-network container result names a destination runtime epoch",
        )
    return {
        "task_id": task_id,
        "worker_id": selected_worker_id,
        "operation_key": operation["operation_key"],
        "operation_kind": operation["operation_kind"],
        "execution_node_id": operation["execution_node_id"],
        "destination_node_id": operation["destination_node_id"],
        "runtime_epoch": runtime_epoch,
        "destination_runtime_epoch": destination_runtime_epoch,
        "container_result_schema_version": CONTAINER_NODE_RESULT_SCHEMA_VERSION,
        "logical_bytes": result["logical_bytes"],
        "physical_bytes": result["physical_bytes"],
        **cache_fields,
        **_operation_telemetry(result, operation),
        "telemetry_provenance_version": TELEMETRY_PROVENANCE_VERSION,
        "telemetry_complete": True,
        "semantic_task_quality_evaluated": False,
        "idempotent_replay": False,
        "api_executor": "api",
        "api_http_status": api["status_code"],
        "container_result_sha256": _sha256_bytes(_canonical_bytes(result)),
        "task_detail_available": task_detail is not None,
    }


def _aggregate_telemetry(
    task_results: Sequence[Mapping[str, Any]],
    *,
    provenance_version: str = TELEMETRY_PROVENANCE_VERSION,
) -> dict[str, Any]:
    """Descriptive aggregates that follow directly from preserved records.

    Deliberately absent: queue time, scheduling delay, end-to-end latency,
    and any bytes-per-second figure. None of those were measured, and a
    throughput ratio over an application-shaped transfer would describe the
    shaper rather than the link.
    """
    _telemetry_contract(provenance_version)
    operation_keys = [
        _text(row.get("operation_key"), "telemetry operation_key")
        for row in task_results
    ]
    _require(
        len(operation_keys) == len(set(operation_keys)),
        "telemetry contains duplicate operation keys",
    )
    service = [float(row["service_time_ms"]) for row in task_results]
    materialization = [
        float(
            row["fixture_materialization_ms_excluded_from_storage_measurement"]
        )
        for row in task_results
    ]
    service_by_key = {
        operation_key: round(float(row["service_time_ms"]), 6)
        for operation_key, row in zip(operation_keys, task_results)
    }
    service_sum_by_kind: dict[str, float] = {}
    for row in task_results:
        kind = str(row["operation_kind"])
        service_sum_by_kind[kind] = (
            service_sum_by_kind.get(kind, 0.0)
            + float(row["service_time_ms"])
        )
    shaping_by_key = {
        operation_key: row["application_shaping_target_ms"]
        for operation_key, row in zip(operation_keys, task_results)
        if row.get("application_shaping_target_ms") is not None
    }

    # Retain the original kind-keyed views for existing consumers.  They are
    # lossy when a DAG repeats an operation kind, so the operation-keyed maps
    # above are the canonical per-operation observations and the kind sums
    # below are the repeat-safe aggregate view.
    by_kind = {
        str(row["operation_kind"]): round(float(row["service_time_ms"]), 6)
        for row in task_results
    }
    shaping = {
        str(row["operation_kind"]): row["application_shaping_target_ms"]
        for row in task_results
        if row.get("application_shaping_target_ms") is not None
    }
    aggregate = {
        "telemetry_provenance_version": provenance_version,
        "record_count": len(task_results),
        "telemetry_complete_record_count": sum(
            1 for row in task_results if row.get("telemetry_complete") is True
        ),
        "service_time_ms_by_operation_key": service_by_key,
        "service_time_ms_sum_by_operation_kind": {
            key: round(value, 6)
            for key, value in sorted(service_sum_by_kind.items())
        },
        "service_time_ms_by_operation_kind": by_kind,
        "service_time_ms_sum": round(sum(service), 6),
        "service_time_ms_max": round(max(service), 6) if service else 0.0,
        "service_time_ms_min": round(min(service), 6) if service else 0.0,
        "fixture_materialization_ms_sum_excluded_from_storage_measurement": (
            round(sum(materialization), 6)
        ),
        "configured_application_shaping_target_ms_by_operation_kind": shaping,
        "configured_application_shaping_target_ms_by_operation_key": (
            shaping_by_key
        ),
        "logical_bytes_sum": sum(int(row["logical_bytes"]) for row in task_results),
        "physical_bytes_sum": sum(
            int(row["physical_bytes"]) for row in task_results
        ),
        "service_time_ms_sum_is_end_to_end_latency": False,
        "network_throughput_derived": False,
        "queue_time_measured": False,
    }
    if provenance_version == TELEMETRY_PROVENANCE_VERSION:
        network_rows = [
            row
            for row in task_results
            if row.get("operation_kind") == "network_transfer"
        ]
        aggregate["network_http_exchange_ms_sum"] = round(
            sum(float(row["network_http_exchange_ms"]) for row in network_rows),
            6,
        )
        aggregate["application_shaping_sleep_ms_sum"] = round(
            sum(
                float(row["application_shaping_sleep_ms"])
                for row in network_rows
            ),
            6,
        )
    return aggregate


def _read_run(
    run_dir: str | Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    """Read and checksum-verify a completed run artifact directory."""

    root = Path(run_dir).resolve()
    _require(root.is_dir(), f"container DAG run directory does not exist: {root}")
    try:
        checksums = (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FlowMeshContainerDagError(
            "container DAG run checksum file is unreadable"
        ) from exc
    observed: dict[str, str] = {}
    for line in checksums:
        digest, separator, name = line.partition("  ")
        _require(
            separator == "  " and name in _RUN_FILES,
            "invalid container DAG run checksum row",
        )
        _require(name not in observed, "duplicate container DAG run checksum")
        observed[name] = digest
    _require(
        set(observed) == _RUN_FILES, "container DAG run checksum set is incomplete"
    )
    for name, digest in observed.items():
        _require((root / name).is_file(), f"container DAG run file is missing: {name}")
        _require(
            _sha256_bytes((root / name).read_bytes()) == digest,
            f"container DAG run checksum mismatch: {name}",
        )
    try:
        summary = json.loads(
            (root / "flowmesh-container-dag-run.json").read_text(encoding="utf-8")
        )
        submission = json.loads(
            (root / "flowmesh-container-dag-submission.json").read_text(
                encoding="utf-8"
            )
        )
        rows = [
            json.loads(line)
            for line in (
                root / "flowmesh-container-dag-task-results.jsonl"
            ).read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FlowMeshContainerDagError(
            "container DAG run artifact is invalid JSON"
        ) from exc
    _require(isinstance(summary, dict), "container DAG run summary must be an object")
    _require(
        isinstance(submission, dict), "container DAG submission must be an object"
    )
    _require(
        all(isinstance(row, dict) for row in rows),
        "each container DAG task result must be an object",
    )
    return summary, rows, submission


def verify_flowmesh_container_operation_dag_run(
    run_dir: str | Path,
    *,
    plan_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Offline verification of a completed container-DAG run artifact.

    Checks the artifact against itself and, when a plan directory is given,
    against the exact plan it claims to have executed. Nothing here contacts
    FlowMesh, a container, or a network.
    """

    summary, rows, submission = _read_run(run_dir)
    schema = summary.get("schema_version")
    _require(
        schema
        in (
            FLOWMESH_CONTAINER_DAG_RUN_SCHEMA_VERSION,
            FLOWMESH_CONTAINER_DAG_RUN_TIMING_V1_SCHEMA_VERSION,
            FLOWMESH_CONTAINER_DAG_RUN_LEGACY_SCHEMA_VERSION,
        ),
        "unsupported container DAG run schema",
    )
    _require(summary.get("status") == "COMPLETE", "container DAG run is not complete")
    legacy = schema == FLOWMESH_CONTAINER_DAG_RUN_LEGACY_SCHEMA_VERSION
    provenance_version = (
        TELEMETRY_PROVENANCE_VERSION
        if schema == FLOWMESH_CONTAINER_DAG_RUN_SCHEMA_VERSION
        else TELEMETRY_PROVENANCE_LEGACY_VERSION
    )

    operation_keys = summary.get("operation_keys")
    _require(
        isinstance(operation_keys, list) and operation_keys,
        "container DAG run summary has no operation keys",
    )
    _require(
        {row.get("operation_key") for row in rows} == set(operation_keys),
        "container DAG task results do not cover the exact run operations",
    )
    _require(
        len(rows) == len(operation_keys),
        "container DAG task result count does not match the run operations",
    )
    _require(
        summary.get("task_result_count") == len(rows),
        "container DAG run summary task_result_count is wrong",
    )

    worker = summary.get("selected_worker")
    _require(isinstance(worker, Mapping), "container DAG run has no selected worker")
    worker_id = worker.get("worker_id")
    _text(worker_id, "selected worker_id")
    _require(
        submission.get("selected_worker_id") == worker_id,
        "container DAG submission worker does not match the run summary",
    )
    _require(
        all(row.get("worker_id") == worker_id for row in rows),
        "a container DAG task result names a different worker than the pin",
    )
    _require(
        all(row.get("api_http_status") == 200 for row in rows),
        "a container DAG task result does not report a 200 API status",
    )
    _require(
        all(row.get("telemetry_complete") is True for row in rows),
        "a container DAG task result does not report complete telemetry",
    )
    for row in rows:
        _text(row.get("container_result_sha256"), "container_result_sha256")

    if plan_dir is not None:
        plan = _read_plan(plan_dir)
        _require(
            plan["plan_sha256"] == summary.get("plan_sha256"),
            "container DAG run is not bound to the supplied plan",
        )
        _require(
            [row["operation_key"] for row in plan["operations"]]
            == list(operation_keys),
            "container DAG run operations do not match the supplied plan",
        )
        _require(
            plan["worker_alias"] == worker.get("alias", plan["worker_alias"]),
            "container DAG run worker alias does not match the plan",
        )

    if legacy:
        return {
            "status": "VERIFIED",
            "schema_version": schema,
            "smoke_id": summary.get("smoke_id"),
            "plan_sha256": summary.get("plan_sha256"),
            "worker_id": worker_id,
            "task_result_count": len(rows),
            "plan_binding_checked": plan_dir is not None,
            "timing_recorded": False,
            "telemetry_recording": "not-recorded-legacy",
            "telemetry": None,
            "eligible_for_scientific_claims": False,
        }

    for row in rows:
        _require(
            row.get("telemetry_provenance_version") == provenance_version,
            "container DAG task result has an unsupported telemetry provenance",
        )
        # Re-validate against the operation kind the record itself declares.
        _operation_telemetry(
            row,
            {"operation_kind": row.get("operation_kind")},
            provenance_version=provenance_version,
        )
        _require(
            type(row.get("logical_bytes")) is int
            and row["logical_bytes"] >= 0
            and type(row.get("physical_bytes")) is int
            and row["physical_bytes"] >= 0,
            "container DAG task result byte counts are invalid",
        )
        if row.get("operation_kind") in _PHYSICAL_IO_OPERATION_KINDS:
            _require(
                row["physical_bytes"] == row["logical_bytes"],
                "container DAG I/O result did not report exact physical bytes",
            )
        else:
            _require(
                row["physical_bytes"] == 0,
                "container DAG non-I/O result must not report transfer bytes",
            )
    _require(
        summary.get("telemetry")
        == _aggregate_telemetry(rows, provenance_version=provenance_version),
        "container DAG run telemetry aggregate does not match its task results",
    )
    fields, disclaimers = _telemetry_contract(provenance_version)
    _require(
        summary.get("telemetry_provenance", {}).get("fields")
        == fields,
        "container DAG run telemetry provenance record changed",
    )
    _require(
        summary.get("telemetry_provenance", {}).get("version")
        == provenance_version,
        "container DAG run telemetry provenance version changed",
    )
    _require(
        summary.get("telemetry_provenance", {}).get("disclaimers")
        == list(disclaimers),
        "container DAG run telemetry provenance disclaimers changed",
    )
    runtime_integrity = "not-recorded-v1"
    if schema == FLOWMESH_CONTAINER_DAG_RUN_SCHEMA_VERSION:
        urls = summary.get("node_api_urls")
        _require(isinstance(urls, Mapping), "container DAG run node API URLs are missing")
        _validate_node_api_urls(rows, urls)
        _verify_runtime_epoch_binding(
            summary.get("runtime_epoch_binding"),
            plan_sha256=summary.get("plan_sha256"),
            node_api_urls=urls,
            operations=rows,
            rows=rows,
        )
        _require(
            all(
                row.get("container_result_schema_version")
                == CONTAINER_NODE_RESULT_SCHEMA_VERSION
                for row in rows
            ),
            "container DAG task result schema changed",
        )
        runtime_integrity = "bound-v2"
        if plan_dir is not None:
            _require(
                urls == plan["node_api_urls"],
                "container DAG run node API URLs do not match the supplied plan",
            )
    return {
        "status": "VERIFIED",
        "schema_version": schema,
        "smoke_id": summary.get("smoke_id"),
        "plan_sha256": summary.get("plan_sha256"),
        "worker_id": worker_id,
        "task_result_count": len(rows),
        "plan_binding_checked": plan_dir is not None,
        "timing_recorded": True,
        "telemetry_recording": (
            "whitelisted-validated-v2"
            if provenance_version == TELEMETRY_PROVENANCE_VERSION
            else "whitelisted-validated-v1"
        ),
        "runtime_epoch_binding": runtime_integrity,
        "telemetry": summary["telemetry"],
        "eligible_for_scientific_claims": False,
    }


def run_flowmesh_container_operation_dag(
    *,
    plan_dir: str | Path,
    output_dir: str | Path,
    client: FlowMeshClientProtocol,
    settings: FlowMeshSettings,
    runtime_epoch_probe: Callable[
        [Mapping[str, str], Sequence[Mapping[str, Any]]], Mapping[str, str]
    ] | None = None,
) -> dict[str, Any]:
    """Submit exactly one validated FlowMesh graph and verify all three tasks.

    This performs no container, Docker, tunnel, or worker lifecycle action.
    Every service must already be running and the selected worker must already
    be visible to the configured FlowMesh Root.
    """

    plan = _read_plan(plan_dir)
    _require(
        settings.worker_alias == plan["worker_alias"],
        "run worker alias must exactly match the frozen DAG plan",
    )
    identity = describe_pinned_worker(client, settings)
    workflow = build_flowmesh_container_operation_workflow(
        plan["operations"],
        node_api_urls=plan["node_api_urls"],
        selected_worker_id=identity.worker_id,
        smoke_id=plan["smoke_id"],
        owner=plan["owner"],
        api_task_timeout_seconds=plan["api_task_timeout_seconds"],
    )
    validation = client.validate(workflow)
    _require(
        validation.ok,
        "FlowMesh rejected the container DAG workflow: "
        + "; ".join(validation.errors),
    )
    probe = runtime_epoch_probe or _probe_container_runtime_epochs
    runtime_epochs_before = _validate_runtime_epochs(
        probe(plan["node_api_urls"], plan["operations"]),
        plan["operations"],
    )
    submitted = client.submit(workflow)
    _require(
        len(submitted.task_ids) == 3,
        "FlowMesh returned a task count other than the three planned DAG nodes",
    )
    terminal = client.wait(submitted.workflow_id, settings.poll_interval_seconds)
    if terminal.status != "DONE":
        raise _workflow_failure(terminal, submitted, client)

    expected_by_key = {row["operation_key"]: row for row in plan["operations"]}
    task_results: list[dict[str, Any]] = []
    for task_id in submitted.task_ids:
        raw = client.retrieve_result(task_id)
        api = extract_api_executor_result(raw)
        try:
            body = json.loads(api["text"])
        except json.JSONDecodeError as exc:
            raise FlowMeshContainerDagError(
                f"FlowMesh task {task_id} returned non-JSON container output"
            ) from exc
        _require(isinstance(body, Mapping), "container result must be an object")
        operation_key = body.get("operation_key")
        _require(
            isinstance(operation_key, str) and operation_key in expected_by_key,
            "FlowMesh task returned an operation outside the frozen DAG plan",
        )
        try:
            task_detail = client.describe_task_failure(task_id)
        except Exception as exc:
            raise FlowMeshContainerDagError(
                "cannot verify the FlowMesh worker assigned to a completed DAG task: "
                + redact_secrets(str(exc))
            ) from exc
        _require(
            isinstance(task_detail, Mapping),
            "FlowMesh did not return task metadata needed to verify the worker pin",
        )
        task_results.append(
            _operation_result(
                raw,
                expected_by_key[operation_key],
                task_id=task_id,
                selected_worker_id=identity.worker_id,
                task_detail=task_detail,
                expected_runtime_epochs=runtime_epochs_before,
            )
        )
    _require(
        {row["operation_key"] for row in task_results} == set(expected_by_key),
        "FlowMesh task results do not cover the exact frozen DAG operations",
    )
    task_results.sort(key=lambda row: _STEP_NAMES.index({
        "storage_read": "storage-read",
        "network_transfer": "network-transfer",
        "compute": "compute",
    }[row["operation_kind"]]))
    runtime_epochs_after = _validate_runtime_epochs(
        probe(plan["node_api_urls"], plan["operations"]),
        plan["operations"],
    )
    runtime_binding = _runtime_epoch_binding(
        plan_sha256=plan["plan_sha256"],
        node_api_urls=plan["node_api_urls"],
        operations=plan["operations"],
        before=runtime_epochs_before,
        after=runtime_epochs_after,
    )
    summary = {
        "schema_version": FLOWMESH_CONTAINER_DAG_RUN_SCHEMA_VERSION,
        "status": "COMPLETE",
        "smoke_id": plan["smoke_id"],
        "plan_sha256": plan["plan_sha256"],
        "workflow_id": submitted.workflow_id,
        "task_ids": list(submitted.task_ids),
        "selected_worker": identity.to_public_dict(),
        "operation_keys": [row["operation_key"] for row in plan["operations"]],
        "execution_nodes": [row["execution_node_id"] for row in plan["operations"]],
        "node_api_urls": plan["node_api_urls"],
        "task_result_count": len(task_results),
        "flowmesh_graph_dependencies": {
            "storage-read": [],
            "network-transfer": ["storage-read"],
            "compute": ["network-transfer"],
        },
        "telemetry": _aggregate_telemetry(task_results),
        "telemetry_provenance": {
            "version": TELEMETRY_PROVENANCE_VERSION,
            "fields": dict(TELEMETRY_FIELD_PROVENANCE),
            "disclaimers": list(TELEMETRY_DISCLAIMERS),
        },
        "runtime_epoch_binding": runtime_binding,
        "evidence_class": "orchestration-transport-compute-conformance",
        "llm_called": False,
        "semantic_task_quality_evaluated": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    submission = {
        "workflow_id": submitted.workflow_id,
        "task_ids": list(submitted.task_ids),
        "selected_worker_id": identity.worker_id,
        "workflow_sha256": _sha256_bytes(_canonical_bytes(workflow)),
        "validated_before_submission": True,
        "credentials_recorded": False,
    }
    documents = {
        "flowmesh-container-dag-run.json": _json_bytes(summary),
        "flowmesh-container-dag-submission.json": _json_bytes(submission),
        "flowmesh-container-dag-task-results.jsonl": _jsonl_bytes(task_results),
    }
    documents["SHA256SUMS"] = _checksums(documents)
    target = Path(output_dir).resolve()
    _write_documents(target, documents)
    return {**summary, "output_dir": str(target)}
