from __future__ import annotations

import json
import math
import os
import re
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence
from urllib.parse import urlsplit

from pathfinder.simulator.container_execution import _atomic_write

from .container_conditional_dag import resolve_conditional_container_trial
from .container_dag import (
    FlowMeshContainerDagError,
    _aggregate_telemetry,
    _canonical_bytes,
    _checksums,
    _document_sha256,
    _json_bytes,
    _jsonl_bytes,
    _operation_result,
    _probe_container_runtime_epochs,
    _require_api_timeout_covers,
    _sha256_bytes,
    _task_spec,
    _text,
    _validate_api_task_timeout,
    _validate_node_api_urls,
    _validate_operation,
    _validate_runtime_epochs,
    _workflow_failure,
    derive_operation_lower_bounds,
)
from .container_formal_profile import (
    verify_flowmesh_container_formal_execution_profile,
)
from .container_matrix import (
    FLOWMESH_CONTAINER_MATRIX_PLAN_SCHEMA_VERSION,
    _read_plan as _read_matrix_plan,
    _verify_plan_contents as _verify_matrix_plan_contents,
)
from .container_matrix_coordinator import (
    _read_json as _read_coordinator_json,
    _read_jsonl as _read_coordinator_jsonl,
    verify_flowmesh_container_matrix_coordinator_dry_run,
)
from .contracts import (
    FlowMeshClientProtocol,
    FlowMeshSettings,
    FlowMeshWorkerIdentity,
    SubmittedWorkflow,
    TerminalWorkflow,
)
from .preflight import describe_pinned_worker
from .redaction import redact_secrets
from .task_recovery_evidence import (
    RESULT_UPLOAD_TIMEOUT_FAILURE_CLASS,
    validate_result_upload_timeout_observation,
)
from .adapter import extract_api_executor_result


FLOWMESH_CONTAINER_MATRIX_RUN_CONTRACT_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-matrix-run-contract/v1alpha1"
)
FLOWMESH_CONTAINER_MATRIX_JOURNAL_ENTRY_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-matrix-journal-entry/v1alpha1"
)
FLOWMESH_CONTAINER_MATRIX_CHECKPOINT_ENTRY_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-matrix-trial-checkpoint/v1alpha1"
)
FLOWMESH_CONTAINER_MATRIX_REPLAY_ADOPTED_CHECKPOINT_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-matrix-trial-checkpoint/v1alpha2"
)
FLOWMESH_CONTAINER_MATRIX_TRIAL_RESULT_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-matrix-trial-result/v1alpha1"
)
FLOWMESH_CONTAINER_MATRIX_REPLAY_ADOPTED_TRIAL_RESULT_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-matrix-trial-result/v1alpha2"
)
FLOWMESH_CONTAINER_MATRIX_OPERATION_RESULT_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-matrix-operation-result/v1alpha1"
)
FLOWMESH_CONTAINER_MATRIX_REPLAY_ADOPTED_OPERATION_RESULT_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-matrix-operation-result/v1alpha2"
)
FLOWMESH_CONTAINER_MATRIX_SUBMISSION_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-matrix-submission/v1alpha1"
)
FLOWMESH_CONTAINER_MATRIX_RUN_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-matrix-run/v1alpha1"
)
FLOWMESH_CONTAINER_MATRIX_RECOVERED_RUN_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-matrix-run/v1alpha2"
)
FLOWMESH_CONTAINER_MATRIX_REPLAY_ADOPTED_RUN_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-matrix-run/v1alpha3"
)
FLOWMESH_CONTAINER_MATRIX_FAILURE_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-matrix-failure/v1alpha1"
)

_CONTRACT_FILE = "flowmesh-container-matrix-run-contract.json"
_JOURNAL_FILE = "flowmesh-container-matrix-journal.jsonl"
_CHECKPOINT_FILE = "flowmesh-container-matrix-trial-checkpoints.jsonl"
_RUN_FILE = "flowmesh-container-matrix-run.json"
_TRIAL_RESULTS_FILE = "flowmesh-container-matrix-trial-results.jsonl"
_OPERATION_RESULTS_FILE = "flowmesh-container-matrix-operation-results.jsonl"
_SUBMISSIONS_FILE = "flowmesh-container-matrix-submissions.jsonl"
_FAILURE_FILE = "flowmesh-container-matrix-failure.json"
_FINAL_FILES = {
    _CONTRACT_FILE,
    _JOURNAL_FILE,
    _CHECKPOINT_FILE,
    _RUN_FILE,
    _TRIAL_RESULTS_FILE,
    _OPERATION_RESULTS_FILE,
    _SUBMISSIONS_FILE,
}
_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_RECOVERY_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_ADOPTION_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_PHASE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,31}")
_CONDITIONAL_DESIGNS = frozenset({"D3", "D7"})
_RECOVERY_STATE = "INFRASTRUCTURE_RECOVERY_AUTHORIZED"
_REPLAY_ADOPTION_STATE = "REPLAY_RESULTS_ADOPTION_AUTHORIZED"
_LEGACY_RECOVERABLE_FAILURE_CLASS = (
    "flowmesh-identity-provider-unavailable-before-dispatch"
)
_LEGACY_RECOVERY_IMPLEMENTATION_SCHEMA = (
    "pathfinder.flowmesh-container-matrix-infrastructure-recovery/v1alpha1"
)
_RECOVERABLE_FAILURE_CLASS = (
    "flowmesh-identity-provider-unavailable-at-safe-schedule-root"
)
_RECOVERY_IMPLEMENTATION_SCHEMA = (
    "pathfinder.flowmesh-container-matrix-infrastructure-recovery/v1alpha2"
)
_RESULT_UPLOAD_TIMEOUT_RECOVERABLE_FAILURE_CLASS = (
    RESULT_UPLOAD_TIMEOUT_FAILURE_CLASS
)
_RESULT_UPLOAD_TIMEOUT_RECOVERY_IMPLEMENTATION_SCHEMA = (
    "pathfinder.flowmesh-container-matrix-infrastructure-recovery/v1alpha3"
)
_REPLAY_ADOPTION_IMPLEMENTATION_SCHEMA = (
    "pathfinder.flowmesh-container-matrix-replay-result-adoption/v1alpha1"
)
_REPLAY_ADOPTION_FAILURE_CLASS = (
    "completed-recovery-workflow-with-idempotent-container-replay"
)
_ROOT_ENDPOINT_IDENTITY_SCHEME = (
    "normalized-scheme-host-effective-port-path/v1"
)
_MATRIX_RESULT_AUGMENTED_FIELDS = frozenset(
    {
        "schema_version",
        "sequence_index",
        "trial_key",
        "operation_id",
        "condition",
        "frozen_operation_sha256",
        "planned_logical_bytes",
        "executed",
        "skip_reason",
        "telemetry_recorded",
        "credentials_recorded",
    }
)
_REPLAY_RESULT_PROVENANCE_FIELDS = frozenset(
    {
        "result_carrier_task_id",
        "result_carrier_workflow_id",
        "measurement_origin",
        "original_flowmesh_task_id_known",
        "original_flowmesh_workflow_id_known",
        "measurement_freshness_established",
    }
)
_CACHE_RESULT_OPERATION_KINDS = frozenset(
    {"cache_lookup", "cache_read", "cache_insert"}
)
_CACHE_RESULT_FIELDS = frozenset(
    {"cache_result", "cache_scope_id", "cache_evictions"}
)
_RAW_MATRIX_OPERATION_RESULT_FIELDS = frozenset(
    {
        "task_id",
        "worker_id",
        "operation_key",
        "operation_kind",
        "execution_node_id",
        "destination_node_id",
        "runtime_epoch",
        "destination_runtime_epoch",
        "container_result_schema_version",
        "logical_bytes",
        "physical_bytes",
        "service_time_ms",
        "fixture_materialization_ms_excluded_from_storage_measurement",
        "application_shaping_target_ms",
        "network_http_exchange_ms",
        "application_shaping_sleep_ms",
        "telemetry_provenance_version",
        "telemetry_complete",
        "semantic_task_quality_evaluated",
        "idempotent_replay",
        "api_executor",
        "api_http_status",
        "container_result_sha256",
        "task_detail_available",
        "started_monotonic_ns",
        "finished_monotonic_ns",
        "phase",
    }
)
_INACTIVE_MATRIX_OPERATION_RESULT_FIELDS = frozenset(
    {
        "schema_version",
        "sequence_index",
        "trial_key",
        "operation_key",
        "operation_id",
        "operation_kind",
        "condition",
        "frozen_operation_sha256",
        "planned_logical_bytes",
        "executed",
        "skip_reason",
        "phase",
        "task_id",
        "worker_id",
        "execution_node_id",
        "destination_node_id",
        "runtime_epoch",
        "destination_runtime_epoch",
        "logical_bytes",
        "physical_bytes",
        "service_time_ms",
        "telemetry_recorded",
        "telemetry_complete",
        "semantic_task_quality_evaluated",
        "idempotent_replay",
        "credentials_recorded",
    }
)


class FlowMeshContainerMatrixRunError(FlowMeshContainerDagError):
    """Raised when the formal matrix cannot be executed canonically."""


def _runner_require(condition: bool, message: str) -> None:
    if not condition:
        raise FlowMeshContainerMatrixRunError(message)


def _run_identifier(value: Any) -> str:
    identifier = _text(value, "run_id")
    _runner_require(
        _RUN_ID.fullmatch(identifier) is not None,
        "run_id contains unsupported characters",
    )
    return identifier


def _recovery_identifier(value: Any) -> str:
    identifier = _text(value, "recovery_id")
    _runner_require(
        _RECOVERY_ID.fullmatch(identifier) is not None,
        "recovery_id contains unsupported characters",
    )
    return identifier


def _adoption_identifier(value: Any) -> str:
    identifier = _text(value, "adoption_id")
    _runner_require(
        _ADOPTION_ID.fullmatch(identifier) is not None,
        "adoption_id contains unsupported characters",
    )
    return identifier


def _validated_recovery_reason(value: Any) -> str:
    reason = _text(value, "recovery_reason").strip()
    _runner_require(bool(reason), "recovery_reason must not be empty")
    _runner_require(
        len(reason) <= 1000,
        "recovery_reason must contain at most 1000 characters",
    )
    return reason


def _recovery_reason(value: Any) -> str:
    reason = _validated_recovery_reason(value)
    return _validated_recovery_reason(redact_secrets(reason, limit=1000))


def _validated_adoption_reason(value: Any) -> str:
    reason = _text(value, "adoption_reason").strip()
    _runner_require(bool(reason), "adoption_reason must not be empty")
    _runner_require(
        len(reason) <= 1000,
        "adoption_reason must contain at most 1000 characters",
    )
    return reason


def _adoption_reason(value: Any) -> str:
    reason = _validated_adoption_reason(value)
    return _validated_adoption_reason(redact_secrets(reason, limit=1000))


def _recovery_runner_module_sha256() -> str:
    """Bind recovery evidence to the exact runner module that authorized it."""

    return _sha256_bytes(Path(__file__).read_bytes())


def _normalize_recovery_request(
    recovery_id: str | None,
    recovery_reason: str | None,
    recover_failed_entry_sha256: str | None,
) -> dict[str, str] | None:
    supplied = (
        recovery_id,
        recovery_reason,
        recover_failed_entry_sha256,
    )
    _runner_require(
        all(value is None for value in supplied)
        or all(value is not None for value in supplied),
        "recovery_id, recovery_reason, and recover_failed_entry_sha256 "
        "must be supplied together",
    )
    if recovery_id is None:
        return None
    assert recovery_reason is not None
    assert recover_failed_entry_sha256 is not None
    _runner_require(
        re.fullmatch(r"[0-9a-f]{64}", recover_failed_entry_sha256)
        is not None,
        "recover_failed_entry_sha256 must be a lowercase SHA-256 digest",
    )
    return {
        "recovery_id": _recovery_identifier(recovery_id),
        "recovery_reason": _recovery_reason(recovery_reason),
        "recover_failed_entry_sha256": recover_failed_entry_sha256,
    }


def _normalize_replay_adoption_request(
    adoption_id: str | None,
    adoption_reason: str | None,
    adopt_failed_entry_sha256: str | None,
) -> dict[str, str]:
    supplied = (adoption_id, adoption_reason, adopt_failed_entry_sha256)
    _runner_require(
        all(value is not None for value in supplied),
        "adoption_id, adoption_reason, and adopt_failed_entry_sha256 "
        "must be supplied together",
    )
    assert adoption_id is not None
    assert adoption_reason is not None
    assert adopt_failed_entry_sha256 is not None
    _runner_require(
        re.fullmatch(r"[0-9a-f]{64}", adopt_failed_entry_sha256) is not None,
        "adopt_failed_entry_sha256 must be a lowercase SHA-256 digest",
    )
    return {
        "adoption_id": _adoption_identifier(adoption_id),
        "adoption_reason": _adoption_reason(adoption_reason),
        "adopt_failed_entry_sha256": adopt_failed_entry_sha256,
    }


def _replay_adoption_operation_evidence(
    operation: Mapping[str, Any],
) -> dict[str, Any]:
    """Return and validate the only operation safe for replay adoption.

    This escape hatch is intentionally narrower than the container node's
    general idempotency contract.  It exists only for a dependency-free,
    zero-byte scheduling marker; no storage, network, cache, index, or compute
    result can become canonical through it.
    """

    evidence = {
        "operation_key": operation.get("operation_key"),
        "operation_id": operation.get("operation_id"),
        "operation_kind": operation.get("operation_kind"),
        "operation_adapter": operation.get("operation_adapter"),
        "dependency_operation_keys": operation.get(
            "dependency_operation_keys"
        ),
        "logical_bytes": operation.get("logical_bytes"),
        "execution_node_id": operation.get("execution_node_id"),
        "destination_node_id": operation.get("destination_node_id"),
        "condition": operation.get("condition"),
        "link_adapter": operation.get("link_adapter"),
        "cache_adapter": operation.get("cache_adapter"),
        "cache_scope_id": operation.get("cache_scope_id"),
        "frozen_operation_sha256": _sha256_bytes(
            _canonical_bytes(operation)
        ),
    }
    _runner_require(
        isinstance(evidence["operation_key"], str)
        and bool(evidence["operation_key"])
        and evidence["operation_id"] == "schedule"
        and evidence["operation_kind"] == "control"
        and evidence["operation_adapter"] == "monotonic-control-v1"
        and evidence["dependency_operation_keys"] == []
        and evidence["logical_bytes"] == 0
        and isinstance(evidence["execution_node_id"], str)
        and evidence["execution_node_id"]
        == evidence["destination_node_id"]
        and evidence["condition"] is None
        and evidence["link_adapter"] is None
        and evidence["cache_adapter"] is None
        and evidence["cache_scope_id"] is None,
        "only the dependency-free zero-byte schedule control operation "
        "may be replay-adopted",
    )
    return evidence


def _validate_replay_adoption_operation_evidence(
    evidence: Any,
    *,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    _runner_require(
        isinstance(evidence, Mapping),
        "replay adoption operation evidence is invalid",
    )
    expected_fields = {
        "operation_key",
        "operation_id",
        "operation_kind",
        "operation_adapter",
        "dependency_operation_keys",
        "logical_bytes",
        "execution_node_id",
        "destination_node_id",
        "condition",
        "link_adapter",
        "cache_adapter",
        "cache_scope_id",
        "frozen_operation_sha256",
    }
    operation_key = evidence.get("operation_key")
    _runner_require(
        set(evidence) == expected_fields
        and isinstance(operation_key, str)
        and bool(operation_key)
        and evidence.get("operation_id") == "schedule"
        and evidence.get("operation_kind") == "control"
        and evidence.get("operation_adapter") == "monotonic-control-v1"
        and evidence.get("dependency_operation_keys") == []
        and evidence.get("logical_bytes") == 0
        and isinstance(evidence.get("execution_node_id"), str)
        and evidence.get("execution_node_id")
        == evidence.get("destination_node_id")
        and evidence.get("condition") is None
        and evidence.get("link_adapter") is None
        and evidence.get("cache_adapter") is None
        and evidence.get("cache_scope_id") is None
        and operation_key
        in contract["planned_operation_sha256_by_operation_key"]
        and evidence.get("frozen_operation_sha256")
        == contract["planned_operation_sha256_by_operation_key"][
            operation_key
        ],
        "replay adoption operation evidence changed from the frozen plan",
    )
    return dict(evidence)


def _phase_identifier(value: Any) -> str:
    phase = _text(value, "phase")
    _runner_require(
        _PHASE_ID.fullmatch(phase) is not None,
        "phase contains unsupported characters",
    )
    return phase


def _root_endpoint_identity_sha256(base_url: Any) -> str:
    """Fingerprint the non-secret identity of one FlowMesh Root.

    A Root can be mounted below a path on a shared host, so the existing
    host-only endpoint fingerprint is intentionally not used here.  User
    information, query parameters, and fragments are never retained.  The
    normalized identity itself is also not persisted: only its digest is.
    """

    _runner_require(
        isinstance(base_url, str) and bool(base_url.strip()),
        "FlowMesh Root endpoint is invalid",
    )
    try:
        parsed = urlsplit(base_url.strip())
        port = parsed.port
    except ValueError as exc:
        raise FlowMeshContainerMatrixRunError(
            "FlowMesh Root endpoint is invalid"
        ) from exc
    scheme = parsed.scheme.lower()
    host = parsed.hostname
    _runner_require(
        scheme in {"http", "https"} and isinstance(host, str) and bool(host),
        "FlowMesh Root endpoint is invalid",
    )
    _runner_require(
        parsed.username is None
        and parsed.password is None
        and not parsed.query
        and not parsed.fragment,
        "FlowMesh Root endpoint must not contain credentials, a query, or "
        "a fragment",
    )
    effective_port = port if port is not None else (443 if scheme == "https" else 80)
    normalized_host = host.lower()
    if ":" in normalized_host:
        normalized_host = f"[{normalized_host}]"
    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/") or "/"
    normalized = f"{scheme}://{normalized_host}:{effective_port}{path}"
    return _sha256_bytes(normalized.encode("utf-8"))


def _is_known_atomic_temp(path: Path) -> bool:
    """Recognize only orphan temps produced for this runner's own files."""

    if path.is_symlink() or not path.is_file():
        return False
    known = _FINAL_FILES | {_FAILURE_FILE, "SHA256SUMS"}
    return any(
        re.fullmatch(rf"\.{re.escape(name)}\.[0-9]+\.tmp", path.name)
        is not None
        for name in known
    )


def _visible_artifact_files(root: Path) -> set[str]:
    visible: set[str] = set()
    for path in root.iterdir():
        if _is_known_atomic_temp(path):
            continue
        _runner_require(
            path.is_file() and not path.is_symlink(),
            "matrix run directory contains a non-regular entry",
        )
        visible.add(path.name)
    return visible


def _cleanup_known_atomic_temps(root: Path) -> None:
    """Remove only runner-owned orphan temps before an exclusive resume."""

    for path in root.iterdir():
        if _is_known_atomic_temp(path):
            path.unlink()


@contextmanager
def _exclusive_run_lock(target: Path) -> Iterator[None]:
    """Hold a cross-process advisory lock for one output directory.

    The lock is a stable sibling of the output directory rather than an
    artifact inside it. It intentionally remains after release: unlinking a
    lock file can split later contenders across different inodes. Operating
    system lock release on process exit makes the lease crash-safe.
    """

    target.parent.mkdir(parents=True, exist_ok=True)
    stem = target.name or _sha256_bytes(str(target).encode("utf-8"))[:16]
    lock_path = target.parent / f".{stem}.pathfinder-matrix-run.lock"
    _runner_require(
        not lock_path.is_symlink(),
        "matrix run lock path must not be a symbolic link",
    )
    try:
        handle = lock_path.open("a+b")
    except OSError as exc:
        raise FlowMeshContainerMatrixRunError(
            "cannot open the matrix run concurrency lock"
        ) from exc

    locked = False
    try:
        if os.name == "nt":
            import msvcrt

            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"\0")
                handle.flush()
            handle.seek(0)
            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError:
                raise FlowMeshContainerMatrixRunError(
                    "another process is already using this matrix run "
                    "output directory"
                ) from None
        else:
            import fcntl

            try:
                fcntl.flock(
                    handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB
                )
            except OSError:
                raise FlowMeshContainerMatrixRunError(
                    "another process is already using this matrix run "
                    "output directory"
                ) from None
        locked = True
        yield
    finally:
        if locked:
            try:
                if os.name == "nt":
                    import msvcrt

                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
        handle.close()


def _stable_worker_identity(
    identity: FlowMeshWorkerIdentity,
) -> dict[str, str | None]:
    """Return the worker fields that are stable enough for crash recovery."""

    public = identity.to_public_dict()
    # Root status is expected to move through IDLE/RUNNING/BUSY while a
    # bound workflow is recovered.  It is evidence about the instant of a
    # lookup, not part of the worker's stable identity.
    public["status"] = None
    return public


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FlowMeshContainerMatrixRunError(
            f"{label} is not valid JSON"
        ) from exc
    _runner_require(isinstance(value, dict), f"{label} must be an object")
    return value


def _read_jsonl(path: Path, label: str) -> list[dict[str, Any]]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise FlowMeshContainerMatrixRunError(
            f"cannot read {label}"
        ) from exc
    _runner_require(
        not raw or raw.endswith(b"\n"),
        f"{label} has a torn final row",
    )
    try:
        values = [
            json.loads(line.decode("utf-8"))
            for line in raw.splitlines()
            if line.strip()
        ]
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise FlowMeshContainerMatrixRunError(
            f"{label} is not valid JSONL"
        ) from exc
    _runner_require(
        all(isinstance(row, dict) for row in values),
        f"{label} rows must be objects",
    )
    return values


def _sha256_path(path: Path, label: str) -> str:
    try:
        return _sha256_bytes(path.read_bytes())
    except OSError as exc:
        raise FlowMeshContainerMatrixRunError(
            f"cannot read {label}"
        ) from exc


def _copy(value: Any, label: str) -> dict[str, Any]:
    try:
        copied = json.loads(_canonical_bytes(value).decode("utf-8"))
    except (TypeError, ValueError, UnicodeError, json.JSONDecodeError) as exc:
        raise FlowMeshContainerMatrixRunError(
            f"{label} cannot be canonicalized"
        ) from exc
    _runner_require(isinstance(copied, dict), f"{label} must be an object")
    return copied


def _entry_sha256(entry: Mapping[str, Any], field: str) -> str:
    return _document_sha256(entry, field)


def _append_digest_entry(
    path: Path,
    entry: Mapping[str, Any],
    *,
    digest_field: str,
) -> dict[str, Any]:
    try:
        existing = path.read_bytes()
    except OSError as exc:
        raise FlowMeshContainerMatrixRunError(
            f"cannot read ledger for update: {path.name}"
        ) from exc
    _runner_require(
        not existing or existing.endswith(b"\n"),
        f"{path.name} has a torn final row",
    )
    copied = _copy(entry, "ledger entry")
    copied[digest_field] = _entry_sha256(copied, digest_field)
    _atomic_write(path, existing + _canonical_bytes(copied) + b"\n")
    return copied


def _safe_workflow_name(run_id: str, trial_key: str, phase: str) -> str:
    suffix = _sha256_bytes(
        _canonical_bytes([run_id, trial_key, phase])
    )[:12]
    prefix = re.sub(r"[^a-z0-9-]+", "-", run_id.lower()).strip("-")
    prefix = prefix[:28] or "matrix"
    return f"pathfinder-matrix-{prefix}-{suffix}"


def _validate_dependencies(
    operations: Sequence[Mapping[str, Any]],
    dependencies: Mapping[str, Sequence[str]],
) -> tuple[list[dict[str, Any]], dict[str, list[str]]]:
    copied = [_validate_operation(row) for row in operations]
    _runner_require(bool(copied), "matrix trial workflow cannot be empty")
    keys = [str(row["operation_key"]) for row in copied]
    _runner_require(
        len(keys) == len(set(keys)),
        "matrix trial workflow contains duplicate operation keys",
    )
    _runner_require(
        set(dependencies) == set(keys),
        "matrix trial dependency map does not cover exactly its operations",
    )
    normalized: dict[str, list[str]] = {}
    for key in keys:
        raw = dependencies[key]
        _runner_require(
            isinstance(raw, Sequence)
            and not isinstance(raw, (str, bytes))
            and all(isinstance(item, str) and item for item in raw),
            f"matrix trial dependencies are invalid for {key}",
        )
        values = list(raw)
        _runner_require(
            len(values) == len(set(values)),
            f"matrix trial dependencies repeat for {key}",
        )
        _runner_require(
            key not in values and all(item in keys for item in values),
            f"matrix trial dependencies escape the phase for {key}",
        )
        normalized[key] = values

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(key: str) -> None:
        _runner_require(
            key not in visiting,
            "matrix trial dependency graph contains a cycle",
        )
        if key in visited:
            return
        visiting.add(key)
        for dependency in normalized[key]:
            visit(dependency)
        visiting.remove(key)
        visited.add(key)

    for key in keys:
        visit(key)
    return copied, normalized


def build_flowmesh_container_matrix_trial_workflow(
    operations: Sequence[Mapping[str, Any]],
    dependencies: Mapping[str, Sequence[str]],
    node_api_urls: Mapping[str, str],
    selected_worker_id: str,
    run_id: str,
    trial_key: str,
    phase: str,
    owner: str,
    api_task_timeout_seconds: int,
) -> dict[str, Any]:
    """Build one worker-pinned arbitrary DAG for a frozen matrix phase.

    Conditional choices are deliberately not made here.  The caller must
    first resolve a D3/D7 trial, then pass only phase A or the selected phase
    B operations and the corresponding cross-phase-pruned dependencies.
    """

    identifier = _run_identifier(run_id)
    phase_id = _phase_identifier(phase)
    expected_trial_key = _text(trial_key, "trial_key")
    copied, normalized_dependencies = _validate_dependencies(
        operations, dependencies
    )
    _runner_require(
        all(row["trial_key"] == expected_trial_key for row in copied),
        "matrix trial workflow mixes trial keys",
    )
    urls = _validate_node_api_urls(copied, node_api_urls)
    timeout = _validate_api_task_timeout(api_task_timeout_seconds)
    _require_api_timeout_covers(derive_operation_lower_bounds(copied), timeout)
    worker_id = _text(selected_worker_id, "selected_worker_id")
    operation_keys = [str(row["operation_key"]) for row in copied]
    name_by_key = {
        key: (
            f"operation-{index:03d}-"
            + str(row["operation_kind"]).replace("_", "-")
        )
        for index, (key, row) in enumerate(
            zip(operation_keys, copied, strict=True), start=1
        )
    }
    nodes: list[dict[str, Any]] = []
    for row in copied:
        key = str(row["operation_key"])
        node: dict[str, Any] = {
            "name": name_by_key[key],
            "spec": _task_spec(
                row,
                urls[str(row["execution_node_id"])],
                timeout,
            ),
        }
        if normalized_dependencies[key]:
            node["dependsOn"] = [
                name_by_key[value]
                for value in normalized_dependencies[key]
            ]
        nodes.append(node)
    return {
        "apiVersion": "flowmesh/v1",
        "kind": "APITask",
        "metadata": {
            "name": _safe_workflow_name(
                identifier, expected_trial_key, phase_id
            ),
            "owner": _text(owner, "owner"),
            "annotations": {
                "schedule_hint": {"selected_worker": worker_id},
                "custom": {
                    "pathfinder_matrix_run_id": identifier,
                    "pathfinder_trial_key": expected_trial_key,
                    "pathfinder_matrix_phase": phase_id,
                    "pathfinder_operation_keys": operation_keys,
                    "pathfinder_evidence_class": (
                        "orchestration-infrastructure-conformance"
                    ),
                    "pathfinder_semantic_quality_evaluated": False,
                },
            },
        },
        "spec": {"graph": {"nodes": nodes}},
    }


def _load_sources(
    matrix_plan_dir: str | Path,
    formal_execution_profile_dir: str | Path,
    coordinator_plan_dir: str | Path,
) -> dict[str, Any]:
    matrix_root = Path(matrix_plan_dir).resolve()
    profile_root = Path(formal_execution_profile_dir).resolve()
    coordinator_root = Path(coordinator_plan_dir).resolve()
    try:
        (
            matrix_root,
            matrix,
            trials,
            operations,
            admission,
        ) = _read_matrix_plan(matrix_root)
        matrix_verified = _verify_matrix_plan_contents(
            matrix_root, matrix, trials, operations, admission
        )
        profile_verified = verify_flowmesh_container_formal_execution_profile(
            profile_root
        )
        coordinator_verified = (
            verify_flowmesh_container_matrix_coordinator_dry_run(
                coordinator_root,
                matrix_plan_dir=matrix_root,
                formal_execution_profile_dir=profile_root,
            )
        )
    except Exception as exc:
        raise FlowMeshContainerMatrixRunError(
            "formal matrix sources do not verify: "
            + redact_secrets(str(exc))
        ) from exc
    _runner_require(
        matrix.get("schema_version")
        == FLOWMESH_CONTAINER_MATRIX_PLAN_SCHEMA_VERSION,
        "formal matrix runner requires a v2 matrix plan",
    )
    profile = _read_json(
        profile_root / "flowmesh-container-formal-execution-profile.json",
        "formal execution profile",
    )
    coordinator = _read_coordinator_json(
        coordinator_root / "flowmesh-container-matrix-coordinator-plan.json",
        "matrix coordinator plan",
    )
    wrappers = _read_coordinator_jsonl(
        coordinator_root
        / "flowmesh-container-matrix-coordinator-trial-wrappers.jsonl",
        "matrix coordinator trial wrappers",
    )
    _runner_require(
        matrix_verified.get("status") == "VERIFIED"
        and profile_verified.get("status") == "VERIFIED"
        and coordinator_verified.get("status") == "VERIFIED",
        "formal matrix source verification did not complete",
    )
    _runner_require(
        len(trials) == len(wrappers) == 64,
        "formal matrix runner requires exactly 64 trial wrappers",
    )
    _runner_require(
        len(operations) == 500,
        "formal matrix runner requires exactly 500 operations",
    )
    return {
        "matrix_root": matrix_root,
        "profile_root": profile_root,
        "coordinator_root": coordinator_root,
        "matrix": matrix,
        "profile": profile,
        "coordinator": coordinator,
        "trials": trials,
        "operations": operations,
        "wrappers": wrappers,
    }


def _operations_by_trial(
    operations: Sequence[Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for raw in operations:
        row = _copy(raw, "matrix operation")
        result.setdefault(str(row["trial_key"]), []).append(row)
    return result


def _expected_execution(
    wrappers: Sequence[Mapping[str, Any]],
    operations_by_trial: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    active: list[str] = []
    inactive: list[str] = []
    workflow_count = 0
    resolution_sha256_by_trial: dict[str, str | None] = {}
    phase_operation_keys_by_trial: dict[str, dict[str, list[str]]] = {}
    for wrapper in wrappers:
        trial_key = str(wrapper["trial_key"])
        rows = operations_by_trial[trial_key]
        if str(wrapper["design_id"]) in _CONDITIONAL_DESIGNS:
            resolution = resolve_conditional_container_trial(
                rows, trial_key=trial_key
            )
            active_set = set(resolution["active_operation_keys"])
            inactive_set = set(resolution["inactive_operation_keys"])
            active.extend(
                str(row["operation_key"])
                for row in rows
                if row["operation_key"] in active_set
            )
            inactive.extend(
                str(row["operation_key"])
                for row in rows
                if row["operation_key"] in inactive_set
            )
            workflow_count += 2
            resolution_sha256_by_trial[trial_key] = _sha256_bytes(
                _canonical_bytes(resolution)
            )
            phase_operation_keys_by_trial[trial_key] = {
                "A": [
                    str(key) for key in resolution["phase_a_operation_keys"]
                ],
                "B": [
                    str(key) for key in resolution["phase_b_operation_keys"]
                ],
            }
        else:
            keys = [str(row["operation_key"]) for row in rows]
            active.extend(keys)
            workflow_count += 1
            resolution_sha256_by_trial[trial_key] = None
            phase_operation_keys_by_trial[trial_key] = {
                "unconditional": keys
            }
    return {
        "active_operation_keys": active,
        "inactive_operation_keys": inactive,
        "workflow_count": workflow_count,
        "resolution_sha256_by_trial": resolution_sha256_by_trial,
        "phase_operation_keys_by_trial": phase_operation_keys_by_trial,
    }


def _source_binding(sources: Mapping[str, Any]) -> dict[str, Any]:
    matrix_root = sources["matrix_root"]
    profile_root = sources["profile_root"]
    coordinator_root = sources["coordinator_root"]
    matrix = sources["matrix"]
    profile = sources["profile"]
    coordinator = sources["coordinator"]
    assert isinstance(matrix_root, Path)
    assert isinstance(profile_root, Path)
    assert isinstance(coordinator_root, Path)
    assert isinstance(matrix, Mapping)
    assert isinstance(profile, Mapping)
    assert isinstance(coordinator, Mapping)
    return {
        "matrix_plan_sha256": matrix["plan_sha256"],
        "matrix_plan_file_sha256": _sha256_path(
            matrix_root / "flowmesh-container-matrix-plan.json",
            "matrix plan",
        ),
        "matrix_operations_sha256": matrix["matrix_operations_sha256"],
        "formal_execution_profile_sha256": profile["profile_sha256"],
        "formal_execution_profile_file_sha256": _sha256_path(
            profile_root
            / "flowmesh-container-formal-execution-profile.json",
            "formal execution profile",
        ),
        "coordinator_plan_sha256": coordinator["plan_sha256"],
        "coordinator_plan_file_sha256": _sha256_path(
            coordinator_root
            / "flowmesh-container-matrix-coordinator-plan.json",
            "matrix coordinator plan",
        ),
        "coordinator_wrapper_ledger_sha256": _sha256_path(
            coordinator_root
            / "flowmesh-container-matrix-coordinator-trial-wrappers.jsonl",
            "matrix coordinator wrapper ledger",
        ),
    }


def _build_contract(
    *,
    sources: Mapping[str, Any],
    run_id: str,
    identity: FlowMeshWorkerIdentity,
    runtime_epochs: Mapping[str, str],
    settings: FlowMeshSettings,
) -> dict[str, Any]:
    matrix = sources["matrix"]
    wrappers = sources["wrappers"]
    operations = sources["operations"]
    assert isinstance(matrix, Mapping)
    assert isinstance(wrappers, Sequence)
    assert isinstance(operations, Sequence)
    by_trial = _operations_by_trial(operations)
    expected = _expected_execution(wrappers, by_trial)
    wrapper_digests = {
        str(row["trial_key"]): _sha256_bytes(_canonical_bytes(row))
        for row in wrappers
    }
    contract: dict[str, Any] = {
        "schema_version": (
            FLOWMESH_CONTAINER_MATRIX_RUN_CONTRACT_SCHEMA_VERSION
        ),
        "run_id": run_id,
        "matrix_id": matrix["matrix_id"],
        "source_binding": _source_binding(sources),
        "ordered_trial_keys": [str(row["trial_key"]) for row in wrappers],
        "trial_wrapper_sha256_by_trial_key": wrapper_digests,
        "planned_operation_keys": [
            str(row["operation_key"]) for row in operations
        ],
        "planned_operation_sha256_by_operation_key": {
            str(row["operation_key"]): _sha256_bytes(_canonical_bytes(row))
            for row in operations
        },
        "expected_active_operation_keys": expected["active_operation_keys"],
        "expected_inactive_operation_keys": expected[
            "inactive_operation_keys"
        ],
        "expected_workflow_count": expected["workflow_count"],
        "resolution_sha256_by_trial_key": expected[
            "resolution_sha256_by_trial"
        ],
        "expected_phase_operation_keys_by_trial_key": expected[
            "phase_operation_keys_by_trial"
        ],
        "planned_trial_count": len(wrappers),
        "planned_operation_count": len(operations),
        "conditional_trial_count": sum(
            str(row["design_id"]) in _CONDITIONAL_DESIGNS
            for row in wrappers
        ),
        "worker_alias": matrix["worker_alias"],
        "selected_worker": _stable_worker_identity(identity),
        "flowmesh_root_endpoint_identity_scheme": (
            _ROOT_ENDPOINT_IDENTITY_SCHEME
        ),
        "flowmesh_root_endpoint_identity_sha256": (
            _root_endpoint_identity_sha256(settings.base_url)
        ),
        "node_api_urls": dict(matrix["node_api_urls"]),
        "node_api_urls_sha256": _sha256_bytes(
            _canonical_bytes(matrix["node_api_urls"])
        ),
        "runtime_epochs": dict(sorted(runtime_epochs.items())),
        "runtime_epochs_sha256": _sha256_bytes(
            _canonical_bytes(dict(sorted(runtime_epochs.items())))
        ),
        "api_task_timeout_seconds": matrix["api_task_timeout_seconds"],
        "poll_interval_seconds": float(settings.poll_interval_seconds),
        "owner": settings.owner,
        "primary_trial_wrapper_max_concurrency": 1,
        "global_serial_execution_required": True,
        "same_cache_lane_serial_execution_required": True,
        "cross_lane_parallelism_authorized": False,
        "resume_requires_same_worker_id": True,
        "resume_requires_same_runtime_epochs": True,
        "ambiguous_submission_must_fail_closed": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    contract["contract_sha256"] = _document_sha256(
        contract, "contract_sha256"
    )
    return contract


def _validate_contract(contract: Mapping[str, Any]) -> None:
    _runner_require(
        contract.get("schema_version")
        == FLOWMESH_CONTAINER_MATRIX_RUN_CONTRACT_SCHEMA_VERSION,
        "unsupported matrix run contract schema",
    )
    _runner_require(
        contract.get("contract_sha256")
        == _document_sha256(contract, "contract_sha256"),
        "matrix run contract digest mismatch",
    )
    _run_identifier(contract.get("run_id"))
    ordered = contract.get("ordered_trial_keys")
    planned = contract.get("planned_operation_keys")
    active = contract.get("expected_active_operation_keys")
    inactive = contract.get("expected_inactive_operation_keys")
    _runner_require(
        isinstance(ordered, list)
        and len(ordered) == 64
        and len(set(ordered)) == 64,
        "matrix run contract does not bind 64 unique trials",
    )
    _runner_require(
        isinstance(planned, list)
        and len(planned) == 500
        and len(set(planned)) == 500,
        "matrix run contract does not bind 500 unique operations",
    )
    _runner_require(
        isinstance(active, list)
        and isinstance(inactive, list)
        and set(active).isdisjoint(inactive)
        and set(active) | set(inactive) == set(planned),
        "matrix run contract active/inactive operation partition is invalid",
    )
    _runner_require(
        contract.get("planned_trial_count") == 64
        and contract.get("planned_operation_count") == 500
        and contract.get("conditional_trial_count") == 16,
        "matrix run contract formal dimensions changed",
    )
    wrapper_digests = contract.get("trial_wrapper_sha256_by_trial_key")
    resolution_digests = contract.get("resolution_sha256_by_trial_key")
    operation_digests = contract.get(
        "planned_operation_sha256_by_operation_key"
    )
    phase_operation_keys = contract.get(
        "expected_phase_operation_keys_by_trial_key"
    )
    _runner_require(
        isinstance(wrapper_digests, Mapping)
        and set(wrapper_digests) == set(ordered)
        and all(
            re.fullmatch(r"[0-9a-f]{64}", str(value)) is not None
            for value in wrapper_digests.values()
        ),
        "matrix run contract wrapper digest map is invalid",
    )
    _runner_require(
        isinstance(operation_digests, Mapping)
        and set(operation_digests) == set(planned)
        and all(
            re.fullmatch(r"[0-9a-f]{64}", str(value)) is not None
            for value in operation_digests.values()
        ),
        "matrix run contract operation digest map is invalid",
    )
    _runner_require(
        isinstance(resolution_digests, Mapping)
        and set(resolution_digests) == set(ordered)
        and sum(value is not None for value in resolution_digests.values())
        == 16
        and all(
            value is None
            or re.fullmatch(r"[0-9a-f]{64}", str(value)) is not None
            for value in resolution_digests.values()
        ),
        "matrix run contract resolution digest map is invalid",
    )
    _runner_require(
        isinstance(phase_operation_keys, Mapping)
        and set(phase_operation_keys) == set(ordered)
        and all(
            isinstance(phases, Mapping)
            and set(phases)
            in ({"unconditional"}, {"A", "B"})
            and all(
                isinstance(keys, list)
                and bool(keys)
                and all(isinstance(key, str) and key for key in keys)
                and len(keys) == len(set(keys))
                for keys in phases.values()
            )
            for phases in phase_operation_keys.values()
        )
        and len(
            [
                key
                for trial_key in ordered
                for keys in phase_operation_keys[trial_key].values()
                for key in keys
            ]
        )
        == len(active)
        and {
            key
            for trial_key in ordered
            for keys in phase_operation_keys[trial_key].values()
            for key in keys
        }
        == set(active),
        "matrix run contract phase operation map is invalid",
    )
    _runner_require(
        contract.get("primary_trial_wrapper_max_concurrency") == 1
        and contract.get("global_serial_execution_required") is True
        and contract.get("same_cache_lane_serial_execution_required") is True
        and contract.get("cross_lane_parallelism_authorized") is False,
        "matrix run contract serial admission changed",
    )
    runtime_epochs = contract.get("runtime_epochs")
    _runner_require(
        isinstance(runtime_epochs, Mapping) and len(runtime_epochs) == 8,
        "matrix run contract does not bind all eight runtime epochs",
    )
    epoch_probe_rows = [
        {
            "execution_node_id": node_id,
            "destination_node_id": node_id,
            "operation_kind": "control",
        }
        for node_id in runtime_epochs
    ]
    try:
        normalized_runtime_epochs = _validate_runtime_epochs(
            runtime_epochs, epoch_probe_rows
        )
    except FlowMeshContainerDagError as exc:
        raise FlowMeshContainerMatrixRunError(
            "matrix run runtime epoch binding is invalid"
        ) from exc
    _runner_require(
        len(set(normalized_runtime_epochs.values())) == 8
        and contract.get("runtime_epochs_sha256")
        == _sha256_bytes(_canonical_bytes(normalized_runtime_epochs)),
        "matrix run runtime epoch digest mismatch",
    )
    node_api_urls = contract.get("node_api_urls")
    _runner_require(
        isinstance(node_api_urls, Mapping)
        and len(node_api_urls) == 8
        and all(
            isinstance(node_id, str) and bool(node_id)
            for node_id in node_api_urls
        ),
        "matrix run node API URL binding is invalid",
    )
    url_probe_rows = [
        {
            "execution_node_id": node_id,
            "destination_node_id": node_id,
            "operation_kind": "control",
        }
        for node_id in node_api_urls
    ]
    try:
        normalized_node_api_urls = _validate_node_api_urls(
            url_probe_rows, node_api_urls
        )
    except FlowMeshContainerDagError as exc:
        raise FlowMeshContainerMatrixRunError(
            "matrix run node API URL binding is invalid"
        ) from exc
    _runner_require(
        dict(node_api_urls) == normalized_node_api_urls
        and set(normalized_node_api_urls) == set(normalized_runtime_epochs)
        and contract.get("node_api_urls_sha256")
        == _sha256_bytes(_canonical_bytes(normalized_node_api_urls)),
        "matrix run node API URL digest mismatch",
    )
    worker = contract.get("selected_worker")
    _runner_require(
        isinstance(worker, Mapping)
        and isinstance(worker.get("worker_id"), str)
        and bool(worker["worker_id"])
        and worker.get("alias") == contract.get("worker_alias")
        and worker.get("status") is None,
        "matrix run stable worker identity is invalid",
    )
    _runner_require(
        contract.get("flowmesh_root_endpoint_identity_scheme")
        == _ROOT_ENDPOINT_IDENTITY_SCHEME
        and re.fullmatch(
            r"[0-9a-f]{64}",
            str(contract.get("flowmesh_root_endpoint_identity_sha256", "")),
        )
        is not None,
        "matrix run FlowMesh Root identity binding is invalid",
    )
    _runner_require(
        contract.get("credentials_recorded") is False
        and contract.get("eligible_for_scientific_claims") is False,
        "matrix run contract safety boundary changed",
    )


def _validate_failure_document(
    failure: Mapping[str, Any],
    contract: Mapping[str, Any],
    checkpoints: Sequence[Mapping[str, Any]],
    journal: Sequence[Mapping[str, Any]],
) -> None:
    expected_fields = {
        "schema_version",
        "status",
        "run_id",
        "matrix_plan_sha256",
        "sequence_index",
        "trial_key",
        "phase",
        "operation_keys",
        "completed_trial_count",
        "error_type",
        "error",
        "later_trials_submitted",
        "credentials_recorded",
        "eligible_for_scientific_claims",
        "failure_sha256",
    }
    failures = [row for row in journal if row.get("state") == "RUN_FAILED"]
    _runner_require(bool(failures), "matrix failure document has no journal failure")
    first = failures[0]
    operation_keys = failure.get("operation_keys")
    first_phase = first.get("phase")
    expected_operation_keys = None
    if first_phase in {"unconditional", "A", "B"}:
        expected_operation_keys = contract[
            "expected_phase_operation_keys_by_trial_key"
        ][str(first["trial_key"])][str(first_phase)]
    _runner_require(
        set(failure) == expected_fields
        and failure.get("schema_version")
        == FLOWMESH_CONTAINER_MATRIX_FAILURE_SCHEMA_VERSION
        and failure.get("status") == "FAILED"
        and failure.get("failure_sha256")
        == _document_sha256(failure, "failure_sha256")
        and failure.get("run_id") == contract["run_id"]
        and failure.get("matrix_plan_sha256")
        == contract["source_binding"]["matrix_plan_sha256"]
        and failure.get("sequence_index") == first.get("sequence_index")
        and failure.get("trial_key") == first.get("trial_key")
        and failure.get("phase") == first.get("phase")
        # The failure document is immutable evidence for the first failure.
        # A successfully recovered run can later contain more checkpoints.
        and type(failure.get("completed_trial_count")) is int
        and failure.get("completed_trial_count") == first.get("sequence_index")
        and failure["completed_trial_count"] <= len(checkpoints)
        and isinstance(operation_keys, list)
        and bool(operation_keys)
        and all(isinstance(key, str) and key for key in operation_keys)
        and len(operation_keys) == len(set(operation_keys))
        and set(operation_keys).issubset(contract["planned_operation_keys"])
        and (
            expected_operation_keys is None
            or operation_keys == expected_operation_keys
        )
        and isinstance(failure.get("error_type"), str)
        and bool(failure["error_type"])
        and failure.get("error") == first.get("payload", {}).get("error")
        and failure.get("later_trials_submitted") is False
        and failure.get("credentials_recorded") is False
        and failure.get("eligible_for_scientific_claims") is False,
        "matrix failure document is invalid",
    )


def _normalize_task_evidence(
    task_id: str,
    detail: Mapping[str, Any],
) -> dict[str, Any]:
    status = str(detail.get("task_status") or "").strip().upper()
    attempts = detail.get("attempts")
    maximum = detail.get("max_attempts")
    assigned = detail.get("assigned_worker")
    last_failed = detail.get("last_failed_worker")
    message = detail.get("detail")
    _runner_require(
        bool(status)
        and type(attempts) is int
        and attempts >= 0
        and type(maximum) is int
        and maximum >= attempts
        and (assigned is None or isinstance(assigned, str))
        and (last_failed is None or isinstance(last_failed, str))
        and (message is None or isinstance(message, str)),
        "FlowMesh task evidence is incomplete for infrastructure recovery",
    )
    return {
        "task_id": task_id,
        "task_status": status,
        "attempts": attempts,
        "max_attempts": maximum,
        "assigned_worker": assigned,
        "last_failed_worker": last_failed,
        "detail": redact_secrets(message) if message else None,
    }


def _classify_recoverable_primary_failure(
    task_id: str,
    detail: str,
    *,
    result_upload_timeout_observation: Mapping[str, Any] | None = None,
) -> str | None:
    identity_provider_match = re.fullmatch(
        rf"HTTP delivery for task {re.escape(task_id)} returned "
        r"status 503: (.+)",
        detail,
    )
    identity_provider_body: Any = None
    if identity_provider_match is not None:
        try:
            identity_provider_body = json.loads(
                identity_provider_match.group(1)
            )
        except json.JSONDecodeError:
            identity_provider_body = None
    if identity_provider_body == {
        "detail": "Identity provider unavailable"
    }:
        return _RECOVERABLE_FAILURE_CLASS
    if (
        validate_result_upload_timeout_observation(
            result_upload_timeout_observation,
            expected_task_id=task_id,
            expected_redacted_detail=detail,
        )
        is not None
    ):
        return _RESULT_UPLOAD_TIMEOUT_RECOVERABLE_FAILURE_CLASS
    return None


def _validate_recoverable_task_evidence(
    evidence: Sequence[Mapping[str, Any]],
    *,
    bound_task_ids: Sequence[str],
    failed_task_ids: Sequence[str],
    selected_worker_id: str,
    expected_failure_class: str | None = None,
    result_upload_timeout_observation: Mapping[str, Any] | None = None,
) -> str:
    _runner_require(
        len(evidence) == len(bound_task_ids)
        and [row.get("task_id") for row in evidence]
        == list(bound_task_ids)
        and bool(failed_task_ids)
        and len(failed_task_ids) == len(set(failed_task_ids))
        and set(failed_task_ids).issubset(bound_task_ids),
        "infrastructure recovery task coverage is invalid",
    )
    primary_failures: list[str] = []
    dependency_failures: list[str] = []
    successful_states = {
        "DONE",
        "COMPLETE",
        "COMPLETED",
        "SUCCESS",
        "SUCCEEDED",
    }
    classified_failure: str | None = None
    for row in evidence:
        _runner_require(
            set(row)
            == {
                "task_id",
                "task_status",
                "attempts",
                "max_attempts",
                "assigned_worker",
                "last_failed_worker",
                "detail",
            },
            "infrastructure recovery task evidence shape changed",
        )
        status = row.get("task_status")
        attempts = row.get("attempts")
        maximum = row.get("max_attempts")
        assigned = row.get("assigned_worker")
        last_failed = row.get("last_failed_worker")
        detail = row.get("detail")
        _runner_require(
            isinstance(status, str)
            and bool(status)
            and status not in successful_states
            and type(attempts) is int
            and attempts >= 0
            and type(maximum) is int
            and maximum >= attempts
            and assigned in {None, selected_worker_id}
            and last_failed in {None, selected_worker_id}
            and (detail is None or isinstance(detail, str)),
            "infrastructure recovery task evidence is unsafe",
        )
        if attempts > 0:
            task_id = str(row["task_id"])
            failure_class = _classify_recoverable_primary_failure(
                task_id,
                detail or "",
                result_upload_timeout_observation=(
                    result_upload_timeout_observation
                ),
            )
            _runner_require(
                status == "FAILED"
                and attempts == 1
                and task_id in failed_task_ids
                and assigned == selected_worker_id
                and last_failed == selected_worker_id
                and failure_class is not None,
                "an attempted task did not fail solely at an allowlisted "
                "delivery boundary",
            )
            classified_failure = failure_class
            primary_failures.append(task_id)
        elif status == "FAILED":
            _runner_require(
                isinstance(detail, str)
                and re.fullmatch(r"Dependency \S+ failed", detail) is not None
                and assigned is None
                and last_failed is None,
                "an unattempted failed task is not dependency fallout",
            )
            dependency_failures.append(str(row["task_id"]))
        else:
            _runner_require(
                status == "PENDING"
                and detail is None
                and assigned is None
                and last_failed is None,
                "an unattempted task is not pristine and pending",
            )
    _runner_require(
        len(primary_failures) == 1,
        "infrastructure recovery requires exactly one primary delivery "
        "failure",
    )
    primary = primary_failures[0]
    for row in evidence:
        if row["task_id"] in dependency_failures:
            _runner_require(
                row["detail"] == f"Dependency {primary} failed",
                "dependency failure does not bind the primary delivery "
                "failure",
            )
    _runner_require(
        set(failed_task_ids) == {primary, *dependency_failures},
        "Root-reported failed tasks differ from the classified failures",
    )
    validated_failure_class = classified_failure
    if (
        expected_failure_class == _LEGACY_RECOVERABLE_FAILURE_CLASS
        and classified_failure == _RECOVERABLE_FAILURE_CLASS
    ):
        # V1alpha1 stored a before-dispatch label for the same exact 503 task
        # evidence.  Preserve offline readability without allowing builders
        # to emit that superseded interpretation.
        validated_failure_class = _LEGACY_RECOVERABLE_FAILURE_CLASS
    _runner_require(
        validated_failure_class is not None
        and (
            expected_failure_class is None
            or validated_failure_class == expected_failure_class
        ),
        "infrastructure recovery failure classification changed",
    )
    return validated_failure_class


def _recovery_schedule_safety_evidence(
    phase_operations: Sequence[Mapping[str, Any]],
    *,
    bound_task_ids: Sequence[str],
    task_evidence: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Prove that the sole delivery failure is the phase's schedule root."""

    try:
        checked = [_validate_operation(row) for row in phase_operations]
    except FlowMeshContainerDagError as exc:
        raise FlowMeshContainerMatrixRunError(
            "infrastructure recovery phase operations are invalid"
        ) from exc
    operation_keys = [str(row["operation_key"]) for row in checked]
    _runner_require(
        bool(checked)
        and len(checked) == len(bound_task_ids) == len(task_evidence)
        and len(bound_task_ids) == len(set(bound_task_ids))
        and [row.get("task_id") for row in task_evidence]
        == list(bound_task_ids),
        "infrastructure recovery cannot bind tasks to frozen operations",
    )
    primary_rows = [row for row in task_evidence if row.get("attempts", 0) > 0]
    _runner_require(
        len(primary_rows) == 1,
        "infrastructure recovery has no unique primary delivery failure",
    )
    primary_task_id = str(primary_rows[0]["task_id"])
    primary_index = list(bound_task_ids).index(primary_task_id)
    primary_operation = checked[primary_index]
    try:
        schedule_evidence = _replay_adoption_operation_evidence(
            primary_operation
        )
    except FlowMeshContainerMatrixRunError as exc:
        raise FlowMeshContainerMatrixRunError(
            "infrastructure recovery requires the attempted task to map "
            "to the dependency-free zero-byte schedule control root"
        ) from exc
    schedule_key = str(primary_operation["operation_key"])
    schedule_candidates = [
        row
        for row in checked
        if row.get("operation_id") == "schedule"
        and row.get("operation_kind") == "control"
        and row.get("logical_bytes") == 0
        and row.get("dependency_operation_keys") == []
        and row.get("operation_adapter") == "monotonic-control-v1"
    ]
    _runner_require(
        len(schedule_candidates) == 1
        and schedule_candidates[0]["operation_key"] == schedule_key,
        "infrastructure recovery requires one unique schedule control root",
    )

    dependency_map = {
        str(row["operation_key"]): [
            str(value) for value in row["dependency_operation_keys"]
        ]
        for row in checked
    }
    _runner_require(
        all(
            dependency in dependency_map
            for dependencies in dependency_map.values()
            for dependency in dependencies
        ),
        "infrastructure recovery phase has an external dependency",
    )

    def descends_from_schedule(operation_key: str) -> bool:
        pending = list(dependency_map[operation_key])
        visited: set[str] = set()
        while pending:
            dependency = pending.pop()
            if dependency == schedule_key:
                return True
            if dependency not in visited:
                visited.add(dependency)
                pending.extend(dependency_map[dependency])
        return False

    downstream_keys = [key for key in operation_keys if key != schedule_key]
    _runner_require(
        all(descends_from_schedule(key) for key in downstream_keys),
        "infrastructure recovery requires every other phase operation to "
        "be transitively downstream of the schedule control",
    )
    return {
        # Include the exact frozen rows so an offline verifier can recompute
        # both their contract digests and the complete dependency proof.
        "frozen_phase_operations": checked,
        "phase_operation_keys": operation_keys,
        "task_to_operation_bindings": [
            {"task_id": task_id, "operation_key": operation_key}
            for task_id, operation_key in zip(bound_task_ids, operation_keys)
        ],
        "primary_failed_task_id": primary_task_id,
        "primary_failed_operation_key": schedule_key,
        "schedule_operation_evidence": schedule_evidence,
        "transitively_downstream_operation_keys": downstream_keys,
        "all_other_phase_operations_transitively_downstream": True,
    }


def _validate_recovery_schedule_safety_evidence(
    evidence: Any,
    *,
    contract: Mapping[str, Any],
    bound_task_ids: Sequence[str],
    task_evidence: Sequence[Mapping[str, Any]],
    expected_phase_operation_keys: Sequence[str],
) -> None:
    _runner_require(
        isinstance(evidence, Mapping),
        "infrastructure recovery schedule-root evidence is invalid",
    )
    expected_fields = {
        "frozen_phase_operations",
        "phase_operation_keys",
        "task_to_operation_bindings",
        "primary_failed_task_id",
        "primary_failed_operation_key",
        "schedule_operation_evidence",
        "transitively_downstream_operation_keys",
        "all_other_phase_operations_transitively_downstream",
    }
    frozen_phase_operations = evidence.get("frozen_phase_operations")
    bindings = evidence.get("task_to_operation_bindings")
    _runner_require(
        isinstance(frozen_phase_operations, list),
        "infrastructure recovery frozen phase evidence is invalid",
    )
    try:
        checked = [
            _validate_operation(row) for row in frozen_phase_operations
        ]
    except (FlowMeshContainerDagError, TypeError) as exc:
        raise FlowMeshContainerMatrixRunError(
            "infrastructure recovery frozen phase evidence is invalid"
        ) from exc
    operation_keys = [str(row["operation_key"]) for row in checked]
    operation_digests = contract[
        "planned_operation_sha256_by_operation_key"
    ]
    _runner_require(
        set(evidence) == expected_fields
        and operation_keys == list(expected_phase_operation_keys)
        and all(
            _sha256_bytes(_canonical_bytes(operation))
            == operation_digests.get(operation["operation_key"])
            for operation in checked
        )
        and isinstance(bindings, list),
        "infrastructure recovery schedule-root evidence is invalid",
    )
    expected = _recovery_schedule_safety_evidence(
        checked,
        bound_task_ids=bound_task_ids,
        task_evidence=task_evidence,
    )
    _runner_require(
        dict(evidence) == expected,
        "infrastructure recovery schedule-root evidence is invalid",
    )


def _validate_recovery_payload(
    payload: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    failure_entry: Mapping[str, Any],
    bound_payload: Mapping[str, Any],
    failure_document: Mapping[str, Any],
    expected_retry_ordinal: int,
) -> None:
    legacy_fields = {
        "recovery_id",
        "recovery_reason",
        "retry_ordinal",
        "failure_class",
        "recovery_implementation_schema",
        "recovery_runner_module_sha256",
        "failed_journal_entry_sha256",
        "initial_failure_sha256",
        "workflow_sha256",
        "failed_workflow_id",
        "bound_task_ids",
        "failed_task_ids",
        "cancelled_task_ids",
        "dispatched_task_ids",
        "task_evidence",
        "task_evidence_sha256",
        "selected_worker_id",
        "runtime_epochs_sha256",
        "root_endpoint_identity_sha256",
        "recovery_evidence_validated",
        "credentials_recorded",
    }
    schema = payload.get("recovery_implementation_schema")
    # Historical v1alpha1 entries used an empty Root dispatch snapshot as a
    # recovery premise.  Keep them readable, but the builder below emits only
    # v1alpha2/v1alpha3 evidence whose safety comes from the frozen DAG
    # structure.  V1alpha3 distinguishes a Root result-upload read timeout
    # from the earlier identity-provider failure class.
    legacy = schema == _LEGACY_RECOVERY_IMPLEMENTATION_SCHEMA
    identity_provider = schema == _RECOVERY_IMPLEMENTATION_SCHEMA
    result_upload_timeout = (
        schema == _RESULT_UPLOAD_TIMEOUT_RECOVERY_IMPLEMENTATION_SCHEMA
    )
    current = identity_provider or result_upload_timeout
    failure_class_by_schema = {
        _LEGACY_RECOVERY_IMPLEMENTATION_SCHEMA: (
            _LEGACY_RECOVERABLE_FAILURE_CLASS
        ),
        _RECOVERY_IMPLEMENTATION_SCHEMA: _RECOVERABLE_FAILURE_CLASS,
        _RESULT_UPLOAD_TIMEOUT_RECOVERY_IMPLEMENTATION_SCHEMA: (
            _RESULT_UPLOAD_TIMEOUT_RECOVERABLE_FAILURE_CLASS
        ),
    }
    expected_failure_class = failure_class_by_schema.get(schema)
    expected_fields = set(legacy_fields)
    if current:
        expected_fields.update(
            {
                "root_dispatch_history_interpretation",
                "schedule_root_safety_evidence",
            }
        )
    if result_upload_timeout:
        expected_fields.add("primary_delivery_failure_evidence")
    evidence = payload.get("task_evidence")
    dispatched = payload.get("dispatched_task_ids")
    _runner_require(
        expected_failure_class is not None
        and set(payload) == expected_fields
        and _recovery_identifier(payload.get("recovery_id"))
        == payload.get("recovery_id")
        and _validated_recovery_reason(payload.get("recovery_reason"))
        == payload.get("recovery_reason")
        and payload.get("retry_ordinal") == expected_retry_ordinal
        and expected_retry_ordinal >= 1
        and payload.get("failure_class") == expected_failure_class
        and re.fullmatch(
            r"[0-9a-f]{64}",
            str(payload.get("recovery_runner_module_sha256") or ""),
        )
        is not None
        and payload.get("failed_journal_entry_sha256")
        == failure_entry.get("entry_sha256")
        and payload.get("initial_failure_sha256")
        == failure_document.get("failure_sha256")
        and payload.get("workflow_sha256")
        == bound_payload.get("workflow_sha256")
        and payload.get("failed_workflow_id")
        == bound_payload.get("workflow_id")
        and payload.get("bound_task_ids") == bound_payload.get("task_ids")
        and isinstance(payload.get("failed_task_ids"), list)
        and payload.get("cancelled_task_ids") == []
        and (
            dispatched == []
            if legacy
            else (
                dispatched is None
                or (
                    isinstance(dispatched, list)
                    and all(
                        isinstance(task_id, str) and bool(task_id)
                        for task_id in dispatched
                    )
                )
            )
        )
        and isinstance(evidence, list)
        and payload.get("task_evidence_sha256")
        == _sha256_bytes(_canonical_bytes(evidence))
        and payload.get("selected_worker_id")
        == contract["selected_worker"]["worker_id"]
        and payload.get("runtime_epochs_sha256")
        == contract["runtime_epochs_sha256"]
        and payload.get("root_endpoint_identity_sha256")
        == contract["flowmesh_root_endpoint_identity_sha256"]
        and payload.get("recovery_evidence_validated") is True
        and payload.get("credentials_recorded") is False,
        "matrix infrastructure recovery payload is invalid",
    )
    classified_failure = _validate_recoverable_task_evidence(
        evidence,
        bound_task_ids=payload["bound_task_ids"],
        failed_task_ids=payload["failed_task_ids"],
        selected_worker_id=str(payload["selected_worker_id"]),
        expected_failure_class=expected_failure_class,
        result_upload_timeout_observation=(
            payload.get("primary_delivery_failure_evidence")
            if result_upload_timeout
            else None
        ),
    )
    _runner_require(
        classified_failure == expected_failure_class,
        "matrix infrastructure recovery payload is invalid",
    )
    if result_upload_timeout:
        _runner_require(
            failure_entry.get("phase") == "unconditional",
            "result-upload timeout recovery supports only an "
            "unconditional phase",
        )
        primary_rows = [
            row for row in evidence if row.get("attempts", 0) > 0
        ]
        _runner_require(
            len(primary_rows) == 1,
            "matrix result-upload timeout evidence has no unique primary",
        )
        _runner_require(
            validate_result_upload_timeout_observation(
                payload.get("primary_delivery_failure_evidence"),
                expected_task_id=str(primary_rows[0]["task_id"]),
                expected_redacted_detail=str(primary_rows[0]["detail"]),
            )
            is not None,
            "matrix result-upload timeout evidence is invalid",
        )
    if current:
        trial_key = str(failure_entry.get("trial_key"))
        phase = str(failure_entry.get("phase"))
        phase_map = contract[
            "expected_phase_operation_keys_by_trial_key"
        ].get(trial_key)
        _runner_require(
            isinstance(phase_map, Mapping)
            and isinstance(phase_map.get(phase), list)
            and payload.get("root_dispatch_history_interpretation")
            == "non-historical-diagnostic-only",
            "matrix infrastructure recovery payload is invalid",
        )
        _validate_recovery_schedule_safety_evidence(
            payload.get("schedule_root_safety_evidence"),
            contract=contract,
            bound_task_ids=payload["bound_task_ids"],
            task_evidence=evidence,
            expected_phase_operation_keys=phase_map[phase],
        )


def _build_recovery_payload(
    client: FlowMeshClientProtocol,
    *,
    contract: Mapping[str, Any],
    phase_operations: Sequence[Mapping[str, Any]],
    failure_entry: Mapping[str, Any],
    bound_payload: Mapping[str, Any],
    failure_document: Mapping[str, Any],
    recovery_request: Mapping[str, str],
    retry_ordinal: int,
) -> dict[str, Any]:
    workflow_id = _text(
        bound_payload.get("workflow_id"), "failed workflow_id"
    )
    task_ids = bound_payload.get("task_ids")
    _runner_require(
        isinstance(task_ids, list)
        and bool(task_ids)
        and all(isinstance(task_id, str) and task_id for task_id in task_ids)
        and len(task_ids) == len(set(task_ids)),
        "failed workflow task binding is invalid",
    )
    try:
        terminal = client.wait(workflow_id, 0.1)
    except Exception as exc:
        raise FlowMeshContainerMatrixRunError(
            "cannot re-read the failed FlowMesh workflow for recovery: "
            + redact_secrets(str(exc))
        ) from exc
    _runner_require(
        isinstance(terminal, TerminalWorkflow)
        and terminal.workflow_id == workflow_id
        and terminal.status == "FAILED"
        and not terminal.cancelled_task_ids
        and bool(terminal.failed_task_ids)
        and set(terminal.failed_task_ids).issubset(task_ids),
        "FlowMesh failure is not an allowlisted terminal workflow",
    )
    evidence: list[dict[str, Any]] = []
    result_upload_observations: dict[str, dict[str, Any]] = {}
    recovery_describe = getattr(
        client,
        "describe_task_recovery_evidence",
        None,
    )
    for task_id in task_ids:
        try:
            if callable(recovery_describe):
                recovery_detail = recovery_describe(task_id)
                _runner_require(
                    isinstance(recovery_detail, Mapping)
                    and set(recovery_detail)
                    == {
                        "task_evidence",
                        "result_upload_read_timeout_observation",
                    }
                    and isinstance(
                        recovery_detail.get("task_evidence"), Mapping
                    ),
                    "FlowMesh returned invalid recovery-only task evidence",
                )
                detail = recovery_detail["task_evidence"]
                observation = recovery_detail[
                    "result_upload_read_timeout_observation"
                ]
                if observation is not None:
                    redacted_detail = detail.get("detail")
                    checked_observation = (
                        validate_result_upload_timeout_observation(
                            observation,
                            expected_task_id=task_id,
                            expected_redacted_detail=(
                                redacted_detail
                                if isinstance(redacted_detail, str)
                                else ""
                            ),
                        )
                    )
                    _runner_require(
                        checked_observation is not None,
                        "FlowMesh returned invalid result-upload timeout "
                        "observation",
                    )
                    result_upload_observations[task_id] = checked_observation
            else:
                detail = client.describe_task_failure(task_id)
        except Exception as exc:
            raise FlowMeshContainerMatrixRunError(
                "cannot read complete task evidence for infrastructure "
                "recovery: " + redact_secrets(str(exc))
            ) from exc
        _runner_require(
            isinstance(detail, Mapping),
            "FlowMesh returned no task evidence for infrastructure recovery",
        )
        evidence.append(_normalize_task_evidence(task_id, detail))
    attempted_task_ids = [
        str(row["task_id"])
        for row in evidence
        if row.get("attempts", 0) > 0
    ]
    primary_observation = (
        result_upload_observations.get(attempted_task_ids[0])
        if len(attempted_task_ids) == 1
        else None
    )
    failure_class = _validate_recoverable_task_evidence(
        evidence,
        bound_task_ids=task_ids,
        failed_task_ids=terminal.failed_task_ids,
        selected_worker_id=str(contract["selected_worker"]["worker_id"]),
        result_upload_timeout_observation=primary_observation,
    )
    _runner_require(
        (
            failure_class
            == _RESULT_UPLOAD_TIMEOUT_RECOVERABLE_FAILURE_CLASS
            and len(result_upload_observations) == 1
            and set(result_upload_observations) == set(attempted_task_ids)
        )
        or (
            failure_class == _RECOVERABLE_FAILURE_CLASS
            and not result_upload_observations
        ),
        "recovery-only task evidence disagrees with the failure class",
    )
    if failure_class == _RESULT_UPLOAD_TIMEOUT_RECOVERABLE_FAILURE_CLASS:
        _runner_require(
            failure_entry.get("phase") == "unconditional",
            "result-upload timeout recovery supports only an "
            "unconditional phase",
        )
    recovery_schema = (
        _RESULT_UPLOAD_TIMEOUT_RECOVERY_IMPLEMENTATION_SCHEMA
        if failure_class == _RESULT_UPLOAD_TIMEOUT_RECOVERABLE_FAILURE_CLASS
        else _RECOVERY_IMPLEMENTATION_SCHEMA
    )
    safety_evidence = _recovery_schedule_safety_evidence(
        phase_operations,
        bound_task_ids=task_ids,
        task_evidence=evidence,
    )
    dispatched = terminal.dispatched_task_ids
    payload: dict[str, Any] = {
        "recovery_id": recovery_request["recovery_id"],
        "recovery_reason": recovery_request["recovery_reason"],
        "retry_ordinal": retry_ordinal,
        "failure_class": failure_class,
        "recovery_implementation_schema": recovery_schema,
        "recovery_runner_module_sha256": (
            _recovery_runner_module_sha256()
        ),
        "failed_journal_entry_sha256": failure_entry["entry_sha256"],
        "initial_failure_sha256": failure_document["failure_sha256"],
        "workflow_sha256": bound_payload["workflow_sha256"],
        "failed_workflow_id": workflow_id,
        "bound_task_ids": list(task_ids),
        "failed_task_ids": list(terminal.failed_task_ids),
        "cancelled_task_ids": list(terminal.cancelled_task_ids),
        # Root dispatch lists are point-in-time diagnostics, not a complete
        # execution history.  Preserve the nullable snapshot without using it
        # to authorize recovery.
        "dispatched_task_ids": (
            None if dispatched is None else list(dispatched)
        ),
        "root_dispatch_history_interpretation": (
            "non-historical-diagnostic-only"
        ),
        "task_evidence": evidence,
        "task_evidence_sha256": _sha256_bytes(_canonical_bytes(evidence)),
        "schedule_root_safety_evidence": safety_evidence,
        "selected_worker_id": contract["selected_worker"]["worker_id"],
        "runtime_epochs_sha256": contract["runtime_epochs_sha256"],
        "root_endpoint_identity_sha256": contract[
            "flowmesh_root_endpoint_identity_sha256"
        ],
        "recovery_evidence_validated": True,
        "credentials_recorded": False,
    }
    if failure_class == _RESULT_UPLOAD_TIMEOUT_RECOVERABLE_FAILURE_CLASS:
        _runner_require(
            primary_observation is not None,
            "result-upload recovery has no unique primary failure",
        )
        payload["primary_delivery_failure_evidence"] = dict(
            primary_observation
        )
    _validate_recovery_payload(
        payload,
        contract=contract,
        failure_entry=failure_entry,
        bound_payload=bound_payload,
        failure_document=failure_document,
        expected_retry_ordinal=retry_ordinal,
    )
    return payload


def _validate_replay_adoption_payload(
    payload: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    failure_entry: Mapping[str, Any],
    intent_entry: Mapping[str, Any],
    bound_entry: Mapping[str, Any],
    recovery_entry: Mapping[str, Any],
    failure_document: Mapping[str, Any],
    expected_operation_keys: Sequence[str],
    expected_adoption_ordinal: int,
) -> None:
    expected_fields = {
        "adoption_id",
        "adoption_reason",
        "adoption_ordinal",
        "failure_class",
        "adoption_implementation_schema",
        "adoption_runner_module_sha256",
        "failed_journal_entry_sha256",
        "initial_failure_sha256",
        "recovery_id",
        "recovery_authorization_entry_sha256",
        "workflow_sha256",
        "submission_intent_entry_sha256",
        "workflow_bound_entry_sha256",
        "workflow_id",
        "bound_task_ids",
        "terminal_status",
        "terminal_failed_task_ids",
        "terminal_cancelled_task_ids",
        "terminal_dispatched_task_ids",
        "selected_worker_id",
        "runtime_epochs_before",
        "runtime_epochs_after",
        "runtime_epochs_sha256",
        "root_endpoint_identity_sha256",
        "operation_result_keys",
        "operation_results",
        "operation_results_sha256",
        "task_evidence",
        "task_evidence_sha256",
        "adopted_replay_operation_count",
        "adopted_replay_operation_keys",
        "adopted_replay_operation_evidence",
        "root_dispatch_history_interpretation",
        "prior_before_dispatch_interpretation_superseded",
        "prior_execution_proven_by_idempotent_replay",
        "no_workflow_submitted",
        "credentials_recorded",
        "eligible_for_scientific_claims",
    }
    result_keys = payload.get("operation_result_keys")
    replay_keys = payload.get("adopted_replay_operation_keys")
    replay_operation_evidence = payload.get(
        "adopted_replay_operation_evidence"
    )
    task_evidence = payload.get("task_evidence")
    operation_results = payload.get("operation_results")
    dispatched = payload.get("terminal_dispatched_task_ids")
    intent_payload = intent_entry.get("payload", {})
    bound_payload = bound_entry.get("payload", {})
    _runner_require(
        set(payload) == expected_fields
        and _adoption_identifier(payload.get("adoption_id"))
        == payload.get("adoption_id")
        and _validated_adoption_reason(payload.get("adoption_reason"))
        == payload.get("adoption_reason")
        and payload.get("adoption_ordinal") == expected_adoption_ordinal
        and expected_adoption_ordinal >= 1
        and payload.get("failure_class") == _REPLAY_ADOPTION_FAILURE_CLASS
        and payload.get("adoption_implementation_schema")
        == _REPLAY_ADOPTION_IMPLEMENTATION_SCHEMA
        and re.fullmatch(
            r"[0-9a-f]{64}",
            str(payload.get("adoption_runner_module_sha256") or ""),
        )
        is not None
        and payload.get("failed_journal_entry_sha256")
        == failure_entry.get("entry_sha256")
        and payload.get("initial_failure_sha256")
        == failure_document.get("failure_sha256")
        and payload.get("recovery_id")
        == recovery_entry.get("payload", {}).get("recovery_id")
        and payload.get("recovery_authorization_entry_sha256")
        == recovery_entry.get("entry_sha256")
        and payload.get("workflow_sha256")
        == bound_payload.get("workflow_sha256")
        and payload.get("submission_intent_entry_sha256")
        == intent_entry.get("entry_sha256")
        and payload.get("workflow_bound_entry_sha256")
        == bound_entry.get("entry_sha256")
        and intent_payload.get("workflow_sha256")
        == bound_payload.get("workflow_sha256")
        and payload.get("workflow_id") == bound_payload.get("workflow_id")
        and payload.get("bound_task_ids") == bound_payload.get("task_ids")
        and payload.get("terminal_status") == "DONE"
        and payload.get("terminal_failed_task_ids") == []
        and payload.get("terminal_cancelled_task_ids") == []
        and (
            dispatched is None
            or (
                isinstance(dispatched, list)
                and len(dispatched) == len(set(dispatched))
                and set(dispatched).issubset(payload["bound_task_ids"])
            )
        )
        and payload.get("selected_worker_id")
        == contract["selected_worker"]["worker_id"]
        and payload.get("runtime_epochs_before")
        == contract["runtime_epochs"]
        and payload.get("runtime_epochs_after")
        == contract["runtime_epochs"]
        and payload.get("runtime_epochs_sha256")
        == contract["runtime_epochs_sha256"]
        and payload.get("root_endpoint_identity_sha256")
        == contract["flowmesh_root_endpoint_identity_sha256"]
        and isinstance(result_keys, list)
        and result_keys == list(expected_operation_keys)
        and len(result_keys) == len(set(result_keys))
        and isinstance(operation_results, list)
        and [row.get("operation_key") for row in operation_results]
        == result_keys
        and payload.get("operation_results_sha256")
        == _sha256_bytes(_canonical_bytes(operation_results))
        and isinstance(replay_keys, list)
        and len(replay_keys) == len(set(replay_keys)) == 1
        and all(key in result_keys for key in replay_keys)
        and payload.get("adopted_replay_operation_count")
        == len(replay_keys)
        and isinstance(replay_operation_evidence, list)
        and len(replay_operation_evidence) == 1
        and _validate_replay_adoption_operation_evidence(
            replay_operation_evidence[0], contract=contract
        )["operation_key"]
        == replay_keys[0]
        and payload.get("root_dispatch_history_interpretation")
        == "non-historical-diagnostic-only"
        and payload.get("prior_before_dispatch_interpretation_superseded")
        is True
        and payload.get("prior_execution_proven_by_idempotent_replay") is True
        and re.fullmatch(
            r"[0-9a-f]{64}",
            str(payload.get("operation_results_sha256") or ""),
        )
        is not None
        and isinstance(task_evidence, list)
        and len(task_evidence) == len(payload["bound_task_ids"])
        and len({row.get("task_id") for row in task_evidence})
        == len(task_evidence)
        and {row.get("task_id") for row in task_evidence}
        == set(payload["bound_task_ids"])
        and [row.get("operation_key") for row in task_evidence]
        == result_keys
        and all(
            set(row)
            == {
                "task_id",
                "task_status",
                "assigned_worker",
                "operation_key",
                "api_http_status",
                "container_result_sha256",
                "idempotent_replay",
            }
            and row.get("task_status") == "DONE"
            and row.get("assigned_worker")
            == contract["selected_worker"]["worker_id"]
            and row.get("api_http_status") == 200
            and re.fullmatch(
                r"[0-9a-f]{64}",
                str(row.get("container_result_sha256") or ""),
            )
            is not None
            and type(row.get("idempotent_replay")) is bool
            for row in task_evidence
        )
        and [
            row["operation_key"]
            for row in task_evidence
            if row["idempotent_replay"] is True
        ]
        == replay_keys
        and payload.get("task_evidence_sha256")
        == _sha256_bytes(_canonical_bytes(task_evidence))
        and payload.get("no_workflow_submitted") is True
        and payload.get("credentials_recorded") is False
        and payload.get("eligible_for_scientific_claims") is False,
        "matrix replay-result adoption payload is invalid",
    )
    assert isinstance(operation_results, list)
    assert isinstance(replay_keys, list)
    for result in operation_results:
        _runner_require(
            isinstance(result, Mapping),
            "matrix replay-result adoption contains a non-object result",
        )
        _validate_raw_operation_result_fields(result)
        if result.get("operation_key") in replay_keys:
            _validate_replay_result_provenance(
                result,
                expected_carrier_task_id=_text(
                    result.get("task_id"), "replay result task_id"
                ),
                expected_carrier_workflow_id=_text(
                    payload.get("workflow_id"),
                    "replay result carrier workflow_id",
                ),
            )
        else:
            _validate_absent_replay_result_provenance(result)


def _build_replay_adoption_payload(
    *,
    contract: Mapping[str, Any],
    failure_entry: Mapping[str, Any],
    intent_entry: Mapping[str, Any],
    bound_entry: Mapping[str, Any],
    recovery_entry: Mapping[str, Any],
    failure_document: Mapping[str, Any],
    adoption_request: Mapping[str, str],
    terminal: TerminalWorkflow,
    records: Sequence[Mapping[str, Any]],
    replay_operation: Mapping[str, Any],
    runtime_epochs_before: Mapping[str, str],
    runtime_epochs_after: Mapping[str, str],
    adoption_ordinal: int,
) -> dict[str, Any]:
    result_keys = [str(row["operation_key"]) for row in records]
    replay_keys = [
        str(row["operation_key"])
        for row in records
        if row.get("idempotent_replay") is True
    ]
    replay_records = [
        row for row in records if row.get("idempotent_replay") is True
    ]
    _runner_require(
        len(replay_records) == 1
        and replay_records[0].get("operation_kind") == "control"
        and replay_records[0].get("logical_bytes") == 0
        and replay_records[0].get("physical_bytes") == 0,
        "only one zero-byte control replay can be adopted",
    )
    replay_evidence = _replay_adoption_operation_evidence(
        replay_operation
    )
    _runner_require(
        replay_evidence["operation_key"] == replay_keys[0],
        "replay result does not identify the frozen schedule operation",
    )
    bound_payload = bound_entry["payload"]
    task_evidence = [
        {
            "task_id": row["task_id"],
            "task_status": "DONE",
            "assigned_worker": row["worker_id"],
            "operation_key": row["operation_key"],
            "api_http_status": row["api_http_status"],
            "container_result_sha256": row["container_result_sha256"],
            "idempotent_replay": row["idempotent_replay"],
        }
        for row in records
    ]
    dispatched = terminal.dispatched_task_ids
    payload: dict[str, Any] = {
        "adoption_id": adoption_request["adoption_id"],
        "adoption_reason": adoption_request["adoption_reason"],
        "adoption_ordinal": adoption_ordinal,
        "failure_class": _REPLAY_ADOPTION_FAILURE_CLASS,
        "adoption_implementation_schema": (
            _REPLAY_ADOPTION_IMPLEMENTATION_SCHEMA
        ),
        "adoption_runner_module_sha256": _recovery_runner_module_sha256(),
        "failed_journal_entry_sha256": failure_entry["entry_sha256"],
        "initial_failure_sha256": failure_document["failure_sha256"],
        "recovery_id": recovery_entry["payload"]["recovery_id"],
        "recovery_authorization_entry_sha256": recovery_entry[
            "entry_sha256"
        ],
        "workflow_sha256": bound_payload["workflow_sha256"],
        "submission_intent_entry_sha256": intent_entry["entry_sha256"],
        "workflow_bound_entry_sha256": bound_entry["entry_sha256"],
        "workflow_id": bound_payload["workflow_id"],
        "bound_task_ids": list(bound_payload["task_ids"]),
        "terminal_status": terminal.status,
        "terminal_failed_task_ids": list(terminal.failed_task_ids),
        "terminal_cancelled_task_ids": list(terminal.cancelled_task_ids),
        # This Root can report an empty/unknown dispatch list even for a DONE
        # workflow.  Preserve it only as raw diagnostic evidence; it is not
        # used as evidence that execution did or did not occur.
        "terminal_dispatched_task_ids": (
            None if dispatched is None else list(dispatched)
        ),
        "selected_worker_id": contract["selected_worker"]["worker_id"],
        "runtime_epochs_before": dict(runtime_epochs_before),
        "runtime_epochs_after": dict(runtime_epochs_after),
        "runtime_epochs_sha256": contract["runtime_epochs_sha256"],
        "root_endpoint_identity_sha256": contract[
            "flowmesh_root_endpoint_identity_sha256"
        ],
        "operation_result_keys": result_keys,
        "operation_results": [dict(row) for row in records],
        "operation_results_sha256": _sha256_bytes(
            _canonical_bytes(list(records))
        ),
        "task_evidence": task_evidence,
        "task_evidence_sha256": _sha256_bytes(
            _canonical_bytes(task_evidence)
        ),
        "adopted_replay_operation_count": len(replay_keys),
        "adopted_replay_operation_keys": replay_keys,
        "adopted_replay_operation_evidence": [replay_evidence],
        "root_dispatch_history_interpretation": (
            "non-historical-diagnostic-only"
        ),
        "prior_before_dispatch_interpretation_superseded": True,
        "prior_execution_proven_by_idempotent_replay": True,
        "no_workflow_submitted": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    _validate_replay_adoption_payload(
        payload,
        contract=contract,
        failure_entry=failure_entry,
        intent_entry=intent_entry,
        bound_entry=bound_entry,
        recovery_entry=recovery_entry,
        failure_document=failure_document,
        expected_operation_keys=result_keys,
        expected_adoption_ordinal=adoption_ordinal,
    )
    return payload


def _journal_entry(
    journal_path: Path,
    *,
    run_id: str,
    state: str,
    sequence_index: int,
    trial_key: str,
    phase: str,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    existing = _read_jsonl(journal_path, "matrix run journal")
    entry: dict[str, Any] = {
        "schema_version": (
            FLOWMESH_CONTAINER_MATRIX_JOURNAL_ENTRY_SCHEMA_VERSION
        ),
        "journal_sequence": len(existing),
        "run_id": run_id,
        "state": state,
        "sequence_index": sequence_index,
        "trial_key": trial_key,
        "phase": phase,
        "payload": dict(payload or {}),
    }
    return _append_digest_entry(
        journal_path, entry, digest_field="entry_sha256"
    )


def _validate_journal(
    entries: Sequence[Mapping[str, Any]],
    contract: Mapping[str, Any],
    checkpoints: Sequence[Mapping[str, Any]],
    *,
    sources: Mapping[str, Any] | None = None,
    failure_document: Mapping[str, Any] | None = None,
    allow_last_checkpoint_completion_gap: bool = False,
    require_complete: bool = False,
) -> None:
    ordered = contract["ordered_trial_keys"]
    resolution_digests = contract["resolution_sha256_by_trial_key"]
    completed_count = 0
    phase_index = 0
    state_index = 0
    ready_for_checkpoint = False
    failed = False
    failure_entry: Mapping[str, Any] | None = None
    recovery_count = 0
    adoption_count = 0
    current_intent: Mapping[str, Any] | None = None
    current_bound: Mapping[str, Any] | None = None
    current_intent_entry: Mapping[str, Any] | None = None
    current_bound_entry: Mapping[str, Any] | None = None
    obtained_payloads: list[Mapping[str, Any]] = []
    seen_workflow_ids: set[str] = set()
    seen_task_ids: set[str] = set()
    seen_recovery_ids: set[str] = set()
    recovered_phases: set[tuple[int, str]] = set()
    seen_adoption_ids: set[str] = set()
    adopted_phases: set[tuple[int, str]] = set()
    current_adoption: Mapping[str, Any] | None = None
    phase_states = (
        "SUBMISSION_INTENT",
        "WORKFLOW_BOUND",
        "RESULTS_OBTAINED",
    )

    def phases_for(sequence_index: int) -> tuple[str, ...]:
        trial_key = ordered[sequence_index]
        return (
            ("A", "B")
            if resolution_digests[trial_key] is not None
            else ("unconditional",)
        )

    for index, row in enumerate(entries):
        _runner_require(
            row.get("schema_version")
            == FLOWMESH_CONTAINER_MATRIX_JOURNAL_ENTRY_SCHEMA_VERSION,
            "unsupported matrix journal entry schema",
        )
        _runner_require(
            row.get("entry_sha256")
            == _entry_sha256(row, "entry_sha256"),
            "matrix journal entry digest mismatch",
        )
        _runner_require(
            row.get("journal_sequence") == index,
            "matrix journal sequence is not contiguous",
        )
        _runner_require(
            row.get("run_id") == contract["run_id"],
            "matrix journal changed run_id",
        )
        sequence = row.get("sequence_index")
        _runner_require(
            type(sequence) is int
            and 0 <= sequence < 64
            and sequence == completed_count,
            "matrix journal trial sequence is invalid",
        )
        _runner_require(
            row.get("trial_key") == ordered[sequence],
            "matrix journal trial order changed",
        )
        state = row.get("state")
        phase = _phase_identifier(row.get("phase"))
        payload = row.get("payload")
        _runner_require(
            isinstance(payload, Mapping),
            "matrix journal payload is invalid",
        )

        if state == "RUN_FAILED":
            followed_by_authorization = (
                index + 1 < len(entries)
                and entries[index + 1].get("state")
                in {_RECOVERY_STATE, _REPLAY_ADOPTION_STATE}
            )
            _runner_require(
                (index == len(entries) - 1 or followed_by_authorization)
                and completed_count < 64,
                "matrix journal failure is neither terminal nor recovered",
            )
            _runner_require(
                phase in {"preflight", "unconditional", "A", "B"}
                and set(payload) == {"error"}
                and isinstance(payload.get("error"), str)
                and bool(payload["error"]),
                "matrix journal failure payload is invalid",
            )
            failed = True
            failure_entry = row
            continue

        if state == _REPLAY_ADOPTION_STATE:
            phases = phases_for(completed_count)
            recovery_key = (completed_count, phase)
            recovery_entries = [
                entry
                for entry in entries[:index]
                if entry.get("state") == _RECOVERY_STATE
                and entry.get("sequence_index") == completed_count
                and entry.get("phase") == phase
            ]
            _runner_require(
                failed
                and failure_entry is not None
                and failure_document is not None
                and current_intent is not None
                and current_bound is not None
                and current_intent_entry is not None
                and current_bound_entry is not None
                and phase_index < len(phases)
                and phase == phases[phase_index]
                and recovery_key in recovered_phases
                and len(recovery_entries) == 1
                and failure_entry.get("payload", {}).get("error")
                == "container operation was replayed",
                "matrix replay-result adoption does not follow the sole "
                "recoverable replay failure",
            )
            expected_keys = current_intent["operation_keys"]
            _validate_replay_adoption_payload(
                payload,
                contract=contract,
                failure_entry=failure_entry,
                intent_entry=current_intent_entry,
                bound_entry=current_bound_entry,
                recovery_entry=recovery_entries[0],
                failure_document=failure_document,
                expected_operation_keys=expected_keys,
                expected_adoption_ordinal=adoption_count + 1,
            )
            adoption_id = str(payload["adoption_id"])
            _runner_require(
                adoption_id not in seen_adoption_ids
                and recovery_key not in adopted_phases,
                "matrix journal reuses an adoption_id or adopts one phase "
                "more than once",
            )
            seen_adoption_ids.add(adoption_id)
            adopted_phases.add(recovery_key)
            adoption_count += 1
            current_adoption = payload
            failed = False
            failure_entry = None
            # Keep the latest recovered submission intent and binding.  The
            # next durable state must be RESULTS_OBTAINED for that exact DONE
            # workflow; no new submission state is permitted.
            state_index = 2
            continue

        if state == _RECOVERY_STATE:
            phases = phases_for(completed_count)
            _runner_require(
                failed
                and failure_entry is not None
                and failure_document is not None
                and current_bound is not None
                and phase_index < len(phases)
                and phase == phases[phase_index],
                "matrix recovery does not follow a recoverable bound phase",
            )
            _validate_recovery_payload(
                payload,
                contract=contract,
                failure_entry=failure_entry,
                bound_payload=current_bound,
                failure_document=failure_document,
                expected_retry_ordinal=recovery_count + 1,
            )
            recovery_id = str(payload["recovery_id"])
            recovery_key = (completed_count, phase)
            _runner_require(
                recovery_id not in seen_recovery_ids
                and recovery_key not in recovered_phases,
                "matrix journal reuses a recovery_id or recovers one phase "
                "more than once",
            )
            seen_recovery_ids.add(recovery_id)
            recovered_phases.add(recovery_key)
            recovery_count += 1
            failed = False
            failure_entry = None
            state_index = 0
            current_intent = None
            current_bound = None
            current_intent_entry = None
            current_bound_entry = None
            current_adoption = None
            continue

        _runner_require(
            not failed and completed_count < 64,
            "matrix journal continues after a terminal state",
        )
        if state == "TRIAL_COMPLETED":
            _runner_require(
                ready_for_checkpoint and phase == "trial",
                "matrix journal completes a trial before all phase results",
            )
            _runner_require(
                completed_count < len(checkpoints),
                "matrix journal completes a trial without a checkpoint",
            )
            checkpoint = checkpoints[completed_count]
            if checkpoint.get("schema_version") == (
                FLOWMESH_CONTAINER_MATRIX_REPLAY_ADOPTED_CHECKPOINT_SCHEMA_VERSION
            ):
                matching_adoptions = [
                    entry
                    for entry in entries[:index]
                    if entry.get("state") == _REPLAY_ADOPTION_STATE
                    and entry.get("sequence_index") == completed_count
                    and entry.get("payload", {}).get("adoption_id")
                    == checkpoint.get("replay_result_adoption_id")
                ]
                _runner_require(
                    len(matching_adoptions) == 1
                    and checkpoint.get(
                        "replay_result_adoption_entry_sha256"
                    )
                    == matching_adoptions[0].get("entry_sha256")
                    and checkpoint.get("adopted_replay_operation_keys")
                    == matching_adoptions[0].get("payload", {}).get(
                        "adopted_replay_operation_keys"
                    ),
                    "matrix replay-adopted checkpoint is not bound to its "
                    "journal authorization",
                )
            _runner_require(
                set(payload) == {"checkpoint_entry_sha256"}
                and payload.get("checkpoint_entry_sha256")
                == checkpoint.get("entry_sha256"),
                "matrix journal completion is not bound to its checkpoint",
            )
            _runner_require(
                [payload["submission"] for payload in obtained_payloads]
                == checkpoint.get("submissions"),
                "matrix journal results differ from checkpoint submissions",
            )
            checkpoint_executed = {
                str(result["operation_key"]): result
                for result in checkpoint.get("operation_results", [])
                if result.get("executed") is True
            }
            journal_results = [
                result
                for obtained in obtained_payloads
                for result in obtained["operation_results"]
            ]
            _runner_require(
                len(journal_results) == len(checkpoint_executed)
                and len(
                    {result.get("operation_key") for result in journal_results}
                )
                == len(journal_results)
                and {
                    result.get("operation_key") for result in journal_results
                }
                == set(checkpoint_executed),
                "matrix journal results do not cover checkpoint executions",
            )
            for journal_result in journal_results:
                checkpoint_result = checkpoint_executed[
                    str(journal_result["operation_key"])
                ]
                expected_journal_result = {
                    key: value
                    for key, value in checkpoint_result.items()
                    if key not in _MATRIX_RESULT_AUGMENTED_FIELDS
                }
                _runner_require(
                    dict(journal_result) == expected_journal_result,
                    "matrix journal operation result differs from checkpoint",
                )
            completed_count += 1
            phase_index = 0
            state_index = 0
            ready_for_checkpoint = False
            current_intent = None
            current_bound = None
            current_intent_entry = None
            current_bound_entry = None
            current_adoption = None
            obtained_payloads = []
            continue

        phases = phases_for(completed_count)
        _runner_require(
            not ready_for_checkpoint
            and phase_index < len(phases)
            and phase == phases[phase_index]
            and state == phase_states[state_index],
            "matrix journal state transition is invalid",
        )
        if state == "SUBMISSION_INTENT":
            operation_keys = payload.get("operation_keys")
            expected_operation_keys = contract[
                "expected_phase_operation_keys_by_trial_key"
            ][ordered[completed_count]][phase]
            _runner_require(
                set(payload)
                == {
                    "workflow_sha256",
                    "operation_keys",
                    "validated_before_submission",
                }
                and re.fullmatch(
                    r"[0-9a-f]{64}", str(payload.get("workflow_sha256", ""))
                )
                is not None
                and isinstance(operation_keys, list)
                and operation_keys == expected_operation_keys
                and all(isinstance(value, str) and value for value in operation_keys)
                and len(operation_keys) == len(set(operation_keys))
                and payload.get("validated_before_submission") is True,
                "matrix journal submission intent payload is invalid",
            )
            current_intent = payload
            current_intent_entry = row
        elif state == "WORKFLOW_BOUND":
            task_ids = payload.get("task_ids")
            workflow_id = payload.get("workflow_id")
            _runner_require(
                current_intent is not None
                and set(payload)
                == {"workflow_sha256", "workflow_id", "task_ids"}
                and payload.get("workflow_sha256")
                == current_intent.get("workflow_sha256")
                and isinstance(workflow_id, str)
                and bool(workflow_id)
                and isinstance(task_ids, list)
                and len(task_ids) == len(current_intent["operation_keys"])
                and all(isinstance(value, str) and value for value in task_ids)
                and len(task_ids) == len(set(task_ids)),
                "matrix journal workflow binding payload is invalid",
            )
            _runner_require(
                workflow_id not in seen_workflow_ids
                and seen_task_ids.isdisjoint(task_ids),
                "matrix journal reuses a FlowMesh workflow or task ID",
            )
            seen_workflow_ids.add(workflow_id)
            seen_task_ids.update(task_ids)
            current_bound = payload
            current_bound_entry = row
        else:
            submission = payload.get("submission")
            operation_results = payload.get("operation_results")
            result_task_ids = [
                result.get("task_id") for result in operation_results
            ] if isinstance(operation_results, list) else []
            _runner_require(
                current_intent is not None
                and current_bound is not None
                and set(payload)
                == {"workflow_sha256", "submission", "operation_results"}
                and payload.get("workflow_sha256")
                == current_intent.get("workflow_sha256")
                and isinstance(submission, Mapping)
                and isinstance(operation_results, list)
                and [result.get("operation_key") for result in operation_results]
                == current_intent["operation_keys"]
                and submission.get("workflow_id")
                == current_bound.get("workflow_id")
                and submission.get("task_ids") == current_bound.get("task_ids")
                and submission.get("workflow_sha256")
                == current_intent.get("workflow_sha256")
                and len(result_task_ids) == len(set(result_task_ids))
                and set(result_task_ids) == set(current_bound["task_ids"]),
                "matrix journal obtained-results payload is invalid",
            )
            for result in operation_results:
                _runner_require(
                    isinstance(result, Mapping),
                    "matrix journal contains a non-object operation result",
                )
                _validate_raw_operation_result_fields(result)
            if current_adoption is not None:
                replay_keys = [
                    result["operation_key"]
                    for result in operation_results
                    if result.get("idempotent_replay") is True
                ]
                task_evidence = [
                    {
                        "task_id": result["task_id"],
                        "task_status": "DONE",
                        "assigned_worker": result["worker_id"],
                        "operation_key": result["operation_key"],
                        "api_http_status": result["api_http_status"],
                        "container_result_sha256": result[
                            "container_result_sha256"
                        ],
                        "idempotent_replay": result[
                            "idempotent_replay"
                        ],
                    }
                    for result in operation_results
                ]
                _runner_require(
                    _sha256_bytes(_canonical_bytes(operation_results))
                    == current_adoption["operation_results_sha256"]
                    and replay_keys
                    == current_adoption["adopted_replay_operation_keys"]
                    and task_evidence == current_adoption["task_evidence"]
                    and _sha256_bytes(_canonical_bytes(task_evidence))
                    == current_adoption["task_evidence_sha256"],
                    "adopted replay results differ from their durable "
                    "authorization",
                )
                for result in operation_results:
                    if result.get("operation_key") in replay_keys:
                        _validate_replay_result_provenance(
                            result,
                            expected_carrier_task_id=_text(
                                result.get("task_id"),
                                "replay journal task_id",
                            ),
                            expected_carrier_workflow_id=_text(
                                current_adoption.get("workflow_id"),
                                "replay journal carrier workflow_id",
                            ),
                        )
                    else:
                        _validate_absent_replay_result_provenance(result)
            if sources is not None:
                wrappers = sources["wrappers"]
                source_operations = sources["operations"]
                assert isinstance(wrappers, Sequence)
                assert isinstance(source_operations, Sequence)
                wrapper = wrappers[completed_count]
                by_trial = _operations_by_trial(source_operations)
                trial_operations = by_trial[ordered[completed_count]]
                conditional = (
                    str(wrapper["design_id"]) in _CONDITIONAL_DESIGNS
                )
                phase_inputs, _inactive, _outcomes = _phase_inputs_for_trial(
                    trial_operations,
                    trial_key=ordered[completed_count],
                    conditional=conditional,
                )
                phase_spec = next(
                    item for item in phase_inputs if item[0] == phase
                )
                _, phase_operations, dependencies = phase_spec
                worker_id = str(contract["selected_worker"]["worker_id"])
                workflow = build_flowmesh_container_matrix_trial_workflow(
                    phase_operations,
                    dependencies,
                    contract["node_api_urls"],
                    worker_id,
                    str(contract["run_id"]),
                    ordered[completed_count],
                    phase,
                    str(contract["owner"]),
                    int(contract["api_task_timeout_seconds"]),
                )
                _runner_require(
                    payload.get("workflow_sha256")
                    == _sha256_bytes(_canonical_bytes(workflow)),
                    "matrix journal workflow differs from frozen sources",
                )
                expected_submission = _submission_record(
                    SubmittedWorkflow(
                        str(submission["workflow_id"]),
                        tuple(submission["task_ids"]),
                    ),
                    workflow,
                    sequence_index=completed_count,
                    trial_key=ordered[completed_count],
                    phase=phase,
                    selected_worker_id=worker_id,
                )
                _runner_require(
                    dict(submission) == expected_submission,
                    "matrix journal submission differs from frozen sources",
                )
                for raw_result, operation in zip(
                    operation_results, phase_operations
                ):
                    replay_keys = set(
                        current_adoption.get(
                            "adopted_replay_operation_keys", []
                        )
                        if current_adoption is not None
                        else []
                    )
                    if str(operation["operation_key"]) in replay_keys:
                        _runner_require(
                            operation.get("operation_kind") == "control"
                            and operation.get("logical_bytes") == 0
                            and operation.get("dependency_operation_keys") == []
                            and operation.get("operation_adapter")
                            == "monotonic-control-v1",
                            "only the dependency-free zero-byte schedule "
                            "control operation may be replay-adopted",
                        )
                    wrapped = _executed_operation_result(
                        operation,
                        raw_result,
                        sequence_index=completed_count,
                    )
                    _revalidate_executed_operation_result(
                        wrapped,
                        operation,
                        contract=contract,
                        sequence_index=completed_count,
                        expected_phase=phase,
                        allow_idempotent_replay=(
                            str(operation["operation_key"]) in replay_keys
                        ),
                        expected_carrier_workflow_id=(
                            str(submission["workflow_id"])
                            if str(operation["operation_key"])
                            in replay_keys
                            else None
                        ),
                    )
            obtained_payloads.append(payload)
            phase_index += 1
            state_index = 0
            current_intent = None
            current_bound = None
            current_intent_entry = None
            current_bound_entry = None
            current_adoption = None
            ready_for_checkpoint = phase_index == len(phases)
            continue
        state_index += 1

    _runner_require(
        completed_count == len(checkpoints)
        or (
            allow_last_checkpoint_completion_gap
            and completed_count + 1 == len(checkpoints)
            and ready_for_checkpoint
            and not failed
        ),
        "matrix journal completed-trial prefix differs from checkpoints",
    )
    if require_complete:
        _runner_require(
            completed_count == len(checkpoints) == 64
            and not ready_for_checkpoint
            and not failed,
            "completed matrix journal is not a complete 64-trial state machine",
        )


def _reconcile_checkpoint_completion(
    journal_path: Path,
    entries: list[dict[str, Any]],
    checkpoints: Sequence[Mapping[str, Any]],
    contract: Mapping[str, Any],
    *,
    sources: Mapping[str, Any] | None = None,
    failure_document: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Recover the sole safe two-file crash window after checkpoint append."""

    _validate_journal(
        entries,
        contract,
        checkpoints,
        sources=sources,
        failure_document=failure_document,
        allow_last_checkpoint_completion_gap=True,
    )
    completed_count = sum(
        row.get("state") == "TRIAL_COMPLETED" for row in entries
    )
    if completed_count + 1 == len(checkpoints):
        checkpoint = checkpoints[-1]
        completed = _journal_entry(
            journal_path,
            run_id=str(contract["run_id"]),
            state="TRIAL_COMPLETED",
            sequence_index=int(checkpoint["sequence_index"]),
            trial_key=str(checkpoint["trial_key"]),
            phase="trial",
            payload={
                "checkpoint_entry_sha256": checkpoint["entry_sha256"]
            },
        )
        entries.append(completed)
    _validate_journal(
        entries,
        contract,
        checkpoints,
        sources=sources,
        failure_document=failure_document,
    )
    return entries


def _phase_progress(
    entries: Sequence[Mapping[str, Any]],
    *,
    sequence_index: int,
    phase: str,
) -> Mapping[str, Any] | None:
    progress: Mapping[str, Any] | None = None
    for row in entries:
        if (
            row.get("sequence_index") != sequence_index
            or row.get("phase") != phase
        ):
            continue
        if row.get("state") == _RECOVERY_STATE:
            progress = None
        elif row.get("state") in {
            "SUBMISSION_INTENT",
            "WORKFLOW_BOUND",
            "RESULTS_OBTAINED",
        }:
            progress = row
    return progress


def _unresolved_failure(
    entries: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    if entries and entries[-1].get("state") == "RUN_FAILED":
        return entries[-1]
    return None


def _authorize_infrastructure_recovery(
    client: FlowMeshClientProtocol,
    *,
    contract: Mapping[str, Any],
    operations: Sequence[Mapping[str, Any]],
    journal_path: Path,
    journal_entries: list[dict[str, Any]],
    failure_document: Mapping[str, Any],
    recovery_request: Mapping[str, str],
) -> dict[str, Any]:
    failure_entry = _unresolved_failure(journal_entries)
    _runner_require(
        failure_entry is not None,
        "matrix run has no unresolved infrastructure failure to recover",
    )
    _runner_require(
        recovery_request["recover_failed_entry_sha256"]
        == failure_entry.get("entry_sha256"),
        "recovery authorization does not bind the terminal RUN_FAILED entry",
    )
    sequence_index = int(failure_entry["sequence_index"])
    phase = str(failure_entry["phase"])
    _runner_require(
        phase in {"unconditional", "A", "B"},
        "only a submitted matrix phase can be recovered",
    )
    progress = _phase_progress(
        journal_entries[:-1],
        sequence_index=sequence_index,
        phase=phase,
    )
    _runner_require(
        progress is not None and progress.get("state") == "WORKFLOW_BOUND",
        "the failed matrix phase was not durably bound before failure",
    )
    _runner_require(
        not any(
            row.get("state") == _RECOVERY_STATE
            and row.get("sequence_index") == sequence_index
            and row.get("phase") == phase
            for row in journal_entries
        ),
        "a matrix phase may receive at most one infrastructure recovery",
    )
    _runner_require(
        not any(
            row.get("state") == _RECOVERY_STATE
            and row.get("payload", {}).get("recovery_id")
            == recovery_request["recovery_id"]
            for row in journal_entries
        ),
        "recovery_id was already used by this matrix run",
    )
    retry_ordinal = 1 + sum(
        row.get("state") == _RECOVERY_STATE for row in journal_entries
    )
    trial_key = str(failure_entry["trial_key"])
    phase_operation_keys = contract[
        "expected_phase_operation_keys_by_trial_key"
    ][trial_key][phase]
    operation_by_key = {
        str(operation["operation_key"]): operation
        for operation in operations
    }
    _runner_require(
        all(key in operation_by_key for key in phase_operation_keys),
        "recovery phase is not covered by the frozen operation ledger",
    )
    phase_operations = [
        operation_by_key[key] for key in phase_operation_keys
    ]
    payload = _build_recovery_payload(
        client,
        contract=contract,
        phase_operations=phase_operations,
        failure_entry=failure_entry,
        bound_payload=progress["payload"],
        failure_document=failure_document,
        recovery_request=recovery_request,
        retry_ordinal=retry_ordinal,
    )
    authorized = _journal_entry(
        journal_path,
        run_id=str(contract["run_id"]),
        state=_RECOVERY_STATE,
        sequence_index=sequence_index,
        trial_key=str(failure_entry["trial_key"]),
        phase=phase,
        payload=payload,
    )
    journal_entries.append(authorized)
    return authorized


def _validate_repeated_recovery_request(
    journal_entries: Sequence[Mapping[str, Any]],
    recovery_request: Mapping[str, str],
) -> None:
    matches = [
        row
        for row in journal_entries
        if row.get("state") == _RECOVERY_STATE
        and row.get("payload", {}).get("recovery_id")
        == recovery_request["recovery_id"]
    ]
    _runner_require(
        len(matches) == 1
        and matches[0].get("payload", {}).get("recovery_reason")
        == recovery_request["recovery_reason"]
        and matches[0].get("payload", {}).get(
            "failed_journal_entry_sha256"
        )
        == recovery_request["recover_failed_entry_sha256"],
        "recovery request does not match a durable authorization",
    )


def _assert_current_worker(
    client: FlowMeshClientProtocol,
    *,
    expected_worker: Mapping[str, Any],
) -> FlowMeshWorkerIdentity:
    expected_worker_id = _text(
        expected_worker.get("worker_id"), "selected worker_id"
    )
    try:
        identity = client.describe_current_worker(worker_id=expected_worker_id)
    except Exception as exc:
        raise FlowMeshContainerMatrixRunError(
            "the pinned FlowMesh worker changed: "
            + redact_secrets(str(exc))
        ) from exc
    _runner_require(
        identity.worker_id == expected_worker_id,
        "the pinned FlowMesh worker ID changed",
    )
    _runner_require(
        _stable_worker_identity(identity) == dict(expected_worker),
        "the pinned FlowMesh worker stable identity changed",
    )
    return identity


def _probe_all_epochs(
    probe: Callable[
        [Mapping[str, str], Sequence[Mapping[str, Any]]], Mapping[str, str]
    ],
    *,
    matrix: Mapping[str, Any],
    operations: Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    node_ids = matrix.get("topology_node_ids")
    _runner_require(
        isinstance(node_ids, list)
        and len(node_ids) == 8
        and all(isinstance(node_id, str) and node_id for node_id in node_ids),
        "matrix topology does not name exactly eight runtime nodes",
    )
    # The formal runtime contract binds the whole topology, including a node
    # which may not appear in a particular operation path.  Both the real
    # probe and test probes derive their requested set from operation-shaped
    # rows, so use one harmless identity row per topology node.
    probe_rows = [
        {
            "execution_node_id": node_id,
            "destination_node_id": node_id,
            "operation_kind": "control",
        }
        for node_id in node_ids
    ]
    epochs = _validate_runtime_epochs(
        probe(matrix["node_api_urls"], probe_rows), probe_rows
    )
    _runner_require(
        len(epochs) == 8,
        "runtime epoch probe did not cover all eight simulator nodes",
    )
    _runner_require(
        len(set(epochs.values())) == 8,
        "runtime epoch probe did not identify eight distinct simulator "
        "runtimes",
    )
    return dict(sorted(epochs.items()))


def _assert_runtime_epochs(
    observed: Mapping[str, str],
    contract: Mapping[str, Any],
) -> None:
    _runner_require(
        dict(sorted(observed.items())) == contract["runtime_epochs"],
        "container runtime epoch changed; start a new output directory",
    )


def _collect_results(
    client: FlowMeshClientProtocol,
    submitted: SubmittedWorkflow,
    *,
    operations: Sequence[Mapping[str, Any]],
    phase: str,
    selected_worker_id: str,
    expected_runtime_epochs: Mapping[str, str],
    allow_idempotent_replay: bool = False,
) -> list[dict[str, Any]]:
    _runner_require(
        len(submitted.task_ids) == len(operations),
        "FlowMesh returned a task count other than the frozen matrix phase",
    )
    expected = {str(row["operation_key"]): row for row in operations}
    records: list[dict[str, Any]] = []
    for task_id in submitted.task_ids:
        raw = client.retrieve_result(task_id)
        api = extract_api_executor_result(raw)
        try:
            body = json.loads(api["text"])
        except json.JSONDecodeError as exc:
            raise FlowMeshContainerMatrixRunError(
                f"FlowMesh task {task_id} returned non-JSON output"
            ) from exc
        _runner_require(
            isinstance(body, Mapping),
            "FlowMesh matrix task output must be an object",
        )
        operation_key = body.get("operation_key")
        _runner_require(
            isinstance(operation_key, str) and operation_key in expected,
            "FlowMesh returned an operation outside the frozen matrix phase",
        )
        try:
            detail = client.describe_task_failure(task_id)
        except Exception as exc:
            raise FlowMeshContainerMatrixRunError(
                "cannot verify the FlowMesh worker assigned to a completed "
                "matrix task: " + redact_secrets(str(exc))
            ) from exc
        _runner_require(
            isinstance(detail, Mapping),
            "FlowMesh did not return task metadata needed to verify the "
            "worker pin",
        )
        if allow_idempotent_replay:
            _runner_require(
                str(detail.get("task_status") or "").upper() == "DONE"
                and detail.get("assigned_worker") == selected_worker_id,
                "replay adoption task is not DONE on the frozen worker",
            )
            _runner_require(
                api.get("status_code") == 200,
                "replay adoption requires exact API HTTP 200 results",
            )
        record = _operation_result(
            raw,
            expected[operation_key],
            task_id=task_id,
            selected_worker_id=selected_worker_id,
            task_detail=detail,
            expected_runtime_epochs=expected_runtime_epochs,
            allow_idempotent_replay=allow_idempotent_replay,
        )
        _runner_require(
            type(body.get("started_monotonic_ns")) is int
            and type(body.get("finished_monotonic_ns")) is int,
            "formal matrix result must preserve its monotonic interval",
        )
        record["started_monotonic_ns"] = body["started_monotonic_ns"]
        record["finished_monotonic_ns"] = body["finished_monotonic_ns"]
        record["phase"] = phase
        if record.get("idempotent_replay") is True:
            record.update(
                {
                    "result_carrier_task_id": task_id,
                    "result_carrier_workflow_id": submitted.workflow_id,
                    "measurement_origin": (
                        "container-node-idempotency-ledger"
                    ),
                    "original_flowmesh_task_id_known": False,
                    "original_flowmesh_workflow_id_known": False,
                    "measurement_freshness_established": False,
                }
            )
        records.append(record)
    _runner_require(
        len(records) == len(expected)
        and {row["operation_key"] for row in records} == set(expected),
        "FlowMesh results do not cover exactly the frozen matrix phase",
    )
    by_key = {str(row["operation_key"]): row for row in records}
    return [by_key[str(row["operation_key"])] for row in operations]


def _submission_record(
    submitted: SubmittedWorkflow,
    workflow: Mapping[str, Any],
    *,
    sequence_index: int,
    trial_key: str,
    phase: str,
    selected_worker_id: str,
) -> dict[str, Any]:
    return {
        "schema_version": FLOWMESH_CONTAINER_MATRIX_SUBMISSION_SCHEMA_VERSION,
        "sequence_index": sequence_index,
        "trial_key": trial_key,
        "phase": phase,
        "workflow_id": submitted.workflow_id,
        "task_ids": list(submitted.task_ids),
        "selected_worker_id": selected_worker_id,
        "workflow_sha256": _sha256_bytes(_canonical_bytes(workflow)),
        "validated_before_submission": True,
        "credentials_recorded": False,
    }


def _execute_phase(
    *,
    client: FlowMeshClientProtocol,
    settings: FlowMeshSettings,
    contract: Mapping[str, Any],
    journal_path: Path,
    journal_entries: list[dict[str, Any]],
    sequence_index: int,
    trial_key: str,
    phase: str,
    operations: Sequence[Mapping[str, Any]],
    dependencies: Mapping[str, Sequence[str]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    worker = contract["selected_worker"]
    assert isinstance(worker, Mapping)
    worker_id = str(worker["worker_id"])
    workflow = build_flowmesh_container_matrix_trial_workflow(
        operations,
        dependencies,
        contract["node_api_urls"],
        worker_id,
        str(contract["run_id"]),
        trial_key,
        phase,
        str(contract["owner"]),
        int(contract["api_task_timeout_seconds"]),
    )
    workflow_sha256 = _sha256_bytes(_canonical_bytes(workflow))
    progress = _phase_progress(
        journal_entries,
        sequence_index=sequence_index,
        phase=phase,
    )
    submitted: SubmittedWorkflow
    if progress is not None and progress["state"] == "RESULTS_OBTAINED":
        payload = progress["payload"]
        _runner_require(
            payload.get("workflow_sha256") == workflow_sha256,
            "resumed matrix phase workflow binding changed",
        )
        submission = payload.get("submission")
        records = payload.get("operation_results")
        _runner_require(
            isinstance(submission, Mapping) and isinstance(records, list),
            "resumed matrix phase result checkpoint is incomplete",
        )
        return dict(submission), [
            _copy(row, "persisted operation result") for row in records
        ]
    if progress is not None and progress["state"] == "SUBMISSION_INTENT":
        raise FlowMeshContainerMatrixRunError(
            "ambiguous FlowMesh submission: intent was durable but no "
            "workflow ID was recorded; refusing to resubmit"
        )
    if progress is None:
        validation = client.validate(workflow)
        _runner_require(
            validation.ok,
            "FlowMesh rejected matrix workflow: "
            + "; ".join(validation.errors),
        )
        intent = _journal_entry(
            journal_path,
            run_id=str(contract["run_id"]),
            state="SUBMISSION_INTENT",
            sequence_index=sequence_index,
            trial_key=trial_key,
            phase=phase,
            payload={
                "workflow_sha256": workflow_sha256,
                "operation_keys": [
                    str(row["operation_key"]) for row in operations
                ],
                "validated_before_submission": True,
            },
        )
        journal_entries.append(intent)
        submitted = client.submit(workflow)
        existing_workflow_ids = {
            str(row["payload"]["workflow_id"])
            for row in journal_entries
            if row.get("state") == "WORKFLOW_BOUND"
        }
        existing_task_ids = {
            str(task_id)
            for row in journal_entries
            if row.get("state") == "WORKFLOW_BOUND"
            for task_id in row["payload"]["task_ids"]
        }
        _runner_require(
            isinstance(submitted.workflow_id, str)
            and bool(submitted.workflow_id)
            and len(submitted.task_ids) == len(operations)
            and all(
                isinstance(task_id, str) and bool(task_id)
                for task_id in submitted.task_ids
            )
            and len(submitted.task_ids) == len(set(submitted.task_ids))
            and submitted.workflow_id not in existing_workflow_ids
            and existing_task_ids.isdisjoint(submitted.task_ids),
            "FlowMesh reused a workflow or task ID; refusing to bind an "
            "ambiguous submission",
        )
        bound = _journal_entry(
            journal_path,
            run_id=str(contract["run_id"]),
            state="WORKFLOW_BOUND",
            sequence_index=sequence_index,
            trial_key=trial_key,
            phase=phase,
            payload={
                "workflow_sha256": workflow_sha256,
                "workflow_id": submitted.workflow_id,
                "task_ids": list(submitted.task_ids),
            },
        )
        journal_entries.append(bound)
    else:
        _runner_require(
            progress["state"] == "WORKFLOW_BOUND",
            "matrix phase journal has an unsupported resume state",
        )
        payload = progress["payload"]
        _runner_require(
            payload.get("workflow_sha256") == workflow_sha256,
            "resumed matrix phase workflow binding changed",
        )
        workflow_id = payload.get("workflow_id")
        task_ids = payload.get("task_ids")
        _runner_require(
            isinstance(workflow_id, str)
            and workflow_id
            and isinstance(task_ids, list)
            and all(isinstance(value, str) and value for value in task_ids),
            "resumed matrix phase workflow IDs are invalid",
        )
        submitted = SubmittedWorkflow(workflow_id, tuple(task_ids))

    _runner_require(
        len(submitted.task_ids) == len(operations),
        "FlowMesh returned a task count other than the frozen matrix phase",
    )
    terminal = client.wait(
        submitted.workflow_id, settings.poll_interval_seconds
    )
    if terminal.status != "DONE":
        raise _workflow_failure(terminal, submitted, client)
    records = _collect_results(
        client,
        submitted,
        operations=operations,
        phase=phase,
        selected_worker_id=worker_id,
        expected_runtime_epochs=contract["runtime_epochs"],
    )
    submission = _submission_record(
        submitted,
        workflow,
        sequence_index=sequence_index,
        trial_key=trial_key,
        phase=phase,
        selected_worker_id=worker_id,
    )
    obtained = _journal_entry(
        journal_path,
        run_id=str(contract["run_id"]),
        state="RESULTS_OBTAINED",
        sequence_index=sequence_index,
        trial_key=trial_key,
        phase=phase,
        payload={
            "workflow_sha256": workflow_sha256,
            "submission": submission,
            "operation_results": records,
        },
    )
    journal_entries.append(obtained)
    return submission, records


def _inactive_operation_result(
    operation: Mapping[str, Any],
    *,
    sequence_index: int,
) -> dict[str, Any]:
    return {
        "schema_version": (
            FLOWMESH_CONTAINER_MATRIX_OPERATION_RESULT_SCHEMA_VERSION
        ),
        "sequence_index": sequence_index,
        "trial_key": operation["trial_key"],
        "operation_key": operation["operation_key"],
        "operation_id": operation["operation_id"],
        "operation_kind": operation["operation_kind"],
        "condition": operation.get("condition"),
        "frozen_operation_sha256": _sha256_bytes(
            _canonical_bytes(operation)
        ),
        "planned_logical_bytes": operation["logical_bytes"],
        "executed": False,
        "skip_reason": "inactive-conditional-branch",
        "phase": None,
        "task_id": None,
        "worker_id": None,
        "execution_node_id": None,
        "destination_node_id": None,
        "runtime_epoch": None,
        "destination_runtime_epoch": None,
        # These are observations, not planned values.  A branch which was
        # never submitted has no measured bytes or service time; recording
        # zero would manufacture a measurement.  The planned byte count is
        # retained separately above.
        "logical_bytes": None,
        "physical_bytes": None,
        "service_time_ms": None,
        "telemetry_recorded": False,
        "telemetry_complete": False,
        "semantic_task_quality_evaluated": False,
        "idempotent_replay": False,
        "credentials_recorded": False,
    }


def _executed_operation_result(
    operation: Mapping[str, Any],
    record: Mapping[str, Any],
    *,
    sequence_index: int,
) -> dict[str, Any]:
    copied = _copy(record, "matrix operation result")
    replayed = copied.get("idempotent_replay") is True
    copied.update(
        {
            "schema_version": (
                FLOWMESH_CONTAINER_MATRIX_REPLAY_ADOPTED_OPERATION_RESULT_SCHEMA_VERSION
                if replayed
                else FLOWMESH_CONTAINER_MATRIX_OPERATION_RESULT_SCHEMA_VERSION
            ),
            "sequence_index": sequence_index,
            "trial_key": operation["trial_key"],
            "operation_id": operation["operation_id"],
            "condition": operation.get("condition"),
            "frozen_operation_sha256": _sha256_bytes(
                _canonical_bytes(operation)
            ),
            "planned_logical_bytes": operation["logical_bytes"],
            "executed": True,
            "skip_reason": None,
            "telemetry_recorded": True,
            "credentials_recorded": False,
        }
    )
    return copied


def _validate_replay_result_provenance(
    result: Mapping[str, Any],
    *,
    expected_carrier_task_id: str,
    expected_carrier_workflow_id: str,
) -> None:
    """Validate the narrow provenance contract for one replayed result."""

    _runner_require(
        result.get("idempotent_replay") is True
        and result.get("result_carrier_task_id")
        == expected_carrier_task_id
        and result.get("result_carrier_workflow_id")
        == expected_carrier_workflow_id
        and result.get("measurement_origin")
        == "container-node-idempotency-ledger"
        and result.get("original_flowmesh_task_id_known") is False
        and result.get("original_flowmesh_workflow_id_known") is False
        and result.get("measurement_freshness_established") is False
        and _REPLAY_RESULT_PROVENANCE_FIELDS.issubset(result),
        "replay-adopted result provenance is invalid",
    )


def _validate_absent_replay_result_provenance(
    result: Mapping[str, Any],
) -> None:
    _runner_require(
        _REPLAY_RESULT_PROVENANCE_FIELDS.isdisjoint(result),
        "non-replayed result contains replay-only provenance",
    )


def _expected_raw_operation_result_fields(
    result: Mapping[str, Any],
) -> frozenset[str]:
    fields = _RAW_MATRIX_OPERATION_RESULT_FIELDS
    if result.get("operation_kind") in _CACHE_RESULT_OPERATION_KINDS:
        fields = fields | _CACHE_RESULT_FIELDS
    if result.get("idempotent_replay") is True:
        fields = fields | _REPLAY_RESULT_PROVENANCE_FIELDS
    return fields


def _validate_raw_operation_result_fields(
    result: Mapping[str, Any],
) -> None:
    _runner_require(
        set(result) == _expected_raw_operation_result_fields(result),
        "matrix operation result fields changed",
    )


def _build_trial_result(
    *,
    wrapper: Mapping[str, Any],
    operation_results: Sequence[Mapping[str, Any]],
    submissions: Sequence[Mapping[str, Any]],
    expected_cache_outcomes: Mapping[str, str] | None,
    observed_cache_outcomes: Mapping[str, str] | None,
) -> dict[str, Any]:
    executed = [row for row in operation_results if row.get("executed") is True]
    active_count = len(executed)
    inactive_count = len(operation_results) - active_count
    return {
        "schema_version": FLOWMESH_CONTAINER_MATRIX_TRIAL_RESULT_SCHEMA_VERSION,
        "status": "COMPLETE",
        "sequence_index": wrapper["sequence_index"],
        "trial_key": wrapper["trial_key"],
        "trial_id": wrapper["trial_id"],
        "order_index": wrapper["order_index"],
        "workload_id": wrapper["workload_id"],
        "workload_class": wrapper["workload_class"],
        "design_id": wrapper["design_id"],
        "repetition": wrapper["repetition"],
        "executor_node_id": wrapper["executor_node_id"],
        "conditional": str(wrapper["design_id"]) in _CONDITIONAL_DESIGNS,
        "expected_cache_outcomes": (
            None
            if expected_cache_outcomes is None
            else dict(sorted(expected_cache_outcomes.items()))
        ),
        "observed_cache_outcomes": (
            None
            if observed_cache_outcomes is None
            else dict(sorted(observed_cache_outcomes.items()))
        ),
        "cache_outcomes_match_frozen_plan": (
            None
            if expected_cache_outcomes is None
            else observed_cache_outcomes == expected_cache_outcomes
        ),
        "workflow_count": len(submissions),
        "planned_operation_count": len(operation_results),
        "executed_operation_count": active_count,
        "inactive_operation_count": inactive_count,
        "telemetry": _aggregate_telemetry(executed),
        "service_time_sum_is_end_to_end_latency": False,
        "queue_time_measured": False,
        "semantic_task_quality_evaluated": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def _trial_checkpoint(
    *,
    contract: Mapping[str, Any],
    wrapper: Mapping[str, Any],
    operations: Sequence[Mapping[str, Any]],
    active_records: Sequence[Mapping[str, Any]],
    submissions: Sequence[Mapping[str, Any]],
    expected_cache_outcomes: Mapping[str, str] | None,
    observed_cache_outcomes: Mapping[str, str] | None,
    runtime_epochs_before: Mapping[str, str],
    runtime_epochs_after: Mapping[str, str],
    replay_adoption: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    sequence_index = int(wrapper["sequence_index"])
    result_by_key = {
        str(row["operation_key"]): row for row in active_records
    }
    operation_results: list[dict[str, Any]] = []
    for operation in operations:
        key = str(operation["operation_key"])
        if key in result_by_key:
            operation_results.append(
                _executed_operation_result(
                    operation,
                    result_by_key[key],
                    sequence_index=sequence_index,
                )
            )
        else:
            operation_results.append(
                _inactive_operation_result(
                    operation, sequence_index=sequence_index
                )
            )
    trial_result = _build_trial_result(
        wrapper=wrapper,
        operation_results=operation_results,
        submissions=submissions,
        expected_cache_outcomes=expected_cache_outcomes,
        observed_cache_outcomes=observed_cache_outcomes,
    )
    adopted_keys: list[str] = []
    if replay_adoption is not None:
        adopted = replay_adoption.get("adopted_replay_operation_keys")
        _runner_require(
            isinstance(adopted, list)
            and bool(adopted)
            and all(isinstance(key, str) and key for key in adopted)
            and len(adopted) == len(set(adopted)),
            "replay adoption checkpoint keys are invalid",
        )
        adopted_keys = list(adopted)
        actual_replays = [
            str(row["operation_key"])
            for row in operation_results
            if row.get("executed") is True
            and row.get("idempotent_replay") is True
        ]
        _runner_require(
            actual_replays == adopted_keys,
            "replay adoption checkpoint differs from adopted results",
        )
        trial_result.update(
            {
                "schema_version": (
                    FLOWMESH_CONTAINER_MATRIX_REPLAY_ADOPTED_TRIAL_RESULT_SCHEMA_VERSION
                ),
                "replay_result_adoption_id": replay_adoption["adoption_id"],
                "adopted_replay_operation_count": len(adopted_keys),
                "adopted_replay_operation_keys": adopted_keys,
                "replayed_operation_service_time_in_non_replayed_result_telemetry": False,
                "non_replayed_result_telemetry": _aggregate_telemetry(
                    [
                        row
                        for row in operation_results
                        if row.get("executed") is True
                        and row.get("idempotent_replay") is False
                    ]
                ),
            }
        )
    checkpoint: dict[str, Any] = {
        "schema_version": (
            FLOWMESH_CONTAINER_MATRIX_REPLAY_ADOPTED_CHECKPOINT_SCHEMA_VERSION
            if replay_adoption is not None
            else FLOWMESH_CONTAINER_MATRIX_CHECKPOINT_ENTRY_SCHEMA_VERSION
        ),
        "checkpoint_sequence": sequence_index,
        "sequence_index": sequence_index,
        "trial_key": wrapper["trial_key"],
        "wrapper_sha256": contract[
            "trial_wrapper_sha256_by_trial_key"
        ][wrapper["trial_key"]],
        "runtime_epochs_before": dict(sorted(runtime_epochs_before.items())),
        "runtime_epochs_after": dict(sorted(runtime_epochs_after.items())),
        "trial_result": trial_result,
        "operation_results": operation_results,
        "submissions": [dict(row) for row in submissions],
    }
    if replay_adoption is not None:
        checkpoint.update(
            {
                "replay_result_adoption_id": replay_adoption["adoption_id"],
                "replay_result_adoption_entry_sha256": replay_adoption[
                    "adoption_entry_sha256"
                ],
                "adopted_replay_operation_count": len(adopted_keys),
                "adopted_replay_operation_keys": adopted_keys,
                "replayed_operation_service_time_in_non_replayed_result_telemetry": False,
            }
        )
    return checkpoint


def _revalidate_executed_operation_result(
    result: Mapping[str, Any],
    operation: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    sequence_index: int,
    expected_phase: str,
    allow_idempotent_replay: bool = False,
    expected_carrier_workflow_id: str | None = None,
) -> dict[str, Any]:
    """Re-run the shared v2 result validator over preserved evidence."""

    checked_operation = _validate_operation(operation)
    for field in ("started_monotonic_ns", "finished_monotonic_ns"):
        _runner_require(
            type(result.get(field)) is int,
            f"executed matrix operation is missing {field}",
        )
    cache_kind = checked_operation["operation_kind"] in {
        "cache_lookup",
        "cache_read",
        "cache_insert",
    }
    body = {
        "schema_version": result.get("container_result_schema_version"),
        "status": "completed",
        "outcome_type": "completed",
        "telemetry_complete": result.get("telemetry_complete"),
        "credentials_recorded": result.get("credentials_recorded"),
        "semantic_task_quality_evaluated": result.get(
            "semantic_task_quality_evaluated"
        ),
        "idempotent_replay": result.get("idempotent_replay"),
        "operation_key": result.get("operation_key"),
        "operation_kind": result.get("operation_kind"),
        "execution_node_id": result.get("execution_node_id"),
        "runtime_epoch": result.get("runtime_epoch"),
        "destination_runtime_epoch": result.get(
            "destination_runtime_epoch"
        ),
        "started_monotonic_ns": result.get("started_monotonic_ns"),
        "finished_monotonic_ns": result.get("finished_monotonic_ns"),
        "service_time_ms": result.get("service_time_ms"),
        "fixture_materialization_ms_excluded_from_storage_measurement": (
            result.get(
                "fixture_materialization_ms_excluded_from_storage_measurement"
            )
        ),
        "application_shaping_target_ms": result.get(
            "application_shaping_target_ms"
        ),
        "network_http_exchange_ms": result.get("network_http_exchange_ms"),
        "application_shaping_sleep_ms": result.get(
            "application_shaping_sleep_ms"
        ),
        "logical_bytes": result.get("logical_bytes"),
        "physical_bytes": result.get("physical_bytes"),
        "cache_result": result.get("cache_result") if cache_kind else None,
        "cache_scope_id": (
            result.get("cache_scope_id") if cache_kind else None
        ),
        "cache_evictions": (
            result.get("cache_evictions") if cache_kind else []
        ),
    }
    status_code = result.get("api_http_status")
    raw = {
        "executor": result.get("api_executor"),
        "ok": True,
        "status_code": status_code,
        "text": _canonical_bytes(body).decode("utf-8"),
    }
    task_id = result.get("task_id")
    _runner_require(
        isinstance(task_id, str) and bool(task_id),
        "executed matrix operation has no task ID",
    )
    selected_worker = contract["selected_worker"]
    assert isinstance(selected_worker, Mapping)
    selected_worker_id = str(selected_worker["worker_id"])
    revalidated = _operation_result(
        raw,
        checked_operation,
        task_id=task_id,
        selected_worker_id=selected_worker_id,
        task_detail={"assigned_worker": result.get("worker_id")},
        expected_runtime_epochs=contract["runtime_epochs"],
        allow_idempotent_replay=allow_idempotent_replay,
    )
    revalidated["started_monotonic_ns"] = result["started_monotonic_ns"]
    revalidated["finished_monotonic_ns"] = result["finished_monotonic_ns"]
    revalidated["phase"] = expected_phase
    expected = _executed_operation_result(
        checked_operation,
        revalidated,
        sequence_index=sequence_index,
    )
    if allow_idempotent_replay:
        _runner_require(
            isinstance(expected_carrier_workflow_id, str)
            and bool(expected_carrier_workflow_id),
            "replay result has no carrier workflow binding",
        )
        expected.update(
            {
                "result_carrier_task_id": task_id,
                "result_carrier_workflow_id": (
                    expected_carrier_workflow_id
                ),
                "measurement_origin": "container-node-idempotency-ledger",
                "original_flowmesh_task_id_known": False,
                "original_flowmesh_workflow_id_known": False,
                "measurement_freshness_established": False,
            }
        )
        _validate_replay_result_provenance(
            result,
            expected_carrier_task_id=task_id,
            expected_carrier_workflow_id=expected_carrier_workflow_id,
        )
    else:
        _validate_absent_replay_result_provenance(result)
    # The original hash covers the complete runtime body, while the offline
    # revalidation body above deliberately contains only whitelisted fields.
    # It remains a correlation value, not a substitute for semantic checks.
    original_result_sha256 = result.get("container_result_sha256")
    _runner_require(
        re.fullmatch(r"[0-9a-f]{64}", str(original_result_sha256 or ""))
        is not None,
        "executed matrix operation has an invalid container result digest",
    )
    expected["container_result_sha256"] = original_result_sha256
    _runner_require(
        dict(result) == expected,
        "executed matrix operation differs from strict v2 revalidation",
    )
    return expected


def _phase_inputs_for_trial(
    operations: Sequence[Mapping[str, Any]],
    *,
    trial_key: str,
    conditional: bool,
) -> tuple[
    list[tuple[str, list[dict[str, Any]], dict[str, list[str]]]],
    set[str],
    Mapping[str, str] | None,
]:
    checked = [_validate_operation(operation) for operation in operations]
    if not conditional:
        return (
            [
                (
                    "unconditional",
                    checked,
                    {
                        str(row["operation_key"]): list(
                            row["dependency_operation_keys"]
                        )
                        for row in checked
                    },
                )
            ],
            set(),
            None,
        )
    resolution = resolve_conditional_container_trial(
        checked, trial_key=trial_key
    )
    by_key = {str(row["operation_key"]): row for row in checked}
    phase_a_keys = [str(key) for key in resolution["phase_a_operation_keys"]]
    phase_b_keys = [str(key) for key in resolution["phase_b_operation_keys"]]
    all_dependencies = resolution["resolved_dependency_operation_keys"]
    phase_a_dependencies = {
        key: list(all_dependencies[key]) for key in phase_a_keys
    }
    phase_b_dependencies = {
        key: list(resolution["phase_b_dependency_operation_keys"][key])
        for key in phase_b_keys
    }
    return (
        [
            ("A", [by_key[key] for key in phase_a_keys], phase_a_dependencies),
            ("B", [by_key[key] for key in phase_b_keys], phase_b_dependencies),
        ],
        {str(key) for key in resolution["inactive_operation_keys"]},
        dict(resolution["cache_outcomes"]),
    )


def _validate_checkpoint_source_binding(
    checkpoint: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    wrapper: Mapping[str, Any],
    source_operations: Sequence[Mapping[str, Any]],
) -> None:
    sequence_index = int(wrapper["sequence_index"])
    trial_key = str(wrapper["trial_key"])
    conditional = str(wrapper["design_id"]) in _CONDITIONAL_DESIGNS
    phases, inactive_keys, expected_outcomes = _phase_inputs_for_trial(
        source_operations,
        trial_key=trial_key,
        conditional=conditional,
    )
    source_rows = [
        _validate_operation(operation) for operation in source_operations
    ]
    source_by_key = {
        str(operation["operation_key"]): operation for operation in source_rows
    }
    phase_by_key = {
        str(operation["operation_key"]): phase
        for phase, phase_operations, _dependencies in phases
        for operation in phase_operations
    }
    adopted_replay_keys = (
        checkpoint.get("adopted_replay_operation_keys", [])
        if checkpoint.get("schema_version")
        == FLOWMESH_CONTAINER_MATRIX_REPLAY_ADOPTED_CHECKPOINT_SCHEMA_VERSION
        else []
    )
    _runner_require(
        isinstance(adopted_replay_keys, list),
        "checkpoint replay adoption keys are invalid",
    )
    adopted_replay_key_set = set(adopted_replay_keys)
    operation_results = checkpoint["operation_results"]
    submissions = checkpoint["submissions"]
    carrier_workflow_by_task_id: dict[str, str] = {}
    if isinstance(submissions, list):
        for submission in submissions:
            if not isinstance(submission, Mapping):
                continue
            workflow_id = submission.get("workflow_id")
            task_ids = submission.get("task_ids")
            if not isinstance(workflow_id, str) or not isinstance(
                task_ids, list
            ):
                continue
            for task_id in task_ids:
                if isinstance(task_id, str):
                    carrier_workflow_by_task_id[task_id] = workflow_id
    _runner_require(
        [row.get("operation_key") for row in operation_results]
        == [row["operation_key"] for row in source_rows],
        "checkpoint operation order differs from the frozen matrix",
    )
    revalidated_results: list[dict[str, Any]] = []
    for result in operation_results:
        key = str(result["operation_key"])
        operation = source_by_key[key]
        if key in inactive_keys:
            expected = _inactive_operation_result(
                operation, sequence_index=sequence_index
            )
            _runner_require(
                dict(result) == expected,
                "inactive matrix operation differs from its frozen operation",
            )
            revalidated_results.append(expected)
        else:
            _runner_require(
                key in phase_by_key,
                "executed matrix operation is outside its frozen phase",
            )
            if key in adopted_replay_key_set:
                _runner_require(
                    operation.get("operation_kind") == "control"
                    and operation.get("logical_bytes") == 0
                    and operation.get("dependency_operation_keys") == []
                    and operation.get("operation_adapter")
                    == "monotonic-control-v1",
                    "only the dependency-free zero-byte schedule control "
                    "operation may be replay-adopted",
                )
            revalidated_results.append(
                _revalidate_executed_operation_result(
                    result,
                    operation,
                    contract=contract,
                    sequence_index=sequence_index,
                    expected_phase=phase_by_key[key],
                    allow_idempotent_replay=key in adopted_replay_key_set,
                    expected_carrier_workflow_id=(
                        carrier_workflow_by_task_id.get(
                            str(result.get("task_id"))
                        )
                        if key in adopted_replay_key_set
                        else None
                    ),
                )
            )

    _runner_require(
        len(submissions) == len(phases),
        "checkpoint workflow count differs from the frozen phase protocol",
    )
    selected_worker_id = str(contract["selected_worker"]["worker_id"])
    expected_task_ids: list[str] = []
    for submission, (phase, phase_operations, dependencies) in zip(
        submissions, phases
    ):
        workflow = build_flowmesh_container_matrix_trial_workflow(
            phase_operations,
            dependencies,
            contract["node_api_urls"],
            selected_worker_id,
            str(contract["run_id"]),
            trial_key,
            phase,
            str(contract["owner"]),
            int(contract["api_task_timeout_seconds"]),
        )
        task_ids = submission.get("task_ids")
        workflow_id = submission.get("workflow_id")
        _runner_require(
            submission.get("sequence_index") == sequence_index
            and submission.get("trial_key") == trial_key
            and submission.get("phase") == phase
            and submission.get("workflow_sha256")
            == _sha256_bytes(_canonical_bytes(workflow))
            and isinstance(task_ids, list)
            and len(task_ids) == len(phase_operations)
            and all(isinstance(task_id, str) and task_id for task_id in task_ids)
            and isinstance(workflow_id, str)
            and bool(workflow_id),
            "checkpoint submission differs from its frozen workflow",
        )
        expected_submission = _submission_record(
            SubmittedWorkflow(workflow_id, tuple(task_ids)),
            workflow,
            sequence_index=sequence_index,
            trial_key=trial_key,
            phase=phase,
            selected_worker_id=selected_worker_id,
        )
        _runner_require(
            dict(submission) == expected_submission,
            "checkpoint submission fields differ from its frozen workflow",
        )
        expected_task_ids.extend(task_ids)
        phase_result_task_ids = [
            row["task_id"]
            for row in revalidated_results
            if row.get("executed") is True and row.get("phase") == phase
        ]
        _runner_require(
            len(phase_result_task_ids) == len(set(phase_result_task_ids))
            and set(phase_result_task_ids) == set(task_ids),
            "checkpoint task results do not match their workflow submission",
        )
    _runner_require(
        len(expected_task_ids) == len(set(expected_task_ids)),
        "checkpoint reuses a FlowMesh task ID",
    )

    observed_outcomes = (
        None
        if expected_outcomes is None
        else {
            key: str(
                next(
                    row["cache_result"]
                    for row in revalidated_results
                    if row["operation_key"] == key
                )
            )
            for key in expected_outcomes
        }
    )
    expected_trial = _build_trial_result(
        wrapper=wrapper,
        operation_results=revalidated_results,
        submissions=submissions,
        expected_cache_outcomes=expected_outcomes,
        observed_cache_outcomes=observed_outcomes,
    )
    if adopted_replay_keys:
        expected_trial.update(
            {
                "schema_version": (
                    FLOWMESH_CONTAINER_MATRIX_REPLAY_ADOPTED_TRIAL_RESULT_SCHEMA_VERSION
                ),
                "replay_result_adoption_id": checkpoint[
                    "replay_result_adoption_id"
                ],
                "adopted_replay_operation_count": len(adopted_replay_keys),
                "adopted_replay_operation_keys": adopted_replay_keys,
                "replayed_operation_service_time_in_non_replayed_result_telemetry": False,
                "non_replayed_result_telemetry": _aggregate_telemetry(
                    [
                        row
                        for row in revalidated_results
                        if row.get("executed") is True
                        and row.get("idempotent_replay") is False
                    ]
                ),
            }
        )
    _runner_require(
        checkpoint.get("trial_result") == expected_trial,
        "matrix trial result or telemetry aggregate differs from its evidence",
    )


def _load_checkpoints(
    path: Path,
    contract: Mapping[str, Any],
    *,
    sources: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    entries = _read_jsonl(path, "matrix trial checkpoint ledger")
    ordered = contract["ordered_trial_keys"]
    _runner_require(
        len(entries) <= len(ordered),
        "matrix checkpoint ledger exceeds the frozen trial count",
    )
    for index, row in enumerate(entries):
        replay_adopted = (
            row.get("schema_version")
            == FLOWMESH_CONTAINER_MATRIX_REPLAY_ADOPTED_CHECKPOINT_SCHEMA_VERSION
        )
        expected_fields = {
            "schema_version",
            "checkpoint_sequence",
            "sequence_index",
            "trial_key",
            "wrapper_sha256",
            "runtime_epochs_before",
            "runtime_epochs_after",
            "trial_result",
            "operation_results",
            "submissions",
            "entry_sha256",
        }
        if replay_adopted:
            expected_fields.update(
                {
                    "replay_result_adoption_id",
                    "replay_result_adoption_entry_sha256",
                    "adopted_replay_operation_count",
                    "adopted_replay_operation_keys",
                    "replayed_operation_service_time_in_non_replayed_result_telemetry",
                }
            )
        _runner_require(
            set(row) == expected_fields,
            "matrix trial checkpoint fields changed",
        )
        _runner_require(
            row.get("schema_version")
            in {
                FLOWMESH_CONTAINER_MATRIX_CHECKPOINT_ENTRY_SCHEMA_VERSION,
                FLOWMESH_CONTAINER_MATRIX_REPLAY_ADOPTED_CHECKPOINT_SCHEMA_VERSION,
            },
            "unsupported matrix trial checkpoint schema",
        )
        _runner_require(
            row.get("entry_sha256")
            == _entry_sha256(row, "entry_sha256"),
            "matrix trial checkpoint digest mismatch",
        )
        _runner_require(
            row.get("checkpoint_sequence") == index
            and row.get("sequence_index") == index,
            "matrix trial checkpoint sequence is not contiguous",
        )
        _runner_require(
            row.get("trial_key") == ordered[index],
            "matrix trial checkpoint is not a completed prefix",
        )
        _runner_require(
            row.get("wrapper_sha256")
            == contract["trial_wrapper_sha256_by_trial_key"][ordered[index]],
            "matrix trial checkpoint wrapper binding changed",
        )
        _runner_require(
            row.get("runtime_epochs_before") == contract["runtime_epochs"]
            and row.get("runtime_epochs_after")
            == contract["runtime_epochs"],
            "matrix trial checkpoint runtime epoch binding changed",
        )
        trial = row.get("trial_result")
        operations = row.get("operation_results")
        submissions = row.get("submissions")
        adopted_replay_keys = (
            row.get("adopted_replay_operation_keys", [])
            if replay_adopted
            else []
        )
        _runner_require(
            isinstance(adopted_replay_keys, list)
            and all(isinstance(key, str) and key for key in adopted_replay_keys)
            and len(adopted_replay_keys) == len(set(adopted_replay_keys))
            and (
                not replay_adopted
                or (
                    bool(adopted_replay_keys)
                    and row.get("adopted_replay_operation_count")
                    == len(adopted_replay_keys)
                    and row.get(
                        "replayed_operation_service_time_in_non_replayed_result_telemetry"
                    )
                    is False
                    and _adoption_identifier(
                        row.get("replay_result_adoption_id")
                    )
                    == row.get("replay_result_adoption_id")
                    and re.fullmatch(
                        r"[0-9a-f]{64}",
                        str(
                            row.get(
                                "replay_result_adoption_entry_sha256", ""
                            )
                        ),
                    )
                    is not None
                )
            ),
            "matrix checkpoint replay adoption metadata is invalid",
        )
        adopted_replay_key_set = set(adopted_replay_keys)
        _runner_require(
            isinstance(trial, Mapping)
            and trial.get("schema_version")
            == (
                FLOWMESH_CONTAINER_MATRIX_REPLAY_ADOPTED_TRIAL_RESULT_SCHEMA_VERSION
                if replay_adopted
                else FLOWMESH_CONTAINER_MATRIX_TRIAL_RESULT_SCHEMA_VERSION
            )
            and trial.get("status") == "COMPLETE"
            and trial.get("trial_key") == ordered[index],
            "matrix trial checkpoint result is invalid",
        )
        _runner_require(
            isinstance(operations, list)
            and len(operations) == trial.get("planned_operation_count"),
            "matrix trial checkpoint operation coverage is invalid",
        )
        _runner_require(
            isinstance(submissions, list)
            and len(submissions) == trial.get("workflow_count"),
            "matrix trial checkpoint submission coverage is invalid",
        )
        operation_keys = [row.get("operation_key") for row in operations]
        _runner_require(
            len(operation_keys) == len(set(operation_keys)),
            "matrix trial checkpoint repeats operation keys",
        )
        carrier_workflow_by_task_id: dict[str, str] = {}
        for submission in submissions:
            if not isinstance(submission, Mapping):
                continue
            workflow_id = submission.get("workflow_id")
            task_ids = submission.get("task_ids")
            if not isinstance(workflow_id, str) or not isinstance(
                task_ids, list
            ):
                continue
            for task_id in task_ids:
                if isinstance(task_id, str):
                    carrier_workflow_by_task_id[task_id] = workflow_id
        for operation in operations:
            replay_operation = (
                operation.get("operation_key") in adopted_replay_key_set
            )
            expected_operation_fields = (
                _expected_raw_operation_result_fields(operation)
                | _MATRIX_RESULT_AUGMENTED_FIELDS
                if operation.get("executed") is True
                else _INACTIVE_MATRIX_OPERATION_RESULT_FIELDS
            )
            _runner_require(
                set(operation) == expected_operation_fields,
                "matrix operation checkpoint fields changed",
            )
            _runner_require(
                operation.get("schema_version")
                == (
                    FLOWMESH_CONTAINER_MATRIX_REPLAY_ADOPTED_OPERATION_RESULT_SCHEMA_VERSION
                    if replay_operation
                    else FLOWMESH_CONTAINER_MATRIX_OPERATION_RESULT_SCHEMA_VERSION
                )
                and operation.get("trial_key") == ordered[index]
                and type(operation.get("executed")) is bool
                and operation.get("frozen_operation_sha256")
                == contract["planned_operation_sha256_by_operation_key"].get(
                    operation.get("operation_key")
                ),
                "matrix operation checkpoint identity is invalid",
            )
            if operation["executed"]:
                _runner_require(
                    operation.get("skip_reason") is None
                    and operation.get("telemetry_recorded") is True
                    and operation.get("telemetry_complete") is True
                    and operation.get("idempotent_replay")
                    is (operation.get("operation_key") in adopted_replay_key_set),
                    "executed matrix operation evidence is invalid",
                )
                if replay_operation:
                    replay_task_id = _text(
                        operation.get("task_id"),
                        "replay checkpoint task_id",
                    )
                    _validate_replay_result_provenance(
                        operation,
                        expected_carrier_task_id=replay_task_id,
                        expected_carrier_workflow_id=_text(
                            carrier_workflow_by_task_id.get(replay_task_id),
                            "replay checkpoint carrier workflow_id",
                        ),
                    )
                else:
                    _validate_absent_replay_result_provenance(operation)
            else:
                _validate_absent_replay_result_provenance(operation)
                _runner_require(
                    operation.get("skip_reason")
                    == "inactive-conditional-branch",
                    "inactive matrix operation has an invalid skip reason",
                )
                _runner_require(
                    operation.get("logical_bytes") is None
                    and operation.get("physical_bytes") is None
                    and operation.get("service_time_ms") is None
                    and operation.get("telemetry_recorded") is False
                    and operation.get("telemetry_complete") is False,
                    "inactive matrix operation invents telemetry",
                )
        _runner_require(
            {
                str(operation["operation_key"])
                for operation in operations
                if operation.get("idempotent_replay") is True
            }
            == adopted_replay_key_set,
            "matrix checkpoint replay adoption coverage changed",
        )
        if replay_adopted:
            replay_operations = [
                operation
                for operation in operations
                if operation.get("idempotent_replay") is True
            ]
            _runner_require(
                len(replay_operations) == 1
                and replay_operations[0].get("operation_kind") == "control"
                and replay_operations[0].get("logical_bytes") == 0
                and replay_operations[0].get("physical_bytes") == 0,
                "only one zero-byte control replay may appear in a checkpoint",
            )
        for submission in submissions:
            workflow_id = submission.get("workflow_id")
            task_ids = submission.get("task_ids")
            _runner_require(
                isinstance(submission, Mapping)
                and submission.get("schema_version")
                == FLOWMESH_CONTAINER_MATRIX_SUBMISSION_SCHEMA_VERSION
                and submission.get("trial_key") == ordered[index]
                and isinstance(workflow_id, str)
                and bool(workflow_id)
                and isinstance(task_ids, list)
                and bool(task_ids)
                and all(
                    isinstance(task_id, str) and bool(task_id)
                    for task_id in task_ids
                )
                and len(task_ids) == len(set(task_ids))
                and submission.get("selected_worker_id")
                == contract["selected_worker"]["worker_id"]
                and submission.get("validated_before_submission") is True
                and submission.get("credentials_recorded") is False,
                "matrix checkpoint submission is invalid",
            )
    all_submissions = [
        submission
        for checkpoint in entries
        for submission in checkpoint["submissions"]
    ]
    workflow_ids = [str(row["workflow_id"]) for row in all_submissions]
    submitted_task_ids = [
        str(task_id)
        for submission in all_submissions
        for task_id in submission["task_ids"]
    ]
    executed_task_ids = [
        str(operation["task_id"])
        for checkpoint in entries
        for operation in checkpoint["operation_results"]
        if operation["executed"] is True
    ]
    _runner_require(
        len(workflow_ids) == len(set(workflow_ids)),
        "matrix checkpoints reuse a FlowMesh workflow ID",
    )
    _runner_require(
        len(submitted_task_ids) == len(set(submitted_task_ids))
        and len(executed_task_ids) == len(set(executed_task_ids))
        and set(submitted_task_ids) == set(executed_task_ids),
        "matrix checkpoint submissions do not uniquely cover executed tasks",
    )
    if sources is not None:
        wrappers = sources["wrappers"]
        source_operations = sources["operations"]
        assert isinstance(wrappers, Sequence)
        assert isinstance(source_operations, Sequence)
        by_trial = _operations_by_trial(source_operations)
        for index, checkpoint in enumerate(entries):
            wrapper = wrappers[index]
            trial_key = str(wrapper["trial_key"])
            _validate_checkpoint_source_binding(
                checkpoint,
                contract=contract,
                wrapper=wrapper,
                source_operations=by_trial[trial_key],
            )
    return entries


def _write_failure(
    target: Path,
    *,
    contract: Mapping[str, Any],
    sequence_index: int,
    trial_key: str,
    phase: str,
    operation_keys: Sequence[str],
    completed_trial_count: int,
    error: BaseException,
) -> None:
    failure_path = target / _FAILURE_FILE
    if failure_path.exists():
        _runner_require(
            failure_path.is_file() and not failure_path.is_symlink(),
            "immutable matrix failure evidence is not a regular file",
        )
        return
    failure: dict[str, Any] = {
        "schema_version": FLOWMESH_CONTAINER_MATRIX_FAILURE_SCHEMA_VERSION,
        "status": "FAILED",
        "run_id": contract["run_id"],
        "matrix_plan_sha256": contract["source_binding"][
            "matrix_plan_sha256"
        ],
        "sequence_index": sequence_index,
        "trial_key": trial_key,
        "phase": phase,
        "operation_keys": list(operation_keys),
        "completed_trial_count": completed_trial_count,
        "error_type": type(error).__name__,
        "error": redact_secrets(str(error)),
        "later_trials_submitted": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    failure["failure_sha256"] = _document_sha256(
        failure, "failure_sha256"
    )
    _atomic_write(failure_path, _json_bytes(failure))


def _reconcile_durable_replay_adoption(
    *,
    contract: Mapping[str, Any],
    sources: Mapping[str, Any],
    journal_path: Path,
    checkpoint_path: Path,
    journal: list[dict[str, Any]],
    checkpoints: list[dict[str, Any]],
    failure_document: Mapping[str, Any],
    adoption_entry: Mapping[str, Any],
    adoption_request: Mapping[str, str],
) -> dict[str, Any]:
    """Finish a partially persisted adoption without any live FlowMesh call."""

    payload = adoption_entry.get("payload")
    _runner_require(
        isinstance(payload, Mapping)
        and payload.get("adoption_id") == adoption_request["adoption_id"]
        and payload.get("adoption_reason")
        == adoption_request["adoption_reason"]
        and payload.get("failed_journal_entry_sha256")
        == adoption_request["adopt_failed_entry_sha256"],
        "replay adoption request differs from its durable authorization",
    )
    sequence_index = int(adoption_entry["sequence_index"])
    trial_key = str(adoption_entry["trial_key"])
    phase = str(adoption_entry["phase"])
    _runner_require(
        phase == "unconditional" and len(checkpoints) in {sequence_index, sequence_index + 1},
        "durable replay adoption prefix is not safely reconcilable",
    )

    wrappers = sources["wrappers"]
    operations = sources["operations"]
    assert isinstance(wrappers, Sequence)
    assert isinstance(operations, Sequence)
    wrapper = wrappers[sequence_index]
    phase_operations = _operations_by_trial(operations)[trial_key]
    expected_keys = [str(row["operation_key"]) for row in phase_operations]
    _runner_require(
        wrapper.get("trial_key") == trial_key
        and str(wrapper.get("design_id")) not in _CONDITIONAL_DESIGNS
        and payload.get("operation_result_keys") == expected_keys,
        "durable replay adoption differs from the frozen trial",
    )

    indexed_entries = {
        str(row.get("entry_sha256")): row
        for row in journal
        if isinstance(row.get("entry_sha256"), str)
    }
    intent_entry = indexed_entries.get(
        str(payload.get("submission_intent_entry_sha256"))
    )
    bound_entry = indexed_entries.get(str(payload.get("workflow_bound_entry_sha256")))
    _runner_require(
        intent_entry is not None
        and intent_entry.get("state") == "SUBMISSION_INTENT"
        and bound_entry is not None
        and bound_entry.get("state") == "WORKFLOW_BOUND",
        "durable replay adoption lost its submitted-workflow binding",
    )
    bound_payload = bound_entry["payload"]
    worker_id = str(contract["selected_worker"]["worker_id"])
    dependencies = {
        str(row["operation_key"]): list(row["dependency_operation_keys"])
        for row in phase_operations
    }
    workflow = build_flowmesh_container_matrix_trial_workflow(
        phase_operations,
        dependencies,
        contract["node_api_urls"],
        worker_id,
        str(contract["run_id"]),
        trial_key,
        phase,
        str(contract["owner"]),
        int(contract["api_task_timeout_seconds"]),
    )
    workflow_sha256 = _sha256_bytes(_canonical_bytes(workflow))
    task_ids = bound_payload.get("task_ids")
    _runner_require(
        payload.get("workflow_sha256") == workflow_sha256
        and bound_payload.get("workflow_sha256") == workflow_sha256
        and isinstance(task_ids, list)
        and len(task_ids) == len(expected_keys),
        "durable replay adoption workflow binding changed",
    )
    submitted = SubmittedWorkflow(str(bound_payload["workflow_id"]), tuple(task_ids))
    submission = _submission_record(
        submitted,
        workflow,
        sequence_index=sequence_index,
        trial_key=trial_key,
        phase=phase,
        selected_worker_id=worker_id,
    )
    records_value = payload.get("operation_results")
    _runner_require(
        isinstance(records_value, list)
        and _sha256_bytes(_canonical_bytes(records_value))
        == payload.get("operation_results_sha256"),
        "durable replay adoption operation results changed",
    )
    records = [dict(row) for row in records_value]
    expected_results_payload = {
        "workflow_sha256": workflow_sha256,
        "submission": submission,
        "operation_results": records,
    }
    adoption_position = journal.index(adoption_entry)
    obtained_entries = [
        row
        for row in journal[adoption_position + 1 :]
        if row.get("state") == "RESULTS_OBTAINED"
        and row.get("sequence_index") == sequence_index
        and row.get("phase") == phase
    ]
    _runner_require(
        len(obtained_entries) <= 1,
        "durable replay adoption has ambiguous obtained results",
    )
    if obtained_entries:
        _runner_require(
            obtained_entries[0].get("payload") == expected_results_payload,
            "durable adopted results differ from their authorization",
        )
    else:
        obtained = _journal_entry(
            journal_path,
            run_id=str(contract["run_id"]),
            state="RESULTS_OBTAINED",
            sequence_index=sequence_index,
            trial_key=trial_key,
            phase=phase,
            payload=expected_results_payload,
        )
        journal.append(obtained)

    if len(checkpoints) == sequence_index:
        checkpoint = _trial_checkpoint(
            contract=contract,
            wrapper=wrapper,
            operations=phase_operations,
            active_records=records,
            submissions=[submission],
            expected_cache_outcomes=None,
            observed_cache_outcomes=None,
            runtime_epochs_before=payload["runtime_epochs_before"],
            runtime_epochs_after=payload["runtime_epochs_after"],
            replay_adoption={
                **payload,
                "adoption_entry_sha256": adoption_entry["entry_sha256"],
            },
        )
        persisted = _append_digest_entry(
            checkpoint_path, checkpoint, digest_field="entry_sha256"
        )
        checkpoints.append(persisted)
    else:
        persisted = checkpoints[sequence_index]
        _runner_require(
            persisted.get("replay_result_adoption_id")
            == payload.get("adoption_id")
            and persisted.get("replay_result_adoption_entry_sha256")
            == adoption_entry.get("entry_sha256"),
            "durable replay-adopted checkpoint binding changed",
        )

    completion_entries = [
        row
        for row in journal[adoption_position + 1 :]
        if row.get("state") == "TRIAL_COMPLETED"
        and row.get("sequence_index") == sequence_index
    ]
    _runner_require(
        len(completion_entries) <= 1,
        "durable replay adoption has duplicate trial completion",
    )
    if completion_entries:
        _runner_require(
            completion_entries[0].get("payload")
            == {"checkpoint_entry_sha256": persisted["entry_sha256"]},
            "durable replay adoption completion binding changed",
        )
    else:
        completed = _journal_entry(
            journal_path,
            run_id=str(contract["run_id"]),
            state="TRIAL_COMPLETED",
            sequence_index=sequence_index,
            trial_key=trial_key,
            phase="trial",
            payload={"checkpoint_entry_sha256": persisted["entry_sha256"]},
        )
        journal.append(completed)
    _validate_journal(
        journal,
        contract,
        checkpoints,
        sources=sources,
        failure_document=failure_document,
    )
    return {
        "status": "REPLAY_RESULTS_ADOPTED",
        "run_id": contract["run_id"],
        "trial_key": trial_key,
        "phase": phase,
        "workflow_id": payload["workflow_id"],
        "adoption_id": payload["adoption_id"],
        "adoption_entry_sha256": adoption_entry["entry_sha256"],
        "adopted_replay_operation_count": 1,
        "adopted_replay_operation_keys": payload[
            "adopted_replay_operation_keys"
        ],
        "completed_trial_count": len(checkpoints),
        "workflow_submitted": False,
        "workflow_validated": False,
        "continue_with_normal_resume": True,
        "eligible_for_scientific_claims": False,
    }


def _adopt_flowmesh_container_matrix_replay_results_exclusive(
    matrix_plan_dir: str | Path,
    formal_execution_profile_dir: str | Path,
    coordinator_plan_dir: str | Path,
    run_dir: str | Path,
    run_id: str,
    client: FlowMeshClientProtocol,
    settings: FlowMeshSettings,
    adoption_id: str,
    adoption_reason: str,
    adopt_failed_entry_sha256: str,
    runtime_epoch_probe: Callable[
        [Mapping[str, str], Sequence[Mapping[str, Any]]], Mapping[str, str]
    ]
    | None = None,
) -> dict[str, Any]:
    """Adopt one safe replay from an already-submitted DONE workflow.

    This is a deliberately separate, adopt-only transaction.  It never calls
    workflow validation or submission and it stops after reconciling the one
    failed phase into a durable trial checkpoint.
    """

    identifier = _run_identifier(run_id)
    request = _normalize_replay_adoption_request(
        adoption_id,
        adoption_reason,
        adopt_failed_entry_sha256,
    )
    _runner_require(
        math.isfinite(float(settings.poll_interval_seconds))
        and float(settings.poll_interval_seconds) > 0.0,
        "FlowMesh poll_interval_seconds must be finite and positive",
    )
    root_identity = _root_endpoint_identity_sha256(settings.base_url)
    target = Path(run_dir).resolve()
    _runner_require(
        target.is_dir() and not (target / "SHA256SUMS").exists(),
        "replay-result adoption requires an incomplete matrix run directory",
    )
    _cleanup_known_atomic_temps(target)
    _runner_require(
        _visible_artifact_files(target)
        <= _FINAL_FILES | {_FAILURE_FILE},
        "incomplete matrix run directory contains unexpected files",
    )

    sources = _load_sources(
        matrix_plan_dir,
        formal_execution_profile_dir,
        coordinator_plan_dir,
    )
    matrix = sources["matrix"]
    wrappers = sources["wrappers"]
    operations = sources["operations"]
    assert isinstance(matrix, Mapping)
    assert isinstance(wrappers, Sequence)
    assert isinstance(operations, Sequence)
    _runner_require(
        settings.worker_alias == matrix["worker_alias"]
        and settings.worker_id is None,
        "adoption worker alias must exactly match the frozen matrix plan",
    )

    contract_path = target / _CONTRACT_FILE
    journal_path = target / _JOURNAL_FILE
    checkpoint_path = target / _CHECKPOINT_FILE
    failure_path = target / _FAILURE_FILE
    _runner_require(
        all(
            path.is_file() and not path.is_symlink()
            for path in (
                contract_path,
                journal_path,
                checkpoint_path,
                failure_path,
            )
        ),
        "replay-result adoption requires complete durable failure evidence",
    )
    contract = _read_json(contract_path, "matrix run contract")
    _validate_contract(contract)
    _runner_require(
        contract["run_id"] == identifier,
        "adoption run_id differs from the durable contract",
    )
    _runner_require(
        contract["flowmesh_root_endpoint_identity_sha256"] == root_identity,
        "FlowMesh Root endpoint changed; refusing replay-result adoption",
    )
    _static_contract_matches_sources(contract, sources)
    checkpoints = _load_checkpoints(
        checkpoint_path,
        contract,
        sources=sources,
    )
    journal = _read_jsonl(journal_path, "matrix run journal")
    failure_document = _read_json(
        failure_path, "matrix run failure document"
    )
    _validate_failure_document(
        failure_document,
        contract,
        checkpoints,
        journal,
    )
    _validate_journal(
        journal,
        contract,
        checkpoints,
        sources=sources,
        failure_document=failure_document,
        allow_last_checkpoint_completion_gap=True,
    )
    matching_adoptions = [
        row
        for row in journal
        if row.get("state") == _REPLAY_ADOPTION_STATE
        and row.get("payload", {}).get("adoption_id")
        == request["adoption_id"]
    ]
    _runner_require(
        len(matching_adoptions) <= 1,
        "replay adoption_id is ambiguous in the durable journal",
    )
    if matching_adoptions:
        return _reconcile_durable_replay_adoption(
            contract=contract,
            sources=sources,
            journal_path=journal_path,
            checkpoint_path=checkpoint_path,
            journal=journal,
            checkpoints=checkpoints,
            failure_document=failure_document,
            adoption_entry=matching_adoptions[0],
            adoption_request=request,
        )
    _runner_require(
        len(journal) >= 4
        and [row.get("state") for row in journal[-4:]]
        == [
            _RECOVERY_STATE,
            "SUBMISSION_INTENT",
            "WORKFLOW_BOUND",
            "RUN_FAILED",
        ],
        "replay-result adoption requires the latest failed post-recovery "
        "workflow binding",
    )
    recovery_entry, intent_entry, bound_entry, failure_entry = journal[-4:]
    sequence_index = int(failure_entry["sequence_index"])
    trial_key = str(failure_entry["trial_key"])
    phase = str(failure_entry["phase"])
    _runner_require(
        request["adopt_failed_entry_sha256"]
        == failure_entry["entry_sha256"]
        and failure_entry["payload"].get("error")
        == "container operation was replayed"
        and phase == "unconditional"
        and all(
            row.get("sequence_index") == sequence_index
            and row.get("trial_key") == trial_key
            and row.get("phase") == phase
            for row in journal[-4:]
        )
        and len(checkpoints) == sequence_index,
        "replay-result adoption is not bound to the latest eligible failure",
    )
    _runner_require(
        not any(
            row.get("state") == _REPLAY_ADOPTION_STATE
            and (
                row.get("payload", {}).get("adoption_id")
                == request["adoption_id"]
                or (
                    row.get("sequence_index") == sequence_index
                    and row.get("phase") == phase
                )
            )
            for row in journal
        ),
        "this matrix phase or adoption_id was already replay-adopted",
    )

    wrapper = wrappers[sequence_index]
    _runner_require(
        wrapper.get("trial_key") == trial_key
        and str(wrapper.get("design_id")) not in _CONDITIONAL_DESIGNS,
        "replay-result adoption supports only the current unconditional trial",
    )
    by_trial = _operations_by_trial(operations)
    phase_operations = by_trial[trial_key]
    expected_keys = [str(row["operation_key"]) for row in phase_operations]
    _runner_require(
        intent_entry["payload"].get("operation_keys") == expected_keys,
        "post-recovery workflow does not cover the exact frozen phase",
    )
    dependencies = {
        str(row["operation_key"]): list(row["dependency_operation_keys"])
        for row in phase_operations
    }
    worker = contract["selected_worker"]
    assert isinstance(worker, Mapping)
    worker_id = str(worker["worker_id"])
    workflow = build_flowmesh_container_matrix_trial_workflow(
        phase_operations,
        dependencies,
        contract["node_api_urls"],
        worker_id,
        identifier,
        trial_key,
        phase,
        str(contract["owner"]),
        int(contract["api_task_timeout_seconds"]),
    )
    workflow_sha256 = _sha256_bytes(_canonical_bytes(workflow))
    bound_payload = bound_entry["payload"]
    _runner_require(
        intent_entry["payload"].get("workflow_sha256") == workflow_sha256
        and bound_payload.get("workflow_sha256") == workflow_sha256,
        "post-recovery workflow differs from the frozen operation request",
    )
    workflow_id = _text(
        bound_payload.get("workflow_id"), "recovered workflow_id"
    )
    task_ids = bound_payload.get("task_ids")
    _runner_require(
        isinstance(task_ids, list)
        and len(task_ids) == len(expected_keys)
        and all(isinstance(task_id, str) and task_id for task_id in task_ids)
        and len(task_ids) == len(set(task_ids)),
        "post-recovery task binding is invalid",
    )

    probe = runtime_epoch_probe or _probe_container_runtime_epochs
    _assert_current_worker(client, expected_worker=worker)
    runtime_epochs_before = _probe_all_epochs(
        probe, matrix=matrix, operations=operations
    )
    _assert_runtime_epochs(runtime_epochs_before, contract)
    terminal = client.wait(workflow_id, settings.poll_interval_seconds)
    _runner_require(
        isinstance(terminal, TerminalWorkflow)
        and terminal.workflow_id == workflow_id
        and terminal.status == "DONE"
        and not terminal.failed_task_ids
        and not terminal.cancelled_task_ids,
        "replay-result adoption requires the exact bound workflow to be DONE",
    )
    submitted = SubmittedWorkflow(workflow_id, tuple(task_ids))
    records = _collect_results(
        client,
        submitted,
        operations=phase_operations,
        phase=phase,
        selected_worker_id=worker_id,
        expected_runtime_epochs=contract["runtime_epochs"],
        allow_idempotent_replay=True,
    )
    _assert_current_worker(client, expected_worker=worker)
    runtime_epochs_after = _probe_all_epochs(
        probe, matrix=matrix, operations=operations
    )
    _assert_runtime_epochs(runtime_epochs_after, contract)
    replay_records = [
        row for row in records if row.get("idempotent_replay") is True
    ]
    _runner_require(
        len(replay_records) == 1,
        "replay-result adoption requires exactly one replayed operation",
    )
    replay_key = str(replay_records[0]["operation_key"])
    replay_operation = next(
        row
        for row in phase_operations
        if str(row["operation_key"]) == replay_key
    )
    _replay_adoption_operation_evidence(replay_operation)

    adoption_payload = _build_replay_adoption_payload(
        contract=contract,
        failure_entry=failure_entry,
        intent_entry=intent_entry,
        bound_entry=bound_entry,
        recovery_entry=recovery_entry,
        failure_document=failure_document,
        adoption_request=request,
        terminal=terminal,
        records=records,
        replay_operation=replay_operation,
        runtime_epochs_before=runtime_epochs_before,
        runtime_epochs_after=runtime_epochs_after,
        adoption_ordinal=(
            1
            + sum(
                row.get("state") == _REPLAY_ADOPTION_STATE
                for row in journal
            )
        ),
    )
    adoption_entry = _journal_entry(
        journal_path,
        run_id=identifier,
        state=_REPLAY_ADOPTION_STATE,
        sequence_index=sequence_index,
        trial_key=trial_key,
        phase=phase,
        payload=adoption_payload,
    )
    journal.append(adoption_entry)
    submission = _submission_record(
        submitted,
        workflow,
        sequence_index=sequence_index,
        trial_key=trial_key,
        phase=phase,
        selected_worker_id=worker_id,
    )
    obtained = _journal_entry(
        journal_path,
        run_id=identifier,
        state="RESULTS_OBTAINED",
        sequence_index=sequence_index,
        trial_key=trial_key,
        phase=phase,
        payload={
            "workflow_sha256": workflow_sha256,
            "submission": submission,
            "operation_results": records,
        },
    )
    journal.append(obtained)
    checkpoint = _trial_checkpoint(
        contract=contract,
        wrapper=wrapper,
        operations=phase_operations,
        active_records=records,
        submissions=[submission],
        expected_cache_outcomes=None,
        observed_cache_outcomes=None,
        runtime_epochs_before=runtime_epochs_before,
        runtime_epochs_after=runtime_epochs_after,
        replay_adoption={
            **adoption_payload,
            "adoption_entry_sha256": adoption_entry["entry_sha256"],
        },
    )
    persisted = _append_digest_entry(
        checkpoint_path,
        checkpoint,
        digest_field="entry_sha256",
    )
    checkpoints.append(persisted)
    completed = _journal_entry(
        journal_path,
        run_id=identifier,
        state="TRIAL_COMPLETED",
        sequence_index=sequence_index,
        trial_key=trial_key,
        phase="trial",
        payload={"checkpoint_entry_sha256": persisted["entry_sha256"]},
    )
    journal.append(completed)
    _validate_journal(
        journal,
        contract,
        checkpoints,
        sources=sources,
        failure_document=failure_document,
    )
    return {
        "status": "REPLAY_RESULTS_ADOPTED",
        "run_id": identifier,
        "trial_key": trial_key,
        "phase": phase,
        "workflow_id": workflow_id,
        "adoption_id": request["adoption_id"],
        "adoption_entry_sha256": adoption_entry["entry_sha256"],
        "adopted_replay_operation_count": 1,
        "adopted_replay_operation_keys": [replay_key],
        "completed_trial_count": len(checkpoints),
        "workflow_submitted": False,
        "workflow_validated": False,
        "continue_with_normal_resume": True,
        "eligible_for_scientific_claims": False,
    }


def adopt_flowmesh_container_matrix_replay_results(
    matrix_plan_dir: str | Path,
    formal_execution_profile_dir: str | Path,
    coordinator_plan_dir: str | Path,
    run_dir: str | Path,
    run_id: str,
    client: FlowMeshClientProtocol,
    settings: FlowMeshSettings,
    adoption_id: str,
    adoption_reason: str,
    adopt_failed_entry_sha256: str,
    runtime_epoch_probe: Callable[
        [Mapping[str, str], Sequence[Mapping[str, Any]]], Mapping[str, str]
    ]
    | None = None,
) -> dict[str, Any]:
    """Safely adopt the sole replayed schedule result without resubmission."""

    target = Path(run_dir).resolve()
    with _exclusive_run_lock(target):
        return _adopt_flowmesh_container_matrix_replay_results_exclusive(
            matrix_plan_dir=matrix_plan_dir,
            formal_execution_profile_dir=formal_execution_profile_dir,
            coordinator_plan_dir=coordinator_plan_dir,
            run_dir=target,
            run_id=run_id,
            client=client,
            settings=settings,
            adoption_id=adoption_id,
            adoption_reason=adoption_reason,
            adopt_failed_entry_sha256=adopt_failed_entry_sha256,
            runtime_epoch_probe=runtime_epoch_probe,
        )


def _finalize(
    target: Path,
    *,
    contract: Mapping[str, Any],
    checkpoints: Sequence[Mapping[str, Any]],
    journal_entries: Sequence[Mapping[str, Any]],
    failure_document: Mapping[str, Any] | None,
    resume_performed: bool,
    reused_trial_count: int,
    executed_this_invocation: int,
) -> dict[str, Any]:
    trial_results = [dict(row["trial_result"]) for row in checkpoints]
    operation_results = [
        dict(operation)
        for checkpoint in checkpoints
        for operation in checkpoint["operation_results"]
    ]
    submissions = [
        dict(submission)
        for checkpoint in checkpoints
        for submission in checkpoint["submissions"]
    ]
    executed = [row for row in operation_results if row["executed"] is True]
    inactive = [row for row in operation_results if row["executed"] is False]
    recovery_entries = [
        row for row in journal_entries if row.get("state") == _RECOVERY_STATE
    ]
    adoption_entries = [
        row
        for row in journal_entries
        if row.get("state") == _REPLAY_ADOPTION_STATE
    ]
    failure_entries = [
        row for row in journal_entries if row.get("state") == "RUN_FAILED"
    ]
    recovered = bool(recovery_entries)
    replay_adopted = bool(adoption_entries)
    _runner_require(
        (not recovered and failure_document is None)
        or (
            recovered
            and failure_document is not None
            and len(failure_entries)
            == len(recovery_entries) + len(adoption_entries)
        ),
        "completed matrix recovery evidence is inconsistent",
    )
    total_workflow_count = sum(
        row.get("state") == "WORKFLOW_BOUND" for row in journal_entries
    )
    summary: dict[str, Any] = {
        "schema_version": (
            FLOWMESH_CONTAINER_MATRIX_REPLAY_ADOPTED_RUN_SCHEMA_VERSION
            if replay_adopted
            else (
                FLOWMESH_CONTAINER_MATRIX_RECOVERED_RUN_SCHEMA_VERSION
                if recovered
                else FLOWMESH_CONTAINER_MATRIX_RUN_SCHEMA_VERSION
            )
        ),
        "status": "COMPLETE",
        "run_id": contract["run_id"],
        "matrix_id": contract["matrix_id"],
        "matrix_plan_sha256": contract["source_binding"][
            "matrix_plan_sha256"
        ],
        "formal_execution_profile_sha256": contract["source_binding"][
            "formal_execution_profile_sha256"
        ],
        "coordinator_plan_sha256": contract["source_binding"][
            "coordinator_plan_sha256"
        ],
        "contract_sha256": contract["contract_sha256"],
        "selected_worker": contract["selected_worker"],
        "runtime_epochs": contract["runtime_epochs"],
        "planned_trial_count": 64,
        "completed_trial_count": len(trial_results),
        "planned_operation_count": 500,
        "executed_operation_count": len(executed),
        "inactive_operation_count": len(inactive),
        "workflow_count": len(submissions),
        "flowmesh_workflow_count": total_workflow_count,
        "primary_trial_wrapper_max_concurrency": 1,
        "global_serial_execution_observed": True,
        "resume_performed": resume_performed,
        "checkpoint_reused_trial_count": reused_trial_count,
        "executed_this_invocation": executed_this_invocation,
        "completed_output_reused": False,
        "queue_time_measured": False,
        "semantic_task_quality_evaluated": False,
        "llm_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    if recovered:
        assert failure_document is not None
        summary.update(
            {
                "canonical_workflow_count": len(submissions),
                "abandoned_workflow_count": len(recovery_entries),
                "infrastructure_failure_count": len(failure_entries),
                "infrastructure_recovery_count": len(recovery_entries),
                "recovery_ids": [
                    row["payload"]["recovery_id"]
                    for row in recovery_entries
                ],
                "recovery_runner_module_sha256_by_recovery_id": {
                    row["payload"]["recovery_id"]: row["payload"][
                        "recovery_runner_module_sha256"
                    ]
                    for row in recovery_entries
                },
                "initial_failure_sha256": failure_document[
                    "failure_sha256"
                ],
            }
        )
        if replay_adopted:
            replayed_operations = [
                row
                for row in executed
                if row.get("idempotent_replay") is True
            ]
            summary.update(
                {
                    "replay_result_adoption_count": len(adoption_entries),
                    "replay_result_adoption_ids": [
                        row["payload"]["adoption_id"]
                        for row in adoption_entries
                    ],
                    "adoption_runner_module_sha256_by_adoption_id": {
                        row["payload"]["adoption_id"]: row["payload"][
                            "adoption_runner_module_sha256"
                        ]
                        for row in adoption_entries
                    },
                    "adopted_replay_operation_count": len(
                        replayed_operations
                    ),
                    "adopted_replay_operation_keys": [
                        row["operation_key"] for row in replayed_operations
                    ],
                    "failed_flowmesh_task_records_in_canonical_results": False,
                    "adopted_prior_node_measurements_in_canonical_results": True,
                    "original_flowmesh_task_ids_known_for_adopted_replays": False,
                    "original_flowmesh_workflow_ids_known_for_adopted_replays": False,
                    "measurement_freshness_established_for_adopted_replays": False,
                    "root_dispatch_history_interpretation": (
                        "non-historical-diagnostic-only"
                    ),
                    "prior_before_dispatch_interpretation_superseded": True,
                }
            )
        else:
            summary[
                "failed_infrastructure_attempts_in_canonical_results"
            ] = False
    summary["run_sha256"] = _document_sha256(summary, "run_sha256")
    documents = {
        _CONTRACT_FILE: (target / _CONTRACT_FILE).read_bytes(),
        _JOURNAL_FILE: (target / _JOURNAL_FILE).read_bytes(),
        _CHECKPOINT_FILE: (target / _CHECKPOINT_FILE).read_bytes(),
        _RUN_FILE: _json_bytes(summary),
        _TRIAL_RESULTS_FILE: _jsonl_bytes(trial_results),
        _OPERATION_RESULTS_FILE: _jsonl_bytes(operation_results),
        _SUBMISSIONS_FILE: _jsonl_bytes(submissions),
    }
    if recovered:
        documents[_FAILURE_FILE] = (target / _FAILURE_FILE).read_bytes()
    for name, content in documents.items():
        if name in {_CONTRACT_FILE, _JOURNAL_FILE, _CHECKPOINT_FILE}:
            continue
        _atomic_write(target / name, content)
    _atomic_write(target / "SHA256SUMS", _checksums(documents))
    return summary


def _static_contract_matches_sources(
    contract: Mapping[str, Any], sources: Mapping[str, Any]
) -> None:
    matrix = sources["matrix"]
    wrappers = sources["wrappers"]
    operations = sources["operations"]
    assert isinstance(matrix, Mapping)
    assert isinstance(wrappers, Sequence)
    assert isinstance(operations, Sequence)
    _runner_require(
        contract.get("source_binding") == _source_binding(sources),
        "matrix run source binding changed",
    )
    _runner_require(
        contract.get("matrix_id") == matrix.get("matrix_id")
        and contract.get("worker_alias") == matrix.get("worker_alias")
        and contract.get("node_api_urls") == matrix.get("node_api_urls")
        and contract.get("api_task_timeout_seconds")
        == matrix.get("api_task_timeout_seconds"),
        "matrix run contract differs from its matrix source",
    )
    _runner_require(
        contract.get("ordered_trial_keys")
        == [str(row["trial_key"]) for row in wrappers],
        "matrix run trial order differs from its coordinator source",
    )
    wrapper_digests = {
        str(row["trial_key"]): _sha256_bytes(_canonical_bytes(row))
        for row in wrappers
    }
    _runner_require(
        contract.get("trial_wrapper_sha256_by_trial_key")
        == wrapper_digests,
        "matrix run wrapper digest map differs from its coordinator source",
    )
    _runner_require(
        contract.get("planned_operation_keys")
        == [str(row["operation_key"]) for row in operations],
        "matrix run operation order differs from its matrix source",
    )
    _runner_require(
        contract.get("planned_operation_sha256_by_operation_key")
        == {
            str(row["operation_key"]): _sha256_bytes(_canonical_bytes(row))
            for row in operations
        },
        "matrix run operation digest map differs from its matrix source",
    )
    by_trial = _operations_by_trial(operations)
    expected = _expected_execution(wrappers, by_trial)
    _runner_require(
        contract.get("expected_active_operation_keys")
        == expected["active_operation_keys"]
        and contract.get("expected_inactive_operation_keys")
        == expected["inactive_operation_keys"]
        and contract.get("expected_workflow_count")
        == expected["workflow_count"],
        "matrix run execution partition differs from its frozen sources",
    )
    _runner_require(
        contract.get("resolution_sha256_by_trial_key")
        == expected["resolution_sha256_by_trial"],
        "matrix run resolution digest map differs from frozen operations",
    )
    _runner_require(
        contract.get("expected_phase_operation_keys_by_trial_key")
        == expected["phase_operation_keys_by_trial"],
        "matrix run phase operation map differs from frozen operations",
    )
    for wrapper in wrappers:
        trial_key = str(wrapper["trial_key"])
        protocol = wrapper.get("conditional_protocol")
        _runner_require(
            isinstance(protocol, Mapping),
            "matrix coordinator wrapper has no conditional protocol",
        )
        expected_resolution = expected["resolution_sha256_by_trial"][
            trial_key
        ]
        if expected_resolution is None:
            _runner_require(
                "resolution_sha256" not in protocol,
                "unconditional coordinator wrapper records a resolution",
            )
        else:
            _runner_require(
                protocol.get("resolution_sha256") == expected_resolution,
                "coordinator wrapper resolution differs from frozen operations",
            )


def _run_flowmesh_container_matrix_exclusive(
    matrix_plan_dir: str | Path,
    formal_execution_profile_dir: str | Path,
    coordinator_plan_dir: str | Path,
    output_dir: str | Path,
    run_id: str,
    client: FlowMeshClientProtocol,
    settings: FlowMeshSettings,
    runtime_epoch_probe: Callable[
        [Mapping[str, str], Sequence[Mapping[str, Any]]], Mapping[str, str]
    ]
    | None = None,
    recovery_id: str | None = None,
    recovery_reason: str | None = None,
    recover_failed_entry_sha256: str | None = None,
) -> dict[str, Any]:
    """Execute or resume the frozen 64-wrapper matrix, strictly serially."""

    identifier = _run_identifier(run_id)
    _runner_require(
        math.isfinite(float(settings.poll_interval_seconds))
        and float(settings.poll_interval_seconds) > 0.0,
        "FlowMesh poll_interval_seconds must be finite and positive",
    )
    # Reject credential-bearing or route-ambiguous Root URLs before any
    # client lookup or container health probe can cause external I/O.
    _root_endpoint_identity_sha256(settings.base_url)
    recovery_request = _normalize_recovery_request(
        recovery_id,
        recovery_reason,
        recover_failed_entry_sha256,
    )
    target = Path(output_dir).resolve()
    if recovery_request is not None:
        _runner_require(
            target.is_dir(),
            "infrastructure recovery requires an existing failed run "
            "directory",
        )
    if target.is_dir() and (target / "SHA256SUMS").is_file():
        completed_contract = _read_json(
            target / _CONTRACT_FILE, "matrix run contract"
        )
        _validate_contract(completed_contract)
        _runner_require(
            settings.worker_alias == completed_contract["worker_alias"]
            and settings.worker_id is None,
            "run worker alias must exactly match the frozen matrix plan",
        )
        _runner_require(
            completed_contract["flowmesh_root_endpoint_identity_sha256"]
            == _root_endpoint_identity_sha256(settings.base_url),
            "FlowMesh Root endpoint changed; refusing completed-run reuse",
        )
        verified = verify_flowmesh_container_matrix_run(
            target,
            matrix_plan_dir=matrix_plan_dir,
            formal_execution_profile_dir=formal_execution_profile_dir,
            coordinator_plan_dir=coordinator_plan_dir,
        )
        summary = _read_json(target / _RUN_FILE, "matrix run summary")
        _runner_require(
            summary.get("run_id") == identifier,
            "completed matrix output has a different run_id",
        )
        if recovery_request is not None:
            _validate_repeated_recovery_request(
                _read_jsonl(target / _JOURNAL_FILE, "matrix run journal"),
                recovery_request,
            )
        return {
            **summary,
            "output_dir": str(target),
            "completed_output_reused": True,
            "executed_this_invocation": 0,
            "checkpoint_reused_trial_count": summary[
                "completed_trial_count"
            ],
            "resume_performed": True,
            "verification_status": verified["status"],
        }

    sources = _load_sources(
        matrix_plan_dir,
        formal_execution_profile_dir,
        coordinator_plan_dir,
    )
    matrix = sources["matrix"]
    wrappers = sources["wrappers"]
    operations = sources["operations"]
    assert isinstance(matrix, Mapping)
    assert isinstance(wrappers, Sequence)
    assert isinstance(operations, Sequence)
    _runner_require(
        settings.worker_alias == matrix["worker_alias"]
        and settings.worker_id is None,
        "run worker alias must exactly match the frozen matrix plan",
    )
    probe = runtime_epoch_probe or _probe_container_runtime_epochs

    contract_path = target / _CONTRACT_FILE
    journal_path = target / _JOURNAL_FILE
    checkpoint_path = target / _CHECKPOINT_FILE
    existing_contract: dict[str, Any] | None = None
    if target.exists():
        _runner_require(
            target.is_dir(), "matrix run output exists and is not a directory"
        )
        _cleanup_known_atomic_temps(target)
        actual = _visible_artifact_files(target)
        _runner_require(
            actual <= _FINAL_FILES | {_FAILURE_FILE},
            "incomplete matrix run directory contains unexpected files",
        )
        _runner_require(
            contract_path.is_file()
            and journal_path.is_file()
            and checkpoint_path.is_file(),
            "incomplete matrix run is missing durable state",
        )
        existing_contract = _read_json(
            contract_path, "matrix run contract"
        )
        _validate_contract(existing_contract)
        _runner_require(
            existing_contract["run_id"] == identifier,
            "resume run_id differs from the durable contract",
        )
        _runner_require(
            existing_contract["flowmesh_root_endpoint_identity_sha256"]
            == _root_endpoint_identity_sha256(settings.base_url),
            "FlowMesh Root endpoint changed; refusing matrix resume",
        )
        _static_contract_matches_sources(existing_contract, sources)
        # Validate the durable recovery request before any Root lookup or
        # container health probe. A wrong digest must never trigger live I/O.
        preflight_checkpoints = _load_checkpoints(
            checkpoint_path,
            existing_contract,
            sources=sources,
        )
        preflight_journal = _read_jsonl(
            journal_path, "matrix run journal"
        )
        preflight_failure: dict[str, Any] | None = None
        if (target / _FAILURE_FILE).is_file():
            preflight_failure = _read_json(
                target / _FAILURE_FILE,
                "matrix run failure document",
            )
            _validate_failure_document(
                preflight_failure,
                existing_contract,
                preflight_checkpoints,
                preflight_journal,
            )
        else:
            _runner_require(
                not any(
                    row.get("state") == "RUN_FAILED"
                    for row in preflight_journal
                ),
                "matrix journal records a failure but its immutable failure "
                "document is missing",
            )
        _validate_journal(
            preflight_journal,
            existing_contract,
            preflight_checkpoints,
            sources=sources,
            failure_document=preflight_failure,
            allow_last_checkpoint_completion_gap=True,
        )
        pending_adoptions = [
            row
            for row in preflight_journal
            if row.get("state") == _REPLAY_ADOPTION_STATE
            and int(row.get("sequence_index", -1))
            >= len(preflight_checkpoints)
        ]
        _runner_require(
            not pending_adoptions,
            "matrix run has an incomplete replay-result adoption; resume it "
            "with adopt-flowmesh-container-matrix-replay-results before "
            "normal execution",
        )
        preflight_unresolved = _unresolved_failure(preflight_journal)
        if preflight_unresolved is not None:
            _runner_require(
                recovery_request is not None,
                "matrix run has a durable terminal failure; explicit audited "
                "recovery requires --recovery-id, --recovery-reason, and "
                "--recover-failed-entry-sha256",
            )
            _runner_require(
                recovery_request["recover_failed_entry_sha256"]
                == preflight_unresolved.get("entry_sha256"),
                "recovery authorization does not bind the terminal "
                "RUN_FAILED entry",
            )
            failed_sequence = preflight_unresolved.get("sequence_index")
            failed_phase = preflight_unresolved.get("phase")
            _runner_require(
                not any(
                    row.get("state") == _RECOVERY_STATE
                    and row.get("sequence_index") == failed_sequence
                    and row.get("phase") == failed_phase
                    for row in preflight_journal
                ),
                "a matrix phase may receive at most one infrastructure "
                "recovery",
            )
            _runner_require(
                not any(
                    row.get("state") == _RECOVERY_STATE
                    and row.get("payload", {}).get("recovery_id")
                    == recovery_request["recovery_id"]
                    for row in preflight_journal
                ),
                "recovery_id was already used by this matrix run",
            )
        elif recovery_request is not None:
            _validate_repeated_recovery_request(
                preflight_journal,
                recovery_request,
            )

    identity = describe_pinned_worker(client, settings)
    runtime_epochs = _probe_all_epochs(
        probe, matrix=matrix, operations=operations
    )
    expected_contract = _build_contract(
        sources=sources,
        run_id=identifier,
        identity=identity,
        runtime_epochs=runtime_epochs,
        settings=settings,
    )
    if existing_contract is None:
        _runner_require(
            not target.exists(), "matrix run output directory already exists"
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        target.mkdir()
        _atomic_write(contract_path, _json_bytes(expected_contract))
        _atomic_write(journal_path, b"")
        _atomic_write(checkpoint_path, b"")
        contract = expected_contract
    else:
        _runner_require(
            existing_contract["selected_worker"]
            == _stable_worker_identity(identity),
            "FlowMesh worker stable identity changed; start a new output "
            "directory",
        )
        _runner_require(
            existing_contract["runtime_epochs"] == runtime_epochs,
            "container runtime epoch changed; start a new output directory",
        )
        _runner_require(
            existing_contract == expected_contract,
            "matrix resume contract changed",
        )
        contract = existing_contract

    checkpoints = _load_checkpoints(
        checkpoint_path, contract, sources=sources
    )
    journal_entries = _read_jsonl(journal_path, "matrix run journal")
    failure_path = target / _FAILURE_FILE
    failure_document: dict[str, Any] | None = None
    if failure_path.is_file():
        failure_document = _read_json(
            failure_path, "matrix run failure document"
        )
        _validate_failure_document(
            failure_document,
            contract,
            checkpoints,
            journal_entries,
        )
    else:
        _runner_require(
            not any(row.get("state") == "RUN_FAILED" for row in journal_entries),
            "matrix journal records a failure but its immutable failure "
            "document is missing",
        )
    journal_entries = _reconcile_checkpoint_completion(
        journal_path,
        journal_entries,
        checkpoints,
        contract,
        sources=sources,
        failure_document=failure_document,
    )
    unresolved_failure = _unresolved_failure(journal_entries)
    if unresolved_failure is not None:
        _runner_require(
            recovery_request is not None,
            "matrix run has a durable terminal failure; explicit audited "
            "recovery requires --recovery-id, --recovery-reason, and "
            "--recover-failed-entry-sha256",
        )
        _runner_require(
            failure_document is not None,
            "matrix infrastructure recovery evidence is missing",
        )
        _authorize_infrastructure_recovery(
            client,
            contract=contract,
            operations=operations,
            journal_path=journal_path,
            journal_entries=journal_entries,
            failure_document=failure_document,
            recovery_request=recovery_request,
        )
        _validate_journal(
            journal_entries,
            contract,
            checkpoints,
            sources=sources,
            failure_document=failure_document,
        )
    elif recovery_request is not None:
        _validate_repeated_recovery_request(
            journal_entries,
            recovery_request,
        )
    reused_trial_count = len(checkpoints)
    resume_performed = existing_contract is not None
    executed_this_invocation = 0
    by_trial = _operations_by_trial(operations)
    worker = contract["selected_worker"]
    assert isinstance(worker, Mapping)
    worker_id = str(worker["worker_id"])

    for wrapper in wrappers[len(checkpoints) :]:
        sequence_index = int(wrapper["sequence_index"])
        trial_key = str(wrapper["trial_key"])
        rows = by_trial[trial_key]
        phase_for_failure = "preflight"
        current_operation_keys = [str(row["operation_key"]) for row in rows]
        try:
            _assert_current_worker(
                client,
                expected_worker=worker,
            )
            before = _probe_all_epochs(
                probe, matrix=matrix, operations=operations
            )
            _assert_runtime_epochs(before, contract)
            submissions: list[dict[str, Any]] = []
            active_records: list[dict[str, Any]] = []
            expected_outcomes: dict[str, str] | None = None
            observed_outcomes: dict[str, str] | None = None

            if str(wrapper["design_id"]) in _CONDITIONAL_DESIGNS:
                resolution = resolve_conditional_container_trial(
                    rows, trial_key=trial_key
                )
                _runner_require(
                    _sha256_bytes(_canonical_bytes(resolution))
                    == contract["resolution_sha256_by_trial_key"][trial_key],
                    "conditional trial resolution changed",
                )
                by_key = {str(row["operation_key"]): row for row in rows}
                phase_a_keys = [
                    str(key) for key in resolution["phase_a_operation_keys"]
                ]
                phase_a_rows = [by_key[key] for key in phase_a_keys]
                all_dependencies = resolution[
                    "resolved_dependency_operation_keys"
                ]
                phase_a_dependencies = {
                    key: list(all_dependencies[key]) for key in phase_a_keys
                }
                phase_for_failure = "A"
                current_operation_keys = phase_a_keys
                submission_a, records_a = _execute_phase(
                    client=client,
                    settings=settings,
                    contract=contract,
                    journal_path=journal_path,
                    journal_entries=journal_entries,
                    sequence_index=sequence_index,
                    trial_key=trial_key,
                    phase="A",
                    operations=phase_a_rows,
                    dependencies=phase_a_dependencies,
                )
                submissions.append(submission_a)
                active_records.extend(records_a)
                expected_outcomes = dict(resolution["cache_outcomes"])
                observed_outcomes = {
                    key: str(
                        next(
                            row["cache_result"]
                            for row in records_a
                            if row["operation_key"] == key
                        )
                    )
                    for key in expected_outcomes
                }
                _runner_require(
                    observed_outcomes == expected_outcomes,
                    "live cache outcomes do not match the frozen conditional "
                    "matrix plan; refusing phase B",
                )
                _assert_current_worker(
                    client,
                    expected_worker=worker,
                )
                after_a = _probe_all_epochs(
                    probe, matrix=matrix, operations=operations
                )
                _assert_runtime_epochs(after_a, contract)
                phase_b_keys = [
                    str(key) for key in resolution["phase_b_operation_keys"]
                ]
                phase_b_rows = [by_key[key] for key in phase_b_keys]
                phase_b_dependencies = {
                    key: list(
                        resolution["phase_b_dependency_operation_keys"][key]
                    )
                    for key in phase_b_keys
                }
                phase_for_failure = "B"
                current_operation_keys = phase_b_keys
                submission_b, records_b = _execute_phase(
                    client=client,
                    settings=settings,
                    contract=contract,
                    journal_path=journal_path,
                    journal_entries=journal_entries,
                    sequence_index=sequence_index,
                    trial_key=trial_key,
                    phase="B",
                    operations=phase_b_rows,
                    dependencies=phase_b_dependencies,
                )
                submissions.append(submission_b)
                active_records.extend(records_b)
            else:
                _runner_require(
                    all(row.get("condition") is None for row in rows),
                    "unconditional matrix wrapper contains a branch operation",
                )
                dependencies = {
                    str(row["operation_key"]): list(
                        row["dependency_operation_keys"]
                    )
                    for row in rows
                }
                phase_for_failure = "unconditional"
                current_operation_keys = [
                    str(row["operation_key"]) for row in rows
                ]
                submission, records = _execute_phase(
                    client=client,
                    settings=settings,
                    contract=contract,
                    journal_path=journal_path,
                    journal_entries=journal_entries,
                    sequence_index=sequence_index,
                    trial_key=trial_key,
                    phase="unconditional",
                    operations=rows,
                    dependencies=dependencies,
                )
                submissions.append(submission)
                active_records.extend(records)

            _assert_current_worker(
                client,
                expected_worker=worker,
            )
            after = _probe_all_epochs(
                probe, matrix=matrix, operations=operations
            )
            _assert_runtime_epochs(after, contract)
            checkpoint = _trial_checkpoint(
                contract=contract,
                wrapper=wrapper,
                operations=rows,
                active_records=active_records,
                submissions=submissions,
                expected_cache_outcomes=expected_outcomes,
                observed_cache_outcomes=observed_outcomes,
                runtime_epochs_before=before,
                runtime_epochs_after=after,
            )
            persisted = _append_digest_entry(
                checkpoint_path,
                checkpoint,
                digest_field="entry_sha256",
            )
            checkpoints.append(persisted)
            completed = _journal_entry(
                journal_path,
                run_id=identifier,
                state="TRIAL_COMPLETED",
                sequence_index=sequence_index,
                trial_key=trial_key,
                phase="trial",
                payload={
                    "checkpoint_entry_sha256": persisted["entry_sha256"]
                },
            )
            journal_entries.append(completed)
            executed_this_invocation += 1
        except Exception as exc:
            failed = _journal_entry(
                journal_path,
                run_id=identifier,
                state="RUN_FAILED",
                sequence_index=sequence_index,
                trial_key=trial_key,
                phase=phase_for_failure,
                payload={"error": redact_secrets(str(exc))},
            )
            journal_entries.append(failed)
            _write_failure(
                target,
                contract=contract,
                sequence_index=sequence_index,
                trial_key=trial_key,
                phase=phase_for_failure,
                operation_keys=current_operation_keys,
                completed_trial_count=len(checkpoints),
                error=exc,
            )
            raise FlowMeshContainerMatrixRunError(
                redact_secrets(str(exc))
                + "; durable RUN_FAILED entry_sha256="
                + str(failed["entry_sha256"])
            ) from None

    _runner_require(
        len(checkpoints) == 64,
        "matrix execution ended without all 64 durable trial checkpoints",
    )
    _validate_journal(
        journal_entries,
        contract,
        checkpoints,
        sources=sources,
        failure_document=failure_document,
        require_complete=True,
    )
    summary = _finalize(
        target,
        contract=contract,
        checkpoints=checkpoints,
        journal_entries=journal_entries,
        failure_document=failure_document,
        resume_performed=resume_performed,
        reused_trial_count=reused_trial_count,
        executed_this_invocation=executed_this_invocation,
    )
    verified = verify_flowmesh_container_matrix_run(
        target,
        matrix_plan_dir=matrix_plan_dir,
        formal_execution_profile_dir=formal_execution_profile_dir,
        coordinator_plan_dir=coordinator_plan_dir,
    )
    return {
        **summary,
        "output_dir": str(target),
        "verification_status": verified["status"],
    }


def run_flowmesh_container_matrix(
    matrix_plan_dir: str | Path,
    formal_execution_profile_dir: str | Path,
    coordinator_plan_dir: str | Path,
    output_dir: str | Path,
    run_id: str,
    client: FlowMeshClientProtocol,
    settings: FlowMeshSettings,
    runtime_epoch_probe: Callable[
        [Mapping[str, str], Sequence[Mapping[str, Any]]], Mapping[str, str]
    ]
    | None = None,
    recovery_id: str | None = None,
    recovery_reason: str | None = None,
    recover_failed_entry_sha256: str | None = None,
) -> dict[str, Any]:
    """Execute or resume one matrix while holding its process-wide lease."""

    target = Path(output_dir).resolve()
    with _exclusive_run_lock(target):
        return _run_flowmesh_container_matrix_exclusive(
            matrix_plan_dir=matrix_plan_dir,
            formal_execution_profile_dir=formal_execution_profile_dir,
            coordinator_plan_dir=coordinator_plan_dir,
            output_dir=target,
            run_id=run_id,
            client=client,
            settings=settings,
            runtime_epoch_probe=runtime_epoch_probe,
            recovery_id=recovery_id,
            recovery_reason=recovery_reason,
            recover_failed_entry_sha256=recover_failed_entry_sha256,
        )


def _verify_checksum_file(root: Path, expected: set[str]) -> None:
    checksum_path = root / "SHA256SUMS"
    _runner_require(checksum_path.is_file(), "matrix run SHA256SUMS is missing")
    observed: dict[str, str] = {}
    try:
        lines = checksum_path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FlowMeshContainerMatrixRunError(
            "cannot read matrix run SHA256SUMS"
        ) from exc
    for line in lines:
        digest, separator, name = line.partition("  ")
        _runner_require(
            separator == "  "
            and re.fullmatch(r"[0-9a-f]{64}", digest) is not None
            and name in expected
            and name not in observed,
            "matrix run SHA256SUMS contains an invalid row",
        )
        observed[name] = digest
    _runner_require(
        set(observed) == expected,
        "matrix run SHA256SUMS file set is incomplete",
    )
    for name, digest in observed.items():
        _runner_require(
            (root / name).is_file()
            and _sha256_path(root / name, name) == digest,
            f"matrix run checksum mismatch: {name}",
        )


def verify_flowmesh_container_matrix_run(
    run_dir: str | Path,
    matrix_plan_dir: str | Path | None = None,
    formal_execution_profile_dir: str | Path | None = None,
    coordinator_plan_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Offline-verify a completed matrix run and optional frozen sources."""

    root = Path(run_dir).resolve()
    _runner_require(root.is_dir(), "container matrix run directory does not exist")
    actual = _visible_artifact_files(root)
    has_failure_document = _FAILURE_FILE in actual
    expected_files = _FINAL_FILES | (
        {_FAILURE_FILE} if has_failure_document else set()
    )
    _runner_require(
        actual == expected_files | {"SHA256SUMS"},
        "completed matrix run file set is invalid",
    )
    _verify_checksum_file(root, expected_files)
    contract = _read_json(root / _CONTRACT_FILE, "matrix run contract")
    _validate_contract(contract)
    supplied = (
        matrix_plan_dir,
        formal_execution_profile_dir,
        coordinator_plan_dir,
    )
    _runner_require(
        all(value is None for value in supplied)
        or all(value is not None for value in supplied),
        "matrix, formal profile, and coordinator sources must be supplied "
        "together",
    )
    sources: Mapping[str, Any] | None = None
    source_binding_checked = False
    if all(value is not None for value in supplied):
        assert matrix_plan_dir is not None
        assert formal_execution_profile_dir is not None
        assert coordinator_plan_dir is not None
        sources = _load_sources(
            matrix_plan_dir,
            formal_execution_profile_dir,
            coordinator_plan_dir,
        )
        _static_contract_matches_sources(contract, sources)
        source_binding_checked = True
    checkpoints = _load_checkpoints(
        root / _CHECKPOINT_FILE, contract, sources=sources
    )
    _runner_require(
        len(checkpoints) == 64,
        "completed matrix run does not contain 64 checkpoints",
    )
    journal = _read_jsonl(root / _JOURNAL_FILE, "matrix run journal")
    failure_document: dict[str, Any] | None = None
    if has_failure_document:
        failure_document = _read_json(
            root / _FAILURE_FILE, "matrix run failure document"
        )
        _validate_failure_document(
            failure_document,
            contract,
            checkpoints,
            journal,
        )
    _validate_journal(
        journal,
        contract,
        checkpoints,
        sources=sources,
        failure_document=failure_document,
        require_complete=True,
    )
    recovery_entries = [
        row for row in journal if row.get("state") == _RECOVERY_STATE
    ]
    adoption_entries = [
        row
        for row in journal
        if row.get("state") == _REPLAY_ADOPTION_STATE
    ]
    failure_entries = [
        row for row in journal if row.get("state") == "RUN_FAILED"
    ]
    recovered = bool(recovery_entries)
    replay_adopted = bool(adoption_entries)
    _runner_require(
        recovered == has_failure_document
        and len(failure_entries)
        == len(recovery_entries) + len(adoption_entries),
        "completed matrix recovery evidence is inconsistent",
    )
    summary = _read_json(root / _RUN_FILE, "matrix run summary")
    trial_results = _read_jsonl(
        root / _TRIAL_RESULTS_FILE, "matrix trial results"
    )
    operation_results = _read_jsonl(
        root / _OPERATION_RESULTS_FILE, "matrix operation results"
    )
    submissions = _read_jsonl(
        root / _SUBMISSIONS_FILE, "matrix submissions"
    )
    expected_trials = [dict(row["trial_result"]) for row in checkpoints]
    expected_operations = [
        dict(operation)
        for checkpoint in checkpoints
        for operation in checkpoint["operation_results"]
    ]
    expected_submissions = [
        dict(submission)
        for checkpoint in checkpoints
        for submission in checkpoint["submissions"]
    ]
    _runner_require(
        trial_results == expected_trials,
        "final matrix trial results differ from durable checkpoints",
    )
    _runner_require(
        operation_results == expected_operations,
        "final matrix operation results differ from durable checkpoints",
    )
    _runner_require(
        submissions == expected_submissions,
        "final matrix submissions differ from durable checkpoints",
    )
    _runner_require(
        summary.get("schema_version")
        == (
            FLOWMESH_CONTAINER_MATRIX_REPLAY_ADOPTED_RUN_SCHEMA_VERSION
            if replay_adopted
            else (
                FLOWMESH_CONTAINER_MATRIX_RECOVERED_RUN_SCHEMA_VERSION
                if recovered
                else FLOWMESH_CONTAINER_MATRIX_RUN_SCHEMA_VERSION
            )
        )
        and summary.get("status") == "COMPLETE"
        and summary.get("run_sha256")
        == _document_sha256(summary, "run_sha256"),
        "matrix run summary is invalid",
    )
    _runner_require(
        summary.get("run_id") == contract["run_id"]
        and summary.get("contract_sha256") == contract["contract_sha256"]
        and summary.get("matrix_plan_sha256")
        == contract["source_binding"]["matrix_plan_sha256"],
        "matrix run summary contract binding changed",
    )
    _runner_require(
        summary.get("completed_trial_count") == len(trial_results) == 64
        and summary.get("planned_trial_count") == 64,
        "matrix run trial counts are invalid",
    )
    operation_keys = [row.get("operation_key") for row in operation_results]
    executed_keys = [
        row["operation_key"]
        for row in operation_results
        if row.get("executed") is True
    ]
    inactive_keys = [
        row["operation_key"]
        for row in operation_results
        if row.get("executed") is False
    ]
    _runner_require(
        operation_keys == contract["planned_operation_keys"]
        and len(operation_keys) == len(set(operation_keys)) == 500,
        "matrix run operation coverage or order changed",
    )
    _runner_require(
        executed_keys == contract["expected_active_operation_keys"]
        and inactive_keys == contract["expected_inactive_operation_keys"],
        "matrix run active/inactive operation partition changed",
    )
    _runner_require(
        summary.get("executed_operation_count") == len(executed_keys)
        and summary.get("inactive_operation_count") == len(inactive_keys)
        and summary.get("planned_operation_count") == 500,
        "matrix run operation counts are invalid",
    )
    _runner_require(
        summary.get("workflow_count")
        == len(submissions)
        == contract["expected_workflow_count"],
        "matrix run workflow count changed",
    )
    total_workflow_count = sum(
        row.get("state") == "WORKFLOW_BOUND" for row in journal
    )
    _runner_require(
        summary.get("flowmesh_workflow_count") == total_workflow_count,
        "matrix run FlowMesh workflow count changed",
    )
    if recovered:
        assert failure_document is not None
        _runner_require(
            summary.get("canonical_workflow_count") == len(submissions)
            and summary.get("abandoned_workflow_count")
            == len(recovery_entries)
            and summary.get("infrastructure_failure_count")
            == len(failure_entries)
            and summary.get("infrastructure_recovery_count")
            == len(recovery_entries)
            and summary.get("recovery_ids")
            == [
                row["payload"]["recovery_id"]
                for row in recovery_entries
            ]
            and summary.get(
                "recovery_runner_module_sha256_by_recovery_id"
            )
            == {
                row["payload"]["recovery_id"]: row["payload"][
                    "recovery_runner_module_sha256"
                ]
                for row in recovery_entries
            }
            and summary.get("initial_failure_sha256")
            == failure_document["failure_sha256"],
            "recovered matrix run summary is invalid",
        )
        if replay_adopted:
            replayed_operations = [
                row
                for row in operation_results
                if row.get("executed") is True
                and row.get("idempotent_replay") is True
            ]
            expected_adoption_ids = [
                row["payload"]["adoption_id"] for row in adoption_entries
            ]
            expected_replay_keys = [
                row["operation_key"] for row in replayed_operations
            ]
            _runner_require(
                summary.get("replay_result_adoption_count")
                == len(adoption_entries)
                and summary.get("replay_result_adoption_ids")
                == expected_adoption_ids
                and summary.get(
                    "adoption_runner_module_sha256_by_adoption_id"
                )
                == {
                    row["payload"]["adoption_id"]: row["payload"][
                        "adoption_runner_module_sha256"
                    ]
                    for row in adoption_entries
                }
                and summary.get("adopted_replay_operation_count")
                == len(replayed_operations)
                and summary.get("adopted_replay_operation_keys")
                == expected_replay_keys
                and len(replayed_operations) == len(adoption_entries)
                and summary.get(
                    "failed_flowmesh_task_records_in_canonical_results"
                )
                is False
                and summary.get(
                    "adopted_prior_node_measurements_in_canonical_results"
                )
                is True
                and summary.get(
                    "original_flowmesh_task_ids_known_for_adopted_replays"
                )
                is False
                and summary.get(
                    "original_flowmesh_workflow_ids_known_for_adopted_replays"
                )
                is False
                and summary.get(
                    "measurement_freshness_established_for_adopted_replays"
                )
                is False
                and summary.get("root_dispatch_history_interpretation")
                == "non-historical-diagnostic-only"
                and summary.get(
                    "prior_before_dispatch_interpretation_superseded"
                )
                is True,
                "replay-adopted matrix run summary is invalid",
            )
        else:
            _runner_require(
                summary.get(
                    "failed_infrastructure_attempts_in_canonical_results"
                )
                is False,
                "recovered matrix run summary is invalid",
            )
    _runner_require(
        summary.get("selected_worker") == contract["selected_worker"]
        and summary.get("runtime_epochs") == contract["runtime_epochs"]
        and summary.get("primary_trial_wrapper_max_concurrency") == 1
        and summary.get("global_serial_execution_observed") is True,
        "matrix run worker, runtime, or serial binding changed",
    )
    _runner_require(
        summary.get("queue_time_measured") is False
        and summary.get("semantic_task_quality_evaluated") is False
        and summary.get("llm_called") is False
        and summary.get("credentials_recorded") is False
        and summary.get("eligible_for_scientific_claims") is False,
        "matrix run evidence boundary changed",
    )

    return {
        "status": "VERIFIED",
        "run_id": summary["run_id"],
        "matrix_id": summary["matrix_id"],
        "completed_trial_count": 64,
        "executed_operation_count": len(executed_keys),
        "inactive_operation_count": len(inactive_keys),
        "workflow_count": len(submissions),
        "flowmesh_workflow_count": total_workflow_count,
        "infrastructure_recovery_count": len(recovery_entries),
        "replay_result_adoption_count": len(adoption_entries),
        "adopted_replay_operation_count": sum(
            row.get("executed") is True
            and row.get("idempotent_replay") is True
            for row in operation_results
        ),
        "abandoned_workflow_count": len(recovery_entries),
        "worker_id": contract["selected_worker"]["worker_id"],
        "source_binding_checked": source_binding_checked,
        "eligible_for_scientific_claims": False,
    }
