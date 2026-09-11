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
)
from .preflight import describe_pinned_worker
from .redaction import redact_secrets
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
FLOWMESH_CONTAINER_MATRIX_TRIAL_RESULT_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-matrix-trial-result/v1alpha1"
)
FLOWMESH_CONTAINER_MATRIX_OPERATION_RESULT_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-matrix-operation-result/v1alpha1"
)
FLOWMESH_CONTAINER_MATRIX_SUBMISSION_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-matrix-submission/v1alpha1"
)
FLOWMESH_CONTAINER_MATRIX_RUN_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-matrix-run/v1alpha1"
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
_PHASE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,31}")
_CONDITIONAL_DESIGNS = frozenset({"D3", "D7"})
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
    current_intent: Mapping[str, Any] | None = None
    current_bound: Mapping[str, Any] | None = None
    obtained_payloads: list[Mapping[str, Any]] = []
    seen_workflow_ids: set[str] = set()
    seen_task_ids: set[str] = set()
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
            _runner_require(
                index == len(entries) - 1 and completed_count < 64,
                "matrix journal failure is not terminal",
            )
            _runner_require(
                phase in {"preflight", "unconditional", "A", "B"}
                and set(payload) == {"error"}
                and isinstance(payload.get("error"), str)
                and bool(payload["error"]),
                "matrix journal failure payload is invalid",
            )
            failed = True
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
                    )
            obtained_payloads.append(payload)
            phase_index += 1
            state_index = 0
            current_intent = None
            current_bound = None
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
) -> list[dict[str, Any]]:
    """Recover the sole safe two-file crash window after checkpoint append."""

    _validate_journal(
        entries,
        contract,
        checkpoints,
        sources=sources,
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
        entries, contract, checkpoints, sources=sources
    )
    return entries


def _phase_progress(
    entries: Sequence[Mapping[str, Any]],
    *,
    sequence_index: int,
    phase: str,
) -> Mapping[str, Any] | None:
    matches = [
        row
        for row in entries
        if row.get("sequence_index") == sequence_index
        and row.get("phase") == phase
        and row.get("state")
        in {"SUBMISSION_INTENT", "WORKFLOW_BOUND", "RESULTS_OBTAINED"}
    ]
    return matches[-1] if matches else None


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
        record = _operation_result(
            raw,
            expected[operation_key],
            task_id=task_id,
            selected_worker_id=selected_worker_id,
            task_detail=detail,
            expected_runtime_epochs=expected_runtime_epochs,
        )
        _runner_require(
            type(body.get("started_monotonic_ns")) is int
            and type(body.get("finished_monotonic_ns")) is int,
            "formal matrix result must preserve its monotonic interval",
        )
        record["started_monotonic_ns"] = body["started_monotonic_ns"]
        record["finished_monotonic_ns"] = body["finished_monotonic_ns"]
        record["phase"] = phase
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
    copied.update(
        {
            "schema_version": (
                FLOWMESH_CONTAINER_MATRIX_OPERATION_RESULT_SCHEMA_VERSION
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
    checkpoint: dict[str, Any] = {
        "schema_version": (
            FLOWMESH_CONTAINER_MATRIX_CHECKPOINT_ENTRY_SCHEMA_VERSION
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
    return checkpoint


def _revalidate_executed_operation_result(
    result: Mapping[str, Any],
    operation: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    sequence_index: int,
    expected_phase: str,
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
    )
    revalidated["started_monotonic_ns"] = result["started_monotonic_ns"]
    revalidated["finished_monotonic_ns"] = result["finished_monotonic_ns"]
    revalidated["phase"] = expected_phase
    expected = _executed_operation_result(
        checked_operation,
        revalidated,
        sequence_index=sequence_index,
    )
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
    operation_results = checkpoint["operation_results"]
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
            revalidated_results.append(
                _revalidate_executed_operation_result(
                    result,
                    operation,
                    contract=contract,
                    sequence_index=sequence_index,
                    expected_phase=phase_by_key[key],
                )
            )

    submissions = checkpoint["submissions"]
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
        _runner_require(
            set(row)
            == {
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
            },
            "matrix trial checkpoint fields changed",
        )
        _runner_require(
            row.get("schema_version")
            == FLOWMESH_CONTAINER_MATRIX_CHECKPOINT_ENTRY_SCHEMA_VERSION,
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
        _runner_require(
            isinstance(trial, Mapping)
            and trial.get("schema_version")
            == FLOWMESH_CONTAINER_MATRIX_TRIAL_RESULT_SCHEMA_VERSION
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
        for operation in operations:
            _runner_require(
                operation.get("schema_version")
                == FLOWMESH_CONTAINER_MATRIX_OPERATION_RESULT_SCHEMA_VERSION
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
                    and operation.get("idempotent_replay") is False,
                    "executed matrix operation evidence is invalid",
                )
            else:
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
    _atomic_write(target / _FAILURE_FILE, _json_bytes(failure))


def _finalize(
    target: Path,
    *,
    contract: Mapping[str, Any],
    checkpoints: Sequence[Mapping[str, Any]],
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
    summary: dict[str, Any] = {
        "schema_version": FLOWMESH_CONTAINER_MATRIX_RUN_SCHEMA_VERSION,
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
        "flowmesh_workflow_count": len(submissions),
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
    target = Path(output_dir).resolve()
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
        _runner_require(
            not (target / _FAILURE_FILE).exists(),
            "matrix run has a durable failure; start a new output directory",
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
    journal_entries = _reconcile_checkpoint_completion(
        journal_path,
        journal_entries,
        checkpoints,
        contract,
        sources=sources,
    )
    _runner_require(
        not any(row["state"] == "RUN_FAILED" for row in journal_entries),
        "matrix journal contains a durable failure",
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
        require_complete=True,
    )
    summary = _finalize(
        target,
        contract=contract,
        checkpoints=checkpoints,
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
        )


def _verify_checksum_file(root: Path) -> None:
    checksum_path = root / "SHA256SUMS"
    _runner_require(checksum_path.is_file(), "matrix run SHA256SUMS is missing")
    expected = _FINAL_FILES
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
    _runner_require(
        actual == _FINAL_FILES | {"SHA256SUMS"},
        "completed matrix run file set is invalid",
    )
    _verify_checksum_file(root)
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
    _validate_journal(
        journal,
        contract,
        checkpoints,
        sources=sources,
        require_complete=True,
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
        == FLOWMESH_CONTAINER_MATRIX_RUN_SCHEMA_VERSION
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
    _runner_require(
        summary.get("flowmesh_workflow_count") == len(submissions),
        "matrix run FlowMesh workflow count changed",
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
        "flowmesh_workflow_count": len(submissions),
        "worker_id": contract["selected_worker"]["worker_id"],
        "source_binding_checked": source_binding_checked,
        "eligible_for_scientific_claims": False,
    }
