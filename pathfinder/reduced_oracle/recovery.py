from __future__ import annotations

import json
import os
import platform
import shutil
import time
import uuid
from collections import Counter, defaultdict
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable, Iterable

from ..integrations.flowmesh.contracts import (
    FlowMeshAgentRunRequest,
)
from ..integrations.flowmesh.pilot import (
    AgentAdapterProtocol,
    FlowMeshPilotConfig,
    PilotTrial,
    _append_jsonl,
    _exclusive_pilot_lock,
    _offered_quotes,
    _record_for_failure,
    _record_for_success,
    _trial_plan_payload,
    _utc_now,
    _write_csv,
    _write_json,
    build_trial_plan,
    load_flowmesh_pilot_config,
    load_pilot_records,
    summarize_paired_contrasts,
    summarize_pilot_records,
    summarize_pilot_records_by_workload,
    validate_flowmesh_pilot_config,
)
from ..models import SystemConfig
from ..synthetic_marker import assert_not_synthetic_fixture
from .contracts import ReducedOracleConfig, ReducedOracleConfigError
from .objective import analyze_reduced_oracle
from .runner import (
    ORACLE_PLAN_SCHEMA_VERSION,
    ORACLE_RUN_SCHEMA_VERSION,
    _design_pilot,
    _frozen_plan,
    _oracle_pilot,
)
from .transition import FilesystemTransitionExecutor


RECOVERY_PLAN_SCHEMA_VERSION = (
    "pathfinder.reduced-oracle-recovery-plan/v1alpha1"
)
RECOVERY_ATTEMPT_SCHEMA_VERSION = (
    "pathfinder.reduced-oracle-recovery-attempt/v1alpha1"
)
RECOVERY_RUN_SCHEMA_VERSION = (
    "pathfinder.reduced-oracle-recovery-run/v1alpha1"
)
CANONICAL_PROVENANCE_SCHEMA_VERSION = (
    "pathfinder.reduced-oracle-canonical-provenance/v1alpha1"
)

_LOCK_FILENAMES = {".oracle.lock", ".pilot.lock", ".recovery.lock"}
_AUTOMATICALLY_RETRYABLE_OUTCOMES = {"infrastructure_failure"}


class ReducedOracleRecoveryError(ReducedOracleConfigError):
    """Raised when an audited recovery cannot proceed safely."""


class RecoveryCircuitOpenError(ReducedOracleRecoveryError):
    """Raised after repeated infrastructure failures stop submissions."""


def _sha256_bytes(payload: bytes) -> str:
    return sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _record_sha256(record: dict[str, Any]) -> str:
    encoded = json.dumps(
        record,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return _sha256_bytes(encoded)


def _read_json(path: Path, description: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ReducedOracleRecoveryError(
            f"{description} does not exist: {path}"
        ) from exc
    except json.JSONDecodeError as exc:
        raise ReducedOracleRecoveryError(
            f"{description} is invalid JSON: {path}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise ReducedOracleRecoveryError(
            f"{description} must be a JSON object: {path}"
        )
    return payload


def _write_jsonl_atomic(
    records: Iterable[dict[str, Any]],
    path: Path,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            for record in records:
                handle.write(
                    json.dumps(record, ensure_ascii=False, sort_keys=True)
                )
                handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _require_separate_directories(
    incident_dir: Path,
    recovery_dir: Path,
) -> None:
    if (
        incident_dir == recovery_dir
        or recovery_dir.is_relative_to(incident_dir)
        or incident_dir.is_relative_to(recovery_dir)
    ):
        raise ReducedOracleRecoveryError(
            "incident and recovery directories must be separate, non-nested "
            "paths so recovery cannot modify its evidence source"
        )


def _incident_evidence(incident_dir: Path) -> dict[str, str]:
    evidence: dict[str, str] = {}
    for path in sorted(incident_dir.rglob("*")):
        if not path.is_file() or path.name in _LOCK_FILENAMES:
            continue
        relative = path.relative_to(incident_dir).as_posix()
        evidence[relative] = _sha256_file(path)
    if not evidence:
        raise ReducedOracleRecoveryError(
            f"incident directory contains no auditable files: {incident_dir}"
        )
    return evidence


def _snapshot_sha256(evidence: dict[str, str]) -> str:
    encoded = json.dumps(
        evidence,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return _sha256_bytes(encoded)


def _verify_incident_evidence(
    incident_dir: Path,
    expected: dict[str, Any],
) -> None:
    if not all(
        isinstance(path, str) and isinstance(digest, str)
        for path, digest in expected.items()
    ):
        raise ReducedOracleRecoveryError(
            "recovery plan incident_evidence_sha256 is invalid"
        )
    actual = _incident_evidence(incident_dir)
    if actual == expected:
        return
    missing = sorted(set(expected) - set(actual))
    added = sorted(set(actual) - set(expected))
    changed = sorted(
        path
        for path in set(actual) & set(expected)
        if actual[path] != expected[path]
    )
    details = []
    if missing:
        details.append("missing=" + ",".join(missing))
    if added:
        details.append("added=" + ",".join(added))
    if changed:
        details.append("changed=" + ",".join(changed))
    raise ReducedOracleRecoveryError(
        "incident evidence changed after the recovery plan was frozen; "
        "refusing to continue (" + "; ".join(details) + ")"
    )


def _trial_from_dict(payload: dict[str, Any]) -> PilotTrial:
    values = dict(payload)
    accepted = values.get("accepted_answer_substrings", [])
    if not isinstance(accepted, list) or not all(
        isinstance(value, str) for value in accepted
    ):
        raise ReducedOracleRecoveryError(
            "recovery-plan trial accepted_answer_substrings is invalid"
        )
    values["accepted_answer_substrings"] = tuple(accepted)
    try:
        return PilotTrial(**values)
    except TypeError as exc:
        raise ReducedOracleRecoveryError(
            f"recovery-plan trial is invalid: {exc}"
        ) from exc


def _valid_completed_record(record: dict[str, Any]) -> bool:
    return (
        record.get("outcome_type") == "completed"
        and record.get("telemetry_complete") is True
        and record.get("artifact_delivery_complete") is True
        and isinstance(record.get("task_success"), bool)
    )


def _load_and_validate_incident(
    *,
    config: ReducedOracleConfig,
    system: SystemConfig,
    incident_dir: Path,
) -> tuple[
    FlowMeshPilotConfig,
    dict[str, dict[str, dict[str, Any]]],
    list[dict[str, Any]],
    dict[str, Any],
]:
    workload_source = load_flowmesh_pilot_config(
        config.workload_pilot_config_path
    )
    pilot = _oracle_pilot(config, workload_source)
    validate_flowmesh_pilot_config(pilot, system)
    if system.source_path != workload_source.system_config_path:
        raise ReducedOracleRecoveryError(
            "system does not match workload_pilot_config.system_config"
        )

    expected_plan = _frozen_plan(config, system, pilot)
    actual_plan = _read_json(
        incident_dir / "oracle_plan.json",
        "incident oracle plan",
    )
    if actual_plan.get("schema_version") != ORACLE_PLAN_SCHEMA_VERSION:
        raise ReducedOracleRecoveryError(
            "incident oracle plan has an unsupported schema_version"
        )
    if actual_plan != expected_plan:
        raise ReducedOracleRecoveryError(
            "incident oracle plan does not match the supplied Oracle and "
            "system configurations"
        )

    records_by_design: dict[str, dict[str, dict[str, Any]]] = {}
    recovery_items: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    disposition_counts: Counter[str] = Counter()

    for design_id in config.design_ids:
        design_pilot = _design_pilot(pilot, design_id)
        trials = build_trial_plan(design_pilot)
        expected_by_key = {trial.trial_key: trial for trial in trials}
        design_dir = incident_dir / "designs" / design_id
        trial_plan_path = design_dir / "trial_plan.json"
        if trial_plan_path.exists():
            expected_trial_plan = _trial_plan_payload(
                design_pilot,
                system,
                trials,
                repetitions=design_pilot.repetitions,
                randomization_seed=design_pilot.randomization_seed,
            )
            actual_trial_plan = _read_json(
                trial_plan_path,
                f"incident trial plan for {design_id}",
            )
            if actual_trial_plan != expected_trial_plan:
                raise ReducedOracleRecoveryError(
                    f"incident trial plan for {design_id} does not match "
                    "the frozen Oracle plan"
                )
        records = load_pilot_records(
            design_dir / "runs.jsonl",
            repair_truncated_tail=False,
        )
        by_key = {str(record["trial_key"]): record for record in records}
        unexpected = sorted(set(by_key) - set(expected_by_key))
        if unexpected:
            raise ReducedOracleRecoveryError(
                f"incident {design_id} contains trials outside the frozen "
                "plan: " + ", ".join(unexpected)
            )
        records_by_design[design_id] = by_key

        for trial in trials:
            record = by_key.get(trial.trial_key)
            if record is None:
                disposition = "missing"
                recovery_items.append(
                    {
                        "design_id": design_id,
                        "trial_key": trial.trial_key,
                        "disposition": disposition,
                        "incident_record_sha256": None,
                        "incident_outcome_type": None,
                        "trial": trial.to_public_dict(),
                    }
                )
            elif _valid_completed_record(record):
                disposition = "preserve_completed"
            elif record.get("outcome_type") in (
                _AUTOMATICALLY_RETRYABLE_OUTCOMES
            ):
                disposition = "retry_infrastructure_failure"
                recovery_items.append(
                    {
                        "design_id": design_id,
                        "trial_key": trial.trial_key,
                        "disposition": disposition,
                        "incident_record_sha256": _record_sha256(record),
                        "incident_outcome_type": record.get("outcome_type"),
                        "trial": trial.to_public_dict(),
                    }
                )
            else:
                disposition = "blocked_non_retryable_record"
                blocked.append(
                    {
                        "design_id": design_id,
                        "trial_key": trial.trial_key,
                        "outcome_type": record.get("outcome_type"),
                        "telemetry_complete": record.get(
                            "telemetry_complete"
                        ),
                        "artifact_delivery_complete": record.get(
                            "artifact_delivery_complete"
                        ),
                        "task_success_type": type(
                            record.get("task_success")
                        ).__name__,
                        "record_sha256": _record_sha256(record),
                    }
                )
            disposition_counts[disposition] += 1

    audit = {
        "expected_trial_count": sum(disposition_counts.values()),
        "recoverable_trial_count": len(recovery_items),
        "blocked_trial_count": len(blocked),
        "disposition_counts": dict(sorted(disposition_counts.items())),
        "blocked_trials": blocked,
    }
    return pilot, records_by_design, recovery_items, audit


def plan_reduced_oracle_recovery(
    *,
    config: ReducedOracleConfig,
    system: SystemConfig,
    incident_dir: str | Path,
    recovery_dir: str | Path,
) -> dict[str, Any]:
    """Freeze a read-only incident snapshot and the exact retry set."""
    incident = Path(incident_dir).resolve()
    recovery = Path(recovery_dir).resolve()
    _require_separate_directories(incident, recovery)
    assert_not_synthetic_fixture(
        incident,
        command="plan-reduced-oracle-recovery",
    )
    plan_path = recovery / "recovery_plan.json"
    if plan_path.exists():
        plan = _read_json(plan_path, "recovery plan")
        _validate_recovery_plan(
            plan,
            config=config,
            system=system,
            incident_dir=incident,
        )
        _, _, recovery_items, audit = _load_and_validate_incident(
            config=config,
            system=system,
            incident_dir=incident,
        )
        _validate_recovery_items(plan, recovery_items, audit)
        return plan

    _, _, recovery_items, audit = _load_and_validate_incident(
        config=config,
        system=system,
        incident_dir=incident,
    )
    if audit["blocked_trial_count"]:
        blocked = audit["blocked_trials"]
        preview = ", ".join(
            f"{item['design_id']}:{item['trial_key']}="
            f"{item['outcome_type']}"
            for item in blocked[:5]
        )
        raise ReducedOracleRecoveryError(
            "incident contains non-retryable or research-integrity records; "
            "automatic recovery is forbidden: " + preview
        )

    evidence = _incident_evidence(incident)
    snapshot = _snapshot_sha256(evidence)
    recovery_id = f"{config.oracle_id}-recovery-{snapshot[:12]}"
    plan = {
        "schema_version": RECOVERY_PLAN_SCHEMA_VERSION,
        "recovery_id": recovery_id,
        "created_at": _utc_now(),
        "oracle_id": config.oracle_id,
        "oracle_config_path": str(config.source_path),
        "oracle_config_sha256": config.source_sha256,
        "system_config_path": str(system.source_path),
        "system_config_sha256": _sha256_file(system.source_path),
        "incident_dir": str(incident),
        "incident_snapshot_sha256": snapshot,
        "incident_evidence_sha256": evidence,
        "automatic_retryable_outcomes": sorted(
            _AUTOMATICALLY_RETRYABLE_OUTCOMES
        ),
        "missing_trials_are_retryable": True,
        "session_identity_policy": (
            "uuid5(recovery_id,design_id,canonical_trial_key,attempt_number)"
        ),
        "raw_incident_mutation_allowed": False,
        "expected_trial_count": audit["expected_trial_count"],
        "preserved_completed_trial_count": audit[
            "disposition_counts"
        ].get("preserve_completed", 0),
        "recoverable_trial_count": len(recovery_items),
        "disposition_counts": audit["disposition_counts"],
        "recovery_items": [
            {"recovery_order_index": index, **item}
            for index, item in enumerate(recovery_items)
        ],
        "canonical_output_dir": str(recovery / "canonical-oracle"),
        "secrets_recorded": False,
    }
    recovery.mkdir(parents=True, exist_ok=True)
    _write_json(plan, plan_path)
    return plan


def _validate_recovery_plan(
    plan: dict[str, Any],
    *,
    config: ReducedOracleConfig,
    system: SystemConfig,
    incident_dir: Path,
) -> None:
    if plan.get("schema_version") != RECOVERY_PLAN_SCHEMA_VERSION:
        raise ReducedOracleRecoveryError(
            "recovery plan has an unsupported schema_version"
        )
    if plan.get("oracle_id") != config.oracle_id or plan.get(
        "oracle_config_sha256"
    ) != config.source_sha256:
        raise ReducedOracleRecoveryError(
            "recovery plan does not match the supplied Oracle configuration"
        )
    if plan.get("system_config_sha256") != _sha256_file(system.source_path):
        raise ReducedOracleRecoveryError(
            "recovery plan does not match the supplied system configuration"
        )
    if Path(str(plan.get("incident_dir"))).resolve() != incident_dir:
        raise ReducedOracleRecoveryError(
            "recovery plan refers to a different incident directory"
        )
    evidence = plan.get("incident_evidence_sha256")
    if not isinstance(evidence, dict):
        raise ReducedOracleRecoveryError(
            "recovery plan has no incident evidence map"
        )
    _verify_incident_evidence(incident_dir, evidence)
    if _snapshot_sha256(evidence) != plan.get("incident_snapshot_sha256"):
        raise ReducedOracleRecoveryError(
            "recovery plan incident snapshot digest is inconsistent"
        )


def _validate_recovery_items(
    plan: dict[str, Any],
    recovery_items: list[dict[str, Any]],
    audit: dict[str, Any],
) -> None:
    expected = [
        {"recovery_order_index": index, **item}
        for index, item in enumerate(recovery_items)
    ]
    if plan.get("recovery_items") != expected:
        raise ReducedOracleRecoveryError(
            "recovery plan retry set does not match the audited incident"
        )
    if plan.get("expected_trial_count") != audit["expected_trial_count"]:
        raise ReducedOracleRecoveryError(
            "recovery plan expected_trial_count is inconsistent"
        )
    if plan.get("recoverable_trial_count") != len(recovery_items):
        raise ReducedOracleRecoveryError(
            "recovery plan recoverable_trial_count is inconsistent"
        )


def _attempt_payload_sha256(record: dict[str, Any]) -> str:
    payload = dict(record)
    payload.pop("attempt_payload_sha256", None)
    payload.pop("attempt_chain_sha256", None)
    return _record_sha256(payload)


def _seal_attempt(
    record: dict[str, Any],
    *,
    previous_chain_sha256: str,
) -> dict[str, Any]:
    sealed = dict(record)
    sealed["previous_attempt_chain_sha256"] = previous_chain_sha256
    payload_sha256 = _attempt_payload_sha256(sealed)
    sealed["attempt_payload_sha256"] = payload_sha256
    sealed["attempt_chain_sha256"] = _sha256_bytes(
        f"{previous_chain_sha256}:{payload_sha256}".encode("utf-8")
    )
    return sealed


def _load_attempts(
    path: Path,
    *,
    chain_seed_sha256: str,
) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    attempts: list[dict[str, Any]] = []
    seen_attempt_keys: set[str] = set()
    previous_chain_sha256 = chain_seed_sha256
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ReducedOracleRecoveryError(
                f"invalid recovery attempt JSONL at {path}:{line_number}: "
                f"{exc}"
            ) from exc
        if not isinstance(record, dict):
            raise ReducedOracleRecoveryError(
                f"recovery attempt at {path}:{line_number} is not an object"
            )
        attempt_key = record.get("recovery_attempt_key")
        if not isinstance(attempt_key, str) or not attempt_key:
            raise ReducedOracleRecoveryError(
                f"recovery attempt at {path}:{line_number} has no attempt key"
            )
        if attempt_key in seen_attempt_keys:
            raise ReducedOracleRecoveryError(
                f"duplicate recovery attempt key: {attempt_key}"
            )
        seen_attempt_keys.add(attempt_key)
        if record.get("previous_attempt_chain_sha256") != previous_chain_sha256:
            raise ReducedOracleRecoveryError(
                f"recovery attempt chain is broken at {path}:{line_number}"
            )
        payload_sha256 = _attempt_payload_sha256(record)
        if record.get("attempt_payload_sha256") != payload_sha256:
            raise ReducedOracleRecoveryError(
                f"recovery attempt payload digest is invalid at "
                f"{path}:{line_number}"
            )
        expected_chain = _sha256_bytes(
            f"{previous_chain_sha256}:{payload_sha256}".encode("utf-8")
        )
        if record.get("attempt_chain_sha256") != expected_chain:
            raise ReducedOracleRecoveryError(
                f"recovery attempt chain digest is invalid at "
                f"{path}:{line_number}"
            )
        previous_chain_sha256 = expected_chain
        attempts.append(record)
    return attempts


def _completed_attempts_by_trial(
    attempts: Iterable[dict[str, Any]],
) -> dict[tuple[str, str], dict[str, Any]]:
    completed: dict[tuple[str, str], dict[str, Any]] = {}
    for attempt in attempts:
        if not _valid_completed_record(attempt):
            continue
        key = (
            str(attempt.get("design_id")),
            str(attempt.get("canonical_trial_key")),
        )
        completed[key] = attempt
    return completed


def _attempts_by_trial(
    attempts: Iterable[dict[str, Any]],
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for attempt in attempts:
        grouped[
            (
                str(attempt.get("design_id")),
                str(attempt.get("canonical_trial_key")),
            )
        ].append(attempt)
    return grouped


def _retry_trial(
    *,
    original: PilotTrial,
    recovery_id: str,
    attempt_number: int,
    recovery_order_index: int,
) -> tuple[PilotTrial, str]:
    identity = (
        f"{recovery_id}:{original.design_id}:{original.trial_key}:"
        f"attempt-{attempt_number}"
    )
    identity_hash = sha256(identity.encode("utf-8")).hexdigest()
    attempt_key = f"{original.trial_key}#recovery-a{attempt_number:04d}"
    return (
        replace(
            original,
            trial_key=attempt_key,
            trial_id=(
                f"{recovery_id}-{identity_hash[:12]}-"
                f"a{attempt_number:04d}"
            ),
            session_id=str(uuid.uuid5(uuid.NAMESPACE_URL, identity)),
            order_index=recovery_order_index,
        ),
        attempt_key,
    )


def _execute_attempt(
    *,
    pilot: FlowMeshPilotConfig,
    system: SystemConfig,
    adapter: AgentAdapterProtocol,
    trial: PilotTrial,
) -> dict[str, Any]:
    started_at = _utc_now()
    monotonic_start = time.monotonic()
    offered_quotes = _offered_quotes(system, trial)
    recovered_from_gateway_state = False
    try:
        request = FlowMeshAgentRunRequest(
            question=trial.question,
            design_id=trial.design_id,
            task_class_id=trial.task_class_id,
            quote_profile_id=trial.quote_profile_id,
            latency_multiplier=trial.latency_multiplier,
            seed=trial.seed,
            trial_id=trial.trial_id,
            session_id=trial.session_id,
            object_id=trial.object_id,
        )
        recover = getattr(adapter, "recover", None)
        result = recover(trial.session_id) if callable(recover) else None
        recovered_from_gateway_state = result is not None
        if result is None:
            result = adapter.run(request)
    except Exception as exc:
        return _record_for_failure(
            pilot,
            trial,
            exc,
            started_at=started_at,
            finished_at=_utc_now(),
            duration_seconds=time.monotonic() - monotonic_start,
            offered_quotes=offered_quotes,
        )
    return _record_for_success(
        pilot,
        trial,
        result,
        started_at=started_at,
        finished_at=_utc_now(),
        duration_seconds=time.monotonic() - monotonic_start,
        offered_quotes=offered_quotes,
        recovered_from_gateway_state=recovered_from_gateway_state,
    )


def _initialize_recovery_runtime(
    *,
    incident_dir: Path,
    recovery_dir: Path,
) -> None:
    target = recovery_dir / "runtime"
    target.mkdir(parents=True, exist_ok=True)
    for name in ("transition_state.json", "active_plan.json"):
        destination = target / name
        source = incident_dir / "runtime" / name
        if destination.exists() or not source.exists():
            continue
        temporary = destination.with_name(
            f".{destination.name}.{uuid.uuid4().hex}.tmp"
        )
        try:
            shutil.copy2(source, temporary)
            if _sha256_file(temporary) != _sha256_file(source):
                raise ReducedOracleRecoveryError(
                    f"failed to copy incident transition state: {source}"
                )
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)


def _write_recovery_manifest(
    *,
    recovery_dir: Path,
    plan: dict[str, Any],
    attempts: list[dict[str, Any]],
    status: str,
    run_id: str,
    safe_design_restored: bool,
    max_consecutive_infrastructure_failures: int,
    max_attempts_per_trial: int,
    detail: str | None = None,
    canonical_output_dir: Path | None = None,
) -> dict[str, Any]:
    counts = Counter(str(row.get("outcome_type")) for row in attempts)
    completed = _completed_attempts_by_trial(attempts)
    manifest = {
        "schema_version": RECOVERY_RUN_SCHEMA_VERSION,
        "recovery_id": plan["recovery_id"],
        "run_id": run_id,
        "status": status,
        "updated_at": _utc_now(),
        "incident_dir": plan["incident_dir"],
        "incident_snapshot_sha256": plan["incident_snapshot_sha256"],
        "recovery_plan_path": str(recovery_dir / "recovery_plan.json"),
        "attempt_records_path": str(recovery_dir / "attempts.jsonl"),
        "recoverable_trial_count": plan["recoverable_trial_count"],
        "recovered_trial_count": len(completed),
        "remaining_recovery_trial_count": (
            int(plan["recoverable_trial_count"]) - len(completed)
        ),
        "attempt_count": len(attempts),
        "attempt_outcome_counts": dict(sorted(counts.items())),
        "latest_attempt_chain_sha256": (
            attempts[-1].get("attempt_chain_sha256") if attempts else None
        ),
        "max_consecutive_infrastructure_failures": (
            max_consecutive_infrastructure_failures
        ),
        "max_attempts_per_trial": max_attempts_per_trial,
        "safe_design_restored": safe_design_restored,
        "canonical_output_dir": (
            str(canonical_output_dir)
            if canonical_output_dir is not None
            else None
        ),
        "detail": detail,
        "python_version": platform.python_version(),
        "worker_id": None,
        "worker_alias": None,
        "worker_lifecycle_managed": False,
        "service_lifecycle_managed": False,
        "secrets_recorded": False,
    }
    _write_json(manifest, recovery_dir / "recovery_manifest.json")
    return manifest


def _select_canonical_transitions(
    *,
    config: ReducedOracleConfig,
    incident_dir: Path,
    recovery_dir: Path,
) -> list[dict[str, Any]]:
    def load(path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        rows = []
        for line_number, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(),
            start=1,
        ):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ReducedOracleRecoveryError(
                    f"invalid transition JSONL at {path}:{line_number}: {exc}"
                ) from exc
            if not isinstance(row, dict):
                raise ReducedOracleRecoveryError(
                    f"transition at {path}:{line_number} is not an object"
                )
            rows.append(row)
        return rows

    incident = load(incident_dir / "transitions.jsonl")
    recovery = load(recovery_dir / "recovery_transitions.jsonl")
    available = incident + recovery
    selected: list[dict[str, Any]] = []
    initial_safe = next(
        (
            row
            for row in available
            if row.get("from_design_id") is None
            and row.get("to_design_id") == config.safe_design_id
        ),
        None,
    )
    if initial_safe is not None:
        selected.append(initial_safe)
    for design_id in config.design_ids:
        if design_id == config.safe_design_id:
            continue
        forward = next(
            (
                row
                for row in available
                if row.get("from_design_id") == config.safe_design_id
                and row.get("to_design_id") == design_id
                and row.get("transition_type") == "forward"
            ),
            None,
        )
        restoration = next(
            (
                row
                for row in available
                if row.get("from_design_id") == design_id
                and row.get("to_design_id") == config.safe_design_id
                and row.get("transition_type") == "restore"
            ),
            None,
        )
        if forward is None or restoration is None:
            raise ReducedOracleRecoveryError(
                f"cannot build canonical transition evidence for {design_id}; "
                "one forward and one restoration observation are required"
            )
        selected.extend((forward, restoration))
    return selected


def _canonical_record_from_attempt(
    attempt: dict[str, Any],
    original: PilotTrial,
) -> dict[str, Any]:
    record = dict(attempt)
    attempt_record_sha256 = _record_sha256(attempt)
    attempt_chain_sha256 = str(attempt["attempt_chain_sha256"])
    for field in (
        "attempt_payload_sha256",
        "previous_attempt_chain_sha256",
        "attempt_chain_sha256",
    ):
        record.pop(field, None)
    actual_trial_key = str(record["trial_key"])
    actual_trial_id = str(record["trial_id"])
    actual_session_id = str(record["session_id"])
    record.update(original.to_public_dict())
    record["trial_key"] = original.trial_key
    record["trial_id"] = actual_trial_id
    record["session_id"] = actual_session_id
    record["canonical_source"] = "recovery_attempt"
    record["recovery_provenance"] = {
        "recovery_id": attempt["recovery_id"],
        "attempt_number": attempt["attempt_number"],
        "recovery_attempt_key": attempt["recovery_attempt_key"],
        "attempt_trial_key": actual_trial_key,
        "planned_trial_id": original.trial_id,
        "planned_session_id": original.session_id,
        "incident_disposition": attempt["incident_disposition"],
        "incident_record_sha256": attempt.get(
            "incident_record_sha256"
        ),
        "attempt_record_sha256": attempt_record_sha256,
        "attempt_chain_sha256": attempt_chain_sha256,
    }
    return record


def _canonical_record_from_incident(
    record: dict[str, Any],
) -> dict[str, Any]:
    canonical = dict(record)
    canonical["canonical_source"] = "incident_completed"
    canonical["incident_record_sha256"] = _record_sha256(record)
    return canonical


def _replace_path_prefix(
    value: Any,
    *,
    old: str,
    new: str,
) -> Any:
    if isinstance(value, dict):
        return {
            key: _replace_path_prefix(item, old=old, new=new)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [
            _replace_path_prefix(item, old=old, new=new) for item in value
        ]
    if isinstance(value, str) and value.startswith(old):
        return new + value[len(old) :]
    return value


def _verify_existing_canonical_output(
    canonical: Path,
    plan: dict[str, Any],
) -> None:
    manifest = _read_json(
        canonical / "oracle_manifest.json",
        "canonical Oracle manifest",
    )
    provenance = _read_json(
        canonical / "canonical_provenance.json",
        "canonical Oracle provenance",
    )
    if manifest.get("status") != "COMPLETE":
        raise ReducedOracleRecoveryError(
            "canonical output exists without a COMPLETE manifest; use a new "
            "recovery directory rather than overwriting evidence"
        )
    if (
        manifest.get("recovery_id") != plan["recovery_id"]
        or provenance.get("recovery_id") != plan["recovery_id"]
        or provenance.get("incident_snapshot_sha256")
        != plan["incident_snapshot_sha256"]
    ):
        raise ReducedOracleRecoveryError(
            "canonical output provenance does not match the recovery plan"
        )
    expected_hashes = provenance.get("core_file_sha256")
    if not isinstance(expected_hashes, dict):
        raise ReducedOracleRecoveryError(
            "canonical provenance has no core-file digest map"
        )
    for relative, expected in expected_hashes.items():
        if not isinstance(relative, str) or not isinstance(expected, str):
            raise ReducedOracleRecoveryError(
                "canonical core-file digest map is invalid"
            )
        path = (canonical / relative).resolve()
        try:
            path.relative_to(canonical)
        except ValueError as exc:
            raise ReducedOracleRecoveryError(
                "canonical provenance contains a path outside its root"
            ) from exc
        if not path.is_file() or _sha256_file(path) != expected:
            raise ReducedOracleRecoveryError(
                f"canonical core file is missing or changed: {relative}"
            )


def _finalize_canonical_oracle(
    *,
    config: ReducedOracleConfig,
    system: SystemConfig,
    pilot: FlowMeshPilotConfig,
    incident_dir: Path,
    recovery_dir: Path,
    plan: dict[str, Any],
    incident_records: dict[str, dict[str, dict[str, Any]]],
    attempts: list[dict[str, Any]],
) -> Path:
    canonical = Path(str(plan["canonical_output_dir"])).resolve()
    if canonical.exists():
        _verify_existing_canonical_output(canonical, plan)
        return canonical

    completed_attempts = _completed_attempts_by_trial(attempts)
    temporary = canonical.with_name(
        f".{canonical.name}.{uuid.uuid4().hex}.tmp"
    )
    temporary.mkdir(parents=True, exist_ok=False)
    try:
        shutil.copy2(
            incident_dir / "oracle_plan.json",
            temporary / "oracle_plan.json",
        )
        transitions = _select_canonical_transitions(
            config=config,
            incident_dir=incident_dir,
            recovery_dir=recovery_dir,
        )
        _write_jsonl_atomic(transitions, temporary / "transitions.jsonl")

        runtime_source = recovery_dir / "runtime"
        runtime_target = temporary / "runtime"
        runtime_target.mkdir(parents=True, exist_ok=True)
        for name in ("active_plan.json", "transition_state.json"):
            source = runtime_source / name
            if source.exists():
                shutil.copy2(source, runtime_target / name)

        provenance_records: list[dict[str, Any]] = []
        for design_id in config.design_ids:
            design_pilot = _design_pilot(pilot, design_id)
            trials = build_trial_plan(design_pilot)
            canonical_records: list[dict[str, Any]] = []
            recovered_count = 0
            for trial in trials:
                incident_record = incident_records[design_id].get(
                    trial.trial_key
                )
                if (
                    incident_record is not None
                    and _valid_completed_record(incident_record)
                ):
                    record = _canonical_record_from_incident(
                        incident_record
                    )
                else:
                    attempt = completed_attempts.get(
                        (design_id, trial.trial_key)
                    )
                    if attempt is None:
                        raise ReducedOracleRecoveryError(
                            f"cannot finalize: no valid result for "
                            f"{design_id}:{trial.trial_key}"
                        )
                    record = _canonical_record_from_attempt(attempt, trial)
                    recovered_count += 1
                if not _valid_completed_record(record):
                    raise ReducedOracleRecoveryError(
                        f"canonical record is incomplete: "
                        f"{design_id}:{trial.trial_key}"
                    )
                canonical_records.append(record)
                provenance_records.append(
                    {
                        "design_id": design_id,
                        "trial_key": trial.trial_key,
                        "source": record["canonical_source"],
                        "record_sha256": _record_sha256(record),
                    }
                )

            design_dir = temporary / "designs" / design_id
            _write_jsonl_atomic(canonical_records, design_dir / "runs.jsonl")
            trial_plan = _trial_plan_payload(
                design_pilot,
                system,
                trials,
                repetitions=design_pilot.repetitions,
                randomization_seed=design_pilot.randomization_seed,
            )
            _write_json(trial_plan, design_dir / "trial_plan.json")
            _write_csv(
                summarize_pilot_records(canonical_records, trials),
                design_dir / "summary.csv",
            )
            _write_csv(
                summarize_pilot_records_by_workload(
                    canonical_records,
                    trials,
                ),
                design_dir / "summary_by_workload.csv",
            )
            _write_csv(
                summarize_paired_contrasts(canonical_records, trials),
                design_dir / "paired_contrasts.csv",
            )
            _write_json(
                {
                    "schema_version": design_pilot.schema_version,
                    "experiment_id": design_pilot.experiment_id,
                    "status": "COMPLETE",
                    "planned_trials": len(trials),
                    "recorded_trials": len(canonical_records),
                    "remaining_trials": 0,
                    "outcome_counts": {"completed": len(trials)},
                    "recovered_trials": recovered_count,
                    "incident_completed_trials": (
                        len(trials) - recovered_count
                    ),
                    "trial_plan_path": str(
                        canonical / "designs" / design_id / "trial_plan.json"
                    ),
                    "records_path": str(
                        canonical / "designs" / design_id / "runs.jsonl"
                    ),
                    "secrets_recorded": False,
                },
                design_dir / "manifest.json",
            )

        analysis = analyze_reduced_oracle(config, output_dir=temporary)
        if (
            analysis["eligible_design_count"] != len(config.design_ids)
            or analysis["safe_design_restored"] is not True
        ):
            raise ReducedOracleRecoveryError(
                "canonical Oracle failed its completeness or safe-design gate"
            )
        summary_path = temporary / "oracle_summary.json"
        summary = _read_json(summary_path, "canonical Oracle summary")
        rewritten = _replace_path_prefix(
            summary,
            old=str(temporary),
            new=str(canonical),
        )
        _write_json(rewritten, summary_path)

        core_files = [
            temporary / "oracle_plan.json",
            temporary / "transitions.jsonl",
            temporary / "oracle_table.csv",
            temporary / "oracle_summary.json",
            temporary / "lock_in_trace.json",
        ] + [
            temporary / "designs" / design_id / name
            for design_id in config.design_ids
            for name in (
                "trial_plan.json",
                "runs.jsonl",
                "manifest.json",
                "summary.csv",
                "summary_by_workload.csv",
                "paired_contrasts.csv",
            )
        ]
        file_hashes = {
            path.relative_to(temporary).as_posix(): _sha256_file(path)
            for path in core_files
        }
        _write_json(
            {
                "schema_version": CANONICAL_PROVENANCE_SCHEMA_VERSION,
                "recovery_id": plan["recovery_id"],
                "incident_snapshot_sha256": plan[
                    "incident_snapshot_sha256"
                ],
                "expected_trial_count": plan["expected_trial_count"],
                "record_count": len(provenance_records),
                "record_sources": dict(
                    sorted(
                        Counter(
                            row["source"] for row in provenance_records
                        ).items()
                    )
                ),
                "records": provenance_records,
                "core_file_sha256": file_hashes,
                "transition_policy": (
                    "one first-observed forward and restoration pair per "
                    "non-safe design; recovery observations are used only "
                    "when incident observations are absent"
                ),
                "secrets_recorded": False,
            },
            temporary / "canonical_provenance.json",
        )
        _write_json(
            {
                "schema_version": ORACLE_RUN_SCHEMA_VERSION,
                "oracle_id": config.oracle_id,
                "status": "COMPLETE",
                "safe_design_id": config.safe_design_id,
                "safe_design_restored": True,
                "design_count": len(config.design_ids),
                "expected_trial_count": plan["expected_trial_count"],
                "canonical_record_count": len(provenance_records),
                "recovery_id": plan["recovery_id"],
                "incident_snapshot_sha256": plan[
                    "incident_snapshot_sha256"
                ],
                "oracle_plan_path": str(canonical / "oracle_plan.json"),
                "transition_records_path": str(
                    canonical / "transitions.jsonl"
                ),
                "oracle_summary_path": str(
                    canonical / "oracle_summary.json"
                ),
                "canonical_provenance_path": str(
                    canonical / "canonical_provenance.json"
                ),
                "worker_lifecycle_managed": False,
                "service_lifecycle_managed": False,
                "secrets_recorded": False,
            },
            temporary / "oracle_manifest.json",
        )
        canonical.parent.mkdir(parents=True, exist_ok=True)
        os.replace(temporary, canonical)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
    return canonical


def run_reduced_oracle_recovery(
    *,
    config: ReducedOracleConfig,
    system: SystemConfig,
    adapter: AgentAdapterProtocol,
    incident_dir: str | Path,
    recovery_dir: str | Path,
    max_consecutive_infrastructure_failures: int = 3,
    max_attempts_per_trial: int = 3,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Retry only audited infrastructure failures and missing trials.

    The incident directory is hash-verified before every run and again before
    canonicalization. Recovery attempts use a new deterministic session ID,
    are appended to a separate ledger, and never overwrite raw evidence.
    """
    for value, name in (
        (
            max_consecutive_infrastructure_failures,
            "max_consecutive_infrastructure_failures",
        ),
        (max_attempts_per_trial, "max_attempts_per_trial"),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ReducedOracleRecoveryError(f"{name} must be positive")
    if not adapter.settings.pinning_requested:
        raise ReducedOracleRecoveryError(
            "reduced-oracle recovery requires a pinned FlowMesh worker"
        )

    incident = Path(incident_dir).resolve()
    recovery = Path(recovery_dir).resolve()
    _require_separate_directories(incident, recovery)
    plan = plan_reduced_oracle_recovery(
        config=config,
        system=system,
        incident_dir=incident,
        recovery_dir=recovery,
    )
    _validate_recovery_plan(
        plan,
        config=config,
        system=system,
        incident_dir=incident,
    )
    pilot, incident_records, recovery_items, audit = (
        _load_and_validate_incident(
            config=config,
            system=system,
            incident_dir=incident,
        )
    )
    _validate_recovery_items(plan, recovery_items, audit)
    object_ids = tuple(
        sorted({workload.object_id for workload in pilot.workloads})
    )
    _initialize_recovery_runtime(
        incident_dir=incident,
        recovery_dir=recovery,
    )
    executor = FilesystemTransitionExecutor(
        config,
        object_ids=object_ids,
        runtime_dir=recovery / "runtime",
    )
    attempts_path = recovery / "attempts.jsonl"
    transitions_path = recovery / "recovery_transitions.jsonl"
    run_id = f"recovery-run-{uuid.uuid4()}"

    with _exclusive_pilot_lock(recovery / ".recovery.lock"):
        attempts = _load_attempts(
            attempts_path,
            chain_seed_sha256=str(plan["incident_snapshot_sha256"]),
        )
        completed_attempts = _completed_attempts_by_trial(attempts)
        grouped_attempts = _attempts_by_trial(attempts)
        consecutive_infrastructure_failures = 0
        status = "RUNNING"
        detail: str | None = None

        if executor.active_design_id() != config.safe_design_id:
            transition = executor.transition(
                config.safe_design_id,
                transition_type=(
                    "activate"
                    if executor.active_design_id() is None
                    else "restore"
                ),
            )
            _append_jsonl(transitions_path, transition.to_dict())

        try:
            items_by_design: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for item in plan["recovery_items"]:
                key = (str(item["design_id"]), str(item["trial_key"]))
                if key not in completed_attempts:
                    items_by_design[key[0]].append(item)

            for design_id in config.design_ids:
                pending = items_by_design.get(design_id, [])
                if not pending:
                    continue
                if design_id != config.safe_design_id:
                    transition = executor.transition(
                        design_id,
                        transition_type="forward",
                    )
                    _append_jsonl(transitions_path, transition.to_dict())
                try:
                    for item in pending:
                        key = (design_id, str(item["trial_key"]))
                        previous = grouped_attempts.get(key, [])
                        if len(previous) >= max_attempts_per_trial:
                            status = "ATTEMPT_LIMIT_REACHED"
                            detail = (
                                f"{design_id}:{item['trial_key']} reached "
                                f"{max_attempts_per_trial} recovery attempts"
                            )
                            raise ReducedOracleRecoveryError(detail)
                        attempt_number = len(previous) + 1
                        original = _trial_from_dict(item["trial"])
                        trial, attempt_key = _retry_trial(
                            original=original,
                            recovery_id=str(plan["recovery_id"]),
                            attempt_number=attempt_number,
                            recovery_order_index=int(
                                item["recovery_order_index"]
                            ),
                        )
                        record = _execute_attempt(
                            pilot=_design_pilot(pilot, design_id),
                            system=system,
                            adapter=adapter,
                            trial=trial,
                        )
                        record.update(
                            {
                                "recovery_schema_version": (
                                    RECOVERY_ATTEMPT_SCHEMA_VERSION
                                ),
                                "recovery_id": plan["recovery_id"],
                                "recovery_run_id": run_id,
                                "recovery_attempt_key": attempt_key,
                                "canonical_trial_key": item["trial_key"],
                                "planned_trial_id": original.trial_id,
                                "planned_session_id": original.session_id,
                                "attempt_number": attempt_number,
                                "incident_disposition": item["disposition"],
                                "incident_record_sha256": item.get(
                                    "incident_record_sha256"
                                ),
                            }
                        )
                        previous_chain_sha256 = (
                            str(attempts[-1]["attempt_chain_sha256"])
                            if attempts
                            else str(plan["incident_snapshot_sha256"])
                        )
                        record = _seal_attempt(
                            record,
                            previous_chain_sha256=previous_chain_sha256,
                        )
                        _append_jsonl(attempts_path, record)
                        attempts.append(record)
                        grouped_attempts[key].append(record)
                        if _valid_completed_record(record):
                            completed_attempts[key] = record

                        outcome = str(record["outcome_type"])
                        if outcome == "infrastructure_failure":
                            consecutive_infrastructure_failures += 1
                        else:
                            consecutive_infrastructure_failures = 0
                        if progress_callback is not None:
                            progress_callback(
                                {
                                    "design_id": design_id,
                                    "trial_key": item["trial_key"],
                                    "attempt_number": attempt_number,
                                    "outcome_type": outcome,
                                    "recovered_trials": len(
                                        completed_attempts
                                    ),
                                    "recoverable_trials": plan[
                                        "recoverable_trial_count"
                                    ],
                                }
                            )
                        if outcome in {
                            "telemetry_failure",
                            "artifact_delivery_failure",
                        }:
                            status = "RESEARCH_INTEGRITY_FAILURE"
                            detail = (
                                f"{design_id}:{item['trial_key']} produced "
                                f"{outcome}; automatic recovery stopped"
                            )
                            raise ReducedOracleRecoveryError(detail)
                        if (
                            consecutive_infrastructure_failures
                            >= max_consecutive_infrastructure_failures
                        ):
                            status = "CIRCUIT_OPEN"
                            detail = (
                                "consecutive infrastructure failures reached "
                                f"{max_consecutive_infrastructure_failures}; "
                                "no more workflows were submitted"
                            )
                            raise RecoveryCircuitOpenError(detail)
                finally:
                    if design_id != config.safe_design_id:
                        restoration = executor.transition(
                            config.safe_design_id,
                            transition_type="restore",
                        )
                        _append_jsonl(
                            transitions_path,
                            restoration.to_dict(),
                        )
        except Exception:
            safe = executor.active_design_id() == config.safe_design_id
            _write_recovery_manifest(
                recovery_dir=recovery,
                plan=plan,
                attempts=attempts,
                status=status,
                run_id=run_id,
                safe_design_restored=safe,
                max_consecutive_infrastructure_failures=(
                    max_consecutive_infrastructure_failures
                ),
                max_attempts_per_trial=max_attempts_per_trial,
                detail=detail,
            )
            raise

        if executor.active_design_id() != config.safe_design_id:
            restoration = executor.transition(
                config.safe_design_id,
                transition_type="restore",
            )
            _append_jsonl(transitions_path, restoration.to_dict())
        _verify_incident_evidence(
            incident,
            plan["incident_evidence_sha256"],
        )
        completed_attempts = _completed_attempts_by_trial(attempts)
        if len(completed_attempts) != int(plan["recoverable_trial_count"]):
            status = "INCOMPLETE"
            detail = "not every recoverable trial has a valid completed attempt"
            _write_recovery_manifest(
                recovery_dir=recovery,
                plan=plan,
                attempts=attempts,
                status=status,
                run_id=run_id,
                safe_design_restored=True,
                max_consecutive_infrastructure_failures=(
                    max_consecutive_infrastructure_failures
                ),
                max_attempts_per_trial=max_attempts_per_trial,
                detail=detail,
            )
            raise ReducedOracleRecoveryError(detail)

        try:
            canonical = _finalize_canonical_oracle(
                config=config,
                system=system,
                pilot=pilot,
                incident_dir=incident,
                recovery_dir=recovery,
                plan=plan,
                incident_records=incident_records,
                attempts=attempts,
            )
        except Exception as exc:
            _write_recovery_manifest(
                recovery_dir=recovery,
                plan=plan,
                attempts=attempts,
                status="FINALIZATION_FAILED",
                run_id=run_id,
                safe_design_restored=True,
                max_consecutive_infrastructure_failures=(
                    max_consecutive_infrastructure_failures
                ),
                max_attempts_per_trial=max_attempts_per_trial,
                detail=(
                    f"{type(exc).__name__}: canonical finalization failed; "
                    "inspect the local exception without copying secrets into "
                    "research records"
                ),
            )
            raise
        manifest = _write_recovery_manifest(
            recovery_dir=recovery,
            plan=plan,
            attempts=attempts,
            status="COMPLETE",
            run_id=run_id,
            safe_design_restored=True,
            max_consecutive_infrastructure_failures=(
                max_consecutive_infrastructure_failures
            ),
            max_attempts_per_trial=max_attempts_per_trial,
            canonical_output_dir=canonical,
        )
        manifest["worker_id"] = adapter.settings.worker_id
        manifest["worker_alias"] = adapter.settings.worker_alias
        _write_json(manifest, recovery / "recovery_manifest.json")
        return manifest
