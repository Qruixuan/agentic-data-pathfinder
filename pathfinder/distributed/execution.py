"""Executable distributed-pilot runner.

Drives the deterministic plan cell by cell through the existing FlowMesh
session adapter, routing every representation access to its declared Data
Agent endpoint and assembling a total-cost ledger for each completed cell.

Nothing here creates a worker, starts a Data Agent or MCP service, or commits
a design. Service lifecycle stays under manual operator control; this runner
only submits work through an adapter the operator already stood up.

Durability is per attempt: every outcome is appended to its ledger and fsynced
before the next cell begins, so a run killed mid-flight resumes without
re-executing a cell it already completed and without inventing one it did not.
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable, Mapping, Protocol

from ..integrations.flowmesh.contracts import FlowMeshAgentRunRequest
from ..integrations.flowmesh.gateway import TelemetryIncompleteError
from ..integrations.flowmesh.pilot import (
    _safe_error_message,
    _sanitized_access_event,
    artifact_delivery_failures,
)
from .cost import CostModel
from .measurements import (
    MeasurementProvider,
    build_measured_cost_ledger,
)
from .amendment import bind_amendment_to_run
from .preregistration import (
    DistributedPilotPreregistration,
    workload_content_sha256,
)
from .registry import EndpointRegistry, EndpointUnreachableError
from .routing import CrossEndpointArtifactError
from .scoring import (
    ACCEPTED_SUBSTRING_SCORING_RULE,
    evaluate_workload_answer,
    load_workload_scoring_contract,
    render_workload_question,
    validate_workload_manifest,
)
from .runner import (
    DistributedTrial,
    ObservationOutcome,
    PilotResumeError,
    PilotRunnerError,
    PilotRunState,
    build_distributed_trial_plan,
    ensure_frozen_plan,
    new_run_state,
    trial_plan_payload,
)


DISTRIBUTED_RECORD_SCHEMA_VERSION = (
    "pathfinder.distributed-pilot-record/v1alpha1"
)
CANONICAL_LEDGER = "canonical_records.jsonl"
ATTEMPT_LEDGER = "attempt_ledger.jsonl"
CELL_JOURNAL = "cell_journal.jsonl"
PLAN_DOCUMENT = "distributed_pilot_plan.json"

#: Ordered lifecycle of one canonical cell. Each transition is fsynced before
#: the action it authorises, so a crash is always observed at a known state
#: rather than inferred from which of two files happened to be written.
#: Outcomes that must never be retried. An unbound session may or may not
#: have reached FlowMesh; retrying it could run the same workload twice on
#: the worker, so the cell is left incomplete for an operator to resolve.
NON_RETRYABLE_OUTCOMES = ("ambiguous_submission",)

CELL_STATES = (
    "PLANNED",
    "STARTED",
    "FLOWMESH_BOUND",
    "RESULT_OBTAINED",
    "CANONICAL_WRITTEN",
    "COMPLETED",
)
_STATE_ORDER = {state: index for index, state in enumerate(CELL_STATES)}

#: The only outcome a canonical record may carry to be recoverable. A
#: delivery, telemetry, or infrastructure failure is an attempt, not an
#: observation, and must never be replayed as one.
COMPLETED_OUTCOME_TYPE = "completed"


class PreflightRequiredError(PilotRunnerError):
    """Raised when execution is attempted without a passing preflight."""


class CanonicalRecordError(PilotResumeError):
    """Raised when the durable canonical ledger cannot be trusted."""


#: Immutable identity of a planned cell. A durable canonical record must
#: agree with its frozen trial on every one of these before it may be
#: replayed: a record that matches only by trial_key could still describe a
#: different design, workload, or repetition, and replaying it would silently
#: substitute one observation for another.
TRIAL_IDENTITY_FIELDS = (
    "trial_key",
    "trial_id",
    "session_id",
    "order_index",
    "stratum_id",
    "workload_id",
    "design_id",
    "is_safe_design",
    "repetition",
    "seed",
)


def load_durable_canonical_records(
    path: str | Path,
    planned_trials: Mapping[str, DistributedTrial],
    *,
    pilot_id: str | None = None,
    schema_version: str | None = DISTRIBUTED_RECORD_SCHEMA_VERSION,
) -> dict[str, dict[str, Any]]:
    """Index the durable canonical ledger, failing closed on any defect.

    Read independently of the cell journal. The canonical append and the
    journal write are two separate fsyncs, so a crash between them leaves a
    durable record the journal does not yet mention; trusting the journal
    here would re-execute that cell and append its record twice.

    ``planned_trials`` maps trial key to the frozen
    :class:`DistributedTrial` and is required. Every identity field is
    compared, because a record is about to be accepted *in place of* running
    that cell -- a mismatch on design, workload, repetition, or session
    identity means the record describes different work than the plan calls
    for. A bare key collection is rejected rather than accepted with weaker
    structural-only checks: silently downgrading validation is exactly how a
    mismatched record would slip through.

    A duplicate, conflicting, mismatched, unknown, or malformed record is
    refused rather than reconciled: the invariant that one canonical cell
    appears at most once is what makes the dataset countable, and a silent
    repair would hide exactly the corruption worth stopping for.
    """
    if not isinstance(planned_trials, Mapping):
        raise CanonicalRecordError(
            "planned_trials must map trial_key to the frozen "
            "DistributedTrial; a bare key collection cannot support "
            "identity validation"
        )
    trials: dict[str, DistributedTrial] = dict(planned_trials)
    for key, trial in trials.items():
        if not isinstance(trial, DistributedTrial):
            raise CanonicalRecordError(
                f"planned_trials[{key!r}] is not a DistributedTrial"
            )

    records: dict[str, dict[str, Any]] = {}
    for index, row in enumerate(_read_jsonl(Path(path))):
        location = f"{Path(path).name}:{index + 1}"
        if not isinstance(row, dict):
            raise CanonicalRecordError(
                f"{location}: canonical record is not an object"
            )
        trial_key = row.get("trial_key")
        if not isinstance(trial_key, str) or not trial_key.strip():
            raise CanonicalRecordError(
                f"{location}: canonical record has no trial_key"
            )
        if trial_key not in trials:
            raise CanonicalRecordError(
                f"{location}: canonical record names a cell that is not in "
                f"the frozen plan: {trial_key}"
            )
        if schema_version is not None:
            found = row.get("schema_version")
            if found != schema_version:
                raise CanonicalRecordError(
                    f"{location}: canonical record has schema_version "
                    f"{found!r}, expected {schema_version!r}"
                )
        if pilot_id is not None:
            found = row.get("experiment_id")
            if found != pilot_id:
                raise CanonicalRecordError(
                    f"{location}: canonical record belongs to pilot "
                    f"{found!r}, not {pilot_id!r}"
                )
        expected = trials[trial_key].to_public_dict()
        for field in TRIAL_IDENTITY_FIELDS:
            if field not in row:
                raise CanonicalRecordError(
                    f"{location}: canonical record is missing "
                    f"identity field {field}"
                )
            if row[field] != expected[field]:
                raise CanonicalRecordError(
                    f"{location}: canonical record {field}="
                    f"{row[field]!r} does not match the frozen trial's "
                    f"{expected[field]!r}; refusing to replay a record "
                    "describing different work"
                )
        # Identity comparison alone does not make a record admissible: it
        # must also assert completeness in the exact form this pipeline
        # writes. Truthiness is not enough here. A replayed record is
        # recorded downstream as telemetry_complete=True and
        # artifact_delivery_complete=True, so a missing, null, or
        # string-typed field accepted as "close enough" would silently
        # promote an unfinished or corrupted cell into a complete
        # scientific observation.
        for field in ("telemetry_complete", "artifact_delivery_complete"):
            if field not in row:
                raise CanonicalRecordError(
                    f"{location}: canonical record is missing {field}; "
                    "only a record that explicitly asserts completeness may "
                    "be replayed"
                )
            value = row[field]
            if value is not True:
                raise CanonicalRecordError(
                    f"{location}: canonical record {field}={value!r} "
                    f"(type {type(value).__name__}) is not the literal True; "
                    "refusing to replay a record that does not assert "
                    "completeness"
                )
        if "outcome_type" not in row:
            raise CanonicalRecordError(
                f"{location}: canonical record is missing outcome_type"
            )
        outcome_type = row["outcome_type"]
        if outcome_type != COMPLETED_OUTCOME_TYPE:
            raise CanonicalRecordError(
                f"{location}: canonical record outcome_type="
                f"{outcome_type!r} is not "
                f"{COMPLETED_OUTCOME_TYPE!r}; only a completed observation "
                "may be recovered"
            )
        existing = records.get(trial_key)
        if existing is not None:
            detail = (
                "duplicate canonical record"
                if existing == row
                else "conflicting canonical records"
            )
            raise CanonicalRecordError(
                f"{location}: {detail} for cell {trial_key}; a canonical "
                "cell may appear at most once"
            )
        records[trial_key] = row
    return records


class CellJournal:
    """Durable per-cell state machine that closes the crash window.

    Writing the canonical record and writing the attempt row are two
    separate appends. Without a journal, a crash between them is
    indistinguishable from a crash before either, and resume has to guess.
    The journal records the transition *before* each side effect, so on
    restart the exact boundary is known:

    * ``STARTED`` but not ``FLOWMESH_BOUND`` -- nothing was submitted, or a
      session exists but is unbound; the adapter's own recovery decides.
    * ``FLOWMESH_BOUND`` -- a task may be running; recover it, never
      resubmit.
    * ``RESULT_OBTAINED`` -- the answer is held by the Gateway; recover and
      rebuild the record.
    * ``CANONICAL_WRITTEN`` -- the record is durable; only the attempt row
      is missing, so replay that and move on without re-executing.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._states: dict[str, str] = {}
        for row in _read_jsonl(self.path):
            trial_key = str(row.get("trial_key"))
            state = str(row.get("state"))
            if state not in _STATE_ORDER:
                raise PilotResumeError(
                    f"cell journal contains an unknown state: {state}"
                )
            current = self._states.get(trial_key)
            if (
                current is None
                or _STATE_ORDER[state] >= _STATE_ORDER[current]
            ):
                self._states[trial_key] = state

    def state(self, trial_key: str) -> str:
        return self._states.get(trial_key, "PLANNED")

    def at_least(self, trial_key: str, state: str) -> bool:
        return _STATE_ORDER[self.state(trial_key)] >= _STATE_ORDER[state]

    def record(
        self,
        trial_key: str,
        state: str,
        **detail: Any,
    ) -> None:
        if state not in _STATE_ORDER:
            raise PilotRunnerError(f"unknown cell state: {state}")
        _append_jsonl(self.path, {
            "trial_key": trial_key,
            "state": state,
            "recorded_at": _utc_now(),
            **detail,
        })
        self._states[trial_key] = state

    def reset(self, trial_key: str) -> None:
        """Return a cell to PLANNED after a failed attempt."""
        self.record(trial_key, "PLANNED", reason="attempt_failed")

    def to_public_dict(self) -> dict[str, Any]:
        counts: dict[str, int] = {state: 0 for state in CELL_STATES}
        for state in self._states.values():
            counts[state] += 1
        return {
            "journal_path": str(self.path),
            "cell_states": counts,
            "states": dict(self._states),
        }


@dataclass(frozen=True)
class TrialExecution:
    """One adapter result, already normalized for record construction."""

    final_answer: str | None
    access_events: tuple[dict[str, Any], ...]
    workflow_id: str | None = None
    task_id: str | None = None
    status: str | None = None


class SessionExecutor(Protocol):
    """Runs one planned cell through the FlowMesh session adapter."""

    def execute(
        self,
        trial: DistributedTrial,
        *,
        workload: Mapping[str, Any],
        journal: Any = None,
        attempt: int = 1,
    ) -> TrialExecution:
        """Execute one cell, or raise to signal a failed attempt."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    """Append one record and flush it to disk before returning."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def classify_failure(exc: Exception) -> tuple[str, str]:
    """Map an exception to (observation_class, outcome_type).

    A cross-endpoint artifact redemption is a system delivery fault, not an
    Agent policy choice, so it is never allowed to depress task success.
    """
    if isinstance(exc, CrossEndpointArtifactError):
        return "recovery_attempt", "artifact_delivery_failure"
    if isinstance(exc, EndpointUnreachableError):
        return "infrastructure", "endpoint_unreachable"
    if isinstance(exc, TelemetryIncompleteError):
        return "recovery_attempt", "telemetry_failure"
    name = type(exc).__name__
    if name == "FlowMeshRunError" and "never bound" in str(exc):
        # The session exists but was never bound to a task: whether FlowMesh
        # accepted work is unknowable from here, so the cell stops rather
        # than risking a duplicate run on the worker.
        return "infrastructure", "ambiguous_submission"
    if name == "FlowMeshPinningError":
        # The worker pin could not be honoured: a control-plane fault, and
        # never evidence about the design under test.
        return "infrastructure", "worker_pin_failure"
    if name == "FlowMeshWorkflowFailureError":
        return "infrastructure", "workflow_failure"
    if name == "FlowMeshRunError":
        return "infrastructure", "flowmesh_failure"
    if "Telemetry" in name:
        return "recovery_attempt", "telemetry_failure"
    if "Unavailable" in name or "Connection" in name or "Timeout" in name:
        return "infrastructure", "endpoint_unreachable"
    return "infrastructure", "infrastructure_failure"


def build_canonical_record(
    preregistration: DistributedPilotPreregistration,
    trial: DistributedTrial,
    execution: TrialExecution,
    *,
    workload: Mapping[str, Any],
    started_at: str,
    finished_at: str,
    cost_model: CostModel,
    provider: MeasurementProvider | None,
    source_node_id: str | None,
    network_transport_for: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Build one Reduced-Oracle-compatible record with its cost ledger."""
    scoring = load_workload_scoring_contract(
        workload,
        preregistration.success_scoring_rule,
        name=f"workloads[{trial.workload_id!r}]",
    )
    events = [
        _sanitized_access_event(dict(event))
        for event in execution.access_events
    ]
    accepted = [event for event in events if event.get("accepted")]
    delivery_failures = artifact_delivery_failures(events)
    artifact_required = any(
        event.get("accepted") and event.get("artifact_handle_sha256")
        for event in events
    )
    record: dict[str, Any] = {
        "schema_version": DISTRIBUTED_RECORD_SCHEMA_VERSION,
        "experiment_id": preregistration.pilot_id,
        **trial.to_public_dict(),
        "object_id": workload.get("object_id"),
        "question": workload.get("question"),
        **scoring.record_fields(),
        "task_class_id": workload.get("task_class_id", "video_qa"),
        "quote_profile_id": workload.get("quote_profile_id", "as_designed"),
        "latency_multiplier": workload.get("latency_multiplier", 1),
        "started_at": started_at,
        "finished_at": finished_at,
        "outcome_type": (
            "artifact_delivery_failure" if delivery_failures else "completed"
        ),
        "telemetry_complete": True,
        "workflow_id": execution.workflow_id,
        "task_id": execution.task_id,
        "flowmesh_status": execution.status,
        "final_answer": execution.final_answer,
        # An undelivered artifact is a system fault. Scoring it as a task
        # failure would charge the design for the platform's mistake.
        "task_success": (
            None
            if delivery_failures
            else evaluate_workload_answer(
                execution.final_answer or "",
                scoring,
            )
        ),
        "access_event_count": len(events),
        "accepted_access_count": len(accepted),
        "selected_representations": [
            str(event["representation_id"])
            for event in accepted
            if event.get("representation_id") is not None
        ],
        "access_events": events,
        "artifact_delivery_required": bool(artifact_required),
        "artifact_delivery_complete": not delivery_failures,
        "artifact_delivery_failure_count": len(delivery_failures),
        "artifact_delivery_failures": delivery_failures,
        "error_type": (
            "ArtifactDeliveryFailure" if delivery_failures else None
        ),
        "error_message": None,
    }
    if provider is not None:
        ledger = build_measured_cost_ledger(
            record,
            model=cost_model,
            provider=provider,
            design_id=trial.design_id,
            object_id=str(workload.get("object_id") or trial.workload_id),
            node_id=source_node_id or "",
            network_transport_for=network_transport_for,
        )
        record["cost_ledger"] = ledger.to_public_dict()
    return record


@dataclass
class DistributedPilotRun:
    """Durable state for one executable distributed-pilot run."""

    preregistration: DistributedPilotPreregistration
    registry: EndpointRegistry
    output_dir: Path
    state: PilotRunState
    trials: tuple[DistributedTrial, ...]
    journal: CellJournal

    @property
    def journal_path(self) -> Path:
        return self.output_dir / CELL_JOURNAL

    @property
    def canonical_path(self) -> Path:
        return self.output_dir / CANONICAL_LEDGER

    @property
    def attempt_path(self) -> Path:
        return self.output_dir / ATTEMPT_LEDGER

    def canonical_records(self) -> list[dict[str, Any]]:
        return _read_jsonl(self.canonical_path)


def build_frozen_plan_document(
    preregistration: DistributedPilotPreregistration,
    registry: EndpointRegistry,
    *,
    workloads: Mapping[str, Mapping[str, Any]] | None = None,
    trials: Iterable[DistributedTrial] | None = None,
) -> dict[str, Any]:
    """Build the exact plan document both planning and execution write.

    Shared so the two paths cannot drift: a plan written by the planning
    command must compare byte-identical to the one execution recomputes, or
    the frozen-plan gate would reject the very plan it was handed.
    """
    if workloads is not None:
        validate_workload_manifest(
            workloads,
            preregistration.workload_ids,
            preregistration.success_scoring_rule,
        )
    ordered = (
        list(trials)
        if trials is not None
        else list(build_distributed_trial_plan(preregistration))
    )
    plan = trial_plan_payload(
        preregistration,
        ordered,
        workloads=workloads,
    )
    plan = {
        **plan,
        "endpoint_registry_sha256": registry.source_sha256,
        "execution_node_id": registry.execution_node_id,
    }
    plan["plan_sha256"] = sha256(
        json.dumps(
            {k: v for k, v in plan.items() if k != "plan_sha256"},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return plan


def open_distributed_pilot_run(
    preregistration: DistributedPilotPreregistration,
    registry: EndpointRegistry,
    *,
    output_dir: str | Path,
    max_attempts: int = 3,
    workloads: Mapping[str, Mapping[str, Any]] | None = None,
) -> DistributedPilotRun:
    """Open or resume a run, refusing any change to a frozen input."""
    output = Path(output_dir).resolve()
    trials = build_distributed_trial_plan(preregistration)
    plan = build_frozen_plan_document(
        preregistration,
        registry,
        workloads=workloads,
        trials=trials,
    )
    ensure_frozen_plan(output / PLAN_DOCUMENT, plan)

    state = new_run_state(preregistration, trials, max_attempts=max_attempts)
    planned = {trial.trial_key for trial in trials}
    for row in _read_jsonl(output / ATTEMPT_LEDGER):
        trial_key = row.get("trial_key")
        if trial_key not in planned:
            raise PilotResumeError(
                "the attempt ledger references a cell that is not in the "
                f"frozen plan: {trial_key}"
            )
        state.record(ObservationOutcome(
            trial_key=str(trial_key),
            observation_class=str(row["observation_class"]),
            attempt=int(row["attempt"]),
            succeeded=bool(row["succeeded"]),
            telemetry_complete=bool(row["telemetry_complete"]),
            artifact_selected=bool(row["artifact_selected"]),
            artifact_delivery_complete=bool(
                row["artifact_delivery_complete"]
            ),
            failure_class=row.get("failure_class"),
            failure_detail=row.get("failure_detail"),
        ))
    return DistributedPilotRun(
        preregistration=preregistration,
        registry=registry,
        output_dir=output,
        state=state,
        trials=trials,
        journal=CellJournal(output / CELL_JOURNAL),
    )


def run_distributed_pilot(
    preregistration: DistributedPilotPreregistration,
    registry: EndpointRegistry,
    executor: SessionExecutor,
    *,
    output_dir: str | Path,
    workloads: Mapping[str, Mapping[str, Any]],
    provider: MeasurementProvider | None = None,
    preflight: Mapping[str, Any] | None = None,
    max_attempts: int = 3,
    require_preflight: bool = True,
    execution_provenance: Mapping[str, Any] | None = None,
    execution_amendment_path: str | Path | None = None,
) -> dict[str, Any]:
    """Execute the frozen plan cell by cell, durably and resumably."""
    if require_preflight:
        if preflight is None:
            raise PreflightRequiredError(
                "a passing preflight report is required before executing a "
                "distributed pilot; run preflight-distributed-pilot first"
            )
        if preflight.get("status") != "ok":
            raise PreflightRequiredError(
                "preflight did not pass; refusing to execute. Failed "
                "checks: " + ", ".join(preflight.get("failed_checks") or [])
            )
        if preflight.get("pilot_id") != preregistration.pilot_id:
            raise PreflightRequiredError(
                "the supplied preflight report is for a different pilot"
            )

    missing_workloads = sorted(
        set(preregistration.workload_ids) - set(workloads)
    )
    if missing_workloads:
        raise PilotRunnerError(
            "no workload definition for: " + ", ".join(missing_workloads)
        )
    # Opened with the definitions so a changed question, object id, or
    # accepted answer is refused before a single cell executes.
    run = open_distributed_pilot_run(
        preregistration,
        registry,
        output_dir=output_dir,
        max_attempts=max_attempts,
        workloads=workloads,
    )
    # Bound before any cell executes, and before any FlowMesh session is
    # opened. On resume this refuses a swapped amendment, so a run cannot
    # start under one compatibility claim and finish under another.
    bound_amendment_sha256 = bind_amendment_to_run(
        run.output_dir,
        execution_amendment_path,
    )

    if provider is not None:
        require = getattr(provider, "require_matching_run", None)
        if callable(require):
            require(
                pilot_id=preregistration.pilot_id,
                preregistration_sha256=preregistration.source_sha256,
                endpoint_registry_sha256=registry.source_sha256,
                execution_node_id=registry.execution_node_id,
            )

    missing_workloads = sorted(
        set(preregistration.workload_ids) - set(workloads)
    )
    if missing_workloads:
        raise PilotRunnerError(
            "no workload definition for: " + ", ".join(missing_workloads)
        )

    # Recovered from the ledger itself, never from the journal: a crash
    # between the canonical fsync and the CANONICAL_WRITTEN journal write
    # leaves a durable record the journal does not mention, and re-executing
    # that cell would append its record a second time.
    durable_records = load_durable_canonical_records(
        run.canonical_path,
        {trial.trial_key: trial for trial in run.trials},
        pilot_id=preregistration.pilot_id,
    )
    for trial in run.trials:
        if trial.trial_key in run.state.completed:
            continue
        record = durable_records.get(trial.trial_key)
        if record is None:
            continue
        outcome = ObservationOutcome(
            trial_key=trial.trial_key,
            observation_class="canonical",
            attempt=run.state.attempts_for(trial.trial_key) + 1,
            succeeded=True,
            telemetry_complete=True,
            artifact_selected=bool(record.get("artifact_delivery_required")),
            artifact_delivery_complete=True,
        )
        if not run.journal.at_least(trial.trial_key, "CANONICAL_WRITTEN"):
            # The record outlived its journal entry; catch the journal up
            # so the two agree before the attempt row is written.
            run.journal.record(
                trial.trial_key,
                "CANONICAL_WRITTEN",
                recovered_from_ledger=True,
            )
        _append_jsonl(run.attempt_path, {
            "trial_key": trial.trial_key,
            "observation_class": "canonical",
            "attempt": outcome.attempt,
            "succeeded": True,
            "telemetry_complete": True,
            "artifact_selected": outcome.artifact_selected,
            "artifact_delivery_complete": True,
            "failure_class": None,
            "failure_detail": None,
            "replayed_from_canonical_record": True,
            "recorded_at": _utc_now(),
        })
        run.state.record(outcome)
        run.journal.record(trial.trial_key, "COMPLETED", replayed=True)

    executed = 0
    for trial in run.trials:
        if trial.trial_key in run.state.completed:
            continue
        while run.state.may_retry(trial.trial_key):
            attempt = run.state.attempts_for(trial.trial_key) + 1
            started_at = _utc_now()
            workload = workloads[trial.workload_id]
            run.journal.record(
                trial.trial_key,
                "STARTED",
                attempt=attempt,
                session_id=trial.session_id,
            )
            # KeyboardInterrupt and SystemExit are not caught. An operator
            # stopping the run must not have that recorded as a failed
            # attempt, which would consume the cell's retry budget and make
            # a deliberate stop look like a flaky endpoint. The journal
            # entry just written is what a later resume reads.
            try:
                execution = executor.execute(
                    trial,
                    workload=workload,
                    journal=run.journal,
                    attempt=attempt,
                )
            except Exception as exc:  # noqa: BLE001 - classified below
                observation_class, outcome_type = classify_failure(exc)
                _append_jsonl(run.attempt_path, {
                    "trial_key": trial.trial_key,
                    "observation_class": observation_class,
                    "attempt": attempt,
                    "succeeded": False,
                    "telemetry_complete": False,
                    "artifact_selected": False,
                    "artifact_delivery_complete": False,
                    "failure_class": outcome_type,
                    "failure_detail": _safe_error_message(exc),
                    "recorded_at": _utc_now(),
                })
                run.state.record(ObservationOutcome(
                    trial_key=trial.trial_key,
                    observation_class=observation_class,
                    attempt=attempt,
                    succeeded=False,
                    telemetry_complete=False,
                    artifact_selected=False,
                    artifact_delivery_complete=False,
                    failure_class=outcome_type,
                    failure_detail=_safe_error_message(exc),
                ))
                run.journal.reset(trial.trial_key)
                if outcome_type in NON_RETRYABLE_OUTCOMES:
                    break
                continue

            run.journal.record(trial.trial_key, "RESULT_OBTAINED")
            record = build_canonical_record(
                preregistration,
                trial,
                execution,
                workload=workload,
                started_at=started_at,
                finished_at=_utc_now(),
                cost_model=preregistration.cost_model,
                provider=provider,
                network_transport_for={
                    endpoint_id: registry.endpoint(
                        endpoint_id
                    ).network_transport
                    for endpoint_id in registry.endpoint_ids
                },
                source_node_id=next(
                    (
                        str(event.get("source_node_id"))
                        for event in execution.access_events
                        if event.get("source_node_id")
                    ),
                    None,
                ),
            )
            artifact_selected = bool(record["artifact_delivery_required"])
            delivered = bool(record["artifact_delivery_complete"])
            if not delivered:
                # A delivery fault is a real attempt that produced no usable
                # observation. It is retried, and never scored as a task.
                _append_jsonl(run.attempt_path, {
                    "trial_key": trial.trial_key,
                    "observation_class": "recovery_attempt",
                    "attempt": attempt,
                    "succeeded": False,
                    "telemetry_complete": True,
                    "artifact_selected": artifact_selected,
                    "artifact_delivery_complete": False,
                    "failure_class": "artifact_delivery_failure",
                    "failure_detail": record.get("error_message"),
                    "recorded_at": _utc_now(),
                })
                run.state.record(ObservationOutcome(
                    trial_key=trial.trial_key,
                    observation_class="recovery_attempt",
                    attempt=attempt,
                    succeeded=False,
                    telemetry_complete=True,
                    artifact_selected=artifact_selected,
                    artifact_delivery_complete=False,
                    failure_class="artifact_delivery_failure",
                ))
                run.journal.reset(trial.trial_key)
                continue

            _append_jsonl(run.canonical_path, record)
            # Journalled immediately after the record is fsynced. A crash
            # between here and the attempt row leaves CANONICAL_WRITTEN, and
            # resume replays that record instead of re-executing the cell.
            run.journal.record(trial.trial_key, "CANONICAL_WRITTEN")
            _append_jsonl(run.attempt_path, {
                "trial_key": trial.trial_key,
                "observation_class": "canonical",
                "attempt": attempt,
                "succeeded": True,
                "telemetry_complete": True,
                "artifact_selected": artifact_selected,
                "artifact_delivery_complete": True,
                "failure_class": None,
                "failure_detail": None,
                "recorded_at": _utc_now(),
            })
            run.state.record(ObservationOutcome(
                trial_key=trial.trial_key,
                observation_class="canonical",
                attempt=attempt,
                succeeded=True,
                telemetry_complete=True,
                artifact_selected=artifact_selected,
                artifact_delivery_complete=True,
            ))
            run.journal.record(trial.trial_key, "COMPLETED")
            executed += 1
            break

    complete_workloads = run.state.complete_repetition_blocks(
        preregistration
    )
    summary = {
        "schema_version": DISTRIBUTED_RECORD_SCHEMA_VERSION,
        "pilot_id": preregistration.pilot_id,
        "status": "COMPLETE" if run.state.complete else "PARTIAL",
        "posthoc": preregistration.posthoc,
        "confirmatory": preregistration.confirmatory,
        "eligible_for_scientific_claims": (
            preregistration.eligible_for_scientific_claims
        ),
        "executed_this_invocation": executed,
        "oracle_complete": run.state.complete,
        "complete_workload_blocks": list(complete_workloads),
        "non_retryable_failures": [
            outcome.trial_key
            for outcome in run.state.infrastructure_failures
            if outcome.failure_class in NON_RETRYABLE_OUTCOMES
        ],
        "complete_workload_count": len(complete_workloads),
        "independent_workload_count": (
            preregistration.independent_workload_count
        ),
        "cost_basis": preregistration.cost_basis,
        "measurement_manifest_sha256": (
            getattr(provider, "manifest_sha256", None)
        ),
        "workload_content_sha256": workload_content_sha256(
            workloads,
            preregistration.workload_ids,
        ),
        "workload_id_manifest_sha256": (
            preregistration.workload_manifest_sha256
        ),
        "protocol_git_revision": (
            (execution_provenance or {}).get("protocol_git_revision")
            or preregistration.source_git_revision
        ),
        "execution_git_revision": (
            (execution_provenance or {}).get("execution_git_revision")
        ),
        "execution_amendment_sha256": bound_amendment_sha256,
        "execution_provenance": (
            dict(execution_provenance) if execution_provenance else None
        ),
        "worker_lifecycle_managed": False,
        "services_started": False,
        "commit_performed": False,
        **run.state.to_public_dict(),
        "cell_journal": run.journal.to_public_dict()["cell_states"],
        "cell_journal_path": str(run.journal_path),
        "canonical_records_path": str(run.canonical_path),
        "attempt_ledger_path": str(run.attempt_path),
        "plan_path": str(run.output_dir / PLAN_DOCUMENT),
    }
    _append_jsonl(run.output_dir / "run_summary.jsonl", summary)
    return summary


def export_reduced_oracle_records(
    run_output_dir: str | Path,
    oracle_output_dir: str | Path,
    design_ids: Iterable[str],
) -> dict[str, Any]:
    """Lay canonical records out as a Reduced Oracle design tree."""
    source = Path(run_output_dir).resolve()
    target = Path(oracle_output_dir).resolve()
    records = _read_jsonl(source / CANONICAL_LEDGER)
    by_design: dict[str, list[dict[str, Any]]] = {
        design_id: [] for design_id in design_ids
    }
    for record in records:
        design_id = str(record.get("design_id"))
        if design_id not in by_design:
            raise PilotRunnerError(
                f"canonical record names an undeclared design: {design_id}"
            )
        by_design[design_id].append(record)
    for design_id, rows in by_design.items():
        path = target / "designs" / design_id / "runs.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "".join(
                json.dumps(row, sort_keys=True) + "\n" for row in rows
            ),
            encoding="utf-8",
        )
    return {
        "oracle_output_dir": str(target),
        "record_count": len(records),
        "designs": {
            design_id: len(rows) for design_id, rows in by_design.items()
        },
    }


class FlowMeshDistributedSessionExecutor:
    """Runs one distributed plan cell through the real FlowMesh adapter.

    This is a thin seam, deliberately. ``FlowMeshAgentAdapter`` remains the
    only owner of worker verification, Gateway registration, workflow
    construction and validation, submit, wait, result retrieval, telemetry
    reconciliation, and session completion. Nothing here reimplements any of
    that; it builds the request, asks the adapter to recover before it asks
    the adapter to run, and translates what comes back into the distributed
    runner's vocabulary.

    Recovery ordering is the crash-safety property: a session that FlowMesh
    already accepted must be recovered, never resubmitted, or the same cell
    would be executed twice and collide on the Gateway primary key.
    """

    def __init__(
        self,
        adapter: Any,
        *,
        default_task_class_id: str = "video_qa",
        success_scoring_rule: str = ACCEPTED_SUBSTRING_SCORING_RULE,
    ) -> None:
        self.adapter = adapter
        self.default_task_class_id = default_task_class_id
        self.success_scoring_rule = success_scoring_rule
        self.recovered_session_ids: list[str] = []
        self.submitted_session_ids: list[str] = []

    @staticmethod
    def session_id_for_attempt(
        trial: DistributedTrial,
        attempt: int,
    ) -> str:
        """Deterministic session id for one attempt at one cell.

        The attempt number is part of the id because the adapter marks a
        session FAILED when submission fails and then refuses to retry it --
        correctly, since a silently reused session would hide a real fault.
        A fresh id per attempt keeps the bounded retry budget usable while
        leaving each attempt individually addressable by ``recover``. Resume
        recomputes the same number from the durable attempt ledger, so a
        crash mid-attempt still recovers that attempt's own session.
        """
        if attempt <= 1:
            return trial.session_id
        return str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"session:{trial.trial_key}:attempt{attempt}",
        ))

    def build_request(
        self,
        trial: DistributedTrial,
        workload: Mapping[str, Any],
        *,
        attempt: int = 1,
    ) -> FlowMeshAgentRunRequest:
        """Bind the plan cell to a deterministic FlowMesh session id."""
        scoring = load_workload_scoring_contract(
            workload,
            self.success_scoring_rule,
            name=f"workloads[{trial.workload_id!r}]",
        )
        return FlowMeshAgentRunRequest(
            question=render_workload_question(workload, scoring),
            design_id=trial.design_id,
            task_class_id=str(
                workload.get("task_class_id", self.default_task_class_id)
            ),
            quote_profile_id=str(
                workload.get("quote_profile_id", "as_designed")
            ),
            latency_multiplier=float(
                workload.get("latency_multiplier", 1.0)
            ),
            seed=trial.seed,
            trial_id=trial.trial_id,
            # Pinning the session id is what makes recover() addressable:
            # without it the adapter would derive a different id and the
            # crash-recovery path could never find the bound task.
            session_id=self.session_id_for_attempt(trial, attempt),
            object_id=(
                None
                if workload.get("object_id") is None
                else str(workload["object_id"])
            ),
        )

    def execute(
        self,
        trial: DistributedTrial,
        *,
        workload: Mapping[str, Any],
        journal: Any = None,
        attempt: int = 1,
    ) -> TrialExecution:
        session_id = self.session_id_for_attempt(trial, attempt)
        request = self.build_request(trial, workload, attempt=attempt)
        # Recovery is always attempted first: a session FlowMesh already
        # accepted must be recovered, never resubmitted.
        run = self.adapter.recover(session_id)
        if run is not None:
            self.recovered_session_ids.append(session_id)
        else:
            if journal is not None:
                journal.record(
                    trial.trial_key,
                    "FLOWMESH_BOUND",
                    session_id=session_id,
                    attempt=attempt,
                    submitted=True,
                )
            run = self.adapter.run(request)
            self.submitted_session_ids.append(session_id)
        if run.session_id != session_id:
            raise PilotRunnerError(
                "the FlowMesh adapter returned a different session than the "
                f"plan cell requested: {run.session_id} != {session_id}"
            )
        return TrialExecution(
            final_answer=run.final_answer,
            access_events=tuple(dict(event) for event in run.access_events),
            workflow_id=run.workflow_id,
            task_id=run.task_id,
            status=run.status,
        )
