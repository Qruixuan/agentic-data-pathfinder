"""Deterministic trial planning and audit-safe resume for the pilot.

The plan is stratum-specific by construction: a causal or descriptive
workload is run against ``D_origin_remote`` and ``D_local_frames``, a temporal
workload against ``D_origin_remote`` and ``D_local_digest``, and every
workload gets one complete repetition block for *both* of its designs. A
partial block is never promoted to an observation, and a partial run is never
promoted to an Oracle.

Recovery attempts are kept apart from canonical observations throughout, so a
retried infrastructure failure can never be counted as a second independent
measurement of a design.
"""

from __future__ import annotations

import json
import uuid
from collections import Counter
from dataclasses import dataclass, asdict
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable, Mapping

from .preregistration import (
    DistributedPilotPreregistration,
    workload_content_sha256,
)


PILOT_PLAN_SCHEMA_VERSION = (
    "pathfinder.distributed-pilot-plan/v1alpha1"
)
PILOT_RUN_STATE_SCHEMA_VERSION = (
    "pathfinder.distributed-pilot-run-state/v1alpha1"
)
#: Outcome classes kept in separate ledgers. Only ``canonical`` observations
#: are eligible to become Oracle rows.
OBSERVATION_CLASSES = ("canonical", "recovery_attempt", "infrastructure")


class PilotRunnerError(RuntimeError):
    """Raised when a pilot run cannot proceed safely."""


class PilotResumeError(PilotRunnerError):
    """Raised when a resumed run does not match its frozen plan."""


class IncompleteOracleError(PilotRunnerError):
    """Raised when an incomplete run is asked to act like a full Oracle."""


@dataclass(frozen=True)
class DistributedTrial:
    """One canonical (workload, design, repetition) cell."""

    trial_key: str
    trial_id: str
    session_id: str
    order_index: int
    stratum_id: str
    workload_id: str
    design_id: str
    is_safe_design: bool
    repetition: int
    seed: int

    def to_public_dict(self) -> dict[str, Any]:
        return asdict(self)


def _trial_key(
    pilot_id: str,
    workload_id: str,
    design_id: str,
    rep: int,
) -> str:
    return f"{pilot_id}|{workload_id}|{design_id}|r{rep}"


def build_distributed_trial_plan(
    preregistration: DistributedPilotPreregistration,
    *,
    base_seed: int = 0,
) -> tuple[DistributedTrial, ...]:
    """Build the deterministic, stratum-specific plan.

    Ordering is a pure function of the preregistration: strata in declared
    order, workloads in declared order, safe design before candidate, and
    repetitions ascending. Two runs of the same document produce byte
    identical plans, which is what makes resume checkable.
    """
    trials: list[DistributedTrial] = []
    order_index = 0
    for stratum in preregistration.strata:
        for workload_id in stratum.workload_ids:
            for design_id in (
                preregistration.safe_design_id,
                stratum.candidate_design_id,
            ):
                for repetition in range(preregistration.repetitions):
                    key = _trial_key(
                        preregistration.pilot_id,
                        workload_id,
                        design_id,
                        repetition,
                    )
                    trials.append(DistributedTrial(
                        trial_key=key,
                        trial_id=str(uuid.uuid5(uuid.NAMESPACE_URL, key)),
                        session_id=str(uuid.uuid5(
                            uuid.NAMESPACE_URL,
                            f"session:{key}",
                        )),
                        order_index=order_index,
                        stratum_id=stratum.stratum_id,
                        workload_id=workload_id,
                        design_id=design_id,
                        is_safe_design=(
                            design_id == preregistration.safe_design_id
                        ),
                        repetition=repetition,
                        seed=base_seed + order_index,
                    ))
                    order_index += 1

    keys = Counter(trial.trial_key for trial in trials)
    duplicates = sorted(key for key, count in keys.items() if count > 1)
    if duplicates:
        raise PilotRunnerError(
            "trial plan contains duplicate canonical cells: "
            + ", ".join(duplicates)
        )
    expected = preregistration.planned_trial_count
    if len(trials) != expected:
        raise PilotRunnerError(
            f"trial plan produced {len(trials)} cells, expected {expected}"
        )
    return tuple(trials)


def trial_plan_payload(
    preregistration: DistributedPilotPreregistration,
    trials: Iterable[DistributedTrial],
    *,
    workloads: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """The frozen plan document written once and checked on every resume.

    ``workloads`` supplies the definitions actually used by the plan so their
    content, not merely their ids, is bound to the run. It is optional only
    so an id-level plan can still be produced for inspection; a run that
    executes anything always supplies it.
    """
    ordered = list(trials)
    payload = {
        "schema_version": PILOT_PLAN_SCHEMA_VERSION,
        "pilot_id": preregistration.pilot_id,
        "preregistration_sha256": preregistration.source_sha256,
        "workload_id_manifest_sha256": (
            preregistration.workload_manifest_sha256
        ),
        "workload_content_sha256": (
            None
            if workloads is None
            else workload_content_sha256(
                workloads,
                preregistration.workload_ids,
            )
        ),
        "excluded_workload_manifest_sha256": (
            preregistration.excluded_workload_manifest_sha256
        ),
        "repetitions": preregistration.repetitions,
        "planned_trial_count": len(ordered),
        "independent_workload_count": (
            preregistration.independent_workload_count
        ),
        "independent_unit": "workload-object-cluster",
        "trials": [trial.to_public_dict() for trial in ordered],
    }
    payload["plan_sha256"] = sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    return payload


def ensure_frozen_plan(path: str | Path, payload: Mapping[str, Any]) -> None:
    """Write the plan once; afterwards require an exact match.

    This is the audit-safe resume gate. A changed preregistration, a changed
    workload manifest, or a changed plan all surface here as a refusal rather
    than as a silently mixed dataset.
    """
    target = Path(path)
    if not target.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return
    try:
        existing = json.loads(target.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise PilotResumeError(
            f"frozen pilot plan is unreadable: {target}"
        ) from exc
    for field, label in (
        ("preregistration_sha256", "preregistration configuration"),
        ("workload_id_manifest_sha256", "workload id manifest"),
        ("workload_content_sha256", "workload definitions"),
        ("excluded_workload_manifest_sha256", "excluded workload manifest"),
        ("plan_sha256", "trial plan"),
    ):
        if existing.get(field) != payload.get(field):
            raise PilotResumeError(
                f"the {label} changed since this run started "
                f"({field}); refusing to resume into a mixed dataset. "
                "Start a new pilot id instead."
            )
    if existing != dict(payload):
        raise PilotResumeError(
            "the frozen pilot plan differs from the recomputed plan; "
            "refusing to resume"
        )


@dataclass(frozen=True)
class ObservationOutcome:
    """One completed attempt against one canonical cell."""

    trial_key: str
    observation_class: str
    attempt: int
    succeeded: bool
    telemetry_complete: bool
    artifact_selected: bool
    artifact_delivery_complete: bool
    failure_class: str | None = None
    failure_detail: str | None = None

    def __post_init__(self) -> None:
        if self.observation_class not in OBSERVATION_CLASSES:
            raise PilotRunnerError(
                f"unknown observation class: {self.observation_class}"
            )

    @property
    def is_canonical(self) -> bool:
        return self.observation_class == "canonical"

    @property
    def usable(self) -> bool:
        """A cell counts only when complete in every declared respect."""
        if not self.succeeded or not self.telemetry_complete:
            return False
        if self.artifact_selected and not self.artifact_delivery_complete:
            return False
        return True


@dataclass
class PilotRunState:
    """Which canonical cells are done, and what else was attempted."""

    pilot_id: str
    planned_trial_keys: tuple[str, ...]
    completed: dict[str, ObservationOutcome]
    recovery_attempts: list[ObservationOutcome]
    infrastructure_failures: list[ObservationOutcome]
    max_attempts: int

    @property
    def remaining_trial_keys(self) -> tuple[str, ...]:
        return tuple(
            key for key in self.planned_trial_keys
            if key not in self.completed
        )

    @property
    def complete(self) -> bool:
        return not self.remaining_trial_keys

    def attempts_for(self, trial_key: str) -> int:
        return sum(
            1
            for outcome in (
                *self.recovery_attempts,
                *self.infrastructure_failures,
            )
            if outcome.trial_key == trial_key
        )

    def record(self, outcome: ObservationOutcome) -> None:
        """File one attempt into exactly one ledger."""
        if outcome.trial_key not in set(self.planned_trial_keys):
            raise PilotRunnerError(
                f"outcome references an unplanned cell: {outcome.trial_key}"
            )
        if outcome.is_canonical:
            if not outcome.usable:
                raise PilotRunnerError(
                    "an incomplete attempt cannot be filed as a canonical "
                    f"observation: {outcome.trial_key}"
                )
            if outcome.trial_key in self.completed:
                raise PilotRunnerError(
                    "duplicate canonical observation for cell: "
                    + outcome.trial_key
                )
            self.completed[outcome.trial_key] = outcome
            return
        if outcome.observation_class == "infrastructure":
            self.infrastructure_failures.append(outcome)
        else:
            self.recovery_attempts.append(outcome)
        if self.attempts_for(outcome.trial_key) > self.max_attempts:
            raise PilotRunnerError(
                f"cell {outcome.trial_key} exceeded its bounded retry "
                f"budget of {self.max_attempts} attempts"
            )

    def may_retry(self, trial_key: str) -> bool:
        if trial_key in self.completed:
            return False
        return self.attempts_for(trial_key) < self.max_attempts

    def complete_repetition_blocks(
        self,
        preregistration: DistributedPilotPreregistration,
    ) -> tuple[str, ...]:
        """Workloads whose *both* designs have a full repetition block."""
        usable: set[str] = set(self.completed)
        ready: list[str] = []
        for stratum in preregistration.strata:
            for workload_id in stratum.workload_ids:
                designs = (
                    preregistration.safe_design_id,
                    stratum.candidate_design_id,
                )
                if all(
                    _trial_key(
                        preregistration.pilot_id,
                        workload_id,
                        design_id,
                        repetition,
                    ) in usable
                    for design_id in designs
                    for repetition in range(preregistration.repetitions)
                ):
                    ready.append(workload_id)
        return tuple(ready)

    def require_complete_oracle(
        self,
        preregistration: DistributedPilotPreregistration,
    ) -> None:
        """Refuse to treat a partial run as a complete Oracle."""
        if self.complete:
            return
        missing = self.remaining_trial_keys
        raise IncompleteOracleError(
            f"{len(missing)} of {len(self.planned_trial_keys)} canonical "
            "cells are missing; a partial run is not a Reduced Oracle. "
            "First missing: " + ", ".join(missing[:3])
        )

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "schema_version": PILOT_RUN_STATE_SCHEMA_VERSION,
            "pilot_id": self.pilot_id,
            "planned_trial_count": len(self.planned_trial_keys),
            "completed_canonical_count": len(self.completed),
            "remaining_trial_count": len(self.remaining_trial_keys),
            "complete": self.complete,
            "recovery_attempt_count": len(self.recovery_attempts),
            "infrastructure_failure_count": len(
                self.infrastructure_failures
            ),
            "max_attempts_per_cell": self.max_attempts,
            "recovery_attempts_included_in_canonical_data": False,
            "worker_lifecycle_managed": False,
        }


def new_run_state(
    preregistration: DistributedPilotPreregistration,
    trials: Iterable[DistributedTrial],
    *,
    max_attempts: int = 3,
) -> PilotRunState:
    if not isinstance(max_attempts, int) or max_attempts < 1:
        raise PilotRunnerError("max_attempts must be a positive integer")
    return PilotRunState(
        pilot_id=preregistration.pilot_id,
        planned_trial_keys=tuple(trial.trial_key for trial in trials),
        completed={},
        recovery_attempts=[],
        infrastructure_failures=[],
        max_attempts=max_attempts,
    )
