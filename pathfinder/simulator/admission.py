"""Shared trial-admission semantics for simulated and measured backends."""

from __future__ import annotations

from typing import Any, Mapping


TRIAL_ADMISSION_SCHEMA_VERSION = "pathfinder.trial-admission-contract/v1alpha1"
TRIAL_ADMISSION_ALGORITHM = "fifo-by-arrival-time-then-order-index"
TRIAL_LATENCY_ORIGIN = "planned-trial-arrival"


class TrialAdmissionError(ValueError):
    """Raised when a frozen trial-admission contract is malformed."""


def trial_admission_contract(slots: int) -> dict[str, Any]:
    """Return the canonical backend-neutral global admission contract."""

    if type(slots) is not int or slots <= 0:
        raise TrialAdmissionError("trial admission slots must be a positive integer")
    return {
        "schema_version": TRIAL_ADMISSION_SCHEMA_VERSION,
        "scope": "whole-trial",
        "algorithm": TRIAL_ADMISSION_ALGORITHM,
        "slots": slots,
        "latency_origin": TRIAL_LATENCY_ORIGIN,
        "admission_queue_metric": "trial_admission_queue_ms",
        "active_execution_metric": "active_execution_latency_ms",
        "resource_queue_excludes_trial_admission": True,
        "slot_released_after_all_trial_operations_complete": True,
    }


def validate_trial_admission_contract(
    value: Any,
    *,
    planned_trial_count: int,
) -> int:
    """Validate a canonical contract and return its frozen slot count."""

    if not isinstance(value, Mapping):
        raise TrialAdmissionError("trial_admission must be an object")
    slots = value.get("slots")
    if type(slots) is not int or not 1 <= slots <= planned_trial_count:
        raise TrialAdmissionError(
            "trial_admission.slots must be between 1 and planned_trial_count"
        )
    if dict(value) != trial_admission_contract(slots):
        raise TrialAdmissionError("trial_admission contract is not canonical")
    return slots
