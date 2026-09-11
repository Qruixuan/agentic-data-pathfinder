"""Resolve one cache-conditional container trial into a safe two-phase DAG.

FlowMesh API graphs are static.  A cache hit or miss, however, is learned at
run time.  Sending both branches to FlowMesh would perform both branches and
would therefore be scientifically and operationally wrong.  This module
resolves the frozen initial cache snapshot before any submission and produces
an explicit two-phase contract:

* phase A executes unconditional ancestors and cache lookups;
* phase B may run only after phase-A cache observations match the frozen
  outcome vector, and contains only the matching branch plus its successors.

It is a pure planning utility.  It neither starts a service nor submits a
workflow.  The later FlowMesh coordinator must use this exact resolution and
refuse phase B when a live lookup disagrees.
"""

from __future__ import annotations

import json
from typing import Any, Mapping, Sequence

from ...simulator.container_contract import CONTAINER_OPERATION_SCHEMA_VERSION
from .container_dag import (
    FlowMeshContainerDagError,
    _canonical_bytes,
    _require,
    _text,
    _validate_operation,
)


def _copy(value: Mapping[str, Any]) -> dict[str, Any]:
    try:
        copied = json.loads(_canonical_bytes(value).decode("utf-8"))
    except (TypeError, ValueError, UnicodeError, json.JSONDecodeError) as exc:
        raise FlowMeshContainerDagError(
            "conditional operation cannot be canonicalized"
        ) from exc
    _require(isinstance(copied, dict), "conditional operation must be an object")
    return copied


def _trial_operations(
    operations: Sequence[Mapping[str, Any]],
    trial_key: str,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]], dict[str, int]]:
    requested = _text(trial_key, "trial_key")
    selected: list[dict[str, Any]] = []
    for raw in operations:
        row = _validate_operation(raw)
        if row["trial_key"] == requested:
            _require(
                row.get("schema_version") == CONTAINER_OPERATION_SCHEMA_VERSION,
                "conditional DAG planning requires cache-scoped v1alpha2 operations",
            )
            selected.append(row)
    _require(selected, "conditional trial is absent from the operation ledger")
    by_key = {row["operation_key"]: row for row in selected}
    _require(len(by_key) == len(selected), "conditional trial operation keys are not unique")
    order = {row["operation_key"]: index for index, row in enumerate(selected)}
    for row in selected:
        for dependency in row["dependency_operation_keys"]:
            _require(
                dependency in by_key,
                "conditional trial dependency is absent or crosses a trial boundary",
            )
    return selected, by_key, order


def _lookup_rows(
    operations: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    rows = {
        row["operation_key"]: row
        for row in operations
        if row["operation_kind"] == "cache_lookup"
    }
    for key, row in rows.items():
        cache = row.get("cache_adapter")
        _require(isinstance(cache, Mapping), "cache lookup has no cache adapter")
        _text(row.get("cache_scope_id"), "cache lookup cache_scope_id")
        _text(cache.get("cache_id"), "cache lookup cache_id")
        _require(
            isinstance(cache.get("initial_entries"), list),
            "cache lookup has no frozen initial entries",
        )
        _require(key == row["operation_key"], "cache lookup key changed")
    return rows


def _ancestors(
    key: str,
    by_key: Mapping[str, Mapping[str, Any]],
) -> set[str]:
    result: set[str] = set()
    pending = list(by_key[key]["dependency_operation_keys"])
    while pending:
        current = pending.pop()
        if current in result:
            continue
        result.add(current)
        pending.extend(by_key[current]["dependency_operation_keys"])
    return result


def derive_initial_cache_outcomes(
    operations: Sequence[Mapping[str, Any]],
    *,
    trial_key: str,
) -> dict[str, str]:
    """Derive every lookup outcome from the frozen initial cache snapshot.

    A lookup with a preceding cache insert cannot be deterministically derived
    from the initial snapshot alone and is refused.  This protects the caller
    from accidentally treating a stateful cache trace as a fresh-cache trial.
    """

    selected, by_key, _ = _trial_operations(operations, trial_key)
    lookups = _lookup_rows(selected)
    _require(lookups, "trial has no cache lookups to resolve")
    outcomes: dict[str, str] = {}
    for lookup_key, row in sorted(lookups.items()):
        _require(
            not any(
                by_key[ancestor]["operation_kind"] == "cache_insert"
                for ancestor in _ancestors(lookup_key, by_key)
            ),
            "cache lookup follows a cache insert and cannot be derived from "
            "the frozen initial cache snapshot",
        )
        cache = row["cache_adapter"]
        assert isinstance(cache, Mapping)
        object_id = _text(row.get("object_id"), "cache lookup object_id")
        representation_id = _text(
            row.get("representation_id"), "cache lookup representation_id"
        )
        initial_entries = cache["initial_entries"]
        assert isinstance(initial_entries, list)
        found = False
        for entry in initial_entries:
            _require(isinstance(entry, Mapping), "cache initial entry is invalid")
            if (
                entry.get("object_id") == object_id
                and entry.get("representation_id") == representation_id
            ):
                found = True
                break
        outcomes[lookup_key] = "hit" if found else "miss"
    return outcomes


def _validate_outcomes(
    expected: Mapping[str, str],
    *,
    derived: Mapping[str, str],
) -> dict[str, str]:
    _require(
        set(expected) == set(derived),
        "cache outcome vector must name exactly every cache lookup in the trial",
    )
    normalized: dict[str, str] = {}
    for key, raw in expected.items():
        lookup_key = _text(key, "cache outcome lookup key")
        outcome = _text(raw, f"cache outcome for {lookup_key}")
        _require(outcome in {"hit", "miss"}, "cache outcome must be hit or miss")
        _require(
            outcome == derived[lookup_key],
            "cache outcome disagrees with the frozen initial cache snapshot",
        )
        normalized[lookup_key] = outcome
    return dict(sorted(normalized.items()))


def _active_rows(
    operations: Sequence[Mapping[str, Any]],
    by_key: Mapping[str, Mapping[str, Any]],
    lookups: Mapping[str, Mapping[str, Any]],
    outcomes: Mapping[str, str],
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    active: dict[str, dict[str, Any]] = {}
    inactive: list[str] = []
    for raw in operations:
        row = _copy(raw)
        condition = row.get("condition")
        if row["operation_kind"] == "cache_read":
            _require(
                isinstance(condition, Mapping),
                "cache read must be gated by a cache lookup",
            )
        if condition is None:
            active[row["operation_key"]] = row
            continue
        _require(isinstance(condition, Mapping), "conditional operation condition is invalid")
        lookup_key = _text(
            condition.get("cache_operation_key"),
            "conditional operation cache lookup key",
        )
        _require(
            lookup_key in outcomes,
            "conditional operation names an outcome outside the frozen vector",
        )
        lookup = lookups[lookup_key]
        _require(
            condition.get("cache_operation_id") == lookup.get("operation_id"),
            "conditional operation cache operation ID does not match its lookup",
        )
        _require(
            lookup_key in _ancestors(row["operation_key"], by_key),
            "conditional operation does not depend on its cache lookup",
        )
        if row["operation_kind"] == "cache_read":
            read_cache = row.get("cache_adapter")
            lookup_cache = lookup.get("cache_adapter")
            _require(
                isinstance(read_cache, Mapping)
                and isinstance(lookup_cache, Mapping),
                "cache read or its lookup has no cache adapter",
            )
            _require(
                read_cache.get("cache_id") == lookup_cache.get("cache_id"),
                "cache read and its lookup use different cache IDs",
            )
            _require(
                _text(row.get("cache_scope_id"), "cache read cache_scope_id")
                == _text(
                    lookup.get("cache_scope_id"),
                    "cache lookup cache_scope_id",
                ),
                "cache read and its lookup use different cache scopes",
            )
        equals = condition.get("equals")
        _require(equals in {"hit", "miss"}, "conditional operation outcome is invalid")
        if outcomes[lookup_key] == equals:
            active[row["operation_key"]] = row
        else:
            inactive.append(row["operation_key"])
    return active, sorted(inactive)


def _resolved_dependencies(
    active: Mapping[str, Mapping[str, Any]],
    all_rows: Mapping[str, Mapping[str, Any]],
    outcomes: Mapping[str, str],
) -> tuple[dict[str, list[str]], list[dict[str, str]]]:
    dependencies: dict[str, list[str]] = {}
    omitted: list[dict[str, str]] = []
    for operation_key, row in active.items():
        current: list[str] = []
        for dependency in row["dependency_operation_keys"]:
            if dependency in active:
                current.append(dependency)
                continue
            omitted_row = all_rows[dependency]
            condition = omitted_row.get("condition")
            _require(
                isinstance(condition, Mapping),
                "active operation omits an unconditional dependency",
            )
            lookup_key = _text(
                condition.get("cache_operation_key"),
                "omitted dependency cache lookup key",
            )
            equals = condition.get("equals")
            _require(
                lookup_key in outcomes
                and equals in {"hit", "miss"}
                and outcomes[lookup_key] != equals,
                "active operation omits a dependency not excluded by the frozen outcome",
            )
            omitted.append(
                {
                    "operation_key": operation_key,
                    "omitted_dependency_operation_key": dependency,
                    "cache_operation_key": lookup_key,
                    "observed_cache_outcome": outcomes[lookup_key],
                    "omitted_dependency_condition_equals": str(equals),
                }
            )
        dependencies[operation_key] = current
    return dependencies, sorted(
        omitted,
        key=lambda row: (
            row["operation_key"],
            row["omitted_dependency_operation_key"],
        ),
    )


def _topological_order(
    active: Mapping[str, Mapping[str, Any]],
    dependencies: Mapping[str, Sequence[str]],
    source_order: Mapping[str, int],
) -> list[str]:
    remaining = {key: set(values) for key, values in dependencies.items()}
    result: list[str] = []
    while remaining:
        ready = sorted(
            (key for key, values in remaining.items() if not values),
            key=lambda key: source_order[key],
        )
        _require(ready, "resolved conditional DAG contains a cycle")
        for key in ready:
            result.append(key)
            remaining.pop(key)
        for values in remaining.values():
            values.difference_update(ready)
    _require(set(result) == set(active), "resolved conditional DAG lost an operation")
    return result


def _phase_a_keys(
    lookups: Mapping[str, Mapping[str, Any]],
    active: Mapping[str, Mapping[str, Any]],
    source_rows: Mapping[str, Mapping[str, Any]],
) -> set[str]:
    phase_a: set[str] = set()
    for lookup_key in lookups:
        phase_a.add(lookup_key)
        phase_a.update(_ancestors(lookup_key, source_rows))
    _require(
        phase_a <= set(active),
        "a cache lookup has an inactive predecessor",
    )
    _require(
        all(active[key].get("condition") is None for key in phase_a),
        "phase-A cache lookup path is conditional",
    )
    return phase_a


def resolve_conditional_container_trial(
    operations: Sequence[Mapping[str, Any]],
    *,
    trial_key: str,
    cache_outcomes: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Resolve a cache-conditional trial into frozen phase-A and phase-B DAGs.

    When ``cache_outcomes`` is omitted, outcomes are derived solely from the
    frozen initial snapshot.  Supplying outcomes is useful for a plan parser,
    but it must be an exact match to that derivation; arbitrary operator
    choices are rejected.
    """

    selected, by_key, source_order = _trial_operations(operations, trial_key)
    lookups = _lookup_rows(selected)
    _require(lookups, "trial has no cache lookup; use the unconditional planner")
    derived = derive_initial_cache_outcomes(selected, trial_key=trial_key)
    outcomes = (
        dict(sorted(derived.items()))
        if cache_outcomes is None
        else _validate_outcomes(cache_outcomes, derived=derived)
    )
    active, inactive = _active_rows(selected, by_key, lookups, outcomes)
    dependencies, omitted = _resolved_dependencies(active, by_key, outcomes)
    order = _topological_order(active, dependencies, source_order)
    phase_a = _phase_a_keys(lookups, active, by_key)
    phase_a_order = [key for key in order if key in phase_a]
    phase_b_order = [key for key in order if key not in phase_a]
    phase_b_dependencies: dict[str, list[str]] = {}
    phase_a_satisfied: list[dict[str, str]] = []
    for key in phase_b_order:
        local_dependencies: list[str] = []
        for dependency in dependencies[key]:
            if dependency in phase_a:
                phase_a_satisfied.append(
                    {
                        "operation_key": key,
                        "phase_a_dependency_operation_key": dependency,
                    }
                )
            else:
                local_dependencies.append(dependency)
        phase_b_dependencies[key] = local_dependencies

    terminal = [
        key
        for key in phase_b_order
        if active[key]["operation_kind"] == "compute"
        and not any(key in values for values in dependencies.values())
    ]
    _require(
        len(terminal) == 1,
        "resolved conditional DAG must have exactly one terminal compute operation",
    )
    cache_scopes = sorted(
        {
            _text(row.get("cache_scope_id"), "cache lookup cache_scope_id")
            for row in lookups.values()
        }
    )
    return {
        "trial_key": _text(trial_key, "trial_key"),
        "cache_outcomes": outcomes,
        "cache_scope_ids": cache_scopes,
        "derived_from_frozen_initial_cache_snapshot": True,
        "phase_a_operation_keys": phase_a_order,
        "phase_b_operation_keys": phase_b_order,
        "active_operation_keys": order,
        "inactive_operation_keys": inactive,
        "resolved_dependency_operation_keys": {
            key: dependencies[key] for key in order
        },
        "phase_b_dependency_operation_keys": {
            key: phase_b_dependencies[key] for key in phase_b_order
        },
        "omitted_inverse_branch_dependencies": omitted,
        "phase_a_satisfied_dependencies": sorted(
            phase_a_satisfied,
            key=lambda row: (
                row["operation_key"],
                row["phase_a_dependency_operation_key"],
            ),
        ),
        "terminal_operation_key": terminal[0],
        "workflow_submitted": False,
        "services_started": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def list_conditional_container_trial_candidates(
    operations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """List every cache-conditional trial and its snapshot-derived outcome."""

    trial_keys = sorted({str(row.get("trial_key")) for row in operations})
    candidates: list[dict[str, Any]] = []
    for trial_key in trial_keys:
        selected, _, _ = _trial_operations(operations, trial_key)
        if not _lookup_rows(selected):
            continue
        # A malformed cache-conditional trial must fail the listing instead of
        # disappearing from it.  Otherwise an operator might mistake an
        # incomplete candidate list for a safe workload subset.
        resolution = resolve_conditional_container_trial(
            operations,
            trial_key=trial_key,
        )
        candidates.append(
            {
                "trial_key": resolution["trial_key"],
                "cache_outcomes": resolution["cache_outcomes"],
                "cache_scope_ids": resolution["cache_scope_ids"],
                "active_operation_count": len(resolution["active_operation_keys"]),
                "inactive_operation_count": len(resolution["inactive_operation_keys"]),
                "terminal_operation_key": resolution["terminal_operation_key"],
            }
        )
    return candidates
