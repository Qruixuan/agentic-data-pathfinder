from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from statistics import mean
from typing import Any, Iterable

from ..config import load_config
from ..integrations.flowmesh.analysis import audit_pilot_records
from ..integrations.flowmesh.pilot import (
    _write_csv,
    _write_json,
    load_flowmesh_pilot_config,
    load_pilot_records,
)
from ..models import SystemConfig
from .contracts import ReducedOracleConfig, load_reduced_oracle_config


ORACLE_ANALYSIS_SCHEMA_VERSION = (
    "pathfinder.reduced-oracle-analysis/v1alpha1"
)


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"invalid JSONL at {path}:{line_number}: {exc}"
            ) from exc
        if not isinstance(row, dict):
            raise RuntimeError(
                f"JSONL entry at {path}:{line_number} is not an object"
            )
        rows.append(row)
    return rows


def _transition_totals(
    events: Iterable[dict[str, Any]],
    *,
    from_design_id: str,
    to_design_id: str,
    transition_type: str,
) -> dict[str, Any]:
    matching = [
        event
        for event in events
        if event.get("from_design_id") == from_design_id
        and event.get("to_design_id") == to_design_id
        and event.get("transition_type") == transition_type
    ]
    return {
        "count": len(matching),
        "cost": round(
            sum(
                float(event.get("realized_transition_cost", 0.0))
                for event in matching
            ),
            9,
        ),
        "elapsed_seconds": round(
            sum(float(event.get("elapsed_seconds", 0.0)) for event in matching),
            9,
        ),
        "copied_bytes": sum(int(event.get("copied_bytes", 0)) for event in matching),
        "active_materialized_bytes": max(
            (int(event.get("active_materialized_bytes", 0)) for event in matching),
            default=0,
        ),
    }


def _design_row(
    config: ReducedOracleConfig,
    system: SystemConfig,
    design_id: str,
    records: list[dict[str, Any]],
    transition_events: list[dict[str, Any]],
) -> dict[str, Any]:
    audited = audit_pilot_records(records)
    completed = [
        record for record in audited if record.get("outcome_type") == "completed"
    ]
    evaluable = [
        record for record in completed if record.get("task_success") is not None
    ]
    accepted_events = [
        event
        for record in completed
        for event in record.get("access_events", [])
        if event.get("accepted")
    ]
    selections = Counter(
        str(event["representation_id"])
        for event in accepted_events
        if event.get("representation_id") is not None
    )
    completion_rate = len(completed) / len(audited) if audited else 0.0
    telemetry_complete = all(
        record.get("telemetry_complete") is True for record in completed
    )
    objective_evaluable = (
        bool(audited)
        and completion_rate >= config.minimum_completion_rate
        and len(evaluable) == len(completed)
        and telemetry_complete
    )
    success_rate = (
        mean(float(bool(record["task_success"])) for record in evaluable)
        if evaluable
        else None
    )
    mean_service_cost = (
        sum(float(event.get("realized_cost", 0.0)) for event in accepted_events)
        / len(completed)
        if completed
        else None
    )
    mean_task_value = None
    if evaluable:
        mean_task_value = mean(
            system.task_classes[str(record["task_class_id"])].task_value
            * float(bool(record["task_success"]))
            for record in evaluable
        )

    safe_design_id = config.safe_design_id
    if design_id == safe_design_id:
        forward = {
            "count": 0,
            "cost": 0.0,
            "elapsed_seconds": 0.0,
            "copied_bytes": 0,
            "active_materialized_bytes": 0,
        }
        restore = dict(forward)
    else:
        forward = _transition_totals(
            transition_events,
            from_design_id=safe_design_id,
            to_design_id=design_id,
            transition_type="forward",
        )
        restore = _transition_totals(
            transition_events,
            from_design_id=design_id,
            to_design_id=safe_design_id,
            transition_type="restore",
        )
    materialized_bytes = int(forward["active_materialized_bytes"])
    design = config.designs[design_id]
    storage_cost = (
        design.fixed_storage_cost
        + materialized_bytes
        / (1024**3)
        * config.horizon_hours
        * config.transition_cost.storage_cost_per_gib_hour
    )
    phi = None
    if (
        objective_evaluable
        and mean_task_value is not None
        and mean_service_cost is not None
    ):
        mean_session_value = (
            mean_task_value
            - system.resource_cost_weight * mean_service_cost
        )
        phi = config.horizon_sessions * mean_session_value - storage_cost
    row = {
        "design_id": design_id,
        "materialization_decision": design.materialization_decision,
        "placement_decision": design.placement_decision,
        "execution_decision": design.execution_decision,
        "recorded_trials": len(audited),
        "completed_trials": len(completed),
        "evaluable_task_count": len(evaluable),
        "completion_rate": round(completion_rate, 6),
        "telemetry_complete": telemetry_complete,
        "objective_evaluable": objective_evaluable,
        "outcome_counts_json": json.dumps(
            dict(sorted(Counter(str(r.get("outcome_type")) for r in audited).items())),
            sort_keys=True,
        ),
        "task_success_rate": (
            round(success_rate, 6) if success_rate is not None else None
        ),
        "mean_realized_service_cost_per_session": (
            round(mean_service_cost, 9)
            if mean_service_cost is not None
            else None
        ),
        "selection_rates_json": json.dumps(
            {
                representation_id: round(count / len(completed), 6)
                for representation_id, count in sorted(selections.items())
            }
            if completed
            else {},
            sort_keys=True,
        ),
        "materialized_bytes": materialized_bytes,
        "storage_cost": round(storage_cost, 9),
        "forward_transition_count": forward["count"],
        "forward_transition_cost": forward["cost"],
        "forward_transition_seconds": forward["elapsed_seconds"],
        "forward_copied_bytes": forward["copied_bytes"],
        "restoration_count": restore["count"],
        "restoration_cost": restore["cost"],
        "restoration_seconds": restore["elapsed_seconds"],
        "phi": round(phi, 9) if phi is not None else None,
    }
    row["transition_adjusted_phi"] = (
        round(float(phi) - float(forward["cost"]), 9)
        if phi is not None
        else None
    )
    row["probe_adjusted_phi"] = (
        round(float(phi) - float(forward["cost"]) - float(restore["cost"]), 9)
        if phi is not None
        else None
    )
    return row


def _selection_rate(row: dict[str, Any], representation_id: str) -> float:
    values = json.loads(str(row["selection_rates_json"]))
    return float(values.get(representation_id, 0.0))


def _lock_in_trace(
    config: ReducedOracleConfig,
    system: SystemConfig,
    rows: list[dict[str, Any]],
    *,
    oracle_design_id: str | None,
    transition_aware_design_id: str | None,
) -> dict[str, Any]:
    by_design = {str(row["design_id"]): row for row in rows}
    safe = by_design[config.safe_design_id]
    candidate_id = config.naive_baseline.candidate_design_id
    candidate = by_design[candidate_id]
    representation_id = config.naive_baseline.representation_id
    observed_access_rate = _selection_rate(safe, representation_id)
    safe_cost = system.designs[config.safe_design_id].paths[
        representation_id
    ].realized_cost
    candidate_cost = system.designs[candidate_id].paths[
        representation_id
    ].realized_cost
    observed_service_savings = (
        config.horizon_sessions
        * observed_access_rate
        * max(0.0, safe_cost - candidate_cost)
        * system.resource_cost_weight
    )
    naive_gain = (
        observed_service_savings
        - float(candidate["storage_cost"])
        - float(candidate["forward_transition_cost"])
        - config.naive_baseline.decision_margin
    )
    naive_design_id = candidate_id if naive_gain > 0 else config.safe_design_id
    oracle_gain = None
    if safe.get("transition_adjusted_phi") is not None and candidate.get(
        "transition_adjusted_phi"
    ) is not None:
        oracle_gain = float(candidate["transition_adjusted_phi"]) - float(
            safe["transition_adjusted_phi"]
        )
    lock_in = (
        naive_design_id == config.safe_design_id
        and transition_aware_design_id == candidate_id
        and oracle_gain is not None
        and oracle_gain > config.naive_baseline.decision_margin
    )
    return {
        "schema_version": ORACLE_ANALYSIS_SCHEMA_VERSION,
        "starting_design_id": config.safe_design_id,
        "candidate_design_id": candidate_id,
        "representation_id": representation_id,
        "observed_incumbent_access_rate": observed_access_rate,
        "observed_service_savings": round(observed_service_savings, 9),
        "forward_transition_cost": candidate["forward_transition_cost"],
        "candidate_storage_cost": candidate["storage_cost"],
        "decision_margin": config.naive_baseline.decision_margin,
        "naive_estimated_gain": round(naive_gain, 9),
        "naive_final_design_id": naive_design_id,
        "steady_state_oracle_design_id": oracle_design_id,
        "transition_aware_oracle_design_id": transition_aware_design_id,
        "oracle_candidate_gain_over_safe": (
            round(oracle_gain, 9) if oracle_gain is not None else None
        ),
        "self_confirming_lock_in_observed": lock_in,
        "interpretation": (
            "The naive policy uses only incumbent demand and serving-cost "
            "savings; it does not observe the candidate-induced task value."
        ),
    }


def analyze_reduced_oracle(
    config: ReducedOracleConfig | str | Path,
    *,
    output_dir: str | Path,
) -> dict[str, Any]:
    resolved = (
        load_reduced_oracle_config(config)
        if isinstance(config, (str, Path))
        else config
    )
    output = Path(output_dir).resolve()
    workload_pilot = load_flowmesh_pilot_config(
        resolved.workload_pilot_config_path
    )
    system = load_config(workload_pilot.system_config_path)
    transitions = _load_jsonl(output / "transitions.jsonl")
    rows: list[dict[str, Any]] = []
    for design_id in resolved.design_ids:
        records = load_pilot_records(
            output / "designs" / design_id / "runs.jsonl",
            repair_truncated_tail=False,
        )
        rows.append(
            _design_row(
                resolved,
                system,
                design_id,
                records,
                transitions,
            )
        )
    eligible = [row for row in rows if row["objective_evaluable"]]
    steady_state_oracle = (
        max(eligible, key=lambda row: float(row["phi"]))["design_id"]
        if eligible
        else None
    )
    transition_aware_oracle = (
        max(
            eligible,
            key=lambda row: float(row["transition_adjusted_phi"]),
        )["design_id"]
        if eligible
        else None
    )
    lock_in = _lock_in_trace(
        resolved,
        system,
        rows,
        oracle_design_id=(
            str(steady_state_oracle) if steady_state_oracle is not None else None
        ),
        transition_aware_design_id=(
            str(transition_aware_oracle)
            if transition_aware_oracle is not None
            else None
        ),
    )
    table_path = output / "oracle_table.csv"
    summary_path = output / "oracle_summary.json"
    lock_in_path = output / "lock_in_trace.json"
    _write_csv(rows, table_path)
    _write_json(lock_in, lock_in_path)
    summary = {
        "schema_version": ORACLE_ANALYSIS_SCHEMA_VERSION,
        "oracle_id": resolved.oracle_id,
        "design_count": len(rows),
        "eligible_design_count": len(eligible),
        "cost_model_status": resolved.cost_model_status,
        "steady_state_oracle_design_id": steady_state_oracle,
        "transition_aware_oracle_design_id": transition_aware_oracle,
        "self_confirming_lock_in_observed": lock_in[
            "self_confirming_lock_in_observed"
        ],
        "oracle_table_path": str(table_path),
        "lock_in_trace_path": str(lock_in_path),
        "transition_records_path": str(output / "transitions.jsonl"),
        "oracle_summary_path": str(summary_path),
        "safe_design_restored": (
            json.loads(
                (output / "runtime" / "active_plan.json").read_text(
                    encoding="utf-8"
                )
            ).get("active_design_id")
            == resolved.safe_design_id
            if (output / "runtime" / "active_plan.json").exists()
            else False
        ),
        "secrets_recorded": False,
    }
    _write_json(summary, summary_path)
    return summary
