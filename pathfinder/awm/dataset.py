from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from ..config import load_config
from ..integrations.flowmesh.analysis import audit_pilot_records
from ..integrations.flowmesh.pilot import (
    load_flowmesh_pilot_config,
    load_pilot_records,
)
from ..models import QuoteProfile, SystemConfig, TaskClass
from ..reduced_oracle.contracts import ReducedOracleConfig
from .contracts import AWMConfig, AWMConfigError


@dataclass(frozen=True)
class DesignSample:
    design_id: str
    attempted_sessions: int
    eligible_sessions: int
    access_counts: dict[str, int]
    group_access_counts: dict[str, int]
    success_count: int
    service_costs: tuple[float, ...]
    repetitions: tuple[int, ...]

    @property
    def excluded_sessions(self) -> int:
        return self.attempted_sessions - self.eligible_sessions


@dataclass(frozen=True)
class AWMDataset:
    model_config: AWMConfig
    oracle_config: ReducedOracleConfig
    system: SystemConfig
    task_class: TaskClass
    quote_profile: QuoteProfile
    design_ids: tuple[str, ...]
    representation_ids: tuple[str, ...]
    training: dict[str, DesignSample]
    holdout: dict[str, DesignSample]
    storage_costs: dict[str, float]
    forward_transition_costs: dict[str, float]
    restoration_transition_costs: dict[str, float]

    @property
    def horizon_sessions(self) -> int:
        return self.oracle_config.horizon_sessions

    def quote(self, design_id: str, representation_id: str) -> float:
        path = self.system.designs[design_id].paths[representation_id]
        return self.quote_profile.quote_for(
            self.task_class.id,
            representation_id,
            path.quotes[self.task_class.id],
        )

    def affordable(self, design_id: str, representation_id: str) -> bool:
        path = self.system.designs[design_id].paths[representation_id]
        return path.available and self.quote(
            design_id,
            representation_id,
        ) <= self.task_class.access_budget


def _group_key(group: tuple[str, ...]) -> str:
    return "+".join(group)


def _eligible_record(record: dict[str, Any]) -> bool:
    return (
        record.get("outcome_type") == "completed"
        and record.get("telemetry_complete") is True
        and record.get("task_success") is not None
    )


def _sample(
    design_id: str,
    records: Iterable[dict[str, Any]],
    *,
    representation_ids: tuple[str, ...],
    groups: tuple[tuple[str, ...], ...],
) -> DesignSample:
    rows = list(records)
    eligible = [record for record in rows if _eligible_record(record)]
    access_counts = {representation_id: 0 for representation_id in representation_ids}
    group_access_counts = {_group_key(group): 0 for group in groups}
    successes = 0
    costs: list[float] = []
    repetitions: set[int] = set()
    for record in eligible:
        repetitions.add(int(record["repetition"]))
        selected = {
            str(value) for value in record.get("selected_representations", [])
        }
        for representation_id in representation_ids:
            access_counts[representation_id] += int(
                representation_id in selected
            )
        for group in groups:
            group_access_counts[_group_key(group)] += int(
                bool(selected.intersection(group))
            )
        successes += int(bool(record["task_success"]))
        costs.append(
            sum(
                float(event.get("realized_cost", 0.0))
                for event in record.get("access_events", [])
                if event.get("accepted")
            )
        )
    return DesignSample(
        design_id=design_id,
        attempted_sessions=len(rows),
        eligible_sessions=len(eligible),
        access_counts=access_counts,
        group_access_counts=group_access_counts,
        success_count=successes,
        service_costs=tuple(costs),
        repetitions=tuple(sorted(repetitions)),
    )


def _load_oracle_table(path: Path) -> dict[str, dict[str, str]]:
    if not path.is_file():
        raise AWMConfigError(
            "Reduced Oracle analysis is missing oracle_table.csv: "
            f"{path}"
        )
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        required = {
            "design_id",
            "storage_cost",
            "forward_transition_cost",
            "restoration_cost",
        }
        missing = required - set(reader.fieldnames or ())
        if missing:
            raise AWMConfigError(
                "Reduced Oracle table is missing required columns: "
                + ", ".join(sorted(missing))
            )
    result = {str(row.get("design_id")): row for row in rows}
    if not result or "" in result:
        raise AWMConfigError(f"invalid Reduced Oracle table: {path}")
    return result


def _excluded_fraction(sample: DesignSample) -> float:
    return (
        sample.excluded_sessions / sample.attempted_sessions
        if sample.attempted_sessions
        else 0.0
    )


def load_oracle_dataset(
    model_config: AWMConfig,
    oracle_config: ReducedOracleConfig,
    *,
    oracle_output_dir: str | Path,
) -> AWMDataset:
    """Load a paired train/holdout view without modifying Oracle output."""
    pilot = load_flowmesh_pilot_config(
        oracle_config.workload_pilot_config_path
    )
    system = load_config(pilot.system_config_path)
    task_class = system.task_classes[pilot.task_class_id]
    if task_class.max_accesses != 1:
        raise AWMConfigError(
            "AWM v1alpha1 requires max_accesses=1 for its exact analytic "
            "envelope solver"
        )
    unknown_designs = set(model_config.observed_design_ids) - set(
        oracle_config.design_ids
    )
    if unknown_designs:
        raise AWMConfigError(
            "observed_design_ids are absent from the Reduced Oracle: "
            + ", ".join(sorted(unknown_designs))
        )
    if model_config.holdout_repetitions >= oracle_config.repetitions:
        raise AWMConfigError(
            "holdout_repetitions must leave at least one training repetition"
        )
    if model_config.holdout_repetitions <= 0:
        raise AWMConfigError(
            "evaluation requires at least one holdout repetition"
        )
    known_representations = set(task_class.candidate_representations)
    for group in model_config.substitution_groups:
        unknown = set(group) - known_representations
        if unknown:
            raise AWMConfigError(
                "substitution group contains unknown representations: "
                + ", ".join(sorted(unknown))
            )

    output = Path(oracle_output_dir).resolve()
    table = _load_oracle_table(output / "oracle_table.csv")
    holdout_start = oracle_config.repetitions - model_config.holdout_repetitions
    training: dict[str, DesignSample] = {}
    holdout: dict[str, DesignSample] = {}
    representation_ids = task_class.candidate_representations
    for design_id in oracle_config.design_ids:
        records = audit_pilot_records(
            load_pilot_records(
                output / "designs" / design_id / "runs.jsonl",
                repair_truncated_tail=False,
            )
        )
        for record in records:
            if record.get("design_id") != design_id:
                raise AWMConfigError(
                    f"Oracle record has wrong design_id in {design_id}"
                )
            if record.get("task_class_id") != pilot.task_class_id:
                raise AWMConfigError("Oracle record has wrong task_class_id")
            if record.get("quote_profile_id") != oracle_config.quote_profile_id:
                raise AWMConfigError("Oracle record has wrong quote_profile_id")
        train_rows = [
            record
            for record in records
            if int(record["repetition"]) < holdout_start
            and design_id in model_config.observed_design_ids
        ]
        holdout_rows = [
            record
            for record in records
            if int(record["repetition"]) >= holdout_start
        ]
        training[design_id] = _sample(
            design_id,
            train_rows,
            representation_ids=representation_ids,
            groups=model_config.substitution_groups,
        )
        holdout[design_id] = _sample(
            design_id,
            holdout_rows,
            representation_ids=representation_ids,
            groups=model_config.substitution_groups,
        )
        if (
            design_id in model_config.observed_design_ids
            and training[design_id].eligible_sessions
            < model_config.minimum_training_sessions
        ):
            raise AWMConfigError(
                f"{design_id} has only "
                f"{training[design_id].eligible_sessions} eligible training "
                "sessions"
            )
        if (
            design_id in model_config.observed_design_ids
            and _excluded_fraction(training[design_id])
            > model_config.maximum_excluded_training_fraction
        ):
            raise AWMConfigError(
                f"{design_id} exceeds maximum excluded training fraction"
            )
        if holdout[design_id].eligible_sessions == 0:
            raise AWMConfigError(
                f"{design_id} has no eligible holdout sessions"
            )
        if (
            _excluded_fraction(holdout[design_id])
            > model_config.maximum_excluded_holdout_fraction
        ):
            raise AWMConfigError(
                f"{design_id} exceeds maximum excluded holdout fraction"
            )

    missing_rows = set(oracle_config.design_ids) - set(table)
    if missing_rows:
        raise AWMConfigError(
            "oracle_table.csv is missing designs: "
            + ", ".join(sorted(missing_rows))
        )
    return AWMDataset(
        model_config=model_config,
        oracle_config=oracle_config,
        system=system,
        task_class=task_class,
        quote_profile=system.quote_profiles[oracle_config.quote_profile_id],
        design_ids=oracle_config.design_ids,
        representation_ids=representation_ids,
        training=training,
        holdout=holdout,
        storage_costs={
            design_id: float(table[design_id]["storage_cost"])
            for design_id in oracle_config.design_ids
        },
        forward_transition_costs={
            design_id: float(table[design_id]["forward_transition_cost"])
            for design_id in oracle_config.design_ids
        },
        restoration_transition_costs={
            design_id: float(table[design_id]["restoration_cost"])
            for design_id in oracle_config.design_ids
        },
    )
