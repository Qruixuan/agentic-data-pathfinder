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
class TrialObservation:
    pairing_key: str
    cluster_key: str
    repetition: int
    seed: int
    success: int
    service_cost: float
    selected_representation_id: str | None


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
    observations: tuple[TrialObservation, ...]

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
    holdout_by_repetition: dict[int, dict[str, DesignSample]]
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


def _pairing_key(record: dict[str, Any]) -> str:
    workload_id = record.get("workload_id")
    task_class_id = record.get("task_class_id")
    quote_profile_id = record.get("quote_profile_id")
    repetition = record.get("repetition")
    seed = record.get("seed")
    latency_multiplier = record.get("latency_multiplier")
    if not isinstance(workload_id, str) or not workload_id:
        raise AWMConfigError(
            "paired AWM record is missing a non-empty workload_id"
        )
    if not isinstance(task_class_id, str) or not task_class_id:
        raise AWMConfigError(
            "paired AWM record is missing a non-empty task_class_id"
        )
    if not isinstance(quote_profile_id, str) or not quote_profile_id:
        raise AWMConfigError(
            "paired AWM record is missing a non-empty quote_profile_id"
        )
    if isinstance(repetition, bool) or not isinstance(repetition, int):
        raise AWMConfigError(
            "paired AWM record is missing an integer repetition"
        )
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise AWMConfigError(
            "paired AWM record is missing an integer seed"
        )
    if isinstance(latency_multiplier, bool) or not isinstance(
        latency_multiplier,
        (int, float),
    ):
        raise AWMConfigError(
            "paired AWM record is missing a numeric latency_multiplier"
        )
    return "|".join(
        (
            workload_id,
            task_class_id,
            quote_profile_id,
            format(float(latency_multiplier), ".12g"),
            f"r{repetition:04d}",
            f"s{seed}",
        )
    )


def _cluster_key(
    record: dict[str, Any],
    fields: tuple[str, ...],
) -> str:
    values: list[str] = []
    for field in fields:
        value = record.get(field)
        if not isinstance(value, str) or not value:
            raise AWMConfigError(
                f"clustered AWM record is missing a non-empty {field}"
            )
        values.append(value)
    return "|".join(values)


def _sample(
    design_id: str,
    records: Iterable[dict[str, Any]],
    *,
    representation_ids: tuple[str, ...],
    groups: tuple[tuple[str, ...], ...],
    require_pairing: bool,
    cluster_key_fields: tuple[str, ...],
) -> DesignSample:
    rows = list(records)
    eligible = [record for record in rows if _eligible_record(record)]
    access_counts = {representation_id: 0 for representation_id in representation_ids}
    group_access_counts = {_group_key(group): 0 for group in groups}
    successes = 0
    costs: list[float] = []
    repetitions: set[int] = set()
    observations: list[TrialObservation] = []
    pairing_keys: set[str] = set()
    for record in eligible:
        repetitions.add(int(record["repetition"]))
        selected = {
            str(value) for value in record.get("selected_representations", [])
        }
        if len(selected) > 1:
            raise AWMConfigError(
                "paired AWM requires at most one selected representation "
                f"for {design_id}"
            )
        unknown_selected = selected - set(representation_ids)
        if unknown_selected:
            raise AWMConfigError(
                "paired AWM record selected an unknown representation for "
                f"{design_id}: " + ", ".join(sorted(unknown_selected))
            )
        selected_representation_id = next(iter(selected), None)
        for representation_id in representation_ids:
            access_counts[representation_id] += int(
                representation_id in selected
            )
        for group in groups:
            group_access_counts[_group_key(group)] += int(
                bool(selected.intersection(group))
            )
        successes += int(bool(record["task_success"]))
        service_cost = sum(
            float(event.get("realized_cost", 0.0))
            for event in record.get("access_events", [])
            if event.get("accepted")
        )
        costs.append(service_cost)
        if require_pairing:
            key = _pairing_key(record)
            if key in pairing_keys:
                raise AWMConfigError(
                    f"duplicate paired AWM key for {design_id}: {key}"
                )
            pairing_keys.add(key)
            observations.append(
                TrialObservation(
                    pairing_key=key,
                    cluster_key=(
                        _cluster_key(record, cluster_key_fields)
                        if cluster_key_fields
                        else key
                    ),
                    repetition=int(record["repetition"]),
                    seed=int(record["seed"]),
                    success=int(bool(record["task_success"])),
                    service_cost=service_cost,
                    selected_representation_id=(
                        selected_representation_id
                    ),
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
        observations=tuple(observations),
    )


def _validate_pairs(
    model_config: AWMConfig,
    samples: dict[str, DesignSample],
    design_ids: tuple[str, ...],
    *,
    partition_name: str,
    minimum_pairs: int,
) -> None:
    if not model_config.confidence.paired_gain_enabled:
        return
    design_set = set(design_ids)
    comparisons = model_config.confidence.paired_comparisons
    if comparisons:
        pairs = tuple(
            (left_id, right_id)
            for left_id, right_id in comparisons
            if left_id in design_set and right_id in design_set
        )
    else:
        pairs = tuple(
            (left_id, right_id)
            for left_index, left_id in enumerate(design_ids)
            for right_id in design_ids[left_index + 1 :]
        )
    for left_id, right_id in pairs:
        left_keys = {
            observation.pairing_key
            for observation in samples[left_id].observations
        }
        right_keys = {
            observation.pairing_key
            for observation in samples[right_id].observations
        }
        common = left_keys.intersection(right_keys)
        if (
            model_config.confidence.require_complete_pairs
            and left_keys != right_keys
        ):
            raise AWMConfigError(
                "paired AWM requires identical eligible "
                f"{partition_name} keys "
                f"for {left_id} and {right_id}; "
                f"left_only={len(left_keys - right_keys)}, "
                f"right_only={len(right_keys - left_keys)}"
            )
        if model_config.confidence.cluster_key_fields:
            left_by_key = {
                observation.pairing_key: observation
                for observation in samples[left_id].observations
            }
            right_by_key = {
                observation.pairing_key: observation
                for observation in samples[right_id].observations
            }
            mismatched_clusters = [
                key
                for key in common
                if left_by_key[key].cluster_key
                != right_by_key[key].cluster_key
            ]
            if mismatched_clusters:
                raise AWMConfigError(
                    "paired AWM records disagree on cluster identity for "
                    f"{left_id} and {right_id}"
                )
            if model_config.confidence.cluster_reduction == (
                "mean-over-complete-repetition-block"
            ):
                repetitions_by_cluster: dict[str, list[int]] = {}
                for key in common:
                    observation = left_by_key[key]
                    repetitions_by_cluster.setdefault(
                        observation.cluster_key,
                        [],
                    ).append(observation.repetition)
                expected_repetitions: tuple[int, ...] | None = None
                for cluster_key, repetitions in sorted(
                    repetitions_by_cluster.items()
                ):
                    unique_repetitions = tuple(sorted(set(repetitions)))
                    if len(unique_repetitions) != len(repetitions):
                        raise AWMConfigError(
                            "cluster-mean paired AWM requires exactly one "
                            "observation per workload and repetition; "
                            f"partition={partition_name}, "
                            f"cluster={cluster_key}"
                        )
                    if expected_repetitions is None:
                        expected_repetitions = unique_repetitions
                    elif unique_repetitions != expected_repetitions:
                        raise AWMConfigError(
                            "cluster-mean paired AWM requires a complete "
                            "common repetition block for every workload "
                            f"cluster in the {partition_name} partition"
                        )
            effective_count = len({
                left_by_key[key].cluster_key for key in common
            })
            unit_name = "independent workload clusters"
        else:
            effective_count = len(common)
            unit_name = f"paired {partition_name} sessions"
        if effective_count < minimum_pairs:
            raise AWMConfigError(
                f"{left_id} and {right_id} have only {effective_count} "
                f"eligible {unit_name}"
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
            "the AWM analytic envelope requires max_accesses=1 for its exact "
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
    comparison_designs = {
        design_id
        for comparison in model_config.confidence.paired_comparisons
        for design_id in comparison
    }
    unknown_comparison_designs = comparison_designs - set(
        oracle_config.design_ids
    )
    if unknown_comparison_designs:
        raise AWMConfigError(
            "paired_comparisons reference designs absent from the Reduced "
            "Oracle: " + ", ".join(sorted(unknown_comparison_designs))
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
    holdout_by_repetition: dict[int, dict[str, DesignSample]] = {
        repetition: {}
        for repetition in range(holdout_start, oracle_config.repetitions)
    }
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
            require_pairing=model_config.confidence.paired_gain_enabled,
            cluster_key_fields=model_config.confidence.cluster_key_fields,
        )
        holdout[design_id] = _sample(
            design_id,
            holdout_rows,
            representation_ids=representation_ids,
            groups=model_config.substitution_groups,
            require_pairing=model_config.confidence.paired_gain_enabled,
            cluster_key_fields=model_config.confidence.cluster_key_fields,
        )
        for repetition in holdout_by_repetition:
            holdout_by_repetition[repetition][design_id] = _sample(
                design_id,
                (
                    record
                    for record in holdout_rows
                    if int(record["repetition"]) == repetition
                ),
                representation_ids=representation_ids,
                groups=model_config.substitution_groups,
                require_pairing=model_config.confidence.paired_gain_enabled,
                cluster_key_fields=(
                    model_config.confidence.cluster_key_fields
                ),
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
    _validate_pairs(
        model_config,
        training,
        tuple(model_config.observed_design_ids),
        partition_name="training",
        minimum_pairs=(
            model_config.confidence.paired_gain_minimum_pairs
        ),
    )
    _validate_pairs(
        model_config,
        holdout,
        oracle_config.design_ids,
        partition_name="holdout",
        minimum_pairs=1,
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
        holdout_by_repetition=holdout_by_repetition,
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
