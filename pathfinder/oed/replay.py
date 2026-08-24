from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path
from typing import Any

from ..awm import (
    AWM_CONFIG_SCHEMA_VERSION_V2,
    AWMDataset,
    AWMConfig,
    AdaptiveWorkloadModel,
    holdout_truth,
    load_awm_config,
    load_oracle_dataset,
)
from ..integrations.flowmesh.pilot import _write_csv, _write_json
from ..reduced_oracle import (
    ReducedOracleConfig,
    load_reduced_oracle_config,
)
from .contracts import OEDConfig, load_oed_config
from .controller import OEDController, OEDState


OED_REPLAY_SCHEMA_VERSION = "pathfinder.oed-replay/v1alpha1"
OED_REPLAY_SCHEMA_VERSION_V2 = "pathfinder.oed-replay/v2alpha1"
_ACTIVE_POLICIES = (
    "full_oed",
    "passive_awm",
    "random_reveal",
    "black_box_reveal",
)


def _write_jsonl(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
                handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _training_config(
    config: AWMConfig,
    observed_design_ids: tuple[str, ...],
) -> AWMConfig:
    return replace(config, observed_design_ids=observed_design_ids)


def _truths(dataset: AWMDataset) -> dict[str, dict[str, Any]]:
    return {
        design_id: holdout_truth(
            dataset,
            design_id,
            dataset.holdout[design_id],
        )
        for design_id in dataset.design_ids
    }


def _actual_gain(
    truths: dict[str, dict[str, Any]],
    dataset: AWMDataset,
    current: str,
    candidate: str,
) -> float:
    return (
        float(truths[candidate]["phi"])
        - float(truths[current]["phi"])
        - dataset.forward_transition_costs[candidate]
    )


def _run_active_policy(
    policy_kind: str,
    oed_config: OEDConfig,
    awm_config: AWMConfig,
    oracle_config: ReducedOracleConfig,
    *,
    oracle_output_dir: Path,
    truths: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    controller = OEDController(oed_config)
    observed = tuple(awm_config.observed_design_ids)
    revealed: tuple[str, ...] = ()
    safe = oracle_config.safe_design_id
    remaining = oed_config.exploration_budget
    reserved_exploration = 0.0
    actual_exploration = 0.0
    committed_transition = 0.0
    trace: list[dict[str, Any]] = []
    terminal_reason: str | None = None

    for iteration in range(oed_config.max_iterations):
        iteration_config = _training_config(awm_config, observed)
        dataset = load_oracle_dataset(
            iteration_config,
            oracle_config,
            oracle_output_dir=oracle_output_dir,
        )
        model = AdaptiveWorkloadModel(
            dataset,
            model_kind=(
                "independent_box"
                if policy_kind == "black_box_reveal"
                else "coupled_awm"
            ),
        )
        state = OEDState(
            safe_design_id=safe,
            observed_design_ids=observed,
            revealed_design_ids=revealed,
            remaining_exploration_purse=remaining,
        )
        decision = controller.decide(
            dataset,
            model,
            state,
            iteration=iteration,
            policy_kind=policy_kind,
        )
        row = decision.to_dict()
        row.update(
            {
                "trigger": "offline_frozen_oracle_replay",
                "controller_id": oed_config.controller_id,
                "awm_model_id": iteration_config.model_id,
                "awm_model_kind": model.model_kind,
                "awm_config_schema_version": iteration_config.schema_version,
                "awm_confidence_contract": model.confidence_contract,
                "cost_model_status": oed_config.cost_model_status,
                "candidate_domain": list(dataset.design_ids),
                "safe_design_id_after": safe,
                "observed_design_ids_before": list(observed),
                "observed_design_ids_after": list(observed),
                "remaining_exploration_purse_after": remaining,
                "actual_forward_transition_cost": 0.0,
                "actual_probe_window_loss": 0.0,
                "actual_restoration_transition_cost": 0.0,
                "actual_excursion_cost": 0.0,
                "reserved_excursion_upper": 0.0,
            }
        )
        selected = decision.selected_design_id
        if decision.selected_action == "REVEAL":
            if selected is None:
                raise RuntimeError("REVEAL decision is missing a design")
            score = next(
                value
                for value in decision.candidate_scores
                if value.design_id == selected
            )
            candidate = oed_config.reveal_candidates[selected]
            forward = dataset.forward_transition_costs[selected]
            restoration = dataset.restoration_transition_costs[selected]
            actual_excursion = (
                forward + restoration + candidate.probe_window_loss
            )
            remaining -= score.reveal_excursion.upper
            reserved_exploration += score.reveal_excursion.upper
            actual_exploration += actual_excursion
            observed = tuple(sorted(set(observed).union({selected})))
            revealed = tuple(sorted(set(revealed).union({selected})))
            row.update(
                {
                    # A Reveal always restores the incumbent safe design.
                    "safe_design_id_after": safe,
                    "observed_design_ids_after": list(observed),
                    "remaining_exploration_purse_after": remaining,
                    "actual_forward_transition_cost": forward,
                    "actual_probe_window_loss": candidate.probe_window_loss,
                    "actual_restoration_transition_cost": restoration,
                    "actual_excursion_cost": actual_excursion,
                    "reserved_excursion_upper": score.reveal_excursion.upper,
                }
            )
        elif decision.selected_action == "COMMIT":
            if selected is None:
                raise RuntimeError("COMMIT decision is missing a design")
            previous_safe = safe
            forward = dataset.forward_transition_costs[selected]
            committed_transition += forward
            safe = selected
            row.update(
                {
                    "safe_design_id_after": safe,
                    "actual_forward_transition_cost": forward,
                    "actual_holdout_safe_gain": _actual_gain(
                        truths,
                        dataset,
                        previous_safe,
                        selected,
                    ),
                }
            )
            terminal_reason = "commit_completed"
        else:
            terminal_reason = decision.stopping_reason
        trace.append(row)
        if decision.selected_action != "REVEAL":
            break
    else:
        terminal_reason = "maximum_iterations_reached"

    commit_rows = [row for row in trace if row["selected_action"] == "COMMIT"]
    regressions = [
        row
        for row in commit_rows
        if float(row["actual_holdout_safe_gain"])
        <= oed_config.commit_margin
    ]
    final_phi = float(truths[safe]["phi"])
    actual_commit_gains = [
        float(row["actual_holdout_safe_gain"])
        for row in commit_rows
    ]
    return (
        {
            "policy_kind": policy_kind,
            "starting_safe_design_id": oracle_config.safe_design_id,
            "final_safe_design_id": safe,
            "terminal_reason": terminal_reason,
            "iterations": len(trace),
            "reveal_count": sum(
                row["selected_action"] == "REVEAL" for row in trace
            ),
            "commit_count": len(commit_rows),
            "hold_count": sum(
                row["selected_action"] == "HOLD" for row in trace
            ),
            "reserved_exploration_budget": reserved_exploration,
            "actual_exploration_cost": actual_exploration,
            "committed_transition_cost": committed_transition,
            "total_control_cost": actual_exploration + committed_transition,
            "final_holdout_phi": final_phi,
            "net_holdout_value": (
                final_phi - actual_exploration - committed_transition
            ),
            "safe_sequence_regression_count": len(regressions),
            "safe_sequence_regressions_reported": True,
            "minimum_actual_commit_gain": (
                min(actual_commit_gains) if actual_commit_gains else None
            ),
            "remaining_exploration_purse": remaining,
            "observed_design_ids": list(observed),
        },
        trace,
    )


def _naive_design(
    dataset: AWMDataset,
    oracle_config: ReducedOracleConfig,
) -> str:
    safe = oracle_config.safe_design_id
    candidate = oracle_config.naive_baseline.candidate_design_id
    representation = oracle_config.naive_baseline.representation_id
    sample = dataset.training[safe]
    access_rate = (
        sample.access_counts[representation] / sample.eligible_sessions
    )
    safe_cost = dataset.system.designs[safe].paths[representation].realized_cost
    candidate_cost = (
        dataset.system.designs[candidate].paths[representation].realized_cost
    )
    estimated_gain = (
        dataset.horizon_sessions
        * access_rate
        * max(0.0, safe_cost - candidate_cost)
        * dataset.system.resource_cost_weight
        - dataset.storage_costs[candidate]
        - dataset.forward_transition_costs[candidate]
    )
    return (
        candidate
        if estimated_gain > oracle_config.naive_baseline.decision_margin
        else safe
    )


def _fixed_policy(
    name: str,
    final_design_id: str,
    dataset: AWMDataset,
    truths: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    transition = (
        dataset.forward_transition_costs[final_design_id]
        if final_design_id != dataset.oracle_config.safe_design_id
        else 0.0
    )
    phi = float(truths[final_design_id]["phi"])
    actual_gain = (
        phi
        - float(truths[dataset.oracle_config.safe_design_id]["phi"])
        - transition
        if final_design_id != dataset.oracle_config.safe_design_id
        else None
    )
    return {
        "policy_kind": name,
        "starting_safe_design_id": dataset.oracle_config.safe_design_id,
        "final_safe_design_id": final_design_id,
        "terminal_reason": "fixed_baseline_decision",
        "iterations": 0,
        "reveal_count": 0,
        "commit_count": int(
            final_design_id != dataset.oracle_config.safe_design_id
        ),
        "hold_count": 0,
        "reserved_exploration_budget": 0.0,
        "actual_exploration_cost": 0.0,
        "committed_transition_cost": transition,
        "total_control_cost": transition,
        "final_holdout_phi": phi,
        "net_holdout_value": phi - transition,
        "safe_sequence_regression_count": int(
            actual_gain is not None and actual_gain <= 0.0
        ),
        "safe_sequence_regressions_reported": True,
        "minimum_actual_commit_gain": actual_gain,
        "remaining_exploration_purse": None,
        "observed_design_ids": [],
    }


def run_oed_replay(
    oed_config: OEDConfig | str | Path,
    awm_config: AWMConfig | str | Path,
    oracle_config: ReducedOracleConfig | str | Path,
    *,
    oracle_output_dir: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Replay OED and equal-budget baselines without mutating a deployment."""
    resolved_oed = (
        load_oed_config(oed_config)
        if isinstance(oed_config, (str, Path))
        else oed_config
    )
    resolved_awm = (
        load_awm_config(awm_config)
        if isinstance(awm_config, (str, Path))
        else awm_config
    )
    resolved_oracle = (
        load_reduced_oracle_config(oracle_config)
        if isinstance(oracle_config, (str, Path))
        else oracle_config
    )
    oracle_output = Path(oracle_output_dir).resolve()
    base_dataset = load_oracle_dataset(
        resolved_awm,
        resolved_oracle,
        oracle_output_dir=oracle_output,
    )
    truths = _truths(base_dataset)
    if (
        resolved_awm.confidence.paired_gain_enabled
        and resolved_oed.max_iterations
        > resolved_awm.confidence.maximum_looks
    ):
        raise ValueError(
            "OED max_iterations exceeds the AWM v2 preregistered "
            "maximum_looks"
        )
    unknown_candidates = set(resolved_oed.reveal_candidates).union(
        resolved_oed.other_design_ids
    ) - set(base_dataset.design_ids)
    if unknown_candidates:
        raise ValueError(
            "OED design sets contain unknown designs: "
            + ", ".join(sorted(unknown_candidates))
        )

    results: dict[str, dict[str, Any]] = {}
    all_trace: list[dict[str, Any]] = []
    for policy_kind in _ACTIVE_POLICIES:
        result, trace = _run_active_policy(
            policy_kind,
            resolved_oed,
            resolved_awm,
            resolved_oracle,
            oracle_output_dir=oracle_output,
            truths=truths,
        )
        results[policy_kind] = result
        all_trace.extend(trace)

    naive = _naive_design(base_dataset, resolved_oracle)
    oracle = max(
        base_dataset.design_ids,
        key=lambda design_id: (
            float(truths[design_id]["phi"])
            - base_dataset.forward_transition_costs[design_id]
        ),
    )
    results["naive_read_react_materialize"] = _fixed_policy(
        "naive_read_react_materialize",
        naive,
        base_dataset,
        truths,
    )
    results["exhaustive_oracle"] = _fixed_policy(
        "exhaustive_oracle",
        oracle,
        base_dataset,
        truths,
    )
    oracle_value = results["exhaustive_oracle"]["net_holdout_value"]
    for result in results.values():
        result["regret_to_exhaustive_oracle"] = (
            oracle_value - result["net_holdout_value"]
        )
        result["reached_oracle_design"] = (
            result["final_safe_design_id"] == oracle
        )

    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    trace_path = output / "oed_trace.jsonl"
    summary_path = output / "oed_policy_summary.csv"
    evaluation_path = output / "oed_evaluation.json"
    manifest_path = output / "oed_manifest.json"
    _write_jsonl(all_trace, trace_path)
    _write_csv(
        [results[name] for name in sorted(results)],
        summary_path,
    )
    replay_schema = (
        OED_REPLAY_SCHEMA_VERSION_V2
        if resolved_awm.schema_version == AWM_CONFIG_SCHEMA_VERSION_V2
        else OED_REPLAY_SCHEMA_VERSION
    )
    evaluation = {
        "schema_version": replay_schema,
        "awm_config_schema_version": resolved_awm.schema_version,
        "awm_confidence_contract": AdaptiveWorkloadModel(
            base_dataset,
            model_kind="coupled_awm",
        ).confidence_contract,
        "controller_id": resolved_oed.controller_id,
        "decision_data": "training_partition_only",
        "evaluation_data": "paired_holdout_partition_only",
        "starting_safe_design_id": resolved_oracle.safe_design_id,
        "exhaustive_oracle_design_id": oracle,
        "exploration_budget": resolved_oed.exploration_budget,
        "per_excursion_cap": resolved_oed.per_excursion_cap,
        "cost_model_status": resolved_oed.cost_model_status,
        "policies": results,
        "gate_b4_pre_server_checks": {
            "full_oed_reached_oracle": results["full_oed"][
                "reached_oracle_design"
            ],
            "no_full_oed_safe_sequence_regression": results["full_oed"][
                "safe_sequence_regression_count"
            ] == 0,
            "full_oed_lower_cost_than_equal_budget_baselines": all(
                results["full_oed"]["total_control_cost"]
                < results[name]["total_control_cost"]
                for name in ("random_reveal", "black_box_reveal")
            ),
        },
        "scientific_status": (
            "offline-controller-replay-only; Gate B4 requires the frozen "
            "physical Oracle and live Reveal execution"
        ),
    }
    _write_json(evaluation, evaluation_path)
    manifest = {
        "schema_version": replay_schema,
        "status": "COMPLETE",
        "controller_id": resolved_oed.controller_id,
        "oed_config_path": str(resolved_oed.source_path),
        "oed_config_sha256": resolved_oed.source_sha256,
        "awm_config_path": str(resolved_awm.source_path),
        "awm_config_sha256": resolved_awm.source_sha256,
        "oracle_config_path": str(resolved_oracle.source_path),
        "oracle_config_sha256": resolved_oracle.source_sha256,
        "oracle_output_dir": str(oracle_output),
        "trace_path": str(trace_path),
        "summary_path": str(summary_path),
        "evaluation_path": str(evaluation_path),
        "manifest_path": str(manifest_path),
        "deployment_mutations_performed": False,
        "holdout_used_for_decisions": False,
        "secrets_recorded": False,
    }
    _write_json(manifest, manifest_path)
    return manifest
