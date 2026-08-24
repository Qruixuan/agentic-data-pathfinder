from __future__ import annotations

import json
from pathlib import Path
from statistics import mean
from typing import Any

from ..integrations.flowmesh.pilot import _write_csv, _write_json
from ..reduced_oracle.contracts import (
    ReducedOracleConfig,
    load_reduced_oracle_config,
)
from .contracts import (
    AWM_CONFIG_SCHEMA_VERSION_V2,
    AWM_CONFIG_SCHEMA_VERSION_V2_1,
    AWM_CONFIG_SCHEMA_VERSION_V3,
    AWM_CONFIG_SCHEMA_VERSION_V3_1,
    AWM_CONFIG_SCHEMA_VERSION_V3_2,
    AWMConfig,
    load_awm_config,
)
from .dataset import AWMDataset, DesignSample, load_oracle_dataset
from .model import AWM_MODEL_KINDS, AdaptiveWorkloadModel


AWM_EVALUATION_SCHEMA_VERSION = "pathfinder.awm-evaluation/v1alpha1"
AWM_EVALUATION_SCHEMA_VERSION_V2 = "pathfinder.awm-evaluation/v2alpha1"
AWM_EVALUATION_SCHEMA_VERSION_V2_1 = "pathfinder.awm-evaluation/v2alpha2"
AWM_EVALUATION_SCHEMA_VERSION_V3 = "pathfinder.awm-evaluation/v3alpha1"
AWM_EVALUATION_SCHEMA_VERSION_V3_1 = "pathfinder.awm-evaluation/v3alpha2"
AWM_EVALUATION_SCHEMA_VERSION_V3_2 = "pathfinder.awm-evaluation/v3alpha3"


def holdout_truth(
    dataset: AWMDataset,
    design_id: str,
    sample: DesignSample,
) -> dict[str, Any]:
    sessions = sample.eligible_sessions
    access = {
        representation_id: count / sessions
        for representation_id, count in sample.access_counts.items()
    }
    groups = {
        group_id: count / sessions
        for group_id, count in sample.group_access_counts.items()
    }
    success = sample.success_count / sessions
    service_cost = mean(sample.service_costs)
    phi = (
        dataset.horizon_sessions
        * (
            dataset.task_class.task_value * success
            - dataset.system.resource_cost_weight * service_cost
        )
        - dataset.storage_costs[design_id]
    )
    return {
        "design_id": design_id,
        "holdout_sessions": sessions,
        "excluded_holdout_sessions": sample.excluded_sessions,
        "access": access,
        "group_access": groups,
        "success": success,
        "service_cost_per_session": service_cost,
        "phi": phi,
        "forward_transition_cost": dataset.forward_transition_costs[
            design_id
        ],
    }


def _evaluate_model(
    dataset: AWMDataset,
    model: AdaptiveWorkloadModel,
    truths: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    per_design: dict[str, Any] = {}
    for design_id in dataset.design_ids:
        bounds = model.bounds[design_id]
        truth = truths[design_id]
        access_covered = all(
            bounds.access[representation_id].contains(value)
            for representation_id, value in truth["access"].items()
        )
        group_covered = all(
            bounds.group_access[group_id].contains(value)
            for group_id, value in truth["group_access"].items()
        )
        success_covered = bounds.success.contains(float(truth["success"]))
        response_covered = (
            access_covered and group_covered and success_covered
        )
        phi_covered = bounds.phi.contains(float(truth["phi"]))
        per_design[design_id] = {
            "response_vector_covered": response_covered,
            "access_vector_covered": access_covered,
            "group_vector_covered": group_covered,
            "success_covered": success_covered,
            "phi_covered": phi_covered,
            "phi_width": bounds.phi.width,
        }

    safe_design_id = dataset.oracle_config.safe_design_id
    commit_decisions: list[dict[str, Any]] = []
    for candidate in dataset.design_ids:
        if candidate == safe_design_id:
            continue
        pessimistic_gain = model.pessimistic_gain(
            safe_design_id,
            candidate,
        )
        optimistic_gain = model.optimistic_gain(
            safe_design_id,
            candidate,
        )
        gain_interval_source = model.gain_interval_source(
            safe_design_id,
            candidate,
        )
        paired_certificate = model.paired_gain_certificate(
            safe_design_id,
            candidate,
        )
        actual_gain = (
            float(truths[candidate]["phi"])
            - float(truths[safe_design_id]["phi"])
            - float(truths[candidate]["forward_transition_cost"])
        )
        would_commit = pessimistic_gain > dataset.model_config.commit_margin
        false_safe = (
            would_commit
            and actual_gain <= dataset.model_config.commit_margin
        )
        commit_decisions.append(
            {
                "current_design_id": safe_design_id,
                "candidate_design_id": candidate,
                "pessimistic_gain": pessimistic_gain,
                "optimistic_gain": optimistic_gain,
                "gain_interval_source": gain_interval_source,
                "paired_gain_certificate": (
                    paired_certificate.to_dict()
                    if paired_certificate is not None
                    else None
                ),
                "actual_holdout_gain": actual_gain,
                "actual_holdout_gain_covered": (
                    model.gain_interval(
                        safe_design_id,
                        candidate,
                    ).contains(actual_gain)
                ),
                "commit_margin": dataset.model_config.commit_margin,
                "would_commit": would_commit,
                "false_safe_commit": false_safe,
            }
        )
    paired_decisions = [
        decision
        for decision in commit_decisions
        if decision["paired_gain_certificate"] is not None
    ]
    return {
        "model_kind": model.model_kind,
        "confidence_contract": model.confidence_contract,
        "joint_response_vector_covered": all(
            value["response_vector_covered"]
            for value in per_design.values()
        ),
        "joint_phi_covered": all(
            value["phi_covered"] for value in per_design.values()
        ),
        "mean_phi_width": mean(
            value["phi_width"] for value in per_design.values()
        ),
        "false_safe_commit_count": sum(
            decision["false_safe_commit"] for decision in commit_decisions
        ),
        "commit_count": sum(
            decision["would_commit"] for decision in commit_decisions
        ),
        "paired_gain_decision_count": len(paired_decisions),
        "joint_paired_gain_covered": (
            all(
                decision["actual_holdout_gain_covered"]
                for decision in paired_decisions
            )
            if paired_decisions
            else None
        ),
        "per_design": per_design,
        "commit_decisions": commit_decisions,
    }


def evaluate_awm(
    model_config: AWMConfig | str | Path,
    oracle_config: ReducedOracleConfig | str | Path,
    *,
    oracle_output_dir: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Fit three envelopes and evaluate one paired held-out repetition set."""
    resolved_model = (
        load_awm_config(model_config)
        if isinstance(model_config, (str, Path))
        else model_config
    )
    resolved_oracle = (
        load_reduced_oracle_config(oracle_config)
        if isinstance(oracle_config, (str, Path))
        else oracle_config
    )
    dataset = load_oracle_dataset(
        resolved_model,
        resolved_oracle,
        oracle_output_dir=oracle_output_dir,
    )
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    truths = {
        design_id: holdout_truth(
            dataset,
            design_id,
            dataset.holdout[design_id],
        )
        for design_id in dataset.design_ids
    }
    truths_by_repetition = {
        str(repetition): {
            design_id: holdout_truth(
                dataset,
                design_id,
                samples[design_id],
            )
            for design_id in dataset.design_ids
        }
        for repetition, samples in sorted(
            dataset.holdout_by_repetition.items()
        )
    }
    models = {
        model_kind: AdaptiveWorkloadModel(
            dataset,
            model_kind=model_kind,
        )
        for model_kind in AWM_MODEL_KINDS
    }
    rows = [
        bounds.to_row()
        for model in models.values()
        for bounds in model.bounds.values()
    ]
    paired_rows = []
    power_rows = []
    for model in models.values():
        for current in dataset.design_ids:
            for candidate in dataset.design_ids:
                if current == candidate:
                    continue
                certificate = model.paired_gain_certificate(
                    current,
                    candidate,
                )
                if certificate is not None:
                    paired_rows.append(
                        certificate.to_row(model_kind=model.model_kind)
                    )
                    power = model.paired_gain_power_analysis(
                        current,
                        candidate,
                    )
                    if power is not None:
                        power_rows.append(
                            power.to_row(model_kind=model.model_kind)
                        )
    results = {
        model_kind: _evaluate_model(dataset, model, truths)
        for model_kind, model in models.items()
    }
    bounds_path = output / "awm_bounds.csv"
    paired_bounds_path = output / "awm_paired_gain_bounds.csv"
    paired_power_path = output / "awm_paired_power_analysis.csv"
    evaluation_path = output / "awm_evaluation.json"
    truth_path = output / "holdout_truth.json"
    truth_by_repetition_path = (
        output / "holdout_truth_by_repetition.json"
    )
    manifest_path = output / "awm_manifest.json"
    _write_csv(rows, bounds_path)
    _write_csv(paired_rows, paired_bounds_path)
    _write_csv(power_rows, paired_power_path)
    _write_json(truths, truth_path)
    _write_json(truths_by_repetition, truth_by_repetition_path)
    evaluation_schema = {
        AWM_CONFIG_SCHEMA_VERSION_V2: AWM_EVALUATION_SCHEMA_VERSION_V2,
        AWM_CONFIG_SCHEMA_VERSION_V2_1: (
            AWM_EVALUATION_SCHEMA_VERSION_V2_1
        ),
        AWM_CONFIG_SCHEMA_VERSION_V3: AWM_EVALUATION_SCHEMA_VERSION_V3,
        AWM_CONFIG_SCHEMA_VERSION_V3_1: (
            AWM_EVALUATION_SCHEMA_VERSION_V3_1
        ),
        AWM_CONFIG_SCHEMA_VERSION_V3_2: (
            AWM_EVALUATION_SCHEMA_VERSION_V3_2
        ),
    }.get(resolved_model.schema_version, AWM_EVALUATION_SCHEMA_VERSION)
    evaluation = {
        "schema_version": evaluation_schema,
        "awm_config_schema_version": resolved_model.schema_version,
        "model_id": resolved_model.model_id,
        "confidence_level": resolved_model.confidence_level,
        "confidence_method": (
            "fixed-family-bonferroni-wilson-and-"
            + resolved_model.confidence.paired_gain_method
            if resolved_model.confidence.paired_gain_enabled
            else "joint-bonferroni-wilson"
        ),
        "confidence_contract": models[
            "coupled_awm"
        ].confidence_contract,
        "coverage_unit": "complete-heldout-design-response-vector",
        "holdout_repetitions": resolved_model.holdout_repetitions,
        "holdout_repetition_ids": [
            int(value) for value in truths_by_repetition
        ],
        "observed_design_ids": list(resolved_model.observed_design_ids),
        "assumptions": {
            name: {
                "enabled": assumption.enabled,
                "status": assumption.status,
            }
            for name, assumption in sorted(
                resolved_model.assumptions.items()
            )
        },
        "models": results,
    }
    _write_json(evaluation, evaluation_path)
    manifest = {
        "schema_version": evaluation_schema,
        "status": "COMPLETE",
        "model_id": resolved_model.model_id,
        "model_config_path": str(resolved_model.source_path),
        "model_config_sha256": resolved_model.source_sha256,
        "oracle_config_path": str(resolved_oracle.source_path),
        "oracle_config_sha256": resolved_oracle.source_sha256,
        "oracle_output_dir": str(Path(oracle_output_dir).resolve()),
        "design_ids": list(dataset.design_ids),
        "training_sessions": {
            design_id: sample.eligible_sessions
            for design_id, sample in dataset.training.items()
        },
        "excluded_training_sessions": {
            design_id: sample.excluded_sessions
            for design_id, sample in dataset.training.items()
        },
        "holdout_sessions": {
            design_id: sample.eligible_sessions
            for design_id, sample in dataset.holdout.items()
        },
        "excluded_holdout_sessions": {
            design_id: sample.excluded_sessions
            for design_id, sample in dataset.holdout.items()
        },
        "bounds_path": str(bounds_path),
        "paired_gain_bounds_path": str(paired_bounds_path),
        "paired_gain_power_analysis_path": str(paired_power_path),
        "evaluation_path": str(evaluation_path),
        "holdout_truth_path": str(truth_path),
        "holdout_truth_by_repetition_path": str(
            truth_by_repetition_path
        ),
        "manifest_path": str(manifest_path),
        "secrets_recorded": False,
    }
    _write_json(manifest, manifest_path)
    return manifest
