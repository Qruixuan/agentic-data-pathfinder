"""Deterministic invented records to exercise the public evaluation format.

No datasets, HTTP clients, workers, LLMs or execution runners are used. This
example is not an experiment and makes no claim about Pathfinder's quality.
"""

from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory

from ..distributed.execution import (
    FlowMeshDistributedSessionExecutor, TrialExecution,
    build_canonical_record, build_frozen_plan_document,
)
from ..distributed.measurements import load_measurement_manifest
from ..distributed.preregistration import load_distributed_pilot_preregistration
from ..distributed.registry import load_endpoint_registry
from ..distributed.runner import PILOT_RUN_STATE_SCHEMA_VERSION, build_distributed_trial_plan
from ..distributed.scoring import (
    ACCEPTED_SUBSTRING_SCORING_RULE,
    MULTIPLE_CHOICE_EXACT_SCORING_RULE,
    SUCCESS_SCORING_RULES,
)
from .distributed import EvaluationError


def _write(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8", newline="\n")


def _jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
                    encoding="utf-8", newline="\n")


def create_evaluation_example(
    output_dir: str | Path,
    *,
    success_scoring_rule: str = ACCEPTED_SUBSTRING_SCORING_RULE,
) -> dict:
    """Write a new, clearly synthetic input snapshot; refuse all overwrites."""
    if success_scoring_rule not in SUCCESS_SCORING_RULES:
        raise EvaluationError(
            f"unsupported success_scoring_rule: {success_scoring_rule}"
        )
    target = Path(output_dir).resolve()
    if target.exists():
        raise EvaluationError("example output directory already exists")
    target.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".pathfinder-example-", dir=target.parent) as temp:
        staging = Path(temp) / "example"
        config, run = staging / "config", staging / "run"
        config.mkdir(parents=True)
        run.mkdir()
        _create(config, run, success_scoring_rule=success_scoring_rule)
        (staging / "SYNTHETIC_EXAMPLE.txt").write_text(
            "Invented format and arithmetic example only. Not empirical data.\n"
            "Three fictional objects; twelve successful executions, one failed\n"
            "infrastructure attempt, one wrong task answer. All costs and times\n"
            "are invented. No video, model, worker or service was contacted.\n",
            encoding="utf-8", newline="\n",
        )
        if target.exists():
            raise EvaluationError("example output directory already exists")
        staging.rename(target)
    return {"status": "COMPLETE", "synthetic": True, "planned_trials": 12,
            "independent_workloads": 3, "live_execution_performed": False,
            "eligible_for_scientific_claims": False}


def _create(
    config: Path,
    run: Path,
    *,
    success_scoring_rule: str,
) -> None:
    pilot_id = "synthetic-workload-evaluation-v1"
    safe, frames, digest = "D_origin_remote", "D_local_frames", "D_local_digest"
    strata = {name: {"candidate_design_id": digest if name == "temporal" else frames,
                     "workload_ids": [f"example-{name}"]}
              for name in ("causal", "descriptive", "temporal")}
    prereg_payload = {
        "schema_version": "pathfinder.distributed-pilot-preregistration/v1alpha1",
        "pilot_id": pilot_id, "source_git_revision": "0" * 40,
        "synthetic": True, "posthoc": False, "confirmatory": False,
        "eligible_for_scientific_claims": False,
        "thresholds": {
            "delta_success_margin": 0.05, "minimum_cost_saving": 0.25,
            "alpha": 0.05,
            "provenance": "pilot-engineering-threshold-fixed-before-new-pilot-outcomes",
        },
        "safe_design_id": safe, "design_ids": [safe, frames, digest, "D_local_pair"],
        "excluded_design_ids": ["D_local_pair"], "strata": strata,
        "excluded_workload_ids": [], "repetitions": 2,
        "success_scoring_rule": success_scoring_rule,
        **(
            {
                "benchmark_bindings": {
                    "selection_protocol_sha256": "1" * 64,
                    "scoring_contract_sha256": "2" * 64,
                    "representation_manifest_sha256": "3" * 64,
                }
            }
            if success_scoring_rule == MULTIPLE_CHOICE_EXACT_SCORING_RULE
            else {}
        ),
        "total_cost_contract": {
            "cost_basis": "total_cost",
            "equation": "total_cost = service + network + storage + amortized_materialization + transition",
            "cost_model": {
                "schema_version": "pathfinder.total-cost-model/v1alpha1",
                "cost_model_id": "synthetic-arithmetic-only",
                "accounting_unit": "invented-example-unit",
                "rate_provenance": "Synthetic arithmetic fixture; NOT real prices or measurements.",
                "materialization_amortization_horizon_sessions": 2,
                "artifact_transfer_accounted_in": "network_cost",
                "conversion_rates": {
                    "network_cost_per_gib": 1048576.0,
                    "storage_cost_per_gib_hour": 104857.6,
                    "materialization_cost_per_gib": 209715.2,
                    "transition_cost_per_gib": 0.0,
                    "elapsed_time_cost_per_second": 0.05,
                },
            },
        },
        "fallback_rule": {"design_id": safe, "applies_to_every_non_safe_result": True},
        "run_declaration": {"immutable_after_first_observation": True},
    }
    _write(config / "preregistration.json", prereg_payload)
    prereg = load_distributed_pilot_preregistration(config / "preregistration.json")
    registry_payload = {
        "schema_version": "pathfinder.data-agent-endpoint-registry/v1alpha1",
        "registry_id": "synthetic-example-registry", "execution_node_id": "example-execution",
        "endpoints": [
            {"endpoint_id": "origin_remote", "node_id": "example-origin",
             "location": "origin", "base_url_env": "EXAMPLE_ORIGIN_URL",
             "network_transport": "remote"},
            {"endpoint_id": "local_materialized", "node_id": "example-execution",
             "location": "local", "base_url_env": "EXAMPLE_LOCAL_URL",
             "network_transport": "local",
             "network_zero_justification": "Fictional endpoint colocated with execution."},
        ],
        "default_endpoint_id": "origin_remote",
        "placement": [
            {"design_id": frames, "representation_id": "sampled_frames",
             "endpoint_id": "local_materialized"},
            {"design_id": digest, "representation_id": "multimodal_digest",
             "endpoint_id": "local_materialized"},
        ],
    }
    _write(config / "endpoint-registry.json", registry_payload)
    registry = load_endpoint_registry(config / "endpoint-registry.json")
    entries = []
    for design in (safe, frames, digest):
        for node in ("example-origin", "example-execution"):
            components = {
                "storage": {"kind": "configured", "bytes": 1024, "hours": 1},
                "materialization": {"kind": "configured", "bytes": 1024},
                "transition": {"kind": "configured", "bytes": 0, "seconds": 1},
            }
            if design == safe:
                components = {name: {"kind": "not_applicable",
                                     "justification": "No added work in synthetic safe design."}
                              for name in components}
            for component in components.values():
                component["provenance"] = "Invented deterministic format example."
            entries.append({"design_id": design, "node_id": node, **components})
    _write(config / "measurements.json", {
        "schema_version": "pathfinder.pilot-measurements/v1alpha1",
        "measurement_id": "synthetic-example-measurements", "pilot_id": pilot_id,
        "preregistration_sha256": prereg.source_sha256,
        "endpoint_registry_sha256": registry.source_sha256,
        "execution_node_id": registry.execution_node_id, "measurements": entries,
    })
    provider = load_measurement_manifest(config / "measurements.json")
    if success_scoring_rule == MULTIPLE_CHOICE_EXACT_SCORING_RULE:
        workloads = {
            f"example-{name}": {
                "object_id": f"fictional-object-{name}",
                "question": "What color is the fictional object?",
                "answer_options": [
                    {"option_id": "A", "text": "red"},
                    {"option_id": "B", "text": "blue"},
                ],
                "correct_answer_id": "B",
            }
            for name in strata
        }
    else:
        workloads = {
            f"example-{name}": {
                "object_id": f"fictional-object-{name}",
                "question": "What color is the fictional object?",
                "accepted_answer_substrings": ["blue"],
            }
            for name in strata
        }
    _write(config / "workloads.json", workloads)
    trials = build_distributed_trial_plan(prereg)
    plan = build_frozen_plan_document(
        prereg, registry, workloads=workloads, trials=trials,
    )
    _write(run / "distributed_pilot_plan.json", plan)
    records, attempts, journal = [], [], []
    for trial in trials:
        artifact = trial.stratum_id == "descriptive"
        representation = "sampled_frames" if artifact else "multimodal_digest"
        route = registry.route(design_id=trial.design_id, representation_id=representation)
        is_local = route.endpoint_id == "local_materialized"
        attempt = 2 if trial.order_index == 0 else 1
        event = {
            "accepted": True, "representation_id": representation,
            "object_id": workloads[trial.workload_id]["object_id"],
            "session_id": FlowMeshDistributedSessionExecutor.session_id_for_attempt(trial, attempt),
            "endpoint_id": route.endpoint_id, "source_node_id": route.source_node_id,
            "source_location": route.source_location,
            "destination_execution_node_id": route.destination_execution_node_id,
            "realized_cost": (0.5 if artifact else 0.25) if is_local else 2.0,
            "bytes_read": 400 if artifact else 100,
            "artifact_bytes_sent": 400 if artifact else 0,
            "artifact_download_request_count": 1 if artifact else 0,
            "artifact_full_download_count": 1 if artifact else 0,
            "felt_latency_ms": 10.0 if is_local else 30.0,
            "data_agent_service_latency_ms": 12.0,
            "data_agent_controlled_delay_ms": 10.0,
            "data_agent_fetch_latency_ms": 1.0,
            "artifact_transfer_latency_ms": 5.0 if artifact else 0.0,
        }
        if artifact:
            event["artifact_handle_sha256"] = sha256(trial.trial_key.encode()).hexdigest()
        correct_answer = (
            "B"
            if success_scoring_rule == MULTIPLE_CHOICE_EXACT_SCORING_RULE
            else "blue"
        )
        incorrect_answer = (
            "A"
            if success_scoring_rule == MULTIPLE_CHOICE_EXACT_SCORING_RULE
            else "red"
        )
        execution = TrialExecution(
            final_answer=(
                incorrect_answer
                if artifact and is_local and trial.repetition == 1
                else correct_answer
            ),
            access_events=(event,), status="DONE",
            workflow_id=f"synthetic-workflow-{trial.order_index}",
            task_id=f"synthetic-task-{trial.order_index}",
        )
        records.append(build_canonical_record(
            prereg, trial, execution, workload=workloads[trial.workload_id],
            started_at="2000-01-01T00:00:00+00:00", finished_at="2000-01-01T00:00:01+00:00",
            cost_model=prereg.cost_model, provider=provider,
            source_node_id=route.source_node_id,
            network_transport_for={key: endpoint.network_transport
                                   for key, endpoint in registry.endpoints.items()},
        ))
        if attempt == 2:
            attempts.append({"trial_key": trial.trial_key, "attempt": 1,
                             "observation_class": "infrastructure", "succeeded": False,
                             "telemetry_complete": False, "artifact_selected": False,
                             "artifact_delivery_complete": True,
                             "failure_class": "infrastructure_failure"})
        attempts.append({"trial_key": trial.trial_key, "attempt": attempt,
                         "observation_class": "canonical", "succeeded": True,
                         "telemetry_complete": True, "artifact_selected": artifact,
                         "artifact_delivery_complete": True, "failure_class": None})
        journal.append({"trial_key": trial.trial_key, "state": "COMPLETED"})
    _jsonl(run / "canonical_records.jsonl", records)
    _jsonl(run / "attempt_ledger.jsonl", attempts)
    _jsonl(run / "cell_journal.jsonl", journal)
    _jsonl(run / "run_summary.jsonl", [{
        "schema_version": PILOT_RUN_STATE_SCHEMA_VERSION,
        "pilot_id": pilot_id, "status": "COMPLETE", "complete": True,
        "oracle_complete": True, "posthoc": False, "confirmatory": False,
        "eligible_for_scientific_claims": False,
        "measurement_manifest_sha256": provider.manifest_sha256,
        "workload_content_sha256": plan["workload_content_sha256"],
        "workload_id_manifest_sha256": plan["workload_id_manifest_sha256"],
        "cost_basis": prereg.cost_basis, "planned_trial_count": len(trials),
        "completed_canonical_count": len(trials), "remaining_trial_count": 0,
        "independent_workload_count": 3, "complete_workload_count": 3,
        "complete_workload_blocks": list(prereg.workload_ids),
        "infrastructure_failure_count": 1, "recovery_attempt_count": 0,
    }])
