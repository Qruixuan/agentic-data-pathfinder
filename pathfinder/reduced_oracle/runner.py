from __future__ import annotations

import json
from dataclasses import asdict, replace
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable

from ..integrations.flowmesh.pilot import (
    AgentAdapterProtocol,
    FlowMeshPilotConfig,
    _append_jsonl,
    _exclusive_pilot_lock,
    _write_json,
    build_trial_plan,
    load_flowmesh_pilot_config,
    run_flowmesh_pilot,
    validate_flowmesh_pilot_config,
)
from ..models import SystemConfig
from .contracts import ReducedOracleConfig, ReducedOracleConfigError
from .objective import analyze_reduced_oracle
from .transition import FilesystemTransitionExecutor


ORACLE_PLAN_SCHEMA_VERSION = "pathfinder.reduced-oracle-plan/v1alpha1"
ORACLE_RUN_SCHEMA_VERSION = "pathfinder.reduced-oracle-run/v1alpha1"


def _oracle_pilot(
    config: ReducedOracleConfig,
    workload_source: FlowMeshPilotConfig,
) -> FlowMeshPilotConfig:
    return FlowMeshPilotConfig(
        schema_version=workload_source.schema_version,
        experiment_id=config.oracle_id,
        source_path=config.source_path,
        system_config_path=workload_source.system_config_path,
        design_ids=config.design_ids,
        task_class_id=workload_source.task_class_id,
        quote_profile_ids=(config.quote_profile_id,),
        latency_multipliers=(config.latency_multiplier,),
        repetitions=config.repetitions,
        base_seed=config.base_seed,
        randomization_seed=config.randomization_seed,
        workloads=workload_source.workloads,
        source_sha256=config.source_sha256,
    )


def _design_pilot(
    pilot: FlowMeshPilotConfig,
    design_id: str,
) -> FlowMeshPilotConfig:
    return replace(
        pilot,
        experiment_id=f"{pilot.experiment_id}-{design_id}",
        design_ids=(design_id,),
    )


def _manifest_complete(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid design manifest: {path}") from exc
    return payload.get("status") == "COMPLETE"


def _frozen_plan(
    config: ReducedOracleConfig,
    system: SystemConfig,
    pilot: FlowMeshPilotConfig,
) -> dict[str, Any]:
    design_batches: dict[str, Any] = {}
    for design_id in config.design_ids:
        design_pilot = _design_pilot(pilot, design_id)
        design_batches[design_id] = {
            "design": json.loads(
                json.dumps(asdict(config.designs[design_id]))
            ),
            "trials": [
                trial.to_public_dict()
                for trial in build_trial_plan(design_pilot)
            ],
        }
    return {
        "schema_version": ORACLE_PLAN_SCHEMA_VERSION,
        "oracle_id": config.oracle_id,
        "oracle_config_sha256": config.source_sha256,
        "workload_pilot_config_path": str(
            config.workload_pilot_config_path
        ),
        "workload_pilot_config_sha256": sha256(
            config.workload_pilot_config_path.read_bytes()
        ).hexdigest(),
        "system_config_path": str(system.source_path),
        "system_config_sha256": sha256(
            system.source_path.read_bytes()
        ).hexdigest(),
        "safe_design_id": config.safe_design_id,
        "design_order": list(config.design_ids),
        "quote_profile_id": config.quote_profile_id,
        "latency_multiplier": config.latency_multiplier,
        "repetitions": config.repetitions,
        "horizon_sessions": config.horizon_sessions,
        "horizon_hours": config.horizon_hours,
        "minimum_completion_rate": config.minimum_completion_rate,
        "cost_model_status": config.cost_model_status,
        "transition_cost_model": asdict(config.transition_cost),
        "pathfinder_seeds_paired_across_designs": True,
        "design_batches": design_batches,
    }


def _ensure_frozen_plan(path: Path, expected: dict[str, Any]) -> None:
    if not path.exists():
        _write_json(expected, path)
        return
    try:
        actual = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid existing oracle plan: {path}") from exc
    if actual != expected:
        raise RuntimeError(
            "existing reduced-oracle plan does not match this invocation; "
            "use a new output directory"
        )


def run_reduced_oracle(
    *,
    config: ReducedOracleConfig,
    system: SystemConfig,
    adapter: AgentAdapterProtocol,
    output_dir: str | Path,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Exhaustively run the declared designs and restore the safe design.

    Worker, Data Agent, and MCP process lifecycle remains operator-managed.
    The only physical mutation performed here is the bounded filesystem
    materialization declared under ``materialization_root``.
    """
    if not adapter.settings.pinning_requested:
        raise ReducedOracleConfigError(
            "reduced-oracle runs require a pinned FlowMesh worker"
        )
    workload_source = load_flowmesh_pilot_config(
        config.workload_pilot_config_path
    )
    pilot = _oracle_pilot(config, workload_source)
    validate_flowmesh_pilot_config(pilot, system)
    if system.source_path != workload_source.system_config_path:
        raise ReducedOracleConfigError(
            "system does not match workload_pilot_config.system_config"
        )
    for design_id in config.design_ids:
        if design_id not in system.designs:
            raise ReducedOracleConfigError(
                f"oracle references unknown system design: {design_id}"
            )
    representation_id = config.naive_baseline.representation_id
    for design_id in (
        config.safe_design_id,
        config.naive_baseline.candidate_design_id,
    ):
        if representation_id not in system.designs[design_id].paths:
            raise ReducedOracleConfigError(
                f"baseline representation {representation_id} is absent "
                f"from {design_id}"
            )

    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    plan_path = output / "oracle_plan.json"
    transitions_path = output / "transitions.jsonl"
    manifest_path = output / "oracle_manifest.json"
    plan = _frozen_plan(config, system, pilot)
    object_ids = tuple(
        sorted({workload.object_id for workload in pilot.workloads})
    )
    executor = FilesystemTransitionExecutor(
        config,
        object_ids=object_ids,
        runtime_dir=output / "runtime",
    )

    with _exclusive_pilot_lock(output / ".oracle.lock"):
        _ensure_frozen_plan(plan_path, plan)
        if executor.active_design_id() != config.safe_design_id:
            transition = executor.transition(
                config.safe_design_id,
                transition_type=(
                    "activate"
                    if executor.active_design_id() is None
                    else "restore"
                ),
            )
            _append_jsonl(transitions_path, transition.to_dict())

        for design_id in config.design_ids:
            design_output = output / "designs" / design_id
            if _manifest_complete(design_output / "manifest.json"):
                continue
            if design_id != config.safe_design_id:
                transition = executor.transition(
                    design_id,
                    transition_type="forward",
                )
                _append_jsonl(transitions_path, transition.to_dict())
            try:
                run_flowmesh_pilot(
                    pilot=_design_pilot(pilot, design_id),
                    system=system,
                    adapter=adapter,
                    output_dir=design_output,
                    progress_callback=(
                        None
                        if progress_callback is None
                        else lambda event, selected=design_id: progress_callback(
                            {"design_id": selected, **event}
                        )
                    ),
                )
            finally:
                if design_id != config.safe_design_id:
                    restoration = executor.transition(
                        config.safe_design_id,
                        transition_type="restore",
                    )
                    _append_jsonl(
                        transitions_path,
                        restoration.to_dict(),
                    )

        if executor.active_design_id() != config.safe_design_id:
            restoration = executor.transition(
                config.safe_design_id,
                transition_type="restore",
            )
            _append_jsonl(transitions_path, restoration.to_dict())
        analysis = analyze_reduced_oracle(config, output_dir=output)
        manifest = {
            "schema_version": ORACLE_RUN_SCHEMA_VERSION,
            "oracle_id": config.oracle_id,
            "status": "COMPLETE",
            "safe_design_id": config.safe_design_id,
            "safe_design_restored": analysis["safe_design_restored"],
            "design_count": len(config.design_ids),
            "oracle_plan_path": str(plan_path),
            "transition_records_path": str(transitions_path),
            "oracle_summary_path": analysis["oracle_summary_path"],
            "worker_id": adapter.settings.worker_id,
            "worker_alias": adapter.settings.worker_alias,
            "worker_lifecycle_managed": False,
            "service_lifecycle_managed": False,
            "secrets_recorded": False,
        }
        _write_json(manifest, manifest_path)
        return {**analysis, "oracle_manifest_path": str(manifest_path)}
