from __future__ import annotations

import math
import random
import uuid
from hashlib import sha256
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from .data_agent import LocalDataAgent
from .models import PilotConfig, SessionObservation, SystemConfig
from .resolver import AccessResolver, SimulatedAgent
from .telemetry import (
    JsonlTelemetryStore,
    summarize_observations,
    write_manifest,
    write_summary_csv,
)


def _success_probability(
    quality: float,
    felt_latency_ms: float,
    midpoint: float,
    temperature: float,
    latency_reference_ms: float,
    latency_penalty_per_ms: float,
) -> float:
    logit = (quality - midpoint) / temperature
    excess_latency = max(0.0, felt_latency_ms - latency_reference_ms)
    logit -= latency_penalty_per_ms * excess_latency
    if logit >= 0:
        return 1.0 / (1.0 + math.exp(-logit))
    exp_logit = math.exp(logit)
    return exp_logit / (1.0 + exp_logit)


def run_session(
    config: SystemConfig,
    design_id: str,
    task_class_id: str,
    quote_profile_id: str = "as_designed",
    latency_multiplier: float = 1.0,
    seed: int = 1,
    trial_id: str | None = None,
) -> SessionObservation:
    if design_id not in config.designs:
        raise ValueError(f"unknown design: {design_id}")
    if task_class_id not in config.task_classes:
        raise ValueError(f"unknown task class: {task_class_id}")
    if quote_profile_id not in config.quote_profiles:
        raise ValueError(f"unknown quote profile: {quote_profile_id}")
    if latency_multiplier <= 0:
        raise ValueError("latency_multiplier must be positive")

    trial_id = trial_id or "manual"
    session_key = (
        f"{config.source_path}:{trial_id}:{design_id}:{task_class_id}:"
        f"{quote_profile_id}:{latency_multiplier}:{seed}"
    )
    session_id = str(uuid.uuid5(uuid.NAMESPACE_URL, session_key))
    rng = random.Random(seed)
    design = config.designs[design_id]
    task = config.task_classes[task_class_id]
    profile = config.quote_profiles[quote_profile_id]
    if profile.id != "as_designed" and latency_multiplier != 1.0:
        intervention_type = "quote_and_latency"
    elif profile.id != "as_designed":
        intervention_type = "quote"
    elif latency_multiplier != 1.0:
        intervention_type = "latency"
    else:
        intervention_type = "physical"

    resolver = AccessResolver(config)
    offers, unavailable = resolver.build_offers(design, task, profile)
    decision = SimulatedAgent().choose(task, offers, rng)

    observation = SessionObservation(
        schema_version=config.schema_version,
        price_universe_version=config.price_universe_version,
        session_id=session_id,
        trial_id=trial_id,
        started_at=datetime.now(timezone.utc).isoformat(),
        seed=seed,
        design_id=design.id,
        design_epoch=1,
        task_class_id=task.id,
        quote_profile_id=profile.id,
        intervention_type=intervention_type,
        latency_multiplier=latency_multiplier,
        access_budget=task.access_budget,
        max_accesses=task.max_accesses,
        offers=offers,
        unavailable_representations=unavailable,
        selected_representation_id=decision.selected_representation_id,
        decision_reason=decision.reason,
        decision_scores={
            key: round(value, 8) for key, value in decision.scores.items()
        },
    )

    if decision.selected_representation_id is None:
        return observation

    representation = config.representations[decision.selected_representation_id]
    execution = LocalDataAgent().serve(
        design=design,
        representation=representation,
        latency_multiplier=latency_multiplier,
        rng=rng,
    )
    success_probability = _success_probability(
        quality=representation.quality_for(task.id),
        felt_latency_ms=execution.felt_latency_ms,
        midpoint=task.success_midpoint,
        temperature=task.success_temperature,
        latency_reference_ms=task.latency_reference_ms,
        latency_penalty_per_ms=task.latency_success_penalty_per_ms,
    )
    terminal_success = rng.random() < success_probability
    session_value = (
        (task.task_value if terminal_success else 0.0)
        - config.resource_cost_weight * execution.realized_cost
    )
    return replace(
        observation,
        felt_latency_ms=round(execution.felt_latency_ms, 8),
        realized_cost=round(execution.realized_cost, 8),
        bytes_read=execution.bytes_read,
        terminal_success=terminal_success,
        success_probability=round(success_probability, 8),
        session_value=round(session_value, 8),
    )


def _override_pilot(
    pilot: PilotConfig,
    design_ids: Iterable[str] | None,
    task_class_ids: Iterable[str] | None,
    quote_profile_ids: Iterable[str] | None,
    latency_multipliers: Iterable[float] | None,
    trials_per_cell: int | None,
) -> PilotConfig:
    return PilotConfig(
        design_ids=tuple(design_ids) if design_ids else pilot.design_ids,
        task_class_ids=(
            tuple(task_class_ids) if task_class_ids else pilot.task_class_ids
        ),
        quote_profile_ids=(
            tuple(quote_profile_ids)
            if quote_profile_ids
            else pilot.quote_profile_ids
        ),
        latency_multipliers=(
            tuple(latency_multipliers)
            if latency_multipliers
            else pilot.latency_multipliers
        ),
        trials_per_cell=(
            trials_per_cell
            if trials_per_cell is not None
            else pilot.trials_per_cell
        ),
        base_seed=pilot.base_seed,
    )


def run_pilot(
    config: SystemConfig,
    output_dir: str | Path,
    *,
    design_ids: Iterable[str] | None = None,
    task_class_ids: Iterable[str] | None = None,
    quote_profile_ids: Iterable[str] | None = None,
    latency_multipliers: Iterable[float] | None = None,
    trials_per_cell: int | None = None,
) -> dict[str, object]:
    pilot = _override_pilot(
        config.pilot,
        design_ids,
        task_class_ids,
        quote_profile_ids,
        latency_multipliers,
        trials_per_cell,
    )
    for design_id in pilot.design_ids:
        if design_id not in config.designs:
            raise ValueError(f"unknown pilot design: {design_id}")
    for task_id in pilot.task_class_ids:
        if task_id not in config.task_classes:
            raise ValueError(f"unknown pilot task class: {task_id}")
    for profile_id in pilot.quote_profile_ids:
        if profile_id not in config.quote_profiles:
            raise ValueError(f"unknown pilot quote profile: {profile_id}")
    if pilot.trials_per_cell <= 0:
        raise ValueError("trials_per_cell must be positive")
    if any(value <= 0 for value in pilot.latency_multipliers):
        raise ValueError("latency multipliers must be positive")

    output_path = Path(output_dir)
    sessions_path = output_path / "sessions.jsonl"
    summary_path = output_path / "summary.csv"
    manifest_path = output_path / "manifest.json"
    telemetry = JsonlTelemetryStore(sessions_path)
    telemetry.reset()

    observations: list[SessionObservation] = []
    trial_number = 0
    for design_id in pilot.design_ids:
        for task_id in pilot.task_class_ids:
            for profile_id in pilot.quote_profile_ids:
                for latency_multiplier in pilot.latency_multipliers:
                    for repetition in range(pilot.trials_per_cell):
                        # Use common random numbers across intervention cells.
                        # The trial id remains unique, while repetition r uses
                        # the same seed for every design/quote/latency condition.
                        seed = pilot.base_seed + repetition
                        observation = run_session(
                            config=config,
                            design_id=design_id,
                            task_class_id=task_id,
                            quote_profile_id=profile_id,
                            latency_multiplier=latency_multiplier,
                            seed=seed,
                            trial_id=f"pilot-{trial_number:06d}-r{repetition:03d}",
                        )
                        telemetry.append(observation)
                        observations.append(observation)
                        trial_number += 1

    rows = summarize_observations(
        observations,
        config.representations.keys(),
    )
    write_summary_csv(rows, summary_path)
    manifest = {
        "schema_version": config.schema_version,
        "price_universe_version": config.price_universe_version,
        "config_path": str(config.source_path),
        "config_sha256": sha256(config.source_path.read_bytes()).hexdigest(),
        "design_ids": list(pilot.design_ids),
        "task_class_ids": list(pilot.task_class_ids),
        "quote_profile_ids": list(pilot.quote_profile_ids),
        "latency_multipliers": list(pilot.latency_multipliers),
        "trials_per_cell": pilot.trials_per_cell,
        "base_seed": pilot.base_seed,
        "paired_seeds_across_cells": True,
        "session_count": len(observations),
        "summary_row_count": len(rows),
        "sessions_path": str(sessions_path.resolve()),
        "summary_path": str(summary_path.resolve()),
    }
    write_manifest(manifest, manifest_path)
    return {
        **manifest,
        "manifest_path": str(manifest_path.resolve()),
    }
