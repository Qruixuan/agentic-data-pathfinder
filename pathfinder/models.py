from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Representation:
    id: str
    description: str
    size_bytes: int
    task_quality: dict[str, float]

    def quality_for(self, task_class_id: str) -> float:
        return self.task_quality.get(task_class_id, 0.0)


@dataclass(frozen=True)
class TaskClass:
    id: str
    description: str
    candidate_representations: tuple[str, ...]
    access_budget: float
    max_accesses: int
    task_value: float
    quality_weight: float
    price_weight: float
    outside_option_utility: float
    choice_noise: float
    success_midpoint: float
    success_temperature: float
    latency_reference_ms: float
    latency_success_penalty_per_ms: float


@dataclass(frozen=True)
class PathSpec:
    available: bool
    location: str
    latency_ms: float
    latency_jitter_ms: float
    realized_cost: float
    quotes: dict[str, float]


@dataclass(frozen=True)
class PhysicalDesign:
    id: str
    description: str
    paths: dict[str, PathSpec]


@dataclass(frozen=True)
class QuoteProfile:
    id: str
    description: str
    overrides: dict[str, dict[str, float]]

    def quote_for(
        self,
        task_class_id: str,
        representation_id: str,
        default: float,
    ) -> float:
        return self.overrides.get(task_class_id, {}).get(
            representation_id,
            default,
        )


@dataclass(frozen=True)
class PilotConfig:
    design_ids: tuple[str, ...]
    task_class_ids: tuple[str, ...]
    quote_profile_ids: tuple[str, ...]
    latency_multipliers: tuple[float, ...]
    trials_per_cell: int
    base_seed: int


@dataclass(frozen=True)
class SystemConfig:
    schema_version: str
    price_universe_version: str
    source_path: Path
    representations: dict[str, Representation]
    task_classes: dict[str, TaskClass]
    designs: dict[str, PhysicalDesign]
    quote_profiles: dict[str, QuoteProfile]
    price_universes: dict[str, dict[str, tuple[float, ...]]]
    pilot: PilotConfig
    resource_cost_weight: float

    def price_universe(
        self,
        task_class_id: str,
        representation_id: str,
    ) -> tuple[float, ...]:
        return self.price_universes[task_class_id][representation_id]


@dataclass(frozen=True)
class AccessOffer:
    representation_id: str
    quoted_price: float
    affordable: bool
    location: str
    expected_latency_ms: float
    task_quality: float


@dataclass(frozen=True)
class AccessDecision:
    selected_representation_id: str | None
    reason: str
    scores: dict[str, float]


@dataclass(frozen=True)
class AccessExecution:
    representation_id: str
    location: str
    felt_latency_ms: float
    realized_cost: float
    bytes_read: int


@dataclass
class SessionObservation:
    schema_version: str
    price_universe_version: str
    session_id: str
    trial_id: str
    started_at: str
    seed: int
    design_id: str
    design_epoch: int
    task_class_id: str
    quote_profile_id: str
    intervention_type: str
    latency_multiplier: float
    access_budget: float
    max_accesses: int
    offers: list[AccessOffer] = field(default_factory=list)
    unavailable_representations: list[str] = field(default_factory=list)
    selected_representation_id: str | None = None
    decision_reason: str = ""
    decision_scores: dict[str, float] = field(default_factory=dict)
    felt_latency_ms: float | None = None
    realized_cost: float = 0.0
    bytes_read: int = 0
    terminal_success: bool = False
    success_probability: float = 0.0
    session_value: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
