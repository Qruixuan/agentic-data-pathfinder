from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping


REDUCED_ORACLE_SCHEMA_VERSION = "pathfinder.reduced-oracle/v1alpha1"


class ReducedOracleConfigError(ValueError):
    """Raised when a reduced-oracle contract is invalid."""


@dataclass(frozen=True)
class MaterializationSpec:
    representation_id: str
    source_template: str
    target_template: str


@dataclass(frozen=True)
class OracleDesignSpec:
    design_id: str
    materializations: tuple[MaterializationSpec, ...]
    materialization_decision: str
    placement_decision: str
    execution_decision: str
    fixed_storage_cost: float = 0.0


@dataclass(frozen=True)
class TransitionCostModel:
    copy_cost_per_gib: float
    elapsed_time_cost_per_second: float
    foreground_loss_per_transition: float
    storage_cost_per_gib_hour: float


@dataclass(frozen=True)
class NaiveBaselineSpec:
    candidate_design_id: str
    representation_id: str
    decision_margin: float


@dataclass(frozen=True)
class ReducedOracleConfig:
    schema_version: str
    oracle_id: str
    source_path: Path
    source_sha256: str
    workload_pilot_config_path: Path
    safe_design_id: str
    design_ids: tuple[str, ...]
    designs: dict[str, OracleDesignSpec]
    quote_profile_id: str
    latency_multiplier: float
    repetitions: int
    base_seed: int
    randomization_seed: int
    horizon_sessions: int
    horizon_hours: float
    minimum_completion_rate: float
    materialization_root: Path
    cost_model_status: str
    transition_cost: TransitionCostModel
    naive_baseline: NaiveBaselineSpec


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReducedOracleConfigError(f"{name} must be an object")
    return value


def _list(value: Any, name: str) -> list[Any]:
    if not isinstance(value, list):
        raise ReducedOracleConfigError(f"{name} must be an array")
    return value


def _string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ReducedOracleConfigError(f"{name} must be a non-empty string")
    return value.strip()


def _finite_number(
    value: Any,
    name: str,
    *,
    minimum: float = 0.0,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ReducedOracleConfigError(f"{name} must be a number")
    result = float(value)
    if not math.isfinite(result) or result < minimum:
        raise ReducedOracleConfigError(
            f"{name} must be finite and at least {minimum}"
        )
    return result


def _positive_integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ReducedOracleConfigError(f"{name} must be a positive integer")
    return value


def _resolve_path(parent: Path, value: Any, name: str) -> Path:
    path = Path(_string(value, name))
    if not path.is_absolute():
        path = parent / path
    return path.resolve()


def load_reduced_oracle_config(
    path: str | Path,
) -> ReducedOracleConfig:
    source_path = Path(path).resolve()
    try:
        raw_bytes = source_path.read_bytes()
        raw = json.loads(raw_bytes.decode("utf-8"))
    except FileNotFoundError as exc:
        raise ReducedOracleConfigError(
            f"reduced-oracle configuration does not exist: {source_path}"
        ) from exc
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReducedOracleConfigError(
            f"invalid reduced-oracle configuration {source_path}: {exc}"
        ) from exc
    root = _mapping(raw, "reduced-oracle configuration")
    if root.get("schema_version") != REDUCED_ORACLE_SCHEMA_VERSION:
        raise ReducedOracleConfigError(
            "unsupported reduced-oracle schema_version"
        )
    oracle_id = _string(root.get("oracle_id"), "oracle_id")
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", oracle_id) is None:
        raise ReducedOracleConfigError(
            "oracle_id may contain only letters, digits, '.', '_', and '-'"
        )

    parent = source_path.parent
    raw_designs = _list(root.get("designs"), "designs")
    designs: dict[str, OracleDesignSpec] = {}
    for index, raw_design in enumerate(raw_designs):
        item = _mapping(raw_design, f"designs[{index}]")
        design_id = _string(item.get("design_id"), f"designs[{index}].design_id")
        if design_id in designs:
            raise ReducedOracleConfigError(f"duplicate design: {design_id}")
        materializations: list[MaterializationSpec] = []
        for materialization_index, raw_materialization in enumerate(
            _list(item.get("materializations", []), "materializations")
        ):
            materialization = _mapping(
                raw_materialization,
                f"designs[{index}].materializations[{materialization_index}]",
            )
            materializations.append(
                MaterializationSpec(
                    representation_id=_string(
                        materialization.get("representation_id"),
                        "materialization.representation_id",
                    ),
                    source_template=_string(
                        materialization.get("source_template"),
                        "materialization.source_template",
                    ),
                    target_template=_string(
                        materialization.get("target_template"),
                        "materialization.target_template",
                    ),
                )
            )
        designs[design_id] = OracleDesignSpec(
            design_id=design_id,
            materializations=tuple(materializations),
            materialization_decision=_string(
                item.get("materialization_decision"),
                f"designs[{index}].materialization_decision",
            ),
            placement_decision=_string(
                item.get("placement_decision"),
                f"designs[{index}].placement_decision",
            ),
            execution_decision=_string(
                item.get("execution_decision"),
                f"designs[{index}].execution_decision",
            ),
            fixed_storage_cost=_finite_number(
                item.get("fixed_storage_cost", 0.0),
                f"designs[{index}].fixed_storage_cost",
            ),
        )
    if len(designs) < 2:
        raise ReducedOracleConfigError(
            "a reduced oracle requires at least two designs"
        )

    safe_design_id = _string(root.get("safe_design_id"), "safe_design_id")
    if safe_design_id not in designs:
        raise ReducedOracleConfigError("safe_design_id is not declared")
    raw_order = root.get("design_order", list(designs))
    design_ids = tuple(
        _string(value, "design_order")
        for value in _list(raw_order, "design_order")
    )
    if len(design_ids) != len(set(design_ids)) or set(design_ids) != set(
        designs
    ):
        raise ReducedOracleConfigError(
            "design_order must contain every declared design exactly once"
        )
    if design_ids[0] != safe_design_id:
        raise ReducedOracleConfigError(
            "design_order must start with the certified safe design"
        )

    cost = _mapping(root.get("transition_cost"), "transition_cost")
    baseline = _mapping(root.get("naive_baseline"), "naive_baseline")
    candidate_design_id = _string(
        baseline.get("candidate_design_id"),
        "naive_baseline.candidate_design_id",
    )
    if candidate_design_id not in designs or candidate_design_id == safe_design_id:
        raise ReducedOracleConfigError(
            "naive baseline candidate must be a non-safe declared design"
        )

    minimum_completion_rate = _finite_number(
        root.get("minimum_completion_rate", 0.95),
        "minimum_completion_rate",
    )
    if minimum_completion_rate > 1.0:
        raise ReducedOracleConfigError(
            "minimum_completion_rate cannot exceed 1"
        )
    return ReducedOracleConfig(
        schema_version=REDUCED_ORACLE_SCHEMA_VERSION,
        oracle_id=oracle_id,
        source_path=source_path,
        source_sha256=sha256(raw_bytes).hexdigest(),
        workload_pilot_config_path=_resolve_path(
            parent,
            root.get("workload_pilot_config"),
            "workload_pilot_config",
        ),
        safe_design_id=safe_design_id,
        design_ids=design_ids,
        designs=designs,
        quote_profile_id=_string(
            root.get("quote_profile_id", "as_designed"),
            "quote_profile_id",
        ),
        latency_multiplier=_finite_number(
            root.get("latency_multiplier", 1.0),
            "latency_multiplier",
            minimum=1e-12,
        ),
        repetitions=_positive_integer(root.get("repetitions", 1), "repetitions"),
        base_seed=_positive_integer(root.get("base_seed", 1), "base_seed"),
        randomization_seed=_positive_integer(
            root.get("randomization_seed", 1),
            "randomization_seed",
        ),
        horizon_sessions=_positive_integer(
            root.get("horizon_sessions"),
            "horizon_sessions",
        ),
        horizon_hours=_finite_number(
            root.get("horizon_hours"),
            "horizon_hours",
            minimum=1e-12,
        ),
        minimum_completion_rate=minimum_completion_rate,
        materialization_root=_resolve_path(
            parent,
            root.get("materialization_root"),
            "materialization_root",
        ),
        cost_model_status=_string(
            root.get("cost_model_status", "unspecified"),
            "cost_model_status",
        ),
        transition_cost=TransitionCostModel(
            copy_cost_per_gib=_finite_number(
                cost.get("copy_cost_per_gib", 0.0),
                "transition_cost.copy_cost_per_gib",
            ),
            elapsed_time_cost_per_second=_finite_number(
                cost.get("elapsed_time_cost_per_second", 0.0),
                "transition_cost.elapsed_time_cost_per_second",
            ),
            foreground_loss_per_transition=_finite_number(
                cost.get("foreground_loss_per_transition", 0.0),
                "transition_cost.foreground_loss_per_transition",
            ),
            storage_cost_per_gib_hour=_finite_number(
                cost.get("storage_cost_per_gib_hour", 0.0),
                "transition_cost.storage_cost_per_gib_hour",
            ),
        ),
        naive_baseline=NaiveBaselineSpec(
            candidate_design_id=candidate_design_id,
            representation_id=_string(
                baseline.get("representation_id"),
                "naive_baseline.representation_id",
            ),
            decision_margin=_finite_number(
                baseline.get("decision_margin", 0.0),
                "naive_baseline.decision_margin",
            ),
        ),
    )
