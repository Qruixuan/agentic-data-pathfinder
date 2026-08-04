from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .models import (
    PathSpec,
    PhysicalDesign,
    PilotConfig,
    QuoteProfile,
    Representation,
    SystemConfig,
    TaskClass,
)


class ConfigError(ValueError):
    """Raised when the experiment configuration violates its contract."""


def _require_mapping(raw: Any, name: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ConfigError(f"{name} must be a JSON object")
    return raw


def _require_list(raw: Any, name: str) -> list[Any]:
    if not isinstance(raw, list):
        raise ConfigError(f"{name} must be a JSON array")
    return raw


def _unique(items: list[Any], name: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for item in items:
        item_id = str(_require_mapping(item, name).get("id", "")).strip()
        if not item_id:
            raise ConfigError(f"every {name} entry requires a non-empty id")
        if item_id in result:
            raise ConfigError(f"duplicate {name} id: {item_id}")
        result[item_id] = item
    return result


def load_config(path: str | Path) -> SystemConfig:
    source_path = Path(path).resolve()
    try:
        raw = json.loads(source_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ConfigError(f"configuration file does not exist: {source_path}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigError(f"invalid JSON in {source_path}: {exc}") from exc

    root = _require_mapping(raw, "configuration")
    raw_representations = _unique(
        _require_list(root.get("representations"), "representations"),
        "representation",
    )
    representations = {
        item_id: Representation(
            id=item_id,
            description=str(item.get("description", "")),
            size_bytes=int(item["size_bytes"]),
            task_quality={
                str(task_id): float(value)
                for task_id, value in _require_mapping(
                    item.get("task_quality"),
                    f"representations.{item_id}.task_quality",
                ).items()
            },
        )
        for item_id, item in raw_representations.items()
    }

    raw_tasks = _unique(
        _require_list(root.get("task_classes"), "task_classes"),
        "task class",
    )
    task_classes = {
        item_id: TaskClass(
            id=item_id,
            description=str(item.get("description", "")),
            candidate_representations=tuple(
                str(value)
                for value in _require_list(
                    item.get("candidate_representations"),
                    f"task_classes.{item_id}.candidate_representations",
                )
            ),
            access_budget=float(item["access_budget"]),
            max_accesses=int(item.get("max_accesses", 1)),
            task_value=float(item["task_value"]),
            quality_weight=float(item["quality_weight"]),
            price_weight=float(item["price_weight"]),
            outside_option_utility=float(item["outside_option_utility"]),
            choice_noise=float(item.get("choice_noise", 0.0)),
            success_midpoint=float(item["success_midpoint"]),
            success_temperature=float(item["success_temperature"]),
            latency_reference_ms=float(item.get("latency_reference_ms", 0.0)),
            latency_success_penalty_per_ms=float(
                item.get("latency_success_penalty_per_ms", 0.0)
            ),
        )
        for item_id, item in raw_tasks.items()
    }

    raw_designs = _unique(
        _require_list(root.get("physical_designs"), "physical_designs"),
        "physical design",
    )
    designs: dict[str, PhysicalDesign] = {}
    for design_id, item in raw_designs.items():
        raw_paths = _require_mapping(
            item.get("paths"),
            f"physical_designs.{design_id}.paths",
        )
        paths = {
            str(rep_id): PathSpec(
                available=bool(path["available"]),
                location=str(path["location"]),
                latency_ms=float(path["latency_ms"]),
                latency_jitter_ms=float(path.get("latency_jitter_ms", 0.0)),
                realized_cost=float(path["realized_cost"]),
                quotes={
                    str(task_id): float(value)
                    for task_id, value in _require_mapping(
                        path.get("quotes", {}),
                        f"physical_designs.{design_id}.paths.{rep_id}.quotes",
                    ).items()
                },
            )
            for rep_id, path_raw in raw_paths.items()
            for path in [
                _require_mapping(
                    path_raw,
                    f"physical_designs.{design_id}.paths.{rep_id}",
                )
            ]
        }
        designs[design_id] = PhysicalDesign(
            id=design_id,
            description=str(item.get("description", "")),
            paths=paths,
        )

    raw_profiles = _unique(
        _require_list(root.get("quote_profiles"), "quote_profiles"),
        "quote profile",
    )
    quote_profiles = {
        item_id: QuoteProfile(
            id=item_id,
            description=str(item.get("description", "")),
            overrides={
                str(task_id): {
                    str(rep_id): float(value)
                    for rep_id, value in _require_mapping(
                        task_overrides,
                        f"quote_profiles.{item_id}.overrides.{task_id}",
                    ).items()
                }
                for task_id, task_overrides in _require_mapping(
                    item.get("overrides", {}),
                    f"quote_profiles.{item_id}.overrides",
                ).items()
            },
        )
        for item_id, item in raw_profiles.items()
    }

    price_universes = {
        str(task_id): {
            str(rep_id): tuple(
                float(value)
                for value in _require_list(
                    universe,
                    f"price_universes.{task_id}.{rep_id}",
                )
            )
            for rep_id, universe in _require_mapping(
                task_universes,
                f"price_universes.{task_id}",
            ).items()
        }
        for task_id, task_universes in _require_mapping(
            root.get("price_universes"),
            "price_universes",
        ).items()
    }

    raw_pilot = _require_mapping(root.get("pilot"), "pilot")
    pilot = PilotConfig(
        design_ids=tuple(str(value) for value in raw_pilot["design_ids"]),
        task_class_ids=tuple(str(value) for value in raw_pilot["task_class_ids"]),
        quote_profile_ids=tuple(
            str(value) for value in raw_pilot["quote_profile_ids"]
        ),
        latency_multipliers=tuple(
            float(value) for value in raw_pilot["latency_multipliers"]
        ),
        trials_per_cell=int(raw_pilot["trials_per_cell"]),
        base_seed=int(raw_pilot.get("base_seed", 1)),
    )

    objective = _require_mapping(root.get("objective", {}), "objective")
    config = SystemConfig(
        schema_version=str(root.get("schema_version", "1.0")),
        price_universe_version=str(
            root.get("price_universe_version", "")
        ).strip(),
        source_path=source_path,
        representations=representations,
        task_classes=task_classes,
        designs=designs,
        quote_profiles=quote_profiles,
        price_universes=price_universes,
        pilot=pilot,
        resource_cost_weight=float(objective.get("resource_cost_weight", 1.0)),
    )
    _validate_config(config)
    return config


def _contains_price(universe: tuple[float, ...], price: float) -> bool:
    return any(abs(candidate - price) < 1e-9 for candidate in universe)


def _validate_config(config: SystemConfig) -> None:
    if not config.representations:
        raise ConfigError("at least one representation is required")
    if not config.price_universe_version:
        raise ConfigError("price_universe_version is required")
    if not config.task_classes:
        raise ConfigError("at least one task class is required")
    if not config.designs:
        raise ConfigError("at least one physical design is required")
    if "as_designed" not in config.quote_profiles:
        raise ConfigError("quote profile 'as_designed' is required")

    for rep in config.representations.values():
        if rep.size_bytes < 0:
            raise ConfigError(f"representation {rep.id} has a negative size")
        for task_id, quality in rep.task_quality.items():
            if task_id not in config.task_classes:
                raise ConfigError(
                    f"representation {rep.id} references unknown task {task_id}"
                )
            if not 0.0 <= quality <= 1.0:
                raise ConfigError(
                    f"quality for {rep.id}/{task_id} must be in [0, 1]"
                )

    for task in config.task_classes.values():
        if task.access_budget < 0:
            raise ConfigError(f"task {task.id} has a negative access budget")
        if task.max_accesses != 1:
            raise ConfigError(
                "the minimal harness currently supports max_accesses = 1"
            )
        if task.success_temperature <= 0:
            raise ConfigError(
                f"task {task.id} requires success_temperature > 0"
            )
        if not task.candidate_representations:
            raise ConfigError(f"task {task.id} requires candidate representations")
        for rep_id in task.candidate_representations:
            if rep_id not in config.representations:
                raise ConfigError(
                    f"task {task.id} references unknown representation {rep_id}"
                )
            try:
                universe = config.price_universe(task.id, rep_id)
            except KeyError as exc:
                raise ConfigError(
                    f"missing price universe for {task.id}/{rep_id}"
                ) from exc
            if not universe:
                raise ConfigError(
                    f"price universe for {task.id}/{rep_id} cannot be empty"
                )
            if any(price < 0 for price in universe):
                raise ConfigError(
                    f"price universe for {task.id}/{rep_id} contains a negative price"
                )
            if len(set(universe)) != len(universe):
                raise ConfigError(
                    f"price universe for {task.id}/{rep_id} contains duplicates"
                )

    for design in config.designs.values():
        for rep_id, path in design.paths.items():
            if rep_id not in config.representations:
                raise ConfigError(
                    f"design {design.id} references unknown representation {rep_id}"
                )
            if path.latency_ms < 0 or path.latency_jitter_ms < 0:
                raise ConfigError(
                    f"design {design.id}/{rep_id} has a negative latency"
                )
            if path.realized_cost < 0:
                raise ConfigError(
                    f"design {design.id}/{rep_id} has a negative realized cost"
                )
            for task_id, quote in path.quotes.items():
                if task_id not in config.task_classes:
                    raise ConfigError(
                        f"design {design.id}/{rep_id} references unknown task {task_id}"
                    )
                if rep_id not in config.task_classes[task_id].candidate_representations:
                    raise ConfigError(
                        f"design {design.id}/{rep_id} quotes task {task_id}, "
                        "but the representation is not a candidate for that task"
                    )
                if not _contains_price(
                    config.price_universe(task_id, rep_id),
                    quote,
                ):
                    raise ConfigError(
                        f"quote {quote} for {design.id}/{task_id}/{rep_id} "
                        "is outside the predeclared P_qv"
                    )

    for profile in config.quote_profiles.values():
        for task_id, overrides in profile.overrides.items():
            if task_id not in config.task_classes:
                raise ConfigError(
                    f"quote profile {profile.id} references unknown task {task_id}"
                )
            for rep_id, quote in overrides.items():
                if rep_id not in config.representations:
                    raise ConfigError(
                        f"quote profile {profile.id} references unknown "
                        f"representation {rep_id}"
                    )
                if not _contains_price(
                    config.price_universe(task_id, rep_id),
                    quote,
                ):
                    raise ConfigError(
                        f"quote {quote} for {profile.id}/{task_id}/{rep_id} "
                        "is outside the predeclared P_qv"
                    )

    for design_id in config.pilot.design_ids:
        if design_id not in config.designs:
            raise ConfigError(f"pilot references unknown design {design_id}")
    for task_id in config.pilot.task_class_ids:
        if task_id not in config.task_classes:
            raise ConfigError(f"pilot references unknown task {task_id}")
    for profile_id in config.pilot.quote_profile_ids:
        if profile_id not in config.quote_profiles:
            raise ConfigError(f"pilot references unknown quote profile {profile_id}")
    if config.pilot.trials_per_cell <= 0:
        raise ConfigError("pilot.trials_per_cell must be positive")
    if any(value <= 0 for value in config.pilot.latency_multipliers):
        raise ConfigError("pilot latency multipliers must be positive")
