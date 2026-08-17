from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping


OED_CONFIG_SCHEMA_VERSION = "pathfinder.oed/v1alpha1"
REVEAL_TIERS = (
    "simultaneous-canonical",
    "pair-canonical",
    "fallback",
)


class OEDConfigError(ValueError):
    """Raised when an OED controller contract is invalid."""


@dataclass(frozen=True)
class RevealCandidateConfig:
    design_id: str
    reveal_tier: str
    probe_window_loss: float


@dataclass(frozen=True)
class OEDConfig:
    schema_version: str
    controller_id: str
    source_path: Path
    source_sha256: str
    commit_margin: float
    reveal_margin: float
    exploration_budget: float
    per_excursion_cap: float
    max_iterations: int
    random_seed: int
    cost_model_status: str
    reveal_candidates: dict[str, RevealCandidateConfig]
    other_design_ids: tuple[str, ...]


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise OEDConfigError(f"{name} must be an object")
    return value


def _list(value: Any, name: str) -> list[Any]:
    if not isinstance(value, list):
        raise OEDConfigError(f"{name} must be an array")
    return value


def _string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise OEDConfigError(f"{name} must be a non-empty string")
    return value.strip()


def _number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise OEDConfigError(f"{name} must be a number")
    result = float(value)
    if not math.isfinite(result) or result < 0.0:
        raise OEDConfigError(f"{name} must be finite and non-negative")
    return result


def _positive_integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise OEDConfigError(f"{name} must be a positive integer")
    return value


def load_oed_config(path: str | Path) -> OEDConfig:
    source_path = Path(path).resolve()
    try:
        raw_bytes = source_path.read_bytes()
        raw = json.loads(raw_bytes.decode("utf-8"))
    except FileNotFoundError as exc:
        raise OEDConfigError(
            f"OED configuration does not exist: {source_path}"
        ) from exc
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OEDConfigError(
            f"invalid OED configuration {source_path}: {exc}"
        ) from exc
    root = _mapping(raw, "OED configuration")
    if root.get("schema_version") != OED_CONFIG_SCHEMA_VERSION:
        raise OEDConfigError("unsupported OED schema_version")
    controller_id = _string(root.get("controller_id"), "controller_id")
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", controller_id) is None:
        raise OEDConfigError(
            "controller_id may contain only letters, digits, '.', '_', "
            "and '-'"
        )

    candidates: dict[str, RevealCandidateConfig] = {}
    for index, raw_candidate in enumerate(
        _list(root.get("reveal_candidates", []), "reveal_candidates")
    ):
        item = _mapping(raw_candidate, f"reveal_candidates[{index}]")
        design_id = _string(
            item.get("design_id"),
            f"reveal_candidates[{index}].design_id",
        )
        if design_id in candidates:
            raise OEDConfigError(f"duplicate Reveal candidate: {design_id}")
        tier = _string(
            item.get("reveal_tier", "fallback"),
            f"reveal_candidates[{index}].reveal_tier",
        )
        if tier not in REVEAL_TIERS:
            raise OEDConfigError(
                f"unsupported Reveal tier for {design_id}: {tier}"
            )
        candidates[design_id] = RevealCandidateConfig(
            design_id=design_id,
            reveal_tier=tier,
            probe_window_loss=_number(
                item.get("probe_window_loss", 0.0),
                f"reveal_candidates[{index}].probe_window_loss",
            ),
        )

    other_design_ids = tuple(
        _string(value, "other_design_ids")
        for value in _list(root.get("other_design_ids", []), "other_design_ids")
    )
    if len(other_design_ids) != len(set(other_design_ids)):
        raise OEDConfigError("other_design_ids contains duplicates")
    overlap = set(candidates).intersection(other_design_ids)
    if overlap:
        raise OEDConfigError(
            "Reveal and other design sets overlap: "
            + ", ".join(sorted(overlap))
        )

    budget = _number(root.get("exploration_budget"), "exploration_budget")
    cap = _number(root.get("per_excursion_cap"), "per_excursion_cap")
    return OEDConfig(
        schema_version=OED_CONFIG_SCHEMA_VERSION,
        controller_id=controller_id,
        source_path=source_path,
        source_sha256=sha256(raw_bytes).hexdigest(),
        commit_margin=_number(root.get("commit_margin", 0.0), "commit_margin"),
        reveal_margin=_number(root.get("reveal_margin", 0.0), "reveal_margin"),
        exploration_budget=budget,
        per_excursion_cap=cap,
        max_iterations=_positive_integer(
            root.get("max_iterations", 32),
            "max_iterations",
        ),
        random_seed=_positive_integer(root.get("random_seed", 1), "random_seed"),
        cost_model_status=_string(
            root.get("cost_model_status", "unspecified"),
            "cost_model_status",
        ),
        reveal_candidates=candidates,
        other_design_ids=other_design_ids,
    )
