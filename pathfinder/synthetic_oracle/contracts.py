from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping


SYNTHETIC_ORACLE_FIXTURE_SCHEMA_VERSION = (
    "pathfinder.synthetic-oracle-fixture/v1alpha1"
)

MINIMUM_FIXTURE_DESIGNS = 4
"""A fixture exists to exercise a multi-candidate domain.

Fewer than four designs reproduces the degenerate one-candidate Reveal set the
fixture was introduced to replace, so it is rejected rather than generated.
"""


class SyntheticFixtureConfigError(ValueError):
    """Raised when a synthetic-fixture contract is invalid."""


@dataclass(frozen=True)
class SyntheticDesignSpec:
    design_id: str
    representation_probabilities: dict[str, float]
    success_probabilities: dict[str, float]
    storage_cost: float
    forward_transition_cost: float
    restoration_cost: float

    def declared_success_rate(self) -> float:
        return sum(
            probability * self.success_probabilities[representation_id]
            for representation_id, probability in (
                self.representation_probabilities.items()
            )
        )


@dataclass(frozen=True)
class SyntheticFixtureConfig:
    schema_version: str
    fixture_id: str
    source_path: Path
    source_sha256: str
    synthetic: bool
    eligible_for_scientific_claims: bool
    fixture_kind: str
    scenario_note: str
    oracle_config_path: Path
    seed: int
    design_ids: tuple[str, ...]
    designs: dict[str, SyntheticDesignSpec]


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise SyntheticFixtureConfigError(f"{name} must be an object")
    return value


def _list(value: Any, name: str) -> list[Any]:
    if not isinstance(value, list):
        raise SyntheticFixtureConfigError(f"{name} must be an array")
    return value


def _string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SyntheticFixtureConfigError(f"{name} must be a non-empty string")
    return value.strip()


def _probability(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SyntheticFixtureConfigError(f"{name} must be a number")
    result = float(value)
    if not math.isfinite(result) or not 0.0 <= result <= 1.0:
        raise SyntheticFixtureConfigError(f"{name} must lie in [0, 1]")
    return result


def _non_negative(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SyntheticFixtureConfigError(f"{name} must be a number")
    result = float(value)
    if not math.isfinite(result) or result < 0.0:
        raise SyntheticFixtureConfigError(
            f"{name} must be finite and non-negative"
        )
    return result


def _positive_integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise SyntheticFixtureConfigError(f"{name} must be a positive integer")
    return value


def _probability_map(
    value: Any,
    name: str,
) -> dict[str, float]:
    raw = _mapping(value, name)
    if not raw:
        raise SyntheticFixtureConfigError(f"{name} cannot be empty")
    return {
        _string(key, f"{name} key"): _probability(item, f"{name}.{key}")
        for key, item in raw.items()
    }


def load_synthetic_fixture_config(
    path: str | Path,
) -> SyntheticFixtureConfig:
    """Load a synthetic-fixture contract, failing closed on mislabelling.

    ``synthetic`` and ``eligible_for_scientific_claims`` are validated rather
    than defaulted. A fixture that could be relabelled as real evidence by
    editing one field would defeat the point of having a separate schema.
    """
    source_path = Path(path).resolve()
    try:
        raw_bytes = source_path.read_bytes()
        raw = json.loads(raw_bytes.decode("utf-8"))
    except FileNotFoundError as exc:
        raise SyntheticFixtureConfigError(
            f"synthetic-fixture configuration does not exist: {source_path}"
        ) from exc
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SyntheticFixtureConfigError(
            f"invalid synthetic-fixture configuration {source_path}: {exc}"
        ) from exc

    root = _mapping(raw, "synthetic-fixture configuration")
    if root.get("schema_version") != SYNTHETIC_ORACLE_FIXTURE_SCHEMA_VERSION:
        raise SyntheticFixtureConfigError(
            "unsupported synthetic-fixture schema_version"
        )
    fixture_id = _string(root.get("fixture_id"), "fixture_id")
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", fixture_id) is None:
        raise SyntheticFixtureConfigError(
            "fixture_id may contain only letters, digits, '.', '_', and '-'"
        )
    if root.get("synthetic") is not True:
        raise SyntheticFixtureConfigError(
            "a synthetic fixture must declare synthetic=true"
        )
    if root.get("eligible_for_scientific_claims") is not False:
        raise SyntheticFixtureConfigError(
            "a synthetic fixture must declare "
            "eligible_for_scientific_claims=false"
        )

    shared_success = _probability_map(
        root.get("representation_success_probabilities"),
        "representation_success_probabilities",
    )

    designs: dict[str, SyntheticDesignSpec] = {}
    for index, raw_design in enumerate(
        _list(root.get("designs"), "designs")
    ):
        item = _mapping(raw_design, f"designs[{index}]")
        design_id = _string(
            item.get("design_id"),
            f"designs[{index}].design_id",
        )
        if design_id in designs:
            raise SyntheticFixtureConfigError(
                f"duplicate synthetic design: {design_id}"
            )
        probabilities = _probability_map(
            item.get("representation_probabilities"),
            f"designs[{index}].representation_probabilities",
        )
        total = sum(probabilities.values())
        if not math.isclose(total, 1.0, abs_tol=1e-9):
            raise SyntheticFixtureConfigError(
                f"designs[{index}].representation_probabilities must sum to "
                f"1, not {total!r}"
            )
        success = dict(shared_success)
        success.update(
            _probability_map(
                item.get("success_probabilities", {}),
                f"designs[{index}].success_probabilities",
            )
            if item.get("success_probabilities")
            else {}
        )
        missing = set(probabilities) - set(success)
        if missing:
            raise SyntheticFixtureConfigError(
                f"designs[{index}] has no success probability for: "
                + ", ".join(sorted(missing))
            )
        designs[design_id] = SyntheticDesignSpec(
            design_id=design_id,
            representation_probabilities=probabilities,
            success_probabilities=success,
            storage_cost=_non_negative(
                item.get("storage_cost", 0.0),
                f"designs[{index}].storage_cost",
            ),
            forward_transition_cost=_non_negative(
                item.get("forward_transition_cost", 0.0),
                f"designs[{index}].forward_transition_cost",
            ),
            restoration_cost=_non_negative(
                item.get("restoration_cost", 0.0),
                f"designs[{index}].restoration_cost",
            ),
        )
    if len(designs) < MINIMUM_FIXTURE_DESIGNS:
        raise SyntheticFixtureConfigError(
            "a synthetic fixture requires at least "
            f"{MINIMUM_FIXTURE_DESIGNS} designs"
        )

    design_ids = tuple(
        _string(value, "design_order")
        for value in _list(
            root.get("design_order", list(designs)),
            "design_order",
        )
    )
    if len(design_ids) != len(set(design_ids)) or set(design_ids) != set(
        designs
    ):
        raise SyntheticFixtureConfigError(
            "design_order must contain every declared design exactly once"
        )

    oracle_config = Path(_string(root.get("oracle_config"), "oracle_config"))
    if not oracle_config.is_absolute():
        oracle_config = source_path.parent / oracle_config
    return SyntheticFixtureConfig(
        schema_version=SYNTHETIC_ORACLE_FIXTURE_SCHEMA_VERSION,
        fixture_id=fixture_id,
        source_path=source_path,
        source_sha256=sha256(raw_bytes).hexdigest(),
        synthetic=True,
        eligible_for_scientific_claims=False,
        fixture_kind=_string(
            root.get("fixture_kind", "engineering-fixture"),
            "fixture_kind",
        ),
        scenario_note=_string(root.get("scenario_note"), "scenario_note"),
        oracle_config_path=oracle_config.resolve(),
        seed=_positive_integer(root.get("seed"), "seed"),
        design_ids=design_ids,
        designs=designs,
    )
