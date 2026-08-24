from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping


AWM_CONFIG_SCHEMA_VERSION = "pathfinder.awm/v1alpha1"
AWM_CONFIG_SCHEMA_VERSION_V2 = "pathfinder.awm/v2alpha1"
AWM_CONFIG_SCHEMA_VERSION_V2_1 = "pathfinder.awm/v2alpha2"
AWM_CONFIG_SCHEMA_VERSION_V3 = "pathfinder.awm/v3alpha1"
AWM_CONFIG_SCHEMA_VERSION_V3_1 = "pathfinder.awm/v3alpha2"
AWM_CONFIG_SCHEMA_VERSIONS = (
    AWM_CONFIG_SCHEMA_VERSION,
    AWM_CONFIG_SCHEMA_VERSION_V2,
    AWM_CONFIG_SCHEMA_VERSION_V2_1,
    AWM_CONFIG_SCHEMA_VERSION_V3,
    AWM_CONFIG_SCHEMA_VERSION_V3_1,
)
CONFIDENCE_FAMILY_MODES = (
    "dynamic-observed-v1",
    "fixed-full-domain",
)
PAIRED_GAIN_METHODS = (
    "disabled",
    "fixed-looks-empirical-bernstein",
    "fixed-snapshot-empirical-bernstein",
    "cluster-first-decomposed-kl-empirical-bernstein",
    "cluster-mean-decomposed-bounded-kl-empirical-bernstein",
)
PAIRED_LOOK_SEMANTICS = (
    "controller-iteration-upper-bound",
    "fixed-training-snapshot-per-pair",
)
_ASSUMPTION_NAMES = (
    "own_price_monotonicity",
    "substitution_group_monotonicity",
    "success_monotonicity",
    "quoted_price_sufficiency",
)


class AWMConfigError(ValueError):
    """Raised when an AWM configuration is invalid."""


@dataclass(frozen=True)
class AssumptionConfig:
    enabled: bool
    status: str


@dataclass(frozen=True)
class ConfidenceConfig:
    family_mode: str
    marginal_alpha_fraction: float
    paired_gain_alpha_fraction: float
    paired_gain_method: str
    paired_gain_minimum_pairs: int
    maximum_looks: int
    require_complete_pairs: bool
    paired_comparisons: tuple[tuple[str, str], ...]
    look_semantics: str
    sampling_unit: str = "paired-workload-repetition-seed"
    cluster_key_fields: tuple[str, ...] = ()
    cluster_reduction: str = "disabled"
    success_alpha_fraction: float = 0.0
    cost_alpha_fraction: float = 0.0

    @property
    def paired_gain_enabled(self) -> bool:
        return self.paired_gain_method != "disabled"


@dataclass(frozen=True)
class AWMConfig:
    schema_version: str
    model_id: str
    source_path: Path
    source_sha256: str
    confidence_level: float
    holdout_repetitions: int
    observed_design_ids: tuple[str, ...]
    minimum_training_sessions: int
    maximum_excluded_training_fraction: float
    maximum_excluded_holdout_fraction: float
    substitution_groups: tuple[tuple[str, ...], ...]
    assumptions: dict[str, AssumptionConfig]
    own_price_requires_other_quotes_equal: bool
    cost_relative_radius: float
    transition_relative_radius: float
    commit_margin: float
    confidence: ConfidenceConfig

    def assumption_enabled(self, name: str) -> bool:
        try:
            return self.assumptions[name].enabled
        except KeyError as exc:
            raise AWMConfigError(f"unknown AWM assumption: {name}") from exc


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise AWMConfigError(f"{name} must be an object")
    return value


def _list(value: Any, name: str) -> list[Any]:
    if not isinstance(value, list):
        raise AWMConfigError(f"{name} must be an array")
    return value


def _string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AWMConfigError(f"{name} must be a non-empty string")
    return value.strip()


def _boolean(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise AWMConfigError(f"{name} must be a boolean")
    return value


def _finite_number(
    value: Any,
    name: str,
    *,
    minimum: float = 0.0,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AWMConfigError(f"{name} must be a number")
    result = float(value)
    if not math.isfinite(result) or result < minimum:
        raise AWMConfigError(
            f"{name} must be finite and at least {minimum}"
        )
    return result


def _nonnegative_integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise AWMConfigError(f"{name} must be a non-negative integer")
    return value


def _fraction(value: Any, name: str) -> float:
    result = _finite_number(value, name)
    if result > 1.0:
        raise AWMConfigError(f"{name} cannot exceed 1")
    return result


def _positive_integer(value: Any, name: str) -> int:
    result = _nonnegative_integer(value, name)
    if result == 0:
        raise AWMConfigError(f"{name} must be a positive integer")
    return result


def _load_confidence_config(
    root: Mapping[str, Any],
    *,
    schema_version: str,
) -> ConfidenceConfig:
    if schema_version == AWM_CONFIG_SCHEMA_VERSION:
        if "confidence" in root:
            raise AWMConfigError(
                "confidence is available only in AWM v2/v3 schemas"
            )
        return ConfidenceConfig(
            family_mode="dynamic-observed-v1",
            marginal_alpha_fraction=1.0,
            paired_gain_alpha_fraction=0.0,
            paired_gain_method="disabled",
            paired_gain_minimum_pairs=0,
            maximum_looks=1,
            require_complete_pairs=False,
            paired_comparisons=(),
            look_semantics="controller-iteration-upper-bound",
        )

    raw = _mapping(root.get("confidence"), "confidence")
    family_mode = _string(
        raw.get("family_mode"),
        "confidence.family_mode",
    )
    if family_mode != "fixed-full-domain":
        raise AWMConfigError(
            "v2/v3 confidence.family_mode must be fixed-full-domain"
        )
    method = _string(
        raw.get("paired_gain_method"),
        "confidence.paired_gain_method",
    )
    expected_method = {
        AWM_CONFIG_SCHEMA_VERSION_V2: (
            "fixed-looks-empirical-bernstein"
        ),
        AWM_CONFIG_SCHEMA_VERSION_V2_1: (
            "fixed-snapshot-empirical-bernstein"
        ),
        AWM_CONFIG_SCHEMA_VERSION_V3: (
            "cluster-first-decomposed-kl-empirical-bernstein"
        ),
        AWM_CONFIG_SCHEMA_VERSION_V3_1: (
            "cluster-mean-decomposed-bounded-kl-empirical-bernstein"
        ),
    }[schema_version]
    if method not in PAIRED_GAIN_METHODS or method != expected_method:
        raise AWMConfigError(
            "confidence.paired_gain_method must be " + expected_method
        )
    marginal_fraction = _fraction(
        raw.get("marginal_alpha_fraction"),
        "confidence.marginal_alpha_fraction",
    )
    paired_fraction = _fraction(
        raw.get("paired_gain_alpha_fraction"),
        "confidence.paired_gain_alpha_fraction",
    )
    if marginal_fraction <= 0.0 or paired_fraction <= 0.0:
        raise AWMConfigError(
            "v2/v3 confidence alpha fractions must both be positive"
        )
    if not math.isclose(
        marginal_fraction + paired_fraction,
        1.0,
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        raise AWMConfigError(
            "confidence alpha fractions must sum to 1"
        )
    maximum_looks = _positive_integer(
        raw.get("maximum_looks"),
        "confidence.maximum_looks",
    )
    paired_comparisons: tuple[tuple[str, str], ...] = ()
    look_semantics = "controller-iteration-upper-bound"
    if schema_version in (
        AWM_CONFIG_SCHEMA_VERSION_V2_1,
        AWM_CONFIG_SCHEMA_VERSION_V3,
        AWM_CONFIG_SCHEMA_VERSION_V3_1,
    ):
        raw_comparisons = _list(
            raw.get("paired_comparisons"),
            "confidence.paired_comparisons",
        )
        comparisons: list[tuple[str, str]] = []
        unordered_seen: set[frozenset[str]] = set()
        for index, raw_comparison in enumerate(raw_comparisons):
            values = _list(
                raw_comparison,
                f"confidence.paired_comparisons[{index}]",
            )
            if len(values) != 2:
                raise AWMConfigError(
                    "each confidence.paired_comparisons entry must contain "
                    "exactly [current_design_id, candidate_design_id]"
                )
            comparison = (
                _string(
                    values[0],
                    f"confidence.paired_comparisons[{index}][0]",
                ),
                _string(
                    values[1],
                    f"confidence.paired_comparisons[{index}][1]",
                ),
            )
            if comparison[0] == comparison[1]:
                raise AWMConfigError(
                    "paired comparison designs must be different"
                )
            unordered = frozenset(comparison)
            if unordered in unordered_seen:
                raise AWMConfigError(
                    "paired_comparisons contains a duplicate unordered pair"
                )
            unordered_seen.add(unordered)
            comparisons.append(comparison)
        if not comparisons:
            raise AWMConfigError(
                "fixed-snapshot confidence.paired_comparisons cannot be empty"
            )
        paired_comparisons = tuple(comparisons)
        look_semantics = _string(
            raw.get("look_semantics"),
            "confidence.look_semantics",
        )
        if look_semantics != "fixed-training-snapshot-per-pair":
            raise AWMConfigError(
                "fixed-snapshot confidence.look_semantics must be "
                "fixed-training-snapshot-per-pair"
            )
        if maximum_looks != 1:
            raise AWMConfigError(
                "fixed-training-snapshot-per-pair requires maximum_looks=1"
            )

    sampling_unit = "paired-workload-repetition-seed"
    cluster_key_fields: tuple[str, ...] = ()
    cluster_reduction = "disabled"
    success_alpha_fraction = 0.0
    cost_alpha_fraction = 0.0
    if schema_version in (
        AWM_CONFIG_SCHEMA_VERSION_V3,
        AWM_CONFIG_SCHEMA_VERSION_V3_1,
    ):
        sampling_unit = _string(
            raw.get("sampling_unit"),
            "confidence.sampling_unit",
        )
        expected_sampling_unit = (
            "workload-cluster-mean-paired-observation"
            if schema_version == AWM_CONFIG_SCHEMA_VERSION_V3_1
            else "workload-cluster-first-paired-observation"
        )
        if sampling_unit != expected_sampling_unit:
            raise AWMConfigError(
                "v3 confidence.sampling_unit must be "
                + expected_sampling_unit
            )
        cluster_key_fields = tuple(
            _string(value, "confidence.cluster_key_fields")
            for value in _list(
                raw.get("cluster_key_fields"),
                "confidence.cluster_key_fields",
            )
        )
        if cluster_key_fields != ("workload_id",):
            raise AWMConfigError(
                "v3 confidence.cluster_key_fields must be exactly "
                "['workload_id']"
            )
        cluster_reduction = _string(
            raw.get("cluster_reduction"),
            "confidence.cluster_reduction",
        )
        expected_cluster_reduction = (
            "mean-over-complete-repetition-block"
            if schema_version == AWM_CONFIG_SCHEMA_VERSION_V3_1
            else "lowest-repetition-then-seed"
        )
        if cluster_reduction != expected_cluster_reduction:
            raise AWMConfigError(
                "v3 confidence.cluster_reduction must be "
                + expected_cluster_reduction
            )
        success_alpha_fraction = _fraction(
            raw.get("success_alpha_fraction"),
            "confidence.success_alpha_fraction",
        )
        cost_alpha_fraction = _fraction(
            raw.get("cost_alpha_fraction"),
            "confidence.cost_alpha_fraction",
        )
        if success_alpha_fraction <= 0.0 or cost_alpha_fraction <= 0.0:
            raise AWMConfigError(
                "v3 component alpha fractions must both be positive"
            )
        if not math.isclose(
            success_alpha_fraction + cost_alpha_fraction,
            1.0,
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            raise AWMConfigError(
                "v3 success and cost alpha fractions must sum to 1"
            )

    require_complete_pairs = _boolean(
        raw.get("require_complete_pairs", True),
        "confidence.require_complete_pairs",
    )
    if (
        schema_version == AWM_CONFIG_SCHEMA_VERSION_V3_1
        and not require_complete_pairs
    ):
        raise AWMConfigError(
            "v3alpha2 confidence.require_complete_pairs must be true"
        )

    return ConfidenceConfig(
        family_mode=family_mode,
        marginal_alpha_fraction=marginal_fraction,
        paired_gain_alpha_fraction=paired_fraction,
        paired_gain_method=method,
        paired_gain_minimum_pairs=_positive_integer(
            raw.get("paired_gain_minimum_pairs"),
            "confidence.paired_gain_minimum_pairs",
        ),
        maximum_looks=maximum_looks,
        require_complete_pairs=require_complete_pairs,
        paired_comparisons=paired_comparisons,
        look_semantics=look_semantics,
        sampling_unit=sampling_unit,
        cluster_key_fields=cluster_key_fields,
        cluster_reduction=cluster_reduction,
        success_alpha_fraction=success_alpha_fraction,
        cost_alpha_fraction=cost_alpha_fraction,
    )


def load_awm_config(path: str | Path) -> AWMConfig:
    source_path = Path(path).resolve()
    try:
        raw_bytes = source_path.read_bytes()
        raw = json.loads(raw_bytes.decode("utf-8"))
    except FileNotFoundError as exc:
        raise AWMConfigError(
            f"AWM configuration does not exist: {source_path}"
        ) from exc
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AWMConfigError(
            f"invalid AWM configuration {source_path}: {exc}"
        ) from exc
    root = _mapping(raw, "AWM configuration")
    schema_version = root.get("schema_version")
    if schema_version not in AWM_CONFIG_SCHEMA_VERSIONS:
        raise AWMConfigError("unsupported AWM schema_version")
    model_id = _string(root.get("model_id"), "model_id")
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", model_id) is None:
        raise AWMConfigError(
            "model_id may contain only letters, digits, '.', '_', and '-'"
        )

    confidence_level = _finite_number(
        root.get("confidence_level"),
        "confidence_level",
        minimum=1e-12,
    )
    if confidence_level >= 1.0:
        raise AWMConfigError("confidence_level must be less than 1")

    observed_design_ids = tuple(
        _string(value, "observed_design_ids")
        for value in _list(
            root.get("observed_design_ids"),
            "observed_design_ids",
        )
    )
    if not observed_design_ids:
        raise AWMConfigError("observed_design_ids cannot be empty")
    if len(observed_design_ids) != len(set(observed_design_ids)):
        raise AWMConfigError("observed_design_ids contains duplicates")

    raw_groups = _list(
        root.get("substitution_groups", []),
        "substitution_groups",
    )
    groups: list[tuple[str, ...]] = []
    seen_representations: set[str] = set()
    for index, raw_group in enumerate(raw_groups):
        group = tuple(
            _string(value, f"substitution_groups[{index}]")
            for value in _list(
                raw_group,
                f"substitution_groups[{index}]",
            )
        )
        if not group or len(group) != len(set(group)):
            raise AWMConfigError(
                "substitution groups must be non-empty and duplicate-free"
            )
        overlap = seen_representations.intersection(group)
        if overlap:
            raise AWMConfigError(
                "substitution groups must be disjoint; overlap: "
                + ", ".join(sorted(overlap))
            )
        seen_representations.update(group)
        groups.append(group)

    raw_assumptions = _mapping(root.get("assumptions"), "assumptions")
    unknown = set(raw_assumptions) - set(_ASSUMPTION_NAMES)
    if unknown:
        raise AWMConfigError(
            "unknown AWM assumptions: " + ", ".join(sorted(unknown))
        )
    assumptions: dict[str, AssumptionConfig] = {}
    for name in _ASSUMPTION_NAMES:
        item = _mapping(raw_assumptions.get(name), f"assumptions.{name}")
        assumptions[name] = AssumptionConfig(
            enabled=_boolean(
                item.get("enabled"),
                f"assumptions.{name}.enabled",
            ),
            status=_string(
                item.get("status"),
                f"assumptions.{name}.status",
            ),
        )
        if assumptions[name].enabled and not any(
            marker in assumptions[name].status.casefold()
            for marker in ("pass", "validated")
        ):
            raise AWMConfigError(
                f"enabled assumption {name} must have passed or validated "
                "status"
            )
    structural_enabled = any(
        assumptions[name].enabled
        for name in (
            "own_price_monotonicity",
            "substitution_group_monotonicity",
            "success_monotonicity",
        )
    )
    if structural_enabled and not assumptions[
        "quoted_price_sufficiency"
    ].enabled:
        raise AWMConfigError(
            "cross-design structural assumptions require "
            "quoted_price_sufficiency"
        )
    if (
        assumptions["substitution_group_monotonicity"].enabled
        and not groups
    ):
        raise AWMConfigError(
            "substitution-group monotonicity requires at least one group"
        )

    return AWMConfig(
        schema_version=str(schema_version),
        model_id=model_id,
        source_path=source_path,
        source_sha256=sha256(raw_bytes).hexdigest(),
        confidence_level=confidence_level,
        holdout_repetitions=_nonnegative_integer(
            root.get("holdout_repetitions", 1),
            "holdout_repetitions",
        ),
        observed_design_ids=observed_design_ids,
        minimum_training_sessions=_nonnegative_integer(
            root.get("minimum_training_sessions", 1),
            "minimum_training_sessions",
        ),
        maximum_excluded_training_fraction=_fraction(
            root.get("maximum_excluded_training_fraction", 0.0),
            "maximum_excluded_training_fraction",
        ),
        maximum_excluded_holdout_fraction=_fraction(
            root.get("maximum_excluded_holdout_fraction", 0.0),
            "maximum_excluded_holdout_fraction",
        ),
        substitution_groups=tuple(groups),
        assumptions=assumptions,
        own_price_requires_other_quotes_equal=_boolean(
            root.get("own_price_requires_other_quotes_equal", True),
            "own_price_requires_other_quotes_equal",
        ),
        cost_relative_radius=_finite_number(
            root.get("cost_relative_radius", 0.1),
            "cost_relative_radius",
        ),
        transition_relative_radius=_finite_number(
            root.get("transition_relative_radius", 0.1),
            "transition_relative_radius",
        ),
        commit_margin=_finite_number(
            root.get("commit_margin", 0.0),
            "commit_margin",
        ),
        confidence=_load_confidence_config(
            root,
            schema_version=str(schema_version),
        ),
    )
