from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from hashlib import sha256
from statistics import NormalDist, mean, variance
from typing import Any

from .contracts import AWMConfigError
from .dataset import AWMDataset, DesignSample


AWM_MODEL_KINDS = (
    "assumption_free_box",
    "independent_box",
    "coupled_awm",
)


@dataclass(frozen=True)
class Interval:
    lower: float
    upper: float

    def __post_init__(self) -> None:
        if (
            not math.isfinite(self.lower)
            or not math.isfinite(self.upper)
            or self.lower > self.upper
        ):
            raise ValueError(
                f"invalid interval [{self.lower}, {self.upper}]"
            )

    @property
    def width(self) -> float:
        return self.upper - self.lower

    def contains(self, value: float, *, tolerance: float = 1e-9) -> bool:
        return self.lower - tolerance <= value <= self.upper + tolerance


@dataclass(frozen=True)
class DesignBounds:
    design_id: str
    model_kind: str
    observed: bool
    training_sessions: int
    access: dict[str, Interval]
    group_access: dict[str, Interval]
    success: Interval
    service_cost_per_session: Interval
    storage_cost: float
    transition_cost: Interval
    phi: Interval
    constraints_applied: tuple[str, ...]

    def to_row(self) -> dict[str, Any]:
        return {
            "model_kind": self.model_kind,
            "design_id": self.design_id,
            "observed": self.observed,
            "training_sessions": self.training_sessions,
            "access_bounds_json": json.dumps(
                _intervals_jsonable(self.access),
                sort_keys=True,
            ),
            "group_access_bounds_json": json.dumps(
                _intervals_jsonable(self.group_access),
                sort_keys=True,
            ),
            "success_lower": self.success.lower,
            "success_upper": self.success.upper,
            "service_cost_lower": self.service_cost_per_session.lower,
            "service_cost_upper": self.service_cost_per_session.upper,
            "storage_cost": self.storage_cost,
            "transition_cost_lower": self.transition_cost.lower,
            "transition_cost_upper": self.transition_cost.upper,
            "phi_lower": self.phi.lower,
            "phi_upper": self.phi.upper,
            "phi_width": self.phi.width,
            "constraints_applied": ";".join(self.constraints_applied),
        }


@dataclass(frozen=True)
class PairedGainCertificate:
    current_design_id: str
    candidate_design_id: str
    method: str
    pair_count: int
    training_snapshot_sha256: str
    family_size: int
    maximum_looks: int
    alpha_per_pair_look: float
    point_estimate_per_session: float
    success_difference_point_estimate: float
    service_cost_difference_point_estimate: float
    sample_variance_per_session: float
    success_difference_sample_variance: float
    service_cost_difference_sample_variance: float
    success_cost_sample_covariance: float
    variance_radius_per_session: float
    range_radius_per_session: float
    total_radius_per_session: float
    support_per_session: Interval
    unclipped_gain_per_session: Interval
    gain_per_session: Interval
    clipped_to_support: bool
    phi_gain: Interval
    transition_adjusted_gain: Interval
    certificate_construction: str = "direct-utility-empirical-bernstein"
    sampling_unit: str = "paired-workload-repetition-seed"
    raw_pair_count: int | None = None
    independent_unit_count: int | None = None
    cluster_key_fields: tuple[str, ...] = ()
    cluster_reduction: str = "disabled"
    success_alpha_per_pair: float | None = None
    cost_alpha_per_pair: float | None = None
    positive_discordance_count: int | None = None
    negative_discordance_count: int | None = None
    positive_discordance_probability: Interval | None = None
    negative_discordance_probability: Interval | None = None
    success_difference: Interval | None = None
    service_cost_difference: Interval | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_row(self, *, model_kind: str) -> dict[str, Any]:
        row = {
            "model_kind": model_kind,
            "current_design_id": self.current_design_id,
            "candidate_design_id": self.candidate_design_id,
            "method": self.method,
            "pair_count": self.pair_count,
            "training_snapshot_sha256": self.training_snapshot_sha256,
            "family_size": self.family_size,
            "maximum_looks": self.maximum_looks,
            "alpha_per_pair_look": self.alpha_per_pair_look,
            "point_estimate_per_session": self.point_estimate_per_session,
            "success_difference_point_estimate": (
                self.success_difference_point_estimate
            ),
            "service_cost_difference_point_estimate": (
                self.service_cost_difference_point_estimate
            ),
            "sample_variance_per_session": (
                self.sample_variance_per_session
            ),
            "success_difference_sample_variance": (
                self.success_difference_sample_variance
            ),
            "service_cost_difference_sample_variance": (
                self.service_cost_difference_sample_variance
            ),
            "success_cost_sample_covariance": (
                self.success_cost_sample_covariance
            ),
            "variance_radius_per_session": (
                self.variance_radius_per_session
            ),
            "range_radius_per_session": self.range_radius_per_session,
            "total_radius_per_session": self.total_radius_per_session,
            "support_lower_per_session": self.support_per_session.lower,
            "support_upper_per_session": self.support_per_session.upper,
            "support_width_per_session": self.support_per_session.width,
            "unclipped_gain_lower_per_session": (
                self.unclipped_gain_per_session.lower
            ),
            "unclipped_gain_upper_per_session": (
                self.unclipped_gain_per_session.upper
            ),
            "gain_lower_per_session": self.gain_per_session.lower,
            "gain_upper_per_session": self.gain_per_session.upper,
            "clipped_to_support": self.clipped_to_support,
            "phi_gain_lower": self.phi_gain.lower,
            "phi_gain_upper": self.phi_gain.upper,
            "transition_adjusted_gain_lower": (
                self.transition_adjusted_gain.lower
            ),
            "transition_adjusted_gain_upper": (
                self.transition_adjusted_gain.upper
            ),
            "certificate_construction": self.certificate_construction,
            "sampling_unit": self.sampling_unit,
            "raw_pair_count": self.raw_pair_count,
            "independent_unit_count": self.independent_unit_count,
            "cluster_key_fields": ";".join(self.cluster_key_fields),
            "cluster_reduction": self.cluster_reduction,
            "success_alpha_per_pair": self.success_alpha_per_pair,
            "cost_alpha_per_pair": self.cost_alpha_per_pair,
            "positive_discordance_count": (
                self.positive_discordance_count
            ),
            "negative_discordance_count": (
                self.negative_discordance_count
            ),
        }
        for prefix, interval in (
            (
                "positive_discordance_probability",
                self.positive_discordance_probability,
            ),
            (
                "negative_discordance_probability",
                self.negative_discordance_probability,
            ),
            ("success_difference", self.success_difference),
            ("service_cost_difference", self.service_cost_difference),
        ):
            row[f"{prefix}_lower"] = (
                interval.lower if interval is not None else None
            )
            row[f"{prefix}_upper"] = (
                interval.upper if interval is not None else None
            )
        return row


@dataclass(frozen=True)
class EmpiricalBernsteinEstimate:
    interval: Interval
    unclipped_interval: Interval
    sample_mean: float
    sample_variance: float
    variance_radius: float
    range_radius: float
    total_radius: float
    clipped_to_support: bool


@dataclass(frozen=True)
class PairedGainPowerAnalysis:
    current_design_id: str
    candidate_design_id: str
    method: str
    planning_status: str
    current_pair_count: int
    alpha_per_pair_look: float
    assumed_point_estimate_per_session: float
    assumed_sample_variance_per_session: float
    support_width_per_session: float
    current_total_radius_per_session: float
    current_clipped_to_support: bool
    estimated_pairs_for_unclipped_interval: int | None
    estimated_pairs_for_50pct_support_width: int | None
    estimated_pairs_for_25pct_support_width: int | None
    estimated_pairs_for_10pct_support_width: int | None
    estimated_pairs_for_positive_commit_lower: int | None
    commit_margin: float
    maximum_planning_pairs: int
    caveat: str
    sampling_unit: str = "paired-workload-repetition-seed"
    raw_pair_count: int | None = None
    assumed_positive_discordance_probability: float | None = None
    assumed_negative_discordance_probability: float | None = None
    current_success_difference_width: float | None = None
    current_service_cost_difference_width: float | None = None

    def to_row(self, *, model_kind: str) -> dict[str, Any]:
        payload = asdict(self)
        return {"model_kind": model_kind, **payload}


def _intervals_jsonable(values: dict[str, Interval]) -> dict[str, Any]:
    return {
        key: {"lower": value.lower, "upper": value.upper}
        for key, value in sorted(values.items())
    }


def _wilson_interval(successes: int, trials: int, z: float) -> Interval:
    if trials <= 0:
        return Interval(0.0, 1.0)
    probability = successes / trials
    z_squared = z * z
    denominator = 1.0 + z_squared / trials
    center = (probability + z_squared / (2.0 * trials)) / denominator
    radius = (
        z
        * math.sqrt(
            probability * (1.0 - probability) / trials
            + z_squared / (4.0 * trials * trials)
        )
        / denominator
    )
    return Interval(
        max(0.0, center - radius),
        min(1.0, center + radius),
    )


def _bernoulli_kl(probability: float, candidate: float) -> float:
    """Return binary relative entropy KL(probability || candidate)."""
    if not 0.0 <= probability <= 1.0:
        raise ValueError("Bernoulli probability must be in [0, 1]")
    if not 0.0 <= candidate <= 1.0:
        raise ValueError("Bernoulli candidate must be in [0, 1]")
    if probability == candidate:
        return 0.0
    if candidate == 0.0 or candidate == 1.0:
        return math.inf
    result = 0.0
    if probability > 0.0:
        result += probability * math.log(probability / candidate)
    if probability < 1.0:
        result += (1.0 - probability) * math.log(
            (1.0 - probability) / (1.0 - candidate)
        )
    return result


def _bernoulli_kl_mean_interval(
    probability: float,
    trials: int,
    delta: float,
) -> Interval:
    """Two-sided Chernoff/KL interval inverted by deterministic bisection."""
    if not 0.0 <= probability <= 1.0:
        raise ValueError("Bernoulli probability must be in [0, 1]")
    if trials <= 0:
        raise ValueError("Bernoulli trial count must be positive")
    if not 0.0 < delta < 1.0:
        raise ValueError("Bernoulli interval delta must be in (0, 1)")
    threshold = math.log(2.0 / delta) / trials

    if probability == 0.0:
        lower = 0.0
    else:
        outside = 0.0
        inside = probability
        for _ in range(80):
            middle = (outside + inside) / 2.0
            if _bernoulli_kl(probability, middle) > threshold:
                outside = middle
            else:
                inside = middle
        lower = inside

    if probability == 1.0:
        upper = 1.0
    else:
        inside = probability
        outside = 1.0
        for _ in range(80):
            middle = (inside + outside) / 2.0
            if _bernoulli_kl(probability, middle) > threshold:
                outside = middle
            else:
                inside = middle
        upper = inside
    return Interval(lower, upper)


def _bernoulli_kl_interval(
    successes: int,
    trials: int,
    delta: float,
) -> Interval:
    if successes < 0 or successes > trials:
        raise ValueError("Bernoulli successes must be between 0 and trials")
    return _bernoulli_kl_mean_interval(successes / trials, trials, delta)


def _empirical_bernstein_estimate(
    values: tuple[float, ...],
    *,
    support: Interval,
    delta: float,
) -> EmpiricalBernsteinEstimate:
    """Two-sided fixed-look Maurer-Pontil empirical Bernstein interval.

    The one-sided bound is applied to both tails with ``delta / 2`` and
    scaled from [0, 1] to the declared finite support. A caller that consults
    multiple design pairs or looks must allocate ``delta`` by a union bound.
    """
    if not values:
        raise ValueError("empirical Bernstein values cannot be empty")
    if not 0.0 < delta < 1.0:
        raise ValueError("empirical Bernstein delta must be in (0, 1)")
    for value in values:
        if not support.contains(value, tolerance=1e-8):
            raise AWMConfigError(
                "paired utility difference exceeds its declared support"
            )
    sample_mean = mean(values)
    if len(values) < 2 or support.width <= 0.0:
        interval = (
            Interval(sample_mean, sample_mean)
            if support.width <= 0.0
            else support
        )
        return EmpiricalBernsteinEstimate(
            interval=interval,
            unclipped_interval=interval,
            sample_mean=sample_mean,
            sample_variance=0.0,
            variance_radius=0.0,
            range_radius=(0.0 if support.width <= 0.0 else support.width),
            total_radius=(0.0 if support.width <= 0.0 else support.width),
            clipped_to_support=False,
        )
    sample_variance = variance(values)
    log_term = math.log(4.0 / delta)
    variance_radius = math.sqrt(
        2.0 * sample_variance * log_term / len(values)
    )
    range_radius = (
        7.0 * support.width * log_term
        / (3.0 * (len(values) - 1))
    )
    radius = variance_radius + range_radius
    unclipped = Interval(sample_mean - radius, sample_mean + radius)
    interval = Interval(
        max(support.lower, unclipped.lower),
        min(support.upper, unclipped.upper),
    )
    return EmpiricalBernsteinEstimate(
        interval=interval,
        unclipped_interval=unclipped,
        sample_mean=sample_mean,
        sample_variance=sample_variance,
        variance_radius=variance_radius,
        range_radius=range_radius,
        total_radius=radius,
        clipped_to_support=(interval != unclipped),
    )


def _sample_variance(values: tuple[float, ...]) -> float:
    return variance(values) if len(values) >= 2 else 0.0


def _sample_covariance(
    left: tuple[float, ...],
    right: tuple[float, ...],
) -> float:
    if len(left) != len(right):
        raise ValueError("covariance inputs must have equal length")
    if len(left) < 2:
        return 0.0
    left_mean = mean(left)
    right_mean = mean(right)
    return sum(
        (left_value - left_mean) * (right_value - right_mean)
        for left_value, right_value in zip(left, right, strict=True)
    ) / (len(left) - 1)


def _projected_empirical_bernstein_radius(
    *,
    sample_variance: float,
    support_width: float,
    delta: float,
    pair_count: int,
) -> float:
    if pair_count < 2:
        return math.inf
    log_term = math.log(4.0 / delta)
    return math.sqrt(
        2.0 * sample_variance * log_term / pair_count
    ) + (
        7.0 * support_width * log_term
        / (3.0 * (pair_count - 1))
    )


def _minimum_projected_pairs(
    predicate: Any,
    *,
    start: int,
    maximum: int,
) -> int | None:
    lower = max(2, start)
    if predicate(lower):
        return lower
    if not predicate(maximum):
        return None
    upper = maximum
    while lower + 1 < upper:
        middle = (lower + upper) // 2
        if predicate(middle):
            upper = middle
        else:
            lower = middle
    return upper


def _group_key(group: tuple[str, ...]) -> str:
    return "+".join(group)


class AdaptiveWorkloadModel:
    """An analytic finite-design envelope with configuration-gated coupling."""

    def __init__(self, dataset: AWMDataset, *, model_kind: str) -> None:
        if model_kind not in AWM_MODEL_KINDS:
            raise ValueError(f"unknown AWM model kind: {model_kind}")
        self.dataset = dataset
        self.model_kind = model_kind
        self._bounds = self._fit()
        self._paired_gain_cache: dict[
            tuple[str, str], PairedGainCertificate | None
        ] = {}

    @property
    def bounds(self) -> dict[str, DesignBounds]:
        return dict(self._bounds)

    def lower_bound(self, design_id: str) -> float:
        return self._bounds[design_id].phi.lower

    def upper_bound(self, design_id: str) -> float:
        return self._bounds[design_id].phi.upper

    @property
    def confidence_contract(self) -> dict[str, Any]:
        config = self.dataset.model_config
        return {
            "family_mode": config.confidence.family_mode,
            "sampling_unit": config.confidence.sampling_unit,
            "cluster_key_fields": list(
                config.confidence.cluster_key_fields
            ),
            "cluster_reduction": config.confidence.cluster_reduction,
            "confidence_level": config.confidence_level,
            "total_alpha": 1.0 - config.confidence_level,
            "marginal_alpha_fraction": (
                config.confidence.marginal_alpha_fraction
            ),
            "marginal_family_size": self._marginal_family_size(),
            "marginal_alpha_per_metric": self._joint_metric_alpha(),
            "paired_gain_enabled": (
                config.confidence.paired_gain_enabled
            ),
            "paired_gain_method": config.confidence.paired_gain_method,
            "paired_gain_alpha_fraction": (
                config.confidence.paired_gain_alpha_fraction
            ),
            "paired_comparisons": [
                list(comparison)
                for comparison in self._declared_paired_comparisons()
            ],
            "paired_design_pair_count": self._design_pair_count(),
            "maximum_looks": config.confidence.maximum_looks,
            "look_semantics": config.confidence.look_semantics,
            "repeated_fixed_snapshot_reads_count_as_new_looks": (
                config.confidence.look_semantics
                != "fixed-training-snapshot-per-pair"
            ),
            "paired_family_size": self._paired_family_size(),
            "paired_alpha_per_pair_look": self._paired_alpha(),
            "success_alpha_fraction": (
                config.confidence.success_alpha_fraction
            ),
            "cost_alpha_fraction": (
                config.confidence.cost_alpha_fraction
            ),
            "paired_support_rule": (
                "task-value-plus-declared-single-access-cost-support"
            ),
        }

    def gain_interval(self, current: str, candidate: str) -> Interval:
        certificate = self.paired_gain_certificate(current, candidate)
        if certificate is not None:
            return certificate.transition_adjusted_gain
        return Interval(
            self.lower_bound(candidate)
            - self.upper_bound(current)
            - self._bounds[candidate].transition_cost.upper,
            self.upper_bound(candidate)
            - self.lower_bound(current)
            - self._bounds[candidate].transition_cost.lower,
        )

    def gain_interval_source(self, current: str, candidate: str) -> str:
        if self.paired_gain_certificate(current, candidate) is None:
            return "marginal-phi-difference"
        if (
            self.dataset.model_config.confidence.paired_gain_method
            == "cluster-first-decomposed-kl-empirical-bernstein"
        ):
            return "paired-cluster-decomposed-kl-empirical-bernstein"
        if (
            self.dataset.model_config.confidence.look_semantics
            == "fixed-training-snapshot-per-pair"
        ):
            return "paired-fixed-snapshot-empirical-bernstein"
        return "paired-fixed-looks-empirical-bernstein"

    def pessimistic_gain(self, current: str, candidate: str) -> float:
        return self.gain_interval(current, candidate).lower

    def optimistic_gain(self, current: str, candidate: str) -> float:
        return self.gain_interval(current, candidate).upper

    def paired_gain_certificate(
        self,
        current: str,
        candidate: str,
    ) -> PairedGainCertificate | None:
        if current == candidate:
            raise ValueError("paired gain requires two different designs")
        cache_key = (current, candidate)
        if cache_key in self._paired_gain_cache:
            return self._paired_gain_cache[cache_key]
        config = self.dataset.model_config.confidence
        if not config.paired_gain_enabled:
            self._paired_gain_cache[cache_key] = None
            return None
        if not self._paired_comparison_declared(current, candidate):
            self._paired_gain_cache[cache_key] = None
            return None
        if not (
            self._bounds[current].observed
            and self._bounds[candidate].observed
        ):
            self._paired_gain_cache[cache_key] = None
            return None

        current_observations = {
            value.pairing_key: value
            for value in self.dataset.training[current].observations
        }
        candidate_observations = {
            value.pairing_key: value
            for value in self.dataset.training[candidate].observations
        }
        raw_keys = tuple(sorted(
            set(current_observations).intersection(candidate_observations)
        ))
        if config.cluster_key_fields:
            keys_by_cluster: dict[str, list[str]] = {}
            for key in raw_keys:
                current_cluster = current_observations[key].cluster_key
                candidate_cluster = candidate_observations[key].cluster_key
                if current_cluster != candidate_cluster:
                    raise AWMConfigError(
                        "paired observations disagree on workload cluster"
                    )
                keys_by_cluster.setdefault(current_cluster, []).append(key)
            keys = tuple(
                min(
                    cluster_keys,
                    key=lambda value: (
                        current_observations[value].repetition,
                        current_observations[value].seed,
                        value,
                    ),
                )
                for _, cluster_keys in sorted(keys_by_cluster.items())
            )
        else:
            keys = raw_keys
        if len(keys) < config.paired_gain_minimum_pairs:
            self._paired_gain_cache[cache_key] = None
            return None

        task_value = self.dataset.task_class.task_value
        resource_weight = self.dataset.system.resource_cost_weight
        success_differences = tuple(
            candidate_observations[key].success
            - current_observations[key].success
            for key in keys
        )
        service_cost_differences = tuple(
            candidate_observations[key].service_cost
            - current_observations[key].service_cost
            for key in keys
        )
        values = tuple(
            task_value * success_difference
            - resource_weight * service_cost_difference
            for success_difference, service_cost_difference in zip(
                success_differences,
                service_cost_differences,
                strict=True,
            )
        )
        snapshot_payload = [
            {
                "pairing_key": key,
                "cluster_key": current_observations[key].cluster_key,
                "repetition": current_observations[key].repetition,
                "seed": current_observations[key].seed,
                "current_success": current_observations[key].success,
                "candidate_success": candidate_observations[key].success,
                "current_service_cost": (
                    current_observations[key].service_cost
                ),
                "candidate_service_cost": (
                    candidate_observations[key].service_cost
                ),
            }
            for key in keys
        ]
        training_snapshot_sha256 = sha256(
            json.dumps(
                {
                    "current_design_id": current,
                    "candidate_design_id": candidate,
                    "sampling_unit": config.sampling_unit,
                    "cluster_key_fields": list(config.cluster_key_fields),
                    "cluster_reduction": config.cluster_reduction,
                    "observations": snapshot_payload,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        current_cost_upper = self._service_cost_support_upper(current)
        candidate_cost_upper = self._service_cost_support_upper(candidate)
        support = Interval(
            -task_value - resource_weight * candidate_cost_upper,
            task_value + resource_weight * current_cost_upper,
        )
        diagnostic_estimate = _empirical_bernstein_estimate(
            values,
            support=support,
            delta=self._paired_alpha(),
        )
        per_session = diagnostic_estimate.interval
        certificate_construction = "direct-utility-empirical-bernstein"
        success_alpha: float | None = None
        cost_alpha: float | None = None
        positive_count: int | None = None
        negative_count: int | None = None
        positive_probability: Interval | None = None
        negative_probability: Interval | None = None
        success_interval: Interval | None = None
        cost_interval: Interval | None = None
        unclipped_per_session = diagnostic_estimate.unclipped_interval
        clipped_to_support = diagnostic_estimate.clipped_to_support
        if (
            config.paired_gain_method
            == "cluster-first-decomposed-kl-empirical-bernstein"
        ):
            success_alpha = (
                self._paired_alpha() * config.success_alpha_fraction
            )
            cost_alpha = self._paired_alpha() * config.cost_alpha_fraction
            positive_count = sum(value == 1 for value in success_differences)
            negative_count = sum(value == -1 for value in success_differences)
            positive_probability = _bernoulli_kl_interval(
                positive_count,
                len(keys),
                success_alpha / 2.0,
            )
            negative_probability = _bernoulli_kl_interval(
                negative_count,
                len(keys),
                success_alpha / 2.0,
            )
            success_interval = Interval(
                positive_probability.lower
                - negative_probability.upper,
                positive_probability.upper
                - negative_probability.lower,
            )
            cost_support = Interval(
                -current_cost_upper,
                candidate_cost_upper,
            )
            cost_estimate = _empirical_bernstein_estimate(
                service_cost_differences,
                support=cost_support,
                delta=cost_alpha,
            )
            cost_interval = cost_estimate.interval
            unclipped_per_session = Interval(
                task_value * success_interval.lower
                - resource_weight * cost_interval.upper,
                task_value * success_interval.upper
                - resource_weight * cost_interval.lower,
            )
            per_session = Interval(
                max(support.lower, unclipped_per_session.lower),
                min(support.upper, unclipped_per_session.upper),
            )
            clipped_to_support = per_session != unclipped_per_session
            certificate_construction = (
                "paired-discordance-bernoulli-kl-plus-"
                "service-cost-empirical-bernstein"
            )
        storage_delta = (
            self.dataset.storage_costs[candidate]
            - self.dataset.storage_costs[current]
        )
        horizon = self.dataset.horizon_sessions
        phi_gain = Interval(
            horizon * per_session.lower - storage_delta,
            horizon * per_session.upper - storage_delta,
        )
        transition = self._bounds[candidate].transition_cost
        adjusted = Interval(
            phi_gain.lower - transition.upper,
            phi_gain.upper - transition.lower,
        )
        certificate = PairedGainCertificate(
            current_design_id=current,
            candidate_design_id=candidate,
            method=config.paired_gain_method,
            pair_count=len(keys),
            training_snapshot_sha256=training_snapshot_sha256,
            family_size=self._paired_family_size(),
            maximum_looks=config.maximum_looks,
            alpha_per_pair_look=self._paired_alpha(),
            point_estimate_per_session=diagnostic_estimate.sample_mean,
            success_difference_point_estimate=mean(success_differences),
            service_cost_difference_point_estimate=mean(
                service_cost_differences
            ),
            sample_variance_per_session=diagnostic_estimate.sample_variance,
            success_difference_sample_variance=_sample_variance(
                success_differences
            ),
            service_cost_difference_sample_variance=_sample_variance(
                service_cost_differences
            ),
            success_cost_sample_covariance=_sample_covariance(
                success_differences,
                service_cost_differences,
            ),
            variance_radius_per_session=(
                diagnostic_estimate.variance_radius
            ),
            range_radius_per_session=diagnostic_estimate.range_radius,
            total_radius_per_session=diagnostic_estimate.total_radius,
            support_per_session=support,
            unclipped_gain_per_session=unclipped_per_session,
            gain_per_session=per_session,
            clipped_to_support=clipped_to_support,
            phi_gain=phi_gain,
            transition_adjusted_gain=adjusted,
            certificate_construction=certificate_construction,
            sampling_unit=config.sampling_unit,
            raw_pair_count=len(raw_keys),
            independent_unit_count=len(keys),
            cluster_key_fields=config.cluster_key_fields,
            cluster_reduction=config.cluster_reduction,
            success_alpha_per_pair=success_alpha,
            cost_alpha_per_pair=cost_alpha,
            positive_discordance_count=positive_count,
            negative_discordance_count=negative_count,
            positive_discordance_probability=positive_probability,
            negative_discordance_probability=negative_probability,
            success_difference=success_interval,
            service_cost_difference=cost_interval,
        )
        self._paired_gain_cache[cache_key] = certificate
        return certificate

    def paired_gain_power_analysis(
        self,
        current: str,
        candidate: str,
        *,
        maximum_planning_pairs: int = 10_000_000,
    ) -> PairedGainPowerAnalysis | None:
        """Return plug-in sample-size planning, never a confidence claim."""
        certificate = self.paired_gain_certificate(current, candidate)
        if certificate is None:
            return None
        if maximum_planning_pairs < 2:
            raise ValueError("maximum_planning_pairs must be at least 2")
        if maximum_planning_pairs < certificate.pair_count:
            raise ValueError(
                "maximum_planning_pairs cannot be smaller than the current "
                "pair count"
            )
        if (
            certificate.method
            == "cluster-first-decomposed-kl-empirical-bernstein"
        ):
            return self._cluster_decomposed_power_analysis(
                certificate,
                maximum_planning_pairs=maximum_planning_pairs,
            )

        support = certificate.support_per_session
        sample_mean = certificate.point_estimate_per_session
        sample_variance = certificate.sample_variance_per_session
        delta = certificate.alpha_per_pair_look

        def radius(pair_count: int) -> float:
            return _projected_empirical_bernstein_radius(
                sample_variance=sample_variance,
                support_width=support.width,
                delta=delta,
                pair_count=pair_count,
            )

        def projected_interval(pair_count: int) -> Interval:
            projected_radius = radius(pair_count)
            return Interval(
                max(support.lower, sample_mean - projected_radius),
                min(support.upper, sample_mean + projected_radius),
            )

        start = certificate.pair_count
        unclipped = _minimum_projected_pairs(
            lambda pair_count: (
                sample_mean - radius(pair_count) >= support.lower
                and sample_mean + radius(pair_count) <= support.upper
            ),
            start=start,
            maximum=maximum_planning_pairs,
        )

        width_targets = {
            fraction: _minimum_projected_pairs(
                lambda pair_count, fraction=fraction: (
                    projected_interval(pair_count).width
                    <= fraction * support.width
                ),
                start=start,
                maximum=maximum_planning_pairs,
            )
            for fraction in (0.5, 0.25, 0.1)
        }
        storage_delta = (
            self.dataset.storage_costs[candidate]
            - self.dataset.storage_costs[current]
        )
        transition_upper = self._bounds[candidate].transition_cost.upper
        horizon = self.dataset.horizon_sessions
        commit_margin = self.dataset.model_config.commit_margin
        positive_commit = _minimum_projected_pairs(
            lambda pair_count: (
                horizon * projected_interval(pair_count).lower
                - storage_delta
                - transition_upper
                > commit_margin
            ),
            start=start,
            maximum=maximum_planning_pairs,
        )
        return PairedGainPowerAnalysis(
            current_design_id=current,
            candidate_design_id=candidate,
            method="plug-in-fixed-variance-empirical-bernstein-planning",
            planning_status="posthoc-planning-not-a-confidence-guarantee",
            current_pair_count=certificate.pair_count,
            alpha_per_pair_look=delta,
            assumed_point_estimate_per_session=sample_mean,
            assumed_sample_variance_per_session=sample_variance,
            support_width_per_session=support.width,
            current_total_radius_per_session=(
                certificate.total_radius_per_session
            ),
            current_clipped_to_support=certificate.clipped_to_support,
            estimated_pairs_for_unclipped_interval=unclipped,
            estimated_pairs_for_50pct_support_width=width_targets[0.5],
            estimated_pairs_for_25pct_support_width=width_targets[0.25],
            estimated_pairs_for_10pct_support_width=width_targets[0.1],
            estimated_pairs_for_positive_commit_lower=positive_commit,
            commit_margin=commit_margin,
            maximum_planning_pairs=maximum_planning_pairs,
            caveat=(
                "Plug-in projection holds the observed mean and variance "
                "fixed, assumes bounded independent paired sampling units, "
                "and must not be interpreted as achieved power or coverage."
            ),
        )

    def _cluster_decomposed_power_analysis(
        self,
        certificate: PairedGainCertificate,
        *,
        maximum_planning_pairs: int,
    ) -> PairedGainPowerAnalysis:
        """Plug-in planning in independent workload-cluster units."""
        if (
            certificate.positive_discordance_count is None
            or certificate.negative_discordance_count is None
            or certificate.success_alpha_per_pair is None
            or certificate.cost_alpha_per_pair is None
            or certificate.success_difference is None
            or certificate.service_cost_difference is None
        ):
            raise RuntimeError("v3 certificate is missing component bounds")
        count = certificate.pair_count
        positive_probability = (
            certificate.positive_discordance_count / count
        )
        negative_probability = (
            certificate.negative_discordance_count / count
        )
        success_alpha = certificate.success_alpha_per_pair
        cost_alpha = certificate.cost_alpha_per_pair
        task_value = self.dataset.task_class.task_value
        resource_weight = self.dataset.system.resource_cost_weight
        current_cost_upper = self._service_cost_support_upper(
            certificate.current_design_id
        )
        candidate_cost_upper = self._service_cost_support_upper(
            certificate.candidate_design_id
        )
        cost_support = Interval(-current_cost_upper, candidate_cost_upper)
        utility_support = certificate.support_per_session
        cost_mean = certificate.service_cost_difference_point_estimate
        cost_variance = (
            certificate.service_cost_difference_sample_variance
        )

        def projected_interval(cluster_count: int) -> Interval:
            positive = _bernoulli_kl_mean_interval(
                positive_probability,
                cluster_count,
                success_alpha / 2.0,
            )
            negative = _bernoulli_kl_mean_interval(
                negative_probability,
                cluster_count,
                success_alpha / 2.0,
            )
            success = Interval(
                positive.lower - negative.upper,
                positive.upper - negative.lower,
            )
            cost_radius = _projected_empirical_bernstein_radius(
                sample_variance=cost_variance,
                support_width=cost_support.width,
                delta=cost_alpha,
                pair_count=cluster_count,
            )
            cost = Interval(
                max(cost_support.lower, cost_mean - cost_radius),
                min(cost_support.upper, cost_mean + cost_radius),
            )
            return Interval(
                max(
                    utility_support.lower,
                    task_value * success.lower
                    - resource_weight * cost.upper,
                ),
                min(
                    utility_support.upper,
                    task_value * success.upper
                    - resource_weight * cost.lower,
                ),
            )

        start = certificate.pair_count
        width_targets = {
            fraction: _minimum_projected_pairs(
                lambda cluster_count, fraction=fraction: (
                    projected_interval(cluster_count).width
                    <= fraction * utility_support.width
                ),
                start=start,
                maximum=maximum_planning_pairs,
            )
            for fraction in (0.5, 0.25, 0.1)
        }
        storage_delta = (
            self.dataset.storage_costs[certificate.candidate_design_id]
            - self.dataset.storage_costs[certificate.current_design_id]
        )
        transition_upper = self._bounds[
            certificate.candidate_design_id
        ].transition_cost.upper
        horizon = self.dataset.horizon_sessions
        commit_margin = self.dataset.model_config.commit_margin
        positive_commit = _minimum_projected_pairs(
            lambda cluster_count: (
                horizon * projected_interval(cluster_count).lower
                - storage_delta
                - transition_upper
                > commit_margin
            ),
            start=start,
            maximum=maximum_planning_pairs,
        )
        return PairedGainPowerAnalysis(
            current_design_id=certificate.current_design_id,
            candidate_design_id=certificate.candidate_design_id,
            method=(
                "plug-in-cluster-decomposed-kl-empirical-"
                "bernstein-planning"
            ),
            planning_status="posthoc-planning-not-a-confidence-guarantee",
            current_pair_count=certificate.pair_count,
            alpha_per_pair_look=certificate.alpha_per_pair_look,
            assumed_point_estimate_per_session=(
                certificate.point_estimate_per_session
            ),
            assumed_sample_variance_per_session=(
                certificate.sample_variance_per_session
            ),
            support_width_per_session=utility_support.width,
            current_total_radius_per_session=(
                certificate.gain_per_session.width / 2.0
            ),
            current_clipped_to_support=certificate.clipped_to_support,
            estimated_pairs_for_unclipped_interval=None,
            estimated_pairs_for_50pct_support_width=width_targets[0.5],
            estimated_pairs_for_25pct_support_width=width_targets[0.25],
            estimated_pairs_for_10pct_support_width=width_targets[0.1],
            estimated_pairs_for_positive_commit_lower=positive_commit,
            commit_margin=commit_margin,
            maximum_planning_pairs=maximum_planning_pairs,
            caveat=(
                "Plug-in projection holds discordance probabilities and "
                "cost moments fixed, counts independent workload clusters, "
                "assumes a common target distribution, and is not achieved "
                "power or coverage."
            ),
            sampling_unit=certificate.sampling_unit,
            raw_pair_count=certificate.raw_pair_count,
            assumed_positive_discordance_probability=(
                positive_probability
            ),
            assumed_negative_discordance_probability=(
                negative_probability
            ),
            current_success_difference_width=(
                certificate.success_difference.width
            ),
            current_service_cost_difference_width=(
                certificate.service_cost_difference.width
            ),
        )

    def _joint_z(self) -> float:
        alpha = self._joint_metric_alpha()
        return NormalDist().inv_cdf(1.0 - alpha / 2.0)

    def _joint_metric_alpha(self) -> float:
        alpha = 1.0 - self.dataset.model_config.confidence_level
        alpha *= self.dataset.model_config.confidence.marginal_alpha_fraction
        return alpha / self._marginal_family_size()

    def _marginal_family_size(self) -> int:
        config = self.dataset.model_config.confidence
        design_count = (
            len(self.dataset.design_ids)
            if config.family_mode == "fixed-full-domain"
            else len(self.dataset.model_config.observed_design_ids)
        )
        metric_count = design_count * (
            len(self.dataset.representation_ids)
            + 2
            + len(self.dataset.model_config.substitution_groups)
        )
        return max(1, metric_count)

    def _design_pair_count(self) -> int:
        return max(1, len(self._declared_paired_comparisons()))

    def _declared_paired_comparisons(
        self,
    ) -> tuple[tuple[str, str], ...]:
        configured = self.dataset.model_config.confidence.paired_comparisons
        if configured:
            return configured
        return tuple(
            (left_id, right_id)
            for left_index, left_id in enumerate(self.dataset.design_ids)
            for right_id in self.dataset.design_ids[left_index + 1 :]
        )

    def _paired_comparison_declared(
        self,
        current: str,
        candidate: str,
    ) -> bool:
        configured = self.dataset.model_config.confidence.paired_comparisons
        if configured:
            return (current, candidate) in configured
        return frozenset((current, candidate)) in {
            frozenset(comparison)
            for comparison in self._declared_paired_comparisons()
        }

    def _paired_family_size(self) -> int:
        if not self.dataset.model_config.confidence.paired_gain_enabled:
            return 0
        return (
            self._design_pair_count()
            * self.dataset.model_config.confidence.maximum_looks
        )

    def _paired_alpha(self) -> float:
        config = self.dataset.model_config
        if not config.confidence.paired_gain_enabled:
            return 0.0
        alpha = 1.0 - config.confidence_level
        alpha *= config.confidence.paired_gain_alpha_fraction
        return alpha / self._paired_family_size()

    def _initial_envelopes(self) -> dict[str, dict[str, Any]]:
        z = self._joint_z()
        config = self.dataset.model_config
        envelopes: dict[str, dict[str, Any]] = {}
        for design_id in self.dataset.design_ids:
            sample = self.dataset.training[design_id]
            observed = (
                design_id in config.observed_design_ids
                and sample.eligible_sessions > 0
            )
            access: dict[str, Interval] = {}
            for representation_id in self.dataset.representation_ids:
                if not self.dataset.affordable(design_id, representation_id):
                    interval = Interval(0.0, 0.0)
                elif self.model_kind == "assumption_free_box" or not observed:
                    interval = Interval(0.0, 1.0)
                else:
                    interval = _wilson_interval(
                        sample.access_counts[representation_id],
                        sample.eligible_sessions,
                        z,
                    )
                access[representation_id] = interval

            groups: dict[str, Interval] = {}
            for group in config.substitution_groups:
                key = _group_key(group)
                if (
                    self.model_kind == "coupled_awm"
                    and observed
                    and config.assumption_enabled(
                        "substitution_group_monotonicity"
                    )
                ):
                    groups[key] = _wilson_interval(
                        sample.group_access_counts[key],
                        sample.eligible_sessions,
                        z,
                    )
                else:
                    groups[key] = Interval(0.0, 1.0)

            success = (
                Interval(0.0, 1.0)
                if self.model_kind == "assumption_free_box" or not observed
                else _wilson_interval(
                    sample.success_count,
                    sample.eligible_sessions,
                    z,
                )
            )
            envelopes[design_id] = {
                "access": access,
                "groups": groups,
                "success": success,
                "constraints": set(
                    ["affordability", "per-class-access-cap"]
                ),
            }
        return envelopes

    def _quotes_componentwise_no_higher(
        self,
        target: str,
        source: str,
        representations: tuple[str, ...],
    ) -> bool:
        return all(
            self.dataset.quote(target, representation_id)
            <= self.dataset.quote(source, representation_id)
            and (
                not self.dataset.system.designs[source]
                .paths[representation_id]
                .available
                or self.dataset.system.designs[target]
                .paths[representation_id]
                .available
            )
            for representation_id in representations
        )

    def _own_price_comparable(
        self,
        target: str,
        source: str,
        representation_id: str,
    ) -> bool:
        if self.dataset.quote(target, representation_id) >= self.dataset.quote(
            source,
            representation_id,
        ):
            return False
        if not self.dataset.model_config.own_price_requires_other_quotes_equal:
            return True
        return all(
            math.isclose(
                self.dataset.quote(target, other),
                self.dataset.quote(source, other),
            )
            for other in self.dataset.representation_ids
            if other != representation_id
        )

    @staticmethod
    def _replace_interval(
        interval: Interval,
        *,
        lower: float | None = None,
        upper: float | None = None,
    ) -> tuple[Interval, bool]:
        result = Interval(
            interval.lower if lower is None else max(interval.lower, lower),
            interval.upper if upper is None else min(interval.upper, upper),
        )
        return result, result != interval

    def _apply_structural_constraints(
        self,
        envelopes: dict[str, dict[str, Any]],
    ) -> None:
        config = self.dataset.model_config
        if self.model_kind != "coupled_awm":
            return
        changed = True
        while changed:
            changed = False
            for target in self.dataset.design_ids:
                for source in self.dataset.design_ids:
                    if target == source:
                        continue
                    if config.assumption_enabled("own_price_monotonicity"):
                        for representation_id in self.dataset.representation_ids:
                            if not self._own_price_comparable(
                                target,
                                source,
                                representation_id,
                            ):
                                continue
                            target_interval = envelopes[target]["access"][
                                representation_id
                            ]
                            source_interval = envelopes[source]["access"][
                                representation_id
                            ]
                            target_interval, target_changed = self._replace_interval(
                                target_interval,
                                lower=source_interval.lower,
                            )
                            source_interval, source_changed = self._replace_interval(
                                source_interval,
                                upper=target_interval.upper,
                            )
                            envelopes[target]["access"][
                                representation_id
                            ] = target_interval
                            envelopes[source]["access"][
                                representation_id
                            ] = source_interval
                            if target_changed or source_changed:
                                changed = True
                                envelopes[target]["constraints"].add(
                                    "own-price-monotonicity"
                                )
                                envelopes[source]["constraints"].add(
                                    "own-price-monotonicity"
                                )

                    if config.assumption_enabled(
                        "substitution_group_monotonicity"
                    ):
                        for group in config.substitution_groups:
                            if not self._quotes_componentwise_no_higher(
                                target,
                                source,
                                group,
                            ):
                                continue
                            if not any(
                                self.dataset.quote(target, representation_id)
                                < self.dataset.quote(source, representation_id)
                                for representation_id in group
                            ):
                                continue
                            key = _group_key(group)
                            target_interval = envelopes[target]["groups"][key]
                            source_interval = envelopes[source]["groups"][key]
                            target_interval, target_changed = self._replace_interval(
                                target_interval,
                                lower=source_interval.lower,
                            )
                            source_interval, source_changed = self._replace_interval(
                                source_interval,
                                upper=target_interval.upper,
                            )
                            envelopes[target]["groups"][key] = target_interval
                            envelopes[source]["groups"][key] = source_interval
                            if target_changed or source_changed:
                                changed = True
                                envelopes[target]["constraints"].add(
                                    "substitution-group-monotonicity"
                                )
                                envelopes[source]["constraints"].add(
                                    "substitution-group-monotonicity"
                                )

                    if config.assumption_enabled("success_monotonicity"):
                        if not self._quotes_componentwise_no_higher(
                            target,
                            source,
                            self.dataset.representation_ids,
                        ):
                            continue
                        if not any(
                            self.dataset.quote(target, representation_id)
                            < self.dataset.quote(source, representation_id)
                            for representation_id in self.dataset.representation_ids
                        ):
                            continue
                        target_interval = envelopes[target]["success"]
                        source_interval = envelopes[source]["success"]
                        target_interval, target_changed = self._replace_interval(
                            target_interval,
                            lower=source_interval.lower,
                        )
                        source_interval, source_changed = self._replace_interval(
                            source_interval,
                            upper=target_interval.upper,
                        )
                        envelopes[target]["success"] = target_interval
                        envelopes[source]["success"] = source_interval
                        if target_changed or source_changed:
                            changed = True
                            envelopes[target]["constraints"].add(
                                "success-monotonicity"
                            )
                            envelopes[source]["constraints"].add(
                                "success-monotonicity"
                            )
            for design_id in self.dataset.design_ids:
                if self._tighten_within_design(envelopes[design_id]):
                    changed = True

    def _tighten_within_design(self, envelope: dict[str, Any]) -> bool:
        changed = False
        access: dict[str, Interval] = envelope["access"]
        groups: dict[str, Interval] = envelope["groups"]
        for group in self.dataset.model_config.substitution_groups:
            key = _group_key(group)
            group_interval = groups[key]
            lower_sum = sum(access[item].lower for item in group)
            upper_sum = sum(access[item].upper for item in group)
            tightened, group_changed = self._replace_interval(
                group_interval,
                lower=lower_sum,
                upper=upper_sum,
            )
            groups[key] = tightened
            changed = changed or group_changed
            for representation_id in group:
                others_upper = sum(
                    access[other].upper
                    for other in group
                    if other != representation_id
                )
                others_lower = sum(
                    access[other].lower
                    for other in group
                    if other != representation_id
                )
                interval, item_changed = self._replace_interval(
                    access[representation_id],
                    lower=max(0.0, tightened.lower - others_upper),
                    upper=tightened.upper - others_lower,
                )
                access[representation_id] = interval
                changed = changed or item_changed
        if sum(value.lower for value in access.values()) > 1.0 + 1e-9:
            raise AWMConfigError(
                "AWM constraints are infeasible: access lower bounds exceed "
                "the per-session access cap"
            )
        return changed

    def _unit_cost_intervals(self, design_id: str) -> dict[str, Interval]:
        radius = self.dataset.model_config.cost_relative_radius
        return {
            representation_id: Interval(
                max(
                    0.0,
                    self.dataset.system.designs[design_id]
                    .paths[representation_id]
                    .realized_cost
                    * (1.0 - radius),
                ),
                self.dataset.system.designs[design_id]
                .paths[representation_id]
                .realized_cost
                * (1.0 + radius),
            )
            for representation_id in self.dataset.representation_ids
        }

    def _service_cost_support_upper(self, design_id: str) -> float:
        return max(
            interval.upper
            for interval in self._unit_cost_intervals(design_id).values()
        )

    def _observed_service_cost_interval(
        self,
        design_id: str,
        sample: DesignSample,
    ) -> Interval:
        costs = self._unit_cost_intervals(design_id)
        support_upper = max(
            interval.upper for interval in costs.values()
        )
        if any(
            value < 0.0 or value > support_upper + 1e-9
            for value in sample.service_costs
        ):
            raise AWMConfigError(
                f"observed service cost for {design_id} exceeds the "
                "declared cost-confidence support"
            )
        sample_mean = sum(sample.service_costs) / sample.eligible_sessions
        radius = support_upper * math.sqrt(
            math.log(2.0 / self._joint_metric_alpha())
            / (2.0 * sample.eligible_sessions)
        )
        return Interval(
            max(0.0, sample_mean - radius),
            min(support_upper, sample_mean + radius),
        )

    def _weighted_access_cost(
        self,
        access: dict[str, Interval],
        groups: dict[str, Interval],
        costs: dict[str, Interval],
        *,
        maximize: bool,
    ) -> float:
        values = {
            representation_id: interval.lower
            for representation_id, interval in access.items()
        }
        weights = {
            representation_id: (
                interval.upper if maximize else interval.lower
            )
            for representation_id, interval in costs.items()
        }

        def allocate(
            candidates: tuple[str, ...],
            amount: float,
            *,
            require_full: bool,
        ) -> None:
            nonlocal values
            order = sorted(
                candidates,
                key=lambda item: weights[item],
                reverse=maximize,
            )
            remaining = amount
            for representation_id in order:
                capacity = access[representation_id].upper - values[
                    representation_id
                ]
                addition = min(capacity, remaining)
                values[representation_id] += addition
                remaining -= addition
                if remaining <= 1e-12:
                    return
            if require_full and remaining > 1e-9:
                raise AWMConfigError(
                    "AWM constraints are infeasible: group floor exceeds "
                    "representation upper bounds"
                )

        for group in self.dataset.model_config.substitution_groups:
            floor = groups[_group_key(group)].lower
            current = sum(values[item] for item in group)
            if floor > current:
                allocate(group, floor - current, require_full=True)

        if sum(values.values()) > 1.0 + 1e-9:
            raise AWMConfigError(
                "AWM constraints are infeasible: group floors exceed the "
                "per-session access cap"
            )

        if maximize:
            remaining_cap = 1.0 - sum(values.values())
            allocate(
                self.dataset.representation_ids,
                max(0.0, remaining_cap),
                require_full=False,
            )
        return sum(
            values[representation_id] * weights[representation_id]
            for representation_id in self.dataset.representation_ids
        )

    def _fit(self) -> dict[str, DesignBounds]:
        envelopes = self._initial_envelopes()
        for envelope in envelopes.values():
            self._tighten_within_design(envelope)
        self._apply_structural_constraints(envelopes)
        result: dict[str, DesignBounds] = {}
        config = self.dataset.model_config
        for design_id in self.dataset.design_ids:
            envelope = envelopes[design_id]
            costs = self._unit_cost_intervals(design_id)
            cost_lower = self._weighted_access_cost(
                envelope["access"],
                envelope["groups"],
                costs,
                maximize=False,
            )
            cost_upper = self._weighted_access_cost(
                envelope["access"],
                envelope["groups"],
                costs,
                maximize=True,
            )
            observed = (
                design_id in config.observed_design_ids
                and self.model_kind != "assumption_free_box"
            )
            if observed:
                observed_cost = self._observed_service_cost_interval(
                    design_id,
                    self.dataset.training[design_id],
                )
                cost_lower = max(cost_lower, observed_cost.lower)
                cost_upper = min(cost_upper, observed_cost.upper)
                if cost_lower > cost_upper + 1e-9:
                    raise AWMConfigError(
                        f"service-cost confidence sets are infeasible for "
                        f"{design_id}"
                    )
                envelope["constraints"].add(
                    "service-cost-confidence-set"
                )
            storage_cost = self.dataset.storage_costs[design_id]
            horizon = self.dataset.horizon_sessions
            task_value = self.dataset.task_class.task_value
            resource_weight = self.dataset.system.resource_cost_weight
            phi = Interval(
                horizon
                * (
                    task_value * envelope["success"].lower
                    - resource_weight * cost_upper
                )
                - storage_cost,
                horizon
                * (
                    task_value * envelope["success"].upper
                    - resource_weight * cost_lower
                )
                - storage_cost,
            )
            transition = self.dataset.forward_transition_costs[design_id]
            transition_radius = config.transition_relative_radius
            result[design_id] = DesignBounds(
                design_id=design_id,
                model_kind=self.model_kind,
                observed=observed,
                training_sessions=self.dataset.training[
                    design_id
                ].eligible_sessions,
                access=dict(envelope["access"]),
                group_access=dict(envelope["groups"]),
                success=envelope["success"],
                service_cost_per_session=Interval(cost_lower, cost_upper),
                storage_cost=storage_cost,
                transition_cost=Interval(
                    max(0.0, transition * (1.0 - transition_radius)),
                    transition * (1.0 + transition_radius),
                ),
                phi=phi,
                constraints_applied=tuple(
                    sorted(envelope["constraints"])
                ),
            )
        return result
