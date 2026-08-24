from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
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
    family_size: int
    maximum_looks: int
    alpha_per_pair_look: float
    point_estimate_per_session: float
    support_per_session: Interval
    gain_per_session: Interval
    phi_gain: Interval
    transition_adjusted_gain: Interval

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_row(self, *, model_kind: str) -> dict[str, Any]:
        return {
            "model_kind": model_kind,
            "current_design_id": self.current_design_id,
            "candidate_design_id": self.candidate_design_id,
            "method": self.method,
            "pair_count": self.pair_count,
            "family_size": self.family_size,
            "maximum_looks": self.maximum_looks,
            "alpha_per_pair_look": self.alpha_per_pair_look,
            "point_estimate_per_session": self.point_estimate_per_session,
            "support_lower_per_session": self.support_per_session.lower,
            "support_upper_per_session": self.support_per_session.upper,
            "gain_lower_per_session": self.gain_per_session.lower,
            "gain_upper_per_session": self.gain_per_session.upper,
            "phi_gain_lower": self.phi_gain.lower,
            "phi_gain_upper": self.phi_gain.upper,
            "transition_adjusted_gain_lower": (
                self.transition_adjusted_gain.lower
            ),
            "transition_adjusted_gain_upper": (
                self.transition_adjusted_gain.upper
            ),
        }


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


def _empirical_bernstein_interval(
    values: tuple[float, ...],
    *,
    support: Interval,
    delta: float,
) -> Interval:
    """Two-sided fixed-look Maurer-Pontil empirical Bernstein interval.

    The one-sided bound is applied to both tails with ``delta / 2`` and
    scaled from [0, 1] to the declared finite support. A caller that consults
    multiple design pairs or looks must allocate ``delta`` by a union bound.
    """
    if not values:
        return support
    if not 0.0 < delta < 1.0:
        raise ValueError("empirical Bernstein delta must be in (0, 1)")
    for value in values:
        if not support.contains(value, tolerance=1e-8):
            raise AWMConfigError(
                "paired utility difference exceeds its declared support"
            )
    sample_mean = mean(values)
    if len(values) < 2 or support.width <= 0.0:
        return Interval(
            max(support.lower, sample_mean),
            min(support.upper, sample_mean),
        ) if support.width <= 0.0 else support
    sample_variance = variance(values)
    log_term = math.log(4.0 / delta)
    radius = math.sqrt(
        2.0 * sample_variance * log_term / len(values)
    ) + (
        7.0 * support.width * log_term
        / (3.0 * (len(values) - 1))
    )
    return Interval(
        max(support.lower, sample_mean - radius),
        min(support.upper, sample_mean + radius),
    )


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
            "sampling_unit": "paired-workload-repetition-seed",
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
            "paired_design_pair_count": self._design_pair_count(),
            "maximum_looks": config.confidence.maximum_looks,
            "paired_family_size": self._paired_family_size(),
            "paired_alpha_per_pair_look": self._paired_alpha(),
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
        return (
            "paired-fixed-looks-empirical-bernstein"
            if self.paired_gain_certificate(current, candidate) is not None
            else "marginal-phi-difference"
        )

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
        keys = tuple(sorted(
            set(current_observations).intersection(candidate_observations)
        ))
        if len(keys) < config.paired_gain_minimum_pairs:
            self._paired_gain_cache[cache_key] = None
            return None

        task_value = self.dataset.task_class.task_value
        resource_weight = self.dataset.system.resource_cost_weight
        values = tuple(
            task_value
            * (
                candidate_observations[key].success
                - current_observations[key].success
            )
            - resource_weight
            * (
                candidate_observations[key].service_cost
                - current_observations[key].service_cost
            )
            for key in keys
        )
        current_cost_upper = self._service_cost_support_upper(current)
        candidate_cost_upper = self._service_cost_support_upper(candidate)
        support = Interval(
            -task_value - resource_weight * candidate_cost_upper,
            task_value + resource_weight * current_cost_upper,
        )
        per_session = _empirical_bernstein_interval(
            values,
            support=support,
            delta=self._paired_alpha(),
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
            family_size=self._paired_family_size(),
            maximum_looks=config.maximum_looks,
            alpha_per_pair_look=self._paired_alpha(),
            point_estimate_per_session=mean(values),
            support_per_session=support,
            gain_per_session=per_session,
            phi_gain=phi_gain,
            transition_adjusted_gain=adjusted,
        )
        self._paired_gain_cache[cache_key] = certificate
        return certificate

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
        count = len(self.dataset.design_ids)
        return max(1, count * (count - 1) // 2)

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
