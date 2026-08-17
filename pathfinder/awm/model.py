from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from statistics import NormalDist
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

    @property
    def bounds(self) -> dict[str, DesignBounds]:
        return dict(self._bounds)

    def lower_bound(self, design_id: str) -> float:
        return self._bounds[design_id].phi.lower

    def upper_bound(self, design_id: str) -> float:
        return self._bounds[design_id].phi.upper

    def pessimistic_gain(self, current: str, candidate: str) -> float:
        return (
            self.lower_bound(candidate)
            - self.upper_bound(current)
            - self._bounds[candidate].transition_cost.upper
        )

    def optimistic_gain(self, current: str, candidate: str) -> float:
        return (
            self.upper_bound(candidate)
            - self.lower_bound(current)
            - self._bounds[candidate].transition_cost.lower
        )

    def _joint_z(self) -> float:
        alpha = self._joint_metric_alpha()
        return NormalDist().inv_cdf(1.0 - alpha / 2.0)

    def _joint_metric_alpha(self) -> float:
        observed = len(self.dataset.model_config.observed_design_ids)
        metric_count = observed * (
            len(self.dataset.representation_ids)
            + 2
            + len(self.dataset.model_config.substitution_groups)
        )
        metric_count = max(1, metric_count)
        return (
            1.0 - self.dataset.model_config.confidence_level
        ) / metric_count

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
