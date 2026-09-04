"""Weighted certificate core for a distributed-policy confirmation run.

The frozen confirmation plan declares a stratified estimand: a weighted
average of per-stratum policy effects, under weights fixed *externally* to
whatever sample gets collected. This module evaluates that estimand and
nothing else.

Three properties carry the scientific weight.

**The weights are the frozen ones.** Aggregation uses the plan's integer
quotas, normalised deterministically. Using the collected sample's empirical
proportions instead would silently redefine the target every time collection
came out uneven -- which is exactly what oversampling an active stratum makes
happen.

**Structural-safe strata are zero by construction.** Where the policy applies
the safe design, the effect is identically zero: no candidate observation
exists, none is required, and the stratum consumes no confidence-family
alpha. Spending alpha on a quantity that cannot vary would widen every other
stratum's interval for nothing.

**The confidence bound is not reimplemented.** Every interval comes from the
existing one-sided bounded-mean KL core in ``pathfinder.awm.certificate``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from ..awm.model import Interval

# ``pathfinder.awm`` imports ``pathfinder.distributed.preregistration``, so
# importing the certificate core at module scope would close a cycle. The
# core is pulled in on first use instead; it is a one-time cost per call
# site, not per observation.


WEIGHTED_CERTIFICATE_SCHEMA_VERSION = (
    "pathfinder.distributed-policy-weighted-certificate/v1alpha1"
)
STRATUM_ROLES = ("active", "structural_safe")
BOUND_METHOD = "workload-cluster-one-sided-bounded-mean-kl"


class WeightedCertificateError(ValueError):
    """Raised when weighted certificate inputs are invalid."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise WeightedCertificateError(message)


def _certificate_core():
    """The existing AWM certificate primitives, imported lazily."""
    from ..awm.certificate import (
        CERTIFICATE_STATES,
        GATE_IDS,
        GATE_SIDES,
        _bounded_mean,
        _BoundedMean,
        _decide,
        _gate,
    )

    return (
        CERTIFICATE_STATES,
        GATE_IDS,
        GATE_SIDES,
        _bounded_mean,
        _BoundedMean,
        _decide,
        _gate,
    )


@dataclass(frozen=True)
class StratumEvidence:
    """One stratum's independent workload-cluster observations.

    ``success_differences`` and ``cost_savings`` hold exactly one value per
    independent workload, with repetitions already averaged inside the
    workload. A structural-safe stratum carries none of either.
    """

    stratum_id: str
    role: str
    integer_weight: int
    success_differences: tuple[float, ...] = ()
    cost_savings: tuple[float, ...] = ()

    def __post_init__(self) -> None:
        _require(
            self.role in STRATUM_ROLES,
            f"{self.stratum_id}: role must be one of "
            + ", ".join(STRATUM_ROLES),
        )
        _require(
            isinstance(self.integer_weight, int)
            and not isinstance(self.integer_weight, bool)
            and self.integer_weight >= 0,
            f"{self.stratum_id}: integer_weight must be a non-negative int",
        )
        _require(
            len(self.success_differences) == len(self.cost_savings),
            f"{self.stratum_id}: success and cost observations must pair "
            "one-to-one per independent workload",
        )
        if self.role == "structural_safe":
            _require(
                not self.success_differences and not self.cost_savings,
                f"{self.stratum_id} applies the safe design, so a candidate "
                "observation cannot exist; supplying one would represent a "
                "structural zero as a measured safe-versus-safe pair",
            )
        else:
            _require(
                self.success_differences,
                f"{self.stratum_id} is active but has no workload clusters",
            )

    @property
    def is_active(self) -> bool:
        return self.role == "active"

    @property
    def independent_workloads(self) -> int:
        return len(self.success_differences)


def _zero_bound(support: Interval, bounded_mean_type):
    """The exact [0, 0] bound of a structurally zero effect."""
    normalized_zero = (0.0 - support.lower) / support.width
    return bounded_mean_type(
        point_estimate=0.0,
        lower_bound=0.0,
        upper_bound=0.0,
        normalized_point_estimate=normalized_zero,
        normalized_lower_bound=normalized_zero,
        normalized_upper_bound=normalized_zero,
        support=support,
    )


def _weighted_bound(
    contributions: Sequence[tuple[float, Any]],
    support: Interval,
    bounded_mean_type,
):
    """Combine stratum bounds under fixed weights.

    Each tail is summed separately, which is what makes the result a
    simultaneous statement: the stratum bounds hold jointly under the family
    adjustment, so any weighted combination of them holds too.
    """
    point = sum(weight * bound.point_estimate for weight, bound in contributions)
    lower = sum(weight * bound.lower_bound for weight, bound in contributions)
    upper = sum(weight * bound.upper_bound for weight, bound in contributions)
    return bounded_mean_type(
        point_estimate=point,
        lower_bound=lower,
        upper_bound=upper,
        normalized_point_estimate=(point - support.lower) / support.width,
        normalized_lower_bound=(lower - support.lower) / support.width,
        normalized_upper_bound=(upper - support.lower) / support.width,
        support=support,
    )


def evaluate_weighted_policy_certificate(
    strata: Iterable[StratumEvidence],
    *,
    success_difference_support: Interval,
    cost_saving_support: Interval,
    alpha: float,
    delta_success_margin: float,
    minimum_cost_saving: float,
    minimum_independent_workloads_by_stratum: Mapping[str, int],
    safe_design_id: str,
    policy_id: str,
    bound_method: str = BOUND_METHOD,
) -> dict[str, Any]:
    """Evaluate the frozen stratified estimand and decide one state.

    ``minimum_independent_workloads_by_stratum`` is the frozen per-active-
    stratum floor. A stratum below its floor forces INSUFFICIENT_EVIDENCE
    even when both numerical bounds pass: a bound computed from three
    clusters is arithmetically valid and evidentially thin, and the plan is
    where that judgement was made.
    """
    (
        CERTIFICATE_STATES,
        GATE_IDS,
        GATE_SIDES,
        _bounded_mean,
        _BoundedMean,
        _decide,
        _gate,
    ) = _certificate_core()
    ordered = sorted(strata, key=lambda item: item.stratum_id)
    _require(ordered, "at least one stratum is required")
    _require(
        len({item.stratum_id for item in ordered}) == len(ordered),
        "duplicate stratum identifiers",
    )
    active = [item for item in ordered if item.is_active]
    _require(
        active,
        "every stratum is structurally safe; there is no candidate effect "
        "to certify",
    )
    weight_total = sum(item.integer_weight for item in ordered)
    _require(
        weight_total > 0,
        "target stratum weights must have a positive total",
    )
    _require(
        0.0 < alpha < 1.0,
        "alpha must be in (0, 1)",
    )

    minima = dict(minimum_independent_workloads_by_stratum)
    active_ids = {item.stratum_id for item in active}
    structural_ids = {
        item.stratum_id for item in ordered if not item.is_active
    }
    _require(
        set(minima) == active_ids,
        "minimum_independent_workloads_by_active_stratum must name exactly "
        f"the active strata {sorted(active_ids)}; got {sorted(minima)}",
    )
    _require(
        not (set(minima) & structural_ids),
        "a structural-safe stratum cannot carry an independent-workload "
        "minimum; it has no candidate observations to count",
    )
    for stratum_id, value in sorted(minima.items()):
        _require(
            isinstance(value, int)
            and not isinstance(value, bool)
            and value >= 1,
            f"minimum for {stratum_id} must be a positive integer",
        )

    # Only active strata consume alpha. A structural zero cannot vary, so a
    # confidence bound for it would spend the family budget on nothing and
    # widen every other stratum's interval.
    family_components = [
        {"stratum_id": item.stratum_id, "gate_id": gate_id, "side": side}
        for item in active
        for gate_id in GATE_IDS
        for side in GATE_SIDES
    ]
    family_size = len(family_components)
    adjusted_alpha = alpha / family_size

    stratum_results: list[dict[str, Any]] = []
    success_contributions: list[tuple[float, Any]] = []
    cost_contributions: list[tuple[float, Any]] = []
    for item in ordered:
        weight = item.integer_weight / weight_total
        if item.is_active:
            success_bound = _bounded_mean(
                tuple(item.success_differences),
                support=success_difference_support,
                delta=adjusted_alpha,
                quantity=f"{item.stratum_id}.success_difference",
                method=bound_method,
            )
            cost_bound = _bounded_mean(
                tuple(item.cost_savings),
                support=cost_saving_support,
                delta=adjusted_alpha,
                quantity=f"{item.stratum_id}.cost_saving",
                method=bound_method,
            )
            alpha_consumed = adjusted_alpha * len(GATE_IDS) * len(GATE_SIDES)
        else:
            success_bound = _zero_bound(
                success_difference_support,
                _BoundedMean,
            )
            cost_bound = _zero_bound(cost_saving_support, _BoundedMean)
            alpha_consumed = 0.0
        success_contributions.append((weight, success_bound))
        cost_contributions.append((weight, cost_bound))
        stratum_results.append({
            "stratum_id": item.stratum_id,
            "role": item.role,
            "structural_zero_effect": not item.is_active,
            "integer_weight": item.integer_weight,
            "normalized_weight": weight,
            "independent_workloads": item.independent_workloads,
            "required_independent_workloads": (
                minima.get(item.stratum_id) if item.is_active else None
            ),
            "meets_minimum_independent_workloads": (
                item.independent_workloads >= minima[item.stratum_id]
                if item.is_active
                else None
            ),
            "candidate_observations_required": item.is_active,
            "alpha_consumed": alpha_consumed,
            "success_difference": {
                "point_estimate": success_bound.point_estimate,
                "lower_bound": success_bound.lower_bound,
                "upper_bound": success_bound.upper_bound,
            },
            "cost_saving": {
                "point_estimate": cost_bound.point_estimate,
                "lower_bound": cost_bound.lower_bound,
                "upper_bound": cost_bound.upper_bound,
            },
        })

    overall_success = _weighted_bound(
        success_contributions,
        success_difference_support,
        _BoundedMean,
    )
    overall_cost = _weighted_bound(
        cost_contributions,
        cost_saving_support,
        _BoundedMean,
    )
    gates = (
        _gate(
            overall_success,
            -delta_success_margin,
            gate_id="success_non_inferiority",
            comparison=(
                "weighted_lower_bound(candidate_success - safe_success) "
                ">= -delta_success_margin"
            ),
        ),
        _gate(
            overall_cost,
            minimum_cost_saving,
            gate_id="cost_improvement",
            comparison=(
                "weighted_lower_bound(safe_cost - candidate_cost) "
                ">= minimum_cost_saving"
            ),
        ),
    )
    active_workloads = sum(item.independent_workloads for item in active)
    deficient = tuple(
        f"{item.stratum_id}={item.independent_workloads}<"
        f"{minima[item.stratum_id]}"
        for item in active
        if item.independent_workloads < minima[item.stratum_id]
    )
    decision = _decide(
        gates,
        independent_workload_count=active_workloads,
        # The scalar floor is neutralised here; sufficiency is a per-stratum
        # question, applied immediately below.
        minimum_independent_workloads=1,
        safe_design_id=safe_design_id,
        candidate_design_id=policy_id,
    )
    if deficient and decision["certificate_state"] != "UNSAFE":
        # An established violation is retained: harm shown on thin data is
        # still harm, and both states fall back to the safe design anyway.
        reason = "insufficient_independent_workloads:" + "+".join(deficient)
        decision = {
            "certificate_state": "INSUFFICIENT_EVIDENCE",
            "applied_design_id": safe_design_id,
            "fallback_design_id": safe_design_id,
            "fallback_applied": True,
            "fallback_reason": reason,
            "decision_reason": reason,
        }
    point_thresholds = {
        "success_non_inferiority_point_passes": (
            overall_success.point_estimate >= -delta_success_margin
        ),
        "cost_improvement_point_passes": (
            overall_cost.point_estimate >= minimum_cost_saving
        ),
        "note": (
            "Point-threshold passage is not a certificate. It says where the "
            "estimate landed, not what the interval can rule out."
        ),
    }
    return {
        "schema_version": WEIGHTED_CERTIFICATE_SCHEMA_VERSION,
        "policy_id": policy_id,
        "safe_design_id": safe_design_id,
        "bound_method": bound_method,
        "aggregation_rule": (
            "overall_bound = sum over strata of "
            "normalized_weight * stratum_bound, evaluated separately for "
            "each tail; weights are the frozen integer quotas normalised, "
            "never the empirical sample proportions"
        ),
        "confidence": {
            "alpha": alpha,
            "family_adjustment": "bonferroni",
            "family_size": family_size,
            "adjusted_alpha": adjusted_alpha,
            "unadjusted_confidence_level": 1.0 - alpha,
            "adjusted_confidence_level": 1.0 - adjusted_alpha,
            "family_components": family_components,
            "structural_zero_strata_consume_no_alpha": True,
            "structural_zero_stratum_ids": [
                item.stratum_id for item in ordered if not item.is_active
            ],
        },
        "estimand": {
            "target_stratum_weights_integer": {
                item.stratum_id: item.integer_weight for item in ordered
            },
            "target_stratum_weights_normalized": {
                item.stratum_id: item.integer_weight / weight_total
                for item in ordered
            },
            "target_stratum_weight_total": weight_total,
            "weights_are_empirical_sample_proportions": False,
        },
        "strata": stratum_results,
        "overall_success_difference": {
            "point_estimate": overall_success.point_estimate,
            "lower_bound": overall_success.lower_bound,
            "upper_bound": overall_success.upper_bound,
            "support_lower": success_difference_support.lower,
            "support_upper": success_difference_support.upper,
        },
        "overall_cost_saving": {
            "point_estimate": overall_cost.point_estimate,
            "lower_bound": overall_cost.lower_bound,
            "upper_bound": overall_cost.upper_bound,
            "support_lower": cost_saving_support.lower,
            "support_upper": cost_saving_support.upper,
        },
        "gates": [dict(gate) for gate in gates],
        "point_thresholds": point_thresholds,
        "certificate_state": decision["certificate_state"],
        "decision": decision,
        "active_independent_workloads": active_workloads,
        "active_stratum_ids": [item.stratum_id for item in active],
        "minimum_independent_workloads": {
            "required_by_active_stratum": dict(sorted(minima.items())),
            "observed_by_active_stratum": {
                item.stratum_id: item.independent_workloads
                for item in active
            },
            "deficient_strata": list(deficient),
            "every_active_stratum_meets_its_minimum": not deficient,
            "repetitions_count_toward_minimum": False,
            "structural_zero_strata_count_toward_minimum": False,
        },
        "thresholds": {
            "delta_success_margin": delta_success_margin,
            "minimum_cost_saving": minimum_cost_saving,
        },
        "certificate_states": list(CERTIFICATE_STATES),
    }


def distance_to_threshold(
    certificate: Mapping[str, Any],
) -> dict[str, float]:
    """How far each weighted lower bound sits from its gate threshold."""
    thresholds = certificate["thresholds"]
    return {
        "success_non_inferiority": (
            certificate["overall_success_difference"]["lower_bound"]
            - (-thresholds["delta_success_margin"])
        ),
        "cost_improvement": (
            certificate["overall_cost_saving"]["lower_bound"]
            - thresholds["minimum_cost_saving"]
        ),
    }
