"""Fixed-seed Monte Carlo calibration of the v3alpha5 safety certificate.

This is an *empirical* calibration, not a decision-logic unit test. It draws
many synthetic workload-cluster datasets whose true stratum-level effects are
known by construction, runs the deployed decision core over each, and
estimates the family-wise false-safe rate, the simultaneous coverage rate,
and the frequency of every certificate state.

The harness can also be pointed at a deliberately invalid bound
(``point-estimate-no-uncertainty``). That negative control exists so that a
calibration result of "no false-safe commits" is informative: the same
harness demonstrably reports a large false-safe rate when the bound really is
anti-conservative.
"""

from __future__ import annotations

import json
import math
import random
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from statistics import mean
from typing import Any, Mapping

from ..integrations.flowmesh.pilot import _write_csv, _write_json
from .certificate import (
    CERTIFICATE_STATES,
    GATE_IDS,
    GATE_SIDES,
    _ClusterObservation,
    evaluate_stratum_certificate,
)
from .contracts import AWMConfigError
from .model import Interval, _bernoulli_kl_interval


CALIBRATION_CONFIG_SCHEMA_VERSION = (
    "pathfinder.awm-certificate-calibration/v1alpha1"
)
CALIBRATION_REPORT_SCHEMA_VERSION = (
    "pathfinder.awm-certificate-calibration-report/v1alpha1"
)
CALIBRATION_BOUND_METHODS = (
    "workload-cluster-one-sided-bounded-mean-kl",
    "point-estimate-no-uncertainty",
)
CONTROL_BOUND_METHOD = "point-estimate-no-uncertainty"
_SAFE_DESIGN_ID = "D_origin_reference"
_CANDIDATE_DESIGN_ID = "D_candidate_reference"


@dataclass(frozen=True)
class CalibrationStratum:
    """One stratum of a simulated confidence family with a known truth."""

    stratum_id: str
    workload_count: int
    true_success_difference: float
    success_heterogeneity: float
    true_cost_saving: float
    cost_heterogeneity: float
    cost_repetition_jitter: float
    origin_success_probabilities: tuple[float, ...] = ()

    def probabilities(
        self,
        default: tuple[float, ...],
    ) -> tuple[float, ...]:
        return self.origin_success_probabilities or default


@dataclass(frozen=True)
class CalibrationScenario:
    scenario_id: str
    description: str
    strata: tuple[CalibrationStratum, ...]


@dataclass(frozen=True)
class CalibrationConfig:
    schema_version: str
    calibration_id: str
    source_path: Path
    source_sha256: str
    seed: int
    simulations: int
    repetitions: int
    alpha: float
    delta_success_margin: float
    minimum_cost_saving: float
    minimum_independent_workloads: int
    success_difference_support: Interval
    cost_saving_support: Interval
    task_value: float
    resource_cost_weight: float
    origin_success_probabilities: tuple[float, ...]
    origin_costs: tuple[float, ...]
    monte_carlo_confidence: float
    false_safe_rate_tolerance: float
    coverage_tolerance: float
    control_minimum_false_safe_rate: float
    scenarios: tuple[CalibrationScenario, ...]


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise AWMConfigError(f"{name} must be an object")
    return value


def _string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AWMConfigError(f"{name} must be a non-empty string")
    return value.strip()


def _finite_number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AWMConfigError(f"{name} must be a number")
    result = float(value)
    if not math.isfinite(result):
        raise AWMConfigError(f"{name} must be finite")
    return result


def _nonnegative_number(value: Any, name: str) -> float:
    result = _finite_number(value, name)
    if result < 0.0:
        raise AWMConfigError(f"{name} must be non-negative")
    return result


def _positive_integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise AWMConfigError(f"{name} must be a positive integer")
    return value


def _integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise AWMConfigError(f"{name} must be an integer")
    return value


def _number_array(value: Any, name: str) -> tuple[float, ...]:
    if not isinstance(value, list) or not value:
        raise AWMConfigError(f"{name} must be a non-empty array")
    return tuple(
        _finite_number(item, f"{name}[{index}]")
        for index, item in enumerate(value)
    )


def _probability_array(value: Any, name: str) -> tuple[float, ...]:
    result = _number_array(value, name)
    for probability in result:
        if not 0.0 <= probability <= 1.0:
            raise AWMConfigError(f"{name} must be in [0, 1]")
    return result


def _support(value: Any, name: str) -> Interval:
    payload = _mapping(value, name)
    lower = _finite_number(payload.get("lower"), f"{name}.lower")
    upper = _finite_number(payload.get("upper"), f"{name}.upper")
    if upper <= lower:
        raise AWMConfigError(f"{name} must have a positive width")
    return Interval(lower, upper)


def _require(payload: Mapping[str, Any], key: str, name: str) -> Any:
    if key not in payload:
        raise AWMConfigError(f"{name} is a required configuration field")
    return payload[key]


def load_calibration_config(path: str | Path) -> CalibrationConfig:
    """Load and validate one fixed-seed Monte Carlo calibration plan."""
    source = Path(path).resolve()
    if not source.is_file():
        raise AWMConfigError(
            f"AWM calibration configuration does not exist: {source}"
        )
    raw_bytes = source.read_bytes()
    try:
        root = _mapping(json.loads(raw_bytes), "calibration config")
    except json.JSONDecodeError as exc:
        raise AWMConfigError(
            f"invalid AWM calibration JSON: {source}"
        ) from exc

    schema_version = _string(root.get("schema_version"), "schema_version")
    if schema_version != CALIBRATION_CONFIG_SCHEMA_VERSION:
        raise AWMConfigError(
            "unsupported AWM calibration schema_version: " + schema_version
        )

    alpha = _finite_number(
        _require(root, "alpha", "alpha"),
        "alpha",
    )
    if not 0.0 < alpha < 1.0:
        raise AWMConfigError("alpha must be in (0, 1)")
    thresholds = _mapping(root.get("thresholds"), "thresholds")
    delta_success_margin = _nonnegative_number(
        _require(
            thresholds,
            "delta_success_margin",
            "thresholds.delta_success_margin",
        ),
        "thresholds.delta_success_margin",
    )
    minimum_cost_saving = _finite_number(
        _require(
            thresholds,
            "minimum_cost_saving",
            "thresholds.minimum_cost_saving",
        ),
        "thresholds.minimum_cost_saving",
    )
    supports = _mapping(root.get("supports"), "supports")
    success_difference_support = _support(
        supports.get("success_difference"),
        "supports.success_difference",
    )
    cost_saving_support = _support(
        supports.get("cost_saving"),
        "supports.cost_saving",
    )
    generator = _mapping(root.get("generator"), "generator")
    origin_success_probabilities = _probability_array(
        generator.get("origin_success_probabilities"),
        "generator.origin_success_probabilities",
    )
    origin_costs = _number_array(
        generator.get("origin_costs"),
        "generator.origin_costs",
    )
    tolerances = _mapping(root.get("tolerances"), "tolerances")

    raw_scenarios = root.get("scenarios")
    if not isinstance(raw_scenarios, list) or not raw_scenarios:
        raise AWMConfigError("scenarios must be a non-empty array")
    scenarios: list[CalibrationScenario] = []
    seen_ids: set[str] = set()
    for index, raw_scenario in enumerate(raw_scenarios):
        payload = _mapping(raw_scenario, f"scenarios[{index}]")
        scenario_id = _string(
            payload.get("scenario_id"),
            f"scenarios[{index}].scenario_id",
        )
        if scenario_id in seen_ids:
            raise AWMConfigError(
                f"duplicate calibration scenario_id: {scenario_id}"
            )
        seen_ids.add(scenario_id)
        raw_strata = payload.get("strata")
        if not isinstance(raw_strata, list) or not raw_strata:
            raise AWMConfigError(
                f"scenarios[{index}].strata must be a non-empty array"
            )
        strata: list[CalibrationStratum] = []
        seen_strata: set[str] = set()
        for stratum_index, raw_stratum in enumerate(raw_strata):
            name = f"scenarios[{index}].strata[{stratum_index}]"
            stratum_payload = _mapping(raw_stratum, name)
            stratum_id = _string(
                stratum_payload.get("stratum_id"),
                f"{name}.stratum_id",
            )
            if stratum_id in seen_strata:
                raise AWMConfigError(
                    f"duplicate stratum_id in {scenario_id}: {stratum_id}"
                )
            seen_strata.add(stratum_id)
            strata.append(CalibrationStratum(
                stratum_id=stratum_id,
                workload_count=_positive_integer(
                    stratum_payload.get("workload_count"),
                    f"{name}.workload_count",
                ),
                true_success_difference=_finite_number(
                    stratum_payload.get("true_success_difference"),
                    f"{name}.true_success_difference",
                ),
                success_heterogeneity=_nonnegative_number(
                    stratum_payload.get("success_heterogeneity"),
                    f"{name}.success_heterogeneity",
                ),
                true_cost_saving=_finite_number(
                    stratum_payload.get("true_cost_saving"),
                    f"{name}.true_cost_saving",
                ),
                cost_heterogeneity=_nonnegative_number(
                    stratum_payload.get("cost_heterogeneity"),
                    f"{name}.cost_heterogeneity",
                ),
                cost_repetition_jitter=_nonnegative_number(
                    stratum_payload.get("cost_repetition_jitter", 0.0),
                    f"{name}.cost_repetition_jitter",
                ),
                origin_success_probabilities=(
                    _probability_array(
                        stratum_payload["origin_success_probabilities"],
                        f"{name}.origin_success_probabilities",
                    )
                    if "origin_success_probabilities" in stratum_payload
                    else ()
                ),
            ))
        scenarios.append(CalibrationScenario(
            scenario_id=scenario_id,
            description=_string(
                payload.get("description"),
                f"scenarios[{index}].description",
            ),
            strata=tuple(strata),
        ))

    config = CalibrationConfig(
        schema_version=schema_version,
        calibration_id=_string(
            root.get("calibration_id"),
            "calibration_id",
        ),
        source_path=source,
        source_sha256=sha256(raw_bytes).hexdigest(),
        seed=_integer(_require(root, "seed", "seed"), "seed"),
        simulations=_positive_integer(
            _require(root, "simulations", "simulations"),
            "simulations",
        ),
        repetitions=_positive_integer(
            _require(root, "repetitions", "repetitions"),
            "repetitions",
        ),
        alpha=alpha,
        delta_success_margin=delta_success_margin,
        minimum_cost_saving=minimum_cost_saving,
        minimum_independent_workloads=_positive_integer(
            root.get("minimum_independent_workloads", 1),
            "minimum_independent_workloads",
        ),
        success_difference_support=success_difference_support,
        cost_saving_support=cost_saving_support,
        task_value=_nonnegative_number(
            generator.get("task_value", 10.0),
            "generator.task_value",
        ),
        resource_cost_weight=_nonnegative_number(
            generator.get("resource_cost_weight", 1.0),
            "generator.resource_cost_weight",
        ),
        origin_success_probabilities=origin_success_probabilities,
        origin_costs=origin_costs,
        monte_carlo_confidence=_finite_number(
            _require(
                tolerances,
                "monte_carlo_confidence",
                "tolerances.monte_carlo_confidence",
            ),
            "tolerances.monte_carlo_confidence",
        ),
        false_safe_rate_tolerance=_nonnegative_number(
            _require(
                tolerances,
                "false_safe_rate_tolerance",
                "tolerances.false_safe_rate_tolerance",
            ),
            "tolerances.false_safe_rate_tolerance",
        ),
        coverage_tolerance=_nonnegative_number(
            _require(
                tolerances,
                "coverage_tolerance",
                "tolerances.coverage_tolerance",
            ),
            "tolerances.coverage_tolerance",
        ),
        control_minimum_false_safe_rate=_nonnegative_number(
            _require(
                tolerances,
                "control_minimum_false_safe_rate",
                "tolerances.control_minimum_false_safe_rate",
            ),
            "tolerances.control_minimum_false_safe_rate",
        ),
        scenarios=tuple(scenarios),
    )
    _validate_generator_domain(config)
    return config


def _validate_generator_domain(config: CalibrationConfig) -> None:
    """Refuse a plan whose draws would silently leave their declared range.

    Clipping a success probability, or truncating a cost saving at its
    support, would move the realized truth away from the declared one and
    quietly invalidate every coverage number computed against it.
    """
    for scenario in config.scenarios:
        for stratum in scenario.strata:
            for sign in (-1.0, 1.0):
                difference = (
                    stratum.true_success_difference
                    + sign * stratum.success_heterogeneity
                )
                if not config.success_difference_support.contains(difference):
                    raise AWMConfigError(
                        f"{scenario.scenario_id}/{stratum.stratum_id}: a "
                        "workload success difference leaves its declared "
                        "support"
                    )
                for probability in stratum.probabilities(
                    config.origin_success_probabilities
                ):
                    candidate = probability + difference
                    if not 0.0 <= candidate <= 1.0:
                        raise AWMConfigError(
                            f"{scenario.scenario_id}/{stratum.stratum_id}: "
                            "a candidate success probability leaves [0, 1]; "
                            "clipping would bias the declared truth"
                        )
                saving = (
                    stratum.true_cost_saving
                    + sign * stratum.cost_heterogeneity
                )
                for jitter in (
                    -2.0 * stratum.cost_repetition_jitter,
                    2.0 * stratum.cost_repetition_jitter,
                ):
                    if not config.cost_saving_support.contains(
                        saving + jitter
                    ):
                        raise AWMConfigError(
                            f"{scenario.scenario_id}/"
                            f"{stratum.stratum_id}: a workload cost saving "
                            "leaves its declared support"
                        )


def _stratum_observations(
    stratum: CalibrationStratum,
    *,
    rng: random.Random,
    config: CalibrationConfig,
) -> tuple[_ClusterObservation, ...]:
    """Draw one independent workload-cluster sample for a stratum.

    Each workload draws its own latent parameters and then its own complete
    repetition block, so the cluster statistics are i.i.d. across workloads
    and their expectation is exactly the declared truth. Repetitions add
    within-workload noise only; they never add an independent unit.
    """
    observations: list[_ClusterObservation] = []
    for index in range(stratum.workload_count):
        origin_probability = rng.choice(
            stratum.probabilities(config.origin_success_probabilities)
        )
        difference = stratum.true_success_difference + rng.choice(
            (-stratum.success_heterogeneity, stratum.success_heterogeneity)
        )
        candidate_probability = origin_probability + difference
        origin_successes = sum(
            1.0 if rng.random() < origin_probability else 0.0
            for _ in range(config.repetitions)
        )
        candidate_successes = sum(
            1.0 if rng.random() < candidate_probability else 0.0
            for _ in range(config.repetitions)
        )
        success_difference = (
            candidate_successes - origin_successes
        ) / config.repetitions

        origin_cost = rng.choice(config.origin_costs)
        saving = stratum.true_cost_saving + rng.choice(
            (-stratum.cost_heterogeneity, stratum.cost_heterogeneity)
        )
        jitter = stratum.cost_repetition_jitter
        origin_block = [
            origin_cost + rng.choice((-jitter, jitter))
            for _ in range(config.repetitions)
        ]
        candidate_block = [
            origin_cost - saving + rng.choice((-jitter, jitter))
            for _ in range(config.repetitions)
        ]
        # Each tail is symmetric and mean-zero within the block, so the
        # block-mean saving stays an unbiased draw around ``saving``.
        cost_saving = mean(origin_block) - mean(candidate_block)
        observations.append(_ClusterObservation(
            workload_id=f"{stratum.stratum_id}-w{index:04d}",
            success_difference=success_difference,
            cost_saving=cost_saving,
            utility_gain=(
                config.task_value * success_difference
                + config.resource_cost_weight * cost_saving
            ),
        ))
    return tuple(observations)


def _rate_interval(
    successes: int,
    trials: int,
    confidence: float,
) -> dict[str, float]:
    interval = _bernoulli_kl_interval(successes, trials, 1.0 - confidence)
    return {
        "rate": successes / trials if trials else 0.0,
        "lower": interval.lower,
        "upper": interval.upper,
        "confidence": confidence,
        "events": successes,
        "trials": trials,
    }


def _truth_violates_gates(
    stratum: CalibrationStratum,
    *,
    delta_success_margin: float,
    minimum_cost_saving: float,
) -> tuple[bool, tuple[str, ...]]:
    violated: list[str] = []
    if stratum.true_success_difference < -delta_success_margin:
        violated.append("success_non_inferiority")
    if stratum.true_cost_saving < minimum_cost_saving:
        violated.append("cost_improvement")
    return bool(violated), tuple(violated)


def run_certificate_monte_carlo(
    config: CalibrationConfig | str | Path,
    *,
    bound_method: str = "workload-cluster-one-sided-bounded-mean-kl",
    simulations: int | None = None,
) -> dict[str, Any]:
    """Estimate the certificate's error rates over repeated synthetic data."""
    plan = (
        load_calibration_config(config)
        if isinstance(config, (str, Path))
        else config
    )
    if bound_method not in CALIBRATION_BOUND_METHODS:
        raise AWMConfigError(
            f"unknown calibration bound method: {bound_method}"
        )
    total_simulations = simulations or plan.simulations
    utility_support = Interval(
        plan.task_value * plan.success_difference_support.lower
        + plan.resource_cost_weight * plan.cost_saving_support.lower,
        plan.task_value * plan.success_difference_support.upper
        + plan.resource_cost_weight * plan.cost_saving_support.upper,
    )

    scenario_reports: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []
    overall_false_safe = 0
    overall_coverage = 0
    overall_simulations = 0
    for scenario in plan.scenarios:
        family_size = len(scenario.strata) * len(GATE_IDS) * len(GATE_SIDES)
        adjusted_alpha = plan.alpha / family_size
        stratum_states: dict[str, dict[str, int]] = {
            stratum.stratum_id: {state: 0 for state in CERTIFICATE_STATES}
            for stratum in scenario.strata
        }
        stratum_false_safe = {
            stratum.stratum_id: 0 for stratum in scenario.strata
        }
        stratum_false_unsafe = {
            stratum.stratum_id: 0 for stratum in scenario.strata
        }
        stratum_coverage = {
            stratum.stratum_id: {"success": 0, "cost": 0}
            for stratum in scenario.strata
        }
        stratum_fallback = {
            stratum.stratum_id: 0 for stratum in scenario.strata
        }
        family_false_safe = 0
        family_simultaneous_coverage = 0
        for simulation in range(total_simulations):
            simultaneous = True
            any_false_safe = False
            for stratum in scenario.strata:
                rng = random.Random(
                    f"{plan.seed}|{bound_method}|{scenario.scenario_id}"
                    f"|{stratum.stratum_id}|{simulation}"
                )
                observations = _stratum_observations(
                    stratum,
                    rng=rng,
                    config=plan,
                )
                result = evaluate_stratum_certificate(
                    observations,
                    success_difference_support=(
                        plan.success_difference_support
                    ),
                    cost_saving_support=plan.cost_saving_support,
                    utility_support=utility_support,
                    adjusted_alpha=adjusted_alpha,
                    delta_success_margin=plan.delta_success_margin,
                    minimum_cost_saving=plan.minimum_cost_saving,
                    minimum_independent_workloads=(
                        plan.minimum_independent_workloads
                    ),
                    safe_design_id=_SAFE_DESIGN_ID,
                    candidate_design_id=_CANDIDATE_DESIGN_ID,
                    bound_method=bound_method,
                )
                state = result.certificate_state
                stratum_states[stratum.stratum_id][state] += 1
                if state != "SAFE_TO_COMMIT":
                    if result.applied_design_id != _SAFE_DESIGN_ID:
                        raise AssertionError(
                            "a non-safe state must fall back to origin"
                        )
                    stratum_fallback[stratum.stratum_id] += 1

                violates, _ = _truth_violates_gates(
                    stratum,
                    delta_success_margin=plan.delta_success_margin,
                    minimum_cost_saving=plan.minimum_cost_saving,
                )
                if violates and state == "SAFE_TO_COMMIT":
                    stratum_false_safe[stratum.stratum_id] += 1
                    any_false_safe = True
                if not violates and state == "UNSAFE":
                    stratum_false_unsafe[stratum.stratum_id] += 1

                success_covered = (
                    result.success_bound.lower_bound
                    <= stratum.true_success_difference
                    <= result.success_bound.upper_bound
                )
                cost_covered = (
                    result.cost_bound.lower_bound
                    <= stratum.true_cost_saving
                    <= result.cost_bound.upper_bound
                )
                stratum_coverage[stratum.stratum_id]["success"] += int(
                    success_covered
                )
                stratum_coverage[stratum.stratum_id]["cost"] += int(
                    cost_covered
                )
                simultaneous = (
                    simultaneous and success_covered and cost_covered
                )
            family_false_safe += int(any_false_safe)
            family_simultaneous_coverage += int(simultaneous)

        overall_false_safe += family_false_safe
        overall_coverage += family_simultaneous_coverage
        overall_simulations += total_simulations

        stratum_reports = []
        for stratum in scenario.strata:
            violates, violated_gates = _truth_violates_gates(
                stratum,
                delta_success_margin=plan.delta_success_margin,
                minimum_cost_saving=plan.minimum_cost_saving,
            )
            counts = stratum_states[stratum.stratum_id]
            stratum_reports.append({
                "stratum_id": stratum.stratum_id,
                "workload_count": stratum.workload_count,
                "repetition_pair_count": (
                    stratum.workload_count * plan.repetitions
                ),
                "true_success_difference": (
                    stratum.true_success_difference
                ),
                "true_cost_saving": stratum.true_cost_saving,
                "truth_violates_a_gate": violates,
                "violated_gates": list(violated_gates),
                "state_counts": dict(counts),
                "state_frequencies": {
                    state: counts[state] / total_simulations
                    for state in CERTIFICATE_STATES
                },
                "origin_fallback_count": stratum_fallback[
                    stratum.stratum_id
                ],
                "false_safe": _rate_interval(
                    stratum_false_safe[stratum.stratum_id],
                    total_simulations,
                    plan.monte_carlo_confidence,
                ),
                "false_unsafe": _rate_interval(
                    stratum_false_unsafe[stratum.stratum_id],
                    total_simulations,
                    plan.monte_carlo_confidence,
                ),
                "success_difference_coverage": _rate_interval(
                    stratum_coverage[stratum.stratum_id]["success"],
                    total_simulations,
                    plan.monte_carlo_confidence,
                ),
                "cost_saving_coverage": _rate_interval(
                    stratum_coverage[stratum.stratum_id]["cost"],
                    total_simulations,
                    plan.monte_carlo_confidence,
                ),
            })
            summary_rows.append({
                "calibration_id": plan.calibration_id,
                "bound_method": bound_method,
                "scenario_id": scenario.scenario_id,
                "stratum_id": stratum.stratum_id,
                "simulations": total_simulations,
                "seed": plan.seed,
                "workload_count": stratum.workload_count,
                "repetition_pair_count": (
                    stratum.workload_count * plan.repetitions
                ),
                "true_success_difference": (
                    stratum.true_success_difference
                ),
                "true_cost_saving": stratum.true_cost_saving,
                "truth_violates_a_gate": violates,
                "family_size": family_size,
                "adjusted_alpha": adjusted_alpha,
                "safe_to_commit_frequency": (
                    counts["SAFE_TO_COMMIT"] / total_simulations
                ),
                "unsafe_frequency": counts["UNSAFE"] / total_simulations,
                "insufficient_evidence_frequency": (
                    counts["INSUFFICIENT_EVIDENCE"] / total_simulations
                ),
                "false_safe_rate": (
                    stratum_false_safe[stratum.stratum_id]
                    / total_simulations
                ),
                "false_unsafe_rate": (
                    stratum_false_unsafe[stratum.stratum_id]
                    / total_simulations
                ),
                "success_coverage_rate": (
                    stratum_coverage[stratum.stratum_id]["success"]
                    / total_simulations
                ),
                "cost_coverage_rate": (
                    stratum_coverage[stratum.stratum_id]["cost"]
                    / total_simulations
                ),
            })
        scenario_reports.append({
            "scenario_id": scenario.scenario_id,
            "description": scenario.description,
            "stratum_count": len(scenario.strata),
            "family_size": family_size,
            "adjusted_alpha": adjusted_alpha,
            "unadjusted_confidence_level": 1.0 - plan.alpha,
            "adjusted_confidence_level": 1.0 - adjusted_alpha,
            "family_components": [
                {
                    "stratum_id": stratum.stratum_id,
                    "gate_id": gate_id,
                    "side": side,
                }
                for stratum in scenario.strata
                for gate_id in GATE_IDS
                for side in GATE_SIDES
            ],
            "family_wise_false_safe": _rate_interval(
                family_false_safe,
                total_simulations,
                plan.monte_carlo_confidence,
            ),
            "simultaneous_coverage": _rate_interval(
                family_simultaneous_coverage,
                total_simulations,
                plan.monte_carlo_confidence,
            ),
            "strata": stratum_reports,
        })

    return {
        "schema_version": CALIBRATION_REPORT_SCHEMA_VERSION,
        "calibration_id": plan.calibration_id,
        "status": "COMPLETE",
        "posthoc": True,
        "eligible_for_scientific_claims": False,
        "synthetic": True,
        "bound_method": bound_method,
        "is_negative_control": bound_method == CONTROL_BOUND_METHOD,
        "seed": plan.seed,
        "simulations_per_scenario": total_simulations,
        "repetitions_per_workload": plan.repetitions,
        "alpha": plan.alpha,
        "unadjusted_confidence_level": 1.0 - plan.alpha,
        "family_adjustment": "bonferroni",
        "thresholds": {
            "delta_success_margin": plan.delta_success_margin,
            "minimum_cost_saving": plan.minimum_cost_saving,
        },
        "tolerances": {
            "monte_carlo_confidence": plan.monte_carlo_confidence,
            "false_safe_rate_tolerance": plan.false_safe_rate_tolerance,
            "coverage_tolerance": plan.coverage_tolerance,
            "control_minimum_false_safe_rate": (
                plan.control_minimum_false_safe_rate
            ),
        },
        "overall": {
            "family_wise_false_safe": _rate_interval(
                overall_false_safe,
                overall_simulations,
                plan.monte_carlo_confidence,
            ),
            "simultaneous_coverage": _rate_interval(
                overall_coverage,
                overall_simulations,
                plan.monte_carlo_confidence,
            ),
            "scenario_families": len(plan.scenarios),
            "total_family_simulations": overall_simulations,
        },
        "scenarios": scenario_reports,
        "summary_rows": summary_rows,
        "limitations": [
            "This calibration is over a synthetic data-generating process, "
            "not over real workloads; it validates the decision rule, not "
            "the frozen Oracle's conclusions.",
            "Coverage is measured against the generator's declared "
            "superpopulation mean, which exists by construction here and "
            "does NOT exist for the frozen Oracle without a declared "
            "workload sampling mechanism.",
        ],
    }


def calibrate_awm_certificate(
    calibration_config: CalibrationConfig | str | Path,
    *,
    output_dir: str | Path,
    include_negative_control: bool = True,
    simulations: int | None = None,
) -> dict[str, Any]:
    """Run the calibration and its negative control, and write the outputs."""
    plan = (
        load_calibration_config(calibration_config)
        if isinstance(calibration_config, (str, Path))
        else calibration_config
    )
    output = Path(output_dir).resolve()
    if output.exists() and any(output.iterdir()):
        raise AWMConfigError(
            f"calibration output directory is not empty: {output}"
        )

    reports = [run_certificate_monte_carlo(
        plan,
        bound_method="workload-cluster-one-sided-bounded-mean-kl",
        simulations=simulations,
    )]
    if include_negative_control:
        reports.append(run_certificate_monte_carlo(
            plan,
            bound_method=CONTROL_BOUND_METHOD,
            simulations=simulations,
        ))

    certificate_report = reports[0]
    control_report = reports[1] if include_negative_control else None
    checks = _calibration_checks(plan, certificate_report, control_report)

    output.mkdir(parents=True, exist_ok=True)
    summary_rows = [
        row
        for report in reports
        for row in report["summary_rows"]
    ]
    summary_path = output / "calibration_summary.csv"
    report_path = output / "calibration_report.json"
    manifest_path = output / "calibration_manifest.json"
    _write_csv(summary_rows, summary_path)
    payload = {
        "schema_version": CALIBRATION_REPORT_SCHEMA_VERSION,
        "calibration_id": plan.calibration_id,
        "status": "COMPLETE",
        "posthoc": True,
        "eligible_for_scientific_claims": False,
        "synthetic": True,
        "checks": checks,
        "certificate": {
            key: value
            for key, value in certificate_report.items()
            if key != "summary_rows"
        },
        "negative_control": (
            None
            if control_report is None
            else {
                key: value
                for key, value in control_report.items()
                if key != "summary_rows"
            }
        ),
    }
    _write_json(payload, report_path)
    digest = sha256()
    for path in (summary_path, report_path):
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(sha256(path.read_bytes()).digest())
    manifest = {
        "schema_version": CALIBRATION_REPORT_SCHEMA_VERSION,
        "calibration_id": plan.calibration_id,
        "status": "COMPLETE",
        "posthoc": True,
        "eligible_for_scientific_claims": False,
        "synthetic": True,
        "deployment_mutations_performed": False,
        "secrets_recorded": False,
        "calibration_config_path": str(plan.source_path),
        "calibration_config_sha256": plan.source_sha256,
        "seed": plan.seed,
        "simulations_per_scenario": (
            simulations or plan.simulations
        ),
        "scenario_count": len(plan.scenarios),
        "summary_path": str(summary_path),
        "report_path": str(report_path),
        "calibration_snapshot_sha256": digest.hexdigest(),
        "all_checks_passed": all(
            check["passed"] for check in checks
        ),
        "failed_checks": [
            check["check_id"] for check in checks if not check["passed"]
        ],
    }
    _write_json(manifest, manifest_path)
    return {**manifest, "manifest_path": str(manifest_path)}


def _calibration_checks(
    plan: CalibrationConfig,
    certificate: Mapping[str, Any],
    control: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    """Evaluate the preregistered numerical tolerances."""
    checks: list[dict[str, Any]] = []
    false_safe = certificate["overall"]["family_wise_false_safe"]
    limit = plan.alpha + plan.false_safe_rate_tolerance
    checks.append({
        "check_id": "family_wise_false_safe_rate_within_alpha",
        "observed": false_safe["rate"],
        "observed_upper_bound": false_safe["upper"],
        "limit": limit,
        "comparison": "observed_upper_bound <= alpha + tolerance",
        "passed": false_safe["upper"] <= limit,
    })
    coverage = certificate["overall"]["simultaneous_coverage"]
    floor = 1.0 - plan.alpha - plan.coverage_tolerance
    checks.append({
        "check_id": "simultaneous_coverage_at_least_nominal",
        "observed": coverage["rate"],
        "observed_lower_bound": coverage["lower"],
        "limit": floor,
        "comparison": "observed_lower_bound >= 1 - alpha - tolerance",
        "passed": coverage["lower"] >= floor,
    })
    per_scenario_failures = [
        scenario["scenario_id"]
        for scenario in certificate["scenarios"]
        if scenario["family_wise_false_safe"]["upper"] > limit
    ]
    checks.append({
        "check_id": "every_scenario_family_within_alpha",
        "observed": len(per_scenario_failures),
        "failing_scenarios": per_scenario_failures,
        "limit": 0,
        "comparison": "no scenario family exceeds alpha + tolerance",
        "passed": not per_scenario_failures,
    })
    if control is not None:
        control_rate = control["overall"]["family_wise_false_safe"]
        worst = max(
            control["scenarios"],
            key=lambda scenario: (
                scenario["family_wise_false_safe"]["lower"]
            ),
        )
        worst_rate = worst["family_wise_false_safe"]
        checks.append({
            "check_id": "negative_control_is_detected_as_miscalibrated",
            "worst_scenario_id": worst["scenario_id"],
            "observed": worst_rate["rate"],
            "observed_lower_bound": worst_rate["lower"],
            "limit": plan.control_minimum_false_safe_rate,
            "comparison": (
                "an anti-conservative bound must produce a materially "
                "higher false-safe rate, proving the harness can detect "
                "miscalibration"
            ),
            "passed": (
                worst_rate["lower"]
                >= plan.control_minimum_false_safe_rate
            ),
        })
        checks.append({
            "check_id": "certificate_strictly_safer_than_control",
            "observed": certificate["overall"][
                "family_wise_false_safe"
            ]["rate"],
            "control": control_rate["rate"],
            "comparison": "certificate false-safe rate < control rate",
            "passed": (
                certificate["overall"]["family_wise_false_safe"]["rate"]
                < control_rate["rate"]
            ),
        })
    return checks
