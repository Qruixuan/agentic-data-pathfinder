from __future__ import annotations

import random
from dataclasses import asdict, dataclass
from typing import Any

from ..awm import AWMDataset, AdaptiveWorkloadModel, Interval
from .contracts import OEDConfig


OED_POLICY_KINDS = (
    "full_oed",
    "passive_awm",
    "random_reveal",
    "black_box_reveal",
)
_STRUCTURAL_CERTIFICATES = {
    "own-price-monotonicity",
    "substitution-group-monotonicity",
    "success-monotonicity",
}
_TIER_ORDER = {
    "simultaneous-canonical": 0,
    "pair-canonical": 1,
    "fallback": 2,
}


@dataclass(frozen=True)
class OEDState:
    safe_design_id: str
    observed_design_ids: tuple[str, ...]
    revealed_design_ids: tuple[str, ...]
    remaining_exploration_purse: float


@dataclass(frozen=True)
class CandidateScore:
    design_id: str
    partition: str
    observed: bool
    unresolved_price_states: tuple[str, ...]
    constraints_applied: tuple[str, ...]
    gain_interval_source: str
    commit_gain_width: float
    paired_gain_pair_count: int | None
    paired_gain_training_snapshot_sha256: str | None
    paired_gain_alpha_per_pair_look: float | None
    paired_gain_point_estimate_per_session: float | None
    paired_gain_variance_radius_per_session: float | None
    paired_gain_range_radius_per_session: float | None
    paired_gain_clipped_to_support: bool | None
    pessimistic_commit_gain: float
    optimistic_reveal_gain: float
    candidate_width: float
    incumbent_width: float
    transition_width: float
    forward_transition: Interval
    restoration_transition: Interval
    probe_window_loss: float
    reveal_excursion: Interval
    reveal_tier: str | None
    value_positive: bool
    cap_feasible: bool
    purse_feasible: bool

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        return payload


@dataclass(frozen=True)
class OEDDecision:
    iteration: int
    policy_kind: str
    safe_design_id: str
    observation_history_version: str
    certifiable_candidates: tuple[str, ...]
    probe_candidates: tuple[str, ...]
    other_candidates: tuple[str, ...]
    candidate_scores: tuple[CandidateScore, ...]
    selected_action: str
    selected_design_id: str | None
    reveal_selection_tier: str | None
    remaining_exploration_purse: float
    per_excursion_cap: float
    stability_radius_delta: float
    stopping_reason: str | None
    action_reason: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["candidate_scores"] = [
            score.to_dict() for score in self.candidate_scores
        ]
        return payload


def _cost_interval(value: float, radius: float) -> Interval:
    return Interval(
        max(0.0, value * (1.0 - radius)),
        value * (1.0 + radius),
    )


def _unresolved_price_states(
    dataset: AWMDataset,
    safe_design_id: str,
    candidate_design_id: str,
) -> tuple[str, ...]:
    unresolved: list[str] = []
    for representation_id in dataset.representation_ids:
        if not dataset.affordable(candidate_design_id, representation_id):
            continue
        safe_affordable = dataset.affordable(
            safe_design_id,
            representation_id,
        )
        candidate_quote = dataset.quote(candidate_design_id, representation_id)
        safe_quote = dataset.quote(safe_design_id, representation_id)
        if not safe_affordable or candidate_quote < safe_quote - 1e-12:
            unresolved.append(
                f"{dataset.task_class.id}|{representation_id}|{candidate_quote:g}"
            )
    return tuple(unresolved)


class OEDController:
    """Pure finite-design Commit/Reveal/Hold/Stop decision logic."""

    def __init__(self, config: OEDConfig) -> None:
        self.config = config

    def _validate_domain(self, dataset: AWMDataset, state: OEDState) -> None:
        designs = set(dataset.design_ids)
        declared = set(self.config.reveal_candidates).union(
            self.config.other_design_ids
        )
        unknown = declared - designs
        if unknown:
            raise ValueError(
                "OED configuration references designs absent from the "
                "Oracle: " + ", ".join(sorted(unknown))
            )
        if state.safe_design_id not in designs:
            raise ValueError("safe design is absent from the Oracle domain")

    def _partition(
        self,
        dataset: AWMDataset,
        model: AdaptiveWorkloadModel,
        state: OEDState,
    ) -> dict[str, tuple[str, ...]]:
        partitions: dict[str, list[str]] = {
            "G_cert": [],
            "G_probe": [],
            "G_other": [],
        }
        observed = set(state.observed_design_ids)
        for design_id in dataset.design_ids:
            if design_id == state.safe_design_id:
                continue
            constraints = set(model.bounds[design_id].constraints_applied)
            structurally_certified = bool(
                constraints.intersection(_STRUCTURAL_CERTIFICATES)
            )
            if design_id in observed or structurally_certified:
                partitions["G_cert"].append(design_id)
            elif (
                design_id in self.config.reveal_candidates
                and _unresolved_price_states(
                    dataset,
                    state.safe_design_id,
                    design_id,
                )
            ):
                partitions["G_probe"].append(design_id)
            else:
                partitions["G_other"].append(design_id)
        return {
            name: tuple(sorted(values))
            for name, values in partitions.items()
        }

    def _score(
        self,
        dataset: AWMDataset,
        model: AdaptiveWorkloadModel,
        state: OEDState,
        design_id: str,
        partition: str,
    ) -> CandidateScore:
        bounds = model.bounds[design_id]
        incumbent = model.bounds[state.safe_design_id]
        radius = dataset.model_config.transition_relative_radius
        restoration = _cost_interval(
            dataset.restoration_transition_costs[design_id],
            radius,
        )
        candidate_config = self.config.reveal_candidates.get(design_id)
        probe_loss = (
            candidate_config.probe_window_loss
            if candidate_config is not None
            else 0.0
        )
        reveal = Interval(
            bounds.transition_cost.lower + restoration.lower + probe_loss,
            bounds.transition_cost.upper + restoration.upper + probe_loss,
        )
        commit_gain = model.gain_interval(
            state.safe_design_id,
            design_id,
        )
        paired_certificate = model.paired_gain_certificate(
            state.safe_design_id,
            design_id,
        )
        optimistic = (
            commit_gain.upper
            - restoration.lower
            - probe_loss
        )
        return CandidateScore(
            design_id=design_id,
            partition=partition,
            observed=bounds.observed,
            unresolved_price_states=_unresolved_price_states(
                dataset,
                state.safe_design_id,
                design_id,
            ),
            constraints_applied=bounds.constraints_applied,
            gain_interval_source=model.gain_interval_source(
                state.safe_design_id, design_id
            ),
            commit_gain_width=commit_gain.width,
            paired_gain_pair_count=(
                paired_certificate.pair_count
                if paired_certificate is not None
                else None
            ),
            paired_gain_training_snapshot_sha256=(
                paired_certificate.training_snapshot_sha256
                if paired_certificate is not None
                else None
            ),
            paired_gain_alpha_per_pair_look=(
                paired_certificate.alpha_per_pair_look
                if paired_certificate is not None
                else None
            ),
            paired_gain_point_estimate_per_session=(
                paired_certificate.point_estimate_per_session
                if paired_certificate is not None
                else None
            ),
            paired_gain_variance_radius_per_session=(
                paired_certificate.variance_radius_per_session
                if paired_certificate is not None
                else None
            ),
            paired_gain_range_radius_per_session=(
                paired_certificate.range_radius_per_session
                if paired_certificate is not None
                else None
            ),
            paired_gain_clipped_to_support=(
                paired_certificate.clipped_to_support
                if paired_certificate is not None
                else None
            ),
            pessimistic_commit_gain=commit_gain.lower,
            optimistic_reveal_gain=optimistic,
            candidate_width=bounds.phi.width,
            incumbent_width=incumbent.phi.width,
            transition_width=bounds.transition_cost.width,
            forward_transition=bounds.transition_cost,
            restoration_transition=restoration,
            probe_window_loss=probe_loss,
            reveal_excursion=reveal,
            reveal_tier=(
                candidate_config.reveal_tier
                if candidate_config is not None
                else None
            ),
            value_positive=optimistic > self.config.reveal_margin,
            cap_feasible=reveal.upper <= self.config.per_excursion_cap,
            purse_feasible=(
                reveal.upper <= state.remaining_exploration_purse
            ),
        )

    def decide(
        self,
        dataset: AWMDataset,
        model: AdaptiveWorkloadModel,
        state: OEDState,
        *,
        iteration: int,
        policy_kind: str,
    ) -> OEDDecision:
        if policy_kind not in OED_POLICY_KINDS:
            raise ValueError(f"unknown OED policy kind: {policy_kind}")
        self._validate_domain(dataset, state)
        partitions = self._partition(dataset, model, state)
        partition_by_design = {
            design_id: partition
            for partition, values in partitions.items()
            for design_id in values
        }
        scores = tuple(
            self._score(
                dataset,
                model,
                state,
                design_id,
                partition_by_design[design_id],
            )
            for design_id in sorted(partition_by_design)
        )
        by_design = {score.design_id: score for score in scores}
        commits = [
            by_design[design_id]
            for design_id in partitions["G_cert"]
            if by_design[design_id].pessimistic_commit_gain
            > self.config.commit_margin
        ]
        if commits:
            selected = max(
                commits,
                key=lambda score: (
                    score.pessimistic_commit_gain,
                    score.design_id,
                ),
            )
            return self._decision(
                iteration,
                policy_kind,
                state,
                partitions,
                scores,
                action="COMMIT",
                selected=selected,
                reason="certified_pessimistic_gain_clears_margin",
            )

        positive_probes = [
            by_design[design_id]
            for design_id in partitions["G_probe"]
            if by_design[design_id].value_positive
        ]
        feasible = [
            score
            for score in positive_probes
            if score.cap_feasible and score.purse_feasible
        ]
        if policy_kind == "passive_awm" and positive_probes:
            return self._decision(
                iteration,
                policy_kind,
                state,
                partitions,
                scores,
                action="STOP",
                selected=None,
                reason="baseline_disables_reveal",
                stopping_reason="policy_limited_stop",
            )
        if feasible:
            if policy_kind == "random_reveal":
                generator = random.Random(
                    self.config.random_seed + iteration
                )
                selected = generator.choice(sorted(
                    feasible,
                    key=lambda score: score.design_id,
                ))
            else:
                selected = min(
                    feasible,
                    key=lambda score: (
                        _TIER_ORDER[score.reveal_tier or "fallback"],
                        -score.optimistic_reveal_gain
                        / max(score.reveal_excursion.upper, 1e-12),
                        score.design_id,
                    ),
                )
            return self._decision(
                iteration,
                policy_kind,
                state,
                partitions,
                scores,
                action="REVEAL",
                selected=selected,
                reason="optimistic_uncensoring_excursion_is_budget_feasible",
            )
        if positive_probes:
            return self._decision(
                iteration,
                policy_kind,
                state,
                partitions,
                scores,
                action="STOP",
                selected=None,
                reason="valuable_reveal_exceeds_budget_or_excursion_cap",
                stopping_reason="budget_limited_stop",
            )
        if partitions["G_other"]:
            return self._decision(
                iteration,
                policy_kind,
                state,
                partitions,
                scores,
                action="HOLD",
                selected=None,
                reason="value_may_remain_outside_current_certificates",
                stopping_reason="structural_hold",
            )
        return self._decision(
            iteration,
            policy_kind,
            state,
            partitions,
            scores,
            action="STOP",
            selected=None,
            reason="no_commit_or_reveal_candidate_clears_value_test",
            stopping_reason="certificate_limited_stop",
        )

    def _decision(
        self,
        iteration: int,
        policy_kind: str,
        state: OEDState,
        partitions: dict[str, tuple[str, ...]],
        scores: tuple[CandidateScore, ...],
        *,
        action: str,
        selected: CandidateScore | None,
        reason: str,
        stopping_reason: str | None = None,
    ) -> OEDDecision:
        history_version = "+".join(sorted(state.observed_design_ids))
        certifiable = set(partitions["G_cert"])
        delta = max(
            (
                score.commit_gain_width
                for score in scores
                if score.design_id in certifiable
            ),
            default=0.0,
        )
        return OEDDecision(
            iteration=iteration,
            policy_kind=policy_kind,
            safe_design_id=state.safe_design_id,
            observation_history_version=history_version,
            certifiable_candidates=partitions["G_cert"],
            probe_candidates=partitions["G_probe"],
            other_candidates=partitions["G_other"],
            candidate_scores=scores,
            selected_action=action,
            selected_design_id=(
                selected.design_id if selected is not None else None
            ),
            reveal_selection_tier=(
                selected.reveal_tier
                if selected is not None and action == "REVEAL"
                else None
            ),
            remaining_exploration_purse=(
                state.remaining_exploration_purse
            ),
            per_excursion_cap=self.config.per_excursion_cap,
            stability_radius_delta=delta,
            stopping_reason=stopping_reason,
            action_reason=reason,
        )
