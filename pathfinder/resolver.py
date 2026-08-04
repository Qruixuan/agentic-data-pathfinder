from __future__ import annotations

import random

from .models import (
    AccessDecision,
    AccessOffer,
    PhysicalDesign,
    QuoteProfile,
    SystemConfig,
    TaskClass,
)


class AccessResolver:
    """Builds the complete class-specific offered set for one session."""

    def __init__(self, config: SystemConfig):
        self._config = config

    def build_offers(
        self,
        design: PhysicalDesign,
        task: TaskClass,
        quote_profile: QuoteProfile,
    ) -> tuple[list[AccessOffer], list[str]]:
        offers: list[AccessOffer] = []
        unavailable: list[str] = []
        for rep_id in task.candidate_representations:
            path = design.paths.get(rep_id)
            if (
                path is None
                or not path.available
                or task.id not in path.quotes
            ):
                unavailable.append(rep_id)
                continue
            quoted_price = quote_profile.quote_for(
                task.id,
                rep_id,
                path.quotes[task.id],
            )
            offers.append(
                AccessOffer(
                    representation_id=rep_id,
                    quoted_price=quoted_price,
                    affordable=quoted_price <= task.access_budget,
                    location=path.location,
                    expected_latency_ms=path.latency_ms,
                    task_quality=self._config.representations[
                        rep_id
                    ].quality_for(task.id),
                )
            )
        return offers, unavailable


class SimulatedAgent:
    """A reproducible agent policy that reacts to quality and quoted price."""

    def choose(
        self,
        task: TaskClass,
        offers: list[AccessOffer],
        rng: random.Random,
    ) -> AccessDecision:
        scores: dict[str, float] = {
            "outside_option": task.outside_option_utility
            + rng.gauss(0.0, task.choice_noise)
        }
        for offer in offers:
            choice_noise = rng.gauss(0.0, task.choice_noise)
            if not offer.affordable:
                continue
            scores[offer.representation_id] = (
                task.quality_weight * offer.task_quality
                - task.price_weight * offer.quoted_price
                + choice_noise
            )

        selected = max(scores, key=scores.get)
        if selected == "outside_option":
            if any(not offer.affordable for offer in offers):
                reason = "outside_option_or_unaffordable"
            else:
                reason = "outside_option"
            return AccessDecision(
                selected_representation_id=None,
                reason=reason,
                scores=scores,
            )
        return AccessDecision(
            selected_representation_id=selected,
            reason="selected_highest_utility_affordable_representation",
            scores=scores,
        )
