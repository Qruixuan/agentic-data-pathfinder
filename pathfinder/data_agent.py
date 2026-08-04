from __future__ import annotations

import random

from .models import AccessExecution, PhysicalDesign, Representation


class LocalDataAgent:
    """Emulates a physical data path behind a replaceable agent interface."""

    def serve(
        self,
        design: PhysicalDesign,
        representation: Representation,
        latency_multiplier: float,
        rng: random.Random,
    ) -> AccessExecution:
        path = design.paths[representation.id]
        base_latency = max(
            0.0,
            rng.gauss(path.latency_ms, path.latency_jitter_ms),
        )
        felt_latency = base_latency * latency_multiplier
        realized_cost = max(
            0.0,
            path.realized_cost * (1.0 + rng.gauss(0.0, 0.02)),
        )
        return AccessExecution(
            representation_id=representation.id,
            location=path.location,
            felt_latency_ms=felt_latency,
            realized_cost=realized_cost,
            bytes_read=representation.size_bytes,
        )
