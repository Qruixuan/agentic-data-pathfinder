from __future__ import annotations

import uuid

from ...data_agent_client import (
    DataAgentAccessRequest,
    DataAgentAccessTelemetry,
    DataAgentClientProtocol,
)
from ...models import SystemConfig
from .gateway import BackendAccessResult, GatewaySession


class RemoteDataAgentBackend:
    """Adapts the generic DataAgentClient to the FlowMesh access gateway."""

    def __init__(
        self,
        client: DataAgentClientProtocol,
        *,
        telemetry_quiescence_timeout_seconds: float = 5.0,
    ):
        if telemetry_quiescence_timeout_seconds < 0:
            raise ValueError(
                "telemetry_quiescence_timeout_seconds cannot be negative"
            )
        self.client = client
        self.telemetry_quiescence_timeout_seconds = (
            telemetry_quiescence_timeout_seconds
        )

    def access(
        self,
        *,
        config: SystemConfig,
        session: GatewaySession,
        representation_id: str,
        event_index: int,
    ) -> BackendAccessResult:
        path = config.designs[session.design_id].paths[representation_id]
        access_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                (
                    f"pathfinder-access:{session.session_id}:"
                    f"{event_index}:{representation_id}"
                ),
            )
        )
        result = self.client.access(
            DataAgentAccessRequest(
                access_id=access_id,
                session_id=session.session_id,
                trial_id=session.trial_id,
                plan_id=session.design_id,
                plan_epoch=0,
                task_class_id=session.task_class_id,
                representation_id=representation_id,
                event_index=event_index,
                latency_multiplier=session.latency_multiplier,
                binding={
                    "location": path.location,
                    "representation_size_bytes": config.representations[
                        representation_id
                    ].size_bytes,
                },
                object_id=session.object_id,
            )
        )
        return BackendAccessResult(
            content=result.payload.to_agent_content(),
            felt_latency_ms=result.observed_latency_ms,
            realized_cost=result.realized_cost,
            bytes_read=result.bytes_read,
            location=result.location,
            payload=result.payload.to_dict(),
            content_sha256=result.payload.sha256,
            data_agent_access_id=result.access_id,
            object_catalog_version=result.object_catalog_version,
        )

    def get_access_telemetry(
        self,
        access_id: str,
    ) -> DataAgentAccessTelemetry:
        # Reconciliation runs after the workflow terminates and writes the
        # realized byte and latency figures used for analysis, so it must see
        # a final summary rather than one missing a just-finished transfer.
        # The initial access() deliberately does not wait: at that point the
        # Agent may not have started downloading at all, so there is nothing
        # to become quiescent and waiting would only add latency.
        #
        # DataAgentTelemetryQuiescenceError is intentionally not caught. A
        # session whose transfers cannot be accounted for must fail rather
        # than yield a partial research observation.
        return self.client.get_access_telemetry(
            access_id,
            wait_for_quiescence=True,
            quiescence_timeout_seconds=(
                self.telemetry_quiescence_timeout_seconds
            ),
        )
