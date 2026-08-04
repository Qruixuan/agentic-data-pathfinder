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

    def __init__(self, client: DataAgentClientProtocol):
        self.client = client

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
        return self.client.get_access_telemetry(access_id)
