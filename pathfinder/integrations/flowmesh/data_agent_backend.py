from __future__ import annotations

import uuid

from ...data_agent_client import (
    DataAgentAccessRequest,
    DataAgentAccessTelemetry,
    DataAgentClientProtocol,
    DataAgentFetchedArtifact,
    DataAgentProtocolError,
    validated_timing_seconds,
)
from ...models import SystemConfig
from .gateway import BackendAccessResult, GatewayAccessEvent, GatewaySession


class RemoteDataAgentBackend:
    """Adapts the generic DataAgentClient to the FlowMesh access gateway."""

    def __init__(
        self,
        client: DataAgentClientProtocol,
        *,
        telemetry_quiescence_timeout_seconds: float = 5.0,
    ):
        self.client = client
        # Same rule as the client's own bound, applied here so a nan or inf
        # cannot reach a reconciliation wait through the backend instead.
        self.telemetry_quiescence_timeout_seconds = validated_timing_seconds(
            telemetry_quiescence_timeout_seconds,
            "telemetry_quiescence_timeout_seconds",
            allow_zero=True,
        )

    def access(
        self,
        *,
        config: SystemConfig,
        session: GatewaySession,
        representation_id: str,
        event_index: int,
    ) -> BackendAccessResult:
        request = self._access_request(
            config=config,
            session=session,
            representation_id=representation_id,
            event_index=event_index,
        )
        result = self.client.access(request)
        if result.payload.kind == "artifact_uri":
            # The expiring signed URL is a bearer capability. Keep it inside
            # the trusted client/backend boundary and expose only metadata;
            # the gateway adds a session-bound opaque handle after the access
            # event has been persisted.
            content = (
                "Artifact access is ready. Call fetch_artifact with the "
                "artifact_handle returned by the gateway."
            )
            payload = {
                "kind": "artifact_handle",
                "media_type": result.payload.media_type,
            }
            if result.payload.sha256 is not None:
                payload["sha256"] = result.payload.sha256
        elif result.payload.kind == "inline_text":
            content = result.payload.to_agent_content()
            payload = result.payload.to_dict()
        else:
            raise DataAgentProtocolError(
                "FlowMesh integration does not support Data Agent payload "
                f"kind: {result.payload.kind}"
            )
        return BackendAccessResult(
            content=content,
            felt_latency_ms=result.observed_latency_ms,
            realized_cost=result.realized_cost,
            bytes_read=result.bytes_read,
            location=result.location,
            service_latency_ms=result.service_latency_ms,
            service_timings_ms=dict(result.timings_ms),
            payload=payload,
            content_sha256=result.payload.sha256,
            data_agent_access_id=result.access_id,
            object_catalog_version=result.object_catalog_version,
        )

    def fetch_artifact(
        self,
        *,
        config: SystemConfig,
        session: GatewaySession,
        event: GatewayAccessEvent,
    ) -> DataAgentFetchedArtifact:
        if event.data_agent_access_id is None:
            raise ValueError("gateway event has no Data Agent access ID")
        request = self._access_request(
            config=config,
            session=session,
            representation_id=event.representation_id,
            event_index=event.event_index,
        )
        if request.access_id != event.data_agent_access_id:
            raise ValueError(
                "gateway event Data Agent access ID does not match its "
                "deterministic request"
            )
        return self.client.fetch_artifact(request)

    def _access_request(
        self,
        *,
        config: SystemConfig,
        session: GatewaySession,
        representation_id: str,
        event_index: int,
    ) -> DataAgentAccessRequest:
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
        return DataAgentAccessRequest(
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
