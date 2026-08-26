"""Endpoint-routed Data Agent access for a distributed pilot.

Wraps the existing ``RemoteDataAgentBackend`` with an endpoint registry so
that every access resolves ``(design_id, representation_id)`` to exactly one
declared Data Agent, is served by that endpoint's own client, and records
which node served it.

Two invariants carry the weight:

* **Artifact handles are endpoint-scoped.** A handle issued while reading
  from endpoint A is redeemed only against endpoint A. Redeeming it elsewhere
  is a delivery/protocol fault, not an Agent policy choice, so it must never
  depress the design's task-success metric.
* **There is no fallback tier.** An unroutable representation or an
  unreachable endpoint raises. Serving origin bytes from the local node would
  invert the exact contrast the pilot exists to measure.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from ..data_agent_client import (
    DataAgentClientProtocol,
    DataAgentFetchedArtifact,
    DataAgentUnavailableError,
)
from ..integrations.flowmesh.data_agent_backend import RemoteDataAgentBackend
from ..integrations.flowmesh.gateway import (
    BackendAccessResult,
    GatewayAccessEvent,
    GatewayError,
    GatewaySession,
)
from ..models import SystemConfig
from .registry import (
    AccessRoute,
    CrossEndpointHandleError,
    EndpointRegistry,
    EndpointRegistryError,
    EndpointUnreachableError,
)


class EndpointRoutingError(GatewayError):
    """Raised when an access cannot be routed to exactly one endpoint."""

    failure_class = "policy"


class CrossEndpointArtifactError(GatewayError):
    """Raised when an artifact is redeemed at the wrong endpoint.

    Classified as a delivery/protocol fault. The Agent asked for bytes it was
    legitimately offered; the system failed to hand them over from the node
    that issued the handle.
    """

    failure_class = "artifact_delivery_failure"
    outcome_type = "artifact_delivery_failure"


@dataclass(frozen=True)
class EndpointClient:
    """One declared endpoint bound to its own client and backend."""

    endpoint_id: str
    node_id: str
    location: str
    client: DataAgentClientProtocol
    backend: RemoteDataAgentBackend


class RoutedDataAgentBackend:
    """A ``RepresentationBackend`` that routes across declared endpoints.

    One client is maintained per declared endpoint for the life of the
    backend. Credentials are resolved from the environment when the client is
    built and are never stored on the registry, the route, or any record.
    """

    def __init__(
        self,
        registry: EndpointRegistry,
        clients: Mapping[str, DataAgentClientProtocol],
        *,
        telemetry_quiescence_timeout_seconds: float = 5.0,
    ) -> None:
        missing = set(registry.endpoint_ids) - set(clients)
        if missing:
            raise EndpointRegistryError(
                "no client was supplied for declared endpoint(s): "
                + ", ".join(sorted(missing))
            )
        unknown = set(clients) - set(registry.endpoint_ids)
        if unknown:
            raise EndpointRegistryError(
                "client supplied for undeclared endpoint(s): "
                + ", ".join(sorted(unknown))
            )
        self.registry = registry
        self._endpoints: dict[str, EndpointClient] = {}
        for endpoint_id, client in clients.items():
            endpoint = registry.endpoint(endpoint_id)
            self._endpoints[endpoint_id] = EndpointClient(
                endpoint_id=endpoint_id,
                node_id=endpoint.node_id,
                location=endpoint.location,
                client=client,
                backend=RemoteDataAgentBackend(
                    client,
                    telemetry_quiescence_timeout_seconds=(
                        telemetry_quiescence_timeout_seconds
                    ),
                ),
            )

    @property
    def endpoint_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._endpoints))

    def endpoint_for(self, endpoint_id: str) -> EndpointClient:
        try:
            return self._endpoints[endpoint_id]
        except KeyError:
            raise EndpointRoutingError(
                f"no client for endpoint: {endpoint_id}"
            ) from None

    def resolve(
        self,
        *,
        design_id: str,
        representation_id: str,
    ) -> tuple[AccessRoute, EndpointClient]:
        try:
            route = self.registry.route(
                design_id=design_id,
                representation_id=representation_id,
            )
        except EndpointRegistryError as exc:
            raise EndpointRoutingError(str(exc)) from exc
        return route, self.endpoint_for(route.endpoint_id)

    def access(
        self,
        *,
        config: SystemConfig,
        session: GatewaySession,
        representation_id: str,
        event_index: int,
    ) -> BackendAccessResult:
        route, endpoint = self.resolve(
            design_id=session.design_id,
            representation_id=representation_id,
        )
        try:
            result = endpoint.backend.access(
                config=config,
                session=session,
                representation_id=representation_id,
                event_index=event_index,
            )
        except DataAgentUnavailableError as exc:
            # Infrastructure, not policy: the node did not answer. Never
            # retry against a different endpoint.
            raise EndpointUnreachableError(
                endpoint.endpoint_id,
                str(exc),
                secret_environment_names=(
                    self.registry.secret_environment_names
                ),
            ) from exc
        return BackendAccessResult(
            content=result.content,
            felt_latency_ms=result.felt_latency_ms,
            realized_cost=result.realized_cost,
            bytes_read=result.bytes_read,
            location=result.location,
            service_latency_ms=result.service_latency_ms,
            service_timings_ms=result.service_timings_ms,
            payload=result.payload,
            content_sha256=result.content_sha256,
            data_agent_access_id=result.data_agent_access_id,
            object_catalog_version=result.object_catalog_version,
            endpoint_id=route.endpoint_id,
            source_node_id=route.source_node_id,
            source_location=route.source_location,
            destination_execution_node_id=(
                route.destination_execution_node_id
            ),
        )

    def fetch_artifact(
        self,
        *,
        config: SystemConfig,
        session: GatewaySession,
        event: GatewayAccessEvent,
    ) -> DataAgentFetchedArtifact:
        """Redeem an artifact only at the endpoint that issued its handle."""
        route, endpoint = self.resolve(
            design_id=session.design_id,
            representation_id=event.representation_id,
        )
        issuing_endpoint_id = event.endpoint_id
        if issuing_endpoint_id is None:
            raise CrossEndpointArtifactError(
                "the access event carries no issuing endpoint, so its "
                "artifact handle cannot be redeemed safely"
            )
        if issuing_endpoint_id != route.endpoint_id:
            raise CrossEndpointArtifactError(
                f"artifact handle was issued by endpoint "
                f"'{issuing_endpoint_id}' but the current route resolves to "
                f"'{route.endpoint_id}'; refusing a cross-endpoint redemption"
            )
        try:
            return endpoint.backend.fetch_artifact(
                config=config,
                session=session,
                event=event,
            )
        except DataAgentUnavailableError as exc:
            raise EndpointUnreachableError(
                endpoint.endpoint_id,
                str(exc),
                secret_environment_names=(
                    self.registry.secret_environment_names
                ),
            ) from exc

    #: Tells the gateway to pass the issuing endpoint to telemetry reads.
    endpoint_aware_telemetry = True

    def get_access_telemetry(
        self,
        access_id: str,
        *,
        endpoint_id: str | None = None,
    ) -> Any:
        """Reconcile telemetry from the endpoint that issued the access.

        Only the issuing endpoint holds the transfer record. Asking it
        directly is both cheaper and correct: two endpoints may legitimately
        mint the same deterministic access id, and polling would then return
        whichever answered first.

        ``endpoint_id`` is absent only on legacy single-endpoint records. In
        that case the sole declared endpoint is used; with several declared
        endpoints the read fails closed rather than guessing.
        """
        if endpoint_id is None:
            if len(self._endpoints) != 1:
                raise EndpointRoutingError(
                    "an access event carries no endpoint identity but this "
                    f"registry declares {len(self._endpoints)} endpoints; "
                    "refusing to guess which one served it"
                )
            endpoint_id = next(iter(self._endpoints))
        endpoint = self.endpoint_for(endpoint_id)
        try:
            return endpoint.backend.get_access_telemetry(access_id)
        except DataAgentUnavailableError as exc:
            raise EndpointUnreachableError(
                endpoint.endpoint_id,
                str(exc),
                secret_environment_names=(
                    self.registry.secret_environment_names
                ),
            ) from exc


def routed_backend_for(
    registry: EndpointRegistry,
    *,
    client_factory: Any,
    environment: Mapping[str, str] | None = None,
    telemetry_quiescence_timeout_seconds: float = 5.0,
) -> RoutedDataAgentBackend:
    """Build one client per declared endpoint from the environment.

    ``client_factory`` receives the endpoint's validated
    ``DataAgentClientSettings``; tests pass an in-process fake, deployments
    pass ``HttpDataAgentClient``.
    """
    clients = {
        endpoint_id: client_factory(
            registry.endpoint(endpoint_id).client_settings(environment)
        )
        for endpoint_id in registry.endpoint_ids
    }
    return RoutedDataAgentBackend(
        registry,
        clients,
        telemetry_quiescence_timeout_seconds=(
            telemetry_quiescence_timeout_seconds
        ),
    )


def build_routed_gateway_backend(
    endpoint_registry_path: str | Path,
    *,
    telemetry_quiescence_timeout_seconds: float = 15.0,
    environment: Mapping[str, str] | None = None,
) -> tuple[RoutedDataAgentBackend, Any]:
    """Build a routed backend plus its registry from a registry document.

    One :class:`HttpDataAgentClient` is created per declared endpoint using
    that endpoint's own URL and token environment variables. Returned
    alongside the registry so a caller can record the digest that the
    distributed runner must agree with.
    """
    from ..data_agent_client import HttpDataAgentClient
    from .registry import load_endpoint_registry

    registry = load_endpoint_registry(endpoint_registry_path)
    backend = routed_backend_for(
        registry,
        client_factory=HttpDataAgentClient,
        environment=environment,
        telemetry_quiescence_timeout_seconds=(
            telemetry_quiescence_timeout_seconds
        ),
    )
    return backend, registry


def close_routed_backend(backend: RoutedDataAgentBackend) -> None:
    """Close every per-endpoint client deterministically."""
    for endpoint_id in backend.endpoint_ids:
        client = backend.endpoint_for(endpoint_id).client
        closer = getattr(client, "close", None)
        if callable(closer):
            closer()
