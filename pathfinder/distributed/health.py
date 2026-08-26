"""Read-only HTTP health probe for declared Data Agent endpoints.

Issues one bounded ``GET /healthz`` per endpoint and checks the answer
against what the registry declares. Nothing here mutates state, follows a
redirect, or falls back to another endpoint: a redirect is refused because it
can silently move a health check to a host the registry never declared.

Every exception detail is passed through dynamic secret redaction using the
registry's own declared token variables before it can reach a report.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from ..data_agent_client import DATA_AGENT_API_VERSION
from ..integrations.flowmesh.redaction import redact_secrets
from .registry import EndpointRegistry, EndpointUnreachableError


HEALTH_PROBE_SCHEMA_VERSION = "pathfinder.data-agent-health/v1alpha1"
MAXIMUM_HEALTH_RESPONSE_BYTES = 64 * 1024


class _NoRedirect:
    """Sentinel documenting that redirects are deliberately unsupported."""


@dataclass(frozen=True)
class EndpointHealth:
    """One endpoint's answer, already compared against the registry."""

    endpoint_id: str
    healthy: bool
    authenticated: bool
    status_code: int | None
    api_version: str | None
    node_id: str | None
    representations: tuple[str, ...]
    object_catalog_version: str | None
    detail: str

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "endpoint_id": self.endpoint_id,
            "healthy": self.healthy,
            "authenticated": self.authenticated,
            "status_code": self.status_code,
            "api_version": self.api_version,
            "node_id": self.node_id,
            "representations": list(self.representations),
            "object_catalog_version": self.object_catalog_version,
            "detail": self.detail,
            "credentials_recorded": False,
        }


class HttpDataAgentHealthProbe:
    """Probe declared endpoints over HTTP, read-only and without fallback."""

    def __init__(
        self,
        registry: EndpointRegistry,
        *,
        environment: Mapping[str, str] | None = None,
        timeout_seconds: float | None = None,
        opener: Any = None,
    ) -> None:
        self.registry = registry
        self.environment = environment
        self.timeout_seconds = timeout_seconds
        # Injected for tests; production uses urllib's default opener, which
        # this class never configures with a redirect handler.
        self._opener = opener or urlopen

    @property
    def _secret_names(self) -> tuple[str, ...]:
        return self.registry.secret_environment_names

    def _redact(self, message: object) -> str:
        return redact_secrets(
            str(message),
            extra_environment_names=self._secret_names,
        )

    def health(self, endpoint_id: str) -> dict[str, Any]:
        """Return a health document shaped for the preflight contract."""
        return self.describe(endpoint_id).to_public_dict()

    def describe(self, endpoint_id: str) -> EndpointHealth:
        endpoint = self.registry.endpoint(endpoint_id)
        try:
            base_url = endpoint.resolve_base_url(self.environment)
        except Exception as exc:
            raise EndpointUnreachableError(
                endpoint_id,
                self._redact(exc),
                secret_environment_names=self._secret_names,
            ) from exc
        # A liveness probe is the wrong place to spend a bearer token. It
        # is attached only when the endpoint explicitly declares that its
        # health route is authenticated.
        token = (
            endpoint.resolve_token(self.environment)
            if endpoint.health_requires_auth
            else None
        )
        timeout = self.timeout_seconds or endpoint.timeout_seconds
        request = Request(  # noqa: S310 - scheme validated by the settings
            base_url.rstrip("/") + "/healthz",
            method="GET",
        )
        request.add_header("Accept", "application/json")
        if token:
            request.add_header("Authorization", f"Bearer {token}")
        try:
            with self._opener(request, timeout=timeout) as response:
                status = int(getattr(response, "status", 0) or 0)
                body = response.read(MAXIMUM_HEALTH_RESPONSE_BYTES + 1)
        except HTTPError as exc:
            # A reachable endpoint that answered non-2xx is still a failed
            # health check, but it is reported with its status so an auth
            # problem is distinguishable from a dead node.
            return EndpointHealth(
                endpoint_id=endpoint_id,
                healthy=False,
                authenticated=bool(token),
                status_code=int(exc.code),
                api_version=None,
                node_id=None,
                representations=(),
                object_catalog_version=None,
                detail=self._redact(
                    f"Data Agent returned HTTP {exc.code}"
                ),
            )
        except (URLError, OSError, ValueError) as exc:
            raise EndpointUnreachableError(
                endpoint_id,
                self._redact(exc),
                secret_environment_names=self._secret_names,
            ) from exc

        if len(body) > MAXIMUM_HEALTH_RESPONSE_BYTES:
            return EndpointHealth(
                endpoint_id=endpoint_id,
                healthy=False,
                authenticated=bool(token),
                status_code=status,
                api_version=None,
                node_id=None,
                representations=(),
                object_catalog_version=None,
                detail="health response exceeded its size bound",
            )
        if status < 200 or status >= 300:
            return EndpointHealth(
                endpoint_id=endpoint_id,
                healthy=False,
                authenticated=bool(token),
                status_code=status,
                api_version=None,
                node_id=None,
                representations=(),
                object_catalog_version=None,
                detail=f"Data Agent returned HTTP {status}",
            )
        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            return EndpointHealth(
                endpoint_id=endpoint_id,
                healthy=False,
                authenticated=bool(token),
                status_code=status,
                api_version=None,
                node_id=None,
                representations=(),
                object_catalog_version=None,
                detail=self._redact(f"unreadable health document: {exc}"),
            )
        if not isinstance(payload, Mapping):
            return EndpointHealth(
                endpoint_id=endpoint_id,
                healthy=False,
                authenticated=bool(token),
                status_code=status,
                api_version=None,
                node_id=None,
                representations=(),
                object_catalog_version=None,
                detail="health document is not an object",
            )

        api_version = payload.get("api_version")
        node_id = payload.get("node_id")
        raw_representations = payload.get("representations") or []
        representations = tuple(
            str(item) for item in raw_representations
            if isinstance(item, str)
        )
        catalog_version = payload.get("object_catalog_version")
        problems: list[str] = []
        if payload.get("status") != "ok":
            problems.append(f"status={payload.get('status')!r}")
        if api_version != DATA_AGENT_API_VERSION:
            problems.append(
                f"api_version={api_version!r} expected "
                f"{DATA_AGENT_API_VERSION!r}"
            )
        if node_id != endpoint.node_id:
            problems.append(
                f"node_id={node_id!r} but the registry declares "
                f"{endpoint.node_id!r}"
            )
        if not representations:
            problems.append("no representation capabilities advertised")
        return EndpointHealth(
            endpoint_id=endpoint_id,
            healthy=not problems,
            authenticated=bool(token),
            status_code=status,
            api_version=(
                str(api_version) if isinstance(api_version, str) else None
            ),
            node_id=str(node_id) if isinstance(node_id, str) else None,
            representations=representations,
            object_catalog_version=(
                str(catalog_version)
                if isinstance(catalog_version, str)
                else None
            ),
            detail=(
                "Data Agent reports healthy and matches the registry"
                if not problems
                else self._redact("; ".join(problems))
            ),
        )
