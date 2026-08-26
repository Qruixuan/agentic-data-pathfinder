"""Multi-Data-Agent endpoint registry and access routing.

A distributed pilot places origin and local representations on different Data
Agent nodes, so every access has to name the endpoint it went to. This module
turns ``endpoint_id -> connection settings + node identity`` into a declared,
auditable mapping and resolves each ``(design, representation)`` pair to
exactly one endpoint.

Two properties are load-bearing:

* **No silent fallback.** An unroutable representation or an unreachable
  endpoint raises. Quietly serving origin bytes from the local node would
  invert the very contrast the pilot exists to measure.
* **No credentials anywhere.** The registry stores the *names* of environment
  variables, never their values, so neither the committed template nor any
  emitted record can become a credential sink.
"""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable, Mapping

from ..data_agent_client import DataAgentClientSettings
from ..integrations.flowmesh.redaction import (
    endpoint_fingerprint,
    redact_secrets,
    sanitize_endpoint,
)


ENDPOINT_REGISTRY_SCHEMA_VERSION = (
    "pathfinder.data-agent-endpoint-registry/v1alpha1"
)
DEFAULT_ROUTE_KEY = "*"
#: Whether reaching an endpoint crosses a network. ``local`` means the bytes
#: never leave the execution node, which is the only case where a zero
#: network quantity is a measurement rather than an omission.
NETWORK_TRANSPORTS = ("remote", "local")


class EndpointRegistryError(ValueError):
    """Raised when an endpoint registry or a route is invalid."""


class EndpointUnreachableError(RuntimeError):
    """Raised when a declared endpoint cannot be reached.

    Deliberately distinct from a policy or data failure: an unreachable node
    is an infrastructure fault that must be retried and reported separately,
    never recorded as a design performing badly.
    """

    def __init__(
        self,
        endpoint_id: str,
        detail: str,
        *,
        secret_environment_names: Iterable[str] = (),
    ) -> None:
        redacted = redact_secrets(
            detail,
            extra_environment_names=secret_environment_names,
        )
        super().__init__(
            f"Data Agent endpoint '{endpoint_id}' is unreachable: {redacted}"
        )
        self.endpoint_id = endpoint_id
        self.detail = redacted

    failure_class = "infrastructure"


class CrossEndpointHandleError(RuntimeError):
    """Raised when an artifact handle is redeemed at the wrong endpoint."""

    failure_class = "policy"


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise EndpointRegistryError(f"{name} must be an object")
    return value


def _string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EndpointRegistryError(f"{name} must be a non-empty string")
    return value.strip()


def _require(payload: Mapping[str, Any], key: str, name: str) -> Any:
    if key not in payload:
        raise EndpointRegistryError(
            f"{name} is a required configuration field"
        )
    return payload[key]


def _positive_number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EndpointRegistryError(f"{name} must be a number")
    result = float(value)
    if not math.isfinite(result) or result <= 0.0:
        raise EndpointRegistryError(f"{name} must be positive and finite")
    return result


def _nonnegative_integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise EndpointRegistryError(
            f"{name} must be a non-negative integer"
        )
    return value


@dataclass(frozen=True)
class DataAgentEndpoint:
    """One declared Data Agent node.

    ``base_url_env`` and ``token_env`` name environment variables. The URL
    and token themselves are resolved at connection time and never stored on
    this object, so a registry document is safe to commit and to print.
    """

    endpoint_id: str
    node_id: str
    location: str
    description: str
    base_url_env: str
    token_env: str | None
    timeout_seconds: float
    max_retries: int
    telemetry_capabilities: tuple[str, ...]
    network_transport: str = "remote"
    network_zero_justification: str | None = None
    health_requires_auth: bool = False

    @property
    def crosses_network(self) -> bool:
        return self.network_transport == "remote"

    def resolve_base_url(
        self,
        environment: Mapping[str, str] | None = None,
    ) -> str:
        env = os.environ if environment is None else environment
        value = env.get(self.base_url_env)
        if not value or not str(value).strip():
            raise EndpointRegistryError(
                f"endpoint '{self.endpoint_id}' requires environment "
                f"variable {self.base_url_env} to hold its base URL; it is "
                "unset or empty"
            )
        return str(value).strip()

    def resolve_token(
        self,
        environment: Mapping[str, str] | None = None,
    ) -> str | None:
        if self.token_env is None:
            return None
        env = os.environ if environment is None else environment
        value = env.get(self.token_env)
        return str(value) if value else None

    def client_settings(
        self,
        environment: Mapping[str, str] | None = None,
    ) -> DataAgentClientSettings:
        return DataAgentClientSettings(
            base_url=self.resolve_base_url(environment),
            token=self.resolve_token(environment),
            timeout_seconds=self.timeout_seconds,
            max_retries=self.max_retries,
        )

    def to_public_dict(
        self,
        environment: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Describe the endpoint without emitting a URL or a token."""
        env = os.environ if environment is None else environment
        configured = bool(str(env.get(self.base_url_env) or "").strip())
        payload: dict[str, Any] = {
            "endpoint_id": self.endpoint_id,
            "node_id": self.node_id,
            "location": self.location,
            "description": self.description,
            "base_url_env": self.base_url_env,
            "base_url_configured": configured,
            "token_env": self.token_env,
            "token_configured": self.resolve_token(env) is not None,
            "credentials_recorded": False,
            "timeout_seconds": self.timeout_seconds,
            "max_retries": self.max_retries,
            "telemetry_capabilities": list(self.telemetry_capabilities),
            "network_transport": self.network_transport,
            "network_zero_justification": self.network_zero_justification,
            "health_requires_auth": self.health_requires_auth,
        }
        if configured:
            # A sanitized scheme://host:port and its fingerprint are safe to
            # correlate on; the raw URL and any query string are not.
            url = self.resolve_base_url(env)
            payload["endpoint"] = sanitize_endpoint(url)
            payload["endpoint_sha256"] = endpoint_fingerprint(url)
        return payload


@dataclass(frozen=True)
class AccessRoute:
    """The resolved endpoint for one access, plus both node identities."""

    endpoint_id: str
    design_id: str
    representation_id: str
    source_node_id: str
    source_location: str
    destination_execution_node_id: str
    rule: str

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "endpoint_id": self.endpoint_id,
            "design_id": self.design_id,
            "representation_id": self.representation_id,
            "source_node_id": self.source_node_id,
            "source_location": self.source_location,
            "destination_execution_node_id": (
                self.destination_execution_node_id
            ),
            "routing_rule": self.rule,
            "credentials_recorded": False,
        }


@dataclass(frozen=True)
class EndpointRegistry:
    """Declared endpoints plus the placement rules that select between them."""

    schema_version: str
    registry_id: str
    execution_node_id: str
    endpoints: dict[str, DataAgentEndpoint]
    placement: dict[tuple[str, str], str]
    default_endpoint_id: str | None
    source_sha256: str

    @property
    def endpoint_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self.endpoints))

    @property
    def secret_environment_names(self) -> tuple[str, ...]:
        """Every declared token variable, for redacting third-party text."""
        return tuple(sorted({
            endpoint.token_env
            for endpoint in self.endpoints.values()
            if endpoint.token_env
        }))

    @property
    def single_endpoint(self) -> bool:
        return len(self.endpoints) == 1

    def endpoint(self, endpoint_id: str) -> DataAgentEndpoint:
        try:
            return self.endpoints[endpoint_id]
        except KeyError:
            raise EndpointRegistryError(
                f"unknown endpoint_id: {endpoint_id}"
            ) from None

    def route(
        self,
        *,
        design_id: str,
        representation_id: str,
    ) -> AccessRoute:
        """Resolve exactly one endpoint, or raise.

        There is no fallback tier. If no rule matches and no default is
        declared, the access fails rather than being served by whichever
        node happens to be reachable.
        """
        key = (design_id, representation_id)
        rule = "explicit-design-representation"
        endpoint_id = self.placement.get(key)
        if endpoint_id is None:
            endpoint_id = self.placement.get((design_id, DEFAULT_ROUTE_KEY))
            rule = "explicit-design-default"
        if endpoint_id is None and self.default_endpoint_id is not None:
            endpoint_id = self.default_endpoint_id
            rule = (
                "single-endpoint-compatibility"
                if self.single_endpoint
                else "registry-default-endpoint"
            )
        if endpoint_id is None:
            raise EndpointRegistryError(
                f"no endpoint is declared for design '{design_id}' "
                f"representation '{representation_id}'; refusing to guess "
                "a Data Agent"
            )
        endpoint = self.endpoint(endpoint_id)
        return AccessRoute(
            endpoint_id=endpoint.endpoint_id,
            design_id=design_id,
            representation_id=representation_id,
            source_node_id=endpoint.node_id,
            source_location=endpoint.location,
            destination_execution_node_id=self.execution_node_id,
            rule=rule,
        )

    def to_public_dict(
        self,
        environment: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "execution_node_id": self.execution_node_id,
            "registry_sha256": self.source_sha256,
            "single_endpoint_compatibility_mode": self.single_endpoint,
            "default_endpoint_id": self.default_endpoint_id,
            "credentials_recorded": False,
            "endpoints": [
                self.endpoints[endpoint_id].to_public_dict(environment)
                for endpoint_id in self.endpoint_ids
            ],
            "placement": [
                {
                    "design_id": design_id,
                    "representation_id": representation_id,
                    "endpoint_id": endpoint_id,
                }
                for (design_id, representation_id), endpoint_id
                in sorted(self.placement.items())
            ],
        }


def load_endpoint_registry(path: str | Path) -> EndpointRegistry:
    """Load and validate an endpoint registry document."""
    source = Path(path).resolve()
    if not source.is_file():
        raise EndpointRegistryError(
            f"endpoint registry does not exist: {source}"
        )
    raw_bytes = source.read_bytes()
    try:
        root = _mapping(json.loads(raw_bytes), "endpoint registry")
    except json.JSONDecodeError as exc:
        raise EndpointRegistryError(
            f"invalid endpoint registry JSON: {source}"
        ) from exc
    return build_endpoint_registry(
        root,
        source_sha256=sha256(raw_bytes).hexdigest(),
    )


def build_endpoint_registry(
    root: Mapping[str, Any],
    *,
    source_sha256: str,
) -> EndpointRegistry:
    schema_version = _string(
        _require(root, "schema_version", "schema_version"),
        "schema_version",
    )
    if schema_version != ENDPOINT_REGISTRY_SCHEMA_VERSION:
        raise EndpointRegistryError(
            f"unsupported endpoint registry schema_version: {schema_version}"
        )
    raw_endpoints = _require(root, "endpoints", "endpoints")
    if not isinstance(raw_endpoints, list) or not raw_endpoints:
        raise EndpointRegistryError("endpoints must be a non-empty array")

    endpoints: dict[str, DataAgentEndpoint] = {}
    for index, raw in enumerate(raw_endpoints):
        name = f"endpoints[{index}]"
        payload = _mapping(raw, name)
        endpoint_id = _string(
            _require(payload, "endpoint_id", f"{name}.endpoint_id"),
            f"{name}.endpoint_id",
        )
        if endpoint_id in endpoints:
            raise EndpointRegistryError(
                f"duplicate endpoint_id: {endpoint_id}"
            )
        for forbidden in ("base_url", "token", "api_key", "password"):
            if forbidden in payload:
                raise EndpointRegistryError(
                    f"{name}.{forbidden} is not allowed; declare "
                    "base_url_env / token_env and supply the value through "
                    "the environment so the registry never stores a URL or "
                    "a credential"
                )
        capabilities = payload.get("telemetry_capabilities", [])
        if not isinstance(capabilities, list):
            raise EndpointRegistryError(
                f"{name}.telemetry_capabilities must be an array"
            )
        token_env = payload.get("token_env")
        transport = _string(
            payload.get("network_transport", "remote"),
            f"{name}.network_transport",
        )
        if transport not in NETWORK_TRANSPORTS:
            raise EndpointRegistryError(
                f"{name}.network_transport must be one of "
                + ", ".join(NETWORK_TRANSPORTS)
            )
        justification = payload.get("network_zero_justification")
        if transport == "local":
            # A zero network quantity is only defensible when the operator
            # says why no bytes cross a network for this endpoint.
            justification = _string(
                justification,
                f"{name}.network_zero_justification",
            )
        elif justification is not None:
            justification = _string(
                justification,
                f"{name}.network_zero_justification",
            )
        health_auth = payload.get("health_requires_auth", False)
        if not isinstance(health_auth, bool):
            raise EndpointRegistryError(
                f"{name}.health_requires_auth must be a boolean"
            )
        endpoints[endpoint_id] = DataAgentEndpoint(
            endpoint_id=endpoint_id,
            node_id=_string(
                _require(payload, "node_id", f"{name}.node_id"),
                f"{name}.node_id",
            ),
            location=_string(
                _require(payload, "location", f"{name}.location"),
                f"{name}.location",
            ),
            description=_string(
                payload.get("description", endpoint_id),
                f"{name}.description",
            ),
            base_url_env=_string(
                _require(payload, "base_url_env", f"{name}.base_url_env"),
                f"{name}.base_url_env",
            ),
            token_env=(
                None
                if token_env is None
                else _string(token_env, f"{name}.token_env")
            ),
            timeout_seconds=_positive_number(
                payload.get("timeout_seconds", 30.0),
                f"{name}.timeout_seconds",
            ),
            max_retries=_nonnegative_integer(
                payload.get("max_retries", 1),
                f"{name}.max_retries",
            ),
            telemetry_capabilities=tuple(
                _string(item, f"{name}.telemetry_capabilities[{position}]")
                for position, item in enumerate(capabilities)
            ),
            network_transport=transport,
            network_zero_justification=justification,
            health_requires_auth=health_auth,
        )

    placement: dict[tuple[str, str], str] = {}
    for index, raw in enumerate(
        _require(root, "placement", "placement")
        if "placement" in root
        else []
    ):
        name = f"placement[{index}]"
        payload = _mapping(raw, name)
        design_id = _string(
            _require(payload, "design_id", f"{name}.design_id"),
            f"{name}.design_id",
        )
        representation_id = _string(
            payload.get("representation_id", DEFAULT_ROUTE_KEY),
            f"{name}.representation_id",
        )
        endpoint_id = _string(
            _require(payload, "endpoint_id", f"{name}.endpoint_id"),
            f"{name}.endpoint_id",
        )
        if endpoint_id not in endpoints:
            raise EndpointRegistryError(
                f"{name}.endpoint_id references an undeclared endpoint: "
                + endpoint_id
            )
        key = (design_id, representation_id)
        if key in placement:
            raise EndpointRegistryError(
                f"duplicate placement rule for design '{design_id}' "
                f"representation '{representation_id}'"
            )
        placement[key] = endpoint_id

    default_endpoint_id = root.get("default_endpoint_id")
    if default_endpoint_id is not None:
        default_endpoint_id = _string(
            default_endpoint_id,
            "default_endpoint_id",
        )
        if default_endpoint_id not in endpoints:
            raise EndpointRegistryError(
                "default_endpoint_id references an undeclared endpoint: "
                + default_endpoint_id
            )
    elif len(endpoints) == 1 and not placement:
        # Backward compatibility: a registry describing exactly one Data
        # Agent and no placement rules behaves like the existing
        # single-endpoint deployment.
        default_endpoint_id = next(iter(endpoints))

    return EndpointRegistry(
        schema_version=schema_version,
        registry_id=_string(
            _require(root, "registry_id", "registry_id"),
            "registry_id",
        ),
        execution_node_id=_string(
            _require(root, "execution_node_id", "execution_node_id"),
            "execution_node_id",
        ),
        endpoints=endpoints,
        placement=placement,
        default_endpoint_id=default_endpoint_id,
        source_sha256=source_sha256,
    )


@dataclass(frozen=True)
class EndpointScopedHandle:
    """An artifact handle bound to the endpoint that issued it."""

    endpoint_id: str
    handle: str

    @property
    def fingerprint(self) -> str:
        return sha256(
            f"{self.endpoint_id}\0{self.handle}".encode("utf-8")
        ).hexdigest()

    def redeem_at(self, endpoint_id: str) -> str:
        """Return the raw handle, but only at its issuing endpoint."""
        if endpoint_id != self.endpoint_id:
            raise CrossEndpointHandleError(
                f"artifact handle was issued by endpoint "
                f"'{self.endpoint_id}' and cannot be redeemed at "
                f"'{endpoint_id}'"
            )
        return self.handle

    def to_public_dict(self) -> dict[str, Any]:
        """Describe the handle without exposing its bearer value."""
        return {
            "endpoint_id": self.endpoint_id,
            "artifact_handle_sha256": self.fingerprint,
            "handle_recorded": False,
        }
