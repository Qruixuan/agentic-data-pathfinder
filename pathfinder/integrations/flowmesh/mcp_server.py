from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from ...config import load_config
from ...data_agent_client import (
    DataAgentClientSettings,
    HttpDataAgentClient,
)
from .data_agent_backend import RemoteDataAgentBackend
from .gateway import AccessGateway, SQLiteSessionStore


class McpDependencyError(RuntimeError):
    """Raised when the optional MCP server dependency is unavailable."""


def build_mcp_server(
    *,
    config_path: str | Path,
    state_db: str | Path,
    host: str,
    port: int,
    data_agent_url: str | None = None,
    data_agent_timeout_seconds: float = 30.0,
    data_agent_max_retries: int = 1,
    telemetry_quiescence_timeout_seconds: float = 15.0,
    endpoint_registry: str | Path | None = None,
) -> Any:
    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError as exc:
        raise McpDependencyError(
            "MCP dependencies are not installed. "
            "Install with `python -m pip install -e .[flowmesh]`."
        ) from exc

    if endpoint_registry is not None:
        # Multi-endpoint mode: the MCP Gateway performs every Data Agent
        # access and artifact download, so it is the process that must hold
        # the routing table. The worker only calls MCP.
        from ...distributed.routing import build_routed_gateway_backend

        backend, _registry = build_routed_gateway_backend(
            endpoint_registry,
            telemetry_quiescence_timeout_seconds=(
                telemetry_quiescence_timeout_seconds
            ),
        )
        return _build_server(
            backend,
            config_path=config_path,
            state_db=state_db,
            host=host,
            port=port,
            fast_mcp=FastMCP,
        )

    resolved_data_agent_url = (
        data_agent_url or os.getenv("PATHFINDER_DATA_AGENT_URL")
    )
    backend = None
    if resolved_data_agent_url:
        settings = DataAgentClientSettings.from_environment(
            base_url=resolved_data_agent_url,
            timeout_seconds=data_agent_timeout_seconds,
            max_retries=data_agent_max_retries,
        )
        backend = RemoteDataAgentBackend(
            HttpDataAgentClient(settings),
            telemetry_quiescence_timeout_seconds=(
                telemetry_quiescence_timeout_seconds
            ),
        )

    return _build_server(
        backend,
        config_path=config_path,
        state_db=state_db,
        host=host,
        port=port,
        fast_mcp=FastMCP,
    )


def _build_server(
    backend: Any,
    *,
    config_path: str | Path,
    state_db: str | Path,
    host: str,
    port: int,
    fast_mcp: Any,
) -> Any:
    """Register the access tools over one Gateway, routed or not."""
    gateway = AccessGateway(
        load_config(config_path),
        SQLiteSessionStore(state_db),
        backend,
    )
    server = fast_mcp(
        "Pathfinder Access Gateway",
        host=host,
        port=port,
    )

    @server.tool()
    def list_offers(session_id: str) -> dict[str, Any]:
        """List offered representations, quotes, and remaining budget."""
        return gateway.list_offers(session_id)

    @server.tool()
    def access_representation(
        session_id: str,
        representation_id: str,
    ) -> dict[str, Any]:
        """Access one offered representation through Pathfinder."""
        return gateway.access_representation(session_id, representation_id)

    @server.tool()
    def fetch_artifact(
        session_id: str,
        artifact_handle: str,
    ) -> dict[str, Any]:
        """Fetch bounded text or JSON using a session-bound artifact handle."""
        return gateway.fetch_artifact(session_id, artifact_handle)

    @server.tool()
    def get_session_state(session_id: str) -> dict[str, Any]:
        """Return the current budget and access-event state."""
        return gateway.get_session_state(session_id)

    return server


def run_mcp_server(
    *,
    config_path: str | Path,
    state_db: str | Path,
    host: str = "0.0.0.0",
    port: int = 8765,
    data_agent_url: str | None = None,
    data_agent_timeout_seconds: float = 30.0,
    data_agent_max_retries: int = 1,
    telemetry_quiescence_timeout_seconds: float = 15.0,
    endpoint_registry: str | Path | None = None,
) -> None:
    server = build_mcp_server(
        config_path=config_path,
        state_db=state_db,
        host=host,
        port=port,
        data_agent_url=data_agent_url,
        data_agent_timeout_seconds=data_agent_timeout_seconds,
        data_agent_max_retries=data_agent_max_retries,
        telemetry_quiescence_timeout_seconds=(
            telemetry_quiescence_timeout_seconds
        ),
        endpoint_registry=endpoint_registry,
    )
    server.run(transport="streamable-http")
