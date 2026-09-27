from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Callable

from ...config import load_config
from ...data_agent_client import (
    DataAgentClientSettings,
    HttpDataAgentClient,
)
from .data_agent_backend import RemoteDataAgentBackend
from .gateway import AccessGateway, SQLiteSessionStore
from .route_action_gateway import RouteActionGateway
from .visual_artifact import N6VisualInferenceClient


class McpDependencyError(RuntimeError):
    """Raised when the optional MCP server dependency is unavailable."""


def _invoke_traced_tool(
    tool_name: str,
    session_id: str,
    action: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    """Optionally log tool boundaries without arguments or response content."""
    if os.getenv("PATHFINDER_PPD_TOOL_TRACE") != "1":
        return action()
    session_digest = hashlib.sha256(session_id.encode("utf-8")).hexdigest()

    def emit(status: str, error_class: str | None = None) -> None:
        print(json.dumps({
            "event": "pathfinder_ppd_tool_boundary",
            "session_sha256": session_digest,
            "tool": tool_name,
            "status": status,
            "error_class": error_class,
        }, sort_keys=True), flush=True)

    emit("started")
    try:
        result = action()
    except Exception as exc:
        emit("error", type(exc).__name__)
        raise
    emit("complete")
    return result


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
    route_action_gateway: RouteActionGateway | None = None,
) -> Any:
    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError as exc:
        raise McpDependencyError(
            "MCP dependencies are not installed. "
            "Install with `python -m pip install -e .[flowmesh]`."
        ) from exc

    n6_url = os.getenv("PATHFINDER_PPD_N6_SEMANTIC_URL")
    n6_token = os.getenv("PATHFINDER_PPD_N6_SEMANTIC_TOKEN")
    if bool(n6_url) != bool(n6_token):
        raise ValueError(
            "PPD N6 visual inference requires both URL and token"
        )
    visual_client = (
        N6VisualInferenceClient(
            n6_url,
            n6_token,
            private_http_service_name=os.getenv(
                "PATHFINDER_PPD_N6_PRIVATE_HTTP_SERVICE_NAME"
            ),
        )
        if n6_url and n6_token else None
    )

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
            visual_inference_client=visual_client,
            route_action_gateway=route_action_gateway,
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
        visual_inference_client=visual_client,
        route_action_gateway=route_action_gateway,
    )


def _build_server(
    backend: Any,
    *,
    config_path: str | Path,
    state_db: str | Path,
    host: str,
    port: int,
    fast_mcp: Any,
    visual_inference_client: N6VisualInferenceClient | None = None,
    route_action_gateway: RouteActionGateway | None = None,
) -> Any:
    """Register the access tools over one Gateway, routed or not."""
    gateway = AccessGateway(
        load_config(config_path),
        SQLiteSessionStore(state_db),
        backend,
        visual_inference_client=visual_inference_client,
    )
    server = fast_mcp(
        "Pathfinder Access Gateway",
        host=host,
        port=port,
    )

    @server.tool()
    def list_offers(session_id: str) -> dict[str, Any]:
        """List offered representations, quotes, and remaining budget."""
        return _invoke_traced_tool(
            "list_offers", session_id,
            lambda: gateway.list_offers(session_id),
        )

    @server.tool()
    def access_representation(
        session_id: str,
        representation_id: str,
    ) -> dict[str, Any]:
        """Access one offered representation through Pathfinder."""
        return _invoke_traced_tool(
            "access_representation", session_id,
            lambda: gateway.access_representation(session_id, representation_id),
        )

    @server.tool()
    def fetch_artifact(
        session_id: str,
        artifact_handle: str,
    ) -> dict[str, Any]:
        """Fetch bounded text or JSON using a session-bound artifact handle."""
        return _invoke_traced_tool(
            "fetch_artifact", session_id,
            lambda: gateway.fetch_artifact(session_id, artifact_handle),
        )

    if visual_inference_client is not None:
        @server.tool()
        def inspect_visual_artifact(
            session_id: str,
            artifact_handle: str,
        ) -> dict[str, Any]:
            """Send a bound MP4/frame bundle to N6; return text and usage."""
            return _invoke_traced_tool(
                "inspect_visual_artifact", session_id,
                lambda: gateway.inspect_visual_artifact(
                    session_id, artifact_handle,
                ),
            )

    @server.tool()
    def get_session_state(session_id: str) -> dict[str, Any]:
        """Return the current budget and access-event state."""
        return _invoke_traced_tool(
            "get_session_state", session_id,
            lambda: gateway.get_session_state(session_id),
        )

    if route_action_gateway is not None:
        @server.tool()
        def list_route_offers(session_id: str) -> dict[str, Any]:
            """List source-bound physical route actions and measured quotes."""
            return _invoke_traced_tool(
                "list_route_offers", session_id,
                lambda: route_action_gateway.list_route_offers(session_id),
            )

        @server.tool()
        def commit_route_choice(
            session_id: str, action_id: str, offer_set_sha256: str,
        ) -> dict[str, Any]:
            """Commit exactly one previously offered route action."""
            return _invoke_traced_tool(
                "commit_route_choice", session_id,
                lambda: route_action_gateway.commit_route_choice(
                    session_id, action_id, offer_set_sha256,
                ),
            )

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
    route_action_gateway: RouteActionGateway | None = None,
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
        route_action_gateway=route_action_gateway,
    )
    server.run(transport="streamable-http")
