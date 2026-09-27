"""Construct an opt-in, offline-preview route Gateway from bound inputs.

This preview does not authorize a FlowMesh submission. A live route executor
still requires a fresh source-bound admission and the runbook pre-submit gates.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .route_action_bridge import RouteActionBridge, RouteActionBridgeError
from .route_action_gateway import RouteActionGateway
from .route_action_quotes import FrozenRouteQuoteSource


PREVIEW_SCHEMA = "pathfinder.route-action-development-preview/v1"
_FIELDS = {
    "schema_version", "preview_only", "credentials_recorded", "plan_dir",
    "quote_package_dir", "plan_sha256", "source_trace_package_sha256",
    "rate_card_sha256", "execution_namespace", "choice_db", "session_db",
}


def require_preview_loopback(host: str) -> None:
    if host not in {"127.0.0.1", "::1", "localhost"}:
        raise RouteActionBridgeError(
            "route-action preview must bind to loopback only"
        )


def _path(config_file: Path, value: Any, name: str) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise RouteActionBridgeError(f"route preview {name} is missing")
    path = Path(value)
    return (path if path.is_absolute() else config_file.parent / path).resolve()


def load_route_action_preview(config_file: str | Path) -> RouteActionGateway:
    """Verify the quote and public plan; never infer live admission from them."""
    source = Path(config_file).resolve()
    try:
        config = json.loads(source.read_bytes())
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RouteActionBridgeError(
            "route preview configuration cannot be read"
        ) from exc
    if (
        not isinstance(config, dict)
        or set(config) != _FIELDS
        or config.get("schema_version") != PREVIEW_SCHEMA
        or config.get("preview_only") is not True
        or config.get("credentials_recorded") is not False
    ):
        raise RouteActionBridgeError(
            "route preview configuration is not a bounded preview"
        )
    plan_dir = _path(source, config["plan_dir"], "plan")
    quote_dir = _path(source, config["quote_package_dir"], "quote package")
    choice_db = _path(source, config["choice_db"], "choice store")
    session_db = _path(source, config["session_db"], "session store")
    if (choice_db == session_db or any(
        state == frozen or frozen in state.parents
        for state in (choice_db, session_db)
        for frozen in (plan_dir, quote_dir)
    )):
        raise RouteActionBridgeError(
            "route preview state must not write into a frozen package"
        )
    quote_source = FrozenRouteQuoteSource(
        quote_dir,
        plan_sha256=config["plan_sha256"],
        source_trace_package_sha256=config["source_trace_package_sha256"],
        rate_card_sha256=config["rate_card_sha256"],
    )
    bridge = RouteActionBridge(
        plan_dir, choice_db,
        price_basis_sha256=quote_source.price_basis_sha256,
        execution_namespace=config["execution_namespace"],
    )
    if bridge.plan_sha256 != quote_source.plan_sha256:
        raise RouteActionBridgeError(
            "route preview plan differs from quote package"
        )
    return RouteActionGateway(
        bridge=bridge,
        session_db=session_db,
        quote_source=quote_source,
    )


__all__ = [
    "PREVIEW_SCHEMA", "load_route_action_preview", "require_preview_loopback",
]
