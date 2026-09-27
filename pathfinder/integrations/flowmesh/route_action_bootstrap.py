"""Construct an opt-in, offline-preview route Gateway from bound inputs.

This preview does not authorize a FlowMesh submission. A live route executor
still requires a fresh source-bound admission and the runbook pre-submit gates.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .route_action_cache_reader import LiveCacheStatusReader
from .route_action_candidate_schedule import (
    MANIFEST as CANDIDATE_MANIFEST, ROWS as CANDIDATE_ROWS,
)
from .route_action_bridge import RouteActionBridge, RouteActionBridgeError
from .route_action_gateway import RouteActionGateway, RouteSession
from .route_action_quotes import FrozenRouteQuoteSource
from .route_action_runtime_admission import (
    verify_route_action_runtime_admission,
)
from ...simulator.full_flow_cache import HttpFullFlowArtifactCacheClient
from ...simulator.interleaved_multiq_runtime_admission import (
    TRIALS as ADMITTED_TRIALS, verify_interleaved_runtime_admission,
)


PREVIEW_SCHEMA = "pathfinder.route-action-development-preview/v1"
LIVE_SCHEMA = "pathfinder.route-action-live-gateway/v1"
_FIELDS = {
    "schema_version", "preview_only", "credentials_recorded", "plan_dir",
    "quote_package_dir", "plan_sha256", "source_trace_package_sha256",
    "rate_card_sha256", "execution_namespace", "choice_db", "session_db",
}
_LIVE_PATHS = (
    "plan_dir", "quote_package_dir", "candidate_dir",
    "route_runtime_admission_dir", "runtime_admission_dir",
    "trial_dag_dir", "route_binding_dir", "n1_public_commitment_dir",
    "n2_index_package_dir", "n3_package_dir", "raw_package_dir",
    "n4_package_dir", "query_dir", "video_index_dir",
    "preparation_dir", "caption_dir", "choice_db", "session_db",
)
_LIVE_FIELDS = set(_LIVE_PATHS) | {
    "schema_version", "live_mode", "credentials_recorded",
    "coordinator_base_urls", "cache_base_urls", "cache_ids",
    "cache_token_env_name",
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


def load_route_action_live(config_file: str | Path) -> RouteActionGateway:
    """Construct a fail-closed live Gateway from canonical public sources.

    Loading makes no FlowMesh, model or cache request. The deployment must
    separately check cache health/auth and worker readiness before submission.
    """
    source = Path(config_file).resolve()
    try:
        config = json.loads(source.read_bytes())
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RouteActionBridgeError(
            "route live configuration cannot be read"
        ) from exc
    if (not isinstance(config, dict) or set(config) != _LIVE_FIELDS
            or config.get("schema_version") != LIVE_SCHEMA
            or config.get("live_mode") is not True
            or config.get("credentials_recorded") is not False):
        raise RouteActionBridgeError(
            "route live configuration is not an exact live contract"
        )
    roots = {name: _path(source, config[name], name)
             for name in _LIVE_PATHS}
    state_roots = (roots["choice_db"], roots["session_db"])
    frozen_roots = tuple(roots[name] for name in _LIVE_PATHS
                         if name not in {"choice_db", "session_db"})
    if (state_roots[0] == state_roots[1] or any(
        state == frozen or frozen in state.parents
        for state in state_roots for frozen in frozen_roots
    )):
        raise RouteActionBridgeError(
            "route live state must not write into a frozen source"
        )
    origins = config["coordinator_base_urls"]
    cache_urls = config["cache_base_urls"]
    cache_ids = config["cache_ids"]
    if (not isinstance(origins, dict) or set(origins) != {"N7", "N8"}
            or not isinstance(cache_urls, dict)
            or set(cache_urls) != {"N7", "N8"}
            or not isinstance(cache_ids, dict)
            or set(cache_ids) != {"N7", "N8"}):
        raise RouteActionBridgeError(
            "route live coordinator or cache identity is incomplete"
        )
    admitted = verify_interleaved_runtime_admission(
        roots["runtime_admission_dir"],
        trial_dag_dir=roots["trial_dag_dir"],
        binding_dir=roots["route_binding_dir"],
        plan_dir=roots["plan_dir"],
        n1_public_commitment_dir=roots["n1_public_commitment_dir"],
        n2_index_package_dir=roots["n2_index_package_dir"],
        n3_package_dir=roots["n3_package_dir"],
        raw_package_dir=roots["raw_package_dir"],
        n4_package_dir=roots["n4_package_dir"],
        query_dir=roots["query_dir"],
        video_index_dir=roots["video_index_dir"],
        preparation_dir=roots["preparation_dir"],
        caption_dir=roots["caption_dir"],
        coordinator_base_urls=origins,
    )
    trials = [json.loads(line) for line in (
        roots["runtime_admission_dir"] / ADMITTED_TRIALS
    ).read_bytes().splitlines()]
    runtime = verify_route_action_runtime_admission(
        roots["route_runtime_admission_dir"],
        candidate_dir=roots["candidate_dir"],
        plan_dir=roots["plan_dir"], quote_dir=roots["quote_package_dir"],
        admitted_trials=trials,
        admission_sha256=admitted["admission_sha256"],
    )
    candidate_doc = json.loads((
        roots["candidate_dir"] / CANDIDATE_MANIFEST
    ).read_bytes())
    quote_doc = json.loads((
        roots["quote_package_dir"] / "route-quotes.json"
    ).read_bytes())
    quote = FrozenRouteQuoteSource(
        roots["quote_package_dir"],
        plan_sha256=quote_doc["plan_sha256"],
        source_trace_package_sha256=quote_doc[
            "source_trace_package_sha256"
        ],
        rate_card_sha256=quote_doc["rate_card_sha256"],
    )
    bridge = RouteActionBridge(
        roots["plan_dir"], roots["choice_db"],
        price_basis_sha256=quote.price_basis_sha256,
        execution_namespace=candidate_doc["execution_namespace"],
    )
    if bridge.plan_sha256 != quote.plan_sha256:
        raise RouteActionBridgeError(
            "live route plan differs from frozen quote"
        )
    sessions: dict[str, RouteSession] = {}
    for line in (roots["candidate_dir"] / CANDIDATE_ROWS).read_bytes(
    ).splitlines():
        row = json.loads(line)
        session = RouteSession(
            row["session_id"], row["question_id"],
            row["physical_design_id"], row["object_id"],
            row["public_task_sha256"],
        )
        prior = sessions.setdefault(session.session_id, session)
        if prior != session:
            raise RouteActionBridgeError(
                "candidate session binds inconsistent public identities"
            )
    if len(sessions) != candidate_doc["session_count"]:
        raise RouteActionBridgeError(
            "live session count differs from frozen candidates"
        )
    token_name = config["cache_token_env_name"]
    if (not isinstance(token_name, str)
            or not token_name.startswith("PATHFINDER_")
            or not token_name.replace("_", "").isalnum()):
        raise RouteActionBridgeError("cache credential name is invalid")
    token = os.environ.get(token_name)
    if not token:
        raise RouteActionBridgeError(
            "route live cache credential environment variable is absent: "
            + token_name
        )
    clients = {
        node: HttpFullFlowArtifactCacheClient(
            base_url=cache_urls[node], token=token,
            expected_node_id=node, expected_cache_id=cache_ids[node],
            simulator_private_http_hosts=(
                "pathfinder-full-flow-n7-persistent-cache",
                "pathfinder-full-flow-n8-persistent-cache",
            ),
        ) for node in ("N7", "N8")
    }
    expected_objects = {session.object_id for session in sessions.values()}
    cache_reader = LiveCacheStatusReader.from_verified_n4_package(
        clients=clients, package_dir=roots["n4_package_dir"],
        expected_object_ids=expected_objects,
    )
    runs = {key: episode for node in runtime["bindings"].values()
            for key, episode in node.items()}
    return RouteActionGateway(
        bridge=bridge, session_db=roots["session_db"],
        quote_source=quote, cache_reader=cache_reader,
        admitted_sessions=sessions, admitted_runs=runs,
    )


__all__ = [
    "PREVIEW_SCHEMA", "LIVE_SCHEMA", "load_route_action_preview",
    "load_route_action_live", "require_preview_loopback",
]
