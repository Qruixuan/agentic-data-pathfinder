"""Agent-facing, opt-in tools for the verified route-action bridge.

The providers are deliberately required dependencies. There is no default
price and no inferred cache hit. This module does not execute a route.
"""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import closing
from dataclasses import asdict, dataclass
from pathlib import Path
import re
import sqlite3
from typing import Any, Protocol

from .route_action_bridge import (
    CacheObservation, RouteActionBridge, RouteActionBridgeError,
    RouteHandoff, RouteQuote,
)


_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True)
class RouteSession:
    session_id: str
    question_id: str
    physical_design_id: str
    object_id: str
    public_task_sha256: str


class VerifiedQuoteSource(Protocol):
    """Supply pre-outcome estimates from a verified frozen price package."""

    def quotes_for(
        self, question_id: str, physical_design_id: str,
    ) -> Mapping[tuple[str, str | None], RouteQuote]: ...


class VerifiedCacheReader(Protocol):
    """Supply read-only, episode-bound observations from live cache state."""

    def observe(
        self, *, question_id: str, object_id: str,
        executor_node_id: str, cache_episode_id: str,
    ) -> CacheObservation: ...


class RouteActionGateway:
    """Expose list/commit tools without changing representation Gateway state."""

    def __init__(
        self, *, bridge: RouteActionBridge, session_db: str | Path,
        quote_source: VerifiedQuoteSource,
        cache_reader: VerifiedCacheReader | None = None,
    ) -> None:
        if quote_source is None:
            raise RouteActionBridgeError("verified quote source is required")
        self.bridge = bridge
        self.quote_source = quote_source
        self.cache_reader = cache_reader
        self.session_db = Path(session_db)

    def register_session(
        self, *, session_id: str, question_id: str,
        physical_design_id: str, object_id: str,
        public_task_sha256: str,
    ) -> RouteSession:
        """Runner-only setup, before the Agent sees any route offer."""
        if not isinstance(session_id, str) or not _ID.fullmatch(session_id):
            raise RouteActionBridgeError("route session identity is invalid")
        if physical_design_id not in self.bridge.supported_physical_designs():
            raise RouteActionBridgeError("physical design is unsupported")
        if not isinstance(public_task_sha256, str) or not _DIGEST.fullmatch(
            public_task_sha256
        ):
            raise RouteActionBridgeError("public task digest is invalid")
        expected = self.bridge.public_question_identity(question_id)
        if expected != (object_id, public_task_sha256):
            raise RouteActionBridgeError(
                "route session differs from the verified public question"
            )
        session = RouteSession(
            session_id, question_id, physical_design_id,
            object_id, public_task_sha256,
        )
        self.session_db.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.session_db)) as connection:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS route_sessions ("
                    "session_id TEXT PRIMARY KEY, question_id TEXT NOT NULL, "
                    "physical_design_id TEXT NOT NULL, "
                    "object_id TEXT NOT NULL, public_task_sha256 TEXT NOT NULL)"
                )
                row = connection.execute(
                    "SELECT question_id, physical_design_id, object_id, "
                    "public_task_sha256 FROM route_sessions "
                    "WHERE session_id = ?", (session_id,),
                ).fetchone()
                values = (
                    question_id, physical_design_id, object_id,
                    public_task_sha256,
                )
                if row is None:
                    connection.execute(
                        "INSERT INTO route_sessions VALUES (?, ?, ?, ?, ?)",
                        (session_id, *values),
                    )
                elif tuple(row) != values:
                    raise RouteActionBridgeError(
                        "route session already binds a different task/design"
                    )
        return session

    def _session(self, session_id: str) -> RouteSession:
        if not isinstance(session_id, str) or not _ID.fullmatch(session_id):
            raise RouteActionBridgeError("route session identity is invalid")
        if not self.session_db.is_file():
            raise RouteActionBridgeError("route session store is absent")
        with closing(sqlite3.connect(self.session_db)) as connection:
            try:
                row = connection.execute(
                    "SELECT question_id, physical_design_id, object_id, "
                    "public_task_sha256 FROM route_sessions "
                    "WHERE session_id = ?", (session_id,),
                ).fetchone()
            except sqlite3.OperationalError as exc:
                raise RouteActionBridgeError(
                    "route session store is not initialized"
                ) from exc
        if row is None:
            raise RouteActionBridgeError("route session is unknown")
        session = RouteSession(session_id, *row)
        expected = self.bridge.public_question_identity(session.question_id)
        if expected != (session.object_id, session.public_task_sha256):
            raise RouteActionBridgeError(
                "route session no longer matches the verified plan"
            )
        return session

    def public_task_for_session(self, session_id: str) -> dict[str, Any]:
        """Runner-only read of the bound public question and options."""
        session = self._session(session_id)
        return self.bridge.public_question(session.question_id)

    def session_binding(self, session_id: str) -> RouteSession:
        """Runner-only read of the verified route session binding."""
        return self._session(session_id)

    def _inputs(self, session: RouteSession) -> tuple[
        Mapping[tuple[str, str | None], RouteQuote],
        dict[str, CacheObservation],
    ]:
        quotes = self.quote_source.quotes_for(
            session.question_id, session.physical_design_id,
        )
        if not isinstance(quotes, Mapping):
            raise RouteActionBridgeError("verified quote source is invalid")
        cache = {}
        if session.physical_design_id in {"D_cache", "D_joint"}:
            if self.cache_reader is None:
                raise RouteActionBridgeError(
                    "cache design requires a verified live cache reader"
                )
            for node, episode in self.bridge.cache_episode_ids(
                session.question_id
            ).items():
                cache[node] = self.cache_reader.observe(
                    question_id=session.question_id,
                    object_id=session.object_id,
                    executor_node_id=node,
                    cache_episode_id=episode,
                )
        return quotes, cache

    def list_route_offers(self, session_id: str) -> dict[str, Any]:
        """MCP tool: show only public route/action/quote information."""
        session = self._session(session_id)
        quotes, cache = self._inputs(session)
        offer_set = self.bridge.list_route_offers(
            question_id=session.question_id,
            physical_design_id=session.physical_design_id,
            quotes=quotes, cache=cache,
        )
        return {"session_id": session_id, **asdict(offer_set)}

    def commit_route_choice(
        self, session_id: str, action_id: str,
        offer_set_sha256: str,
    ) -> dict[str, Any]:
        """MCP tool: atomically commit one action without revealing a trial."""
        session = self._session(session_id)
        quotes, cache = self._inputs(session)
        choice = self.bridge.commit_choice(
            session_id=session_id,
            question_id=session.question_id,
            physical_design_id=session.physical_design_id,
            action_id=action_id,
            offer_set_sha256=offer_set_sha256,
            quotes=quotes, cache=cache,
        )
        return {
            "session_id": session_id,
            "action_id": choice.action_id,
            "choice_sha256": choice.choice_sha256,
            "offer_set_sha256": choice.offer_set_sha256,
            "status": "COMMITTED",
        }

    def handoff_for_session(
        self, session_id: str, bound_trial: Mapping[str, Any],
    ) -> RouteHandoff:
        """Runner-only handoff after canonical admission and live preflight."""
        self._session(session_id)
        choice = self.bridge.load_choice(session_id)
        return self.bridge.handoff(choice, bound_trial)
