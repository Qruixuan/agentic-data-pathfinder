from __future__ import annotations

import random
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, Protocol

from ...data_agent import LocalDataAgent
from ...models import AccessOffer, SystemConfig
from ...resolver import AccessResolver
from .contracts import FlowMeshAgentRunRequest


class GatewayError(RuntimeError):
    """Base error for the Pathfinder access gateway."""


class SessionNotFoundError(GatewayError):
    """Raised when a FlowMesh tool references an unknown session."""


class TelemetryIncompleteError(GatewayError):
    """Raised when transfer telemetry is not final at reconciliation time.

    Byte and latency figures are only research observations once every
    transfer attributed to the access is durably counted. A backend that hands
    back a summary with transfers still in flight is reporting a lower bound,
    not a measurement, so the gateway refuses to persist it.
    """

    def __init__(
        self,
        access_id: str,
        in_flight_request_count: int | None,
        *,
        missing_fields: tuple[str, ...] = (),
    ):
        if missing_fields:
            detail = (
                "the backend cannot report completeness (missing "
                f"{', '.join(missing_fields)}), so the summary may be "
                "truncated without saying so"
            )
        elif in_flight_request_count is None:
            detail = "the backend did not report an in-flight transfer count"
        elif in_flight_request_count == 0:
            detail = (
                "no transfer is in flight now, but transfer activity changed "
                "while the summary was read, so it is not a stable snapshot"
            )
        else:
            detail = f"{in_flight_request_count} transfer(s) still in flight"
        super().__init__(
            f"Data Agent telemetry for access {access_id} is not final: "
            f"{detail}"
        )
        self.access_id = access_id
        self.in_flight_request_count = in_flight_request_count
        self.missing_fields = missing_fields


@dataclass(frozen=True)
class GatewaySession:
    session_id: str
    trial_id: str
    question: str
    design_id: str
    task_class_id: str
    quote_profile_id: str
    latency_multiplier: float
    seed: int
    price_universe_version: str
    status: str
    flowmesh_workflow_id: str | None = None
    flowmesh_task_id: str | None = None
    final_answer: str | None = None
    object_id: str | None = None


@dataclass(frozen=True)
class GatewayAccessEvent:
    event_id: int | None
    session_id: str
    event_index: int
    representation_id: str
    quoted_price: float | None
    accepted: bool
    rejection_reason: str | None
    felt_latency_ms: float | None
    realized_cost: float
    bytes_read: int
    location: str | None
    content_sha256: str | None
    created_at: str
    object_id: str | None = None
    data_agent_access_id: str | None = None
    artifact_bytes_sent: int = 0
    artifact_transfer_latency_ms: float = 0.0
    artifact_download_request_count: int = 0
    artifact_full_download_count: int = 0
    object_catalog_version: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BackendAccessResult:
    content: str
    felt_latency_ms: float
    realized_cost: float
    bytes_read: int
    location: str
    payload: dict[str, Any] | None = None
    content_sha256: str | None = None
    data_agent_access_id: str | None = None
    object_catalog_version: str | None = None


class RepresentationBackend(Protocol):
    def access(
        self,
        *,
        config: SystemConfig,
        session: GatewaySession,
        representation_id: str,
        event_index: int,
    ) -> BackendAccessResult:
        """Execute one representation access."""


class EmulatedRepresentationBackend:
    """Uses the existing local physical-path emulator behind gateway tools."""

    def __init__(
        self,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._sleep = sleep

    def access(
        self,
        *,
        config: SystemConfig,
        session: GatewaySession,
        representation_id: str,
        event_index: int,
    ) -> BackendAccessResult:
        design = config.designs[session.design_id]
        representation = config.representations[representation_id]
        rng = random.Random(session.seed + event_index * 1_000_003)
        execution = LocalDataAgent().serve(
            design=design,
            representation=representation,
            latency_multiplier=session.latency_multiplier,
            rng=rng,
        )
        self._sleep(execution.felt_latency_ms / 1_000.0)
        content = (
            f"Synthetic placeholder for {representation.id}. "
            f"Description: {representation.description}. "
            "Replace EmulatedRepresentationBackend with a real video, "
            "embedding, or digest backend before collecting research results."
        )
        return BackendAccessResult(
            content=content,
            felt_latency_ms=execution.felt_latency_ms,
            realized_cost=execution.realized_cost,
            bytes_read=execution.bytes_read,
            location=execution.location,
        )


class SQLiteSessionStore:
    """Persistent state shared by the experiment runner and MCP process."""

    def __init__(self, path: str | Path):
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._connection() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS gateway_sessions (
                    session_id TEXT PRIMARY KEY,
                    trial_id TEXT NOT NULL,
                    question TEXT NOT NULL,
                    design_id TEXT NOT NULL,
                    task_class_id TEXT NOT NULL,
                    quote_profile_id TEXT NOT NULL,
                    latency_multiplier REAL NOT NULL,
                    seed INTEGER NOT NULL,
                    price_universe_version TEXT NOT NULL,
                    status TEXT NOT NULL,
                    flowmesh_workflow_id TEXT,
                    flowmesh_task_id TEXT,
                    final_answer TEXT,
                    object_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS gateway_access_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    event_index INTEGER NOT NULL,
                    representation_id TEXT NOT NULL,
                    quoted_price REAL,
                    accepted INTEGER NOT NULL,
                    rejection_reason TEXT,
                    felt_latency_ms REAL,
                    realized_cost REAL NOT NULL,
                    bytes_read INTEGER NOT NULL,
                    location TEXT,
                    content_sha256 TEXT,
                    object_id TEXT,
                    data_agent_access_id TEXT,
                    artifact_bytes_sent INTEGER NOT NULL DEFAULT 0,
                    artifact_transfer_latency_ms REAL NOT NULL DEFAULT 0,
                    artifact_download_request_count INTEGER NOT NULL DEFAULT 0,
                    artifact_full_download_count INTEGER NOT NULL DEFAULT 0,
                    object_catalog_version TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(session_id)
                        REFERENCES gateway_sessions(session_id)
                );

                CREATE INDEX IF NOT EXISTS idx_gateway_events_session
                    ON gateway_access_events(session_id, event_index);
                """
            )
            session_columns = {
                row["name"]
                for row in connection.execute(
                    "PRAGMA table_info(gateway_sessions)"
                ).fetchall()
            }
            if "object_id" not in session_columns:
                connection.execute(
                    "ALTER TABLE gateway_sessions ADD COLUMN object_id TEXT"
                )
            event_columns = {
                row["name"]
                for row in connection.execute(
                    "PRAGMA table_info(gateway_access_events)"
                ).fetchall()
            }
            migrations = {
                "object_id": "TEXT",
                "data_agent_access_id": "TEXT",
                "artifact_bytes_sent": "INTEGER NOT NULL DEFAULT 0",
                "artifact_transfer_latency_ms": "REAL NOT NULL DEFAULT 0",
                "artifact_download_request_count": "INTEGER NOT NULL DEFAULT 0",
                "artifact_full_download_count": "INTEGER NOT NULL DEFAULT 0",
                "object_catalog_version": "TEXT",
            }
            for name, declaration in migrations.items():
                if name not in event_columns:
                    connection.execute(
                        f"ALTER TABLE gateway_access_events ADD COLUMN "
                        f"{name} {declaration}"
                    )

    def create_session(self, session: GatewaySession) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO gateway_sessions (
                    session_id, trial_id, question, design_id,
                    task_class_id, quote_profile_id, latency_multiplier,
                    seed, price_universe_version, status,
                    flowmesh_workflow_id, flowmesh_task_id, final_answer,
                    object_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session.session_id,
                    session.trial_id,
                    session.question,
                    session.design_id,
                    session.task_class_id,
                    session.quote_profile_id,
                    session.latency_multiplier,
                    session.seed,
                    session.price_universe_version,
                    session.status,
                    session.flowmesh_workflow_id,
                    session.flowmesh_task_id,
                    session.final_answer,
                    session.object_id,
                    now,
                    now,
                ),
            )

    def get_session(self, session_id: str) -> GatewaySession:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT * FROM gateway_sessions WHERE session_id = ?",
                (session_id,),
            ).fetchone()
        if row is None:
            raise SessionNotFoundError(f"unknown Pathfinder session: {session_id}")
        return GatewaySession(
            session_id=row["session_id"],
            trial_id=row["trial_id"],
            question=row["question"],
            design_id=row["design_id"],
            task_class_id=row["task_class_id"],
            quote_profile_id=row["quote_profile_id"],
            latency_multiplier=row["latency_multiplier"],
            seed=row["seed"],
            price_universe_version=row["price_universe_version"],
            status=row["status"],
            flowmesh_workflow_id=row["flowmesh_workflow_id"],
            flowmesh_task_id=row["flowmesh_task_id"],
            final_answer=row["final_answer"],
            object_id=row["object_id"],
        )

    def bind_flowmesh(
        self,
        session_id: str,
        workflow_id: str,
        task_id: str,
    ) -> None:
        self._update_session(
            session_id,
            status="RUNNING",
            flowmesh_workflow_id=workflow_id,
            flowmesh_task_id=task_id,
        )

    def finish_session(
        self,
        session_id: str,
        *,
        status: str,
        final_answer: str | None,
    ) -> None:
        self._update_session(
            session_id,
            status=status,
            final_answer=final_answer,
        )

    def _update_session(self, session_id: str, **values: Any) -> None:
        if not values:
            return
        values["updated_at"] = datetime.now(timezone.utc).isoformat()
        assignments = ", ".join(f"{name} = ?" for name in values)
        parameters = [*values.values(), session_id]
        with self._connection() as connection:
            cursor = connection.execute(
                f"UPDATE gateway_sessions SET {assignments} "
                "WHERE session_id = ?",
                parameters,
            )
            if cursor.rowcount != 1:
                raise SessionNotFoundError(
                    f"unknown Pathfinder session: {session_id}"
                )

    def append_event(self, event: GatewayAccessEvent) -> GatewayAccessEvent:
        with self._connection() as connection:
            cursor = connection.execute(
                """
                INSERT INTO gateway_access_events (
                    session_id, event_index, representation_id,
                    quoted_price, accepted, rejection_reason,
                    felt_latency_ms, realized_cost, bytes_read,
                    location, content_sha256, object_id,
                    data_agent_access_id, artifact_bytes_sent,
                    artifact_transfer_latency_ms,
                    artifact_download_request_count,
                    artifact_full_download_count, object_catalog_version,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.session_id,
                    event.event_index,
                    event.representation_id,
                    event.quoted_price,
                    int(event.accepted),
                    event.rejection_reason,
                    event.felt_latency_ms,
                    event.realized_cost,
                    event.bytes_read,
                    event.location,
                    event.content_sha256,
                    event.object_id,
                    event.data_agent_access_id,
                    event.artifact_bytes_sent,
                    event.artifact_transfer_latency_ms,
                    event.artifact_download_request_count,
                    event.artifact_full_download_count,
                    event.object_catalog_version,
                    event.created_at,
                ),
            )
            event_id = int(cursor.lastrowid)
        return GatewayAccessEvent(**{**asdict(event), "event_id": event_id})

    def list_events(self, session_id: str) -> list[GatewayAccessEvent]:
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM gateway_access_events
                WHERE session_id = ?
                ORDER BY event_index, event_id
                """,
                (session_id,),
            ).fetchall()
        return [
            GatewayAccessEvent(
                event_id=row["event_id"],
                session_id=row["session_id"],
                event_index=row["event_index"],
                representation_id=row["representation_id"],
                quoted_price=row["quoted_price"],
                accepted=bool(row["accepted"]),
                rejection_reason=row["rejection_reason"],
                felt_latency_ms=row["felt_latency_ms"],
                realized_cost=row["realized_cost"],
                bytes_read=row["bytes_read"],
                location=row["location"],
                content_sha256=row["content_sha256"],
                created_at=row["created_at"],
                object_id=row["object_id"],
                data_agent_access_id=row["data_agent_access_id"],
                artifact_bytes_sent=row["artifact_bytes_sent"],
                artifact_transfer_latency_ms=row[
                    "artifact_transfer_latency_ms"
                ],
                artifact_download_request_count=row[
                    "artifact_download_request_count"
                ],
                artifact_full_download_count=row[
                    "artifact_full_download_count"
                ],
                object_catalog_version=row["object_catalog_version"],
            )
            for row in rows
        ]

    def update_artifact_telemetry(
        self,
        event_id: int,
        *,
        bytes_sent: int,
        transfer_latency_ms: float,
        download_request_count: int,
        full_download_count: int,
    ) -> None:
        with self._connection() as connection:
            cursor = connection.execute(
                """
                UPDATE gateway_access_events
                SET artifact_bytes_sent = ?,
                    artifact_transfer_latency_ms = ?,
                    artifact_download_request_count = ?,
                    artifact_full_download_count = ?
                WHERE event_id = ?
                """,
                (
                    bytes_sent,
                    transfer_latency_ms,
                    download_request_count,
                    full_download_count,
                    event_id,
                ),
            )
            if cursor.rowcount != 1:
                raise GatewayError(f"unknown gateway event: {event_id}")


class AccessGateway:
    """Owns session offers, budgets, physical access, and experiment logs."""

    def __init__(
        self,
        config: SystemConfig,
        store: SQLiteSessionStore,
        backend: RepresentationBackend | None = None,
    ):
        self.config = config
        self.store = store
        self.backend = backend or EmulatedRepresentationBackend()
        self._lock = threading.RLock()

    def register_session(
        self,
        request: FlowMeshAgentRunRequest,
    ) -> GatewaySession:
        self._validate_request(request)
        session_id = request.session_id or str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                (
                    f"{request.trial_id}:{request.design_id}:"
                    f"{request.task_class_id}:{request.quote_profile_id}:"
                    f"{request.latency_multiplier}:{request.seed}:"
                    f"{request.object_id or ''}"
                ),
            )
        )
        session = GatewaySession(
            session_id=session_id,
            trial_id=request.trial_id,
            question=request.question,
            design_id=request.design_id,
            task_class_id=request.task_class_id,
            quote_profile_id=request.quote_profile_id,
            latency_multiplier=request.latency_multiplier,
            seed=request.seed,
            price_universe_version=self.config.price_universe_version,
            status="CREATED",
            object_id=request.object_id,
        )
        self.store.create_session(session)
        return session

    def _validate_request(self, request: FlowMeshAgentRunRequest) -> None:
        if not request.question.strip():
            raise ValueError("FlowMesh agent question cannot be empty")
        if request.design_id not in self.config.designs:
            raise ValueError(f"unknown design: {request.design_id}")
        if request.task_class_id not in self.config.task_classes:
            raise ValueError(f"unknown task class: {request.task_class_id}")
        if request.quote_profile_id not in self.config.quote_profiles:
            raise ValueError(
                f"unknown quote profile: {request.quote_profile_id}"
            )
        if request.latency_multiplier <= 0:
            raise ValueError("latency_multiplier must be positive")
        if request.object_id is not None and (
            not isinstance(request.object_id, str)
            or not request.object_id.strip()
        ):
            raise ValueError("object_id must be a non-empty string or null")

    def list_offers(self, session_id: str) -> dict[str, Any]:
        with self._lock:
            session = self.store.get_session(session_id)
            design = self.config.designs[session.design_id]
            task = self.config.task_classes[session.task_class_id]
            profile = self.config.quote_profiles[session.quote_profile_id]
            base_offers, unavailable = AccessResolver(
                self.config
            ).build_offers(design, task, profile)
            events = self.store.list_events(session_id)
            accepted = [event for event in events if event.accepted]
            spent = sum(
                event.quoted_price or 0.0
                for event in accepted
            )
            remaining_budget = max(0.0, task.access_budget - spent)
            slots_remaining = max(0, task.max_accesses - len(accepted))
            offers = [
                self._public_offer(
                    offer,
                    remaining_budget=remaining_budget,
                    slots_remaining=slots_remaining,
                )
                for offer in base_offers
            ]
            return {
                "session_id": session_id,
                "object_id": session.object_id,
                "task_class_id": task.id,
                "price_universe_version": session.price_universe_version,
                "remaining_budget": remaining_budget,
                "access_slots_remaining": slots_remaining,
                "offers": offers,
                "unavailable_representations": unavailable,
            }

    def _public_offer(
        self,
        offer: AccessOffer,
        *,
        remaining_budget: float,
        slots_remaining: int,
    ) -> dict[str, Any]:
        representation = self.config.representations[
            offer.representation_id
        ]
        return {
            "representation_id": offer.representation_id,
            "description": representation.description,
            "quoted_price": offer.quoted_price,
            "affordable": (
                slots_remaining > 0
                and offer.quoted_price <= remaining_budget
            ),
        }

    def access_representation(
        self,
        session_id: str,
        representation_id: str,
    ) -> dict[str, Any]:
        with self._lock:
            session = self.store.get_session(session_id)
            event_index = len(self.store.list_events(session_id))
            if session.status not in {"CREATED", "RUNNING"}:
                return self._reject(
                    session,
                    event_index,
                    representation_id,
                    None,
                    "session_not_active",
                )
            state = self.list_offers(session_id)
            offer = next(
                (
                    candidate
                    for candidate in state["offers"]
                    if candidate["representation_id"] == representation_id
                ),
                None,
            )
            if offer is None:
                return self._reject(
                    session,
                    event_index,
                    representation_id,
                    None,
                    "representation_not_offered",
                )
            if not offer["affordable"]:
                reason = (
                    "access_limit_reached"
                    if state["access_slots_remaining"] <= 0
                    else "insufficient_budget"
                )
                return self._reject(
                    session,
                    event_index,
                    representation_id,
                    float(offer["quoted_price"]),
                    reason,
                )

            result = self.backend.access(
                config=self.config,
                session=session,
                representation_id=representation_id,
                event_index=event_index,
            )
            content_hash = result.content_sha256 or sha256(
                result.content.encode("utf-8")
            ).hexdigest()
            event = self.store.append_event(
                GatewayAccessEvent(
                    event_id=None,
                    session_id=session_id,
                    event_index=event_index,
                    representation_id=representation_id,
                    quoted_price=float(offer["quoted_price"]),
                    accepted=True,
                    rejection_reason=None,
                    felt_latency_ms=result.felt_latency_ms,
                    realized_cost=result.realized_cost,
                    bytes_read=result.bytes_read,
                    location=result.location,
                    content_sha256=content_hash,
                    created_at=datetime.now(timezone.utc).isoformat(),
                    object_id=session.object_id,
                    data_agent_access_id=result.data_agent_access_id,
                    object_catalog_version=result.object_catalog_version,
                )
            )
            updated_state = self.list_offers(session_id)
            response = {
                "ok": True,
                "session_id": session_id,
                "object_id": session.object_id,
                "representation_id": representation_id,
                "quoted_price_charged": event.quoted_price,
                "remaining_budget": updated_state["remaining_budget"],
                "access_slots_remaining": updated_state[
                    "access_slots_remaining"
                ],
                "felt_latency_ms": result.felt_latency_ms,
                "content": result.content,
            }
            if result.payload is not None:
                response["payload"] = result.payload
            return response

    def _reject(
        self,
        session: GatewaySession,
        event_index: int,
        representation_id: str,
        quoted_price: float | None,
        reason: str,
    ) -> dict[str, Any]:
        self.store.append_event(
            GatewayAccessEvent(
                event_id=None,
                session_id=session.session_id,
                event_index=event_index,
                representation_id=representation_id,
                quoted_price=quoted_price,
                accepted=False,
                rejection_reason=reason,
                felt_latency_ms=None,
                realized_cost=0.0,
                bytes_read=0,
                location=None,
                content_sha256=None,
                created_at=datetime.now(timezone.utc).isoformat(),
                object_id=session.object_id,
            )
        )
        return {
            "ok": False,
            "session_id": session.session_id,
            "representation_id": representation_id,
            "error": reason,
        }

    def get_session_state(self, session_id: str) -> dict[str, Any]:
        state = self.list_offers(session_id)
        state["events"] = [
            {
                "event_index": event.event_index,
                "representation_id": event.representation_id,
                "quoted_price": event.quoted_price,
                "accepted": event.accepted,
                "rejection_reason": event.rejection_reason,
                "felt_latency_ms": event.felt_latency_ms,
                "object_id": event.object_id,
                "artifact_bytes_sent": event.artifact_bytes_sent,
                "artifact_transfer_latency_ms": (
                    event.artifact_transfer_latency_ms
                ),
                "artifact_download_request_count": (
                    event.artifact_download_request_count
                ),
                "artifact_full_download_count": (
                    event.artifact_full_download_count
                ),
                "object_catalog_version": event.object_catalog_version,
            }
            for event in self.store.list_events(session_id)
        ]
        return state

    def reconcile_artifact_telemetry(
        self,
        session_id: str,
    ) -> list[GatewayAccessEvent]:
        """Fold final Data Agent transfer telemetry into the session events.

        Collection is fully separated from persistence. Every access is read
        and validated first, so an incomplete or mismatched summary for any
        one access aborts before a single event is written; no telemetry
        failure can leave some events carrying reconciled figures and others
        carrying zeros.

        That all-or-nothing property covers telemetry completeness only. The
        write-back loop is one transaction per event rather than one per
        session, so a store failure part way through it can still leave
        earlier events updated.

        Raises :class:`TelemetryIncompleteError` when a summary still has
        transfers in flight or cannot report whether it does, and propagates
        whatever the backend raises when it cannot obtain a final summary at
        all.
        """
        self.store.get_session(session_id)
        getter = getattr(self.backend, "get_access_telemetry", None)
        if not callable(getter):
            return self.store.list_events(session_id)

        pending: list[tuple[int, Any]] = []
        for event in self.store.list_events(session_id):
            if event.event_id is None or event.data_agent_access_id is None:
                continue
            telemetry = getter(event.data_agent_access_id)
            # Checked here as well as in the client, because the gateway is
            # the component that actually writes the research observation and
            # must not depend on some other backend having enforced this.
            # A backend that cannot answer the question is treated as
            # incomplete: unverifiable completeness is not completeness, and
            # a legacy summary that merely looks settled is exactly the silent
            # under-count this contract exists to prevent.
            missing = getattr(
                telemetry,
                "missing_completeness_fields",
                ("in_flight_request_count", "telemetry_complete"),
            )
            if missing:
                raise TelemetryIncompleteError(
                    event.data_agent_access_id,
                    getattr(telemetry, "in_flight_request_count", None),
                    missing_fields=tuple(missing),
                )
            # Use the combined verdict, not the raw counter: a transfer that
            # started and finished while the Data Agent was reading its own
            # summary leaves the counter at zero but the snapshot unstable.
            if getattr(telemetry, "telemetry_complete", None) is not True:
                raise TelemetryIncompleteError(
                    event.data_agent_access_id,
                    getattr(telemetry, "in_flight_request_count", None),
                )
            if (
                telemetry.object_id != event.object_id
                or telemetry.representation_id != event.representation_id
                or telemetry.object_catalog_version
                != event.object_catalog_version
            ):
                raise GatewayError(
                    "Data Agent telemetry does not match the gateway event: "
                    f"{event.data_agent_access_id}"
                )
            pending.append((event.event_id, telemetry))

        for event_id, telemetry in pending:
            self.store.update_artifact_telemetry(
                event_id,
                bytes_sent=telemetry.bytes_sent,
                transfer_latency_ms=telemetry.transfer_latency_ms,
                download_request_count=telemetry.download_request_count,
                full_download_count=telemetry.full_download_count,
            )
        return self.store.list_events(session_id)
