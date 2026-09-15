"""Strict N7/N8 HTTP coordinator for FlowMesh W4 candidate trials.

One request represents one complete W4 trial.  The coordinator executes the
already-frozen N2/index, N3/N4/Data-Agent, N7/N8/cache, and N6/ranking route
through ``LiveW4CandidateOperationExecutor``.  Runtime endpoints and secrets
are injected as live objects and never enter the response or health document.

The service owns an exclusive fresh cache namespace for one frozen run.  Its
SQLite journal makes a completed request byte-replayable after restart while
ambiguous RUNNING or FAILED requests remain fail-closed.  This is component
and orchestration conformance, not cloud performance or monetary evidence.
"""

from __future__ import annotations

import hmac
import json
import os
import re
import sqlite3
import threading
import uuid
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import urlsplit

from ..integrations.flowmesh.w4_candidate_matrix import (
    FLOWMESH_W4_RESPONSE_SCHEMA_VERSION,
    W4_COORDINATOR_ENDPOINT_PATH,
    build_flowmesh_w4_trial_request,
    validate_flowmesh_w4_trial_request,
    validate_flowmesh_w4_trial_response,
)
from ._full_flow_primitives import (
    LOWER_SHA256_PATTERN as _SHA256,
    canonical_json_bytes,
    sha256_hex,
    strict_json_loads,
)
from .container_node import (
    FULL_FLOW_INGRESS_SIGNATURE_HEADER,
    full_flow_request_hmac_sha256,
)
from .full_flow_w4_candidate_coordinator import (
    W4CandidateOperationExecutor,
    _cache_key,
    _execute_trial,
)
from .full_flow_w4_candidate_routes import (
    load_full_flow_w4_candidate_route_inputs,
)
from .full_flow_w4_live_executor import LiveW4CandidateOperationExecutor
from .full_flow_w4_local_factory import (
    W4LocalRuntimeInputs,
    build_local_w4_live_components,
)


W4_FLOWMESH_COORDINATOR_HEALTH_SCHEMA_VERSION = (
    "pathfinder.flowmesh-w4-coordinator-health/v1alpha2"
)
_MAX_REQUEST_BYTES = 2 * 1024 * 1024
_MAX_RESPONSE_BYTES = 16 * 1024 * 1024
class FullFlowW4FlowMeshServiceError(RuntimeError):
    """Raised when the route service cannot preserve its frozen contract."""


class _Unauthorized(FullFlowW4FlowMeshServiceError):
    pass


class _Conflict(FullFlowW4FlowMeshServiceError):
    pass


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowW4FlowMeshServiceError(message)


def _canonical(value: Any) -> bytes:
    return canonical_json_bytes(
        value,
        error_type=FullFlowW4FlowMeshServiceError,
        error_message="W4 coordinator value is not canonical JSON",
    )


def _sha256(value: bytes) -> str:
    return sha256_hex(value)


def _strict_json(
    raw: bytes,
    *,
    maximum_bytes: int = _MAX_REQUEST_BYTES,
) -> dict[str, Any]:
    _require(len(raw) <= maximum_bytes, "W4 JSON exceeds its byte limit")

    try:
        value = strict_json_loads(
            raw,
            error_type=FullFlowW4FlowMeshServiceError,
            duplicate_key_message=lambda _key: "W4 request repeats a JSON key",
            nonfinite_number_message=(
                lambda token: f"W4 request contains non-finite number {token}"
            ),
        )
    except FullFlowW4FlowMeshServiceError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowW4FlowMeshServiceError("W4 request is invalid JSON") from exc
    _require(isinstance(value, dict), "W4 request must be an object")
    return value


class FullFlowW4FlowMeshCoordinator:
    """Execute one source-bound W4 trial at a time for N7 or N8."""

    def __init__(
        self,
        *,
        coordinator_node_id: str,
        route_package_dir: str | Path,
        executor: W4CandidateOperationExecutor,
        cache_health: Callable[[], Mapping[str, Any]],
        state_db: str | Path,
    ) -> None:
        _require(
            coordinator_node_id in {"N7", "N8"},
            "W4 coordinator node must be N7 or N8",
        )
        _require(
            isinstance(executor, W4CandidateOperationExecutor),
            "W4 coordinator executor is invalid",
        )
        _require(callable(cache_health), "cache health provider is required")
        self.node_id = coordinator_node_id
        self._route_root = Path(route_package_dir).resolve()
        self._source = load_full_flow_w4_candidate_route_inputs(self._route_root)
        self._executor = executor
        self._evidence_class = getattr(executor, "evidence_class", None)
        _require(
            self._evidence_class in {
                "live-local-component-execution",
                "strict-fake-component-conformance",
            },
            "W4 executor evidence class is invalid",
        )
        self._cache_health = cache_health
        self._state_db = Path(state_db).resolve()
        _require(
            not self._state_db.is_symlink(),
            "W4 coordinator state database cannot be a symlink",
        )
        self._state_db.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._trial_by_key = {
            str(row["trial_key"]): row
            for row in self._source.trials
            if row["executor_node_id"] == self.node_id
        }
        self._ordered_trial_keys = [
            str(row["trial_key"])
            for row in sorted(
                self._trial_by_key.values(),
                key=lambda item: int(item["order_index"]),
            )
        ]
        _require(
            len(self._ordered_trial_keys) == 8,
            "W4 coordinator must own exactly eight trials",
        )
        self._operations_by_trial: dict[str, list[Mapping[str, Any]]] = {
            key: [] for key in self._ordered_trial_keys
        }
        self._operation_by_key: dict[str, Mapping[str, Any]] = {}
        for operation in self._source.operations:
            key = str(operation["operation_key"])
            self._operation_by_key[key] = operation
            trial_key = str(operation["trial_key"])
            if trial_key in self._operations_by_trial:
                self._operations_by_trial[trial_key].append(operation)
        self._cache = {"N7": set(), "N8": set()}
        self._service_epoch = ""
        self._initialize_state()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._state_db, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _initialize_state(self) -> None:
        with closing(self._connect()) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    name TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS executions (
                    idempotency_key TEXT PRIMARY KEY,
                    request_sha256 TEXT NOT NULL UNIQUE,
                    trial_key TEXT NOT NULL UNIQUE,
                    order_index INTEGER NOT NULL UNIQUE,
                    status TEXT NOT NULL,
                    response BLOB
                );
                """
            )
            metadata = {
                str(row["name"]): str(row["value"])
                for row in connection.execute(
                    "SELECT name, value FROM metadata ORDER BY name"
                )
            }
            expected = {
                "coordinator_node_id": self.node_id,
                "physical_plan_id": str(self._source.plan["physical_plan_id"]),
                "route_plan_sha256": str(self._source.plan["plan_sha256"]),
            }
            if not metadata:
                health = self._verified_cache_health()
                _require(
                    health["entry_count"] == 0 and health["used_bytes"] == 0,
                    f"{self.node_id} cache namespace is not fresh",
                )
                expected["service_epoch"] = uuid.uuid4().hex
                connection.executemany(
                    "INSERT INTO metadata(name, value) VALUES (?, ?)",
                    sorted(expected.items()),
                )
                connection.commit()
                metadata = expected
            else:
                _require(
                    set(metadata) == set(expected) | {"service_epoch"}
                    and all(metadata[name] == value for name, value in expected.items())
                    and re.fullmatch(r"[0-9a-f]{32}", metadata["service_epoch"])
                    is not None,
                    "W4 coordinator state source binding changed",
                )
            self._service_epoch = metadata["service_epoch"]
            rows = list(
                connection.execute(
                    """
                    SELECT request_sha256, trial_key, order_index, status, response
                    FROM executions ORDER BY order_index
                    """
                )
            )
        _require(
            all(
                row["status"] == "COMPLETE" and row["response"] is not None
                for row in rows
            ),
            "W4 coordinator contains an ambiguous prior execution",
        )
        _require(
            [str(row["trial_key"]) for row in rows]
            == self._ordered_trial_keys[: len(rows)],
            "W4 durable trial sequence is not a frozen-order prefix",
        )
        for row in rows:
            response = _strict_json(
                bytes(row["response"]), maximum_bytes=_MAX_RESPONSE_BYTES
            )
            trial = self._trial_by_key[str(row["trial_key"])]
            stored_trial = response.get("trial_result")
            _require(
                isinstance(stored_trial, Mapping),
                "stored W4 trial result is invalid",
            )
            request = build_flowmesh_w4_trial_request(
                run_id=stored_trial.get("run_id"),
                route_plan=self._source.plan,
                public_task=self._source.public_task,
                trial=trial,
            )
            _require(
                request["request_sha256"] == row["request_sha256"],
                "stored W4 request binding changed",
            )
            response = validate_flowmesh_w4_trial_response(
                response,
                request=request,
            )
            self._restore_cache_from_response(response)
        health = self._verified_cache_health()
        _require(
            health["entry_count"] == len(self._cache[self.node_id]),
            f"{self.node_id} cache occupancy differs from durable coordinator state",
        )

    def _verified_cache_health(self) -> dict[str, Any]:
        value = dict(self._cache_health())
        _require(
            value.get("status") == "ok"
            and value.get("node_id") == self.node_id
            and type(value.get("entry_count")) is int
            and value["entry_count"] >= 0
            and type(value.get("used_bytes")) is int
            and value["used_bytes"] >= 0
            and value.get("credentials_recorded") is False,
            f"{self.node_id} cache health identity is invalid",
        )
        return value

    def _restore_cache_from_response(self, response: Mapping[str, Any]) -> None:
        evidence = response.get("operation_evidence")
        _require(isinstance(evidence, list), "stored W4 evidence is invalid")
        for row in evidence:
            if (
                not isinstance(row, Mapping)
                or row.get("action") != "insert"
                or row.get("execution_status") != "COMPLETED"
            ):
                continue
            operation = self._operation_by_key.get(str(row.get("operation_key")))
            _require(
                operation is not None
                and operation.get("representation_identity") is not None,
                "stored cache insertion lost its source operation",
            )
            identity = operation["representation_identity"]
            self._cache[self.node_id].add(
                _cache_key(self.node_id, str(operation["object_id"]), identity)
            )

    def health(self) -> dict[str, Any]:
        with self._lock:
            cache: dict[str, Any] | None
            try:
                cache = self._verified_cache_health()
            except Exception:
                # Health must fail closed without copying a downstream error,
                # endpoint, or credential into the public response.  A cache
                # health transport failure and an invalid cache identity are
                # therefore intentionally indistinguishable here.
                cache = None
            with closing(self._connect()) as connection:
                row = connection.execute(
                    """
                    SELECT COUNT(*) AS completed
                    FROM executions WHERE status = 'COMPLETE'
                    """
                ).fetchone()
            completed = int(row["completed"])
            cache_health_verified = cache is not None
            consistent = (
                cache is not None
                and cache["entry_count"] == len(self._cache[self.node_id])
            )
            runtime_service_contract_id = (
                f"{self.node_id}.w4-candidate-coordinator"
            )
            return {
                "schema_version": W4_FLOWMESH_COORDINATOR_HEALTH_SCHEMA_VERSION,
                "status": "ok" if consistent else "blocked",
                "node_id": self.node_id,
                "coordinator_node_id": self.node_id,
                "runtime_service_contract_id": runtime_service_contract_id,
                "service_epoch": self._service_epoch,
                "physical_plan_id": self._source.plan["physical_plan_id"],
                "route_plan_sha256": self._source.plan["plan_sha256"],
                "owned_trial_count": len(self._ordered_trial_keys),
                "completed_trial_count": completed,
                "next_trial_key": (
                    self._ordered_trial_keys[completed]
                    if completed < len(self._ordered_trial_keys)
                    else None
                ),
                "cache_entry_count": (
                    cache["entry_count"] if cache is not None else None
                ),
                "cache_health_verified": cache_health_verified,
                "cache_state_consistent": consistent,
                "evidence_class": self._evidence_class,
                "durable_completed_response_replay": True,
                "ambiguous_execution_recovery": False,
                "endpoint_values_included": False,
                "credentials_recorded": False,
                "eligible_for_scientific_claims": False,
            }

    def execute(self, raw_request: Mapping[str, Any]) -> dict[str, Any]:
        request = validate_flowmesh_w4_trial_request(
            raw_request,
            route_package_dir=self._route_root,
        )
        _require(
            request["coordinator_node_id"] == self.node_id,
            "W4 request targeted the wrong coordinator node",
        )
        request_bytes = _canonical(request)
        with self._lock:
            with closing(self._connect()) as connection:
                existing = connection.execute(
                    """
                    SELECT request_sha256, status, response
                    FROM executions WHERE idempotency_key = ?
                    """,
                    (request["idempotency_key"],),
                ).fetchone()
                if existing is not None:
                    _require(
                        existing["request_sha256"] == request["request_sha256"],
                        "W4 idempotency key was reused for different content",
                    )
                    if existing["status"] == "COMPLETE" and existing["response"]:
                        replay = _strict_json(
                            bytes(existing["response"]),
                            maximum_bytes=_MAX_RESPONSE_BYTES,
                        )
                        _require(
                            _canonical(replay) == bytes(existing["response"]),
                            "stored W4 response is not canonical",
                        )
                        return validate_flowmesh_w4_trial_response(
                            replay,
                            request=request,
                        )
                    raise _Conflict("W4 request has ambiguous prior execution state")
                completed = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM executions WHERE status = 'COMPLETE'"
                    ).fetchone()[0]
                )
                _require(
                    completed < len(self._ordered_trial_keys)
                    and request["trial_key"] == self._ordered_trial_keys[completed],
                    "W4 request violates the frozen cache-preserving order",
                )
                cache_health = self._verified_cache_health()
                _require(
                    cache_health["entry_count"] == len(self._cache[self.node_id]),
                    "W4 external cache changed outside the coordinator",
                )
                connection.execute(
                    """
                    INSERT INTO executions(
                        idempotency_key, request_sha256, trial_key,
                        order_index, status, response
                    ) VALUES (?, ?, ?, ?, 'RUNNING', NULL)
                    """,
                    (
                        request["idempotency_key"],
                        request["request_sha256"],
                        request["trial_key"],
                        request["order_index"],
                    ),
                )
                connection.commit()
            before_event_count = len(getattr(self._executor, "events", ()))
            try:
                trial, evidence, observation = _execute_trial(
                    run_id=request["run_id"],
                    source=self._source,
                    trial=self._trial_by_key[request["trial_key"]],
                    operations=self._operations_by_trial[request["trial_key"]],
                    executor=self._executor,
                    cache=self._cache,
                )
                all_events = list(getattr(self._executor, "events", ()))
                events = [dict(row) for row in all_events[before_event_count:]]
                _require(
                    len(events)
                    == sum(row["execution_status"] == "COMPLETED" for row in evidence),
                    "W4 component event coverage changed",
                )
                post_cache = self._verified_cache_health()
                _require(
                    post_cache["entry_count"] == len(self._cache[self.node_id]),
                    "W4 cache mutation differs from coordinator evidence",
                )
                response: dict[str, Any] = {
                    "schema_version": FLOWMESH_W4_RESPONSE_SCHEMA_VERSION,
                    "status": "COMPLETE",
                    "request_sha256": request["request_sha256"],
                    "coordinator_node_id": self.node_id,
                    "route_plan_sha256": request["route_plan_sha256"],
                    "evidence_class": self._evidence_class,
                    "trial_result": trial,
                    "operation_evidence": evidence,
                    "observation": observation,
                    "component_events": events,
                    "component_events_sha256": _sha256(
                        b"".join(_canonical(row) + b"\n" for row in events)
                    ),
                    "hidden_relevance_values_read": False,
                    "endpoint_values_included": False,
                    "credentials_recorded": False,
                    "eligible_for_scientific_claims": False,
                }
                response["response_sha256"] = _sha256(_canonical(response))
                response = validate_flowmesh_w4_trial_response(
                    response,
                    request=request,
                )
                response_bytes = _canonical(response)
                _require(
                    len(response_bytes) <= _MAX_RESPONSE_BYTES,
                    "W4 response exceeds its byte limit",
                )
            except Exception:
                with closing(self._connect()) as connection:
                    connection.execute(
                        "UPDATE executions SET status = 'FAILED' "
                        "WHERE request_sha256 = ?",
                        (request["request_sha256"],),
                    )
                    connection.commit()
                raise
            with closing(self._connect()) as connection:
                connection.execute(
                    """
                    UPDATE executions SET status = 'COMPLETE', response = ?
                    WHERE request_sha256 = ? AND status = 'RUNNING'
                    """,
                    (response_bytes, request["request_sha256"]),
                )
                _require(connection.total_changes == 1, "W4 response commit failed")
                connection.commit()
            return response


def build_local_full_flow_w4_flowmesh_coordinator(
    *,
    coordinator_node_id: str,
    route_package_dir: str | Path,
    crosswalk_dir: str | Path,
    runtime: W4LocalRuntimeInputs,
    state_db: str | Path,
) -> FullFlowW4FlowMeshCoordinator:
    """Bind existing local W4 clients to one N7/N8 coordinator service."""

    components = build_local_w4_live_components(runtime)
    executor = LiveW4CandidateOperationExecutor(
        route_package_dir=route_package_dir,
        crosswalk_dir=crosswalk_dir,
        canonical_index_package_dir=runtime.index_package_dirs["N2"],
        components=components,
        evidence_class="live-local-component-execution",
    )
    cache_adapter = components.caches[coordinator_node_id]
    _require(
        callable(getattr(cache_adapter, "health", None)),
        "W4 cache adapter cannot prove fresh-cache state",
    )
    return FullFlowW4FlowMeshCoordinator(
        coordinator_node_id=coordinator_node_id,
        route_package_dir=route_package_dir,
        executor=executor,
        cache_health=cache_adapter.health,
        state_db=state_db,
    )


class FullFlowW4FlowMeshHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    coordinator: FullFlowW4FlowMeshCoordinator
    hmac_secret: str


class _FullFlowW4FlowMeshHandler(BaseHTTPRequestHandler):
    server: FullFlowW4FlowMeshHTTPServer

    def log_message(self, format: str, *args: object) -> None:
        return

    def _write_json(self, status: int, value: Mapping[str, Any]) -> None:
        payload = _canonical(value) + b"\n"
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:
        parsed = urlsplit(self.path)
        if parsed.path == "/healthz" and not parsed.query:
            health = self.server.coordinator.health()
            self._write_json(
                200 if health.get("status") == "ok" else 503,
                health,
            )
            return
        self._write_json(404, {"status": "error", "message": "not found"})

    def do_POST(self) -> None:
        try:
            parsed = urlsplit(self.path)
            _require(
                parsed.path == W4_COORDINATOR_ENDPOINT_PATH and not parsed.query,
                "not found",
            )
            _require(
                self.headers.get_content_type() == "application/json",
                "W4 request Content-Type must be application/json",
            )
            raw_length = self.headers.get("Content-Length")
            _require(
                isinstance(raw_length, str) and raw_length.isdecimal(),
                "W4 request Content-Length is invalid",
            )
            length = int(raw_length)
            _require(0 < length <= _MAX_REQUEST_BYTES, "W4 request is too large")
            raw = self.rfile.read(length)
            _require(len(raw) == length, "W4 request is truncated")
            request = _strict_json(raw)
            signatures = self.headers.get_all(FULL_FLOW_INGRESS_SIGNATURE_HEADER)
            supplied = signatures[0] if signatures and len(signatures) == 1 else ""
            expected = full_flow_request_hmac_sha256(
                request,
                self.server.hmac_secret,
            )
            if _SHA256.fullmatch(supplied) is None or not hmac.compare_digest(
                supplied,
                expected,
            ):
                raise _Unauthorized("unauthorized")
            self._write_json(200, self.server.coordinator.execute(request))
        except _Unauthorized:
            self._write_json(401, {"status": "error", "message": "unauthorized"})
        except _Conflict as exc:
            self._write_json(409, {"status": "error", "message": str(exc)})
        except (FullFlowW4FlowMeshServiceError, ValueError) as exc:
            self._write_json(400, {"status": "error", "message": str(exc)})
        except Exception:
            self._write_json(
                500,
                {"status": "error", "message": "W4 coordinator internal error"},
            )


def create_full_flow_w4_flowmesh_http_server(
    coordinator: FullFlowW4FlowMeshCoordinator,
    *,
    host: str,
    port: int,
    hmac_secret: str,
) -> FullFlowW4FlowMeshHTTPServer:
    _require(
        isinstance(coordinator, FullFlowW4FlowMeshCoordinator),
        "W4 coordinator type changed",
    )
    _require(isinstance(host, str) and bool(host), "server host is invalid")
    _require(type(port) is int and 0 <= port <= 65535, "server port is invalid")
    full_flow_request_hmac_sha256({}, hmac_secret)
    server = FullFlowW4FlowMeshHTTPServer((host, port), _FullFlowW4FlowMeshHandler)
    server.coordinator = coordinator
    server.hmac_secret = hmac_secret
    return server


__all__ = [
    "FullFlowW4FlowMeshCoordinator",
    "FullFlowW4FlowMeshHTTPServer",
    "FullFlowW4FlowMeshServiceError",
    "W4_FLOWMESH_COORDINATOR_HEALTH_SCHEMA_VERSION",
    "build_local_full_flow_w4_flowmesh_coordinator",
    "create_full_flow_w4_flowmesh_http_server",
]
