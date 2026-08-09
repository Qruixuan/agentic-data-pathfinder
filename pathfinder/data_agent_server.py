from __future__ import annotations

import hmac
import json
import logging
import os
import sqlite3
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, unquote, urlencode, urlparse

from .data_agent_client import (
    DATA_AGENT_API_VERSION,
    DataAgentAccessRequest,
    DataAgentProtocolError,
)
from .data_agent_manifest import (
    DataAgentBindingMismatchError,
    DataAgentManifest,
    DataAgentManifestLookupError,
    ResolvedDataAgentAccess,
    load_data_agent_manifest,
)


logger = logging.getLogger("pathfinder.data_agent")


class DataAgentServerError(RuntimeError):
    """Base error raised by the Data Agent server."""


class DataAgentAuthenticationError(DataAgentServerError):
    """Raised when a control or artifact request is unauthorized."""


class DataAgentIdempotencyConflict(DataAgentServerError):
    """Raised when one access ID is reused for a different request."""


class DataAgentInlinePayloadTooLarge(DataAgentServerError):
    """Raised when an inline artifact exceeds the configured limit."""


class DataAgentArtifactAuthorizationError(DataAgentServerError):
    """Raised when an artifact URL is absent, invalid, or expired."""


class DataAgentRangeNotSatisfiable(DataAgentServerError):
    """Raised when an artifact byte range cannot be served."""


@dataclass(frozen=True)
class DataAgentServerSettings:
    host: str = "0.0.0.0"
    port: int = 8780
    public_base_url: str | None = None
    token: str | None = None
    artifact_secret: str | None = None
    artifact_url_ttl_seconds: int = 300
    max_request_bytes: int = 1024 * 1024
    max_inline_bytes: int = 1024 * 1024

    def __post_init__(self) -> None:
        if not isinstance(self.host, str) or not self.host.strip():
            raise ValueError("Data Agent host cannot be empty")
        if (
            not isinstance(self.port, int)
            or isinstance(self.port, bool)
            or not 0 <= self.port <= 65535
        ):
            raise ValueError("Data Agent port must be between 0 and 65535")
        if self.public_base_url is not None:
            parsed = urlparse(self.public_base_url)
            if (
                parsed.scheme not in {"http", "https"}
                or not parsed.netloc
                or parsed.username is not None
                or parsed.password is not None
            ):
                raise ValueError(
                    "Data Agent public_base_url must be an absolute HTTP(S) "
                    "URL without credentials"
                )
        for name in (
            "artifact_url_ttl_seconds",
            "max_request_bytes",
            "max_inline_bytes",
        ):
            value = getattr(self, name)
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value <= 0
            ):
                raise ValueError(
                    f"Data Agent {name} must be a positive integer"
                )

    @classmethod
    def from_environment(
        cls,
        *,
        host: str = "0.0.0.0",
        port: int = 8780,
        public_base_url: str | None = None,
        artifact_url_ttl_seconds: int = 300,
        max_request_bytes: int = 1024 * 1024,
        max_inline_bytes: int = 1024 * 1024,
    ) -> "DataAgentServerSettings":
        return cls(
            host=host,
            port=port,
            public_base_url=(
                public_base_url
                or os.getenv("PATHFINDER_DATA_AGENT_PUBLIC_BASE_URL")
                or None
            ),
            token=os.getenv("PATHFINDER_DATA_AGENT_TOKEN") or None,
            artifact_secret=(
                os.getenv("PATHFINDER_DATA_AGENT_ARTIFACT_SECRET") or None
            ),
            artifact_url_ttl_seconds=artifact_url_ttl_seconds,
            max_request_bytes=max_request_bytes,
            max_inline_bytes=max_inline_bytes,
        )


@dataclass(frozen=True)
class DataAgentOperation:
    access_id: str
    request_sha256: str
    request_json: str
    response: dict[str, Any]


@dataclass(frozen=True)
class DataAgentArtifactDownload:
    access_id: str
    range_start: int
    range_end: int
    bytes_sent: int
    duration_ms: float
    completed: bool
    full_artifact: bool
    started_at: float
    completed_at: float


class SQLiteDataAgentOperationStore:
    """Persists completed idempotent access operations."""

    def __init__(self, path: str | Path):
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        # journal_mode is a persistent property of the database file, so it
        # is set once at initialization rather than re-asserted on every
        # connection: switching journal mode takes a lock and can itself
        # block on disk, which put avoidable I/O on the request hot path.
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
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
            connection.execute("PRAGMA journal_mode = WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS data_agent_operations (
                    access_id TEXT PRIMARY KEY,
                    request_sha256 TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    response_json TEXT NOT NULL,
                    created_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS data_agent_artifact_downloads (
                    download_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    access_id TEXT NOT NULL,
                    range_start INTEGER NOT NULL,
                    range_end INTEGER NOT NULL,
                    bytes_sent INTEGER NOT NULL,
                    duration_ms REAL NOT NULL,
                    completed INTEGER NOT NULL,
                    full_artifact INTEGER NOT NULL,
                    started_at REAL NOT NULL,
                    completed_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_data_agent_downloads_access
                    ON data_agent_artifact_downloads(access_id, download_id);
                """
            )

    def get(self, access_id: str) -> DataAgentOperation | None:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT * FROM data_agent_operations WHERE access_id = ?",
                (access_id,),
            ).fetchone()
        if row is None:
            return None
        response = json.loads(row["response_json"])
        if not isinstance(response, dict):
            raise DataAgentServerError(
                f"stored response is invalid for access {access_id}"
            )
        return DataAgentOperation(
            access_id=row["access_id"],
            request_sha256=row["request_sha256"],
            request_json=row["request_json"],
            response=response,
        )

    def put_if_absent(
        self,
        operation: DataAgentOperation,
    ) -> DataAgentOperation:
        response_json = json.dumps(
            operation.response,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        try:
            with self._connection() as connection:
                connection.execute(
                    """
                    INSERT INTO data_agent_operations (
                        access_id, request_sha256, request_json,
                        response_json, created_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        operation.access_id,
                        operation.request_sha256,
                        operation.request_json,
                        response_json,
                        time.time(),
                    ),
                )
            return operation
        except sqlite3.IntegrityError:
            existing = self.get(operation.access_id)
            if existing is None:
                raise
            if existing.request_sha256 != operation.request_sha256:
                raise DataAgentIdempotencyConflict(
                    "access_id was already used for a different request"
                )
            return existing

    def count(self) -> int:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM data_agent_operations"
            ).fetchone()
        assert row is not None
        return int(row["count"])

    def record_artifact_download(
        self,
        download: DataAgentArtifactDownload,
    ) -> None:
        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO data_agent_artifact_downloads (
                    access_id, range_start, range_end, bytes_sent,
                    duration_ms, completed, full_artifact,
                    started_at, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    download.access_id,
                    download.range_start,
                    download.range_end,
                    download.bytes_sent,
                    download.duration_ms,
                    int(download.completed),
                    int(download.full_artifact),
                    download.started_at,
                    download.completed_at,
                ),
            )

    def artifact_download_summary(self, access_id: str) -> dict[str, Any]:
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT
                    COUNT(*) AS download_request_count,
                    COALESCE(SUM(completed), 0) AS completed_request_count,
                    COALESCE(SUM(
                        CASE WHEN completed = 1 AND full_artifact = 1
                        THEN 1 ELSE 0 END
                    ), 0) AS full_download_count,
                    COALESCE(SUM(bytes_sent), 0) AS bytes_sent,
                    COALESCE(SUM(duration_ms), 0.0) AS transfer_latency_ms,
                    MAX(
                        CASE WHEN completed = 1 THEN completed_at ELSE NULL END
                    ) AS latest_completed_at
                FROM data_agent_artifact_downloads
                WHERE access_id = ?
                """,
                (access_id,),
            ).fetchone()
        assert row is not None
        return {
            "download_request_count": int(row["download_request_count"]),
            "completed_request_count": int(row["completed_request_count"]),
            "full_download_count": int(row["full_download_count"]),
            "bytes_sent": int(row["bytes_sent"]),
            "transfer_latency_ms": float(row["transfer_latency_ms"]),
            "latest_completed_at": (
                float(row["latest_completed_at"])
                if row["latest_completed_at"] is not None
                else None
            ),
        }


@dataclass
class _LockEntry:
    lock: threading.Lock
    references: int = 0


class _AccessLockPool:
    """Serializes one access ID without serializing unrelated accesses."""

    def __init__(self) -> None:
        self._guard = threading.Lock()
        self._entries: dict[str, _LockEntry] = {}

    @contextmanager
    def acquire(self, access_id: str) -> Iterator[None]:
        with self._guard:
            entry = self._entries.get(access_id)
            if entry is None:
                entry = _LockEntry(threading.Lock())
                self._entries[access_id] = entry
            entry.references += 1
        entry.lock.acquire()
        try:
            yield
        finally:
            entry.lock.release()
            with self._guard:
                entry.references -= 1
                if entry.references == 0:
                    self._entries.pop(access_id, None)


@dataclass(frozen=True)
class DataAgentArtifact:
    access_id: str
    path: Path
    media_type: str
    sha256: str
    size_bytes: int


class DataAgentService:
    """Executes manifest-backed accesses and persists idempotent results."""

    def __init__(
        self,
        manifest: DataAgentManifest,
        store: SQLiteDataAgentOperationStore,
        settings: DataAgentServerSettings,
        public_base_url: str,
        *,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.manifest = manifest
        self.store = store
        self.settings = settings
        self.public_base_url = public_base_url.rstrip("/")
        self._sleep = sleep
        self._locks = _AccessLockPool()
        self._in_flight_downloads: dict[str, int] = {}
        # Monotonic per access, bumped on every reservation and never reset.
        # A zero in-flight count alone cannot prove that no transfer existed
        # during a telemetry read, because a transfer can start and finish
        # entirely inside that window; a changed generation reveals it.
        self._download_generations: dict[str, int] = {}
        self._in_flight_lock = threading.Lock()

    def authorize_control(self, authorization: str | None) -> None:
        if self.settings.token is None:
            return
        expected = f"Bearer {self.settings.token}"
        if authorization is None or not hmac.compare_digest(
            authorization,
            expected,
        ):
            raise DataAgentAuthenticationError(
                "missing or invalid Data Agent bearer token"
            )

    def execute(
        self,
        request: DataAgentAccessRequest,
    ) -> dict[str, Any]:
        request_json = json.dumps(
            request.to_dict(),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        request_digest = sha256(request_json.encode("utf-8")).hexdigest()
        with self._locks.acquire(request.access_id):
            cached = self.store.get(request.access_id)
            if cached is not None:
                if cached.request_sha256 != request_digest:
                    raise DataAgentIdempotencyConflict(
                        "access_id was already used for a different request"
                    )
                return self._refresh_artifact_url(cached.response)

            resolved = self._resolve(request)
            response = self._execute_uncached(request, resolved)
            stored = self.store.put_if_absent(
                DataAgentOperation(
                    access_id=request.access_id,
                    request_sha256=request_digest,
                    request_json=request_json,
                    response=response,
                )
            )
            return self._refresh_artifact_url(stored.response)

    def _resolve(
        self,
        request: DataAgentAccessRequest,
    ) -> ResolvedDataAgentAccess:
        requested_location = request.binding.get("location")
        if not isinstance(requested_location, str) or not requested_location:
            raise DataAgentProtocolError(
                "Data Agent request binding.location must be a "
                "non-empty string"
            )
        return self.manifest.resolve(
            plan_id=request.plan_id,
            object_id=request.object_id,
            representation_id=request.representation_id,
            requested_location=requested_location,
        )

    def _execute_uncached(
        self,
        request: DataAgentAccessRequest,
        resolved: ResolvedDataAgentAccess,
    ) -> dict[str, Any]:
        started = time.perf_counter()
        fetch_started = time.perf_counter()
        if resolved.kind == "inline_text":
            raw = self._read_inline(resolved.path)
            content_digest = sha256(raw).hexdigest()
            payload_value: Any = raw.decode("utf-8")
            bytes_read = len(raw)
        else:
            content_digest, bytes_read = _hash_file(resolved.path)
            payload_value = self._artifact_url(
                request.access_id,
                content_digest,
            )
        fetch_ms = (time.perf_counter() - fetch_started) * 1_000.0

        target_latency_ms = (
            resolved.minimum_latency_ms * request.latency_multiplier
        )
        elapsed_ms = (time.perf_counter() - started) * 1_000.0
        controlled_delay_ms = max(0.0, target_latency_ms - elapsed_ms)
        if controlled_delay_ms > 0:
            self._sleep(controlled_delay_ms / 1_000.0)
        service_latency_ms = (time.perf_counter() - started) * 1_000.0

        return {
            "api_version": DATA_AGENT_API_VERSION,
            "status": "succeeded",
            "access_id": request.access_id,
            "object_id": request.object_id,
            "object_catalog_version": self._object_catalog_version(),
            "payload": {
                "kind": resolved.kind,
                "media_type": resolved.media_type,
                "value": payload_value,
                "sha256": content_digest,
            },
            "telemetry": {
                "service_latency_ms": service_latency_ms,
                "realized_cost": resolved.realized_cost,
                "bytes_read": bytes_read,
                "location": resolved.location,
                "cache_hit": resolved.cache_hit,
                "timings_ms": {
                    "fetch": fetch_ms,
                    "controlled_delay": controlled_delay_ms,
                },
            },
        }

    def _read_inline(self, path: Path) -> bytes:
        with path.open("rb") as stream:
            raw = stream.read(self.settings.max_inline_bytes + 1)
        if len(raw) > self.settings.max_inline_bytes:
            raise DataAgentInlinePayloadTooLarge(
                f"inline representation exceeds {self.settings.max_inline_bytes} "
                "bytes; use artifact_uri instead"
            )
        return raw

    def _artifact_url(self, access_id: str, digest: str) -> str:
        base = (
            f"{self.public_base_url}/v1/artifacts/"
            f"{quote(access_id, safe='')}"
        )
        if self.settings.artifact_secret is None:
            return base
        expires = int(time.time()) + self.settings.artifact_url_ttl_seconds
        signature = self._artifact_signature(access_id, expires, digest)
        return f"{base}?{urlencode({'expires': expires, 'signature': signature})}"

    def _artifact_signature(
        self,
        access_id: str,
        expires: int,
        digest: str,
    ) -> str:
        secret = self.settings.artifact_secret
        assert secret is not None
        message = f"{access_id}:{expires}:{digest}".encode("utf-8")
        return hmac.new(
            secret.encode("utf-8"),
            message,
            sha256,
        ).hexdigest()

    def _refresh_artifact_url(
        self,
        response: dict[str, Any],
    ) -> dict[str, Any]:
        copied = json.loads(json.dumps(response))
        payload = copied.get("payload")
        if isinstance(payload, dict) and payload.get("kind") == "artifact_uri":
            digest = payload.get("sha256")
            access_id = copied.get("access_id")
            if isinstance(digest, str) and isinstance(access_id, str):
                payload["value"] = self._artifact_url(access_id, digest)
        return copied

    def resolve_artifact(self, access_id: str) -> DataAgentArtifact:
        operation = self.store.get(access_id)
        if operation is None:
            raise DataAgentManifestLookupError(
                f"artifact access is unknown: {access_id}"
            )
        request_payload = json.loads(operation.request_json)
        if not isinstance(request_payload, Mapping):
            raise DataAgentServerError(
                f"stored request is invalid for access {access_id}"
            )
        request = DataAgentAccessRequest.from_dict(request_payload)
        resolved = self._resolve(request)
        payload = operation.response.get("payload")
        if not isinstance(payload, Mapping) or payload.get("kind") != "artifact_uri":
            raise DataAgentManifestLookupError(
                f"access does not refer to a downloadable artifact: {access_id}"
            )
        digest = payload.get("sha256")
        if not isinstance(digest, str):
            raise DataAgentServerError(
                f"stored artifact digest is invalid for access {access_id}"
            )
        size = resolved.path.stat().st_size
        return DataAgentArtifact(
            access_id=access_id,
            path=resolved.path,
            media_type=resolved.media_type,
            sha256=digest,
            size_bytes=size,
        )

    def begin_artifact_download(self, access_id: str) -> None:
        """Reserve one artifact transfer before any byte is written.

        The transfer is only durably recorded once the response body has been
        written, so a consumer that reads telemetry immediately after its
        download completes can otherwise observe a summary that does not yet
        include that transfer. Publishing the in-flight count lets the consumer
        wait for a consistent snapshot instead of silently under-counting.

        The generation is bumped under the same lock so that a reader can tell
        that a transfer existed even if it both started and finished between
        two observations.
        """
        with self._in_flight_lock:
            self._in_flight_downloads[access_id] = (
                self._in_flight_downloads.get(access_id, 0) + 1
            )
            self._download_generations[access_id] = (
                self._download_generations.get(access_id, 0) + 1
            )

    def record_artifact_download(
        self,
        download: DataAgentArtifactDownload,
    ) -> None:
        """Commit one transfer, then release its in-flight reservation.

        The order matters and is the whole basis of the quiescence contract:
        the row is durable before the counter drops, so a reader that observes
        zero in-flight transfers and *then* reads the summary cannot miss a
        finished transfer.

        If the commit fails the reservation is deliberately NOT released. The
        transfer really happened but will never appear in the summary, so the
        access must never look quiescent again; readers time out and fail the
        session instead of recording silently truncated bytes and latency.
        """
        self.store.record_artifact_download(download)
        with self._in_flight_lock:
            remaining = self._in_flight_downloads.get(
                download.access_id, 0
            ) - 1
            if remaining > 0:
                self._in_flight_downloads[download.access_id] = remaining
            else:
                self._in_flight_downloads.pop(download.access_id, None)

    def in_flight_download_count(self, access_id: str) -> int:
        with self._in_flight_lock:
            return self._in_flight_downloads.get(access_id, 0)

    def transfer_watermark(self, access_id: str) -> tuple[int, int]:
        """Atomically sample the in-flight count and the transfer generation.

        Both values must come from one critical section; sampling them
        separately would reintroduce the very window this exists to detect.
        """
        with self._in_flight_lock:
            return (
                self._in_flight_downloads.get(access_id, 0),
                self._download_generations.get(access_id, 0),
            )

    def access_telemetry(self, access_id: str) -> dict[str, Any]:
        operation = self.store.get(access_id)
        if operation is None:
            raise DataAgentManifestLookupError(
                f"access is unknown: {access_id}"
            )
        request_payload = json.loads(operation.request_json)
        if not isinstance(request_payload, Mapping):
            raise DataAgentServerError(
                f"stored request is invalid for access {access_id}"
            )
        request = DataAgentAccessRequest.from_dict(request_payload)
        # Bracket the durable read with two watermarks. Reading the counter
        # once before the summary is NOT enough: a transfer that starts after
        # that read and has not committed by the time the summary is taken is
        # invisible to both, so the snapshot would claim completeness while
        # under-counting. The summary is only final when no transfer was in
        # flight at either edge AND no transfer was reserved in between --
        # the generation catches one that started and finished inside the
        # window, which the counters alone cannot see.
        before_in_flight, before_generation = self.transfer_watermark(
            access_id
        )
        artifact_download = self.store.artifact_download_summary(access_id)
        after_in_flight, after_generation = self.transfer_watermark(access_id)

        stable = (
            before_in_flight == 0
            and after_in_flight == 0
            and before_generation == after_generation
        )
        # Report the count truthfully and carry the verdict separately, so a
        # transfer that began and ended inside the window is still flagged
        # incomplete even though both counts read zero.
        artifact_download["in_flight_request_count"] = after_in_flight
        artifact_download["telemetry_complete"] = stable
        return {
            "api_version": DATA_AGENT_API_VERSION,
            "status": "succeeded",
            "access_id": access_id,
            "object_id": request.object_id,
            "representation_id": request.representation_id,
            "object_catalog_version": self._object_catalog_version(),
            "artifact_download": artifact_download,
        }

    def _object_catalog_version(self) -> str | None:
        if self.manifest.object_catalog is None:
            return None
        return self.manifest.object_catalog.catalog_version

    def authorize_artifact(
        self,
        artifact: DataAgentArtifact,
        query: Mapping[str, list[str]],
        authorization: str | None,
    ) -> None:
        if self.settings.artifact_secret is None:
            self.authorize_control(authorization)
            return
        raw_expires = query.get("expires", [None])[0]
        signature = query.get("signature", [None])[0]
        try:
            expires = int(raw_expires) if raw_expires is not None else -1
        except ValueError as exc:
            raise DataAgentArtifactAuthorizationError(
                "invalid artifact URL expiry"
            ) from exc
        if expires < int(time.time()):
            raise DataAgentArtifactAuthorizationError(
                "artifact URL has expired"
            )
        expected = self._artifact_signature(
            artifact.access_id,
            expires,
            artifact.sha256,
        )
        if signature is None or not hmac.compare_digest(signature, expected):
            raise DataAgentArtifactAuthorizationError(
                "invalid artifact URL signature"
            )

    def health(self) -> dict[str, Any]:
        payload = {
            "status": "ok",
            "api_version": DATA_AGENT_API_VERSION,
            "node_id": self.manifest.node_id,
            "representations": sorted(self.manifest.representations),
        }
        if self.manifest.object_catalog is not None:
            payload["object_catalog_version"] = (
                self.manifest.object_catalog.catalog_version
            )
            payload["object_count"] = len(
                self.manifest.object_catalog.objects
            )
        return payload


class DataAgentHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    service: DataAgentService


class DataAgentHTTPRequestHandler(BaseHTTPRequestHandler):
    server_version = "PathfinderDataAgent/0.1"

    @property
    def service(self) -> DataAgentService:
        assert isinstance(self.server, DataAgentHTTPServer)
        return self.server.service

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/v1/access":
            self._write_error(HTTPStatus.NOT_FOUND, "not_found", "unknown endpoint")
            return
        try:
            self.service.authorize_control(self.headers.get("Authorization"))
            if (
                self.headers.get("X-Pathfinder-Protocol-Version")
                != DATA_AGENT_API_VERSION
            ):
                raise DataAgentProtocolError(
                    "missing or unsupported protocol version header"
                )
            content_length = self._content_length()
            if content_length > self.service.settings.max_request_bytes:
                self._write_error(
                    HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                    "request_too_large",
                    "request body exceeds max_request_bytes",
                )
                return
            raw = self.rfile.read(content_length)
            if len(raw) != content_length:
                raise DataAgentProtocolError("request body is incomplete")
            try:
                payload = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise DataAgentProtocolError(
                    "request body must be valid UTF-8 JSON"
                ) from exc
            if not isinstance(payload, Mapping):
                raise DataAgentProtocolError(
                    "request body must be a JSON object"
                )
            request = DataAgentAccessRequest.from_dict(payload)
            idempotency_key = self.headers.get("Idempotency-Key")
            if idempotency_key != request.access_id:
                raise DataAgentProtocolError(
                    "Idempotency-Key must equal request access_id"
                )
            self._write_json(HTTPStatus.OK, self.service.execute(request))
        except DataAgentAuthenticationError as exc:
            self._write_error(HTTPStatus.UNAUTHORIZED, "unauthorized", str(exc))
        except DataAgentProtocolError as exc:
            self._write_error(HTTPStatus.BAD_REQUEST, "invalid_request", str(exc))
        except DataAgentIdempotencyConflict as exc:
            self._write_error(HTTPStatus.CONFLICT, "idempotency_conflict", str(exc))
        except DataAgentBindingMismatchError as exc:
            self._write_error(HTTPStatus.CONFLICT, "binding_mismatch", str(exc))
        except DataAgentManifestLookupError as exc:
            self._write_error(HTTPStatus.NOT_FOUND, "not_found", str(exc))
        except FileNotFoundError as exc:
            self._write_error(HTTPStatus.NOT_FOUND, "artifact_not_found", str(exc))
        except DataAgentInlinePayloadTooLarge as exc:
            self._write_error(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                "inline_payload_too_large",
                str(exc),
            )
        except UnicodeDecodeError as exc:
            self._write_error(
                HTTPStatus.UNPROCESSABLE_ENTITY,
                "invalid_inline_text",
                str(exc),
            )
        except OSError as exc:
            logger.exception("Data Agent file access failed")
            self._write_error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "io_error",
                str(exc),
            )
        except Exception:
            logger.exception("Unhandled Data Agent access failure")
            self._write_error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "internal_error",
                "Data Agent access failed",
            )

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/healthz":
            self._write_json(HTTPStatus.OK, self.service.health())
            return
        telemetry_prefix = "/v1/accesses/"
        telemetry_suffix = "/telemetry"
        if (
            parsed.path.startswith(telemetry_prefix)
            and parsed.path.endswith(telemetry_suffix)
        ):
            access_id = unquote(
                parsed.path[
                    len(telemetry_prefix) : -len(telemetry_suffix)
                ]
            )
            if not access_id or "/" in access_id or "\\" in access_id:
                self._write_error(
                    HTTPStatus.BAD_REQUEST,
                    "invalid_access_id",
                    "access ID is invalid",
                )
                return
            try:
                self.service.authorize_control(
                    self.headers.get("Authorization")
                )
                if (
                    self.headers.get("X-Pathfinder-Protocol-Version")
                    != DATA_AGENT_API_VERSION
                ):
                    raise DataAgentProtocolError(
                        "missing or unsupported protocol version header"
                    )
                self._write_json(
                    HTTPStatus.OK,
                    self.service.access_telemetry(access_id),
                )
            except DataAgentAuthenticationError as exc:
                self._write_error(
                    HTTPStatus.UNAUTHORIZED,
                    "unauthorized",
                    str(exc),
                )
            except DataAgentProtocolError as exc:
                self._write_error(
                    HTTPStatus.BAD_REQUEST,
                    "invalid_request",
                    str(exc),
                )
            except DataAgentManifestLookupError as exc:
                self._write_error(
                    HTTPStatus.NOT_FOUND,
                    "not_found",
                    str(exc),
                )
            return
        prefix = "/v1/artifacts/"
        if not parsed.path.startswith(prefix):
            self._write_error(HTTPStatus.NOT_FOUND, "not_found", "unknown endpoint")
            return
        access_id = unquote(parsed.path[len(prefix) :])
        if not access_id or "/" in access_id or "\\" in access_id:
            self._write_error(
                HTTPStatus.BAD_REQUEST,
                "invalid_artifact_id",
                "artifact access ID is invalid",
            )
            return
        try:
            artifact = self.service.resolve_artifact(access_id)
            self.service.authorize_artifact(
                artifact,
                parse_qs(parsed.query),
                self.headers.get("Authorization"),
            )
            self._write_artifact(artifact)
        except (
            DataAgentAuthenticationError,
            DataAgentArtifactAuthorizationError,
        ) as exc:
            self._write_error(HTTPStatus.UNAUTHORIZED, "unauthorized", str(exc))
        except (DataAgentManifestLookupError, FileNotFoundError) as exc:
            self._write_error(HTTPStatus.NOT_FOUND, "not_found", str(exc))
        except DataAgentRangeNotSatisfiable:
            self.send_response(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
            self.send_header("Content-Range", f"bytes */{artifact.size_bytes}")
            self.send_header("Content-Length", "0")
            self.end_headers()
        except OSError as exc:
            logger.exception("Data Agent artifact transfer failed")
            self._write_error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "io_error",
                str(exc),
            )

    def _content_length(self) -> int:
        raw = self.headers.get("Content-Length")
        try:
            value = int(raw) if raw is not None else -1
        except ValueError as exc:
            raise DataAgentProtocolError("invalid Content-Length") from exc
        if value < 0:
            raise DataAgentProtocolError("Content-Length is required")
        return value

    def _write_artifact(self, artifact: DataAgentArtifact) -> None:
        start, end = self._parse_range(artifact.size_bytes)
        partial = start != 0 or end != artifact.size_bytes - 1
        content_length = max(0, end - start + 1)
        started_at = time.time()
        started = time.perf_counter()
        bytes_sent = 0
        completed = False
        # Registered before any byte reaches the client, so a telemetry read
        # issued after the download cannot miss this transfer.
        self.service.begin_artifact_download(artifact.access_id)
        try:
            self.send_response(
                HTTPStatus.PARTIAL_CONTENT if partial else HTTPStatus.OK
            )
            self.send_header("Content-Type", artifact.media_type)
            self.send_header("Content-Length", str(content_length))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("ETag", f'"{artifact.sha256}"')
            self.send_header("Cache-Control", "private, no-store")
            if partial:
                self.send_header(
                    "Content-Range",
                    f"bytes {start}-{end}/{artifact.size_bytes}",
                )
            self.end_headers()
            remaining = content_length
            if remaining > 0:
                with artifact.path.open("rb") as stream:
                    stream.seek(start)
                    while remaining > 0:
                        chunk = stream.read(min(1024 * 1024, remaining))
                        if not chunk:
                            break
                        self.wfile.write(chunk)
                        bytes_sent += len(chunk)
                        remaining -= len(chunk)
                self.wfile.flush()
            completed = remaining == 0
        finally:
            completed_at = time.time()
            try:
                self.service.record_artifact_download(
                    DataAgentArtifactDownload(
                        access_id=artifact.access_id,
                        range_start=start,
                        range_end=end,
                        bytes_sent=bytes_sent,
                        duration_ms=(time.perf_counter() - started) * 1_000.0,
                        completed=completed,
                        full_artifact=(
                            artifact.size_bytes == 0
                            or (start == 0 and end == artifact.size_bytes - 1)
                        ),
                        started_at=started_at,
                        completed_at=completed_at,
                    )
                )
            except Exception:
                # The reservation stays held on purpose, so telemetry for this
                # access can never report quiescence and no downstream reader
                # will treat the truncated summary as a complete observation.
                logger.exception(
                    "Data Agent could not record the artifact transfer for "
                    "access %s; its telemetry is permanently incomplete",
                    artifact.access_id,
                )

    def _parse_range(self, size: int) -> tuple[int, int]:
        if size == 0:
            return 0, -1
        raw = self.headers.get("Range")
        if raw is None:
            return 0, size - 1
        if not raw.startswith("bytes=") or "," in raw:
            raise DataAgentRangeNotSatisfiable
        value = raw[6:]
        start_text, separator, end_text = value.partition("-")
        if not separator:
            raise DataAgentRangeNotSatisfiable
        try:
            if start_text:
                start = int(start_text)
                end = int(end_text) if end_text else size - 1
            else:
                suffix = int(end_text)
                if suffix <= 0:
                    raise DataAgentRangeNotSatisfiable
                start = max(0, size - suffix)
                end = size - 1
        except ValueError:
            raise DataAgentRangeNotSatisfiable
        if start < 0 or end < start or start >= size:
            raise DataAgentRangeNotSatisfiable
        return start, min(end, size - 1)

    def _write_error(
        self,
        status: HTTPStatus,
        code: str,
        message: str,
    ) -> None:
        self._write_json(
            status,
            {
                "api_version": DATA_AGENT_API_VERSION,
                "status": "error",
                "error": {"code": code, "message": message},
            },
        )

    def _write_json(
        self,
        status: HTTPStatus,
        payload: Mapping[str, Any],
    ) -> None:
        body = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        logger.info("%s - %s", self.address_string(), format % args)


def create_data_agent_http_server(
    *,
    manifest_path: str | Path,
    operation_db: str | Path,
    settings: DataAgentServerSettings,
    sleep: Callable[[float], None] = time.sleep,
) -> DataAgentHTTPServer:
    manifest = load_data_agent_manifest(manifest_path)
    store = SQLiteDataAgentOperationStore(operation_db)
    server = DataAgentHTTPServer(
        (settings.host, settings.port),
        DataAgentHTTPRequestHandler,
    )
    actual_host, actual_port = server.server_address[:2]
    if settings.public_base_url is not None:
        public_base_url = settings.public_base_url
    else:
        public_host = (
            "127.0.0.1" if actual_host in {"0.0.0.0", "::"} else actual_host
        )
        public_base_url = f"http://{public_host}:{actual_port}"
    server.service = DataAgentService(
        manifest,
        store,
        settings,
        public_base_url,
        sleep=sleep,
    )
    return server


def run_data_agent_server(
    *,
    manifest_path: str | Path,
    operation_db: str | Path,
    settings: DataAgentServerSettings,
) -> None:
    server = create_data_agent_http_server(
        manifest_path=manifest_path,
        operation_db=operation_db,
        settings=settings,
    )
    host, port = server.server_address[:2]
    print(
        json.dumps(
            {
                "status": "serving",
                "node_id": server.service.manifest.node_id,
                "listen": f"{host}:{port}",
                "public_base_url": server.service.public_base_url,
                "api_version": DATA_AGENT_API_VERSION,
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        flush=True,
    )
    try:
        server.serve_forever()
    finally:
        server.server_close()


def _hash_file(path: Path) -> tuple[str, int]:
    digest = sha256()
    size = 0
    with path.open("rb") as stream:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size
