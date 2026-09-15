"""Durable real-byte artifact caches for logical execution nodes N7 and N8.

The cache is intentionally independent of Docker and cloud addressing.  A
container, VM, or test process can mount a state directory and expose the same
runtime contract.  Stored evidence contains content identities, never host
paths or credentials.
"""

from __future__ import annotations

import hashlib
import hmac
import http.server
import json
import os
import re
import signal
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


FULL_FLOW_CACHE_SCHEMA_VERSION = (
    "pathfinder.full-flow-artifact-cache/v1alpha1"
)
FULL_FLOW_CACHE_RESULT_SCHEMA_VERSION = (
    "pathfinder.full-flow-artifact-cache-result/v1alpha1"
)

_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._|:-]{0,255}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_TEMP_OBJECT = re.compile(r"\.[0-9a-f]{64}\.\d+\.\d+\.tmp")
_ALLOWED_NODES = frozenset({"N7", "N8"})
_MAX_JSON_BYTES = 1024 * 1024


class FullFlowCacheError(RuntimeError):
    """Raised when a cache operation cannot be safely completed."""


class FullFlowCacheConflict(FullFlowCacheError):
    """Raised when an idempotency key is reused for different content."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowCacheError(message)


def _identifier(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
        f"{name} is invalid",
    )
    return value


def _digest(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{name} is not a lowercase SHA-256 digest",
    )
    return value


def _positive_integer(value: Any, name: str) -> int:
    _require(
        type(value) is int and value > 0,
        f"{name} must be a positive integer",
    )
    return value


def _canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def cache_key(object_id: str, representation_id: str) -> str:
    """Return a stable opaque key for one logical cached representation."""

    identity = {
        "object_id": _identifier(object_id, "object_id"),
        "representation_id": _identifier(
            representation_id,
            "representation_id",
        ),
    }
    return _sha256(_canonical_bytes(identity))


@dataclass(frozen=True)
class CachedArtifact:
    """A verified cached payload and its portable identity."""

    cache_id: str
    node_id: str
    cache_key: str
    object_id: str
    representation_id: str
    content_sha256: str
    size_bytes: int
    payload: bytes
    event_id: int

    def metadata(self) -> dict[str, Any]:
        return {
            "schema_version": FULL_FLOW_CACHE_RESULT_SCHEMA_VERSION,
            "status": "HIT",
            "cache_id": self.cache_id,
            "node_id": self.node_id,
            "cache_key": self.cache_key,
            "object_id": self.object_id,
            "representation_id": self.representation_id,
            "content_sha256": self.content_sha256,
            "size_bytes": self.size_bytes,
            "event_id": self.event_id,
            "payload_included": False,
            "credentials_recorded": False,
        }


class _FullFlowArtifactCacheBase:
    """SQLite-indexed, content-addressed, capacity-bounded artifact cache."""

    def __init__(
        self,
        state_dir: str | Path,
        *,
        node_id: str,
        cache_id: str,
        capacity_bytes: int,
    ) -> None:
        _require(node_id in _ALLOWED_NODES, "cache node_id must be N7 or N8")
        self.node_id = node_id
        self.cache_id = _identifier(cache_id, "cache_id")
        self.capacity_bytes = _positive_integer(
            capacity_bytes,
            "capacity_bytes",
        )
        self._root = Path(state_dir).resolve()
        self._objects = self._root / "objects"
        self._database = self._root / "cache.sqlite3"
        self._lock = threading.RLock()
        self._root.mkdir(parents=True, exist_ok=True)
        self._objects.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self._database,
            timeout=30.0,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _initialize(self) -> None:
        with closing(self._connect()) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS cache_state (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    schema_version TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    cache_id TEXT NOT NULL,
                    capacity_bytes INTEGER NOT NULL,
                    access_sequence INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS cache_entries (
                    cache_key TEXT PRIMARY KEY,
                    object_id TEXT NOT NULL,
                    representation_id TEXT NOT NULL,
                    content_sha256 TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    last_access_sequence INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS cache_requests (
                    request_id TEXT PRIMARY KEY,
                    request_sha256 TEXT NOT NULL,
                    result_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS cache_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_kind TEXT NOT NULL,
                    cache_key TEXT NOT NULL,
                    object_id TEXT NOT NULL,
                    representation_id TEXT NOT NULL,
                    content_sha256 TEXT,
                    size_bytes INTEGER NOT NULL,
                    created_monotonic_ns INTEGER NOT NULL
                );
                """
            )
            current = connection.execute(
                "SELECT * FROM cache_state WHERE singleton = 1"
            ).fetchone()
            if current is None:
                connection.execute(
                    """
                    INSERT INTO cache_state (
                        singleton, schema_version, node_id, cache_id,
                        capacity_bytes, access_sequence
                    ) VALUES (1, ?, ?, ?, ?, 0)
                    """,
                    (
                        FULL_FLOW_CACHE_SCHEMA_VERSION,
                        self.node_id,
                        self.cache_id,
                        self.capacity_bytes,
                    ),
                )
            else:
                _require(
                    current["schema_version"]
                    == FULL_FLOW_CACHE_SCHEMA_VERSION
                    and current["node_id"] == self.node_id
                    and current["cache_id"] == self.cache_id
                    and current["capacity_bytes"] == self.capacity_bytes,
                    "cache state binding differs from configured cache",
                )
        self.verify()
        self._reconcile_object_directory()

    def _sync_object_directory(self) -> None:
        """Persist object-name changes on platforms that support dir fsync."""

        if os.name == "nt":
            return
        descriptor = os.open(self._objects, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _reconcile_object_directory(self) -> None:
        """Remove only known crash debris and reject unexpected entries."""

        with self._lock, closing(self._connect()) as connection:
            referenced = {
                str(row["content_sha256"])
                for row in connection.execute(
                    "SELECT DISTINCT content_sha256 FROM cache_entries"
                )
            }
            removed = False
            for candidate in self._objects.iterdir():
                _require(
                    candidate.is_file() and not candidate.is_symlink(),
                    "cache object directory contains a non-regular entry",
                )
                name = candidate.name
                if _TEMP_OBJECT.fullmatch(name) is not None:
                    candidate.unlink()
                    removed = True
                    continue
                _require(
                    _SHA256.fullmatch(name) is not None,
                    "cache object directory contains an unexpected file",
                )
                if name not in referenced:
                    candidate.unlink()
                    removed = True
            if removed:
                self._sync_object_directory()

    def _content_path(self, content_sha256: str) -> Path:
        digest = _digest(content_sha256, "content_sha256")
        return self._objects / digest

    @staticmethod
    def _next_sequence(connection: sqlite3.Connection) -> int:
        row = connection.execute(
            "SELECT access_sequence FROM cache_state WHERE singleton = 1"
        ).fetchone()
        _require(row is not None, "cache state is absent")
        sequence = int(row["access_sequence"]) + 1
        connection.execute(
            "UPDATE cache_state SET access_sequence = ? WHERE singleton = 1",
            (sequence,),
        )
        return sequence

    def health(self) -> dict[str, Any]:
        with self._lock, closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS entry_count,
                       COALESCE(SUM(size_bytes), 0) AS used_bytes
                FROM cache_entries
                """
            ).fetchone()
        return {
            "schema_version": FULL_FLOW_CACHE_SCHEMA_VERSION,
            "status": "ok",
            "node_id": self.node_id,
            "cache_id": self.cache_id,
            "capacity_bytes": self.capacity_bytes,
            "used_bytes": int(row["used_bytes"]),
            "entry_count": int(row["entry_count"]),
            "persistent_state": True,
            "payload_bytes_are_real": True,
            "credentials_recorded": False,
        }


def _strict_json(raw: bytes, name: str) -> dict[str, Any]:
    _require(len(raw) <= _MAX_JSON_BYTES, f"{name} exceeds its byte limit")
    try:
        text = raw.decode("utf-8")
        value = json.loads(text)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowCacheError(f"{name} is not valid JSON") from exc
    _require(isinstance(value, dict), f"{name} is not an object")
    return value


@dataclass(frozen=True)
class FullFlowCacheServerSettings:
    """Ephemeral cache service configuration; the token is never persisted."""

    host: str
    port: int
    token: str
    max_artifact_bytes: int

    def __post_init__(self) -> None:
        _require(isinstance(self.host, str) and bool(self.host), "host is invalid")
        _require(type(self.port) is int and 0 <= self.port <= 65535, "port is invalid")
        _require(
            isinstance(self.token, str) and len(self.token.encode("utf-8")) >= 16,
            "cache bearer token must contain at least 16 bytes",
        )
        _positive_integer(self.max_artifact_bytes, "max_artifact_bytes")


class FullFlowCacheHTTPServer(http.server.ThreadingHTTPServer):
    daemon_threads = True
    cache: FullFlowArtifactCache
    settings: FullFlowCacheServerSettings


class FullFlowCacheRequestHandler(http.server.BaseHTTPRequestHandler):
    """Bounded authenticated HTTP surface for a real-byte artifact cache."""

    server: FullFlowCacheHTTPServer

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _authorized(self) -> bool:
        supplied = self.headers.get("Authorization")
        expected = "Bearer " + self.server.settings.token
        return isinstance(supplied, str) and hmac.compare_digest(
            supplied,
            expected,
        )

    def _json_response(self, status: int, value: Mapping[str, Any]) -> None:
        payload = _canonical_bytes(dict(value))
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    def _error(self, status: int, message: str) -> None:
        self._json_response(status, {
            "schema_version": FULL_FLOW_CACHE_RESULT_SCHEMA_VERSION,
            "status": "ERROR",
            "message": message,
            "credentials_recorded": False,
        })

    def _require_auth(self) -> bool:
        if self._authorized():
            return True
        self._error(401, "missing or invalid cache bearer token")
        return False

    def do_GET(self) -> None:
        try:
            parsed = urllib.parse.urlsplit(self.path)
            if parsed.path == "/healthz" and not parsed.query:
                self._json_response(200, self.server.cache.health())
                return
            _require(
                parsed.path == "/v1/cache/artifact",
                "not found",
            )
            if not self._require_auth():
                return
            query = urllib.parse.parse_qs(
                parsed.query,
                keep_blank_values=True,
                strict_parsing=True,
            )
            _require(
                set(query) in (
                    {"object_id", "representation_id"},
                    {"object_id", "representation_id", "expected_sha256"},
                )
                and all(len(values) == 1 for values in query.values()),
                "cache artifact query is invalid",
            )
            artifact = self.server.cache.lookup(
                object_id=query["object_id"][0],
                representation_id=query["representation_id"][0],
                expected_sha256=(
                    query.get("expected_sha256", [None])[0]
                ),
            )
            if artifact is None:
                self._json_response(404, {
                    "schema_version": FULL_FLOW_CACHE_RESULT_SCHEMA_VERSION,
                    "status": "MISS",
                    "node_id": self.server.cache.node_id,
                    "cache_id": self.server.cache.cache_id,
                    "credentials_recorded": False,
                })
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(artifact.size_bytes))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Pathfinder-Cache-Node", artifact.node_id)
            self.send_header("X-Pathfinder-Cache-Id", artifact.cache_id)
            self.send_header("X-Pathfinder-Cache-Key", artifact.cache_key)
            self.send_header(
                "X-Pathfinder-Content-SHA256",
                artifact.content_sha256,
            )
            self.send_header(
                "X-Pathfinder-Cache-Event-Id",
                str(artifact.event_id),
            )
            self.end_headers()
            self.wfile.write(artifact.payload)
        except (FullFlowCacheError, ValueError) as exc:
            self._error(400, str(exc))
        except Exception:
            self._error(500, "cache service internal error")

    def do_PUT(self) -> None:
        try:
            parsed = urllib.parse.urlsplit(self.path)
            _require(
                parsed.path == "/v1/cache/artifact" and not parsed.query,
                "not found",
            )
            if not self._require_auth():
                return
            _require(
                self.headers.get_content_type() == "application/octet-stream",
                "cache artifact content type is invalid",
            )
            raw_length = self.headers.get("Content-Length")
            _require(
                isinstance(raw_length, str) and raw_length.isdecimal(),
                "cache artifact Content-Length is invalid",
            )
            length = int(raw_length)
            _require(
                0 < length <= self.server.settings.max_artifact_bytes,
                "cache artifact length is outside the configured limit",
            )
            payload = self.rfile.read(length)
            _require(len(payload) == length, "cache artifact body is truncated")
            result = self.server.cache.put(
                request_id=self.headers.get("X-Pathfinder-Request-Id"),
                object_id=self.headers.get("X-Pathfinder-Object-Id"),
                representation_id=self.headers.get(
                    "X-Pathfinder-Representation-Id"
                ),
                payload=payload,
                expected_sha256=self.headers.get(
                    "X-Pathfinder-Content-SHA256"
                ),
            )
            self._json_response(200, result)
        except (FullFlowCacheError, ValueError) as exc:
            self._error(400, str(exc))
        except Exception:
            self._error(500, "cache service internal error")


def create_full_flow_cache_http_server(
    cache: FullFlowArtifactCache,
    settings: FullFlowCacheServerSettings,
) -> FullFlowCacheHTTPServer:
    server = FullFlowCacheHTTPServer((settings.host, settings.port), (
        FullFlowCacheRequestHandler
    ))
    server.cache = cache
    server.settings = settings
    return server


class _RejectRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: Any,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        raise FullFlowCacheError("cache service redirect refused")


def _validated_cache_origin(
    base_url: str,
    simulator_private_http_hosts: tuple[str, ...],
) -> str:
    _require(isinstance(base_url, str), "cache base_url is invalid")
    parsed = urllib.parse.urlsplit(base_url)
    _require(
        parsed.scheme in {"http", "https"}
        and parsed.hostname is not None
        and parsed.username is None
        and parsed.password is None
        and parsed.path in {"", "/"}
        and not parsed.query
        and not parsed.fragment,
        "cache base_url must be an HTTP(S) origin without credentials",
    )
    private_hosts = tuple(
        _identifier(host, "simulator_private_http_host")
        for host in simulator_private_http_hosts
    )
    _require(
        len(private_hosts) == len(set(private_hosts)),
        "simulator private cache hosts contain duplicates",
    )
    loopback = parsed.hostname in {"127.0.0.1", "localhost", "::1"}
    private = parsed.hostname in private_hosts and parsed.hostname.startswith(
        ("pathfinder-sim-", "pathfinder-full-flow-")
    )
    _require(
        parsed.scheme == "https" or loopback or private,
        "non-local cache base_url must use HTTPS",
    )
    return base_url.rstrip("/")


class HttpFullFlowArtifactCacheClient:
    """Proxy-free client for N7/N8 cache services."""

    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        expected_node_id: str,
        expected_cache_id: str,
        timeout_seconds: float = 30.0,
        max_artifact_bytes: int = 64 * 1024 * 1024,
        simulator_private_http_hosts: tuple[str, ...] = (),
    ) -> None:
        _require(
            expected_node_id in _ALLOWED_NODES,
            "expected cache node must be N7 or N8",
        )
        self._base = _validated_cache_origin(
            base_url,
            simulator_private_http_hosts,
        )
        _require(
            isinstance(token, str) and len(token.encode("utf-8")) >= 16,
            "cache bearer token must contain at least 16 bytes",
        )
        self._token = token
        self._node = expected_node_id
        self._cache = _identifier(expected_cache_id, "expected_cache_id")
        _require(
            isinstance(timeout_seconds, (int, float))
            and not isinstance(timeout_seconds, bool)
            and float(timeout_seconds) > 0.0,
            "cache timeout_seconds must be positive",
        )
        self._timeout = float(timeout_seconds)
        self._max_artifact = _positive_integer(
            max_artifact_bytes,
            "max_artifact_bytes",
        )
        self._opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            _RejectRedirects(),
        ).open

    def _authorization(self) -> str:
        return "Bearer " + self._token

    def health(self) -> dict[str, Any]:
        request = urllib.request.Request(
            self._base + "/healthz",
            headers={"Accept": "application/json"},
            method="GET",
        )
        try:
            with self._opener(request, timeout=self._timeout) as response:
                raw = response.read(_MAX_JSON_BYTES + 1)
        except (urllib.error.URLError, OSError) as exc:
            raise FullFlowCacheError("cache health request failed") from exc
        value = _strict_json(raw, "cache health response")
        _require(
            value.get("status") == "ok"
            and value.get("node_id") == self._node
            and value.get("cache_id") == self._cache
            and value.get("credentials_recorded") is False,
            "cache health identity is invalid",
        )
        return value

    def get(
        self,
        *,
        object_id: str,
        representation_id: str,
        expected_sha256: str | None = None,
    ) -> CachedArtifact | None:
        query: dict[str, str] = {
            "object_id": _identifier(object_id, "object_id"),
            "representation_id": _identifier(
                representation_id,
                "representation_id",
            ),
        }
        if expected_sha256 is not None:
            query["expected_sha256"] = _digest(
                expected_sha256,
                "expected_sha256",
            )
        request = urllib.request.Request(
            self._base
            + "/v1/cache/artifact?"
            + urllib.parse.urlencode(query),
            headers={
                "Accept": "application/octet-stream",
                "Authorization": self._authorization(),
            },
            method="GET",
        )
        try:
            with self._opener(request, timeout=self._timeout) as response:
                raw = response.read(self._max_artifact + 1)
                headers = response.headers
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            raise FullFlowCacheError(
                f"cache artifact request returned HTTP {exc.code}"
            ) from exc
        except (urllib.error.URLError, OSError) as exc:
            raise FullFlowCacheError("cache artifact request failed") from exc
        _require(len(raw) <= self._max_artifact, "cache artifact is too large")
        raw_length = headers.get("Content-Length")
        _require(
            isinstance(raw_length, str)
            and raw_length.isdecimal()
            and int(raw_length) == len(raw),
            "cache response Content-Length is invalid",
        )
        content_sha256 = _digest(
            headers.get("X-Pathfinder-Content-SHA256"),
            "cache response content digest",
        )
        _require(_sha256(raw) == content_sha256, "cache response digest changed")
        if expected_sha256 is not None:
            _require(
                content_sha256 == expected_sha256,
                "cache response differs from expected content",
            )
        node_id = headers.get("X-Pathfinder-Cache-Node")
        cache_id = headers.get("X-Pathfinder-Cache-Id")
        _require(
            node_id == self._node and cache_id == self._cache,
            "cache response identity changed",
        )
        raw_event_id = headers.get("X-Pathfinder-Cache-Event-Id")
        _require(
            isinstance(raw_event_id, str) and raw_event_id.isdecimal(),
            "cache response event ID is invalid",
        )
        return CachedArtifact(
            cache_id=self._cache,
            node_id=self._node,
            cache_key=_digest(
                headers.get("X-Pathfinder-Cache-Key"),
                "cache response key",
            ),
            object_id=object_id,
            representation_id=representation_id,
            content_sha256=content_sha256,
            size_bytes=len(raw),
            payload=raw,
            event_id=int(raw_event_id),
        )

    def put(
        self,
        *,
        request_id: str,
        object_id: str,
        representation_id: str,
        payload: bytes,
        expected_sha256: str | None = None,
    ) -> dict[str, Any]:
        _require(
            isinstance(payload, bytes)
            and 0 < len(payload) <= self._max_artifact,
            "cache artifact is outside the configured byte limit",
        )
        digest = _sha256(payload)
        if expected_sha256 is not None:
            _require(
                digest == _digest(expected_sha256, "expected_sha256"),
                "cache artifact differs from expected content",
            )
        request = urllib.request.Request(
            self._base + "/v1/cache/artifact",
            data=payload,
            headers={
                "Accept": "application/json",
                "Authorization": self._authorization(),
                "Content-Type": "application/octet-stream",
                "X-Pathfinder-Request-Id": _identifier(
                    request_id,
                    "request_id",
                ),
                "X-Pathfinder-Object-Id": _identifier(
                    object_id,
                    "object_id",
                ),
                "X-Pathfinder-Representation-Id": _identifier(
                    representation_id,
                    "representation_id",
                ),
                "X-Pathfinder-Content-SHA256": digest,
            },
            method="PUT",
        )
        try:
            with self._opener(request, timeout=self._timeout) as response:
                raw = response.read(_MAX_JSON_BYTES + 1)
        except urllib.error.HTTPError as exc:
            raise FullFlowCacheError(
                f"cache store request returned HTTP {exc.code}"
            ) from exc
        except (urllib.error.URLError, OSError) as exc:
            raise FullFlowCacheError("cache store request failed") from exc
        result = _strict_json(raw, "cache store response")
        _require(
            result.get("status") == "STORED"
            and result.get("node_id") == self._node
            and result.get("cache_id") == self._cache
            and result.get("content_sha256") == digest
            and result.get("size_bytes") == len(payload)
            and result.get("credentials_recorded") is False,
            "cache store response is invalid",
        )
        return result


class FullFlowArtifactCache(_FullFlowArtifactCacheBase):
    """Complete local cache store, layered over the service configuration."""

    def lookup(
        self,
        *,
        object_id: str,
        representation_id: str,
        expected_sha256: str | None = None,
    ) -> CachedArtifact | None:
        object_id = _identifier(object_id, "object_id")
        representation_id = _identifier(
            representation_id,
            "representation_id",
        )
        expected = (
            None
            if expected_sha256 is None
            else _digest(expected_sha256, "expected_sha256")
        )
        key = cache_key(object_id, representation_id)
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM cache_entries WHERE cache_key = ?",
                (key,),
            ).fetchone()
            if row is None or (
                expected is not None and row["content_sha256"] != expected
            ):
                event = connection.execute(
                    """
                    INSERT INTO cache_events (
                        event_kind, cache_key, object_id, representation_id,
                        content_sha256, size_bytes, created_monotonic_ns
                    ) VALUES ('MISS', ?, ?, ?, ?, 0, ?)
                    """,
                    (key, object_id, representation_id, expected, time.monotonic_ns()),
                )
                connection.execute("COMMIT")
                _require(event.lastrowid is not None, "cache event ID is absent")
                return None
            sequence = self._next_sequence(connection)
            connection.execute(
                """
                UPDATE cache_entries SET last_access_sequence = ?
                WHERE cache_key = ?
                """,
                (sequence, key),
            )
            event = connection.execute(
                """
                INSERT INTO cache_events (
                    event_kind, cache_key, object_id, representation_id,
                    content_sha256, size_bytes, created_monotonic_ns
                ) VALUES ('HIT', ?, ?, ?, ?, ?, ?)
                """,
                (
                    key,
                    object_id,
                    representation_id,
                    row["content_sha256"],
                    row["size_bytes"],
                    time.monotonic_ns(),
                ),
            )
            connection.execute("COMMIT")
        path = self._content_path(str(row["content_sha256"]))
        try:
            payload = path.read_bytes()
        except OSError as exc:
            raise FullFlowCacheError("cached payload is not readable") from exc
        _require(len(payload) == row["size_bytes"], "cached payload size changed")
        _require(
            _sha256(payload) == row["content_sha256"],
            "cached payload digest changed",
        )
        _require(event.lastrowid is not None, "cache event ID is absent")
        return CachedArtifact(
            cache_id=self.cache_id,
            node_id=self.node_id,
            cache_key=key,
            object_id=object_id,
            representation_id=representation_id,
            content_sha256=str(row["content_sha256"]),
            size_bytes=int(row["size_bytes"]),
            payload=payload,
            event_id=int(event.lastrowid),
        )

    def put(
        self,
        *,
        request_id: str,
        object_id: str,
        representation_id: str,
        payload: bytes,
        expected_sha256: str | None = None,
    ) -> dict[str, Any]:
        request_id = _identifier(request_id, "request_id")
        object_id = _identifier(object_id, "object_id")
        representation_id = _identifier(
            representation_id,
            "representation_id",
        )
        _require(isinstance(payload, bytes) and bool(payload), "payload is empty")
        size_bytes = len(payload)
        _require(
            size_bytes <= self.capacity_bytes,
            "payload exceeds cache capacity",
        )
        content_sha256 = _sha256(payload)
        if expected_sha256 is not None:
            _require(
                content_sha256
                == _digest(expected_sha256, "expected_sha256"),
                "payload digest differs from expected_sha256",
            )
        key = cache_key(object_id, representation_id)
        request_sha256 = _sha256(_canonical_bytes({
            "request_id": request_id,
            "cache_key": key,
            "content_sha256": content_sha256,
            "size_bytes": size_bytes,
        }))
        final_path = self._content_path(content_sha256)
        temporary_path = self._objects / (
            f".{content_sha256}.{os.getpid()}.{threading.get_ident()}.tmp"
        )

        with self._lock:
            with closing(self._connect()) as connection:
                replay = connection.execute(
                    "SELECT * FROM cache_requests WHERE request_id = ?",
                    (request_id,),
                ).fetchone()
                if replay is not None:
                    if replay["request_sha256"] != request_sha256:
                        raise FullFlowCacheConflict(
                            "request_id was reused for different cache content"
                        )
                    result = json.loads(str(replay["result_json"]))
                    result["idempotent_replay"] = True
                    return result

            try:
                temporary_path.write_bytes(payload)
                _require(
                    _sha256(temporary_path.read_bytes()) == content_sha256,
                    "temporary cache payload verification failed",
                )
                if not final_path.exists():
                    os.replace(temporary_path, final_path)
                    self._sync_object_directory()
                else:
                    temporary_path.unlink(missing_ok=True)
                    _require(
                        _sha256(final_path.read_bytes()) == content_sha256,
                        "existing content-addressed payload is corrupt",
                    )
            except OSError as exc:
                temporary_path.unlink(missing_ok=True)
                raise FullFlowCacheError("cache payload write failed") from exc

            evicted: list[dict[str, Any]] = []
            with closing(self._connect()) as connection:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    "SELECT * FROM cache_entries WHERE cache_key = ?",
                    (key,),
                ).fetchone()
                used_row = connection.execute(
                    "SELECT COALESCE(SUM(size_bytes), 0) AS used FROM cache_entries"
                ).fetchone()
                used = int(used_row["used"])
                if existing is not None:
                    used -= int(existing["size_bytes"])
                required = used + size_bytes
                while required > self.capacity_bytes:
                    victim = connection.execute(
                        """
                        SELECT * FROM cache_entries
                        WHERE cache_key != ?
                        ORDER BY last_access_sequence ASC, cache_key ASC
                        LIMIT 1
                        """,
                        (key,),
                    ).fetchone()
                    _require(victim is not None, "cache cannot select an eviction")
                    connection.execute(
                        "DELETE FROM cache_entries WHERE cache_key = ?",
                        (victim["cache_key"],),
                    )
                    used -= int(victim["size_bytes"])
                    required = used + size_bytes
                    evicted.append({
                        "cache_key": victim["cache_key"],
                        "object_id": victim["object_id"],
                        "representation_id": victim["representation_id"],
                        "content_sha256": victim["content_sha256"],
                        "size_bytes": victim["size_bytes"],
                    })
                sequence = self._next_sequence(connection)
                connection.execute(
                    """
                    INSERT INTO cache_entries (
                        cache_key, object_id, representation_id,
                        content_sha256, size_bytes, last_access_sequence
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(cache_key) DO UPDATE SET
                        object_id = excluded.object_id,
                        representation_id = excluded.representation_id,
                        content_sha256 = excluded.content_sha256,
                        size_bytes = excluded.size_bytes,
                        last_access_sequence = excluded.last_access_sequence
                    """,
                    (
                        key,
                        object_id,
                        representation_id,
                        content_sha256,
                        size_bytes,
                        sequence,
                    ),
                )
                event = connection.execute(
                    """
                    INSERT INTO cache_events (
                        event_kind, cache_key, object_id, representation_id,
                        content_sha256, size_bytes, created_monotonic_ns
                    ) VALUES ('STORE', ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        key,
                        object_id,
                        representation_id,
                        content_sha256,
                        size_bytes,
                        time.monotonic_ns(),
                    ),
                )
                _require(event.lastrowid is not None, "cache event ID is absent")
                result = {
                    "schema_version": FULL_FLOW_CACHE_RESULT_SCHEMA_VERSION,
                    "status": "STORED",
                    "cache_id": self.cache_id,
                    "node_id": self.node_id,
                    "cache_key": key,
                    "object_id": object_id,
                    "representation_id": representation_id,
                    "content_sha256": content_sha256,
                    "size_bytes": size_bytes,
                    "event_id": int(event.lastrowid),
                    "evicted": evicted,
                    "idempotent_replay": False,
                    "credentials_recorded": False,
                }
                connection.execute(
                    """
                    INSERT INTO cache_requests (
                        request_id, request_sha256, result_json
                    ) VALUES (?, ?, ?)
                    """,
                    (
                        request_id,
                        request_sha256,
                        _canonical_bytes(result).decode("utf-8").strip(),
                    ),
                )
                connection.execute("COMMIT")
            self._remove_orphaned_objects()
            return result

    def _remove_orphaned_objects(self) -> None:
        self._reconcile_object_directory()

    def events(self) -> list[dict[str, Any]]:
        with self._lock, closing(self._connect()) as connection:
            rows = connection.execute(
                "SELECT * FROM cache_events ORDER BY event_id"
            ).fetchall()
        return [
            {
                "event_id": int(row["event_id"]),
                "event_kind": str(row["event_kind"]),
                "cache_key": str(row["cache_key"]),
                "object_id": str(row["object_id"]),
                "representation_id": str(row["representation_id"]),
                "content_sha256": row["content_sha256"],
                "size_bytes": int(row["size_bytes"]),
                "credentials_recorded": False,
            }
            for row in rows
        ]

    def verify(self) -> dict[str, Any]:
        with self._lock, closing(self._connect()) as connection:
            state = connection.execute(
                "SELECT * FROM cache_state WHERE singleton = 1"
            ).fetchone()
            _require(state is not None, "cache state is absent")
            _require(
                state["schema_version"] == FULL_FLOW_CACHE_SCHEMA_VERSION
                and state["node_id"] == self.node_id
                and state["cache_id"] == self.cache_id
                and state["capacity_bytes"] == self.capacity_bytes,
                "cache state binding changed",
            )
            rows = connection.execute(
                "SELECT * FROM cache_entries ORDER BY cache_key"
            ).fetchall()
        used = 0
        for row in rows:
            _digest(str(row["cache_key"]), "cache_key")
            _identifier(row["object_id"], "object_id")
            _identifier(row["representation_id"], "representation_id")
            digest = _digest(row["content_sha256"], "content_sha256")
            size = _positive_integer(row["size_bytes"], "size_bytes")
            path = self._content_path(digest)
            try:
                payload = path.read_bytes()
            except OSError as exc:
                raise FullFlowCacheError("cached payload is absent") from exc
            _require(len(payload) == size, "cached payload size changed")
            _require(_sha256(payload) == digest, "cached payload digest changed")
            used += size
        _require(used <= self.capacity_bytes, "cache exceeds capacity")
        return {
            "schema_version": FULL_FLOW_CACHE_SCHEMA_VERSION,
            "status": "VERIFIED",
            "node_id": self.node_id,
            "cache_id": self.cache_id,
            "capacity_bytes": self.capacity_bytes,
            "used_bytes": used,
            "entry_count": len(rows),
            "persistent_state": True,
            "payload_bytes_are_real": True,
            "credentials_recorded": False,
        }


def serve_full_flow_cache(
    state_dir: str | Path,
    *,
    node_id: str,
    cache_id: str,
    capacity_bytes: int,
    token: str,
    host: str = "0.0.0.0",
    port: int = 9081,
    max_artifact_bytes: int = 64 * 1024 * 1024,
) -> None:
    """Serve one durable N7/N8 cache and stop cleanly on SIGTERM/SIGINT."""

    cache = FullFlowArtifactCache(
        state_dir,
        node_id=node_id,
        cache_id=cache_id,
        capacity_bytes=capacity_bytes,
    )
    settings = FullFlowCacheServerSettings(
        host=host,
        port=port,
        token=token,
        max_artifact_bytes=max_artifact_bytes,
    )
    server = create_full_flow_cache_http_server(cache, settings)
    shutdown_requested = threading.Event()
    previous: dict[int, Any] = {}

    def request_shutdown(signum: int, frame: Any) -> None:
        del signum, frame
        if shutdown_requested.is_set():
            return
        shutdown_requested.set()
        threading.Thread(target=server.shutdown, daemon=True).start()

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            previous[signum] = signal.getsignal(signum)
            signal.signal(signum, request_shutdown)
        except (OSError, ValueError):
            pass
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        request_shutdown(signal.SIGINT, None)
    finally:
        server.server_close()
        for signum, handler in previous.items():
            try:
                signal.signal(signum, handler)
            except (OSError, ValueError):
                pass


__all__ = [
    "FULL_FLOW_CACHE_RESULT_SCHEMA_VERSION",
    "FULL_FLOW_CACHE_SCHEMA_VERSION",
    "CachedArtifact",
    "FullFlowArtifactCache",
    "FullFlowCacheConflict",
    "FullFlowCacheError",
    "FullFlowCacheHTTPServer",
    "FullFlowCacheServerSettings",
    "HttpFullFlowArtifactCacheClient",
    "cache_key",
    "create_full_flow_cache_http_server",
    "serve_full_flow_cache",
]
