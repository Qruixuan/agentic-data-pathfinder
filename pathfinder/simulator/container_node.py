"""Small standard-library node service for local container emulation.

The service executes infrastructure operations only.  It never evaluates a
video answer and reports that semantic quality is unavailable.  Synthetic
payloads are deterministic, size preserving, bounded, and written beneath a
dedicated state directory so local smoke tests exercise real file reads and
HTTP byte transfer without requiring the benchmark dataset or an LLM.
"""

from __future__ import annotations

import hashlib
import http.client
import json
import os
import re
import signal
import threading
import time
import uuid
from collections import OrderedDict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit

from .container_contract import CONTAINER_OPERATION_SCHEMA_VERSION


CONTAINER_NODE_API_VERSION = "pathfinder.container-node/v1alpha1"
CONTAINER_NODE_RESULT_SCHEMA_VERSION = (
    "pathfinder.container-node-operation-result/v1alpha1"
)

_CHUNK_BYTES = 64 * 1024
_MAX_JSON_BYTES = 2 * 1024 * 1024
_SHA256 = re.compile(r"[0-9a-f]{64}")


class ContainerNodeError(ValueError):
    """Raised when an operation is unsafe or assigned to the wrong node."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ContainerNodeError(message)


def _text(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and bool(value.strip()),
        f"{name} must be a non-empty string",
    )
    return value.strip()


def _integer(value: Any, name: str) -> int:
    _require(type(value) is int and value >= 0, f"{name} must be non-negative")
    return value


def _fixture_block(key: str) -> bytes:
    seed = hashlib.sha256(key.encode("utf-8")).digest()
    return (seed * ((_CHUNK_BYTES + len(seed) - 1) // len(seed)))[:_CHUNK_BYTES]


def _payload_digest(key: str, size: int) -> str:
    digest = hashlib.sha256()
    block = _fixture_block(key)
    remaining = size
    while remaining:
        take = min(remaining, len(block))
        digest.update(block[:take])
        remaining -= take
    return digest.hexdigest()


class _CacheState:
    def __init__(
        self,
        capacity_bytes: int,
        initial_entries: list[Mapping[str, Any]],
    ) -> None:
        self.capacity_bytes = capacity_bytes
        self.entries: OrderedDict[str, int] = OrderedDict(
            (
                f"{_text(entry.get('object_id'), 'cache object_id')}:"
                f"{_text(entry.get('representation_id'), 'cache representation_id')}",
                _integer(entry.get("size_bytes"), "cache entry size_bytes"),
            )
            for entry in initial_entries
        )
        self.used_bytes = sum(self.entries.values())
        _require(
            self.used_bytes <= capacity_bytes,
            "initial cache entries exceed capacity",
        )

    def lookup(self, key: str) -> bool:
        if key not in self.entries:
            return False
        self.entries.move_to_end(key)
        return True

    def insert(self, key: str, size: int) -> list[str]:
        if size > self.capacity_bytes:
            return []
        previous = self.entries.pop(key, None)
        if previous is not None:
            self.used_bytes -= previous
        evicted: list[str] = []
        while self.entries and self.used_bytes + size > self.capacity_bytes:
            old_key, old_size = self.entries.popitem(last=False)
            self.used_bytes -= old_size
            evicted.append(old_key)
        self.entries[key] = size
        self.used_bytes += size
        return evicted


class ContainerNodeRuntime:
    """Stateful executor used by the HTTP service and protocol tests."""

    def __init__(
        self,
        node_id: str,
        state_dir: str | Path,
        *,
        max_operation_bytes: int = 1024 * 1024 * 1024,
        transfer_port: int = 9080,
    ) -> None:
        self.node_id = _text(node_id, "node_id")
        self.state_dir = Path(state_dir).resolve()
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.max_operation_bytes = _integer(
            max_operation_bytes,
            "max_operation_bytes",
        )
        _require(self.max_operation_bytes > 0, "max_operation_bytes must be positive")
        self.transfer_port = _integer(transfer_port, "transfer_port")
        _require(0 < self.transfer_port <= 65535, "transfer_port is invalid")
        self.runtime_epoch = uuid.uuid4().hex
        self._caches: dict[str, _CacheState] = {}
        self._lock = threading.RLock()
        self._operation_condition = threading.Condition(self._lock)
        self._operation_request_sha256: dict[str, str] = {}
        self._operations_in_flight: set[str] = set()
        self._operation_results: dict[str, dict[str, Any]] = {}

    def health(self) -> dict[str, Any]:
        return {
            "api_version": CONTAINER_NODE_API_VERSION,
            "status": "ok",
            "node_id": self.node_id,
            "runtime_epoch": self.runtime_epoch,
            "payload_mode": "deterministic-size-preserving-fixture",
            "semantic_quality_enabled": False,
            "credentials_recorded": False,
        }

    def _fixture_key(self, operation: Mapping[str, Any], size: int) -> str:
        return (
            f"{operation['object_id']}|"
            f"{operation.get('representation_id') or 'explicit-bytes'}|"
            f"{size}"
        )

    def _fixture_path(self, operation: Mapping[str, Any], size: int) -> Path:
        key = self._fixture_key(operation, size)
        return self.state_dir / f"{hashlib.sha256(key.encode()).hexdigest()}.bin"

    def _ensure_fixture(
        self,
        operation: Mapping[str, Any],
        size: int,
    ) -> tuple[Path, float]:
        path = self._fixture_path(operation, size)
        if path.is_file() and path.stat().st_size == size:
            return path, 0.0
        started = time.perf_counter_ns()
        temporary = path.with_suffix(".tmp")
        block = _fixture_block(self._fixture_key(operation, size))
        with temporary.open("wb") as handle:
            remaining = size
            while remaining:
                take = min(remaining, len(block))
                handle.write(block[:take])
                remaining -= take
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        return path, (time.perf_counter_ns() - started) / 1_000_000.0

    def _read_fixture(self, path: Path, size: int) -> str:
        digest = hashlib.sha256()
        read = 0
        with path.open("rb") as handle:
            while True:
                block = handle.read(_CHUNK_BYTES)
                if not block:
                    break
                digest.update(block)
                read += len(block)
        _require(read == size, "fixture read did not return the exact logical bytes")
        return digest.hexdigest()

    def _cache(self, operation: Mapping[str, Any]) -> tuple[str, _CacheState]:
        binding = operation.get("cache_adapter")
        _require(isinstance(binding, Mapping), "operation has no cache adapter")
        cache_id = _text(binding.get("cache_id"), "cache_id")
        capacity = _integer(binding.get("capacity_bytes"), "cache capacity")
        initial_entries = binding.get("initial_entries")
        _require(isinstance(initial_entries, list), "cache initial_entries missing")
        with self._lock:
            cache = self._caches.get(cache_id)
            if cache is None:
                cache = _CacheState(capacity, initial_entries)
                self._caches[cache_id] = cache
            _require(cache.capacity_bytes == capacity, "cache capacity changed")
        return cache_id, cache

    def _send_payload(
        self,
        operation: Mapping[str, Any],
        size: int,
    ) -> tuple[str, float]:
        destination = operation.get("destination_url")
        if destination is None:
            destination = (
                f"http://{operation['destination_container']}:"
                f"{self.transfer_port}"
            )
        parsed = urlsplit(_text(destination, "destination_url"))
        _require(parsed.scheme == "http", "destination_url must use http")
        _require(parsed.hostname is not None, "destination_url has no host")
        _require(
            parsed.path in ("", "/"),
            "destination_url must not include a path",
        )
        key = self._fixture_key(operation, size)
        expected_digest = _payload_digest(key, size)
        link = operation.get("link_adapter")
        _require(isinstance(link, Mapping), "network operation has no link adapter")
        bandwidth = float(link["bandwidth_bytes_per_second"])
        rtt_ms = float(link["round_trip_time_ms"])
        _require(bandwidth > 0.0 and rtt_ms >= 0.0, "link shaping is invalid")
        started = time.perf_counter_ns()
        connection = http.client.HTTPConnection(
            parsed.hostname,
            parsed.port or self.transfer_port,
            timeout=60,
        )
        try:
            connection.putrequest("POST", "/v1/transfer/sink")
            connection.putheader("Content-Length", str(size))
            connection.putheader("Content-Type", "application/octet-stream")
            connection.putheader("X-Pathfinder-Payload-SHA256", expected_digest)
            connection.endheaders()
            block = _fixture_block(key)
            remaining = size
            while remaining:
                take = min(remaining, len(block))
                connection.send(block[:take])
                remaining -= take
            response = connection.getresponse()
            response_bytes = response.read(_MAX_JSON_BYTES + 1)
            _require(len(response_bytes) <= _MAX_JSON_BYTES, "sink response is too large")
            _require(response.status == 200, f"sink returned HTTP {response.status}")
            result = json.loads(response_bytes.decode("utf-8"))
            _require(result.get("sha256") == expected_digest, "sink digest mismatch")
            _require(result.get("bytes_received") == size, "sink byte count mismatch")
        finally:
            connection.close()
        elapsed = (time.perf_counter_ns() - started) / 1_000_000.0
        target_ms = rtt_ms + size / bandwidth * 1000.0
        remaining_ms = target_ms - elapsed
        if remaining_ms > 0.0:
            time.sleep(remaining_ms / 1000.0)
        return expected_digest, target_ms

    def execute(self, operation: Mapping[str, Any]) -> dict[str, Any]:
        """Execute once per operation key within this runtime epoch.

        A client may lose the HTTP response after the operation has already
        mutated cache state. Repeating that exact request returns the original
        result, while reusing the key for different input fails closed.
        """

        operation_key = _text(operation.get("operation_key"), "operation_key")
        try:
            request_bytes = json.dumps(
                operation,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ContainerNodeError("operation is not canonical JSON") from exc
        request_sha256 = hashlib.sha256(request_bytes).hexdigest()

        while True:
            with self._operation_condition:
                existing_sha256 = self._operation_request_sha256.get(
                    operation_key
                )
                _require(
                    existing_sha256 in (None, request_sha256),
                    "operation_key was reused with different input",
                )
                completed = self._operation_results.get(operation_key)
                if completed is not None:
                    replay = dict(completed)
                    replay["idempotent_replay"] = True
                    return replay
                if operation_key not in self._operations_in_flight:
                    self._operation_request_sha256[operation_key] = request_sha256
                    self._operations_in_flight.add(operation_key)
                    break
                self._operation_condition.wait()

        try:
            result = self._execute_once(operation)
        except BaseException:
            with self._operation_condition:
                self._operations_in_flight.discard(operation_key)
                self._operation_condition.notify_all()
            raise
        with self._operation_condition:
            stored = dict(result)
            stored["idempotent_replay"] = False
            self._operation_results[operation_key] = stored
            self._operations_in_flight.discard(operation_key)
            self._operation_condition.notify_all()
            return dict(stored)

    def _execute_once(self, operation: Mapping[str, Any]) -> dict[str, Any]:
        _require(
            operation.get("schema_version") == CONTAINER_OPERATION_SCHEMA_VERSION,
            "unsupported container operation schema_version",
        )
        _require(
            operation.get("execution_node_id") == self.node_id,
            "operation is assigned to a different node",
        )
        operation_key = _text(operation.get("operation_key"), "operation_key")
        kind = _text(operation.get("operation_kind"), "operation_kind")
        logical_bytes = _integer(operation.get("logical_bytes"), "logical_bytes")
        _require(
            logical_bytes <= self.max_operation_bytes,
            "operation exceeds the local-smoke byte safety limit",
        )
        fixture_path = None
        materialization_ms = 0.0
        if kind in ("storage_read", "cache_read"):
            # Concurrent repetitions may address the same fixture.  Protect
            # creation/replacement while allowing already-materialized files
            # to be read concurrently after this short critical section.
            with self._lock:
                fixture_path, materialization_ms = self._ensure_fixture(
                    operation,
                    logical_bytes,
                )
        started_ns = time.perf_counter_ns()
        physical_bytes = 0
        cache_result = None
        cache_evictions: list[str] = []
        payload_sha256 = None
        shaped_target_ms = None
        if kind in ("storage_read", "cache_read"):
            assert fixture_path is not None
            payload_sha256 = self._read_fixture(
                fixture_path,
                logical_bytes,
            )
            physical_bytes = logical_bytes
        elif kind == "network_transfer":
            payload_sha256, shaped_target_ms = self._send_payload(
                operation,
                logical_bytes,
            )
            physical_bytes = logical_bytes
        elif kind == "cache_lookup":
            _, cache = self._cache(operation)
            key = f"{operation['object_id']}:{operation.get('representation_id')}"
            with self._lock:
                cache_result = "hit" if cache.lookup(key) else "miss"
        elif kind == "cache_insert":
            _, cache = self._cache(operation)
            key = f"{operation['object_id']}:{operation.get('representation_id')}"
            with self._lock:
                cache_evictions = cache.insert(key, logical_bytes)
            # The local-smoke cache is a metadata LRU.  Do not claim payload
            # I/O that did not occur.
            physical_bytes = 0
        elif kind in ("barrier", "compute", "control", "index_query"):
            hashlib.sha256(operation_key.encode("utf-8")).digest()
        else:
            raise ContainerNodeError(f"unsupported operation kind: {kind}")
        finished_ns = time.perf_counter_ns()
        return {
            "schema_version": CONTAINER_NODE_RESULT_SCHEMA_VERSION,
            "api_version": CONTAINER_NODE_API_VERSION,
            "status": "completed",
            "outcome_type": "completed",
            "telemetry_complete": True,
            "operation_key": operation_key,
            "operation_kind": kind,
            "execution_node_id": self.node_id,
            "started_monotonic_ns": started_ns,
            "finished_monotonic_ns": finished_ns,
            "service_time_ms": (finished_ns - started_ns) / 1_000_000.0,
            "fixture_materialization_ms_excluded_from_storage_measurement": (
                materialization_ms
            ),
            "application_shaping_target_ms": shaped_target_ms,
            "logical_bytes": logical_bytes,
            "physical_bytes": physical_bytes,
            "payload_sha256": payload_sha256,
            "cache_result": cache_result,
            "cache_evictions": cache_evictions,
            "infrastructure_operation_success": True,
            "semantic_task_quality_evaluated": False,
            "credentials_recorded": False,
        }


class ContainerNodeHTTPServer(ThreadingHTTPServer):
    runtime: ContainerNodeRuntime


class ContainerNodeRequestHandler(BaseHTTPRequestHandler):
    server: ContainerNodeHTTPServer

    def log_message(self, format: str, *args: object) -> None:
        return

    def _write_json(self, status: int, payload: Mapping[str, Any]) -> None:
        encoded = (
            json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self) -> None:
        if self.path == "/healthz":
            self._write_json(200, self.server.runtime.health())
            return
        self._write_json(404, {"status": "error", "message": "not found"})

    def do_POST(self) -> None:
        try:
            length = _integer(int(self.headers.get("Content-Length", "-1")), "length")
            if self.path == "/v1/transfer/sink":
                _require(
                    length <= self.server.runtime.max_operation_bytes,
                    "transfer exceeds the local-smoke byte safety limit",
                )
                expected = _text(
                    self.headers.get("X-Pathfinder-Payload-SHA256"),
                    "payload digest",
                )
                _require(
                    _SHA256.fullmatch(expected) is not None,
                    "payload digest must be lowercase SHA-256",
                )
                digest = hashlib.sha256()
                remaining = length
                while remaining:
                    block = self.rfile.read(min(remaining, _CHUNK_BYTES))
                    _require(bool(block), "transfer ended before Content-Length")
                    digest.update(block)
                    remaining -= len(block)
                actual = digest.hexdigest()
                _require(actual == expected, "received payload digest mismatch")
                self._write_json(200, {
                    "status": "complete",
                    "node_id": self.server.runtime.node_id,
                    "bytes_received": length,
                    "sha256": actual,
                    "credentials_recorded": False,
                })
                return
            _require(self.path == "/v1/operations/execute", "not found")
            _require(length <= _MAX_JSON_BYTES, "operation request is too large")
            raw = self.rfile.read(length)
            _require(len(raw) == length, "operation request is truncated")
            operation = json.loads(raw.decode("utf-8"))
            _require(isinstance(operation, Mapping), "operation must be an object")
            self._write_json(200, self.server.runtime.execute(operation))
        except (ContainerNodeError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
            self._write_json(400, {"status": "error", "message": str(exc)})
        except Exception as exc:
            self._write_json(500, {
                "status": "error",
                "message": f"container node internal error: {type(exc).__name__}",
            })


def create_container_node_server(
    node_id: str,
    state_dir: str | Path,
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    max_operation_bytes: int = 1024 * 1024 * 1024,
) -> ContainerNodeHTTPServer:
    """Create, but do not start, one local container-node HTTP server."""

    runtime = ContainerNodeRuntime(
        node_id,
        state_dir,
        max_operation_bytes=max_operation_bytes,
        transfer_port=port if port else 9080,
    )
    server = ContainerNodeHTTPServer((host, port), ContainerNodeRequestHandler)
    server.runtime = runtime
    if port == 0:
        runtime.transfer_port = int(server.server_address[1])
    return server


def serve_container_node(
    node_id: str,
    state_dir: str | Path,
    *,
    host: str = "0.0.0.0",
    port: int = 9080,
    max_operation_bytes: int = 1024 * 1024 * 1024,
) -> None:
    """Run one node service and drain it cleanly on termination signals."""

    server = create_container_node_server(
        node_id,
        state_dir,
        host=host,
        port=port,
        max_operation_bytes=max_operation_bytes,
    )
    shutdown_requested = threading.Event()
    server_loop_finished = threading.Event()
    previous_handlers: dict[int, Any] = {}

    def coordinate_shutdown() -> None:
        shutdown_requested.wait()
        if not server_loop_finished.is_set():
            # BaseServer.shutdown() must run in a thread other than the one
            # executing serve_forever(), otherwise it deadlocks.
            server.shutdown()

    coordinator = threading.Thread(
        target=coordinate_shutdown,
        name=f"pathfinder-{node_id}-shutdown",
        daemon=True,
    )
    coordinator.start()

    def request_shutdown(signum: int, frame: Any) -> None:
        del signum, frame
        # Keep the signal handler minimal and idempotent.  The coordinator
        # performs the blocking shutdown call outside the signal context.
        shutdown_requested.set()

    install_signal_handlers = (
        threading.current_thread() is threading.main_thread()
    )
    if install_signal_handlers:
        for signal_name in ("SIGTERM", "SIGINT"):
            signal_number = getattr(signal, signal_name, None)
            if signal_number is None:
                continue
            previous_handlers[signal_number] = signal.getsignal(signal_number)
            signal.signal(signal_number, request_shutdown)

    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        server_loop_finished.set()
        shutdown_requested.set()
        if install_signal_handlers:
            for signal_number, previous_handler in previous_handlers.items():
                signal.signal(signal_number, previous_handler)
        server.server_close()
        coordinator.join(timeout=1.0)
