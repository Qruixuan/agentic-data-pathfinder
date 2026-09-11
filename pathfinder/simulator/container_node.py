"""Small standard-library node service for local container emulation.

By default, a node executes infrastructure operations only and reports that
semantic quality is unavailable.  An explicitly enabled semantic endpoint can
be added to one executor node; it obtains all LLM configuration at runtime and
never writes a prompt or credential to the node result.  Synthetic payloads
remain deterministic, size preserving, bounded, and written beneath a
dedicated state directory so infrastructure tests need neither a benchmark
dataset nor an LLM.
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
from pathlib import Path, PurePosixPath
from typing import Any, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import Request, urlopen

from .container_contract import (
    CONTAINER_NODE_RESULT_LEGACY_SCHEMA_VERSION,
    CONTAINER_NODE_RESULT_SCHEMA_VERSION,
    CONTAINER_OPERATION_LEGACY_SCHEMA_VERSION,
    CONTAINER_OPERATION_SCHEMA_VERSION,
)


CONTAINER_NODE_API_VERSION = "pathfinder.container-node/v1alpha1"
CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION = (
    "pathfinder.container-node-semantic-request/v1alpha1"
)
CONTAINER_NODE_SEMANTIC_RESULT_SCHEMA_VERSION = (
    "pathfinder.container-node-semantic-result/v1alpha1"
)

_CHUNK_BYTES = 64 * 1024
_MAX_JSON_BYTES = 2 * 1024 * 1024
_MAX_SEMANTIC_PROMPT_BYTES = 1024 * 1024
_SHA256 = re.compile(r"[0-9a-f]{64}")
_RUNTIME_EPOCH = re.compile(r"[0-9a-f]{32}")


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


def _runtime_epoch(value: Any, name: str) -> str:
    epoch = _text(value, name)
    _require(
        _RUNTIME_EPOCH.fullmatch(epoch) is not None,
        f"{name} must be a lowercase runtime epoch",
    )
    return epoch


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
        enable_semantic_llm: bool = False,
        semantic_artifact_root: str | Path | None = None,
        semantic_allowed_source_containers: tuple[str, ...] = (),
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
        _require(
            type(enable_semantic_llm) is bool,
            "enable_semantic_llm must be a boolean",
        )
        self.enable_semantic_llm = enable_semantic_llm
        if semantic_artifact_root is None:
            self.semantic_artifact_root: Path | None = None
        else:
            artifact_root = Path(semantic_artifact_root).resolve()
            _require(
                artifact_root.is_dir(),
                "semantic artifact root must be a directory",
            )
            self.semantic_artifact_root = artifact_root
        self.semantic_allowed_source_containers = frozenset(
            _text(value, "semantic allowed source container")
            for value in semantic_allowed_source_containers
        )
        _require(
            len(self.semantic_allowed_source_containers)
            == len(semantic_allowed_source_containers),
            "semantic allowed source containers contain duplicates",
        )
        self.runtime_epoch = uuid.uuid4().hex
        self._caches: dict[tuple[str, str], _CacheState] = {}
        self._lock = threading.RLock()
        self._operation_condition = threading.Condition(self._lock)
        self._operation_request_sha256: dict[str, str] = {}
        self._operations_in_flight: set[str] = set()
        self._operation_results: dict[str, dict[str, Any]] = {}
        self._semantic_request_sha256: dict[str, str] = {}
        self._semantic_requests_in_flight: set[str] = set()
        self._semantic_results: dict[str, dict[str, Any]] = {}

    def health(self) -> dict[str, Any]:
        configured = all(
            isinstance(os.environ.get(name), str)
            and bool(os.environ[name].strip())
            for name in (
                "PATHFINDER_SEMANTIC_LLM_BASE_URL",
                "PATHFINDER_SEMANTIC_LLM_MODEL",
                "PATHFINDER_SEMANTIC_LLM_API_KEY",
            )
        )
        return {
            "api_version": CONTAINER_NODE_API_VERSION,
            "status": "ok",
            "node_id": self.node_id,
            "runtime_epoch": self.runtime_epoch,
            "operation_result_schema_version": (
                CONTAINER_NODE_RESULT_SCHEMA_VERSION
            ),
            "payload_mode": "deterministic-size-preserving-fixture",
            "semantic_quality_enabled": self.enable_semantic_llm,
            "semantic_llm_configured": configured,
            "semantic_artifact_serving": self.semantic_artifact_root is not None,
            "credentials_recorded": False,
        }

    def _semantic_artifact_path(self, relative_path: str) -> Path:
        _require(
            self.semantic_artifact_root is not None,
            "semantic artifact endpoint is disabled on this node",
        )
        relative = PurePosixPath(
            _text(relative_path, "representation_path").replace("\\", "/")
        )
        _require(not relative.is_absolute(), "representation_path must be relative")
        _require(
            ".." not in relative.parts and "." not in relative.parts and bool(relative.parts),
            "representation_path escapes artifact root",
        )
        _require(
            relative.suffix in {".txt", ".json"},
            "semantic artifact must be a UTF-8 text representation",
        )
        candidate = (self.semantic_artifact_root / Path(*relative.parts)).resolve()
        try:
            candidate.relative_to(self.semantic_artifact_root)
        except ValueError as exc:
            raise ContainerNodeError("representation_path escapes artifact root") from exc
        _require(candidate.is_file(), "semantic representation does not exist")
        _require(
            candidate.stat().st_size <= _MAX_SEMANTIC_PROMPT_BYTES,
            "semantic representation exceeds the local safety limit",
        )
        return candidate

    def read_semantic_representation(self, relative_path: str) -> tuple[bytes, str]:
        """Read one bounded immutable text representation for an allowed peer."""

        path = self._semantic_artifact_path(relative_path)
        try:
            payload = path.read_bytes()
            payload.decode("utf-8")
        except (OSError, UnicodeError) as exc:
            raise ContainerNodeError("semantic representation is not readable UTF-8") from exc
        _require(bool(payload), "semantic representation is empty")
        return payload, hashlib.sha256(payload).hexdigest()

    def _fetch_semantic_representation(
        self,
        *,
        source_container_url: str,
        source_node_id: str,
        relative_path: str,
        expected_sha256: str,
    ) -> tuple[bytes, str]:
        parsed = urlsplit(_text(source_container_url, "source_container_url"))
        _require(parsed.scheme == "http", "semantic source URL must use http")
        _require(parsed.hostname is not None, "semantic source URL must name a container")
        _require(parsed.path in ("", "/"), "semantic source URL must not include a path")
        _require(
            parsed.hostname in self.semantic_allowed_source_containers,
            "semantic source container is not allowed for this executor",
        )
        _require(
            parsed.port in (None, self.transfer_port),
            "semantic source URL has an unexpected port",
        )
        _require(
            _SHA256.fullmatch(expected_sha256) is not None,
            "expected representation digest is invalid",
        )
        target = "/v1/semantic/representation/read?" + urlencode({"path": relative_path})
        connection = http.client.HTTPConnection(
            parsed.hostname,
            parsed.port or self.transfer_port,
            timeout=60,
        )
        try:
            connection.request(
                "GET",
                target,
                headers={"Accept": "application/octet-stream"},
            )
            response = connection.getresponse()
            payload = response.read(_MAX_SEMANTIC_PROMPT_BYTES + 1)
            _require(
                response.status == 200,
                f"semantic source returned HTTP {response.status}",
            )
            _require(
                len(payload) <= _MAX_SEMANTIC_PROMPT_BYTES,
                "semantic source payload is too large",
            )
            digest = response.getheader("X-Pathfinder-Representation-SHA256")
            source = response.getheader("X-Pathfinder-Source-Node")
            _require(digest == expected_sha256, "semantic source digest header changed")
            _require(source == source_node_id, "semantic source node header changed")
            _require(
                hashlib.sha256(payload).hexdigest() == expected_sha256,
                "semantic source payload digest changed",
            )
            payload.decode("utf-8")
        except (OSError, UnicodeError) as exc:
            raise ContainerNodeError(
                f"semantic source transfer failed: {type(exc).__name__}"
            ) from exc
        finally:
            connection.close()
        return payload, expected_sha256

    @staticmethod
    def build_semantic_prompt(
        representation_id: str,
        representation_text: str,
        question: str,
    ) -> str:
        return (
            "You are executing a controlled Pathfinder semantic task.\n"
            "Use only the supplied precomputed representation. Do not use outside knowledge.\n\n"
            f"Representation ID: {representation_id}\n"
            "--- representation begins ---\n"
            f"{representation_text}\n"
            "--- representation ends ---\n\n"
            f"{question}"
        )

    def _semantic_llm_configuration(self) -> tuple[str, str, str, float]:
        _require(
            self.enable_semantic_llm,
            "semantic LLM endpoint is disabled on this node",
        )
        base_url = _text(
            os.environ.get("PATHFINDER_SEMANTIC_LLM_BASE_URL"),
            "PATHFINDER_SEMANTIC_LLM_BASE_URL",
        ).rstrip("/")
        model = _text(
            os.environ.get("PATHFINDER_SEMANTIC_LLM_MODEL"),
            "PATHFINDER_SEMANTIC_LLM_MODEL",
        )
        api_key = _text(
            os.environ.get("PATHFINDER_SEMANTIC_LLM_API_KEY"),
            "PATHFINDER_SEMANTIC_LLM_API_KEY",
        )
        parsed = urlsplit(base_url)
        _require(parsed.scheme in ("http", "https"), "LLM base URL must be HTTP(S)")
        _require(parsed.hostname is not None, "LLM base URL must name a host")
        _require(parsed.username is None and parsed.password is None, "LLM base URL must not embed credentials")
        _require(
            parsed.scheme == "https"
            or parsed.hostname in {"127.0.0.1", "localhost", "::1"},
            "non-local LLM base URL must use HTTPS",
        )
        raw_timeout = os.environ.get("PATHFINDER_SEMANTIC_LLM_TIMEOUT_SECONDS", "180")
        try:
            timeout = float(raw_timeout)
        except ValueError as exc:
            raise ContainerNodeError("semantic LLM timeout must be numeric") from exc
        _require(timeout > 0.0, "semantic LLM timeout must be positive")
        return base_url, model, api_key, timeout

    def _call_semantic_llm(self, prompt: str) -> tuple[str, str]:
        base_url, model, api_key, timeout = self._semantic_llm_configuration()
        body = json.dumps(
            {
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0,
            },
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        request = Request(
            base_url + "/chat/completions",
            data=body,
            method="POST",
            headers={
                "Authorization": "Bearer " + api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        try:
            with urlopen(request, timeout=timeout) as response:
                raw = response.read(_MAX_JSON_BYTES + 1)
        except HTTPError as exc:
            raise ContainerNodeError(
                f"semantic LLM request failed with HTTP {exc.code}"
            ) from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise ContainerNodeError(
                f"semantic LLM request failed: {type(exc).__name__}"
            ) from exc
        _require(len(raw) <= _MAX_JSON_BYTES, "semantic LLM response is too large")
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ContainerNodeError("semantic LLM response is not valid JSON") from exc
        _require(isinstance(payload, Mapping), "semantic LLM response must be an object")
        choices = payload.get("choices")
        _require(isinstance(choices, list) and bool(choices), "semantic LLM response has no choices")
        first = choices[0]
        _require(isinstance(first, Mapping), "semantic LLM choice must be an object")
        message = first.get("message")
        _require(isinstance(message, Mapping), "semantic LLM choice has no message")
        answer = message.get("content")
        _require(isinstance(answer, str), "semantic LLM answer must be text")
        return answer, model

    def semantic_complete(self, request: Mapping[str, Any]) -> dict[str, Any]:
        """Run one idempotent, credential-free-recording semantic request.

        Only the request digest and result are retained in node memory.  The
        prompt and API credential are never written to a node result, ledger,
        or health response.
        """

        _require(
            request.get("schema_version")
            == CONTAINER_NODE_SEMANTIC_REQUEST_SCHEMA_VERSION,
            "unsupported semantic request schema_version",
        )
        request_id = _text(request.get("semantic_request_id"), "semantic_request_id")
        try:
            request_bytes = json.dumps(
                request,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ContainerNodeError("semantic request is not canonical JSON") from exc
        request_sha256 = hashlib.sha256(request_bytes).hexdigest()

        while True:
            with self._operation_condition:
                existing_sha256 = self._semantic_request_sha256.get(request_id)
                _require(
                    existing_sha256 in (None, request_sha256),
                    "semantic_request_id was reused with different input",
                )
                completed = self._semantic_results.get(request_id)
                if completed is not None:
                    replay = dict(completed)
                    replay["idempotent_replay"] = True
                    return replay
                if request_id not in self._semantic_requests_in_flight:
                    self._semantic_request_sha256[request_id] = request_sha256
                    self._semantic_requests_in_flight.add(request_id)
                    break
                self._operation_condition.wait()

        try:
            result = self._semantic_complete_once(request, request_sha256)
        except BaseException:
            with self._operation_condition:
                self._semantic_requests_in_flight.discard(request_id)
                self._operation_condition.notify_all()
            raise
        with self._operation_condition:
            stored = dict(result)
            stored["idempotent_replay"] = False
            self._semantic_results[request_id] = stored
            self._semantic_requests_in_flight.discard(request_id)
            self._operation_condition.notify_all()
            return dict(stored)

    def _semantic_complete_once(
        self,
        request: Mapping[str, Any],
        request_sha256: str,
    ) -> dict[str, Any]:
        _require(
            request.get("execution_node_id") == self.node_id,
            "semantic request is assigned to a different node",
        )
        request_id = _text(request.get("semantic_request_id"), "semantic_request_id")
        representation_sha256 = _text(
            request.get("representation_sha256"),
            "representation_sha256",
        )
        _require(
            _SHA256.fullmatch(representation_sha256) is not None,
            "representation_sha256 must be lowercase SHA-256",
        )
        source_node_id: str | None = None
        representation_delivery_bytes: int | None = None
        if "prompt" in request:
            _require(
                "source_container_url" not in request,
                "direct semantic prompt cannot name a source container",
            )
            prompt = _text(request.get("prompt"), "semantic prompt")
            route_coupled = False
        else:
            source_node_id = _text(request.get("source_node_id"), "source_node_id")
            source_container_url = _text(
                request.get("source_container_url"),
                "source_container_url",
            )
            representation_path = _text(
                request.get("representation_path"),
                "representation_path",
            )
            representation_id = _text(
                request.get("representation_id"),
                "representation_id",
            )
            question = _text(request.get("question"), "question")
            payload, observed_sha256 = self._fetch_semantic_representation(
                source_container_url=source_container_url,
                source_node_id=source_node_id,
                relative_path=representation_path,
                expected_sha256=representation_sha256,
            )
            _require(
                observed_sha256 == representation_sha256,
                "semantic representation digest changed",
            )
            prompt = self.build_semantic_prompt(
                representation_id,
                payload.decode("utf-8"),
                question,
            )
            route_coupled = True
            representation_delivery_bytes = len(payload)
        prompt_bytes = prompt.encode("utf-8")
        _require(
            len(prompt_bytes) <= _MAX_SEMANTIC_PROMPT_BYTES,
            "semantic prompt exceeds the local safety limit",
        )
        prompt_sha256 = request.get("prompt_sha256")
        if prompt_sha256 is None:
            prompt_sha256 = hashlib.sha256(prompt_bytes).hexdigest()
        prompt_sha256 = _text(prompt_sha256, "prompt_sha256")
        _require(
            _SHA256.fullmatch(prompt_sha256) is not None,
            "prompt_sha256 must be lowercase SHA-256",
        )
        _require(
            hashlib.sha256(prompt_bytes).hexdigest() == prompt_sha256,
            "semantic prompt digest mismatch",
        )
        started_ns = time.perf_counter_ns()
        answer, model = self._call_semantic_llm(prompt)
        finished_ns = time.perf_counter_ns()
        return {
            "schema_version": CONTAINER_NODE_SEMANTIC_RESULT_SCHEMA_VERSION,
            "api_version": CONTAINER_NODE_API_VERSION,
            "status": "completed",
            "outcome_type": "completed",
            "telemetry_complete": True,
            "semantic_request_id": request_id,
            "execution_node_id": self.node_id,
            "started_monotonic_ns": started_ns,
            "finished_monotonic_ns": finished_ns,
            "service_time_ms": (finished_ns - started_ns) / 1_000_000.0,
            "request_sha256": request_sha256,
            "prompt_sha256": prompt_sha256,
            "representation_sha256": representation_sha256,
            "data_plane_artifact_delivery_verified": route_coupled,
            "source_node_id": source_node_id,
            "representation_delivery_bytes": representation_delivery_bytes,
            "model": model,
            "final_answer": answer,
            "final_answer_sha256": hashlib.sha256(answer.encode("utf-8")).hexdigest(),
            "llm_called": True,
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

    def _cache(
        self,
        operation: Mapping[str, Any],
    ) -> tuple[str, str, _CacheState]:
        binding = operation.get("cache_adapter")
        _require(isinstance(binding, Mapping), "operation has no cache adapter")
        cache_id = _text(binding.get("cache_id"), "cache_id")
        raw_scope = operation.get("cache_scope_id")
        if raw_scope is None:
            # Historical v1alpha1 ledgers had no namespace.  Continue to read
            # them for old smoke artifacts, but never confuse that behavior
            # with the scoped semantics required by a newly frozen matrix.
            scope_id = "legacy-unscoped-v1"
        else:
            scope_id = _text(raw_scope, "cache_scope_id")
        capacity = _integer(binding.get("capacity_bytes"), "cache capacity")
        initial_entries = binding.get("initial_entries")
        _require(isinstance(initial_entries, list), "cache initial_entries missing")
        state_key = (cache_id, scope_id)
        with self._lock:
            cache = self._caches.get(state_key)
            if cache is None:
                cache = _CacheState(capacity, initial_entries)
                self._caches[state_key] = cache
            _require(cache.capacity_bytes == capacity, "cache capacity changed")
        return cache_id, scope_id, cache

    def _send_payload(
        self,
        operation: Mapping[str, Any],
        size: int,
    ) -> tuple[str, float, float, float, str]:
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
            _require(
                result.get("node_id") == operation.get("destination_node_id"),
                "sink node identity mismatch",
            )
            destination_runtime_epoch = _runtime_epoch(
                result.get("runtime_epoch"),
                "sink runtime_epoch",
            )
        finally:
            connection.close()
        http_exchange_ms = (time.perf_counter_ns() - started) / 1_000_000.0
        target_ms = rtt_ms + size / bandwidth * 1000.0
        remaining_ms = target_ms - http_exchange_ms
        shaping_sleep_ms = 0.0
        if remaining_ms > 0.0:
            sleep_started = time.perf_counter_ns()
            time.sleep(remaining_ms / 1000.0)
            shaping_sleep_ms = (
                time.perf_counter_ns() - sleep_started
            ) / 1_000_000.0
        return (
            expected_digest,
            target_ms,
            http_exchange_ms,
            shaping_sleep_ms,
            destination_runtime_epoch,
        )

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
            operation.get("schema_version") in {
                CONTAINER_OPERATION_SCHEMA_VERSION,
                CONTAINER_OPERATION_LEGACY_SCHEMA_VERSION,
            },
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
        cache_scope_id = None
        payload_sha256 = None
        shaped_target_ms = None
        network_http_exchange_ms = None
        application_shaping_sleep_ms = None
        destination_runtime_epoch = None
        if kind == "storage_read":
            assert fixture_path is not None
            payload_sha256 = self._read_fixture(
                fixture_path,
                logical_bytes,
            )
            physical_bytes = logical_bytes
        elif kind == "cache_read":
            # A cache-hit branch is not entitled to read a local fixture just
            # because a prior lookup once said "hit".  For current scoped
            # ledgers, re-check the same cache namespace immediately before
            # serving the data.  This makes a restart/lost entry fail closed
            # instead of turning a stale hit into a successful local read.
            if operation.get("cache_adapter") is None:
                _require(
                    operation.get("schema_version")
                    == CONTAINER_OPERATION_LEGACY_SCHEMA_VERSION,
                    "cache_read requires a cache adapter in a scoped ledger",
                )
            else:
                _, cache_scope_id, cache = self._cache(operation)
                cache_key = (
                    f"{operation['object_id']}:"
                    f"{operation.get('representation_id')}"
                )
                with self._lock:
                    _require(
                        cache.lookup(cache_key),
                        "cache_read is not backed by a current cache entry",
                    )
                cache_result = "hit"
            assert fixture_path is not None
            payload_sha256 = self._read_fixture(
                fixture_path,
                logical_bytes,
            )
            physical_bytes = logical_bytes
        elif kind == "network_transfer":
            (
                payload_sha256,
                shaped_target_ms,
                network_http_exchange_ms,
                application_shaping_sleep_ms,
                destination_runtime_epoch,
            ) = self._send_payload(operation, logical_bytes)
            physical_bytes = logical_bytes
        elif kind == "cache_lookup":
            _, cache_scope_id, cache = self._cache(operation)
            key = f"{operation['object_id']}:{operation.get('representation_id')}"
            with self._lock:
                cache_result = "hit" if cache.lookup(key) else "miss"
        elif kind == "cache_insert":
            _, cache_scope_id, cache = self._cache(operation)
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
            "runtime_epoch": self.runtime_epoch,
            "destination_runtime_epoch": destination_runtime_epoch,
            "started_monotonic_ns": started_ns,
            "finished_monotonic_ns": finished_ns,
            "service_time_ms": (finished_ns - started_ns) / 1_000_000.0,
            "fixture_materialization_ms_excluded_from_storage_measurement": (
                materialization_ms
            ),
            "application_shaping_target_ms": shaped_target_ms,
            "network_http_exchange_ms": network_http_exchange_ms,
            "application_shaping_sleep_ms": application_shaping_sleep_ms,
            "logical_bytes": logical_bytes,
            "physical_bytes": physical_bytes,
            "payload_sha256": payload_sha256,
            "cache_result": cache_result,
            "cache_evictions": cache_evictions,
            "cache_scope_id": cache_scope_id,
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

    def _write_representation(self, payload: bytes, digest: str) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("X-Pathfinder-Representation-SHA256", digest)
        self.send_header("X-Pathfinder-Source-Node", self.server.runtime.node_id)
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:
        try:
            parsed = urlsplit(self.path)
            if parsed.path == "/healthz" and not parsed.query:
                self._write_json(200, self.server.runtime.health())
                return
            if parsed.path == "/v1/semantic/representation/read":
                query = parse_qs(parsed.query, keep_blank_values=True)
                _require(set(query) == {"path"}, "semantic representation query is invalid")
                values = query["path"]
                _require(len(values) == 1, "semantic representation path is invalid")
                payload, digest = self.server.runtime.read_semantic_representation(
                    values[0]
                )
                self._write_representation(payload, digest)
                return
            self._write_json(404, {"status": "error", "message": "not found"})
        except (ContainerNodeError, ValueError) as exc:
            self._write_json(400, {"status": "error", "message": str(exc)})

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
                    "api_version": CONTAINER_NODE_API_VERSION,
                    "node_id": self.server.runtime.node_id,
                    "runtime_epoch": self.server.runtime.runtime_epoch,
                    "bytes_received": length,
                    "sha256": actual,
                    "credentials_recorded": False,
                })
                return
            if self.path == "/v1/semantic/chat-completions":
                _require(
                    length <= _MAX_SEMANTIC_PROMPT_BYTES + _MAX_JSON_BYTES,
                    "semantic request exceeds the local safety limit",
                )
                raw = self.rfile.read(length)
                _require(len(raw) == length, "semantic request is truncated")
                semantic_request = json.loads(raw.decode("utf-8"))
                _require(
                    isinstance(semantic_request, Mapping),
                    "semantic request must be an object",
                )
                self._write_json(
                    200,
                    self.server.runtime.semantic_complete(semantic_request),
                )
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
    enable_semantic_llm: bool = False,
    semantic_artifact_root: str | Path | None = None,
    semantic_allowed_source_containers: tuple[str, ...] = (),
) -> ContainerNodeHTTPServer:
    """Create, but do not start, one local container-node HTTP server."""

    runtime = ContainerNodeRuntime(
        node_id,
        state_dir,
        max_operation_bytes=max_operation_bytes,
        transfer_port=port if port else 9080,
        enable_semantic_llm=enable_semantic_llm,
        semantic_artifact_root=semantic_artifact_root,
        semantic_allowed_source_containers=semantic_allowed_source_containers,
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
    enable_semantic_llm: bool = False,
    semantic_artifact_root: str | Path | None = None,
    semantic_allowed_source_containers: tuple[str, ...] = (),
) -> None:
    """Run one node service and drain it cleanly on termination signals."""

    server = create_container_node_server(
        node_id,
        state_dir,
        host=host,
        port=port,
        max_operation_bytes=max_operation_bytes,
        enable_semantic_llm=enable_semantic_llm,
        semantic_artifact_root=semantic_artifact_root,
        semantic_allowed_source_containers=semantic_allowed_source_containers,
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
