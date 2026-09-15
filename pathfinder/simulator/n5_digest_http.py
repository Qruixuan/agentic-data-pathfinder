"""Durable authenticated HTTP adapter for N5 semantic digest generation.

Frozen digest plans remain endpoint- and credential-free.  Deployment passes
an immutable plan registry, a runtime-only vision adapter, and a bearer token
to this service.  Source MP4 bytes are staged by content digest before an
idempotent request executes the existing verified N5 materializer.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import http.server
import io
import json
import os
import re
import tempfile
import threading
import urllib.parse
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Callable, Mapping, Sequence

from .n5_digest_materialization import (
    DIGEST_NAME,
    PLAN_NAME,
    N5DigestMaterializationError,
    OpenAICompatibleVisionDigestAdapter,
    VisionDigestAdapter,
    materialize_n5_multimodal_digest,
    verify_n5_multimodal_digest_materialization,
)


N5_DIGEST_HTTP_API_VERSION = (
    "pathfinder.simulator-n5-digest-http/v1alpha1"
)
N5_DIGEST_HTTP_EXECUTE_SCHEMA_VERSION = (
    "pathfinder.simulator-n5-digest-http-execute/v1alpha1"
)
N5_DIGEST_HTTP_RESULT_SCHEMA_VERSION = (
    "pathfinder.simulator-n5-digest-http-result/v1alpha1"
)

_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,255}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_DEFAULT_MAX_SOURCE_BYTES = 512 * 1024 * 1024
_DEFAULT_MAX_JSON_BYTES = 1024 * 1024
_DEFAULT_MAX_RESULT_BYTES = 4 * 1024 * 1024


class N5DigestHTTPError(RuntimeError):
    """Raised for invalid HTTP adapter state or request bindings."""


class N5DigestHTTPConflict(N5DigestHTTPError):
    """Raised when an idempotency key is reused for another request."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise N5DigestHTTPError(message)


def _identifier(value: Any, label: str) -> str:
    _require(
        isinstance(value, str) and _SAFE_ID.fullmatch(value) is not None,
        f"{label} is invalid",
    )
    return value


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise N5DigestHTTPError("N5 digest value is not canonical JSON") from exc


def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _strict_json(payload: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_pairs,
            parse_constant=lambda item: (_ for _ in ()).throw(
                N5DigestHTTPError(
                    f"{label} contains non-finite number {item}"
                )
            ),
        )
    except N5DigestHTTPError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise N5DigestHTTPError(f"cannot parse {label}") from exc
    _require(isinstance(value, dict), f"{label} must be an object")
    _require(payload == _canonical_bytes(value), f"{label} is not canonical JSON")
    return value


def _sha256_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("rb") as handle:
            while block := handle.read(1024 * 1024):
                size += len(block)
                digest.update(block)
    except OSError as exc:
        raise N5DigestHTTPError("cannot read staged N5 source") from exc
    return size, digest.hexdigest()


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, raw = tempfile.mkstemp(
        prefix=".n5-http-",
        dir=path.parent,
    )
    temporary = Path(raw)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


@dataclass(frozen=True)
class _PlanBinding:
    plan_id: str
    plan_dir: Path
    object_id: str
    source_size_bytes: int
    source_sha256: str


def _load_plan_binding(plan_dir: Path) -> _PlanBinding:
    path = plan_dir / PLAN_NAME
    _require(path.is_file() and not path.is_symlink(), "N5 digest plan is missing")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise N5DigestHTTPError("cannot read N5 digest plan registry") from exc
    _require(isinstance(value, dict), "N5 digest plan must be an object")
    source = value.get("source")
    _require(isinstance(source, Mapping), "N5 digest plan source is invalid")
    plan_id = _identifier(value.get("plan_id"), "plan_id")
    object_id = _identifier(value.get("object_id"), "object_id")
    size = source.get("size_bytes")
    digest = source.get("sha256")
    _require(
        type(size) is int and size > 0,
        "N5 digest plan source size is invalid",
    )
    _require(
        isinstance(digest, str) and _SHA256.fullmatch(digest) is not None,
        "N5 digest plan source digest is invalid",
    )
    return _PlanBinding(
        plan_id=plan_id,
        plan_dir=plan_dir.resolve(),
        object_id=object_id,
        source_size_bytes=size,
        source_sha256=digest,
    )


@dataclass(frozen=True)
class N5DigestHTTPSettings:
    bearer_token: str
    host: str = "127.0.0.1"
    port: int = 0
    max_source_bytes: int = _DEFAULT_MAX_SOURCE_BYTES
    max_json_bytes: int = _DEFAULT_MAX_JSON_BYTES
    max_result_bytes: int = _DEFAULT_MAX_RESULT_BYTES

    def __post_init__(self) -> None:
        _require(
            isinstance(self.bearer_token, str) and bool(self.bearer_token),
            "N5 digest bearer token is required",
        )
        _require(
            isinstance(self.host, str) and bool(self.host),
            "N5 digest listen host is invalid",
        )
        _require(
            type(self.port) is int and 0 <= self.port <= 65535,
            "N5 digest listen port is invalid",
        )
        for name in (
            "max_source_bytes",
            "max_json_bytes",
            "max_result_bytes",
        ):
            value = getattr(self, name)
            _require(
                type(value) is int and value > 0,
                f"{name} must be a positive integer",
            )


class N5DigestHTTPService:
    """Content-bound and durable adapter for a frozen digest-plan registry."""

    def __init__(
        self,
        state_dir: str | Path,
        plan_directories: Sequence[str | Path],
        *,
        vision_adapter: VisionDigestAdapter,
        settings: N5DigestHTTPSettings,
        sampler: Callable[..., Any] | None = None,
    ) -> None:
        _require(
            isinstance(plan_directories, Sequence)
            and not isinstance(plan_directories, (str, bytes))
            and bool(plan_directories),
            "N5 digest plan registry is empty",
        )
        _require(
            isinstance(settings, N5DigestHTTPSettings),
            "N5 digest HTTP settings are invalid",
        )
        _require(
            callable(getattr(vision_adapter, "generate_digest", None)),
            "N5 digest vision adapter is invalid",
        )
        _require(
            sampler is None or callable(sampler),
            "N5 digest sampler is invalid",
        )
        bindings = [
            _load_plan_binding(Path(path).resolve())
            for path in plan_directories
        ]
        self.plans: dict[str, _PlanBinding] = {}
        self.allowed_sources: dict[str, int] = {}
        for binding in bindings:
            _require(
                binding.plan_id not in self.plans,
                "N5 digest plan registry repeats a plan_id",
            )
            self.plans[binding.plan_id] = binding
            previous_size = self.allowed_sources.setdefault(
                binding.source_sha256,
                binding.source_size_bytes,
            )
            _require(
                previous_size == binding.source_size_bytes,
                "N5 digest plans disagree on a source size",
            )
            _require(
                binding.source_size_bytes <= settings.max_source_bytes,
                "registered N5 source exceeds max_source_bytes",
            )
        self.state_dir = Path(state_dir).resolve()
        self.sources = self.state_dir / "sources"
        self.outputs = self.state_dir / "outputs"
        self.requests = self.state_dir / "requests"
        for directory in (self.sources, self.outputs, self.requests):
            directory.mkdir(parents=True, exist_ok=True)
        self.vision_adapter = vision_adapter
        self.settings = settings
        self.sampler = sampler
        self._lock = threading.RLock()
        self._reconcile_sources()

    def _reconcile_sources(self) -> None:
        """Remove incomplete/unregistered files from the dedicated CAS root."""

        for candidate in self.sources.iterdir():
            _require(
                candidate.is_file() and not candidate.is_symlink(),
                "N5 source directory contains a non-regular entry",
            )
            name = candidate.name
            if name.startswith(".n5-upload-") and name.endswith(".tmp"):
                candidate.unlink()
                continue
            match = re.fullmatch(r"([0-9a-f]{64})\.mp4", name)
            _require(match is not None, "N5 source directory has an unknown file")
            if match.group(1) not in self.allowed_sources:
                candidate.unlink()

    def authorized(self, header: str) -> bool:
        return hmac.compare_digest(
            header.encode("utf-8"),
            ("Bearer " + self.settings.bearer_token).encode("utf-8"),
        )

    def health(self) -> dict[str, Any]:
        return {
            "api_version": N5_DIGEST_HTTP_API_VERSION,
            "status": "ok",
            "node_id": "N5",
            "registered_plan_count": len(self.plans),
            "semantic_digest_execution": True,
            "credentials_recorded": False,
        }

    def stage_source(
        self,
        source_handle: str,
        payload: bytes,
        declared_sha256: str,
    ) -> dict[str, Any]:
        return self.stage_source_stream(
            source_handle,
            io.BytesIO(payload),
            source_size_bytes=len(payload),
            declared_sha256=declared_sha256,
        )

    def stage_source_stream(
        self,
        source_handle: str,
        source_stream: BinaryIO,
        *,
        source_size_bytes: int,
        declared_sha256: str,
    ) -> dict[str, Any]:
        """Stream one registered MP4 into durable CAS without buffering it."""

        _require(
            isinstance(source_handle, str)
            and _SHA256.fullmatch(source_handle) is not None,
            "source handle must be a lowercase SHA-256 digest",
        )
        _require(
            declared_sha256 == source_handle,
            "N5 digest source binding failed",
        )
        expected_size = self.allowed_sources.get(source_handle)
        _require(
            expected_size is not None
            and source_size_bytes == expected_size
            and 16 <= source_size_bytes <= self.settings.max_source_bytes,
            "N5 digest source is not registered with this exact size",
        )
        path = self.sources / f"{source_handle}.mp4"
        descriptor, raw_temporary = tempfile.mkstemp(
            prefix=".n5-upload-",
            suffix=".tmp",
            dir=self.sources,
        )
        temporary = Path(raw_temporary)
        with self._lock:
            try:
                digest = hashlib.sha256()
                received = 0
                prefix = bytearray()
                with os.fdopen(descriptor, "wb") as handle:
                    while received < source_size_bytes:
                        block = source_stream.read(
                            min(1024 * 1024, source_size_bytes - received)
                        )
                        _require(bool(block), "N5 digest source was truncated")
                        received += len(block)
                        digest.update(block)
                        if len(prefix) < 32:
                            prefix.extend(block[: 32 - len(prefix)])
                        handle.write(block)
                    handle.flush()
                    os.fsync(handle.fileno())
                _require(
                    received == source_size_bytes
                    and digest.hexdigest() == source_handle,
                    "N5 digest source binding failed",
                )
                _require(
                    b"ftyp" in bytes(prefix)[4:32],
                    "N5 digest source is not an MP4 artifact",
                )
                replay = path.exists()
                if replay:
                    size, stored_digest = _sha256_file(path)
                    _require(
                        size == source_size_bytes
                        and stored_digest == source_handle,
                        "staged N5 digest source changed",
                    )
                else:
                    os.replace(temporary, path)
            finally:
                if temporary.exists():
                    temporary.unlink()
        return {
            "status": "STAGED",
            "source_handle": source_handle,
            "source_size_bytes": source_size_bytes,
            "idempotent_replay": replay,
            "credentials_recorded": False,
        }

    def _request_record(self, request_id: str) -> Path:
        return self.requests / f"{request_id}.json"

    def _output_dir(self, request_id: str) -> Path:
        return self.outputs / request_id

    def _response(
        self,
        *,
        request: Mapping[str, Any],
        binding: _PlanBinding,
        idempotent_replay: bool,
    ) -> dict[str, Any]:
        source = self.sources / f"{request['source_handle']}.mp4"
        output = self._output_dir(request["request_id"])
        verified = verify_n5_multimodal_digest_materialization(
            output,
            binding.plan_dir,
            source,
        )
        return {
            "schema_version": N5_DIGEST_HTTP_RESULT_SCHEMA_VERSION,
            "status": "COMPLETE",
            "request_id": request["request_id"],
            "plan_id": binding.plan_id,
            "object_id": binding.object_id,
            "source_handle": request["source_handle"],
            "result_handle": request["request_id"],
            "artifact_sha256": verified["digest_sha256"],
            "artifact_size_bytes": verified["digest_size_bytes"],
            "model_id": verified["model_id"],
            "llm_called": True,
            "idempotent_replay": idempotent_replay,
            "credentials_recorded": False,
        }

    def execute(self, payload: bytes) -> dict[str, Any]:
        _require(
            0 < len(payload) <= self.settings.max_json_bytes,
            "N5 digest execute request exceeds its byte limit",
        )
        request = _strict_json(payload, "N5 digest execute request")
        _require(
            set(request)
            == {"plan_id", "request_id", "schema_version", "source_handle"},
            "N5 digest execute request fields changed",
        )
        _require(
            request.get("schema_version")
            == N5_DIGEST_HTTP_EXECUTE_SCHEMA_VERSION,
            "N5 digest execute request schema changed",
        )
        request_id = _identifier(request.get("request_id"), "request_id")
        plan_id = _identifier(request.get("plan_id"), "plan_id")
        source_handle = request.get("source_handle")
        _require(
            isinstance(source_handle, str)
            and _SHA256.fullmatch(source_handle) is not None,
            "source_handle is invalid",
        )
        binding = self.plans.get(plan_id)
        _require(binding is not None, "unknown N5 digest plan_id")
        _require(
            source_handle == binding.source_sha256,
            "source handle does not match the frozen N5 digest plan",
        )
        source = self.sources / f"{source_handle}.mp4"
        _require(source.is_file() and not source.is_symlink(), "source is not staged")
        size, digest = _sha256_file(source)
        _require(
            size == binding.source_size_bytes and digest == binding.source_sha256,
            "staged source differs from the frozen N5 digest plan",
        )
        normalized_request = {
            "schema_version": N5_DIGEST_HTTP_EXECUTE_SCHEMA_VERSION,
            "request_id": request_id,
            "plan_id": plan_id,
            "source_handle": source_handle,
        }
        fingerprint = hashlib.sha256(
            _canonical_bytes(normalized_request)
        ).hexdigest()
        record_path = self._request_record(request_id)
        with self._lock:
            if record_path.exists():
                record = _strict_json(
                    record_path.read_bytes(),
                    "N5 digest request record",
                )
                if record.get("request_sha256") != fingerprint:
                    raise N5DigestHTTPConflict(
                        "request_id was already used for another request"
                    )
                return self._response(
                    request=normalized_request,
                    binding=binding,
                    idempotent_replay=True,
                )
            output = self._output_dir(request_id)
            if output.exists():
                verify_n5_multimodal_digest_materialization(
                    output,
                    binding.plan_dir,
                    source,
                )
            else:
                kwargs: dict[str, Any] = {}
                if self.sampler is not None:
                    kwargs["sampler"] = self.sampler
                materialize_n5_multimodal_digest(
                    binding.plan_dir,
                    source,
                    output_dir=output,
                    vision_adapter=self.vision_adapter,
                    **kwargs,
                )
            record = {
                "schema_version": N5_DIGEST_HTTP_EXECUTE_SCHEMA_VERSION,
                "request_id": request_id,
                "plan_id": plan_id,
                "source_handle": source_handle,
                "request_sha256": fingerprint,
                "credentials_recorded": False,
            }
            _atomic_write(record_path, _canonical_bytes(record))
            return self._response(
                request=normalized_request,
                binding=binding,
                idempotent_replay=False,
            )

    def result(self, result_handle: str) -> tuple[bytes, str]:
        request_id = _identifier(result_handle, "result_handle")
        record_path = self._request_record(request_id)
        _require(record_path.is_file(), "N5 digest result handle is unknown")
        record = _strict_json(record_path.read_bytes(), "N5 digest request record")
        binding = self.plans.get(record.get("plan_id"))
        _require(binding is not None, "stored N5 digest plan is no longer registered")
        source = self.sources / f"{record['source_handle']}.mp4"
        output = self._output_dir(request_id)
        verified = verify_n5_multimodal_digest_materialization(
            output,
            binding.plan_dir,
            source,
        )
        payload = (output / DIGEST_NAME).read_bytes()
        _require(
            0 < len(payload) <= self.settings.max_result_bytes
            and len(payload) == verified["digest_size_bytes"],
            "N5 digest result exceeds its byte limit",
        )
        return payload, verified["digest_sha256"]


class _N5RequestError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


class _N5DigestHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "PathfinderN5Digest/1"
    sys_version = ""

    @property
    def _service(self) -> N5DigestHTTPService:
        return self.server.service  # type: ignore[attr-defined]

    def log_message(self, format: str, *args: Any) -> None:
        del format, args

    def _json(self, status: int, value: Mapping[str, Any]) -> None:
        payload = _canonical_bytes(value)
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        if status == 401:
            self.send_header("WWW-Authenticate", "Bearer")
        self.end_headers()
        self.wfile.write(payload)

    def _error(self, status: int, code: str, message: str) -> None:
        self.close_connection = True
        self._json(
            status,
            {
                "api_version": N5_DIGEST_HTTP_API_VERSION,
                "status": "error",
                "error": {"code": code, "message": message},
                "credentials_recorded": False,
            },
        )

    def _authorized(self) -> None:
        values = self.headers.get_all("Authorization") or []
        if len(values) != 1 or not self._service.authorized(values[0]):
            raise _N5RequestError(401, "missing or invalid bearer token")

    def _body(self, maximum: int, media_type: str) -> bytes:
        length = self._body_length(maximum, media_type)
        payload = self.rfile.read(length)
        if len(payload) != length:
            raise _N5RequestError(400, "request body was truncated")
        return payload

    def _body_length(self, maximum: int, media_type: str) -> int:
        if self.headers.get("Transfer-Encoding") is not None:
            raise _N5RequestError(400, "transfer encoding is not supported")
        lengths = self.headers.get_all("Content-Length") or []
        if len(lengths) != 1 or re.fullmatch(r"[0-9]+", lengths[0]) is None:
            raise _N5RequestError(411, "one valid Content-Length is required")
        length = int(lengths[0])
        if not 1 <= length <= maximum:
            raise _N5RequestError(413, "request body size is outside its limit")
        if self.headers.get_content_type() != media_type:
            raise _N5RequestError(415, "request media type is not supported")
        return length

    def _dispatch(self) -> None:
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.scheme or parsed.netloc or parsed.query or parsed.fragment:
            raise _N5RequestError(404, "route not found")
        path = parsed.path
        if self.command == "GET" and path == "/healthz":
            self._json(200, self._service.health())
            return
        prefix = "/v1/digest-inputs/"
        if self.command == "PUT" and path.startswith(prefix):
            self._authorized()
            handle = path.removeprefix(prefix)
            digests = self.headers.get_all("X-Pathfinder-Content-SHA256") or []
            if len(digests) != 1:
                raise _N5RequestError(400, "one content digest is required")
            try:
                source_size = self._body_length(
                    self._service.settings.max_source_bytes,
                    "video/mp4",
                )
                result = self._service.stage_source_stream(
                    handle,
                    self.rfile,
                    source_size_bytes=source_size,
                    declared_sha256=digests[0],
                )
            except N5DigestHTTPError as exc:
                raise _N5RequestError(400, "invalid source upload") from exc
            self._json(200 if result["idempotent_replay"] else 201, result)
            return
        if self.command == "POST" and path == "/v1/digest-materializations/execute":
            self._authorized()
            try:
                result = self._service.execute(
                    self._body(
                        self._service.settings.max_json_bytes,
                        "application/json",
                    )
                )
            except N5DigestHTTPConflict as exc:
                raise _N5RequestError(409, "request conflict") from exc
            except (N5DigestHTTPError, N5DigestMaterializationError) as exc:
                raise _N5RequestError(
                    400,
                    "invalid digest materialization request",
                ) from exc
            self._json(200, result)
            return
        result_prefix = "/v1/digest-results/"
        if self.command == "GET" and path.startswith(result_prefix):
            self._authorized()
            try:
                payload, digest = self._service.result(
                    path.removeprefix(result_prefix)
                )
            except (N5DigestHTTPError, N5DigestMaterializationError) as exc:
                raise _N5RequestError(404, "digest result not found") from exc
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("X-Pathfinder-Content-SHA256", digest)
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(payload)
            return
        raise _N5RequestError(404, "route not found")

    def _handle(self) -> None:
        try:
            self._dispatch()
        except _N5RequestError as exc:
            codes = {
                400: "bad_request",
                401: "unauthorized",
                404: "not_found",
                409: "conflict",
                411: "length_required",
                413: "payload_too_large",
                415: "unsupported_media_type",
            }
            self._error(
                exc.status,
                codes.get(exc.status, "request_failed"),
                exc.message,
            )
        except Exception:
            self._error(500, "internal_error", "N5 digest materialization failed")

    def do_GET(self) -> None:  # noqa: N802
        self._handle()

    def do_POST(self) -> None:  # noqa: N802
        self._handle()

    def do_PUT(self) -> None:  # noqa: N802
        self._handle()

    def do_DELETE(self) -> None:  # noqa: N802
        self._handle()


class N5DigestHTTPServer(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        service: N5DigestHTTPService,
    ) -> None:
        self.service = service
        super().__init__(address, _N5DigestHandler)


def create_n5_digest_http_server(
    state_dir: str | Path,
    plan_directories: Sequence[str | Path],
    *,
    vision_adapter: VisionDigestAdapter,
    settings: N5DigestHTTPSettings,
    sampler: Callable[..., Any] | None = None,
) -> N5DigestHTTPServer:
    """Create but do not start an N5 digest materialization server."""

    service = N5DigestHTTPService(
        state_dir,
        plan_directories,
        vision_adapter=vision_adapter,
        settings=settings,
        sampler=sampler,
    )
    return N5DigestHTTPServer((settings.host, settings.port), service)


def serve_n5_digest(
    state_dir: str | Path,
    plan_directories: Sequence[str | Path],
    *,
    vision_adapter: VisionDigestAdapter,
    settings: N5DigestHTTPSettings,
) -> None:
    """Run N5 digest materialization until interrupted."""

    server = create_n5_digest_http_server(
        state_dir,
        plan_directories,
        vision_adapter=vision_adapter,
        settings=settings,
    )
    try:
        server.serve_forever(poll_interval=0.2)
    finally:
        server.server_close()


def _runtime_environment(name: str, *aliases: str) -> str:
    for candidate in (name, *aliases):
        value = os.environ.get(candidate)
        if value:
            return value
    raise N5DigestHTTPError(f"required runtime environment is missing: {name}")


def _main() -> int:
    parser = argparse.ArgumentParser(
        description="serve authenticated N5 semantic digest materialization",
    )
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--plan-dir", type=Path, action="append", required=True)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9086)
    parser.add_argument(
        "--allowed-http-simulator-host",
        action="append",
        default=[],
    )
    parser.add_argument("--llm-timeout-seconds", type=float, default=180.0)
    parser.add_argument("--llm-max-attempts", type=int, default=3)
    arguments = parser.parse_args()
    token = _runtime_environment("PATHFINDER_N5_DIGEST_TOKEN")
    base_url = _runtime_environment(
        "PATHFINDER_N5_DIGEST_LLM_BASE_URL",
        "UTU_LLM_BASE_URL",
    )
    model_id = _runtime_environment(
        "PATHFINDER_N5_DIGEST_LLM_MODEL",
        "UTU_LLM_MODEL",
    )
    api_key = _runtime_environment(
        "PATHFINDER_N5_DIGEST_LLM_API_KEY",
        "UTU_LLM_API_KEY",
    )
    adapter = OpenAICompatibleVisionDigestAdapter(
        base_url=base_url,
        api_key=api_key,
        model_id=model_id,
        allowed_http_simulator_hosts=(
            arguments.allowed_http_simulator_host
        ),
        timeout_seconds=arguments.llm_timeout_seconds,
        max_attempts=arguments.llm_max_attempts,
    )
    serve_n5_digest(
        arguments.state_dir,
        arguments.plan_dir,
        vision_adapter=adapter,
        settings=N5DigestHTTPSettings(
            bearer_token=token,
            host=arguments.host,
            port=arguments.port,
        ),
    )
    return 0


__all__ = [
    "N5_DIGEST_HTTP_API_VERSION",
    "N5_DIGEST_HTTP_EXECUTE_SCHEMA_VERSION",
    "N5_DIGEST_HTTP_RESULT_SCHEMA_VERSION",
    "N5DigestHTTPConflict",
    "N5DigestHTTPError",
    "N5DigestHTTPServer",
    "N5DigestHTTPService",
    "N5DigestHTTPSettings",
    "create_n5_digest_http_server",
    "serve_n5_digest",
]


if __name__ == "__main__":
    raise SystemExit(_main())
