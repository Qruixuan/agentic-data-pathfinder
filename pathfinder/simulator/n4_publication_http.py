"""Authenticated HTTP adapter for atomic N4 derived-artifact publication.

This is intentionally separate from the read-only standard Data Agent.  N5
publishes a complete, content-bound request here; the adapter delegates all
validation, compare-and-swap, and durable idempotency to
``N4DerivedRepresentationStore``.  A supervisor may then bind/reload the
standard Data Agent from the returned immutable generation between the
provisioning and evaluation phases.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import hmac
import http.server
import json
import os
import re
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .n4_derived_data_plane import (
    N4ArtifactProvenance,
    N4DerivedArtifactInput,
    N4DerivedDataPlaneError,
    N4DerivedRepresentationStore,
    N4PublicationConflict,
)


N4_PUBLICATION_HTTP_API_VERSION = (
    "pathfinder.simulator-n4-publication-http/v1alpha1"
)
N4_PUBLICATION_HTTP_REQUEST_SCHEMA_VERSION = (
    "pathfinder.simulator-n4-publication-http-request/v1alpha1"
)
N4_PUBLICATION_HTTP_RESULT_SCHEMA_VERSION = (
    "pathfinder.simulator-n4-publication-http-result/v1alpha1"
)

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_DEFAULT_MAX_REQUEST_BYTES = 96 * 1024 * 1024
_DEFAULT_MAX_ARTIFACT_BYTES = 64 * 1024 * 1024
_MAX_RESPONSE_BYTES = 1024 * 1024


class N4PublicationHTTPError(RuntimeError):
    """Raised when the N4 publication HTTP contract is invalid."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise N4PublicationHTTPError(message)


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
        raise N4PublicationHTTPError(
            "N4 publication value is not canonical JSON"
        ) from exc


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
                N4PublicationHTTPError(
                    f"{label} contains non-finite number {item}"
                )
            ),
        )
    except N4PublicationHTTPError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise N4PublicationHTTPError(f"cannot parse {label}") from exc
    _require(isinstance(value, dict), f"{label} must be an object")
    _require(
        payload == _canonical_bytes(value),
        f"{label} is not canonical JSON",
    )
    return value


def _positive_limit(value: Any, label: str) -> int:
    _require(
        type(value) is int and value > 0,
        f"{label} must be a positive integer",
    )
    return value


def _decode_artifact(
    value: Mapping[str, Any],
    *,
    max_artifact_bytes: int,
) -> N4DerivedArtifactInput:
    expected_fields = {
        "artifact_base64",
        "artifact_sha256",
        "artifact_size_bytes",
        "object_id",
        "plan_ids",
        "provenance",
        "representation_id",
    }
    _require(
        set(value) == expected_fields,
        "N4 publication artifact fields changed",
    )
    encoded = value.get("artifact_base64")
    _require(
        isinstance(encoded, str) and encoded.isascii(),
        "artifact_base64 must be ASCII text",
    )
    _require(
        len(encoded) <= 4 * ((max_artifact_bytes + 2) // 3),
        "encoded N4 artifact exceeds its byte limit",
    )
    try:
        raw = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise N4PublicationHTTPError(
            "artifact_base64 is invalid"
        ) from exc
    expected_size = value.get("artifact_size_bytes")
    expected_digest = value.get("artifact_sha256")
    _require(
        type(expected_size) is int
        and 0 < expected_size <= max_artifact_bytes
        and len(raw) == expected_size,
        "N4 artifact size binding failed",
    )
    _require(
        isinstance(expected_digest, str)
        and _SHA256.fullmatch(expected_digest) is not None
        and hashlib.sha256(raw).hexdigest() == expected_digest,
        "N4 artifact digest binding failed",
    )
    plan_ids = value.get("plan_ids")
    _require(
        isinstance(plan_ids, list)
        and bool(plan_ids)
        and all(isinstance(item, str) for item in plan_ids),
        "N4 artifact plan_ids are invalid",
    )
    provenance = value.get("provenance")
    _require(
        isinstance(provenance, Mapping),
        "N4 artifact provenance is invalid",
    )
    return N4DerivedArtifactInput(
        object_id=value.get("object_id"),
        representation_id=value.get("representation_id"),
        artifact_bytes=raw,
        plan_ids=tuple(plan_ids),
        provenance=N4ArtifactProvenance.from_dict(provenance),
        expected_sha256=expected_digest,
        expected_size_bytes=expected_size,
    )


@dataclass(frozen=True)
class N4PublicationHTTPSettings:
    bearer_token: str
    host: str = "127.0.0.1"
    port: int = 0
    max_request_bytes: int = _DEFAULT_MAX_REQUEST_BYTES
    max_artifact_bytes: int = _DEFAULT_MAX_ARTIFACT_BYTES

    def __post_init__(self) -> None:
        _require(
            isinstance(self.bearer_token, str) and bool(self.bearer_token),
            "N4 publication bearer token is required",
        )
        _require(
            isinstance(self.host, str) and bool(self.host),
            "N4 publication listen host is invalid",
        )
        _require(
            type(self.port) is int and 0 <= self.port <= 65535,
            "N4 publication listen port is invalid",
        )
        _positive_limit(self.max_request_bytes, "max_request_bytes")
        _positive_limit(self.max_artifact_bytes, "max_artifact_bytes")


class N4PublicationHTTPService:
    """Bounded authenticated adapter around the durable N4 store."""

    def __init__(
        self,
        store: N4DerivedRepresentationStore,
        settings: N4PublicationHTTPSettings,
    ) -> None:
        _require(
            isinstance(store, N4DerivedRepresentationStore),
            "N4 publication store is invalid",
        )
        _require(
            isinstance(settings, N4PublicationHTTPSettings),
            "N4 publication HTTP settings are invalid",
        )
        self.store = store
        self.settings = settings

    def authorized(self, header: str) -> bool:
        return hmac.compare_digest(
            header.encode("utf-8"),
            ("Bearer " + self.settings.bearer_token).encode("utf-8"),
        )

    def health(self) -> dict[str, Any]:
        current = self.store.current_snapshot()
        return {
            "api_version": N4_PUBLICATION_HTTP_API_VERSION,
            "status": "ok",
            "node_id": "N4",
            "atomic_publication": True,
            "current_generation_present": current is not None,
            "credentials_recorded": False,
        }

    def publish(self, payload: bytes) -> dict[str, Any]:
        _require(
            0 < len(payload) <= self.settings.max_request_bytes,
            "N4 publication request exceeds its byte limit",
        )
        request = _strict_json(payload, "N4 publication request")
        _require(
            set(request)
            == {
                "artifacts",
                "catalog_version",
                "expected_current_catalog_version",
                "package_id",
                "publication_id",
                "schema_version",
            },
            "N4 publication request fields changed",
        )
        _require(
            request.get("schema_version")
            == N4_PUBLICATION_HTTP_REQUEST_SCHEMA_VERSION,
            "N4 publication request schema changed",
        )
        artifact_rows = request.get("artifacts")
        _require(
            isinstance(artifact_rows, list) and bool(artifact_rows),
            "N4 publication request has no artifacts",
        )
        artifacts: list[N4DerivedArtifactInput] = []
        for row in artifact_rows:
            _require(
                isinstance(row, Mapping),
                "N4 publication artifact must be an object",
            )
            artifacts.append(
                _decode_artifact(
                    row,
                    max_artifact_bytes=self.settings.max_artifact_bytes,
                )
            )
        result = self.store.publish(
            publication_id=request.get("publication_id"),
            package_id=request.get("package_id"),
            catalog_version=request.get("catalog_version"),
            expected_current_catalog_version=request.get(
                "expected_current_catalog_version"
            ),
            artifacts=artifacts,
        )
        response = {
            "schema_version": N4_PUBLICATION_HTTP_RESULT_SCHEMA_VERSION,
            "status": "COMMITTED",
            "publication_id": result.receipt["publication_id"],
            "generation_id": result.snapshot.generation_id,
            "package_sha256": result.snapshot.package_sha256,
            "catalog_version": result.snapshot.catalog_version,
            "receipt": result.receipt,
            "idempotent_replay": result.idempotent_replay,
            "data_agent_reload_required": True,
            "credentials_recorded": False,
        }
        _require(
            len(_canonical_bytes(response)) <= _MAX_RESPONSE_BYTES,
            "N4 publication response exceeds its byte limit",
        )
        return response


class _N4RequestError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


class _N4PublicationHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "PathfinderN4Publication/1"
    sys_version = ""

    @property
    def _service(self) -> N4PublicationHTTPService:
        return self.server.service  # type: ignore[attr-defined]

    def log_message(self, format: str, *args: Any) -> None:
        del format, args

    def _send(self, status: int, value: Mapping[str, Any]) -> None:
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
        self._send(
            status,
            {
                "api_version": N4_PUBLICATION_HTTP_API_VERSION,
                "status": "error",
                "error": {"code": code, "message": message},
                "credentials_recorded": False,
            },
        )

    def _authorized(self) -> None:
        values = self.headers.get_all("Authorization") or []
        if len(values) != 1 or not self._service.authorized(values[0]):
            raise _N4RequestError(401, "missing or invalid bearer token")

    def _body(self) -> bytes:
        if self.headers.get("Transfer-Encoding") is not None:
            raise _N4RequestError(400, "transfer encoding is not supported")
        lengths = self.headers.get_all("Content-Length") or []
        if len(lengths) != 1 or re.fullmatch(r"[0-9]+", lengths[0]) is None:
            raise _N4RequestError(411, "one valid Content-Length is required")
        length = int(lengths[0])
        if not 1 <= length <= self._service.settings.max_request_bytes:
            raise _N4RequestError(413, "request body size is outside its limit")
        if self.headers.get_content_type() != "application/json":
            raise _N4RequestError(415, "request media type is not supported")
        payload = self.rfile.read(length)
        if len(payload) != length:
            raise _N4RequestError(400, "request body was truncated")
        return payload

    def _dispatch(self) -> None:
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.scheme or parsed.netloc or parsed.query or parsed.fragment:
            raise _N4RequestError(404, "route not found")
        if self.command == "GET" and parsed.path == "/healthz":
            self._send(200, self._service.health())
            return
        if self.command == "POST" and parsed.path == "/v1/publications":
            self._authorized()
            try:
                result = self._service.publish(self._body())
            except N4PublicationConflict as exc:
                raise _N4RequestError(409, "publication conflict") from exc
            except (N4PublicationHTTPError, N4DerivedDataPlaneError) as exc:
                raise _N4RequestError(
                    400,
                    "invalid publication request",
                ) from exc
            self._send(200 if result["idempotent_replay"] else 201, result)
            return
        raise _N4RequestError(404, "route not found")

    def _handle(self) -> None:
        try:
            self._dispatch()
        except _N4RequestError as exc:
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
            self._error(500, "internal_error", "N4 publication failed")

    def do_GET(self) -> None:  # noqa: N802
        self._handle()

    def do_POST(self) -> None:  # noqa: N802
        self._handle()

    def do_PUT(self) -> None:  # noqa: N802
        self._handle()

    def do_DELETE(self) -> None:  # noqa: N802
        self._handle()


class N4PublicationHTTPServer(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        service: N4PublicationHTTPService,
    ) -> None:
        self.service = service
        super().__init__(address, _N4PublicationHandler)


def create_n4_publication_http_server(
    store_root: str | Path,
    *,
    settings: N4PublicationHTTPSettings,
) -> N4PublicationHTTPServer:
    """Create but do not start an authenticated N4 publication server."""

    service = N4PublicationHTTPService(
        N4DerivedRepresentationStore(store_root),
        settings,
    )
    return N4PublicationHTTPServer((settings.host, settings.port), service)


def serve_n4_publication(
    store_root: str | Path,
    *,
    settings: N4PublicationHTTPSettings,
) -> None:
    """Run N4 publication until interrupted."""

    server = create_n4_publication_http_server(store_root, settings=settings)
    try:
        server.serve_forever(poll_interval=0.2)
    finally:
        server.server_close()


def _main() -> int:
    parser = argparse.ArgumentParser(
        description="serve authenticated atomic N4 publication",
    )
    parser.add_argument("--store-root", type=Path, required=True)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9084)
    parser.add_argument(
        "--max-request-bytes",
        type=int,
        default=_DEFAULT_MAX_REQUEST_BYTES,
    )
    parser.add_argument(
        "--max-artifact-bytes",
        type=int,
        default=_DEFAULT_MAX_ARTIFACT_BYTES,
    )
    arguments = parser.parse_args()
    token = os.environ.get("PATHFINDER_N4_PUBLICATION_TOKEN")
    if not token:
        parser.error("PATHFINDER_N4_PUBLICATION_TOKEN is required")
    serve_n4_publication(
        arguments.store_root,
        settings=N4PublicationHTTPSettings(
            bearer_token=token,
            host=arguments.host,
            port=arguments.port,
            max_request_bytes=arguments.max_request_bytes,
            max_artifact_bytes=arguments.max_artifact_bytes,
        ),
    )
    return 0


__all__ = [
    "N4_PUBLICATION_HTTP_API_VERSION",
    "N4_PUBLICATION_HTTP_REQUEST_SCHEMA_VERSION",
    "N4_PUBLICATION_HTTP_RESULT_SCHEMA_VERSION",
    "N4PublicationHTTPError",
    "N4PublicationHTTPServer",
    "N4PublicationHTTPService",
    "N4PublicationHTTPSettings",
    "create_n4_publication_http_server",
    "serve_n4_publication",
]


if __name__ == "__main__":
    raise SystemExit(_main())
