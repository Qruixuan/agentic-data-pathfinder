from __future__ import annotations

import json
import math
import os
import socket
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from hashlib import sha256
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urljoin, urlparse
from urllib.request import Request, urlopen


DATA_AGENT_API_VERSION = "pathfinder.data-agent/v1alpha1"


class DataAgentClientError(RuntimeError):
    """Base error raised by the Pathfinder Data Agent client."""


class DataAgentUnavailableError(DataAgentClientError):
    """Raised when the remote Data Agent cannot be reached."""


class DataAgentHTTPError(DataAgentClientError):
    """Raised when the remote Data Agent rejects an access request."""

    def __init__(self, status_code: int, message: str):
        super().__init__(
            f"Data Agent returned HTTP {status_code}: {message}"
        )
        self.status_code = status_code


class DataAgentProtocolError(DataAgentClientError):
    """Raised when the Data Agent returns an invalid protocol response."""


@dataclass(frozen=True)
class DataAgentClientSettings:
    base_url: str
    token: str | None = None
    timeout_seconds: float = 30.0
    max_retries: int = 1
    max_response_bytes: int = 4 * 1024 * 1024

    def __post_init__(self) -> None:
        if not isinstance(self.base_url, str) or not self.base_url.strip():
            raise ValueError("Data Agent base_url cannot be empty")
        parsed_url = urlparse(self.base_url)
        if (
            parsed_url.scheme not in {"http", "https"}
            or not parsed_url.netloc
        ):
            raise ValueError(
                "Data Agent base_url must be an absolute HTTP(S) URL"
            )
        if parsed_url.username is not None or parsed_url.password is not None:
            raise ValueError(
                "Data Agent base_url cannot contain credentials; use "
                "PATHFINDER_DATA_AGENT_TOKEN"
            )
        if (
            not isinstance(self.timeout_seconds, (int, float))
            or isinstance(self.timeout_seconds, bool)
            or not math.isfinite(float(self.timeout_seconds))
            or self.timeout_seconds <= 0
        ):
            raise ValueError(
                "Data Agent timeout_seconds must be positive and finite"
            )
        if (
            not isinstance(self.max_retries, int)
            or isinstance(self.max_retries, bool)
            or self.max_retries < 0
        ):
            raise ValueError(
                "Data Agent max_retries must be a non-negative integer"
            )
        if (
            not isinstance(self.max_response_bytes, int)
            or isinstance(self.max_response_bytes, bool)
            or self.max_response_bytes <= 0
        ):
            raise ValueError(
                "Data Agent max_response_bytes must be a positive integer"
            )

    @classmethod
    def from_environment(
        cls,
        *,
        base_url: str | None = None,
        timeout_seconds: float | None = None,
        max_retries: int | None = None,
    ) -> "DataAgentClientSettings":
        resolved_url = base_url or os.getenv("PATHFINDER_DATA_AGENT_URL")
        if not resolved_url:
            raise ValueError(
                "Data Agent URL is required; pass --data-agent-url or set "
                "PATHFINDER_DATA_AGENT_URL"
            )
        return cls(
            base_url=resolved_url,
            token=os.getenv("PATHFINDER_DATA_AGENT_TOKEN") or None,
            timeout_seconds=(
                timeout_seconds
                if timeout_seconds is not None
                else 30.0
            ),
            max_retries=(
                max_retries if max_retries is not None else 1
            ),
        )


@dataclass(frozen=True)
class DataAgentAccessRequest:
    access_id: str
    session_id: str
    trial_id: str
    plan_id: str
    plan_epoch: int
    task_class_id: str
    representation_id: str
    event_index: int
    latency_multiplier: float
    binding: Mapping[str, Any]
    plan_capability: Mapping[str, Any] | None = None
    object_id: str | None = None

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any],
    ) -> "DataAgentAccessRequest":
        if payload.get("api_version") != DATA_AGENT_API_VERSION:
            raise DataAgentProtocolError(
                "Data Agent request has an unsupported api_version"
            )
        binding = payload.get("binding")
        if not isinstance(binding, Mapping):
            raise DataAgentProtocolError(
                "Data Agent request binding must be an object"
            )
        plan_capability = payload.get("plan_capability")
        if (
            plan_capability is not None
            and not isinstance(plan_capability, Mapping)
        ):
            raise DataAgentProtocolError(
                "Data Agent request plan_capability must be an object or null"
            )
        try:
            return cls(
                access_id=payload["access_id"],
                session_id=payload["session_id"],
                trial_id=payload["trial_id"],
                plan_id=payload["plan_id"],
                plan_epoch=payload["plan_epoch"],
                task_class_id=payload["task_class_id"],
                representation_id=payload["representation_id"],
                event_index=payload["event_index"],
                latency_multiplier=payload["latency_multiplier"],
                binding=dict(binding),
                plan_capability=(
                    dict(plan_capability)
                    if plan_capability is not None
                    else None
                ),
                object_id=payload.get("object_id"),
            )
        except KeyError as exc:
            raise DataAgentProtocolError(
                f"Data Agent request is missing field: {exc.args[0]}"
            ) from exc
        except ValueError as exc:
            raise DataAgentProtocolError(str(exc)) from exc

    def __post_init__(self) -> None:
        for name in (
            "access_id",
            "session_id",
            "trial_id",
            "plan_id",
            "task_class_id",
            "representation_id",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"Data Agent {name} cannot be empty")
        if (
            not isinstance(self.plan_epoch, int)
            or isinstance(self.plan_epoch, bool)
            or self.plan_epoch < 0
        ):
            raise ValueError(
                "Data Agent plan_epoch must be a non-negative integer"
            )
        if (
            not isinstance(self.event_index, int)
            or isinstance(self.event_index, bool)
            or self.event_index < 0
        ):
            raise ValueError(
                "Data Agent event_index must be a non-negative integer"
            )
        if (
            not isinstance(self.latency_multiplier, (int, float))
            or isinstance(self.latency_multiplier, bool)
            or not math.isfinite(float(self.latency_multiplier))
            or self.latency_multiplier <= 0
        ):
            raise ValueError(
                "Data Agent latency_multiplier must be positive and finite"
            )
        if not isinstance(self.binding, Mapping):
            raise ValueError("Data Agent binding must be an object")
        if (
            self.plan_capability is not None
            and not isinstance(self.plan_capability, Mapping)
        ):
            raise ValueError(
                "Data Agent plan_capability must be an object or null"
            )
        if self.object_id is not None and (
            not isinstance(self.object_id, str) or not self.object_id.strip()
        ):
            raise ValueError(
                "Data Agent object_id must be a non-empty string or null"
            )

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "api_version": DATA_AGENT_API_VERSION,
            "access_id": self.access_id,
            "session_id": self.session_id,
            "trial_id": self.trial_id,
            "plan_id": self.plan_id,
            "plan_epoch": self.plan_epoch,
            "task_class_id": self.task_class_id,
            "representation_id": self.representation_id,
            "event_index": self.event_index,
            "latency_multiplier": self.latency_multiplier,
            "binding": dict(self.binding),
        }
        if self.plan_capability is not None:
            payload["plan_capability"] = dict(self.plan_capability)
        if self.object_id is not None:
            payload["object_id"] = self.object_id
        return payload


@dataclass(frozen=True)
class DataAgentPayload:
    kind: str
    media_type: str
    value: Any
    sha256: str | None = None

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "DataAgentPayload":
        kind = payload.get("kind")
        media_type = payload.get("media_type")
        if not isinstance(kind, str) or not kind.strip():
            raise DataAgentProtocolError(
                "Data Agent payload.kind must be a non-empty string"
            )
        if not isinstance(media_type, str) or not media_type.strip():
            raise DataAgentProtocolError(
                "Data Agent payload.media_type must be a non-empty string"
            )
        if "value" not in payload:
            raise DataAgentProtocolError(
                "Data Agent payload.value is required"
            )
        value = payload["value"]
        if kind == "inline_text" and not isinstance(value, str):
            raise DataAgentProtocolError(
                "Data Agent inline_text payload.value must be a string"
            )
        digest = payload.get("sha256")
        if digest is not None and (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(
                character not in "0123456789abcdefABCDEF"
                for character in digest
            )
        ):
            raise DataAgentProtocolError(
                "Data Agent payload.sha256 must be a 64-character hex digest"
            )
        normalized_digest = digest.lower() if digest is not None else None
        if (
            kind == "inline_text"
            and normalized_digest is not None
            and sha256(value.encode("utf-8")).hexdigest()
            != normalized_digest
        ):
            raise DataAgentProtocolError(
                "Data Agent inline_text payload.sha256 does not match value"
            )
        return cls(
            kind=kind,
            media_type=media_type,
            value=value,
            sha256=normalized_digest,
        )

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "kind": self.kind,
            "media_type": self.media_type,
            "value": self.value,
        }
        if self.sha256 is not None:
            payload["sha256"] = self.sha256
        return payload

    def to_agent_content(self) -> str:
        if self.kind == "inline_text":
            return str(self.value)
        return json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            sort_keys=True,
        )


@dataclass(frozen=True)
class DataAgentAccessResult:
    access_id: str
    payload: DataAgentPayload
    service_latency_ms: float
    realized_cost: float
    bytes_read: int
    location: str
    cache_hit: bool | None = None
    timings_ms: dict[str, float] = field(default_factory=dict)
    client_round_trip_ms: float | None = None
    object_id: str | None = None
    object_catalog_version: str | None = None

    @property
    def observed_latency_ms(self) -> float:
        if self.client_round_trip_ms is not None:
            return self.client_round_trip_ms
        return self.service_latency_ms

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any],
    ) -> "DataAgentAccessResult":
        if payload.get("api_version") != DATA_AGENT_API_VERSION:
            raise DataAgentProtocolError(
                "Data Agent response has an unsupported api_version"
            )
        if payload.get("status") != "succeeded":
            raise DataAgentProtocolError(
                "Data Agent successful HTTP response must have "
                "status='succeeded'"
            )
        access_id = payload.get("access_id")
        if not isinstance(access_id, str) or not access_id.strip():
            raise DataAgentProtocolError(
                "Data Agent response access_id is missing"
            )
        object_id = payload.get("object_id")
        if object_id is not None and (
            not isinstance(object_id, str) or not object_id.strip()
        ):
            raise DataAgentProtocolError(
                "Data Agent response object_id must be a non-empty string or null"
            )
        object_catalog_version = payload.get("object_catalog_version")
        if object_catalog_version is not None and (
            not isinstance(object_catalog_version, str)
            or not object_catalog_version.strip()
        ):
            raise DataAgentProtocolError(
                "Data Agent response object_catalog_version is invalid"
            )
        raw_payload = payload.get("payload")
        if not isinstance(raw_payload, Mapping):
            raise DataAgentProtocolError(
                "Data Agent response payload must be an object"
            )
        telemetry = payload.get("telemetry")
        if not isinstance(telemetry, Mapping):
            raise DataAgentProtocolError(
                "Data Agent response telemetry must be an object"
            )

        service_latency_ms = _nonnegative_number(
            telemetry,
            "service_latency_ms",
        )
        realized_cost = _nonnegative_number(
            telemetry,
            "realized_cost",
        )
        bytes_read = telemetry.get("bytes_read")
        if (
            not isinstance(bytes_read, int)
            or isinstance(bytes_read, bool)
            or bytes_read < 0
        ):
            raise DataAgentProtocolError(
                "Data Agent telemetry.bytes_read must be a "
                "non-negative integer"
            )
        location = telemetry.get("location")
        if not isinstance(location, str) or not location.strip():
            raise DataAgentProtocolError(
                "Data Agent telemetry.location must be a non-empty string"
            )
        cache_hit = telemetry.get("cache_hit")
        if cache_hit is not None and not isinstance(cache_hit, bool):
            raise DataAgentProtocolError(
                "Data Agent telemetry.cache_hit must be a boolean or null"
            )

        raw_timings = telemetry.get("timings_ms", {})
        if not isinstance(raw_timings, Mapping):
            raise DataAgentProtocolError(
                "Data Agent telemetry.timings_ms must be an object"
            )
        timings = {
            str(name): _validated_nonnegative_number(value, str(name))
            for name, value in raw_timings.items()
        }
        return cls(
            access_id=access_id,
            payload=DataAgentPayload.from_dict(raw_payload),
            service_latency_ms=service_latency_ms,
            realized_cost=realized_cost,
            bytes_read=bytes_read,
            location=location,
            cache_hit=cache_hit,
            timings_ms=timings,
            object_id=object_id,
            object_catalog_version=object_catalog_version,
        )


@dataclass(frozen=True)
class DataAgentAccessTelemetry:
    access_id: str
    object_id: str | None
    representation_id: str
    object_catalog_version: str | None
    download_request_count: int
    completed_request_count: int
    full_download_count: int
    bytes_sent: int
    transfer_latency_ms: float
    latest_completed_at: float | None = None

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any],
    ) -> "DataAgentAccessTelemetry":
        if payload.get("api_version") != DATA_AGENT_API_VERSION:
            raise DataAgentProtocolError(
                "Data Agent telemetry response has an unsupported api_version"
            )
        if payload.get("status") != "succeeded":
            raise DataAgentProtocolError(
                "Data Agent telemetry response must have status='succeeded'"
            )
        access_id = payload.get("access_id")
        representation_id = payload.get("representation_id")
        for name, value in (
            ("access_id", access_id),
            ("representation_id", representation_id),
        ):
            if not isinstance(value, str) or not value.strip():
                raise DataAgentProtocolError(
                    f"Data Agent telemetry response {name} is invalid"
                )
        object_id = payload.get("object_id")
        if object_id is not None and (
            not isinstance(object_id, str) or not object_id.strip()
        ):
            raise DataAgentProtocolError(
                "Data Agent telemetry response object_id is invalid"
            )
        object_catalog_version = payload.get("object_catalog_version")
        if object_catalog_version is not None and (
            not isinstance(object_catalog_version, str)
            or not object_catalog_version.strip()
        ):
            raise DataAgentProtocolError(
                "Data Agent telemetry response object_catalog_version is invalid"
            )
        downloads = payload.get("artifact_download")
        if not isinstance(downloads, Mapping):
            raise DataAgentProtocolError(
                "Data Agent telemetry response artifact_download must be an object"
            )
        integer_fields: dict[str, int] = {}
        for name in (
            "download_request_count",
            "completed_request_count",
            "full_download_count",
            "bytes_sent",
        ):
            value = downloads.get(name)
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
            ):
                raise DataAgentProtocolError(
                    f"Data Agent artifact_download.{name} must be a "
                    "non-negative integer"
                )
            integer_fields[name] = value
        latest_completed_at = downloads.get("latest_completed_at")
        if latest_completed_at is not None:
            latest_completed_at = _validated_nonnegative_number(
                latest_completed_at,
                "latest_completed_at",
            )
        return cls(
            access_id=access_id,
            object_id=object_id,
            representation_id=representation_id,
            object_catalog_version=object_catalog_version,
            download_request_count=integer_fields["download_request_count"],
            completed_request_count=integer_fields["completed_request_count"],
            full_download_count=integer_fields["full_download_count"],
            bytes_sent=integer_fields["bytes_sent"],
            transfer_latency_ms=_validated_nonnegative_number(
                downloads.get("transfer_latency_ms"),
                "transfer_latency_ms",
            ),
            latest_completed_at=latest_completed_at,
        )


class DataAgentClientProtocol(Protocol):
    def access(
        self,
        request: DataAgentAccessRequest,
    ) -> DataAgentAccessResult:
        """Execute one idempotent physical representation access."""

    def get_access_telemetry(
        self,
        access_id: str,
    ) -> DataAgentAccessTelemetry:
        """Return transfer telemetry attributed to one access."""


class HttpDataAgentClient:
    """Synchronous standard-library client for one Data Agent endpoint."""

    _RETRYABLE_HTTP_STATUSES = frozenset({502, 503, 504})

    def __init__(
        self,
        settings: DataAgentClientSettings,
        *,
        opener: Callable[..., Any] = urlopen,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.settings = settings
        self._opener = opener
        self._sleep = sleep
        self._access_url = urljoin(
            settings.base_url.rstrip("/") + "/",
            "v1/access",
        )

    def access(
        self,
        request: DataAgentAccessRequest,
    ) -> DataAgentAccessResult:
        body = json.dumps(
            request.to_dict(),
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Idempotency-Key": request.access_id,
            "User-Agent": "pathfinder-data-agent-client/0.1",
            "X-Pathfinder-Protocol-Version": DATA_AGENT_API_VERSION,
        }
        if self.settings.token:
            headers["Authorization"] = f"Bearer {self.settings.token}"

        started = time.perf_counter()
        raw_response = self._send_with_retries(
            Request(
                self._access_url,
                data=body,
                headers=headers,
                method="POST",
            )
        )
        elapsed_ms = (time.perf_counter() - started) * 1_000.0
        result = DataAgentAccessResult.from_dict(raw_response)
        if result.access_id != request.access_id:
            raise DataAgentProtocolError(
                "Data Agent response access_id does not match the request"
            )
        return replace(result, client_round_trip_ms=elapsed_ms)

    def get_access_telemetry(
        self,
        access_id: str,
    ) -> DataAgentAccessTelemetry:
        if not isinstance(access_id, str) or not access_id.strip():
            raise ValueError("Data Agent access_id cannot be empty")
        headers = {
            "Accept": "application/json",
            "User-Agent": "pathfinder-data-agent-client/0.1",
            "X-Pathfinder-Protocol-Version": DATA_AGENT_API_VERSION,
        }
        if self.settings.token:
            headers["Authorization"] = f"Bearer {self.settings.token}"
        telemetry_url = urljoin(
            self.settings.base_url.rstrip("/") + "/",
            f"v1/accesses/{quote(access_id, safe='')}/telemetry",
        )
        payload = self._send_with_retries(
            Request(telemetry_url, headers=headers, method="GET")
        )
        result = DataAgentAccessTelemetry.from_dict(payload)
        if result.access_id != access_id:
            raise DataAgentProtocolError(
                "Data Agent telemetry access_id does not match the request"
            )
        return result

    def _send_with_retries(self, request: Request) -> Mapping[str, Any]:
        for attempt in range(self.settings.max_retries + 1):
            try:
                return self._send_once(request)
            except DataAgentHTTPError as exc:
                if (
                    exc.status_code not in self._RETRYABLE_HTTP_STATUSES
                    or attempt >= self.settings.max_retries
                ):
                    raise
            except DataAgentUnavailableError:
                if attempt >= self.settings.max_retries:
                    raise
            self._sleep(0.1 * (2**attempt))
        raise AssertionError("unreachable Data Agent retry state")

    def _send_once(self, request: Request) -> Mapping[str, Any]:
        try:
            with self._opener(
                request,
                timeout=self.settings.timeout_seconds,
            ) as response:
                raw = response.read(self.settings.max_response_bytes + 1)
        except HTTPError as exc:
            try:
                message = exc.read(4096).decode("utf-8", errors="replace")
            finally:
                exc.close()
            raise DataAgentHTTPError(
                exc.code,
                message.strip() or exc.reason,
            ) from exc
        except (URLError, TimeoutError, socket.timeout) as exc:
            raise DataAgentUnavailableError(
                f"cannot reach Data Agent at {self._access_url}: {exc}"
            ) from exc

        if len(raw) > self.settings.max_response_bytes:
            raise DataAgentProtocolError(
                "Data Agent response exceeds max_response_bytes"
            )
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DataAgentProtocolError(
                "Data Agent response is not valid UTF-8 JSON"
            ) from exc
        if not isinstance(payload, Mapping):
            raise DataAgentProtocolError(
                "Data Agent response must be a JSON object"
            )
        return payload


def _nonnegative_number(
    mapping: Mapping[str, Any],
    name: str,
) -> float:
    if name not in mapping:
        raise DataAgentProtocolError(
            f"Data Agent telemetry.{name} is required"
        )
    return _validated_nonnegative_number(mapping[name], name)


def _validated_nonnegative_number(value: Any, name: str) -> float:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(float(value))
        or float(value) < 0
    ):
        raise DataAgentProtocolError(
            f"Data Agent telemetry.{name} must be a finite "
            "non-negative number"
        )
    return float(value)
