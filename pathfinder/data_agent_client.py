from __future__ import annotations

import json
import logging
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
from urllib.request import HTTPRedirectHandler, Request, build_opener, urlopen


DATA_AGENT_API_VERSION = "pathfinder.data-agent/v1alpha1"

logger = logging.getLogger("pathfinder.data_agent_client")


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


class DataAgentArtifactError(DataAgentClientError):
    """Base error for a rejected or failed artifact fetch."""


class DataAgentArtifactSecurityError(DataAgentArtifactError):
    """Raised when an artifact URL violates the configured trust boundary."""


class DataAgentArtifactRedirectError(DataAgentArtifactSecurityError):
    """Raised when the Data Agent attempts to redirect an artifact request."""


class DataAgentArtifactTooLargeError(DataAgentArtifactError):
    """Raised when an artifact exceeds the configured response bound."""


class DataAgentArtifactUnsupportedError(DataAgentArtifactError):
    """Raised when an artifact cannot be represented safely to the Agent."""


class DataAgentArtifactIntegrityError(DataAgentArtifactError):
    """Raised when downloaded bytes do not match the access response."""


def _incompleteness_detail(in_flight_request_count: int | None) -> str:
    """Explain *why* a summary is not final, for humans reading a failure.

    A zero count is the confusing case: it does not mean nothing happened, it
    means the Data Agent saw the transfer generation move while it was reading
    the summary, so the numbers do not describe any single instant.
    """
    if in_flight_request_count is None:
        return (
            "the Data Agent did not report the fields required to prove "
            "completeness"
        )
    if in_flight_request_count == 0:
        return (
            "no transfer is in flight now, but transfer activity changed "
            "while the summary was being read, so the snapshot is not a "
            "stable point in time"
        )
    return f"{in_flight_request_count} transfer(s) are still in flight"


class DataAgentTelemetryQuiescenceError(DataAgentClientError):
    """Raised when transfer telemetry never reaches a final state.

    Research observations are only meaningful when every transfer attributed
    to an access has been durably recorded. Returning the provisional summary
    instead would silently under-report bytes and latency, so the wait fails
    closed and the caller is expected to discard the session.
    """

    def __init__(
        self,
        access_id: str,
        in_flight_request_count: int | None,
        timeout_seconds: float,
        telemetry: "DataAgentAccessTelemetry",
    ):
        super().__init__(
            f"Data Agent telemetry for access {access_id} did not become "
            f"final within {timeout_seconds:.3f}s: "
            f"{_incompleteness_detail(in_flight_request_count)}"
        )
        self.access_id = access_id
        self.in_flight_request_count = in_flight_request_count
        self.timeout_seconds = timeout_seconds
        self.telemetry = telemetry


class DataAgentTelemetryUnsupportedError(DataAgentProtocolError):
    """Raised when a Data Agent cannot prove its telemetry is final.

    A server that omits ``in_flight_request_count`` or ``telemetry_complete``
    predates the quiescence contract. Its summary may be perfectly accurate,
    but nothing in the response distinguishes that from one truncated by a
    transfer still being written, and polling cannot help because the missing
    field will never appear. Treating silence as completeness is exactly the
    silent under-count the contract exists to prevent, so the wait fails
    immediately instead.
    """

    def __init__(self, access_id: str, missing_fields: tuple[str, ...]):
        super().__init__(
            f"Data Agent telemetry for access {access_id} omits "
            f"{', '.join(missing_fields)}, so its completeness cannot be "
            "verified; upgrade the Data Agent to one that reports transfer "
            "quiescence"
        )
        self.access_id = access_id
        self.missing_fields = missing_fields


@dataclass(frozen=True)
class DataAgentClientSettings:
    base_url: str
    token: str | None = None
    timeout_seconds: float = 30.0
    max_retries: int = 1
    max_response_bytes: int = 4 * 1024 * 1024
    max_artifact_bytes: int = 4 * 1024 * 1024

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
        if (
            not isinstance(self.max_artifact_bytes, int)
            or isinstance(self.max_artifact_bytes, bool)
            or self.max_artifact_bytes <= 0
        ):
            raise ValueError(
                "Data Agent max_artifact_bytes must be a positive integer"
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
        if kind == "artifact_uri" and not isinstance(value, str):
            raise DataAgentProtocolError(
                "Data Agent artifact_uri payload.value must be a string"
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
class DataAgentFetchedArtifact:
    """A bounded, verified artifact safe to return through an Agent tool."""

    access_id: str
    media_type: str
    content: Any
    size_bytes: int
    sha256: str


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
    in_flight_request_count: int | None = None
    """Transfers started but not yet durably recorded.

    A summary read while this is above zero is not final. ``None`` means the
    Data Agent did not report the field at all, which is deliberately *not*
    the same as an explicit zero: zero is a measurement, absence is silence.
    """
    server_reported_complete: bool | None = None
    """The Data Agent's own stability verdict for this snapshot.

    The counter alone cannot express a transfer that both started and finished
    while the durable summary was being read: it reads zero at both edges. The
    Data Agent brackets the read with a generation check and publishes the
    verdict here. ``None`` means the Data Agent predates the field.
    """

    @property
    def missing_completeness_fields(self) -> tuple[str, ...]:
        """Names of the fields this response needed but did not carry."""
        missing = []
        if self.in_flight_request_count is None:
            missing.append("in_flight_request_count")
        if self.server_reported_complete is None:
            missing.append("telemetry_complete")
        return tuple(missing)

    @property
    def telemetry_supported(self) -> bool:
        """Whether the Data Agent can answer the completeness question at all.

        False for a server predating the quiescence contract. Such a summary
        is still parsed and readable, but it can never be treated as a final
        research observation, because nothing in it distinguishes an accurate
        total from one truncated by a transfer still being written.
        """
        return not self.missing_completeness_fields

    @property
    def telemetry_complete(self) -> bool:
        """Whether every transfer that process handled is already counted.

        Fails closed on silence. Both signals must be present and must agree:
        the Data Agent increments the in-flight counter before the first byte
        is written and decrements it only after the transfer row is committed,
        and it separately confirms that no transfer was reserved while the
        summary was being read. A server that supplies neither signal is
        reported as incomplete rather than trusted by default.
        """
        if not self.telemetry_supported:
            return False
        if self.in_flight_request_count != 0:
            return False
        return bool(self.server_reported_complete)

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
        # Absent stays None rather than collapsing to zero: a legacy server
        # that says nothing must not be indistinguishable from a current one
        # reporting that nothing is in flight.
        in_flight = downloads.get("in_flight_request_count")
        if in_flight is not None and (
            not isinstance(in_flight, int)
            or isinstance(in_flight, bool)
            or in_flight < 0
        ):
            raise DataAgentProtocolError(
                "Data Agent artifact_download.in_flight_request_count must be "
                "a non-negative integer or absent"
            )
        server_reported_complete = downloads.get("telemetry_complete")
        if server_reported_complete is not None and not isinstance(
            server_reported_complete,
            bool,
        ):
            raise DataAgentProtocolError(
                "Data Agent artifact_download.telemetry_complete must be a "
                "boolean or absent"
            )
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
            in_flight_request_count=in_flight,
            server_reported_complete=server_reported_complete,
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
        *,
        wait_for_quiescence: bool = False,
        quiescence_timeout_seconds: float = 5.0,
    ) -> DataAgentAccessTelemetry:
        """Return transfer telemetry attributed to one access."""

    def fetch_artifact(
        self,
        request: DataAgentAccessRequest,
    ) -> DataAgentFetchedArtifact:
        """Refresh and fetch one bounded artifact for an existing access."""


class _RejectRedirects(HTTPRedirectHandler):
    """Turn every redirect into an HTTPError instead of following it."""

    def redirect_request(
        self,
        req: Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        return None


class HttpDataAgentClient:
    """Synchronous standard-library client for one Data Agent endpoint."""

    _RETRYABLE_HTTP_STATUSES = frozenset({502, 503, 504})

    def __init__(
        self,
        settings: DataAgentClientSettings,
        *,
        opener: Callable[..., Any] = urlopen,
        artifact_opener: Callable[..., Any] | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.settings = settings
        self._opener = opener
        self._artifact_opener = (
            artifact_opener
            if artifact_opener is not None
            else build_opener(_RejectRedirects()).open
        )
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

    def fetch_artifact(
        self,
        request: DataAgentAccessRequest,
    ) -> DataAgentFetchedArtifact:
        """Refresh, download, and validate one Data Agent artifact.

        The caller supplies the original access request, never a URL. Replaying
        that idempotent request lets the Data Agent issue a fresh signed URL
        without persisting an expiring capability in Pathfinder. The URL stays
        inside this client and is accepted only when it points to the exact
        artifact route on the configured Data Agent origin.
        """
        refreshed = self.access(request)
        payload = refreshed.payload
        if payload.kind != "artifact_uri":
            raise DataAgentArtifactUnsupportedError(
                f"Data Agent access {request.access_id} does not refer to "
                "a downloadable artifact"
            )
        artifact_url = self._validated_artifact_url(
            payload.value,
            request.access_id,
        )
        raw, response_media_type = self._download_artifact(artifact_url)
        expected_media_type = _base_media_type(payload.media_type)
        if response_media_type != expected_media_type:
            raise DataAgentProtocolError(
                "Data Agent artifact Content-Type does not match the access "
                "response"
            )

        digest = sha256(raw).hexdigest()
        if payload.sha256 is None:
            raise DataAgentProtocolError(
                "Data Agent artifact payload must include sha256"
            )
        if digest != payload.sha256:
            raise DataAgentArtifactIntegrityError(
                f"Data Agent artifact for access {request.access_id} failed "
                "SHA-256 verification"
            )

        if _is_json_media_type(response_media_type):
            try:
                content = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise DataAgentProtocolError(
                    "Data Agent artifact is not valid UTF-8 JSON"
                ) from exc
        elif response_media_type.startswith("text/"):
            try:
                content = raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise DataAgentProtocolError(
                    "Data Agent text artifact is not valid UTF-8"
                ) from exc
        else:
            raise DataAgentArtifactUnsupportedError(
                "Data Agent artifact media type is not Agent-readable: "
                f"{response_media_type}"
            )
        return DataAgentFetchedArtifact(
            access_id=request.access_id,
            media_type=response_media_type,
            content=content,
            size_bytes=len(raw),
            sha256=digest,
        )

    def _validated_artifact_url(self, value: Any, access_id: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise DataAgentProtocolError(
                "Data Agent artifact URL must be a non-empty string"
            )
        candidate = urlparse(value)
        configured = urlparse(self.settings.base_url)
        try:
            same_origin = (
                candidate.scheme.lower() == configured.scheme.lower()
                and candidate.hostname == configured.hostname
                and _effective_port(candidate) == _effective_port(configured)
            )
        except ValueError as exc:
            raise DataAgentArtifactSecurityError(
                "Data Agent artifact URL has an invalid port"
            ) from exc
        expected_url = urljoin(
            self.settings.base_url.rstrip("/") + "/",
            f"v1/artifacts/{quote(access_id, safe='')}",
        )
        expected_path = urlparse(expected_url).path
        if (
            candidate.scheme not in {"http", "https"}
            or not candidate.netloc
            or candidate.username is not None
            or candidate.password is not None
            or candidate.fragment
            or not same_origin
            or candidate.path != expected_path
        ):
            raise DataAgentArtifactSecurityError(
                "Data Agent artifact URL is outside the configured artifact "
                "endpoint"
            )
        return value

    def _download_artifact(self, artifact_url: str) -> tuple[bytes, str]:
        headers = {
            "Accept": "application/json, text/*;q=0.9",
            "User-Agent": "pathfinder-data-agent-client/0.1",
            "X-Pathfinder-Protocol-Version": DATA_AGENT_API_VERSION,
        }
        if self.settings.token:
            headers["Authorization"] = f"Bearer {self.settings.token}"
        request = Request(artifact_url, headers=headers, method="GET")
        try:
            with self._artifact_opener(
                request,
                timeout=self.settings.timeout_seconds,
            ) as response:
                status = getattr(response, "status", None)
                if status is None and hasattr(response, "getcode"):
                    status = response.getcode()
                if status != 200:
                    raise DataAgentProtocolError(
                        "Data Agent artifact response must use HTTP 200"
                    )
                content_length = response.headers.get("Content-Length")
                if content_length is not None:
                    try:
                        declared_length = int(content_length)
                    except ValueError as exc:
                        raise DataAgentProtocolError(
                            "Data Agent artifact Content-Length is invalid"
                        ) from exc
                    if declared_length < 0:
                        raise DataAgentProtocolError(
                            "Data Agent artifact Content-Length is invalid"
                        )
                    if declared_length > self.settings.max_artifact_bytes:
                        raise DataAgentArtifactTooLargeError(
                            "Data Agent artifact exceeds max_artifact_bytes"
                        )
                media_type = _base_media_type(
                    response.headers.get("Content-Type", "")
                )
                raw = response.read(self.settings.max_artifact_bytes + 1)
        except HTTPError as exc:
            try:
                status_code = exc.code
            finally:
                exc.close()
            if 300 <= status_code < 400:
                raise DataAgentArtifactRedirectError(
                    "Data Agent artifact redirects are not allowed"
                ) from None
            raise DataAgentHTTPError(
                status_code,
                "artifact request failed",
            ) from None
        except (URLError, TimeoutError, socket.timeout) as exc:
            # Never include the signed URL (and therefore its query
            # capability) in an error, log, or Agent-visible tool response.
            raise DataAgentUnavailableError(
                "cannot reach the configured Data Agent artifact endpoint"
            ) from None

        if len(raw) > self.settings.max_artifact_bytes:
            raise DataAgentArtifactTooLargeError(
                "Data Agent artifact exceeds max_artifact_bytes"
            )
        if not media_type:
            raise DataAgentProtocolError(
                "Data Agent artifact Content-Type is required"
            )
        return raw, media_type

    def get_access_telemetry(
        self,
        access_id: str,
        *,
        wait_for_quiescence: bool = False,
        quiescence_timeout_seconds: float = 5.0,
        quiescence_poll_seconds: float = 0.02,
    ) -> DataAgentAccessTelemetry:
        """Return transfer telemetry attributed to one access.

        Artifact transfers are recorded after the response body is written, so
        a summary read immediately after a download can omit that transfer.
        With ``wait_for_quiescence`` the client polls until the Data Agent
        reports no in-flight transfers, yielding a final summary.

        The wait is bounded, and it fails closed: on timeout this raises
        :class:`DataAgentTelemetryQuiescenceError` instead of returning the
        provisional summary. A stalled transfer must fail the Pathfinder
        session rather than contribute silently under-counted bytes and
        latency to the research record.

        A Data Agent that omits the completeness fields altogether raises
        :class:`DataAgentTelemetryUnsupportedError` on the first response,
        without waiting; its summary cannot be verified at any timeout.
        """
        if not isinstance(access_id, str) or not access_id.strip():
            raise ValueError("Data Agent access_id cannot be empty")
        # Both are checked even when not waiting, so a bad value is reported
        # by the call that passed it rather than by some later caller that
        # happens to be the first to set wait_for_quiescence.
        quiescence_timeout_seconds = validated_timing_seconds(
            quiescence_timeout_seconds,
            "quiescence_timeout_seconds",
            allow_zero=True,
        )
        # A zero or negative poll interval would spin without ever yielding.
        quiescence_poll_seconds = validated_timing_seconds(
            quiescence_poll_seconds,
            "quiescence_poll_seconds",
            allow_zero=False,
        )
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
        deadline = time.monotonic() + quiescence_timeout_seconds
        while True:
            payload = self._send_with_retries(
                Request(telemetry_url, headers=headers, method="GET")
            )
            result = DataAgentAccessTelemetry.from_dict(payload)
            if result.access_id != access_id:
                raise DataAgentProtocolError(
                    "Data Agent telemetry access_id does not match the request"
                )
            if not wait_for_quiescence:
                return result
            if not result.telemetry_supported:
                # Polling cannot help: the field will never appear. Fail now
                # rather than burning the timeout to reach the same verdict.
                raise DataAgentTelemetryUnsupportedError(
                    access_id,
                    result.missing_completeness_fields,
                )
            if result.telemetry_complete:
                return result
            if time.monotonic() >= deadline:
                logger.error(
                    "Data Agent telemetry for access %s did not become final "
                    "within %.3fs (%s); refusing to return a provisional "
                    "summary",
                    access_id,
                    quiescence_timeout_seconds,
                    _incompleteness_detail(result.in_flight_request_count),
                )
                raise DataAgentTelemetryQuiescenceError(
                    access_id,
                    result.in_flight_request_count,
                    quiescence_timeout_seconds,
                    result,
                )
            self._sleep(quiescence_poll_seconds)

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
            # The URL that actually failed, not the access endpoint: a
            # telemetry read that cannot connect must not be reported as an
            # access failure, or the operator debugs the wrong route.
            raise DataAgentUnavailableError(
                f"cannot reach Data Agent at {request.full_url}: {exc}"
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


def _base_media_type(value: str) -> str:
    return value.split(";", 1)[0].strip().lower()


def _is_json_media_type(media_type: str) -> bool:
    return media_type == "application/json" or media_type.endswith("+json")


def _effective_port(parsed: Any) -> int | None:
    if parsed.port is not None:
        return parsed.port
    if parsed.scheme.lower() == "http":
        return 80
    if parsed.scheme.lower() == "https":
        return 443
    return None


def _nonnegative_number(
    mapping: Mapping[str, Any],
    name: str,
) -> float:
    if name not in mapping:
        raise DataAgentProtocolError(
            f"Data Agent telemetry.{name} is required"
        )
    return _validated_nonnegative_number(mapping[name], name)


def validated_timing_seconds(
    value: Any,
    name: str,
    *,
    allow_zero: bool,
) -> float:
    """Validate a caller-supplied duration in seconds.

    Bad timing arguments do not fail loudly on their own, which is why they
    are rejected here rather than diagnosed later from a stuck session:
    ``nan`` makes every deadline comparison false, so a bounded poll loop
    never exits; ``inf`` turns the same bounded wait into an unbounded one.
    Either one silently removes the timeout that the fail-closed telemetry
    contract depends on. ``True`` is likewise refused, because ``bool`` is an
    ``int`` and would otherwise pass as a one-second duration.

    Raises ``ValueError``: this is a caller bug, not a Data Agent protocol
    violation.
    """
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"{name} must be a number, not {type(value).__name__}")
    numeric = float(value)
    if not math.isfinite(numeric):
        raise ValueError(f"{name} must be finite, not {value!r}")
    if allow_zero:
        if numeric < 0:
            raise ValueError(f"{name} cannot be negative")
    elif numeric <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return numeric


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
