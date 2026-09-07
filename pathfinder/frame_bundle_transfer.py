"""Native, fail-closed delivery of ``sampled_frame_bundle`` artifacts.

This is the operation that replaces the ad hoc script used for the first
cross-node smoke. It performs, in order: a Data Agent access, a bounded
binary download, artifact digest and bundle validation, a quiescent
telemetry read, and strict delivery-completeness checks.

Why the checks are strict
-------------------------
A frame bundle that arrives corrupted, truncated, or unaccounted for in
transfer telemetry is not a smaller observation — it is not an observation
at all. Every failure below raises rather than degrading the result, so a
delivery or telemetry problem can never be recorded as a completed
measurement. The smoke this module drives is *transfer conformance*
evidence: it shows the path works and the bytes are what they claim to be.
It is not a latency benchmark and never confirmatory scientific evidence.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .data_agent_client import (
    DataAgentAccessRequest,
    DataAgentAccessTelemetry,
    DataAgentArtifactIntegrityError,
    DataAgentArtifactRedirectError,
    DataAgentArtifactSecurityError,
    DataAgentArtifactTooLargeError,
    DataAgentArtifactUnsupportedError,
    DataAgentBinaryArtifact,
    DataAgentClientSettings,
    DataAgentHTTPError,
    DataAgentProtocolError,
    DataAgentTelemetryQuiescenceError,
    DataAgentTelemetryUnsupportedError,
    DataAgentUnavailableError,
    HttpDataAgentClient,
    validated_timing_seconds,
)
from .frame_bundle import REPRESENTATION_ID
from .frame_bundle_ingest import (
    DEFAULT_FRAME_BUNDLE_LIMITS,
    DEFERRED_DECODE_REQUIREMENTS,
    FRAME_BUNDLE_ALLOWED_MEDIA_TYPES,
    FRAME_BUNDLE_MEDIA_TYPE,
    FrameBundleArchiveError,
    FrameBundleCanonicalizationError,
    FrameBundleIdentityError,
    FrameBundleLimitError,
    FrameBundleLimits,
    FrameBundleManifestError,
    ValidatedFrameBundle,
    validate_frame_bundle_bytes,
)


FRAME_BUNDLE_SMOKE_SCHEMA_VERSION = (
    "pathfinder.frame-bundle-transfer-smoke/v0.1"
)

SMOKE_REPORT_NAME = "smoke-result.json"
SMOKE_FAILURE_NAME = "smoke-failure.json"

_EVIDENCE_CLASS = "transfer_conformance"
_EVIDENCE_STATEMENT = (
    "This report is transfer and conformance evidence only: it shows that a "
    "sampled_frame_bundle artifact crossed the node boundary intact, that "
    "its bytes matched the declared digest, that its tar and manifest satisfy "
    "the frame bundle contract, and that Data Agent transfer telemetry "
    "accounts for the delivery. The recorded durations describe this single "
    "unreplicated transfer under uncontrolled conditions; they are not a "
    "performance measurement, not a cost measurement, and not confirmatory "
    "scientific evidence."
)


class FrameBundleTransferError(RuntimeError):
    """Base error for a refused frame bundle delivery."""

    failure_class = "frame_bundle_transfer_failed"


class FrameBundleDeliveryError(FrameBundleTransferError):
    """Raised when transfer telemetry cannot account for the delivery."""

    def __init__(self, failure_class: str, message: str):
        super().__init__(message)
        self.failure_class = failure_class


#: Every way a delivery can be refused, as stable strings for reports.
FRAME_BUNDLE_FAILURE_CLASSES: tuple[str, ...] = (
    "artifact_digest_mismatch",
    "artifact_media_type_rejected",
    "artifact_too_large",
    "artifact_url_rejected",
    "artifact_redirect_rejected",
    "bundle_archive_invalid",
    "bundle_identity_mismatch",
    "bundle_limit_exceeded",
    "bundle_manifest_invalid",
    "bundle_not_canonical",
    "bytes_sent_below_artifact_size",
    "catalog_version_mismatch",
    "data_agent_http_error",
    "data_agent_unavailable",
    "no_completed_request",
    "no_download_recorded",
    "object_id_mismatch",
    "partial_download_only",
    "protocol_error",
    "representation_id_mismatch",
    "telemetry_incomplete",
    "telemetry_not_quiescent",
    "telemetry_unsupported",
    "frame_bundle_transfer_failed",
)

# Ordered most specific first: several of these are subclasses of one
# another, so a dict lookup on type() would miss and isinstance order
# decides the reported class.
_FAILURE_CLASS_BY_TYPE: tuple[tuple[type, str], ...] = (
    (FrameBundleDeliveryError, ""),  # carries its own class
    (FrameBundleLimitError, "bundle_limit_exceeded"),
    (FrameBundleIdentityError, "bundle_identity_mismatch"),
    (FrameBundleManifestError, "bundle_manifest_invalid"),
    (FrameBundleCanonicalizationError, "bundle_not_canonical"),
    (FrameBundleArchiveError, "bundle_archive_invalid"),
    (DataAgentTelemetryUnsupportedError, "telemetry_unsupported"),
    (DataAgentTelemetryQuiescenceError, "telemetry_not_quiescent"),
    (DataAgentArtifactRedirectError, "artifact_redirect_rejected"),
    (DataAgentArtifactSecurityError, "artifact_url_rejected"),
    (DataAgentArtifactTooLargeError, "artifact_too_large"),
    (DataAgentArtifactUnsupportedError, "artifact_media_type_rejected"),
    (DataAgentArtifactIntegrityError, "artifact_digest_mismatch"),
    (DataAgentHTTPError, "data_agent_http_error"),
    (DataAgentUnavailableError, "data_agent_unavailable"),
    (DataAgentProtocolError, "protocol_error"),
)


def classify_frame_bundle_failure(error: BaseException) -> str:
    """Map an exception to a stable failure class for reports."""
    for kind, name in _FAILURE_CLASS_BY_TYPE:
        if isinstance(error, kind):
            return name or getattr(
                error, "failure_class", "frame_bundle_transfer_failed"
            )
    return "frame_bundle_transfer_failed"


#: Attribute name used to attach audit evidence to a raised exception. The
#: original exception is re-raised unchanged so its type and message stay the
#: primary classification; the evidence rides alongside rather than wrapping.
AUDIT_ATTRIBUTE = "frame_bundle_audit"

_QUERY_STRING = re.compile(r"\?\S*")


def _redact(message: str) -> str:
    """Strip any query string from a message before it is recorded.

    Errors on this path already avoid signed URLs, but an audit record is
    written to disk and read by operators, so the query string — the part
    that would carry a signature or token if one ever leaked into a message
    — is removed defensively rather than trusted to stay absent.
    """
    return _QUERY_STRING.sub("?<redacted>", message)


@dataclass(frozen=True)
class FrameBundleExecutionPhase:
    """How far the operation got before it failed.

    Recorded because "no telemetry" means two very different things: nothing
    crossed the network, or bytes moved and were then refused. Only the
    second consumed remote resources that must still be accounted for.
    """

    access_completed: bool = False
    artifact_download_started: bool = False
    artifact_download_completed: bool = False
    bundle_validation_completed: bool = False
    telemetry_reconciliation_attempted: bool = False

    def to_dict(self) -> dict[str, bool]:
        return {
            "access_completed": self.access_completed,
            "artifact_download_started": self.artifact_download_started,
            "artifact_download_completed": (
                self.artifact_download_completed
            ),
            "bundle_validation_completed": (
                self.bundle_validation_completed
            ),
            "telemetry_reconciliation_attempted": (
                self.telemetry_reconciliation_attempted
            ),
        }


@dataclass(frozen=True)
class FrameBundleTelemetryEvidence:
    """Best-effort transfer accounting attached to a failed delivery.

    Counters are ``None`` unless the Data Agent actually reported them. A
    missing counter is never rendered as zero: absence is silence, not a
    measurement of nothing.
    """

    status: str
    telemetry_complete: bool | None = None
    in_flight_request_count: int | None = None
    download_request_count: int | None = None
    completed_request_count: int | None = None
    full_download_count: int | None = None
    bytes_sent: int | None = None
    transfer_latency_ms: float | None = None
    object_id: str | None = None
    object_catalog_version: str | None = None
    representation_id: str | None = None
    error_class: str | None = None
    error_message: str | None = None

    @classmethod
    def not_attempted(cls, reason: str) -> "FrameBundleTelemetryEvidence":
        return cls(status="not_attempted", error_message=reason)

    @classmethod
    def unavailable(
        cls, error: BaseException
    ) -> "FrameBundleTelemetryEvidence":
        return cls(
            status="unavailable",
            error_class=type(error).__name__,
            error_message=_redact(str(error)),
        )

    @classmethod
    def from_telemetry(
        cls, telemetry: DataAgentAccessTelemetry
    ) -> "FrameBundleTelemetryEvidence":
        return cls(
            status=(
                "complete" if telemetry.telemetry_complete else "incomplete"
            ),
            telemetry_complete=telemetry.telemetry_complete,
            in_flight_request_count=telemetry.in_flight_request_count,
            download_request_count=telemetry.download_request_count,
            completed_request_count=telemetry.completed_request_count,
            full_download_count=telemetry.full_download_count,
            bytes_sent=telemetry.bytes_sent,
            transfer_latency_ms=telemetry.transfer_latency_ms,
            object_id=telemetry.object_id,
            object_catalog_version=telemetry.object_catalog_version,
            representation_id=telemetry.representation_id,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "telemetry_complete": self.telemetry_complete,
            "in_flight_request_count": self.in_flight_request_count,
            "download_request_count": self.download_request_count,
            "completed_request_count": self.completed_request_count,
            "full_download_count": self.full_download_count,
            "bytes_sent": self.bytes_sent,
            "transfer_latency_ms": self.transfer_latency_ms,
            "object_id": self.object_id,
            "object_catalog_version": self.object_catalog_version,
            "representation_id": self.representation_id,
            "error_class": self.error_class,
            "error_message": self.error_message,
        }


@dataclass(frozen=True)
class FrameBundleTransferAudit:
    """Everything known about a refused delivery, primary failure first."""

    primary_failure_class: str
    primary_error_class: str
    primary_message: str
    execution_phase: FrameBundleExecutionPhase
    telemetry_reconciliation: FrameBundleTelemetryEvidence

    def to_dict(self) -> dict[str, Any]:
        return {
            "primary_failure_class": self.primary_failure_class,
            "primary_error_class": self.primary_error_class,
            "primary_message": self.primary_message,
            "execution_phase": self.execution_phase.to_dict(),
            "telemetry_reconciliation": (
                self.telemetry_reconciliation.to_dict()
            ),
        }


class _PhaseRecorder:
    """Collects the client's phase callbacks into an immutable snapshot."""

    _EVENTS = frozenset({
        "access_completed",
        "artifact_download_started",
        "artifact_download_completed",
    })

    def __init__(self) -> None:
        self._reached: set[str] = set()
        self.bundle_validation_completed = False
        self.telemetry_reconciliation_attempted = False

    def note(self, event: str) -> None:
        if event in self._EVENTS:
            self._reached.add(event)

    @property
    def download_started(self) -> bool:
        return "artifact_download_started" in self._reached

    def snapshot(self) -> FrameBundleExecutionPhase:
        return FrameBundleExecutionPhase(
            access_completed="access_completed" in self._reached,
            artifact_download_started=self.download_started,
            artifact_download_completed=(
                "artifact_download_completed" in self._reached
            ),
            bundle_validation_completed=self.bundle_validation_completed,
            telemetry_reconciliation_attempted=(
                self.telemetry_reconciliation_attempted
            ),
        )


def _best_effort_telemetry(
    client: Any,
    access_id: str,
    quiescence_timeout_seconds: float,
) -> FrameBundleTelemetryEvidence:
    """Read quiescent telemetry for audit only; never raise.

    Any error here is *secondary*. It is recorded as evidence and must not
    displace the failure that actually refused the bundle.
    """
    try:
        telemetry = client.get_access_telemetry(
            access_id,
            wait_for_quiescence=True,
            quiescence_timeout_seconds=quiescence_timeout_seconds,
        )
    except Exception as exc:  # noqa: BLE001 - secondary evidence only
        return FrameBundleTelemetryEvidence.unavailable(exc)
    if not isinstance(telemetry, DataAgentAccessTelemetry):
        return FrameBundleTelemetryEvidence(
            status="unavailable",
            error_class="TypeError",
            error_message="telemetry response was not a parsed summary",
        )
    return FrameBundleTelemetryEvidence.from_telemetry(telemetry)


def _attach_audit(
    error: BaseException,
    *,
    phase: FrameBundleExecutionPhase,
    telemetry: FrameBundleTelemetryEvidence,
) -> None:
    setattr(
        error,
        AUDIT_ATTRIBUTE,
        FrameBundleTransferAudit(
            primary_failure_class=classify_frame_bundle_failure(error),
            primary_error_class=type(error).__name__,
            primary_message=_redact(str(error)),
            execution_phase=phase,
            telemetry_reconciliation=telemetry,
        ),
    )


def transfer_audit_of(
    error: BaseException,
) -> FrameBundleTransferAudit | None:
    """Return the audit evidence attached to a refused delivery, if any."""
    audit = getattr(error, AUDIT_ATTRIBUTE, None)
    return audit if isinstance(audit, FrameBundleTransferAudit) else None


@dataclass(frozen=True)
class FrameBundleDeliveryChecks:
    """The transfer accounting behind an accepted delivery."""

    telemetry_supported: bool
    telemetry_complete: bool
    in_flight_request_count: int | None
    download_request_count: int
    completed_request_count: int
    full_download_count: int
    bytes_sent: int
    artifact_size_bytes: int
    server_reported_transfer_latency_ms: float
    exactly_one_full_download: bool
    bytes_sent_equals_artifact_size: bool
    telemetry_object_id: str | None
    telemetry_object_catalog_version: str | None
    expected_object_catalog_version: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "telemetry_supported": self.telemetry_supported,
            "telemetry_complete": self.telemetry_complete,
            "in_flight_request_count": self.in_flight_request_count,
            "download_request_count": self.download_request_count,
            "completed_request_count": self.completed_request_count,
            "full_download_count": self.full_download_count,
            "bytes_sent": self.bytes_sent,
            "artifact_size_bytes": self.artifact_size_bytes,
            "server_reported_transfer_latency_ms": (
                self.server_reported_transfer_latency_ms
            ),
            "exactly_one_full_download": self.exactly_one_full_download,
            "bytes_sent_equals_artifact_size": (
                self.bytes_sent_equals_artifact_size
            ),
            "telemetry_object_id": self.telemetry_object_id,
            "telemetry_object_catalog_version": (
                self.telemetry_object_catalog_version
            ),
            "expected_object_catalog_version": (
                self.expected_object_catalog_version
            ),
        }


@dataclass(frozen=True)
class FrameBundleTransfer:
    """A completed, fully accounted-for frame bundle delivery."""

    bundle: ValidatedFrameBundle
    artifact: DataAgentBinaryArtifact
    telemetry: DataAgentAccessTelemetry
    delivery: FrameBundleDeliveryChecks
    access_request: DataAgentAccessRequest

    @property
    def object_id(self) -> str:
        return self.bundle.object_id

    def latency_ms(self) -> dict[str, float | None]:
        """The four durations, kept separate because they measure
        different things: the Data Agent's own service time, the client's
        access round trip, the artifact body transfer, and the server's
        own view of the transfer."""
        return {
            "data_agent_service": self.artifact.service_latency_ms,
            "client_access_round_trip": self.artifact.client_round_trip_ms,
            "artifact_download_elapsed": self.artifact.download_elapsed_ms,
            "server_reported_transfer": (
                self.delivery.server_reported_transfer_latency_ms
            ),
        }


def build_frame_bundle_access_request(
    *,
    object_id: str,
    plan_id: str,
    requested_location: str,
    session_id: str,
    trial_id: str,
    task_class_id: str = "video_qa",
    representation_id: str = REPRESENTATION_ID,
    access_id: str | None = None,
    plan_epoch: int = 0,
    event_index: int = 0,
    latency_multiplier: float = 1.0,
) -> DataAgentAccessRequest:
    """Build the idempotent access request for one bundle download.

    The access ID is derived from the session, object, and representation
    unless supplied, so re-running the same session is idempotent at the
    Data Agent while a fresh session gets a fresh access.
    """
    resolved_access_id = access_id or str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            (
                "pathfinder-frame-bundle:"
                f"{session_id}:{object_id}:{representation_id}"
            ),
        )
    )
    return DataAgentAccessRequest(
        access_id=resolved_access_id,
        session_id=session_id,
        trial_id=trial_id,
        plan_id=plan_id,
        plan_epoch=plan_epoch,
        task_class_id=task_class_id,
        representation_id=representation_id,
        event_index=event_index,
        latency_multiplier=latency_multiplier,
        binding={"location": requested_location},
        object_id=object_id,
    )


def _reconcile_delivery(
    telemetry: DataAgentAccessTelemetry,
    *,
    artifact_size_bytes: int,
    expected_object_id: str,
    expected_representation_id: str,
    expected_object_catalog_version: str | None,
) -> FrameBundleDeliveryChecks:
    if not telemetry.telemetry_supported:
        raise FrameBundleDeliveryError(
            "telemetry_unsupported",
            "Data Agent transfer telemetry omits "
            f"{', '.join(telemetry.missing_completeness_fields)}, so the "
            "delivery cannot be proven complete",
        )
    if telemetry.in_flight_request_count != 0:
        raise FrameBundleDeliveryError(
            "telemetry_not_quiescent",
            f"{telemetry.in_flight_request_count} transfer(s) are still in "
            "flight, so the summary is not a stable point in time",
        )
    if not telemetry.telemetry_complete:
        raise FrameBundleDeliveryError(
            "telemetry_incomplete",
            "the Data Agent did not report its transfer summary as complete",
        )
    if telemetry.representation_id != expected_representation_id:
        raise FrameBundleDeliveryError(
            "representation_id_mismatch",
            f"telemetry reports representation {telemetry.representation_id!r},"
            f" expected {expected_representation_id!r}",
        )
    if (
        telemetry.object_id is not None
        and telemetry.object_id != expected_object_id
    ):
        raise FrameBundleDeliveryError(
            "object_id_mismatch",
            f"telemetry reports object {telemetry.object_id!r}, expected "
            f"{expected_object_id!r}",
        )
    if expected_object_catalog_version is not None and (
        telemetry.object_catalog_version != expected_object_catalog_version
    ):
        raise FrameBundleDeliveryError(
            "catalog_version_mismatch",
            "telemetry reports object catalog version "
            f"{telemetry.object_catalog_version!r}, expected "
            f"{expected_object_catalog_version!r}",
        )
    if telemetry.download_request_count < 1:
        raise FrameBundleDeliveryError(
            "no_download_recorded",
            "the Data Agent recorded no artifact download for this access",
        )
    if telemetry.completed_request_count < 1:
        raise FrameBundleDeliveryError(
            "no_completed_request",
            "the Data Agent recorded no completed artifact request",
        )
    if telemetry.full_download_count < 1:
        raise FrameBundleDeliveryError(
            "partial_download_only",
            "the Data Agent recorded only partial artifact transfers",
        )
    if telemetry.bytes_sent < artifact_size_bytes:
        raise FrameBundleDeliveryError(
            "bytes_sent_below_artifact_size",
            f"the Data Agent reports {telemetry.bytes_sent} bytes sent, "
            f"below the {artifact_size_bytes}-byte artifact",
        )
    return FrameBundleDeliveryChecks(
        telemetry_supported=True,
        telemetry_complete=True,
        in_flight_request_count=telemetry.in_flight_request_count,
        download_request_count=telemetry.download_request_count,
        completed_request_count=telemetry.completed_request_count,
        full_download_count=telemetry.full_download_count,
        bytes_sent=telemetry.bytes_sent,
        artifact_size_bytes=artifact_size_bytes,
        server_reported_transfer_latency_ms=telemetry.transfer_latency_ms,
        exactly_one_full_download=(
            telemetry.full_download_count == 1
            and telemetry.download_request_count == 1
            and telemetry.completed_request_count == 1
        ),
        bytes_sent_equals_artifact_size=(
            telemetry.bytes_sent == artifact_size_bytes
        ),
        telemetry_object_id=telemetry.object_id,
        telemetry_object_catalog_version=telemetry.object_catalog_version,
        expected_object_catalog_version=expected_object_catalog_version,
    )


def fetch_validated_frame_bundle(
    client: Any,
    request: DataAgentAccessRequest,
    *,
    expected_object_id: str | None = None,
    expected_artifact_sha256: str | None = None,
    expected_artifact_size_bytes: int | None = None,
    expected_object_catalog_version: str | None = None,
    limits: FrameBundleLimits = DEFAULT_FRAME_BUNDLE_LIMITS,
    quiescence_timeout_seconds: float = 5.0,
) -> FrameBundleTransfer:
    """Access, download, validate, and reconcile one frame bundle.

    Raises rather than returning a degraded result: a bundle that fails any
    check contributes no *observation* at all, and there is no partial
    success to recover.

    It does still contribute an *audit record*. Once an artifact download
    has started, the bytes crossed the network whether or not the bundle is
    accepted, so a post-download failure carries a
    :class:`FrameBundleTransferAudit` on the raised exception — reachable
    via :func:`transfer_audit_of` — holding the execution phase reached and
    a best-effort quiescent telemetry read. The original exception is
    re-raised unchanged, so the primary classification is always the failure
    that actually refused the bundle, never the secondary telemetry result.
    """
    quiescence_timeout_seconds = validated_timing_seconds(
        quiescence_timeout_seconds,
        "quiescence_timeout_seconds",
        allow_zero=True,
    )
    object_id = expected_object_id or request.object_id
    if not object_id:
        raise ValueError(
            "a frame bundle download requires an expected object_id"
        )

    if not hasattr(client, "fetch_binary_artifact"):
        raise TypeError(
            "a frame bundle download needs a client with "
            "fetch_binary_artifact(); the Agent-facing fetch_artifact() "
            "deliberately cannot return tar bytes"
        )
    recorder = _PhaseRecorder()

    try:
        artifact = client.fetch_binary_artifact(
            request,
            allowed_media_types=FRAME_BUNDLE_ALLOWED_MEDIA_TYPES,
            on_phase=recorder.note,
        )
        if (
            artifact.object_id is not None
            and artifact.object_id != object_id
        ):
            raise FrameBundleDeliveryError(
                "object_id_mismatch",
                f"the access response is for object {artifact.object_id!r}, "
                f"expected {object_id!r}",
            )
        if expected_object_catalog_version is not None and (
            artifact.object_catalog_version != expected_object_catalog_version
        ):
            raise FrameBundleDeliveryError(
                "catalog_version_mismatch",
                "the access response reports object catalog version "
                f"{artifact.object_catalog_version!r}, expected "
                f"{expected_object_catalog_version!r}",
            )
        bundle = validate_frame_bundle_bytes(
            artifact.data,
            expected_object_id=object_id,
            expected_sha256=expected_artifact_sha256,
            expected_size_bytes=expected_artifact_size_bytes,
            artifact_media_type=artifact.media_type,
            limits=limits,
        )
        recorder.bundle_validation_completed = True
    except Exception as exc:
        # A refused bundle is still a transfer that consumed remote
        # resources. Reconcile telemetry for the audit record whenever bytes
        # were actually requested, but only as *secondary* evidence: the
        # exception below is re-raised unchanged, so the bundle failure stays
        # the primary classification and this can never become a completed
        # observation.
        if recorder.download_started:
            recorder.telemetry_reconciliation_attempted = True
            evidence = _best_effort_telemetry(
                client, request.access_id, quiescence_timeout_seconds
            )
        else:
            evidence = FrameBundleTelemetryEvidence.not_attempted(
                "no artifact transfer was started, so there is nothing for "
                "the Data Agent to have recorded"
            )
        _attach_audit(exc, phase=recorder.snapshot(), telemetry=evidence)
        raise

    try:
        telemetry = client.get_access_telemetry(
            request.access_id,
            wait_for_quiescence=True,
            quiescence_timeout_seconds=quiescence_timeout_seconds,
        )
        recorder.telemetry_reconciliation_attempted = True
    except Exception as exc:
        recorder.telemetry_reconciliation_attempted = True
        _attach_audit(
            exc,
            phase=recorder.snapshot(),
            telemetry=FrameBundleTelemetryEvidence.unavailable(exc),
        )
        raise

    evidence = FrameBundleTelemetryEvidence.from_telemetry(telemetry)
    try:
        delivery = _reconcile_delivery(
            telemetry,
            artifact_size_bytes=artifact.size_bytes,
            expected_object_id=object_id,
            expected_representation_id=request.representation_id,
            expected_object_catalog_version=expected_object_catalog_version,
        )
    except Exception as exc:
        _attach_audit(exc, phase=recorder.snapshot(), telemetry=evidence)
        raise

    return FrameBundleTransfer(
        bundle=bundle,
        artifact=artifact,
        telemetry=telemetry,
        delivery=delivery,
        access_request=request,
    )


def _json_safe(value: Any) -> Any:
    """Normalize tuples to lists so the returned report is exactly the
    written one.

    Several sources here are immutable tuples on purpose. Without this, the
    dict handed back to a caller would compare unequal to the JSON it just
    wrote, which is a trap for anyone diffing the two.
    """
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    return value


def _canonical_json(value: Any) -> bytes:
    return (
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False)
        + "\n"
    ).encode("utf-8")


def _atomic_write(path: Path, payload: bytes) -> None:
    """Write via a sibling temporary file so a reader never sees a partial
    report."""
    temporary = path.with_name(f".{path.name}.partial")
    with temporary.open("wb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _origin_of(base_url: str | None) -> str | None:
    """Scheme, host, and port only.

    The base URL is operator-supplied configuration, not a capability, but
    the report still keeps only its origin so no path or query string can
    ride along into a checked-in evidence file.
    """
    if not base_url:
        return None
    parsed = urlparse(base_url)
    try:
        port_number = parsed.port
    except ValueError:
        return None
    if not parsed.scheme or not parsed.hostname:
        return None
    port = f":{port_number}" if port_number is not None else ""
    return f"{parsed.scheme}://{parsed.hostname}{port}"


def _client_base_url(client: Any) -> str | None:
    settings = getattr(client, "settings", None)
    return getattr(settings, "base_url", None)


def run_frame_bundle_transfer_smoke(
    *,
    object_id: str,
    plan_id: str,
    requested_location: str,
    output_dir: str | Path,
    base_url: str | None = None,
    client: Any = None,
    representation_id: str = REPRESENTATION_ID,
    task_class_id: str = "video_qa",
    access_id: str | None = None,
    session_id: str | None = None,
    trial_id: str | None = None,
    latency_multiplier: float = 1.0,
    expected_artifact_sha256: str | None = None,
    expected_artifact_size_bytes: int | None = None,
    expected_object_catalog_version: str | None = None,
    limits: FrameBundleLimits = DEFAULT_FRAME_BUNDLE_LIMITS,
    quiescence_timeout_seconds: float = 5.0,
    timeout_seconds: float = 30.0,
    max_retries: int = 0,
    retain_artifact: bool = False,
) -> dict[str, Any]:
    """Run one real cross-node frame bundle transfer and write a report.

    The bearer token is read from the environment by
    :meth:`DataAgentClientSettings.from_environment`; it is never a
    parameter here and never reaches the report. On failure the output
    directory keeps a ``smoke-failure.json`` and no success report, so a
    refused delivery can never be mistaken for a completed one.
    """
    destination = Path(output_dir)
    if destination.exists():
        raise FrameBundleTransferError(
            f"refusing to overwrite an existing output directory: "
            f"{destination}"
        )
    resolved_session = session_id or f"frame-bundle-smoke-{uuid.uuid4()}"
    resolved_trial = trial_id or resolved_session

    owns_client = client is None
    if owns_client:
        settings = DataAgentClientSettings.from_environment(
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
        )
        settings = replace(
            settings,
            max_artifact_bytes=limits.max_artifact_bytes,
        )
        client = HttpDataAgentClient(settings)
    origin = _origin_of(base_url or _client_base_url(client))

    request = build_frame_bundle_access_request(
        object_id=object_id,
        plan_id=plan_id,
        requested_location=requested_location,
        session_id=resolved_session,
        trial_id=resolved_trial,
        task_class_id=task_class_id,
        representation_id=representation_id,
        access_id=access_id,
        latency_multiplier=latency_multiplier,
    )

    destination.mkdir(parents=True, exist_ok=False)
    try:
        transfer = fetch_validated_frame_bundle(
            client,
            request,
            expected_object_id=object_id,
            expected_artifact_sha256=expected_artifact_sha256,
            expected_artifact_size_bytes=expected_artifact_size_bytes,
            expected_object_catalog_version=expected_object_catalog_version,
            limits=limits,
            quiescence_timeout_seconds=quiescence_timeout_seconds,
        )
    except Exception as exc:
        audit = transfer_audit_of(exc)
        if audit is None:
            audit = FrameBundleTransferAudit(
                primary_failure_class=classify_frame_bundle_failure(exc),
                primary_error_class=type(exc).__name__,
                primary_message=_redact(str(exc)),
                execution_phase=FrameBundleExecutionPhase(),
                telemetry_reconciliation=(
                    FrameBundleTelemetryEvidence.not_attempted(
                        "the operation failed before any artifact transfer "
                        "was started"
                    )
                ),
            )
        failure = {
            "schema_version": FRAME_BUNDLE_SMOKE_SCHEMA_VERSION,
            "status": "failed",
            "failure_class": audit.primary_failure_class,
            "message": audit.primary_message,
            "audit": audit.to_dict(),
            "object_id": object_id,
            "representation_id": representation_id,
            "plan_id": plan_id,
            "requested_location": requested_location,
            "access_id": request.access_id,
            "data_agent_origin": origin,
            "credentials_recorded": False,
            "llm_called": False,
            "eligible_for_scientific_claims": False,
        }
        _atomic_write(
            destination / SMOKE_FAILURE_NAME,
            _canonical_json(_json_safe(failure)),
        )
        raise

    retained: dict[str, Any] | None = None
    if retain_artifact:
        filename = f"{object_id}-{representation_id}.tar"
        _atomic_write(destination / filename, transfer.artifact.data)
        retained = {
            "filename": filename,
            "size_bytes": transfer.artifact.size_bytes,
            "sha256": transfer.artifact.sha256,
        }

    bundle = transfer.bundle
    report: dict[str, Any] = {
        "schema_version": FRAME_BUNDLE_SMOKE_SCHEMA_VERSION,
        "status": "succeeded",
        "evidence_class": _EVIDENCE_CLASS,
        "evidence_statement": _EVIDENCE_STATEMENT,
        "eligible_for_scientific_claims": False,
        "is_performance_measurement": False,
        "is_cost_measurement": False,
        "llm_called": False,
        "credentials_recorded": False,
        "network_calls_performed": True,
        "data_agent_origin": origin,
        "access": {
            "access_id": transfer.artifact.access_id,
            "session_id": resolved_session,
            "trial_id": resolved_trial,
            "plan_id": plan_id,
            "task_class_id": task_class_id,
            "representation_id": representation_id,
            "requested_location": requested_location,
            "served_location": transfer.artifact.location,
            "object_id": transfer.artifact.object_id,
            "object_catalog_version": (
                transfer.artifact.object_catalog_version
            ),
            "latency_multiplier": latency_multiplier,
        },
        "artifact": {
            "media_type": transfer.artifact.media_type,
            "size_bytes": transfer.artifact.size_bytes,
            "sha256": transfer.artifact.sha256,
            "expected_sha256_supplied": (
                expected_artifact_sha256 is not None
            ),
            "expected_size_bytes_supplied": (
                expected_artifact_size_bytes is not None
            ),
        },
        "bundle": {
            "schema_version": bundle.schema_version,
            "representation_id": bundle.representation_id,
            "object_id": bundle.object_id,
            "tar_member_count": bundle.member_count,
            "frame_count": bundle.frame_count,
            "total_jpeg_bytes": bundle.total_jpeg_bytes,
            "manifest_sha256": bundle.manifest_sha256,
            "source": bundle.source.to_dict(),
            "software_versions": dict(bundle.software_versions),
            "sampling_alignment_statement": (
                bundle.sampling_alignment_statement
            ),
            "claims_byte_identity_with_historical_visual_input": False,
            "historical_visual_bytes_retained": False,
            "pixel_decoding_performed": False,
            "deferred_decode_requirements": dict(
                DEFERRED_DECODE_REQUIREMENTS
            ),
            "frames": [
                frame.to_metadata_dict() for frame in bundle.frames
            ],
        },
        "latency_ms": transfer.latency_ms(),
        "delivery": transfer.delivery.to_dict(),
        "limits": limits.to_dict(),
        "outputs": {
            "report": SMOKE_REPORT_NAME,
            "retained_artifact": retained,
        },
    }
    report = _json_safe(report)
    _atomic_write(
        destination / SMOKE_REPORT_NAME,
        _canonical_json(report),
    )
    return report


def frame_bundle_media_types() -> frozenset[str]:
    """The media types this path will download. Exposed for tests."""
    return FRAME_BUNDLE_ALLOWED_MEDIA_TYPES


__all__ = [
    "AUDIT_ATTRIBUTE",
    "FRAME_BUNDLE_FAILURE_CLASSES",
    "FRAME_BUNDLE_MEDIA_TYPE",
    "FRAME_BUNDLE_SMOKE_SCHEMA_VERSION",
    "FrameBundleDeliveryChecks",
    "FrameBundleExecutionPhase",
    "FrameBundleTelemetryEvidence",
    "FrameBundleTransferAudit",
    "FrameBundleDeliveryError",
    "FrameBundleTransfer",
    "FrameBundleTransferError",
    "SMOKE_FAILURE_NAME",
    "SMOKE_REPORT_NAME",
    "build_frame_bundle_access_request",
    "classify_frame_bundle_failure",
    "fetch_validated_frame_bundle",
    "frame_bundle_media_types",
    "run_frame_bundle_transfer_smoke",
    "transfer_audit_of",
]
