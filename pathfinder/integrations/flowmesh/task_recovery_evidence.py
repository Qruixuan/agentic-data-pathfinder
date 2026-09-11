from __future__ import annotations

import hashlib
import ipaddress
import math
import re
from typing import Any, Mapping
from urllib.parse import urlsplit


RESULT_UPLOAD_TIMEOUT_FAILURE_CLASS = (
    "flowmesh-result-upload-read-timeout-at-safe-schedule-root"
)
RESULT_UPLOAD_TIMEOUT_OBSERVATION_SCHEMA = (
    "pathfinder.flowmesh-task-recovery-observation/v1alpha1"
)
RESULTS_ENDPOINT_IDENTITY_SCHEME = (
    "normalized-scheme-host-effective-port-path/v1"
)

_RESULT_UPLOAD_READ_TIMEOUT = re.compile(
    r"\AFailed to deliver task (?P<task_id>\S+) result to "
    r"(?P<results_url>https?://\S+): "
    r"(?P<pool_class>HTTPS?ConnectionPool)\("
    r"host='(?P<pool_host>[^'\r\n]+)', "
    r"port=(?P<pool_port>[0-9]{1,5})\): "
    r"Read timed out\. \(read timeout=(?P<timeout>"
    r"(?:[0-9]{1,9}(?:\.[0-9]{0,9})?|\.[0-9]{1,9})"
    r"(?:[eE][+-]?[0-9]{1,3})?"
    r")\)\Z"
)
_ASCII_DNS_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
_HEX_SHA256 = re.compile(r"[0-9a-f]{64}")


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _has_forbidden_url_text(value: str) -> bool:
    return (
        not value
        or not value.isascii()
        or "\\" in value
        or "%" in value
        or ".." in value
        or any(
            ord(character) < 32 or ord(character) == 127
            for character in value
        )
    )


def _canonical_host(value: str) -> tuple[str, bool] | None:
    """Return a canonical ASCII host and whether it is IPv6.

    DNS names are deliberately restricted to lowercase wire-style labels.
    IP literals must already use the canonical spelling produced by the
    standard library.  This avoids Unicode/case-folding and alternate-IP
    equivalences in evidence that authorizes a retry.
    """

    if _has_forbidden_url_text(value) or value != value.lower():
        return None
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        if (
            len(value) > 253
            or value.startswith(".")
            or value.endswith(".")
            or ":" in value
        ):
            return None
        labels = value.split(".")
        if not labels or any(
            _ASCII_DNS_LABEL.fullmatch(label) is None for label in labels
        ):
            return None
        if len(labels) == 4 and all(label.isdigit() for label in labels):
            # A malformed IPv4 spelling must not be reinterpreted as DNS.
            return None
        return value, False
    canonical = str(address)
    if value != canonical:
        return None
    return canonical, address.version == 6


def observe_result_upload_read_timeout(
    task_id: str,
    pre_redaction_detail: str,
) -> dict[str, Any] | None:
    """Parse raw SDK task detail and return only sanitized retry evidence.

    ``pre_redaction_detail`` is the whitespace-normalized text selected from
    the SDK's error fields. It is used transiently and hashed here before being
    discarded. Neither it nor endpoint text is retained in the return value.
    The digest of a deterministic endpoint-free detail binds the sanitized
    observation to the safe task evidence the recovery client will return.
    """

    if (
        not isinstance(task_id, str)
        or not task_id
        or not isinstance(pre_redaction_detail, str)
        or len(pre_redaction_detail) > 4096
        or any(
            ord(character) < 32 or ord(character) == 127
            for character in pre_redaction_detail
        )
    ):
        return None
    match = _RESULT_UPLOAD_READ_TIMEOUT.fullmatch(pre_redaction_detail)
    if match is None or match.group("task_id") != task_id:
        return None

    results_url = match.group("results_url")
    if _has_forbidden_url_text(results_url):
        return None
    try:
        parsed = urlsplit(results_url)
        explicit_port = parsed.port
    except ValueError:
        return None
    if (
        parsed.scheme not in {"http", "https"}
        or not results_url.startswith(f"{parsed.scheme}://")
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path != "/api/v1/results"
        or parsed.query
        or parsed.fragment
    ):
        return None

    url_host = _canonical_host(parsed.hostname)
    pool_host = _canonical_host(match.group("pool_host"))
    if url_host is None or pool_host is None or url_host != pool_host:
        return None
    canonical_host, ipv6 = url_host
    authority_host = f"[{canonical_host}]" if ipv6 else canonical_host
    expected_authority = authority_host + (
        f":{explicit_port}" if explicit_port is not None else ""
    )
    if parsed.netloc != expected_authority:
        # This also rejects an empty explicit port and non-canonical authority.
        return None

    effective_port = (
        explicit_port
        if explicit_port is not None
        else (443 if parsed.scheme == "https" else 80)
    )
    pool_port = int(match.group("pool_port"))
    timeout = float(match.group("timeout"))
    expected_pool_class = (
        "HTTPSConnectionPool"
        if parsed.scheme == "https"
        else "HTTPConnectionPool"
    )
    if (
        not 0 < effective_port <= 65535
        or not 0 < pool_port <= 65535
        or match.group("pool_port") != str(pool_port)
        or pool_port != effective_port
        or match.group("pool_class") != expected_pool_class
        or not math.isfinite(timeout)
        or timeout <= 0.0
    ):
        return None

    normalized_endpoint = (
        f"{parsed.scheme}://{authority_host}:{effective_port}"
        "/api/v1/results"
    )
    redacted_detail = result_upload_timeout_redacted_detail(task_id, timeout)
    return {
        "schema_version": RESULT_UPLOAD_TIMEOUT_OBSERVATION_SCHEMA,
        "failure_class": RESULT_UPLOAD_TIMEOUT_FAILURE_CLASS,
        "stage": "worker-to-root-result-upload",
        "root_result_acknowledgement": "unknown-after-read-timeout",
        "task_id": task_id,
        "read_timeout_seconds": timeout,
        "results_endpoint_identity_scheme": RESULTS_ENDPOINT_IDENTITY_SCHEME,
        "results_endpoint_identity_sha256": _sha256(normalized_endpoint),
        "results_endpoint_evidence_source": "worker-reported-error-detail",
        "worker_result_upload_endpoint_matches_configured_root": (
            "not-verified"
        ),
        "pre_redaction_detail_sha256": _sha256(pre_redaction_detail),
        "redacted_detail_sha256": _sha256(redacted_detail),
        "pre_redaction_detail_persisted": False,
        "pre_redaction_detail_digest_offline_reconstructible": False,
    }


def result_upload_timeout_redacted_detail(
    task_id: str,
    read_timeout_seconds: float,
) -> str:
    """Return deterministic endpoint-free detail for persisted evidence."""

    return (
        f"Failed to deliver task {task_id} result to "
        "<worker-reported-results-endpoint>: read timed out after "
        f"{read_timeout_seconds!r}s"
    )


def validate_result_upload_timeout_observation(
    value: Any,
    *,
    expected_task_id: str,
    expected_redacted_detail: str,
) -> dict[str, Any] | None:
    """Validate the exact sanitized observation shape offline.

    The pre-redaction error is intentionally absent, so an offline verifier
    can bind and integrity-check its digest but cannot reconstruct it.
    """

    if not isinstance(value, Mapping):
        return None
    expected_fields = {
        "schema_version",
        "failure_class",
        "stage",
        "root_result_acknowledgement",
        "task_id",
        "read_timeout_seconds",
        "results_endpoint_identity_scheme",
        "results_endpoint_identity_sha256",
        "results_endpoint_evidence_source",
        "worker_result_upload_endpoint_matches_configured_root",
        "pre_redaction_detail_sha256",
        "redacted_detail_sha256",
        "pre_redaction_detail_persisted",
        "pre_redaction_detail_digest_offline_reconstructible",
    }
    timeout = value.get("read_timeout_seconds")
    if (
        set(value) != expected_fields
        or value.get("schema_version")
        != RESULT_UPLOAD_TIMEOUT_OBSERVATION_SCHEMA
        or value.get("failure_class") != RESULT_UPLOAD_TIMEOUT_FAILURE_CLASS
        or value.get("stage") != "worker-to-root-result-upload"
        or value.get("root_result_acknowledgement")
        != "unknown-after-read-timeout"
        or value.get("task_id") != expected_task_id
        or type(timeout) is not float
        or not math.isfinite(timeout)
        or timeout <= 0.0
        or value.get("results_endpoint_identity_scheme")
        != RESULTS_ENDPOINT_IDENTITY_SCHEME
        or _HEX_SHA256.fullmatch(
            str(value.get("results_endpoint_identity_sha256") or "")
        )
        is None
        or value.get("results_endpoint_evidence_source")
        != "worker-reported-error-detail"
        or value.get("worker_result_upload_endpoint_matches_configured_root")
        != "not-verified"
        or _HEX_SHA256.fullmatch(
            str(value.get("pre_redaction_detail_sha256") or "")
        )
        is None
        or value.get("redacted_detail_sha256")
        != _sha256(expected_redacted_detail)
        or value.get("pre_redaction_detail_persisted") is not False
        or value.get(
            "pre_redaction_detail_digest_offline_reconstructible"
        )
        is not False
    ):
        return None
    return dict(value)
