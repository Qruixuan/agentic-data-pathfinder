"""Local-only authenticated N5-to-N4 live provisioning conformance.

This module exercises the deployment boundary which the normal semantic
matrix deliberately replaces with a frozen, preprovisioned N4 snapshot.  It
can materialize either a frame bundle or a multimodal digest through the
authenticated N5 HTTP APIs and publish those exact bytes through the
authenticated, compare-and-swap N4 HTTP API.  Each portable receipt records
the N5 materialization plan as lineage separately from the N4 Data Agent
access/serving plan IDs, and binds the derived bytes and N4
atomic-publication receipt.

The receipt proves local protocol and content-lineage conformance only.  It
does not authorize the N4 ``serve-frozen`` profile, replace the separately
frozen N4 serve gate, measure materialization or publication performance, or
provide evidence about UpCloud.  Digest generation may use an external model
at runtime, but model-call authenticity and the external network path are not
observable through the current N5 digest HTTP contract and are not claimed.
"""

from __future__ import annotations

import base64
import hashlib
import ipaddress
import json
import os
import re
import shutil
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Protocol

from ..frame_bundle_ingest import FRAME_BUNDLE_MEDIA_TYPE
from .n4_derived_data_plane import (
    FRAME_BUNDLE_REPRESENTATION_ID,
    N4ArtifactProvenance,
    verify_n4_publication_receipt,
)
from .n4_publication_http import (
    N4_PUBLICATION_HTTP_API_VERSION,
    N4_PUBLICATION_HTTP_REQUEST_SCHEMA_VERSION,
    N4_PUBLICATION_HTTP_RESULT_SCHEMA_VERSION,
)
from .n5_materialization import (
    HttpN5MaterializationClient,
    N5MaterializationHttpClientConfig,
    N5_MATERIALIZATION_EVIDENCE_SCHEMA_VERSION,
    N5_MATERIALIZATION_HTTP_RESULT_SCHEMA_VERSION,
    N5_SOURCE_MEDIA_TYPE,
    verify_n5_materialization_plan,
)
from .n5_digest_http import (
    N5_DIGEST_HTTP_API_VERSION,
    N5_DIGEST_HTTP_EXECUTE_SCHEMA_VERSION,
    N5_DIGEST_HTTP_RESULT_SCHEMA_VERSION,
)
from .n5_digest_materialization import (
    MEDIA_TYPE as MULTIMODAL_DIGEST_MEDIA_TYPE,
    PLAN_NAME as N5_DIGEST_PLAN_NAME,
    REPRESENTATION_ID as MULTIMODAL_DIGEST_REPRESENTATION_ID,
    SOURCE_REPRESENTATION_ID as DIGEST_SOURCE_REPRESENTATION_ID,
    verify_n5_multimodal_digest_plan,
)


_LEGACY_LIVE_PROVISIONING_SMOKE_SCHEMA_VERSION = (
    "pathfinder.local-n5-n4-live-provisioning-smoke/v1alpha1"
)
LIVE_PROVISIONING_SMOKE_SCHEMA_VERSION = (
    "pathfinder.local-n5-n4-live-provisioning-smoke/v1alpha2"
)
_LEGACY_LIVE_DIGEST_PROVISIONING_SMOKE_SCHEMA_VERSION = (
    "pathfinder.local-n5-n4-live-digest-provisioning-smoke/v1alpha1"
)
LIVE_DIGEST_PROVISIONING_SMOKE_SCHEMA_VERSION = (
    "pathfinder.local-n5-n4-live-digest-provisioning-smoke/v1alpha2"
)
N5_DIGEST_TRANSPORT_RECEIPT_SCHEMA_VERSION = (
    "pathfinder.local-n5-digest-http-transport-receipt/v1alpha1"
)
RECEIPT_NAME = "local-n5-n4-live-provisioning-receipt.json"
DIGEST_RECEIPT_NAME = "local-n5-n4-live-digest-provisioning-receipt.json"
CHECKSUMS_NAME = "SHA256SUMS"

_FILES = {RECEIPT_NAME, CHECKSUMS_NAME}
_DIGEST_FILES = {DIGEST_RECEIPT_NAME, CHECKSUMS_NAME}
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._|:+-]{0,255}\Z")
_PRIVATE_HOST = re.compile(
    r"pathfinder-sim-[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z"
)
_SENSITIVE_KEY = re.compile(
    r"(?:^|_)(?:api_?key|bearer|password|secret|token)(?:$|_)",
    re.IGNORECASE,
)

_LEGACY_RECEIPT_FIELDS = {
    "schema_version",
    "status",
    "evidence_class",
    "smoke_id",
    "representation_id",
    "source_representation_id",
    "object_id",
    "n5_plan_id",
    "n5_plan_sha256",
    "n5_transformation_contract_sha256",
    "n5_runtime_epoch",
    "n5_materialization_evidence",
    "n5_materialization_evidence_sha256",
    "n5_transport_receipt",
    "n5_transport_receipt_sha256",
    "n5_fresh_materialization_executed",
    "n5_materialization_idempotent_replay",
    "n5_http_authentication_observed",
    "artifact_size_bytes",
    "artifact_sha256",
    "n4_publication_id",
    "n4_previous_catalog_version",
    "n4_committed_catalog_version",
    "n4_generation_id",
    "n4_package_sha256",
    "n4_publication_receipt",
    "n4_http_authentication_observed",
    "n4_fresh_publication_executed",
    "n4_publication_idempotent_replay",
    "n4_compare_and_swap_verified",
    "n4_atomic_visibility_verified",
    "n5_to_n4_content_binding_verified",
    "local_http_service_boundaries_exercised",
    "local_live_provisioning_conformance_verified",
    "fresh_end_to_end_execution_observed",
    "durable_replay_adopted",
    "semantic_trial_serve_gate_replaced",
    "preprovisioned_serve_gate_still_required",
    "n4_data_agent_rebind_required",
    "multimodal_digest_live_provisioning_verified",
    "materialization_latency_measured",
    "publication_latency_measured",
    "monetary_cost_measured",
    "cloud_network_measured",
    "upcloud_used",
    "external_network_called",
    "source_services_cryptographically_attested",
    "credentials_recorded",
    "eligible_for_scientific_claims",
    "receipt_sha256",
}
_RECEIPT_FIELDS = _LEGACY_RECEIPT_FIELDS | {
    "n4_access_plan_ids",
    "n4_access_plan_ids_source",
    "n4_package_id",
    "n4_publication_request_sha256",
}

_LEGACY_DIGEST_RECEIPT_FIELDS = {
    "schema_version",
    "status",
    "evidence_class",
    "smoke_id",
    "representation_id",
    "source_representation_id",
    "object_id",
    "n5_digest_plan_id",
    "n5_digest_plan_sha256",
    "n5_digest_model_id",
    "n5_digest_sampling_metadata_sha256",
    "n5_digest_result",
    "n5_digest_result_sha256",
    "n5_digest_transport_receipt",
    "n5_digest_transport_receipt_sha256",
    "n5_fresh_digest_materialization_executed",
    "n5_digest_materialization_idempotent_replay",
    "n5_semantic_model_call_attested",
    "n5_http_authentication_observed",
    "artifact_size_bytes",
    "artifact_sha256",
    "n4_publication_id",
    "n4_previous_catalog_version",
    "n4_committed_catalog_version",
    "n4_generation_id",
    "n4_package_sha256",
    "n4_publication_receipt",
    "n4_http_authentication_observed",
    "n4_fresh_publication_executed",
    "n4_publication_idempotent_replay",
    "n4_compare_and_swap_verified",
    "n4_atomic_visibility_verified",
    "n5_to_n4_content_binding_verified",
    "local_http_service_boundaries_exercised",
    "local_live_digest_provisioning_conformance_verified",
    "fresh_end_to_end_execution_observed",
    "durable_replay_adopted",
    "semantic_trial_serve_gate_replaced",
    "preprovisioned_serve_gate_still_required",
    "n4_data_agent_rebind_required",
    "frame_bundle_live_provisioning_verified_by_this_receipt",
    "materialization_latency_measured",
    "publication_latency_measured",
    "monetary_cost_measured",
    "cloud_network_measured",
    "upcloud_used",
    "external_network_call_status",
    "semantic_model_call_authenticity_verified",
    "source_services_cryptographically_attested",
    "credentials_recorded",
    "eligible_for_scientific_claims",
    "receipt_sha256",
}
_DIGEST_RECEIPT_FIELDS = _LEGACY_DIGEST_RECEIPT_FIELDS | {
    "n4_access_plan_ids",
    "n4_access_plan_ids_source",
    "n4_package_id",
    "n4_publication_request_sha256",
}

_EXPLICIT_N4_ACCESS_PLAN_IDS = "explicit"
_DEFAULT_N4_ACCESS_PLAN_IDS = "n5-materialization-plan-default"
_LEGACY_INFERRED_N4_ACCESS_PLAN_IDS = "legacy-schema-inference"

_N5_DIGEST_RESULT_FIELDS = {
    "schema_version",
    "status",
    "request_id",
    "plan_id",
    "object_id",
    "source_handle",
    "result_handle",
    "artifact_sha256",
    "artifact_size_bytes",
    "model_id",
    "llm_called",
    "idempotent_replay",
    "credentials_recorded",
}

_N5_DIGEST_TRANSPORT_FIELDS = {
    "schema_version",
    "status",
    "node_id",
    "registered_plan_count",
    "request_id",
    "plan_id",
    "plan_sha256",
    "source_handle",
    "result_handle",
    "artifact_size_bytes",
    "artifact_sha256",
    "model_id",
    "source_stage_idempotent_replay",
    "materialization_idempotent_replay",
    "source_content_binding_verified",
    "result_content_binding_verified",
    "plan_registry_stable_verified",
    "semantic_model_call_attested",
    "http_authentication_observed",
    "redirects_followed",
    "ambient_proxies_used",
    "credentials_recorded",
}


class FullFlowLiveProvisioningSmokeError(RuntimeError):
    """Raised when the local N5-to-N4 conformance chain is not trustworthy."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowLiveProvisioningSmokeError(message)


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FullFlowLiveProvisioningSmokeError(
            "live provisioning value is not canonical JSON"
        ) from exc


def _json_bytes(value: Any) -> bytes:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                indent=2,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FullFlowLiveProvisioningSmokeError(
            "live provisioning value is not JSON"
        ) from exc


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _digest(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{name} is not lowercase SHA-256",
    )
    return str(value)


def _identifier(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
        f"{name} is invalid",
    )
    return str(value)


def _canonical_n4_access_plan_ids(value: Any) -> list[str]:
    _require(
        isinstance(value, Sequence)
        and not isinstance(value, (str, bytes))
        and bool(value),
        "n4_access_plan_ids must be a non-empty sequence",
    )
    checked = [
        _identifier(item, "n4_access_plan_id") for item in value
    ]
    _require(
        len(checked) == len(set(checked)),
        "n4_access_plan_ids contain a duplicate",
    )
    return sorted(checked)


def _n4_access_plan_contract(
    value: Sequence[str] | None,
    *,
    n5_materialization_plan_id: str,
    expected_current_catalog_version: str | None,
) -> tuple[list[str], str]:
    """Resolve the explicit serving contract without confusing lineage.

    The historical one-object smoke used the N5 materialization plan ID as
    the N4 Data Agent access plan ID.  Preserve that default for diagnostic
    single-object runs, but label it explicitly: it is not sufficient for a
    formal live-serve gate.  Formal and multi-object callers supply the
    representation-wide access contract explicitly.
    """

    materialization_plan_id = _identifier(
        n5_materialization_plan_id,
        "n5_materialization_plan_id",
    )
    if value is None:
        if expected_current_catalog_version is not None:
            _identifier(
                expected_current_catalog_version,
                "expected_current_catalog_version",
            )
            raise FullFlowLiveProvisioningSmokeError(
                "cumulative N4 publication requires explicit "
                "n4_access_plan_ids before N5 materialization; the N5 "
                "materialization plan ID is provenance only"
            )
        return [materialization_plan_id], _DEFAULT_N4_ACCESS_PLAN_IDS
    return _canonical_n4_access_plan_ids(value), _EXPLICIT_N4_ACCESS_PLAN_IDS


def _recorded_n4_access_plan_contract(
    document: Mapping[str, Any],
    *,
    legacy_schema_version: str,
    n5_materialization_plan_id: str,
) -> tuple[list[str], str]:
    if document.get("schema_version") == legacy_schema_version:
        return (
            [_identifier(
                n5_materialization_plan_id,
                "n5_materialization_plan_id",
            )],
            _LEGACY_INFERRED_N4_ACCESS_PLAN_IDS,
        )
    plan_ids = _canonical_n4_access_plan_ids(
        document.get("n4_access_plan_ids")
    )
    _require(
        plan_ids == document.get("n4_access_plan_ids"),
        "recorded n4_access_plan_ids are not canonical",
    )
    source = document.get("n4_access_plan_ids_source")
    _require(
        source in {
            _EXPLICIT_N4_ACCESS_PLAN_IDS,
            _DEFAULT_N4_ACCESS_PLAN_IDS,
        },
        "n4_access_plan_ids_source is invalid",
    )
    if source == _DEFAULT_N4_ACCESS_PLAN_IDS:
        _require(
            plan_ids == [n5_materialization_plan_id],
            "the N5-plan default does not match materialization lineage",
        )
    return plan_ids, str(source)


def _n4_publication_request_commitment(
    *,
    publication_id: Any,
    package_id: Any,
    catalog_version: Any,
    expected_current_catalog_version: Any,
    object_id: Any,
    representation_id: Any,
    artifact_size_bytes: Any,
    artifact_sha256: Any,
    n4_access_plan_ids: Sequence[str],
    provenance: Mapping[str, Any],
) -> dict[str, Any]:
    """Rebuild the exact endpoint-free request hashed by the N4 store."""

    previous = expected_current_catalog_version
    if previous is not None:
        previous = _identifier(previous, "expected_current_catalog_version")
    _require(
        type(artifact_size_bytes) is int and artifact_size_bytes > 0,
        "N4 artifact_size_bytes must be a positive integer",
    )
    try:
        checked_provenance = N4ArtifactProvenance.from_dict(
            provenance
        ).to_dict()
    except Exception as exc:
        raise FullFlowLiveProvisioningSmokeError(
            "N4 artifact provenance is invalid"
        ) from exc
    return {
        "publication_id": _identifier(publication_id, "publication_id"),
        "package_id": _identifier(package_id, "package_id"),
        "catalog_version": _identifier(
            catalog_version,
            "catalog_version",
        ),
        "expected_current_catalog_version": previous,
        "artifacts": [{
            "object_id": _identifier(object_id, "object_id"),
            "representation_id": _identifier(
                representation_id,
                "representation_id",
            ),
            "artifact_size_bytes": artifact_size_bytes,
            "artifact_sha256": _digest(
                artifact_sha256,
                "artifact_sha256",
            ),
            "plan_ids": _canonical_n4_access_plan_ids(
                n4_access_plan_ids
            ),
            "provenance": checked_provenance,
        }],
    }


def _verify_n4_publication_request_commitment(
    document: Mapping[str, Any],
    n4_receipt: Mapping[str, Any],
    *,
    schema_version: str,
    legacy_schema_version: str,
    object_id: str,
    representation_id: str,
    artifact_size_bytes: int,
    artifact_sha256: str,
    n4_access_plan_ids: Sequence[str],
    provenance: Mapping[str, Any],
) -> None:
    if schema_version == legacy_schema_version:
        return
    commitment = _n4_publication_request_commitment(
        publication_id=document.get("n4_publication_id"),
        package_id=document.get("n4_package_id"),
        catalog_version=document.get("n4_committed_catalog_version"),
        expected_current_catalog_version=document.get(
            "n4_previous_catalog_version"
        ),
        object_id=object_id,
        representation_id=representation_id,
        artifact_size_bytes=artifact_size_bytes,
        artifact_sha256=artifact_sha256,
        n4_access_plan_ids=n4_access_plan_ids,
        provenance=provenance,
    )
    commitment_sha256 = _sha256(_canonical(commitment))
    _require(
        _digest(
            document.get("n4_publication_request_sha256"),
            "n4_publication_request_sha256",
        )
        == commitment_sha256
        == n4_receipt.get("request_sha256"),
        "recorded N4 access plans do not match the publication request",
    )


def _strict_json(payload: bytes, name: str) -> dict[str, Any]:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            _require(key not in result, f"{name} repeats key {key}")
            result[key] = value
        return result

    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=unique,
            parse_constant=lambda token: (_ for _ in ()).throw(
                FullFlowLiveProvisioningSmokeError(
                    f"{name} contains invalid number {token}"
                )
            ),
        )
    except FullFlowLiveProvisioningSmokeError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowLiveProvisioningSmokeError(
            f"cannot parse {name}"
        ) from exc
    _require(isinstance(value, dict), f"{name} must be an object")
    return value


def _is_loopback(hostname: str | None) -> bool:
    if hostname is None:
        return False
    if hostname.casefold() == "localhost":
        return True
    try:
        return ipaddress.ip_address(hostname).is_loopback
    except ValueError:
        return False


def _local_origin(value: str, private_hosts: Sequence[str]) -> str:
    _require(isinstance(value, str) and value == value.strip(), "invalid URL")
    parsed = urllib.parse.urlsplit(value)
    _require(
        parsed.scheme == "http"
        and parsed.hostname is not None
        and parsed.username is None
        and parsed.password is None
        and parsed.path in {"", "/"}
        and not parsed.query
        and not parsed.fragment,
        "local provisioning origin must be a credential-free HTTP origin",
    )
    try:
        port = parsed.port
    except ValueError as exc:
        raise FullFlowLiveProvisioningSmokeError(
            "local provisioning origin port is invalid"
        ) from exc
    _require(port is not None, "local provisioning HTTP origin needs a port")
    hostname = str(parsed.hostname).casefold()
    allowed = tuple(str(item).casefold() for item in private_hosts)
    _require(
        len(allowed) == len(set(allowed))
        and all(_PRIVATE_HOST.fullmatch(item) is not None for item in allowed),
        "simulator private HTTP hosts are invalid",
    )
    _require(
        _is_loopback(hostname) or hostname in allowed,
        "live provisioning smoke is restricted to loopback or an explicitly "
        "bound pathfinder-sim-* host",
    )
    return value.rstrip("/")


@dataclass(frozen=True)
class N4PublicationHttpClientConfig:
    """Runtime-only N4 endpoint and bearer credential."""

    base_url: str = field(repr=False)
    bearer_token: str = field(repr=False, compare=False)
    simulator_private_http_hosts: tuple[str, ...] = ()
    timeout_seconds: float = 300.0
    max_json_bytes: int = 96 * 1024 * 1024

    def __post_init__(self) -> None:
        _local_origin(self.base_url, self.simulator_private_http_hosts)
        _require(
            isinstance(self.bearer_token, str)
            and self.bearer_token == self.bearer_token.strip()
            and 1 <= len(self.bearer_token.encode("utf-8")) <= 8192
            and all(item not in self.bearer_token for item in "\r\n\x00"),
            "N4 bearer token is invalid",
        )
        _require(
            isinstance(self.timeout_seconds, (int, float))
            and not isinstance(self.timeout_seconds, bool)
            and float(self.timeout_seconds) > 0,
            "N4 timeout is invalid",
        )
        _require(
            type(self.max_json_bytes) is int and self.max_json_bytes > 0,
            "N4 JSON byte limit is invalid",
        )


class _RejectRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        request: Any,
        file_pointer: Any,
        code: int,
        message: str,
        headers: Any,
        new_url: str,
    ) -> None:
        del request, file_pointer, code, message, headers, new_url
        return None


class HttpN4PublicationClient:
    """Proxy-free, redirect-free client for one exact local N4 origin."""

    def __init__(self, config: N4PublicationHttpClientConfig) -> None:
        _require(
            isinstance(config, N4PublicationHttpClientConfig),
            "N4 client config is invalid",
        )
        self._config = config
        self._base_url = _local_origin(
            config.base_url,
            config.simulator_private_http_hosts,
        )
        self._authorization = "Bearer " + config.bearer_token
        self._open = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            _RejectRedirects(),
        ).open

    def _request(
        self,
        path: str,
        *,
        method: str,
        value: Mapping[str, Any] | None = None,
        authenticated: bool,
    ) -> tuple[int, dict[str, Any]]:
        _require(path.startswith("/") and "://" not in path, "unsafe N4 path")
        body = None if value is None else _canonical(value)
        if body is not None:
            _require(
                len(body) <= self._config.max_json_bytes,
                "N4 request exceeds its JSON byte limit",
            )
        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "User-Agent": "pathfinder-live-provisioning/1",
        }
        if body is not None:
            headers["Content-Type"] = "application/json"
        if authenticated:
            headers["Authorization"] = self._authorization
        request = urllib.request.Request(
            self._base_url + path,
            method=method,
            data=body,
            headers=headers,
        )
        try:
            with self._open(
                request,
                timeout=float(self._config.timeout_seconds),
            ) as response:
                lengths = response.headers.get_all("Content-Length") or []
                _require(
                    len(lengths) == 1
                    and re.fullmatch(r"[0-9]+", lengths[0]) is not None,
                    "N4 response lacks one valid Content-Length",
                )
                length = int(lengths[0])
                _require(
                    length <= self._config.max_json_bytes,
                    "N4 response exceeds its JSON byte limit",
                )
                _require(
                    response.headers.get_content_type() == "application/json"
                    and response.headers.get("Content-Encoding")
                    in {None, "identity"},
                    "N4 response media encoding changed",
                )
                payload = response.read(self._config.max_json_bytes + 1)
                _require(
                    len(payload) == length,
                    "N4 response length binding failed",
                )
                _require(
                    self._config.bearer_token.encode("utf-8") not in payload,
                    "N4 response contains configured credential material",
                )
                value = _strict_json(payload, "N4 response")
                _require(
                    payload == _canonical(value),
                    "N4 response is not canonical JSON",
                )
                return response.status, value
        except urllib.error.HTTPError as exc:
            raise FullFlowLiveProvisioningSmokeError(
                f"N4 endpoint returned HTTP {exc.code}"
            ) from exc
        except (urllib.error.URLError, OSError) as exc:
            raise FullFlowLiveProvisioningSmokeError(
                "N4 endpoint is unreachable"
            ) from exc

    def health(self) -> dict[str, Any]:
        status, value = self._request(
            "/healthz",
            method="GET",
            authenticated=False,
        )
        _require(status == 200, "N4 health status changed")
        _require(
            set(value)
            == {
                "api_version",
                "status",
                "node_id",
                "atomic_publication",
                "current_generation_present",
                "credentials_recorded",
            }
            and value.get("api_version") == N4_PUBLICATION_HTTP_API_VERSION
            and value.get("status") == "ok"
            and value.get("node_id") == "N4"
            and value.get("atomic_publication") is True
            and type(value.get("current_generation_present")) is bool
            and value.get("credentials_recorded") is False,
            "N4 health identity or capability changed",
        )
        return value

    def publish(self, request: Mapping[str, Any]) -> dict[str, Any]:
        status, value = self._request(
            "/v1/publications",
            method="POST",
            value=request,
            authenticated=True,
        )
        _require(status in {200, 201}, "N4 publication status changed")
        _require(
            set(value)
            == {
                "schema_version",
                "status",
                "publication_id",
                "generation_id",
                "package_sha256",
                "catalog_version",
                "receipt",
                "idempotent_replay",
                "data_agent_reload_required",
                "credentials_recorded",
            },
            "N4 publication response fields changed",
        )
        receipt = verify_n4_publication_receipt(value.get("receipt"))
        _require(
            value.get("schema_version")
            == N4_PUBLICATION_HTTP_RESULT_SCHEMA_VERSION
            and value.get("status") == "COMMITTED"
            and value.get("publication_id") == request.get("publication_id")
            and value.get("generation_id") == receipt["generation_id"]
            and value.get("package_sha256") == receipt["package_sha256"]
            and value.get("catalog_version")
            == receipt["committed_catalog_version"]
            and type(value.get("idempotent_replay")) is bool
            and value.get("data_agent_reload_required") is True
            and value.get("credentials_recorded") is False,
            "N4 publication response binding failed",
        )
        artifacts = request.get("artifacts")
        _require(
            receipt["previous_catalog_version"]
            == request.get("expected_current_catalog_version")
            and receipt["committed_catalog_version"]
            == request.get("catalog_version")
            and isinstance(artifacts, list)
            and len(artifacts) == 1
            and receipt["published_artifacts"]
            == [
                {
                    key: artifacts[0][key]
                    for key in (
                        "object_id",
                        "representation_id",
                        "artifact_size_bytes",
                        "artifact_sha256",
                    )
                }
            ],
            "N4 publication receipt differs from the submitted request",
        )
        _require(
            status == (200 if value["idempotent_replay"] else 201),
            "N4 publication replay status is inconsistent",
        )
        return value


@dataclass(frozen=True)
class N5DigestHttpExecution:
    """Safe result of one authenticated N5 digest HTTP exchange."""

    result: dict[str, Any]
    transport_receipt: dict[str, Any]
    artifact_bytes: bytes = field(repr=False)


class N5DigestExecutor(Protocol):
    """Runtime seam used by the digest provisioning smoke.

    Production uses :class:`HttpN5DigestMaterializationExecutor`.  Tests may
    inject an executor backed by an offline vision adapter; credentials and
    the actual model call remain runtime concerns rather than frozen inputs.
    """

    def execute(
        self,
        plan_dir: Path,
        source_video_path: Path,
        *,
        request_id: str,
    ) -> N5DigestHttpExecution:
        """Return exact digest bytes plus validated HTTP observations."""


@dataclass(frozen=True)
class N5DigestHttpClientConfig:
    """Runtime-only local N5 digest endpoint and bearer credential."""

    base_url: str = field(repr=False)
    bearer_token: str = field(repr=False, compare=False)
    simulator_private_http_hosts: tuple[str, ...] = ()
    timeout_seconds: float = 300.0
    max_json_bytes: int = 1024 * 1024
    max_source_bytes: int = 512 * 1024 * 1024
    max_result_bytes: int = 1024 * 1024

    def __post_init__(self) -> None:
        _local_origin(self.base_url, self.simulator_private_http_hosts)
        _require(
            isinstance(self.bearer_token, str)
            and self.bearer_token == self.bearer_token.strip()
            and 1 <= len(self.bearer_token.encode("utf-8")) <= 8192
            and all(item not in self.bearer_token for item in "\r\n\x00"),
            "N5 digest bearer token is invalid",
        )
        _require(
            isinstance(self.timeout_seconds, (int, float))
            and not isinstance(self.timeout_seconds, bool)
            and float(self.timeout_seconds) > 0,
            "N5 digest timeout is invalid",
        )
        for name in ("max_json_bytes", "max_source_bytes", "max_result_bytes"):
            _require(
                type(getattr(self, name)) is int and getattr(self, name) > 0,
                f"N5 digest {name} is invalid",
            )


class HttpN5DigestMaterializationExecutor:
    """Strict proxy-free client for the existing N5 digest HTTP service."""

    def __init__(self, config: N5DigestHttpClientConfig) -> None:
        _require(
            isinstance(config, N5DigestHttpClientConfig),
            "N5 digest client config is invalid",
        )
        self._config = config
        self._base_url = _local_origin(
            config.base_url,
            config.simulator_private_http_hosts,
        )
        self._authorization = "Bearer " + config.bearer_token
        self._open = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            _RejectRedirects(),
        ).open

    def _request(
        self,
        path: str,
        *,
        method: str,
        body: bytes | None,
        content_type: str | None,
        authenticated: bool,
        maximum: int,
        content_sha256: str | None = None,
    ) -> tuple[int, bytes, Any]:
        _require(path.startswith("/") and "://" not in path, "unsafe N5 path")
        headers = {
            "Accept-Encoding": "identity",
            "User-Agent": "pathfinder-live-digest-provisioning/1",
        }
        if content_type is not None:
            headers["Content-Type"] = content_type
        if authenticated:
            headers["Authorization"] = self._authorization
        if content_sha256 is not None:
            headers["X-Pathfinder-Content-SHA256"] = content_sha256
        request = urllib.request.Request(
            self._base_url + path,
            method=method,
            data=body,
            headers=headers,
        )
        try:
            with self._open(
                request,
                timeout=float(self._config.timeout_seconds),
            ) as response:
                lengths = response.headers.get_all("Content-Length") or []
                _require(
                    len(lengths) == 1
                    and re.fullmatch(r"[0-9]+", lengths[0]) is not None,
                    "N5 digest response lacks one valid Content-Length",
                )
                length = int(lengths[0])
                _require(
                    length <= maximum
                    and response.headers.get("Content-Encoding")
                    in {None, "identity"},
                    "N5 digest response size or encoding changed",
                )
                payload = response.read(maximum + 1)
                _require(
                    len(payload) == length,
                    "N5 digest response length binding failed",
                )
                _require(
                    self._config.bearer_token.encode("utf-8") not in payload,
                    "N5 digest response contains configured credential material",
                )
                return response.status, payload, response.headers
        except urllib.error.HTTPError as exc:
            raise FullFlowLiveProvisioningSmokeError(
                f"N5 digest endpoint returned HTTP {exc.code}"
            ) from exc
        except (urllib.error.URLError, OSError) as exc:
            raise FullFlowLiveProvisioningSmokeError(
                "N5 digest endpoint is unreachable"
            ) from exc

    def _json_request(
        self,
        path: str,
        *,
        method: str,
        value: Mapping[str, Any] | None = None,
        authenticated: bool,
    ) -> tuple[int, dict[str, Any]]:
        body = None if value is None else _canonical(value)
        _require(
            body is None or len(body) <= self._config.max_json_bytes,
            "N5 digest request exceeds its JSON byte limit",
        )
        status, payload, headers = self._request(
            path,
            method=method,
            body=body,
            content_type="application/json" if body is not None else None,
            authenticated=authenticated,
            maximum=self._config.max_json_bytes,
        )
        _require(
            headers.get_content_type() == "application/json",
            "N5 digest endpoint returned non-JSON content",
        )
        value = _strict_json(payload, "N5 digest response")
        _require(
            payload == _canonical(value),
            "N5 digest response is not canonical JSON",
        )
        return status, value

    def health(self) -> dict[str, Any]:
        status, value = self._json_request(
            "/healthz",
            method="GET",
            authenticated=False,
        )
        _require(
            status == 200
            and set(value)
            == {
                "api_version",
                "status",
                "node_id",
                "registered_plan_count",
                "semantic_digest_execution",
                "credentials_recorded",
            }
            and value.get("api_version") == N5_DIGEST_HTTP_API_VERSION
            and value.get("status") == "ok"
            and value.get("node_id") == "N5"
            and type(value.get("registered_plan_count")) is int
            and value["registered_plan_count"] > 0
            and value.get("semantic_digest_execution") is True
            and value.get("credentials_recorded") is False,
            "N5 digest health identity or capability changed",
        )
        return value

    def execute(
        self,
        plan_dir: Path,
        source_video_path: Path,
        *,
        request_id: str,
    ) -> N5DigestHttpExecution:
        verified = verify_n5_multimodal_digest_plan(
            plan_dir,
            source_video_path,
        )
        request_id = _identifier(request_id, "N5 digest request_id")
        try:
            source = source_video_path.read_bytes()
        except OSError as exc:
            raise FullFlowLiveProvisioningSmokeError(
                "cannot read the plan-bound N5 digest source"
            ) from exc
        _require(
            0 < len(source) <= self._config.max_source_bytes
            and len(source) >= 16
            and _sha256(source) == verified["source_video_sha256"],
            "N5 digest source exceeds its limit or changed",
        )
        before = self.health()
        handle = verified["source_video_sha256"]
        stage_status, stage_payload, stage_headers = self._request(
            f"/v1/digest-inputs/{handle}",
            method="PUT",
            body=source,
            content_type="video/mp4",
            authenticated=True,
            maximum=self._config.max_json_bytes,
            content_sha256=handle,
        )
        _require(
            stage_headers.get_content_type() == "application/json",
            "N5 digest stage returned non-JSON content",
        )
        stage = _strict_json(stage_payload, "N5 digest stage response")
        _require(
            stage_payload == _canonical(stage)
            and stage_status in {200, 201}
            and set(stage)
            == {
                "status",
                "source_handle",
                "source_size_bytes",
                "idempotent_replay",
                "credentials_recorded",
            }
            and stage.get("status") == "STAGED"
            and stage.get("source_handle") == handle
            and stage.get("source_size_bytes") == len(source)
            and type(stage.get("idempotent_replay")) is bool
            and stage.get("credentials_recorded") is False
            and stage_status == (200 if stage["idempotent_replay"] else 201),
            "N5 digest stage response binding failed",
        )
        request = {
            "schema_version": N5_DIGEST_HTTP_EXECUTE_SCHEMA_VERSION,
            "request_id": request_id,
            "plan_id": verified["plan_id"],
            "source_handle": handle,
        }
        status, result = self._json_request(
            "/v1/digest-materializations/execute",
            method="POST",
            value=request,
            authenticated=True,
        )
        _require(
            status == 200
            and set(result) == _N5_DIGEST_RESULT_FIELDS
            and result.get("schema_version")
            == N5_DIGEST_HTTP_RESULT_SCHEMA_VERSION
            and result.get("status") == "COMPLETE"
            and result.get("request_id") == request_id
            and result.get("result_handle") == request_id
            and result.get("plan_id") == verified["plan_id"]
            and result.get("object_id") == verified["object_id"]
            and result.get("source_handle") == handle
            and result.get("model_id") == verified["model_id"]
            and result.get("llm_called") is True
            and type(result.get("idempotent_replay")) is bool
            and result.get("credentials_recorded") is False,
            "N5 digest execution response binding failed",
        )
        size = result.get("artifact_size_bytes")
        digest = result.get("artifact_sha256")
        _require(
            type(size) is int and 0 < size <= self._config.max_result_bytes,
            "N5 digest result size is invalid",
        )
        _digest(digest, "N5 digest artifact_sha256")
        result_status, artifact, result_headers = self._request(
            f"/v1/digest-results/{request_id}",
            method="GET",
            body=None,
            content_type=None,
            authenticated=True,
            maximum=self._config.max_result_bytes,
        )
        response_digests = (
            result_headers.get_all("X-Pathfinder-Content-SHA256") or []
        )
        _require(
            result_status == 200
            and result_headers.get_content_type() == "text/plain"
            and result_headers.get_content_charset() == "utf-8"
            and response_digests == [digest]
            and len(artifact) == size
            and _sha256(artifact) == digest,
            "N5 digest artifact content binding failed",
        )
        after = self.health()
        _require(
            before == after,
            "N5 digest plan registry changed during execution",
        )
        transport = {
            "schema_version": N5_DIGEST_TRANSPORT_RECEIPT_SCHEMA_VERSION,
            "status": "VERIFIED",
            "node_id": "N5",
            "registered_plan_count": before["registered_plan_count"],
            "request_id": request_id,
            "plan_id": verified["plan_id"],
            "plan_sha256": verified["plan_sha256"],
            "source_handle": handle,
            "result_handle": request_id,
            "artifact_size_bytes": size,
            "artifact_sha256": digest,
            "model_id": verified["model_id"],
            "source_stage_idempotent_replay": stage["idempotent_replay"],
            "materialization_idempotent_replay": result[
                "idempotent_replay"
            ],
            "source_content_binding_verified": True,
            "result_content_binding_verified": True,
            "plan_registry_stable_verified": True,
            "semantic_model_call_attested": True,
            "http_authentication_observed": True,
            "redirects_followed": False,
            "ambient_proxies_used": False,
            "credentials_recorded": False,
        }
        return N5DigestHttpExecution(
            result=result,
            transport_receipt=transport,
            artifact_bytes=artifact,
        )


def _assert_public(value: Any, name: str = "receipt") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            _require(isinstance(key, str), f"{name} has a non-string key")
            if key != "credentials_recorded":
                _require(
                    _SENSITIVE_KEY.search(key) is None,
                    f"{name} contains a credential-shaped field",
                )
            _assert_public(child, f"{name}.{key}")
        return
    if isinstance(value, list):
        for index, child in enumerate(value):
            _assert_public(child, f"{name}[{index}]")
        return
    if isinstance(value, str):
        _require("://" not in value, f"{name} contains an endpoint")
        posix = PurePosixPath(value)
        windows = PureWindowsPath(value)
        _require(
            not posix.is_absolute()
            and not windows.is_absolute()
            and not windows.drive,
            f"{name} contains an absolute path",
        )


def _publication_request(
    *,
    plan: Mapping[str, Any],
    artifact: bytes,
    n4_access_plan_ids: Sequence[str],
    publication_id: str,
    package_id: str,
    catalog_version: str,
    expected_current_catalog_version: str | None,
) -> dict[str, Any]:
    provenance = N4ArtifactProvenance(
        producer_node_id="N5",
        publication_source_id=str(plan["idempotency_key"]),
        source_representation_id="raw_video",
        source_content_sha256=str(plan["input"]["sha256"]),
        derivation_id="n5-uniform-midpoint-frame-bundle-v1",
        derivation_sha256=str(plan["transformation_contract_sha256"]),
    )
    return {
        "schema_version": N4_PUBLICATION_HTTP_REQUEST_SCHEMA_VERSION,
        "publication_id": _identifier(publication_id, "publication_id"),
        "package_id": _identifier(package_id, "package_id"),
        "catalog_version": _identifier(catalog_version, "catalog_version"),
        "expected_current_catalog_version": (
            None
            if expected_current_catalog_version is None
            else _identifier(
                expected_current_catalog_version,
                "expected_current_catalog_version",
            )
        ),
        "artifacts": [
            {
                "object_id": plan["input"]["object_id"],
                "representation_id": FRAME_BUNDLE_REPRESENTATION_ID,
                "artifact_base64": base64.b64encode(artifact).decode("ascii"),
                "artifact_sha256": _sha256(artifact),
                "artifact_size_bytes": len(artifact),
                "plan_ids": list(n4_access_plan_ids),
                "provenance": provenance.to_dict(),
            }
        ],
    }


def _receipt_document(
    *,
    smoke_id: str,
    plan: Mapping[str, Any],
    n5_execution: Any,
    n4_request: Mapping[str, Any],
    n4_access_plan_ids: Sequence[str],
    n4_access_plan_ids_source: str,
    n4_before: Mapping[str, Any],
    n4_result: Mapping[str, Any],
    n4_after: Mapping[str, Any],
) -> dict[str, Any]:
    n5_evidence = n5_execution.evidence
    n5_transport = n5_execution.transport_receipt
    n4_receipt = verify_n4_publication_receipt(n4_result["receipt"])
    expected = plan["expected_output"]
    _require(
        n5_evidence.get("schema_version")
        == N5_MATERIALIZATION_EVIDENCE_SCHEMA_VERSION
        and n5_evidence.get("plan_sha256") == plan["plan_sha256"]
        and n5_evidence.get("output") == expected
        and type(n5_evidence.get("idempotent_replay")) is bool
        and n5_evidence.get("input_content_binding_verified") is True
        and n5_evidence.get("output_binding_verified") is True
        and n5_evidence.get("canonical_frame_bundle_verified") is True
        and n5_evidence.get("credentials_recorded") is False,
        "N5 materialization evidence is incomplete",
    )
    _require(
        n5_transport.get("schema_version")
        == N5_MATERIALIZATION_HTTP_RESULT_SCHEMA_VERSION
        and n5_transport.get("status") == "VERIFIED"
        and n5_transport.get("plan_sha256") == plan["plan_sha256"]
        and n5_transport.get("artifact_size_bytes")
        == expected["artifact_size_bytes"]
        and n5_transport.get("artifact_sha256")
        == expected["artifact_sha256"]
        and n5_transport.get("source_content_binding_verified") is True
        and n5_transport.get("result_content_binding_verified") is True
        and n5_transport.get("stable_runtime_epoch_verified") is True
        and n5_transport.get("redirects_followed") is False
        and n5_transport.get("ambient_proxies_used") is False
        and n5_transport.get("credentials_recorded") is False,
        "N5 HTTP transport evidence is incomplete",
    )
    published = n4_receipt["published_artifacts"]
    request_artifact = n4_request["artifacts"][0]
    request_commitment = _n4_publication_request_commitment(
        publication_id=n4_request["publication_id"],
        package_id=n4_request["package_id"],
        catalog_version=n4_request["catalog_version"],
        expected_current_catalog_version=n4_request[
            "expected_current_catalog_version"
        ],
        object_id=request_artifact["object_id"],
        representation_id=request_artifact["representation_id"],
        artifact_size_bytes=request_artifact["artifact_size_bytes"],
        artifact_sha256=request_artifact["artifact_sha256"],
        n4_access_plan_ids=request_artifact["plan_ids"],
        provenance=request_artifact["provenance"],
    )
    request_commitment_sha256 = _sha256(_canonical(request_commitment))
    _require(
        len(published) == 1
        and published[0]
        == {
            "object_id": plan["input"]["object_id"],
            "representation_id": FRAME_BUNDLE_REPRESENTATION_ID,
            "artifact_size_bytes": expected["artifact_size_bytes"],
            "artifact_sha256": expected["artifact_sha256"],
        }
        and request_commitment_sha256 == n4_receipt["request_sha256"]
        and n4_receipt["atomic_visibility"] is True
        and (
            n4_before["current_generation_present"] is True
            if n4_result["idempotent_replay"]
            else n4_before["current_generation_present"]
            is (n4_receipt["previous_catalog_version"] is not None)
        )
        and n4_after["current_generation_present"] is True,
        "N4 atomic-publication evidence is incomplete",
    )
    n5_replay = bool(n5_evidence["idempotent_replay"])
    n4_replay = bool(n4_result["idempotent_replay"])
    document: dict[str, Any] = {
        "schema_version": LIVE_PROVISIONING_SMOKE_SCHEMA_VERSION,
        "status": "VERIFIED_LOCAL_LIVE_PROVISIONING",
        "evidence_class": "local-container-protocol-conformance",
        "smoke_id": _identifier(smoke_id, "smoke_id"),
        "representation_id": FRAME_BUNDLE_REPRESENTATION_ID,
        "source_representation_id": "raw_video",
        "object_id": plan["input"]["object_id"],
        "n5_plan_id": plan["plan_id"],
        "n5_plan_sha256": plan["plan_sha256"],
        "n5_transformation_contract_sha256": plan[
            "transformation_contract_sha256"
        ],
        "n5_runtime_epoch": n5_transport["runtime_epoch"],
        "n5_materialization_evidence": n5_evidence,
        "n5_materialization_evidence_sha256": _sha256(
            _canonical(n5_evidence)
        ),
        "n5_transport_receipt": n5_transport,
        "n5_transport_receipt_sha256": _sha256(_canonical(n5_transport)),
        "n5_fresh_materialization_executed": not n5_replay,
        "n5_materialization_idempotent_replay": n5_replay,
        "n5_http_authentication_observed": True,
        "artifact_size_bytes": expected["artifact_size_bytes"],
        "artifact_sha256": expected["artifact_sha256"],
        "n4_access_plan_ids": list(n4_access_plan_ids),
        "n4_access_plan_ids_source": n4_access_plan_ids_source,
        "n4_package_id": request_commitment["package_id"],
        "n4_publication_request_sha256": request_commitment_sha256,
        "n4_publication_id": n4_receipt["publication_id"],
        "n4_previous_catalog_version": n4_receipt[
            "previous_catalog_version"
        ],
        "n4_committed_catalog_version": n4_receipt[
            "committed_catalog_version"
        ],
        "n4_generation_id": n4_receipt["generation_id"],
        "n4_package_sha256": n4_receipt["package_sha256"],
        "n4_publication_receipt": n4_receipt,
        "n4_http_authentication_observed": True,
        "n4_fresh_publication_executed": not n4_replay,
        "n4_publication_idempotent_replay": n4_replay,
        "n4_compare_and_swap_verified": True,
        "n4_atomic_visibility_verified": True,
        "n5_to_n4_content_binding_verified": True,
        "local_http_service_boundaries_exercised": True,
        "local_live_provisioning_conformance_verified": True,
        "fresh_end_to_end_execution_observed": not (n5_replay or n4_replay),
        "durable_replay_adopted": n5_replay or n4_replay,
        "semantic_trial_serve_gate_replaced": False,
        "preprovisioned_serve_gate_still_required": True,
        "n4_data_agent_rebind_required": True,
        "multimodal_digest_live_provisioning_verified": False,
        "materialization_latency_measured": False,
        "publication_latency_measured": False,
        "monetary_cost_measured": False,
        "cloud_network_measured": False,
        "upcloud_used": False,
        "external_network_called": False,
        "source_services_cryptographically_attested": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    _assert_public(document)
    document["receipt_sha256"] = _sha256(_canonical(document))
    return document


def _write_receipt(output_dir: Path, document: Mapping[str, Any]) -> None:
    _require(not output_dir.exists(), "live provisioning output already exists")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    parent = Path(
        tempfile.mkdtemp(prefix=".live-provisioning-", dir=output_dir.parent)
    )
    stage = parent / "output"
    try:
        stage.mkdir()
        payload = _json_bytes(document)
        (stage / RECEIPT_NAME).write_bytes(payload)
        (stage / CHECKSUMS_NAME).write_text(
            f"{_sha256(payload)}  {RECEIPT_NAME}\n",
            encoding="utf-8",
        )
        os.replace(stage, output_dir)
    finally:
        shutil.rmtree(parent, ignore_errors=True)


def run_n5_n4_live_frame_bundle_provisioning_smoke(
    n5_plan: Mapping[str, Any],
    source_video_bytes: bytes,
    *,
    n5_config: N5MaterializationHttpClientConfig,
    n4_config: N4PublicationHttpClientConfig,
    smoke_id: str,
    publication_id: str,
    package_id: str,
    catalog_version: str,
    expected_current_catalog_version: str | None,
    n4_access_plan_ids: Sequence[str] | None = None,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Execute one fresh authenticated frame-bundle provision and freeze it."""

    plan = verify_n5_materialization_plan(n5_plan)
    _identifier(smoke_id, "smoke_id")
    access_plan_ids, access_plan_ids_source = _n4_access_plan_contract(
        n4_access_plan_ids,
        n5_materialization_plan_id=plan["plan_id"],
        expected_current_catalog_version=expected_current_catalog_version,
    )
    _require(
        isinstance(source_video_bytes, bytes)
        and len(source_video_bytes) == plan["input"]["size_bytes"]
        and _sha256(source_video_bytes) == plan["input"]["sha256"]
        and plan["input"]["media_type"] == N5_SOURCE_MEDIA_TYPE
        and plan["expected_output"]["media_type"]
        == FRAME_BUNDLE_MEDIA_TYPE,
        "source bytes or representation differ from the N5 plan",
    )
    # Re-validate both origins here so an HTTPS cloud endpoint cannot be used
    # to obtain a receipt whose class says local-only.
    _local_origin(n5_config.base_url, n5_config.simulator_private_http_hosts)
    _local_origin(n4_config.base_url, n4_config.simulator_private_http_hosts)
    n5 = HttpN5MaterializationClient(n5_config)
    n4 = HttpN4PublicationClient(n4_config)
    n5.health()
    n4_before = n4.health()
    n5_execution = n5.execute(plan, source_video_bytes)
    artifact = n5_execution.artifact_bytes
    _require(
        len(artifact) == plan["expected_output"]["artifact_size_bytes"]
        and _sha256(artifact) == plan["expected_output"]["artifact_sha256"],
        "N5 returned bytes outside the frozen output binding",
    )
    request = _publication_request(
        plan=plan,
        artifact=artifact,
        n4_access_plan_ids=access_plan_ids,
        publication_id=publication_id,
        package_id=package_id,
        catalog_version=catalog_version,
        expected_current_catalog_version=expected_current_catalog_version,
    )
    n4_result = n4.publish(request)
    n4_after = n4.health()
    document = _receipt_document(
        smoke_id=smoke_id,
        plan=plan,
        n5_execution=n5_execution,
        n4_request=request,
        n4_access_plan_ids=access_plan_ids,
        n4_access_plan_ids_source=access_plan_ids_source,
        n4_before=n4_before,
        n4_result=n4_result,
        n4_after=n4_after,
    )
    output = Path(output_dir).resolve()
    _write_receipt(output, document)
    verified = verify_n5_n4_live_frame_bundle_provisioning_smoke(
        output,
        n5_plan=plan,
        n4_access_plan_ids=access_plan_ids,
    )
    return verified | {"output_dir": str(output)}


def verify_n5_n4_live_frame_bundle_provisioning_smoke(
    output_dir: str | Path,
    *,
    n5_plan: Mapping[str, Any],
    n4_access_plan_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Verify one frozen receipt against its exact portable N5 plan."""

    plan = verify_n5_materialization_plan(n5_plan)
    root = Path(output_dir).resolve()
    _require(root.is_dir(), "live provisioning receipt directory is missing")
    files = list(root.iterdir())
    _require(
        {path.name for path in files} == _FILES
        and all(path.is_file() and not path.is_symlink() for path in files),
        "live provisioning receipt file set changed",
    )
    payload = (root / RECEIPT_NAME).read_bytes()
    _require(
        (root / CHECKSUMS_NAME).read_text(encoding="utf-8")
        == f"{_sha256(payload)}  {RECEIPT_NAME}\n",
        "live provisioning receipt checksum failed",
    )
    document = _strict_json(payload, "live provisioning receipt")
    schema_version = document.get("schema_version")
    expected_fields = (
        _LEGACY_RECEIPT_FIELDS
        if schema_version == _LEGACY_LIVE_PROVISIONING_SMOKE_SCHEMA_VERSION
        else _RECEIPT_FIELDS
    )
    _require(
        schema_version
        in {
            _LEGACY_LIVE_PROVISIONING_SMOKE_SCHEMA_VERSION,
            LIVE_PROVISIONING_SMOKE_SCHEMA_VERSION,
        }
        and set(document) == expected_fields,
        "live provisioning receipt field set changed",
    )
    _require(
        payload == _json_bytes(document),
        "live provisioning receipt is not canonical",
    )
    supplied_digest = _digest(document.get("receipt_sha256"), "receipt_sha256")
    unsigned = dict(document)
    del unsigned["receipt_sha256"]
    _require(
        supplied_digest == _sha256(_canonical(unsigned)),
        "live provisioning receipt digest failed",
    )
    n4_receipt = verify_n4_publication_receipt(
        document.get("n4_publication_receipt")
    )
    recorded_access_plan_ids, recorded_access_plan_ids_source = (
        _recorded_n4_access_plan_contract(
            document,
            legacy_schema_version=(
                _LEGACY_LIVE_PROVISIONING_SMOKE_SCHEMA_VERSION
            ),
            n5_materialization_plan_id=plan["plan_id"],
        )
    )
    if n4_access_plan_ids is not None:
        _require(
            recorded_access_plan_ids
            == _canonical_n4_access_plan_ids(n4_access_plan_ids),
            "live provisioning N4 access-plan binding changed",
        )
    expected = plan["expected_output"]
    _verify_n4_publication_request_commitment(
        document,
        n4_receipt,
        schema_version=str(schema_version),
        legacy_schema_version=(
            _LEGACY_LIVE_PROVISIONING_SMOKE_SCHEMA_VERSION
        ),
        object_id=str(plan["input"]["object_id"]),
        representation_id=FRAME_BUNDLE_REPRESENTATION_ID,
        artifact_size_bytes=int(expected["artifact_size_bytes"]),
        artifact_sha256=str(expected["artifact_sha256"]),
        n4_access_plan_ids=recorded_access_plan_ids,
        provenance=N4ArtifactProvenance(
            producer_node_id="N5",
            publication_source_id=str(plan["idempotency_key"]),
            source_representation_id="raw_video",
            source_content_sha256=str(plan["input"]["sha256"]),
            derivation_id="n5-uniform-midpoint-frame-bundle-v1",
            derivation_sha256=str(
                plan["transformation_contract_sha256"]
            ),
        ).to_dict(),
    )
    _require(
        document.get("status") == "VERIFIED_LOCAL_LIVE_PROVISIONING"
        and document.get("evidence_class")
        == "local-container-protocol-conformance"
        and document.get("representation_id")
        == FRAME_BUNDLE_REPRESENTATION_ID
        and document.get("source_representation_id") == "raw_video"
        and document.get("object_id") == plan["input"]["object_id"]
        and document.get("n5_plan_id") == plan["plan_id"]
        and document.get("n5_plan_sha256") == plan["plan_sha256"]
        and document.get("n5_transformation_contract_sha256")
        == plan["transformation_contract_sha256"]
        and document.get("artifact_size_bytes")
        == expected["artifact_size_bytes"]
        and document.get("artifact_sha256") == expected["artifact_sha256"],
        "live provisioning N5 or artifact binding changed",
    )
    _identifier(document.get("smoke_id"), "smoke_id")
    _require(
        isinstance(document.get("n5_runtime_epoch"), str)
        and re.fullmatch(r"[0-9a-f]{32}", document["n5_runtime_epoch"])
        is not None,
        "n5_runtime_epoch is invalid",
    )
    for name in (
        "n5_materialization_evidence_sha256",
        "n5_transport_receipt_sha256",
        "n4_package_sha256",
    ):
        _digest(document.get(name), name)
    n5_evidence = document.get("n5_materialization_evidence")
    n5_transport = document.get("n5_transport_receipt")
    _require(
        isinstance(n5_evidence, dict)
        and n5_evidence.get("schema_version")
        == N5_MATERIALIZATION_EVIDENCE_SCHEMA_VERSION
        and n5_evidence.get("plan_sha256") == plan["plan_sha256"]
        and n5_evidence.get("output") == expected
        and type(n5_evidence.get("idempotent_replay")) is bool
        and n5_evidence.get("input_content_binding_verified") is True
        and n5_evidence.get("canonical_frame_bundle_verified") is True
        and n5_evidence.get("output_binding_verified") is True
        and n5_evidence.get("credentials_recorded") is False
        and document["n5_materialization_evidence_sha256"]
        == _sha256(_canonical(n5_evidence)),
        "recorded N5 materialization evidence is invalid",
    )
    _require(
        isinstance(n5_transport, dict)
        and n5_transport.get("schema_version")
        == N5_MATERIALIZATION_HTTP_RESULT_SCHEMA_VERSION
        and n5_transport.get("status") == "VERIFIED"
        and n5_transport.get("runtime_epoch")
        == document["n5_runtime_epoch"]
        and n5_transport.get("plan_sha256") == plan["plan_sha256"]
        and n5_transport.get("artifact_size_bytes")
        == expected["artifact_size_bytes"]
        and n5_transport.get("artifact_sha256")
        == expected["artifact_sha256"]
        and n5_transport.get("source_content_binding_verified") is True
        and n5_transport.get("result_content_binding_verified") is True
        and n5_transport.get("stable_runtime_epoch_verified") is True
        and n5_transport.get("redirects_followed") is False
        and n5_transport.get("ambient_proxies_used") is False
        and n5_transport.get("credentials_recorded") is False
        and document["n5_transport_receipt_sha256"]
        == _sha256(_canonical(n5_transport)),
        "recorded N5 transport receipt is invalid",
    )
    published = n4_receipt["published_artifacts"]
    _require(
        len(published) == 1
        and published[0]
        == {
            "object_id": plan["input"]["object_id"],
            "representation_id": FRAME_BUNDLE_REPRESENTATION_ID,
            "artifact_size_bytes": expected["artifact_size_bytes"],
            "artifact_sha256": expected["artifact_sha256"],
        }
        and document.get("n4_publication_id")
        == n4_receipt["publication_id"]
        and document.get("n4_previous_catalog_version")
        == n4_receipt["previous_catalog_version"]
        and document.get("n4_committed_catalog_version")
        == n4_receipt["committed_catalog_version"]
        and document.get("n4_generation_id") == n4_receipt["generation_id"]
        and document.get("n4_package_sha256")
        == n4_receipt["package_sha256"]
        and n4_receipt["atomic_visibility"] is True,
        "live provisioning N4 publication binding changed",
    )
    true_flags = (
        "n5_http_authentication_observed",
        "n4_http_authentication_observed",
        "n4_compare_and_swap_verified",
        "n4_atomic_visibility_verified",
        "n5_to_n4_content_binding_verified",
        "local_http_service_boundaries_exercised",
        "local_live_provisioning_conformance_verified",
        "preprovisioned_serve_gate_still_required",
        "n4_data_agent_rebind_required",
    )
    false_flags = (
        "semantic_trial_serve_gate_replaced",
        "multimodal_digest_live_provisioning_verified",
        "materialization_latency_measured",
        "publication_latency_measured",
        "monetary_cost_measured",
        "cloud_network_measured",
        "upcloud_used",
        "external_network_called",
        "source_services_cryptographically_attested",
        "credentials_recorded",
        "eligible_for_scientific_claims",
    )
    _require(
        all(document.get(name) is True for name in true_flags)
        and all(document.get(name) is False for name in false_flags),
        "live provisioning claim boundary changed",
    )
    n5_replay = document.get("n5_materialization_idempotent_replay")
    n4_replay = document.get("n4_publication_idempotent_replay")
    _require(
        type(n5_replay) is bool
        and type(n4_replay) is bool
        and document.get("n5_fresh_materialization_executed")
        is (not n5_replay)
        and document.get("n4_fresh_publication_executed") is (not n4_replay)
        and document.get("fresh_end_to_end_execution_observed")
        is (not (n5_replay or n4_replay))
        and document.get("durable_replay_adopted")
        is (n5_replay or n4_replay)
        and n5_evidence["idempotent_replay"] is n5_replay,
        "live provisioning replay accounting changed",
    )
    _assert_public(document)
    return {
        "status": "VERIFIED",
        "smoke_id": document["smoke_id"],
        "representation_id": document["representation_id"],
        "object_id": document["object_id"],
        "artifact_size_bytes": document["artifact_size_bytes"],
        "artifact_sha256": document["artifact_sha256"],
        "n4_access_plan_ids": recorded_access_plan_ids,
        "n4_access_plan_ids_source": recorded_access_plan_ids_source,
        "n4_committed_catalog_version": document[
            "n4_committed_catalog_version"
        ],
        "n5_fresh_materialization_executed": document[
            "n5_fresh_materialization_executed"
        ],
        "n4_fresh_publication_executed": document[
            "n4_fresh_publication_executed"
        ],
        "fresh_end_to_end_execution_observed": document[
            "fresh_end_to_end_execution_observed"
        ],
        "durable_replay_adopted": document["durable_replay_adopted"],
        "n4_atomic_visibility_verified": True,
        "local_live_provisioning_conformance_verified": True,
        "preprovisioned_serve_gate_still_required": True,
        "multimodal_digest_live_provisioning_verified": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def _digest_plan(
    plan_dir: Path,
    source_video_path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        verified = verify_n5_multimodal_digest_plan(
            plan_dir,
            source_video_path,
        )
        document = _strict_json(
            (plan_dir / N5_DIGEST_PLAN_NAME).read_bytes(),
            "N5 digest plan",
        )
    except Exception as exc:
        raise FullFlowLiveProvisioningSmokeError(
            "N5 digest plan or source did not verify"
        ) from exc
    _require(
        document.get("plan_id") == verified["plan_id"]
        and document.get("plan_sha256") == verified["plan_sha256"]
        and document.get("object_id") == verified["object_id"]
        and document.get("node_id") == "N5"
        and document.get("source", {}).get("representation_id")
        == DIGEST_SOURCE_REPRESENTATION_ID
        and document.get("source", {}).get("media_type") == "video/mp4"
        and document.get("generation", {}).get("representation_id")
        == MULTIMODAL_DIGEST_REPRESENTATION_ID
        and document.get("generation", {}).get("media_type")
        == MULTIMODAL_DIGEST_MEDIA_TYPE
        and document.get("generation", {}).get("model_id")
        == verified["model_id"],
        "N5 digest plan summary binding changed",
    )
    return verified, document


def _write_named_receipt(
    output_dir: Path,
    document: Mapping[str, Any],
    *,
    receipt_name: str,
) -> None:
    _require(not output_dir.exists(), "live provisioning output already exists")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    parent = Path(
        tempfile.mkdtemp(prefix=".live-provisioning-", dir=output_dir.parent)
    )
    stage = parent / "output"
    try:
        stage.mkdir()
        payload = _json_bytes(document)
        (stage / receipt_name).write_bytes(payload)
        (stage / CHECKSUMS_NAME).write_text(
            f"{_sha256(payload)}  {receipt_name}\n",
            encoding="utf-8",
        )
        os.replace(stage, output_dir)
    finally:
        shutil.rmtree(parent, ignore_errors=True)


def run_n5_n4_live_multimodal_digest_provisioning_smoke(
    n5_digest_plan_dir: str | Path,
    source_video_path: str | Path,
    *,
    n5_executor: N5DigestExecutor,
    n4_config: N4PublicationHttpClientConfig,
    smoke_id: str,
    request_id: str,
    publication_id: str,
    package_id: str,
    catalog_version: str,
    expected_current_catalog_version: str | None,
    n4_access_plan_ids: Sequence[str] | None = None,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Execute one authenticated digest generation and atomic N4 publish.

    ``n5_executor`` is a runtime-only seam.  Normal deployment supplies
    :class:`HttpN5DigestMaterializationExecutor`; focused tests can bind the
    existing N5 service to a non-networked fake vision adapter without
    changing the frozen plan or this receipt contract.
    """

    plan_root = Path(n5_digest_plan_dir).resolve()
    source = Path(source_video_path).resolve()
    verified, plan = _digest_plan(plan_root, source)
    smoke_id = _identifier(smoke_id, "smoke_id")
    request_id = _identifier(request_id, "request_id")
    access_plan_ids, access_plan_ids_source = _n4_access_plan_contract(
        n4_access_plan_ids,
        n5_materialization_plan_id=verified["plan_id"],
        expected_current_catalog_version=expected_current_catalog_version,
    )
    _require(
        callable(getattr(n5_executor, "execute", None)),
        "N5 digest runtime executor is invalid",
    )
    _local_origin(n4_config.base_url, n4_config.simulator_private_http_hosts)
    n4 = HttpN4PublicationClient(n4_config)
    n4_before = n4.health()
    try:
        execution = n5_executor.execute(
            plan_root,
            source,
            request_id=request_id,
        )
    except FullFlowLiveProvisioningSmokeError:
        raise
    except Exception as exc:
        raise FullFlowLiveProvisioningSmokeError(
            "N5 digest runtime executor failed"
        ) from exc
    _require(
        isinstance(execution, N5DigestHttpExecution),
        "N5 digest runtime executor returned the wrong type",
    )
    result = execution.result
    transport = execution.transport_receipt
    artifact = execution.artifact_bytes
    _require(
        isinstance(result, dict)
        and set(result) == _N5_DIGEST_RESULT_FIELDS
        and result.get("schema_version")
        == N5_DIGEST_HTTP_RESULT_SCHEMA_VERSION
        and result.get("status") == "COMPLETE"
        and result.get("request_id") == request_id
        and result.get("result_handle") == request_id
        and result.get("plan_id") == verified["plan_id"]
        and result.get("object_id") == verified["object_id"]
        and result.get("source_handle") == verified["source_video_sha256"]
        and result.get("model_id") == verified["model_id"]
        and result.get("llm_called") is True
        and type(result.get("idempotent_replay")) is bool
        and type(result.get("artifact_size_bytes")) is int
        and result["artifact_size_bytes"] > 0
        and result["artifact_size_bytes"]
        <= plan["generation"]["maximum_digest_bytes"]
        and isinstance(result.get("artifact_sha256"), str)
        and _SHA256.fullmatch(result["artifact_sha256"]) is not None
        and result.get("credentials_recorded") is False,
        "N5 digest result differs from its frozen plan",
    )
    _require(
        isinstance(artifact, bytes)
        and len(artifact) == result.get("artifact_size_bytes")
        and _sha256(artifact) == result.get("artifact_sha256")
        and isinstance(transport, dict)
        and set(transport) == _N5_DIGEST_TRANSPORT_FIELDS
        and transport.get("schema_version")
        == N5_DIGEST_TRANSPORT_RECEIPT_SCHEMA_VERSION
        and transport.get("status") == "VERIFIED"
        and transport.get("node_id") == "N5"
        and type(transport.get("registered_plan_count")) is int
        and transport["registered_plan_count"] > 0
        and transport.get("request_id") == request_id
        and transport.get("plan_id") == verified["plan_id"]
        and transport.get("plan_sha256") == verified["plan_sha256"]
        and transport.get("source_handle")
        == verified["source_video_sha256"]
        and transport.get("result_handle") == request_id
        and transport.get("artifact_size_bytes") == len(artifact)
        and transport.get("artifact_sha256") == _sha256(artifact)
        and transport.get("model_id") == verified["model_id"]
        and type(transport.get("source_stage_idempotent_replay")) is bool
        and transport.get("materialization_idempotent_replay")
        is result["idempotent_replay"]
        and transport.get("source_content_binding_verified") is True
        and transport.get("result_content_binding_verified") is True
        and transport.get("plan_registry_stable_verified") is True
        and transport.get("semantic_model_call_attested") is True
        and transport.get("http_authentication_observed") is True
        and transport.get("redirects_followed") is False
        and transport.get("ambient_proxies_used") is False
        and transport.get("credentials_recorded") is False,
        "N5 digest transport or artifact evidence is incomplete",
    )
    provenance = N4ArtifactProvenance(
        producer_node_id="N5",
        publication_source_id=request_id,
        source_representation_id=DIGEST_SOURCE_REPRESENTATION_ID,
        source_content_sha256=verified["source_video_sha256"],
        derivation_id="n5-multimodal-digest-v1",
        derivation_sha256=verified["plan_sha256"],
    )
    n4_request = {
        "schema_version": N4_PUBLICATION_HTTP_REQUEST_SCHEMA_VERSION,
        "publication_id": _identifier(publication_id, "publication_id"),
        "package_id": _identifier(package_id, "package_id"),
        "catalog_version": _identifier(catalog_version, "catalog_version"),
        "expected_current_catalog_version": (
            None
            if expected_current_catalog_version is None
            else _identifier(
                expected_current_catalog_version,
                "expected_current_catalog_version",
            )
        ),
        "artifacts": [
            {
                "object_id": verified["object_id"],
                "representation_id": MULTIMODAL_DIGEST_REPRESENTATION_ID,
                "artifact_base64": base64.b64encode(artifact).decode("ascii"),
                "artifact_sha256": _sha256(artifact),
                "artifact_size_bytes": len(artifact),
                "plan_ids": access_plan_ids,
                "provenance": provenance.to_dict(),
            }
        ],
    }
    n4_result = n4.publish(n4_request)
    n4_after = n4.health()
    n4_receipt = verify_n4_publication_receipt(n4_result["receipt"])
    published = n4_receipt["published_artifacts"]
    request_commitment = _n4_publication_request_commitment(
        publication_id=n4_request["publication_id"],
        package_id=n4_request["package_id"],
        catalog_version=n4_request["catalog_version"],
        expected_current_catalog_version=n4_request[
            "expected_current_catalog_version"
        ],
        object_id=verified["object_id"],
        representation_id=MULTIMODAL_DIGEST_REPRESENTATION_ID,
        artifact_size_bytes=len(artifact),
        artifact_sha256=_sha256(artifact),
        n4_access_plan_ids=access_plan_ids,
        provenance=provenance.to_dict(),
    )
    request_commitment_sha256 = _sha256(_canonical(request_commitment))
    _require(
        len(published) == 1
        and published[0]
        == {
            "object_id": verified["object_id"],
            "representation_id": MULTIMODAL_DIGEST_REPRESENTATION_ID,
            "artifact_size_bytes": len(artifact),
            "artifact_sha256": _sha256(artifact),
        }
        and request_commitment_sha256 == n4_receipt["request_sha256"]
        and n4_receipt["atomic_visibility"] is True
        and (
            n4_before["current_generation_present"] is True
            if n4_result["idempotent_replay"]
            else n4_before["current_generation_present"]
            is (n4_receipt["previous_catalog_version"] is not None)
        )
        and n4_after["current_generation_present"] is True,
        "N4 digest publication evidence is incomplete",
    )
    n5_replay = bool(result["idempotent_replay"])
    n4_replay = bool(n4_result["idempotent_replay"])
    document: dict[str, Any] = {
        "schema_version": LIVE_DIGEST_PROVISIONING_SMOKE_SCHEMA_VERSION,
        "status": "VERIFIED_LOCAL_LIVE_DIGEST_PROVISIONING",
        "evidence_class": "local-container-protocol-conformance",
        "smoke_id": smoke_id,
        "representation_id": MULTIMODAL_DIGEST_REPRESENTATION_ID,
        "source_representation_id": DIGEST_SOURCE_REPRESENTATION_ID,
        "object_id": verified["object_id"],
        "n5_digest_plan_id": verified["plan_id"],
        "n5_digest_plan_sha256": verified["plan_sha256"],
        "n5_digest_model_id": verified["model_id"],
        "n5_digest_sampling_metadata_sha256": plan["sampling"][
            "metadata_sha256"
        ],
        "n5_digest_result": result,
        "n5_digest_result_sha256": _sha256(_canonical(result)),
        "n5_digest_transport_receipt": transport,
        "n5_digest_transport_receipt_sha256": _sha256(
            _canonical(transport)
        ),
        "n5_fresh_digest_materialization_executed": not n5_replay,
        "n5_digest_materialization_idempotent_replay": n5_replay,
        "n5_semantic_model_call_attested": True,
        "n5_http_authentication_observed": True,
        "artifact_size_bytes": len(artifact),
        "artifact_sha256": _sha256(artifact),
        "n4_access_plan_ids": access_plan_ids,
        "n4_access_plan_ids_source": access_plan_ids_source,
        "n4_package_id": request_commitment["package_id"],
        "n4_publication_request_sha256": request_commitment_sha256,
        "n4_publication_id": n4_receipt["publication_id"],
        "n4_previous_catalog_version": n4_receipt[
            "previous_catalog_version"
        ],
        "n4_committed_catalog_version": n4_receipt[
            "committed_catalog_version"
        ],
        "n4_generation_id": n4_receipt["generation_id"],
        "n4_package_sha256": n4_receipt["package_sha256"],
        "n4_publication_receipt": n4_receipt,
        "n4_http_authentication_observed": True,
        "n4_fresh_publication_executed": not n4_replay,
        "n4_publication_idempotent_replay": n4_replay,
        "n4_compare_and_swap_verified": True,
        "n4_atomic_visibility_verified": True,
        "n5_to_n4_content_binding_verified": True,
        "local_http_service_boundaries_exercised": True,
        "local_live_digest_provisioning_conformance_verified": True,
        "fresh_end_to_end_execution_observed": not (n5_replay or n4_replay),
        "durable_replay_adopted": n5_replay or n4_replay,
        "semantic_trial_serve_gate_replaced": False,
        "preprovisioned_serve_gate_still_required": True,
        "n4_data_agent_rebind_required": True,
        "frame_bundle_live_provisioning_verified_by_this_receipt": False,
        "materialization_latency_measured": False,
        "publication_latency_measured": False,
        "monetary_cost_measured": False,
        "cloud_network_measured": False,
        "upcloud_used": False,
        "external_network_call_status": (
            "not-observable-through-n5-digest-http-contract"
        ),
        "semantic_model_call_authenticity_verified": False,
        "source_services_cryptographically_attested": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    _assert_public(document)
    document["receipt_sha256"] = _sha256(_canonical(document))
    output = Path(output_dir).resolve()
    _write_named_receipt(
        output,
        document,
        receipt_name=DIGEST_RECEIPT_NAME,
    )
    verified_output = verify_n5_n4_live_multimodal_digest_provisioning_smoke(
        output,
        n5_digest_plan_dir=plan_root,
        source_video_path=source,
        n4_access_plan_ids=access_plan_ids,
    )
    return verified_output | {"output_dir": str(output)}


def verify_n5_n4_live_multimodal_digest_provisioning_smoke(
    output_dir: str | Path,
    *,
    n5_digest_plan_dir: str | Path,
    source_video_path: str | Path,
    n4_access_plan_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Verify one digest receipt against its exact plan and source video."""

    plan_root = Path(n5_digest_plan_dir).resolve()
    source = Path(source_video_path).resolve()
    verified, plan = _digest_plan(plan_root, source)
    root = Path(output_dir).resolve()
    _require(root.is_dir(), "live digest receipt directory is missing")
    files = list(root.iterdir())
    _require(
        {path.name for path in files} == _DIGEST_FILES
        and all(path.is_file() and not path.is_symlink() for path in files),
        "live digest receipt file set changed",
    )
    payload = (root / DIGEST_RECEIPT_NAME).read_bytes()
    _require(
        (root / CHECKSUMS_NAME).read_text(encoding="utf-8")
        == f"{_sha256(payload)}  {DIGEST_RECEIPT_NAME}\n",
        "live digest receipt checksum failed",
    )
    document = _strict_json(payload, "live digest receipt")
    schema_version = document.get("schema_version")
    expected_fields = (
        _LEGACY_DIGEST_RECEIPT_FIELDS
        if schema_version
        == _LEGACY_LIVE_DIGEST_PROVISIONING_SMOKE_SCHEMA_VERSION
        else _DIGEST_RECEIPT_FIELDS
    )
    _require(
        payload == _json_bytes(document)
        and schema_version
        in {
            _LEGACY_LIVE_DIGEST_PROVISIONING_SMOKE_SCHEMA_VERSION,
            LIVE_DIGEST_PROVISIONING_SMOKE_SCHEMA_VERSION,
        }
        and set(document) == expected_fields,
        "live digest receipt is noncanonical or its field set changed",
    )
    recorded = _digest(document.get("receipt_sha256"), "receipt_sha256")
    unsigned = dict(document)
    del unsigned["receipt_sha256"]
    _require(
        recorded == _sha256(_canonical(unsigned)),
        "live digest receipt digest failed",
    )
    result = document.get("n5_digest_result")
    transport = document.get("n5_digest_transport_receipt")
    _require(
        isinstance(result, dict)
        and set(result) == _N5_DIGEST_RESULT_FIELDS
        and result.get("schema_version")
        == N5_DIGEST_HTTP_RESULT_SCHEMA_VERSION
        and result.get("status") == "COMPLETE"
        and result.get("request_id") == result.get("result_handle")
        and result.get("plan_id") == verified["plan_id"]
        and result.get("object_id") == verified["object_id"]
        and result.get("source_handle") == verified["source_video_sha256"]
        and result.get("model_id") == verified["model_id"]
        and result.get("llm_called") is True
        and type(result.get("idempotent_replay")) is bool
        and type(result.get("artifact_size_bytes")) is int
        and result["artifact_size_bytes"] > 0
        and result["artifact_size_bytes"]
        <= plan["generation"]["maximum_digest_bytes"]
        and isinstance(result.get("artifact_sha256"), str)
        and _SHA256.fullmatch(result["artifact_sha256"]) is not None
        and result.get("credentials_recorded") is False
        and document.get("n5_digest_result_sha256")
        == _sha256(_canonical(result)),
        "recorded N5 digest result is invalid",
    )
    _require(
        isinstance(transport, dict)
        and set(transport) == _N5_DIGEST_TRANSPORT_FIELDS
        and transport.get("schema_version")
        == N5_DIGEST_TRANSPORT_RECEIPT_SCHEMA_VERSION
        and transport.get("status") == "VERIFIED"
        and transport.get("node_id") == "N5"
        and type(transport.get("registered_plan_count")) is int
        and transport["registered_plan_count"] > 0
        and transport.get("request_id") == result.get("request_id")
        and transport.get("plan_id") == verified["plan_id"]
        and transport.get("plan_sha256") == verified["plan_sha256"]
        and transport.get("source_handle") == verified["source_video_sha256"]
        and transport.get("result_handle") == result.get("result_handle")
        and transport.get("artifact_size_bytes")
        == result.get("artifact_size_bytes")
        and transport.get("artifact_sha256") == result.get("artifact_sha256")
        and transport.get("model_id") == verified["model_id"]
        and type(transport.get("source_stage_idempotent_replay")) is bool
        and transport.get("materialization_idempotent_replay")
        is result["idempotent_replay"]
        and transport.get("source_content_binding_verified") is True
        and transport.get("result_content_binding_verified") is True
        and transport.get("plan_registry_stable_verified") is True
        and transport.get("semantic_model_call_attested") is True
        and transport.get("http_authentication_observed") is True
        and transport.get("redirects_followed") is False
        and transport.get("ambient_proxies_used") is False
        and transport.get("credentials_recorded") is False
        and document.get("n5_digest_transport_receipt_sha256")
        == _sha256(_canonical(transport)),
        "recorded N5 digest transport receipt is invalid",
    )
    _require(
        document.get("status")
        == "VERIFIED_LOCAL_LIVE_DIGEST_PROVISIONING"
        and document.get("evidence_class")
        == "local-container-protocol-conformance"
        and document.get("representation_id")
        == MULTIMODAL_DIGEST_REPRESENTATION_ID
        and document.get("source_representation_id")
        == DIGEST_SOURCE_REPRESENTATION_ID
        and document.get("object_id") == verified["object_id"]
        and document.get("n5_digest_plan_id") == verified["plan_id"]
        and document.get("n5_digest_plan_sha256") == verified["plan_sha256"]
        and document.get("n5_digest_model_id") == verified["model_id"]
        and document.get("n5_digest_sampling_metadata_sha256")
        == plan["sampling"]["metadata_sha256"]
        and document.get("artifact_size_bytes")
        == result["artifact_size_bytes"]
        and document.get("artifact_sha256") == result["artifact_sha256"],
        "live digest plan or artifact binding changed",
    )
    _identifier(document.get("smoke_id"), "smoke_id")
    for name in (
        "n5_digest_plan_sha256",
        "n5_digest_sampling_metadata_sha256",
        "n5_digest_result_sha256",
        "n5_digest_transport_receipt_sha256",
        "artifact_sha256",
        "n4_package_sha256",
    ):
        _digest(document.get(name), name)
    n4_receipt = verify_n4_publication_receipt(
        document.get("n4_publication_receipt")
    )
    recorded_access_plan_ids, recorded_access_plan_ids_source = (
        _recorded_n4_access_plan_contract(
            document,
            legacy_schema_version=(
                _LEGACY_LIVE_DIGEST_PROVISIONING_SMOKE_SCHEMA_VERSION
            ),
            n5_materialization_plan_id=verified["plan_id"],
        )
    )
    if n4_access_plan_ids is not None:
        _require(
            recorded_access_plan_ids
            == _canonical_n4_access_plan_ids(n4_access_plan_ids),
            "live digest N4 access-plan binding changed",
        )
    _verify_n4_publication_request_commitment(
        document,
        n4_receipt,
        schema_version=str(schema_version),
        legacy_schema_version=(
            _LEGACY_LIVE_DIGEST_PROVISIONING_SMOKE_SCHEMA_VERSION
        ),
        object_id=str(verified["object_id"]),
        representation_id=MULTIMODAL_DIGEST_REPRESENTATION_ID,
        artifact_size_bytes=int(result["artifact_size_bytes"]),
        artifact_sha256=str(result["artifact_sha256"]),
        n4_access_plan_ids=recorded_access_plan_ids,
        provenance=N4ArtifactProvenance(
            producer_node_id="N5",
            publication_source_id=str(result["request_id"]),
            source_representation_id=DIGEST_SOURCE_REPRESENTATION_ID,
            source_content_sha256=str(verified["source_video_sha256"]),
            derivation_id="n5-multimodal-digest-v1",
            derivation_sha256=str(verified["plan_sha256"]),
        ).to_dict(),
    )
    _require(
        n4_receipt["published_artifacts"]
        == [
            {
                "object_id": verified["object_id"],
                "representation_id": MULTIMODAL_DIGEST_REPRESENTATION_ID,
                "artifact_size_bytes": result["artifact_size_bytes"],
                "artifact_sha256": result["artifact_sha256"],
            }
        ]
        and document.get("n4_publication_id")
        == n4_receipt["publication_id"]
        and document.get("n4_previous_catalog_version")
        == n4_receipt["previous_catalog_version"]
        and document.get("n4_committed_catalog_version")
        == n4_receipt["committed_catalog_version"]
        and document.get("n4_generation_id") == n4_receipt["generation_id"]
        and document.get("n4_package_sha256")
        == n4_receipt["package_sha256"]
        and n4_receipt["atomic_visibility"] is True,
        "live digest N4 publication binding changed",
    )
    true_flags = (
        "n5_semantic_model_call_attested",
        "n5_http_authentication_observed",
        "n4_http_authentication_observed",
        "n4_compare_and_swap_verified",
        "n4_atomic_visibility_verified",
        "n5_to_n4_content_binding_verified",
        "local_http_service_boundaries_exercised",
        "local_live_digest_provisioning_conformance_verified",
        "preprovisioned_serve_gate_still_required",
        "n4_data_agent_rebind_required",
    )
    false_flags = (
        "semantic_trial_serve_gate_replaced",
        "frame_bundle_live_provisioning_verified_by_this_receipt",
        "materialization_latency_measured",
        "publication_latency_measured",
        "monetary_cost_measured",
        "cloud_network_measured",
        "upcloud_used",
        "semantic_model_call_authenticity_verified",
        "source_services_cryptographically_attested",
        "credentials_recorded",
        "eligible_for_scientific_claims",
    )
    _require(
        all(document.get(name) is True for name in true_flags)
        and all(document.get(name) is False for name in false_flags),
        "live digest claim boundary changed",
    )
    _require(
        document.get("external_network_call_status")
        == "not-observable-through-n5-digest-http-contract",
        "live digest external-network claim changed",
    )
    n5_replay = document.get("n5_digest_materialization_idempotent_replay")
    n4_replay = document.get("n4_publication_idempotent_replay")
    _require(
        type(n5_replay) is bool
        and type(n4_replay) is bool
        and n5_replay is result["idempotent_replay"]
        and document.get("n5_fresh_digest_materialization_executed")
        is (not n5_replay)
        and document.get("n4_fresh_publication_executed") is (not n4_replay)
        and document.get("fresh_end_to_end_execution_observed")
        is (not (n5_replay or n4_replay))
        and document.get("durable_replay_adopted")
        is (n5_replay or n4_replay),
        "live digest replay accounting changed",
    )
    _assert_public(document)
    return {
        "status": "VERIFIED",
        "smoke_id": document["smoke_id"],
        "representation_id": document["representation_id"],
        "object_id": document["object_id"],
        "model_id": document["n5_digest_model_id"],
        "artifact_size_bytes": document["artifact_size_bytes"],
        "artifact_sha256": document["artifact_sha256"],
        "n4_access_plan_ids": recorded_access_plan_ids,
        "n4_access_plan_ids_source": recorded_access_plan_ids_source,
        "n4_committed_catalog_version": document[
            "n4_committed_catalog_version"
        ],
        "n5_fresh_digest_materialization_executed": document[
            "n5_fresh_digest_materialization_executed"
        ],
        "n4_fresh_publication_executed": document[
            "n4_fresh_publication_executed"
        ],
        "fresh_end_to_end_execution_observed": document[
            "fresh_end_to_end_execution_observed"
        ],
        "durable_replay_adopted": document["durable_replay_adopted"],
        "local_live_digest_provisioning_conformance_verified": True,
        "preprovisioned_serve_gate_still_required": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "CHECKSUMS_NAME",
    "DIGEST_RECEIPT_NAME",
    "FullFlowLiveProvisioningSmokeError",
    "HttpN4PublicationClient",
    "HttpN5DigestMaterializationExecutor",
    "LIVE_DIGEST_PROVISIONING_SMOKE_SCHEMA_VERSION",
    "LIVE_PROVISIONING_SMOKE_SCHEMA_VERSION",
    "N5_DIGEST_TRANSPORT_RECEIPT_SCHEMA_VERSION",
    "N4PublicationHttpClientConfig",
    "N5DigestExecutor",
    "N5DigestHttpClientConfig",
    "N5DigestHttpExecution",
    "RECEIPT_NAME",
    "run_n5_n4_live_frame_bundle_provisioning_smoke",
    "run_n5_n4_live_multimodal_digest_provisioning_smoke",
    "verify_n5_n4_live_frame_bundle_provisioning_smoke",
    "verify_n5_n4_live_multimodal_digest_provisioning_smoke",
]
