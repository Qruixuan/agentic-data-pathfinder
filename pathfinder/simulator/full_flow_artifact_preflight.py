"""Content-level preflight for every artifact in a semantic matrix.

Deployment health only proves that a process answers.  Before a semantic run,
Pathfinder also needs evidence that N3/N4 can return the exact bytes frozen in
the matrix.  This module performs one authenticated full fetch per unique
artifact through an injected probe and records only content identities and
telemetry; endpoints, credentials, payloads, and hidden labels are excluded.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import tempfile
import uuid
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence

from ..data_agent_client import (
    DataAgentAccessRequest,
    DataAgentClientSettings,
    DataAgentHTTPError,
    HttpDataAgentClient,
)
from ..data_agent_manifest import load_data_agent_manifest
from ..frame_bundle_ingest import FRAME_BUNDLE_MEDIA_TYPE
from .n4_derived_data_plane import (
    DATA_AGENT_MANIFEST_PATH as N4_DATA_AGENT_MANIFEST_PATH,
    MULTIMODAL_DIGEST_MEDIA_TYPE,
    N4_LOGICAL_LOCATION,
    N4_LOGICAL_NODE_ID,
    PACKAGE_MANIFEST_NAME as N4_PACKAGE_MANIFEST_NAME,
    verify_n4_derived_data_package,
)
from .raw_cold_data_plane import (
    ARTIFACT_MEDIA_TYPE as RAW_VIDEO_MEDIA_TYPE,
    DATA_AGENT_MANIFEST_PATH as N3_DATA_AGENT_MANIFEST_PATH,
    PACKAGE_MANIFEST_NAME as N3_PACKAGE_MANIFEST_NAME,
    SOURCE_LOCATION as N3_SOURCE_LOCATION,
    SOURCE_NODE_ID as N3_SOURCE_NODE_ID,
)
from .n3_indexed_data_plane import (
    INDEXED_REPRESENTATION_ID,
    verify_n3_semantic_data_plane_package,
)

from .full_flow_semantic_execution_admission import (
    ADMISSION_NAME as SOURCE_ADMISSION_NAME,
    CHECKSUMS_NAME as SOURCE_CHECKSUMS_NAME,
    TRIALS_NAME as SOURCE_TRIALS_NAME,
)
from .full_flow_semantic_route_runtime import ArtifactIdentity


ARTIFACT_PREFLIGHT_SCHEMA_VERSION = (
    "pathfinder.full-flow-semantic-artifact-preflight/v1alpha1"
)
ARTIFACT_OBSERVATION_SCHEMA_VERSION = (
    "pathfinder.full-flow-semantic-artifact-observation/v1alpha1"
)
MANIFEST_NAME = "semantic-artifact-preflight.json"
OBSERVATIONS_NAME = "semantic-artifact-observations.jsonl"
CHECKSUMS_NAME = "SHA256SUMS"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}\Z")
_PLAN_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._|:+-]{0,2047}\Z")
_CONTENT = {MANIFEST_NAME, OBSERVATIONS_NAME}
_FILES = _CONTENT | {CHECKSUMS_NAME}
_SOURCE_CONTENT = {
    "semantic-execution-admission.json",
    "semantic-execution-runtime-gaps.json",
    "semantic-execution-smokes.jsonl",
    "semantic-execution-stages.jsonl",
    "semantic-execution-trials.jsonl",
}
_EXPECTED_ARTIFACT_MEDIA_TYPES = {
    "raw_video": RAW_VIDEO_MEDIA_TYPE,
    "sampled_frame_bundle": FRAME_BUNDLE_MEDIA_TYPE,
    "multimodal_digest": MULTIMODAL_DIGEST_MEDIA_TYPE,
}
_EXPECTED_SOURCE_LOCATIONS = {
    "N3": N3_SOURCE_LOCATION,
    "N4": N4_LOGICAL_LOCATION,
}
_OBSERVATION_KEYS = frozenset({
    "schema_version",
    "source_node_id",
    "service_contract_id",
    "object_id",
    "representation_id",
    "object_catalog_version",
    "expected_sha256",
    "expected_size_bytes",
    "observed_sha256",
    "observed_size_bytes",
    "media_type",
    "plan_id",
    "plan_binding_source_sha256",
    "expected_location",
    "package_binding_verified",
    "authenticated",
    "authentication_challenge_verified",
    "full_content_fetched",
    "request_count",
    "bytes_read",
    "service_time_ms",
    "client_round_trip_ms",
    "artifact_download_elapsed_ms",
    "artifact_download_request_count",
    "artifact_completed_request_count",
    "artifact_full_download_count",
    "artifact_bytes_sent",
    "artifact_transfer_latency_ms",
    "telemetry_complete",
    "content_identity_verified",
    "endpoint_included",
    "credential_value_included",
    "credentials_recorded",
    "observation_sha256",
})


class FullFlowArtifactPreflightError(ValueError):
    """Raised when deployed artifact content is incomplete or unauthenticated."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowArtifactPreflightError(message)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


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
        raise FullFlowArtifactPreflightError(
            "artifact preflight is not canonical JSON"
        ) from exc


def _json_bytes(value: Any) -> bytes:
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


def _jsonl_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    return b"".join(_canonical(row) + b"\n" for row in rows)


def _digest(value: Any, label: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{label} is not lowercase SHA-256",
    )
    return str(value)


def _identifier(value: Any, label: str) -> str:
    _require(
        isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
        f"{label} is invalid",
    )
    return str(value)


def _plan_identifier(value: Any, label: str) -> str:
    _require(
        isinstance(value, str) and _PLAN_IDENTIFIER.fullmatch(value) is not None,
        f"{label} is invalid",
    )
    return str(value)


def _integer(value: Any, label: str, *, minimum: int = 0) -> int:
    _require(
        type(value) is int and value >= minimum,
        f"{label} must be an integer >= {minimum}",
    )
    return int(value)


def _number(value: Any, label: str) -> float:
    _require(
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
        and float(value) >= 0,
        f"{label} must be a finite non-negative number",
    )
    return float(value)


def _strict_json(path: Path, label: str) -> dict[str, Any]:
    _require(path.is_file() and not path.is_symlink(), f"{label} is missing")

    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            _require(key not in result, f"{label} repeats key {key}")
            result[key] = value
        return result

    try:
        value = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=unique,
            parse_constant=lambda token: (_ for _ in ()).throw(
                FullFlowArtifactPreflightError(
                    f"{label} contains invalid constant {token}"
                )
            ),
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowArtifactPreflightError(f"cannot read {label}") from exc
    _require(isinstance(value, dict), f"{label} must be an object")
    return value


def _strict_jsonl(path: Path, label: str) -> list[dict[str, Any]]:
    _require(path.is_file() and not path.is_symlink(), f"{label} is missing")
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FullFlowArtifactPreflightError(f"cannot read {label}") from exc
    _require(bool(lines), f"{label} is empty")
    rows: list[dict[str, Any]] = []
    for index, line in enumerate(lines, start=1):
        _require(bool(line.strip()), f"{label} line {index} is blank")
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise FullFlowArtifactPreflightError(
                f"cannot read {label} line {index}"
            ) from exc
        _require(isinstance(row, dict), f"{label} line {index} is not an object")
        rows.append(row)
    return rows


def _verify_source(root: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    _require(root.is_dir(), "semantic admission directory is missing")
    actual = {path.name for path in root.iterdir()}
    _require(
        actual == _SOURCE_CONTENT | {SOURCE_CHECKSUMS_NAME},
        "semantic admission file set changed",
    )
    expected = b"".join(
        f"{_sha256((root / name).read_bytes())}  {name}\n".encode("utf-8")
        for name in sorted(_SOURCE_CONTENT)
    )
    _require(
        (root / SOURCE_CHECKSUMS_NAME).read_bytes() == expected,
        "semantic admission checksums failed",
    )
    admission = _strict_json(root / SOURCE_ADMISSION_NAME, "semantic admission")
    supplied = _digest(
        admission.get("admission_sha256"), "source admission SHA-256"
    )
    unsigned = dict(admission)
    unsigned.pop("admission_sha256", None)
    _require(
        supplied == _sha256(_canonical(unsigned)),
        "semantic admission digest failed",
    )
    _require(
        admission.get("schema_version")
        == "pathfinder.full-flow-semantic-execution-admission/v1alpha1"
        and admission.get("status")
        == "BLOCKED_MISSING_ROUTE_RUNTIME_ADAPTERS",
        "semantic admission schema or status changed",
    )
    trials = _strict_jsonl(root / SOURCE_TRIALS_NAME, "bound semantic trials")
    dimensions = admission.get("matrix_dimensions")
    expected_trial_count = (
        dimensions.get("trial_count")
        if isinstance(dimensions, Mapping)
        else 64
    )
    _require(
        isinstance(expected_trial_count, int)
        and expected_trial_count > 0
        and len(trials) == expected_trial_count,
        "semantic admission trial count disagrees with its dimensions",
    )
    return admission, trials


def _expected_artifacts(
    trials: Sequence[Mapping[str, Any]],
) -> list[tuple[str, str, ArtifactIdentity]]:
    by_key: dict[tuple[str, str], tuple[str, str, ArtifactIdentity]] = {}
    for trial in trials:
        identities = trial.get("representation_identities")
        _require(isinstance(identities, list), "trial artifact identities are invalid")
        for row in identities:
            _require(isinstance(row, Mapping), "trial artifact identity is invalid")
            binding = row.get("representation_binding")
            _require(isinstance(binding, Mapping), "representation binding is missing")
            identity = ArtifactIdentity(
                object_id=_identifier(row.get("artifact_object_id"), "object_id"),
                representation_id=_identifier(
                    row.get("representation_id"), "representation_id"
                ),
                artifact_sha256=_digest(
                    binding.get("artifact_sha256"), "artifact_sha256"
                ),
                artifact_size_bytes=_integer(
                    binding.get("artifact_size_bytes"),
                    "artifact_size_bytes",
                    minimum=1,
                ),
                object_catalog_version=_identifier(
                    binding.get("object_catalog_version"),
                    "object_catalog_version",
                ),
            )
            _require(
                binding.get("representation_id") == identity.representation_id,
                "representation binding identity changed",
            )
            source_node = "N3" if identity.representation_id == "raw_video" else "N4"
            _require(
                identity.representation_id
                in {"raw_video", "sampled_frame_bundle", "multimodal_digest"},
                "artifact preflight found an unsupported representation",
            )
            service = (
                "N3.raw-data-agent"
                if source_node == "N3"
                else "N4.derived-data-agent"
            )
            key = (identity.object_id, identity.representation_id)
            value = (source_node, service, identity)
            previous = by_key.setdefault(key, value)
            _require(previous == value, "artifact identity differs across trials")
    values = list(by_key.values())
    values.sort(key=lambda item: (item[2].object_id, item[2].representation_id))
    _require(bool(values), "semantic matrix has no artifact identities")
    return values


@dataclass(frozen=True)
class _FrozenDataAgentArtifactBinding:
    source_node_id: str
    service_contract_id: str
    identity: ArtifactIdentity
    media_type: str
    location: str
    plan_id: str
    plan_binding_source_sha256: str


def _file_identity(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
                size += len(block)
    except OSError as exc:
        raise FullFlowArtifactPreflightError(
            "cannot re-read a package-bound Data Agent artifact"
        ) from exc
    return digest.hexdigest(), size


def _package_bindings(
    n3_package_dir: str | Path,
    n4_package_dir: str | Path,
) -> dict[tuple[str, str], _FrozenDataAgentArtifactBinding]:
    """Re-derive one deterministic, valid Data Agent plan per artifact.

    The package verifiers establish the complete endpoint-free package
    contract first.  We then resolve the selected plan through the standard
    Data Agent manifest and re-identify the resulting bytes.  No hidden task
    labels, endpoint values, or credentials participate in plan selection.
    """

    roots = {
        "N3": Path(n3_package_dir).resolve(),
        "N4": Path(n4_package_dir).resolve(),
    }
    try:
        verify_n3_semantic_data_plane_package(roots["N3"])
        verify_n4_derived_data_package(roots["N4"])
    except Exception as exc:
        raise FullFlowArtifactPreflightError(
            "N3/N4 Data Agent package verification failed"
        ) from exc

    specifications = (
        (
            "N3",
            "N3.raw-data-agent",
            N3_PACKAGE_MANIFEST_NAME,
            N3_DATA_AGENT_MANIFEST_PATH,
        ),
        (
            "N4",
            "N4.derived-data-agent",
            N4_PACKAGE_MANIFEST_NAME,
            N4_DATA_AGENT_MANIFEST_PATH,
        ),
    )
    bindings: dict[tuple[str, str], _FrozenDataAgentArtifactBinding] = {}
    for node, service, package_manifest_name, data_agent_manifest_path in (
        specifications
    ):
        root = roots[node]
        package_manifest_path = root / package_manifest_name
        package_manifest_raw = package_manifest_path.read_bytes()
        package = _strict_json(
            package_manifest_path,
            f"{node} package manifest",
        )
        try:
            data_agent = load_data_agent_manifest(
                root / data_agent_manifest_path
            )
        except Exception as exc:
            raise FullFlowArtifactPreflightError(
                f"{node} Data Agent manifest verification failed"
            ) from exc
        _require(data_agent.node_id == node, f"{node} Data Agent node changed")
        _require(
            data_agent.object_catalog is not None,
            f"{node} Data Agent object catalog is missing",
        )
        catalog_version = _identifier(
            package.get("catalog_version"),
            f"{node} catalog_version",
        )
        _require(
            data_agent.object_catalog.catalog_version == catalog_version,
            f"{node} package catalog binding changed",
        )
        rows = package.get("objects")
        _require(isinstance(rows, list) and bool(rows), f"{node} objects changed")
        package_digest = _sha256(package_manifest_raw)
        location = _EXPECTED_SOURCE_LOCATIONS[node]
        for index, row in enumerate(rows):
            _require(
                isinstance(row, Mapping),
                f"{node} objects[{index}] is invalid",
            )
            object_id = _identifier(
                row.get("object_id"),
                f"{node} objects[{index}].object_id",
            )
            representation_id = _identifier(
                row.get("representation_id"),
                f"{node} objects[{index}].representation_id",
            )
            if (
                node == "N3"
                and representation_id == INDEXED_REPRESENTATION_ID
            ):
                # Internal N2-selected transport representation. Its source
                # binding is verified by the exact-selection catalog rather
                # than exposed as a semantic-matrix artifact identity.
                continue
            expected_node = "N3" if representation_id == "raw_video" else "N4"
            _require(node == expected_node, "artifact package assigns wrong node")
            media_type = row.get(
                "artifact_media_type" if node == "N3" else "media_type"
            )
            _require(
                media_type == _EXPECTED_ARTIFACT_MEDIA_TYPES.get(
                    representation_id
                ),
                "artifact package media type changed",
            )
            plans = row.get("plan_ids")
            _require(
                isinstance(plans, list)
                and bool(plans)
                and plans == sorted(set(plans)),
                "artifact package plan bindings are not canonical",
            )
            plan_id = _plan_identifier(
                plans[0],
                f"{node} objects[{index}].plan_ids[0]",
            )
            artifact_digest = _digest(
                row.get("artifact_sha256"),
                f"{node} objects[{index}].artifact_sha256",
            )
            artifact_size = _integer(
                row.get("artifact_size_bytes"),
                f"{node} objects[{index}].artifact_size_bytes",
                minimum=1,
            )
            try:
                resolved = data_agent.resolve(
                    plan_id=plan_id,
                    object_id=object_id,
                    representation_id=representation_id,
                    requested_location=location,
                )
            except Exception as exc:
                raise FullFlowArtifactPreflightError(
                    "frozen Data Agent plan does not resolve exactly"
                ) from exc
            _require(
                resolved.object_id == object_id
                and resolved.representation_id == representation_id
                and resolved.location == location
                and resolved.media_type == media_type,
                "frozen Data Agent plan resolves different artifact metadata",
            )
            actual_digest, actual_size = _file_identity(resolved.path)
            _require(
                actual_digest == artifact_digest and actual_size == artifact_size,
                "frozen Data Agent plan resolves different artifact bytes",
            )
            key = (object_id, representation_id)
            _require(key not in bindings, "N3/N4 package artifact repeats")
            bindings[key] = _FrozenDataAgentArtifactBinding(
                source_node_id=node,
                service_contract_id=service,
                identity=ArtifactIdentity(
                    object_id=object_id,
                    representation_id=representation_id,
                    artifact_sha256=artifact_digest,
                    artifact_size_bytes=artifact_size,
                    object_catalog_version=catalog_version,
                ),
                media_type=str(media_type),
                location=location,
                plan_id=plan_id,
                plan_binding_source_sha256=package_digest,
            )
    _require(bool(bindings), "N3/N4 Data Agent packages have no artifacts")
    return bindings


@dataclass(frozen=True)
class ArtifactAvailabilityObservation:
    source_node_id: str
    service_contract_id: str
    identity: ArtifactIdentity
    observed_sha256: str
    observed_size_bytes: int
    media_type: str
    plan_id: str
    plan_binding_source_sha256: str
    expected_location: str
    package_binding_verified: bool
    authenticated: bool
    authentication_challenge_verified: bool
    full_content_fetched: bool
    request_count: int = 1
    bytes_read: int = 0
    service_time_ms: float = 0.0
    client_round_trip_ms: float = 0.0
    artifact_download_elapsed_ms: float = 0.0
    artifact_download_request_count: int = 0
    artifact_completed_request_count: int = 0
    artifact_full_download_count: int = 0
    artifact_bytes_sent: int = 0
    artifact_transfer_latency_ms: float = 0.0
    telemetry_complete: bool = False

    def __post_init__(self) -> None:
        _require(self.source_node_id in {"N3", "N4"}, "source node is invalid")
        _identifier(self.service_contract_id, "service_contract_id")
        _digest(self.observed_sha256, "observed_sha256")
        _integer(self.observed_size_bytes, "observed_size_bytes", minimum=1)
        _require(
            isinstance(self.media_type, str) and "/" in self.media_type,
            "media_type is invalid",
        )
        _plan_identifier(self.plan_id, "plan_id")
        _digest(
            self.plan_binding_source_sha256,
            "plan_binding_source_sha256",
        )
        _identifier(self.expected_location, "expected_location")
        _require(
            self.package_binding_verified is True,
            "artifact plan was not verified against its frozen package",
        )
        _require(self.authenticated is True, "artifact probe was unauthenticated")
        _require(
            self.authentication_challenge_verified is True,
            "Data Agent bearer enforcement was not verified",
        )
        _require(
            self.full_content_fetched is True,
            "artifact probe was not a full fetch",
        )
        _integer(self.request_count, "request_count", minimum=1)
        _integer(self.bytes_read, "bytes_read")
        _number(self.service_time_ms, "service_time_ms")
        _number(self.client_round_trip_ms, "client_round_trip_ms")
        _number(
            self.artifact_download_elapsed_ms,
            "artifact_download_elapsed_ms",
        )
        _integer(
            self.artifact_download_request_count,
            "artifact_download_request_count",
        )
        _integer(
            self.artifact_completed_request_count,
            "artifact_completed_request_count",
        )
        _integer(
            self.artifact_full_download_count,
            "artifact_full_download_count",
        )
        _integer(self.artifact_bytes_sent, "artifact_bytes_sent")
        _number(
            self.artifact_transfer_latency_ms,
            "artifact_transfer_latency_ms",
        )
        _require(
            self.telemetry_complete is True,
            "artifact transfer telemetry is incomplete",
        )


class ArtifactAvailabilityProbe(Protocol):
    def fetch_and_verify(
        self,
        *,
        identity: ArtifactIdentity,
        source_node_id: str,
        service_contract_id: str,
    ) -> ArtifactAvailabilityObservation: ...


class HttpDataAgentArtifactAvailabilityProbe:
    """Production full-content probe for the frozen N3/N4 Data Agents.

    Successful HTTP access with a non-empty bearer credential establishes the
    authenticated control request.  Binary representations are then fully
    downloaded and their quiescent transfer telemetry is required to report
    exactly one completed full download.  The digest representation is
    returned inline by the Data Agent; it is still a full-content access, but
    honestly records zero artifact-download requests and bytes sent.
    """

    def __init__(
        self,
        *,
        n3_client: HttpDataAgentClient,
        n4_client: HttpDataAgentClient,
        n3_package_dir: str | Path,
        n4_package_dir: str | Path,
        preflight_id: str,
        telemetry_quiescence_timeout_seconds: float = 5.0,
    ) -> None:
        self._clients = {"N3": n3_client, "N4": n4_client}
        _require(
            all(
                isinstance(client.settings.token, str)
                and bool(client.settings.token)
                for client in self._clients.values()
            ),
            "N3/N4 artifact preflight requires bearer credentials",
        )
        self._bindings = _package_bindings(n3_package_dir, n4_package_dir)
        self._authentication_verified_nodes: set[str] = set()
        self._preflight_id = _identifier(preflight_id, "preflight_id")
        self._telemetry_timeout = _number(
            telemetry_quiescence_timeout_seconds,
            "telemetry_quiescence_timeout_seconds",
        )
        _require(
            self._telemetry_timeout > 0.0,
            "telemetry_quiescence_timeout_seconds must be positive",
        )

    def fetch_and_verify(
        self,
        *,
        identity: ArtifactIdentity,
        source_node_id: str,
        service_contract_id: str,
    ) -> ArtifactAvailabilityObservation:
        key = (identity.object_id, identity.representation_id)
        binding = self._bindings.get(key)
        _require(binding is not None, "artifact is absent from N3/N4 packages")
        _require(
            binding.identity == identity
            and binding.source_node_id == source_node_id
            and binding.service_contract_id == service_contract_id,
            "artifact admission identity differs from its N3/N4 package",
        )
        access_id = _sha256(_canonical({
            "domain": "pathfinder.artifact-preflight-http-access/v1",
            "preflight_id": self._preflight_id,
            "invocation_nonce": uuid.uuid4().hex,
            "source_node_id": source_node_id,
            "object_id": identity.object_id,
            "representation_id": identity.representation_id,
            "plan_id": binding.plan_id,
        }))
        request = DataAgentAccessRequest(
            access_id=access_id,
            session_id=self._preflight_id,
            trial_id=(
                f"artifact-preflight|{source_node_id}|{identity.object_id}|"
                f"{identity.representation_id}"
            ),
            plan_id=binding.plan_id,
            plan_epoch=0,
            task_class_id="artifact_preflight",
            representation_id=identity.representation_id,
            event_index=0,
            latency_multiplier=1.0,
            binding={"location": binding.location},
            object_id=identity.object_id,
        )
        client = self._clients[source_node_id]
        self._verify_authentication_enforced(
            source_node_id,
            client,
            request,
        )
        download_elapsed_ms = 0.0
        if identity.representation_id == "multimodal_digest":
            result = client.access(request)
            _require(
                result.object_id == identity.object_id
                and result.object_catalog_version
                == identity.object_catalog_version
                and result.location == binding.location,
                "Data Agent inline access changed artifact metadata",
            )
            _require(
                result.payload.kind == "inline_text"
                and result.payload.media_type == binding.media_type
                and isinstance(result.payload.value, str),
                "multimodal digest is not the frozen inline media type",
            )
            raw = result.payload.value.encode("utf-8")
            _require(
                result.payload.sha256 == identity.artifact_sha256,
                "Data Agent inline digest commitment changed",
            )
            service_time_ms = result.service_latency_ms
            client_round_trip_ms = result.client_round_trip_ms
            media_type = result.payload.media_type
        else:
            artifact = client.fetch_binary_artifact(
                request,
                allowed_media_types={binding.media_type},
            )
            _require(
                artifact.access_id == request.access_id
                and artifact.object_id == identity.object_id
                and artifact.object_catalog_version
                == identity.object_catalog_version
                and artifact.location == binding.location,
                "Data Agent binary access changed artifact metadata",
            )
            raw = artifact.data
            media_type = artifact.media_type
            service_time_ms = artifact.service_latency_ms
            client_round_trip_ms = artifact.client_round_trip_ms
            download_elapsed_ms = artifact.download_elapsed_ms or 0.0
        _require(
            media_type == binding.media_type,
            "Data Agent returned a different artifact media type",
        )
        observed_digest = _sha256(raw)
        _require(
            len(raw) == identity.artifact_size_bytes
            and observed_digest == identity.artifact_sha256,
            "Data Agent returned bytes outside the frozen artifact identity",
        )
        telemetry = client.get_access_telemetry(
            request.access_id,
            wait_for_quiescence=True,
            quiescence_timeout_seconds=self._telemetry_timeout,
        )
        _require(
            telemetry.telemetry_complete
            and telemetry.object_id == identity.object_id
            and telemetry.representation_id == identity.representation_id
            and telemetry.object_catalog_version
            == identity.object_catalog_version,
            "Data Agent transfer telemetry changed artifact identity",
        )
        binary = identity.representation_id != "multimodal_digest"
        expected_downloads = 1 if binary else 0
        expected_sent = identity.artifact_size_bytes if binary else 0
        _require(
            telemetry.download_request_count == expected_downloads
            and telemetry.completed_request_count == expected_downloads
            and telemetry.full_download_count == expected_downloads
            and telemetry.bytes_sent == expected_sent,
            "Data Agent transfer telemetry does not prove one exact full fetch",
        )
        return ArtifactAvailabilityObservation(
            source_node_id=source_node_id,
            service_contract_id=service_contract_id,
            identity=identity,
            observed_sha256=observed_digest,
            observed_size_bytes=len(raw),
            media_type=media_type,
            plan_id=binding.plan_id,
            plan_binding_source_sha256=(
                binding.plan_binding_source_sha256
            ),
            expected_location=binding.location,
            package_binding_verified=True,
            authenticated=True,
            authentication_challenge_verified=True,
            full_content_fetched=True,
            request_count=1,
            bytes_read=len(raw),
            service_time_ms=float(service_time_ms or 0.0),
            client_round_trip_ms=float(client_round_trip_ms or 0.0),
            artifact_download_elapsed_ms=float(download_elapsed_ms),
            artifact_download_request_count=(
                telemetry.download_request_count
            ),
            artifact_completed_request_count=(
                telemetry.completed_request_count
            ),
            artifact_full_download_count=telemetry.full_download_count,
            artifact_bytes_sent=telemetry.bytes_sent,
            artifact_transfer_latency_ms=telemetry.transfer_latency_ms,
            telemetry_complete=True,
        )

    def _verify_authentication_enforced(
        self,
        source_node_id: str,
        client: HttpDataAgentClient,
        request: DataAgentAccessRequest,
    ) -> None:
        if source_node_id in self._authentication_verified_nodes:
            return
        challenge = replace(
            request,
            access_id=_sha256(_canonical({
                "domain": "pathfinder.artifact-preflight-auth-challenge/v1",
                "nonce": uuid.uuid4().hex,
                "source_node_id": source_node_id,
            })),
        )
        invalid_client = HttpDataAgentClient(replace(
            client.settings,
            token=f"invalid-preflight-{uuid.uuid4().hex}",
        ))
        try:
            invalid_client.access(challenge)
        except DataAgentHTTPError as exc:
            _require(
                exc.status_code == 401,
                "Data Agent bearer enforcement challenge returned a "
                "non-authentication status",
            )
        except Exception as exc:
            raise FullFlowArtifactPreflightError(
                "Data Agent bearer enforcement challenge could not be "
                "verified"
            ) from exc
        else:
            raise FullFlowArtifactPreflightError(
                "Data Agent accepted an invalid bearer credential"
            )
        self._authentication_verified_nodes.add(source_node_id)


def preflight_full_flow_semantic_artifacts_over_http(
    semantic_execution_admission_dir: str | Path,
    n3_package_dir: str | Path,
    n4_package_dir: str | Path,
    *,
    n3_base_url: str,
    n4_base_url: str,
    n3_token: str,
    n4_token: str,
    preflight_id: str,
    output_dir: str | Path,
    timeout_seconds: float = 30.0,
    max_retries: int = 1,
    max_artifact_bytes: int = 64 * 1024 * 1024 * 1024,
    telemetry_quiescence_timeout_seconds: float = 5.0,
    simulator_private_http_hosts: Sequence[str] = (),
) -> dict[str, Any]:
    """Run the source-bound N3/N4 preflight through authenticated HTTP."""

    _require(
        isinstance(n3_token, str)
        and bool(n3_token)
        and isinstance(n4_token, str)
        and bool(n4_token),
        "N3/N4 artifact preflight requires bearer credentials",
    )
    private_hosts = tuple(simulator_private_http_hosts)
    settings = {
        "N3": DataAgentClientSettings(
            base_url=n3_base_url,
            token=n3_token,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            max_artifact_bytes=max_artifact_bytes,
            simulator_private_http_hosts=private_hosts,
        ),
        "N4": DataAgentClientSettings(
            base_url=n4_base_url,
            token=n4_token,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            max_artifact_bytes=max_artifact_bytes,
            simulator_private_http_hosts=private_hosts,
        ),
    }
    probe = HttpDataAgentArtifactAvailabilityProbe(
        n3_client=HttpDataAgentClient(settings["N3"]),
        n4_client=HttpDataAgentClient(settings["N4"]),
        n3_package_dir=n3_package_dir,
        n4_package_dir=n4_package_dir,
        preflight_id=preflight_id,
        telemetry_quiescence_timeout_seconds=(
            telemetry_quiescence_timeout_seconds
        ),
    )
    preflight_full_flow_semantic_artifacts(
        semantic_execution_admission_dir,
        preflight_id=preflight_id,
        probe=probe,
        output_dir=output_dir,
    )
    return verify_full_flow_semantic_artifact_preflight(
        output_dir,
        semantic_execution_admission_dir=(
            semantic_execution_admission_dir
        ),
        n3_package_dir=n3_package_dir,
        n4_package_dir=n4_package_dir,
    ) | {"output_dir": str(Path(output_dir).resolve())}


def _observation_row(
    expected_node: str,
    expected_service: str,
    expected: ArtifactIdentity,
    observed: ArtifactAvailabilityObservation,
) -> dict[str, Any]:
    _require(
        observed.source_node_id == expected_node
        and observed.service_contract_id == expected_service
        and observed.identity == expected,
        "artifact probe returned a different source or identity",
    )
    _require(
        observed.observed_sha256 == expected.artifact_sha256
        and observed.observed_size_bytes == expected.artifact_size_bytes,
        "deployed artifact content differs from the frozen identity",
    )
    _require(
        observed.bytes_read == expected.artifact_size_bytes,
        "artifact probe byte count does not prove one full fetch",
    )
    _require(
        observed.media_type
        == _EXPECTED_ARTIFACT_MEDIA_TYPES[expected.representation_id],
        "artifact probe returned the wrong media type",
    )
    _require(
        observed.expected_location == _EXPECTED_SOURCE_LOCATIONS[expected_node],
        "artifact probe returned the wrong source location",
    )
    expected_downloads = 0 if expected.representation_id == "multimodal_digest" else 1
    expected_sent = 0 if expected_downloads == 0 else expected.artifact_size_bytes
    _require(
        observed.request_count == 1
        and observed.artifact_download_request_count == expected_downloads
        and observed.artifact_completed_request_count == expected_downloads
        and observed.artifact_full_download_count == expected_downloads
        and observed.artifact_bytes_sent == expected_sent,
        "artifact probe telemetry does not describe one exact full fetch",
    )
    row: dict[str, Any] = {
        "schema_version": ARTIFACT_OBSERVATION_SCHEMA_VERSION,
        "source_node_id": expected_node,
        "service_contract_id": expected_service,
        "object_id": expected.object_id,
        "representation_id": expected.representation_id,
        "object_catalog_version": expected.object_catalog_version,
        "expected_sha256": expected.artifact_sha256,
        "expected_size_bytes": expected.artifact_size_bytes,
        "observed_sha256": observed.observed_sha256,
        "observed_size_bytes": observed.observed_size_bytes,
        "media_type": observed.media_type,
        "plan_id": observed.plan_id,
        "plan_binding_source_sha256": (
            observed.plan_binding_source_sha256
        ),
        "expected_location": observed.expected_location,
        "package_binding_verified": True,
        "authenticated": True,
        "authentication_challenge_verified": True,
        "full_content_fetched": True,
        "request_count": observed.request_count,
        "bytes_read": observed.bytes_read,
        "service_time_ms": float(observed.service_time_ms),
        "client_round_trip_ms": float(observed.client_round_trip_ms),
        "artifact_download_elapsed_ms": float(
            observed.artifact_download_elapsed_ms
        ),
        "artifact_download_request_count": (
            observed.artifact_download_request_count
        ),
        "artifact_completed_request_count": (
            observed.artifact_completed_request_count
        ),
        "artifact_full_download_count": (
            observed.artifact_full_download_count
        ),
        "artifact_bytes_sent": observed.artifact_bytes_sent,
        "artifact_transfer_latency_ms": float(
            observed.artifact_transfer_latency_ms
        ),
        "telemetry_complete": True,
        "content_identity_verified": True,
        "endpoint_included": False,
        "credential_value_included": False,
        "credentials_recorded": False,
    }
    row["observation_sha256"] = _sha256(_canonical(row))
    return row


def _manifest(
    admission: Mapping[str, Any],
    source_root: Path,
    rows: Sequence[Mapping[str, Any]],
    preflight_id: str,
) -> dict[str, Any]:
    binding = {
        "source_admission_id": _identifier(
            admission.get("admission_id"), "source admission_id"
        ),
        "source_admission_sha256": _digest(
            admission.get("admission_sha256"), "source admission SHA-256"
        ),
        "source_admission_file_sha256": _sha256(
            (source_root / SOURCE_ADMISSION_NAME).read_bytes()
        ),
        "source_admission_checksums_sha256": _sha256(
            (source_root / SOURCE_CHECKSUMS_NAME).read_bytes()
        ),
    }
    document: dict[str, Any] = {
        "schema_version": ARTIFACT_PREFLIGHT_SCHEMA_VERSION,
        "status": "COMPLETE",
        "preflight_id": _identifier(preflight_id, "preflight_id"),
        "source_binding": binding,
        "source_binding_sha256": _sha256(_canonical(binding)),
        "artifact_count": len(rows),
        "source_node_counts": {
            node: sum(row["source_node_id"] == node for row in rows)
            for node in ("N3", "N4")
        },
        "verified_artifact_bytes": sum(int(row["bytes_read"]) for row in rows),
        "observation_file": OBSERVATIONS_NAME,
        "observation_file_sha256": _sha256(_jsonl_bytes(rows)),
        "all_content_identities_verified": True,
        "all_fetches_authenticated": True,
        "all_fetches_full_content": True,
        "artifact_payloads_included": False,
        "endpoint_values_included": False,
        "credential_values_included": False,
        "performance_claimed": False,
        "cost_claimed": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    document["preflight_sha256"] = _sha256(_canonical(document))
    return document


def _verify_output(root: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    _require(root.is_dir(), "artifact preflight directory is missing")
    entries = list(root.iterdir())
    _require(
        all(path.is_file() and not path.is_symlink() for path in entries),
        "artifact preflight contains a non-regular file",
    )
    _require({path.name for path in entries} == _FILES, "preflight file set changed")
    expected_checksums = b"".join(
        f"{_sha256((root / name).read_bytes())}  {name}\n".encode("utf-8")
        for name in sorted(_CONTENT)
    )
    _require(
        (root / CHECKSUMS_NAME).read_bytes() == expected_checksums,
        "artifact preflight checksums failed",
    )
    manifest = _strict_json(root / MANIFEST_NAME, "artifact preflight manifest")
    rows = _strict_jsonl(root / OBSERVATIONS_NAME, "artifact observations")
    supplied = _digest(manifest.pop("preflight_sha256", None), "preflight_sha256")
    _require(supplied == _sha256(_canonical(manifest)), "preflight digest failed")
    manifest["preflight_sha256"] = supplied
    _require(
        manifest.get("schema_version") == ARTIFACT_PREFLIGHT_SCHEMA_VERSION
        and manifest.get("status") == "COMPLETE",
        "artifact preflight schema or status changed",
    )
    _require(
        manifest.get("artifact_count") == len(rows)
        and manifest.get("observation_file_sha256") == _sha256(_jsonl_bytes(rows)),
        "artifact observation count or digest changed",
    )
    identities: list[tuple[str, str]] = []
    for row in rows:
        _require(
            set(row) == _OBSERVATION_KEYS
            and row.get("package_binding_verified") is True
            and row.get("schema_version")
            == ARTIFACT_OBSERVATION_SCHEMA_VERSION
            and row.get("authenticated") is True
            and row.get("authentication_challenge_verified") is True
            and row.get("full_content_fetched") is True
            and row.get("telemetry_complete") is True
            and row.get("content_identity_verified") is True
            and row.get("expected_sha256") == row.get("observed_sha256")
            and row.get("expected_size_bytes") == row.get("observed_size_bytes")
            and row.get("bytes_read") == row.get("observed_size_bytes")
            and row.get("credentials_recorded") is False,
            "artifact observation is incomplete",
        )
        representation_id = row.get("representation_id")
        source_node_id = row.get("source_node_id")
        _require(
            representation_id in _EXPECTED_ARTIFACT_MEDIA_TYPES
            and source_node_id in _EXPECTED_SOURCE_LOCATIONS
            and row.get("media_type")
            == _EXPECTED_ARTIFACT_MEDIA_TYPES[representation_id]
            and row.get("expected_location")
            == _EXPECTED_SOURCE_LOCATIONS[source_node_id],
            "artifact observation media type or location changed",
        )
        _plan_identifier(row.get("plan_id"), "observation plan_id")
        _digest(
            row.get("plan_binding_source_sha256"),
            "observation plan binding source SHA-256",
        )
        _identifier(
            row.get("expected_location"),
            "observation expected_location",
        )
        for name in (
            "request_count",
            "bytes_read",
            "artifact_download_request_count",
            "artifact_completed_request_count",
            "artifact_full_download_count",
            "artifact_bytes_sent",
        ):
            _integer(row.get(name), f"observation {name}")
        for name in (
            "service_time_ms",
            "client_round_trip_ms",
            "artifact_download_elapsed_ms",
            "artifact_transfer_latency_ms",
        ):
            _number(row.get(name), f"observation {name}")
        expected_downloads = 0 if representation_id == "multimodal_digest" else 1
        expected_sent = (
            0 if expected_downloads == 0 else row.get("observed_size_bytes")
        )
        _require(
            row.get("request_count") == 1
            and row.get("artifact_download_request_count")
            == expected_downloads
            and row.get("artifact_completed_request_count")
            == expected_downloads
            and row.get("artifact_full_download_count")
            == expected_downloads
            and row.get("artifact_bytes_sent") == expected_sent,
            "artifact observation transfer telemetry changed",
        )
        supplied_row = _digest(
            row.pop("observation_sha256", None), "observation_sha256"
        )
        _require(
            supplied_row == _sha256(_canonical(row)),
            "artifact observation digest failed",
        )
        row["observation_sha256"] = supplied_row
        identities.append((
            str(row.get("object_id")),
            str(row.get("representation_id")),
        ))
    _require(identities == sorted(identities), "artifact observations are not ordered")
    _require(len(identities) == len(set(identities)), "artifact observation repeats")
    return manifest, rows


def preflight_full_flow_semantic_artifacts(
    semantic_execution_admission_dir: str | Path,
    *,
    preflight_id: str,
    probe: ArtifactAvailabilityProbe,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Fetch and verify every unique deployed semantic artifact exactly once."""

    source_root = Path(semantic_execution_admission_dir).resolve()
    admission, trials = _verify_source(source_root)
    expected = _expected_artifacts(trials)
    rows = [
        _observation_row(
            source_node,
            service,
            identity,
            probe.fetch_and_verify(
                identity=identity,
                source_node_id=source_node,
                service_contract_id=service,
            ),
        )
        for source_node, service, identity in expected
    ]
    manifest = _manifest(admission, source_root, rows, preflight_id)
    documents = {
        MANIFEST_NAME: _json_bytes(manifest),
        OBSERVATIONS_NAME: _jsonl_bytes(rows),
    }
    documents[CHECKSUMS_NAME] = b"".join(
        f"{_sha256(documents[name])}  {name}\n".encode("utf-8")
        for name in sorted(_CONTENT)
    )
    target = Path(output_dir).resolve()
    _require(not target.exists(), f"output directory already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    parent = Path(tempfile.mkdtemp(prefix=".artifact-preflight-", dir=target.parent))
    stage = parent / "output"
    try:
        stage.mkdir()
        for name, payload in documents.items():
            (stage / name).write_bytes(payload)
        _verify_output(stage)
        os.replace(stage, target)
    finally:
        shutil.rmtree(parent, ignore_errors=True)
    return verify_full_flow_semantic_artifact_preflight(
        target,
        semantic_execution_admission_dir=source_root,
    ) | {"output_dir": str(target)}


def verify_full_flow_semantic_artifact_preflight(
    output_dir: str | Path,
    *,
    semantic_execution_admission_dir: str | Path,
    n3_package_dir: str | Path | None = None,
    n4_package_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Verify preflight evidence and rebind it to all expected artifacts."""

    root = Path(output_dir).resolve()
    source_root = Path(semantic_execution_admission_dir).resolve()
    manifest, rows = _verify_output(root)
    admission, trials = _verify_source(source_root)
    expected = _expected_artifacts(trials)
    expected_identities = [
        (identity.object_id, identity.representation_id, identity.artifact_sha256,
         identity.artifact_size_bytes, source_node, service)
        for source_node, service, identity in expected
    ]
    observed_identities = [
        (row["object_id"], row["representation_id"], row["observed_sha256"],
         row["observed_size_bytes"], row["source_node_id"], row["service_contract_id"])
        for row in rows
    ]
    _require(
        observed_identities == expected_identities,
        "artifact preflight does not cover the current semantic admission",
    )
    _require(
        (n3_package_dir is None) == (n4_package_dir is None),
        "N3 and N4 package directories must be supplied together",
    )
    package_bindings_checked = n3_package_dir is not None
    if package_bindings_checked:
        assert n3_package_dir is not None
        assert n4_package_dir is not None
        bindings = _package_bindings(n3_package_dir, n4_package_dir)
        for row in rows:
            binding = bindings.get(
                (str(row["object_id"]), str(row["representation_id"]))
            )
            _require(
                binding is not None
                and binding.source_node_id == row["source_node_id"]
                and binding.service_contract_id == row["service_contract_id"]
                and binding.identity.artifact_sha256 == row["observed_sha256"]
                and binding.identity.artifact_size_bytes
                == row["observed_size_bytes"]
                and binding.identity.object_catalog_version
                == row["object_catalog_version"]
                and binding.media_type == row["media_type"]
                and binding.location == row["expected_location"]
                and binding.plan_id == row["plan_id"]
                and binding.plan_binding_source_sha256
                == row["plan_binding_source_sha256"],
                "artifact observation no longer matches its N3/N4 package",
            )
    expected_manifest = _manifest(
        admission,
        source_root,
        rows,
        str(manifest["preflight_id"]),
    )
    _require(
        (root / MANIFEST_NAME).read_bytes() == _json_bytes(expected_manifest),
        "artifact preflight manifest does not match its source admission",
    )
    return {
        "status": "VERIFIED",
        "preflight_id": manifest["preflight_id"],
        "preflight_sha256": manifest["preflight_sha256"],
        "artifact_count": len(rows),
        "verified_artifact_bytes": manifest["verified_artifact_bytes"],
        "all_content_identities_verified": True,
        "source_binding_checked": True,
        "data_agent_package_bindings_checked": package_bindings_checked,
        "artifact_payloads_included": False,
        "endpoint_values_included": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "ARTIFACT_OBSERVATION_SCHEMA_VERSION",
    "ARTIFACT_PREFLIGHT_SCHEMA_VERSION",
    "ArtifactAvailabilityObservation",
    "ArtifactAvailabilityProbe",
    "CHECKSUMS_NAME",
    "FullFlowArtifactPreflightError",
    "HttpDataAgentArtifactAvailabilityProbe",
    "MANIFEST_NAME",
    "OBSERVATIONS_NAME",
    "preflight_full_flow_semantic_artifacts",
    "preflight_full_flow_semantic_artifacts_over_http",
    "verify_full_flow_semantic_artifact_preflight",
]
