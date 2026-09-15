"""Durable local bulk N5-to-N4 provisioning for the full-flow simulator.

The one-object live provisioning smokes are the authority for materializing
and publishing one derived representation.  This module only coordinates
those authorities over an exact frozen provisioning catalog.  Endpoint and
credential configuration remains inside injected executors; the durable
run records contain content commitments, never those runtime values.

The resulting evidence establishes local data/protocol conformance.  It is
not a performance, monetary-cost, cloud-network, UpCloud, or scientific
result.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence

from .full_flow_live_provisioning_smoke import (
    CHECKSUMS_NAME as ONE_OBJECT_CHECKSUMS_NAME,
    DIGEST_RECEIPT_NAME,
    RECEIPT_NAME,
    N4PublicationHttpClientConfig,
    N5DigestExecutor,
    run_n5_n4_live_frame_bundle_provisioning_smoke,
    run_n5_n4_live_multimodal_digest_provisioning_smoke,
    verify_n5_n4_live_frame_bundle_provisioning_smoke,
    verify_n5_n4_live_multimodal_digest_provisioning_smoke,
)
from .full_flow_provisioning_catalog import (
    CATALOG_NAME as PROVISIONING_CATALOG_NAME,
    PROVISIONING_CATALOG_SCHEMA_VERSION,
    PROVISIONING_ENTRY_SCHEMA_VERSION,
    verify_full_flow_provisioning_catalog,
)
from .n4_derived_data_plane import (
    FRAME_BUNDLE_REPRESENTATION_ID,
    MULTIMODAL_DIGEST_REPRESENTATION_ID,
    PACKAGE_MANIFEST_NAME as N4_PACKAGE_MANIFEST_NAME,
    verify_n4_publication_receipt,
)
from .n5_digest_materialization import (
    CHECKSUMS_NAME as N5_DIGEST_CHECKSUMS_NAME,
    PLAN_NAME as N5_DIGEST_PLAN_NAME,
    verify_n5_multimodal_digest_plan,
)
from .n5_materialization import (
    N5MaterializationHttpClientConfig,
    verify_n5_materialization_plan,
)


SOURCE_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.full-flow-live-provisioning-source-manifest/v1alpha1"
)
SOURCE_MAPPING_SCHEMA_VERSION = (
    "pathfinder.full-flow-live-provisioning-source-mapping/v1alpha1"
)
_LEGACY_BULK_RUN_SCHEMA_VERSION = (
    "pathfinder.full-flow-bulk-live-provisioning/v1alpha1"
)
BULK_RUN_SCHEMA_VERSION = (
    "pathfinder.full-flow-bulk-live-provisioning/v1alpha2"
)
_LEGACY_JOURNAL_SCHEMA_VERSION = (
    "pathfinder.full-flow-bulk-live-provisioning-journal/v1alpha1"
)
JOURNAL_SCHEMA_VERSION = (
    "pathfinder.full-flow-bulk-live-provisioning-journal/v1alpha2"
)
_LEGACY_CHECKPOINT_SCHEMA_VERSION = (
    "pathfinder.full-flow-bulk-live-provisioning-checkpoint/v1alpha1"
)
CHECKPOINT_SCHEMA_VERSION = (
    "pathfinder.full-flow-bulk-live-provisioning-checkpoint/v1alpha2"
)
_LEGACY_AGGREGATE_RECEIPT_SCHEMA_VERSION = (
    "pathfinder.full-flow-bulk-live-provisioning-receipt/v1alpha1"
)
AGGREGATE_RECEIPT_SCHEMA_VERSION = (
    "pathfinder.full-flow-bulk-live-provisioning-receipt/v1alpha2"
)

JOURNAL_NAME = "bulk-live-provisioning-journal.jsonl"
RECEIPTS_DIRECTORY_NAME = "receipts"
CHECKPOINTS_DIRECTORY_NAME = "checkpoints"
FINAL_DIRECTORY_NAME = "final"
AGGREGATE_RECEIPT_NAME = "bulk-live-provisioning-receipt.json"
LIVE_RECEIPT_BINDINGS_NAME = "live-receipt-bindings.json"
FINAL_CHECKSUMS_NAME = "SHA256SUMS"

_REPRESENTATION_ORDER = (
    FRAME_BUNDLE_REPRESENTATION_ID,
    MULTIMODAL_DIGEST_REPRESENTATION_ID,
)
_RECEIPT_BY_REPRESENTATION = {
    FRAME_BUNDLE_REPRESENTATION_ID: RECEIPT_NAME,
    MULTIMODAL_DIGEST_REPRESENTATION_ID: DIGEST_RECEIPT_NAME,
}
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._|:+-]{0,255}\Z")
_SENSITIVE_KEY = re.compile(
    r"(?:^|_)(?:api_?key|bearer|password|secret|token)(?:$|_)",
    re.IGNORECASE,
)

_SOURCE_MANIFEST_FIELDS = {
    "schema_version",
    "provisioning_catalog_sha256",
    "object_count",
    "entries",
    "credentials_recorded",
}
_SOURCE_ENTRY_FIELDS = {
    "object_id",
    "source_video_path",
    "source_video_size_bytes",
    "source_video_sha256",
    "frame_plan_path",
    "frame_plan_file_sha256",
    "frame_plan_sha256",
    "digest_plan_dir",
    "digest_plan_file_sha256",
    "digest_plan_checksums_file_sha256",
    "digest_plan_sha256",
}
_SOURCE_MAPPING_FIELDS = {
    "schema_version",
    "entries",
    "credentials_recorded",
}
_SOURCE_MAPPING_ENTRY_FIELDS = {
    "object_id",
    "source_video_path",
    "frame_plan_path",
    "digest_plan_dir",
}
_CATALOG_FIELDS = {
    "schema_version",
    "status",
    "catalog_id",
    "artifact_binding_set_sha256",
    "n4_package_manifest_sha256",
    "n4_package_sha256",
    "entry_count",
    "entries",
    "all_artifacts_available",
    "live_materialization_executed",
    "live_materialization_cost_measured",
    "credentials_recorded",
    "eligible_for_scientific_claims",
    "catalog_sha256",
}
_CATALOG_ENTRY_FIELDS = {
    "schema_version",
    "chain_id",
    "logical_object_id",
    "artifact_identity",
    "artifact_identity_sha256",
    "n5_evidence_sha256",
    "n4_publication_sha256",
    "evidence_class",
    "available",
    "live_materialization_executed",
    "live_materialization_cost_measured",
    "entry_sha256",
}
_IDENTITY_FIELDS = {
    "object_id",
    "representation_id",
    "artifact_sha256",
    "artifact_size_bytes",
    "object_catalog_version",
}
_JOURNAL_FIELDS = {
    "schema_version",
    "sequence",
    "previous_entry_sha256",
    "state",
    "run_id",
    "context_sha256",
    "operation_index",
    "object_id",
    "representation_id",
    "failure_class",
    "failure_code",
    "resumable",
    "receipt_relative_dir",
    "receipt_file_sha256",
    "n4_committed_catalog_version",
    "entry_sha256",
}
_CHECKPOINT_FIELDS = {
    "schema_version",
    "run_id",
    "context_sha256",
    "operation_index",
    "object_id",
    "representation_id",
    "expected_artifact_size_bytes",
    "expected_artifact_sha256",
    "source_video_sha256",
    "plan_sha256",
    "n4_access_plan_ids",
    "receipt_relative_dir",
    "receipt_file_sha256",
    "n4_previous_catalog_version",
    "n4_committed_catalog_version",
    "n4_generation_id",
    "n4_package_sha256",
    "n5_idempotent_replay",
    "n4_idempotent_replay",
    "crash_window_receipt_adopted",
    "checkpoint_sha256",
}
_LEGACY_CHECKPOINT_FIELDS = _CHECKPOINT_FIELDS - {"n4_access_plan_ids"}
_FINAL_FILES = {
    AGGREGATE_RECEIPT_NAME,
    LIVE_RECEIPT_BINDINGS_NAME,
    FINAL_CHECKSUMS_NAME,
}
_RESUMABLE_FAILURE_CLASS = "infrastructure"
_FAILURE_CLASSES = {"infrastructure", "semantic", "data", "internal"}
_EXPLICIT_N4_ACCESS_PLAN_IDS = "explicit"
_DEFAULT_N4_ACCESS_PLAN_IDS = "n5-materialization-plan-default"


class FullFlowBulkLiveProvisioningError(RuntimeError):
    """Raised when a bulk provisioning run cannot be trusted."""


class LiveProvisioningOperationFailure(RuntimeError):
    """A safe, classified executor failure.

    ``failure_code`` is deliberately an identifier rather than a raw error
    message: upstream HTTP errors can contain endpoints or credentials.
    """

    failure_class = "internal"
    resumable = False

    def __init__(self, failure_code: str) -> None:
        self.failure_code = _identifier(failure_code, "failure_code")
        super().__init__(self.failure_code)


class InfrastructureProvisioningFailure(LiveProvisioningOperationFailure):
    failure_class = "infrastructure"
    resumable = True


class SemanticProvisioningFailure(LiveProvisioningOperationFailure):
    failure_class = "semantic"


class DataProvisioningFailure(LiveProvisioningOperationFailure):
    failure_class = "data"


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowBulkLiveProvisioningError(message)


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
        raise FullFlowBulkLiveProvisioningError(
            "bulk provisioning value is not canonical JSON"
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
        raise FullFlowBulkLiveProvisioningError(
            "bulk provisioning value is not JSON"
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


def _integer(value: Any, name: str, *, minimum: int = 0) -> int:
    _require(
        type(value) is int and value >= minimum,
        f"{name} must be an integer >= {minimum}",
    )
    return int(value)


def _strict_json_bytes(payload: bytes, name: str) -> Any:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            _require(key not in value, f"{name} repeats key {key}")
            value[key] = item
        return value

    try:
        return json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=unique,
            parse_constant=lambda token: (_ for _ in ()).throw(
                FullFlowBulkLiveProvisioningError(
                    f"{name} contains invalid number {token}"
                )
            ),
        )
    except FullFlowBulkLiveProvisioningError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowBulkLiveProvisioningError(
            f"cannot parse {name}"
        ) from exc


def _strict_json_file(path: Path, name: str) -> Any:
    _require(path.is_file() and not path.is_symlink(), f"{name} is missing")
    try:
        return _strict_json_bytes(path.read_bytes(), name)
    except OSError as exc:
        raise FullFlowBulkLiveProvisioningError(
            f"cannot read {name}"
        ) from exc


def _assert_public(value: Any, name: str = "output") -> None:
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key)
            _require(
                _SENSITIVE_KEY.search(key) is None,
                f"{name} contains credential-shaped key {key}",
            )
            if "reasoning" in key.casefold():
                _require(
                    child is False,
                    f"{name} contains LLM reasoning",
                )
            _assert_public(child, f"{name}.{key}")
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for index, child in enumerate(value):
            _assert_public(child, f"{name}[{index}]")
    elif isinstance(value, str):
        _require(
            "://" not in value and not value.casefold().startswith("bearer "),
            f"{name} contains an endpoint or credential",
        )


def _safe_local_path(value: Any, manifest_root: Path, name: str) -> Path:
    _require(
        isinstance(value, str)
        and bool(value)
        and value == value.strip()
        and all(character not in value for character in "\r\n\x00")
        and "://" not in value,
        f"{name} is not a local path",
    )
    raw = Path(value)
    return (raw if raw.is_absolute() else manifest_root / raw).resolve()


def _write_atomic_file(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{os.getpid()}.tmp"
    _require(not temporary.exists(), "stale atomic-write temporary exists")
    try:
        with temporary.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _file_inventory(root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(
        (item for item in root.rglob("*") if item.is_file()),
        key=lambda item: item.relative_to(root).as_posix(),
    ):
        _require(not path.is_symlink(), "output inventory contains a symlink")
        payload = path.read_bytes()
        rows.append({
            "relative_path": path.relative_to(root).as_posix(),
            "size_bytes": len(payload),
            "sha256": _sha256(payload),
        })
    return rows


@dataclass(frozen=True)
class BulkProvisioningOperation:
    """One deterministic invocation of an existing one-object smoke."""

    run_id: str
    operation_index: int
    object_id: str
    representation_id: str
    source_video_path: Path = field(repr=False)
    source_video_size_bytes: int
    source_video_sha256: str
    plan_sha256: str
    expected_artifact_size_bytes: int
    expected_artifact_sha256: str
    n4_access_plan_ids: tuple[str, ...]
    n4_access_plan_ids_source: str
    frame_plan: Mapping[str, Any] | None = field(default=None, repr=False)
    digest_plan_dir: Path | None = field(default=None, repr=False)
    smoke_id: str = ""
    request_id: str = ""
    publication_id: str = ""
    package_id: str = ""
    catalog_version: str = ""
    expected_current_catalog_version: str | None = None
    output_dir: Path = field(default_factory=Path)


class FrameBundleBulkExecutor(Protocol):
    def execute(self, operation: BulkProvisioningOperation) -> Mapping[str, Any]:
        """Run the existing one-object frame-bundle provisioning smoke."""


class DigestBulkExecutor(Protocol):
    def execute(self, operation: BulkProvisioningOperation) -> Mapping[str, Any]:
        """Run the existing one-object digest provisioning smoke."""


def _classify_existing_failure(
    exc: Exception,
    *,
    digest: bool,
) -> LiveProvisioningOperationFailure:
    text = str(exc).casefold()
    if any(
        marker in text
        for marker in (
            "unreachable",
            "http ",
            "timeout",
            "timed out",
            "connection",
            "identity provider",
            "service unavailable",
        )
    ):
        return InfrastructureProvisioningFailure(
            "local-service-or-control-plane-unavailable"
        )
    if digest and any(
        marker in text
        for marker in ("vision", "model", "semantic", "digest generation")
    ):
        return SemanticProvisioningFailure("semantic-generation-failed")
    return DataProvisioningFailure("content-or-contract-verification-failed")


@dataclass(frozen=True)
class ExistingFrameBundleBulkExecutor:
    """Production adapter around the one-object frame provisioning API."""

    n5_config: N5MaterializationHttpClientConfig = field(repr=False)
    n4_config: N4PublicationHttpClientConfig = field(repr=False)

    def execute(self, operation: BulkProvisioningOperation) -> Mapping[str, Any]:
        _require(
            operation.representation_id == FRAME_BUNDLE_REPRESENTATION_ID
            and operation.frame_plan is not None,
            "frame executor received the wrong operation",
        )
        try:
            return run_n5_n4_live_frame_bundle_provisioning_smoke(
                operation.frame_plan,
                operation.source_video_path.read_bytes(),
                n5_config=self.n5_config,
                n4_config=self.n4_config,
                smoke_id=operation.smoke_id,
                publication_id=operation.publication_id,
                package_id=operation.package_id,
                catalog_version=operation.catalog_version,
                expected_current_catalog_version=(
                    operation.expected_current_catalog_version
                ),
                n4_access_plan_ids=operation.n4_access_plan_ids,
                output_dir=operation.output_dir,
            )
        except Exception as exc:
            raise _classify_existing_failure(exc, digest=False) from None


@dataclass(frozen=True)
class ExistingDigestBulkExecutor:
    """Production adapter around the one-object digest provisioning API."""

    n5_executor: N5DigestExecutor = field(repr=False)
    n4_config: N4PublicationHttpClientConfig = field(repr=False)

    def execute(self, operation: BulkProvisioningOperation) -> Mapping[str, Any]:
        _require(
            operation.representation_id == MULTIMODAL_DIGEST_REPRESENTATION_ID
            and operation.digest_plan_dir is not None,
            "digest executor received the wrong operation",
        )
        try:
            return run_n5_n4_live_multimodal_digest_provisioning_smoke(
                operation.digest_plan_dir,
                operation.source_video_path,
                n5_executor=self.n5_executor,
                n4_config=self.n4_config,
                smoke_id=operation.smoke_id,
                request_id=operation.request_id,
                publication_id=operation.publication_id,
                package_id=operation.package_id,
                catalog_version=operation.catalog_version,
                expected_current_catalog_version=(
                    operation.expected_current_catalog_version
                ),
                n4_access_plan_ids=operation.n4_access_plan_ids,
                output_dir=operation.output_dir,
            )
        except Exception as exc:
            raise _classify_existing_failure(exc, digest=True) from None


@dataclass(frozen=True)
class _SourceRow:
    object_id: str
    source_video_path: Path = field(repr=False)
    source_video_size_bytes: int
    source_video_sha256: str
    frame_plan_path: Path = field(repr=False)
    frame_plan_file_sha256: str
    frame_plan_sha256: str
    frame_plan: dict[str, Any] = field(repr=False)
    digest_plan_dir: Path = field(repr=False)
    digest_plan_file_sha256: str
    digest_plan_checksums_file_sha256: str
    digest_plan_sha256: str
    digest_plan_id: str


@dataclass(frozen=True)
class _RunContext:
    schema_version: str
    journal_schema_version: str
    checkpoint_schema_version: str
    aggregate_receipt_schema_version: str
    run_id: str
    catalog_sha256: str
    catalog_file_sha256: str
    source_manifest_file_sha256: str
    source_rows: Mapping[str, _SourceRow] = field(repr=False)
    required: tuple[dict[str, Any], ...]
    operations: tuple[BulkProvisioningOperation, ...]
    context_sha256: str


def _catalog_required(
    catalog_root: Path,
    artifact_binding_dir: Path,
    n4_package_dir: Path,
) -> tuple[str, str, tuple[dict[str, Any], ...]]:
    try:
        verified = verify_full_flow_provisioning_catalog(
            catalog_root,
            artifact_binding_dir=artifact_binding_dir,
            n4_package_dir=n4_package_dir,
        )
    except Exception as exc:
        raise FullFlowBulkLiveProvisioningError(
            "frozen provisioning catalog failed source verification"
        ) from exc
    path = catalog_root / PROVISIONING_CATALOG_NAME
    value = _strict_json_file(path, "provisioning catalog")
    _require(
        isinstance(value, dict) and set(value) == _CATALOG_FIELDS,
        "provisioning catalog field set changed",
    )
    _require(
        value.get("schema_version") == PROVISIONING_CATALOG_SCHEMA_VERSION
        and value.get("status")
        == "FROZEN_PREPROVISIONED_DERIVED_ARTIFACTS"
        and value.get("catalog_sha256") == verified.get("catalog_sha256")
        and value.get("all_artifacts_available") is True
        and value.get("live_materialization_executed") is False
        and value.get("live_materialization_cost_measured") is False
        and value.get("credentials_recorded") is False
        and value.get("eligible_for_scientific_claims") is False,
        "provisioning catalog safety contract changed",
    )
    entries = value.get("entries")
    _require(
        isinstance(entries, list)
        and len(entries) == value.get("entry_count")
        and bool(entries),
        "provisioning catalog entries are invalid",
    )
    required: list[dict[str, Any]] = []
    identities: set[tuple[str, str]] = set()
    for raw in entries:
        _require(
            isinstance(raw, dict) and set(raw) == _CATALOG_ENTRY_FIELDS,
            "provisioning catalog entry field set changed",
        )
        unsigned = dict(raw)
        supplied_entry_sha = _digest(
            unsigned.pop("entry_sha256", None), "catalog entry SHA-256"
        )
        _require(
            supplied_entry_sha == _sha256(_canonical(unsigned))
            and raw.get("schema_version")
            == PROVISIONING_ENTRY_SCHEMA_VERSION
            and raw.get("available") is True
            and raw.get("live_materialization_executed") is False
            and raw.get("live_materialization_cost_measured") is False,
            "provisioning catalog entry contract changed",
        )
        identity = raw.get("artifact_identity")
        _require(
            isinstance(identity, dict) and set(identity) == _IDENTITY_FIELDS,
            "catalog artifact identity changed",
        )
        object_id = _identifier(identity.get("object_id"), "object_id")
        representation_id = _identifier(
            identity.get("representation_id"), "representation_id"
        )
        _require(
            representation_id in _REPRESENTATION_ORDER,
            "provisioning catalog includes an unsupported representation",
        )
        key = (object_id, representation_id)
        _require(key not in identities, "required derived identity repeats")
        identities.add(key)
        size = _integer(
            identity.get("artifact_size_bytes"),
            "artifact_size_bytes",
            minimum=1,
        )
        artifact_sha = _digest(
            identity.get("artifact_sha256"), "artifact_sha256"
        )
        _identifier(
            identity.get("object_catalog_version"), "object_catalog_version"
        )
        _require(
            raw.get("artifact_identity_sha256") == _sha256(_canonical(identity)),
            "catalog artifact identity commitment failed",
        )
        required.append({
            "object_id": object_id,
            "representation_id": representation_id,
            "artifact_size_bytes": size,
            "artifact_sha256": artifact_sha,
            "catalog_entry_sha256": supplied_entry_sha,
        })
    object_ids = sorted({row[0] for row in identities})
    _require(
        identities
        == {
            (object_id, representation_id)
            for object_id in object_ids
            for representation_id in _REPRESENTATION_ORDER
        },
        "every catalog object must require both derived representations",
    )
    n4_manifest_path = n4_package_dir / N4_PACKAGE_MANIFEST_NAME
    n4_manifest_payload = (
        n4_manifest_path.read_bytes()
        if n4_manifest_path.is_file() and not n4_manifest_path.is_symlink()
        else b""
    )
    _require(
        bool(n4_manifest_payload)
        and _sha256(n4_manifest_payload)
        == value.get("n4_package_manifest_sha256"),
        "frozen N4 package manifest binding failed",
    )
    n4_manifest = _strict_json_bytes(
        n4_manifest_payload,
        "frozen N4 package manifest",
    )
    _require(
        isinstance(n4_manifest, dict),
        "frozen N4 package manifest must be an object",
    )
    n4_rows = n4_manifest.get("objects")
    _require(
        isinstance(n4_rows, list) and bool(n4_rows),
        "frozen N4 package has no artifact rows",
    )
    n4_bindings: dict[tuple[str, str], tuple[str, ...]] = {}
    n4_identities: dict[tuple[str, str], tuple[int, str]] = {}
    for raw in n4_rows:
        _require(
            isinstance(raw, dict),
            "frozen N4 package artifact row is invalid",
        )
        object_id = _identifier(raw.get("object_id"), "N4 object_id")
        representation_id = _identifier(
            raw.get("representation_id"),
            "N4 representation_id",
        )
        key = (object_id, representation_id)
        _require(
            key not in n4_bindings,
            "frozen N4 package repeats an artifact identity",
        )
        raw_plan_ids = raw.get("plan_ids")
        _require(
            isinstance(raw_plan_ids, list)
            and bool(raw_plan_ids)
            and all(isinstance(plan_id, str) for plan_id in raw_plan_ids)
            and raw_plan_ids == sorted(set(raw_plan_ids)),
            "frozen N4 access plan IDs are not canonical",
        )
        plan_ids = tuple(
            _identifier(plan_id, "N4 access plan_id")
            for plan_id in raw_plan_ids
        )
        n4_bindings[key] = plan_ids
        n4_identities[key] = (
            _integer(
                raw.get("artifact_size_bytes"),
                "N4 artifact_size_bytes",
                minimum=1,
            ),
            _digest(raw.get("artifact_sha256"), "N4 artifact_sha256"),
        )
    _require(
        set(n4_bindings) == identities,
        "frozen N4 package identity set differs from the provisioning catalog",
    )
    for representation_id in _REPRESENTATION_ORDER:
        binding_sets = {
            n4_bindings[key]
            for key in sorted(n4_bindings)
            if key[1] == representation_id
        }
        _require(
            len(binding_sets) == 1,
            "frozen N4 access plan bindings differ within a representation",
        )
    for row in required:
        key = (row["object_id"], row["representation_id"])
        _require(
            n4_identities[key]
            == (row["artifact_size_bytes"], row["artifact_sha256"]),
            "frozen N4 artifact identity differs from the provisioning catalog",
        )
        row["n4_access_plan_ids"] = list(n4_bindings[key])
    required.sort(
        key=lambda row: (
            row["object_id"],
            _REPRESENTATION_ORDER.index(row["representation_id"]),
        )
    )
    return (
        str(value["catalog_sha256"]),
        _sha256(path.read_bytes()),
        tuple(required),
    )


def freeze_full_flow_bulk_live_provisioning_source_manifest(
    provisioning_catalog_dir: str | Path,
    artifact_binding_dir: str | Path,
    n4_package_dir: str | Path,
    object_mapping_path: str | Path,
    *,
    output_path: str | Path,
) -> dict[str, Any]:
    """Compute the strict operator-local source manifest offline.

    The input mapping contains paths only.  This helper verifies every
    portable plan and source, computes all content hashes, and atomically
    freezes one no-overwrite manifest.  Neither the mapping nor any source
    bytes are copied into the output.
    """

    catalog_root = Path(provisioning_catalog_dir).resolve()
    binding_root = Path(artifact_binding_dir).resolve()
    n4_root = Path(n4_package_dir).resolve()
    mapping_path = Path(object_mapping_path).resolve()
    target = Path(output_path).resolve()
    catalog_sha, _catalog_file_sha, required = _catalog_required(
        catalog_root,
        binding_root,
        n4_root,
    )
    mapping = _strict_json_file(mapping_path, "operator-local source mapping")
    _require(
        isinstance(mapping, dict) and set(mapping) == _SOURCE_MAPPING_FIELDS,
        "source mapping field set changed",
    )
    _require(
        mapping.get("schema_version") == SOURCE_MAPPING_SCHEMA_VERSION
        and mapping.get("credentials_recorded") is False,
        "source mapping schema or safety contract changed",
    )
    _assert_public(mapping, "operator-local source mapping")
    raw_entries = mapping.get("entries")
    required_objects = sorted({row["object_id"] for row in required})
    _require(
        isinstance(raw_entries, list)
        and len(raw_entries) == len(required_objects),
        "source mapping object count differs from the catalog",
    )
    entries: list[dict[str, Any]] = []
    seen: set[str] = set()
    mapping_root = mapping_path.parent
    protected_paths = {
        catalog_root,
        binding_root,
        n4_root,
        mapping_path,
    }
    for raw in raw_entries:
        _require(
            isinstance(raw, dict) and set(raw) == _SOURCE_MAPPING_ENTRY_FIELDS,
            "source mapping entry field set changed",
        )
        object_id = _identifier(raw.get("object_id"), "mapping object_id")
        _require(object_id not in seen, "source mapping object repeats")
        seen.add(object_id)
        source = _safe_local_path(
            raw.get("source_video_path"), mapping_root, "source_video_path"
        )
        frame_path = _safe_local_path(
            raw.get("frame_plan_path"), mapping_root, "frame_plan_path"
        )
        digest_root = _safe_local_path(
            raw.get("digest_plan_dir"), mapping_root, "digest_plan_dir"
        )
        _require(
            source.is_file()
            and not source.is_symlink()
            and frame_path.is_file()
            and not frame_path.is_symlink()
            and digest_root.is_dir()
            and not digest_root.is_symlink(),
            "source mapping points to a missing or unsafe input",
        )
        source_payload = source.read_bytes()
        frame_payload = frame_path.read_bytes()
        frame_raw = _strict_json_bytes(frame_payload, "frame plan")
        _require(isinstance(frame_raw, dict), "frame plan must be an object")
        try:
            frame_plan = verify_n5_materialization_plan(frame_raw)
        except Exception as exc:
            raise FullFlowBulkLiveProvisioningError(
                "frame plan failed exact verification"
            ) from exc
        digest_path = digest_root / N5_DIGEST_PLAN_NAME
        digest_checksums = digest_root / N5_DIGEST_CHECKSUMS_NAME
        _require(
            digest_path.is_file()
            and not digest_path.is_symlink()
            and digest_checksums.is_file()
            and not digest_checksums.is_symlink(),
            "digest plan directory is incomplete",
        )
        try:
            digest_plan = verify_n5_multimodal_digest_plan(digest_root, source)
        except Exception as exc:
            raise FullFlowBulkLiveProvisioningError(
                "digest plan failed exact source verification"
            ) from exc
        _require(
            frame_plan.get("input", {}).get("object_id") == object_id
            and digest_plan.get("object_id") == object_id,
            "source mapping object differs from a portable plan",
        )
        protected_paths.update({source, frame_path, digest_root})
        entries.append({
            "object_id": object_id,
            "source_video_path": str(source),
            "source_video_size_bytes": len(source_payload),
            "source_video_sha256": _sha256(source_payload),
            "frame_plan_path": str(frame_path),
            "frame_plan_file_sha256": _sha256(frame_payload),
            "frame_plan_sha256": _digest(
                frame_plan.get("plan_sha256"), "frame plan SHA-256"
            ),
            "digest_plan_dir": str(digest_root),
            "digest_plan_file_sha256": _sha256(digest_path.read_bytes()),
            "digest_plan_checksums_file_sha256": _sha256(
                digest_checksums.read_bytes()
            ),
            "digest_plan_sha256": _digest(
                digest_plan.get("plan_sha256"), "digest plan SHA-256"
            ),
        })
    _require(
        sorted(seen) == required_objects,
        "source mapping object set differs from the catalog",
    )
    entries.sort(key=lambda row: row["object_id"])
    document = {
        "schema_version": SOURCE_MANIFEST_SCHEMA_VERSION,
        "provisioning_catalog_sha256": catalog_sha,
        "object_count": len(entries),
        "entries": entries,
        "credentials_recorded": False,
    }
    _assert_public(document, "operator-local source manifest")
    _require(not target.exists(), "source manifest output already exists")
    for source_path in protected_paths:
        if source_path.is_dir():
            _require(
                target != source_path
                and not target.is_relative_to(source_path)
                and not source_path.is_relative_to(target),
                "source manifest output overlaps a verified source directory",
            )
        else:
            _require(
                target != source_path and not source_path.is_relative_to(target),
                "source manifest output overlaps a verified source file",
            )
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = target.parent / f".{target.name}.{os.getpid()}.tmp"
    _require(not staging.exists(), "stale source-manifest staging file exists")
    try:
        with staging.open("xb") as handle:
            handle.write(_json_bytes(document))
            handle.flush()
            os.fsync(handle.fileno())
        manifest_sha, verified_rows = _source_manifest(
            staging,
            catalog_sha256=catalog_sha,
            required=required,
        )
        _require(
            len(verified_rows) == len(entries),
            "generated source manifest verification is incomplete",
        )
        os.replace(staging, target)
    finally:
        staging.unlink(missing_ok=True)
    return {
        "status": "FROZEN",
        "schema_version": SOURCE_MANIFEST_SCHEMA_VERSION,
        "provisioning_catalog_sha256": catalog_sha,
        "object_count": len(entries),
        "required_derived_identity_count": len(required),
        "source_manifest_file_sha256": manifest_sha,
        "output_path": str(target),
        "source_bytes_copied": False,
        "runtime_configuration_recorded": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def _source_manifest(
    path: Path,
    *,
    catalog_sha256: str,
    required: tuple[dict[str, Any], ...],
) -> tuple[str, Mapping[str, _SourceRow]]:
    payload = path.read_bytes() if path.is_file() else b""
    _require(bool(payload) and not path.is_symlink(), "source manifest is missing")
    value = _strict_json_bytes(payload, "operator-local source manifest")
    _require(
        isinstance(value, dict) and set(value) == _SOURCE_MANIFEST_FIELDS,
        "source manifest field set changed",
    )
    _require(
        value.get("schema_version") == SOURCE_MANIFEST_SCHEMA_VERSION
        and value.get("provisioning_catalog_sha256") == catalog_sha256
        and value.get("credentials_recorded") is False,
        "source manifest binding or safety contract changed",
    )
    _assert_public(value, "operator-local source manifest")
    entries = value.get("entries")
    required_objects = sorted({row["object_id"] for row in required})
    required_by_identity = {
        (row["object_id"], row["representation_id"]): row
        for row in required
    }
    _require(
        isinstance(entries, list)
        and value.get("object_count") == len(required_objects)
        and len(entries) == len(required_objects),
        "source manifest object count differs from the catalog",
    )
    rows: dict[str, _SourceRow] = {}
    manifest_root = path.parent.resolve()
    for raw in entries:
        _require(
            isinstance(raw, dict) and set(raw) == _SOURCE_ENTRY_FIELDS,
            "source manifest entry field set changed",
        )
        object_id = _identifier(raw.get("object_id"), "source object_id")
        _require(object_id not in rows, "source manifest object repeats")
        source = _safe_local_path(
            raw.get("source_video_path"), manifest_root, "source_video_path"
        )
        _require(
            source.is_file() and not source.is_symlink(),
            "source video is missing or not a regular file",
        )
        source_payload = source.read_bytes()
        source_size = _integer(
            raw.get("source_video_size_bytes"),
            "source_video_size_bytes",
            minimum=1,
        )
        source_sha = _digest(
            raw.get("source_video_sha256"), "source_video_sha256"
        )
        _require(
            len(source_payload) == source_size
            and _sha256(source_payload) == source_sha,
            "source video content binding failed",
        )
        frame_path = _safe_local_path(
            raw.get("frame_plan_path"), manifest_root, "frame_plan_path"
        )
        frame_payload = (
            frame_path.read_bytes()
            if frame_path.is_file() and not frame_path.is_symlink()
            else b""
        )
        _require(bool(frame_payload), "frame plan is missing")
        _require(
            _sha256(frame_payload)
            == _digest(
                raw.get("frame_plan_file_sha256"),
                "frame_plan_file_sha256",
            ),
            "frame plan file binding failed",
        )
        frame_raw = _strict_json_bytes(frame_payload, "frame plan")
        _require(isinstance(frame_raw, dict), "frame plan must be an object")
        try:
            frame_plan = verify_n5_materialization_plan(frame_raw)
        except Exception as exc:
            raise FullFlowBulkLiveProvisioningError(
                "frame plan failed exact verification"
            ) from exc
        frame_plan_sha = _digest(
            raw.get("frame_plan_sha256"), "frame_plan_sha256"
        )
        _require(
            frame_plan.get("plan_sha256") == frame_plan_sha
            and frame_plan.get("input", {}).get("object_id") == object_id
            and frame_plan.get("input", {}).get("size_bytes") == source_size
            and frame_plan.get("input", {}).get("sha256") == source_sha,
            "frame plan differs from its object or source",
        )
        expected_frame = required_by_identity[
            (object_id, FRAME_BUNDLE_REPRESENTATION_ID)
        ]
        _require(
            frame_plan.get("expected_output", {}).get("artifact_size_bytes")
            == expected_frame["artifact_size_bytes"]
            and frame_plan.get("expected_output", {}).get("artifact_sha256")
            == expected_frame["artifact_sha256"],
            "frame plan output differs from the frozen provisioning catalog",
        )
        digest_root = _safe_local_path(
            raw.get("digest_plan_dir"), manifest_root, "digest_plan_dir"
        )
        digest_path = digest_root / N5_DIGEST_PLAN_NAME
        digest_checksums = digest_root / N5_DIGEST_CHECKSUMS_NAME
        _require(
            digest_root.is_dir()
            and not digest_root.is_symlink()
            and digest_path.is_file()
            and not digest_path.is_symlink()
            and digest_checksums.is_file()
            and not digest_checksums.is_symlink(),
            "digest plan directory is incomplete",
        )
        _require(
            _sha256(digest_path.read_bytes())
            == _digest(
                raw.get("digest_plan_file_sha256"),
                "digest_plan_file_sha256",
            )
            and _sha256(digest_checksums.read_bytes())
            == _digest(
                raw.get("digest_plan_checksums_file_sha256"),
                "digest_plan_checksums_file_sha256",
            ),
            "digest plan directory binding failed",
        )
        try:
            digest_verified = verify_n5_multimodal_digest_plan(
                digest_root, source
            )
        except Exception as exc:
            raise FullFlowBulkLiveProvisioningError(
                "digest plan failed exact source verification"
            ) from exc
        digest_plan_sha = _digest(
            raw.get("digest_plan_sha256"), "digest_plan_sha256"
        )
        digest_plan_id = _identifier(
            digest_verified.get("plan_id"), "digest plan_id"
        )
        _require(
            digest_verified.get("plan_sha256") == digest_plan_sha
            and digest_verified.get("object_id") == object_id
            and digest_verified.get("source_video_sha256") == source_sha,
            "digest plan differs from its object or source",
        )
        rows[object_id] = _SourceRow(
            object_id=object_id,
            source_video_path=source,
            source_video_size_bytes=source_size,
            source_video_sha256=source_sha,
            frame_plan_path=frame_path,
            frame_plan_file_sha256=str(raw["frame_plan_file_sha256"]),
            frame_plan_sha256=frame_plan_sha,
            frame_plan=dict(frame_plan),
            digest_plan_dir=digest_root,
            digest_plan_file_sha256=str(raw["digest_plan_file_sha256"]),
            digest_plan_checksums_file_sha256=str(
                raw["digest_plan_checksums_file_sha256"]
            ),
            digest_plan_sha256=digest_plan_sha,
            digest_plan_id=digest_plan_id,
        )
    _require(
        sorted(rows) == required_objects,
        "source manifest object set differs from the catalog",
    )
    return _sha256(payload), rows


def _run_context(
    provisioning_catalog_dir: Path,
    artifact_binding_dir: Path,
    n4_package_dir: Path,
    operator_source_manifest: Path,
    *,
    run_id: str,
    output_dir: Path,
    schema_version: str = BULK_RUN_SCHEMA_VERSION,
) -> _RunContext:
    run_id = _identifier(run_id, "run_id")
    _require(
        schema_version
        in {BULK_RUN_SCHEMA_VERSION, _LEGACY_BULK_RUN_SCHEMA_VERSION},
        "bulk run schema version is unsupported",
    )
    legacy = schema_version == _LEGACY_BULK_RUN_SCHEMA_VERSION
    journal_schema_version = (
        _LEGACY_JOURNAL_SCHEMA_VERSION if legacy else JOURNAL_SCHEMA_VERSION
    )
    checkpoint_schema_version = (
        _LEGACY_CHECKPOINT_SCHEMA_VERSION
        if legacy
        else CHECKPOINT_SCHEMA_VERSION
    )
    aggregate_receipt_schema_version = (
        _LEGACY_AGGREGATE_RECEIPT_SCHEMA_VERSION
        if legacy
        else AGGREGATE_RECEIPT_SCHEMA_VERSION
    )
    _require(
        len(run_id) <= 180,
        "run_id is too long for deterministic operation identifiers",
    )
    catalog_sha, catalog_file_sha, required = _catalog_required(
        provisioning_catalog_dir,
        artifact_binding_dir,
        n4_package_dir,
    )
    source_manifest_sha, sources = _source_manifest(
        operator_source_manifest,
        catalog_sha256=catalog_sha,
        required=required,
    )
    context_required = tuple(
        {
            key: value
            for key, value in row.items()
            if not legacy or key != "n4_access_plan_ids"
        }
        for row in required
    )
    protected_directories = {
        provisioning_catalog_dir,
        artifact_binding_dir,
        n4_package_dir,
        *(row.digest_plan_dir for row in sources.values()),
    }
    protected_files = {
        operator_source_manifest,
        *(row.source_video_path for row in sources.values()),
        *(row.frame_plan_path for row in sources.values()),
    }
    for source_dir in protected_directories:
        _require(
            output_dir != source_dir
            and not output_dir.is_relative_to(source_dir)
            and not source_dir.is_relative_to(output_dir),
            "bulk run output overlaps a verified source directory",
        )
    for source_file in protected_files:
        _require(
            output_dir != source_file and not source_file.is_relative_to(output_dir),
            "bulk run output overlaps a verified source file",
        )
    source_commitments = [
        {
            "object_id": object_id,
            "source_video_size_bytes": row.source_video_size_bytes,
            "source_video_sha256": row.source_video_sha256,
            "frame_plan_file_sha256": row.frame_plan_file_sha256,
            "frame_plan_sha256": row.frame_plan_sha256,
            "digest_plan_file_sha256": row.digest_plan_file_sha256,
            "digest_plan_checksums_file_sha256": (
                row.digest_plan_checksums_file_sha256
            ),
            "digest_plan_sha256": row.digest_plan_sha256,
        }
        for object_id, row in sorted(sources.items())
    ]
    context_document = {
        "schema_version": schema_version,
        "run_id": run_id,
        "provisioning_catalog_sha256": catalog_sha,
        "provisioning_catalog_file_sha256": catalog_file_sha,
        "source_manifest_file_sha256": source_manifest_sha,
        "required_derived_identities": list(context_required),
        "source_content_commitments": source_commitments,
        "credentials_recorded": False,
    }
    context_sha = _sha256(_canonical(context_document))
    operations: list[BulkProvisioningOperation] = []
    previous_catalog: str | None = None
    for index, required_row in enumerate(required, start=1):
        representation = str(required_row["representation_id"])
        source = sources[str(required_row["object_id"])]
        short_kind = (
            "frames"
            if representation == FRAME_BUNDLE_REPRESENTATION_ID
            else "digest"
        )
        stem = f"{run_id}-{index:04d}-{short_kind}"
        plan_sha = (
            source.frame_plan_sha256
            if representation == FRAME_BUNDLE_REPRESENTATION_ID
            else source.digest_plan_sha256
        )
        if legacy:
            if representation == FRAME_BUNDLE_REPRESENTATION_ID:
                access_plan_ids = (
                    _identifier(
                        source.frame_plan.get("plan_id"), "frame plan_id"
                    ),
                )
            else:
                access_plan_ids = (source.digest_plan_id,)
            access_plan_ids_source = _DEFAULT_N4_ACCESS_PLAN_IDS
        else:
            access_plan_ids = tuple(required_row["n4_access_plan_ids"])
            access_plan_ids_source = _EXPLICIT_N4_ACCESS_PLAN_IDS
        catalog_version = f"{run_id}-n4-catalog-{index:04d}"
        operations.append(BulkProvisioningOperation(
            run_id=run_id,
            operation_index=index,
            object_id=source.object_id,
            representation_id=representation,
            source_video_path=source.source_video_path,
            source_video_size_bytes=source.source_video_size_bytes,
            source_video_sha256=source.source_video_sha256,
            plan_sha256=plan_sha,
            expected_artifact_size_bytes=int(
                required_row["artifact_size_bytes"]
            ),
            expected_artifact_sha256=str(required_row["artifact_sha256"]),
            n4_access_plan_ids=access_plan_ids,
            n4_access_plan_ids_source=access_plan_ids_source,
            frame_plan=(
                source.frame_plan
                if representation == FRAME_BUNDLE_REPRESENTATION_ID
                else None
            ),
            digest_plan_dir=(
                source.digest_plan_dir
                if representation == MULTIMODAL_DIGEST_REPRESENTATION_ID
                else None
            ),
            smoke_id=stem,
            request_id=f"{stem}-request",
            publication_id=f"{stem}-publication",
            package_id=f"{stem}-package",
            catalog_version=catalog_version,
            expected_current_catalog_version=previous_catalog,
            output_dir=(
                output_dir
                / RECEIPTS_DIRECTORY_NAME
                / f"{index:04d}-{source.object_id}-{short_kind}"
            ),
        ))
        previous_catalog = catalog_version
    return _RunContext(
        schema_version=schema_version,
        journal_schema_version=journal_schema_version,
        checkpoint_schema_version=checkpoint_schema_version,
        aggregate_receipt_schema_version=aggregate_receipt_schema_version,
        run_id=run_id,
        catalog_sha256=catalog_sha,
        catalog_file_sha256=catalog_file_sha,
        source_manifest_file_sha256=source_manifest_sha,
        source_rows=sources,
        required=context_required,
        operations=tuple(operations),
        context_sha256=context_sha,
    )


def _journal_row(
    context: _RunContext,
    *,
    sequence: int,
    previous: str | None,
    state: str,
    operation: BulkProvisioningOperation | None = None,
    failure_class: str | None = None,
    failure_code: str | None = None,
    resumable: bool = False,
    receipt_file_sha256: str | None = None,
    n4_committed_catalog_version: str | None = None,
) -> dict[str, Any]:
    if failure_class is not None:
        _require(failure_class in _FAILURE_CLASSES, "failure class is invalid")
        _identifier(failure_code, "failure_code")
    value: dict[str, Any] = {
        "schema_version": context.journal_schema_version,
        "sequence": sequence,
        "previous_entry_sha256": previous,
        "state": state,
        "run_id": context.run_id,
        "context_sha256": context.context_sha256,
        "operation_index": (
            operation.operation_index if operation is not None else None
        ),
        "object_id": operation.object_id if operation is not None else None,
        "representation_id": (
            operation.representation_id if operation is not None else None
        ),
        "failure_class": failure_class,
        "failure_code": failure_code,
        "resumable": resumable,
        "receipt_relative_dir": (
            operation.output_dir.parent.name + "/" + operation.output_dir.name
            if operation is not None and receipt_file_sha256 is not None
            else None
        ),
        "receipt_file_sha256": receipt_file_sha256,
        "n4_committed_catalog_version": n4_committed_catalog_version,
    }
    value["entry_sha256"] = _sha256(_canonical(value))
    return value


def _read_journal(path: Path, context: _RunContext) -> list[dict[str, Any]]:
    _require(path.is_file() and not path.is_symlink(), "journal is missing")
    try:
        lines = path.read_bytes().splitlines()
    except OSError as exc:
        raise FullFlowBulkLiveProvisioningError("cannot read journal") from exc
    _require(bool(lines), "journal is empty")
    rows: list[dict[str, Any]] = []
    previous: str | None = None
    for index, line in enumerate(lines, start=1):
        value = _strict_json_bytes(line, f"journal row {index}")
        _require(
            isinstance(value, dict)
            and set(value) == _JOURNAL_FIELDS
            and value.get("schema_version") == context.journal_schema_version
            and value.get("sequence") == index
            and value.get("previous_entry_sha256") == previous
            and value.get("run_id") == context.run_id
            and value.get("context_sha256") == context.context_sha256
            and type(value.get("resumable")) is bool,
            "journal row structure or binding changed",
        )
        supplied = _digest(value.get("entry_sha256"), "journal entry SHA-256")
        unsigned = dict(value)
        del unsigned["entry_sha256"]
        _require(
            supplied == _sha256(_canonical(unsigned)),
            "journal hash chain failed",
        )
        previous = supplied
        rows.append(value)
    _require(rows[0]["state"] == "RUN_STARTED", "journal has no run start")
    _validate_journal_semantics(rows, context)
    return rows


def _validate_journal_semantics(
    rows: Sequence[Mapping[str, Any]],
    context: _RunContext,
) -> None:
    allowed = {
        "RUN_STARTED",
        "OPERATION_INTENT",
        "OPERATION_COMPLETED",
        "RECEIPT_ADOPTED",
        "RUN_FAILED",
        "RUN_COMPLETED",
    }
    completed: set[int] = set()
    current_index = 1
    terminal_complete = False
    for position, row in enumerate(rows):
        state = row.get("state")
        _require(state in allowed, "journal state is unsupported")
        operation_index = row.get("operation_index")
        if state in {"RUN_STARTED", "RUN_COMPLETED"}:
            _require(
                operation_index is None
                and row.get("object_id") is None
                and row.get("representation_id") is None
                and row.get("failure_class") is None
                and row.get("failure_code") is None
                and row.get("resumable") is False
                and row.get("receipt_relative_dir") is None
                and row.get("receipt_file_sha256") is None
                and row.get("n4_committed_catalog_version") is None,
                "run-level journal row contains operation data",
            )
            _require(
                (state == "RUN_STARTED" and position == 0)
                or (
                    state == "RUN_COMPLETED"
                    and position == len(rows) - 1
                    and len(completed) == len(context.operations)
                ),
                "run-level journal state is out of order",
            )
            terminal_complete = state == "RUN_COMPLETED"
            continue
        _require(
            type(operation_index) is int
            and 1 <= operation_index <= len(context.operations),
            "journal operation index is invalid",
        )
        operation = context.operations[operation_index - 1]
        _require(
            row.get("object_id") == operation.object_id
            and row.get("representation_id") == operation.representation_id,
            "journal operation identity differs from the frozen order",
        )
        if state == "OPERATION_INTENT":
            _require(
                operation_index == current_index
                and row.get("failure_class") is None
                and row.get("failure_code") is None
                and row.get("resumable") is False
                and row.get("receipt_relative_dir") is None
                and row.get("receipt_file_sha256") is None
                and row.get("n4_committed_catalog_version") is None,
                "operation intent is invalid or out of order",
            )
        elif state == "RUN_FAILED":
            failure_class = row.get("failure_class")
            _require(
                operation_index == current_index
                and failure_class in _FAILURE_CLASSES
                and isinstance(row.get("failure_code"), str)
                and row.get("resumable")
                is (failure_class == _RESUMABLE_FAILURE_CLASS)
                and row.get("receipt_relative_dir") is None
                and row.get("receipt_file_sha256") is None
                and row.get("n4_committed_catalog_version") is None,
                "failure journal row is invalid",
            )
        else:
            _require(
                operation_index == current_index
                and operation_index not in completed
                and row.get("failure_class") is None
                and row.get("failure_code") is None
                and row.get("resumable") is False
                and row.get("receipt_relative_dir")
                == operation.output_dir.relative_to(
                    operation.output_dir.parents[1]
                ).as_posix()
                and isinstance(row.get("receipt_file_sha256"), str)
                and _SHA256.fullmatch(row["receipt_file_sha256"]) is not None
                and row.get("n4_committed_catalog_version")
                == operation.catalog_version,
                "completed-operation journal row is invalid or out of order",
            )
            completed.add(operation_index)
            current_index += 1
    _require(
        not terminal_complete or len(completed) == len(context.operations),
        "completed journal lacks all operations",
    )


def _append_journal(
    root: Path,
    context: _RunContext,
    *,
    state: str,
    operation: BulkProvisioningOperation | None = None,
    failure_class: str | None = None,
    failure_code: str | None = None,
    resumable: bool = False,
    receipt_file_sha256: str | None = None,
    n4_committed_catalog_version: str | None = None,
) -> dict[str, Any]:
    path = root / JOURNAL_NAME
    rows = _read_journal(path, context) if path.exists() else []
    value = _journal_row(
        context,
        sequence=len(rows) + 1,
        previous=rows[-1]["entry_sha256"] if rows else None,
        state=state,
        operation=operation,
        failure_class=failure_class,
        failure_code=failure_code,
        resumable=resumable,
        receipt_file_sha256=receipt_file_sha256,
        n4_committed_catalog_version=n4_committed_catalog_version,
    )
    payload = _canonical(value) + b"\n"
    with path.open("ab") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    return value


def _receipt_document(operation: BulkProvisioningOperation) -> dict[str, Any]:
    receipt_name = _RECEIPT_BY_REPRESENTATION[operation.representation_id]
    value = _strict_json_file(
        operation.output_dir / receipt_name,
        f"receipt for operation {operation.operation_index}",
    )
    _require(isinstance(value, dict), "one-object receipt is not an object")
    return value


def _verify_operation_receipt(
    operation: BulkProvisioningOperation,
) -> dict[str, Any]:
    try:
        if operation.representation_id == FRAME_BUNDLE_REPRESENTATION_ID:
            _require(operation.frame_plan is not None, "frame plan is missing")
            verify_n5_n4_live_frame_bundle_provisioning_smoke(
                operation.output_dir,
                n5_plan=operation.frame_plan,
                n4_access_plan_ids=operation.n4_access_plan_ids,
            )
        else:
            _require(
                operation.digest_plan_dir is not None,
                "digest plan directory is missing",
            )
            verify_n5_n4_live_multimodal_digest_provisioning_smoke(
                operation.output_dir,
                n5_digest_plan_dir=operation.digest_plan_dir,
                source_video_path=operation.source_video_path,
                n4_access_plan_ids=operation.n4_access_plan_ids,
            )
    except Exception as exc:
        raise FullFlowBulkLiveProvisioningError(
            "one-object live provisioning receipt failed exact verification"
        ) from exc
    document = _receipt_document(operation)
    receipt_name = _RECEIPT_BY_REPRESENTATION[operation.representation_id]
    receipt_payload = (operation.output_dir / receipt_name).read_bytes()
    n4 = verify_n4_publication_receipt(
        document.get("n4_publication_receipt")
    )
    published = n4.get("published_artifacts")
    if operation.n4_access_plan_ids_source == _EXPLICIT_N4_ACCESS_PLAN_IDS:
        access_contract_matches = (
            document.get("n4_access_plan_ids")
            == list(operation.n4_access_plan_ids)
            and document.get("n4_access_plan_ids_source")
            == _EXPLICIT_N4_ACCESS_PLAN_IDS
        )
    else:
        access_contract_matches = (
            "n4_access_plan_ids" not in document
            and "n4_access_plan_ids_source" not in document
        )
    _require(
        document.get("object_id") == operation.object_id
        and document.get("representation_id") == operation.representation_id
        and access_contract_matches
        and document.get("artifact_size_bytes")
        == operation.expected_artifact_size_bytes
        and document.get("artifact_sha256")
        == operation.expected_artifact_sha256
        and isinstance(published, list)
        and published
        == [{
            "object_id": operation.object_id,
            "representation_id": operation.representation_id,
            "artifact_size_bytes": operation.expected_artifact_size_bytes,
            "artifact_sha256": operation.expected_artifact_sha256,
        }]
        and document.get("n4_publication_id") == operation.publication_id
        and document.get("n4_previous_catalog_version")
        == operation.expected_current_catalog_version
        and document.get("n4_committed_catalog_version")
        == operation.catalog_version
        and document.get("n4_generation_id") == n4.get("generation_id")
        and document.get("n4_package_sha256") == n4.get("package_sha256")
        and n4.get("atomic_visibility") is True,
        "one-object receipt differs from the deterministic bulk operation",
    )
    if operation.representation_id == FRAME_BUNDLE_REPRESENTATION_ID:
        _require(
            document.get("n5_plan_sha256") == operation.plan_sha256,
            "frame receipt plan binding changed",
        )
        n5_replay = document.get("n5_materialization_idempotent_replay")
    else:
        _require(
            document.get("n5_digest_plan_sha256") == operation.plan_sha256,
            "digest receipt plan binding changed",
        )
        n5_replay = document.get(
            "n5_digest_materialization_idempotent_replay"
        )
    n4_replay = document.get("n4_publication_idempotent_replay")
    _require(
        type(n5_replay) is bool and type(n4_replay) is bool,
        "one-object replay accounting is invalid",
    )
    _require(
        (operation.output_dir / ONE_OBJECT_CHECKSUMS_NAME).is_file(),
        "one-object receipt checksums are missing",
    )
    return {
        "receipt_file_sha256": _sha256(receipt_payload),
        "n4_previous_catalog_version": n4["previous_catalog_version"],
        "n4_committed_catalog_version": n4["committed_catalog_version"],
        "n4_generation_id": n4["generation_id"],
        "n4_package_sha256": n4["package_sha256"],
        "n5_idempotent_replay": n5_replay,
        "n4_idempotent_replay": n4_replay,
    }


def _checkpoint_path(root: Path, operation: BulkProvisioningOperation) -> Path:
    return root / CHECKPOINTS_DIRECTORY_NAME / f"{operation.operation_index:04d}.json"


def _checkpoint_document(
    root: Path,
    context: _RunContext,
    operation: BulkProvisioningOperation,
    summary: Mapping[str, Any],
    *,
    adopted: bool,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "schema_version": context.checkpoint_schema_version,
        "run_id": context.run_id,
        "context_sha256": context.context_sha256,
        "operation_index": operation.operation_index,
        "object_id": operation.object_id,
        "representation_id": operation.representation_id,
        "expected_artifact_size_bytes": (
            operation.expected_artifact_size_bytes
        ),
        "expected_artifact_sha256": operation.expected_artifact_sha256,
        "source_video_sha256": operation.source_video_sha256,
        "plan_sha256": operation.plan_sha256,
        "receipt_relative_dir": operation.output_dir.relative_to(root).as_posix(),
        "receipt_file_sha256": summary["receipt_file_sha256"],
        "n4_previous_catalog_version": summary[
            "n4_previous_catalog_version"
        ],
        "n4_committed_catalog_version": summary[
            "n4_committed_catalog_version"
        ],
        "n4_generation_id": summary["n4_generation_id"],
        "n4_package_sha256": summary["n4_package_sha256"],
        "n5_idempotent_replay": summary["n5_idempotent_replay"],
        "n4_idempotent_replay": summary["n4_idempotent_replay"],
        "crash_window_receipt_adopted": adopted,
    }
    if context.schema_version != _LEGACY_BULK_RUN_SCHEMA_VERSION:
        value["n4_access_plan_ids"] = list(operation.n4_access_plan_ids)
    value["checkpoint_sha256"] = _sha256(_canonical(value))
    return value


def _write_checkpoint(
    root: Path,
    context: _RunContext,
    operation: BulkProvisioningOperation,
    summary: Mapping[str, Any],
    *,
    adopted: bool,
) -> dict[str, Any]:
    path = _checkpoint_path(root, operation)
    _require(not path.exists(), "operation checkpoint already exists")
    value = _checkpoint_document(
        root, context, operation, summary, adopted=adopted
    )
    _write_atomic_file(path, _json_bytes(value))
    return value


def _verify_checkpoint(
    root: Path,
    context: _RunContext,
    operation: BulkProvisioningOperation,
) -> dict[str, Any]:
    path = _checkpoint_path(root, operation)
    value = _strict_json_file(
        path, f"checkpoint {operation.operation_index}"
    )
    expected_fields = (
        _LEGACY_CHECKPOINT_FIELDS
        if context.schema_version == _LEGACY_BULK_RUN_SCHEMA_VERSION
        else _CHECKPOINT_FIELDS
    )
    _require(
        isinstance(value, dict)
        and set(value) == expected_fields
        and value.get("schema_version") == context.checkpoint_schema_version,
        "checkpoint field set changed",
    )
    supplied = _digest(
        value.get("checkpoint_sha256"), "checkpoint SHA-256"
    )
    unsigned = dict(value)
    del unsigned["checkpoint_sha256"]
    _require(
        supplied == _sha256(_canonical(unsigned)),
        "checkpoint digest failed",
    )
    summary = _verify_operation_receipt(operation)
    expected = _checkpoint_document(
        root,
        context,
        operation,
        summary,
        adopted=value.get("crash_window_receipt_adopted") is True,
    )
    _require(
        _json_bytes(value) == _json_bytes(expected),
        "checkpoint does not match its receipt or run context",
    )
    return value


def _checkpoint_set(root: Path, operation_count: int) -> set[int]:
    directory = root / CHECKPOINTS_DIRECTORY_NAME
    _require(
        directory.is_dir() and not directory.is_symlink(),
        "checkpoint directory is missing",
    )
    indices: set[int] = set()
    for path in directory.iterdir():
        _require(
            path.is_file()
            and not path.is_symlink()
            and re.fullmatch(r"[0-9]{4}\.json", path.name) is not None,
            "checkpoint directory contains an unexpected entry",
        )
        index = int(path.stem)
        _require(
            1 <= index <= operation_count and index not in indices,
            "checkpoint index is invalid or duplicated",
        )
        indices.add(index)
    if indices:
        _require(
            indices == set(range(1, max(indices) + 1)),
            "completed checkpoints are not a contiguous prefix",
        )
    return indices


def _binding_for_operation(operation: BulkProvisioningOperation) -> dict[str, Any]:
    if operation.representation_id == FRAME_BUNDLE_REPRESENTATION_ID:
        _require(operation.frame_plan is not None, "frame plan is missing")
        return {
            "kind": "frame_bundle",
            "receipt_dir": str(operation.output_dir),
            "n5_plan": dict(operation.frame_plan),
        }
    _require(operation.digest_plan_dir is not None, "digest plan is missing")
    return {
        "kind": "multimodal_digest",
        "receipt_dir": str(operation.output_dir),
        "n5_digest_plan_dir": str(operation.digest_plan_dir),
        "source_video_path": str(operation.source_video_path),
    }


def _state_inventory(root: Path) -> list[dict[str, Any]]:
    rows = _file_inventory(root / RECEIPTS_DIRECTORY_NAME)
    receipt_rows = [
        {
            **row,
            "relative_path": f"{RECEIPTS_DIRECTORY_NAME}/{row['relative_path']}",
        }
        for row in rows
    ]
    checkpoint_rows = [
        {
            **row,
            "relative_path": f"{CHECKPOINTS_DIRECTORY_NAME}/{row['relative_path']}",
        }
        for row in _file_inventory(root / CHECKPOINTS_DIRECTORY_NAME)
    ]
    journal = root / JOURNAL_NAME
    journal_payload = journal.read_bytes()
    return sorted(
        receipt_rows
        + checkpoint_rows
        + [{
            "relative_path": JOURNAL_NAME,
            "size_bytes": len(journal_payload),
            "sha256": _sha256(journal_payload),
        }],
        key=lambda row: row["relative_path"],
    )


def _n4_access_plan_ids_by_representation(
    context: _RunContext,
) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for representation_id in _REPRESENTATION_ORDER:
        binding_sets = {
            operation.n4_access_plan_ids
            for operation in context.operations
            if operation.representation_id == representation_id
        }
        _require(
            len(binding_sets) == 1,
            "bulk N4 access plan bindings differ within a representation",
        )
        result[representation_id] = list(next(iter(binding_sets)))
    return result


def _aggregate_document(
    root: Path,
    context: _RunContext,
    checkpoints: Sequence[Mapping[str, Any]],
    descriptor_bytes: bytes,
) -> dict[str, Any]:
    state_inventory = _state_inventory(root)
    replay_count = sum(
        1
        for row in checkpoints
        if row["n5_idempotent_replay"]
        or row["n4_idempotent_replay"]
        or row["crash_window_receipt_adopted"]
    )
    value: dict[str, Any] = {
        "schema_version": context.aggregate_receipt_schema_version,
        "status": "COMPLETE",
        "evidence_class": "local-bulk-data-protocol-conformance",
        "run_id": context.run_id,
        "context_sha256": context.context_sha256,
        "provisioning_catalog_sha256": context.catalog_sha256,
        "provisioning_catalog_file_sha256": context.catalog_file_sha256,
        "operator_source_manifest_file_sha256": (
            context.source_manifest_file_sha256
        ),
        "object_count": len(context.source_rows),
        "required_derived_identity_count": len(context.operations),
        "completed_operation_count": len(checkpoints),
        "frame_bundle_operation_count": sum(
            row["representation_id"] == FRAME_BUNDLE_REPRESENTATION_ID
            for row in checkpoints
        ),
        "multimodal_digest_operation_count": sum(
            row["representation_id"]
            == MULTIMODAL_DIGEST_REPRESENTATION_ID
            for row in checkpoints
        ),
        "durable_replay_adopted_operation_count": replay_count,
        "fresh_operation_count": len(checkpoints) - replay_count,
        "final_n4_catalog_version": checkpoints[-1][
            "n4_committed_catalog_version"
        ],
        "final_n4_generation_id": checkpoints[-1]["n4_generation_id"],
        "final_n4_package_sha256": checkpoints[-1]["n4_package_sha256"],
        "live_receipt_bindings_file_sha256": _sha256(descriptor_bytes),
        "state_file_count": len(state_inventory),
        "state_file_inventory_sha256": _sha256(_canonical(state_inventory)),
        "state_file_inventory": state_inventory,
        "all_required_derived_identities_provisioned_exactly_once": True,
        "n4_compare_and_swap_chain_contiguous": True,
        "one_object_receipts_individually_verified": True,
        "live_receipt_bindings_ready_for_n4_live_serve_gate": True,
        "performance_measured": False,
        "monetary_cost_measured": False,
        "cloud_network_measured": False,
        "upcloud_used": False,
        "credentials_recorded": False,
        "raw_video_recorded": False,
        "llm_reasoning_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    if context.schema_version != _LEGACY_BULK_RUN_SCHEMA_VERSION:
        value["n4_access_plan_ids_by_representation"] = (
            _n4_access_plan_ids_by_representation(context)
        )
    value["aggregate_receipt_sha256"] = _sha256(_canonical(value))
    return value


def _write_final(
    root: Path,
    context: _RunContext,
    checkpoints: Sequence[Mapping[str, Any]],
) -> None:
    final = root / FINAL_DIRECTORY_NAME
    _require(not final.exists(), "final aggregate directory already exists")
    descriptor = [
        _binding_for_operation(operation) for operation in context.operations
    ]
    _assert_public(descriptor, "live receipt bindings")
    descriptor_bytes = _json_bytes(descriptor)
    aggregate = _aggregate_document(root, context, checkpoints, descriptor_bytes)
    _assert_public(aggregate, "aggregate receipt")
    aggregate_bytes = _json_bytes(aggregate)
    checksums = b"".join(
        f"{_sha256(payload)}  {name}\n".encode("utf-8")
        for name, payload in sorted({
            AGGREGATE_RECEIPT_NAME: aggregate_bytes,
            LIVE_RECEIPT_BINDINGS_NAME: descriptor_bytes,
        }.items())
    )
    parent = Path(tempfile.mkdtemp(prefix=".bulk-final-", dir=root))
    stage = parent / FINAL_DIRECTORY_NAME
    try:
        stage.mkdir()
        (stage / AGGREGATE_RECEIPT_NAME).write_bytes(aggregate_bytes)
        (stage / LIVE_RECEIPT_BINDINGS_NAME).write_bytes(descriptor_bytes)
        (stage / FINAL_CHECKSUMS_NAME).write_bytes(checksums)
        os.replace(stage, final)
    finally:
        shutil.rmtree(parent, ignore_errors=True)


def _verify_final_files(
    root: Path,
    context: _RunContext,
    checkpoints: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    final = root / FINAL_DIRECTORY_NAME
    _require(final.is_dir() and not final.is_symlink(), "final aggregate is missing")
    actual = {path.name for path in final.iterdir()}
    _require(
        actual == _FINAL_FILES
        and all(path.is_file() and not path.is_symlink() for path in final.iterdir()),
        "final aggregate file set changed",
    )
    descriptor_bytes = (final / LIVE_RECEIPT_BINDINGS_NAME).read_bytes()
    descriptor = _strict_json_bytes(descriptor_bytes, "live receipt bindings")
    expected_descriptor = [
        _binding_for_operation(operation) for operation in context.operations
    ]
    _require(
        isinstance(descriptor, list)
        and descriptor_bytes == _json_bytes(expected_descriptor),
        "live receipt bindings differ from the verified operations",
    )
    aggregate_bytes = (final / AGGREGATE_RECEIPT_NAME).read_bytes()
    aggregate = _strict_json_bytes(aggregate_bytes, "aggregate receipt")
    expected_aggregate = _aggregate_document(
        root, context, checkpoints, descriptor_bytes
    )
    _require(
        isinstance(aggregate, dict)
        and aggregate_bytes == _json_bytes(expected_aggregate),
        "aggregate receipt differs from durable run state",
    )
    expected_checksums = b"".join(
        f"{_sha256(payload)}  {name}\n".encode("utf-8")
        for name, payload in sorted({
            AGGREGATE_RECEIPT_NAME: aggregate_bytes,
            LIVE_RECEIPT_BINDINGS_NAME: descriptor_bytes,
        }.items())
    )
    _require(
        (final / FINAL_CHECKSUMS_NAME).read_bytes() == expected_checksums,
        "final aggregate checksums failed",
    )
    _assert_public(aggregate, "aggregate receipt")
    _assert_public(descriptor, "live receipt bindings")
    return aggregate


def _prepare_root(root: Path, context: _RunContext, *, resume: bool) -> None:
    if root.exists():
        _require(resume, "bulk provisioning output already exists; use resume")
        _require(root.is_dir() and not root.is_symlink(), "run output is unsafe")
        rows = _read_journal(root / JOURNAL_NAME, context)
        terminal = rows[-1]
        if terminal["state"] == "RUN_FAILED":
            _require(
                terminal["failure_class"] == _RESUMABLE_FAILURE_CLASS
                and terminal["resumable"] is True,
                "only infrastructure failures may be resumed",
            )
        elif terminal["state"] != "RUN_COMPLETED":
            _require(
                terminal["state"] in {
                    "OPERATION_INTENT",
                    "OPERATION_COMPLETED",
                    "RECEIPT_ADOPTED",
                },
                "run journal has an unsupported terminal state",
            )
        return
    _require(not resume, "cannot resume a missing bulk provisioning run")
    root.mkdir(parents=True)
    (root / RECEIPTS_DIRECTORY_NAME).mkdir()
    (root / CHECKPOINTS_DIRECTORY_NAME).mkdir()
    _append_journal(root, context, state="RUN_STARTED")


def _bulk_schema_and_run_id_from_journal(path: Path) -> tuple[str, str]:
    _require(path.is_file() and not path.is_symlink(), "journal is missing")
    try:
        first = path.read_bytes().splitlines()[0]
    except (OSError, IndexError) as exc:
        raise FullFlowBulkLiveProvisioningError("journal is empty") from exc
    value = _strict_json_bytes(first, "journal start")
    _require(
        isinstance(value, dict)
        and set(value) == _JOURNAL_FIELDS
        and value.get("sequence") == 1
        and value.get("state") == "RUN_STARTED",
        "journal start is invalid",
    )
    journal_schema = value.get("schema_version")
    by_journal_schema = {
        JOURNAL_SCHEMA_VERSION: BULK_RUN_SCHEMA_VERSION,
        _LEGACY_JOURNAL_SCHEMA_VERSION: _LEGACY_BULK_RUN_SCHEMA_VERSION,
    }
    _require(
        journal_schema in by_journal_schema,
        "journal schema version is unsupported",
    )
    return (
        by_journal_schema[str(journal_schema)],
        _identifier(value.get("run_id"), "run_id"),
    )


def run_full_flow_bulk_live_provisioning(
    provisioning_catalog_dir: str | Path,
    artifact_binding_dir: str | Path,
    n4_package_dir: str | Path,
    operator_source_manifest: str | Path,
    *,
    frame_executor: FrameBundleBulkExecutor,
    digest_executor: DigestBulkExecutor,
    run_id: str,
    output_dir: str | Path,
    resume: bool = False,
) -> dict[str, Any]:
    """Provision every required derived identity with durable recovery."""

    root = Path(output_dir).resolve()
    if resume and root.exists() and (root / JOURNAL_NAME).exists():
        existing_schema, _existing_run_id = (
            _bulk_schema_and_run_id_from_journal(root / JOURNAL_NAME)
        )
        _require(
            existing_schema == BULK_RUN_SCHEMA_VERSION,
            "legacy v1alpha1 bulk evidence is verify-only; start a new "
            "v1alpha2 run",
        )
    context = _run_context(
        Path(provisioning_catalog_dir).resolve(),
        Path(artifact_binding_dir).resolve(),
        Path(n4_package_dir).resolve(),
        Path(operator_source_manifest).resolve(),
        run_id=run_id,
        output_dir=root,
    )
    _prepare_root(root, context, resume=resume)
    rows = _read_journal(root / JOURNAL_NAME, context)
    if rows[-1]["state"] == "RUN_COMPLETED":
        if not (root / FINAL_DIRECTORY_NAME).exists():
            checkpoints = [
                _verify_checkpoint(root, context, operation)
                for operation in context.operations
            ]
            _write_final(root, context, checkpoints)
        return verify_full_flow_bulk_live_provisioning(
            root,
            provisioning_catalog_dir=provisioning_catalog_dir,
            artifact_binding_dir=artifact_binding_dir,
            n4_package_dir=n4_package_dir,
            operator_source_manifest=operator_source_manifest,
        )

    existing = _checkpoint_set(root, len(context.operations))
    for operation in context.operations:
        checkpoint_path = _checkpoint_path(root, operation)
        if operation.operation_index in existing:
            _verify_checkpoint(root, context, operation)
            continue
        _require(
            not any(
                index > operation.operation_index for index in existing
            ),
            "a later checkpoint exists before the current operation",
        )
        if operation.output_dir.exists():
            summary = _verify_operation_receipt(operation)
            checkpoint = _write_checkpoint(
                root, context, operation, summary, adopted=True
            )
            _append_journal(
                root,
                context,
                state="RECEIPT_ADOPTED",
                operation=operation,
                receipt_file_sha256=checkpoint["receipt_file_sha256"],
                n4_committed_catalog_version=checkpoint[
                    "n4_committed_catalog_version"
                ],
            )
            existing.add(operation.operation_index)
            continue
        _append_journal(
            root, context, state="OPERATION_INTENT", operation=operation
        )
        executor: FrameBundleBulkExecutor | DigestBulkExecutor = (
            frame_executor
            if operation.representation_id == FRAME_BUNDLE_REPRESENTATION_ID
            else digest_executor
        )
        try:
            executor.execute(operation)
        except LiveProvisioningOperationFailure as exc:
            _append_journal(
                root,
                context,
                state="RUN_FAILED",
                operation=operation,
                failure_class=exc.failure_class,
                failure_code=exc.failure_code,
                resumable=exc.resumable,
            )
            raise FullFlowBulkLiveProvisioningError(
                f"bulk provisioning stopped after {exc.failure_class} failure; "
                f"failure_code={exc.failure_code}"
            ) from None
        except Exception as exc:
            _append_journal(
                root,
                context,
                state="RUN_FAILED",
                operation=operation,
                failure_class="internal",
                failure_code="unclassified-executor-failure",
                resumable=False,
            )
            raise FullFlowBulkLiveProvisioningError(
                "bulk provisioning stopped after an unclassified executor failure"
            ) from None
        try:
            summary = _verify_operation_receipt(operation)
        except Exception as exc:
            _append_journal(
                root,
                context,
                state="RUN_FAILED",
                operation=operation,
                failure_class="data",
                failure_code="one-object-receipt-verification-failed",
                resumable=False,
            )
            raise FullFlowBulkLiveProvisioningError(
                "bulk provisioning stopped after data failure; "
                "failure_code=one-object-receipt-verification-failed"
            ) from None
        checkpoint = _write_checkpoint(
            root, context, operation, summary, adopted=False
        )
        _append_journal(
            root,
            context,
            state="OPERATION_COMPLETED",
            operation=operation,
            receipt_file_sha256=checkpoint["receipt_file_sha256"],
            n4_committed_catalog_version=checkpoint[
                "n4_committed_catalog_version"
            ],
        )
        existing.add(operation.operation_index)

    checkpoints = [
        _verify_checkpoint(root, context, operation)
        for operation in context.operations
    ]
    _append_journal(root, context, state="RUN_COMPLETED")
    _write_final(root, context, checkpoints)
    return verify_full_flow_bulk_live_provisioning(
        root,
        provisioning_catalog_dir=provisioning_catalog_dir,
        artifact_binding_dir=artifact_binding_dir,
        n4_package_dir=n4_package_dir,
        operator_source_manifest=operator_source_manifest,
    )


def verify_full_flow_bulk_live_provisioning(
    output_dir: str | Path,
    *,
    provisioning_catalog_dir: str | Path,
    artifact_binding_dir: str | Path,
    n4_package_dir: str | Path,
    operator_source_manifest: str | Path,
) -> dict[str, Any]:
    """Reproduce a complete bulk result from frozen and local sources."""

    root = Path(output_dir).resolve()
    _require(root.is_dir() and not root.is_symlink(), "bulk run is missing")
    _require(
        {path.name for path in root.iterdir()}
        == {
            JOURNAL_NAME,
            RECEIPTS_DIRECTORY_NAME,
            CHECKPOINTS_DIRECTORY_NAME,
            FINAL_DIRECTORY_NAME,
        },
        "bulk run top-level file set changed",
    )
    schema_version, run_id = _bulk_schema_and_run_id_from_journal(
        root / JOURNAL_NAME
    )
    context = _run_context(
        Path(provisioning_catalog_dir).resolve(),
        Path(artifact_binding_dir).resolve(),
        Path(n4_package_dir).resolve(),
        Path(operator_source_manifest).resolve(),
        run_id=run_id,
        output_dir=root,
        schema_version=schema_version,
    )
    rows = _read_journal(root / JOURNAL_NAME, context)
    _require(rows[-1]["state"] == "RUN_COMPLETED", "bulk run is incomplete")
    _require(
        _checkpoint_set(root, len(context.operations))
        == set(range(1, len(context.operations) + 1)),
        "bulk run lacks an exact complete checkpoint set",
    )
    receipts_root = root / RECEIPTS_DIRECTORY_NAME
    expected_receipt_dirs = {operation.output_dir for operation in context.operations}
    actual_receipt_dirs = {
        path.resolve()
        for path in receipts_root.iterdir()
        if path.is_dir() and not path.is_symlink()
    }
    _require(
        actual_receipt_dirs == expected_receipt_dirs
        and all(
            path.is_dir() and not path.is_symlink()
            for path in receipts_root.iterdir()
        ),
        "receipt directory set differs from the exact operation set",
    )
    checkpoints = [
        _verify_checkpoint(root, context, operation)
        for operation in context.operations
    ]
    previous: str | None = None
    identities: set[tuple[str, str]] = set()
    for operation, checkpoint in zip(context.operations, checkpoints):
        identity = (operation.object_id, operation.representation_id)
        _require(identity not in identities, "completed derived identity repeats")
        identities.add(identity)
        _require(
            checkpoint["n4_previous_catalog_version"] == previous
            and checkpoint["n4_committed_catalog_version"]
            == operation.catalog_version,
            "completed N4 compare-and-swap chain is not contiguous",
        )
        previous = str(checkpoint["n4_committed_catalog_version"])
    aggregate = _verify_final_files(root, context, checkpoints)
    failure_rows = [row for row in rows if row["state"] == "RUN_FAILED"]
    result = {
        "status": "VERIFIED",
        "run_id": run_id,
        "object_count": aggregate["object_count"],
        "required_derived_identity_count": aggregate[
            "required_derived_identity_count"
        ],
        "completed_operation_count": aggregate["completed_operation_count"],
        "frame_bundle_operation_count": aggregate[
            "frame_bundle_operation_count"
        ],
        "multimodal_digest_operation_count": aggregate[
            "multimodal_digest_operation_count"
        ],
        "durable_replay_adopted_operation_count": aggregate[
            "durable_replay_adopted_operation_count"
        ],
        "infrastructure_failure_count": sum(
            row["failure_class"] == "infrastructure" for row in failure_rows
        ),
        "semantic_failure_count": sum(
            row["failure_class"] == "semantic" for row in failure_rows
        ),
        "data_failure_count": sum(
            row["failure_class"] == "data" for row in failure_rows
        ),
        "final_n4_catalog_version": aggregate[
            "final_n4_catalog_version"
        ],
        "final_n4_generation_id": aggregate["final_n4_generation_id"],
        "live_receipt_bindings_path": str(
            root / FINAL_DIRECTORY_NAME / LIVE_RECEIPT_BINDINGS_NAME
        ),
        "source_binding_checked": True,
        "all_required_derived_identities_provisioned_exactly_once": True,
        "n4_compare_and_swap_chain_contiguous": True,
        "performance_measured": False,
        "monetary_cost_measured": False,
        "cloud_network_measured": False,
        "upcloud_used": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    if schema_version != _LEGACY_BULK_RUN_SCHEMA_VERSION:
        result["n4_access_plan_ids_by_representation"] = aggregate[
            "n4_access_plan_ids_by_representation"
        ]
    return result


__all__ = [
    "AGGREGATE_RECEIPT_NAME",
    "AGGREGATE_RECEIPT_SCHEMA_VERSION",
    "BULK_RUN_SCHEMA_VERSION",
    "BulkProvisioningOperation",
    "CHECKPOINTS_DIRECTORY_NAME",
    "CHECKPOINT_SCHEMA_VERSION",
    "DataProvisioningFailure",
    "DigestBulkExecutor",
    "ExistingDigestBulkExecutor",
    "ExistingFrameBundleBulkExecutor",
    "FINAL_CHECKSUMS_NAME",
    "FINAL_DIRECTORY_NAME",
    "FrameBundleBulkExecutor",
    "FullFlowBulkLiveProvisioningError",
    "InfrastructureProvisioningFailure",
    "JOURNAL_NAME",
    "JOURNAL_SCHEMA_VERSION",
    "LIVE_RECEIPT_BINDINGS_NAME",
    "LiveProvisioningOperationFailure",
    "RECEIPTS_DIRECTORY_NAME",
    "SOURCE_MANIFEST_SCHEMA_VERSION",
    "SOURCE_MAPPING_SCHEMA_VERSION",
    "SemanticProvisioningFailure",
    "freeze_full_flow_bulk_live_provisioning_source_manifest",
    "run_full_flow_bulk_live_provisioning",
    "verify_full_flow_bulk_live_provisioning",
]
