"""Portable N4 derived-representation storage and atomic publication.

The package built here is a normal Pathfinder Data Agent manifest/catalog
pair backed by immutable artifact bytes.  It deliberately contains only
logical identities and package-relative paths: endpoint selection belongs to
deployment, not to the data package.

``N4DerivedRepresentationStore`` adds a small durable publication layer for
N5.  Every publication first creates and verifies a complete immutable
generation.  A SQLite transaction then changes the single current-generation
pointer.  Readers therefore observe either the old complete catalog or the
new complete catalog, never a partially copied artifact/catalog combination.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import tempfile
import unicodedata
import uuid
from contextlib import closing
from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Mapping, Sequence

from ..data_agent_manifest import (
    DATA_AGENT_MANIFEST_VERSION,
    DATA_OBJECT_CATALOG_VERSION,
    load_data_agent_manifest,
)
from ..frame_bundle import REPRESENTATION_ID as FRAME_BUNDLE_REPRESENTATION_ID
from ..frame_bundle_ingest import (
    DEFAULT_FRAME_BUNDLE_LIMITS,
    FRAME_BUNDLE_MEDIA_TYPE,
    FrameBundleLimits,
    validate_frame_bundle_bytes,
)


N4_DERIVED_PACKAGE_SCHEMA_VERSION = (
    "pathfinder.simulator-n4-derived-data-package/v1alpha1"
)
N4_DERIVED_BINDINGS_SCHEMA_VERSION = (
    "pathfinder.simulator-n4-derived-bindings/v1alpha1"
)
N4_DERIVED_PACKAGE_STATUS = "FROZEN_N4_DERIVED_DATA_PACKAGE"
N4_DERIVED_PACKAGE_VERIFIED_STATUS = "VERIFIED_N4_DERIVED_DATA_PACKAGE"
N4_PUBLICATION_RECEIPT_SCHEMA_VERSION = (
    "pathfinder.simulator-n4-publication-receipt/v1alpha1"
)
N4_PUBLICATION_STATUS = "COMMITTED"

N4_LOGICAL_NODE_ID = "N4"
N4_LOGICAL_LOCATION = "origin-warm"
N5_LOGICAL_NODE_ID = "N5"
MULTIMODAL_DIGEST_REPRESENTATION_ID = "multimodal_digest"
MULTIMODAL_DIGEST_MEDIA_TYPE = "text/plain; charset=utf-8"
SUPPORTED_REPRESENTATIONS = frozenset({
    FRAME_BUNDLE_REPRESENTATION_ID,
    MULTIMODAL_DIGEST_REPRESENTATION_ID,
})

PACKAGE_MANIFEST_NAME = "n4-derived-data-package.json"
DATA_AGENT_MANIFEST_PATH = "config/data-agent-manifest.json"
OBJECT_CATALOG_PATH = "config/object-catalog.json"
CHECKSUMS_NAME = "SHA256SUMS"
STORE_DATABASE_NAME = "n4-publications.sqlite3"
GENERATIONS_DIRECTORY_NAME = "generations"

_MAX_DIGEST_BYTES = 4 * 1024 * 1024
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._|:-]{0,511}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_NODE_ID = re.compile(r"N[1-8]\Z")
_URL = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://")
_SENSITIVE_KEY = re.compile(
    r"(?:^|_)(?:api_?key|bearer|password|secret|token)(?:$|_)",
    re.IGNORECASE,
)
_DEPLOYMENT_KEY = re.compile(
    r"(?:^|_)(?:endpoint|host|hostname|port|url)(?:$|_)",
    re.IGNORECASE,
)
_MEASUREMENT_KEY = re.compile(
    r"(?:^|_)(?:cost|latency|duration|elapsed)(?:$|_)",
    re.IGNORECASE,
)

_PROVENANCE_KEYS = frozenset({
    "schema_version",
    "producer_node_id",
    "publication_source_id",
    "source_representation_id",
    "source_content_sha256",
    "derivation_id",
    "derivation_sha256",
})
_PROVENANCE_SCHEMA_VERSION = (
    "pathfinder.simulator-derived-artifact-provenance/v1alpha1"
)


class N4DerivedDataPlaneError(RuntimeError):
    """Raised when an N4 package or publication cannot be trusted."""


class N4PublicationConflict(N4DerivedDataPlaneError):
    """Raised on idempotency or compare-and-swap conflicts."""


@dataclass(frozen=True)
class N4ArtifactProvenance:
    """Endpoint-free lineage for one derived artifact.

    ``publication_source_id`` is normally the N5 idempotency key or evidence
    ID.  ``derivation_sha256`` binds the frozen transformation contract, while
    ``source_content_sha256`` binds the exact input representation.
    """

    producer_node_id: str
    publication_source_id: str
    source_representation_id: str
    source_content_sha256: str
    derivation_id: str
    derivation_sha256: str

    def to_dict(self) -> dict[str, Any]:
        value = {
            "schema_version": _PROVENANCE_SCHEMA_VERSION,
            "producer_node_id": _node_id(
                self.producer_node_id,
                "provenance.producer_node_id",
            ),
            "publication_source_id": _identifier(
                self.publication_source_id,
                "provenance.publication_source_id",
            ),
            "source_representation_id": _identifier(
                self.source_representation_id,
                "provenance.source_representation_id",
            ),
            "source_content_sha256": _digest(
                self.source_content_sha256,
                "provenance.source_content_sha256",
            ),
            "derivation_id": _identifier(
                self.derivation_id,
                "provenance.derivation_id",
            ),
            "derivation_sha256": _digest(
                self.derivation_sha256,
                "provenance.derivation_sha256",
            ),
        }
        _assert_portable_metadata(value, "provenance")
        return value

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "N4ArtifactProvenance":
        _require(
            isinstance(value, Mapping) and set(value) == _PROVENANCE_KEYS,
            "artifact provenance field set changed",
        )
        _require(
            value.get("schema_version") == _PROVENANCE_SCHEMA_VERSION,
            "artifact provenance schema changed",
        )
        result = cls(
            producer_node_id=value.get("producer_node_id"),
            publication_source_id=value.get("publication_source_id"),
            source_representation_id=value.get("source_representation_id"),
            source_content_sha256=value.get("source_content_sha256"),
            derivation_id=value.get("derivation_id"),
            derivation_sha256=value.get("derivation_sha256"),
        )
        _require(
            result.to_dict() == dict(value),
            "artifact provenance is not canonical",
        )
        return result


@dataclass(frozen=True)
class N4DerivedArtifactInput:
    """Bytes and frozen routing/lineage metadata for one N4 artifact."""

    object_id: str
    representation_id: str
    artifact_bytes: bytes
    plan_ids: tuple[str, ...]
    provenance: N4ArtifactProvenance
    expected_sha256: str | None = None
    expected_size_bytes: int | None = None

    @classmethod
    def from_path(
        cls,
        *,
        object_id: str,
        representation_id: str,
        artifact_path: str | Path,
        plan_ids: Sequence[str],
        provenance: N4ArtifactProvenance,
        expected_sha256: str | None = None,
        expected_size_bytes: int | None = None,
    ) -> "N4DerivedArtifactInput":
        try:
            raw = Path(artifact_path).read_bytes()
        except OSError as exc:
            raise N4DerivedDataPlaneError(
                f"cannot read artifact for {object_id}/{representation_id}"
            ) from exc
        return cls(
            object_id=object_id,
            representation_id=representation_id,
            artifact_bytes=raw,
            plan_ids=tuple(plan_ids),
            provenance=provenance,
            expected_sha256=expected_sha256,
            expected_size_bytes=expected_size_bytes,
        )


@dataclass(frozen=True)
class N4PublishedSnapshot:
    """One immutable, fully verified Data Agent generation."""

    generation_id: str
    package_sha256: str
    catalog_version: str
    package_dir: Path
    data_agent_manifest_path: Path
    object_catalog_path: Path


@dataclass(frozen=True)
class N4PublicationResult:
    """Durable publication receipt plus the immutable committed snapshot."""

    receipt: dict[str, Any]
    snapshot: N4PublishedSnapshot
    idempotent_replay: bool


def _require(condition: object, message: str) -> None:
    if not condition:
        raise N4DerivedDataPlaneError(message)


def _identifier(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
        f"{name} is invalid",
    )
    return value


def _node_id(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _NODE_ID.fullmatch(value) is not None,
        f"{name} is not a logical Pathfinder node ID",
    )
    return value


def _digest(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{name} is not a lowercase SHA-256 digest",
    )
    return value


def _positive_integer(value: Any, name: str) -> int:
    _require(
        isinstance(value, int) and not isinstance(value, bool) and value > 0,
        f"{name} must be a positive integer",
    )
    return value


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _json_bytes(value: Any) -> bytes:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise N4DerivedDataPlaneError(
            "value cannot be serialized as canonical JSON"
        ) from exc


def _compact_json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise N4DerivedDataPlaneError(
            "value cannot be serialized as canonical JSON"
        ) from exc


def _strict_json(raw: bytes, name: str) -> dict[str, Any]:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, child in pairs:
            if key in result:
                raise N4DerivedDataPlaneError(
                    f"{name} contains duplicate key {key!r}"
                )
            result[key] = child
        return result

    def reject_constant(value: str) -> None:
        raise N4DerivedDataPlaneError(
            f"{name} contains non-finite number {value}"
        )

    try:
        value = json.loads(
            raw,
            object_pairs_hook=reject_duplicates,
            parse_constant=reject_constant,
        )
    except N4DerivedDataPlaneError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise N4DerivedDataPlaneError(f"{name} is not valid JSON") from exc
    _require(isinstance(value, dict), f"{name} must be a JSON object")
    return value


def _relative_path(value: Any, name: str) -> str:
    _require(isinstance(value, str) and bool(value), f"{name} is invalid")
    path = PurePosixPath(value)
    _require(not path.is_absolute(), f"{name} must be package-relative")
    _require("\\" not in value, f"{name} must use POSIX separators")
    _require(".." not in path.parts, f"{name} escapes the package")
    _require(str(path) == value, f"{name} is not canonical")
    return value


def _assert_portable_metadata(value: Any, name: str = "metadata") -> None:
    """Reject deployment, measurement, credential, and host-path material."""

    if isinstance(value, Mapping):
        for key, child in value.items():
            _require(isinstance(key, str), f"{name} has a non-string key")
            if key != "credentials_recorded":
                _require(
                    _SENSITIVE_KEY.search(key) is None,
                    f"{name} contains sensitive field {key!r}",
                )
            if key != "endpoint_free":
                _require(
                    _DEPLOYMENT_KEY.search(key) is None,
                    f"{name} contains deployment field {key!r}",
                )
            _require(
                _MEASUREMENT_KEY.search(key) is None,
                f"{name} contains measured/economic field {key!r}",
            )
            _assert_portable_metadata(child, f"{name}.{key}")
        return
    if isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            _assert_portable_metadata(child, f"{name}[{index}]")
        return
    if isinstance(value, str):
        _require(_URL.search(value) is None, f"{name} contains a URL")
        _require(
            not PurePosixPath(value).is_absolute()
            and not PureWindowsPath(value).is_absolute(),
            f"{name} contains an absolute host path",
        )


def _validate_digest_text(raw: bytes, object_id: str) -> tuple[str, int]:
    _require(bool(raw), f"digest for {object_id} is empty")
    _require(
        len(raw) <= _MAX_DIGEST_BYTES,
        f"digest for {object_id} exceeds {_MAX_DIGEST_BYTES} bytes",
    )
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise N4DerivedDataPlaneError(
            f"digest for {object_id} is not UTF-8"
        ) from exc
    _require("\x00" not in text, f"digest for {object_id} contains NUL")
    _require(bool(text.strip()), f"digest for {object_id} is blank")
    _require(
        unicodedata.normalize("NFC", text) == text,
        f"digest for {object_id} is not Unicode NFC",
    )
    return text, len(text)


def _artifact_filename(representation_id: str) -> str:
    if representation_id == FRAME_BUNDLE_REPRESENTATION_ID:
        return f"{FRAME_BUNDLE_REPRESENTATION_ID}.tar"
    if representation_id == MULTIMODAL_DIGEST_REPRESENTATION_ID:
        return f"{MULTIMODAL_DIGEST_REPRESENTATION_ID}.txt"
    raise N4DerivedDataPlaneError(
        f"unsupported N4 representation: {representation_id}"
    )


def _media_type(representation_id: str) -> str:
    if representation_id == FRAME_BUNDLE_REPRESENTATION_ID:
        return FRAME_BUNDLE_MEDIA_TYPE
    if representation_id == MULTIMODAL_DIGEST_REPRESENTATION_ID:
        return MULTIMODAL_DIGEST_MEDIA_TYPE
    raise N4DerivedDataPlaneError(
        f"unsupported N4 representation: {representation_id}"
    )


def _delivery_kind(representation_id: str) -> str:
    return (
        "artifact_uri"
        if representation_id == FRAME_BUNDLE_REPRESENTATION_ID
        else "inline_text"
    )


def _normalize_artifact(
    value: N4DerivedArtifactInput,
    limits: FrameBundleLimits,
) -> dict[str, Any]:
    _require(
        isinstance(value, N4DerivedArtifactInput),
        "artifact inputs must be N4DerivedArtifactInput values",
    )
    object_id = _identifier(value.object_id, "object_id")
    representation_id = _identifier(
        value.representation_id,
        "representation_id",
    )
    _require(
        representation_id in SUPPORTED_REPRESENTATIONS,
        f"unsupported N4 representation: {representation_id}",
    )
    _require(
        isinstance(value.artifact_bytes, bytes),
        f"artifact for {object_id}/{representation_id} must be bytes",
    )
    raw = value.artifact_bytes
    _require(bool(raw), f"artifact for {object_id}/{representation_id} is empty")
    digest = _sha256(raw)
    if value.expected_sha256 is not None:
        _require(
            _digest(value.expected_sha256, "expected_sha256") == digest,
            f"artifact SHA-256 mismatch for {object_id}/{representation_id}",
        )
    if value.expected_size_bytes is not None:
        _require(
            _positive_integer(
                value.expected_size_bytes,
                "expected_size_bytes",
            )
            == len(raw),
            f"artifact size mismatch for {object_id}/{representation_id}",
        )
    plan_ids = tuple(sorted({
        _identifier(plan_id, "plan_id") for plan_id in value.plan_ids
    }))
    _require(
        bool(plan_ids),
        f"artifact {object_id}/{representation_id} has no plan binding",
    )
    provenance = value.provenance.to_dict()
    representation_metadata: dict[str, Any]
    if representation_id == FRAME_BUNDLE_REPRESENTATION_ID:
        try:
            bundle = validate_frame_bundle_bytes(
                raw,
                expected_object_id=object_id,
                expected_sha256=digest,
                expected_size_bytes=len(raw),
                artifact_media_type=FRAME_BUNDLE_MEDIA_TYPE,
                limits=limits,
            )
        except Exception as exc:
            raise N4DerivedDataPlaneError(
                f"artifact for {object_id} is not a canonical frame bundle"
            ) from exc
        representation_metadata = {
            "frame_count": bundle.frame_count,
            "member_count": bundle.member_count,
            "manifest_sha256": bundle.manifest_sha256,
            "total_jpeg_bytes": bundle.total_jpeg_bytes,
        }
    else:
        _text, character_count = _validate_digest_text(raw, object_id)
        representation_metadata = {
            "utf8_character_count": character_count,
        }
    path = f"artifacts/{object_id}/{_artifact_filename(representation_id)}"
    return {
        "object_id": object_id,
        "representation_id": representation_id,
        "artifact_bytes": raw,
        "artifact_package_path": path,
        "artifact_size_bytes": len(raw),
        "artifact_sha256": digest,
        "media_type": _media_type(representation_id),
        "delivery_kind": _delivery_kind(representation_id),
        "plan_ids": plan_ids,
        "provenance": provenance,
        "representation_metadata": representation_metadata,
    }


def _data_agent_documents(
    records: Sequence[Mapping[str, Any]],
    catalog_version: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    representation_ids = sorted({
        record["representation_id"] for record in records
    })
    manifest_representations: dict[str, Any] = {}
    for representation_id in representation_ids:
        plan_ids = sorted({
            plan_id
            for record in records
            if record["representation_id"] == representation_id
            for plan_id in record["plan_ids"]
        })
        binding = {"location": N4_LOGICAL_LOCATION}
        manifest_representations[representation_id] = {
            "kind": _delivery_kind(representation_id),
            "media_type": _media_type(representation_id),
            "default_binding": dict(binding),
            "plan_bindings": {
                plan_id: dict(binding) for plan_id in plan_ids
            },
        }
    manifest = {
        "schema_version": DATA_AGENT_MANIFEST_VERSION,
        "node_id": N4_LOGICAL_NODE_ID,
        "require_plan_binding": True,
        "object_catalog_path": "object-catalog.json",
        "representations": manifest_representations,
    }
    objects: dict[str, Any] = {}
    for record in records:
        representation = {
            "path": "../" + record["artifact_package_path"],
            "plan_paths": {
                plan_id: "../" + record["artifact_package_path"]
                for plan_id in record["plan_ids"]
            },
        }
        objects.setdefault(
            record["object_id"],
            {"representations": {}},
        )["representations"][record["representation_id"]] = representation
    catalog = {
        "schema_version": DATA_OBJECT_CATALOG_VERSION,
        "catalog_version": catalog_version,
        "objects": objects,
    }
    return manifest, catalog


def _checksum_bytes(documents: Mapping[str, bytes]) -> bytes:
    return b"".join(
        f"{_sha256(payload)}  {name}\n".encode("utf-8")
        for name, payload in sorted(documents.items())
    )


def _fsync_directory(path: Path) -> None:
    """Best-effort directory durability on platforms that expose it."""

    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        # Windows does not provide a portable directory fsync operation.
        pass
    finally:
        os.close(descriptor)


def _fsync_tree(root: Path) -> None:
    for path in sorted(
        (candidate for candidate in root.rglob("*") if candidate.is_file()),
        key=lambda candidate: candidate.relative_to(root).as_posix(),
    ):
        # Windows' CRT rejects fsync on a read-only descriptor. Opening an
        # existing package file read/write does not alter it and keeps this
        # durability barrier portable.
        with path.open("r+b") as stream:
            os.fsync(stream.fileno())
    for path in sorted(
        (candidate for candidate in root.rglob("*") if candidate.is_dir()),
        key=lambda candidate: len(candidate.parts),
        reverse=True,
    ):
        _fsync_directory(path)
    _fsync_directory(root)


def build_n4_derived_data_package(
    artifacts: Sequence[N4DerivedArtifactInput],
    *,
    output_dir: str | Path,
    package_id: str,
    catalog_version: str,
    limits: FrameBundleLimits = DEFAULT_FRAME_BUNDLE_LIMITS,
) -> dict[str, Any]:
    """Build an immutable endpoint-free N4 Data Agent package."""

    package_id = _identifier(package_id, "package_id")
    catalog_version = _identifier(catalog_version, "catalog_version")
    _require(isinstance(limits, FrameBundleLimits), "limits is invalid")
    _require(bool(artifacts), "at least one N4 artifact is required")
    normalized = [_normalize_artifact(value, limits) for value in artifacts]
    keys = [
        (record["object_id"], record["representation_id"])
        for record in normalized
    ]
    _require(
        len(keys) == len(set(keys)),
        "N4 artifacts repeat an object/representation pair",
    )
    normalized.sort(key=lambda row: (row["object_id"], row["representation_id"]))
    for representation_id in sorted(SUPPORTED_REPRESENTATIONS):
        binding_sets = {
            tuple(row["plan_ids"])
            for row in normalized
            if row["representation_id"] == representation_id
        }
        _require(
            len(binding_sets) <= 1,
            "standard Data Agent plan bindings must be identical for every "
            f"{representation_id} object",
        )

    output = Path(output_dir).resolve()
    _require(not output.exists(), f"output directory already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f".{output.name}.", dir=output.parent))
    try:
        documents: dict[str, bytes] = {}
        report_records: list[dict[str, Any]] = []
        for record in normalized:
            path = record["artifact_package_path"]
            documents[path] = record["artifact_bytes"]
            report_record = {
                key: value
                for key, value in record.items()
                if key != "artifact_bytes"
            }
            report_record["plan_ids"] = list(report_record["plan_ids"])
            report_records.append(report_record)

        manifest, catalog = _data_agent_documents(
            report_records,
            catalog_version,
        )
        documents[DATA_AGENT_MANIFEST_PATH] = _json_bytes(manifest)
        documents[OBJECT_CATALOG_PATH] = _json_bytes(catalog)
        object_ids = sorted({row["object_id"] for row in report_records})
        representation_ids = sorted({
            row["representation_id"] for row in report_records
        })
        report = {
            "schema_version": N4_DERIVED_PACKAGE_SCHEMA_VERSION,
            "status": N4_DERIVED_PACKAGE_STATUS,
            "package_id": package_id,
            "logical_node_id": N4_LOGICAL_NODE_ID,
            "logical_location": N4_LOGICAL_LOCATION,
            "catalog_version": catalog_version,
            "data_agent_manifest_package_path": DATA_AGENT_MANIFEST_PATH,
            "object_catalog_package_path": OBJECT_CATALOG_PATH,
            "representation_ids": representation_ids,
            "object_count": len(object_ids),
            "artifact_count": len(report_records),
            "artifact_bytes": sum(
                row["artifact_size_bytes"] for row in report_records
            ),
            "objects": report_records,
            "endpoint_free": True,
            "atomic_publication_compatible": True,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }
        _assert_portable_metadata(report)
        documents[PACKAGE_MANIFEST_NAME] = _json_bytes(report)
        for relative, payload in documents.items():
            target = stage / Path(*PurePosixPath(relative).parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(payload)
        (stage / CHECKSUMS_NAME).write_bytes(_checksum_bytes(documents))
        verify_n4_derived_data_package(stage, limits=limits)
        _fsync_tree(stage)
        os.replace(stage, output)
        _fsync_directory(output.parent)
    except Exception:
        shutil.rmtree(stage, ignore_errors=True)
        raise
    result = verify_n4_derived_data_package(output, limits=limits)
    result["output_dir"] = str(output)
    return result


def build_n4_derived_data_package_from_manifest(
    binding_manifest: str | Path,
    *,
    output_dir: str | Path,
    limits: FrameBundleLimits = DEFAULT_FRAME_BUNDLE_LIMITS,
) -> dict[str, Any]:
    """Build an N4 package from an operator-only local path manifest.

    Artifact paths are resolved only while packaging and are never copied
    into the resulting portable package.  Every artifact must carry an
    expected digest and length, so a changed local file fails closed.
    """

    source = Path(binding_manifest).resolve()
    try:
        value = _strict_json(source.read_bytes(), "N4 binding manifest")
    except OSError as exc:
        raise N4DerivedDataPlaneError(
            "cannot read N4 binding manifest"
        ) from exc
    _require(
        set(value)
        == {
            "schema_version",
            "package_id",
            "catalog_version",
            "artifacts",
            "credentials_recorded",
        },
        "N4 binding manifest field set changed",
    )
    _require(
        value.get("schema_version") == N4_DERIVED_BINDINGS_SCHEMA_VERSION,
        "unsupported N4 binding manifest schema",
    )
    _require(
        value.get("credentials_recorded") is False,
        "N4 binding manifest records credentials",
    )
    package_id = _identifier(value.get("package_id"), "package_id")
    catalog_version = _identifier(
        value.get("catalog_version"),
        "catalog_version",
    )
    rows = value.get("artifacts")
    _require(isinstance(rows, list) and bool(rows), "N4 artifacts are missing")
    artifacts: list[N4DerivedArtifactInput] = []
    for position, row in enumerate(rows):
        _require(
            isinstance(row, dict)
            and set(row)
            == {
                "object_id",
                "representation_id",
                "artifact_path",
                "plan_ids",
                "expected_sha256",
                "expected_size_bytes",
                "provenance",
            },
            f"N4 artifact binding {position} fields changed",
        )
        raw_path = row.get("artifact_path")
        _require(
            isinstance(raw_path, str) and bool(raw_path),
            f"N4 artifact binding {position} path is invalid",
        )
        artifact_path = Path(raw_path)
        if not artifact_path.is_absolute():
            artifact_path = source.parent / artifact_path
        plan_ids = row.get("plan_ids")
        _require(
            isinstance(plan_ids, list)
            and bool(plan_ids)
            and all(isinstance(item, str) for item in plan_ids),
            f"N4 artifact binding {position} plan_ids are invalid",
        )
        expected_size = row.get("expected_size_bytes")
        _require(
            type(expected_size) is int and expected_size > 0,
            f"N4 artifact binding {position} size is invalid",
        )
        artifacts.append(
            N4DerivedArtifactInput.from_path(
                object_id=row.get("object_id"),
                representation_id=row.get("representation_id"),
                artifact_path=artifact_path,
                plan_ids=plan_ids,
                provenance=N4ArtifactProvenance.from_dict(
                    row.get("provenance")
                ),
                expected_sha256=_digest(
                    row.get("expected_sha256"),
                    f"N4 artifact binding {position} expected_sha256",
                ),
                expected_size_bytes=expected_size,
            )
        )
    return build_n4_derived_data_package(
        artifacts,
        output_dir=output_dir,
        package_id=package_id,
        catalog_version=catalog_version,
        limits=limits,
    )


def _read_checksums(root: Path) -> tuple[dict[str, str], bytes]:
    path = root / CHECKSUMS_NAME
    _require(path.is_file(), f"{CHECKSUMS_NAME} is missing")
    try:
        raw = path.read_bytes()
        lines = raw.decode("utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise N4DerivedDataPlaneError(f"{CHECKSUMS_NAME} is unreadable") from exc
    _require(bool(lines), f"{CHECKSUMS_NAME} is empty")
    checksums: dict[str, str] = {}
    previous = ""
    for line in lines:
        digest, separator, name = line.partition("  ")
        _require(bool(separator), f"{CHECKSUMS_NAME} has a malformed row")
        digest = _digest(digest, f"{CHECKSUMS_NAME} digest")
        name = _relative_path(name, f"{CHECKSUMS_NAME} path")
        _require(name > previous, f"{CHECKSUMS_NAME} is not sorted")
        _require(name not in checksums, f"{CHECKSUMS_NAME} repeats a path")
        checksums[name] = digest
        previous = name
    documents: dict[str, bytes] = {}
    for name in checksums:
        candidate = root / Path(*PurePosixPath(name).parts)
        _require(
            candidate.is_file() and not candidate.is_symlink(),
            f"checksummed path is missing or not a regular file: {name}",
        )
        documents[name] = candidate.read_bytes()
    _require(
        raw == _checksum_bytes(documents),
        f"{CHECKSUMS_NAME} is not canonical or disagrees with package bytes",
    )
    _require(
        not any(path.is_symlink() for path in root.rglob("*")),
        "N4 package contains a symbolic link",
    )
    actual = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() and path.name != CHECKSUMS_NAME
    }
    _require(
        actual == set(checksums),
        "package file set does not match SHA256SUMS",
    )
    for name, expected in checksums.items():
        path = root / Path(*PurePosixPath(name).parts)
        _require(path.is_file(), f"checksummed path is missing: {name}")
        _require(
            _sha256(documents[name]) == expected,
            f"checksum mismatch: {name}",
        )
    return checksums, raw


def _validated_report_record(
    root: Path,
    value: Any,
    *,
    limits: FrameBundleLimits,
) -> dict[str, Any]:
    required = {
        "object_id",
        "representation_id",
        "artifact_package_path",
        "artifact_size_bytes",
        "artifact_sha256",
        "media_type",
        "delivery_kind",
        "plan_ids",
        "provenance",
        "representation_metadata",
    }
    _require(
        isinstance(value, dict) and set(value) == required,
        "N4 artifact record field set changed",
    )
    object_id = _identifier(value.get("object_id"), "object_id")
    representation_id = _identifier(
        value.get("representation_id"),
        "representation_id",
    )
    _require(
        representation_id in SUPPORTED_REPRESENTATIONS,
        f"unsupported N4 representation: {representation_id}",
    )
    expected_path = (
        f"artifacts/{object_id}/{_artifact_filename(representation_id)}"
    )
    _require(
        _relative_path(
            value.get("artifact_package_path"),
            "artifact_package_path",
        )
        == expected_path,
        "N4 artifact path changed",
    )
    _require(
        value.get("media_type") == _media_type(representation_id)
        and value.get("delivery_kind") == _delivery_kind(representation_id),
        "N4 representation delivery contract changed",
    )
    size = _positive_integer(
        value.get("artifact_size_bytes"),
        "artifact_size_bytes",
    )
    expected_sha256 = _digest(
        value.get("artifact_sha256"),
        "artifact_sha256",
    )
    artifact_path = root / Path(*PurePosixPath(expected_path).parts)
    _require(artifact_path.is_file(), f"N4 artifact is missing: {expected_path}")
    raw = artifact_path.read_bytes()
    _require(
        len(raw) == size and _sha256(raw) == expected_sha256,
        f"N4 artifact content binding changed: {object_id}/{representation_id}",
    )
    plan_ids = value.get("plan_ids")
    _require(
        isinstance(plan_ids, list)
        and bool(plan_ids)
        and plan_ids == sorted(set(plan_ids)),
        "N4 artifact plan IDs are not canonical",
    )
    for plan_id in plan_ids:
        _identifier(plan_id, "plan_id")
    N4ArtifactProvenance.from_dict(value.get("provenance"))
    metadata = value.get("representation_metadata")
    _require(isinstance(metadata, dict), "representation_metadata is invalid")
    if representation_id == FRAME_BUNDLE_REPRESENTATION_ID:
        _require(
            set(metadata)
            == {
                "frame_count",
                "member_count",
                "manifest_sha256",
                "total_jpeg_bytes",
            },
            "frame-bundle metadata field set changed",
        )
        try:
            bundle = validate_frame_bundle_bytes(
                raw,
                expected_object_id=object_id,
                expected_sha256=expected_sha256,
                expected_size_bytes=size,
                artifact_media_type=FRAME_BUNDLE_MEDIA_TYPE,
                limits=limits,
            )
        except Exception as exc:
            raise N4DerivedDataPlaneError(
                f"artifact for {object_id} failed canonical bundle validation"
            ) from exc
        expected_metadata = {
            "frame_count": bundle.frame_count,
            "member_count": bundle.member_count,
            "manifest_sha256": bundle.manifest_sha256,
            "total_jpeg_bytes": bundle.total_jpeg_bytes,
        }
    else:
        _require(
            set(metadata) == {"utf8_character_count"},
            "digest metadata field set changed",
        )
        _text, character_count = _validate_digest_text(raw, object_id)
        expected_metadata = {"utf8_character_count": character_count}
    _require(
        metadata == expected_metadata,
        f"representation metadata changed: {object_id}/{representation_id}",
    )
    _assert_portable_metadata(value, "artifact record")
    return value


def verify_n4_derived_data_package(
    package_dir: str | Path,
    *,
    limits: FrameBundleLimits = DEFAULT_FRAME_BUNDLE_LIMITS,
) -> dict[str, Any]:
    """Fail closed over package bytes, catalog semantics, and provenance."""

    root = Path(package_dir).resolve()
    _require(root.is_dir(), f"N4 package directory does not exist: {root}")
    _require(isinstance(limits, FrameBundleLimits), "limits is invalid")
    _checksums, checksum_bytes = _read_checksums(root)
    manifest_path = root / PACKAGE_MANIFEST_NAME
    _require(manifest_path.is_file(), "N4 package manifest is missing")
    report_bytes = manifest_path.read_bytes()
    report = _strict_json(report_bytes, PACKAGE_MANIFEST_NAME)
    _require(
        report_bytes == _json_bytes(report),
        "N4 package manifest is not canonical JSON",
    )
    required = {
        "schema_version",
        "status",
        "package_id",
        "logical_node_id",
        "logical_location",
        "catalog_version",
        "data_agent_manifest_package_path",
        "object_catalog_package_path",
        "representation_ids",
        "object_count",
        "artifact_count",
        "artifact_bytes",
        "objects",
        "endpoint_free",
        "atomic_publication_compatible",
        "credentials_recorded",
        "eligible_for_scientific_claims",
    }
    _require(set(report) == required, "N4 package manifest field set changed")
    _require(
        report.get("schema_version") == N4_DERIVED_PACKAGE_SCHEMA_VERSION
        and report.get("status") == N4_DERIVED_PACKAGE_STATUS,
        "N4 package schema or status changed",
    )
    _identifier(report.get("package_id"), "package_id")
    _require(
        report.get("logical_node_id") == N4_LOGICAL_NODE_ID
        and report.get("logical_location") == N4_LOGICAL_LOCATION,
        "N4 logical identity changed",
    )
    catalog_version = _identifier(
        report.get("catalog_version"),
        "catalog_version",
    )
    _require(
        report.get("data_agent_manifest_package_path")
        == DATA_AGENT_MANIFEST_PATH
        and report.get("object_catalog_package_path") == OBJECT_CATALOG_PATH,
        "N4 package configuration paths changed",
    )
    _require(
        report.get("endpoint_free") is True
        and report.get("atomic_publication_compatible") is True
        and report.get("credentials_recorded") is False
        and report.get("eligible_for_scientific_claims") is False,
        "N4 package safety flags changed",
    )
    rows = report.get("objects")
    _require(isinstance(rows, list) and bool(rows), "N4 objects are invalid")
    validated = [
        _validated_report_record(root, row, limits=limits) for row in rows
    ]
    keys = [(row["object_id"], row["representation_id"]) for row in validated]
    _require(keys == sorted(keys), "N4 artifact records are not canonical")
    _require(len(keys) == len(set(keys)), "N4 artifact records are duplicated")
    representation_ids = sorted({
        row["representation_id"] for row in validated
    })
    for representation_id in representation_ids:
        binding_sets = {
            tuple(row["plan_ids"])
            for row in validated
            if row["representation_id"] == representation_id
        }
        _require(
            len(binding_sets) == 1,
            "N4 object-level plan bindings cannot be represented exactly by "
            "the standard Data Agent manifest",
        )
    object_ids = sorted({row["object_id"] for row in validated})
    _require(
        report.get("representation_ids") == representation_ids,
        "N4 representation set changed",
    )
    _require(
        report.get("object_count") == len(object_ids)
        and report.get("artifact_count") == len(validated)
        and report.get("artifact_bytes")
        == sum(row["artifact_size_bytes"] for row in validated),
        "N4 package aggregate counts changed",
    )
    expected_manifest, expected_catalog = _data_agent_documents(
        validated,
        catalog_version,
    )
    data_agent_bytes = (root / DATA_AGENT_MANIFEST_PATH).read_bytes()
    catalog_bytes = (root / OBJECT_CATALOG_PATH).read_bytes()
    _require(
        data_agent_bytes == _json_bytes(expected_manifest),
        "N4 Data Agent manifest does not match the package",
    )
    _require(
        catalog_bytes == _json_bytes(expected_catalog),
        "N4 object catalog does not match the package",
    )
    try:
        data_agent = load_data_agent_manifest(root / DATA_AGENT_MANIFEST_PATH)
    except Exception as exc:
        raise N4DerivedDataPlaneError(
            "N4 standard Data Agent manifest is invalid"
        ) from exc
    _require(
        data_agent.node_id == N4_LOGICAL_NODE_ID
        and data_agent.object_catalog is not None
        and data_agent.object_catalog.catalog_version == catalog_version,
        "N4 standard Data Agent identity or catalog changed",
    )
    for row in validated:
        for plan_id in row["plan_ids"]:
            resolved = data_agent.resolve(
                plan_id=plan_id,
                object_id=row["object_id"],
                representation_id=row["representation_id"],
                requested_location=N4_LOGICAL_LOCATION,
            )
            expected_path = (
                root
                / Path(*PurePosixPath(row["artifact_package_path"]).parts)
            ).resolve()
            _require(
                resolved.path == expected_path,
                "N4 Data Agent catalog resolves a different artifact",
            )
    _assert_portable_metadata(report)
    return {
        "schema_version": N4_DERIVED_PACKAGE_SCHEMA_VERSION,
        "status": N4_DERIVED_PACKAGE_VERIFIED_STATUS,
        "package_id": report["package_id"],
        "logical_node_id": N4_LOGICAL_NODE_ID,
        "catalog_version": catalog_version,
        "object_count": len(object_ids),
        "artifact_count": len(validated),
        "representation_ids": representation_ids,
        "package_sha256": _sha256(checksum_bytes),
        "endpoint_free": True,
        "credentials_recorded": False,
    }


def _input_from_report(root: Path, row: Mapping[str, Any]) -> N4DerivedArtifactInput:
    path = root / Path(*PurePosixPath(row["artifact_package_path"]).parts)
    return N4DerivedArtifactInput(
        object_id=row["object_id"],
        representation_id=row["representation_id"],
        artifact_bytes=path.read_bytes(),
        plan_ids=tuple(row["plan_ids"]),
        provenance=N4ArtifactProvenance.from_dict(row["provenance"]),
        expected_sha256=row["artifact_sha256"],
        expected_size_bytes=row["artifact_size_bytes"],
    )


def _publication_request(
    publication_id: str,
    package_id: str,
    catalog_version: str,
    expected_current_catalog_version: str | None,
    records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    artifacts = [{
        "object_id": row["object_id"],
        "representation_id": row["representation_id"],
        "artifact_size_bytes": row["artifact_size_bytes"],
        "artifact_sha256": row["artifact_sha256"],
        "plan_ids": list(row["plan_ids"]),
        "provenance": row["provenance"],
    } for row in records]
    artifacts.sort(key=lambda row: (row["object_id"], row["representation_id"]))
    return {
        "publication_id": publication_id,
        "package_id": package_id,
        "catalog_version": catalog_version,
        "expected_current_catalog_version": expected_current_catalog_version,
        "artifacts": artifacts,
    }


class N4DerivedRepresentationStore:
    """Durable compare-and-swap publication store for immutable N4 packages."""

    def __init__(
        self,
        root: str | Path,
        *,
        limits: FrameBundleLimits = DEFAULT_FRAME_BUNDLE_LIMITS,
    ) -> None:
        self.root = Path(root).resolve()
        self.generations = self.root / GENERATIONS_DIRECTORY_NAME
        self.database_path = self.root / STORE_DATABASE_NAME
        self.limits = limits
        _require(isinstance(limits, FrameBundleLimits), "limits is invalid")
        self.generations.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS n4_current_generation (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    generation_id TEXT NOT NULL,
                    package_sha256 TEXT NOT NULL,
                    catalog_version TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS n4_publications (
                    publication_id TEXT PRIMARY KEY,
                    request_sha256 TEXT NOT NULL,
                    generation_id TEXT NOT NULL,
                    receipt_json TEXT NOT NULL
                );
                """
            )
            connection.commit()

    def _snapshot(
        self,
        generation_id: str,
        package_sha256: str,
        catalog_version: str,
    ) -> N4PublishedSnapshot:
        generation_id = _identifier(generation_id, "generation_id")
        package_sha256 = _digest(package_sha256, "package_sha256")
        catalog_version = _identifier(catalog_version, "catalog_version")
        package_dir = self.generations / generation_id
        result = verify_n4_derived_data_package(
            package_dir,
            limits=self.limits,
        )
        _require(
            result["package_sha256"] == package_sha256
            and result["catalog_version"] == catalog_version,
            "N4 publication database disagrees with immutable generation",
        )
        return N4PublishedSnapshot(
            generation_id=generation_id,
            package_sha256=package_sha256,
            catalog_version=catalog_version,
            package_dir=package_dir,
            data_agent_manifest_path=package_dir / DATA_AGENT_MANIFEST_PATH,
            object_catalog_path=package_dir / OBJECT_CATALOG_PATH,
        )

    def current_snapshot(self) -> N4PublishedSnapshot | None:
        """Resolve and verify the one atomically visible generation."""

        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT generation_id, package_sha256, catalog_version
                FROM n4_current_generation
                WHERE singleton = 1
                """
            ).fetchone()
        if row is None:
            return None
        return self._snapshot(
            row["generation_id"],
            row["package_sha256"],
            row["catalog_version"],
        )

    def _stored_publication(
        self,
        connection: sqlite3.Connection,
        publication_id: str,
        request_sha256: str,
    ) -> N4PublicationResult | None:
        row = connection.execute(
            """
            SELECT request_sha256, generation_id, receipt_json
            FROM n4_publications
            WHERE publication_id = ?
            """,
            (publication_id,),
        ).fetchone()
        if row is None:
            return None
        if row["request_sha256"] != request_sha256:
            raise N4PublicationConflict(
                "publication_id was already used for a different request"
            )
        receipt = _strict_json(
            row["receipt_json"].encode("utf-8"),
            "stored publication receipt",
        )
        verify_n4_publication_receipt(receipt)
        snapshot = self._snapshot(
            row["generation_id"],
            receipt["package_sha256"],
            receipt["committed_catalog_version"],
        )
        return N4PublicationResult(
            receipt=receipt,
            snapshot=snapshot,
            idempotent_replay=True,
        )

    def publish(
        self,
        *,
        publication_id: str,
        package_id: str,
        catalog_version: str,
        expected_current_catalog_version: str | None,
        artifacts: Sequence[N4DerivedArtifactInput],
    ) -> N4PublicationResult:
        """Publish additions/replacements with durable catalog-level CAS.

        ``expected_current_catalog_version`` is ``None`` only for the first
        generation.  Later calls must name the exact current version, making
        accidental lost updates impossible.  A retry with the same
        ``publication_id`` and request is idempotent across process restarts.
        """

        publication_id = _identifier(publication_id, "publication_id")
        package_id = _identifier(package_id, "package_id")
        catalog_version = _identifier(catalog_version, "catalog_version")
        if expected_current_catalog_version is not None:
            expected_current_catalog_version = _identifier(
                expected_current_catalog_version,
                "expected_current_catalog_version",
            )
        _require(bool(artifacts), "publication contains no artifacts")
        normalized = [
            _normalize_artifact(artifact, self.limits) for artifact in artifacts
        ]
        new_keys = [
            (row["object_id"], row["representation_id"]) for row in normalized
        ]
        _require(
            len(new_keys) == len(set(new_keys)),
            "publication repeats an object/representation pair",
        )
        request = _publication_request(
            publication_id,
            package_id,
            catalog_version,
            expected_current_catalog_version,
            normalized,
        )
        request_sha256 = _sha256(_compact_json_bytes(request))

        with closing(self._connect()) as connection:
            replay = self._stored_publication(
                connection,
                publication_id,
                request_sha256,
            )
            if replay is not None:
                return replay

        current = self.current_snapshot()
        actual_current = None if current is None else current.catalog_version
        if actual_current != expected_current_catalog_version:
            raise N4PublicationConflict(
                "N4 catalog compare-and-swap failed: expected "
                f"{expected_current_catalog_version!r}, current "
                f"{actual_current!r}"
            )
        combined: dict[tuple[str, str], N4DerivedArtifactInput] = {}
        if current is not None:
            report = _strict_json(
                (current.package_dir / PACKAGE_MANIFEST_NAME).read_bytes(),
                PACKAGE_MANIFEST_NAME,
            )
            for row in report["objects"]:
                inherited = _input_from_report(current.package_dir, row)
                combined[(inherited.object_id, inherited.representation_id)] = (
                    inherited
                )
        for artifact, row in zip(artifacts, normalized, strict=True):
            combined[(row["object_id"], row["representation_id"])] = replace(
                artifact,
                artifact_bytes=row["artifact_bytes"],
                plan_ids=tuple(row["plan_ids"]),
                expected_sha256=row["artifact_sha256"],
                expected_size_bytes=row["artifact_size_bytes"],
            )

        candidate = self.generations / (
            ".candidate-" + uuid.uuid4().hex
        )
        try:
            build_n4_derived_data_package(
                list(combined.values()),
                output_dir=candidate,
                package_id=package_id,
                catalog_version=catalog_version,
                limits=self.limits,
            )
            verified = verify_n4_derived_data_package(
                candidate,
                limits=self.limits,
            )
        except Exception:
            if candidate.exists():
                shutil.rmtree(candidate, ignore_errors=True)
            raise
        package_sha256 = verified["package_sha256"]
        generation_id = "generation-" + package_sha256
        generation = self.generations / generation_id
        try:
            if generation.exists():
                existing = verify_n4_derived_data_package(
                    generation,
                    limits=self.limits,
                )
                _require(
                    existing["package_sha256"] == package_sha256,
                    "N4 generation identity collision",
                )
                shutil.rmtree(candidate)
            else:
                try:
                    os.replace(candidate, generation)
                    _fsync_directory(self.generations)
                except OSError:
                    if not generation.exists():
                        raise
                    existing = verify_n4_derived_data_package(
                        generation,
                        limits=self.limits,
                    )
                    _require(
                        existing["package_sha256"] == package_sha256,
                        "N4 generation identity collision",
                    )
                    shutil.rmtree(candidate)

            with closing(self._connect()) as connection:
                connection.execute("BEGIN IMMEDIATE")
                replay = self._stored_publication(
                    connection,
                    publication_id,
                    request_sha256,
                )
                if replay is not None:
                    connection.rollback()
                    return replay
                current_row = connection.execute(
                    """
                    SELECT catalog_version
                    FROM n4_current_generation
                    WHERE singleton = 1
                    """
                ).fetchone()
                observed = (
                    None
                    if current_row is None
                    else current_row["catalog_version"]
                )
                if observed != expected_current_catalog_version:
                    connection.rollback()
                    raise N4PublicationConflict(
                        "N4 catalog compare-and-swap failed during commit: "
                        f"expected {expected_current_catalog_version!r}, "
                        f"current {observed!r}"
                    )
                receipt = {
                    "schema_version": N4_PUBLICATION_RECEIPT_SCHEMA_VERSION,
                    "status": N4_PUBLICATION_STATUS,
                    "publication_id": publication_id,
                    "request_sha256": request_sha256,
                    "logical_node_id": N4_LOGICAL_NODE_ID,
                    "previous_catalog_version": observed,
                    "committed_catalog_version": catalog_version,
                    "generation_id": generation_id,
                    "package_sha256": package_sha256,
                    "object_count": verified["object_count"],
                    "artifact_count": verified["artifact_count"],
                    "published_artifacts": [
                        {
                            "object_id": row["object_id"],
                            "representation_id": row["representation_id"],
                            "artifact_size_bytes": row["artifact_size_bytes"],
                            "artifact_sha256": row["artifact_sha256"],
                        }
                        for row in sorted(
                            normalized,
                            key=lambda item: (
                                item["object_id"],
                                item["representation_id"],
                            ),
                        )
                    ],
                    "atomic_visibility": True,
                    "credentials_recorded": False,
                }
                receipt["receipt_sha256"] = _sha256(
                    _compact_json_bytes(receipt)
                )
                verify_n4_publication_receipt(receipt)
                receipt_json = _compact_json_bytes(receipt).decode("utf-8")
                connection.execute(
                    """
                    INSERT INTO n4_publications (
                        publication_id,
                        request_sha256,
                        generation_id,
                        receipt_json
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (
                        publication_id,
                        request_sha256,
                        generation_id,
                        receipt_json,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO n4_current_generation (
                        singleton,
                        generation_id,
                        package_sha256,
                        catalog_version
                    ) VALUES (1, ?, ?, ?)
                    ON CONFLICT(singleton) DO UPDATE SET
                        generation_id = excluded.generation_id,
                        package_sha256 = excluded.package_sha256,
                        catalog_version = excluded.catalog_version
                    """,
                    (generation_id, package_sha256, catalog_version),
                )
                connection.commit()
        finally:
            if candidate.exists():
                shutil.rmtree(candidate, ignore_errors=True)

        snapshot = self._snapshot(
            generation_id,
            package_sha256,
            catalog_version,
        )
        return N4PublicationResult(
            receipt=receipt,
            snapshot=snapshot,
            idempotent_replay=False,
        )


def verify_n4_publication_receipt(
    receipt: Mapping[str, Any],
) -> dict[str, Any]:
    """Verify the content-bound, endpoint-free N4 commit receipt."""

    _require(isinstance(receipt, Mapping), "N4 receipt must be an object")
    value = json.loads(_compact_json_bytes(dict(receipt)).decode("utf-8"))
    required = {
        "schema_version",
        "status",
        "publication_id",
        "request_sha256",
        "logical_node_id",
        "previous_catalog_version",
        "committed_catalog_version",
        "generation_id",
        "package_sha256",
        "object_count",
        "artifact_count",
        "published_artifacts",
        "atomic_visibility",
        "credentials_recorded",
        "receipt_sha256",
    }
    _require(set(value) == required, "N4 receipt field set changed")
    _require(
        value.get("schema_version") == N4_PUBLICATION_RECEIPT_SCHEMA_VERSION
        and value.get("status") == N4_PUBLICATION_STATUS,
        "N4 receipt schema or status changed",
    )
    _identifier(value.get("publication_id"), "publication_id")
    _digest(value.get("request_sha256"), "request_sha256")
    _require(
        value.get("logical_node_id") == N4_LOGICAL_NODE_ID,
        "N4 receipt logical identity changed",
    )
    previous = value.get("previous_catalog_version")
    if previous is not None:
        _identifier(previous, "previous_catalog_version")
    _identifier(
        value.get("committed_catalog_version"),
        "committed_catalog_version",
    )
    _identifier(value.get("generation_id"), "generation_id")
    _digest(value.get("package_sha256"), "package_sha256")
    _positive_integer(value.get("object_count"), "object_count")
    _positive_integer(value.get("artifact_count"), "artifact_count")
    published = value.get("published_artifacts")
    _require(
        isinstance(published, list) and bool(published),
        "published_artifacts is invalid",
    )
    keys: list[tuple[str, str]] = []
    for row in published:
        _require(
            isinstance(row, dict)
            and set(row)
            == {
                "object_id",
                "representation_id",
                "artifact_size_bytes",
                "artifact_sha256",
            },
            "published artifact receipt field set changed",
        )
        object_id = _identifier(row.get("object_id"), "object_id")
        representation_id = _identifier(
            row.get("representation_id"),
            "representation_id",
        )
        _require(
            representation_id in SUPPORTED_REPRESENTATIONS,
            "receipt contains an unsupported representation",
        )
        _positive_integer(row.get("artifact_size_bytes"), "artifact_size_bytes")
        _digest(row.get("artifact_sha256"), "artifact_sha256")
        keys.append((object_id, representation_id))
    _require(keys == sorted(set(keys)), "published artifacts are not canonical")
    _require(
        value.get("atomic_visibility") is True
        and value.get("credentials_recorded") is False,
        "N4 receipt safety flags changed",
    )
    recorded = _digest(value.get("receipt_sha256"), "receipt_sha256")
    unsigned = dict(value)
    unsigned.pop("receipt_sha256")
    _require(
        recorded == _sha256(_compact_json_bytes(unsigned)),
        "N4 receipt digest changed",
    )
    _assert_portable_metadata(value, "publication receipt")
    return value


def resolve_current_n4_snapshot(
    store_root: str | Path,
) -> N4PublishedSnapshot | None:
    """Convenience API for an N4 supervisor before Data Agent startup/reload."""

    return N4DerivedRepresentationStore(store_root).current_snapshot()


__all__ = [
    "CHECKSUMS_NAME",
    "DATA_AGENT_MANIFEST_PATH",
    "FRAME_BUNDLE_REPRESENTATION_ID",
    "MULTIMODAL_DIGEST_MEDIA_TYPE",
    "MULTIMODAL_DIGEST_REPRESENTATION_ID",
    "N4ArtifactProvenance",
    "N4DerivedArtifactInput",
    "N4DerivedDataPlaneError",
    "N4DerivedRepresentationStore",
    "N4PublicationConflict",
    "N4PublicationResult",
    "N4PublishedSnapshot",
    "N4_DERIVED_PACKAGE_SCHEMA_VERSION",
    "N4_DERIVED_BINDINGS_SCHEMA_VERSION",
    "N4_LOGICAL_LOCATION",
    "N4_LOGICAL_NODE_ID",
    "N4_PUBLICATION_RECEIPT_SCHEMA_VERSION",
    "N5_LOGICAL_NODE_ID",
    "OBJECT_CATALOG_PATH",
    "PACKAGE_MANIFEST_NAME",
    "build_n4_derived_data_package",
    "build_n4_derived_data_package_from_manifest",
    "resolve_current_n4_snapshot",
    "verify_n4_derived_data_package",
    "verify_n4_publication_receipt",
]
