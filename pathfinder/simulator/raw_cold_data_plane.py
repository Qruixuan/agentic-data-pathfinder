"""Freeze authoritative raw videos for the logical N3 cold-data node.

The package produced here is a deployment-neutral data-plane input.  It
contains the exact source bytes, their identities, and standard Pathfinder
Data Agent manifest/catalog documents.  It intentionally contains no URL,
credential, host path, mount path, or artificial price/latency.  A container
deployment and a real multi-machine deployment therefore consume the same
package and bind only its endpoint, storage device, and secrets separately.

This module proves byte identity and Data Agent route compatibility.  It does
not claim that the bytes were read from a physical HDD, crossed a real network,
were decoded successfully, or produced a semantic answer.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Mapping, Sequence

from ..data_agent_manifest import (
    DATA_AGENT_MANIFEST_VERSION,
    DATA_OBJECT_CATALOG_VERSION,
    load_data_agent_manifest,
)


RAW_COLD_DATA_PLANE_SCHEMA_VERSION = (
    "pathfinder.simulator-raw-cold-data-plane/v1alpha1"
)
RAW_COLD_BINDINGS_SCHEMA_VERSION = (
    "pathfinder.simulator-raw-cold-bindings/v1alpha1"
)
RAW_OBJECT_PROVENANCE_SCHEMA_VERSION = (
    "pathfinder.raw-object-provenance/v1alpha1"
)
RAW_COLD_DATA_PLANE_STATUS = "FROZEN_RAW_COLD_DATA_PLANE"
RAW_COLD_DATA_PLANE_VERIFIED_STATUS = "VERIFIED_RAW_COLD_DATA_PLANE"

SOURCE_NODE_ID = "N3"
SOURCE_LOCATION = "origin-cold"
STORAGE_ROLE = "authoritative-cold-object-store"
REPRESENTATION_ID = "raw_video"
ARTIFACT_MEDIA_TYPE = "video/mp4"

PACKAGE_MANIFEST_NAME = "raw-cold-data-plane.json"
DATA_AGENT_MANIFEST_PATH = "config/data-agent-manifest.json"
OBJECT_CATALOG_PATH = "config/object-catalog.json"
CHECKSUMS_NAME = "SHA256SUMS"
DEFAULT_MAX_ARTIFACT_BYTES = 64 * 1024 * 1024 * 1024

_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_PLAN_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._|:-]{0,255}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_WINDOWS_ABSOLUTE = re.compile(r"(?:[A-Za-z]:[\\/]|\\\\)")
_CREDENTIAL_KEY = re.compile(
    r"(?:^|_)(?:api_?key|password|secret|token)(?:$|_)",
    re.IGNORECASE,
)

_REPORT_KEYS = frozenset({
    "schema_version",
    "status",
    "package_id",
    "route",
    "catalog_version",
    "object_count",
    "artifact_count",
    "artifact_bytes",
    "objects",
    "portable_data_agent_contract_complete",
    "deployment_binding_required",
    "runtime_execution_verified",
    "workflow_submitted",
    "llm_called",
    "credentials_recorded",
    "eligible_for_scientific_claims",
})

_ROUTE_KEYS = frozenset({
    "source_node_id",
    "source_location",
    "storage_role",
    "representation_id",
    "artifact_media_type",
    "data_agent_protocol",
    "data_agent_manifest_package_path",
    "object_catalog_package_path",
    "artifact_root_package_path",
    "authoritative_copy",
})

_OBJECT_KEYS = frozenset({
    "object_id",
    "representation_id",
    "artifact_media_type",
    "artifact_package_path",
    "artifact_size_bytes",
    "artifact_sha256",
    "catalog_version",
    "plan_ids",
    "provenance",
})

_PROVENANCE_KEYS = frozenset({
    "schema_version",
    "dataset_id",
    "dataset_revision",
    "source_object_id",
    "snapshot_semantics",
    "source_artifact_size_bytes",
    "source_artifact_sha256",
})


class RawColdDataPlaneError(RuntimeError):
    """Raised when an N3 raw/cold package cannot be trusted."""


@dataclass(frozen=True)
class RawColdObjectBinding:
    """Bind one authoritative raw MP4 to its portable source identity."""

    object_id: str
    artifact_path: str | Path
    catalog_version: str
    plan_ids: tuple[str, ...]
    dataset_id: str
    dataset_revision: str
    source_object_id: str
    artifact_sha256: str | None = None
    artifact_size_bytes: int | None = None


def _require(condition: object, message: str) -> None:
    if not condition:
        raise RawColdDataPlaneError(message)


def _identifier(value: Any, name: str) -> str:
    _require(isinstance(value, str), f"{name} must be a string")
    _require(_IDENTIFIER.fullmatch(value) is not None, f"{name} is invalid")
    return value


def _plan_identifier(value: Any, name: str = "plan_id") -> str:
    _require(isinstance(value, str), f"{name} must be a string")
    _require(
        _PLAN_IDENTIFIER.fullmatch(value) is not None,
        f"{name} is invalid",
    )
    return value


def _digest(value: Any, name: str) -> str:
    _require(isinstance(value, str), f"{name} must be a string")
    _require(_SHA256.fullmatch(value) is not None, f"{name} is invalid")
    return value


def _positive_integer(value: Any, name: str) -> int:
    _require(
        isinstance(value, int) and not isinstance(value, bool) and value > 0,
        f"{name} must be a positive integer",
    )
    return value


def _json_bytes(value: Any) -> bytes:
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


def _strict_json(raw: bytes, name: str) -> dict[str, Any]:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, child in pairs:
            if key in result:
                raise RawColdDataPlaneError(
                    f"{name} contains duplicate key {key!r}"
                )
            result[key] = child
        return result

    def reject_constant(value: str) -> None:
        raise RawColdDataPlaneError(
            f"{name} contains non-finite number {value}"
        )

    try:
        value = json.loads(
            raw,
            object_pairs_hook=reject_duplicates,
            parse_constant=reject_constant,
        )
    except RawColdDataPlaneError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise RawColdDataPlaneError(f"{name} is not valid JSON") from exc
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


def _assert_safe_metadata(value: Any, name: str = "metadata") -> None:
    """Reject deployment endpoints, secrets, and host paths."""

    if isinstance(value, Mapping):
        for key, child in value.items():
            _require(isinstance(key, str), f"{name} has a non-string key")
            _require(
                _CREDENTIAL_KEY.search(key) is None,
                f"{name} contains credential-bearing field {key!r}",
            )
            _assert_safe_metadata(child, f"{name}.{key}")
        return
    if isinstance(value, list):
        for index, child in enumerate(value):
            _assert_safe_metadata(child, f"{name}[{index}]")
        return
    if isinstance(value, str):
        _require("://" not in value, f"{name} contains a URL")
        _require(not value.startswith("/"), f"{name} contains an absolute path")
        _require(
            _WINDOWS_ABSOLUTE.match(value) is None,
            f"{name} contains a Windows host path",
        )


def _portable_route() -> dict[str, Any]:
    return {
        "source_node_id": SOURCE_NODE_ID,
        "source_location": SOURCE_LOCATION,
        "storage_role": STORAGE_ROLE,
        "representation_id": REPRESENTATION_ID,
        "artifact_media_type": ARTIFACT_MEDIA_TYPE,
        "data_agent_protocol": "pathfinder.data-agent/v1alpha1",
        "data_agent_manifest_package_path": DATA_AGENT_MANIFEST_PATH,
        "object_catalog_package_path": OBJECT_CATALOG_PATH,
        "artifact_root_package_path": "artifacts",
        "authoritative_copy": True,
    }


def _normalize_binding(binding: RawColdObjectBinding) -> dict[str, Any]:
    _require(
        isinstance(binding, RawColdObjectBinding),
        "raw object binding is invalid",
    )
    object_id = _identifier(binding.object_id, "object_id")
    catalog_version = _identifier(binding.catalog_version, "catalog_version")
    dataset_id = _identifier(binding.dataset_id, "dataset_id")
    dataset_revision = _identifier(
        binding.dataset_revision,
        "dataset_revision",
    )
    source_object_id = _identifier(
        binding.source_object_id,
        "source_object_id",
    )
    _require(isinstance(binding.plan_ids, tuple), "plan_ids must be a tuple")
    normalized_plan_ids = tuple(
        _plan_identifier(item) for item in binding.plan_ids
    )
    _require(
        len(normalized_plan_ids) == len(set(normalized_plan_ids)),
        f"object {object_id} repeats a plan binding",
    )
    plan_ids = tuple(sorted(normalized_plan_ids))
    _require(bool(plan_ids), f"object {object_id} has no plan binding")
    if binding.artifact_sha256 is not None:
        _digest(binding.artifact_sha256, "artifact_sha256")
    if binding.artifact_size_bytes is not None:
        _positive_integer(binding.artifact_size_bytes, "artifact_size_bytes")
    try:
        source = Path(binding.artifact_path).resolve(strict=True)
    except (OSError, TypeError, ValueError) as exc:
        raise RawColdDataPlaneError(
            f"raw artifact path for {object_id} is invalid"
        ) from exc
    _require(source.is_file(), f"raw artifact for {object_id} is missing")
    _require(
        not Path(binding.artifact_path).is_symlink(),
        f"raw artifact for {object_id} must not be a symbolic link",
    )
    return {
        "object_id": object_id,
        "catalog_version": catalog_version,
        "dataset_id": dataset_id,
        "dataset_revision": dataset_revision,
        "source_object_id": source_object_id,
        "plan_ids": plan_ids,
        "source": source,
        "expected_sha256": binding.artifact_sha256,
        "expected_size_bytes": binding.artifact_size_bytes,
    }


def _validate_mp4_header(head: bytes, size_bytes: int, name: str) -> None:
    """Apply a bounded ISO-BMFF/MP4 identity gate, not a decode claim."""

    _require(size_bytes >= 12 and len(head) >= 12, f"{name} is not an MP4")
    box_size = int.from_bytes(head[0:4], "big")
    _require(head[4:8] == b"ftyp", f"{name} has no leading ftyp box")
    if box_size == 1:
        _require(len(head) >= 20, f"{name} has a truncated extended ftyp box")
        box_size = int.from_bytes(head[8:16], "big")
        header_size = 16
    else:
        header_size = 8
    _require(
        header_size + 4 <= box_size <= size_bytes,
        f"{name} has an invalid ftyp box size",
    )
    major_brand = head[header_size : header_size + 4]
    _require(
        len(major_brand) == 4 and major_brand != b"\x00\x00\x00\x00",
        f"{name} has an invalid MP4 major brand",
    )


def _copy_artifact(
    source: Path,
    target: Path,
    *,
    max_artifact_bytes: int,
) -> tuple[str, int]:
    before = source.stat()
    _require(
        0 < before.st_size <= max_artifact_bytes,
        f"raw artifact size must be within 1..{max_artifact_bytes} bytes",
    )
    digest = hashlib.sha256()
    size = 0
    head = bytearray()
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        with source.open("rb") as input_stream, target.open("xb") as output_stream:
            while True:
                block = input_stream.read(1024 * 1024)
                if not block:
                    break
                size += len(block)
                _require(
                    size <= max_artifact_bytes,
                    f"raw artifact exceeds {max_artifact_bytes} bytes",
                )
                if len(head) < 32:
                    head.extend(block[: 32 - len(head)])
                digest.update(block)
                output_stream.write(block)
    except OSError as exc:
        raise RawColdDataPlaneError(
            f"cannot freeze raw artifact for {source.name}"
        ) from exc
    after = source.stat()
    _require(
        (before.st_size, before.st_mtime_ns)
        == (after.st_size, after.st_mtime_ns),
        "source artifact changed while it was being frozen",
    )
    _require(size == before.st_size, "source artifact size changed during copy")
    _validate_mp4_header(bytes(head), size, "raw artifact")
    return digest.hexdigest(), size


def _identify_artifact(
    path: Path,
    *,
    max_artifact_bytes: int,
) -> tuple[str, int]:
    _require(path.is_file(), "raw artifact is missing")
    _require(not path.is_symlink(), "raw artifact must not be a symbolic link")
    digest = hashlib.sha256()
    size = 0
    head = bytearray()
    try:
        with path.open("rb") as stream:
            while True:
                block = stream.read(1024 * 1024)
                if not block:
                    break
                size += len(block)
                _require(
                    size <= max_artifact_bytes,
                    f"raw artifact exceeds {max_artifact_bytes} bytes",
                )
                if len(head) < 32:
                    head.extend(block[: 32 - len(head)])
                digest.update(block)
    except OSError as exc:
        raise RawColdDataPlaneError("raw artifact is unreadable") from exc
    _validate_mp4_header(bytes(head), size, "raw artifact")
    return digest.hexdigest(), size


def _data_agent_documents(
    rows: Sequence[Mapping[str, Any]],
    catalog_version: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    plan_ids = sorted({
        plan_id
        for row in rows
        for plan_id in row["plan_ids"]
    })
    binding = {
        "location": SOURCE_LOCATION,
        "minimum_latency_ms": 0.0,
        "realized_cost": 0.0,
        "cache_hit": False,
    }
    manifest = {
        "schema_version": DATA_AGENT_MANIFEST_VERSION,
        "node_id": SOURCE_NODE_ID,
        "require_plan_binding": True,
        "object_catalog_path": "object-catalog.json",
        "representations": {
            REPRESENTATION_ID: {
                "kind": "artifact_uri",
                "media_type": ARTIFACT_MEDIA_TYPE,
                "default_binding": dict(binding),
                "plan_bindings": {
                    plan_id: dict(binding) for plan_id in plan_ids
                },
            }
        },
    }
    catalog = {
        "schema_version": DATA_OBJECT_CATALOG_VERSION,
        "catalog_version": catalog_version,
        "objects": {
            row["object_id"]: {
                "representations": {
                    REPRESENTATION_ID: {
                        "path": "../" + row["artifact_package_path"],
                        "plan_paths": {
                            plan_id: "../" + row["artifact_package_path"]
                            for plan_id in row["plan_ids"]
                        },
                    }
                }
            }
            for row in rows
        },
    }
    return manifest, catalog


def _actual_files(root: Path) -> dict[str, Path]:
    result: dict[str, Path] = {}
    for path in root.rglob("*"):
        if path.is_symlink():
            raise RawColdDataPlaneError("package contains a symbolic link")
        if path.is_file():
            result[path.relative_to(root).as_posix()] = path
    return result


def _checksums_bytes(root: Path, names: Sequence[str]) -> bytes:
    rows = []
    for name in sorted(names):
        path = root / Path(*PurePosixPath(name).parts)
        digest, _ = _hash_file(path)
        rows.append(f"{digest}  {name}\n")
    return "".join(rows).encode("utf-8")


def _hash_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("rb") as stream:
            while True:
                block = stream.read(1024 * 1024)
                if not block:
                    break
                digest.update(block)
                size += len(block)
    except OSError as exc:
        raise RawColdDataPlaneError(f"cannot read package file {path.name}") from exc
    return digest.hexdigest(), size


def build_raw_cold_data_plane_package_from_manifest(
    binding_manifest: str | Path,
    *,
    output_dir: str | Path,
    max_artifact_bytes: int = DEFAULT_MAX_ARTIFACT_BYTES,
) -> dict[str, Any]:
    """Build N3 from an operator input manifest without freezing host paths."""

    source = Path(binding_manifest).resolve()
    try:
        value = _strict_json(source.read_bytes(), "raw/cold binding manifest")
    except OSError as exc:
        raise RawColdDataPlaneError(
            "cannot read raw/cold binding manifest"
        ) from exc
    expected = {
        "schema_version",
        "package_id",
        "catalog_version",
        "plan_ids",
        "dataset_id",
        "dataset_revision",
        "objects",
        "credentials_recorded",
    }
    _require(set(value) == expected, "raw/cold binding manifest fields changed")
    _require(
        value.get("schema_version") == RAW_COLD_BINDINGS_SCHEMA_VERSION,
        "raw/cold binding manifest schema is unsupported",
    )
    package_id = _identifier(value.get("package_id"), "package_id")
    catalog_version = _identifier(
        value.get("catalog_version"),
        "catalog_version",
    )
    dataset_id = _identifier(value.get("dataset_id"), "dataset_id")
    dataset_revision = _identifier(
        value.get("dataset_revision"),
        "dataset_revision",
    )
    raw_plans = value.get("plan_ids")
    _require(
        isinstance(raw_plans, list) and bool(raw_plans),
        "plan_ids must be a non-empty array",
    )
    plan_ids = tuple(_plan_identifier(item) for item in raw_plans)
    _require(
        list(plan_ids) == sorted(set(plan_ids)),
        "plan_ids must be sorted and unique",
    )
    objects = value.get("objects")
    _require(
        isinstance(objects, list) and bool(objects),
        "objects must be a non-empty array",
    )
    _require(
        value.get("credentials_recorded") is False,
        "binding manifest records credentials",
    )
    bindings: list[RawColdObjectBinding] = []
    for index, item in enumerate(objects):
        _require(isinstance(item, dict), f"objects[{index}] must be an object")
        _require(
            set(item)
            == {
                "object_id",
                "artifact_path",
                "source_object_id",
                "artifact_sha256",
                "artifact_size_bytes",
            },
            f"objects[{index}] fields changed",
        )
        object_id = _identifier(item.get("object_id"), "object_id")
        artifact_path = item.get("artifact_path")
        _require(
            isinstance(artifact_path, str) and bool(artifact_path.strip()),
            f"objects[{index}].artifact_path is invalid",
        )
        resolved = Path(artifact_path)
        if not resolved.is_absolute():
            resolved = source.parent / resolved
        digest = item.get("artifact_sha256")
        if digest is not None:
            digest = _digest(digest, "artifact_sha256")
        size = item.get("artifact_size_bytes")
        if size is not None:
            size = _positive_integer(size, "artifact_size_bytes")
        bindings.append(RawColdObjectBinding(
            object_id=object_id,
            artifact_path=resolved,
            catalog_version=catalog_version,
            plan_ids=plan_ids,
            dataset_id=dataset_id,
            dataset_revision=dataset_revision,
            source_object_id=_identifier(
                item.get("source_object_id"),
                "source_object_id",
            ),
            artifact_sha256=digest,
            artifact_size_bytes=size,
        ))
    return build_raw_cold_data_plane_package(
        bindings,
        output_dir=output_dir,
        package_id=package_id,
        max_artifact_bytes=max_artifact_bytes,
    )


def build_raw_cold_data_plane_package(
    bindings: Sequence[RawColdObjectBinding],
    *,
    output_dir: str | Path,
    package_id: str,
    max_artifact_bytes: int = DEFAULT_MAX_ARTIFACT_BYTES,
) -> dict[str, Any]:
    """Build and verify one deterministic, portable N3 package."""

    _identifier(package_id, "package_id")
    _positive_integer(max_artifact_bytes, "max_artifact_bytes")
    _require(bool(bindings), "at least one raw object binding is required")
    normalized = [_normalize_binding(binding) for binding in bindings]
    object_ids = [item["object_id"] for item in normalized]
    _require(
        len(object_ids) == len(set(object_ids)),
        "raw object bindings contain duplicate object IDs",
    )
    catalog_versions = {item["catalog_version"] for item in normalized}
    _require(
        len(catalog_versions) == 1,
        "all raw object bindings must use one catalog version",
    )
    catalog_version = next(iter(catalog_versions))
    plan_sets = {item["plan_ids"] for item in normalized}
    _require(
        len(plan_sets) == 1,
        "all raw objects must share one plan binding set because the Data "
        "Agent v1alpha1 manifest binds plans at representation scope",
    )
    normalized.sort(key=lambda item: item["object_id"])

    output = Path(output_dir).resolve()
    _require(not output.exists(), f"output directory already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f".{output.name}.", dir=output.parent))
    try:
        rows: list[dict[str, Any]] = []
        for item in normalized:
            object_id = item["object_id"]
            relative = f"artifacts/{object_id}/{REPRESENTATION_ID}.mp4"
            target = stage / Path(*PurePosixPath(relative).parts)
            actual_digest, actual_size = _copy_artifact(
                item["source"],
                target,
                max_artifact_bytes=max_artifact_bytes,
            )
            if item["expected_sha256"] is not None:
                _require(
                    actual_digest == item["expected_sha256"],
                    f"artifact SHA-256 mismatch for {object_id}",
                )
            if item["expected_size_bytes"] is not None:
                _require(
                    actual_size == item["expected_size_bytes"],
                    f"artifact size mismatch for {object_id}",
                )
            provenance = {
                "schema_version": RAW_OBJECT_PROVENANCE_SCHEMA_VERSION,
                "dataset_id": item["dataset_id"],
                "dataset_revision": item["dataset_revision"],
                "source_object_id": item["source_object_id"],
                "snapshot_semantics": "byte-exact-operator-supplied-source",
                "source_artifact_size_bytes": actual_size,
                "source_artifact_sha256": actual_digest,
            }
            rows.append({
                "object_id": object_id,
                "representation_id": REPRESENTATION_ID,
                "artifact_media_type": ARTIFACT_MEDIA_TYPE,
                "artifact_package_path": relative,
                "artifact_size_bytes": actual_size,
                "artifact_sha256": actual_digest,
                "catalog_version": catalog_version,
                "plan_ids": list(item["plan_ids"]),
                "provenance": provenance,
            })

        data_agent_manifest, object_catalog = _data_agent_documents(
            rows,
            catalog_version,
        )
        report = {
            "schema_version": RAW_COLD_DATA_PLANE_SCHEMA_VERSION,
            "status": RAW_COLD_DATA_PLANE_STATUS,
            "package_id": package_id,
            "route": _portable_route(),
            "catalog_version": catalog_version,
            "object_count": len(rows),
            "artifact_count": len(rows),
            "artifact_bytes": sum(row["artifact_size_bytes"] for row in rows),
            "objects": rows,
            "portable_data_agent_contract_complete": True,
            "deployment_binding_required": True,
            "runtime_execution_verified": False,
            "workflow_submitted": False,
            "llm_called": False,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }
        _assert_safe_metadata(report)
        documents = {
            DATA_AGENT_MANIFEST_PATH: _json_bytes(data_agent_manifest),
            OBJECT_CATALOG_PATH: _json_bytes(object_catalog),
            PACKAGE_MANIFEST_NAME: _json_bytes(report),
        }
        for relative, payload in documents.items():
            target = stage / Path(*PurePosixPath(relative).parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(payload)
        names = sorted(
            path.relative_to(stage).as_posix()
            for path in stage.rglob("*")
            if path.is_file()
        )
        (stage / CHECKSUMS_NAME).write_bytes(_checksums_bytes(stage, names))
        verify_raw_cold_data_plane_package(
            stage,
            max_artifact_bytes=max_artifact_bytes,
        )
        os.replace(stage, output)
    except BaseException:
        shutil.rmtree(stage, ignore_errors=True)
        raise

    summary = verify_raw_cold_data_plane_package(
        output,
        max_artifact_bytes=max_artifact_bytes,
    )
    summary["output_dir"] = str(output)
    return summary


def _verified_checksums(root: Path) -> dict[str, str]:
    checksum_path = root / CHECKSUMS_NAME
    _require(checksum_path.is_file(), f"{CHECKSUMS_NAME} is missing")
    try:
        raw = checksum_path.read_bytes()
        text = raw.decode("utf-8")
    except (OSError, UnicodeError) as exc:
        raise RawColdDataPlaneError(f"{CHECKSUMS_NAME} is unreadable") from exc
    lines = text.splitlines()
    _require(bool(lines), f"{CHECKSUMS_NAME} is empty")
    result: dict[str, str] = {}
    previous = ""
    for line in lines:
        parts = line.split("  ", 1)
        _require(len(parts) == 2, f"{CHECKSUMS_NAME} has a malformed row")
        digest = _digest(parts[0], f"{CHECKSUMS_NAME} digest")
        relative = _relative_path(parts[1], f"{CHECKSUMS_NAME} path")
        _require(relative > previous, f"{CHECKSUMS_NAME} is not sorted")
        _require(relative not in result, f"{CHECKSUMS_NAME} repeats a path")
        _require(relative != CHECKSUMS_NAME, f"{CHECKSUMS_NAME} binds itself")
        result[relative] = digest
        previous = relative
    _require(
        raw == _checksums_bytes(root, list(result)),
        f"{CHECKSUMS_NAME} is not canonical or does not match package bytes",
    )
    return result


def verify_raw_cold_data_plane_package(
    package_dir: str | Path,
    *,
    max_artifact_bytes: int = DEFAULT_MAX_ARTIFACT_BYTES,
) -> dict[str, Any]:
    """Re-derive raw identities and all standard Data Agent bindings."""

    _positive_integer(max_artifact_bytes, "max_artifact_bytes")
    root = Path(package_dir).resolve()
    _require(root.is_dir(), "raw/cold data-plane package is missing")
    files = _actual_files(root)
    checksums = _verified_checksums(root)
    _require(
        set(files) == set(checksums) | {CHECKSUMS_NAME},
        "package file set differs from SHA256SUMS",
    )

    report_raw = files.get(PACKAGE_MANIFEST_NAME)
    _require(report_raw is not None, f"{PACKAGE_MANIFEST_NAME} is missing")
    report_bytes = report_raw.read_bytes()
    report = _strict_json(report_bytes, PACKAGE_MANIFEST_NAME)
    _require(report_bytes == _json_bytes(report), "package manifest is not canonical")
    _require(set(report) == _REPORT_KEYS, "package manifest field set changed")
    _require(
        report["schema_version"] == RAW_COLD_DATA_PLANE_SCHEMA_VERSION,
        "package manifest schema is unsupported",
    )
    _require(report["status"] == RAW_COLD_DATA_PLANE_STATUS, "status changed")
    _identifier(report["package_id"], "package_id")
    catalog_version = _identifier(
        report["catalog_version"],
        "catalog_version",
    )
    _require(
        isinstance(report["route"], dict)
        and set(report["route"]) == _ROUTE_KEYS,
        "portable route field set changed",
    )
    _require(report["route"] == _portable_route(), "portable route changed")
    for name, expected in (
        ("portable_data_agent_contract_complete", True),
        ("deployment_binding_required", True),
        ("runtime_execution_verified", False),
        ("workflow_submitted", False),
        ("llm_called", False),
        ("credentials_recorded", False),
        ("eligible_for_scientific_claims", False),
    ):
        _require(report[name] is expected, f"package manifest {name} changed")
    _assert_safe_metadata(report)

    rows = report["objects"]
    _require(isinstance(rows, list) and bool(rows), "objects must be non-empty")
    for index, row in enumerate(rows):
        _require(isinstance(row, dict), f"objects[{index}] is not an object")
        _require(set(row) == _OBJECT_KEYS, f"objects[{index}] fields changed")
        _identifier(row["object_id"], f"objects[{index}].object_id")
    _require(
        [row.get("object_id") for row in rows]
        == sorted(row.get("object_id") for row in rows),
        "objects are not sorted",
    )
    _require(
        len({row.get("object_id") for row in rows}) == len(rows),
        "objects contain duplicates",
    )
    expected_files = {
        PACKAGE_MANIFEST_NAME,
        DATA_AGENT_MANIFEST_PATH,
        OBJECT_CATALOG_PATH,
    }
    total_bytes = 0
    for index, row in enumerate(rows):
        object_id = _identifier(row["object_id"], f"objects[{index}].object_id")
        _require(
            row["representation_id"] == REPRESENTATION_ID,
            f"representation changed for {object_id}",
        )
        _require(
            row["artifact_media_type"] == ARTIFACT_MEDIA_TYPE,
            f"artifact media type changed for {object_id}",
        )
        _require(
            row["catalog_version"] == catalog_version,
            f"catalog version changed for {object_id}",
        )
        relative = _relative_path(
            row["artifact_package_path"],
            f"artifact path for {object_id}",
        )
        expected_relative = f"artifacts/{object_id}/{REPRESENTATION_ID}.mp4"
        _require(
            relative == expected_relative,
            f"artifact path changed for {object_id}",
        )
        expected_files.add(relative)
        artifact = root / Path(*PurePosixPath(relative).parts)
        actual_digest, actual_size = _identify_artifact(
            artifact,
            max_artifact_bytes=max_artifact_bytes,
        )
        _require(
            actual_digest == _digest(row["artifact_sha256"], "artifact_sha256"),
            f"artifact SHA-256 mismatch for {object_id}",
        )
        _require(
            actual_size
            == _positive_integer(row["artifact_size_bytes"], "artifact_size_bytes"),
            f"artifact size mismatch for {object_id}",
        )
        plans = row["plan_ids"]
        _require(
            isinstance(plans, list) and bool(plans),
            "plan_ids must be non-empty",
        )
        _require(
            all(isinstance(plan_id, str) for plan_id in plans),
            "plan_ids must contain only strings",
        )
        _require(plans == sorted(set(plans)), "plan_ids are not canonical")
        for plan_id in plans:
            _plan_identifier(plan_id)
        provenance = row["provenance"]
        _require(
            isinstance(provenance, dict)
            and set(provenance) == _PROVENANCE_KEYS,
            f"provenance fields changed for {object_id}",
        )
        _require(
            provenance["schema_version"]
            == RAW_OBJECT_PROVENANCE_SCHEMA_VERSION,
            f"provenance schema changed for {object_id}",
        )
        for field in ("dataset_id", "dataset_revision", "source_object_id"):
            _identifier(provenance[field], f"provenance.{field}")
        _require(
            provenance["snapshot_semantics"]
            == "byte-exact-operator-supplied-source",
            f"snapshot semantics changed for {object_id}",
        )
        _require(
            provenance["source_artifact_sha256"] == actual_digest,
            f"source digest binding changed for {object_id}",
        )
        _require(
            _positive_integer(
                provenance["source_artifact_size_bytes"],
                "provenance.source_artifact_size_bytes",
            )
            == actual_size,
            f"source size binding changed for {object_id}",
        )
        total_bytes += actual_size

    _require(
        len({tuple(row["plan_ids"]) for row in rows}) == 1,
        "raw objects do not share one representation-scope plan binding set",
    )
    _require(set(checksums) == expected_files, "package contains unexpected files")
    object_count = _positive_integer(report["object_count"], "object_count")
    artifact_count = _positive_integer(
        report["artifact_count"],
        "artifact_count",
    )
    _require(
        object_count == len(rows) and artifact_count == len(rows),
        "package object/artifact counts changed",
    )
    _require(
        _positive_integer(report["artifact_bytes"], "artifact_bytes")
        == total_bytes,
        "artifact byte total changed",
    )

    expected_manifest, expected_catalog = _data_agent_documents(
        rows,
        catalog_version,
    )
    for relative, expected in (
        (DATA_AGENT_MANIFEST_PATH, expected_manifest),
        (OBJECT_CATALOG_PATH, expected_catalog),
    ):
        path = root / Path(*PurePosixPath(relative).parts)
        raw = path.read_bytes()
        actual = _strict_json(raw, relative)
        _require(raw == _json_bytes(actual), f"{relative} is not canonical")
        _require(actual == expected, f"{relative} does not match package binding")

    try:
        data_agent = load_data_agent_manifest(root / DATA_AGENT_MANIFEST_PATH)
    except Exception as exc:
        raise RawColdDataPlaneError("Data Agent manifest is invalid") from exc
    _require(data_agent.node_id == SOURCE_NODE_ID, "Data Agent node identity changed")
    _require(data_agent.object_catalog is not None, "object catalog is not loaded")
    _require(
        data_agent.object_catalog.catalog_version == catalog_version,
        "Data Agent catalog version changed",
    )
    for row in rows:
        for plan_id in row["plan_ids"]:
            resolved = data_agent.resolve(
                plan_id=plan_id,
                object_id=row["object_id"],
                representation_id=REPRESENTATION_ID,
                requested_location=SOURCE_LOCATION,
            )
            _require(
                resolved.path.is_relative_to(root),
                "artifact path escapes package",
            )
            _require(
                resolved.path
                == root
                / Path(*PurePosixPath(row["artifact_package_path"]).parts),
                "Data Agent path differs from package binding",
            )

    return {
        "schema_version": RAW_COLD_DATA_PLANE_SCHEMA_VERSION,
        "status": RAW_COLD_DATA_PLANE_VERIFIED_STATUS,
        "package_id": report["package_id"],
        "catalog_version": catalog_version,
        "source_node_id": SOURCE_NODE_ID,
        "source_location": SOURCE_LOCATION,
        "object_count": len(rows),
        "artifact_count": len(rows),
        "artifact_bytes": total_bytes,
        "portable_data_agent_contract_complete": True,
        "deployment_binding_required": True,
        "runtime_execution_verified": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "ARTIFACT_MEDIA_TYPE",
    "CHECKSUMS_NAME",
    "DATA_AGENT_MANIFEST_PATH",
    "DEFAULT_MAX_ARTIFACT_BYTES",
    "OBJECT_CATALOG_PATH",
    "PACKAGE_MANIFEST_NAME",
    "RAW_COLD_DATA_PLANE_SCHEMA_VERSION",
    "RAW_COLD_BINDINGS_SCHEMA_VERSION",
    "REPRESENTATION_ID",
    "RawColdDataPlaneError",
    "RawColdObjectBinding",
    "SOURCE_LOCATION",
    "SOURCE_NODE_ID",
    "build_raw_cold_data_plane_package",
    "build_raw_cold_data_plane_package_from_manifest",
    "verify_raw_cold_data_plane_package",
]
