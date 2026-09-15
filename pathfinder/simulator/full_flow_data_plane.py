"""Freeze real frame bundles into a portable N4 -> N7 -> N6 data plane.

This module builds the immutable *data-plane input* used by the simulator's
full Pathfinder vertical.  It deliberately contains no host address, URL, or
credential.  The package root is mounted at ``/data`` on the logical origin
node, so the exact same Data Agent manifest and object catalog can be used by
a container today and by a real N4 machine later.

Runtime execution is outside this module.  Producing this package proves that
the real semantic artifact is canonical and that its portable Data Agent
route is completely specified; it does not prove that FlowMesh dispatched a
workflow or that an LLM ran.
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
from ..frame_bundle import REPRESENTATION_ID
from ..frame_bundle_ingest import (
    DEFAULT_FRAME_BUNDLE_LIMITS,
    FRAME_BUNDLE_MEDIA_TYPE,
    FrameBundleLimits,
    validate_frame_bundle_bytes,
)
from .data_agent_semantic_vertical import (
    DATA_AGENT_SEMANTIC_EXECUTOR_NODE_ID,
    load_data_agent_frame_bundle_semantic_spec,
)


FULL_FLOW_DATA_PLANE_SCHEMA_VERSION = (
    "pathfinder.simulator-full-flow-data-plane/v1alpha1"
)
FULL_FLOW_DATA_PLANE_STATUS = "FROZEN_FULL_FLOW_DATA_PLANE"
FULL_FLOW_DATA_PLANE_VERIFIED_STATUS = "VERIFIED_FULL_FLOW_DATA_PLANE"

SOURCE_NODE_ID = "N4"
EXECUTOR_NODE_ID = "N7"
INFERENCE_NODE_ID = DATA_AGENT_SEMANTIC_EXECUTOR_NODE_ID
SOURCE_LOCATION = "origin-warm"

PACKAGE_MANIFEST_NAME = "full-flow-data-plane.json"
DATA_AGENT_MANIFEST_PATH = "config/data-agent-manifest.json"
OBJECT_CATALOG_PATH = "config/object-catalog.json"
CHECKSUMS_NAME = "SHA256SUMS"
PACKAGE_MOUNT_PATH = "/data"

_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_WINDOWS_ABSOLUTE = re.compile(r"(?:[A-Za-z]:[\\/]|\\\\)")
_CREDENTIAL_KEY = re.compile(
    r"(?:^|_)(?:api_?key|password|secret|token)(?:$|_)", re.IGNORECASE
)

_REPORT_KEYS = frozenset({
    "schema_version",
    "status",
    "package_id",
    "route",
    "catalog_version",
    "object_count",
    "artifact_count",
    "semantic_spec_count",
    "artifact_bytes",
    "frame_count",
    "objects",
    "portable_route_contract_complete",
    "runtime_execution_verified",
    "workflow_submitted",
    "llm_called",
    "credentials_recorded",
    "eligible_for_scientific_claims",
})

_ROUTE_KEYS = frozenset({
    "source_node_id",
    "executor_node_id",
    "inference_node_id",
    "source_location",
    "representation_id",
    "artifact_media_type",
    "data_agent_protocol",
    "package_mount_path",
    "data_agent_manifest_package_path",
    "data_agent_manifest_container_path",
    "object_catalog_package_path",
    "object_catalog_container_path",
    "artifact_root_package_path",
    "artifact_root_container_path",
})

_OBJECT_KEYS = frozenset({
    "object_id",
    "representation_id",
    "artifact_package_path",
    "artifact_container_path",
    "artifact_size_bytes",
    "artifact_sha256",
    "frame_count",
    "member_count",
    "manifest_sha256",
    "total_jpeg_bytes",
    "catalog_version",
    "plan_ids",
    "semantic_specs",
})

_SPEC_KEYS = frozenset({
    "package_path",
    "sha256",
    "semantic_run_id",
    "trial_key",
    "workload_id",
    "data_agent_plan_id",
})


class FullFlowDataPlaneError(RuntimeError):
    """Raised when a portable full-flow package cannot be trusted."""


@dataclass(frozen=True)
class FullFlowArtifactBinding:
    """Bind one real canonical artifact to the portable N4 Data Agent.

    ``artifact_sha256`` and ``artifact_size_bytes`` may be omitted for a new
    direct binding; the builder will freeze the observed values.  A binding
    derived from a semantic spec always carries both expected values.
    """

    object_id: str
    artifact_path: str | Path
    catalog_version: str
    plan_ids: tuple[str, ...]
    artifact_sha256: str | None = None
    artifact_size_bytes: int | None = None
    semantic_spec_paths: tuple[str | Path, ...] = ()


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowDataPlaneError(message)


def _identifier(value: Any, name: str) -> str:
    _require(isinstance(value, str), f"{name} must be a string")
    _require(_IDENTIFIER.fullmatch(value) is not None, f"{name} is invalid")
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


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


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
                raise FullFlowDataPlaneError(
                    f"{name} contains duplicate key {key!r}"
                )
            result[key] = child
        return result

    def reject_constant(value: str) -> None:
        raise FullFlowDataPlaneError(
            f"{name} contains non-finite number {value}"
        )

    try:
        value = json.loads(
            raw,
            object_pairs_hook=reject_duplicates,
            parse_constant=reject_constant,
        )
    except FullFlowDataPlaneError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowDataPlaneError(f"{name} is not valid JSON") from exc
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


def _container_path(value: Any, name: str) -> str:
    _require(isinstance(value, str), f"{name} must be a string")
    _require(
        value == PACKAGE_MOUNT_PATH or value.startswith(PACKAGE_MOUNT_PATH + "/"),
        f"{name} must be rooted at {PACKAGE_MOUNT_PATH}",
    )
    _require(".." not in PurePosixPath(value).parts, f"{name} is invalid")
    _require(str(PurePosixPath(value)) == value, f"{name} is not canonical")
    return value


def _assert_safe_metadata(value: Any, name: str = "metadata") -> None:
    """Reject endpoints, credentials, and accidental host paths."""

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
        _require(
            _WINDOWS_ABSOLUTE.match(value) is None,
            f"{name} contains a Windows host path",
        )
        if value.startswith("/"):
            _container_path(value, name)


def artifact_binding_from_semantic_spec(
    semantic_spec: str | Path,
    artifact_path: str | Path,
) -> FullFlowArtifactBinding:
    """Create a checked artifact binding from one frozen semantic spec."""

    try:
        spec = load_data_agent_frame_bundle_semantic_spec(semantic_spec)
    except Exception as exc:
        raise FullFlowDataPlaneError("semantic spec is invalid") from exc
    document = spec.document
    _require(
        document["representation_id"] == REPRESENTATION_ID,
        f"semantic spec representation_id must be {REPRESENTATION_ID}",
    )
    _require(
        document["semantic_executor_node_id"] == INFERENCE_NODE_ID,
        f"semantic spec inference node must be {INFERENCE_NODE_ID}",
    )
    _assert_safe_metadata(document, "semantic spec")
    return FullFlowArtifactBinding(
        object_id=document["artifact_object_id"],
        artifact_path=artifact_path,
        catalog_version=document["object_catalog_version"],
        plan_ids=(document["data_agent_plan_id"],),
        artifact_sha256=document["artifact_sha256"],
        artifact_size_bytes=document["artifact_size_bytes"],
        semantic_spec_paths=(semantic_spec,),
    )


def _merge_semantic_bindings(
    bindings: Sequence[FullFlowArtifactBinding],
) -> tuple[FullFlowArtifactBinding, ...]:
    grouped: dict[str, FullFlowArtifactBinding] = {}
    for binding in bindings:
        previous = grouped.get(binding.object_id)
        if previous is None:
            grouped[binding.object_id] = binding
            continue
        _require(
            Path(previous.artifact_path).resolve()
            == Path(binding.artifact_path).resolve(),
            f"object {binding.object_id} maps to multiple artifact files",
        )
        _require(
            previous.catalog_version == binding.catalog_version,
            f"object {binding.object_id} changes catalog version",
        )
        _require(
            previous.artifact_sha256 == binding.artifact_sha256
            and previous.artifact_size_bytes == binding.artifact_size_bytes,
            f"object {binding.object_id} changes artifact identity",
        )
        grouped[binding.object_id] = FullFlowArtifactBinding(
            object_id=binding.object_id,
            artifact_path=binding.artifact_path,
            catalog_version=binding.catalog_version,
            plan_ids=tuple(sorted(set(previous.plan_ids) | set(binding.plan_ids))),
            artifact_sha256=binding.artifact_sha256,
            artifact_size_bytes=binding.artifact_size_bytes,
            semantic_spec_paths=(
                *previous.semantic_spec_paths,
                *binding.semantic_spec_paths,
            ),
        )
    return tuple(grouped[key] for key in sorted(grouped))


def build_full_flow_data_plane_package_from_semantic_specs(
    semantic_spec_artifacts: Sequence[
        tuple[str | Path, str | Path]
    ],
    *,
    output_dir: str | Path,
    package_id: str,
    limits: FrameBundleLimits = DEFAULT_FRAME_BUNDLE_LIMITS,
) -> dict[str, Any]:
    """Freeze semantic specs and their real artifacts into one package.

    Each pair is ``(semantic_spec_path, artifact_path)``.  Multiple specs may
    refer to the same object; they are merged into one catalog entry.
    """

    bindings = [
        artifact_binding_from_semantic_spec(spec, artifact)
        for spec, artifact in semantic_spec_artifacts
    ]
    return build_full_flow_data_plane_package(
        _merge_semantic_bindings(bindings),
        output_dir=output_dir,
        package_id=package_id,
        limits=limits,
    )


def _normalized_binding(
    binding: FullFlowArtifactBinding,
    limits: FrameBundleLimits,
) -> dict[str, Any]:
    object_id = _identifier(binding.object_id, "object_id")
    catalog_version = _identifier(binding.catalog_version, "catalog_version")
    try:
        source = Path(binding.artifact_path).resolve()
    except (OSError, TypeError, ValueError) as exc:
        raise FullFlowDataPlaneError(
            f"artifact path for {object_id} is invalid"
        ) from exc
    _require(source.is_file(), f"artifact for {object_id} is missing")
    try:
        raw = source.read_bytes()
    except OSError as exc:
        raise FullFlowDataPlaneError(
            f"artifact for {object_id} is unreadable"
        ) from exc
    _require(bool(raw), f"artifact for {object_id} is empty")
    actual_sha256 = _sha256_bytes(raw)
    actual_size = len(raw)
    if binding.artifact_sha256 is not None:
        _require(
            _digest(binding.artifact_sha256, "artifact_sha256")
            == actual_sha256,
            f"artifact SHA-256 mismatch for {object_id}",
        )
    if binding.artifact_size_bytes is not None:
        _require(
            _positive_integer(
                binding.artifact_size_bytes, "artifact_size_bytes"
            )
            == actual_size,
            f"artifact size mismatch for {object_id}",
        )
    plan_ids = tuple(
        sorted({_identifier(value, "plan_id") for value in binding.plan_ids})
    )
    _require(bool(plan_ids), f"object {object_id} has no plan binding")

    try:
        bundle = validate_frame_bundle_bytes(
            raw,
            expected_object_id=object_id,
            expected_sha256=actual_sha256,
            expected_size_bytes=actual_size,
            artifact_media_type=FRAME_BUNDLE_MEDIA_TYPE,
            limits=limits,
        )
    except Exception as exc:
        raise FullFlowDataPlaneError(
            f"artifact for {object_id} is not a canonical frame bundle"
        ) from exc
    _assert_safe_metadata(
        bundle.source.to_dict(), f"frame bundle source for {object_id}"
    )

    specs: list[dict[str, Any]] = []
    seen_spec_digests: set[str] = set()
    for raw_spec_path in binding.semantic_spec_paths:
        try:
            spec = load_data_agent_frame_bundle_semantic_spec(raw_spec_path)
        except Exception as exc:
            raise FullFlowDataPlaneError(
                f"semantic spec for {object_id} is invalid"
            ) from exc
        document = spec.document
        _assert_safe_metadata(document, f"semantic spec for {object_id}")
        _require(
            document["artifact_object_id"] == object_id,
            f"semantic spec object differs from {object_id}",
        )
        _require(
            document["representation_id"] == REPRESENTATION_ID,
            f"semantic spec representation differs for {object_id}",
        )
        _require(
            document["artifact_sha256"] == actual_sha256,
            f"semantic spec artifact SHA-256 differs for {object_id}",
        )
        _require(
            document["artifact_size_bytes"] == actual_size,
            f"semantic spec artifact size differs for {object_id}",
        )
        _require(
            document["object_catalog_version"] == catalog_version,
            f"semantic spec catalog version differs for {object_id}",
        )
        _require(
            document["data_agent_plan_id"] in plan_ids,
            f"semantic spec plan is not bound for {object_id}",
        )
        _require(
            document["semantic_executor_node_id"] == INFERENCE_NODE_ID,
            f"semantic spec inference node differs for {object_id}",
        )
        payload = _json_bytes(document)
        digest = _sha256_bytes(payload)
        _require(
            digest not in seen_spec_digests,
            f"object {object_id} repeats a semantic spec",
        )
        seen_spec_digests.add(digest)
        specs.append({
            "document": document,
            "payload": payload,
            "sha256": digest,
            "package_path": f"semantic-specs/{digest}.json",
            "semantic_run_id": document["semantic_run_id"],
            "trial_key": document["trial_key"],
            "workload_id": document["workload_id"],
            "data_agent_plan_id": document["data_agent_plan_id"],
        })

    specs.sort(key=lambda item: (item["sha256"], item["trial_key"]))
    return {
        "object_id": object_id,
        "catalog_version": catalog_version,
        "plan_ids": plan_ids,
        "raw": raw,
        "artifact_sha256": actual_sha256,
        "artifact_size_bytes": actual_size,
        "bundle": bundle,
        "semantic_specs": specs,
    }


def _portable_route() -> dict[str, Any]:
    return {
        "source_node_id": SOURCE_NODE_ID,
        "executor_node_id": EXECUTOR_NODE_ID,
        "inference_node_id": INFERENCE_NODE_ID,
        "source_location": SOURCE_LOCATION,
        "representation_id": REPRESENTATION_ID,
        "artifact_media_type": FRAME_BUNDLE_MEDIA_TYPE,
        "data_agent_protocol": "pathfinder.data-agent/v1alpha1",
        "package_mount_path": PACKAGE_MOUNT_PATH,
        "data_agent_manifest_package_path": DATA_AGENT_MANIFEST_PATH,
        "data_agent_manifest_container_path": (
            f"{PACKAGE_MOUNT_PATH}/{DATA_AGENT_MANIFEST_PATH}"
        ),
        "object_catalog_package_path": OBJECT_CATALOG_PATH,
        "object_catalog_container_path": (
            f"{PACKAGE_MOUNT_PATH}/{OBJECT_CATALOG_PATH}"
        ),
        "artifact_root_package_path": "artifacts",
        "artifact_root_container_path": f"{PACKAGE_MOUNT_PATH}/artifacts",
    }


def _data_agent_documents(
    records: Sequence[Mapping[str, Any]],
    catalog_version: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    plan_ids = sorted({
        plan_id
        for record in records
        for plan_id in record["plan_ids"]
    })
    binding = {
        "location": SOURCE_LOCATION,
        "minimum_latency_ms": 0.0,
        "realized_cost": 0.0,
        "cache_hit": None,
    }
    manifest = {
        "schema_version": DATA_AGENT_MANIFEST_VERSION,
        "node_id": SOURCE_NODE_ID,
        "require_plan_binding": True,
        "object_catalog_path": "object-catalog.json",
        "representations": {
            REPRESENTATION_ID: {
                "kind": "artifact_uri",
                "media_type": FRAME_BUNDLE_MEDIA_TYPE,
                "default_binding": binding,
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
            record["object_id"]: {
                "representations": {
                    REPRESENTATION_ID: {
                        "path": "../" + record["artifact_package_path"],
                        "plan_paths": {
                            plan_id: "../" + record["artifact_package_path"]
                            for plan_id in record["plan_ids"]
                        },
                    }
                }
            }
            for record in records
        },
    }
    return manifest, catalog


def _checksum_bytes(documents: Mapping[str, bytes]) -> bytes:
    return b"".join(
        f"{_sha256_bytes(payload)}  {name}\n".encode("utf-8")
        for name, payload in sorted(documents.items())
    )


def build_full_flow_data_plane_package(
    bindings: Sequence[FullFlowArtifactBinding],
    *,
    output_dir: str | Path,
    package_id: str,
    limits: FrameBundleLimits = DEFAULT_FRAME_BUNDLE_LIMITS,
) -> dict[str, Any]:
    """Build and verify one deterministic portable data-plane package."""

    _identifier(package_id, "package_id")
    _require(isinstance(limits, FrameBundleLimits), "limits is invalid")
    _require(bool(bindings), "at least one artifact binding is required")
    normalized = [_normalized_binding(binding, limits) for binding in bindings]
    object_ids = [item["object_id"] for item in normalized]
    _require(
        len(object_ids) == len(set(object_ids)),
        "artifact bindings contain duplicate object IDs",
    )
    catalog_versions = {item["catalog_version"] for item in normalized}
    _require(
        len(catalog_versions) == 1,
        "all artifact bindings must use one catalog version",
    )
    catalog_version = next(iter(catalog_versions))
    normalized.sort(key=lambda item: item["object_id"])

    output = Path(output_dir).resolve()
    _require(not output.exists(), f"output directory already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(
        tempfile.mkdtemp(prefix=f".{output.name}.", dir=output.parent)
    )
    try:
        documents: dict[str, bytes] = {}
        rows: list[dict[str, Any]] = []
        seen_spec_paths: set[str] = set()
        for item in normalized:
            object_id = item["object_id"]
            artifact_path = (
                f"artifacts/{object_id}/{REPRESENTATION_ID}.tar"
            )
            artifact_container_path = f"{PACKAGE_MOUNT_PATH}/{artifact_path}"
            specs: list[dict[str, Any]] = []
            for spec in item["semantic_specs"]:
                package_path = spec["package_path"]
                previous = documents.get(package_path)
                if previous is not None:
                    _require(
                        previous == spec["payload"],
                        "semantic spec digest collision",
                    )
                else:
                    documents[package_path] = spec["payload"]
                seen_spec_paths.add(package_path)
                specs.append({
                    "package_path": package_path,
                    "sha256": spec["sha256"],
                    "semantic_run_id": spec["semantic_run_id"],
                    "trial_key": spec["trial_key"],
                    "workload_id": spec["workload_id"],
                    "data_agent_plan_id": spec["data_agent_plan_id"],
                })
            bundle = item["bundle"]
            documents[artifact_path] = item["raw"]
            rows.append({
                "object_id": object_id,
                "representation_id": REPRESENTATION_ID,
                "artifact_package_path": artifact_path,
                "artifact_container_path": artifact_container_path,
                "artifact_size_bytes": item["artifact_size_bytes"],
                "artifact_sha256": item["artifact_sha256"],
                "frame_count": bundle.frame_count,
                "member_count": bundle.member_count,
                "manifest_sha256": bundle.manifest_sha256,
                "total_jpeg_bytes": bundle.total_jpeg_bytes,
                "catalog_version": catalog_version,
                "plan_ids": list(item["plan_ids"]),
                "semantic_specs": specs,
            })

        data_agent_manifest, object_catalog = _data_agent_documents(
            rows, catalog_version
        )
        documents[DATA_AGENT_MANIFEST_PATH] = _json_bytes(data_agent_manifest)
        documents[OBJECT_CATALOG_PATH] = _json_bytes(object_catalog)
        report = {
            "schema_version": FULL_FLOW_DATA_PLANE_SCHEMA_VERSION,
            "status": FULL_FLOW_DATA_PLANE_STATUS,
            "package_id": package_id,
            "route": _portable_route(),
            "catalog_version": catalog_version,
            "object_count": len(rows),
            "artifact_count": len(rows),
            "semantic_spec_count": len(seen_spec_paths),
            "artifact_bytes": sum(
                row["artifact_size_bytes"] for row in rows
            ),
            "frame_count": sum(row["frame_count"] for row in rows),
            "objects": rows,
            "portable_route_contract_complete": True,
            "runtime_execution_verified": False,
            "workflow_submitted": False,
            "llm_called": False,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }
        _assert_safe_metadata(report)
        documents[PACKAGE_MANIFEST_NAME] = _json_bytes(report)

        for relative, payload in documents.items():
            target = stage / Path(*PurePosixPath(relative).parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(payload)
        (stage / CHECKSUMS_NAME).write_bytes(_checksum_bytes(documents))
        verify_full_flow_data_plane_package(stage, limits=limits)
        os.replace(stage, output)
    except Exception:
        shutil.rmtree(stage, ignore_errors=True)
        raise

    summary = verify_full_flow_data_plane_package(output, limits=limits)
    summary["output_dir"] = str(output)
    return summary


def _read_checksums(root: Path) -> dict[str, str]:
    path = root / CHECKSUMS_NAME
    _require(path.is_file(), f"{CHECKSUMS_NAME} is missing")
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FullFlowDataPlaneError(f"{CHECKSUMS_NAME} is unreadable") from exc
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
    return result


def _actual_package_files(root: Path) -> set[str]:
    files: set[str] = set()
    for path in root.rglob("*"):
        if path.is_symlink():
            raise FullFlowDataPlaneError("package contains a symbolic link")
        if path.is_file():
            files.add(path.relative_to(root).as_posix())
    return files


def verify_full_flow_data_plane_package(
    package_dir: str | Path,
    *,
    limits: FrameBundleLimits = DEFAULT_FRAME_BUNDLE_LIMITS,
) -> dict[str, Any]:
    """Re-derive every package binding without trusting recorded digests."""

    _require(isinstance(limits, FrameBundleLimits), "limits is invalid")
    root = Path(package_dir).resolve()
    _require(root.is_dir(), "full-flow data-plane package is missing")
    checksums = _read_checksums(root)
    actual_files = _actual_package_files(root)
    _require(
        actual_files == set(checksums) | {CHECKSUMS_NAME},
        "package file set differs from SHA256SUMS",
    )
    for relative, expected in checksums.items():
        target = root / Path(*PurePosixPath(relative).parts)
        _require(target.is_file(), f"checksummed file is missing: {relative}")
        _require(
            _sha256_bytes(target.read_bytes()) == expected,
            f"checksum mismatch for {relative}",
        )

    report_path = root / PACKAGE_MANIFEST_NAME
    report = _strict_json(report_path.read_bytes(), PACKAGE_MANIFEST_NAME)
    _require(set(report) == _REPORT_KEYS, "package manifest field set changed")
    _require(
        report["schema_version"] == FULL_FLOW_DATA_PLANE_SCHEMA_VERSION,
        "package manifest schema is unsupported",
    )
    _require(report["status"] == FULL_FLOW_DATA_PLANE_STATUS, "status changed")
    _identifier(report["package_id"], "package_id")
    _identifier(report["catalog_version"], "catalog_version")
    _require(
        isinstance(report["route"], dict)
        and set(report["route"]) == _ROUTE_KEYS,
        "portable route field set changed",
    )
    _require(report["route"] == _portable_route(), "portable route changed")
    for name, expected in (
        ("portable_route_contract_complete", True),
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
        [row["object_id"] for row in rows]
        == sorted(row["object_id"] for row in rows),
        "objects are not sorted",
    )
    _require(
        len({row["object_id"] for row in rows}) == len(rows),
        "objects contain duplicates",
    )
    expected_files = {
        PACKAGE_MANIFEST_NAME,
        DATA_AGENT_MANIFEST_PATH,
        OBJECT_CATALOG_PATH,
    }
    total_bytes = 0
    total_frames = 0
    spec_paths: set[str] = set()
    for index, row in enumerate(rows):
        object_id = _identifier(row["object_id"], f"objects[{index}].object_id")
        _require(
            row["representation_id"] == REPRESENTATION_ID,
            f"representation changed for {object_id}",
        )
        _require(
            row["catalog_version"] == report["catalog_version"],
            f"catalog version changed for {object_id}",
        )
        artifact_relative = _relative_path(
            row["artifact_package_path"], "artifact package path"
        )
        expected_artifact_relative = (
            f"artifacts/{object_id}/{REPRESENTATION_ID}.tar"
        )
        _require(
            artifact_relative == expected_artifact_relative,
            f"artifact path changed for {object_id}",
        )
        _require(
            _container_path(
                row["artifact_container_path"], "artifact container path"
            )
            == f"{PACKAGE_MOUNT_PATH}/{artifact_relative}",
            f"artifact container path changed for {object_id}",
        )
        artifact_size = _positive_integer(
            row["artifact_size_bytes"], "artifact_size_bytes"
        )
        artifact_digest = _digest(
            row["artifact_sha256"], "artifact_sha256"
        )
        artifact_bytes = (
            root / Path(*PurePosixPath(artifact_relative).parts)
        ).read_bytes()
        try:
            bundle = validate_frame_bundle_bytes(
                artifact_bytes,
                expected_object_id=object_id,
                expected_sha256=artifact_digest,
                expected_size_bytes=artifact_size,
                artifact_media_type=FRAME_BUNDLE_MEDIA_TYPE,
                limits=limits,
            )
        except Exception as exc:
            raise FullFlowDataPlaneError(
                f"artifact for {object_id} failed canonical validation"
            ) from exc
        _assert_safe_metadata(
            bundle.source.to_dict(), f"frame bundle source for {object_id}"
        )
        _require(
            bundle.frame_count
            == _positive_integer(row["frame_count"], "frame_count"),
            "frame count changed",
        )
        _require(
            bundle.member_count
            == _positive_integer(row["member_count"], "member_count"),
            "member count changed",
        )
        _digest(row["manifest_sha256"], "manifest_sha256")
        _require(
            bundle.manifest_sha256 == row["manifest_sha256"],
            "embedded manifest digest changed",
        )
        _positive_integer(row["total_jpeg_bytes"], "total_jpeg_bytes")
        _require(
            bundle.total_jpeg_bytes == row["total_jpeg_bytes"],
            "total JPEG bytes changed",
        )
        plan_ids = row["plan_ids"]
        _require(
            isinstance(plan_ids, list) and bool(plan_ids),
            f"object {object_id} has no plan IDs",
        )
        _require(
            all(isinstance(plan_id, str) for plan_id in plan_ids),
            f"object {object_id} has a non-string plan ID",
        )
        _require(
            plan_ids == sorted(set(plan_ids)),
            f"object {object_id} plan IDs are not canonical",
        )
        for plan_id in plan_ids:
            _identifier(plan_id, "plan_id")
        specs = row["semantic_specs"]
        _require(isinstance(specs, list), "semantic_specs must be an array")
        for spec_index, spec_row in enumerate(specs):
            _require(
                isinstance(spec_row, dict) and set(spec_row) == _SPEC_KEYS,
                f"semantic_specs[{spec_index}] fields changed",
            )
            _digest(spec_row["sha256"], "semantic spec sha256")
            _require(
                isinstance(spec_row["trial_key"], str),
                "semantic spec trial_key is invalid",
            )
        _require(
            specs == sorted(
                specs, key=lambda item: (item["sha256"], item["trial_key"])
            ),
            "semantic specs are not sorted",
        )
        for spec_row in specs:
            relative = _relative_path(
                spec_row["package_path"], "semantic spec package path"
            )
            digest = _digest(spec_row["sha256"], "semantic spec sha256")
            _require(
                relative == f"semantic-specs/{digest}.json",
                "semantic spec package path changed",
            )
            packaged_path = root / Path(*PurePosixPath(relative).parts)
            _require(
                _sha256_bytes(packaged_path.read_bytes()) == digest,
                "semantic spec digest changed",
            )
            try:
                spec = load_data_agent_frame_bundle_semantic_spec(packaged_path)
            except Exception as exc:
                raise FullFlowDataPlaneError(
                    "packaged semantic spec is invalid"
                ) from exc
            document = spec.document
            _assert_safe_metadata(document, "packaged semantic spec")
            for key in (
                "semantic_run_id",
                "trial_key",
                "workload_id",
                "data_agent_plan_id",
            ):
                _require(
                    document[key] == spec_row[key],
                    f"semantic spec {key} binding changed",
                )
            _require(
                document["artifact_object_id"] == object_id,
                "spec object changed",
            )
            _require(
                document["artifact_sha256"] == artifact_digest,
                "spec digest changed",
            )
            _require(
                document["artifact_size_bytes"] == artifact_size,
                "spec size changed",
            )
            _require(
                document["object_catalog_version"] == report["catalog_version"],
                "spec catalog version changed",
            )
            _require(
                document["data_agent_plan_id"] in plan_ids,
                "spec plan is not in the Data Agent binding",
            )
            _require(
                document["semantic_executor_node_id"] == INFERENCE_NODE_ID,
                "spec inference node changed",
            )
            _require(
                relative not in spec_paths,
                "semantic spec binding is duplicated",
            )
            spec_paths.add(relative)
        expected_files.add(artifact_relative)
        expected_files.update(spec_row["package_path"] for spec_row in specs)
        total_bytes += artifact_size
        total_frames += bundle.frame_count

    _require(
        _positive_integer(report["object_count"], "object_count") == len(rows),
        "object_count changed",
    )
    _require(
        _positive_integer(report["artifact_count"], "artifact_count")
        == len(rows),
        "artifact_count changed",
    )
    _require(
        isinstance(report["semantic_spec_count"], int)
        and not isinstance(report["semantic_spec_count"], bool)
        and report["semantic_spec_count"] >= 0
        and report["semantic_spec_count"] == len(spec_paths),
        "semantic_spec_count changed",
    )
    _require(
        _positive_integer(report["artifact_bytes"], "artifact_bytes")
        == total_bytes,
        "artifact_bytes changed",
    )
    _require(
        _positive_integer(report["frame_count"], "frame_count")
        == total_frames,
        "frame_count changed",
    )
    _require(set(checksums) == expected_files, "manifest file set changed")

    expected_manifest, expected_catalog = _data_agent_documents(
        rows, report["catalog_version"]
    )
    _require(
        (root / DATA_AGENT_MANIFEST_PATH).read_bytes()
        == _json_bytes(expected_manifest),
        "Data Agent manifest does not match the portable route",
    )
    _require(
        (root / OBJECT_CATALOG_PATH).read_bytes()
        == _json_bytes(expected_catalog),
        "object catalog does not match the portable route",
    )
    try:
        manifest = load_data_agent_manifest(root / DATA_AGENT_MANIFEST_PATH)
    except Exception as exc:
        raise FullFlowDataPlaneError("Data Agent manifest is invalid") from exc
    _require(manifest.node_id == SOURCE_NODE_ID, "Data Agent source node changed")
    _require(manifest.require_plan_binding is True, "plan binding was disabled")
    _require(manifest.object_catalog is not None, "object catalog is missing")
    _require(
        manifest.object_catalog.catalog_version == report["catalog_version"],
        "loaded catalog version changed",
    )
    for row in rows:
        for plan_id in row["plan_ids"]:
            resolved = manifest.resolve(
                plan_id=plan_id,
                object_id=row["object_id"],
                representation_id=REPRESENTATION_ID,
                requested_location=SOURCE_LOCATION,
            )
            _require(
                resolved.path.is_relative_to(root),
                "Data Agent artifact path escapes the package",
            )
            _require(
                resolved.path
                == root / Path(*PurePosixPath(row["artifact_package_path"]).parts),
                "Data Agent artifact path differs from the package binding",
            )

    return {
        "schema_version": FULL_FLOW_DATA_PLANE_SCHEMA_VERSION,
        "status": FULL_FLOW_DATA_PLANE_VERIFIED_STATUS,
        "package_id": report["package_id"],
        "catalog_version": report["catalog_version"],
        "object_count": len(rows),
        "artifact_count": len(rows),
        "semantic_spec_count": len(spec_paths),
        "artifact_bytes": total_bytes,
        "frame_count": total_frames,
        "source_node_id": SOURCE_NODE_ID,
        "executor_node_id": EXECUTOR_NODE_ID,
        "inference_node_id": INFERENCE_NODE_ID,
        "portable_route_contract_complete": True,
        "runtime_execution_verified": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "CHECKSUMS_NAME",
    "DATA_AGENT_MANIFEST_PATH",
    "EXECUTOR_NODE_ID",
    "FullFlowArtifactBinding",
    "FullFlowDataPlaneError",
    "INFERENCE_NODE_ID",
    "OBJECT_CATALOG_PATH",
    "PACKAGE_MANIFEST_NAME",
    "SOURCE_NODE_ID",
    "artifact_binding_from_semantic_spec",
    "build_full_flow_data_plane_package",
    "build_full_flow_data_plane_package_from_semantic_specs",
    "verify_full_flow_data_plane_package",
]
