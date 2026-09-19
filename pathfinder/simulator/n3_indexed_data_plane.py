"""Freeze real source-side temporal projections beside N3 raw videos.

The legacy N3 package contains only complete MP4 objects.  That is the right
source of truth, but it means an ``indexed-raw`` route can execute only a
full-object HTTP Range.  This module upgrades a verified legacy package with
one deterministic, decodable frame bundle per object.  The bundle is produced
at freeze time from a declared temporal window and is served by the same N3
Data Agent as a private execution representation.

The source MP4 remains authoritative.  The projection row binds its source
digest, temporal window, exact bundle digest, and frame metadata; it is not
published as an alternative logical representation in the semantic matrix.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Mapping, Sequence

from ..data_agent_manifest import (
    DATA_AGENT_MANIFEST_VERSION,
    DATA_OBJECT_CATALOG_VERSION,
    load_data_agent_manifest,
)
from ..frame_bundle import (
    FRAME_BUNDLE_SCHEMA_VERSION,
    JPEG_OPTIMIZE,
    JPEG_QUALITY,
    OBJECT_MANIFEST_NAME,
    SOURCE_REPRESENTATION_ID,
    deterministic_frame_bundle_tar,
)
from ..frame_bundle_ingest import (
    DEFAULT_FRAME_BUNDLE_LIMITS,
    FRAME_BUNDLE_MEDIA_TYPE,
    FrameBundleLimits,
    validate_frame_bundle_bytes,
)
from ..video_prep import SampledImage, sample_video
from .raw_cold_data_plane import (
    ARTIFACT_MEDIA_TYPE as RAW_MEDIA_TYPE,
    CHECKSUMS_NAME,
    DATA_AGENT_MANIFEST_PATH,
    OBJECT_CATALOG_PATH,
    PACKAGE_MANIFEST_NAME,
    REPRESENTATION_ID as RAW_REPRESENTATION_ID,
    SOURCE_LOCATION,
    SOURCE_NODE_ID,
    verify_raw_cold_data_plane_package,
)


N3_INDEXED_DATA_PLANE_SCHEMA_VERSION = (
    "pathfinder.simulator-n3-indexed-data-plane/v1alpha1"
)
N3_INDEXED_DATA_PLANE_STATUS = "FROZEN_N3_INDEXED_DATA_PLANE"
N3_INDEXED_DATA_PLANE_VERIFIED_STATUS = "VERIFIED_N3_INDEXED_DATA_PLANE"
INDEXED_REPRESENTATION_ID = "indexed_temporal_frame_bundle"
INDEXED_PROVENANCE_SCHEMA_VERSION = (
    "pathfinder.n3-indexed-temporal-projection/v1alpha1"
)
DEFAULT_FRAME_COUNT = 8
DEFAULT_JPEG_MAX_DIMENSION = 768
DEFAULT_TEMPORAL_START_FRACTION = 0.25
DEFAULT_TEMPORAL_END_FRACTION = 0.75

# The legacy projection picks a fixed middle window with no knowledge of the
# question. The query-aware projection decodes the interval a temporal index
# selected for one specific public question; the interval is carried as a
# fraction pair like any other window, but the provenance that produced it is
# frozen alongside it so the selection can be audited rather than assumed.
UNIFORM_MIDPOINT_SAMPLING_METHOD = "uniform-midpoint-temporal-window"
TEMPORAL_INDEX_SELECTED_SAMPLING_METHOD = "temporal-index-selected-interval"
_SAMPLING_METHODS = frozenset({
    UNIFORM_MIDPOINT_SAMPLING_METHOD,
    TEMPORAL_INDEX_SELECTED_SAMPLING_METHOD,
})
_SELECTION_PROVENANCE_KEYS = frozenset({
    "action_id",
    "anchor_window_ordinals",
    "expansion_basis",
    "fallback_used",
    "max_selected_windows",
    "anchor_top_k",
    "merged_intervals_seconds",
    "public_question_sha256",
    "relation",
    "selected_window_ordinals",
    "temporal_index_package_sha256",
})

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+|-]{0,255}\Z")
_REPORT_KEYS = {
    "schema_version",
    "status",
    "package_id",
    "catalog_version",
    "object_count",
    "artifact_count",
    "artifact_bytes",
    "objects",
    "source_raw_package_binding",
    "selection_policy",
    "portable_data_agent_contract_complete",
    "deployment_binding_required",
    "runtime_execution_verified",
    "workflow_submitted",
    "llm_called",
    "credentials_recorded",
    "eligible_for_scientific_claims",
}
_ROW_KEYS = {
    "object_id",
    "representation_id",
    "artifact_media_type",
    "artifact_package_path",
    "artifact_size_bytes",
    "artifact_sha256",
    "catalog_version",
    "plan_ids",
    "provenance",
}
_SOURCE_BINDING_KEYS = {
    "package_id",
    "catalog_version",
    "manifest_sha256",
    "checksums_sha256",
}


class N3IndexedDataPlaneError(RuntimeError):
    """Raised when an N3 indexed package is not exact and reproducible."""


@dataclass(frozen=True)
class N3TemporalSelectionPolicy:
    frame_count: int = DEFAULT_FRAME_COUNT
    jpeg_max_dimension: int = DEFAULT_JPEG_MAX_DIMENSION
    temporal_start_fraction: float = DEFAULT_TEMPORAL_START_FRACTION
    temporal_end_fraction: float = DEFAULT_TEMPORAL_END_FRACTION
    sampling_method: str = UNIFORM_MIDPOINT_SAMPLING_METHOD
    selection_provenance: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        _require(
            type(self.frame_count) is int and 0 < self.frame_count <= 32,
            "frame_count must be within 1..32",
        )
        _require(
            type(self.jpeg_max_dimension) is int
            and 0 < self.jpeg_max_dimension <= 4096,
            "jpeg_max_dimension must be within 1..4096",
        )
        start = self.temporal_start_fraction
        end = self.temporal_end_fraction
        _require(
            not isinstance(start, bool)
            and not isinstance(end, bool)
            and isinstance(start, (int, float))
            and isinstance(end, (int, float))
            and 0.0 <= float(start) < float(end) <= 1.0,
            "temporal selection fractions are invalid",
        )
        _require(
            self.sampling_method in _SAMPLING_METHODS,
            "unsupported N3 temporal sampling method",
        )
        if self.sampling_method == TEMPORAL_INDEX_SELECTED_SAMPLING_METHOD:
            _require(
                isinstance(self.selection_provenance, Mapping)
                and set(self.selection_provenance) == _SELECTION_PROVENANCE_KEYS,
                "a query-aware projection requires its full selection provenance",
            )
            _require(
                self.selection_provenance["fallback_used"] is False,
                "a query-aware projection must not record a fallback selection",
            )
        else:
            _require(
                self.selection_provenance is None,
                "a fixed-window projection cannot carry selection provenance",
            )

    def to_dict(self) -> dict[str, Any]:
        document: dict[str, Any] = {
            "selection_semantics": "source-decoded-temporal-frame-bundle",
            "sampling_method": self.sampling_method,
            "frame_count": self.frame_count,
            "jpeg_max_dimension": self.jpeg_max_dimension,
            "temporal_window_fraction": [
                float(self.temporal_start_fraction),
                float(self.temporal_end_fraction),
            ],
            "partial_mp4_byte_range_claimed": False,
            "source_side_projection_executed": True,
        }
        if self.selection_provenance is not None:
            document["temporal_index_selection"] = json.loads(
                _canonical(dict(self.selection_provenance)).decode("utf-8")
            )
            document["query_aware_selection"] = True
        return document


FrameSampler = Callable[..., tuple[Sequence[SampledImage], float]]


def _require(condition: object, message: str) -> None:
    if not condition:
        raise N3IndexedDataPlaneError(message)


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _pretty(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
        + "\n"
    ).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _digest(value: Any, label: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{label} is not lowercase SHA-256",
    )
    return value


def _identifier(value: Any, label: str) -> str:
    _require(
        isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
        f"{label} is invalid",
    )
    return value


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise N3IndexedDataPlaneError(f"cannot read {label}") from exc
    _require(isinstance(value, dict), f"{label} must be an object")
    _require(raw == _pretty(value), f"{label} is not canonical")
    return value


def _hash_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
                size += len(block)
    except OSError as exc:
        raise N3IndexedDataPlaneError("cannot read package artifact") from exc
    return digest.hexdigest(), size


def _software_versions() -> dict[str, str | None]:
    result: dict[str, str | None] = {}
    for name in ("av", "Pillow", "pathfinder-minimal"):
        try:
            result[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            result[name] = None
    return result


def _bundle(
    source: Path,
    raw_row: Mapping[str, Any],
    policy: N3TemporalSelectionPolicy,
    sampler: FrameSampler,
) -> tuple[bytes, dict[str, Any], float]:
    sampled_raw, duration = sampler(
        source,
        frame_count=policy.frame_count,
        jpeg_max_dimension=policy.jpeg_max_dimension,
        temporal_start_fraction=policy.temporal_start_fraction,
        temporal_end_fraction=policy.temporal_end_fraction,
    )
    sampled = tuple(sampled_raw)
    _require(
        len(sampled) == policy.frame_count,
        "N3 temporal sampler returned the wrong frame count",
    )
    frames: list[dict[str, Any]] = []
    members: list[tuple[str, bytes]] = []
    for index, image in enumerate(sampled):
        _require(
            isinstance(image, SampledImage)
            and image.frame_index == index
            and isinstance(image.jpeg_bytes, bytes)
            and bool(image.jpeg_bytes),
            f"N3 temporal sampler returned invalid frame {index}",
        )
        member = f"frames/{index:03d}.jpg"
        frames.append({
            "frame_index": index,
            "timestamp_seconds": image.timestamp_seconds,
            "width": image.width,
            "height": image.height,
            "path": member,
            "jpeg_size_bytes": len(image.jpeg_bytes),
            "jpeg_sha256": _sha256(image.jpeg_bytes),
        })
        members.append((member, image.jpeg_bytes))
    policy_sha = _sha256(_canonical(policy.to_dict()))
    manifest = {
        "schema_version": FRAME_BUNDLE_SCHEMA_VERSION,
        "representation_id": "sampled_frame_bundle",
        "object_id": raw_row["object_id"],
        "source_video_id": raw_row["object_id"],
        "source_video_filename": source.name,
        "source_video_size_bytes": raw_row["artifact_size_bytes"],
        "source_video_sha256": raw_row["artifact_sha256"],
        "source_duration_seconds": float(duration),
        "sampling": {
            "method": policy.sampling_method,
            "frame_count": policy.frame_count,
            "jpeg_max_dimension": policy.jpeg_max_dimension,
            "jpeg_quality": JPEG_QUALITY,
            "jpeg_optimize": JPEG_OPTIMIZE,
        },
        "source_frame_descriptions": {
            "representation_id": SOURCE_REPRESENTATION_ID,
            "path": "n3-indexed-temporal-selection-policy.json",
            "sha256": policy_sha,
        },
        "generation_manifest_sha256": policy_sha,
        "frames": frames,
        "frame_count": len(frames),
        "total_jpeg_bytes": sum(row["jpeg_size_bytes"] for row in frames),
        "software_versions": _software_versions(),
        "historical_visual_bytes_retained": False,
        "sampling_alignment_statement": (
            "Frames are aligned with the frozen sampling metadata and do "
            "not claim byte identity with the historical visual input."
        ),
        "claims_byte_identity_with_historical_visual_input": False,
        "credentials_recorded": False,
        "llm_called": False,
        "network_calls_performed": False,
    }
    manifest_bytes = _pretty(manifest)
    artifact = deterministic_frame_bundle_tar(
        [(OBJECT_MANIFEST_NAME, manifest_bytes), *members]
    )
    validate_frame_bundle_bytes(
        artifact,
        expected_object_id=str(raw_row["object_id"]),
        expected_sha256=_sha256(artifact),
        expected_size_bytes=len(artifact),
        artifact_media_type=FRAME_BUNDLE_MEDIA_TYPE,
        limits=DEFAULT_FRAME_BUNDLE_LIMITS,
    )
    return artifact, manifest, float(duration)


def _data_agent_documents(
    rows: Sequence[Mapping[str, Any]],
    catalog_version: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    plan_ids = sorted({
        plan_id for row in rows for plan_id in row["plan_ids"]
    })
    binding = {
        "location": SOURCE_LOCATION,
        "minimum_latency_ms": 0.0,
        "realized_cost": 0.0,
        "cache_hit": False,
    }
    representations = {
        RAW_REPRESENTATION_ID: {
            "kind": "artifact_uri",
            "media_type": RAW_MEDIA_TYPE,
            "default_binding": dict(binding),
            "plan_bindings": {
                plan_id: dict(binding) for plan_id in plan_ids
            },
        },
        INDEXED_REPRESENTATION_ID: {
            "kind": "artifact_uri",
            "media_type": FRAME_BUNDLE_MEDIA_TYPE,
            "default_binding": dict(binding),
            "plan_bindings": {
                plan_id: dict(binding) for plan_id in plan_ids
            },
        },
    }
    by_object: dict[str, dict[str, Any]] = {}
    for row in rows:
        path = "../" + str(row["artifact_package_path"])
        by_object.setdefault(str(row["object_id"]), {"representations": {}})
        by_object[str(row["object_id"])]["representations"][
            str(row["representation_id"])
        ] = {
            "path": path,
            "plan_paths": {plan_id: path for plan_id in row["plan_ids"]},
        }
    manifest = {
        "schema_version": DATA_AGENT_MANIFEST_VERSION,
        "node_id": SOURCE_NODE_ID,
        "require_plan_binding": True,
        "object_catalog_path": "object-catalog.json",
        "representations": representations,
    }
    catalog = {
        "schema_version": DATA_OBJECT_CATALOG_VERSION,
        "catalog_version": catalog_version,
        "objects": by_object,
    }
    return manifest, catalog


def _checksums(root: Path) -> bytes:
    rows = []
    names = sorted(
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() and path.name != CHECKSUMS_NAME
    )
    for name in names:
        digest, _size = _hash_file(root / Path(*PurePosixPath(name).parts))
        rows.append(f"{digest}  {name}\n")
    return "".join(rows).encode("utf-8")


def build_n3_indexed_data_plane_package(
    source_raw_package_dir: str | Path,
    *,
    output_dir: str | Path,
    package_id: str,
    policy: N3TemporalSelectionPolicy = N3TemporalSelectionPolicy(),
    sampler: FrameSampler = sample_video,
) -> dict[str, Any]:
    """Upgrade one verified raw package with real N3 temporal projections."""

    source_root = Path(source_raw_package_dir).resolve()
    source_summary = verify_raw_cold_data_plane_package(source_root)
    _identifier(package_id, "package_id")
    raw_report = _read_json(source_root / PACKAGE_MANIFEST_NAME, "raw package")
    raw_rows = [dict(row) for row in raw_report["objects"]]
    policy_document = policy.to_dict()
    target = Path(output_dir).resolve()
    _require(not target.exists(), f"output directory already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging_parent = Path(tempfile.mkdtemp(prefix=".n3-indexed-", dir=target.parent))
    stage = staging_parent / "package"
    try:
        stage.mkdir()
        rows: list[dict[str, Any]] = []
        for raw_row in raw_rows:
            object_id = str(raw_row["object_id"])
            raw_relative = str(raw_row["artifact_package_path"])
            raw_source = source_root / Path(*PurePosixPath(raw_relative).parts)
            raw_target = stage / Path(*PurePosixPath(raw_relative).parts)
            raw_target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(raw_source, raw_target)
            rows.append(dict(raw_row))

            artifact, embedded, duration = _bundle(
                raw_source,
                raw_row,
                policy,
                sampler,
            )
            indexed_relative = (
                f"artifacts/{object_id}/{INDEXED_REPRESENTATION_ID}.tar"
            )
            indexed_target = stage / Path(*PurePosixPath(indexed_relative).parts)
            indexed_target.write_bytes(artifact)
            embedded_sha = _sha256(_pretty(embedded))
            rows.append({
                "object_id": object_id,
                "representation_id": INDEXED_REPRESENTATION_ID,
                "artifact_media_type": FRAME_BUNDLE_MEDIA_TYPE,
                "artifact_package_path": indexed_relative,
                "artifact_size_bytes": len(artifact),
                "artifact_sha256": _sha256(artifact),
                "catalog_version": raw_row["catalog_version"],
                "plan_ids": list(raw_row["plan_ids"]),
                "provenance": {
                    "schema_version": INDEXED_PROVENANCE_SCHEMA_VERSION,
                    "source_representation_id": RAW_REPRESENTATION_ID,
                    "source_artifact_size_bytes": raw_row[
                        "artifact_size_bytes"
                    ],
                    "source_artifact_sha256": raw_row["artifact_sha256"],
                    "selection_policy": policy_document,
                    "selection_policy_sha256": _sha256(
                        _canonical(policy_document)
                    ),
                    "embedded_manifest_sha256": embedded_sha,
                    "source_duration_seconds": duration,
                    "snapshot_semantics": (
                        "deterministic-source-decoded-temporal-projection"
                    ),
                },
            })
        rows.sort(key=lambda row: (row["object_id"], row["representation_id"]))
        catalog_version = str(raw_report["catalog_version"])
        agent_manifest, object_catalog = _data_agent_documents(
            rows, catalog_version
        )
        source_binding = {
            "package_id": source_summary["package_id"],
            "catalog_version": source_summary["catalog_version"],
            "manifest_sha256": _sha256(
                (source_root / PACKAGE_MANIFEST_NAME).read_bytes()
            ),
            "checksums_sha256": _sha256(
                (source_root / CHECKSUMS_NAME).read_bytes()
            ),
        }
        report = {
            "schema_version": N3_INDEXED_DATA_PLANE_SCHEMA_VERSION,
            "status": N3_INDEXED_DATA_PLANE_STATUS,
            "package_id": package_id,
            "catalog_version": catalog_version,
            "object_count": len(raw_rows),
            "artifact_count": len(rows),
            "artifact_bytes": sum(row["artifact_size_bytes"] for row in rows),
            "objects": rows,
            "source_raw_package_binding": source_binding,
            "selection_policy": policy_document,
            "portable_data_agent_contract_complete": True,
            "deployment_binding_required": True,
            "runtime_execution_verified": False,
            "workflow_submitted": False,
            "llm_called": False,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }
        documents = {
            DATA_AGENT_MANIFEST_PATH: _pretty(agent_manifest),
            OBJECT_CATALOG_PATH: _pretty(object_catalog),
            PACKAGE_MANIFEST_NAME: _pretty(report),
        }
        for relative, payload in documents.items():
            path = stage / Path(*PurePosixPath(relative).parts)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)
        (stage / CHECKSUMS_NAME).write_bytes(_checksums(stage))
        verify_n3_indexed_data_plane_package(stage)
        os.replace(stage, target)
    finally:
        shutil.rmtree(staging_parent, ignore_errors=True)
    result = verify_n3_indexed_data_plane_package(target)
    result["output_dir"] = str(target)
    return result


def verify_n3_indexed_data_plane_package(
    package_dir: str | Path,
    *,
    limits: FrameBundleLimits = DEFAULT_FRAME_BUNDLE_LIMITS,
) -> dict[str, Any]:
    """Verify raw bytes, temporal bundles, and Data Agent resolution."""

    root = Path(package_dir).resolve()
    _require(root.is_dir(), "N3 indexed data-plane package is missing")
    report = _read_json(root / PACKAGE_MANIFEST_NAME, "N3 indexed package")
    _require(set(report) == _REPORT_KEYS, "N3 indexed report fields changed")
    _require(
        report.get("schema_version") == N3_INDEXED_DATA_PLANE_SCHEMA_VERSION
        and report.get("status") == N3_INDEXED_DATA_PLANE_STATUS,
        "N3 indexed report schema or status changed",
    )
    _identifier(report.get("package_id"), "package_id")
    catalog_version = _identifier(
        report.get("catalog_version"), "catalog_version"
    )
    policy = N3TemporalSelectionPolicy(
        frame_count=report["selection_policy"].get("frame_count"),
        jpeg_max_dimension=report["selection_policy"].get(
            "jpeg_max_dimension"
        ),
        temporal_start_fraction=report["selection_policy"].get(
            "temporal_window_fraction", [None, None]
        )[0],
        temporal_end_fraction=report["selection_policy"].get(
            "temporal_window_fraction", [None, None]
        )[1],
        sampling_method=report["selection_policy"].get(
            "sampling_method", UNIFORM_MIDPOINT_SAMPLING_METHOD
        ),
        selection_provenance=report["selection_policy"].get(
            "temporal_index_selection"
        ),
    )
    _require(
        report["selection_policy"] == policy.to_dict(),
        "N3 indexed selection policy changed",
    )
    source_binding = report.get("source_raw_package_binding")
    _require(
        isinstance(source_binding, Mapping)
        and set(source_binding) == _SOURCE_BINDING_KEYS,
        "N3 source-package binding fields changed",
    )
    _identifier(source_binding.get("package_id"), "source package_id")
    _require(
        source_binding.get("catalog_version") == catalog_version,
        "N3 source-package catalog binding changed",
    )
    _digest(source_binding.get("manifest_sha256"), "source manifest digest")
    _digest(
        source_binding.get("checksums_sha256"),
        "source checksums digest",
    )
    for name, expected in (
        ("portable_data_agent_contract_complete", True),
        ("deployment_binding_required", True),
        ("runtime_execution_verified", False),
        ("workflow_submitted", False),
        ("llm_called", False),
        ("credentials_recorded", False),
        ("eligible_for_scientific_claims", False),
    ):
        _require(report.get(name) is expected, f"{name} changed")
    rows = report.get("objects")
    _require(isinstance(rows, list) and bool(rows), "N3 indexed rows are empty")
    _require(
        rows == sorted(
            rows, key=lambda row: (row["object_id"], row["representation_id"])
        ),
        "N3 indexed rows are not sorted",
    )
    by_key: dict[tuple[str, str], Mapping[str, Any]] = {}
    expected_files = {
        PACKAGE_MANIFEST_NAME,
        DATA_AGENT_MANIFEST_PATH,
        OBJECT_CATALOG_PATH,
    }
    total_bytes = 0
    for index, row in enumerate(rows):
        _require(isinstance(row, Mapping), f"objects[{index}] is invalid")
        _require(set(row) == _ROW_KEYS, f"objects[{index}] fields changed")
        object_id = _identifier(row.get("object_id"), "object_id")
        representation = row.get("representation_id")
        _require(
            representation in {
                RAW_REPRESENTATION_ID,
                INDEXED_REPRESENTATION_ID,
            },
            "N3 indexed package has an unsupported representation",
        )
        key = (object_id, str(representation))
        _require(key not in by_key, "N3 indexed package repeats an artifact")
        by_key[key] = row
        _require(
            row.get("catalog_version") == catalog_version,
            "N3 indexed row catalog changed",
        )
        plans = row.get("plan_ids")
        _require(
            isinstance(plans, list)
            and bool(plans)
            and plans == sorted(set(plans))
            and all(isinstance(value, str) for value in plans),
            "N3 indexed row plan IDs are invalid",
        )
        relative = str(row.get("artifact_package_path"))
        _require(
            not PurePosixPath(relative).is_absolute()
            and ".." not in PurePosixPath(relative).parts,
            "N3 indexed artifact path is unsafe",
        )
        expected_files.add(relative)
        path = root / Path(*PurePosixPath(relative).parts)
        digest, size = _hash_file(path)
        _require(
            digest == _digest(row.get("artifact_sha256"), "artifact digest")
            and size == row.get("artifact_size_bytes")
            and type(size) is int
            and size > 0,
            "N3 indexed artifact identity changed",
        )
        total_bytes += size
        if representation == RAW_REPRESENTATION_ID:
            _require(
                row.get("artifact_media_type") == RAW_MEDIA_TYPE
                and relative == f"artifacts/{object_id}/raw_video.mp4",
                "N3 raw artifact binding changed",
            )
        else:
            _require(
                row.get("artifact_media_type") == FRAME_BUNDLE_MEDIA_TYPE
                and relative
                == f"artifacts/{object_id}/{INDEXED_REPRESENTATION_ID}.tar",
                "N3 indexed projection binding changed",
            )
            bundle = validate_frame_bundle_bytes(
                path.read_bytes(),
                expected_object_id=object_id,
                expected_sha256=digest,
                expected_size_bytes=size,
                artifact_media_type=FRAME_BUNDLE_MEDIA_TYPE,
                limits=limits,
            )
            provenance = row.get("provenance")
            _require(
                isinstance(provenance, Mapping)
                and provenance.get("schema_version")
                == INDEXED_PROVENANCE_SCHEMA_VERSION
                and provenance.get("selection_policy") == policy.to_dict()
                and provenance.get("selection_policy_sha256")
                == _sha256(_canonical(policy.to_dict()))
                and provenance.get("embedded_manifest_sha256")
                == bundle.manifest_sha256,
                "N3 indexed projection provenance changed",
            )
    object_ids = sorted({key[0] for key in by_key})
    _require(
        set(by_key)
        == {
            (object_id, representation)
            for object_id in object_ids
            for representation in (
                RAW_REPRESENTATION_ID,
                INDEXED_REPRESENTATION_ID,
            )
        },
        "N3 indexed package does not pair every raw object and projection",
    )
    for object_id in object_ids:
        raw = by_key[(object_id, RAW_REPRESENTATION_ID)]
        indexed = by_key[(object_id, INDEXED_REPRESENTATION_ID)]
        provenance = indexed["provenance"]
        _require(
            provenance.get("source_artifact_sha256")
            == raw.get("artifact_sha256")
            and provenance.get("source_artifact_size_bytes")
            == raw.get("artifact_size_bytes")
            and indexed.get("plan_ids") == raw.get("plan_ids")
            and indexed["artifact_size_bytes"] < raw["artifact_size_bytes"],
            "N3 temporal projection is not bound to a smaller source artifact",
        )
    _require(
        report.get("object_count") == len(object_ids)
        and report.get("artifact_count") == len(rows)
        and report.get("artifact_bytes") == total_bytes,
        "N3 indexed package counts changed",
    )
    actual_files = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file()
    }
    _require(
        actual_files == expected_files | {CHECKSUMS_NAME},
        "N3 indexed package file set changed",
    )
    _require(
        (root / CHECKSUMS_NAME).read_bytes() == _checksums(root),
        "N3 indexed package checksums failed",
    )
    expected_manifest, expected_catalog = _data_agent_documents(
        rows, catalog_version
    )
    _require(
        _read_json(root / DATA_AGENT_MANIFEST_PATH, "Data Agent manifest")
        == expected_manifest
        and _read_json(root / OBJECT_CATALOG_PATH, "object catalog")
        == expected_catalog,
        "N3 indexed Data Agent documents changed",
    )
    agent = load_data_agent_manifest(root / DATA_AGENT_MANIFEST_PATH)
    for row in rows:
        for plan_id in row["plan_ids"]:
            resolved = agent.resolve(
                plan_id=plan_id,
                object_id=row["object_id"],
                representation_id=row["representation_id"],
                requested_location=SOURCE_LOCATION,
            )
            _require(
                resolved.path
                == root / Path(*PurePosixPath(row["artifact_package_path"]).parts),
                "N3 indexed Data Agent resolution changed",
            )
    return {
        "schema_version": N3_INDEXED_DATA_PLANE_SCHEMA_VERSION,
        "status": N3_INDEXED_DATA_PLANE_VERIFIED_STATUS,
        "package_id": report["package_id"],
        "catalog_version": catalog_version,
        "source_node_id": SOURCE_NODE_ID,
        "object_count": len(object_ids),
        "artifact_count": len(rows),
        "artifact_bytes": total_bytes,
        "indexed_projection_count": len(object_ids),
        "source_side_projection_verified": True,
        "byte_reduction_verified": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def verify_n3_semantic_data_plane_package(
    package_dir: str | Path,
) -> dict[str, Any]:
    """Accept either the legacy raw package or its indexed upgrade."""

    root = Path(package_dir).resolve()
    document = _read_json(root / PACKAGE_MANIFEST_NAME, "N3 package manifest")
    if document.get("schema_version") == N3_INDEXED_DATA_PLANE_SCHEMA_VERSION:
        return verify_n3_indexed_data_plane_package(root)
    return verify_raw_cold_data_plane_package(root)


__all__ = [
    "DEFAULT_FRAME_COUNT",
    "DEFAULT_JPEG_MAX_DIMENSION",
    "DEFAULT_TEMPORAL_END_FRACTION",
    "DEFAULT_TEMPORAL_START_FRACTION",
    "TEMPORAL_INDEX_SELECTED_SAMPLING_METHOD",
    "UNIFORM_MIDPOINT_SAMPLING_METHOD",
    "INDEXED_PROVENANCE_SCHEMA_VERSION",
    "INDEXED_REPRESENTATION_ID",
    "N3_INDEXED_DATA_PLANE_SCHEMA_VERSION",
    "N3IndexedDataPlaneError",
    "N3TemporalSelectionPolicy",
    "build_n3_indexed_data_plane_package",
    "verify_n3_indexed_data_plane_package",
    "verify_n3_semantic_data_plane_package",
]
