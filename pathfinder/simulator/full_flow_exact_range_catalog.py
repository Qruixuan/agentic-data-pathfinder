"""Content-bound source selections for the semantic indexed-raw route.

The infrastructure-only 4x8 scenario models an indexed read as an estimated
fraction of a raw video.  That estimate is useful for simulation, but it is
not a safe data-plane instruction: a semantic execution needs exact inclusive
byte offsets and a digest for the bytes returned by N3.

For a legacy N3 package, this module freezes the conservative executable
full-object fallback.  For an upgraded N3 package, it binds the authoritative
MP4 to a real, source-decoded temporal frame bundle with an exact digest.  The
second form reduces transferred bytes without inventing an undecodable MP4
byte range.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any, Mapping

from ._full_flow_primitives import (
    canonical_json_bytes,
    checked_identifier,
    checked_lower_sha256,
    pretty_json_bytes,
    sha256_hex,
    strict_json_loads,
)
from .full_flow_semantic_route_runtime import (
    ArtifactIdentity,
    ExactContentRange,
    ExactSourceSelection,
    ExactTemporalFrameSelection,
)
from .n3_indexed_data_plane import (
    INDEXED_PROVENANCE_SCHEMA_VERSION,
    INDEXED_REPRESENTATION_ID,
    N3_INDEXED_DATA_PLANE_SCHEMA_VERSION,
    verify_n3_semantic_data_plane_package,
)
from .raw_cold_data_plane import (
    CHECKSUMS_NAME as N3_CHECKSUMS_NAME,
    PACKAGE_MANIFEST_NAME as N3_MANIFEST_NAME,
    REPRESENTATION_ID,
)


EXACT_RANGE_CATALOG_SCHEMA_VERSION = (
    "pathfinder.full-flow-exact-range-catalog/v1alpha1"
)
EXACT_RANGE_ENTRY_SCHEMA_VERSION = (
    "pathfinder.full-flow-exact-range-entry/v1alpha1"
)
EXACT_SELECTION_CATALOG_SCHEMA_VERSION = (
    "pathfinder.full-flow-exact-selection-catalog/v1alpha2"
)
EXACT_TEMPORAL_SELECTION_ENTRY_SCHEMA_VERSION = (
    "pathfinder.full-flow-exact-temporal-selection-entry/v1alpha1"
)
CATALOG_NAME = "full-flow-exact-range-catalog.json"
CHECKSUMS_NAME = "SHA256SUMS"

_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_FILES = {CATALOG_NAME, CHECKSUMS_NAME}


class FullFlowExactRangeCatalogError(ValueError):
    """Raised when an exact-range commitment is absent or ambiguous."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowExactRangeCatalogError(message)


def _sha256(value: bytes) -> str:
    return sha256_hex(value)


def _canonical(value: Any) -> bytes:
    return canonical_json_bytes(
        value,
        error_type=FullFlowExactRangeCatalogError,
        error_message="exact-range catalog is not canonical JSON",
    )


def _json_bytes(value: Any) -> bytes:
    return pretty_json_bytes(value)


def _identifier(value: Any, label: str) -> str:
    return str(
        checked_identifier(
            value,
            label,
            error_type=FullFlowExactRangeCatalogError,
            pattern=_IDENTIFIER,
        )
    )


def _digest(value: Any, label: str) -> str:
    return str(
        checked_lower_sha256(
            value,
            label,
            error_type=FullFlowExactRangeCatalogError,
            pattern=_SHA256,
        )
    )


def _integer(value: Any, label: str, *, minimum: int = 0) -> int:
    _require(
        type(value) is int and value >= minimum,
        f"{label} must be an integer >= {minimum}",
    )
    return int(value)


def _strict_json(path: Path, label: str) -> dict[str, Any]:
    _require(path.is_file() and not path.is_symlink(), f"{label} is missing")

    try:
        value = strict_json_loads(
            path.read_text(encoding="utf-8"),
            error_type=FullFlowExactRangeCatalogError,
            duplicate_key_message=lambda key: f"{label} repeats key {key}",
            nonfinite_number_message=(
                lambda token: f"{label} contains invalid constant {token}"
            ),
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowExactRangeCatalogError(f"cannot read {label}") from exc
    _require(isinstance(value, dict), f"{label} must be an object")
    return value


def _source_rows(n3_root: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    try:
        verify_n3_semantic_data_plane_package(n3_root)
    except Exception as exc:
        raise FullFlowExactRangeCatalogError(
            "N3 semantic data-plane package verification failed"
        ) from exc
    manifest = _strict_json(n3_root / N3_MANIFEST_NAME, "N3 manifest")
    rows = manifest.get("objects")
    _require(isinstance(rows, list) and bool(rows), "N3 manifest has no objects")
    by_key: dict[tuple[str, str], Mapping[str, Any]] = {}
    for raw in rows:
        _require(isinstance(raw, Mapping), "N3 object entry is invalid")
        key = (
            _identifier(raw.get("object_id"), "N3 object_id"),
            _identifier(raw.get("representation_id"), "N3 representation_id"),
        )
        _require(key not in by_key, "N3 package repeats an artifact identity")
        by_key[key] = raw
    normalized: list[dict[str, Any]] = []
    raw_rows = [
        row
        for (object_id, representation), row in by_key.items()
        if representation == REPRESENTATION_ID
    ]
    _require(bool(raw_rows), "N3 manifest has no raw_video objects")
    indexed_package = (
        manifest.get("schema_version") == N3_INDEXED_DATA_PLANE_SCHEMA_VERSION
    )
    for raw in raw_rows:
        object_id = str(raw["object_id"])
        size = _integer(
            raw.get("artifact_size_bytes"),
            f"N3 object {object_id} size",
            minimum=1,
        )
        digest = _digest(
            raw.get("artifact_sha256"),
            f"N3 object {object_id} digest",
        )
        catalog_version = _identifier(
            raw.get("catalog_version"),
            f"N3 object {object_id} catalog version",
        )
        if indexed_package:
            selected = by_key.get((object_id, INDEXED_REPRESENTATION_ID))
            _require(
                selected is not None,
                f"N3 object {object_id} has no temporal projection",
            )
            provenance = selected.get("provenance")
            _require(
                isinstance(provenance, Mapping)
                and provenance.get("schema_version")
                == INDEXED_PROVENANCE_SCHEMA_VERSION,
                f"N3 object {object_id} projection provenance is invalid",
            )
            policy = provenance.get("selection_policy")
            _require(
                isinstance(policy, Mapping),
                f"N3 object {object_id} projection policy is invalid",
            )
            window = policy.get("temporal_window_fraction")
            _require(
                isinstance(window, list) and len(window) == 2,
                f"N3 object {object_id} projection window is invalid",
            )
            selected_size = _integer(
                selected.get("artifact_size_bytes"),
                f"N3 object {object_id} projection size",
                minimum=1,
            )
            _require(
                selected_size < size,
                f"N3 object {object_id} projection does not reduce bytes",
            )
            normalized.append({
                "schema_version": EXACT_TEMPORAL_SELECTION_ENTRY_SCHEMA_VERSION,
                "object_id": object_id,
                "representation_id": REPRESENTATION_ID,
                "object_catalog_version": catalog_version,
                "full_artifact_size_bytes": size,
                "full_artifact_sha256": digest,
                "selected_representation_id": INDEXED_REPRESENTATION_ID,
                "selected_artifact_size_bytes": selected_size,
                "selected_artifact_sha256": _digest(
                    selected.get("artifact_sha256"),
                    f"N3 object {object_id} projection digest",
                ),
                "frame_count": _integer(
                    policy.get("frame_count"),
                    f"N3 object {object_id} frame count",
                    minimum=1,
                ),
                "temporal_start_fraction": float(window[0]),
                "temporal_end_fraction": float(window[1]),
                "selection_policy_sha256": _digest(
                    provenance.get("selection_policy_sha256"),
                    f"N3 object {object_id} policy digest",
                ),
                "selection_semantics": (
                    "source-decoded-temporal-frame-bundle"
                ),
                "partial_mp4_ranges_supported": False,
                "index_selectivity_claimed": True,
                "byte_reduction_claimed": True,
            })
            continue
        normalized.append({
            "schema_version": EXACT_RANGE_ENTRY_SCHEMA_VERSION,
            "object_id": object_id,
            "representation_id": REPRESENTATION_ID,
            "object_catalog_version": catalog_version,
            "full_artifact_size_bytes": size,
            "full_artifact_sha256": digest,
            "range_start": 0,
            "range_end": size - 1,
            "range_size_bytes": size,
            "range_sha256": digest,
            "selection_semantics": "exact-full-object-fallback",
            "partial_range_selected": False,
            "index_selectivity_claimed": False,
            "byte_reduction_claimed": False,
        })
    normalized.sort(key=lambda row: row["object_id"])
    _require(
        len(normalized) == len({row["object_id"] for row in normalized}),
        "N3 package repeats an object identity",
    )
    return manifest, normalized


def _document(n3_root: Path, catalog_id: str) -> dict[str, Any]:
    manifest, entries = _source_rows(n3_root)
    source_side_projection = all(
        row.get("selection_semantics")
        == "source-decoded-temporal-frame-bundle"
        for row in entries
    )
    source = {
        "n3_package_id": _identifier(manifest.get("package_id"), "N3 package_id"),
        "n3_catalog_version": _identifier(
            manifest.get("catalog_version"), "N3 catalog_version"
        ),
        "n3_manifest_sha256": _sha256((n3_root / N3_MANIFEST_NAME).read_bytes()),
        "n3_checksums_sha256": _sha256(
            (n3_root / N3_CHECKSUMS_NAME).read_bytes()
        ),
    }
    report: dict[str, Any] = {
        "schema_version": (
            EXACT_SELECTION_CATALOG_SCHEMA_VERSION
            if source_side_projection
            else EXACT_RANGE_CATALOG_SCHEMA_VERSION
        ),
        "status": (
            "FROZEN_EXACT_TEMPORAL_SELECTIONS"
            if source_side_projection
            else "FROZEN_EXACT_FULL_OBJECT_FALLBACK"
        ),
        "catalog_id": _identifier(catalog_id, "catalog_id"),
        "source_binding": source,
        "source_binding_sha256": _sha256(_canonical(source)),
        "entry_count": len(entries),
        "entries": entries,
        "executable_indexed_raw_handoff": True,
        "partial_mp4_ranges_supported": False,
        "index_selectivity_claimed": source_side_projection,
        "byte_reduction_claimed": source_side_projection,
        "source_side_projection_executed": source_side_projection,
        "infrastructure_fraction_estimates_consumed": False,
        "artifact_bytes_copied": False,
        "services_started": False,
        "workflow_submitted": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    report["catalog_sha256"] = _sha256(_canonical(report))
    return report


def _verify_files(root: Path) -> dict[str, Any]:
    _require(root.is_dir(), "exact-range catalog directory is missing")
    entries = list(root.iterdir())
    _require(
        all(path.is_file() and not path.is_symlink() for path in entries),
        "exact-range catalog contains a non-regular file",
    )
    _require({path.name for path in entries} == _FILES, "catalog file set changed")
    expected = f"{_sha256((root / CATALOG_NAME).read_bytes())}  {CATALOG_NAME}\n"
    _require(
        (root / CHECKSUMS_NAME).read_text(encoding="utf-8") == expected,
        "exact-range catalog checksums failed",
    )
    report = _strict_json(root / CATALOG_NAME, "exact-range catalog")
    legacy = (
        report.get("schema_version") == EXACT_RANGE_CATALOG_SCHEMA_VERSION
        and report.get("status") == "FROZEN_EXACT_FULL_OBJECT_FALLBACK"
    )
    projected = (
        report.get("schema_version") == EXACT_SELECTION_CATALOG_SCHEMA_VERSION
        and report.get("status") == "FROZEN_EXACT_TEMPORAL_SELECTIONS"
    )
    _require(legacy or projected, "exact-selection catalog status changed")
    supplied = _digest(report.pop("catalog_sha256", None), "catalog_sha256")
    _require(supplied == _sha256(_canonical(report)), "catalog digest failed")
    report["catalog_sha256"] = supplied
    rows = report.get("entries")
    _require(isinstance(rows, list) and bool(rows), "catalog has no entries")
    for row in rows:
        _require(isinstance(row, Mapping), "range entry is invalid")
        size = _integer(
            row.get("full_artifact_size_bytes"),
            "full size",
            minimum=1,
        )
        full_digest = _digest(row.get("full_artifact_sha256"), "full digest")
        if legacy:
            _require(
                row.get("schema_version") == EXACT_RANGE_ENTRY_SCHEMA_VERSION
                and row.get("representation_id") == REPRESENTATION_ID
                and row.get("range_start") == 0
                and row.get("range_end") == size - 1
                and row.get("range_size_bytes") == size
                and row.get("range_sha256") == full_digest
                and row.get("selection_semantics")
                == "exact-full-object-fallback"
                and row.get("partial_range_selected") is False
                and row.get("index_selectivity_claimed") is False
                and row.get("byte_reduction_claimed") is False,
                "range entry is not the exact full-object fallback",
            )
        else:
            selected_size = _integer(
                row.get("selected_artifact_size_bytes"),
                "selected size",
                minimum=1,
            )
            _require(
                row.get("schema_version")
                == EXACT_TEMPORAL_SELECTION_ENTRY_SCHEMA_VERSION
                and row.get("representation_id") == REPRESENTATION_ID
                and row.get("selected_representation_id")
                == INDEXED_REPRESENTATION_ID
                and selected_size < size
                and _digest(
                    row.get("selected_artifact_sha256"),
                    "selected digest",
                )
                and row.get("selection_semantics")
                == "source-decoded-temporal-frame-bundle"
                and row.get("partial_mp4_ranges_supported") is False
                and row.get("index_selectivity_claimed") is True
                and row.get("byte_reduction_claimed") is True,
                "temporal selection entry is invalid",
            )
    _require(
        report.get("entry_count") == len(rows)
        and [row.get("object_id") for row in rows]
        == sorted(row.get("object_id") for row in rows)
        and len(rows) == len({row.get("object_id") for row in rows}),
        "range entry count, order, or identity changed",
    )
    _require(
        report.get("executable_indexed_raw_handoff") is True
        and report.get("partial_mp4_ranges_supported") is False
        and report.get("index_selectivity_claimed") is projected
        and report.get("byte_reduction_claimed") is projected
        and report.get("source_side_projection_executed") is projected
        and report.get("infrastructure_fraction_estimates_consumed") is False
        and report.get("artifact_bytes_copied") is False
        and report.get("credentials_recorded") is False
        and report.get("eligible_for_scientific_claims") is False,
        "exact-range claim boundary changed",
    )
    return report


def _publish(target: Path, report: Mapping[str, Any]) -> None:
    _require(not target.exists(), f"output directory already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    parent = Path(tempfile.mkdtemp(prefix=".exact-ranges-", dir=target.parent))
    stage = parent / "output"
    try:
        stage.mkdir()
        content = _json_bytes(report)
        (stage / CATALOG_NAME).write_bytes(content)
        (stage / CHECKSUMS_NAME).write_text(
            f"{_sha256(content)}  {CATALOG_NAME}\n",
            encoding="utf-8",
        )
        _verify_files(stage)
        os.replace(stage, target)
    finally:
        shutil.rmtree(parent, ignore_errors=True)


def build_full_flow_exact_range_catalog(
    n3_package_dir: str | Path,
    *,
    catalog_id: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze exact source selections from a verified N3 package."""

    n3_root = Path(n3_package_dir).resolve()
    report = _document(n3_root, catalog_id)
    target = Path(output_dir).resolve()
    _publish(target, report)
    verified = _verify_files(target)
    return {
        "status": verified["status"],
        "catalog_id": verified["catalog_id"],
        "catalog_sha256": verified["catalog_sha256"],
        "entry_count": verified["entry_count"],
        "partial_mp4_ranges_supported": False,
        "index_selectivity_claimed": verified["index_selectivity_claimed"],
        "byte_reduction_claimed": verified["byte_reduction_claimed"],
        "source_side_projection_executed": verified[
            "source_side_projection_executed"
        ],
        "output_dir": str(target),
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def verify_full_flow_exact_range_catalog(
    catalog_dir: str | Path,
    n3_package_dir: str | Path,
) -> dict[str, Any]:
    """Re-derive every exact source selection from its frozen N3 source."""

    root = Path(catalog_dir).resolve()
    supplied = _verify_files(root)
    expected = _document(
        Path(n3_package_dir).resolve(),
        supplied["catalog_id"],
    )
    _require(
        (root / CATALOG_NAME).read_bytes() == _json_bytes(expected),
        "exact-range catalog does not match the N3 package",
    )
    return {
        "status": "VERIFIED",
        "catalog_id": supplied["catalog_id"],
        "catalog_sha256": supplied["catalog_sha256"],
        "entry_count": supplied["entry_count"],
        "source_binding_checked": True,
        "partial_mp4_ranges_supported": False,
        "index_selectivity_claimed": supplied["index_selectivity_claimed"],
        "byte_reduction_claimed": supplied["byte_reduction_claimed"],
        "source_side_projection_executed": supplied[
            "source_side_projection_executed"
        ],
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


class ExactFullObjectRangeCatalog:
    """Backward-compatible exact source-selection resolver."""

    def __init__(
        self,
        catalog_dir: str | Path,
        n3_package_dir: str | Path,
    ) -> None:
        verify_full_flow_exact_range_catalog(catalog_dir, n3_package_dir)
        report = _verify_files(Path(catalog_dir).resolve())
        self.catalog_id = str(report["catalog_id"])
        self.catalog_sha256 = str(report["catalog_sha256"])
        self._entries = {
            str(row["object_id"]): dict(row) for row in report["entries"]
        }

    def resolve(self, identity: ArtifactIdentity) -> ExactSourceSelection:
        _require(
            identity.representation_id == REPRESENTATION_ID,
            "exact-range catalog accepts raw_video only",
        )
        row = self._entries.get(identity.object_id)
        _require(row is not None, "raw object is absent from exact-range catalog")
        if row.get("selection_semantics") == "exact-full-object-fallback":
            result: ExactSourceSelection = ExactContentRange(
                object_id=str(row["object_id"]),
                representation_id=str(row["representation_id"]),
                object_catalog_version=str(row["object_catalog_version"]),
                full_artifact_size_bytes=int(row["full_artifact_size_bytes"]),
                full_artifact_sha256=str(row["full_artifact_sha256"]),
                range_start=int(row["range_start"]),
                range_end=int(row["range_end"]),
                range_sha256=str(row["range_sha256"]),
            )
        else:
            result = ExactTemporalFrameSelection(
                object_id=str(row["object_id"]),
                representation_id=str(row["representation_id"]),
                object_catalog_version=str(row["object_catalog_version"]),
                full_artifact_size_bytes=int(row["full_artifact_size_bytes"]),
                full_artifact_sha256=str(row["full_artifact_sha256"]),
                selected_representation_id=str(
                    row["selected_representation_id"]
                ),
                selected_artifact_size_bytes=int(
                    row["selected_artifact_size_bytes"]
                ),
                selected_artifact_sha256=str(
                    row["selected_artifact_sha256"]
                ),
                frame_count=int(row["frame_count"]),
                temporal_start_fraction=float(
                    row["temporal_start_fraction"]
                ),
                temporal_end_fraction=float(row["temporal_end_fraction"]),
                selection_policy_sha256=str(
                    row["selection_policy_sha256"]
                ),
            )
        _require(result.matches(identity), "exact range differs from trial identity")
        return result


__all__ = [
    "CATALOG_NAME",
    "CHECKSUMS_NAME",
    "EXACT_RANGE_CATALOG_SCHEMA_VERSION",
    "EXACT_RANGE_ENTRY_SCHEMA_VERSION",
    "EXACT_SELECTION_CATALOG_SCHEMA_VERSION",
    "EXACT_TEMPORAL_SELECTION_ENTRY_SCHEMA_VERSION",
    "ExactFullObjectRangeCatalog",
    "FullFlowExactRangeCatalogError",
    "build_full_flow_exact_range_catalog",
    "verify_full_flow_exact_range_catalog",
]
