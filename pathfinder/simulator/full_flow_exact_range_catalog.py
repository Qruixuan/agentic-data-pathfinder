"""Content-bound fallback ranges for the semantic indexed-raw route.

The infrastructure-only 4x8 scenario models an indexed read as an estimated
fraction of a raw video.  That estimate is useful for simulation, but it is
not a safe data-plane instruction: a semantic execution needs exact inclusive
byte offsets and a digest for the bytes returned by N3.

This module freezes the conservative executable fallback.  Each raw object is
bound to the exact full-object range ``0..size-1``.  N2 may therefore exercise
real selection and handoff without inventing a decodable partial MP4 range.
The catalog explicitly makes no selectivity or byte-saving claim.  A future
temporal/fragment index can replace an entry only by providing equally exact
content evidence.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any, Mapping

from .full_flow_semantic_route_runtime import ArtifactIdentity, ExactContentRange
from .raw_cold_data_plane import (
    CHECKSUMS_NAME as N3_CHECKSUMS_NAME,
    PACKAGE_MANIFEST_NAME as N3_MANIFEST_NAME,
    REPRESENTATION_ID,
    verify_raw_cold_data_plane_package,
)


EXACT_RANGE_CATALOG_SCHEMA_VERSION = (
    "pathfinder.full-flow-exact-range-catalog/v1alpha1"
)
EXACT_RANGE_ENTRY_SCHEMA_VERSION = (
    "pathfinder.full-flow-exact-range-entry/v1alpha1"
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
        raise FullFlowExactRangeCatalogError(
            "exact-range catalog is not canonical JSON"
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


def _identifier(value: Any, label: str) -> str:
    _require(
        isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
        f"{label} is invalid",
    )
    return str(value)


def _digest(value: Any, label: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{label} is not lowercase SHA-256",
    )
    return str(value)


def _integer(value: Any, label: str, *, minimum: int = 0) -> int:
    _require(
        type(value) is int and value >= minimum,
        f"{label} must be an integer >= {minimum}",
    )
    return int(value)


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
                FullFlowExactRangeCatalogError(
                    f"{label} contains invalid constant {token}"
                )
            ),
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowExactRangeCatalogError(f"cannot read {label}") from exc
    _require(isinstance(value, dict), f"{label} must be an object")
    return value


def _source_rows(n3_root: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    try:
        verify_raw_cold_data_plane_package(n3_root)
    except Exception as exc:
        raise FullFlowExactRangeCatalogError(
            "N3 raw package verification failed"
        ) from exc
    manifest = _strict_json(n3_root / N3_MANIFEST_NAME, "N3 manifest")
    rows = manifest.get("objects")
    _require(isinstance(rows, list) and bool(rows), "N3 manifest has no objects")
    normalized: list[dict[str, Any]] = []
    for raw in rows:
        _require(isinstance(raw, Mapping), "N3 object entry is invalid")
        object_id = _identifier(raw.get("object_id"), "N3 object_id")
        _require(
            raw.get("representation_id") == REPRESENTATION_ID,
            f"N3 object {object_id} is not raw_video",
        )
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
        "schema_version": EXACT_RANGE_CATALOG_SCHEMA_VERSION,
        "status": "FROZEN_EXACT_FULL_OBJECT_FALLBACK",
        "catalog_id": _identifier(catalog_id, "catalog_id"),
        "source_binding": source,
        "source_binding_sha256": _sha256(_canonical(source)),
        "entry_count": len(entries),
        "entries": entries,
        "executable_indexed_raw_handoff": True,
        "partial_mp4_ranges_supported": False,
        "index_selectivity_claimed": False,
        "byte_reduction_claimed": False,
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
    _require(
        report.get("schema_version") == EXACT_RANGE_CATALOG_SCHEMA_VERSION
        and report.get("status") == "FROZEN_EXACT_FULL_OBJECT_FALLBACK",
        "exact-range catalog status or schema changed",
    )
    supplied = _digest(report.pop("catalog_sha256", None), "catalog_sha256")
    _require(supplied == _sha256(_canonical(report)), "catalog digest failed")
    report["catalog_sha256"] = supplied
    rows = report.get("entries")
    _require(isinstance(rows, list) and bool(rows), "catalog has no entries")
    for row in rows:
        _require(isinstance(row, Mapping), "range entry is invalid")
        size = _integer(row.get("full_artifact_size_bytes"), "full size", minimum=1)
        full_digest = _digest(row.get("full_artifact_sha256"), "full digest")
        _require(
            row.get("schema_version") == EXACT_RANGE_ENTRY_SCHEMA_VERSION
            and row.get("representation_id") == REPRESENTATION_ID
            and row.get("range_start") == 0
            and row.get("range_end") == size - 1
            and row.get("range_size_bytes") == size
            and row.get("range_sha256") == full_digest
            and row.get("selection_semantics") == "exact-full-object-fallback"
            and row.get("partial_range_selected") is False
            and row.get("index_selectivity_claimed") is False
            and row.get("byte_reduction_claimed") is False,
            "range entry is not the exact full-object fallback",
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
        and report.get("index_selectivity_claimed") is False
        and report.get("byte_reduction_claimed") is False
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
    """Freeze exact full-object fallback ranges from a verified N3 package."""

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
        "index_selectivity_claimed": False,
        "byte_reduction_claimed": False,
        "output_dir": str(target),
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def verify_full_flow_exact_range_catalog(
    catalog_dir: str | Path,
    n3_package_dir: str | Path,
) -> dict[str, Any]:
    """Re-derive every exact fallback range from its frozen N3 source."""

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
        "index_selectivity_claimed": False,
        "byte_reduction_claimed": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


class ExactFullObjectRangeCatalog:
    """Read-only resolver used after package verification at process start."""

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

    def resolve(self, identity: ArtifactIdentity) -> ExactContentRange:
        _require(
            identity.representation_id == REPRESENTATION_ID,
            "exact-range catalog accepts raw_video only",
        )
        row = self._entries.get(identity.object_id)
        _require(row is not None, "raw object is absent from exact-range catalog")
        result = ExactContentRange(
            object_id=str(row["object_id"]),
            representation_id=str(row["representation_id"]),
            object_catalog_version=str(row["object_catalog_version"]),
            full_artifact_size_bytes=int(row["full_artifact_size_bytes"]),
            full_artifact_sha256=str(row["full_artifact_sha256"]),
            range_start=int(row["range_start"]),
            range_end=int(row["range_end"]),
            range_sha256=str(row["range_sha256"]),
        )
        _require(result.matches(identity), "exact range differs from trial identity")
        return result


__all__ = [
    "CATALOG_NAME",
    "CHECKSUMS_NAME",
    "EXACT_RANGE_CATALOG_SCHEMA_VERSION",
    "EXACT_RANGE_ENTRY_SCHEMA_VERSION",
    "ExactFullObjectRangeCatalog",
    "FullFlowExactRangeCatalogError",
    "build_full_flow_exact_range_catalog",
    "verify_full_flow_exact_range_catalog",
]
