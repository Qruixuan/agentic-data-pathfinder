"""Freeze public per-unit accounting from a verified formal collection.

The formal runner deliberately preserves the canonical ten-path evidence
unchanged.  The offline replay builder consumes a smaller public accounting
schema, so this module performs that conversion without copying prompts,
answers, predictions, credentials, endpoints, or hidden-label material.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any, Mapping, Sequence

from .offline_replay import (
    ACCOUNTING_SCHEMA_VERSION,
    CHECKSUMS_NAME,
    _checksum_bytes,
    _json_bytes,
    _load_json_bytes,
    _load_jsonl,
    _require,
    _sha256,
    _verify_checksum_directory,
    _verify_no_leakage,
)


FORMAL_ACCOUNTING_SCHEMA_VERSION = (
    "pathfinder.rsi-exam-formal-accounting/v1alpha1"
)
MANIFEST_NAME = "formal-accounting-manifest.json"
SPLIT_MANIFEST_NAME = "split-manifest.json"
COLLECTION_RECEIPT_NAME = "formal-trace-collection-receipt.json"
COLLECTION_PROGRESS_NAME = "formal-trace-collection-progress.json"
SMOKE_RECEIPT_NAME = "local-semantic-smoke-receipt.json"
SMOKE_RESULTS_NAME = "local-semantic-smoke-results.jsonl"
ACCOUNTING_NAME = "accounting.json"
_SHA1 = re.compile(r"[0-9a-f]{40}\Z")


class FormalAccountingError(ValueError):
    """A formal collection cannot be converted to public accounting."""


def _strict_json(path: Path, label: str) -> dict[str, Any]:
    _require(path.is_file() and not path.is_symlink(), f"{label} is missing")
    value = _load_json_bytes(path.read_bytes(), label)
    _require(isinstance(value, dict), f"{label} must be an object")
    return value


def _stage_metrics(result: Mapping[str, Any]) -> dict[str, dict[str, float]]:
    measurements = result.get("measurements")
    _require(isinstance(measurements, list), "measurements are missing")
    stages: dict[str, dict[str, float]] = {}
    for measurement in measurements:
        _require(isinstance(measurement, dict), "measurement is not an object")
        component = measurement.get("component_id")
        metric = measurement.get("metric_id")
        value = measurement.get("value")
        _require(
            isinstance(component, str)
            and component
            and isinstance(metric, str)
            and metric
            and isinstance(value, (int, float))
            and not isinstance(value, bool)
            and value >= 0,
            "measurement is invalid",
        )
        stages.setdefault(component, {})[metric] = float(value)
    return stages


def _pick_stage(
    stages: Mapping[str, Mapping[str, float]],
    *needles: str,
) -> tuple[str, Mapping[str, float]]:
    for name in sorted(stages):
        if any(needle in name for needle in needles):
            return name, stages[name]
    return "", {}


def _nonnegative_int(value: Any, label: str) -> int:
    _require(
        isinstance(value, int) and not isinstance(value, bool) and value >= 0,
        f"{label} must be a non-negative integer",
    )
    return value


def _public_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    _require(len(rows) == 10, "formal unit must contain exactly ten rows")
    public: list[dict[str, Any]] = []
    for row in rows:
        result = row.get("result")
        _require(isinstance(result, dict), "result is missing")
        _require(result.get("status") == "COMPLETE", "route is not complete")
        evidence = result.get("semantic_route_evidence")
        _require(isinstance(evidence, dict), "semantic route evidence is missing")
        route = evidence.get("route")
        model_input = evidence.get("model_input")
        scoring = evidence.get("scoring")
        _require(
            isinstance(route, dict)
            and isinstance(model_input, dict)
            and isinstance(scoring, dict),
            "public route fields are missing",
        )
        stages = _stage_metrics(result)
        source_name, source = _pick_stage(
            stages, "access-raw-artifact", "access-derived"
        )
        lookup_name, lookup = _pick_stage(stages, "lookup")
        _, prepare = _pick_stage(stages, "prepare-model-input")
        infer_name, infer = _pick_stage(stages, "infer")
        _, index = _pick_stage(stages, "query-index")
        _, score = _pick_stage(stages, "score-hidden-answer")
        origin_bytes = int(source.get("bytes-read", 0))
        cache_bytes = int(lookup.get("bytes-read", 0))
        cache_branch = None
        if lookup_name:
            cache_branch = "hit" if cache_bytes > 0 else "miss"
        public.append({
            "case_id": str(row.get("case_id", "")),
            "design_id": str(evidence.get("design_id", "")),
            "trial_key": str(row.get("trial_key", "")),
            "route_family": str(route.get("route_family", "")),
            "executor_node_id": str(route.get("executor_node_id", "")),
            "status": "COMPLETE",
            "task_success": scoring.get("task_success"),
            "n1_exactly_once_authenticated": evidence.get(
                "n1_exactly_once_authenticated_score_verified"
            ),
            "semantic_input_profile_id": str(
                model_input.get("semantic_input_profile_id", "")
            ),
            "cache_branch": cache_branch,
            "origin_bytes_read": origin_bytes,
            "cache_bytes_read": cache_bytes,
            "model_input_bytes_sent_to_n6": _nonnegative_int(
                model_input.get("payload_size_bytes"),
                "model input payload size",
            ),
            "index_query_bytes_read": int(index.get("bytes-read", 0)),
            "index_query_bytes_sent": int(index.get("bytes-sent", 0)),
            "index_query_ms": round(float(index.get("service-time", 0.0)), 3),
            "source_read_ms": round(float(source.get("service-time", 0.0)), 3),
            "prepare_model_input_ms": round(
                float(prepare.get("service-time", 0.0)), 3
            ),
            "n6_infer_ms": round(float(infer.get("service-time", 0.0)), 3),
            "n1_score_ms": round(float(score.get("service-time", 0.0)), 3),
            "route_wall_ms_excluding_inference": round(
                sum(
                    float(metrics.get("service-time", 0.0))
                    for name, metrics in stages.items()
                    if name != infer_name
                ),
                3,
            ),
        })
    _verify_no_leakage(public)
    return public


def _one_positive(
    rows: Sequence[Mapping[str, Any]],
    *,
    family: str,
    field: str,
) -> int:
    values = {
        int(row[field])
        for row in rows
        if row.get("route_family") == family and int(row[field]) > 0
    }
    _require(len(values) == 1, f"{family} {field} is not unique and positive")
    return next(iter(values))


def _accounting_document(
    unit: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    runtime_manifest: Mapping[str, Any],
    *,
    collection_id: str,
) -> dict[str, Any]:
    object_id = str(unit.get("object_id", ""))
    _require(runtime_manifest.get("object_id") == object_id, "runtime object differs")
    _require(
        runtime_manifest.get("credentials_recorded") is False,
        "runtime manifest records credentials",
    )
    source_bytes = _nonnegative_int(
        runtime_manifest.get("original_object_bytes_read"),
        "original_object_bytes_read",
    )
    projection_bytes = _nonnegative_int(
        runtime_manifest.get("frame_bundle_size_bytes"),
        "frame_bundle_size_bytes",
    )
    _require(
        _one_positive(rows, family="raw", field="origin_bytes_read")
        == source_bytes,
        "raw route bytes differ from the frozen source object",
    )
    _require(
        _one_positive(rows, family="indexed-raw", field="origin_bytes_read")
        == projection_bytes,
        "indexed route bytes differ from the frozen projection",
    )
    document = {
        "schema_version": ACCOUNTING_SCHEMA_VERSION,
        "collection_id": collection_id,
        "run_id": str(unit.get("run_id", "")),
        "object_id": object_id,
        "public_case_id": str(unit.get("case_id", "")),
        "rows": list(rows),
        "one_time_build": {
            "this_object_source_bytes_read": source_bytes,
            "this_object_projection_bytes": projection_bytes,
            "read_mode": str(runtime_manifest.get("original_object_read_mode", "")),
            "partial_mp4_byte_range_claimed": runtime_manifest.get(
                "partial_mp4_byte_range_claimed"
            ),
            "reduced_source_storage_io_claimed": runtime_manifest.get(
                "reduced_source_storage_io_claimed"
            ),
        },
        "source_evidence_receipt_sha256": str(
            unit.get("evidence_receipt_sha256", "")
        ),
        "source_runtime_frame_manifest_sha256": str(
            runtime_manifest.get("manifest_sha256", "")
        ),
        "credentials_recorded": False,
        "hidden_label_values_included": False,
    }
    _verify_no_leakage([document])
    return document


def _expected_package(
    collection_dir: Path,
    runtime_frame_manifest_root: Path,
    *,
    source_commit: str,
) -> tuple[dict[str, bytes], dict[str, dict[str, bytes]]]:
    _require(_SHA1.fullmatch(source_commit) is not None, "source_commit is invalid")
    root_entries = _verify_checksum_directory(collection_dir)
    _require(
        set(root_entries) == {COLLECTION_RECEIPT_NAME, COLLECTION_PROGRESS_NAME},
        "formal collection root checksum file set differs",
    )
    receipt = _strict_json(
        collection_dir / COLLECTION_RECEIPT_NAME, "formal collection receipt"
    )
    _require(
        receipt.get("status") == "VERIFIED"
        and receipt.get("all_units_verified") is True
        and receipt.get("credentials_recorded") is False
        and receipt.get("hidden_label_values_included") is False,
        "formal collection is not eligible for public accounting",
    )
    units = receipt.get("units")
    _require(isinstance(units, list) and bool(units), "formal units are missing")
    collection_id = str(receipt.get("collection_id", ""))
    per_unit: dict[str, dict[str, bytes]] = {}
    splits: dict[str, str] = {}
    records: list[dict[str, Any]] = []
    for raw_unit in units:
        _require(isinstance(raw_unit, dict), "formal unit is not an object")
        unit = raw_unit
        source_name = str(unit.get("evidence_directory", ""))
        source = collection_dir / source_name
        entries = _verify_checksum_directory(source)
        _require(
            set(entries) == {SMOKE_RECEIPT_NAME, SMOKE_RESULTS_NAME},
            "formal unit checksum file set differs",
        )
        smoke_receipt = _strict_json(source / SMOKE_RECEIPT_NAME, "smoke receipt")
        _require(
            smoke_receipt.get("status") == "COMPLETE"
            and unit.get("status") == "VERIFIED"
            and smoke_receipt.get("run_id") == unit.get("run_id")
            and smoke_receipt.get("receipt_sha256")
            == unit.get("evidence_receipt_sha256")
            and smoke_receipt.get("credentials_recorded") is False,
            "formal unit receipt differs",
        )
        raw_rows = _load_jsonl(
            (source / SMOKE_RESULTS_NAME).read_bytes(), SMOKE_RESULTS_NAME
        )
        rows = _public_rows(raw_rows)
        object_id = str(unit.get("object_id", ""))
        runtime_dir = runtime_frame_manifest_root / object_id
        runtime_entries = _verify_checksum_directory(runtime_dir)
        _require(
            "runtime-frame-manifest.json" in runtime_entries,
            "runtime frame manifest is not checksum bound",
        )
        runtime_manifest = _strict_json(
            runtime_dir / "runtime-frame-manifest.json", "runtime frame manifest"
        )
        accounting = _accounting_document(
            unit, rows, runtime_manifest, collection_id=collection_id
        )
        accounting_bytes = _json_bytes(accounting)
        documents = {ACCOUNTING_NAME: accounting_bytes}
        documents[CHECKSUMS_NAME] = _checksum_bytes(documents)
        output_name = f"accounting-{int(unit['ordinal']):04d}-{object_id}-r{int(unit['repetition']):04d}"
        per_unit[output_name] = documents
        split = str(unit.get("split", ""))
        _require(split in {"train", "development", "test"}, "unit split is invalid")
        existing_split = splits.setdefault(object_id, split)
        _require(existing_split == split, "object appears in multiple splits")
        records.append({
            "ordinal": int(unit["ordinal"]),
            "case_id": str(unit["case_id"]),
            "object_id": object_id,
            "repetition": int(unit["repetition"]),
            "accounting_directory": output_name,
            "accounting_sha256": _sha256(accounting_bytes),
            "accounting_sha256s_sha256": _sha256(documents[CHECKSUMS_NAME]),
            "source_evidence_receipt_sha256": str(
                unit["evidence_receipt_sha256"]
            ),
            "runtime_frame_manifest_sha256": str(
                runtime_manifest["manifest_sha256"]
            ),
        })
    manifest = {
        "schema_version": FORMAL_ACCOUNTING_SCHEMA_VERSION,
        "status": "FROZEN_FORMAL_ACCOUNTING",
        "collection_id": collection_id,
        "collection_receipt_sha256": str(receipt.get("receipt_sha256", "")),
        "experiment_source_commit": source_commit,
        "case_count": len(splits),
        "accounting_directory_count": len(records),
        "route_row_count": sum(
            len(_load_json_bytes(documents[ACCOUNTING_NAME], ACCOUNTING_NAME)["rows"])
            for documents in per_unit.values()
        ),
        "records": sorted(records, key=lambda row: row["ordinal"]),
        "credentials_recorded": False,
        "hidden_label_values_included": False,
        "eligible_for_scientific_claims": False,
    }
    manifest["manifest_sha256"] = _sha256(_json_bytes(manifest))
    root_documents = {
        MANIFEST_NAME: _json_bytes(manifest),
        SPLIT_MANIFEST_NAME: _json_bytes(dict(sorted(splits.items()))),
    }
    root_documents[CHECKSUMS_NAME] = _checksum_bytes(root_documents)
    return root_documents, per_unit


def _write_package(
    target: Path,
    root_documents: Mapping[str, bytes],
    per_unit: Mapping[str, Mapping[str, bytes]],
) -> None:
    _require(not target.exists(), "formal accounting output already exists")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    try:
        for name, payload in root_documents.items():
            (staging / name).write_bytes(payload)
        for directory, documents in per_unit.items():
            child = staging / directory
            child.mkdir()
            for name, payload in documents.items():
                (child / name).write_bytes(payload)
        os.replace(staging, target)
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def freeze_formal_accounting(
    collection_dir: str | Path,
    runtime_frame_manifest_root: str | Path,
    *,
    source_commit: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze all public accounting rows from one verified collection."""

    root_documents, per_unit = _expected_package(
        Path(collection_dir).resolve(),
        Path(runtime_frame_manifest_root).resolve(),
        source_commit=source_commit,
    )
    target = Path(output_dir).resolve()
    _write_package(target, root_documents, per_unit)
    return verify_formal_accounting(
        target,
        collection_dir=collection_dir,
        runtime_frame_manifest_root=runtime_frame_manifest_root,
        source_commit=source_commit,
    )


def verify_formal_accounting(
    accounting_root: str | Path,
    *,
    collection_dir: str | Path,
    runtime_frame_manifest_root: str | Path,
    source_commit: str,
) -> dict[str, Any]:
    """Reproduce and verify a frozen formal accounting package."""

    root = Path(accounting_root).resolve()
    _require(root.is_dir(), "formal accounting directory is missing")
    expected_root, expected_units = _expected_package(
        Path(collection_dir).resolve(),
        Path(runtime_frame_manifest_root).resolve(),
        source_commit=source_commit,
    )
    actual_files = {path.name for path in root.iterdir() if path.is_file()}
    _require(actual_files == set(expected_root), "formal accounting file set changed")
    for name, payload in expected_root.items():
        _require((root / name).read_bytes() == payload, f"{name} differs")
    actual_dirs = {path.name for path in root.iterdir() if path.is_dir()}
    _require(actual_dirs == set(expected_units), "accounting directory set changed")
    for directory, documents in expected_units.items():
        child = root / directory
        _require(
            {path.name for path in child.iterdir() if path.is_file()}
            == set(documents),
            f"{directory} file set changed",
        )
        for name, payload in documents.items():
            _require(
                (child / name).read_bytes() == payload,
                f"{directory}/{name} differs",
            )
    manifest = _load_json_bytes(expected_root[MANIFEST_NAME], MANIFEST_NAME)
    return {
        "status": "VERIFIED_FORMAL_ACCOUNTING",
        "collection_id": manifest["collection_id"],
        "case_count": manifest["case_count"],
        "accounting_directory_count": manifest["accounting_directory_count"],
        "route_row_count": manifest["route_row_count"],
        "manifest_sha256": manifest["manifest_sha256"],
        "credentials_recorded": False,
        "hidden_label_values_included": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "freeze_formal_accounting",
    "verify_formal_accounting",
]
