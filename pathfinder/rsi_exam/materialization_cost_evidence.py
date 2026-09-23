"""Freeze measured provider usage without changing replay outcomes.

The receipt distinguishes measured provider units from missing CPU, storage,
wall-time, and invoice data. Raw model responses are read only to bind usage
to the frozen captions; their content is never copied into the output.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any, Mapping

from ..simulator.n4_derived_data_plane import verify_n4_derived_data_package
from .offline_replay_v2 import load_offline_replay_v2
from .temporal_index_collection import (
    verify_formal_temporal_caption_package,
)


SCHEMA = "pathfinder.rsi-exam-materialization-cost-evidence/v1alpha1"
FILES = ("cost-evidence.json", "caption-requests.jsonl",
         "embedding-requests.jsonl", "objects.jsonl")
MANIFEST_FIELDS = frozenset({
    "schema_version", "package_id", "builder_commit",
    "source_replay_sha256", "source_preparation_sha256",
    "source_caption_package_sha256", "source_caption_cost_sha256",
    "source_index_package_sha256", "source_index_checksums_sha256",
    "source_n4_package_sha256",
    "object_count", "caption_window_count", "caption_request_count",
    "caption_input_units", "caption_output_units", "caption_total_units",
    "caption_service_time_covered_windows", "embedding_request_count",
    "embedding_input_count", "embedding_input_units",
    "embedding_provider_service_seconds", "measurement_boundaries",
    "credentials_recorded", "hidden_label_values_included",
})
CAPTION_FIELDS = frozenset({
    "object_id", "window_id", "attempt", "request_sha256",
    "response_sha256", "raw_record_sha256", "input_units",
    "output_units", "total_units", "selected_caption_response",
})
EMBEDDING_FIELDS = frozenset({
    "ordinal", "input_count", "request_sha256", "response_sha256",
    "input_units", "service_time_seconds",
})
OBJECT_FIELDS = frozenset({
    "object_id", "source_video_sha256", "caption_window_count",
    "caption_request_count", "caption_input_units", "caption_output_units",
    "caption_total_units", "caption_provider_service_seconds",
    "embedding_units_allocated_to_object", "frame_decode_cpu_seconds",
    "n4_publication_seconds", "actual_billed_cost",
})


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      allow_nan=False, indent=2).encode("utf-8") + b"\n"


def _jsonl_bytes(rows: list[dict[str, Any]]) -> bytes:
    return b"".join(_canonical(row) + b"\n" for row in rows)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_bytes())
    _require(isinstance(value, dict), f"invalid JSON object: {path.name}")
    return value


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = [json.loads(line) for line in path.read_bytes().splitlines()]
    _require(all(isinstance(row, dict) for row in rows),
             f"invalid JSONL rows: {path.name}")
    return rows


def _verify_checksums(root: Path) -> dict[str, str]:
    lines = (root / "SHA256SUMS").read_text(encoding="ascii").splitlines()
    entries: dict[str, str] = {}
    for line in lines:
        match = re.fullmatch(r"([0-9a-f]{64})  ([A-Za-z0-9_.-]+)", line)
        _require(match is not None, "invalid checksum manifest")
        digest, name = match.groups()
        _require(name not in entries, "duplicate checksum entry")
        _require(_sha((root / name).read_bytes()) == digest,
                 f"checksum mismatch: {name}")
        entries[name] = digest
    return entries


def _nonnegative_int(value: Any, label: str) -> int:
    _require(type(value) is int and value >= 0, f"invalid {label}")
    return value


def _caption_request_rows(
    captions: list[dict[str, Any]], cache_root: Path,
) -> list[dict[str, Any]]:
    """Recover only requests for the exact frozen caption request bytes."""

    rows: list[dict[str, Any]] = []
    for caption in sorted(captions, key=lambda row: (
        row["object_id"], row["ordinal"]
    )):
        object_id = str(caption["object_id"])
        ordinal = _nonnegative_int(caption["ordinal"], "caption ordinal")
        window_id = f"{object_id}#win{ordinal:02d}"
        files = sorted((cache_root / "raw" / object_id).glob(
            f"{ordinal:02d}.attempt-*.json"
        ))
        _require(files, f"raw cache is missing: {window_id}")
        matches: list[dict[str, Any]] = []
        for path in files:
            match = re.fullmatch(
                rf"{ordinal:02d}\.attempt-([0-9]+)\.json", path.name
            )
            _require(match is not None, "raw cache filename is invalid")
            attempt = int(match.group(1))
            raw_bytes = path.read_bytes()
            record = json.loads(raw_bytes)
            _require(isinstance(record, dict), "raw cache row is invalid")
            if (record.get("window_id") != window_id or
                    record.get("window_descriptor_sha256") !=
                    caption["window_descriptor_sha256"] or
                    record.get("request_input_sha256") !=
                    caption["request_input_sha256"]):
                continue  # A prior request for a different frozen input.
            _require(record.get("credentials_recorded") is False,
                     "raw cache records credentials")
            usage = record.get("usage")
            _require(isinstance(usage, dict), "provider usage is absent")
            input_units = _nonnegative_int(
                usage.get("prompt_tokens"), "caption input units"
            )
            output_units = _nonnegative_int(
                usage.get("completion_tokens"), "caption output units"
            )
            total_units = _nonnegative_int(
                usage.get("total_tokens"), "caption total units"
            )
            _require(input_units + output_units == total_units,
                     "caption provider usage does not add up")
            matches.append({
                "object_id": object_id,
                "window_id": window_id,
                "attempt": attempt,
                "request_sha256": record["request_input_sha256"],
                "response_sha256": record["response_sha256"],
                "raw_record_sha256": _sha(raw_bytes),
                "input_units": input_units,
                "output_units": output_units,
                "total_units": total_units,
                "selected_caption_response": (
                    record["response_sha256"] == caption["response_sha256"]
                ),
            })
        matches.sort(key=lambda row: row["attempt"])
        _require(matches, f"no exact raw request matches {window_id}")
        _require(len({row["attempt"] for row in matches}) == len(matches),
                 "duplicate raw request attempt")
        selected = [row for row in matches if row["selected_caption_response"]]
        _require(len(selected) == 1, "frozen caption response is not unique")
        _require(selected[0]["attempt"] == max(
            row["attempt"] for row in matches
        ), "selected caption is not the last matching request")
        rows.extend(matches)
    return rows


def _embedding_request_rows(index: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for ordinal, receipt in enumerate(index["embedding_receipts"]):
        usage = receipt.get("usage")
        _require(isinstance(usage, dict), "embedding usage is absent")
        units = _nonnegative_int(usage.get("total_tokens"),
                                 "embedding units")
        _require(usage.get("prompt_tokens") == units,
                 "embedding input and total units differ")
        seconds = receipt.get("service_time_seconds")
        _require(type(seconds) in (int, float) and seconds >= 0,
                 "embedding service time is invalid")
        rows.append({
            "ordinal": ordinal,
            "input_count": _nonnegative_int(receipt.get("input_count"),
                                             "embedding input count"),
            "request_sha256": receipt["request_sha256"],
            "response_sha256": receipt["response_sha256"],
            "input_units": units,
            "service_time_seconds": seconds,
        })
    _require(len(rows) == index["embedding_request_count"],
             "embedding request count differs")
    _require(sum(row["input_count"] for row in rows)
             == index["embedding_input_count"],
             "embedding input count differs")
    return rows


def _caption_service_times(
    cost: Mapping[str, Any], caption_rows: list[dict[str, Any]],
) -> dict[str, float]:
    selected = {
        row["window_id"]: row for row in caption_rows
        if row["selected_caption_response"]
    }
    times: dict[str, float] = {}
    for receipt in cost["per_request_usage"]:
        window_id = receipt["window_id"]
        _require(window_id in selected and window_id not in times,
                 "caption cost receipt names an unknown or duplicate window")
        usage = receipt["provider_usage"]
        row = selected[window_id]
        _require(usage["prompt_tokens"] == row["input_units"] and
                 usage["completion_tokens"] == row["output_units"],
                 "caption cost receipt differs from raw provider usage")
        seconds = receipt["service_time_seconds"]
        _require(type(seconds) in (int, float) and seconds >= 0,
                 "caption service time is invalid")
        times[window_id] = seconds
    _require(len(times) == cost["provider_request_count_this_run"],
             "caption receipt request count differs")
    return times


def _object_rows(
    cases: list[dict[str, Any]], captions: list[dict[str, Any]],
    requests: list[dict[str, Any]], times: Mapping[str, float],
) -> list[dict[str, Any]]:
    by_object: dict[str, list[dict[str, Any]]] = {}
    for row in requests:
        by_object.setdefault(row["object_id"], []).append(row)
    caption_by_object: dict[str, list[dict[str, Any]]] = {}
    for row in captions:
        caption_by_object.setdefault(row["object_id"], []).append(row)
    _require(set(by_object) == {case["object_id"] for case in cases},
             "caption costs do not cover the replay objects")
    rows = []
    for case in sorted(cases, key=lambda row: row["object_id"]):
        object_id = case["object_id"]
        own = by_object[object_id]
        windows = caption_by_object[object_id]
        _require(len(windows) == sum(
            row["selected_caption_response"] for row in own
        ), "caption response coverage differs")
        complete_time = all(
            f"{object_id}#win{int(row['ordinal']):02d}" in times
            for row in windows
        )
        rows.append({
            "object_id": object_id,
            "source_video_sha256": case["materialization_source_video_sha256"],
            "caption_window_count": len(windows),
            "caption_request_count": len(own),
            "caption_input_units": sum(row["input_units"] for row in own),
            "caption_output_units": sum(row["output_units"] for row in own),
            "caption_total_units": sum(row["total_units"] for row in own),
            "caption_provider_service_seconds": (
                round(sum(times[f"{object_id}#win{int(row['ordinal']):02d}"]
                          for row in windows), 6)
                if complete_time else None
            ),
            "embedding_units_allocated_to_object": None,
            "frame_decode_cpu_seconds": None,
            "n4_publication_seconds": None,
            "actual_billed_cost": None,
        })
    return rows


def _verify_n4_derivations(
    n4: Mapping[str, Any], cases: list[dict[str, Any]],
    preparation_sha256: str, caption_sha256: str,
) -> None:
    by_object = {case["object_id"]: case for case in cases}
    rows = n4["objects"]
    _require(len(rows) == 2 * len(cases),
             "N4 representation count differs from replay")
    observed: set[tuple[str, str]] = set()
    for row in rows:
        object_id = row["object_id"]
        representation = row["representation_id"]
        key = (object_id, representation)
        _require(object_id in by_object and key not in observed and
                 representation in {"sampled_frame_bundle", "multimodal_digest"},
                 "N4 object or representation differs")
        observed.add(key)
        provenance = row["provenance"]
        source_sha = by_object[object_id][
            "materialization_source_video_sha256"
        ]
        _require(provenance["source_content_sha256"] == source_sha,
                 "N4 source video differs from replay")
        derivation = {
            "derivation_id": provenance["derivation_id"],
            "object_id": object_id,
            "representation_id": representation,
            "source_video_sha256": source_sha,
            "preparation_sha256": preparation_sha256,
            "caption_package_sha256": caption_sha256,
        }
        _require(provenance["derivation_sha256"] == _sha(
            _canonical(derivation)
        ), "N4 artifact does not bind measured caption production")


def build_materialization_cost_evidence(
    replay_dir: str | Path, preparation_dir: str | Path,
    caption_dir: str | Path, index_dir: str | Path,
    n4_package_dir: str | Path,
    raw_cache_dir: str | Path, *, output_dir: str | Path,
    package_id: str, builder_commit: str,
) -> dict[str, Any]:
    _require(re.fullmatch(r"[0-9a-f]{40}", builder_commit) is not None,
             "builder commit is invalid")
    _require(re.fullmatch(r"[A-Za-z0-9_.-]+", package_id) is not None,
             "package ID is invalid")
    target = Path(output_dir).resolve()
    _require(not target.exists(), "cost evidence output already exists")
    replay = load_offline_replay_v2(replay_dir)
    cases = replay["cases"]
    prep_root = Path(preparation_dir).resolve()
    caption_root = Path(caption_dir).resolve()
    index_root = Path(index_dir).resolve()
    caption_verified = verify_formal_temporal_caption_package(
        caption_root, prep_root
    )
    caption_manifest = _read_object(
        caption_root / "temporal-caption-package.json"
    )
    captions = _read_jsonl(caption_root / "fine-captions.jsonl")
    _require(len(captions) == caption_verified["caption_count"],
             "caption row count differs")
    case_by_object = {case["object_id"]: case for case in cases}
    _require(set(case_by_object) == {row["object_id"] for row in captions},
             "caption objects differ from replay cases")
    for row in captions:
        _require(row["source_video_sha256"] == case_by_object[
            row["object_id"]
        ]["materialization_source_video_sha256"],
                 "caption source video differs from replay")
    index_checksums = _verify_checksums(index_root)
    index_bytes = (index_root / "temporal-index-package.json").read_bytes()
    index = json.loads(index_bytes)
    supplied = index.pop("package_sha256")
    _require(supplied == _sha(_canonical(index)),
             "index package self digest differs")
    _require(index["caption_package_sha256"]
             == caption_verified["package_sha256"] and
             index["preparation_sha256"]
             == caption_manifest["preparation_sha256"],
             "index does not bind these captions and preparation")
    replay_bindings = replay["manifest"]["materialization_source_bindings"]
    _require(replay_bindings["preparation_sha256"]
             == index["preparation_sha256"] and
             replay_bindings["caption_package_sha256"]
             == caption_verified["package_sha256"],
             "replay does not bind measured preparation and captions")
    n4_root = Path(n4_package_dir).resolve()
    n4_verified = verify_n4_derived_data_package(n4_root)
    _require(replay_bindings["n4_package_sha256"]
             == n4_verified["package_sha256"],
             "replay N4 package differs from supplied package")
    _verify_n4_derivations(
        _read_object(n4_root / "n4-derived-data-package.json"), cases,
        index["preparation_sha256"], caption_verified["package_sha256"],
    )
    _require(index["object_count"] == len(cases) and
             index["window_vector_count"] == len(captions) and
             index["anchor_vector_count"] == len(cases),
             "index dimensions differ from replay cohort")
    _require("temporal-index-package.json" in index_checksums,
             "index manifest is not checksum-bound")
    cost_bytes = (caption_root / "temporal-index-build-cost.json").read_bytes()
    cost = json.loads(cost_bytes)
    _require(cost["caption_model_id"] == caption_manifest["model_id"] and
             cost["caption_window_count"] == len(captions),
             "caption cost receipt differs from package")
    caption_requests = _caption_request_rows(
        captions, Path(raw_cache_dir).resolve()
    )
    embedding_requests = _embedding_request_rows(index)
    times = _caption_service_times(cost, caption_requests)
    objects = _object_rows(cases, captions, caption_requests, times)
    manifest = {
        "schema_version": SCHEMA,
        "package_id": package_id,
        "builder_commit": builder_commit,
        "source_replay_sha256": replay["package_sha256"],
        "source_preparation_sha256": index["preparation_sha256"],
        "source_caption_package_sha256": caption_verified["package_sha256"],
        "source_caption_cost_sha256": _sha(cost_bytes),
        "source_index_package_sha256": supplied,
        "source_index_checksums_sha256": _sha(
            (index_root / "SHA256SUMS").read_bytes()
        ),
        "source_n4_package_sha256": n4_verified["package_sha256"],
        "object_count": len(objects),
        "caption_window_count": len(captions),
        "caption_request_count": len(caption_requests),
        "caption_input_units": sum(
            row["input_units"] for row in caption_requests
        ),
        "caption_output_units": sum(
            row["output_units"] for row in caption_requests
        ),
        "caption_total_units": sum(
            row["total_units"] for row in caption_requests
        ),
        "caption_service_time_covered_windows": len(times),
        "embedding_request_count": len(embedding_requests),
        "embedding_input_count": sum(
            row["input_count"] for row in embedding_requests
        ),
        "embedding_input_units": sum(
            row["input_units"] for row in embedding_requests
        ),
        "embedding_provider_service_seconds": round(sum(
            row["service_time_seconds"] for row in embedding_requests
        ), 6),
        "measurement_boundaries": {
            "provider_usage": "measured-for-this-frozen-cohort",
            "caption_build_wall_time": "unknown",
            "frame_decode_cpu": "unknown",
            "n4_publication": "unknown",
            "n6_inference_usage": "unknown",
            "actual_billed_cost": "unknown",
            "list_price_estimate_included": False,
            "cross_batch_embedding_allocation": False,
            "raw_response_content_included": False,
            "derived_n4_provenance_checked": True,
        },
        "credentials_recorded": False,
        "hidden_label_values_included": False,
    }
    payloads = {
        FILES[0]: _json_bytes(manifest),
        FILES[1]: _jsonl_bytes(caption_requests),
        FILES[2]: _jsonl_bytes(embedding_requests),
        FILES[3]: _jsonl_bytes(objects),
    }
    payloads["SHA256SUMS"] = b"".join(
        f"{_sha(payloads[name])}  {name}\n".encode("ascii")
        for name in FILES
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.",
                                        dir=target.parent))
    try:
        for name, data in payloads.items():
            (staging / name).write_bytes(data)
        verify_materialization_cost_evidence(staging)
        os.replace(staging, target)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return {
        "status": "FROZEN_MEASURED_PROVIDER_USAGE_PARTIAL_COST",
        "package_dir": str(target),
        "caption_request_count": len(caption_requests),
        "embedding_request_count": len(embedding_requests),
        "actual_billed_cost_measured": False,
        "external_calls_made": False,
    }


def verify_materialization_cost_evidence(
    package_dir: str | Path,
) -> dict[str, Any]:
    root = Path(package_dir).resolve()
    _require(root.is_dir(), "cost evidence package is missing")
    _require({path.name for path in root.iterdir() if path.is_file()}
             == set(FILES) | {"SHA256SUMS"},
             "cost evidence file set differs")
    entries = _verify_checksums(root)
    _require(set(entries) == set(FILES), "cost evidence checksums differ")
    for name in FILES:
        _require(b"\r" not in (root / name).read_bytes(),
                 "cost evidence contains CR bytes")
    manifest = _read_object(root / FILES[0])
    captions = _read_jsonl(root / FILES[1])
    embeddings = _read_jsonl(root / FILES[2])
    objects = _read_jsonl(root / FILES[3])
    _require(manifest["schema_version"] == SCHEMA and
             manifest["credentials_recorded"] is False and
             manifest["hidden_label_values_included"] is False,
             "cost evidence identity or safety flags differ")
    _require(set(manifest) == MANIFEST_FIELDS,
             "cost evidence manifest field set differs")
    _require(manifest["measurement_boundaries"] == {
        "provider_usage": "measured-for-this-frozen-cohort",
        "caption_build_wall_time": "unknown",
        "frame_decode_cpu": "unknown",
        "n4_publication": "unknown",
        "n6_inference_usage": "unknown",
        "actual_billed_cost": "unknown",
        "list_price_estimate_included": False,
        "cross_batch_embedding_allocation": False,
        "raw_response_content_included": False,
        "derived_n4_provenance_checked": True,
    }, "cost evidence claim boundary changed")
    for row in captions:
        _require(set(row) == CAPTION_FIELDS and
                 re.fullmatch(r"nextqa-val-[0-9]+", row["object_id"])
                 is not None and
                 row["window_id"].startswith(row["object_id"] + "#win"),
                 "caption cost row has unsafe fields")
        _require(row["input_units"] + row["output_units"]
                 == row["total_units"], "caption row units differ")
    for row in embeddings:
        _require(set(row) == EMBEDDING_FIELDS,
                 "embedding cost row has unsafe fields")
    for row in objects:
        _require(set(row) == OBJECT_FIELDS and
                 re.fullmatch(r"nextqa-val-[0-9]+", row["object_id"])
                 is not None and
                 row["embedding_units_allocated_to_object"] is None and
                 row["frame_decode_cpu_seconds"] is None and
                 row["n4_publication_seconds"] is None,
                 "object cost row overstates measured fields")
    _require((root / FILES[0]).read_bytes() == _json_bytes(manifest),
             "cost evidence manifest is not canonical")
    for name, rows in zip(FILES[1:], (captions, embeddings, objects), strict=True):
        _require((root / name).read_bytes() == _jsonl_bytes(rows),
                 f"{name} is not canonical")
    _require(manifest["caption_request_count"] == len(captions) and
             manifest["caption_input_units"] == sum(
                 row["input_units"] for row in captions
             ) and manifest["caption_output_units"] == sum(
                 row["output_units"] for row in captions
             ) and manifest["caption_total_units"] == sum(
                 row["total_units"] for row in captions
             ), "caption totals differ")
    _require(manifest["embedding_request_count"] == len(embeddings) and
             manifest["embedding_input_count"] == sum(
                 row["input_count"] for row in embeddings
             ) and manifest["embedding_input_units"] == sum(
                 row["input_units"] for row in embeddings
             ), "embedding totals differ")
    _require(manifest["object_count"] == len(objects) and
             manifest["caption_window_count"] == sum(
                 row["caption_window_count"] for row in objects
             ), "object totals differ")
    _require(all(row["actual_billed_cost"] is None for row in objects),
             "cost evidence incorrectly claims actual billed cost")
    return {
        "status": "VERIFIED_MEASURED_PROVIDER_USAGE_PARTIAL_COST",
        "package_id": manifest["package_id"],
        "package_sha256": _sha((root / "SHA256SUMS").read_bytes()),
        "object_count": len(objects),
        "caption_request_count": len(captions),
        "embedding_request_count": len(embeddings),
        "actual_billed_cost_measured": False,
        "credentials_recorded": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay-dir", type=Path, required=True)
    parser.add_argument("--preparation-dir", type=Path, required=True)
    parser.add_argument("--caption-dir", type=Path, required=True)
    parser.add_argument("--index-dir", type=Path, required=True)
    parser.add_argument("--n4-package-dir", type=Path, required=True)
    parser.add_argument("--raw-cache-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--package-id", required=True)
    parser.add_argument("--builder-commit", required=True)
    args = parser.parse_args()
    print(json.dumps(build_materialization_cost_evidence(
        args.replay_dir, args.preparation_dir, args.caption_dir,
        args.index_dir, args.n4_package_dir, args.raw_cache_dir,
        output_dir=args.output_dir,
        package_id=args.package_id, builder_commit=args.builder_commit,
    ), sort_keys=True))


if __name__ == "__main__":
    main()
