"""Freeze a question-independent, caption-free N4 frame representation.

This package is deliberately independent of temporal captions and embeddings.
Its only media input is the source MP4 in the verified N3 raw package.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import tempfile
from time import perf_counter_ns, process_time_ns
from typing import Any, Callable

from ..simulator.n4_derived_data_plane import (
    N4ArtifactProvenance,
    N4DerivedArtifactInput,
    build_n4_derived_data_package,
    verify_n4_derived_data_package,
)
from ..simulator.raw_cold_data_plane import (
    verify_raw_cold_data_plane_package,
)
from ..video_prep import sample_video
from .formal_foundation import _frame_bundle

SCHEMA = "pathfinder.rsi-exam-lightweight-derived-build/v1"
POLICY_ID = "uniform-24-source-decoded-jpeg-768-q82-v1"
DERIVATION_ID = "rsi-exam-question-independent-frame-only-v1"
FRAME_COUNT = 24
JPEG_MAX_DIMENSION = 768
PLAN_IDS = tuple(f"D{i}" for i in range(8))


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _pretty(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False,
                       allow_nan=False) + "\n").encode("utf-8")


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _source_rows(raw_dir: Path) -> dict[str, dict[str, Any]]:
    verify_raw_cold_data_plane_package(raw_dir)
    manifest = json.loads((raw_dir / "raw-cold-data-plane.json").read_bytes())
    rows = {row["object_id"]: row for row in manifest["objects"]}
    if len(rows) != len(manifest["objects"]) or not rows:
        raise ValueError("raw object identities repeat or are empty")
    return rows


def _build_inputs(
    raw_dir: Path, work_dir: Path, *, package_id: str,
    sampler: Callable[..., Any],
) -> tuple[list[N4DerivedArtifactInput], list[dict[str, Any]], str]:
    rows = _source_rows(raw_dir)
    policy = {
        "policy_id": POLICY_ID, "frame_count": FRAME_COUNT,
        "jpeg_max_dimension": JPEG_MAX_DIMENSION,
        "jpeg_quality": 82, "jpeg_optimize": True,
        "temporal_start_fraction": 0, "temporal_end_fraction": 1,
        "caption_or_embedding_required": False,
    }
    policy_sha = _sha(_canonical(policy))
    artifacts: list[N4DerivedArtifactInput] = []
    timings: list[dict[str, Any]] = []
    for object_id, raw in sorted(rows.items()):
        relative = PurePosixPath(raw["artifact_package_path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("raw artifact path escapes its package")
        source = raw_dir.joinpath(*relative.parts)
        if _sha(source.read_bytes()) != raw["artifact_sha256"]:
            raise ValueError("source MP4 differs from its verified identity")
        wall_start = perf_counter_ns()
        cpu_start = process_time_ns()
        sampled, duration = sampler(
            source, frame_count=FRAME_COUNT,
            jpeg_max_dimension=JPEG_MAX_DIMENSION,
            temporal_start_fraction=0.0, temporal_end_fraction=1.0,
        )
        decode_wall = perf_counter_ns() - wall_start
        decode_cpu = process_time_ns() - cpu_start
        if len(sampled) != FRAME_COUNT:
            raise ValueError("source decode did not yield 24 frames")
        frame_rows = []
        for index, frame in enumerate(sampled):
            if frame.frame_index != index:
                raise ValueError("source-decoded frames are out of order")
            path = work_dir / object_id / f"{index:03d}.jpg"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(frame.jpeg_bytes)
            frame_rows.append({
                "frame_index": index, "object_id": object_id,
                "package_path": path.relative_to(work_dir).as_posix(),
                "timestamp_seconds": frame.timestamp_seconds,
                "width": frame.width, "height": frame.height,
                "jpeg_size_bytes": len(frame.jpeg_bytes),
                "jpeg_sha256": _sha(frame.jpeg_bytes),
            })
        assembly_start = perf_counter_ns()
        bundle = _frame_bundle(
            object_id=object_id,
            object_row={
                "source_video_sha256": raw["artifact_sha256"],
                "source_video_size_bytes": raw["artifact_size_bytes"],
                "duration_seconds": duration,
            },
            frame_rows=frame_rows, preparation_root=work_dir,
            preparation_sha256=policy_sha,
            frame_description_path="source-decoded-frames.jsonl",
        )
        assembly_wall = perf_counter_ns() - assembly_start
        derivation = {
            "derivation_id": DERIVATION_ID,
            "object_id": object_id,
            "source_video_sha256": raw["artifact_sha256"],
            "policy_sha256": policy_sha,
            "frame_bundle_sha256": _sha(bundle),
        }
        artifacts.append(N4DerivedArtifactInput(
            object_id=object_id,
            representation_id="sampled_frame_bundle",
            artifact_bytes=bundle,
            plan_ids=PLAN_IDS,
            provenance=N4ArtifactProvenance(
                producer_node_id="N5",
                publication_source_id=f"{package_id}-{object_id}",
                source_representation_id="raw_video",
                source_content_sha256=raw["artifact_sha256"],
                derivation_id=DERIVATION_ID,
                derivation_sha256=_sha(_canonical(derivation)),
            ),
        ))
        timings.append({
            "object_id": object_id,
            "source_mp4_size_bytes": raw["artifact_size_bytes"],
            "selected_jpeg_bytes": sum(r["jpeg_size_bytes"]
                                       for r in frame_rows),
            "bundle_bytes": len(bundle),
            "bundle_sha256": _sha(bundle),
            "decode_wall_ns": decode_wall,
            "decode_cpu_ns": decode_cpu,
            "assembly_wall_ns": assembly_wall,
        })
    return artifacts, timings, policy_sha


def freeze_lightweight_derived(
    raw_package_dir: str | Path, *, output_dir: str | Path,
    package_id: str, sampler: Callable[..., Any] = sample_video,
) -> dict[str, Any]:
    """Build a new immutable eight-object N4 package and measured receipt."""
    raw = Path(raw_package_dir).resolve()
    target = Path(output_dir).resolve()
    if target.exists() or not package_id:
        raise ValueError("output exists or package ID is empty")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.",
                                        dir=target.parent))
    started = _utc()
    try:
        work = staging / ".frames"
        inputs, timings, policy_sha = _build_inputs(
            raw, work, package_id=package_id, sampler=sampler,
        )
        shutil.rmtree(work)
        build_n4_derived_data_package(
            inputs, output_dir=staging / "n4", package_id=package_id,
            catalog_version=f"{package_id}-catalog",
        )
        verified = verify_n4_derived_data_package(staging / "n4")
        receipt = {
            "schema_version": SCHEMA,
            "status": "FROZEN_LIGHTWEIGHT_DERIVED",
            "package_id": package_id,
            "policy_sha256": policy_sha,
            "source_raw_manifest_sha256": _sha(
                (raw / "raw-cold-data-plane.json").read_bytes()),
            "object_count": len(timings),
            "objects": timings,
            "started_utc": started, "finished_utc": _utc(),
            "n4_catalog_version": verified["catalog_version"],
            "provider_requests_made": 0,
            "caption_or_embedding_required": False,
            "task_outcomes_read": False,
            "hidden_label_values_read": False,
            "credentials_recorded": False,
        }
        (staging / "build-receipt.json").write_bytes(_pretty(receipt))
        (staging / "SHA256SUMS").write_bytes(
            f"{_sha((staging / 'build-receipt.json').read_bytes())}  "
            "build-receipt.json\n".encode("ascii"))
        os.replace(staging, target)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return verify_lightweight_derived(target, raw)


def verify_lightweight_derived(
    output_dir: str | Path, raw_package_dir: str | Path,
) -> dict[str, Any]:
    root = Path(output_dir).resolve()
    raw = Path(raw_package_dir).resolve()
    if {path.name for path in root.iterdir()} != {
        "n4", "build-receipt.json", "SHA256SUMS",
    }:
        raise ValueError("lightweight package file set changed")
    receipt_bytes = (root / "build-receipt.json").read_bytes()
    expected_sums = f"{_sha(receipt_bytes)}  build-receipt.json\n".encode()
    if (root / "SHA256SUMS").read_bytes() != expected_sums:
        raise ValueError("lightweight receipt checksum differs")
    receipt = json.loads(receipt_bytes)
    rows = _source_rows(raw)
    verified = verify_n4_derived_data_package(root / "n4")
    n4 = json.loads((root / "n4/n4-derived-data-package.json").read_bytes())
    artifacts = {row["object_id"]: row for row in n4["objects"]}
    timing_rows = receipt.get("objects")
    timings = ({row["object_id"]: row for row in timing_rows}
               if isinstance(timing_rows, list)
               and all(isinstance(row, dict) and "object_id" in row
                       for row in timing_rows) else {})
    if (receipt.get("schema_version") != SCHEMA
            or receipt.get("caption_or_embedding_required") is not False
            or receipt.get("provider_requests_made") != 0
            or receipt.get("task_outcomes_read") is not False
            or receipt.get("hidden_label_values_read") is not False
            or receipt.get("credentials_recorded") is not False
            or receipt.get("source_raw_manifest_sha256") != _sha(
                (raw / "raw-cold-data-plane.json").read_bytes())
            or set(artifacts) != set(rows)
            or len(n4["objects"]) != len(rows)
            or set(timings) != set(rows)
            or len(timings) != len(rows)
            or receipt.get("object_count") != len(rows)):
        raise ValueError("lightweight build does not bind its source")
    for object_id, artifact in artifacts.items():
        if (artifact["representation_id"] != "sampled_frame_bundle"
                or artifact["provenance"]["derivation_id"] != DERIVATION_ID
                or artifact["provenance"]["source_content_sha256"]
                != rows[object_id]["artifact_sha256"]
                or artifact["plan_ids"] != list(PLAN_IDS)
                or timings[object_id]["bundle_sha256"]
                != artifact["artifact_sha256"]
                or timings[object_id]["bundle_bytes"]
                != artifact["artifact_size_bytes"]
                or timings[object_id]["source_mp4_size_bytes"]
                != rows[object_id]["artifact_size_bytes"]
                or any(type(timings[object_id].get(field)) is not int
                       or timings[object_id][field] < 0
                       for field in ("decode_wall_ns", "decode_cpu_ns",
                                     "assembly_wall_ns", "selected_jpeg_bytes"))):
            raise ValueError("lightweight N4 artifact provenance differs")
    return {
        "status": "VERIFIED_LIGHTWEIGHT_DERIVED",
        "object_count": len(rows),
        "n4_package_sha256": verified["package_sha256"],
        "provider_requests_made": 0,
        "credentials_recorded": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build or verify a caption-free source-decoded N4 package",
    )
    parser.add_argument("action", choices=("freeze", "verify"))
    parser.add_argument("--raw-package-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--package-id")
    args = parser.parse_args()
    if args.action == "freeze":
        if not args.package_id:
            parser.error("--package-id is required for freeze")
        result = freeze_lightweight_derived(
            args.raw_package_dir, output_dir=args.output_dir,
            package_id=args.package_id,
        )
    else:
        if args.package_id:
            parser.error("--package-id is not used for verification")
        result = verify_lightweight_derived(
            args.output_dir, args.raw_package_dir,
        )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
