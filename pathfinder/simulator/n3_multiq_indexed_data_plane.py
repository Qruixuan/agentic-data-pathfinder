"""Question-bound N3 projections sharing one authoritative video object.

Each question receives a different Data Agent plan ID. The external
representation remains ``indexed_temporal_frame_bundle``; the plan path
selects the exact predecoded bundle. No MP4 byte-range saving is claimed.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any, Mapping, Sequence

from ..data_agent_manifest import (
    DATA_AGENT_MANIFEST_VERSION,
    DATA_OBJECT_CATALOG_VERSION,
    load_data_agent_manifest,
)
from ..frame_bundle_ingest import FRAME_BUNDLE_MEDIA_TYPE
from ..rsi_exam.interleaved_multiq_plan import verify_interleaved_plan
from ..rsi_exam.temporal_index_collection import (
    PREPARATION_MANIFEST,
    _fraction_pair,
    verify_formal_temporal_index_preparation,
)
from ..rsi_exam.temporal_index_layers import (
    QUERY_SELECTIONS,
    verify_temporal_query_batch,
)
from ..video_prep import sample_video
from .n3_indexed_data_plane import (
    INDEXED_REPRESENTATION_ID,
    N3TemporalSelectionPolicy,
    TEMPORAL_INDEX_SELECTED_SAMPLING_METHOD,
    _bundle,
    _canonical,
    _checksums,
    _hash_file,
    _identifier,
    _pretty,
    _read_json,
    _sha256,
)
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

SCHEMA = "pathfinder.simulator-n3-multiq-indexed-data-plane/v1alpha1"
_QUESTION_FIELDS = frozenset({
    "question_id", "object_id", "task_binding_sha256",
    "public_question_sha256", "selection_policy",
})


class N3MultiQuestionPackageError(ValueError):
    """The public question, raw source, or projection binding differs."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise N3MultiQuestionPackageError(message)


def _relative(path: str) -> Path:
    pure = PurePosixPath(path)
    _require(not pure.is_absolute() and ".." not in pure.parts,
             "package path escapes its directory")
    return Path(*pure.parts)


def _questions(
    values: Sequence[Mapping[str, Any]], raw_ids: set[str],
) -> list[dict[str, Any]]:
    _require(bool(values), "multi-question N3 package has no questions")
    result = []
    for value in values:
        _require(isinstance(value, Mapping)
                 and set(value) == _QUESTION_FIELDS,
                 "multi-question selection has unsafe or missing fields")
        question_id = _identifier(value["question_id"], "question_id")
        object_id = _identifier(value["object_id"], "object_id")
        _require(object_id in raw_ids, "question video is absent from N3")
        task_sha = str(value["task_binding_sha256"])
        question_sha = str(value["public_question_sha256"])
        _require(len(task_sha) == 64 and len(question_sha) == 64
                 and all(ch in "0123456789abcdef" for ch in task_sha + question_sha),
                 "question binding digest is invalid")
        policy = value["selection_policy"]
        _require(isinstance(policy, N3TemporalSelectionPolicy)
                 and policy.sampling_method
                 == TEMPORAL_INDEX_SELECTED_SAMPLING_METHOD
                 and policy.selection_provenance is not None
                 and policy.selection_provenance["public_question_sha256"]
                 == question_sha,
                 "question policy is not bound to the public question")
        result.append({
            "question_id": question_id,
            "object_id": object_id,
            "task_binding_sha256": task_sha,
            "public_question_sha256": question_sha,
            "selection_policy": policy.to_dict(),
        })
    _require(len({row["question_id"] for row in result}) == len(result)
             and len({row["task_binding_sha256"] for row in result})
             == len(result), "multi-question identities repeat")
    return sorted(result, key=lambda row: row["question_id"])


def _agent_documents(
    raw_rows: Sequence[Mapping[str, Any]],
    selections: Sequence[Mapping[str, Any]],
    catalog_version: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    binding = {
        "location": SOURCE_LOCATION,
        "minimum_latency_ms": 0.0,
        "realized_cost": 0.0,
        "cache_hit": False,
    }
    plan_ids = sorted({str(row["plan_id"]) for row in selections})
    by_object: dict[str, dict[str, Any]] = {}
    for row in raw_rows:
        object_id = str(row["object_id"])
        path = "../" + str(row["artifact_package_path"])
        by_object[object_id] = {"representations": {
            RAW_REPRESENTATION_ID: {
                "path": path,
                "plan_paths": {plan_id: path for plan_id in plan_ids},
            }
        }}
    for row in selections:
        representations = by_object[row["object_id"]]["representations"]
        path = "../" + row["artifact_package_path"]
        entry = representations.setdefault(INDEXED_REPRESENTATION_ID, {
            # The object-catalog API falls back to ``path`` for an unknown
            # plan ID even when the manifest requires a plan binding.
            # Point that fallback at a deliberately absent file so another
            # question's valid global plan cannot select this video's first
            # question-specific bundle by accident.
            "path": "../unbound-indexed-plan-do-not-use", "plan_paths": {},
        })
        entry["plan_paths"][row["plan_id"]] = path
    representations = {}
    for representation, media_type in (
        (RAW_REPRESENTATION_ID, RAW_MEDIA_TYPE),
        (INDEXED_REPRESENTATION_ID, FRAME_BUNDLE_MEDIA_TYPE),
    ):
        representations[representation] = {
            "kind": "artifact_uri",
            "media_type": media_type,
            "default_binding": dict(binding),
            "plan_bindings": {plan_id: dict(binding) for plan_id in plan_ids},
        }
    return (
        {
            "schema_version": DATA_AGENT_MANIFEST_VERSION,
            "node_id": SOURCE_NODE_ID,
            "require_plan_binding": True,
            "object_catalog_path": "object-catalog.json",
            "representations": representations,
        },
        {
            "schema_version": DATA_OBJECT_CATALOG_VERSION,
            "catalog_version": catalog_version,
            "objects": by_object,
        },
    )


def _build_documents(
    raw_root: Path, stage: Path, questions: Sequence[Mapping[str, Any]],
    *, package_id: str, sampler: Any,
) -> dict[str, Any]:
    raw_summary = verify_raw_cold_data_plane_package(raw_root)
    raw_report = _read_json(raw_root / PACKAGE_MANIFEST_NAME, "raw package")
    raw_rows = [dict(row) for row in raw_report["objects"]]
    by_raw = {str(row["object_id"]): row for row in raw_rows}
    _require(len(by_raw) == len(raw_rows), "N3 raw objects repeat")
    normalized = _questions(questions, set(by_raw))
    for row in raw_rows:
        relative = _relative(str(row["artifact_package_path"]))
        target = stage / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(raw_root / relative, target)
    selections = []
    for question in normalized:
        object_id = question["object_id"]
        raw_row = by_raw[object_id]
        policy = N3TemporalSelectionPolicy(
            frame_count=question["selection_policy"]["frame_count"],
            jpeg_max_dimension=question["selection_policy"]["jpeg_max_dimension"],
            temporal_start_fraction=(
                question["selection_policy"]["temporal_window_fraction"][0]
            ),
            temporal_end_fraction=(
                question["selection_policy"]["temporal_window_fraction"][1]
            ),
            sampling_method=question["selection_policy"]["sampling_method"],
            selection_provenance=question["selection_policy"][
                "temporal_index_selection"
            ],
        )
        artifact, embedded, duration = _bundle(
            stage / _relative(str(raw_row["artifact_package_path"])),
            raw_row, policy, sampler,
        )
        task_sha = question["task_binding_sha256"]
        relative = (
            f"artifacts/{object_id}/multiq-indexed/{task_sha}.tar"
        )
        target = stage / _relative(relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(artifact)
        selections.append({
            **question,
            "plan_id": f"n3-multiq-{task_sha}",
            "artifact_package_path": relative,
            "artifact_sha256": _sha256(artifact),
            "artifact_size_bytes": len(artifact),
            "source_artifact_sha256": raw_row["artifact_sha256"],
            "source_artifact_size_bytes": raw_row["artifact_size_bytes"],
            "source_duration_seconds": duration,
            "embedded_manifest_sha256": _sha256(_pretty(embedded)),
            "selection_policy_sha256": _sha256(
                _canonical(question["selection_policy"])
            ),
        })
    agent, catalog = _agent_documents(
        raw_rows, selections, str(raw_report["catalog_version"])
    )
    source_binding = {
        "package_id": raw_summary["package_id"],
        "catalog_version": raw_summary["catalog_version"],
        "manifest_sha256": _sha256(
            (raw_root / PACKAGE_MANIFEST_NAME).read_bytes()
        ),
        "checksums_sha256": _sha256(
            (raw_root / CHECKSUMS_NAME).read_bytes()
        ),
    }
    report = {
        "schema_version": SCHEMA,
        "status": "FROZEN_N3_MULTIQ_INDEXED_DATA_PLANE",
        "package_id": package_id,
        "catalog_version": raw_report["catalog_version"],
        "object_count": len(raw_rows),
        "question_count": len(selections),
        "raw_objects": raw_rows,
        "question_selections": selections,
        "source_raw_package_binding": source_binding,
        "partial_mp4_byte_range_claimed": False,
        "reduced_source_storage_io_claimed": False,
        "runtime_execution_verified": False,
        "workflow_submitted": False,
        "llm_called": False,
        "credentials_recorded": False,
    }
    for relative, value in (
        (DATA_AGENT_MANIFEST_PATH, agent),
        (OBJECT_CATALOG_PATH, catalog),
        (PACKAGE_MANIFEST_NAME, report),
    ):
        target = stage / _relative(relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(_pretty(value))
    (stage / CHECKSUMS_NAME).write_bytes(_checksums(stage))
    return report


def build_n3_multiq_indexed_package(
    raw_package_dir: str | Path,
    *, output_dir: str | Path, package_id: str,
    question_policies: Sequence[Mapping[str, Any]],
    sampler: Any = sample_video,
) -> dict[str, Any]:
    """Freeze question-specific bundles without changing video identity."""

    _identifier(package_id, "package_id")
    raw_root = Path(raw_package_dir).resolve()
    target = Path(output_dir).resolve()
    _require(not target.exists(), "N3 multi-question output already exists")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".n3-multiq-", dir=target.parent))
    stage = staging / "package"
    try:
        stage.mkdir()
        _build_documents(raw_root, stage, question_policies,
                         package_id=package_id, sampler=sampler)
        verify_n3_multiq_indexed_package(
            stage, raw_package_dir=raw_root,
            question_policies=question_policies, sampler=sampler,
        )
        os.replace(stage, target)
    finally:
        shutil.rmtree(staging)
    return verify_n3_multiq_indexed_package(
        target, raw_package_dir=raw_root,
        question_policies=question_policies, sampler=sampler,
    )


def verify_n3_multiq_indexed_package(
    package_dir: str | Path,
    *, raw_package_dir: str | Path,
    question_policies: Sequence[Mapping[str, Any]],
    sampler: Any = sample_video,
) -> dict[str, Any]:
    """Rebuild into a temporary root and compare the exact frozen file set."""

    root = Path(package_dir).resolve()
    _require(root.is_dir(), "N3 multi-question package is missing")
    _require(
        all(not path.is_symlink() for path in root.rglob("*")),
        "N3 multi-question package contains a symbolic link",
    )
    report = _read_json(root / PACKAGE_MANIFEST_NAME, "multiq N3 package")
    _require(report.get("schema_version") == SCHEMA
             and report.get("status") == "FROZEN_N3_MULTIQ_INDEXED_DATA_PLANE"
             and report.get("workflow_submitted") is False
             and report.get("llm_called") is False
             and report.get("credentials_recorded") is False,
             "N3 multi-question package safety contract differs")
    _require((root / CHECKSUMS_NAME).read_bytes() == _checksums(root),
             "N3 multi-question checksums differ")
    with tempfile.TemporaryDirectory(prefix="n3-multiq-verify-") as temporary:
        expected = Path(temporary)
        _build_documents(
            Path(raw_package_dir).resolve(), expected, question_policies,
            package_id=report["package_id"], sampler=sampler,
        )
        expected_files = {path.relative_to(expected).as_posix()
                          for path in expected.rglob("*") if path.is_file()}
        actual_files = {path.relative_to(root).as_posix()
                        for path in root.rglob("*") if path.is_file()}
        _require(actual_files == expected_files,
                 "N3 multi-question package file set differs")
        for name in expected_files:
            _require((root / _relative(name)).read_bytes()
                     == (expected / _relative(name)).read_bytes(),
                     f"N3 multi-question package content differs: {name}")
    manifest = load_data_agent_manifest(root / DATA_AGENT_MANIFEST_PATH)
    _require(not (root / "unbound-indexed-plan-do-not-use").exists(),
             "N3 unbound-plan sentinel unexpectedly exists")
    for row in report["question_selections"]:
        resolved = manifest.resolve(
            plan_id=row["plan_id"], object_id=row["object_id"],
            representation_id=INDEXED_REPRESENTATION_ID,
            requested_location=SOURCE_LOCATION,
        )
        digest, size = _hash_file(resolved.path)
        _require(digest == row["artifact_sha256"]
                 and size == row["artifact_size_bytes"],
                 "question-specific Data Agent plan resolves wrong bytes")
        for other in report["raw_objects"]:
            if other["object_id"] == row["object_id"]:
                continue
            unrelated = manifest.resolve(
                plan_id=row["plan_id"], object_id=other["object_id"],
                representation_id=INDEXED_REPRESENTATION_ID,
                requested_location=SOURCE_LOCATION,
            )
            _require(
                not unrelated.path.is_file(),
                "question plan unexpectedly resolves another video's bundle",
            )
    return {
        "status": "VERIFIED_N3_MULTIQ_INDEXED_DATA_PLANE",
        "package_id": report["package_id"],
        "object_count": report["object_count"],
        "question_count": report["question_count"],
        "workflow_submitted": False,
        "credentials_recorded": False,
    }


def derive_n3_multiq_question_policies(
    *,
    plan_dir: str | Path,
    public_questions: Sequence[Mapping[str, Any]],
    public_source_sha256: str,
    query_dir: str | Path,
    video_index_dir: str | Path,
    preparation_dir: str | Path,
    caption_dir: str | Path,
    raw_package_dir: str | Path,
    frame_count: int = 4,
    jpeg_max_dimension: int = 768,
) -> list[dict[str, Any]]:
    """Derive six task-bound N3 policies from verified, public query results.

    A selection with disjoint caption windows uses its explicitly frozen
    ``selected_span_seconds`` as a convex-hull projection.  The provenance
    retains the exact merged intervals, so this cannot be misreported as
    decoding only the disjoint windows or as reduced MP4 storage I/O.
    """

    plan = verify_interleaved_plan(
        plan_dir, public_questions,
        public_source_sha256=public_source_sha256,
    )
    simplified = [
        {key: row[key] for key in ("question_id", "object_id", "question")}
        for row in public_questions
    ]
    query = verify_temporal_query_batch(
        query_dir, video_index_dir, preparation_dir, caption_dir, simplified,
    )
    verify_formal_temporal_index_preparation(preparation_dir)
    verify_raw_cold_data_plane_package(raw_package_dir)
    prep = _read_json(
        Path(preparation_dir) / PREPARATION_MANIFEST, "preparation manifest"
    )
    raw = _read_json(
        Path(raw_package_dir) / PACKAGE_MANIFEST_NAME, "raw package"
    )
    prep_by_object = {row["object_id"]: row for row in prep["objects"]}
    raw_by_object = {row["object_id"]: row for row in raw["objects"]}
    question_by_id = {row["question_id"]: row for row in public_questions}
    selections = [json.loads(line) for line in
                  (Path(query_dir) / QUERY_SELECTIONS)
                  .read_text(encoding="utf-8").splitlines()]
    _require(
        len(selections) == plan["question_count"]
        and {row["question_id"] for row in selections} == set(question_by_id),
        "query selections differ from the frozen public questions",
    )
    result = []
    for row in sorted(selections, key=lambda item: item["question_id"]):
        question = question_by_id[row["question_id"]]
        object_id = question["object_id"]
        original = raw_by_object.get(object_id)
        prepared = prep_by_object.get(object_id)
        _require(
            row["object_id"] == object_id
            and row["question_sha256"]
            == _sha256(question["question"].encode("utf-8"))
            and isinstance(original, Mapping)
            and isinstance(prepared, Mapping)
            and prepared["source_video_sha256"]
            == original["artifact_sha256"]
            and prepared["source_video_size_bytes"]
            == original["artifact_size_bytes"],
            "query selection does not bind the N3 source and public question",
        )
        selection = row["selection"]
        _require(selection["fallback_used"] is False,
                 "query-aware N3 policy cannot use a fallback")
        start, end = selection["selected_span_seconds"]
        duration = float(prepared["duration_seconds"])
        start_pair = _fraction_pair(float(start), duration, is_end=False)
        end_pair = _fraction_pair(float(end), duration, is_end=True)
        provenance = {
            "action_id": selection["action_id"],
            "anchor_top_k": selection["anchor_top_k"],
            "anchor_window_ordinals": selection["anchor_window_ordinals"],
            "expansion_basis": selection["expansion_basis"],
            "fallback_used": False,
            "max_selected_windows": selection["max_selected_windows"],
            "merged_intervals_seconds": selection["merged_intervals_seconds"],
            "public_question_sha256": row["question_sha256"],
            "relation": selection["relation"],
            "selected_window_ordinals": selection["selected_window_ordinals"],
            "temporal_index_package_sha256": query["package_sha256"],
        }
        policy = N3TemporalSelectionPolicy(
            frame_count=frame_count,
            jpeg_max_dimension=jpeg_max_dimension,
            temporal_start_fraction=start_pair[0] / start_pair[1],
            temporal_end_fraction=end_pair[0] / end_pair[1],
            sampling_method=TEMPORAL_INDEX_SELECTED_SAMPLING_METHOD,
            selection_provenance=provenance,
        )
        result.append({
            "question_id": question["question_id"],
            "object_id": object_id,
            "task_binding_sha256": question["public_task_sha256"],
            "public_question_sha256": row["question_sha256"],
            "selection_policy": policy,
        })
    return result


__all__ = [
    "N3MultiQuestionPackageError", "SCHEMA",
    "build_n3_multiq_indexed_package",
    "derive_n3_multiq_question_policies",
    "verify_n3_multiq_indexed_package",
]
