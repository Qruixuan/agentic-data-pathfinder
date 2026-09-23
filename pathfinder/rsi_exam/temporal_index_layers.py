"""Reusable video index and separately frozen per-question temporal queries.

The existing one-question-per-object finalizer remains unchanged.  This
additive format lets several public questions share one immutable set of
caption vectors without sharing their anchor vectors or selected intervals.
It does not materialize N3 frame bundles or submit an experiment.
"""

from __future__ import annotations

import os
import re
import shutil
import tempfile
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from ..simulator.full_flow_fine_temporal_windows import caption_search_text
from ..simulator.full_flow_temporal_embeddings import (
    VECTOR_POLICY_ID,
    rank_segments_semantic,
)
from ..simulator.full_flow_temporal_index_v2 import (
    contextualize_anchor_clause,
    select_v2,
)
from ..simulator.raw_cold_data_plane import CHECKSUMS_NAME
from .temporal_index_collection import (
    BUILD_COST_NAME,
    CAPTIONS_NAME,
    WINDOWS_NAME,
    Transport,
    _canonical,
    _checksums,
    _default_transport,
    _embedding_batches,
    _hash_file,
    _json_bytes,
    _jsonl_bytes,
    _read_json,
    _read_jsonl,
    _require,
    _sha256,
    _verify_checksums,
    verify_formal_temporal_caption_package,
    verify_formal_temporal_index_preparation,
)

VIDEO_SCHEMA = "pathfinder.rsi-exam-video-temporal-index/v1alpha1"
QUERY_SCHEMA = "pathfinder.rsi-exam-temporal-query-batch/v1alpha1"
VIDEO_MANIFEST = "video-temporal-index.json"
VIDEO_VECTORS = "video-temporal-vectors.jsonl"
QUERY_MANIFEST = "temporal-query-batch.json"
QUERY_VECTORS = "temporal-query-vectors.jsonl"
QUERY_SELECTIONS = "temporal-query-selections.jsonl"
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")


def _source_rows(
    preparation_dir: str | Path,
    caption_dir: str | Path,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    prep_root = Path(preparation_dir).resolve()
    caption_root = Path(caption_dir).resolve()
    prep = verify_formal_temporal_index_preparation(prep_root)
    captions = verify_formal_temporal_caption_package(caption_root, prep_root)
    windows = _read_jsonl(prep_root / WINDOWS_NAME, "fine windows")
    caption_rows = _read_jsonl(caption_root / CAPTIONS_NAME, "fine captions")
    window_by_key = {(r["object_id"], r["ordinal"]): r for r in windows}
    caption_by_key = {(r["object_id"], r["ordinal"]): r for r in caption_rows}
    _require(
        len(window_by_key) == len(windows)
        and len(caption_by_key) == len(caption_rows)
        and set(window_by_key) == set(caption_by_key),
        "fine windows and captions do not match one-to-one",
    )
    rows = []
    for object_id, ordinal in sorted(window_by_key):
        window = window_by_key[(object_id, ordinal)]
        caption = caption_by_key[(object_id, ordinal)]
        rendered = caption_search_text(caption["structured_caption"])
        rows.append({
            "object_id": object_id,
            "ordinal": ordinal,
            "segment_ordinal": ordinal,
            "segment_id": window["window_id"],
            "window_id": window["window_id"],
            "start_seconds": window["start_seconds"],
            "end_seconds": window["end_seconds"],
            "input_sha256": _sha256(rendered.encode("utf-8")),
            "_text": rendered,
        })
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        counts[row["object_id"]] += 1
    _require(
        bool(counts) and all(count >= 2 for count in counts.values()),
        "every indexed video needs at least two windows",
    )
    return prep, captions, rows


def _public_rows(questions: Sequence[Mapping[str, Any]]) -> list[dict[str, str]]:
    """Accept only public, question-level fields; reject labels and outcomes."""

    _require(bool(questions), "at least one public question is required")
    rows = []
    for value in questions:
        _require(
            isinstance(value, Mapping)
            and set(value) == {"question_id", "object_id", "question"},
            "question row has missing or non-public fields",
        )
        question_id, object_id, question = (
            value["question_id"], value["object_id"], value["question"]
        )
        _require(
            isinstance(question_id, str)
            and _IDENTIFIER.fullmatch(question_id) is not None
            and isinstance(object_id, str)
            and _IDENTIFIER.fullmatch(object_id) is not None
            and isinstance(question, str)
            and bool(question.strip())
            and len(question) <= 4096,
            "public question identity or text is invalid",
        )
        rows.append({
            "question_id": question_id,
            "object_id": object_id,
            "question": question,
        })
    rows.sort(key=lambda row: row["question_id"])
    _require(
        len({row["question_id"] for row in rows}) == len(rows),
        "question IDs repeat",
    )
    return rows


def _vector_is_valid(vector: Any, dimension: int) -> bool:
    return (
        isinstance(vector, list)
        and len(vector) == dimension
        and any(vector)
        and all(type(value) is int and -32767 <= value <= 32767
                for value in vector)
    )


def _write_immutable(
    target: Path,
    files: Mapping[str, bytes],
    verify: Callable[[Path], Any],
) -> None:
    _require(not target.exists(), f"output directory already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    try:
        for name, payload in files.items():
            (staging / name).write_bytes(payload)
        (staging / CHECKSUMS_NAME).write_bytes(_checksums(staging))
        verify(staging)
        os.replace(staging, target)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def build_video_temporal_index(
    preparation_dir: str | Path,
    caption_dir: str | Path,
    *,
    output_dir: str | Path,
    package_id: str,
    embedding_model_id: str,
    base_url: str,
    api_key: str,
    dimension: int = 1024,
    batch_size: int = 10,
    timeout_seconds: float = 180.0,
    transport: Transport = _default_transport,
) -> dict[str, Any]:
    """Embed each video's frozen captions once, with no question input."""

    target = Path(output_dir).resolve()
    _require(not target.exists(), "video index output already exists")
    _require(bool(package_id) and bool(base_url) and bool(api_key),
             "video index configuration is incomplete")
    prep, captions, source_rows = _source_rows(preparation_dir, caption_dir)
    source_by_object: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in source_rows:
        source_by_object[row["object_id"]].append(row)
    vectors = []
    receipts = []
    for object_id, object_rows in sorted(source_by_object.items()):
        object_vectors, object_receipts = _embedding_batches(
            texts=[row["_text"] for row in object_rows],
            model_id=embedding_model_id,
            dimension=dimension,
            base_url=base_url,
            api_key=api_key,
            batch_size=batch_size,
            timeout_seconds=timeout_seconds,
            transport=transport,
        )
        vectors.extend(object_vectors)
        receipts.extend({"object_id": object_id, **receipt}
                        for receipt in object_receipts)
    rows = [
        {
            **{key: value for key, value in row.items() if key != "_text"},
            "model_id": embedding_model_id,
            "dimension": dimension,
            "vector_policy_id": VECTOR_POLICY_ID,
            "vector": vector,
        }
        for row, vector in zip(source_rows, vectors, strict=True)
    ]
    vector_bytes = _jsonl_bytes(rows)
    caption_cost_sha, _ = _hash_file(Path(caption_dir) / BUILD_COST_NAME)
    manifest = {
        "schema_version": VIDEO_SCHEMA,
        "package_id": package_id,
        "preparation_sha256": prep["preparation_sha256"],
        "caption_package_sha256": captions["package_sha256"],
        "caption_build_cost_sha256": caption_cost_sha,
        "embedding_model_id": embedding_model_id,
        "embedding_dimension": dimension,
        "vector_policy_id": VECTOR_POLICY_ID,
        "object_count": len({row["object_id"] for row in rows}),
        "window_vector_count": len(rows),
        "vectors_sha256": _sha256(vector_bytes),
        "video_build_embedding_input_count": len(rows),
        "video_build_embedding_inputs_by_object": {
            object_id: len(object_rows)
            for object_id, object_rows in sorted(source_by_object.items())
        },
        "video_build_embedding_request_count": len(receipts),
        "video_build_embedding_receipts": receipts,
        "question_independent": True,
        "query_embedding_inputs_charged_here": 0,
        "query_frame_materialization_included": False,
        "monetary_cost_measured": False,
        "task_outcomes_read": False,
        "hidden_label_values_read": False,
        "credentials_recorded": False,
    }
    manifest["package_sha256"] = _sha256(_canonical(manifest))
    _write_immutable(target, {
        VIDEO_VECTORS: vector_bytes,
        VIDEO_MANIFEST: _json_bytes(manifest),
    }, lambda root: verify_video_temporal_index(
        root, preparation_dir, caption_dir
    ))
    return verify_video_temporal_index(target, preparation_dir, caption_dir)


def verify_video_temporal_index(
    video_index_dir: str | Path,
    preparation_dir: str | Path,
    caption_dir: str | Path,
) -> dict[str, Any]:
    root = Path(video_index_dir).resolve()
    _require(root.is_dir(), "video index directory is missing")
    _require(
        {p.name for p in root.iterdir()} ==
        {VIDEO_VECTORS, VIDEO_MANIFEST, CHECKSUMS_NAME},
        "video index file set changed",
    )
    _verify_checksums(root)
    raw = (root / VIDEO_MANIFEST).read_bytes()
    manifest = _read_json(root / VIDEO_MANIFEST, "video index manifest")
    _require(raw == _json_bytes(manifest), "video index manifest is not canonical")
    package_sha = manifest.pop("package_sha256", None)
    _require(package_sha == _sha256(_canonical(manifest)),
             "video index package digest differs")
    manifest["package_sha256"] = package_sha
    prep, captions, source_rows = _source_rows(preparation_dir, caption_dir)
    caption_cost_sha, _ = _hash_file(Path(caption_dir) / BUILD_COST_NAME)
    rows = _read_jsonl(root / VIDEO_VECTORS, "video vectors")
    dimension = manifest.get("embedding_dimension")
    _require(type(dimension) is int and dimension > 0,
             "video embedding dimension is invalid")
    _require(
        manifest.get("schema_version") == VIDEO_SCHEMA
        and manifest.get("preparation_sha256") == prep["preparation_sha256"]
        and manifest.get("caption_package_sha256") == captions["package_sha256"]
        and manifest.get("caption_build_cost_sha256") == caption_cost_sha
        and manifest.get("vector_policy_id") == VECTOR_POLICY_ID
        and manifest.get("vectors_sha256") == _sha256(_jsonl_bytes(rows))
        and manifest.get("window_vector_count") == len(source_rows) == len(rows)
        and manifest.get("video_build_embedding_input_count") == len(rows)
        and manifest.get("video_build_embedding_inputs_by_object") == {
            object_id: sum(r["object_id"] == object_id for r in rows)
            for object_id in sorted({r["object_id"] for r in rows})
        }
        and manifest.get("object_count") == len({r["object_id"] for r in rows})
        and manifest.get("video_build_embedding_request_count") == len(
            manifest.get("video_build_embedding_receipts", [])
        )
        and manifest.get("question_independent") is True
        and manifest.get("query_embedding_inputs_charged_here") == 0
        and manifest.get("query_frame_materialization_included") is False
        and manifest.get("monetary_cost_measured") is False
        and manifest.get("task_outcomes_read") is False
        and manifest.get("hidden_label_values_read") is False
        and manifest.get("credentials_recorded") is False,
        "video index source or accounting binding differs",
    )
    for row, source in zip(rows, source_rows, strict=True):
        expected = {key: value for key, value in source.items()
                    if key != "_text"}
        _require(
            set(row) == set(expected) |
            {"model_id", "dimension", "vector_policy_id", "vector"}
            and all(row.get(key) == value for key, value in expected.items())
            and row.get("model_id") == manifest["embedding_model_id"]
            and row.get("dimension") == dimension
            and row.get("vector_policy_id") == VECTOR_POLICY_ID
            and _vector_is_valid(row.get("vector"), dimension),
            "video vector differs from its caption or embedding contract",
        )
    receipt_objects = [r.get("object_id") for r in
                       manifest["video_build_embedding_receipts"]]
    _require(
        receipt_objects == sorted(receipt_objects)
        and set(receipt_objects) == set(manifest[
            "video_build_embedding_inputs_by_object"
        ])
        and all(type(r.get("input_count")) is int
                and r["input_count"] > 0 for r in
                manifest["video_build_embedding_receipts"])
        and {
            object_id: sum(r["input_count"] for r in
                           manifest["video_build_embedding_receipts"]
                           if r["object_id"] == object_id)
            for object_id in set(receipt_objects)
        } == manifest["video_build_embedding_inputs_by_object"],
        "video build receipts do not account for every object input",
    )
    return {
        "status": "VERIFIED_REUSABLE_VIDEO_TEMPORAL_INDEX",
        "package_sha256": package_sha,
        "object_count": manifest["object_count"],
        "window_vector_count": len(rows),
        "video_build_embedding_input_count": len(rows),
        "video_build_embedding_request_count": manifest[
            "video_build_embedding_request_count"
        ],
        "credentials_recorded": False,
    }


def build_temporal_query_batch(
    video_index_dir: str | Path,
    preparation_dir: str | Path,
    caption_dir: str | Path,
    questions: Sequence[Mapping[str, Any]],
    *,
    output_dir: str | Path,
    package_id: str,
    base_url: str,
    api_key: str,
    batch_size: int = 1,
    timeout_seconds: float = 180.0,
    transport: Transport = _default_transport,
) -> dict[str, Any]:
    """Embed and rank public questions without rebuilding video vectors."""

    target = Path(output_dir).resolve()
    _require(not target.exists(), "query batch output already exists")
    _require(bool(package_id) and bool(base_url) and bool(api_key),
             "query batch configuration is incomplete")
    _require(batch_size == 1,
             "query embedding must use one request per question for accounting")
    video = verify_video_temporal_index(
        video_index_dir, preparation_dir, caption_dir
    )
    manifest = _read_json(Path(video_index_dir) / VIDEO_MANIFEST,
                          "video index manifest")
    source = _public_rows(questions)
    windows = _read_jsonl(Path(preparation_dir) / WINDOWS_NAME, "fine windows")
    by_object: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in _read_jsonl(Path(video_index_dir) / VIDEO_VECTORS,
                           "video vectors"):
        by_object[row["object_id"]].append(row)
    windows_by_object: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
    for row in windows:
        windows_by_object[row["object_id"]][row["ordinal"]] = row
    _require(all(row["object_id"] in by_object for row in source),
             "public question references a video outside the base index")
    anchors = [contextualize_anchor_clause(row["question"]) for row in source]
    vectors = []
    receipts = []
    for question, anchor in zip(source, anchors, strict=True):
        one_vector, one_receipt = _embedding_batches(
            texts=[anchor["contextualized_anchor_text"]],
            model_id=manifest["embedding_model_id"],
            dimension=manifest["embedding_dimension"],
            base_url=base_url,
            api_key=api_key,
            batch_size=1,
            timeout_seconds=timeout_seconds,
            transport=transport,
        )
        vectors.extend(one_vector)
        receipts.append({"question_id": question["question_id"],
                         **one_receipt[0]})
    query_rows = []
    selection_rows = []
    for question, anchor, vector in zip(source, anchors, vectors, strict=True):
        object_id = question["object_id"]
        ranked = rank_segments_semantic(
            question_vector=vector, segment_vectors=by_object[object_id]
        )
        selection = select_v2(
            question=question["question"],
            ranked=ranked,
            windows_by_ordinal=windows_by_object[object_id],
        )
        _require(selection["fallback_used"] is False,
                 "temporal query used a fallback")
        query_rows.append({
            "question_id": question["question_id"],
            "object_id": object_id,
            "question_sha256": anchor["question_sha256"],
            "anchor_sha256": anchor["contextualized_anchor_sha256"],
            "model_id": manifest["embedding_model_id"],
            "dimension": manifest["embedding_dimension"],
            "vector_policy_id": VECTOR_POLICY_ID,
            "vector": vector,
        })
        selection_rows.append({
            "question_id": question["question_id"],
            "object_id": object_id,
            "question_sha256": anchor["question_sha256"],
            "video_index_sha256": video["package_sha256"],
            "ranking": ranked,
            "selection": selection,
        })
    vector_bytes = _jsonl_bytes(query_rows)
    selection_bytes = _jsonl_bytes(selection_rows)
    package = {
        "schema_version": QUERY_SCHEMA,
        "package_id": package_id,
        "video_index_sha256": video["package_sha256"],
        "public_questions_sha256": _sha256(_canonical(source)),
        "embedding_model_id": manifest["embedding_model_id"],
        "embedding_dimension": manifest["embedding_dimension"],
        "vector_policy_id": VECTOR_POLICY_ID,
        "question_count": len(source),
        "object_count": len({row["object_id"] for row in source}),
        "query_embedding_input_count": len(source),
        "query_embedding_request_count": len(receipts),
        "query_embedding_receipts": receipts,
        "query_vectors_sha256": _sha256(vector_bytes),
        "selections_sha256": _sha256(selection_bytes),
        "video_build_embedding_inputs_charged_here": 0,
        "query_frame_materialization_included": False,
        "monetary_cost_measured": False,
        "task_outcomes_read": False,
        "hidden_label_values_read": False,
        "credentials_recorded": False,
    }
    package["package_sha256"] = _sha256(_canonical(package))
    _write_immutable(target, {
        QUERY_VECTORS: vector_bytes,
        QUERY_SELECTIONS: selection_bytes,
        QUERY_MANIFEST: _json_bytes(package),
    }, lambda root: verify_temporal_query_batch(
        root, video_index_dir, preparation_dir, caption_dir, questions
    ))
    return verify_temporal_query_batch(
        target, video_index_dir, preparation_dir, caption_dir, questions
    )


def verify_temporal_query_batch(
    query_dir: str | Path,
    video_index_dir: str | Path,
    preparation_dir: str | Path,
    caption_dir: str | Path,
    questions: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    root = Path(query_dir).resolve()
    _require(root.is_dir(), "query batch directory is missing")
    _require(
        {p.name for p in root.iterdir()} ==
        {QUERY_MANIFEST, QUERY_VECTORS, QUERY_SELECTIONS, CHECKSUMS_NAME},
        "query batch file set changed",
    )
    _verify_checksums(root)
    raw = (root / QUERY_MANIFEST).read_bytes()
    package = _read_json(root / QUERY_MANIFEST, "query batch manifest")
    _require(raw == _json_bytes(package), "query batch manifest is not canonical")
    package_sha = package.pop("package_sha256", None)
    _require(package_sha == _sha256(_canonical(package)),
             "query batch package digest differs")
    package["package_sha256"] = package_sha
    video = verify_video_temporal_index(
        video_index_dir, preparation_dir, caption_dir
    )
    video_manifest = _read_json(Path(video_index_dir) / VIDEO_MANIFEST,
                                "video index manifest")
    source = _public_rows(questions)
    query_rows = _read_jsonl(root / QUERY_VECTORS, "query vectors")
    selection_rows = _read_jsonl(root / QUERY_SELECTIONS, "query selections")
    _require(
        package.get("schema_version") == QUERY_SCHEMA
        and package.get("video_index_sha256") == video["package_sha256"]
        and package.get("public_questions_sha256") == _sha256(_canonical(source))
        and package.get("embedding_model_id") == video_manifest["embedding_model_id"]
        and package.get("embedding_dimension") ==
        video_manifest["embedding_dimension"]
        and package.get("vector_policy_id") == VECTOR_POLICY_ID
        and package.get("question_count") == len(source) == len(query_rows) ==
        len(selection_rows)
        and package.get("object_count") == len({r["object_id"] for r in source})
        and package.get("query_embedding_input_count") == len(source)
        and package.get("query_embedding_request_count") == len(
            package.get("query_embedding_receipts", [])
        )
        and package.get("query_vectors_sha256") == _sha256(_jsonl_bytes(query_rows))
        and package.get("selections_sha256") == _sha256(_jsonl_bytes(selection_rows))
        and package.get("video_build_embedding_inputs_charged_here") == 0
        and package.get("query_frame_materialization_included") is False
        and package.get("monetary_cost_measured") is False
        and package.get("task_outcomes_read") is False
        and package.get("hidden_label_values_read") is False
        and package.get("credentials_recorded") is False,
        "query batch source or accounting binding differs",
    )
    by_object: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in _read_jsonl(Path(video_index_dir) / VIDEO_VECTORS,
                           "video vectors"):
        by_object[row["object_id"]].append(row)
    windows_by_object: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
    for row in _read_jsonl(Path(preparation_dir) / WINDOWS_NAME, "fine windows"):
        windows_by_object[row["object_id"]][row["ordinal"]] = row
    dimension = package["embedding_dimension"]
    for question, query, selection in zip(
        source, query_rows, selection_rows, strict=True
    ):
        anchor = contextualize_anchor_clause(question["question"])
        _require(
            set(query) == {"question_id", "object_id", "question_sha256",
                           "anchor_sha256", "model_id", "dimension",
                           "vector_policy_id", "vector"}
            and query["question_id"] == question["question_id"]
            and query["object_id"] == question["object_id"]
            and query["question_sha256"] == anchor["question_sha256"]
            and query["anchor_sha256"] == anchor["contextualized_anchor_sha256"]
            and query["model_id"] == package["embedding_model_id"]
            and query["dimension"] == dimension
            and query["vector_policy_id"] == VECTOR_POLICY_ID
            and _vector_is_valid(query.get("vector"), dimension),
            "query vector differs from its public question",
        )
        ranked = rank_segments_semantic(
            question_vector=query["vector"],
            segment_vectors=by_object[question["object_id"]],
        )
        expected = {
            "question_id": question["question_id"],
            "object_id": question["object_id"],
            "question_sha256": anchor["question_sha256"],
            "video_index_sha256": video["package_sha256"],
            "ranking": ranked,
            "selection": select_v2(
                question=question["question"],
                ranked=ranked,
                windows_by_ordinal=windows_by_object[question["object_id"]],
            ),
        }
        _require(selection == expected and selection["selection"]["fallback_used"]
                 is False, "query selection does not rebuild from frozen inputs")
    receipts = package["query_embedding_receipts"]
    _require(
        [r.get("question_id") for r in receipts] ==
        [row["question_id"] for row in source]
        and all(r.get("input_count") == 1 for r in receipts),
        "query embedding receipts do not bind one request per question",
    )
    return {
        "status": "VERIFIED_MULTI_QUESTION_TEMPORAL_SELECTIONS",
        "package_sha256": package_sha,
        "video_index_sha256": video["package_sha256"],
        "question_count": len(source),
        "object_count": package["object_count"],
        "query_embedding_input_count": len(source),
        "video_build_embedding_inputs_charged_here": 0,
        "credentials_recorded": False,
    }


__all__ = [
    "QUERY_MANIFEST",
    "QUERY_SCHEMA",
    "QUERY_SELECTIONS",
    "QUERY_VECTORS",
    "VIDEO_MANIFEST",
    "VIDEO_SCHEMA",
    "VIDEO_VECTORS",
    "build_temporal_query_batch",
    "build_video_temporal_index",
    "verify_temporal_query_batch",
    "verify_video_temporal_index",
]
