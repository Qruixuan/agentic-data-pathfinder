"""Frozen text embeddings for query-aware temporal segment retrieval.

Embeddings are generated once, offline, and frozen.  Runtime performs only
deterministic integer arithmetic over the frozen vectors, so a live experiment
or an offline replay never needs an embedding endpoint, credentials or network
access.

Determinism is the point of the representation choice here.  Float cosine
similarity is platform sensitive, so every vector is L2-normalized once at
freeze time and quantized to fixed-scale integers.  Ranking is then an exact
integer dot product, which is byte-identical on Windows and Linux.

No hidden label, oracle answer, task outcome, model prediction or credential is
accepted, embedded, or recorded.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

EMBEDDING_PACKAGE_SCHEMA_VERSION = (
    "pathfinder.full-flow-temporal-embedding-package/v1alpha1"
)
# One declared, fixed normalization + quantization policy.  Changing it must
# change this identifier so frozen packages can never be silently reinterpreted.
VECTOR_POLICY_ID = "l2-normalized-int16-symmetric-scale32767-v1"
QUANT_SCALE = 32767

PACKAGE_NAME = "temporal-embedding-package.json"
VECTORS_NAME = "temporal-embedding-vectors.jsonl"
CHECKSUMS_NAME = "SHA256SUMS"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
# Field names that must never appear in an embedding package.
_FORBIDDEN_KEYS = frozenset({
    "correct_answer_id", "hidden_label", "hidden_labels", "answer",
    "task_success", "predicted_answer", "prediction", "score",
    "api_key", "authorization", "bearer_token", "token", "secret",
    "password", "signed_url", "credential_value",
})


class TemporalEmbeddingError(ValueError):
    """Raised before an unbound or non-deterministic embedding can be used."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise TemporalEmbeddingError(message)


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _digest(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{name} is not lowercase SHA-256",
    )
    return str(value)


def _assert_no_forbidden_fields(value: Any, path: str = "package") -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            _require(
                str(key).casefold().replace("-", "_") not in _FORBIDDEN_KEYS,
                f"forbidden field entered the embedding package at {path}.{key}",
            )
            _assert_no_forbidden_fields(nested, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, nested in enumerate(value):
            _assert_no_forbidden_fields(nested, f"{path}[{index}]")


def normalize_and_quantize(vector: Sequence[float], *, dimension: int) -> tuple[int, ...]:
    """L2-normalize once, then quantize with the single declared scale."""

    _require(
        type(dimension) is int and dimension > 0,
        "embedding dimension must be a positive integer",
    )
    _require(
        isinstance(vector, (list, tuple)) and len(vector) == dimension,
        f"embedding vector must have exactly {dimension} components",
    )
    values: list[float] = []
    for component in vector:
        _require(
            not isinstance(component, bool)
            and isinstance(component, (int, float))
            and math.isfinite(float(component)),
            "embedding vector contains a non-finite component",
        )
        values.append(float(component))
    norm = math.sqrt(sum(value * value for value in values))
    _require(norm > 0.0, "embedding vector has zero norm")
    # round-half-away-from-zero keeps quantization symmetric and independent of
    # the host's banker's-rounding behaviour.
    quantized = []
    for value in values:
        scaled = (value / norm) * QUANT_SCALE
        quantized.append(int(math.floor(scaled + 0.5)) if scaled >= 0 else -int(math.floor(-scaled + 0.5)))
    _require(any(quantized), "quantized embedding vector is all zero")
    return tuple(quantized)


def integer_similarity(left: Sequence[int], right: Sequence[int]) -> int:
    """Exact integer dot product; identical on every platform."""

    _require(len(left) == len(right), "similarity needs equal-length vectors")
    total = 0
    for a, b in zip(left, right):
        _require(type(a) is int and type(b) is int, "vectors must be integers")
        total += a * b
    return total


def build_embedding_package(
    *,
    package_id: str,
    model_id: str,
    dimension: int,
    caption_records: Sequence[Mapping[str, Any]],
    question_records: Sequence[Mapping[str, Any]],
    source_bindings: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze caption and question vectors into an immutable package.

    Each record supplies ``text`` plus its identity; the text itself is never
    stored, only its digest, so a package cannot leak content it embedded.
    """

    _require(bool(package_id), "package_id is required")
    _require(bool(model_id), "model_id is required")
    _require(bool(caption_records), "at least one caption record is required")
    _require(bool(question_records), "at least one question record is required")

    rows: list[dict[str, Any]] = []
    for kind, records in (("segment_caption", caption_records), ("public_question", question_records)):
        for record in records:
            # Refuse rather than silently drop: a caller that supplies a label
            # or credential must fail loudly, not have it quietly filtered.
            _assert_no_forbidden_fields(record, f"{kind}_record")
            text = record.get("text")
            _require(isinstance(text, str) and bool(text.strip()), "record text is empty")
            vector = normalize_and_quantize(record["vector"], dimension=dimension)
            row = {
                "kind": kind,
                "object_id": str(record["object_id"]),
                "input_sha256": _sha256(text.encode("utf-8")),
                "input_character_count": len(text),
                "model_id": model_id,
                "dimension": dimension,
                "vector_policy_id": VECTOR_POLICY_ID,
                "vector": list(vector),
                "vector_sha256": _sha256(_canonical(list(vector))),
                "request_sha256": _digest(record["request_sha256"], "request_sha256"),
                "response_sha256": _digest(record["response_sha256"], "response_sha256"),
            }
            if kind == "segment_caption":
                row["segment_id"] = str(record["segment_id"])
                row["segment_ordinal"] = int(record["segment_ordinal"])
            else:
                row["task_binding_sha256"] = _digest(
                    record["task_binding_sha256"], "task_binding_sha256"
                )
            rows.append(row)

    rows.sort(key=lambda r: (r["kind"], r["object_id"], r.get("segment_ordinal", -1)))
    vector_bytes = b"".join(_canonical(row) + b"\n" for row in rows)

    package = {
        "schema_version": EMBEDDING_PACKAGE_SCHEMA_VERSION,
        "package_id": package_id,
        "model_id": model_id,
        "dimension": dimension,
        "vector_policy_id": VECTOR_POLICY_ID,
        "quantization_scale": QUANT_SCALE,
        "similarity": "integer-dot-product-of-quantized-unit-vectors",
        "segment_caption_count": sum(1 for r in rows if r["kind"] == "segment_caption"),
        "public_question_count": sum(1 for r in rows if r["kind"] == "public_question"),
        "object_ids": sorted({r["object_id"] for r in rows}),
        "vectors_sha256": _sha256(vector_bytes),
        "source_bindings": json.loads(_canonical(source_bindings)),
        "runtime_embedding_calls_required": False,
        "credentials_recorded": False,
        "hidden_label_values_included": False,
        "task_outcomes_included": False,
        "eligible_for_scientific_claims": False,
        "status": "FROZEN_TEMPORAL_EMBEDDINGS",
    }
    package["package_sha256"] = _sha256(_canonical(package))
    _assert_no_forbidden_fields(package)
    _assert_no_forbidden_fields(rows)

    target = Path(output_dir).resolve()
    _require(not target.exists(), f"output directory already exists: {target}")
    target.mkdir(parents=True)
    (target / PACKAGE_NAME).write_bytes(_canonical(package))
    (target / VECTORS_NAME).write_bytes(vector_bytes)
    manifest = "\n".join(
        f"{_sha256((target / name).read_bytes())}  {name}"
        for name in sorted((PACKAGE_NAME, VECTORS_NAME))
    )
    (target / CHECKSUMS_NAME).write_bytes((manifest + "\n").encode("utf-8"))
    return package


def load_embedding_package(package_dir: str | Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Load and fully re-verify a frozen embedding package."""

    root = Path(package_dir).resolve()
    manifest = (root / CHECKSUMS_NAME).read_text(encoding="utf-8")
    seen = 0
    for line in manifest.splitlines():
        if not line.strip():
            continue
        expected, _, name = line.partition("  ")
        actual = _sha256((root / name.strip()).read_bytes())
        _require(actual == expected.strip(), f"checksum mismatch: {name.strip()}")
        seen += 1
    _require(seen == 2, "embedding package manifest is incomplete")

    package = json.loads((root / PACKAGE_NAME).read_text(encoding="utf-8"))
    _require(
        package.get("schema_version") == EMBEDDING_PACKAGE_SCHEMA_VERSION,
        "embedding package schema changed",
    )
    _require(
        package.get("vector_policy_id") == VECTOR_POLICY_ID
        and package.get("quantization_scale") == QUANT_SCALE,
        "embedding package uses a different vector policy",
    )
    _require(
        package.get("runtime_embedding_calls_required") is False,
        "frozen package must not require a runtime embedding call",
    )
    supplied = dict(package)
    recorded = supplied.pop("package_sha256", None)
    _require(
        _sha256(_canonical(supplied)) == recorded,
        "embedding package digest changed",
    )
    vector_bytes = (root / VECTORS_NAME).read_bytes()
    _require(
        _sha256(vector_bytes) == package["vectors_sha256"],
        "embedding vectors differ from their frozen digest",
    )
    rows = [
        json.loads(line)
        for line in vector_bytes.decode("utf-8").splitlines()
        if line.strip()
    ]
    dimension = package["dimension"]
    for row in rows:
        _require(row["model_id"] == package["model_id"], "row binds a different model")
        _require(row["dimension"] == dimension, "row has the wrong dimension")
        _require(len(row["vector"]) == dimension, "row vector length is wrong")
        _require(
            _sha256(_canonical(row["vector"])) == row["vector_sha256"],
            "row vector digest changed",
        )
        _require(any(row["vector"]), "row vector is all zero")
    _assert_no_forbidden_fields(rows)
    return package, rows


def rank_segments_semantic(
    *,
    question_vector: Sequence[int],
    segment_vectors: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Deterministically rank segments by integer similarity.

    Ties are resolved by the lowest segment ordinal, never by chance ordering.
    """

    _require(bool(segment_vectors), "cannot rank an empty segment set")
    scored = [
        {
            "segment_id": row["segment_id"],
            "ordinal": int(row["segment_ordinal"]),
            "similarity_score_units": integer_similarity(question_vector, row["vector"]),
        }
        for row in segment_vectors
    ]
    return sorted(scored, key=lambda r: (-r["similarity_score_units"], r["ordinal"]))


def anchor_confidence(ranked: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Report the rank-1/rank-2 margin and whether the anchor is decisive."""

    _require(bool(ranked), "cannot assess an empty ranking")
    top = int(ranked[0]["similarity_score_units"])
    runner = int(ranked[1]["similarity_score_units"]) if len(ranked) > 1 else None
    margin = None if runner is None else top - runner
    return {
        "top_score_units": top,
        "runner_up_score_units": runner,
        "score_margin_units": margin,
        # A margin of exactly zero means the content did not separate the
        # candidates; that must surface as low confidence, never as a silent
        # ordinal-zero pick.
        "semantic_anchor_decisive": bool(margin is not None and margin > 0),
    }


__all__ = [
    "CHECKSUMS_NAME",
    "EMBEDDING_PACKAGE_SCHEMA_VERSION",
    "PACKAGE_NAME",
    "QUANT_SCALE",
    "VECTORS_NAME",
    "VECTOR_POLICY_ID",
    "TemporalEmbeddingError",
    "anchor_confidence",
    "build_embedding_package",
    "integer_similarity",
    "load_embedding_package",
    "normalize_and_quantize",
    "rank_segments_semantic",
]
