"""Deterministic W4 retrieval cohort, lexical index, and quality evaluation.

The implementation is deliberately an offline development instrument.  It
requires explicit relevance annotations, verifies every digest against the
frozen representation manifest, and never asks an LLM to invent ground truth.
The bundled BM25 index gives the simulator a real, reproducible retrieval
path while remaining clearly distinct from a future multimodal ANN index.
"""

from __future__ import annotations

import json
import math
import os
import re
import shutil
import tempfile
from collections import Counter
from hashlib import sha256
from pathlib import Path, PurePosixPath
from statistics import mean
from typing import Any, Iterable, Mapping


RETRIEVAL_CONFIG_SCHEMA_VERSION = (
    "pathfinder.simulator-retrieval-config/v1alpha1"
)
RETRIEVAL_COHORT_SCHEMA_VERSION = (
    "pathfinder.simulator-retrieval-cohort/v1alpha1"
)
RETRIEVAL_INDEX_SCHEMA_VERSION = (
    "pathfinder.simulator-lexical-index/v1alpha1"
)
RETRIEVAL_RANKING_SCHEMA_VERSION = (
    "pathfinder.simulator-retrieval-ranking/v1alpha1"
)
RETRIEVAL_EVALUATION_SCHEMA_VERSION = (
    "pathfinder.simulator-retrieval-evaluation/v1alpha1"
)
RETRIEVAL_ANSWER_SCHEMA_VERSION = (
    "pathfinder.simulator-retrieval-answer-observations/v1alpha1"
)
RETRIEVAL_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.simulator-retrieval-run/v1alpha1"
)

_TOKEN = re.compile(r"[a-z0-9]+")
_SPLITS = ("train", "validation", "test")
_ANNOTATION_STATES = (
    "operator-verified",
    "ai-drafted-requires-operator-verification",
)


class SimulatorRetrievalError(ValueError):
    """Raised when retrieval inputs are ambiguous, unsafe, or unbound."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SimulatorRetrievalError(message)


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _invalid_number(value: str) -> None:
    raise SimulatorRetrievalError(f"non-finite JSON number: {value}")


def _read_json(path: Path, name: str) -> tuple[bytes, Any]:
    try:
        raw = path.read_bytes()
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_keys,
            parse_constant=_invalid_number,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SimulatorRetrievalError(
            f"cannot read valid {name}: {path}"
        ) from exc
    return raw, value


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    _require(isinstance(value, Mapping), f"{name} must be an object")
    return value


def _array(value: Any, name: str) -> list[Any]:
    _require(isinstance(value, list), f"{name} must be an array")
    return value


def _text(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and bool(value.strip()),
        f"{name} must be a non-empty string",
    )
    return value.strip()


def _positive_number(value: Any, name: str) -> float:
    _require(
        type(value) in (int, float)
        and math.isfinite(float(value))
        and float(value) > 0.0,
        f"{name} must be a finite positive number",
    )
    return float(value)


def _positive_integer(value: Any, name: str) -> int:
    _require(type(value) is int and value > 0, f"{name} must be positive")
    return value


def _digest(value: Any, name: str) -> str:
    text = _text(value, name)
    _require(
        len(text) == 64
        and all(character in "0123456789abcdef" for character in text),
        f"{name} must be a lowercase SHA-256 digest",
    )
    return text


def _sha256_bytes(value: bytes) -> str:
    return sha256(value).hexdigest()


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _jsonl_bytes(values: Iterable[Mapping[str, Any]]) -> bytes:
    return "".join(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
        for value in values
    ).encode("utf-8")


def _resolve_beneath(root: Path, relative: str, name: str) -> Path:
    candidate = PurePosixPath(relative)
    _require(
        not candidate.is_absolute() and ".." not in candidate.parts,
        f"{name} must be a contained relative path",
    )
    base = root.resolve()
    resolved = base.joinpath(*candidate.parts).resolve()
    _require(
        resolved == base or base in resolved.parents,
        f"{name} escapes its evidence root",
    )
    return resolved


def _tokenize(value: str) -> list[str]:
    return _TOKEN.findall(value.casefold())


def _load_documents(
    manifest_path: Path,
) -> tuple[bytes, list[dict[str, Any]]]:
    raw, value = _read_json(manifest_path, "representation manifest")
    root = _mapping(value, "representation manifest")
    _require(
        root.get("credentials_recorded") is False,
        "representation manifest must record credentials_recorded=false",
    )
    documents: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, item in enumerate(_array(root.get("objects"), "objects")):
        obj = _mapping(item, f"objects[{index}]")
        object_id = _text(obj.get("object_id"), f"objects[{index}].object_id")
        _require(object_id not in seen, f"duplicate object_id: {object_id}")
        seen.add(object_id)
        representations = _mapping(
            obj.get("representations"),
            f"objects[{index}].representations",
        )
        digest = _mapping(
            representations.get("multimodal_digest"),
            f"{object_id}.multimodal_digest",
        )
        path = _resolve_beneath(
            manifest_path.parent,
            _text(digest.get("path"), f"{object_id}.digest.path"),
            f"{object_id}.digest.path",
        )
        try:
            content = path.read_bytes()
            text = content.decode("utf-8")
        except (OSError, UnicodeError) as exc:
            raise SimulatorRetrievalError(
                f"cannot read UTF-8 digest for {object_id}"
            ) from exc
        size = _positive_integer(
            digest.get("size_bytes"),
            f"{object_id}.digest.size_bytes",
        )
        _require(len(content) == size, f"digest size mismatch: {object_id}")
        expected = _digest(digest.get("sha256"), f"{object_id}.digest.sha256")
        _require(
            _sha256_bytes(content) == expected,
            f"digest checksum mismatch: {object_id}",
        )
        tokens = _tokenize(text)
        _require(bool(tokens), f"digest has no indexable tokens: {object_id}")
        documents.append({
            "object_id": object_id,
            "digest_sha256": expected,
            "digest_size_bytes": size,
            "tokens": tokens,
        })
    _require(len(documents) >= 2, "retrieval corpus requires at least two objects")
    return raw, sorted(documents, key=lambda item: item["object_id"])


def _load_config(
    path: Path,
    candidate_ids: tuple[str, ...],
) -> tuple[bytes, Mapping[str, Any], list[dict[str, Any]]]:
    raw, value = _read_json(path, "retrieval config")
    root = _mapping(value, "retrieval config")
    _require(
        root.get("schema_version") == RETRIEVAL_CONFIG_SCHEMA_VERSION,
        "unsupported retrieval config schema_version",
    )
    retrieval_id = _text(root.get("retrieval_id"), "retrieval_id")
    _require(
        root.get("candidate_corpus")
        == "all-representation-manifest-objects",
        "candidate_corpus must bind all representation-manifest objects",
    )
    annotation_status = _text(
        root.get("annotation_status"),
        "annotation_status",
    )
    _require(
        annotation_status in _ANNOTATION_STATES,
        "annotation_status is unsupported",
    )
    _require(
        root.get("independent_unit") == "source-object-group",
        "independent_unit must be source-object-group",
    )
    index = _mapping(root.get("index"), "index")
    _require(index.get("kind") == "bm25-lexical-v1", "unsupported index kind")
    _positive_number(index.get("k1"), "index.k1")
    b = _positive_number(index.get("b"), "index.b")
    _require(b <= 1.0, "index.b must be at most 1")
    top_k = _array(index.get("top_k"), "index.top_k")
    _require(bool(top_k), "index.top_k must not be empty")
    top_values = [_positive_integer(value, "index.top_k value") for value in top_k]
    _require(
        top_values == sorted(set(top_values)),
        "index.top_k must be sorted and unique",
    )
    _require(
        max(top_values) <= len(candidate_ids),
        "index.top_k exceeds the candidate corpus",
    )

    queries: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    group_splits: dict[str, str] = {}
    candidates = set(candidate_ids)
    for index_value, item in enumerate(_array(root.get("queries"), "queries")):
        query = _mapping(item, f"queries[{index_value}]")
        query_id = _text(query.get("query_id"), f"queries[{index_value}].query_id")
        _require(query_id not in seen_ids, f"duplicate query_id: {query_id}")
        seen_ids.add(query_id)
        query_text = _text(
            query.get("query_text"),
            f"queries[{index_value}].query_text",
        )
        split = _text(query.get("split"), f"queries[{index_value}].split")
        _require(split in _SPLITS, f"query {query_id} has unsupported split")
        group = _text(
            query.get("source_object_group"),
            f"queries[{index_value}].source_object_group",
        )
        previous = group_splits.setdefault(group, split)
        _require(
            previous == split,
            f"source object group {group!r} leaks across splits",
        )
        raw_relevant = _array(
            query.get("relevant_object_ids"),
            f"queries[{index_value}].relevant_object_ids",
        )
        relevant = tuple(_text(item, "relevant object ID") for item in raw_relevant)
        _require(bool(relevant), f"query {query_id} has no relevant objects")
        _require(
            len(relevant) == len(set(relevant)),
            f"query {query_id} repeats a relevant object",
        )
        missing = sorted(set(relevant) - candidates)
        _require(not missing, f"query {query_id} names unknown relevant objects")
        lowered = query_text.casefold()
        for object_id in candidate_ids:
            suffix = object_id.rsplit("-", 1)[-1]
            _require(
                object_id.casefold() not in lowered
                and not (suffix.isdecimal() and suffix in lowered),
                f"query {query_id} leaks a candidate object identifier",
            )
        queries.append({
            "query_id": query_id,
            "query_text": query_text,
            "split": split,
            "source_object_group": group,
            "relevant_object_ids": list(relevant),
        })
    _require(bool(queries), "queries must not be empty")
    _require(
        set(_SPLITS).issubset({query["split"] for query in queries}),
        "queries must include train, validation, and test splits",
    )
    _require(
        retrieval_id not in seen_ids,
        "retrieval_id must not duplicate a query_id",
    )
    return raw, root, sorted(queries, key=lambda item: item["query_id"])


def _build_index(
    retrieval_id: str,
    documents: list[dict[str, Any]],
    *,
    k1: float,
    b: float,
) -> dict[str, Any]:
    term_frequencies: dict[str, dict[str, int]] = {}
    document_lengths: dict[str, int] = {}
    document_frequency: Counter[str] = Counter()
    source_sha: dict[str, str] = {}
    source_bytes: dict[str, int] = {}
    for document in documents:
        object_id = document["object_id"]
        frequencies = Counter(document["tokens"])
        term_frequencies[object_id] = dict(sorted(frequencies.items()))
        document_lengths[object_id] = len(document["tokens"])
        document_frequency.update(frequencies)
        source_sha[object_id] = document["digest_sha256"]
        source_bytes[object_id] = document["digest_size_bytes"]
    return {
        "schema_version": RETRIEVAL_INDEX_SCHEMA_VERSION,
        "retrieval_id": retrieval_id,
        "index_kind": "bm25-lexical-v1",
        "parameters": {"b": b, "k1": k1},
        "document_count": len(documents),
        "average_document_length": mean(document_lengths.values()),
        "document_lengths": document_lengths,
        "document_frequency": dict(sorted(document_frequency.items())),
        "term_frequencies": term_frequencies,
        "source_digest_sha256": source_sha,
        "source_digest_size_bytes": source_bytes,
        "llm_called": False,
        "credentials_recorded": False,
    }


def _rank(index: Mapping[str, Any], query_text: str) -> list[dict[str, Any]]:
    query_terms = Counter(_tokenize(query_text))
    _require(bool(query_terms), "retrieval query has no indexable tokens")
    document_count = int(index["document_count"])
    average_length = float(index["average_document_length"])
    parameters = _mapping(index["parameters"], "index.parameters")
    k1 = float(parameters["k1"])
    b = float(parameters["b"])
    frequencies = _mapping(index["term_frequencies"], "term frequencies")
    doc_frequency = _mapping(index["document_frequency"], "document frequency")
    lengths = _mapping(index["document_lengths"], "document lengths")
    scores: list[tuple[str, float]] = []
    for object_id, raw_counts in frequencies.items():
        counts = _mapping(raw_counts, f"term frequencies for {object_id}")
        length = float(lengths[object_id])
        score = 0.0
        for term, query_count in query_terms.items():
            frequency = int(counts.get(term, 0))
            if frequency == 0:
                continue
            df = int(doc_frequency[term])
            inverse = math.log(1.0 + (document_count - df + 0.5) / (df + 0.5))
            denominator = frequency + k1 * (
                1.0 - b + b * length / average_length
            )
            score += query_count * inverse * frequency * (k1 + 1.0) / denominator
        scores.append((str(object_id), score))
    scores.sort(key=lambda item: (-item[1], item[0]))
    return [
        {"rank": rank, "object_id": object_id, "score": score}
        for rank, (object_id, score) in enumerate(scores, start=1)
    ]


def _load_answer_observations(
    path: Path,
    *,
    retrieval_id: str,
    rankings: Mapping[str, list[dict[str, Any]]],
) -> tuple[bytes, dict[str, dict[str, Any]]]:
    raw, value = _read_json(path, "answer observations")
    root = _mapping(value, "answer observations")
    _require(
        root.get("schema_version") == RETRIEVAL_ANSWER_SCHEMA_VERSION,
        "unsupported answer-observation schema_version",
    )
    _require(root.get("retrieval_id") == retrieval_id, "retrieval_id mismatch")
    _require(
        root.get("credentials_recorded") is False,
        "answer observations must record credentials_recorded=false",
    )
    result: dict[str, dict[str, Any]] = {}
    for index, item in enumerate(_array(root.get("observations"), "observations")):
        observation = _mapping(item, f"observations[{index}]")
        query_id = _text(observation.get("query_id"), "observation.query_id")
        _require(query_id in rankings, f"unknown answer query_id: {query_id}")
        _require(query_id not in result, f"duplicate answer query_id: {query_id}")
        top_k = _positive_integer(observation.get("top_k"), "observation.top_k")
        expected = [row["object_id"] for row in rankings[query_id][:top_k]]
        observed = _array(
            observation.get("retrieved_object_ids"),
            "observation.retrieved_object_ids",
        )
        _require(observed == expected, f"answer ranking mismatch: {query_id}")
        _require(
            observation.get("outcome_type") == "completed",
            f"answer outcome is not completed: {query_id}",
        )
        _require(
            observation.get("telemetry_complete") is True,
            f"answer telemetry is incomplete: {query_id}",
        )
        _require(
            type(observation.get("answer_correct")) is bool,
            f"answer_correct must be literal boolean: {query_id}",
        )
        result[query_id] = dict(observation)
    return raw, result


def _evaluation(
    retrieval_id: str,
    queries: list[dict[str, Any]],
    rankings: Mapping[str, list[dict[str, Any]]],
    top_k_values: list[int],
    answers: Mapping[str, Mapping[str, Any]],
    *,
    annotation_status: str,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for query in queries:
        query_id = query["query_id"]
        ranking = rankings[query_id]
        relevant = set(query["relevant_object_ids"])
        reciprocal_rank = 0.0
        for row in ranking:
            if row["object_id"] in relevant:
                reciprocal_rank = 1.0 / row["rank"]
                break
        metrics: dict[str, Any] = {"reciprocal_rank": reciprocal_rank}
        for top_k in top_k_values:
            selected = {row["object_id"] for row in ranking[:top_k]}
            matched = len(selected & relevant)
            metrics[f"recall_at_{top_k}"] = matched / len(relevant)
            metrics[f"hit_at_{top_k}"] = matched > 0
        answer = answers.get(query_id)
        answer_correct = None if answer is None else answer["answer_correct"]
        answer_top_k = None if answer is None else answer["top_k"]
        joint = None
        if answer is not None:
            selected = {
                row["object_id"] for row in ranking[: int(answer_top_k)]
            }
            joint = bool(answer_correct and selected & relevant)
        rows.append({
            "query_id": query_id,
            "split": query["split"],
            "source_object_group": query["source_object_group"],
            "relevant_object_count": len(relevant),
            **metrics,
            "answer_observed": answer is not None,
            "answer_top_k": answer_top_k,
            "answer_correct": answer_correct,
            "joint_success": joint,
        })

    def aggregate(group: list[dict[str, Any]], scope: str) -> dict[str, Any]:
        answered = [row for row in group if row["answer_observed"]]
        payload: dict[str, Any] = {
            "scope": scope,
            "query_count": len(group),
            "independent_source_object_groups": len({
                row["source_object_group"] for row in group
            }),
            "mrr": mean(row["reciprocal_rank"] for row in group),
            "answer_observation_count": len(answered),
            "answer_accuracy": (
                mean(bool(row["answer_correct"]) for row in answered)
                if answered
                else None
            ),
            "joint_success_rate": (
                mean(bool(row["joint_success"]) for row in answered)
                if answered
                else None
            ),
        }
        for top_k in top_k_values:
            payload[f"recall_at_{top_k}"] = mean(
                row[f"recall_at_{top_k}"] for row in group
            )
            payload[f"hit_at_{top_k}"] = mean(
                bool(row[f"hit_at_{top_k}"]) for row in group
            )
        return payload

    aggregates = [aggregate(rows, "overall")]
    for split in _SPLITS:
        group = [row for row in rows if row["split"] == split]
        aggregates.append(aggregate(group, f"split:{split}"))
    return {
        "schema_version": RETRIEVAL_EVALUATION_SCHEMA_VERSION,
        "status": "COMPLETE",
        "retrieval_id": retrieval_id,
        "index_kind": "bm25-lexical-v1",
        "annotation_status": annotation_status,
        "query_metrics": rows,
        "aggregates": aggregates,
        "answer_metrics_are_null_without_bound_observations": True,
        "posthoc": annotation_status != "operator-verified",
        "simulated": False,
        "llm_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def _verify_output(root: Path) -> dict[str, Any]:
    expected = {
        "retrieval_cohort.json",
        "lexical_index.json",
        "rankings.jsonl",
        "retrieval_evaluation.json",
        "retrieval_manifest.json",
    }
    actual = {path.name for path in root.iterdir() if path.is_file()}
    _require(actual == expected | {"SHA256SUMS"}, "retrieval output set changed")
    found: dict[str, str] = {}
    for line in (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
        digest, separator, name = line.partition("  ")
        _require(separator == "  " and name in expected, "malformed SHA256SUMS")
        _require(name not in found, f"duplicate checksum: {name}")
        _require(
            _sha256_bytes((root / name).read_bytes()) == digest,
            f"retrieval checksum mismatch: {name}",
        )
        found[name] = digest
    _require(set(found) == expected, "retrieval checksums are incomplete")
    _, value = _read_json(root / "retrieval_manifest.json", "retrieval manifest")
    manifest = _mapping(value, "retrieval manifest")
    _require(
        manifest.get("schema_version") == RETRIEVAL_MANIFEST_SCHEMA_VERSION,
        "unsupported retrieval manifest schema_version",
    )
    _require(manifest.get("status") == "COMPLETE", "retrieval run incomplete")
    _require(
        manifest.get("output_sha256")
        == {
            name: digest
            for name, digest in found.items()
            if name != "retrieval_manifest.json"
        },
        "retrieval manifest digests disagree",
    )
    return dict(manifest)


def build_simulator_retrieval_cohort(
    config_path: str | Path,
    representation_manifest_path: str | Path,
    *,
    output_dir: str | Path,
    answer_observations_path: str | Path | None = None,
) -> dict[str, Any]:
    """Build an evidence-bound W4 cohort, BM25 index, and evaluation."""
    config_source = Path(config_path).resolve()
    representation_source = Path(representation_manifest_path).resolve()
    target = Path(output_dir).resolve()
    _require(not target.exists(), f"retrieval output already exists: {target}")
    representation_raw, documents = _load_documents(representation_source)
    candidate_ids = tuple(document["object_id"] for document in documents)
    config_raw, config, queries = _load_config(config_source, candidate_ids)
    retrieval_id = _text(config["retrieval_id"], "retrieval_id")
    index_config = _mapping(config["index"], "index")
    index = _build_index(
        retrieval_id,
        documents,
        k1=float(index_config["k1"]),
        b=float(index_config["b"]),
    )
    rankings = {
        query["query_id"]: _rank(index, query["query_text"])
        for query in queries
    }
    ranking_rows = [
        {
            "schema_version": RETRIEVAL_RANKING_SCHEMA_VERSION,
            "retrieval_id": retrieval_id,
            "query_id": query["query_id"],
            "split": query["split"],
            "relevant_object_ids": query["relevant_object_ids"],
            "ranking": rankings[query["query_id"]],
        }
        for query in queries
    ]
    answers: dict[str, dict[str, Any]] = {}
    answer_raw = None
    if answer_observations_path is not None:
        answer_raw, answers = _load_answer_observations(
            Path(answer_observations_path).resolve(),
            retrieval_id=retrieval_id,
            rankings=rankings,
        )
    top_k = [int(value) for value in index_config["top_k"]]
    evaluation = _evaluation(
        retrieval_id,
        queries,
        rankings,
        top_k,
        answers,
        annotation_status=str(config["annotation_status"]),
    )
    cohort = {
        "schema_version": RETRIEVAL_COHORT_SCHEMA_VERSION,
        "status": "COMPLETE",
        "retrieval_id": retrieval_id,
        "candidate_corpus": "all-representation-manifest-objects",
        "candidate_object_ids": list(candidate_ids),
        "candidate_object_count": len(candidate_ids),
        "independent_unit": "source-object-group",
        "annotation_status": config["annotation_status"],
        "queries": queries,
        "split_counts": dict(sorted(Counter(
            query["split"] for query in queries
        ).items())),
        "llm_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    documents_bytes = {
        "retrieval_cohort.json": _json_bytes(cohort),
        "lexical_index.json": _json_bytes(index),
        "rankings.jsonl": _jsonl_bytes(ranking_rows),
        "retrieval_evaluation.json": _json_bytes(evaluation),
    }
    manifest = {
        "schema_version": RETRIEVAL_MANIFEST_SCHEMA_VERSION,
        "status": "COMPLETE",
        "retrieval_id": retrieval_id,
        "query_count": len(queries),
        "candidate_object_count": len(candidate_ids),
        "index_kind": "bm25-lexical-v1",
        "index_size_bytes": len(documents_bytes["lexical_index.json"]),
        "annotation_status": config["annotation_status"],
        "answer_observation_count": len(answers),
        "source_sha256": {
            "retrieval_config": _sha256_bytes(config_raw),
            "representation_manifest": _sha256_bytes(representation_raw),
            "answer_observations": (
                _sha256_bytes(answer_raw) if answer_raw is not None else None
            ),
        },
        "external_services_called": False,
        "llm_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
        "output_sha256": {
            name: _sha256_bytes(content)
            for name, content in sorted(documents_bytes.items())
        },
    }
    documents_bytes["retrieval_manifest.json"] = _json_bytes(manifest)
    documents_bytes["SHA256SUMS"] = "".join(
        f"{_sha256_bytes(content)}  {name}\n"
        for name, content in sorted(documents_bytes.items())
    ).encode("utf-8")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging_parent = Path(tempfile.mkdtemp(prefix=".retrieval-", dir=target.parent))
    staging = staging_parent / "output"
    try:
        staging.mkdir()
        for name, content in documents_bytes.items():
            (staging / name).write_bytes(content)
        _verify_output(staging)
        _require(not target.exists(), f"retrieval output already exists: {target}")
        os.replace(staging, target)
    finally:
        shutil.rmtree(staging_parent, ignore_errors=True)
    return {
        "status": "COMPLETE",
        "retrieval_id": retrieval_id,
        "query_count": len(queries),
        "candidate_object_count": len(candidate_ids),
        "index_kind": "bm25-lexical-v1",
        "index_size_bytes": manifest["index_size_bytes"],
        "answer_observation_count": len(answers),
        "annotation_status": config["annotation_status"],
        "output_dir": str(target),
        "external_services_called": False,
        "eligible_for_scientific_claims": False,
    }


def verify_simulator_retrieval(output_dir: str | Path) -> dict[str, Any]:
    """Verify the exact immutable output set for a retrieval run."""
    root = Path(output_dir).resolve()
    _require(root.is_dir(), f"retrieval output does not exist: {root}")
    manifest = _verify_output(root)
    return {
        "status": "VERIFIED",
        "retrieval_id": manifest["retrieval_id"],
        "query_count": manifest["query_count"],
        "candidate_object_count": manifest["candidate_object_count"],
        "checked_files": 5,
        "eligible_for_scientific_claims": False,
    }
