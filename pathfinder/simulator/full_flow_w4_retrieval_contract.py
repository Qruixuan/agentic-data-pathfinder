"""Prospective, hidden-label W4 retrieval contract for the full-flow matrix.

The current v1 semantic matrix binds every workload, including W4, to the
multiple-choice N1 task schema.  That is useful for transport conformance but
is not a retrieval-quality experiment.  This module freezes an explicit W4
overlay without pretending that the existing matrix already consumes it.

The public directory contains the query, the complete multi-object candidate
set, metric definitions, and bindings for all sixteen W4 physical trials.  A
separate ``n1-private`` directory contains the relevance judgement.  Only a
SHA-256 commitment to that private document enters the public contract.

The evaluator is deliberately offline and emits metric values and ranking
hashes, never relevant object IDs.  Runtime/FlowMesh integration remains a
separate fail-closed gate recorded in the public commitment.
"""

from __future__ import annotations

import json
import math
import os
import shutil
import tempfile
from pathlib import Path
from statistics import mean
from typing import Any, Iterable, Mapping

from ._full_flow_primitives import (
    canonical_json_bytes,
    canonical_json_lines_bytes,
    checked_identifier,
    checked_lower_sha256,
    pretty_json_bytes,
    sha256_hex,
    strict_json_loads,
)
from .full_flow_semantic_matrix import (
    PLAN_NAME as SEMANTIC_PLAN_NAME,
    PUBLIC_TASKS_NAME as SEMANTIC_PUBLIC_TASKS_NAME,
    TRIALS_NAME as SEMANTIC_TRIALS_NAME,
    _verify_published as _verify_semantic_matrix_package,
)
from .retrieval import build_simulator_retrieval_cohort


W4_RETRIEVAL_TASK_SCHEMA_VERSION = (
    "pathfinder.full-flow-w4-retrieval-task/v1alpha1"
)
W4_RETRIEVAL_TRIAL_BINDING_SCHEMA_VERSION = (
    "pathfinder.full-flow-w4-retrieval-trial-binding/v1alpha1"
)
W4_RETRIEVAL_COMMITMENT_SCHEMA_VERSION = (
    "pathfinder.full-flow-w4-retrieval-commitment/v1alpha1"
)
W4_RETRIEVAL_HIDDEN_SOURCE_SCHEMA_VERSION = (
    "pathfinder.n1-w4-hidden-relevance/v1alpha1"
)
W4_RETRIEVAL_ORACLE_SCHEMA_VERSION = (
    "pathfinder.n1-w4-retrieval-oracle/v1alpha1"
)
W4_RETRIEVAL_OBSERVATIONS_SCHEMA_VERSION = (
    "pathfinder.full-flow-w4-retrieval-observations/v1alpha1"
)
W4_RETRIEVAL_EVALUATION_SCHEMA_VERSION = (
    "pathfinder.full-flow-w4-retrieval-evaluation/v1alpha1"
)

PUBLIC_DIRECTORY_NAME = "public"
PRIVATE_DIRECTORY_NAME = "n1-private"
PUBLIC_TASK_NAME = "w4-retrieval-task.json"
PUBLIC_BINDINGS_NAME = "w4-retrieval-trial-bindings.jsonl"
PUBLIC_COMMITMENT_NAME = "w4-retrieval-commitment.json"
PRIVATE_RELEVANCE_NAME = "hidden-relevance.json"
PRIVATE_ORACLE_NAME = "n1-w4-retrieval-oracle.json"
EVALUATION_NAME = "w4-retrieval-evaluation.json"
EVALUATION_ROWS_NAME = "w4-retrieval-trial-metrics.jsonl"
CHECKSUMS_NAME = "SHA256SUMS"

_PUBLIC_FILES = frozenset(
    {PUBLIC_TASK_NAME, PUBLIC_BINDINGS_NAME, PUBLIC_COMMITMENT_NAME}
)
_PRIVATE_FILES = frozenset({PRIVATE_RELEVANCE_NAME, PRIVATE_ORACLE_NAME})
_EVALUATION_FILES = frozenset({EVALUATION_NAME, EVALUATION_ROWS_NAME})


class FullFlowW4RetrievalContractError(ValueError):
    """Raised when a W4 retrieval contract is ambiguous or unsafe."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowW4RetrievalContractError(message)


def _read_json(path: Path, label: str) -> tuple[bytes, dict[str, Any]]:
    try:
        raw = path.read_bytes()
        value = strict_json_loads(
            raw.decode("utf-8"),
            error_type=FullFlowW4RetrievalContractError,
            duplicate_key_message=lambda key: f"duplicate JSON key: {key}",
            nonfinite_number_message=(
                lambda token: f"non-finite JSON number: {token}"
            ),
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowW4RetrievalContractError(
            f"cannot read valid {label}: {path}"
        ) from exc
    _require(isinstance(value, dict), f"{label} must be an object")
    return raw, value


def _read_jsonl(path: Path, label: str) -> tuple[bytes, list[dict[str, Any]]]:
    try:
        raw = path.read_bytes()
        lines = raw.decode("utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FullFlowW4RetrievalContractError(
            f"cannot read valid {label}: {path}"
        ) from exc
    rows: list[dict[str, Any]] = []
    for position, line in enumerate(lines):
        _require(bool(line), f"{label} contains a blank line")
        try:
            row = strict_json_loads(
                line,
                error_type=FullFlowW4RetrievalContractError,
                duplicate_key_message=lambda key: f"duplicate JSON key: {key}",
                nonfinite_number_message=(
                    lambda token: f"non-finite JSON number: {token}"
                ),
            )
        except json.JSONDecodeError as exc:
            raise FullFlowW4RetrievalContractError(
                f"invalid {label} row {position}"
            ) from exc
        _require(isinstance(row, dict), f"{label} row must be an object")
        rows.append(row)
    _require(bool(rows), f"{label} must not be empty")
    return raw, rows


def _json_bytes(value: Any) -> bytes:
    return pretty_json_bytes(value)


def _jsonl_bytes(values: Iterable[Mapping[str, Any]]) -> bytes:
    return canonical_json_lines_bytes(values)


def _canonical_bytes(value: Any) -> bytes:
    return canonical_json_bytes(value)


def _sha256(value: bytes) -> str:
    return sha256_hex(value)


def _identifier(value: Any, label: str) -> str:
    return checked_identifier(
        value,
        label,
        error_type=FullFlowW4RetrievalContractError,
    )


def _digest(value: Any, label: str) -> str:
    return checked_lower_sha256(
        value,
        label,
        error_type=FullFlowW4RetrievalContractError,
        message=f"{label} is not a lowercase SHA-256 digest",
    )


def _strict_fields(
    value: Mapping[str, Any], expected: set[str], label: str
) -> None:
    _require(set(value) == expected, f"{label} fields changed")


def _checksum_bytes(documents: Mapping[str, bytes]) -> bytes:
    return "".join(
        f"{_sha256(content)}  {name}\n"
        for name, content in sorted(documents.items())
    ).encode("utf-8")


def _verify_checksums(root: Path, expected: frozenset[str]) -> dict[str, str]:
    _require(root.is_dir(), f"contract directory does not exist: {root}")
    entries = list(root.iterdir())
    _require(
        all(path.is_file() and not path.is_symlink() for path in entries),
        "contract directory contains a non-regular file",
    )
    actual = {path.name for path in entries}
    _require(actual == expected | {CHECKSUMS_NAME}, "contract file set changed")
    try:
        lines = (root / CHECKSUMS_NAME).read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise FullFlowW4RetrievalContractError(
            "cannot read contract SHA256SUMS"
        ) from exc
    found: dict[str, str] = {}
    for line in lines:
        digest, separator, name = line.partition("  ")
        _require(separator == "  " and name in expected, "malformed SHA256SUMS")
        _digest(digest, "checksum")
        _require(name not in found, f"duplicate checksum: {name}")
        _require(
            _sha256((root / name).read_bytes()) == digest,
            f"checksum mismatch: {name}",
        )
        found[name] = digest
    _require(set(found) == set(expected), "contract checksums are incomplete")
    return found


def _contract_roots(root: Path) -> tuple[Path, Path]:
    _require(root.is_dir() and not root.is_symlink(), "W4 contract root is invalid")
    entries = list(root.iterdir())
    _require(
        {path.name for path in entries}
        == {PUBLIC_DIRECTORY_NAME, PRIVATE_DIRECTORY_NAME}
        and all(path.is_dir() and not path.is_symlink() for path in entries),
        "W4 contract root structure changed",
    )
    return root / PUBLIC_DIRECTORY_NAME, root / PRIVATE_DIRECTORY_NAME


def _metric_top_k(evaluation: Mapping[str, Any]) -> list[int]:
    aggregates = evaluation.get("aggregates")
    _require(
        isinstance(aggregates, list) and bool(aggregates),
        "retrieval evaluation aggregates are missing",
    )
    overall = aggregates[0]
    _require(isinstance(overall, Mapping), "overall retrieval aggregate is invalid")
    values = sorted(
        int(key.removeprefix("recall_at_"))
        for key in overall
        if key.startswith("recall_at_")
    )
    _require(bool(values) and values == sorted(set(values)), "top-k contract missing")
    _require(all(value > 0 for value in values), "top-k values must be positive")
    return values


def _load_retrieval_source(
    config_path: Path,
    representation_manifest_path: Path,
    selected_query_id: str,
) -> dict[str, Any]:
    """Use the existing strict builder as the single source validator."""

    with tempfile.TemporaryDirectory(prefix="pathfinder-w4-contract-") as temp:
        output = Path(temp) / "retrieval"
        build_simulator_retrieval_cohort(
            config_path,
            representation_manifest_path,
            output_dir=output,
        )
        _, cohort = _read_json(output / "retrieval_cohort.json", "cohort")
        _, index = _read_json(output / "lexical_index.json", "index")
        _, evaluation = _read_json(
            output / "retrieval_evaluation.json", "evaluation"
        )
        _, manifest = _read_json(output / "retrieval_manifest.json", "manifest")
    _require(
        cohort.get("annotation_status") == "operator-verified",
        "prospective W4 contract requires operator-verified relevance labels",
    )
    queries = cohort.get("queries")
    _require(isinstance(queries, list), "retrieval queries are missing")
    selected = [row for row in queries if row.get("query_id") == selected_query_id]
    _require(len(selected) == 1, "selected W4 query is not unique")
    query = selected[0]
    _require(
        query.get("split") == "test",
        "prospective W4 query must come from the test split",
    )
    candidate_ids = cohort.get("candidate_object_ids")
    _require(
        isinstance(candidate_ids, list)
        and len(candidate_ids) >= 2
        and candidate_ids == sorted(set(candidate_ids)),
        "W4 candidate corpus must contain multiple unique sorted objects",
    )
    source_sha = index.get("source_digest_sha256")
    source_sizes = index.get("source_digest_size_bytes")
    _require(
        isinstance(source_sha, Mapping)
        and isinstance(source_sizes, Mapping)
        and set(source_sha) == set(candidate_ids)
        and set(source_sizes) == set(candidate_ids),
        "retrieval index does not bind the complete candidate corpus",
    )
    candidates = []
    for object_id in candidate_ids:
        _identifier(object_id, "candidate object_id")
        size = source_sizes[object_id]
        _require(type(size) is int and size > 0, "candidate digest size is invalid")
        candidates.append(
            {
                "object_id": object_id,
                "representation_id": "multimodal_digest",
                "artifact_sha256": _digest(
                    source_sha[object_id], "candidate artifact_sha256"
                ),
                "artifact_size_bytes": size,
            }
        )
    return {
        "retrieval_id": _identifier(cohort.get("retrieval_id"), "retrieval_id"),
        "query": query,
        "candidates": candidates,
        "top_k": _metric_top_k(evaluation),
        "source_sha256": dict(manifest["source_sha256"]),
        "annotation_status": cohort["annotation_status"],
    }


def _load_w4_matrix_source(root: Path) -> dict[str, Any]:
    try:
        plan = _verify_semantic_matrix_package(root)
    except Exception as exc:
        raise FullFlowW4RetrievalContractError(
            "semantic matrix package failed verification"
        ) from exc
    _, public = _read_json(root / SEMANTIC_PUBLIC_TASKS_NAME, "public tasks")
    _, trials = _read_jsonl(root / SEMANTIC_TRIALS_NAME, "semantic trials")
    w4 = [row for row in trials if row.get("workload_class") == "W4"]
    _require(len(w4) == 16, "semantic matrix must contain sixteen W4 trials")
    _require(
        [row.get("order_index") for row in w4]
        == sorted(row.get("order_index") for row in w4),
        "W4 trial order is not canonical",
    )
    workload_ids = {row.get("workload_id") for row in w4}
    _require(len(workload_ids) == 1, "W4 trials do not share one workload")
    workload_id = next(iter(workload_ids))
    tasks = public.get("tasks")
    _require(isinstance(tasks, list), "semantic public tasks are missing")
    matched = [row for row in tasks if row.get("workload_id") == workload_id]
    _require(len(matched) == 1, "W4 public task is not unique")
    placeholder = matched[0]
    _require(
        placeholder.get("task_class_id") == "video_retrieval",
        "W4 public task class is not video_retrieval",
    )
    _require(
        isinstance(placeholder.get("answer_options"), list)
        and bool(placeholder["answer_options"]),
        "W4 matrix no longer has the expected multiple-choice placeholder",
    )
    placeholder_sha = _digest(
        placeholder.get("task_binding_sha256"),
        "W4 placeholder task binding",
    )
    _require(
        {row.get("public_task_binding_sha256") for row in w4}
        == {placeholder_sha},
        "W4 trials do not bind the placeholder task consistently",
    )
    artifact_ids = {row.get("artifact_object_id") for row in w4}
    _require(len(artifact_ids) == 1, "W4 trials do not share one target object")
    return {
        "matrix_plan_sha256": _digest(plan.get("plan_sha256"), "matrix plan SHA"),
        "task_plane_id": _identifier(plan.get("task_plane_id"), "task_plane_id"),
        "workload_id": _identifier(workload_id, "W4 workload_id"),
        "placeholder_task_binding_sha256": placeholder_sha,
        "matrix_target_object_id": _identifier(
            next(iter(artifact_ids)), "W4 matrix target object_id"
        ),
        "trials": w4,
    }


def _build_documents(
    *,
    contract_id: str,
    matrix: Mapping[str, Any],
    retrieval: Mapping[str, Any],
) -> tuple[dict[str, bytes], dict[str, bytes]]:
    candidates = retrieval["candidates"]
    candidate_set_sha = _sha256(_canonical_bytes(candidates))
    query = retrieval["query"]
    top_k = retrieval["top_k"]
    task_core = {
        "schema_version": W4_RETRIEVAL_TASK_SCHEMA_VERSION,
        "contract_id": contract_id,
        "workload_id": matrix["workload_id"],
        "workload_class": "W4",
        "task_class_id": "video_retrieval",
        "retrieval_id": retrieval["retrieval_id"],
        "query_id": query["query_id"],
        "query_text": query["query_text"],
        "candidate_corpus": "all-representation-manifest-objects",
        "candidate_objects": candidates,
        "candidate_set_sha256": candidate_set_sha,
        "required_ranking_length": len(candidates),
        "quality_metrics": {
            "per_query": [
                "reciprocal_rank",
                *[f"recall_at_{value}" for value in top_k],
                *[f"hit_at_{value}" for value in top_k],
                *[f"ndcg_at_{value}" for value in top_k],
            ],
            "aggregate": [
                "mrr",
                *[f"mean_recall_at_{value}" for value in top_k],
                *[f"mean_hit_at_{value}" for value in top_k],
                *[f"mean_ndcg_at_{value}" for value in top_k],
            ],
            "top_k": top_k,
            "relevance": "binary",
        },
        "relevance_values_included": False,
        "source_object_group_included": False,
        "credentials_recorded": False,
    }
    task = dict(task_core)
    task["task_binding_sha256"] = _sha256(_canonical_bytes(task_core))
    trial_bindings = [
        {
            "schema_version": W4_RETRIEVAL_TRIAL_BINDING_SCHEMA_VERSION,
            "contract_id": contract_id,
            "trial_key": row["trial_key"],
            "order_index": row["order_index"],
            "design_id": row["design_id"],
            "repetition": row["repetition"],
            "query_id": query["query_id"],
            "retrieval_task_binding_sha256": task["task_binding_sha256"],
            "replaces_multiple_choice_task_binding_sha256": matrix[
                "placeholder_task_binding_sha256"
            ],
            "credentials_recorded": False,
        }
        for row in matrix["trials"]
    ]
    hidden = {
        "schema_version": W4_RETRIEVAL_HIDDEN_SOURCE_SCHEMA_VERSION,
        "contract_id": contract_id,
        "logical_node_id": "N1",
        "query_id": query["query_id"],
        "retrieval_task_binding_sha256": task["task_binding_sha256"],
        "candidate_set_sha256": candidate_set_sha,
        "relevant_object_ids": query["relevant_object_ids"],
        "source_object_group": query["source_object_group"],
        "annotation_status": retrieval["annotation_status"],
        "credentials_recorded": False,
    }
    hidden_bytes = _json_bytes(hidden)
    oracle = {
        "schema_version": W4_RETRIEVAL_ORACLE_SCHEMA_VERSION,
        "status": "FROZEN_N1_PRIVATE_W4_RETRIEVAL_ORACLE",
        "contract_id": contract_id,
        "logical_node_id": "N1",
        "retrieval_task_binding_sha256": task["task_binding_sha256"],
        "candidate_set_sha256": candidate_set_sha,
        "hidden_relevance_sha256": _sha256(hidden_bytes),
        "hidden_relevance_values_returned": False,
        "not_for_flowmesh_plan": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    task_bytes = _json_bytes(task)
    bindings_bytes = _jsonl_bytes(trial_bindings)
    oracle_bytes = _json_bytes(oracle)
    commitment = {
        "schema_version": W4_RETRIEVAL_COMMITMENT_SCHEMA_VERSION,
        "status": "FROZEN_PUBLIC_W4_RETRIEVAL_CONTRACT",
        "contract_id": contract_id,
        "matrix_plan_sha256": matrix["matrix_plan_sha256"],
        "task_plane_id": matrix["task_plane_id"],
        "w4_trial_count": len(trial_bindings),
        "retrieval_task_binding_sha256": task["task_binding_sha256"],
        "candidate_set_sha256": candidate_set_sha,
        "candidate_object_count": len(candidates),
        "hidden_relevance_sha256": _sha256(hidden_bytes),
        "hidden_relevance_committed": True,
        "relevance_values_included": False,
        "annotation_status": retrieval["annotation_status"],
        "source_sha256": retrieval["source_sha256"],
        "current_matrix_native_w4_semantics": "multiple-choice-placeholder",
        "overlay_required_at_compile_and_runtime": True,
        "runtime_executor_binding_present": False,
        "ready_for_current_64_trial_execution": False,
        "readiness_blocker": "W4_RETRIEVAL_OVERLAY_NOT_YET_CONSUMED_BY_RUNTIME",
        "external_services_called": False,
        "llm_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
        "output_sha256": {
            PUBLIC_TASK_NAME: _sha256(task_bytes),
            PUBLIC_BINDINGS_NAME: _sha256(bindings_bytes),
        },
    }
    public = {
        PUBLIC_TASK_NAME: task_bytes,
        PUBLIC_BINDINGS_NAME: bindings_bytes,
        PUBLIC_COMMITMENT_NAME: _json_bytes(commitment),
    }
    private = {
        PRIVATE_RELEVANCE_NAME: hidden_bytes,
        PRIVATE_ORACLE_NAME: oracle_bytes,
    }
    return public, private


def _publish(
    root: Path,
    public: Mapping[str, bytes],
    private: Mapping[str, bytes],
) -> None:
    _require(not root.exists(), f"W4 contract output already exists: {root}")
    root.parent.mkdir(parents=True, exist_ok=True)
    stage_parent = Path(tempfile.mkdtemp(prefix=".w4-contract-", dir=root.parent))
    stage = stage_parent / "contract"
    try:
        public_root = stage / PUBLIC_DIRECTORY_NAME
        private_root = stage / PRIVATE_DIRECTORY_NAME
        public_root.mkdir(parents=True)
        private_root.mkdir()
        for name, content in public.items():
            (public_root / name).write_bytes(content)
        (public_root / CHECKSUMS_NAME).write_bytes(_checksum_bytes(public))
        for name, content in private.items():
            (private_root / name).write_bytes(content)
        (private_root / CHECKSUMS_NAME).write_bytes(_checksum_bytes(private))
        _verify_contract_roots(public_root, private_root)
        _require(not root.exists(), f"W4 contract output already exists: {root}")
        os.replace(stage, root)
    finally:
        shutil.rmtree(stage_parent, ignore_errors=True)


def _validate_task(task: Mapping[str, Any]) -> dict[str, Any]:
    expected = {
        "schema_version",
        "contract_id",
        "workload_id",
        "workload_class",
        "task_class_id",
        "retrieval_id",
        "query_id",
        "query_text",
        "candidate_corpus",
        "candidate_objects",
        "candidate_set_sha256",
        "required_ranking_length",
        "quality_metrics",
        "relevance_values_included",
        "source_object_group_included",
        "credentials_recorded",
        "task_binding_sha256",
    }
    _strict_fields(task, expected, "W4 retrieval task")
    _require(
        task.get("schema_version") == W4_RETRIEVAL_TASK_SCHEMA_VERSION,
        "unsupported W4 retrieval task schema_version",
    )
    for field in ("contract_id", "workload_id", "retrieval_id", "query_id"):
        _identifier(task.get(field), field)
    _require(task.get("workload_class") == "W4", "W4 workload class changed")
    _require(
        task.get("task_class_id") == "video_retrieval",
        "W4 task class changed",
    )
    _require(
        isinstance(task.get("query_text"), str)
        and bool(task["query_text"])
        and task["query_text"] == task["query_text"].strip()
        and len(task["query_text"].encode("utf-8")) <= 64 * 1024,
        "W4 query text is empty",
    )
    _require(
        task.get("candidate_corpus")
        == "all-representation-manifest-objects",
        "W4 candidate corpus changed",
    )
    candidates = task.get("candidate_objects")
    _require(
        isinstance(candidates, list) and len(candidates) >= 2,
        "W4 candidate set is not multi-object",
    )
    ids: list[str] = []
    for candidate in candidates:
        _require(isinstance(candidate, Mapping), "candidate must be an object")
        _strict_fields(
            candidate,
            {
                "object_id",
                "representation_id",
                "artifact_sha256",
                "artifact_size_bytes",
            },
            "candidate",
        )
        ids.append(_identifier(candidate.get("object_id"), "candidate object_id"))
        _require(
            candidate.get("representation_id") == "multimodal_digest",
            "candidate representation changed",
        )
        _digest(candidate.get("artifact_sha256"), "candidate artifact SHA")
        _require(
            type(candidate.get("artifact_size_bytes")) is int
            and candidate["artifact_size_bytes"] > 0,
            "candidate size is invalid",
        )
    _require(ids == sorted(set(ids)), "candidate IDs are not sorted and unique")
    _require(
        task.get("candidate_set_sha256") == _sha256(_canonical_bytes(candidates)),
        "candidate set digest mismatch",
    )
    _require(
        task.get("required_ranking_length") == len(candidates),
        "required ranking length changed",
    )
    metrics = task.get("quality_metrics")
    _require(isinstance(metrics, Mapping), "quality metrics are missing")
    _strict_fields(
        metrics,
        {"per_query", "aggregate", "top_k", "relevance"},
        "quality metrics",
    )
    top_k = metrics.get("top_k")
    _require(
        isinstance(top_k, list)
        and bool(top_k)
        and top_k == sorted(set(top_k))
        and all(type(value) is int and 0 < value <= len(candidates) for value in top_k),
        "quality metric top-k values are invalid",
    )
    _require(metrics.get("relevance") == "binary", "relevance model changed")
    _require(
        metrics.get("per_query")
        == [
            "reciprocal_rank",
            *[f"recall_at_{value}" for value in top_k],
            *[f"hit_at_{value}" for value in top_k],
            *[f"ndcg_at_{value}" for value in top_k],
        ],
        "per-query metric contract changed",
    )
    _require(
        metrics.get("aggregate")
        == [
            "mrr",
            *[f"mean_recall_at_{value}" for value in top_k],
            *[f"mean_hit_at_{value}" for value in top_k],
            *[f"mean_ndcg_at_{value}" for value in top_k],
        ],
        "aggregate metric contract changed",
    )
    for field in (
        "relevance_values_included",
        "source_object_group_included",
        "credentials_recorded",
    ):
        _require(task.get(field) is False, f"{field} must be false")
    supplied = _digest(task.get("task_binding_sha256"), "task binding")
    core = dict(task)
    del core["task_binding_sha256"]
    _require(supplied == _sha256(_canonical_bytes(core)), "task binding mismatch")
    return dict(task)


def _validate_hidden(
    hidden: Mapping[str, Any], task: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {
        "schema_version",
        "contract_id",
        "logical_node_id",
        "query_id",
        "retrieval_task_binding_sha256",
        "candidate_set_sha256",
        "relevant_object_ids",
        "source_object_group",
        "annotation_status",
        "credentials_recorded",
    }
    _strict_fields(hidden, expected, "hidden relevance")
    _require(
        hidden.get("schema_version") == W4_RETRIEVAL_HIDDEN_SOURCE_SCHEMA_VERSION,
        "unsupported hidden relevance schema_version",
    )
    _require(hidden.get("logical_node_id") == "N1", "hidden oracle node changed")
    _require(hidden.get("contract_id") == task["contract_id"], "contract mismatch")
    _require(hidden.get("query_id") == task["query_id"], "query mismatch")
    _require(
        hidden.get("retrieval_task_binding_sha256")
        == task["task_binding_sha256"],
        "hidden task binding mismatch",
    )
    _require(
        hidden.get("candidate_set_sha256") == task["candidate_set_sha256"],
        "hidden candidate set mismatch",
    )
    relevant = hidden.get("relevant_object_ids")
    candidate_ids = {row["object_id"] for row in task["candidate_objects"]}
    _require(
        isinstance(relevant, list)
        and bool(relevant)
        and len(relevant) == len(set(relevant))
        and set(relevant) <= candidate_ids,
        "hidden relevance judgement is invalid",
    )
    for object_id in relevant:
        _identifier(object_id, "relevant object_id")
    _identifier(hidden.get("source_object_group"), "source_object_group")
    _require(
        hidden.get("annotation_status") == "operator-verified",
        "hidden relevance is not operator verified",
    )
    _require(
        hidden.get("credentials_recorded") is False,
        "hidden source records credentials",
    )
    return dict(hidden)


def _verify_contract_roots(
    public_root: Path, private_root: Path
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    public_sums = _verify_checksums(public_root, _PUBLIC_FILES)
    private_sums = _verify_checksums(private_root, _PRIVATE_FILES)
    task_raw, task_value = _read_json(
        public_root / PUBLIC_TASK_NAME, "W4 task"
    )
    _require(task_raw == _json_bytes(task_value), "W4 task is not canonical")
    task = _validate_task(task_value)
    bindings_raw, bindings = _read_jsonl(
        public_root / PUBLIC_BINDINGS_NAME, "W4 bindings"
    )
    _require(
        bindings_raw == _jsonl_bytes(bindings),
        "W4 trial bindings are not canonical",
    )
    _require(len(bindings) == 16, "W4 binding count changed")
    _require(
        [row.get("order_index") for row in bindings]
        == sorted(row.get("order_index") for row in bindings),
        "W4 bindings are not ordered",
    )
    trial_keys: set[str] = set()
    for row in bindings:
        _strict_fields(
            row,
            {
                "schema_version",
                "contract_id",
                "trial_key",
                "order_index",
                "design_id",
                "repetition",
                "query_id",
                "retrieval_task_binding_sha256",
                "replaces_multiple_choice_task_binding_sha256",
                "credentials_recorded",
            },
            "W4 trial binding",
        )
        _require(
            row.get("schema_version")
            == W4_RETRIEVAL_TRIAL_BINDING_SCHEMA_VERSION,
            "unsupported W4 trial binding schema_version",
        )
        key = _identifier(row.get("trial_key"), "trial_key")
        _require(key not in trial_keys, "duplicate W4 trial binding")
        trial_keys.add(key)
        _require(
            row.get("contract_id") == task["contract_id"],
            "contract mismatch",
        )
        _require(row.get("query_id") == task["query_id"], "query mismatch")
        _require(
            row.get("retrieval_task_binding_sha256") == task["task_binding_sha256"],
            "trial task binding mismatch",
        )
        _digest(
            row.get("replaces_multiple_choice_task_binding_sha256"),
            "placeholder task binding",
        )
        _require(
            row.get("credentials_recorded") is False,
            "binding records credentials",
        )
    _require(
        {(row.get("design_id"), row.get("repetition")) for row in bindings}
        == {
            (f"D{design}", repetition)
            for design in range(8)
            for repetition in range(2)
        },
        "W4 trial matrix coverage changed",
    )
    hidden_raw, hidden_value = _read_json(
        private_root / PRIVATE_RELEVANCE_NAME, "hidden relevance"
    )
    _require(
        hidden_raw == _json_bytes(hidden_value),
        "hidden relevance is not canonical",
    )
    hidden = _validate_hidden(hidden_value, task)
    oracle_raw, oracle = _read_json(
        private_root / PRIVATE_ORACLE_NAME, "private oracle"
    )
    _require(
        oracle_raw == _json_bytes(oracle),
        "private oracle is not canonical",
    )
    _require(
        oracle.get("schema_version") == W4_RETRIEVAL_ORACLE_SCHEMA_VERSION
        and oracle.get("status") == "FROZEN_N1_PRIVATE_W4_RETRIEVAL_ORACLE",
        "private oracle schema or status changed",
    )
    _require(
        set(oracle)
        == {
            "schema_version",
            "status",
            "contract_id",
            "logical_node_id",
            "retrieval_task_binding_sha256",
            "candidate_set_sha256",
            "hidden_relevance_sha256",
            "hidden_relevance_values_returned",
            "not_for_flowmesh_plan",
            "credentials_recorded",
            "eligible_for_scientific_claims",
        },
        "private oracle fields changed",
    )
    _require(
        oracle.get("contract_id") == task["contract_id"],
        "oracle contract mismatch",
    )
    _require(oracle.get("logical_node_id") == "N1", "oracle node changed")
    _require(
        oracle.get("retrieval_task_binding_sha256")
        == task["task_binding_sha256"]
        and oracle.get("candidate_set_sha256")
        == task["candidate_set_sha256"],
        "private oracle public binding mismatch",
    )
    _require(
        oracle.get("hidden_relevance_sha256")
        == private_sums[PRIVATE_RELEVANCE_NAME],
        "oracle hidden commitment mismatch",
    )
    for field in (
        "hidden_relevance_values_returned",
        "credentials_recorded",
        "eligible_for_scientific_claims",
    ):
        _require(oracle.get(field) is False, f"{field} must be false")
    _require(
        oracle.get("not_for_flowmesh_plan") is True,
        "private oracle is not isolated",
    )
    commitment_raw, commitment = _read_json(
        public_root / PUBLIC_COMMITMENT_NAME, "public commitment"
    )
    _require(
        commitment_raw == _json_bytes(commitment),
        "public commitment is not canonical",
    )
    _strict_fields(
        commitment,
        {
            "schema_version",
            "status",
            "contract_id",
            "matrix_plan_sha256",
            "task_plane_id",
            "w4_trial_count",
            "retrieval_task_binding_sha256",
            "candidate_set_sha256",
            "candidate_object_count",
            "hidden_relevance_sha256",
            "hidden_relevance_committed",
            "relevance_values_included",
            "annotation_status",
            "source_sha256",
            "current_matrix_native_w4_semantics",
            "overlay_required_at_compile_and_runtime",
            "runtime_executor_binding_present",
            "ready_for_current_64_trial_execution",
            "readiness_blocker",
            "external_services_called",
            "llm_called",
            "credentials_recorded",
            "eligible_for_scientific_claims",
            "output_sha256",
        },
        "public commitment",
    )
    _require(
        commitment.get("schema_version") == W4_RETRIEVAL_COMMITMENT_SCHEMA_VERSION
        and commitment.get("status") == "FROZEN_PUBLIC_W4_RETRIEVAL_CONTRACT",
        "public commitment schema or status changed",
    )
    _require(
        commitment.get("contract_id") == task["contract_id"]
        and commitment.get("retrieval_task_binding_sha256")
        == task["task_binding_sha256"]
        and commitment.get("candidate_set_sha256") == task["candidate_set_sha256"],
        "public commitment binding mismatch",
    )
    _require(
        commitment.get("hidden_relevance_sha256")
        == private_sums[PRIVATE_RELEVANCE_NAME],
        "public hidden commitment mismatch",
    )
    _require(
        commitment.get("output_sha256")
        == {
            PUBLIC_TASK_NAME: public_sums[PUBLIC_TASK_NAME],
            PUBLIC_BINDINGS_NAME: public_sums[PUBLIC_BINDINGS_NAME],
        },
        "public output bindings changed",
    )
    _require(commitment.get("w4_trial_count") == 16, "W4 trial count changed")
    _require(
        commitment.get("candidate_object_count")
        == len(task["candidate_objects"]),
        "candidate object count changed",
    )
    _digest(commitment.get("matrix_plan_sha256"), "matrix plan SHA")
    _identifier(commitment.get("task_plane_id"), "task_plane_id")
    _require(
        commitment.get("annotation_status") == "operator-verified",
        "public commitment is not prospective",
    )
    sources = commitment.get("source_sha256")
    _require(
        isinstance(sources, Mapping)
        and set(sources)
        == {
            "retrieval_config",
            "representation_manifest",
            "answer_observations",
        }
        and sources["answer_observations"] is None,
        "public source bindings changed",
    )
    _digest(sources["retrieval_config"], "retrieval config SHA")
    _digest(sources["representation_manifest"], "representation manifest SHA")
    _require(
        commitment.get("current_matrix_native_w4_semantics")
        == "multiple-choice-placeholder"
        and commitment.get("overlay_required_at_compile_and_runtime") is True
        and commitment.get("runtime_executor_binding_present") is False
        and commitment.get("ready_for_current_64_trial_execution") is False
        and commitment.get("readiness_blocker")
        == "W4_RETRIEVAL_OVERLAY_NOT_YET_CONSUMED_BY_RUNTIME",
        "W4 fail-closed runtime boundary changed",
    )
    _require(
        commitment.get("hidden_relevance_committed") is True,
        "hidden relevance is not committed",
    )
    _require(
        commitment.get("relevance_values_included") is False,
        "public contract leaks relevance",
    )
    _require(
        commitment.get("credentials_recorded") is False,
        "public contract records credentials",
    )
    for field in (
        "external_services_called",
        "llm_called",
        "eligible_for_scientific_claims",
    ):
        _require(commitment.get(field) is False, f"{field} must be false")
    return task, bindings, hidden


def freeze_full_flow_w4_retrieval_contract(
    semantic_matrix_dir: str | Path,
    retrieval_config_path: str | Path,
    representation_manifest_path: str | Path,
    *,
    selected_query_id: str,
    contract_id: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze a public W4 overlay and a separate N1 hidden relevance oracle."""

    contract = _identifier(contract_id, "contract_id")
    query_id = _identifier(selected_query_id, "selected_query_id")
    root = Path(output_dir).resolve()
    _require(not root.exists(), f"W4 contract output already exists: {root}")
    matrix = _load_w4_matrix_source(Path(semantic_matrix_dir).resolve())
    retrieval = _load_retrieval_source(
        Path(retrieval_config_path).resolve(),
        Path(representation_manifest_path).resolve(),
        query_id,
    )
    candidate_ids = {
        row["object_id"] for row in retrieval["candidates"]
    }
    _require(
        matrix["matrix_target_object_id"] in candidate_ids,
        "W4 matrix target is absent from the retrieval candidate corpus",
    )
    public, private = _build_documents(
        contract_id=contract,
        matrix=matrix,
        retrieval=retrieval,
    )
    _publish(root, public, private)
    verification = verify_full_flow_w4_retrieval_contract(root)
    return {
        **verification,
        "status": "FROZEN_PUBLIC_AND_N1_PRIVATE_W4_RETRIEVAL_CONTRACT",
        "output_dir": str(root),
        "public_dir": str(root / PUBLIC_DIRECTORY_NAME),
        "n1_private_dir": str(root / PRIVATE_DIRECTORY_NAME),
    }


def verify_full_flow_w4_retrieval_contract(
    output_dir: str | Path,
) -> dict[str, Any]:
    """Verify both halves while returning no relevance-label value."""

    root = Path(output_dir).resolve()
    public_root, private_root = _contract_roots(root)
    task, bindings, _hidden = _verify_contract_roots(public_root, private_root)
    return {
        "status": "VERIFIED",
        "contract_id": task["contract_id"],
        "query_id": task["query_id"],
        "candidate_object_count": len(task["candidate_objects"]),
        "w4_trial_count": len(bindings),
        "hidden_relevance_values_returned": False,
        "runtime_executor_binding_present": False,
        "ready_for_current_64_trial_execution": False,
        "readiness_blocker": "W4_RETRIEVAL_OVERLAY_NOT_YET_CONSUMED_BY_RUNTIME",
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def _ranking_sha256(ranking: list[str]) -> str:
    return _sha256(_canonical_bytes(ranking))


def _score_ranking(
    ranking: list[str], relevant: set[str], top_k: list[int]
) -> dict[str, float | bool]:
    first = next(
        (
            position
            for position, object_id in enumerate(ranking, start=1)
            if object_id in relevant
        ),
        None,
    )
    metrics: dict[str, float | bool] = {
        "reciprocal_rank": 0.0 if first is None else 1.0 / first
    }
    for value in top_k:
        selected = ranking[:value]
        matched = sum(object_id in relevant for object_id in selected)
        metrics[f"recall_at_{value}"] = matched / len(relevant)
        metrics[f"hit_at_{value}"] = matched > 0
        dcg = sum(
            1.0 / math.log2(position + 1)
            for position, object_id in enumerate(selected, start=1)
            if object_id in relevant
        )
        ideal_hits = min(value, len(relevant))
        ideal = sum(
            1.0 / math.log2(position + 1)
            for position in range(1, ideal_hits + 1)
        )
        metrics[f"ndcg_at_{value}"] = dcg / ideal
    return metrics


def evaluate_full_flow_w4_retrieval(
    contract_dir: str | Path,
    observations_path: str | Path,
    *,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Score complete rankings at N1 without emitting relevance identities."""

    root = Path(contract_dir).resolve()
    target = Path(output_dir).resolve()
    _require(not target.exists(), f"W4 evaluation output already exists: {target}")
    public_root, private_root = _contract_roots(root)
    task, bindings, hidden = _verify_contract_roots(public_root, private_root)
    observation_raw, observations = _read_json(
        Path(observations_path).resolve(), "W4 observations"
    )
    _strict_fields(
        observations,
        {"schema_version", "contract_id", "observations", "credentials_recorded"},
        "W4 observations",
    )
    _require(
        observations.get("schema_version")
        == W4_RETRIEVAL_OBSERVATIONS_SCHEMA_VERSION,
        "unsupported W4 observation schema_version",
    )
    _require(
        observations.get("contract_id") == task["contract_id"],
        "observation contract mismatch",
    )
    _require(
        observations.get("credentials_recorded") is False,
        "observations record credentials",
    )
    raw_rows = observations.get("observations")
    _require(isinstance(raw_rows, list), "observations must be an array")
    binding_by_key = {row["trial_key"]: row for row in bindings}
    _require(len(raw_rows) == len(binding_by_key), "observation count is incomplete")
    candidate_ids = [row["object_id"] for row in task["candidate_objects"]]
    candidate_set = set(candidate_ids)
    relevant = set(hidden["relevant_object_ids"])
    top_k = task["quality_metrics"]["top_k"]
    scored: list[dict[str, Any]] = []
    seen: set[str] = set()
    for position, raw_row in enumerate(raw_rows):
        _require(
            isinstance(raw_row, Mapping),
            f"observation {position} must be an object",
        )
        _strict_fields(
            raw_row,
            {
                "trial_key",
                "retrieval_task_binding_sha256",
                "ranked_object_ids",
                "outcome_type",
                "telemetry_complete",
            },
            "W4 observation",
        )
        trial_key = _identifier(raw_row.get("trial_key"), "observation trial_key")
        _require(trial_key in binding_by_key, f"unknown W4 trial: {trial_key}")
        _require(trial_key not in seen, f"duplicate W4 observation: {trial_key}")
        seen.add(trial_key)
        _require(
            raw_row.get("retrieval_task_binding_sha256") == task["task_binding_sha256"],
            "observation task binding mismatch",
        )
        ranking = raw_row.get("ranked_object_ids")
        _require(
            isinstance(ranking, list)
            and ranking == list(dict.fromkeys(ranking))
            and len(ranking) == len(candidate_ids)
            and set(ranking) == candidate_set,
            "observation must rank every candidate exactly once",
        )
        _require(
            raw_row.get("outcome_type") == "completed",
            "observation is not completed",
        )
        _require(
            raw_row.get("telemetry_complete") is True,
            "observation telemetry is incomplete",
        )
        metrics = _score_ranking(ranking, relevant, top_k)
        binding = binding_by_key[trial_key]
        scored.append(
            {
                "trial_key": trial_key,
                "order_index": binding["order_index"],
                "design_id": binding["design_id"],
                "repetition": binding["repetition"],
                "ranking_sha256": _ranking_sha256(ranking),
                **metrics,
            }
        )
    scored.sort(key=lambda row: row["order_index"])

    def aggregate(rows: list[dict[str, Any]], scope: str) -> dict[str, Any]:
        result: dict[str, Any] = {
            "scope": scope,
            "trial_count": len(rows),
            "mrr": mean(row["reciprocal_rank"] for row in rows),
        }
        for value in top_k:
            result[f"mean_recall_at_{value}"] = mean(
                row[f"recall_at_{value}"] for row in rows
            )
            result[f"mean_hit_at_{value}"] = mean(
                bool(row[f"hit_at_{value}"]) for row in rows
            )
            result[f"mean_ndcg_at_{value}"] = mean(
                row[f"ndcg_at_{value}"] for row in rows
            )
        return result

    aggregates = [aggregate(scored, "overall")]
    for design_id in sorted({row["design_id"] for row in scored}):
        aggregates.append(
            aggregate(
                [row for row in scored if row["design_id"] == design_id],
                f"design:{design_id}",
            )
        )
    row_bytes = _jsonl_bytes(scored)
    report = {
        "schema_version": W4_RETRIEVAL_EVALUATION_SCHEMA_VERSION,
        "status": "COMPLETE",
        "contract_id": task["contract_id"],
        "query_id": task["query_id"],
        "trial_count": len(scored),
        "candidate_object_count": len(candidate_ids),
        "quality_semantics": "ranked-retrieval-not-multiple-choice-task-success",
        "aggregates": aggregates,
        "hidden_relevance_values_returned": False,
        "source_sha256": {
            "observations": _sha256(observation_raw),
            "retrieval_task": _sha256(
                (root / PUBLIC_DIRECTORY_NAME / PUBLIC_TASK_NAME).read_bytes()
            ),
            "hidden_relevance_commitment": _sha256(
                (root / PRIVATE_DIRECTORY_NAME / PRIVATE_RELEVANCE_NAME).read_bytes()
            ),
        },
        "output_sha256": {EVALUATION_ROWS_NAME: _sha256(row_bytes)},
        "external_services_called": False,
        "llm_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    documents = {
        EVALUATION_ROWS_NAME: row_bytes,
        EVALUATION_NAME: _json_bytes(report),
    }
    target.parent.mkdir(parents=True, exist_ok=True)
    stage_parent = Path(tempfile.mkdtemp(prefix=".w4-evaluation-", dir=target.parent))
    stage = stage_parent / "evaluation"
    try:
        stage.mkdir()
        for name, content in documents.items():
            (stage / name).write_bytes(content)
        (stage / CHECKSUMS_NAME).write_bytes(_checksum_bytes(documents))
        _verify_evaluation(stage)
        os.replace(stage, target)
    finally:
        shutil.rmtree(stage_parent, ignore_errors=True)
    return {
        "status": "COMPLETE",
        "contract_id": task["contract_id"],
        "trial_count": len(scored),
        "candidate_object_count": len(candidate_ids),
        "output_dir": str(target),
        "hidden_relevance_values_returned": False,
        "eligible_for_scientific_claims": False,
    }


def _verify_evaluation(root: Path) -> dict[str, Any]:
    sums = _verify_checksums(root, _EVALUATION_FILES)
    report_raw, report = _read_json(root / EVALUATION_NAME, "W4 evaluation")
    _require(
        report_raw == _json_bytes(report),
        "W4 evaluation report is not canonical",
    )
    _strict_fields(
        report,
        {
            "schema_version",
            "status",
            "contract_id",
            "query_id",
            "trial_count",
            "candidate_object_count",
            "quality_semantics",
            "aggregates",
            "hidden_relevance_values_returned",
            "source_sha256",
            "output_sha256",
            "external_services_called",
            "llm_called",
            "credentials_recorded",
            "eligible_for_scientific_claims",
        },
        "W4 evaluation report",
    )
    _require(
        report.get("schema_version") == W4_RETRIEVAL_EVALUATION_SCHEMA_VERSION
        and report.get("status") == "COMPLETE",
        "W4 evaluation schema or status changed",
    )
    _require(
        report.get("output_sha256")
        == {EVALUATION_ROWS_NAME: sums[EVALUATION_ROWS_NAME]},
        "W4 evaluation output binding changed",
    )
    _identifier(report.get("contract_id"), "evaluation contract_id")
    _identifier(report.get("query_id"), "evaluation query_id")
    _require(report.get("trial_count") == 16, "W4 evaluation trial count changed")
    _require(
        type(report.get("candidate_object_count")) is int
        and report["candidate_object_count"] >= 2,
        "W4 evaluation candidate count is invalid",
    )
    _require(
        report.get("quality_semantics")
        == "ranked-retrieval-not-multiple-choice-task-success",
        "W4 evaluation quality semantics changed",
    )
    _require(
        report.get("hidden_relevance_values_returned") is False,
        "evaluation leaks relevance",
    )
    _require(
        report.get("credentials_recorded") is False,
        "evaluation records credentials",
    )
    _require(
        report.get("eligible_for_scientific_claims") is False,
        "evaluation claim class changed",
    )
    for field in ("external_services_called", "llm_called"):
        _require(report.get(field) is False, f"{field} must be false")
    sources = report.get("source_sha256")
    _require(
        isinstance(sources, Mapping)
        and set(sources)
        == {
            "observations",
            "retrieval_task",
            "hidden_relevance_commitment",
        },
        "W4 evaluation source bindings changed",
    )
    for label, value in sources.items():
        _digest(value, f"{label} SHA")
    rows_raw, rows = _read_jsonl(
        root / EVALUATION_ROWS_NAME, "W4 metric rows"
    )
    _require(
        rows_raw == _jsonl_bytes(rows),
        "W4 metric rows are not canonical",
    )
    _require(len(rows) == report.get("trial_count"), "W4 metric row count mismatch")
    forbidden = {
        "relevant_object_ids",
        "ranked_object_ids",
        "source_object_group",
    }
    _require(
        not any(forbidden & set(row) for row in rows),
        "W4 public metric rows contain hidden or raw ranking values",
    )
    first = rows[0]
    recall_keys = sorted(
        (key for key in first if key.startswith("recall_at_")),
        key=lambda key: int(key.rsplit("_", 1)[-1]),
    )
    top_k = [int(key.rsplit("_", 1)[-1]) for key in recall_keys]
    _require(bool(top_k), "W4 metric rows contain no recall metrics")
    metric_fields = {
        "trial_key",
        "order_index",
        "design_id",
        "repetition",
        "ranking_sha256",
        "reciprocal_rank",
        *recall_keys,
        *[f"hit_at_{value}" for value in top_k],
        *[f"ndcg_at_{value}" for value in top_k],
    }
    for row in rows:
        _strict_fields(row, metric_fields, "W4 metric row")
        _identifier(row.get("trial_key"), "metric trial_key")
        _digest(row.get("ranking_sha256"), "ranking SHA")
        for key in {"reciprocal_rank", *recall_keys, *[
            f"ndcg_at_{value}" for value in top_k
        ]}:
            metric = row.get(key)
            _require(
                type(metric) in (int, float)
                and math.isfinite(float(metric))
                and 0.0 <= float(metric) <= 1.0,
                f"metric {key} is outside [0, 1]",
            )
        for value in top_k:
            _require(
                type(row.get(f"hit_at_{value}")) is bool,
                f"hit_at_{value} must be boolean",
            )
    _require(
        {(row["design_id"], row["repetition"]) for row in rows}
        == {
            (f"D{design}", repetition)
            for design in range(8)
            for repetition in range(2)
        },
        "W4 evaluation matrix coverage changed",
    )

    def aggregate(group: list[dict[str, Any]], scope: str) -> dict[str, Any]:
        value: dict[str, Any] = {
            "scope": scope,
            "trial_count": len(group),
            "mrr": mean(row["reciprocal_rank"] for row in group),
        }
        for top in top_k:
            value[f"mean_recall_at_{top}"] = mean(
                row[f"recall_at_{top}"] for row in group
            )
            value[f"mean_hit_at_{top}"] = mean(
                row[f"hit_at_{top}"] for row in group
            )
            value[f"mean_ndcg_at_{top}"] = mean(
                row[f"ndcg_at_{top}"] for row in group
            )
        return value

    expected_aggregates = [aggregate(rows, "overall")]
    for design_id in sorted({row["design_id"] for row in rows}):
        expected_aggregates.append(
            aggregate(
                [row for row in rows if row["design_id"] == design_id],
                f"design:{design_id}",
            )
        )
    _require(
        report.get("aggregates") == expected_aggregates,
        "W4 evaluation aggregates do not match metric rows",
    )
    return report


def verify_full_flow_w4_retrieval_evaluation(
    output_dir: str | Path,
    *,
    contract_dir: str | Path | None = None,
    observations_path: str | Path | None = None,
) -> dict[str, Any]:
    """Verify output integrity, optionally replaying its exact private inputs."""

    root = Path(output_dir).resolve()
    report = _verify_evaluation(root)
    _require(
        (contract_dir is None) == (observations_path is None),
        "source-bound verification requires both contract and observations",
    )
    source_bound = contract_dir is not None
    if source_bound:
        with tempfile.TemporaryDirectory(prefix="pathfinder-w4-verify-") as temp:
            reproduced = Path(temp) / "evaluation"
            evaluate_full_flow_w4_retrieval(
                Path(contract_dir).resolve(),
                Path(observations_path).resolve(),
                output_dir=reproduced,
            )
            _require(
                {
                    path.name: path.read_bytes()
                    for path in reproduced.iterdir()
                    if path.is_file()
                }
                == {
                    path.name: path.read_bytes()
                    for path in root.iterdir()
                    if path.is_file()
                },
                "W4 evaluation differs from source-bound replay",
            )
    return {
        "status": "VERIFIED_SOURCE_BOUND" if source_bound else "VERIFIED_INTEGRITY",
        "contract_id": report["contract_id"],
        "trial_count": report["trial_count"],
        "candidate_object_count": report["candidate_object_count"],
        "hidden_relevance_values_returned": False,
        "source_bound_replay_performed": source_bound,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "FullFlowW4RetrievalContractError",
    "W4_RETRIEVAL_COMMITMENT_SCHEMA_VERSION",
    "W4_RETRIEVAL_EVALUATION_SCHEMA_VERSION",
    "W4_RETRIEVAL_HIDDEN_SOURCE_SCHEMA_VERSION",
    "W4_RETRIEVAL_OBSERVATIONS_SCHEMA_VERSION",
    "W4_RETRIEVAL_ORACLE_SCHEMA_VERSION",
    "W4_RETRIEVAL_TASK_SCHEMA_VERSION",
    "W4_RETRIEVAL_TRIAL_BINDING_SCHEMA_VERSION",
    "evaluate_full_flow_w4_retrieval",
    "freeze_full_flow_w4_retrieval_contract",
    "verify_full_flow_w4_retrieval_contract",
    "verify_full_flow_w4_retrieval_evaluation",
]
