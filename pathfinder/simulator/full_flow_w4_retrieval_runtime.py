"""Public-only runtime bridge for the prospective W4 retrieval overlay.

The frozen 64-trial semantic matrix still models W4 as a single-object
multiple-choice task.  The separate W4 retrieval contract, by contrast,
defines a public multi-object ranking task and keeps relevance judgements at
N1.  This module connects that *public* contract to a ranker runtime without
rewriting or relabelling the historical matrix.

The bridge deliberately stops at the ranking boundary.  It verifies and
executes a ranker adapter for all sixteen W4 design/repetition bindings and
emits the exact observation document consumed by the existing N1 evaluator.
It does not claim that the current single-object physical stage DAG expanded
and accessed every candidate.  A candidate-wide physical route compiler and
complete per-candidate representation bindings remain separate prerequisites
for a D0--D7 physical retrieval comparison.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence, runtime_checkable

from ._full_flow_primitives import (
    LOWER_SHA256_PATTERN as _SHA256,
    canonical_json_bytes,
    canonical_json_lines_bytes,
    checked_identifier,
    checked_lower_sha256,
    pretty_json_bytes,
    sha256_hex,
    strict_json_loads,
)
from .full_flow_local_semantic_admission import (
    TRIALS_NAME as LOCAL_TRIALS_NAME,
    load_full_flow_local_semantic_execution_inputs,
    verify_full_flow_local_semantic_runtime_package,
)
from .full_flow_w4_retrieval_contract import (
    PUBLIC_BINDINGS_NAME,
    PUBLIC_COMMITMENT_NAME,
    PUBLIC_DIRECTORY_NAME,
    PUBLIC_TASK_NAME,
    W4_RETRIEVAL_COMMITMENT_SCHEMA_VERSION,
    W4_RETRIEVAL_OBSERVATIONS_SCHEMA_VERSION,
    W4_RETRIEVAL_TASK_SCHEMA_VERSION,
    W4_RETRIEVAL_TRIAL_BINDING_SCHEMA_VERSION,
)
from .index_service import (
    INDEX_ALGORITHM,
    INDEX_PUBLIC_QUERY_RESULT_SCHEMA_VERSION,
    INDEX_TOKENIZER,
    build_n2_index_query_request,
    verify_n2_index_package,
    verify_n2_public_index_query_result,
)


W4_RETRIEVAL_RUNTIME_PLAN_SCHEMA_VERSION = (
    "pathfinder.full-flow-w4-retrieval-runtime-plan/v1alpha1"
)
W4_RETRIEVAL_RUNTIME_TRIAL_SCHEMA_VERSION = (
    "pathfinder.full-flow-w4-retrieval-runtime-trial/v1alpha1"
)
W4_RETRIEVAL_RANKER_RESULT_SCHEMA_VERSION = (
    "pathfinder.full-flow-w4-retrieval-ranker-result/v1alpha1"
)
W4_RETRIEVAL_RUNTIME_RUN_SCHEMA_VERSION = (
    "pathfinder.full-flow-w4-retrieval-runtime-run/v1alpha1"
)
W4_RETRIEVAL_RUNTIME_EVIDENCE_SCHEMA_VERSION = (
    "pathfinder.full-flow-w4-retrieval-runtime-evidence/v1alpha1"
)

RUNTIME_PLAN_NAME = "w4-retrieval-runtime-plan.json"
RUNTIME_TASK_NAME = "w4-retrieval-runtime-task.json"
RUNTIME_TRIALS_NAME = "w4-retrieval-runtime-trials.jsonl"
OBSERVATIONS_NAME = "w4-retrieval-observations.json"
RUNTIME_EVIDENCE_NAME = "w4-retrieval-runtime-evidence.jsonl"
RUNTIME_RUN_NAME = "w4-retrieval-runtime-run.json"
CHECKSUMS_NAME = "SHA256SUMS"

_PACKAGE_FILES = frozenset(
    {RUNTIME_PLAN_NAME, RUNTIME_TASK_NAME, RUNTIME_TRIALS_NAME}
)
_RUN_FILES = frozenset(
    {OBSERVATIONS_NAME, RUNTIME_EVIDENCE_NAME, RUNTIME_RUN_NAME}
)
_ROUTE_FAMILY_BY_DESIGN = {
    "D0": "raw",
    "D1": "indexed-raw",
    "D2": "remote-derived",
    "D3": "local-cache-derived",
    "D4": "raw",
    "D5": "indexed-raw",
    "D6": "remote-derived",
    "D7": "local-cache-derived",
}
_EXECUTOR_BY_DESIGN = {
    **{design: "N7" for design in ("D0", "D1", "D2", "D3")},
    **{design: "N8" for design in ("D4", "D5", "D6", "D7")},
}
W4_INDEX_QUERY_MAX_CANDIDATES = 1_000


class FullFlowW4RetrievalRuntimeError(ValueError):
    """Raised when the public W4 runtime boundary is ambiguous or unsafe."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowW4RetrievalRuntimeError(message)


def _read_json(path: Path, label: str) -> tuple[bytes, dict[str, Any]]:
    try:
        raw = path.read_bytes()
        value = strict_json_loads(
            raw.decode("utf-8"),
            error_type=FullFlowW4RetrievalRuntimeError,
            duplicate_key_message=lambda key: f"duplicate JSON key: {key}",
            nonfinite_number_message=(
                lambda token: f"non-finite JSON number: {token}"
            ),
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowW4RetrievalRuntimeError(
            f"cannot read valid {label}: {path}"
        ) from exc
    _require(isinstance(value, dict), f"{label} must be an object")
    return raw, value


def _read_jsonl(path: Path, label: str) -> tuple[bytes, list[dict[str, Any]]]:
    try:
        raw = path.read_bytes()
        lines = raw.decode("utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FullFlowW4RetrievalRuntimeError(
            f"cannot read valid {label}: {path}"
        ) from exc
    rows: list[dict[str, Any]] = []
    for position, line in enumerate(lines):
        _require(bool(line), f"{label} contains a blank line")
        try:
            value = strict_json_loads(
                line,
                error_type=FullFlowW4RetrievalRuntimeError,
                duplicate_key_message=lambda key: f"duplicate JSON key: {key}",
                nonfinite_number_message=(
                    lambda token: f"non-finite JSON number: {token}"
                ),
            )
        except json.JSONDecodeError as exc:
            raise FullFlowW4RetrievalRuntimeError(
                f"invalid {label} row {position}"
            ) from exc
        _require(isinstance(value, dict), f"{label} row is not an object")
        rows.append(value)
    _require(bool(rows), f"{label} is empty")
    return raw, rows


def _json_bytes(value: Any) -> bytes:
    return pretty_json_bytes(value)


def _canonical(value: Any) -> bytes:
    return canonical_json_bytes(value)


def _jsonl_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    return canonical_json_lines_bytes(rows)


def _sha256(value: bytes) -> str:
    return sha256_hex(value)


def _digest(value: Any, label: str) -> str:
    return checked_lower_sha256(
        value,
        label,
        error_type=FullFlowW4RetrievalRuntimeError,
        message=f"{label} is not a lowercase SHA-256 digest",
    )


def _identifier(value: Any, label: str) -> str:
    return checked_identifier(
        value,
        label,
        error_type=FullFlowW4RetrievalRuntimeError,
    )


def _strict_fields(
    value: Mapping[str, Any], expected: set[str], label: str
) -> None:
    _require(set(value) == expected, f"{label} fields changed")


def _checksum_bytes(documents: Mapping[str, bytes]) -> bytes:
    return b"".join(
        f"{_sha256(documents[name])}  {name}\n".encode("utf-8")
        for name in sorted(documents)
    )


def _verify_checksums(root: Path, expected: frozenset[str]) -> dict[str, str]:
    _require(root.is_dir() and not root.is_symlink(), "output root is invalid")
    entries = list(root.iterdir())
    _require(
        all(path.is_file() and not path.is_symlink() for path in entries),
        "output contains a non-regular file",
    )
    _require(
        {path.name for path in entries} == expected | {CHECKSUMS_NAME},
        "output file set changed",
    )
    try:
        lines = (root / CHECKSUMS_NAME).read_text(
            encoding="utf-8"
        ).splitlines()
    except OSError as exc:
        raise FullFlowW4RetrievalRuntimeError(
            "cannot read SHA256SUMS"
        ) from exc
    found: dict[str, str] = {}
    for line in lines:
        digest, separator, name = line.partition("  ")
        _require(
            separator == "  " and name in expected and name not in found,
            "malformed SHA256SUMS",
        )
        _digest(digest, "checksum")
        _require(
            _sha256((root / name).read_bytes()) == digest,
            f"checksum mismatch: {name}",
        )
        found[name] = digest
    _require(set(found) == set(expected), "checksums are incomplete")
    return found


def _publish(target: Path, documents: Mapping[str, bytes]) -> None:
    _require(not target.exists(), f"output directory already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    parent = Path(tempfile.mkdtemp(prefix=".w4-runtime-", dir=target.parent))
    stage = parent / "output"
    try:
        stage.mkdir()
        for name, content in documents.items():
            path = stage / name
            with path.open("wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
        (stage / CHECKSUMS_NAME).write_bytes(_checksum_bytes(documents))
        _verify_checksums(stage, frozenset(documents))
        os.replace(stage, target)
    finally:
        shutil.rmtree(parent, ignore_errors=True)


def _require_disjoint_output(target: Path, sources: Sequence[Path]) -> None:
    for source in sources:
        _require(
            target != source
            and not target.is_relative_to(source)
            and not source.is_relative_to(target),
            f"output directory overlaps an input: {source}",
        )


def _validate_task(task: Mapping[str, Any]) -> dict[str, Any]:
    _strict_fields(
        task,
        {
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
        },
        "W4 runtime task",
    )
    _require(
        task.get("schema_version") == W4_RETRIEVAL_TASK_SCHEMA_VERSION
        and task.get("workload_class") == "W4"
        and task.get("task_class_id") == "video_retrieval",
        "W4 runtime task semantics changed",
    )
    for key in ("contract_id", "workload_id", "retrieval_id", "query_id"):
        _identifier(task.get(key), key)
    _require(
        isinstance(task.get("query_text"), str)
        and task["query_text"] == task["query_text"].strip()
        and bool(task["query_text"])
        and len(task["query_text"].encode("utf-8")) <= 64 * 1024,
        "W4 query text is invalid",
    )
    _require(
        task.get("candidate_corpus")
        == "all-representation-manifest-objects",
        "W4 candidate corpus changed",
    )
    candidates = task.get("candidate_objects")
    _require(
        isinstance(candidates, list) and len(candidates) >= 2,
        "W4 runtime candidate set is not multi-object",
    )
    candidate_ids: list[str] = []
    for candidate in candidates:
        _require(isinstance(candidate, Mapping), "candidate is not an object")
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
        candidate_ids.append(
            _identifier(candidate.get("object_id"), "candidate object_id")
        )
        _require(
            candidate.get("representation_id") == "multimodal_digest",
            "candidate representation changed",
        )
        _digest(candidate.get("artifact_sha256"), "candidate artifact SHA")
        _require(
            type(candidate.get("artifact_size_bytes")) is int
            and candidate["artifact_size_bytes"] > 0,
            "candidate artifact size is invalid",
        )
    _require(
        candidate_ids == sorted(set(candidate_ids)),
        "candidate IDs are not sorted and unique",
    )
    _require(
        task.get("candidate_set_sha256") == _sha256(_canonical(candidates))
        and task.get("required_ranking_length") == len(candidates),
        "candidate set binding changed",
    )
    metrics = task.get("quality_metrics")
    _require(isinstance(metrics, Mapping), "W4 quality metrics are missing")
    _strict_fields(
        metrics,
        {"per_query", "aggregate", "top_k", "relevance"},
        "W4 quality metrics",
    )
    top_k = metrics.get("top_k")
    _require(
        isinstance(top_k, list)
        and bool(top_k)
        and top_k == sorted(set(top_k))
        and all(
            type(value) is int and 0 < value <= len(candidates)
            for value in top_k
        )
        and metrics.get("relevance") == "binary",
        "W4 quality metric contract is invalid",
    )
    _require(
        metrics.get("per_query")
        == [
            "reciprocal_rank",
            *[f"recall_at_{value}" for value in top_k],
            *[f"hit_at_{value}" for value in top_k],
            *[f"ndcg_at_{value}" for value in top_k],
        ]
        and metrics.get("aggregate")
        == [
            "mrr",
            *[f"mean_recall_at_{value}" for value in top_k],
            *[f"mean_hit_at_{value}" for value in top_k],
            *[f"mean_ndcg_at_{value}" for value in top_k],
        ],
        "W4 quality metric names changed",
    )
    _require(
        task.get("relevance_values_included") is False
        and task.get("source_object_group_included") is False
        and task.get("credentials_recorded") is False,
        "public W4 task includes private or credential data",
    )
    supplied = _digest(task.get("task_binding_sha256"), "task binding")
    core = dict(task)
    del core["task_binding_sha256"]
    _require(supplied == _sha256(_canonical(core)), "task binding mismatch")
    return dict(task)


def validate_full_flow_w4_public_runtime_task(
    task: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate and copy the label-free W4 task at an adapter boundary."""

    return _validate_task(task)


def _validate_runtime_trial(row: Mapping[str, Any]) -> dict[str, Any]:
    _strict_fields(
        row,
        {
            "schema_version",
            "runtime_overlay_id",
            "contract_id",
            "trial_key",
            "order_index",
            "design_id",
            "repetition",
            "route_family",
            "executor_node_id",
            "retrieval_task_binding_sha256",
            "candidate_set_sha256",
            "candidate_object_count",
            "replaces_multiple_choice_task_binding_sha256",
            "source_local_semantic_trial_sha256",
            "source_bound_stage_set_sha256",
            "runtime_semantics",
            "current_single_object_stage_dag_used_as_retrieval_evidence",
            "credentials_recorded",
        },
        "W4 runtime trial",
    )
    _require(
        row.get("schema_version") == W4_RETRIEVAL_RUNTIME_TRIAL_SCHEMA_VERSION,
        "unsupported W4 runtime trial schema",
    )
    for key in ("runtime_overlay_id", "contract_id", "trial_key"):
        _identifier(row.get(key), key)
    _require(row.get("design_id") in {f"D{i}" for i in range(8)}, "bad design")
    _require(row.get("repetition") in {0, 1}, "bad repetition")
    design_id = str(row["design_id"])
    _require(
        row.get("executor_node_id") == _EXECUTOR_BY_DESIGN[design_id],
        "W4 runtime executor differs from the frozen design semantics",
    )
    _require(
        row.get("route_family") == _ROUTE_FAMILY_BY_DESIGN[design_id],
        "W4 runtime route family differs from the frozen design semantics",
    )
    for key in (
        "retrieval_task_binding_sha256",
        "candidate_set_sha256",
        "replaces_multiple_choice_task_binding_sha256",
        "source_local_semantic_trial_sha256",
        "source_bound_stage_set_sha256",
    ):
        _digest(row.get(key), key)
    _require(
        type(row.get("candidate_object_count")) is int
        and row["candidate_object_count"] >= 2,
        "candidate count is invalid",
    )
    _require(
        row.get("runtime_semantics") == "public-multi-object-ranking"
        and row.get(
            "current_single_object_stage_dag_used_as_retrieval_evidence"
        )
        is False,
        "W4 runtime semantics boundary changed",
    )
    _require(row.get("credentials_recorded") is False, "trial records credentials")
    return dict(row)


def _verify_public_contract_half(
    contract_dir: Path,
) -> tuple[
    bytes,
    dict[str, Any],
    bytes,
    list[dict[str, Any]],
    bytes,
    dict[str, Any],
]:
    """Verify only the public W4 directory; never enumerate N1-private files."""

    public_root = contract_dir / PUBLIC_DIRECTORY_NAME
    checksums = _verify_checksums(
        public_root,
        frozenset({PUBLIC_TASK_NAME, PUBLIC_BINDINGS_NAME, PUBLIC_COMMITMENT_NAME}),
    )
    task_raw, task_value = _read_json(public_root / PUBLIC_TASK_NAME, "W4 task")
    task = _validate_task(task_value)
    _require(task_raw == _json_bytes(task), "W4 task is not canonical")
    bindings_raw, bindings = _read_jsonl(
        public_root / PUBLIC_BINDINGS_NAME, "W4 trial bindings"
    )
    _require(
        bindings_raw == _jsonl_bytes(bindings),
        "W4 trial bindings are not canonical",
    )
    binding_keys: set[str] = set()
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
            "W4 public trial binding",
        )
        key = _identifier(row.get("trial_key"), "trial_key")
        _require(key not in binding_keys, "duplicate W4 public trial binding")
        binding_keys.add(key)
        _require(
            row.get("schema_version")
            == W4_RETRIEVAL_TRIAL_BINDING_SCHEMA_VERSION
            and row.get("contract_id") == task["contract_id"]
            and row.get("query_id") == task["query_id"]
            and row.get("retrieval_task_binding_sha256")
            == task["task_binding_sha256"]
            and row.get("design_id") in _ROUTE_FAMILY_BY_DESIGN
            and row.get("repetition") in {0, 1}
            and type(row.get("order_index")) is int
            and row["order_index"] >= 0
            and row.get("credentials_recorded") is False,
            "W4 public trial binding semantics changed",
        )
        _digest(
            row.get("replaces_multiple_choice_task_binding_sha256"),
            "replaced MCQ task binding",
        )
    _require(
        {(row["design_id"], row["repetition"]) for row in bindings}
        == {(f"D{index}", repetition) for index in range(8) for repetition in (0, 1)},
        "W4 public trial matrix coverage changed",
    )
    _require(
        [row["order_index"] for row in bindings]
        == sorted(row["order_index"] for row in bindings)
        and len({row["order_index"] for row in bindings}) == len(bindings),
        "W4 public trial bindings are not uniquely ordered",
    )
    commitment_raw, commitment = _read_json(
        public_root / PUBLIC_COMMITMENT_NAME, "W4 public commitment"
    )
    _require(
        commitment_raw == _json_bytes(commitment),
        "W4 public commitment is not canonical",
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
        "W4 public commitment",
    )
    sources = commitment.get("source_sha256")
    _require(
        commitment.get("schema_version")
        == W4_RETRIEVAL_COMMITMENT_SCHEMA_VERSION
        and commitment.get("status") == "FROZEN_PUBLIC_W4_RETRIEVAL_CONTRACT"
        and commitment.get("contract_id") == task["contract_id"]
        and commitment.get("retrieval_task_binding_sha256")
        == task["task_binding_sha256"]
        and commitment.get("candidate_set_sha256")
        == task["candidate_set_sha256"]
        and commitment.get("candidate_object_count")
        == len(task["candidate_objects"])
        and commitment.get("w4_trial_count") == len(bindings) == 16
        and commitment.get("output_sha256")
        == {
            PUBLIC_TASK_NAME: checksums[PUBLIC_TASK_NAME],
            PUBLIC_BINDINGS_NAME: checksums[PUBLIC_BINDINGS_NAME],
        },
        "W4 public commitment binding changed",
    )
    _digest(commitment.get("matrix_plan_sha256"), "matrix plan SHA-256")
    _digest(commitment.get("hidden_relevance_sha256"), "hidden commitment")
    _identifier(commitment.get("task_plane_id"), "task_plane_id")
    _require(
        isinstance(sources, Mapping)
        and set(sources)
        == {"retrieval_config", "representation_manifest", "answer_observations"}
        and sources.get("answer_observations") is None,
        "W4 public source bindings changed",
    )
    _digest(sources.get("retrieval_config"), "retrieval config SHA-256")
    _digest(
        sources.get("representation_manifest"),
        "representation manifest SHA-256",
    )
    _require(
        commitment.get("annotation_status") == "operator-verified"
        and commitment.get("hidden_relevance_committed") is True
        and commitment.get("relevance_values_included") is False
        and commitment.get("current_matrix_native_w4_semantics")
        == "multiple-choice-placeholder"
        and commitment.get("overlay_required_at_compile_and_runtime") is True
        and commitment.get("runtime_executor_binding_present") is False
        and commitment.get("ready_for_current_64_trial_execution") is False
        and commitment.get("readiness_blocker")
        == "W4_RETRIEVAL_OVERLAY_NOT_YET_CONSUMED_BY_RUNTIME"
        and commitment.get("external_services_called") is False
        and commitment.get("llm_called") is False
        and commitment.get("credentials_recorded") is False
        and commitment.get("eligible_for_scientific_claims") is False,
        "W4 public commitment claim boundary changed",
    )
    return (
        task_raw,
        task,
        bindings_raw,
        bindings,
        commitment_raw,
        commitment,
    )


def _documents(
    contract_dir: Path,
    local_semantic_admission_dir: Path,
    runtime_overlay_id: str,
) -> dict[str, bytes]:
    overlay_id = _identifier(runtime_overlay_id, "runtime_overlay_id")
    local_report = verify_full_flow_local_semantic_runtime_package(
        local_semantic_admission_dir
    )
    (
        task_raw,
        task,
        bindings_raw,
        bindings,
        commitment_raw,
        commitment,
    ) = _verify_public_contract_half(contract_dir)
    _require(
        commitment.get("runtime_executor_binding_present") is False
        and commitment.get("ready_for_current_64_trial_execution") is False,
        "source W4 contract no longer has the expected fail-closed boundary",
    )

    local = load_full_flow_local_semantic_execution_inputs(
        local_semantic_admission_dir
    )
    local_w4 = {
        row["trial_key"]: row
        for row in local.bound_trials
        if row.get("workload_class") == "W4"
    }
    _require(len(local_w4) == 16, "local semantic package lacks sixteen W4 trials")
    runtime_trials: list[dict[str, Any]] = []
    for binding in bindings:
        _require(
            binding.get("schema_version")
            == W4_RETRIEVAL_TRIAL_BINDING_SCHEMA_VERSION,
            "W4 trial binding schema changed",
        )
        trial_key = _identifier(binding.get("trial_key"), "trial_key")
        source = local_w4.get(trial_key)
        _require(source is not None, "W4 overlay trial is absent from local runtime")
        _require(
            binding.get("design_id") == source.get("design_id")
            and binding.get("repetition") == source.get("repetition")
            and binding.get("order_index") == source.get("order_index"),
            "W4 overlay and local trial coordinates differ",
        )
        _require(
            binding.get("replaces_multiple_choice_task_binding_sha256")
            == source.get("public_task_binding_sha256"),
            "W4 overlay does not replace the bound MCQ task",
        )
        task_binding = source.get("public_task_binding")
        _require(
            isinstance(task_binding, Mapping)
            and isinstance(task_binding.get("answer_options"), list)
            and bool(task_binding["answer_options"]),
            "source W4 trial is no longer the declared MCQ placeholder",
        )
        stage_hashes = source.get("bound_stage_sha256")
        _require(
            isinstance(stage_hashes, list)
            and bool(stage_hashes)
            and all(_SHA256.fullmatch(str(value)) for value in stage_hashes),
            "source W4 stage binding is invalid",
        )
        runtime_trials.append(
            {
                "schema_version": W4_RETRIEVAL_RUNTIME_TRIAL_SCHEMA_VERSION,
                "runtime_overlay_id": overlay_id,
                "contract_id": task["contract_id"],
                "trial_key": trial_key,
                "order_index": source["order_index"],
                "design_id": source["design_id"],
                "repetition": source["repetition"],
                "route_family": source["route_family"],
                "executor_node_id": source["executor_node_id"],
                "retrieval_task_binding_sha256": task[
                    "task_binding_sha256"
                ],
                "candidate_set_sha256": task["candidate_set_sha256"],
                "candidate_object_count": len(task["candidate_objects"]),
                "replaces_multiple_choice_task_binding_sha256": binding[
                    "replaces_multiple_choice_task_binding_sha256"
                ],
                "source_local_semantic_trial_sha256": _sha256(
                    _canonical(source)
                ),
                "source_bound_stage_set_sha256": _sha256(
                    _canonical(stage_hashes)
                ),
                "runtime_semantics": "public-multi-object-ranking",
                "current_single_object_stage_dag_used_as_retrieval_evidence": False,
                "credentials_recorded": False,
            }
        )
    _require(
        len(runtime_trials) == 16
        and [row["order_index"] for row in runtime_trials]
        == sorted(row["order_index"] for row in runtime_trials)
        and {
            (row["design_id"], row["repetition"])
            for row in runtime_trials
        }
        == {(f"D{i}", repetition) for i in range(8) for repetition in range(2)},
        "W4 runtime overlay matrix coverage changed",
    )

    runtime_task_bytes = _json_bytes(task)
    runtime_trial_bytes = _jsonl_bytes(runtime_trials)
    plan: dict[str, Any] = {
        "schema_version": W4_RETRIEVAL_RUNTIME_PLAN_SCHEMA_VERSION,
        "status": "FROZEN_PUBLIC_W4_RANKER_RUNTIME",
        "runtime_overlay_id": overlay_id,
        "contract_id": task["contract_id"],
        "local_semantic_promotion_id": local_report["promotion_id"],
        "local_semantic_admission_sha256": local_report["admission_sha256"],
        "retrieval_task_binding_sha256": task["task_binding_sha256"],
        "candidate_set_sha256": task["candidate_set_sha256"],
        "candidate_object_count": len(task["candidate_objects"]),
        "w4_trial_count": len(runtime_trials),
        "source_sha256": {
            "w4_public_task": _sha256(task_raw),
            "w4_public_trial_bindings": _sha256(bindings_raw),
            "w4_public_commitment": _sha256(commitment_raw),
            "representation_manifest": commitment["source_sha256"][
                "representation_manifest"
            ],
            "local_semantic_trials": _sha256(
                (local_semantic_admission_dir / LOCAL_TRIALS_NAME).read_bytes()
            ),
        },
        "output_sha256": {
            RUNTIME_TASK_NAME: _sha256(runtime_task_bytes),
            RUNTIME_TRIALS_NAME: _sha256(runtime_trial_bytes),
        },
        "runtime_boundary": {
            "public_ranker_adapter_contract_complete": True,
            "n1_private_relevance_required_by_ranker": False,
            "n1_private_relevance_present": False,
            "ranking_observations_accepted_by_n1_evaluator": True,
            "current_64_trial_matrix_modified": False,
            "current_64_trial_matrix_native_w4_semantics": (
                "multiple-choice-placeholder"
            ),
            "current_single_object_stage_dag_expanded_to_candidate_corpus": False,
            "physical_design_retrieval_comparison_ready": False,
            "remaining_physical_prerequisites": [
                "candidate-wide-route-compiler",
                "all-candidate-raw-video-bindings-for-raw-designs",
                "all-candidate-derived-bindings-for-derived-designs",
                "candidate-wide-index-and-exact-range-bindings",
                "route-bound-ranking-executor-evidence",
            ],
        },
        "hidden_relevance_values_included": False,
        "operator_verified_relevance_committed": True,
        "external_services_called": False,
        "llm_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    plan["plan_sha256"] = _sha256(_canonical(plan))
    return {
        RUNTIME_PLAN_NAME: _json_bytes(plan),
        RUNTIME_TASK_NAME: runtime_task_bytes,
        RUNTIME_TRIALS_NAME: runtime_trial_bytes,
    }


def _verify_package(
    root: Path,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    _verify_checksums(root, _PACKAGE_FILES)
    plan_raw, plan = _read_json(root / RUNTIME_PLAN_NAME, "W4 runtime plan")
    task_raw, task_value = _read_json(root / RUNTIME_TASK_NAME, "W4 runtime task")
    trials_raw, trial_values = _read_jsonl(
        root / RUNTIME_TRIALS_NAME, "W4 runtime trials"
    )
    _require(plan_raw == _json_bytes(plan), "W4 runtime plan is not canonical")
    task = _validate_task(task_value)
    _require(task_raw == _json_bytes(task), "W4 runtime task is not canonical")
    trials = [_validate_runtime_trial(row) for row in trial_values]
    _require(
        trials_raw == _jsonl_bytes(trials), "W4 runtime trials are not canonical"
    )
    expected_plan_fields = {
        "schema_version",
        "status",
        "runtime_overlay_id",
        "contract_id",
        "local_semantic_promotion_id",
        "local_semantic_admission_sha256",
        "retrieval_task_binding_sha256",
        "candidate_set_sha256",
        "candidate_object_count",
        "w4_trial_count",
        "source_sha256",
        "output_sha256",
        "runtime_boundary",
        "hidden_relevance_values_included",
        "operator_verified_relevance_committed",
        "external_services_called",
        "llm_called",
        "credentials_recorded",
        "eligible_for_scientific_claims",
        "plan_sha256",
    }
    _strict_fields(plan, expected_plan_fields, "W4 runtime plan")
    _require(
        plan.get("schema_version") == W4_RETRIEVAL_RUNTIME_PLAN_SCHEMA_VERSION
        and plan.get("status") == "FROZEN_PUBLIC_W4_RANKER_RUNTIME",
        "W4 runtime plan schema or status changed",
    )
    for name in ("runtime_overlay_id", "contract_id", "local_semantic_promotion_id"):
        _identifier(plan.get(name), name)
    _digest(
        plan.get("local_semantic_admission_sha256"),
        "local semantic admission SHA-256",
    )
    supplied = _digest(plan.get("plan_sha256"), "plan SHA-256")
    core = dict(plan)
    del core["plan_sha256"]
    _require(supplied == _sha256(_canonical(core)), "plan digest mismatch")
    _require(
        plan.get("contract_id") == task["contract_id"]
        and plan.get("retrieval_task_binding_sha256")
        == task["task_binding_sha256"]
        and plan.get("candidate_set_sha256") == task["candidate_set_sha256"]
        and plan.get("candidate_object_count") == len(task["candidate_objects"])
        and plan.get("w4_trial_count") == len(trials) == 16,
        "W4 runtime task or trial binding changed",
    )
    _require(
        plan.get("output_sha256")
        == {
            RUNTIME_TASK_NAME: _sha256(task_raw),
            RUNTIME_TRIALS_NAME: _sha256(trials_raw),
        },
        "W4 runtime output binding changed",
    )
    source_sha = plan.get("source_sha256")
    _require(
        isinstance(source_sha, Mapping)
        and set(source_sha)
        == {
            "w4_public_task",
            "w4_public_trial_bindings",
            "w4_public_commitment",
            "representation_manifest",
            "local_semantic_trials",
        },
        "W4 runtime source binding changed",
    )
    for label, value in source_sha.items():
        _digest(value, f"{label} SHA-256")
    boundary = plan.get("runtime_boundary")
    _require(
        isinstance(boundary, Mapping),
        "W4 runtime claim boundary is missing",
    )
    _strict_fields(
        boundary,
        {
            "public_ranker_adapter_contract_complete",
            "n1_private_relevance_required_by_ranker",
            "n1_private_relevance_present",
            "ranking_observations_accepted_by_n1_evaluator",
            "current_64_trial_matrix_modified",
            "current_64_trial_matrix_native_w4_semantics",
            "current_single_object_stage_dag_expanded_to_candidate_corpus",
            "physical_design_retrieval_comparison_ready",
            "remaining_physical_prerequisites",
        },
        "W4 runtime claim boundary",
    )
    _require(
        boundary.get("public_ranker_adapter_contract_complete") is True
        and boundary.get("n1_private_relevance_required_by_ranker") is False
        and boundary.get("n1_private_relevance_present") is False
        and boundary.get("ranking_observations_accepted_by_n1_evaluator") is True
        and boundary.get("current_64_trial_matrix_modified") is False
        and boundary.get("current_64_trial_matrix_native_w4_semantics")
        == "multiple-choice-placeholder"
        and boundary.get(
            "current_single_object_stage_dag_expanded_to_candidate_corpus"
        )
        is False
        and boundary.get("physical_design_retrieval_comparison_ready") is False,
        "W4 runtime claim boundary changed",
    )
    _require(
        boundary.get("remaining_physical_prerequisites")
        == [
            "candidate-wide-route-compiler",
            "all-candidate-raw-video-bindings-for-raw-designs",
            "all-candidate-derived-bindings-for-derived-designs",
            "candidate-wide-index-and-exact-range-bindings",
            "route-bound-ranking-executor-evidence",
        ],
        "W4 physical prerequisite declaration changed",
    )
    _require(
        plan.get("hidden_relevance_values_included") is False
        and plan.get("operator_verified_relevance_committed") is True
        and plan.get("external_services_called") is False
        and plan.get("llm_called") is False
        and plan.get("credentials_recorded") is False
        and plan.get("eligible_for_scientific_claims") is False,
        "W4 runtime plan overstates its evidence",
    )
    for position, row in enumerate(trials):
        _require(
            row["order_index"] == sorted(
                trial["order_index"] for trial in trials
            )[position]
            and row["runtime_overlay_id"] == plan["runtime_overlay_id"]
            and row["contract_id"] == plan["contract_id"]
            and row["retrieval_task_binding_sha256"]
            == task["task_binding_sha256"]
            and row["candidate_set_sha256"] == task["candidate_set_sha256"]
            and row["candidate_object_count"] == len(task["candidate_objects"]),
            "W4 runtime trial does not bind the plan",
        )
    _require(
        len({row["trial_key"] for row in trials}) == len(trials)
        and [row["order_index"] for row in trials]
        == sorted(row["order_index"] for row in trials)
        and all(type(row["order_index"]) is int for row in trials)
        and len({row["order_index"] for row in trials}) == len(trials),
        "W4 runtime trial identities or order indices repeat",
    )
    _require(
        len({row["order_index"] for row in trials}) == len(trials),
        "W4 runtime trial order indices repeat",
    )
    _require(
        {(row["design_id"], row["repetition"]) for row in trials}
        == {(f"D{i}", repetition) for i in range(8) for repetition in range(2)},
        "W4 runtime trial coverage changed",
    )
    return plan, task, trials


def freeze_full_flow_w4_retrieval_runtime_overlay(
    contract_dir: str | Path,
    local_semantic_admission_dir: str | Path,
    *,
    runtime_overlay_id: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Bind the public W4 task to all local W4 trial coordinates."""

    contract_root = Path(contract_dir).resolve()
    admission_root = Path(local_semantic_admission_dir).resolve()
    target = Path(output_dir).resolve()
    _require_disjoint_output(target, (contract_root, admission_root))
    documents = _documents(contract_root, admission_root, runtime_overlay_id)
    _publish(target, documents)
    report = verify_full_flow_w4_retrieval_runtime_overlay(target)
    return {
        **report,
        "status": "FROZEN_PUBLIC_W4_RANKER_RUNTIME",
        "output_dir": str(target),
    }


def verify_full_flow_w4_retrieval_runtime_overlay(
    output_dir: str | Path,
) -> dict[str, Any]:
    """Verify a self-contained public runtime package without N1 labels."""

    plan, _task, trials = _verify_package(Path(output_dir).resolve())
    return {
        "status": "VERIFIED_PUBLIC_W4_RANKER_RUNTIME",
        "runtime_overlay_id": plan["runtime_overlay_id"],
        "contract_id": plan["contract_id"],
        "w4_trial_count": len(trials),
        "candidate_object_count": plan["candidate_object_count"],
        "public_ranker_adapter_contract_complete": True,
        "physical_design_retrieval_comparison_ready": False,
        "remaining_physical_prerequisites": list(
            plan["runtime_boundary"]["remaining_physical_prerequisites"]
        ),
        "n1_private_relevance_read": False,
        "current_64_trial_matrix_modified": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


@runtime_checkable
class W4PublicRankingExecutor(Protocol):
    """Adapter seam for a local or remotely scheduled public ranker."""

    def rank(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        public_task: Mapping[str, Any],
    ) -> Mapping[str, Any]: ...


@runtime_checkable
class W4IndexClient(Protocol):
    """Small common surface of an HTTP or in-process N2 index client."""

    def health(self) -> Mapping[str, Any]: ...

    def query_public(self, value: Mapping[str, Any]) -> Mapping[str, Any]: ...


def _public_index_ranking(
    *,
    result: Mapping[str, Any],
    request: Mapping[str, Any],
    health: Mapping[str, Any],
    node_id: str,
    index_id: str,
    candidate_ids: Sequence[str],
) -> tuple[list[dict[str, Any]], str]:
    """Validate one source-group-free public index-result shard."""

    value = dict(result)
    _strict_fields(
        value,
        {
            "schema_version",
            "status",
            "request_id",
            "query_id",
            "node_id",
            "index_id",
            "index_sha256",
            "source_manifest_sha256",
            "request_sha256",
            "query_text_sha256",
            "candidate_set_sha256",
            "ranking_sha256",
            "result_content_sha256",
            "algorithm",
            "tokenizer",
            "query_token_count",
            "candidate_count",
            "top_k",
            "ranked_candidates",
            "lexical_retrieval_executed",
            "llm_called",
            "credentials_recorded",
            "eligible_for_scientific_claims",
        },
        "W4 index result",
    )
    _require(
        value.get("schema_version") == INDEX_PUBLIC_QUERY_RESULT_SCHEMA_VERSION
        and value.get("status") == "COMPLETED"
        and value.get("request_id") == request["request_id"]
        and value.get("query_id") == request["query_id"]
        and value.get("node_id") == node_id
        and value.get("index_id") == index_id
        and value.get("index_sha256") == health["index_sha256"]
        and value.get("request_sha256") == _sha256(_canonical(request))
        and value.get("query_text_sha256")
        == _sha256(str(request["query_text"]).encode("utf-8"))
        and value.get("candidate_set_sha256")
        == _sha256(_canonical(list(candidate_ids)))
        and value.get("candidate_count") == len(candidate_ids)
        and value.get("top_k") == len(candidate_ids)
        and value.get("algorithm") == INDEX_ALGORITHM
        and value.get("tokenizer") == INDEX_TOKENIZER
        and type(value.get("query_token_count")) is int
        and value["query_token_count"] > 0
        and value.get("lexical_retrieval_executed") is True
        and value.get("llm_called") is False
        and value.get("credentials_recorded") is False
        and value.get("eligible_for_scientific_claims") is False,
        "W4 index result is not safely bound to the public request",
    )
    _digest(value.get("source_manifest_sha256"), "source manifest SHA-256")
    ranked = value.get("ranked_candidates")
    _require(isinstance(ranked, list), "W4 index ranking is missing")
    ranking: list[dict[str, Any]] = []
    sort_keys: list[tuple[int, int, str]] = []
    for position, raw in enumerate(ranked, start=1):
        _require(isinstance(raw, Mapping), "W4 index ranking row is invalid")
        row = dict(raw)
        _strict_fields(
            row,
            {
                "rank",
                "object_id",
                "lexical_score_units",
                "matched_terms",
                "visible_fields_sha256",
            },
            "W4 index ranking row",
        )
        object_id = _identifier(row.get("object_id"), "ranked object_id")
        matched_terms = row.get("matched_terms")
        _require(
            row.get("rank") == position
            and type(row.get("lexical_score_units")) is int
            and row["lexical_score_units"] >= 0
            and isinstance(matched_terms, list)
            and matched_terms == sorted(set(matched_terms))
            and all(isinstance(term, str) and bool(term) for term in matched_terms),
            "W4 index ranking row semantics changed",
        )
        _digest(row.get("visible_fields_sha256"), "visible fields SHA-256")
        ranking.append(row)
        sort_keys.append(
            (-len(matched_terms), -row["lexical_score_units"], object_id)
        )
    _require(
        len(ranking) == len(candidate_ids)
        and len({row["object_id"] for row in ranking}) == len(ranking)
        and {row["object_id"] for row in ranking} == set(candidate_ids),
        "W4 index did not return the complete candidate permutation",
    )
    _require(sort_keys == sorted(sort_keys), "W4 index ranking order changed")
    _require(
        value.get("ranking_sha256") == _sha256(_canonical(ranked)),
        "W4 index ranking digest mismatch",
    )
    supplied = _digest(
        value.get("result_content_sha256"),
        "index result content SHA-256",
    )
    core = dict(value)
    del core["result_content_sha256"]
    _require(
        supplied == _sha256(_canonical(core)),
        "W4 index result content digest mismatch",
    )
    return ranking, supplied


class W4LexicalIndexRankingExecutor:
    """Concrete public-ranker adapter over the existing index contract.

    This adapter is intentionally a *ranking-contract* executor, not a
    physical D0--D7 executor.  It queries N7/N8 for the two local-index
    designs and N2 otherwise, including D0/D4 where the physical design is a
    raw scan.  Using N2 for those two coordinates supplies a complete public
    ranking for plumbing and hidden-evaluator tests only; it never establishes
    that the design-specific artifact route ran.  The surrounding runtime
    records exactly that claim boundary.
    """

    def __init__(
        self,
        *,
        clients: Mapping[str, W4IndexClient],
        index_package_dir: str | Path,
        index_id: str,
        index_sha256: str,
        source_manifest_sha256: str,
    ) -> None:
        checked = dict(clients)
        _require(
            set(checked) == {"N2", "N7", "N8"},
            "W4 lexical ranker must bind exactly N2, N7, and N8",
        )
        _require(
            all(isinstance(value, W4IndexClient) for value in checked.values()),
            "W4 lexical ranker received an invalid index client",
        )
        self._clients = checked
        self._index_id = _identifier(index_id, "index_id")
        self._index_sha256 = _digest(index_sha256, "index SHA-256")
        self._source_manifest_sha256 = _digest(
            source_manifest_sha256,
            "source manifest SHA-256",
        )
        package_dir = Path(index_package_dir).resolve()
        try:
            package = verify_n2_index_package(package_dir)
        except Exception as exc:
            raise FullFlowW4RetrievalRuntimeError(
                "W4 lexical ranker index package failed offline verification"
            ) from exc
        _require(
            package.get("index_id") == self._index_id
            and package.get("index_sha256") == self._index_sha256
            and package.get("source_manifest_sha256")
            == self._source_manifest_sha256,
            "W4 lexical ranker identity differs from its frozen index package",
        )
        self._index_package_dir = package_dir

    @staticmethod
    def _node_for_design(design_id: str) -> str:
        if design_id == "D3":
            return "N7"
        if design_id == "D7":
            return "N8"
        return "N2"

    def rank(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        public_task: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        run = _identifier(run_id, "run_id")
        task = _validate_task(public_task)
        design_id = str(trial.get("design_id"))
        _require(design_id in {f"D{i}" for i in range(8)}, "bad W4 design")
        node_id = self._node_for_design(design_id)
        client = self._clients[node_id]
        candidate_ids = [
            str(row["object_id"]) for row in task["candidate_objects"]
        ]
        before = dict(client.health())
        _require(
            before.get("status") == "ok"
            and before.get("node_id") == node_id
            and before.get("index_id") == self._index_id
            and before.get("index_sha256") == self._index_sha256
            and before.get("credentials_recorded") is False,
            f"{node_id} index health is not safely bound",
        )
        requests: list[dict[str, Any]] = []
        result_hashes: list[str] = []
        scored_rows: list[dict[str, Any]] = []
        for shard_index, start in enumerate(
            range(0, len(candidate_ids), W4_INDEX_QUERY_MAX_CANDIDATES)
        ):
            shard = candidate_ids[start : start + W4_INDEX_QUERY_MAX_CANDIDATES]
            request_id = _sha256(
                _canonical({
                    "domain": "pathfinder.w4-public-lexical-ranking-shard/v1",
                    "run_id": run,
                    "trial_key": trial.get("trial_key"),
                    "task_binding_sha256": task["task_binding_sha256"],
                    "node_id": node_id,
                    "index_id": self._index_id,
                    "shard_index": shard_index,
                    "candidate_object_ids": shard,
                })
            )
            request = build_n2_index_query_request(
                request_id=request_id,
                query_id=str(task["query_id"]),
                index_id=self._index_id,
                query_text=str(task["query_text"]),
                top_k=len(shard),
                candidate_object_ids=shard,
                requested_node_id=node_id,
            )
            result = dict(client.query_public(request))
            rows, result_sha = _public_index_ranking(
                result=result,
                request=request,
                health=before,
                node_id=node_id,
                index_id=self._index_id,
                candidate_ids=shard,
            )
            try:
                replay = verify_n2_public_index_query_result(
                    package_dir=self._index_package_dir,
                    request=request,
                    result=result,
                    expected_node_id=node_id,
                )
            except Exception as exc:
                raise FullFlowW4RetrievalRuntimeError(
                    "W4 index result differs from frozen-package offline replay"
                ) from exc
            _require(
                replay.get("status") == "VERIFIED_PUBLIC_PROJECTION"
                and replay.get("result_content_sha256") == result_sha,
                "W4 index replay verification did not bind the public result",
            )
            _require(
                result.get("source_manifest_sha256")
                == self._source_manifest_sha256,
                "W4 index source manifest differs from the public task source",
            )
            requests.append(request)
            result_hashes.append(result_sha)
            scored_rows.extend(rows)
        after = dict(client.health())
        _require(before == after, f"{node_id} index identity changed during query")
        scored_rows.sort(
            key=lambda row: (
                -len(row["matched_terms"]),
                -row["lexical_score_units"],
                row["object_id"],
            )
        )
        ranking = [row["object_id"] for row in scored_rows]
        _require(
            len(ranking) == len(candidate_ids)
            and len(set(ranking)) == len(ranking)
            and set(ranking) == set(candidate_ids),
            "W4 shard merge did not return the complete candidate permutation",
        )
        evidence = {
            "domain": "pathfinder.w4-public-lexical-ranker-evidence/v1",
            "run_id": run,
            "trial_key": trial.get("trial_key"),
            "design_id": design_id,
            "index_node_id": node_id,
            "index_id": self._index_id,
            "request_set_sha256": _sha256(_canonical(requests)),
            "result_content_sha256": result_hashes,
            "query_shard_count": len(requests),
            "max_candidates_per_query": W4_INDEX_QUERY_MAX_CANDIDATES,
            "ranking_sha256": _sha256(_canonical(ranking)),
            "ranking_semantics": "public-lexical-contract-conformance-only",
            "physical_design_route_executed": False,
        }
        return {
            "schema_version": W4_RETRIEVAL_RANKER_RESULT_SCHEMA_VERSION,
            "status": "COMPLETE",
            "trial_key": trial["trial_key"],
            "retrieval_task_binding_sha256": task["task_binding_sha256"],
            "ranked_object_ids": ranking,
            "executor_evidence_sha256": _sha256(_canonical(evidence)),
            "telemetry_complete": True,
            "llm_called": False,
            "flowmesh_workflow_submitted": False,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }


@dataclass(frozen=True)
class FrozenW4RetrievalRuntimeInputs:
    plan: Mapping[str, Any]
    public_task: Mapping[str, Any]
    trials: tuple[Mapping[str, Any], ...]


def load_full_flow_w4_retrieval_runtime_inputs(
    output_dir: str | Path,
) -> FrozenW4RetrievalRuntimeInputs:
    """Load public ranker inputs; the N1-private directory is never read."""

    plan, task, trials = _verify_package(Path(output_dir).resolve())
    return FrozenW4RetrievalRuntimeInputs(
        plan=plan,
        public_task=task,
        trials=tuple(trials),
    )


def _validate_ranker_result(
    value: Mapping[str, Any],
    *,
    trial: Mapping[str, Any],
    task: Mapping[str, Any],
) -> dict[str, Any]:
    _require(isinstance(value, Mapping), "ranker result is not an object")
    _strict_fields(
        value,
        {
            "schema_version",
            "status",
            "trial_key",
            "retrieval_task_binding_sha256",
            "ranked_object_ids",
            "executor_evidence_sha256",
            "telemetry_complete",
            "llm_called",
            "flowmesh_workflow_submitted",
            "credentials_recorded",
            "eligible_for_scientific_claims",
        },
        "W4 ranker result",
    )
    _require(
        value.get("schema_version") == W4_RETRIEVAL_RANKER_RESULT_SCHEMA_VERSION
        and value.get("status") == "COMPLETE",
        "W4 ranker result schema or status changed",
    )
    _require(
        value.get("trial_key") == trial["trial_key"]
        and value.get("retrieval_task_binding_sha256")
        == task["task_binding_sha256"],
        "ranker result binds a different trial or task",
    )
    ranking = value.get("ranked_object_ids")
    candidate_ids = [row["object_id"] for row in task["candidate_objects"]]
    _require(
        isinstance(ranking, list)
        and all(isinstance(item, str) for item in ranking)
        and ranking == list(dict.fromkeys(ranking))
        and len(ranking) == len(candidate_ids)
        and set(ranking) == set(candidate_ids),
        "ranker must return a complete candidate permutation",
    )
    _digest(value.get("executor_evidence_sha256"), "executor evidence SHA-256")
    _require(value.get("telemetry_complete") is True, "ranker telemetry incomplete")
    _require(type(value.get("llm_called")) is bool, "llm_called must be boolean")
    _require(
        type(value.get("flowmesh_workflow_submitted")) is bool,
        "flowmesh_workflow_submitted must be boolean",
    )
    _require(
        value.get("credentials_recorded") is False
        and value.get("eligible_for_scientific_claims") is False,
        "ranker result records credentials or overstates evidence",
    )
    return dict(value)


def run_full_flow_w4_retrieval_ranker(
    runtime_overlay_dir: str | Path,
    *,
    run_id: str,
    executor: W4PublicRankingExecutor,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Execute all public W4 rankings and emit N1-evaluator observations."""

    run = _identifier(run_id, "run_id")
    _require(isinstance(executor, W4PublicRankingExecutor), "invalid ranker")
    source_root = Path(runtime_overlay_dir).resolve()
    source = load_full_flow_w4_retrieval_runtime_inputs(source_root)
    target = Path(output_dir).resolve()
    _require_disjoint_output(target, (source_root,))
    _require(not target.exists(), f"output directory already exists: {target}")
    observations: list[dict[str, Any]] = []
    evidence_rows: list[dict[str, Any]] = []
    any_llm = False
    any_flowmesh = False
    for trial in source.trials:
        result = _validate_ranker_result(
            executor.rank(
                run_id=run,
                trial=trial,
                public_task=source.public_task,
            ),
            trial=trial,
            task=source.public_task,
        )
        ranking = list(result["ranked_object_ids"])
        ranking_sha = _sha256(_canonical(ranking))
        observations.append(
            {
                "trial_key": trial["trial_key"],
                "retrieval_task_binding_sha256": source.public_task[
                    "task_binding_sha256"
                ],
                "ranked_object_ids": ranking,
                "outcome_type": "completed",
                "telemetry_complete": True,
            }
        )
        evidence_rows.append(
            {
                "schema_version": W4_RETRIEVAL_RUNTIME_EVIDENCE_SCHEMA_VERSION,
                "run_id": run,
                "runtime_overlay_id": source.plan["runtime_overlay_id"],
                "trial_key": trial["trial_key"],
                "order_index": trial["order_index"],
                "design_id": trial["design_id"],
                "repetition": trial["repetition"],
                "ranking_sha256": ranking_sha,
                "executor_evidence_sha256": result[
                    "executor_evidence_sha256"
                ],
                "telemetry_complete": True,
                "llm_called": result["llm_called"],
                "flowmesh_workflow_submitted": result[
                    "flowmesh_workflow_submitted"
                ],
                "physical_candidate_route_execution_verified": False,
                "credentials_recorded": False,
                "eligible_for_scientific_claims": False,
            }
        )
        any_llm = any_llm or bool(result["llm_called"])
        any_flowmesh = any_flowmesh or bool(
            result["flowmesh_workflow_submitted"]
        )
    observation_document = {
        "schema_version": W4_RETRIEVAL_OBSERVATIONS_SCHEMA_VERSION,
        "contract_id": source.plan["contract_id"],
        "observations": observations,
        "credentials_recorded": False,
    }
    observation_bytes = _json_bytes(observation_document)
    evidence_bytes = _jsonl_bytes(evidence_rows)
    report: dict[str, Any] = {
        "schema_version": W4_RETRIEVAL_RUNTIME_RUN_SCHEMA_VERSION,
        "status": "COMPLETE",
        "run_id": run,
        "runtime_overlay_id": source.plan["runtime_overlay_id"],
        "runtime_plan_sha256": source.plan["plan_sha256"],
        "contract_id": source.plan["contract_id"],
        "trial_count": len(observations),
        "candidate_object_count": source.plan["candidate_object_count"],
        "ranking_observations_complete": True,
        "ready_for_n1_hidden_relevance_evaluation": True,
        "current_64_trial_matrix_modified": False,
        "physical_candidate_route_execution_verified": False,
        "physical_design_retrieval_comparison_ready": False,
        "quality_claim_scope": "public-ranker-output-only",
        "output_sha256": {
            OBSERVATIONS_NAME: _sha256(observation_bytes),
            RUNTIME_EVIDENCE_NAME: _sha256(evidence_bytes),
        },
        "llm_called": any_llm,
        "flowmesh_workflow_submitted": any_flowmesh,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    report["run_sha256"] = _sha256(_canonical(report))
    documents = {
        OBSERVATIONS_NAME: observation_bytes,
        RUNTIME_EVIDENCE_NAME: evidence_bytes,
        RUNTIME_RUN_NAME: _json_bytes(report),
    }
    _publish(target, documents)
    verified = verify_full_flow_w4_retrieval_ranker_run(
        target,
        runtime_overlay_dir=runtime_overlay_dir,
    )
    return {**verified, "status": "COMPLETE", "output_dir": str(target)}


def verify_full_flow_w4_retrieval_ranker_run(
    output_dir: str | Path,
    *,
    runtime_overlay_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Verify ranker output and optionally bind it to its public runtime."""

    root = Path(output_dir).resolve()
    _verify_checksums(root, _RUN_FILES)
    observation_raw, observations = _read_json(
        root / OBSERVATIONS_NAME, "W4 observations"
    )
    evidence_raw, evidence = _read_jsonl(
        root / RUNTIME_EVIDENCE_NAME, "W4 runtime evidence"
    )
    report_raw, report = _read_json(root / RUNTIME_RUN_NAME, "W4 runtime run")
    _require(
        observation_raw == _json_bytes(observations)
        and evidence_raw == _jsonl_bytes(evidence)
        and report_raw == _json_bytes(report),
        "W4 runtime output is not canonical",
    )
    expected_report_fields = {
        "schema_version",
        "status",
        "run_id",
        "runtime_overlay_id",
        "runtime_plan_sha256",
        "contract_id",
        "trial_count",
        "candidate_object_count",
        "ranking_observations_complete",
        "ready_for_n1_hidden_relevance_evaluation",
        "current_64_trial_matrix_modified",
        "physical_candidate_route_execution_verified",
        "physical_design_retrieval_comparison_ready",
        "quality_claim_scope",
        "output_sha256",
        "llm_called",
        "flowmesh_workflow_submitted",
        "credentials_recorded",
        "eligible_for_scientific_claims",
        "run_sha256",
    }
    _strict_fields(report, expected_report_fields, "W4 runtime report")
    _require(
        report.get("schema_version") == W4_RETRIEVAL_RUNTIME_RUN_SCHEMA_VERSION
        and report.get("status") == "COMPLETE",
        "W4 runtime report schema or status changed",
    )
    for name in ("run_id", "runtime_overlay_id", "contract_id"):
        _identifier(report.get(name), name)
    _digest(report.get("runtime_plan_sha256"), "runtime plan SHA-256")
    _require(
        type(report.get("candidate_object_count")) is int
        and report["candidate_object_count"] >= 2,
        "W4 runtime candidate count is invalid",
    )
    supplied = _digest(report.get("run_sha256"), "run SHA-256")
    core = dict(report)
    del core["run_sha256"]
    _require(supplied == _sha256(_canonical(core)), "run digest mismatch")
    _require(
        report.get("output_sha256")
        == {
            OBSERVATIONS_NAME: _sha256(observation_raw),
            RUNTIME_EVIDENCE_NAME: _sha256(evidence_raw),
        },
        "W4 runtime report output binding changed",
    )
    _require(
        report.get("trial_count") == len(evidence) == 16
        and report.get("ranking_observations_complete") is True
        and report.get("ready_for_n1_hidden_relevance_evaluation") is True
        and report.get("current_64_trial_matrix_modified") is False
        and report.get("physical_candidate_route_execution_verified") is False
        and report.get("physical_design_retrieval_comparison_ready") is False
        and report.get("quality_claim_scope") == "public-ranker-output-only"
        and report.get("credentials_recorded") is False
        and report.get("eligible_for_scientific_claims") is False,
        "W4 runtime report overstates its evidence",
    )
    _require(
        type(report.get("llm_called")) is bool
        and type(report.get("flowmesh_workflow_submitted")) is bool,
        "W4 runtime invocation flags are invalid",
    )
    _strict_fields(
        observations,
        {"schema_version", "contract_id", "observations", "credentials_recorded"},
        "W4 observation document",
    )
    _require(
        observations.get("schema_version")
        == W4_RETRIEVAL_OBSERVATIONS_SCHEMA_VERSION
        and observations.get("contract_id") == report["contract_id"]
        and observations.get("credentials_recorded") is False,
        "W4 observation document binding changed",
    )
    rows = observations.get("observations")
    _require(isinstance(rows, list) and len(rows) == 16, "observation count changed")
    evidence_by_key: dict[str, Mapping[str, Any]] = {}
    for row in evidence:
        _strict_fields(
            row,
            {
                "schema_version",
                "run_id",
                "runtime_overlay_id",
                "trial_key",
                "order_index",
                "design_id",
                "repetition",
                "ranking_sha256",
                "executor_evidence_sha256",
                "telemetry_complete",
                "llm_called",
                "flowmesh_workflow_submitted",
                "physical_candidate_route_execution_verified",
                "credentials_recorded",
                "eligible_for_scientific_claims",
            },
            "W4 runtime evidence row",
        )
        key = _identifier(row.get("trial_key"), "trial_key")
        _require(key not in evidence_by_key, "duplicate runtime evidence trial")
        evidence_by_key[key] = row
        _require(
            row.get("schema_version")
            == W4_RETRIEVAL_RUNTIME_EVIDENCE_SCHEMA_VERSION
            and row.get("run_id") == report["run_id"]
            and row.get("runtime_overlay_id") == report["runtime_overlay_id"]
            and row.get("telemetry_complete") is True
            and type(row.get("llm_called")) is bool
            and type(row.get("flowmesh_workflow_submitted")) is bool
            and row.get("physical_candidate_route_execution_verified") is False
            and row.get("credentials_recorded") is False
            and row.get("eligible_for_scientific_claims") is False,
            "W4 runtime evidence semantics changed",
        )
        _require(
            row.get("design_id") in _ROUTE_FAMILY_BY_DESIGN
            and row.get("repetition") in {0, 1}
            and type(row.get("order_index")) is int
            and row["order_index"] >= 0,
            "W4 runtime evidence coordinate is invalid",
        )
        _digest(row.get("ranking_sha256"), "ranking SHA-256")
        _digest(row.get("executor_evidence_sha256"), "executor evidence SHA-256")
    _require(
        report["llm_called"] == any(row["llm_called"] for row in evidence)
        and report["flowmesh_workflow_submitted"]
        == any(row["flowmesh_workflow_submitted"] for row in evidence),
        "W4 runtime invocation summary differs from evidence",
    )
    _require(
        {(row["design_id"], row["repetition"]) for row in evidence}
        == {(f"D{index}", repetition) for index in range(8) for repetition in (0, 1)}
        and len({row["order_index"] for row in evidence}) == len(evidence),
        "W4 runtime evidence matrix coverage changed",
    )
    seen: set[str] = set()
    for observation in rows:
        _strict_fields(
            observation,
            {
                "trial_key",
                "retrieval_task_binding_sha256",
                "ranked_object_ids",
                "outcome_type",
                "telemetry_complete",
            },
            "W4 observation",
        )
        key = _identifier(observation.get("trial_key"), "trial_key")
        _require(key not in seen and key in evidence_by_key, "bad observation trial")
        seen.add(key)
        ranking = observation.get("ranked_object_ids")
        _require(
            observation.get("outcome_type") == "completed"
            and observation.get("telemetry_complete") is True
            and isinstance(ranking, list)
            and len(ranking) == report["candidate_object_count"]
            and len(set(ranking)) == len(ranking)
            and all(isinstance(item, str) for item in ranking)
            and _SHA256.fullmatch(
                str(observation.get("retrieval_task_binding_sha256"))
            )
            is not None
            and _sha256(_canonical(ranking))
            == evidence_by_key[key]["ranking_sha256"],
            "W4 observation differs from runtime evidence",
        )
    source_bound = runtime_overlay_dir is not None
    if source_bound:
        plan, task, trials = _verify_package(Path(runtime_overlay_dir).resolve())
        _require(
            report["runtime_plan_sha256"] == plan["plan_sha256"]
            and report["runtime_overlay_id"] == plan["runtime_overlay_id"]
            and report["contract_id"] == plan["contract_id"]
            and report["candidate_object_count"]
            == plan["candidate_object_count"],
            "W4 runtime run binds a different public plan",
        )
        candidate_ids = [row["object_id"] for row in task["candidate_objects"]]
        by_key = {row["trial_key"]: row for row in trials}
        _require(set(by_key) == seen, "W4 runtime plan/run coverage differs")
        for observation in rows:
            trial = by_key[observation["trial_key"]]
            evidence_row = evidence_by_key[observation["trial_key"]]
            ranking = observation["ranked_object_ids"]
            _require(
                observation["retrieval_task_binding_sha256"]
                == task["task_binding_sha256"]
                and isinstance(ranking, list)
                and len(ranking) == len(candidate_ids)
                and len(set(ranking)) == len(ranking)
                and set(ranking) == set(candidate_ids)
                and evidence_row["order_index"] == trial["order_index"]
                and evidence_row["design_id"] == trial["design_id"]
                and evidence_row["repetition"] == trial["repetition"],
                "W4 runtime observation is not source bound",
            )
    return {
        "status": (
            "VERIFIED_SOURCE_BOUND_PUBLIC_W4_RANKER_RUN"
            if source_bound
            else "VERIFIED_PUBLIC_W4_RANKER_RUN_INTEGRITY"
        ),
        "run_id": report["run_id"],
        "runtime_overlay_id": report["runtime_overlay_id"],
        "trial_count": report["trial_count"],
        "candidate_object_count": report["candidate_object_count"],
        "ready_for_n1_hidden_relevance_evaluation": source_bound,
        "physical_design_retrieval_comparison_ready": False,
        "source_bound": source_bound,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "CHECKSUMS_NAME",
    "FullFlowW4RetrievalRuntimeError",
    "FrozenW4RetrievalRuntimeInputs",
    "OBSERVATIONS_NAME",
    "RUNTIME_EVIDENCE_NAME",
    "RUNTIME_PLAN_NAME",
    "RUNTIME_RUN_NAME",
    "RUNTIME_TASK_NAME",
    "RUNTIME_TRIALS_NAME",
    "W4PublicRankingExecutor",
    "W4IndexClient",
    "W4LexicalIndexRankingExecutor",
    "W4_RETRIEVAL_RANKER_RESULT_SCHEMA_VERSION",
    "W4_RETRIEVAL_RUNTIME_EVIDENCE_SCHEMA_VERSION",
    "W4_RETRIEVAL_RUNTIME_PLAN_SCHEMA_VERSION",
    "W4_RETRIEVAL_RUNTIME_RUN_SCHEMA_VERSION",
    "W4_RETRIEVAL_RUNTIME_TRIAL_SCHEMA_VERSION",
    "freeze_full_flow_w4_retrieval_runtime_overlay",
    "load_full_flow_w4_retrieval_runtime_inputs",
    "run_full_flow_w4_retrieval_ranker",
    "verify_full_flow_w4_retrieval_ranker_run",
    "verify_full_flow_w4_retrieval_runtime_overlay",
    "validate_full_flow_w4_public_runtime_task",
]
