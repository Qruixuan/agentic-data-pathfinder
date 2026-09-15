"""Strict in-process execution of frozen W4 candidate-wide route plans.

The coordinator is deliberately independent of any particular deployment.
It resolves the conditional operations in a verified candidate-route package,
then delegates every activated operation to one injected executor.  The
executor may be backed by local fixtures, containers, or a later FlowMesh
adapter, but it only receives public retrieval inputs.

This module produces complete public rankings in the exact observation shape
accepted by the existing N1 hidden-label evaluator.  It does not read labels,
persist endpoints or credentials, calculate money, or claim real performance.
"""

from __future__ import annotations

import json
import math
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence, runtime_checkable

from ._full_flow_primitives import (
    canonical_json_bytes,
    canonical_json_lines_bytes,
    checked_identifier,
    checked_lower_sha256,
    pretty_json_bytes,
    sha256_hex,
    strict_json_loads,
)
from .full_flow_w4_candidate_routes import (
    FullFlowW4CandidateRouteError,
    load_full_flow_w4_candidate_route_inputs,
    verify_full_flow_w4_candidate_routes,
)
from .full_flow_w4_retrieval_contract import (
    W4_RETRIEVAL_OBSERVATIONS_SCHEMA_VERSION,
)


W4_CANDIDATE_COORDINATOR_RUN_SCHEMA_VERSION = (
    "pathfinder.full-flow-w4-candidate-coordinator-run/v1alpha1"
)
W4_CANDIDATE_COORDINATOR_TRIAL_SCHEMA_VERSION = (
    "pathfinder.full-flow-w4-candidate-coordinator-trial/v1alpha1"
)
W4_CANDIDATE_COORDINATOR_EVIDENCE_SCHEMA_VERSION = (
    "pathfinder.full-flow-w4-candidate-operation-evidence/v1alpha1"
)
W4_CANDIDATE_OPERATION_RESULT_SCHEMA_VERSION = (
    "pathfinder.full-flow-w4-candidate-operation-result/v1alpha1"
)

RUN_NAME = "w4-candidate-coordinator-run.json"
TRIAL_RESULTS_NAME = "w4-candidate-coordinator-trials.jsonl"
OPERATION_EVIDENCE_NAME = "w4-candidate-operation-evidence.jsonl"
OBSERVATIONS_NAME = "w4-retrieval-observations.json"
CHECKSUMS_NAME = "SHA256SUMS"

_FILES = frozenset({
    RUN_NAME,
    TRIAL_RESULTS_NAME,
    OPERATION_EVIDENCE_NAME,
    OBSERVATIONS_NAME,
})
_RANKING_ACTIONS = frozenset({
    "query-candidate-index-shard",
    "merge-complete-candidate-index",
    "rank-digest-prefix-and-append-index-tail",
    "transfer-ranking-fallback",
    "rank-complete-candidate-set",
    "return-public-ranking-to-n1",
})
_BYTE_PRESERVING_ACTIONS = frozenset({
    "access-raw",
    "access-exact-raw-range",
    "access-derived-artifact",
    "transfer-artifact-bytes",
    "read",
    "insert",
    "prepare-retrieval-candidate",
    "join-hit-or-miss-branch",
    "transfer-prepared-model-input",
})
_KNOWN_ACTIONS = _RANKING_ACTIONS | _BYTE_PRESERVING_ACTIONS | {
    "admit-public-retrieval",
    "lookup",
}
_CACHE_DESIGNS = frozenset({"D3", "D7"})


class FullFlowW4CandidateCoordinatorError(ValueError):
    """Raised when a candidate route cannot be executed without ambiguity."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowW4CandidateCoordinatorError(message)


def _canonical(value: Any) -> bytes:
    return canonical_json_bytes(
        value,
        error_type=FullFlowW4CandidateCoordinatorError,
        error_message="coordinator value is not canonical JSON",
    )


def _json_bytes(value: Any) -> bytes:
    return pretty_json_bytes(value)


def _jsonl_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    return canonical_json_lines_bytes(
        rows,
        error_type=FullFlowW4CandidateCoordinatorError,
        error_message="coordinator value is not canonical JSON",
    )


def _sha256(value: bytes) -> str:
    return sha256_hex(value)


def _identifier(value: Any, name: str) -> str:
    return str(
        checked_identifier(
            value,
            name,
            error_type=FullFlowW4CandidateCoordinatorError,
        )
    )


def _digest(value: Any, name: str) -> str:
    return str(
        checked_lower_sha256(
            value,
            name,
            error_type=FullFlowW4CandidateCoordinatorError,
        )
    )


def _strict_fields(
    value: Mapping[str, Any], expected: set[str], name: str
) -> None:
    _require(set(value) == expected, f"{name} fields changed")


def _read_json(path: Path, name: str) -> tuple[bytes, dict[str, Any]]:
    _require(path.is_file() and not path.is_symlink(), f"{name} is missing")

    try:
        raw = path.read_bytes()
        value = strict_json_loads(
            raw,
            error_type=FullFlowW4CandidateCoordinatorError,
            duplicate_key_message=lambda key: f"{name} repeats key {key}",
            nonfinite_number_message=(
                lambda token: f"{name} contains non-finite number {token}"
            ),
        )
    except FullFlowW4CandidateCoordinatorError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowW4CandidateCoordinatorError(
            f"cannot read {name}"
        ) from exc
    _require(isinstance(value, dict), f"{name} must be an object")
    return raw, value


def _read_jsonl(path: Path, name: str) -> tuple[bytes, list[dict[str, Any]]]:
    _require(path.is_file() and not path.is_symlink(), f"{name} is missing")
    try:
        raw = path.read_bytes()
        rows = [
            json.loads(line)
            for line in raw.decode("utf-8").splitlines()
            if line
        ]
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowW4CandidateCoordinatorError(
            f"cannot read {name}"
        ) from exc
    _require(all(isinstance(row, dict) for row in rows), f"{name} is invalid")
    return raw, rows


def _checksums(documents: Mapping[str, bytes]) -> bytes:
    return b"".join(
        f"{_sha256(documents[name])}  {name}\n".encode("utf-8")
        for name in sorted(documents)
    )


def _publish(target: Path, documents: Mapping[str, bytes]) -> None:
    _require(not target.exists(), f"output directory already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(
        tempfile.mkdtemp(prefix=".w4-candidate-run-", dir=target.parent)
    )
    try:
        for name, content in documents.items():
            (temporary / name).write_bytes(content)
        (temporary / CHECKSUMS_NAME).write_bytes(_checksums(documents))
        os.replace(temporary, target)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def _verify_files(root: Path) -> dict[str, bytes]:
    _require(root.is_dir() and not root.is_symlink(), "run directory is missing")
    entries = list(root.iterdir())
    _require(
        all(path.is_file() and not path.is_symlink() for path in entries),
        "run directory contains a non-regular file",
    )
    _require(
        {path.name for path in entries} == _FILES | {CHECKSUMS_NAME},
        "run directory file set changed",
    )
    documents = {name: (root / name).read_bytes() for name in _FILES}
    _require(
        (root / CHECKSUMS_NAME).read_bytes() == _checksums(documents),
        "candidate coordinator checksums failed",
    )
    return documents


def _require_disjoint_output(target: Path, source: Path) -> None:
    _require(target != source, "output directory overlaps route package")
    try:
        target.relative_to(source)
    except ValueError:
        pass
    else:
        raise FullFlowW4CandidateCoordinatorError(
            "output directory overlaps route package"
        )
    try:
        source.relative_to(target)
    except ValueError:
        pass
    else:
        raise FullFlowW4CandidateCoordinatorError(
            "output directory overlaps route package"
        )


@runtime_checkable
class W4CandidateOperationExecutor(Protocol):
    """Public operation adapter used by the in-process coordinator.

    Implementations must treat ``execution_token`` as an idempotency key.
    ``context`` contains only public, already-validated identities and the
    expected cache state; it never contains hidden relevance labels.
    """

    def execute(
        self,
        *,
        run_id: str,
        execution_token: str,
        trial: Mapping[str, Any],
        operation: Mapping[str, Any],
        dependency_results: Sequence[Mapping[str, Any]],
        public_task: Mapping[str, Any],
        context: Mapping[str, Any],
    ) -> Mapping[str, Any]: ...


class DeterministicW4CandidateOperationExecutor:
    """Offline adapter for contract and orchestration conformance tests.

    It preserves the frozen artifact identities and derives rankings solely
    from public query text and object IDs.  It performs no semantic inference,
    storage I/O, network transfer, or cost measurement.  A real deployment
    replaces this class while retaining the same strict result contract.
    """

    def execute(
        self,
        *,
        run_id: str,
        execution_token: str,
        trial: Mapping[str, Any],
        operation: Mapping[str, Any],
        dependency_results: Sequence[Mapping[str, Any]],
        public_task: Mapping[str, Any],
        context: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        del run_id, trial
        _strict_fields(
            context,
            {
                "artifact_identity",
                "exact_content_range",
                "ranking_candidate_ids",
                "required_ranking",
                "expected_cache_outcome",
                "index_binding",
                "expected_logical_bytes",
                "expected_physical_bytes",
                "hidden_relevance_values_included",
                "credentials_recorded",
            },
            "deterministic operation context",
        )
        _require(
            context.get("hidden_relevance_values_included") is False
            and context.get("credentials_recorded") is False,
            "deterministic operation context crosses the safety boundary",
        )
        ranking = context.get("required_ranking")
        candidates = context.get("ranking_candidate_ids")
        if isinstance(candidates, list):
            query_text = str(public_task.get("query_text"))
            if operation.get("action") == "merge-complete-candidate-index":
                shard_values = [
                    object_id
                    for dependency in dependency_results
                    for object_id in dependency.get("ranked_object_ids", [])
                ]
                _require(
                    len(shard_values) == len(candidates)
                    and len(set(shard_values)) == len(shard_values)
                    and set(shard_values) == set(candidates),
                    "deterministic index merge received incomplete shards",
                )
            ranking = sorted(
                candidates,
                key=lambda object_id: _sha256(_canonical({
                    "domain": "pathfinder.w4-deterministic-ranking/v1",
                    "query_text": query_text,
                    "object_id": object_id,
                })),
            )
        action = str(operation["action"])
        return {
            "schema_version": W4_CANDIDATE_OPERATION_RESULT_SCHEMA_VERSION,
            "status": "COMPLETED",
            "execution_token": execution_token,
            "operation_key": operation["operation_key"],
            "action": action,
            "accepted": action == "admit-public-retrieval",
            "artifact_identity": context["artifact_identity"],
            "exact_content_range": context["exact_content_range"],
            "ranked_object_ids": ranking,
            "cache_outcome": context["expected_cache_outcome"],
            "index_binding": context["index_binding"],
            "logical_bytes": context["expected_logical_bytes"],
            "physical_bytes": context["expected_physical_bytes"],
            "service_time_ms": 0.0,
            "telemetry_complete": True,
            "llm_called": False,
            "flowmesh_workflow_submitted": False,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }


def _catalog_by_id(catalog: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    rows = catalog.get("objects")
    _require(isinstance(rows, list), "candidate artifact catalog is invalid")
    result = {
        str(row["object_id"]): row
        for row in rows
        if isinstance(row, Mapping) and isinstance(row.get("object_id"), str)
    }
    _require(len(result) == len(rows), "candidate artifact identities repeat")
    return result


def _artifact_for_operation(
    operation: Mapping[str, Any],
    states: Mapping[str, Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    identity = operation.get("representation_identity")
    if isinstance(identity, Mapping):
        return dict(identity)
    if operation.get("action") == "transfer-prepared-model-input":
        dependencies = operation.get("dependency_operation_keys")
        _require(
            isinstance(dependencies, list) and len(dependencies) == 1,
            "prepared model input has ambiguous provenance",
        )
        dependency = states.get(str(dependencies[0]))
        _require(
            isinstance(dependency, Mapping)
            and dependency.get("status") == "COMPLETED"
            and isinstance(dependency.get("artifact_identity"), Mapping),
            "prepared model input lost artifact provenance",
        )
        return dict(dependency["artifact_identity"])
    return None


def _ranking_domain(
    operation: Mapping[str, Any], candidate_ids: Sequence[str]
) -> list[str] | None:
    action = operation.get("action")
    if action == "query-candidate-index-shard":
        template = operation.get("index_query_template")
        _require(isinstance(template, Mapping), "index query template is missing")
        values = template.get("candidate_object_ids")
        _require(isinstance(values, list), "index shard candidates are missing")
        return [str(value) for value in values]
    if action in {
        "merge-complete-candidate-index",
        "rank-digest-prefix-and-append-index-tail",
        "rank-complete-candidate-set",
    }:
        return list(candidate_ids)
    return None


def _index_binding(operation: Mapping[str, Any]) -> dict[str, Any] | None:
    if operation.get("action") != "query-candidate-index-shard":
        return None
    template = operation.get("index_query_template")
    _require(isinstance(template, Mapping), "index query template is missing")
    return {
        "requested_node_id": template.get("requested_node_id"),
        "index_id": template.get("index_id"),
        "index_sha256": template.get("index_sha256"),
        "source_manifest_sha256": template.get("source_manifest_sha256"),
        "shard_index": template.get("shard_index"),
        "candidate_object_ids_sha256": _sha256(
            _canonical(template.get("candidate_object_ids"))
        ),
    }


def _dependency_ranking(
    operation: Mapping[str, Any],
    states: Mapping[str, Mapping[str, Any]],
) -> list[str] | None:
    if operation.get("action") not in {
        "transfer-ranking-fallback",
        "return-public-ranking-to-n1",
    }:
        return None
    dependencies = operation.get("dependency_operation_keys")
    _require(
        isinstance(dependencies, list) and len(dependencies) == 1,
        "ranking transfer has ambiguous provenance",
    )
    result = states.get(str(dependencies[0]))
    _require(
        isinstance(result, Mapping)
        and result.get("status") == "COMPLETED"
        and isinstance(result.get("ranked_object_ids"), list),
        "ranking transfer lost ranking provenance",
    )
    return list(result["ranked_object_ids"])


def _validate_route_ranking_rule(
    operation: Mapping[str, Any],
    ranking: Any,
    states: Mapping[str, Mapping[str, Any]],
    trial: Mapping[str, Any],
) -> None:
    action = operation.get("action")
    if action == "merge-complete-candidate-index":
        dependencies = operation.get("dependency_operation_keys")
        _require(isinstance(dependencies, list), "index merge dependencies changed")
        shard_values = [
            object_id
            for key in dependencies
            for object_id in states[str(key)]["ranked_object_ids"]
        ]
        _require(
            isinstance(ranking, list)
            and len(shard_values) == len(ranking)
            and len(set(shard_values)) == len(shard_values)
            and set(shard_values) == set(ranking),
            "index shard merge did not preserve exact candidate coverage",
        )
        return
    if action not in {
        "rank-digest-prefix-and-append-index-tail",
        "rank-complete-candidate-set",
    }:
        return
    dependencies = operation.get("dependency_operation_keys")
    _require(isinstance(dependencies, list), "ranking dependencies changed")
    base_rankings = [
        states[str(key)]["ranked_object_ids"]
        for key in dependencies
        if states[str(key)].get("status") == "COMPLETED"
        and isinstance(states[str(key)].get("ranked_object_ids"), list)
    ]
    if action == "rank-complete-candidate-set" and trial.get("design_id") in {
        "D0",
        "D4",
    }:
        _require(not base_rankings, "raw full-corpus rank gained a fallback")
        return
    _require(
        len(base_rankings) == 1 and isinstance(ranking, list),
        "prefix rank has ambiguous fallback ranking",
    )
    base = list(base_rankings[0])
    prepared_count = sum(
        states[str(key)].get("status") == "COMPLETED"
        and isinstance(states[str(key)].get("artifact_identity"), Mapping)
        for key in dependencies
    )
    _require(
        0 < prepared_count <= len(base)
        and len(ranking) == len(base)
        and set(ranking[:prepared_count]) == set(base[:prepared_count])
        and ranking[prepared_count:] == base[prepared_count:],
        "semantic ranking changed candidates outside the activated prefix",
    )


def _cache_key(
    node_id: str, object_id: str, artifact: Mapping[str, Any]
) -> str:
    return _sha256(_canonical({
        "node_id": node_id,
        "object_id": object_id,
        "representation_id": artifact.get("representation_id"),
        "artifact_sha256": artifact.get("artifact_sha256"),
        "artifact_size_bytes": artifact.get("artifact_size_bytes"),
        "object_catalog_version": artifact.get("object_catalog_version"),
    }))


def _expected_bytes(
    operation: Mapping[str, Any], artifact: Mapping[str, Any] | None
) -> int:
    if operation.get("action") not in _BYTE_PRESERVING_ACTIONS:
        return 0
    _require(artifact is not None, "byte operation lost artifact identity")
    exact_range = operation.get("exact_content_range")
    if isinstance(exact_range, Mapping):
        return int(exact_range["range_size_bytes"])
    return int(artifact["artifact_size_bytes"])


def _active(
    operation: Mapping[str, Any], states: Mapping[str, Mapping[str, Any]]
) -> bool:
    activation = operation.get("activation")
    _require(isinstance(activation, Mapping), "operation activation is invalid")
    kind = activation.get("kind")
    if kind == "always":
        _require(set(activation) == {"kind"}, "always activation changed")
        return True
    _require(
        kind in {"ranking-prefix", "ranking-prefix-and-cache-branch"},
        "unsupported candidate activation",
    )
    ranking_key = activation.get("ranking_operation_key")
    ranking = states.get(str(ranking_key))
    _require(
        isinstance(ranking, Mapping)
        and ranking.get("status") == "COMPLETED"
        and isinstance(ranking.get("ranked_object_ids"), list),
        "candidate activation ranking is unavailable",
    )
    limit = activation.get("limit")
    object_id = activation.get("object_id")
    _require(
        type(limit) is int
        and limit > 0
        and isinstance(object_id, str),
        "candidate prefix activation changed",
    )
    in_prefix = object_id in ranking["ranked_object_ids"][:limit]
    if kind == "ranking-prefix":
        _require(
            set(activation)
            == {"kind", "ranking_operation_key", "limit", "object_id"},
            "ranking prefix fields changed",
        )
        return in_prefix
    _require(
        set(activation)
        == {
            "kind",
            "ranking_operation_key",
            "limit",
            "object_id",
            "cache_lookup_operation_key",
            "equals",
        }
        and activation.get("equals") in {"hit", "miss"},
        "cache branch activation changed",
    )
    if not in_prefix:
        return False
    lookup = states.get(str(activation.get("cache_lookup_operation_key")))
    _require(
        isinstance(lookup, Mapping)
        and lookup.get("status") == "COMPLETED"
        and lookup.get("cache_outcome") in {"hit", "miss"},
        "cache branch lookup is unavailable",
    )
    return lookup["cache_outcome"] == activation["equals"]


def _execution_token(
    run_id: str, plan_sha256: str, operation_key: str
) -> str:
    return _sha256(_canonical({
        "domain": "pathfinder.w4-candidate-operation-idempotency/v1",
        "run_id": run_id,
        "plan_sha256": plan_sha256,
        "operation_key": operation_key,
    }))


_RESULT_FIELDS = {
    "schema_version",
    "status",
    "execution_token",
    "operation_key",
    "action",
    "accepted",
    "artifact_identity",
    "exact_content_range",
    "ranked_object_ids",
    "cache_outcome",
    "index_binding",
    "logical_bytes",
    "physical_bytes",
    "service_time_ms",
    "telemetry_complete",
    "llm_called",
    "flowmesh_workflow_submitted",
    "credentials_recorded",
    "eligible_for_scientific_claims",
}


def _validate_executor_result(
    raw: Mapping[str, Any],
    *,
    execution_token: str,
    operation: Mapping[str, Any],
    candidate_ids: Sequence[str],
    states: Mapping[str, Mapping[str, Any]],
    trial: Mapping[str, Any],
    expected_cache_outcome: str | None,
) -> dict[str, Any]:
    _require(isinstance(raw, Mapping), "operation result is not an object")
    value = dict(raw)
    _strict_fields(value, _RESULT_FIELDS, "operation result")
    action = str(operation.get("action"))
    _require(action in _KNOWN_ACTIONS, "operation action is unsupported")
    _require(
        value.get("schema_version")
        == W4_CANDIDATE_OPERATION_RESULT_SCHEMA_VERSION
        and value.get("status") == "COMPLETED"
        and value.get("execution_token") == execution_token
        and value.get("operation_key") == operation.get("operation_key")
        and value.get("action") == action,
        "operation result identity changed",
    )
    artifact = _artifact_for_operation(operation, states)
    _require(
        value.get("artifact_identity") == artifact,
        "operation result artifact identity changed",
    )
    exact_range = operation.get("exact_content_range")
    _require(
        value.get("exact_content_range") == exact_range,
        "operation result content range changed",
    )
    ranking_domain = _ranking_domain(operation, candidate_ids)
    fixed_ranking = _dependency_ranking(operation, states)
    ranking = value.get("ranked_object_ids")
    if ranking_domain is not None:
        _require(
            isinstance(ranking, list)
            and all(isinstance(item, str) for item in ranking)
            and len(ranking) == len(ranking_domain)
            and len(set(ranking)) == len(ranking)
            and set(ranking) == set(ranking_domain),
            "operation did not return the required candidate permutation",
        )
    elif fixed_ranking is not None:
        _require(ranking == fixed_ranking, "ranking transfer changed order")
    else:
        _require(ranking is None, "non-ranking operation returned a ranking")
    _validate_route_ranking_rule(operation, ranking, states, trial)
    _require(
        value.get("cache_outcome") == expected_cache_outcome,
        "operation cache outcome changed",
    )
    _require(
        value.get("index_binding") == _index_binding(operation),
        "operation index binding changed",
    )
    _require(
        value.get("accepted") is (action == "admit-public-retrieval"),
        "operation admission result changed",
    )
    logical_bytes = _expected_bytes(operation, artifact)
    _require(
        type(value.get("logical_bytes")) is int
        and value["logical_bytes"] == logical_bytes
        and type(value.get("physical_bytes")) is int
        and value["physical_bytes"] >= 0,
        "operation byte accounting changed",
    )
    if action in _BYTE_PRESERVING_ACTIONS:
        _require(
            value["physical_bytes"] == logical_bytes,
            "byte-preserving operation changed physical bytes",
        )
    else:
        _require(value["physical_bytes"] == 0, "control operation emitted bytes")
    service_time = value.get("service_time_ms")
    _require(
        type(service_time) in {int, float}
        and math.isfinite(float(service_time))
        and float(service_time) >= 0.0,
        "operation service time is invalid",
    )
    _require(
        value.get("telemetry_complete") is True
        and type(value.get("llm_called")) is bool
        and type(value.get("flowmesh_workflow_submitted")) is bool
        and value.get("credentials_recorded") is False
        and value.get("eligible_for_scientific_claims") is False,
        "operation result is unsafe or overstates evidence",
    )
    return value


def _operation_context(
    operation: Mapping[str, Any],
    candidate_ids: Sequence[str],
    states: Mapping[str, Mapping[str, Any]],
    expected_cache_outcome: str | None,
) -> dict[str, Any]:
    artifact = _artifact_for_operation(operation, states)
    ranking_domain = _ranking_domain(operation, candidate_ids)
    fixed_ranking = _dependency_ranking(operation, states)
    return {
        "artifact_identity": artifact,
        "exact_content_range": operation.get("exact_content_range"),
        "ranking_candidate_ids": ranking_domain,
        "required_ranking": fixed_ranking,
        "expected_cache_outcome": expected_cache_outcome,
        "index_binding": _index_binding(operation),
        "expected_logical_bytes": _expected_bytes(operation, artifact),
        "expected_physical_bytes": _expected_bytes(operation, artifact),
        "hidden_relevance_values_included": False,
        "credentials_recorded": False,
    }


def _evidence_row(
    *,
    run_id: str,
    trial: Mapping[str, Any],
    operation: Mapping[str, Any],
    token: str,
    active: bool,
    result: Mapping[str, Any] | None,
) -> dict[str, Any]:
    identity = (
        result.get("artifact_identity")
        if result is not None
        else operation.get("representation_identity")
    )
    exact_range = operation.get("exact_content_range")
    return {
        "schema_version": W4_CANDIDATE_COORDINATOR_EVIDENCE_SCHEMA_VERSION,
        "run_id": run_id,
        "physical_plan_id": operation["physical_plan_id"],
        "trial_key": trial["trial_key"],
        "order_index": trial["order_index"],
        "design_id": trial["design_id"],
        "repetition": trial["repetition"],
        "operation_key": operation["operation_key"],
        "operation_index": operation["operation_index"],
        "action": operation["action"],
        "execution_status": "COMPLETED" if active else "INACTIVE",
        "execution_token": token,
        "executor_result_sha256": (
            _sha256(_canonical(result)) if result is not None else None
        ),
        "artifact_identity_sha256": (
            _sha256(_canonical(identity))
            if isinstance(identity, Mapping)
            else None
        ),
        "exact_content_range_sha256": (
            _sha256(_canonical(exact_range))
            if isinstance(exact_range, Mapping)
            else None
        ),
        "ranked_object_ids": (
            list(result["ranked_object_ids"])
            if result is not None
            and isinstance(result.get("ranked_object_ids"), list)
            else None
        ),
        "cache_outcome": (
            result.get("cache_outcome") if result is not None else None
        ),
        "index_binding_sha256": (
            _sha256(_canonical(result["index_binding"]))
            if result is not None
            and isinstance(result.get("index_binding"), Mapping)
            else None
        ),
        "logical_bytes": result.get("logical_bytes", 0) if result else 0,
        "physical_bytes": result.get("physical_bytes", 0) if result else 0,
        "service_time_ms": result.get("service_time_ms") if result else None,
        "telemetry_complete": True,
        "llm_called": bool(result.get("llm_called")) if result else False,
        "flowmesh_workflow_submitted": (
            bool(result.get("flowmesh_workflow_submitted")) if result else False
        ),
        "hidden_relevance_values_read": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def _execute_trial(
    *,
    run_id: str,
    source: Any,
    trial: Mapping[str, Any],
    operations: Sequence[Mapping[str, Any]],
    executor: W4CandidateOperationExecutor,
    cache: dict[str, set[str]],
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    candidate_ids = [
        str(row["object_id"]) for row in source.public_task["candidate_objects"]
    ]
    states: dict[str, Mapping[str, Any]] = {}
    evidence: list[dict[str, Any]] = []
    cache_counts = {"hit": 0, "miss": 0}
    for operation in operations:
        operation_key = str(operation["operation_key"])
        token = _execution_token(
            run_id, str(source.plan["plan_sha256"]), operation_key
        )
        is_active = _active(operation, states)
        if not is_active:
            state = {
                "status": "INACTIVE",
                "artifact_identity": operation.get("representation_identity"),
                "ranked_object_ids": None,
                "cache_outcome": None,
            }
            states[operation_key] = state
            evidence.append(_evidence_row(
                run_id=run_id,
                trial=trial,
                operation=operation,
                token=token,
                active=False,
                result=None,
            ))
            continue
        dependencies = operation.get("dependency_operation_keys")
        _require(isinstance(dependencies, list), "operation dependencies changed")
        dependency_results = [
            states[str(key)]
            for key in dependencies
            if states[str(key)].get("status") == "COMPLETED"
        ]
        action = str(operation["action"])
        expected_cache: str | None = None
        artifact = _artifact_for_operation(operation, states)
        if action == "lookup":
            _require(
                isinstance(artifact, Mapping), "cache lookup lost artifact identity"
            )
            nodes = operation.get("logical_node_ids")
            _require(
                isinstance(nodes, list) and len(nodes) == 1,
                "cache lookup node changed",
            )
            node = str(nodes[0])
            expected_cache = (
                "hit"
                if _cache_key(node, str(operation["object_id"]), artifact)
                in cache[node]
                else "miss"
            )
            if trial["design_id"] in _CACHE_DESIGNS:
                expected_repetition = "miss" if trial["repetition"] == 0 else "hit"
                _require(
                    expected_cache == expected_repetition,
                    "cache lifecycle is not miss-then-hit for the frozen repetition",
                )
        context = _operation_context(
            operation, candidate_ids, states, expected_cache
        )
        raw = executor.execute(
            run_id=run_id,
            execution_token=token,
            trial=trial,
            operation=operation,
            dependency_results=dependency_results,
            public_task=source.public_task,
            context=context,
        )
        result = _validate_executor_result(
            raw,
            execution_token=token,
            operation=operation,
            candidate_ids=candidate_ids,
            states=states,
            trial=trial,
            expected_cache_outcome=expected_cache,
        )
        state = dict(result)
        states[operation_key] = state
        if expected_cache is not None:
            cache_counts[expected_cache] += 1
        if action == "insert":
            _require(isinstance(artifact, Mapping), "cache insert lost identity")
            nodes = operation.get("logical_node_ids")
            _require(
                isinstance(nodes, list) and len(nodes) == 1,
                "cache insert node changed",
            )
            node = str(nodes[0])
            key = _cache_key(node, str(operation["object_id"]), artifact)
            _require(key not in cache[node], "cache insert replayed an artifact")
            cache[node].add(key)
        evidence.append(_evidence_row(
            run_id=run_id,
            trial=trial,
            operation=operation,
            token=token,
            active=True,
            result=result,
        ))
    terminal = states.get(str(trial["terminal_operation_key"]))
    _require(
        isinstance(terminal, Mapping)
        and terminal.get("status") == "COMPLETED"
        and isinstance(terminal.get("ranked_object_ids"), list),
        "trial did not produce a terminal public ranking",
    )
    ranking = list(terminal["ranked_object_ids"])
    _require(
        len(ranking) == len(candidate_ids)
        and len(set(ranking)) == len(ranking)
        and set(ranking) == set(candidate_ids),
        "terminal ranking is not a complete candidate permutation",
    )
    result = {
        "schema_version": W4_CANDIDATE_COORDINATOR_TRIAL_SCHEMA_VERSION,
        "run_id": run_id,
        "physical_plan_id": source.plan["physical_plan_id"],
        "route_plan_sha256": source.plan["plan_sha256"],
        "retrieval_task_binding_sha256": source.public_task[
            "task_binding_sha256"
        ],
        "trial_key": trial["trial_key"],
        "order_index": trial["order_index"],
        "design_id": trial["design_id"],
        "repetition": trial["repetition"],
        "ranked_object_ids": ranking,
        "ranking_sha256": _sha256(_canonical(ranking)),
        "activated_operation_count": sum(
            row["execution_status"] == "COMPLETED" for row in evidence
        ),
        "inactive_operation_count": sum(
            row["execution_status"] == "INACTIVE" for row in evidence
        ),
        "cache_lookup_hit_count": cache_counts["hit"],
        "cache_lookup_miss_count": cache_counts["miss"],
        "operation_evidence_sha256": _sha256(_jsonl_bytes(evidence)),
        "telemetry_complete": True,
        "hidden_relevance_values_read": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    observation = {
        "trial_key": trial["trial_key"],
        "retrieval_task_binding_sha256": source.public_task[
            "task_binding_sha256"
        ],
        "ranked_object_ids": ranking,
        "outcome_type": "completed",
        "telemetry_complete": True,
    }
    return result, evidence, observation


def run_full_flow_w4_candidate_coordinator(
    route_package_dir: str | Path,
    *,
    run_id: str,
    executor: W4CandidateOperationExecutor,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Execute every activated operation in all sixteen frozen W4 routes."""

    run = _identifier(run_id, "run_id")
    _require(
        isinstance(executor, W4CandidateOperationExecutor),
        "invalid candidate operation executor",
    )
    source_root = Path(route_package_dir).resolve()
    source = load_full_flow_w4_candidate_route_inputs(source_root)
    target = Path(output_dir).resolve()
    _require_disjoint_output(target, source_root)
    _require(not target.exists(), f"output directory already exists: {target}")
    operations_by_trial: dict[str, list[Mapping[str, Any]]] = {
        str(trial["trial_key"]): [] for trial in source.trials
    }
    for operation in source.operations:
        operations_by_trial[str(operation["trial_key"])].append(operation)
    cache = {"N7": set(), "N8": set()}
    trial_results: list[dict[str, Any]] = []
    evidence_rows: list[dict[str, Any]] = []
    observations: list[dict[str, Any]] = []
    for trial in sorted(source.trials, key=lambda row: int(row["order_index"])):
        trial_result, trial_evidence, observation = _execute_trial(
            run_id=run,
            source=source,
            trial=trial,
            operations=operations_by_trial[str(trial["trial_key"])],
            executor=executor,
            cache=cache,
        )
        trial_results.append(trial_result)
        evidence_rows.extend(trial_evidence)
        observations.append(observation)
    return publish_full_flow_w4_candidate_coordinator_results(
        source_root,
        run_id=run,
        trial_results=trial_results,
        operation_evidence=evidence_rows,
        observations=observations,
        output_dir=target,
    )


def publish_full_flow_w4_candidate_coordinator_results(
    route_package_dir: str | Path,
    *,
    run_id: str,
    trial_results: Sequence[Mapping[str, Any]],
    operation_evidence: Sequence[Mapping[str, Any]],
    observations: Sequence[Mapping[str, Any]],
    output_dir: str | Path,
) -> dict[str, Any]:
    """Publish externally executed trial results in coordinator-run form.

    The FlowMesh W4 wrapper executes one whole candidate trial at a remote
    N7/N8 coordinator.  This source-bound publisher intentionally shares the
    exact durable format and offline verifier used by the in-process runner,
    so crossing FlowMesh does not create a weaker evidence dialect.
    """

    run = _identifier(run_id, "run_id")
    source_root = Path(route_package_dir).resolve()
    source = load_full_flow_w4_candidate_route_inputs(source_root)
    target = Path(output_dir).resolve()
    _require_disjoint_output(target, source_root)
    _require(not target.exists(), f"output directory already exists: {target}")
    trial_results = [dict(row) for row in trial_results]
    evidence_rows = [dict(row) for row in operation_evidence]
    observations = [dict(row) for row in observations]
    _require(
        len(trial_results) == len(source.trials) == 16
        and len(evidence_rows) == len(source.operations)
        and len(observations) == len(source.trials),
        "externally executed W4 result coverage changed",
    )
    operation_by_key = {
        str(operation["operation_key"]): operation
        for operation in source.operations
    }
    observation_document = {
        "schema_version": W4_RETRIEVAL_OBSERVATIONS_SCHEMA_VERSION,
        "contract_id": source.public_task["contract_id"],
        "observations": observations,
        "credentials_recorded": False,
    }
    trial_bytes = _jsonl_bytes(trial_results)
    evidence_bytes = _jsonl_bytes(evidence_rows)
    observation_bytes = _json_bytes(observation_document)
    any_llm = any(row["llm_called"] for row in evidence_rows)
    any_flowmesh = any(
        row["flowmesh_workflow_submitted"] for row in evidence_rows
    )
    report: dict[str, Any] = {
        "schema_version": W4_CANDIDATE_COORDINATOR_RUN_SCHEMA_VERSION,
        "status": "COMPLETE",
        "run_id": run,
        "physical_plan_id": source.plan["physical_plan_id"],
        "route_plan_sha256": source.plan["plan_sha256"],
        "artifact_catalog_sha256": source.artifact_catalog["catalog_sha256"],
        "retrieval_task_binding_sha256": source.public_task[
            "task_binding_sha256"
        ],
        "candidate_set_sha256": source.public_task["candidate_set_sha256"],
        "trial_count": len(trial_results),
        "planned_operation_count": len(source.operations),
        "activated_operation_count": sum(
            row["execution_status"] == "COMPLETED" for row in evidence_rows
        ),
        "inactive_operation_count": sum(
            row["execution_status"] == "INACTIVE" for row in evidence_rows
        ),
        "cache_lookup_hit_count_by_node": {
            node: sum(
                row["cache_outcome"] == "hit"
                and row["design_id"] in _CACHE_DESIGNS
                and node
                in operation_by_key[row["operation_key"]]["logical_node_ids"]
                for row in evidence_rows
            )
            for node in ("N7", "N8")
        },
        "cache_lookup_miss_count_by_node": {
            node: sum(
                row["cache_outcome"] == "miss"
                and row["design_id"] in _CACHE_DESIGNS
                and node
                in operation_by_key[row["operation_key"]]["logical_node_ids"]
                for row in evidence_rows
            )
            for node in ("N7", "N8")
        },
        "n2_index_shard_merge_executed": any(
            row["action"] == "merge-complete-candidate-index"
            and row["execution_status"] == "COMPLETED"
            for row in evidence_rows
        ),
        "n6_complete_candidate_permutations": True,
        "candidate_prefix_activation_verified": True,
        "exact_n3_ranges_verified": True,
        "n4_digest_and_selected_frame_access_verified": True,
        "independent_n7_n8_cache_miss_then_hit_verified": True,
        "operation_idempotency_tokens_bound": True,
        "durable_resume_supported": False,
        "ready_for_n1_hidden_relevance_evaluation": True,
        "quality_claim_scope": "public-route-conformance-only",
        "output_sha256": {
            TRIAL_RESULTS_NAME: _sha256(trial_bytes),
            OPERATION_EVIDENCE_NAME: _sha256(evidence_bytes),
            OBSERVATIONS_NAME: _sha256(observation_bytes),
        },
        "llm_called": any_llm,
        "flowmesh_workflow_submitted": any_flowmesh,
        "hidden_relevance_values_read": False,
        "endpoints_recorded": False,
        "credentials_recorded": False,
        "monetary_cost_computed": False,
        "eligible_for_scientific_claims": False,
    }
    _require(
        all(
            report["cache_lookup_hit_count_by_node"][node] > 0
            for node in ("N7", "N8")
        )
        and all(
            report["cache_lookup_miss_count_by_node"][node] > 0
            for node in ("N7", "N8")
        ),
        "independent N7/N8 cache miss-then-hit evidence is incomplete",
    )
    report["run_sha256"] = _sha256(_canonical(report))
    documents = {
        RUN_NAME: _json_bytes(report),
        TRIAL_RESULTS_NAME: trial_bytes,
        OPERATION_EVIDENCE_NAME: evidence_bytes,
        OBSERVATIONS_NAME: observation_bytes,
    }
    _publish(target, documents)
    verified = verify_full_flow_w4_candidate_coordinator_run(
        target, route_package_dir=source_root
    )
    return {**verified, "status": "COMPLETE", "output_dir": str(target)}


def _verify_observation_document(
    value: Mapping[str, Any],
    *,
    contract_id: str,
    trial_by_key: Mapping[str, Mapping[str, Any]],
    trial_results: Mapping[str, Mapping[str, Any]],
    candidate_ids: Sequence[str],
) -> None:
    _strict_fields(
        value,
        {"schema_version", "contract_id", "observations", "credentials_recorded"},
        "W4 observation document",
    )
    _require(
        value.get("schema_version") == W4_RETRIEVAL_OBSERVATIONS_SCHEMA_VERSION
        and value.get("contract_id") == contract_id
        and value.get("credentials_recorded") is False,
        "W4 observation binding changed",
    )
    observations = value.get("observations")
    _require(
        isinstance(observations, list)
        and len(observations) == len(trial_by_key),
        "W4 observation coverage changed",
    )
    seen: set[str] = set()
    for observation in observations:
        _require(isinstance(observation, Mapping), "W4 observation is invalid")
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
        key = _identifier(observation.get("trial_key"), "observation trial_key")
        ranking = observation.get("ranked_object_ids")
        _require(
            key not in seen
            and key in trial_by_key
            and observation.get("outcome_type") == "completed"
            and observation.get("telemetry_complete") is True
            and isinstance(ranking, list)
            and len(ranking) == len(candidate_ids)
            and len(set(ranking)) == len(ranking)
            and set(ranking) == set(candidate_ids)
            and ranking == trial_results[key]["ranked_object_ids"],
            "W4 observation is not source-bound complete ranking",
        )
        seen.add(key)


def verify_full_flow_w4_candidate_coordinator_run(
    output_dir: str | Path,
    *,
    route_package_dir: str | Path,
) -> dict[str, Any]:
    """Verify a completed coordinator run against its frozen route package."""

    root = Path(output_dir).resolve()
    route_root = Path(route_package_dir).resolve()
    documents = _verify_files(root)
    try:
        verify_full_flow_w4_candidate_routes(route_root)
        source = load_full_flow_w4_candidate_route_inputs(route_root)
    except FullFlowW4CandidateRouteError as exc:
        raise FullFlowW4CandidateCoordinatorError(
            "candidate route package verification failed"
        ) from exc
    report_raw, report = _read_json(root / RUN_NAME, "coordinator run")
    trial_raw, trials = _read_jsonl(root / TRIAL_RESULTS_NAME, "trial results")
    evidence_raw, evidence = _read_jsonl(
        root / OPERATION_EVIDENCE_NAME, "operation evidence"
    )
    observations_raw, observations = _read_json(
        root / OBSERVATIONS_NAME, "W4 observations"
    )
    _require(
        report_raw == _json_bytes(report)
        and trial_raw == _jsonl_bytes(trials)
        and evidence_raw == _jsonl_bytes(evidence)
        and observations_raw == _json_bytes(observations),
        "candidate coordinator output is not canonical",
    )
    _strict_fields(
        report,
        {
            "schema_version",
            "status",
            "run_id",
            "physical_plan_id",
            "route_plan_sha256",
            "artifact_catalog_sha256",
            "retrieval_task_binding_sha256",
            "candidate_set_sha256",
            "trial_count",
            "planned_operation_count",
            "activated_operation_count",
            "inactive_operation_count",
            "cache_lookup_hit_count_by_node",
            "cache_lookup_miss_count_by_node",
            "n2_index_shard_merge_executed",
            "n6_complete_candidate_permutations",
            "candidate_prefix_activation_verified",
            "exact_n3_ranges_verified",
            "n4_digest_and_selected_frame_access_verified",
            "independent_n7_n8_cache_miss_then_hit_verified",
            "operation_idempotency_tokens_bound",
            "durable_resume_supported",
            "ready_for_n1_hidden_relevance_evaluation",
            "quality_claim_scope",
            "output_sha256",
            "llm_called",
            "flowmesh_workflow_submitted",
            "hidden_relevance_values_read",
            "endpoints_recorded",
            "credentials_recorded",
            "monetary_cost_computed",
            "eligible_for_scientific_claims",
            "run_sha256",
        },
        "coordinator run",
    )
    supplied_run_sha = _digest(report.get("run_sha256"), "run SHA-256")
    core = dict(report)
    del core["run_sha256"]
    _require(
        supplied_run_sha == _sha256(_canonical(core)), "run digest mismatch"
    )
    _require(
        report.get("schema_version")
        == W4_CANDIDATE_COORDINATOR_RUN_SCHEMA_VERSION
        and report.get("status") == "COMPLETE"
        and report.get("physical_plan_id") == source.plan["physical_plan_id"]
        and report.get("route_plan_sha256") == source.plan["plan_sha256"]
        and report.get("artifact_catalog_sha256")
        == source.artifact_catalog["catalog_sha256"]
        and report.get("retrieval_task_binding_sha256")
        == source.public_task["task_binding_sha256"]
        and report.get("candidate_set_sha256")
        == source.public_task["candidate_set_sha256"],
        "coordinator run source binding changed",
    )
    _require(
        report.get("output_sha256")
        == {
            TRIAL_RESULTS_NAME: _sha256(documents[TRIAL_RESULTS_NAME]),
            OPERATION_EVIDENCE_NAME: _sha256(
                documents[OPERATION_EVIDENCE_NAME]
            ),
            OBSERVATIONS_NAME: _sha256(documents[OBSERVATIONS_NAME]),
        },
        "coordinator output binding changed",
    )
    candidate_ids = [
        str(row["object_id"])
        for row in source.public_task["candidate_objects"]
    ]
    source_trials = {str(row["trial_key"]): row for row in source.trials}
    result_by_key: dict[str, Mapping[str, Any]] = {}
    for result in trials:
        _strict_fields(
            result,
            {
                "schema_version",
                "run_id",
                "physical_plan_id",
                "route_plan_sha256",
                "retrieval_task_binding_sha256",
                "trial_key",
                "order_index",
                "design_id",
                "repetition",
                "ranked_object_ids",
                "ranking_sha256",
                "activated_operation_count",
                "inactive_operation_count",
                "cache_lookup_hit_count",
                "cache_lookup_miss_count",
                "operation_evidence_sha256",
                "telemetry_complete",
                "hidden_relevance_values_read",
                "credentials_recorded",
                "eligible_for_scientific_claims",
            },
            "coordinator trial result",
        )
        key = _identifier(result.get("trial_key"), "trial_key")
        ranking = result.get("ranked_object_ids")
        _require(
            key not in result_by_key
            and key in source_trials
            and result.get("schema_version")
            == W4_CANDIDATE_COORDINATOR_TRIAL_SCHEMA_VERSION
            and result.get("run_id") == report["run_id"]
            and result.get("physical_plan_id") == source.plan["physical_plan_id"]
            and result.get("route_plan_sha256") == source.plan["plan_sha256"]
            and result.get("retrieval_task_binding_sha256")
            == source.public_task["task_binding_sha256"]
            and result.get("order_index") == source_trials[key]["order_index"]
            and result.get("design_id") == source_trials[key]["design_id"]
            and result.get("repetition") == source_trials[key]["repetition"]
            and isinstance(ranking, list)
            and len(ranking) == len(candidate_ids)
            and len(set(ranking)) == len(ranking)
            and set(ranking) == set(candidate_ids)
            and result.get("ranking_sha256") == _sha256(_canonical(ranking))
            and result.get("telemetry_complete") is True
            and result.get("hidden_relevance_values_read") is False
            and result.get("credentials_recorded") is False
            and result.get("eligible_for_scientific_claims") is False,
            "coordinator trial result changed",
        )
        result_by_key[key] = result
    _require(set(result_by_key) == set(source_trials), "trial coverage changed")
    _require(
        [result["trial_key"] for result in trials]
        == [
            trial["trial_key"]
            for trial in sorted(
                source.trials, key=lambda value: int(value["order_index"])
            )
        ],
        "trial result order changed",
    )
    operation_by_key = {
        str(row["operation_key"]): row for row in source.operations
    }
    evidence_by_trial: dict[str, list[Mapping[str, Any]]] = {
        key: [] for key in source_trials
    }
    states: dict[str, Mapping[str, Any]] = {}
    cache = {"N7": set(), "N8": set()}
    llm_called = False
    flowmesh_submitted = False
    _require(
        [row.get("operation_key") for row in evidence]
        == [row["operation_key"] for row in source.operations],
        "operation evidence order changed",
    )
    for row in evidence:
        _strict_fields(
            row,
            {
                "schema_version",
                "run_id",
                "physical_plan_id",
                "trial_key",
                "order_index",
                "design_id",
                "repetition",
                "operation_key",
                "operation_index",
                "action",
                "execution_status",
                "execution_token",
                "executor_result_sha256",
                "artifact_identity_sha256",
                "exact_content_range_sha256",
                "ranked_object_ids",
                "cache_outcome",
                "index_binding_sha256",
                "logical_bytes",
                "physical_bytes",
                "service_time_ms",
                "telemetry_complete",
                "llm_called",
                "flowmesh_workflow_submitted",
                "hidden_relevance_values_read",
                "credentials_recorded",
                "eligible_for_scientific_claims",
            },
            "operation evidence",
        )
        key = _identifier(row.get("operation_key"), "operation_key")
        trial_key = _identifier(row.get("trial_key"), "operation trial_key")
        _require(
            key in operation_by_key and trial_key in evidence_by_trial,
            "operation evidence identity changed",
        )
        operation = operation_by_key[key]
        trial = source_trials[trial_key]
        expected_active = _active(operation, states)
        status = "COMPLETED" if expected_active else "INACTIVE"
        identity = operation.get("representation_identity")
        exact_range = operation.get("exact_content_range")
        _require(
            row.get("schema_version")
            == W4_CANDIDATE_COORDINATOR_EVIDENCE_SCHEMA_VERSION
            and row.get("run_id") == report["run_id"]
            and row.get("physical_plan_id") == source.plan["physical_plan_id"]
            and row.get("order_index") == trial["order_index"]
            and row.get("design_id") == trial["design_id"]
            and row.get("repetition") == trial["repetition"]
            and row.get("operation_index") == operation["operation_index"]
            and row.get("action") == operation["action"]
            and row.get("execution_status") == status
            and row.get("execution_token")
            == _execution_token(report["run_id"], source.plan["plan_sha256"], key)
            and row.get("exact_content_range_sha256")
            == (
                _sha256(_canonical(exact_range))
                if isinstance(exact_range, Mapping)
                else None
            )
            and row.get("telemetry_complete") is True
            and type(row.get("llm_called")) is bool
            and type(row.get("flowmesh_workflow_submitted")) is bool
            and row.get("hidden_relevance_values_read") is False
            and row.get("credentials_recorded") is False
            and row.get("eligible_for_scientific_claims") is False,
            "operation evidence binding changed",
        )
        if expected_active:
            _digest(row.get("executor_result_sha256"), "executor result SHA-256")
            ranking_domain = _ranking_domain(operation, candidate_ids)
            fixed_ranking = _dependency_ranking(operation, states)
            ranking = row.get("ranked_object_ids")
            if ranking_domain is not None:
                _require(
                    isinstance(ranking, list)
                    and len(ranking) == len(ranking_domain)
                    and len(set(ranking)) == len(ranking)
                    and set(ranking) == set(ranking_domain),
                    "operation evidence ranking changed",
                )
            elif fixed_ranking is not None:
                _require(ranking == fixed_ranking, "ranking evidence reordered")
            else:
                _require(ranking is None, "unexpected ranking evidence")
            _validate_route_ranking_rule(operation, ranking, states, trial)
            artifact = _artifact_for_operation(operation, states)
            _require(
                row.get("artifact_identity_sha256")
                == (
                    _sha256(_canonical(artifact))
                    if isinstance(artifact, Mapping)
                    else None
                ),
                "operation evidence artifact lineage changed",
            )
            expected_index = _index_binding(operation)
            _require(
                row.get("index_binding_sha256")
                == (
                    _sha256(_canonical(expected_index))
                    if expected_index is not None
                    else None
                ),
                "operation evidence index lineage changed",
            )
            expected_bytes = _expected_bytes(operation, artifact)
            _require(
                row.get("logical_bytes") == expected_bytes
                and row.get("physical_bytes")
                == (
                    expected_bytes
                    if operation["action"] in _BYTE_PRESERVING_ACTIONS
                    else 0
                )
                and type(row.get("service_time_ms")) in {int, float}
                and math.isfinite(float(row["service_time_ms"]))
                and float(row["service_time_ms"]) >= 0.0,
                "operation evidence accounting changed",
            )
            cache_outcome = row.get("cache_outcome")
            if operation["action"] == "lookup":
                _require(isinstance(artifact, Mapping), "lookup artifact missing")
                node = str(operation["logical_node_ids"][0])
                expected_outcome = (
                    "hit"
                    if _cache_key(node, str(operation["object_id"]), artifact)
                    in cache[node]
                    else "miss"
                )
                _require(
                    cache_outcome == expected_outcome
                    and expected_outcome
                    == ("miss" if trial["repetition"] == 0 else "hit"),
                    "cache evidence is not independent miss-then-hit",
                )
            else:
                _require(cache_outcome is None, "non-lookup cache outcome present")
            state = {
                "status": "COMPLETED",
                "artifact_identity": artifact,
                "ranked_object_ids": ranking,
                "cache_outcome": cache_outcome,
            }
            if operation["action"] == "insert":
                _require(isinstance(artifact, Mapping), "insert artifact missing")
                node = str(operation["logical_node_ids"][0])
                cache[node].add(
                    _cache_key(node, str(operation["object_id"]), artifact)
                )
        else:
            _require(
                row.get("executor_result_sha256") is None
                and row.get("artifact_identity_sha256")
                == (
                    _sha256(_canonical(identity))
                    if isinstance(identity, Mapping)
                    else None
                )
                and row.get("ranked_object_ids") is None
                and row.get("cache_outcome") is None
                and row.get("index_binding_sha256") is None
                and row.get("logical_bytes") == 0
                and row.get("physical_bytes") == 0
                and row.get("service_time_ms") is None
                and row.get("llm_called") is False
                and row.get("flowmesh_workflow_submitted") is False,
                "inactive operation carries execution evidence",
            )
            state = {
                "status": "INACTIVE",
                "artifact_identity": identity,
                "ranked_object_ids": None,
                "cache_outcome": None,
            }
        states[key] = state
        evidence_by_trial[trial_key].append(row)
        llm_called = llm_called or bool(row["llm_called"])
        flowmesh_submitted = flowmesh_submitted or bool(
            row["flowmesh_workflow_submitted"]
        )
    _require(
        len(evidence) == len(source.operations)
        and set(states) == set(operation_by_key),
        "operation evidence coverage changed",
    )
    for key, rows in evidence_by_trial.items():
        result = result_by_key[key]
        _require(
            result["operation_evidence_sha256"] == _sha256(_jsonl_bytes(rows))
            and result["activated_operation_count"]
            == sum(row["execution_status"] == "COMPLETED" for row in rows)
            and result["inactive_operation_count"]
            == sum(row["execution_status"] == "INACTIVE" for row in rows)
            and result["cache_lookup_hit_count"]
            == sum(row["cache_outcome"] == "hit" for row in rows)
            and result["cache_lookup_miss_count"]
            == sum(row["cache_outcome"] == "miss" for row in rows),
            "trial evidence summary changed",
        )
    _verify_observation_document(
        observations,
        contract_id=str(source.public_task["contract_id"]),
        trial_by_key=source_trials,
        trial_results=result_by_key,
        candidate_ids=candidate_ids,
    )
    active_count = sum(row["execution_status"] == "COMPLETED" for row in evidence)
    inactive_count = len(evidence) - active_count
    _require(
        report.get("trial_count") == len(trials) == 16
        and report.get("planned_operation_count") == len(evidence)
        and report.get("activated_operation_count") == active_count
        and report.get("inactive_operation_count") == inactive_count
        and report.get("n2_index_shard_merge_executed") is True
        and report.get("n6_complete_candidate_permutations") is True
        and report.get("candidate_prefix_activation_verified") is True
        and report.get("exact_n3_ranges_verified") is True
        and report.get("n4_digest_and_selected_frame_access_verified") is True
        and report.get("independent_n7_n8_cache_miss_then_hit_verified") is True
        and report.get("operation_idempotency_tokens_bound") is True
        and report.get("durable_resume_supported") is False
        and report.get("ready_for_n1_hidden_relevance_evaluation") is True
        and report.get("quality_claim_scope") == "public-route-conformance-only"
        and report.get("llm_called") == llm_called
        and report.get("flowmesh_workflow_submitted") == flowmesh_submitted
        and report.get("hidden_relevance_values_read") is False
        and report.get("endpoints_recorded") is False
        and report.get("credentials_recorded") is False
        and report.get("monetary_cost_computed") is False
        and report.get("eligible_for_scientific_claims") is False,
        "coordinator run overstates or misstates evidence",
    )
    for node in ("N7", "N8"):
        hit = sum(
            row["cache_outcome"] == "hit"
            and node in operation_by_key[row["operation_key"]]["logical_node_ids"]
            for row in evidence
        )
        miss = sum(
            row["cache_outcome"] == "miss"
            and node in operation_by_key[row["operation_key"]]["logical_node_ids"]
            for row in evidence
        )
        _require(
            isinstance(report.get("cache_lookup_hit_count_by_node"), Mapping)
            and isinstance(
                report.get("cache_lookup_miss_count_by_node"), Mapping
            )
            and report["cache_lookup_hit_count_by_node"].get(node) == hit > 0
            and report["cache_lookup_miss_count_by_node"].get(node) == miss > 0,
            f"{node} cache summary changed",
        )
    return {
        "status": "VERIFIED_W4_CANDIDATE_COORDINATOR_RUN",
        "run_id": report["run_id"],
        "physical_plan_id": report["physical_plan_id"],
        "trial_count": report["trial_count"],
        "planned_operation_count": report["planned_operation_count"],
        "activated_operation_count": report["activated_operation_count"],
        "inactive_operation_count": report["inactive_operation_count"],
        "ready_for_n1_hidden_relevance_evaluation": True,
        "hidden_relevance_values_read": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "CHECKSUMS_NAME",
    "DeterministicW4CandidateOperationExecutor",
    "FullFlowW4CandidateCoordinatorError",
    "OBSERVATIONS_NAME",
    "OPERATION_EVIDENCE_NAME",
    "publish_full_flow_w4_candidate_coordinator_results",
    "RUN_NAME",
    "TRIAL_RESULTS_NAME",
    "W4CandidateOperationExecutor",
    "W4_CANDIDATE_COORDINATOR_EVIDENCE_SCHEMA_VERSION",
    "W4_CANDIDATE_COORDINATOR_RUN_SCHEMA_VERSION",
    "W4_CANDIDATE_COORDINATOR_TRIAL_SCHEMA_VERSION",
    "W4_CANDIDATE_OPERATION_RESULT_SCHEMA_VERSION",
    "run_full_flow_w4_candidate_coordinator",
    "verify_full_flow_w4_candidate_coordinator_run",
]
