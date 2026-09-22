"""Deployment-bound admission for the 4x8 semantic full-flow matrix.

The endpoint-free semantic matrix proves what every trial means.  A full-flow
deployment binding proves where each logical service lives.  Neither fact is
enough to make the matrix executable through the FlowMesh contract currently
used by Pathfinder: stage results such as Data Agent artifact handles, index
ranges, and cache decisions must still be carried into later calls by a route
runtime.

This module joins those two frozen inputs without overstating that boundary.
It emits all 64 deployment-bound trial DAGs, a ten-case representative smoke
set, and an exact runtime-gap ledger.  With the implementations currently in
the repository the result is deliberately *not* a submittable workflow.  A
future route-runtime implementation can replace this admission boundary; it
must not silently reinterpret this package as an executable plan.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping

from .full_flow_deployment import (
    DEPLOYMENT_BINDING_NAME,
    verify_full_flow_deployment_binding,
)
from .full_flow_semantic_matrix import (
    ARTIFACT_BINDINGS_NAME,
    CHECKSUMS_NAME as SEMANTIC_CHECKSUMS_NAME,
    PLAN_NAME as SEMANTIC_PLAN_NAME,
    PUBLIC_TASKS_NAME,
    STAGES_NAME as SEMANTIC_STAGES_NAME,
    TRIALS_NAME as SEMANTIC_TRIALS_NAME,
    verify_full_flow_semantic_matrix,
)
from .hidden_oracle import verify_n1_oracle_package


ADMISSION_SCHEMA_VERSION = (
    "pathfinder.full-flow-semantic-execution-admission/v1alpha1"
)
BOUND_TRIAL_SCHEMA_VERSION = (
    "pathfinder.full-flow-semantic-bound-trial/v1alpha1"
)
BOUND_STAGE_SCHEMA_VERSION = (
    "pathfinder.full-flow-semantic-bound-stage/v1alpha1"
)
RUNTIME_GAPS_SCHEMA_VERSION = (
    "pathfinder.full-flow-semantic-runtime-gaps/v1alpha1"
)
SMOKE_SELECTION_SCHEMA_VERSION = (
    "pathfinder.full-flow-semantic-smoke-selection/v1alpha1"
)

ADMISSION_NAME = "semantic-execution-admission.json"
TRIALS_NAME = "semantic-execution-trials.jsonl"
STAGES_NAME = "semantic-execution-stages.jsonl"
GAPS_NAME = "semantic-execution-runtime-gaps.json"
SMOKES_NAME = "semantic-execution-smokes.jsonl"
CHECKSUMS_NAME = "SHA256SUMS"

_CONTENT_FILES = {
    ADMISSION_NAME,
    TRIALS_NAME,
    STAGES_NAME,
    GAPS_NAME,
    SMOKES_NAME,
}
_ALL_FILES = _CONTENT_FILES | {CHECKSUMS_NAME}
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}\Z")
_ENVIRONMENT_NAME = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_EXPECTED_ROUTE_FAMILIES = {
    "raw",
    "indexed-raw",
    "remote-derived",
    "local-cache-derived",
}
_EXPECTED_N1_ENV_NAMES = {
    "PATHFINDER_N1_ORACLE_EVIDENCE_SECRET",
    "PATHFINDER_N1_ORACLE_TOKEN",
}

# These are admission requirements, not statements that no component code
# exists.  For example, the repository has a label-free N1 scorer and a
# frame-bundle-only N4 -> N7 -> N6 -> N1 coordinator.  What is absent is a
# matrix-bound runtime that implements each requirement for every applicable
# route and preserves the frozen stage identities.
_GAP_CATALOG: dict[str, dict[str, Any]] = {
    "semantic-matrix-flowmesh-renderer-v1": {
        "scope": "matrix",
        "route_families": sorted(_EXPECTED_ROUTE_FAMILIES),
        "reason": (
            "No renderer converts a semantic-matrix trial into one "
            "worker-pinned, request-authenticated FlowMesh coordinator task."
        ),
    },
    "semantic-artifact-availability-preflight-v1": {
        "scope": "matrix",
        "route_families": sorted(_EXPECTED_ROUTE_FAMILIES),
        "reason": (
            "Deployment health checks do not prove that every frozen artifact "
            "digest and byte length is retrievable from its planned service."
        ),
    },
    "semantic-matrix-durable-runner-v1": {
        "scope": "matrix",
        "route_families": sorted(_EXPECTED_ROUTE_FAMILIES),
        "reason": (
            "The infrastructure matrix runner does not yet persist semantic "
            "answers, authenticated N1 scores, and neutral AWM/OED evidence."
        ),
    },
    "n1-authenticated-score-handoff-v2": {
        "scope": "matrix",
        "route_families": sorted(_EXPECTED_ROUTE_FAMILIES),
        "reason": (
            "The generic route runtimes must submit exactly one v1alpha2 N1 "
            "score request per frozen run/trial identity and verify its HMAC."
        ),
    },
    "raw-route-coordinator-v1": {
        "scope": "route-family",
        "route_families": ["raw"],
        "reason": (
            "No coordinator carries an N3 raw-video artifact through the "
            "N7/N8 preparation and N6 semantic boundaries."
        ),
    },
    "indexed-raw-route-coordinator-v1": {
        "scope": "route-family",
        "route_families": ["indexed-raw"],
        "reason": (
            "No coordinator binds the N2 index result to an exact N3 range "
            "access and then carries that artifact to N6."
        ),
    },
    "remote-derived-route-coordinator-v1": {
        "scope": "route-family",
        "route_families": ["remote-derived"],
        "reason": (
            "The existing frame-bundle runtime covers only one N4/N7 subset; "
            "there is no matrix-bound N7/N8 runtime for every derived input."
        ),
    },
    "conditional-cache-derived-route-coordinator-v1": {
        "scope": "route-family",
        "route_families": ["local-cache-derived"],
        "reason": (
            "No semantic coordinator turns an authenticated N7/N8 cache "
            "lookup into one exclusive hit/miss branch and a joined model input."
        ),
    },
    "cache-state-lifecycle-attestation-v1": {
        "scope": "route-family",
        "route_families": ["local-cache-derived"],
        "reason": (
            "The semantic runner must attest an empty scope for the miss case "
            "and unchanged runtime epoch plus prerequisite insertion for the "
            "paired hit case."
        ),
    },
    "raw-video-model-input-adapter-v1": {
        "scope": "representation",
        "route_families": ["raw", "indexed-raw"],
        "reason": (
            "N6 currently accepts ordered JPEG frames, not a raw-video object "
            "or index-selected byte range."
        ),
    },
    "digest-model-input-adapter-v1": {
        "scope": "representation",
        "route_families": ["remote-derived", "local-cache-derived"],
        "reason": (
            "N6 has no frozen semantic request contract for multimodal_digest."
        ),
    },
    "multi-representation-fusion-adapter-v1": {
        "scope": "representation",
        "route_families": ["remote-derived", "local-cache-derived"],
        "reason": (
            "No request contract preserves and fuses both digest and frame "
            "bundle identities for W3/W4 trials."
        ),
    },
    "n8-full-flow-route-runtime-v1": {
        "scope": "executor-node",
        "route_families": sorted(_EXPECTED_ROUTE_FAMILIES),
        "reason": "The existing semantic full-flow coordinator is N7-only.",
    },
    "n5-materialize-publish-runtime-v1": {
        "scope": "provisioning",
        "route_families": ["remote-derived", "local-cache-derived"],
        "reason": (
            "N5 frame/digest services and N4 publication exist, but no frozen "
            "coordinator performs the exact N3 -> N5 -> N4 provisioning chain."
        ),
    },
    "semantic-evidence-to-awm-oed-bridge-v1": {
        "scope": "matrix",
        "route_families": sorted(_EXPECTED_ROUTE_FAMILIES),
        "reason": (
            "The AWM/OED bridge accepts authenticated evidence, but no matrix "
            "runner emits one neutral observation for every semantic trial."
        ),
    },
}

_COMMON_GAPS = {
    "semantic-matrix-flowmesh-renderer-v1",
    "semantic-artifact-availability-preflight-v1",
    "semantic-matrix-durable-runner-v1",
    "n1-authenticated-score-handoff-v2",
    "semantic-evidence-to-awm-oed-bridge-v1",
}
_ROUTE_GAP = {
    "raw": "raw-route-coordinator-v1",
    "indexed-raw": "indexed-raw-route-coordinator-v1",
    "remote-derived": "remote-derived-route-coordinator-v1",
    "local-cache-derived": "conditional-cache-derived-route-coordinator-v1",
}


class FullFlowSemanticExecutionAdmissionError(ValueError):
    """Raised when an admission package is incomplete or misleading."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowSemanticExecutionAdmissionError(message)


def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _invalid_number(value: str) -> None:
    raise FullFlowSemanticExecutionAdmissionError(
        f"non-finite JSON number: {value}"
    )


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_unique_pairs,
            parse_constant=_invalid_number,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowSemanticExecutionAdmissionError(
            f"cannot read valid {label}"
        ) from exc
    _require(isinstance(value, dict), f"{label} must be an object")
    return value


def _read_jsonl(path: Path, label: str) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FullFlowSemanticExecutionAdmissionError(
            f"cannot read {label}"
        ) from exc
    rows: list[dict[str, Any]] = []
    for position, line in enumerate(lines, start=1):
        _require(bool(line.strip()), f"blank {label} line: {position}")
        try:
            value = json.loads(
                line,
                object_pairs_hook=_unique_pairs,
                parse_constant=_invalid_number,
            )
        except (json.JSONDecodeError, FullFlowSemanticExecutionAdmissionError) as exc:
            raise FullFlowSemanticExecutionAdmissionError(
                f"invalid {label} line: {position}"
            ) from exc
        _require(isinstance(value, dict), f"{label} row must be an object")
        rows.append(value)
    return rows


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            indent=2,
        )
        + "\n"
    ).encode("utf-8")


def _jsonl_bytes(rows: Iterable[Mapping[str, Any]]) -> bytes:
    return b"".join(
        (
            json.dumps(
                row,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")
        for row in rows
    )


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _identifier(value: Any, label: str) -> str:
    _require(
        isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
        f"{label} is invalid",
    )
    return str(value)


def _digest(value: Any, label: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{label} is not lowercase SHA-256",
    )
    return str(value)


def _assert_no_private_values(value: Any, path: str = "$") -> None:
    """Reject label values and credential-bearing fields from public output."""

    if isinstance(value, Mapping):
        forbidden = {
            "api_key",
            "authorization",
            "bearer_token",
            "correct_answer_id",
            "credential_value",
            "password",
            "secret",
            "task_success",
            "token",
        }
        for key, child in value.items():
            _require(
                key.lower() not in forbidden,
                f"private field entered admission package at {path}.{key}",
            )
            _assert_no_private_values(child, f"{path}.{key}")
    elif isinstance(value, list):
        for position, child in enumerate(value):
            _assert_no_private_values(child, f"{path}[{position}]")


def _public_task_commitment(public_tasks: Mapping[str, Any]) -> str:
    rows = public_tasks.get("tasks")
    _require(isinstance(rows, list), "semantic public tasks are missing")
    commitments: list[dict[str, Any]] = []
    for row in rows:
        _require(isinstance(row, Mapping), "semantic public task is invalid")
        options = row.get("answer_options")
        _require(isinstance(options, list), "answer options are missing")
        commitments.append({
            "object_id": _identifier(row.get("object_id"), "object_id"),
            "task_binding_sha256": _digest(
                row.get("task_binding_sha256"),
                "task_binding_sha256",
            ),
            "success_scoring_rule": _identifier(
                row.get("success_scoring_rule"),
                "success_scoring_rule",
            ),
            "answer_option_ids": [
                _identifier(option.get("option_id"), "option_id")
                for option in options
                if isinstance(option, Mapping)
            ],
        })
    commitments.sort(
        key=lambda row: (row["object_id"], row["task_binding_sha256"])
    )
    return _sha256(_canonical_bytes(commitments))


def _trial_gap_ids(trial: Mapping[str, Any]) -> list[str]:
    family = trial.get("route_family")
    _require(family in _EXPECTED_ROUTE_FAMILIES, "unknown route family")
    gap_ids = set(_COMMON_GAPS)
    gap_ids.add(_ROUTE_GAP[str(family)])
    if family == "local-cache-derived":
        gap_ids.add("cache-state-lifecycle-attestation-v1")
    logical = trial.get("logical_trial")
    _require(isinstance(logical, Mapping), "semantic logical trial is missing")
    executor = logical.get("executor_node_id")
    if executor == "N8":
        gap_ids.add("n8-full-flow-route-runtime-v1")
    representations = trial.get("representation_identities")
    _require(isinstance(representations, list), "representations are missing")
    representation_ids = {
        row.get("representation_id")
        for row in representations
        if isinstance(row, Mapping)
    }
    if "raw_video" in representation_ids:
        gap_ids.add("raw-video-model-input-adapter-v1")
    if "multimodal_digest" in representation_ids:
        gap_ids.add("digest-model-input-adapter-v1")
    if len(representation_ids) > 1:
        gap_ids.add("multi-representation-fusion-adapter-v1")
    chains = trial.get("required_provisioning_chain_ids")
    _require(isinstance(chains, list), "provisioning chains are missing")
    if chains:
        gap_ids.add("n5-materialize-publish-runtime-v1")
    _require(gap_ids <= set(_GAP_CATALOG), "runtime gap catalog is incomplete")
    return sorted(gap_ids)


def _representative_smokes(
    trials: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    _require(bool(trials), "semantic trials are empty")
    representative_workload_id = str(min(
        trials,
        key=lambda row: int(row["order_index"]),
    )["workload_id"])
    by_cell = {
        (row["workload_id"], row["design_id"], row["repetition"]): row
        for row in trials
    }
    selections = [
        (
            "n7-raw",
            (representative_workload_id, "D0", 0),
            "raw",
            "N7",
            None,
            None,
        ),
        (
            "n7-indexed-raw",
            (representative_workload_id, "D1", 0),
            "indexed-raw",
            "N7",
            None,
            None,
        ),
        (
            "n7-remote-derived",
            (representative_workload_id, "D2", 0),
            "remote-derived",
            "N7",
            None,
            None,
        ),
        (
            "n7-cache-miss",
            (representative_workload_id, "D3", 0),
            "local-cache-derived",
            "N7",
            "miss",
            None,
        ),
        (
            "n7-cache-hit",
            (representative_workload_id, "D3", 1),
            "local-cache-derived",
            "N7",
            "hit",
            (representative_workload_id, "D3", 0),
        ),
        (
            "n8-raw",
            (representative_workload_id, "D4", 0),
            "raw",
            "N8",
            None,
            None,
        ),
        (
            "n8-indexed-raw",
            (representative_workload_id, "D5", 0),
            "indexed-raw",
            "N8",
            None,
            None,
        ),
        (
            "n8-remote-derived",
            (representative_workload_id, "D6", 0),
            "remote-derived",
            "N8",
            None,
            None,
        ),
        (
            "n8-cache-miss",
            (representative_workload_id, "D7", 0),
            "local-cache-derived",
            "N8",
            "miss",
            None,
        ),
        (
            "n8-cache-hit",
            (representative_workload_id, "D7", 1),
            "local-cache-derived",
            "N8",
            "hit",
            (representative_workload_id, "D7", 0),
        ),
    ]
    rows: list[dict[str, Any]] = []
    for (
        case_id,
        key,
        expected_family,
        expected_executor,
        branch,
        prerequisite,
    ) in selections:
        _require(key in by_cell, f"representative smoke cell is missing: {key}")
        trial = by_cell[key]
        _require(
            trial["route_family"] == expected_family,
            f"representative smoke route changed: {case_id}",
        )
        _require(
            trial["executor_node_id"] == expected_executor,
            f"representative smoke executor changed: {case_id}",
        )
        prerequisite_key = (
            None if prerequisite is None else by_cell[prerequisite]["trial_key"]
        )
        rows.append({
            "schema_version": SMOKE_SELECTION_SCHEMA_VERSION,
            "case_id": case_id,
            "trial_key": trial["trial_key"],
            "workload_id": trial["workload_id"],
            "workload_class": trial["workload_class"],
            "design_id": trial["design_id"],
            "repetition": trial["repetition"],
            "route_family": trial["route_family"],
            "expected_executor_node_id": expected_executor,
            "expected_cache_branch": branch,
            "cache_precondition": (
                None
                if branch is None
                else (
                    "empty-cache-scope-for-representation"
                    if branch == "miss"
                    else "prerequisite-insert-same-runtime-epoch"
                )
            ),
            "prerequisite_trial_key": prerequisite_key,
            "public_task_binding_sha256": trial[
                "public_task_binding_sha256"
            ],
            "representation_identities": trial[
                "representation_identities"
            ],
            "semantic_stage_keys": trial["semantic_stage_keys"],
            "required_runtime_adapter_ids": trial[
                "required_runtime_adapter_ids"
            ],
            "flowmesh_submission_authorized": False,
            "semantic_execution_performed": False,
        })
    return rows


def _documents(
    *,
    admission_id: str,
    worker_alias: str,
    semantic_root: Path,
    deployment_root: Path,
    oracle_root: Path,
    semantic_report: Mapping[str, Any],
    deployment_report: Mapping[str, Any],
    oracle_report: Mapping[str, Any],
) -> dict[str, bytes]:
    admission_id = _identifier(admission_id, "admission_id")
    worker_alias = _identifier(worker_alias, "worker_alias")
    semantic_plan = _read_json(
        semantic_root / SEMANTIC_PLAN_NAME,
        "semantic matrix plan",
    )
    semantic_trials = _read_jsonl(
        semantic_root / SEMANTIC_TRIALS_NAME,
        "semantic matrix trials",
    )
    semantic_stages = _read_jsonl(
        semantic_root / SEMANTIC_STAGES_NAME,
        "semantic matrix stages",
    )
    public_tasks = _read_json(
        semantic_root / PUBLIC_TASKS_NAME,
        "semantic public tasks",
    )
    deployment = _read_json(
        deployment_root / DEPLOYMENT_BINDING_NAME,
        "deployment binding",
    )
    oracle_manifest = _read_json(
        oracle_root / "n1-oracle-package.json",
        "N1 oracle manifest",
    )

    _require(
        semantic_plan["source_bindings"]["logical_route_plan_sha256"]
        == deployment["logical_plan_sha256"],
        "semantic matrix and deployment bind different logical plans",
    )
    public_commitment = _public_task_commitment(public_tasks)
    _require(
        oracle_report.get("public_task_set_sha256") == public_commitment,
        "N1 oracle does not bind the semantic public task set",
    )
    tasks_by_workload: dict[str, dict[str, Any]] = {}
    for task in public_tasks["tasks"]:
        _require(isinstance(task, dict), "semantic public task is invalid")
        workload_id = _identifier(task.get("workload_id"), "workload_id")
        _require(workload_id not in tasks_by_workload, "public workload repeats")
        tasks_by_workload[workload_id] = task

    bindings: dict[str, dict[str, Any]] = {}
    for row in deployment.get("service_bindings", []):
        _require(isinstance(row, dict), "deployment service binding is invalid")
        contract_id = _identifier(
            row.get("service_contract_id"),
            "service_contract_id",
        )
        _require(contract_id not in bindings, "deployment service repeats")
        bindings[contract_id] = row
    _require("N1.hidden-score" in bindings, "N1 score binding is missing")
    n1_env_names = set(bindings["N1.hidden-score"]["credential_env_names"])
    _require(
        _EXPECTED_N1_ENV_NAMES <= n1_env_names,
        "N1 hidden-score binding lacks runtime authentication env names",
    )
    credential_names = sorted({
        name
        for binding in bindings.values()
        for name in binding.get("credential_env_names", [])
    })
    _require(
        all(_ENVIRONMENT_NAME.fullmatch(name) for name in credential_names),
        "deployment contains a non-environment credential name",
    )

    bound_stages: list[dict[str, Any]] = []
    stage_by_key: dict[str, dict[str, Any]] = {}
    for semantic in semantic_stages:
        logical = semantic.get("logical_stage")
        _require(isinstance(logical, dict), "semantic logical stage is missing")
        contract_id = logical.get("service_contract_id")
        _require(contract_id in bindings, f"stage service is unbound: {contract_id}")
        binding = bindings[str(contract_id)]
        row = {
            "schema_version": BOUND_STAGE_SCHEMA_VERSION,
            "stage_key": semantic["stage_key"],
            "trial_key": semantic["trial_key"],
            "stage_index": logical["stage_index"],
            "phase": logical["phase"],
            "action": logical["action"],
            "condition": logical["condition"],
            "dependency_stage_keys": logical["dependency_stage_keys"],
            "service_contract_id": contract_id,
            "logical_node_ids": logical["logical_node_ids"],
            "deployment_adapter_id": binding["adapter_id"],
            "service_base_url": binding["base_url"],
            "credential_env_names": binding["credential_env_names"],
            "network_binding": (
                deployment["network_binding"]
                if binding["base_url"] is None
                else None
            ),
            "public_task_binding_sha256": semantic[
                "public_task_binding_sha256"
            ],
            "object_representation_identity": semantic[
                "object_representation_identity"
            ],
            "source_semantic_stage_sha256": _sha256(
                _canonical_bytes(semantic)
            ),
            "stage_result_handoff_mode": "route-coordinator-required",
            "direct_flowmesh_api_task_ready": False,
            "credential_values_included": False,
        }
        _require(row["stage_key"] not in stage_by_key, "bound stage repeats")
        stage_by_key[row["stage_key"]] = row
        bound_stages.append(row)

    bound_trials: list[dict[str, Any]] = []
    for semantic in semantic_trials:
        stage_keys = semantic["semantic_stage_keys"]
        _require(
            all(key in stage_by_key for key in stage_keys),
            f"semantic trial has an unbound stage: {semantic['trial_key']}",
        )
        gap_ids = _trial_gap_ids(semantic)
        logical = semantic["logical_trial"]
        executor_node_id = logical["executor_node_id"]
        coordinator_contract_id = f"{executor_node_id}.execution-compute"
        _require(
            coordinator_contract_id in bindings,
            f"route coordinator service is unbound: {coordinator_contract_id}",
        )
        coordinator_binding = bindings[coordinator_contract_id]
        _require(
            semantic["workload_id"] in tasks_by_workload,
            f"public task is missing: {semantic['workload_id']}",
        )
        public_task = tasks_by_workload[semantic["workload_id"]]
        _require(
            public_task["task_binding_sha256"]
            == semantic["public_task_binding_sha256"],
            f"public task binding changed: {semantic['trial_key']}",
        )
        row = {
            "schema_version": BOUND_TRIAL_SCHEMA_VERSION,
            "trial_key": semantic["trial_key"],
            "order_index": semantic["order_index"],
            "workload_id": semantic["workload_id"],
            "workload_class": semantic["workload_class"],
            "design_id": semantic["design_id"],
            "repetition": semantic["repetition"],
            "route_family": semantic["route_family"],
            "executor_node_id": executor_node_id,
            "public_task_binding_sha256": semantic[
                "public_task_binding_sha256"
            ],
            "public_task_binding": public_task,
            "artifact_object_id": semantic["artifact_object_id"],
            "representation_identities": semantic[
                "representation_identities"
            ],
            "semantic_stage_keys": stage_keys,
            "bound_stage_sha256": [
                _sha256(_canonical_bytes(stage_by_key[key]))
                for key in stage_keys
            ],
            "required_provisioning_chain_ids": semantic[
                "required_provisioning_chain_ids"
            ],
            "source_semantic_trial_sha256": _sha256(
                _canonical_bytes(semantic)
            ),
            "worker_alias": worker_alias,
            "flowmesh_execution_shape": "one-api-task-to-route-coordinator",
            "route_coordinator_binding": {
                "service_contract_id": coordinator_contract_id,
                "base_url": coordinator_binding["base_url"],
                "adapter_id": coordinator_binding["adapter_id"],
                "credential_env_names": coordinator_binding[
                    "credential_env_names"
                ],
            },
            "required_runtime_adapter_ids": gap_ids,
            "flowmesh_submission_authorized": False,
            "semantic_execution_performed": False,
            "credentials_recorded": False,
        }
        bound_trials.append(row)

    workload_ids = sorted({row["workload_id"] for row in bound_trials})
    design_ids = sorted({row["design_id"] for row in bound_trials})
    repetitions = sorted({row["repetition"] for row in bound_trials})
    expected_trial_count = (
        len(workload_ids) * len(design_ids) * len(repetitions)
    )
    _require(
        bool(workload_ids)
        and design_ids == [f"D{index}" for index in range(8)]
        and repetitions == list(range(len(repetitions)))
        and len(bound_trials) == expected_trial_count,
        "bound semantic matrix is not a complete workload-design-repetition grid",
    )
    _require(
        [row["order_index"] for row in bound_trials]
        == list(range(expected_trial_count)),
        "bound semantic trials are not in frozen order",
    )
    smokes = _representative_smokes(bound_trials)
    affected = Counter(
        gap_id
        for trial in bound_trials
        for gap_id in trial["required_runtime_adapter_ids"]
    )
    gaps = {
        "schema_version": RUNTIME_GAPS_SCHEMA_VERSION,
        "status": "BLOCKING_RUNTIME_ADAPTERS_ENUMERATED",
        "flowmesh_architecture": "one-api-task-to-route-coordinator",
        "why_static_stage_tasks_are_not_used": (
            "The FlowMesh APITask contract used by this repository preserves "
            "ordering but does not bind a predecessor response into a "
            "successor request body."
        ),
        "required_adapters": [
            {
                "adapter_id": gap_id,
                **_GAP_CATALOG[gap_id],
                "affected_trial_count": affected[gap_id],
                "implementation_status": "missing-for-semantic-matrix",
                "requires_upcloud": False,
            }
            for gap_id in sorted(affected)
        ],
        "known_partial_implementations": [
            {
                "implementation_id": (
                    "pathfinder.integrations.flowmesh.full_flow_trial-v2"
                ),
                "coverage": (
                    "one N4 sampled_frame_bundle -> N7 -> N6 -> N1 trial"
                ),
                "matrix_complete": False,
            },
            {
                "implementation_id": (
                    "pathfinder.integrations.flowmesh.container_matrix_runner"
                ),
                "coverage": "infrastructure-only static operation DAGs",
                "semantic_results_recorded": False,
            },
        ],
        "all_required_adapters_implemented": False,
        "flowmesh_workflow_rendering_authorized": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }

    trial_bytes = _jsonl_bytes(bound_trials)
    stage_bytes = _jsonl_bytes(bound_stages)
    gaps_bytes = _json_bytes(gaps)
    smokes_bytes = _jsonl_bytes(smokes)
    source_bindings = {
        "semantic_matrix_plan_sha256": semantic_report["plan_sha256"],
        "semantic_matrix_source_binding_sha256": semantic_report[
            "source_binding_sha256"
        ],
        "semantic_matrix_checksums_sha256": _sha256(
            (semantic_root / SEMANTIC_CHECKSUMS_NAME).read_bytes()
        ),
        "deployment_binding_sha256": deployment_report["binding_sha256"],
        "deployment_binding_file_sha256": _sha256(
            (deployment_root / DEPLOYMENT_BINDING_NAME).read_bytes()
        ),
        "oracle_id": oracle_report["oracle_id"],
        "oracle_public_task_set_sha256": public_commitment,
        "oracle_manifest_sha256": _sha256(
            (oracle_root / "n1-oracle-package.json").read_bytes()
        ),
    }
    admission: dict[str, Any] = {
        "schema_version": ADMISSION_SCHEMA_VERSION,
        "status": "BLOCKED_MISSING_ROUTE_RUNTIME_ADAPTERS",
        "admission_id": admission_id,
        "scenario_id": semantic_report["scenario_id"],
        "deployment_id": deployment_report["deployment_id"],
        "backend": deployment_report["backend"],
        "worker_pin": {"kind": "worker_alias", "value": worker_alias},
        "source_bindings": source_bindings,
        "source_binding_sha256": _sha256(_canonical_bytes(source_bindings)),
        "matrix_dimensions": {
            "workload_count": len(workload_ids),
            "design_count": len(design_ids),
            "repetitions": len(repetitions),
            "matrix_cell_count": len(workload_ids) * len(design_ids),
            "trial_count": len(bound_trials),
            "semantic_stage_count": len(bound_stages),
        },
        "route_family_trial_counts": dict(sorted(Counter(
            row["route_family"] for row in bound_trials
        ).items())),
        "runtime_credential_env_names": credential_names,
        "hidden_score_authentication": {
            "service_contract_id": "N1.hidden-score",
            "oracle_id": oracle_report["oracle_id"],
            "request_schema_version": "pathfinder.n1-score-request/v1alpha2",
            "result_schema_version": "pathfinder.n1-score-result/v1alpha2",
            "required_runtime_env_names": sorted(_EXPECTED_N1_ENV_NAMES),
            "one_request_per_run_trial_identity_required": True,
            "hmac_verification_required": True,
            "hidden_label_content_included": False,
        },
        "execution_admission": {
            "deployment_capability_coverage_complete": True,
            "artifact_content_availability_verified": False,
            "dynamic_stage_handoff_complete": False,
            "route_runtime_coverage_complete": False,
            "representative_smoke_count": len(smokes),
            "flowmesh_workflow_templates_included": False,
            "flowmesh_submission_authorized": False,
        },
        "runtime_gap_count": len(gaps["required_adapters"]),
        "output_sha256": {
            TRIALS_NAME: _sha256(trial_bytes),
            STAGES_NAME: _sha256(stage_bytes),
            GAPS_NAME: _sha256(gaps_bytes),
            SMOKES_NAME: _sha256(smokes_bytes),
        },
        "services_started": False,
        "workflow_submitted": False,
        "semantic_execution_performed": False,
        "performance_measured": False,
        "cost_measured": False,
        "credential_values_included": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    admission["admission_sha256"] = _sha256(_canonical_bytes(admission))
    _assert_no_private_values([
        admission,
        bound_trials,
        bound_stages,
        gaps,
        smokes,
    ])
    documents = {
        ADMISSION_NAME: _json_bytes(admission),
        TRIALS_NAME: trial_bytes,
        STAGES_NAME: stage_bytes,
        GAPS_NAME: gaps_bytes,
        SMOKES_NAME: smokes_bytes,
    }
    documents[CHECKSUMS_NAME] = b"".join(
        f"{_sha256(documents[name])}  {name}\n".encode("utf-8")
        for name in sorted(_CONTENT_FILES)
    )
    return documents


def _verify_files(root: Path) -> dict[str, Any]:
    _require(root.is_dir(), "semantic execution admission directory is missing")
    entries = list(root.iterdir())
    _require(
        all(path.is_file() and not path.is_symlink() for path in entries),
        "semantic execution admission contains a non-regular file",
    )
    _require(
        {path.name for path in entries} == _ALL_FILES,
        "semantic execution admission file set changed",
    )
    expected = b"".join(
        f"{_sha256((root / name).read_bytes())}  {name}\n".encode("utf-8")
        for name in sorted(_CONTENT_FILES)
    )
    _require(
        (root / CHECKSUMS_NAME).read_bytes() == expected,
        "semantic execution admission checksums failed",
    )
    admission = _read_json(root / ADMISSION_NAME, "execution admission")
    _require(
        admission.get("schema_version") == ADMISSION_SCHEMA_VERSION
        and admission.get("status")
        == "BLOCKED_MISSING_ROUTE_RUNTIME_ADAPTERS",
        "semantic execution admission status was weakened",
    )
    supplied_sha = _digest(
        admission.pop("admission_sha256", None),
        "admission_sha256",
    )
    _require(
        supplied_sha == _sha256(_canonical_bytes(admission)),
        "semantic execution admission digest failed",
    )
    admission["admission_sha256"] = supplied_sha
    trials = _read_jsonl(root / TRIALS_NAME, "bound semantic trials")
    stages = _read_jsonl(root / STAGES_NAME, "bound semantic stages")
    smokes = _read_jsonl(root / SMOKES_NAME, "semantic smoke selection")
    gaps = _read_json(root / GAPS_NAME, "semantic runtime gaps")
    dimensions = admission.get("matrix_dimensions")
    _require(
        isinstance(dimensions, dict)
        and isinstance(dimensions.get("trial_count"), int)
        and dimensions["trial_count"] > 0
        and len(trials) == dimensions["trial_count"],
        "semantic execution trial count changed",
    )
    _require(len(smokes) == 10, "semantic smoke selection count changed")
    _require(
        {row.get("case_id") for row in smokes}
        == {
            "n7-raw",
            "n7-indexed-raw",
            "n7-remote-derived",
            "n7-cache-miss",
            "n7-cache-hit",
            "n8-raw",
            "n8-indexed-raw",
            "n8-remote-derived",
            "n8-cache-miss",
            "n8-cache-hit",
        },
        "semantic smoke cases changed",
    )
    _require(
        all(row.get("flowmesh_submission_authorized") is False for row in trials),
        "a blocked semantic trial became submittable",
    )
    _require(
        gaps.get("all_required_adapters_implemented") is False
        and gaps.get("flowmesh_workflow_rendering_authorized") is False,
        "runtime gap classification was weakened",
    )
    _require(
        admission.get("output_sha256")
        == {
            TRIALS_NAME: _sha256((root / TRIALS_NAME).read_bytes()),
            STAGES_NAME: _sha256((root / STAGES_NAME).read_bytes()),
            GAPS_NAME: _sha256((root / GAPS_NAME).read_bytes()),
            SMOKES_NAME: _sha256((root / SMOKES_NAME).read_bytes()),
        },
        "semantic execution output digests changed",
    )
    _require(
        admission.get("matrix_dimensions", {}).get("semantic_stage_count")
        == len(stages),
        "semantic execution stage count changed",
    )
    _assert_no_private_values([admission, trials, stages, gaps, smokes])
    return admission


def _publish(target: Path, documents: Mapping[str, bytes]) -> None:
    _require(not target.exists(), f"admission output already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    parent = Path(tempfile.mkdtemp(prefix=".semantic-admission-", dir=target.parent))
    stage = parent / "output"
    try:
        stage.mkdir()
        for name, payload in documents.items():
            path = stage / name
            with path.open("wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        _verify_files(stage)
        os.replace(stage, target)
    finally:
        shutil.rmtree(parent, ignore_errors=True)


def freeze_full_flow_semantic_execution_admission(
    semantic_matrix_dir: str | Path,
    deployment_binding_dir: str | Path,
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    public_task_set_path: str | Path,
    artifact_binding_path: str | Path,
    n1_oracle_package_dir: str | Path,
    *,
    worker_alias: str,
    output_dir: str | Path,
    admission_id: str = "full-flow-semantic-execution-admission-v1",
) -> dict[str, Any]:
    """Freeze a fail-closed, deployment-bound semantic execution admission."""

    semantic_root = Path(semantic_matrix_dir).resolve()
    deployment_root = Path(deployment_binding_dir).resolve()
    oracle_root = Path(n1_oracle_package_dir).resolve()
    semantic_report = verify_full_flow_semantic_matrix(
        semantic_root,
        logical_route_dir,
        scenario_path,
        container_plan_dir,
        public_task_set_path,
        artifact_binding_path,
    )
    deployment_report = verify_full_flow_deployment_binding(
        deployment_root,
        logical_plan_dir=logical_route_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    oracle_report = verify_n1_oracle_package(oracle_root)
    documents = _documents(
        admission_id=admission_id,
        worker_alias=worker_alias,
        semantic_root=semantic_root,
        deployment_root=deployment_root,
        oracle_root=oracle_root,
        semantic_report=semantic_report,
        deployment_report=deployment_report,
        oracle_report=oracle_report,
    )
    target = Path(output_dir).resolve()
    _publish(target, documents)
    admission = _verify_files(target)
    return {
        "status": admission["status"],
        "admission_id": admission["admission_id"],
        "admission_sha256": admission["admission_sha256"],
        "trial_count": admission["matrix_dimensions"]["trial_count"],
        "semantic_stage_count": admission["matrix_dimensions"][
            "semantic_stage_count"
        ],
        "representative_smoke_count": 10,
        "runtime_gap_count": admission["runtime_gap_count"],
        "flowmesh_submission_authorized": False,
        "output_dir": str(target),
        "services_started": False,
        "workflow_submitted": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def verify_full_flow_semantic_execution_admission(
    admission_dir: str | Path,
    semantic_matrix_dir: str | Path,
    deployment_binding_dir: str | Path,
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    public_task_set_path: str | Path,
    artifact_binding_path: str | Path,
    n1_oracle_package_dir: str | Path,
) -> dict[str, Any]:
    """Verify checksums and reproduce the admission from every frozen input."""

    root = Path(admission_dir).resolve()
    admission = _verify_files(root)
    semantic_root = Path(semantic_matrix_dir).resolve()
    deployment_root = Path(deployment_binding_dir).resolve()
    oracle_root = Path(n1_oracle_package_dir).resolve()
    semantic_report = verify_full_flow_semantic_matrix(
        semantic_root,
        logical_route_dir,
        scenario_path,
        container_plan_dir,
        public_task_set_path,
        artifact_binding_path,
    )
    deployment_report = verify_full_flow_deployment_binding(
        deployment_root,
        logical_plan_dir=logical_route_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    oracle_report = verify_n1_oracle_package(oracle_root)
    expected = _documents(
        admission_id=admission["admission_id"],
        worker_alias=admission["worker_pin"]["value"],
        semantic_root=semantic_root,
        deployment_root=deployment_root,
        oracle_root=oracle_root,
        semantic_report=semantic_report,
        deployment_report=deployment_report,
        oracle_report=oracle_report,
    )
    for name in sorted(_ALL_FILES):
        _require(
            (root / name).read_bytes() == expected[name],
            f"semantic execution admission does not match inputs: {name}",
        )
    return {
        "status": "VERIFIED_BLOCKED",
        "admission_id": admission["admission_id"],
        "admission_sha256": admission["admission_sha256"],
        "trial_count": admission["matrix_dimensions"]["trial_count"],
        "semantic_stage_count": admission["matrix_dimensions"][
            "semantic_stage_count"
        ],
        "representative_smoke_count": 10,
        "runtime_gap_count": admission["runtime_gap_count"],
        "flowmesh_submission_authorized": False,
        "source_binding_checked": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "ADMISSION_NAME",
    "ADMISSION_SCHEMA_VERSION",
    "CHECKSUMS_NAME",
    "GAPS_NAME",
    "SMOKES_NAME",
    "STAGES_NAME",
    "TRIALS_NAME",
    "FullFlowSemanticExecutionAdmissionError",
    "freeze_full_flow_semantic_execution_admission",
    "verify_full_flow_semantic_execution_admission",
]
