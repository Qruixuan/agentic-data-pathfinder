"""Freeze public semantic bindings over the endpoint-free 4x8 route plan.

This compiler is intentionally a planning boundary, not an executor.  It
combines the already verified logical service graph with operator-supplied
public tasks.  The result is self-contained enough for a later deployment or
FlowMesh renderer while remaining free of endpoints, credentials, hidden
labels, and claims about measured quality, performance, or cost.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

from .full_flow_logical_routes import (
    PLAN_NAME as LOGICAL_PLAN_NAME,
    SERVICE_CATALOG_NAME as LOGICAL_SERVICE_CATALOG_NAME,
    STAGES_NAME as LOGICAL_STAGES_NAME,
    TRIALS_NAME as LOGICAL_TRIALS_NAME,
    verify_full_flow_logical_routes,
)
from .hidden_oracle import (
    N1OracleError,
    N1_PUBLIC_TASK_SCHEMA_VERSION,
    assert_hidden_oracle_fields_absent,
    build_n1_public_task_binding,
)


SEMANTIC_MATRIX_PLAN_SCHEMA_VERSION = (
    "pathfinder.full-flow-semantic-matrix-plan/v1alpha1"
)
SEMANTIC_MATRIX_TRIAL_SCHEMA_VERSION = (
    "pathfinder.full-flow-semantic-matrix-trial/v1alpha1"
)
SEMANTIC_MATRIX_STAGE_SCHEMA_VERSION = (
    "pathfinder.full-flow-semantic-matrix-stage/v1alpha1"
)
SEMANTIC_SERVICE_CATALOG_SCHEMA_VERSION = (
    "pathfinder.full-flow-semantic-service-catalog/v1alpha1"
)
PUBLIC_TASK_SET_SCHEMA_VERSION = "pathfinder.public-task-set/v1alpha1"
ARTIFACT_BINDING_SET_SCHEMA_VERSION = (
    "pathfinder.full-flow-artifact-binding-set/v1alpha1"
)

PLAN_NAME = "semantic-matrix-plan.json"
TRIALS_NAME = "semantic-matrix-trials.jsonl"
STAGES_NAME = "semantic-matrix-stages.jsonl"
SERVICE_CATALOG_NAME = "semantic-service-contracts.json"
PUBLIC_TASKS_NAME = "semantic-public-tasks.json"
ARTIFACT_BINDINGS_NAME = "semantic-artifact-bindings.json"
CHECKSUMS_NAME = "SHA256SUMS"

_CONTENT_FILES = {
    PLAN_NAME,
    TRIALS_NAME,
    STAGES_NAME,
    SERVICE_CATALOG_NAME,
    PUBLIC_TASKS_NAME,
    ARTIFACT_BINDINGS_NAME,
}
_ALL_FILES = _CONTENT_FILES | {CHECKSUMS_NAME}
_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_WINDOWS_ABSOLUTE = re.compile(r"^[A-Za-z]:[\\/]")
_URL_PREFIXES = ("http://", "https://", "file://", "ssh://")
_WORKLOAD_CLASSES = {f"W{index}" for index in range(1, 5)}
_DESIGN_IDS = {f"D{index}" for index in range(8)}
_ROUTE_FAMILIES = {
    "raw",
    "indexed-raw",
    "remote-derived",
    "local-cache-derived",
}
_ALLOWED_ACTIONS = {
    "access-derived-artifact",
    "access-raw-artifact",
    "admit-trial",
    "infer",
    "insert",
    "join-hit-or-miss-branch",
    "lookup",
    "materialize-representation",
    "prepare-model-input",
    "publish-derived-artifact",
    "query-index",
    "read",
    "score-hidden-answer",
    "transfer-bytes",
}
_FORBIDDEN_BINDING_FIELDS = {
    "api_key",
    "authorization",
    "bearer_token",
    "credential",
    "credentials",
    "endpoint",
    "endpoint_url",
    "host",
    "host_path",
    "hostname",
    "mount_path",
    "password",
    "secret",
    "token",
    "url",
}
_FORBIDDEN_OUTCOME_FIELDS = {
    "correct_answer_id",
    "score",
    "task_success",
    "task_success_by_design",
}


class FullFlowSemanticMatrixError(ValueError):
    """Raised when the public semantic matrix cannot be frozen safely."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowSemanticMatrixError(message)


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _invalid_number(value: str) -> None:
    raise FullFlowSemanticMatrixError(f"non-finite JSON number: {value}")


def _json(path: Path, label: str) -> tuple[bytes, dict[str, Any]]:
    try:
        raw = path.read_bytes()
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_keys,
            parse_constant=_invalid_number,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowSemanticMatrixError(
            f"cannot read valid {label}: {path.name}"
        ) from exc
    _require(isinstance(value, dict), f"{label} must be an object")
    return raw, value


def _jsonl(path: Path, label: str) -> tuple[bytes, list[dict[str, Any]]]:
    try:
        raw = path.read_bytes()
        text = raw.decode("utf-8")
    except (OSError, UnicodeError) as exc:
        raise FullFlowSemanticMatrixError(
            f"cannot read valid {label}: {path.name}"
        ) from exc
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        _require(bool(line.strip()), f"blank {label} line: {line_number}")
        try:
            value = json.loads(
                line,
                object_pairs_hook=_unique_keys,
                parse_constant=_invalid_number,
            )
        except (json.JSONDecodeError, FullFlowSemanticMatrixError) as exc:
            raise FullFlowSemanticMatrixError(
                f"invalid {label} at line {line_number}: {exc}"
            ) from exc
        _require(isinstance(value, dict), f"{label} row must be an object")
        rows.append(value)
    return raw, rows


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
        isinstance(value, str) and _SAFE_ID.fullmatch(value) is not None,
        f"{label} is invalid",
    )
    return str(value)


def _strict_fields(
    value: Mapping[str, Any],
    expected: set[str],
    label: str,
) -> None:
    _require(
        set(value) == expected,
        f"{label} fields changed: expected {sorted(expected)}, got "
        f"{sorted(value)}",
    )


def _assert_public_endpoint_free(value: Any, label: str = "document") -> None:
    """Reject hidden fields, runtime bindings, addresses, and host paths."""

    try:
        assert_hidden_oracle_fields_absent(value)
    except N1OracleError as exc:
        raise FullFlowSemanticMatrixError(str(exc)) from exc
    if isinstance(value, Mapping):
        for key, child in value.items():
            _require(
                key.lower() not in _FORBIDDEN_BINDING_FIELDS,
                f"{label} contains forbidden runtime binding field: {key}",
            )
            _require(
                key.lower() not in _FORBIDDEN_OUTCOME_FIELDS,
                f"{label} contains a semantic outcome field: {key}",
            )
            _assert_public_endpoint_free(child, f"{label}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _assert_public_endpoint_free(child, f"{label}[{index}]")
    elif isinstance(value, str):
        lowered = value.lower()
        _require(
            not lowered.startswith(_URL_PREFIXES),
            f"{label} contains a runtime address",
        )
        _require(
            not value.startswith(("/", "\\\\"))
            and _WINDOWS_ABSOLUTE.match(value) is None,
            f"{label} contains an absolute host path",
        )
    elif isinstance(value, float):
        _require(math.isfinite(value), f"{label} contains a non-finite number")


def _normalise_artifact_bindings(
    artifact_binding_path: Path,
    logical_stages: list[dict[str, Any]],
    logical_trials: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    _, document = _json(artifact_binding_path, "artifact binding set")
    _assert_public_endpoint_free(document, "artifact binding set")
    _strict_fields(
        document,
        {
            "schema_version",
            "binding_set_id",
            "objects",
            "credentials_recorded",
        },
        "artifact binding set",
    )
    _require(
        document.get("schema_version") == ARTIFACT_BINDING_SET_SCHEMA_VERSION,
        "unsupported artifact binding set schema_version",
    )
    binding_set_id = _identifier(
        document.get("binding_set_id"),
        "binding_set_id",
    )
    _require(
        document.get("credentials_recorded") is False,
        "artifact binding set records credentials",
    )
    objects = document.get("objects")
    _require(isinstance(objects, list), "artifact bindings must be a list")

    expected_representations: dict[str, set[str]] = defaultdict(set)
    for trial in logical_trials:
        logical_object_id = _identifier(
            trial.get("object_id"),
            "logical object_id",
        )
        for representation_id in trial.get("representation_ids", []):
            expected_representations[logical_object_id].add(
                _identifier(representation_id, "representation_id")
            )
    for stage in logical_stages:
        logical_object_id = _identifier(
            stage.get("object_id"),
            "logical stage object_id",
        )
        representation_id = stage.get("representation_id")
        if representation_id is not None:
            expected_representations[logical_object_id].add(
                _identifier(representation_id, "representation_id")
            )
    _require(
        len(expected_representations) == 4,
        "logical route package does not contain four artifact objects",
    )

    by_logical_object: dict[str, dict[str, Any]] = {}
    artifact_object_ids: set[str] = set()
    for position, item in enumerate(objects):
        _require(
            isinstance(item, Mapping),
            f"artifact bindings[{position}] must be an object",
        )
        _strict_fields(
            item,
            {
                "logical_object_id",
                "artifact_object_id",
                "representations",
            },
            f"artifact bindings[{position}]",
        )
        logical_object_id = _identifier(
            item.get("logical_object_id"),
            "logical_object_id",
        )
        artifact_object_id = _identifier(
            item.get("artifact_object_id"),
            "artifact_object_id",
        )
        _require(
            logical_object_id in expected_representations,
            f"artifact binding has no logical object: {logical_object_id}",
        )
        _require(
            logical_object_id not in by_logical_object,
            f"duplicate logical artifact binding: {logical_object_id}",
        )
        _require(
            artifact_object_id not in artifact_object_ids,
            f"artifact object is bound more than once: {artifact_object_id}",
        )
        representations = item.get("representations")
        _require(
            isinstance(representations, list),
            f"artifact representations must be a list: {logical_object_id}",
        )
        by_representation: dict[str, dict[str, Any]] = {}
        for rep_position, representation in enumerate(representations):
            _require(
                isinstance(representation, Mapping),
                f"artifact representation {rep_position} must be an object",
            )
            _strict_fields(
                representation,
                {
                    "representation_id",
                    "artifact_sha256",
                    "artifact_size_bytes",
                    "object_catalog_version",
                },
                f"artifact representation {logical_object_id}[{rep_position}]",
            )
            representation_id = _identifier(
                representation.get("representation_id"),
                "representation_id",
            )
            digest = representation.get("artifact_sha256")
            size = representation.get("artifact_size_bytes")
            catalog = _identifier(
                representation.get("object_catalog_version"),
                "object_catalog_version",
            )
            _require(
                isinstance(digest, str) and _SHA256.fullmatch(digest) is not None,
                "artifact_sha256 is invalid",
            )
            _require(
                type(size) is int and size > 0,
                "artifact_size_bytes must be a positive integer",
            )
            _require(
                representation_id not in by_representation,
                f"duplicate artifact representation: {representation_id}",
            )
            by_representation[representation_id] = {
                "representation_id": representation_id,
                "artifact_sha256": digest,
                "artifact_size_bytes": size,
                "object_catalog_version": catalog,
            }
        _require(
            set(by_representation) == expected_representations[logical_object_id],
            f"artifact representations do not match logical routes: "
            f"{logical_object_id}",
        )
        normalised = {
            "logical_object_id": logical_object_id,
            "artifact_object_id": artifact_object_id,
            "representations": [
                by_representation[key] for key in sorted(by_representation)
            ],
        }
        by_logical_object[logical_object_id] = normalised
        artifact_object_ids.add(artifact_object_id)
    _require(
        set(by_logical_object) == set(expected_representations),
        "artifact binding set is missing one or more logical objects",
    )
    normalised_document = {
        "schema_version": ARTIFACT_BINDING_SET_SCHEMA_VERSION,
        "binding_set_id": binding_set_id,
        "objects": [
            by_logical_object[key] for key in sorted(by_logical_object)
        ],
        "credentials_recorded": False,
    }
    return normalised_document, by_logical_object


def _normalise_public_tasks(
    public_task_set_path: Path,
    logical_trials: list[dict[str, Any]],
    artifacts_by_logical_object: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    _, document = _json(public_task_set_path, "public task set")
    _assert_public_endpoint_free(document, "public task set")
    _strict_fields(
        document,
        {
            "schema_version",
            "task_plane_id",
            "tasks",
            "label_values_included",
            "credentials_recorded",
        },
        "public task set",
    )
    _require(
        document.get("schema_version") == PUBLIC_TASK_SET_SCHEMA_VERSION,
        "unsupported public task set schema_version",
    )
    task_plane_id = _identifier(document.get("task_plane_id"), "task_plane_id")
    _require(
        document.get("label_values_included") is False,
        "public task set claims to contain label values",
    )
    _require(
        document.get("credentials_recorded") is False,
        "public task set records credentials",
    )
    tasks_value = document.get("tasks")
    _require(isinstance(tasks_value, list), "public tasks must be a list")

    expected_workloads: dict[str, str] = {}
    for trial in logical_trials:
        workload_id = _identifier(trial.get("workload_id"), "workload_id")
        logical_object_id = _identifier(
            trial.get("object_id"),
            "logical object_id",
        )
        _require(
            logical_object_id in artifacts_by_logical_object,
            f"logical workload has no artifact binding: {workload_id}",
        )
        artifact_object_id = artifacts_by_logical_object[logical_object_id][
            "artifact_object_id"
        ]
        previous = expected_workloads.setdefault(workload_id, artifact_object_id)
        _require(
            previous == artifact_object_id,
            f"logical workload maps to multiple objects: {workload_id}",
        )
    _require(
        len(expected_workloads) == 4,
        "logical route package does not contain four workloads",
    )

    by_workload: dict[str, dict[str, Any]] = {}
    by_object: dict[str, dict[str, Any]] = {}
    for position, item in enumerate(tasks_value):
        _require(
            isinstance(item, Mapping),
            f"public tasks[{position}] must be an object",
        )
        try:
            rebuilt = build_n1_public_task_binding(
                workload_id=item.get("workload_id"),
                object_id=item.get("object_id"),
                task_class_id=item.get("task_class_id"),
                question=item.get("question"),
                answer_options=item.get("answer_options"),
                success_scoring_rule=item.get("success_scoring_rule"),
            )
        except N1OracleError as exc:
            raise FullFlowSemanticMatrixError(
                f"invalid public task binding at position {position}: {exc}"
            ) from exc
        _require(
            dict(item) == rebuilt,
            f"public task binding changed at position {position}",
        )
        workload_id = rebuilt["workload_id"]
        object_id = rebuilt["object_id"]
        _require(
            workload_id in expected_workloads,
            f"public task has no logical workload: {workload_id}",
        )
        _require(
            expected_workloads[workload_id] == object_id,
            f"public task artifact object does not match logical workload: "
            f"{workload_id}",
        )
        _require(
            workload_id not in by_workload,
            f"duplicate public task workload: {workload_id}",
        )
        _require(
            object_id not in by_object,
            f"multiple public tasks bind object: {object_id}",
        )
        by_workload[workload_id] = rebuilt
        by_object[object_id] = rebuilt
    _require(
        set(by_workload) == set(expected_workloads),
        "public task set is missing one or more logical workloads",
    )
    tasks = [by_workload[key] for key in sorted(by_workload)]
    normalised = {
        "schema_version": PUBLIC_TASK_SET_SCHEMA_VERSION,
        "task_plane_id": task_plane_id,
        "tasks": tasks,
        "label_values_included": False,
        "credentials_recorded": False,
    }
    _assert_public_endpoint_free(normalised, "public task set")
    return normalised, by_workload


def _load_verified_logical_source(
    logical_route_dir: Path,
    scenario_path: Path,
    container_plan_dir: Path,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    try:
        verified = verify_full_flow_logical_routes(
            logical_route_dir,
            scenario_path,
            container_plan_dir,
        )
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        raise FullFlowSemanticMatrixError(
            "logical route package failed source-bound verification"
        ) from exc
    _, plan = _json(logical_route_dir / LOGICAL_PLAN_NAME, "logical route plan")
    _, catalog = _json(
        logical_route_dir / LOGICAL_SERVICE_CATALOG_NAME,
        "logical service catalog",
    )
    _, stages = _jsonl(
        logical_route_dir / LOGICAL_STAGES_NAME,
        "logical stages",
    )
    _, trials = _jsonl(
        logical_route_dir / LOGICAL_TRIALS_NAME,
        "logical trials",
    )
    _require(
        verified.get("plan_sha256") == plan.get("plan_sha256"),
        "logical route verification returned a different plan digest",
    )
    _require(len(trials) == 64, "logical route package is not 64 trials")
    _require(len(stages) > 64, "logical route stage graph is incomplete")
    _assert_public_endpoint_free([plan, catalog, stages, trials], "logical source")
    return plan, catalog, stages, trials


def _validate_service_actions(
    catalog: Mapping[str, Any],
    stages: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    rows = catalog.get("service_contracts")
    _require(isinstance(rows, list), "logical service contracts are missing")
    contracts: dict[str, dict[str, Any]] = {}
    for position, row in enumerate(rows):
        _require(
            isinstance(row, dict),
            f"service contract {position} must be an object",
        )
        contract_id = _identifier(
            row.get("service_contract_id"),
            "service_contract_id",
        )
        actions = row.get("actions")
        _require(
            isinstance(actions, list)
            and bool(actions)
            and len(actions) == len(set(actions))
            and all(action in _ALLOWED_ACTIONS for action in actions),
            f"unsupported service action in contract: {contract_id}",
        )
        _require(contract_id not in contracts, "duplicate service contract")
        contracts[contract_id] = row
    for stage in stages:
        _require(isinstance(stage, dict), "logical stage must be an object")
        key = stage.get("stage_key")
        contract_id = stage.get("service_contract_id")
        action = stage.get("action")
        _require(
            contract_id in contracts,
            f"stage references unknown service contract: {key}",
        )
        _require(
            action in _ALLOWED_ACTIONS
            and action in contracts[contract_id]["actions"],
            f"unsupported service action for stage: {key}",
        )
    return contracts


def _semantic_documents(
    logical_route_dir: Path,
    scenario_path: Path,
    container_plan_dir: Path,
    public_task_set_path: Path,
    artifact_binding_path: Path,
    compiler_id: str,
) -> dict[str, bytes]:
    compiler_id = _identifier(compiler_id, "compiler_id")
    logical_plan, logical_catalog, logical_stages, logical_trials = (
        _load_verified_logical_source(
            logical_route_dir,
            scenario_path,
            container_plan_dir,
        )
    )
    contracts = _validate_service_actions(logical_catalog, logical_stages)
    artifact_bindings, artifacts_by_logical_object = (
        _normalise_artifact_bindings(
            artifact_binding_path,
            logical_stages,
            logical_trials,
        )
    )
    public_tasks, tasks_by_workload = _normalise_public_tasks(
        public_task_set_path,
        logical_trials,
        artifacts_by_logical_object,
    )
    task_by_logical_object = {
        trial["object_id"]: tasks_by_workload[trial["workload_id"]]
        for trial in logical_trials
    }

    service_catalog = {
        "schema_version": SEMANTIC_SERVICE_CATALOG_SCHEMA_VERSION,
        "source_logical_plan_sha256": logical_plan["plan_sha256"],
        "source_logical_service_catalog_sha256": _sha256(
            _canonical_bytes(logical_catalog)
        ),
        "service_contracts": [contracts[key] for key in sorted(contracts)],
        "deployment_binding_included": False,
        "credentials_recorded": False,
    }
    semantic_stages: list[dict[str, Any]] = []
    stages_by_trial: dict[str, list[str]] = defaultdict(list)
    for stage in logical_stages:
        object_id = stage["object_id"]
        _require(
            object_id in task_by_logical_object,
            f"logical stage object has no public task: {object_id}",
        )
        task = task_by_logical_object[object_id]
        artifact = artifacts_by_logical_object[object_id]
        representation_bindings = {
            row["representation_id"]: row
            for row in artifact["representations"]
        }
        representation_id = stage["representation_id"]
        identity = {
            "logical_object_id": object_id,
            "artifact_object_id": artifact["artifact_object_id"],
            "representation_id": representation_id,
            "representation_binding": (
                None
                if representation_id is None
                else representation_bindings[representation_id]
            ),
        }
        row = {
            "schema_version": SEMANTIC_MATRIX_STAGE_SCHEMA_VERSION,
            "stage_key": stage["stage_key"],
            "trial_key": stage["trial_key"],
            "public_task_workload_id": task["workload_id"],
            "public_task_binding_sha256": task["task_binding_sha256"],
            "object_representation_identity": identity,
            "source_logical_stage_sha256": _sha256(
                _canonical_bytes(stage)
            ),
            "logical_stage": stage,
            "deployment_binding_included": False,
            "credentials_recorded": False,
            "hidden_label_included": False,
        }
        semantic_stages.append(row)
        if stage["trial_key"] is not None:
            stages_by_trial[stage["trial_key"]].append(stage["stage_key"])

    semantic_trials: list[dict[str, Any]] = []
    for trial in logical_trials:
        workload_id = trial["workload_id"]
        task = tasks_by_workload[workload_id]
        artifact = artifacts_by_logical_object[trial["object_id"]]
        representation_bindings = {
            row["representation_id"]: row
            for row in artifact["representations"]
        }
        representations = [
            {
                "logical_object_id": trial["object_id"],
                "artifact_object_id": artifact["artifact_object_id"],
                "representation_id": representation_id,
                "representation_binding": representation_bindings[
                    representation_id
                ],
            }
            for representation_id in trial["representation_ids"]
        ]
        expected_stage_keys = (
            trial["execution_stage_keys"] + trial["evaluation_stage_keys"]
        )
        _require(
            stages_by_trial[trial["trial_key"]] == expected_stage_keys,
            f"logical trial stage order changed: {trial['trial_key']}",
        )
        semantic_trials.append({
            "schema_version": SEMANTIC_MATRIX_TRIAL_SCHEMA_VERSION,
            "trial_key": trial["trial_key"],
            "order_index": trial["order_index"],
            "workload_id": workload_id,
            "workload_class": trial["workload_class"],
            "design_id": trial["design_id"],
            "repetition": trial["repetition"],
            "route_family": trial["route_family"],
            "logical_object_id": trial["object_id"],
            "artifact_object_id": artifact["artifact_object_id"],
            "representation_identities": representations,
            "public_task_binding_sha256": task["task_binding_sha256"],
            "public_task_schema_version": N1_PUBLIC_TASK_SCHEMA_VERSION,
            "semantic_stage_keys": expected_stage_keys,
            "required_provisioning_chain_ids": trial[
                "required_provisioning_chain_ids"
            ],
            "source_logical_trial_sha256": _sha256(
                _canonical_bytes(trial)
            ),
            "logical_trial": trial,
            "deployment_binding_included": False,
            "credentials_recorded": False,
            "hidden_label_included": False,
        })

    public_bytes = _json_bytes(public_tasks)
    artifact_bytes = _json_bytes(artifact_bindings)
    service_bytes = _json_bytes(service_catalog)
    stage_bytes = _jsonl_bytes(semantic_stages)
    trial_bytes = _jsonl_bytes(semantic_trials)
    source_bindings = {
        "logical_route_plan_sha256": logical_plan["plan_sha256"],
        "logical_route_source_binding_sha256": logical_plan[
            "source_binding_sha256"
        ],
        "logical_route_package_checksums_sha256": _sha256(
            (logical_route_dir / CHECKSUMS_NAME).read_bytes()
        ),
        "public_task_set_sha256": _sha256(public_bytes),
        "artifact_binding_set_sha256": _sha256(artifact_bytes),
    }
    output_sha256 = {
        PUBLIC_TASKS_NAME: _sha256(public_bytes),
        ARTIFACT_BINDINGS_NAME: _sha256(artifact_bytes),
        SERVICE_CATALOG_NAME: _sha256(service_bytes),
        STAGES_NAME: _sha256(stage_bytes),
        TRIALS_NAME: _sha256(trial_bytes),
    }
    route_counts = Counter(row["route_family"] for row in semantic_trials)
    plan: dict[str, Any] = {
        "schema_version": SEMANTIC_MATRIX_PLAN_SCHEMA_VERSION,
        "status": "FROZEN_ENDPOINT_FREE_SEMANTIC_MATRIX",
        "compiler_id": compiler_id,
        "scenario_id": logical_plan["scenario_id"],
        "task_plane_id": public_tasks["task_plane_id"],
        "artifact_binding_set_id": artifact_bindings["binding_set_id"],
        "source_bindings": source_bindings,
        "source_binding_sha256": _sha256(_canonical_bytes(source_bindings)),
        "matrix_dimensions": {
            "workload_classes": sorted(_WORKLOAD_CLASSES),
            "design_ids": sorted(_DESIGN_IDS),
            "repetitions": 2,
            "matrix_cell_count": 32,
            "trial_count": len(semantic_trials),
        },
        "coverage_summary": {
            "public_task_count": len(public_tasks["tasks"]),
            "artifact_object_binding_count": len(artifact_bindings["objects"]),
            "service_contract_count": len(contracts),
            "semantic_stage_count": len(semantic_stages),
            "trial_stage_count": sum(
                row["logical_stage"]["scope_kind"] == "trial"
                for row in semantic_stages
            ),
            "provisioning_stage_count": sum(
                row["logical_stage"]["scope_kind"] == "artifact-provisioning"
                for row in semantic_stages
            ),
            "conditional_stage_count": sum(
                row["logical_stage"]["condition"] is not None
                for row in semantic_stages
            ),
            "route_family_trial_counts": dict(sorted(route_counts.items())),
        },
        "execution_boundary": {
            "supported_capabilities": [
                "conditional-cache-branch-preservation",
                "endpoint-free-deployment-input",
                "logical-service-dag-preservation",
                "public-task-binding",
            ],
            "unsupported_capabilities": [
                "hidden-label-scoring",
                "semantic-task-execution",
                "performance-measurement",
                "cost-measurement",
            ],
            "deployment_binding_required": True,
            "flowmesh_rendering_required": True,
            "hidden_oracle_package_required_at_n1": True,
            "representation_content_identities_frozen": True,
            "representation_content_availability_validation_required_at_deployment": (
                True
            ),
            "logical_object_aliases_resolved": True,
            "logical_planned_bytes_are_artifact_measurements": False,
            "synthetic_task_success_by_design_consumed": False,
            "semantic_outcome_values_included": False,
            "semantic_execution_performed": False,
            "semantic_quality_evaluated": False,
            "performance_measured": False,
            "cost_measured": False,
        },
        "output_sha256": output_sha256,
        "deployment_binding_included": False,
        "hidden_label_content_included": False,
        "external_services_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    plan["plan_sha256"] = _sha256(_canonical_bytes(plan))
    _assert_public_endpoint_free(
        [
            public_tasks,
            artifact_bindings,
            service_catalog,
            semantic_stages,
            semantic_trials,
            plan,
        ]
    )
    documents = {
        PUBLIC_TASKS_NAME: public_bytes,
        ARTIFACT_BINDINGS_NAME: artifact_bytes,
        SERVICE_CATALOG_NAME: service_bytes,
        STAGES_NAME: stage_bytes,
        TRIALS_NAME: trial_bytes,
        PLAN_NAME: _json_bytes(plan),
    }
    documents[CHECKSUMS_NAME] = b"".join(
        f"{_sha256(documents[name])}  {name}\n".encode("utf-8")
        for name in sorted(_CONTENT_FILES)
    )
    return documents


_STAGE_FIELDS = {
    "credentials_recorded",
    "deployment_binding_included",
    "hidden_label_included",
    "logical_stage",
    "object_representation_identity",
    "public_task_binding_sha256",
    "public_task_workload_id",
    "schema_version",
    "source_logical_stage_sha256",
    "stage_key",
    "trial_key",
}
_TRIAL_FIELDS = {
    "artifact_object_id",
    "credentials_recorded",
    "deployment_binding_included",
    "design_id",
    "hidden_label_included",
    "logical_trial",
    "logical_object_id",
    "order_index",
    "public_task_binding_sha256",
    "public_task_schema_version",
    "repetition",
    "representation_identities",
    "required_provisioning_chain_ids",
    "route_family",
    "schema_version",
    "semantic_stage_keys",
    "source_logical_trial_sha256",
    "trial_key",
    "workload_class",
    "workload_id",
}


def _verify_published(root: Path) -> dict[str, Any]:
    _require(root.is_dir(), "semantic matrix plan directory does not exist")
    entries = list(root.iterdir())
    _require(
        all(path.is_file() and not path.is_symlink() for path in entries),
        "semantic matrix plan must contain regular files only",
    )
    _require(
        {path.name for path in entries} == _ALL_FILES,
        "semantic matrix plan file set changed",
    )
    try:
        lines = (root / CHECKSUMS_NAME).read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FullFlowSemanticMatrixError("cannot read SHA256SUMS") from exc
    _require(
        len(lines) == len(_CONTENT_FILES),
        "semantic matrix checksums are incomplete",
    )
    checksums: dict[str, str] = {}
    for line in lines:
        digest, separator, name = line.partition("  ")
        _require(
            separator == "  "
            and name in _CONTENT_FILES
            and _SHA256.fullmatch(digest) is not None,
            "semantic matrix SHA256SUMS is malformed",
        )
        _require(name not in checksums, f"duplicate checksum entry: {name}")
        _require(
            _sha256((root / name).read_bytes()) == digest,
            f"semantic matrix checksum mismatch: {name}",
        )
        checksums[name] = digest
    _require(
        list(checksums) == sorted(_CONTENT_FILES),
        "semantic matrix checksums are not canonical",
    )

    _, plan = _json(root / PLAN_NAME, "semantic matrix plan")
    _require(
        plan.get("schema_version") == SEMANTIC_MATRIX_PLAN_SCHEMA_VERSION,
        "unsupported semantic matrix plan schema_version",
    )
    _require(
        plan.get("status") == "FROZEN_ENDPOINT_FREE_SEMANTIC_MATRIX",
        "semantic matrix plan is incomplete",
    )
    recorded_plan_sha256 = plan.pop("plan_sha256", None)
    _require(
        recorded_plan_sha256 == _sha256(_canonical_bytes(plan)),
        "semantic matrix plan_sha256 mismatch",
    )
    plan["plan_sha256"] = recorded_plan_sha256
    _require(
        plan.get("output_sha256")
        == {
            name: checksums[name]
            for name in sorted(_CONTENT_FILES - {PLAN_NAME})
        },
        "semantic matrix output digests disagree",
    )
    _require(
        plan.get("source_binding_sha256")
        == _sha256(_canonical_bytes(plan.get("source_bindings"))),
        "semantic matrix source binding digest mismatch",
    )
    dimensions = plan.get("matrix_dimensions")
    _require(
        isinstance(dimensions, dict)
        and dimensions.get("workload_classes") == sorted(_WORKLOAD_CLASSES)
        and dimensions.get("design_ids") == sorted(_DESIGN_IDS)
        and dimensions.get("repetitions") == 2
        and dimensions.get("matrix_cell_count") == 32
        and dimensions.get("trial_count") == 64,
        "semantic matrix dimensions changed",
    )
    boundary = plan.get("execution_boundary")
    _require(isinstance(boundary, dict), "execution boundary is missing")
    for field in (
        "synthetic_task_success_by_design_consumed",
        "semantic_outcome_values_included",
        "semantic_execution_performed",
        "semantic_quality_evaluated",
        "performance_measured",
        "cost_measured",
    ):
        _require(boundary.get(field) is False, f"unsafe claim changed: {field}")
    _require(
        boundary.get("supported_capabilities")
        == [
            "conditional-cache-branch-preservation",
            "endpoint-free-deployment-input",
            "logical-service-dag-preservation",
            "public-task-binding",
        ]
        and boundary.get("unsupported_capabilities")
        == [
            "hidden-label-scoring",
            "semantic-task-execution",
            "performance-measurement",
            "cost-measurement",
        ],
        "semantic capability boundary changed",
    )
    _require(
        boundary.get("deployment_binding_required") is True
        and boundary.get("flowmesh_rendering_required") is True
        and boundary.get("hidden_oracle_package_required_at_n1") is True
        and boundary.get("representation_content_identities_frozen") is True
        and boundary.get(
            "representation_content_availability_validation_required_at_deployment"
        )
        is True
        and boundary.get("logical_object_aliases_resolved") is True
        and boundary.get("logical_planned_bytes_are_artifact_measurements")
        is False,
        "semantic execution boundary weakened",
    )
    _require(
        plan.get("deployment_binding_included") is False
        and plan.get("hidden_label_content_included") is False
        and plan.get("external_services_called") is False
        and plan.get("credentials_recorded") is False
        and plan.get("eligible_for_scientific_claims") is False,
        "semantic matrix safety classification changed",
    )

    _, public_tasks = _json(root / PUBLIC_TASKS_NAME, "semantic public tasks")
    _, artifact_bindings = _json(
        root / ARTIFACT_BINDINGS_NAME,
        "semantic artifact bindings",
    )
    _, service_catalog = _json(
        root / SERVICE_CATALOG_NAME,
        "semantic service catalog",
    )
    _, stages = _jsonl(root / STAGES_NAME, "semantic matrix stages")
    _, trials = _jsonl(root / TRIALS_NAME, "semantic matrix trials")
    contracts = _validate_service_actions(
        {"service_contracts": service_catalog.get("service_contracts")},
        [row.get("logical_stage") for row in stages],
    )
    _require(
        service_catalog.get("schema_version")
        == SEMANTIC_SERVICE_CATALOG_SCHEMA_VERSION
        and service_catalog.get("deployment_binding_included") is False
        and service_catalog.get("credentials_recorded") is False,
        "semantic service catalog classification changed",
    )
    _require(
        len(contracts) == plan["coverage_summary"]["service_contract_count"],
        "semantic service contract count changed",
    )
    logical_trial_rows = [row["logical_trial"] for row in trials]
    logical_stage_rows = [row["logical_stage"] for row in stages]
    _, artifacts_by_logical_object = _normalise_artifact_bindings(
        root / ARTIFACT_BINDINGS_NAME,
        logical_stage_rows,
        logical_trial_rows,
    )
    _, tasks_by_workload = _normalise_public_tasks(
        root / PUBLIC_TASKS_NAME,
        logical_trial_rows,
        artifacts_by_logical_object,
    )
    source_bindings = plan.get("source_bindings")
    _require(isinstance(source_bindings, dict), "source bindings are missing")
    _require(
        source_bindings.get("public_task_set_sha256")
        == _sha256(_json_bytes(public_tasks)),
        "public task source binding changed",
    )
    _require(
        source_bindings.get("artifact_binding_set_sha256")
        == _sha256(_json_bytes(artifact_bindings)),
        "artifact binding source changed",
    )

    stage_by_key: dict[str, dict[str, Any]] = {}
    stages_by_trial: dict[str, list[str]] = defaultdict(list)
    scopes: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in stages:
        _strict_fields(row, _STAGE_FIELDS, "semantic matrix stage")
        _require(
            row["schema_version"] == SEMANTIC_MATRIX_STAGE_SCHEMA_VERSION,
            "unsupported semantic matrix stage schema_version",
        )
        logical = row["logical_stage"]
        _require(isinstance(logical, dict), "logical stage must be an object")
        key = logical.get("stage_key")
        _require(
            row["stage_key"] == key
            and key not in stage_by_key
            and row["trial_key"] == logical.get("trial_key")
            and row["source_logical_stage_sha256"]
            == _sha256(_canonical_bytes(logical)),
            f"semantic stage source binding changed: {key}",
        )
        task = tasks_by_workload.get(row["public_task_workload_id"])
        artifact = artifacts_by_logical_object.get(logical.get("object_id"))
        _require(artifact is not None, f"stage artifact binding is missing: {key}")
        representation_id = logical.get("representation_id")
        representation_by_id = {
            item["representation_id"]: item
            for item in artifact["representations"]
        }
        _require(
            task is not None
            and row["public_task_binding_sha256"]
            == task["task_binding_sha256"]
            and artifact["artifact_object_id"] == task["object_id"],
            f"semantic stage public task binding changed: {key}",
        )
        _require(
            row["object_representation_identity"]
            == {
                "logical_object_id": logical.get("object_id"),
                "artifact_object_id": artifact["artifact_object_id"],
                "representation_id": representation_id,
                "representation_binding": (
                    None
                    if representation_id is None
                    else representation_by_id[representation_id]
                ),
            },
            f"semantic stage representation identity changed: {key}",
        )
        _require(
            row["deployment_binding_included"] is False
            and row["credentials_recorded"] is False
            and row["hidden_label_included"] is False,
            f"semantic stage safety classification changed: {key}",
        )
        stage_by_key[key] = row
        scopes[logical["scope_id"]].append(logical)
        if logical.get("trial_key") is not None:
            stages_by_trial[logical["trial_key"]].append(key)
    for scope_id, rows in scopes.items():
        _require(
            [row["stage_index"] for row in rows] == list(range(len(rows))),
            f"semantic stage order changed: {scope_id}",
        )
        seen: set[str] = set()
        for row in rows:
            dependencies = row.get("dependency_stage_keys")
            _require(
                isinstance(dependencies, list) and set(dependencies) <= seen,
                f"semantic stage DAG is not topological: {row['stage_key']}",
            )
            condition = row.get("condition")
            if condition is not None:
                _require(
                    isinstance(condition, dict)
                    and condition.get("equals") in {"hit", "miss"}
                    and condition.get("cache_operation_key") in seen
                    and stage_by_key[condition["cache_operation_key"]][
                        "logical_stage"
                    ]["action"]
                    == "lookup",
                    f"semantic cache condition changed: {row['stage_key']}",
                )
            seen.add(row["stage_key"])

    _require(len(trials) == 64, "semantic trial count changed")
    trial_keys: set[str] = set()
    cells: set[tuple[str, str]] = set()
    route_counts: Counter[str] = Counter()
    for row in trials:
        _strict_fields(row, _TRIAL_FIELDS, "semantic matrix trial")
        _require(
            row["schema_version"] == SEMANTIC_MATRIX_TRIAL_SCHEMA_VERSION,
            "unsupported semantic matrix trial schema_version",
        )
        logical = row["logical_trial"]
        _require(isinstance(logical, dict), "logical trial must be an object")
        trial_key = logical.get("trial_key")
        _require(
            row["trial_key"] == trial_key
            and trial_key not in trial_keys
            and row["source_logical_trial_sha256"]
            == _sha256(_canonical_bytes(logical)),
            f"semantic trial source binding changed: {trial_key}",
        )
        task = tasks_by_workload.get(row["workload_id"])
        artifact = artifacts_by_logical_object.get(logical.get("object_id"))
        _require(
            task is not None
            and artifact is not None
            and row["public_task_binding_sha256"]
            == task["task_binding_sha256"]
            and row["logical_object_id"] == logical.get("object_id")
            and row["artifact_object_id"] == artifact["artifact_object_id"]
            and row["artifact_object_id"] == task["object_id"]
            and row["public_task_schema_version"]
            == N1_PUBLIC_TASK_SCHEMA_VERSION,
            f"semantic trial public task binding changed: {trial_key}",
        )
        copied_fields = (
            "order_index",
            "workload_id",
            "workload_class",
            "design_id",
            "repetition",
            "route_family",
            "required_provisioning_chain_ids",
        )
        _require(
            all(row[field] == logical.get(field) for field in copied_fields),
            f"semantic trial logical identity changed: {trial_key}",
        )
        expected_representations = [
            {
                "logical_object_id": logical["object_id"],
                "artifact_object_id": artifact["artifact_object_id"],
                "representation_id": representation_id,
                "representation_binding": next(
                    item
                    for item in artifact["representations"]
                    if item["representation_id"] == representation_id
                ),
            }
            for representation_id in logical["representation_ids"]
        ]
        _require(
            row["representation_identities"] == expected_representations,
            f"semantic trial representation binding changed: {trial_key}",
        )
        expected_stage_keys = (
            logical["execution_stage_keys"] + logical["evaluation_stage_keys"]
        )
        _require(
            row["semantic_stage_keys"] == expected_stage_keys
            and stages_by_trial[trial_key] == expected_stage_keys,
            f"semantic trial stage binding changed: {trial_key}",
        )
        _require(
            row["deployment_binding_included"] is False
            and row["credentials_recorded"] is False
            and row["hidden_label_included"] is False,
            f"semantic trial safety classification changed: {trial_key}",
        )
        trial_keys.add(trial_key)
        cells.add((row["workload_class"], row["design_id"]))
        route_counts[row["route_family"]] += 1
    _require(
        cells
        == {
            (workload_class, design_id)
            for workload_class in _WORKLOAD_CLASSES
            for design_id in _DESIGN_IDS
        },
        "semantic matrix 4x8 coverage changed",
    )
    _require(
        set(route_counts) == _ROUTE_FAMILIES
        and dict(sorted(route_counts.items()))
        == plan["coverage_summary"]["route_family_trial_counts"],
        "semantic route family coverage changed",
    )
    _require(
        len(stages) == plan["coverage_summary"]["semantic_stage_count"],
        "semantic stage count changed",
    )
    _assert_public_endpoint_free(
        [
            plan,
            public_tasks,
            artifact_bindings,
            service_catalog,
            stages,
            trials,
        ]
    )
    return plan


def _publish(target: Path, documents: Mapping[str, bytes]) -> None:
    _require(not target.exists(), f"semantic matrix output already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    parent = Path(tempfile.mkdtemp(prefix=".semantic-matrix-", dir=target.parent))
    stage = parent / "output"
    try:
        stage.mkdir()
        for name, content in documents.items():
            path = stage / name
            with path.open("wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
        _verify_published(stage)
        os.replace(stage, target)
    finally:
        shutil.rmtree(parent, ignore_errors=True)


def compile_full_flow_semantic_matrix(
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    public_task_set_path: str | Path,
    artifact_binding_path: str | Path,
    *,
    output_dir: str | Path,
    compiler_id: str = "full-flow-semantic-matrix-compiler-v1",
) -> dict[str, Any]:
    """Freeze all 64 endpoint-free semantic trial envelopes."""

    documents = _semantic_documents(
        Path(logical_route_dir).resolve(),
        Path(scenario_path).resolve(),
        Path(container_plan_dir).resolve(),
        Path(public_task_set_path).resolve(),
        Path(artifact_binding_path).resolve(),
        compiler_id,
    )
    target = Path(output_dir).resolve()
    _publish(target, documents)
    plan = _verify_published(target)
    return {
        "status": plan["status"],
        "scenario_id": plan["scenario_id"],
        "task_plane_id": plan["task_plane_id"],
        "artifact_binding_set_id": plan["artifact_binding_set_id"],
        "plan_sha256": plan["plan_sha256"],
        "matrix_cell_count": plan["matrix_dimensions"]["matrix_cell_count"],
        "trial_count": plan["matrix_dimensions"]["trial_count"],
        "semantic_stage_count": plan["coverage_summary"][
            "semantic_stage_count"
        ],
        "route_family_trial_counts": plan["coverage_summary"][
            "route_family_trial_counts"
        ],
        "output_dir": str(target),
        "semantic_execution_performed": False,
        "semantic_quality_evaluated": False,
        "performance_measured": False,
        "cost_measured": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def verify_full_flow_semantic_matrix(
    plan_dir: str | Path,
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    public_task_set_path: str | Path,
    artifact_binding_path: str | Path,
) -> dict[str, Any]:
    """Verify the package and reproduce it byte-for-byte from its inputs."""

    root = Path(plan_dir).resolve()
    plan = _verify_published(root)
    expected = _semantic_documents(
        Path(logical_route_dir).resolve(),
        Path(scenario_path).resolve(),
        Path(container_plan_dir).resolve(),
        Path(public_task_set_path).resolve(),
        Path(artifact_binding_path).resolve(),
        _identifier(plan.get("compiler_id"), "compiler_id"),
    )
    for name in sorted(_ALL_FILES):
        _require(
            (root / name).read_bytes() == expected[name],
            f"semantic matrix does not match deterministic source "
            f"recompilation: {name}",
        )
    return {
        "status": "VERIFIED",
        "scenario_id": plan["scenario_id"],
        "task_plane_id": plan["task_plane_id"],
        "artifact_binding_set_id": plan["artifact_binding_set_id"],
        "plan_sha256": plan["plan_sha256"],
        "source_binding_sha256": plan["source_binding_sha256"],
        "matrix_cell_count": 32,
        "trial_count": 64,
        "semantic_stage_count": plan["coverage_summary"][
            "semantic_stage_count"
        ],
        "deployment_binding_included": False,
        "hidden_label_content_included": False,
        "semantic_execution_performed": False,
        "semantic_quality_evaluated": False,
        "performance_measured": False,
        "cost_measured": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "ARTIFACT_BINDINGS_NAME",
    "ARTIFACT_BINDING_SET_SCHEMA_VERSION",
    "CHECKSUMS_NAME",
    "PLAN_NAME",
    "PUBLIC_TASKS_NAME",
    "SEMANTIC_MATRIX_PLAN_SCHEMA_VERSION",
    "SERVICE_CATALOG_NAME",
    "STAGES_NAME",
    "TRIALS_NAME",
    "FullFlowSemanticMatrixError",
    "compile_full_flow_semantic_matrix",
    "verify_full_flow_semantic_matrix",
]
