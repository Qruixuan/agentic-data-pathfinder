"""Endpoint-free logical routes for the eight-node full-flow simulator.

This module turns the measured container-operation ledger into a portable
service graph.  It deliberately separates two things which are easy to blur:

* a logical service contract (what N1--N8 must do), and
* a deployment binding (where and how that contract is reached).

The compiler records only the former.  URLs, credentials, host paths,
container names, storage mounts, and link-shaping values stay in a later
deployment binding.  No service is contacted while compiling or verifying a
plan.
"""

from __future__ import annotations

import json
import math
import os
import re
import shutil
import tempfile
from collections import Counter, defaultdict
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable, Mapping

from .config import SimulatorScenario, load_simulator_scenario
from .container_contract import (
    CONTAINER_OPERATION_SCHEMA_VERSION,
    ContainerContractError,
    verify_container_backend_plan,
)
from .engine import SimulatorTrial, build_simulator_trials


LOGICAL_ROUTE_PLAN_SCHEMA_VERSION = (
    "pathfinder.full-flow-logical-route-plan/v1alpha1"
)
LOGICAL_SERVICE_CATALOG_SCHEMA_VERSION = (
    "pathfinder.full-flow-logical-service-catalog/v1alpha1"
)
LOGICAL_SERVICE_CONTRACT_SCHEMA_VERSION = (
    "pathfinder.full-flow-logical-service-contract/v1alpha1"
)
LOGICAL_STAGE_SCHEMA_VERSION = "pathfinder.full-flow-logical-stage/v1alpha1"
LOGICAL_TRIAL_SCHEMA_VERSION = "pathfinder.full-flow-logical-trial/v1alpha1"
LOGICAL_COVERAGE_SCHEMA_VERSION = (
    "pathfinder.full-flow-logical-route-coverage/v1alpha1"
)

SERVICE_CATALOG_NAME = "logical-service-contracts.json"
STAGES_NAME = "logical-route-stages.jsonl"
TRIALS_NAME = "logical-route-trials.jsonl"
COVERAGE_NAME = "logical-route-coverage.jsonl"
PLAN_NAME = "logical-route-plan.json"
CHECKSUMS_NAME = "SHA256SUMS"

_CONTENT_FILES = {
    SERVICE_CATALOG_NAME,
    STAGES_NAME,
    TRIALS_NAME,
    COVERAGE_NAME,
    PLAN_NAME,
}
_ALL_FILES = _CONTENT_FILES | {CHECKSUMS_NAME}
_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
_HEX_SHA256 = re.compile(r"[0-9a-f]{64}")
_WINDOWS_ABSOLUTE = re.compile(r"^[A-Za-z]:[\\/]")
_URL_PREFIXES = ("http://", "https://", "file://", "ssh://")
_EXPECTED_NODES = {f"N{index}" for index in range(1, 9)}
_EXPECTED_WORKLOAD_CLASSES = {f"W{index}" for index in range(1, 5)}
_EXPECTED_DESIGNS = {f"D{index}" for index in range(8)}
_DERIVED_REPRESENTATIONS = {
    "multimodal_digest",
    "sampled_frame_bundle",
}


class FullFlowLogicalRouteError(ValueError):
    """Raised when a logical route cannot be compiled or verified safely."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise FullFlowLogicalRouteError(message)


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _invalid_number(value: str) -> None:
    raise FullFlowLogicalRouteError(f"non-finite JSON number: {value}")


def _read_json(path: Path, label: str) -> tuple[bytes, Any]:
    try:
        raw = path.read_bytes()
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_keys,
            parse_constant=_invalid_number,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowLogicalRouteError(
            f"cannot read valid {label}: {path.name}"
        ) from exc
    return raw, value


def _read_jsonl(path: Path, label: str) -> tuple[bytes, list[dict[str, Any]]]:
    try:
        raw = path.read_bytes()
        text = raw.decode("utf-8")
    except (OSError, UnicodeError) as exc:
        raise FullFlowLogicalRouteError(
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
        except (json.JSONDecodeError, FullFlowLogicalRouteError) as exc:
            raise FullFlowLogicalRouteError(
                f"invalid {label} at line {line_number}: {exc}"
            ) from exc
        _require(isinstance(value, dict), f"{label} row must be an object")
        rows.append(value)
    return raw, rows


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


def _jsonl_bytes(rows: Iterable[Mapping[str, Any]]) -> bytes:
    return b"".join(
        (
            json.dumps(
                row,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
        for row in rows
    )


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return sha256(value).hexdigest()


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    _require(isinstance(value, Mapping), f"{label} must be an object")
    return value


def _string(value: Any, label: str) -> str:
    _require(
        isinstance(value, str) and value == value.strip() and bool(value),
        f"{label} must be a non-empty trimmed string",
    )
    return value


def _identifier(value: Any, label: str) -> str:
    result = _string(value, label)
    _require(bool(_SAFE_ID.fullmatch(result)), f"{label} is not portable")
    return result


def _integer(value: Any, label: str) -> int:
    _require(
        type(value) is int and value >= 0,
        f"{label} must be a non-negative integer",
    )
    return value


def _strict_keys(
    value: Mapping[str, Any],
    expected: set[str],
    label: str,
) -> None:
    actual = set(value)
    _require(
        actual == expected,
        f"{label} fields changed: expected {sorted(expected)}, got "
        f"{sorted(actual)}",
    )


def _assert_safe_document(value: Any, label: str = "document") -> None:
    """Reject runtime bindings, secrets, URLs, and absolute host paths."""

    if isinstance(value, Mapping):
        forbidden_keys = {
            "api_key",
            "authorization",
            "bearer_token",
            "credential",
            "endpoint",
            "host_path",
            "mount_path",
            "password",
            "secret",
            "token",
            "url",
        }
        for key, child in value.items():
            _require(
                key.lower() not in forbidden_keys,
                f"{label} contains forbidden runtime binding field: {key}",
            )
            _assert_safe_document(child, f"{label}.{key}")
        return
    if isinstance(value, list):
        for index, child in enumerate(value):
            _assert_safe_document(child, f"{label}[{index}]")
        return
    if isinstance(value, str):
        lowered = value.lower()
        _require(
            not lowered.startswith(_URL_PREFIXES),
            f"{label} contains a runtime address",
        )
        _require(
            not value.startswith(("/", "\\\\"))
            and not _WINDOWS_ABSOLUTE.match(value),
            f"{label} contains an absolute host path",
        )
    if isinstance(value, float):
        _require(math.isfinite(value), f"{label} contains a non-finite number")


def _service_contract(
    contract_id: str,
    *,
    role: str,
    nodes: list[str],
    actions: list[str],
    protocols: list[str],
    state_semantics: str,
    scope: str,
    binding_kind: str,
) -> dict[str, Any]:
    return {
        "schema_version": LOGICAL_SERVICE_CONTRACT_SCHEMA_VERSION,
        "service_contract_id": contract_id,
        "role": role,
        "logical_node_ids": nodes,
        "actions": actions,
        "protocol_contracts": protocols,
        "state_semantics": state_semantics,
        "invocation_scope": scope,
        "deployment_binding_kind": binding_kind,
        "deployment_binding_included": False,
        "secret_material_included": False,
    }


def _base_service_contracts(scenario: SimulatorScenario) -> dict[str, dict[str, Any]]:
    contracts = [
        _service_contract(
            "N1.hidden-score",
            role="hidden-oracle-scoring",
            nodes=["N1"],
            actions=["score-hidden-answer"],
            protocols=[
                "pathfinder.n1-oracle-service/v1alpha2",
                "pathfinder.n1-score-request/v1alpha2",
                "pathfinder.n1-score-result/v1alpha2",
            ],
            state_semantics="immutable-hidden-oracle",
            scope="one-answer-per-trial",
            binding_kind="score-service",
        ),
        _service_contract(
            "N1.trial-control",
            role="trial-admission-and-control",
            nodes=["N1"],
            actions=["admit-trial"],
            protocols=["pathfinder.logical-trial-control/v1alpha1"],
            state_semantics="durable-trial-identity",
            scope="one-control-record-per-trial",
            binding_kind="control-service",
        ),
        _service_contract(
            "N2.global-index",
            role="global-object-and-representation-index",
            nodes=["N2"],
            actions=["query-index"],
            protocols=[
                "pathfinder.n2-index-service/v1alpha1",
                "pathfinder.n2-index-query-request/v1alpha1",
                "pathfinder.n2-index-query-result/v1alpha1",
            ],
            state_semantics="frozen-index-snapshot",
            scope="shared-by-matrix",
            binding_kind="index-service",
        ),
        _service_contract(
            "N3.raw-data-agent",
            role="raw-cold-object-access",
            nodes=["N3"],
            actions=["access-raw-artifact"],
            protocols=["pathfinder.data-agent/v1alpha1"],
            state_semantics="immutable-content-addressed-artifacts",
            scope="shared-by-matrix",
            binding_kind="data-agent-service-and-object-store",
        ),
        _service_contract(
            "N4.derived-data-agent",
            role="derived-warm-object-access",
            nodes=["N4"],
            actions=["access-derived-artifact", "publish-derived-artifact"],
            protocols=["pathfinder.data-agent/v1alpha1"],
            state_semantics="immutable-content-addressed-artifacts",
            scope="shared-by-matrix",
            binding_kind="data-agent-service-and-object-store",
        ),
        _service_contract(
            "N5.materializer",
            role="representation-materialization",
            nodes=["N5"],
            actions=["materialize-representation"],
            protocols=[
                "pathfinder.simulator-n5-materialization-http/v1alpha1",
                "pathfinder.simulator-n5-materialization-http-execute/"
                "v1alpha1",
                "pathfinder.simulator-n5-materialization-http-result/"
                "v1alpha1",
            ],
            state_semantics="idempotent-content-addressed-output",
            scope="one-artifact-provisioning-chain",
            binding_kind="materialization-service",
        ),
        _service_contract(
            "N6.semantic-inference",
            role="semantic-model-inference",
            nodes=["N6"],
            actions=["infer"],
            protocols=[
                "pathfinder.container-node-semantic-request/v1alpha2",
                "pathfinder.container-node-semantic-result/v1alpha2",
            ],
            state_semantics="request-scoped-model-execution",
            scope="one-inference-per-trial",
            binding_kind="inference-service",
        ),
    ]
    for node_id in ("N7", "N8"):
        contracts.extend([
            _service_contract(
                f"{node_id}.execution-compute",
                role="execution-side-input-preparation",
                nodes=[node_id],
                actions=["prepare-model-input"],
                protocols=["pathfinder.logical-task-operation/v1alpha1"],
                state_semantics="trial-scoped",
                scope="one-or-more-steps-per-trial",
                binding_kind="execution-service",
            ),
            _service_contract(
                f"{node_id}.local-index",
                role="execution-side-local-index",
                nodes=[node_id],
                actions=["query-index"],
                protocols=[
                    "pathfinder.n2-index-service/v1alpha1",
                    "pathfinder.n2-index-query-request/v1alpha1",
                    "pathfinder.n2-index-query-result/v1alpha1",
                ],
                state_semantics="frozen-index-snapshot",
                scope="shared-by-cache-scope",
                binding_kind="index-service",
            ),
            _service_contract(
                f"{node_id}.persistent-cache",
                role="execution-side-derived-artifact-cache",
                nodes=[node_id],
                actions=["lookup", "read", "insert"],
                protocols=["pathfinder.full-flow-artifact-cache/v1alpha1"],
                state_semantics="persistent-with-explicit-cache-scope",
                scope="design-and-repetition-cache-scope",
                binding_kind="cache-service-and-persistent-volume",
            ),
            _service_contract(
                f"{node_id}.branch-join",
                role="conditional-cache-branch-join",
                nodes=[node_id],
                actions=["join-hit-or-miss-branch"],
                protocols=["pathfinder.logical-branch-join/v1alpha1"],
                state_semantics="trial-scoped",
                scope="one-join-per-conditional-representation",
                binding_kind="execution-service",
            ),
        ])
    for link_id in sorted(scenario.links):
        link = scenario.links[link_id]
        contracts.append(_service_contract(
            f"transport.{link_id}",
            role="logical-byte-transfer",
            nodes=[link.source_node_id, link.destination_node_id],
            actions=["transfer-bytes"],
            protocols=["pathfinder.logical-byte-transfer/v1alpha1"],
            state_semantics="stateless",
            scope="one-transfer-per-stage",
            binding_kind="network-link",
        ))
    for source, destination, purpose in (
        ("N3", "N5", "raw-materialization-input"),
        ("N5", "N4", "derived-publication"),
        ("N6", "N1", "answer-scoring-input"),
    ):
        contracts.append(_service_contract(
            f"transport.{source}-{destination}-{purpose}",
            role="logical-byte-transfer",
            nodes=[source, destination],
            actions=["transfer-bytes"],
            protocols=["pathfinder.logical-byte-transfer/v1alpha1"],
            state_semantics="stateless",
            scope="one-transfer-per-stage",
            binding_kind="network-link-not-present-in-source-matrix",
        ))
    result = {row["service_contract_id"]: row for row in contracts}
    _require(len(result) == len(contracts), "duplicate service contract ID")
    return result


_SOURCE_OPERATION_KEYS = {
    "backend_id",
    "cache_adapter",
    "cache_scope_id",
    "condition",
    "dependency_operation_keys",
    "destination_container",
    "destination_node_id",
    "execution_container",
    "execution_node_id",
    "link_adapter",
    "logical_bytes",
    "measure_actual_duration",
    "object_id",
    "operation_adapter",
    "operation_id",
    "operation_key",
    "operation_kind",
    "portable_plan_sha256",
    "representation_id",
    "resource_adapter",
    "schema_version",
    "simulation_hint_used_as_measured_duration",
    "task_executor",
    "trial_key",
}


def _source_identifier(
    adapter: Any,
    key: str,
    label: str,
) -> str | None:
    if adapter is None:
        return None
    mapped = _mapping(adapter, label)
    return _string(mapped.get(key), f"{label}.{key}")


def _load_source(
    scenario: SimulatorScenario,
    container_root: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, str]]:
    try:
        verified = verify_container_backend_plan(container_root)
    except (
        ContainerContractError,
        OSError,
        UnicodeError,
        json.JSONDecodeError,
    ) as exc:
        raise FullFlowLogicalRouteError(
            "container backend plan did not pass source verification"
        ) from exc
    _require(
        verified.get("scenario_id") == scenario.scenario_id,
        "container plan and scenario IDs differ",
    )
    manifest_raw, manifest_value = _read_json(
        container_root / "container_plan_manifest.json",
        "container plan manifest",
    )
    manifest = dict(_mapping(manifest_value, "container plan manifest"))
    operations_raw, operations = _read_jsonl(
        container_root / "container_operations.jsonl",
        "container operation ledger",
    )
    topology_raw, topology_value = _read_json(
        container_root / "container_topology.json",
        "container topology",
    )
    _mapping(topology_value, "container topology")
    _require(
        manifest.get("scenario_sha256") == scenario.source_sha256,
        "container plan is bound to different scenario content",
    )
    _require(
        manifest.get("planned_operation_count") == len(operations),
        "container manifest operation count mismatch",
    )
    bindings = {
        "scenario_sha256": scenario.source_sha256,
        "portable_plan_sha256": _string(
            manifest.get("portable_plan_sha256"),
            "container manifest portable_plan_sha256",
        ),
        "container_manifest_sha256": _sha256_bytes(manifest_raw),
        "container_operations_sha256": _sha256_bytes(operations_raw),
        "container_topology_sha256": _sha256_bytes(topology_raw),
    }
    _require(
        all(_HEX_SHA256.fullmatch(value) for value in bindings.values()),
        "source binding contains an invalid SHA-256 digest",
    )
    return manifest, operations, bindings


def _validate_source_operations(
    scenario: SimulatorScenario,
    trials: tuple[SimulatorTrial, ...],
    manifest: Mapping[str, Any],
    operations: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    trial_by_key = {trial.trial_key: trial for trial in trials}
    workload_by_id = {
        workload.workload_id: workload for workload in scenario.workloads
    }
    design_by_id = {design.design_id: design for design in scenario.designs}
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    operation_keys: set[str] = set()
    backend_id = _string(manifest.get("backend_id"), "container backend_id")
    portable_sha = _string(
        manifest.get("portable_plan_sha256"),
        "container portable_plan_sha256",
    )
    for index, row in enumerate(operations):
        label = f"container operations[{index}]"
        _strict_keys(row, _SOURCE_OPERATION_KEYS, label)
        _require(
            row["schema_version"] == CONTAINER_OPERATION_SCHEMA_VERSION,
            "logical routes require cache-scoped v1alpha2 operations",
        )
        _require(row["backend_id"] == backend_id, f"{label} backend changed")
        _require(
            row["portable_plan_sha256"] == portable_sha,
            f"{label} portable plan binding changed",
        )
        trial_key = _string(row["trial_key"], f"{label}.trial_key")
        _require(trial_key in trial_by_key, f"{label} has unknown trial_key")
        operation_key = _string(
            row["operation_key"], f"{label}.operation_key"
        )
        operation_id = _string(row["operation_id"], f"{label}.operation_id")
        _require(
            operation_key == f"{trial_key}|{operation_id}",
            f"{label} operation identity is inconsistent",
        )
        _require(
            operation_key not in operation_keys,
            f"duplicate operation_key: {operation_key}",
        )
        operation_keys.add(operation_key)
        trial = trial_by_key[trial_key]
        _require(
            row["object_id"] == trial.object_id,
            f"{label} object differs from its trial",
        )
        _integer(row["logical_bytes"], f"{label}.logical_bytes")
        for key in ("execution_node_id", "destination_node_id"):
            node_id = _string(row[key], f"{label}.{key}")
            _require(node_id in scenario.nodes, f"{label} names unknown {key}")
        _require(
            row["measure_actual_duration"] is True
            and row["simulation_hint_used_as_measured_duration"] is False,
            f"{label} is not a measured container operation",
        )
        dependencies = row["dependency_operation_keys"]
        _require(isinstance(dependencies, list), f"{label} dependencies invalid")
        _require(
            len(dependencies) == len(set(dependencies)),
            f"{label} has duplicate dependencies",
        )
        for dependency in dependencies:
            _require(
                isinstance(dependency, str)
                and dependency.startswith(f"{trial_key}|"),
                f"{label} has a cross-trial dependency",
            )
        condition = row["condition"]
        if condition is not None:
            condition = _mapping(condition, f"{label}.condition")
            _strict_keys(
                condition,
                {"cache_operation_id", "cache_operation_key", "equals"},
                f"{label}.condition",
            )
            _require(
                condition["equals"] in {"hit", "miss"},
                f"{label} condition is neither hit nor miss",
            )
            _require(
                condition["cache_operation_key"]
                == f"{trial_key}|{condition['cache_operation_id']}",
                f"{label} condition identity is inconsistent",
            )
        representation_id = row["representation_id"]
        if representation_id is not None:
            _require(
                representation_id
                in scenario.objects[trial.object_id].representations,
                f"{label} names an unavailable representation",
            )
        grouped[trial_key].append(row)

    _require(
        set(grouped) == set(trial_by_key),
        "container operations do not cover every planned trial exactly",
    )
    for trial in trials:
        design = design_by_id[trial.design_id]
        expected = scenario.operations_for(design, trial.workload_class)
        rows = grouped[trial.trial_key]
        _require(
            [row["operation_id"] for row in rows]
            == [operation.op_id for operation in expected],
            f"operation order changed for trial {trial.trial_key}",
        )
        for row, operation in zip(rows, expected):
            _require(
                row["operation_kind"] == operation.kind,
                f"operation kind changed for {row['operation_key']}",
            )
            _require(
                row["representation_id"] == operation.representation_id,
                f"representation changed for {row['operation_key']}",
            )
            _require(
                row["dependency_operation_keys"]
                == [
                    f"{trial.trial_key}|{dependency}"
                    for dependency in operation.depends_on
                ],
                f"dependency order changed for {row['operation_key']}",
            )
        _require(
            workload_by_id[trial.workload_id].object_id == trial.object_id,
            f"workload object binding changed for {trial.trial_key}",
        )
    _require(
        len(operation_keys) == manifest.get("planned_operation_count"),
        "container operation identity count mismatch",
    )
    return grouped


def _route_family(route_template: str) -> str:
    if route_template in {"raw-full", "raw-scan"}:
        return "raw"
    if route_template == "raw-indexed":
        return "indexed-raw"
    if route_template.startswith("remote-"):
        return "remote-derived"
    if route_template.startswith("local-"):
        return "local-cache-derived"
    raise FullFlowLogicalRouteError(
        f"unsupported full-flow route template: {route_template}"
    )


def _service_for_operation(row: Mapping[str, Any]) -> tuple[str, str, list[str]]:
    kind = row["operation_kind"]
    node = row["execution_node_id"]
    destination = row["destination_node_id"]
    if kind == "control" and node == "N1":
        return "N1.trial-control", "admit-trial", ["N1"]
    if kind == "index_query" and node == "N2":
        return "N2.global-index", "query-index", ["N2"]
    if kind == "index_query" and node in {"N7", "N8"}:
        return f"{node}.local-index", "query-index", [node]
    if kind == "storage_read" and node == "N3":
        _require(
            row["representation_id"] == "raw_video",
            "N3 may expose only raw_video through the raw Data Agent",
        )
        return "N3.raw-data-agent", "access-raw-artifact", ["N3"]
    if kind == "storage_read" and node == "N4":
        _require(
            row["representation_id"] in _DERIVED_REPRESENTATIONS,
            "N4 may expose only frozen derived representations",
        )
        return "N4.derived-data-agent", "access-derived-artifact", ["N4"]
    if kind == "network_transfer":
        link_id = _source_identifier(
            row["link_adapter"], "link_id", "network link adapter"
        )
        _require(link_id is not None, "network transfer has no logical link")
        return (
            f"transport.{link_id}",
            "transfer-bytes",
            [node, destination],
        )
    if kind in {"cache_lookup", "cache_read", "cache_insert"}:
        _require(node in {"N7", "N8"}, "cache stage is not on N7 or N8")
        action = {
            "cache_lookup": "lookup",
            "cache_read": "read",
            "cache_insert": "insert",
        }[kind]
        return f"{node}.persistent-cache", action, [node]
    if kind == "barrier":
        _require(node in {"N7", "N8"}, "branch join is not on N7 or N8")
        return f"{node}.branch-join", "join-hit-or-miss-branch", [node]
    if kind == "compute" and node == "N6":
        _require(row["operation_id"] == "infer", "N6 compute is not inference")
        return "N6.semantic-inference", "infer", ["N6"]
    if kind == "compute" and node in {"N7", "N8"}:
        return f"{node}.execution-compute", "prepare-model-input", [node]
    raise FullFlowLogicalRouteError(
        f"unsupported logical service mapping: {kind} on {node}"
    )


def _stage(
    *,
    stage_key: str,
    scope_kind: str,
    scope_id: str,
    trial_key: str | None,
    stage_index: int,
    phase: str,
    service_contract_id: str,
    nodes: list[str],
    action: str,
    object_id: str,
    representation_id: str | None,
    logical_bytes: int | None,
    dependencies: list[str],
    condition: Mapping[str, Any] | None,
    source_operation_key: str | None,
    source_operation_id: str | None,
    source_operation_kind: str | None,
    source_resource_id: str | None,
    source_link_id: str | None,
    source_cache_id: str | None,
    counted_in_source_matrix: bool,
) -> dict[str, Any]:
    return {
        "schema_version": LOGICAL_STAGE_SCHEMA_VERSION,
        "stage_key": stage_key,
        "scope_kind": scope_kind,
        "scope_id": scope_id,
        "trial_key": trial_key,
        "stage_index": stage_index,
        "phase": phase,
        "service_contract_id": service_contract_id,
        "logical_node_ids": nodes,
        "action": action,
        "object_id": object_id,
        "representation_id": representation_id,
        "planned_logical_bytes": logical_bytes,
        "dependency_stage_keys": dependencies,
        "condition": None if condition is None else dict(condition),
        "source_operation_key": source_operation_key,
        "source_operation_id": source_operation_id,
        "source_operation_kind": source_operation_kind,
        "source_resource_id": source_resource_id,
        "source_link_id": source_link_id,
        "source_cache_id": source_cache_id,
        "counted_in_source_matrix": counted_in_source_matrix,
        "deployment_binding_included": False,
        "secret_material_included": False,
    }


def _node_order(node_id: str) -> tuple[int, str]:
    if re.fullmatch(r"N[0-9]+", node_id):
        return int(node_id[1:]), node_id
    return 10_000, node_id


def _validate_scenario_shape(scenario: SimulatorScenario) -> None:
    _require(
        set(scenario.nodes) == _EXPECTED_NODES,
        "full-flow logical compiler requires logical nodes N1 through N8",
    )
    classes = {workload.workload_class for workload in scenario.workloads}
    designs = {design.design_id for design in scenario.designs}
    _require(
        bool(classes) and classes <= _EXPECTED_WORKLOAD_CLASSES,
        "full-flow logical compiler requires a non-empty subset of workload "
        "classes W1 through W4",
    )
    _require(
        designs == _EXPECTED_DESIGNS,
        "full-flow logical compiler requires designs D0 through D7",
    )
    _require(scenario.repetitions >= 1, "scenario repetitions must be positive")
    for value, label in (
        (scenario.scenario_id, "scenario_id"),
        *(
            (workload.workload_id, "workload_id")
            for workload in scenario.workloads
        ),
        *((design.design_id, "design_id") for design in scenario.designs),
        *((object_id, "object_id") for object_id in scenario.objects),
    ):
        _identifier(value, label)
    for design in scenario.designs:
        _require(
            set(design.route_templates) == classes,
            f"{design.design_id} does not bind every workload class",
        )
        _require(
            design.executor_node_id in {"N7", "N8"},
            f"{design.design_id} executor must be N7 or N8",
        )
        for route_template in design.route_templates.values():
            _route_family(route_template)
    for object_id, data_object in scenario.objects.items():
        _require(
            "raw_video" in data_object.representations,
            f"{object_id} has no raw_video source",
        )
        for representation_id in data_object.representations:
            _identifier(representation_id, "representation_id")


def _provisioning_stages(
    scenario: SimulatorScenario,
    artifacts: set[tuple[str, str]],
) -> tuple[list[dict[str, Any]], dict[tuple[str, str], str]]:
    stages: list[dict[str, Any]] = []
    chain_ids: dict[tuple[str, str], str] = {}
    for object_id, representation_id in sorted(artifacts):
        _require(
            representation_id in _DERIVED_REPRESENTATIONS,
            f"cannot provision unsupported representation {representation_id}",
        )
        data_object = scenario.objects[object_id]
        raw_size = data_object.representations["raw_video"].size_bytes
        derived_size = data_object.representations[
            representation_id
        ].size_bytes
        chain_id = f"artifact|{object_id}|{representation_id}"
        chain_ids[(object_id, representation_id)] = chain_id
        specs = [
            (
                "access-raw",
                "N3.raw-data-agent",
                ["N3"],
                "access-raw-artifact",
                "raw_video",
                raw_size,
                [],
            ),
            (
                "handoff-raw",
                "transport.N3-N5-raw-materialization-input",
                ["N3", "N5"],
                "transfer-bytes",
                "raw_video",
                raw_size,
                ["access-raw"],
            ),
            (
                "materialize",
                "N5.materializer",
                ["N5"],
                "materialize-representation",
                representation_id,
                derived_size,
                ["handoff-raw"],
            ),
            (
                "handoff-derived",
                "transport.N5-N4-derived-publication",
                ["N5", "N4"],
                "transfer-bytes",
                representation_id,
                derived_size,
                ["materialize"],
            ),
            (
                "publish-derived",
                "N4.derived-data-agent",
                ["N4"],
                "publish-derived-artifact",
                representation_id,
                derived_size,
                ["handoff-derived"],
            ),
        ]
        keys = {name: f"provision|{object_id}|{representation_id}|{name}" for (
            name,
            *_rest,
        ) in specs}
        for stage_index, spec in enumerate(specs):
            (
                name,
                contract_id,
                nodes,
                action,
                stage_representation,
                logical_bytes,
                dependency_names,
            ) = spec
            stages.append(_stage(
                stage_key=keys[name],
                scope_kind="artifact-provisioning",
                scope_id=chain_id,
                trial_key=None,
                stage_index=stage_index,
                phase="provisioning",
                service_contract_id=contract_id,
                nodes=nodes,
                action=action,
                object_id=object_id,
                representation_id=stage_representation,
                logical_bytes=logical_bytes,
                dependencies=[keys[value] for value in dependency_names],
                condition=None,
                source_operation_key=None,
                source_operation_id=None,
                source_operation_kind=None,
                source_resource_id=None,
                source_link_id=None,
                source_cache_id=None,
                counted_in_source_matrix=False,
            ))
    return stages, chain_ids


def _source_stage(
    row: Mapping[str, Any],
    stage_index: int,
) -> dict[str, Any]:
    contract_id, action, nodes = _service_for_operation(row)
    return _stage(
        stage_key=row["operation_key"],
        scope_kind="trial",
        scope_id=row["trial_key"],
        trial_key=row["trial_key"],
        stage_index=stage_index,
        phase="execution",
        service_contract_id=contract_id,
        nodes=nodes,
        action=action,
        object_id=row["object_id"],
        representation_id=row["representation_id"],
        logical_bytes=row["logical_bytes"],
        dependencies=list(row["dependency_operation_keys"]),
        condition=row["condition"],
        source_operation_key=row["operation_key"],
        source_operation_id=row["operation_id"],
        source_operation_kind=row["operation_kind"],
        source_resource_id=_source_identifier(
            row["resource_adapter"], "resource_id", "resource adapter"
        ),
        source_link_id=_source_identifier(
            row["link_adapter"], "link_id", "link adapter"
        ),
        source_cache_id=_source_identifier(
            row["cache_adapter"], "cache_id", "cache adapter"
        ),
        counted_in_source_matrix=True,
    )


def _compile_trials(
    scenario: SimulatorScenario,
    trials: tuple[SimulatorTrial, ...],
    grouped: Mapping[str, list[dict[str, Any]]],
    contracts: Mapping[str, dict[str, Any]],
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    design_by_id = {design.design_id: design for design in scenario.designs}
    derived_artifacts: set[tuple[str, str]] = set()
    trial_metadata: list[tuple[SimulatorTrial, str, str, list[str]]] = []
    for trial in trials:
        route_template = design_by_id[trial.design_id].route_templates[
            trial.workload_class
        ]
        family = _route_family(route_template)
        representations: list[str] = []
        for row in grouped[trial.trial_key]:
            representation_id = row["representation_id"]
            if (
                representation_id is not None
                and representation_id not in representations
            ):
                representations.append(representation_id)
            if (
                family in {"remote-derived", "local-cache-derived"}
                and representation_id in _DERIVED_REPRESENTATIONS
            ):
                derived_artifacts.add((trial.object_id, representation_id))
        _require(bool(representations), f"trial has no data: {trial.trial_key}")
        if family in {"raw", "indexed-raw"}:
            _require(
                set(representations) == {"raw_video"},
                f"raw route exposes derived data: {trial.trial_key}",
            )
        else:
            _require(
                bool(set(representations) & _DERIVED_REPRESENTATIONS)
                and "raw_video" not in representations,
                f"derived route has invalid representations: {trial.trial_key}",
            )
        trial_metadata.append((trial, route_template, family, representations))

    provisioning, chain_ids = _provisioning_stages(
        scenario,
        derived_artifacts,
    )
    stages = list(provisioning)
    trial_rows: list[dict[str, Any]] = []
    for trial, route_template, family, representations in trial_metadata:
        source_rows = grouped[trial.trial_key]
        trial_stages = [
            _source_stage(row, index)
            for index, row in enumerate(source_rows)
        ]
        infer = [
            row for row in trial_stages
            if row["service_contract_id"] == "N6.semantic-inference"
        ]
        _require(
            len(infer) == 1,
            f"trial must have exactly one inference: {trial.trial_key}",
        )
        result_key = f"{trial.trial_key}|return-answer"
        score_key = f"{trial.trial_key}|hidden-score"
        result_stage = _stage(
            stage_key=result_key,
            scope_kind="trial",
            scope_id=trial.trial_key,
            trial_key=trial.trial_key,
            stage_index=len(trial_stages),
            phase="evaluation",
            service_contract_id="transport.N6-N1-answer-scoring-input",
            nodes=["N6", "N1"],
            action="transfer-bytes",
            object_id=trial.object_id,
            representation_id=None,
            logical_bytes=None,
            dependencies=[infer[0]["stage_key"]],
            condition=None,
            source_operation_key=None,
            source_operation_id=None,
            source_operation_kind=None,
            source_resource_id=None,
            source_link_id=None,
            source_cache_id=None,
            counted_in_source_matrix=False,
        )
        score_stage = _stage(
            stage_key=score_key,
            scope_kind="trial",
            scope_id=trial.trial_key,
            trial_key=trial.trial_key,
            stage_index=len(trial_stages) + 1,
            phase="evaluation",
            service_contract_id="N1.hidden-score",
            nodes=["N1"],
            action="score-hidden-answer",
            object_id=trial.object_id,
            representation_id=None,
            logical_bytes=None,
            dependencies=[result_key],
            condition=None,
            source_operation_key=None,
            source_operation_id=None,
            source_operation_kind=None,
            source_resource_id=None,
            source_link_id=None,
            source_cache_id=None,
            counted_in_source_matrix=False,
        )
        trial_stages.extend([result_stage, score_stage])
        stages.extend(trial_stages)

        required_pairs = sorted(
            (trial.object_id, representation_id)
            for representation_id in representations
            if representation_id in _DERIVED_REPRESENTATIONS
        )
        required_chain_ids = [chain_ids[pair] for pair in required_pairs]
        trial_contract_ids = {
            row["service_contract_id"] for row in trial_stages
        }
        required_contract_ids = set(trial_contract_ids)
        for pair in required_pairs:
            chain_id = chain_ids[pair]
            required_contract_ids.update(
                row["service_contract_id"]
                for row in provisioning
                if row["scope_id"] == chain_id
            )
        missing_contracts = required_contract_ids - set(contracts)
        _require(
            not missing_contracts,
            f"trial requires unknown contracts: {sorted(missing_contracts)}",
        )
        required_nodes = {
            node_id
            for contract_id in required_contract_ids
            for node_id in contracts[contract_id]["logical_node_ids"]
        }
        trial_nodes = {
            node_id
            for contract_id in trial_contract_ids
            for node_id in contracts[contract_id]["logical_node_ids"]
        }
        index_nodes = {
            row["logical_node_ids"][0]
            for row in trial_stages
            if row["action"] == "query-index"
        }
        if "N2" in index_nodes:
            index_mode = "global"
        elif index_nodes:
            _require(
                index_nodes <= {trial.executor_node_id},
                f"local index is not colocated for {trial.trial_key}",
            )
            index_mode = "local"
        else:
            index_mode = "none"
        cache_nodes = {
            row["logical_node_ids"][0]
            for row in trial_stages
            if row["action"] == "lookup"
        }
        _require(
            len(cache_nodes) <= 1,
            f"trial spans multiple cache nodes: {trial.trial_key}",
        )
        conditional = any(row["condition"] is not None for row in trial_stages)
        _require(
            conditional == (family == "local-cache-derived"),
            f"conditional semantics disagree with route family: {trial.trial_key}",
        )
        trial_rows.append({
            "schema_version": LOGICAL_TRIAL_SCHEMA_VERSION,
            "trial_key": trial.trial_key,
            "order_index": trial.order_index,
            "workload_id": trial.workload_id,
            "workload_class": trial.workload_class,
            "design_id": trial.design_id,
            "repetition": trial.repetition,
            "object_id": trial.object_id,
            "executor_node_id": trial.executor_node_id,
            "route_template_id": route_template,
            "route_family": family,
            "index_mode": index_mode,
            "representation_ids": representations,
            "conditional_cache_branch": conditional,
            "cache_node_id": next(iter(cache_nodes), None),
            "required_provisioning_chain_ids": required_chain_ids,
            "required_service_nodes": sorted(
                required_nodes,
                key=_node_order,
            ),
            "required_service_contract_ids": sorted(required_contract_ids),
            "required_trial_service_nodes": sorted(
                trial_nodes,
                key=_node_order,
            ),
            "required_trial_service_contract_ids": sorted(
                trial_contract_ids
            ),
            "execution_stage_keys": [
                row["stage_key"]
                for row in trial_stages
                if row["phase"] == "execution"
            ],
            "evaluation_stage_keys": [result_key, score_key],
            "source_matrix_operation_count": len(source_rows),
            "full_flow_trial_stage_count": len(trial_stages),
            "deployment_binding_included": False,
            "secret_material_included": False,
        })

    coverage_groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in trial_rows:
        coverage_groups[(row["workload_class"], row["design_id"])].append(row)
    coverage_rows: list[dict[str, Any]] = []
    for key in sorted(coverage_groups):
        members = coverage_groups[key]
        first = members[0]
        invariant_fields = (
            "route_template_id",
            "route_family",
            "index_mode",
            "representation_ids",
            "conditional_cache_branch",
            "cache_node_id",
            "required_service_nodes",
            "required_service_contract_ids",
            "required_trial_service_nodes",
            "required_trial_service_contract_ids",
        )
        for field in invariant_fields:
            _require(
                all(member[field] == first[field] for member in members),
                f"repetitions disagree for coverage cell {key}: {field}",
            )
        coverage_rows.append({
            "schema_version": LOGICAL_COVERAGE_SCHEMA_VERSION,
            "workload_class": key[0],
            "design_id": key[1],
            "trial_count": len(members),
            "repetitions": sorted(member["repetition"] for member in members),
            "route_template_id": first["route_template_id"],
            "route_family": first["route_family"],
            "index_mode": first["index_mode"],
            "representation_ids": first["representation_ids"],
            "conditional_cache_branch": first[
                "conditional_cache_branch"
            ],
            "cache_node_id": first["cache_node_id"],
            "required_service_nodes": first["required_service_nodes"],
            "required_service_contract_ids": first[
                "required_service_contract_ids"
            ],
            "required_trial_service_nodes": first[
                "required_trial_service_nodes"
            ],
            "required_trial_service_contract_ids": first[
                "required_trial_service_contract_ids"
            ],
            "deployment_binding_included": False,
        })
    return stages, trial_rows, coverage_rows


def _coverage_summary(
    contracts: Mapping[str, dict[str, Any]],
    stages: list[dict[str, Any]],
    trials: list[dict[str, Any]],
    coverage: list[dict[str, Any]],
) -> dict[str, Any]:
    route_counts = Counter(row["route_family"] for row in trials)
    workload_counts = Counter(row["workload_class"] for row in trials)
    design_counts = Counter(row["design_id"] for row in trials)
    node_counts: Counter[str] = Counter()
    contract_counts: Counter[str] = Counter()
    for row in trials:
        node_counts.update(row["required_service_nodes"])
        contract_counts.update(row["required_service_contract_ids"])
    used_contracts = set(contract_counts)
    return {
        "matrix_cell_count": len(coverage),
        "trial_count": len(trials),
        "route_family_trial_counts": dict(sorted(route_counts.items())),
        "workload_class_trial_counts": dict(sorted(workload_counts.items())),
        "design_trial_counts": dict(sorted(design_counts.items())),
        "service_node_trial_coverage": {
            key: node_counts[key]
            for key in sorted(node_counts, key=_node_order)
        },
        "service_contract_trial_coverage": {
            key: contract_counts[key] for key in sorted(contract_counts)
        },
        "catalog_service_contract_count": len(contracts),
        "required_service_contract_count": len(used_contracts),
        "unused_service_contract_ids": sorted(set(contracts) - used_contracts),
        "logical_stage_count": len(stages),
        "stage_phase_counts": dict(sorted(Counter(
            row["phase"] for row in stages
        ).items())),
        "source_matrix_operation_stage_count": sum(
            row["counted_in_source_matrix"] for row in stages
        ),
        "full_flow_extension_stage_count": sum(
            not row["counted_in_source_matrix"] for row in stages
        ),
        "conditional_stage_count": sum(
            row["condition"] is not None for row in stages
        ),
        "conditional_trial_count": sum(
            row["conditional_cache_branch"] for row in trials
        ),
        "artifact_provisioning_chain_count": len({
            row["scope_id"]
            for row in stages
            if row["scope_kind"] == "artifact-provisioning"
        }),
    }


def _documents(
    scenario: SimulatorScenario,
    container_root: Path,
    compiler_id: str,
) -> dict[str, bytes]:
    _validate_scenario_shape(scenario)
    compiler_id = _identifier(compiler_id, "compiler_id")
    manifest, operations, source_bindings = _load_source(
        scenario,
        container_root,
    )
    trials = build_simulator_trials(scenario)
    grouped = _validate_source_operations(
        scenario,
        trials,
        manifest,
        operations,
    )
    contracts = _base_service_contracts(scenario)
    stages, trial_rows, coverage_rows = _compile_trials(
        scenario,
        trials,
        grouped,
        contracts,
    )
    service_catalog = {
        "schema_version": LOGICAL_SERVICE_CATALOG_SCHEMA_VERSION,
        "compiler_id": compiler_id,
        "scenario_id": scenario.scenario_id,
        "service_contract_count": len(contracts),
        "service_contracts": [contracts[key] for key in sorted(contracts)],
        "contract_layer": "logical-service-semantics-only",
        "deployment_binding_included": False,
        "deployment_implementation_coverage_claimed": False,
        "external_services_called": False,
        "secret_material_included": False,
    }
    catalog_bytes = _json_bytes(service_catalog)
    stages_bytes = _jsonl_bytes(stages)
    trials_bytes = _jsonl_bytes(trial_rows)
    coverage_bytes = _jsonl_bytes(coverage_rows)
    coverage_summary = _coverage_summary(
        contracts,
        stages,
        trial_rows,
        coverage_rows,
    )
    source_binding_sha256 = _sha256_bytes(_canonical_bytes(source_bindings))
    output_sha256 = {
        SERVICE_CATALOG_NAME: _sha256_bytes(catalog_bytes),
        STAGES_NAME: _sha256_bytes(stages_bytes),
        TRIALS_NAME: _sha256_bytes(trials_bytes),
        COVERAGE_NAME: _sha256_bytes(coverage_bytes),
    }
    plan: dict[str, Any] = {
        "schema_version": LOGICAL_ROUTE_PLAN_SCHEMA_VERSION,
        "status": "FROZEN_ENDPOINT_FREE_LOGICAL_ROUTES",
        "compiler_id": compiler_id,
        "scenario_id": scenario.scenario_id,
        "source_bindings": source_bindings,
        "source_binding_sha256": source_binding_sha256,
        "matrix_dimensions": {
            "workload_classes": sorted({
                workload.workload_class for workload in scenario.workloads
            }),
            "design_ids": sorted(_EXPECTED_DESIGNS),
            "repetitions": scenario.repetitions,
            "trial_count": len(trial_rows),
        },
        "coverage_summary": coverage_summary,
        "source_matrix_semantics": {
            "operation_count": len(operations),
            "execution_stages_preserved_one_to_one": True,
            "source_container_binding_values_copied": False,
        },
        "full_flow_extensions": {
            "artifact_provisioning_is_pretrial": True,
            "hidden_scoring_is_post_inference": True,
            "extensions_counted_in_source_matrix": False,
            "hidden_oracle_content_included": False,
            "extension_runtime_capability_claimed": False,
        },
        "deployment_boundary": {
            "logical_plan_contains_network_addresses": False,
            "logical_plan_contains_secret_material": False,
            "logical_plan_contains_host_paths": False,
            "service_contract_binding_required_at_runtime": True,
            "storage_binding_required_at_runtime": True,
            "network_link_binding_required_at_runtime": True,
            "container_and_multi_node_deployments_share_contracts": True,
            "source_container_bindings_retained_only_by_hash": True,
            "implementation_capability_validation_required": True,
            "unbound_cross_service_handoffs": [
                "transport.N3-N5-raw-materialization-input",
                "transport.N5-N4-derived-publication",
                "transport.N6-N1-answer-scoring-input",
            ],
        },
        "output_sha256": output_sha256,
        "services_started": False,
        "external_services_called": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    plan["plan_sha256"] = _sha256_bytes(_canonical_bytes(plan))
    for document in (
        service_catalog,
        stages,
        trial_rows,
        coverage_rows,
        plan,
    ):
        _assert_safe_document(document)
    documents = {
        SERVICE_CATALOG_NAME: catalog_bytes,
        STAGES_NAME: stages_bytes,
        TRIALS_NAME: trials_bytes,
        COVERAGE_NAME: coverage_bytes,
        PLAN_NAME: _json_bytes(plan),
    }
    documents[CHECKSUMS_NAME] = b"".join(
        f"{_sha256_bytes(documents[name])}  {name}\n".encode("utf-8")
        for name in sorted(_CONTENT_FILES)
    )
    return documents


_STAGE_KEYS = {
    "action",
    "condition",
    "counted_in_source_matrix",
    "dependency_stage_keys",
    "deployment_binding_included",
    "logical_node_ids",
    "object_id",
    "phase",
    "planned_logical_bytes",
    "representation_id",
    "schema_version",
    "scope_id",
    "scope_kind",
    "secret_material_included",
    "service_contract_id",
    "source_cache_id",
    "source_link_id",
    "source_operation_id",
    "source_operation_key",
    "source_operation_kind",
    "source_resource_id",
    "stage_index",
    "stage_key",
    "trial_key",
}

_TRIAL_KEYS = {
    "cache_node_id",
    "conditional_cache_branch",
    "deployment_binding_included",
    "design_id",
    "evaluation_stage_keys",
    "execution_stage_keys",
    "executor_node_id",
    "full_flow_trial_stage_count",
    "index_mode",
    "object_id",
    "order_index",
    "repetition",
    "representation_ids",
    "required_provisioning_chain_ids",
    "required_service_contract_ids",
    "required_service_nodes",
    "required_trial_service_contract_ids",
    "required_trial_service_nodes",
    "route_family",
    "route_template_id",
    "schema_version",
    "secret_material_included",
    "source_matrix_operation_count",
    "trial_key",
    "workload_class",
    "workload_id",
}


def _verify_published(root: Path) -> dict[str, Any]:
    _require(root.is_dir(), "logical route plan directory does not exist")
    entries = list(root.iterdir())
    _require(
        all(path.is_file() and not path.is_symlink() for path in entries),
        "logical route plan must contain regular files only",
    )
    _require(
        {path.name for path in entries} == _ALL_FILES,
        "logical route plan file set changed",
    )
    try:
        checksum_lines = (root / CHECKSUMS_NAME).read_text(
            encoding="utf-8"
        ).splitlines()
    except (OSError, UnicodeError) as exc:
        raise FullFlowLogicalRouteError("cannot read SHA256SUMS") from exc
    _require(
        len(checksum_lines) == len(_CONTENT_FILES),
        "logical route checksums are incomplete",
    )
    checksums: dict[str, str] = {}
    for line in checksum_lines:
        digest, separator, name = line.partition("  ")
        _require(
            separator == "  "
            and name in _CONTENT_FILES
            and bool(_HEX_SHA256.fullmatch(digest)),
            "logical route SHA256SUMS is malformed",
        )
        _require(name not in checksums, f"duplicate checksum entry: {name}")
        _require(
            _sha256_bytes((root / name).read_bytes()) == digest,
            f"logical route checksum mismatch: {name}",
        )
        checksums[name] = digest
    _require(
        list(checksums) == sorted(_CONTENT_FILES),
        "logical route checksums are not canonical",
    )

    _, plan_value = _read_json(root / PLAN_NAME, "logical route plan")
    plan = dict(_mapping(plan_value, "logical route plan"))
    _require(
        plan.get("schema_version") == LOGICAL_ROUTE_PLAN_SCHEMA_VERSION,
        "unsupported logical route plan schema_version",
    )
    _require(
        plan.get("status") == "FROZEN_ENDPOINT_FREE_LOGICAL_ROUTES",
        "logical route plan is incomplete",
    )
    recorded_plan_sha = plan.pop("plan_sha256", None)
    _require(
        recorded_plan_sha == _sha256_bytes(_canonical_bytes(plan)),
        "logical route plan_sha256 mismatch",
    )
    plan["plan_sha256"] = recorded_plan_sha
    _require(
        plan.get("output_sha256")
        == {
            name: checksums[name]
            for name in sorted(_CONTENT_FILES - {PLAN_NAME})
        },
        "logical route output digests disagree",
    )
    _require(
        plan.get("source_binding_sha256")
        == _sha256_bytes(_canonical_bytes(plan.get("source_bindings"))),
        "logical route source binding digest mismatch",
    )
    boundary = _mapping(
        plan.get("deployment_boundary"), "deployment boundary"
    )
    _require(
        boundary.get("logical_plan_contains_network_addresses") is False
        and boundary.get("logical_plan_contains_secret_material") is False
        and boundary.get("logical_plan_contains_host_paths") is False,
        "logical plan incorrectly claims to contain runtime bindings",
    )

    _, catalog_value = _read_json(
        root / SERVICE_CATALOG_NAME,
        "logical service catalog",
    )
    catalog = _mapping(catalog_value, "logical service catalog")
    _require(
        catalog.get("schema_version")
        == LOGICAL_SERVICE_CATALOG_SCHEMA_VERSION,
        "unsupported logical service catalog schema_version",
    )
    contract_rows = catalog.get("service_contracts")
    _require(isinstance(contract_rows, list), "service contracts must be a list")
    contracts: dict[str, Mapping[str, Any]] = {}
    for row in contract_rows:
        row = _mapping(row, "service contract")
        _require(
            row.get("schema_version")
            == LOGICAL_SERVICE_CONTRACT_SCHEMA_VERSION,
            "unsupported logical service contract schema_version",
        )
        contract_id = _string(
            row.get("service_contract_id"), "service_contract_id"
        )
        _require(contract_id not in contracts, "duplicate service contract")
        _require(
            row.get("deployment_binding_included") is False
            and row.get("secret_material_included") is False,
            f"service contract contains a runtime binding: {contract_id}",
        )
        nodes = row.get("logical_node_ids")
        _require(
            isinstance(nodes, list)
            and bool(nodes)
            and set(nodes) <= _EXPECTED_NODES,
            f"service contract has invalid nodes: {contract_id}",
        )
        contracts[contract_id] = row
    _require(
        len(contracts) == catalog.get("service_contract_count"),
        "service contract count mismatch",
    )

    _, stages = _read_jsonl(root / STAGES_NAME, "logical route stages")
    stage_by_key: dict[str, dict[str, Any]] = {}
    by_scope: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in stages:
        _strict_keys(row, _STAGE_KEYS, "logical stage")
        _require(
            row["schema_version"] == LOGICAL_STAGE_SCHEMA_VERSION,
            "unsupported logical stage schema_version",
        )
        key = _string(row["stage_key"], "stage_key")
        _require(key not in stage_by_key, f"duplicate logical stage: {key}")
        _require(
            row["service_contract_id"] in contracts,
            f"logical stage has unknown service contract: {key}",
        )
        _require(
            row["logical_node_ids"]
            == contracts[row["service_contract_id"]]["logical_node_ids"],
            f"logical stage nodes disagree with service contract: {key}",
        )
        logical_bytes = row["planned_logical_bytes"]
        _require(
            logical_bytes is None
            or (type(logical_bytes) is int and logical_bytes >= 0),
            f"logical stage has invalid planned bytes: {key}",
        )
        _require(
            row["deployment_binding_included"] is False
            and row["secret_material_included"] is False,
            f"logical stage contains a runtime binding: {key}",
        )
        if row["counted_in_source_matrix"]:
            _require(
                row["phase"] == "execution"
                and row["source_operation_key"] == key,
                f"source operation mapping is invalid: {key}",
            )
        else:
            _require(
                row["source_operation_key"] is None,
                f"extension stage claims a source operation: {key}",
            )
        stage_by_key[key] = row
        by_scope[_string(row["scope_id"], "scope_id")].append(row)
    for scope_id, rows in by_scope.items():
        _require(
            [row["stage_index"] for row in rows] == list(range(len(rows))),
            f"logical stage indices are not canonical: {scope_id}",
        )
        seen: set[str] = set()
        for row in rows:
            dependencies = row["dependency_stage_keys"]
            _require(
                isinstance(dependencies, list)
                and len(dependencies) == len(set(dependencies)),
                f"logical stage dependencies are invalid: {row['stage_key']}",
            )
            _require(
                set(dependencies) <= seen,
                f"logical stage dependencies are not topological: "
                f"{row['stage_key']}",
            )
            condition = row["condition"]
            if condition is not None:
                condition = _mapping(condition, "logical stage condition")
                lookup_key = condition.get("cache_operation_key")
                _require(
                    lookup_key in seen
                    and stage_by_key[lookup_key]["action"] == "lookup",
                    f"logical condition has no cache lookup: {row['stage_key']}",
                )
            seen.add(row["stage_key"])

    _, trial_rows = _read_jsonl(root / TRIALS_NAME, "logical route trials")
    trial_keys: set[str] = set()
    for row in trial_rows:
        _strict_keys(row, _TRIAL_KEYS, "logical trial")
        _require(
            row["schema_version"] == LOGICAL_TRIAL_SCHEMA_VERSION,
            "unsupported logical trial schema_version",
        )
        trial_key = _string(row["trial_key"], "logical trial_key")
        _require(trial_key not in trial_keys, "duplicate logical trial")
        trial_keys.add(trial_key)
        required_contracts = row["required_service_contract_ids"]
        _require(
            isinstance(required_contracts, list)
            and required_contracts == sorted(set(required_contracts))
            and set(required_contracts) <= set(contracts),
            f"logical trial service coverage is invalid: {trial_key}",
        )
        required_nodes = {
            node
            for contract_id in required_contracts
            for node in contracts[contract_id]["logical_node_ids"]
        }
        _require(
            row["required_service_nodes"]
            == sorted(required_nodes, key=_node_order),
            f"logical trial node coverage is invalid: {trial_key}",
        )
        trial_contracts = row["required_trial_service_contract_ids"]
        _require(
            isinstance(trial_contracts, list)
            and trial_contracts == sorted(set(trial_contracts))
            and set(trial_contracts) <= set(required_contracts),
            f"logical trial-only service coverage is invalid: {trial_key}",
        )
        trial_nodes = {
            node
            for contract_id in trial_contracts
            for node in contracts[contract_id]["logical_node_ids"]
        }
        _require(
            row["required_trial_service_nodes"]
            == sorted(trial_nodes, key=_node_order),
            f"logical trial-only node coverage is invalid: {trial_key}",
        )
        execution_keys = row["execution_stage_keys"]
        evaluation_keys = row["evaluation_stage_keys"]
        _require(
            all(key in stage_by_key for key in execution_keys + evaluation_keys),
            f"logical trial references an unknown stage: {trial_key}",
        )
        _require(
            len(execution_keys) == row["source_matrix_operation_count"]
            and len(execution_keys) + len(evaluation_keys)
            == row["full_flow_trial_stage_count"],
            f"logical trial stage counts disagree: {trial_key}",
        )
        _require(
            len(evaluation_keys) == 2,
            f"logical trial has no answer handoff and hidden score: {trial_key}",
        )
        _require(
            row["deployment_binding_included"] is False
            and row["secret_material_included"] is False,
            f"logical trial contains a runtime binding: {trial_key}",
        )
    _, coverage_rows = _read_jsonl(
        root / COVERAGE_NAME,
        "logical route coverage",
    )
    dimensions = _mapping(plan.get("matrix_dimensions"), "matrix dimensions")
    workload_classes = dimensions.get("workload_classes")
    _require(
        isinstance(workload_classes, list)
        and bool(workload_classes)
        and workload_classes == sorted(set(workload_classes))
        and set(workload_classes) <= _EXPECTED_WORKLOAD_CLASSES,
        "logical route workload-class dimensions are invalid",
    )
    _require(
        len(coverage_rows) == len(workload_classes) * len(_EXPECTED_DESIGNS),
        "logical route coverage dimensions disagree",
    )
    coverage_keys = {
        (row.get("workload_class"), row.get("design_id"))
        for row in coverage_rows
    }
    _require(
        coverage_keys
        == {
            (workload_class, design_id)
            for workload_class in workload_classes
            for design_id in _EXPECTED_DESIGNS
        },
        "logical route coverage cells changed",
    )
    summary = _mapping(plan.get("coverage_summary"), "coverage summary")
    _require(
        summary.get("trial_count") == len(trial_rows)
        and summary.get("logical_stage_count") == len(stages)
        and summary.get("matrix_cell_count") == len(coverage_rows),
        "logical route coverage summary counts disagree",
    )
    _assert_safe_document([catalog, stages, trial_rows, coverage_rows, plan])
    return plan


def _publish(target: Path, documents: Mapping[str, bytes]) -> None:
    _require(not target.exists(), f"logical route output already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging_parent = Path(
        tempfile.mkdtemp(prefix=".logical-routes-", dir=target.parent)
    )
    staging = staging_parent / "output"
    try:
        staging.mkdir()
        for name, content in documents.items():
            path = staging / name
            with path.open("wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
        _verify_published(staging)
        os.replace(staging, target)
    finally:
        shutil.rmtree(staging_parent, ignore_errors=True)


def compile_full_flow_logical_routes(
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    *,
    output_dir: str | Path,
    compiler_id: str = "full-flow-logical-route-compiler-v1",
) -> dict[str, Any]:
    """Compile a frozen workload-by-design plan into endpoint-free routes."""

    scenario = load_simulator_scenario(scenario_path)
    container_root = Path(container_plan_dir).resolve()
    _require(
        container_root.is_dir(), "container backend plan directory is missing"
    )
    documents = _documents(scenario, container_root, compiler_id)
    target = Path(output_dir).resolve()
    _publish(target, documents)
    plan = _verify_published(target)
    summary = plan["coverage_summary"]
    return {
        "status": plan["status"],
        "scenario_id": plan["scenario_id"],
        "plan_sha256": plan["plan_sha256"],
        "trial_count": summary["trial_count"],
        "matrix_cell_count": summary["matrix_cell_count"],
        "logical_stage_count": summary["logical_stage_count"],
        "route_family_trial_counts": summary[
            "route_family_trial_counts"
        ],
        "service_node_trial_coverage": summary[
            "service_node_trial_coverage"
        ],
        "output_dir": str(target),
        "services_started": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def verify_full_flow_logical_routes(
    plan_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
) -> dict[str, Any]:
    """Strictly verify checksums, semantics, and deterministic source binding."""

    root = Path(plan_dir).resolve()
    plan = _verify_published(root)
    scenario = load_simulator_scenario(scenario_path)
    expected = _documents(
        scenario,
        Path(container_plan_dir).resolve(),
        _identifier(plan.get("compiler_id"), "compiler_id"),
    )
    for name in sorted(_ALL_FILES):
        _require(
            (root / name).read_bytes() == expected[name],
            f"logical route output does not match deterministic source "
            f"recompilation: {name}",
        )
    summary = plan["coverage_summary"]
    return {
        "status": "VERIFIED",
        "scenario_id": plan["scenario_id"],
        "plan_sha256": plan["plan_sha256"],
        "source_binding_sha256": plan["source_binding_sha256"],
        "trial_count": summary["trial_count"],
        "matrix_cell_count": summary["matrix_cell_count"],
        "logical_stage_count": summary["logical_stage_count"],
        "route_family_trial_counts": summary[
            "route_family_trial_counts"
        ],
        "service_node_trial_coverage": summary[
            "service_node_trial_coverage"
        ],
        "deployment_binding_included": False,
        "services_started": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
