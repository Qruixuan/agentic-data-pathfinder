"""Bind simulator infrastructure evidence to Data Agent semantic evidence.

This module deliberately creates a simulator-specific evidence bundle.  It
does not manufacture a distributed-pilot record, a monetary cost ledger, or
an AWM/OED input.  The bridge's only job is to prove that one semantic
observation and one completed infrastructure-matrix trial share an exact
trial-key association, with the synthetic matrix object and benchmark
artifact joined only through an explicit binding specification.

Every source is verified by its native offline verifier before it is read.
The output retains only identifiers, digests, bounded telemetry, and the
semantic score; prompts, answers, artifact bytes, credentials, and absolute
paths are never copied into the bundle.
"""

from __future__ import annotations

import json
import math
import os
import re
import shutil
import tempfile
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Sequence

from .container_matrix import verify_flowmesh_container_matrix_plan
from .container_matrix_runner import verify_flowmesh_container_matrix_run

if TYPE_CHECKING:
    from ...distributed.registry import EndpointRegistry


PATHFINDER_EVIDENCE_RECORD_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-pathfinder-evidence-record/v1alpha1"
)
PATHFINDER_EVIDENCE_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-pathfinder-evidence-bundle/v1alpha1"
)
PATHFINDER_EVIDENCE_SPEC_SCHEMA_VERSION = (
    "pathfinder.flowmesh-container-pathfinder-evidence-spec/v1alpha1"
)

_RECORDS_FILE = "pathfinder-evidence-records.jsonl"
_MANIFEST_FILE = "pathfinder-evidence-manifest.json"
_OUTPUT_FILES = {_RECORDS_FILE, _MANIFEST_FILE}
_HEX_SHA256 = re.compile(r"[0-9a-f]{64}")
_RUNTIME_EPOCH = re.compile(r"[0-9a-f]{32}")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._|:-]{0,255}")
PATHFINDER_EVIDENCE_SPEC_REQUIRED_KEYS = frozenset({
    "schema_version",
    "evidence_bundle_id",
    "matrix_id",
    "matrix_plan_sha256",
    "matrix_run_id",
    "bindings",
    "execution_and_semantic_route_unified",
    "cost_basis",
    "credentials_recorded",
    "eligible_for_awm_oed",
    "eligible_for_scientific_claims",
})
PATHFINDER_EVIDENCE_BINDING_REQUIRED_KEYS = frozenset({
    "semantic_run_id",
    "semantic_spec_sha256",
    "event_index",
    "expected_model",
    "trial_key",
    "workload_id",
    "workload_class",
    "matrix_design_id",
    "data_agent_route_design_id",
    "repetition",
    "matrix_object_id",
    "artifact_object_id",
    "representation_id",
})

# Backward-compatible private aliases.  New consumers should use the public,
# immutable contract constants above rather than depending on internals.
_SPEC_KEYS = PATHFINDER_EVIDENCE_SPEC_REQUIRED_KEYS
_BINDING_KEYS = PATHFINDER_EVIDENCE_BINDING_REQUIRED_KEYS


class FlowMeshPathfinderEvidenceError(ValueError):
    """Raised when the three evidence layers cannot be joined exactly."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise FlowMeshPathfinderEvidenceError(message)


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _invalid_number(value: str) -> None:
    raise FlowMeshPathfinderEvidenceError(f"non-finite JSON number: {value}")


def _read_json(path: Path, label: str) -> tuple[bytes, dict[str, Any]]:
    _require(path.is_file() and not path.is_symlink(), f"{label} is missing")
    try:
        raw = path.read_bytes()
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_keys,
            parse_constant=_invalid_number,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FlowMeshPathfinderEvidenceError(
            f"{label} is not valid UTF-8 JSON"
        ) from exc
    _require(isinstance(value, dict), f"{label} must be a JSON object")
    return raw, value


def _read_jsonl(path: Path, label: str) -> tuple[bytes, list[dict[str, Any]]]:
    _require(path.is_file() and not path.is_symlink(), f"{label} is missing")
    try:
        raw = path.read_bytes()
        text = raw.decode("utf-8")
    except (OSError, UnicodeError) as exc:
        raise FlowMeshPathfinderEvidenceError(
            f"{label} is not readable UTF-8"
        ) from exc
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            row = json.loads(
                line,
                object_pairs_hook=_unique_keys,
                parse_constant=_invalid_number,
            )
        except (json.JSONDecodeError, FlowMeshPathfinderEvidenceError) as exc:
            raise FlowMeshPathfinderEvidenceError(
                f"{label}:{line_number} is invalid JSON: {exc}"
            ) from exc
        _require(isinstance(row, dict), f"{label}:{line_number} is not an object")
        rows.append(row)
    _require(bool(rows), f"{label} is empty")
    return raw, rows


def _sha256_bytes(value: bytes) -> str:
    return sha256(value).hexdigest()


def _sha256_path(path: Path, label: str) -> str:
    try:
        return _sha256_bytes(path.read_bytes())
    except OSError as exc:
        raise FlowMeshPathfinderEvidenceError(f"cannot read {label}") from exc


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


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
    return b"".join(_canonical_bytes(value) + b"\n" for value in values)


def _checksums(documents: Mapping[str, bytes]) -> bytes:
    return "".join(
        f"{_sha256_bytes(content)}  {name}\n"
        for name, content in sorted(documents.items())
    ).encode("utf-8")


def _text(value: Any, label: str) -> str:
    _require(
        isinstance(value, str) and bool(value.strip()),
        f"{label} must be a non-empty string",
    )
    return value.strip()


def _identifier(value: Any, label: str) -> str:
    result = _text(value, label)
    _require(_IDENTIFIER.fullmatch(result) is not None, f"{label} is invalid")
    return result


def _digest(value: Any, label: str) -> str:
    result = _text(value, label)
    _require(_HEX_SHA256.fullmatch(result) is not None, f"{label} is invalid")
    return result


def _integer(value: Any, label: str, *, minimum: int = 0) -> int:
    _require(
        type(value) is int and value >= minimum,
        f"{label} must be an integer of at least {minimum}",
    )
    return value


def _number_or_none(value: Any, label: str) -> float | None:
    if value is None:
        return None
    _require(
        type(value) in (int, float)
        and math.isfinite(float(value))
        and float(value) >= 0.0,
        f"{label} must be a non-negative finite number or null",
    )
    return float(value)


def _literal(value: Any, expected: Any, label: str) -> None:
    _require(type(value) is type(expected) and value == expected, f"{label} changed")


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    _require(isinstance(value, Mapping), f"{label} must be an object")
    return value


def _load_binding_spec(path: str | Path) -> tuple[dict[str, Any], str]:
    raw, value = _read_json(Path(path).resolve(), "Pathfinder evidence spec")
    _require(set(value) == _SPEC_KEYS, "Pathfinder evidence spec fields changed")
    _require(
        value.get("schema_version") == PATHFINDER_EVIDENCE_SPEC_SCHEMA_VERSION,
        "unsupported Pathfinder evidence spec schema",
    )
    _identifier(value.get("evidence_bundle_id"), "evidence_bundle_id")
    _identifier(value.get("matrix_id"), "spec matrix_id")
    _digest(value.get("matrix_plan_sha256"), "spec matrix_plan_sha256")
    _identifier(value.get("matrix_run_id"), "spec matrix_run_id")
    _literal(
        value.get("execution_and_semantic_route_unified"),
        False,
        "spec execution_and_semantic_route_unified",
    )
    _require(
        value.get("cost_basis") == "unavailable",
        "spec cost_basis must be unavailable",
    )
    _literal(value.get("credentials_recorded"), False, "spec credentials_recorded")
    _literal(value.get("eligible_for_awm_oed"), False, "spec eligible_for_awm_oed")
    _literal(
        value.get("eligible_for_scientific_claims"),
        False,
        "spec eligible_for_scientific_claims",
    )
    bindings = value.get("bindings")
    _require(
        isinstance(bindings, list) and bool(bindings),
        "spec bindings must be a non-empty array",
    )
    semantic_ids: set[str] = set()
    trial_keys: set[str] = set()
    normalized: list[dict[str, Any]] = []
    for index, item in enumerate(bindings):
        _require(isinstance(item, dict), f"bindings[{index}] must be an object")
        _require(set(item) == _BINDING_KEYS, f"bindings[{index}] fields changed")
        binding = {
            "semantic_run_id": _identifier(
                item.get("semantic_run_id"), f"bindings[{index}].semantic_run_id"
            ),
            "semantic_spec_sha256": _digest(
                item.get("semantic_spec_sha256"),
                f"bindings[{index}].semantic_spec_sha256",
            ),
            "event_index": _integer(
                item.get("event_index"),
                f"bindings[{index}].event_index",
            ),
            "expected_model": _text(
                item.get("expected_model"),
                f"bindings[{index}].expected_model",
            ),
            "trial_key": _identifier(
                item.get("trial_key"), f"bindings[{index}].trial_key"
            ),
            "workload_id": _identifier(
                item.get("workload_id"), f"bindings[{index}].workload_id"
            ),
            "workload_class": _identifier(
                item.get("workload_class"), f"bindings[{index}].workload_class"
            ),
            "matrix_design_id": _identifier(
                item.get("matrix_design_id"),
                f"bindings[{index}].matrix_design_id",
            ),
            "data_agent_route_design_id": _identifier(
                item.get("data_agent_route_design_id"),
                f"bindings[{index}].data_agent_route_design_id",
            ),
            "repetition": _integer(
                item.get("repetition"), f"bindings[{index}].repetition"
            ),
            "matrix_object_id": _identifier(
                item.get("matrix_object_id"), f"bindings[{index}].matrix_object_id"
            ),
            "artifact_object_id": _identifier(
                item.get("artifact_object_id"), f"bindings[{index}].artifact_object_id"
            ),
            "representation_id": _identifier(
                item.get("representation_id"), f"bindings[{index}].representation_id"
            ),
        }
        _require(
            binding["semantic_run_id"] not in semantic_ids,
            "spec semantic_run_id values must be unique",
        )
        _require(
            binding["trial_key"] not in trial_keys,
            "spec trial_key values must be unique",
        )
        semantic_ids.add(binding["semantic_run_id"])
        trial_keys.add(binding["trial_key"])
        normalized.append(binding)
    value["bindings"] = normalized
    return value, _sha256_bytes(raw)


def _verify_semantic_source(
    root: Path,
    matrix_plan_dir: Path,
    endpoint_registry: EndpointRegistry,
    semantic_spec: Path,
) -> Mapping[str, Any]:
    # Kept lazy so this evidence module stays importable while a deployment
    # that predates the optional Data Agent semantic vertical is inspected.
    from ...simulator.data_agent_semantic_vertical import (
        verify_data_agent_frame_bundle_semantic_trial,
    )

    return verify_data_agent_frame_bundle_semantic_trial(
        output_dir=root,
        matrix_plan_dir=matrix_plan_dir,
        endpoint_registry=endpoint_registry,
        semantic_spec=semantic_spec,
    )


def _load_sources(
    *,
    binding_spec: str | Path,
    matrix_plan_dir: str | Path,
    matrix_run_dir: str | Path,
    endpoint_registry: EndpointRegistry,
    data_agent_semantic_dirs: Sequence[str | Path],
    data_agent_semantic_specs: Sequence[str | Path],
) -> dict[str, Any]:
    spec, spec_sha256 = _load_binding_spec(binding_spec)
    plan_root = Path(matrix_plan_dir).resolve()
    run_root = Path(matrix_run_dir).resolve()
    semantic_roots = [Path(value).resolve() for value in data_agent_semantic_dirs]
    semantic_specs = [Path(value).resolve() for value in data_agent_semantic_specs]
    _require(
        bool(semantic_roots),
        "at least one Data Agent semantic directory is required",
    )
    _require(
        len(semantic_specs) == len(semantic_roots),
        "Data Agent semantic directories and specs must have equal counts",
    )
    _require(
        len(set(semantic_roots)) == len(semantic_roots),
        "Data Agent semantic directories contain duplicates",
    )
    _require(
        len(set(semantic_specs)) == len(semantic_specs),
        "Data Agent semantic specs contain duplicates",
    )

    plan_verification = verify_flowmesh_container_matrix_plan(plan_root)
    _require(
        plan_verification.get("status") == "VERIFIED",
        "matrix plan is not verified",
    )
    run_verification = verify_flowmesh_container_matrix_run(run_root)
    _require(run_verification.get("status") == "VERIFIED", "matrix run is not verified")

    plan_raw, plan = _read_json(
        plan_root / "flowmesh-container-matrix-plan.json", "matrix plan"
    )
    trials_raw, trials = _read_jsonl(
        plan_root / "flowmesh-container-matrix-trials.jsonl", "matrix trials"
    )
    operations_raw, operations = _read_jsonl(
        plan_root / "flowmesh-container-matrix-operations.jsonl",
        "matrix operations",
    )
    run_raw, run = _read_json(
        run_root / "flowmesh-container-matrix-run.json", "matrix run"
    )
    trial_results_raw, trial_results = _read_jsonl(
        run_root / "flowmesh-container-matrix-trial-results.jsonl",
        "matrix trial results",
    )
    operation_results_raw, operation_results = _read_jsonl(
        run_root / "flowmesh-container-matrix-operation-results.jsonl",
        "matrix operation results",
    )

    plan_sha256 = _digest(plan.get("plan_sha256"), "matrix plan_sha256")
    matrix_id = _identifier(plan.get("matrix_id"), "matrix_id")
    _require(
        run.get("matrix_plan_sha256") == plan_sha256,
        "matrix run is bound to a different matrix plan",
    )
    run_id = _identifier(run.get("run_id"), "matrix run_id")
    run_sha256 = _digest(run.get("run_sha256"), "matrix run_sha256")
    _require(spec["matrix_id"] == matrix_id, "spec is bound to a different matrix_id")
    _require(
        spec["matrix_plan_sha256"] == plan_sha256,
        "spec is bound to a different matrix plan",
    )
    _require(spec["matrix_run_id"] == run_id, "spec is bound to a different matrix run")

    trial_by_key: dict[str, dict[str, Any]] = {}
    for row in trials:
        key = _identifier(row.get("trial_key"), "matrix trial_key")
        _require(key not in trial_by_key, f"duplicate matrix trial_key: {key}")
        trial_by_key[key] = row
    result_by_key: dict[str, dict[str, Any]] = {}
    for row in trial_results:
        key = _identifier(row.get("trial_key"), "matrix trial result trial_key")
        _require(key not in result_by_key, f"duplicate matrix result: {key}")
        result_by_key[key] = row
    _require(
        set(trial_by_key) == set(result_by_key),
        "matrix plan and run trial coverage differ",
    )

    operations_by_trial: dict[str, list[dict[str, Any]]] = {
        key: [] for key in trial_by_key
    }
    for row in operations:
        key = _identifier(row.get("trial_key"), "matrix operation trial_key")
        _require(key in operations_by_trial, "matrix operation names an unknown trial")
        operations_by_trial[key].append(row)
    operation_results_by_trial: dict[str, list[dict[str, Any]]] = {
        key: [] for key in trial_by_key
    }
    seen_operation_results: set[str] = set()
    for row in operation_results:
        key = _identifier(row.get("trial_key"), "operation result trial_key")
        operation_key = _identifier(row.get("operation_key"), "operation result key")
        _require(
            key in operation_results_by_trial,
            "operation result names an unknown trial",
        )
        _require(
            operation_key not in seen_operation_results,
            f"duplicate operation result: {operation_key}",
        )
        seen_operation_results.add(operation_key)
        operation_results_by_trial[key].append(row)

    semantic_sources: list[dict[str, Any]] = []
    for semantic_root, semantic_spec in zip(
        semantic_roots,
        semantic_specs,
        strict=True,
    ):
        _require(
            semantic_spec.is_file() and not semantic_spec.is_symlink(),
            "Data Agent semantic spec is missing",
        )
        semantic_spec_sha256 = _sha256_path(
            semantic_spec,
            "Data Agent semantic spec",
        )
        verification = _verify_semantic_source(
            semantic_root,
            plan_root,
            endpoint_registry,
            semantic_spec,
        )
        _require(
            verification.get("status") in {"VERIFIED", "VERIFIED_OFFLINE"},
            "Data Agent semantic evidence is not verified",
        )
        record_raw, record = _read_json(
            semantic_root / "data-agent-frame-bundle-semantic-record.json",
            "Data Agent semantic record",
        )
        manifest_raw, manifest = _read_json(
            semantic_root / "data-agent-frame-bundle-semantic-manifest.json",
            "Data Agent semantic manifest",
        )
        semantic_sources.append(
            {
                "record": record,
                "manifest": manifest,
                "record_sha256": _sha256_bytes(record_raw),
                "manifest_sha256": _sha256_bytes(manifest_raw),
                "semantic_spec_sha256": semantic_spec_sha256,
            }
        )

    binding_ids = {row["semantic_run_id"] for row in spec["bindings"]}
    observed_ids = {
        _identifier(
            row["record"].get("semantic_run_id"),
            "Data Agent semantic_run_id",
        )
        for row in semantic_sources
    }
    _require(
        observed_ids == binding_ids,
        "spec and Data Agent semantic evidence coverage differ",
    )

    return {
        "spec": spec,
        "spec_sha256": spec_sha256,
        "plan": plan,
        "plan_sha256": plan_sha256,
        "matrix_id": matrix_id,
        "run": run,
        "run_id": run_id,
        "run_sha256": run_sha256,
        "trial_by_key": trial_by_key,
        "result_by_key": result_by_key,
        "operations_by_trial": operations_by_trial,
        "operation_results_by_trial": operation_results_by_trial,
        "semantic_sources": semantic_sources,
        "source_files_sha256": {
            "matrix_plan": _sha256_bytes(plan_raw),
            "matrix_trials": _sha256_bytes(trials_raw),
            "matrix_operations": _sha256_bytes(operations_raw),
            "matrix_run": _sha256_bytes(run_raw),
            "matrix_trial_results": _sha256_bytes(trial_results_raw),
            "matrix_operation_results": _sha256_bytes(operation_results_raw),
        },
    }


def _semantic_fields(record: Mapping[str, Any]) -> dict[str, Any]:
    """Select the stable, non-sensitive fields of one vertical record.

    The vertical slice owns its more detailed protocol.  This bridge depends
    only on the public identity, delivery, semantic, and telemetry fields
    listed here, and rejects silence instead of inventing defaults.
    """

    artifact = _mapping(record.get("artifact"), "semantic artifact")
    frame_bundle = _mapping(record.get("frame_bundle"), "semantic frame_bundle")
    delivery = _mapping(record.get("delivery"), "semantic delivery")
    route = _mapping(record.get("route"), "semantic route")
    latency = _mapping(record.get("latency_ms"), "semantic latency_ms")
    return {
        "semantic_run_id": _identifier(
            record.get("semantic_run_id"), "semantic_run_id"
        ),
        "matrix_id": _identifier(record.get("matrix_id"), "semantic matrix_id"),
        "matrix_plan_sha256": _digest(
            record.get("matrix_plan_sha256"), "semantic matrix_plan_sha256"
        ),
        "trial_key": _identifier(record.get("trial_key"), "semantic trial_key"),
        "trial_id": _identifier(record.get("trial_id"), "semantic trial_id"),
        "workload_id": _identifier(record.get("workload_id"), "semantic workload_id"),
        "workload_class": _identifier(
            record.get("workload_class"), "semantic workload_class"
        ),
        "matrix_design_id": _identifier(
            record.get("matrix_design_id"),
            "semantic matrix_design_id",
        ),
        "data_agent_route_design_id": _identifier(
            record.get("data_agent_route_design_id"),
            "semantic data_agent_route_design_id",
        ),
        "repetition": _integer(record.get("repetition"), "semantic repetition"),
        "matrix_object_id": _identifier(
            record.get("matrix_object_id"), "semantic matrix_object_id"
        ),
        "artifact_object_id": _identifier(
            record.get("artifact_object_id"), "semantic artifact_object_id"
        ),
        "matrix_executor_node_id": _identifier(
            record.get("matrix_executor_node_id"),
            "semantic matrix_executor_node_id",
        ),
        "semantic_executor_node_id": _identifier(
            record.get("semantic_executor_node_id"),
            "semantic semantic_executor_node_id",
        ),
        "representation_id": _identifier(
            record.get("representation_id"), "semantic representation_id"
        ),
        "endpoint_registry_id": _identifier(
            record.get("endpoint_registry_id"), "semantic endpoint_registry_id"
        ),
        "endpoint_registry_sha256": _digest(
            record.get("endpoint_registry_sha256"),
            "semantic endpoint_registry_sha256",
        ),
        "semantic_spec_sha256": _digest(
            record.get("semantic_spec_sha256"),
            "semantic semantic_spec_sha256",
        ),
        "event_index": _integer(
            record.get("event_index"),
            "semantic event_index",
        ),
        "route": dict(route),
        "data_agent_plan_id": _identifier(
            record.get("data_agent_plan_id"), "semantic data_agent_plan_id"
        ),
        "data_agent_plan_epoch": _integer(
            record.get("data_agent_plan_epoch"), "semantic data_agent_plan_epoch"
        ),
        "data_agent_access_id": _identifier(
            record.get("data_agent_access_id"), "semantic data_agent_access_id"
        ),
        "artifact": dict(artifact),
        "frame_bundle": dict(frame_bundle),
        "delivery": dict(delivery),
        "latency_ms": dict(latency),
        "success_scoring_rule": _identifier(
            record.get("success_scoring_rule"), "semantic success_scoring_rule"
        ),
        "final_answer_sha256": _digest(
            record.get("final_answer_sha256"), "semantic final_answer_sha256"
        ),
        "task_success": record.get("task_success"),
        "model": _text(record.get("model"), "semantic model"),
        "expected_model": _text(
            record.get("expected_model"),
            "semantic expected_model",
        ),
        "semantic_service_time_ms": _number_or_none(
            record.get("semantic_service_time_ms"),
            "semantic service_time_ms",
        ),
        "semantic_runtime_epoch": _text(
            record.get("semantic_runtime_epoch"),
            "semantic runtime_epoch",
        ),
        "semantic_health_verified": record.get("semantic_health_verified"),
        "container_data_plane_artifact_delivery_verified": record.get(
            "container_data_plane_artifact_delivery_verified"
        ),
        "semantic_frame_payload_integrity_verified": record.get(
            "semantic_frame_payload_integrity_verified"
        ),
        "llm_called": record.get("llm_called"),
        "semantic_telemetry_complete": record.get(
            "semantic_telemetry_complete"
        ),
        "data_agent_artifact_delivery_verified": record.get(
            "data_agent_artifact_delivery_verified"
        ),
        "container_semantic_response_consistency_verified": record.get(
            "container_semantic_response_consistency_verified"
        ),
        "container_runtime_code_provenance_verified": record.get(
            "container_runtime_code_provenance_verified"
        ),
        "scoring_verified": record.get("scoring_verified"),
        "execution_and_semantic_route_unified": record.get(
            "execution_and_semantic_route_unified"
        ),
        "credentials_recorded": record.get("credentials_recorded"),
        "eligible_for_scientific_claims": record.get(
            "eligible_for_scientific_claims"
        ),
    }


def _build_record(
    source: Mapping[str, Any],
    semantic_source: Mapping[str, Any],
    binding: Mapping[str, Any],
) -> dict[str, Any]:
    semantic_record = _mapping(semantic_source.get("record"), "semantic record")
    semantic_manifest = _mapping(semantic_source.get("manifest"), "semantic manifest")
    semantic = _semantic_fields(semantic_record)
    _require(
        _RUNTIME_EPOCH.fullmatch(semantic["semantic_runtime_epoch"]) is not None,
        "semantic runtime epoch is invalid",
    )

    _require(
        semantic_record.get("status") == "COMPLETE",
        "semantic record is incomplete",
    )
    _require(
        semantic_manifest.get("status") == "COMPLETE",
        "semantic manifest is incomplete",
    )
    _require(
        semantic_manifest.get("semantic_run_id") == semantic["semantic_run_id"],
        "semantic manifest run ID changed",
    )
    _require(
        semantic_manifest.get("matrix_id") == semantic["matrix_id"]
        and semantic_manifest.get("matrix_plan_sha256")
        == semantic["matrix_plan_sha256"],
        "semantic manifest matrix binding changed",
    )
    _require(
        semantic_manifest.get("trial_key") == semantic["trial_key"],
        "semantic manifest trial binding changed",
    )
    _require(
        semantic_manifest.get("record_sha256")
        == semantic_source.get("record_sha256"),
        "semantic manifest record digest changed",
    )
    _require(
        semantic_manifest.get("semantic_spec_sha256")
        == semantic["semantic_spec_sha256"]
        == semantic_source.get("semantic_spec_sha256"),
        "semantic spec digest changed",
    )
    _require(
        semantic_manifest.get("semantic_runtime_epoch")
        == semantic["semantic_runtime_epoch"],
        "semantic runtime epoch changed",
    )
    _require(
        semantic_manifest.get("event_index") == semantic["event_index"],
        "semantic event index changed",
    )
    _require(
        semantic_manifest.get("expected_model")
        == semantic["expected_model"]
        == semantic["model"],
        "semantic model differs from its frozen expectation",
    )
    _literal(
        semantic_manifest.get("semantic_health_verified"),
        True,
        "semantic manifest semantic_health_verified",
    )
    for field in (
        "data_agent_artifact_delivery_verified",
        "container_semantic_response_consistency_verified",
        "scoring_verified",
        "llm_called",
    ):
        _literal(semantic_manifest.get(field), True, f"semantic manifest {field}")
    _literal(
        semantic_manifest.get("container_runtime_code_provenance_verified"),
        False,
        "semantic manifest container_runtime_code_provenance_verified",
    )
    _literal(
        semantic_manifest.get("execution_and_semantic_route_unified"),
        False,
        "semantic manifest execution_and_semantic_route_unified",
    )
    _literal(
        semantic_manifest.get("credentials_recorded"),
        False,
        "semantic manifest credentials_recorded",
    )
    _literal(
        semantic_manifest.get("eligible_for_scientific_claims"),
        False,
        "semantic manifest eligible_for_scientific_claims",
    )

    _require(
        type(semantic["task_success"]) is bool,
        "semantic task_success must be boolean",
    )
    for field in (
        "semantic_telemetry_complete",
        "data_agent_artifact_delivery_verified",
        "container_semantic_response_consistency_verified",
        "scoring_verified",
        "llm_called",
        "semantic_health_verified",
        "semantic_frame_payload_integrity_verified",
    ):
        _literal(semantic[field], True, f"semantic {field}")
    _literal(
        semantic["container_runtime_code_provenance_verified"],
        False,
        "semantic container_runtime_code_provenance_verified",
    )
    _literal(
        semantic["container_data_plane_artifact_delivery_verified"],
        False,
        "semantic container_data_plane_artifact_delivery_verified",
    )
    _literal(
        semantic["execution_and_semantic_route_unified"],
        False,
        "semantic execution_and_semantic_route_unified",
    )
    _literal(semantic["credentials_recorded"], False, "semantic credentials_recorded")
    _literal(
        semantic["eligible_for_scientific_claims"],
        False,
        "semantic eligible_for_scientific_claims",
    )
    route = _mapping(semantic["route"], "Data Agent route")
    _require(
        route.get("design_id") == semantic["data_agent_route_design_id"],
        "Data Agent route design differs from semantic association",
    )
    destination_host_node_id = _identifier(
        route.get("destination_execution_node_id"),
        "Data Agent destination host node_id",
    )
    _require(
        route.get("representation_id") == semantic["representation_id"],
        "Data Agent route representation changed",
    )
    _literal(
        route.get("credentials_recorded"),
        False,
        "Data Agent route credentials_recorded",
    )

    artifact = _mapping(semantic["artifact"], "Data Agent artifact")
    artifact_sha256 = _digest(artifact.get("sha256"), "Data Agent artifact sha256")
    artifact_size = _integer(
        artifact.get("size_bytes"), "Data Agent artifact size_bytes", minimum=1
    )
    _require(
        artifact.get("access_id") == semantic["data_agent_access_id"],
        "Data Agent artifact access_id changed",
    )
    _require(
        artifact.get("object_id") == semantic["artifact_object_id"],
        "Data Agent artifact object_id changed",
    )
    catalog_version = artifact.get("object_catalog_version")
    if catalog_version is not None:
        _text(catalog_version, "Data Agent catalog version")
    _text(artifact.get("location"), "Data Agent artifact location")
    _text(artifact.get("media_type"), "Data Agent artifact media_type")

    frame_bundle = _mapping(semantic["frame_bundle"], "semantic frame bundle")
    _require(
        frame_bundle.get("representation_id") == semantic["representation_id"],
        "frame bundle representation_id changed",
    )
    _require(
        frame_bundle.get("artifact_sha256") == artifact_sha256,
        "frame bundle and Data Agent artifact digests differ",
    )
    _require(
        frame_bundle.get("artifact_size_bytes") == artifact_size,
        "frame bundle and Data Agent artifact sizes differ",
    )
    _integer(frame_bundle.get("frame_count"), "frame bundle frame_count", minimum=1)

    delivery = _mapping(semantic["delivery"], "Data Agent delivery")
    for field in (
        "telemetry_supported",
        "telemetry_complete",
        "exactly_one_full_download",
        "bytes_sent_equals_artifact_size",
    ):
        _literal(delivery.get(field), True, f"Data Agent delivery {field}")
    _require(
        delivery.get("artifact_size_bytes") == artifact_size
        and delivery.get("bytes_sent") == artifact_size,
        "Data Agent delivery bytes differ from the artifact",
    )
    _require(
        delivery.get("telemetry_object_id") == semantic["artifact_object_id"],
        "Data Agent telemetry object_id changed",
    )
    _integer(
        delivery.get("download_request_count"),
        "download request count",
        minimum=1,
    )
    _integer(delivery.get("full_download_count"), "full download count", minimum=1)
    for name, value in semantic["latency_ms"].items():
        _number_or_none(value, f"semantic latency_ms.{name}")
    _require(semantic["matrix_id"] == source["matrix_id"], "semantic matrix_id changed")
    _require(
        semantic["matrix_plan_sha256"] == source["plan_sha256"],
        "semantic evidence is bound to a different matrix plan",
    )
    trial_key = semantic["trial_key"]
    trial_by_key = _mapping(source.get("trial_by_key"), "matrix trials")
    result_by_key = _mapping(source.get("result_by_key"), "matrix results")
    _require(trial_key in trial_by_key, "semantic trial is absent from matrix plan")
    _require(trial_key in result_by_key, "semantic trial is absent from matrix run")
    trial = _mapping(trial_by_key[trial_key], "matrix trial")
    trial_result = _mapping(result_by_key[trial_key], "matrix trial result")

    expected_identity = {
        "workload_id": semantic["workload_id"],
        "workload_class": semantic["workload_class"],
        "design_id": semantic["matrix_design_id"],
        "repetition": semantic["repetition"],
    }
    for field, expected in expected_identity.items():
        _require(
            trial.get(field) == expected,
            f"matrix plan {field} differs from semantic evidence",
        )
        _require(
            trial_result.get(field) == expected,
            f"matrix run {field} differs from semantic evidence",
        )
    _require(
        trial.get("object_id") == semantic["matrix_object_id"],
        "matrix plan object differs from semantic matrix_object_id",
    )
    _require(
        trial.get("trial_id") == semantic["trial_id"]
        == trial_result.get("trial_id"),
        "matrix trial_id differs from semantic evidence",
    )
    _require(
        trial.get("executor_node_id") == semantic["matrix_executor_node_id"]
        == trial_result.get("executor_node_id"),
        "matrix executor identity differs from semantic evidence",
    )
    for field in _BINDING_KEYS:
        _require(
            semantic[field] == binding[field],
            f"binding {field} differs from Data Agent semantic evidence",
        )
    _require(trial_result.get("status") == "COMPLETE", "matrix trial did not complete")
    _literal(
        trial_result.get("semantic_task_quality_evaluated"),
        False,
        "matrix semantic_task_quality_evaluated",
    )
    _literal(
        trial_result.get("credentials_recorded"),
        False,
        "matrix trial credentials_recorded",
    )

    operations_by_trial = _mapping(
        source.get("operations_by_trial"), "matrix operations by trial"
    )
    results_by_trial = _mapping(
        source.get("operation_results_by_trial"),
        "matrix operation results by trial",
    )
    planned_operations = list(operations_by_trial[trial_key])
    operation_results = list(results_by_trial[trial_key])
    _require(bool(planned_operations), "matrix trial has no planned operations")
    _require(bool(operation_results), "matrix trial has no operation results")
    planned_keys = [
        _identifier(row.get("operation_key"), "planned operation key")
        for row in planned_operations
    ]
    result_keys = [
        _identifier(row.get("operation_key"), "operation result key")
        for row in operation_results
    ]
    _require(
        planned_keys == result_keys,
        "matrix operation result order or coverage changed",
    )
    executed = [row for row in operation_results if row.get("executed") is True]
    inactive = [row for row in operation_results if row.get("executed") is False]
    _require(
        len(executed) + len(inactive) == len(operation_results),
        "matrix operation state is invalid",
    )
    _require(
        all(row.get("telemetry_complete") is True for row in executed),
        "an executed matrix operation has incomplete telemetry",
    )
    _require(
        all(
            row.get("semantic_task_quality_evaluated") is False
            for row in operation_results
        ),
        "matrix operation unexpectedly carries semantic quality",
    )

    telemetry = _mapping(trial_result.get("telemetry"), "matrix trial telemetry")
    infrastructure = {
        "planned_operation_count": len(planned_operations),
        "executed_operation_count": len(executed),
        "inactive_operation_count": len(inactive),
        "planned_operation_keys": planned_keys,
        "executed_operation_keys": [row["operation_key"] for row in executed],
        "logical_bytes_sum": _integer(
            telemetry.get("logical_bytes_sum"), "matrix logical_bytes_sum"
        ),
        "physical_bytes_sum": _integer(
            telemetry.get("physical_bytes_sum"), "matrix physical_bytes_sum"
        ),
        "service_time_ms_sum": _number_or_none(
            telemetry.get("service_time_ms_sum"), "matrix service_time_ms_sum"
        ),
        "service_time_ms_sum_by_operation_kind": dict(
            _mapping(
                telemetry.get("service_time_ms_sum_by_operation_kind"),
                "matrix service time by operation kind",
            )
        ),
        "service_time_sum_is_end_to_end_latency": False,
        "queue_time_measured": False,
    }
    _literal(
        trial_result.get("service_time_sum_is_end_to_end_latency"),
        False,
        "matrix service-time interpretation",
    )
    _literal(
        trial_result.get("queue_time_measured"),
        False,
        "matrix queue-time interpretation",
    )

    semantic_evidence_sha256 = _digest(
        semantic_source.get("record_sha256"), "semantic record digest"
    )
    binding_sha256 = _sha256_bytes(
        _canonical_bytes(
            {
                "matrix_plan_sha256": source["plan_sha256"],
                "matrix_run_sha256": source["run_sha256"],
                "trial_key": trial_key,
                "matrix_object_id": semantic["matrix_object_id"],
                "artifact_object_id": semantic["artifact_object_id"],
                "semantic_spec_sha256": semantic["semantic_spec_sha256"],
                "event_index": semantic["event_index"],
                "expected_model": semantic["expected_model"],
                "data_agent_route_design_id": semantic[
                    "data_agent_route_design_id"
                ],
                "planned_operation_keys": planned_keys,
                "semantic_record_sha256": semantic_evidence_sha256,
                "artifact_sha256": artifact_sha256,
            }
        )
    )
    evidence_id = _sha256_bytes(
        _canonical_bytes([source["run_id"], trial_key, semantic_evidence_sha256])
    )
    return {
        "schema_version": PATHFINDER_EVIDENCE_RECORD_SCHEMA_VERSION,
        "evidence_id": evidence_id,
        "binding_sha256": binding_sha256,
        "trial_key": trial_key,
        "trial_id": _identifier(trial.get("trial_id"), "matrix trial_id"),
        "sequence_index": _integer(
            trial_result.get("sequence_index"), "matrix sequence_index"
        ),
        "workload_id": semantic["workload_id"],
        "workload_class": semantic["workload_class"],
        "matrix_object_id": semantic["matrix_object_id"],
        "artifact_object_id": semantic["artifact_object_id"],
        "matrix_design_id": semantic["matrix_design_id"],
        "data_agent_route_design_id": semantic["data_agent_route_design_id"],
        "repetition": semantic["repetition"],
        "matrix": {
            "matrix_id": source["matrix_id"],
            "matrix_plan_sha256": source["plan_sha256"],
            "matrix_run_id": source["run_id"],
            "matrix_run_sha256": source["run_sha256"],
            "trial_result_sha256": _sha256_bytes(_canonical_bytes(trial_result)),
            "operation_results_sha256": _sha256_bytes(
                _canonical_bytes(operation_results)
            ),
        },
        "association": {
            "binding_spec_sha256": source["spec_sha256"],
            "semantic_run_id": semantic["semantic_run_id"],
            "semantic_spec_sha256": semantic["semantic_spec_sha256"],
            "event_index": semantic["event_index"],
            "exact_trial_key_association_verified": True,
            "matrix_object_and_artifact_object_explicitly_bound": True,
            "execution_and_semantic_route_unified": False,
            "flowmesh_semantic_execution_verified": False,
            "host_to_container_ownership_mapping_verified": False,
            "interpretation": (
                "Posthoc cross-layer association under one explicitly bound "
                "trial key; "
                "not evidence that the synthetic infrastructure operation "
                "and semantic artifact traversed one physical byte path."
            ),
        },
        "data_agent": {
            "plan_id": semantic["data_agent_plan_id"],
            "plan_epoch": semantic["data_agent_plan_epoch"],
            "access_id": semantic["data_agent_access_id"],
            "endpoint_registry_id": semantic["endpoint_registry_id"],
            "endpoint_registry_sha256": semantic["endpoint_registry_sha256"],
            "destination_host_node_id": destination_host_node_id,
            "route": semantic["route"],
            "artifact": semantic["artifact"],
            "delivery": semantic["delivery"],
            "latency_ms": semantic["latency_ms"],
            "telemetry_complete": True,
        },
        "semantic": {
            "frame_bundle": semantic["frame_bundle"],
            "model": semantic["model"],
            "expected_model": semantic["expected_model"],
            "success_scoring_rule": semantic["success_scoring_rule"],
            "final_answer_sha256": semantic["final_answer_sha256"],
            "task_success": semantic["task_success"],
            "llm_service_time_ms": semantic["semantic_service_time_ms"],
            "container_node_id": semantic["semantic_executor_node_id"],
            "runtime_epoch": semantic["semantic_runtime_epoch"],
            "health_verified": True,
            "frame_payload_integrity_verified": True,
            "container_data_plane_artifact_delivery_verified": False,
            "response_consistency_verified": True,
            "container_runtime_code_provenance_verified": False,
            "llm_called": True,
            "telemetry_complete": True,
            "source_record_sha256": semantic_evidence_sha256,
            "source_manifest_sha256": _digest(
                semantic_source.get("manifest_sha256"),
                "semantic manifest digest",
            ),
        },
        "infrastructure": infrastructure,
        "cost": {
            "cost_basis": "unavailable",
            "cost_unit": None,
            "component_costs": None,
            "total_cost": None,
            "monetary_cost_measured": False,
            "simulated_cost_computed": False,
        },
        "source_vertical_status": _text(
            semantic_manifest.get("status"), "semantic manifest status"
        ),
        "eligible_for_awm_oed": False,
        "eligible_for_scientific_claims": False,
        "credentials_recorded": False,
    }


def _build_documents(source: Mapping[str, Any]) -> dict[str, bytes]:
    spec = _mapping(source.get("spec"), "Pathfinder evidence spec")
    binding_by_semantic_run = {
        row["semantic_run_id"]: row for row in spec["bindings"]
    }
    records = [
        _build_record(
            source,
            semantic_source,
            binding_by_semantic_run[
                _identifier(
                    semantic_source["record"].get("semantic_run_id"),
                    "semantic_run_id",
                )
            ],
        )
        for semantic_source in source["semantic_sources"]
    ]
    records.sort(key=lambda row: (row["sequence_index"], row["trial_key"]))
    trial_keys = [row["trial_key"] for row in records]
    access_ids = [row["data_agent"]["access_id"] for row in records]
    evidence_ids = [row["evidence_id"] for row in records]
    _require(
        len(trial_keys) == len(set(trial_keys)),
        "semantic trial evidence is duplicated",
    )
    _require(
        len(access_ids) == len(set(access_ids)),
        "Data Agent access evidence is reused",
    )
    _require(len(evidence_ids) == len(set(evidence_ids)), "evidence IDs are duplicated")

    records_bytes = _jsonl_bytes(records)
    manifest = {
        "schema_version": PATHFINDER_EVIDENCE_MANIFEST_SCHEMA_VERSION,
        "status": "COMPLETE_SIMULATOR_PATHFINDER_EVIDENCE",
        "evidence_class": "cross-layer-posthoc-association-conformance",
        "evidence_bundle_id": spec["evidence_bundle_id"],
        "binding_spec_sha256": source["spec_sha256"],
        "matrix_id": source["matrix_id"],
        "matrix_plan_sha256": source["plan_sha256"],
        "matrix_run_id": source["run_id"],
        "matrix_run_sha256": source["run_sha256"],
        "record_count": len(records),
        "trial_keys": trial_keys,
        "records_sha256": _sha256_bytes(records_bytes),
        "source_files_sha256": dict(source["source_files_sha256"]),
        "semantic_source_record_sha256": [
            row["semantic"]["source_record_sha256"] for row in records
        ],
        "semantic_spec_sha256": [
            row["association"]["semantic_spec_sha256"] for row in records
        ],
        "semantic_task_quality_evaluated": True,
        "data_agent_artifact_delivery_verified": True,
        "matrix_infrastructure_telemetry_verified": True,
        "container_semantic_response_consistency_verified": True,
        "container_runtime_code_provenance_verified": False,
        "execution_and_semantic_route_unified": False,
        "flowmesh_semantic_execution_verified": False,
        "host_to_container_ownership_mapping_verified": False,
        "cost_basis": "unavailable",
        "monetary_cost_measured": False,
        "simulated_cost_computed": False,
        "eligible_for_awm_oed": False,
        "eligible_for_scientific_claims": False,
        "credentials_recorded": False,
        "limitations": [
            "Component service-time sums are not end-to-end latency.",
            "Queue time is not measured by the matrix runner.",
            "No physical or simulated monetary cost is emitted.",
            "Semantic execution is not verified as a FlowMesh-scheduled path.",
            "The mutable container image is not bound to source-code provenance.",
            "No host-to-container ownership mapping is inferred.",
            "This bundle is not a distributed-pilot evaluation or an AWM/OED input.",
        ],
    }
    documents = {
        _RECORDS_FILE: records_bytes,
        _MANIFEST_FILE: _json_bytes(manifest),
    }
    documents["SHA256SUMS"] = _checksums(documents)
    return documents


def _write_atomic(target: Path, documents: Mapping[str, bytes]) -> None:
    _require(not target.exists(), "Pathfinder evidence output directory already exists")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    try:
        for name, content in documents.items():
            (staging / name).write_bytes(content)
        os.replace(staging, target)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def build_flowmesh_pathfinder_evidence(
    *,
    binding_spec: str | Path,
    matrix_plan_dir: str | Path,
    matrix_run_dir: str | Path,
    endpoint_registry: EndpointRegistry,
    data_agent_semantic_dirs: Sequence[str | Path],
    data_agent_semantic_specs: Sequence[str | Path],
    output_dir: str | Path,
) -> dict[str, Any]:
    """Build one immutable cross-layer simulator evidence bundle."""

    source = _load_sources(
        binding_spec=binding_spec,
        matrix_plan_dir=matrix_plan_dir,
        matrix_run_dir=matrix_run_dir,
        endpoint_registry=endpoint_registry,
        data_agent_semantic_dirs=data_agent_semantic_dirs,
        data_agent_semantic_specs=data_agent_semantic_specs,
    )
    target = Path(output_dir).resolve()
    documents = _build_documents(source)
    _write_atomic(target, documents)
    try:
        verified = verify_flowmesh_pathfinder_evidence(
            evidence_dir=target,
            binding_spec=binding_spec,
            matrix_plan_dir=matrix_plan_dir,
            matrix_run_dir=matrix_run_dir,
            endpoint_registry=endpoint_registry,
            data_agent_semantic_dirs=data_agent_semantic_dirs,
            data_agent_semantic_specs=data_agent_semantic_specs,
        )
    except Exception:
        shutil.rmtree(target, ignore_errors=True)
        raise
    return {**verified, "output_dir": str(target)}


def _verify_checksum_file(root: Path) -> None:
    checksum_path = root / "SHA256SUMS"
    _require(
        checksum_path.is_file() and not checksum_path.is_symlink(),
        "Pathfinder evidence SHA256SUMS is missing",
    )
    try:
        lines = checksum_path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FlowMeshPathfinderEvidenceError(
            "cannot read Pathfinder evidence SHA256SUMS"
        ) from exc
    observed: dict[str, str] = {}
    for line in lines:
        digest, separator, name = line.partition("  ")
        _require(
            separator == "  "
            and name in _OUTPUT_FILES
            and _HEX_SHA256.fullmatch(digest) is not None
            and name not in observed,
            "Pathfinder evidence SHA256SUMS contains an invalid row",
        )
        observed[name] = digest
    _require(
        set(observed) == _OUTPUT_FILES,
        "Pathfinder evidence checksums are incomplete",
    )
    for name, digest in observed.items():
        path = root / name
        _require(
            path.is_file()
            and not path.is_symlink()
            and _sha256_path(path, name) == digest,
            f"Pathfinder evidence checksum mismatch: {name}",
        )


def verify_flowmesh_pathfinder_evidence(
    *,
    evidence_dir: str | Path,
    binding_spec: str | Path,
    matrix_plan_dir: str | Path,
    matrix_run_dir: str | Path,
    endpoint_registry: EndpointRegistry,
    data_agent_semantic_dirs: Sequence[str | Path],
    data_agent_semantic_specs: Sequence[str | Path],
) -> dict[str, Any]:
    """Offline-verify the bundle and re-derive every cross-source join."""

    root = Path(evidence_dir).resolve()
    _require(
        root.is_dir() and not root.is_symlink(),
        "Pathfinder evidence directory is missing",
    )
    actual = {
        path.name
        for path in root.iterdir()
        if path.is_file() or path.is_symlink()
    }
    _require(
        actual == _OUTPUT_FILES | {"SHA256SUMS"},
        "Pathfinder evidence file set changed",
    )
    _verify_checksum_file(root)

    source = _load_sources(
        binding_spec=binding_spec,
        matrix_plan_dir=matrix_plan_dir,
        matrix_run_dir=matrix_run_dir,
        endpoint_registry=endpoint_registry,
        data_agent_semantic_dirs=data_agent_semantic_dirs,
        data_agent_semantic_specs=data_agent_semantic_specs,
    )
    expected = _build_documents(source)
    for name in _OUTPUT_FILES:
        _require(
            (root / name).read_bytes() == expected[name],
            f"Pathfinder evidence content does not match verified sources: {name}",
        )
    _, manifest = _read_json(root / _MANIFEST_FILE, "Pathfinder evidence manifest")
    _require(
        manifest.get("schema_version") == PATHFINDER_EVIDENCE_MANIFEST_SCHEMA_VERSION,
        "unsupported Pathfinder evidence manifest schema",
    )
    _require(
        manifest.get("status") == "COMPLETE_SIMULATOR_PATHFINDER_EVIDENCE",
        "Pathfinder evidence is incomplete",
    )
    return {
        "status": "VERIFIED",
        "evidence_class": manifest["evidence_class"],
        "matrix_id": manifest["matrix_id"],
        "matrix_run_id": manifest["matrix_run_id"],
        "record_count": manifest["record_count"],
        "semantic_task_quality_evaluated": True,
        "data_agent_artifact_delivery_verified": True,
        "matrix_infrastructure_telemetry_verified": True,
        "container_semantic_response_consistency_verified": True,
        "container_runtime_code_provenance_verified": False,
        "cost_basis": "unavailable",
        "monetary_cost_measured": False,
        "eligible_for_awm_oed": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "FlowMeshPathfinderEvidenceError",
    "PATHFINDER_EVIDENCE_MANIFEST_SCHEMA_VERSION",
    "PATHFINDER_EVIDENCE_RECORD_SCHEMA_VERSION",
    "PATHFINDER_EVIDENCE_BINDING_REQUIRED_KEYS",
    "PATHFINDER_EVIDENCE_SPEC_REQUIRED_KEYS",
    "PATHFINDER_EVIDENCE_SPEC_SCHEMA_VERSION",
    "build_flowmesh_pathfinder_evidence",
    "verify_flowmesh_pathfinder_evidence",
]
