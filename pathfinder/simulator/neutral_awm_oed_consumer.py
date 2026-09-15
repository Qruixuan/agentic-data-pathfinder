"""Offline consumer for authenticated simulator-neutral observations.

The legacy AWM certificate treats a cost scalar as part of every response.
Simulator-neutral observations intentionally do not manufacture that scalar.
This module therefore exposes two sharply separated paths:

* every complete 4 x 8 x 2 package can produce a descriptive
  quality/infrastructure policy and a prospective *collection* priority; and
* the existing weighted certificate core is called only when the bridge
  package contains a separately frozen external-real-cost manifest and the
  caller supplies a predeclared support for cost savings.

Neither path turns a post-hoc, single-host simulator run into scientific or
commit evidence.  The output keeps that boundary machine-readable.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import tempfile
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from statistics import mean
from typing import Any

from ..awm.model import Interval
from ..distributed.weighted_certificate import (
    StratumEvidence,
    evaluate_weighted_policy_certificate,
)
from .policy_oed_bridge import (
    CHECKSUMS_NAME as SOURCE_CHECKSUMS_NAME,
    EXTERNAL_COST_NAME,
    EXTERNAL_REAL_COST_MANIFEST_SCHEMA_VERSION,
    NEUTRAL_OBSERVATION_MANIFEST_SCHEMA_VERSION,
    NEUTRAL_OBSERVATION_SCHEMA_VERSION,
    OBSERVATION_MANIFEST_NAME,
    OBSERVATIONS_NAME,
)


MANIFEST_SCHEMA_VERSION = (
    "pathfinder.simulator-neutral-awm-oed-manifest/v1alpha1"
)
DATASET_SCHEMA_VERSION = "pathfinder.simulator-neutral-awm-dataset/v1alpha1"
DATASET_ROW_SCHEMA_VERSION = (
    "pathfinder.simulator-neutral-awm-dataset-row/v1alpha1"
)
EVALUATION_SCHEMA_VERSION = (
    "pathfinder.simulator-neutral-awm-evaluation/v1alpha1"
)
OED_SELECTION_SCHEMA_VERSION = (
    "pathfinder.simulator-neutral-oed-selection/v1alpha1"
)
OED_ROW_SCHEMA_VERSION = (
    "pathfinder.simulator-neutral-oed-selected-pair/v1alpha1"
)

MANIFEST_NAME = "neutral-awm-oed-manifest.json"
DATASET_NAME = "neutral-awm-dataset.jsonl"
EVALUATION_NAME = "neutral-awm-evaluation.json"
OED_SELECTION_NAME = "neutral-oed-selection.jsonl"
CHECKSUMS_NAME = "SHA256SUMS"

_WORKLOAD_CLASSES = tuple(f"W{index}" for index in range(1, 5))
_DESIGN_IDS = tuple(f"D{index}" for index in range(8))
_REPETITIONS = (0, 1)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,255}\Z")
_CURRENCY = re.compile(r"[A-Z]{3}\Z")

_PRIVATE_OR_CREDENTIAL_KEYS = {
    "api_key",
    "apikey",
    "authorization",
    "bearer_token",
    "password",
    "secret",
    "access_token",
    "correct_answer",
    "correct_answer_id",
    "hidden_answer",
    "hidden_label",
    "hidden_labels",
    "label_values",
    "relevance_values",
}

_GENERIC_OBSERVATION_FIELDS = {
    "schema_version",
    "trial_key",
    "source_full_flow_evidence_sha256",
    "source_full_flow_evidence_schema_version",
    "source_route_row_sha256",
    "source_semantic_admission_sha256",
    "source_bound_trial_sha256",
    "source_stage_dag_sha256",
    "source_route_evidence_commitment_sha256",
    "source_order_index",
    "workload_id",
    "workload_class",
    "design_id",
    "repetition",
    "object_id",
    "route_family",
    "executor_node_id",
    "cache_branch",
    "task_success",
    "score_authenticity_verified",
    "score_authentication",
    "latency_measurements_ms",
    "latency_measurement_semantics",
    "end_to_end_latency_available",
    "byte_measurements",
    "byte_measurement_semantics",
    "monetary_cost_available",
    "monetary_cost",
    "synthetic_simulator_cost_hints_consumed",
    "performance_evidence_claimed",
    "scientific_evidence_claimed",
    "hidden_label_values_included",
    "hidden_label_values_consumed_by_bridge",
    "credentials_recorded",
}

_SOURCE_MANIFEST_REQUIRED_FIELDS = {
    "schema_version",
    "status",
    "observation_set_id",
    "logical_route_plan_sha256",
    "scenario_id",
    "observation_count",
    "legacy_full_flow_observation_count",
    "generic_semantic_route_observation_count",
    "semantic_matrix_run_integrity_verified",
    "semantic_execution_admission_sha256",
    "observations_file_sha256",
    "task_success_available",
    "all_score_authenticity_verified",
    "hidden_v2_score_authentication_required",
    "component_latency_available",
    "end_to_end_latency_available",
    "measured_bytes_available",
    "monetary_cost_available",
    "external_real_cost_manifest_sha256",
    "external_real_cost_file_sha256",
    "external_cost_claim_independently_verified",
    "synthetic_simulator_cost_hints_consumed",
    "hidden_label_values_included",
    "hidden_label_values_consumed_by_bridge",
    "component_measurements_are_performance_claims",
    "performance_analysis_performed",
    "statistical_analysis_performed",
    "awm_oed_mathematics_modified",
    "endpoint_free",
    "credentials_recorded",
    "eligible_for_scientific_claims",
    "observation_manifest_sha256",
}


class NeutralAwmOedConsumerError(ValueError):
    """Raised when neutral evidence cannot be consumed without overclaiming."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise NeutralAwmOedConsumerError(message)


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise NeutralAwmOedConsumerError(
            "value is not canonical JSON"
        ) from exc


def _json_document(value: Any) -> bytes:
    return _canonical(value) + b"\n"


def _jsonl_document(rows: Sequence[Mapping[str, Any]]) -> bytes:
    return b"".join(_canonical(row) + b"\n" for row in rows)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _read_json_bytes(payload: bytes, name: str) -> dict[str, Any]:
    try:
        result = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_pairs,
            parse_constant=lambda value: (_ for _ in ()).throw(
                NeutralAwmOedConsumerError(f"{name} contains {value}")
            ),
        )
    except NeutralAwmOedConsumerError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise NeutralAwmOedConsumerError(
            f"{name} is not valid JSON"
        ) from exc
    _require(isinstance(result, dict), f"{name} must be an object")
    return result


def _read_json(path: Path, name: str) -> dict[str, Any]:
    try:
        return _read_json_bytes(path.read_bytes(), name)
    except OSError as exc:
        raise NeutralAwmOedConsumerError(f"cannot read {name}") from exc


def _read_jsonl(path: Path, name: str) -> list[dict[str, Any]]:
    try:
        lines = path.read_bytes().splitlines()
    except OSError as exc:
        raise NeutralAwmOedConsumerError(f"cannot read {name}") from exc
    _require(bool(lines), f"{name} cannot be empty")
    return [
        _read_json_bytes(line, f"{name} line {index}")
        for index, line in enumerate(lines, start=1)
    ]


def _digest(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{name} must be a lowercase SHA-256 digest",
    )
    return value


def _identifier(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _SAFE_ID.fullmatch(value) is not None,
        f"{name} is invalid",
    )
    return value


def _number(value: Any, name: str, *, minimum: float = 0.0) -> float:
    _require(
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        and float(value) >= minimum,
        f"{name} must be a finite number >= {minimum}",
    )
    return float(value)


def _walk_public(value: Any, path: str = "$") -> None:
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key).casefold().replace("-", "_")
            _require(
                key not in _PRIVATE_OR_CREDENTIAL_KEYS,
                f"{path}.{raw_key} crosses the hidden/credential boundary",
            )
            _walk_public(child, f"{path}.{raw_key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _walk_public(child, f"{path}[{index}]")
    elif isinstance(value, str):
        _require(
            "bearer " not in value.casefold(),
            f"{path} contains credential material",
        )


def _verify_checksum_file(root: Path, expected_content: set[str]) -> None:
    expected_files = expected_content | {SOURCE_CHECKSUMS_NAME}
    _require(root.is_dir(), "neutral observation directory does not exist")
    entries = list(root.iterdir())
    _require(
        all(path.is_file() and not path.is_symlink() for path in entries),
        "neutral observation package must contain regular files only",
    )
    _require(
        {path.name for path in entries} == expected_files,
        "neutral observation package file set changed",
    )
    try:
        lines = (root / SOURCE_CHECKSUMS_NAME).read_text(
            encoding="utf-8"
        ).splitlines()
    except (OSError, UnicodeError) as exc:
        raise NeutralAwmOedConsumerError(
            "cannot read neutral observation SHA256SUMS"
        ) from exc
    ordered = sorted(expected_content)
    _require(len(lines) == len(ordered), "source checksums are incomplete")
    for line, expected_name in zip(lines, ordered, strict=True):
        digest, separator, name = line.partition("  ")
        _require(
            separator == "  "
            and name == expected_name
            and _SHA256.fullmatch(digest) is not None,
            "source checksum line is not canonical",
        )
        _require(
            _sha256((root / name).read_bytes()) == digest,
            f"source checksum mismatch: {name}",
        )


def _manifest_digest(manifest: Mapping[str, Any], field: str) -> str:
    unsigned = dict(manifest)
    recorded = _digest(unsigned.pop(field, None), field)
    _require(recorded == _sha256(_canonical(unsigned)), f"{field} mismatch")
    return recorded


def _validate_external_cost_file(
    value: Mapping[str, Any],
    *,
    source_manifest: Mapping[str, Any],
    trial_keys: set[str],
) -> dict[str, Any]:
    expected_fields = {
        "schema_version",
        "status",
        "calibration_id",
        "calibration_evidence_sha256",
        "logical_route_plan_sha256",
        "currency",
        "entries",
        "external_calibration",
        "synthetic_simulator_inputs_used",
        "credentials_recorded",
        "eligible_for_scientific_claims",
        "manifest_sha256",
    }
    _require(set(value) == expected_fields, "external cost fields changed")
    _require(
        value["schema_version"]
        == EXTERNAL_REAL_COST_MANIFEST_SCHEMA_VERSION
        and value["status"] == "FROZEN_EXTERNALLY_CALIBRATED_REAL_COSTS",
        "external cost schema or status changed",
    )
    _identifier(value["calibration_id"], "calibration_id")
    _digest(
        value["calibration_evidence_sha256"],
        "calibration_evidence_sha256",
    )
    _require(
        value["logical_route_plan_sha256"]
        == source_manifest["logical_route_plan_sha256"],
        "external cost route-plan binding changed",
    )
    currency = value["currency"]
    _require(
        isinstance(currency, str) and _CURRENCY.fullmatch(currency),
        "external cost currency is invalid",
    )
    _require(
        value["external_calibration"] is True
        and value["synthetic_simulator_inputs_used"] is False
        and value["credentials_recorded"] is False
        and value["eligible_for_scientific_claims"] is False,
        "external cost provenance flags changed",
    )
    entries = value["entries"]
    _require(isinstance(entries, list), "external cost entries must be an array")
    observed: set[str] = set()
    for entry in entries:
        _require(
            isinstance(entry, dict)
            and set(entry) == {"trial_key", "amount", "measurement_sha256"},
            "external cost entry fields changed",
        )
        trial_key = entry["trial_key"]
        _require(
            isinstance(trial_key, str)
            and trial_key in trial_keys
            and trial_key not in observed,
            "external cost trial binding is invalid",
        )
        observed.add(trial_key)
        _number(entry["amount"], "external cost amount")
        _digest(entry["measurement_sha256"], "cost measurement SHA-256")
    _require(observed == trial_keys, "external cost coverage is incomplete")
    _require(
        [entry["trial_key"] for entry in entries] == sorted(trial_keys),
        "external cost entries are not canonical",
    )
    _manifest_digest(value, "manifest_sha256")
    _walk_public(value)
    return dict(value)


def _load_verified_source(
    observation_dir: str | Path,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    root = Path(observation_dir).resolve()
    manifest = _read_json(
        root / OBSERVATION_MANIFEST_NAME,
        "neutral observation manifest",
    )
    _require(
        _SOURCE_MANIFEST_REQUIRED_FIELDS <= set(manifest),
        "neutral observation manifest fields are incomplete",
    )
    _require(
        manifest["schema_version"]
        == NEUTRAL_OBSERVATION_MANIFEST_SCHEMA_VERSION
        and manifest["status"] == "FROZEN_NEUTRAL_FULL_FLOW_OBSERVATIONS",
        "neutral observation manifest schema or status changed",
    )
    monetary = manifest["monetary_cost_available"]
    _require(type(monetary) is bool, "monetary_cost_available is invalid")
    expected = {OBSERVATION_MANIFEST_NAME, OBSERVATIONS_NAME}
    if monetary:
        expected.add(EXTERNAL_COST_NAME)
    _verify_checksum_file(root, expected)
    _manifest_digest(manifest, "observation_manifest_sha256")
    rows = _read_jsonl(root / OBSERVATIONS_NAME, "neutral observations")
    _require(
        (root / OBSERVATION_MANIFEST_NAME).read_bytes()
        == _json_document(manifest)
        and (root / OBSERVATIONS_NAME).read_bytes()
        == _jsonl_document(rows),
        "neutral observation package is not canonically encoded",
    )
    _require(
        len(rows) == 64
        and manifest["observation_count"] == 64
        and manifest["observations_file_sha256"]
        == _sha256((root / OBSERVATIONS_NAME).read_bytes()),
        "neutral observation package is not a complete 4x8x2 matrix",
    )
    required_true = (
        "task_success_available",
        "all_score_authenticity_verified",
        "hidden_v2_score_authentication_required",
        "component_latency_available",
        "measured_bytes_available",
        "endpoint_free",
    )
    required_false = (
        "end_to_end_latency_available",
        "synthetic_simulator_cost_hints_consumed",
        "hidden_label_values_included",
        "hidden_label_values_consumed_by_bridge",
        "component_measurements_are_performance_claims",
        "performance_analysis_performed",
        "statistical_analysis_performed",
        "awm_oed_mathematics_modified",
        "credentials_recorded",
        "eligible_for_scientific_claims",
    )
    _require(
        all(manifest.get(field) is True for field in required_true)
        and all(manifest.get(field) is False for field in required_false),
        "neutral observation provenance/authentication flags changed",
    )
    _require(
        manifest["legacy_full_flow_observation_count"] == 0
        and manifest["generic_semantic_route_observation_count"] == 64
        and manifest["semantic_matrix_run_integrity_verified"] is True
        and manifest["external_cost_claim_independently_verified"] is False,
        "neutral observation source is not the authenticated generic matrix",
    )
    _digest(
        manifest["semantic_execution_admission_sha256"],
        "semantic_execution_admission_sha256",
    )
    _digest(
        manifest["logical_route_plan_sha256"],
        "logical_route_plan_sha256",
    )
    _identifier(manifest["observation_set_id"], "observation_set_id")
    _identifier(manifest["scenario_id"], "scenario_id")

    cells: set[tuple[str, str, int]] = set()
    trial_keys: set[str] = set()
    order_indexes: set[int] = set()
    workload_ids: dict[str, str] = {}
    object_ids: dict[str, str] = {}
    for row in rows:
        _require(
            set(row) == _GENERIC_OBSERVATION_FIELDS,
            "neutral observation fields changed",
        )
        _require(
            row["schema_version"] == NEUTRAL_OBSERVATION_SCHEMA_VERSION,
            "neutral observation schema changed",
        )
        workload_class = row["workload_class"]
        design_id = row["design_id"]
        repetition = row["repetition"]
        _require(
            workload_class in _WORKLOAD_CLASSES
            and design_id in _DESIGN_IDS
            and repetition in _REPETITIONS,
            "neutral observation matrix coordinates are invalid",
        )
        cell = (workload_class, design_id, repetition)
        _require(cell not in cells, "duplicate neutral observation cell")
        cells.add(cell)
        trial_key = row["trial_key"]
        _require(
            isinstance(trial_key, str)
            and bool(trial_key)
            and trial_key not in trial_keys,
            "neutral observation trial key is invalid or duplicated",
        )
        trial_keys.add(trial_key)
        order_index = row["source_order_index"]
        _require(
            isinstance(order_index, int)
            and not isinstance(order_index, bool)
            and 0 <= order_index < 64
            and order_index not in order_indexes,
            "neutral observation order index is invalid or duplicated",
        )
        order_indexes.add(order_index)
        for field in (
            "source_full_flow_evidence_sha256",
            "source_route_row_sha256",
            "source_semantic_admission_sha256",
            "source_bound_trial_sha256",
            "source_stage_dag_sha256",
            "source_route_evidence_commitment_sha256",
        ):
            _digest(row[field], field)
        for field in (
            "workload_id",
            "object_id",
            "route_family",
            "executor_node_id",
        ):
            _identifier(row[field], field)
        previous_workload = workload_ids.setdefault(
            workload_class, row["workload_id"]
        )
        _require(
            previous_workload == row["workload_id"],
            "workload class maps to multiple independent workload IDs",
        )
        previous_object = object_ids.setdefault(workload_class, row["object_id"])
        _require(
            previous_object == row["object_id"],
            "workload class maps to multiple object IDs",
        )
        _require(
            type(row["task_success"]) is bool
            and row["score_authenticity_verified"] is True
            and row["score_authentication"]
            in {"n1-hmac-verified", "n1-privileged-offline-hmac-verification"},
            "neutral observation lacks authenticated N1 scoring",
        )
        _require(
            row["end_to_end_latency_available"] is False
            and row["monetary_cost_available"] is monetary
            and row["synthetic_simulator_cost_hints_consumed"] is False
            and row["performance_evidence_claimed"] is False
            and row["scientific_evidence_claimed"] is False
            and row["hidden_label_values_included"] is False
            and row["hidden_label_values_consumed_by_bridge"] is False
            and row["credentials_recorded"] is False,
            "neutral observation claim-boundary flags changed",
        )
        latency = row["latency_measurements_ms"]
        byte_values = row["byte_measurements"]
        _require(
            isinstance(latency, dict)
            and bool(latency)
            and isinstance(byte_values, dict)
            and bool(byte_values),
            "neutral component telemetry is missing",
        )
        for key, value in latency.items():
            _identifier(key, "latency component")
            if value is not None:
                _number(value, f"latency {key}")
        for key, value in byte_values.items():
            _identifier(key, "byte component")
            _require(
                isinstance(value, int)
                and not isinstance(value, bool)
                and value >= 0,
                f"byte component {key} is invalid",
            )
        cost = row["monetary_cost"]
        if monetary:
            _require(
                isinstance(cost, dict)
                and set(cost) == {"amount", "currency", "measurement_sha256"},
                "neutral monetary measurement fields changed",
            )
            _number(cost["amount"], "monetary amount")
            _require(
                isinstance(cost["currency"], str)
                and _CURRENCY.fullmatch(cost["currency"]),
                "monetary currency is invalid",
            )
            _digest(cost["measurement_sha256"], "cost measurement SHA-256")
        else:
            _require(cost is None, "cost appeared without external calibration")
        _walk_public(row)
    expected_cells = {
        (workload_class, design_id, repetition)
        for workload_class in _WORKLOAD_CLASSES
        for design_id in _DESIGN_IDS
        for repetition in _REPETITIONS
    }
    _require(cells == expected_cells, "neutral 4x8x2 cell coverage is incomplete")
    _require(order_indexes == set(range(64)), "neutral order indexes are incomplete")
    if monetary:
        cost_value = _read_json(root / EXTERNAL_COST_NAME, "external cost")
        _validate_external_cost_file(
            cost_value,
            source_manifest=manifest,
            trial_keys=trial_keys,
        )
        cost_by_trial = {
            entry["trial_key"]: entry for entry in cost_value["entries"]
        }
        _require(
            all(
                row["monetary_cost"]
                == {
                    "amount": cost_by_trial[row["trial_key"]]["amount"],
                    "currency": cost_value["currency"],
                    "measurement_sha256": cost_by_trial[row["trial_key"]][
                        "measurement_sha256"
                    ],
                }
                for row in rows
            ),
            "neutral observation cost differs from external cost manifest",
        )
        _require(
            manifest["external_real_cost_manifest_sha256"]
            == cost_value["manifest_sha256"]
            and manifest["external_real_cost_file_sha256"]
            == _sha256((root / EXTERNAL_COST_NAME).read_bytes()),
            "external cost manifest binding changed",
        )
    else:
        _require(
            manifest["external_real_cost_manifest_sha256"] is None
            and manifest["external_real_cost_file_sha256"] is None,
            "absent external cost has non-null source bindings",
        )
    _walk_public(manifest)
    rows.sort(key=lambda row: row["source_order_index"])
    return manifest, rows


def _dataset_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        latency = {
            key: value
            for key, value in sorted(row["latency_measurements_ms"].items())
        }
        byte_values = {
            key: value for key, value in sorted(row["byte_measurements"].items())
        }
        result.append({
            "schema_version": DATASET_ROW_SCHEMA_VERSION,
            "source_observation_sha256": _sha256(_canonical(row)),
            "trial_key": row["trial_key"],
            "source_order_index": row["source_order_index"],
            "workload_id": row["workload_id"],
            "workload_class": row["workload_class"],
            "design_id": row["design_id"],
            "repetition": row["repetition"],
            "object_id": row["object_id"],
            "route_family": row["route_family"],
            "executor_node_id": row["executor_node_id"],
            "cache_branch": row["cache_branch"],
            "task_success": row["task_success"],
            "score_authenticity_verified": True,
            "component_service_time_ms": latency,
            "component_service_time_ms_sum": sum(
                value for value in latency.values() if value is not None
            ),
            "byte_measurements": byte_values,
            "byte_measurement_sum": sum(byte_values.values()),
            "monetary_cost": row["monetary_cost"],
            "monetary_cost_available": row["monetary_cost_available"],
            "latency_is_end_to_end": False,
            "network_throughput_derived": False,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        })
    return result


def _summaries(
    dataset: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[tuple[str, str], list[Mapping[str, Any]]]]:
    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in dataset:
        grouped[(row["workload_class"], row["design_id"])].append(row)
    summaries: list[dict[str, Any]] = []
    for workload_class in _WORKLOAD_CLASSES:
        for design_id in _DESIGN_IDS:
            block = sorted(
                grouped[(workload_class, design_id)],
                key=lambda row: row["repetition"],
            )
            _require(len(block) == 2, "dataset cell lost a repetition")
            costs = [
                row["monetary_cost"]["amount"]
                for row in block
                if row["monetary_cost"] is not None
            ]
            summaries.append({
                "workload_class": workload_class,
                "workload_id": block[0]["workload_id"],
                "design_id": design_id,
                "repetitions": [0, 1],
                "task_success_rate": mean(
                    float(row["task_success"]) for row in block
                ),
                "task_success_repetition_disagreement": (
                    block[0]["task_success"] != block[1]["task_success"]
                ),
                "component_service_time_ms_mean": mean(
                    row["component_service_time_ms_sum"] for row in block
                ),
                "byte_measurement_sum_mean": mean(
                    row["byte_measurement_sum"] for row in block
                ),
                "monetary_cost_mean": mean(costs) if costs else None,
            })
    return summaries, grouped


def _preferred_designs(
    summaries: Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    selected: dict[str, str] = {}
    for workload_class in _WORKLOAD_CLASSES:
        candidates = [
            row for row in summaries if row["workload_class"] == workload_class
        ]
        preferred = min(
            candidates,
            key=lambda row: (
                -row["task_success_rate"],
                row["component_service_time_ms_mean"],
                row["byte_measurement_sum_mean"],
                _DESIGN_IDS.index(row["design_id"]),
            ),
        )
        selected[workload_class] = preferred["design_id"]
    return selected


def _paired_values(
    grouped: Mapping[tuple[str, str], Sequence[Mapping[str, Any]]],
    workload_class: str,
    candidate: str,
    baseline: str,
) -> tuple[float, float | None]:
    candidate_rows = grouped[(workload_class, candidate)]
    baseline_rows = grouped[(workload_class, baseline)]
    success = mean(
        float(candidate_row["task_success"])
        - float(baseline_row["task_success"])
        for candidate_row, baseline_row in zip(
            candidate_rows, baseline_rows, strict=True
        )
    )
    if candidate_rows[0]["monetary_cost"] is None:
        return success, None
    cost = mean(
        baseline_row["monetary_cost"]["amount"]
        - candidate_row["monetary_cost"]["amount"]
        for candidate_row, baseline_row in zip(
            candidate_rows, baseline_rows, strict=True
        )
    )
    return success, cost


def _evaluation(
    *,
    analysis_id: str,
    source_manifest: Mapping[str, Any],
    dataset: Sequence[Mapping[str, Any]],
    baseline_design_id: str,
    alpha: float,
    delta_success_margin: float,
    minimum_cost_saving: float,
    cost_saving_support: tuple[float, float] | None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    summaries, grouped = _summaries(dataset)
    selected = _preferred_designs(summaries)
    monetary = source_manifest["monetary_cost_available"]
    comparisons: list[dict[str, Any]] = []
    for workload_class in _WORKLOAD_CLASSES:
        candidate = selected[workload_class]
        success, cost = _paired_values(
            grouped, workload_class, candidate, baseline_design_id
        )
        baseline_summary = next(
            row
            for row in summaries
            if row["workload_class"] == workload_class
            and row["design_id"] == baseline_design_id
        )
        candidate_summary = next(
            row
            for row in summaries
            if row["workload_class"] == workload_class
            and row["design_id"] == candidate
        )
        comparisons.append({
            "workload_class": workload_class,
            "workload_id": candidate_summary["workload_id"],
            "baseline_design_id": baseline_design_id,
            "candidate_design_id": candidate,
            "candidate_selected_posthoc": True,
            "success_difference": success,
            "component_service_time_reduction_ms": (
                baseline_summary["component_service_time_ms_mean"]
                - candidate_summary["component_service_time_ms_mean"]
            ),
            "byte_measurement_reduction": (
                baseline_summary["byte_measurement_sum_mean"]
                - candidate_summary["byte_measurement_sum_mean"]
            ),
            "real_cost_saving": cost,
        })

    weighted_certificate: dict[str, Any] | None = None
    if monetary:
        _require(
            cost_saving_support is not None,
            "external real cost requires a predeclared cost_saving_support",
        )
        lower, upper = cost_saving_support
        _require(
            math.isfinite(lower) and math.isfinite(upper) and lower < upper,
            "cost_saving_support must be a finite increasing pair",
        )
        cost_values = [
            comparison["real_cost_saving"]
            for comparison in comparisons
            if comparison["candidate_design_id"] != baseline_design_id
        ]
        _require(
            all(lower <= value <= upper for value in cost_values),
            "observed real cost saving falls outside predeclared support",
        )
        strata: list[StratumEvidence] = []
        minima: dict[str, int] = {}
        for comparison in comparisons:
            workload_class = comparison["workload_class"]
            if comparison["candidate_design_id"] == baseline_design_id:
                strata.append(StratumEvidence(
                    stratum_id=workload_class,
                    role="structural_safe",
                    integer_weight=1,
                ))
            else:
                strata.append(StratumEvidence(
                    stratum_id=workload_class,
                    role="active",
                    integer_weight=1,
                    success_differences=(comparison["success_difference"],),
                    cost_savings=(comparison["real_cost_saving"],),
                ))
                minima[workload_class] = 1
        if minima:
            weighted_certificate = evaluate_weighted_policy_certificate(
                strata,
                success_difference_support=Interval(-1.0, 1.0),
                cost_saving_support=Interval(lower, upper),
                alpha=alpha,
                delta_success_margin=delta_success_margin,
                minimum_cost_saving=minimum_cost_saving,
                minimum_independent_workloads_by_stratum=minima,
                safe_design_id=baseline_design_id,
                policy_id=f"{analysis_id}-posthoc-policy",
            )

    if not monetary:
        certificate_state = "NOT_EVALUATED_MISSING_EXTERNAL_REAL_COST"
    elif weighted_certificate is None:
        certificate_state = "NO_NON_BASELINE_POLICY_EFFECT"
    else:
        certificate_state = weighted_certificate["certificate_state"]
    evaluation = {
        "schema_version": EVALUATION_SCHEMA_VERSION,
        "status": "COMPLETE",
        "analysis_id": analysis_id,
        "source_observation_set_id": source_manifest["observation_set_id"],
        "source_observation_manifest_sha256": source_manifest[
            "observation_manifest_sha256"
        ],
        "matrix_dimensions": {
            "workload_classes": list(_WORKLOAD_CLASSES),
            "design_ids": list(_DESIGN_IDS),
            "repetitions": list(_REPETITIONS),
            "trial_count": 64,
        },
        "baseline_design_id": baseline_design_id,
        "selection_kind": "posthoc-quality-infrastructure-heuristic",
        "selection_rule": (
            "max authenticated task success; then min component-service-time "
            "sum; then min byte-accounting sum; then design ID"
        ),
        "component_latency_is_end_to_end": False,
        "component_latency_is_real_performance_evidence": False,
        "byte_measurements_are_network_throughput": False,
        "policy_assignments": [
            {"workload_class": workload_class, "design_id": selected[workload_class]}
            for workload_class in _WORKLOAD_CLASSES
        ],
        "cell_summaries": summaries,
        "baseline_comparisons": comparisons,
        "monetary_cost_available": monetary,
        "cost_source": (
            "external-real-cost-manifest"
            if monetary
            else "absent-no-simulator-cost-invented"
        ),
        "weighted_certificate_evaluated": weighted_certificate is not None,
        "weighted_certificate": weighted_certificate,
        "mathematical_certificate_state": certificate_state,
        "decision_state": "POSTHOC_SIMULATOR_ANALYSIS_ONLY",
        "commit_authorized": False,
        "prospective_confirmation_required": True,
        "real_multinode_validation_required": True,
        "posthoc": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
        "limitations": [
            "the policy was selected and evaluated on the same 64 trials",
            "the two repetitions are not independent workload clusters",
            (
                "the legacy OED allocator was not run because it requires a "
                "monetary paired-block objective; this output only freezes "
                "fresh-workload collection priorities"
            ),
            "component service-time sums are not end-to-end latency",
            "byte accounting is not measured network throughput",
            (
                "no monetary certificate was evaluated because no external "
                "real-cost manifest was present"
                if not monetary
                else (
                    "external costs are structurally bound but not "
                    "independently verified"
                )
            ),
        ],
    }
    return evaluation, comparisons


def _oed_rows(
    comparisons: Sequence[Mapping[str, Any]],
    dataset: Sequence[Mapping[str, Any]],
    *,
    baseline_design_id: str,
    selection_size: int,
) -> list[dict[str, Any]]:
    by_cell: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in dataset:
        by_cell[(row["workload_class"], row["design_id"])].append(row)
    candidates: list[dict[str, Any]] = []
    for comparison in comparisons:
        workload_class = comparison["workload_class"]
        candidate = comparison["candidate_design_id"]
        if candidate == baseline_design_id:
            alternatives = sorted(
                (
                    design_id
                    for design_id in _DESIGN_IDS
                    if design_id != baseline_design_id
                ),
                key=lambda design_id: (
                    -mean(
                        float(row["task_success"])
                        for row in by_cell[(workload_class, design_id)]
                    ),
                    design_id,
                ),
            )
            candidate = alternatives[0]
        baseline_rows = sorted(
            by_cell[(workload_class, baseline_design_id)],
            key=lambda row: row["repetition"],
        )
        candidate_rows = sorted(
            by_cell[(workload_class, candidate)],
            key=lambda row: row["repetition"],
        )
        disagreement = int(
            baseline_rows[0]["task_success"]
            != baseline_rows[1]["task_success"]
        ) + int(
            candidate_rows[0]["task_success"]
            != candidate_rows[1]["task_success"]
        )
        success_delta = mean(
            float(candidate_row["task_success"])
            - float(baseline_row["task_success"])
            for candidate_row, baseline_row in zip(
                candidate_rows, baseline_rows, strict=True
            )
        )
        candidates.append({
            "workload_class": workload_class,
            "workload_id": candidate_rows[0]["workload_id"],
            "baseline_design_id": baseline_design_id,
            "candidate_design_id": candidate,
            "repetition_disagreement_count": disagreement,
            "absolute_observed_success_difference": abs(success_delta),
        })
    candidates.sort(key=lambda row: (
        -row["repetition_disagreement_count"],
        row["absolute_observed_success_difference"],
        _WORKLOAD_CLASSES.index(row["workload_class"]),
        _DESIGN_IDS.index(row["candidate_design_id"]),
    ))
    selected = candidates[:selection_size]
    return [
        {
            "schema_version": OED_ROW_SCHEMA_VERSION,
            "selection_index": index,
            **row,
            "action": "COLLECT_FRESH_INDEPENDENT_WORKLOAD_PAIR",
            "action_unit": "one-new-workload-safe-candidate-pair",
            "planned_repetitions_per_arm": 2,
            "pilot_repetitions_count_as_independent_workloads": False,
            "outcomes_for_fresh_workload_consumed": False,
            "selection_basis": "posthoc-quality-ambiguity-planning-only",
            "commit_authorized": False,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }
        for index, row in enumerate(selected)
    ]


def _documents(
    *,
    observation_dir: str | Path,
    analysis_id: str,
    baseline_design_id: str,
    oed_selection_size: int,
    require_real_cost: bool,
    alpha: float,
    delta_success_margin: float,
    minimum_cost_saving: float,
    cost_saving_support: tuple[float, float] | None,
) -> dict[str, bytes]:
    analysis_id = _identifier(analysis_id, "analysis_id")
    _require(baseline_design_id in _DESIGN_IDS, "baseline design is invalid")
    _require(
        isinstance(oed_selection_size, int)
        and not isinstance(oed_selection_size, bool)
        and 1 <= oed_selection_size <= 4,
        "oed_selection_size must be an integer from 1 to 4",
    )
    _require(
        isinstance(require_real_cost, bool),
        "require_real_cost must be boolean",
    )
    _require(
        isinstance(alpha, (int, float))
        and not isinstance(alpha, bool)
        and 0.0 < float(alpha) < 1.0,
        "alpha must be in (0, 1)",
    )
    delta_success_margin = _number(
        delta_success_margin, "delta_success_margin"
    )
    minimum_cost_saving = _number(
        minimum_cost_saving, "minimum_cost_saving", minimum=-math.inf
    )
    if cost_saving_support is not None:
        _require(
            isinstance(cost_saving_support, Sequence)
            and not isinstance(cost_saving_support, (str, bytes))
            and len(cost_saving_support) == 2,
            "cost_saving_support must contain exactly two numbers",
        )
        cost_support_lower = _number(
            cost_saving_support[0],
            "cost_saving_support lower",
            minimum=-math.inf,
        )
        cost_support_upper = _number(
            cost_saving_support[1],
            "cost_saving_support upper",
            minimum=-math.inf,
        )
        _require(
            cost_support_lower < cost_support_upper,
            "cost_saving_support lower must be less than upper",
        )
        cost_saving_support = (cost_support_lower, cost_support_upper)
    source_manifest, source_rows = _load_verified_source(observation_dir)
    _require(
        not require_real_cost or source_manifest["monetary_cost_available"],
        "external real cost is required but absent; refusing to invent cost",
    )
    dataset = _dataset_rows(source_rows)
    evaluation, comparisons = _evaluation(
        analysis_id=analysis_id,
        source_manifest=source_manifest,
        dataset=dataset,
        baseline_design_id=baseline_design_id,
        alpha=float(alpha),
        delta_success_margin=delta_success_margin,
        minimum_cost_saving=minimum_cost_saving,
        cost_saving_support=cost_saving_support,
    )
    oed = _oed_rows(
        comparisons,
        dataset,
        baseline_design_id=baseline_design_id,
        selection_size=oed_selection_size,
    )
    dataset_bytes = _jsonl_document(dataset)
    evaluation_bytes = _json_document(evaluation)
    oed_bytes = _jsonl_document(oed)
    manifest: dict[str, Any] = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "status": "FROZEN_NEUTRAL_AWM_OED_ANALYSIS",
        "analysis_id": analysis_id,
        "source_observation_set_id": source_manifest["observation_set_id"],
        "source_observation_manifest_sha256": source_manifest[
            "observation_manifest_sha256"
        ],
        "source_observations_file_sha256": source_manifest[
            "observations_file_sha256"
        ],
        "source_package_checksums_verified": True,
        "source_n1_score_authentication_required": True,
        "source_all_n1_scores_authenticated": True,
        "source_n1_authentication_provenance": (
            "verified-upstream-policy-oed-bridge-package"
        ),
        "n1_hmac_replayed_by_this_consumer": False,
        "trial_count": 64,
        "dataset_schema_version": DATASET_SCHEMA_VERSION,
        "oed_selection_schema_version": OED_SELECTION_SCHEMA_VERSION,
        "workload_count": 4,
        "design_count": 8,
        "repetitions": [0, 1],
        "baseline_design_id": baseline_design_id,
        "oed_selection_size": oed_selection_size,
        "require_real_cost": require_real_cost,
        "alpha": float(alpha),
        "delta_success_margin": delta_success_margin,
        "minimum_cost_saving": minimum_cost_saving,
        "cost_saving_support": (
            list(cost_saving_support) if cost_saving_support is not None else None
        ),
        "monetary_cost_available": source_manifest["monetary_cost_available"],
        "legacy_awm_dataset_emitted": False,
        "legacy_access_events_invented": False,
        "legacy_oed_allocator_used": False,
        "legacy_oed_allocator_not_used_reason": (
            "neutral evidence does not provide the legacy monetary paired-"
            "block planning contract"
        ),
        "weighted_certificate_core_used": evaluation[
            "weighted_certificate_evaluated"
        ],
        "decision_state": evaluation["decision_state"],
        "mathematical_certificate_state": evaluation[
            "mathematical_certificate_state"
        ],
        "dataset_file_sha256": _sha256(dataset_bytes),
        "evaluation_file_sha256": _sha256(evaluation_bytes),
        "oed_selection_file_sha256": _sha256(oed_bytes),
        "oed_selected_pair_count": len(oed),
        "oed_targets_fresh_independent_workloads": True,
        "posthoc": True,
        "commit_authorized": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    manifest["manifest_sha256"] = _sha256(_canonical(manifest))
    _walk_public([manifest, dataset, evaluation, oed])
    return {
        MANIFEST_NAME: _json_document(manifest),
        DATASET_NAME: dataset_bytes,
        EVALUATION_NAME: evaluation_bytes,
        OED_SELECTION_NAME: oed_bytes,
    }


def _checksums(documents: Mapping[str, bytes]) -> bytes:
    return b"".join(
        f"{_sha256(documents[name])}  {name}\n".encode("utf-8")
        for name in sorted(documents)
    )


def _publish(output_dir: str | Path, documents: Mapping[str, bytes]) -> Path:
    target = Path(output_dir).resolve()
    _require(not target.exists(), "neutral AWM/OED output already exists")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging_root = Path(
        tempfile.mkdtemp(prefix=".neutral-awm-oed-", dir=target.parent)
    )
    staging = staging_root / "output"
    try:
        staging.mkdir()
        complete = dict(documents)
        complete[CHECKSUMS_NAME] = _checksums(documents)
        for name, payload in complete.items():
            path = staging / name
            with path.open("wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        _verify_output_files(staging, set(complete))
        os.replace(staging, target)
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)
    return target


def _verify_output_files(root: Path, expected: set[str]) -> None:
    _require(root.is_dir(), "neutral AWM/OED output does not exist")
    entries = list(root.iterdir())
    _require(
        all(path.is_file() and not path.is_symlink() for path in entries)
        and {path.name for path in entries} == expected,
        "neutral AWM/OED output file set changed",
    )
    content = expected - {CHECKSUMS_NAME}
    lines = (root / CHECKSUMS_NAME).read_text(encoding="utf-8").splitlines()
    _require(len(lines) == len(content), "output checksums are incomplete")
    for line, name in zip(lines, sorted(content), strict=True):
        digest, separator, observed_name = line.partition("  ")
        _require(
            separator == "  " and observed_name == name,
            "output checksum line is not canonical",
        )
        _require(
            _SHA256.fullmatch(digest) is not None
            and _sha256((root / name).read_bytes()) == digest,
            f"output checksum mismatch: {name}",
        )


def freeze_neutral_awm_oed_analysis(
    *,
    observation_dir: str | Path,
    analysis_id: str,
    output_dir: str | Path,
    baseline_design_id: str = "D0",
    oed_selection_size: int = 4,
    require_real_cost: bool = False,
    alpha: float = 0.05,
    delta_success_margin: float = 0.0,
    minimum_cost_saving: float = 0.0,
    cost_saving_support: tuple[float, float] | None = None,
) -> dict[str, Any]:
    """Freeze a post-hoc neutral analysis and fresh-workload OED plan."""

    documents = _documents(
        observation_dir=observation_dir,
        analysis_id=analysis_id,
        baseline_design_id=baseline_design_id,
        oed_selection_size=oed_selection_size,
        require_real_cost=require_real_cost,
        alpha=alpha,
        delta_success_margin=delta_success_margin,
        minimum_cost_saving=minimum_cost_saving,
        cost_saving_support=cost_saving_support,
    )
    target = _publish(output_dir, documents)
    verified = verify_neutral_awm_oed_analysis(
        observation_dir=observation_dir,
        analysis_dir=target,
    )
    return {**verified, "output_dir": str(target)}


def verify_neutral_awm_oed_analysis(
    *,
    observation_dir: str | Path,
    analysis_dir: str | Path,
) -> dict[str, Any]:
    """Recompute the output from its frozen source and stored parameters."""

    root = Path(analysis_dir).resolve()
    expected_names = {
        MANIFEST_NAME,
        DATASET_NAME,
        EVALUATION_NAME,
        OED_SELECTION_NAME,
        CHECKSUMS_NAME,
    }
    _verify_output_files(root, expected_names)
    manifest = _read_json(root / MANIFEST_NAME, "neutral AWM/OED manifest")
    _require(
        manifest.get("schema_version") == MANIFEST_SCHEMA_VERSION
        and manifest.get("status") == "FROZEN_NEUTRAL_AWM_OED_ANALYSIS",
        "neutral AWM/OED manifest schema or status changed",
    )
    _manifest_digest(manifest, "manifest_sha256")
    support_value = manifest.get("cost_saving_support")
    support = None
    if support_value is not None:
        _require(
            isinstance(support_value, list) and len(support_value) == 2,
            "stored cost_saving_support is invalid",
        )
        support = (float(support_value[0]), float(support_value[1]))
    expected = _documents(
        observation_dir=observation_dir,
        analysis_id=manifest.get("analysis_id"),
        baseline_design_id=manifest.get("baseline_design_id"),
        oed_selection_size=manifest.get("oed_selection_size"),
        require_real_cost=manifest.get("require_real_cost"),
        alpha=manifest.get("alpha"),
        delta_success_margin=manifest.get("delta_success_margin"),
        minimum_cost_saving=manifest.get("minimum_cost_saving"),
        cost_saving_support=support,
    )
    _require(
        all(
            (root / name).read_bytes() == payload
            for name, payload in expected.items()
        ),
        "neutral AWM/OED output differs from deterministic recomputation",
    )
    return {
        "status": "VERIFIED",
        "analysis_id": manifest["analysis_id"],
        "trial_count": manifest["trial_count"],
        "workload_count": manifest["workload_count"],
        "design_count": manifest["design_count"],
        "monetary_cost_available": manifest["monetary_cost_available"],
        "weighted_certificate_core_used": manifest[
            "weighted_certificate_core_used"
        ],
        "mathematical_certificate_state": manifest[
            "mathematical_certificate_state"
        ],
        "decision_state": manifest["decision_state"],
        "oed_selected_pair_count": manifest["oed_selected_pair_count"],
        "commit_authorized": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "CHECKSUMS_NAME",
    "DATASET_NAME",
    "DATASET_ROW_SCHEMA_VERSION",
    "DATASET_SCHEMA_VERSION",
    "EVALUATION_NAME",
    "EVALUATION_SCHEMA_VERSION",
    "MANIFEST_NAME",
    "MANIFEST_SCHEMA_VERSION",
    "NeutralAwmOedConsumerError",
    "OED_ROW_SCHEMA_VERSION",
    "OED_SELECTION_NAME",
    "OED_SELECTION_SCHEMA_VERSION",
    "freeze_neutral_awm_oed_analysis",
    "verify_neutral_awm_oed_analysis",
]
