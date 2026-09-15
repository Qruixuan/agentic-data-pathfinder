"""Endpoint-free AWM/OED bridge for the 4x8 full-flow simulator.

This module does not implement or alter any AWM, OED, certificate, or power
calculation.  It has three deliberately narrow responsibilities:

* bind a prospective workload-class policy assignment to verified logical
  routes and compile the exact trials which that assignment permits;
* bind an OED-requested prospective subset and order without accepting any
  outcome data or allowing post-freeze mutation; and
* project either the legacy N4--N7--N6 evidence or the promoted generic
  raw/indexed/derived/cache N7/N8 route evidence into neutral success,
  component latency, and byte observations without inventing monetary cost.

Monetary values are admitted only from a separately frozen, explicitly
external calibration manifest.  Simulator rate cards, quoted prices, and
service-cost hints in execution evidence are rejected rather than relabelled
as real cost or scientific evidence.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .full_flow_logical_routes import (
    PLAN_NAME as LOGICAL_PLAN_NAME,
    TRIALS_NAME as LOGICAL_TRIALS_NAME,
    verify_full_flow_logical_routes,
)
from .full_flow_local_semantic_admission import (
    FrozenLocalSemanticExecutionInputs,
    load_full_flow_local_semantic_execution_inputs,
)
from .full_flow_matrix_runner import (
    FullFlowSemanticMatrixRunnerError,
    load_full_flow_semantic_matrix_route_evidence,
)
from .full_flow_runtime import (
    FULL_FLOW_EVIDENCE_SCHEMA_VERSION,
    FULL_FLOW_EVIDENCE_V2_SCHEMA_VERSION,
    FULL_FLOW_REQUEST_V2_SCHEMA_VERSION,
    full_flow_n1_score_request_id,
)
from .full_flow_semantic_route_runtime import (
    NEUTRAL_OBSERVATION_CANDIDATE_SCHEMA_VERSION,
    SEMANTIC_ROUTE_EVIDENCE_SCHEMA_VERSION,
)
from .hidden_oracle import (
    build_n1_public_task_binding,
    build_n1_score_request,
    verify_n1_score_result,
)
from ..integrations.flowmesh.semantic_matrix_trial import (
    verify_semantic_route_evidence,
)


POLICY_ASSIGNMENT_SCHEMA_VERSION = (
    "pathfinder.simulator-policy-assignment/v1alpha1"
)
POLICY_SELECTED_ROUTE_SCHEMA_VERSION = (
    "pathfinder.simulator-policy-selected-route/v1alpha1"
)
OED_PROSPECTIVE_PLAN_SCHEMA_VERSION = (
    "pathfinder.simulator-oed-prospective-plan/v1alpha1"
)
OED_SELECTED_ROUTE_SCHEMA_VERSION = (
    "pathfinder.simulator-oed-selected-route/v1alpha1"
)
NEUTRAL_OBSERVATION_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.simulator-neutral-observation-manifest/v1alpha3"
)
NEUTRAL_OBSERVATION_SCHEMA_VERSION = (
    "pathfinder.simulator-neutral-observation/v1alpha2"
)
EXTERNAL_REAL_COST_MANIFEST_SCHEMA_VERSION = (
    "pathfinder.external-real-cost-manifest/v1alpha1"
)

POLICY_MANIFEST_NAME = "policy-assignment.json"
POLICY_ROUTES_NAME = "policy-selected-routes.jsonl"
OED_MANIFEST_NAME = "oed-prospective-plan.json"
OED_ROUTES_NAME = "oed-selected-routes.jsonl"
OBSERVATION_MANIFEST_NAME = "neutral-observation-manifest.json"
OBSERVATIONS_NAME = "neutral-observations.jsonl"
EXTERNAL_COST_NAME = "external-real-cost-manifest.json"
CHECKSUMS_NAME = "SHA256SUMS"

_WORKLOAD_CLASSES = tuple(f"W{index}" for index in range(1, 5))
_DESIGN_IDS = tuple(f"D{index}" for index in range(8))
_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,255}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_CURRENCY = re.compile(r"[A-Z]{3}\Z")
_WINDOWS_ABSOLUTE = re.compile(r"^[A-Za-z]:[\\/]")


class PolicyOedBridgeError(ValueError):
    """Raised when a bridge artifact cannot be frozen or verified safely."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise PolicyOedBridgeError(message)


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise PolicyOedBridgeError("value is not canonical JSON") from exc


def _json_document(value: Any) -> bytes:
    return _canonical_bytes(value) + b"\n"


def _jsonl_document(rows: Sequence[Mapping[str, Any]]) -> bytes:
    return b"".join(_canonical_bytes(row) + b"\n" for row in rows)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


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


def _integer(value: Any, name: str, *, minimum: int = 0) -> int:
    _require(
        isinstance(value, int)
        and not isinstance(value, bool)
        and value >= minimum,
        f"{name} must be an integer >= {minimum}",
    )
    return value


def _number(value: Any, name: str) -> float:
    _require(
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        and float(value) >= 0,
        f"{name} must be a finite non-negative number",
    )
    return float(value)


def _optional_number(value: Any, name: str) -> float | None:
    return None if value is None else _number(value, name)


def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        _require(key not in value, f"duplicate JSON key: {key}")
        value[key] = item
    return value


def _read_json_bytes(payload: bytes, name: str) -> dict[str, Any]:
    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_pairs,
            parse_constant=lambda item: (_ for _ in ()).throw(
                PolicyOedBridgeError(f"{name} contains {item}")
            ),
        )
    except PolicyOedBridgeError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise PolicyOedBridgeError(f"{name} is not valid JSON") from exc
    _require(isinstance(value, dict), f"{name} must be an object")
    return value


def _read_json(path: Path, name: str) -> dict[str, Any]:
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise PolicyOedBridgeError(f"cannot read {name}") from exc
    return _read_json_bytes(payload, name)


def _read_jsonl(path: Path, name: str) -> list[dict[str, Any]]:
    try:
        lines = path.read_bytes().splitlines()
    except OSError as exc:
        raise PolicyOedBridgeError(f"cannot read {name}") from exc
    _require(bool(lines), f"{name} cannot be empty")
    return [
        _read_json_bytes(line, f"{name} line {index}")
        for index, line in enumerate(lines, start=1)
    ]


def _assert_endpoint_credential_free(value: Any) -> None:
    def visit(item: Any) -> None:
        if isinstance(item, Mapping):
            for key, child in item.items():
                lowered = str(key).casefold()
                _require(
                    lowered not in {
                        "api_key",
                        "apikey",
                        "authorization",
                        "bearer_token",
                        "password",
                        "secret",
                    },
                    "bridge output contains a credential field",
                )
                visit(child)
        elif isinstance(item, list):
            for child in item:
                visit(child)
        elif isinstance(item, str):
            lowered = item.casefold()
            _require(
                "://" not in lowered
                and not item.startswith(("/", "~"))
                and _WINDOWS_ABSOLUTE.match(item) is None
                and "bearer " not in lowered,
                "bridge output contains endpoint, path, or credential material",
            )

    visit(value)


def _verified_routes(
    *,
    logical_route_plan_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, dict[str, Any]]]:
    report = verify_full_flow_logical_routes(
        logical_route_plan_dir,
        scenario_path,
        container_plan_dir,
    )
    root = Path(logical_route_plan_dir).resolve()
    plan = _read_json(root / LOGICAL_PLAN_NAME, "logical route plan")
    rows = _read_jsonl(root / LOGICAL_TRIALS_NAME, "logical route trials")
    _require(
        report.get("status") == "VERIFIED"
        and report.get("plan_sha256") == plan.get("plan_sha256"),
        "logical route plan was not verified",
    )
    trial_map: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = row.get("trial_key")
        _require(
            isinstance(key, str) and key and key not in trial_map,
            "logical route trial keys are invalid",
        )
        trial_map[key] = row
    _require(len(trial_map) == 64, "logical route plan is not the frozen 4x8 run")
    _assert_endpoint_credential_free([plan, rows])
    return plan, rows, trial_map


def _compiled_route(
    row: Mapping[str, Any],
    *,
    schema_version: str,
    selection_index: int,
) -> dict[str, Any]:
    return {
        "schema_version": schema_version,
        "selection_index": selection_index,
        "trial_key": row["trial_key"],
        "source_route_row_sha256": _sha256(_canonical_bytes(row)),
        "source_order_index": row["order_index"],
        "workload_id": row["workload_id"],
        "workload_class": row["workload_class"],
        "design_id": row["design_id"],
        "repetition": row["repetition"],
        "object_id": row["object_id"],
        "executor_node_id": row["executor_node_id"],
        "route_template_id": row["route_template_id"],
        "route_family": row["route_family"],
        "index_mode": row["index_mode"],
        "representation_ids": list(row["representation_ids"]),
        "conditional_cache_branch": row["conditional_cache_branch"],
        "required_service_nodes": list(row["required_service_nodes"]),
        "required_service_contract_ids": list(
            row["required_service_contract_ids"]
        ),
        "execution_stage_keys": list(row["execution_stage_keys"]),
        "evaluation_stage_keys": list(row["evaluation_stage_keys"]),
        "endpoint_binding_included": False,
        "credentials_recorded": False,
    }


def _checksums(documents: Mapping[str, bytes]) -> bytes:
    return b"".join(
        f"{_sha256(documents[name])}  {name}\n".encode("utf-8")
        for name in sorted(documents)
    )


def _publish(output_dir: str | Path, documents: Mapping[str, bytes]) -> Path:
    target = Path(output_dir).resolve()
    _require(not target.exists(), "bridge output directory already exists")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging_root = Path(
        tempfile.mkdtemp(prefix=".policy-oed-bridge-", dir=target.parent)
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
        _verify_files(staging, set(complete))
        os.replace(staging, target)
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)
    return target


def _verify_files(root: Path, expected_names: set[str]) -> None:
    _require(root.is_dir(), "bridge output directory does not exist")
    entries = list(root.iterdir())
    _require(
        all(path.is_file() and not path.is_symlink() for path in entries),
        "bridge output must contain regular files only",
    )
    _require(
        {path.name for path in entries} == expected_names,
        "bridge output file set changed",
    )
    try:
        lines = (root / CHECKSUMS_NAME).read_text(
            encoding="utf-8"
        ).splitlines()
    except (OSError, UnicodeError) as exc:
        raise PolicyOedBridgeError("cannot read bridge SHA256SUMS") from exc
    content_names = sorted(expected_names - {CHECKSUMS_NAME})
    _require(len(lines) == len(content_names), "bridge checksums are incomplete")
    observed: list[str] = []
    for line, expected_name in zip(lines, content_names, strict=True):
        digest, separator, name = line.partition("  ")
        _require(
            separator == "  "
            and name == expected_name
            and _SHA256.fullmatch(digest) is not None,
            "bridge checksum line is not canonical",
        )
        _require(
            _sha256((root / name).read_bytes()) == digest,
            f"bridge checksum mismatch: {name}",
        )
        observed.append(name)
    _require(observed == content_names, "bridge checksum order changed")


def _manifest_digest(value: Mapping[str, Any], field: str) -> str:
    unsigned = dict(value)
    recorded = _digest(unsigned.pop(field, None), field)
    _require(
        recorded == _sha256(_canonical_bytes(unsigned)),
        f"{field} mismatch",
    )
    return recorded


def _assignment_rows(
    assignments: Mapping[str, Sequence[str]],
) -> list[dict[str, Any]]:
    _require(isinstance(assignments, Mapping), "assignments must be an object")
    _require(
        set(assignments) == set(_WORKLOAD_CLASSES),
        "assignments must define W1 through W4 exactly",
    )
    rows: list[dict[str, Any]] = []
    for workload_class in _WORKLOAD_CLASSES:
        raw = assignments[workload_class]
        _require(
            isinstance(raw, Sequence)
            and not isinstance(raw, (str, bytes))
            and bool(raw),
            f"{workload_class} allowed designs must be a non-empty array",
        )
        values = list(raw)
        _require(
            all(type(item) is str and item in _DESIGN_IDS for item in values)
            and len(values) == len(set(values)),
            f"{workload_class} allowed designs are invalid",
        )
        rows.append({
            "workload_class": workload_class,
            "allowed_design_ids": sorted(
                values,
                key=_DESIGN_IDS.index,
            ),
        })
    return rows


def _policy_documents(
    *,
    policy_id: str,
    awm_policy_sha256: str,
    assignments: Mapping[str, Sequence[str]],
    route_plan: Mapping[str, Any],
    route_rows: Sequence[Mapping[str, Any]],
) -> dict[str, bytes]:
    policy_id = _identifier(policy_id, "policy_id")
    awm_policy_sha256 = _digest(awm_policy_sha256, "awm_policy_sha256")
    assignment_rows = _assignment_rows(assignments)
    allowed = {
        row["workload_class"]: set(row["allowed_design_ids"])
        for row in assignment_rows
    }
    selected_source = [
        row
        for row in route_rows
        if row["design_id"] in allowed[row["workload_class"]]
    ]
    selected_source.sort(key=lambda row: row["order_index"])
    _require(bool(selected_source), "policy selected no logical routes")
    selected = [
        _compiled_route(
            row,
            schema_version=POLICY_SELECTED_ROUTE_SCHEMA_VERSION,
            selection_index=index,
        )
        for index, row in enumerate(selected_source)
    ]
    routes_bytes = _jsonl_document(selected)
    manifest: dict[str, Any] = {
        "schema_version": POLICY_ASSIGNMENT_SCHEMA_VERSION,
        "status": "FROZEN_PROSPECTIVE_POLICY_ASSIGNMENT",
        "policy_id": policy_id,
        "awm_policy_sha256": awm_policy_sha256,
        "logical_route_plan_sha256": route_plan["plan_sha256"],
        "scenario_id": route_plan["scenario_id"],
        "assignments": assignment_rows,
        "selected_trial_count": len(selected),
        "selected_trial_keys_sha256": _sha256(_canonical_bytes([
            row["trial_key"] for row in selected
        ])),
        "selected_routes_file_sha256": _sha256(routes_bytes),
        "bridge_recomputed_awm_mathematics": False,
        "outcomes_consumed": False,
        "outcome_adaptive": False,
        "endpoint_free": True,
        "credentials_recorded": False,
        "synthetic_cost_claimed_as_real": False,
        "eligible_for_scientific_claims": False,
    }
    manifest["assignment_sha256"] = _sha256(_canonical_bytes(manifest))
    _assert_endpoint_credential_free([manifest, selected])
    return {
        POLICY_MANIFEST_NAME: _json_document(manifest),
        POLICY_ROUTES_NAME: routes_bytes,
    }


def freeze_policy_assignment(
    *,
    logical_route_plan_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    policy_id: str,
    awm_policy_sha256: str,
    assignments: Mapping[str, Sequence[str]],
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze an AWM-selected W1--W4 to allowed D0--D7 assignment."""

    plan, rows, _ = _verified_routes(
        logical_route_plan_dir=logical_route_plan_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    documents = _policy_documents(
        policy_id=policy_id,
        awm_policy_sha256=awm_policy_sha256,
        assignments=assignments,
        route_plan=plan,
        route_rows=rows,
    )
    target = _publish(output_dir, documents)
    verified = verify_policy_assignment(
        assignment_dir=target,
        logical_route_plan_dir=logical_route_plan_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    return {**verified, "output_dir": str(target)}


def verify_policy_assignment(
    *,
    assignment_dir: str | Path,
    logical_route_plan_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
) -> dict[str, Any]:
    root = Path(assignment_dir).resolve()
    _verify_files(
        root,
        {POLICY_MANIFEST_NAME, POLICY_ROUTES_NAME, CHECKSUMS_NAME},
    )
    manifest = _read_json(root / POLICY_MANIFEST_NAME, "policy assignment")
    _require(
        manifest.get("schema_version") == POLICY_ASSIGNMENT_SCHEMA_VERSION
        and manifest.get("status") == "FROZEN_PROSPECTIVE_POLICY_ASSIGNMENT",
        "policy assignment schema or status changed",
    )
    _manifest_digest(manifest, "assignment_sha256")
    plan, rows, _ = _verified_routes(
        logical_route_plan_dir=logical_route_plan_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    assignment_map = {
        row["workload_class"]: row["allowed_design_ids"]
        for row in manifest.get("assignments", [])
        if isinstance(row, dict)
        and "workload_class" in row
        and "allowed_design_ids" in row
    }
    expected = _policy_documents(
        policy_id=manifest.get("policy_id"),
        awm_policy_sha256=manifest.get("awm_policy_sha256"),
        assignments=assignment_map,
        route_plan=plan,
        route_rows=rows,
    )
    _require(
        all(
            (root / name).read_bytes() == payload
            for name, payload in expected.items()
        ),
        "policy assignment does not match deterministic route compilation",
    )
    return {
        "status": "VERIFIED",
        "policy_id": manifest["policy_id"],
        "assignment_sha256": manifest["assignment_sha256"],
        "logical_route_plan_sha256": plan["plan_sha256"],
        "selected_trial_count": manifest["selected_trial_count"],
        "outcome_adaptive": False,
        "endpoint_free": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def _oed_documents(
    *,
    oed_request_id: str,
    oed_request_sha256: str,
    requested_trial_keys: Sequence[str],
    route_plan: Mapping[str, Any],
    trial_map: Mapping[str, Mapping[str, Any]],
) -> dict[str, bytes]:
    oed_request_id = _identifier(oed_request_id, "oed_request_id")
    oed_request_sha256 = _digest(
        oed_request_sha256,
        "oed_request_sha256",
    )
    _require(
        isinstance(requested_trial_keys, Sequence)
        and not isinstance(requested_trial_keys, (str, bytes))
        and bool(requested_trial_keys),
        "requested_trial_keys must be a non-empty array",
    )
    keys = list(requested_trial_keys)
    _require(
        all(isinstance(key, str) and key in trial_map for key in keys),
        "OED request contains an unknown trial key",
    )
    _require(len(keys) == len(set(keys)), "OED trial keys cannot repeat")
    selected = [
        _compiled_route(
            trial_map[key],
            schema_version=OED_SELECTED_ROUTE_SCHEMA_VERSION,
            selection_index=index,
        )
        for index, key in enumerate(keys)
    ]
    routes_bytes = _jsonl_document(selected)
    manifest: dict[str, Any] = {
        "schema_version": OED_PROSPECTIVE_PLAN_SCHEMA_VERSION,
        "status": "FROZEN_PROSPECTIVE_OED_SELECTION",
        "oed_request_id": oed_request_id,
        "oed_request_sha256": oed_request_sha256,
        "logical_route_plan_sha256": route_plan["plan_sha256"],
        "scenario_id": route_plan["scenario_id"],
        "requested_trial_count": len(keys),
        "requested_trial_keys_sha256": _sha256(_canonical_bytes(keys)),
        "selected_routes_file_sha256": _sha256(routes_bytes),
        "bridge_recomputed_oed_mathematics": False,
        "bridge_consumed_outcomes": False,
        "order_frozen_before_execution": True,
        "outcome_adaptive_changes_allowed": False,
        "upstream_prospectiveness_independently_verified": False,
        "endpoint_free": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    manifest["prospective_plan_sha256"] = _sha256(
        _canonical_bytes(manifest)
    )
    _assert_endpoint_credential_free([manifest, selected])
    return {
        OED_MANIFEST_NAME: _json_document(manifest),
        OED_ROUTES_NAME: routes_bytes,
    }


def freeze_oed_prospective_selection(
    *,
    logical_route_plan_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    oed_request_id: str,
    oed_request_sha256: str,
    requested_trial_keys: Sequence[str],
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze an exact prospective subset and order requested by OED."""

    plan, _, trial_map = _verified_routes(
        logical_route_plan_dir=logical_route_plan_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    documents = _oed_documents(
        oed_request_id=oed_request_id,
        oed_request_sha256=oed_request_sha256,
        requested_trial_keys=requested_trial_keys,
        route_plan=plan,
        trial_map=trial_map,
    )
    target = _publish(output_dir, documents)
    verified = verify_oed_prospective_selection(
        selection_dir=target,
        logical_route_plan_dir=logical_route_plan_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    return {**verified, "output_dir": str(target)}


def verify_oed_prospective_selection(
    *,
    selection_dir: str | Path,
    logical_route_plan_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
) -> dict[str, Any]:
    root = Path(selection_dir).resolve()
    _verify_files(root, {OED_MANIFEST_NAME, OED_ROUTES_NAME, CHECKSUMS_NAME})
    manifest = _read_json(root / OED_MANIFEST_NAME, "OED prospective plan")
    _require(
        manifest.get("schema_version") == OED_PROSPECTIVE_PLAN_SCHEMA_VERSION
        and manifest.get("status") == "FROZEN_PROSPECTIVE_OED_SELECTION",
        "OED prospective plan schema or status changed",
    )
    _manifest_digest(manifest, "prospective_plan_sha256")
    selected = _read_jsonl(root / OED_ROUTES_NAME, "OED selected routes")
    keys = [row.get("trial_key") for row in selected]
    plan, _, trial_map = _verified_routes(
        logical_route_plan_dir=logical_route_plan_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    expected = _oed_documents(
        oed_request_id=manifest.get("oed_request_id"),
        oed_request_sha256=manifest.get("oed_request_sha256"),
        requested_trial_keys=keys,
        route_plan=plan,
        trial_map=trial_map,
    )
    _require(
        all(
            (root / name).read_bytes() == payload
            for name, payload in expected.items()
        ),
        "OED selection does not match deterministic route compilation",
    )
    return {
        "status": "VERIFIED",
        "oed_request_id": manifest["oed_request_id"],
        "prospective_plan_sha256": manifest["prospective_plan_sha256"],
        "logical_route_plan_sha256": plan["plan_sha256"],
        "requested_trial_count": len(keys),
        "bridge_consumed_outcomes": False,
        "order_frozen_before_execution": True,
        "outcome_adaptive_changes_allowed": False,
        "endpoint_free": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }


def _reject_cost_or_scientific_claims(value: Any, path: str = "evidence") -> None:
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key).casefold().replace("-", "_")
            if (
                "cost" in key
                or "price" in key
                or "billing" in key
                or "currency" in key
                or "rate_card" in key
                or ("service" in key and "hint" in key)
                or ("synthetic" in key and "hint" in key)
            ):
                raise PolicyOedBridgeError(
                    f"{path}.{raw_key} is a forbidden embedded cost claim"
                )
            if key in {
                "eligible_for_scientific_claims",
                "scientific_evidence",
                "scientific_claim",
            } and child is not False:
                raise PolicyOedBridgeError(
                    f"{path}.{raw_key} claims scientific eligibility"
                )
            _reject_cost_or_scientific_claims(child, f"{path}.{raw_key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_cost_or_scientific_claims(child, f"{path}[{index}]")
    elif isinstance(value, str):
        lowered = value.casefold()
        _require(
            "pilot-cost-unit" not in lowered
            and "synthetic-rate-card" not in lowered,
            f"{path} contains a synthetic monetary-cost label",
        )


_REAL_COST_KEYS = {
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
_REAL_COST_ENTRY_KEYS = {"trial_key", "amount", "measurement_sha256"}


def validate_external_real_cost_manifest(
    manifest: Mapping[str, Any],
    *,
    logical_route_plan_sha256: str,
    expected_trial_keys: set[str],
) -> dict[str, Any]:
    """Validate an external cost attestation without estimating any cost."""

    _require(isinstance(manifest, Mapping), "real-cost manifest must be an object")
    value = json.loads(_canonical_bytes(manifest))
    _require(set(value) == _REAL_COST_KEYS, "real-cost manifest fields changed")
    _require(
        value["schema_version"] == EXTERNAL_REAL_COST_MANIFEST_SCHEMA_VERSION
        and value["status"] == "FROZEN_EXTERNALLY_CALIBRATED_REAL_COSTS",
        "real-cost manifest schema or status changed",
    )
    _identifier(value["calibration_id"], "calibration_id")
    _digest(
        value["calibration_evidence_sha256"],
        "calibration_evidence_sha256",
    )
    _require(
        value["logical_route_plan_sha256"] == logical_route_plan_sha256,
        "real-cost manifest route-plan binding changed",
    )
    _require(
        isinstance(value["currency"], str)
        and _CURRENCY.fullmatch(value["currency"]) is not None,
        "real-cost currency must be an uppercase ISO-style code",
    )
    _require(
        value["external_calibration"] is True
        and value["synthetic_simulator_inputs_used"] is False
        and value["credentials_recorded"] is False
        and value["eligible_for_scientific_claims"] is False,
        "real-cost provenance flags are invalid",
    )
    entries = value["entries"]
    _require(isinstance(entries, list), "real-cost entries must be an array")
    costs: dict[str, dict[str, Any]] = {}
    for entry in entries:
        _require(
            isinstance(entry, dict) and set(entry) == _REAL_COST_ENTRY_KEYS,
            "real-cost entry fields changed",
        )
        trial_key = entry["trial_key"]
        _require(
            isinstance(trial_key, str)
            and trial_key in expected_trial_keys
            and trial_key not in costs,
            "real-cost entry has an unknown or duplicate trial key",
        )
        costs[trial_key] = {
            "amount": _number(entry["amount"], "real-cost amount"),
            "measurement_sha256": _digest(
                entry["measurement_sha256"],
                "real-cost measurement_sha256",
            ),
        }
    _require(
        set(costs) == expected_trial_keys,
        "real-cost manifest does not cover the observation set exactly",
    )
    _require(
        [entry["trial_key"] for entry in entries] == sorted(expected_trial_keys),
        "real-cost entries must be sorted by trial key",
    )
    _manifest_digest(value, "manifest_sha256")
    _assert_endpoint_credential_free(value)
    return value


def _legacy_full_flow_observation(
    evidence: Mapping[str, Any],
    route: Mapping[str, Any],
    *,
    cost: Mapping[str, Any] | None,
    currency: str | None,
    n1_oracle_package_dir: str | Path | None,
    n1_evidence_secret: bytes | None,
) -> dict[str, Any]:
    _require(isinstance(evidence, Mapping), "full-flow evidence must be an object")
    _reject_cost_or_scientific_claims(evidence)
    evidence_schema = evidence.get("schema_version")
    _require(
        evidence_schema
        in {
            FULL_FLOW_EVIDENCE_SCHEMA_VERSION,
            FULL_FLOW_EVIDENCE_V2_SCHEMA_VERSION,
        }
        and evidence.get("status") == "COMPLETE",
        "full-flow evidence is not complete",
    )
    trial_key = evidence.get("trial_key")
    _require(trial_key == route["trial_key"], "full-flow trial binding changed")
    _require(
        evidence.get("workload_id") == route["workload_id"]
        and evidence.get("object_id") == route["object_id"],
        "full-flow workload or object binding changed",
    )
    evidence_route = evidence.get("route")
    _require(isinstance(evidence_route, Mapping), "full-flow route is missing")
    _require(
        evidence_route.get("source_node_id") == "N4"
        and evidence_route.get("executor_node_id") == "N7"
        and evidence_route.get("inference_node_id") == "N6",
        "full-flow unified N4-N7-N6 route changed",
    )
    required_true = (
        "route_unified",
        "real_object_identity_verified",
        "data_agent_source_identity_verified",
        "data_agent_artifact_delivery_verified",
        "semantic_frame_payload_integrity_verified",
        "semantic_health_verified",
        "scoring_verified",
        "telemetry_complete",
    )
    _require(
        all(evidence.get(name) is True for name in required_true)
        and evidence.get("credentials_recorded") is False
        and evidence.get("eligible_for_scientific_claims") is False,
        "full-flow evidence verification flags are incomplete",
    )
    scoring = evidence.get("scoring")
    data_agent = evidence.get("data_agent")
    semantic = evidence.get("semantic")
    _require(
        isinstance(scoring, Mapping)
        and isinstance(data_agent, Mapping)
        and isinstance(semantic, Mapping),
        "full-flow component evidence is missing",
    )
    task_success = scoring.get("task_success")
    _require(type(task_success) is bool, "task_success must be boolean")
    score_authenticity_verified = False
    score_authentication = "legacy-structural-only"
    if evidence_schema == FULL_FLOW_EVIDENCE_V2_SCHEMA_VERSION:
        _require(
            n1_oracle_package_dir is not None
            and n1_evidence_secret is not None,
            "hidden-oracle v2 evidence requires privileged N1 verification",
        )
        oracle_result = scoring.get("oracle_result")
        final_answer = scoring.get("final_answer")
        _require(
            isinstance(oracle_result, Mapping)
            and isinstance(final_answer, str)
            and bool(final_answer),
            "hidden-oracle v2 scoring evidence is incomplete",
        )
        score_identity_request = {
            "schema_version": FULL_FLOW_REQUEST_V2_SCHEMA_VERSION,
            "oracle_id": oracle_result.get("oracle_id"),
            "run_id": evidence.get("run_id"),
            "trial_id": evidence.get("trial_id"),
            "full_flow_request_id": evidence.get("full_flow_request_id"),
            "frozen_binding_sha256": evidence.get(
                "frozen_binding_sha256"
            ),
            "task_binding_sha256": oracle_result.get(
                "task_binding_sha256"
            ),
        }
        try:
            expected_score_request_id = full_flow_n1_score_request_id(
                score_identity_request,
                final_answer=final_answer,
            )
        except Exception as exc:
            raise PolicyOedBridgeError(
                "hidden-oracle v2 score identity is invalid"
            ) from exc
        _require(
            oracle_result.get("score_request_id")
            == expected_score_request_id,
            "hidden-oracle score request identity changed",
        )
        score_request = build_n1_score_request(
            score_request_id=expected_score_request_id,
            oracle_id=oracle_result.get("oracle_id"),
            run_id=evidence.get("run_id"),
            trial_id=evidence.get("trial_id"),
            object_id=evidence.get("object_id"),
            task_binding_sha256=oracle_result.get(
                "task_binding_sha256"
            ),
            predicted_answer=final_answer,
        )
        try:
            authenticated = verify_n1_score_result(
                package_dir=n1_oracle_package_dir,
                request=score_request,
                result=oracle_result,
                evidence_secret=n1_evidence_secret,
            )
        except Exception as exc:
            raise PolicyOedBridgeError(
                "hidden-oracle v2 score authentication failed"
            ) from exc
        _require(
            authenticated.get("correct") is task_success,
            "authenticated hidden-oracle score differs from task_success",
        )
        score_authenticity_verified = True
        score_authentication = "n1-hmac-verified"
    latency = data_agent.get("latency_ms")
    delivery = data_agent.get("delivery")
    _require(
        isinstance(latency, Mapping) and isinstance(delivery, Mapping),
        "full-flow transfer telemetry is missing",
    )
    latency_fields = {
        "data_agent_service": _optional_number(
            latency.get("data_agent_service"),
            "data_agent_service latency",
        ),
        "client_access_round_trip": _optional_number(
            latency.get("client_access_round_trip"),
            "client_access_round_trip latency",
        ),
        "artifact_download_elapsed": _optional_number(
            latency.get("artifact_download_elapsed"),
            "artifact_download_elapsed latency",
        ),
        "server_reported_transfer": _optional_number(
            latency.get("server_reported_transfer"),
            "server_reported_transfer latency",
        ),
        "semantic_service": _number(
            semantic.get("service_time_ms"),
            "semantic_service latency",
        ),
    }
    artifact_size = _integer(
        data_agent.get("artifact_size_bytes"),
        "artifact_size_bytes",
    )
    bytes_sent = _integer(delivery.get("bytes_sent"), "delivery.bytes_sent")
    semantic_bytes = _integer(
        semantic.get("representation_delivery_bytes"),
        "semantic.representation_delivery_bytes",
    )
    _require(
        delivery.get("telemetry_complete") is True
        and delivery.get("exactly_one_full_download") is True
        and delivery.get("bytes_sent_equals_artifact_size") is True
        and bytes_sent == artifact_size,
        "full-flow byte-delivery accounting is incomplete",
    )
    monetary = None
    if cost is not None:
        monetary = {
            "amount": cost["amount"],
            "currency": currency,
            "measurement_sha256": cost["measurement_sha256"],
        }
    return {
        "schema_version": NEUTRAL_OBSERVATION_SCHEMA_VERSION,
        "trial_key": trial_key,
        "source_full_flow_evidence_sha256": _sha256(
            _canonical_bytes(evidence)
        ),
        "source_full_flow_evidence_schema_version": evidence_schema,
        "source_route_row_sha256": _sha256(_canonical_bytes(route)),
        "source_order_index": route["order_index"],
        "workload_id": route["workload_id"],
        "workload_class": route["workload_class"],
        "design_id": route["design_id"],
        "repetition": route["repetition"],
        "object_id": route["object_id"],
        "task_success": task_success,
        "score_authenticity_verified": score_authenticity_verified,
        "score_authentication": score_authentication,
        "latency_measurements_ms": latency_fields,
        "latency_measurement_semantics": (
            "component-observations-not-end-to-end"
        ),
        "end_to_end_latency_available": False,
        "byte_measurements": {
            "data_agent_artifact_size": artifact_size,
            "data_agent_bytes_sent": bytes_sent,
            "semantic_representation_delivery": semantic_bytes,
            "two_leg_transfer_bytes_sum": bytes_sent + semantic_bytes,
        },
        "byte_measurement_semantics": (
            "delivery-accounting-not-network-throughput"
        ),
        "monetary_cost_available": cost is not None,
        "monetary_cost": monetary,
        "synthetic_simulator_cost_hints_consumed": False,
        "performance_evidence_claimed": False,
        "scientific_evidence_claimed": False,
        "hidden_label_values_included": False,
        "hidden_label_values_consumed_by_bridge": False,
        "credentials_recorded": False,
    }


_GENERIC_CANDIDATE_FIELDS = {
    "schema_version",
    "trial_key",
    "order_index",
    "workload_id",
    "workload_class",
    "design_id",
    "repetition",
    "object_id",
    "route_family",
    "executor_node_id",
    "task_success",
    "score",
    "score_authenticity_verified",
    "score_authentication",
    "component_service_time_ms",
    "byte_measurements",
    "monetary_measurement_available",
    "monetary_values_included",
    "synthetic_monetary_inputs_consumed",
    "credentials_recorded",
    "eligible_for_scientific_claims",
}
_GENERIC_BYTE_FIELDS = {
    "adapter_bytes_read",
    "adapter_bytes_sent",
    "semantic_input_bytes",
}
_GENERIC_PRIVATE_FIELDS = {
    "correct_answer",
    "correct_answer_id",
    "hidden_answer",
    "hidden_label",
    "hidden_labels",
    "label_values",
    "labels",
}


def _assert_hidden_label_free(value: Any, path: str = "evidence") -> None:
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key).casefold().replace("-", "_")
            _require(
                key not in _GENERIC_PRIVATE_FIELDS,
                f"{path}.{raw_key} crosses the hidden-label boundary",
            )
            _assert_hidden_label_free(child, f"{path}.{raw_key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _assert_hidden_label_free(child, f"{path}[{index}]")


def _generic_runtime_bindings(
    *,
    semantic_execution_admission_dir: str | Path,
    route_plan: Mapping[str, Any],
    trial_map: Mapping[str, Mapping[str, Any]],
) -> tuple[
    FrozenLocalSemanticExecutionInputs,
    dict[str, Mapping[str, Any]],
    dict[str, Mapping[str, Any]],
]:
    try:
        inputs = load_full_flow_local_semantic_execution_inputs(
            semantic_execution_admission_dir
        )
    except Exception as exc:
        raise PolicyOedBridgeError(
            "local semantic execution admission is not verified"
        ) from exc
    admission = inputs.admission
    _require(
        admission.get("scenario_id") == route_plan.get("scenario_id"),
        "local semantic admission is bound to a different scenario",
    )
    bound_by_trial: dict[str, Mapping[str, Any]] = {}
    for bound in inputs.bound_trials:
        trial_key = bound.get("trial_key")
        _require(
            isinstance(trial_key, str)
            and trial_key in trial_map
            and trial_key not in bound_by_trial,
            "local semantic admission trial coverage changed",
        )
        route = trial_map[trial_key]
        copied_fields = (
            "order_index",
            "workload_id",
            "workload_class",
            "design_id",
            "repetition",
            "route_family",
            "executor_node_id",
        )
        _require(
            all(bound.get(field) == route.get(field) for field in copied_fields),
            f"local semantic trial differs from logical route: {trial_key}",
        )
        identities = bound.get("representation_identities")
        _require(
            isinstance(identities, list)
            and [row.get("representation_id") for row in identities]
            == route.get("representation_ids"),
            f"local semantic representations differ from logical route: "
            f"{trial_key}",
        )
        _require(
            all(
                row.get("logical_object_id") == route.get("object_id")
                and row.get("artifact_object_id")
                == bound.get("artifact_object_id")
                for row in identities
            ),
            f"local semantic artifact identity changed: {trial_key}",
        )
        public_task = bound.get("public_task_binding")
        _require(
            isinstance(public_task, Mapping),
            f"local semantic public task is missing: {trial_key}",
        )
        try:
            rebuilt_task = build_n1_public_task_binding(
                workload_id=public_task.get("workload_id"),
                object_id=public_task.get("object_id"),
                task_class_id=public_task.get("task_class_id"),
                question=public_task.get("question"),
                answer_options=public_task.get("answer_options"),
                success_scoring_rule=public_task.get("success_scoring_rule"),
            )
        except Exception as exc:
            raise PolicyOedBridgeError(
                f"local semantic public task is invalid: {trial_key}"
            ) from exc
        _require(
            dict(public_task) == rebuilt_task
            and public_task.get("workload_id") == route.get("workload_id")
            and public_task.get("object_id") == bound.get("artifact_object_id")
            and public_task.get("task_binding_sha256")
            == bound.get("public_task_binding_sha256"),
            f"local semantic public task binding changed: {trial_key}",
        )
        expected_stage_keys = (
            list(route.get("execution_stage_keys", []))
            + list(route.get("evaluation_stage_keys", []))
        )
        _require(
            bound.get("semantic_stage_keys") == expected_stage_keys,
            f"local semantic stage route binding changed: {trial_key}",
        )
        _digest(
            bound.get("source_semantic_trial_sha256"),
            "source_semantic_trial_sha256",
        )
        _require(
            bound.get("flowmesh_submission_authorized") is True
            and bound.get("credentials_recorded") is False,
            f"local semantic trial is not safely promoted: {trial_key}",
        )
        bound_by_trial[trial_key] = bound
    _require(
        set(bound_by_trial) == set(trial_map),
        "local semantic admission does not bind all 64 logical routes",
    )
    stage_by_key: dict[str, Mapping[str, Any]] = {}
    for stage in inputs.bound_stages:
        key = stage.get("stage_key")
        _require(
            isinstance(key, str) and key and key not in stage_by_key,
            "local semantic stage identities changed",
        )
        stage_by_key[key] = stage
    for trial_key, bound in bound_by_trial.items():
        keys = bound["semantic_stage_keys"]
        hashes = bound.get("bound_stage_sha256")
        _require(
            isinstance(hashes, list)
            and len(hashes) == len(keys)
            and all(key in stage_by_key for key in keys)
            and all(
                _sha256(_canonical_bytes(stage_by_key[key]))
                == _digest(digest, "bound stage SHA-256")
                for key, digest in zip(keys, hashes, strict=True)
            ),
            f"local semantic stage digest binding changed: {trial_key}",
        )
    return inputs, bound_by_trial, stage_by_key


def _generic_semantic_route_observation(
    evidence: Mapping[str, Any],
    route: Mapping[str, Any],
    *,
    bound_trial: Mapping[str, Any],
    bound_stages: Sequence[Mapping[str, Any]],
    admission_sha256: str,
    cost: Mapping[str, Any] | None,
    currency: str | None,
    n1_oracle_package_dir: str | Path,
    n1_evidence_secret: bytes,
) -> dict[str, Any]:
    _require(isinstance(evidence, Mapping), "route evidence must be an object")
    _reject_cost_or_scientific_claims(evidence)
    _assert_hidden_label_free(evidence)
    run_id = evidence.get("run_id")
    _require(
        isinstance(run_id, str) and bool(run_id),
        "generic semantic route evidence run_id is invalid",
    )
    try:
        verified = verify_semantic_route_evidence(
            evidence,
            run_id=run_id,
            bound_trial=bound_trial,
            bound_stages=bound_stages,
        )
    except Exception as exc:
        raise PolicyOedBridgeError(
            "generic semantic route evidence failed frozen-input validation"
        ) from exc

    stage_results = verified.get("stage_results")
    _require(
        isinstance(stage_results, list)
        and len(stage_results) == len(bound_stages),
        "generic route stage evidence is incomplete",
    )
    component_ms: dict[str, float] = {}
    bytes_read = 0
    bytes_sent = 0
    for expected, observed in zip(bound_stages, stage_results, strict=True):
        _require(
            observed.get("stage_key") == expected.get("stage_key")
            and observed.get("stage_index") == expected.get("stage_index")
            and observed.get("action") == expected.get("action")
            and observed.get("condition") == expected.get("condition"),
            "generic route stage identity differs from its frozen DAG",
        )
        state = observed.get("state")
        _require(
            state
            in {"EXECUTED", "INACTIVE", "SKIPPED_INACTIVE_CONDITION"},
            "stage state is invalid",
        )
        if state != "EXECUTED":
            continue
        action = _identifier(observed.get("action"), "stage action")
        service_time_ms = _number(
            observed.get("service_time_ms"),
            "stage service_time_ms",
        )
        read = _integer(observed.get("bytes_read"), "stage bytes_read")
        sent = _integer(observed.get("bytes_sent"), "stage bytes_sent")
        component_ms[action] = component_ms.get(action, 0.0) + service_time_ms
        bytes_read += read
        bytes_sent += sent
    component_ms = dict(sorted(component_ms.items()))

    candidate = verified.get("neutral_observation_candidate")
    _require(
        isinstance(candidate, Mapping)
        and set(candidate) == _GENERIC_CANDIDATE_FIELDS
        and candidate.get("schema_version")
        == NEUTRAL_OBSERVATION_CANDIDATE_SCHEMA_VERSION,
        "generic neutral observation candidate fields changed",
    )
    copied_fields = (
        "trial_key",
        "order_index",
        "workload_id",
        "workload_class",
        "design_id",
        "repetition",
        "route_family",
        "executor_node_id",
    )
    _require(
        all(candidate.get(field) == bound_trial.get(field) for field in copied_fields)
        and candidate.get("object_id") == bound_trial.get("artifact_object_id"),
        "generic neutral observation candidate identity changed",
    )
    scoring = verified.get("scoring")
    model_input = verified.get("model_input")
    score_request = verified.get("n1_score_request")
    score_result = verified.get("n1_score_result")
    _require(
        isinstance(scoring, Mapping)
        and isinstance(model_input, Mapping)
        and isinstance(score_request, Mapping)
        and isinstance(score_result, Mapping)
        and type(scoring.get("task_success")) is bool,
        "generic semantic scoring or model-input evidence is missing",
    )
    try:
        authenticated_score = verify_n1_score_result(
            package_dir=n1_oracle_package_dir,
            request=score_request,
            result=score_result,
            evidence_secret=n1_evidence_secret,
        )
    except Exception as exc:
        raise PolicyOedBridgeError(
            "generic semantic route score failed privileged N1 verification"
        ) from exc
    semantic = verified.get("semantic")
    _require(
        isinstance(semantic, Mapping)
        and score_request.get("run_id") == run_id
        and score_request.get("trial_id") == bound_trial.get("trial_key")
        and score_request.get("object_id")
        == bound_trial.get("artifact_object_id")
        and score_request.get("task_binding_sha256")
        == bound_trial.get("public_task_binding_sha256")
        and score_result.get("score_request_id")
        == scoring.get("score_request_id")
        and score_result.get("evaluation_unit_id")
        == scoring.get("evaluation_unit_id")
        and score_result.get("oracle_id") == scoring.get("oracle_id")
        and score_result.get("task_binding_sha256")
        == scoring.get("task_binding_sha256")
        and score_result.get("correct") is scoring.get("task_success")
        and score_result.get("score") == scoring.get("score")
        and score_result.get("score_evidence_hmac_sha256")
        == scoring.get("score_evidence_hmac_sha256")
        and score_result.get("result_content_sha256")
        == scoring.get("result_content_sha256")
        and score_result.get("prediction_sha256")
        == semantic.get("final_answer_sha256")
        and authenticated_score.get("correct") is scoring.get("task_success")
        and authenticated_score.get("score") == scoring.get("score"),
        "generic semantic score is not bound to N6, N1, and the frozen trial",
    )
    candidate_bytes = candidate.get("byte_measurements")
    expected_bytes = {
        "adapter_bytes_read": bytes_read,
        "adapter_bytes_sent": bytes_sent,
        "semantic_input_bytes": _integer(
            model_input.get("payload_size_bytes"),
            "semantic input bytes",
        ),
    }
    _require(
        candidate.get("task_success") is scoring["task_success"]
        and candidate.get("score_authenticity_verified") is True
        and candidate.get("score_authentication") == "n1-hmac-verified"
        and candidate.get("component_service_time_ms") == component_ms
        and isinstance(candidate_bytes, Mapping)
        and set(candidate_bytes) == _GENERIC_BYTE_FIELDS
        and dict(candidate_bytes) == expected_bytes
        and candidate.get("monetary_measurement_available") is False
        and candidate.get("monetary_values_included") is False
        and candidate.get("synthetic_monetary_inputs_consumed") is False
        and candidate.get("credentials_recorded") is False
        and candidate.get("eligible_for_scientific_claims") is False,
        "generic neutral observation candidate is not a neutral projection",
    )
    _number(candidate.get("score"), "generic task score")
    _require(
        candidate.get("score") == scoring.get("score"),
        "generic neutral score differs from authenticated N1 evidence",
    )
    _require(
        verified.get("endpoint_values_included") is False
        and verified.get("credential_values_included") is False
        and verified.get("credentials_recorded") is False
        and verified.get("eligible_for_scientific_claims") is False,
        "generic route evidence crosses its public evidence boundary",
    )
    cache_rows = verified.get("cache_branches")
    _require(isinstance(cache_rows, list), "cache branch evidence is missing")
    cache_branch: str | None = None
    if route["route_family"] == "local-cache-derived":
        expected_branch = "miss" if route["repetition"] == 0 else "hit"
        lookup_stage_keys = {
            stage["stage_key"]
            for stage in bound_stages
            if stage.get("action") == "lookup"
        }
        representation_ids = {
            row["representation_id"]
            for row in verified["artifact_identities"]
        }
        prior_trial_key = None
        if expected_branch == "hit":
            prefix, separator, suffix = route["trial_key"].rpartition("|")
            _require(
                bool(separator) and suffix == f"r{route['repetition']:04d}",
                "cache-hit trial key does not encode its repetition",
            )
            prior_trial_key = f"{prefix}|r{route['repetition'] - 1:04d}"
        _require(
            bool(cache_rows)
            and all(
                row.get("branch") == expected_branch
                and row.get("cache_node_id") == route["executor_node_id"]
                and row.get("source_insert_trial_key") == prior_trial_key
                for row in cache_rows
            )
            and {row.get("lookup_stage_key") for row in cache_rows}
            == lookup_stage_keys
            and {row.get("representation_id") for row in cache_rows}
            == representation_ids,
            "cache branch differs from the frozen repetition lifecycle",
        )
        for row in cache_rows:
            _digest(row.get("lookup_sha256"), "cache lookup SHA-256")
        cache_branch = expected_branch
    else:
        _require(
            cache_rows == [],
            "non-cache route unexpectedly contains cache branch evidence",
        )
    monetary = None
    if cost is not None:
        monetary = {
            "amount": cost["amount"],
            "currency": currency,
            "measurement_sha256": cost["measurement_sha256"],
        }
    return {
        "schema_version": NEUTRAL_OBSERVATION_SCHEMA_VERSION,
        "trial_key": route["trial_key"],
        "source_full_flow_evidence_sha256": _sha256(
            _canonical_bytes(evidence)
        ),
        "source_full_flow_evidence_schema_version": (
            SEMANTIC_ROUTE_EVIDENCE_SCHEMA_VERSION
        ),
        "source_route_row_sha256": _sha256(_canonical_bytes(route)),
        "source_semantic_admission_sha256": admission_sha256,
        "source_bound_trial_sha256": _sha256(_canonical_bytes(bound_trial)),
        "source_stage_dag_sha256": _sha256(_canonical_bytes(bound_stages)),
        "source_route_evidence_commitment_sha256": verified[
            "evidence_sha256"
        ],
        "source_order_index": route["order_index"],
        "workload_id": route["workload_id"],
        "workload_class": route["workload_class"],
        "design_id": route["design_id"],
        "repetition": route["repetition"],
        "object_id": route["object_id"],
        "route_family": route["route_family"],
        "executor_node_id": route["executor_node_id"],
        "cache_branch": cache_branch,
        "task_success": scoring["task_success"],
        "score_authenticity_verified": True,
        "score_authentication": "n1-privileged-offline-hmac-verification",
        "latency_measurements_ms": component_ms,
        "latency_measurement_semantics": (
            "executed-stage-service-time-sums-not-end-to-end"
        ),
        "end_to_end_latency_available": False,
        "byte_measurements": expected_bytes,
        "byte_measurement_semantics": (
            "adapter-read-write-and-semantic-input-not-network-throughput"
        ),
        "monetary_cost_available": cost is not None,
        "monetary_cost": monetary,
        "synthetic_simulator_cost_hints_consumed": False,
        "performance_evidence_claimed": False,
        "scientific_evidence_claimed": False,
        "hidden_label_values_included": False,
        "hidden_label_values_consumed_by_bridge": False,
        "credentials_recorded": False,
    }


def _resolve_observation_evidence(
    *,
    evidence_records: Sequence[Mapping[str, Any]] | None,
    semantic_matrix_run_dir: str | Path | None,
) -> tuple[
    Sequence[Mapping[str, Any]],
    Mapping[str, Any] | None,
]:
    """Resolve exactly one evidence source without recapturing FlowMesh data."""

    explicit = evidence_records is not None
    matrix_run = semantic_matrix_run_dir is not None
    _require(
        explicit != matrix_run,
        "supply exactly one of evidence_records or semantic_matrix_run_dir",
    )
    if explicit:
        assert evidence_records is not None
        return evidence_records, None

    assert semantic_matrix_run_dir is not None
    try:
        loaded = load_full_flow_semantic_matrix_route_evidence(
            semantic_matrix_run_dir
        )
    except FullFlowSemanticMatrixRunnerError as exc:
        raise PolicyOedBridgeError(
            f"semantic matrix run evidence failed verification: {exc}"
        ) from exc
    _require(
        loaded.get("status") == "VERIFIED_PUBLIC_ROUTE_EVIDENCE"
        and loaded.get("route_evidence_count") == 64
        and loaded.get("credentials_recorded") is False
        and loaded.get("eligible_for_scientific_claims") is False,
        "semantic matrix run evidence is incomplete or unsafe",
    )
    for name in (
        "report_sha256",
        "report_file_sha256",
        "route_evidence_file_sha256",
    ):
        _digest(loaded.get(name), f"semantic matrix {name}")
    records = loaded.get("evidence_records")
    _require(
        isinstance(records, Sequence)
        and not isinstance(records, (str, bytes))
        and len(records) == 64,
        "semantic matrix run must provide exactly 64 route-evidence records",
    )
    run_id = loaded.get("run_id")
    _require(
        isinstance(run_id, str) and 1 <= len(run_id) <= 256,
        "semantic matrix run_id is invalid",
    )
    source = {
        "run_id": run_id,
        "report_sha256": loaded["report_sha256"],
        "report_file_sha256": loaded["report_file_sha256"],
        "route_evidence_file_sha256": loaded[
            "route_evidence_file_sha256"
        ],
    }
    return records, source


def _observation_documents(
    *,
    observation_set_id: str,
    evidence_records: Sequence[Mapping[str, Any]],
    route_plan: Mapping[str, Any],
    trial_map: Mapping[str, Mapping[str, Any]],
    external_real_cost_manifest: Mapping[str, Any] | None,
    n1_oracle_package_dir: str | Path | None,
    n1_evidence_secret: bytes | None,
    semantic_execution_admission_dir: str | Path | None,
    semantic_matrix_run_source: Mapping[str, Any] | None,
) -> dict[str, bytes]:
    observation_set_id = _identifier(
        observation_set_id,
        "observation_set_id",
    )
    _require(
        isinstance(evidence_records, Sequence)
        and not isinstance(evidence_records, (str, bytes))
        and bool(evidence_records),
        "evidence_records must be a non-empty array",
    )
    evidence_by_trial: dict[str, Mapping[str, Any]] = {}
    for evidence in evidence_records:
        _require(isinstance(evidence, Mapping), "evidence record must be an object")
        trial_key = evidence.get("trial_key")
        _require(
            isinstance(trial_key, str)
            and trial_key in trial_map
            and trial_key not in evidence_by_trial,
            "evidence has an unknown or duplicate trial key",
        )
        evidence_by_trial[trial_key] = evidence
    generic_present = any(
        evidence.get("schema_version") == SEMANTIC_ROUTE_EVIDENCE_SCHEMA_VERSION
        for evidence in evidence_by_trial.values()
    )
    semantic_inputs: FrozenLocalSemanticExecutionInputs | None = None
    semantic_trials: dict[str, Mapping[str, Any]] = {}
    semantic_stages: dict[str, Mapping[str, Any]] = {}
    if generic_present:
        _require(
            semantic_execution_admission_dir is not None,
            "generic route evidence requires a local semantic admission",
        )
        _require(
            n1_oracle_package_dir is not None
            and n1_evidence_secret is not None,
            "generic route evidence requires privileged N1 verification",
        )
        semantic_inputs, semantic_trials, semantic_stages = (
            _generic_runtime_bindings(
                semantic_execution_admission_dir=(
                    semantic_execution_admission_dir
                ),
                route_plan=route_plan,
                trial_map=trial_map,
            )
        )
    costs: dict[str, Mapping[str, Any]] = {}
    currency: str | None = None
    cost_manifest: dict[str, Any] | None = None
    if external_real_cost_manifest is not None:
        cost_manifest = validate_external_real_cost_manifest(
            external_real_cost_manifest,
            logical_route_plan_sha256=route_plan["plan_sha256"],
            expected_trial_keys=set(evidence_by_trial),
        )
        currency = cost_manifest["currency"]
        costs = {
            entry["trial_key"]: entry
            for entry in cost_manifest["entries"]
        }
    ordered_keys = sorted(
        evidence_by_trial,
        key=lambda key: trial_map[key]["order_index"],
    )
    observations: list[dict[str, Any]] = []
    for key in ordered_keys:
        evidence = evidence_by_trial[key]
        route = trial_map[key]
        if evidence.get("schema_version") == SEMANTIC_ROUTE_EVIDENCE_SCHEMA_VERSION:
            bound_trial = semantic_trials[key]
            bound_stages = [
                semantic_stages[stage_key]
                for stage_key in bound_trial["semantic_stage_keys"]
            ]
            observations.append(_generic_semantic_route_observation(
                evidence,
                route,
                bound_trial=bound_trial,
                bound_stages=bound_stages,
                admission_sha256=semantic_inputs.admission[
                    "admission_sha256"
                ],
                cost=costs.get(key),
                currency=currency,
                n1_oracle_package_dir=n1_oracle_package_dir,
                n1_evidence_secret=n1_evidence_secret,
            ))
        else:
            observations.append(_legacy_full_flow_observation(
                evidence,
                route,
                cost=costs.get(key),
                currency=currency,
                n1_oracle_package_dir=n1_oracle_package_dir,
                n1_evidence_secret=n1_evidence_secret,
            ))
    observations_bytes = _jsonl_document(observations)
    documents: dict[str, bytes] = {OBSERVATIONS_NAME: observations_bytes}
    cost_file_sha256 = None
    cost_manifest_sha256 = None
    if cost_manifest is not None:
        cost_bytes = _json_document(cost_manifest)
        documents[EXTERNAL_COST_NAME] = cost_bytes
        cost_file_sha256 = _sha256(cost_bytes)
        cost_manifest_sha256 = cost_manifest["manifest_sha256"]
    manifest: dict[str, Any] = {
        "schema_version": NEUTRAL_OBSERVATION_MANIFEST_SCHEMA_VERSION,
        "status": "FROZEN_NEUTRAL_FULL_FLOW_OBSERVATIONS",
        "observation_set_id": observation_set_id,
        "logical_route_plan_sha256": route_plan["plan_sha256"],
        "scenario_id": route_plan["scenario_id"],
        "observation_count": len(observations),
        "legacy_full_flow_observation_count": sum(
            evidence.get("schema_version")
            in {
                FULL_FLOW_EVIDENCE_SCHEMA_VERSION,
                FULL_FLOW_EVIDENCE_V2_SCHEMA_VERSION,
            }
            for evidence in evidence_by_trial.values()
        ),
        "generic_semantic_route_observation_count": sum(
            evidence.get("schema_version")
            == SEMANTIC_ROUTE_EVIDENCE_SCHEMA_VERSION
            for evidence in evidence_by_trial.values()
        ),
        "evidence_source_kind": (
            "verified-semantic-matrix-run"
            if semantic_matrix_run_source is not None
            else "explicit-records"
        ),
        "semantic_matrix_run_id": (
            semantic_matrix_run_source["run_id"]
            if semantic_matrix_run_source is not None
            else None
        ),
        "semantic_matrix_run_report_sha256": (
            semantic_matrix_run_source["report_sha256"]
            if semantic_matrix_run_source is not None
            else None
        ),
        "semantic_matrix_run_report_file_sha256": (
            semantic_matrix_run_source["report_file_sha256"]
            if semantic_matrix_run_source is not None
            else None
        ),
        "semantic_matrix_route_evidence_file_sha256": (
            semantic_matrix_run_source["route_evidence_file_sha256"]
            if semantic_matrix_run_source is not None
            else None
        ),
        "semantic_matrix_run_integrity_verified": (
            semantic_matrix_run_source is not None
        ),
        "semantic_execution_admission_sha256": (
            semantic_inputs.admission["admission_sha256"]
            if semantic_inputs is not None
            else None
        ),
        "trial_keys_sha256": _sha256(_canonical_bytes(ordered_keys)),
        "observations_file_sha256": _sha256(observations_bytes),
        "task_success_available": True,
        "all_score_authenticity_verified": all(
            row["score_authenticity_verified"] for row in observations
        ),
        "hidden_v2_score_authentication_required": True,
        "component_latency_available": True,
        "end_to_end_latency_available": False,
        "measured_bytes_available": True,
        "monetary_cost_available": cost_manifest is not None,
        "external_real_cost_manifest_sha256": cost_manifest_sha256,
        "external_real_cost_file_sha256": cost_file_sha256,
        "external_cost_claim_independently_verified": False,
        "synthetic_simulator_cost_hints_consumed": False,
        "hidden_label_values_included": False,
        "hidden_label_values_consumed_by_bridge": False,
        "component_measurements_are_performance_claims": False,
        "performance_analysis_performed": False,
        "generic_route_source_trial_stage_binding_verified": generic_present,
        "statistical_analysis_performed": False,
        "awm_oed_mathematics_modified": False,
        "endpoint_free": True,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    manifest["observation_manifest_sha256"] = _sha256(
        _canonical_bytes(manifest)
    )
    documents[OBSERVATION_MANIFEST_NAME] = _json_document(manifest)
    _assert_endpoint_credential_free([manifest, observations, cost_manifest])
    return documents


def freeze_full_flow_observations(
    *,
    logical_route_plan_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    observation_set_id: str,
    evidence_records: Sequence[Mapping[str, Any]] | None = None,
    output_dir: str | Path,
    external_real_cost_manifest: Mapping[str, Any] | None = None,
    n1_oracle_package_dir: str | Path | None = None,
    n1_evidence_secret: bytes | None = None,
    semantic_execution_admission_dir: str | Path | None = None,
    semantic_matrix_run_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Freeze neutral observations; never derive simulator monetary cost.

    Supply exactly one evidence source: explicit ``evidence_records`` or a
    frozen ``semantic_matrix_run_dir``.  The latter is reverified from its
    checksums and checkpoints, so raw FlowMesh task results are not recaptured.
    ``semantic_execution_admission_dir`` is required for the promoted generic
    64-trial route-evidence schema. Generic evidence also requires the frozen
    N1 package and runtime-only evidence secret so task success is replayed
    cryptographically rather than trusted from an archived boolean. Legacy
    N4--N7--N6 evidence keeps its original validation path unchanged.
    """

    _require(
        (n1_oracle_package_dir is None) == (n1_evidence_secret is None),
        "N1 package and evidence secret must be supplied together",
    )
    plan, _, trial_map = _verified_routes(
        logical_route_plan_dir=logical_route_plan_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    resolved_evidence, matrix_source = _resolve_observation_evidence(
        evidence_records=evidence_records,
        semantic_matrix_run_dir=semantic_matrix_run_dir,
    )
    documents = _observation_documents(
        observation_set_id=observation_set_id,
        evidence_records=resolved_evidence,
        route_plan=plan,
        trial_map=trial_map,
        external_real_cost_manifest=external_real_cost_manifest,
        n1_oracle_package_dir=n1_oracle_package_dir,
        n1_evidence_secret=n1_evidence_secret,
        semantic_execution_admission_dir=semantic_execution_admission_dir,
        semantic_matrix_run_source=matrix_source,
    )
    target = _publish(output_dir, documents)
    verified = verify_full_flow_observations(
        observation_dir=target,
        logical_route_plan_dir=logical_route_plan_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
        evidence_records=evidence_records,
        n1_oracle_package_dir=n1_oracle_package_dir,
        n1_evidence_secret=n1_evidence_secret,
        semantic_execution_admission_dir=semantic_execution_admission_dir,
        semantic_matrix_run_dir=semantic_matrix_run_dir,
    )
    return {**verified, "output_dir": str(target)}


def verify_full_flow_observations(
    *,
    observation_dir: str | Path,
    logical_route_plan_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    evidence_records: Sequence[Mapping[str, Any]] | None = None,
    n1_oracle_package_dir: str | Path | None = None,
    n1_evidence_secret: bytes | None = None,
    semantic_execution_admission_dir: str | Path | None = None,
    semantic_matrix_run_dir: str | Path | None = None,
) -> dict[str, Any]:
    _require(
        (n1_oracle_package_dir is None) == (n1_evidence_secret is None),
        "N1 package and evidence secret must be supplied together",
    )
    root = Path(observation_dir).resolve()
    _require(root.is_dir(), "bridge output directory does not exist")
    entries = list(root.iterdir())
    _require(
        all(path.is_file() and not path.is_symlink() for path in entries),
        "bridge output must contain regular files only",
    )
    observed_names = {path.name for path in entries}
    without_cost = {
        OBSERVATION_MANIFEST_NAME,
        OBSERVATIONS_NAME,
        CHECKSUMS_NAME,
    }
    with_cost = without_cost | {EXTERNAL_COST_NAME}
    _require(
        observed_names in (without_cost, with_cost),
        "bridge output file set changed",
    )
    _verify_files(root, observed_names)
    manifest = _read_json(
        root / OBSERVATION_MANIFEST_NAME,
        "neutral observation manifest",
    )
    _require(
        manifest.get("schema_version")
        == NEUTRAL_OBSERVATION_MANIFEST_SCHEMA_VERSION
        and manifest.get("status")
        == "FROZEN_NEUTRAL_FULL_FLOW_OBSERVATIONS",
        "neutral observation manifest schema or status changed",
    )
    cost_available = manifest.get("monetary_cost_available")
    _require(type(cost_available) is bool, "monetary_cost_available is invalid")
    _require(
        (EXTERNAL_COST_NAME in observed_names) is cost_available,
        "real-cost file presence disagrees with observation manifest",
    )
    _manifest_digest(manifest, "observation_manifest_sha256")
    cost_manifest = (
        _read_json(root / EXTERNAL_COST_NAME, "external real-cost manifest")
        if cost_available
        else None
    )
    plan, _, trial_map = _verified_routes(
        logical_route_plan_dir=logical_route_plan_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    resolved_evidence, matrix_source = _resolve_observation_evidence(
        evidence_records=evidence_records,
        semantic_matrix_run_dir=semantic_matrix_run_dir,
    )
    expected = _observation_documents(
        observation_set_id=manifest.get("observation_set_id"),
        evidence_records=resolved_evidence,
        route_plan=plan,
        trial_map=trial_map,
        external_real_cost_manifest=cost_manifest,
        n1_oracle_package_dir=n1_oracle_package_dir,
        n1_evidence_secret=n1_evidence_secret,
        semantic_execution_admission_dir=semantic_execution_admission_dir,
        semantic_matrix_run_source=matrix_source,
    )
    _require(
        all(
            (root / name).read_bytes() == payload
            for name, payload in expected.items()
        ),
        "neutral observations do not match their bound full-flow evidence",
    )
    return {
        "status": "VERIFIED",
        "observation_set_id": manifest["observation_set_id"],
        "observation_manifest_sha256": manifest[
            "observation_manifest_sha256"
        ],
        "logical_route_plan_sha256": plan["plan_sha256"],
        "observation_count": manifest["observation_count"],
        "monetary_cost_available": cost_available,
        "all_score_authenticity_verified": manifest[
            "all_score_authenticity_verified"
        ],
        "legacy_full_flow_observation_count": manifest[
            "legacy_full_flow_observation_count"
        ],
        "generic_semantic_route_observation_count": manifest[
            "generic_semantic_route_observation_count"
        ],
        "semantic_execution_admission_sha256": manifest[
            "semantic_execution_admission_sha256"
        ],
        "evidence_source_kind": manifest["evidence_source_kind"],
        "semantic_matrix_run_id": manifest["semantic_matrix_run_id"],
        "semantic_matrix_run_report_sha256": manifest[
            "semantic_matrix_run_report_sha256"
        ],
        "semantic_matrix_route_evidence_file_sha256": manifest[
            "semantic_matrix_route_evidence_file_sha256"
        ],
        "semantic_matrix_run_integrity_verified": manifest[
            "semantic_matrix_run_integrity_verified"
        ],
        "synthetic_simulator_cost_hints_consumed": False,
        "performance_analysis_performed": False,
        "statistical_analysis_performed": False,
        "eligible_for_scientific_claims": False,
        "credentials_recorded": False,
    }


__all__ = [
    "CHECKSUMS_NAME",
    "EXTERNAL_COST_NAME",
    "EXTERNAL_REAL_COST_MANIFEST_SCHEMA_VERSION",
    "NEUTRAL_OBSERVATION_MANIFEST_SCHEMA_VERSION",
    "NEUTRAL_OBSERVATION_SCHEMA_VERSION",
    "OBSERVATION_MANIFEST_NAME",
    "OBSERVATIONS_NAME",
    "OED_MANIFEST_NAME",
    "OED_PROSPECTIVE_PLAN_SCHEMA_VERSION",
    "OED_ROUTES_NAME",
    "POLICY_ASSIGNMENT_SCHEMA_VERSION",
    "POLICY_MANIFEST_NAME",
    "POLICY_ROUTES_NAME",
    "PolicyOedBridgeError",
    "freeze_full_flow_observations",
    "freeze_oed_prospective_selection",
    "freeze_policy_assignment",
    "validate_external_real_cost_manifest",
    "verify_full_flow_observations",
    "verify_oed_prospective_selection",
    "verify_policy_assignment",
]
