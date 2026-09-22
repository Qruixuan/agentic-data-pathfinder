"""Representative runtime gate for the promoted local semantic matrix.

Ten source-bound trials exercise raw, indexed-raw, remote-derived, cache
miss, and cache-hit routes on both N7 and N8 before the 64-trial matrix may
be submitted.  The
gate records only the strict neutral executor result: no answer, hidden label,
endpoint, credential, or payload is persisted.  A wrong model answer is not a
gate failure; successfully obtaining an authenticated N1 score is the
interoperability property under test and avoids selecting a model on visible
smoke accuracy.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, TypedDict
from urllib.parse import urlsplit

from .full_flow_local_semantic_admission import (
    ADMISSION_NAME as SOURCE_ADMISSION_NAME,
    CHECKSUMS_NAME as SOURCE_CHECKSUMS_NAME,
    SMOKES_NAME as SOURCE_SMOKES_NAME,
    FrozenLocalSemanticExecutionInputs,
    load_full_flow_local_semantic_execution_inputs,
)
from .full_flow_matrix_runner import (
    SemanticTrialExecutionError,
    SemanticTrialExecutor,
    validate_semantic_trial_result,
)
from .full_flow_deployment import verify_full_flow_deployment_binding
from .full_flow_n4_serve_gate import (
    CHECKSUMS_NAME as N4_GATE_CHECKSUMS_NAME,
    GATE_NAME as N4_GATE_NAME,
    verify_full_flow_n4_preprovisioned_serve_gate,
)
from .full_flow_n4_live_serve_gate import (
    CHECKSUMS_NAME as N4_LIVE_GATE_CHECKSUMS_NAME,
    GATE_NAME as N4_LIVE_GATE_NAME,
    verify_full_flow_n4_live_serve_gate,
)
from .full_flow_one_case import (
    CHECKSUMS_NAME as ONE_CASE_CHECKSUMS_NAME,
    PLAN_NAME as ONE_CASE_PLAN_NAME,
    TRIALS_NAME as ONE_CASE_TRIALS_NAME,
    FrozenFullFlowOneCasePlan,
    load_full_flow_one_case_plan,
)


SMOKE_RECEIPT_SCHEMA_VERSION = (
    "pathfinder.full-flow-local-semantic-smoke-receipt/v1alpha2"
)
SMOKE_RESULT_SCHEMA_VERSION = (
    "pathfinder.full-flow-local-semantic-smoke-result/v1alpha1"
)
ONE_CASE_RECEIPT_SCHEMA_VERSION = (
    "pathfinder.full-flow-one-case-run-receipt/v1alpha1"
)
RECEIPT_NAME = "local-semantic-smoke-receipt.json"
RESULTS_NAME = "local-semantic-smoke-results.jsonl"
CHECKSUMS_NAME = "SHA256SUMS"

_CONTENT = {RECEIPT_NAME, RESULTS_NAME}
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}\Z")
_CASE_ORDER = (
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
)

_PREPROVISIONED_GATE_KIND = "preprovisioned-snapshot"
_LIVE_GATE_KIND = "live-n5-publication"
_LIVE_GATE_SOURCE_FIELDS = {
    "live_receipt_bindings",
    "n4_publication_store_root",
    "rebound_artifact_binding_dir",
    "rebound_semantic_matrix_dir",
    "rebound_admission_dir",
}


class N4LiveServeGateSources(TypedDict):
    """Current sources needed to replay a live N5 -> N4 serve gate."""

    live_receipt_bindings: Sequence[Mapping[str, Any]]
    n4_publication_store_root: str | Path
    rebound_artifact_binding_dir: str | Path
    rebound_semantic_matrix_dir: str | Path
    rebound_admission_dir: str | Path


class FullFlowLocalSemanticSmokeError(ValueError):
    """Raised when the representative semantic gate is incomplete."""


class FullFlowLocalSemanticSmokeExecutionError(
    FullFlowLocalSemanticSmokeError
):
    """A sanitized failure from one representative external execution."""

    def __init__(
        self,
        *,
        case_id: str,
        trial_key: str,
        failure_class: str,
        failure_code: str,
    ) -> None:
        super().__init__(
            "representative semantic smoke failed; "
            f"case={case_id}; trial={trial_key}; "
            f"class={failure_class}; code={failure_code}"
        )
        self.case_id = case_id
        self.trial_key = trial_key
        self.failure_class = failure_class
        self.failure_code = failure_code


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowLocalSemanticSmokeError(message)


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FullFlowLocalSemanticSmokeError(
            "smoke evidence is not canonical JSON"
        ) from exc


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


def _jsonl_bytes(rows: list[Mapping[str, Any]]) -> bytes:
    return b"".join(_canonical(row) + b"\n" for row in rows)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _digest(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{name} is not lowercase SHA-256",
    )
    return str(value)


def _identifier(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
        f"{name} is invalid",
    )
    return str(value)


def _normal_live_gate_sources(
    value: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if value is None:
        return None
    _require(
        isinstance(value, Mapping) and set(value) == _LIVE_GATE_SOURCE_FIELDS,
        "live N4 serve-gate sources are incomplete or contain extra fields",
    )
    bindings = value.get("live_receipt_bindings")
    _require(
        isinstance(bindings, Sequence)
        and not isinstance(bindings, (str, bytes, bytearray))
        and bool(bindings)
        and all(isinstance(row, Mapping) for row in bindings),
        "live N4 receipt bindings are invalid",
    )
    return {
        "live_receipt_bindings": tuple(dict(row) for row in bindings),
        "n4_publication_store_root": Path(
            value["n4_publication_store_root"]
        ).resolve(),
        "rebound_artifact_binding_dir": Path(
            value["rebound_artifact_binding_dir"]
        ).resolve(),
        "rebound_semantic_matrix_dir": Path(
            value["rebound_semantic_matrix_dir"]
        ).resolve(),
        "rebound_admission_dir": Path(
            value["rebound_admission_dir"]
        ).resolve(),
    }


def _verify_n4_serve_gate(
    n4_gate_root: Path,
    *,
    preprovisioned_sources: Mapping[str, Path],
    live_sources: Mapping[str, Any] | None,
    admission_root: Path,
    artifact_binding_root: Path,
) -> dict[str, Any]:
    """Verify exactly one N4 gate mode and normalize its safe claims."""

    normalized_live = _normal_live_gate_sources(live_sources)
    if normalized_live is None:
        try:
            gate = verify_full_flow_n4_preprovisioned_serve_gate(
                n4_gate_root,
                **preprovisioned_sources,
            )
        except Exception as exc:
            raise FullFlowLocalSemanticSmokeError(
                "source-bound N4 preprovisioned serve gate failed"
            ) from exc
        _require(
            gate.get("status") == "VERIFIED"
            and gate.get("serve_profile_authorized") is True
            and gate.get("authorized_compose_profile") == "serve-frozen"
            and gate.get("preprovisioned_snapshot_used") is True
            and gate.get("live_n5_materialization_executed") is False
            and gate.get("source_binding_checked") is True,
            "N4 preprovisioned gate does not authorize frozen serving",
        )
        return {
            **dict(gate),
            "gate_kind": _PREPROVISIONED_GATE_KIND,
            "gate_file_name": N4_GATE_NAME,
            "gate_checksums_name": N4_GATE_CHECKSUMS_NAME,
            "publication_companion_excluded": True,
            "rebound_inputs_verified": False,
        }

    _require(
        normalized_live["rebound_admission_dir"] == admission_root,
        "live N4 gate binds another rebound semantic admission",
    )
    _require(
        normalized_live["rebound_artifact_binding_dir"]
        == artifact_binding_root,
        "live N4 gate binds another rebound artifact-binding package",
    )
    try:
        gate = verify_full_flow_n4_live_serve_gate(
            n4_gate_root,
            **normalized_live,
        )
    except Exception as exc:
        raise FullFlowLocalSemanticSmokeError(
            "source-bound live N5-to-N4 serve gate failed"
        ) from exc
    _require(
        gate.get("status") == "VERIFIED"
        and gate.get("authorized_compose_profile") == "serve-frozen"
        and gate.get("live_n5_materialization_executed") is True
        and gate.get("publication_companion_excluded") is True
        and gate.get("n4_data_agent_rebind_inputs_verified") is True
        and gate.get("n4_data_agent_runtime_rebind_executed") is False
        and gate.get("source_binding_checked") is True,
        "live N4 gate does not prove immutable rebound frozen serving",
    )
    return {
        **dict(gate),
        "serve_profile_authorized": True,
        "preprovisioned_snapshot_used": False,
        "gate_kind": _LIVE_GATE_KIND,
        "gate_file_name": N4_LIVE_GATE_NAME,
        "gate_checksums_name": N4_LIVE_GATE_CHECKSUMS_NAME,
        "rebound_inputs_verified": True,
    }


def _strict_json(path: Path, name: str) -> dict[str, Any]:
    _require(path.is_file() and not path.is_symlink(), f"{name} is missing")

    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            _require(key not in result, f"{name} repeats key {key}")
            result[key] = value
        return result

    try:
        value = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=unique,
            parse_constant=lambda token: (_ for _ in ()).throw(
                FullFlowLocalSemanticSmokeError(
                    f"{name} contains invalid number {token}"
                )
            ),
        )
    except FullFlowLocalSemanticSmokeError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowLocalSemanticSmokeError(f"cannot read {name}") from exc
    _require(isinstance(value, dict), f"{name} must be an object")
    return value


def _strict_jsonl(path: Path, name: str) -> list[dict[str, Any]]:
    _require(path.is_file() and not path.is_symlink(), f"{name} is missing")
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FullFlowLocalSemanticSmokeError(f"cannot read {name}") from exc
    _require(lines and all(line.strip() for line in lines), f"{name} is empty")
    rows: list[dict[str, Any]] = []
    for position, line in enumerate(lines, start=1):
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise FullFlowLocalSemanticSmokeError(
                f"{name} line {position} is invalid"
            ) from exc
        _require(isinstance(row, dict), f"{name} row is not an object")
        rows.append(row)
    return rows


def _smoke_rows(
    inputs: FrozenLocalSemanticExecutionInputs,
    one_case: FrozenFullFlowOneCasePlan | None = None,
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    if one_case is None:
        selection = inputs.representative_smokes
    else:
        selection = tuple({
            "case_id": row.get("case_id"),
            "trial_key": row.get("trial_key"),
            "expected_executor_node_id": row.get("executor_node_id"),
            "expected_cache_branch": row.get("expected_cache_branch"),
            "prerequisite_trial_key": row.get("prerequisite_trial_key"),
            "flowmesh_submission_authorized": True,
            "runtime_gate_state": "REQUIRED_NOT_EXECUTED",
            "semantic_execution_performed": False,
        } for row in one_case.trials)
    by_case = {
        str(row.get("case_id")): dict(row)
        for row in selection
    }
    _require(
        set(by_case) == set(_CASE_ORDER) and len(by_case) == len(_CASE_ORDER),
        "representative smoke selection changed",
    )
    by_trial = {
        str(row.get("trial_key")): dict(row)
        for row in inputs.bound_trials
    }
    result: list[tuple[dict[str, Any], dict[str, Any]]] = []
    completed: set[str] = set()
    for case_id in _CASE_ORDER:
        smoke = by_case[case_id]
        trial_key = str(smoke.get("trial_key"))
        _require(trial_key in by_trial, "smoke trial is absent from admission")
        trial = by_trial[trial_key]
        _require(
            smoke.get("flowmesh_submission_authorized") is True
            and (
                one_case is None
                or trial.get("flowmesh_submission_authorized") is True
            )
            and smoke.get("runtime_gate_state") == "REQUIRED_NOT_EXECUTED"
            and smoke.get("semantic_execution_performed") is False,
            "smoke authorization contract changed",
        )
        expected_executor = smoke.get("expected_executor_node_id")
        _require(
            expected_executor in {"N7", "N8"}
            and trial.get("executor_node_id") == expected_executor,
            "smoke executor-node coverage changed",
        )
        prerequisite = smoke.get("prerequisite_trial_key")
        if prerequisite is not None:
            _require(
                prerequisite in completed,
                "smoke prerequisite is not ordered before its dependent case",
            )
        result.append((smoke, trial))
        completed.add(trial_key)
    return result


def _idempotency_key(
    *,
    admission_sha256: str,
    run_id: str,
    case_id: str,
    trial_key: str,
) -> str:
    return _sha256(_canonical({
        "domain": "pathfinder.local-semantic-representative-smoke/v1",
        "admission_sha256": admission_sha256,
        "run_id": run_id,
        "case_id": case_id,
        "trial_key": trial_key,
    }))


def _result_row(
    *,
    smoke: Mapping[str, Any],
    result: Mapping[str, Any],
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "schema_version": SMOKE_RESULT_SCHEMA_VERSION,
        "case_id": smoke["case_id"],
        "trial_key": smoke["trial_key"],
        "expected_executor_node_id": smoke["expected_executor_node_id"],
        "expected_cache_branch": smoke.get("expected_cache_branch"),
        "prerequisite_trial_key": smoke.get("prerequisite_trial_key"),
        "result": dict(result),
        "result_sha256": _sha256(_canonical(result)),
        "task_success_required_for_gate": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    row["smoke_result_sha256"] = _sha256(_canonical(row))
    return row


def _semantic_input_invariants(
    inputs: FrozenLocalSemanticExecutionInputs,
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    profile_count = sum(
        "semantic_input_profile" in trial for trial in inputs.bound_trials
    )
    _require(
        profile_count in {0, len(inputs.bound_trials)},
        "semantic input profiles are only partially frozen",
    )
    if profile_count == 0:
        return {
            "semantic_input_profiles_verified": False,
            "execution_location_semantic_invariance_verified": False,
            "cache_state_semantic_invariance_verified": False,
            "route_family_semantic_separation_verified": False,
            "semantic_input_content_sha256_by_case": {},
        }

    content_by_case: dict[str, str] = {}
    profile_by_case: dict[str, str] = {}
    for row in rows:
        case_id = str(row.get("case_id"))
        result = row.get("result")
        evidence = (
            result.get("semantic_route_evidence")
            if isinstance(result, Mapping)
            else None
        )
        model_input = (
            evidence.get("model_input")
            if isinstance(evidence, Mapping)
            else None
        )
        _require(
            isinstance(model_input, Mapping)
            and evidence.get("semantic_input_profile_verified") is True
            and model_input.get("semantic_input_profile_verified") is True,
            f"semantic input profile evidence is missing for {case_id}",
        )
        content_by_case[case_id] = _digest(
            model_input.get("semantic_content_sha256"),
            f"{case_id} semantic_content_sha256",
        )
        profile_by_case[case_id] = _digest(
            model_input.get("semantic_input_profile_sha256"),
            f"{case_id} semantic_input_profile_sha256",
        )
    _require(
        set(content_by_case) == set(_CASE_ORDER),
        "semantic input evidence does not cover every smoke case",
    )

    location_pairs = (
        ("n7-raw", "n8-raw"),
        ("n7-indexed-raw", "n8-indexed-raw"),
        ("n7-remote-derived", "n8-remote-derived"),
        ("n7-cache-miss", "n8-cache-miss"),
        ("n7-cache-hit", "n8-cache-hit"),
    )
    _require(
        all(
            content_by_case[left] == content_by_case[right]
            and profile_by_case[left] == profile_by_case[right]
            for left, right in location_pairs
        ),
        "N7 and N8 changed semantic input content",
    )
    cache_pairs = (
        ("n7-cache-miss", "n7-cache-hit"),
        ("n8-cache-miss", "n8-cache-hit"),
    )
    _require(
        all(
            content_by_case[left] == content_by_case[right]
            and profile_by_case[left] == profile_by_case[right]
            for left, right in cache_pairs
        ),
        "cache hit and miss changed semantic input content",
    )
    representatives = {
        content_by_case["n7-raw"],
        content_by_case["n7-indexed-raw"],
        content_by_case["n7-remote-derived"],
    }
    _require(
        len(representatives) == 3,
        "raw, indexed, and derived routes do not have three distinct "
        "semantic inputs",
    )
    return {
        "semantic_input_profiles_verified": True,
        "execution_location_semantic_invariance_verified": True,
        "cache_state_semantic_invariance_verified": True,
        "route_family_semantic_separation_verified": True,
        "semantic_input_content_sha256_by_case": dict(sorted(
            content_by_case.items()
        )),
    }


def _receipt(
    *,
    inputs: FrozenLocalSemanticExecutionInputs,
    source_root: Path,
    n4_gate_root: Path,
    n4_gate: Mapping[str, Any],
    run_id: str,
    rows: list[Mapping[str, Any]],
    one_case_root: Path | None = None,
    one_case: FrozenFullFlowOneCasePlan | None = None,
) -> dict[str, Any]:
    admission = inputs.admission
    is_one_case = one_case_root is not None and one_case is not None
    _require(
        (one_case_root is None) is (one_case is None),
        "one-case receipt inputs are incomplete",
    )
    selection_path = (
        source_root / SOURCE_SMOKES_NAME
        if not is_one_case
        else one_case_root / ONE_CASE_TRIALS_NAME
    )
    semantic_invariants = _semantic_input_invariants(inputs, rows)
    document: dict[str, Any] = {
        "schema_version": (
            SMOKE_RECEIPT_SCHEMA_VERSION
            if not is_one_case
            else ONE_CASE_RECEIPT_SCHEMA_VERSION
        ),
        "status": "COMPLETE",
        "run_id": run_id,
        "promotion_id": admission["promotion_id"],
        "admission_sha256": admission["admission_sha256"],
        "admission_file_sha256": _sha256(
            (source_root / SOURCE_ADMISSION_NAME).read_bytes()
        ),
        "admission_checksums_sha256": _sha256(
            (source_root / SOURCE_CHECKSUMS_NAME).read_bytes()
        ),
        "smoke_selection_file_sha256": _sha256(
            selection_path.read_bytes()
        ),
        "n4_serve_gate_id": n4_gate["gate_id"],
        "n4_serve_gate_sha256": n4_gate["gate_sha256"],
        "n4_serve_gate_kind": n4_gate["gate_kind"],
        "n4_serve_gate_file_name": n4_gate["gate_file_name"],
        "n4_serve_gate_file_sha256": _sha256(
            (n4_gate_root / n4_gate["gate_file_name"]).read_bytes()
        ),
        "n4_serve_gate_checksums_sha256": _sha256(
            (n4_gate_root / n4_gate["gate_checksums_name"]).read_bytes()
        ),
        "n4_authorized_compose_profile": "serve-frozen",
        "n4_source_binding_checked": True,
        "n4_publication_companion_excluded": True,
        "n4_rebound_inputs_verified": n4_gate["rebound_inputs_verified"],
        "n4_preprovisioned_snapshot_used": n4_gate[
            "preprovisioned_snapshot_used"
        ],
        "n4_live_materialization_executed": n4_gate[
            "live_n5_materialization_executed"
        ],
        "case_ids": list(_CASE_ORDER),
        "smoke_count": len(rows),
        "results_file": RESULTS_NAME,
        "results_file_sha256": _sha256(_jsonl_bytes(rows)),
        "all_routes_completed": True,
        "all_n1_score_authenticity_verified": True,
        "all_telemetry_complete": True,
        "all_flowmesh_transport": True,
        "all_llm_calls_completed": True,
        **semantic_invariants,
        "task_success_required_for_gate": False,
        "full_matrix_runtime_gate_satisfied": not is_one_case,
        "full_matrix_submission_authorized": not is_one_case,
        "w4_task_semantics": (
            "multiple-choice-placeholder"
            if not is_one_case
            else "source-bound-public-one-case"
        ),
        "w4_retrieval_quality_evaluated": False,
        "performance_measured": False,
        "monetary_cost_measured": False,
        "upcloud_ready": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    if is_one_case:
        assert one_case is not None
        assert one_case_root is not None
        document.update({
            "selection_kind": "engineering-demonstration",
            "one_case_execution_complete": True,
            "one_case_plan_sha256": one_case.plan["plan_sha256"],
            "one_case_plan_file_sha256": _sha256(
                (one_case_root / ONE_CASE_PLAN_NAME).read_bytes()
            ),
            "one_case_checksums_file_sha256": _sha256(
                (one_case_root / ONE_CASE_CHECKSUMS_NAME).read_bytes()
            ),
            "one_case_id": one_case.plan["case_id"],
            "one_case_workload_id": one_case.plan["workload_id"],
            "one_case_artifact_object_id": one_case.plan[
                "artifact_object_id"
            ],
            "one_case_safe_design_id": one_case.plan["safe_design_id"],
            "formal_sampling_claimed": False,
        })
    document["receipt_sha256"] = _sha256(_canonical(document))
    return document


def _verify_files(
    root: Path,
    *,
    local_semantic_admission_dir: Path,
    n4_serve_gate_dir: Path,
    n4_gate_sources: Mapping[str, Path],
    n4_live_gate_sources: Mapping[str, Any] | None,
    one_case_plan_dir: Path | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    _require(root.is_dir(), "semantic smoke receipt directory is missing")
    files = list(root.iterdir())
    _require(
        {path.name for path in files} == _CONTENT | {CHECKSUMS_NAME}
        and all(path.is_file() and not path.is_symlink() for path in files),
        "semantic smoke receipt file set changed",
    )
    expected_checksums = b"".join(
        f"{_sha256((root / name).read_bytes())}  {name}\n".encode("utf-8")
        for name in sorted(_CONTENT)
    )
    _require(
        (root / CHECKSUMS_NAME).read_bytes() == expected_checksums,
        "semantic smoke receipt checksums failed",
    )
    inputs = load_full_flow_local_semantic_execution_inputs(
        local_semantic_admission_dir
    )
    one_case = (
        None
        if one_case_plan_dir is None
        else load_full_flow_one_case_plan(
            one_case_plan_dir,
            local_semantic_admission_dir=local_semantic_admission_dir,
        )
    )
    n4_gate = _verify_n4_serve_gate(
        n4_serve_gate_dir,
        preprovisioned_sources=n4_gate_sources,
        live_sources=n4_live_gate_sources,
        admission_root=local_semantic_admission_dir,
        artifact_binding_root=n4_gate_sources["artifact_binding_dir"],
    )
    selections = _smoke_rows(inputs, one_case)
    receipt = _strict_json(root / RECEIPT_NAME, "semantic smoke receipt")
    rows = _strict_jsonl(root / RESULTS_NAME, "semantic smoke results")
    _require(
        len(rows) == len(_CASE_ORDER),
        "semantic smoke result count changed",
    )
    supplied = _digest(receipt.get("receipt_sha256"), "receipt_sha256")
    unsigned = dict(receipt)
    del unsigned["receipt_sha256"]
    _require(
        supplied == _sha256(_canonical(unsigned)),
        "semantic smoke receipt digest failed",
    )
    _require(
        receipt.get("schema_version")
        == (
            SMOKE_RECEIPT_SCHEMA_VERSION
            if one_case is None
            else ONE_CASE_RECEIPT_SCHEMA_VERSION
        )
        and receipt.get("status") == "COMPLETE"
        and receipt.get("case_ids") == list(_CASE_ORDER)
        and receipt.get("smoke_count") == len(_CASE_ORDER)
        and receipt.get("full_matrix_runtime_gate_satisfied")
        is (one_case is None)
        and receipt.get("full_matrix_submission_authorized")
        is (one_case is None)
        and receipt.get("task_success_required_for_gate") is False
        and receipt.get("w4_retrieval_quality_evaluated") is False
        and receipt.get("performance_measured") is False
        and receipt.get("monetary_cost_measured") is False
        and receipt.get("upcloud_ready") is False
        and receipt.get("credentials_recorded") is False,
        "semantic smoke gate claims changed",
    )
    admission = inputs.admission
    _require(
        receipt.get("promotion_id") == admission.get("promotion_id")
        and receipt.get("admission_sha256")
        == admission.get("admission_sha256")
        and receipt.get("admission_file_sha256")
        == _sha256(
            (local_semantic_admission_dir / SOURCE_ADMISSION_NAME).read_bytes()
        )
        and receipt.get("admission_checksums_sha256")
        == _sha256(
            (local_semantic_admission_dir / SOURCE_CHECKSUMS_NAME).read_bytes()
        )
        and receipt.get("smoke_selection_file_sha256")
        == _sha256(
            (
                local_semantic_admission_dir / SOURCE_SMOKES_NAME
                if one_case is None
                else one_case_plan_dir / ONE_CASE_TRIALS_NAME
            ).read_bytes()
        ),
        "semantic smoke receipt binds another admission",
    )
    if one_case is not None:
        assert one_case_plan_dir is not None
        _require(
            receipt.get("selection_kind")
            == "engineering-demonstration"
            and receipt.get("one_case_execution_complete") is True
            and receipt.get("one_case_plan_sha256")
            == one_case.plan.get("plan_sha256")
            and receipt.get("one_case_plan_file_sha256")
            == _sha256(
                (one_case_plan_dir / ONE_CASE_PLAN_NAME).read_bytes()
            )
            and receipt.get("one_case_checksums_file_sha256")
            == _sha256(
                (one_case_plan_dir / ONE_CASE_CHECKSUMS_NAME).read_bytes()
            )
            and receipt.get("one_case_id")
            == one_case.plan.get("case_id")
            and receipt.get("one_case_workload_id")
            == one_case.plan.get("workload_id")
            and receipt.get("one_case_artifact_object_id")
            == one_case.plan.get("artifact_object_id")
            and receipt.get("one_case_safe_design_id")
            == one_case.plan.get("safe_design_id")
            and receipt.get("formal_sampling_claimed") is False,
            "semantic smoke receipt binds another one-case plan",
        )
    _require(
        receipt.get("n4_serve_gate_id") == n4_gate.get("gate_id")
        and receipt.get("n4_serve_gate_sha256")
        == n4_gate.get("gate_sha256")
        and receipt.get("n4_serve_gate_kind") == n4_gate.get("gate_kind")
        and receipt.get("n4_serve_gate_file_name")
        == n4_gate.get("gate_file_name")
        and receipt.get("n4_serve_gate_file_sha256")
        == _sha256(
            (n4_serve_gate_dir / n4_gate["gate_file_name"]).read_bytes()
        )
        and receipt.get("n4_serve_gate_checksums_sha256")
        == _sha256(
            (
                n4_serve_gate_dir / n4_gate["gate_checksums_name"]
            ).read_bytes()
        )
        and receipt.get("n4_authorized_compose_profile") == "serve-frozen"
        and receipt.get("n4_source_binding_checked") is True
        and receipt.get("n4_publication_companion_excluded") is True
        and receipt.get("n4_rebound_inputs_verified")
        is n4_gate.get("rebound_inputs_verified")
        and receipt.get("n4_preprovisioned_snapshot_used")
        is n4_gate.get("preprovisioned_snapshot_used")
        and receipt.get("n4_live_materialization_executed")
        is n4_gate.get("live_n5_materialization_executed"),
        "semantic smoke receipt binds another N4 serve authorization",
    )
    _identifier(receipt.get("run_id"), "run_id")
    _require(
        receipt.get("results_file") == RESULTS_NAME
        and receipt.get("results_file_sha256")
        == _sha256((root / RESULTS_NAME).read_bytes()),
        "semantic smoke result file binding changed",
    )
    for position, (row, (smoke, trial)) in enumerate(
        zip(rows, selections, strict=True)
    ):
        supplied_row = _digest(
            row.get("smoke_result_sha256"), "smoke_result_sha256"
        )
        unsigned_row = dict(row)
        del unsigned_row["smoke_result_sha256"]
        _require(
            supplied_row == _sha256(_canonical(unsigned_row)),
            f"semantic smoke row {position} digest failed",
        )
        _require(
            row.get("schema_version") == SMOKE_RESULT_SCHEMA_VERSION
            and row.get("case_id") == smoke.get("case_id")
            and row.get("trial_key") == smoke.get("trial_key")
            and row.get("expected_executor_node_id")
            == smoke.get("expected_executor_node_id")
            and row.get("expected_cache_branch")
            == smoke.get("expected_cache_branch")
            and row.get("prerequisite_trial_key")
            == smoke.get("prerequisite_trial_key")
            and row.get("task_success_required_for_gate") is False
            and row.get("credentials_recorded") is False,
            f"semantic smoke row {position} selection changed",
        )
        result = row.get("result")
        _require(isinstance(result, Mapping), "semantic smoke result is missing")
        _require(
            row.get("result_sha256") == _sha256(_canonical(result)),
            "semantic smoke executor-result binding changed",
        )
        validated = validate_semantic_trial_result(
            result,
            trial=trial,
            idempotency_key=str(result.get("idempotency_key")),
        )
        _require(
            validated.get("execution_transport") == "flowmesh"
            and validated.get("n1_score_authenticity_verified") is True
            and validated.get("telemetry_complete") is True
            and validated.get("llm_called") is True,
            "semantic smoke did not complete the required runtime path",
        )
    expected_receipt = _receipt(
        inputs=inputs,
        source_root=local_semantic_admission_dir,
        n4_gate_root=n4_serve_gate_dir,
        n4_gate=n4_gate,
        run_id=str(receipt["run_id"]),
        rows=rows,
        one_case_root=one_case_plan_dir,
        one_case=one_case,
    )
    _require(
        (root / RECEIPT_NAME).read_bytes() == _json_bytes(expected_receipt),
        "semantic smoke receipt is not reproducible from its evidence",
    )
    return receipt, rows


def run_full_flow_local_semantic_smokes(
    local_semantic_admission_dir: str | Path,
    n4_serve_gate_dir: str | Path,
    compose_overlay_dir: str | Path,
    service_bootstrap_dir: str | Path,
    deployment_binding_dir: str | Path,
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    provisioning_catalog_dir: str | Path,
    artifact_binding_dir: str | Path,
    n4_package_dir: str | Path,
    *,
    run_id: str,
    executor: SemanticTrialExecutor,
    output_dir: str | Path,
    n4_live_gate_sources: N4LiveServeGateSources | None = None,
    one_case_plan_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Execute ten mandatory local smokes in dependency-safe order."""

    source_root = Path(local_semantic_admission_dir).resolve()
    n4_gate_root = Path(n4_serve_gate_dir).resolve()
    n4_gate_sources = {
        "compose_overlay_dir": Path(compose_overlay_dir).resolve(),
        "service_bootstrap_dir": Path(service_bootstrap_dir).resolve(),
        "deployment_binding_dir": Path(deployment_binding_dir).resolve(),
        "logical_plan_dir": Path(logical_route_dir).resolve(),
        "scenario_path": Path(scenario_path).resolve(),
        "container_plan_dir": Path(container_plan_dir).resolve(),
        "provisioning_catalog_dir": Path(provisioning_catalog_dir).resolve(),
        "artifact_binding_dir": Path(artifact_binding_dir).resolve(),
        "n4_package_dir": Path(n4_package_dir).resolve(),
    }
    inputs = load_full_flow_local_semantic_execution_inputs(source_root)
    one_case_root = (
        None
        if one_case_plan_dir is None
        else Path(one_case_plan_dir).resolve()
    )
    one_case = (
        None
        if one_case_root is None
        else load_full_flow_one_case_plan(
            one_case_root,
            local_semantic_admission_dir=source_root,
        )
    )
    normalized_live_sources = _normal_live_gate_sources(
        n4_live_gate_sources
    )
    n4_gate = _verify_n4_serve_gate(
        n4_gate_root,
        preprovisioned_sources=n4_gate_sources,
        live_sources=normalized_live_sources,
        admission_root=source_root,
        artifact_binding_root=n4_gate_sources["artifact_binding_dir"],
    )
    run_id = _identifier(run_id, "run_id")
    target = Path(output_dir).resolve()
    _require(not target.exists(), f"output directory already exists: {target}")
    rows: list[dict[str, Any]] = []
    for smoke, trial in _smoke_rows(inputs, one_case):
        case_id = str(smoke["case_id"])
        trial_key = str(smoke["trial_key"])
        idempotency_key = _idempotency_key(
            admission_sha256=str(inputs.admission["admission_sha256"]),
            run_id=run_id,
            case_id=case_id,
            trial_key=trial_key,
        )
        try:
            raw = executor.execute(
                trial=json.loads(_canonical(trial)),
                idempotency_key=idempotency_key,
            )
            result = validate_semantic_trial_result(
                raw,
                trial=trial,
                idempotency_key=idempotency_key,
            )
        except SemanticTrialExecutionError as exc:
            raise FullFlowLocalSemanticSmokeExecutionError(
                case_id=case_id,
                trial_key=trial_key,
                failure_class=exc.failure_class,
                failure_code=exc.failure_code,
            ) from exc
        except FullFlowLocalSemanticSmokeError:
            raise
        except Exception as exc:
            raise FullFlowLocalSemanticSmokeExecutionError(
                case_id=case_id,
                trial_key=trial_key,
                failure_class="semantic",
                failure_code="invalid-representative-smoke-result",
            ) from exc
        _require(
            result.get("execution_transport") == "flowmesh"
            and result.get("n1_score_authenticity_verified") is True
            and result.get("telemetry_complete") is True
            and result.get("llm_called") is True,
            "representative smoke did not complete the full runtime path",
        )
        rows.append(_result_row(smoke=smoke, result=result))

    receipt = _receipt(
        inputs=inputs,
        source_root=source_root,
        n4_gate_root=n4_gate_root,
        n4_gate=n4_gate,
        run_id=run_id,
        rows=rows,
        one_case_root=one_case_root,
        one_case=one_case,
    )
    documents = {
        RECEIPT_NAME: _json_bytes(receipt),
        RESULTS_NAME: _jsonl_bytes(rows),
    }
    documents[CHECKSUMS_NAME] = b"".join(
        f"{_sha256(documents[name])}  {name}\n".encode("utf-8")
        for name in sorted(_CONTENT)
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    parent = Path(tempfile.mkdtemp(prefix=".semantic-smoke-", dir=target.parent))
    stage = parent / "output"
    try:
        stage.mkdir()
        for name, payload in documents.items():
            (stage / name).write_bytes(payload)
        _verify_files(
            stage,
            local_semantic_admission_dir=source_root,
            n4_serve_gate_dir=n4_gate_root,
            n4_gate_sources=n4_gate_sources,
            n4_live_gate_sources=normalized_live_sources,
            one_case_plan_dir=one_case_root,
        )
        os.replace(stage, target)
    finally:
        shutil.rmtree(parent, ignore_errors=True)
    return verify_full_flow_local_semantic_smokes(
        target,
        local_semantic_admission_dir=source_root,
        n4_serve_gate_dir=n4_gate_root,
        compose_overlay_dir=n4_gate_sources["compose_overlay_dir"],
        service_bootstrap_dir=n4_gate_sources["service_bootstrap_dir"],
        deployment_binding_dir=n4_gate_sources["deployment_binding_dir"],
        logical_route_dir=n4_gate_sources["logical_plan_dir"],
        scenario_path=n4_gate_sources["scenario_path"],
        container_plan_dir=n4_gate_sources["container_plan_dir"],
        provisioning_catalog_dir=n4_gate_sources[
            "provisioning_catalog_dir"
        ],
        artifact_binding_dir=n4_gate_sources["artifact_binding_dir"],
        n4_package_dir=n4_gate_sources["n4_package_dir"],
        n4_live_gate_sources=normalized_live_sources,
        one_case_plan_dir=one_case_root,
    ) | {"output_dir": str(target)}


def verify_full_flow_local_semantic_smokes(
    smoke_dir: str | Path,
    *,
    local_semantic_admission_dir: str | Path,
    n4_serve_gate_dir: str | Path,
    compose_overlay_dir: str | Path,
    service_bootstrap_dir: str | Path,
    deployment_binding_dir: str | Path,
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    provisioning_catalog_dir: str | Path,
    artifact_binding_dir: str | Path,
    n4_package_dir: str | Path,
    n4_live_gate_sources: N4LiveServeGateSources | None = None,
    one_case_plan_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Verify the ten-smoke receipt and its full-matrix gate decision."""

    root = Path(smoke_dir).resolve()
    source_root = Path(local_semantic_admission_dir).resolve()
    n4_gate_root = Path(n4_serve_gate_dir).resolve()
    receipt, rows = _verify_files(
        root,
        local_semantic_admission_dir=source_root,
        n4_serve_gate_dir=n4_gate_root,
        n4_gate_sources={
            "compose_overlay_dir": Path(compose_overlay_dir).resolve(),
            "service_bootstrap_dir": Path(service_bootstrap_dir).resolve(),
            "deployment_binding_dir": Path(deployment_binding_dir).resolve(),
            "logical_plan_dir": Path(logical_route_dir).resolve(),
            "scenario_path": Path(scenario_path).resolve(),
            "container_plan_dir": Path(container_plan_dir).resolve(),
            "provisioning_catalog_dir": Path(
                provisioning_catalog_dir
            ).resolve(),
            "artifact_binding_dir": Path(artifact_binding_dir).resolve(),
            "n4_package_dir": Path(n4_package_dir).resolve(),
        },
        n4_live_gate_sources=_normal_live_gate_sources(
            n4_live_gate_sources
        ),
        one_case_plan_dir=(
            None
            if one_case_plan_dir is None
            else Path(one_case_plan_dir).resolve()
        ),
    )
    report = {
        "status": "VERIFIED",
        "run_id": receipt["run_id"],
        "receipt_sha256": receipt["receipt_sha256"],
        "smoke_count": len(rows),
        "case_ids": list(_CASE_ORDER),
        "full_matrix_runtime_gate_satisfied": receipt[
            "full_matrix_runtime_gate_satisfied"
        ],
        "full_matrix_submission_authorized": receipt[
            "full_matrix_submission_authorized"
        ],
        "n4_serve_gate_sha256": receipt["n4_serve_gate_sha256"],
        "n4_serve_gate_kind": receipt["n4_serve_gate_kind"],
        "n4_preprovisioned_snapshot_used": receipt[
            "n4_preprovisioned_snapshot_used"
        ],
        "n4_live_materialization_executed": receipt[
            "n4_live_materialization_executed"
        ],
        "n4_rebound_inputs_verified": receipt[
            "n4_rebound_inputs_verified"
        ],
        "n4_source_binding_checked": True,
        "n4_publication_companion_excluded": True,
        "n4_authorized_compose_profile": "serve-frozen",
        "semantic_input_profiles_verified": receipt[
            "semantic_input_profiles_verified"
        ],
        "execution_location_semantic_invariance_verified": receipt[
            "execution_location_semantic_invariance_verified"
        ],
        "cache_state_semantic_invariance_verified": receipt[
            "cache_state_semantic_invariance_verified"
        ],
        "route_family_semantic_separation_verified": receipt[
            "route_family_semantic_separation_verified"
        ],
        "task_success_required_for_gate": False,
        "w4_retrieval_quality_evaluated": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    if one_case_plan_dir is not None:
        report.update({
            "one_case_execution_complete": True,
            "one_case_id": receipt["one_case_id"],
            "one_case_workload_id": receipt["one_case_workload_id"],
            "one_case_artifact_object_id": receipt[
                "one_case_artifact_object_id"
            ],
            "one_case_safe_design_id": receipt[
                "one_case_safe_design_id"
            ],
            "formal_sampling_claimed": False,
        })
    return report


def _verify_multi_host_smoke_sources(
    *,
    local_semantic_admission_dir: str | Path,
    deployment_binding_dir: str | Path,
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
) -> dict[str, Any]:
    """Verify that every promoted trial uses its frozen multi-host origin."""

    deployment_root = Path(deployment_binding_dir).resolve()
    report = verify_full_flow_deployment_binding(
        deployment_root,
        logical_plan_dir=logical_route_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    binding = _strict_json(
        deployment_root / "full-flow-deployment-binding.json",
        "deployment binding",
    )
    _require(
        report.get("status") == "VERIFIED"
        and report.get("backend") == "multi-host-private-network"
        and binding.get("network_binding", {}).get("mode")
        == "physical-private-network",
        "semantic smoke requires a verified multi-host private-network binding",
    )
    service_origins = {
        row.get("service_contract_id"): row.get("base_url")
        for row in binding.get("service_bindings", [])
        if isinstance(row, Mapping)
        and row.get("service_contract_id")
        in {"N7.execution-compute", "N8.execution-compute"}
    }
    _require(
        set(service_origins)
        == {"N7.execution-compute", "N8.execution-compute"},
        "multi-host binding omits an execution coordinator",
    )
    inputs = load_full_flow_local_semantic_execution_inputs(
        Path(local_semantic_admission_dir).resolve()
    )
    _require(
        inputs.admission.get("deployment_id")
        == binding.get("deployment_id"),
        "semantic admission binds another deployment",
    )
    source_commitments = inputs.admission.get("source_commitments")
    original_bindings = (
        source_commitments.get("legacy_original_source_bindings")
        if isinstance(source_commitments, Mapping)
        else None
    )
    _require(
        isinstance(original_bindings, Mapping)
        and original_bindings.get("deployment_binding_sha256")
        == report.get("binding_sha256"),
        "semantic admission binds another deployment digest",
    )
    observed_contracts: set[str] = set()
    for trial in inputs.bound_trials:
        coordinator = trial.get("route_coordinator_binding")
        _require(
            isinstance(coordinator, Mapping),
            "semantic trial omits its route coordinator",
        )
        contract = coordinator.get("service_contract_id")
        base_url = coordinator.get("base_url")
        _require(
            contract in service_origins
            and base_url == service_origins[contract],
            "semantic trial coordinator differs from the deployment binding",
        )
        parsed = urlsplit(str(base_url))
        _require(
            parsed.scheme in {"http", "https"}
            and parsed.hostname not in {None, "localhost", "127.0.0.1", "::1"},
            "multi-host semantic trial uses a loopback coordinator",
        )
        observed_contracts.add(str(contract))
    _require(
        observed_contracts == set(service_origins),
        "semantic admission does not cover both multi-host coordinators",
    )
    return {
        "runtime_environment": "multi-host-private-network",
        "deployment_id": report["deployment_id"],
        "deployment_binding_sha256": report["binding_sha256"],
        "coordinator_origins_verified": True,
    }


def run_full_flow_semantic_smokes(
    local_semantic_admission_dir: str | Path,
    n4_serve_gate_dir: str | Path,
    deployment_binding_dir: str | Path,
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    artifact_binding_dir: str | Path,
    *,
    run_id: str,
    executor: SemanticTrialExecutor,
    output_dir: str | Path,
    n4_live_gate_sources: N4LiveServeGateSources | None = None,
    compose_overlay_dir: str | Path | None = None,
    service_bootstrap_dir: str | Path | None = None,
    n4_gate_deployment_binding_dir: str | Path | None = None,
    provisioning_catalog_dir: str | Path | None = None,
    n4_package_dir: str | Path | None = None,
    one_case_plan_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Run the same source-bound ten cases on a multi-host deployment."""

    environment = _verify_multi_host_smoke_sources(
        local_semantic_admission_dir=local_semantic_admission_dir,
        deployment_binding_dir=deployment_binding_dir,
        logical_route_dir=logical_route_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    if n4_live_gate_sources is None:
        _require(
            all(
                value is not None
                for value in (
                    compose_overlay_dir,
                    service_bootstrap_dir,
                    provisioning_catalog_dir,
                    n4_package_dir,
                )
            ),
            "multi-host preprovisioned N4 gate sources are incomplete",
        )
    report = run_full_flow_local_semantic_smokes(
        local_semantic_admission_dir,
        n4_serve_gate_dir,
        (
            deployment_binding_dir
            if compose_overlay_dir is None
            else compose_overlay_dir
        ),
        (
            deployment_binding_dir
            if service_bootstrap_dir is None
            else service_bootstrap_dir
        ),
        (
            deployment_binding_dir
            if n4_gate_deployment_binding_dir is None
            else n4_gate_deployment_binding_dir
        ),
        logical_route_dir,
        scenario_path,
        container_plan_dir,
        (
            artifact_binding_dir
            if provisioning_catalog_dir is None
            else provisioning_catalog_dir
        ),
        artifact_binding_dir,
        artifact_binding_dir if n4_package_dir is None else n4_package_dir,
        run_id=run_id,
        executor=executor,
        output_dir=output_dir,
        n4_live_gate_sources=n4_live_gate_sources,
        one_case_plan_dir=one_case_plan_dir,
    )
    return report | environment


def verify_full_flow_semantic_smokes(
    smoke_dir: str | Path,
    *,
    local_semantic_admission_dir: str | Path,
    n4_serve_gate_dir: str | Path,
    deployment_binding_dir: str | Path,
    logical_route_dir: str | Path,
    scenario_path: str | Path,
    container_plan_dir: str | Path,
    artifact_binding_dir: str | Path,
    n4_live_gate_sources: N4LiveServeGateSources | None = None,
    compose_overlay_dir: str | Path | None = None,
    service_bootstrap_dir: str | Path | None = None,
    n4_gate_deployment_binding_dir: str | Path | None = None,
    provisioning_catalog_dir: str | Path | None = None,
    n4_package_dir: str | Path | None = None,
    one_case_plan_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Verify a ten-case receipt against its multi-host deployment."""

    environment = _verify_multi_host_smoke_sources(
        local_semantic_admission_dir=local_semantic_admission_dir,
        deployment_binding_dir=deployment_binding_dir,
        logical_route_dir=logical_route_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
    )
    if n4_live_gate_sources is None:
        _require(
            all(
                value is not None
                for value in (
                    compose_overlay_dir,
                    service_bootstrap_dir,
                    provisioning_catalog_dir,
                    n4_package_dir,
                )
            ),
            "multi-host preprovisioned N4 gate sources are incomplete",
        )
    report = verify_full_flow_local_semantic_smokes(
        smoke_dir,
        local_semantic_admission_dir=local_semantic_admission_dir,
        n4_serve_gate_dir=n4_serve_gate_dir,
        compose_overlay_dir=(
            deployment_binding_dir
            if compose_overlay_dir is None
            else compose_overlay_dir
        ),
        service_bootstrap_dir=(
            deployment_binding_dir
            if service_bootstrap_dir is None
            else service_bootstrap_dir
        ),
        deployment_binding_dir=(
            deployment_binding_dir
            if n4_gate_deployment_binding_dir is None
            else n4_gate_deployment_binding_dir
        ),
        logical_route_dir=logical_route_dir,
        scenario_path=scenario_path,
        container_plan_dir=container_plan_dir,
        provisioning_catalog_dir=(
            artifact_binding_dir
            if provisioning_catalog_dir is None
            else provisioning_catalog_dir
        ),
        artifact_binding_dir=artifact_binding_dir,
        n4_package_dir=(
            artifact_binding_dir
            if n4_package_dir is None
            else n4_package_dir
        ),
        n4_live_gate_sources=n4_live_gate_sources,
        one_case_plan_dir=one_case_plan_dir,
    )
    return report | environment


__all__ = [
    "CHECKSUMS_NAME",
    "FullFlowLocalSemanticSmokeError",
    "FullFlowLocalSemanticSmokeExecutionError",
    "N4LiveServeGateSources",
    "RECEIPT_NAME",
    "RESULTS_NAME",
    "SMOKE_RECEIPT_SCHEMA_VERSION",
    "SMOKE_RESULT_SCHEMA_VERSION",
    "run_full_flow_local_semantic_smokes",
    "run_full_flow_semantic_smokes",
    "verify_full_flow_local_semantic_smokes",
    "verify_full_flow_semantic_smokes",
]
