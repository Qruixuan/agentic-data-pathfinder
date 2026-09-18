"""Freeze one public workload across the ten representative data paths.

The one-case plan is an engineering demonstration input, not a scientific
sample.  It selects source-bound trials already present in a verified local
semantic admission without reading the N1 private oracle package.  Live
execution is deliberately separate from this module.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .full_flow_local_semantic_admission import (
    ADMISSION_NAME as SOURCE_ADMISSION_NAME,
    CHECKSUMS_NAME as SOURCE_CHECKSUMS_NAME,
    TRIALS_NAME as SOURCE_TRIALS_NAME,
    FrozenLocalSemanticExecutionInputs,
    load_full_flow_local_semantic_execution_inputs,
)


PLAN_SCHEMA_VERSION = "pathfinder.full-flow-one-case-plan/v1alpha1"
TRIAL_SCHEMA_VERSION = "pathfinder.full-flow-one-case-trial/v1alpha1"
PLAN_NAME = "full-flow-one-case-plan.json"
TRIALS_NAME = "full-flow-one-case-trials.jsonl"
CHECKSUMS_NAME = "SHA256SUMS"

_CONTENT = {PLAN_NAME, TRIALS_NAME}
_FILES = _CONTENT | {CHECKSUMS_NAME}
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}\Z")
_TRIAL_KEY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+|/-]{0,511}\Z")

_CASE_SELECTIONS = (
    ("n7-raw", "D0", 0, "N7", None),
    ("n7-indexed-raw", "D1", 0, "N7", None),
    ("n7-remote-derived", "D2", 0, "N7", None),
    ("n7-cache-miss", "D3", 0, "N7", "miss"),
    ("n7-cache-hit", "D3", 1, "N7", "hit"),
    ("n8-raw", "D4", 0, "N8", None),
    ("n8-indexed-raw", "D5", 0, "N8", None),
    ("n8-remote-derived", "D6", 0, "N8", None),
    ("n8-cache-miss", "D7", 0, "N8", "miss"),
    ("n8-cache-hit", "D7", 1, "N8", "hit"),
)


class FullFlowOneCaseError(ValueError):
    """Raised when a one-case plan is incomplete or has drifted."""


@dataclass(frozen=True)
class FrozenFullFlowOneCasePlan:
    """Verified public selection safe to hand to an execution runner."""

    plan: Mapping[str, Any]
    trials: tuple[Mapping[str, Any], ...]


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowOneCaseError(message)


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
        raise FullFlowOneCaseError(
            "one-case evidence is not canonical JSON"
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


def _identifier(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
        f"{name} is invalid",
    )
    return str(value)


def _trial_key(value: Any) -> str:
    _require(
        isinstance(value, str) and _TRIAL_KEY.fullmatch(value) is not None,
        "trial_key is invalid",
    )
    return str(value)


def _digest(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{name} is invalid",
    )
    return str(value)


def _strict_json(path: Path, name: str) -> dict[str, Any]:
    _require(path.is_file() and not path.is_symlink(), f"{name} is missing")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowOneCaseError(f"cannot read {name}") from exc
    _require(isinstance(value, dict), f"{name} must be an object")
    return value


def _strict_jsonl(path: Path, name: str) -> list[dict[str, Any]]:
    _require(path.is_file() and not path.is_symlink(), f"{name} is missing")
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise FullFlowOneCaseError(f"cannot read {name}") from exc
    _require(lines and all(line.strip() for line in lines), f"{name} is empty")
    rows: list[dict[str, Any]] = []
    for position, line in enumerate(lines, start=1):
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise FullFlowOneCaseError(
                f"{name} line {position} is invalid"
            ) from exc
        _require(isinstance(row, dict), f"{name} row is not an object")
        rows.append(row)
    return rows


def _normal_public_task(trial: Mapping[str, Any]) -> dict[str, Any]:
    task = trial.get("public_task_binding")
    _require(isinstance(task, Mapping), "trial omits its public task binding")
    _require(
        task.get("label_values_included") is not True
        and "correct_answer_id" not in task,
        "public task binding includes a hidden label",
    )
    return dict(task)


def _select_trials(
    inputs: FrozenLocalSemanticExecutionInputs,
    workload_id: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    candidates = [
        dict(row)
        for row in inputs.bound_trials
        if row.get("workload_id") == workload_id
    ]
    _require(candidates, f"workload is absent from admission: {workload_id}")

    public_tasks: dict[bytes, dict[str, Any]] = {}
    for row in candidates:
        task = _normal_public_task(row)
        public_tasks[_canonical(task)] = task
    _require(
        len(public_tasks) == 1,
        "one-case workload has multiple public task bindings",
    )
    public_task = next(iter(public_tasks.values()))
    _require(
        public_task.get("workload_id") in {None, workload_id},
        "public task binds another workload",
    )
    artifact_object_id = _identifier(
        public_task.get("object_id"), "artifact_object_id"
    )
    _require(
        all(
            row.get("artifact_object_id") in {None, artifact_object_id}
            for row in candidates
        ),
        "one-case trials bind another artifact object",
    )

    selected: list[dict[str, Any]] = []
    trial_keys: dict[tuple[str, int], str] = {}
    for case_id, design_id, repetition, executor_node_id, cache_branch in (
        _CASE_SELECTIONS
    ):
        matches = [
            row
            for row in candidates
            if row.get("design_id") == design_id
            and row.get("repetition") == repetition
        ]
        _require(
            len(matches) == 1,
            f"case {case_id} does not resolve to exactly one bound trial",
        )
        trial = matches[0]
        _require(
            trial.get("executor_node_id") == executor_node_id,
            f"case {case_id} executor changed",
        )
        _require(
            trial.get("flowmesh_submission_authorized") is True,
            f"case {case_id} is not authorized for FlowMesh submission",
        )
        trial_key = _trial_key(trial.get("trial_key"))
        trial_keys[(design_id, repetition)] = trial_key
        representations = trial.get("representation_identities")
        _require(
            isinstance(representations, list) and representations,
            f"case {case_id} has no representation identity",
        )
        representation_ids = sorted({
            str(row.get("representation_id"))
            for row in representations
            if isinstance(row, Mapping)
        })
        _require(
            representation_ids and "None" not in representation_ids,
            f"case {case_id} has an invalid representation identity",
        )
        source_semantic_trial_sha256 = _digest(
            trial.get("source_semantic_trial_sha256"),
            "source_semantic_trial_sha256",
        )
        selected.append({
            "schema_version": TRIAL_SCHEMA_VERSION,
            "case_id": case_id,
            "trial_key": trial_key,
            "design_id": design_id,
            "repetition": repetition,
            "executor_node_id": executor_node_id,
            "route_family": trial.get("route_family"),
            "representation_ids": representation_ids,
            "expected_cache_branch": cache_branch,
            "prerequisite_trial_key": None,
            "source_semantic_trial_sha256": source_semantic_trial_sha256,
            "bound_trial_sha256": _sha256(_canonical(trial)),
            "credentials_recorded": False,
            "endpoint_values_included": False,
            "hidden_label_values_included": False,
        })

    for row in selected:
        if row["case_id"] == "n7-cache-hit":
            row["prerequisite_trial_key"] = trial_keys[("D3", 0)]
        elif row["case_id"] == "n8-cache-hit":
            row["prerequisite_trial_key"] = trial_keys[("D7", 0)]
        row["selection_sha256"] = _sha256(_canonical(row))
    return public_task, selected


def _documents(
    source_root: Path,
    inputs: FrozenLocalSemanticExecutionInputs,
    *,
    case_id: str,
    workload_id: str,
    safe_design_id: str,
) -> dict[str, bytes]:
    public_task, selected = _select_trials(inputs, workload_id)
    _require(
        safe_design_id in {str(row["design_id"]) for row in selected},
        "safe design is absent from the selected trials",
    )
    trials_bytes = _jsonl_bytes(selected)
    public_task_binding_sha256 = _digest(
        public_task.get("task_binding_sha256"),
        "public_task_binding_sha256",
    )
    artifact_object_id = _identifier(
        public_task.get("object_id"), "artifact_object_id"
    )
    promotion_id = _identifier(
        inputs.admission.get("promotion_id"), "promotion_id"
    )
    admission_sha256 = _digest(
        inputs.admission.get("admission_sha256"), "admission_sha256"
    )
    plan: dict[str, Any] = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "status": "FROZEN_ONE_CASE_INPUTS",
        "case_id": case_id,
        "workload_id": workload_id,
        "artifact_object_id": artifact_object_id,
        "public_task_binding": public_task,
        "public_task_binding_sha256": public_task_binding_sha256,
        "safe_design_id": safe_design_id,
        "design_ids": sorted({row["design_id"] for row in selected}),
        "case_ids": [row["case_id"] for row in selected],
        "trial_count": len(selected),
        "trials_file": TRIALS_NAME,
        "trials_file_sha256": _sha256(trials_bytes),
        "promotion_id": promotion_id,
        "admission_sha256": admission_sha256,
        "source_admission_file_sha256": _sha256(
            (source_root / SOURCE_ADMISSION_NAME).read_bytes()
        ),
        "source_trials_file_sha256": _sha256(
            (source_root / SOURCE_TRIALS_NAME).read_bytes()
        ),
        "source_checksums_file_sha256": _sha256(
            (source_root / SOURCE_CHECKSUMS_NAME).read_bytes()
        ),
        "selection_kind": "engineering-demonstration",
        "selection_provenance": (
            "preexisting-complete-representation-case;not-formal-random-sample"
        ),
        "formal_sampling_claimed": False,
        "workflow_submitted": False,
        "llm_called": False,
        "services_started": False,
        "credentials_recorded": False,
        "endpoint_values_included": False,
        "hidden_label_values_included": False,
        "eligible_for_scientific_claims": False,
    }
    plan["plan_sha256"] = _sha256(_canonical(plan))
    return {
        PLAN_NAME: _json_bytes(plan),
        TRIALS_NAME: trials_bytes,
    }


def _verify_files(
    root: Path,
    source_root: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    _require(root.is_dir(), "one-case plan directory is missing")
    files = list(root.iterdir())
    _require(
        {path.name for path in files} == _FILES
        and all(path.is_file() and not path.is_symlink() for path in files),
        "one-case plan file set changed",
    )
    expected_checksums = b"".join(
        f"{_sha256((root / name).read_bytes())}  {name}\n".encode("utf-8")
        for name in sorted(_CONTENT)
    )
    _require(
        (root / CHECKSUMS_NAME).read_bytes() == expected_checksums,
        "one-case plan checksums failed",
    )
    plan = _strict_json(root / PLAN_NAME, "one-case plan")
    trials = _strict_jsonl(root / TRIALS_NAME, "one-case trials")
    supplied = plan.get("plan_sha256")
    _require(
        isinstance(supplied, str) and _SHA256.fullmatch(supplied) is not None,
        "plan_sha256 is invalid",
    )
    unsigned = dict(plan)
    del unsigned["plan_sha256"]
    _require(supplied == _sha256(_canonical(unsigned)), "plan digest failed")
    _require(
        plan.get("schema_version") == PLAN_SCHEMA_VERSION
        and plan.get("status") == "FROZEN_ONE_CASE_INPUTS"
        and plan.get("trial_count") == len(_CASE_SELECTIONS)
        and len(trials) == len(_CASE_SELECTIONS)
        and plan.get("case_ids") == [row[0] for row in _CASE_SELECTIONS]
        and plan.get("design_ids") == [f"D{index}" for index in range(8)]
        and plan.get("formal_sampling_claimed") is False
        and plan.get("workflow_submitted") is False
        and plan.get("llm_called") is False
        and plan.get("services_started") is False
        and plan.get("credentials_recorded") is False
        and plan.get("endpoint_values_included") is False
        and plan.get("hidden_label_values_included") is False
        and plan.get("eligible_for_scientific_claims") is False,
        "one-case safety claims changed",
    )
    _require(
        plan.get("trials_file_sha256")
        == _sha256((root / TRIALS_NAME).read_bytes()),
        "one-case trials digest changed",
    )
    inputs = load_full_flow_local_semantic_execution_inputs(source_root)
    expected = _documents(
        source_root,
        inputs,
        case_id=_identifier(plan.get("case_id"), "case_id"),
        workload_id=_identifier(plan.get("workload_id"), "workload_id"),
        safe_design_id=_identifier(
            plan.get("safe_design_id"), "safe_design_id"
        ),
    )
    _require(
        (root / PLAN_NAME).read_bytes() == expected[PLAN_NAME]
        and (root / TRIALS_NAME).read_bytes() == expected[TRIALS_NAME],
        "one-case plan no longer matches its source admission",
    )
    return plan, trials


def freeze_full_flow_one_case_plan(
    local_semantic_admission_dir: str | Path,
    *,
    case_id: str,
    workload_id: str,
    safe_design_id: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze one workload's ten representative paths without executing it."""

    source_root = Path(local_semantic_admission_dir).resolve()
    case_id = _identifier(case_id, "case_id")
    workload_id = _identifier(workload_id, "workload_id")
    safe_design_id = _identifier(safe_design_id, "safe_design_id")
    inputs = load_full_flow_local_semantic_execution_inputs(source_root)
    documents = _documents(
        source_root,
        inputs,
        case_id=case_id,
        workload_id=workload_id,
        safe_design_id=safe_design_id,
    )
    documents[CHECKSUMS_NAME] = b"".join(
        f"{_sha256(documents[name])}  {name}\n".encode("utf-8")
        for name in sorted(_CONTENT)
    )
    target = Path(output_dir).resolve()
    _require(not target.exists(), f"output directory already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    parent = Path(tempfile.mkdtemp(prefix=".one-case-", dir=target.parent))
    stage = parent / "output"
    try:
        stage.mkdir()
        for name, payload in documents.items():
            (stage / name).write_bytes(payload)
        _verify_files(stage, source_root)
        os.replace(stage, target)
    finally:
        shutil.rmtree(parent, ignore_errors=True)
    return verify_full_flow_one_case_plan(
        target,
        local_semantic_admission_dir=source_root,
    ) | {"output_dir": str(target)}


def verify_full_flow_one_case_plan(
    plan_dir: str | Path,
    *,
    local_semantic_admission_dir: str | Path,
) -> dict[str, Any]:
    """Verify a one-case plan against its complete public source admission."""

    root = Path(plan_dir).resolve()
    source_root = Path(local_semantic_admission_dir).resolve()
    plan, trials = _verify_files(root, source_root)
    return {
        "status": "VERIFIED",
        "case_id": plan["case_id"],
        "workload_id": plan["workload_id"],
        "artifact_object_id": plan["artifact_object_id"],
        "safe_design_id": plan["safe_design_id"],
        "design_ids": plan["design_ids"],
        "trial_count": len(trials),
        "plan_sha256": plan["plan_sha256"],
        "formal_sampling_claimed": False,
        "workflow_submitted": False,
        "llm_called": False,
        "credentials_recorded": False,
        "hidden_label_values_included": False,
        "eligible_for_scientific_claims": False,
    }


def load_full_flow_one_case_plan(
    plan_dir: str | Path,
    *,
    local_semantic_admission_dir: str | Path,
) -> FrozenFullFlowOneCasePlan:
    """Load a plan only after re-deriving it from its public admission."""

    plan, trials = _verify_files(
        Path(plan_dir).resolve(),
        Path(local_semantic_admission_dir).resolve(),
    )
    return FrozenFullFlowOneCasePlan(
        plan=plan,
        trials=tuple(trials),
    )


__all__ = [
    "FrozenFullFlowOneCasePlan",
    "FullFlowOneCaseError",
    "freeze_full_flow_one_case_plan",
    "load_full_flow_one_case_plan",
    "verify_full_flow_one_case_plan",
]
