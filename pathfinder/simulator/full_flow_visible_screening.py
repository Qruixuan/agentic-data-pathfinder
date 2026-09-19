"""Frozen visible-development demo screening over frozen physical actions.

This module selects an *illustrative* development/demo case.  It is not the
hidden formal evaluation: it deliberately inspects authenticated public
outcomes in order to choose which case to display, so every artifact it emits
records ``selection_kind`` as a visible-development screening and refuses to
claim scientific eligibility.

It never reads a hidden label.  Only N1's authenticated public
``task_success`` boolean, the model's public prediction, and public task
metadata are consumed.  The candidate pool and its order are frozen from
public metadata before any new outcome is observed.

Nothing here weakens frozen admission or evidence verification: candidate
trials are selected from the already-frozen admission by their frozen
coordinates, and each result is validated by the same source-bound verifier
the ten-case runner uses.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Callable

from .full_flow_local_semantic_admission import (
    load_full_flow_local_semantic_execution_inputs,
)

VISIBLE_SCREENING_PLAN_SCHEMA_VERSION = (
    "pathfinder.full-flow-visible-screening-plan/v1alpha1"
)
VISIBLE_SCREENING_RUN_SCHEMA_VERSION = (
    "pathfinder.full-flow-visible-screening-run/v1alpha1"
)
SELECTION_KIND = "visible-development-demo-screening"

PLAN_NAME = "visible-screening-plan.json"
CANDIDATES_NAME = "visible-screening-candidates.jsonl"
RESULTS_NAME = "visible-screening-results.jsonl"
RECEIPT_NAME = "visible-screening-receipt.json"
CHECKSUMS_NAME = "SHA256SUMS"

# A bare option ID is the only answer shape the legacy exact-match rule can
# score.  A bracketed marker is legitimate under the canonical rule, so a
# false result under exact-match with a non-bare answer is not evidence of a
# semantic difference and must not drive demo selection.
_BARE_OPTION = re.compile(r"[A-Z][A-Z0-9_-]{0,15}\Z")
_EXACT_MATCH_RULE = "multiple-choice-option-id-exact-match-v1"
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class VisibleScreeningError(ValueError):
    """Raised before an unbound screening plan or result can be frozen."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise VisibleScreeningError(message)


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _identifier(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
        f"{name} is invalid",
    )
    return str(value)


def _digest(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{name} is not lowercase SHA-256",
    )
    return str(value)


def _jsonl_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    return b"".join(_canonical(row) + b"\n" for row in rows)


def _write_package(target: Path, files: Mapping[str, bytes]) -> None:
    _require(not target.exists(), f"output directory already exists: {target}")
    target.mkdir(parents=True)
    lines = []
    for name in sorted(files):
        (target / name).write_bytes(files[name])
        lines.append(f"{_sha256(files[name])}  {name}")
    # Write explicit LF bytes: text mode would apply the platform newline
    # translation, so a manifest frozen on Windows would not verify with a
    # strict sha256sum on Linux.
    manifest = ("\n".join(lines) + "\n").encode("utf-8")
    (target / CHECKSUMS_NAME).write_bytes(manifest)


def verify_checksums(root: Path) -> None:
    """Re-verify a frozen package from inside its own directory."""

    manifest = (root / CHECKSUMS_NAME).read_text(encoding="utf-8")
    seen = 0
    for line in manifest.splitlines():
        if not line.strip():
            continue
        expected, _, name = line.partition("  ")
        name = name.strip()
        actual = _sha256((root / name).read_bytes())
        _require(actual == expected.strip(), f"checksum mismatch: {name}")
        seen += 1
    _require(seen > 0, "checksum manifest is empty")


def _candidate_order_key(seed: str, workload_id: str) -> str:
    """Deterministic, seeded, outcome-independent candidate ordering."""

    return _sha256(f"{seed}\x00{workload_id}".encode("utf-8"))


def build_candidate_pool(
    *,
    bound_trials: Sequence[Mapping[str, Any]],
    public_tasks: Sequence[Mapping[str, Any]],
    screening_actions: Sequence[Mapping[str, Any]],
    stratum_by_workload: Mapping[str, str],
    seed: str,
    exclude_workload_ids: Sequence[str] = (),
) -> list[dict[str, Any]]:
    """Derive an ordered candidate pool from public metadata only.

    No outcome, prediction, or hidden label participates in pool membership
    or ordering.  A workload is eligible only when every planned physical
    action already exists as a frozen trial.
    """

    excluded = set(exclude_workload_ids)
    task_by_workload: dict[str, Mapping[str, Any]] = {}
    for task in public_tasks:
        _require(isinstance(task, Mapping), "public task row is invalid")
        workload_id = _identifier(task.get("workload_id"), "workload_id")
        _require(
            workload_id not in task_by_workload,
            "public task set repeats a workload",
        )
        task_by_workload[workload_id] = task

    # Index the frozen trials by their frozen coordinates.
    by_coordinate: dict[tuple[str, str, str], Mapping[str, Any]] = {}
    for trial in bound_trials:
        _require(isinstance(trial, Mapping), "bound trial is invalid")
        parts = str(trial.get("trial_key")).split("|")
        _require(len(parts) == 4, "bound trial key shape changed")
        workload_id = str(trial.get("workload_id"))
        design_id = parts[2]
        repetition = parts[3]
        key = (workload_id, design_id, repetition)
        _require(key not in by_coordinate, "bound trials repeat a coordinate")
        by_coordinate[key] = trial

    candidates: list[dict[str, Any]] = []
    for workload_id, task in task_by_workload.items():
        if workload_id in excluded:
            continue
        actions: list[dict[str, Any]] = []
        eligible = True
        for action in screening_actions:
            design_id = _identifier(action.get("design_id"), "design_id")
            repetition = _identifier(action.get("repetition"), "repetition")
            trial = by_coordinate.get((workload_id, design_id, repetition))
            if trial is None:
                eligible = False
                break
            profile = trial.get("semantic_input_profile") or {}
            actions.append({
                "action_id": _identifier(action.get("action_id"), "action_id"),
                "design_id": design_id,
                "repetition": repetition,
                "executor_node_id": str(trial.get("executor_node_id")),
                "route_family": str(trial.get("route_family")),
                "semantic_input_profile_id": profile.get("profile_id"),
                "direct_video_input": bool(profile.get("direct_video_input")),
                "trial_key": str(trial.get("trial_key")),
            })
            expected_node = action.get("executor_node_id")
            if expected_node is not None:
                _require(
                    str(trial.get("executor_node_id")) == str(expected_node),
                    f"{workload_id} {design_id} is not on the planned node",
                )
        if not eligible:
            continue
        candidates.append({
            "workload_id": workload_id,
            "object_id": str(task.get("object_id")),
            "task_class_id": str(task.get("task_class_id")),
            "stratum": str(stratum_by_workload.get(workload_id, workload_id)),
            "success_scoring_rule": str(task.get("success_scoring_rule")),
            "public_question_sha256": _sha256(
                str(task.get("question")).encode("utf-8")
            ),
            "public_option_ids": [
                str(option.get("option_id"))
                for option in (task.get("answer_options") or [])
            ],
            "actions": actions,
            "order_key": _candidate_order_key(seed, workload_id),
        })

    candidates.sort(key=lambda row: (row["order_key"], row["workload_id"]))
    for index, row in enumerate(candidates):
        row["order_index"] = index
    return candidates


def freeze_visible_screening_plan(
    *,
    local_semantic_admission_dir: str | Path,
    public_task_set: str | Path,
    screening_id: str,
    seed: str,
    git_commit: str,
    screening_actions: Sequence[Mapping[str, Any]],
    stratum_by_workload: Mapping[str, str],
    selection_rule: Mapping[str, Any],
    max_candidates: int,
    max_workflows: int,
    exclude_workload_ids: Sequence[str],
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze an immutable, outcome-blind screening plan."""

    admission_root = Path(local_semantic_admission_dir).resolve()
    inputs = load_full_flow_local_semantic_execution_inputs(admission_root)
    task_path = Path(public_task_set).resolve()
    task_document = json.loads(task_path.read_text(encoding="utf-8"))
    public_tasks = task_document.get("tasks")
    _require(isinstance(public_tasks, list), "public task set is invalid")
    _require(
        type(max_candidates) is int and max_candidates > 0,
        "max_candidates must be a positive integer",
    )
    _require(
        type(max_workflows) is int and max_workflows > 0,
        "max_workflows must be a positive integer",
    )

    candidates = build_candidate_pool(
        bound_trials=inputs.bound_trials,
        public_tasks=public_tasks,
        screening_actions=screening_actions,
        stratum_by_workload=stratum_by_workload,
        seed=seed,
        exclude_workload_ids=exclude_workload_ids,
    )
    _require(bool(candidates), "no eligible screening candidate was found")
    screened = candidates[:max_candidates]
    _require(
        len(screened) * len(screening_actions) <= max_workflows,
        "planned screening exceeds its own workflow budget",
    )

    candidate_bytes = _jsonl_bytes(screened)
    plan = {
        "schema_version": VISIBLE_SCREENING_PLAN_SCHEMA_VERSION,
        "screening_id": _identifier(screening_id, "screening_id"),
        "selection_kind": SELECTION_KIND,
        "git_commit": _identifier(git_commit, "git_commit"),
        "screening_seed": str(seed),
        "candidate_order_rule": "sha256(seed|workload_id) ascending",
        "selection_rule": json.loads(_canonical(selection_rule)),
        "screening_actions": json.loads(_canonical(list(screening_actions))),
        "candidate_count": len(screened),
        "eligible_candidate_count": len(candidates),
        "excluded_workload_ids": sorted(set(exclude_workload_ids)),
        "max_candidates": max_candidates,
        "max_workflows": max_workflows,
        "source_admission_sha256": _digest(
            inputs.admission["admission_sha256"], "admission_sha256"
        ),
        "public_task_set_sha256": _sha256(task_path.read_bytes()),
        "candidates_sha256": _sha256(candidate_bytes),
        "strata": sorted({row["stratum"] for row in screened}),
        "credentials_recorded": False,
        "hidden_label_values_included": False,
        "outcome_inspected_for_pool_construction": False,
        "eligible_for_scientific_claims": False,
        "formal_sampling_claimed": False,
        "status": "FROZEN_VISIBLE_SCREENING_PLAN",
    }
    plan["plan_sha256"] = _sha256(_canonical(plan))
    target = Path(output_dir).resolve()
    _write_package(
        target,
        {PLAN_NAME: _canonical(plan), CANDIDATES_NAME: candidate_bytes},
    )
    return plan


def load_visible_screening_plan(plan_dir: str | Path) -> tuple[dict, list[dict]]:
    """Load and re-verify a frozen screening plan."""

    root = Path(plan_dir).resolve()
    verify_checksums(root)
    plan = json.loads((root / PLAN_NAME).read_text(encoding="utf-8"))
    _require(
        plan.get("schema_version") == VISIBLE_SCREENING_PLAN_SCHEMA_VERSION
        and plan.get("selection_kind") == SELECTION_KIND
        and plan.get("eligible_for_scientific_claims") is False
        and plan.get("formal_sampling_claimed") is False,
        "screening plan schema or claim boundary changed",
    )
    candidate_bytes = (root / CANDIDATES_NAME).read_bytes()
    _require(
        _sha256(candidate_bytes) == plan.get("candidates_sha256"),
        "screening candidates differ from their frozen digest",
    )
    supplied = dict(plan)
    recorded = supplied.pop("plan_sha256", None)
    _require(
        _sha256(_canonical(supplied)) == recorded,
        "screening plan digest changed",
    )
    candidates = [
        json.loads(line)
        for line in candidate_bytes.decode("utf-8").splitlines()
        if line.strip()
    ]
    _require(
        [row.get("order_index") for row in candidates]
        == list(range(len(candidates))),
        "screening candidate order changed",
    )
    return plan, candidates


def classify_action_outcome(
    *,
    success_scoring_rule: str,
    predicted_answer: str,
    task_success: bool,
) -> dict[str, Any]:
    """Flag results whose falseness could be an answer-format artifact.

    Under the legacy exact-match rule a bracketed marker scores false even
    when it names the right option.  Such a result is ambiguous and must not
    be used to claim a representation-quality difference.
    """

    bare = _BARE_OPTION.fullmatch(predicted_answer.strip()) is not None
    ambiguous = (
        success_scoring_rule == _EXACT_MATCH_RULE
        and task_success is False
        and not bare
    )
    return {
        "task_success": bool(task_success),
        "answer_is_bare_option_id": bare,
        "format_ambiguous": ambiguous,
        "usable_for_demo_difference": not ambiguous,
    }


def evaluate_selection_rule(
    candidate_results: Mapping[str, Mapping[str, Any]],
    *,
    primary_action_id: str,
    cheap_action_id: str,
) -> dict[str, Any]:
    """Apply the frozen selection rule to one candidate's outcomes."""

    primary = candidate_results.get(primary_action_id)
    cheap = candidate_results.get(cheap_action_id)
    if primary is None or cheap is None:
        return {"rule": "incomplete", "selected": False, "reason": "missing-action"}
    if not (
        primary["usable_for_demo_difference"]
        and cheap["usable_for_demo_difference"]
    ):
        return {
            "rule": "rejected",
            "selected": False,
            "reason": "answer-format-ambiguous",
        }
    if primary["task_success"] and not cheap["task_success"]:
        return {"rule": "primary", "selected": True, "reason": "direct-video-only-success"}
    if primary["task_success"] != cheap["task_success"]:
        return {"rule": "fallback", "selected": True, "reason": "differing-task-success"}
    if primary["task_success"] and cheap["task_success"]:
        return {
            "rule": "fallback-cost",
            "selected": True,
            "reason": "both-successful-cheaper-representation-preferred",
        }
    return {"rule": "rejected", "selected": False, "reason": "both-unsuccessful"}


def run_visible_screening(
    plan_dir: str | Path,
    local_semantic_admission_dir: str | Path,
    *,
    run_id: str,
    execute_action: Callable[[Mapping[str, Any], Mapping[str, Any], str], Mapping[str, Any]],
    output_dir: str | Path,
) -> dict[str, Any]:
    """Screen candidates in frozen order, stopping at the first primary hit.

    ``execute_action`` receives the frozen candidate, the frozen action, and a
    fresh idempotency key, and must return an already source-bound validated
    trial result.  Transport and FlowMesh pinning stay in the caller so this
    module remains candidate- and deployment-independent.
    """

    plan, candidates = load_visible_screening_plan(plan_dir)
    admission_root = Path(local_semantic_admission_dir).resolve()
    inputs = load_full_flow_local_semantic_execution_inputs(admission_root)
    _require(
        str(inputs.admission["admission_sha256"])
        == plan["source_admission_sha256"],
        "screening plan binds a different semantic admission",
    )
    run_id = _identifier(run_id, "run_id")
    target = Path(output_dir).resolve()
    _require(not target.exists(), f"output directory already exists: {target}")

    actions = plan["screening_actions"]
    primary_action_id = str(plan["selection_rule"]["primary_action_id"])
    cheap_action_id = str(plan["selection_rule"]["cheap_action_id"])

    rows: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    selected: dict[str, Any] | None = None
    workflows = 0

    def _persist(status: str, failure: Mapping[str, Any] | None) -> dict[str, Any]:
        """Freeze whatever screening work has completed so far.

        Every action that reached the model is LLM-bearing and paid for, so a
        later failure must never discard an earlier authenticated result.
        """

        result_bytes = _jsonl_bytes(rows)
        receipt = {
            "schema_version": VISIBLE_SCREENING_RUN_SCHEMA_VERSION,
            "selection_kind": SELECTION_KIND,
            "screening_id": plan["screening_id"],
            "run_id": run_id,
            "plan_sha256": plan["plan_sha256"],
            "source_admission_sha256": plan["source_admission_sha256"],
            "candidates_screened": len(summaries),
            "workflows_submitted": workflows,
            "llm_bearing_calls": workflows,
            "candidate_summaries": summaries,
            "selected_workload_id": (
                None if selected is None else selected["workload_id"]
            ),
            "selected_object_id": (
                None if selected is None else selected["object_id"]
            ),
            "selection_rule_satisfied": (
                None
                if selected is None
                else selected["selection_verdict"]["rule"]
            ),
            "results_sha256": _sha256(result_bytes),
            "failure": None if failure is None else dict(failure),
            "credentials_recorded": False,
            "hidden_label_values_included": False,
            "eligible_for_scientific_claims": False,
            "formal_sampling_claimed": False,
            "status": status,
        }
        receipt["receipt_sha256"] = _sha256(_canonical(receipt))
        _write_package(
            target,
            {RESULTS_NAME: result_bytes, RECEIPT_NAME: _canonical(receipt)},
        )
        return receipt

    for candidate in candidates:
        if selected is not None:
            break
        outcomes: dict[str, dict[str, Any]] = {}
        for action in candidate["actions"]:
            _require(
                workflows < plan["max_workflows"],
                "screening exceeded its frozen workflow budget",
            )
            idempotency_key = _sha256(
                _canonical({
                    "domain": "pathfinder.visible-screening-action/v1",
                    "screening_id": plan["screening_id"],
                    "run_id": run_id,
                    "workload_id": candidate["workload_id"],
                    "action_id": action["action_id"],
                    "trial_key": action["trial_key"],
                })
            )
            try:
                result = execute_action(candidate, action, idempotency_key)
            except Exception as exc:
                workflows += 1
                _persist(
                    "INCOMPLETE_VISIBLE_SCREENING",
                    {
                        "workload_id": candidate["workload_id"],
                        "action_id": action["action_id"],
                        "trial_key": action["trial_key"],
                        "idempotency_key": idempotency_key,
                        "failure_type": type(exc).__name__,
                        "failure_reason": str(exc),
                    },
                )
                raise
            workflows += 1
            evidence = result["semantic_route_evidence"]
            prediction = str(evidence["n1_score_request"]["predicted_answer"])
            outcome = classify_action_outcome(
                success_scoring_rule=candidate["success_scoring_rule"],
                predicted_answer=prediction,
                task_success=bool(evidence["scoring"]["task_success"]),
            )
            model_input = evidence["model_input"]
            row = {
                "screening_id": plan["screening_id"],
                "run_id": run_id,
                "order_index": candidate["order_index"],
                "workload_id": candidate["workload_id"],
                "object_id": candidate["object_id"],
                "stratum": candidate["stratum"],
                "action_id": action["action_id"],
                "design_id": action["design_id"],
                "executor_node_id": action["executor_node_id"],
                "route_family": action["route_family"],
                "trial_key": action["trial_key"],
                "semantic_input_profile_id": model_input[
                    "semantic_input_profile_id"
                ],
                "n6_input_mode": model_input["mode"],
                "direct_video_input": bool(model_input["direct_video_input"]),
                "model_input_bytes": int(model_input["payload_size_bytes"]),
                "frame_count": model_input["frame_count"],
                "predicted_answer_sha256": _sha256(prediction.encode("utf-8")),
                "idempotency_key": idempotency_key,
                "execution_transport": result["execution_transport"],
                "n1_score_authenticity_verified": result[
                    "n1_score_authenticity_verified"
                ],
                "route_evidence_sha256": result["route_evidence_sha256"],
                "llm_called": result["llm_called"],
                **outcome,
            }
            rows.append(row)
            outcomes[action["action_id"]] = outcome
        verdict = evaluate_selection_rule(
            outcomes,
            primary_action_id=primary_action_id,
            cheap_action_id=cheap_action_id,
        )
        summaries.append({
            "order_index": candidate["order_index"],
            "workload_id": candidate["workload_id"],
            "object_id": candidate["object_id"],
            "stratum": candidate["stratum"],
            "outcomes": {
                action_id: dict(value) for action_id, value in outcomes.items()
            },
            "selection_verdict": verdict,
        })
        if verdict["rule"] == "primary" and verdict["selected"]:
            selected = summaries[-1]

    if selected is None:
        for summary in summaries:
            if summary["selection_verdict"]["selected"]:
                selected = summary
                break

    return _persist("COMPLETED_VISIBLE_SCREENING", None)


def verify_visible_screening_run(
    run_dir: str | Path,
    *,
    plan_dir: str | Path,
) -> dict[str, Any]:
    """Re-verify a screening receipt against its frozen plan."""

    root = Path(run_dir).resolve()
    verify_checksums(root)
    plan, candidates = load_visible_screening_plan(plan_dir)
    receipt = json.loads((root / RECEIPT_NAME).read_text(encoding="utf-8"))
    supplied = dict(receipt)
    recorded = supplied.pop("receipt_sha256", None)
    _require(
        _sha256(_canonical(supplied)) == recorded,
        "screening receipt digest changed",
    )
    _require(
        receipt.get("plan_sha256") == plan["plan_sha256"]
        and receipt.get("screening_id") == plan["screening_id"]
        and receipt.get("selection_kind") == SELECTION_KIND
        and receipt.get("eligible_for_scientific_claims") is False,
        "screening receipt does not bind its frozen plan",
    )
    _require(
        receipt.get("status")
        in {"COMPLETED_VISIBLE_SCREENING", "INCOMPLETE_VISIBLE_SCREENING"},
        "screening receipt status is unsupported",
    )
    result_bytes = (root / RESULTS_NAME).read_bytes()
    _require(
        _sha256(result_bytes) == receipt.get("results_sha256"),
        "screening results differ from their frozen digest",
    )
    rows = [
        json.loads(line)
        for line in result_bytes.decode("utf-8").splitlines()
        if line.strip()
    ]
    failed = receipt.get("failure") is not None
    _require(
        len(rows) == receipt["workflows_submitted"] - (1 if failed else 0),
        "screening result count differs from its recorded workflow count",
    )
    by_order = {row["order_index"] for row in rows}
    allowed = {row["order_index"] for row in candidates}
    _require(by_order <= allowed, "screening executed an unplanned candidate")
    _require(
        all(row["execution_transport"] == "flowmesh" for row in rows)
        and all(row["n1_score_authenticity_verified"] is True for row in rows)
        and all(row["llm_called"] is True for row in rows),
        "screening result did not complete the authenticated runtime path",
    )
    return {
        "status": "VERIFIED",
        "screening_id": receipt["screening_id"],
        "run_id": receipt["run_id"],
        "candidates_screened": receipt["candidates_screened"],
        "workflows_submitted": receipt["workflows_submitted"],
        "selected_workload_id": receipt["selected_workload_id"],
        "selection_rule_satisfied": receipt["selection_rule_satisfied"],
        "eligible_for_scientific_claims": False,
        "selection_kind": SELECTION_KIND,
    }


__all__ = [
    "CANDIDATES_NAME",
    "PLAN_NAME",
    "RECEIPT_NAME",
    "RESULTS_NAME",
    "SELECTION_KIND",
    "VISIBLE_SCREENING_PLAN_SCHEMA_VERSION",
    "VISIBLE_SCREENING_RUN_SCHEMA_VERSION",
    "VisibleScreeningError",
    "build_candidate_pool",
    "classify_action_outcome",
    "evaluate_selection_rule",
    "freeze_visible_screening_plan",
    "load_visible_screening_plan",
    "run_visible_screening",
    "verify_checksums",
    "verify_visible_screening_run",
]
