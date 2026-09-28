"""Run one admitted PPD choice followed by exactly its committed route.

The command does not discover or retry work. A caller must first freeze and
verify the runbook pre-submit ledger, then invoke one fresh admitted session.
Every phase writes a narrow, credential-free receipt before moving forward.
An ambiguous submission is never automatically retried.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
from typing import Any

from pathfinder.integrations.flowmesh.client import SdkFlowMeshClient
from pathfinder.integrations.flowmesh.contracts import FlowMeshSettings
from pathfinder.integrations.flowmesh.route_action_bootstrap import (
    load_route_action_live,
)
from pathfinder.integrations.flowmesh.route_action_gateway import (
    RouteActionGateway,
)
from pathfinder.integrations.flowmesh.route_action_workflow import (
    ROUTE_ACTION_AGENT_CONFIG,
    build_route_choice_workflow,
)
from pathfinder.integrations.flowmesh.semantic_matrix_trial import (
    FlowMeshSemanticTrialExecutor,
    full_flow_hmac_header_provider,
)


_PLACEMENT = ("pathfinder", "upcloud-sg-sin1", "pathfinder-n7")


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_new(path: Path, payload: Mapping[str, Any]) -> None:
    data = (json.dumps(dict(payload), sort_keys=True, separators=(",", ":"))
            + "\n").encode("utf-8")
    with path.open("xb") as handle:
        handle.write(data)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_bytes().splitlines()]


def _sealed_ready(gate_dir: Path) -> dict[str, Any]:
    """Check the local operator gate receipt without trusting a bare flag."""
    receipt = gate_dir / "pre-submit-gates.json"
    checksums = gate_dir / "SHA256SUMS"
    if not receipt.is_file() or not checksums.is_file():
        raise RuntimeError("frozen pre-submit gate package is absent")
    import hashlib

    expected = {}
    for line in checksums.read_text(encoding="ascii").splitlines():
        digest, name = line.split("  ", 1)
        if name in expected or name in {"", ".", ".."} or "/" in name:
            raise RuntimeError("pre-submit gate checksum manifest is invalid")
        expected[name] = digest
    if set(expected) != {"pre-submit-gates.json"}:  # exact file set
        raise RuntimeError("pre-submit gate file set differs")
    actual = hashlib.sha256(receipt.read_bytes()).hexdigest()
    if expected["pre-submit-gates.json"] != actual:
        raise RuntimeError("pre-submit gate checksum differs")
    payload = json.loads(receipt.read_bytes())
    if (payload.get("status") != "READY_FOR_FIRST_FORMAL_SESSION"
            or payload.get("experiment_id")
            != "pathfinder-ppd-physical-path-pilot-20260928-v1"
            or payload.get("workflow_submitted") is not False
            or payload.get("credentials_recorded") is not False
            or payload.get("hidden_labels_included") is not False):
        raise RuntimeError("pre-submit gate does not authorize a session")
    return payload


def _candidate_session(
    candidate_dir: Path, session_id: str,
) -> dict[str, Any]:
    rows = [row for row in _read_jsonl(
        candidate_dir / "route-action-candidates.jsonl"
    ) if row["session_id"] == session_id]
    if not rows:
        raise RuntimeError("session is absent from frozen candidates")
    fields = ("question_id", "physical_design_id", "object_id",
              "public_task_sha256", "question_order", "design_order")
    if any(tuple(row[name] for name in fields) != tuple(
            rows[0][name] for name in fields) for row in rows):
        raise RuntimeError("candidate session has conflicting bindings")
    return rows[0]


def execute_one(
    *, gateway: RouteActionGateway, client: Any,
    choice_settings: FlowMeshSettings,
    route_settings: FlowMeshSettings,
    session: Mapping[str, Any], bound_trials: Sequence[Mapping[str, Any]],
    bound_stages: Sequence[Mapping[str, Any]],
    runtime_header_provider: Callable[[Mapping[str, Any]], Mapping[str, str]],
    output_dir: Path, choice_worker_id: str, route_worker_id: str,
) -> dict[str, Any]:
    """One fail-closed choice→handoff→route, with no implicit retry."""
    if output_dir.exists():
        raise RuntimeError("output identity is already used")
    if not choice_worker_id or not route_worker_id:
        raise RuntimeError("a FlowMesh worker ID is missing")
    if choice_settings.worker_alias == route_settings.worker_alias:
        raise RuntimeError("choice and route worker aliases must differ")
    trial_by_key = {row["trial_key"]: row for row in bound_trials}
    if len(trial_by_key) != len(bound_trials):
        raise RuntimeError("bound trial catalog repeats a key")
    output_dir.mkdir(parents=True, exist_ok=False)
    session_id = session["session_id"]
    _write_new(output_dir / "started.json", {
        "status": "STARTED", "session_id": session_id,
        "question_id": session["question_id"],
        "physical_design_id": session["physical_design_id"],
        "choice_worker_alias": choice_settings.worker_alias,
        "choice_worker_id": choice_worker_id,
        "route_worker_alias": route_settings.worker_alias,
        "route_worker_id": route_worker_id,
        "started_utc": _utc(), "credentials_recorded": False,
    })
    phase = "register"
    try:
        gateway.register_session(
            session_id=session_id,
            question_id=session["question_id"],
            physical_design_id=session["physical_design_id"],
            object_id=session["object_id"],
            public_task_sha256=session["public_task_sha256"],
        )
        phase = "choice_validation"
        workflow = build_route_choice_workflow(
            gateway=gateway, session_id=session_id, settings=choice_settings,
            selected_worker_id=choice_worker_id,
        )
        if not client.validate(workflow).ok:
            raise RuntimeError("FlowMesh choice workflow validation failed")
        phase = "choice_submission"
        submitted = client.submit(workflow)
        if len(submitted.task_ids) != 1:
            raise RuntimeError("choice workflow did not return one task")
        _write_new(output_dir / "choice-submitted.json", {
            "status": "SUBMITTED", "workflow_id": submitted.workflow_id,
            "task_id": submitted.task_ids[0], "submitted_utc": _utc(),
            "credentials_recorded": False,
        })
        phase = "choice_wait"
        terminal = client.wait(
            submitted.workflow_id, choice_settings.poll_interval_seconds,
        )
        if (terminal.status != "DONE"
                or terminal.workflow_id != submitted.workflow_id):
            raise RuntimeError("choice workflow did not finish DONE")
        detail = client.describe_task_failure(submitted.task_ids[0])
        if (not isinstance(detail, Mapping)
                or detail.get("assigned_worker") != choice_worker_id):
            raise RuntimeError("choice task ran on the wrong worker")
        phase = "committed_handoff"
        choice = gateway.bridge.load_choice(session_id)
        trial = trial_by_key.get(choice.trial_key)
        if trial is None:
            raise RuntimeError("committed trial is absent from admission")
        handoff = gateway.handoff_for_session(session_id, trial)
        _write_new(output_dir / "choice-committed.json", {
            "status": "COMMITTED", "action_id": choice.action_id,
            "choice_sha256": choice.choice_sha256,
            "offer_set_sha256": choice.offer_set_sha256,
            "trial_key": choice.trial_key, "run_id": handoff.run_id,
            "cache_episode_id": handoff.cache_episode_id,
            "committed_utc": _utc(), "credentials_recorded": False,
        })
        phase = "selected_route_execution"
        executor = FlowMeshSemanticTrialExecutor(
            client=client, settings=route_settings, run_id=handoff.run_id,
            bound_trials=bound_trials, bound_stages=bound_stages,
            runtime_header_provider=runtime_header_provider,
            api_task_timeout_seconds=route_settings.task_timeout_seconds,
            cache_episode_id=handoff.cache_episode_id,
        )
        result = executor.execute(
            trial=handoff.trial,
            idempotency_key=handoff.idempotency_key,
        )
        if (result.get("status") != "COMPLETE"
                or result.get("credentials_recorded") is not False
                or result.get("n1_score_authenticity_verified") is not True):
            raise RuntimeError("selected route did not complete authentically")
        receipt = {
            "status": "COMPLETE", "session_id": session_id,
            "action_id": choice.action_id, "trial_key": choice.trial_key,
            "run_id": handoff.run_id,
            "cache_episode_id": handoff.cache_episode_id,
            "task_success": result["task_success"],
            "route_evidence_sha256": result["route_evidence_sha256"],
            "n1_score_evidence_sha256": result["n1_score_evidence_sha256"],
            "execution_transport": result["execution_transport"],
            "completed_utc": _utc(), "credentials_recorded": False,
        }
        _write_new(output_dir / "complete.json", receipt)
        return receipt
    except Exception as exc:
        # No automatic retry: a submit/wait exception can be ambiguous.
        _write_new(output_dir / "stopped.json", {
            "status": "STOPPED_NO_RETRY", "phase": phase,
            "error_class": type(exc).__name__, "stopped_utc": _utc(),
            "credentials_recorded": False,
        })
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--live-config", type=Path, required=True)
    parser.add_argument("--candidate-dir", type=Path, required=True)
    parser.add_argument("--runtime-admission-dir", type=Path, required=True)
    parser.add_argument("--pre-submit-gate-dir", type=Path, required=True)
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--choice-worker-alias", required=True)
    parser.add_argument("--route-worker-alias", required=True)
    parser.add_argument("--flowmesh-base-url", required=True)
    args = parser.parse_args()
    gate = _sealed_ready(args.pre_submit_gate_dir)
    live = json.loads(args.live_config.read_bytes())
    for field, requested in (
        ("candidate_dir", args.candidate_dir),
        ("runtime_admission_dir", args.runtime_admission_dir),
    ):
        if Path(live[field]).resolve() != requested.resolve():
            raise RuntimeError(f"{field} differs from live Gateway binding")
    if (args.flowmesh_base_url != "http://10.70.0.19:8000"
            or gate.get("choice_worker_alias") != args.choice_worker_alias
            or gate.get("route_worker_alias") != args.route_worker_alias):
        raise RuntimeError("Root or worker alias differs from pre-submit gate")
    gateway = load_route_action_live(args.live_config)
    session = _candidate_session(args.candidate_dir, args.session_id)
    if not os.environ.get("PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET"):
        raise RuntimeError("route ingress HMAC credential is missing")
    choice_settings = FlowMeshSettings.from_environment(
        base_url=args.flowmesh_base_url,
        worker_alias=args.choice_worker_alias,
        agent_config_name=ROUTE_ACTION_AGENT_CONFIG,
        validate_before_submit=True,
        task_timeout_seconds=900,
    )
    route_settings = FlowMeshSettings.from_environment(
        base_url=args.flowmesh_base_url,
        worker_alias=args.route_worker_alias,
        agent_config_name=ROUTE_ACTION_AGENT_CONFIG,
        validate_before_submit=True,
        task_timeout_seconds=900,
    )
    client = SdkFlowMeshClient(choice_settings)
    try:
        workers = {}
        for role, alias in (
            ("choice", args.choice_worker_alias),
            ("route", args.route_worker_alias),
        ):
            identity = client.describe_current_worker(alias=alias)
            placement = (
                identity.namespace, identity.cluster, identity.node_alias,
            )
            if (placement != _PLACEMENT
                    or identity.status not in {"IDLE", "RUNNING"}):
                raise RuntimeError(f"pinned {role} FlowMesh worker is not ready")
            workers[role] = identity
        trials = _read_jsonl(args.runtime_admission_dir
                             / "admitted-trials.jsonl")
        stages = _read_jsonl(args.runtime_admission_dir
                             / "admitted-stages.jsonl")
        if (not trials or any(
                trial.get("worker_alias") != args.route_worker_alias
                for trial in trials)):
            raise RuntimeError("route worker differs from frozen trials")
        receipt = execute_one(
            gateway=gateway, client=client,
            choice_settings=choice_settings,
            route_settings=route_settings,
            session=session, bound_trials=trials, bound_stages=stages,
            runtime_header_provider=full_flow_hmac_header_provider(
                os.environ["PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET"]
            ),
            output_dir=args.output_dir,
            choice_worker_id=workers["choice"].worker_id,
            route_worker_id=workers["route"].worker_id,
        )
        print(json.dumps({
            "status": receipt["status"], "session_id": receipt["session_id"],
            "action_id": receipt["action_id"],
            "task_success": receipt["task_success"],
            "output_dir": str(args.output_dir),
        }, sort_keys=True))
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({"status": "STOPPED_NO_RETRY",
                          "error_class": type(exc).__name__},
                         sort_keys=True), file=sys.stderr)
        raise SystemExit(2) from None
