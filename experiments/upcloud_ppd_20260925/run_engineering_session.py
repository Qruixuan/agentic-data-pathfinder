"""Run one public PPD engineering session using the deployed Gateway state.

Preflight only reads the Root, configuration, and session database. Execute
submits exactly one pinned FlowMesh workflow and never retries it implicitly.
Receipts exclude the question, raw model response, credentials, and artifact
URLs. A uniquely parsed final option ID may be recorded separately.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener

from pathfinder.config import load_config
from pathfinder.distributed.registry import load_endpoint_registry
from pathfinder.distributed.routing import (
    build_routed_gateway_backend,
    close_routed_backend,
)
from pathfinder.integrations.flowmesh.adapter import FlowMeshAgentAdapter
from pathfinder.integrations.flowmesh.client import SdkFlowMeshClient
from pathfinder.integrations.flowmesh.contracts import (
    FlowMeshAgentRunRequest,
    FlowMeshSettings,
)
from pathfinder.integrations.flowmesh.gateway import AccessGateway, SQLiteSessionStore
from pathfinder.integrations.flowmesh.visual_artifact import N6VisualInferenceClient
from pathfinder.integrations.flowmesh.workflow import build_agent_workflow


EXPECTED_PLACEMENT = ("pathfinder", "upcloud-sg-sin1", "pathfinder-n7")
_OPTION_TOKEN = re.compile(r"(?<![A-Za-z])[A-E](?![A-Za-z])")


def _extract_explicit_final_option(answer: str) -> tuple[str, str] | None:
    """Extract a structurally unique option without rewriting the raw answer.

    This is an engineering closure check, not the frozen N1 scoring rule.
    A verbose answer needs one bare or bold option on its final line and no
    other standalone option marker anywhere in the response.
    """
    stripped = answer.strip()
    if not stripped:
        return None
    if re.fullmatch(r"[A-E]", stripped):
        return stripped, "bare-option"
    final_line = stripped.splitlines()[-1].strip()
    for pattern, format_name in (
        (r"([A-E])", "bare-final-line"),
        (r"\*\*([A-E])\*\*", "markdown-bold-final-line"),
    ):
        match = re.fullmatch(pattern, final_line)
        if match and _OPTION_TOKEN.findall(stripped) == [match.group(1)]:
            return match.group(1), format_name
    return None


def _explicit_final_option_format(answer: str) -> str | None:
    parsed = _extract_explicit_final_option(answer)
    return None if parsed is None else parsed[1]


def _answer_fields(answer: str) -> dict[str, object]:
    parsed = _extract_explicit_final_option(answer)
    return {
        "answer_present": bool(answer.strip()),
        "answer_is_single_option": parsed is not None
        and parsed[1] == "bare-option",
        "answer_has_explicit_final_option": parsed is not None,
        "answer_format": None if parsed is None else parsed[1],
        "extracted_option_id": None if parsed is None else parsed[0],
    }


def _session_absent(state_db: Path, session_id: str) -> None:
    if not state_db.is_file():
        raise RuntimeError("deployed Gateway state database is missing")
    connection = sqlite3.connect(f"file:{state_db}?mode=ro", uri=True)
    try:
        row = connection.execute(
            "SELECT 1 FROM gateway_sessions WHERE session_id = ?", (session_id,)
        ).fetchone()
    finally:
        connection.close()
    if row is not None:
        raise RuntimeError("fresh session identity is already present")


def _health(url: str, node_id: str) -> None:
    opener = build_opener(ProxyHandler({}))
    with opener.open(url, timeout=5) as response:
        value = json.load(response)
        if response.status != 200 or value.get("status") != "ok":
            raise RuntimeError("PPD dependency health failed")
        if value.get("node_id") != node_id:
            raise RuntimeError("PPD dependency identity differs")


def _auth_status(origin: str, token: str) -> int:
    request = Request(
        origin.rstrip("/") + "/v1/access", data=b"{}", method="POST",
        headers={"Authorization": "Bearer " + token,
                 "Content-Type": "application/json"},
    )
    try:
        with build_opener(ProxyHandler({})).open(request, timeout=8) as response:
            return response.status
    except HTTPError as exc:
        return exc.code


def _request(args: argparse.Namespace) -> FlowMeshAgentRunRequest:
    question = args.question_file.read_text(encoding="utf-8").strip()
    if not question or len(question) > 8192:
        raise RuntimeError("public question file is empty or too large")
    return FlowMeshAgentRunRequest(
        question=question,
        design_id=args.design,
        task_class_id="video_qa",
        trial_id=args.trial_id,
        session_id=args.session_id,
        object_id=args.object_id,
    )


def _client_settings(worker_alias: str, agent_config_name: str) -> FlowMeshSettings:
    settings = FlowMeshSettings.from_environment(
        base_url="http://10.70.0.19:8000",
        worker_alias=worker_alias,
        agent_config_name=agent_config_name,
        validate_before_submit=True,
        task_timeout_seconds=600,
    )
    if settings.base_url != "http://10.70.0.19:8000":
        raise RuntimeError("FlowMesh Root origin differs from the N7 contract")
    return settings


def _worker(client: SdkFlowMeshClient, worker_alias: str) -> str:
    identity = client.describe_current_worker(alias=worker_alias)
    placement = (identity.namespace, identity.cluster, identity.node_alias)
    if placement != EXPECTED_PLACEMENT:
        raise RuntimeError("pinned worker placement differs")
    if identity.status not in {"RUNNING", "IDLE"}:
        raise RuntimeError("pinned worker is not ready")
    return identity.worker_id


def _preflight(
    args: argparse.Namespace, request: FlowMeshAgentRunRequest,
    client: SdkFlowMeshClient, settings: FlowMeshSettings,
) -> dict[str, object]:
    print("preflight_stage=session_identity", file=sys.stderr, flush=True)
    _session_absent(args.state_db, request.session_id)
    print("preflight_stage=frozen_config", file=sys.stderr, flush=True)
    config = load_config(args.config)
    registry = load_endpoint_registry(args.endpoint_registry)
    if request.design_id not in config.designs or request.object_id is None:
        raise RuntimeError("public case does not bind the frozen PPD design")
    if len(registry.endpoints) != 3:
        raise RuntimeError("PPD endpoint registry is incomplete")
    print("preflight_stage=dependency_health", file=sys.stderr, flush=True)
    for url, node in (
        ("http://pathfinder-full-flow-ppd-n3-raw:19133/healthz", "N3"),
        ("http://pathfinder-full-flow-ppd-n4-remote:19134/healthz", "N4"),
        ("http://pathfinder-full-flow-ppd-n4-n7-replica:19137/healthz", "N4"),
        ("http://pathfinder-full-flow-ppd-n6-semantic:18886/healthz", "N6"),
    ):
        _health(url, node)
    with socket.create_connection(("127.0.0.1", 18765), timeout=3):
        pass
    for endpoint in registry.endpoints.values():
        origin = os.getenv(endpoint.base_url_env)
        token = os.getenv(endpoint.token_env)
        if not origin or not token:
            raise RuntimeError("PPD endpoint configuration is incomplete")
        if _auth_status(origin, token) != 400:
            raise RuntimeError("PPD valid-token validation boundary differs")
        if _auth_status(origin, "invalid-ppd-preflight") != 401:
            raise RuntimeError("PPD invalid-token boundary differs")
    n6_url = os.getenv("PATHFINDER_PPD_N6_SEMANTIC_URL")
    n6_token = os.getenv("PATHFINDER_PPD_N6_SEMANTIC_TOKEN")
    if not n6_url or not n6_token:
        raise RuntimeError("N6 visual client configuration is incomplete")
    N6VisualInferenceClient(
        n6_url, n6_token,
        private_http_service_name=os.getenv(
            "PATHFINDER_PPD_N6_PRIVATE_HTTP_SERVICE_NAME"
        ),
    )
    print("preflight_stage=root_worker", file=sys.stderr, flush=True)
    worker_id = _worker(client, args.worker_alias)
    workflow = build_agent_workflow(
        request.session_id, request, settings, selected_worker_id=worker_id
    )
    print("preflight_stage=workflow_validation", file=sys.stderr, flush=True)
    validation = client.validate(workflow)
    if not validation.ok:
        raise RuntimeError("FlowMesh workflow validation failed")
    return {
        "status": "PREFLIGHT_OK", "session_id": request.session_id,
        "worker_alias": args.worker_alias, "observed_worker_id": worker_id,
        "agent_config_name": args.agent_config_name,
        "design_id": request.design_id,
        "workflow_validated": True, "workflow_submitted": False,
        "llm_called": False, "claim_class": "engineering-conformance",
    }


def _execute(
    args: argparse.Namespace, request: FlowMeshAgentRunRequest,
    client: SdkFlowMeshClient, settings: FlowMeshSettings,
    preflight: dict[str, object],
) -> dict[str, object]:
    backend, _ = build_routed_gateway_backend(args.endpoint_registry)
    try:
        visual = N6VisualInferenceClient(
            os.environ["PATHFINDER_PPD_N6_SEMANTIC_URL"],
            os.environ["PATHFINDER_PPD_N6_SEMANTIC_TOKEN"],
            private_http_service_name=os.getenv(
                "PATHFINDER_PPD_N6_PRIVATE_HTTP_SERVICE_NAME"
            ),
        )
        gateway = AccessGateway(
            load_config(args.config), SQLiteSessionStore(args.state_db),
            backend, visual_inference_client=visual,
        )
        result = FlowMeshAgentAdapter(client, gateway, settings).run(request)
    finally:
        close_routed_backend(backend)
    accepted = [event for event in result.access_events if event.get("accepted")]
    # A FlowMesh DONE task can contain an agent response with no Gateway
    # access. That is not a completed Pathfinder Agent session. Read the
    # already-uploaded result again for its numeric agent-model usage rather
    # than carrying the preflight's llm_called=false into the receipt.
    task_result = client.retrieve_result(result.task_id)
    usage = task_result.get("usage")
    request_count = (
        usage.get("num_requests") if isinstance(usage, dict) else None
    )
    if not isinstance(request_count, int) or request_count < 0:
        request_count = None
    answer_fields = _answer_fields(result.final_answer)
    closure_verified = bool(accepted) and bool(
        answer_fields["answer_has_explicit_final_option"]
    )
    return {
        **preflight,
        "status": result.status if closure_verified else "INCOMPLETE_AGENT_CLOSURE",
        "workflow_id": result.workflow_id, "task_id": result.task_id,
        "workflow_submitted": True, "access_event_count": len(result.access_events),
        "accepted_representations": [
            event.get("representation_id") for event in accepted
        ],
        **answer_fields,
        "closure_verified": closure_verified,
        "agent_model_request_count": request_count,
        "llm_called": None if request_count is None else request_count > 0,
        "task_success_evaluated": False, "eligible_for_scientific_claims": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("preflight", "execute"))
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--endpoint-registry", type=Path, required=True)
    parser.add_argument("--state-db", type=Path, required=True)
    parser.add_argument("--question-file", type=Path, required=True)
    parser.add_argument("--design", required=True)
    parser.add_argument("--object-id", required=True)
    parser.add_argument("--trial-id", required=True)
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--worker-alias", default="pathfinder_ppd_visual_20260926b")
    parser.add_argument(
        "--agent-config-name",
        choices=("pathfinder_video_visual_cost_aware",
                 "pathfinder_video_visual_qwen_first_offer"),
        default="pathfinder_video_visual_cost_aware",
    )
    parser.add_argument("--receipt", type=Path)
    args = parser.parse_args()
    if args.mode == "execute" and args.receipt is None:
        parser.error("execute requires --receipt")
    client = None
    try:
        request = _request(args)
        settings = _client_settings(args.worker_alias, args.agent_config_name)
        client = SdkFlowMeshClient(settings)
        ready = _preflight(args, request, client, settings)
        result = (
            ready if args.mode == "preflight"
            else _execute(args, request, client, settings, ready)
        )
        if args.receipt is not None:
            args.receipt.parent.mkdir(parents=True, exist_ok=True)
            with args.receipt.open("x", encoding="utf-8", newline="\n") as file:
                json.dump({**result, "recorded_at": datetime.now(timezone.utc).isoformat()}, file, sort_keys=True)
                file.write("\n")
        print(json.dumps(result, sort_keys=True))
        return 0 if args.mode != "execute" or result["closure_verified"] else 2
    except Exception as exc:
        # Never print provider bodies, secrets, signed URLs, or raw answers.
        if args.mode == "execute" and args.receipt is not None:
            workflow_id = task_id = None
            if args.state_db.is_file():
                connection = sqlite3.connect(
                    f"file:{args.state_db}?mode=ro", uri=True
                )
                try:
                    row = connection.execute(
                        "SELECT flowmesh_workflow_id, flowmesh_task_id "
                        "FROM gateway_sessions WHERE session_id = ?",
                        (args.session_id,),
                    ).fetchone()
                    if row is not None:
                        workflow_id, task_id = row
                finally:
                    connection.close()
            failure = {
                "status": "BLOCKED_OR_FAILED", "session_id": args.session_id,
                "error_class": type(exc).__name__,
                "workflow_id": workflow_id, "task_id": task_id,
                "workflow_submitted": workflow_id is not None,
                "eligible_for_scientific_claims": False,
            }
            args.receipt.parent.mkdir(parents=True, exist_ok=True)
            with args.receipt.open("x", encoding="utf-8", newline="\n") as file:
                json.dump(failure, file, sort_keys=True)
                file.write("\n")
        print(json.dumps({"status": "BLOCKED_OR_FAILED", "error_class": type(exc).__name__, "session_id": args.session_id}, sort_keys=True), file=sys.stderr)
        return 2
    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    raise SystemExit(main())
