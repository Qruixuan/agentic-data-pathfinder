from __future__ import annotations

from typing import Any

from .contracts import (
    FlowMeshAgentRun,
    FlowMeshAgentRunRequest,
    FlowMeshClientProtocol,
    FlowMeshSettings,
)
from .gateway import AccessGateway
from .workflow import build_agent_workflow


class FlowMeshRunError(RuntimeError):
    """Raised when FlowMesh cannot complete a Pathfinder Agent session."""


class FlowMeshAgentAdapter:
    """Coordinates one Pathfinder session through an external FlowMesh."""

    def __init__(
        self,
        client: FlowMeshClientProtocol,
        gateway: AccessGateway,
        settings: FlowMeshSettings,
    ):
        self.client = client
        self.gateway = gateway
        self.settings = settings

    def run(self, request: FlowMeshAgentRunRequest) -> FlowMeshAgentRun:
        session = self.gateway.register_session(request)
        workflow = build_agent_workflow(
            session.session_id,
            request,
            self.settings,
        )
        try:
            submitted = self.client.submit(workflow)
            if len(submitted.task_ids) != 1:
                raise FlowMeshRunError(
                    "Pathfinder expects one FlowMesh Agent task per session, "
                    f"but submission returned {len(submitted.task_ids)} tasks"
                )
            task_id = submitted.task_ids[0]
            self.gateway.store.bind_flowmesh(
                session.session_id,
                submitted.workflow_id,
                task_id,
            )
            terminal = self.client.wait(
                submitted.workflow_id,
                self.settings.poll_interval_seconds,
            )
            if terminal.status != "DONE":
                raise FlowMeshRunError(
                    f"FlowMesh workflow {terminal.workflow_id} ended with "
                    f"status {terminal.status}"
                )
            raw_result = self.client.retrieve_result(task_id)
            final_answer = extract_agent_answer(raw_result)
            self.gateway.reconcile_artifact_telemetry(session.session_id)
            self.gateway.store.finish_session(
                session.session_id,
                status="DONE",
                final_answer=final_answer,
            )
        except Exception:
            self.gateway.store.finish_session(
                session.session_id,
                status="FAILED",
                final_answer=None,
            )
            raise

        events = tuple(
            event.to_dict()
            for event in self.gateway.store.list_events(session.session_id)
        )
        return FlowMeshAgentRun(
            session_id=session.session_id,
            workflow_id=submitted.workflow_id,
            task_id=task_id,
            status="DONE",
            final_answer=final_answer,
            access_events=events,
            raw_result=raw_result,
        )


def extract_agent_answer(payload: dict[str, Any]) -> str:
    result = payload.get("result", payload)
    if not isinstance(result, dict):
        raise FlowMeshRunError("FlowMesh result payload is not an object")
    items = result.get("items")
    if not isinstance(items, list) or not items:
        raise FlowMeshRunError("FlowMesh Agent result has no output items")
    first = items[0]
    if not isinstance(first, dict):
        raise FlowMeshRunError("FlowMesh Agent output item is not an object")
    answer = first.get("output") or first.get("response")
    if not isinstance(answer, str) or not answer.strip():
        raise FlowMeshRunError("FlowMesh Agent result contains an empty answer")
    return answer
