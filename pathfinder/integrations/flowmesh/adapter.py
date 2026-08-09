from __future__ import annotations

from typing import Any

from .contracts import (
    FlowMeshAgentRun,
    FlowMeshAgentRunRequest,
    FlowMeshClientProtocol,
    FlowMeshSettings,
)
from .gateway import AccessGateway
from .workflow import build_agent_workflow, workflow_selected_worker


class FlowMeshRunError(RuntimeError):
    """Raised when FlowMesh cannot complete a Pathfinder Agent session."""


class FlowMeshPinningError(FlowMeshRunError):
    """Raised when a requested worker pin cannot be applied."""


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

    def resolve_selected_worker(self) -> str | None:
        """Return the concrete worker ID this session must run on.

        Returns None only when no pin was requested. A requested pin that
        cannot be resolved raises instead of falling back to free scheduling.
        """
        if self.settings.worker_id is not None:
            return self.settings.worker_id.strip()
        if self.settings.worker_alias is None:
            return None
        resolver = getattr(self.client, "resolve_worker_alias", None)
        if not callable(resolver):
            raise FlowMeshPinningError(
                "FlowMesh client cannot resolve worker aliases, so the "
                f"requested pin to alias '{self.settings.worker_alias}' "
                "cannot be honoured"
            )
        try:
            resolved = resolver(self.settings.worker_alias)
        except FlowMeshRunError:
            raise
        except Exception as exc:
            raise FlowMeshPinningError(
                "Cannot resolve FlowMesh worker alias "
                f"'{self.settings.worker_alias}': {exc}"
            ) from exc
        if not isinstance(resolved, str) or not resolved.strip():
            raise FlowMeshPinningError(
                f"FlowMesh worker alias '{self.settings.worker_alias}' "
                "resolved to an empty worker ID"
            )
        return resolved.strip()

    def run(self, request: FlowMeshAgentRunRequest) -> FlowMeshAgentRun:
        selected_worker_id = self.resolve_selected_worker()
        session = self.gateway.register_session(request)
        workflow = build_agent_workflow(
            session.session_id,
            request,
            self.settings,
            selected_worker_id=selected_worker_id,
        )
        # Guard against a builder regression quietly dropping the pin: an
        # unpinned run on a shared deployment would break experiment
        # isolation without any visible error.
        if self.settings.pinning_requested:
            emitted = workflow_selected_worker(workflow)
            if emitted != selected_worker_id:
                self.gateway.store.finish_session(
                    session.session_id,
                    status="FAILED",
                    final_answer=None,
                )
                raise FlowMeshPinningError(
                    "Pathfinder requested a worker pin but the generated "
                    f"workflow carries {emitted!r} instead of "
                    f"{selected_worker_id!r}"
                )
        try:
            if self.settings.validate_before_submit:
                self._validate(workflow)
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

    def _validate(self, workflow: dict[str, Any]) -> None:
        validator = getattr(self.client, "validate", None)
        if not callable(validator):
            raise FlowMeshRunError(
                "FlowMesh client does not support workflow validation"
            )
        validation = validator(workflow)
        if not validation.ok:
            detail = "; ".join(validation.errors) or "no detail reported"
            raise FlowMeshRunError(
                f"FlowMesh rejected the Pathfinder workflow: {detail}"
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
