from __future__ import annotations

from typing import Any, Mapping

from .contracts import (
    FlowMeshSettings,
    SubmittedWorkflow,
    TerminalWorkflow,
)


class FlowMeshDependencyError(RuntimeError):
    """Raised when the optional FlowMesh SDK is unavailable."""


class SdkFlowMeshClient:
    """Small adapter around the public FlowMesh Python SDK."""

    def __init__(self, settings: FlowMeshSettings):
        try:
            from flowmesh import FlowMesh
        except ImportError as exc:
            raise FlowMeshDependencyError(
                "FlowMesh integration dependencies are not installed. "
                "Install with `python -m pip install -e .[flowmesh]` or "
                "install the local SDK from D:\\Code\\FlowMesh\\sdk."
            ) from exc
        self._client = FlowMesh(
            base_url=settings.base_url,
            api_key=settings.api_key,
        )

    def submit(self, workflow: Mapping[str, Any]) -> SubmittedWorkflow:
        response = self._client.workflows.submit(dict(workflow))
        return SubmittedWorkflow(
            workflow_id=response.workflow_id,
            task_ids=tuple(task.task_id for task in response.tasks),
        )

    def wait(
        self,
        workflow_id: str,
        poll_interval_seconds: float,
    ) -> TerminalWorkflow:
        response = self._client.workflows.wait(
            workflow_id,
            interval=poll_interval_seconds,
        )
        status = getattr(response.status, "value", response.status)
        return TerminalWorkflow(
            workflow_id=response.workflow_id,
            status=str(status).upper(),
        )

    def retrieve_result(self, task_id: str) -> dict[str, Any]:
        return self._client.results.retrieve(task_id)

    def close(self) -> None:
        self._client.close()
