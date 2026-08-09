from __future__ import annotations

from typing import Any, Mapping

from .contracts import (
    FlowMeshSettings,
    SubmittedWorkflow,
    TerminalWorkflow,
    WorkflowValidation,
)


class FlowMeshDependencyError(RuntimeError):
    """Raised when the optional FlowMesh SDK is unavailable."""


class WorkerResolutionError(RuntimeError):
    """Raised when a worker alias does not resolve to exactly one worker."""


class SdkFlowMeshClient:
    """Small adapter around the public FlowMesh Python SDK."""

    def __init__(self, settings: FlowMeshSettings):
        try:
            from flowmesh import FlowMesh
        except ImportError as exc:
            raise FlowMeshDependencyError(
                "FlowMesh integration dependencies are not installed. "
                "Install with `python -m pip install -e .[flowmesh]` or "
                "install the local SDK from /path/to/FlowMesh/sdk."
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

    def validate(self, workflow: Mapping[str, Any]) -> WorkflowValidation:
        """Validate a workflow through FlowMesh without submitting it.

        Uses the SDK's ``workflows.validate`` endpoint, which parses and
        checks the payload but neither enqueues a workflow nor dispatches a
        task.
        """
        response = self._client.workflows.validate(dict(workflow))
        errors: list[str] = []
        for task in getattr(response, "tasks", None) or []:
            for error in getattr(task, "errors", None) or []:
                errors.append(str(error))
        top_level = getattr(response, "errors", None) or []
        errors.extend(str(error) for error in top_level)
        return WorkflowValidation(
            ok=bool(getattr(response, "ok", False)),
            errors=tuple(errors),
        )

    def resolve_worker_alias(self, alias: str) -> str:
        """Return the current worker ID for a stable alias.

        FlowMesh reassigns worker IDs across restarts, so the alias is
        resolved immediately before submission. Anything other than exactly
        one match raises: pinning must never degrade into an unpinned run.
        """
        if not isinstance(alias, str) or not alias.strip():
            raise ValueError("FlowMesh worker alias cannot be empty")
        alias = alias.strip()
        matches = self._client.workers.list(alias=alias)
        worker_ids = [
            worker_id
            for worker_id in (
                getattr(worker, "id", None) for worker in matches
            )
            if isinstance(worker_id, str) and worker_id.strip()
        ]
        if not worker_ids:
            raise WorkerResolutionError(
                f"FlowMesh worker alias '{alias}' matched no worker; refusing "
                "to submit an unpinned Pathfinder workflow"
            )
        if len(worker_ids) > 1:
            raise WorkerResolutionError(
                f"FlowMesh worker alias '{alias}' matched "
                f"{len(worker_ids)} workers ({', '.join(sorted(worker_ids))}); "
                "a Pathfinder session must pin exactly one worker"
            )
        return worker_ids[0]

    def close(self) -> None:
        self._client.close()
