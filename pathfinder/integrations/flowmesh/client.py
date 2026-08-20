from __future__ import annotations

from typing import Any, Mapping

from .contracts import (
    FlowMeshSettings,
    FlowMeshWorkerIdentity,
    SubmittedWorkflow,
    TerminalWorkflow,
    WorkflowValidation,
)
from .redaction import redact_secrets


def _optional_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _required_text(value: Any, name: str) -> str:
    text = _optional_text(value)
    if text is None:
        raise ValueError(f"FlowMesh {name} cannot be empty")
    return text


def _worker_identity(worker: Any) -> "FlowMeshWorkerIdentity | None":
    worker_id = _optional_text(getattr(worker, "id", None))
    if worker_id is None:
        return None
    status = getattr(worker, "status", None)
    return FlowMeshWorkerIdentity(
        worker_id=worker_id,
        alias=_optional_text(getattr(worker, "alias", None)),
        status=_optional_text(str(getattr(status, "value", status) or "")),
        namespace=_optional_text(getattr(worker, "namespace", None)),
        cluster=_optional_text(getattr(worker, "cluster", None)),
        node_alias=_optional_text(getattr(worker, "node_alias", None)),
    )


class FlowMeshDependencyError(RuntimeError):
    """Raised when the optional FlowMesh SDK is unavailable."""


class WorkerResolutionError(RuntimeError):
    """Raised when a worker alias does not resolve to exactly one worker."""


class TaskDetailUnavailableError(RuntimeError):
    """Raised when no public API is available to read task failure detail."""


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
        # A terminal workflow carries no free-text error, but which tasks the
        # Root considers failed or cancelled is the difference between "the
        # task ran and failed" and "the task was never dispatched at all".
        failed = tuple(
            str(value)
            for value in (getattr(response, "failed_tasks", None) or [])
        )
        cancelled = tuple(
            str(value)
            for value in (getattr(response, "cancelled_tasks", None) or [])
        )
        dispatched = tuple(
            str(value)
            for value in (getattr(response, "dispatched_tasks", None) or [])
        )
        notes: list[str] = []
        if failed:
            notes.append("root-reported failed tasks: " + ", ".join(failed))
        if cancelled:
            notes.append(
                "root-reported cancelled tasks: " + ", ".join(cancelled)
            )
        if not dispatched:
            notes.append(
                "the Root never recorded a dispatched task for this workflow"
            )
        return TerminalWorkflow(
            workflow_id=response.workflow_id,
            status=str(status).upper(),
            failed_task_ids=failed,
            cancelled_task_ids=cancelled,
            detail="; ".join(notes) or None,
        )

    def retrieve_result(self, task_id: str) -> dict[str, Any]:
        return self._client.results.retrieve(task_id)

    def describe_task_failure(self, task_id: str) -> dict[str, Any] | None:
        """Return read-only terminal failure detail for one task.

        Uses the public ``tasks.retrieve`` resource, which on flowmesh-sdk
        0.1.8rc1 is a plain ``GET /tasks/{id}``: it never stops, retries, or
        otherwise mutates the task. If an SDK does not expose that public
        method this raises rather than reaching for a private path; the caller
        turns that into "no additional detail available". Everything echoed
        back is redacted, because a worker-side error string can quote a
        request header or a signed URL.
        """
        tasks = getattr(self._client, "tasks", None)
        retrieve = getattr(tasks, "retrieve", None)
        if not callable(retrieve):
            raise TaskDetailUnavailableError(
                "this FlowMesh SDK exposes no public tasks.retrieve, so task "
                "failure detail cannot be read"
            )
        info = retrieve(task_id)
        status = getattr(info, "status", None)
        detail = _optional_text(
            getattr(info, "error", None)
        ) or _optional_text(getattr(info, "last_error", None))
        payload = {
            "task_status": str(getattr(status, "value", status) or "").upper()
            or None,
            "attempts": getattr(info, "attempts", None),
            "max_attempts": getattr(info, "max_attempts", None),
            "assigned_worker": _optional_text(
                getattr(info, "assigned_worker", None)
            ),
            "last_failed_worker": _optional_text(
                getattr(info, "last_failed_worker", None)
            ),
            "detail": redact_secrets(detail) if detail else None,
        }
        if not any(value is not None for value in payload.values()):
            return None
        return payload

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

    def _list_current_workers(
        self,
        *,
        worker_id: str | None = None,
        alias: str | None = None,
    ) -> list[Any]:
        """List current workers, narrowing server side when the SDK allows.

        ``workers.list`` on the pinned flowmesh-sdk 0.1.8rc1 accepts both
        ``worker_id`` and ``alias`` (they become the ``id`` and ``alias``
        query parameters). An SDK that does not is not a reason to fail: fall
        back to the unnarrowed current-worker list, since the caller filters
        by ID again anyway and an alias mismatch is caught by the same
        exactly-one check.
        """
        filters: dict[str, Any] = {"stale": False}
        if worker_id is not None:
            filters["worker_id"] = worker_id
        if alias is not None:
            filters["alias"] = alias
        try:
            return list(self._client.workers.list(**filters))
        except TypeError:
            fallback = list(self._client.workers.list(stale=False))
            if alias is None:
                return fallback
            return [
                worker
                for worker in fallback
                if _optional_text(getattr(worker, "alias", None)) == alias
            ]

    def describe_current_worker(
        self,
        *,
        worker_id: str | None = None,
        alias: str | None = None,
    ) -> FlowMeshWorkerIdentity:
        """Return the one current worker the configured Root reports.

        FlowMesh reassigns worker IDs across restarts, so a pin is resolved
        against the Root immediately before it is used. Anything other than
        exactly one match raises: pinning must never degrade into an unpinned
        run, and an exact ID is a claim to verify rather than a reason to skip
        the Root entirely. A worker that only the local Node Server can see is
        not a worker this Root will dispatch to.

        ``stale=False`` excludes workers FlowMesh no longer considers live.
        Stale entries linger after a restart under the same alias, so without
        the filter a restarted worker looks like two matches and pinning
        fails, or the single match returned is a dead ID.
        """
        if (worker_id is None) == (alias is None):
            raise ValueError(
                "describe_current_worker requires exactly one of worker_id "
                "or alias"
            )
        if worker_id is not None:
            selector, value = "ID", _required_text(worker_id, "worker ID")
            matches = self._list_current_workers(worker_id=value)
        else:
            selector, value = "alias", _required_text(alias, "worker alias")
            matches = self._list_current_workers(alias=value)

        identities = [
            identity
            for identity in (_worker_identity(worker) for worker in matches)
            if identity is not None
        ]
        if worker_id is not None:
            # Re-check client side: a Root that silently ignores the id filter
            # would otherwise turn "verify this exact worker" into "return
            # whichever worker happens to be first".
            identities = [
                identity
                for identity in identities
                if identity.worker_id == value
            ]
        if not identities:
            raise WorkerResolutionError(
                f"FlowMesh worker {selector} '{value}' matched no current "
                "worker; refusing to submit an unpinned Pathfinder workflow"
            )
        if len(identities) > 1:
            raise WorkerResolutionError(
                f"FlowMesh worker {selector} '{value}' matched "
                f"{len(identities)} current workers "
                f"({', '.join(sorted(item.worker_id for item in identities))}"
                "); a Pathfinder session must pin exactly one worker"
            )
        return identities[0]

    def resolve_worker_alias(self, alias: str) -> str:
        """Return the current worker ID for a stable alias."""
        return self.describe_current_worker(alias=alias).worker_id

    def close(self) -> None:
        self._client.close()
