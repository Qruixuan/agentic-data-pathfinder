from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Protocol


def _bool_from_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class FlowMeshSettings:
    base_url: str = "http://127.0.0.1:8000"
    api_key: str | None = None
    agent_config_name: str = "pathfinder_video"
    owner: str = "pathfinder"
    task_timeout_seconds: int = 600
    poll_interval_seconds: float = 2.0
    worker_id: str | None = None
    """Pin the session to this exact FlowMesh worker ID, e.g. ``wkr-16``."""
    worker_alias: str | None = None
    """Pin the session to the worker currently holding this stable alias.

    FlowMesh worker IDs are reassigned when a worker restarts, so an alias is
    the durable way to name the Pathfinder worker across runs. It is resolved
    to a concrete ID immediately before submission.
    """
    validate_before_submit: bool = False
    """Validate the workflow through FlowMesh before submitting it."""

    def __post_init__(self) -> None:
        if not self.base_url.strip():
            raise ValueError("FlowMesh base_url cannot be empty")
        if not self.agent_config_name.strip():
            raise ValueError("FlowMesh agent_config_name cannot be empty")
        if self.task_timeout_seconds <= 0:
            raise ValueError("FlowMesh task_timeout_seconds must be positive")
        if self.poll_interval_seconds <= 0:
            raise ValueError("FlowMesh poll_interval_seconds must be positive")
        if self.worker_id is not None and self.worker_alias is not None:
            raise ValueError(
                "FlowMesh worker_id and worker_alias are mutually exclusive; "
                "set at most one to pin a Pathfinder session"
            )
        if self.worker_id is not None and not self.worker_id.strip():
            raise ValueError("FlowMesh worker_id cannot be blank")
        if self.worker_alias is not None and not self.worker_alias.strip():
            raise ValueError("FlowMesh worker_alias cannot be blank")

    @property
    def pinning_requested(self) -> bool:
        return self.worker_id is not None or self.worker_alias is not None

    @classmethod
    def from_environment(
        cls,
        *,
        base_url: str | None = None,
        agent_config_name: str | None = None,
        task_timeout_seconds: int | None = None,
        poll_interval_seconds: float | None = None,
        worker_id: str | None = None,
        worker_alias: str | None = None,
        validate_before_submit: bool | None = None,
    ) -> "FlowMeshSettings":
        return cls(
            worker_id=(
                worker_id
                or os.getenv("PATHFINDER_FLOWMESH_WORKER_ID")
                or None
            ),
            worker_alias=(
                worker_alias
                or os.getenv("PATHFINDER_FLOWMESH_WORKER_ALIAS")
                or None
            ),
            validate_before_submit=(
                validate_before_submit
                if validate_before_submit is not None
                else _bool_from_env("PATHFINDER_FLOWMESH_VALIDATE", False)
            ),
            base_url=base_url
            or os.getenv("FLOWMESH_BASE_URL")
            or "http://127.0.0.1:8000",
            api_key=os.getenv("FLOWMESH_API_KEY") or None,
            agent_config_name=agent_config_name
            or os.getenv("PATHFINDER_FLOWMESH_AGENT_CONFIG")
            or "pathfinder_video",
            owner=os.getenv("PATHFINDER_FLOWMESH_OWNER") or "pathfinder",
            task_timeout_seconds=(
                task_timeout_seconds
                if task_timeout_seconds is not None
                else 600
            ),
            poll_interval_seconds=(
                poll_interval_seconds
                if poll_interval_seconds is not None
                else 2.0
            ),
        )


@dataclass(frozen=True)
class FlowMeshAgentRunRequest:
    question: str
    design_id: str
    task_class_id: str
    quote_profile_id: str = "as_designed"
    latency_multiplier: float = 1.0
    seed: int = 1
    trial_id: str = "flowmesh-manual"
    session_id: str | None = None
    object_id: str | None = None


@dataclass(frozen=True)
class SubmittedWorkflow:
    workflow_id: str
    task_ids: tuple[str, ...]


@dataclass(frozen=True)
class FlowMeshWorkerIdentity:
    """Non-secret worker metadata as the configured Root reports it.

    Deliberately excludes the worker environment, tags, and hardware block:
    those can carry deployment secrets, and none of them are needed to decide
    whether a requested pin names exactly one current worker.
    """

    worker_id: str
    alias: str | None = None
    status: str | None = None
    namespace: str | None = None
    cluster: str | None = None
    node_alias: str | None = None

    def to_public_dict(self) -> dict[str, str | None]:
        return {
            "worker_id": self.worker_id,
            "alias": self.alias,
            "status": self.status,
            "namespace": self.namespace,
            "cluster": self.cluster,
            "node_alias": self.node_alias,
        }


@dataclass(frozen=True)
class TerminalWorkflow:
    workflow_id: str
    status: str
    failed_task_ids: tuple[str, ...] = ()
    cancelled_task_ids: tuple[str, ...] = ()
    detail: str | None = None


@dataclass(frozen=True)
class FlowMeshAgentRun:
    session_id: str
    workflow_id: str
    task_id: str
    status: str
    final_answer: str
    access_events: tuple[dict[str, Any], ...]
    raw_result: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class WorkflowValidation:
    ok: bool
    errors: tuple[str, ...] = ()


class FlowMeshClientProtocol(Protocol):
    def submit(self, workflow: Mapping[str, Any]) -> SubmittedWorkflow:
        """Submit one workflow and return its IDs."""

    def validate(self, workflow: Mapping[str, Any]) -> WorkflowValidation:
        """Validate one workflow without submitting or executing it."""

    def resolve_worker_alias(self, alias: str) -> str:
        """Return the current worker ID for a stable alias.

        Must raise unless exactly one worker matches.
        """

    def describe_current_worker(
        self,
        *,
        worker_id: str | None = None,
        alias: str | None = None,
    ) -> FlowMeshWorkerIdentity:
        """Return the one current worker the configured Root reports.

        Exactly one of ``worker_id`` or ``alias`` must be supplied. Must raise
        unless the configured Root reports exactly one current (non-stale)
        match: an exact ID is a request to verify a pin, never a licence to
        skip Root visibility.
        """

    def describe_task_failure(self, task_id: str) -> dict[str, Any] | None:
        """Return read-only terminal failure detail for one task, if any."""

    def wait(
        self,
        workflow_id: str,
        poll_interval_seconds: float,
    ) -> TerminalWorkflow:
        """Wait for a terminal workflow state."""

    def retrieve_result(self, task_id: str) -> dict[str, Any]:
        """Retrieve one completed task result."""
