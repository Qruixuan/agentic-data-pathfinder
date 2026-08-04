from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Protocol


@dataclass(frozen=True)
class FlowMeshSettings:
    base_url: str = "http://127.0.0.1:8000"
    api_key: str | None = None
    agent_config_name: str = "pathfinder_video"
    owner: str = "pathfinder"
    task_timeout_seconds: int = 600
    poll_interval_seconds: float = 2.0

    def __post_init__(self) -> None:
        if not self.base_url.strip():
            raise ValueError("FlowMesh base_url cannot be empty")
        if not self.agent_config_name.strip():
            raise ValueError("FlowMesh agent_config_name cannot be empty")
        if self.task_timeout_seconds <= 0:
            raise ValueError("FlowMesh task_timeout_seconds must be positive")
        if self.poll_interval_seconds <= 0:
            raise ValueError("FlowMesh poll_interval_seconds must be positive")

    @classmethod
    def from_environment(
        cls,
        *,
        base_url: str | None = None,
        agent_config_name: str | None = None,
        task_timeout_seconds: int | None = None,
        poll_interval_seconds: float | None = None,
    ) -> "FlowMeshSettings":
        return cls(
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
class TerminalWorkflow:
    workflow_id: str
    status: str


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


class FlowMeshClientProtocol(Protocol):
    def submit(self, workflow: Mapping[str, Any]) -> SubmittedWorkflow:
        """Submit one workflow and return its IDs."""

    def wait(
        self,
        workflow_id: str,
        poll_interval_seconds: float,
    ) -> TerminalWorkflow:
        """Wait for a terminal workflow state."""

    def retrieve_result(self, task_id: str) -> dict[str, Any]:
        """Retrieve one completed task result."""
