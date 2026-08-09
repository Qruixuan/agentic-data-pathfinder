from __future__ import annotations

from typing import Any

from .contracts import (
    FlowMeshAgentRunRequest,
    FlowMeshSettings,
)

PATHFINDER_GRAPH_NODE_NAME = "pathfinder-agent"
"""Name of the single graph node carrying the Pathfinder Agent task.

FlowMesh v0.1.8-rc.1 only reads ``metadata.annotations.schedule_hint`` while
expanding ``spec.graph`` or ``spec.stages``. A bare single-task spec parses
successfully but silently discards the hint, so every Pathfinder workflow is
emitted as a one-node graph whether or not it is pinned. Keeping one shape for
pinned and unpinned runs means both travel an identical parsing and scheduling
path, so enabling pinning cannot alter anything else about the workload.
"""


def build_agent_task_prompt(
    session_id: str,
    request: FlowMeshAgentRunRequest,
) -> str:
    return f"""You are participating in a controlled Pathfinder experiment.

Session ID: {session_id}
Task class: {request.task_class_id}

Rules:
1. Call list_offers with the session ID before accessing data.
2. Use access_representation to obtain any representation you need.
3. Never invent a representation or bypass the quoted access budget.
4. You may make additional accesses only while the gateway reports budget
   and access slots remaining.
5. Return a concise final answer to the task after using the available data.

Task:
{request.question}
"""


def build_agent_workflow(
    session_id: str,
    request: FlowMeshAgentRunRequest,
    settings: FlowMeshSettings,
    *,
    selected_worker_id: str | None = None,
) -> dict[str, Any]:
    """Build one FlowMesh workflow for a Pathfinder Agent session.

    ``selected_worker_id`` must already be a concrete FlowMesh worker ID.
    Alias resolution happens in the adapter so this function stays pure and
    can be checked directly against the deployed parser.
    """
    if selected_worker_id is not None and not selected_worker_id.strip():
        raise ValueError("selected_worker_id cannot be blank")

    # FlowMesh v0.1.8-rc.1 validates metadata.annotations with extra="forbid"
    # and permits only schedule_hint, description, and custom. Pathfinder
    # provenance therefore lives under custom; anything else is rejected at
    # submission with extra_forbidden errors.
    custom: dict[str, Any] = {
        "pathfinder_session_id": session_id,
        "pathfinder_trial_id": request.trial_id,
        "pathfinder_design_id": request.design_id,
        "pathfinder_task_class_id": request.task_class_id,
    }
    if request.object_id is not None:
        custom["pathfinder_object_id"] = request.object_id

    annotations: dict[str, Any] = {"custom": custom}
    if selected_worker_id is not None:
        annotations["schedule_hint"] = {
            "selected_worker": selected_worker_id.strip()
        }

    agent_spec: dict[str, Any] = {
        "taskType": "agent",
        "configName": settings.agent_config_name,
        "task": build_agent_task_prompt(session_id, request),
        # On FlowMesh v0.1.8-rc.1 these are scheduling requests used for
        # placement, not Docker worker-container limits: the supervisor does
        # not turn them into --cpus/--memory or cgroup constraints. They do
        # not confine the worker to one core or 2 GiB.
        "resources": {
            "hardware": {
                "cpu": 1,
                "memory": "2Gi",
                "gpu": {"type": "any", "count": 0},
            }
        },
        "agent": {"timeout": settings.task_timeout_seconds},
        "output": {
            "destination": {"type": "local"},
            "artifacts": ["agent_output.txt"],
        },
    }

    return {
        "apiVersion": "flowmesh/v1",
        "kind": "AgentTask",
        "metadata": {
            "name": f"pathfinder-{session_id[:12]}",
            "owner": settings.owner,
            "annotations": annotations,
        },
        "spec": {
            "graph": {
                "nodes": [
                    {
                        "name": PATHFINDER_GRAPH_NODE_NAME,
                        "spec": agent_spec,
                    }
                ]
            }
        },
    }


def workflow_selected_worker(workflow: Any) -> str | None:
    """Return the worker ID a built workflow pins to, if any.

    Used to confirm a pin survived construction before submission, so a
    structural regression cannot silently produce an unpinned experiment run.
    """
    try:
        hint = workflow["metadata"]["annotations"]["schedule_hint"]
        selected = hint["selected_worker"]
    except (KeyError, TypeError):
        return None
    return selected if isinstance(selected, str) else None
