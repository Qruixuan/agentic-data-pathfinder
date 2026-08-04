from __future__ import annotations

from typing import Any

from .contracts import (
    FlowMeshAgentRunRequest,
    FlowMeshSettings,
)


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
) -> dict[str, Any]:
    annotations = {
        "pathfinder_session_id": session_id,
        "pathfinder_trial_id": request.trial_id,
        "pathfinder_design_id": request.design_id,
        "pathfinder_task_class_id": request.task_class_id,
    }
    if request.object_id is not None:
        annotations["pathfinder_object_id"] = request.object_id
    return {
        "apiVersion": "flowmesh/v1",
        "kind": "AgentTask",
        "metadata": {
            "name": f"pathfinder-{session_id[:12]}",
            "owner": settings.owner,
            "annotations": annotations,
        },
        "spec": {
            "taskType": "agent",
            "configName": settings.agent_config_name,
            "task": build_agent_task_prompt(session_id, request),
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
        },
    }
