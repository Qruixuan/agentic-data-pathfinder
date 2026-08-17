"""Pathfinder-owned coupling layer for FlowMesh."""

from .adapter import (
    FlowMeshAgentAdapter,
    FlowMeshPinningError,
    FlowMeshRunError,
)
from .analysis import (
    ANALYSIS_SCHEMA_VERSION,
    analyze_flowmesh_pilot,
    audit_pilot_records,
)
from .client import SdkFlowMeshClient, WorkerResolutionError
from .contracts import (
    FlowMeshAgentRun,
    FlowMeshAgentRunRequest,
    FlowMeshSettings,
    WorkflowValidation,
)
from .data_agent_backend import RemoteDataAgentBackend
from .gateway import (
    AccessGateway,
    ArtifactFetchUnsupportedError,
    ArtifactHandleError,
    GatewayError,
    SQLiteSessionStore,
    TelemetryIncompleteError,
    artifact_handle_fingerprint,
)
from .pilot import (
    FlowMeshPilotConfig,
    FlowMeshPilotConfigError,
    PilotTrial,
    PilotWorkload,
    build_trial_plan,
    load_flowmesh_pilot_config,
    run_flowmesh_pilot,
    summarize_pilot_records,
    summarize_pilot_records_by_workload,
    summarize_paired_contrasts,
    validate_flowmesh_pilot_config,
)
from .workflow import (
    PATHFINDER_GRAPH_NODE_NAME,
    build_agent_workflow,
    workflow_selected_worker,
)

__all__ = [
    "AccessGateway",
    "ANALYSIS_SCHEMA_VERSION",
    "ArtifactFetchUnsupportedError",
    "ArtifactHandleError",
    "FlowMeshAgentAdapter",
    "FlowMeshAgentRun",
    "FlowMeshAgentRunRequest",
    "FlowMeshPinningError",
    "FlowMeshPilotConfig",
    "FlowMeshPilotConfigError",
    "FlowMeshRunError",
    "FlowMeshSettings",
    "GatewayError",
    "PATHFINDER_GRAPH_NODE_NAME",
    "PilotTrial",
    "PilotWorkload",
    "RemoteDataAgentBackend",
    "SdkFlowMeshClient",
    "SQLiteSessionStore",
    "TelemetryIncompleteError",
    "WorkerResolutionError",
    "WorkflowValidation",
    "analyze_flowmesh_pilot",
    "artifact_handle_fingerprint",
    "audit_pilot_records",
    "build_agent_workflow",
    "build_trial_plan",
    "load_flowmesh_pilot_config",
    "run_flowmesh_pilot",
    "summarize_pilot_records",
    "summarize_pilot_records_by_workload",
    "summarize_paired_contrasts",
    "validate_flowmesh_pilot_config",
    "workflow_selected_worker",
]
