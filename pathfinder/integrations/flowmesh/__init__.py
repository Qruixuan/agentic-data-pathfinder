"""Pathfinder-owned coupling layer for FlowMesh."""

from .adapter import (
    FlowMeshAgentAdapter,
    FlowMeshPinningError,
    FlowMeshRunError,
    FlowMeshWorkflowFailureError,
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
    FlowMeshWorkerIdentity,
    TerminalWorkflow,
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
from .preflight import (
    PREFLIGHT_SCHEMA_VERSION,
    WorkerPreflightError,
    describe_pinned_worker,
    preflight_flowmesh_worker,
)
from .redaction import (
    endpoint_fingerprint,
    redact_secrets,
    sanitize_endpoint,
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
    "FlowMeshWorkerIdentity",
    "FlowMeshWorkflowFailureError",
    "GatewayError",
    "PATHFINDER_GRAPH_NODE_NAME",
    "PREFLIGHT_SCHEMA_VERSION",
    "PilotTrial",
    "PilotWorkload",
    "RemoteDataAgentBackend",
    "SdkFlowMeshClient",
    "SQLiteSessionStore",
    "TelemetryIncompleteError",
    "TerminalWorkflow",
    "WorkerPreflightError",
    "WorkerResolutionError",
    "WorkflowValidation",
    "analyze_flowmesh_pilot",
    "artifact_handle_fingerprint",
    "audit_pilot_records",
    "build_agent_workflow",
    "build_trial_plan",
    "describe_pinned_worker",
    "endpoint_fingerprint",
    "load_flowmesh_pilot_config",
    "preflight_flowmesh_worker",
    "redact_secrets",
    "run_flowmesh_pilot",
    "sanitize_endpoint",
    "summarize_pilot_records",
    "summarize_pilot_records_by_workload",
    "summarize_paired_contrasts",
    "validate_flowmesh_pilot_config",
    "workflow_selected_worker",
]
