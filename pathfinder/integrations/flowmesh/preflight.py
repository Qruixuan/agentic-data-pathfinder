from __future__ import annotations

from typing import Any

from .contracts import (
    FlowMeshClientProtocol,
    FlowMeshSettings,
    FlowMeshWorkerIdentity,
)
from .redaction import endpoint_fingerprint, redact_secrets, sanitize_endpoint


PREFLIGHT_SCHEMA_VERSION = "pathfinder.flowmesh-preflight/v1alpha1"

ROOT_VISIBILITY_STATEMENT = (
    "Root visibility was verified: the configured Root Server reports "
    "exactly one current worker for this pin. This API cannot prove that "
    "the local Node Server, this worker, and this Root agree on task "
    "dispatch or result upload, so that alignment is reported as unverified."
)

UNVERIFIED_PROPERTIES = (
    "node_server_and_root_agree_on_this_worker",
    "worker_result_upload_endpoint_matches_this_root",
    "worker_will_receive_a_dispatched_task",
    "worker_agent_config_and_mcp_url_are_correct",
)

OPERATOR_NEXT_STEPS = (
    "Confirm the worker's own Root registration, not only the local Node "
    "Server view.",
    "Confirm the worker logs a received task for a subsequent submission "
    "before treating scheduling as healthy.",
    "Treat a workflow that fails without the worker logging a received task "
    "as a Root/Node control-plane mismatch, not an Agent failure.",
)


class WorkerPreflightError(RuntimeError):
    """Raised when a read-only FlowMesh worker preflight fails closed."""


def _requested_pin(settings: FlowMeshSettings) -> tuple[str, str]:
    if settings.worker_id is not None:
        return "worker_id", settings.worker_id.strip()
    if settings.worker_alias is not None:
        return "worker_alias", settings.worker_alias.strip()
    raise WorkerPreflightError(
        "preflight-flowmesh requires --worker-alias or --worker-id; there is "
        "nothing to verify without a requested pin"
    )


def describe_pinned_worker(
    client: FlowMeshClientProtocol,
    settings: FlowMeshSettings,
) -> FlowMeshWorkerIdentity:
    """Verify a requested pin against the configured Root, read-only.

    Both pin kinds go through the same Root query. An exact ``--worker-id``
    used to be trusted verbatim, which is exactly how a worker that only the
    local Node Server can see becomes a workflow that is accepted, never
    dispatched, and fails minutes later with no worker-side log line.
    """
    kind, value = _requested_pin(settings)
    describe = getattr(client, "describe_current_worker", None)
    if not callable(describe):
        raise WorkerPreflightError(
            "this FlowMesh client cannot verify a worker against the "
            "configured Root, so the requested pin cannot be checked"
        )
    try:
        identity = describe(
            **(
                {"worker_id": value}
                if kind == "worker_id"
                else {"alias": value}
            )
        )
    except Exception as exc:
        raise WorkerPreflightError(redact_secrets(str(exc))) from exc
    worker_id = getattr(identity, "worker_id", None)
    if not isinstance(worker_id, str) or not worker_id.strip():
        raise WorkerPreflightError(
            f"the configured Root returned no usable worker ID for "
            f"{kind} '{value}'"
        )
    return identity


def preflight_flowmesh_worker(
    client: FlowMeshClientProtocol,
    settings: FlowMeshSettings,
) -> dict[str, Any]:
    """Verify one pinned worker without submitting or mutating anything.

    Only read paths are used: no workflow is validated or submitted, no
    Gateway session is registered, and no task, worker, or node lifecycle API
    is touched.
    """
    kind, value = _requested_pin(settings)
    identity = describe_pinned_worker(client, settings)
    return {
        "schema_version": PREFLIGHT_SCHEMA_VERSION,
        "status": "ok",
        "endpoint": {
            "root_endpoint": sanitize_endpoint(settings.base_url),
            "root_endpoint_sha256": endpoint_fingerprint(settings.base_url),
            "api_key_configured": settings.api_key is not None,
            "credentials_recorded": False,
        },
        "requested_pin": {"kind": kind, "value": value},
        "worker": identity.to_public_dict(),
        "checks": {
            "root_visibility_verified": True,
            "stale_workers_excluded": True,
            "exactly_one_current_worker": True,
            "exact_worker_id_bypassed_root": False,
            "gateway_session_registered": False,
            "workflow_validated": False,
            "workflow_submitted": False,
            "mutating_api_called": False,
        },
        "not_verified": list(UNVERIFIED_PROPERTIES),
        "interpretation": ROOT_VISIBILITY_STATEMENT,
        "operator_next_steps": list(OPERATOR_NEXT_STEPS),
    }
