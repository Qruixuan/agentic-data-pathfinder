"""FlowMesh effect adapter for one deployment-bound semantic matrix trial.

FlowMesh schedules exactly one API task for a trial.  That task calls the
selected N7/N8 route coordinator, which performs the dynamic N1--N6 service
handoffs internally.  This is intentional: the API-task graph contract does
not bind a predecessor response into a successor request body.

The adapter returns the strict, endpoint-free result consumed by
``full_flow_matrix_runner``.  Workflow IDs, endpoints, and runtime headers
are reduced to commitments.  The model's public prediction is retained for
privileged N1 score replay, while the hidden reference label and credential
values never leave their trust boundaries.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Callable, Mapping, Sequence
from typing import Any
from urllib.parse import urlsplit

from ...simulator.full_flow_matrix_runner import (
    TRIAL_RESULT_SCHEMA_VERSION,
    SemanticTrialExecutionError,
)
from ...simulator.container_node import (
    FULL_FLOW_INGRESS_SIGNATURE_HEADER,
    full_flow_request_hmac_sha256,
)
from ...simulator.full_flow_semantic_execution_admission import (
    BOUND_STAGE_SCHEMA_VERSION,
    BOUND_TRIAL_SCHEMA_VERSION,
)
from ...simulator.full_flow_semantic_route_evidence import (
    SEMANTIC_ROUTE_EVIDENCE_SCHEMA_VERSION,
    SemanticRouteEvidenceValidationError,
    verify_public_semantic_route_evidence,
)
from ...simulator.full_flow_semantic_input_profiles import (
    SemanticInputProfileError,
    model_input_frontier_representation_ids,
    profile_sha256,
    validate_semantic_input_profile,
)
from ...simulator.full_flow_semantic_route_runtime import (
    GenericSemanticRouteCoordinator,
)
from .adapter import extract_api_executor_result
from .contracts import FlowMeshClientProtocol, FlowMeshSettings
from .preflight import describe_pinned_worker


SEMANTIC_ROUTE_REQUEST_SCHEMA_VERSION = (
    "pathfinder.flowmesh-semantic-route-request/v1alpha1"
)
SEMANTIC_WORKFLOW_EVIDENCE_SCHEMA_VERSION = (
    "pathfinder.flowmesh-semantic-workflow-evidence/v1alpha1"
)
SEMANTIC_ROUTE_ENDPOINT_PATH = "/v1/full-flow/execute"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}\Z")
_TRIAL_KEY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+|/-]{0,1023}\Z")
_REQUEST_FIELDS = {
    "schema_version",
    "run_id",
    "idempotency_key",
    "bound_trial",
    "bound_stages",
    "request_sha256",
    "credentials_recorded",
}
_PRIVATE_REQUEST_KEYS = {
    "api_key",
    "authorization",
    "bearer_token",
    "correct_answer",
    "correct_answer_id",
    "credential_value",
    "hidden_answer",
    "hidden_label",
    "password",
    "secret",
    "token",
}


class FlowMeshSemanticTrialError(ValueError):
    """Raised when a semantic trial cannot safely cross FlowMesh."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FlowMeshSemanticTrialError(message)


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FlowMeshSemanticTrialError(
            "semantic FlowMesh value is not canonical JSON"
        ) from exc


def _copy_json(value: Any) -> Any:
    return json.loads(_canonical(value))


def _strict_json_text(value: str) -> dict[str, Any]:
    def pairs_hook(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, child in pairs:
            _require(key not in result, "API evidence contains a duplicate key")
            result[key] = child
        return result

    try:
        result = json.loads(
            value,
            object_pairs_hook=pairs_hook,
            parse_constant=lambda _item: (_ for _ in ()).throw(
                FlowMeshSemanticTrialError(
                    "API evidence contains a non-finite number"
                )
            ),
        )
    except FlowMeshSemanticTrialError:
        raise
    except json.JSONDecodeError as exc:
        raise FlowMeshSemanticTrialError("API evidence is not valid JSON") from exc
    _require(isinstance(result, dict), "API evidence must be an object")
    return result


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _digest(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{name} is not lowercase SHA-256",
    )
    return str(value)


def _identifier(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
        f"{name} is invalid",
    )
    return str(value)


def _trial_key(value: Any) -> str:
    _require(
        isinstance(value, str) and _TRIAL_KEY.fullmatch(value) is not None,
        "trial_key is invalid",
    )
    return str(value)


def _number(value: Any, name: str) -> int | float:
    _require(
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        and float(value) >= 0,
        f"{name} must be a finite non-negative number",
    )
    return value


def _origin(value: Any) -> str:
    _require(isinstance(value, str) and value == value.strip(), "base_url is invalid")
    parsed = urlsplit(str(value))
    _require(
        parsed.scheme in {"http", "https"}
        and bool(parsed.hostname)
        and parsed.username is None
        and parsed.password is None
        and parsed.path in {"", "/"}
        and not parsed.query
        and not parsed.fragment,
        "base_url must be an HTTP(S) origin without credentials",
    )
    return str(value).rstrip("/")


def _request_digest(request: Mapping[str, Any]) -> str:
    core = dict(request)
    core.pop("request_sha256", None)
    return _sha256(_canonical(core))


def _assert_public_request(value: Any) -> None:
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key).casefold().replace("-", "_")
            _require(
                key not in _PRIVATE_REQUEST_KEYS,
                f"semantic route request contains private field: {raw_key}",
            )
            if key in {
                "credential_values_included",
                "credentials_recorded",
                "hidden_label_included",
            }:
                _require(child is False, f"unsafe request flag is true: {raw_key}")
            _assert_public_request(child)
    elif isinstance(value, list):
        for child in value:
            _assert_public_request(child)


def validate_semantic_route_request(value: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the public request accepted by an N7/N8 coordinator."""

    _require(isinstance(value, Mapping), "semantic route request must be an object")
    request = _copy_json(value)
    _require(set(request) == _REQUEST_FIELDS, "semantic route request fields changed")
    _require(
        request.get("schema_version") == SEMANTIC_ROUTE_REQUEST_SCHEMA_VERSION,
        "semantic route request schema changed",
    )
    _identifier(request.get("run_id"), "run_id")
    _digest(request.get("idempotency_key"), "idempotency_key")
    trial = request.get("bound_trial")
    stages = request.get("bound_stages")
    _require(isinstance(trial, dict), "bound_trial is missing")
    _require(
        trial.get("schema_version") == BOUND_TRIAL_SCHEMA_VERSION,
        "bound trial schema changed",
    )
    trial_key = _trial_key(trial.get("trial_key"))
    _require(
        trial.get("executor_node_id") in {"N7", "N8"},
        "semantic route coordinator must be N7 or N8",
    )
    _require(
        trial.get("flowmesh_execution_shape")
        == "one-api-task-to-route-coordinator",
        "bound trial has a different FlowMesh execution shape",
    )
    _require(
        trial.get("flowmesh_submission_authorized") is True,
        "bound trial is not authorized for FlowMesh submission",
    )
    _require(
        trial.get("required_runtime_adapter_ids") == [],
        "bound trial still declares missing runtime adapters",
    )
    keys = trial.get("semantic_stage_keys")
    hashes = trial.get("bound_stage_sha256")
    _require(
        isinstance(keys, list)
        and isinstance(hashes, list)
        and bool(keys)
        and len(keys) == len(hashes)
        and len(keys) == len(set(keys)),
        "bound trial stage identities are invalid",
    )
    _require(
        isinstance(stages, list) and len(stages) == len(keys),
        "bound stage set is incomplete",
    )
    by_key: dict[str, dict[str, Any]] = {}
    for raw in stages:
        _require(isinstance(raw, dict), "bound stage must be an object")
        stage = dict(raw)
        key = stage.get("stage_key")
        _require(
            isinstance(key, str)
            and bool(key)
            and key not in by_key
            and stage.get("schema_version") == BOUND_STAGE_SCHEMA_VERSION
            and stage.get("trial_key") == trial_key,
            "bound stage identity changed",
        )
        by_key[key] = stage
    _require(set(by_key) == set(keys), "bound stages differ from the trial")
    for key, expected in zip(keys, hashes):
        _require(
            _sha256(_canonical(by_key[key])) == _digest(expected, "bound stage digest"),
            f"bound stage content changed: {key}",
        )
    _require(
        request.get("credentials_recorded") is False,
        "semantic route request cannot record credentials",
    )
    _require(
        request.get("request_sha256") == _request_digest(request),
        "semantic route request digest changed",
    )
    _assert_public_request(request)
    return request


def build_semantic_route_request(
    *,
    run_id: str,
    idempotency_key: str,
    bound_trial: Mapping[str, Any],
    bound_stages: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Build the content-bound request passed through FlowMesh."""

    value: dict[str, Any] = {
        "schema_version": SEMANTIC_ROUTE_REQUEST_SCHEMA_VERSION,
        "run_id": _identifier(run_id, "run_id"),
        "idempotency_key": _digest(idempotency_key, "idempotency_key"),
        "bound_trial": _copy_json(bound_trial),
        "bound_stages": _copy_json(list(bound_stages)),
        "credentials_recorded": False,
    }
    value["request_sha256"] = _request_digest(value)
    return validate_semantic_route_request(value)


def _runtime_headers(
    request: Mapping[str, Any],
    provider: Callable[[Mapping[str, Any]], Mapping[str, str]],
) -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    supplied = provider(_copy_json(request))
    _require(isinstance(supplied, Mapping), "runtime header provider is invalid")
    _require(
        set(supplied) == {FULL_FLOW_INGRESS_SIGNATURE_HEADER}
        and _SHA256.fullmatch(
            str(supplied.get(FULL_FLOW_INGRESS_SIGNATURE_HEADER))
        )
        is not None,
        "runtime header provider must return one full-flow HMAC digest",
    )
    for raw_name, raw_value in supplied.items():
        _require(
            isinstance(raw_name, str)
            and bool(raw_name.strip())
            and isinstance(raw_value, str)
            and bool(raw_value),
            "runtime header provider returned an invalid header",
        )
        name = raw_name.strip()
        _require(
            name.casefold() != "content-type",
            "runtime headers cannot replace Content-Type",
        )
        headers[name] = raw_value
    return headers


def full_flow_hmac_header_provider(
    secret: str,
) -> Callable[[Mapping[str, Any]], Mapping[str, str]]:
    """Return a runtime-only signer for the existing full-flow HMAC gate.

    The secret is captured in memory and used only while rendering the
    submitted workflow.  The credential-free public workflow used for the
    durable evidence digest is built separately.
    """

    def provide(request: Mapping[str, Any]) -> Mapping[str, str]:
        return {
            FULL_FLOW_INGRESS_SIGNATURE_HEADER: full_flow_request_hmac_sha256(
                request,
                secret,
            )
        }

    return provide


def build_flowmesh_semantic_trial_workflow(
    request: Mapping[str, Any],
    *,
    coordinator_base_url: str,
    selected_worker_id: str,
    owner: str,
    api_task_timeout_seconds: int,
    runtime_headers: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Render one worker-pinned API task for one semantic trial."""

    request = validate_semantic_route_request(request)
    worker_id = _identifier(selected_worker_id, "selected_worker_id")
    owner = _identifier(owner, "owner")
    _require(
        type(api_task_timeout_seconds) is int and api_task_timeout_seconds > 0,
        "api_task_timeout_seconds must be a positive integer",
    )
    headers = {"Content-Type": "application/json"}
    if runtime_headers is not None:
        for name, value in runtime_headers.items():
            _require(
                isinstance(name, str)
                and bool(name)
                and isinstance(value, str)
                and bool(value),
                "runtime header is invalid",
            )
            _require(
                name.casefold() != "content-type",
                "runtime headers cannot replace Content-Type",
            )
            headers[name] = value
    trial = request["bound_trial"]
    return {
        "apiVersion": "flowmesh/v1",
        "kind": "APITask",
        "metadata": {
            "name": "pathfinder-semantic-" + request["idempotency_key"][:32],
            "owner": owner,
            "annotations": {
                "schedule_hint": {"selected_worker": worker_id},
                "custom": {
                    "pathfinder_semantic_request_sha256": request[
                        "request_sha256"
                    ],
                    "pathfinder_semantic_trial_key_sha256": _sha256(
                        str(trial["trial_key"]).encode("utf-8")
                    ),
                    "pathfinder_route_coordinator_node_id": trial[
                        "executor_node_id"
                    ],
                    "pathfinder_dynamic_handoff_internal": True,
                },
            },
        },
        "spec": {
            "graph": {
                "nodes": [{
                    "name": "pathfinder-semantic-route",
                    "spec": {
                        "taskType": "api",
                        "api": {
                            "url": _origin(coordinator_base_url)
                            + SEMANTIC_ROUTE_ENDPOINT_PATH,
                            "method": "POST",
                            "headers": headers,
                            "body": request,
                            "timeout_sec": api_task_timeout_seconds,
                            "response": {
                                "parse_json": False,
                                "return_body": True,
                                "raise_for_status": True,
                                "max_body_bytes": 4 * 1024 * 1024,
                            },
                        },
                        "output": {
                            "destination": {"type": "http"},
                            "artifacts": [],
                        },
                    },
                }]
            }
        },
    }


class GenericSemanticRouteRequestHandler:
    """Adapt the HTTP request body to ``GenericSemanticRouteCoordinator``.

    ``ContainerNodeRuntime`` only requires an object with ``execute``.  This
    adapter keeps request validation at that ingress boundary and leaves all
    physical stage effects in the coordinator's injected adapters.
    """

    def __init__(self, coordinator: GenericSemanticRouteCoordinator) -> None:
        _require(
            callable(getattr(coordinator, "execute", None)),
            "generic semantic route coordinator is invalid",
        )
        self._coordinator = coordinator

    def execute(self, value: Mapping[str, Any]) -> dict[str, Any]:
        request = validate_semantic_route_request(value)
        return self._coordinator.execute(
            run_id=request["run_id"],
            bound_trial=request["bound_trial"],
            bound_stages=request["bound_stages"],
        )


def _identity_core(
    object_id: Any,
    representation_id: Any,
    binding: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the canonical identity core shared by trials and stages."""

    return {
        "object_id": object_id,
        "representation_id": representation_id,
        "artifact_sha256": binding.get("artifact_sha256"),
        "artifact_size_bytes": binding.get("artifact_size_bytes"),
        "object_catalog_version": binding.get("object_catalog_version"),
    }


def _bound_stage_identity(stage: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return a stage's fully bound artifact identity, or None.

    A stage that carries no identity, or carries the unbound placeholder the
    frozen DAG uses for control stages, returns None so the caller keeps
    walking. A partially bound identity is never treated as absent: it raises,
    because silently walking past it would widen the derived frontier.
    """

    identity = stage.get("object_representation_identity")
    if identity is None:
        return None
    _require(
        isinstance(identity, Mapping),
        "frozen stage artifact identity is malformed",
    )
    representation_id = identity.get("representation_id")
    binding = identity.get("representation_binding")
    if representation_id is None and not binding:
        return None
    _require(
        isinstance(representation_id, str)
        and bool(representation_id)
        and isinstance(binding, Mapping),
        "frozen stage artifact identity is not fully bound",
    )
    core = _identity_core(
        identity.get("artifact_object_id"),
        representation_id,
        binding,
    )
    _digest(core["artifact_sha256"], "frozen stage artifact SHA-256")
    _number(core["artifact_size_bytes"], "frozen stage artifact size")
    _require(
        core["object_id"] is not None
        and core["object_catalog_version"] is not None,
        "frozen stage artifact identity is not fully bound",
    )
    return core


def _model_input_frontier(
    *,
    bound_stages: Sequence[Mapping[str, Any]],
    frozen_identity_digests: Mapping[str, dict[str, Any]],
) -> set[str]:
    """Derive the N6 model-input identities from the frozen stage DAG.

    The model input is the frontier reached by walking back from the single
    infer stage and stopping at the first fully bound artifact identity on each
    branch. An artifact accessed further upstream -- a retrieval intermediate
    such as the candidate digest -- is deliberately not on that frontier: it is
    routed and must appear in the route's artifact identities, but it is never
    sent to N6. Requiring the model input to name every routed artifact would
    contradict the frozen workload.

    Only checksum-bound stage metadata is consulted, never runtime evidence.
    """

    by_key: dict[str, Mapping[str, Any]] = {}
    for stage in bound_stages:
        _require(isinstance(stage, Mapping), "frozen stage is malformed")
        key = stage.get("stage_key")
        _require(
            isinstance(key, str) and bool(key),
            "frozen stage is missing its stage key",
        )
        previous = by_key.get(key)
        _require(
            previous is None or previous == stage,
            "frozen stages bind the same stage key twice",
        )
        by_key[key] = stage

    infer_stages = [
        stage for stage in by_key.values() if stage.get("action") == "infer"
    ]
    _require(
        len(infer_stages) == 1,
        "frozen stages must contain exactly one infer stage",
    )

    frontier: dict[str, dict[str, Any]] = {}
    seen: set[str] = set()

    def walk(stage_key: Any, path: tuple[str, ...]) -> None:
        _require(
            isinstance(stage_key, str) and bool(stage_key),
            "frozen stage dependency key is malformed",
        )
        _require(
            stage_key not in path,
            "frozen stage dependencies contain a cycle",
        )
        stage = by_key.get(stage_key)
        _require(
            stage is not None,
            "frozen stage dependency is missing from the bound stages",
        )
        core = _bound_stage_identity(stage)
        if core is not None:
            digest = _sha256(_canonical(core))
            _require(
                digest in frozen_identity_digests,
                "model input identity is absent from the frozen trial",
            )
            existing = frontier.get(core["representation_id"])
            _require(
                existing is None or existing == core,
                "frozen stages bind one representation two different ways",
            )
            frontier[core["representation_id"]] = core
            return
        if stage_key in seen:
            return
        seen.add(stage_key)
        dependencies = stage.get("dependency_stage_keys")
        _require(
            isinstance(dependencies, Sequence)
            and not isinstance(dependencies, (str, bytes)),
            "frozen stage dependencies are malformed",
        )
        for dependency in dependencies:
            walk(dependency, (*path, stage_key))

    infer_dependencies = infer_stages[0].get("dependency_stage_keys")
    _require(
        isinstance(infer_dependencies, Sequence)
        and not isinstance(infer_dependencies, (str, bytes))
        and len(infer_dependencies) > 0,
        "the frozen infer stage declares no dependencies",
    )
    for dependency in infer_dependencies:
        walk(dependency, (infer_stages[0]["stage_key"],))

    _require(
        len(frontier) > 0,
        "no model input identity is reachable from the infer stage",
    )
    return {_sha256(_canonical(core)) for core in frontier.values()}


def _verify_route_evidence(
    value: Mapping[str, Any],
    *,
    run_id: str,
    bound_trial: Mapping[str, Any],
    bound_stages: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    try:
        evidence = verify_public_semantic_route_evidence(value)
    except SemanticRouteEvidenceValidationError as exc:
        raise FlowMeshSemanticTrialError(
            f"public route evidence failed strict validation: {exc}"
        ) from exc
    trial_key = bound_trial["trial_key"]
    _require(
        evidence.get("run_id") == run_id
        and evidence.get("trial_id") == trial_key
        and evidence.get("trial_key") == trial_key
        and evidence.get("trial_sha256") == _sha256(_canonical(bound_trial))
        and evidence.get("stage_dag_sha256") == _sha256(_canonical(bound_stages)),
        "route evidence is bound to a different trial",
    )
    _require(
        evidence.get("workload_id") == bound_trial.get("workload_id")
        and evidence.get("workload_class") == bound_trial.get("workload_class")
        and evidence.get("design_id") == bound_trial.get("design_id")
        and evidence.get("repetition") == bound_trial.get("repetition")
        and evidence.get("artifact_object_id")
        == bound_trial.get("artifact_object_id")
        and evidence.get("public_task_binding_sha256")
        == bound_trial.get("public_task_binding_sha256"),
        "route evidence changed the workload or public task binding",
    )
    route = evidence.get("route")
    _require(
        isinstance(route, dict)
        and route.get("route_family") == bound_trial.get("route_family")
        and route.get("executor_node_id") == bound_trial.get("executor_node_id")
        and route.get("inference_node_id") == "N6"
        and route.get("score_node_id") == "N1",
        "route evidence names a different physical route",
    )
    stage_results = evidence.get("stage_results")
    _require(
        isinstance(stage_results, list)
        and len(stage_results) == len(bound_stages),
        "route evidence does not account for every frozen stage",
    )
    expected_keys = [row["stage_key"] for row in bound_stages]
    _require(
        [row.get("stage_key") for row in stage_results] == expected_keys
        and all(
            row.get("state")
            in {"EXECUTED", "INACTIVE", "SKIPPED_INACTIVE_CONDITION"}
            for row in stage_results
        ),
        "route stage evidence changed identity or state",
    )
    for expected, row in zip(bound_stages, stage_results, strict=True):
        _require(
            row.get("stage_index") == expected.get("stage_index")
            and row.get("action") == expected.get("action")
            and row.get("condition") == expected.get("condition"),
            "route stage evidence differs from its frozen stage",
        )
        if row["state"] == "EXECUTED":
            _number(row.get("service_time_ms"), "stage service_time_ms")
            _number(row.get("bytes_read"), "stage bytes_read")
            _number(row.get("bytes_sent"), "stage bytes_sent")
        else:
            _require(
                expected.get("condition") is not None
                and row.get("outcome_kind") is None
                and row.get("outcome_sha256") is None
                and row.get("service_time_ms") == 0.0
                and row.get("bytes_read") == 0
                and row.get("bytes_sent") == 0,
                "inactive route stage evidence is not neutral",
            )
    expected_artifacts: list[dict[str, Any]] = []
    for raw in bound_trial.get("representation_identities", []):
        _require(isinstance(raw, dict), "bound artifact identity is invalid")
        binding = raw.get("representation_binding")
        _require(isinstance(binding, dict), "representation binding is missing")
        core = _identity_core(
            raw.get("artifact_object_id"),
            raw.get("representation_id"),
            binding,
        )
        _digest(core["artifact_sha256"], "artifact SHA-256")
        _number(core["artifact_size_bytes"], "artifact size")
        identity_digest = _sha256(_canonical(core))
        expected_artifacts.append({
            "logical_object_id": raw.get("logical_object_id"),
            **core,
            "identity_sha256": identity_digest,
        })
    actual_artifacts = evidence.get("artifact_identities")
    _require(
        isinstance(actual_artifacts, list)
        and sorted(actual_artifacts, key=lambda row: row["representation_id"])
        == sorted(expected_artifacts, key=lambda row: row["representation_id"]),
        "route evidence artifact identities differ from the frozen trial",
    )
    expected_chains = set(bound_trial.get("required_provisioning_chain_ids", []))
    provisioning = evidence.get("provisioning_references")
    _require(
        isinstance(provisioning, list)
        and {row.get("chain_id") for row in provisioning} == expected_chains
        and all(
            row.get("available") is True
            and _SHA256.fullmatch(str(row.get("n5_evidence_sha256")))
            and _SHA256.fullmatch(str(row.get("n4_publication_sha256")))
            for row in provisioning
        ),
        "N5 provisioning evidence differs from the frozen trial",
    )
    semantic = evidence.get("semantic")
    scoring = evidence.get("scoring")
    _require(isinstance(semantic, dict), "semantic evidence is missing")
    _require(isinstance(scoring, dict), "N1 scoring evidence is missing")
    _digest(semantic.get("final_answer_sha256"), "semantic answer digest")
    _require(
        type(scoring.get("task_success")) is bool
        and scoring.get("authenticated_n1_v1alpha2") is True
        and scoring.get("task_binding_sha256")
        == bound_trial.get("public_task_binding_sha256"),
        "N1 score is not authenticated v1alpha2 evidence",
    )
    _digest(
        scoring.get("authentication_verification_sha256"),
        "N1 authentication verification digest",
    )
    _digest(scoring.get("result_content_sha256"), "N1 result content digest")
    _digest(scoring.get("score_evidence_hmac_sha256"), "N1 score HMAC digest")
    # The model input binds the frozen N6 input frontier, not every artifact
    # the route touched: a retrieval intermediate is routed but never sent to
    # N6. The frontier is derived from the checksum-bound stage DAG alone.
    expected_model_input = _model_input_frontier(
        bound_stages=bound_stages,
        frozen_identity_digests={
            row["identity_sha256"]: row for row in expected_artifacts
        },
    )
    model_input = evidence.get("model_input")
    _require(isinstance(model_input, dict), "N6 model input evidence is missing")
    components = model_input.get("component_identity_sha256")
    _require(
        isinstance(components, list)
        and all(_SHA256.fullmatch(str(row)) for row in components)
        and len(components) == len(set(components)),
        "N6 model input component identities are malformed",
    )
    _require(
        set(components) == expected_model_input,
        "N6 model input does not bind the frozen model-input frontier",
    )
    semantic_profile = bound_trial.get("semantic_input_profile")
    if semantic_profile is not None:
        frontier_representations = model_input_frontier_representation_ids(
            bound_stages
        )
        try:
            expected_profile = validate_semantic_input_profile(
                semantic_profile,
                route_family=str(bound_trial.get("route_family")),
                model_input_representation_ids=frontier_representations,
            )
        except SemanticInputProfileError as exc:
            raise FlowMeshSemanticTrialError(str(exc)) from exc
        selection = expected_profile.get("frame_selection")
        expected_frame_count = (
            0 if selection is None else selection.get("frame_count")
        )
        expected_window = (
            None
            if selection is None
            else selection.get("temporal_window_fraction")
        )
        _require(
            model_input.get("semantic_input_profile_id")
            == expected_profile["profile_id"]
            and model_input.get("semantic_input_profile_sha256")
            == profile_sha256(expected_profile)
            and model_input.get("semantic_input_profile_verified") is True
            and evidence.get("semantic_input_profile_verified") is True
            and model_input.get("mode") == expected_profile["input_mode"]
            and model_input.get("frame_count") == expected_frame_count
            and model_input.get("temporal_window_fraction")
            == expected_window
            and (
                model_input.get("digest_input_sha256") is not None
            ) is expected_profile["digest_included"]
            and (
                model_input.get("frame_sequence_sha256") is not None
            ) is (expected_frame_count > 0)
            and model_input.get("direct_video_input") is False,
            "N6 semantic input differs from its frozen profile",
        )
        _digest(
            model_input.get("semantic_content_sha256"),
            "semantic content digest",
        )
    observation = evidence.get("neutral_observation_candidate")
    _require(
        isinstance(observation, dict)
        and observation.get("task_success") is scoring["task_success"]
        and observation.get("monetary_values_included") is False,
        "neutral observation disagrees with authenticated scoring",
    )
    _require(
        evidence.get("all_frozen_stages_accounted_for") is True
        and evidence.get("exclusive_cache_branches_verified") is True
        and evidence.get("n3_n4_artifact_identity_verified") is True
        and evidence.get("n5_provisioning_references_verified") is True
        and evidence.get("n6_input_mode_verified") is True
        and evidence.get("n1_exactly_once_authenticated_score_verified") is True
        and evidence.get("credentials_recorded") is False
        and evidence.get("eligible_for_scientific_claims") is False,
        "route evidence safety or completeness claims changed",
    )
    return evidence


def verify_semantic_route_evidence(
    value: Mapping[str, Any],
    *,
    run_id: str,
    bound_trial: Mapping[str, Any],
    bound_stages: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Verify endpoint-free route evidence against its frozen public inputs.

    This is the side-effect-free form of the check used by
    :class:`FlowMeshSemanticTrialExecutor`.  Consumers such as the AWM/OED
    bridge can therefore validate archived route evidence without submitting
    a workflow, contacting a service, or reading an N1 hidden-label package.
    """

    return _verify_route_evidence(
        value,
        run_id=run_id,
        bound_trial=bound_trial,
        bound_stages=bound_stages,
    )


def _measurements(evidence: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, stage in enumerate(evidence["stage_results"]):
        if stage["state"] != "EXECUTED":
            continue
        component = f"stage-{index:04d}-{stage['action']}"
        for metric, field, unit in (
            ("bytes-read", "bytes_read", "bytes"),
            ("bytes-sent", "bytes_sent", "bytes"),
            ("service-time", "service_time_ms", "milliseconds"),
        ):
            rows.append({
                "component_id": component,
                "metric_id": metric,
                "value": stage[field],
                "unit": unit,
                "measurement_class": "derived",
            })
    return sorted(rows, key=lambda row: (row["component_id"], row["metric_id"]))


class FlowMeshSemanticTrialExecutor:
    """Execute runner trials through one pinned FlowMesh coordinator task."""

    def __init__(
        self,
        *,
        client: FlowMeshClientProtocol,
        settings: FlowMeshSettings,
        run_id: str,
        bound_trials: Sequence[Mapping[str, Any]],
        bound_stages: Sequence[Mapping[str, Any]],
        runtime_header_provider: Callable[
            [Mapping[str, Any]], Mapping[str, str]
        ],
        api_task_timeout_seconds: int | None = None,
    ) -> None:
        self._client = client
        self._settings = settings
        self._run_id = _identifier(run_id, "run_id")
        _require(
            settings.worker_alias is not None and settings.worker_id is None,
            "semantic FlowMesh execution requires one stable worker alias",
        )
        _require(
            callable(runtime_header_provider),
            "semantic FlowMesh execution requires a runtime HMAC provider",
        )
        self._runtime_header_provider = runtime_header_provider
        self._timeout = (
            settings.task_timeout_seconds
            if api_task_timeout_seconds is None
            else api_task_timeout_seconds
        )
        _require(
            type(self._timeout) is int and self._timeout > 0,
            "api task timeout must be a positive integer",
        )
        self._trials: dict[str, dict[str, Any]] = {}
        for raw in bound_trials:
            _require(isinstance(raw, Mapping), "bound trial catalog is invalid")
            trial = _copy_json(raw)
            key = _trial_key(trial.get("trial_key"))
            _require(key not in self._trials, "bound trial catalog repeats a key")
            self._trials[key] = trial
        self._stages: dict[str, dict[str, Any]] = {}
        for raw in bound_stages:
            _require(isinstance(raw, Mapping), "bound stage catalog is invalid")
            stage = _copy_json(raw)
            key = stage.get("stage_key")
            _require(
                isinstance(key, str) and key and key not in self._stages,
                "bound stage catalog repeats or omits a key",
            )
            self._stages[key] = stage

    def execute(
        self,
        *,
        trial: Mapping[str, Any],
        idempotency_key: str,
    ) -> Mapping[str, Any]:
        """Implement ``SemanticTrialExecutor`` without leaking runtime data."""

        try:
            return self._execute_once(trial, idempotency_key)
        except SemanticTrialExecutionError:
            raise
        except FlowMeshSemanticTrialError as exc:
            raise SemanticTrialExecutionError(
                "semantic", "invalid-semantic-flowmesh-binding"
            ) from exc
        except Exception as exc:
            raise SemanticTrialExecutionError(
                "infrastructure", "flowmesh-semantic-execution-failed"
            ) from exc

    def _execute_once(
        self,
        source_trial: Mapping[str, Any],
        idempotency_key: str,
    ) -> dict[str, Any]:
        _require(isinstance(source_trial, Mapping), "source trial is invalid")
        trial_key = _trial_key(source_trial.get("trial_key"))
        _digest(idempotency_key, "idempotency_key")
        _require(trial_key in self._trials, "trial is absent from bound catalog")
        trial = self._trials[trial_key]
        source_is_original_semantic_trial = (
            trial.get("source_semantic_trial_sha256")
            == _sha256(_canonical(source_trial))
        )
        source_is_verified_bound_trial = (
            _canonical(source_trial) == _canonical(trial)
        )
        _require(
            source_is_original_semantic_trial
            or source_is_verified_bound_trial,
            "bound trial does not match either the runner's frozen semantic "
            "trial or the verified executable catalog",
        )
        _require(
            trial.get("worker_alias") == self._settings.worker_alias,
            "bound trial and FlowMesh settings use different worker aliases",
        )
        coordinator = trial.get("route_coordinator_binding")
        _require(isinstance(coordinator, dict), "route coordinator binding is missing")
        expected_contract = f"{trial.get('executor_node_id')}.execution-compute"
        _require(
            coordinator.get("service_contract_id") == expected_contract,
            "route coordinator binding names the wrong executor",
        )
        stage_keys = trial.get("semantic_stage_keys")
        _require(
            isinstance(stage_keys, list)
            and all(key in self._stages for key in stage_keys),
            "bound trial references a missing stage",
        )
        stages = [self._stages[key] for key in stage_keys]
        request = build_semantic_route_request(
            run_id=self._run_id,
            idempotency_key=idempotency_key,
            bound_trial=trial,
            bound_stages=stages,
        )

        try:
            identity = describe_pinned_worker(self._client, self._settings)
            worker_id = _identifier(identity.worker_id, "Root worker_id")
            if identity.alias not in {None, self._settings.worker_alias}:
                raise FlowMeshSemanticTrialError(
                    "Root returned a different worker alias"
                )
        except Exception as exc:
            raise SemanticTrialExecutionError(
                "infrastructure", "flowmesh-worker-preflight-failed"
            ) from exc
        public_workflow = build_flowmesh_semantic_trial_workflow(
            request,
            coordinator_base_url=coordinator.get("base_url"),
            selected_worker_id=worker_id,
            owner=self._settings.owner,
            api_task_timeout_seconds=self._timeout,
        )
        runtime_headers = _runtime_headers(
            request,
            self._runtime_header_provider,
        )
        runtime_headers.pop("Content-Type", None)
        workflow = build_flowmesh_semantic_trial_workflow(
            request,
            coordinator_base_url=coordinator.get("base_url"),
            selected_worker_id=worker_id,
            owner=self._settings.owner,
            api_task_timeout_seconds=self._timeout,
            runtime_headers=runtime_headers,
        )
        try:
            validation = self._client.validate(workflow)
        except Exception as exc:
            raise SemanticTrialExecutionError(
                "infrastructure", "flowmesh-workflow-validation-unavailable"
            ) from exc
        if not validation.ok:
            raise SemanticTrialExecutionError(
                "semantic", "flowmesh-workflow-validation-failed"
            )
        try:
            submitted = self._client.submit(workflow)
        except Exception as exc:
            raise SemanticTrialExecutionError(
                "infrastructure", "flowmesh-workflow-submit-failed"
            ) from exc
        if len(submitted.task_ids) != 1:
            raise SemanticTrialExecutionError(
                "infrastructure", "flowmesh-returned-nonunit-task-count"
            )
        try:
            workflow_id = _identifier(submitted.workflow_id, "workflow_id")
            task_id = _identifier(submitted.task_ids[0], "task_id")
        except FlowMeshSemanticTrialError as exc:
            raise SemanticTrialExecutionError(
                "infrastructure", "flowmesh-returned-invalid-task-identity"
            ) from exc
        try:
            terminal = self._client.wait(
                workflow_id,
                self._settings.poll_interval_seconds,
            )
        except Exception as exc:
            raise SemanticTrialExecutionError(
                "infrastructure", "flowmesh-workflow-wait-failed"
            ) from exc
        if terminal.status != "DONE" or terminal.workflow_id != workflow_id:
            raise SemanticTrialExecutionError(
                "infrastructure", "flowmesh-workflow-terminal-failure"
            )
        try:
            raw = self._client.retrieve_result(task_id)
            api = extract_api_executor_result(raw)
        except Exception as exc:
            raise SemanticTrialExecutionError(
                "infrastructure", "flowmesh-result-retrieval-failed"
            ) from exc
        try:
            route_evidence = _strict_json_text(api["text"])
            evidence = _verify_route_evidence(
                route_evidence,
                run_id=self._run_id,
                bound_trial=trial,
                bound_stages=stages,
            )
        except Exception as exc:
            raise SemanticTrialExecutionError(
                "semantic", "invalid-semantic-route-evidence"
            ) from exc
        try:
            task_detail = self._client.describe_task_failure(task_id)
        except Exception as exc:
            raise SemanticTrialExecutionError(
                "infrastructure", "flowmesh-worker-assignment-unavailable"
            ) from exc
        if not isinstance(task_detail, Mapping) or task_detail.get(
            "assigned_worker"
        ) != worker_id:
            raise SemanticTrialExecutionError(
                "infrastructure", "flowmesh-worker-assignment-mismatch"
            )

        workflow_evidence = {
            "schema_version": SEMANTIC_WORKFLOW_EVIDENCE_SCHEMA_VERSION,
            "public_workflow_sha256": _sha256(_canonical(public_workflow)),
            "workflow_id": workflow_id,
            "task_id": task_id,
            "worker_id": worker_id,
            "api_executor": api["executor"],
            "api_http_status": api["status_code"],
            "semantic_route_evidence_sha256": evidence["evidence_sha256"],
            "validated_before_submission": True,
            "credentials_recorded": False,
        }
        artifact_evidence = {
            "artifact_identities": evidence.get("artifact_identities"),
            "provisioning_references": evidence.get("provisioning_references"),
            "trial_representation_identities": trial.get(
                "representation_identities"
            ),
        }
        scoring = evidence["scoring"]
        return {
            "schema_version": TRIAL_RESULT_SCHEMA_VERSION,
            "status": "COMPLETE",
            "trial_key": trial_key,
            "idempotency_key": idempotency_key,
            "task_success": scoring["task_success"],
            "semantic_answer_sha256": evidence["semantic"][
                "final_answer_sha256"
            ],
            "n1_score_evidence_sha256": scoring[
                "authentication_verification_sha256"
            ],
            "n1_score_authenticity_verified": True,
            "route_evidence_sha256": evidence["evidence_sha256"],
            "semantic_route_evidence": evidence,
            "artifact_binding_evidence_sha256": _sha256(
                _canonical(artifact_evidence)
            ),
            "measurements": _measurements(evidence),
            "execution_transport": "flowmesh",
            "flowmesh_workflow_evidence_sha256": _sha256(
                _canonical(workflow_evidence)
            ),
            # Completing the generic route's sole ``infer`` stage means its
            # N6 SemanticInferenceAdapter was invoked.  This says nothing
            # about model quality or scientific eligibility.
            "llm_called": True,
            "telemetry_complete": True,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }


__all__ = [
    "FlowMeshSemanticTrialError",
    "FlowMeshSemanticTrialExecutor",
    "GenericSemanticRouteRequestHandler",
    "SEMANTIC_ROUTE_ENDPOINT_PATH",
    "SEMANTIC_ROUTE_REQUEST_SCHEMA_VERSION",
    "SEMANTIC_WORKFLOW_EVIDENCE_SCHEMA_VERSION",
    "build_flowmesh_semantic_trial_workflow",
    "build_semantic_route_request",
    "full_flow_hmac_header_provider",
    "validate_semantic_route_request",
    "verify_semantic_route_evidence",
]
