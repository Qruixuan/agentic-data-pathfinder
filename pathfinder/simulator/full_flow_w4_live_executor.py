"""Strict component-backed execution boundary for public W4 candidate routes.

The candidate coordinator deliberately knows nothing about deployment.  This
module supplies the smallest adapter that can execute those routes against
real local components (in-process services, HTTP clients, or container
clients) without inventing any endpoint.  Every effect is injected and every
response is checked before it is projected into the coordinator's exact
operation-result schema.

The module also freezes an endpoint-free index-to-artifact crosswalk.  The
crosswalk binds the visible N2 document for an object to the three physical
representations used by the route plan.  It contains no relevance labels.

No component implementation in this file opens a socket, calls an LLM, or
submits a FlowMesh workflow.  Those effects belong to injected adapters.  A
receipt explicitly distinguishes strict-fake conformance tests from a live
local-component execution; neither is real-cloud performance evidence.
"""

from __future__ import annotations

import json
import math
import os
import shutil
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence, runtime_checkable

from ._full_flow_primitives import (
    canonical_json_bytes,
    canonical_json_lines_bytes,
    checked_identifier,
    checked_lower_sha256,
    pretty_json_bytes,
    sha256_hex,
    strict_json_loads,
)
from .full_flow_w4_candidate_coordinator import (
    OPERATION_EVIDENCE_NAME,
    RUN_NAME as COORDINATOR_RUN_NAME,
    W4_CANDIDATE_OPERATION_RESULT_SCHEMA_VERSION,
    W4CandidateOperationExecutor,
    verify_full_flow_w4_candidate_coordinator_run,
)
from .full_flow_w4_candidate_routes import (
    FrozenW4CandidateRouteInputs,
    load_full_flow_w4_candidate_route_inputs,
)
from .index_service import (
    build_n2_index_query_request,
    verify_n2_index_package,
    verify_n2_public_index_query_result,
)


W4_INDEX_ARTIFACT_CROSSWALK_SCHEMA_VERSION = (
    "pathfinder.full-flow-w4-index-artifact-crosswalk/v1alpha1"
)
W4_LIVE_COMPONENT_RECEIPT_SCHEMA_VERSION = (
    "pathfinder.full-flow-w4-component-execution-receipt/v1alpha1"
)
W4_LIVE_COMPONENT_EVENT_SCHEMA_VERSION = (
    "pathfinder.full-flow-w4-component-execution-event/v1alpha1"
)
W4_ARTIFACT_ACCESS_RESULT_SCHEMA_VERSION = (
    "pathfinder.full-flow-w4-artifact-access-result/v1alpha1"
)
W4_BYTE_TRANSFER_RESULT_SCHEMA_VERSION = (
    "pathfinder.full-flow-w4-byte-transfer-result/v1alpha1"
)
W4_CONTROL_RESULT_SCHEMA_VERSION = (
    "pathfinder.full-flow-w4-public-control-result/v1alpha1"
)
W4_SEMANTIC_RANKING_RESULT_SCHEMA_VERSION = (
    "pathfinder.full-flow-w4-semantic-ranking-result/v1alpha1"
)

CROSSWALK_NAME = "w4-index-artifact-crosswalk.json"
RECEIPT_NAME = "w4-component-execution-receipt.json"
EVENTS_NAME = "w4-component-execution-events.jsonl"
CHECKSUMS_NAME = "SHA256SUMS"

_EVIDENCE_CLASSES = frozenset({
    "strict-fake-component-conformance",
    "live-local-component-execution",
})


class FullFlowW4LiveExecutorError(ValueError):
    """Raised when a component result cannot be source-bound safely."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowW4LiveExecutorError(message)


def _canonical(value: Any) -> bytes:
    return canonical_json_bytes(
        value,
        error_type=FullFlowW4LiveExecutorError,
        error_message="value is not canonical JSON",
    )


def _json_bytes(value: Any) -> bytes:
    return pretty_json_bytes(value)


def _jsonl_bytes(values: Sequence[Mapping[str, Any]]) -> bytes:
    return canonical_json_lines_bytes(
        values,
        error_type=FullFlowW4LiveExecutorError,
        error_message="value is not canonical JSON",
    )


def _sha256(value: bytes) -> str:
    return sha256_hex(value)


def _digest(value: Any, name: str) -> str:
    return str(
        checked_lower_sha256(
            value,
            name,
            error_type=FullFlowW4LiveExecutorError,
        )
    )


def _identifier(value: Any, name: str) -> str:
    return str(
        checked_identifier(
            value,
            name,
            error_type=FullFlowW4LiveExecutorError,
        )
    )


def _strict_fields(
    value: Mapping[str, Any], expected: set[str], name: str
) -> None:
    _require(set(value) == expected, f"{name} fields changed")


def _nonnegative_number(value: Any, name: str) -> float:
    _require(
        type(value) in {int, float}
        and math.isfinite(float(value))
        and float(value) >= 0.0,
        f"{name} is invalid",
    )
    return float(value)


def _read_json(path: Path, name: str) -> dict[str, Any]:
    _require(path.is_file() and not path.is_symlink(), f"{name} is missing")

    try:
        value = strict_json_loads(
            path.read_text(encoding="utf-8"),
            error_type=FullFlowW4LiveExecutorError,
            duplicate_key_message=lambda key: f"{name} repeats key {key}",
            nonfinite_number_message=(
                lambda token: f"{name} contains non-finite number {token}"
            ),
        )
    except FullFlowW4LiveExecutorError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowW4LiveExecutorError(f"cannot read {name}") from exc
    _require(isinstance(value, dict), f"{name} must be an object")
    return value


def _read_jsonl(path: Path, name: str) -> list[dict[str, Any]]:
    _require(path.is_file() and not path.is_symlink(), f"{name} is missing")

    try:
        values = [
            strict_json_loads(
                line,
                error_type=FullFlowW4LiveExecutorError,
                duplicate_key_message=lambda key: f"{name} repeats key {key}",
                nonfinite_number_message=(
                    lambda token: f"{name} contains non-finite number {token}"
                ),
            )
            for line in path.read_text(encoding="utf-8").splitlines()
            if line
        ]
    except FullFlowW4LiveExecutorError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FullFlowW4LiveExecutorError(f"cannot read {name}") from exc
    _require(all(isinstance(row, dict) for row in values), f"{name} is invalid")
    return values


def _checksums(documents: Mapping[str, bytes]) -> bytes:
    return b"".join(
        f"{_sha256(documents[name])}  {name}\n".encode("utf-8")
        for name in sorted(documents)
    )


def _publish(target: Path, documents: Mapping[str, bytes], prefix: str) -> None:
    _require(not target.exists(), f"output directory already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=prefix, dir=target.parent))
    stage = temporary / "output"
    try:
        stage.mkdir()
        for name, content in documents.items():
            (stage / name).write_bytes(content)
        (stage / CHECKSUMS_NAME).write_bytes(_checksums(documents))
        os.replace(stage, target)
    finally:
        shutil.rmtree(temporary, ignore_errors=True)


def _identity_digest(value: Mapping[str, Any]) -> str:
    return _sha256(_canonical(value))


def _index_documents(index_package_dir: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    verified = verify_n2_index_package(index_package_dir)
    artifact = _read_json(index_package_dir / "lexical-index.json", "N2 index")
    return verified, artifact


def _crosswalk_document(
    source: FrozenW4CandidateRouteInputs,
    *,
    index_verified: Mapping[str, Any],
    index_artifact: Mapping[str, Any],
) -> dict[str, Any]:
    binding = source.artifact_catalog["index_binding"]
    _require(
        binding["index_id"] == index_verified["index_id"]
        and binding["index_sha256"] == index_verified["index_sha256"]
        and binding["source_manifest_sha256"]
        == index_verified["source_manifest_sha256"],
        "route and index package bindings differ",
    )
    documents = index_artifact.get("documents")
    _require(isinstance(documents, list), "N2 index documents are invalid")
    by_id = {
        str(row.get("object_id")): row
        for row in documents
        if isinstance(row, Mapping)
    }
    objects = source.artifact_catalog.get("objects")
    _require(isinstance(objects, list), "route artifact catalog is invalid")
    route_ids = [str(row["object_id"]) for row in objects]
    _require(
        set(by_id) == set(route_ids) and len(by_id) == len(documents),
        "index-to-artifact object coverage differs",
    )
    rows: list[dict[str, Any]] = []
    for route_row in objects:
        object_id = str(route_row["object_id"])
        index_row = by_id[object_id]
        representations = route_row.get("representations")
        _require(
            isinstance(representations, Mapping)
            and set(representations)
            == {"raw_video", "multimodal_digest", "sampled_frame_bundle"},
            "route representation coverage changed",
        )
        rows.append({
            "object_id": object_id,
            "visible_fields_sha256": _digest(
                index_row.get("visible_fields_sha256"),
                "visible_fields_sha256",
            ),
            "representation_identity_sha256": {
                name: _identity_digest(dict(representations[name]))
                for name in sorted(representations)
            },
        })
    rows.sort(key=lambda row: row["object_id"])
    document: dict[str, Any] = {
        "schema_version": W4_INDEX_ARTIFACT_CROSSWALK_SCHEMA_VERSION,
        "status": "FROZEN_PUBLIC_INDEX_ARTIFACT_CROSSWALK",
        "physical_plan_id": source.plan["physical_plan_id"],
        "route_plan_sha256": source.plan["plan_sha256"],
        "retrieval_task_binding_sha256": source.public_task[
            "task_binding_sha256"
        ],
        "candidate_set_sha256": source.public_task["candidate_set_sha256"],
        "index_id": index_verified["index_id"],
        "index_sha256": index_verified["index_sha256"],
        "index_source_manifest_sha256": index_verified[
            "source_manifest_sha256"
        ],
        "objects": rows,
        "hidden_relevance_values_included": False,
        "artifact_bytes_copied": False,
        "endpoints_included": False,
        "credentials_recorded": False,
        "eligible_for_scientific_claims": False,
    }
    document["crosswalk_sha256"] = _sha256(_canonical(document))
    return document


def freeze_full_flow_w4_index_artifact_crosswalk(
    route_package_dir: str | Path,
    index_package_dir: str | Path,
    *,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze a label-safe, source-verified N2-to-representation crosswalk."""

    route_root = Path(route_package_dir).resolve()
    index_root = Path(index_package_dir).resolve()
    target = Path(output_dir).resolve()
    source = load_full_flow_w4_candidate_route_inputs(route_root)
    verified, artifact = _index_documents(index_root)
    document = _crosswalk_document(
        source,
        index_verified=verified,
        index_artifact=artifact,
    )
    payload = _json_bytes(document)
    _publish(
        target,
        {CROSSWALK_NAME: payload},
        ".w4-index-artifact-crosswalk-",
    )
    result = verify_full_flow_w4_index_artifact_crosswalk(
        target,
        route_package_dir=route_root,
        index_package_dir=index_root,
    )
    return {**result, "status": "FROZEN", "output_dir": str(target)}


def verify_full_flow_w4_index_artifact_crosswalk(
    output_dir: str | Path,
    *,
    route_package_dir: str | Path,
    index_package_dir: str | Path,
) -> dict[str, Any]:
    root = Path(output_dir).resolve()
    _require(root.is_dir() and not root.is_symlink(), "crosswalk directory missing")
    _require(
        {path.name for path in root.iterdir()}
        == {CROSSWALK_NAME, CHECKSUMS_NAME}
        and all(path.is_file() and not path.is_symlink() for path in root.iterdir()),
        "crosswalk file set changed",
    )
    document = _read_json(root / CROSSWALK_NAME, "crosswalk")
    payload = _json_bytes(document)
    _require(
        (root / CROSSWALK_NAME).read_bytes() == payload
        and (root / CHECKSUMS_NAME).read_bytes()
        == _checksums({CROSSWALK_NAME: payload}),
        "crosswalk bytes or checksums changed",
    )
    source = load_full_flow_w4_candidate_route_inputs(route_package_dir)
    verified, artifact = _index_documents(Path(index_package_dir).resolve())
    expected = _crosswalk_document(
        source,
        index_verified=verified,
        index_artifact=artifact,
    )
    _require(document == expected, "crosswalk differs from frozen sources")
    return {
        "status": "VERIFIED",
        "physical_plan_id": document["physical_plan_id"],
        "object_count": len(document["objects"]),
        "index_sha256": document["index_sha256"],
        "crosswalk_sha256": document["crosswalk_sha256"],
        "hidden_relevance_values_included": False,
        "eligible_for_scientific_claims": False,
    }


@runtime_checkable
class W4AdmissionAdapter(Protocol):
    def admit(self, request: Mapping[str, Any]) -> Mapping[str, Any]: ...


@runtime_checkable
class W4PublicIndexAdapter(Protocol):
    def query_public(self, request: Mapping[str, Any]) -> Mapping[str, Any]: ...


@runtime_checkable
class W4ArtifactAccessAdapter(Protocol):
    def access(self, request: Mapping[str, Any]) -> Mapping[str, Any]: ...


@runtime_checkable
class W4CacheAdapter(Protocol):
    def get(
        self,
        *,
        object_id: str,
        representation_id: str,
        expected_sha256: str | None = None,
    ) -> Any: ...

    def put(
        self,
        *,
        request_id: str,
        object_id: str,
        representation_id: str,
        payload: bytes,
        expected_sha256: str | None = None,
    ) -> Mapping[str, Any]: ...


@runtime_checkable
class W4ByteTransportAdapter(Protocol):
    def transfer(
        self,
        request: Mapping[str, Any],
        payload: bytes,
    ) -> Mapping[str, Any]: ...


@runtime_checkable
class W4SemanticRankingAdapter(Protocol):
    def rank(self, request: Mapping[str, Any]) -> Mapping[str, Any]: ...


@runtime_checkable
class W4RankingReturnAdapter(Protocol):
    def return_ranking(self, request: Mapping[str, Any]) -> Mapping[str, Any]: ...


@dataclass(frozen=True)
class W4IndexDeployment:
    adapter: W4PublicIndexAdapter
    package_dir: Path


@dataclass(frozen=True)
class W4LiveComponents:
    admission: W4AdmissionAdapter
    indexes: Mapping[str, W4IndexDeployment]
    artifacts: Mapping[str, W4ArtifactAccessAdapter]
    caches: Mapping[str, W4CacheAdapter]
    transport: W4ByteTransportAdapter
    semantic_ranker: W4SemanticRankingAdapter
    ranking_return: W4RankingReturnAdapter


_CONTROL_FIELDS = {
    "schema_version",
    "status",
    "execution_token",
    "operation_key",
    "request_sha256",
    "accepted",
    "ranking_sha256",
    "service_time_ms",
    "telemetry_complete",
    "flowmesh_workflow_submitted",
    "credentials_recorded",
}
_ARTIFACT_FIELDS = {
    "schema_version",
    "status",
    "execution_token",
    "operation_key",
    "request_sha256",
    "node_id",
    "object_id",
    "representation_id",
    "content_sha256",
    "size_bytes",
    "range_start",
    "range_end",
    "payload",
    "service_time_ms",
    "telemetry_complete",
    "flowmesh_workflow_submitted",
    "credentials_recorded",
}
_TRANSFER_FIELDS = {
    "schema_version",
    "status",
    "execution_token",
    "operation_key",
    "request_sha256",
    "source_node_id",
    "destination_node_id",
    "content_sha256",
    "size_bytes",
    "payload",
    "service_time_ms",
    "telemetry_complete",
    "flowmesh_workflow_submitted",
    "credentials_recorded",
}
_RANKING_FIELDS = {
    "schema_version",
    "status",
    "execution_token",
    "operation_key",
    "request_sha256",
    "ranked_object_ids",
    "candidate_inputs_sha256",
    "fallback_ranking_sha256",
    "complete_output_ranking",
    "service_time_ms",
    "telemetry_complete",
    "llm_called",
    "flowmesh_workflow_submitted",
    "credentials_recorded",
}
_EVENT_FIELDS = {
    "schema_version",
    "execution_token",
    "operation_key",
    "action",
    "component_kind",
    "adapter_invoked",
    "adapter_telemetry_complete",
    "artifact_identity_sha256",
    "exact_content_range_sha256",
    "index_binding_sha256",
    "ranking_sha256",
    "cache_outcome",
    "logical_bytes",
    "physical_bytes",
    "service_time_ms",
    "llm_called",
    "flowmesh_workflow_submitted",
    "executor_result_sha256",
    "idempotent_replay",
    "credentials_recorded",
    "eligible_for_scientific_claims",
}


def _telemetry(value: Mapping[str, Any], name: str) -> tuple[float, bool, bool]:
    service = _nonnegative_number(value.get("service_time_ms"), f"{name} time")
    _require(
        value.get("telemetry_complete") is True
        and type(value.get("flowmesh_workflow_submitted")) is bool
        and value.get("credentials_recorded") is False,
        f"{name} telemetry is incomplete or unsafe",
    )
    return service, bool(value["flowmesh_workflow_submitted"]), True


def _cache_payload(value: Any) -> tuple[bytes, str, int] | None:
    if value is None:
        return None
    payload = getattr(value, "payload", None)
    digest = getattr(value, "content_sha256", None)
    size = getattr(value, "size_bytes", None)
    _require(
        isinstance(payload, bytes)
        and isinstance(digest, str)
        and type(size) is int,
        "cache adapter returned an invalid artifact",
    )
    return payload, digest, size


class LiveW4CandidateOperationExecutor(W4CandidateOperationExecutor):
    """Execute public W4 operations through source-bound injected components."""

    def __init__(
        self,
        *,
        route_package_dir: str | Path,
        crosswalk_dir: str | Path,
        canonical_index_package_dir: str | Path,
        components: W4LiveComponents,
        evidence_class: str,
    ) -> None:
        _require(evidence_class in _EVIDENCE_CLASSES, "evidence_class is invalid")
        _require(isinstance(components, W4LiveComponents), "components are invalid")
        self._route_root = Path(route_package_dir).resolve()
        self._source = load_full_flow_w4_candidate_route_inputs(self._route_root)
        self._crosswalk_root = Path(crosswalk_dir).resolve()
        self._canonical_index = Path(canonical_index_package_dir).resolve()
        verify_full_flow_w4_index_artifact_crosswalk(
            self._crosswalk_root,
            route_package_dir=self._route_root,
            index_package_dir=self._canonical_index,
        )
        self._crosswalk = _read_json(
            self._crosswalk_root / CROSSWALK_NAME,
            "crosswalk",
        )
        self._crosswalk_by_id = {
            row["object_id"]: row for row in self._crosswalk["objects"]
        }
        _require(
            set(components.indexes) == {"N2", "N7", "N8"},
            "index deployments must cover N2, N7, and N8",
        )
        for node_id, deployment in components.indexes.items():
            _require(
                isinstance(deployment, W4IndexDeployment)
                and isinstance(deployment.adapter, W4PublicIndexAdapter),
                f"{node_id} index deployment is invalid",
            )
            verified = verify_n2_index_package(deployment.package_dir)
            _require(
                verified["index_sha256"] == self._crosswalk["index_sha256"]
                and verified["source_manifest_sha256"]
                == self._crosswalk["index_source_manifest_sha256"],
                f"{node_id} index package differs from crosswalk",
            )
        _require(
            set(components.artifacts) == {"N3", "N4"},
            "artifact adapters must cover N3 and N4",
        )
        _require(
            set(components.caches) == {"N7", "N8"},
            "cache adapters must cover N7 and N8",
        )
        self._components = components
        self.evidence_class = evidence_class
        self._request_by_token: dict[str, str] = {}
        self._result_by_token: dict[str, dict[str, Any]] = {}
        self._payload_by_operation: dict[
            str, tuple[bytes, Mapping[str, Any], str]
        ] = {}
        self._index_rows_by_operation: dict[str, list[dict[str, Any]]] = {}
        self._ranking_by_operation: dict[str, list[str]] = {}
        self._events: list[dict[str, Any]] = []

    @property
    def events(self) -> tuple[Mapping[str, Any], ...]:
        return tuple(json.loads(json.dumps(row)) for row in self._events)

    def _result(
        self,
        *,
        execution_token: str,
        operation: Mapping[str, Any],
        context: Mapping[str, Any],
        accepted: bool = False,
        ranking: Sequence[str] | None = None,
        cache_outcome: str | None = None,
        service_time_ms: float = 0.0,
        llm_called: bool = False,
        flowmesh: bool = False,
    ) -> dict[str, Any]:
        return {
            "schema_version": W4_CANDIDATE_OPERATION_RESULT_SCHEMA_VERSION,
            "status": "COMPLETED",
            "execution_token": execution_token,
            "operation_key": operation["operation_key"],
            "action": operation["action"],
            "accepted": accepted,
            "artifact_identity": context["artifact_identity"],
            "exact_content_range": context["exact_content_range"],
            "ranked_object_ids": None if ranking is None else list(ranking),
            "cache_outcome": cache_outcome,
            "index_binding": context["index_binding"],
            "logical_bytes": context["expected_logical_bytes"],
            "physical_bytes": context["expected_physical_bytes"],
            "service_time_ms": service_time_ms,
            "telemetry_complete": True,
            "llm_called": llm_called,
            "flowmesh_workflow_submitted": flowmesh,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }

    def _payload_from_dependencies(
        self, operation: Mapping[str, Any]
    ) -> tuple[bytes, Mapping[str, Any], str]:
        keys = operation["dependency_operation_keys"]
        values = [
            self._payload_by_operation[key]
            for key in keys
            if key in self._payload_by_operation
        ]
        _require(len(values) == 1, "operation has ambiguous payload provenance")
        return values[0]

    def _check_crosswalk_identity(
        self,
        object_id: str,
        identity: Mapping[str, Any],
    ) -> None:
        row = self._crosswalk_by_id.get(object_id)
        _require(row is not None, "artifact object is absent from crosswalk")
        representation = identity.get("representation_id")
        expected = row["representation_identity_sha256"].get(representation)
        _require(
            expected == _identity_digest(identity),
            "artifact identity differs from index-to-artifact crosswalk",
        )

    def _admit(
        self,
        run_id: str,
        token: str,
        operation: Mapping[str, Any],
        trial: Mapping[str, Any],
        public_task: Mapping[str, Any],
        context: Mapping[str, Any],
    ) -> tuple[dict[str, Any], str, bool]:
        request = {
            "run_id": run_id,
            "execution_token": token,
            "operation_key": operation["operation_key"],
            "trial_key": trial["trial_key"],
            "task_binding_sha256": public_task["task_binding_sha256"],
            "candidate_set_sha256": public_task["candidate_set_sha256"],
            "credentials_recorded": False,
        }
        raw = dict(self._components.admission.admit(request))
        _strict_fields(raw, _CONTROL_FIELDS, "admission result")
        _require(
            raw.get("schema_version") == W4_CONTROL_RESULT_SCHEMA_VERSION
            and raw.get("status") == "COMPLETED"
            and raw.get("execution_token") == token
            and raw.get("operation_key") == operation["operation_key"]
            and raw.get("request_sha256") == _sha256(_canonical(request))
            and raw.get("accepted") is True
            and raw.get("ranking_sha256") is None,
            "admission result binding changed",
        )
        service, flowmesh, _ = _telemetry(raw, "admission")
        return self._result(
            execution_token=token,
            operation=operation,
            context=context,
            accepted=True,
            service_time_ms=service,
            flowmesh=flowmesh,
        ), "n1-admission", True

    def _query_index(
        self,
        token: str,
        operation: Mapping[str, Any],
        context: Mapping[str, Any],
    ) -> tuple[dict[str, Any], str, bool]:
        template = operation["index_query_template"]
        node_id = str(template["requested_node_id"])
        deployment = self._components.indexes[node_id]
        request = build_n2_index_query_request(
            request_id=token,
            query_id=template["query_id"],
            index_id=template["index_id"],
            query_text=template["query_text"],
            top_k=template["top_k"],
            candidate_object_ids=template["candidate_object_ids"],
            requested_node_id=node_id,
        )
        started = time.monotonic_ns()
        raw = dict(deployment.adapter.query_public(request))
        service = (time.monotonic_ns() - started) / 1_000_000.0
        verified = verify_n2_public_index_query_result(
            package_dir=deployment.package_dir,
            request=request,
            result=raw,
            expected_node_id=node_id,
        )
        _require(
            verified["index_id"] == context["index_binding"]["index_id"]
            and raw["index_sha256"]
            == context["index_binding"]["index_sha256"]
            and raw["source_manifest_sha256"]
            == context["index_binding"]["source_manifest_sha256"],
            "index result differs from operation binding",
        )
        rows = list(raw["ranked_candidates"])
        for row in rows:
            crosswalk = self._crosswalk_by_id.get(row["object_id"])
            _require(
                crosswalk is not None
                and row["visible_fields_sha256"]
                == crosswalk["visible_fields_sha256"],
                "index result differs from source-verified crosswalk",
            )
        self._index_rows_by_operation[str(operation["operation_key"])] = rows
        ranking = [row["object_id"] for row in rows]
        return self._result(
            execution_token=token,
            operation=operation,
            context=context,
            ranking=ranking,
            service_time_ms=service,
        ), f"{node_id.lower()}-public-index", True

    def _merge_index(
        self,
        token: str,
        operation: Mapping[str, Any],
        context: Mapping[str, Any],
    ) -> tuple[dict[str, Any], str, bool]:
        rows = [
            row
            for key in operation["dependency_operation_keys"]
            for row in self._index_rows_by_operation.get(str(key), [])
        ]
        expected = context["ranking_candidate_ids"]
        _require(
            isinstance(expected, list)
            and len(rows) == len(expected)
            and {row["object_id"] for row in rows} == set(expected),
            "index merge did not receive exact shard coverage",
        )
        rows.sort(key=lambda row: (
            -len(row["matched_terms"]),
            -int(row["lexical_score_units"]),
            row["object_id"],
        ))
        self._index_rows_by_operation[str(operation["operation_key"])] = rows
        return self._result(
            execution_token=token,
            operation=operation,
            context=context,
            ranking=[row["object_id"] for row in rows],
        ), "verified-index-merge", False

    def _access_artifact(
        self,
        token: str,
        operation: Mapping[str, Any],
        context: Mapping[str, Any],
    ) -> tuple[dict[str, Any], str, bool]:
        identity = context["artifact_identity"]
        _require(isinstance(identity, Mapping), "artifact identity is missing")
        object_id = str(operation["object_id"])
        self._check_crosswalk_identity(object_id, identity)
        node_id = str(operation["logical_node_ids"][0])
        request = {
            "execution_token": token,
            "operation_key": operation["operation_key"],
            "node_id": node_id,
            "object_id": object_id,
            "artifact_identity": dict(identity),
            "exact_content_range": context["exact_content_range"],
            "data_agent_plan_id": operation.get("data_agent_plan_id"),
            "credentials_recorded": False,
        }
        raw = dict(self._components.artifacts[node_id].access(request))
        _strict_fields(raw, _ARTIFACT_FIELDS, "artifact access result")
        exact_range = context["exact_content_range"]
        start = 0 if exact_range is None else int(exact_range["range_start"])
        end = identity["artifact_size_bytes"] - 1 if exact_range is None else int(
            exact_range["range_end"]
        )
        expected_sha = (
            identity["artifact_sha256"]
            if exact_range is None
            else exact_range["range_sha256"]
        )
        payload = raw.get("payload")
        _require(
            raw.get("schema_version") == W4_ARTIFACT_ACCESS_RESULT_SCHEMA_VERSION
            and raw.get("status") == "COMPLETED"
            and raw.get("execution_token") == token
            and raw.get("operation_key") == operation["operation_key"]
            and raw.get("request_sha256") == _sha256(_canonical(request))
            and raw.get("node_id") == node_id
            and raw.get("object_id") == object_id
            and raw.get("representation_id") == identity["representation_id"]
            and isinstance(payload, bytes)
            and raw.get("size_bytes") == len(payload)
            and raw.get("content_sha256") == expected_sha == _sha256(payload)
            and raw.get("range_start") == start
            and raw.get("range_end") == end,
            "artifact access bytes or binding changed",
        )
        service, flowmesh, _ = _telemetry(raw, "artifact access")
        self._payload_by_operation[str(operation["operation_key"])] = (
            payload,
            identity,
            object_id,
        )
        return self._result(
            execution_token=token,
            operation=operation,
            context=context,
            service_time_ms=service,
            flowmesh=flowmesh,
        ), f"{node_id.lower()}-artifact-access", True

    def _transfer(
        self,
        token: str,
        operation: Mapping[str, Any],
        context: Mapping[str, Any],
    ) -> tuple[dict[str, Any], str, bool]:
        payload, identity, object_id = self._payload_from_dependencies(operation)
        nodes = operation["logical_node_ids"]
        request = {
            "execution_token": token,
            "operation_key": operation["operation_key"],
            "source_node_id": nodes[0],
            "destination_node_id": nodes[-1],
            "content_sha256": _sha256(payload),
            "size_bytes": len(payload),
            "credentials_recorded": False,
        }
        raw = dict(self._components.transport.transfer(request, payload))
        _strict_fields(raw, _TRANSFER_FIELDS, "byte transfer result")
        returned = raw.get("payload")
        _require(
            raw.get("schema_version") == W4_BYTE_TRANSFER_RESULT_SCHEMA_VERSION
            and raw.get("status") == "COMPLETED"
            and raw.get("execution_token") == token
            and raw.get("operation_key") == operation["operation_key"]
            and raw.get("request_sha256") == _sha256(_canonical(request))
            and raw.get("source_node_id") == nodes[0]
            and raw.get("destination_node_id") == nodes[-1]
            and isinstance(returned, bytes)
            and returned == payload
            and raw.get("size_bytes") == len(payload)
            and raw.get("content_sha256") == _sha256(payload),
            "transport did not preserve exact bytes or binding",
        )
        service, flowmesh, _ = _telemetry(raw, "byte transfer")
        self._payload_by_operation[str(operation["operation_key"])] = (
            returned,
            context["artifact_identity"] or identity,
            str(operation.get("object_id") or object_id),
        )
        return self._result(
            execution_token=token,
            operation=operation,
            context=context,
            service_time_ms=service,
            flowmesh=flowmesh,
        ), "byte-preserving-transport", True

    def _cache(
        self,
        token: str,
        operation: Mapping[str, Any],
        context: Mapping[str, Any],
    ) -> tuple[dict[str, Any], str, bool]:
        action = str(operation["action"])
        node_id = str(operation["logical_node_ids"][0])
        adapter = self._components.caches[node_id]
        identity = context["artifact_identity"]
        _require(isinstance(identity, Mapping), "cache artifact identity missing")
        object_id = str(operation["object_id"])
        self._check_crosswalk_identity(object_id, identity)
        started = time.monotonic_ns()
        if action in {"lookup", "read"}:
            raw = adapter.get(
                object_id=object_id,
                representation_id=identity["representation_id"],
                expected_sha256=identity["artifact_sha256"],
            )
            cached = _cache_payload(raw)
            if action == "lookup":
                outcome = "hit" if cached is not None else "miss"
                _require(
                    outcome == context["expected_cache_outcome"],
                    "cache adapter outcome differs from frozen lifecycle",
                )
            else:
                _require(cached is not None, "cache read returned a miss")
                outcome = None
            if cached is not None:
                payload, digest, size = cached
                _require(
                    digest == identity["artifact_sha256"]
                    and size == identity["artifact_size_bytes"]
                    and len(payload) == size
                    and _sha256(payload) == digest,
                    "cache returned different artifact bytes",
                )
                self._payload_by_operation[str(operation["operation_key"])] = (
                    payload,
                    identity,
                    object_id,
                )
        else:
            payload, source_identity, source_object_id = (
                self._payload_from_dependencies(operation)
            )
            _require(
                source_identity == identity and source_object_id == object_id,
                "cache insert dependency identity changed",
            )
            raw = adapter.put(
                request_id=token,
                object_id=object_id,
                representation_id=identity["representation_id"],
                payload=payload,
                expected_sha256=identity["artifact_sha256"],
            )
            _require(
                isinstance(raw, Mapping)
                and raw.get("status") in {"STORED", "PRESENT"}
                and raw.get("content_sha256") == identity["artifact_sha256"]
                and raw.get("size_bytes") == len(payload)
                and raw.get("credentials_recorded") is False,
                "cache insert result changed",
            )
            self._payload_by_operation[str(operation["operation_key"])] = (
                payload,
                identity,
                object_id,
            )
            outcome = None
        elapsed = (time.monotonic_ns() - started) / 1_000_000.0
        return self._result(
            execution_token=token,
            operation=operation,
            context=context,
            cache_outcome=outcome,
            service_time_ms=elapsed,
        ), f"{node_id.lower()}-artifact-cache", True

    def _local_payload_join(
        self,
        token: str,
        operation: Mapping[str, Any],
        context: Mapping[str, Any],
    ) -> tuple[dict[str, Any], str, bool]:
        payload, identity, object_id = self._payload_from_dependencies(operation)
        _require(
            identity == context["artifact_identity"],
            "local payload preparation identity changed",
        )
        self._payload_by_operation[str(operation["operation_key"])] = (
            payload,
            identity,
            object_id,
        )
        return self._result(
            execution_token=token,
            operation=operation,
            context=context,
        ), "verified-local-payload-preparation", False

    def _ranking(
        self,
        token: str,
        operation: Mapping[str, Any],
        public_task: Mapping[str, Any],
        context: Mapping[str, Any],
    ) -> tuple[dict[str, Any], str, bool]:
        ranking_dependencies = [
            self._ranking_by_operation[str(dependency)]
            for dependency in operation["dependency_operation_keys"]
            if str(dependency) in self._ranking_by_operation
        ]
        _require(
            len(ranking_dependencies) <= 1,
            "semantic ranking received ambiguous fallback rankings",
        )
        fallback: list[str] | None = (
            None if not ranking_dependencies else list(ranking_dependencies[0])
        )
        if context["required_ranking"] is not None:
            fallback = list(context["required_ranking"])
        payloads: list[dict[str, Any]] = []
        for dependency in operation["dependency_operation_keys"]:
            item = self._payload_by_operation.get(str(dependency))
            if item is None:
                continue
            payload, identity, object_id = item
            self._check_crosswalk_identity(object_id, identity)
            payloads.append({
                "object_id": object_id,
                "representation_id": identity["representation_id"],
                "content_sha256": _sha256(payload),
                "size_bytes": len(payload),
            })
        payloads.sort(key=lambda row: row["object_id"])
        candidate_inputs_sha256 = _sha256(_canonical(payloads))
        fallback_sha256 = (
            None if fallback is None else _sha256(_canonical(fallback))
        )
        request = {
            "execution_token": token,
            "operation_key": operation["operation_key"],
            "action": operation["action"],
            "query_id": public_task["query_id"],
            "query_text": public_task["query_text"],
            "candidate_object_ids": context["ranking_candidate_ids"],
            "candidate_inputs": payloads,
            "fallback_ranking": fallback,
            "credentials_recorded": False,
        }
        raw = dict(self._components.semantic_ranker.rank(request))
        _strict_fields(raw, _RANKING_FIELDS, "semantic ranking result")
        ranking = raw.get("ranked_object_ids")
        expected_ids = context["ranking_candidate_ids"]
        _require(
            raw.get("schema_version") == W4_SEMANTIC_RANKING_RESULT_SCHEMA_VERSION
            and raw.get("status") == "COMPLETED"
            and raw.get("execution_token") == token
            and raw.get("operation_key") == operation["operation_key"]
            and raw.get("request_sha256") == _sha256(_canonical(request))
            and raw.get("candidate_inputs_sha256") == candidate_inputs_sha256
            and raw.get("fallback_ranking_sha256") == fallback_sha256
            and isinstance(ranking, list)
            and ranking == list(dict.fromkeys(ranking))
            and set(ranking) == set(expected_ids)
            and len(ranking) == len(expected_ids)
            and raw.get("complete_output_ranking") is True
            and raw.get("llm_called") is True,
            "semantic ranker returned an incomplete or unbound ranking",
        )
        service, flowmesh, _ = _telemetry(raw, "semantic ranking")
        return self._result(
            execution_token=token,
            operation=operation,
            context=context,
            ranking=ranking,
            service_time_ms=service,
            llm_called=True,
            flowmesh=flowmesh,
        ), "n6-semantic-ranking", True

    def _return_ranking(
        self,
        token: str,
        operation: Mapping[str, Any],
        context: Mapping[str, Any],
    ) -> tuple[dict[str, Any], str, bool]:
        ranking = context["required_ranking"]
        _require(isinstance(ranking, list), "public return ranking is missing")
        request = {
            "execution_token": token,
            "operation_key": operation["operation_key"],
            "ranked_object_ids": ranking,
            "ranking_sha256": _sha256(_canonical(ranking)),
            "credentials_recorded": False,
        }
        raw = dict(self._components.ranking_return.return_ranking(request))
        _strict_fields(raw, _CONTROL_FIELDS, "ranking return result")
        _require(
            raw.get("schema_version") == W4_CONTROL_RESULT_SCHEMA_VERSION
            and raw.get("status") == "COMPLETED"
            and raw.get("execution_token") == token
            and raw.get("operation_key") == operation["operation_key"]
            and raw.get("request_sha256") == _sha256(_canonical(request))
            and raw.get("accepted") is False
            and raw.get("ranking_sha256") == request["ranking_sha256"],
            "N1 ranking return changed the public ranking",
        )
        service, flowmesh, _ = _telemetry(raw, "ranking return")
        return self._result(
            execution_token=token,
            operation=operation,
            context=context,
            ranking=ranking,
            service_time_ms=service,
            flowmesh=flowmesh,
        ), "n1-public-ranking-return", True

    def execute(
        self,
        *,
        run_id: str,
        execution_token: str,
        trial: Mapping[str, Any],
        operation: Mapping[str, Any],
        dependency_results: Sequence[Mapping[str, Any]],
        public_task: Mapping[str, Any],
        context: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        _require(
            public_task["task_binding_sha256"]
            == self._source.public_task["task_binding_sha256"],
            "public task differs from executor binding",
        )
        invocation = {
            "run_id": run_id,
            "execution_token": execution_token,
            "trial_key": trial["trial_key"],
            "operation": dict(operation),
            "dependency_result_sha256": [
                _sha256(_canonical(value)) for value in dependency_results
            ],
            "public_task_binding_sha256": public_task["task_binding_sha256"],
            "context": dict(context),
        }
        invocation_sha = _sha256(_canonical(invocation))
        previous_request = self._request_by_token.get(execution_token)
        if previous_request is not None:
            _require(
                previous_request == invocation_sha,
                "execution token was reused for a different operation request",
            )
            return json.loads(json.dumps(self._result_by_token[execution_token]))

        action = str(operation["action"])
        if action == "admit-public-retrieval":
            result, component, invoked = self._admit(
                run_id,
                execution_token,
                operation,
                trial,
                public_task,
                context,
            )
        elif action == "query-candidate-index-shard":
            result, component, invoked = self._query_index(
                execution_token, operation, context
            )
        elif action == "merge-complete-candidate-index":
            result, component, invoked = self._merge_index(
                execution_token, operation, context
            )
        elif action in {
            "access-raw",
            "access-exact-raw-range",
            "access-derived-artifact",
        }:
            result, component, invoked = self._access_artifact(
                execution_token, operation, context
            )
        elif action in {
            "transfer-artifact-bytes",
            "transfer-prepared-model-input",
        }:
            result, component, invoked = self._transfer(
                execution_token, operation, context
            )
        elif action in {"lookup", "read", "insert"}:
            result, component, invoked = self._cache(
                execution_token, operation, context
            )
        elif action in {
            "prepare-retrieval-candidate",
            "join-hit-or-miss-branch",
        }:
            result, component, invoked = self._local_payload_join(
                execution_token, operation, context
            )
        elif action == "transfer-ranking-fallback":
            ranking = context["required_ranking"]
            _require(isinstance(ranking, list), "fallback ranking is missing")
            result = self._result(
                execution_token=execution_token,
                operation=operation,
                context=context,
                ranking=ranking,
            )
            component, invoked = "verified-ranking-propagation", False
        elif action in {
            "rank-digest-prefix-and-append-index-tail",
            "rank-complete-candidate-set",
        }:
            result, component, invoked = self._ranking(
                execution_token, operation, public_task, context
            )
        elif action == "return-public-ranking-to-n1":
            result, component, invoked = self._return_ranking(
                execution_token, operation, context
            )
        else:
            raise FullFlowW4LiveExecutorError(
                f"unsupported W4 candidate action: {action}"
            )

        self._request_by_token[execution_token] = invocation_sha
        self._result_by_token[execution_token] = result
        if isinstance(result.get("ranked_object_ids"), list):
            self._ranking_by_operation[str(operation["operation_key"])] = list(
                result["ranked_object_ids"]
            )
        identity = context["artifact_identity"]
        exact_range = context["exact_content_range"]
        ranking = result["ranked_object_ids"]
        event: dict[str, Any] = {
            "schema_version": W4_LIVE_COMPONENT_EVENT_SCHEMA_VERSION,
            "execution_token": execution_token,
            "operation_key": operation["operation_key"],
            "action": action,
            "component_kind": component,
            "adapter_invoked": invoked,
            "adapter_telemetry_complete": result["telemetry_complete"],
            "artifact_identity_sha256": (
                None if identity is None else _identity_digest(identity)
            ),
            "exact_content_range_sha256": (
                None if exact_range is None else _sha256(_canonical(exact_range))
            ),
            "index_binding_sha256": (
                None
                if context["index_binding"] is None
                else _sha256(_canonical(context["index_binding"]))
            ),
            "ranking_sha256": (
                None if ranking is None else _sha256(_canonical(ranking))
            ),
            "cache_outcome": result["cache_outcome"],
            "logical_bytes": result["logical_bytes"],
            "physical_bytes": result["physical_bytes"],
            "service_time_ms": result["service_time_ms"],
            "llm_called": result["llm_called"],
            "flowmesh_workflow_submitted": result[
                "flowmesh_workflow_submitted"
            ],
            "executor_result_sha256": _sha256(_canonical(result)),
            "idempotent_replay": False,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }
        self._events.append(event)
        return json.loads(json.dumps(result))


def _validate_component_events(
    events: Sequence[Mapping[str, Any]],
    completed: Mapping[str, Mapping[str, Any]],
) -> None:
    seen_tokens: set[str] = set()
    for row in events:
        _strict_fields(row, _EVENT_FIELDS, "component event")
        token = row.get("execution_token")
        _require(
            row.get("schema_version")
            == W4_LIVE_COMPONENT_EVENT_SCHEMA_VERSION
            and isinstance(token, str)
            and token not in seen_tokens
            and token in completed
            and row.get("operation_key")
            == completed[token]["operation_key"]
            and row.get("action") == completed[token]["action"]
            and row.get("executor_result_sha256")
            == completed[token]["executor_result_sha256"]
            and row.get("artifact_identity_sha256")
            == completed[token]["artifact_identity_sha256"]
            and row.get("exact_content_range_sha256")
            == completed[token]["exact_content_range_sha256"]
            and row.get("index_binding_sha256")
            == completed[token]["index_binding_sha256"]
            and row.get("ranking_sha256")
            == (
                None
                if completed[token]["ranked_object_ids"] is None
                else _sha256(
                    _canonical(completed[token]["ranked_object_ids"])
                )
            )
            and row.get("cache_outcome")
            == completed[token]["cache_outcome"]
            and row.get("logical_bytes")
            == completed[token]["logical_bytes"]
            and row.get("physical_bytes")
            == completed[token]["physical_bytes"]
            and row.get("service_time_ms")
            == completed[token]["service_time_ms"]
            and row.get("llm_called") == completed[token]["llm_called"]
            and row.get("flowmesh_workflow_submitted")
            == completed[token]["flowmesh_workflow_submitted"]
            and type(row.get("adapter_invoked")) is bool
            and row.get("adapter_telemetry_complete") is True
            and type(row.get("llm_called")) is bool
            and type(row.get("flowmesh_workflow_submitted")) is bool
            and row.get("idempotent_replay") is False
            and type(row.get("logical_bytes")) is int
            and row["logical_bytes"] >= 0
            and type(row.get("physical_bytes")) is int
            and row["physical_bytes"] >= 0
            and row.get("credentials_recorded") is False
            and row.get("eligible_for_scientific_claims") is False,
            "component event differs from coordinator evidence",
        )
        _identifier(row.get("operation_key"), "component operation_key")
        _identifier(row.get("action"), "component action")
        _identifier(row.get("component_kind"), "component_kind")
        _digest(token, "component execution_token")
        _digest(row.get("executor_result_sha256"), "executor result digest")
        for name in (
            "artifact_identity_sha256",
            "exact_content_range_sha256",
            "index_binding_sha256",
            "ranking_sha256",
        ):
            if row.get(name) is not None:
                _digest(row[name], name)
        _require(
            row.get("cache_outcome") in {None, "hit", "miss"},
            "component cache outcome is invalid",
        )
        _nonnegative_number(
            row.get("service_time_ms"), "component service time"
        )
        seen_tokens.add(token)
    _require(
        set(seen_tokens) == set(completed),
        "component receipt coverage differs from coordinator evidence",
    )


def freeze_full_flow_w4_component_execution_receipt(
    coordinator_run_dir: str | Path,
    *,
    route_package_dir: str | Path,
    crosswalk_dir: str | Path,
    index_package_dir: str | Path,
    executor: LiveW4CandidateOperationExecutor | None = None,
    component_events_path: str | Path | None = None,
    evidence_class: str | None = None,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Freeze public component events after a completed coordinator run.

    In-process callers may supply the executor that produced the coordinator
    run.  Operator tooling instead supplies its atomically persisted public
    event JSONL plus an explicit evidence class.  Exactly one event source is
    required; URLs, bearer tokens, and raw artifact bytes are not accepted by
    this interface or copied into the receipt.
    """

    run_root = Path(coordinator_run_dir).resolve()
    route_root = Path(route_package_dir).resolve()
    crosswalk_root = Path(crosswalk_dir).resolve()
    index_root = Path(index_package_dir).resolve()
    verified_run = verify_full_flow_w4_candidate_coordinator_run(
        run_root,
        route_package_dir=route_root,
    )
    verified_crosswalk = verify_full_flow_w4_index_artifact_crosswalk(
        crosswalk_root,
        route_package_dir=route_root,
        index_package_dir=index_root,
    )
    evidence = _read_jsonl(
        run_root / OPERATION_EVIDENCE_NAME,
        "coordinator operation evidence",
    )
    completed: dict[str, Mapping[str, Any]] = {
        row["execution_token"]: row
        for row in evidence
        if row["execution_status"] == "COMPLETED"
    }
    _require(
        (executor is None) is not (component_events_path is None),
        "exactly one of executor or component_events_path is required",
    )
    if executor is not None:
        events = [dict(row) for row in executor.events]
        resolved_evidence_class = executor.evidence_class
        _require(
            evidence_class is None
            or evidence_class == resolved_evidence_class,
            "evidence_class differs from executor",
        )
    else:
        _require(
            evidence_class in _EVIDENCE_CLASSES,
            "evidence_class is unsupported",
        )
        events = _read_jsonl(
            Path(component_events_path).resolve(),
            "persisted component events",
        )
        resolved_evidence_class = str(evidence_class)
    _require(
        len(events) == verified_run["activated_operation_count"]
        == len(completed),
        "component receipt coverage differs from coordinator evidence",
    )
    _validate_component_events(events, completed)
    events_bytes = _jsonl_bytes(events)
    coordinator_report = _read_json(
        run_root / COORDINATOR_RUN_NAME,
        "coordinator run",
    )
    report: dict[str, Any] = {
        "schema_version": W4_LIVE_COMPONENT_RECEIPT_SCHEMA_VERSION,
        "status": "FROZEN_COMPONENT_EXECUTION_RECEIPT",
        "run_id": coordinator_report["run_id"],
        "physical_plan_id": coordinator_report["physical_plan_id"],
        "route_plan_sha256": coordinator_report["route_plan_sha256"],
        "coordinator_run_sha256": coordinator_report["run_sha256"],
        "crosswalk_sha256": verified_crosswalk["crosswalk_sha256"],
        "index_sha256": verified_crosswalk["index_sha256"],
        "evidence_class": resolved_evidence_class,
        "declared_live_local_component_execution": (
            resolved_evidence_class == "live-local-component-execution"
        ),
        "strict_fake_components_declared": (
            resolved_evidence_class == "strict-fake-component-conformance"
        ),
        "operation_count": len(events),
        "adapter_invocation_count": sum(row["adapter_invoked"] for row in events),
        "component_kinds": sorted({row["component_kind"] for row in events}),
        "events_sha256": _sha256(events_bytes),
        "all_results_bound_to_coordinator_evidence": True,
        "all_adapter_telemetry_complete": all(
            row["adapter_telemetry_complete"] for row in events
        ),
        "llm_called": any(row["llm_called"] for row in events),
        "flowmesh_workflow_submitted": any(
            row["flowmesh_workflow_submitted"] for row in events
        ),
        "hidden_relevance_values_read": False,
        "endpoints_recorded": False,
        "credentials_recorded": False,
        "real_cloud_performance_measured": False,
        "eligible_for_scientific_claims": False,
    }
    report["receipt_sha256"] = _sha256(_canonical(report))
    documents = {
        RECEIPT_NAME: _json_bytes(report),
        EVENTS_NAME: events_bytes,
    }
    target = Path(output_dir).resolve()
    _publish(target, documents, ".w4-component-receipt-")
    verified = verify_full_flow_w4_component_execution_receipt(
        target,
        coordinator_run_dir=run_root,
        route_package_dir=route_root,
        crosswalk_dir=crosswalk_root,
        index_package_dir=index_root,
    )
    return {**verified, "status": "FROZEN", "output_dir": str(target)}


def verify_full_flow_w4_component_execution_receipt(
    output_dir: str | Path,
    *,
    coordinator_run_dir: str | Path,
    route_package_dir: str | Path,
    crosswalk_dir: str | Path,
    index_package_dir: str | Path,
) -> dict[str, Any]:
    root = Path(output_dir).resolve()
    _require(root.is_dir() and not root.is_symlink(), "receipt directory missing")
    files = {RECEIPT_NAME, EVENTS_NAME}
    _require(
        {path.name for path in root.iterdir()} == files | {CHECKSUMS_NAME}
        and all(path.is_file() and not path.is_symlink() for path in root.iterdir()),
        "component receipt file set changed",
    )
    report = _read_json(root / RECEIPT_NAME, "component receipt")
    events = _read_jsonl(root / EVENTS_NAME, "component events")
    documents = {
        RECEIPT_NAME: _json_bytes(report),
        EVENTS_NAME: _jsonl_bytes(events),
    }
    _require(
        (root / RECEIPT_NAME).read_bytes() == documents[RECEIPT_NAME]
        and (root / EVENTS_NAME).read_bytes() == documents[EVENTS_NAME]
        and (root / CHECKSUMS_NAME).read_bytes() == _checksums(documents),
        "component receipt bytes or checksums changed",
    )
    verified_run = verify_full_flow_w4_candidate_coordinator_run(
        coordinator_run_dir,
        route_package_dir=route_package_dir,
    )
    verified_crosswalk = verify_full_flow_w4_index_artifact_crosswalk(
        crosswalk_dir,
        route_package_dir=route_package_dir,
        index_package_dir=index_package_dir,
    )
    expected_report_fields = {
        "schema_version", "status", "run_id", "physical_plan_id",
        "route_plan_sha256", "coordinator_run_sha256", "crosswalk_sha256",
        "index_sha256", "evidence_class",
        "declared_live_local_component_execution",
        "strict_fake_components_declared", "operation_count",
        "adapter_invocation_count", "component_kinds", "events_sha256",
        "all_results_bound_to_coordinator_evidence",
        "all_adapter_telemetry_complete", "llm_called",
        "flowmesh_workflow_submitted", "hidden_relevance_values_read",
        "endpoints_recorded", "credentials_recorded",
        "real_cloud_performance_measured", "eligible_for_scientific_claims",
        "receipt_sha256",
    }
    _strict_fields(report, expected_report_fields, "component receipt")
    _require(
        report["schema_version"] == W4_LIVE_COMPONENT_RECEIPT_SCHEMA_VERSION
        and report["status"] == "FROZEN_COMPONENT_EXECUTION_RECEIPT"
        and report["evidence_class"] in _EVIDENCE_CLASSES
        and report["declared_live_local_component_execution"]
        is (report["evidence_class"] == "live-local-component-execution")
        and report["strict_fake_components_declared"]
        is (report["evidence_class"] == "strict-fake-component-conformance")
        and report["operation_count"] == len(events)
        == verified_run["activated_operation_count"]
        and report["events_sha256"] == _sha256(documents[EVENTS_NAME])
        and report["crosswalk_sha256"] == verified_crosswalk["crosswalk_sha256"]
        and report["index_sha256"] == verified_crosswalk["index_sha256"]
        and report["all_results_bound_to_coordinator_evidence"] is True
        and report["all_adapter_telemetry_complete"] is True
        and report["hidden_relevance_values_read"] is False
        and report["endpoints_recorded"] is False
        and report["credentials_recorded"] is False
        and report["real_cloud_performance_measured"] is False
        and report["eligible_for_scientific_claims"] is False,
        "component receipt claims or bindings changed",
    )
    coordinator_report = _read_json(
        Path(coordinator_run_dir).resolve() / COORDINATOR_RUN_NAME,
        "coordinator run",
    )
    _require(
        report["run_id"] == coordinator_report["run_id"]
        and report["physical_plan_id"]
        == coordinator_report["physical_plan_id"]
        and report["route_plan_sha256"]
        == coordinator_report["route_plan_sha256"]
        and report["coordinator_run_sha256"]
        == coordinator_report["run_sha256"],
        "component receipt coordinator binding changed",
    )
    expected_digest = report.pop("receipt_sha256")
    _require(expected_digest == _sha256(_canonical(report)), "receipt digest changed")
    report["receipt_sha256"] = expected_digest
    coordinator_evidence = _read_jsonl(
        Path(coordinator_run_dir).resolve() / OPERATION_EVIDENCE_NAME,
        "coordinator evidence",
    )
    completed = {
        row["execution_token"]: row
        for row in coordinator_evidence
        if row["execution_status"] == "COMPLETED"
    }
    _validate_component_events(events, completed)
    _require(
        report["adapter_invocation_count"]
        == sum(row["adapter_invoked"] for row in events)
        and report["component_kinds"]
        == sorted({row["component_kind"] for row in events})
        and report["llm_called"] == any(row["llm_called"] for row in events)
        and report["flowmesh_workflow_submitted"]
        == any(row["flowmesh_workflow_submitted"] for row in events),
        "component receipt summary differs from events",
    )
    return {
        "status": "VERIFIED",
        "run_id": report["run_id"],
        "evidence_class": report["evidence_class"],
        "operation_count": len(events),
        "adapter_invocation_count": report["adapter_invocation_count"],
        "llm_called": report["llm_called"],
        "flowmesh_workflow_submitted": report["flowmesh_workflow_submitted"],
        "real_cloud_performance_measured": False,
        "eligible_for_scientific_claims": False,
    }


__all__ = [
    "CROSSWALK_NAME",
    "EVENTS_NAME",
    "FullFlowW4LiveExecutorError",
    "LiveW4CandidateOperationExecutor",
    "RECEIPT_NAME",
    "W4AdmissionAdapter",
    "W4ArtifactAccessAdapter",
    "W4_ARTIFACT_ACCESS_RESULT_SCHEMA_VERSION",
    "W4ByteTransportAdapter",
    "W4_BYTE_TRANSFER_RESULT_SCHEMA_VERSION",
    "W4CacheAdapter",
    "W4_CONTROL_RESULT_SCHEMA_VERSION",
    "W4IndexDeployment",
    "W4_INDEX_ARTIFACT_CROSSWALK_SCHEMA_VERSION",
    "W4LiveComponents",
    "W4_LIVE_COMPONENT_EVENT_SCHEMA_VERSION",
    "W4_LIVE_COMPONENT_RECEIPT_SCHEMA_VERSION",
    "W4PublicIndexAdapter",
    "W4RankingReturnAdapter",
    "W4SemanticRankingAdapter",
    "W4_SEMANTIC_RANKING_RESULT_SCHEMA_VERSION",
    "freeze_full_flow_w4_component_execution_receipt",
    "freeze_full_flow_w4_index_artifact_crosswalk",
    "verify_full_flow_w4_component_execution_receipt",
    "verify_full_flow_w4_index_artifact_crosswalk",
]
