"""Concrete adapters for the generic full-flow semantic route runtime.

The route coordinator intentionally depends on small Protocol interfaces.
This module binds those interfaces to the services that already exist in the
repository without putting an endpoint or credential into route evidence.

Two boundaries are worth spelling out:

* N3/N4, N2/N7/N8 index, N7/N8 cache, N6, and N1 calls can use real HTTP
  clients.  Their addresses and credentials live only in the adapter objects.
* Trial admission and byte handoff inside the coordinator remain in-process.
  An explicit local-index service seam is retained for unit tests and embedded
  deployments, but the node-parametric index HTTP client is preferred.  These
  in-process adapters measure only their own process work and never claim
  network, queue, or monetary measurements.

Indexed raw access is fail-closed.  The N2 result is paired with an exact
source selection supplied by a verified catalog resolver.  A legacy entry is
an ``ExactContentRange``; the real-data profile uses a content-bound temporal
frame bundle produced and served by N3.  Neither form accepts an estimate.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence

from ..data_agent_client import (
    DataAgentAccessRequest,
    DataAgentAccessResult,
    DataAgentBinaryArtifact,
    HttpDataAgentClient,
)
from .full_flow_cache import CachedArtifact, HttpFullFlowArtifactCacheClient
from .full_flow_n6_adapters import (
    BoundN6SemanticInferenceAdapter,
    N6ModelInputAdapter,
)
from .full_flow_semantic_route_runtime import (
    AdapterTelemetry,
    ArtifactAccess,
    ArtifactIdentity,
    AuthenticatedN1Score,
    BranchJoinResult,
    CacheInsertResult,
    CacheLookupResult,
    ControlAdmission,
    ExactContentRange,
    ExactSourceSelection,
    ExactTemporalFrameSelection,
    IndexSelection,
    PreparedSemanticInput,
    ProvisioningReference,
    SemanticInferenceResult,
    SemanticRouteAdapters,
    TransferResult,
)
from .hidden_oracle import (
    N1OracleHTTPClient,
    verify_n1_score_result,
)
from .index_service import (
    N2IndexHTTPClient,
    build_n2_index_query_request,
)


ROUTE_ADAPTERS_VERSION = "pathfinder.full-flow-route-adapters/v1alpha1"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}\Z")
_PRIVATE_SIMULATOR_HOST = re.compile(
    r"(?:pathfinder-sim|pathfinder-full-flow)-[a-z0-9-]+\Z"
)
_RUNTIME_EPOCH = re.compile(r"[0-9a-f]{32}\Z")
_MAX_JSON_RESPONSE_BYTES = 4 * 1024 * 1024


class FullFlowRouteAdapterError(RuntimeError):
    """Raised when a service response cannot satisfy the frozen route."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise FullFlowRouteAdapterError(message)


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FullFlowRouteAdapterError("value is not canonical JSON") from exc


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


def _text(value: Any, name: str, *, maximum: int = 16_384) -> str:
    _require(isinstance(value, str) and bool(value), f"{name} must be text")
    _require(len(value.encode("utf-8")) <= maximum, f"{name} is too large")
    return str(value)


def _integer(value: Any, name: str, *, minimum: int = 0) -> int:
    _require(
        type(value) is int and value >= minimum,
        f"{name} must be an integer >= {minimum}",
    )
    return int(value)


def _number(value: Any, name: str) -> float:
    _require(
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
        and float(value) >= 0.0,
        f"{name} must be a finite non-negative number",
    )
    return float(value)


def _elapsed_ms(started_ns: int, finished_ns: int) -> float:
    _require(finished_ns >= started_ns, "adapter monotonic clock moved backwards")
    return (finished_ns - started_ns) / 1_000_000.0


def _raw_identity(trial: Mapping[str, Any]) -> ArtifactIdentity:
    rows = trial.get("representation_identities")
    _require(isinstance(rows, list), "trial representation identities are missing")
    matches = [
        row
        for row in rows
        if isinstance(row, Mapping) and row.get("representation_id") == "raw_video"
    ]
    _require(len(matches) == 1, "indexed trial does not bind one raw_video")
    row = matches[0]
    binding = row.get("representation_binding")
    _require(isinstance(binding, Mapping), "raw_video binding is missing")
    return ArtifactIdentity(
        object_id=_identifier(row.get("artifact_object_id"), "artifact_object_id"),
        representation_id="raw_video",
        artifact_sha256=_digest(
            binding.get("artifact_sha256"), "raw artifact SHA-256"
        ),
        artifact_size_bytes=_integer(
            binding.get("artifact_size_bytes"),
            "raw artifact size",
            minimum=1,
        ),
        object_catalog_version=_identifier(
            binding.get("object_catalog_version"), "raw catalog version"
        ),
    )


@dataclass(frozen=True)
class FrozenIndexQueryPlan:
    """Visible, label-free query inputs bound to one semantic trial."""

    trial_key: str
    task_binding_sha256: str
    index_id: str
    query_id: str
    query_text: str
    top_k: int
    candidate_object_ids: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        _text(self.trial_key, "index trial_key", maximum=2048)
        _digest(self.task_binding_sha256, "index task binding")
        _identifier(self.index_id, "index_id")
        _identifier(self.query_id, "query_id")
        _text(self.query_text, "query_text", maximum=64 * 1024)
        _integer(self.top_k, "top_k", minimum=1)
        if self.candidate_object_ids is not None:
            values = tuple(
                _identifier(value, "candidate object ID")
                for value in self.candidate_object_ids
            )
            _require(
                values == tuple(sorted(set(values))) and bool(values),
                "candidate object IDs must be sorted, unique, and non-empty",
            )
            _require(
                self.top_k <= len(values),
                "top_k exceeds the frozen candidate set",
            )


class IndexQueryPlanResolver(Protocol):
    def resolve(
        self,
        *,
        trial: Mapping[str, Any],
        public_task: Mapping[str, Any],
    ) -> FrozenIndexQueryPlan: ...


class ExactRangeResolver(Protocol):
    def resolve(self, identity: ArtifactIdentity) -> ExactSourceSelection: ...


class LocalIndexService(Protocol):
    """Optional in-process seam for embedded N7/N8 index deployments."""

    def health(self) -> Mapping[str, Any]: ...

    def query(self, value: Mapping[str, Any]) -> Mapping[str, Any]: ...


class FrozenIndexQueryPlanCatalog:
    """Read-only exact trial-key resolver for visible query plans."""

    def __init__(self, plans: Sequence[FrozenIndexQueryPlan]) -> None:
        rows: dict[str, FrozenIndexQueryPlan] = {}
        for plan in plans:
            _require(
                isinstance(plan, FrozenIndexQueryPlan),
                "index query plan has the wrong type",
            )
            _require(plan.trial_key not in rows, "index query plan repeats a trial")
            rows[plan.trial_key] = plan
        _require(bool(rows), "index query plan catalog is empty")
        self._plans = rows

    def resolve(
        self,
        *,
        trial: Mapping[str, Any],
        public_task: Mapping[str, Any],
    ) -> FrozenIndexQueryPlan:
        trial_key = _text(trial.get("trial_key"), "trial_key", maximum=2048)
        plan = self._plans.get(trial_key)
        _require(plan is not None, "trial is absent from the index query catalog")
        _require(
            plan.task_binding_sha256 == public_task.get("task_binding_sha256"),
            "index query plan binds a different public task",
        )
        _require(
            plan.query_text == public_task.get("question"),
            "index query text differs from the frozen public question",
        )
        return plan


class BoundIndexQueryAdapter:
    """Use deployment-bound N2/N7/N8 HTTP clients for index queries.

    ``N2IndexService`` can also be bound through ``local_index_services`` for
    a deliberately embedded deployment or local test.  That path is labelled
    in-process by construction and is not proof of a networked local index.
    """

    def __init__(
        self,
        *,
        clients: Mapping[str, N2IndexHTTPClient],
        query_plans: IndexQueryPlanResolver,
        exact_ranges: ExactRangeResolver,
        local_index_services: Mapping[str, LocalIndexService] | None = None,
        clock_ns: Callable[[], int] = time.perf_counter_ns,
    ) -> None:
        http = dict(clients)
        _require("N2" in http, "index HTTP clients must bind N2")
        _require(
            set(http).issubset({"N2", "N7", "N8"}),
            "index HTTP client map contains an invalid node",
        )
        self._plans = query_plans
        self._ranges = exact_ranges
        local = dict(local_index_services or {})
        _require(
            set(local).issubset({"N7", "N8"}),
            "local index service map contains a non-executor node",
        )
        _require(
            set(http).isdisjoint(local),
            "index node cannot bind both HTTP and in-process services",
        )
        self._http = http
        self._local = local
        self._clock_ns = clock_ns

    @staticmethod
    def _selection(result: Mapping[str, Any], expected_object_id: str) -> str:
        _require(
            result.get("status") == "COMPLETED"
            and result.get("lexical_retrieval_executed") is True
            and result.get("llm_called") is False
            and result.get("credentials_recorded") is False
            and result.get("eligible_for_scientific_claims") is False,
            "index query did not return safe completed evidence",
        )
        ranked = result.get("ranked_candidates")
        _require(isinstance(ranked, list) and bool(ranked), "index ranking is empty")
        first = ranked[0]
        _require(isinstance(first, Mapping), "index ranking row is invalid")
        selected = _identifier(first.get("object_id"), "selected object ID")
        _require(
            selected == expected_object_id,
            "index selected a different object from the frozen semantic trial",
        )
        return selected

    def query(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        public_task: Mapping[str, Any],
        expected_object_id: str,
    ) -> IndexSelection:
        del run_id
        plan = self._plans.resolve(trial=trial, public_task=public_task)
        request_id = _sha256(_canonical({
            "domain": "pathfinder.route-index-query/v1",
            "trial_key": trial.get("trial_key"),
            "stage_key": stage.get("stage_key"),
            "task_binding_sha256": plan.task_binding_sha256,
            "index_id": plan.index_id,
        }))
        nodes = stage.get("logical_node_ids")
        _require(
            isinstance(nodes, list) and len(nodes) == 1,
            "index stage must name one logical node",
        )
        logical_node = nodes[0]
        _require(
            logical_node in {"N2", "N7", "N8"},
            "index query is not assigned to N2, N7, or N8",
        )
        request = build_n2_index_query_request(
            request_id=request_id,
            query_id=plan.query_id,
            index_id=plan.index_id,
            query_text=plan.query_text,
            top_k=plan.top_k,
            candidate_object_ids=plan.candidate_object_ids,
            requested_node_id=logical_node,
        )
        started = self._clock_ns()
        if logical_node in self._http:
            client = self._http[logical_node]
            before = client.health()
            result = client.query(request)
            after = client.health()
            _require(
                before == after and before.get("node_id") == logical_node,
                f"{logical_node} index identity changed during query",
            )
            result_sha = _digest(
                result.get("result_content_sha256"),
                f"{logical_node} result content SHA-256",
            )
        else:
            service = self._local.get(logical_node)
            _require(
                service is not None,
                f"no in-process local index service was bound for {logical_node}",
            )
            before = dict(service.health())
            result = dict(service.query(request))
            after = dict(service.health())
            _require(
                before == after
                and before.get("status") == "ok"
                and before.get("node_id") == logical_node
                and before.get("credentials_recorded") is False,
                f"{logical_node} local index identity changed during query",
            )
            result_sha = _digest(
                result.get("result_content_sha256"),
                f"{logical_node} local result SHA-256",
            )
        selected = self._selection(result, expected_object_id)
        finished = self._clock_ns()
        segment = None
        if trial.get("route_family") == "indexed-raw":
            _require(logical_node == "N2", "indexed raw did not use global N2")
            identity = _raw_identity(trial)
            segment = self._ranges.resolve(identity)
            _require(
                isinstance(
                    segment,
                    (ExactContentRange, ExactTemporalFrameSelection),
                )
                and segment.matches(identity),
                "exact selection resolver changed the raw artifact identity",
            )
        return IndexSelection(
            selected_object_id=selected,
            index_result_sha256=result_sha,
            segment=segment,
            telemetry=AdapterTelemetry(
                service_time_ms=_elapsed_ms(started, finished),
                bytes_read=len(_canonical(result)),
                bytes_sent=len(_canonical(request)),
            ),
        )


class InProcessTrialControlAdapter:
    """Canonical local N1 admission until trial control has its own API."""

    def __init__(self, clock_ns: Callable[[], int] = time.perf_counter_ns) -> None:
        self._clock_ns = clock_ns

    def admit(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
    ) -> ControlAdmission:
        started = self._clock_ns()
        digest = _sha256(_canonical({
            "domain": "pathfinder.in-process-trial-admission/v1",
            "run_id": run_id,
            "trial_key": trial.get("trial_key"),
            "stage_key": stage.get("stage_key"),
            "order_index": trial.get("order_index"),
        }))
        finished = self._clock_ns()
        return ControlAdmission(
            admission_sha256=digest,
            telemetry=AdapterTelemetry(
                service_time_ms=_elapsed_ms(started, finished)
            ),
        )


class DataAgentPlanIdResolver(Protocol):
    def resolve(
        self,
        *,
        source_node_id: str,
        trial: Mapping[str, Any],
        identity: ArtifactIdentity,
    ) -> str: ...


class StaticDataAgentPlanIdResolver:
    """Resolve one fixed package plan ID per node/representation.

    This compact resolver is useful for a bounded smoke whose verified Data
    Agent manifests expose a shared plan ID.  A matrix with trial-specific
    bindings should use :class:`FrozenDataAgentPlanIdCatalog` instead.
    """

    def __init__(self, values: Mapping[tuple[str, str], str]) -> None:
        normalized: dict[tuple[str, str], str] = {}
        for (node, representation), plan_id in values.items():
            _require(node in {"N3", "N4"}, "Data Agent plan binds wrong node")
            key = (node, _identifier(representation, "representation ID"))
            _require(key not in normalized, "Data Agent plan binding repeats")
            normalized[key] = _identifier(plan_id, "Data Agent plan ID")
        _require(bool(normalized), "Data Agent plan resolver is empty")
        self._values = normalized

    def resolve(
        self,
        *,
        source_node_id: str,
        trial: Mapping[str, Any],
        identity: ArtifactIdentity,
    ) -> str:
        del trial
        value = self._values.get((source_node_id, identity.representation_id))
        _require(value is not None, "Data Agent plan binding is missing")
        return value


class FrozenDataAgentPlanIdCatalog:
    """Exact trial/object/representation plan bindings from frozen inputs."""

    def __init__(
        self,
        values: Mapping[tuple[str, str, str, str], str],
    ) -> None:
        normalized: dict[tuple[str, str, str, str], str] = {}
        for raw_key, plan_id in values.items():
            _require(
                isinstance(raw_key, tuple) and len(raw_key) == 4,
                "Data Agent plan catalog key is invalid",
            )
            trial_key, node, object_id, representation_id = raw_key
            key = (
                _text(trial_key, "plan catalog trial_key", maximum=2048),
                _text(node, "plan catalog node", maximum=2),
                _identifier(object_id, "plan catalog object_id"),
                _identifier(
                    representation_id,
                    "plan catalog representation_id",
                ),
            )
            _require(node in {"N3", "N4"}, "Data Agent plan binds wrong node")
            _require(key not in normalized, "Data Agent plan binding repeats")
            normalized[key] = _identifier(plan_id, "Data Agent plan ID")
        _require(bool(normalized), "Data Agent plan catalog is empty")
        self._values = normalized

    def resolve(
        self,
        *,
        source_node_id: str,
        trial: Mapping[str, Any],
        identity: ArtifactIdentity,
    ) -> str:
        key = (
            _text(trial.get("trial_key"), "trial_key", maximum=2048),
            source_node_id,
            identity.object_id,
            identity.representation_id,
        )
        value = self._values.get(key)
        _require(value is not None, "exact Data Agent plan binding is missing")
        return value


class BoundDataAgentAccessRequestFactory:
    """Build deterministic requests while keeping locations runtime-only."""

    def __init__(
        self,
        *,
        source_locations: Mapping[str, str],
        plan_ids: DataAgentPlanIdResolver,
        plan_epoch: int = 0,
        latency_multiplier: float = 1.0,
    ) -> None:
        _require(
            set(source_locations) == {"N3", "N4"},
            "source locations must bind exactly N3 and N4",
        )
        self._locations = {
            node: _identifier(value, f"{node} source location")
            for node, value in source_locations.items()
        }
        self._plans = plan_ids
        self._plan_epoch = _integer(plan_epoch, "plan_epoch")
        self._latency_multiplier = _number(
            latency_multiplier, "latency_multiplier"
        )
        _require(self._latency_multiplier > 0.0, "latency_multiplier must be positive")

    def build_for_source(
        self,
        *,
        source_node_id: str,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        identity: ArtifactIdentity,
    ) -> DataAgentAccessRequest:
        _require(source_node_id in self._locations, "unknown Data Agent source")
        plan_id = self._plans.resolve(
            source_node_id=source_node_id,
            trial=trial,
            identity=identity,
        )
        stage_index = _integer(stage.get("stage_index"), "stage_index")
        access_id = _sha256(_canonical({
            "domain": "pathfinder.semantic-route-data-agent-access/v1",
            "run_id": run_id,
            "trial_key": trial.get("trial_key"),
            "stage_key": stage.get("stage_key"),
            "source_node_id": source_node_id,
            "object_id": identity.object_id,
            "representation_id": identity.representation_id,
            "plan_id": plan_id,
        }))
        return DataAgentAccessRequest(
            access_id=access_id,
            session_id=run_id,
            trial_id=_text(
                trial.get("trial_key"), "trial_key", maximum=2048
            ),
            plan_id=plan_id,
            plan_epoch=self._plan_epoch,
            task_class_id=_identifier(
                (
                    trial.get("public_task_binding", {})
                    if isinstance(trial.get("public_task_binding"), Mapping)
                    else {}
                ).get("task_class_id"),
                "task_class_id",
            ),
            representation_id=identity.representation_id,
            event_index=stage_index,
            latency_multiplier=self._latency_multiplier,
            binding={"location": self._locations[source_node_id]},
            object_id=identity.object_id,
        )

    def build_request(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        identity: ArtifactIdentity,
        selection: IndexSelection,
    ) -> DataAgentAccessRequest:
        _require(
            selection.selected_object_id == identity.object_id
            and selection.segment is not None,
            "N3 range request lacks an exact N2 selection",
        )
        return self.build_for_source(
            source_node_id="N3",
            run_id=run_id,
            trial=trial,
            stage=stage,
            identity=identity,
        )

    def build_selected_request(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        source_identity: ArtifactIdentity,
        selection: ExactTemporalFrameSelection,
    ) -> DataAgentAccessRequest:
        _require(
            selection.matches(source_identity),
            "N3 projection request differs from its raw source identity",
        )
        return self.build_for_source(
            source_node_id="N3",
            run_id=run_id,
            trial=trial,
            stage=stage,
            identity=selection.selected_identity,
        )


class DataAgentArtifactSourceAdapter:
    """Fetch exact N3/N4 identities through standard Data Agent clients."""

    def __init__(
        self,
        *,
        clients: Mapping[str, HttpDataAgentClient],
        request_factory: BoundDataAgentAccessRequestFactory,
        allowed_media_types: Mapping[str, Sequence[str]],
    ) -> None:
        _require(set(clients) == {"N3", "N4"}, "clients must bind N3 and N4")
        self._clients = dict(clients)
        self._requests = request_factory
        normalized: dict[str, tuple[str, ...]] = {}
        for representation, values in allowed_media_types.items():
            media = tuple(sorted(set(values)))
            _require(
                bool(media)
                and all(isinstance(value, str) and "/" in value for value in media),
                "artifact media type allowlist is invalid",
            )
            normalized[_identifier(representation, "representation ID")] = media
        self._media = normalized

    @property
    def n3_range_fetcher(self) -> HttpDataAgentClient:
        return self._clients["N3"]

    def fetch_full(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        identity: ArtifactIdentity,
        upstream_values: Sequence[Any],
    ) -> ArtifactAccess:
        del upstream_values
        nodes = stage.get("logical_node_ids")
        _require(nodes in [["N3"], ["N4"]], "artifact source is not N3 or N4")
        node = nodes[0]
        request = self._requests.build_for_source(
            source_node_id=node,
            run_id=run_id,
            trial=trial,
            stage=stage,
            identity=identity,
        )
        client = self._clients[node]
        if identity.representation_id == "multimodal_digest":
            result = client.access(request)
            payload = self._inline_digest(result, identity)
            service_ms = result.service_latency_ms
            bytes_read = result.bytes_read
        else:
            allowed = self._media.get(identity.representation_id)
            _require(
                allowed is not None,
                "artifact representation has no media allowlist",
            )
            artifact = client.fetch_binary_artifact(
                request,
                allowed_media_types=allowed,
            )
            self._validate_binary(artifact, identity, request)
            payload = artifact.data
            service_ms = (
                artifact.service_latency_ms
                if artifact.service_latency_ms is not None
                else 0.0
            )
            bytes_read = artifact.size_bytes
        _require(
            len(payload) == identity.artifact_size_bytes
            and _sha256(payload) == identity.artifact_sha256,
            "Data Agent returned bytes outside the frozen artifact identity",
        )
        return ArtifactAccess(
            source_identity=identity,
            payload=payload,
            telemetry=AdapterTelemetry(
                service_time_ms=_number(service_ms, "Data Agent service time"),
                bytes_read=_integer(bytes_read, "Data Agent bytes read"),
            ),
        )

    def fetch_selected(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        source_identity: ArtifactIdentity,
        selection: ExactTemporalFrameSelection,
    ) -> ArtifactAccess:
        """Fetch a frozen source-side projection without downloading the MP4."""

        selected = selection.selected_identity
        request = self._requests.build_selected_request(
            run_id=run_id,
            trial=trial,
            stage=stage,
            source_identity=source_identity,
            selection=selection,
        )
        allowed = self._media.get(selected.representation_id)
        _require(
            allowed is not None,
            "selected N3 representation has no media allowlist",
        )
        artifact = self._clients["N3"].fetch_binary_artifact(
            request,
            allowed_media_types=allowed,
        )
        self._validate_binary(artifact, selected, request)
        _require(
            artifact.size_bytes == selection.selected_artifact_size_bytes
            and artifact.sha256 == selection.selected_artifact_sha256,
            "N3 projection bytes differ from the N2 descriptor",
        )
        return ArtifactAccess(
            source_identity=source_identity,
            payload=artifact.data,
            segment=selection,
            telemetry=AdapterTelemetry(
                service_time_ms=(
                    artifact.service_latency_ms
                    if artifact.service_latency_ms is not None
                    else 0.0
                ),
                bytes_read=artifact.size_bytes,
            ),
        )

    @staticmethod
    def _inline_digest(
        result: DataAgentAccessResult,
        identity: ArtifactIdentity,
    ) -> bytes:
        _require(result.access_id, "Data Agent result has no access identity")
        _require(
            result.object_id == identity.object_id
            and result.object_catalog_version == identity.object_catalog_version,
            "Data Agent inline result changed object identity",
        )
        payload = result.payload
        _require(
            payload.kind == "inline_text"
            and payload.media_type.startswith("text/")
            and isinstance(payload.value, str),
            "multimodal digest is not an inline text artifact",
        )
        raw = payload.value.encode("utf-8")
        _require(
            payload.sha256 == identity.artifact_sha256,
            "Data Agent inline digest commitment changed",
        )
        return raw

    @staticmethod
    def _validate_binary(
        artifact: DataAgentBinaryArtifact,
        identity: ArtifactIdentity,
        request: DataAgentAccessRequest,
    ) -> None:
        _require(
            artifact.access_id == request.access_id
            and artifact.object_id == identity.object_id
            and artifact.object_catalog_version == identity.object_catalog_version
            and artifact.size_bytes == identity.artifact_size_bytes
            and artifact.sha256 == identity.artifact_sha256,
            "Data Agent binary metadata changed the frozen artifact identity",
        )


def _value_payload(value: Any) -> tuple[str, int]:
    while isinstance(value, TransferResult):
        value = value.value
    if isinstance(value, BranchJoinResult):
        value = value.artifact
    if isinstance(value, CacheInsertResult):
        value = value.artifact
    if isinstance(value, ArtifactAccess):
        return value.payload_sha256, len(value.payload)
    if isinstance(value, PreparedSemanticInput):
        return value.payload_sha256, len(value.payload)
    if isinstance(value, IndexSelection):
        # The N2 -> N3 handoff carries the selection decision, not the
        # selected video. Bind every field that makes the selection
        # meaningful -- including range_descriptor_sha256 as an explicit
        # null when the selection is whole-object -- and report the size of
        # that canonical metadata rather than any artifact or estimated
        # network payload. This mirrors _value_commitment()'s index-selection
        # shape so both sides of the transfer agree.
        selection = _canonical({
            "domain": "pathfinder.index-selection-handoff/v1",
            "selected_object_id": value.selected_object_id,
            "index_result_sha256": value.index_result_sha256,
            "range_descriptor_sha256": (
                None if value.segment is None
                else value.segment.descriptor_sha256
            ),
        })
        return _sha256(selection), len(selection)
    if isinstance(value, SemanticInferenceResult):
        # The N6 -> N1 return-answer stage transports the inference result.
        # result_sha256 is its existing commitment, and the bytes actually
        # carried are the UTF-8 final answer, so bytes_sent must match that
        # length rather than the size of any container object.
        return (
            value.result_sha256,
            len(value.final_answer.encode("utf-8")),
        )
    raise FullFlowRouteAdapterError(
        f"transport cannot bind {type(value).__name__}"
    )


class InProcessByteTransferAdapter:
    """Preserve bytes across a coordinator handoff without simulating a link."""

    def __init__(self, clock_ns: Callable[[], int] = time.perf_counter_ns) -> None:
        self._clock_ns = clock_ns

    def transfer(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        value: Any,
    ) -> TransferResult:
        started = self._clock_ns()
        payload_sha, size = _value_payload(value)
        transfer_sha = _sha256(_canonical({
            "domain": "pathfinder.in-process-byte-handoff/v1",
            "run_id": run_id,
            "trial_key": trial.get("trial_key"),
            "stage_key": stage.get("stage_key"),
            "payload_sha256": payload_sha,
            "payload_size_bytes": size,
            "network_measurement_claimed": False,
        }))
        finished = self._clock_ns()
        return TransferResult(
            value=value,
            transfer_sha256=transfer_sha,
            telemetry=AdapterTelemetry(
                service_time_ms=_elapsed_ms(started, finished),
                bytes_sent=size,
            ),
        )

class ApplicationShapedByteTransferAdapter:
    """Apply one explicit executor-link envelope to logical byte handoffs.

    The route still uses real HTTP for service calls.  This adapter adds only
    the frozen application-level bandwidth/RTT floor for handoffs that touch
    the selected N7/N8 executor.  It is therefore controlled shaping, not a
    claim that the underlying UpCloud network was measured at this rate.
    """

    def __init__(
        self,
        *,
        profile_id: str,
        executor_node_id: str,
        bandwidth_bytes_per_second: float,
        round_trip_time_ms: float,
        clock_ns: Callable[[], int] = time.perf_counter_ns,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self._profile_id = _identifier(profile_id, "shaping profile ID")
        _require(
            executor_node_id in {"N7", "N8"},
            "shaping executor must be N7 or N8",
        )
        self._executor = executor_node_id
        self._bandwidth = _number(
            bandwidth_bytes_per_second,
            "shaping bandwidth",
        )
        _require(self._bandwidth > 0.0, "shaping bandwidth must be positive")
        self._rtt_ms = _number(round_trip_time_ms, "shaping RTT")
        self._clock_ns = clock_ns
        self._sleeper = sleeper

    def transfer(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        value: Any,
    ) -> TransferResult:
        started = self._clock_ns()
        payload_sha, size = _value_payload(value)
        logical_nodes = stage.get("logical_node_ids")
        _require(
            isinstance(logical_nodes, list)
            and all(isinstance(node, str) for node in logical_nodes),
            "transfer stage logical nodes are invalid",
        )
        shaped = self._executor in logical_nodes and len(logical_nodes) > 1
        target_ms = (
            self._rtt_ms + (size / self._bandwidth) * 1000.0
            if shaped
            else 0.0
        )
        if target_ms > 0.0:
            self._sleeper(target_ms / 1000.0)
        transfer_sha = _sha256(_canonical({
            "domain": "pathfinder.application-shaped-byte-handoff/v1",
            "run_id": run_id,
            "trial_key": trial.get("trial_key"),
            "stage_key": stage.get("stage_key"),
            "payload_sha256": payload_sha,
            "payload_size_bytes": size,
            "application_shaping_profile_id": self._profile_id,
            "bandwidth_bytes_per_second": self._bandwidth,
            "round_trip_time_ms": self._rtt_ms,
            "configured_application_shaping_target_ms": target_ms,
            "network_measurement_claimed": False,
        }))
        finished = self._clock_ns()
        return TransferResult(
            value=value,
            transfer_sha256=transfer_sha,
            telemetry=AdapterTelemetry(
                service_time_ms=_elapsed_ms(started, finished),
                bytes_sent=size,
            ),
            application_shaping_profile_id=self._profile_id,
            configured_application_shaping_target_ms=target_ms,
        )


class CacheLineageStore(Protocol):
    def resolve(
        self,
        *,
        run_id: str,
        node_id: str,
        cache_id: str,
        runtime_epoch: str,
        identity: ArtifactIdentity,
    ) -> str | None: ...

    def record(
        self,
        *,
        run_id: str,
        node_id: str,
        cache_id: str,
        runtime_epoch: str,
        identity: ArtifactIdentity,
        source_trial_key: str,
    ) -> None: ...


class SQLiteCacheLineageStore:
    """Durable cache-insertion lineage without endpoint or credential data."""

    def __init__(self, database: str | Path) -> None:
        self._path = Path(database).resolve()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS cache_lineage (
                    run_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    cache_id TEXT NOT NULL,
                    runtime_epoch TEXT NOT NULL,
                    object_id TEXT NOT NULL,
                    representation_id TEXT NOT NULL,
                    artifact_sha256 TEXT NOT NULL,
                    artifact_size_bytes INTEGER NOT NULL,
                    object_catalog_version TEXT NOT NULL,
                    source_trial_key TEXT NOT NULL,
                    PRIMARY KEY (
                        run_id, node_id, cache_id, runtime_epoch,
                        object_id, representation_id
                    )
                )
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        return connection

    @staticmethod
    def _key(
        run_id: str,
        node_id: str,
        cache_id: str,
        runtime_epoch: str,
        identity: ArtifactIdentity,
    ) -> tuple[Any, ...]:
        _text(run_id, "lineage run_id", maximum=256)
        _require(node_id in {"N7", "N8"}, "lineage node must be N7 or N8")
        _identifier(cache_id, "lineage cache_id")
        _require(
            _RUNTIME_EPOCH.fullmatch(runtime_epoch) is not None,
            "lineage runtime_epoch is invalid",
        )
        return (
            run_id,
            node_id,
            cache_id,
            runtime_epoch,
            identity.object_id,
            identity.representation_id,
        )

    def resolve(
        self,
        *,
        run_id: str,
        node_id: str,
        cache_id: str,
        runtime_epoch: str,
        identity: ArtifactIdentity,
    ) -> str | None:
        key = self._key(run_id, node_id, cache_id, runtime_epoch, identity)
        with self._lock, closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT * FROM cache_lineage
                WHERE run_id = ? AND node_id = ? AND cache_id = ?
                  AND runtime_epoch = ? AND object_id = ?
                  AND representation_id = ?
                """,
                key,
            ).fetchone()
        if row is None:
            return None
        _require(
            row["artifact_sha256"] == identity.artifact_sha256
            and row["artifact_size_bytes"] == identity.artifact_size_bytes
            and row["object_catalog_version"] == identity.object_catalog_version,
            "cache lineage artifact identity changed",
        )
        return str(row["source_trial_key"])

    def record(
        self,
        *,
        run_id: str,
        node_id: str,
        cache_id: str,
        runtime_epoch: str,
        identity: ArtifactIdentity,
        source_trial_key: str,
    ) -> None:
        key = self._key(run_id, node_id, cache_id, runtime_epoch, identity)
        _text(source_trial_key, "source_trial_key", maximum=2048)
        values = key + (
            identity.artifact_sha256,
            identity.artifact_size_bytes,
            identity.object_catalog_version,
            source_trial_key,
        )
        with self._lock, closing(self._connect()) as connection:
            try:
                connection.execute(
                    """
                    INSERT INTO cache_lineage (
                        run_id, node_id, cache_id, runtime_epoch,
                        object_id, representation_id, artifact_sha256,
                        artifact_size_bytes, object_catalog_version,
                        source_trial_key
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    values,
                )
                connection.commit()
            except sqlite3.IntegrityError:
                connection.rollback()
                existing = self.resolve(
                    run_id=run_id,
                    node_id=node_id,
                    cache_id=cache_id,
                    runtime_epoch=runtime_epoch,
                    identity=identity,
                )
                _require(
                    existing == source_trial_key,
                    "cache lineage key was reused by another trial",
                )


class HttpArtifactCacheRouteAdapter:
    """N7/N8 persistent-cache adapter over the existing HTTP client."""

    def __init__(
        self,
        *,
        clients: Mapping[str, HttpFullFlowArtifactCacheClient],
        runtime_epoch_probes: Mapping[str, Callable[[], Mapping[str, Any]]],
        lineage: CacheLineageStore,
        clock_ns: Callable[[], int] = time.perf_counter_ns,
    ) -> None:
        _require(set(clients) == {"N7", "N8"}, "cache clients must bind N7/N8")
        _require(
            set(runtime_epoch_probes) == {"N7", "N8"},
            "cache epoch probes must bind N7/N8",
        )
        self._clients = dict(clients)
        self._probes = dict(runtime_epoch_probes)
        self._lineage = lineage
        self._clock_ns = clock_ns
        self._lock = threading.RLock()
        self._hits: dict[str, CachedArtifact] = {}

    def _identity(self, node: str) -> tuple[str, str]:
        health = self._clients[node].health()
        _require(
            health.get("status") == "ok"
            and health.get("node_id") == node
            and health.get("credentials_recorded") is False,
            "cache service health identity is invalid",
        )
        cache_id = _identifier(health.get("cache_id"), "cache_id")
        node_health = dict(self._probes[node]())
        _require(
            node_health.get("status") == "ok"
            and node_health.get("node_id") == node
            and node_health.get("credentials_recorded") is False,
            "container-node cache epoch probe is invalid",
        )
        epoch = _text(node_health.get("runtime_epoch"), "runtime_epoch")
        _require(_RUNTIME_EPOCH.fullmatch(epoch) is not None, "runtime epoch invalid")
        return cache_id, epoch

    @staticmethod
    def _node(trial: Mapping[str, Any], stage: Mapping[str, Any]) -> str:
        node = trial.get("executor_node_id")
        _require(node in {"N7", "N8"}, "cache executor is invalid")
        _require(stage.get("logical_node_ids") == [node], "cache stage node changed")
        return str(node)

    def lookup(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        identity: ArtifactIdentity,
    ) -> CacheLookupResult:
        node = self._node(trial, stage)
        cache_id, epoch_before = self._identity(node)
        started = self._clock_ns()
        artifact = self._clients[node].get(
            object_id=identity.object_id,
            representation_id=identity.representation_id,
            expected_sha256=identity.artifact_sha256,
        )
        finished = self._clock_ns()
        cache_after, epoch_after = self._identity(node)
        _require(
            (cache_id, epoch_before) == (cache_after, epoch_after),
            "cache runtime identity changed during lookup",
        )
        source_trial = self._lineage.resolve(
            run_id=run_id,
            node_id=node,
            cache_id=cache_id,
            runtime_epoch=epoch_before,
            identity=identity,
        )
        branch = "hit" if artifact is not None else "miss"
        _require(
            (branch == "hit" and source_trial is not None)
            or (branch == "miss" and source_trial is None),
            "cache bytes and durable insertion lineage disagree",
        )
        lookup_sha = _sha256(_canonical({
            "domain": "pathfinder.http-cache-lookup/v1",
            "run_id": run_id,
            "trial_key": trial.get("trial_key"),
            "stage_key": stage.get("stage_key"),
            "node_id": node,
            "cache_id": cache_id,
            "runtime_epoch": epoch_before,
            "artifact_identity_sha256": identity.commitment,
            "branch": branch,
            "source_insert_trial_key": source_trial,
        }))
        if artifact is not None:
            _require(
                artifact.content_sha256 == identity.artifact_sha256
                and artifact.size_bytes == identity.artifact_size_bytes,
                "cache lookup returned a different artifact identity",
            )
            with self._lock:
                self._hits[lookup_sha] = artifact
        return CacheLookupResult(
            node_id=node,
            cache_id=cache_id,
            branch=branch,
            runtime_epoch=epoch_before,
            lookup_sha256=lookup_sha,
            source_insert_trial_key=source_trial,
            telemetry=AdapterTelemetry(
                service_time_ms=_elapsed_ms(started, finished),
                bytes_read=0 if artifact is None else artifact.size_bytes,
            ),
        )

    def read(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        identity: ArtifactIdentity,
        lookup: CacheLookupResult,
    ) -> ArtifactAccess:
        del run_id, trial, stage
        with self._lock:
            artifact = self._hits.pop(lookup.lookup_sha256, None)
        _require(artifact is not None, "cache hit payload is no longer available")
        _require(
            artifact.node_id == lookup.node_id
            and artifact.cache_id == lookup.cache_id
            and artifact.object_id == identity.object_id
            and artifact.representation_id == identity.representation_id
            and artifact.content_sha256 == identity.artifact_sha256
            and artifact.size_bytes == identity.artifact_size_bytes,
            "cache read changed the frozen artifact identity",
        )
        return ArtifactAccess(
            source_identity=identity,
            payload=artifact.payload,
            telemetry=AdapterTelemetry(bytes_read=artifact.size_bytes),
        )

    def insert(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        stage: Mapping[str, Any],
        identity: ArtifactIdentity,
        lookup: CacheLookupResult,
        artifact: ArtifactAccess,
    ) -> CacheInsertResult:
        node = self._node(trial, stage)
        _require(
            lookup.node_id == node
            and lookup.branch == "miss"
            and artifact.source_identity == identity
            and artifact.segment is None
            and artifact.payload_sha256 == identity.artifact_sha256
            and len(artifact.payload) == identity.artifact_size_bytes,
            "cache insert input differs from the miss artifact",
        )
        cache_id, epoch_before = self._identity(node)
        _require(
            (cache_id, epoch_before) == (lookup.cache_id, lookup.runtime_epoch),
            "cache runtime identity changed after the miss",
        )
        request_id = _sha256(_canonical({
            "domain": "pathfinder.semantic-route-cache-insert/v1",
            "run_id": run_id,
            "trial_key": trial.get("trial_key"),
            "stage_key": stage.get("stage_key"),
            "artifact_identity_sha256": identity.commitment,
        }))
        started = self._clock_ns()
        result = self._clients[node].put(
            request_id=request_id,
            object_id=identity.object_id,
            representation_id=identity.representation_id,
            payload=artifact.payload,
            expected_sha256=identity.artifact_sha256,
        )
        finished = self._clock_ns()
        cache_after, epoch_after = self._identity(node)
        _require(
            (cache_id, epoch_before) == (cache_after, epoch_after),
            "cache runtime identity changed during insert",
        )
        insert_sha = _sha256(_canonical({
            "domain": "pathfinder.http-cache-insert/v1",
            "request_id": request_id,
            "cache_id": cache_id,
            "runtime_epoch": epoch_before,
            "artifact_identity_sha256": identity.commitment,
            "service_result_sha256": _sha256(_canonical(result)),
        }))
        source_trial = _text(
            trial.get("trial_key"), "source trial key", maximum=2048
        )
        self._lineage.record(
            run_id=run_id,
            node_id=node,
            cache_id=cache_id,
            runtime_epoch=epoch_before,
            identity=identity,
            source_trial_key=source_trial,
        )
        return CacheInsertResult(
            node_id=node,
            cache_id=cache_id,
            runtime_epoch=epoch_before,
            insert_sha256=insert_sha,
            artifact=artifact,
            telemetry=AdapterTelemetry(
                service_time_ms=_elapsed_ms(started, finished),
                bytes_sent=len(artifact.payload),
            ),
        )


class N1ScoreEvidenceVerifier(Protocol):
    def verify(
        self,
        *,
        request: Mapping[str, Any],
        result: Mapping[str, Any],
    ) -> Mapping[str, Any]: ...


@dataclass(frozen=True)
class N1PackageScoreEvidenceVerifier:
    """Privileged verifier for an N1-local process or isolated sidecar.

    It must not be instantiated in an N7/N8 trust domain in a real
    deployment: the hidden package and evidence secret belong to N1.  The
    current repository has no remote verification endpoint, so this class is
    the exact local/in-process bridge and the verifier Protocol is the future
    service seam.
    """

    package_dir: Path
    evidence_secret: bytes = field(repr=False)

    def verify(
        self,
        *,
        request: Mapping[str, Any],
        result: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        return verify_n1_score_result(
            package_dir=self.package_dir,
            request=request,
            result=result,
            evidence_secret=self.evidence_secret,
        )


class VerifiedN1HTTPScoringAdapter:
    """Score via N1 HTTP, then require independent HMAC verification."""

    def __init__(
        self,
        *,
        client: N1OracleHTTPClient,
        verifier: N1ScoreEvidenceVerifier,
        clock_ns: Callable[[], int] = time.perf_counter_ns,
    ) -> None:
        self._client = client
        self._verifier = verifier
        self._clock_ns = clock_ns

    def score_once_and_verify(
        self,
        request: Mapping[str, Any],
    ) -> AuthenticatedN1Score:
        before = self._client.health()
        started = self._clock_ns()
        result = self._client.score(request)
        finished = self._clock_ns()
        after = self._client.health()
        _require(before == after, "N1 public identity changed during scoring")
        verified = dict(self._verifier.verify(request=request, result=result))
        _require(
            verified.get("status") == "VERIFIED"
            and verified.get("oracle_id") == result.get("oracle_id")
            and verified.get("score_request_id")
            == result.get("score_request_id")
            and verified.get("request_sha256") == result.get("request_sha256")
            and verified.get("prediction_sha256")
            == result.get("prediction_sha256")
            and verified.get("correct") is result.get("correct")
            and verified.get("score") == result.get("score")
            and verified.get("hidden_answer_returned") is False,
            "N1 verifier did not authenticate the exact score result",
        )
        verification_sha = _sha256(_canonical({
            "domain": "pathfinder.authenticated-n1-score-verification/v1",
            "request_sha256": result.get("request_sha256"),
            "result_content_sha256": result.get("result_content_sha256"),
            "score_evidence_hmac_sha256": result.get(
                "score_evidence_hmac_sha256"
            ),
        }))
        return AuthenticatedN1Score(
            result=dict(result),
            authentication_verified=True,
            verification_sha256=verification_sha,
            telemetry=AdapterTelemetry(
                service_time_ms=_elapsed_ms(started, finished),
                bytes_read=len(_canonical(result)),
                bytes_sent=len(_canonical(request)),
            ),
        )


class FrozenProvisioningReferenceAdapter:
    """Resolve only pre-verified N5-to-N4 publication references."""

    def __init__(self, references: Sequence[ProvisioningReference]) -> None:
        rows: dict[str, ProvisioningReference] = {}
        for value in references:
            _require(
                isinstance(value, ProvisioningReference),
                "provisioning reference has the wrong type",
            )
            _require(
                value.chain_id not in rows,
                "provisioning reference repeats a chain ID",
            )
            rows[value.chain_id] = value
        self._rows = rows

    def resolve(
        self,
        *,
        run_id: str,
        trial: Mapping[str, Any],
        chain_id: str,
        logical_object_id: str,
        identity: ArtifactIdentity,
    ) -> ProvisioningReference:
        del run_id, trial
        value = self._rows.get(chain_id)
        _require(value is not None, "N5/N4 provisioning reference is missing")
        _require(
            value.logical_object_id == logical_object_id
            and value.artifact_identity == identity
            and value.available is True,
            "N5/N4 provisioning reference changed the artifact identity",
        )
        return value


class _NoRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: Any,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        del req, fp, code, msg, headers, newurl
        return None


class HttpContainerNodeSemanticClient:
    """Bounded proxy-free N6 health and semantic request client."""

    def __init__(
        self,
        *,
        base_url: str,
        bearer_token: str,
        timeout_seconds: float = 300.0,
        max_request_bytes: int = 32 * 1024 * 1024,
        max_response_bytes: int = _MAX_JSON_RESPONSE_BYTES,
        simulator_private_http_hosts: Sequence[str] = (),
    ) -> None:
        parsed = urllib.parse.urlsplit(base_url)
        _require(
            parsed.scheme in {"http", "https"}
            and parsed.hostname is not None
            and parsed.username is None
            and parsed.password is None
            and parsed.path in {"", "/"}
            and not parsed.query
            and not parsed.fragment,
            "N6 base_url must be an HTTP(S) origin without credentials",
        )
        private = tuple(sorted(set(simulator_private_http_hosts)))
        _require(
            all(_PRIVATE_SIMULATOR_HOST.fullmatch(value) for value in private),
            "N6 simulator private host allowlist is invalid",
        )
        loopback = parsed.hostname in {"127.0.0.1", "localhost", "::1"}
        _require(
            parsed.scheme == "https" or loopback or parsed.hostname in private,
            "plain HTTP is allowed only for loopback or a simulator host",
        )
        _require(
            isinstance(bearer_token, str)
            and 16 <= len(bearer_token.encode("utf-8")) <= 8192,
            "N6 bearer token length is invalid",
        )
        self._base = base_url.rstrip("/")
        self._authorization = "Bearer " + bearer_token
        self._timeout = _number(timeout_seconds, "N6 timeout")
        _require(self._timeout > 0.0, "N6 timeout must be positive")
        self._max_request = _integer(
            max_request_bytes, "N6 max request bytes", minimum=1
        )
        self._max_response = _integer(
            max_response_bytes, "N6 max response bytes", minimum=1
        )
        handlers: list[Any] = [_NoRedirects()]
        if loopback or parsed.hostname in private:
            handlers.insert(0, urllib.request.ProxyHandler({}))
        self._opener = urllib.request.build_opener(*handlers).open

    def _request(
        self,
        path: str,
        *,
        method: str,
        value: Mapping[str, Any] | None = None,
        authenticated: bool = False,
    ) -> dict[str, Any]:
        body = None if value is None else _canonical(value)
        if body is not None:
            _require(len(body) <= self._max_request, "N6 request is too large")
        headers = {
            "Accept": "application/json",
            "User-Agent": "pathfinder-full-flow-route-adapter/1",
        }
        if body is not None:
            headers["Content-Type"] = "application/json"
        if authenticated:
            headers["Authorization"] = self._authorization
        request = urllib.request.Request(
            self._base + path,
            data=body,
            headers=headers,
            method=method,
        )
        try:
            with self._opener(request, timeout=self._timeout) as response:
                _require(
                    response.status == 200
                    and response.headers.get_content_type() == "application/json",
                    "N6 returned an invalid HTTP response",
                )
                raw = response.read(self._max_response + 1)
        except urllib.error.HTTPError as exc:
            raise FullFlowRouteAdapterError(
                f"N6 returned HTTP {exc.code}"
            ) from exc
        except (urllib.error.URLError, OSError) as exc:
            raise FullFlowRouteAdapterError("N6 request failed") from exc
        _require(len(raw) <= self._max_response, "N6 response is too large")
        try:
            result = json.loads(raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise FullFlowRouteAdapterError("N6 response is not JSON") from exc
        _require(isinstance(result, dict), "N6 response is not an object")
        return result

    def health(self) -> dict[str, Any]:
        value = self._request("/healthz", method="GET")
        _require(
            value.get("status") == "ok"
            and value.get("node_id") == "N6"
            and value.get("credentials_recorded") is False,
            "N6 health identity is invalid",
        )
        return value

    def execute(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._request(
            "/v1/semantic/chat-completions",
            method="POST",
            value=request,
            authenticated=True,
        )


def build_http_semantic_route_adapters(
    *,
    index: BoundIndexQueryAdapter,
    artifacts: DataAgentArtifactSourceAdapter,
    request_factory: BoundDataAgentAccessRequestFactory,
    cache: HttpArtifactCacheRouteAdapter,
    model_input: N6ModelInputAdapter,
    semantic_client: HttpContainerNodeSemanticClient,
    semantic_model: str,
    scorer: VerifiedN1HTTPScoringAdapter,
    provisioning: FrozenProvisioningReferenceAdapter,
    control: InProcessTrialControlAdapter | None = None,
    transport: (
        InProcessByteTransferAdapter
        | ApplicationShapedByteTransferAdapter
        | None
    ) = None,
    raw_range_allowed_media_types: Sequence[str] = ("video/mp4",),
) -> SemanticRouteAdapters:
    """Assemble the production-facing adapter bundle.

    This construction function intentionally accepts already-built clients.
    Consequently endpoint URLs, tokens, and HMAC material never enter a
    frozen plan or returned evidence object.
    """

    inference = BoundN6SemanticInferenceAdapter(
        executor=semantic_client,
        health_probe=semantic_client.health,
        expected_model=semantic_model,
    )
    return SemanticRouteAdapters(
        control=control or InProcessTrialControlAdapter(),
        index=index,
        artifacts=artifacts,
        range_fetcher=artifacts.n3_range_fetcher,
        range_request_factory=request_factory,
        transport=transport or InProcessByteTransferAdapter(),
        cache=cache,
        model_input=model_input,
        semantic=inference,
        scorer=scorer,
        provisioning=provisioning,
        raw_range_allowed_media_types=tuple(raw_range_allowed_media_types),
    )


__all__ = [
    "ApplicationShapedByteTransferAdapter",
    "BoundDataAgentAccessRequestFactory",
    "BoundIndexQueryAdapter",
    "CacheLineageStore",
    "DataAgentArtifactSourceAdapter",
    "DataAgentPlanIdResolver",
    "ExactRangeResolver",
    "FrozenIndexQueryPlan",
    "FrozenIndexQueryPlanCatalog",
    "FrozenDataAgentPlanIdCatalog",
    "FrozenProvisioningReferenceAdapter",
    "FullFlowRouteAdapterError",
    "HttpArtifactCacheRouteAdapter",
    "HttpContainerNodeSemanticClient",
    "InProcessByteTransferAdapter",
    "InProcessTrialControlAdapter",
    "IndexQueryPlanResolver",
    "LocalIndexService",
    "N1PackageScoreEvidenceVerifier",
    "N1ScoreEvidenceVerifier",
    "ROUTE_ADAPTERS_VERSION",
    "SQLiteCacheLineageStore",
    "StaticDataAgentPlanIdResolver",
    "VerifiedN1HTTPScoringAdapter",
    "build_http_semantic_route_adapters",
]
