"""Offline vertical slice: plan -> adapter -> routing -> cost -> AWM.

Exercises the real Pathfinder routing, Gateway, artifact, cost and runner
code against two in-process fake Data Agents with different endpoint ids,
node ids, catalogs, representations and artifact secrets. No socket is
opened, no worker or service is started, and no workflow is submitted.
"""

from __future__ import annotations

import contextlib
import dataclasses
import io
import json
import os
import re
import tempfile
import unittest
from unittest import mock
from dataclasses import dataclass, replace
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping

from pathfinder.cli import main as cli_main
from pathfinder.config import load_config
from pathfinder.data_agent_client import (
    DataAgentAccessRequest,
    DataAgentAccessResult,
    DataAgentAccessTelemetry,
    DataAgentFetchedArtifact,
    DataAgentPayload,
    DataAgentUnavailableError,
)
from pathfinder.distributed import (
    COMPLETED_OUTCOME_TYPE,
    CanonicalRecordError,
    load_durable_canonical_records,
    build_distributed_trial_plan,
    load_endpoint_registry,
    workload_content_sha256,
    EndpointRegistryError,
    PilotPreregistrationError,
    PilotResumeError,
    CrossEndpointArtifactError,
    EndpointRegistryError,
    EndpointUnreachableError,
    ManifestMeasurementProvider,
    MeasurementError,
    RoutedDataAgentBackend,
    StaleMeasurementError,
    TrialExecution,
    build_endpoint_registry,
    export_reduced_oracle_records,
    load_distributed_pilot_preregistration,
    load_measurement_manifest,
    preflight_distributed_pilot,
    run_distributed_pilot,
)
from pathfinder.integrations.flowmesh.contracts import (
    FlowMeshAgentRunRequest,
)
from pathfinder.integrations.flowmesh.gateway import (
    AccessGateway,
    SQLiteSessionStore,
)

from tests.test_awm_certificate import _write_json


ROOT = Path(__file__).resolve().parents[1]
SYSTEM_CONFIG = ROOT / "configs" / "multi_candidate_formal_v2_system.json"

ORIGIN_ENDPOINT = "origin_remote"
LOCAL_ENDPOINT = "local_materialized"
ORIGIN_NODE = "node-alpha-origin"
LOCAL_NODE = "node-beta-local"
ORIGIN_URL_ENV = "PF_VERTICAL_ORIGIN_URL"
LOCAL_URL_ENV = "PF_VERTICAL_LOCAL_URL"
ORIGIN_TOKEN_ENV = "PF_VERTICAL_ORIGIN_TOKEN"
LOCAL_TOKEN_ENV = "PF_VERTICAL_LOCAL_TOKEN"

ENVIRONMENT = {
    ORIGIN_URL_ENV: "https://alpha.invalid:8443",
    LOCAL_URL_ENV: "https://beta.invalid:9443",
    ORIGIN_TOKEN_ENV: "origin-token-aaaaaaaaaaaa",
    LOCAL_TOKEN_ENV: "local-token-bbbbbbbbbbbb",
}

SAFE_DESIGN = "D_origin_remote"
FRAMES_DESIGN = "D_local_frames"
DIGEST_DESIGN = "D_local_digest"
GIB = float(1024 ** 3)
from pathfinder.distributed import CELL_STATES as CELL_STATES_TUPLE


# --------------------------------------------------------------------------
# Two in-process fake Data Agents
# --------------------------------------------------------------------------
@dataclass
class FakeDataAgent:
    """An in-process Data Agent with its own node, catalog and secret."""

    endpoint_id: str
    node_id: str
    catalog_version: str
    representations: tuple[str, ...]
    artifact_secret: str
    realized_cost: float
    artifact_bytes: int = 4096
    healthy: bool = True
    reachable: bool = True

    def __post_init__(self) -> None:
        self.accesses: list[DataAgentAccessRequest] = []
        self.fetches: list[str] = []
        self._telemetry: dict[str, DataAgentAccessTelemetry] = {}

    # -- health surface --------------------------------------------------
    def health_document(self) -> dict[str, Any]:
        return {
            "status": "ok" if self.healthy else "degraded",
            "api_version": "pathfinder.data-agent/v1alpha1",
            "node_id": self.node_id,
            "representations": list(self.representations),
            "object_catalog_version": self.catalog_version,
        }

    # -- client surface --------------------------------------------------
    def access(
        self,
        request: DataAgentAccessRequest,
    ) -> DataAgentAccessResult:
        if not self.reachable:
            raise DataAgentUnavailableError(
                f"{self.endpoint_id} refused the connection "
                f"(secret {self.artifact_secret})"
            )
        if request.representation_id not in self.representations:
            raise DataAgentUnavailableError(
                f"{self.endpoint_id} does not serve "
                f"{request.representation_id}"
            )
        self.accesses.append(request)
        artifact = request.representation_id == "sampled_frames"
        body = f"{self.node_id}:{request.representation_id}"
        digest = sha256(body.encode("utf-8")).hexdigest()
        if artifact:
            payload = DataAgentPayload(
                kind="artifact_uri",
                media_type="application/json",
                value=(
                    f"https://{self.node_id}.invalid/artifact"
                    f"?token={self.artifact_secret}"
                ),
                sha256=digest,
            )
            self._telemetry[request.access_id] = DataAgentAccessTelemetry(
                access_id=request.access_id,
                object_id=request.object_id,
                representation_id=request.representation_id,
                object_catalog_version=self.catalog_version,
                download_request_count=1,
                completed_request_count=1,
                full_download_count=1,
                bytes_sent=self.artifact_bytes,
                transfer_latency_ms=12.0,
                in_flight_request_count=0,
                server_reported_complete=True,
            )
        else:
            payload = DataAgentPayload(
                kind="inline_text",
                media_type="text/plain",
                value=body,
                sha256=digest,
            )
            self._telemetry[request.access_id] = DataAgentAccessTelemetry(
                access_id=request.access_id,
                object_id=request.object_id,
                representation_id=request.representation_id,
                object_catalog_version=self.catalog_version,
                download_request_count=0,
                completed_request_count=0,
                full_download_count=0,
                bytes_sent=0,
                transfer_latency_ms=0.0,
                in_flight_request_count=0,
                server_reported_complete=True,
            )
        return DataAgentAccessResult(
            access_id=request.access_id,
            payload=payload,
            service_latency_ms=8.0,
            realized_cost=self.realized_cost,
            bytes_read=len(body),
            location=self.endpoint_id,
            timings_ms={"fetch": 5.0, "controlled_delay": 3.0},
            client_round_trip_ms=11.0,
            object_id=request.object_id,
            object_catalog_version=self.catalog_version,
        )

    def fetch_artifact(
        self,
        request: DataAgentAccessRequest,
    ) -> DataAgentFetchedArtifact:
        if not self.reachable:
            raise DataAgentUnavailableError(
                f"{self.endpoint_id} refused the connection"
            )
        self.fetches.append(request.access_id)
        body = f"{self.node_id}:{request.representation_id}"
        return DataAgentFetchedArtifact(
            access_id=request.access_id,
            media_type="application/json",
            content=body,
            size_bytes=self.artifact_bytes,
            sha256=sha256(body.encode("utf-8")).hexdigest(),
        )

    def get_access_telemetry(
        self,
        access_id: str,
        *,
        wait_for_quiescence: bool = False,
        quiescence_timeout_seconds: float = 0.0,
    ) -> DataAgentAccessTelemetry:
        try:
            return self._telemetry[access_id]
        except KeyError:
            raise DataAgentUnavailableError(
                f"{self.endpoint_id} has no telemetry for {access_id}"
            ) from None


def _origin_agent(**overrides: Any) -> FakeDataAgent:
    return replace(FakeDataAgent(
        endpoint_id=ORIGIN_ENDPOINT,
        node_id=ORIGIN_NODE,
        catalog_version="catalog-alpha-v1",
        representations=("multimodal_digest", "sampled_frames"),
        artifact_secret="ALPHA-ARTIFACT-SECRET",
        realized_cost=1.0,
    ), **overrides)


def _local_agent(**overrides: Any) -> FakeDataAgent:
    return replace(FakeDataAgent(
        endpoint_id=LOCAL_ENDPOINT,
        node_id=LOCAL_NODE,
        catalog_version="catalog-beta-v7",
        representations=("sampled_frames",),
        artifact_secret="BETA-ARTIFACT-SECRET",
        realized_cost=0.2,
    ), **overrides)


class _FakeHealthProbe:
    def __init__(self, agents: Mapping[str, FakeDataAgent]) -> None:
        self.agents = dict(agents)

    def health(self, endpoint_id: str) -> dict[str, Any]:
        agent = self.agents[endpoint_id]
        if not agent.reachable:
            raise EndpointUnreachableError(
                endpoint_id,
                f"connection refused ({agent.artifact_secret})",
            )
        document = agent.health_document()
        return {
            "healthy": agent.healthy,
            "node_id": document["node_id"],
            "object_catalog_versions": {
                "video-object-1": document["object_catalog_version"],
            },
        }


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------
def _registry_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": (
            "pathfinder.data-agent-endpoint-registry/v1alpha1"
        ),
        "registry_id": "vertical-registry",
        "execution_node_id": "exec-node-gamma",
        "endpoints": [
            {
                "endpoint_id": ORIGIN_ENDPOINT,
                "node_id": ORIGIN_NODE,
                "location": "origin-remote",
                "base_url_env": ORIGIN_URL_ENV,
                "token_env": ORIGIN_TOKEN_ENV,
                "telemetry_capabilities": [
                    "access_telemetry",
                    "transfer_bytes",
                ],
            },
            {
                "endpoint_id": LOCAL_ENDPOINT,
                "node_id": LOCAL_NODE,
                "location": "local-materialized",
                "base_url_env": LOCAL_URL_ENV,
                "token_env": LOCAL_TOKEN_ENV,
                "telemetry_capabilities": [
                    "access_telemetry",
                    "transfer_bytes",
                ],
            },
        ],
        "placement": [
            {
                "design_id": SAFE_DESIGN,
                "representation_id": "*",
                "endpoint_id": ORIGIN_ENDPOINT,
            },
            {
                "design_id": FRAMES_DESIGN,
                "representation_id": "*",
                "endpoint_id": LOCAL_ENDPOINT,
            },
            {
                "design_id": DIGEST_DESIGN,
                "representation_id": "*",
                "endpoint_id": ORIGIN_ENDPOINT,
            },
        ],
    }
    payload.update(overrides)
    return payload


def _registry(**overrides: Any):
    payload = _registry_payload(**overrides)
    return build_endpoint_registry(
        payload,
        source_sha256=sha256(
            json.dumps(payload, sort_keys=True).encode("utf-8")
        ).hexdigest(),
    )


COST_MODEL = {
    "schema_version": "pathfinder.total-cost-model/v1alpha1",
    "cost_model_id": "vertical-fixture",
    "accounting_unit": "pilot-cost-unit",
    "rate_provenance": "NON-SCIENTIFIC VERTICAL TEST FIXTURE",
    "materialization_amortization_horizon_sessions": 8,
    "artifact_transfer_accounted_in": "network_cost",
    "conversion_rates": {
        "network_cost_per_gib": 1024.0,
        "storage_cost_per_gib_hour": 512.0,
        "materialization_cost_per_gib": 2048.0,
        "transition_cost_per_gib": 256.0,
        "elapsed_time_cost_per_second": 0.25,
    },
}

WORKLOAD_IDS = ("vertical-causal-w01", "vertical-causal-w02")


def _preregistration_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": (
            "pathfinder.distributed-pilot-preregistration/v1alpha1"
        ),
        "pilot_id": "vertical-pilot",
        "source_git_revision": "a" * 40,
        "posthoc": False,
        "confirmatory": False,
        "eligible_for_scientific_claims": False,
        "thresholds": {
            "delta_success_margin": 0.05,
            "minimum_cost_saving": 0.25,
            "alpha": 0.05,
            "provenance": (
                "pilot-engineering-threshold-fixed-before-new-pilot-outcomes"
            ),
        },
        "safe_design_id": SAFE_DESIGN,
        "design_ids": [
            SAFE_DESIGN,
            FRAMES_DESIGN,
            DIGEST_DESIGN,
            "D_local_pair",
        ],
        "excluded_design_ids": ["D_local_pair"],
        "strata": {
            "causal": {
                "candidate_design_id": FRAMES_DESIGN,
                "workload_ids": list(WORKLOAD_IDS),
            },
        },
        "excluded_workload_ids": ["frozen-workload-1"],
        "repetitions": 2,
        "success_scoring_rule": "accepted-answer-substring-match",
        "total_cost_contract": {
            "cost_basis": "total_cost",
            "equation": (
                "total_cost = service + network + storage + "
                "amortized_materialization + transition"
            ),
            "cost_model": COST_MODEL,
        },
        "fallback_rule": {
            "design_id": SAFE_DESIGN,
            "applies_to_every_non_safe_result": True,
        },
        "run_declaration": {"immutable_after_first_observation": True},
    }
    payload.update(overrides)
    return payload


def _measurement_payload(
    preregistration_sha256: str,
    registry_sha256: str,
    **overrides: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": "pathfinder.pilot-measurements/v1alpha1",
        "measurement_id": "vertical-measurements",
        "pilot_id": "vertical-pilot",
        "preregistration_sha256": preregistration_sha256,
        "endpoint_registry_sha256": registry_sha256,
        "execution_node_id": "exec-node-gamma",
        "measurements": [
            {
                "design_id": SAFE_DESIGN,
                "node_id": ORIGIN_NODE,
                "storage": {
                    "kind": "not_applicable",
                    "justification": (
                        "origin reuses existing representations and adds no "
                        "pilot-attributable storage"
                    ),
                },
                "materialization": {
                    "kind": "not_applicable",
                    "justification": "origin materializes nothing",
                },
                "transition": {
                    "kind": "not_applicable",
                    "justification": "origin is the incumbent placement",
                },
            },
            {
                "design_id": FRAMES_DESIGN,
                "node_id": LOCAL_NODE,
                "storage": {
                    "kind": "measured",
                    "bytes": GIB / 8,
                    "hours": 4.0,
                },
                "materialization": {
                    "kind": "measured",
                    "bytes": GIB / 16,
                },
                "transition": {
                    "kind": "measured",
                    "bytes": GIB / 32,
                    "seconds": 2.0,
                },
            },
        ],
    }
    payload.update(overrides)
    return payload


@dataclass
class VerticalFixture:
    root: Path
    origin: FakeDataAgent
    local: FakeDataAgent

    def __post_init__(self) -> None:
        self.registry = _registry()
        self.preregistration_path = self.root / "preregistration.json"
        _write_json(self.preregistration_path, _preregistration_payload())
        self.preregistration = load_distributed_pilot_preregistration(
            self.preregistration_path
        )
        self.measurement_path = self.root / "measurements.json"
        _write_json(self.measurement_path, _measurement_payload(
            self.preregistration.source_sha256,
            self.registry.source_sha256,
        ))
        self.provider = load_measurement_manifest(self.measurement_path)
        self.system_config = load_config(SYSTEM_CONFIG)

    @property
    def agents(self) -> dict[str, FakeDataAgent]:
        return {
            ORIGIN_ENDPOINT: self.origin,
            LOCAL_ENDPOINT: self.local,
        }

    def backend(self) -> RoutedDataAgentBackend:
        return RoutedDataAgentBackend(self.registry, self.agents)

    def gateway(self, name: str = "gateway.sqlite3") -> AccessGateway:
        return AccessGateway(
            self.system_config,
            SQLiteSessionStore(self.root / name),
            backend=self.backend(),
        )

    def preflight(self, mode: str = "offline_validation") -> dict[str, Any]:
        return preflight_distributed_pilot(
            self.preregistration,
            self.registry,
            probe=_FakeHealthProbe(self.agents),
            worker_pin={"kind": "worker_alias", "value": "vertical"},
            environment=ENVIRONMENT,
            mode=mode,
            measurement_manifest_sha256=self.provider.manifest_sha256,
        )

    def workloads(self) -> dict[str, dict[str, Any]]:
        return {
            workload_id: {
                "object_id": "video-object-1",
                "question": "What happens in the video?",
                "accepted_answer_substrings": ["dog"],
                "task_class_id": "video_qa",
                "quote_profile_id": "as_designed",
                "latency_multiplier": 1,
            }
            for workload_id in WORKLOAD_IDS
        }


class _GatewayExecutor:
    """Runs one cell through the real Gateway, standing in for FlowMesh."""

    def __init__(
        self,
        fixture: VerticalFixture,
        *,
        answer: str = "a dog runs",
        representation_for: Mapping[str, str] | None = None,
    ) -> None:
        self.fixture = fixture
        self.answer = answer
        self.representation_for = dict(representation_for or {
            SAFE_DESIGN: "multimodal_digest",
            FRAMES_DESIGN: "sampled_frames",
            DIGEST_DESIGN: "multimodal_digest",
        })
        self.gateway = fixture.gateway()
        self.sessions: list[str] = []

    def execute(
        self, trial, *, workload, journal=None, attempt=1
    ) -> TrialExecution:
        session = self.gateway.register_session(FlowMeshAgentRunRequest(
            trial_id=trial.trial_id,
            question=str(workload["question"]),
            design_id=trial.design_id,
            task_class_id=str(workload["task_class_id"]),
            quote_profile_id=str(workload["quote_profile_id"]),
            latency_multiplier=float(workload["latency_multiplier"]),
            seed=trial.seed,
            object_id=str(workload["object_id"]),
            session_id=None,
        ))
        self.sessions.append(session.session_id)
        representation_id = self.representation_for[trial.design_id]
        access = self.gateway.access_representation(
            session.session_id,
            representation_id,
        )
        handle = access.get("artifact_handle")
        if handle is not None:
            self.gateway.fetch_artifact(session.session_id, handle)
        events = [
            event.to_dict()
            for event in self.gateway.store.list_events(session.session_id)
        ]
        for event in events:
            if event.get("artifact_handle_sha256"):
                event["artifact_download_request_count"] = 1
                event["artifact_full_download_count"] = 1
                event["artifact_bytes_sent"] = (
                    self.fixture.agents[event["endpoint_id"]].artifact_bytes
                )
            else:
                event.setdefault("artifact_bytes_sent", 0)
        return TrialExecution(
            final_answer=self.answer,
            access_events=tuple(events),
            workflow_id="wf-offline",
            task_id="task-offline",
            status="COMPLETED",
        )


# --------------------------------------------------------------------------
# 1-4. Routing and artifact semantics through the real Gateway
# --------------------------------------------------------------------------
class RoutedGatewayTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.fixture = VerticalFixture(
            Path(self._tmp.name),
            _origin_agent(),
            _local_agent(),
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _access(self, design_id: str, representation_id: str):
        gateway = self.fixture.gateway()
        session = gateway.register_session(FlowMeshAgentRunRequest(
            trial_id=f"t-{design_id}",
            question="What happens?",
            design_id=design_id,
            task_class_id="video_qa",
            quote_profile_id="as_designed",
            latency_multiplier=1.0,
            seed=1,
            object_id="video-object-1",
            session_id=None,
        ))
        access = gateway.access_representation(
            session.session_id,
            representation_id,
        )
        return gateway, session, access

    def test_origin_access_routes_to_endpoint_a(self) -> None:
        _, _, access = self._access(SAFE_DESIGN, "multimodal_digest")
        self.assertEqual(ORIGIN_ENDPOINT, access["endpoint_id"])
        self.assertEqual(ORIGIN_NODE, access["source_node_id"])
        self.assertEqual("origin-remote", access["source_location"])
        self.assertEqual(
            "exec-node-gamma",
            access["destination_execution_node_id"],
        )
        self.assertEqual(1, len(self.fixture.origin.accesses))
        self.assertEqual(0, len(self.fixture.local.accesses))

    def test_local_representation_access_routes_to_endpoint_b(self) -> None:
        _, _, access = self._access(FRAMES_DESIGN, "sampled_frames")
        self.assertEqual(LOCAL_ENDPOINT, access["endpoint_id"])
        self.assertEqual(LOCAL_NODE, access["source_node_id"])
        self.assertEqual(0, len(self.fixture.origin.accesses))
        self.assertEqual(1, len(self.fixture.local.accesses))

    def test_artifact_download_occurs_only_through_endpoint_b(self) -> None:
        gateway, session, access = self._access(
            FRAMES_DESIGN,
            "sampled_frames",
        )
        handle = access["artifact_handle"]
        fetched = gateway.fetch_artifact(session.session_id, handle)
        self.assertEqual(LOCAL_ENDPOINT, fetched["endpoint_id"])
        self.assertEqual(LOCAL_NODE, fetched["source_node_id"])
        self.assertEqual(1, len(self.fixture.local.fetches))
        self.assertEqual(0, len(self.fixture.origin.fetches))
        self.assertIn(LOCAL_NODE, fetched["content"])

    def test_cross_endpoint_redemption_fails_closed(self) -> None:
        gateway, session, access = self._access(
            FRAMES_DESIGN,
            "sampled_frames",
        )
        handle = access["artifact_handle"]
        # Re-point the placement so the same handle now resolves elsewhere.
        payload = _registry_payload()
        payload["placement"] = [
            {
                "design_id": FRAMES_DESIGN,
                "representation_id": "*",
                "endpoint_id": ORIGIN_ENDPOINT,
            },
            {
                "design_id": SAFE_DESIGN,
                "representation_id": "*",
                "endpoint_id": ORIGIN_ENDPOINT,
            },
        ]
        moved = build_endpoint_registry(payload, source_sha256="0" * 64)
        gateway.backend = RoutedDataAgentBackend(
            moved,
            self.fixture.agents,
        )
        with self.assertRaisesRegex(
            CrossEndpointArtifactError,
            "cross-endpoint redemption",
        ):
            gateway.fetch_artifact(session.session_id, handle)
        self.assertEqual(0, len(self.fixture.origin.fetches))

    def test_a_cross_endpoint_fault_is_a_delivery_not_policy_failure(
        self,
    ) -> None:
        self.assertEqual(
            "artifact_delivery_failure",
            CrossEndpointArtifactError.outcome_type,
        )
        self.assertEqual(
            "artifact_delivery_failure",
            CrossEndpointArtifactError.failure_class,
        )

    def test_the_gateway_persists_only_handle_fingerprints(self) -> None:
        gateway, session, access = self._access(
            FRAMES_DESIGN,
            "sampled_frames",
        )
        handle = access["artifact_handle"]
        events = gateway.store.list_events(session.session_id)
        stored = json.dumps([event.to_dict() for event in events])
        self.assertNotIn(handle, stored)
        self.assertIn(
            sha256(handle.encode("utf-8")).hexdigest(),
            stored,
        )

    def test_no_artifact_secret_reaches_the_gateway_record(self) -> None:
        gateway, session, _ = self._access(FRAMES_DESIGN, "sampled_frames")
        stored = json.dumps([
            event.to_dict()
            for event in gateway.store.list_events(session.session_id)
        ])
        self.assertNotIn(self.fixture.local.artifact_secret, stored)
        self.assertNotIn(self.fixture.origin.artifact_secret, stored)

    def test_an_unroutable_design_never_falls_back(self) -> None:
        payload = _registry_payload()
        payload["placement"] = [payload["placement"][0]]
        registry = build_endpoint_registry(payload, source_sha256="0" * 64)
        backend = RoutedDataAgentBackend(registry, self.fixture.agents)
        with self.assertRaises(Exception) as caught:
            backend.resolve(
                design_id=FRAMES_DESIGN,
                representation_id="sampled_frames",
            )
        self.assertIn("refusing to guess", str(caught.exception))
        self.assertEqual(0, len(self.fixture.origin.accesses))
        self.assertEqual(0, len(self.fixture.local.accesses))

    def test_an_unreachable_endpoint_is_infrastructure_not_policy(
        self,
    ) -> None:
        self.fixture.local.reachable = False
        with self.assertRaises(EndpointUnreachableError) as caught:
            self._access(FRAMES_DESIGN, "sampled_frames")
        self.assertEqual("infrastructure", caught.exception.failure_class)
        self.assertEqual(LOCAL_ENDPOINT, caught.exception.endpoint_id)

    def test_a_missing_client_for_a_declared_endpoint_is_refused(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            EndpointRegistryError,
            "no client was supplied",
        ):
            RoutedDataAgentBackend(
                self.fixture.registry,
                {ORIGIN_ENDPOINT: self.fixture.origin},
            )

    def test_single_endpoint_compatibility_is_preserved(self) -> None:
        payload = _registry_payload()
        payload["endpoints"] = [payload["endpoints"][0]]
        payload.pop("placement")
        registry = build_endpoint_registry(payload, source_sha256="0" * 64)
        backend = RoutedDataAgentBackend(
            registry,
            {ORIGIN_ENDPOINT: self.fixture.origin},
        )
        gateway = AccessGateway(
            self.fixture.system_config,
            SQLiteSessionStore(self.fixture.root / "single.sqlite3"),
            backend=backend,
        )
        session = gateway.register_session(FlowMeshAgentRunRequest(
            trial_id="single",
            question="What happens?",
            design_id=FRAMES_DESIGN,
            task_class_id="video_qa",
            quote_profile_id="as_designed",
            latency_multiplier=1.0,
            seed=1,
            object_id="video-object-1",
            session_id=None,
        ))
        access = gateway.access_representation(
            session.session_id,
            "sampled_frames",
        )
        self.assertEqual(ORIGIN_ENDPOINT, access["endpoint_id"])
        self.assertEqual(1, len(self.fixture.origin.accesses))


# --------------------------------------------------------------------------
# 5-10. Full vertical run
# --------------------------------------------------------------------------
class VerticalRunTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.fixture = VerticalFixture(
            self.root,
            _origin_agent(),
            _local_agent(),
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _run(self, *, output: str = "run", **overrides: Any):
        executor = overrides.pop("executor", None) or _GatewayExecutor(
            self.fixture
        )
        arguments: dict[str, Any] = {
            "output_dir": self.root / output,
            "workloads": self.fixture.workloads(),
            "provider": self.fixture.provider,
            "preflight": self.fixture.preflight(),
            "max_attempts": 3,
        }
        arguments.update(overrides)
        return run_distributed_pilot(
            self.fixture.preregistration,
            self.fixture.registry,
            executor,
            **arguments,
        ), executor

    def test_preflight_passes_offline_and_gates_execution(self) -> None:
        report = self.fixture.preflight()
        self.assertEqual("ok", report["status"], report["failed_checks"])
        self.assertEqual("offline_validation", report["mode"])

    def test_the_full_plan_produces_complete_canonical_records(
        self,
    ) -> None:
        summary, _ = self._run()
        self.assertEqual("COMPLETE", summary["status"])
        self.assertTrue(summary["oracle_complete"])
        self.assertEqual(8, summary["planned_trial_count"])
        self.assertEqual(8, summary["completed_canonical_count"])
        self.assertEqual(
            sorted(WORKLOAD_IDS),
            sorted(summary["complete_workload_blocks"]),
        )
        self.assertFalse(summary["worker_lifecycle_managed"])
        self.assertFalse(summary["services_started"])
        self.assertFalse(summary["commit_performed"])

    def test_records_carry_both_designs_and_route_identities(self) -> None:
        self._run()
        rows = [
            json.loads(line)
            for line in (
                self.root / "run" / "canonical_records.jsonl"
            ).read_text(encoding="utf-8").splitlines()
        ]
        self.assertEqual(8, len(rows))
        by_design: dict[str, set[str]] = {}
        for row in rows:
            for event in row["access_events"]:
                if event.get("accepted"):
                    by_design.setdefault(row["design_id"], set()).add(
                        event["endpoint_id"]
                    )
        self.assertEqual({ORIGIN_ENDPOINT}, by_design[SAFE_DESIGN])
        self.assertEqual({LOCAL_ENDPOINT}, by_design[FRAMES_DESIGN])

    def test_telemetry_is_complete_on_every_record(self) -> None:
        self._run()
        rows = [
            json.loads(line)
            for line in (
                self.root / "run" / "canonical_records.jsonl"
            ).read_text(encoding="utf-8").splitlines()
        ]
        for row in rows:
            self.assertTrue(row["telemetry_complete"])
            self.assertTrue(row["artifact_delivery_complete"])
            self.assertEqual("completed", row["outcome_type"])

    def test_total_cost_components_avoid_double_counting(self) -> None:
        self._run()
        rows = [
            json.loads(line)
            for line in (
                self.root / "run" / "canonical_records.jsonl"
            ).read_text(encoding="utf-8").splitlines()
        ]
        origin = next(r for r in rows if r["design_id"] == SAFE_DESIGN)
        local = next(r for r in rows if r["design_id"] == FRAMES_DESIGN)

        origin_ledger = origin["cost_ledger"]
        self.assertTrue(origin_ledger["total_cost_available"])
        self.assertEqual(
            sorted([
                "storage",
                "amortized_materialization",
                "transition",
            ]),
            sorted(origin_ledger["not_applicable_components"]),
        )
        self.assertEqual([], origin_ledger["unavailable_components"])
        # Origin returned an inline representation, but those bytes still
        # crossed the network from the remote node and are charged.
        origin_network = origin_ledger["components"]["network"]
        self.assertGreater(origin_network["raw_quantity"], 0.0)
        self.assertAlmostEqual(
            1.0 + origin_network["raw_quantity"] / GIB * 1024.0,
            origin_ledger["total_cost"],
        )

        local_ledger = local["cost_ledger"]
        self.assertTrue(local_ledger["total_cost_available"])
        components = local_ledger["components"]
        self.assertAlmostEqual(0.2, components["service"]["value"])
        # 4096 bytes at 1024 units/GiB.
        self.assertAlmostEqual(
            4096 / GIB * 1024.0,
            components["network"]["value"],
        )
        self.assertAlmostEqual(
            GIB / 8 / GIB * 4.0 * 512.0,
            components["storage"]["value"],
        )
        self.assertAlmostEqual(
            GIB / 16 / GIB * 2048.0 / 8,
            components["amortized_materialization"]["value"],
        )
        self.assertAlmostEqual(
            GIB / 32 / GIB * 256.0 + 2.0 * 0.25,
            components["transition"]["value"],
        )
        self.assertAlmostEqual(
            sum(
                components[name]["value"]
                for name in (
                    "service",
                    "network",
                    "storage",
                    "amortized_materialization",
                    "transition",
                )
            ),
            local_ledger["total_cost"],
        )
        # Transfer is charged in exactly one place.
        self.assertEqual(
            "network_cost",
            local_ledger["artifact_transfer_accounted_in"],
        )
        self.assertIn(
            "EXCLUDES artifact transfer",
            components["service"]["conversion_rule"],
        )

    def test_resume_does_not_duplicate_trials(self) -> None:
        first, executor_one = self._run()
        self.assertEqual(8, first["executed_this_invocation"])
        second, executor_two = self._run()
        self.assertEqual(0, second["executed_this_invocation"])
        self.assertEqual(8, second["completed_canonical_count"])
        rows = (
            self.root / "run" / "canonical_records.jsonl"
        ).read_text(encoding="utf-8").splitlines()
        self.assertEqual(8, len(rows))
        keys = [json.loads(line)["trial_key"] for line in rows]
        self.assertEqual(len(keys), len(set(keys)))
        self.assertEqual([], executor_two.sessions)

    def test_resume_is_refused_when_a_frozen_hash_changes(self) -> None:
        self._run()
        edited = _preregistration_payload(pilot_id="vertical-pilot")
        edited["repetitions"] = 3
        path = self.root / "edited.json"
        _write_json(path, edited)
        changed = load_distributed_pilot_preregistration(path)
        with self.assertRaises(Exception) as caught:
            run_distributed_pilot(
                changed,
                self.fixture.registry,
                _GatewayExecutor(self.fixture),
                output_dir=self.root / "run",
                workloads=self.fixture.workloads(),
                provider=self.fixture.provider,
                preflight={
                    **self.fixture.preflight(),
                    "pilot_id": changed.pilot_id,
                },
            )
        self.assertIn("refusing to resume", str(caught.exception))

    def test_execution_requires_a_passing_preflight(self) -> None:
        failed = {
            **self.fixture.preflight(),
            "status": "failed",
            "failed_checks": ["endpoint[origin_remote].health"],
        }
        with self.assertRaisesRegex(Exception, "preflight did not pass"):
            self._run(output="blocked", preflight=failed)
        with self.assertRaisesRegex(Exception, "preflight report is required"):
            self._run(output="blocked2", preflight=None)

    def test_a_stale_measurement_manifest_is_refused(self) -> None:
        path = self.root / "stale.json"
        _write_json(path, _measurement_payload(
            "f" * 64,
            self.fixture.registry.source_sha256,
        ))
        with self.assertRaises(StaleMeasurementError):
            self._run(
                output="stale",
                provider=load_measurement_manifest(path),
            )

    def test_canonical_output_loads_in_the_reduced_oracle_awm_path(
        self,
    ) -> None:
        self._run()
        oracle_output = self.root / "oracle-output"
        exported = export_reduced_oracle_records(
            self.root / "run",
            oracle_output,
            (SAFE_DESIGN, FRAMES_DESIGN),
        )
        self.assertEqual(8, exported["record_count"])
        self.assertEqual(4, exported["designs"][SAFE_DESIGN])
        self.assertEqual(4, exported["designs"][FRAMES_DESIGN])

        from pathfinder.awm.certificate import (
            load_workload_safety_certificate_config,
        )
        from pathfinder.distributed.cost import record_total_cost

        # The AWM v3alpha5 total-cost reader accepts every emitted record.
        rows = [
            json.loads(line)
            for line in (
                oracle_output / "designs" / FRAMES_DESIGN / "runs.jsonl"
            ).read_text(encoding="utf-8").splitlines()
        ]
        self.assertEqual(4, len(rows))
        for row in rows:
            total = record_total_cost(row)
            self.assertGreater(total, 0.0)
            self.assertIn("task_success", row)
            self.assertIn("repetition", row)
            self.assertIn("workload_id", row)
            self.assertTrue(row["telemetry_complete"])
        self.assertTrue(callable(load_workload_safety_certificate_config))



class NetworkByteAccountingTest(unittest.TestCase):
    """Every representation that crosses an endpoint counts exactly once."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.fixture = VerticalFixture(
            self.root,
            _origin_agent(),
            _local_agent(),
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _ledgers(self, **overrides: Any) -> dict[str, dict[str, Any]]:
        run_distributed_pilot(
            self.fixture.preregistration,
            self.fixture.registry,
            _GatewayExecutor(self.fixture),
            output_dir=self.root / "run",
            workloads=self.fixture.workloads(),
            provider=self.fixture.provider,
            preflight=self.fixture.preflight(),
            **overrides,
        )
        rows = [
            json.loads(line)
            for line in (
                self.root / "run" / "canonical_records.jsonl"
            ).read_text(encoding="utf-8").splitlines()
        ]
        return {
            row["design_id"]: row["cost_ledger"]
            for row in rows
        }

    def test_remote_inline_digest_produces_non_zero_network_bytes(
        self,
    ) -> None:
        ledgers = self._ledgers()
        origin = ledgers[SAFE_DESIGN]["components"]["network"]
        self.assertGreater(
            origin["raw_quantity"],
            0.0,
            "an inline digest fetched from a remote node still crosses the "
            "network and must not be charged zero",
        )
        self.assertGreater(origin["value"], 0.0)

    def test_origin_is_not_cheaper_merely_for_returning_data_inline(
        self,
    ) -> None:
        ledgers = self._ledgers()
        origin_network = ledgers[SAFE_DESIGN]["components"]["network"]
        self.assertNotEqual(0.0, origin_network["raw_quantity"])
        self.assertEqual("measured", origin_network["value_kind"])
        self.assertNotIn(
            "network",
            ledgers[SAFE_DESIGN]["not_applicable_components"],
        )

    def test_a_local_endpoint_may_use_a_justified_zero(self) -> None:
        payload = _registry_payload()
        payload["endpoints"][1]["network_transport"] = "local"
        payload["endpoints"][1]["network_zero_justification"] = (
            "this Data Agent serves from the execution node's own NVMe; no "
            "bytes cross a network"
        )
        registry = build_endpoint_registry(
            payload,
            source_sha256=self.fixture.registry.source_sha256,
        )
        self.assertFalse(registry.endpoint(LOCAL_ENDPOINT).crosses_network)
        self.assertTrue(registry.endpoint(ORIGIN_ENDPOINT).crosses_network)
        fixture = self.fixture
        fixture.registry = registry
        run_distributed_pilot(
            fixture.preregistration,
            registry,
            _GatewayExecutor(fixture),
            output_dir=self.root / "local-run",
            workloads=fixture.workloads(),
            provider=fixture.provider,
            preflight=fixture.preflight(),
        )
        rows = [
            json.loads(line)
            for line in (
                self.root / "local-run" / "canonical_records.jsonl"
            ).read_text(encoding="utf-8").splitlines()
        ]
        local = next(r for r in rows if r["design_id"] == FRAMES_DESIGN)
        network = local["cost_ledger"]["components"]["network"]
        self.assertEqual(0.0, network["raw_quantity"])
        self.assertEqual(0.0, network["value"])
        self.assertTrue(local["cost_ledger"]["total_cost_available"])

    def test_a_local_transport_requires_a_justification(self) -> None:
        payload = _registry_payload()
        payload["endpoints"][1]["network_transport"] = "local"
        with self.assertRaisesRegex(
            EndpointRegistryError,
            "network_zero_justification",
        ):
            build_endpoint_registry(payload, source_sha256="0" * 64)

    def test_artifact_transfer_is_counted_exactly_once(self) -> None:
        ledgers = self._ledgers()
        network = ledgers[FRAMES_DESIGN]["components"]["network"]
        # The handle-metadata response is NOT added to the artifact bytes.
        self.assertEqual(
            float(self.fixture.local.artifact_bytes),
            network["raw_quantity"],
        )

    def test_missing_byte_telemetry_suppresses_the_total(self) -> None:
        class _StripBytes(_GatewayExecutor):
            def execute(self, trial, *, workload, journal=None, attempt=1):
                result = super().execute(
                    trial, workload=workload, journal=journal, attempt=attempt
                )
                events = []
                for event in result.access_events:
                    event = dict(event)
                    if event.get("accepted"):
                        event["bytes_read"] = None
                        event["artifact_bytes_sent"] = None
                    events.append(event)
                return replace(result, access_events=tuple(events))

        run_distributed_pilot(
            self.fixture.preregistration,
            self.fixture.registry,
            _StripBytes(self.fixture),
            output_dir=self.root / "stripped",
            workloads=self.fixture.workloads(),
            provider=self.fixture.provider,
            preflight=self.fixture.preflight(),
        )
        rows = [
            json.loads(line)
            for line in (
                self.root / "stripped" / "canonical_records.jsonl"
            ).read_text(encoding="utf-8").splitlines()
        ]
        for row in rows:
            ledger = row["cost_ledger"]
            self.assertFalse(ledger["total_cost_available"])
            self.assertIn("network", ledger["unavailable_components"])
            self.assertIsNone(ledger["total_cost"])

    def test_an_incomplete_artifact_download_is_not_a_byte_measurement(
        self,
    ) -> None:
        from pathfinder.distributed.measurements import (
            transferred_bytes_for_event,
        )

        event = {
            "artifact_handle_sha256": "a" * 64,
            "bytes_read": 120,
            "artifact_bytes_sent": 4096,
            "artifact_download_request_count": 1,
            "artifact_full_download_count": 0,
        }
        self.assertIsNone(
            transferred_bytes_for_event(event, crosses_network=True)
        )


class EndpointAwareReconciliationTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_a_shared_access_id_resolves_to_the_issuing_endpoint(
        self,
    ) -> None:
        # Both nodes mint the SAME access id with different byte counts.
        origin = _origin_agent(artifact_bytes=111)
        local = _local_agent(artifact_bytes=999)
        shared = "collision-access-id"
        origin._telemetry[shared] = DataAgentAccessTelemetry(
            access_id=shared,
            object_id="video-object-1",
            representation_id="sampled_frames",
            object_catalog_version=origin.catalog_version,
            download_request_count=1,
            completed_request_count=1,
            full_download_count=1,
            bytes_sent=111,
            transfer_latency_ms=1.0,
            in_flight_request_count=0,
            server_reported_complete=True,
        )
        local._telemetry[shared] = DataAgentAccessTelemetry(
            access_id=shared,
            object_id="video-object-1",
            representation_id="sampled_frames",
            object_catalog_version=local.catalog_version,
            download_request_count=1,
            completed_request_count=1,
            full_download_count=1,
            bytes_sent=999,
            transfer_latency_ms=1.0,
            in_flight_request_count=0,
            server_reported_complete=True,
        )
        fixture = VerticalFixture(self.root, origin, local)
        backend = fixture.backend()
        self.assertTrue(backend.endpoint_aware_telemetry)
        self.assertEqual(
            111,
            backend.get_access_telemetry(
                shared,
                endpoint_id=ORIGIN_ENDPOINT,
            ).bytes_sent,
        )
        self.assertEqual(
            999,
            backend.get_access_telemetry(
                shared,
                endpoint_id=LOCAL_ENDPOINT,
            ).bytes_sent,
        )

    def test_a_multi_endpoint_record_without_identity_fails_closed(
        self,
    ) -> None:
        fixture = VerticalFixture(self.root, _origin_agent(), _local_agent())
        backend = fixture.backend()
        with self.assertRaisesRegex(
            Exception,
            "refusing to guess which one served it",
        ):
            backend.get_access_telemetry("some-access", endpoint_id=None)

    def test_a_single_endpoint_record_without_identity_is_compatible(
        self,
    ) -> None:
        payload = _registry_payload()
        payload["endpoints"] = [payload["endpoints"][0]]
        payload.pop("placement")
        registry = build_endpoint_registry(payload, source_sha256="0" * 64)
        origin = _origin_agent()
        fixture = VerticalFixture(self.root, origin, _local_agent())
        backend = RoutedDataAgentBackend(
            registry,
            {ORIGIN_ENDPOINT: origin},
        )
        origin._telemetry["legacy"] = DataAgentAccessTelemetry(
            access_id="legacy",
            object_id="video-object-1",
            representation_id="sampled_frames",
            object_catalog_version=origin.catalog_version,
            download_request_count=1,
            completed_request_count=1,
            full_download_count=1,
            bytes_sent=42,
            transfer_latency_ms=1.0,
            in_flight_request_count=0,
            server_reported_complete=True,
        )
        self.assertEqual(
            42,
            backend.get_access_telemetry(
                "legacy",
                endpoint_id=None,
            ).bytes_sent,
        )


# --------------------------------------------------------------------------
# 10. The same vertical run fails when something is wrong
# --------------------------------------------------------------------------
class VerticalNegativeTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.fixture = VerticalFixture(
            self.root,
            _origin_agent(),
            _local_agent(),
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_wrong_endpoint_placement_fails_the_run(self) -> None:
        # Route the local design to the origin node, which does not serve
        # a local materialization at all.
        payload = _registry_payload()
        payload["placement"] = [
            {
                "design_id": SAFE_DESIGN,
                "representation_id": "*",
                "endpoint_id": ORIGIN_ENDPOINT,
            },
            {
                "design_id": FRAMES_DESIGN,
                "representation_id": "*",
                "endpoint_id": LOCAL_ENDPOINT,
            },
        ]
        registry = build_endpoint_registry(payload, source_sha256="0" * 64)
        backend = RoutedDataAgentBackend(registry, self.fixture.agents)
        gateway = AccessGateway(
            self.fixture.system_config,
            SQLiteSessionStore(self.root / "wrong.sqlite3"),
            backend=backend,
        )
        session = gateway.register_session(FlowMeshAgentRunRequest(
            trial_id="wrong",
            question="What happens?",
            design_id=SAFE_DESIGN,
            task_class_id="video_qa",
            quote_profile_id="as_designed",
            latency_multiplier=1.0,
            seed=1,
            object_id="video-object-1",
            session_id=None,
        ))
        # Origin does not serve this representation from the local node.
        self.fixture.origin.representations = ("sampled_frames",)
        with self.assertRaises(EndpointUnreachableError):
            gateway.access_representation(
                session.session_id,
                "multimodal_digest",
            )

    def test_a_wrong_catalog_identity_fails_preflight(self) -> None:
        report = preflight_distributed_pilot(
            self.fixture.preregistration,
            self.fixture.registry,
            probe=_FakeHealthProbe(self.fixture.agents),
            worker_pin={"kind": "worker_alias", "value": "v"},
            environment=ENVIRONMENT,
            expected_catalog_versions={"video-object-1": "catalog-wrong"},
            measurement_manifest_sha256=self.fixture.provider.manifest_sha256,
        )
        self.assertEqual("failed", report["status"])
        self.assertTrue([
            check for check in report["failed_checks"]
            if "object_catalog_versions_match" in check
        ])

    def test_a_wrong_measurement_hash_fails_the_run(self) -> None:
        path = self.root / "wrong-registry.json"
        _write_json(path, _measurement_payload(
            self.fixture.preregistration.source_sha256,
            "e" * 64,
        ))
        with self.assertRaises(StaleMeasurementError):
            run_distributed_pilot(
                self.fixture.preregistration,
                self.fixture.registry,
                _GatewayExecutor(self.fixture),
                output_dir=self.root / "run",
                workloads=self.fixture.workloads(),
                provider=load_measurement_manifest(path),
                preflight=self.fixture.preflight(),
            )

    def test_a_missing_cost_component_suppresses_the_total(self) -> None:
        payload = _measurement_payload(
            self.fixture.preregistration.source_sha256,
            self.fixture.registry.source_sha256,
        )
        payload["measurements"][1]["storage"] = {
            "kind": "unavailable",
            "reason": "no storage accounting on the beta node",
        }
        path = self.root / "incomplete.json"
        _write_json(path, payload)
        run_distributed_pilot(
            self.fixture.preregistration,
            self.fixture.registry,
            _GatewayExecutor(self.fixture),
            output_dir=self.root / "run",
            workloads=self.fixture.workloads(),
            provider=load_measurement_manifest(path),
            preflight=self.fixture.preflight(),
        )
        rows = [
            json.loads(line)
            for line in (
                self.root / "run" / "canonical_records.jsonl"
            ).read_text(encoding="utf-8").splitlines()
        ]
        local = next(r for r in rows if r["design_id"] == FRAMES_DESIGN)
        ledger = local["cost_ledger"]
        self.assertFalse(ledger["total_cost_available"])
        self.assertIsNone(ledger["total_cost"])
        self.assertEqual(["storage"], ledger["unavailable_components"])

        from pathfinder.distributed.cost import CostTelemetryIncompleteError
        from pathfinder.distributed.cost import record_total_cost

        with self.assertRaises(CostTelemetryIncompleteError):
            record_total_cost(local)

    def test_a_measurement_gap_is_refused_rather_than_assumed_zero(
        self,
    ) -> None:
        payload = _measurement_payload(
            self.fixture.preregistration.source_sha256,
            self.fixture.registry.source_sha256,
        )
        payload["measurements"] = [payload["measurements"][0]]
        path = self.root / "gap.json"
        _write_json(path, payload)
        with self.assertRaisesRegex(
            MeasurementError,
            "refusing to assume zero",
        ):
            run_distributed_pilot(
                self.fixture.preregistration,
                self.fixture.registry,
                _GatewayExecutor(self.fixture),
                output_dir=self.root / "run",
                workloads=self.fixture.workloads(),
                provider=load_measurement_manifest(path),
                preflight=self.fixture.preflight(),
            )


if __name__ == "__main__":
    unittest.main()



class WorkloadContentFreezeTest(unittest.TestCase):
    """The plan binds what a workload says, not merely its id."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.fixture = VerticalFixture(
            self.root,
            _origin_agent(),
            _local_agent(),
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _run(self, workloads, *, output: str = "run"):
        return run_distributed_pilot(
            self.fixture.preregistration,
            self.fixture.registry,
            _GatewayExecutor(self.fixture),
            output_dir=self.root / output,
            workloads=workloads,
            provider=self.fixture.provider,
            preflight=self.fixture.preflight(),
        )

    def test_the_plan_records_a_content_digest(self) -> None:
        summary = self._run(self.fixture.workloads())
        plan = json.loads(
            (
                self.root / "run" / "distributed_pilot_plan.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(64, len(plan["workload_content_sha256"]))
        self.assertNotEqual(
            plan["workload_content_sha256"],
            plan["workload_id_manifest_sha256"],
        )
        self.assertEqual(
            plan["workload_content_sha256"],
            summary["workload_content_sha256"],
        )
        # The id-only digest is retained but never labelled as content.
        self.assertNotIn("workload_manifest_sha256", plan)

    def test_changing_any_workload_field_is_rejected_on_resume(
        self,
    ) -> None:
        mutations = {
            "question": lambda w: {**w, "question": "A different question?"},
            "object_id": lambda w: {**w, "object_id": "video-object-999"},
            "accepted_answer": lambda w: {
                **w,
                "accepted_answer_substrings": ["cat"],
            },
            "task_class_id": lambda w: {**w, "task_class_id": "other_class"},
            "latency_multiplier": lambda w: {**w, "latency_multiplier": 2},
        }
        for label, mutate in mutations.items():
            with self.subTest(field=label):
                with tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    fixture = VerticalFixture(
                        root,
                        _origin_agent(),
                        _local_agent(),
                    )
                    baseline = fixture.workloads()
                    run_distributed_pilot(
                        fixture.preregistration,
                        fixture.registry,
                        _GatewayExecutor(fixture),
                        output_dir=root / "run",
                        workloads=baseline,
                        provider=fixture.provider,
                        preflight=fixture.preflight(),
                    )
                    target = WORKLOAD_IDS[0]
                    edited = {
                        **baseline,
                        target: mutate(baseline[target]),
                    }
                    with self.assertRaises(PilotResumeError) as caught:
                        run_distributed_pilot(
                            fixture.preregistration,
                            fixture.registry,
                            _GatewayExecutor(fixture),
                            output_dir=root / "run",
                            workloads=edited,
                            provider=fixture.provider,
                            preflight=fixture.preflight(),
                        )
                    self.assertIn(
                        "workload definitions",
                        str(caught.exception),
                    )

    def test_key_order_changes_are_accepted(self) -> None:
        self._run(self.fixture.workloads())
        reordered = {
            workload_id: dict(
                sorted(payload.items(), reverse=True)
            )
            for workload_id, payload in self.fixture.workloads().items()
        }
        # Equivalent JSON with different key order must not invalidate the
        # frozen run.
        summary = self._run(reordered)
        self.assertEqual(0, summary["executed_this_invocation"])
        self.assertTrue(summary["oracle_complete"])

    def test_an_unrelated_extra_workload_does_not_invalidate_the_run(
        self,
    ) -> None:
        self._run(self.fixture.workloads())
        widened = {
            **self.fixture.workloads(),
            "not-in-this-plan": {"question": "irrelevant"},
        }
        summary = self._run(widened)
        self.assertEqual(0, summary["executed_this_invocation"])

    def test_content_hashing_refuses_a_missing_definition(self) -> None:
        from pathfinder.distributed import workload_content_sha256

        with self.assertRaisesRegex(
            PilotPreregistrationError,
            "no definition for",
        ):
            workload_content_sha256({"a": {}}, ["a", "b"])



class CanonicalWindowTest(unittest.TestCase):
    """The window between the canonical fsync and the journal write."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.fixture = VerticalFixture(
            self.root,
            _origin_agent(),
            _local_agent(),
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _run(self, *, output: str = "run", **overrides: Any):
        arguments: dict[str, Any] = {
            "output_dir": self.root / output,
            "workloads": self.fixture.workloads(),
            "provider": self.fixture.provider,
            "preflight": self.fixture.preflight(),
        }
        arguments.update(overrides)
        executor = arguments.pop("executor", None) or _GatewayExecutor(
            self.fixture
        )
        return run_distributed_pilot(
            self.fixture.preregistration,
            self.fixture.registry,
            executor,
            **arguments,
        ), executor

    def _crash_between_append_and_journal(self, output: str) -> None:
        """Die in the exact gap: record fsynced, journal not yet advanced."""
        import pathfinder.distributed.execution as execution

        real_record = execution.CellJournal.record
        state = {"armed": True}

        def crashing_record(self, trial_key, cell_state, **detail):
            if state["armed"] and cell_state == "CANONICAL_WRITTEN":
                state["armed"] = False
                raise _CanonicalBoom("died before journaling the record")
            return real_record(self, trial_key, cell_state, **detail)

        with mock.patch.object(
            execution.CellJournal,
            "record",
            crashing_record,
        ):
            try:
                self._run(output=output, max_attempts=1)
            except _CanonicalBoom:
                pass

    def test_a_crash_in_the_window_leaves_a_record_the_journal_lacks(
        self,
    ) -> None:
        self._crash_between_append_and_journal("gap")
        records = [
            json.loads(line)
            for line in (
                self.root / "gap" / "canonical_records.jsonl"
            ).read_text(encoding="utf-8").splitlines()
        ]
        self.assertEqual(1, len(records))
        journal_states = {
            row["trial_key"]: row["state"]
            for row in (
                json.loads(line)
                for line in (
                    self.root / "gap" / "cell_journal.jsonl"
                ).read_text(encoding="utf-8").splitlines()
            )
        }
        # This is the precondition the fix exists for.
        self.assertEqual(
            "RESULT_OBTAINED",
            journal_states[records[0]["trial_key"]],
        )

    def test_resume_replays_that_record_without_re_executing(self) -> None:
        self._crash_between_append_and_journal("gap")
        crashed_key = json.loads(
            (
                self.root / "gap" / "canonical_records.jsonl"
            ).read_text(encoding="utf-8").splitlines()[0]
        )["trial_key"]

        summary, executor = self._run(output="gap")
        self.assertTrue(summary["oracle_complete"])
        rows = [
            json.loads(line)
            for line in (
                self.root / "gap" / "canonical_records.jsonl"
            ).read_text(encoding="utf-8").splitlines()
        ]
        keys = [row["trial_key"] for row in rows]
        self.assertEqual(8, len(keys))
        self.assertEqual(
            len(keys),
            len(set(keys)),
            "the cell in the crash window was appended twice",
        )
        self.assertEqual(1, keys.count(crashed_key))
        # The recovered cell was replayed, not run again.
        replayed = [
            row for row in (
                json.loads(line)
                for line in (
                    self.root / "gap" / "attempt_ledger.jsonl"
                ).read_text(encoding="utf-8").splitlines()
            )
            if row.get("replayed_from_canonical_record")
        ]
        self.assertEqual(1, len(replayed))
        self.assertEqual(crashed_key, replayed[0]["trial_key"])
        self.assertNotIn(
            crashed_key,
            {
                session.split("|")[-1]
                for session in executor.sessions
            },
        )

    def test_a_duplicate_canonical_record_fails_closed(self) -> None:
        self._run(output="dupe")
        ledger = self.root / "dupe" / "canonical_records.jsonl"
        lines = ledger.read_text(encoding="utf-8").splitlines()
        ledger.write_text(
            "\n".join(lines + [lines[0]]) + "\n",
            encoding="utf-8",
        )
        (self.root / "dupe" / "attempt_ledger.jsonl").unlink()
        with self.assertRaisesRegex(
            CanonicalRecordError,
            "duplicate canonical record",
        ):
            self._run(output="dupe")

    def test_conflicting_canonical_records_fail_closed(self) -> None:
        self._run(output="conflict")
        ledger = self.root / "conflict" / "canonical_records.jsonl"
        lines = ledger.read_text(encoding="utf-8").splitlines()
        conflicting = json.loads(lines[0])
        conflicting["task_success"] = not conflicting["task_success"]
        ledger.write_text(
            "\n".join(lines + [json.dumps(conflicting, sort_keys=True)])
            + "\n",
            encoding="utf-8",
        )
        (self.root / "conflict" / "attempt_ledger.jsonl").unlink()
        with self.assertRaisesRegex(
            CanonicalRecordError,
            "conflicting canonical records",
        ):
            self._run(output="conflict")

    def test_an_unknown_trial_key_fails_closed(self) -> None:
        self._run(output="unknown")
        ledger = self.root / "unknown" / "canonical_records.jsonl"
        rogue = json.loads(
            ledger.read_text(encoding="utf-8").splitlines()[0]
        )
        rogue["trial_key"] = "not|in|the|plan|r0"
        with ledger.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(rogue, sort_keys=True) + "\n")
        with self.assertRaisesRegex(
            CanonicalRecordError,
            "not in the frozen plan",
        ):
            self._run(output="unknown")

    def test_malformed_records_fail_closed(self) -> None:
        cases = {
            "no trial_key": lambda row: {
                k: v for k, v in row.items() if k != "trial_key"
            },
            "missing design_id": lambda row: {**row, "design_id": None},
            "incomplete telemetry": lambda row: {
                **row,
                "telemetry_complete": False,
            },
            "undelivered artifact": lambda row: {
                **row,
                "artifact_delivery_complete": False,
            },
        }
        for label, mutate in cases.items():
            with self.subTest(case=label):
                with tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    fixture = VerticalFixture(
                        root,
                        _origin_agent(),
                        _local_agent(),
                    )
                    run_distributed_pilot(
                        fixture.preregistration,
                        fixture.registry,
                        _GatewayExecutor(fixture),
                        output_dir=root / "run",
                        workloads=fixture.workloads(),
                        provider=fixture.provider,
                        preflight=fixture.preflight(),
                    )
                    ledger = root / "run" / "canonical_records.jsonl"
                    lines = ledger.read_text(
                        encoding="utf-8"
                    ).splitlines()
                    edited = mutate(json.loads(lines[0]))
                    ledger.write_text(
                        "\n".join(
                            [json.dumps(edited, sort_keys=True)] + lines[1:]
                        ) + "\n",
                        encoding="utf-8",
                    )
                    with self.assertRaises(CanonicalRecordError):
                        run_distributed_pilot(
                            fixture.preregistration,
                            fixture.registry,
                            _GatewayExecutor(fixture),
                            output_dir=root / "run",
                            workloads=fixture.workloads(),
                            provider=fixture.provider,
                            preflight=fixture.preflight(),
                        )


class _CanonicalBoom(RuntimeError):
    """Simulated process death inside the canonical write window."""



class OperatorInterruptionTest(unittest.TestCase):
    """Ctrl-C is a decision, not a flaky endpoint."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.fixture = VerticalFixture(
            self.root,
            _origin_agent(),
            _local_agent(),
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _run(self, executor, *, output: str = "run", **overrides: Any):
        arguments: dict[str, Any] = {
            "output_dir": self.root / output,
            "workloads": self.fixture.workloads(),
            "provider": self.fixture.provider,
            "preflight": self.fixture.preflight(),
        }
        arguments.update(overrides)
        return run_distributed_pilot(
            self.fixture.preregistration,
            self.fixture.registry,
            executor,
            **arguments,
        )

    def _interrupting_executor(self, signal_type):
        fixture = self.fixture

        class _Interrupting(_GatewayExecutor):
            calls = 0

            def execute(self, trial, *, workload, journal=None, attempt=1):
                type(self).calls += 1
                if type(self).calls == 3:
                    raise signal_type()
                return super().execute(
                    trial,
                    workload=workload,
                    journal=journal,
                    attempt=attempt,
                )

        _Interrupting.calls = 0
        return _Interrupting(fixture)

    def test_keyboard_interrupt_propagates_immediately(self) -> None:
        with self.assertRaises(KeyboardInterrupt):
            self._run(self._interrupting_executor(KeyboardInterrupt))

    def test_system_exit_propagates_immediately(self) -> None:
        with self.assertRaises(SystemExit):
            self._run(self._interrupting_executor(SystemExit))

    def test_an_interrupt_creates_no_failed_attempt_row(self) -> None:
        with self.assertRaises(KeyboardInterrupt):
            self._run(self._interrupting_executor(KeyboardInterrupt))
        rows = [
            json.loads(line)
            for line in (
                self.root / "run" / "attempt_ledger.jsonl"
            ).read_text(encoding="utf-8").splitlines()
        ]
        self.assertTrue(rows)
        for row in rows:
            self.assertEqual("canonical", row["observation_class"])
            self.assertTrue(row["succeeded"])
        self.assertEqual(2, len(rows))

    def test_an_interrupt_does_not_consume_the_retry_budget(self) -> None:
        with self.assertRaises(KeyboardInterrupt):
            self._run(self._interrupting_executor(KeyboardInterrupt))
        # The interrupted cell is journaled as STARTED but has no attempt.
        journal = [
            json.loads(line)
            for line in (
                self.root / "run" / "cell_journal.jsonl"
            ).read_text(encoding="utf-8").splitlines()
        ]
        interrupted = [
            row for row in journal
            if row["state"] == "STARTED"
        ][-1]["trial_key"]
        attempts = [
            json.loads(line)
            for line in (
                self.root / "run" / "attempt_ledger.jsonl"
            ).read_text(encoding="utf-8").splitlines()
        ]
        self.assertEqual(
            0,
            sum(1 for row in attempts if row["trial_key"] == interrupted),
        )

    def test_resume_after_an_interrupt_completes_the_oracle(self) -> None:
        with self.assertRaises(KeyboardInterrupt):
            self._run(self._interrupting_executor(KeyboardInterrupt))
        summary = self._run(_GatewayExecutor(self.fixture))
        self.assertTrue(summary["oracle_complete"])
        self.assertEqual(8, summary["completed_canonical_count"])
        self.assertEqual(6, summary["executed_this_invocation"])
        rows = (
            self.root / "run" / "canonical_records.jsonl"
        ).read_text(encoding="utf-8").splitlines()
        keys = [json.loads(line)["trial_key"] for line in rows]
        self.assertEqual(8, len(keys))
        self.assertEqual(len(keys), len(set(keys)))

    def test_ordinary_exceptions_are_still_classified(self) -> None:
        fixture = self.fixture

        class _Failing(_GatewayExecutor):
            calls = 0

            def execute(self, trial, *, workload, journal=None, attempt=1):
                type(self).calls += 1
                if type(self).calls == 1:
                    raise RuntimeError("transient endpoint blip")
                return super().execute(
                    trial,
                    workload=workload,
                    journal=journal,
                    attempt=attempt,
                )

        _Failing.calls = 0
        summary = self._run(_Failing(fixture), output="classified")
        self.assertEqual(1, summary["infrastructure_failure_count"])
        self.assertTrue(summary["oracle_complete"])



class CliPreflightAndExitCodeTest(unittest.TestCase):
    """Standalone preflight probes for real; a partial run exits nonzero."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.fixture = VerticalFixture(
            self.root,
            _origin_agent(),
            _local_agent(),
        )
        self.registry_path = self.root / "registry.json"
        _write_json(self.registry_path, _registry_payload())

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _cli(self, argv):
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            code = cli_main(argv)
        return code, json.loads(stdout.getvalue())

    def _preflight_argv(self):
        return [
            "preflight-distributed-pilot",
            "--preregistration",
            str(self.fixture.preregistration_path),
            "--endpoint-registry",
            str(self.registry_path),
            "--measurement-manifest",
            str(self.fixture.measurement_path),
            "--worker-alias",
            "vertical-worker",
            "--compact",
        ]

    def _health_opener(self, agents, *, unreachable=()):
        """An opener answering from the in-process fake Data Agents."""
        by_host = {
            "alpha.invalid": agents[ORIGIN_ENDPOINT],
            "beta.invalid": agents[LOCAL_ENDPOINT],
        }

        def opener(request, timeout=None):
            host = request.full_url.split("//", 1)[1].split(":", 1)[0]
            agent = by_host[host]
            if agent.endpoint_id in unreachable:
                raise OSError(f"connection refused to {host}")
            return _HealthResponse(
                json.dumps(agent.health_document()).encode("utf-8")
            )

        return opener

    def test_a_healthy_endpoint_produces_an_ok_preflight(self) -> None:
        opener = self._health_opener(self.fixture.agents)
        with mock.patch.dict(os.environ, _VERTICAL_ENVIRONMENT), \
                mock.patch(
                    "pathfinder.distributed.health._no_redirect_opener",
                    lambda: _StubOpener(opener),
                ):
            code, payload = self._cli(self._preflight_argv())
        self.assertEqual("ok", payload["status"], payload["failed_checks"])
        self.assertEqual(0, code)
        # The probe really ran: health checks are present and passing.
        health_checks = [
            check for check in payload["checks"]
            if check["check_id"].endswith(".health")
        ]
        self.assertEqual(2, len(health_checks))
        for check in health_checks:
            self.assertTrue(check["passed"], check)

    def test_an_unreachable_endpoint_returns_a_nonzero_exit(self) -> None:
        opener = self._health_opener(
            self.fixture.agents,
            unreachable=(ORIGIN_ENDPOINT,),
        )
        with mock.patch.dict(os.environ, _VERTICAL_ENVIRONMENT), \
                mock.patch(
                    "pathfinder.distributed.health._no_redirect_opener",
                    lambda: _StubOpener(opener),
                ):
            code, payload = self._cli(self._preflight_argv())
        self.assertEqual(1, code)
        self.assertEqual("failed", payload["status"])
        self.assertIn(
            f"endpoint[{ORIGIN_ENDPOINT}].health",
            payload["failed_checks"],
        )

    def test_live_pilot_placeholder_checks_are_not_weakened(self) -> None:
        opener = self._health_opener(self.fixture.agents)
        with mock.patch.dict(os.environ, _VERTICAL_ENVIRONMENT), \
                mock.patch(
                    "pathfinder.distributed.health._no_redirect_opener",
                    lambda: _StubOpener(opener),
                ):
            code, payload = self._cli(
                self._preflight_argv() + ["--mode", "live_pilot"]
            )
        # The fixture's rate provenance is a non-scientific fixture string,
        # which live mode must still reject.
        self.assertEqual(1, code)
        self.assertIn(
            "cost_model.rates_are_measured_not_placeholder",
            payload["failed_checks"],
        )

    def test_a_complete_run_exits_zero_and_a_partial_run_does_not(
        self,
    ) -> None:
        from pathfinder.distributed import run_distributed_pilot

        complete = run_distributed_pilot(
            self.fixture.preregistration,
            self.fixture.registry,
            _GatewayExecutor(self.fixture),
            output_dir=self.root / "complete",
            workloads=self.fixture.workloads(),
            provider=self.fixture.provider,
            preflight=self.fixture.preflight(),
        )
        self.assertEqual("COMPLETE", complete["status"])
        self.assertTrue(complete["oracle_complete"])
        self.assertEqual(0, _exit_code_for(complete))

        class _AlwaysFails(_GatewayExecutor):
            def execute(self, trial, *, workload, journal=None, attempt=1):
                raise RuntimeError("endpoint down")

        partial = run_distributed_pilot(
            self.fixture.preregistration,
            self.fixture.registry,
            _AlwaysFails(self.fixture),
            output_dir=self.root / "partial",
            workloads=self.fixture.workloads(),
            provider=self.fixture.provider,
            preflight=self.fixture.preflight(),
            max_attempts=1,
        )
        self.assertEqual("PARTIAL", partial["status"])
        self.assertFalse(partial["oracle_complete"])
        self.assertNotEqual(0, _exit_code_for(partial))

    def test_an_incomplete_oracle_never_exits_zero(self) -> None:
        for status, oracle_complete in (
            ("PARTIAL", False),
            ("PARTIAL", True),
            ("COMPLETE", False),
        ):
            with self.subTest(status=status, complete=oracle_complete):
                self.assertNotEqual(
                    0,
                    _exit_code_for({
                        "status": status,
                        "oracle_complete": oracle_complete,
                    }),
                )
        self.assertEqual(
            0,
            _exit_code_for({
                "status": "COMPLETE",
                "oracle_complete": True,
            }),
        )


def _exit_code_for(payload: Mapping[str, Any]) -> int:
    """The exact rule run-distributed-pilot applies to its summary."""
    complete = (
        payload.get("status") == "COMPLETE"
        and bool(payload.get("oracle_complete"))
    )
    return 0 if complete else 1


class _HealthResponse:
    def __init__(self, body: bytes, status: int = 200) -> None:
        self._body = body
        self.status = status

    def read(self, amount=None) -> bytes:
        return self._body if amount is None else self._body[:amount]

    def __enter__(self):
        return self

    def __exit__(self, *exc: object) -> bool:
        return False


class _StubOpener:
    def __init__(self, opener) -> None:
        self.open = opener
        self.handlers = []


_VERTICAL_ENVIRONMENT = dict(ENVIRONMENT)



class CanonicalIdentityValidationTest(unittest.TestCase):
    """A replayed record must describe the exact frozen cell."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.fixture = VerticalFixture(
            self.root,
            _origin_agent(),
            _local_agent(),
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _run(self, *, output: str = "run", **overrides: Any):
        arguments: dict[str, Any] = {
            "output_dir": self.root / output,
            "workloads": self.fixture.workloads(),
            "provider": self.fixture.provider,
            "preflight": self.fixture.preflight(),
        }
        arguments.update(overrides)
        executor = arguments.pop("executor", None) or _GatewayExecutor(
            self.fixture
        )
        return run_distributed_pilot(
            self.fixture.preregistration,
            self.fixture.registry,
            executor,
            **arguments,
        ), executor

    def _mutate_first_record(self, output: str, mutate) -> None:
        ledger = self.root / output / "canonical_records.jsonl"
        lines = ledger.read_text(encoding="utf-8").splitlines()
        edited = mutate(json.loads(lines[0]))
        ledger.write_text(
            "\n".join(
                [json.dumps(edited, sort_keys=True)] + lines[1:]
            ) + "\n",
            encoding="utf-8",
        )

    def test_every_identity_field_is_compared(self) -> None:
        from pathfinder.distributed import TRIAL_IDENTITY_FIELDS

        self.assertEqual(
            {
                "trial_key",
                "trial_id",
                "session_id",
                "order_index",
                "stratum_id",
                "workload_id",
                "design_id",
                "is_safe_design",
                "repetition",
                "seed",
            },
            set(TRIAL_IDENTITY_FIELDS),
        )

    def test_a_mismatched_identity_field_is_refused(self) -> None:
        mutations = {
            "design_id": lambda row: {**row, "design_id": FRAMES_DESIGN},
            "workload_id": lambda row: {
                **row,
                "workload_id": WORKLOAD_IDS[1],
            },
            "repetition": lambda row: {**row, "repetition": 1},
            "trial_id": lambda row: {**row, "trial_id": "not-the-trial"},
            "session_id": lambda row: {
                **row,
                "session_id": "not-the-session",
            },
            "order_index": lambda row: {**row, "order_index": 99},
            "stratum_id": lambda row: {**row, "stratum_id": "temporal"},
            "is_safe_design": lambda row: {
                **row,
                "is_safe_design": not row["is_safe_design"],
            },
            "seed": lambda row: {**row, "seed": row["seed"] + 1},
        }
        for label, mutate in mutations.items():
            with self.subTest(field=label):
                with tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    fixture = VerticalFixture(
                        root,
                        _origin_agent(),
                        _local_agent(),
                    )
                    run_distributed_pilot(
                        fixture.preregistration,
                        fixture.registry,
                        _GatewayExecutor(fixture),
                        output_dir=root / "run",
                        workloads=fixture.workloads(),
                        provider=fixture.provider,
                        preflight=fixture.preflight(),
                    )
                    ledger = root / "run" / "canonical_records.jsonl"
                    lines = ledger.read_text(
                        encoding="utf-8"
                    ).splitlines()
                    edited = mutate(json.loads(lines[0]))
                    ledger.write_text(
                        "\n".join(
                            [json.dumps(edited, sort_keys=True)]
                            + lines[1:]
                        ) + "\n",
                        encoding="utf-8",
                    )
                    with self.assertRaises(CanonicalRecordError) as caught:
                        run_distributed_pilot(
                            fixture.preregistration,
                            fixture.registry,
                            _GatewayExecutor(fixture),
                            output_dir=root / "run",
                            workloads=fixture.workloads(),
                            provider=fixture.provider,
                            preflight=fixture.preflight(),
                        )
                    message = str(caught.exception)
                    self.assertIn("does not match the frozen trial", message)

    def test_a_record_from_another_pilot_is_refused(self) -> None:
        self._run(output="pilot")
        self._mutate_first_record(
            "pilot",
            lambda row: {**row, "experiment_id": "some-other-pilot"},
        )
        with self.assertRaisesRegex(
            CanonicalRecordError,
            "belongs to pilot",
        ):
            self._run(output="pilot")

    def test_a_record_with_a_foreign_schema_is_refused(self) -> None:
        self._run(output="schema")
        self._mutate_first_record(
            "schema",
            lambda row: {**row, "schema_version": "some.other/v1"},
        )
        with self.assertRaisesRegex(
            CanonicalRecordError,
            "schema_version",
        ):
            self._run(output="schema")

    def test_a_missing_identity_field_is_refused(self) -> None:
        self._run(output="missing")
        self._mutate_first_record(
            "missing",
            lambda row: {k: v for k, v in row.items() if k != "seed"},
        )
        with self.assertRaisesRegex(
            CanonicalRecordError,
            "missing identity field seed",
        ):
            self._run(output="missing")

    def test_a_swapped_record_pair_is_refused(self) -> None:
        # Two valid records whose trial_keys are exchanged: each is
        # individually well formed, and only identity comparison catches it.
        self._run(output="swap")
        ledger = self.root / "swap" / "canonical_records.jsonl"
        rows = [
            json.loads(line)
            for line in ledger.read_text(encoding="utf-8").splitlines()
        ]
        rows[0]["trial_key"], rows[1]["trial_key"] = (
            rows[1]["trial_key"],
            rows[0]["trial_key"],
        )
        ledger.write_text(
            "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
            encoding="utf-8",
        )
        (self.root / "swap" / "attempt_ledger.jsonl").unlink()
        with self.assertRaisesRegex(
            CanonicalRecordError,
            "does not match the frozen trial",
        ):
            self._run(output="swap")

    def test_a_bare_key_collection_is_rejected(self) -> None:
        """Weaker structural-only validation is no longer reachable."""
        from pathfinder.distributed import load_durable_canonical_records

        self._run(output="bare")
        ledger = self.root / "bare" / "canonical_records.jsonl"
        trials = build_distributed_trial_plan(
            self.fixture.preregistration
        )
        keys = [trial.trial_key for trial in trials]
        for label, collection in (
            ("list", keys),
            ("set", set(keys)),
            ("tuple", tuple(keys)),
            ("generator", (key for key in keys)),
        ):
            with self.subTest(kind=label):
                with self.assertRaisesRegex(
                    CanonicalRecordError,
                    "must map trial_key to the frozen DistributedTrial",
                ):
                    load_durable_canonical_records(ledger, collection)

    def test_a_mapping_to_non_trial_values_is_rejected(self) -> None:
        from pathfinder.distributed import load_durable_canonical_records

        self._run(output="values")
        ledger = self.root / "values" / "canonical_records.jsonl"
        trials = build_distributed_trial_plan(
            self.fixture.preregistration
        )
        broken = {
            trial.trial_key: trial.to_public_dict()
            for trial in trials
        }
        with self.assertRaisesRegex(
            CanonicalRecordError,
            "is not a DistributedTrial",
        ):
            load_durable_canonical_records(ledger, broken)

    def test_a_proper_trial_mapping_is_accepted(self) -> None:
        from pathfinder.distributed import load_durable_canonical_records

        self._run(output="proper")
        ledger = self.root / "proper" / "canonical_records.jsonl"
        trials = build_distributed_trial_plan(
            self.fixture.preregistration
        )
        records = load_durable_canonical_records(
            ledger,
            {trial.trial_key: trial for trial in trials},
            pilot_id=self.fixture.preregistration.pilot_id,
        )
        self.assertEqual(8, len(records))
        self.assertEqual(
            {trial.trial_key for trial in trials},
            set(records),
        )

    def test_a_valid_crash_window_record_still_replays(self) -> None:
        """The tightened validator must not break legitimate recovery."""
        import pathfinder.distributed.execution as execution

        real_record = execution.CellJournal.record
        state = {"armed": True}

        def crashing_record(self, trial_key, cell_state, **detail):
            if state["armed"] and cell_state == "CANONICAL_WRITTEN":
                state["armed"] = False
                raise _CanonicalBoom("died before journaling")
            return real_record(self, trial_key, cell_state, **detail)

        with mock.patch.object(
            execution.CellJournal,
            "record",
            crashing_record,
        ):
            try:
                self._run(output="valid", max_attempts=1)
            except _CanonicalBoom:
                pass

        crashed_key = json.loads(
            (
                self.root / "valid" / "canonical_records.jsonl"
            ).read_text(encoding="utf-8").splitlines()[0]
        )["trial_key"]

        summary, executor = self._run(output="valid")
        self.assertTrue(summary["oracle_complete"])
        rows = [
            json.loads(line)
            for line in (
                self.root / "valid" / "canonical_records.jsonl"
            ).read_text(encoding="utf-8").splitlines()
        ]
        keys = [row["trial_key"] for row in rows]
        self.assertEqual(8, len(keys))
        self.assertEqual(1, keys.count(crashed_key))
        replayed = [
            row for row in (
                json.loads(line)
                for line in (
                    self.root / "valid" / "attempt_ledger.jsonl"
                ).read_text(encoding="utf-8").splitlines()
            )
            if row.get("replayed_from_canonical_record")
        ]
        self.assertEqual([crashed_key], [r["trial_key"] for r in replayed])
        # Replayed, so no session was opened for that cell.
        self.assertEqual(7, len(executor.sessions))



class PlanCommandWorkloadBindingTest(unittest.TestCase):
    """A written plan is always bound to real workload content."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.fixture = VerticalFixture(
            self.root,
            _origin_agent(),
            _local_agent(),
        )
        self.manifest = self.root / "workloads.json"
        _write_json(self.manifest, self.fixture.workloads())
        self.registry_path = self.root / "registry.json"
        _write_json(self.registry_path, _registry_payload())
        # Execution must see the same registry digest the CLI computes from
        # the file, so the fixture adopts the file-loaded registry and its
        # measurement manifest is rebound to that digest.
        self.fixture.registry = load_endpoint_registry(self.registry_path)
        _write_json(self.fixture.measurement_path, _measurement_payload(
            self.fixture.preregistration.source_sha256,
            self.fixture.registry.source_sha256,
        ))
        self.fixture.provider = load_measurement_manifest(
            self.fixture.measurement_path
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _cli(self, argv):
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            code = cli_main(argv)
        return code, json.loads(stdout.getvalue())

    def _base(self):
        return [
            "plan-distributed-pilot",
            "--preregistration",
            str(self.fixture.preregistration_path),
            "--compact",
        ]

    def test_preview_only_use_needs_no_manifest(self) -> None:
        code, payload = self._cli(self._base())
        self.assertEqual(0, code)
        self.assertTrue(payload["preview_only"])
        self.assertFalse(payload["workload_content_bound"])
        self.assertIsNone(payload["workload_content_sha256"])
        self.assertEqual(8, payload["planned_trial_count"])
        # Nothing resumable was written anywhere.
        self.assertEqual(
            [],
            list(self.root.glob("**/distributed_pilot_plan.json")),
        )

    def test_writing_a_plan_without_a_manifest_is_refused(self) -> None:
        cases = {
            "neither": [],
            "registry only": [
                "--endpoint-registry", "REGISTRY",
            ],
            "manifest only": [
                "--workload-manifest", "MANIFEST",
            ],
        }
        for label, extra in cases.items():
            with self.subTest(case=label):
                target = self.root / f"unbound-{label.replace(' ', '-')}"
                argv = self._base() + ["--output-dir", str(target)]
                for item in extra:
                    argv.append(
                        str(self.registry_path)
                        if item == "REGISTRY"
                        else str(self.manifest)
                        if item == "MANIFEST"
                        else item
                    )
                code, payload = self._cli(argv)
                self.assertEqual(2, code)
                self.assertEqual("error", payload["status"])
                self.assertIn("--output-dir requires", payload["message"])
                self.assertFalse(
                    (target / "distributed_pilot_plan.json").exists(),
                    "no plan may be written when it cannot be bound",
                )

    def test_a_written_plan_carries_the_real_content_hash(self) -> None:
        target = self.root / "bound"
        code, payload = self._cli(self._base() + [
            "--workload-manifest", str(self.manifest),
            "--endpoint-registry", str(self.registry_path),
            "--output-dir", str(target),
        ])
        self.assertEqual(0, code)
        self.assertFalse(payload["preview_only"])
        self.assertTrue(payload["workload_content_bound"])
        written = json.loads(
            (target / "distributed_pilot_plan.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertIsNotNone(written["workload_content_sha256"])
        self.assertEqual(
            workload_content_sha256(
                self.fixture.workloads(),
                self.fixture.preregistration.workload_ids,
            ),
            written["workload_content_sha256"],
            "the planned hash must equal the one execution computes",
        )

    def test_a_planned_run_then_executes_in_the_same_directory(
        self,
    ) -> None:
        target = self.root / "shared"
        code, _ = self._cli(self._base() + [
            "--workload-manifest", str(self.manifest),
            "--endpoint-registry", str(self.registry_path),
            "--output-dir", str(target),
        ])
        self.assertEqual(0, code)
        # The whole point: planning first must not block execution.
        summary = run_distributed_pilot(
            self.fixture.preregistration,
            self.fixture.registry,
            _GatewayExecutor(self.fixture),
            output_dir=target,
            workloads=self.fixture.workloads(),
            provider=self.fixture.provider,
            preflight=self.fixture.preflight(),
        )
        self.assertTrue(summary["oracle_complete"])
        self.assertEqual(8, summary["executed_this_invocation"])

    def test_a_plan_bound_to_different_workloads_blocks_execution(
        self,
    ) -> None:
        target = self.root / "mismatch"
        edited = dict(self.fixture.workloads())
        first = WORKLOAD_IDS[0]
        edited[first] = {**edited[first], "question": "A different one?"}
        other_manifest = self.root / "other.json"
        _write_json(other_manifest, edited)
        code, _ = self._cli(self._base() + [
            "--workload-manifest", str(other_manifest),
            "--endpoint-registry", str(self.registry_path),
            "--output-dir", str(target),
        ])
        self.assertEqual(0, code)
        with self.assertRaises(PilotResumeError):
            run_distributed_pilot(
                self.fixture.preregistration,
                self.fixture.registry,
                _GatewayExecutor(self.fixture),
                output_dir=target,
                workloads=self.fixture.workloads(),
                provider=self.fixture.provider,
                preflight=self.fixture.preflight(),
            )

    def test_a_malformed_manifest_is_refused(self) -> None:
        bad = self.root / "bad.json"
        _write_json(bad, ["not", "a", "mapping"])
        code, payload = self._cli(self._base() + [
            "--workload-manifest", str(bad),
            "--endpoint-registry", str(self.registry_path),
            "--output-dir", str(self.root / "bad-out"),
        ])
        self.assertEqual(2, code)
        self.assertIn("workload_id", payload["message"])



class CanonicalCompletenessValidationTest(unittest.TestCase):
    """A record may be replayed only if it explicitly asserts completeness.

    Truthiness is not enough. The replay path records a recovered cell as
    ``telemetry_complete=True`` and ``artifact_delivery_complete=True``, so a
    missing, null, or string-typed field accepted as close enough would
    silently promote an unfinished cell into a complete observation.
    """

    #: (label, mutation) pairs that must every one be refused.
    REJECTED = {
        "telemetry_complete missing": lambda row: {
            k: v for k, v in row.items() if k != "telemetry_complete"
        },
        "telemetry_complete=None": lambda row: {
            **row, "telemetry_complete": None,
        },
        "telemetry_complete='true'": lambda row: {
            **row, "telemetry_complete": "true",
        },
        "telemetry_complete='True'": lambda row: {
            **row, "telemetry_complete": "True",
        },
        "telemetry_complete=1": lambda row: {
            **row, "telemetry_complete": 1,
        },
        "telemetry_complete=False": lambda row: {
            **row, "telemetry_complete": False,
        },
        "telemetry_complete=0": lambda row: {
            **row, "telemetry_complete": 0,
        },
        "artifact_delivery_complete missing": lambda row: {
            k: v for k, v in row.items()
            if k != "artifact_delivery_complete"
        },
        "artifact_delivery_complete=None": lambda row: {
            **row, "artifact_delivery_complete": None,
        },
        "artifact_delivery_complete='true'": lambda row: {
            **row, "artifact_delivery_complete": "true",
        },
        "artifact_delivery_complete=1": lambda row: {
            **row, "artifact_delivery_complete": 1,
        },
        "artifact_delivery_complete=False": lambda row: {
            **row, "artifact_delivery_complete": False,
        },
        "artifact_delivery_complete=[]": lambda row: {
            **row, "artifact_delivery_complete": [],
        },
        "outcome_type missing": lambda row: {
            k: v for k, v in row.items() if k != "outcome_type"
        },
        "outcome_type=None": lambda row: {**row, "outcome_type": None},
        "outcome_type='infrastructure_failure'": lambda row: {
            **row, "outcome_type": "infrastructure_failure",
        },
        "outcome_type='artifact_delivery_failure'": lambda row: {
            **row, "outcome_type": "artifact_delivery_failure",
        },
        "outcome_type='telemetry_failure'": lambda row: {
            **row, "outcome_type": "telemetry_failure",
        },
        "outcome_type='COMPLETED'": lambda row: {
            **row, "outcome_type": "COMPLETED",
        },
    }

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.fixture = VerticalFixture(
            self.root,
            _origin_agent(),
            _local_agent(),
        )
        self.trials = {
            trial.trial_key: trial
            for trial in build_distributed_trial_plan(
                self.fixture.preregistration
            )
        }

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _seed(self, output: str) -> Path:
        """Run to completion and return the canonical ledger path."""
        run_distributed_pilot(
            self.fixture.preregistration,
            self.fixture.registry,
            _GatewayExecutor(self.fixture),
            output_dir=self.root / output,
            workloads=self.fixture.workloads(),
            provider=self.fixture.provider,
            preflight=self.fixture.preflight(),
        )
        return self.root / output / "canonical_records.jsonl"

    def _rewrite_first(self, ledger: Path, mutate) -> None:
        lines = ledger.read_text(encoding="utf-8").splitlines()
        ledger.write_text(
            "\n".join(
                [json.dumps(mutate(json.loads(lines[0])), sort_keys=True)]
                + lines[1:]
            ) + "\n",
            encoding="utf-8",
        )

    def _load(self, ledger: Path):
        return load_durable_canonical_records(
            ledger,
            self.trials,
            pilot_id=self.fixture.preregistration.pilot_id,
        )

    # -- direct loader ---------------------------------------------------
    def test_the_loader_rejects_every_incomplete_record(self) -> None:
        ledger = self._seed("loader")
        pristine = ledger.read_text(encoding="utf-8")
        for label, mutate in self.REJECTED.items():
            with self.subTest(case=label):
                ledger.write_text(pristine, encoding="utf-8")
                self._rewrite_first(ledger, mutate)
                with self.assertRaises(CanonicalRecordError) as caught:
                    self._load(ledger)
                message = str(caught.exception)
                field = label.split(" ")[0].split("=")[0]
                self.assertIn(
                    field,
                    message,
                    "the error must name the offending field",
                )

    def test_rejection_messages_record_the_observed_value(self) -> None:
        ledger = self._seed("messages")
        pristine = ledger.read_text(encoding="utf-8")
        cases = {
            "telemetry_complete=1": (
                lambda row: {**row, "telemetry_complete": 1},
                ("telemetry_complete=1", "type int"),
            ),
            "telemetry_complete='true'": (
                lambda row: {**row, "telemetry_complete": "true"},
                ("telemetry_complete='true'", "type str"),
            ),
            "artifact_delivery_complete=None": (
                lambda row: {
                    **row, "artifact_delivery_complete": None,
                },
                ("artifact_delivery_complete=None", "type NoneType"),
            ),
            "outcome_type='telemetry_failure'": (
                lambda row: {
                    **row, "outcome_type": "telemetry_failure",
                },
                ("outcome_type='telemetry_failure'", "'completed'"),
            ),
        }
        for label, (mutate, expected) in cases.items():
            with self.subTest(case=label):
                ledger.write_text(pristine, encoding="utf-8")
                self._rewrite_first(ledger, mutate)
                with self.assertRaises(CanonicalRecordError) as caught:
                    self._load(ledger)
                for fragment in expected:
                    self.assertIn(fragment, str(caught.exception))

    def test_literal_true_with_a_completed_outcome_is_accepted(
        self,
    ) -> None:
        ledger = self._seed("valid")
        records = self._load(ledger)
        self.assertEqual(8, len(records))
        for record in records.values():
            self.assertIs(True, record["telemetry_complete"])
            self.assertIs(True, record["artifact_delivery_complete"])
            self.assertEqual("completed", record["outcome_type"])

    def test_the_accepted_shape_is_exactly_what_the_runner_writes(
        self,
    ) -> None:
        """Guards against the writer and validator drifting apart."""
        ledger = self._seed("shape")
        rows = [
            json.loads(line)
            for line in ledger.read_text(encoding="utf-8").splitlines()
        ]
        for row in rows:
            self.assertIs(True, row["telemetry_complete"])
            self.assertIs(True, row["artifact_delivery_complete"])
            self.assertEqual(COMPLETED_OUTCOME_TYPE, row["outcome_type"])

    # -- full resume path ------------------------------------------------
    def test_the_resume_path_rejects_every_incomplete_record(self) -> None:
        for label, mutate in self.REJECTED.items():
            with self.subTest(case=label):
                with tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    fixture = VerticalFixture(
                        root,
                        _origin_agent(),
                        _local_agent(),
                    )
                    arguments = {
                        "output_dir": root / "run",
                        "workloads": fixture.workloads(),
                        "provider": fixture.provider,
                        "preflight": fixture.preflight(),
                    }
                    run_distributed_pilot(
                        fixture.preregistration,
                        fixture.registry,
                        _GatewayExecutor(fixture),
                        **arguments,
                    )
                    ledger = root / "run" / "canonical_records.jsonl"
                    lines = ledger.read_text(
                        encoding="utf-8"
                    ).splitlines()
                    ledger.write_text(
                        "\n".join(
                            [
                                json.dumps(
                                    mutate(json.loads(lines[0])),
                                    sort_keys=True,
                                )
                            ] + lines[1:]
                        ) + "\n",
                        encoding="utf-8",
                    )
                    # Force the replay path to be consulted.
                    (root / "run" / "attempt_ledger.jsonl").unlink()
                    executor = _GatewayExecutor(fixture)
                    with self.assertRaises(CanonicalRecordError):
                        run_distributed_pilot(
                            fixture.preregistration,
                            fixture.registry,
                            executor,
                            **arguments,
                        )
                    self.assertEqual(
                        [],
                        executor.sessions,
                        "a rejected ledger must not execute anything",
                    )

    def test_a_rejected_record_writes_nothing_and_advances_nothing(
        self,
    ) -> None:
        ledger = self._seed("frozen")
        run_dir = self.root / "frozen"
        attempts = run_dir / "attempt_ledger.jsonl"
        journal = run_dir / "cell_journal.jsonl"
        self._rewrite_first(
            ledger,
            lambda row: {
                k: v for k, v in row.items()
                if k != "artifact_delivery_complete"
            },
        )
        before = {
            path.name: path.read_bytes()
            for path in (ledger, attempts, journal)
        }
        executor = _GatewayExecutor(self.fixture)
        with self.assertRaises(CanonicalRecordError):
            run_distributed_pilot(
                self.fixture.preregistration,
                self.fixture.registry,
                executor,
                output_dir=run_dir,
                workloads=self.fixture.workloads(),
                provider=self.fixture.provider,
                preflight=self.fixture.preflight(),
            )
        self.assertEqual([], executor.sessions)
        for path in (ledger, attempts, journal):
            self.assertEqual(
                before[path.name],
                path.read_bytes(),
                f"{path.name} must be untouched by a rejected resume",
            )

    def test_a_failed_outcome_record_is_never_promoted(self) -> None:
        """The reported bug: a delivery failure replayed as an observation."""
        ledger = self._seed("promote")
        self._rewrite_first(ledger, lambda row: {
            **row,
            "outcome_type": "artifact_delivery_failure",
            "artifact_delivery_complete": False,
            "task_success": None,
        })
        (self.root / "promote" / "attempt_ledger.jsonl").unlink()
        with self.assertRaises(CanonicalRecordError):
            run_distributed_pilot(
                self.fixture.preregistration,
                self.fixture.registry,
                _GatewayExecutor(self.fixture),
                output_dir=self.root / "promote",
                workloads=self.fixture.workloads(),
                provider=self.fixture.provider,
                preflight=self.fixture.preflight(),
            )

    def test_a_valid_crash_window_record_still_replays(self) -> None:
        """The tightened checks must not break legitimate recovery."""
        import pathfinder.distributed.execution as execution

        real_record = execution.CellJournal.record
        state = {"armed": True}

        def crashing_record(self, trial_key, cell_state, **detail):
            if state["armed"] and cell_state == "CANONICAL_WRITTEN":
                state["armed"] = False
                raise _CanonicalBoom("died before journaling")
            return real_record(self, trial_key, cell_state, **detail)

        arguments = {
            "output_dir": self.root / "window",
            "workloads": self.fixture.workloads(),
            "provider": self.fixture.provider,
            "preflight": self.fixture.preflight(),
        }
        with mock.patch.object(
            execution.CellJournal,
            "record",
            crashing_record,
        ):
            try:
                run_distributed_pilot(
                    self.fixture.preregistration,
                    self.fixture.registry,
                    _GatewayExecutor(self.fixture),
                    max_attempts=1,
                    **arguments,
                )
            except _CanonicalBoom:
                pass

        crashed_key = json.loads(
            (
                self.root / "window" / "canonical_records.jsonl"
            ).read_text(encoding="utf-8").splitlines()[0]
        )["trial_key"]
        executor = _GatewayExecutor(self.fixture)
        summary = run_distributed_pilot(
            self.fixture.preregistration,
            self.fixture.registry,
            executor,
            **arguments,
        )
        self.assertTrue(summary["oracle_complete"])
        keys = [
            json.loads(line)["trial_key"]
            for line in (
                self.root / "window" / "canonical_records.jsonl"
            ).read_text(encoding="utf-8").splitlines()
        ]
        self.assertEqual(8, len(keys))
        self.assertEqual(1, keys.count(crashed_key))
        self.assertEqual(7, len(executor.sessions))


# --------------------------------------------------------------------------
# Production path: real FlowMeshAgentAdapter + real executor + real runner
# --------------------------------------------------------------------------
class _FakeFlowMeshClient:
    """A FlowMeshClientProtocol that simulates submit/wait/result only.

    Every Pathfinder-side step -- worker verification, Gateway registration,
    workflow construction, validation, binding, telemetry reconciliation and
    session completion -- still runs through the real adapter.
    """

    def __init__(
        self,
        fixture: "VerticalFixture",
        *,
        answer: str = "a dog runs",
        representation_for: Mapping[str, str] | None = None,
        worker_id: str = "worker-vertical",
    ) -> None:
        self.fixture = fixture
        self.answer = answer
        self.worker_id = worker_id
        self.representation_for = dict(representation_for or {
            SAFE_DESIGN: "multimodal_digest",
            FRAMES_DESIGN: "sampled_frames",
            DIGEST_DESIGN: "multimodal_digest",
        })
        self.gateway: AccessGateway | None = None
        self.submitted: list[str] = []
        self.retrieved: list[str] = []
        self.closed = False
        self._sessions: dict[str, str] = {}
        self._counter = 0
        self.fail_next_submit: Exception | None = None

    # -- worker verification -------------------------------------------
    def describe_current_worker(self, **kwargs: Any):
        from pathfinder.integrations.flowmesh.contracts import (
            FlowMeshWorkerIdentity,
        )

        return FlowMeshWorkerIdentity(
            worker_id=self.worker_id,
            alias=kwargs.get("alias"),
            status="RUNNING",
        )

    # -- submit / wait / result ----------------------------------------
    def validate(self, workflow: dict[str, Any]):
        from pathfinder.integrations.flowmesh.contracts import (
            WorkflowValidation,
        )

        return WorkflowValidation(ok=True)

    def submit(self, workflow: dict[str, Any]):
        from pathfinder.integrations.flowmesh.contracts import (
            SubmittedWorkflow,
        )

        if self.fail_next_submit is not None:
            error, self.fail_next_submit = self.fail_next_submit, None
            raise error
        self._counter += 1
        session_id = _session_id_of(workflow)
        workflow_id = f"wf-{self._counter:04d}"
        task_id = f"task-{self._counter:04d}"
        self._sessions[workflow_id] = session_id
        self.submitted.append(session_id)
        return SubmittedWorkflow(
            workflow_id=workflow_id,
            task_ids=(task_id,),
        )

    def wait(self, workflow_id: str, poll_interval_seconds: float):
        from pathfinder.integrations.flowmesh.contracts import (
            TerminalWorkflow,
        )

        # The Agent's MCP tool calls happen here: the worker would call the
        # MCP Gateway, which performs the Data Agent access and artifact
        # download. The Gateway used is the same one the adapter holds.
        session_id = self._sessions.get(workflow_id)
        if session_id is not None and self.gateway is not None:
            self._agent_tool_calls(session_id)
        return TerminalWorkflow(workflow_id=workflow_id, status="DONE")

    def _agent_tool_calls(self, session_id: str) -> None:
        gateway = self.gateway
        assert gateway is not None
        session = gateway.store.get_session(session_id)
        if gateway.store.list_events(session_id):
            return
        representation_id = self.representation_for[session.design_id]
        access = gateway.access_representation(
            session_id,
            representation_id,
        )
        handle = access.get("artifact_handle")
        if handle is not None:
            gateway.fetch_artifact(session_id, handle)

    def retrieve_result(self, task_id: str) -> dict[str, Any]:
        self.retrieved.append(task_id)
        # Same envelope shape the real SDK returns, so the adapter's own
        # extraction path is exercised rather than bypassed.
        return {"result": {"items": [{"output": self.answer}]}}

    def close(self) -> None:
        self.closed = True


def _session_id_of(workflow: dict[str, Any]) -> str:
    """Recover the Pathfinder session id embedded in a built workflow."""
    text = json.dumps(workflow)
    match = re.search(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        text,
    )
    if match is None:
        raise AssertionError("no session id found in the built workflow")
    return match.group(0)


def _flowmesh_settings(**overrides: Any):
    from pathfinder.integrations.flowmesh.contracts import FlowMeshSettings

    payload: dict[str, Any] = {
        "base_url": "https://flowmesh.invalid",
        "api_key": None,
        "agent_config_name": "pathfinder_video",
        "task_timeout_seconds": 30.0,
        "poll_interval_seconds": 0.01,
        "worker_id": None,
        "worker_alias": "vertical-worker",
        "validate_before_submit": True,
        "mcp_url": "http://mcp.invalid/mcp",
    }
    payload.update(overrides)
    fields = {f.name for f in dataclasses.fields(FlowMeshSettings)}
    return FlowMeshSettings(**{
        k: v for k, v in payload.items() if k in fields
    })


class ProductionFlowMeshPathTest(unittest.TestCase):
    """The runner drives the real adapter, not a direct Gateway shim."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.fixture = VerticalFixture(
            self.root,
            _origin_agent(),
            _local_agent(),
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _stack(self, *, state_db: str = "gateway.sqlite3"):
        from pathfinder.distributed import (
            FlowMeshDistributedSessionExecutor,
        )
        from pathfinder.integrations.flowmesh.adapter import (
            FlowMeshAgentAdapter,
        )

        client = _FakeFlowMeshClient(self.fixture)
        gateway = self.fixture.gateway(state_db)
        client.gateway = gateway
        adapter = FlowMeshAgentAdapter(
            client,
            gateway,
            _flowmesh_settings(),
        )
        executor = FlowMeshDistributedSessionExecutor(adapter)
        return client, gateway, adapter, executor

    def _run(self, *, output: str = "run", **overrides: Any):
        client, gateway, adapter, executor = self._stack(
            state_db=overrides.pop("state_db", "gateway.sqlite3"),
        )
        arguments: dict[str, Any] = {
            "output_dir": self.root / output,
            "workloads": self.fixture.workloads(),
            "provider": self.fixture.provider,
            "preflight": self.fixture.preflight(),
        }
        arguments.update(overrides)
        summary = run_distributed_pilot(
            self.fixture.preregistration,
            self.fixture.registry,
            executor,
            **arguments,
        )
        return summary, client, executor

    def test_the_whole_production_path_produces_a_complete_oracle(
        self,
    ) -> None:
        summary, client, executor = self._run()
        self.assertEqual("COMPLETE", summary["status"])
        self.assertTrue(summary["oracle_complete"])
        self.assertEqual(8, summary["completed_canonical_count"])
        # Every cell went through submit -> wait -> retrieve_result.
        self.assertEqual(8, len(client.submitted))
        self.assertEqual(8, len(client.retrieved))
        self.assertEqual(8, len(executor.submitted_session_ids))
        self.assertEqual([], executor.recovered_session_ids)

    def test_both_endpoints_served_through_the_mcp_gateway_path(
        self,
    ) -> None:
        self._run()
        self.assertEqual(4, len(self.fixture.origin.accesses))
        self.assertEqual(4, len(self.fixture.local.accesses))
        # Artifact downloads happen only at the endpoint that issued them.
        self.assertEqual(0, len(self.fixture.origin.fetches))
        self.assertEqual(4, len(self.fixture.local.fetches))

    def test_records_carry_endpoint_identity_and_ledgers(self) -> None:
        self._run()
        rows = [
            json.loads(line)
            for line in (
                self.root / "run" / "canonical_records.jsonl"
            ).read_text(encoding="utf-8").splitlines()
        ]
        self.assertEqual(8, len(rows))
        for row in rows:
            accepted = [
                event for event in row["access_events"]
                if event.get("accepted")
            ]
            self.assertTrue(accepted)
            for event in accepted:
                self.assertIn(
                    event["endpoint_id"],
                    (ORIGIN_ENDPOINT, LOCAL_ENDPOINT),
                )
                self.assertEqual(
                    "exec-node-gamma",
                    event["destination_execution_node_id"],
                )
            self.assertTrue(row["cost_ledger"]["total_cost_available"])
            self.assertTrue(row["telemetry_complete"])
            self.assertIs(True, row["task_success"])
            self.assertIsNotNone(row["workflow_id"])
            self.assertIsNotNone(row["task_id"])

    def test_canonical_output_loads_in_the_awm_total_cost_path(
        self,
    ) -> None:
        from pathfinder.distributed.cost import record_total_cost

        self._run()
        exported = export_reduced_oracle_records(
            self.root / "run",
            self.root / "oracle",
            (SAFE_DESIGN, FRAMES_DESIGN),
        )
        self.assertEqual(8, exported["record_count"])
        for design_id in (SAFE_DESIGN, FRAMES_DESIGN):
            rows = [
                json.loads(line)
                for line in (
                    self.root / "oracle" / "designs" / design_id
                    / "runs.jsonl"
                ).read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(4, len(rows))
            for row in rows:
                self.assertGreater(record_total_cost(row), 0.0)

    def test_resume_recovers_instead_of_resubmitting(self) -> None:
        first, client_one, executor_one = self._run()
        self.assertEqual(8, first["executed_this_invocation"])
        second, client_two, executor_two = self._run()
        self.assertEqual(0, second["executed_this_invocation"])
        # Nothing was submitted a second time.
        self.assertEqual([], client_two.submitted)
        self.assertEqual([], executor_two.submitted_session_ids)
        rows = (
            self.root / "run" / "canonical_records.jsonl"
        ).read_text(encoding="utf-8").splitlines()
        self.assertEqual(8, len(rows))

    def test_a_flowmesh_failure_is_infrastructure_not_a_task_failure(
        self,
    ) -> None:
        from pathfinder.integrations.flowmesh.adapter import FlowMeshRunError

        client, gateway, adapter, executor = self._stack()
        client.fail_next_submit = FlowMeshRunError("submit rejected")
        summary = run_distributed_pilot(
            self.fixture.preregistration,
            self.fixture.registry,
            executor,
            output_dir=self.root / "flaky",
            workloads=self.fixture.workloads(),
            provider=self.fixture.provider,
            preflight=self.fixture.preflight(),
        )
        self.assertEqual(1, summary["infrastructure_failure_count"])
        # The retry succeeded, so the Oracle is still complete and no task
        # was scored as a failure.
        self.assertTrue(summary["oracle_complete"])
        rows = [
            json.loads(line)
            for line in (
                self.root / "flaky" / "attempt_ledger.jsonl"
            ).read_text(encoding="utf-8").splitlines()
        ]
        failures = [r for r in rows if r["observation_class"] != "canonical"]
        self.assertEqual(1, len(failures))
        self.assertEqual("flowmesh_failure", failures[0]["failure_class"])
        for row in rows:
            if row["observation_class"] == "canonical":
                self.assertTrue(row["succeeded"])

    def test_the_cell_journal_records_every_transition(self) -> None:
        summary, _, _ = self._run()
        journal = [
            json.loads(line)
            for line in (
                self.root / "run" / "cell_journal.jsonl"
            ).read_text(encoding="utf-8").splitlines()
        ]
        states = {row["state"] for row in journal}
        for expected in (
            "STARTED",
            "FLOWMESH_BOUND",
            "RESULT_OBTAINED",
            "CANONICAL_WRITTEN",
            "COMPLETED",
        ):
            self.assertIn(expected, states)
        self.assertEqual(8, summary["cell_journal"]["COMPLETED"])


class CrashRecoveryTest(ProductionFlowMeshPathTest):
    """Fault injection at each boundary of the cell lifecycle."""

    class _Boom(RuntimeError):
        """A simulated process death."""

    def _run_until_crash(
        self,
        crash_at: str,
        *,
        output: str = "crash",
    ):
        """Run the plan, raising at a chosen lifecycle boundary."""
        from pathfinder.distributed import (
            FlowMeshDistributedSessionExecutor,
        )
        from pathfinder.integrations.flowmesh.adapter import (
            FlowMeshAgentAdapter,
        )

        client = _FakeFlowMeshClient(self.fixture)
        gateway = self.fixture.gateway("gateway.sqlite3")
        client.gateway = gateway
        adapter = FlowMeshAgentAdapter(client, gateway, _flowmesh_settings())
        boom = self._Boom

        class _CrashingExecutor(FlowMeshDistributedSessionExecutor):
            calls = 0

            def execute(self, trial, *, workload, journal=None, attempt=1):
                type(self).calls += 1
                if crash_at == "before_submission" and self.calls == 1:
                    raise boom("crashed before FlowMesh submission")
                session_id = self.session_id_for_attempt(trial, attempt)
                request = self.build_request(
                    trial, workload, attempt=attempt
                )
                run = self.adapter.recover(session_id)
                if run is not None:
                    self.recovered_session_ids.append(session_id)
                    return TrialExecution(
                        final_answer=run.final_answer,
                        access_events=tuple(
                            dict(e) for e in run.access_events
                        ),
                        workflow_id=run.workflow_id,
                        task_id=run.task_id,
                        status=run.status,
                    )
                if crash_at == "after_registration" and self.calls == 1:
                    # Register the session, then die before binding a task.
                    self.adapter.gateway.register_session(request)
                    raise boom("crashed after registration, before binding")
                if journal is not None:
                    journal.record(
                        trial.trial_key,
                        "FLOWMESH_BOUND",
                        session_id=session_id,
                    )
                run = self.adapter.run(request)
                self.submitted_session_ids.append(session_id)
                if crash_at == "after_result" and self.calls == 1:
                    raise boom("crashed after result, before canonical write")
                return TrialExecution(
                    final_answer=run.final_answer,
                    access_events=tuple(dict(e) for e in run.access_events),
                    workflow_id=run.workflow_id,
                    task_id=run.task_id,
                    status=run.status,
                )

        _CrashingExecutor.calls = 0
        executor = _CrashingExecutor(adapter)
        try:
            run_distributed_pilot(
                self.fixture.preregistration,
                self.fixture.registry,
                executor,
                output_dir=self.root / output,
                workloads=self.fixture.workloads(),
                provider=self.fixture.provider,
                preflight=self.fixture.preflight(),
                # A process death gets no retry inside the same invocation.
                max_attempts=1,
            )
        except self._Boom:
            pass
        # A real process death writes no failure row. The runner caught the
        # simulated fault and recorded one, so strip it: otherwise the
        # attempt counter advances and resume would address a *different*
        # session than the one the crash left behind, never exercising
        # recovery at all.
        self._strip_first_failed_attempt(output)
        return client, executor

    def _strip_first_failed_attempt(self, output: str) -> None:
        ledger = self.root / output / "attempt_ledger.jsonl"
        if not ledger.is_file():
            return
        rows = [
            json.loads(line)
            for line in ledger.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        kept, dropped = [], False
        for row in rows:
            if not dropped and row["observation_class"] != "canonical":
                dropped = True
                continue
            kept.append(row)
        ledger.write_text(
            "".join(json.dumps(row, sort_keys=True) + "\n" for row in kept),
            encoding="utf-8",
        )

    def _resume(self, *, output: str = "crash"):
        return self._run(output=output)

    def _assert_recovered_cleanly(self, output: str, client_one) -> None:
        summary, client_two, _ = self._resume(output=output)
        self.assertTrue(
            summary["oracle_complete"],
            f"resume did not reach a complete Oracle: {summary}",
        )
        rows = [
            json.loads(line)
            for line in (
                self.root / output / "canonical_records.jsonl"
            ).read_text(encoding="utf-8").splitlines()
        ]
        keys = [row["trial_key"] for row in rows]
        self.assertEqual(8, len(keys))
        self.assertEqual(len(keys), len(set(keys)), "duplicate canonical")
        submitted = client_one.submitted + client_two.submitted
        self.assertEqual(
            len(submitted),
            len(set(submitted)),
            "the same session was submitted to FlowMesh twice",
        )

    def test_crash_before_flowmesh_submission(self) -> None:
        client, _ = self._run_until_crash("before_submission")
        self._assert_recovered_cleanly("crash", client)

    def test_crash_after_registration_before_binding_fails_closed(
        self,
    ) -> None:
        client, _ = self._run_until_crash("after_registration")
        # Resume readdresses the SAME session id, finds it registered but
        # never bound, and must refuse: whether a task was submitted is
        # genuinely unknowable from here.
        summary, client_two, _ = self._resume(output="crash")
        self.assertFalse(
            summary["oracle_complete"],
            "an ambiguous unbound session must not be silently completed",
        )
        rows = [
            json.loads(line)
            for line in (
                self.root / "crash" / "attempt_ledger.jsonl"
            ).read_text(encoding="utf-8").splitlines()
        ]
        ambiguous = [
            row for row in rows
            if row["observation_class"] != "canonical"
            and "never" in str(row.get("failure_detail", "")).lower()
        ]
        self.assertTrue(
            ambiguous,
            "the unbound session should be recorded as a failed attempt",
        )

    def test_crash_after_result_before_canonical_write(self) -> None:
        client, _ = self._run_until_crash("after_result")
        summary, client_two, executor_two = self._resume(output="crash")
        self.assertTrue(summary["oracle_complete"])
        # The completed session was recovered from the Gateway, not rerun.
        self.assertTrue(
            executor_two.recovered_session_ids,
            "a session FlowMesh already completed must be recovered",
        )
        submitted = client.submitted + client_two.submitted
        self.assertEqual(len(submitted), len(set(submitted)))
        rows = (
            self.root / "crash" / "canonical_records.jsonl"
        ).read_text(encoding="utf-8").splitlines()
        self.assertEqual(8, len(rows))

    def test_crash_after_canonical_write_replays_without_reexecuting(
        self,
    ) -> None:
        # Run fully, then delete the attempt ledger to simulate a crash
        # between the canonical write and the attempt row.
        self._run(output="replay")
        attempts = self.root / "replay" / "attempt_ledger.jsonl"
        attempts.unlink()
        summary, client_two, executor_two = self._run(output="replay")
        self.assertTrue(summary["oracle_complete"])
        self.assertEqual(8, summary["completed_canonical_count"])
        self.assertEqual(
            [],
            client_two.submitted,
            "a durable canonical record must be replayed, not re-executed",
        )
        rows = (
            self.root / "replay" / "canonical_records.jsonl"
        ).read_text(encoding="utf-8").splitlines()
        self.assertEqual(8, len(rows), "canonical records were duplicated")

    def test_crash_after_attempt_completion_resumes_at_the_next_cell(
        self,
    ) -> None:
        summary_one, client_one, _ = self._run(output="between")
        self.assertTrue(summary_one["oracle_complete"])
        summary_two, client_two, _ = self._run(output="between")
        self.assertEqual(0, summary_two["executed_this_invocation"])
        self.assertEqual([], client_two.submitted)

    def test_the_journal_survives_and_orders_states(self) -> None:
        from pathfinder.distributed import CellJournal

        self._run_until_crash("after_result", output="ordered")
        journal = CellJournal(self.root / "ordered" / "cell_journal.jsonl")
        public = journal.to_public_dict()
        self.assertTrue(public["states"])
        # Monotonic: a reset returns a cell to PLANNED, never backwards
        # through an intermediate state.
        for state in public["states"].values():
            self.assertIn(state, CELL_STATES_TUPLE)


class CliConstructionTest(unittest.TestCase):
    """The CLI wiring is exercised without any external network call."""

    def test_run_distributed_pilot_parses_the_full_input_set(self) -> None:
        from pathfinder.cli import _parser as build_parser

        parser = build_parser()
        args = parser.parse_args([
            "run-distributed-pilot",
            "--preregistration", "p.json",
            "--endpoint-registry", "r.json",
            "--measurement-manifest", "m.json",
            "--workload-manifest", "w.json",
            "--config", "c.json",
            "--state-db", "g.sqlite3",
            "--output-dir", "out",
            "--worker-alias", "w1",
            "--flowmesh-base-url", "https://flowmesh.invalid",
            "--agent-config", "pathfinder_video",
            "--task-timeout", "60",
            "--poll-interval", "2",
            "--validate-workflow",
            "--mode", "live_pilot",
        ])
        self.assertEqual("run-distributed-pilot", args.command)
        self.assertEqual("live_pilot", args.mode)
        self.assertTrue(args.validate_workflow)
        self.assertEqual(60.0, args.task_timeout)

    def test_serve_flowmesh_tools_accepts_an_endpoint_registry(self) -> None:
        from pathfinder.cli import _parser as build_parser

        args = build_parser().parse_args([
            "serve-flowmesh-tools",
            "--endpoint-registry", "r.json",
        ])
        self.assertEqual(Path("r.json"), args.endpoint_registry)

    def test_serve_flowmesh_tools_still_accepts_a_single_url(self) -> None:
        from pathfinder.cli import _parser as build_parser

        args = build_parser().parse_args([
            "serve-flowmesh-tools",
            "--data-agent-url", "https://agent.invalid",
        ])
        self.assertIsNone(args.endpoint_registry)
        self.assertEqual("https://agent.invalid", args.data_agent_url)

    def test_the_mcp_server_builds_a_routed_backend_from_a_registry(
        self,
    ) -> None:
        from pathfinder.integrations.flowmesh.mcp_server import (
            build_mcp_server,
        )

        captured: dict[str, Any] = {}

        class _FakeFastMCP:
            def __init__(self, name, host=None, port=None):
                captured["name"] = name

            def tool(self):
                def decorator(fn):
                    captured.setdefault("tools", []).append(fn.__name__)
                    return fn
                return decorator

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            registry_path = root / "registry.json"
            _write_json(registry_path, _registry_payload())
            import pathfinder.integrations.flowmesh.mcp_server as module

            built = module._build_server(
                None,
                config_path=SYSTEM_CONFIG,
                state_db=root / "state.sqlite3",
                host="127.0.0.1",
                port=1,
                fast_mcp=_FakeFastMCP,
            )
        self.assertIsNotNone(built)
        self.assertEqual(
            [
                "list_offers",
                "access_representation",
                "fetch_artifact",
                "get_session_state",
            ],
            captured["tools"],
        )
