from __future__ import annotations

import copy
import inspect
import json
import os
import sqlite3
import sys
import tempfile
import types
import unittest
from contextlib import closing
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping
from unittest.mock import patch

from pathfinder.cli import _parser as cli_parser
from pathfinder.config import load_config
from pathfinder.data_agent_client import (
    DataAgentAccessRequest,
    DataAgentAccessResult,
    DataAgentAccessTelemetry,
    DataAgentFetchedArtifact,
    DataAgentPayload,
    DataAgentTelemetryQuiescenceError,
)
from pathfinder.integrations.flowmesh import adapter as adapter_module
from pathfinder.integrations.flowmesh.adapter import (
    FlowMeshAgentAdapter,
    FlowMeshPinningError,
    FlowMeshRunError,
    FlowMeshWorkflowFailureError,
    extract_agent_answer,
)
from pathfinder.integrations.flowmesh.client import (
    SdkFlowMeshClient,
    TaskDetailUnavailableError,
    WorkerResolutionError,
)
from pathfinder.integrations.flowmesh.contracts import (
    FlowMeshAgentRunRequest,
    FlowMeshSettings,
    FlowMeshWorkerIdentity,
    SubmittedWorkflow,
    TerminalWorkflow,
    WorkflowValidation,
)
from pathfinder.integrations.flowmesh.preflight import (
    PREFLIGHT_SCHEMA_VERSION,
    WorkerPreflightError,
    preflight_flowmesh_worker,
)
from pathfinder.integrations.flowmesh.redaction import (
    redact_secrets,
    sanitize_endpoint,
)
from pathfinder.integrations.flowmesh.data_agent_backend import (
    RemoteDataAgentBackend,
)
from pathfinder.integrations.flowmesh.gateway import (
    AccessGateway,
    ArtifactHandleError,
    EmulatedRepresentationBackend,
    SQLiteSessionStore,
)
from pathfinder.integrations.flowmesh.mcp_server import build_mcp_server
from pathfinder.integrations.flowmesh.workflow import (
    PATHFINDER_GRAPH_NODE_NAME,
    build_agent_workflow,
    workflow_selected_worker,
)


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "configs" / "minimal_system.json"
DEFAULT_FLOWMESH_SOURCE = "/tmp/fm-v018rc1"


def agent_spec_of(workflow: Mapping[str, Any]) -> Mapping[str, Any]:
    """Return the single Agent task spec inside the one-node graph."""
    nodes = workflow["spec"]["graph"]["nodes"]
    if len(nodes) != 1:
        raise AssertionError(
            f"expected exactly one graph node, found {len(nodes)}"
        )
    return nodes[0]["spec"]


class FakeFlowMeshClient:
    """A Root that answers worker questions and never mutates anything.

    ``workers_by_alias`` and ``workers_by_id`` both model the same Root view:
    an entry means the Root reports that worker as current. An ID absent from
    ``workers_by_id`` is therefore invisible to this Root, which is exactly
    the state a pin must refuse to run against.
    """

    def __init__(
        self,
        *,
        workers_by_alias: Mapping[str, list[str]] | None = None,
        workers_by_id: Mapping[str, list[str]] | None = None,
        validation: WorkflowValidation | None = None,
        terminal: TerminalWorkflow | None = None,
        task_failure: dict[str, Any] | None = None,
    ) -> None:
        self.submitted_workflow: Mapping[str, Any] | None = None
        self.validated_workflows: list[Mapping[str, Any]] = []
        self.alias_lookups: list[str] = []
        self.worker_id_lookups: list[str] = []
        self.task_failure_lookups: list[str] = []
        self._workers_by_alias = dict(workers_by_alias or {})
        self._workers_by_id = dict(workers_by_id or {})
        self._validation = validation or WorkflowValidation(ok=True)
        self._terminal = terminal
        self._task_failure = task_failure

    def submit(self, workflow: Mapping[str, Any]) -> SubmittedWorkflow:
        self.submitted_workflow = workflow
        return SubmittedWorkflow(
            workflow_id="wfl-test",
            task_ids=("tsk-test",),
        )

    def validate(self, workflow: Mapping[str, Any]) -> WorkflowValidation:
        self.validated_workflows.append(workflow)
        return self._validation

    def describe_current_worker(
        self,
        *,
        worker_id: str | None = None,
        alias: str | None = None,
    ) -> FlowMeshWorkerIdentity:
        if (worker_id is None) == (alias is None):
            raise ValueError("exactly one selector is required")
        if alias is not None:
            self.alias_lookups.append(alias)
            selector, value = "alias", alias
            matches = self._workers_by_alias.get(alias, [])
        else:
            self.worker_id_lookups.append(worker_id)
            selector, value = "ID", worker_id
            matches = self._workers_by_id.get(worker_id, [])
        if not matches:
            raise WorkerResolutionError(
                f"FlowMesh worker {selector} '{value}' matched no worker"
            )
        if len(matches) > 1:
            raise WorkerResolutionError(
                f"FlowMesh worker {selector} '{value}' matched "
                f"{len(matches)} workers"
            )
        return FlowMeshWorkerIdentity(
            worker_id=matches[0],
            alias=alias,
            status="ready",
            namespace="pathfinder-namespace",
            cluster="pathfinder-cluster",
            node_alias="pathfinder-node",
        )

    def resolve_worker_alias(self, alias: str) -> str:
        return self.describe_current_worker(alias=alias).worker_id

    def describe_task_failure(self, task_id: str) -> dict[str, Any] | None:
        self.task_failure_lookups.append(task_id)
        return self._task_failure

    def wait(
        self,
        workflow_id: str,
        poll_interval_seconds: float,
    ) -> TerminalWorkflow:
        if self._terminal is not None:
            return self._terminal
        return TerminalWorkflow(workflow_id=workflow_id, status="DONE")

    def retrieve_result(self, task_id: str) -> dict[str, Any]:
        return {
            "task_id": task_id,
            "result": {
                "ok": True,
                "items": [
                    {
                        "index": 0,
                        "output": "The car enters at 00:17.",
                    }
                ],
            },
        }


class StubDataAgentClient:
    """A Data Agent whose transfer telemetry settles after N polls.

    ``settles_after`` polls report an in-flight transfer with zeroed figures;
    every later poll reports the committed summary. ``settles_after=None``
    never settles, standing in for a stalled or crashed transfer.
    """

    def __init__(self, *, settles_after: int | None = 0) -> None:
        self.settles_after = settles_after
        self.poll_counts: dict[str, int] = {}
        self.quiescence_requests: list[bool] = []
        self.fetch_requests: list[DataAgentAccessRequest] = []

    def access(
        self,
        request: DataAgentAccessRequest,
    ) -> DataAgentAccessResult:
        return DataAgentAccessResult(
            access_id=request.access_id,
            payload=DataAgentPayload(
                kind="artifact_uri",
                media_type="application/json",
                value=f"http://data-agent.invalid/v1/artifacts/{request.access_id}",
                sha256=sha256(
                    b'{"timestamp_seconds":17,"event":"red car"}'
                ).hexdigest(),
            ),
            service_latency_ms=40.0,
            client_round_trip_ms=55.0,
            realized_cost=0.5,
            bytes_read=4096,
            location="remote_digest_service",
            object_id=request.object_id,
            object_catalog_version="flowmesh-catalog-v1",
        )

    def fetch_artifact(
        self,
        request: DataAgentAccessRequest,
    ) -> DataAgentFetchedArtifact:
        self.fetch_requests.append(request)
        raw = b'{"timestamp_seconds":17,"event":"red car"}'
        return DataAgentFetchedArtifact(
            access_id=request.access_id,
            media_type="application/json",
            content={"timestamp_seconds": 17, "event": "red car"},
            size_bytes=len(raw),
            sha256=sha256(raw).hexdigest(),
        )

    def get_access_telemetry(
        self,
        access_id: str,
        *,
        wait_for_quiescence: bool = False,
        quiescence_timeout_seconds: float = 5.0,
    ) -> DataAgentAccessTelemetry:
        self.quiescence_requests.append(wait_for_quiescence)
        polls = self.poll_counts.get(access_id, 0)
        self.poll_counts[access_id] = polls + 1
        settled = (
            self.settles_after is not None and polls >= self.settles_after
        )
        if not wait_for_quiescence and not settled:
            in_flight = 1
        elif not settled:
            # A waiting caller must fail closed rather than see this.
            raise DataAgentTelemetryQuiescenceError(
                access_id,
                1,
                quiescence_timeout_seconds,
                DataAgentAccessTelemetry(
                    access_id=access_id,
                    object_id="video-001",
                    representation_id="multimodal_digest",
                    object_catalog_version="flowmesh-catalog-v1",
                    download_request_count=0,
                    completed_request_count=0,
                    full_download_count=0,
                    bytes_sent=0,
                    transfer_latency_ms=0.0,
                    in_flight_request_count=1,
                    server_reported_complete=False,
                ),
            )
        else:
            in_flight = 0
        return DataAgentAccessTelemetry(
            access_id=access_id,
            object_id="video-001",
            representation_id="multimodal_digest",
            object_catalog_version="flowmesh-catalog-v1",
            download_request_count=1 if settled else 0,
            completed_request_count=1 if settled else 0,
            full_download_count=1 if settled else 0,
            bytes_sent=4096 if settled else 0,
            transfer_latency_ms=31.5 if settled else 0.0,
            latest_completed_at=456.0 if settled else None,
            in_flight_request_count=in_flight,
            # This fake stands in for a current Data Agent, so it always
            # answers the completeness question rather than staying silent.
            server_reported_complete=(in_flight == 0),
        )


class AccessingFlowMeshClient(FakeFlowMeshClient):
    """Simulates an Agent that uses the session access tool mid-workflow.

    The session ID is read back out of the submitted workflow, which is how
    the real Agent learns it.
    """

    def __init__(self, gateway: AccessGateway) -> None:
        super().__init__()
        self.gateway = gateway
        self.access_responses: list[dict[str, Any]] = []
        self.fetch_responses: list[dict[str, Any]] = []

    def wait(
        self,
        workflow_id: str,
        poll_interval_seconds: float,
    ) -> TerminalWorkflow:
        assert self.submitted_workflow is not None
        annotations = self.submitted_workflow["metadata"]["annotations"]
        session_id = annotations["custom"]["pathfinder_session_id"]
        access = self.gateway.access_representation(
            session_id,
            "multimodal_digest",
        )
        self.access_responses.append(access)
        self.fetch_responses.append(
            self.gateway.fetch_artifact(
                session_id,
                access["artifact_handle"],
            )
        )
        return super().wait(workflow_id, poll_interval_seconds)


class StubWorker:
    def __init__(self, worker_id: str) -> None:
        self.id = worker_id


class RecordingFastMCP:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.tools: dict[str, Any] = {}

    def tool(self) -> Any:
        def register(function: Any) -> Any:
            self.tools[function.__name__] = function
            return function

        return register


class RecordingWorkersApi:
    """Records the keyword filters the SDK adapter sends."""

    def __init__(self, workers: list[StubWorker]) -> None:
        self._workers = workers
        self.calls: list[dict[str, Any]] = []

    def list(self, **filters: Any) -> list[StubWorker]:
        self.calls.append(dict(filters))
        return self._workers


class StubSdk:
    def __init__(self, workers: list[StubWorker]) -> None:
        self.workers = RecordingWorkersApi(workers)


class SdkWorkerResolutionTest(unittest.TestCase):
    """Alias resolution must consider only workers FlowMesh calls current."""

    def build(self, worker_ids: list[str]) -> SdkFlowMeshClient:
        # Bypass __init__: it imports the optional FlowMesh SDK, which is not
        # installed here, and the behaviour under test is the query it sends.
        client = SdkFlowMeshClient.__new__(SdkFlowMeshClient)
        client._client = StubSdk([StubWorker(w) for w in worker_ids])
        return client

    def test_alias_lookup_filters_out_stale_workers(self) -> None:
        client = self.build(["wkr-16"])
        self.assertEqual("wkr-16", client.resolve_worker_alias("pathfinder-a"))
        # The regression this guards: without stale=False a restarted worker
        # is returned twice under one alias, or the single ID handed back is
        # a dead registration the scheduler will never dispatch to.
        self.assertEqual(
            [{"alias": "pathfinder-a", "stale": False}],
            client._client.workers.calls,
        )

    def test_no_current_worker_fails_closed(self) -> None:
        client = self.build([])
        with self.assertRaises(WorkerResolutionError) as context:
            client.resolve_worker_alias("pathfinder-a")
        self.assertIn("matched no current worker", str(context.exception))
        self.assertEqual(
            [{"alias": "pathfinder-a", "stale": False}],
            client._client.workers.calls,
        )

    def test_multiple_current_workers_fail_closed(self) -> None:
        client = self.build(["wkr-16", "wkr-17"])
        with self.assertRaises(WorkerResolutionError) as context:
            client.resolve_worker_alias("pathfinder-a")
        message = str(context.exception)
        self.assertIn("matched 2 current workers", message)
        self.assertIn("wkr-16", message)
        self.assertIn("wkr-17", message)

    def test_exact_worker_id_queries_the_root_with_the_same_filter(
        self,
    ) -> None:
        client = self.build(["wkr-000"])
        identity = client.describe_current_worker(worker_id="wkr-000")
        self.assertEqual("wkr-000", identity.worker_id)
        self.assertEqual(
            [{"worker_id": "wkr-000", "stale": False}],
            client._client.workers.calls,
        )

    def test_exact_worker_id_absent_from_the_root_fails_closed(self) -> None:
        client = self.build([])
        with self.assertRaises(WorkerResolutionError) as context:
            client.describe_current_worker(worker_id="wkr-000")
        self.assertIn("matched no current worker", str(context.exception))

    def test_root_ignoring_the_id_filter_is_not_read_as_a_match(self) -> None:
        """A Root that returns some other worker has not verified this pin."""
        client = self.build(["wkr-99"])
        with self.assertRaises(WorkerResolutionError) as context:
            client.describe_current_worker(worker_id="wkr-000")
        self.assertIn("matched no current worker", str(context.exception))

    def test_exactly_one_selector_is_required(self) -> None:
        client = self.build(["wkr-16"])
        with self.assertRaises(ValueError):
            client.describe_current_worker()
        with self.assertRaises(ValueError):
            client.describe_current_worker(worker_id="wkr-16", alias="pf")
        self.assertEqual([], client._client.workers.calls)


class DeployedSdkCompatibilityTest(unittest.TestCase):
    """Pin what this integration assumes about flowmesh-sdk 0.1.8rc1.

    Skipped when the SDK is absent so the suite stays runnable without it.
    """

    @classmethod
    def setUpClass(cls) -> None:
        try:
            import flowmesh
        except ImportError as exc:  # pragma: no cover - environment dependent
            raise unittest.SkipTest(f"flowmesh SDK is not installed: {exc}")
        cls.flowmesh = flowmesh

    def test_pinned_sdk_version_matches_the_deployed_control_plane(
        self,
    ) -> None:
        self.assertEqual(
            "0.1.8rc1",
            getattr(type(self).flowmesh, "__version__", None),
        )

    def test_workers_list_accepts_the_filters_this_client_sends(self) -> None:
        from flowmesh.resources.workers import Workers

        parameters = inspect.signature(Workers.list).parameters
        # Both are sent by describe_current_worker; worker_id becomes the
        # `id` query parameter on this version.
        self.assertIn("worker_id", parameters)
        self.assertIn("alias", parameters)
        self.assertIn("stale", parameters)

    def test_worker_model_exposes_only_the_metadata_we_report(self) -> None:
        from flowmesh.models.workers import Worker

        fields = Worker.model_fields
        for name in (
            "id",
            "alias",
            "status",
            "namespace",
            "cluster",
            "node_alias",
        ):
            self.assertIn(name, fields)
        # Present on the model and deliberately never read back: these can
        # carry deployment secrets.
        self.assertIn("env", fields)

    def test_task_failure_detail_is_available_through_a_public_method(
        self,
    ) -> None:
        from flowmesh.models.tasks import TaskInfo
        from flowmesh.resources.tasks import Tasks

        self.assertTrue(callable(getattr(Tasks, "retrieve", None)))
        self.assertFalse(Tasks.retrieve.__name__.startswith("_"))
        for name in (
            "status",
            "error",
            "last_error",
            "attempts",
            "max_attempts",
            "assigned_worker",
            "last_failed_worker",
        ):
            self.assertIn(name, TaskInfo.model_fields)

    def test_terminal_workflow_exposes_the_task_lists_we_report(self) -> None:
        from flowmesh.models.workflows import Workflow

        for name in (
            "status",
            "failed_tasks",
            "cancelled_tasks",
            "dispatched_tasks",
        ):
            self.assertIn(name, Workflow.model_fields)

    def test_client_exposes_workers_and_tasks_as_public_resources(
        self,
    ) -> None:
        source = inspect.getsource(type(self).flowmesh.FlowMesh.__init__)
        self.assertIn("self.workers", source)
        self.assertIn("self.tasks", source)


class WorkerListFallbackTest(unittest.TestCase):
    """An SDK without the worker_id filter must not break pin verification."""

    def build(self, workers: list[StubWorker], *, reject_kwargs: bool):
        client = SdkFlowMeshClient.__new__(SdkFlowMeshClient)
        client._client = StubSdk(workers)
        if reject_kwargs:
            api = client._client.workers
            original = api.list

            def strict(**filters: Any) -> list[StubWorker]:
                if "worker_id" in filters or "alias" in filters:
                    raise TypeError(
                        "list() got an unexpected keyword argument"
                    )
                return original(**filters)

            api.list = strict  # type: ignore[method-assign]
        return client

    def test_exact_id_falls_back_to_local_filtering(self) -> None:
        client = self.build(
            [StubWorker("wkr-000"), StubWorker("wkr-001")],
            reject_kwargs=True,
        )
        identity = client.describe_current_worker(worker_id="wkr-000")
        self.assertEqual("wkr-000", identity.worker_id)
        # The unnarrowed current-worker list was still requested with the
        # stale filter, and the ID was matched locally.
        self.assertEqual(
            [{"stale": False}],
            client._client.workers.calls[-1:],
        )

    def test_fallback_still_fails_closed_on_an_absent_id(self) -> None:
        client = self.build([StubWorker("wkr-999")], reject_kwargs=True)
        with self.assertRaises(WorkerResolutionError) as context:
            client.describe_current_worker(worker_id="wkr-000")
        self.assertIn("matched no current worker", str(context.exception))

    def test_fallback_filters_aliases_locally(self) -> None:
        client = self.build([StubWorker("wkr-000")], reject_kwargs=True)
        client._client.workers._workers[0].alias = "pf"
        self.assertEqual(
            "wkr-000",
            client.describe_current_worker(alias="pf").worker_id,
        )
        with self.assertRaises(WorkerResolutionError):
            client.describe_current_worker(alias="other")

    def test_task_detail_without_a_public_method_fails_explicitly(
        self,
    ) -> None:
        client = self.build([], reject_kwargs=False)
        with self.assertRaises(TaskDetailUnavailableError) as context:
            client.describe_task_failure("tsk-synthetic-1")
        self.assertIn("no public tasks.retrieve", str(context.exception))


class EndpointSanitizationTest(unittest.TestCase):
    def test_user_info_path_and_query_are_never_echoed(self) -> None:
        self.assertEqual(
            "https://root.invalid:9443",
            sanitize_endpoint(
                "https://user:not-a-real-pat-fixture@root.invalid:9443/api/v1?token=abc"
            ),
        )

    def test_scheme_and_host_survive_without_a_port(self) -> None:
        self.assertEqual(
            "http://root.invalid",
            sanitize_endpoint("http://root.invalid/api"),
        )

    def test_unparsable_endpoint_is_reported_rather_than_guessed(
        self,
    ) -> None:
        self.assertEqual("<unparsable-endpoint>", sanitize_endpoint("nonsense"))

    def test_bearer_tokens_and_signed_urls_are_redacted(self) -> None:
        redacted = redact_secrets(
            "Authorization: Bearer not-a-real-pat-fixture while fetching "
            "https://root.invalid/results?sig=private"
        )
        self.assertNotIn("not-a-real-pat-fixture", redacted)
        self.assertNotIn("sig=private", redacted)
        self.assertIn("<redacted>", redacted)

    def test_configured_api_key_value_is_redacted(self) -> None:
        with patch.dict("os.environ", {"FLOWMESH_API_KEY": "not-a-real-key-fixture"}):
            self.assertNotIn(
                "not-a-real-key-fixture",
                redact_secrets("connect failed for not-a-real-key-fixture"),
            )


class WorkerPreflightTest(unittest.TestCase):
    """The preflight is read-only: it may only ask the Root questions."""

    def settings(self, **overrides: Any) -> FlowMeshSettings:
        return FlowMeshSettings(
            base_url="https://user:not-a-real-pat-fixture@root.invalid:9443/api",
            **overrides,
        )

    def test_alias_matching_one_current_worker_reports_sanitized_identity(
        self,
    ) -> None:
        client = FakeFlowMeshClient(workers_by_alias={"pf": ["wkr-000"]})
        payload = preflight_flowmesh_worker(
            client,
            self.settings(worker_alias="pf"),
        )

        self.assertEqual(PREFLIGHT_SCHEMA_VERSION, payload["schema_version"])
        self.assertEqual("ok", payload["status"])
        self.assertEqual(
            {"kind": "worker_alias", "value": "pf"},
            payload["requested_pin"],
        )
        self.assertEqual(
            {
                "worker_id": "wkr-000",
                "alias": "pf",
                "status": "ready",
                "namespace": "pathfinder-namespace",
                "cluster": "pathfinder-cluster",
                "node_alias": "pathfinder-node",
            },
            payload["worker"],
        )
        self.assertEqual(
            "https://root.invalid:9443",
            payload["endpoint"]["root_endpoint"],
        )
        encoded = json.dumps(payload)
        self.assertNotIn("not-a-real-pat-fixture", encoded)
        self.assertNotIn("/api", payload["endpoint"]["root_endpoint"])
        # Nothing was submitted, validated, or registered.
        self.assertIsNone(client.submitted_workflow)
        self.assertEqual([], client.validated_workflows)
        self.assertFalse(payload["checks"]["workflow_submitted"])
        self.assertFalse(payload["checks"]["gateway_session_registered"])
        self.assertFalse(payload["checks"]["mutating_api_called"])

    def test_root_alignment_is_reported_as_verified_and_node_as_unproven(
        self,
    ) -> None:
        client = FakeFlowMeshClient(workers_by_alias={"pf": ["wkr-000"]})
        payload = preflight_flowmesh_worker(
            client,
            self.settings(worker_alias="pf"),
        )
        self.assertTrue(payload["checks"]["root_visibility_verified"])
        self.assertFalse(payload["checks"]["exact_worker_id_bypassed_root"])
        self.assertIn(
            "node_server_and_root_agree_on_this_worker",
            payload["not_verified"],
        )
        self.assertIn(
            "worker_result_upload_endpoint_matches_this_root",
            payload["not_verified"],
        )
        self.assertIn("unverified", payload["interpretation"])
        self.assertTrue(payload["operator_next_steps"])

    def test_alias_matching_no_worker_fails_closed(self) -> None:
        client = FakeFlowMeshClient(workers_by_alias={})
        with self.assertRaises(WorkerPreflightError) as context:
            preflight_flowmesh_worker(
                client,
                self.settings(worker_alias="pf"),
            )
        self.assertIn("matched no worker", str(context.exception))

    def test_alias_matching_many_workers_fails_closed(self) -> None:
        client = FakeFlowMeshClient(
            workers_by_alias={"pf": ["wkr-000", "wkr-001"]}
        )
        with self.assertRaises(WorkerPreflightError) as context:
            preflight_flowmesh_worker(
                client,
                self.settings(worker_alias="pf"),
            )
        self.assertIn("matched 2 workers", str(context.exception))

    def test_exact_worker_id_is_verified_through_the_root(self) -> None:
        client = FakeFlowMeshClient(workers_by_id={"wkr-000": ["wkr-000"]})
        payload = preflight_flowmesh_worker(
            client,
            self.settings(worker_id="wkr-000"),
        )
        self.assertEqual(
            {"kind": "worker_id", "value": "wkr-000"},
            payload["requested_pin"],
        )
        self.assertEqual("wkr-000", payload["worker"]["worker_id"])
        self.assertEqual(["wkr-000"], client.worker_id_lookups)
        self.assertEqual([], client.alias_lookups)

    def test_exact_worker_id_absent_from_the_root_fails_closed(self) -> None:
        client = FakeFlowMeshClient(workers_by_id={})
        with self.assertRaises(WorkerPreflightError) as context:
            preflight_flowmesh_worker(
                client,
                self.settings(worker_id="wkr-000"),
            )
        self.assertIn("matched no worker", str(context.exception))
        self.assertIsNone(client.submitted_workflow)

    def test_preflight_without_a_requested_pin_fails_closed(self) -> None:
        with self.assertRaises(WorkerPreflightError) as context:
            preflight_flowmesh_worker(FakeFlowMeshClient(), self.settings())
        self.assertIn("nothing to verify", str(context.exception))

    def test_cli_exposes_both_pin_kinds_without_a_system_config(self) -> None:
        parser = cli_parser()
        alias = parser.parse_args(
            [
                "preflight-flowmesh",
                "--worker-alias",
                "pf",
                "--flowmesh-base-url",
                "https://root.invalid:9443",
            ]
        )
        self.assertEqual("preflight-flowmesh", alias.command)
        self.assertEqual("pf", alias.worker_alias)
        self.assertIsNone(alias.worker_id)

        exact = parser.parse_args(
            ["preflight-flowmesh", "--worker-id", "wkr-000"]
        )
        self.assertEqual("wkr-000", exact.worker_id)
        self.assertIsNone(exact.worker_alias)

        with self.assertRaises(SystemExit):
            parser.parse_args(
                [
                    "preflight-flowmesh",
                    "--worker-id",
                    "wkr-000",
                    "--worker-alias",
                    "pf",
                ]
            )

    def test_preflight_error_text_is_redacted(self) -> None:
        class LeakyClient(FakeFlowMeshClient):
            def describe_current_worker(self, **kwargs: Any) -> Any:
                raise RuntimeError("refused: Authorization: Bearer not-a-real-pat-fixture")

        with self.assertRaises(WorkerPreflightError) as context:
            preflight_flowmesh_worker(
                LeakyClient(),
                self.settings(worker_alias="pf"),
            )
        message = str(context.exception)
        self.assertNotIn("not-a-real-pat-fixture", message)
        self.assertIn("<redacted>", message)


class AdapterTelemetryReconciliationTest(unittest.TestCase):
    """End-to-end adapter path: access, terminate, reconcile, finish."""

    def setUp(self) -> None:
        self.config = load_config(CONFIG_PATH)
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.store = SQLiteSessionStore(
            Path(self.temporary_directory.name) / "gateway.sqlite3"
        )

    def build(
        self,
        *,
        settles_after: int | None = 0,
    ) -> tuple[AccessingFlowMeshClient, StubDataAgentClient, AccessGateway]:
        data_agent = StubDataAgentClient(settles_after=settles_after)
        gateway = AccessGateway(
            self.config,
            self.store,
            RemoteDataAgentBackend(
                data_agent,
                telemetry_quiescence_timeout_seconds=0.05,
            ),
        )
        return AccessingFlowMeshClient(gateway), data_agent, gateway

    def request(self) -> FlowMeshAgentRunRequest:
        return FlowMeshAgentRunRequest(
            question="When does the red car enter the tunnel?",
            design_id="D_structured_digest",
            task_class_id="video_qa",
            quote_profile_id="digest_low",
            seed=42,
            trial_id="adapter-reconciliation",
            object_id="video-001",
        )

    def test_adapter_reconciles_final_telemetry_into_session_events(
        self,
    ) -> None:
        client, data_agent, gateway = self.build(settles_after=0)
        run = FlowMeshAgentAdapter(
            client,
            gateway,
            FlowMeshSettings(),
        ).run(self.request())

        self.assertEqual("DONE", run.status)
        access_response = client.access_responses[0]
        self.assertTrue(access_response["ok"])
        self.assertEqual(
            access_response["artifact_handle"],
            access_response["payload"]["value"],
        )
        self.assertNotIn("http", json.dumps(access_response).lower())
        self.assertEqual(
            {"timestamp_seconds": 17, "event": "red car"},
            client.fetch_responses[0]["content"],
        )
        self.assertEqual(1, len(data_agent.fetch_requests))
        self.assertEqual(
            data_agent.fetch_requests[0].access_id,
            run.access_events[0]["data_agent_access_id"],
        )
        # Reconciliation asked for a final summary, and only then.
        self.assertEqual([True], data_agent.quiescence_requests)

        session = self.store.get_session(run.session_id)
        self.assertEqual("DONE", session.status)

        self.assertEqual(1, len(run.access_events))
        event = run.access_events[0]
        self.assertEqual(4096, event["artifact_bytes_sent"])
        self.assertEqual(31.5, event["artifact_transfer_latency_ms"])
        self.assertEqual(1, event["artifact_download_request_count"])
        self.assertEqual(1, event["artifact_full_download_count"])
        self.assertEqual(
            "flowmesh-catalog-v1",
            event["object_catalog_version"],
        )

    def test_quiescence_timeout_fails_the_session_instead_of_recording(
        self,
    ) -> None:
        client, _, gateway = self.build(settles_after=None)
        adapter = FlowMeshAgentAdapter(client, gateway, FlowMeshSettings())

        with self.assertRaises(DataAgentTelemetryQuiescenceError):
            adapter.run(self.request())

        submitted = client.submitted_workflow
        assert submitted is not None
        session_id = submitted["metadata"]["annotations"]["custom"][
            "pathfinder_session_id"
        ]
        # The workflow itself succeeded, but the session is failed because
        # its transfer figures could never be established.
        self.assertEqual("FAILED", self.store.get_session(session_id).status)

        events = self.store.list_events(session_id)
        self.assertEqual(1, len(events))
        # No partial byte or latency values were written.
        self.assertEqual(0, events[0].artifact_bytes_sent)
        self.assertEqual(0.0, events[0].artifact_transfer_latency_ms)
        self.assertEqual(0, events[0].artifact_download_request_count)
        self.assertEqual(0, events[0].artifact_full_download_count)

    def test_workflow_build_failure_fails_the_registered_session(
        self,
    ) -> None:
        """The gap this closes: a throw between register and submit.

        Workflow construction used to sit outside the failure handler, so a
        builder error left a registered session stuck in its initial state --
        never finished, never failed, and therefore counted as neither in
        analysis.
        """
        client, _, gateway = self.build(settles_after=0)

        def exploding_builder(*args: Any, **kwargs: Any) -> dict[str, Any]:
            raise RuntimeError("workflow builder regression")

        original = adapter_module.build_agent_workflow
        adapter_module.build_agent_workflow = exploding_builder
        self.addCleanup(
            setattr,
            adapter_module,
            "build_agent_workflow",
            original,
        )

        registered: list[str] = []
        real_register = gateway.register_session

        def spy(request: FlowMeshAgentRunRequest) -> Any:
            session = real_register(request)
            registered.append(session.session_id)
            return session

        gateway.register_session = spy  # type: ignore[method-assign]
        self.addCleanup(delattr, gateway, "register_session")

        adapter = FlowMeshAgentAdapter(client, gateway, FlowMeshSettings())
        with self.assertRaises(RuntimeError) as context:
            adapter.run(self.request())
        self.assertIn("workflow builder regression", str(context.exception))

        self.assertIsNone(client.submitted_workflow)
        self.assertEqual([], client.validated_workflows)
        self.assertEqual(1, len(registered))
        self.assertEqual(
            "FAILED",
            self.store.get_session(registered[0]).status,
        )

    def test_pin_guard_failure_fails_the_registered_session(self) -> None:
        """The pin guard now reports through the same handler as the rest."""
        client, _, gateway = self.build(settles_after=0)
        client._workers_by_id = {"wkr-16": ["wkr-16"]}

        def unpinning_builder(*args: Any, **kwargs: Any) -> dict[str, Any]:
            kwargs.pop("selected_worker_id", None)
            return build_agent_workflow(*args, **kwargs)

        original = adapter_module.build_agent_workflow
        adapter_module.build_agent_workflow = unpinning_builder
        self.addCleanup(
            setattr,
            adapter_module,
            "build_agent_workflow",
            original,
        )

        registered: list[str] = []
        real_register = gateway.register_session

        def spy(request: FlowMeshAgentRunRequest) -> Any:
            session = real_register(request)
            registered.append(session.session_id)
            return session

        gateway.register_session = spy  # type: ignore[method-assign]
        self.addCleanup(delattr, gateway, "register_session")

        adapter = FlowMeshAgentAdapter(
            client,
            gateway,
            FlowMeshSettings(worker_id="wkr-16"),
        )
        with self.assertRaises(FlowMeshPinningError):
            adapter.run(self.request())

        self.assertIsNone(client.submitted_workflow)
        self.assertEqual(1, len(registered))
        self.assertEqual(
            "FAILED",
            self.store.get_session(registered[0]).status,
        )


class ArtifactHandleBindingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.config = load_config(CONFIG_PATH)
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.store = SQLiteSessionStore(
            Path(self.temporary_directory.name) / "gateway.sqlite3"
        )
        self.data_agent = StubDataAgentClient()
        self.gateway = AccessGateway(
            self.config,
            self.store,
            RemoteDataAgentBackend(self.data_agent),
        )

    @staticmethod
    def request(session_id: str) -> FlowMeshAgentRunRequest:
        return FlowMeshAgentRunRequest(
            question="When does the red car enter the tunnel?",
            design_id="D_structured_digest",
            task_class_id="video_qa",
            quote_profile_id="digest_low",
            seed=42,
            trial_id=f"artifact-{session_id}",
            session_id=session_id,
            object_id="video-001",
        )

    def test_handle_fingerprint_is_persisted_and_bound_to_session(self) -> None:
        owner = self.gateway.register_session(self.request("owner-session"))
        other = self.gateway.register_session(self.request("other-session"))
        access = self.gateway.access_representation(
            owner.session_id,
            "multimodal_digest",
        )
        handle = access["artifact_handle"]

        self.assertIsInstance(handle, str)
        self.assertGreaterEqual(len(handle), 24)
        self.assertNotIn("http", json.dumps(access).lower())
        event = self.store.list_events(owner.session_id)[0]
        fingerprint = sha256(handle.encode("utf-8")).hexdigest()
        self.assertEqual(fingerprint, event.artifact_handle_sha256)
        self.assertNotIn(handle, json.dumps(event.to_dict()))
        with closing(sqlite3.connect(self.store.path)) as connection:
            stored = connection.execute(
                """
                SELECT artifact_handle, artifact_handle_sha256
                FROM gateway_access_events
                WHERE event_id = ?
                """,
                (event.event_id,),
            ).fetchone()
        self.assertEqual((None, fingerprint), stored)

        fetched = self.gateway.fetch_artifact(owner.session_id, handle)
        self.assertEqual(
            {"timestamp_seconds": 17, "event": "red car"},
            fetched["content"],
        )
        self.assertEqual(event.content_sha256, fetched["sha256"])
        self.assertNotIn("artifact_handle", fetched)
        self.assertEqual(fingerprint, fetched["artifact_handle_sha256"])
        self.assertEqual(1, len(self.data_agent.fetch_requests))
        replay = self.data_agent.fetch_requests[0]
        self.assertEqual(event.data_agent_access_id, replay.access_id)
        self.assertEqual(owner.session_id, replay.session_id)
        self.assertEqual(event.event_index, replay.event_index)

        for session_id, invalid_handle in (
            (other.session_id, handle),
            (owner.session_id, "unknown-handle"),
        ):
            with self.subTest(session_id=session_id):
                with self.assertLogs(
                    "pathfinder.integrations.flowmesh.gateway",
                    level="WARNING",
                ) as captured:
                    with self.assertRaises(ArtifactHandleError) as context:
                        self.gateway.fetch_artifact(
                            session_id,
                            invalid_handle,
                        )
                self.assertEqual(
                    "artifact handle is invalid for this Pathfinder session",
                    str(context.exception),
                )
                self.assertNotIn(invalid_handle, "\n".join(captured.output))
        self.assertEqual(1, len(self.data_agent.fetch_requests))

    def test_legacy_cleartext_handle_is_hashed_and_erased(self) -> None:
        session = self.gateway.register_session(self.request("legacy-session"))
        access = self.gateway.access_representation(
            session.session_id,
            "multimodal_digest",
        )
        handle = access["artifact_handle"]
        with closing(sqlite3.connect(self.store.path)) as connection:
            connection.execute(
                """
                UPDATE gateway_access_events
                SET artifact_handle = ?, artifact_handle_sha256 = NULL
                WHERE session_id = ?
                """,
                (handle, session.session_id),
            )
            connection.commit()

        migrated = SQLiteSessionStore(self.store.path)
        event = migrated.list_events(session.session_id)[0]
        fingerprint = sha256(handle.encode("utf-8")).hexdigest()
        self.assertEqual(fingerprint, event.artifact_handle_sha256)
        with closing(sqlite3.connect(self.store.path)) as connection:
            raw = connection.execute(
                """
                SELECT artifact_handle
                FROM gateway_access_events
                WHERE session_id = ?
                """,
                (session.session_id,),
            ).fetchone()[0]
        self.assertIsNone(raw)
        self.assertEqual(event, migrated.get_artifact_event(session.session_id, handle))

    def test_blank_and_oversized_handles_are_rejected_before_lookup(self) -> None:
        session = self.gateway.register_session(self.request("bad-handles"))
        for handle in ("", " " * 4, "x" * 257):
            with self.subTest(length=len(handle)):
                with self.assertRaises(ArtifactHandleError):
                    self.gateway.fetch_artifact(session.session_id, handle)
        self.assertEqual([], self.data_agent.fetch_requests)


class FlowMeshIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.config = load_config(CONFIG_PATH)
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.store = SQLiteSessionStore(
            Path(self.temporary_directory.name) / "gateway.sqlite3"
        )
        self.gateway = AccessGateway(
            self.config,
            self.store,
            EmulatedRepresentationBackend(sleep=lambda _: None),
        )

    def request(
        self,
        *,
        quote_profile_id: str = "digest_low",
        trial_id: str = "integration-test",
    ) -> FlowMeshAgentRunRequest:
        return FlowMeshAgentRunRequest(
            question="When does the red car enter the tunnel?",
            design_id="D_structured_digest",
            task_class_id="video_qa",
            quote_profile_id=quote_profile_id,
            seed=42,
            trial_id=trial_id,
        )

    def test_gateway_lists_only_agent_visible_offer_fields(self) -> None:
        session = self.gateway.register_session(self.request())
        state = self.gateway.list_offers(session.session_id)
        digest = next(
            offer
            for offer in state["offers"]
            if offer["representation_id"] == "multimodal_digest"
        )
        self.assertEqual(2.0, digest["quoted_price"])
        self.assertTrue(digest["affordable"])
        self.assertNotIn("realized_cost", digest)
        self.assertNotIn("expected_latency_ms", digest)
        self.assertNotIn("task_quality", digest)

    def test_gateway_enforces_budget_and_access_limit(self) -> None:
        session = self.gateway.register_session(self.request())
        first = self.gateway.access_representation(
            session.session_id,
            "multimodal_digest",
        )
        second = self.gateway.access_representation(
            session.session_id,
            "sampled_frames",
        )
        self.assertTrue(first["ok"])
        self.assertEqual(2.0, first["quoted_price_charged"])
        self.assertEqual(4.0, first["remaining_budget"])
        self.assertIn("felt_latency_ms", first)
        self.assertNotIn("realized_cost", first)
        self.assertFalse(second["ok"])
        self.assertEqual("access_limit_reached", second["error"])

        events = self.store.list_events(session.session_id)
        self.assertEqual(2, len(events))
        self.assertTrue(events[0].accepted)
        self.assertGreater(events[0].realized_cost, 0.0)
        self.assertFalse(events[1].accepted)

    def test_emulated_backend_imposes_felt_latency(self) -> None:
        slept_seconds: list[float] = []
        gateway = AccessGateway(
            self.config,
            self.store,
            EmulatedRepresentationBackend(sleep=slept_seconds.append),
        )
        session = gateway.register_session(
            self.request(trial_id="felt-latency")
        )
        result = gateway.access_representation(
            session.session_id,
            "compressed_video",
        )
        self.assertTrue(result["ok"])
        self.assertEqual(1, len(slept_seconds))
        self.assertAlmostEqual(
            result["felt_latency_ms"] / 1_000.0,
            slept_seconds[0],
        )

    def test_high_quote_is_rejected_as_insufficient_budget(self) -> None:
        session = self.gateway.register_session(
            self.request(
                quote_profile_id="digest_high",
                trial_id="high-quote",
            )
        )
        result = self.gateway.access_representation(
            session.session_id,
            "multimodal_digest",
        )
        self.assertFalse(result["ok"])
        self.assertEqual("insufficient_budget", result["error"])

    def test_workflow_uses_agent_config_and_session_tools(self) -> None:
        request = self.request()
        settings = FlowMeshSettings(agent_config_name="pathfinder_video")
        workflow = build_agent_workflow(
            "session-123",
            request,
            settings,
        )
        spec = agent_spec_of(workflow)
        self.assertEqual("agent", spec["taskType"])
        self.assertEqual("pathfinder_video", spec["configName"])
        self.assertIn("session-123", spec["task"])
        self.assertIn("list_offers", spec["task"])

    def test_mcp_server_registers_fetch_artifact(self) -> None:
        mcp_module = types.ModuleType("mcp")
        server_module = types.ModuleType("mcp.server")
        fastmcp_module = types.ModuleType("mcp.server.fastmcp")
        mcp_module.__path__ = []  # type: ignore[attr-defined]
        server_module.__path__ = []  # type: ignore[attr-defined]
        fastmcp_module.FastMCP = RecordingFastMCP  # type: ignore[attr-defined]
        mcp_module.server = server_module  # type: ignore[attr-defined]
        server_module.fastmcp = fastmcp_module  # type: ignore[attr-defined]
        modules = {
            "mcp": mcp_module,
            "mcp.server": server_module,
            "mcp.server.fastmcp": fastmcp_module,
        }
        with patch.dict(sys.modules, modules):
            server = build_mcp_server(
                config_path=CONFIG_PATH,
                state_db=Path(self.temporary_directory.name)
                / "mcp-gateway.sqlite3",
                host="127.0.0.1",
                port=8765,
            )
        self.assertEqual(
            {
                "list_offers",
                "access_representation",
                "fetch_artifact",
                "get_session_state",
            },
            set(server.tools),
        )

    def test_worker_agent_config_activates_fetch_artifact(self) -> None:
        agent_config = (
            ROOT
            / "integrations"
            / "flowmesh"
            / "agent_configs"
            / "pathfinder_video.yaml"
        ).read_text(encoding="utf-8")
        self.assertIn("      - fetch_artifact\n", agent_config)
        self.assertIn("Never construct or fetch", agent_config)

    def test_cost_aware_worker_config_defines_a_neutral_price_objective(
        self,
    ) -> None:
        config_path = (
            ROOT
            / "integrations"
            / "flowmesh"
            / "agent_configs"
            / "pathfinder_video_cost_aware.yaml"
        )
        agent_config = config_path.read_text(encoding="utf-8")
        self.assertIn("Unspent budget has value", agent_config)
        self.assertIn("Compare all affordable offers", agent_config)
        self.assertIn("      - fetch_artifact\n", agent_config)
        self.assertNotIn("choose multimodal_digest", agent_config.casefold())

        dockerfile = (
            ROOT / "integrations" / "flowmesh" / "Dockerfile.worker-overlay"
        ).read_text(encoding="utf-8")
        self.assertIn("pathfinder_video_cost_aware.yaml", dockerfile)

    def test_workflow_requests_http_result_delivery(self) -> None:
        """Guard the fix for results.retrieve() answering 404 result not found.

        A local destination leaves results.json on the worker and uploads
        nothing, so the Root Server never holds a result to return.
        """
        for selected_worker_id in (None, "wkr-16"):
            with self.subTest(selected_worker=selected_worker_id):
                workflow = build_agent_workflow(
                    "session-123",
                    self.request(),
                    FlowMeshSettings(),
                    selected_worker_id=selected_worker_id,
                )
                destination = agent_spec_of(workflow)["output"]["destination"]
                self.assertEqual("http", destination["type"])
                self.assertNotEqual("local", destination["type"])
                # url, method, and headers must stay absent: the worker fills
                # in FLOWMESH_BASE_URL + /api/v1/results and attaches its own
                # bearer token. Setting them here would hard-code a deployment
                # address or carry a credential inside the workflow document.
                self.assertEqual({"type"}, set(destination))

    def test_no_workflow_variant_uses_a_local_destination(self) -> None:
        variants = [
            build_agent_workflow(
                "session-123", self.request(), FlowMeshSettings()
            ),
            build_agent_workflow(
                "session-123",
                self.request(),
                FlowMeshSettings(),
                selected_worker_id="wkr-16",
            ),
            build_agent_workflow(
                "session-123",
                FlowMeshAgentRunRequest(
                    question="When does the red car enter the tunnel?",
                    design_id="D_structured_digest",
                    task_class_id="video_qa",
                    trial_id="local-destination-guard",
                    object_id="video-001",
                ),
                FlowMeshSettings(agent_config_name="pathfinder_video"),
            ),
        ]
        for workflow in variants:
            self.assertNotIn('"local"', json.dumps(workflow))

    def test_workflow_is_a_single_node_graph(self) -> None:
        workflow = build_agent_workflow(
            "session-123",
            self.request(),
            FlowMeshSettings(),
        )
        nodes = workflow["spec"]["graph"]["nodes"]
        self.assertEqual(1, len(nodes))
        self.assertEqual(PATHFINDER_GRAPH_NODE_NAME, nodes[0]["name"])
        # The bare single-task form is what silently drops schedule_hint on
        # FlowMesh v0.1.8-rc.1, so it must not reappear at the top level.
        self.assertNotIn("taskType", workflow["spec"])

    def test_workflow_nests_provenance_under_annotations_custom(self) -> None:
        request = self.request(trial_id="provenance-trial")
        workflow = build_agent_workflow(
            "session-123",
            request,
            FlowMeshSettings(),
        )
        annotations = workflow["metadata"]["annotations"]
        # FlowMesh forbids unknown keys directly under annotations.
        self.assertEqual({"custom"}, set(annotations))
        custom = annotations["custom"]
        self.assertEqual("session-123", custom["pathfinder_session_id"])
        self.assertEqual("provenance-trial", custom["pathfinder_trial_id"])
        self.assertEqual(
            "D_structured_digest",
            custom["pathfinder_design_id"],
        )
        self.assertEqual("video_qa", custom["pathfinder_task_class_id"])

    def test_workflow_includes_object_id_when_present(self) -> None:
        request = FlowMeshAgentRunRequest(
            question="q",
            design_id="D_structured_digest",
            task_class_id="video_qa",
            object_id="video-001",
        )
        workflow = build_agent_workflow("s", request, FlowMeshSettings())
        custom = workflow["metadata"]["annotations"]["custom"]
        self.assertEqual("video-001", custom["pathfinder_object_id"])

    def test_unpinned_workflow_omits_schedule_hint(self) -> None:
        workflow = build_agent_workflow(
            "session-123",
            self.request(),
            FlowMeshSettings(),
        )
        self.assertNotIn(
            "schedule_hint",
            workflow["metadata"]["annotations"],
        )
        self.assertIsNone(workflow_selected_worker(workflow))

    def test_pinned_workflow_carries_selected_worker(self) -> None:
        workflow = build_agent_workflow(
            "session-123",
            self.request(),
            FlowMeshSettings(),
            selected_worker_id="wkr-16",
        )
        hint = workflow["metadata"]["annotations"]["schedule_hint"]
        self.assertEqual("wkr-16", hint["selected_worker"])
        self.assertEqual("wkr-16", workflow_selected_worker(workflow))
        # custom must survive alongside the hint
        self.assertIn(
            "pathfinder_session_id",
            workflow["metadata"]["annotations"]["custom"],
        )

    def test_blank_selected_worker_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            build_agent_workflow(
                "session-123",
                self.request(),
                FlowMeshSettings(),
                selected_worker_id="   ",
            )

    def test_adapter_submits_waits_and_extracts_result(self) -> None:
        client = FakeFlowMeshClient()
        result = FlowMeshAgentAdapter(
            client,
            self.gateway,
            FlowMeshSettings(),
        ).run(self.request())
        self.assertEqual("DONE", result.status)
        self.assertEqual("wfl-test", result.workflow_id)
        self.assertEqual("tsk-test", result.task_id)
        self.assertEqual(
            "The car enters at 00:17.",
            result.final_answer,
        )
        session = self.store.get_session(result.session_id)
        self.assertEqual("DONE", session.status)
        self.assertEqual("wfl-test", session.flowmesh_workflow_id)
        self.assertIsNotNone(client.submitted_workflow)

    def test_adapter_recovers_a_completed_gateway_session(self) -> None:
        first_client = FakeFlowMeshClient()
        adapter = FlowMeshAgentAdapter(
            first_client,
            self.gateway,
            FlowMeshSettings(),
        )
        completed = adapter.run(self.request(trial_id="recover-done"))

        recovery_client = FakeFlowMeshClient()
        recovered = FlowMeshAgentAdapter(
            recovery_client,
            self.gateway,
            FlowMeshSettings(),
        ).recover(completed.session_id)

        self.assertIsNotNone(recovered)
        self.assertEqual(completed.final_answer, recovered.final_answer)
        self.assertEqual(
            {"recovered_from_gateway_state": True},
            recovered.raw_result,
        )
        self.assertIsNone(recovery_client.submitted_workflow)

    def test_adapter_resumes_a_bound_running_session(self) -> None:
        session = self.gateway.register_session(
            self.request(trial_id="recover-bound")
        )
        self.store.bind_flowmesh(session.session_id, "wfl-old", "tsk-old")
        client = FakeFlowMeshClient()

        recovered = FlowMeshAgentAdapter(
            client,
            self.gateway,
            FlowMeshSettings(),
        ).recover(session.session_id)

        self.assertIsNotNone(recovered)
        self.assertEqual("wfl-old", recovered.workflow_id)
        self.assertEqual("tsk-old", recovered.task_id)
        self.assertEqual("DONE", self.store.get_session(session.session_id).status)
        self.assertIsNone(client.submitted_workflow)

    def test_adapter_fails_closed_on_an_unbound_created_session(self) -> None:
        session = self.gateway.register_session(
            self.request(trial_id="recover-unbound")
        )
        with self.assertRaisesRegex(FlowMeshRunError, "ambiguous"):
            FlowMeshAgentAdapter(
                FakeFlowMeshClient(),
                self.gateway,
                FlowMeshSettings(),
            ).recover(session.session_id)
        self.assertEqual(
            "FAILED",
            self.store.get_session(session.session_id).status,
        )

    def test_result_parser_accepts_direct_result_shape(self) -> None:
        answer = extract_agent_answer(
            {"items": [{"response": "Direct response"}]}
        )
        self.assertEqual("Direct response", answer)

    # ---------------------------------------------------------------- #
    # Worker pinning
    # ---------------------------------------------------------------- #

    def test_worker_id_and_alias_are_mutually_exclusive(self) -> None:
        with self.assertRaises(ValueError) as context:
            FlowMeshSettings(worker_id="wkr-16", worker_alias="pathfinder")
        self.assertIn("mutually exclusive", str(context.exception))

    def test_blank_worker_pin_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            FlowMeshSettings(worker_id="  ")
        with self.assertRaises(ValueError):
            FlowMeshSettings(worker_alias="  ")

    def test_pinning_requested_flag(self) -> None:
        self.assertFalse(FlowMeshSettings().pinning_requested)
        self.assertTrue(FlowMeshSettings(worker_id="wkr-16").pinning_requested)
        self.assertTrue(
            FlowMeshSettings(worker_alias="pf").pinning_requested
        )

    def test_adapter_pins_explicit_worker_id(self) -> None:
        client = FakeFlowMeshClient(workers_by_id={"wkr-16": ["wkr-16"]})
        FlowMeshAgentAdapter(
            client,
            self.gateway,
            FlowMeshSettings(worker_id="wkr-16"),
        ).run(self.request())
        self.assertEqual(
            "wkr-16",
            workflow_selected_worker(client.submitted_workflow),
        )
        # An explicit ID must not trigger an alias lookup, but it must still
        # be verified against the configured Root.
        self.assertEqual([], client.alias_lookups)
        self.assertEqual(["wkr-16"], client.worker_id_lookups)

    def test_exact_worker_id_absent_from_root_fails_without_submitting(
        self,
    ) -> None:
        """The regression this closes: an ID trusted without asking the Root.

        A worker the local Node Server still lists, but the configured Root
        does not, used to produce a submitted workflow that was never
        dispatched and failed minutes later with nothing in the worker log.
        """
        client = FakeFlowMeshClient(workers_by_id={})
        with self.assertRaises(FlowMeshPinningError) as context:
            FlowMeshAgentAdapter(
                client,
                self.gateway,
                FlowMeshSettings(worker_id="wkr-000"),
            ).run(self.request())
        self.assertIn("matched no worker", str(context.exception))
        self.assertEqual(["wkr-000"], client.worker_id_lookups)
        self.assertIsNone(client.submitted_workflow)
        self.assertEqual([], client.validated_workflows)

    def test_pin_verification_precedes_gateway_session_registration(
        self,
    ) -> None:
        client = FakeFlowMeshClient(workers_by_id={})
        registered: list[str] = []
        real_register = self.gateway.register_session

        def spy(request: FlowMeshAgentRunRequest) -> Any:
            session = real_register(request)
            registered.append(session.session_id)
            return session

        self.gateway.register_session = spy  # type: ignore[method-assign]
        self.addCleanup(delattr, self.gateway, "register_session")

        with self.assertRaises(FlowMeshPinningError):
            FlowMeshAgentAdapter(
                client,
                self.gateway,
                FlowMeshSettings(worker_id="wkr-000"),
            ).run(self.request())
        # Nothing ran, so nothing may be left behind to audit.
        self.assertEqual([], registered)
        self.assertIsNone(client.submitted_workflow)

    def test_client_without_root_verification_cannot_honour_a_pin(
        self,
    ) -> None:
        class UnverifiableClient(FakeFlowMeshClient):
            describe_current_worker = None  # type: ignore[assignment]

        client = UnverifiableClient()
        with self.assertRaises(FlowMeshPinningError) as context:
            FlowMeshAgentAdapter(
                client,
                self.gateway,
                FlowMeshSettings(worker_id="wkr-16"),
            ).run(self.request())
        self.assertIn("configured Root", str(context.exception))
        self.assertIsNone(client.submitted_workflow)

    def test_adapter_resolves_alias_to_current_worker_id(self) -> None:
        client = FakeFlowMeshClient(workers_by_alias={"pf": ["wkr-42"]})
        FlowMeshAgentAdapter(
            client,
            self.gateway,
            FlowMeshSettings(worker_alias="pf"),
        ).run(self.request())
        self.assertEqual(["pf"], client.alias_lookups)
        self.assertEqual(
            "wkr-42",
            workflow_selected_worker(client.submitted_workflow),
        )

    def test_alias_matching_no_worker_fails_without_submitting(self) -> None:
        client = FakeFlowMeshClient(workers_by_alias={})
        with self.assertRaises(FlowMeshPinningError) as context:
            FlowMeshAgentAdapter(
                client,
                self.gateway,
                FlowMeshSettings(worker_alias="missing"),
            ).run(self.request())
        self.assertIn("matched no worker", str(context.exception))
        # Never silently fall back to an unpinned run.
        self.assertIsNone(client.submitted_workflow)

    def test_alias_matching_many_workers_fails_without_submitting(
        self,
    ) -> None:
        client = FakeFlowMeshClient(
            workers_by_alias={"pf": ["wkr-16", "wkr-17"]}
        )
        with self.assertRaises(FlowMeshPinningError) as context:
            FlowMeshAgentAdapter(
                client,
                self.gateway,
                FlowMeshSettings(worker_alias="pf"),
            ).run(self.request())
        message = str(context.exception)
        self.assertIn("matched 2 workers", message)
        self.assertIsNone(client.submitted_workflow)

    def test_unpinned_adapter_run_submits_without_hint(self) -> None:
        client = FakeFlowMeshClient()
        FlowMeshAgentAdapter(
            client,
            self.gateway,
            FlowMeshSettings(),
        ).run(self.request())
        self.assertIsNotNone(client.submitted_workflow)
        self.assertIsNone(
            workflow_selected_worker(client.submitted_workflow)
        )
        self.assertEqual([], client.alias_lookups)

    # ---------------------------------------------------------------- #
    # Terminal failure diagnostics
    # ---------------------------------------------------------------- #

    def test_terminal_failure_carries_flowmesh_reported_detail(self) -> None:
        client = FakeFlowMeshClient(
            terminal=TerminalWorkflow(
                workflow_id="wfl-synthetic-1",
                status="FAILED",
                failed_task_ids=("tsk-synthetic-1",),
                detail="the Root never recorded a dispatched task",
            ),
            task_failure={
                "task_status": "FAILED",
                "attempts": 1,
                "detail": "no worker accepted the task",
                "assigned_worker": None,
            },
        )
        with self.assertRaises(FlowMeshWorkflowFailureError) as context:
            FlowMeshAgentAdapter(
                client,
                self.gateway,
                FlowMeshSettings(),
            ).run(self.request())

        error = context.exception
        self.assertEqual("wfl-synthetic-1", error.workflow_id)
        self.assertEqual("tsk-test", error.task_id)
        self.assertEqual("FAILED", error.terminal_status)
        message = str(error)
        self.assertIn("wfl-synthetic-1", message)
        self.assertIn("tsk-test", message)
        self.assertIn("FAILED", message)
        self.assertIn("never recorded a dispatched task", message)
        self.assertIn("no worker accepted the task", message)
        self.assertEqual(["tsk-test"], client.task_failure_lookups)

    def test_terminal_failure_without_detail_directs_the_operator(
        self,
    ) -> None:
        client = FakeFlowMeshClient(
            terminal=TerminalWorkflow(
                workflow_id="wfl-synthetic-1",
                status="FAILED",
            ),
            task_failure=None,
        )
        with self.assertRaises(FlowMeshWorkflowFailureError) as context:
            FlowMeshAgentAdapter(
                client,
                self.gateway,
                FlowMeshSettings(),
            ).run(self.request())

        message = str(context.exception)
        self.assertIsNone(context.exception.detail)
        self.assertIn("returned no failure detail", message)
        self.assertIn("Root scheduling state", message)
        self.assertIn("Node Server dispatch", message)
        self.assertIn("worker's logs", message)

    def test_terminal_failure_detail_is_redacted(self) -> None:
        client = FakeFlowMeshClient(
            terminal=TerminalWorkflow(
                workflow_id="wfl-1",
                status="FAILED",
                detail="upload rejected: Authorization: Bearer not-a-real-pat-fixture",
            ),
        )
        with self.assertRaises(FlowMeshWorkflowFailureError) as context:
            FlowMeshAgentAdapter(
                client,
                self.gateway,
                FlowMeshSettings(),
            ).run(self.request())
        message = str(context.exception)
        self.assertNotIn("not-a-real-pat-fixture", message)
        self.assertIn("<redacted>", message)

    def test_task_detail_lookup_failure_does_not_mask_the_workflow_failure(
        self,
    ) -> None:
        class ExplodingDetailClient(FakeFlowMeshClient):
            def describe_task_failure(self, task_id: str) -> Any:
                raise RuntimeError("task API unavailable")

        client = ExplodingDetailClient(
            terminal=TerminalWorkflow(workflow_id="wfl-1", status="FAILED"),
        )
        with self.assertRaises(FlowMeshWorkflowFailureError) as context:
            FlowMeshAgentAdapter(
                client,
                self.gateway,
                FlowMeshSettings(),
            ).run(self.request())
        message = str(context.exception)
        self.assertIn("ended with status FAILED", message)
        self.assertIn("task detail lookup failed", message)

    def test_client_without_task_detail_still_reports_the_failure(
        self,
    ) -> None:
        class LegacyClient(FakeFlowMeshClient):
            describe_task_failure = None  # type: ignore[assignment]

        client = LegacyClient(
            terminal=TerminalWorkflow(workflow_id="wfl-1", status="FAILED"),
        )
        with self.assertRaises(FlowMeshWorkflowFailureError) as context:
            FlowMeshAgentAdapter(
                client,
                self.gateway,
                FlowMeshSettings(),
            ).run(self.request())
        self.assertIn("returned no failure detail", str(context.exception))
        self.assertIn(
            "no additional detail available",
            str(context.exception),
        )

    def _failing_detail_client(self, exception: Exception) -> Any:
        outer = self

        class FailingDetailClient(FakeFlowMeshClient):
            def describe_task_failure(self, task_id: str) -> Any:
                outer.assertEqual("tsk-test", task_id)
                raise exception

        return FailingDetailClient(
            terminal=TerminalWorkflow(
                workflow_id="wfl-synthetic-1",
                status="FAILED",
                failed_task_ids=("tsk-synthetic-1",),
                detail="root-reported failed tasks: tsk-synthetic-1",
            ),
        )

    def test_every_detail_lookup_failure_preserves_the_terminal_failure(
        self,
    ) -> None:
        """Timeout, 401/403, 404, malformed response, SDK incompatibility."""
        cases = {
            "timeout": TimeoutError("read timed out after 30s"),
            "unauthorized": RuntimeError("401 Unauthorized"),
            "forbidden": RuntimeError("403 Forbidden"),
            "not_found": RuntimeError("404 task not found"),
            "incompatible_sdk": TypeError(
                "retrieve() got an unexpected keyword argument"
            ),
            "malformed": ValueError("response is not valid JSON"),
            "attribute": AttributeError("'FlowMesh' object has no 'tasks'"),
        }
        for label, exception in cases.items():
            with self.subTest(failure=label):
                client = self._failing_detail_client(exception)
                with self.assertRaises(
                    FlowMeshWorkflowFailureError
                ) as context:
                    FlowMeshAgentAdapter(
                        client,
                        self.gateway,
                        FlowMeshSettings(),
                    ).run(self.request(trial_id=f"detail-{label}"))

                error = context.exception
                # The original terminal failure survives intact.
                self.assertEqual("wfl-synthetic-1", error.workflow_id)
                self.assertEqual("tsk-test", error.task_id)
                self.assertEqual("FAILED", error.terminal_status)
                self.assertIn("tsk-synthetic-1", error.detail or "")
                message = str(error)
                self.assertIn("ended with status FAILED", message)
                self.assertIn("root-reported failed tasks", message)
                self.assertIn("no additional detail available", message)
                self.assertIn(type(exception).__name__, message)
                # The diagnostic exception is described, never propagated in
                # place of the workflow failure it was meant to explain.
                self.assertIsNot(error, exception)
                self.assertIsInstance(error, FlowMeshWorkflowFailureError)
                self.assertIsNone(error.__cause__)

    def test_a_malformed_detail_response_is_reported_not_trusted(
        self,
    ) -> None:
        for label, payload in (
            ("string", "unexpected string payload"),
            ("list", ["unexpected", "list"]),
            ("empty", {}),
            ("all_none", {"detail": None, "attempts": None}),
            ("none", None),
        ):
            with self.subTest(payload=label):
                client = FakeFlowMeshClient(
                    terminal=TerminalWorkflow(
                        workflow_id="wfl-synthetic-1",
                        status="FAILED",
                    ),
                    task_failure=payload,
                )
                with self.assertRaises(
                    FlowMeshWorkflowFailureError
                ) as context:
                    FlowMeshAgentAdapter(
                        client,
                        self.gateway,
                        FlowMeshSettings(),
                    ).run(self.request(trial_id=f"malformed-{label}"))
                message = str(context.exception)
                self.assertIn("ended with status FAILED", message)
                self.assertIn("no additional detail available", message)
                self.assertIsNone(context.exception.detail)

    def test_no_credential_value_appears_in_a_diagnostic_failure(
        self,
    ) -> None:
        with patch.dict(
            "os.environ",
            {
                "FLOWMESH_API_KEY": "not-a-real-pat-fixture",
                "PATHFINDER_DATA_AGENT_TOKEN": "not-a-real-token-fixture",
            },
        ):
            client = self._failing_detail_client(
                RuntimeError(
                    "401 for Authorization: Bearer not-a-real-pat-fixture at "
                    "https://root.invalid/tasks/x?sig=private "
                    "(token not-a-real-token-fixture)"
                )
            )
            with self.assertRaises(FlowMeshWorkflowFailureError) as context:
                FlowMeshAgentAdapter(
                    client,
                    self.gateway,
                    FlowMeshSettings(),
                ).run(self.request())

            error = context.exception
            for rendering in (
                str(error),
                error.detail or "",
                error.lookup_note or "",
            ):
                self.assertNotIn("not-a-real-pat-fixture", rendering)
                self.assertNotIn("not-a-real-token-fixture", rendering)
                self.assertNotIn("sig=private", rendering)
            self.assertIn("<redacted>", str(error))
            # The failure is still fully identified despite the redaction.
            self.assertIn("wfl-synthetic-1", str(error))
            self.assertIn("tsk-test", str(error))
            self.assertIn("FAILED", str(error))

    # ---------------------------------------------------------------- #
    # Workflow validation
    # ---------------------------------------------------------------- #

    def test_validation_runs_before_submission_when_enabled(self) -> None:
        client = FakeFlowMeshClient()
        FlowMeshAgentAdapter(
            client,
            self.gateway,
            FlowMeshSettings(validate_before_submit=True),
        ).run(self.request())
        self.assertEqual(1, len(client.validated_workflows))
        self.assertIsNotNone(client.submitted_workflow)

    def test_validation_is_skipped_by_default(self) -> None:
        client = FakeFlowMeshClient()
        FlowMeshAgentAdapter(
            client,
            self.gateway,
            FlowMeshSettings(),
        ).run(self.request())
        self.assertEqual([], client.validated_workflows)

    def test_failed_validation_prevents_submission(self) -> None:
        client = FakeFlowMeshClient(
            validation=WorkflowValidation(ok=False, errors=("bad spec",))
        )
        with self.assertRaises(FlowMeshRunError) as context:
            FlowMeshAgentAdapter(
                client,
                self.gateway,
                FlowMeshSettings(validate_before_submit=True),
            ).run(self.request())
        self.assertIn("bad spec", str(context.exception))
        self.assertEqual(1, len(client.validated_workflows))
        self.assertIsNone(client.submitted_workflow)


class DeployedParserCompatibilityTest(unittest.TestCase):
    """Parse generated workflows with the exact deployed FlowMesh parser.

    Skipped when the reference source tree is absent so the suite stays
    runnable without a FlowMesh checkout.
    """

    @classmethod
    def setUpClass(cls) -> None:
        source = os.getenv(
            "PATHFINDER_FLOWMESH_SOURCE",
            DEFAULT_FLOWMESH_SOURCE,
        )
        src_root = Path(source) / "src"
        if not (src_root / "server" / "task" / "parser.py").exists():
            raise unittest.SkipTest(
                f"FlowMesh v0.1.8-rc.1 source not found at {src_root}; set "
                "PATHFINDER_FLOWMESH_SOURCE to enable deployed-parser checks"
            )
        sys.path.insert(0, str(src_root))
        cls.addClassCleanup(_remove_sys_path_entry, str(src_root))
        try:
            from server.task.parser import parse_workflow
        except Exception as exc:  # pragma: no cover - environment dependent
            raise unittest.SkipTest(
                f"cannot import the deployed FlowMesh parser: {exc}"
            )
        cls.parse_workflow = staticmethod(parse_workflow)

    def parse(self, workflow: Mapping[str, Any]) -> Any:
        return type(self).parse_workflow(json.dumps(workflow), "native")

    def request(self) -> FlowMeshAgentRunRequest:
        return FlowMeshAgentRunRequest(
            question="When does the red car enter the tunnel?",
            design_id="D_structured_digest",
            task_class_id="video_qa",
            trial_id="parser-check",
            object_id="video-001",
        )

    def test_unpinned_workflow_parses(self) -> None:
        workflow = build_agent_workflow(
            "session-abcdef123456",
            self.request(),
            FlowMeshSettings(),
        )
        parsed = self.parse(workflow)
        self.assertEqual(1, len(parsed.tasks))
        self.assertIsNone(parsed.tasks[0].selected_worker)

    def test_pinned_workflow_retains_exact_selected_worker(self) -> None:
        workflow = build_agent_workflow(
            "session-abcdef123456",
            self.request(),
            FlowMeshSettings(),
            selected_worker_id="wkr-16",
        )
        parsed = self.parse(workflow)
        self.assertEqual(1, len(parsed.tasks))
        # The regression this guards: a bare single-task spec parses fine but
        # yields selected_worker=None, silently unpinning the experiment.
        self.assertEqual(["wkr-16"], parsed.tasks[0].selected_worker)
        self.assertEqual(
            PATHFINDER_GRAPH_NODE_NAME,
            parsed.tasks[0].graph_node_name,
        )

    def test_deployed_parser_reads_an_http_result_destination(self) -> None:
        """The deployed parser must accept http delivery with no url.

        On v0.1.8-rc.1 the worker resolves an http destination whose url is
        absent to FLOWMESH_BASE_URL + /api/v1/results and attaches its own
        auth headers, which is why Pathfinder omits both. A local destination
        instead keeps results.json on the worker, and results.retrieve()
        against the Root Server then answers 404 result not found.
        """
        workflow = build_agent_workflow(
            "session-abcdef123456",
            self.request(),
            FlowMeshSettings(),
            selected_worker_id="wkr-16",
        )
        parsed = self.parse(workflow)
        destination = parsed.tasks[0].task.spec.output.destination
        self.assertEqual("http", destination.type)
        self.assertIsNone(destination.url)
        self.assertIsNone(destination.headers)

    def test_annotations_directly_under_annotations_are_rejected(
        self,
    ) -> None:
        """Pin the constraint that motivated the annotations.custom move."""
        workflow = build_agent_workflow(
            "session-abcdef123456",
            self.request(),
            FlowMeshSettings(),
        )
        flattened = copy.deepcopy(workflow)
        annotations = flattened["metadata"]["annotations"]
        annotations.update(annotations.pop("custom"))
        with self.assertRaises(ValueError) as context:
            self.parse(flattened)
        self.assertIn("Extra inputs are not permitted", str(context.exception))


def _remove_sys_path_entry(entry: str) -> None:
    while entry in sys.path:
        sys.path.remove(entry)


if __name__ == "__main__":
    unittest.main()
