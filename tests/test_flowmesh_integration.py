from __future__ import annotations

import copy
import json
import os
import sys
import tempfile
import unittest
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping

from pathfinder.config import load_config
from pathfinder.data_agent_client import (
    DataAgentAccessRequest,
    DataAgentAccessResult,
    DataAgentAccessTelemetry,
    DataAgentPayload,
    DataAgentTelemetryQuiescenceError,
)
from pathfinder.integrations.flowmesh.adapter import (
    FlowMeshAgentAdapter,
    FlowMeshPinningError,
    FlowMeshRunError,
    extract_agent_answer,
)
from pathfinder.integrations.flowmesh.client import WorkerResolutionError
from pathfinder.integrations.flowmesh.contracts import (
    FlowMeshAgentRunRequest,
    FlowMeshSettings,
    SubmittedWorkflow,
    TerminalWorkflow,
    WorkflowValidation,
)
from pathfinder.integrations.flowmesh.data_agent_backend import (
    RemoteDataAgentBackend,
)
from pathfinder.integrations.flowmesh.gateway import (
    AccessGateway,
    EmulatedRepresentationBackend,
    SQLiteSessionStore,
)
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
    def __init__(
        self,
        *,
        workers_by_alias: Mapping[str, list[str]] | None = None,
        validation: WorkflowValidation | None = None,
    ) -> None:
        self.submitted_workflow: Mapping[str, Any] | None = None
        self.validated_workflows: list[Mapping[str, Any]] = []
        self.alias_lookups: list[str] = []
        self._workers_by_alias = dict(workers_by_alias or {})
        self._validation = validation or WorkflowValidation(ok=True)

    def submit(self, workflow: Mapping[str, Any]) -> SubmittedWorkflow:
        self.submitted_workflow = workflow
        return SubmittedWorkflow(
            workflow_id="wfl-test",
            task_ids=("tsk-test",),
        )

    def validate(self, workflow: Mapping[str, Any]) -> WorkflowValidation:
        self.validated_workflows.append(workflow)
        return self._validation

    def resolve_worker_alias(self, alias: str) -> str:
        self.alias_lookups.append(alias)
        matches = self._workers_by_alias.get(alias, [])
        if not matches:
            raise WorkerResolutionError(
                f"FlowMesh worker alias '{alias}' matched no worker"
            )
        if len(matches) > 1:
            raise WorkerResolutionError(
                f"FlowMesh worker alias '{alias}' matched "
                f"{len(matches)} workers"
            )
        return matches[0]

    def wait(
        self,
        workflow_id: str,
        poll_interval_seconds: float,
    ) -> TerminalWorkflow:
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

    def access(
        self,
        request: DataAgentAccessRequest,
    ) -> DataAgentAccessResult:
        return DataAgentAccessResult(
            access_id=request.access_id,
            payload=DataAgentPayload(
                kind="artifact_uri",
                media_type="application/octet-stream",
                value=f"http://data-agent.invalid/v1/artifacts/{request.access_id}",
                sha256=sha256(b"artifact").hexdigest(),
            ),
            service_latency_ms=40.0,
            client_round_trip_ms=55.0,
            realized_cost=0.5,
            bytes_read=4096,
            location="remote_digest_service",
            object_id=request.object_id,
            object_catalog_version="flowmesh-catalog-v1",
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

    def wait(
        self,
        workflow_id: str,
        poll_interval_seconds: float,
    ) -> TerminalWorkflow:
        assert self.submitted_workflow is not None
        annotations = self.submitted_workflow["metadata"]["annotations"]
        session_id = annotations["custom"]["pathfinder_session_id"]
        self.access_responses.append(
            self.gateway.access_representation(
                session_id,
                "multimodal_digest",
            )
        )
        return super().wait(workflow_id, poll_interval_seconds)


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
        self.assertTrue(client.access_responses[0]["ok"])
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
        client = FakeFlowMeshClient()
        FlowMeshAgentAdapter(
            client,
            self.gateway,
            FlowMeshSettings(worker_id="wkr-16"),
        ).run(self.request())
        self.assertEqual(
            "wkr-16",
            workflow_selected_worker(client.submitted_workflow),
        )
        # An explicit ID must not trigger an alias lookup.
        self.assertEqual([], client.alias_lookups)

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
