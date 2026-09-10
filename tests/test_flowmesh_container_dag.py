from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any, Mapping

from pathfinder.integrations.flowmesh.container_dag import (
    FLOWMESH_CONTAINER_DAG_PLAN_LEGACY_SCHEMA_VERSION,
    FLOWMESH_CONTAINER_DAG_RUN_LEGACY_SCHEMA_VERSION,
    FlowMeshContainerDagError,
    _document_sha256,
    derive_operation_lower_bounds,
    build_flowmesh_container_operation_workflow,
    list_linear_container_operation_dag_candidates,
    plan_flowmesh_container_operation_dag,
    run_flowmesh_container_operation_dag,
    verify_flowmesh_container_operation_dag_run,
    select_linear_container_operation_dag,
    verify_flowmesh_container_operation_dag_plan,
)
from pathfinder.integrations.flowmesh.contracts import (
    FlowMeshSettings,
    FlowMeshWorkerIdentity,
    SubmittedWorkflow,
    TerminalWorkflow,
    WorkflowValidation,
)
from pathfinder.simulator.container_contract import (
    CONTAINER_OPERATION_SCHEMA_VERSION,
)


def _operation(
    key: str,
    kind: str,
    node: str,
    dependencies: list[str],
    *,
    trial_key: str = "smoke-trial",
    logical_bytes: int = 4096,
    condition: dict[str, Any] | None = None,
    link_adapter: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": CONTAINER_OPERATION_SCHEMA_VERSION,
        "backend_id": "container-smoke",
        "portable_plan_sha256": "a" * 64,
        "operation_key": key,
        "trial_key": trial_key,
        "operation_id": key.rsplit("|", 1)[-1],
        "operation_kind": kind,
        "dependency_operation_keys": dependencies,
        "condition": condition,
        "object_id": "fixture-001",
        "representation_id": "raw_video",
        "logical_bytes": logical_bytes,
        "operation_adapter": "fixture-v1",
        "resource_adapter": None,
        "link_adapter": link_adapter,
        "cache_adapter": None,
        "task_executor": {"semantic_quality_enabled": False},
        "execution_node_id": node,
        "execution_container": f"node-{node.lower()}",
        "destination_node_id": node,
        "destination_container": f"node-{node.lower()}",
        "measure_actual_duration": True,
        "simulation_hint_used_as_measured_duration": False,
    }


def _chain() -> list[dict[str, Any]]:
    read = _operation("smoke-trial|read", "storage_read", "N3", [])
    transfer = _operation(
        "smoke-trial|transfer",
        "network_transfer",
        "N7",
        [read["operation_key"]],
    )
    compute = _operation(
        "smoke-trial|compute",
        "compute",
        "N6",
        [transfer["operation_key"]],
        logical_bytes=0,
    )
    return [read, transfer, compute]


def _condition(cache_key: str, equals: str = "hit") -> dict[str, Any]:
    """A cache hit/miss gate, in the frozen ledger's exact shape."""
    return {
        "cache_operation_key": cache_key,
        "cache_operation_id": cache_key.rsplit("|", 1)[-1],
        "equals": equals,
    }


def _cache_branch(trial_key: str, *, suffix: str = "") -> list[dict[str, Any]]:
    """A conditional cache-branch path, as a real D3/D7 trial contains.

    Structurally this is a complete read -> transfer chain; the only thing
    disqualifying it from a smoke is that every member is gated on a cache
    lookup result.
    """
    lookup = f"{trial_key}|lookup{suffix}"
    gate = _condition(lookup, "hit")
    read = _operation(
        f"{trial_key}|read-local{suffix}",
        "storage_read",
        "N5",
        [lookup],
        trial_key=trial_key,
        condition=gate,
    )
    transfer = _operation(
        f"{trial_key}|transfer-remote{suffix}",
        "network_transfer",
        "N5",
        [read["operation_key"]],
        trial_key=trial_key,
        condition=gate,
    )
    compute = _operation(
        f"{trial_key}|decode-cached{suffix}",
        "compute",
        "N7",
        [transfer["operation_key"]],
        trial_key=trial_key,
        logical_bytes=0,
        condition=gate,
    )
    lookup_row = _operation(
        lookup, "cache_read", "N5", [], trial_key=trial_key, logical_bytes=0
    )
    return [lookup_row, read, transfer, compute]


def _link(bandwidth: int, rtt_ms: float = 30.0) -> dict[str, Any]:
    return {
        "adapter": "application-rate-rtt-shaper-v1",
        "link_id": "N3-N8-edge",
        "source_node_id": "N3",
        "destination_node_id": "N8",
        "bandwidth_bytes_per_second": bandwidth,
        "round_trip_time_ms": rtt_ms,
        "required_capabilities": [],
    }


def _d4_chain() -> list[dict[str, Any]]:
    """A D4-style edge trial: 240 MB across a 1.25 MB/s link.

    The transfer alone cannot finish in under 192 s, so the historical fixed
    120 s API timeout would kill it mid-flight.
    """
    read = _operation("smoke-trial|read-raw", "storage_read", "N3", [],
                      logical_bytes=240_000_000)
    transfer = _operation(
        "smoke-trial|transfer-raw", "network_transfer", "N3",
        [read["operation_key"]], logical_bytes=240_000_000,
        link_adapter=_link(1_250_000),
    )
    compute = _operation("smoke-trial|decode", "compute", "N8",
                         [transfer["operation_key"]], logical_bytes=0)
    return [read, transfer, compute]


_D4_URLS = {"N3": "http://127.0.0.1:19083", "N8": "http://127.0.0.1:19088"}


def _freeze(root: Path, ledger: list[dict[str, Any]], **kwargs: Any) -> Any:
    source = root / "container_operations.jsonl"
    source.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in ledger),
        encoding="utf-8",
    )
    options: dict[str, Any] = {
        "container_operations_path": source,
        "node_api_urls": _D4_URLS,
        "worker_alias": "fixture-alias",
        "smoke_id": "d4-timeout-smoke",
        "output_dir": root / "plan",
    }
    options.update(kwargs)
    return plan_flowmesh_container_operation_dag(**options)


def _restamp(plan_dir: Path, mutate: Any) -> None:
    """Apply an edit and re-stamp both digests.

    Without re-stamping, the plan digest fires first and a test would prove
    nothing about the timeout or lower-bound guards.
    """
    import hashlib

    path = plan_dir / "flowmesh-container-dag-plan.json"
    plan = json.loads(path.read_text(encoding="utf-8"))
    mutate(plan)
    plan["plan_sha256"] = _document_sha256(plan, "plan_sha256")
    body = json.dumps(
        plan, indent=2, sort_keys=True, ensure_ascii=False
    ).encode("utf-8") + b"\n"
    path.write_bytes(body)
    sums = plan_dir / "SHA256SUMS"
    rows = []
    for line in sums.read_text(encoding="utf-8").splitlines():
        digest, _, name = line.partition("  ")
        if name == path.name:
            digest = hashlib.sha256(body).hexdigest()
        rows.append(f"{digest}  {name}")
    sums.write_text("\n".join(rows) + "\n", encoding="utf-8")


_RUN_URLS = {
    "N3": "http://127.0.0.1:29083",
    "N7": "http://127.0.0.1:29087",
    "N6": "http://127.0.0.1:29086",
}


def _plan_and_run(
    root: Path, *, mutate_result: Any = None, assigned_worker: str | None = None
) -> tuple[dict[str, Any], Path, Path]:
    source = root / "container_operations.jsonl"
    source.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in _chain()),
        encoding="utf-8",
    )
    plan_dir = root / "plan"
    plan_flowmesh_container_operation_dag(
        container_operations_path=source,
        node_api_urls=_RUN_URLS,
        worker_alias="container-smoke-worker",
        smoke_id="telemetry-smoke",
        trial_key="smoke-trial",
        output_dir=plan_dir,
    )
    client = FakeFlowMeshClient()
    client.mutate_result = mutate_result
    if assigned_worker is not None:
        client.assigned_worker = assigned_worker
    run_dir = root / "run"
    summary = run_flowmesh_container_operation_dag(
        plan_dir=plan_dir,
        output_dir=run_dir,
        client=client,
        settings=FlowMeshSettings(
            worker_alias="container-smoke-worker",
            validate_before_submit=True,
        ),
    )
    return summary, plan_dir, run_dir


def _records(run_dir: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in (
            run_dir / "flowmesh-container-dag-task-results.jsonl"
        ).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _restamp_run(run_dir: Path, mutate: Any) -> None:
    """Edit a run artifact and re-stamp its checksums."""
    import hashlib

    path = run_dir / "flowmesh-container-dag-task-results.jsonl"
    rows = _records(run_dir)
    mutate(rows)
    body = "".join(
        json.dumps(row, sort_keys=True, ensure_ascii=False) + "\n"
        for row in rows
    ).encode("utf-8")
    path.write_bytes(body)
    sums = run_dir / "SHA256SUMS"
    lines = []
    for line in sums.read_text(encoding="utf-8").splitlines():
        digest, _, name = line.partition("  ")
        if name == path.name:
            digest = hashlib.sha256(body).hexdigest()
        lines.append(f"{digest}  {name}")
    sums.write_text("\n".join(lines) + "\n", encoding="utf-8")


class ContainerTelemetryTest(unittest.TestCase):
    """Real measured container timing must survive into the run artifact.

    The point is auditable infrastructure telemetry, not a performance
    claim -- so every preserved number is validated and labelled with what it
    actually is.
    """

    def test_the_whitelisted_telemetry_is_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            summary, _, run_dir = _plan_and_run(Path(temporary))
            rows = {row["operation_kind"]: row for row in _records(run_dir)}
            self.assertEqual(12.5, rows["storage_read"]["service_time_ms"])
            self.assertEqual(96.03, rows["network_transfer"]["service_time_ms"])
            self.assertEqual(3.25, rows["compute"]["service_time_ms"])
            self.assertEqual(
                4.75,
                rows["storage_read"][
                    "fixture_materialization_ms_excluded_from_storage_measurement"
                ],
            )
            self.assertEqual(
                96_000.0,
                rows["network_transfer"]["application_shaping_target_ms"],
            )
            self.assertEqual(3, summary["task_result_count"])

    def test_non_whitelisted_container_fields_are_not_copied(self) -> None:
        # A payload digest or cache internal must not ride along.
        with tempfile.TemporaryDirectory() as temporary:
            _, _, run_dir = _plan_and_run(Path(temporary))
            blob = (
                run_dir / "flowmesh-container-dag-task-results.jsonl"
            ).read_text(encoding="utf-8")
            for leaked in ("payload_sha256", "cache_result", "cache_evictions"):
                self.assertNotIn(leaked, blob)
            for secret in ("api_key", "authorization", "bearer", "prompt"):
                self.assertNotIn(secret, blob.lower())

    def test_a_null_shaping_target_is_kept_only_off_the_transfer_path(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            _, _, run_dir = _plan_and_run(Path(temporary))
            rows = {row["operation_kind"]: row for row in _records(run_dir)}
            self.assertIsNone(rows["storage_read"]["application_shaping_target_ms"])
            self.assertIsNone(rows["compute"]["application_shaping_target_ms"])
            self.assertIsNotNone(
                rows["network_transfer"]["application_shaping_target_ms"]
            )

    def test_a_transfer_without_a_shaping_target_is_refused(self) -> None:
        def drop(body: dict[str, Any]) -> None:
            if body["operation_kind"] == "network_transfer":
                body["application_shaping_target_ms"] = None

        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(FlowMeshContainerDagError) as context:
                _plan_and_run(Path(temporary), mutate_result=drop)
        self.assertIn("shaping target", str(context.exception))

    def test_a_shaping_target_on_a_non_transfer_is_refused(self) -> None:
        def add(body: dict[str, Any]) -> None:
            if body["operation_kind"] == "compute":
                body["application_shaping_target_ms"] = 5.0

        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(FlowMeshContainerDagError):
                _plan_and_run(Path(temporary), mutate_result=add)

    def test_malformed_timing_refuses_the_run_artifact(self) -> None:
        cases = {
            "missing": lambda b: b.pop("service_time_ms"),
            "nan": lambda b: b.__setitem__("service_time_ms", float("nan")),
            "infinite": lambda b: b.__setitem__("service_time_ms", float("inf")),
            "negative": lambda b: b.__setitem__("service_time_ms", -1.0),
            "string": lambda b: b.__setitem__("service_time_ms", "12.5"),
            "boolean": lambda b: b.__setitem__("service_time_ms", True),
            "none": lambda b: b.__setitem__("service_time_ms", None),
        }
        for name, mutate in cases.items():
            with self.subTest(case=name):
                with tempfile.TemporaryDirectory() as temporary:
                    with self.assertRaises(FlowMeshContainerDagError):
                        _plan_and_run(Path(temporary), mutate_result=mutate)

    def test_missing_or_negative_materialization_is_refused(self) -> None:
        for mutate in (
            lambda b: b.pop(
                "fixture_materialization_ms_excluded_from_storage_measurement"
            ),
            lambda b: b.__setitem__(
                "fixture_materialization_ms_excluded_from_storage_measurement",
                -0.5,
            ),
        ):
            with self.subTest(mutate=mutate):
                with tempfile.TemporaryDirectory() as temporary:
                    with self.assertRaises(FlowMeshContainerDagError):
                        _plan_and_run(Path(temporary), mutate_result=mutate)

    def test_materialization_time_off_the_read_path_is_refused(self) -> None:
        def mislabel(body: dict[str, Any]) -> None:
            if body["operation_kind"] == "compute":
                body[
                    "fixture_materialization_ms_excluded_from_storage_measurement"
                ] = 2.0

        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(FlowMeshContainerDagError):
                _plan_and_run(Path(temporary), mutate_result=mislabel)

    def test_service_time_disagreeing_with_its_own_clock_is_refused(
        self,
    ) -> None:
        # Same-node consistency only; this is not a cross-container check.
        def skew(body: dict[str, Any]) -> None:
            body["service_time_ms"] = body["service_time_ms"] + 5.0

        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(FlowMeshContainerDagError) as context:
                _plan_and_run(Path(temporary), mutate_result=skew)
        self.assertIn("monotonic interval", str(context.exception))

    def test_inconsistent_bytes_refuse_the_run(self) -> None:
        for mutate in (
            lambda b: b.__setitem__("physical_bytes", b["physical_bytes"] + 1),
            lambda b: b.__setitem__("logical_bytes", b["logical_bytes"] + 1),
        ):
            with self.subTest(mutate=mutate):
                with tempfile.TemporaryDirectory() as temporary:
                    with self.assertRaises(FlowMeshContainerDagError):
                        _plan_and_run(Path(temporary), mutate_result=mutate)

    def test_the_aggregate_is_descriptive_and_claims_nothing_extra(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            summary, _, _ = _plan_and_run(Path(temporary))
            telemetry = summary["telemetry"]
            self.assertEqual(
                round(12.5 + 96.03 + 3.25, 6), telemetry["service_time_ms_sum"]
            )
            self.assertEqual(96.03, telemetry["service_time_ms_max"])
            self.assertEqual(3.25, telemetry["service_time_ms_min"])
            self.assertEqual(
                4.75,
                telemetry[
                    "fixture_materialization_ms_sum_excluded_from_storage_measurement"
                ],
            )
            self.assertEqual(3, telemetry["telemetry_complete_record_count"])
            # Explicitly not claimed.
            self.assertFalse(
                telemetry["service_time_ms_sum_is_end_to_end_latency"]
            )
            self.assertFalse(telemetry["network_throughput_derived"])
            self.assertFalse(telemetry["queue_time_measured"])
            for absent in (
                "throughput_bytes_per_second",
                "queue_time_ms",
                "end_to_end_latency_ms",
            ):
                self.assertNotIn(absent, telemetry)

    def test_the_summary_records_field_level_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            summary, _, _ = _plan_and_run(Path(temporary))
            provenance = summary["telemetry_provenance"]
            fields = provenance["fields"]
            self.assertIn(
                "measured inside the container operation",
                fields["service_time_ms"],
            )
            self.assertIn(
                "EXCLUDED from service_time_ms",
                fields[
                    "fixture_materialization_ms_excluded_from_storage_measurement"
                ],
            )
            self.assertIn(
                "CONFIGURED", fields["application_shaping_target_ms"]
            )
            self.assertIn(
                "not an independently measured network latency",
                fields["application_shaping_target_ms"],
            )
            joined = " ".join(provenance["disclaimers"])
            self.assertIn("No cross-container clock comparison", joined)
            self.assertIn("No queue time", joined)


class RunVerificationTest(unittest.TestCase):
    """Offline verification of a completed run artifact."""

    def test_a_complete_run_verifies_and_binds_to_its_plan(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            _, plan_dir, run_dir = _plan_and_run(Path(temporary))
            report = verify_flowmesh_container_operation_dag_run(
                run_dir, plan_dir=plan_dir
            )
            self.assertEqual("VERIFIED", report["status"])
            self.assertTrue(report["timing_recorded"])
            self.assertTrue(report["plan_binding_checked"])
            self.assertEqual(3, report["task_result_count"])
            self.assertEqual("wkr-77", report["worker_id"])
            self.assertFalse(report["eligible_for_scientific_claims"])

    def test_verification_without_a_plan_still_checks_the_artifact(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            _, _, run_dir = _plan_and_run(Path(temporary))
            report = verify_flowmesh_container_operation_dag_run(run_dir)
            self.assertEqual("VERIFIED", report["status"])
            self.assertFalse(report["plan_binding_checked"])

    def test_a_run_bound_to_a_different_plan_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _, _, run_dir = _plan_and_run(root)
            other = root / "other"
            other.mkdir()
            source = other / "container_operations.jsonl"
            source.write_text(
                "".join(
                    json.dumps(row, sort_keys=True) + "\n" for row in _chain()
                ),
                encoding="utf-8",
            )
            plan_flowmesh_container_operation_dag(
                container_operations_path=source,
                node_api_urls=_RUN_URLS,
                worker_alias="container-smoke-worker",
                smoke_id="a-different-smoke",
                trial_key="smoke-trial",
                output_dir=other / "plan",
            )
            with self.assertRaises(FlowMeshContainerDagError) as context:
                verify_flowmesh_container_operation_dag_run(
                    run_dir, plan_dir=other / "plan"
                )
            self.assertIn("not bound", str(context.exception))

    def test_a_tampered_task_result_fails_verification(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            _, _, run_dir = _plan_and_run(Path(temporary))
            _restamp_run(
                run_dir,
                lambda rows: rows[0].__setitem__("service_time_ms", 0.1),
            )
            with self.assertRaises(FlowMeshContainerDagError) as context:
                verify_flowmesh_container_operation_dag_run(run_dir)
            self.assertIn("aggregate does not match", str(context.exception))

    def test_a_tampered_worker_identity_fails_verification(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            _, _, run_dir = _plan_and_run(Path(temporary))
            _restamp_run(
                run_dir,
                lambda rows: rows[1].__setitem__("worker_id", "wkr-other"),
            )
            with self.assertRaises(FlowMeshContainerDagError) as context:
                verify_flowmesh_container_operation_dag_run(run_dir)
            self.assertIn("different worker", str(context.exception))

    def test_a_dropped_task_result_fails_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            _, _, run_dir = _plan_and_run(Path(temporary))
            _restamp_run(run_dir, lambda rows: rows.pop())
            with self.assertRaises(FlowMeshContainerDagError) as context:
                verify_flowmesh_container_operation_dag_run(run_dir)
            self.assertIn("do not cover", str(context.exception))

    def test_a_corrupted_checksum_fails_verification(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            _, _, run_dir = _plan_and_run(Path(temporary))
            path = run_dir / "flowmesh-container-dag-task-results.jsonl"
            path.write_text(path.read_text(encoding="utf-8") + " ", encoding="utf-8")
            with self.assertRaises(FlowMeshContainerDagError) as context:
                verify_flowmesh_container_operation_dag_run(run_dir)
            self.assertIn("checksum mismatch", str(context.exception))

    def test_a_legacy_run_is_labelled_timing_not_recorded(self) -> None:
        # A v1alpha1 run never preserved timing. It stays readable, is
        # labelled, and is not rewritten.
        import hashlib

        with tempfile.TemporaryDirectory() as temporary:
            _, _, run_dir = _plan_and_run(Path(temporary))
            summary_path = run_dir / "flowmesh-container-dag-run.json"
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            summary["schema_version"] = (
                FLOWMESH_CONTAINER_DAG_RUN_LEGACY_SCHEMA_VERSION
            )
            del summary["telemetry"]
            del summary["telemetry_provenance"]
            body = json.dumps(
                summary, indent=2, sort_keys=True, ensure_ascii=False
            ).encode("utf-8") + b"\n"
            summary_path.write_bytes(body)

            def strip(rows: list[dict[str, Any]]) -> None:
                for row in rows:
                    for field in (
                        "service_time_ms",
                        "fixture_materialization_ms_excluded_from_storage_measurement",
                        "application_shaping_target_ms",
                        "telemetry_provenance_version",
                    ):
                        row.pop(field, None)

            _restamp_run(run_dir, strip)
            sums = run_dir / "SHA256SUMS"
            lines = []
            for line in sums.read_text(encoding="utf-8").splitlines():
                digest, _, name = line.partition("  ")
                if name == summary_path.name:
                    digest = hashlib.sha256(body).hexdigest()
                lines.append(f"{digest}  {name}")
            sums.write_text("\n".join(lines) + "\n", encoding="utf-8")

            before = summary_path.read_bytes()
            report = verify_flowmesh_container_operation_dag_run(run_dir)
            self.assertEqual("VERIFIED", report["status"])
            self.assertFalse(report["timing_recorded"])
            self.assertEqual("not-recorded-legacy", report["telemetry_recording"])
            self.assertIsNone(report["telemetry"])
            self.assertEqual(before, summary_path.read_bytes())


class ApiTaskTimeoutTest(unittest.TestCase):
    """The API executor timeout must be planned, not fixed at 120 s.

    ``--task-timeout`` on the run command controls workflow polling, which
    cannot rescue a task the FlowMesh API executor has already abandoned.
    """

    def test_the_derived_bound_is_transfer_time_plus_one_round_trip(
        self,
    ) -> None:
        bounds = derive_operation_lower_bounds(_d4_chain())
        self.assertEqual(
            [0.0, 192.03, 0.0],
            [row["lower_bound_seconds"] for row in bounds],
        )
        self.assertEqual(
            ["not-derivable", "link-rate-and-round-trip", "not-derivable"],
            [row["basis"] for row in bounds],
        )
        # Storage and compute carry no rate in the frozen record, so no
        # duration is invented for them.
        self.assertEqual({}, bounds[0]["components"])
        self.assertEqual(
            {"transfer_seconds": 192.0, "round_trip_seconds": 0.03},
            bounds[1]["components"],
        )

    def test_a_240mb_transfer_rejects_120_seconds(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(FlowMeshContainerDagError) as context:
                _freeze(Path(temporary), _d4_chain(),
                        api_task_timeout_seconds=120)
        message = str(context.exception)
        self.assertIn("smoke-trial|transfer-raw", message)
        self.assertIn("120s", message)
        self.assertIn("192.03s", message)

    def test_the_default_timeout_is_the_legacy_120_seconds(self) -> None:
        # The default is unchanged, so an oversized plan is refused rather
        # than silently widened.
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(FlowMeshContainerDagError) as context:
                _freeze(Path(temporary), _d4_chain())
        self.assertIn("requested 120s", str(context.exception))

    def test_a_conservative_300_second_timeout_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            payload = _freeze(root, _d4_chain(), api_task_timeout_seconds=300)
            self.assertEqual("FROZEN_WORKFLOW_INPUTS", payload["status"])
            self.assertEqual(300, payload["api_task_timeout_seconds"])
            self.assertEqual(192.03, payload["max_operation_lower_bound_seconds"])
            plan = json.loads(
                (root / "plan" / "flowmesh-container-dag-plan.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(300, plan["api_task_timeout_seconds"])
            self.assertEqual(
                derive_operation_lower_bounds(plan["operations"]),
                plan["operation_lower_bound_seconds"],
            )

    def test_the_frozen_timeout_reaches_every_generated_api_task(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _freeze(root, _d4_chain(), api_task_timeout_seconds=300)
            template = json.loads(
                (
                    root / "plan"
                    / "flowmesh-container-dag-workflow-template.json"
                ).read_text(encoding="utf-8")
            )
            self.assertEqual(
                [300, 300, 300],
                [
                    node["spec"]["api"]["timeout_sec"]
                    for node in template["spec"]["graph"]["nodes"]
                ],
            )
        workflow = build_flowmesh_container_operation_workflow(
            _d4_chain(),
            node_api_urls=_D4_URLS,
            selected_worker_id="worker-1",
            smoke_id="d4",
            api_task_timeout_seconds=300,
        )
        self.assertEqual(
            [300, 300, 300],
            [
                node["spec"]["api"]["timeout_sec"]
                for node in workflow["spec"]["graph"]["nodes"]
            ],
        )

    def test_the_workflow_builder_also_refuses_an_impossible_timeout(
        self,
    ) -> None:
        with self.assertRaises(FlowMeshContainerDagError):
            build_flowmesh_container_operation_workflow(
                _d4_chain(),
                node_api_urls=_D4_URLS,
                selected_worker_id="worker-1",
                smoke_id="d4",
                api_task_timeout_seconds=120,
            )

    def test_verification_reports_the_timeout_and_bound_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _freeze(root, _d4_chain(), api_task_timeout_seconds=300)
            report = verify_flowmesh_container_operation_dag_plan(
                root / "plan"
            )
            self.assertEqual("VERIFIED", report["status"])
            self.assertEqual(300, report["api_task_timeout_seconds"])
            self.assertEqual("plan", report["api_task_timeout_source"])
            self.assertEqual(
                192.03, report["max_operation_lower_bound_seconds"]
            )
            self.assertEqual(
                ["not-derivable", "link-rate-and-round-trip", "not-derivable"],
                [
                    row["basis"]
                    for row in report["operation_lower_bound_seconds"]
                ],
            )

    def test_a_tampered_timeout_fails_verification(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _freeze(root, _d4_chain(), api_task_timeout_seconds=300)
            _restamp(
                root / "plan",
                lambda plan: plan.__setitem__("api_task_timeout_seconds", 120),
            )
            with self.assertRaises(FlowMeshContainerDagError) as context:
                verify_flowmesh_container_operation_dag_plan(root / "plan")
            self.assertIn("192.03s", str(context.exception))

    def test_a_tampered_lower_bound_record_fails_verification(self) -> None:
        # Shrinking the recorded bound would otherwise make an impossible
        # timeout look adequate; the bound is re-derived from the operations.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _freeze(root, _d4_chain(), api_task_timeout_seconds=300)

            def shrink(plan: dict[str, Any]) -> None:
                plan["operation_lower_bound_seconds"][1][
                    "lower_bound_seconds"
                ] = 1.0
                plan["max_operation_lower_bound_seconds"] = 1.0

            _restamp(root / "plan", shrink)
            with self.assertRaises(FlowMeshContainerDagError) as context:
                verify_flowmesh_container_operation_dag_plan(root / "plan")
            self.assertIn("does not match its operations", str(context.exception))

    def test_a_tampered_max_bound_alone_fails_verification(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _freeze(root, _d4_chain(), api_task_timeout_seconds=300)
            _restamp(
                root / "plan",
                lambda plan: plan.__setitem__(
                    "max_operation_lower_bound_seconds", 1.0
                ),
            )
            with self.assertRaises(FlowMeshContainerDagError):
                verify_flowmesh_container_operation_dag_plan(root / "plan")

    def test_a_nonpositive_timeout_is_refused(self) -> None:
        for value in (0, -1, 1.5, True, "300"):
            with self.subTest(timeout=value):
                with tempfile.TemporaryDirectory() as temporary:
                    with self.assertRaises(FlowMeshContainerDagError):
                        _freeze(Path(temporary), _d4_chain(),
                                api_task_timeout_seconds=value)

    def test_a_legacy_plan_verifies_under_its_recorded_semantics(self) -> None:
        # A v1alpha1 plan predates the field entirely. It must stay
        # verifiable, be reported as legacy, and not be rewritten.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _freeze(root, _d4_chain(), api_task_timeout_seconds=300)
            plan_path = root / "plan" / "flowmesh-container-dag-plan.json"

            def downgrade(plan: dict[str, Any]) -> None:
                plan["schema_version"] = (
                    FLOWMESH_CONTAINER_DAG_PLAN_LEGACY_SCHEMA_VERSION
                )
                del plan["api_task_timeout_seconds"]
                del plan["operation_lower_bound_seconds"]
                del plan["max_operation_lower_bound_seconds"]

            _restamp(root / "plan", downgrade)
            before = plan_path.read_bytes()
            report = verify_flowmesh_container_operation_dag_plan(
                root / "plan"
            )
            self.assertEqual("VERIFIED", report["status"])
            self.assertEqual(120, report["api_task_timeout_seconds"])
            self.assertEqual(
                "legacy-fixed-default", report["api_task_timeout_source"]
            )
            self.assertEqual(
                FLOWMESH_CONTAINER_DAG_PLAN_LEGACY_SCHEMA_VERSION,
                report["schema_version"],
            )
            # Verification never rewrites the plan on disk.
            self.assertEqual(before, plan_path.read_bytes())


class ConditionalLedgerTest(unittest.TestCase):
    """A full frozen ledger legitimately contains cache-branch operations.

    Rejecting the whole file because it is complete was the bug. These tests
    pin the corrected boundary: structural validation applies to every row,
    while "must be unconditional" applies only to chain selection.
    """

    def test_a_conditional_branch_beside_a_valid_chain_is_ignored(self) -> None:
        ledger = _chain() + _cache_branch("cached-trial")
        read, transfer, compute = select_linear_container_operation_dag(ledger)
        self.assertEqual("smoke-trial|read", read["operation_key"])
        self.assertEqual("smoke-trial|transfer", transfer["operation_key"])
        self.assertEqual("smoke-trial|compute", compute["operation_key"])
        for row in (read, transfer, compute):
            self.assertIsNone(row["condition"])

    def test_candidates_list_the_unconditional_trial_only(self) -> None:
        ledger = _chain() + _cache_branch("cached-trial")
        candidates = list_linear_container_operation_dag_candidates(ledger)
        self.assertEqual(
            ["smoke-trial"], [item["trial_key"] for item in candidates]
        )
        self.assertEqual(
            ["storage_read", "network_transfer", "compute"],
            candidates[0]["operation_kinds"],
        )

    def test_a_ledger_of_only_conditional_chains_yields_no_candidate(
        self,
    ) -> None:
        ledger = _cache_branch("cached-trial") + _cache_branch("other-trial")
        self.assertEqual(
            [], list_linear_container_operation_dag_candidates(ledger)
        )

    def test_only_conditional_chains_refuse_planning_clearly(self) -> None:
        ledger = _cache_branch("cached-trial")
        with self.assertRaises(FlowMeshContainerDagError) as context:
            select_linear_container_operation_dag(ledger)
        self.assertIn("unconditional", str(context.exception))

    def test_a_conditional_member_is_never_selected(self) -> None:
        # Each member in turn is gated; every case must refuse rather than
        # silently fall back to the conditional operation.
        gate = _condition("smoke-trial|lookup")
        for index, name in enumerate(("read", "transfer", "compute")):
            with self.subTest(conditional_member=name):
                ledger = _chain()
                ledger[index] = dict(ledger[index], condition=gate)
                with self.assertRaises(FlowMeshContainerDagError):
                    select_linear_container_operation_dag(ledger)

    def test_a_conditional_omitted_predecessor_disqualifies_the_chain(
        self,
    ) -> None:
        # The read's predecessor is non-physical and would normally just be
        # disclosed as omitted -- but a conditional marker means the chain
        # itself only exists on one branch.
        gate = _condition("smoke-trial|lookup")
        marker = _operation(
            "smoke-trial|schedule",
            "control",
            "N1",
            [],
            logical_bytes=0,
            condition=gate,
        )
        ledger = _chain()
        ledger[0] = dict(ledger[0], dependency_operation_keys=[marker["operation_key"]])
        with self.assertRaises(FlowMeshContainerDagError):
            select_linear_container_operation_dag(ledger + [marker])

    def test_a_conditional_row_still_gets_structural_validation(self) -> None:
        for broken, reason in (
            ({"cache_operation_key": "k", "cache_operation_id": "i",
              "equals": "maybe"}, "hit or miss"),
            ({"cache_operation_id": "i", "equals": "hit"},
             "cache_operation_key"),
            ("hit", "must be an object or null"),
        ):
            with self.subTest(condition=broken):
                ledger = _chain() + [
                    _operation(
                        "cached-trial|read-local",
                        "storage_read",
                        "N5",
                        [],
                        trial_key="cached-trial",
                        condition=broken,  # type: ignore[arg-type]
                    )
                ]
                with self.assertRaises(FlowMeshContainerDagError) as context:
                    select_linear_container_operation_dag(ledger)
                self.assertIn(reason, str(context.exception))

    def test_planning_from_a_mixed_ledger_binds_the_full_source_hash(
        self,
    ) -> None:
        ledger = _chain() + _cache_branch("cached-trial")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "container_operations.jsonl"
            source.write_text(
                "".join(json.dumps(row, sort_keys=True) + "\n" for row in ledger),
                encoding="utf-8",
            )
            payload = plan_flowmesh_container_operation_dag(
                container_operations_path=source,
                node_api_urls={
                    "N3": "http://127.0.0.1:19083",
                    "N7": "http://127.0.0.1:19087",
                    "N6": "http://127.0.0.1:19086",
                },
                worker_alias="fixture-alias",
                smoke_id="mixed-ledger-smoke",
                output_dir=root / "plan",
            )
            self.assertEqual("FROZEN_WORKFLOW_INPUTS", payload["status"])
            self.assertEqual("smoke-trial", payload["trial_key"])
            plan = json.loads(
                (root / "plan" / "flowmesh-container-dag-plan.json").read_text(
                    encoding="utf-8"
                )
            )
            # Provenance binds to the intact ledger, conditional rows included,
            # not to a filtered subset.
            import hashlib

            self.assertEqual(
                hashlib.sha256(source.read_bytes()).hexdigest(),
                plan["container_operations_source_sha256"],
            )
            self.assertTrue(
                all(row["condition"] is None for row in plan["operations"])
            )
            self.assertEqual(
                "VERIFIED",
                verify_flowmesh_container_operation_dag_plan(
                    root / "plan"
                )["status"],
            )

    def test_a_hand_edited_conditional_plan_fails_verification(self) -> None:
        ledger = _chain()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "container_operations.jsonl"
            source.write_text(
                "".join(json.dumps(row, sort_keys=True) + "\n" for row in ledger),
                encoding="utf-8",
            )
            plan_flowmesh_container_operation_dag(
                container_operations_path=source,
                node_api_urls={
                    "N3": "http://127.0.0.1:19083",
                    "N7": "http://127.0.0.1:19087",
                    "N6": "http://127.0.0.1:19086",
                },
                worker_alias="fixture-alias",
                smoke_id="edited-plan",
                output_dir=root / "plan",
            )
            path = root / "plan" / "flowmesh-container-dag-plan.json"
            plan = json.loads(path.read_text(encoding="utf-8"))
            plan["operations"][0]["condition"] = _condition("smoke-trial|lookup")
            # Re-stamp the plan's own self-digest as well, so the forged plan
            # is internally consistent and only the conditional guard can
            # reject it.
            plan["plan_sha256"] = _document_sha256(plan, "plan_sha256")
            body = json.dumps(
                plan, indent=2, sort_keys=True, ensure_ascii=False
            ).encode("utf-8") + b"\n"
            path.write_bytes(body)
            # Re-stamp the checksum file, otherwise the digest check fires
            # first and this proves nothing about the conditional guard.
            import hashlib

            sums = root / "plan" / "SHA256SUMS"
            rows = []
            for line in sums.read_text(encoding="utf-8").splitlines():
                digest, _, name = line.partition("  ")
                if name == path.name:
                    digest = hashlib.sha256(body).hexdigest()
                rows.append(f"{digest}  {name}")
            sums.write_text("\n".join(rows) + "\n", encoding="utf-8")

            with self.assertRaises(FlowMeshContainerDagError) as context:
                verify_flowmesh_container_operation_dag_plan(root / "plan")
            self.assertIn("conditional operation", str(context.exception))


#: Timings a real ContainerNodeRuntime returns. The monotonic pair is kept
#: consistent with service_time_ms so the same-record clock check passes.
_SERVICE_MS = {"storage_read": 12.5, "network_transfer": 96.03, "compute": 3.25}


def _container_body(
    operation: Mapping[str, Any], physical_bytes: int
) -> dict[str, Any]:
    kind = operation["operation_kind"]
    service_ms = _SERVICE_MS[kind]
    started = 1_000_000_000
    return {
        "status": "completed",
        "outcome_type": "completed",
        "telemetry_complete": True,
        "credentials_recorded": False,
        "idempotent_replay": False,
        "operation_key": operation["operation_key"],
        "operation_kind": kind,
        "execution_node_id": operation["execution_node_id"],
        "logical_bytes": operation["logical_bytes"],
        "physical_bytes": physical_bytes,
        "started_monotonic_ns": started,
        "finished_monotonic_ns": started + int(service_ms * 1_000_000),
        "service_time_ms": service_ms,
        "fixture_materialization_ms_excluded_from_storage_measurement": (
            4.75 if kind == "storage_read" else 0.0
        ),
        "application_shaping_target_ms": (
            96_000.0 if kind == "network_transfer" else None
        ),
        # Fields the whitelist must NOT carry into the artifact.
        "payload_sha256": "c" * 64,
        "cache_result": None,
        "cache_evictions": [],
    }


class FakeFlowMeshClient:
    def __init__(self, *, assigned_worker: str = "wkr-77") -> None:
        self.assigned_worker = assigned_worker
        self.validated: list[dict[str, Any]] = []
        self.submitted: dict[str, Any] | None = None
        self.results: dict[str, dict[str, Any]] = {}

    def describe_current_worker(
        self,
        *,
        worker_id: str | None = None,
        alias: str | None = None,
    ) -> FlowMeshWorkerIdentity:
        if alias != "container-smoke-worker" or worker_id is not None:
            raise RuntimeError("unexpected worker selector")
        return FlowMeshWorkerIdentity(
            worker_id=self.assigned_worker,
            alias=alias,
            status="IDLE",
        )

    mutate_result: Any = None

    def validate(self, workflow: Mapping[str, Any]) -> WorkflowValidation:
        self.validated.append(dict(workflow))
        return WorkflowValidation(ok=True)

    def submit(self, workflow: Mapping[str, Any]) -> SubmittedWorkflow:
        self.submitted = dict(workflow)
        nodes = workflow["spec"]["graph"]["nodes"]
        task_ids = tuple(f"tsk-{index}" for index in range(len(nodes)))
        for task_id, node in zip(task_ids, nodes):
            operation = node["spec"]["api"]["body"]
            physical_bytes = (
                operation["logical_bytes"]
                if operation["operation_kind"]
                in ("storage_read", "network_transfer")
                else 0
            )
            body = _container_body(operation, physical_bytes)
            if self.mutate_result is not None:
                self.mutate_result(body)
            self.results[task_id] = {
                "executor": "api",
                "ok": True,
                "status_code": 200,
                "text": json.dumps(body),
            }
        return SubmittedWorkflow("wfl-container-smoke", task_ids)

    def wait(
        self,
        workflow_id: str,
        poll_interval_seconds: float,
    ) -> TerminalWorkflow:
        return TerminalWorkflow(workflow_id, "DONE")

    def retrieve_result(self, task_id: str) -> dict[str, Any]:
        return self.results[task_id]

    def describe_task_failure(self, task_id: str) -> dict[str, Any]:
        return {"task_status": "DONE", "assigned_worker": self.assigned_worker}


class FlowMeshContainerDagTest(unittest.TestCase):
    def test_selects_exact_linear_chain(self) -> None:
        selected = select_linear_container_operation_dag(
            _chain(), trial_key="smoke-trial"
        )
        self.assertEqual(
            ["storage_read", "network_transfer", "compute"],
            [row["operation_kind"] for row in selected],
        )

    def test_refuses_ambiguous_chain_without_trial_pin(self) -> None:
        alternative = _chain()
        for item in alternative:
            item["trial_key"] = "second-trial"
            item["operation_key"] = item["operation_key"].replace(
                "smoke-trial", "second-trial"
            )
        alternative[1]["dependency_operation_keys"] = [
            alternative[0]["operation_key"]
        ]
        alternative[2]["dependency_operation_keys"] = [
            alternative[1]["operation_key"]
        ]
        with self.assertRaises(FlowMeshContainerDagError) as context:
            select_linear_container_operation_dag(_chain() + alternative)
        self.assertIn("multiple linear", str(context.exception))

    def test_workflow_is_a_pinned_three_node_flowmesh_graph(self) -> None:
        workflow = build_flowmesh_container_operation_workflow(
            _chain(),
            node_api_urls={
                "N3": "http://127.0.0.1:29083",
                "N7": "http://127.0.0.1:29087",
                "N6": "http://127.0.0.1:29086",
            },
            selected_worker_id="wkr-77",
            smoke_id="dag-smoke-001",
        )
        self.assertEqual("wkr-77", workflow["metadata"]["annotations"]["schedule_hint"]["selected_worker"])
        nodes = workflow["spec"]["graph"]["nodes"]
        self.assertEqual(["storage-read", "network-transfer", "compute"], [node["name"] for node in nodes])
        self.assertNotIn("dependsOn", nodes[0])
        self.assertEqual(["storage-read"], nodes[1]["dependsOn"])
        self.assertEqual(["network-transfer"], nodes[2]["dependsOn"])
        self.assertTrue(
            nodes[0]["spec"]["api"]["url"].endswith(
                "/v1/operations/execute"
            )
        )

    def test_control_predecessor_is_disclosed_but_index_predecessor_is_not_skipped(self) -> None:
        schedule = _operation("smoke-trial|schedule", "control", "N1", [])
        chain = _chain()
        chain[0]["dependency_operation_keys"] = [schedule["operation_key"]]
        selected = select_linear_container_operation_dag(
            [schedule] + chain, trial_key="smoke-trial"
        )
        self.assertEqual("smoke-trial|read", selected[0]["operation_key"])
        candidates = list_linear_container_operation_dag_candidates(
            [schedule] + chain
        )
        self.assertEqual([schedule["operation_key"]], candidates[0]["omitted_nonphysical_predecessor_operation_keys"])

        index = _operation("smoke-trial|index", "index_query", "N2", [])
        chain[0]["dependency_operation_keys"] = [index["operation_key"]]
        self.assertEqual(
            [],
            list_linear_container_operation_dag_candidates([index] + chain),
        )

    def test_plan_and_fake_flowmesh_run_are_bound_and_complete(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            operations_path = root / "container_operations.jsonl"
            operations_path.write_text(
                "".join(json.dumps(row) + "\n" for row in _chain()),
                encoding="utf-8",
            )
            plan_dir = root / "plan"
            result = plan_flowmesh_container_operation_dag(
                container_operations_path=operations_path,
                node_api_urls={
                    "N3": "http://127.0.0.1:29083",
                    "N7": "http://127.0.0.1:29087",
                    "N6": "http://127.0.0.1:29086",
                },
                worker_alias="container-smoke-worker",
                smoke_id="dag-smoke-001",
                trial_key="smoke-trial",
                output_dir=plan_dir,
            )
            self.assertEqual("FROZEN_WORKFLOW_INPUTS", result["status"])
            self.assertEqual(
                "VERIFIED",
                verify_flowmesh_container_operation_dag_plan(plan_dir)["status"],
            )
            client = FakeFlowMeshClient()
            run = run_flowmesh_container_operation_dag(
                plan_dir=plan_dir,
                output_dir=root / "run",
                client=client,
                settings=FlowMeshSettings(
                    worker_alias="container-smoke-worker",
                    validate_before_submit=True,
                ),
            )
            self.assertEqual("COMPLETE", run["status"])
            self.assertEqual(3, run["task_result_count"])
            self.assertFalse(run["llm_called"])
            self.assertFalse(run["semantic_task_quality_evaluated"])
            self.assertTrue(client.validated)
            self.assertEqual("wkr-77", client.submitted["metadata"]["annotations"]["schedule_hint"]["selected_worker"])

    def test_run_refuses_wrong_assigned_worker(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            operations_path = root / "container_operations.jsonl"
            operations_path.write_text(
                "".join(json.dumps(row) + "\n" for row in _chain()),
                encoding="utf-8",
            )
            plan_flowmesh_container_operation_dag(
                container_operations_path=operations_path,
                node_api_urls={
                    "N3": "http://127.0.0.1:29083",
                    "N7": "http://127.0.0.1:29087",
                    "N6": "http://127.0.0.1:29086",
                },
                worker_alias="container-smoke-worker",
                smoke_id="dag-smoke-001",
                trial_key="smoke-trial",
                output_dir=root / "plan",
            )
            client = FakeFlowMeshClient(assigned_worker="wkr-wrong")
            # The client reports wkr-wrong at pin resolution too, so alter the
            # finished task metadata after construction to simulate a scheduler
            # pin violation rather than a Root-resolution failure.
            client.describe_task_failure = lambda task_id: {
                "task_status": "DONE",
                "assigned_worker": "wkr-other",
            }
            with self.assertRaises(FlowMeshContainerDagError) as context:
                run_flowmesh_container_operation_dag(
                    plan_dir=root / "plan",
                    output_dir=root / "run",
                    client=client,
                    settings=FlowMeshSettings(
                        worker_alias="container-smoke-worker",
                        validate_before_submit=True,
                    ),
                )
            self.assertIn("other than the pin", str(context.exception))
            self.assertFalse((root / "run").exists())


if __name__ == "__main__":
    unittest.main()
