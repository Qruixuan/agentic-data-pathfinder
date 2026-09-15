from __future__ import annotations

import hashlib
import io
import json
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

import pathfinder.simulator as simulator_api
from pathfinder.cli import main as cli_main
from pathfinder.simulator.full_flow_cache import FullFlowArtifactCache
from pathfinder.simulator.full_flow_w4_candidate_coordinator import (
    run_full_flow_w4_candidate_coordinator,
)
from pathfinder.simulator.full_flow_w4_candidate_routes import (
    freeze_full_flow_w4_candidate_routes,
)
from pathfinder.simulator.full_flow_w4_live_executor import (
    CROSSWALK_NAME,
    RECEIPT_NAME,
    FullFlowW4LiveExecutorError,
    LiveW4CandidateOperationExecutor,
    W4IndexDeployment,
    W4LiveComponents,
    W4_ARTIFACT_ACCESS_RESULT_SCHEMA_VERSION,
    W4_BYTE_TRANSFER_RESULT_SCHEMA_VERSION,
    W4_CONTROL_RESULT_SCHEMA_VERSION,
    W4_SEMANTIC_RANKING_RESULT_SCHEMA_VERSION,
    freeze_full_flow_w4_component_execution_receipt,
    freeze_full_flow_w4_index_artifact_crosswalk,
    verify_full_flow_w4_component_execution_receipt,
    verify_full_flow_w4_index_artifact_crosswalk,
)
from pathfinder.simulator.index_service import (
    INDEX_SOURCE_SCHEMA_VERSION,
    N2IndexService,
    build_n2_index_package,
    verify_n2_index_package,
)
from tests import test_simulator_full_flow_w4_candidate_routes as route_fixture


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


class Admission:
    def admit(self, request):
        return {
            "schema_version": W4_CONTROL_RESULT_SCHEMA_VERSION,
            "status": "COMPLETED",
            "execution_token": request["execution_token"],
            "operation_key": request["operation_key"],
            "request_sha256": _sha(_canonical(request)),
            "accepted": True,
            "ranking_sha256": None,
            "service_time_ms": 0.1,
            "telemetry_complete": True,
            "flowmesh_workflow_submitted": False,
            "credentials_recorded": False,
        }


class RankingReturn:
    def return_ranking(self, request):
        return {
            "schema_version": W4_CONTROL_RESULT_SCHEMA_VERSION,
            "status": "COMPLETED",
            "execution_token": request["execution_token"],
            "operation_key": request["operation_key"],
            "request_sha256": _sha(_canonical(request)),
            "accepted": False,
            "ranking_sha256": request["ranking_sha256"],
            "service_time_ms": 0.1,
            "telemetry_complete": True,
            "flowmesh_workflow_submitted": False,
            "credentials_recorded": False,
        }


class ArtifactAccess:
    def __init__(self, payloads, *, corrupt=False):
        self.payloads = payloads
        self.corrupt = corrupt
        self.calls = []

    def access(self, request):
        self.calls.append(dict(request))
        identity = request["artifact_identity"]
        payload = self.payloads[
            (request["object_id"], identity["representation_id"])
        ]
        content_range = request["exact_content_range"]
        if content_range is None:
            start, end = 0, len(payload) - 1
        else:
            start = content_range["range_start"]
            end = content_range["range_end"]
            payload = payload[start : end + 1]
        if self.corrupt:
            payload = payload + b"x"
        return {
            "schema_version": W4_ARTIFACT_ACCESS_RESULT_SCHEMA_VERSION,
            "status": "COMPLETED",
            "execution_token": request["execution_token"],
            "operation_key": request["operation_key"],
            "request_sha256": _sha(_canonical(request)),
            "node_id": request["node_id"],
            "object_id": request["object_id"],
            "representation_id": identity["representation_id"],
            "content_sha256": _sha(payload),
            "size_bytes": len(payload),
            "range_start": start,
            "range_end": end,
            "payload": payload,
            "service_time_ms": 0.2,
            "telemetry_complete": True,
            "flowmesh_workflow_submitted": False,
            "credentials_recorded": False,
        }


class Transport:
    def __init__(self, *, corrupt=False):
        self.corrupt = corrupt
        self.calls = []

    def transfer(self, request, payload):
        self.calls.append((dict(request), payload))
        returned = payload + b"x" if self.corrupt else payload
        return {
            "schema_version": W4_BYTE_TRANSFER_RESULT_SCHEMA_VERSION,
            "status": "COMPLETED",
            "execution_token": request["execution_token"],
            "operation_key": request["operation_key"],
            "request_sha256": _sha(_canonical(request)),
            "source_node_id": request["source_node_id"],
            "destination_node_id": request["destination_node_id"],
            "content_sha256": _sha(returned),
            "size_bytes": len(returned),
            "payload": returned,
            "service_time_ms": 0.3,
            "telemetry_complete": True,
            "flowmesh_workflow_submitted": False,
            "credentials_recorded": False,
        }


class SemanticRanker:
    def __init__(self, *, incomplete=False):
        self.incomplete = incomplete
        self.calls = []

    def rank(self, request):
        self.calls.append(dict(request))
        fallback = request["fallback_ranking"]
        prepared = [row["object_id"] for row in request["candidate_inputs"]]
        if fallback is None:
            ranking = sorted(request["candidate_object_ids"], reverse=True)
        else:
            prepared_set = set(prepared)
            prefix = [value for value in fallback if value in prepared_set]
            tail = [value for value in fallback if value not in prepared_set]
            ranking = [*reversed(prefix), *tail]
        if self.incomplete:
            ranking = ranking[:-1]
        candidate_digest = _sha(_canonical(request["candidate_inputs"]))
        fallback_digest = None if fallback is None else _sha(_canonical(fallback))
        return {
            "schema_version": W4_SEMANTIC_RANKING_RESULT_SCHEMA_VERSION,
            "status": "COMPLETED",
            "execution_token": request["execution_token"],
            "operation_key": request["operation_key"],
            "request_sha256": _sha(_canonical(request)),
            "ranked_object_ids": ranking,
            "candidate_inputs_sha256": candidate_digest,
            "fallback_ranking_sha256": fallback_digest,
            "complete_output_ranking": True,
            "service_time_ms": 0.4,
            "telemetry_complete": True,
            "llm_called": True,
            "flowmesh_workflow_submitted": False,
            "credentials_recorded": False,
        }


class CacheAdapter:
    def __init__(self, cache):
        self.cache = cache

    def get(self, **kwargs):
        return self.cache.lookup(**kwargs)

    def put(self, **kwargs):
        return self.cache.put(**kwargs)


class FullFlowW4LiveExecutorTest(unittest.TestCase):
    def setUp(self):
        self.fixture = route_fixture.FullFlowW4CandidateRouteTest(
            "test_compiles_candidate_wide_routes_without_hidden_labels"
        )
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.root = self.fixture.root
        # The route fixture deliberately uses ``|`` to test injective operation
        # keys; the production N2 index contract intentionally rejects that
        # character.  Rewrite that one public ID consistently for this real
        # index-service integration fixture.
        old_id = "candidate-a|b"
        new_id = "candidate-c"
        self.fixture.candidates = [
            new_id if value == old_id else value
            for value in self.fixture.candidates
        ]
        self.fixture.digest_payload[new_id] = self.fixture.digest_payload.pop(old_id)
        task = self.fixture.loaded.public_task
        for row in task["candidate_objects"]:
            if row["object_id"] == old_id:
                row["object_id"] = new_id
        task["candidate_set_sha256"] = _sha(
            _canonical(task["candidate_objects"])
        )
        task_core = dict(task)
        del task_core["task_binding_sha256"]
        task["task_binding_sha256"] = _sha(_canonical(task_core))
        for manifest_path in (
            self.fixture.n3 / "raw-cold-data-plane.json",
            self.fixture.n4 / "n4-derived-data-package.json",
            self.fixture.ranges / "full-flow-exact-range-catalog.json",
        ):
            document = json.loads(manifest_path.read_text(encoding="utf-8"))
            rows = document.get("objects", document.get("entries", []))
            for row in rows:
                if row.get("object_id") == old_id:
                    row["object_id"] = new_id
            manifest_path.write_text(json.dumps(document), encoding="utf-8")
        shutil.rmtree(self.fixture.index)
        source = self.root / "visible-index-source.json"
        source.write_text(
            json.dumps({
                "schema_version": INDEX_SOURCE_SCHEMA_VERSION,
                "index_id": "w4-index-v1",
                "logical_node_id": "N2",
                "documents": [
                    {
                        "object_id": object_id,
                        "source_object_group": f"group-{index}",
                        "visible_fields": {
                            "digest": (
                                "requested visible event "
                                + ("music" if index == 0 else "road")
                            ),
                            "media_type": "video",
                        },
                    }
                    for index, object_id in enumerate(self.fixture.candidates)
                ],
                "credentials_recorded": False,
            }, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        build_n2_index_package(source, output_dir=self.fixture.index)
        index_verified = verify_n2_index_package(self.fixture.index)
        patches = self.fixture._patches()
        with (
            patches[0],
            patches[1],
            patches[2],
            mock.patch(
                "pathfinder.simulator.full_flow_w4_candidate_routes."
                "verify_n2_index_package",
                return_value=index_verified,
            ),
            patches[4],
        ):
            self.route = self.root / "routes"
            freeze_full_flow_w4_candidate_routes(
                self.fixture.runtime,
                self.fixture.n3,
                self.fixture.n4,
                self.fixture.index,
                self.fixture.ranges,
                physical_plan_id="w4-candidate-physical-v1",
                output_dir=self.route,
            )
        self.crosswalk = self.root / "crosswalk"
        freeze_full_flow_w4_index_artifact_crosswalk(
            self.route,
            self.fixture.index,
            output_dir=self.crosswalk,
        )
        self.payloads = {}
        for position, object_id in enumerate(self.fixture.candidates):
            raw_stem = old_id if object_id == new_id else object_id
            self.payloads[(object_id, "raw_video")] = (
                raw_stem.encode() + b"-"
            ) * (20 + position)
            self.payloads[(object_id, "multimodal_digest")] = (
                self.fixture.digest_payload[object_id]
            )
            self.payloads[(object_id, "sampled_frame_bundle")] = (
                b"bundle-" + raw_stem.encode()
            )

    def executor(
        self,
        *,
        corrupt_artifact=False,
        corrupt_transport=False,
        incomplete_ranking=False,
    ):
        indexes = {
            node: W4IndexDeployment(
                adapter=N2IndexService(self.fixture.index, node_id=node),
                package_dir=self.fixture.index,
            )
            for node in ("N2", "N7", "N8")
        }
        artifacts = {
            node: ArtifactAccess(self.payloads, corrupt=corrupt_artifact)
            for node in ("N3", "N4")
        }
        caches = {
            node: CacheAdapter(FullFlowArtifactCache(
                self.root / f"cache-{node}",
                node_id=node,
                cache_id=f"w4-{node.lower()}-cache",
                capacity_bytes=1024 * 1024,
            ))
            for node in ("N7", "N8")
        }
        ranker = SemanticRanker(incomplete=incomplete_ranking)
        transport = Transport(corrupt=corrupt_transport)
        executor = LiveW4CandidateOperationExecutor(
            route_package_dir=self.route,
            crosswalk_dir=self.crosswalk,
            canonical_index_package_dir=self.fixture.index,
            components=W4LiveComponents(
                admission=Admission(),
                indexes=indexes,
                artifacts=artifacts,
                caches=caches,
                transport=transport,
                semantic_ranker=ranker,
                ranking_return=RankingReturn(),
            ),
            evidence_class="strict-fake-component-conformance",
        )
        return executor, artifacts, transport, ranker

    def invoke_cli(self, arguments):
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            status = cli_main([*arguments, "--compact"])
        lines = stdout.getvalue().splitlines()
        self.assertEqual(1, len(lines), stdout.getvalue())
        return status, json.loads(lines[0])

    def test_public_package_exports_stable_w4_artifact_apis(self):
        self.assertIs(
            freeze_full_flow_w4_index_artifact_crosswalk,
            simulator_api.freeze_full_flow_w4_index_artifact_crosswalk,
        )
        self.assertIs(
            verify_full_flow_w4_index_artifact_crosswalk,
            simulator_api.verify_full_flow_w4_index_artifact_crosswalk,
        )
        self.assertIs(
            freeze_full_flow_w4_component_execution_receipt,
            simulator_api.freeze_full_flow_w4_component_execution_receipt,
        )
        self.assertIs(
            verify_full_flow_w4_component_execution_receipt,
            simulator_api.verify_full_flow_w4_component_execution_receipt,
        )

    def test_crosswalk_is_source_verified_and_label_safe(self):
        result = verify_full_flow_w4_index_artifact_crosswalk(
            self.crosswalk,
            route_package_dir=self.route,
            index_package_dir=self.fixture.index,
        )
        self.assertEqual("VERIFIED", result["status"])
        self.assertEqual(2, result["object_count"])
        text = (self.crosswalk / CROSSWALK_NAME).read_text(encoding="utf-8")
        for forbidden in (
            '"relevance_labels"',
            '"hidden_label"',
            '"source_object_group"',
        ):
            self.assertNotIn(forbidden, text)

    def test_executes_all_sixteen_routes_through_strict_components(self):
        executor, artifacts, transport, ranker = self.executor()
        run = self.root / "live-component-run"
        result = run_full_flow_w4_candidate_coordinator(
            self.route,
            run_id="w4-live-component-test-v1",
            executor=executor,
            output_dir=run,
        )
        self.assertEqual("COMPLETE", result["status"])
        self.assertEqual(16, result["trial_count"])
        trials = [
            json.loads(line)
            for line in (run / "w4-candidate-coordinator-trials.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        self.assertEqual(
            {(f"D{design}", repetition)
             for design in range(8) for repetition in (0, 1)},
            {(row["design_id"], row["repetition"]) for row in trials},
        )
        run_report = json.loads(
            (run / "w4-candidate-coordinator-run.json").read_text()
        )
        self.assertTrue(run_report["llm_called"])
        self.assertFalse(run_report["flowmesh_workflow_submitted"])
        self.assertTrue(artifacts["N3"].calls)
        self.assertTrue(artifacts["N4"].calls)
        self.assertTrue(transport.calls)
        self.assertTrue(ranker.calls)
        actions = {row["action"] for row in executor.events}
        self.assertTrue({
            "admit-public-retrieval",
            "query-candidate-index-shard",
            "access-raw",
            "access-exact-raw-range",
            "access-derived-artifact",
            "lookup",
            "read",
            "insert",
            "rank-complete-candidate-set",
            "return-public-ranking-to-n1",
        } <= actions)

        receipt = self.root / "component-receipt"
        frozen = freeze_full_flow_w4_component_execution_receipt(
            run,
            route_package_dir=self.route,
            crosswalk_dir=self.crosswalk,
            index_package_dir=self.fixture.index,
            executor=executor,
            output_dir=receipt,
        )
        self.assertEqual("FROZEN", frozen["status"])
        self.assertEqual(
            "strict-fake-component-conformance", frozen["evidence_class"]
        )
        document = json.loads((receipt / RECEIPT_NAME).read_text())
        self.assertTrue(document["strict_fake_components_declared"])
        self.assertFalse(document["declared_live_local_component_execution"])
        self.assertFalse(document["real_cloud_performance_measured"])

    def test_artifact_and_transport_byte_corruption_fail_closed(self):
        cases = [
            (self.executor(corrupt_artifact=True)[0], "artifact access bytes"),
            (self.executor(corrupt_transport=True)[0], "transport did not preserve"),
        ]
        for index, (executor, message) in enumerate(cases):
            with self.subTest(message=message):
                output = self.root / f"corrupt-{index}"
                with self.assertRaisesRegex(FullFlowW4LiveExecutorError, message):
                    run_full_flow_w4_candidate_coordinator(
                        self.route,
                        run_id=f"w4-corrupt-{index}",
                        executor=executor,
                        output_dir=output,
                    )
                self.assertFalse(output.exists())

    def test_incomplete_semantic_ranking_fails_closed(self):
        executor = self.executor(incomplete_ranking=True)[0]
        with self.assertRaisesRegex(
            FullFlowW4LiveExecutorError,
            "incomplete or unbound ranking",
        ):
            run_full_flow_w4_candidate_coordinator(
                self.route,
                run_id="w4-incomplete-ranking",
                executor=executor,
                output_dir=self.root / "incomplete-ranking",
            )

    def test_persisted_component_events_freeze_and_strict_jsonl(self):
        executor = self.executor()[0]
        run = self.root / "persisted-events-run"
        run_full_flow_w4_candidate_coordinator(
            self.route,
            run_id="w4-persisted-events-test",
            executor=executor,
            output_dir=run,
        )
        events_path = self.root / "persisted-component-events.jsonl"
        events_path.write_bytes(b"".join(
            _canonical(row) + b"\n" for row in executor.events
        ))
        receipt = self.root / "persisted-events-receipt"
        result = freeze_full_flow_w4_component_execution_receipt(
            run,
            route_package_dir=self.route,
            crosswalk_dir=self.crosswalk,
            index_package_dir=self.fixture.index,
            component_events_path=events_path,
            evidence_class="strict-fake-component-conformance",
            output_dir=receipt,
        )
        self.assertEqual("FROZEN", result["status"])
        self.assertEqual(
            "strict-fake-component-conformance", result["evidence_class"]
        )

        invalid_values = {
            "duplicate": '{"schema_version":"a","schema_version":"b"}\n',
            "non-finite": '{"service_time_ms":NaN}\n',
        }
        for name, content in invalid_values.items():
            with self.subTest(name=name):
                invalid = self.root / f"invalid-{name}.jsonl"
                invalid.write_text(content, encoding="utf-8")
                with self.assertRaises(FullFlowW4LiveExecutorError) as raised:
                    freeze_full_flow_w4_component_execution_receipt(
                        run,
                        route_package_dir=self.route,
                        crosswalk_dir=self.crosswalk,
                        index_package_dir=self.fixture.index,
                        component_events_path=invalid,
                        evidence_class=(
                            "strict-fake-component-conformance"
                        ),
                        output_dir=self.root / f"invalid-{name}-receipt",
                    )
                self.assertIn(
                    "repeats key" if name == "duplicate" else "non-finite",
                    str(raised.exception),
                )

    def test_artifact_commands_are_wired_without_runtime_secrets(self):
        with mock.patch(
            "pathfinder.simulator.full_flow_w4_live_executor."
            "freeze_full_flow_w4_index_artifact_crosswalk",
            return_value={"status": "FROZEN"},
        ) as freeze_crosswalk:
            status, result = self.invoke_cli([
                "freeze-simulator-full-flow-w4-index-artifact-crosswalk",
                "--route-package-dir",
                "routes",
                "--index-package-dir",
                "index",
                "--output-dir",
                "crosswalk",
            ])
        self.assertEqual(0, status)
        self.assertEqual("FROZEN", result["status"])
        freeze_crosswalk.assert_called_once_with(
            Path("routes"),
            Path("index"),
            output_dir=Path("crosswalk"),
        )

        with mock.patch(
            "pathfinder.simulator.full_flow_w4_live_executor."
            "verify_full_flow_w4_index_artifact_crosswalk",
            return_value={"status": "VERIFIED"},
        ) as verify_crosswalk:
            status, _ = self.invoke_cli([
                "verify-simulator-full-flow-w4-index-artifact-crosswalk",
                "--output-dir",
                "crosswalk",
                "--route-package-dir",
                "routes",
                "--index-package-dir",
                "index",
            ])
        self.assertEqual(0, status)
        verify_crosswalk.assert_called_once_with(
            Path("crosswalk"),
            route_package_dir=Path("routes"),
            index_package_dir=Path("index"),
        )

        with mock.patch(
            "pathfinder.simulator.full_flow_w4_live_executor."
            "freeze_full_flow_w4_component_execution_receipt",
            return_value={"status": "FROZEN"},
        ) as freeze_receipt:
            status, _ = self.invoke_cli([
                "freeze-simulator-full-flow-w4-component-execution-receipt",
                "--coordinator-run-dir",
                "run",
                "--route-package-dir",
                "routes",
                "--crosswalk-dir",
                "crosswalk",
                "--index-package-dir",
                "index",
                "--component-events",
                "events.jsonl",
                "--evidence-class",
                "live-local-component-execution",
                "--output-dir",
                "receipt",
            ])
        self.assertEqual(0, status)
        freeze_receipt.assert_called_once_with(
            Path("run"),
            route_package_dir=Path("routes"),
            crosswalk_dir=Path("crosswalk"),
            index_package_dir=Path("index"),
            component_events_path=Path("events.jsonl"),
            evidence_class="live-local-component-execution",
            output_dir=Path("receipt"),
        )

        with mock.patch(
            "pathfinder.simulator.full_flow_w4_live_executor."
            "verify_full_flow_w4_component_execution_receipt",
            return_value={"status": "VERIFIED"},
        ) as verify_receipt:
            status, _ = self.invoke_cli([
                "verify-simulator-full-flow-w4-component-execution-receipt",
                "--output-dir",
                "receipt",
                "--coordinator-run-dir",
                "run",
                "--route-package-dir",
                "routes",
                "--crosswalk-dir",
                "crosswalk",
                "--index-package-dir",
                "index",
            ])
        self.assertEqual(0, status)
        verify_receipt.assert_called_once_with(
            Path("receipt"),
            coordinator_run_dir=Path("run"),
            route_package_dir=Path("routes"),
            crosswalk_dir=Path("crosswalk"),
            index_package_dir=Path("index"),
        )

    def test_crosswalk_and_receipt_tampering_are_rejected(self):
        crosswalk = json.loads((self.crosswalk / CROSSWALK_NAME).read_text())
        crosswalk["objects"][0]["visible_fields_sha256"] = "f" * 64
        del crosswalk["crosswalk_sha256"]
        crosswalk["crosswalk_sha256"] = _sha(_canonical(crosswalk))
        payload = (
            json.dumps(crosswalk, sort_keys=True, indent=2) + "\n"
        ).encode()
        (self.crosswalk / CROSSWALK_NAME).write_bytes(payload)
        (self.crosswalk / "SHA256SUMS").write_bytes(
            f"{_sha(payload)}  {CROSSWALK_NAME}\n".encode()
        )
        with self.assertRaisesRegex(FullFlowW4LiveExecutorError, "frozen sources"):
            verify_full_flow_w4_index_artifact_crosswalk(
                self.crosswalk,
                route_package_dir=self.route,
                index_package_dir=self.fixture.index,
            )

    def test_component_receipt_tampering_is_rejected(self):
        executor = self.executor()[0]
        run = self.root / "receipt-source-run"
        run_full_flow_w4_candidate_coordinator(
            self.route,
            run_id="w4-receipt-tamper-test",
            executor=executor,
            output_dir=run,
        )
        receipt = self.root / "receipt-to-tamper"
        freeze_full_flow_w4_component_execution_receipt(
            run,
            route_package_dir=self.route,
            crosswalk_dir=self.crosswalk,
            index_package_dir=self.fixture.index,
            executor=executor,
            output_dir=receipt,
        )
        event_path = receipt / "w4-component-execution-events.jsonl"
        rows = [json.loads(line) for line in event_path.read_text().splitlines()]
        rows[0]["service_time_ms"] += 1.0
        events_payload = "".join(
            json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n"
            for row in rows
        ).encode()
        event_path.write_bytes(events_payload)
        report_path = receipt / RECEIPT_NAME
        report = json.loads(report_path.read_text())
        report["events_sha256"] = _sha(events_payload)
        del report["receipt_sha256"]
        report["receipt_sha256"] = _sha(_canonical(report))
        report_payload = (
            json.dumps(report, sort_keys=True, indent=2) + "\n"
        ).encode()
        report_path.write_bytes(report_payload)
        checksum_payload = (
            f"{_sha(events_payload)}  w4-component-execution-events.jsonl\n"
            f"{_sha(report_payload)}  {RECEIPT_NAME}\n"
        ).encode()
        (receipt / "SHA256SUMS").write_bytes(checksum_payload)
        with self.assertRaisesRegex(
            FullFlowW4LiveExecutorError,
            "component event differs",
        ):
            verify_full_flow_w4_component_execution_receipt(
                receipt,
                coordinator_run_dir=run,
                route_package_dir=self.route,
                crosswalk_dir=self.crosswalk,
                index_package_dir=self.fixture.index,
            )


if __name__ == "__main__":
    unittest.main()
