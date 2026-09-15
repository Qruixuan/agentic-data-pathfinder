from __future__ import annotations

import hashlib
import io
import json
import os
import shutil
import tempfile
import unittest
from contextlib import ExitStack, contextmanager, redirect_stdout
from pathlib import Path
from typing import Any
from unittest.mock import patch

from pathfinder.cli import _parser, main as cli_main
import pathfinder.simulator as simulator
from pathfinder.simulator import full_flow_bulk_live_provisioning as bulk
from pathfinder.simulator.n4_derived_data_plane import (
    FRAME_BUNDLE_REPRESENTATION_ID,
    MULTIMODAL_DIGEST_REPRESENTATION_ID,
)


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    ).encode("utf-8")


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _artifact(object_id: str, representation_id: str) -> tuple[int, str]:
    payload = f"artifact|{object_id}|{representation_id}".encode()
    return len(payload), _sha256(payload)


class _Fixture:
    def __init__(self, root: Path, object_count: int) -> None:
        self.root = root
        self.catalog = root / "catalog"
        self.bindings = root / "bindings"
        self.n4 = root / "historical-n4"
        self.sources = root / "sources"
        for directory in (
            self.catalog,
            self.bindings,
            self.n4,
            self.sources,
        ):
            directory.mkdir(parents=True)
        self.object_ids = [
            f"nextqa-val-{index:010d}" for index in range(1, object_count + 1)
        ]
        self.catalog_document = self._write_catalog()
        self.source_manifest = self.root / "operator-sources.json"
        self.source_document = self._write_sources()

    def _write_catalog(self) -> dict[str, Any]:
        entries: list[dict[str, Any]] = []
        for object_id in self.object_ids:
            for representation_id in (
                FRAME_BUNDLE_REPRESENTATION_ID,
                MULTIMODAL_DIGEST_REPRESENTATION_ID,
            ):
                size, digest = _artifact(object_id, representation_id)
                identity = {
                    "object_id": object_id,
                    "representation_id": representation_id,
                    "artifact_sha256": digest,
                    "artifact_size_bytes": size,
                    "object_catalog_version": "frozen-catalog-v1",
                }
                entry: dict[str, Any] = {
                    "schema_version": bulk.PROVISIONING_ENTRY_SCHEMA_VERSION,
                    "chain_id": f"artifact|{object_id}|{representation_id}",
                    "logical_object_id": object_id,
                    "artifact_identity": identity,
                    "artifact_identity_sha256": _sha256(_canonical(identity)),
                    "n5_evidence_sha256": _sha256(
                        f"n5|{object_id}|{representation_id}".encode()
                    ),
                    "n4_publication_sha256": _sha256(
                        f"n4|{object_id}|{representation_id}".encode()
                    ),
                    "evidence_class": (
                        "verified-preprovisioned-snapshot-provenance"
                    ),
                    "available": True,
                    "live_materialization_executed": False,
                    "live_materialization_cost_measured": False,
                }
                entry["entry_sha256"] = _sha256(_canonical(entry))
                entries.append(entry)
        document: dict[str, Any] = {
            "schema_version": bulk.PROVISIONING_CATALOG_SCHEMA_VERSION,
            "status": "FROZEN_PREPROVISIONED_DERIVED_ARTIFACTS",
            "catalog_id": "synthetic-provisioning-v1",
            "artifact_binding_set_sha256": "a" * 64,
            "n4_package_manifest_sha256": "b" * 64,
            "n4_package_sha256": "c" * 64,
            "entry_count": len(entries),
            "entries": entries,
            "all_artifacts_available": True,
            "live_materialization_executed": False,
            "live_materialization_cost_measured": False,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }
        document["catalog_sha256"] = _sha256(_canonical(document))
        (self.catalog / bulk.PROVISIONING_CATALOG_NAME).write_bytes(
            _json_bytes(document)
        )
        return document

    def _write_sources(self) -> dict[str, Any]:
        entries: list[dict[str, Any]] = []
        for object_id in self.object_ids:
            object_root = self.sources / object_id
            object_root.mkdir()
            source = object_root / f"{object_id}.mp4"
            source_payload = b"\x00\x00\x00\x18ftypmp42" + object_id.encode()
            source.write_bytes(source_payload)
            source_sha = _sha256(source_payload)
            frame_size, frame_sha = _artifact(
                object_id, FRAME_BUNDLE_REPRESENTATION_ID
            )
            frame_plan_sha = _sha256(f"frame-plan|{object_id}".encode())
            frame_plan = {
                "plan_sha256": frame_plan_sha,
                "input": {
                    "object_id": object_id,
                    "size_bytes": len(source_payload),
                    "sha256": source_sha,
                },
                "expected_output": {
                    "artifact_size_bytes": frame_size,
                    "artifact_sha256": frame_sha,
                },
            }
            frame_path = object_root / "frame-plan.json"
            frame_path.write_bytes(_json_bytes(frame_plan))
            digest_plan_sha = _sha256(f"digest-plan|{object_id}".encode())
            digest_dir = object_root / "digest-plan"
            digest_dir.mkdir()
            digest_plan = {
                "plan_sha256": digest_plan_sha,
                "object_id": object_id,
                "source": {"sha256": source_sha},
            }
            digest_path = digest_dir / bulk.N5_DIGEST_PLAN_NAME
            digest_path.write_bytes(_json_bytes(digest_plan))
            digest_checksums = digest_dir / bulk.N5_DIGEST_CHECKSUMS_NAME
            digest_checksums.write_text(
                f"{_sha256(digest_path.read_bytes())}  "
                f"{bulk.N5_DIGEST_PLAN_NAME}\n",
                encoding="utf-8",
            )
            entries.append({
                "object_id": object_id,
                "source_video_path": str(source),
                "source_video_size_bytes": len(source_payload),
                "source_video_sha256": source_sha,
                "frame_plan_path": str(frame_path),
                "frame_plan_file_sha256": _sha256(frame_path.read_bytes()),
                "frame_plan_sha256": frame_plan_sha,
                "digest_plan_dir": str(digest_dir),
                "digest_plan_file_sha256": _sha256(digest_path.read_bytes()),
                "digest_plan_checksums_file_sha256": _sha256(
                    digest_checksums.read_bytes()
                ),
                "digest_plan_sha256": digest_plan_sha,
            })
        document = {
            "schema_version": bulk.SOURCE_MANIFEST_SCHEMA_VERSION,
            "provisioning_catalog_sha256": self.catalog_document[
                "catalog_sha256"
            ],
            "object_count": len(entries),
            "entries": entries,
            "credentials_recorded": False,
        }
        self.source_manifest.write_bytes(_json_bytes(document))
        return document

    def arguments(self, output: Path) -> dict[str, Any]:
        return {
            "provisioning_catalog_dir": self.catalog,
            "artifact_binding_dir": self.bindings,
            "n4_package_dir": self.n4,
            "operator_source_manifest": self.source_manifest,
            "run_id": "bulk-live-synthetic-v1",
            "output_dir": output,
        }


class _FakeExecutor:
    def __init__(
        self,
        representation_id: str,
        calls: list[tuple[int, str, str]],
        *,
        fail_index: int | None = None,
        failure: type[bulk.LiveProvisioningOperationFailure] = (
            bulk.InfrastructureProvisioningFailure
        ),
        write_before_failure: bool = False,
        n5_replay_indices: set[int] | None = None,
    ) -> None:
        self.representation_id = representation_id
        self.calls = calls
        self.fail_index = fail_index
        self.failure = failure
        self.write_before_failure = write_before_failure
        self.n5_replay_indices = n5_replay_indices or set()
        self.failed = False
        self.runtime_token = "runtime-only-token"
        self.runtime_endpoint = "http://127.0.0.1:19085"

    def _write(self, operation: bulk.BulkProvisioningOperation) -> None:
        operation.output_dir.mkdir(parents=True)
        package_sha = _sha256(
            f"package|{operation.operation_index}".encode()
        )
        n4_receipt = {
            "publication_id": operation.publication_id,
            "previous_catalog_version": (
                operation.expected_current_catalog_version
            ),
            "committed_catalog_version": operation.catalog_version,
            "generation_id": f"generation-{package_sha}",
            "package_sha256": package_sha,
            "published_artifacts": [{
                "object_id": operation.object_id,
                "representation_id": operation.representation_id,
                "artifact_size_bytes": operation.expected_artifact_size_bytes,
                "artifact_sha256": operation.expected_artifact_sha256,
            }],
            "atomic_visibility": True,
        }
        document: dict[str, Any] = {
            "object_id": operation.object_id,
            "representation_id": operation.representation_id,
            "artifact_size_bytes": operation.expected_artifact_size_bytes,
            "artifact_sha256": operation.expected_artifact_sha256,
            "n4_publication_id": operation.publication_id,
            "n4_previous_catalog_version": (
                operation.expected_current_catalog_version
            ),
            "n4_committed_catalog_version": operation.catalog_version,
            "n4_generation_id": n4_receipt["generation_id"],
            "n4_package_sha256": package_sha,
            "n4_publication_receipt": n4_receipt,
            "n4_publication_idempotent_replay": False,
        }
        if operation.representation_id == FRAME_BUNDLE_REPRESENTATION_ID:
            name = bulk.RECEIPT_NAME
            document["n5_plan_sha256"] = operation.plan_sha256
            document["n5_materialization_idempotent_replay"] = (
                operation.operation_index in self.n5_replay_indices
            )
        else:
            name = bulk.DIGEST_RECEIPT_NAME
            document["n5_digest_plan_sha256"] = operation.plan_sha256
            document["n5_digest_materialization_idempotent_replay"] = (
                operation.operation_index in self.n5_replay_indices
            )
        payload = _json_bytes(document)
        (operation.output_dir / name).write_bytes(payload)
        (operation.output_dir / bulk.ONE_OBJECT_CHECKSUMS_NAME).write_text(
            f"{_sha256(payload)}  {name}\n",
            encoding="utf-8",
        )

    def execute(
        self, operation: bulk.BulkProvisioningOperation
    ) -> dict[str, Any]:
        if operation.representation_id != self.representation_id:
            raise AssertionError("wrong fake executor")
        self.calls.append((
            operation.operation_index,
            operation.object_id,
            operation.representation_id,
        ))
        if operation.operation_index == self.fail_index and not self.failed:
            self.failed = True
            if self.write_before_failure:
                self._write(operation)
            raise self.failure("synthetic-safe-failure")
        self._write(operation)
        return {"status": "VERIFIED"}


class FullFlowBulkLiveProvisioningTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @contextmanager
    def patched_authorities(self, fixture: _Fixture):
        with ExitStack() as stack:
            catalog = stack.enter_context(patch.object(
                bulk,
                "verify_full_flow_provisioning_catalog",
                return_value={
                    "status": "VERIFIED",
                    "catalog_sha256": fixture.catalog_document[
                        "catalog_sha256"
                    ],
                },
            ))
            frame_plan = stack.enter_context(patch.object(
                bulk,
                "verify_n5_materialization_plan",
                side_effect=lambda value: dict(value),
            ))

            def verify_digest(plan_dir: Path, source: Path) -> dict[str, Any]:
                value = json.loads(
                    (Path(plan_dir) / bulk.N5_DIGEST_PLAN_NAME).read_text()
                )
                return {
                    "status": "VERIFIED",
                    "plan_id": "digest-plan",
                    "plan_sha256": value["plan_sha256"],
                    "object_id": value["object_id"],
                    "model_id": "offline-test-model",
                    "frame_count": 2,
                    "source_video_sha256": value["source"]["sha256"],
                    "credentials_recorded": False,
                    "eligible_for_scientific_claims": False,
                }

            digest_plan = stack.enter_context(patch.object(
                bulk,
                "verify_n5_multimodal_digest_plan",
                side_effect=verify_digest,
            ))
            frame_receipt = stack.enter_context(patch.object(
                bulk,
                "verify_n5_n4_live_frame_bundle_provisioning_smoke",
                return_value={"status": "VERIFIED"},
            ))
            digest_receipt = stack.enter_context(patch.object(
                bulk,
                "verify_n5_n4_live_multimodal_digest_provisioning_smoke",
                return_value={"status": "VERIFIED"},
            ))
            n4_receipt = stack.enter_context(patch.object(
                bulk,
                "verify_n4_publication_receipt",
                side_effect=lambda value: dict(value),
            ))
            yield {
                "catalog": catalog,
                "frame_plan": frame_plan,
                "digest_plan": digest_plan,
                "frame_receipt": frame_receipt,
                "digest_receipt": digest_receipt,
                "n4_receipt": n4_receipt,
            }

    @staticmethod
    def executors(
        calls: list[tuple[int, str, str]],
        **kwargs: Any,
    ) -> tuple[_FakeExecutor, _FakeExecutor]:
        return (
            _FakeExecutor(
                FRAME_BUNDLE_REPRESENTATION_ID, calls, **kwargs
            ),
            _FakeExecutor(
                MULTIMODAL_DIGEST_REPRESENTATION_ID, calls, **kwargs
            ),
        )

    def test_full_synthetic_36_by_2_run_is_exact_and_gate_consumable(self) -> None:
        fixture = _Fixture(self.root / "full", 36)
        output = fixture.root / "run"
        calls: list[tuple[int, str, str]] = []
        frame, digest = self.executors(calls)
        with self.patched_authorities(fixture) as mocks:
            report = bulk.run_full_flow_bulk_live_provisioning(
                **fixture.arguments(output),
                frame_executor=frame,
                digest_executor=digest,
            )

        self.assertEqual("VERIFIED", report["status"])
        self.assertEqual(36, report["object_count"])
        self.assertEqual(72, report["completed_operation_count"])
        self.assertEqual(36, report["frame_bundle_operation_count"])
        self.assertEqual(36, report["multimodal_digest_operation_count"])
        self.assertEqual(list(range(1, 73)), [row[0] for row in calls])
        self.assertEqual(
            [
                representation
                for _object in fixture.object_ids
                for representation in (
                    FRAME_BUNDLE_REPRESENTATION_ID,
                    MULTIMODAL_DIGEST_REPRESENTATION_ID,
                )
            ],
            [row[2] for row in calls],
        )
        descriptor = json.loads(
            (
                output
                / bulk.FINAL_DIRECTORY_NAME
                / bulk.LIVE_RECEIPT_BINDINGS_NAME
            ).read_text()
        )
        self.assertEqual(72, len(descriptor))
        self.assertEqual(
            {"kind", "receipt_dir", "n5_plan"}, set(descriptor[0])
        )
        self.assertEqual(
            {
                "kind",
                "receipt_dir",
                "n5_digest_plan_dir",
                "source_video_path",
            },
            set(descriptor[1]),
        )
        self.assertGreaterEqual(mocks["frame_receipt"].call_count, 36)
        self.assertGreaterEqual(mocks["digest_receipt"].call_count, 36)
        combined = b"\n".join(
            path.read_bytes()
            for path in output.rglob("*")
            if path.is_file()
        )
        self.assertNotIn(b"runtime-only-token", combined)
        self.assertNotIn(b"http://", combined)
        self.assertNotIn(b"https://", combined)
        self.assertNotIn(b"reasoning_content", combined)

    def test_offline_helper_freezes_36_source_rows_without_handwritten_hashes(
        self,
    ) -> None:
        fixture = _Fixture(self.root / "source-helper", 36)
        mapping_path = fixture.root / "object-mapping.json"
        mapping_path.write_bytes(_json_bytes({
            "schema_version": bulk.SOURCE_MAPPING_SCHEMA_VERSION,
            "entries": [
                {
                    "object_id": row["object_id"],
                    "source_video_path": row["source_video_path"],
                    "frame_plan_path": row["frame_plan_path"],
                    "digest_plan_dir": row["digest_plan_dir"],
                }
                for row in fixture.source_document["entries"]
            ],
            "credentials_recorded": False,
        }))
        generated = fixture.root / "generated-source-manifest.json"
        with self.patched_authorities(fixture):
            report = (
                bulk.freeze_full_flow_bulk_live_provisioning_source_manifest(
                    fixture.catalog,
                    fixture.bindings,
                    fixture.n4,
                    mapping_path,
                    output_path=generated,
                )
            )
            with self.assertRaisesRegex(
                bulk.FullFlowBulkLiveProvisioningError,
                "already exists",
            ):
                bulk.freeze_full_flow_bulk_live_provisioning_source_manifest(
                    fixture.catalog,
                    fixture.bindings,
                    fixture.n4,
                    mapping_path,
                    output_path=generated,
                )
        self.assertEqual("FROZEN", report["status"])
        self.assertEqual(36, report["object_count"])
        self.assertEqual(72, report["required_derived_identity_count"])
        self.assertFalse(report["source_bytes_copied"])
        self.assertEqual(
            fixture.source_manifest.read_bytes(), generated.read_bytes()
        )

    def test_infrastructure_failure_resumes_from_contiguous_checkpoint(self) -> None:
        fixture = _Fixture(self.root / "resume", 3)
        output = fixture.root / "run"
        first_calls: list[tuple[int, str, str]] = []
        frame = _FakeExecutor(
            FRAME_BUNDLE_REPRESENTATION_ID,
            first_calls,
            fail_index=3,
        )
        digest = _FakeExecutor(
            MULTIMODAL_DIGEST_REPRESENTATION_ID,
            first_calls,
        )
        with self.patched_authorities(fixture):
            with self.assertRaisesRegex(
                bulk.FullFlowBulkLiveProvisioningError,
                "infrastructure failure",
            ):
                bulk.run_full_flow_bulk_live_provisioning(
                    **fixture.arguments(output),
                    frame_executor=frame,
                    digest_executor=digest,
                )
            resumed_calls: list[tuple[int, str, str]] = []
            resumed_frame, resumed_digest = self.executors(resumed_calls)
            report = bulk.run_full_flow_bulk_live_provisioning(
                **fixture.arguments(output),
                frame_executor=resumed_frame,
                digest_executor=resumed_digest,
                resume=True,
            )
        self.assertEqual([1, 2, 3], [row[0] for row in first_calls])
        self.assertEqual([3, 4, 5, 6], [row[0] for row in resumed_calls])
        self.assertEqual(1, report["infrastructure_failure_count"])
        self.assertEqual(6, report["completed_operation_count"])

    def test_receipt_before_failure_is_adopted_without_reexecution(self) -> None:
        fixture = _Fixture(self.root / "adopt", 2)
        output = fixture.root / "run"
        calls: list[tuple[int, str, str]] = []
        frame = _FakeExecutor(
            FRAME_BUNDLE_REPRESENTATION_ID,
            calls,
            fail_index=3,
            write_before_failure=True,
        )
        digest = _FakeExecutor(MULTIMODAL_DIGEST_REPRESENTATION_ID, calls)
        with self.patched_authorities(fixture):
            with self.assertRaises(bulk.FullFlowBulkLiveProvisioningError):
                bulk.run_full_flow_bulk_live_provisioning(
                    **fixture.arguments(output),
                    frame_executor=frame,
                    digest_executor=digest,
                )
            resumed_calls: list[tuple[int, str, str]] = []
            resumed_frame, resumed_digest = self.executors(resumed_calls)
            report = bulk.run_full_flow_bulk_live_provisioning(
                **fixture.arguments(output),
                frame_executor=resumed_frame,
                digest_executor=resumed_digest,
                resume=True,
            )
        self.assertEqual([4], [row[0] for row in resumed_calls])
        self.assertEqual(1, report["durable_replay_adopted_operation_count"])
        checkpoint = json.loads(
            (output / bulk.CHECKPOINTS_DIRECTORY_NAME / "0003.json").read_text()
        )
        self.assertTrue(checkpoint["crash_window_receipt_adopted"])

    def test_one_object_idempotent_replay_is_accounted(self) -> None:
        fixture = _Fixture(self.root / "replay", 1)
        output = fixture.root / "run"
        calls: list[tuple[int, str, str]] = []
        frame = _FakeExecutor(
            FRAME_BUNDLE_REPRESENTATION_ID,
            calls,
            n5_replay_indices={1},
        )
        digest = _FakeExecutor(MULTIMODAL_DIGEST_REPRESENTATION_ID, calls)
        with self.patched_authorities(fixture):
            report = bulk.run_full_flow_bulk_live_provisioning(
                **fixture.arguments(output),
                frame_executor=frame,
                digest_executor=digest,
            )
        self.assertEqual(1, report["durable_replay_adopted_operation_count"])

    def test_semantic_and_data_failures_are_terminal_and_distinguished(self) -> None:
        for failure, expected in (
            (bulk.SemanticProvisioningFailure, "semantic failure"),
            (bulk.DataProvisioningFailure, "data failure"),
        ):
            with self.subTest(failure=failure.__name__):
                case = self.root / failure.__name__
                fixture = _Fixture(case, 1)
                output = fixture.root / "run"
                calls: list[tuple[int, str, str]] = []
                frame = _FakeExecutor(
                    FRAME_BUNDLE_REPRESENTATION_ID,
                    calls,
                    fail_index=1,
                    failure=failure,
                )
                digest = _FakeExecutor(
                    MULTIMODAL_DIGEST_REPRESENTATION_ID, calls
                )
                with self.patched_authorities(fixture):
                    with self.assertRaisesRegex(
                        bulk.FullFlowBulkLiveProvisioningError, expected
                    ):
                        bulk.run_full_flow_bulk_live_provisioning(
                            **fixture.arguments(output),
                            frame_executor=frame,
                            digest_executor=digest,
                        )
                    resumed_frame, resumed_digest = self.executors([])
                    with self.assertRaisesRegex(
                        bulk.FullFlowBulkLiveProvisioningError,
                        "only infrastructure failures may be resumed",
                    ):
                        bulk.run_full_flow_bulk_live_provisioning(
                            **fixture.arguments(output),
                            frame_executor=resumed_frame,
                            digest_executor=resumed_digest,
                            resume=True,
                        )

    def test_missing_duplicate_and_duplicate_json_sources_fail_closed(self) -> None:
        for mutation, expected in (
            ("missing", "object count differs"),
            ("duplicate", "object repeats"),
            ("duplicate-json", "repeats key"),
        ):
            with self.subTest(mutation=mutation):
                fixture = _Fixture(self.root / mutation, 2)
                if mutation == "duplicate-json":
                    text = fixture.source_manifest.read_text()
                    text = text.replace(
                        '  "credentials_recorded": false,',
                        '  "credentials_recorded": false,\n'
                        '  "credentials_recorded": false,',
                        1,
                    )
                    fixture.source_manifest.write_text(text)
                else:
                    value = json.loads(fixture.source_manifest.read_text())
                    if mutation == "missing":
                        value["entries"] = value["entries"][:-1]
                    else:
                        value["entries"][-1] = dict(value["entries"][0])
                    fixture.source_manifest.write_bytes(_json_bytes(value))
                calls: list[tuple[int, str, str]] = []
                frame, digest = self.executors(calls)
                with self.patched_authorities(fixture):
                    with self.assertRaisesRegex(
                        bulk.FullFlowBulkLiveProvisioningError, expected
                    ):
                        bulk.run_full_flow_bulk_live_provisioning(
                            **fixture.arguments(fixture.root / "run"),
                            frame_executor=frame,
                            digest_executor=digest,
                        )
                self.assertEqual([], calls)

    def test_checkpoint_and_final_descriptor_tamper_are_rejected(self) -> None:
        fixture = _Fixture(self.root / "tamper", 1)
        output = fixture.root / "run"
        calls: list[tuple[int, str, str]] = []
        frame, digest = self.executors(calls)
        with self.patched_authorities(fixture):
            bulk.run_full_flow_bulk_live_provisioning(
                **fixture.arguments(output),
                frame_executor=frame,
                digest_executor=digest,
            )
            checkpoint_path = (
                output / bulk.CHECKPOINTS_DIRECTORY_NAME / "0001.json"
            )
            original = checkpoint_path.read_bytes()
            checkpoint = json.loads(original)
            checkpoint["expected_artifact_sha256"] = "f" * 64
            checkpoint_path.write_bytes(_json_bytes(checkpoint))
            with self.assertRaisesRegex(
                bulk.FullFlowBulkLiveProvisioningError,
                "checkpoint digest failed",
            ):
                bulk.verify_full_flow_bulk_live_provisioning(
                    output,
                    provisioning_catalog_dir=fixture.catalog,
                    artifact_binding_dir=fixture.bindings,
                    n4_package_dir=fixture.n4,
                    operator_source_manifest=fixture.source_manifest,
                )
            checkpoint_path.write_bytes(original)
            descriptor_path = (
                output
                / bulk.FINAL_DIRECTORY_NAME
                / bulk.LIVE_RECEIPT_BINDINGS_NAME
            )
            descriptor = json.loads(descriptor_path.read_text())
            descriptor.reverse()
            descriptor_path.write_bytes(_json_bytes(descriptor))
            with self.assertRaisesRegex(
                bulk.FullFlowBulkLiveProvisioningError,
                "bindings differ",
            ):
                bulk.verify_full_flow_bulk_live_provisioning(
                    output,
                    provisioning_catalog_dir=fixture.catalog,
                    artifact_binding_dir=fixture.bindings,
                    n4_package_dir=fixture.n4,
                    operator_source_manifest=fixture.source_manifest,
                )


class BulkLiveProvisioningCliTest(unittest.TestCase):
    commands = {
        "freeze-simulator-full-flow-bulk-provisioning-source-manifest",
        "run-simulator-full-flow-bulk-live-provisioning",
        "verify-simulator-full-flow-bulk-live-provisioning",
    }

    def _invoke(self, arguments: list[str]) -> tuple[int, dict[str, Any], str]:
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            status = cli_main([*arguments, "--compact"])
        text = stdout.getvalue()
        lines = text.splitlines()
        self.assertEqual(1, len(lines), text)
        return status, json.loads(lines[0]), text

    def test_public_exports_and_parser_defaults(self) -> None:
        parser = _parser()
        command_action = next(
            action for action in parser._actions if action.dest == "command"
        )
        self.assertTrue(self.commands.issubset(command_action.choices))
        parsed = parser.parse_args([
            "run-simulator-full-flow-bulk-live-provisioning",
            "--provisioning-catalog-dir",
            "catalog",
            "--artifact-binding-dir",
            "bindings",
            "--n4-package-dir",
            "n4",
            "--operator-source-manifest",
            "sources.json",
            "--n5-base-url",
            "http://127.0.0.1:19085",
            "--n5-digest-base-url",
            "http://127.0.0.1:19185",
            "--n4-base-url",
            "http://127.0.0.1:19184",
            "--run-id",
            "bulk-run-v1",
            "--output-dir",
            "run",
        ])
        self.assertEqual("http://127.0.0.1:19085", parsed.n5_frame_base_url)
        self.assertEqual(300.0, parsed.timeout_seconds)
        self.assertEqual([], parsed.allow_http_simulator_host)
        self.assertFalse(parsed.resume)
        for name in (
            "ExistingDigestBulkExecutor",
            "ExistingFrameBundleBulkExecutor",
            "freeze_full_flow_bulk_live_provisioning_source_manifest",
            "run_full_flow_bulk_live_provisioning",
            "verify_full_flow_bulk_live_provisioning",
        ):
            self.assertIn(name, simulator.__all__)
            self.assertTrue(hasattr(simulator, name))

    def test_source_manifest_freeze_and_bulk_verifier_dispatch(self) -> None:
        with patch(
            "pathfinder.simulator.full_flow_bulk_live_provisioning."
            "freeze_full_flow_bulk_live_provisioning_source_manifest",
            return_value={"status": "FROZEN"},
        ) as freeze:
            status, payload, _ = self._invoke([
                "freeze-simulator-full-flow-bulk-provisioning-source-manifest",
                "--provisioning-catalog-dir",
                "catalog",
                "--artifact-binding-dir",
                "bindings",
                "--n4-package-dir",
                "n4",
                "--object-mapping",
                "mapping.json",
                "--output-path",
                "sources.json",
            ])
        self.assertEqual(0, status)
        self.assertEqual("FROZEN", payload["status"])
        freeze.assert_called_once_with(
            Path("catalog"),
            Path("bindings"),
            Path("n4"),
            Path("mapping.json"),
            output_path=Path("sources.json"),
        )

        with patch(
            "pathfinder.simulator.full_flow_bulk_live_provisioning."
            "verify_full_flow_bulk_live_provisioning",
            return_value={"status": "VERIFIED"},
        ) as verify:
            status, payload, _ = self._invoke([
                "verify-simulator-full-flow-bulk-live-provisioning",
                "--provisioning-catalog-dir",
                "catalog",
                "--artifact-binding-dir",
                "bindings",
                "--n4-package-dir",
                "n4",
                "--operator-source-manifest",
                "sources.json",
                "--output-dir",
                "run",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        verify.assert_called_once_with(
            Path("run"),
            provisioning_catalog_dir=Path("catalog"),
            artifact_binding_dir=Path("bindings"),
            n4_package_dir=Path("n4"),
            operator_source_manifest=Path("sources.json"),
        )

    def test_bulk_run_uses_only_runtime_tokens_and_production_adapters(
        self,
    ) -> None:
        environment = {
            "PATHFINDER_N5_MATERIALIZATION_TOKEN": "frame-runtime-token",
            "PATHFINDER_N5_DIGEST_TOKEN": "digest-runtime-token",
            "PATHFINDER_N4_PUBLICATION_TOKEN": "n4-runtime-token",
        }
        with (
            patch.dict(os.environ, environment, clear=False),
            patch(
                "pathfinder.simulator.full_flow_bulk_live_provisioning."
                "run_full_flow_bulk_live_provisioning",
                return_value={
                    "status": "VERIFIED",
                    "credentials_recorded": False,
                },
            ) as run,
        ):
            status, payload, output = self._invoke([
                "run-simulator-full-flow-bulk-live-provisioning",
                "--provisioning-catalog-dir",
                "catalog",
                "--artifact-binding-dir",
                "bindings",
                "--n4-package-dir",
                "n4",
                "--operator-source-manifest",
                "sources.json",
                "--n5-frame-base-url",
                "http://pathfinder-sim-n5-frame:19085",
                "--n5-digest-base-url",
                "http://pathfinder-sim-n5-digest:19185",
                "--n4-base-url",
                "http://pathfinder-sim-n4:19184",
                "--allow-http-simulator-host",
                "pathfinder-sim-n5-frame",
                "--allow-http-simulator-host",
                "pathfinder-sim-n5-digest",
                "--allow-http-simulator-host",
                "pathfinder-sim-n4",
                "--timeout-seconds",
                "42",
                "--run-id",
                "bulk-run-v1",
                "--output-dir",
                "run",
                "--resume",
            ])
        self.assertEqual(0, status)
        self.assertEqual("VERIFIED", payload["status"])
        for secret in environment.values():
            self.assertNotIn(secret, output)
        call = run.call_args
        self.assertEqual(
            (
                Path("catalog"),
                Path("bindings"),
                Path("n4"),
                Path("sources.json"),
            ),
            call.args,
        )
        frame = call.kwargs["frame_executor"]
        digest = call.kwargs["digest_executor"]
        self.assertIsInstance(frame, bulk.ExistingFrameBundleBulkExecutor)
        self.assertIsInstance(digest, bulk.ExistingDigestBulkExecutor)
        self.assertEqual("frame-runtime-token", frame.n5_config.bearer_token)
        self.assertEqual(
            "http://pathfinder-sim-n5-frame:19085",
            frame.n5_config.base_url,
        )
        self.assertEqual(
            (
                "pathfinder-sim-n5-frame",
                "pathfinder-sim-n5-digest",
                "pathfinder-sim-n4",
            ),
            frame.n5_config.simulator_private_http_hosts,
        )
        self.assertEqual(42.0, frame.n5_config.timeout_seconds)
        self.assertEqual(
            "digest-runtime-token",
            digest.n5_executor._config.bearer_token,
        )
        self.assertEqual(
            "http://pathfinder-sim-n5-digest:19185",
            digest.n5_executor._config.base_url,
        )
        self.assertEqual("n4-runtime-token", frame.n4_config.bearer_token)
        self.assertIs(frame.n4_config, digest.n4_config)
        self.assertEqual(42.0, frame.n4_config.timeout_seconds)
        self.assertEqual("bulk-run-v1", call.kwargs["run_id"])
        self.assertEqual(Path("run"), call.kwargs["output_dir"])
        self.assertTrue(call.kwargs["resume"])

    def test_bulk_run_refuses_missing_runtime_tokens(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            status, payload, output = self._invoke([
                "run-simulator-full-flow-bulk-live-provisioning",
                "--provisioning-catalog-dir",
                "catalog",
                "--artifact-binding-dir",
                "bindings",
                "--n4-package-dir",
                "n4",
                "--operator-source-manifest",
                "sources.json",
                "--n5-frame-base-url",
                "https://n5-frame.invalid",
                "--n5-digest-base-url",
                "https://n5-digest.invalid",
                "--n4-base-url",
                "https://n4.invalid",
                "--run-id",
                "bulk-run-v1",
                "--output-dir",
                "run",
            ])
        self.assertEqual(2, status)
        self.assertEqual("error", payload["status"])
        self.assertIn("PATHFINDER_N5_MATERIALIZATION_TOKEN", payload["message"])
        self.assertNotIn("https://", output)


if __name__ == "__main__":
    unittest.main()
