"""Tests for the portable N4 derived-representation store."""

from __future__ import annotations

import base64
import hashlib
import json
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from pathfinder.data_agent_client import (
    DataAgentAccessRequest,
    DataAgentClientSettings,
    HttpDataAgentClient,
)
from pathfinder.data_agent_manifest import load_data_agent_manifest
from pathfinder.data_agent_server import (
    DataAgentServerSettings,
    create_data_agent_http_server,
)
from pathfinder.frame_bundle import (
    FRAME_BUNDLE_SCHEMA_VERSION,
    OBJECT_MANIFEST_NAME,
    deterministic_frame_bundle_tar,
)
from pathfinder.simulator.n4_derived_data_plane import (
    CHECKSUMS_NAME,
    DATA_AGENT_MANIFEST_PATH,
    FRAME_BUNDLE_REPRESENTATION_ID,
    MULTIMODAL_DIGEST_MEDIA_TYPE,
    MULTIMODAL_DIGEST_REPRESENTATION_ID,
    N4ArtifactProvenance,
    N4DerivedArtifactInput,
    N4DerivedDataPlaneError,
    N4DerivedRepresentationStore,
    N4PublicationConflict,
    N4_DERIVED_BINDINGS_SCHEMA_VERSION,
    N4_LOGICAL_LOCATION,
    N4_LOGICAL_NODE_ID,
    OBJECT_CATALOG_PATH,
    PACKAGE_MANIFEST_NAME,
    build_n4_derived_data_package,
    build_n4_derived_data_package_from_manifest,
    resolve_current_n4_snapshot,
    verify_n4_derived_data_package,
    verify_n4_publication_receipt,
)


OBJECT_A = "nextqa-val-0000000001"
OBJECT_B = "nextqa-val-0000000002"
PLAN_FRAMES = "D-origin-warm-frames"
PLAN_DIGEST = "D-origin-warm-digest"

_TEST_JPEG_BASE64 = (
    "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAYEBAUEBAYFBQUGBgYHCQ4JCQgICRINDQoOFRIW"
    "FhUSFBQXGiEcFxgfGRQUHScdHyIjJSUlFhwpLCgkKyEkJST/2wBDAQYGBgkICREJCREkGBQY"
    "JCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCT/"
    "wAARCAACAAIDASIAAhEBAxEB/8QAFQABAQAAAAAAAAAAAAAAAAAAAAf/xAAUEAEAAAAAAAAA"
    "AAAAAAAAAAAA/8QAFQEBAQAAAAAAAAAAAAAAAAAABgj/xAAUEQEAAAAAAAAAAAAAAAAAAAAA"
    "/9oADAMBAAIRAxEAPwCdAAyqX//Z"
)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _jpeg(marker: int) -> bytes:
    payload = base64.b64decode(_TEST_JPEG_BASE64, validate=True)
    filler = bytes((marker, marker + 1))
    comment = b"\xff\xfe" + (len(filler) + 2).to_bytes(2, "big") + filler
    return payload[:2] + comment + payload[2:]


def _bundle(object_id: str, marker: int = 11) -> bytes:
    frames = [_jpeg(marker), _jpeg(marker + 2)]
    rows = [
        {
            "frame_index": index,
            "timestamp_seconds": 0.5 + index,
            "width": 2,
            "height": 2,
            "path": f"frames/{index:03d}.jpg",
            "jpeg_size_bytes": len(frame),
            "jpeg_sha256": _sha256(frame),
        }
        for index, frame in enumerate(frames)
    ]
    video_id = object_id.rsplit("-", 1)[-1]
    manifest = {
        "schema_version": FRAME_BUNDLE_SCHEMA_VERSION,
        "representation_id": FRAME_BUNDLE_REPRESENTATION_ID,
        "object_id": object_id,
        "source_video_id": video_id,
        "source_video_filename": f"{video_id}.mp4",
        "source_video_size_bytes": 1234,
        "source_video_sha256": "a" * 64,
        "source_duration_seconds": 12.5,
        "sampling": {
            "method": "uniform-midpoint",
            "frame_count": 2,
            "jpeg_max_dimension": 768,
            "jpeg_quality": 82,
            "jpeg_optimize": True,
        },
        "source_frame_descriptions": {
            "representation_id": "sampled_frames",
            "path": f"{object_id}/sampled_frames.json",
            "sha256": "b" * 64,
        },
        "generation_manifest_sha256": "c" * 64,
        "frames": rows,
        "frame_count": 2,
        "total_jpeg_bytes": sum(len(frame) for frame in frames),
        "software_versions": {"av": "17.0.1", "Pillow": "12.3.0"},
        "historical_visual_bytes_retained": False,
        "sampling_alignment_statement": (
            "These JPEG frames were regenerated from the same source video "
            "using the same sampling algorithm and are aligned with the "
            "frozen sampling metadata. The historical visual bytes were not "
            "retained, so this artifact does not claim byte identity with "
            "the historical visual input."
        ),
        "claims_byte_identity_with_historical_visual_input": False,
        "credentials_recorded": False,
        "llm_called": False,
        "network_calls_performed": False,
    }
    manifest_bytes = (
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    return deterministic_frame_bundle_tar(
        [(OBJECT_MANIFEST_NAME, manifest_bytes)]
        + [
            (f"frames/{index:03d}.jpg", frame)
            for index, frame in enumerate(frames)
        ]
    )


def _provenance(
    representation_id: str,
    *,
    source_marker: str = "a",
) -> N4ArtifactProvenance:
    return N4ArtifactProvenance(
        producer_node_id="N5",
        publication_source_id=(
            f"n5-materialization-{representation_id}-{source_marker}"
        ),
        source_representation_id="raw_video",
        source_content_sha256=source_marker * 64,
        derivation_id=f"derive-{representation_id}-v1",
        derivation_sha256="d" * 64,
    )


def _input(
    representation_id: str,
    raw: bytes,
    *,
    object_id: str = OBJECT_A,
    source_marker: str = "a",
) -> N4DerivedArtifactInput:
    plan = (
        PLAN_FRAMES
        if representation_id == FRAME_BUNDLE_REPRESENTATION_ID
        else PLAN_DIGEST
    )
    return N4DerivedArtifactInput(
        object_id=object_id,
        representation_id=representation_id,
        artifact_bytes=raw,
        plan_ids=(plan,),
        provenance=_provenance(
            representation_id,
            source_marker=source_marker,
        ),
        expected_sha256=_sha256(raw),
        expected_size_bytes=len(raw),
    )


def _request(
    representation_id: str,
    *,
    access_id: str,
) -> DataAgentAccessRequest:
    return DataAgentAccessRequest(
        access_id=access_id,
        session_id="n4-loopback-session",
        trial_id="n4-loopback-trial",
        plan_id=(
            PLAN_FRAMES
            if representation_id == FRAME_BUNDLE_REPRESENTATION_ID
            else PLAN_DIGEST
        ),
        plan_epoch=0,
        task_class_id="video_qa",
        representation_id=representation_id,
        event_index=0,
        latency_multiplier=1.0,
        binding={"location": N4_LOGICAL_LOCATION},
        object_id=OBJECT_A,
    )


class N4DerivedDataPlaneTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.bundle = _bundle(OBJECT_A)
        self.digest = (
            "Two musicians perform while another person approaches.\n"
        ).encode("utf-8")

    def artifacts(self) -> list[N4DerivedArtifactInput]:
        return [
            _input(FRAME_BUNDLE_REPRESENTATION_ID, self.bundle),
            _input(MULTIMODAL_DIGEST_REPRESENTATION_ID, self.digest),
        ]

    def build(self, name: str = "package") -> Path:
        output = self.root / name
        build_n4_derived_data_package(
            self.artifacts(),
            output_dir=output,
            package_id="n4-derived-test-v1",
            catalog_version="n4-catalog-v1",
        )
        return output

    def test_builds_endpoint_free_standard_data_agent_package(self) -> None:
        output = self.build()
        verified = verify_n4_derived_data_package(output)
        self.assertEqual(
            "VERIFIED_N4_DERIVED_DATA_PACKAGE",
            verified["status"],
        )
        self.assertEqual(N4_LOGICAL_NODE_ID, verified["logical_node_id"])
        self.assertEqual(1, verified["object_count"])
        self.assertEqual(2, verified["artifact_count"])
        self.assertEqual(
            [
                MULTIMODAL_DIGEST_REPRESENTATION_ID,
                FRAME_BUNDLE_REPRESENTATION_ID,
            ],
            verified["representation_ids"],
        )

        manifest = load_data_agent_manifest(output / DATA_AGENT_MANIFEST_PATH)
        self.assertEqual("N4", manifest.node_id)
        self.assertEqual("n4-catalog-v1", manifest.object_catalog.catalog_version)
        for representation_id, expected in (
            (FRAME_BUNDLE_REPRESENTATION_ID, self.bundle),
            (MULTIMODAL_DIGEST_REPRESENTATION_ID, self.digest),
        ):
            resolved = manifest.resolve(
                plan_id=(
                    PLAN_FRAMES
                    if representation_id == FRAME_BUNDLE_REPRESENTATION_ID
                    else PLAN_DIGEST
                ),
                object_id=OBJECT_A,
                representation_id=representation_id,
                requested_location=N4_LOGICAL_LOCATION,
            )
            self.assertEqual(expected, resolved.path.read_bytes())

    def test_operator_manifest_builds_without_persisting_source_paths(
        self,
    ) -> None:
        bundle_path = self.root / "inputs" / "frames.tar"
        digest_path = self.root / "inputs" / "digest.txt"
        bundle_path.parent.mkdir()
        bundle_path.write_bytes(self.bundle)
        digest_path.write_bytes(self.digest)
        bindings = {
            "schema_version": N4_DERIVED_BINDINGS_SCHEMA_VERSION,
            "package_id": "n4-manifest-test-v1",
            "catalog_version": "n4-manifest-catalog-v1",
            "artifacts": [
                {
                    "object_id": OBJECT_A,
                    "representation_id": representation_id,
                    "artifact_path": path.relative_to(self.root).as_posix(),
                    "plan_ids": [plan_id],
                    "expected_sha256": _sha256(raw),
                    "expected_size_bytes": len(raw),
                    "provenance": _provenance(
                        representation_id,
                    ).to_dict(),
                }
                for representation_id, path, raw, plan_id in (
                    (
                        FRAME_BUNDLE_REPRESENTATION_ID,
                        bundle_path,
                        self.bundle,
                        PLAN_FRAMES,
                    ),
                    (
                        MULTIMODAL_DIGEST_REPRESENTATION_ID,
                        digest_path,
                        self.digest,
                        PLAN_DIGEST,
                    ),
                )
            ],
            "credentials_recorded": False,
        }
        source = self.root / "bindings.json"
        source.write_text(
            json.dumps(bindings, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        output = self.root / "from-manifest"

        result = build_n4_derived_data_package_from_manifest(
            source,
            output_dir=output,
        )

        self.assertEqual(
            "VERIFIED_N4_DERIVED_DATA_PACKAGE",
            result["status"],
        )
        published = "".join(
            path.read_text(encoding="utf-8")
            for path in output.rglob("*.json")
        )
        self.assertNotIn(str(self.root), published)
        self.assertNotIn("artifact_path", published)

    def test_package_metadata_contains_no_deployment_or_cost_values(self) -> None:
        output = self.build()
        for relative in (
            PACKAGE_MANIFEST_NAME,
            DATA_AGENT_MANIFEST_PATH,
            OBJECT_CATALOG_PATH,
        ):
            text = (output / relative).read_text(encoding="utf-8").lower()
            self.assertNotIn("://", text)
            self.assertNotIn("api_key", text)
            self.assertNotIn("bearer", text)
            self.assertNotIn("minimum_latency", text)
            self.assertNotIn("realized_cost", text)
            self.assertNotIn(str(self.root).lower(), text)

    def test_build_is_byte_deterministic(self) -> None:
        first = self.build("first")
        second = self.build("second")
        first_files = {
            path.relative_to(first).as_posix(): path.read_bytes()
            for path in first.rglob("*")
            if path.is_file()
        }
        second_files = {
            path.relative_to(second).as_posix(): path.read_bytes()
            for path in second.rglob("*")
            if path.is_file()
        }
        self.assertEqual(first_files, second_files)

    def test_noncanonical_frame_bundle_is_rejected(self) -> None:
        broken = self.bundle + b"trailing"
        with self.assertRaisesRegex(
            N4DerivedDataPlaneError,
            "canonical frame bundle",
        ):
            build_n4_derived_data_package(
                [_input(FRAME_BUNDLE_REPRESENTATION_ID, broken)],
                output_dir=self.root / "bad",
                package_id="n4-bad-v1",
                catalog_version="n4-bad-catalog-v1",
            )

    def test_non_utf8_and_non_nfc_digest_are_rejected(self) -> None:
        for name, payload, message in (
            ("binary", b"\xff", "UTF-8"),
            ("non-nfc", "Cafe\u0301".encode("utf-8"), "NFC"),
        ):
            with self.subTest(name=name):
                with self.assertRaisesRegex(N4DerivedDataPlaneError, message):
                    build_n4_derived_data_package(
                        [_input(MULTIMODAL_DIGEST_REPRESENTATION_ID, payload)],
                        output_dir=self.root / name,
                        package_id=f"n4-{name}-v1",
                        catalog_version=f"n4-{name}-catalog-v1",
                    )

    def test_checksum_and_catalog_tampering_fail_closed(self) -> None:
        output = self.build()
        digest_path = (
            output
            / "artifacts"
            / OBJECT_A
            / f"{MULTIMODAL_DIGEST_REPRESENTATION_ID}.txt"
        )
        digest_path.write_bytes(self.digest + b"changed")
        with self.assertRaisesRegex(N4DerivedDataPlaneError, "canonical"):
            verify_n4_derived_data_package(output)

        other = self.build("catalog-tamper")
        catalog_path = other / OBJECT_CATALOG_PATH
        catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
        catalog["objects"][OBJECT_A]["representations"][
            MULTIMODAL_DIGEST_REPRESENTATION_ID
        ]["path"] = "../artifacts/other.txt"
        catalog_path.write_text(
            json.dumps(catalog, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        checksummed = {
            path.relative_to(other).as_posix(): path.read_bytes()
            for path in other.rglob("*")
            if path.is_file() and path.name != CHECKSUMS_NAME
        }
        (other / CHECKSUMS_NAME).write_bytes(
            "".join(
                f"{_sha256(payload)}  {name}\n"
                for name, payload in sorted(checksummed.items())
            ).encode("utf-8"),
        )
        with self.assertRaisesRegex(
            N4DerivedDataPlaneError,
            "catalog does not match",
        ):
            verify_n4_derived_data_package(other)

    def test_loopback_standard_data_agent_serves_both_representations(self) -> None:
        output = self.build()
        token = "n4-loopback-control-token"
        server = create_data_agent_http_server(
            manifest_path=output / DATA_AGENT_MANIFEST_PATH,
            operation_db=self.root / "data-agent.sqlite3",
            settings=DataAgentServerSettings(
                host="127.0.0.1",
                port=0,
                token=token,
                artifact_secret="n4-loopback-artifact-secret",
            ),
        )
        thread = threading.Thread(
            target=server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )
        thread.start()

        def close() -> None:
            try:
                server.shutdown()
            finally:
                server.server_close()
                thread.join(5)

        self.addCleanup(close)
        host, port = server.server_address[:2]
        client = HttpDataAgentClient(
            DataAgentClientSettings(
                base_url=f"http://{host}:{port}",
                token=token,
                timeout_seconds=5,
                max_retries=0,
                max_artifact_bytes=len(self.bundle) + 1024,
            )
        )
        inline = client.access(
            _request(
                MULTIMODAL_DIGEST_REPRESENTATION_ID,
                access_id="n4-digest-access",
            )
        )
        self.assertEqual(self.digest.decode("utf-8"), inline.payload.value)
        self.assertEqual(_sha256(self.digest), inline.payload.sha256)
        self.assertEqual("n4-catalog-v1", inline.object_catalog_version)
        self.assertEqual(MULTIMODAL_DIGEST_MEDIA_TYPE, inline.payload.media_type)

        binary = client.fetch_binary_artifact(
            _request(
                FRAME_BUNDLE_REPRESENTATION_ID,
                access_id="n4-bundle-access",
            ),
            allowed_media_types={"application/x-tar"},
        )
        self.assertEqual(self.bundle, binary.data)
        self.assertEqual(_sha256(self.bundle), binary.sha256)
        self.assertEqual("n4-catalog-v1", binary.object_catalog_version)


class N4PublicationStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.store_root = self.root / "n4-store"
        self.bundle = _bundle(OBJECT_A)
        self.digest_v1 = b"First derived digest.\n"
        self.digest_v2 = b"Second derived digest with more evidence.\n"

    def artifact(
        self,
        representation_id: str,
        raw: bytes,
        *,
        source_marker: str = "a",
    ) -> N4DerivedArtifactInput:
        return _input(
            representation_id,
            raw,
            source_marker=source_marker,
        )

    def initial_publish(self) -> tuple[N4DerivedRepresentationStore, object]:
        store = N4DerivedRepresentationStore(self.store_root)
        result = store.publish(
            publication_id="n5-publish-object-a-v1",
            package_id="n4-generation-v1",
            catalog_version="n4-catalog-v1",
            expected_current_catalog_version=None,
            artifacts=[
                self.artifact(
                    FRAME_BUNDLE_REPRESENTATION_ID,
                    self.bundle,
                ),
                self.artifact(
                    MULTIMODAL_DIGEST_REPRESENTATION_ID,
                    self.digest_v1,
                ),
            ],
        )
        return store, result

    def test_publication_commits_complete_generation_and_receipt(self) -> None:
        store, result = self.initial_publish()
        self.assertFalse(result.idempotent_replay)
        verified_receipt = verify_n4_publication_receipt(result.receipt)
        self.assertEqual("COMMITTED", verified_receipt["status"])
        self.assertTrue(verified_receipt["atomic_visibility"])
        self.assertEqual("N4", verified_receipt["logical_node_id"])
        self.assertEqual("n4-catalog-v1", result.snapshot.catalog_version)
        self.assertEqual(result.snapshot, store.current_snapshot())
        self.assertEqual(
            result.snapshot,
            resolve_current_n4_snapshot(self.store_root),
        )
        self.assertNotIn(
            str(self.root),
            json.dumps(result.receipt, sort_keys=True),
        )

    def test_update_replaces_one_artifact_and_preserves_old_snapshot(self) -> None:
        store, first = self.initial_publish()
        second = store.publish(
            publication_id="n5-publish-object-a-v2",
            package_id="n4-generation-v2",
            catalog_version="n4-catalog-v2",
            expected_current_catalog_version="n4-catalog-v1",
            artifacts=[
                self.artifact(
                    MULTIMODAL_DIGEST_REPRESENTATION_ID,
                    self.digest_v2,
                    source_marker="b",
                )
            ],
        )
        self.assertNotEqual(first.snapshot.generation_id, second.snapshot.generation_id)
        self.assertEqual(
            self.digest_v1,
            (
                first.snapshot.package_dir
                / "artifacts"
                / OBJECT_A
                / "multimodal_digest.txt"
            ).read_bytes(),
        )
        self.assertEqual(
            self.digest_v2,
            (
                second.snapshot.package_dir
                / "artifacts"
                / OBJECT_A
                / "multimodal_digest.txt"
            ).read_bytes(),
        )
        self.assertEqual(
            self.bundle,
            (
                second.snapshot.package_dir
                / "artifacts"
                / OBJECT_A
                / "sampled_frame_bundle.tar"
            ).read_bytes(),
        )
        self.assertEqual(second.snapshot, store.current_snapshot())

    def test_retry_is_idempotent_across_store_restart(self) -> None:
        _store, first = self.initial_publish()
        reopened = N4DerivedRepresentationStore(self.store_root)
        replay = reopened.publish(
            publication_id="n5-publish-object-a-v1",
            package_id="n4-generation-v1",
            catalog_version="n4-catalog-v1",
            expected_current_catalog_version=None,
            artifacts=[
                self.artifact(
                    FRAME_BUNDLE_REPRESENTATION_ID,
                    self.bundle,
                ),
                self.artifact(
                    MULTIMODAL_DIGEST_REPRESENTATION_ID,
                    self.digest_v1,
                ),
            ],
        )
        self.assertTrue(replay.idempotent_replay)
        self.assertEqual(first.receipt, replay.receipt)
        self.assertEqual(first.snapshot, replay.snapshot)

    def test_publication_id_conflict_and_stale_catalog_fail_closed(self) -> None:
        store, first = self.initial_publish()
        with self.assertRaisesRegex(N4PublicationConflict, "different request"):
            store.publish(
                publication_id="n5-publish-object-a-v1",
                package_id="n4-generation-v1",
                catalog_version="n4-catalog-v1",
                expected_current_catalog_version=None,
                artifacts=[
                    self.artifact(
                        MULTIMODAL_DIGEST_REPRESENTATION_ID,
                        self.digest_v2,
                        source_marker="b",
                    )
                ],
            )
        with self.assertRaisesRegex(N4PublicationConflict, "compare-and-swap"):
            store.publish(
                publication_id="n5-publish-stale-v2",
                package_id="n4-generation-v2",
                catalog_version="n4-catalog-v2",
                expected_current_catalog_version="wrong-catalog-v1",
                artifacts=[
                    self.artifact(
                        MULTIMODAL_DIGEST_REPRESENTATION_ID,
                        self.digest_v2,
                        source_marker="b",
                    )
                ],
            )
        self.assertEqual(first.snapshot, store.current_snapshot())
        self.assertEqual(
            [],
            list(store.generations.glob(".candidate-*")),
        )

    def test_failed_staging_never_changes_visible_catalog(self) -> None:
        store, first = self.initial_publish()

        def fail_after_partial(*args: object, **kwargs: object) -> object:
            output = Path(kwargs["output_dir"])
            output.mkdir()
            (output / "partial-artifact").write_bytes(b"partial")
            raise N4DerivedDataPlaneError("injected staging failure")

        with patch(
            "pathfinder.simulator.n4_derived_data_plane."
            "build_n4_derived_data_package",
            side_effect=fail_after_partial,
        ):
            with self.assertRaisesRegex(
                N4DerivedDataPlaneError,
                "injected staging failure",
            ):
                store.publish(
                    publication_id="n5-publish-failing-v2",
                    package_id="n4-generation-v2",
                    catalog_version="n4-catalog-v2",
                    expected_current_catalog_version="n4-catalog-v1",
                    artifacts=[
                        self.artifact(
                            MULTIMODAL_DIGEST_REPRESENTATION_ID,
                            self.digest_v2,
                            source_marker="b",
                        )
                    ],
                )
        self.assertEqual(first.snapshot, store.current_snapshot())
        self.assertEqual(
            [],
            list(store.generations.glob(".candidate-*")),
        )


if __name__ == "__main__":
    unittest.main()
