from __future__ import annotations

import base64
import hashlib
import json
import struct
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from pathfinder.simulator.full_flow_live_provisioning_smoke import (
    CHECKSUMS_NAME,
    DIGEST_RECEIPT_NAME,
    RECEIPT_NAME,
    FullFlowLiveProvisioningSmokeError,
    HttpN5DigestMaterializationExecutor,
    N4PublicationHttpClientConfig,
    N5DigestHttpClientConfig,
    run_n5_n4_live_frame_bundle_provisioning_smoke,
    run_n5_n4_live_multimodal_digest_provisioning_smoke,
    verify_n5_n4_live_frame_bundle_provisioning_smoke,
    verify_n5_n4_live_multimodal_digest_provisioning_smoke,
)
from pathfinder.simulator.n4_derived_data_plane import (
    N4DerivedRepresentationStore,
    PACKAGE_MANIFEST_NAME,
)
from pathfinder.simulator.n4_publication_http import (
    N4PublicationHTTPSettings,
    create_n4_publication_http_server,
)
from pathfinder.simulator.n5_materialization import (
    N5MaterializationHttpClientConfig,
    N5MaterializationHttpServer,
    N5MaterializationHttpService,
    N5MaterializationRuntime,
    freeze_n5_materialization_plan,
)
from pathfinder.simulator.n5_digest_http import (
    N5DigestHTTPSettings,
    create_n5_digest_http_server,
)
from pathfinder.simulator.n5_digest_materialization import (
    VisionDigestResult,
    freeze_n5_multimodal_digest_plan,
)
from pathfinder.video_prep import (
    FRAME_SCHEMA_VERSION,
    PREP_SCHEMA_VERSION,
    SampledImage,
)


OBJECT_ID = "nextqa-val-0000000001"
SECOND_OBJECT_ID = "nextqa-val-0000000002"
VIDEO_ID = "0000000001"
VIDEO_NAME = f"{VIDEO_ID}.mp4"
SOURCE = b"\x00\x00\x00\x18ftypmp42local-live-provisioning-video"
VERSIONS = {
    "Pillow": "12.3.0-test",
    "av": "17.0.1-test",
    "pathfinder-minimal": "0.1-test",
}
N5_TOKEN = "runtime-only-n5-token"
N4_TOKEN = "runtime-only-n4-token"
DIGEST_TOKEN = "runtime-only-digest-token"
DIGEST_MODEL = "qwen3.8-27b"

_JPEG_BASE64 = (
    "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAYEBAUEBAYFBQUGBgYHCQ4JCQgICRINDQoOFRIW"
    "FhUSFBQXGiEcFxgfGRQUHScdHyIjJSUlFhwpLCgkKyEkJST/2wBDAQYGBgkICREJCREkGBQY"
    "JCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCT/wAAR"
    "CAACAAIDASIAAhEBAxEB/8QAFQABAQAAAAAAAAAAAAAAAAAAAAf/xAAUEAEAAAAAAAAAAAAA"
    "AAAAAAAA/8QAFQEBAQAAAAAAAAAAAAAAAAAABgj/xAAUEQEAAAAAAAAAAAAAAAAAAAAA/9oA"
    "DAMBAAIRAxEAPwCdAAyqX//Z"
)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _jpeg(index: int) -> bytes:
    payload = base64.b64decode(_JPEG_BASE64, validate=True)
    marker = b"live" + bytes([index])
    comment = b"\xff\xfe" + (len(marker) + 2).to_bytes(2, "big") + marker
    return payload[:2] + comment + payload[2:]


def _digest_mp4() -> bytes:
    brands = b"isom" + struct.pack(">I", 512) + b"isomiso2mp41"
    ftyp = struct.pack(">I", len(brands) + 8) + b"ftyp" + brands
    body = bytes(range(128))
    return ftyp + struct.pack(">I", len(body) + 8) + b"mdat" + body


def _digest_frames() -> list[SampledImage]:
    return [
        SampledImage(
            frame_index=index,
            timestamp_seconds=float(index * 2 + 1),
            width=320,
            height=180,
            jpeg_bytes=(
                b"\xff\xd8"
                + bytes((index * 7 + offset) % 256 for offset in range(32))
                + b"\xff\xd9"
            ),
        )
        for index in range(3)
    ]


class _OfflineVisionAdapter:
    def __init__(self) -> None:
        self.calls = 0

    def generate_digest(self, **kwargs) -> VisionDigestResult:
        self.calls += 1
        return VisionDigestResult(
            model_id=kwargs["expected_model_id"],
            digest={
                "events": [
                    {
                        "start_seconds": 1.0,
                        "end_seconds": 3.0,
                        "description": "A person walks through a room.",
                    }
                ],
                "summary": "A person walks through a room.",
            },
            response_sha256=hashlib.sha256(
                b"offline-vision-response"
            ).hexdigest(),
            protocol_attempts=1,
            llm_called=True,
            adapter_id="offline-test-vision-v1",
        )


class _Sampler:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(
        self,
        path: Path,
        *,
        frame_count: int,
        jpeg_max_dimension: int,
    ) -> tuple[list[SampledImage], float]:
        self.calls += 1
        if path.read_bytes() != SOURCE or jpeg_max_dimension != 768:
            raise RuntimeError("test source or dimension changed")
        return (
            [
                SampledImage(
                    frame_index=index,
                    timestamp_seconds=0.5 + index,
                    width=2,
                    height=2,
                    jpeg_bytes=_jpeg(index),
                )
                for index in range(frame_count)
            ],
            12.0,
        )


def _description(
    *,
    object_id: str = OBJECT_ID,
    video_id: str = VIDEO_ID,
) -> bytes:
    value = {
        "schema_version": FRAME_SCHEMA_VERSION,
        "object_id": object_id,
        "source_video_id": video_id,
        "source_video_sha256": _sha256(SOURCE),
        "source_duration_seconds": 12.0,
        "sampling": {
            "method": "uniform-midpoint",
            "frame_count": 2,
            "jpeg_max_dimension": 768,
        },
        "generator": {
            "model": "historical-model",
            "temperature": 0,
            "prompt_sha256": "a" * 64,
        },
        "frames": [
            {
                "frame_index": index,
                "timestamp_seconds": 0.5 + index,
                "width": 2,
                "height": 2,
                "description": f"frame {index}",
                "visible_text": None,
            }
            for index in range(2)
        ],
    }
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def _generation_manifest(
    description: bytes,
    *,
    object_id: str = OBJECT_ID,
    video_id: str = VIDEO_ID,
) -> bytes:
    video_name = f"{video_id}.mp4"
    value = {
        "schema_version": PREP_SCHEMA_VERSION,
        "frame_count": 2,
        "jpeg_max_dimension": 768,
        "credentials_recorded": False,
        "objects": [
            {
                "object_id": object_id,
                "source_video": {
                    "filename": video_name,
                    "size_bytes": len(SOURCE),
                    "sha256": _sha256(SOURCE),
                },
                "representations": {
                    "sampled_frames": {
                        "path": f"{object_id}/sampled_frames.json",
                        "size_bytes": len(description),
                        "sha256": _sha256(description),
                    }
                },
            }
        ],
    }
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


class _RunningServer:
    def __init__(self, server: object) -> None:
        self.server = server
        self.thread = threading.Thread(
            target=server.serve_forever,
            daemon=True,
        )
        self.thread.start()

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_port}"

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(5)


class LiveProvisioningSmokeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name)
        description = _description()
        self.freeze_sampler = _Sampler()
        self.frozen = freeze_n5_materialization_plan(
            plan_id="live-frame-bundle-plan-v1",
            idempotency_key="live-frame-bundle-request-v1",
            object_id=OBJECT_ID,
            source_video_id=VIDEO_ID,
            source_video_filename=VIDEO_NAME,
            source_video_bytes=SOURCE,
            source_frame_descriptions_path=(
                f"{OBJECT_ID}/sampled_frames.json"
            ),
            source_frame_descriptions_bytes=description,
            generation_manifest_bytes=_generation_manifest(description),
            frame_count=2,
            jpeg_max_dimension=768,
            sampler=self.freeze_sampler,
            software_versions=VERSIONS,
        )
        self.runtime_sampler = _Sampler()
        n5_service = N5MaterializationHttpService(
            runtime=N5MaterializationRuntime(
                sampler=self.runtime_sampler,
                software_versions=VERSIONS,
            ),
            bearer_token=N5_TOKEN,
            state_dir=self.root / "n5-state",
        )
        self.n5 = _RunningServer(
            N5MaterializationHttpServer(("127.0.0.1", 0), n5_service)
        )
        self.n4_store_root = self.root / "n4-state"
        self.n4 = _RunningServer(
            create_n4_publication_http_server(
                self.n4_store_root,
                settings=N4PublicationHTTPSettings(
                    bearer_token=N4_TOKEN,
                    host="127.0.0.1",
                    port=0,
                    max_request_bytes=1024 * 1024,
                    max_artifact_bytes=512 * 1024,
                ),
            )
        )

    def tearDown(self) -> None:
        self.n5.close()
        self.n4.close()
        self.temporary.cleanup()

    def arguments(self, output: Path) -> dict:
        return {
            "n5_plan": self.frozen.plan,
            "source_video_bytes": SOURCE,
            "n5_config": N5MaterializationHttpClientConfig(
                base_url=self.n5.base_url,
                bearer_token=N5_TOKEN,
                timeout_seconds=5,
            ),
            "n4_config": N4PublicationHttpClientConfig(
                base_url=self.n4.base_url,
                bearer_token=N4_TOKEN,
                timeout_seconds=5,
                max_json_bytes=1024 * 1024,
            ),
            "smoke_id": "local-live-provision-v1",
            "publication_id": "local-live-publication-v1",
            "package_id": "local-live-n4-package-v1",
            "catalog_version": "local-live-n4-catalog-v1",
            "expected_current_catalog_version": None,
            "output_dir": output,
        }

    def test_real_local_http_chain_freezes_bound_receipt(self) -> None:
        output = self.root / "receipt"
        result = run_n5_n4_live_frame_bundle_provisioning_smoke(
            **self.arguments(output)
        )

        self.assertEqual("VERIFIED", result["status"])
        self.assertTrue(result["n5_fresh_materialization_executed"])
        self.assertTrue(result["n4_fresh_publication_executed"])
        self.assertTrue(result["fresh_end_to_end_execution_observed"])
        self.assertFalse(result["durable_replay_adopted"])
        self.assertTrue(result["n4_atomic_visibility_verified"])
        self.assertTrue(result["preprovisioned_serve_gate_still_required"])
        self.assertFalse(result["multimodal_digest_live_provisioning_verified"])
        self.assertEqual(
            [self.frozen.plan["plan_id"]],
            result["n4_access_plan_ids"],
        )
        self.assertEqual(
            "n5-materialization-plan-default",
            result["n4_access_plan_ids_source"],
        )
        self.assertEqual(1, self.runtime_sampler.calls)
        self.assertEqual(
            {CHECKSUMS_NAME, RECEIPT_NAME},
            {path.name for path in output.iterdir()},
        )
        receipt = json.loads((output / RECEIPT_NAME).read_text())
        serialized = json.dumps(receipt, sort_keys=True)
        self.assertNotIn(self.n5.base_url, serialized)
        self.assertNotIn(self.n4.base_url, serialized)
        self.assertNotIn(N5_TOKEN, serialized)
        self.assertNotIn(N4_TOKEN, serialized)
        self.assertFalse(receipt["materialization_latency_measured"])
        self.assertFalse(receipt["monetary_cost_measured"])
        self.assertFalse(receipt["upcloud_used"])
        self.assertEqual(
            [self.frozen.plan["plan_id"]],
            receipt["n4_access_plan_ids"],
        )

        snapshot = N4DerivedRepresentationStore(
            self.n4_store_root
        ).current_snapshot()
        self.assertIsNotNone(snapshot)
        manifest = json.loads(
            (snapshot.package_dir / PACKAGE_MANIFEST_NAME)
            .read_text()
        )
        artifact = (
            snapshot.package_dir
            / manifest["objects"][0]["artifact_package_path"]
        ).read_bytes()
        self.assertEqual(
            self.frozen.plan["expected_output"]["artifact_sha256"],
            _sha256(artifact),
        )
        self.assertEqual(
            [self.frozen.plan["plan_id"]],
            manifest["objects"][0]["plan_ids"],
        )

    def test_explicit_n4_access_plan_ids_are_not_n5_lineage(self) -> None:
        output = self.root / "explicit-access-receipt"
        arguments = self.arguments(output)
        arguments["n4_access_plan_ids"] = ["D7", "D3"]
        result = run_n5_n4_live_frame_bundle_provisioning_smoke(**arguments)

        self.assertEqual(["D3", "D7"], result["n4_access_plan_ids"])
        self.assertEqual("explicit", result["n4_access_plan_ids_source"])
        self.assertNotIn(
            self.frozen.plan["plan_id"],
            result["n4_access_plan_ids"],
        )
        snapshot = N4DerivedRepresentationStore(
            self.n4_store_root
        ).current_snapshot()
        manifest = json.loads(
            (snapshot.package_dir / PACKAGE_MANIFEST_NAME).read_text()
        )
        self.assertEqual(["D3", "D7"], manifest["objects"][0]["plan_ids"])
        verify_n5_n4_live_frame_bundle_provisioning_smoke(
            output,
            n5_plan=self.frozen.plan,
            n4_access_plan_ids=["D7", "D3"],
        )
        with self.assertRaisesRegex(
            FullFlowLiveProvisioningSmokeError,
            "access-plan binding",
        ):
            verify_n5_n4_live_frame_bundle_provisioning_smoke(
                output,
                n5_plan=self.frozen.plan,
                n4_access_plan_ids=["D2"],
            )

    def test_two_n5_plans_share_one_n4_access_contract(self) -> None:
        access_plan_ids = ["D3", "D7"]
        first_arguments = self.arguments(self.root / "first-object")
        first_arguments["n4_access_plan_ids"] = access_plan_ids
        first = run_n5_n4_live_frame_bundle_provisioning_smoke(
            **first_arguments
        )

        second_object_id = "nextqa-val-0000000002"
        second_video_id = "0000000002"
        description = _description(
            object_id=second_object_id,
            video_id=second_video_id,
        )
        second_frozen = freeze_n5_materialization_plan(
            plan_id="live-frame-bundle-plan-v2",
            idempotency_key="live-frame-bundle-request-v2",
            object_id=second_object_id,
            source_video_id=second_video_id,
            source_video_filename=f"{second_video_id}.mp4",
            source_video_bytes=SOURCE,
            source_frame_descriptions_path=(
                f"{second_object_id}/sampled_frames.json"
            ),
            source_frame_descriptions_bytes=description,
            generation_manifest_bytes=_generation_manifest(
                description,
                object_id=second_object_id,
                video_id=second_video_id,
            ),
            frame_count=2,
            jpeg_max_dimension=768,
            sampler=_Sampler(),
            software_versions=VERSIONS,
        )
        second_arguments = self.arguments(self.root / "second-object")
        second_arguments.update({
            "n5_plan": second_frozen.plan,
            "smoke_id": "local-live-provision-v2",
            "publication_id": "local-live-publication-v2",
            "package_id": "local-live-n4-package-v2",
            "catalog_version": "local-live-n4-catalog-v2",
            "expected_current_catalog_version": first[
                "n4_committed_catalog_version"
            ],
            "n4_access_plan_ids": access_plan_ids,
        })
        second = run_n5_n4_live_frame_bundle_provisioning_smoke(
            **second_arguments
        )

        self.assertEqual(access_plan_ids, second["n4_access_plan_ids"])
        self.assertEqual("explicit", second["n4_access_plan_ids_source"])
        snapshot = N4DerivedRepresentationStore(
            self.n4_store_root
        ).current_snapshot()
        manifest = json.loads(
            (snapshot.package_dir / PACKAGE_MANIFEST_NAME).read_text()
        )
        self.assertEqual(
            {OBJECT_ID, second_object_id},
            {row["object_id"] for row in manifest["objects"]},
        )
        self.assertEqual(
            {tuple(access_plan_ids)},
            {tuple(row["plan_ids"]) for row in manifest["objects"]},
        )
        self.assertNotEqual(
            self.frozen.plan["plan_id"],
            second_frozen.plan["plan_id"],
        )
        self.assertEqual(2, self.runtime_sampler.calls)

    def test_legacy_frame_receipt_remains_verifiable(self) -> None:
        output = self.root / "legacy-receipt"
        run_n5_n4_live_frame_bundle_provisioning_smoke(
            **self.arguments(output)
        )
        receipt_path = output / RECEIPT_NAME
        value = json.loads(receipt_path.read_text())
        value["schema_version"] = (
            "pathfinder.local-n5-n4-live-provisioning-smoke/v1alpha1"
        )
        del value["n4_access_plan_ids"]
        del value["n4_access_plan_ids_source"]
        del value["n4_package_id"]
        del value["n4_publication_request_sha256"]
        del value["receipt_sha256"]
        value["receipt_sha256"] = _sha256(_canonical(value))
        payload = (
            json.dumps(value, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        receipt_path.write_bytes(payload)
        (output / CHECKSUMS_NAME).write_text(
            f"{_sha256(payload)}  {RECEIPT_NAME}\n",
            encoding="utf-8",
        )

        verified = verify_n5_n4_live_frame_bundle_provisioning_smoke(
            output,
            n5_plan=self.frozen.plan,
        )
        self.assertEqual(
            [self.frozen.plan["plan_id"]],
            verified["n4_access_plan_ids"],
        )
        self.assertEqual(
            "legacy-schema-inference",
            verified["n4_access_plan_ids_source"],
        )

    def test_receipt_is_plan_bound_and_tamper_evident(self) -> None:
        output = self.root / "receipt"
        run_n5_n4_live_frame_bundle_provisioning_smoke(
            **self.arguments(output)
        )
        verify_n5_n4_live_frame_bundle_provisioning_smoke(
            output,
            n5_plan=self.frozen.plan,
        )

        receipt_path = output / RECEIPT_NAME
        value = json.loads(receipt_path.read_text())
        value["artifact_size_bytes"] += 1
        del value["receipt_sha256"]
        value["receipt_sha256"] = _sha256(_canonical(value))
        payload = (
            json.dumps(value, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        receipt_path.write_bytes(payload)
        (output / CHECKSUMS_NAME).write_text(
            f"{_sha256(payload)}  {RECEIPT_NAME}\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            FullFlowLiveProvisioningSmokeError,
            "binding",
        ):
            verify_n5_n4_live_frame_bundle_provisioning_smoke(
                output,
                n5_plan=self.frozen.plan,
            )

    def test_replay_is_adopted_but_cannot_masquerade_as_fresh(self) -> None:
        run_n5_n4_live_frame_bundle_provisioning_smoke(
            **self.arguments(self.root / "first")
        )
        result = run_n5_n4_live_frame_bundle_provisioning_smoke(
            **self.arguments(self.root / "replay")
        )
        self.assertFalse(result["n5_fresh_materialization_executed"])
        self.assertFalse(result["n4_fresh_publication_executed"])
        self.assertFalse(result["fresh_end_to_end_execution_observed"])
        self.assertTrue(result["durable_replay_adopted"])
        self.assertEqual(1, self.runtime_sampler.calls)

    def test_wrong_n4_credential_fails_without_receipt(self) -> None:
        arguments = self.arguments(self.root / "unauthorized")
        arguments["n4_config"] = N4PublicationHttpClientConfig(
            base_url=self.n4.base_url,
            bearer_token="wrong-runtime-only-token",
            timeout_seconds=5,
            max_json_bytes=1024 * 1024,
        )
        with self.assertRaisesRegex(
            FullFlowLiveProvisioningSmokeError,
            "HTTP 401",
        ):
            run_n5_n4_live_frame_bundle_provisioning_smoke(**arguments)
        self.assertFalse((self.root / "unauthorized").exists())

        recovered = run_n5_n4_live_frame_bundle_provisioning_smoke(
            **self.arguments(self.root / "recovered")
        )
        self.assertFalse(recovered["n5_fresh_materialization_executed"])
        self.assertTrue(recovered["n4_fresh_publication_executed"])
        self.assertTrue(recovered["durable_replay_adopted"])
        self.assertEqual(1, self.runtime_sampler.calls)

    def test_cloud_origin_is_rejected_for_local_evidence_class(self) -> None:
        with self.assertRaisesRegex(
            FullFlowLiveProvisioningSmokeError,
            "credential-free HTTP origin",
        ):
            N4PublicationHttpClientConfig(
                base_url="https://example.invalid",
                bearer_token=N4_TOKEN,
            )


class LiveDigestProvisioningSmokeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.source = self.root / "source.mp4"
        self.source.write_bytes(_digest_mp4())
        self.frames = _digest_frames()
        self.plan = self.root / "digest-plan"
        freeze_n5_multimodal_digest_plan(
            self.source,
            self.frames,
            source_duration_seconds=8.0,
            object_id=OBJECT_ID,
            model_id=DIGEST_MODEL,
            output_dir=self.plan,
            plan_id="live-digest-plan-v1",
            jpeg_max_dimension=768,
            seed=17,
        )
        self.second_plan = self.root / "digest-plan-second"
        freeze_n5_multimodal_digest_plan(
            self.source,
            self.frames,
            source_duration_seconds=8.0,
            object_id=SECOND_OBJECT_ID,
            model_id=DIGEST_MODEL,
            output_dir=self.second_plan,
            plan_id="live-digest-plan-v2",
            jpeg_max_dimension=768,
            seed=17,
        )
        self.source_sha256 = _sha256(self.source.read_bytes())
        self.adapter = _OfflineVisionAdapter()

        def sampler(path, *, frame_count, jpeg_max_dimension):
            self.assertEqual(self.source_sha256, _sha256(path.read_bytes()))
            self.assertEqual(3, frame_count)
            self.assertEqual(768, jpeg_max_dimension)
            return list(self.frames), 8.0

        self.n5 = _RunningServer(
            create_n5_digest_http_server(
                self.root / "n5-digest-state",
                [self.plan, self.second_plan],
                vision_adapter=self.adapter,
                settings=N5DigestHTTPSettings(
                    bearer_token=DIGEST_TOKEN,
                    host="127.0.0.1",
                    port=0,
                    max_source_bytes=1024 * 1024,
                    max_json_bytes=64 * 1024,
                    max_result_bytes=1024 * 1024,
                ),
                sampler=sampler,
            )
        )
        self.n4_store_root = self.root / "n4-digest-state"
        self.n4 = _RunningServer(
            create_n4_publication_http_server(
                self.n4_store_root,
                settings=N4PublicationHTTPSettings(
                    bearer_token=N4_TOKEN,
                    host="127.0.0.1",
                    port=0,
                    max_request_bytes=1024 * 1024,
                    max_artifact_bytes=512 * 1024,
                ),
            )
        )

    def tearDown(self) -> None:
        self.n5.close()
        self.n4.close()
        self.temporary.cleanup()

    def arguments(self, output: Path) -> dict:
        return {
            "n5_digest_plan_dir": self.plan,
            "source_video_path": self.source,
            "n5_executor": HttpN5DigestMaterializationExecutor(
                N5DigestHttpClientConfig(
                    base_url=self.n5.base_url,
                    bearer_token=DIGEST_TOKEN,
                    timeout_seconds=5,
                    max_json_bytes=64 * 1024,
                    max_source_bytes=1024 * 1024,
                    max_result_bytes=1024 * 1024,
                )
            ),
            "n4_config": N4PublicationHttpClientConfig(
                base_url=self.n4.base_url,
                bearer_token=N4_TOKEN,
                timeout_seconds=5,
                max_json_bytes=1024 * 1024,
            ),
            "smoke_id": "local-live-digest-provision-v1",
            "request_id": "local-live-digest-request-v1",
            "publication_id": "local-live-digest-publication-v1",
            "package_id": "local-live-digest-n4-package-v1",
            "catalog_version": "local-live-digest-n4-catalog-v1",
            "expected_current_catalog_version": None,
            "output_dir": output,
        }

    def test_real_local_http_digest_chain_freezes_bound_receipt(self) -> None:
        output = self.root / "digest-receipt"
        result = run_n5_n4_live_multimodal_digest_provisioning_smoke(
            **self.arguments(output)
        )

        self.assertEqual("VERIFIED", result["status"])
        self.assertEqual("multimodal_digest", result["representation_id"])
        self.assertEqual(DIGEST_MODEL, result["model_id"])
        self.assertTrue(result["n5_fresh_digest_materialization_executed"])
        self.assertTrue(result["n4_fresh_publication_executed"])
        self.assertTrue(result["fresh_end_to_end_execution_observed"])
        self.assertFalse(result["durable_replay_adopted"])
        self.assertEqual(
            ["live-digest-plan-v1"],
            result["n4_access_plan_ids"],
        )
        self.assertEqual(
            "n5-materialization-plan-default",
            result["n4_access_plan_ids_source"],
        )
        self.assertEqual(1, self.adapter.calls)
        self.assertEqual(
            {CHECKSUMS_NAME, DIGEST_RECEIPT_NAME},
            {path.name for path in output.iterdir()},
        )
        receipt = json.loads((output / DIGEST_RECEIPT_NAME).read_text())
        serialized = json.dumps(receipt, sort_keys=True)
        self.assertNotIn(self.n5.base_url, serialized)
        self.assertNotIn(self.n4.base_url, serialized)
        self.assertNotIn(DIGEST_TOKEN, serialized)
        self.assertNotIn(N4_TOKEN, serialized)
        self.assertEqual(
            "not-observable-through-n5-digest-http-contract",
            receipt["external_network_call_status"],
        )
        self.assertFalse(
            receipt["semantic_model_call_authenticity_verified"]
        )
        self.assertTrue(receipt["n5_semantic_model_call_attested"])
        self.assertFalse(receipt["materialization_latency_measured"])
        self.assertFalse(receipt["monetary_cost_measured"])
        self.assertFalse(receipt["upcloud_used"])

        snapshot = N4DerivedRepresentationStore(
            self.n4_store_root
        ).current_snapshot()
        self.assertIsNotNone(snapshot)
        manifest = json.loads(
            (snapshot.package_dir / PACKAGE_MANIFEST_NAME).read_text()
        )
        artifact = (
            snapshot.package_dir
            / manifest["objects"][0]["artifact_package_path"]
        ).read_bytes()
        self.assertEqual(result["artifact_sha256"], _sha256(artifact))
        self.assertEqual(result["artifact_size_bytes"], len(artifact))
        self.assertEqual(
            ["live-digest-plan-v1"],
            manifest["objects"][0]["plan_ids"],
        )

    def test_digest_explicit_n4_access_plan_ids_are_bound(self) -> None:
        output = self.root / "digest-explicit-receipt"
        arguments = self.arguments(output)
        arguments["n4_access_plan_ids"] = ["D7", "D3"]
        result = run_n5_n4_live_multimodal_digest_provisioning_smoke(
            **arguments
        )

        self.assertEqual(["D3", "D7"], result["n4_access_plan_ids"])
        self.assertEqual("explicit", result["n4_access_plan_ids_source"])
        receipt = json.loads((output / DIGEST_RECEIPT_NAME).read_text())
        self.assertEqual(["D3", "D7"], receipt["n4_access_plan_ids"])
        snapshot = N4DerivedRepresentationStore(
            self.n4_store_root
        ).current_snapshot()
        manifest = json.loads(
            (snapshot.package_dir / PACKAGE_MANIFEST_NAME).read_text()
        )
        self.assertEqual(["D3", "D7"], manifest["objects"][0]["plan_ids"])
        verify_n5_n4_live_multimodal_digest_provisioning_smoke(
            output,
            n5_digest_plan_dir=self.plan,
            source_video_path=self.source,
            n4_access_plan_ids=["D3", "D7"],
        )

    def test_digest_access_ids_cannot_be_rehashed_outside_n4_request(
        self,
    ) -> None:
        output = self.root / "digest-access-request-binding"
        arguments = self.arguments(output)
        arguments["n4_access_plan_ids"] = ["D3", "D7"]
        run_n5_n4_live_multimodal_digest_provisioning_smoke(**arguments)

        receipt_path = output / DIGEST_RECEIPT_NAME
        value = json.loads(receipt_path.read_text())
        value["n4_access_plan_ids"] = ["D2"]
        del value["receipt_sha256"]
        value["receipt_sha256"] = _sha256(_canonical(value))
        payload = (
            json.dumps(value, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        receipt_path.write_bytes(payload)
        (output / CHECKSUMS_NAME).write_text(
            f"{_sha256(payload)}  {DIGEST_RECEIPT_NAME}\n",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(
            FullFlowLiveProvisioningSmokeError,
            "do not match the publication request",
        ):
            verify_n5_n4_live_multimodal_digest_provisioning_smoke(
                output,
                n5_digest_plan_dir=self.plan,
                source_video_path=self.source,
            )

    def test_two_digest_plans_share_one_n4_access_contract(self) -> None:
        access_plan_ids = ["D2", "D3", "D6", "D7"]
        first_arguments = self.arguments(self.root / "digest-first-object")
        first_arguments["n4_access_plan_ids"] = access_plan_ids
        first = run_n5_n4_live_multimodal_digest_provisioning_smoke(
            **first_arguments
        )

        second_arguments = self.arguments(
            self.root / "digest-second-object"
        )
        second_arguments.update({
            "n5_digest_plan_dir": self.second_plan,
            "smoke_id": "local-live-digest-provision-v2",
            "request_id": "local-live-digest-request-v2",
            "publication_id": "local-live-digest-publication-v2",
            "package_id": "local-live-digest-n4-package-v2",
            "catalog_version": "local-live-digest-n4-catalog-v2",
            "expected_current_catalog_version": first[
                "n4_committed_catalog_version"
            ],
            "n4_access_plan_ids": access_plan_ids,
        })
        second = run_n5_n4_live_multimodal_digest_provisioning_smoke(
            **second_arguments
        )

        self.assertEqual(access_plan_ids, second["n4_access_plan_ids"])
        self.assertEqual("explicit", second["n4_access_plan_ids_source"])
        snapshot = N4DerivedRepresentationStore(
            self.n4_store_root
        ).current_snapshot()
        manifest = json.loads(
            (snapshot.package_dir / PACKAGE_MANIFEST_NAME).read_text()
        )
        self.assertEqual(
            {OBJECT_ID, SECOND_OBJECT_ID},
            {row["object_id"] for row in manifest["objects"]},
        )
        self.assertEqual(
            {tuple(access_plan_ids)},
            {tuple(row["plan_ids"]) for row in manifest["objects"]},
        )
        receipts = [
            json.loads(
                (directory / DIGEST_RECEIPT_NAME).read_text()
            )
            for directory in (
                first_arguments["output_dir"],
                second_arguments["output_dir"],
            )
        ]
        self.assertEqual(
            {"live-digest-plan-v1", "live-digest-plan-v2"},
            {receipt["n5_digest_plan_id"] for receipt in receipts},
        )
        self.assertTrue(all(
            receipt["n4_access_plan_ids"] == access_plan_ids
            for receipt in receipts
        ))
        self.assertEqual(2, self.adapter.calls)

    def test_invalid_digest_access_contract_fails_before_model_call(self) -> None:
        arguments = self.arguments(self.root / "invalid-access-receipt")
        arguments["n4_access_plan_ids"] = ["D3", "D3"]
        with self.assertRaisesRegex(
            FullFlowLiveProvisioningSmokeError,
            "duplicate",
        ):
            run_n5_n4_live_multimodal_digest_provisioning_smoke(
                **arguments
            )
        self.assertEqual(0, self.adapter.calls)
        self.assertFalse((self.root / "invalid-access-receipt").exists())

    def test_cumulative_digest_requires_access_contract_before_model_call(
        self,
    ) -> None:
        access_plan_ids = ["D2", "D3", "D6", "D7"]
        first_arguments = self.arguments(self.root / "cumulative-first")
        first_arguments["n4_access_plan_ids"] = access_plan_ids
        first = run_n5_n4_live_multimodal_digest_provisioning_smoke(
            **first_arguments
        )

        output = self.root / "cumulative-missing-access-contract"
        second_arguments = self.arguments(output)
        second_arguments.update({
            "n5_digest_plan_dir": self.second_plan,
            "smoke_id": "cumulative-missing-access-contract",
            "request_id": "cumulative-missing-access-contract-request",
            "publication_id": "cumulative-missing-access-contract-publication",
            "package_id": "cumulative-missing-access-contract-package",
            "catalog_version": "cumulative-missing-access-contract-catalog",
            "expected_current_catalog_version": first[
                "n4_committed_catalog_version"
            ],
        })
        with self.assertRaisesRegex(
            FullFlowLiveProvisioningSmokeError,
            "cumulative N4 publication requires explicit",
        ):
            run_n5_n4_live_multimodal_digest_provisioning_smoke(
                **second_arguments
            )

        self.assertEqual(1, self.adapter.calls)
        self.assertFalse(output.exists())

    def test_legacy_digest_receipt_remains_verifiable(self) -> None:
        output = self.root / "legacy-digest-receipt"
        run_n5_n4_live_multimodal_digest_provisioning_smoke(
            **self.arguments(output)
        )
        receipt_path = output / DIGEST_RECEIPT_NAME
        value = json.loads(receipt_path.read_text())
        value["schema_version"] = (
            "pathfinder.local-n5-n4-live-digest-provisioning-smoke/"
            "v1alpha1"
        )
        del value["n4_access_plan_ids"]
        del value["n4_access_plan_ids_source"]
        del value["n4_package_id"]
        del value["n4_publication_request_sha256"]
        del value["receipt_sha256"]
        value["receipt_sha256"] = _sha256(_canonical(value))
        payload = (
            json.dumps(value, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        receipt_path.write_bytes(payload)
        (output / CHECKSUMS_NAME).write_text(
            f"{_sha256(payload)}  {DIGEST_RECEIPT_NAME}\n",
            encoding="utf-8",
        )

        verified = verify_n5_n4_live_multimodal_digest_provisioning_smoke(
            output,
            n5_digest_plan_dir=self.plan,
            source_video_path=self.source,
        )
        self.assertEqual(
            ["live-digest-plan-v1"],
            verified["n4_access_plan_ids"],
        )
        self.assertEqual(
            "legacy-schema-inference",
            verified["n4_access_plan_ids_source"],
        )

    def test_digest_replay_is_adopted_without_fresh_claim(self) -> None:
        run_n5_n4_live_multimodal_digest_provisioning_smoke(
            **self.arguments(self.root / "digest-first")
        )
        result = run_n5_n4_live_multimodal_digest_provisioning_smoke(
            **self.arguments(self.root / "digest-replay")
        )
        self.assertFalse(result["n5_fresh_digest_materialization_executed"])
        self.assertFalse(result["n4_fresh_publication_executed"])
        self.assertFalse(result["fresh_end_to_end_execution_observed"])
        self.assertTrue(result["durable_replay_adopted"])
        self.assertEqual(1, self.adapter.calls)

    def test_wrong_digest_credential_fails_before_model_or_receipt(self) -> None:
        arguments = self.arguments(self.root / "digest-unauthorized")
        arguments["n5_executor"] = HttpN5DigestMaterializationExecutor(
            N5DigestHttpClientConfig(
                base_url=self.n5.base_url,
                bearer_token="wrong-runtime-only-token",
                timeout_seconds=5,
            )
        )
        with self.assertRaisesRegex(
            FullFlowLiveProvisioningSmokeError,
            "HTTP 401",
        ):
            run_n5_n4_live_multimodal_digest_provisioning_smoke(**arguments)
        self.assertEqual(0, self.adapter.calls)
        self.assertFalse((self.root / "digest-unauthorized").exists())

    def test_digest_publish_retry_adopts_materialization_replay(self) -> None:
        arguments = self.arguments(self.root / "digest-publish-failure")
        arguments["n4_config"] = N4PublicationHttpClientConfig(
            base_url=self.n4.base_url,
            bearer_token="wrong-runtime-only-token",
            timeout_seconds=5,
            max_json_bytes=1024 * 1024,
        )
        with self.assertRaisesRegex(
            FullFlowLiveProvisioningSmokeError,
            "HTTP 401",
        ):
            run_n5_n4_live_multimodal_digest_provisioning_smoke(**arguments)
        self.assertEqual(1, self.adapter.calls)
        self.assertFalse((self.root / "digest-publish-failure").exists())

        recovered = run_n5_n4_live_multimodal_digest_provisioning_smoke(
            **self.arguments(self.root / "digest-recovered")
        )
        self.assertFalse(
            recovered["n5_fresh_digest_materialization_executed"]
        )
        self.assertTrue(recovered["n4_fresh_publication_executed"])
        self.assertTrue(recovered["durable_replay_adopted"])
        self.assertEqual(1, self.adapter.calls)

    def test_cumulative_digest_retry_reuses_n5_result_and_repairs_n4_binding(
        self,
    ) -> None:
        access_plan_ids = ["D2", "D3", "D6", "D7"]
        first_arguments = self.arguments(self.root / "binding-retry-first")
        first_arguments["n4_access_plan_ids"] = access_plan_ids
        first = run_n5_n4_live_multimodal_digest_provisioning_smoke(
            **first_arguments
        )

        failed_output = self.root / "binding-retry-failed"
        second_arguments = self.arguments(failed_output)
        second_arguments.update({
            "n5_digest_plan_dir": self.second_plan,
            "smoke_id": "binding-retry-second",
            "request_id": "binding-retry-second-request",
            "publication_id": "binding-retry-second-publication",
            "package_id": "binding-retry-second-package",
            "catalog_version": "binding-retry-second-catalog",
            "expected_current_catalog_version": first[
                "n4_committed_catalog_version"
            ],
            "n4_access_plan_ids": access_plan_ids,
            "n4_config": N4PublicationHttpClientConfig(
                base_url=self.n4.base_url,
                bearer_token="wrong-runtime-only-token",
                timeout_seconds=5,
                max_json_bytes=1024 * 1024,
            ),
        })
        with self.assertRaisesRegex(
            FullFlowLiveProvisioningSmokeError,
            "HTTP 401",
        ):
            run_n5_n4_live_multimodal_digest_provisioning_smoke(
                **second_arguments
            )
        self.assertEqual(2, self.adapter.calls)
        self.assertFalse(failed_output.exists())

        recovered_output = self.root / "binding-retry-recovered"
        recovered_arguments = dict(second_arguments)
        recovered_arguments.update({
            "output_dir": recovered_output,
            "n4_config": N4PublicationHttpClientConfig(
                base_url=self.n4.base_url,
                bearer_token=N4_TOKEN,
                timeout_seconds=5,
                max_json_bytes=1024 * 1024,
            ),
        })
        recovered = run_n5_n4_live_multimodal_digest_provisioning_smoke(
            **recovered_arguments
        )

        self.assertFalse(
            recovered["n5_fresh_digest_materialization_executed"]
        )
        self.assertTrue(recovered["n4_fresh_publication_executed"])
        self.assertTrue(recovered["durable_replay_adopted"])
        self.assertEqual(access_plan_ids, recovered["n4_access_plan_ids"])
        self.assertEqual("explicit", recovered["n4_access_plan_ids_source"])
        self.assertEqual(2, self.adapter.calls)

        snapshot = N4DerivedRepresentationStore(
            self.n4_store_root
        ).current_snapshot()
        manifest = json.loads(
            (snapshot.package_dir / PACKAGE_MANIFEST_NAME).read_text()
        )
        self.assertEqual(
            {OBJECT_ID, SECOND_OBJECT_ID},
            {row["object_id"] for row in manifest["objects"]},
        )
        self.assertEqual(
            {tuple(access_plan_ids)},
            {tuple(row["plan_ids"]) for row in manifest["objects"]},
        )

    def test_digest_receipt_is_source_bound_after_outer_rehash(self) -> None:
        output = self.root / "digest-receipt"
        run_n5_n4_live_multimodal_digest_provisioning_smoke(
            **self.arguments(output)
        )
        receipt_path = output / DIGEST_RECEIPT_NAME
        value = json.loads(receipt_path.read_text())
        value["n5_digest_plan_sha256"] = "f" * 64
        del value["receipt_sha256"]
        value["receipt_sha256"] = _sha256(_canonical(value))
        payload = (
            json.dumps(value, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        receipt_path.write_bytes(payload)
        (output / CHECKSUMS_NAME).write_text(
            f"{_sha256(payload)}  {DIGEST_RECEIPT_NAME}\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            FullFlowLiveProvisioningSmokeError,
            "binding",
        ):
            verify_n5_n4_live_multimodal_digest_provisioning_smoke(
                output,
                n5_digest_plan_dir=self.plan,
                source_video_path=self.source,
            )

    def test_digest_cloud_origin_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            FullFlowLiveProvisioningSmokeError,
            "credential-free HTTP origin",
        ):
            N5DigestHttpClientConfig(
                base_url="https://example.invalid",
                bearer_token=DIGEST_TOKEN,
            )


if __name__ == "__main__":
    unittest.main()
