from __future__ import annotations

import base64
import hashlib
import json
import struct
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

from pathfinder.data_agent_client import (
    DataAgentClientSettings,
    HttpDataAgentClient,
)
from pathfinder.data_agent_server import (
    DataAgentServerSettings,
    create_data_agent_http_server,
)
from pathfinder.frame_bundle import (
    FRAME_BUNDLE_SCHEMA_VERSION,
    OBJECT_MANIFEST_NAME,
    deterministic_frame_bundle_tar,
)
from pathfinder.simulator.full_flow_artifact_preflight import (
    FullFlowArtifactPreflightError,
    HttpDataAgentArtifactAvailabilityProbe,
    OBSERVATIONS_NAME,
    preflight_full_flow_semantic_artifacts,
    preflight_full_flow_semantic_artifacts_over_http,
)
from pathfinder.simulator.n4_derived_data_plane import (
    N4ArtifactProvenance,
    N4DerivedArtifactInput,
    build_n4_derived_data_package,
)
from pathfinder.simulator.raw_cold_data_plane import (
    CHECKSUMS_NAME as N3_CHECKSUMS_NAME,
    PACKAGE_MANIFEST_NAME as N3_PACKAGE_MANIFEST_NAME,
    RawColdObjectBinding,
    build_raw_cold_data_plane_package,
)


OBJECT_ID = "nextqa-val-0000000001"
RAW_PLAN = "preflight-raw-plan"
BUNDLE_PLAN = "preflight-bundle-plan"
DIGEST_PLAN = "preflight-digest-plan"
N3_CATALOG = "preflight-n3-catalog-v1"
N4_CATALOG = "preflight-n4-catalog-v1"

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


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _mp4_bytes() -> bytes:
    compatible = b"isom" + b"iso2" + b"mp41"
    ftyp_payload = b"isom" + struct.pack(">I", 512) + compatible
    ftyp = struct.pack(">I", 8 + len(ftyp_payload)) + b"ftyp" + ftyp_payload
    body = bytes(range(96))
    return ftyp + struct.pack(">I", 8 + len(body)) + b"mdat" + body


def _bundle() -> bytes:
    jpeg = base64.b64decode(_TEST_JPEG_BASE64, validate=True)
    row = {
        "frame_index": 0,
        "timestamp_seconds": 0.5,
        "width": 2,
        "height": 2,
        "path": "frames/000.jpg",
        "jpeg_size_bytes": len(jpeg),
        "jpeg_sha256": _sha256(jpeg),
    }
    manifest = {
        "schema_version": FRAME_BUNDLE_SCHEMA_VERSION,
        "representation_id": "sampled_frame_bundle",
        "object_id": OBJECT_ID,
        "source_video_id": "0000000001",
        "source_video_filename": "0000000001.mp4",
        "source_video_size_bytes": 128,
        "source_video_sha256": "a" * 64,
        "source_duration_seconds": 1.0,
        "sampling": {
            "method": "uniform-midpoint",
            "frame_count": 1,
            "jpeg_max_dimension": 768,
            "jpeg_quality": 82,
            "jpeg_optimize": True,
        },
        "source_frame_descriptions": {
            "representation_id": "sampled_frames",
            "path": f"{OBJECT_ID}/sampled_frames.json",
            "sha256": "b" * 64,
        },
        "generation_manifest_sha256": "c" * 64,
        "frames": [row],
        "frame_count": 1,
        "total_jpeg_bytes": len(jpeg),
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
    return deterministic_frame_bundle_tar([
        (OBJECT_MANIFEST_NAME, _json_bytes(manifest)),
        ("frames/000.jpg", jpeg),
    ])


def _write_admission(
    root: Path,
    raw: bytes,
    bundle: bytes,
    digest: bytes,
) -> Path:
    source = root / "admission"
    source.mkdir()
    identities = [
        ("raw_video", raw, N3_CATALOG),
        ("sampled_frame_bundle", bundle, N4_CATALOG),
        ("multimodal_digest", digest, N4_CATALOG),
    ]
    bindings = [
        {
            "logical_object_id": "logical-object-1",
            "artifact_object_id": OBJECT_ID,
            "representation_id": representation,
            "representation_binding": {
                "representation_id": representation,
                "artifact_sha256": _sha256(payload),
                "artifact_size_bytes": len(payload),
                "object_catalog_version": catalog,
            },
        }
        for representation, payload, catalog in identities
    ]
    trials = [
        {
            "trial_key": f"scenario|workload|D{index % 8}|r{index // 32:04d}",
            "representation_identities": bindings,
        }
        for index in range(64)
    ]
    admission = {
        "schema_version": (
            "pathfinder.full-flow-semantic-execution-admission/v1alpha1"
        ),
        "status": "BLOCKED_MISSING_ROUTE_RUNTIME_ADAPTERS",
        "admission_id": "http-artifact-preflight-v1",
    }
    admission["admission_sha256"] = _sha256(_canonical(admission))
    documents = {
        "semantic-execution-admission.json": _json_bytes(admission),
        "semantic-execution-runtime-gaps.json": b"{}\n",
        "semantic-execution-smokes.jsonl": b"{}\n",
        "semantic-execution-stages.jsonl": b"{}\n",
        "semantic-execution-trials.jsonl": b"".join(
            _canonical(row) + b"\n" for row in trials
        ),
    }
    for name, payload in documents.items():
        (source / name).write_bytes(payload)
    (source / "SHA256SUMS").write_bytes(b"".join(
        f"{_sha256(documents[name])}  {name}\n".encode("utf-8")
        for name in sorted(documents)
    ))
    return source


def _stop_server(server: object, thread: threading.Thread) -> None:
    try:
        server.shutdown()  # type: ignore[attr-defined]
    finally:
        server.server_close()  # type: ignore[attr-defined]
        thread.join(timeout=5)


class HttpArtifactPreflightTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.raw = _mp4_bytes()
        self.bundle = _bundle()
        self.digest = b"Two musicians perform on a stage.\n"
        raw_path = self.root / "source.mp4"
        raw_path.write_bytes(self.raw)
        self.n3 = self.root / "n3-package"
        build_raw_cold_data_plane_package(
            [RawColdObjectBinding(
                object_id=OBJECT_ID,
                artifact_path=raw_path,
                catalog_version=N3_CATALOG,
                plan_ids=(RAW_PLAN,),
                dataset_id="nextqa",
                dataset_revision="test-v1",
                source_object_id="0000000001",
                artifact_sha256=_sha256(self.raw),
                artifact_size_bytes=len(self.raw),
            )],
            output_dir=self.n3,
            package_id="preflight-n3-v1",
        )
        provenance = N4ArtifactProvenance(
            producer_node_id="N5",
            publication_source_id="preflight-publication-v1",
            source_representation_id="raw_video",
            source_content_sha256=_sha256(self.raw),
            derivation_id="preflight-derivation-v1",
            derivation_sha256="d" * 64,
        )
        self.n4 = self.root / "n4-package"
        build_n4_derived_data_package(
            [
                N4DerivedArtifactInput(
                    object_id=OBJECT_ID,
                    representation_id="sampled_frame_bundle",
                    artifact_bytes=self.bundle,
                    plan_ids=(BUNDLE_PLAN,),
                    provenance=provenance,
                ),
                N4DerivedArtifactInput(
                    object_id=OBJECT_ID,
                    representation_id="multimodal_digest",
                    artifact_bytes=self.digest,
                    plan_ids=(DIGEST_PLAN,),
                    provenance=provenance,
                ),
            ],
            output_dir=self.n4,
            package_id="preflight-n4-v1",
            catalog_version=N4_CATALOG,
        )
        self.admission = _write_admission(
            self.root,
            self.raw,
            self.bundle,
            self.digest,
        )
        self.n3_token = "n3-loopback-control-token"
        self.n4_token = "n4-loopback-control-token"
        self.n3_url = self._start_server(
            self.n3,
            self.n3_token,
            "n3-loopback-artifact-secret",
            "n3-operations.sqlite3",
        )
        self.n4_url = self._start_server(
            self.n4,
            self.n4_token,
            "n4-loopback-artifact-secret",
            "n4-operations.sqlite3",
        )

    def _start_server(
        self,
        package: Path,
        token: str,
        artifact_secret: str,
        database_name: str,
    ) -> str:
        server = create_data_agent_http_server(
            manifest_path=package / "config" / "data-agent-manifest.json",
            operation_db=self.root / database_name,
            settings=DataAgentServerSettings(
                host="127.0.0.1",
                port=0,
                token=token,
                artifact_secret=artifact_secret,
            ),
        )
        thread = threading.Thread(
            target=server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )
        thread.start()
        self.addCleanup(_stop_server, server, thread)
        host, port = server.server_address[:2]
        return f"http://{host}:{port}"

    def _client(self, url: str, token: str) -> HttpDataAgentClient:
        return HttpDataAgentClient(DataAgentClientSettings(
            base_url=url,
            token=token,
            max_retries=0,
            max_artifact_bytes=2 * 1024 * 1024,
        ))

    def test_authenticated_loopback_preflight_fetches_all_content(self) -> None:
        output = self.root / "preflight"
        result = preflight_full_flow_semantic_artifacts_over_http(
            self.admission,
            self.n3,
            self.n4,
            n3_base_url=self.n3_url,
            n4_base_url=self.n4_url,
            n3_token=self.n3_token,
            n4_token=self.n4_token,
            preflight_id="http-preflight-v1",
            output_dir=output,
            max_retries=0,
            max_artifact_bytes=2 * 1024 * 1024,
        )

        self.assertEqual("VERIFIED", result["status"])
        self.assertTrue(result["data_agent_package_bindings_checked"])
        rows = [
            json.loads(line)
            for line in (output / OBSERVATIONS_NAME).read_text().splitlines()
        ]
        by_representation = {row["representation_id"]: row for row in rows}
        self.assertEqual(RAW_PLAN, by_representation["raw_video"]["plan_id"])
        self.assertEqual(
            BUNDLE_PLAN,
            by_representation["sampled_frame_bundle"]["plan_id"],
        )
        self.assertEqual(
            DIGEST_PLAN,
            by_representation["multimodal_digest"]["plan_id"],
        )
        self.assertEqual(
            0,
            by_representation["multimodal_digest"][
                "artifact_full_download_count"
            ],
        )
        self.assertEqual(
            1,
            by_representation["raw_video"]["artifact_full_download_count"],
        )
        self.assertEqual(
            len(self.bundle),
            by_representation["sampled_frame_bundle"]["artifact_bytes_sent"],
        )
        combined = b"".join(path.read_bytes() for path in output.iterdir())
        for forbidden in (
            self.n3_url.encode(),
            self.n4_url.encode(),
            self.n3_token.encode(),
            self.n4_token.encode(),
            self.raw,
            self.bundle,
            self.digest,
        ):
            self.assertNotIn(forbidden, combined)

    def test_wrong_bearer_token_fails_without_publishing_output(self) -> None:
        output = self.root / "wrong-token"
        with self.assertRaisesRegex(Exception, "HTTP 401"):
            preflight_full_flow_semantic_artifacts_over_http(
                self.admission,
                self.n3,
                self.n4,
                n3_base_url=self.n3_url,
                n4_base_url=self.n4_url,
                n3_token="wrong-control-token",
                n4_token=self.n4_token,
                preflight_id="wrong-token-v1",
                output_dir=output,
                max_retries=0,
                max_artifact_bytes=2 * 1024 * 1024,
            )
        self.assertFalse(output.exists())

    def test_endpoint_without_bearer_enforcement_fails_closed(self) -> None:
        server = create_data_agent_http_server(
            manifest_path=(
                self.n3 / "config" / "data-agent-manifest.json"
            ),
            operation_db=self.root / "unauthenticated.sqlite3",
            settings=DataAgentServerSettings(
                host="127.0.0.1",
                port=0,
                token=None,
                artifact_secret="unauthenticated-artifact-secret",
            ),
        )
        thread = threading.Thread(
            target=server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )
        thread.start()
        self.addCleanup(_stop_server, server, thread)
        host, port = server.server_address[:2]
        client = self._client(f"http://{host}:{port}", "configured-token")
        probe = HttpDataAgentArtifactAvailabilityProbe(
            n3_client=client,
            n4_client=self._client(self.n4_url, self.n4_token),
            n3_package_dir=self.n3,
            n4_package_dir=self.n4,
            preflight_id="unenforced-auth-v1",
        )
        with self.assertRaisesRegex(
            FullFlowArtifactPreflightError,
            "accepted an invalid bearer",
        ):
            preflight_full_flow_semantic_artifacts(
                self.admission,
                preflight_id="unenforced-auth-v1",
                probe=probe,
                output_dir=self.root / "unenforced-auth",
            )

    def test_wrong_media_type_fails_closed(self) -> None:
        n3_client = self._client(self.n3_url, self.n3_token)
        n4_client = self._client(self.n4_url, self.n4_token)
        probe = HttpDataAgentArtifactAvailabilityProbe(
            n3_client=n3_client,
            n4_client=n4_client,
            n3_package_dir=self.n3,
            n4_package_dir=self.n4,
            preflight_id="wrong-media-v1",
        )
        original = n3_client.fetch_binary_artifact

        def wrong_media(*args, **kwargs):
            return replace(
                original(*args, **kwargs),
                media_type="application/octet-stream",
            )

        output = self.root / "wrong-media"
        with mock.patch.object(
            n3_client,
            "fetch_binary_artifact",
            side_effect=wrong_media,
        ):
            with self.assertRaisesRegex(
                FullFlowArtifactPreflightError,
                "media type",
            ):
                preflight_full_flow_semantic_artifacts(
                    self.admission,
                    preflight_id="wrong-media-v1",
                    probe=probe,
                    output_dir=output,
                )
        self.assertFalse(output.exists())

    def test_plan_binding_tamper_is_rejected(self) -> None:
        manifest_path = self.n3 / N3_PACKAGE_MANIFEST_NAME
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["objects"][0]["plan_ids"] = ["wrong-plan"]
        manifest_path.write_bytes(_json_bytes(manifest))
        names = sorted(
            path.relative_to(self.n3).as_posix()
            for path in self.n3.rglob("*")
            if path.is_file() and path.name != N3_CHECKSUMS_NAME
        )
        (self.n3 / N3_CHECKSUMS_NAME).write_bytes(b"".join(
            f"{_sha256((self.n3 / name).read_bytes())}  {name}\n".encode()
            for name in names
        ))

        with self.assertRaisesRegex(
            FullFlowArtifactPreflightError,
            "package verification failed",
        ):
            HttpDataAgentArtifactAvailabilityProbe(
                n3_client=self._client(self.n3_url, self.n3_token),
                n4_client=self._client(self.n4_url, self.n4_token),
                n3_package_dir=self.n3,
                n4_package_dir=self.n4,
                preflight_id="wrong-plan-v1",
            )

    def test_package_byte_tamper_is_rejected(self) -> None:
        artifact = next((self.n4 / "artifacts").rglob("*.txt"))
        artifact.write_bytes(artifact.read_bytes() + b"tamper")
        with self.assertRaisesRegex(
            FullFlowArtifactPreflightError,
            "package verification failed",
        ):
            HttpDataAgentArtifactAvailabilityProbe(
                n3_client=self._client(self.n3_url, self.n3_token),
                n4_client=self._client(self.n4_url, self.n4_token),
                n3_package_dir=self.n3,
                n4_package_dir=self.n4,
                preflight_id="tampered-package-v1",
            )


if __name__ == "__main__":
    unittest.main()
