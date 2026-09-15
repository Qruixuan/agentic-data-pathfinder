from __future__ import annotations

import hashlib
import json
import struct
import tempfile
import threading
import unittest
from pathlib import Path

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
from pathfinder.simulator.raw_cold_data_plane import (
    ARTIFACT_MEDIA_TYPE,
    CHECKSUMS_NAME,
    DATA_AGENT_MANIFEST_PATH,
    OBJECT_CATALOG_PATH,
    PACKAGE_MANIFEST_NAME,
    RAW_COLD_BINDINGS_SCHEMA_VERSION,
    REPRESENTATION_ID,
    SOURCE_LOCATION,
    RawColdDataPlaneError,
    RawColdObjectBinding,
    build_raw_cold_data_plane_package,
    build_raw_cold_data_plane_package_from_manifest,
    verify_raw_cold_data_plane_package,
)


CATALOG_VERSION = "nextqa-raw-catalog-v1"
DATASET_ID = "nextqa"
DATASET_REVISION = "restricted-pilot-v0.1"


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _mp4_bytes(seed: int, payload_bytes: int = 64) -> bytes:
    compatible = b"isom" + b"iso2" + b"mp41"
    ftyp_payload = b"isom" + struct.pack(">I", 512) + compatible
    ftyp = struct.pack(">I", 8 + len(ftyp_payload)) + b"ftyp" + ftyp_payload
    body = bytes((seed + index) % 256 for index in range(payload_bytes))
    mdat = struct.pack(">I", 8 + len(body)) + b"mdat" + body
    return ftyp + mdat


def _write_json(path: Path, value: object) -> None:
    path.write_bytes(
        (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    )


def _restamp(root: Path) -> None:
    paths = sorted(
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() and path.name != CHECKSUMS_NAME
    )
    (root / CHECKSUMS_NAME).write_bytes(
        "".join(
            f"{_sha256((root / path).read_bytes())}  {path}\n"
            for path in paths
        ).encode("utf-8"),
    )


def _tree(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }


class RawColdDataPlaneTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.raw_a = _mp4_bytes(7)
        self.raw_b = _mp4_bytes(19, 91)
        self.source_a = self.root / "operator-source-a.mp4"
        self.source_b = self.root / "operator-source-b.mp4"
        self.source_a.write_bytes(self.raw_a)
        self.source_b.write_bytes(self.raw_b)

    def _binding(
        self,
        object_id: str = "nextqa-val-4010069381",
        source: Path | None = None,
        *,
        catalog_version: str = CATALOG_VERSION,
    ) -> RawColdObjectBinding:
        path = source or self.source_a
        raw = path.read_bytes()
        return RawColdObjectBinding(
            object_id=object_id,
            artifact_path=path,
            catalog_version=catalog_version,
            plan_ids=("D0", "D1", "matrix-v1|W1|D0|r0000"),
            dataset_id=DATASET_ID,
            dataset_revision=DATASET_REVISION,
            source_object_id=object_id.removeprefix("nextqa-val-"),
            artifact_sha256=_sha256(raw),
            artifact_size_bytes=len(raw),
        )

    def test_builds_portable_n3_data_agent_package(self) -> None:
        output = self.root / "package"
        result = build_raw_cold_data_plane_package(
            [self._binding()],
            output_dir=output,
            package_id="n3-raw-cold-v1",
        )

        self.assertEqual("VERIFIED_RAW_COLD_DATA_PLANE", result["status"])
        self.assertEqual("N3", result["source_node_id"])
        report = json.loads((output / PACKAGE_MANIFEST_NAME).read_text())
        self.assertEqual("N3", report["route"]["source_node_id"])
        self.assertEqual("origin-cold", report["route"]["source_location"])
        self.assertEqual(REPRESENTATION_ID, report["route"]["representation_id"])
        self.assertTrue(report["route"]["authoritative_copy"])
        self.assertTrue(report["deployment_binding_required"])
        self.assertFalse(report["runtime_execution_verified"])

        manifest = load_data_agent_manifest(output / DATA_AGENT_MANIFEST_PATH)
        self.assertEqual("N3", manifest.node_id)
        resolved = manifest.resolve(
            plan_id="D0",
            object_id="nextqa-val-4010069381",
            representation_id=REPRESENTATION_ID,
            requested_location=SOURCE_LOCATION,
        )
        self.assertEqual(self.raw_a, resolved.path.read_bytes())
        self.assertEqual(ARTIFACT_MEDIA_TYPE, resolved.media_type)
        self.assertFalse(resolved.cache_hit)
        self.assertEqual(0.0, resolved.realized_cost)

    def test_operator_manifest_builds_without_persisting_source_path(self) -> None:
        manifest = self.root / "bindings.json"
        _write_json(manifest, {
            "schema_version": RAW_COLD_BINDINGS_SCHEMA_VERSION,
            "package_id": "n3-manifest-v1",
            "catalog_version": CATALOG_VERSION,
            "plan_ids": ["D0", "D1"],
            "dataset_id": DATASET_ID,
            "dataset_revision": DATASET_REVISION,
            "objects": [{
                "object_id": "nextqa-val-4010069381",
                "artifact_path": self.source_a.name,
                "source_object_id": "4010069381",
                "artifact_sha256": _sha256(self.raw_a),
                "artifact_size_bytes": len(self.raw_a),
            }],
            "credentials_recorded": False,
        })
        output = self.root / "manifest-package"
        result = build_raw_cold_data_plane_package_from_manifest(
            manifest,
            output_dir=output,
        )

        self.assertEqual("VERIFIED_RAW_COLD_DATA_PLANE", result["status"])
        combined = b"".join(
            path.read_bytes() for path in output.rglob("*") if path.is_file()
        ).decode("utf-8", errors="ignore")
        self.assertNotIn(str(self.root), combined)
        self.assertNotIn(self.source_a.name, combined)

    def test_freezes_exact_identity_and_explicit_provenance(self) -> None:
        output = self.root / "package"
        build_raw_cold_data_plane_package(
            [self._binding()],
            output_dir=output,
            package_id="identity-v1",
        )
        report = json.loads((output / PACKAGE_MANIFEST_NAME).read_text())
        row = report["objects"][0]
        provenance = row["provenance"]

        self.assertEqual(_sha256(self.raw_a), row["artifact_sha256"])
        self.assertEqual(len(self.raw_a), row["artifact_size_bytes"])
        self.assertEqual(row["artifact_sha256"], provenance["source_artifact_sha256"])
        self.assertEqual(
            row["artifact_size_bytes"],
            provenance["source_artifact_size_bytes"],
        )
        self.assertEqual(DATASET_ID, provenance["dataset_id"])
        self.assertEqual(DATASET_REVISION, provenance["dataset_revision"])
        frozen = output / row["artifact_package_path"]
        self.assertEqual(self.raw_a, frozen.read_bytes())

    def test_documents_have_no_endpoint_credential_or_absolute_path(self) -> None:
        output = self.root / "package"
        build_raw_cold_data_plane_package(
            [self._binding()],
            output_dir=output,
            package_id="portable-v1",
        )

        for relative in (
            PACKAGE_MANIFEST_NAME,
            DATA_AGENT_MANIFEST_PATH,
            OBJECT_CATALOG_PATH,
        ):
            text = (output / relative).read_text(encoding="utf-8")
            self.assertNotIn("://", text)
            self.assertNotIn(str(self.root), text)
            self.assertNotIn("api_key", text.casefold())
            self.assertNotIn("password", text.casefold())
            self.assertNotIn("secret", text.casefold())
            self.assertNotIn("token", text.casefold())

    def test_package_is_byte_deterministic_across_binding_order(self) -> None:
        bindings = [
            self._binding(),
            self._binding("nextqa-val-2435100235", self.source_b),
        ]
        first = self.root / "first"
        second = self.root / "second"
        build_raw_cold_data_plane_package(
            list(reversed(bindings)),
            output_dir=first,
            package_id="deterministic-v1",
        )
        build_raw_cold_data_plane_package(
            bindings,
            output_dir=second,
            package_id="deterministic-v1",
        )
        self.assertEqual(_tree(first), _tree(second))

    def test_data_agent_serves_exact_raw_bytes_over_standard_contract(self) -> None:
        output = self.root / "package"
        build_raw_cold_data_plane_package(
            [self._binding()],
            output_dir=output,
            package_id="served-v1",
        )
        server = create_data_agent_http_server(
            manifest_path=output / DATA_AGENT_MANIFEST_PATH,
            operation_db=self.root / "operations.sqlite3",
            settings=DataAgentServerSettings(
                host="127.0.0.1",
                port=0,
                token="test-control-token",
                artifact_secret="test-artifact-secret",
            ),
        )
        thread = threading.Thread(
            target=server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )
        thread.start()
        self.addCleanup(self._stop_server, server, thread)
        host, port = server.server_address[:2]
        client = HttpDataAgentClient(
            DataAgentClientSettings(
                base_url=f"http://{host}:{port}",
                token="test-control-token",
                max_retries=0,
                max_artifact_bytes=1024 * 1024,
            )
        )
        request = DataAgentAccessRequest(
            access_id="n3-raw-access-v1",
            session_id="n3-raw-session-v1",
            trial_id="n3-raw-trial-v1",
            plan_id="D0",
            plan_epoch=0,
            task_class_id="video_qa",
            representation_id=REPRESENTATION_ID,
            event_index=0,
            latency_multiplier=1.0,
            binding={"location": SOURCE_LOCATION},
            object_id="nextqa-val-4010069381",
        )

        artifact = client.fetch_binary_artifact(
            request,
            allowed_media_types={ARTIFACT_MEDIA_TYPE},
        )

        self.assertEqual(self.raw_a, artifact.data)
        self.assertEqual(_sha256(self.raw_a), artifact.sha256)
        self.assertEqual("nextqa-val-4010069381", artifact.object_id)
        self.assertEqual(CATALOG_VERSION, artifact.object_catalog_version)
        self.assertEqual(SOURCE_LOCATION, artifact.location)
        telemetry = client.get_access_telemetry(
            request.access_id,
            wait_for_quiescence=True,
        )
        self.assertTrue(telemetry.telemetry_complete)
        self.assertEqual(1, telemetry.full_download_count)
        self.assertEqual(len(self.raw_a), telemetry.bytes_sent)

    @staticmethod
    def _stop_server(server: object, thread: threading.Thread) -> None:
        try:
            server.shutdown()  # type: ignore[attr-defined]
        finally:
            server.server_close()  # type: ignore[attr-defined]
            thread.join(timeout=5)

    def test_wrong_expected_digest_fails_without_output(self) -> None:
        binding = self._binding()
        bad = RawColdObjectBinding(
            **{**binding.__dict__, "artifact_sha256": "f" * 64}
        )
        output = self.root / "package"
        with self.assertRaisesRegex(RawColdDataPlaneError, "SHA-256 mismatch"):
            build_raw_cold_data_plane_package(
                [bad],
                output_dir=output,
                package_id="wrong-digest-v1",
            )
        self.assertFalse(output.exists())

    def test_invalid_mp4_is_rejected_without_output(self) -> None:
        source = self.root / "not-video.mp4"
        source.write_bytes(b"this is not an MP4 artifact")
        binding = self._binding(source=source)
        output = self.root / "package"
        with self.assertRaisesRegex(RawColdDataPlaneError, "ftyp"):
            build_raw_cold_data_plane_package(
                [binding],
                output_dir=output,
                package_id="bad-mp4-v1",
            )
        self.assertFalse(output.exists())

    def test_duplicate_objects_and_mixed_catalogs_are_rejected(self) -> None:
        with self.assertRaisesRegex(RawColdDataPlaneError, "duplicate object"):
            build_raw_cold_data_plane_package(
                [self._binding(), self._binding()],
                output_dir=self.root / "duplicate",
                package_id="duplicate-v1",
            )
        with self.assertRaisesRegex(RawColdDataPlaneError, "one catalog"):
            build_raw_cold_data_plane_package(
                [
                    self._binding(),
                    self._binding(
                        "nextqa-val-2435100235",
                        self.source_b,
                        catalog_version="different-catalog-v1",
                    ),
                ],
                output_dir=self.root / "catalogs",
                package_id="catalogs-v1",
            )

    def test_mixed_per_object_plan_sets_are_rejected(self) -> None:
        second = self._binding("nextqa-val-2435100235", self.source_b)
        second = RawColdObjectBinding(
            **{**second.__dict__, "plan_ids": ("D0",)}
        )
        with self.assertRaisesRegex(
            RawColdDataPlaneError,
            "representation scope",
        ):
            build_raw_cold_data_plane_package(
                [self._binding(), second],
                output_dir=self.root / "mixed-plan-sets",
                package_id="mixed-plan-sets-v1",
            )

    def test_artifact_bound_is_enforced(self) -> None:
        output = self.root / "package"
        with self.assertRaisesRegex(RawColdDataPlaneError, "within"):
            build_raw_cold_data_plane_package(
                [self._binding()],
                output_dir=output,
                package_id="bounded-v1",
                max_artifact_bytes=len(self.raw_a) - 1,
            )
        self.assertFalse(output.exists())

    def test_re_stamped_identity_tampering_is_rejected(self) -> None:
        output = self.root / "package"
        build_raw_cold_data_plane_package(
            [self._binding()],
            output_dir=output,
            package_id="tamper-v1",
        )
        manifest_path = output / PACKAGE_MANIFEST_NAME
        report = json.loads(manifest_path.read_text())
        report["objects"][0]["provenance"]["source_artifact_sha256"] = "e" * 64
        _write_json(manifest_path, report)
        _restamp(output)

        with self.assertRaisesRegex(RawColdDataPlaneError, "source digest"):
            verify_raw_cold_data_plane_package(output)

    def test_catalog_escape_is_rejected_even_when_checksums_are_restamped(self) -> None:
        output = self.root / "package"
        build_raw_cold_data_plane_package(
            [self._binding()],
            output_dir=output,
            package_id="catalog-escape-v1",
        )
        catalog_path = output / OBJECT_CATALOG_PATH
        catalog = json.loads(catalog_path.read_text())
        catalog["objects"]["nextqa-val-4010069381"]["representations"][
            REPRESENTATION_ID
        ]["path"] = "../../../outside.mp4"
        _write_json(catalog_path, catalog)
        _restamp(output)

        with self.assertRaisesRegex(RawColdDataPlaneError, "does not match"):
            verify_raw_cold_data_plane_package(output)

    def test_unexpected_file_is_rejected(self) -> None:
        output = self.root / "package"
        build_raw_cold_data_plane_package(
            [self._binding()],
            output_dir=output,
            package_id="extra-file-v1",
        )
        (output / "unbound.txt").write_text("not bound", encoding="utf-8")
        with self.assertRaisesRegex(RawColdDataPlaneError, "file set"):
            verify_raw_cold_data_plane_package(output)


if __name__ == "__main__":
    unittest.main()
