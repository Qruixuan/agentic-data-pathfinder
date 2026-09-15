from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from pathfinder.simulator.full_flow_artifact_preflight import (
    ArtifactAvailabilityObservation,
    FullFlowArtifactPreflightError,
    MANIFEST_NAME,
    OBSERVATIONS_NAME,
    preflight_full_flow_semantic_artifacts,
    verify_full_flow_semantic_artifact_preflight,
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


def _admission(root: Path) -> Path:
    source = root / "admission"
    source.mkdir()
    bindings = []
    for representation, payload in (
        ("raw_video", b"raw-content"),
        ("multimodal_digest", b"digest-content"),
        ("sampled_frame_bundle", b"bundle-content"),
    ):
        bindings.append({
            "logical_object_id": "logical-object-1",
            "artifact_object_id": "artifact-object-1",
            "representation_id": representation,
            "representation_binding": {
                "representation_id": representation,
                "artifact_sha256": _sha256(payload),
                "artifact_size_bytes": len(payload),
                "object_catalog_version": "catalog-v1",
            },
        })
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
        "admission_id": "test-semantic-admission-v1",
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


class FakeProbe:
    def __init__(
        self,
        *,
        bad_digest: bool = False,
        authenticated: bool = True,
        plan_ids: dict[str, str] | None = None,
        plan_binding_source_sha256: dict[str, str] | None = None,
    ) -> None:
        self.bad_digest = bad_digest
        self.authenticated = authenticated
        self.plan_ids = dict(plan_ids or {})
        self.plan_binding_source_sha256 = dict(
            plan_binding_source_sha256 or {}
        )
        self.calls = []

    def fetch_and_verify(self, *, identity, source_node_id, service_contract_id):
        self.calls.append((identity, source_node_id, service_contract_id))
        media = {
            "raw_video": "video/mp4",
            "sampled_frame_bundle": "application/x-tar",
            "multimodal_digest": "text/plain; charset=utf-8",
        }[identity.representation_id]
        binary = identity.representation_id != "multimodal_digest"
        return ArtifactAvailabilityObservation(
            source_node_id=source_node_id,
            service_contract_id=service_contract_id,
            identity=identity,
            observed_sha256=("f" * 64 if self.bad_digest else identity.artifact_sha256),
            observed_size_bytes=identity.artifact_size_bytes,
            media_type=media,
            plan_id=self.plan_ids.get(
                identity.representation_id,
                "plan-v1",
            ),
            plan_binding_source_sha256=(
                self.plan_binding_source_sha256.get(source_node_id, "a" * 64)
            ),
            expected_location=(
                "origin-cold" if source_node_id == "N3" else "origin-warm"
            ),
            package_binding_verified=True,
            authenticated=self.authenticated,
            authentication_challenge_verified=True,
            full_content_fetched=True,
            request_count=1,
            bytes_read=identity.artifact_size_bytes,
            service_time_ms=1.25,
            client_round_trip_ms=2.5,
            artifact_download_elapsed_ms=(1.0 if binary else 0.0),
            artifact_download_request_count=(1 if binary else 0),
            artifact_completed_request_count=(1 if binary else 0),
            artifact_full_download_count=(1 if binary else 0),
            artifact_bytes_sent=(
                identity.artifact_size_bytes if binary else 0
            ),
            artifact_transfer_latency_ms=(0.75 if binary else 0.0),
            telemetry_complete=True,
        )


class FullFlowArtifactPreflightTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.admission = _admission(self.root)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_fetches_each_unique_artifact_once_and_verifies(self) -> None:
        probe = FakeProbe()
        output = self.root / "preflight"
        result = preflight_full_flow_semantic_artifacts(
            self.admission,
            preflight_id="artifact-preflight-v1",
            probe=probe,
            output_dir=output,
        )
        self.assertEqual("VERIFIED", result["status"])
        self.assertEqual(3, result["artifact_count"])
        self.assertEqual(3, len(probe.calls))
        self.assertEqual({"N3", "N4"}, {call[1] for call in probe.calls})
        verified = verify_full_flow_semantic_artifact_preflight(
            output,
            semantic_execution_admission_dir=self.admission,
        )
        self.assertTrue(verified["all_content_identities_verified"])

    def test_output_excludes_payload_endpoints_and_credentials(self) -> None:
        output = self.root / "preflight"
        preflight_full_flow_semantic_artifacts(
            self.admission,
            preflight_id="artifact-preflight-v1",
            probe=FakeProbe(),
            output_dir=output,
        )
        text = (output / MANIFEST_NAME).read_text(encoding="utf-8")
        text += (output / OBSERVATIONS_NAME).read_text(encoding="utf-8")
        self.assertNotIn("http://", text)
        self.assertNotIn("https://", text)
        self.assertNotIn("bearer", text.casefold())
        self.assertNotIn("api_key", text.casefold())
        self.assertNotIn("raw-content", text)

    def test_content_mismatch_fails_before_output_is_published(self) -> None:
        output = self.root / "preflight"
        with self.assertRaisesRegex(
            FullFlowArtifactPreflightError,
            "content differs",
        ):
            preflight_full_flow_semantic_artifacts(
                self.admission,
                preflight_id="artifact-preflight-v1",
                probe=FakeProbe(bad_digest=True),
                output_dir=output,
            )
        self.assertFalse(output.exists())

    def test_unauthenticated_probe_fails_closed(self) -> None:
        with self.assertRaisesRegex(
            FullFlowArtifactPreflightError,
            "unauthenticated",
        ):
            preflight_full_flow_semantic_artifacts(
                self.admission,
                preflight_id="artifact-preflight-v1",
                probe=FakeProbe(authenticated=False),
                output_dir=self.root / "preflight",
            )

    def test_source_admission_drift_invalidates_preflight(self) -> None:
        output = self.root / "preflight"
        preflight_full_flow_semantic_artifacts(
            self.admission,
            preflight_id="artifact-preflight-v1",
            probe=FakeProbe(),
            output_dir=output,
        )
        path = self.admission / "semantic-execution-trials.jsonl"
        path.write_bytes(path.read_bytes() + b"{}\n")
        with self.assertRaisesRegex(
            FullFlowArtifactPreflightError,
            "checksums failed",
        ):
            verify_full_flow_semantic_artifact_preflight(
                output,
                semantic_execution_admission_dir=self.admission,
            )


if __name__ == "__main__":
    unittest.main()
