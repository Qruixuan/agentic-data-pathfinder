"""The indexed-raw route must carry one exact descriptor from N2 to N6.

Unit tests that hand :class:`N6ModelInputAdapter` a hand-built
``ArtifactAccess`` prove only that the adapter accepts a correct descriptor.
They cannot see a descriptor that is produced correctly, routed correctly and
then rejected at the final consumption gate, which is exactly what a real
multi-host run hit: the N6 adapter demanded the fixed-window sampling method
by name, so a query-aware N3 projection -- a legitimate, verified package --
failed with "N3 temporal bundle does not bind its raw source and policy".

This test walks the whole lifecycle through the production classes, with a
real frozen package underneath, for both projection methods.
"""

from __future__ import annotations

import hashlib
import json
import struct
import tempfile
import unittest
from pathlib import Path
from typing import Any, Mapping

from pathfinder.data_agent_client import DataAgentBinaryArtifact
from pathfinder.simulator.full_flow_exact_range_catalog import (
    ExactFullObjectRangeCatalog,
    build_full_flow_exact_range_catalog,
    verify_full_flow_exact_range_catalog,
)
from pathfinder.simulator.full_flow_n6_adapters import (
    N6AdapterError,
    N6ModelInputAdapter,
)
from pathfinder.simulator.full_flow_route_adapters import (
    ApplicationShapedByteTransferAdapter,
    BoundDataAgentAccessRequestFactory,
    BoundIndexQueryAdapter,
    DataAgentArtifactSourceAdapter,
    FrozenIndexQueryPlan,
    FrozenIndexQueryPlanCatalog,
    StaticDataAgentPlanIdResolver,
)
from pathfinder.simulator.hidden_oracle import build_n1_public_task_binding
from pathfinder.simulator.full_flow_semantic_input_profiles import (
    build_semantic_input_profile,
)
from pathfinder.simulator.full_flow_semantic_route_runtime import (
    ArtifactAccess,
    ArtifactIdentity,
    ExactTemporalFrameSelection,
    _find_values,
    _unwrap,
)
from pathfinder.simulator.n3_indexed_data_plane import (
    INDEXED_REPRESENTATION_ID,
    TEMPORAL_INDEX_SELECTED_SAMPLING_METHOD,
    N3TemporalSelectionPolicy,
    build_n3_indexed_data_plane_package,
)
from pathfinder.simulator.raw_cold_data_plane import (
    PACKAGE_MANIFEST_NAME,
    RawColdObjectBinding,
    build_raw_cold_data_plane_package,
)
from pathfinder.video_prep import SampledImage

OBJECT = "nextqa-val-3429509208"
CATALOG_VERSION = "n3-lifecycle-catalog-v1"
QUESTION = "Which action is visible in the selected interval?"
FRAME_COUNT = 10
WINDOW = (0.4, 1.0)


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _mp4() -> bytes:
    compatible = b"isom" + b"iso2" + b"mp41"
    payload = b"isom" + struct.pack(">I", 512) + compatible
    ftyp = struct.pack(">I", len(payload) + 8) + b"ftyp" + payload
    body = bytes(index % 251 for index in range(1024 * 1024))
    return ftyp + struct.pack(">I", len(body) + 8) + b"mdat" + body


def _jpeg(index: int) -> bytes:
    app0 = b"\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00"
    sof0 = (
        b"\xff\xc0\x00\x0b\x08"
        + (2).to_bytes(2, "big")
        + (2).to_bytes(2, "big")
        + b"\x01\x01\x11\x00"
    )
    sos = b"\xff\xda\x00\x08\x01\x01\x00\x00\x3f\x00"
    return b"\xff\xd8" + app0 + sof0 + sos + bytes([index, 0x22]) + b"\xff\xd9"


class _FreezeSampler:
    """Deterministic frames, so the package is real but decoding is not."""

    duration = 40.0

    def __call__(
        self,
        path: Path,
        *,
        frame_count: int,
        jpeg_max_dimension: int,
        temporal_start_fraction: float,
        temporal_end_fraction: float,
    ) -> tuple[list[SampledImage], float]:
        del path, jpeg_max_dimension
        lower = self.duration * temporal_start_fraction
        upper = self.duration * temporal_end_fraction
        span = (upper - lower) / frame_count
        return ([
            SampledImage(
                frame_index=index,
                timestamp_seconds=lower + span * (index + 0.5),
                width=2,
                height=2,
                jpeg_bytes=_jpeg(index),
            )
            for index in range(frame_count)
        ], self.duration)


class _RefusingRawSampler:
    """An indexed route must never decode the MP4 a second time."""

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        raise AssertionError(
            "the N6 adapter re-decoded the source MP4 instead of consuming "
            "the frozen N3 projection"
        )


class _IndexClient:
    def __init__(self) -> None:
        self.requests: list[Mapping[str, Any]] = []

    def health(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "node_id": "N2",
            "index_id": "lifecycle-index-v1",
            "index_sha256": "b" * 64,
            "credentials_recorded": False,
        }

    def query(self, request: Mapping[str, Any]) -> dict[str, Any]:
        self.requests.append(request)
        result = {
            "status": "COMPLETED",
            "ranked_candidates": [{"object_id": OBJECT}],
            "lexical_retrieval_executed": True,
            "llm_called": False,
            "credentials_recorded": False,
            "eligible_for_scientific_claims": False,
        }
        result["result_content_sha256"] = _sha(_canonical(result))
        return result


class _BinaryDataAgentClient:
    """Return exactly the frozen bytes N3 holds, and record every fetch."""

    def __init__(self, payloads: Mapping[str, bytes]) -> None:
        self.payloads = dict(payloads)
        self.fetched: list[str] = []

    def access(self, request: Any) -> Any:
        raise AssertionError("the indexed route must not use inline access")

    def fetch_binary_artifact(
        self,
        request: Any,
        *,
        allowed_media_types: Any,
    ) -> DataAgentBinaryArtifact:
        del allowed_media_types
        self.fetched.append(request.representation_id)
        payload = self.payloads[request.representation_id]
        return DataAgentBinaryArtifact(
            access_id=request.access_id,
            media_type="application/x-tar",
            data=payload,
            size_bytes=len(payload),
            sha256=_sha(payload),
            object_id=request.object_id,
            object_catalog_version=CATALOG_VERSION,
            location=request.binding["location"],
            service_latency_ms=2.5,
        )


def _provenance() -> dict[str, Any]:
    return {
        "action_id": "D1",
        "anchor_window_ordinals": [2, 3],
        "anchor_top_k": 2,
        "expansion_basis": "anchor-window-union",
        "fallback_used": False,
        "max_selected_windows": 4,
        "merged_intervals_seconds": [[16.0, 40.0]],
        "public_question_sha256": "c" * 64,
        "relation": "after",
        "selected_window_ordinals": [2, 3, 4, 5],
        "temporal_index_package_sha256": "d" * 64,
    }


class IndexedRouteDescriptorLifecycleTest(unittest.TestCase):
    """One descriptor, produced by N3, must survive to the N6 request."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.payload = _mp4()
        source = self.root / "source.mp4"
        source.write_bytes(self.payload)
        self.raw_dir = self.root / "raw"
        build_raw_cold_data_plane_package(
            [RawColdObjectBinding(
                object_id=OBJECT,
                artifact_path=source,
                catalog_version=CATALOG_VERSION,
                plan_ids=tuple(f"D{index}" for index in range(8)),
                dataset_id="nextqa",
                dataset_revision="lifecycle-v1",
                source_object_id="3429509208",
                artifact_sha256=_sha(self.payload),
                artifact_size_bytes=len(self.payload),
            )],
            output_dir=self.raw_dir,
            package_id="n3-raw-lifecycle-v1",
        )

    # ---------------------------------------------------------------- setup

    def _freeze(self, name: str, policy: N3TemporalSelectionPolicy) -> Path:
        package = self.root / name
        build_n3_indexed_data_plane_package(
            self.raw_dir,
            output_dir=package,
            package_id=f"n3-indexed-{name}-v1",
            policy=policy,
            sampler=_FreezeSampler(),
        )
        return package

    def _catalog(self, package: Path, name: str) -> ExactFullObjectRangeCatalog:
        catalog_dir = self.root / f"{name}-selections"
        build_full_flow_exact_range_catalog(
            package,
            catalog_id=f"{name}-selection-v1",
            output_dir=catalog_dir,
        )
        verify_full_flow_exact_range_catalog(catalog_dir, package)
        return ExactFullObjectRangeCatalog(catalog_dir, package)

    def _bundle_bytes(self, package: Path) -> bytes:
        report = json.loads(
            (package / PACKAGE_MANIFEST_NAME).read_text(encoding="utf-8")
        )
        row = next(
            value for value in report["objects"]
            if value["representation_id"] == INDEXED_REPRESENTATION_ID
        )
        return (package / row["artifact_package_path"]).read_bytes()

    def _identity(self) -> ArtifactIdentity:
        return ArtifactIdentity(
            object_id=OBJECT,
            representation_id="raw_video",
            artifact_sha256=_sha(self.payload),
            artifact_size_bytes=len(self.payload),
            object_catalog_version=CATALOG_VERSION,
        )

    def _trial(self, profile: Mapping[str, Any] | None) -> dict[str, Any]:
        identity = self._identity()
        trial: dict[str, Any] = {
            "trial_key": "lifecycle|smoke-temporal|D1|r0000",
            "order_index": 0,
            "route_family": "indexed-raw",
            "executor_node_id": "N7",
            "artifact_object_id": OBJECT,
            "public_task_binding": {"task_class_id": "video_qa"},
            "representation_identities": [{
                "artifact_object_id": identity.object_id,
                "representation_id": identity.representation_id,
                "representation_binding": {
                    "artifact_sha256": identity.artifact_sha256,
                    "artifact_size_bytes": identity.artifact_size_bytes,
                    "object_catalog_version": identity.object_catalog_version,
                },
            }],
        }
        if profile is not None:
            trial["semantic_input_profile"] = dict(profile)
        return trial

    @staticmethod
    def _task() -> dict[str, Any]:
        return build_n1_public_task_binding(
            workload_id="W1",
            object_id=OBJECT,
            task_class_id="temporal",
            question=QUESTION,
            answer_options=[
                {"option_id": "A", "text": "A vehicle crosses a river."},
                {"option_id": "B", "text": "Two people perform music."},
            ],
            success_scoring_rule=(
                "multiple-choice-option-id-exact-match-v1"
            ),
        )

    # ------------------------------------------------------------ lifecycle

    def _run_lifecycle(
        self,
        package: Path,
        name: str,
        profile: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        """Drive query-index -> read-range -> transfer-range -> decode."""

        catalog = self._catalog(package, name)
        bundle = self._bundle_bytes(package)
        identity = self._identity()
        trial = self._trial(profile)
        observed: dict[str, Any] = {}

        # 1. ExactFullObjectRangeCatalog.resolve
        resolved = catalog.resolve(identity)
        self.assertIs(type(resolved), ExactTemporalFrameSelection)
        self.assertEqual(
            "pathfinder.simulator.full_flow_semantic_route_runtime",
            type(resolved).__module__,
        )
        self.assertEqual(
            "ExactTemporalFrameSelection", type(resolved).__qualname__
        )
        observed["descriptor_sha256"] = resolved.descriptor_sha256
        observed["selection_policy_sha256"] = resolved.selection_policy_sha256
        observed["frame_count"] = resolved.frame_count
        observed["window"] = (
            resolved.temporal_start_fraction,
            resolved.temporal_end_fraction,
        )
        self.assertEqual(_sha(bundle), resolved.selected_artifact_sha256)
        self.assertEqual(len(bundle), resolved.selected_artifact_size_bytes)
        self.assertEqual(
            _sha(self.payload), resolved.full_artifact_sha256
        )

        # 2. BoundIndexQueryAdapter.query -- exactly one exact descriptor
        query_adapter = BoundIndexQueryAdapter(
            clients={"N2": _IndexClient()},
            query_plans=FrozenIndexQueryPlanCatalog([FrozenIndexQueryPlan(
                trial_key=trial["trial_key"],
                task_binding_sha256=self._task()["task_binding_sha256"],
                index_id="lifecycle-index-v1",
                query_id="lifecycle-query-v1",
                query_text=QUESTION,
                top_k=1,
                candidate_object_ids=(OBJECT,),
            )]),
            exact_ranges=catalog,
        )
        selection = query_adapter.query(
            run_id="lifecycle-run-v1",
            trial=trial,
            stage={"stage_key": "query-index", "logical_node_ids": ["N2"]},
            public_task=self._task(),
            expected_object_id=OBJECT,
        )
        found = _find_values(selection.segment, ExactTemporalFrameSelection)
        self.assertEqual(1, len(found))
        self.assertEqual(
            observed["descriptor_sha256"], found[0].descriptor_sha256
        )

        # 3/4. read-range: fetch_selected must preserve the same descriptor
        n3 = _BinaryDataAgentClient({
            INDEXED_REPRESENTATION_ID: bundle,
            "raw_video": self.payload,
        })
        source_adapter = DataAgentArtifactSourceAdapter(
            clients={"N3": n3, "N4": _BinaryDataAgentClient({})},
            request_factory=BoundDataAgentAccessRequestFactory(
                source_locations={"N3": "origin-cold", "N4": "origin-warm"},
                plan_ids=StaticDataAgentPlanIdResolver({
                    ("N3", INDEXED_REPRESENTATION_ID): "projection-plan-v1",
                }),
            ),
            allowed_media_types={
                INDEXED_REPRESENTATION_ID: ("application/x-tar",),
            },
        )
        access = source_adapter.fetch_selected(
            run_id="lifecycle-run-v1",
            trial=trial,
            stage={
                "stage_index": 3,
                "stage_key": "read-range",
                "logical_node_ids": ["N3"],
            },
            source_identity=identity,
            selection=selection.segment,
        )
        self.assertIs(selection.segment, access.segment)
        self.assertEqual([INDEXED_REPRESENTATION_ID], n3.fetched)
        self.assertEqual(bundle, access.payload)
        self.assertEqual(identity, access.source_identity)

        # 5. transfer-range must not rewrap or drop the descriptor
        transfer = ApplicationShapedByteTransferAdapter(
            profile_id="lifecycle-shaping-v1",
            executor_node_id="N7",
            bandwidth_bytes_per_second=1_000_000_000.0,
            round_trip_time_ms=0.0,
        ).transfer(
            run_id="lifecycle-run-v1",
            trial=trial,
            stage={
                "stage_index": 4,
                "stage_key": "transfer-range",
                "logical_node_ids": ["N3", "N7"],
            },
            value=access,
        )

        # 6. _unwrap / _find_values must hand the same artifact to decode
        self.assertIs(access, _unwrap(transfer))
        artifacts = _find_values(transfer, ArtifactAccess)
        self.assertEqual(1, len(artifacts))
        self.assertIs(access, artifacts[0])
        self.assertIs(selection.segment, artifacts[0].segment)

        # 7. decode: prepare consumes the frozen bundle, never the MP4
        prepared = N6ModelInputAdapter(
            raw_sampler=_RefusingRawSampler(),
        ).prepare(
            run_id="lifecycle-run-v1",
            trial=trial,
            stage={
                "stage_index": 5,
                "stage_key": "decode",
                "logical_node_ids": ["N7"],
            },
            public_task=self._task(),
            mode="raw-prepared-frames",
            artifacts=artifacts,
        )
        observed["prepared"] = prepared
        observed["frames"] = json.loads(
            prepared.payload.decode("utf-8")
        )["frames"]
        return observed

    # ---------------------------------------------------------------- tests

    def test_query_aware_projection_survives_the_whole_route(self) -> None:
        policy = N3TemporalSelectionPolicy(
            frame_count=FRAME_COUNT,
            temporal_start_fraction=WINDOW[0],
            temporal_end_fraction=WINDOW[1],
            sampling_method=TEMPORAL_INDEX_SELECTED_SAMPLING_METHOD,
            selection_provenance=_provenance(),
        )
        package = self._freeze("query-aware", policy)
        profile = build_semantic_input_profile(
            route_family="indexed-raw",
            model_input_representation_ids=["raw_video"],
            indexed_selection_kind="query-aware-temporal-index",
            indexed_frame_count=FRAME_COUNT,
            indexed_temporal_window_fraction=WINDOW,
        )
        observed = self._run_lifecycle(package, "query-aware", profile)

        self.assertEqual(FRAME_COUNT, observed["frame_count"])
        self.assertEqual(WINDOW, observed["window"])
        prepared = observed["prepared"]
        self.assertEqual("raw-prepared-frames", prepared.mode)
        self.assertEqual(FRAME_COUNT, len(observed["frames"]))

    def test_fixed_window_projection_still_survives_the_whole_route(self) -> None:
        # The fix must not be a loosening that only the new method exercises.
        package = self._freeze("fixed-window", N3TemporalSelectionPolicy())
        profile = build_semantic_input_profile(
            route_family="indexed-raw",
            model_input_representation_ids=["raw_video"],
        )
        observed = self._run_lifecycle(package, "fixed-window", profile)

        self.assertEqual(8, observed["frame_count"])
        self.assertEqual((0.25, 0.75), observed["window"])
        self.assertEqual(8, len(observed["frames"]))

    def test_a_trial_without_a_profile_cannot_consume_a_projection(self) -> None:
        # Pre-existing contract, asserted here so the lifecycle file records
        # it: with no frozen profile the coordinator has no declared
        # selection, so it falls back to the whole-object default and must
        # refuse the exact descriptor rather than silently reinterpret it.
        package = self._freeze("no-profile", N3TemporalSelectionPolicy())
        with self.assertRaisesRegex(
            N6AdapterError,
            "semantic profile differs from the N3 temporal selection",
        ):
            self._run_lifecycle(package, "no-profile", None)

    def test_a_bundle_from_another_policy_is_still_refused(self) -> None:
        # The gate must bind the exact policy, not merely allow any method.
        query_aware = self._freeze("mismatch-qa", N3TemporalSelectionPolicy(
            frame_count=FRAME_COUNT,
            temporal_start_fraction=WINDOW[0],
            temporal_end_fraction=WINDOW[1],
            sampling_method=TEMPORAL_INDEX_SELECTED_SAMPLING_METHOD,
            selection_provenance=_provenance(),
        ))
        other = self._freeze("mismatch-other", N3TemporalSelectionPolicy(
            frame_count=FRAME_COUNT,
            temporal_start_fraction=WINDOW[0],
            temporal_end_fraction=WINDOW[1],
        ))
        catalog = self._catalog(query_aware, "mismatch-qa")
        descriptor = catalog.resolve(self._identity())
        foreign = self._bundle_bytes(other)

        # Same frame count and window, a different frozen policy document.
        swapped = ExactTemporalFrameSelection(
            object_id=descriptor.object_id,
            representation_id=descriptor.representation_id,
            object_catalog_version=descriptor.object_catalog_version,
            full_artifact_size_bytes=descriptor.full_artifact_size_bytes,
            full_artifact_sha256=descriptor.full_artifact_sha256,
            selected_representation_id=descriptor.selected_representation_id,
            selected_artifact_size_bytes=len(foreign),
            selected_artifact_sha256=_sha(foreign),
            frame_count=descriptor.frame_count,
            temporal_start_fraction=descriptor.temporal_start_fraction,
            temporal_end_fraction=descriptor.temporal_end_fraction,
            selection_policy_sha256=descriptor.selection_policy_sha256,
        )
        profile = build_semantic_input_profile(
            route_family="indexed-raw",
            model_input_representation_ids=["raw_video"],
            indexed_selection_kind="query-aware-temporal-index",
            indexed_frame_count=FRAME_COUNT,
            indexed_temporal_window_fraction=WINDOW,
        )
        access = ArtifactAccess(
            source_identity=self._identity(),
            payload=foreign,
            segment=swapped,
        )
        with self.assertRaisesRegex(
            N6AdapterError,
            "N3 temporal bundle does not bind its raw source and policy",
        ):
            N6ModelInputAdapter(raw_sampler=_RefusingRawSampler()).prepare(
                run_id="lifecycle-run-v1",
                trial=self._trial(profile),
                stage={
                    "stage_index": 5,
                    "stage_key": "decode",
                    "logical_node_ids": ["N7"],
                },
                public_task=self._task(),
                mode="raw-prepared-frames",
                artifacts=[access],
            )


if __name__ == "__main__":
    unittest.main()
