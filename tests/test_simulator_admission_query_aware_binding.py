"""Focused coverage for the query-aware selection authority in promotion.

The N3 package is the only authority for which interval was projected, and the
sibling runtime frame manifest is the only evidence of which frames were
decoded inside it.  Both are read here rather than trusted from a template.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pathfinder.simulator.full_flow_local_semantic_admission import (
    FullFlowLocalSemanticAdmissionError,
    _n3_indexed_selection,
    _n3_indexed_selections,
    _runtime_frame_binding,
    _runtime_frame_bindings,
)
from pathfinder.simulator.full_flow_semantic_input_profiles import (
    QUERY_AWARE_TEMPORAL_INDEX_SELECTION,
)

POLICY = {
    "frame_count": 10,
    "jpeg_max_dimension": 768,
    "partial_mp4_byte_range_claimed": False,
    "query_aware_selection": True,
    "sampling_method": "temporal-index-selected-interval",
    "selection_semantics": "source-decoded-temporal-frame-bundle",
    "source_side_projection_executed": True,
    "temporal_index_selection": {"fallback_used": False, "relation": "following"},
    "temporal_window_fraction": [0.4, 1.0],
}
MANIFEST = {
    "all_frame_payloads_distinct": True,
    "all_frames_bound_to_source_video": True,
    "all_frames_inside_selected_interval": True,
    "all_frames_strictly_ordered": True,
    "manifest_sha256": "a" * 64,
    "n3_package_id": "n3-query-aware-test-v1",
    "object_id": "nextqa-val-3429509208",
    "original_object_bytes_read": 1626982,
    "partial_mp4_byte_range_claimed": False,
    "plan_id": "runtime-frame-plan-test",
    "plan_sha256": "b" * 64,
    "reduced_source_storage_io_claimed": False,
    "representation_label": "query-aware temporal-index-selected frames",
    "runtime_frame_count": 10,
    "selected_artifact_bytes": 367904,
    "selection_policy": POLICY,
    "source_video_sha256": "c" * 64,
}


class QueryAwareBindingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.counter = 0

    def _next(self, prefix: str) -> Path:
        self.counter += 1
        directory = self.root / f"{prefix}-{self.counter}"
        directory.mkdir()
        return directory

    def _package(
        self, policy=POLICY, package_id="n3-query-aware-test-v1"
    ) -> Path:
        directory = self._next("pkg")
        document = {"package_id": package_id}
        if policy is not None:
            document["selection_policy"] = policy
        (directory / "raw-cold-data-plane.json").write_text(
            json.dumps(document), encoding="utf-8"
        )
        return directory

    def _manifest_dir(self, **overrides) -> Path:
        directory = self._next("rfm")
        (directory / "runtime-frame-manifest.json").write_text(
            json.dumps({**MANIFEST, **overrides}), encoding="utf-8"
        )
        (directory / "SHA256SUMS").write_text("x\n", encoding="utf-8")
        return directory

    # --- the N3 package is the selection authority ------------------------
    def test_a_query_aware_package_yields_its_exact_selection(self) -> None:
        self.assertEqual(
            {
                "indexed_selection_kind": QUERY_AWARE_TEMPORAL_INDEX_SELECTION,
                "indexed_frame_count": 10,
                "indexed_temporal_window_fraction": (0.4, 1.0),
            },
            _n3_indexed_selection(self._package()),
        )

    def test_a_package_without_a_policy_stays_fixed_window(self) -> None:
        self.assertIsNone(_n3_indexed_selection(self._package(policy=None)))

    def test_a_fixed_window_package_stays_fixed_window(self) -> None:
        policy = dict(
            POLICY,
            sampling_method="uniform-midpoint-temporal-window",
            query_aware_selection=False,
        )
        self.assertIsNone(_n3_indexed_selection(self._package(policy=policy)))

    def test_a_fixed_window_package_may_not_claim_query_awareness(self) -> None:
        policy = dict(POLICY, sampling_method="uniform-midpoint-temporal-window")
        with self.assertRaises(FullFlowLocalSemanticAdmissionError):
            _n3_indexed_selection(self._package(policy=policy))

    def test_a_fallback_selection_is_refused(self) -> None:
        policy = json.loads(json.dumps(POLICY))
        policy["temporal_index_selection"]["fallback_used"] = True
        with self.assertRaises(FullFlowLocalSemanticAdmissionError):
            _n3_indexed_selection(self._package(policy=policy))

    def test_object_specific_selections_remain_distinct(self) -> None:
        directory = self._next("multi-pkg")
        other = dict(POLICY, temporal_window_fraction=[0.1, 0.5])
        (directory / "raw-cold-data-plane.json").write_text(
            json.dumps({
                "package_id": "n3-query-aware-multi-v1",
                "selection_policies": {
                    "nextqa-val-111": POLICY,
                    "nextqa-val-222": other,
                },
            }),
            encoding="utf-8",
        )
        selections = _n3_indexed_selections(directory)
        self.assertEqual((0.4, 1.0), selections["nextqa-val-111"][
            "indexed_temporal_window_fraction"
        ])
        self.assertEqual((0.1, 0.5), selections["nextqa-val-222"][
            "indexed_temporal_window_fraction"
        ])
        with self.assertRaisesRegex(
            FullFlowLocalSemanticAdmissionError, "object-aware"
        ):
            _n3_indexed_selection(directory)

    # --- the sibling manifest must be bound -------------------------------
    def _bind(self, package, manifest_dir, selection=None):
        if selection is None:
            selection = _n3_indexed_selection(package)
        return _runtime_frame_binding(
            manifest_dir,
            n3_package_dir=package,
            indexed_selection=selection,
        )

    def test_the_manifest_digests_enter_the_commitment(self) -> None:
        package = self._package()
        binding = self._bind(package, self._manifest_dir())
        self.assertEqual("b" * 64, binding["runtime_frame_plan_sha256"])
        self.assertEqual("a" * 64, binding["runtime_frame_manifest_sha256"])
        self.assertEqual(64, len(binding["runtime_frame_manifest_file_sha256"]))
        self.assertEqual(10, binding["runtime_frame_count"])
        self.assertEqual(367904, binding["runtime_selected_artifact_bytes"])
        self.assertEqual(1626982, binding["runtime_original_object_bytes_read"])
        self.assertFalse(binding["reduced_source_storage_io_claimed"])

    def test_a_query_aware_package_requires_a_manifest(self) -> None:
        package = self._package()
        with self.assertRaisesRegex(
            FullFlowLocalSemanticAdmissionError, "requires its runtime frame"
        ):
            self._bind(package, None)

    def test_a_fixed_window_package_refuses_a_manifest(self) -> None:
        package = self._package(policy=None)
        with self.assertRaises(FullFlowLocalSemanticAdmissionError):
            self._bind(package, self._manifest_dir())

    def test_a_manifest_from_another_package_is_refused(self) -> None:
        package = self._package()
        with self.assertRaisesRegex(
            FullFlowLocalSemanticAdmissionError, "different N3 package"
        ):
            self._bind(package, self._manifest_dir(n3_package_id="other-v1"))

    def test_a_manifest_recording_another_policy_is_refused(self) -> None:
        package = self._package()
        other = dict(POLICY, frame_count=8)
        with self.assertRaisesRegex(
            FullFlowLocalSemanticAdmissionError, "different selection policy"
        ):
            self._bind(package, self._manifest_dir(selection_policy=other))

    def test_unverified_runtime_frames_are_refused(self) -> None:
        package = self._package()
        for flag in (
            "all_frames_inside_selected_interval",
            "all_frames_strictly_ordered",
            "all_frame_payloads_distinct",
            "all_frames_bound_to_source_video",
        ):
            with self.subTest(flag=flag):
                with self.assertRaises(FullFlowLocalSemanticAdmissionError):
                    self._bind(package, self._manifest_dir(**{flag: False}))

    def test_a_storage_io_reduction_claim_is_refused(self) -> None:
        package = self._package()
        for flag in (
            "partial_mp4_byte_range_claimed",
            "reduced_source_storage_io_claimed",
        ):
            with self.subTest(flag=flag):
                with self.assertRaisesRegex(
                    FullFlowLocalSemanticAdmissionError, "byte-range reduction"
                ):
                    self._bind(package, self._manifest_dir(**{flag: True}))

    def test_object_specific_runtime_manifests_are_bound_by_object(self) -> None:
        package = self._next("multi-pkg")
        other_policy = dict(POLICY, temporal_window_fraction=[0.1, 0.5])
        (package / "raw-cold-data-plane.json").write_text(
            json.dumps({
                "package_id": "n3-query-aware-test-v1",
                "selection_policies": {
                    "nextqa-val-111": POLICY,
                    "nextqa-val-222": other_policy,
                },
            }),
            encoding="utf-8",
        )
        root = self._next("multi-rfm")
        for object_id, policy in (
            ("nextqa-val-111", POLICY),
            ("nextqa-val-222", other_policy),
        ):
            directory = root / object_id
            directory.mkdir()
            (directory / "runtime-frame-manifest.json").write_text(
                json.dumps({
                    **MANIFEST,
                    "object_id": object_id,
                    "selection_policy": policy,
                }),
                encoding="utf-8",
            )
            (directory / "SHA256SUMS").write_text("x\n", encoding="utf-8")
        selections = _n3_indexed_selections(package)
        binding = _runtime_frame_bindings(
            root,
            n3_package_dir=package,
            indexed_selections=selections,
        )
        self.assertEqual(
            {"nextqa-val-111", "nextqa-val-222"},
            set(binding["object_bindings"]),
        )


if __name__ == "__main__":
    unittest.main()
