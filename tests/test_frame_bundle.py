"""Deterministic offline generation of the sampled_frame_bundle artifact."""

from __future__ import annotations

import io
import json
import tarfile
import tempfile
import unittest
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping, Sequence

from pathfinder.frame_bundle import (
    BUNDLE_NAME,
    CHECKSUM_NAME,
    FRAME_BUNDLE_GENERATION_SCHEMA_VERSION,
    FRAME_BUNDLE_SCHEMA_VERSION,
    OBJECT_MANIFEST_NAME,
    REPRESENTATION_ID,
    FrameBundleError,
    build_frame_bundles,
)
from pathfinder.video_prep import SampledImage


FRAME_COUNT = 4
MAX_DIMENSION = 768
OBJECTS = ("nextqa-val-1000000001", "nextqa-val-1000000002")


def _jpeg(object_id: str, index: int) -> bytes:
    """Deterministic stand-in bytes with a real JPEG signature."""
    body = f"{object_id}:{index}".encode("utf-8") * 8
    return b"\xff\xd8\xff\xe0" + body + b"\xff\xd9"


def _frames(object_id: str) -> list[SampledImage]:
    return [
        SampledImage(
            frame_index=index,
            timestamp_seconds=round(1.5 * index + 0.75, 6),
            width=480,
            height=640,
            jpeg_bytes=_jpeg(object_id, index),
        )
        for index in range(FRAME_COUNT)
    ]


class FakeSampler:
    """The decoder boundary, replaced so tests need no media or codecs."""

    def __init__(self, overrides: Mapping[str, Any] | None = None) -> None:
        self.overrides = dict(overrides or {})
        self.calls: list[tuple[str, int, int]] = []

    def __call__(
        self,
        path: Path,
        *,
        frame_count: int,
        jpeg_max_dimension: int,
    ) -> tuple[list[SampledImage], float]:
        object_id = f"nextqa-val-{path.stem}"
        self.calls.append((path.name, frame_count, jpeg_max_dimension))
        frames = _frames(object_id)
        mutate = self.overrides.get(object_id)
        if mutate is not None:
            frames = mutate(frames)
        return frames, 6.0


def _write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def _canonical(value: Any) -> bytes:
    return (
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False)
        + "\n"
    ).encode("utf-8")


def build_inputs(
    root: Path,
    *,
    objects: Sequence[str] = OBJECTS,
    video_sha_override: str | None = None,
    description_sha_override: str | None = None,
    duplicate_object: bool = False,
    frame_count_override: int | None = None,
    timestamp_override: float | None = None,
    width_override: int | None = None,
    traversal_video: bool = False,
    traversal_representation: bool = False,
    method_override: str | None = None,
    drop_sampling_field: str | None = None,
) -> tuple[Path, Path, Path]:
    videos = root / "videos"
    representations = root / "representations"
    videos.mkdir(parents=True, exist_ok=True)
    representations.mkdir(parents=True, exist_ok=True)

    entries = []
    for object_id in objects:
        video_id = object_id.rsplit("-", 1)[-1]
        filename = f"{video_id}.mp4"
        video_bytes = f"fake-video::{video_id}".encode("utf-8")
        _write(videos / filename, video_bytes)

        sampling = {
            "method": method_override or "uniform-midpoint",
            "frame_count": FRAME_COUNT,
            "jpeg_max_dimension": MAX_DIMENSION,
        }
        if drop_sampling_field:
            sampling.pop(drop_sampling_field, None)
        frames = []
        for index, image in enumerate(_frames(object_id)):
            frames.append({
                "frame_index": image.frame_index,
                "timestamp_seconds": (
                    timestamp_override
                    if timestamp_override is not None and index == 0
                    else image.timestamp_seconds
                ),
                "width": (
                    width_override
                    if width_override is not None and index == 0
                    else image.width
                ),
                "height": image.height,
                "description": f"frame {index}",
                "visible_text": None,
            })
        if frame_count_override is not None:
            frames = frames[:frame_count_override]
            sampling["frame_count"] = frame_count_override
        document = {
            "schema_version": (
                "pathfinder.sampled-frame-observations/v1alpha1"
            ),
            "object_id": object_id,
            "source_video_id": video_id,
            "source_video_sha256": sha256(video_bytes).hexdigest(),
            "source_duration_seconds": 6.0,
            "sampling": sampling,
            "frames": frames,
        }
        description_bytes = _canonical(document)
        relative = f"{object_id}/sampled_frames.json"
        _write(representations / relative, description_bytes)

        entries.append({
            "object_id": object_id,
            "source_video": {
                "filename": (
                    "../outside.mp4" if traversal_video else filename
                ),
                "size_bytes": len(video_bytes),
                "sha256": (
                    video_sha_override
                    or sha256(video_bytes).hexdigest()
                ),
            },
            "representations": {
                "sampled_frames": {
                    "path": (
                        "../outside.json" if traversal_representation
                        else relative
                    ),
                    "size_bytes": len(description_bytes),
                    "sha256": (
                        description_sha_override
                        or sha256(description_bytes).hexdigest()
                    ),
                },
            },
        })
    if duplicate_object:
        entries.append(dict(entries[0]))

    manifest = {
        "schema_version": "pathfinder.video-representation-prep/v1alpha1",
        "frame_count": FRAME_COUNT,
        "jpeg_max_dimension": MAX_DIMENSION,
        "objects": entries,
    }
    manifest_path = representations / "generation-manifest.json"
    _write(manifest_path, _canonical(manifest))
    return videos, representations, manifest_path


class BuildSuccessTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.videos, self.representations, self.manifest = build_inputs(
            self.root
        )
        self.sampler = FakeSampler()
        self.result = build_frame_bundles(
            video_dir=self.videos,
            representation_dir=self.representations,
            generation_manifest=self.manifest,
            output_dir=self.root / "out",
            sampler=self.sampler,
        )
        self.out = self.root / "out"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_the_expected_tree_is_produced(self) -> None:
        self.assertEqual("COMPLETE", self.result["status"])
        self.assertEqual(REPRESENTATION_ID, self.result["representation_id"])
        self.assertEqual(2, self.result["object_count"])
        self.assertEqual(2 * FRAME_COUNT, self.result["total_frame_count"])
        names = {p.name for p in self.out.iterdir()}
        self.assertEqual(
            {
                "frame-bundle-generation-manifest.json",
                CHECKSUM_NAME,
                *OBJECTS,
            },
            names,
        )
        for object_id in OBJECTS:
            directory = self.out / object_id
            self.assertTrue((directory / BUNDLE_NAME).is_file())
            self.assertTrue((directory / OBJECT_MANIFEST_NAME).is_file())
            frames = sorted(
                p.name for p in (directory / "frames").iterdir()
            )
            self.assertEqual(
                ["000.jpg", "001.jpg", "002.jpg", "003.jpg"], frames
            )

    def test_jpeg_bytes_are_preserved_exactly(self) -> None:
        for object_id in OBJECTS:
            for index, image in enumerate(_frames(object_id)):
                written = (
                    self.out / object_id / "frames" / f"{index:03d}.jpg"
                ).read_bytes()
                self.assertEqual(image.jpeg_bytes, written)

    def test_the_sampler_receives_the_frozen_settings(self) -> None:
        for _name, frame_count, dimension in self.sampler.calls:
            self.assertEqual(FRAME_COUNT, frame_count)
            self.assertEqual(MAX_DIMENSION, dimension)

    def test_per_frame_hashes_are_correct(self) -> None:
        document = json.loads(
            (self.out / OBJECTS[0] / OBJECT_MANIFEST_NAME).read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(FRAME_COUNT, document["frame_count"])
        total = 0
        for entry, image in zip(document["frames"], _frames(OBJECTS[0])):
            self.assertEqual(image.frame_index, entry["frame_index"])
            self.assertEqual(
                image.timestamp_seconds, entry["timestamp_seconds"]
            )
            self.assertEqual(image.width, entry["width"])
            self.assertEqual(image.height, entry["height"])
            self.assertEqual(len(image.jpeg_bytes), entry["jpeg_size_bytes"])
            self.assertEqual(
                sha256(image.jpeg_bytes).hexdigest(), entry["jpeg_sha256"]
            )
            total += entry["jpeg_size_bytes"]
        self.assertEqual(total, document["total_jpeg_bytes"])

    def test_the_embedded_manifest_matches_the_external_copy(self) -> None:
        for object_id in OBJECTS:
            external = (
                self.out / object_id / OBJECT_MANIFEST_NAME
            ).read_bytes()
            with tarfile.open(
                self.out / object_id / BUNDLE_NAME, "r"
            ) as archive:
                embedded = archive.extractfile(
                    OBJECT_MANIFEST_NAME
                ).read()
            self.assertEqual(external, embedded)

    def test_the_tar_has_deterministic_metadata_and_order(self) -> None:
        with tarfile.open(self.out / OBJECTS[0] / BUNDLE_NAME, "r") as tar:
            members = tar.getmembers()
        self.assertEqual(
            [
                OBJECT_MANIFEST_NAME,
                "frames/000.jpg",
                "frames/001.jpg",
                "frames/002.jpg",
                "frames/003.jpg",
            ],
            [m.name for m in members],
        )
        for member in members:
            self.assertEqual(0, member.mtime)
            self.assertEqual(0, member.uid)
            self.assertEqual(0, member.gid)
            self.assertEqual("", member.uname)
            self.assertEqual("", member.gname)
            self.assertEqual(0o644, member.mode)
            self.assertTrue(member.isfile())
            self.assertNotIn("..", member.name)
            self.assertFalse(member.name.startswith("/"))

    def test_the_tar_contains_the_exact_jpeg_bytes(self) -> None:
        with tarfile.open(self.out / OBJECTS[0] / BUNDLE_NAME, "r") as tar:
            for index, image in enumerate(_frames(OBJECTS[0])):
                payload = tar.extractfile(f"frames/{index:03d}.jpg").read()
                self.assertEqual(image.jpeg_bytes, payload)

    def test_the_generation_manifest_records_bundle_hashes(self) -> None:
        document = json.loads(
            (
                self.out / "frame-bundle-generation-manifest.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(
            FRAME_BUNDLE_GENERATION_SCHEMA_VERSION,
            document["schema_version"],
        )
        for entry in document["objects"]:
            bundle = self.out / entry["bundle_path"]
            self.assertEqual(
                sha256(bundle.read_bytes()).hexdigest(),
                entry["bundle_sha256"],
            )
            self.assertEqual(
                bundle.stat().st_size, entry["bundle_size_bytes"]
            )
            manifest = self.out / entry["object_manifest_path"]
            self.assertEqual(
                sha256(manifest.read_bytes()).hexdigest(),
                entry["object_manifest_sha256"],
            )

    def test_the_tar_hash_is_not_self_referential(self) -> None:
        document = json.loads(
            (self.out / OBJECTS[0] / OBJECT_MANIFEST_NAME).read_text(
                encoding="utf-8"
            )
        )
        rendered = json.dumps(document)
        self.assertNotIn("bundle_sha256", rendered)
        self.assertNotIn("tar", rendered.lower().replace("statement", ""))

    def test_checksums_cover_every_file_and_verify(self) -> None:
        lines = (self.out / CHECKSUM_NAME).read_text(
            encoding="utf-8"
        ).splitlines()
        listed = {line.split("  ", 1)[1] for line in lines}
        actual = {
            p.relative_to(self.out).as_posix()
            for p in self.out.rglob("*")
            if p.is_file() and p.name != CHECKSUM_NAME
        }
        self.assertEqual(actual, listed)
        self.assertEqual(sorted(listed), [ln.split("  ",1)[1] for ln in lines])
        for line in lines:
            digest, name = line.split("  ", 1)
            self.assertEqual(
                sha256((self.out / name).read_bytes()).hexdigest(), digest
            )

    def test_no_absolute_path_or_timestamp_in_checksummed_output(
        self,
    ) -> None:
        for path in self.out.rglob("*"):
            if not path.is_file() or path.suffix == ".jpg":
                continue
            if path.name == BUNDLE_NAME:
                continue
            content = path.read_text(encoding="utf-8")
            self.assertNotIn(str(self.root), content, path.name)
            self.assertNotIn("/home/", content, path.name)
            self.assertNotIn("created_at", content, path.name)
            self.assertNotIn("generated_at", content, path.name)

    def test_safety_flags_and_the_non_claim_are_recorded(self) -> None:
        for path in (
            self.out / "frame-bundle-generation-manifest.json",
            self.out / OBJECTS[0] / OBJECT_MANIFEST_NAME,
        ):
            document = json.loads(path.read_text(encoding="utf-8"))
            self.assertFalse(document["llm_called"])
            self.assertFalse(document["credentials_recorded"])
            self.assertFalse(document["network_calls_performed"])
            self.assertFalse(
                document["historical_visual_bytes_retained"]
            )
            self.assertFalse(
                document[
                    "claims_byte_identity_with_historical_visual_input"
                ]
            )
            statement = document["sampling_alignment_statement"]
            self.assertIn("aligned with the frozen sampling metadata",
                          statement)
            self.assertIn("were not retained", statement)
            self.assertNotIn(
                "identical to the historical visual input", statement
            )
        self.assertFalse(self.result["llm_called"])

    def test_the_object_manifest_schema_and_provenance(self) -> None:
        document = json.loads(
            (self.out / OBJECTS[0] / OBJECT_MANIFEST_NAME).read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(
            FRAME_BUNDLE_SCHEMA_VERSION, document["schema_version"]
        )
        for key in (
            "object_id", "source_video_id", "source_video_filename",
            "source_video_size_bytes", "source_video_sha256",
            "source_duration_seconds", "sampling",
            "source_frame_descriptions", "generation_manifest_sha256",
            "frames", "total_jpeg_bytes", "software_versions",
        ):
            self.assertIn(key, document)
        self.assertEqual(82, document["sampling"]["jpeg_quality"])
        self.assertTrue(document["sampling"]["jpeg_optimize"])
        self.assertEqual(
            "uniform-midpoint", document["sampling"]["method"]
        )
        self.assertEqual(
            "sampled_frames",
            document["source_frame_descriptions"]["representation_id"],
        )

    def test_sampled_frames_is_not_replaced(self) -> None:
        document = json.loads(
            (
                self.out / "frame-bundle-generation-manifest.json"
            ).read_text(encoding="utf-8")
        )
        self.assertFalse(
            document["replaces_sampled_frames_representation"]
        )
        self.assertEqual(REPRESENTATION_ID, document["representation_id"])
        self.assertNotEqual(REPRESENTATION_ID, "sampled_frames")


class DeterminismTest(unittest.TestCase):
    def test_two_independent_output_trees_are_byte_identical(self) -> None:
        trees = []
        for suffix in ("alpha", "beta"):
            with tempfile.TemporaryDirectory(suffix=suffix) as temporary:
                root = Path(temporary) / suffix / "nested"
                root.mkdir(parents=True)
                videos, representations, manifest = build_inputs(root)
                build_frame_bundles(
                    video_dir=videos,
                    representation_dir=representations,
                    generation_manifest=manifest,
                    output_dir=root / "out",
                    sampler=FakeSampler(),
                )
                trees.append({
                    p.relative_to(root / "out").as_posix(): p.read_bytes()
                    for p in (root / "out").rglob("*") if p.is_file()
                })
        self.assertEqual(sorted(trees[0]), sorted(trees[1]))
        for name in trees[0]:
            self.assertEqual(trees[0][name], trees[1][name], name)


class ValidationTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _build(self, name: str, **kwargs: Any):
        target = self.root / name
        target.mkdir(parents=True, exist_ok=True)
        sampler = kwargs.pop("sampler", None) or FakeSampler()
        videos, representations, manifest = build_inputs(target, **kwargs)
        return build_frame_bundles(
            video_dir=videos,
            representation_dir=representations,
            generation_manifest=manifest,
            output_dir=target / "out",
            sampler=sampler,
        )

    def test_a_source_video_checksum_mismatch_is_refused(self) -> None:
        with self.assertRaisesRegex(FrameBundleError, "source video SHA-256"):
            self._build("videosha", video_sha_override="f" * 64)

    def test_a_description_checksum_mismatch_is_refused(self) -> None:
        with self.assertRaisesRegex(FrameBundleError, "SHA-256 disagrees"):
            self._build("descsha", description_sha_override="e" * 64)

    def test_a_duplicate_object_is_refused(self) -> None:
        with self.assertRaisesRegex(FrameBundleError, "duplicate object"):
            self._build("dupe", duplicate_object=True)

    def test_a_video_path_traversal_is_refused(self) -> None:
        # A video filename is guarded twice: it must be a bare name, and it
        # must resolve inside the declared root. Either refusal is correct.
        with self.assertRaisesRegex(
            FrameBundleError, "bare name|traverse upwards|escapes"
        ):
            self._build("trav1", traversal_video=True)

    def test_the_containment_guard_rejects_traversal_directly(self) -> None:
        from pathfinder.frame_bundle import _contained

        root = self.root / "contain"
        root.mkdir(parents=True)
        for candidate in ("../outside.json", "a/../../outside.json"):
            with self.subTest(candidate=candidate):
                with self.assertRaisesRegex(
                    FrameBundleError, "traverse upwards"
                ):
                    _contained(root, candidate, "path")
        with self.assertRaisesRegex(FrameBundleError, "must be relative"):
            _contained(root, "/etc/passwd", "path")
        with self.assertRaisesRegex(FrameBundleError, "non-empty"):
            _contained(root, "   ", "path")
        inside = _contained(root, "a/b.json", "path")
        self.assertTrue(str(inside).startswith(str(root.resolve())))

    def test_a_representation_path_traversal_is_refused(self) -> None:
        with self.assertRaisesRegex(FrameBundleError, "traverse upwards"):
            self._build("trav2", traversal_representation=True)

    def test_a_frame_count_mismatch_is_refused(self) -> None:
        with self.assertRaisesRegex(
            FrameBundleError, "disagree with the generation manifest"
        ):
            self._build("count", frame_count_override=3)

    def test_a_timestamp_mismatch_is_refused(self) -> None:
        with self.assertRaisesRegex(
            FrameBundleError, "timestamp_seconds is"
        ):
            self._build("stamp", timestamp_override=99.0)

    def test_a_width_mismatch_is_refused(self) -> None:
        with self.assertRaisesRegex(FrameBundleError, "width is"):
            self._build("width", width_override=123)

    def test_a_resampled_height_mismatch_is_refused(self) -> None:
        def shrink(frames):
            first = frames[0]
            return [
                SampledImage(
                    first.frame_index, first.timestamp_seconds,
                    first.width, first.height + 1, first.jpeg_bytes,
                ),
                *frames[1:],
            ]

        with self.assertRaisesRegex(FrameBundleError, "height is"):
            self._build(
                "height",
                sampler=FakeSampler({OBJECTS[0]: shrink}),
            )

    def test_a_short_resample_is_refused(self) -> None:
        with self.assertRaisesRegex(
            FrameBundleError, "resampling produced"
        ):
            self._build(
                "short",
                sampler=FakeSampler({OBJECTS[0]: lambda f: f[:-1]}),
            )

    def test_a_missing_sampling_field_is_refused(self) -> None:
        for field in ("frame_count", "jpeg_max_dimension", "method"):
            with self.subTest(field=field):
                with self.assertRaisesRegex(
                    FrameBundleError, "has no default|is required"
                ):
                    self._build(
                        f"drop-{field}", drop_sampling_field=field
                    )

    def test_an_unsupported_sampling_method_is_refused(self) -> None:
        with self.assertRaisesRegex(
            FrameBundleError, "unsupported sampling method"
        ):
            self._build("method", method_override="keyframe")

    def test_an_existing_output_directory_is_refused(self) -> None:
        target = self.root / "exists"
        target.mkdir(parents=True)
        videos, representations, manifest = build_inputs(target)
        (target / "out").mkdir()
        with self.assertRaisesRegex(
            FrameBundleError, "output directory already exists"
        ):
            build_frame_bundles(
                video_dir=videos,
                representation_dir=representations,
                generation_manifest=manifest,
                output_dir=target / "out",
                sampler=FakeSampler(),
            )

    def test_a_failure_leaves_no_partial_output(self) -> None:
        target = self.root / "partial"
        target.mkdir(parents=True)
        videos, representations, manifest = build_inputs(target)
        with self.assertRaises(FrameBundleError):
            build_frame_bundles(
                video_dir=videos,
                representation_dir=representations,
                generation_manifest=manifest,
                output_dir=target / "out",
                sampler=FakeSampler(
                    {OBJECTS[1]: lambda f: f[:-1]}
                ),
            )
        self.assertFalse((target / "out").exists())
        leftovers = [
            p for p in target.iterdir()
            if p.name.startswith(".pathfinder-frame-bundle-")
        ]
        self.assertEqual([], leftovers)

    def test_an_operator_directory_is_never_deleted(self) -> None:
        target = self.root / "keep"
        target.mkdir(parents=True)
        videos, representations, manifest = build_inputs(target)
        existing = target / "out"
        existing.mkdir()
        (existing / "operator.txt").write_text("keep me", encoding="utf-8")
        with self.assertRaises(FrameBundleError):
            build_frame_bundles(
                video_dir=videos,
                representation_dir=representations,
                generation_manifest=manifest,
                output_dir=existing,
                sampler=FakeSampler(),
            )
        self.assertEqual(
            "keep me",
            (existing / "operator.txt").read_text(encoding="utf-8"),
        )

    def test_a_malformed_generation_manifest_is_refused(self) -> None:
        target = self.root / "malformed"
        target.mkdir(parents=True)
        videos, representations, manifest = build_inputs(target)
        manifest.write_text("{not json", encoding="utf-8")
        with self.assertRaisesRegex(FrameBundleError, "malformed"):
            build_frame_bundles(
                video_dir=videos,
                representation_dir=representations,
                generation_manifest=manifest,
                output_dir=target / "out",
                sampler=FakeSampler(),
            )

    def test_an_unexpected_requested_object_is_refused(self) -> None:
        target = self.root / "unexpected"
        target.mkdir(parents=True)
        videos, representations, manifest = build_inputs(target)
        with self.assertRaisesRegex(FrameBundleError, "not in the"):
            build_frame_bundles(
                video_dir=videos,
                representation_dir=representations,
                generation_manifest=manifest,
                output_dir=target / "out",
                sampler=FakeSampler(),
                object_ids=["nextqa-val-9999999999"],
            )

    def test_the_manifest_can_be_discovered_in_the_representation_dir(
        self,
    ) -> None:
        target = self.root / "discover"
        target.mkdir(parents=True)
        videos, representations, _ = build_inputs(target)
        result = build_frame_bundles(
            video_dir=videos,
            representation_dir=representations,
            output_dir=target / "out",
            sampler=FakeSampler(),
        )
        self.assertEqual("COMPLETE", result["status"])


if __name__ == "__main__":
    unittest.main()
