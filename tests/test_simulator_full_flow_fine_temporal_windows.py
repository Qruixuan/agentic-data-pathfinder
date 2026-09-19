"""Focused coverage for generic fine-grained overlapping temporal windows."""

from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from pathfinder.simulator.full_flow_fine_temporal_windows import (
    CAPTION_PROMPT,
    CAPTION_PROMPT_SHA256,
    MAX_WINDOWS,
    SEGMENTATION_POLICY_ID,
    FineWindowError,
    assert_caption_request_is_question_independent,
    build_windows,
    caption_search_text,
    freeze_window_package,
    validate_structured_caption,
    window_geometry,
)

SRC = "c" * 64


def _frames(count: int, duration: float):
    step = duration / count
    return [
        {
            "timestamp_seconds": round(step * (2 * i + 1) / 2, 6),
            "jpeg_sha256": hashlib.sha256(f"f{i}".encode()).hexdigest(),
        }
        for i in range(count)
    ]


def _caption(**overrides):
    base = {
        "subjects": ["a person"],
        "subject_actions": ["walks forward"],
        "objects_interacted_with": ["a door"],
        "camera_relation": "moves closer to the camera",
        "initial_visible_state": "standing at the far side",
        "final_visible_state": "close to the lens",
        "observable_transition": "moves from far to near across the frames",
        "uncertainty": "face partly occluded",
    }
    base.update(overrides)
    return base


class WindowConstructionTest(unittest.TestCase):
    def test_geometry_is_duration_based_and_overlapping(self) -> None:
        window, stride = window_geometry(20.0)
        self.assertAlmostEqual(4.0, window)
        self.assertAlmostEqual(2.0, stride)
        # Overlap means the stride is strictly shorter than the window.
        self.assertLess(stride, window)

    def test_windows_are_deterministic_and_capped(self) -> None:
        frames = _frames(16, 15.6)
        first = build_windows(
            object_id="obj-x", duration_seconds=15.6, frames=frames,
            source_video_sha256=SRC, source_video_size_bytes=100,
        )
        again = build_windows(
            object_id="obj-x", duration_seconds=15.6, frames=frames,
            source_video_sha256=SRC, source_video_size_bytes=100,
        )
        self.assertEqual(first, again)
        self.assertGreater(len(first), 1)
        self.assertLessEqual(len(first), MAX_WINDOWS)
        self.assertEqual(list(range(len(first))), [w["ordinal"] for w in first])

    def test_windows_overlap_and_cover_the_tail(self) -> None:
        windows = build_windows(
            object_id="obj-x", duration_seconds=15.6, frames=_frames(16, 15.6),
            source_video_sha256=SRC, source_video_size_bytes=100,
        )
        for earlier, later in zip(windows, windows[1:]):
            self.assertLess(later["start_seconds"], earlier["end_seconds"])
        self.assertAlmostEqual(15.6, max(w["end_seconds"] for w in windows), places=3)

    def test_every_window_binds_source_and_exact_frames(self) -> None:
        frames = _frames(16, 15.6)
        by_sha = {f["jpeg_sha256"] for f in frames}
        for window in build_windows(
            object_id="obj-x", duration_seconds=15.6, frames=frames,
            source_video_sha256=SRC, source_video_size_bytes=100,
        ):
            self.assertEqual(SRC, window["source_video_sha256"])
            self.assertEqual(SEGMENTATION_POLICY_ID, window["segmentation_policy_id"])
            self.assertEqual(window["frame_count"], len(window["frame_sha256"]))
            self.assertTrue(set(window["frame_sha256"]) <= by_sha)
            for timestamp in window["frame_timestamps_seconds"]:
                self.assertGreaterEqual(timestamp, window["start_seconds"])
                self.assertLessEqual(timestamp, window["end_seconds"])

    def test_too_few_frames_cannot_form_an_index(self) -> None:
        with self.assertRaisesRegex(FineWindowError, "at least two windows"):
            build_windows(
                object_id="obj-x", duration_seconds=10.0, frames=_frames(1, 10.0),
                source_video_sha256=SRC, source_video_size_bytes=100,
            )

    def test_different_durations_yield_different_windowing(self) -> None:
        short = build_windows(
            object_id="obj-a", duration_seconds=10.0, frames=_frames(16, 10.0),
            source_video_sha256=SRC, source_video_size_bytes=100,
        )
        long = build_windows(
            object_id="obj-b", duration_seconds=40.0, frames=_frames(16, 40.0),
            source_video_sha256=SRC, source_video_size_bytes=100,
        )
        self.assertNotEqual(short[0]["end_seconds"], long[0]["end_seconds"])


class CaptionContractTest(unittest.TestCase):
    def test_prompt_is_single_and_question_independent(self) -> None:
        self.assertEqual(
            hashlib.sha256(CAPTION_PROMPT.encode()).hexdigest(), CAPTION_PROMPT_SHA256
        )
        lowered = CAPTION_PROMPT.lower()
        for banned in ("option", "correct", "task_success", "answer the"):
            self.assertNotIn(banned, lowered)
        # It must steer toward action rather than framing alone.
        self.assertIn("motion", lowered)
        self.assertIn("do not guess intent", lowered)

    def test_caption_request_rejects_task_material(self) -> None:
        for bad in (
            {"question": "what happened"},
            {"messages": [{"answer_options": ["A"]}]},
            {"meta": {"task_success": True}},
            {"meta": {"predicted_answer": "C"}},
        ):
            with self.subTest(payload=bad):
                with self.assertRaisesRegex(FineWindowError, "forbidden field"):
                    assert_caption_request_is_question_independent(bad)

    def test_clean_request_is_accepted(self) -> None:
        assert_caption_request_is_question_independent(
            {"model": "m", "messages": [{"role": "user", "content": "frames"}]}
        )

    def test_structured_caption_validation(self) -> None:
        validated = validate_structured_caption(_caption())
        self.assertEqual(["walks forward"], validated["subject_actions"])
        with self.assertRaisesRegex(FineWindowError, "fields changed"):
            validate_structured_caption({"subjects": ["x"]})
        with self.assertRaisesRegex(FineWindowError, "must be a list"):
            validate_structured_caption(_caption(subjects="not a list"))
        with self.assertRaisesRegex(FineWindowError, "action or transition"):
            validate_structured_caption(
                _caption(subject_actions=[], observable_transition="")
            )

    def test_search_text_leads_with_action_not_framing(self) -> None:
        text = caption_search_text(_caption())
        self.assertTrue(text.startswith("walks forward"))
        # Framing is present but last, so motion dominates the vector.
        self.assertLess(text.index("walks forward"), text.index("moves closer"))


class PackageTest(unittest.TestCase):
    def test_window_package_freezes_before_captions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            windows = build_windows(
                object_id="obj-x", duration_seconds=15.6, frames=_frames(16, 15.6),
                source_video_sha256=SRC, source_video_size_bytes=100,
            )
            package = freeze_window_package(
                package_id="pilot", windows=windows,
                source_bindings={"source_video_sha256": SRC},
                output_dir=Path(tmp) / "pkg",
            )
            self.assertFalse(package["captions_materialized"])
            self.assertEqual(CAPTION_PROMPT_SHA256, package["caption_prompt_sha256"])
            root = Path(tmp) / "pkg"
            for line in (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
                expected, _, name = line.partition("  ")
                actual = hashlib.sha256((root / name).read_bytes()).hexdigest()
                self.assertEqual(expected, actual)

    def test_package_refuses_to_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            windows = build_windows(
                object_id="obj-x", duration_seconds=15.6, frames=_frames(16, 15.6),
                source_video_sha256=SRC, source_video_size_bytes=100,
            )
            freeze_window_package(
                package_id="pilot", windows=windows, source_bindings={},
                output_dir=Path(tmp) / "pkg",
            )
            with self.assertRaisesRegex(FineWindowError, "already exists"):
                freeze_window_package(
                    package_id="pilot", windows=windows, source_bindings={},
                    output_dir=Path(tmp) / "pkg",
                )


class SourceHygieneTest(unittest.TestCase):
    def test_no_visible_set_literals_and_no_control_bytes(self) -> None:
        import pathfinder.simulator.full_flow_fine_temporal_windows as module

        raw = Path(module.__file__).read_bytes()
        stray = {b for b in raw if b < 0x20 and b not in (0x09, 0x0A, 0x0D)}
        self.assertEqual(set(), stray, f"stray control bytes: {sorted(stray)}")
        source = raw.decode("utf-8")
        for forbidden in (
            "nextqa", "3429509208", "2435100235", "2461993294", "4010069381",
            "smoke-temporal", "baby", "vacuum", "13.68", "9.76",
        ):
            self.assertNotIn(forbidden, source, f"hard-coded token {forbidden!r}")


if __name__ == "__main__":
    unittest.main()


class CaptionShapeNormalizationTest(unittest.TestCase):
    """Providers vary scalar/list shape; normalize without changing the ask."""

    def test_list_valued_scalar_field_is_joined_deterministically(self) -> None:
        caption = _caption(camera_relation=["far from camera", "then close"])
        first = validate_structured_caption(caption)
        again = validate_structured_caption(caption)
        self.assertEqual("far from camera, then close", first["camera_relation"])
        self.assertEqual(first, again)

    def test_null_scalar_field_becomes_empty_string(self) -> None:
        self.assertEqual("", validate_structured_caption(_caption(uncertainty=None))["uncertainty"])

    def test_non_string_list_items_are_still_rejected(self) -> None:
        with self.assertRaisesRegex(FineWindowError, "only strings"):
            validate_structured_caption(_caption(camera_relation=["ok", 7]))

    def test_normalization_does_not_change_the_prompt(self) -> None:
        # Shape tolerance must never alter what was asked of the model.
        self.assertEqual(
            hashlib.sha256(CAPTION_PROMPT.encode()).hexdigest(), CAPTION_PROMPT_SHA256
        )


class ResumeCacheTest(unittest.TestCase):
    """A cached caption may be reused only when every binding verifies."""

    MODEL = "vision-model-x"
    PROMPT = CAPTION_PROMPT_SHA256
    PKG = "d" * 64

    def _windows(self):
        return build_windows(
            object_id="obj-x", duration_seconds=15.6, frames=_frames(16, 15.6),
            source_video_sha256=SRC, source_video_size_bytes=100,
        )

    def _entry(self, window, **overrides):
        from pathfinder.simulator.full_flow_fine_temporal_windows import (
            cache_entry_bindings,
        )

        entry = dict(cache_entry_bindings(
            window=window, model_id=self.MODEL, prompt_sha256=self.PROMPT,
            segmentation_package_sha256=self.PKG,
        ))
        entry.update({
            "request_input_sha256": "e" * 64,
            "response_sha256": "f" * 64,
            "structured_caption": _caption(),
        })
        entry.update(overrides)
        return entry

    def test_atomic_write_leaves_no_partial_file(self) -> None:
        from pathfinder.simulator.full_flow_fine_temporal_windows import (
            atomic_write_json,
        )

        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "nested" / "00.json"
            digest = atomic_write_json(target, {"a": 1})
            self.assertTrue(target.is_file())
            self.assertEqual(64, len(digest))
            self.assertFalse(list(target.parent.glob("*.partial")))

    def test_remaining_is_derived_from_verified_cache_state(self) -> None:
        from pathfinder.simulator.full_flow_fine_temporal_windows import (
            atomic_write_json, remaining_windows,
        )

        with tempfile.TemporaryDirectory() as tmp:
            windows = self._windows()
            atomic_write_json(Path(tmp) / "00.json", self._entry(windows[0]))
            atomic_write_json(Path(tmp) / "01.json", self._entry(windows[1]))
            cached, todo = remaining_windows(
                windows=windows, cache_dir=tmp, model_id=self.MODEL,
                prompt_sha256=self.PROMPT, segmentation_package_sha256=self.PKG,
            )
            self.assertEqual(2, len(cached))
            self.assertEqual(len(windows) - 2, len(todo))
            self.assertNotIn(0, [w["ordinal"] for w in todo])

    def test_entry_from_another_window_is_refused(self) -> None:
        from pathfinder.simulator.full_flow_fine_temporal_windows import (
            atomic_write_json, remaining_windows,
        )

        with tempfile.TemporaryDirectory() as tmp:
            windows = self._windows()
            # Window 1's caption written under window 0's slot.
            atomic_write_json(Path(tmp) / "00.json", self._entry(windows[1]))
            cached, todo = remaining_windows(
                windows=windows, cache_dir=tmp, model_id=self.MODEL,
                prompt_sha256=self.PROMPT, segmentation_package_sha256=self.PKG,
            )
            self.assertEqual(0, len(cached))
            self.assertIn(0, [w["ordinal"] for w in todo])

    def test_entry_from_another_model_prompt_or_package_is_refused(self) -> None:
        from pathfinder.simulator.full_flow_fine_temporal_windows import (
            verify_cache_entry, cache_entry_bindings,
        )

        window = self._windows()[0]
        expected = cache_entry_bindings(
            window=window, model_id=self.MODEL, prompt_sha256=self.PROMPT,
            segmentation_package_sha256=self.PKG,
        )
        for field, value in (
            ("model_id", "another-model"),
            ("caption_prompt_sha256", "a" * 64),
            ("segmentation_package_sha256", "b" * 64),
            ("window_descriptor_sha256", "c" * 64),
            ("frame_sha256", ["9" * 64]),
        ):
            with self.subTest(field=field):
                with self.assertRaisesRegex(FineWindowError, "does not match"):
                    verify_cache_entry(self._entry(window, **{field: value}), expected)

    def test_corrupt_or_unparseable_entry_is_treated_as_absent(self) -> None:
        from pathfinder.simulator.full_flow_fine_temporal_windows import (
            remaining_windows,
        )

        with tempfile.TemporaryDirectory() as tmp:
            windows = self._windows()
            (Path(tmp) / "00.json").write_text("{not json", encoding="utf-8")
            cached, todo = remaining_windows(
                windows=windows, cache_dir=tmp, model_id=self.MODEL,
                prompt_sha256=self.PROMPT, segmentation_package_sha256=self.PKG,
            )
            self.assertEqual(0, len(cached))
            self.assertEqual(len(windows), len(todo))

    def test_cache_entry_never_carries_credentials(self) -> None:
        entry = self._entry(self._windows()[0])
        blob = json.dumps(entry).casefold()
        for forbidden in ("authorization", "api_key", "bearer", "secret", "password"):
            self.assertNotIn(forbidden, blob)


class JsonExtractionTest(unittest.TestCase):
    """Exactly one complete object, never repaired, never guessed."""

    VALID = '{"a": 1, "b": "x"}'

    def _extract(self, text):
        from pathfinder.simulator.full_flow_fine_temporal_windows import (
            extract_single_json_object,
        )
        return extract_single_json_object(text)

    def _error(self, text):
        from pathfinder.simulator.full_flow_fine_temporal_windows import (
            CaptionParseError, extract_single_json_object,
        )
        with self.assertRaises(CaptionParseError) as caught:
            extract_single_json_object(text)
        return caught.exception

    def test_strict_json(self) -> None:
        self.assertEqual({"a": 1, "b": "x"}, self._extract(self.VALID))
        self.assertEqual({"a": 1, "b": "x"}, self._extract("  \n" + self.VALID + " \n"))

    def test_fenced_json(self) -> None:
        self.assertEqual({"a": 1, "b": "x"}, self._extract(f"```json\n{self.VALID}\n```"))
        self.assertEqual({"a": 1, "b": "x"}, self._extract(f"```\n{self.VALID}\n```"))

    def test_harmless_prose_around_one_object(self) -> None:
        self.assertEqual(
            {"a": 1, "b": "x"},
            self._extract(f"Here is the result:\n{self.VALID}\nHope that helps."),
        )

    def test_braces_and_escaped_quotes_inside_strings(self) -> None:
        # Built via json.dumps so the fixture cannot be mangled by escaping.
        inner = 'a brace { and } a "quote" and a backslash ' + chr(92)
        text = json.dumps({"note": inner})
        self.assertEqual(inner, self._extract(text)["note"])
        # The scanner must also survive a stray brace inside a string when the
        # object is surrounded by prose.
        self.assertEqual(inner, self._extract("before\n" + text + "\nafter")["note"])

    def test_missing_comma_is_rejected(self) -> None:
        self.assertIn(self._error('{"a": 1 "b": 2}').stage, {"strict", "scan"})

    def test_truncated_object_is_rejected(self) -> None:
        error = self._error('{"a": 1, "b": "unterminated')
        self.assertEqual("scan", error.stage)
        self.assertIn("no complete top-level JSON object", str(error))

    def test_token_limit_truncation_is_rejected(self) -> None:
        # Output cut mid-value by a token limit must never be repaired.
        error = self._error('{"subjects": ["a child"], "subject_actions": ["walks')
        self.assertEqual("scan", error.stage)

    def test_multiple_objects_are_rejected(self) -> None:
        error = self._error(f"{self.VALID}\n{self.VALID}")
        self.assertEqual("scan", error.stage)
        self.assertIn("2 top-level objects", str(error))

    def test_malformed_escape_is_rejected(self) -> None:
        # A literal backslash-q is not a valid JSON escape.
        self._error('{"a": "bad ' + chr(92) + 'q escape"}')

    def test_non_finite_values_are_rejected(self) -> None:
        self._error('{"a": NaN}')

    def test_non_object_json_is_rejected(self) -> None:
        self._error("[1, 2, 3]")

    def test_empty_content_is_rejected(self) -> None:
        from pathfinder.simulator.full_flow_fine_temporal_windows import (
            FineWindowError, extract_single_json_object,
        )
        with self.assertRaises(FineWindowError):
            extract_single_json_object("   ")

    def test_nothing_is_repaired(self) -> None:
        # Each of these is a repair the parser must refuse to perform.
        for broken in (
            '{"a": 1,}',                 # trailing comma
            '{a: 1}',                    # unquoted key
            "{'a': 1}",                  # single quotes
            '{"a": 1',                   # unclosed
        ):
            with self.subTest(broken=broken):
                self._error(broken)


class RawResponseRecordTest(unittest.TestCase):
    def _window(self):
        return build_windows(
            object_id="obj-x", duration_seconds=15.6, frames=_frames(16, 15.6),
            source_video_sha256=SRC, source_video_size_bytes=100,
        )[0]

    def _record(self, content="{}", **doc):
        from pathfinder.simulator.full_flow_fine_temporal_windows import (
            build_raw_response_record,
        )
        document = {
            "model": "m",
            "choices": [{"finish_reason": "stop", "message": {"content": content}}],
            "usage": {"total_tokens": 7},
        }
        document.update(doc)
        return build_raw_response_record(
            window=self._window(), model_id="m",
            prompt_sha256=CAPTION_PROMPT_SHA256, segmentation_package_sha256="d" * 64,
            request_input_sha256="e" * 64, response_bytes=b"{}", document=document,
        )

    def test_record_captures_diagnosis_fields(self) -> None:
        record = self._record(content='{"a": 1}')
        self.assertEqual("stop", record["finish_reason"])
        self.assertEqual({"total_tokens": 7}, record["usage"])
        self.assertEqual('{"a": 1}', record["raw_content"])
        self.assertEqual(8, record["raw_content_length"])
        self.assertTrue(record["raw_content_sha256"])
        self.assertFalse(record["credentials_recorded"])

    def test_record_carries_no_credential_fields(self) -> None:
        blob = json.dumps(self._record()).casefold()
        for forbidden in ("authorization", "api_key", "bearer", "secret", "base_url"):
            self.assertNotIn(forbidden, blob)

    def test_reasoning_is_retained_only_as_digest(self) -> None:
        record = self._record(
            choices=[{"finish_reason": "length",
                      "message": {"content": "{}", "reasoning_content": "long chain"}}],
        )
        self.assertEqual("length", record["finish_reason"])
        self.assertNotIn("long chain", json.dumps(record))
        self.assertEqual(10, record["reasoning_content_length"])

    def test_persisted_raw_content_can_be_reparsed_offline(self) -> None:
        from pathfinder.simulator.full_flow_fine_temporal_windows import (
            extract_single_json_object,
        )
        # A stored raw response is re-parsable with no provider call.
        record = self._record(content='prose\n{"a": 1}\nmore prose')
        self.assertEqual({"a": 1}, extract_single_json_object(record["raw_content"]))
