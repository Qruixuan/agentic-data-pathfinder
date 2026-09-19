"""Focused coverage for frozen, deterministic temporal embeddings."""

from __future__ import annotations

import hashlib
import json
import math
import tempfile
import unittest
from pathlib import Path

from pathfinder.simulator.full_flow_temporal_embeddings import (
    EMBEDDING_PACKAGE_SCHEMA_VERSION,
    PACKAGE_NAME,
    QUANT_SCALE,
    VECTOR_POLICY_ID,
    VECTORS_NAME,
    TemporalEmbeddingError,
    anchor_confidence,
    build_embedding_package,
    integer_similarity,
    load_embedding_package,
    normalize_and_quantize,
    rank_segments_semantic,
)

DIM = 4
SHA = "b" * 64


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _caption(obj, ordinal, vector, text=None):
    return {
        "object_id": obj,
        "segment_id": f"{obj}#seg{ordinal:02d}",
        "segment_ordinal": ordinal,
        "text": text or f"caption {obj} {ordinal}",
        "vector": vector,
        "request_sha256": SHA,
        "response_sha256": SHA,
    }


def _question(obj, vector, text="a public question"):
    return {
        "object_id": obj,
        "text": text,
        "task_binding_sha256": SHA,
        "vector": vector,
        "request_sha256": SHA,
        "response_sha256": SHA,
    }


def _build(tmp, captions=None, questions=None, dimension=DIM):
    return build_embedding_package(
        package_id="pkg-test",
        model_id="text-embedding-v4",
        dimension=dimension,
        caption_records=captions or [
            _caption("obj-a", 0, [1.0, 0.0, 0.0, 0.0]),
            _caption("obj-a", 1, [0.0, 1.0, 0.0, 0.0]),
        ],
        question_records=questions or [_question("obj-a", [0.9, 0.1, 0.0, 0.0])],
        source_bindings={"public_task_set_sha256": SHA},
        output_dir=Path(tmp) / "pkg",
    )


class QuantizationTest(unittest.TestCase):
    def test_normalization_and_quantization_are_deterministic(self) -> None:
        raw = [3.0, 4.0, 0.0, 0.0]
        first = normalize_and_quantize(raw, dimension=DIM)
        again = normalize_and_quantize(raw, dimension=DIM)
        self.assertEqual(first, again)
        # 3-4-5 triangle: components normalize to 0.6 and 0.8 exactly.
        self.assertEqual((19660, 26214, 0, 0), first)
        self.assertTrue(all(type(v) is int for v in first))

    def test_scale_is_unchanged_and_declared(self) -> None:
        self.assertEqual(32767, QUANT_SCALE)
        self.assertIn("int16", VECTOR_POLICY_ID)

    def test_magnitude_does_not_change_direction(self) -> None:
        small = normalize_and_quantize([0.003, 0.004, 0.0, 0.0], dimension=DIM)
        large = normalize_and_quantize([3000.0, 4000.0, 0.0, 0.0], dimension=DIM)
        self.assertEqual(small, large)

    def test_invalid_vectors_are_rejected(self) -> None:
        for bad, label in (
            ([0.0, 0.0, 0.0, 0.0], "zero norm"),
            ([float("nan"), 1.0, 0.0, 0.0], "nan"),
            ([float("inf"), 1.0, 0.0, 0.0], "inf"),
            ([1.0, 0.0, 0.0], "short"),
            ([1.0, 0.0, 0.0, 0.0, 0.0], "long"),
        ):
            with self.subTest(vector=label):
                with self.assertRaises(TemporalEmbeddingError):
                    normalize_and_quantize(bad, dimension=DIM)

    def test_integer_similarity_is_exact_and_symmetric(self) -> None:
        a = normalize_and_quantize([1.0, 0.0, 0.0, 0.0], dimension=DIM)
        b = normalize_and_quantize([1.0, 0.0, 0.0, 0.0], dimension=DIM)
        c = normalize_and_quantize([0.0, 1.0, 0.0, 0.0], dimension=DIM)
        self.assertEqual(QUANT_SCALE**2, integer_similarity(a, b))
        self.assertEqual(0, integer_similarity(a, c))
        self.assertEqual(integer_similarity(a, c), integer_similarity(c, a))


class PackageTest(unittest.TestCase):
    def test_package_round_trips_byte_identically(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            built = _build(tmp)
            package, rows = load_embedding_package(Path(tmp) / "pkg")
            self.assertEqual(EMBEDDING_PACKAGE_SCHEMA_VERSION, package["schema_version"])
            self.assertEqual(built["package_sha256"], package["package_sha256"])
            self.assertEqual(2, package["segment_caption_count"])
            self.assertEqual(1, package["public_question_count"])
            self.assertFalse(package["runtime_embedding_calls_required"])
            # Ranking is stable across a serialization round trip.
            question = [r for r in rows if r["kind"] == "public_question"][0]
            segments = [r for r in rows if r["kind"] == "segment_caption"]
            first = rank_segments_semantic(question_vector=question["vector"], segment_vectors=segments)
            reloaded = json.loads(json.dumps(segments))
            second = rank_segments_semantic(question_vector=question["vector"], segment_vectors=reloaded)
            self.assertEqual(first, second)

    def test_text_is_never_stored_only_digested(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            secret_ish = "a distinctive caption phrase"
            _build(tmp, captions=[
                _caption("obj-a", 0, [1.0, 0.0, 0.0, 0.0], text=secret_ish),
                _caption("obj-a", 1, [0.0, 1.0, 0.0, 0.0]),
            ])
            blob = (Path(tmp) / "pkg" / VECTORS_NAME).read_text(encoding="utf-8")
            self.assertNotIn(secret_ish, blob)
            self.assertIn(_sha(secret_ish), blob)

    def test_tampered_package_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _build(tmp)
            target = Path(tmp) / "pkg" / VECTORS_NAME
            rows = [json.loads(l) for l in target.read_text(encoding="utf-8").splitlines() if l.strip()]
            rows[0]["vector"] = [0] * DIM
            target.write_bytes(b"".join(
                json.dumps(r, sort_keys=True, separators=(",", ":")).encode() + b"\n" for r in rows
            ))
            with self.assertRaises(TemporalEmbeddingError):
                load_embedding_package(Path(tmp) / "pkg")

    def test_wrong_model_or_dimension_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _build(tmp)
            path = Path(tmp) / "pkg" / PACKAGE_NAME
            package = json.loads(path.read_text(encoding="utf-8"))
            package["model_id"] = "some-other-model"
            path.write_bytes(json.dumps(package, sort_keys=True, separators=(",", ":")).encode())
            with self.assertRaises(TemporalEmbeddingError):
                load_embedding_package(Path(tmp) / "pkg")

    def test_forbidden_fields_cannot_enter_the_package(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            bad = _caption("obj-a", 0, [1.0, 0.0, 0.0, 0.0])
            bad["correct_answer_id"] = "X"
            with self.assertRaisesRegex(TemporalEmbeddingError, "forbidden field"):
                _build(tmp, captions=[bad, _caption("obj-a", 1, [0.0, 1.0, 0.0, 0.0])])

    def test_package_declares_no_outcomes_or_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            package = _build(tmp)
            self.assertFalse(package["credentials_recorded"])
            self.assertFalse(package["hidden_label_values_included"])
            self.assertFalse(package["task_outcomes_included"])
            self.assertFalse(package["eligible_for_scientific_claims"])


class RankingConfidenceTest(unittest.TestCase):
    @staticmethod
    def _segments(vectors):
        return [
            {
                "segment_id": f"obj#seg{i:02d}",
                "segment_ordinal": i,
                "vector": list(normalize_and_quantize(v, dimension=DIM)),
            }
            for i, v in enumerate(vectors)
        ]

    def test_multiple_segments_are_genuinely_ranked(self) -> None:
        question = list(normalize_and_quantize([1.0, 0.2, 0.0, 0.0], dimension=DIM))
        ranked = rank_segments_semantic(
            question_vector=question,
            segment_vectors=self._segments([
                [0.0, 0.0, 1.0, 0.0], [1.0, 0.0, 0.0, 0.0], [0.0, 1.0, 0.0, 0.0],
            ]),
        )
        self.assertEqual(3, len(ranked))
        self.assertEqual("obj#seg01", ranked[0]["segment_id"])
        self.assertGreater(ranked[0]["similarity_score_units"], ranked[1]["similarity_score_units"])

    def test_exact_tie_breaks_on_lowest_ordinal_and_is_low_confidence(self) -> None:
        question = list(normalize_and_quantize([1.0, 0.0, 0.0, 0.0], dimension=DIM))
        ranked = rank_segments_semantic(
            question_vector=question,
            segment_vectors=self._segments([[1.0, 0.0, 0.0, 0.0], [1.0, 0.0, 0.0, 0.0]]),
        )
        self.assertEqual("obj#seg00", ranked[0]["segment_id"])
        confidence = anchor_confidence(ranked)
        self.assertEqual(0, confidence["score_margin_units"])
        # A tie must surface as not-decisive rather than a silent pick.
        self.assertFalse(confidence["semantic_anchor_decisive"])

    def test_clear_winner_is_decisive(self) -> None:
        question = list(normalize_and_quantize([1.0, 0.0, 0.0, 0.0], dimension=DIM))
        ranked = rank_segments_semantic(
            question_vector=question,
            segment_vectors=self._segments([[1.0, 0.0, 0.0, 0.0], [0.0, 1.0, 0.0, 0.0]]),
        )
        confidence = anchor_confidence(ranked)
        self.assertTrue(confidence["semantic_anchor_decisive"])
        self.assertGreater(confidence["score_margin_units"], 0)


class NoHardCodingTest(unittest.TestCase):
    def test_source_has_no_visible_set_literals(self) -> None:
        import pathfinder.simulator.full_flow_temporal_embeddings as module

        source = Path(module.__file__).read_text(encoding="utf-8")
        for forbidden in (
            "nextqa", "3429509208", "2435100235", "2461993294", "4010069381",
            "smoke-temporal", "baby", "child", "vacuum", "camera",
        ):
            self.assertNotIn(forbidden, source, f"hard-coded token {forbidden!r}")


if __name__ == "__main__":
    unittest.main()
