"""Query-aware temporal index over segments of an already-known video.

The workload identifies its target video, so object-level retrieval cannot
affect anything.  What a policy can legitimately choose is *where in that video
to look*.  This module indexes each object as multiple independently
addressable temporal segments carrying question-independent caption content,
searches those segments with the public question, applies the question's
temporal relation, and returns exact content-bound selections.

It is deliberately not unknown-video retrieval, and it is not a fixed window: a
fixed window would ignore the query entirely and is kept elsewhere only under
the explicit name ``fixed-middle-window-projection-v1``.

Nothing here consumes a hidden label, an oracle answer, a credential or a task
outcome, and no object, workload, question or timestamp is hard-coded.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

TEMPORAL_INDEX_SCHEMA_VERSION = "pathfinder.full-flow-temporal-index/v1alpha1"
TEMPORAL_SELECTION_SCHEMA_VERSION = (
    "pathfinder.full-flow-temporal-index-selection/v1alpha1"
)
TEMPORAL_INDEX_ALGORITHM = "segment-caption-idf-overlap-v1"
TEMPORAL_INDEX_TOKENIZER = "unicode-nfkc-alphanumeric-casefold-v1"

# Relation cue tokens.  These only choose which segments around an
# already-retrieved anchor to take; they never select a time range directly and
# are removed before content matching so they cannot bias the anchor.
RELATION_CUES: dict[str, tuple[str, ...]] = {
    "following": ("after", "afterwards", "afterward", "then", "next", "subsequently"),
    "preceding": ("before", "prior", "earlier", "previously"),
    "during": ("during", "while", "as", "when", "whilst"),
    "start": ("start", "starts", "started", "begin", "begins", "beginning", "first", "initially"),
    "end": ("end", "ends", "ended", "finally", "last", "conclusion"),
}
# Checked in this order so that an explicit relative cue wins over a weaker
# positional cue when a question contains both.
_RELATION_PRIORITY = ("following", "preceding", "during", "start", "end")

# Generic interrogative / functional words carry no segment-discriminating
# content.  This list is question-shape based, never task specific.
_STOPWORDS = frozenset({
    "what", "who", "whom", "whose", "which", "where", "why", "how", "did", "do",
    "does", "done", "is", "are", "was", "were", "be", "been", "being", "the",
    "a", "an", "of", "to", "in", "on", "at", "for", "with", "and", "or", "but",
    "he", "she", "it", "they", "them", "his", "her", "its", "their", "this",
    "that", "these", "those", "there", "here", "happen", "happens", "happened",
    "happening", "near",
})

_TOKEN = re.compile(r"[0-9a-z]+")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_TIMELINE_ENTRY = re.compile(
    r"^-\s*\[(?P<start>\d+(?:\.\d+)?)s(?:\s*-\s*(?P<end>\d+(?:\.\d+)?)s)?\]\s*(?P<text>.+?)\s*$"
)
_QUESTION_INDEPENDENT_MARKER = "QUESTION-INDEPENDENT"


class TemporalIndexError(ValueError):
    """Raised before an unbound index, query or selection can be used."""


def _require(condition: object, message: str) -> None:
    if not condition:
        raise TemporalIndexError(message)


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _digest(value: Any, name: str) -> str:
    _require(
        isinstance(value, str) and _SHA256.fullmatch(value) is not None,
        f"{name} is not lowercase SHA-256",
    )
    return str(value)


def tokenize(text: str) -> tuple[str, ...]:
    """Tokenize with the repository's established normalization scheme."""

    _require(isinstance(text, str), "text to tokenize must be a string")
    folded = unicodedata.normalize("NFKC", text).casefold()
    return tuple(_TOKEN.findall(folded))


def detect_relation(question: str) -> str:
    """Detect the temporal relation from generic cue tokens only."""

    tokens = set(tokenize(question))
    for relation in _RELATION_PRIORITY:
        if tokens & set(RELATION_CUES[relation]):
            return relation
    return "none"


def anchor_query_tokens(question: str) -> tuple[str, ...]:
    """Content tokens of the question, with relation cues and stopwords removed.

    Relation words are stripped so the anchor is chosen by what the question is
    *about*, never by the temporal word it happens to use.
    """

    cues = {token for values in RELATION_CUES.values() for token in values}
    return tuple(
        token
        for token in tokenize(question)
        if token not in cues and token not in _STOPWORDS and len(token) > 1
    )


def anchor_clause(question: str) -> str:
    """Return the clause describing the anchor *event*, not the whole question.

    "what did X do after <event>" asks about the consequence, but the segment
    to retrieve is the one showing <event>.  Embedding the whole question
    conflates the two, so the relation cue is used as a clause boundary and the
    event side is kept.  This is positional, not a synonym or keyword table.
    """

    _require(isinstance(question, str) and bool(question.strip()), "question is empty")
    relation = detect_relation(question)
    if relation in ("none", "start", "end"):
        return question.strip()
    cues = RELATION_CUES[relation]
    # Split on the first cue token occurrence, keeping the event side.
    alternation = "|".join(re.escape(cue) for cue in cues)
    pattern = r"\b(?:" + alternation + r")\b"
    parts = re.split(pattern, question, maxsplit=1, flags=re.IGNORECASE)
    if len(parts) < 2:
        return question.strip()
    tail = parts[1].strip(" ,;:?.")
    # A cue with nothing meaningful after it cannot define an anchor clause.
    return tail if len(tail.split()) >= 2 else question.strip()


@dataclass(frozen=True)
class TemporalSegment:
    """One independently addressable, content-bound temporal segment."""

    segment_id: str
    ordinal: int
    object_id: str
    start_seconds: float
    end_seconds: float
    caption: str
    source_video_sha256: str
    source_video_size_bytes: int
    projection_sha256: str
    projection_size_bytes: int
    projection_frame_count: int
    projection_frame_timestamps: tuple[float, ...] = ()

    def __post_init__(self) -> None:
        _require(bool(self.segment_id), "segment_id is required")
        _require(
            type(self.ordinal) is int and self.ordinal >= 0,
            "segment ordinal must be a non-negative integer",
        )
        _require(bool(self.object_id), "segment object_id is required")
        _require(
            isinstance(self.start_seconds, float)
            and isinstance(self.end_seconds, float)
            and math.isfinite(self.start_seconds)
            and math.isfinite(self.end_seconds)
            and 0.0 <= self.start_seconds < self.end_seconds,
            "segment window is invalid",
        )
        _require(bool(self.caption.strip()), "segment caption is empty")
        _digest(self.source_video_sha256, "source_video_sha256")
        _digest(self.projection_sha256, "projection_sha256")
        _require(
            type(self.projection_size_bytes) is int and self.projection_size_bytes > 0,
            "projection_size_bytes must be a positive integer",
        )
        _require(
            type(self.projection_frame_count) is int
            and self.projection_frame_count > 0,
            "a segment projection must contain at least one frame",
        )

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "segment_id": self.segment_id,
            "ordinal": self.ordinal,
            "object_id": self.object_id,
            "start_seconds": self.start_seconds,
            "end_seconds": self.end_seconds,
            "caption_sha256": _sha256(self.caption.encode("utf-8")),
            "caption_token_count": len(tokenize(self.caption)),
            "source_video_sha256": self.source_video_sha256,
            "source_video_size_bytes": self.source_video_size_bytes,
            "projection_sha256": self.projection_sha256,
            "projection_size_bytes": self.projection_size_bytes,
            "projection_frame_count": self.projection_frame_count,
            "projection_frame_timestamps": list(self.projection_frame_timestamps),
        }


@dataclass(frozen=True)
class RankedSegment:
    segment_id: str
    ordinal: int
    score_milli: int
    matched_terms: tuple[str, ...]


@dataclass(frozen=True)
class TemporalIndexSelection:
    """Exact, content-bound result of one query-aware temporal search.

    Deliberately a distinct type from the object-level ``IndexSelection`` so a
    fixed-window fallback can never masquerade as query-aware selection.
    """

    object_id: str
    query_sha256: str
    temporal_index_sha256: str
    relation: str
    anchor_segment_id: str
    selected_segments: tuple[TemporalSegment, ...]
    ranked_candidates: tuple[RankedSegment, ...]
    candidate_segment_count: int
    fallback_used: bool = False
    telemetry: Any = None

    def __post_init__(self) -> None:
        _require(bool(self.selected_segments), "temporal selection is empty")
        _require(
            self.relation in set(RELATION_CUES) | {"none"},
            "temporal relation is unsupported",
        )
        _digest(self.query_sha256, "query_sha256")
        _digest(self.temporal_index_sha256, "temporal_index_sha256")
        ordinals = [segment.ordinal for segment in self.selected_segments]
        _require(
            len(set(ordinals)) == len(ordinals),
            "temporal selection repeats a segment",
        )
        _require(
            ordinals == sorted(ordinals),
            "temporal selection is not in ascending temporal order",
        )
        _require(
            all(segment.object_id == self.object_id for segment in self.selected_segments),
            "temporal selection mixes objects",
        )
        _require(
            self.candidate_segment_count >= len(self.selected_segments),
            "candidate count is smaller than the selection",
        )

    @property
    def total_selected_bytes(self) -> int:
        return sum(s.projection_size_bytes for s in self.selected_segments)

    @property
    def selected_timestamp_range(self) -> tuple[float, float]:
        return (
            min(s.start_seconds for s in self.selected_segments),
            max(s.end_seconds for s in self.selected_segments),
        )

    def to_public_evidence(self) -> dict[str, Any]:
        """Public, payload-free and credential-free selection evidence."""

        return {
            "schema_version": TEMPORAL_SELECTION_SCHEMA_VERSION,
            "algorithm": TEMPORAL_INDEX_ALGORITHM,
            "tokenizer": TEMPORAL_INDEX_TOKENIZER,
            "temporal_index_sha256": self.temporal_index_sha256,
            "query_sha256": self.query_sha256,
            "object_id": self.object_id,
            "candidate_segment_count": self.candidate_segment_count,
            "ranked_segments": [
                {
                    "segment_id": r.segment_id,
                    "ordinal": r.ordinal,
                    "score_milli": r.score_milli,
                    "matched_terms": list(r.matched_terms),
                }
                for r in self.ranked_candidates
            ],
            "relation": self.relation,
            "anchor_segment_id": self.anchor_segment_id,
            "selected_segment_ids": [s.segment_id for s in self.selected_segments],
            "selected_segments": [s.to_public_dict() for s in self.selected_segments],
            "selected_timestamp_range_seconds": list(self.selected_timestamp_range),
            "total_selected_artifact_bytes": self.total_selected_bytes,
            "query_aware_selection": True,
            "fixed_window_fallback_used": self.fallback_used,
            "credentials_recorded": False,
            "hidden_label_values_included": False,
        }


def parse_question_independent_timeline(digest_text: str) -> list[tuple[float, float | None, str]]:
    """Parse time-ranged captions from a frozen question-independent digest."""

    _require(
        _QUESTION_INDEPENDENT_MARKER in digest_text,
        "digest is not declared question-independent",
    )
    entries: list[tuple[float, float | None, str]] = []
    for line in digest_text.splitlines():
        match = _TIMELINE_ENTRY.match(line.strip())
        if match is None:
            continue
        start = float(match.group("start"))
        end = match.group("end")
        entries.append((start, None if end is None else float(end), match.group("text")))
    _require(
        len(entries) >= 2,
        "a temporal index needs at least two segments per object",
    )
    return entries


def build_segments(
    *,
    object_id: str,
    digest_text: str,
    duration_seconds: float,
    source_video_sha256: str,
    source_video_size_bytes: int,
    projection_for_window,
) -> tuple[TemporalSegment, ...]:
    """Build content-bound segments from a frozen question-independent digest.

    ``projection_for_window(start, end)`` must return
    ``(sha256, size_bytes, frame_count, timestamps)`` for the exact projection
    the data plane will serve for that window.
    """

    _require(
        isinstance(duration_seconds, float) and duration_seconds > 0.0,
        "duration_seconds must be positive",
    )
    entries = parse_question_independent_timeline(digest_text)
    segments: list[TemporalSegment] = []
    for ordinal, (start, end, caption) in enumerate(entries):
        # A trailing open-ended caption runs to the end of the video; any other
        # missing bound runs to the next entry's start.
        if end is None:
            end = (
                entries[ordinal + 1][0]
                if ordinal + 1 < len(entries)
                else duration_seconds
            )
        end = min(float(end), duration_seconds)
        _require(
            float(start) < end,
            f"segment {ordinal} of {object_id} has a non-positive duration",
        )
        sha, size, frames, timestamps = projection_for_window(float(start), float(end))
        segments.append(
            TemporalSegment(
                segment_id=f"{object_id}#seg{ordinal:02d}",
                ordinal=ordinal,
                object_id=object_id,
                start_seconds=float(start),
                end_seconds=float(end),
                caption=caption,
                source_video_sha256=source_video_sha256,
                source_video_size_bytes=source_video_size_bytes,
                projection_sha256=sha,
                projection_size_bytes=size,
                projection_frame_count=frames,
                projection_frame_timestamps=tuple(timestamps),
            )
        )
    return tuple(segments)


def rank_segments(
    question: str,
    segments: Sequence[TemporalSegment],
) -> tuple[RankedSegment, ...]:
    """Score every segment by IDF-weighted overlap with the question content.

    Document frequency is computed over this object's own segments, so a term
    present in every caption contributes nothing and cannot pin the anchor.
    """

    _require(bool(segments), "cannot rank an empty segment set")
    query_terms = anchor_query_tokens(question)
    caption_tokens = [set(tokenize(s.caption)) for s in segments]
    total = len(segments)
    ranked: list[RankedSegment] = []
    for segment, tokens in zip(segments, caption_tokens):
        score = 0.0
        matched: list[str] = []
        for term in dict.fromkeys(query_terms):
            if term not in tokens:
                continue
            frequency = sum(1 for other in caption_tokens if term in other)
            weight = math.log((total + 1) / (frequency + 0.5))
            if weight <= 0.0:
                continue
            score += weight
            matched.append(term)
        ranked.append(
            RankedSegment(
                segment_id=segment.segment_id,
                ordinal=segment.ordinal,
                # Integer score units keep evidence byte-stable across the
                # FlowMesh Pydantic boundary, which normalizes floats.
                score_milli=int(round(score * 1000)),
                matched_terms=tuple(matched),
            )
        )
    # Deterministic: highest score first, then lowest ordinal.
    return tuple(sorted(ranked, key=lambda r: (-r.score_milli, r.ordinal)))


def expand_relation(
    relation: str,
    anchor_ordinal: int,
    segment_count: int,
    *,
    max_selected_segments: int,
) -> tuple[int, ...]:
    """Choose which segment ordinals the relation implies around the anchor."""

    _require(
        type(max_selected_segments) is int and max_selected_segments > 0,
        "max_selected_segments must be a positive integer",
    )
    _require(
        0 <= anchor_ordinal < segment_count,
        "anchor ordinal is outside the segment set",
    )
    everything = range(segment_count)
    if relation == "following":
        chosen = [o for o in everything if o > anchor_ordinal][:max_selected_segments]
        # An anchor at the very end has nothing after it; keep the anchor so the
        # selection stays non-empty and honest about what exists.
        return tuple(chosen) if chosen else (anchor_ordinal,)
    if relation == "preceding":
        chosen = [o for o in everything if o < anchor_ordinal][-max_selected_segments:]
        return tuple(chosen) if chosen else (anchor_ordinal,)
    if relation == "during":
        return (anchor_ordinal,)
    if relation == "start":
        return tuple(list(everything)[:max_selected_segments])
    if relation == "end":
        return tuple(list(everything)[-max_selected_segments:])
    neighbours = [
        o for o in everything if abs(o - anchor_ordinal) <= 1
    ][:max_selected_segments]
    return tuple(neighbours)


def search_temporal_index(
    *,
    question: str,
    object_id: str,
    segments: Sequence[TemporalSegment],
    temporal_index_sha256: str,
    max_selected_segments: int = 2,
) -> TemporalIndexSelection:
    """Run one query-aware temporal search over a known object's segments."""

    _require(bool(question.strip()), "question is empty")
    candidates = [s for s in segments if s.object_id == object_id]
    _require(
        len(candidates) >= 2,
        "a query-aware temporal search needs at least two candidate segments",
    )
    ordered = sorted(candidates, key=lambda s: s.ordinal)
    ranked = rank_segments(question, ordered)
    anchor = ranked[0]
    relation = detect_relation(question)
    chosen = expand_relation(
        relation,
        anchor.ordinal,
        len(ordered),
        max_selected_segments=max_selected_segments,
    )
    by_ordinal = {s.ordinal: s for s in ordered}
    return TemporalIndexSelection(
        object_id=object_id,
        query_sha256=_sha256(question.encode("utf-8")),
        temporal_index_sha256=_digest(temporal_index_sha256, "temporal_index_sha256"),
        relation=relation,
        anchor_segment_id=anchor.segment_id,
        selected_segments=tuple(by_ordinal[o] for o in sorted(chosen)),
        ranked_candidates=ranked,
        candidate_segment_count=len(ordered),
        fallback_used=False,
    )


def verify_selection(
    evidence: Mapping[str, Any],
    *,
    question: str,
    segments: Sequence[TemporalSegment],
    temporal_index_sha256: str,
    max_selected_segments: int = 2,
) -> dict[str, Any]:
    """Re-derive the selection and reject forged or inconsistent evidence."""

    _require(isinstance(evidence, Mapping), "temporal selection evidence is missing")
    _require(
        evidence.get("schema_version") == TEMPORAL_SELECTION_SCHEMA_VERSION,
        "temporal selection schema changed",
    )
    _require(
        evidence.get("query_aware_selection") is True
        and evidence.get("fixed_window_fallback_used") is False,
        "evidence does not describe a query-aware selection",
    )
    object_id = str(evidence.get("object_id"))
    expected = search_temporal_index(
        question=question,
        object_id=object_id,
        segments=segments,
        temporal_index_sha256=temporal_index_sha256,
        max_selected_segments=max_selected_segments,
    )
    rebuilt = expected.to_public_evidence()
    supplied = json.loads(_canonical(evidence))
    _require(
        supplied == json.loads(_canonical(rebuilt)),
        "temporal selection evidence differs from its frozen reconstruction",
    )
    return {
        "status": "VERIFIED",
        "object_id": object_id,
        "relation": expected.relation,
        "anchor_segment_id": expected.anchor_segment_id,
        "selected_segment_ids": [s.segment_id for s in expected.selected_segments],
        "candidate_segment_count": expected.candidate_segment_count,
        "query_aware_selection": True,
        "credentials_recorded": False,
        "hidden_label_values_included": False,
    }


__all__ = [
    "RELATION_CUES",
    "TEMPORAL_INDEX_ALGORITHM",
    "TEMPORAL_INDEX_SCHEMA_VERSION",
    "TEMPORAL_INDEX_TOKENIZER",
    "TEMPORAL_SELECTION_SCHEMA_VERSION",
    "RankedSegment",
    "TemporalIndexError",
    "TemporalIndexSelection",
    "TemporalSegment",
    "anchor_clause",
    "anchor_query_tokens",
    "build_segments",
    "detect_relation",
    "expand_relation",
    "parse_question_independent_timeline",
    "rank_segments",
    "search_temporal_index",
    "tokenize",
    "verify_selection",
]
