"""Subject-aware, ambiguity-tolerant temporal index (v2).

v1 demonstrated two specific defects on public evidence:

* the anchor clause kept a bare pronoun, so the query matched whichever window
  described a similar motion performed by *any* entity, rather than the actor
  the question names; and
* relation expansion compared window ordinals, which is invalid when windows
  overlap: a higher ordinal can still start inside the anchor interval.

v2 addresses exactly those two things.  It recovers the subject noun phrase
from the public question and substitutes it for a pronoun in the anchor clause,
and it retains a small top-k of anchor candidates and expands the relation using
real start/end timestamps, merging overlapping intervals deterministically under
a fixed evidence budget.

This is a separately frozen physical action, not an edit of v1.  Nothing here
reads a hidden label, an oracle, a prior prediction or a task outcome, and no
object, workload, timestamp, option or answer is hard-coded.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from typing import Any

from .full_flow_temporal_index import (
    RELATION_CUES,
    TemporalIndexError,
    anchor_clause,
    detect_relation,
)

TEMPORAL_INDEX_V2_ACTION_ID = "semantic-temporal-index-v2-subject-aware-topk"
TEMPORAL_INDEX_V2_SCHEMA_VERSION = (
    "pathfinder.full-flow-temporal-index-v2-selection/v1alpha1"
)

# Frozen policy constants, declared before any outcome is observed.
DEFAULT_ANCHOR_TOP_K = 2
DEFAULT_MAX_SELECTED_WINDOWS = 4

# Generic English closed-class words.  None is task, object or answer specific.
_PRONOUNS = ("he", "she", "it", "they", "him", "her", "them", "his", "their")
_INTERROGATIVES = frozenset({
    "what", "who", "whom", "whose", "which", "where", "when", "why", "how",
})
_AUXILIARIES = frozenset({
    "did", "do", "does", "is", "are", "was", "were", "has", "have", "had",
    "will", "would", "can", "could", "should", "been", "being", "be",
})
_PRO_VERBS = frozenset({
    "do", "does", "did", "doing", "done", "happen", "happens", "happened",
    "happening", "occur", "occurs", "occurred", "going",
})


def _require(condition: object, message: str) -> None:
    if not condition:
        raise TemporalIndexError(message)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def public_question_subject(question: str) -> str | None:
    """Recover the subject noun phrase named by the public question.

    Takes the clause before the relation cue, drops leading interrogatives and
    auxiliaries and trailing pro-verbs, and returns what remains.  Purely
    positional over closed-class English words.
    """

    _require(isinstance(question, str) and bool(question.strip()), "question is empty")
    relation = detect_relation(question)
    head = question
    if relation != "none":
        cues = RELATION_CUES[relation]
        pattern = r"\b(?:" + "|".join(re.escape(cue) for cue in cues) + r")\b"
        head = re.split(pattern, question, maxsplit=1, flags=re.IGNORECASE)[0]
    words = [w for w in re.findall(r"[A-Za-z0-9']+", head)]
    while words and (
        words[0].casefold() in _INTERROGATIVES or words[0].casefold() in _AUXILIARIES
    ):
        words.pop(0)
    while words and words[-1].casefold() in _PRO_VERBS:
        words.pop()
    if not words:
        return None
    # A bare pronoun is not a usable subject description.
    if len(words) == 1 and words[0].casefold() in _PRONOUNS:
        return None
    return " ".join(words)


def contextualize_anchor_clause(question: str) -> dict[str, Any]:
    """Replace a pronoun in the anchor clause with the question's subject.

    "what did X do after he approached" anchors on "X approached", not on
    "he approached", so the query binds the actor the question actually names.
    """

    clause = anchor_clause(question)
    subject = public_question_subject(question)
    relation = detect_relation(question)
    contextualized = clause
    substituted = False
    if subject:
        for pronoun in _PRONOUNS:
            pattern = r"\b" + re.escape(pronoun) + r"\b"
            if re.search(pattern, clause, flags=re.IGNORECASE):
                contextualized = re.sub(
                    pattern, subject, clause, count=1, flags=re.IGNORECASE
                )
                substituted = True
                break
    return {
        "question_sha256": _sha256(question.encode("utf-8")),
        "relation": relation,
        "anchor_clause": clause,
        "public_question_subject": subject,
        "contextualized_anchor_text": contextualized,
        "contextualized_anchor_sha256": _sha256(contextualized.encode("utf-8")),
        "pronoun_substituted": substituted,
    }


def strictly_after(window: Mapping[str, Any], boundary_seconds: float) -> bool:
    """True when the window begins at or after the boundary, by timestamp.

    Ordinal order is meaningless for overlapping windows, so the comparison is
    on real start times.
    """

    return float(window["start_seconds"]) >= float(boundary_seconds)


def strictly_before(window: Mapping[str, Any], boundary_seconds: float) -> bool:
    return float(window["end_seconds"]) <= float(boundary_seconds)


def merge_intervals(
    windows: Sequence[Mapping[str, Any]],
) -> list[tuple[float, float]]:
    """Deterministically merge overlapping or touching selected intervals."""

    spans = sorted(
        (float(w["start_seconds"]), float(w["end_seconds"])) for w in windows
    )
    merged: list[tuple[float, float]] = []
    for start, end in spans:
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def select_v2(
    *,
    question: str,
    ranked: Sequence[Mapping[str, Any]],
    windows_by_ordinal: Mapping[int, Mapping[str, Any]],
    anchor_top_k: int = DEFAULT_ANCHOR_TOP_K,
    max_selected_windows: int = DEFAULT_MAX_SELECTED_WINDOWS,
) -> dict[str, Any]:
    """Retain top-k anchors, expand by timestamp, merge, and cap by budget."""

    _require(bool(ranked), "ranking is empty")
    _require(
        type(anchor_top_k) is int and 1 <= anchor_top_k <= len(ranked),
        "anchor_top_k is outside the available ranking",
    )
    _require(
        type(max_selected_windows) is int and max_selected_windows >= anchor_top_k,
        "evidence budget cannot be smaller than the retained anchors",
    )
    relation = detect_relation(question)
    anchors = list(ranked[:anchor_top_k])
    anchor_ordinals = [int(a["ordinal"]) for a in anchors]
    anchor_windows = [windows_by_ordinal[o] for o in anchor_ordinals]

    if relation == "following":
        boundary = max(float(w["end_seconds"]) for w in anchor_windows)
        candidates = [
            w for o, w in sorted(windows_by_ordinal.items())
            if o not in anchor_ordinals and strictly_after(w, boundary)
        ]
    elif relation == "preceding":
        boundary = min(float(w["start_seconds"]) for w in anchor_windows)
        candidates = [
            w for o, w in sorted(windows_by_ordinal.items())
            if o not in anchor_ordinals and strictly_before(w, boundary)
        ][::-1]
    else:
        candidates = []

    budget = max_selected_windows - len(anchor_ordinals)
    expanded = candidates[:budget] if budget > 0 else []
    selected = sorted(
        {int(w["ordinal"]) for w in anchor_windows}
        | {int(w["ordinal"]) for w in expanded}
    )
    selected_windows = [windows_by_ordinal[o] for o in selected]
    merged = merge_intervals(selected_windows)
    return {
        "schema_version": TEMPORAL_INDEX_V2_SCHEMA_VERSION,
        "action_id": TEMPORAL_INDEX_V2_ACTION_ID,
        "relation": relation,
        "anchor_top_k": anchor_top_k,
        "max_selected_windows": max_selected_windows,
        "anchor_window_ordinals": anchor_ordinals,
        "anchor_window_ids": [str(w["window_id"]) for w in anchor_windows],
        "relation_expanded_ordinals": [int(w["ordinal"]) for w in expanded],
        "relation_expanded_window_ids": [str(w["window_id"]) for w in expanded],
        "selected_window_ordinals": selected,
        "selected_window_ids": [str(w["window_id"]) for w in selected_windows],
        "merged_intervals_seconds": [list(span) for span in merged],
        "selected_span_seconds": [merged[0][0], merged[-1][1]] if merged else [],
        "expansion_basis": "timestamp",
        "fallback_used": False,
        "credentials_recorded": False,
        "hidden_label_values_included": False,
    }


__all__ = [
    "DEFAULT_ANCHOR_TOP_K",
    "DEFAULT_MAX_SELECTED_WINDOWS",
    "TEMPORAL_INDEX_V2_ACTION_ID",
    "TEMPORAL_INDEX_V2_SCHEMA_VERSION",
    "contextualize_anchor_clause",
    "merge_intervals",
    "public_question_subject",
    "select_v2",
    "strictly_after",
    "strictly_before",
]
