# Query-aware temporal index — design note

Written before implementation, per the operator's Phase 1 requirement.

## Why the old path is replaced

The frozen `indexed-raw` action froze `candidate_object_ids` to the single
already-known target with `top_k: 1`, so the N2 lexical object query could not
affect object selection. It then applied a compile-time constant 25 %–75 %,
eight-frame projection to every question. That is a fixed-window baseline, not
an index. It is retained only under the explicit name
`fixed-middle-window-projection-v1` and is never described as retrieval.

## Index unit

The new unit is a **temporal segment** of a known object:

- stable `segment_id` (`<object_id>#seg<NN>`, ordinal-stable);
- `object_id`, source MP4 SHA-256 + size, object catalog version;
- `start_seconds`, `end_seconds`;
- question-independent caption text;
- an exact segment projection: frame-bundle SHA-256, size, frame count and
  frame timestamps;
- index generation policy + software version digests.

## Searchable segment content (no new model calls)

The repository already materializes, per object, a
`multimodal_digest.txt` whose first line declares
`PATHFINDER QUESTION-INDEPENDENT MULTIMODAL DIGEST` and whose `Timeline:`
section is a list of **time-ranged captions**. These are frozen artifacts,
produced once offline by the existing N5 vision-digest path, and they are
question-independent by construction.

The temporal index therefore parses those timeline entries into segments. This
is repository preference 3, "another genuine segment-level content feature
already supported by the repository". It requires **zero new LLM calls**.

Rejected alternatives and why:

- *Local visual/text embeddings* (preference 1): unavailable. The environment
  has no `torch`, `clip`, `open_clip`, `sentence_transformers` or
  `transformers`; only `numpy` and `PIL`.
- *Newly generated per-segment captions* (preference 2): would require bounded
  but real paid vision calls. Unnecessary, because equivalent frozen captions
  already exist.
- *Non-text visual features* (histograms, motion): genuinely computable
  locally, but there is no text–vision alignment, so matching a natural-language
  question to them would require a hard-coded keyword→feature table. The
  operator explicitly forbids that, so it is rejected.

## Segment projections

A segment's projection is the deterministic subset of the already-frozen
16-frame sampled grid whose timestamps fall inside the segment window,
re-packaged as a frame bundle bound to the same source MP4 identity. Frames are
byte-identical to frames already frozen, so no decoder is required (the
environment has neither `ffmpeg` nor `cv2`) and no new visual bytes are
invented. This is the same mechanism that produced the existing indexed bundle,
which is provably a contiguous slice of that grid.

## Anchor retrieval

1. Tokenize the public question with the repository's existing
   `unicode-nfkc-alphanumeric-casefold` scheme.
2. Remove relation cue tokens and generic interrogative stopwords to form the
   *anchor query*. Relation words must not bias content matching.
3. Score every segment of the object by IDF-weighted lexical overlap between
   the anchor query and that segment's caption. Document frequency is computed
   over the object's own segment set, so a term appearing in every segment
   carries no discriminative weight.
4. The anchor is the highest scoring segment; ties break deterministically on
   the lowest segment ordinal.
5. Every candidate segment and its score is recorded in evidence.

This searches multiple segments of the known object using real content. It is
weaker than an embedding retriever — recorded as a limitation — but it is
genuine content matching, not a timestamp or keyword→time mapping.

## Temporal relation expansion

Relation detection runs **after** anchor retrieval and only selects which
segments around the anchor to take:

| Relation | Cue tokens | Selection |
| --- | --- | --- |
| `following` | after, afterwards, then, next, subsequently | segments after the anchor |
| `preceding` | before, prior, earlier, previously | segments before the anchor |
| `during` | during, while, as, when | the anchor segment |
| `start` | start, begin, beginning, first, initially | first segment |
| `end` | end, finally, last, conclusion | last segment |
| `none` | – | anchor plus its immediate neighbours |

Selections are bounded by `max_selected_segments`, returned in ascending
temporal order, de-duplicated, and each bound to its exact projection digest.

## Source binding and verification

The runtime fails closed when the selected object differs from the known
target, a segment is not bound to the frozen source MP4, a payload digest or
size differs, a timestamp falls outside the video, the selection is empty,
repeated, unordered or over limit, a source binding changed, or evidence claims
query-aware selection while carrying only a fixed fallback. The verifier
re-derives the expected ranking and selection from the frozen index plus the
public query, so forged, reordered or omitted segments are rejected.

## Why this is not specific to the visible example

No object ID, workload ID, question text, timestamp or answer appears anywhere
in the implementation. Segment boundaries come from each object's own frozen
caption timeline, never from an observed outcome. Different questions over one
video select different anchors; one question over different videos selects
different segments, because the captions differ. Tests assert both directions
and assert that a fixed-window implementation fails.

## Claim boundary

This index locates *where in a known video* to look. It is not unknown-video
retrieval and must not be reported as such. Direct-video, derived, cache, N1
scoring and `canonical-match-v1` semantics are untouched.
