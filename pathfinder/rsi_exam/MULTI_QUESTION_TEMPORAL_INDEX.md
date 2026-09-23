# Multi-question temporal-index boundary

The existing `finalize_formal_temporal_index` package is retained for legacy
single-question runs. It combines caption vectors, one public-question anchor
vector, a selected interval, and an N3 frame-bundle policy per object. Its
collection plan deliberately selects one question per object, so it must not
be treated as a measured multi-question index.

`temporal_index_layers.py` introduces two separately verified, immutable
packages:

1. **Video index:** verified question-independent window captions are embedded
   once per video. The package binds the preparation and caption packages,
   window identities, vector model and policy, and the existing caption-build
   cost receipt. Embedding receipts carry an object ID, so video-build inputs
   and request usage can be charged to that video once.
2. **Query batch:** any number of public questions may reference the same
   video. Each question gets its own contextualized anchor, embedding request,
   ranking and timestamp-based selection. The package binds the base-index
   digest and public-question digest. It reports zero video-build inputs
   charged at this layer, with one query-embedding receipt per question.

Both verifiers rebuild source and selection bindings; changing a caption,
question, vector, rank, interval or package digest invalidates the result.
The query builder accepts exactly `question_id`, `object_id`, and `question`;
it rejects explicit answer-label and outcome fields. The caller still must
prove that each question string came from the intended public task set before
using it for a formal experiment.

This split is a preparation step, **not** a full-route multi-question run.
It does not yet create per-question N3 frame bundles, select shared cache
namespaces, change the legacy formal collection schema, or claim measured
cross-question cache hits. Query-frame materialization and its VM/storage
cost remain separate from both embedding cost layers. Old frozen artifacts
and single-question execution are unchanged.

For a future cost comparison, charge captioning plus video-vector build once
per video; charge anchor embedding, ranking and any selected-frame projection
per question. Do not amortize the former by a *hypothetical* question count:
record the actual number of questions using the video, their order, and any
cache eviction or namespace boundary.
