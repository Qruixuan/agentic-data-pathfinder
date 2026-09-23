# Interleaved multi-question-per-video pilot

Status: **LOCAL INPUT PROTOCOL TESTED, NOT SOURCE-BOUND OR EXECUTED**. The source-bound public
question scheduler is implemented in `interleaved_multiq_plan.py`; a dry run
against the existing two-video public diagnostic plan verified 6 questions
and 24 unique route slots. It has not frozen a production admission, supplied
multi-question N1 labels, or changed the deployed cache/route protocol. This
document authorizes no FlowMesh
submission, external model request, deployment, or cloud spending. Follow
`EXPERIMENT_OPERATIONS_RUNBOOK.md` before any later freeze or run. The claim
class of the first experiment is exploratory full-route conformance and
question-level quality/cost comparison, not statistical superiority.

## Question and cohort

Test whether a video-level temporal index and question-independent derived
artifact become worthwhile when the same video is queried again after other
videos have intervened. Keep task identity separate from video identity:
`question_id != object_id` is allowed, and several question IDs may refer to
one immutable MP4. Never use answer values or previous predictions to choose
videos, questions, index parameters, or order.

First use the two previously diagnosed videos, three public questions each,
as a **plumbing gate only** (6 questions x 4 arms = 24 route executions).
Do not count their known outcomes as a fresh held-out finding. For the main
pilot, deterministically sample four different, previously unused videos
from the public NextQA validation set, each with at least one causal, one
temporal, and one descriptive question; choose exactly one of each with a
predeclared hash seed. If four eligible unused videos do not exist, stop and
report feasibility before freezing a smaller cohort. Hidden labels stay on
N1. Main pilot size: 4 videos x 3 questions x 4 arms = **48 full routes**.
No answer-generation request is used to validate source binding or readiness.

## Frozen interleaved schedule

Freeze one seeded order before execution and use it in every arm. Shuffle
questions within each video, then place one question from each video in each
of three rounds; deterministically rotate a round if its first video would
repeat the preceding round's last video. This guarantees intervening videos between
repeat visits without selecting an order based on outcomes. Preserve the
exact order and revisit distance in public evidence. Within each question,
rotate the arm order deterministically to reduce time-of-day/provider-order
confounding. A later replication may use a second predeclared seed, but the
first pilot does not silently add repetitions after seeing results.

| Arm | Representation/path | Purpose |
| --- | --- | --- |
| R | Full encoded MP4, no Pathfinder artifact cache | Quality/cost reference |
| D | Same question-independent digest + frame artifact, cache disabled | Derived representation without cache benefit |
| DC | Byte-identical D input, N7 artifact cache enabled | Isolate cross-question cache benefit |
| I | D's digest plus query-aware temporal-index-selected frames, cache disabled | Compare frame selection and its build/query cost |

Keep N6 model, answer format, question text, options, N1 scorer, and initial
executor placement fixed. D, DC and I must use the same question-independent
digest, four-frame budget, JPEG settings and N6 prompt; only I's frame
selection changes. R intentionally carries the complete MP4. Use N7 for
this first paired test; N8 placement is a later factor, not mixed into this
pilot. D and DC must give N6 byte-identical question/digest/frame content
for a given question. Their full request digests differ because each route
has a distinct request ID; compare the content fields, not that outer digest.
Different model answers remain possible, but cache
must not change content.

Each arm has an isolated experiment episode. DC starts on a new empty cache
volume and keeps its namespace across the 12 questions; unrelated traffic
must not share it. Other arms must not warm that cache. Freeze actual cache
capacity and record every hit, miss, insert and eviction. Do not increase
capacity or change order after observing hit rate. The first access to each
video is cold; later access is warm only if the same immutable artifact is
still present on that node. Never claim a hit merely because the video was
seen before.

## Required engineering gates before submission

1. Add a new multi-question collection schema instead of weakening the
   legacy one-question schema. Bind public `question_id`, `object_id`,
   workload/stratum, task digest and hidden-score commitment separately.
   One-case plans, N1 scoring and route evidence must identify the question,
   not assume `case_id == object_id`.
2. Bind the official public-task set to the new query package. Reuse the
   immutable video index from `temporal_index_layers.py`; independently
   verify each question's anchor, rank, interval and source MP4. Materialize
   N3 selected-frame bundles per question with query/selection identity and
   measure projection work. Caption/index build is video-level, not repeated
   for every question.
3. Separate unique route `run_id` from an explicit, signed cache `episode_id`.
   Do **not** reuse a run ID to force cache sharing. DC questions must derive
   one namespace from the episode; different arms/repetitions must not.
4. Cache only genuinely question-independent D artifacts under video-level
   identity. If a later experiment caches I's selected frames, include the
   query/selection digest in the logical key: the current cache key is
   namespace + object ID + representation ID, so different selections would
   otherwise compete for one slot. Expected payload SHA checks are necessary
   but do not make that key suitable for multiple question-specific bundles.
5. Extend offline replay state from per-case to per-video/per-node/per-episode
   build and cache state, while retaining per-question outcomes. An agent may
   observe the current public question and prior build/cache state, but never
   future questions, hidden labels or previous hidden-score results unless a
   separate feedback protocol explicitly authorizes them.
6. Run focused tests for duplicate-object/different-question bindings,
   source tampering, cache cross-question hits, namespace isolation,
   eviction, and cold-build accounting. Preserve legacy frozen artifacts.
   Pass source-bound verifiers, checksums, N1/N3/N4/N6/N7 health, dependency
   and authentication probes, FlowMesh worker preflight, and fresh-ID gates
   before any submission. Stop at the first failed boundary; no blind retry.

## Accounting and evaluation

Keep build and query costs separate. Charge captioning, video-window
embeddings and D materialization **once per video when actually built**.
Charge I anchor embedding, ranking and N3 selected-frame projection per
question. Charge N6 provider tokens, transfer, execution VM time and storage
according to their own measured usage. Allocate shared VM/Root expense by
recorded experiment time, as agreed for the cost schedule. Price provider
usage with a frozen official rate card; report it as list-price cost, not
necessarily the amount paid after credits. Missing prices or usage remain
unknown, never zero. Precomputed frozen artifacts can be charged as
one-time build cost, but their build latency must not be presented as if it
occurred on the first live route.

Record per question/arm: N1-authenticated correctness; source/selected/N6
bytes; N6 request ID and provider input/cached-input/output units; route and
component latency; build and query receipts; cache event lineage; and the
VM-time interval. Report paired R/D/DC/I results for all 12 questions and
cumulative cost after each of a video's first, second and third questions.
Compute an index break-even query count only from separately observed build
and marginal-query charges, clearly labelling extrapolation beyond three
questions. DC-versus-D savings should primarily affect storage/network
access and time, not be presumed to reduce N6 inference tokens.

Do not select a showcase video after seeing these outcomes and then call the
same sample held out. Negative results, no cross-question cache hits, or no
quality-feasible index action are valid reported outcomes. A second cohort
and repeated seeds would be required for performance or accuracy claims.

## Current pre-submit gap (24 September 2026)

The scheduler verifies public questions and order. An additive signed cache
episode request and evidence protocol now permits cross-question reuse while
the legacy run-scoped request and fixed repetition checks remain unchanged.
Focused tests prove cross-run hits, episode isolation and eviction misses.
The additive N3 multi-question package now verifies different predecoded
bundles for one video under distinct Data Agent plan IDs. Its task-bound
resolver rejects another question's selection without inventing video IDs.
The frozen route handler rejects episode requests by default; only an
explicit `(run_id, trial_key) -> episode_id` binding can allow one, so an
existing single-question admission cannot silently enable cross-run cache
sharing. N3 also verifies that a question's plan cannot resolve another
video's projected bundle.
This is **not yet** an executable full-route admission: the service factory,
promotion, formal collection and experiment runner do not bind these new
packages and episode IDs to the frozen schedule. The legacy collection
verifier still requires `case_id == object_id` and distinct video objects.
The new `indexed-derived` route locally verifies that N3's exact temporal
frame bundle and N4's digest both reach N6. It keeps D's N6 prompt label and
digest while changing only the ordered frames; the route evidence binds the
raw source and digest identities separately. This is a tested source path,
not yet a production admission or deployed service. R's direct-video input
is unchanged. D and DC's content fields are tested equal even though their
request IDs differ. The service factory rejects an indexed-derived admission
before creating state until its source-bound multi-question N3 plan and
selection catalog are wired. The factory still needs
to bind the question-specific N3 plan catalog and the new four-arm trial DAG
to the frozen multi-question schedule.
Those source-bound integrations and canonical verifiers are required before
the 24-route plumbing gate. The 48-route pilot also needs a fresh
video-disjoint public cohort and N1-confined private labels. No service has
been redeployed or workflow submitted.
