# Ten-route, cache-aware multi-question experiment plan

Status: **IMPLEMENTATION IN PROGRESS (25 September 2026)**. This document
replaces the *proposed next experiment*, not any frozen input or historical
result. The public cohort, ten-observation plan, and reusable binder/runner
profile are implemented, but live preparation, runtime deployment, measured
route collection, cost binding, and sealed replay remain separate gates. This
document alone does not authorize FlowMesh submission or an LLM request.
Follow `EXPERIMENT_OPERATIONS_RUNBOOK.md` and its fail-closed checklist before
those operations. The old four-arm and single-question ten-route contracts
remain distinct.

## Objective and bounded claim

Build a small, video-disjoint, interleaved VideoQA evaluation in which a policy
chooses representation, route placement, and whether to admit a reusable
derived artifact to a finite cache. The current question and past public
state are visible; future questions, hidden labels, and hidden-score feedback
are not. Measure whether a fixed baseline's quality/latency/list-price-cost
trade-off differs from always using one representation. Do not claim that an
RSI agent has been trained, that N8 performs local GPU inference, or that a
small pilot establishes statistical superiority.

The old D0--D7 protocol contains **eight path designs and ten observations**:

| Observation | Node | Physical input / cache condition |
| --- | --- | --- |
| D0 | N7 | Full encoded MP4 (R) |
| D1 | N7 | Query-selected N3 frames, no N4 digest (indexed-raw, I) |
| D2 | N7 | N4 derived artifact without cache admission (D) |
| D3 r0000 | N7 | Derived artifact on a genuine cache miss; admit (DC) |
| D3 r0001 | N7 | The same artifact on a genuine subsequent cache hit |
| D4 | N8 | Full encoded MP4 (R) |
| D5 | N8 | Query-selected N3 frames, no N4 digest (indexed-raw, I) |
| D6 | N8 | N4 derived artifact without cache admission (D) |
| D7 r0000 | N8 | Derived artifact on a genuine cache miss; admit (DC) |
| D7 r0001 | N8 | The same artifact on a genuine subsequent cache hit |

This is not the recent R/D/DC/I four-arm experiment: that experiment's I sent
selected frames **plus** an N4 digest, while D1/D5 above are indexed-raw.
Its rows cannot simply be relabelled as these ten observations. N7/N8 choose
route placement, transfer, preparation and local cache; both use N6 for model
inference. Compare them on the same questions with order counterbalanced, not
as “GPU cloud versus GPU edge.”

## The policy decision and the cache mechanism

The policy action is `(node in {N7,N8}, route in {R,I,D,DC})`: eight actions.
For D and DC, N6 receives identical task/representation content. On a DC
miss, the policy authorizes insertion; on a DC hit, the service reuses the
valid cached artifact automatically. **Hit and miss are observed states, not
two actions the policy can choose.** The policy does not select an eviction
victim in this version; the deployed cache uses its existing capacity limit
and LRU rule. R and I do not warm the derived-artifact cache. Do not cache
query-specific I frames under a video-only key; a later extension requires a
selection-digest-aware key.

The deployed cache stores D's `multimodal_digest` and
`sampled_frame_bundle` as separate entries. Therefore a DC route may see
two hits, two misses, or a partial hit after eviction. Replay must reproduce
the ordered per-entry lookup/insert events; it cannot turn a partial hit into
a whole-video hit or assume pair-atomic insertion. The ten-observation
sheet's “cache hit” control qualifies only when both entries actually hit.

The tension must be real: an admission for video B can evict video A, making
the next A query cold. A baseline may bypass admission for B to preserve A.
The replay observation exposes the current public question, node, previous
build state, per-node cache occupancy, capacity and recency, and measured
prices; it does not expose the next video, future order, correct answer,
previous hidden score or full sealed outcome table. An episode has one
isolated cache namespace per node and durable cross-question state. Cache
reuse must cross distinct question/run IDs without reusing a run ID.

Use a **predeclared one-video-equivalent capacity treatment** for the cache
decision test: after deriving public artifact sizes but before any outcome or
policy result, set the tight capacity to `ceil(1.10 * largest one-video
cacheable payload)` and require each selected video's artifact to fit alone
but any two selected videos' artifacts to exceed the capacity. Otherwise stop
and redesign the
cohort/capacity *before* evaluating outcomes. Keep a second non-binding
no-pressure capacity as a control. Label the tight setting a controlled
stress condition; do not describe it as the present UpCloud default. Read the
actual deployed capacity before any change. Freeze capacity, initial empty
state, LRU version, namespace, question order and both treatment assignments.
Preserve existing volumes; use isolated new state for the test.

For example, an A, B, C, A, C, B sequence can create a real admission/eviction
choice without changing any answer. It is illustrative, **not** the chosen
sequence. A two-video strictly alternating schedule would reveal the next
video after the first two queries, making the cache decision too predictable.
The evaluator retains the seeded future schedule privately and reveals only
one question at a time.

## Representations and construction cost

R is genuine direct video. D is a **lightweight, question-independent**
derived representation with its own explicit build contract. DC is exactly
the same D artifact with cache access; it must not silently use richer
content. I is a video-level, question-independent temporal index built once
per video, followed by query-specific ranking/selection and N3 frame
projection per question. I is not a separate index built around each known
answer. Record full MP4 source reads even when N3 emits fewer frames; never
equate selected bytes with reduced storage I/O.

The recent development pilot's D build consumed dense temporal captions
also needed for I. That implementation coupling made cold D appear expensive
and must not be treated as an intrinsic cost of derived representations.
Before the new trial, either implement and freeze a genuinely independent
lightweight D build, or report the current coupled D exactly as deployed and
drop any “lightweight D” claim. In either case, charge D materialization and
I caption/vector build only when actually performed; share each verified
video-level build across that video's questions, never across unrelated
videos. Charge I anchor embedding/ranking/selected-frame projection per
question. Prebuilt inputs retain their measured one-time cost but not a
fictional live first-query build latency.

## Cohorts, order and collection budget

1. **Development:** retain all previously inspected videos/results as
   development evidence only, including the two-video/six-question 24-route
   pilot. Use one already exposed question for a ten-observation integration
   gate. This gate proves bindings and cache events, not generalization.
2. **Sealed small test:** select **three new videos, two public questions per
   video** (six questions total) from eligible NextQA validation material,
   video-disjoint from development/diagnostic examples. Predeclare the public
   selection seed and eligibility rules. Balance causal, temporal and
   descriptive strata twice over the six questions where available; do not
   select by model response or hidden outcome. If eligibility fails, stop
   before expensive preparation rather than replacing a video after seeing
   quality.
3. Freeze a nontrivial interleaved order with at least one intervening video
   between repeat visits and differing revisit distances. Do not guarantee
   simple alternation. The same question order is used for every paired path
   and baseline. Counterbalance N7/N8 and route measurement order using a
   predeclared schedule; preserve actual timestamps and provider conditions.
4. A complete ten-observation sheet for six questions means **60 full route
   observations**, not 60 policy decisions. Budget and time-box this as a
   proposal-scale pilot, with an operator-approved ceiling before submission.
   A later three-questions-per-video or ten-route larger matrix is a separate
   frozen replication, not an adaptive extension of this test.

The ten observations are collected in **isolated, reproducible state
branches**, not by letting D3/D7 measurement warm the policy's main episode.
For each question, record cold/primed cache snapshots and the exact artifact
identity for both entries, then verify hit/miss/eviction lineage. The ordered
offline policy
episode uses its own state transitions. If an action/state pair lacks a valid
measured route, measured cache-only delta, or a canonically verified
deterministic transition, the replay rejects that branch; it must not invent
a counterfactual hit, answer, latency or token bill. Targeted cache-only
probes may fill state-transition gaps without unnecessary N6 calls, but their
cost scope must be labelled separately.

## Cost, quality and replay contract

Every route receipt binds question/video ID, action, initial and final cache
state, build lineage, source bytes read, selected artifact bytes, handoff
bytes, N6 model-input bytes, request ID and provider usage, component and
end-to-end latency, and authenticated N1 score. Match N6 request usage to
route evidence rather than estimating tokens from bytes. Price provider
usage with a frozen official rate card and report **list-price cost**, not
credit-adjusted cash paid. Allocate shared VM/Root time by the recorded
experiment-time convention, with one-time build VM intervals kept distinct
from per-query VM intervals. Record storage/network charges separately when
measured. Unknown usage or rates remain unknown, never zero. Preserve cold
start, amortized after actual Q visits, and marginal warm-query views.

The offline evaluator state is per episode, video, node and representation,
with byte capacity, LRU recency, build/index state and explicit
insertion/eviction events. It advances one question at a time and returns
quality, latency and
full-path cost for the selected action; the agent never sees the sealed
future schedule or hidden outcome table. The current replay's monotone
`cache_warm_by_node` boolean and exposed `remaining_queries` are inadequate:
replace the former with capacity/eviction state, and do not reveal future
video identity through the latter. Support a limited public horizon only if
the formal observation contract explicitly allows it.

Freeze development-chosen baseline policies **before** opening the sealed
test: always-R, always-I, always-D-bypass, always-DC-admit, and one simple
capacity-aware heuristic (for example, admit only when the public estimate
of reuse benefit exceeds insertion/eviction cost). Each baseline gets the
same current-question observation. Report cumulative correctness, total
list-price cost, latency distribution, build amortization, hits/misses,
evictions and regret against a clearly labelled hindsight oracle. The
hindsight oracle is a diagnostic upper bound, not a deployable policy.

D and DC provide the same N6 semantic content; a one-off accuracy difference
is model stochasticity until repeated/controlled evidence says otherwise.
Use matched question/content digests and a predeclared small repeat subset
to estimate this variability if budget permits. Never credit the cache with
a quality improvement solely because two stochastic calls answered
differently. A negative result or an always-one-arm winner is reportable;
do not replace questions or tune the policy on sealed outcomes.

## Implementation and fail-closed gates

1. Extend the reusable experiment configuration/scheduler to support
   multi-question **x** ten-observation collection without a dated runner.
   Bind distinct question, video, run and cache-episode identities; preserve
   old single-question D0--D7 and four-arm evidence verifiers unchanged.
2. Make D construction independent or explicitly retain the coupled-cost
   label; verify I's video-build/query split and per-question N3 selection.
   Add capacity/eviction-aware replay state and sealed observation API.
3. Add focused synthetic tests: A/B/C revisit, admit-versus-bypass,
   LRU eviction, partial digest/frame hit, cross-question hit,
   node/episode isolation, invalidated
   artifact, missing counterfactual, identical D/DC semantic input, and
   legacy frozen-artifact compatibility. Avoid a full suite for a small
   isolated edit unless its impact requires one.
4. Before any live submission, run the operations-runbook checklist:
   source-bound packages and checksums, fresh IDs, N1 privacy, service and
   dependency health, endpoint/authentication probes, worker preflight,
   capacity and state-volume identity, budget, and credential-free evidence.
   An argparse/source-binding/endpoint failure is a gate failure, never a
   reason to submit a workflow for diagnosis. Record each recurring failure
   and its prevention rule in the runbook as it occurs.
5. Execute and verify the exposed integration gate, then the frozen sealed
   cohort once. Run offline baselines against the sealed trace without
   changing their definitions; publish the full negative and positive
   results with missing-cost fields visible.

This plan needs implementation and a new frozen cohort before it can be run.
It does not retroactively convert the 24-route pilot, 28-route result, or
historical ten-case smoke into a capacity-aware multi-question evaluation.
