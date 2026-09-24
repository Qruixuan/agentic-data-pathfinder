# Sealed 60-route multi-question pilot (2026-09-25)

This is a descriptive engineering pilot, **not** a held-out RSI-Exam result or
a scientific estimate of accuracy, performance or cost. The six public
questions from three videos were interleaved under the frozen schedule. Each
question had the original ten measured executions: N7/N8 × R, I, D, DC-miss,
DC-hit. The source-bound verifier returned
`VERIFIED_INTERLEAVED_BATCH_OUTPUT`; 60/60 FlowMesh routes completed, all 60
N1-authenticated scores were correct, and no case was unavailable. The full
route run took 559.889433 seconds (18:47:24–18:56:44 UTC on 2026-09-24).

## What was reused

The existing `experiments.interleaved_batch` runner and canonical verifier
submitted and sealed the routes. Existing N6 numeric-usage export code read
the two durable N6 journals. The existing `TenRouteEpisodeReplay` and
`ReplayCacheState` performed offline policy evaluation; the new t60 adapter
only joins frozen evidence and validates cache events. Its shared-caption
build-cost mode preserves the legacy cost contract for prior callers. No
FlowMesh source was changed.

## Provider API list-price evidence

Each of the 60 semantic result SHA-256 values uniquely matched one numeric
N6 usage row: 30 in the N6 `:18886` journal and 30 in the separate N6
`:8780` journal. Each matched result had exactly one completed HTTP 200
attempt and no retry/failure attempt. The frozen Alibaba Cloud Singapore
rate snapshot applies
USD per million tokens: qwen3.8-27b regular input 0.50, cached input 0.10,
output 3.00; text-embedding-v4 input 0.07. This is **list-price modeling**,
not the discounted invoice or a cash charge. Captions, video-index embeddings
and question embeddings are counted once at their actual build/query scope.
The frozen rate card links the [qwen3.8-27b pricing](https://www.alibabacloud.com/help/en/model-studio/qwen3-8-27b)
and [text-embedding-v4 pricing](https://www.alibabacloud.com/help/en/model-studio/text-embedding-v4).

| Measured activity | Provider API list price (USD) |
| --- | ---: |
| 60 N6 route inferences | 0.108997600 |
| 27 question-independent caption requests (3 videos) | 0.134890000 |
| 3 video-index embedding requests | 0.000423290 |
| 6 question embedding requests | 0.000004690 |
| **All activity above** | **0.244315580** |

The table above is the **actual experimental workload at list price**: it
includes all 60 routes, not one hypothetical policy. The offline baseline
table below instead charges only the six routes a particular policy would
choose, plus the video builds it would actually need. The same caption build
is shared by I, D and DC; it is not charged twice if a policy switches arms.
Index query embeddings are charged only on I. These values **exclude** VM,
storage, network and historical build-period machine time.

| Six-question N7 offline baseline | Correct | Provider API list price (USD) |
| --- | ---: | ---: |
| Always R | 6/6 | 0.013376400 |
| Always I | 6/6 | 0.143958980 |
| Always D | 6/6 | 0.146255600 |
| Always DC | 6/6 | 0.145071200 |
| No-eviction cache admission | 6/6 | 0.146252600 |

N8 fixed-policy and no-eviction values are in the sealed replay artifact.
The no-eviction policy chose DC on the first question, bypassed caching for
the three intervening questions, and selected the *measured* DC-hit branch
when that first video returned on question five. Always-DC selected six
measured cold-miss branches because the frozen one-video-equivalent capacity
forced eviction as the videos alternated. A cache hit avoids artifact fetch,
not the N6 inference request; its provider API saving is therefore not a
general cache-benefit estimate.

The real cache volumes each recorded 12 MISS, 12 STORE and 12 HIT events plus
10 verified LRU evictions. The live measurement controls used a fresh
namespace per question/node. The offline policy episode uses a distinct
shared cross-question namespace and matches only the already measured
miss/hit branch whose finite cache transition agrees with the durable event
identities. It is **counterfactual replay**, not a live shared-namespace
trace. All ten baselines completed without `UNSUPPORTED_ACTION`; unknown
states would fail closed.

## What cannot be claimed

- **No quality decision space in this cohort.** All ten routes per question
  answered correctly. The six-question provider API baseline makes always-R
  cheapest because video-level captioning is amortized over only two
  questions per video. This is a legitimate negative result, not evidence
  that the RSI policy task is solved or that index/cache are generally useless.
- **N7 versus N8 latency is confounded.** N7 used N6 at `:18886`; N8 used a
  different healthy N6 container at `:8780` on the same host. Both returned
  qwen3.8-27b, but images and usage journals differ. Do not attribute a
  cross-node timing gap to placement. The run was frozen before this was
  discovered; no origin was silently changed afterwards.
- **Full dollar path cost is still partial.** Route and provider-build token
  usage is complete. The observed batch interval is 559.889433 seconds, so
  an analytic shared-VM allocation is
  `0.1555248425 × sum(active Root + N1–N8 hourly USD rates)`.
  This is not the UpCloud invoice: the account-specific hourly rate snapshot
  and historical build-period VM intervals were not captured. Neither is
  imputed as zero. Storage and any other charged resources also remain
  outside the provider API table.
- Six questions/three videos provide no statistical inference. These
  questions have no quality disagreement and should not be treated as a
  representative held-out RSI-Exam evaluation.

## Evidence and next gate

- Sealed route evidence: `artifacts/t60-route-output-v1/routes-v1/`.
- Exact N6 result-hash/token join and build accounting:
  `artifacts/t60-list-cost-accounting-v3/`.
- Read-only, checksum-sealed durable cache events:
  `artifacts/t60-cache-events-v2/`.
- Canonically reverified offline baselines:
  `artifacts/t60-offline-baselines-v5/`.
- Pre-submit and deployment identities:
  `experiments/ten_route_multiq_20260925/PRE_SUBMIT_LEDGER.md` and
  `artifacts/t60-deployment-specs-v5/`.

Before another paired N7/N8 run, pin both coordinators to the **same**
specified N6 image/origin or explicitly design a replica factor, then run the
new no-call origin check. For an RSI-Exam quality/cost task, choose a larger
video-disjoint, outcome-blind cohort with more questions per video. Use its
development split to diagnose whether physical actions disagree on quality;
do not select or revise the held-out split by peeking at its labels. A true
live cross-question cache
episode may be valuable as a separate validation of the offline state model.
