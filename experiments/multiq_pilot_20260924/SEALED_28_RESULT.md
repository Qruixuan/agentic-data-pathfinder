# Seven-question interleaved pilot: verified exploratory result

Execution: `sealed-28route-20260924t080919z-c7df0003` (2026-09-24).
Runtime source was frozen at Git commit `3727a92106028ad57ec979f5ccd18db43e868d58`.
The seven public questions span two videos and four physical route arms each.
Question seven was selected from those same two videos by a predefined seed,
without reading answers or prior outcomes. The policy specification was frozen
before this batch produced any held-out result.

The runner exited 0. An independent, source-bound verifier accepted all 28
route evidences, N1 scoring authenticity, timestamps, and the 58 checksummed
output files. FlowMesh resolved the pinned alias to observed worker `wkr-2`;
that ID is an observation, not a future contract. N6's durable numeric usage
journal increased from 24 to 52 completed rows. All 28 new route results
matched one-to-one by both request and result SHA-256. No hidden-label values,
credentials, raw prompts, or answer text are present in the public cost export.

| Arm | Correct / 7 | N6 input tokens | Cached input | Output tokens | N6 official list price (USD) |
| --- | ---: | ---: | ---: | ---: | ---: |
| R | 7 / 7 | 56,827 | 1,664 | 3,142 | 0.037173900 |
| D | 7 / 7 | 27,322 | 0 | 4,984 | 0.028613000 |
| DC | 7 / 7 | 27,322 | 14,592 | 7,117 | 0.029175200 |
| I | 7 / 7 | 27,322 | 0 | 10,179 | 0.044198000 |

The DC evidence shows two first-use misses (one per video), followed by five
hits. Its total N6 list-price cost is slightly above D in this one batch
because output-token counts differed; a cache hit does not promise a lower
total when model output varies. This is not a repeated latency or quality
comparison.

The frozen Singapore list-price calculation uses `qwen3.8-27b` noncached
input $0.50, implicit-cache input $0.10, and output $3.00 per million tokens;
`text-embedding-v4` input is $0.07 per million. These are comparable standard
prices, not the account's paid invoice. The snapshot is in the cost audit and
runbook. Official references:
[qwen3.8-27b](https://www.alibabacloud.com/help/en/model-studio/qwen3-8-27b),
[model pricing](https://www.alibabacloud.com/help/en/model-studio/model-pricing).

| Known provider bucket | USD |
| --- | ---: |
| 28 N6 route inferences | 0.139160100 |
| Two video-level caption builds | 0.087108000 |
| Two video-level index embeddings | 0.000374010 |
| Seven query embeddings | 0.000003850 |
| **Total known provider list price** | **0.226645960** |

A separate retrospective UpCloud rate-list check found a continuously
running nine-VM plan cost of $0.127227/hour in `sg-sin1`. Prorating it over the
batch's exact 707.462051 seconds gives **$0.025002298** of shared server-plan
cost, so known provider price plus this declared VM-time allocation is
$0.251648258. The check is recorded in
`artifacts/multiq-sealed-28route-vm-allocation-20260924-v1/` and is **not**
an observed incremental invoice charge; UpCloud bills by started hour.
The API price snapshot was read after execution, so this supplemental
allocation is retrospective, not a pre-frozen rate-card result.

The five frozen baseline policies replay against exactly the observed arm for
each public question. DC replay checks the real miss/hit transition; no
unobserved counterfactual answer is fabricated.

| Frozen baseline | Correct / 7 | Known provider list price (USD) |
| --- | ---: | ---: |
| always-R | 7 / 7 | 0.037173900 |
| always-D | 7 / 7 | 0.028613000 |
| always-DC | 7 / 7 | 0.029175200 |
| always-I | 7 / 7 | 0.131683860 |
| first-R-then-I | 7 / 7 | 0.135701950 |

These baseline totals are **partial**: I is charged its frozen caption/index
build and query embeddings once at the applicable state transition, whereas
D/DC's historical derived-generation and N4-publication cost is not fully
recoverable. Historical frame decode, query-specific projection compute,
storage/network, and VM/Root experiment-time allocation are also not priced
from verified cloud-rate evidence. The full-path cost and policy winner are
therefore `null`, not zero or `always-D`. Failed provider attempts may have
unobserved charges.

This batch is exploratory proposal evidence, not a scientific claim. These
two videos were excluded from the immediate 24-route development batch but
had appeared in a broader earlier cohort; they are not globally unseen.
All arms answering all seven questions correctly shows the present small set
does **not** yet demonstrate a quality-constrained path choice. The planned
four-route development supplement has not been executed. Before a formal
RSI-Exam evaluation, freeze genuinely video-disjoint held-out questions,
complete cold-build and VM pricing, then rerun the predeclared baselines
without policy changes based on held-out outcomes.

Evidence: `artifacts/multiq-sealed-28route-20260924t080919z-c7df0003/`
contains checksummed route, N6 usage, cost, and baseline-replay directories.
