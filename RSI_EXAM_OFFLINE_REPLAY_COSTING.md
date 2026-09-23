# RSI-Exam replay list-price accounting

`pathfinder.rsi_exam.offline_replay_costing` is an additive layer over the
immutable v2 replay and materialization-cost receipts. It does not rewrite
outcomes, invoke a provider, or claim an actual invoice amount.

The fixed snapshot dated 2026-09-23 uses Alibaba Cloud Model Studio's
Singapore public prices: `qwen3.8-27b` input $0.50, implicit-cache input
$0.10, output $3.00, and `text-embedding-v4` input $0.07, all per million
tokens. Source URLs and rates are stored in `PRICE_SNAPSHOT`. Credits,
promotions, taxes, exchange rates, and measured UpCloud resource charges are
excluded. The default is a **standard no-cache scenario**; provider cache
hits are not inferred from a lower bill. An optional provider-log workbook
can be reconciled by *unique* input/output token-count pairs to recover
observed implicit-cache usage; duplicate or missing matches fail closed.
Only model, status and numeric usage are read; account identifiers, request
IDs and client IPs are not copied to the table.

Caption input and output units come from the verified per-object receipt,
including paid validation retries. Embedding input units were measured only
for 12 batches. The cost layer allocates each batch's measured token total
among its inputs in source-builder order (all caption texts by object and
window, then one anchor per object). It uses largest-remainder rounding, so
the allocated object totals sum to the measured cohort total. This is a
**deterministic accounting allocation, not measured per-object tokens**.

The read-only command below verifies the receipt before printing the object
table. It does not print prompts, answers, credentials, or hidden labels:

```text
python -m pathfinder.rsi_exam.offline_replay_costing
  --cost-evidence-dir <verified-materialization-cost-evidence-directory>
  --caption-package-dir <bound-caption-package-directory>
  --index-package-dir <bound-index-package-directory>
  [--provider-log-xlsx <request-log-workbook.xlsx>]
```

For the frozen 12-object receipt whose `SHA256SUMS` digest is
`edd42b7160c80c3152fd32120f9967265831272d0f720928fe78ae5634bff1d5`,
the standard no-cache calculation gives 172,066 caption input tokens,
170,187 caption output tokens, and 35,098 embedding input tokens. The
cohort's **known caption-plus-embedding list price is $0.599050860**.
Per-object known build prices range from $0.042489220 to $0.057536190.
These are not full cold-path prices and should not be compared with an
invoice total.

With the supplied request-log workbook (SHA-256
`01d0acfa87c72ee90af2007918b49e903aa6d28bae9c53ba45ef1fe7539d7526`),
all 111 frozen caption request token pairs matched uniquely. The matched
records show 768 implicit-cache input tokens. Pricing those at the separate
cache rate changes the **known build-model subtotal to $0.598743660**.
This is a list-price estimate for the frozen cohort, not an invoiced amount.

| Object suffix | Caption USD | Allocated embedding USD | Cold caption+embedding USD |
| --- | ---: | ---: | ---: |
| 2834146886 | 0.042290000 | 0.000199220 | 0.042489220 |
| 2976913210 | 0.046940000 | 0.000230650 | 0.047170650 |
| 3462517143 | 0.047693000 | 0.000216720 | 0.047909720 |
| 4130504920 | 0.051391000 | 0.000214690 | 0.051605690 |
| 4260763967 | 0.044309000 | 0.000189770 | 0.044498770 |
| 4942054721 | 0.057335500 | 0.000200690 | 0.057536190 |
| 5296635780 | 0.055909000 | 0.000221130 | 0.056130130 |
| 5735711594 | 0.046257000 | 0.000184380 | 0.046441380 |
| 5840177726 | 0.049949000 | 0.000186620 | 0.050135620 |
| 8132842161 | 0.056419000 | 0.000257740 | 0.056676740 |
| 8547321641 | 0.044818000 | 0.000192640 | 0.045010640 |
| 9088819598 | 0.052976300 | 0.000162610 | 0.053138910 |
| **Total** | **0.596286800** | **0.002456860** | **0.598743660** |

All object IDs have the `nextqa-val-` prefix. Cold charges apply only to
the components an action actually builds. After a component is built and
reused, its *incremental provider build cost* is zero; every new query still
incurs its own, presently unbound N6 inference cost.
This table is a reproducible analysis result, **not yet a frozen formal
replay package**: the costing code must be committed and run from a clean Git
archive before source-bound experiment publication.

For a v2 replay result, `price_replay_result` charges caption and embedding
list prices only when their build components first appear. It therefore
separates cold-build from warm/reuse queries. N6 inference prices require
request-level `input_units`, `cached_input_units`, and `output_units` bound to
each exact replay `outcome_id`. Source object bytes or model-input bytes must
never be converted into token counts. If N6 usage is absent, the N6 field and
complete provider total stay null; the reported known subtotal is **not** a
complete path price.

Current formal evidence does **not** contain the required N6 request-level
usage binding: the historical N6 service did not retain the provider's
`usage` response. Historical provider logs cannot be joined to individual
route outcomes solely by time or byte size. New N6 instrumentation records
only four validated token counts and the canonical N6 request/result digests
in `/state/n6-provider-usage-v1.sqlite3`. It never records the prompt, answer,
API key, or full provider response. The result digest is the exact value in
the coordinator's independently verified route evidence, so a fresh smoke
can be joined by digest rather than by timestamp. The N6 health response
exposes a journal-write error count; a nonzero count or a missing digest
makes that run's N6 cost incomplete, not zero.

`price_verified_smoke_n6_usage` prices one freshly verified ten-route smoke
using this journal. It does not rewrite historical outcomes or reconstruct
the earlier 360 trials. The first bounded collection is ten paths; scaling
to the full cohort requires a separate measured run and verification.

Even with complete N6 usage, CPU decode, N4 publication/storage, data
transfer, VM occupancy, and latency are separate cost dimensions. The
`complete_path_cost_usd` field remains null until those dimensions are
measured or explicitly excluded by a preregistered objective.
