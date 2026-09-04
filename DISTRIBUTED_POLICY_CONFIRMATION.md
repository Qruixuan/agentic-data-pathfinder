# Confirming a distributed policy: why a fresh cohort is required

The restricted two-node pilot finished. One policy passes its point
thresholds. Nothing about that authorises a commit, and this document
explains exactly why, and what a confirmation would have to be.

## Why the full-Oracle OED action space does not fit

The existing OED controller was built for a *complete design Oracle*: every
workload observed under every design, so the action "reveal design D" is
meaningful because D's outcome exists for every workload.

The distributed restricted pilot is not that. Each workload was executed under
the safe origin design **and exactly one** stratum-assigned candidate. There
is no cell for the designs a workload was never assigned, and there never will
be without running them. Reshaping this into a workload-by-design matrix would
require imputing outcomes that were never measured — which is why the audit
records `complete_design_oracle: false` and
`unobserved_design_outcomes_imputed: false`.

So the action unit changes. It is not "reveal a design"; it is:

```
one additional independent workload block in a declared stratum
```

For an **active** stratum that block means the safe *and* candidate arms at
every repetition. For a **structural-safe** stratum there is no candidate arm
to buy.

## Point thresholds are not a certificate

The selected policy's numbers:

| quantity | point estimate | interval |
|---|---|---|
| mean success delta | −0.0139 | [−0.5071, +0.4841] |
| mean cost saving | +0.7188 | [−0.3347, +1.5243] |

Both point estimates sit on the passing side of their thresholds
(−0.05 and +0.25). Both intervals comfortably contain values that fail them.
The certificate is `INSUFFICIENT_EVIDENCE`, and that is the honest reading:
**the point estimate is where the effect landed once; the certificate is what
the interval can rule out.** A 36-workload pilot with these supports cannot
rule out enough.

Treating threshold passage as a result is the specific error this layer
exists to prevent.

## Why `temporal_origin_fallback` is post-hoc

That policy was not preregistered. It was constructed *after* looking at the
pilot's stratum-level outcomes and noticing that temporal workloads did not
favour the candidate. That is a reasonable engineering read, and it is also
exactly what makes the pilot unusable as its own confirmation: the data
selected the hypothesis, so it cannot independently test it.

The frozen plan therefore records two separate things and never merges them:

* **policy-selection evidence** — the 36-workload pilot; already inspected,
  permanently spent for this purpose;
* **future certification evidence** — a fresh cohort that does not exist yet.

Disjointness is enforced on **both** workload IDs and object/video IDs. Two
different workload IDs over the same video are not independent evidence.

## Target weights versus collection allocation

These are two different things and the plan reports them separately.

**Target weights define the estimand.** Exactly one mode is supported:
`fixed_external_stratum_weights`. Weights are declared as **integer quotas**
and normalised deterministically in the generated document, which records
both forms:

```
causal=14  descriptive=8  temporal=14      (total 36)
  ->  14/36 = 7/18, 8/36 = 4/18, 14/36 = 7/18
```

They come from the benchmark's own **outcome-blind selection protocol**
(`weights_provenance`), not from a post-hoc choice. Rounded fractions such as
`0.4/0.2/0.4` are **refused**: they are not the 14:8:14 mixture, and silently
substituting them would change the target. Changing the quotas changes the
plan hash and requires an explicit operator decision.

The later certificate must aggregate stratum effects using these frozen
weights, **not** the empirical proportions of whatever cohort is collected.

**Collection allocation is a precision decision** and may legitimately differ
from the target weights — indeed it must, since structural-safe strata get
zero active blocks. The plan records
`collection_allocation.matches_target_weights` explicitly so the difference is
visible rather than assumed.

A second "cohort composition defines the estimand" mode is deliberately *not*
offered: the two are easy to conflate, and one fully validated mode is safer
than two partial ones.

Where the policy's design equals the safe design, the policy effect is
**exactly zero by construction, not by measurement**. Such a stratum:

* is recorded as `structural_zero_effect: true`;
* schedules one arm per block, never two — running the safe design twice and
  calling it a pair would manufacture a comparison that does not exist;
* is never allocated additional blocks by the OED planner, because a
  structurally zero effect cannot be estimated more precisely;
* is never counted as newly measured candidate evidence.

Within an active stratum the independent unit is one workload/object cluster.
Repetitions are averaged inside the workload before inference, so more
repetitions buy within-cluster precision and never additional independent
units.

## What the OED planner does, and does not, tell you

Greedy rule, applied only to active strata:

```
width(n)  = normalised upper − lower bound at n independent workloads,
            for the wider of the two certificate gates
score(s)  = [width(n_s) − width(n_s + 1)] / paired_block_cost(s)

active block cost           = 2 arms (safe + candidate) x repetitions
structural-safe block cost  = 1 arm x repetitions, no candidate execution
```

### The budget counts active evidence blocks, not cohort workloads

`--active-evidence-block-budget N` buys **N paired safe/candidate workload
blocks in active strata**. It is *not* an N-workload benchmark cohort with the
target mixture: structural-safe strata consume none. A budget of 40 against
the real audit yields `+17 causal, +23 descriptive, +0 temporal` — 40 paired
comparisons, 160 sessions, and no temporal candidate execution at all.

The plan reports `target_stratum_weights`,
`active_evidence_blocks_by_stratum`, `structural_zero_strata`,
`planned_safe_sessions_by_stratum`, `planned_candidate_sessions_by_stratum`,
`planned_total_sessions`, `repetitions_per_active_block`, the
`block_cost_formula`, and
`collection_allocation_matches_target_weights` so this can never be misread.

Highest score wins; ties break by `stratum_id` ascending, which makes the
allocation deterministic.

Every projected width is a **plug-in planning quantity**: the pilot's post-hoc
point estimate is held fixed while only the sample size grows. It is not
achieved power, not a confidence guarantee, and not a prediction of the
confirmation result — the true effect is unknown and is precisely what the
confirmation would test.

Allocation is fixed *before* outcomes exist. Outcome-adaptive sampling would
need an anytime-valid or alpha-spending contract, which this version
deliberately does not implement.

The planner emits only `COLLECT`, `STOP_INSUFFICIENT_BUDGET`, and
`FREEZE_CONFIRMATION_PLAN`. `COMMIT` is not in its vocabulary.

### Read the margin warnings first

Against the real audit with a 40-block budget, all four gates report a
normalised margin of 0.03–0.09 against a projected half-width of ~0.26. The
effect sits far closer to its threshold than the interval can resolve at that
cohort size. **A confirmation at this scale would very likely return
`INSUFFICIENT_EVIDENCE` again**, and the planner says so rather than letting
an operator discover it after paying for the collection.

## CLI

Freeze a confirmation plan (placeholder paths):

```bash
PYTHONPATH=. python -m pathfinder freeze-distributed-policy-confirmation \
  --config              "$PLAN_CONFIG" \
  --policy-audit-dir    "$POLICY_AUDIT_DIR" \
  --inspected-workload-manifest "$PILOT_WORKLOAD_MANIFEST" \
  --fresh-cohort-manifest       "$FRESH_COHORT_MANIFEST" \
  --output-dir          "$CONFIRMATION_PLAN_DIR"
```

Plan the future cohort allocation:

```bash
PYTHONPATH=. python -m pathfinder plan-distributed-policy-oed \
  --policy-audit-dir  "$POLICY_AUDIT_DIR" \
  --policy-id         temporal_origin_fallback \
  --target-stratum-weight causal=14 \
  --target-stratum-weight descriptive=8 \
  --target-stratum-weight temporal=14 \
  --active-evidence-block-budget 40 \
  --output-dir        "$OED_PLAN_DIR"
```

`--inspected-workload-manifest` is repeatable; pass every manifest whose
outcomes have been seen. Both commands are offline: no FlowMesh, worker, Data
Agent, MCP, or network call, and both write atomically with `SHA256SUMS`.

### Portable output

No checksummed file — manifests included — contains an absolute filesystem
path. Inputs are identified by logical role, stable filename, content hash,
schema version, and identifier only. Operator-local paths are printed to the
console under `console_only_paths` and never stored. Identical inputs under
different absolute directory trees therefore produce byte-identical output
trees, `SHA256SUMS` included.

## What the future certificate will require

The frozen plan records the contract a later evaluation must satisfy: exact
policy identity, exact execution model (`qwen3.8-27b`), exact thresholds and
supports, fresh disjoint workloads, complete safe/candidate blocks in every
active stratum, literally `true` telemetry and artifact-delivery
completeness, matching frozen stratum weights, and no post-hoc policy change.
Every non-safe certificate result retains `D_origin_remote`.

The execution model `qwen3.8-27b` is bound inside the portable plan document
and therefore inside the plan hash. A completed run on any other model —
including the earlier `qwen/qwen3.6-27b` assumption, which is **not** inherited
implicitly — fails the frozen model check.

None of that is evidence. It is a description of the experiment that would
produce evidence.

---

# The weighted certificate evaluator

The plan above is a contract. This section describes the evaluator that
enforces it once a fresh run exists.

## The weighted estimand

The target is a stratified average of per-stratum policy effects under the
frozen integer quotas:

```
overall_success_lower = Σ_s  w_s × success_lower_s
overall_success_upper = Σ_s  w_s × success_upper_s
overall_cost_lower    = Σ_s  w_s × cost_lower_s
overall_cost_upper    = Σ_s  w_s × cost_upper_s

w_causal = 14/36   w_descriptive = 8/36   w_temporal = 14/36
```

Each tail is summed separately. That is what makes the result simultaneous:
the per-stratum bounds hold jointly under the family adjustment, so any fixed
weighted combination of them holds too.

Every interval comes from the existing one-sided bounded-mean KL core in
`pathfinder.awm.certificate`. No second confidence-bound algorithm exists.

## Why collection allocation may differ from target weights

Weights define *what* is estimated; allocation decides *how precisely*.
Oversampling `causal` tenfold shrinks its interval and leaves the target
untouched, because aggregation uses `w_s`, not the sample's proportions.
Using empirical proportions would silently redefine the estimand every time
collection came out uneven — which is exactly what oversampling makes happen.

## Structural zeros

Where the policy applies the safe design the effect is identically zero. Such
a stratum contributes `point = lower = upper = 0` exactly, keeps its full
weight in the aggregation, requires no candidate record, and **consumes no
alpha**. Spending family budget on a quantity that cannot vary would widen
every other stratum's interval for nothing.

A paired record for such a workload is refused: comparing the safe design
against itself is not an observation.

## Family adjustment

```
family_size    = |active strata| × |gates| × |tails| = 2 × 2 × 2 = 8
adjusted_alpha = alpha / family_size = 0.05 / 8 = 0.00625
```

Structural-safe strata appear in no family component. The certificate records
the unadjusted alpha, family size, adjusted alpha, the full per-stratum /
gate / tail allocation, and which strata consumed none.

## Decision rules

```
success non-inferiority:  overall_success_lower >= -delta_success_margin
cost improvement:         overall_cost_lower    >= minimum_cost_saving
```

- `SAFE_TO_COMMIT` — both lower-bound gates pass and every frozen requirement
  is satisfied.
- `UNSAFE` — an upper bound establishes violation of at least one gate.
- `INSUFFICIENT_EVIDENCE` — otherwise.

Every state other than `SAFE_TO_COMMIT` applies `D_origin_remote`.

## Why point-threshold passage is insufficient

The certificate reports both, separately. A point estimate on the passing side
of a threshold says where the estimate landed; the certificate says what the
interval can rule out. The pilot passed both point thresholds and still
returned `INSUFFICIENT_EVIDENCE` — that is the normal case, not an anomaly.

## Why the old post-hoc evidence is rejected

The 36-workload pilot selected the policy. The evaluator refuses any evidence
whose `pilot_id` equals the plan's recorded selection pilot, refuses reuse of
its workload and object IDs, and requires the collected active workloads to
equal the plan's frozen fresh cohort exactly.

## Claim eligibility is not promoted automatically

Statistical state and claim eligibility are separate. A `SAFE_TO_COMMIT`
certificate on a fresh holdout is a valid *engineering* result. It becomes a
scientific claim only if the frozen plan's threshold and sampling provenance
independently meet those requirements, and the evaluator never promotes
`eligible_for_scientific_claims` on its own.

Recorded digests establish **consistency and reproducibility, not
authenticity**. Nothing is signed.

## CLI

```bash
PYTHONPATH=. python -m pathfinder certify-distributed-policy-confirmation \
  --plan-dir           "$CONFIRMATION_PLAN_DIR" \
  --evidence-dir       "$FRESH_EVALUATION_DIR" \
  --execution-evidence "$EXECUTION_EVIDENCE_JSON" \
  --output-dir         "$CERTIFICATE_DIR"
```

The execution-evidence manifest
(`pathfinder.distributed-execution-evidence/v1alpha1`) is the smallest
portable binding of a run to its plan and runtime model:

```json
{
  "schema_version": "pathfinder.distributed-execution-evidence/v1alpha1",
  "execution_model_id": "qwen3.8-27b",
  "confirmation_plan_sha256": "<sha256 of confirmation_plan.json>",
  "evaluation_sha256": "<sha256 of evaluation.json>",
  "pilot_id": "<fresh confirmation pilot id>",
  "run_id": "<run identifier>",
  "evaluation_id": "<evaluation identifier>"
}
```

It exists because the distributed evaluation format records what happened but
cannot by itself prove which runtime model produced it.

## OED projections use the same core

The planner now calls the identical weighted aggregation core, feeding it the
pilot's point estimates repeated across projected workload counts.

The plug-in projection holds each future stratum mean at its post-hoc point
estimate. The bounded-KL interval is determined by the declared support,
independent workload count, allocated alpha, and assumed mean; it does not
estimate or assume an empirical within-stratum variance. A fresh cohort may
shift the stratum means and therefore may produce either better or worse
certificate bounds.

Projections are labelled `posthoc-plugin-planning-projection`,
`not-achieved-power`, `not-a-confidence-guarantee`,
`not-a-commit-authorization`.

## Minimum independent workloads

The plan freezes a floor per active stratum:

```json
"minimum_independent_workloads_by_active_stratum": {
  "causal": 3, "descriptive": 3
},
"minimum_independent_workloads_provenance":
  "engineering-placeholder-not-scientifically-justified"
```

Every active stratum must appear exactly once; structural-safe strata must
not appear at all, having no candidate observations to count. Repetitions
never count toward the floor. A stratum below its floor yields
`INSUFFICIENT_EVIDENCE` **even when both numerical bounds pass** — a bound
computed from three clusters is arithmetically valid and evidentially thin,
and the plan is where that judgement belongs. An *established violation*
(`UNSAFE`) is retained, since both states fall back to the safe design and
discarding a harm signal would be the worse error.

The value `3` in the template is an explicitly labelled engineering
placeholder, not a scientifically justified figure. The OED planner seats
these floors first, as feasibility constraints, before optimising precision.

## Feasibility: three distinct answers

A projection that does not pass has two very different causes, and the
planner classifies which:

| classification | meaning |
|---|---|
| `POINT_ESTIMATE_BELOW_THRESHOLD` | the assumed point estimate is already on the failing side, so under the fixed-effect assumption no sample size helps — the interval would have to be centred somewhere it is not |
| `PROJECTED_NOT_WITHIN_SEARCH_BUDGET` | the point estimate passes, but no tested allocation up to the search limit narrows the lower bound enough; a statement about the budget, not about possibility |
| `PROJECTED_PASS_WITHIN_SEARCH_BUDGET` | at least one tested allocation passes both weighted gates |

The search is a deterministic bounded ladder (50 … 25,600 active evidence
blocks). It never searches unboundedly: an arbitrarily large "passing" cohort
is not a useful planning answer.

**Current real-audit result.** The weighted cost-saving point estimate is
`0.2522193762995198` against a `0.25` minimum — a margin of `+0.00222`, on the
passing side but minute. No allocation up to 25,600 blocks passes, so the
classification is **`PROJECTED_NOT_WITHIN_SEARCH_BUDGET`**, *not* mathematical
impossibility. An earlier draft of this document asserted that sample size
alone could not fix it; that was wrong, and the classification above replaces
it.

## Allocation arithmetic

Two invariants are asserted in code and tested:

```
sum(active_evidence_blocks_by_stratum) == active_evidence_block_budget
planned_total_sessions == Σ_s (blocks_s × 2 arms × repetitions)
```

At a 200-block budget against the real audit the allocation is
`causal: 100, descriptive: 100` (sum 200) with `temporal` receiving zero, and
`planned_total_sessions = 200 × 2 × 2 = 800`. Note that *blocks allocated* and
*projected independent workloads* are different quantities and are reported
separately; do not read one as the other.

Projections are computed over **freshly allocated blocks only**. The pilot's
workloads selected the policy and cannot serve as its confirmation evidence,
so counting them toward projected precision would overstate what the fresh
cohort can establish.
