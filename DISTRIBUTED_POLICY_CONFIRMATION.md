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
