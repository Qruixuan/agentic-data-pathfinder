# Adaptive Workload Model MVP

## Purpose

The AWM implementation consumes a completed, frozen Reduced Oracle run and
constructs uncertainty envelopes over each design's access response, task
success, service cost, transition cost, and horizon objective `Phi(D)`.

It deliberately exposes one interface for three models:

1. `assumption_free_box`: affordability and the per-session access cap only;
2. `independent_box`: joint-confidence intervals for directly observed
   designs, with no propagation to unobserved designs; and
3. `coupled_awm`: the same direct intervals plus only the structural
   assumptions explicitly enabled in the AWM config.

Every model provides:

```text
lower_bound(design)
upper_bound(design)
pessimistic_gain(current, candidate)
optimistic_gain(current, candidate)
```

This is the interface used by the offline OED Commit/Reveal/Hold controller.
See [`OED.md`](OED.md).

## Input and Split Contract

The evaluator reads each design's original `runs.jsonl`; it does not infer
sample uncertainty from `oracle_table.csv`. Repetitions are split as paired
blocks across every design. With the current three-repetition Oracle config:

- repetitions 0 and 1 form the training history; and
- repetition 2 is the untouched held-out response vector.

`observed_design_ids` controls which training records are visible to the model.
The committed MVP exposes only `D_remote_digest`, even though the exhaustive
Oracle output contains both designs. Candidate training records remain hidden
until an OED Reveal explicitly adds that design to the observed history;
candidate holdout records remain evaluation-only. This recreates the
censored-history setting without discarding the candidate ground truth needed
for evaluation.

Only records classified as `completed`, with complete telemetry and an
evaluable task answer, enter an estimate. Excluded training and holdout counts
are reported in the manifest. The committed config permits an excluded
fraction of zero for both partitions. Exceeding either configured limit, or
leaving a configured observed design with too few eligible training sessions,
fails closed.

## Confidence and Constraint Contract

Access, substitution-group access, and success use Wilson intervals with a
Bonferroni allocation across the complete observed response vector. Service
cost uses a bounded Hoeffding interval intersected with the access-weighted
unit-cost envelope. A cost observation outside the configured support
invalidates the model. Transition cost uses a declared relative radius because
the current Oracle records only one forward and one restoration transition per
clean design run. AWM Commit bounds use the forward interval; OED adds the
separate restoration interval and probe-window loss for a complete Reveal.

The following empirical assumptions are separately gated:

- own-price monotonicity;
- substitution-group total monotonicity;
- success monotonicity; and
- quoted-price sufficiency.

Cross-design constraints cannot be enabled unless quoted-price sufficiency is
also enabled. An enabled assumption must also carry an explicit passed or
validated status; a pending or failed status is rejected. Substitution groups
must be declared, disjoint catalog groups.
The v1alpha1 analytic solver supports `max_accesses=1`; it solves the resulting
access-cost extrema exactly under individual bounds, disjoint group floors,
affordability, and the class access cap.

### AWM v2 paired-gain contract

`pathfinder.awm/v2alpha1` is an opt-in, backward-compatible confidence
contract. It addresses two limitations found in the first four-design run:

- the marginal Bonferroni family is fixed over the complete preregistered
  design domain, so revealing another design cannot change an existing
  design's alpha allocation; and
- when both designs have observations for the same workload, task class,
  quote profile, latency multiplier, repetition, and Pathfinder seed, Commit
  uses the paired per-session utility difference directly.

For candidate `c` and incumbent `i`, each paired observation is

```text
task_value * (success_c - success_i)
  - resource_weight * (service_cost_c - service_cost_i)
```

The implementation places a two-sided empirical Bernstein interval on that
bounded difference, scales it by the declared horizon, subtracts the storage
difference, and finally subtracts the candidate's forward-transition
interval. Alpha is allocated across every unordered design pair and a fixed
preregistered maximum number of looks. OED refuses to run if its configured
maximum iterations exceed that look budget.

Pairing fails closed. With `require_complete_pairs=true`, the eligible keys
must be identical across every observed design in the training partition and
across every design in the holdout partition. Missing, duplicated, or
misaligned keys are rejected rather than silently converted to independent
samples. When either design is unobserved, the controller falls back to the
old marginal `Phi`-difference interval and records that source explicitly.

Within the OED path, this is a fixed-look sequential guarantee under the
declared bounded, independent paired-sampling-unit model; it is not an
anytime-valid confidence sequence. Changing the design domain, alpha split,
maximum looks, pairing rule, or sampling-unit interpretation after seeing the
result creates a new exploratory analysis. The fixed-look bound follows the
empirical Bernstein construction of Maurer and Pontil (2009); an anytime-valid
replacement is future work rather than a current claim.

### AWM v2alpha2 fixed-snapshot diagnostic

`pathfinder.awm/v2alpha2` narrows the preregistered paired family without
changing or overwriting the frozen v1 or v2alpha1 results. Its confidence
block must list directed `paired_comparisons` explicitly. For the current
four-design diagnostic, the family contains only the three decision-relevant
comparisons from `D_origin_remote` to each candidate. Candidate-to-candidate
and reverse comparisons fall back to marginal bounds and cannot silently
reuse the paired certificate.

The required `look_semantics` is
`fixed-training-snapshot-per-pair`, with `maximum_looks=1`. OED may read the
same immutable training snapshot in several controller iterations because
the fixed-family intervals do not change. Adding, removing, repairing, or
otherwise changing a training observation creates a new statistical look and
is outside this contract. Each certificate records a canonical SHA-256 of the
paired success/cost snapshot so repeated OED reads can be audited without
recording prompts, answers, or credentials. v2alpha1 retains its old
controller-iteration look accounting for reproducibility.

Each paired certificate exposes the quantities needed to diagnose a vacuous
interval: paired success and service-cost means and variances, their sample
covariance, the empirical-variance radius, the bounded-range radius, the
unclipped interval, and whether the final interval was clipped to support.
The direct paired utility difference remains the certified statistic; the
component breakdown is diagnostic and does not assume success and cost are
independent.

The evaluator also writes a plug-in sample-size table. It projects the sample
counts at which the interval would become unclipped, reach 50%, 25%, and 10%
of support width, or obtain a positive Commit lower bound while holding the
observed mean and variance fixed. These are planning estimates, not achieved
power or coverage guarantees. They additionally rely on bounded, independent
paired sampling units; clustered workloads require a cluster-aware analysis.

References:

- Andreas Maurer and Massimiliano Pontil, “Empirical Bernstein Bounds and
  Sample Variance Penalization,” COLT 2009:
  <https://www.cs.mcgill.ca/~colt2009/papers/012.pdf>
- Steven R. Howard et al., “Time-uniform, nonparametric, nonasymptotic
  confidence sequences,” *Annals of Statistics*, 2021:
  <https://doi.org/10.1214/20-AOS1991>

### AWM v3alpha1 cluster-first decomposed diagnostic

`pathfinder.awm/v3alpha1` is a separate post-hoc diagnostic motivated by the
v2alpha2 finding that its finite-support range term dominated the certificate.
It does not rewrite any v1 or v2 result. Its primary sampling unit is one
independent workload cluster, not every repeated Agent run. The current
contract requires `cluster_key_fields=["workload_id"]` and deterministically
uses the lowest-repetition, then lowest-seed paired observation in each
cluster. A certificate records both the raw pair count and the smaller
independent-unit count. Repetitions discarded by this rule remain useful for
robustness analysis but do not inflate the primary certificate's sample size.

The v3 per-session gain certificate is component-decomposed:

1. paired binary success is represented by the two discordance rates
   `P(candidate succeeds, incumbent fails)` and
   `P(candidate fails, incumbent succeeds)`;
2. each discordance rate receives a conservative two-sided Bernoulli-KL
   interval, and their difference bounds the success-rate difference;
3. paired service-cost differences receive a bounded empirical Bernstein
   interval; and
4. the success and cost intervals are combined through the declared utility
   function before horizon, storage, and transition adjustments.

The paired alpha assigned to each directed comparison is split explicitly
between success and cost. The committed diagnostic uses 80% for success and
20% for cost. This split was selected during post-hoc method development and
is not confirmatory. The certificate assumes independent workload clusters
drawn from a common target distribution. It is fail-closed about missing or
misaligned pairs, but it is not a cluster bootstrap, an anytime-valid
sequence, or evidence that the workload clusters are representative.

Power planning is also expressed in independent workload clusters. It holds
the observed discordance probabilities and cost moments fixed while projecting
component interval widths and the Commit lower bound. Those projections are
planning diagnostics only.

Additional references for this construction:

- Aurélien Garivier and Olivier Cappé, “The KL-UCB Algorithm for Bounded
  Stochastic Bandits and Beyond,” COLT 2011:
  <https://proceedings.mlr.press/v19/garivier11a.html>
- Robert G. Newcombe, “Improved confidence intervals for the difference
  between binomial proportions based on paired data,” *Statistics in
  Medicine*, 1998 (paired-binary score intervals provide context, but are not
  the fail-closed v3 certificate):
  <https://pubmed.ncbi.nlm.nih.gov/9839354/>

### AWM v3alpha2 complete-block cluster-mean diagnostic

`pathfinder.awm/v3alpha2` is an isolated correction to the v3alpha1
first-repetition rule. The server sensitivity check showed that a candidate's
paired utility could change sign between the two fixed training repetitions.
Selecting only the lowest repetition was therefore deterministic but not a
stable point estimator. v3alpha2 preserves the independent sampling unit—the
workload object—while using every preregistered repetition inside that
workload's fixed block.

For each workload cluster, v3alpha2 computes:

- the mean paired success difference across the complete repetition block;
- the mean paired service-cost difference;
- the fraction of repetitions with positive and negative success
  discordance; and
- the mean paired utility difference.

The eight workload-level values, not the 16 raw Agent runs, enter the
cross-workload confidence calculation. All clusters must contain exactly one
paired observation for every common repetition ID. Missing repetitions or
duplicate `(workload, repetition)` observations fail closed. The frozen
snapshot hash covers all raw observations in the block, so changing a later
repetition invalidates the certificate.

The positive and negative discordance fractions are independent bounded
variables in `[0,1]` across workload clusters. v3alpha2 applies the same
binary-KL inversion to their sample means as a conservative bounded-variable
Chernoff/Hoeffding construction; it does not claim that the cluster means are
Bernoulli trials. The cost component remains empirical Bernstein over
workload-level mean cost differences. The trace additionally reports
per-repetition utility means and whether their signs disagree. A sign flip is
a sensitivity warning, not an automatic rejection of the pooled certificate.

Power planning grows only the number of independent workloads while holding
the number of repetitions per workload fixed. It reports the current interval
width explicitly and remains a post-hoc planning diagnostic, not achieved
power.

Additional reference for the bounded-variable construction:

- Wassily Hoeffding, “Probability Inequalities for Sums of Bounded Random
  Variables,” *Journal of the American Statistical Association*, 1963:
  <https://doi.org/10.1080/01621459.1963.10500830>

### AWM v3alpha3 direct bounded-utility diagnostic

`pathfinder.awm/v3alpha3` keeps the v3alpha2 evidence unit and snapshot
unchanged: one independent value per workload, formed by averaging its
complete preregistered repetition block. It changes only the primary paired
gain certificate. Each workload-level utility difference is normalized from
its declared finite support to `[0,1]`; one bounded-mean binary-KL inversion
then produces the primary utility interval, which is mapped back to the
original per-session scale.

This avoids adding a success interval to a separate service-cost interval
when the decision variable is their paired utility combination. The existing
discordance and cost decompositions remain in CSV, JSON, and OED trace output
for diagnosis, but `component_interval_role` marks them as
`diagnostic-only-not-part-of-primary-confidence-family`. They must not be
substituted for, intersected with, or added to the direct primary certificate.

The method remains fixed-snapshot and post-hoc. It does not change the number
of independent workloads, cure workload-selection bias, or turn the frozen
Oracle into confirmatory evidence. Its power projection varies only the
number of independent workload clusters while holding the within-workload
repetition block and normalized point estimate fixed.

The committed [`awm_reduced_mvp.json`](../../configs/awm_reduced_mvp.json)
keeps every empirical assumption disabled. This is intentional: coupled AWM
should initially match the independent model. After Phase B is reviewed, copy
the config to a new version, enable only the assumptions whose gates passed,
record their status, and use a new output directory.

## Run After the Reduced Oracle

No FlowMesh worker, Data Agent, MCP Gateway, LLM key, or FlowMesh PAT is needed
for this step. It is an offline, read-only analysis of the Oracle directory and
writes only to the requested AWM output directory.

```bash
cd "$HOME/agentic-data-pathfinder"
source "$HOME/.venvs/pf312/bin/activate"

export PF_ORACLE_ROOT="$HOME/pathfinder-reduced-oracle-mvp"
export PF_ORACLE_OUT="$PF_ORACLE_ROOT/run-v1"
export PF_AWM_OUT="$PF_ORACLE_ROOT/awm-v1"

PYTHONPATH=. python -m pathfinder evaluate-awm \
  --awm-config configs/awm_reduced_mvp.json \
  --oracle-config configs/reduced_oracle_mvp.json \
  --oracle-output-dir "$PF_ORACLE_OUT" \
  --output-dir "$PF_AWM_OUT"
```

## Run Against the Synthetic Fixture

The same command reads a synthetic fixture directory, which is useful when
real Oracle execution is blocked. Point it at the dedicated synthetic
contracts, never at the committed real ones:

```bash
PYTHONPATH=. python -m pathfinder generate-synthetic-oracle \
  --fixture-config configs/synthetic_oracle_fixture.json \
  --output-dir "$PF_SYNTHETIC_OUT/oracle"

PYTHONPATH=. python -m pathfinder evaluate-awm \
  --awm-config configs/synthetic_multi_candidate_awm.json \
  --oracle-config configs/synthetic_multi_candidate_oracle.json \
  --oracle-output-dir "$PF_SYNTHETIC_OUT/oracle" \
  --output-dir "$PF_SYNTHETIC_OUT/awm"
```

`configs/synthetic_multi_candidate_awm.json` observes only the synthetic safe
incumbent and keeps every empirical assumption disabled, exactly like the
committed real config. Coupled AWM therefore still matches the independent
model over the fixture: the fixture widens the *design domain*, it does not
enable any structural coupling, and it must not be used to argue that coupling
helps. Fixture-derived bounds, containment results, and false-safe counts are
engineering output only; see the scientific boundary in
[`REDUCED_ORACLE.md`](REDUCED_ORACLE.md).

## Outputs

- `awm_bounds.csv`: access, group, success, cost, transition, and `Phi` bounds
  for every model/design pair;
- `awm_paired_gain_bounds.csv`: every available directed paired-gain
  certificate, including pair count, family size, per-look alpha, finite
  support, radius decomposition, clipping status, and transition-adjusted
  interval (empty for v1);
- `awm_paired_power_analysis.csv`: post-hoc plug-in sample-size planning for
  every available paired certificate, with an explicit non-guarantee label;
- `holdout_truth.json`: the empirical complete response vector hidden from
  model fitting;
- `holdout_truth_by_repetition.json`: the same hidden truth reported
  separately for every paired holdout repetition, so rank instability is not
  concealed by aggregation;
- `awm_evaluation.json`: joint response-vector containment, `Phi` containment,
  mean width, Commit decisions, and `false_safe_commit` counts; and
- `awm_manifest.json`: input hashes, split sizes, exclusions, and output paths.

The evaluator reports complete-vector containment, not a set of unrelated
marginal success claims. A pessimistic Commit is marked false-safe whenever its
held-out gain fails the same configured margin.

## Post-hoc AWM v2 Diagnostic on the Frozen Four-Design Run

The frozen v1 result must remain unchanged. The v2 configuration is therefore
named and labelled as a post-hoc method diagnostic:

```bash
PYTHONPATH=. python -m pathfinder evaluate-awm \
  --awm-config configs/multi_candidate_formal_v1_awm_v2_diagnostic.json \
  --oracle-config configs/multi_candidate_formal_v1_oracle.json \
  --oracle-output-dir "$PF_MULTI_ORACLE_OUT" \
  --output-dir "$PF_MULTI_AWM_V2_OUT"
```

The output can diagnose whether paired intervals are materially narrower and
whether their held-out gains are covered. It cannot retroactively turn the
frozen v1 experiment into confirmatory evidence. Any Commit rule selected
from this diagnostic must be frozen and rerun on independent objects or seeds.

After the v2alpha1 diagnostic, run v2alpha2 in two offline passes. The first
observes all designs only to produce decomposition and sample-size planning;
the second starts from the origin only and exercises the normal Reveal path:

```bash
PYTHONPATH=. python -m pathfinder evaluate-awm \
  --awm-config configs/multi_candidate_formal_v1_awm_v2alpha2_power_diagnostic.json \
  --oracle-config configs/multi_candidate_formal_v1_oracle.json \
  --oracle-output-dir "$PF_MULTI_ORACLE_OUT" \
  --output-dir "$PF_MULTI_AWM_V2_ALPHA2_POWER_OUT"

PYTHONPATH=. python -m pathfinder evaluate-awm \
  --awm-config configs/multi_candidate_formal_v1_awm_v2alpha2_oed_diagnostic.json \
  --oracle-config configs/multi_candidate_formal_v1_oracle.json \
  --oracle-output-dir "$PF_MULTI_ORACLE_OUT" \
  --output-dir "$PF_MULTI_AWM_V2_ALPHA2_OUT"
```

Both passes are post-hoc method development on an already inspected Oracle.
Only a newly frozen configuration run on independent objects or seeds can be
used as confirmatory evidence.

Run v3 in the same two isolated offline passes:

```bash
PYTHONPATH=. python -m pathfinder evaluate-awm \
  --awm-config configs/multi_candidate_formal_v1_awm_v3_power_diagnostic.json \
  --oracle-config configs/multi_candidate_formal_v1_oracle.json \
  --oracle-output-dir "$PF_MULTI_ORACLE_OUT" \
  --output-dir "$PF_MULTI_AWM_V3_POWER_OUT"

PYTHONPATH=. python -m pathfinder evaluate-awm \
  --awm-config configs/multi_candidate_formal_v1_awm_v3_oed_diagnostic.json \
  --oracle-config configs/multi_candidate_formal_v1_oracle.json \
  --oracle-output-dir "$PF_MULTI_ORACLE_OUT" \
  --output-dir "$PF_MULTI_AWM_V3_OUT"
```

For the frozen four-design run, 16 raw training pairs reduce to eight
independent workload units under this contract. A new output directory is
mandatory; do not point these commands at a v1 or v2 result directory.

Run v3alpha2 in two further isolated directories. These commands read the
same frozen Oracle and do not call FlowMesh:

```bash
PYTHONPATH=. python -m pathfinder evaluate-awm \
  --awm-config configs/multi_candidate_formal_v1_awm_v3alpha2_power_diagnostic.json \
  --oracle-config configs/multi_candidate_formal_v1_oracle.json \
  --oracle-output-dir "$PF_MULTI_ORACLE_OUT" \
  --output-dir "$PF_MULTI_AWM_V3_ALPHA2_POWER_OUT"

PYTHONPATH=. python -m pathfinder evaluate-awm \
  --awm-config configs/multi_candidate_formal_v1_awm_v3alpha2_oed_diagnostic.json \
  --oracle-config configs/multi_candidate_formal_v1_oracle.json \
  --oracle-output-dir "$PF_MULTI_ORACLE_OUT" \
  --output-dir "$PF_MULTI_AWM_V3_ALPHA2_OUT"
```

Do not reuse either v3alpha1 output directory. Compare the snapshot hash,
pooled point estimate, repetition sign-flip flag, component widths, and
cluster-only power projections before choosing a method for independent data.

Run v3alpha3 in fresh directories to compare only the certificate
construction against v3alpha2 on the identical frozen snapshot:

```bash
PYTHONPATH=. python -m pathfinder evaluate-awm \
  --awm-config configs/multi_candidate_formal_v1_awm_v3alpha3_power_diagnostic.json \
  --oracle-config configs/multi_candidate_formal_v1_oracle.json \
  --oracle-output-dir "$PF_MULTI_ORACLE_OUT" \
  --output-dir "$PF_MULTI_AWM_V3_ALPHA3_POWER_OUT"

PYTHONPATH=. python -m pathfinder evaluate-awm \
  --awm-config configs/multi_candidate_formal_v1_awm_v3alpha3_oed_diagnostic.json \
  --oracle-config configs/multi_candidate_formal_v1_oracle.json \
  --oracle-output-dir "$PF_MULTI_ORACLE_OUT" \
  --output-dir "$PF_MULTI_AWM_V3_ALPHA3_OUT"
```

Compare the normalized utility interval, mapped gain width, held-out
containment, and OED action with v3alpha2. Do not choose a confirmatory method
from held-out performance without a subsequent independent workload split.

## Current Boundary

The implementation is ready before the server experiment, and its synthetic
tests demonstrate interval tightening, held-out containment, and false-safe
detection under deliberate drift. It is not yet a Gate B3 result.

Before a paper claim, the real Oracle must be frozen and the following must be
pre-registered or calibrated:

- which Phase B assumptions are enabled;
- the joint confidence level and required coverage rate;
- unit-cost support and transition-cost confidence construction;
- the number of independent held-out response vectors or cross-fitting folds;
- the material width reduction relative to both baselines; and
- the decision-relevant false-safe threshold.

The current single paired holdout block is an engineering check. Reaching a
claimed frequentist coverage level requires repeated independent objects,
folds, or experiment replications rather than interpreting one Boolean
containment result as a calibrated coverage probability.
