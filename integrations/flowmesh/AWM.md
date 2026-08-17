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

## Outputs

- `awm_bounds.csv`: access, group, success, cost, transition, and `Phi` bounds
  for every model/design pair;
- `holdout_truth.json`: the empirical complete response vector hidden from
  model fitting;
- `awm_evaluation.json`: joint response-vector containment, `Phi` containment,
  mean width, Commit decisions, and `false_safe_commit` counts; and
- `awm_manifest.json`: input hashes, split sizes, exclusions, and output paths.

The evaluator reports complete-vector containment, not a set of unrelated
marginal success claims. A pessimistic Commit is marked false-safe whenever its
held-out gain fails the same configured margin.

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
