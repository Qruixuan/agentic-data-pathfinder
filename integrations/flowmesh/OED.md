# Offline OED Closed-Loop Replay

## Purpose

The OED v1alpha1 implementation turns the finite-design AWM interface into an
auditable `Commit`, `Reveal`, `Hold`, or `Stop` controller. It operates against
a completed, frozen Reduced Oracle directory and does not start workers,
services, or physical transitions.

The replay is intentionally split into two data roles:

- only the Oracle training repetitions are made visible to a policy; and
- paired holdout repetitions are excluded from every policy fit and decision,
  and are used only to evaluate final value, regret, and safe-sequence
  regressions.

An offline `Reveal` exposes the selected design's training records, charges a
complete excursion, restores the incumbent safe design, and retains the new
observation. A later iteration must issue a separate certified `Commit` before
the safe design can change.

## Candidate and Cost Contract

Each iteration partitions every non-incumbent design into:

1. `G_cert`: directly observed designs or designs carrying an enabled
   structural AWM certificate;
2. `G_probe`: declared Reveal candidates that expose a previously unresolved
   lower-price or newly affordable class-representation-price state; and
3. `G_other`: everything outside the current certificate and Reveal
   mechanisms.

Commit uses the AWM pessimistic gain, including the upper forward-transition
cost. Reveal uses optimistic value after the lower complete excursion cost:

```text
forward transition + probe-window loss + restoration transition
```

Authorization separately checks the upper complete excursion against both
the per-excursion cap and the remaining exploration purse. A positive but
unaffordable probe produces `budget_limited_stop`; unreachable `G_other`
produces `structural_hold`. These outcomes do not certify the unexplored
frontier.

With an AWM v2 config, the trace additionally reports
`gain_interval_source`, `commit_gain_width`, the paired sample count, and the
per-pair/per-look alpha. Before a candidate is observed, the source is
`marginal-phi-difference`. After a valid paired Reveal, Commit and optimistic
value use `paired-fixed-looks-empirical-bernstein`. The OED stability radius is
the width of the actual Commit-gain interval, rather than reconstructing it
from separate marginal widths. The configured OED iteration limit may not
exceed AWM v2's preregistered maximum number of looks.

For `pathfinder.awm/v2alpha2`, the look is instead the immutable training
snapshot for one explicitly preregistered directed comparison. Re-reading the
same snapshot in later controller iterations is not a new look, so the OED
iteration count is not compared with `maximum_looks=1`. Any change to the
underlying paired observations invalidates that reuse and requires a new
analysis. The trace records the paired point estimate, variance-radius term,
range-radius term, and support-clipping flag so a
`certificate_limited_stop` can be attributed to its uncertainty source.

With `pathfinder.awm/v3alpha1`, OED uses the fixed-snapshot rule but changes
the evidence unit and certificate. One deterministic paired observation per
workload cluster enters the decision; repeated runs of the same workload do
not increase the primary sample count. The trace reports raw pair count,
independent workload count, sampling rule, success-difference interval, and
cost-difference interval. Its `gain_interval_source` is
`paired-cluster-decomposed-kl-empirical-bernstein`. OED never substitutes the
more aggressive direct-utility diagnostic radius for this primary interval.

The committed
[`oed_reduced_mvp.json`](../../configs/oed_reduced_mvp.json) declares the
finite Reveal set, tier, budget, cap, and probe-window loss. Its cost status is
an engineering placeholder. Replace it with a new preregistered config after
the physical transition and foreground-loss measurements are frozen.

## Policies Compared

Every active policy starts from the same safe design, observation history,
candidate domain, and purse:

- `full_oed`: coupled AWM and tiered value-per-cost Reveal selection;
- `passive_awm`: the same AWM but no Reveal authorization;
- `random_reveal`: random budget-feasible Reveal with a fixed seed;
- `black_box_reveal`: an independent-box model with the same budget;
- `naive_read_react_materialize`: incumbent-demand-only decision; and
- `exhaustive_oracle`: omniscient reduced-domain reference, used only for
  evaluation.

The final two policies do not consume hidden data inside the OED controller.
They are evaluation references constructed after the active-policy replays.

## Run

No FlowMesh credential or LLM key is needed. The Reduced Oracle directory must
already contain per-design `runs.jsonl` files and `oracle_table.csv`.

```bash
cd "$HOME/agentic-data-pathfinder"
source "$HOME/.venvs/pf312/bin/activate"

export PF_ORACLE_OUT="$HOME/pathfinder-reduced-oracle-mvp/run-v1"
export PF_OED_OUT="$HOME/pathfinder-reduced-oracle-mvp/oed-v1"

PYTHONPATH=. python -m pathfinder run-oed-replay \
  --oed-config configs/oed_reduced_mvp.json \
  --awm-config configs/awm_reduced_mvp.json \
  --oracle-config configs/reduced_oracle_mvp.json \
  --oracle-output-dir "$PF_ORACLE_OUT" \
  --output-dir "$PF_OED_OUT"
```

## Run Against the Synthetic Multi-Candidate Fixture

The committed two-design MVP has exactly one Reveal candidate, so random and
value-directed Reveal cannot differ. The synthetic fixture exists to remove
that degeneracy offline:

```bash
PYTHONPATH=. python -m pathfinder generate-synthetic-oracle \
  --fixture-config configs/synthetic_oracle_fixture.json \
  --output-dir "$PF_SYNTHETIC_OUT/oracle"

PYTHONPATH=. python -m pathfinder evaluate-awm \
  --awm-config configs/synthetic_multi_candidate_awm.json \
  --oracle-config configs/synthetic_multi_candidate_oracle.json \
  --oracle-output-dir "$PF_SYNTHETIC_OUT/oracle" \
  --output-dir "$PF_SYNTHETIC_OUT/awm"

PYTHONPATH=. python -m pathfinder run-oed-replay \
  --oed-config configs/synthetic_multi_candidate_oed.json \
  --awm-config configs/synthetic_multi_candidate_awm.json \
  --oracle-config configs/synthetic_multi_candidate_oracle.json \
  --oracle-output-dir "$PF_SYNTHETIC_OUT/oracle" \
  --output-dir "$PF_SYNTHETIC_OUT/oed"
```

`configs/synthetic_multi_candidate_oed.json` declares four Reveal candidates
across three tiers over a five-design domain. With `per_excursion_cap` 3.0 and
an `exploration_budget` of 8.0, three candidates are excursion-feasible at the
first iteration and one is blocked by the cap, so tier ordering, the
value-per-cost rule, the cap, and the purse are all exercised rather than
short-circuited by a single admissible option.

What the fixture demonstrates is *mechanism*, not merit. On the committed
scenario the policies genuinely diverge — `passive_awm` stops at the
incumbent, `full_oed` reveals the best tier-0 value-per-cost candidate and
commits as soon as its pessimistic gain clears the margin, and `random_reveal`
takes a different, more expensive path — but `full_oed` does **not** reach the
exhaustive-oracle design, and the pre-server gate checks for reaching the
oracle and for beating the equal-budget baselines both report false. That is
the honest result of the declared scenario and is reported as such. Because
every empirical AWM assumption is disabled, `coupled_awm` equals
`independent_box`, so `full_oed` and `black_box_reveal` are expected to
coincide over this fixture.

No policy comparison computed from the fixture is evidence of anything about
a real system. See the scientific boundary in
[`REDUCED_ORACLE.md`](REDUCED_ORACLE.md).

## Outputs

- `oed_trace.jsonl`: complete per-iteration partitions, bounds, gains, widths,
  action reason, Reveal tier, three-part cost, safe design before/after, and
  remaining purse;
- `oed_policy_summary.csv`: final design, control cost, held-out value, regret,
  Reveal/Commit counts, and safe-sequence regression count for all policies;
- `oed_evaluation.json`: policy comparison and explicit pre-server Gate B4
  checks; and
- `oed_manifest.json`: input hashes, output paths, data-role declaration, and
  confirmation that no deployment mutation was performed.

## Post-hoc OED v2 Diagnostic

After producing the AWM v2 diagnostic above, replay the same frozen Oracle
with the separately labelled controller config:

```bash
PYTHONPATH=. python -m pathfinder run-oed-replay \
  --oed-config configs/multi_candidate_formal_v1_oed_v2_diagnostic.json \
  --awm-config configs/multi_candidate_formal_v1_awm_v2_diagnostic.json \
  --oracle-config configs/multi_candidate_formal_v1_oracle.json \
  --oracle-output-dir "$PF_MULTI_ORACLE_OUT" \
  --output-dir "$PF_MULTI_OED_V2_OUT"
```

This run is offline and performs no worker, Data Agent, FlowMesh, or physical
storage mutation. Its purpose is to determine whether the revised certificate
changes the width or terminal action on already-frozen data. It remains
post-hoc method development, not a new confirmatory OED result.

The follow-up fixed-snapshot replay is also separate and post-hoc:

```bash
PYTHONPATH=. python -m pathfinder run-oed-replay \
  --oed-config configs/multi_candidate_formal_v1_oed_v2alpha2_diagnostic.json \
  --awm-config configs/multi_candidate_formal_v1_awm_v2alpha2_oed_diagnostic.json \
  --oracle-config configs/multi_candidate_formal_v1_oracle.json \
  --oracle-output-dir "$PF_MULTI_ORACLE_OUT" \
  --output-dir "$PF_MULTI_OED_V2_ALPHA2_OUT"
```

The v3 replay is another isolated post-hoc analysis of the same frozen Oracle:

```bash
PYTHONPATH=. python -m pathfinder run-oed-replay \
  --oed-config configs/multi_candidate_formal_v1_oed_v3_diagnostic.json \
  --awm-config configs/multi_candidate_formal_v1_awm_v3_oed_diagnostic.json \
  --oracle-config configs/multi_candidate_formal_v1_oracle.json \
  --oracle-output-dir "$PF_MULTI_ORACLE_OUT" \
  --output-dir "$PF_MULTI_OED_V3_OUT"
```

This command is read-only with respect to FlowMesh and physical data. Its
actions are controller replays, not live transitions, and its output remains
post-hoc until the v3 contract is frozen on independent workload objects.

## Current Boundary

Synthetic tests cover candidate partitioning, a Reveal-restore-observe-Commit
sequence, structural Hold, budget-limited Stop, deterministic baselines, and
holdout isolation. This validates the controller implementation, not the
scientific OED claim.

Gate B4 still requires a frozen physical Reduced Oracle, calibrated forward,
foreground, and restoration costs, reviewed AWM assumption gates, live Reveal
execution with restoration evidence, repeated held-out workloads, and a
cost/regret comparison showing full OED improves on equal-budget baselines.
The current two-design domain has only one probe candidate, so random and
value-directed Reveal can take the same action at the same cost. A scientific
policy-efficiency comparison therefore needs a preregistered domain with at
least several feasible candidates, not just more repetitions of this MVP.
