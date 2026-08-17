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
