# Pathfinder PPD Validation Protocol

## Purpose and Current Evidence

This protocol turns the engineering smoke tests into a staged validation of
Performative Physical Design (PPD). It does not treat a successful FlowMesh run
as evidence for the full research claim.

The completed Phase A engineering pilot established three useful facts:

- the FlowMesh worker, MCP Gateway, and Data Agent execute end to end;
- changing the digest quote changes the selected representation for some task
  strata while the task answer remains correct; and
- artifact bytes and transfer telemetry can be reconciled fail closed.

The result is directional evidence for Gate P1, not a complete PPD result. It
uses one synthetic object, the response is concentrated in one question type,
there is no matched physical intervention, and AWM/OED have not yet been
evaluated.

## What Counts as Complete PPD Validation

The validation is complete only when the following chain is supported on a
frozen multi-object corpus:

```text
physical design D
  -> offered quote and felt service path
  -> Agent representation access W(D)
  -> task success and realized resource cost
  -> session value Phi(D)
  -> AWM bounds
  -> OED Commit / Reveal / Hold decision
  -> safe deployment or restoration
```

The work is split into four evidence gates. A later gate must not be used to
paper over failure of an earlier one.

## Gate B1: Causal Access Response

### Interventions

Run a blocked factorial experiment over:

- physical design: remote digest versus node-local materialized digest;
- digest quote: low, high-but-affordable, and unaffordable;
- latency multiplier: `0.5`, `1.0`, and `2.0`; and
- workload/object/repetition blocks.

Every workload/repetition block contains every intervention cell. This permits
matched comparisons while spreading transient host load across conditions.

The repository includes an engineering fixture for this gate:

- `configs/phase_b_causal_gate_system.json`;
- `configs/phase_b_causal_gate_dry_run.json`;
- `configs/phase_b_causal_gate.json`; and
- `configs/phase_b_data_agent_manifest.json`.

The fixture validates the harness only. Its two design bindings use controlled
service paths over one small artifact, so it must not be reported as the real
physical-design result.

### Required tests

1. **P1 access elasticity:** report access probability by workload stratum,
   quote, and representation. The primary contrast and material-effect margin
   must be registered before the real run.
2. **P2 group-total monotonicity:** when every representation in a declared
   substitution group becomes no more expensive, total group access must not
   systematically decrease.
3. **P3 success monotonicity:** any claimed representation-quality ordering
   must agree with task success after stratifying by workload and controlling
   for latency.
4. **P4 quoted-price sufficiency:** at a fixed physical design and quote,
   latency interventions must remain inside pre-registered two-sided
   equivalence margins for access and success. Failure requires a latency-aware
   response model or an enforced latency reservation.
5. **Matched physical construct check:** compare remote and materialized designs
   at the same advertised quote. Record Data Agent service latency separately
   from client round-trip latency, and verify the expected location, bytes, and
   realized cost rather than inferring a design change from the quote.

### Required dataset

Replace the fixture workload list with a versioned video corpus containing
multiple independent objects in at least these pre-declared strata:

- temporal localization;
- object or attribute identification; and
- cross-video or structured-evidence synthesis.

Questions, acceptable answers, object IDs, representation hashes, and the
train/validation/test split must be frozen before the confirmatory run. Select
the number of objects and repetitions from a power analysis using Phase A as a
pilot; repetitions of one synthetic object are not independent samples.

## Gate B2: Reduced Oracle and Lock-In

Declare a finite governance-feasible design set small enough to deploy
exhaustively. At minimum it must include:

- a certified remote/cheap-path incumbent;
- a design with sampled frames or embeddings staged locally;
- a design with the structured digest materialized locally; and
- any placement/execution variant needed to distinguish `M`, `L`, and `E`.

For every design, measure the empirical response `W(D)`, session value
`Phi(D)`, forward transition cost, foreground loss, and restoration cost.

Implement and replay a naive read-react-materialize baseline. The lock-in gate
passes only if it remains at an inferior design because the valuable
representation is not observed, while exhaustive deployment shows that the
superior design still wins after complete transition accounting. A synthetic
counterexample alone is not sufficient.

The resulting exhaustive table is the frozen oracle dataset for AWM and OED.

## Gate B3: Adaptive Workload Model

Build AWM only after Gates B1 and B2 identify which assumptions survive. The
first implementation should be an exactly solvable coupled model over the
reduced design set and must include:

- affordability and per-class access caps;
- only the own-price, substitution-group, and success constraints that passed
  the causal gates;
- confidence sets for access, task success, realized service cost, transition
  cost, and probe-window loss; and
- explicit invalidation when an assumption or quoted-price-sufficiency gate
  fails.

Evaluate joint held-out coverage over complete candidate response vectors, not
independent marginal endpoints. Compare bound width and false-safe decisions
against an assumption-free box and an independent-box baseline.

Gate B3 passes only if coverage reaches its pre-registered level and the bounds
are materially tighter and decision-relevant.

## Gate B4: OED Closed Loop

Start every controller from the same `D_safe`, history, candidate set, and
exploration purse. Compare at least:

- naive read-react-materialize;
- passive AWM without probes;
- random feasible Reveal;
- an equal-budget black-box or bandit policy;
- full AWM + OED; and
- the exhaustive reduced oracle.

For every iteration, record candidate partition (`G_cert`, `G_probe`,
`G_other`), pessimistic Commit gain, optimistic Reveal gain, Reveal tier,
forward/foreground/restoration cost, remaining purse, and the final
Commit/Reveal/Hold reason.

A Reveal is a complete excursion: prepare, activate, observe, restore
`D_safe`, and retain the observation. Only a separately certified Commit may
change `D_safe`.

Gate B4 passes when OED reaches the oracle decision or a better safe design
with lower cumulative exploration and transition cost than equal-budget
baselines, without an unreported safe-sequence regression. Claims remain
candidate-relative whenever `G_other` is nonempty.

## Harness Outputs and Audit Contract

Each Phase B batch writes:

- `trial_plan.json`: immutable randomized plan;
- `runs.jsonl`: one durable record per planned trial;
- `summary.csv`: design/quote/latency cells;
- `summary_by_workload.csv`: the same metrics without hiding task
  heterogeneity;
- `paired_contrasts.csv`: matched one-factor physical, quote, and latency
  contrasts; and
- `manifest.json`: configuration hashes, versions, worker pin, timeout, and
  output paths.

`felt_latency_ms` is the client round trip. The new
`data_agent_service_latency_ms`, `data_agent_fetch_latency_ms`, and
`data_agent_controlled_delay_ms` fields describe the Data Agent path itself.
Do not use round-trip noise as a proxy for physical service latency.

Artifact telemetry waits up to the configured
`--telemetry-quiescence-timeout` (15 seconds by default for the CLI). A timeout
remains a telemetry failure; increasing the bounded wait does not permit stale
bytes or latency to be marked complete.

## Immediate Execution Order

1. Run the 54-session Phase B fixture dry run and inspect all five output
   artifacts.
2. Replace the single fixture object with a frozen multi-object corpus and run
   a small balanced feasibility batch.
3. Pre-register P1-P4 estimands, equivalence margins, exclusion rules, and
   power-based sample sizes.
4. Run the confirmatory causal gate once in a new output directory.
5. Build and exhaustively deploy the reduced design oracle.
6. Implement AWM against the frozen oracle, then OED and its baselines.

Do not launch the 540-session fixture plan as a substitute for Step 2. More
repetitions of the same synthetic object improve engineering repeatability but
do not create independent scientific evidence.
