# Evaluation Plan: Pathfinder Performative Physical Design

## Purpose

This document defines how to evaluate the research direction in
[AUTORESEARCH_MULTIMODAL_DATALAKE.md](AUTORESEARCH_MULTIMODAL_DATALAKE.md)
and the system in
[SYSTEM_DESIGN_MULTIMODAL_DATALAKE.md](SYSTEM_DESIGN_MULTIMODAL_DATALAKE.md).
It is an experiment plan, not a record of completed results.

The central obligation is no longer only to show that a physical design
improves a fixed workload. It is to establish that:

1. a physical design changes which representations agents can afford to use;
2. this access change measurably changes the realized workload and session
   value;
3. conventional read-react-materialize control can become locked into an
   inferior design; and
4. Pathfinder can make useful decisions under partial identification without
   silently treating assumptions as observations.

Any numerical example inherited from the paper skeleton is a motivating
example until it is reproduced by the implementation.

## Evaluation Principles

1. Test the performative premise before optimizing it.
2. Audit theorem definitions and assumptions before presenting empirical
   results as validation of formal guarantees.
3. Keep quoted access prices distinct from realized execution cost.
4. Treat quoted price and realized/felt latency as separate causal variables;
   holding the agent binary and prompt fixed does not eliminate the latter.
5. Count materialization, migration, probe execution, foreground disruption,
   and restoration in the total cost.
6. Compare methods over the same governance-feasible design space and
   experiment budget.
7. Report candidate-pool limitations: an OED certificate over
   `G_cert ∪ G_probe` is not a global certificate if `G_other` is nonempty.
8. Treat governance as an externally supplied feasibility domain, not as
   policy interpretation performed by Pathfinder.

## Theory-Readiness Gate

Before experiments are used to support the paper's theorems, the formal model
must pass a written audit covering:

- a precise distinction among a performative optimum, a performatively stable
  point, and OED's candidate-relative certificate;
- an access-budget-consistent non-identifiability construction over the full
  declared price domain;
- an ex ante finite `P_qv` price universe over the declared design domain,
  with pair-canonical and simultaneously canonical Reveal conditions checked
  against that universe;
- a tightness statement relative to the declared ambiguity set, with safe
  deployment loss separated from exploration loss;
- explicit initial-design and transition-counting conventions in lower bounds;
  and
- a complete task, access, and success mapping in any NP-hardness reduction.

Failure of this gate does not invalidate the systems experiment. It changes the
claim from a proved guarantee to an empirically evaluated control policy.

## Research Questions

### RQ0: Does the Performative Premise Hold?

Does changing the physical design, while holding the agent and offered tasks
fixed, change representation access, task completion, session utility, and
subsequent demand? Are these effects large and repeatable enough to matter?

### RQ1: Which Workload Components Are Endogenous and How Robust Is Exogeneity?

Is design dependence concentrated in queued agentic inference, as currently
assumed? Can recurring training and analytics be treated as fixed contributors,
or do they also change behavior in ways the model must represent? Because the
first experiment schedule fixes arrival counts, deliberately allow a fraction
of sessions to re-arrive under cheaper access and measure how quickly the
finite per-class cap and controller decisions degrade.

### RQ2: Does Read-React-Materialize Lock In?

Can a controller that materializes only from observed reads remain at a cheap
but inferior design because expensive representations are never observed?
Which combinations of access budget, transition cost, and latent task value
create this failure?

### RQ3: Is the Adaptive Workload Model Sound and Useful?

Do AWM's coupled monotonicity, affordability, and task-success constraints
contain held-out behavior at the claimed confidence level? Are the resulting
bounds tight enough to enable Commit or Reveal decisions?

### RQ4: Does OED Improve Safe Decision Making?

Compared with passive, random, and generic exploration, does OED find a better
session-level physical design with lower total deployment and exploration cost?
How often does it Commit, Reveal, Hold, or escalate?

### RQ5: What Is the Value of Joint Physical Design?

How much is lost when representation materialization `M`, distributed layout
`L`, and execution/delivery placement `E` are optimized independently? Do
agentic inference, training, and analytics create meaningful cross-mode reuse
or contention?

### RQ6: What Limits the Certificate?

How much opportunity lies outside `G_cert ∪ G_probe`? How sensitive are results
to the finite price alphabet, candidate generator, ambiguity set, and allowed
probe mechanisms?

### RQ7: Are Correctness and Overhead Acceptable?

Does the resolver enforce access and governance constraints at every serve and
materialization boundary? What are the costs of catalog operations, modeling,
probes, restoration, and telemetry?

## Causal Access-Response Harness

The first implementation milestone is a causal harness around the access-path
resolver, not the full optimizer.

For each task class, the harness must record:

- offered task and session identifiers;
- design, task-class, and price-universe versions;
- every available representation and its class-specific quote `p_qv(D)`;
- the agent's declared access budget;
- the selected or rejected representation;
- realized bytes, compute, felt latency, and monetary/normalized
  `realized_cost_qv(D)`; and
- terminal success, quality, latency, and session value.

Two complementary interventions are required:

1. **Quote intervention:** randomize `p_qv` within the ex ante finite `P_qv`
   universe while keeping the underlying artifact and serving path fixed. This
   estimates class-specific access elasticity without rebuilding every design.
2. **Physical intervention:** deploy a real design change and verify that its
   measured response is consistent with the response predicted by the
   corresponding quoted price.
3. **Quoted-price-sufficiency intervention:** factorially vary advertised
   `p_qv` and felt latency over the deployed operating range. Conditional on
   `p_qv`, latency must not materially change access or success under a
   pre-registered equivalence margin.

The quote intervention alone is insufficient: an injected price may fail to
capture latency, freshness, quality, or contention effects of the real design.
The physical intervention is therefore the construct-validity check. A real
design and injected intervention must be matched on quoted `p_qv`, not merely
on realized cost, while felt latency is separately held or measured.

## Initial Workloads

### Primary Workload: Full-Corpus Video Agent Sessions

Use one video corpus with a representation graph such as:

`compressed video -> sampled frames -> embeddings -> structured multimodal digest`

The cheap path should expose embeddings or thumbnails. The expensive path
should create a reusable structured digest that can improve the success or
cost of multiple task classes but is initially unaffordable or absent.

The first task classes should be small and explicit, for example:

- semantic retrieval;
- temporal event localization; and
- cross-video synthesis requiring structured evidence.

Queued agentic sessions are the primary endogenous workload. Recurring
training and batch analytics consume the same artifacts as fixed cross-mode
contributors in the first paper.

### Secondary Workload

Add one image-text or audio-text graph only after the primary workload passes
the elasticity and lock-in gates. Its purpose is to test whether the mechanism
generalizes to a different size-versus-compute trade-off, not to maximize the
number of modalities.

## Testbed and Design Space

The initial testbed should include:

- S3-compatible remote object storage;
- multiple FlowMesh compute workers;
- RAM and node-local NVMe tiers;
- controllable or emulated network bandwidth and latency;
- at least one accelerator-backed consumer;
- versioned zone, administrative-domain, and trust-domain labels; and
- a dataset larger than aggregate fast-tier capacity.

The executable design space must be finite and reproducible. Declare:

- allowable representation nodes and substitution groups;
- placement and replication choices;
- execution/delivery locations;
- the ex ante finite `P_qv` universe for every task-class/representation pair;
- candidate-generation rules;
- transition and restoration actions; and
- the externally supplied governance-feasible domain `D_gov`.

Use reduced instances on which every feasible design can be deployed as an
oracle. Larger experiments may use generated candidate pools, but must report
which legal designs were not searched.

## Baselines

### Physical-Design Baselines

- remote streaming with no application-managed materialization;
- node-local LRU/LFU caching;
- static full-dataset staging when capacity permits;
- locality-aware task placement;
- a strong intermediate-representation caching policy;
- independent and sequential optimization of `M`, `L`, and `E`;
- a single-workload-mode optimizer;
- a hand-tuned plan; and
- exhaustive deployment on reduced instances.

For governance experiments, additionally compare an unconstrained infeasible
lower bound, performance-first planning followed by post-hoc repair, and
planning directly over `D_gov`.

### Performative-Controller Baselines

- naive read-react-materialize;
- read-react-materialize with perfect conventional cost estimation;
- passive AWM without active probing;
- analytical prediction without empirical correction;
- random feasible probes;
- a contextual or combinatorial bandit over the same actions;
- a generic Bayesian/black-box tuner;
- a trivial independent-box uncertainty model;
- AWM with only one structural assumption family at a time;
- OED without partial materialization;
- full AWM + OED + escalation; and
- an omniscient or exhaustive oracle on reduced instances.

All controllers begin from the same certified design, catalog state,
observation history, candidate space, and total cost budget.

## Metrics

### End-to-End Metrics

- session value `Phi(D)` and its task-class decomposition;
- task success, answer quality, and terminal latency;
- amortized workflow makespan and valid-sample goodput;
- accelerator starvation and utilization;
- bytes transferred and storage footprint; and
- normalized or monetary resource cost.

### Performative-Response Metrics

- access probability by task class, representation, and quote;
- `epsilon_qv` own-price elasticity and the within-class cross-price matrix;
- group-total access under price decreases;
- success probability conditional on the resolved representation;
- workload distance between `W(D)` under physical interventions; and
- agreement between quote interventions and matched physical designs; and
- conditional effect of felt latency given `p_qv`, with the Gate P4
  equivalence interval.

### AWM and Certificate Metrics

- full-feasible-set and held-out coverage of response bounds;
- bound width by task class and candidate;
- upper and lower gain bounds;
- `delta_t` and its candidate-value, incumbent-value, and transition-cost
  width components;
- Commit/Reveal/Hold frequency and escalation reason;
- simultaneously canonical, pair-canonical, and fallback Reveal counts;
- observed resolution levels relative to `P_qv`;
- false-safe and false-hopeless decisions;
- fraction of candidates in `G_cert`, `G_probe`, and `G_other`; and
- oracle gap attributable to model uncertainty versus candidate omission.

Coverage must be evaluated jointly over the coupled feasible set. Substituting
independent interval endpoints is not a valid test of the AWM.

### Exploration and Transition Metrics

- forward materialization/migration cost;
- probe foreground-performance loss;
- restoration cost;
- reusable versus discarded probe artifacts;
- observations retained after restoration;
- cumulative exploration loss;
- number of safe-sequence regressions; and
- time and cost to recover after a change.

### Governance and Correctness Metrics

- unauthorized materializations, transfers, replicas, or serves, which must be
  zero at enforcement boundaries;
- illegal-plan rejection over constructed cases;
- audit-event completeness;
- revocation and erasure propagation time; and
- constraint-check overhead and performance opportunity cost.

## Required Falsification Gates

### Gate P1: Access Elasticity

At least one important task class must exhibit a repeatable change in access or
completion when its relevant representation price changes. Otherwise the
performative premise is too weak for the headline.

### Gate P2: Group-Total Monotonicity

When all relevant representations in a declared substitution group become no
more expensive, total group access must not systematically decrease. If it
does, either narrow the group/model scope or remove this AWM assumption.

### Gate P3: Success Monotonicity

Any declared ordering of representation informativeness must match measured
task success after controlling for latency and agent policy. Violations require
task-class refinement or a weaker structural assumption.

### Gate P4: Quoted-Price Sufficiency

Conditional on the quoted-price matrix `p(D)`, changing felt latency across the
deployed operating range must not materially change `n_qv` or `r_q` under a
pre-registered two-sided equivalence margin. Identical agent code, model, and
prompt do not establish this because later accesses in a session may respond
to latency already experienced.

If this gate fails, either enforce a tight latency reservation per quote or
expand the response model to `eta(p, latency)` and `rho(p, latency)`. The latter
invalidates the scalar-price Reveal-count theorem until a new frontier-based
resolution argument is supplied.

## Required Lock-In Evidence

Construct at least one real, reproducible instance in which naive
read-react-materialize fails to reach a superior feasible design because its
access path is censored. A purely synthetic counterexample is not enough for
the systems claim.

Exogenous arrival counts are imposed by the queued schedule rather than treated
as a fifth falsification gate. Report a separate robustness curve after
deliberately allowing design-responsive re-arrivals. Training and analytics
that respond materially despite their fixed schedule should be moved into the
endogenous model or removed from the first-paper scope.

## Experimental Phases

### Phase T0: Formal and Static Audit

Resolve the theory-readiness items, enumerate the reduced design space, test
resolver invariants, and validate that every logged observation is associated
with an actually offered access choice.

### Phase A: Premise and Assumption Pilot

Run the causal harness and evaluate Gates P1–P4. Pre-register task classes,
`P_qv`, latency interventions, equivalence margins, success metrics, and
exclusion rules before examining the full result. Run the exogeneity-violation
tolerance sweep separately.

The first executable slice is the real-FlowMesh quote-response pilot described
in `integrations/flowmesh/PILOT.md`. It fixes `D_structured_digest` and latency,
block-randomizes digest quotes 2 versus 6, and records completed behavior,
telemetry failure, and infrastructure failure as distinct outcomes. The
included repeated fixture is an engineering validation only; the registered
Phase A analysis must replace it with a frozen multi-object workload and must
not infer a PPD effect from the smoke or fixture runs alone.

### Phase B: Reduced Oracle and Lock-In Study

Deploy every feasible reduced-instance design. Measure the true response
surface, construct the lock-in case, test AWM coverage, and compare certificates
with the oracle. This is the main scientific debugging environment.

### Phase C: End-to-End Pathfinder

Compare full Pathfinder with physical-design and performative-controller
baselines. Report performance against cumulative transition plus exploration
cost, not iteration count alone.

### Phase D: Cross-Mode and Scale Study

Add fixed training and analytics consumers, heterogeneous nodes, and a larger
candidate space. Evaluate joint `M/L/E` choices and expose candidate-generation
limits.

### Phase E: Adaptation and Governance

Change one workload or resource factor at a time and measure recovery. In
separate correctness runs, revoke a permission or advance an erasure epoch and
verify enforcement. Do not mix policy-revocation correctness with ordinary
performance adaptation.

## Required Ablations

At minimum, remove or replace:

- representation choice, layout choice, and execution choice separately;
- the access-path resolver;
- class-specific quotes, replacing them with one quote per representation;
- the quoted-price-sufficiency assumption;
- each AWM assumption family;
- coupled feasible-set reasoning, using independent boxes instead;
- active Reveal;
- canonical-tier Reveal selection;
- partial materialization;
- restoration and observation retention;
- transition-cost accounting;
- the explicit Hold/refusal rule;
- escalation to broader measurements;
- cross-mode reuse; and
- governance-aware candidate generation, using post-hoc filtering instead.

## Statistical and Reproducibility Requirements

- Repeat randomized trials and report distributions or confidence intervals.
- Use held-out sessions and at least one held-out environment configuration.
- Randomize or counterbalance design order to limit warm-cache and time bias.
- Record code, data, model, prompt, seed, design, `P_qv` version, complete
  quote matrix, felt latency, cache state, catalog version, task mix, and
  background load.
- Verify selected designs with longer runs not used to fit the AWM.
- Report negative assumption tests and refused decisions, not only successful
  commits.
- Publish the reduced design enumerator and oracle traces.

Parallel probes should be enabled only after isolated repeats show stable
ranking or interference is explicitly modeled.

## Success and Stop Criteria

The direction has a strong core result if:

1. physical design causally changes agent access and session value;
2. naive read-react-materialize reproducibly locks into an inferior design;
3. all four falsification gates hold in the declared operating range;
4. AWM bounds achieve declared coverage and are tighter than trivial bounds;
5. OED reaches a better safe design at lower total cost than equal-budget
   passive, random, bandit, and black-box baselines; and
6. gains remain after probe, restoration, transition, and telemetry costs.

Narrow or stop the direction if access elasticity is negligible, the lock-in
case exists only under unrealistic interventions, structural assumptions fail
without a defensible narrower scope, or the ambiguity set remains too wide to
make decisions. Remove formal-guarantee language if the theory-readiness gate
cannot be completed. Keep governance as a supported constraint unless its
interaction with performative physical design produces a distinct, evaluated
research contribution.
