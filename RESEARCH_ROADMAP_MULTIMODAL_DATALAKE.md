# Research Roadmap and Risks for Pathfinder

## Purpose

This roadmap turns
[AUTORESEARCH_MULTIMODAL_DATALAKE.md](AUTORESEARCH_MULTIMODAL_DATALAKE.md)
into staged research work. It follows the revised paper direction:
**performative physical design for agentic workloads**, implemented by
Pathfinder through an Adaptive Workload Model (AWM) and Optimistic Elastic
Design (OED).

The stages are evidence gates. A failed early gate should narrow or stop the
corresponding claim before a large system is built. Concrete implementation
tasks are tracked in
[DEVELOPMENT_CHECKLIST_MULTIMODAL_DATALAKE.md](DEVELOPMENT_CHECKLIST_MULTIMODAL_DATALAKE.md).

## Scope Decisions

The first paper uses:

- one full-corpus multimodal representation graph, initially video;
- queued agentic inference as the endogenous workload;
- recurring training and analytics as fixed cross-mode contributors;
- the design abstraction `D = (M, L, E)`;
- a class-specific, ex ante finite `P_qv` access-price universe;
- externally supplied governance constraints as `D_gov`; and
- FlowMesh as the execution substrate rather than the research contribution.

The first paper does not claim arbitrary distributed-system optimization,
online arrival elasticity, automatic legal interpretation, continuous price
menus, or a universal behavioral model for all agents. These can be future
extensions only after the central mechanism is established.

## Stage 0: Formal Model Audit

Resolve the paper skeleton's definitions before treating its theorem statements
as settled results.

Deliverables:

- separate definitions of performative optimum, performatively stable design,
  and candidate-relative OED certificate;
- an access-budget-consistent non-identifiability construction;
- verification of the corrected Reveal bound over ex ante `P_qv`, including
  the pair-canonical and simultaneously canonical specializations;
- a tightness statement relative to an explicit ambiguity set;
- a lower-bound convention with an initial design and transition unit;
- a complete task/success mapping for the hardness reduction; and
- a notation table shared by the paper and design documents.

Exit criterion: every theorem has a mechanically checkable list of assumptions
and a claim whose quantifiers match the implemented decision problem.

Fallback: proceed with an empirical control-policy paper, but remove or weaken
formal-guarantee language until the proof obligations are resolved.

## Stage 1: Causal Access-Response Pilot

Build the access-path resolver and a controlled harness before building the
full optimizer. Hold agent code, model, prompt, task mix, and random seeds fixed
while varying offered representation prices and real physical designs.

Deliverables:

- explicit task classes, access budgets, substitution groups, and terminal
  outcomes;
- a finite `P_qv` universe and class-specific quote matrices;
- quote intervention, matched physical intervention, and advertised-price ×
  felt-latency factorial experiments;
- estimates of `epsilon_qv`, cross-price response, and
  representation-dependent success;
- tests of group-total and success monotonicity; and
- a quoted-price-sufficiency equivalence test;
- a deliberate design-responsive re-arrival sweep measuring tolerance to
  violation of the scheduled-exogeneity assumption; and
- an audit of whether training and analytics remain sufficiently fixed for the
  initial scope.

Exit criterion: at least one important task class exhibits a material,
repeatable design-dependent access response, and the selected AWM assumptions
survive or can be narrowed to a defensible scope.

Go/no-go decisions:

- If access elasticity is negligible, stop using performativity as the
  headline and return to fixed-workload physical design.
- If monotonicity fails, refine task classes/substitution groups or weaken the
  ambiguity set before implementing OED.
- If quoted-price sufficiency fails, enforce latency reservations or expand the
  response state; do not retain the scalar-price Reveal-count theorem by
  assumption.
- If training or analytics are design-responsive, model them as endogenous or
  remove them from the first paper.

## Stage 2: Minimal Physical-Design Substrate

Implement the smallest trustworthy substrate required to deploy and observe
`D = (M, L, E)`.

Deliverables:

- stable logical identities and a versioned representation graph;
- materialization, placement, replication, staging, serving, and eviction;
- a catalog for task classes, substitution groups, design observations, and
  session artifacts;
- resolver-side affordability and governance enforcement;
- versioned `D_gov` supplied by an external authority;
- auditable plan transitions and failure-safe restoration; and
- a small set of static plans exposing cheap and expensive access paths.

Exit criterion: the system can deploy, distinguish, and safely restore several
physical designs while recording exactly which choices were offered to each
task class.

## Stage 3: Reduced Oracle and Lock-In Demonstration

Create a small design instance on which every feasible design can be deployed.
Use it to debug the science before scaling candidate generation.

Deliverables:

- exhaustive enumeration of the reduced governance-feasible design space;
- the true empirical response surface `D -> W(D) -> Phi(D)`;
- a reproducible read-react-materialize lock-in instance;
- transition and restoration cost measurements;
- a candidate-pool accounting for `G_cert`, `G_probe`, and `G_other`; and
- an oracle trace for later controller comparisons.

Exit criterion: naive read-react-materialize remains at an inferior design for
the predicted censoring reason, and the superior design still wins after
transition costs.

Fallback: if lock-in occurs only in a contrived simulation, narrow the claim to
design-dependent workload estimation rather than a general physical-design
failure mode.

## Stage 4: Adaptive Workload Model

Implement AWM over the history-indexed feasible response set. Begin with a
small linear or otherwise exactly solvable formulation.

Deliverables:

- observation ingestion and assumption-version tracking;
- coupled affordability, substitution, monotonicity, and success constraints;
- lower and upper session-value/gain bounds computed over the full feasible
  set;
- class-by-representation realized-cost boxes and time-uniform confidence
  events covering demand, cost, transitions, and probe-window loss;
- held-out and full-oracle coverage tests;
- assumption ablations and violation diagnostics; and
- comparison with trivial independent uncertainty boxes.

Exit criterion: AWM attains its declared coverage on the reduced oracle and
produces meaningfully tighter decision bounds than assumption-free or
independent-box baselines.

Fallback: if the bounds remain vacuous, reduce task and representation scope,
add targeted measurements, or present workload identification as the research
problem rather than claiming a strong controller.

## Stage 5: OED and Escalation

Implement Commit, Reveal, and Hold with `D_safe` as the certified incumbent.
Reveal may deploy a probe, but must restore `D_safe` afterward and retain the
observation unless a separate Commit certificate is established.

Deliverables:

- candidate classification into `G_cert`, `G_probe`, and `G_other`;
- simultaneously canonical, pair-canonical, and fallback Reveal tiers;
- resolution levels over the ex ante `P_qv` universes;
- Commit and Reveal bounds with complete cost accounting;
- `delta_t` decomposed into candidate, incumbent, and transition widths;
- partial-materialization probes;
- restoration and reusable-probe-artifact handling;
- explicit Hold and refusal reasons;
- an escalation ladder for structural ambiguity; and
- comparisons with passive, random, bandit, Bayesian, and black-box policies.

Exit criterion: OED finds a better safe design or reaches the oracle decision
with lower cumulative exploration and transition cost than equal-budget
baselines, without unreported regressions in the safe sequence.

Claims must remain candidate-relative whenever `G_other` is nonempty.

## Stage 6: End-to-End Cross-Mode Evaluation

Scale the candidate space and add recurring training and analytics consumers
that reuse or contend for the same representations.

Deliverables:

- joint versus independent/sequential `M/L/E` comparisons;
- agent-only versus cross-mode planning;
- heterogeneous-node and constrained-capacity experiments;
- overhead and candidate-generation studies;
- held-out task and environment results; and
- an explicit accounting of opportunity excluded by candidate generation.

Exit criterion: joint physical design provides a repeatable advantage that
cannot be explained by one conventional cache decision alone.

## Stage 7: Adaptation and Governance Validation

Only after the core result, test controlled drift and policy changes.

Deliverables:

- recovery after a task-mix, bandwidth, capacity, or service-rate change;
- separate revocation and erasure correctness runs;
- performance comparison between planning within `D_gov` and post-hoc repair;
- audit completeness and enforcement overhead; and
- a final decision on whether governance is a supported constraint or a
  distinct contribution.

Governance remains a constraint feature unless it creates and empirically
supports a separate research insight.

## Major Research Risks

### Risk 1: The Agent Does Not Respond to Physical Access

If class-specific representation prices do not change behavior, then `W(D)` is
effectively fixed. Test this first with causal interventions rather than
inferring elasticity from ordinary traces.

### Risk 2: Behavioral Assumptions Are False

Group monotonicity, affordability gates, and representation-success ordering
may fail because of task heterogeneity or agent policy. Version assumptions,
test them explicitly, and narrow task classes rather than averaging away
violations.

### Risk 3: OED Certificates Are Mistaken for Global Optimality

OED reasons only over modeled behavior and generated candidates. Always expose
the ambiguity set and `G_other`; use a reduced exhaustive oracle to quantify
the missing opportunity.

### Risk 4: The Formal Results Outrun the Model

Non-identifiability, tightness, lower-bound, and hardness arguments are
plausible but currently require careful repair. Keep theorem status separate
from implementation status and empirical findings.

### Risk 5: The Price Abstraction Lacks Construct Validity

An injected quote may not represent felt latency, quality, freshness, or
contention. Pair quote interventions with matched physical deployments, run
the quoted-price-sufficiency factorial, and log `p_qv`, felt latency, and
realized cost separately.

### Risk 6: Exploration Is More Expensive Than Its Benefit

Large representations may be costly to create and restore. Use partial
materialization, count all forward/restore/foreground costs, and let OED Hold
when no probe has positive robust value.

### Risk 7: The Contribution Collapses into Caching

Demonstrate an interaction among representation materialization, distributed
layout, execution placement, and agent access. If the gain comes from a single
cache choice, revise the novelty claim.

### Risk 8: The Search Space Becomes Generic Tuning

Keep a semantic representation graph, explicit task access, structured design
transformations, and candidate-relative certificates. Compare against a generic
tuner over the same encoded actions.

### Risk 9: Cross-Mode Scope Obscures the Core Result

Agentic access is the initial endogenous mechanism. Treat training and
analytics as fixed contributors unless evidence forces a model expansion.

### Risk 10: Infrastructure Consumes the Schedule

FlowMesh integration, federated identity, and production operations can become
projects of their own. Build only the interfaces required to test the
performative claim and mock noncritical operational features.

### Risk 11: Governance Becomes a Compliance Project

Pathfinder consumes versioned policies and attestations from external
authorities. It does not interpret law or certify data semantics.

### Risk 12: One Workload Cannot Support Broad Claims

Establish the causal mechanism deeply on one corpus, then validate it on one
different representation graph. Generality comes from mechanism replication,
not a long list of shallow demos.

## Decision Log

Maintain a versioned record of:

- the current theorem status and unresolved proof obligations;
- endogenous and fixed workload components;
- task classes, substitution groups, and ex ante `P_qv` universes;
- supported and rejected AWM assumptions;
- the exact design and governance-feasible domains;
- the sizes of `G_cert`, `G_probe`, and `G_other`;
- all go/no-go decisions and negative results;
- which FlowMesh features are reused or newly implemented; and
- every change to the paper's claims caused by evidence.

This prevents a modeling convenience, synthetic example, or provisional
implementation choice from silently becoming a research result.
