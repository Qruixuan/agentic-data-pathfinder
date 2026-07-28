# Performative Physical Design for Agentic Multimodal Workloads

## Purpose of This Document

This file defines the research direction only. Detailed mechanisms,
implementation choices, experiments, and development order belong in:

- [System Design](SYSTEM_DESIGN_MULTIMODAL_DATALAKE.md);
- [Evaluation Plan](EVALUATION_PLAN_MULTIMODAL_DATALAKE.md);
- [Research Roadmap](RESEARCH_ROADMAP_MULTIMODAL_DATALAKE.md);
- [Development Checklist](DEVELOPMENT_CHECKLIST_MULTIMODAL_DATALAKE.md); and
- [Background Research](BACKGROUND_RESEARCH/00_SUBDIRECTION_MAP.md).

The working system name is **Pathfinder**. The research problem is
**Performative Physical Design (PPD)**.

## Research Direction in One Paragraph

Classical physical design treats the workload as an external input. Agentic
workloads can violate that assumption: an agent decides which data,
representations, and tools to access partly according to what is affordable
under the deployed design. Materializing an embedding, digest, or other
derived representation may therefore reduce access cost and induce requests
that are absent from the current log. Pathfinder studies physical design over
a versioned multimodal representation graph, jointly selecting what to
materialize, where to place it, and where transformations execute. Its
Adaptive Workload Model represents design-induced demand as a set rather than
a point estimate. Optimistic Elastic Design then commits only to changes that
are beneficial under a pessimistic bound, and buys an expensive reveal only
when an optimistic bound shows that the otherwise censored demand may justify
the transition. The central empirical question is whether this endogenous
workload effect is real and large enough to require a new optimizer.

## The Change from the Previous Direction

The previous direction assumed a recurring workload `W` and asked whether
joint `M/L/E` planning plus Structured Autoresearch could optimize it:

```text
observed workload W
  -> estimate plan costs
  -> choose P = (M, L, E)
  -> run experiments when cost estimates are uncertain
```

The updated direction makes the workload a function of the deployed design:

```text
physical design D
  -> changes access prices and availability
  -> changes which tasks an agent attempts
  -> induces workload W(D)
  -> changes which design is valuable
```

Cost-side uncertainty remains important, but it is no longer the main reason
for Autoresearch. The harder uncertainty is demand that cannot be observed
while a representation remains too expensive to reach.

## Physical-Design Object

Let the representation graph be:

```text
R = (V, T)

V: versioned physical representations
T: deterministic, fingerprint-addressable transformations
```

Examples of nodes include compressed objects, decoded media, sampled clips,
tensors, tokens, embeddings, structured digests, and promotable session
artifacts. Representation sizes may expand and contract non-monotonically.

A conceptual design is:

```text
D = (M, L, E)

M: representations and shards to materialize
L: node, zone, and tier placement of replicas
E: transformation executors and delivery paths
```

The system compiles `D` into an executable `PhysicalPlan` with a plan epoch,
operations, capabilities, fallbacks, telemetry, and audit requirements.
`D` denotes the research-level choice; `PhysicalPlan` denotes its executable
realization.

## Endogenous Workload

Let `W(D)` be the workload induced by design `D`. The first model uses task
classes `q in Q`:

- class arrival rate `Lambda_q`;
- per-session access budget `k_bar_q`;
- task value `u_q`;
- a terminal representation `tau(q)`;
- class-by-representation access rate `n_qv(D)`; and
- successful-session rate `r_q(D)`.

Access is class-specific. Let:

```text
p_qv(D)           = quoted access price shown to task class q for representation v
realized_cost_qv(D) = realized resource/latency cost charged by the objective
```

The class index is necessary because the same replica can be local to a
training consumer and remote from an analytics or agent consumer. Quoted price
and realized cost are distinct: the resolver gates and the agent respond to
`p_qv(D)`, while `Phi` charges `realized_cost_qv(D)`.

Utility is credited once per successful session, not once per access. This
prevents an optimizer from treating additional tool calls as inherently
valuable. Accesses contribute cost even when they are intermediate operations
that do not complete a task.

The initial scope treats queued session arrivals as exogenous:

```text
sum_v n_qv(D) <= Lambda_q * k_bar_q
r_q(D) <= Lambda_q
```

The design changes what a session can afford and therefore what it touches. It
does not create additional user sessions. Interactive systems in which lower
latency attracts more sessions are outside the first guarantee.

The current theory also assumes quoted-price sufficiency over the deployed
operating range:

```text
n(D) = eta(p(D))
r(D) = rho(p(D))
```

That is, conditional on the quoted price matrix, felt latency or another
unmodeled feature of the design does not materially change access or success.
Identical agents and prompts do not establish this assumption; the evaluation
must test it directly.

## Objective and Solution Concepts

For a horizon `H`, define the performative value of a design:

```text
Phi(D) =
    H * sum_q u_q * r_q(D)
  - H * lambda * sum_qv n_qv(D) * realized_cost_qv(D)
  - storage_cost(D)
```

The steady-state performative optimum is:

```text
D_PO = argmax_{D in D_gov} Phi(D)
```

A transition is evaluated separately:

```text
Gain(D_t -> D') =
    Phi(D') - Phi(D_t) - transition_cost(D_t -> D')
```

This separation ensures that transition cost appears exactly once.

Three notions must remain distinct:

1. **Performative optimum:** maximizes value under the workload the same
   design induces.
2. **Performative stable point:** a best response to the distribution induced
   by the incumbent. This is the fixed-point notion used in performative
   prediction.
3. **OED certificate:** a candidate-relative robust statement over the
   generated, governance-valid designs that the current demand envelope can
   certify.

Pathfinder targets safe progress and a bounded uncertainty certificate over
its generated candidate pools. It does not initially claim to compute the
global performative optimum.

## Why the Classical Loop Can Fail

### Degenerate cost minimization

If observed demand disappears when a task is unaffordable, directly
substituting `W(D)` into a cost-only objective rewards a design for driving
demand away. A design serving no derived task can then appear cheapest.
The PPD objective must credit successful work, not only charge served work.

### Design-induced censoring

When the access-path resolver rejects a task whose quoted path cost exceeds
its declared budget, the current log contains zero demand for that
class-representation pair. The log alone cannot distinguish:

- no latent demand; from
- substantial demand that would appear if the representation became
  affordable.

This is an identification problem, not merely a noisy cost estimate.

### Self-confirming lock-in

Repeatedly optimizing against the last observed workload can retain a shallow
representation with visible demand while never materializing a deeper
representation whose higher latent value remains censored. The resulting
design can be a stable fixed point of re-optimization and still be far from
the performative optimum.

The claim is about re-optimization that imputes missing demand from its own
censored log. A bandit that preserves exploration bonuses may escape, but
must pay expensive physical transitions. It is therefore a strong baseline,
not something the project assumes away.

## Adaptive Workload Model

Pathfinder does not require a point prediction for the entire response
function. It maintains a history-indexed feasible set:

```text
O_t = all deployed designs, observations, costs, and task outcomes
E_t(D) = demand and success states consistent with O_t and the assumptions
```

The first envelope uses:

1. **Own-price monotonicity:** lowering a representation's quoted price does
   not reduce its own access rate.
2. **Declared substitution groups:** lowering one member may move demand away
   from another, but should not reduce the class's total demand for the group.
3. **Success monotonicity:** lowering access prices does not reduce graded task
   success.
4. **Exogenous arrivals and finite per-session access:** provide a finite
   class-level upper bound.
5. **Graph locality:** changing one representation price affects only its
   declared graph/resource closure.
6. **Quoted-price sufficiency:** demand and success respond to `D` through
   the class-specific quoted-price matrix over the declared operating range.

These are falsifiable assumptions, not universal properties of agents.

For a candidate `D`, the planner computes:

```text
Phi_t^-(D) = minimum value over E_t(D) and cost-confidence sets
Phi_t^+(D) = maximum value over E_t(D) and cost-confidence sets
```

The extrema are solved over the coupled feasible set. Substituting independent
per-cell lower and upper bounds is unsound because demand shares a session
budget and can move within substitution groups.

Observed probe designs remain in `O_t` after rollback. A Reveal is useful only
if later rounds retain and use its observation.

## Optimistic Elastic Design

Every planning round begins on the last certified safe design
`D_safe`. Candidate designs are divided into:

- **certifiable candidates:** their envelope is sound relative to the safe
  design, or the exact design has already been observed;
- **probe candidates:** they make at least one unresolved class-representation
  state affordable and can therefore buy new information; and
- **other candidates:** neither certifiable nor uncensoring under the current
  structure.

The third pool is an explicit limitation. The initial OED guarantee does not
cover the entire governance-legal design space.

### Commit

Commit when the robust pessimistic gain exceeds a safety margin:

```text
Phi_t^-(D')
  - Phi_t^+(D_safe)
  - upper_transition_cost(D_safe -> D')
  > margin
```

A Commit changes `D_safe`. The certified safe-design sequence should improve
monotonically when all envelope and cost-confidence assumptions hold.

### Reveal

When no Commit is available, consider a probe only if its optimistic value can
cover:

- transition to the probe;
- transition back to `D_safe`;
- foreground loss during the observation window; and
- the declared per-excursion and cumulative exploration budgets.

After observing the probe, restore `D_safe` but retain the observation.
The next round may then certify, reject, or continue holding the probe design.

Resolution is indexed by price, not only by a class-representation pair. Before
the run, declare the finite affordable price universe:

```text
P_qv = {
  p_qv(D) :
  D in D_gov and p_qv(D) <= choke_price_qv
}

canonical_price_qv = max P_qv
```

The general Reveal-count bound is:

```text
sum_qv |P_qv|
```

It improves to `|Q||V|` only when every Reveal is taken at a candidate already
quoting the canonical price for its target pair. It improves to `|V|` only
when every Reveal is simultaneously canonical for all classes that can afford
the target representation. A common affordability gate alone is insufficient;
class-independent quoting together with a common gate is a sufficient
structural condition.

Candidate selection should therefore prefer:

```text
simultaneously canonical probes
  -> pair-canonical probes
  -> other budget-feasible probes
```

A non-canonical Reveal remains legal, but consumes one level of the general
`sum_qv |P_qv|` bound.

### Hold and stop

A candidate that clears neither test is held, not declared globally inferior.
The loop can stop in two different states:

- **certificate-limited stability:** no generated certifiable candidate has
  positive pessimistic gain and no generated probe has positive optimistic
  gain;
- **budget-limited:** a useful probe may exist but cannot be afforded.

Only the first state supports a candidate-relative stability statement.

For certifiable candidates, the unresolved stability radius includes all
uncertainty used by the failed Commit test:

```text
delta_t =
  max over D' in G_cert of
    value_width_t(D')
  + value_width_t(D_safe)
  + transition_cost_width_t(D_safe, D')
```

Candidate width alone is insufficient unless incumbent value and transition
cost are known exactly. Termination follows from the finite predeclared price
universe and the finite design domain; it does not require every Reveal to
have a strictly positive minimum excursion cost. The exploration purse limits
financial exposure rather than proving termination.

## Escalation Ladder

Full materialization should be the last information-gathering instrument:

```text
analytical model
  -> operator or transfer microbenchmark
  -> workload slice on existing replicas
  -> partial materialization or lazy population
  -> full Reveal deployment
```

The cheaper rungs resolve cost-side uncertainty and may create reusable
shards. They cannot identify demand that appears only after the complete
representation or coverage threshold becomes affordable.

Partial materialization is useful only when value scales sufficiently with
coverage. If a representation becomes useful only after full corpus coverage,
partial routing cannot substitute for a Reveal.

## Governance Boundary

Governance restricts the feasible domain:

```text
D_gov = designs satisfying the active semantic and governance constraints
```

It is not a soft penalty and not a fourth physical decision variable.
Pathfinder receives versioned constraints and transformation-specific
attestations from an external policy authority. It does not interpret laws or
certify de-identification.

By default, derived representations inherit their parents' restrictions. A
broader eligibility set requires an external attestation bound to the source
policy version and transformation fingerprint. The planner validates the
attestation; the Data Agents enforce authenticated plan capabilities locally.

The first paper evaluates policy enforcement and whether a policy changes the
best physical path. It does not claim that an embedding, caption, digest, or
other derived representation is legally anonymous.

## Initial Workload Scope

The paper studies a shared corpus consumed through:

1. **Agentic inference:** endogenous access intensity and task abandonment;
2. **Batch training:** primarily fixed scheduled demand, but a consumer of
   shared representations; and
3. **Analytics:** primarily fixed query arrivals, also sharing the same
   representations.

The core PPD premise depends on the agentic stream. Training and analytics
provide cross-mode amortization and resource interaction; they do not need to
be modeled as equally elastic.

The primary case study should contain:

- a representation whose construction is expensive enough that full
  experimentation is material;
- a non-monotonic representation graph;
- at least two access modes that reuse the candidate artifact;
- a task whose demand is censored while the artifact is absent; and
- one policy constraint that changes a legal placement or execution boundary.

A structured multimodal digest is a stronger candidate than a cheap embedding
index when each individual mode already justifies the index.

## Core Research Questions

### RQ1: Does Performative Physical Design occur?

Does reducing representation access cost change which tasks an agent attempts
or how many representations it accesses per session?

### RQ2: Are the envelope assumptions empirically defensible?

Do substitution-group totals, task success, finite session budgets, and graph
locality behave as required in the target queued-agent regime?

### RQ3: Does design-induced censoring cause practical lock-in?

Does repeated re-optimization settle on a self-confirming design while a
deployed reduced-instance oracle finds a materially better design?

### RQ4: Is the Adaptive Workload Model useful?

Does it contain the realized demand and success state while remaining narrow
enough to classify a useful fraction of candidates?

### RQ5: Does OED improve the information-cost trade-off?

Does Commit/Reveal/Hold reach a strong safe design with less exploration loss
than bandit, Bayesian, passive, random, and repeated-reoptimization baselines?

### RQ6: Does joint cross-mode `M/L/E` planning add value?

After accounting for full materialization, storage, transition, rollback, and
governance costs, does joint planning outperform single-mode and sequential
planners?

## Intended Contributions

The target contribution stack is:

1. **PPD problem formulation:** physical design with endogenous,
   design-censored agent demand and a non-degenerate session-value objective.
2. **Failure characterization:** conditions under which cost-only optimization
   degenerates and observed-log re-optimization can lock in.
3. **Adaptive Workload Model:** a history-indexed, coupled feasible set for
   latent access and task-success states.
4. **Optimistic Elastic Design:** robust Commit, budgeted Reveal, Hold, and
   cost-side escalation over a structured physical-design space.
5. **Pathfinder system:** representation catalog, access-path resolver,
   Session Manager, per-node Data Agents, safe plan epochs, and artifact
   promotion.
6. **Falsifiable evidence:** pilot gates for the PPD premise and end-to-end
   comparison against strong physical-design and exploration baselines.

Hardness, reveal-count, no-thrashing, and commit-tightness results remain
candidate theoretical contributions until their definitions and proofs pass a
separate formal audit.

## Formalization Items to Resolve Before Freezing Claims

The paper and implementation should not depend on unresolved theorem wording.
Before claiming the current guarantees:

1. distinguish performative optimality, performative stability, and
   candidate-relative OED stability;
2. repair the non-identifiability construction so the response functions obey
   the class access budget at every price vector, not only at the incumbent;
3. verify that the implemented candidate generator draws every quote from the
   finite, predeclared price universe required by the corrected Reveal-count
   theorem;
4. state robust commit tightness relative to the explicit ambiguity set and
   distinguish safe commits from total exploration loss;
5. define the initial design and counting unit in the irreversible-deployment
   lower bound; and
6. complete the task-class and success mapping in the NP-hardness reduction.

These are mathematical specification tasks. They must be completed before
experiments are used to support the theorems.

## Falsification and Stop Conditions

The direction should be narrowed or stopped if:

1. agent access intensity and task selection are insensitive to representation
   access price in the target workloads;
2. group-total or task-success monotonicity fails broadly enough that the
   envelope cannot certify useful candidates;
3. quoted price is not a sufficient mediator because felt latency or another
   design feature materially changes access or success after conditioning on
   `p(D)`, unless an enforceable latency reservation repairs the model;
4. a strong forecaster accurately predicts censored demand from the incumbent
   trace without costly deployment;
5. partial materialization cheaply reveals the relevant demand in every target
   regime;
6. the envelope is sound but too wide to classify candidates;
7. repeated re-optimization does not exhibit a meaningful lock-in gap;
8. the generator's excluded or `other` candidate pool contains most oracle
   value; or
9. joint cross-mode `M/L/E` planning offers no benefit over strong sequential
   planners.

If PPD fails but joint physical planning remains valuable, the project may
fall back to a transition-aware `M/L/E` planner. If joint planning also fails,
the direction should not be expanded through more Autoresearch machinery.

## Non-Goals for the First Paper

The first paper does not attempt to:

- optimize interactive user arrival rates that respond to latency;
- solve arbitrary continuous-price demand response;
- guarantee global optimality over the full governance-legal design space;
- automatically discover correct substitution groups without validation;
- schedule KV-cache pages, prefixes, batches, or intra-node GPU kernels;
- choose model architectures, prompts, or logical agent workflows;
- design cross-organization identity or plan-negotiation protocols;
- interpret laws or certify anonymization;
- build a new object-store consistency protocol; or
- use an LLM as the correctness mechanism for physical planning.

## Target Paper Narrative

1. Agentic systems can make physical design performative: the design changes
   what agents can afford, and therefore changes the workload used to judge
   the design.
2. Cost-only optimization becomes ill-posed under abandonment, while
   observed-log re-optimization can become self-confirming.
3. The missing demand cannot always be point-estimated from the incumbent
   trace, but structural and operational assumptions can define a sound,
   testable feasible set.
4. Pathfinder commits only when robustly safe, reveals only when the upside
   can pay for an explicitly budgeted excursion, and otherwise holds.
5. A representation-aware physical system realizes those decisions over
   materialization, placement, execution, governance, session artifacts, and
   safe plan epochs.
6. The evaluation first tries to falsify the PPD premise and its envelope
   assumptions, then measures whether the surviving method improves physical
   design and exploration cost.

## Short Research Pitch

Pathfinder studies physical design for agentic workloads whose demand depends
on the design itself. A representation that is absent or too expensive can
record zero demand even when making it affordable would unlock valuable agent
tasks, so re-optimizing against the current log can become self-confirming.
Pathfinder models versioned multimodal representations and jointly selects
materialization, placement, and transformation execution under external
governance constraints. Its Adaptive Workload Model brackets censored demand;
Optimistic Elastic Design commits only to robust improvements and spends a
budgeted physical Reveal only when cheaper probes cannot resolve a potentially
valuable demand state. The project succeeds only if agent demand is measurably
elastic, the envelope assumptions survive falsification tests, and the system
beats strong physical-design and exploration baselines after charging every
transition and probe.
