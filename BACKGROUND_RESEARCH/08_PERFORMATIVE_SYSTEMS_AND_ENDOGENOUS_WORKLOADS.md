# D8. Performative Systems, Endogenous Workloads, and Censored Feedback

## 1. Scope

This subdirection studies systems in which a deployed decision changes the
future data or behavior used to evaluate that decision.

For Pathfinder:

```text
physical design D
  -> offered representations and class-specific access prices p_qv(D)
  -> agent choices and task outcomes W(D)
  -> session-level value Phi(D)
```

The design is therefore not evaluated against a single fixed workload trace.
Ordinary traces reveal only behavior under the incumbent access path, and an
unavailable or unaffordable representation may have no reads even when making
it accessible would create substantial value.

## 2. Closest Research Foundations

| Area or work | Reusable idea | Boundary relative to Pathfinder |
|---|---|---|
| [Performative Prediction, ICML 2020](https://proceedings.mlr.press/v119/perdomo20a.html) | A deployed model induces the distribution on which it is evaluated; defines performative stability and studies repeated retraining | The decision is a predictive model, not a costly physical design with representation, layout, execution, and transition state |
| [Outside the Echo Chamber, ICML 2021](https://proceedings.mlr.press/v139/miller21a.html) | A performatively stable point can be far from the optimizer of performative risk | Direct warning that Pathfinder must not equate fixed-point stability with global session-value optimality |
| Selective-label and censored-feedback research, e.g. [Chang and Wiens, ICML 2024](https://proceedings.mlr.press/v235/chang24e.html) | Outcomes are observed only after an earlier decision permits the relevant action or test | Pathfinder observes value for representations the design offers and the agent selects; counterfactual success can remain hidden |
| [DBA Bandits, ICDE 2021](https://renata.borovica-gajic.com/data/2021_icde.pdf) | Online physical design through exploration and direct performance observation with safety concerns | Treats queries as incoming workload and indexes as actions; it does not model the index design as changing which queries or task outcomes appear |
| [HMAB, PVLDB 2022](https://www.vldb.org/pvldb/vol16/p216-perera.pdf) | Hierarchical bandits reduce a combinatorial physical-design action space | Useful baseline for candidate exploration, but observations are still rewards of chosen structures under the current workload |
| Active experiment design and safe tuning in D6 | Targeted measurements, canaries, rollback, refusal, and switching-cost control | Supplies mechanisms for Reveal, but not the workload ambiguity model or the performative physical-design objective |

## 3. What Is Already Established

### 3.1 Deployed decisions can induce their evaluation distribution

Performative-prediction work formalizes a decision-to-distribution map and
shows why optimizing on a fixed historical distribution can be wrong. This
provides the conceptual basis for replacing a fixed workload `W` with `W(D)`.

Pathfinder should reuse the distinction, not claim that feedback-induced
distribution shift is new.

### 3.2 Stability and optimality are different

Repeatedly optimizing against the distribution induced by the current
decision may converge to a performatively stable point. That point need not
minimize global performative risk. The corresponding systems lesson is:

- **performative optimum:** maximizes `Phi(D)` under the workload induced by
  that same design;
- **performative stability:** is locally self-consistent with an optimization
  against its induced workload; and
- **OED certificate:** establishes only the declared lower/upper-bound
  relation over the modeled candidate pools.

These terms must not be used interchangeably.

### 3.3 Observation is decision-dependent

Selective-label research studies settings where outcomes are visible only
after a prior decision. Pathfinder has an analogous but distinct censoring
mechanism: a task cannot reveal demand or success for a representation that
was never offered, was unaffordable, or was rejected by policy.

This makes "no reads" ambiguous among:

- no latent task value;
- insufficient access budget;
- an absent or slow representation;
- substitution toward another representation;
- an agent policy that did not recognize the option; and
- policy or resolver rejection.

Ordinary telemetry cannot identify these cases without interventions or
structural assumptions.

### 3.4 Bandits provide exploration machinery, not the whole model

Bandit physical-design systems show how to learn rewards of candidate
structures online and provide important safety and regret baselines.
Pathfinder differs only if it demonstrates all of the following:

- the action changes the workload response, not only query execution time;
- a representation graph creates structured counterfactual constraints;
- observations are coupled through task classes and substitution groups;
- actions have large, state-dependent transition and restoration costs; and
- experiments can leave reusable physical artifacts.

Without these properties, a contextual or combinatorial bandit is likely the
simpler formulation.

## 4. Mechanisms to Reuse

### 4.1 Explicit decision-to-workload map

Treat workload response as a first-class function `W(D)` rather than residual
noise around a fixed trace. Log the complete offered choice set so the observed
response is interpretable.

### 4.2 Causal interventions

Use randomized access quotes for low-cost identification, then validate their
construct validity with real physical changes matched on the complete
class-specific quote matrix `p_qv`. Separately vary felt latency while holding
the quote fixed: the current response model is valid only if access and success
are conditionally insensitive to that latency. Fix agent code, task
distribution, and seeds where possible.

### 4.3 Partial identification

When counterfactual behavior cannot be point-identified, maintain a feasible
set of response functions consistent with:

- observations;
- class-specific affordability over a predeclared finite price universe
  `P_qv`;
- task-class and substitution-group structure;
- justified monotonicity assumptions; and
- bounded success/quality relationships.

Optimize extrema over the coupled set. Independent per-variable intervals may
violate the shared constraints and should not be substituted without proof.

### 4.4 Separate safe deployment from exploration

Maintain `D_safe` as the certified incumbent. A Reveal action may temporarily
deploy another design to acquire evidence, but it returns to `D_safe` unless a
separate Commit condition is satisfied. The safe sequence and exploratory
trajectory require different loss accounting.

## 5. Remaining Research Gap

The plausible gap is **performative physical design**:

> Select a costly, stateful multimodal physical design when that design changes
> which representations agents can access and therefore changes the workload
> and session value used to evaluate it.

This intersection adds constraints not normally present together in
performative prediction, selective-label learning, bandits, or conventional
physical design:

- a representation DAG rather than an unconstrained action vector;
- joint materialization, layout, and execution decisions;
- class-specific access budgets and ex ante finite price-level sets `P_qv`;
- persistent state and nontrivial transition/restoration cost;
- probe-created artifacts with possible future reuse;
- multiple task classes with substitution effects;
- external governance constraints; and
- unreachable candidates outside the current probe mechanism.

## 6. Pathfinder's Proposed Mechanisms

### Adaptive Workload Model

AWM maintains a history-indexed ambiguity set for `W(D)` rather than predicting
one response surface with unjustified certainty. It should expose:

- which observations and assumption versions define the set;
- lower/upper values for each candidate;
- which constraints make a bound tight; and
- where assumption violations invalidate a certificate.

### Optimistic Elastic Design

OED partitions candidates into:

- `G_cert`: evaluable well enough for a safe Commit test;
- `G_probe`: not certifiable, but reachable by a bounded Reveal; and
- `G_other`: currently neither certified nor probeable.

It can:

- **Commit** a design whose conservative gain is positive;
- **Reveal** when optimistic information and improvement value cover the full
  probe cost; or
- **Hold** when neither action is justified.

The resulting guarantee is relative to the ambiguity set and candidate pools,
not automatically a global performative optimum.

Reveal resolution is also relative to the ex ante price-level universe:
the general count is bounded by `sum_{q,v}|P_qv|`; `|Q||V|` requires every
Reveal to be pair-canonical; and `|V|` requires every Reveal to be
simultaneously canonical for all affording classes. OED should prefer the
simultaneously canonical tier, then the pair-canonical tier, before using a
budget-feasible fallback. Finite access gates alone do not imply the smaller
bounds.

The no-thrashing margin must include uncertainty in candidate value, incumbent
value, and transition cost. Termination follows from the finite design and
price-level domains, not from assuming every excursion has a positive minimum
cost.

## 7. Required Counterfactuals and Falsification

The new subdirection is justified only if experiments show:

1. real physical designs causally change access and session value;
2. naive read-react-materialize locks into an inferior design through censored
   demand;
3. AWM assumptions pass predeclared falsification tests;
4. conditional on quoted `p_qv`, felt latency has no material residual effect
   on access or success, or the implementation enforces an equivalent latency
   reservation;
5. AWM bounds cover held-out or reduced-oracle responses and improve on trivial
   bounds;
6. OED outperforms equal-budget passive, random, bandit, Bayesian, and
   black-box exploration after all costs; and
7. the remaining gap is not explained entirely by omitted candidates in
   `G_other`.

If design-dependent response is weak, use fixed-workload physical planning. If
the response is strong but the structural assumptions fail, the scientific
problem becomes workload identification. If quoted-price sufficiency fails,
reserve latency or expand AWM to `eta(p, latency)` and `rho(p, latency)` before
claiming the scalar-price theory. If a bandit performs equally well, OED
should not be claimed as necessary.

## 8. Formal Issues to Resolve

Before finalizing theoretical claims:

- ensure the non-identifiability witness respects all access budgets over the
  full price domain;
- verify that the implementation constructs `P_qv` from the full predeclared
  legal design domain and applies the exact conditions for each reduced
  Reveal bound;
- state tightness only relative to an explicit ambiguity set;
- distinguish safe-sequence guarantees from probe loss;
- specify the starting design and transition-counting unit in lower bounds;
  and
- make hardness reductions encode the intended task and success semantics.

## 9. Main Takeaway

Prior work already establishes performative feedback, the difference between
stability and optimality, decision-dependent observation, bandit exploration,
and safe online tuning. Pathfinder's possible contribution is their
combination with a versioned, distributed representation path whose physical
state changes agent affordability and whose experiments have persistent
transition effects. The project must validate that causal chain before its new
formal and systems machinery is justified.
