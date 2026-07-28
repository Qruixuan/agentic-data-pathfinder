# Synthesis: Performative Physical Design for Agentic Workloads

## 1. Why the Synthesis Changed

The revised direction is no longer primarily "joint physical planning plus
Structured Autoresearch." It is:

> A physical design changes which multimodal representations agents can afford
> to access; this changes the realized workload and session value; Pathfinder
> must optimize the resulting endogenous objective under incomplete,
> design-censored observations.

The physical substrate remains `D=(M,L,E)`, but the central modeling change is
from a fixed workload `W` to a design-induced workload `W(D)`.

Structured experimentation has a narrower role. AWM represents ambiguity about
counterfactual workload response. OED uses Commit, Reveal, and Hold to make
decisions or acquire evidence. Partial materialization, canaries, rollback, and
artifact reuse implement Reveal and escalation.

## 2. Conclusions First

### 2.1 The physical-design substrate is useful but crowded

Existing systems already provide:

- multimodal and tensor representations;
- transformation-pipeline optimization;
- multistage caching across memory and storage tiers;
- distributed placement, prefetching, and remote execution;
- workflow materialization, lineage, and reuse;
- joint cost-based physical design;
- transition-aware continuous tuning; and
- safe candidate isolation, deployment, rollback, and cleanup.

A paper centered only on jointly choosing `M`, `L`, and `E` would face strong
prior art. The representation-path interaction remains useful, especially
because transforms expand or contract data and parents can be shared, but it
is now the systems substrate rather than the whole novelty claim.

### 2.2 Conventional traces can be self-confirming

A read-react-materialize controller sees only the representations offered by
its current design. If an expensive structured representation is absent or
unaffordable, it receives no reads and creates no evidence of latent value.
The controller can therefore rationally reinforce the incumbent and remain
locked in.

This is stronger than ordinary workload drift. The design is part of the
causal mechanism generating the observed workload.

### 2.3 Performative research supplies the right warning, not a complete system

[Performative Prediction](https://proceedings.mlr.press/v119/perdomo20a.html)
formalizes decision-induced distributions and performative stability.
[Outside the Echo Chamber](https://proceedings.mlr.press/v139/miller21a.html)
shows why a stable point can be far from the optimizer of performative risk.

Pathfinder should inherit the distinction:

- a performative optimum maximizes `Phi(D)` under `W(D)`;
- a stable design is self-consistent under a specified update rule; and
- an OED certificate is relative to the AWM ambiguity set and the candidate
  pools it actually covers.

The three are not equivalent.

### 2.4 Censored feedback and exploration have strong precedents

Selective-label research studies outcomes observed only after an earlier
decision permits them. Bandit physical-design systems explore indexes or
materialized views through direct rewards. Automated-tuning work supplies
active sampling, canaries, stopping, rollback, and transition-aware trial
ordering.

Pathfinder must therefore demonstrate a narrower intersection:

- representation access, not only execution time, responds to design;
- observations are coupled by task classes and substitution groups;
- physical interventions are expensive and stateful;
- probes may create reusable artifacts; and
- some candidates are not reachable by the current probe mechanism.

### 2.5 AWM is justified only by partial identification

If an ordinary supervised model or bandit can learn the response cheaply, a
new ambiguity-set model is unnecessary. AWM matters when incumbent observations
do not point-identify counterfactual demand, but defensible structural
assumptions still produce useful lower and upper bounds.

Those assumptions must be versioned and falsifiable. Their constraints are
coupled; independently choosing the worst endpoint of every interval may
construct a response function that is not feasible.

The current AWM also makes a quoted-price-sufficiency assumption:
`n(D)=eta(p(D))` and `r(D)=rho(p(D))`. Because workload modes may run on
different consumers, `p(D)` is a class-specific matrix `p_qv(D)`, not one
scalar per representation. Felt latency and realized physical cost remain
separate observables. If felt latency changes response conditional on the
quote, the current scalar-price model and its Reveal-count theorem do not
apply without latency reservation or a larger response model.

### 2.6 OED is a candidate-relative safe controller

OED separates:

- `G_cert`: candidates with sufficient bounds for a Commit decision;
- `G_probe`: candidates reachable by a bounded Reveal; and
- `G_other`: candidates outside the current certificate and intervention set.

Reveal returns to `D_safe` unless a separate Commit certificate is established.
It retains the observation and may retain valid artifacts. This is useful, but
it is not a global optimality guarantee while `G_other` is nonempty.

Before a run, each legal task-class/representation pair has a fixed executable
price-level universe:

```text
P_qv = { p_qv(D) : D in D_gov and p_qv(D) is within the access gate }
```

The general Reveal-resolution bound is `sum_{q,v}|P_qv|`. The reduced
`|Q||V|` and `|V|` bounds require, respectively, pair-canonical Reveals and
Reveals simultaneously canonical for all affording classes; uniform gates
alone are insufficient. OED should prefer simultaneous-canonical,
pair-canonical, and then budget-feasible fallback probes in that order.

Its stability margin must include candidate-value width, incumbent-value
width, and transition-cost width. With time-uniform confidence sets, this
supports no-thrashing reasoning without pretending physical cost is known
exactly. Termination follows from finite legal design and price-level domains;
the exploration purse limits exposure but is not the termination proof.

### 2.7 Governance remains an external feasibility constraint

Geo-distributed compliant planning, provenance, local authorization, auditing,
revocation, and deletion all have precedents. Pathfinder should consume a
versioned `D_gov` and external attestations, then enforce them at the resolver
and Data Agents.

It should not interpret law, infer that a derived representation is safe, or
claim a governance contribution unless constraints create a distinct measured
interaction with performative design.

## 3. Capability Matrices

### 3.1 Physical-Design Foundations

Legend: **Primary** is a central decision, **Partial** is narrower or uses a
different object, and **No** is not central.

| Work | `M` | `L` | `E` | Versioned reuse | Transition state | Endogenous access |
|---|---|---|---|---|---|---|
| [KeystoneML](https://shivaram.org/publications/keystoneml-icde17.pdf) | Primary | Partial | Primary | Partial | Partial | No |
| [HyCache](https://www.usenix.org/conference/atc25/presentation/jha) | Primary | Partial | Partial | Partial | Partial | No |
| [Seneca](https://www.usenix.org/conference/fast26/presentation/desai) | Primary | Partial | Partial | Partial | Partial | No |
| [Blaze](https://doi.org/10.1145/3627703.3629558) | Primary | Primary | Partial | Partial | Primary | No |
| [JellyBean](https://users.cs.duke.edu/~ml579/papers/jellybean_vldb23.pdf) | No | Primary | Primary | No | No | No |
| [Nectar](https://www.microsoft.com/en-us/research/publication/nectar-automatic-management-of-data-and-computation-in-data-centers/) / [HELIX](https://www.vldb.org/pvldb/vol12/p446-xin.pdf) | Primary | Partial | Partial | Primary | Partial | No |
| [Budget-Conscious Physical Design](https://www.vldb.org/pvldb/vol15/p4079-richly.pdf) | Primary | Primary | No | No | Primary | No |
| [Online Physical Design](https://www.microsoft.com/en-us/research/publication/an-online-approach-to-physical-design-tuning/) | Primary | No | No | No | Primary | No |
| [DBA Bandits](https://renata.borovica-gajic.com/data/2021_icde.pdf) | Primary | No | No | No | Primary | No |
| **Pathfinder** | **Primary** | **Primary** | **Primary** | **Primary** | **Primary** | **Primary** |

The last column is the proposed distinction and must be demonstrated causally.
It cannot be inferred merely because workloads change over time.

### 3.2 Behavioral and Experimental Foundations

| Work/area | Decision-induced response | Censored observation | Structural bounds | Safe exploration | Stateful artifacts |
|---|---|---|---|---|---|
| [Performative Prediction](https://proceedings.mlr.press/v119/perdomo20a.html) | Primary | Partial | Primary | No | No |
| [Performative-risk optimization](https://proceedings.mlr.press/v139/miller21a.html) | Primary | Partial | Primary | Partial | No |
| [Selective-label learning](https://proceedings.mlr.press/v235/chang24e.html) | Partial | Primary | Primary | No | No |
| [iTuned](https://www.vldb.org/pvldb/vol2/vldb09-193.pdf) / [Ernest](https://pages.cs.wisc.edu/~shivaram/publications/ernest-nsdi.pdf) | No | No | Partial | Partial | No |
| [OnlineTune](https://arxiv.org/abs/2203.14473) / [OPPerTune](https://www.usenix.org/conference/nsdi24/presentation/somashekar) | No | No | Partial | Primary | No |
| [UDO](https://www.vldb.org/pvldb/vol14/p3402-wang.pdf) | No | No | No | Partial | Partial |
| **Pathfinder AWM/OED** | **Primary** | **Primary** | **Primary** | **Primary** | **Primary** |

Having every column is not sufficient novelty. Evaluation must show that their
interaction changes decisions and outperforms simpler combinations.

## 4. Mature Capabilities to Reuse

### Representation and Execution Substrate

- versioned representation DAGs and transformation fingerprints;
- object, tensor, and columnar multimodal formats;
- operator profiling, parallelism, prefetching, and offload;
- caching and replica management across RAM, NVMe, peers, and object storage;
- lineage, equivalence, invalidation, and reuse; and
- capacity-aware distributed scheduling.

### Physical Planning

- semantic feasibility before cost ranking;
- analytical resource and critical-path models;
- joint and transition-aware physical design;
- candidate dominance and structured search;
- reduced-instance exhaustive oracles; and
- incremental deployment and anti-thrashing.

### Experiments and Safety

- active sampling, optimal design, Bayesian optimization, and bandits;
- trial namespaces and child plans;
- canaries, rollback, restoration, and regression protection;
- evidence stores and experiment provenance;
- expensive-trial ordering and switching-cost accounting; and
- stopping and refusal rules.

### Governance

- externally defined policy facts and compliant candidate generation;
- local authenticated enforcement;
- provenance and auditable operations; and
- revocation, retention, and deletion propagation.

These are required mechanisms and baselines, not independent contributions.

## 5. Updated Research Gaps

### G1. Performative Physical Design — Central Systems Gap

Select a distributed multimodal `D=(M,L,E)` while its access profile changes
the agentic workload and session value. The access profile is the
class-specific quote matrix `p_qv(D)`; felt latency and realized cost are
modeled separately.

**Required evidence:** matched quote and physical interventions must show a
repeatable `D -> access -> W(D) -> Phi(D)` causal chain, and a quote-fixed
latency intervention must support quoted-price sufficiency.

### G2. Censored Read-React-Materialize Lock-In — Central Motivation

Show that an incumbent-dependent trace can hide a valuable representation and
cause a natural materialization controller to remain at an inferior design.

**Required evidence:** a real agent and physical path must reproduce the
lock-in; the superior design must still win after transition cost.

### G3. Adaptive Workload Model — Core Modeling Gap

Maintain a history-indexed coupled ambiguity set for counterfactual task-class
access and success, with explicit affordability and substitution structure.

**Required evidence:** assumption falsification, held-out/reduced-oracle
coverage, tighter bounds than trivial alternatives, and decisions changed by
those bounds.

### G4. Optimistic Elastic Design — Core Control Gap

Choose Commit, Reveal, or Hold relative to `D_safe`, including full forward,
foreground, restoration, and artifact-state costs.

**Required evidence:** equal-budget improvement over passive, random, bandit,
Bayesian, black-box, and UDO-style baselines. Report safe-sequence results
separately from exploratory loss.

### G5. Causal Access-Path Resolver — Measurement Substrate

Expose complete offered choices, class-specific quotes `p_qv`, felt latency,
and realized cost; enforce access budgets; and bind terminal outcomes to task
class and design. Use quote interventions for efficient identification, real
physical interventions matched on `p_qv` for construct validity, and a
quote-fixed latency factorial for the sufficiency assumption.

This is essential infrastructure. It becomes a contribution only if its
semantics support measurements that existing storage traces cannot express.
Queued scheduling imposes the current exogeneity contract; deliberate
re-arrivals should be evaluated as an out-of-scope robustness stress test, not
misreported as a fifth falsification gate.

### G6. Representation-Path and Artifact-Aware State — Physical Substrate

Jointly represent materialization, replicas, transform/serve placement, and
probe-created artifacts over a versioned DAG.

**Required evidence:** joint `M/L/E` must beat strong independent/sequential
planning, and artifact-aware transitions must improve over scalar switching
cost and heavy/light grouping.

### G7. Candidate and Price-Domain Completeness — Guarantee Boundary

The fixed ex ante sets `P_qv`, candidate generator, and probe mechanism define
what OED can certify. `G_other` must remain visible. The implementation must
label each Reveal as simultaneously canonical, pair-canonical, or fallback
and report only the corresponding valid resolution bound.

**Required evidence:** reduced exhaustive instances must decompose loss into
behavioral uncertainty, exploration policy, unresolved price levels, and
omitted-candidate opportunity.

### G8. Governance-Constrained Performative Design — Conditional Extension

Apply the same framework within externally supplied `D_gov`.

**Required evidence for a separate contribution:** governance must change
representation access and the best performative `M/L/E` design in a way that
post-hoc repair handles poorly. Otherwise it remains a correctness feature.

## 6. Formal Gaps in the Current Paper Skeleton

The following are human/theory work, not questions that experiments alone will
repair:

1. distinguish performative optimum, stability, and OED certification;
2. make the non-identifiability witness respect the global access-budget
   domain;
3. verify that the ex ante `P_qv` construction covers the full declared legal
   design domain and that each claimed reduced Reveal bound satisfies its
   exact canonicality condition;
4. state tightness relative to the ambiguity set and separate safe deployment
   from exploration loss;
5. define the initial state and counting unit in transition lower bounds; and
6. complete task and success semantics in the hardness reduction.

Experiments may test assumptions and relevance, but cannot substitute for
correct definitions or proofs.

## 7. Priority Order

| Priority | Item | Reason |
|---|---|---|
| P0 | Formal audit | Prevents experiments from being attached to incorrect claims |
| P0 | G5 causal resolver, elasticity pilot, and quote-sufficiency factorial | Go/no-go gates for the performative premise and current scalar-price theory |
| P0 | G2 reduced real lock-in case | Establishes the central failure mode |
| P1 | G6 minimal `M/L/E` substrate | Makes real design interventions possible |
| P1 | G3 AWM on a reduced oracle | Tests identification and bound soundness |
| P1 | G4 OED with restoration | Tests the proposed controller |
| P1 | G7 candidate/price completeness | Defines honest certificate scope |
| P2 | Cross-mode scale and adaptation | Expands only after the core result |
| P2 | G8 governance interaction | Headline only with separate evidence |

Recommended order:

```text
formal audit
  -> causal resolver and ex ante P_qv
  -> elasticity/monotonicity and quote-sufficiency pilots
  -> static Designs A/B/C/D
  -> reduced exhaustive response surface
  -> real lock-in demonstration
  -> AWM coverage
  -> OED Commit/Reveal/Hold
  -> cross-mode and scale
```

## 8. Required Baselines

### Physical Baselines

1. origin streaming;
2. local LRU/LFU and prefetching;
3. static staging;
4. multistage representation caching;
5. locality-aware transform/consumer placement;
6. independent and sequential `M/L/E` planning;
7. fixed-workload joint `M/L/E` planning;
8. hand tuning; and
9. reduced exhaustive deployment.

### Behavioral/Controller Baselines

1. naive read-react-materialize;
2. passive AWM;
3. analytical point prediction;
4. trivial independent uncertainty boxes;
5. random feasible probes;
6. contextual or combinatorial bandits;
7. Bayesian/black-box tuning;
8. UDO-style transition ordering;
9. OED without partial materialization or artifact reuse;
10. full AWM + OED + escalation; and
11. an omniscient reduced oracle.

All methods must share the same initial `D_safe`, observations, artifacts,
candidate domain, governance constraints, and total budget.

## 9. Minimum Research Slice

- **Corpus:** one full-corpus video dataset.
- **Representation graph:** compressed video, sampled frames, embeddings, and
  one expensive structured multimodal digest.
- **Tasks:** two or three explicit queued agent task classes.
- **Endogenous scope:** agent representation access and completion.
- **Fixed contributors:** recurring training and analytics over shared
  representations.
- **Tiers:** object storage, node NVMe, and host RAM.
- **Executors:** data/storage-near CPU and consumer-local CPU/GPU.
- **Designs:** A/B/C cheap paths plus D exposing the expensive censored path.
- **Price domain:** class-specific finite sets `P_qv`, derived from the full
  legal design domain and frozen before data collection.
- **Oracle:** a reduced instance where every legal design is physically
  deployed.
- **Governance:** one externally supplied multi-zone constraint, initially as
  correctness support.

Add a second modality only after this slice passes the performative premise,
AWM coverage, and lock-in gates.

## 10. Updated Narrative

The defensible thesis is:

> Existing data systems optimize physical designs for observed or forecast
> workloads. For agentic multimodal sessions, however, the physical design can
> determine which representations are affordable and therefore which workload
> and value become observable. Pathfinder models this feedback as performative
> physical design. It uses an Adaptive Workload Model to bound
> counterfactual task-class responses and Optimistic Elastic Design to Commit,
> Reveal, or Hold while accounting for distributed representation state,
> transition, restoration, and candidate coverage.

The project should not claim to be the first joint physical optimizer, online
tuner, safe canary system, bandit designer, performative-learning method, or
governance-aware planner. Its viability depends on demonstrating the narrower
causal and systems intersection against strong simpler alternatives.
