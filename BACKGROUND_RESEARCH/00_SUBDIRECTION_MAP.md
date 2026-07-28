# Background Research: Subdirection Map

## 1. Purpose

This directory maps the communities that intersect with Pathfinder and
separates mature mechanisms from the remaining research gap.

The revised project studies a performative physical design:

```text
D = (M, L, E)

M: materialized reusable representations
L: cross-node and cross-tier locations and replicas
E: transformation, serving, and delivery execution

D -> offered access and class-specific prices p_qv(D) -> W(D) -> session value Phi(D)
```

The new premise is that the workload is not entirely fixed. A design changes
which representations an agent can afford or observe, and this can change the
tasks it completes and the value the system subsequently measures.

## 2. Eight Research Subdirections

### D1. Multimodal Representations and Physical Formats

Studies compressed objects, frames, tensors, tokens, embeddings, structured
artifacts, and formats for random access, scans, streaming, and versioning.

Reusable foundation: representation nodes, sizes, schemas, granularities, and
metadata. Missing intersection: choosing a representation path jointly with
distributed layout and design-dependent agent access.

See [D1: Multimodal Representations and Data Lakes](01_MULTIMODAL_REPRESENTATION_AND_LAKE.md).

### D2. Input Pipelines and Transformation Optimization

Studies decode, sampling, augmentation, batching, operator ordering,
parallelism, profiling, and remote or storage-near execution.

Reusable foundation: transformation graphs, service-rate measurements, and
execution split points. Missing intersection: persistent cross-session
representations whose availability changes agent behavior.

See [D2: Input Pipelines and Transformation](02_INPUT_PIPELINE_AND_TRANSFORMATION.md).

### D3. Distributed Caching, Placement, Prefetching, and Delivery

Studies caching, replication, prefetching, eviction, and scheduling across RAM,
NVMe, shared storage, and remote nodes.

Reusable foundation: tier and replica management under capacity and
contention. Missing intersection: demand that is censored by the current
physical access path rather than exogenously given.

See [D3: Distributed Caching, Placement, and Delivery](03_DISTRIBUTED_CACHE_PLACEMENT_AND_DELIVERY.md).

### D4. Workflow Materialization, Reuse, Lineage, and Correctness

Studies derived-data identity, intermediate reuse, incremental recomputation,
garbage collection, and semantic equivalence.

Reusable foundation: compatibility, lineage, invalidation, and version
contracts. Missing intersection: probe-created artifacts that are both
observations and potentially reusable physical state.

See [D4: Workflow Materialization and Semantics](04_WORKFLOW_MATERIALIZATION_AND_LINEAGE.md).

### D5. Cost Models, Physical Planning, and Search

Studies cost-based operator and physical-structure selection, resource
allocation, placement, combinatorial search, and transition-aware planning.

Reusable foundation: structured candidate generation, constrained
optimization, deployment cost, and reduced-instance oracles. Missing
intersection: optimizing session-level value when the workload itself is a
function of design.

See [D5: Physical Planning and Search](05_COST_MODEL_PLANNING_AND_SEARCH.md).

### D6. Automated Experiments and Safe Adaptation

Studies active experiment selection, optimal design, bandits, Bayesian tuning,
canaries, rollback, drift, and low-disruption online control.

Reusable foundation: evidence acquisition and safe deployment. In the revised
project these mechanisms become Reveal and escalation machinery over
predeclared class-specific price levels; they are not the main novelty by
themselves.

See [D6: Automated Experiments, Reveal, and Safe Adaptation](06_TELEMETRY_AND_ONLINE_ADAPTATION.md).

### D7. Governance-Constrained and Geo-Distributed Planning

Studies residency, ownership, retention, consumer, transfer, attestation,
audit, revocation, and deletion constraints on distributed execution.

Reusable foundation: externally supplied feasibility constraints and local
enforcement. Pathfinder consumes `D_gov`; it does not infer legal meaning.

See [D7: Governance-Constrained and Geo-Distributed Physical Planning](07_GOVERNANCE_AND_FEDERATED_DATA_PLACEMENT.md).

### D8. Performative Systems, Endogenous Workloads, and Censored Feedback

Studies decisions that change the future data distribution, the difference
between stable and globally optimal solutions, selective observation, and
exploration under action-dependent feedback.

Reusable foundation: distribution maps, performative risk, stability
distinctions, causal interventions, partial identification, and exploration.
Missing intersection: expensive, stateful physical designs that determine
representation affordability and leave reusable artifacts after experiments.

See [D8: Performative Systems and Endogenous Workloads](08_PERFORMATIVE_SYSTEMS_AND_ENDOGENOUS_WORKLOADS.md).

## 3. Relationship to the Core Problem

| Subdirection | `M` | `L` | `E` | `W(D)` / evidence role |
|---|---:|---:|---:|---|
| D1 Representations | Strong | Medium | Medium | Defines alternatives offered to agents |
| D2 Transformations | Medium | Medium | Strong | Maps designs to price, latency, and quality |
| D3 Caching/delivery | Medium | Strong | Medium | Controls physical availability and realized cost |
| D4 Lineage/reuse | Strong | Medium | Medium | Validates reusable and probe-created state |
| D5 Planning/search | Strong | Strong | Strong | Optimizes feasible designs and transitions |
| D6 Experiments/adaptation | Medium | Medium | Medium | Implements Reveal, restoration, and escalation |
| D7 Governance | Constraint | Constraint | Constraint | Defines externally authorized `D_gov` |
| D8 Performative systems | Indirect | Indirect | Indirect | Models endogenous demand and censored outcomes |

The new core is the intersection of D1–D5 with D8. D6 supplies the active
observation mechanism. D7 limits the legal intervention space.

## 4. Common Review Questions

Each subdirection should be reviewed using:

1. What object and task class does the system model?
2. Which parts of `M`, `L`, and `E` are controlled?
3. Is demand fixed, drifting independently, or induced by the design?
4. Which choices and outcomes are unobserved under the incumbent design?
5. Are class-specific quoted prices, felt latency, and realized costs
   distinguished, and is quoted-price sufficiency tested?
6. What assumptions identify counterfactual behavior?
7. Are stability, optimality, and candidate-relative guarantees separated?
8. What does an experiment change physically, and can its artifacts be reused?
9. Are transition, disruption, and restoration costs included?
10. Which external authority defines legality, and where is it enforced?
11. Which strong baseline or falsification test can refute the claimed gap?

## 5. Core and Peripheral Boundaries

The first paper's core is:

- a versioned multimodal representation graph;
- joint `M/L/E` design;
- design-dependent access for queued agent sessions;
- an Adaptive Workload Model over a coupled ambiguity set;
- OED decisions over certified, probeable, and unreachable candidates; and
- complete transition and exploration accounting.

Training and analytics initially contribute fixed reuse and resource demand.
Governance is a supported constraint unless it creates a distinct evaluated
interaction.

The first paper does not target:

- universal optimization for arbitrary distributed systems;
- arbitrary online arrival elasticity;
- a new object-store consistency protocol;
- model, kernel, or collective-communication optimization;
- natural-language data discovery;
- continuous unrestricted price design;
- automatic policy or legal interpretation;
- anonymization certification;
- identity federation or trusted-execution design; or
- a general-purpose agent behavior theory.

## 6. Research Artifacts

1. This map defines the eight subdirections and their boundaries.
2. Files `01`–`08` review representative mechanisms and limitations.
3. [The synthesis](99_SYNTHESIS_AND_RESEARCH_GAPS.md) identifies the central
   performative physical-design gap and its required falsification tests.
