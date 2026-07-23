# Background Research: Subdirection Map

## 1. Purpose

This directory answers three questions:

1. Which established research communities intersect with this project?
2. What has each community already solved, and what remains open?
3. Which gaps arise only when representation, placement, and execution
   boundaries are considered together?

The project studies the following physical plan:

```text
P = (M, L, E)

M: which reusable representations to materialize
L: which tiers and nodes hold them, and with what replication
E: where transformations execute and how data is delivered to consumers
```

These are not independent features. A transformation may expand or shrink
data, so `M` changes network cost. `L` changes the CPU, storage, and bandwidth
available to transformations. `E` changes which intermediate representations
are worth retaining. The potential contribution comes from this coupling, not
from inventing another isolated cache policy.

## 2. Six Research Subdirections

### D1. Multimodal Representations and Physical Formats for AI Data Lakes

This area studies compressed objects, decoded outputs, tensors, tokens,
embeddings, and physical formats for random access, scans, versioning, and
streaming.

It provides:

- a definition of logical objects and representation graphs;
- physical properties such as size, access granularity, encoding, and version;
- metadata for discovering, querying, and tracking representations.

It usually does not choose an end-to-end cross-node transformation and
delivery path.

See [D1: Multimodal Representations and Data Lakes](01_MULTIMODAL_REPRESENTATION_AND_LAKE.md).

### D2. ML Input Pipelines and Transformation Optimization

This area studies reading, decoding, sampling, augmentation, batching,
profiling, parallelism, operator ordering, scaling, and remote offload.

It provides:

- operator-level profiling and bottleneck diagnosis;
- candidate transformation orders, split points, and execution locations;
- mechanisms for executing transformations on local, remote, or
  storage-near CPUs.

It usually optimizes the current job without managing the full cross-job,
cross-version lifetime of derived representations.

See [D2: Input Pipelines and Transformation](02_INPUT_PIPELINE_AND_TRANSFORMATION.md).

### D3. Distributed Caching, Placement, Prefetching, and Delivery

This area studies caching, replication, prefetching, eviction, and scheduling
across RAM, local SSD, shared storage, and remote nodes.

It provides:

- hierarchical caching and cross-node placement mechanisms;
- future-access-aware prefetching and replica management;
- capacity allocation and delivery under multi-job contention.

It often treats a sample as one physical object rather than a versioned graph
of transformable representations.

See [D3: Distributed Caching, Placement, and Delivery](03_DISTRIBUTED_CACHE_PLACEMENT_AND_DELIVERY.md).

### D4. Workflow Materialization, Reuse, Lineage, and Correctness

This area studies derived-data identity, intermediate materialization,
incremental recomputation, reuse across runs, garbage collection, and the
equivalence conditions required for safe reuse.

It provides:

- representation identity, lineage, and compatibility rules;
- comparisons between materialization benefit and recomputation cost;
- reuse boundaries under randomness and code or model version changes.

It usually emphasizes whether a result can be reused, with less emphasis on
jointly choosing its cross-tier replicas and transformation/network boundary.

See [D4: Workflow Materialization and Semantics](04_WORKFLOW_MATERIALIZATION_AND_LINEAGE.md).

### D5. Cost Models, Physical Planning, and Search

This area formulates operator selection, materialization, resource allocation,
and device or node placement as constrained optimization problems solved with
dynamic programming, ILP, heuristics, Bayesian optimization, or learning.

It provides:

- a formal basis for `P=(M,L,E)`;
- structured pruning, performance prediction, and multi-resource constraints;
- methods for amortizing materialization and migration cost.

The goal is not another generic tuner. It is to use representation-graph
semantics to reduce the search space and expose coupled physical decisions.

See [D5: Physical Planning and Search](05_COST_MODEL_PLANNING_AND_SEARCH.md).

### D6. Structured Autoresearch, Automated Experiments, and Safe Adaptation

This area studies how a system actively selects decision-relevant experiments,
updates evidence from fine-grained telemetry, and safely validates, deploys,
rejects, or revisits plans under model error, drift, and contention.

It provides:

- measurements that separate operator, storage, network, and queueing time;
- adaptive sampling, optimal experiment design, and Bayesian optimization;
- production flighting, candidate isolation, rollback, and safe adaptation;
- accounting for trial, disruption, persistent materialization, and switching
  costs.

Automated experimentation and self-tuning are already mature topics.
Therefore, experiment selection or safe canaries are not novel by themselves.
This project must show that representation DAGs, `M/L/E` plan differences, and
reusable materialized artifacts create a new experiment-selection problem.

See [D6: Structured Autoresearch and Safe Adaptation](06_TELEMETRY_AND_ONLINE_ADAPTATION.md).

## 3. Relationship to the Core Decisions

| Subdirection | Supports `M` | Supports `L` | Supports `E` | Cross-run/online role |
|---|---:|---:|---:|---|
| D1 Representations and formats | Strong | Medium | Medium | Versions and metadata |
| D2 Input pipelines | Medium | Medium | Strong | Job-level adaptation |
| D3 Caching and delivery | Medium | Strong | Medium | Cross-node/cross-job |
| D4 Materialization and lineage | Strong | Medium | Medium | Cross-run reuse |
| D5 Planning and search | Strong | Strong | Strong | Unified optimization |
| D6 Autoresearch and adaptation | Medium | Medium | Medium | Experiments, deployment, drift |

No single neighboring community naturally covers all three decisions. The
review must ask which decisions a system controls, which couplings it omits,
and whether those omissions cause measurable plan errors.

## 4. Common Review Questions

Each subdirection is evaluated with the same questions:

1. **Object model:** files, samples, tensors, workflow nodes, or versioned
   representations?
2. **Decision variables:** which parts of `M`, `L`, and `E` are controlled?
3. **Scope:** single-node or distributed; single-job or multi-job; one run or a
   reuse horizon?
4. **Objective:** throughput, makespan, tail latency, cost, bandwidth, or
   constrained multi-objective optimization?
5. **Correctness:** how are lineage, stochastic transformations, code/model
   versions, and stale data handled?
6. **Dynamics:** are contention, drift, and transition costs included?
7. **Experiments:** are trials selected actively, what state do they change,
   and can their artifacts be reused?
8. **Evidence:** which strong baselines are used, and under which conditions
   does the claimed benefit disappear?

## 5. Core and Peripheral Boundaries

The core intersection is D2–D5: transformation execution, representation
materialization, distributed placement, and semantic constraints in one
physical planning problem. D1 supplies representation and storage foundations.
D6 is an explicit second research question: can this structure acquire
decision-changing evidence more efficiently than generic automated
experimentation?

The first paper does not independently target:

- a new object-store consistency protocol;
- GPU kernels, model parallelism, or gradient communication;
- general lakehouse query optimization or natural-language data discovery;
- tail-latency control for online inference;
- a complete data quality, privacy, and access-control system.

These may later become constraints or workload extensions, but including them
now would weaken the causal argument and explode the experimental matrix.

## 6. Research Artifacts

1. This file defines the subdirections and their boundaries.
2. Files `01`–`06` review representative work, reusable mechanisms, and limits.
3. [The synthesis](99_SYNTHESIS_AND_RESEARCH_GAPS.md) compares capabilities and
   retains only gaps supported by both prior work and falsifiable experiments.
