# A Self-Driving Physical Data Path Optimizer for Distributed Multimodal Data Lakes

## Research Direction Description

### Executive Summary

Modern AI pipelines consume large collections of heterogeneous data, including
text, images, audio, video, documents, tensors, tokens, and embeddings. These
objects are commonly stored in remote data lakes while computation runs on a
distributed and heterogeneous cluster. Keeping accelerators supplied with data
is difficult because the end-to-end data path crosses object storage, network
links, local disks, host memory, preprocessing workers, and accelerators.

Existing systems usually optimize one part of this path in isolation. A data
loader may prefetch objects, a cache may retain frequently accessed files, a
scheduler may place computation near cached data, and a storage system may move
objects between tiers. Such local decisions are often insufficient for
multimodal workloads. The same logical item can exist as a compressed object,
decoded frames, resized tensors, tokens, or an embedding. Materializing a later
representation saves computation but consumes more storage and network
bandwidth. Replicating data improves locality but may congest shared links.
Staging more data can hide latency but may evict representations needed by a
concurrent pipeline.

This project proposes a distributed multimodal data lake with a self-driving
physical data path optimizer. Given a workload and a cluster, the optimizer
jointly decides:

- which representations of each logical object to materialize;
- how to chunk, place, and replicate those representations;
- what to admit to each cache tier and what to evict;
- when and where to stage or prefetch data;
- which logical transfer topology should deliver data to consumers; and
- where data transformation operators should execute.

The system uses an autoresearch loop to improve these decisions from empirical
measurements. Each iteration proposes a small batch of legal physical plans,
executes them on representative workload slices, records end-to-end and
operator-level telemetry, and updates a model of the data path. The goal is not
generic configuration tuning. The central research problem is the joint
optimization of representation, placement, transformation, and delivery for
multimodal data.

### Research Hypothesis

The primary hypothesis is:

> A multimodal workload should be optimized through a unified physical data
> path plan. Jointly optimizing representation materialization, data placement,
> caching, staging, and delivery topology can achieve substantially higher
> goodput than independently optimizing storage, network, and execution layers.

A second hypothesis is:

> An analytical model alone cannot reliably predict performance under data
> skew, heterogeneous transformations, shared-resource contention, and changing
> workloads. A hybrid optimizer that combines structural cost modeling with
> targeted empirical trials can find strong plans using a practical experiment
> budget and adapt when the environment changes.

### Scope and Terminology

In this document, a **multimodal data lake** is a data management layer that
stores immutable or versioned logical objects and their derived
representations. It maintains metadata about identity, representation,
lineage, location, size, freshness, and compatibility.

A **workload** is a collection or stream of data-consuming workflow DAGs. A DAG
may contain scans, sampling, decoding, filtering, augmentation, tokenization,
embedding, retrieval, training, and inference operators.

A **physical data path plan** specifies how logical inputs reach their
consumers. It includes persistent physical design decisions and transient
execution decisions. It is analogous to a database physical plan, but spans
storage, network, preprocessing, and accelerator-facing delivery.

The term **network topology** refers primarily to a configurable logical
delivery topology, such as direct reads from the origin, peer-assisted reads,
relay trees, or rack-local distribution. The project does not assume that the
physical datacenter network can be rewired for every workload.

The initial scope should emphasize recurring batch AI pipelines because they
provide explicit DAGs, measurable reuse, and stable evaluation. Online serving
can be added as a second workload class after the batch design is established.

### Why Multimodal Data Changes the Optimization Problem

For relational data, a physical designer commonly chooses layouts, indexes,
partitions, replicas, and materialized views. Multimodal AI pipelines expose a
related but different representation hierarchy. For example:

```text
compressed video
    -> decoded frames
    -> sampled and resized frames
    -> model-ready tensors
    -> visual tokens
    -> embeddings
```

Each node in this representation graph has a different size, production cost,
transfer cost, reuse scope, and invalidation rule. Some transformations are
deterministic and reusable across jobs. Others depend on random seeds, model
versions, preprocessing code, or query-specific parameters.

Consequently, the cheapest source representation is not always the cheapest
end-to-end choice. Sending compressed video saves network bandwidth but consumes
CPU near the accelerator. Sending decoded frames saves CPU but may multiply
network traffic. Caching embeddings is valuable for repeated retrieval, but is
invalid after an embedding model changes. The optimizer must reason about these
interactions explicitly.

### Formal Problem Definition

The system receives three principal inputs.

1. A workload graph `W` describing operators, dependencies, access patterns,
   deadlines, and expected repetition.
2. A representation graph `R` describing logical objects, physical
   representations, transformation edges, versions, sizes, and compatibility.
3. An infrastructure graph `G` describing compute nodes, storage tiers, cache
   capacities, links, bandwidth, latency, and current utilization.

For a workload `W`, the optimizer chooses a physical plan:

```text
P = (M, L, C, S, T, O)
```

where:

- `M` selects representations to materialize;
- `L` selects storage-tier placement and replication;
- `C` selects cache admission, allocation, and eviction policies;
- `S` selects staging and prefetch schedules;
- `T` selects logical transfer and sharing topology; and
- `O` selects the placement and parallelism of transformation operators.

For recurring batch pipelines, the main objective can be expressed as:

```text
maximize   sustained valid samples per second
```

or equivalently as minimizing workflow makespan, subject to storage, compute,
network, and monetary budgets. The metric should count only data that is
successfully consumed by the target computation, so speculative work that is
never used does not inflate performance.

For online workloads, the objective should be goodput under a latency service
level objective rather than unconstrained throughput:

```text
maximize   completed requests per second
subject to p99 latency <= L
           storage usage <= B_storage
           network usage <= B_network
           monetary cost <= B_cost
```

Freshness, representation compatibility, and correctness are hard constraints,
not quantities that the optimizer may trade away for speed.

### Proposed System Architecture

The proposed system contains four logical layers.

```text
Workload API and FlowMesh workflow DAGs
                  |
                  v
Representation catalog and physical data path planner
                  |
                  v
Distributed data plane: object store, caches, staging, transformations
                  |
                  v
FlowMesh workers and AI consumers on CPUs and GPUs

Telemetry --------------------------------> Autoresearch optimizer
     ^                                               |
     +---------------- candidate plans --------------+
```

#### 1. Representation Catalog

The catalog provides the database metadata needed to distinguish this system
from an unmanaged collection of files. It records:

- stable logical object identifiers;
- modality and schema information;
- chunks and physical encodings;
- derived representation lineage;
- transformation code, parameters, and version fingerprints;
- object locations and replica states;
- size and observed access statistics;
- freshness and compatibility constraints; and
- cacheability and determinism properties.

The catalog should allow the planner to ask whether two workflows can safely
reuse the same decoded data, tokens, or embeddings.

#### 2. Distributed Data Plane

The data plane manages remote object storage, node-local NVMe, host memory, and
optionally accelerator memory. It exposes explicit operations for materialize,
replicate, stage, pin, evict, and invalidate. A node-local agent reports cache
contents, tier capacities, transfer rates, and transformation throughput.

The data plane should support multiple delivery strategies without changing
the logical workload. Examples include direct reads from object storage,
rack-local relay caches, peer-assisted reads, and a producer that decodes an
object once and distributes the derived representation to multiple consumers.

#### 3. Physical Data Path Planner

The planner compiles the logical workload into a legal physical plan. It uses
the catalog and cluster graph to prune infeasible or semantically invalid
choices before empirical search begins. For example, it must not reuse an
embedding generated by an incompatible model version or cache the result of a
random transformation as if it were deterministic.

The planner should expose structured plan transformations rather than a flat
collection of unrelated knobs. Candidate transformations may include:

- move a transformation before or after a network boundary;
- materialize or remove an intermediate representation;
- change the replication factor of a hot shard;
- replace origin reads with a peer or relay delivery tree;
- resize cache allocation between modalities or representations;
- alter chunk size and prefetch depth; and
- co-locate a transformation with its producer or consumer.

#### 4. Telemetry and Provenance

Every trial should record enough information to explain its result. Required
telemetry includes per-operator service time, queueing time, bytes transferred,
link utilization, cache hits by representation, staging waste, CPU and GPU
utilization, accelerator starvation, object-store requests, and end-to-end
goodput. Measurements must be linked to the exact workload, plan, dataset
version, code version, and cluster state.

### The Autoresearch Loop

The optimizer executes the following loop:

```text
propose plans -> validate plans -> execute trials -> measure -> learn -> deploy
```

#### Candidate Proposal

The optimizer proposes `K=3` candidate physical plans per iteration. Candidate
generation should combine domain-aware plan transformations with exploration.
An LLM may help explain results or suggest transformations, but plan validity
and the core optimization procedure should not depend on unconstrained natural
language generation.

#### Trial Execution

Candidates are evaluated using representative workload slices, short pilot
runs, or canary traffic. Expensive persistent changes, such as creating a large
derived representation, must be accounted for as reconfiguration cost. The
optimizer should amortize or avoid such changes rather than measuring only
steady-state performance after preparation is complete.

Parallel trials introduce a subtle measurement problem. Although candidates
may run in separate containers, they can still share object-store bandwidth,
network links, and disks. The system must either isolate trial resources,
schedule non-conflicting candidates together, or estimate and remove
cross-trial interference. Otherwise, the autoresearch loop can learn an
incorrect ranking.

#### Learning and Selection

A promising design is a hybrid cost model:

```text
estimated cost = structural analytical model + learned residual
```

The analytical component captures object sizes, transformation DAGs, cache
capacity, placement, and nominal bandwidth. The learned residual captures
contention, skew, implementation effects, and hardware behavior that are hard
to model. Operator-level traces provide denser supervision than a single final
throughput value.

The optimizer maintains a Pareto frontier over goodput, cost, and resource
usage. It should periodically re-evaluate selected plans to detect workload or
infrastructure drift. A deployed plan is replaced only when the expected gain
exceeds migration cost and uncertainty.

### Intended Research Contributions

The intended paper should make the following contributions.

1. **A multimodal physical data path abstraction.** The abstraction unifies
   representation lineage, materialization, placement, transformation, cache,
   staging, and delivery decisions under explicit correctness constraints.
2. **A joint optimizer for representation, placement, and routing.** The work
   should demonstrate analytically and empirically why independent layer-by-layer
   optimization can be arbitrarily or substantially suboptimal.
3. **A trace-guided autoresearch algorithm.** The algorithm searches structured
   physical plans, learns unknown costs from targeted trials, accounts for
   reconfiguration cost, and adapts to workload drift.
4. **A distributed prototype integrated with FlowMesh.** The prototype should
   execute real multimodal AI pipelines and demonstrate end-to-end improvements
   across modalities, workloads, and cluster configurations.

The novelty claim should be based on the combination of a new physical plan
abstraction and an optimizer designed for that abstraction. Merely combining
an object store, an LRU cache, and a generic Bayesian optimizer would not be a
sufficient contribution.

### Relationship to Existing Research

Several adjacent areas must be separated carefully from this project.

- Multimodal lakehouse systems already provide formats and streaming access for
  complex objects. This project focuses on workload-driven physical data paths
  across derived representations and distributed resources.
- ML input pipeline systems already autoscale preprocessing and cache selected
  outputs. This project jointly optimizes representation, placement, staging,
  and delivery topology across a workload DAG.
- Distributed training caches already exploit repeated sample access or sample
  importance. This project models multiple representations of the same logical
  item and cross-job reuse with versioned lineage.
- Storage fabrics already use workload information to place data in faster
  tiers. This project includes transformation placement and network delivery as
  part of the same physical plan.
- Database tuning systems already use Bayesian optimization or reinforcement
  learning to search configuration knobs. This project searches a structured,
  semantically constrained plan space and uses data-path traces rather than only
  scalar benchmark outcomes.

Relevant starting points include:

- [Deep Lake: a Lakehouse for Deep Learning](https://vldb.org/cidrdb/2023/deep-lake-a-lakehouse-for-deep-learning.html)
- [Symphony: Natural-Language Query Answering over Multi-modal Data Lakes](https://vldb.org/cidrdb/2023/symphony-towards-natural-language-query-answering-over-multi-modal-data-lakes.html)
- [Cachew: Machine Learning Input Data Processing as a Service](https://www.usenix.org/conference/atc22/presentation/graur)
- [SHADE: Enable Fundamental Cacheability for Distributed Deep Learning Training](https://www.usenix.org/conference/fast23/presentation/khan)
- [Tectonic-Shift: A Composite Storage Fabric for Large-Scale ML Training](https://www.usenix.org/conference/atc23/presentation/zhao)
- [LlamaTune: Sample-Efficient DBMS Configuration Tuning](https://www.vldb.org/pvldb/vol15/p2953-kanellis.pdf)
- [UDO: Universal Database Optimization using Reinforcement Learning](https://www.vldb.org/pvldb/vol14/p3402-wang.pdf)

A comprehensive related-work review is still required before finalizing the
novelty claim.

### Integration with FlowMesh

FlowMesh can serve as the control and execution plane rather than the data lake
itself. Existing capabilities that are directly useful include:

- workflow DAG parsing and task lifecycle management;
- distributed worker registration and dispatch;
- S3, SQL, and external data retrieval;
- artifact references between stages;
- cached model and dataset locality hints;
- stage stickiness and task merging;
- heterogeneous inference, training, retrieval, and multimodal executors; and
- task-level runtime and resource telemetry.

The project requires new components beyond the current FlowMesh runtime:

- a representation and lineage catalog;
- a chunk and replica metadata service;
- a distributed multi-tier cache agent;
- explicit staging and materialization operations;
- network topology and bandwidth monitoring;
- a physical data path plan schema and compiler;
- fine-grained data-path telemetry; and
- an experiment manager and optimizer.

The desired division of responsibility is:

```text
new multimodal data plane
        +
FlowMesh control and execution plane
        +
autoresearch physical plan optimizer
```

### Evaluation Plan

The evaluation should answer five research questions.

#### RQ1: End-to-End Performance

Does joint data path optimization improve sustained goodput, workflow makespan,
accelerator utilization, and tail latency relative to existing approaches?

#### RQ2: Value of Joint Optimization

How much performance is lost when representation materialization, placement,
caching, staging, and routing are optimized independently? This question is
central to justifying the system architecture.

#### RQ3: Search Efficiency

How many trials, how much data, and how much wall-clock time are required to
reach a strong plan? The optimizer should be compared with random search, a
generic black-box tuner, the analytical model alone, and an oracle obtained by
exhaustive search on small instances.

#### RQ4: Adaptation and Generalization

Can the optimizer reuse knowledge across dataset scales, modalities, models,
and cluster configurations? How quickly does it recover after changes in
workload mix, network bandwidth, cache capacity, or transformation cost?

#### RQ5: System Overhead and Robustness

What are the overheads of metadata maintenance, telemetry, plan transitions,
and experimental trials? How robust are measurements when candidate plans run
in parallel and share infrastructure?

#### Candidate Workloads

The initial benchmark suite should contain at least three pipelines:

- image-text embedding or contrastive training;
- video-text decoding, sampling, and embedding; and
- audio-text preprocessing and inference or training.

An additional mixed workload should run pipelines concurrently to expose cache
competition and network contention. Workloads should be repeated across several
epochs or invocations so that materialization and caching decisions have a
meaningful amortization horizon.

#### Testbed

A useful initial testbed contains:

- remote S3-compatible object storage;
- multiple FlowMesh compute workers;
- heterogeneous CPU and GPU resources;
- RAM and local NVMe cache tiers;
- configurable or emulated link bandwidth and latency; and
- enough data to exceed aggregate cache capacity.

The final evaluation should include multiple cluster scales and at least one
heterogeneous configuration. A small homogeneous cluster alone would not
exercise the proposed planner.

#### Baselines

At minimum, the evaluation should compare against:

- remote streaming with no application-managed cache;
- node-local LRU or LFU caching;
- static full-dataset staging when capacity permits;
- locality-aware task placement;
- independent per-layer optimization;
- a generic black-box tuner over the same plan parameters;
- the analytical cost model without empirical correction; and
- an exhaustive-search oracle on reduced problem instances.

#### Metrics

Primary metrics include:

- valid samples or requests consumed per second;
- batch makespan and online p50/p95/p99 latency;
- accelerator utilization and starvation time;
- bytes read from each storage tier;
- cross-rack and total network traffic;
- cache hit rate by representation;
- preprocessing CPU and GPU time;
- storage footprint and monetary cost;
- optimizer convergence time and number of trials; and
- reconfiguration and trial interference overhead.

### Suggested Implementation Stages

#### Stage 1: Measurement and Reproducible Baselines

Build two or three end-to-end multimodal pipelines on FlowMesh. Add operator,
cache, storage, and network telemetry. Demonstrate that data delivery is a real
bottleneck and characterize how the best static configuration changes across
workloads and cluster conditions.

#### Stage 2: Representation Catalog and Controllable Data Plane

Implement logical object identities, representation lineage, version checks,
and explicit materialize, stage, replicate, and evict operations. Support RAM,
NVMe, and remote object storage.

#### Stage 3: Physical Plan and Analytical Model

Define the physical plan schema and legal transformations. Build a first-order
model based on sizes, measured operator throughput, cache capacity, and network
bandwidth. Use exhaustive search on small instances to identify interactions
and validate the model.

#### Stage 4: Trace-Guided Autoresearch

Add batched candidate selection, workload slicing, learned cost correction,
experiment provenance, Pareto tracking, and regression sweeps. Compare
structured search with generic black-box optimization.

#### Stage 5: Dynamic Adaptation

Add workload-drift detection, migration-aware plan changes, and optionally
online workloads. This stage should be attempted only after the static and
recurring-batch story is convincing.

### Major Research Risks

#### Risk 1: The Search Space Is Too Broad

Searching every storage, network, cache, and scheduler parameter at once can
produce an unfocused system. The mitigation is to center the plan around three
decisions first: representation materialization, placement/replication, and
staging/delivery. Secondary knobs should be fixed or tuned within these
decisions.

#### Risk 2: Autoresearch Appears to Be Generic Tuning

If the optimizer sees only a flat parameter vector and final throughput, the
work will be difficult to distinguish from prior database tuning. The
mitigation is a structured plan grammar, explicit semantics, trace-level
learning, and baselines using the same generic tuner.

#### Risk 3: The System Is Not Clearly a Database Contribution

A distributed cache in front of an object store may be viewed as a storage or
ML systems paper. The mitigation is to make the representation catalog,
lineage, declarative workload, physical planning, cost model, and correctness
constraints first-class components.

#### Risk 4: Trial Cost Dominates the Benefit

Large representations can take hours to materialize, and full workload runs
may be expensive. The optimizer must model plan transition cost and use pilot
runs, samples, reuse of prior measurements, and analytical pruning.

#### Risk 5: Evaluation Covers Only One Workload

A result tied to one model and one dataset will not support the general system
claim. The evaluation must vary modality, reuse pattern, dataset size,
transformation cost, network condition, and cache capacity.

### Non-Goals for the Initial Paper

The initial project should not attempt to:

- design a new object storage consistency protocol;
- replace S3-compatible durable storage;
- physically reconfigure datacenter switches;
- optimize model parallelism or gradient communication;
- support every online and batch workload with one objective;
- use an LLM as the sole optimizer; or
- claim novelty from adding a cache to FlowMesh.

### Target Paper Narrative

A concise database-paper narrative is:

1. Multimodal AI pipelines expose multiple physical representations of each
   logical object.
2. Representation, placement, transformation, cache, and delivery decisions
   interact strongly, so existing layer-local optimizations leave accelerators
   starved or waste network and storage resources.
3. We introduce a physical data path abstraction that makes these decisions
   jointly optimizable under lineage and compatibility constraints.
4. We design a hybrid optimizer that combines a structural cost model with
   trace-guided autoresearch to learn unknown contention and adapt to workload
   changes.
5. A distributed implementation on FlowMesh improves end-to-end goodput and
   resource efficiency across multiple modalities and cluster configurations.

This framing is suitable for a database venue because the primary contribution
is a new physical design and optimization problem for data-intensive
workloads. FlowMesh provides the execution substrate, while autoresearch is the
mechanism that makes the physical designer empirical and adaptive.

### One-Paragraph Research Pitch

We propose a self-driving physical data path optimizer for distributed
multimodal data lakes. Unlike conventional data loaders and caches, our system
models each logical object as a versioned graph of physical representations,
such as compressed media, decoded data, tensors, tokens, and embeddings. Given
a workload DAG and a heterogeneous cluster, it jointly chooses representation
materialization, placement, replication, cache allocation, staging, operator
placement, and logical delivery topology. Because analytical estimates cannot
fully capture data skew and shared-resource contention, the optimizer runs
small batches of candidate plans, learns from operator-level execution traces,
and continuously revises its cost model. We will implement the system as a new
distributed data plane integrated with FlowMesh and evaluate whether joint,
trace-guided optimization improves goodput, accelerator utilization, and cost
across image, video, audio, and text pipelines.

### Immediate Decisions Before Implementation

Before committing to a full implementation, the project should settle four
questions experimentally:

1. Which recurring batch workload exhibits the clearest interaction between
   representation choice and network or cache behavior?
2. Which three physical decisions account for most of the performance
   variation in the initial cluster?
3. Can operator-level measurements predict the effect of a plan on a larger
   workload slice better than end-to-end throughput alone?
4. Is logical delivery topology genuinely configurable and beneficial on the
   available hardware, or should the first paper focus on representation,
   placement, and staging?

The answers should determine the final paper scope. The project should expand
only after the central interaction and performance opportunity are reproduced
reliably.
