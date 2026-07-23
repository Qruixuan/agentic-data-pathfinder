# Autoresearch for Self-Driving Physical Data Path Optimization in Distributed Multimodal Data Lakes

## Purpose of This Document

This document defines the research direction: the problem, scope, hypotheses,
and intended contributions. It deliberately avoids committing to a detailed
architecture or benchmark setup.

Companion documents contain the working details:

- [System Design](SYSTEM_DESIGN_MULTIMODAL_DATALAKE.md)
- [Evaluation Plan](EVALUATION_PLAN_MULTIMODAL_DATALAKE.md)
- [Research Roadmap and Risks](RESEARCH_ROADMAP_MULTIMODAL_DATALAKE.md)
- [Ordered Development Checklist](DEVELOPMENT_CHECKLIST_MULTIMODAL_DATALAKE.md)
- [Background Research and Subdirection Map](BACKGROUND_RESEARCH/00_SUBDIRECTION_MAP.md)

## Research Direction in One Paragraph

Modern AI pipelines repeatedly transform remote multimodal objects into
model-consumable representations. A video, for example, may appear as a
compressed object, decoded frames, sampled clips, tensors, visual tokens, and
embeddings. Each representation has a different storage footprint, production
cost, transfer cost, reuse scope, and validity condition. This project studies
a workload-aware physical data path optimizer that jointly chooses
representation materialization, distributed placement, and
transformation/delivery placement. Its autoresearch loop turns plan selection
into an explicit sequence of testable hypotheses and controlled experiments:
it proposes semantically legal plan changes, selects trials that can resolve
uncertain plan rankings, collects operator- and resource-level evidence, and
updates or deploys a plan only when the expected horizon-wide benefit exceeds
the experiment and transition cost. The initial target is recurring batch AI
pipelines over an existing object store and heterogeneous CPU/GPU cluster.

## What Autoresearch Means in This Project

Here, **autoresearch** is the system's evidence-acquisition loop for a
structured physical-plan space. It does not mean automating literature review
or paper writing, and it is more specific than repeatedly running a generic
black-box tuner.

At iteration `t`, the system maintains:

```text
S_t = (W, R, G, P_t, D_t)
```

where `W` is the recurring workload, `R` is the versioned representation
graph, `G` is the infrastructure and current resource state, `P_t` is the
deployed physical plan, and `D_t` is the accumulated evidence from profiles,
trials, and production traces.

The autoresearch loop performs seven explicit steps:

1. **Observe:** detect an uncertain plan choice, model error, workload change,
   or resource-state change.
2. **Hypothesize:** propose a small number of structured `M/L/E` plan changes
   and record why each could improve the current plan.
3. **Validate:** reject candidates that violate lineage, compatibility,
   capacity, or safety constraints.
4. **Design an experiment:** choose a microbenchmark, workload slice, or canary
   that is expected to resolve a decision-relevant uncertainty at low cost.
5. **Execute and measure:** collect representation-, operator-, storage-, and
   network-level traces, including materialization and migration work.
6. **Update evidence:** correct local cost-model components and update the
   confidence of candidate-plan rankings.
7. **Decide or stop:** deploy only when horizon-wide expected benefit exceeds
   transition cost and uncertainty; otherwise retain the current plan, run
   another useful experiment, or stop when further information is not worth
   its cost.

Autoresearch and self-driving optimization are related but not identical.
Autoresearch decides **what evidence the system should acquire next**;
self-driving optimization uses that evidence to **select, deploy, and revisit
a physical plan**. The same closed loop supports both initial plan discovery
and later adaptation to contention or workload drift.

The loop is deliberately bounded:

- it explores structured plan transformations, not arbitrary system knobs;
- a semantic validator, rather than a learned model or LLM, decides legality;
- every experiment has a measurement, disruption, and transition budget;
- every result is tied to exact workload, data, plan, code, and cluster
  provenance; and
- the system may conclude that the analytical model is already sufficient and
  that no further experiment is justified.

## Motivation

AI workloads consume text, images, audio, video, documents, tensors, tokens,
and embeddings from distributed storage. Before an accelerator can use an
input, its data path may cross object storage, shared network links, local SSD,
host memory, preprocessing workers, and accelerator memory.

Existing systems commonly optimize one part of this path at a time. A data
loader prefetches objects, a cache retains selected outputs, a scheduler places
tasks near data, and a storage tier absorbs repeated reads. These mechanisms
are useful, but their decisions interact:

- Caching decoded frames saves CPU work but can multiply storage and network
  traffic relative to compressed video.
- Replicating a derived representation improves locality but consumes capacity
  and may add substantial one-time materialization cost.
- Moving decoding near storage reduces transferred bytes after filtering, while
  moving it near GPUs may exploit available worker capacity and avoid managing
  a large intermediate representation.
- Staging data for one job can evict a representation reused by another job.

The opportunity is therefore not merely a better cache policy. It is to give
the system a representation-aware physical plan and optimize the whole path by
which logical data becomes valid model input.

## Why Multimodal Data Is a Distinct Physical-Design Problem

A logical multimodal object naturally forms a graph of physical
representations:

```text
compressed video
    -> decoded frames
    -> sampled and resized frames
    -> model-ready tensors
    -> visual tokens
    -> embeddings
```

This resembles database physical design—choosing layouts, replicas, and
materialized views—but adds two important properties.

First, representation size is not monotonic. Decoding a compact object can
greatly expand it, while tokenization or embedding may reduce it again. The
best place to cross a network boundary therefore depends on both computation
and data expansion.

Second, reuse has semantic constraints. A decoded frame may be reusable across
jobs, while a random augmentation is not generally reusable. Tokens and
embeddings depend on preprocessing code, parameters, and model versions. The
optimizer must preserve lineage, determinism, freshness, and compatibility;
these are correctness constraints rather than performance trade-offs.

## Core Research Question

> Can a representation-aware optimizer jointly choose what multimodal data to
> materialize, where to place it, and where to transform and deliver it so that
> recurring AI pipelines achieve better end-to-end performance and resource
> efficiency than systems that optimize these layers independently?

This question has three parts:

1. **Abstraction:** Can one physical data path plan express the decisions and
   correctness constraints shared by multimodal pipelines?
2. **Joint planning:** Do materialization, placement, and
   transformation/delivery need to be optimized together once representation
   expansion, cross-job reuse, and transition cost are included?
3. **Autoresearch:** Can the system autonomously choose decision-relevant
   experiments and use their traces to find or maintain a strong plan within a
   practical measurement, disruption, and reconfiguration budget?

## Initial Scope

The first paper should study **recurring batch pipelines** whose workflow DAGs
and reuse horizons are observable. Examples include repeated embedding jobs,
multimodal training epochs, and versioned offline inference pipelines.

The system is an optimization and management layer over an existing durable
object store; it is not a new object-store consistency protocol or a complete
replacement for a lakehouse. A multimodal data lake in this project means a
collection of immutable or versioned logical objects plus metadata for their
derived representations, lineage, locations, and compatibility.

The initial optimization scope contains three coupled decisions:

1. **Representation materialization:** which deterministic, reusable
   intermediate representations should exist and for what reuse horizon.
2. **Placement and replication:** which storage or cache tiers and cluster
   locations should hold each selected representation.
3. **Transformation and delivery placement:** where representation-changing
   operators execute and from which selected representation consumers are
   staged or served.

Cache allocation, prefetch depth, chunk size, and logical transfer topology may
be exposed by the prototype, but they should not all become independent primary
research dimensions in the first version. They are either derived from the
three decisions above, fixed during an experiment, or added only after an
ablation shows that they materially affect the central result.

Online serving is a possible later workload class. It has a different objective
(goodput under tail-latency constraints) and should not be mixed into the first
evaluation unless the recurring-batch result is already convincing.

## Problem Abstraction

The optimizer receives:

1. A workload graph `W` describing operators, dependencies, access patterns,
   repetition, and required representation versions.
2. A representation graph `R` describing logical objects, physical
   representations, transformation edges, sizes, determinism, lineage, and
   compatibility.
3. An infrastructure graph `G` describing compute nodes, storage tiers,
   capacities, network links, and measured resource state.
4. An evidence store `D` containing versioned microbenchmarks, prior trial
   results, model uncertainty, and traces linked to exact plans and system
   states.

For the focused research problem, a physical plan is:

```text
P = (M, L, E)
```

where:

- `M` selects reusable representations to materialize;
- `L` selects their tier placement and replication; and
- `E` selects transformation placement and the delivery path to consumers.

A concrete system plan may contain lower-level cache, staging, chunking, and
routing fields, but those fields implement `M`, `L`, and `E` rather than define
six unrelated research problems.

For a known recurring workload horizon `H`, the primary objective is to
minimize amortized end-to-end completion time:

```text
minimize   execution_time(P, H) + transition_cost(P_previous -> P)
subject to storage_usage(P) <= B_storage
           network_usage(P) <= B_network
           monetary_cost(P, H) <= B_cost
           lineage, compatibility, and freshness constraints
```

The same outcome can be reported as valid samples consumed per second, but the
objective must include materialization and migration costs. A plan should not
appear better merely because its expensive preparation was omitted from the
measurement window.

When the current evidence cannot rank the best candidates confidently, the
autoresearch problem is to select the next safe experiment `e`:

```text
maximize   expected_decision_improvement(e | D)
           ------------------------------------------------
           trial_cost(e) + disruption(e) + transition_cost(e)

subject to semantic validity, resource budgets, and safety constraints
```

This is an experiment-selection objective, not the runtime performance
objective itself. It asks whether acquiring a particular trace is worth its
cost because it can change a consequential plan decision.

## Research Hypotheses

### H1: Joint Planning Has Measurable Value

Jointly optimizing representation materialization, placement, and
transformation/delivery will outperform layer-local policies under workloads
where transformations change data size, representations are reused, and
storage, CPU, or network resources contend.

This hypothesis is falsifiable: if independent per-layer optimization reaches
the same plans or performance across representative conditions, the claimed
need for a unified optimizer is weak.

### H2: Reuse Horizon and Transition Cost Change the Best Plan

Plans selected from steady-state throughput alone will make systematically
poor choices when materialization, replication, migration, invalidation, and
reuse horizon are significant. A transition-aware planner will choose
different plans as reuse count and version churn change and will reduce
horizon-wide completion time.

### H3: Structured Autoresearch Is Experiment-Efficient

A structural analytical model will capture sizes, nominal bandwidth, and
operator costs but will be inaccurate under skew, queueing, and shared-resource
contention. An autoresearch policy that uses representation and resource
structure to select targeted experiments will improve plan ranking or
adaptation at lower total trial and transition cost than passive observation,
random/exhaustive trials, or a generic black-box tuner with the same budget.

This hypothesis is deliberately falsifiable. If the analytical model already
ranks plans correctly, or if a generic tuner performs equally well at equal
budget, the planner may remain useful but autoresearch should not be claimed as
a separate contribution. The label is not itself novelty; decision-relevant
experiment selection and the evidence it produces must be measurably useful.

## Intended Contributions

The intended paper should make three defensible contributions:

1. **A multimodal physical data path abstraction.** A representation-aware plan
   that unifies materialization, placement, and transformation/delivery under
   explicit lineage and compatibility constraints, reuse horizons, and
   transition costs.
2. **A structured autoresearch method for physical planning.** A method that
   generates legal plan hypotheses, chooses decision-relevant trials, combines
   an analytical model with trace evidence, and stops or deploys according to
   experiment and reconfiguration budgets.
3. **End-to-end evidence from a distributed prototype.** An implementation on
   an existing execution substrate, initially FlowMesh, demonstrating when and
   why joint optimization and targeted experimentation improve recurring
   multimodal pipelines—and when either provides no additional value.

The main novelty claim should be the combination of the physical-plan
abstraction, transition-aware joint planning, and an autoresearch policy
designed for that structured space. Combining an object store, an LRU cache,
and a generic black-box tuner would not be sufficient.

## Position Relative to Adjacent Work

The novelty boundary must be established against at least four neighboring
areas:

- **ML input-pipeline services and intermediate caching** already choose
  preprocessing outputs to cache and may scale data workers. The proposed work
  must go beyond this by optimizing distributed representation placement and
  transformation/delivery boundaries across jobs.
- **Distributed training caches and storage fabrics** exploit access patterns,
  locality, or sample importance. The proposed work adds versioned
  representations of the same logical item and treats their production and
  delivery as plan choices.
- **Multimodal lakehouse and ML storage formats** organize and stream complex
  data. The proposed work focuses on workload-driven physical paths across
  derived representations and heterogeneous resources.
- **Database physical design and configuration tuning** search materialization,
  placement, or configuration choices. The proposed work needs a structured,
  semantically constrained representation graph and data-path traces rather
  than only a flat knob vector and scalar benchmark result.
- **Self-driving systems and generic online tuners** already profile workloads,
  explore configurations, and adapt deployed systems. The proposed
  autoresearch loop must exploit shared structure across representations,
  operators, and resources to choose more informative experiments at the same
  trial and disruption budget.

Particularly close work includes systems that automatically cache outputs from
multiple preprocessing stages and coordinate memory and storage. Therefore,
“choosing which intermediate representation to cache” cannot stand alone as
the novelty claim. The differentiating experiment must exercise distributed
placement or replication together with a transformation/network boundary.

The closest starting references from the completed background map include:

- [Deep Lake: a Lakehouse for Deep Learning](https://vldb.org/cidrdb/2023/deep-lake-a-lakehouse-for-deep-learning.html)
- [Plumber: Diagnosing and Removing Performance Bottlenecks in Machine Learning Data Pipelines](https://proceedings.mlsys.org/paper_files/paper/2022/hash/d0e90e9a9310570dfa643aa3b2da6e89-Abstract.html)
- [Cachew: Machine Learning Input Data Processing as a Service](https://www.usenix.org/conference/atc22/presentation/graur)
- [Pecan: Cost-Efficient ML Data Preprocessing with Automatic Transformation Ordering and Placement](https://www.usenix.org/conference/atc24/presentation/graur)
- [HyCache: Hybrid Caching for Accelerating DNN Input Preprocessing Pipelines](https://www.usenix.org/conference/atc25/presentation/jha)
- [Seneca: Intermediate-Representation Caching for DNN Input Pipelines](https://www.usenix.org/conference/fast26/presentation/desai)
- [KeystoneML: Optimizing Pipelines for Large-Scale Advanced Analytics](https://shivaram.org/publications/keystoneml-icde17.pdf)
- [Blaze: Holistic Caching for Iterative Data Analytics](https://doi.org/10.1145/3627703.3629558)
- [LlamaTune: Sample-Efficient DBMS Configuration Tuning](https://www.vldb.org/pvldb/vol15/p2953-kanellis.pdf)
- [UDO: Universal Database Optimization using Reinforcement Learning](https://www.vldb.org/pvldb/vol14/p3402-wang.pdf)

The detailed comparison and caveats are recorded in
[Background Research Synthesis](BACKGROUND_RESEARCH/99_SYNTHESIS_AND_RESEARCH_GAPS.md).
The final novelty claim must still be revised after a systematic literature
search.

## Evidence Required for the Direction to Succeed

Before expanding the system, a small testbed should establish all of the
following:

1. At least one recurring multimodal workload is materially bottlenecked on its
   data path rather than model computation.
2. The best representation changes with network bandwidth, transformation
   cost, placement, cache capacity, or reuse horizon.
3. A joint plan materially outperforms a strong intermediate-caching baseline
   and an independent per-layer optimizer.
4. The autoresearch experiment selector identifies useful traces and reaches a
   better plan—or the same plan at lower total experiment cost—than
   analytical-only, passive, and generic-tuner baselines at equal budgets.
5. The loop can refuse low-value experiments, account for artifacts reused
   across trials, and stop or roll back without hiding transition work.

If item 2 is not observed, the representation-aware problem may collapse into
ordinary caching. If item 3 is not observed, the joint-planning thesis should
be narrowed. If items 4 and 5 are not observed, the system can still contribute
a physical planner, but autoresearch should not be the headline.

## Non-Goals for the Initial Paper

The initial work should not attempt to:

- design a new durable object-storage or consistency protocol;
- replace an existing S3-compatible data lake;
- physically reconfigure datacenter switches;
- optimize model parallelism, gradient communication, or GPU kernels;
- cover every online and batch objective in one optimizer;
- automate literature review, paper writing, or unrestricted scientific
  discovery;
- flatten every system parameter into an unstructured black-box search space;
- use an LLM as the sole plan generator or correctness mechanism; or
- claim novelty from adding a distributed cache to FlowMesh.

## Target Paper Narrative

1. Multimodal pipelines expose several physical representations for each
   logical object.
2. Their sizes, production costs, reuse conditions, and validity constraints
   make representation, placement, and transformation/delivery decisions
   strongly interdependent.
3. We introduce a physical data path abstraction that makes these decisions
   jointly optimizable.
4. We formulate autoresearch as decision-relevant experiment selection over
   this structured plan space, with semantic, trial, disruption, and transition
   constraints.
5. The system automatically proposes plan hypotheses, runs targeted canaries,
   updates local cost models from traces, and stops or deploys according to the
   value of further evidence.
6. A distributed prototype demonstrates the conditions under which joint
   planning and autoresearch improve end-to-end performance and cost, including
   negative regimes where one or both are unnecessary.

This framing can fit a database or data-systems venue if the catalog,
representation lineage, declarative plan, cost model, and correctness
constraints remain first-class. FlowMesh is the execution substrate, not the
research contribution by itself.

## Short Research Pitch

We study autoresearch for self-driving physical data path optimization in
distributed multimodal data lakes. The system models each logical object as a
versioned graph of compressed media, decoded data, tensors, tokens, and
embeddings, then jointly plans which representations to materialize, where to
place them, and where to transform and deliver them. When the structural cost
model cannot rank plans confidently, an autoresearch controller proposes
testable plan hypotheses, selects a low-cost microbenchmark or canary that can
resolve the uncertainty, records operator- and resource-level evidence, and
updates or deploys a plan only when its reuse-horizon benefit exceeds
experiment and transition cost. The research asks both whether this joint plan
beats strong layer-local optimizers and whether structured experiment selection
reaches good decisions more efficiently than passive measurement or generic
black-box tuning.
