# Subdirection D3: Distributed Caching, Placement, Prefetching, and Data Delivery

## 1. Scope

This subdirection studies how training and analytics data should be placed in memory, local SSDs, remote nodes, and shared storage, and how prefetching, partitioning, replication, eviction, and scheduling can sustain high throughput. It is directly related to location decisions `L` and indirectly affects execution decisions `E` through locality and delivery paths.

Traditional cache systems manage files, blocks, or samples. This project adds a representation-level challenge: one logical sample may have raw, decoded, resized, tensor, token, and embedding forms, all connected by transformations. Choosing what to cache therefore also constrains where transformations occur.

## 2. Representative Work

| Work | What It Achieves | Boundary Relative to This Project |
|---|---|---|
| [Quiver, FAST 2020](https://www.usenix.org/conference/fast20/presentation/kumar) | Shares input caches across users and jobs using content addressing and job-aware cache allocation | Reuses input objects but does not optimize multilevel derived representations and their production locations |
| [NoPFS, SC 2021](https://arxiv.org/abs/2101.08734) | Uses predictable training access sequences to coordinate prefetching and caching across node memory, local storage, and remote nodes | Assumes requested samples are fixed objects rather than candidates generated from a representation DAG |
| [Lobster, ICPP 2022](https://pasalabs.org/papers/2022/Lobster_ICPP22.pdf) | Jointly manages loading, preprocessing threads, load imbalance, and cache eviction for distributed training | Coordinates execution and caching, but versions and cross-run representation materialization are not first-class decisions |
| [SHADE, FAST 2023](https://www.usenix.org/conference/fast23/presentation/khan) | Uses sample importance to improve the utility of distributed training caches | Optimizes sample selection rather than physical representation selection |
| [Tectonic-Shift, ATC 2023](https://www.usenix.org/conference/atc23/presentation/zhao) | Uses application dataset descriptions to predict production ML access and turns flash into an absorption tier before shared storage | Provides a high-performance storage fabric; applications declare objects rather than asking the system to plan their derived transformations |
| [Blaze, EuroSys 2024](https://doi.org/10.1145/3627703.3629558) | Unifies caching, eviction, and recovery for iterative Spark, placing state across memory and disk with workload and performance models | Closely related to joint physical design, but does not model multimodal representation semantics or transformation/network split points |
| [FalconFS, NSDI 2026](https://www.usenix.org/system/files/conference/nsdi26/nsdi26spring_xu_prepub.pdf) | Builds a production file system for massive deep-learning datasets with many small multimodal files and heavy metadata traffic | Supplies scalable infrastructure rather than workload-adaptive planning of derived representations |

[Teaching the Old Dog New Tricks, OSDI 2026](https://www.usenix.org/conference/osdi26/presentation/chen-luofan) also combines hotspot replication with storage-side transformations in production LLM and multimodal pipelines and is an important recent comparison point.

## 3. State of the Art

### 3.1 Access-aware distributed prefetching is mature

Training epochs, seeded shuffles, and declarative dataset specifications expose future access. NoPFS uses access order to stage data across distributed nodes, while Tectonic-Shift uses dataset intent to predict demand in production storage. These approaches are much stronger than generic LRU.

Future-access prediction should therefore be an input rather than a primary contribution. Workflow DAGs, epochs, schedules, and seeds can be converted into demand traces and reuse horizons for `M/L/E` optimization.

### 3.2 Cross-job sharing and workload-aware capacity allocation are established

Quiver demonstrates safe cache sharing across users and jobs. Blaze shows that caching, eviction, recomputation, and recovery can be evaluated together for iterative computation. Distributed caches are no longer limited to per-job local LRU.

Sharing derived representations is harder because semantically compatible jobs may have different downstream paths, and a useful decoded replica may saturate shared links or SSDs. Compatibility, expansion ratio, and transform cost must enter the global placement model.

### 3.3 Storage tiers and compute scheduling are increasingly coupled

Lobster coordinates loading, preprocessing, and caching; Blaze trades among memory, disk, and recomputation; and production systems combine replication with transform offload. It is no longer accurate to characterize the frontier as storage-only optimization.

The less explored three-way coupling is which representation to cache, where to retain its replicas, and where along the delivery path to change representation. The project must show this precise increment against strong literature and baselines.

### 3.4 Production infrastructure already handles large small-file workloads

Tectonic-Shift and FalconFS show mature approaches to metadata scalability, flash absorption, and parallel file access. The first prototype should implement a control layer above an existing object store or file system rather than rebuild a complete storage substrate.

## 4. Designs to Reuse

1. **NoPFS-style access foresight.** Derive future demand from workflow DAGs, epochs, schedules, and seeds, falling back to probabilistic forecasts or online observations when access is uncertain.
2. **Quiver-style global content identity.** Location and job ID must not define identity; only compatible representations may be shared across jobs.
3. **Tectonic-Shift-style intent interfaces.** Consumers declare datasets, deadlines or reuse horizons, and required representations instead of forcing lower layers to infer all intent from block access.
4. **Blaze-style unified benefit models.** Evaluate retention, eviction, recomputation, and recovery together rather than applying an independent policy per tier.
5. **Tiered but non-inclusive caching.** DRAM, SSD, and remote replicas need not hold the same objects; distinct tiers may store different representations.
6. **Separate replica feasibility from capacity allocation.** Generate legal candidate locations first, then allocate capacity under multi-job budgets and fairness constraints.

## 5. Remaining Research Gaps

### 5.1 From a sample object to a representation graph

Most cache and prefetch systems assume that the requested object is already determined. A video may instead be delivered as MP4 or decoded and sampled before frame delivery. The placement object itself therefore depends on the transform plan, extending cache-key selection into a physical-design problem.

### 5.2 Global coordination of distributed derived representations

Existing systems globally coordinate raw samples or allow nodes to follow similar local cache plans. There remains room for one planner to decide which versioned representations each node and tier should retain, including shared and job-private replicas.

### 5.3 Full amortization of replica generation and migration

Replicating a raw object mainly incurs transfer cost; replicating a decoded or tensor representation also incurs transformation and larger capacity costs. Benefits must be evaluated over the full reuse horizon, including creation, migration, and cleanup, rather than only warm-cache throughput.

### 5.4 Placement and transform boundaries interact nontrivially

“Move computation to data” is not universally correct. Decoding expands bytes, filtering contracts them, GPU hosts may have spare CPUs, and storage-side CPUs may become shared bottlenecks. A valid delivery plan must compare bytes before and after each transform, CPU queues, link contention, and reuse value.

## 6. Implication for This Project

D3 has already addressed distributed prefetching, cross-job sharing, tiered caching, and some storage-compute coordination. Baselines must include access-aware designs such as NoPFS, unified cache strategies such as Blaze, and production-style hotspot replication.

The plausible research increment is **representation-aware placement**: the planner places a transformable, versioned, conditionally reusable representation graph rather than fixed samples, and solves placement together with transformation split points. Its value must be demonstrated under data expansion or contraction, resource contention, and cross-job reuse.
