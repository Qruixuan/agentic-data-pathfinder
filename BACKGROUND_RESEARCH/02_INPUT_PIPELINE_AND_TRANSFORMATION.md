# Subdirection D2: ML Input Pipelines and Transformation Optimization

## 1. Scope

Before training or offline inference, a system commonly performs reads, decompression, decoding, sampling, augmentation, tokenization, batching, and device transfer. This subdirection studies how to detect input-pipeline bottlenecks and improve accelerator utilization through parallelism, caching, operator reordering, remote execution, and elastic capacity.

This area is especially close to the project. Prior systems already cover automatic diagnosis, remote workers, intermediate-result caching, and storage-side offload, so none of these mechanisms alone is a sufficient novelty claim.

## 2. Representative Work

| Work | Main Decision | What It Achieves | Remaining Boundary |
|---|---|---|---|
| [Plumber, MLSys 2022](https://proceedings.mlsys.org/paper_files/paper/2022/hash/d0e90e9a9310570dfa643aa3b2da6e89-Abstract.html) | Parallelism, prefetch, cache, and related parameters | Uses interpretable operator performance models to diagnose and tune input pipelines | Optimizes resources within one pipeline rather than versioned derived representations across nodes |
| [The Hidden Trade-Offs of Preprocessing, SIGMOD 2022](https://arxiv.org/abs/2202.08679) | Preprocessing strategy | Characterizes performance and resource tradeoffs among online, offline, and partially materialized preprocessing | Compares strategies primarily at job scope rather than providing a distributed joint planner |
| [Cachew, ATC 2022](https://www.usenix.org/conference/atc22/presentation/graur) | Worker elasticity and cross-job caching | Provides managed input processing with elastic workers and reuse within and across jobs, including hints for nondeterminism | Centers on a cache service rather than jointly planning representation topology and transform boundaries |
| [FastFlow, PVLDB 2023](https://www.vldb.org/pvldb/vol16/p1086-um.pdf) | Whether, where, and how much of a pipeline to offload | Offloads part of image input processing to remote CPUs while using both local and remote resources | Focuses on current-run remote scale-out rather than cross-run materialization and placement |
| [Pecan, ATC 2024](https://www.usenix.org/conference/atc24/presentation/graur) | Transform order and local-versus-remote worker placement | Automatically chooses transform order and whether input workers should be colocated with accelerators | Leaves joint optimization of order, split point, and placement as future work, close to this project's execution-edge problem |
| [SOPHON, HotStorage 2024](https://research.ibm.com/publications/a-selective-preprocessing-offloading-framework-for-reducing-data-traffic-in-dl-training) | Per-sample and per-operator storage-side offload | Selectively pushes image preprocessing near storage to reduce traffic and execution time | Does not manage long-lived materialization, cross-node replication, or multi-job coordination |
| [HyCache, ATC 2025](https://www.usenix.org/conference/atc25/presentation/jha) | Which stage to cache and whether to use DRAM or SSD | Profiles multistage preprocessing and optimizes intermediate-stage and cache-medium choices | Primarily targets a single node; the distributed discussion does not globally coordinate per-node plans |
| [Seneca, FAST 2026](https://www.usenix.org/conference/fast26/presentation/desai) | Cache allocation among encoded, decoded, and augmented representations | Allocates cache among intermediate representations with a performance model and uses concurrent jobs for opportunistic sampling | Assumes a relatively fixed representation set and execution structure rather than searching transform locations and cross-node replicas |
| [Teaching the Old Dog New Tricks, OSDI 2026](https://www.usenix.org/conference/osdi26/presentation/chen-luofan) | Hot-file replication, transform offload, and checkpoint replication | Predictively replicates hotspots and offloads transformations to storage CPUs in production LLM and multimodal pipelines | Demonstrates the value of combined mechanisms for a particular architecture but does not expose a general `M/L/E` physical-planning abstraction |

## 3. State of the Art

### 3.1 Automatic bottleneck diagnosis is mature

Plumber demonstrates that operator throughput, CPU utilization, and dataflow models can locate read, map, batch, and prefetch bottlenecks. FastFlow, SOPHON, HyCache, and Seneca further show that short profiling runs can support many structured decisions.

This project can therefore adopt static workflow information, operator microbenchmarks, and short online profiles. A general profiler is infrastructure, not the central contribution. The new question is how to compare complete plans composed of representation, location, and transformation-boundary choices.

### 3.2 Remote preprocessing and near-storage offload are established

FastFlow extends CPU-intensive processing to remote workers; Pecan studies worker placement relative to accelerators; SOPHON chooses near-storage offload at sample and operator granularity; and production systems show that storage CPUs can execute transformations at scale.

Thus, moving decode from a GPU host to storage is not a sufficient contribution. The remaining questions are whether the resulting representation should persist, where it should reside, which later jobs should share it, and whether transfer remains worthwhile after data expansion.

### 3.3 Multistage caching is already close to physical design

HyCache chooses among preprocessing stages and allocates DRAM and SSD capacity. Seneca jointly considers encoded, decoded, and augmented forms. This directly covers part of the project's materialization variable `M`, so “select which intermediate representation to cache” cannot stand alone as the contribution.

The remaining space is to:

- extend local or homogeneous caching to globally coordinated distributed replicas;
- generalize a small fixed set of stages into a versioned representation DAG;
- solve transformation split and delivery path together with cache planning; and
- include first materialization, migration, cleanup, and plan-switching costs within the reuse horizon.

### 3.4 Transform order, placement, and split point are coupled

Pecan separately studies automatic ordering and placement and identifies their joint treatment with split points as future work. Reimplementing its ordering or worker placement would overlap heavily. Binding split points to materialization and replica choices could instead form a more complete research problem.

## 4. Designs to Reuse

1. **Plumber-style structured profiles.** Measure service time, scaling curves, input/output byte ratios, and resource occupancy per operator, not only end-to-end throughput.
2. **FastFlow/Pecan-style placement feasibility.** Filter impossible placements using local CPU headroom, remote-worker capacity, and network bandwidth.
3. **SOPHON-style fine-grained offload.** Permit operator- or partition-level choices, while using partition granularity in the first prototype to keep the search space manageable.
4. **HyCache/Seneca-style stage-benefit models.** Compare saved upstream computation with additional capacity and read costs, then calibrate with measurements.
5. **Cachew-style nondeterminism hints.** Make random operators, seeds, and reuse scope explicit planning constraints.
6. **Progressive deployment.** Begin with shadow profiling and plan recommendations, then use small canaries, rollback, and gradual expansion.

## 5. Remaining Research Gaps

### 5.1 Globally coordinated multi-representation, multi-node planning

Strong input systems usually couple multiple stages with cache tiers, or operators with execution locations. Few simultaneously manage representation replicas and transformation paths across many nodes. A locally optimal plan for one job may exhaust capacity or network bandwidth needed by another.

### 5.2 Cross-run reuse horizons and switching costs

Many evaluations start from a warm cache or an already deployed worker pool. This project must account for initial decode, writes, replication, migration, and cleanup over a known or predicted reuse window so that expensive preparation is not hidden outside the experimental window.

### 5.3 Semantic versions as physical-planning constraints

Cachew addresses nondeterminism hints and data systems record versions, but a unified compatibility rule across model versions, preprocessing code, parameters, and random seeds is not yet central to distributed input planning. Without it, cross-job reuse can be incorrect or overly conservative.

### 5.4 Composing mechanisms is not joint optimization

A production system may independently enable caching, replication, and offload, yet still create conflicts: a large offloaded representation may immediately cross the network, be replicated, and then be evicted for lack of capacity. Joint planning must be compared with strong compositions of existing mechanisms, not only with a default framework loader.

## 6. Implication for This Project

D2 already covers many single-mechanism and pairwise optimizations. The
greatest overlap risk comes from HyCache and Seneca on multistage caching, and
from Pecan and SOPHON on transformation placement.

Pathfinder should reuse these execution mechanisms and add the access-facing
effect they usually omit: the chosen transformation boundary changes the
price, latency, and quality offered to an agent task. A causal pilot must show
that this change alters access or completion before the project claims that
`W(D)` requires new planning machinery.
