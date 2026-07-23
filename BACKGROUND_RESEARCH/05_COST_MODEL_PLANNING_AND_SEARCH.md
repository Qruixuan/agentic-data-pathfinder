# Subdirection D5: Cost Models, Physical Planning, and Search

## 1. Scope

This subdirection represents system choices as physical plans, estimates their time and resource costs, and selects executable plans under capacity, correctness, and service constraints. Techniques include dynamic programming, ILP, heuristic search, Bayesian optimization, reinforcement learning, and learned performance models.

This project is not a generic knob tuner. It searches a structured plan `P = (M, L, E)`: a representation DAG constrains materialization `M`; capacity and topology constrain locations `L`; and operator dependencies and compatibility constrain execution paths `E`. Preserving this structure is the principal distinction from black-box configuration tuning.

## 2. Representative Work

| Work | Planning Capability | Implication and Boundary |
|---|---|---|
| [KeystoneML, ICDE 2017](https://shivaram.org/publications/keystoneml-icde17.pdf) | Selects ML pipeline physical operators and intermediate materializations using end-to-end computation and communication costs | Proves database-style physical planning for ML workflows; this project must add distributed, versioned representation replicas and continuous adaptation |
| [JellyBean, PVLDB 2023](https://users.cs.duke.edu/~ml579/papers/jellybean_vldb23.pdf) | Selects model variants and worker allocations across edge, local, and cloud resources under compute, network, cost, and accuracy constraints | Provides heterogeneous operator/model placement without long-lived materialization and cross-job caching |
| [HyCache, ATC 2025](https://www.usenix.org/conference/atc25/presentation/jha) | Profiles preprocessing stages and optimizes the cached stage plus DRAM/SSD tier | Strong precedent for `M` and part of `L`, but primarily local and without a global transformation path |
| [Seneca, FAST 2026](https://www.usenix.org/conference/fast26/presentation/desai) | Uses performance models and online samples to allocate cache among encoded, decoded, and augmented forms | Close to representation-aware caching, but with a more fixed topology and execution structure |
| [Blaze, EuroSys 2024](https://doi.org/10.1145/3627703.3629558) | Unifies cache, eviction, memory/disk placement, and recovery for iterative computation | Shows the benefit of unified planning, but targets Spark state rather than multimodal versions and transformation boundaries |
| [Compression-Aware Physical Database Design, PVLDB 2011](https://www.vldb.org/pvldb/vol4/p657-kimura.pdf) | Jointly selects indexes and compression and shows that staged decisions can be suboptimal | Direct precedent for interacting physical-design choices; this project must demonstrate a different object and mechanism for `M/L/E` coupling |
| [Online Physical Design Tuning, ICDE 2007](https://www.microsoft.com/en-us/research/publication/an-online-approach-to-physical-design-tuning/) | Continuously monitors workloads, creates or drops indexes, and balances future benefit, creation cost, and anti-thrashing safeguards | Establishes continuous physical design, transition costs, and do-no-harm principles |
| [Budget-Conscious Fine-Grained Configuration, PVLDB 2022](https://www.vldb.org/pvldb/vol15/p4079-richly.pdf) | Jointly optimizes compression, sorting, indexing, and tiering with reconfiguration cost and workload robustness | Strong multidimensional physical-design precedent for relational partitions rather than distributed representation paths |
| [Automatic Indexing in Oracle, PVLDB 2025](https://www.vldb.org/pvldb/vol18/p4924-chakkappen.pdf) | Automates candidate discovery, isolation, validation, creation, deployment, regression protection, and reclamation | Shows that production-safe validation, incremental deployment, and accountability are mature |
| [UDO, PVLDB 2021](https://www.vldb.org/pvldb/vol14/p3402-wang.pdf) | Distinguishes expensive and cheap database parameters and orders trials to amortize expensive transitions | Directly motivates experiment scheduling for materialization and migration, but searches a DBMS configuration space |
| [LlamaTune, PVLDB 2022](https://www.vldb.org/pvldb/vol15/p2953-kanellis.pdf) | Uses domain knowledge and search-space reduction to improve DBMS tuning sample efficiency | Reinforces the value of structural priors; this project should prune with a representation DAG before applying a generic tuner |

## 3. State of the Art

### 3.1 Database-style physical planning for ML pipelines already exists

KeystoneML jointly optimizes physical operators, computation, communication, and intermediate materialization. JellyBean maps ML operators and model variants onto heterogeneous locations. “The first cost-based optimizer for an ML dataflow” is therefore not a defensible claim.

The new abstraction must lead to different plans: multiple physical representations persist across jobs; transformations change network bytes; and one representation may have replicas across tiers and nodes. If these properties do not change optimal choices in evaluation, the problem collapses to established workflow placement.

### 3.2 Joint physical design and transition-aware tuning have strong prior art

HyCache, Seneca, and Blaze show that performance models combined with constrained combinatorial optimization are viable when candidates and capacities are measurable. Relational physical-design work further shows that compression, indexing, sorting, and tiering can be solved jointly and that decoupled selection can perform poorly.

Budget-Conscious Physical Design includes reconfiguration cost. Online Physical Design balances creation cost, future benefit, and thrashing. Thus, neither “jointly optimize multiple physical decisions” nor “include transition cost” is a standalone contribution. The project must demonstrate a genuinely different state space in which transform placement determines the materialized object, representations are reusable across jobs, and replicas span nodes.

### 3.3 Generic black-box tuning is crowded and poorly matched to structured plans

UDO, LlamaTune, and many system tuners optimize numerical and categorical settings. Flattening `M/L/E` into booleans and enums for Bayesian optimization or RL would produce:

- many illegal or semantically incorrect combinations;
- redundant encodings of the same logical path;
- poor transfer to new DAG sizes or cluster topologies;
- weak treatment of one-time materialization and migration effects; and
- little explanation of why joint plans outperform local policies.

Generic tuners are valuable equal-budget baselines, but should not be the core architecture.

### 3.4 Reconfiguration costs and trial order should be inherited

UDO orders expensive configuration trials so related candidates share transition work. Online Physical Design and Oracle Automatic Indexing provide direct precedents for continuous monitoring, candidate isolation, protected deployment, and anti-thrashing. The objective must compare not only steady-state performance but also the cost of moving from the current state into a candidate plan.

## 4. Recommended Planning Framework

### 4.1 Layered search

```text
semantic validation
  -> enumerate compatible representation and transform paths
  -> enumerate feasible locations and replica counts
  -> rank with analytical models and constrained optimization
  -> run a small number of trials for uncertain close candidates
  -> deploy after accounting for transition cost
```

The layers are:

1. **Legality:** lineage, determinism, versions, quality, and operator dependencies.
2. **Candidate generation:** remove dominated representations and locations.
3. **Static planning:** estimate CPU, storage, network, and reuse benefit under budgets.
4. **Empirical correction:** measure only candidates whose rankings are both consequential and uncertain.
5. **Deployment:** evaluate `transition_cost(P_old -> P_new)` and support gradual switching and rollback.

### 4.2 Interpretable cost decomposition

For reuse horizon `H`:

```text
Total(P, H) = Transition(P_old, P)
            + Materialize(P, H)
            + Read(P, H)
            + Transform(P, H)
            + Transfer(P, H)
            + QueueingCorrection(P, observed_state)
```

Sizes, operator service times, and path bandwidths come from representation metadata and profiles. Trace-guided corrections handle shared-resource queues, skew, and cache interference. The first prototype should minimize total completion time over the horizon while treating monetary cost and capacity as constraints, avoiding premature multi-objective weighting.

### 4.3 Separate decision timescales

- **Slow:** large representation materialization, cross-node migration, and replica topology.
- **Medium:** transform-worker placement and task-to-replica mapping.
- **Fast:** prefetch, parallelism, batch, and chunk parameters.

The first paper should focus on slow and medium decisions while delegating fast knobs to existing loaders or local controllers. This keeps the search space manageable and follows UDO's distinction between expensive and cheap changes.

## 5. Remaining Research Gaps

### 5.1 Unified versioned representation, location, and transform-path planning

Prior work covers many pairwise combinations. In the reviewed literature, a planner that simultaneously treats a versioned representation DAG as the physical-design object, globally chooses cross-node and cross-tier replicas, and selects transformation or delivery split points remains a plausible gap. This is a literature-based assessment, not yet a formal “first” claim.

### 5.2 Coupling created by data expansion and contraction

Ordinary placement models count communication and ordinary caches count object size. Here, a cache choice changes the representation that crosses the next link, and one upstream materialization can serve multiple downstream paths. The cost model and experiments must explicitly expose this interaction rather than merely adding more decision variables.

### 5.3 Artifact-aware transition graphs

Transition-aware planning itself is established. A more specific possible increment is that experiments create versioned representation shards; a losing candidate may leave a legal parent representation that a later plan can adopt; and replication, transformation, and location changes can share part of a transition.

The system should model generation, replication, migration, warm-up, adoption, invalidation, and cleanup in one artifact-aware transition graph, and compare this with assigning each configuration switch an independent scalar cost.

### 5.4 Plan-difference-directed targeted experiments

Analytical models struggle with shared links and CPU queues, while pure black-box trials are expensive. Active sampling, optimal design, and Bayesian optimization are already established. The narrower opportunity is to derive uncertain terms from differences among candidate `M/L/E` plans, choose a component, materialization, or canary experiment that resolves their ranking, and account for reusable artifacts created by that experiment.

This approach should be compared at equal budget with analytical-only planning, plan-level Bayesian optimization, generic optimal design, and UDO-style ordering.

## 6. Implication for This Project

The most suitable technical path is structured, database-style physical planning with limited empirical calibration, not an undifferentiated “AI tunes the system” story. KeystoneML, JellyBean, HyCache, and Blaze are close data-path precedents; relational physical-design and tuning work closes off broad claims about joint design, switching cost, and continuous deployment.

The planner is research-worthy only if representation graphs and `M/L/E` coupling systematically make local methods choose the wrong plan. The contribution should center on cross-node representation-transformation paths and reusable transition artifacts, not on relabeling the established autonomous physical-design lifecycle.
