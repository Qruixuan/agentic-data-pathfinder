# Updated Synthesis: Physical Planning and Structured Autoresearch

## 1. Why This Update Was Needed

The direction now contains two connected problems:

1. **Physical planning:** jointly select materialization `M`, cross-node and cross-tier locations `L`, and transformation or delivery execution `E` for versioned multimodal representations.
2. **Structured Autoresearch:** when existing evidence cannot reliably rank candidate plans, formulate a structured hypothesis, choose a decision-relevant experiment, update evidence, and then deploy, continue, refuse, or stop.

The physical system has converged on a central controller plus per-node Data Agents. Immutable representation shards are the optimization unit, and child plan epochs isolate canary execution.

The updated survey specifically examined active experimentation, continuous physical design, transition costs, candidate isolation, rollback, and stopping. Most broad capabilities already have mature precedents, so the novelty boundary must be narrower.

## 2. Conclusions First

The representative literature reviewed through July 2026 supports four conclusions.

### 2.1 A physical-planning gap may remain, but it must be stated precisely

Existing systems cover:

- physical operators and intermediate materialization in ML workflows;
- multistage representation caching across DRAM and SSD;
- local, remote, and storage-near transform placement;
- distributed sample caching, prefetching, and replication;
- joint database design across compression, indexing, sorting, and tiering; and
- online physical design with reconfiguration costs.

The surveyed systems do not appear to simultaneously use a **versioned multimodal representation DAG** to globally select:

```text
M: materialized representation shards
L: cross-node and cross-tier replicas
E: transform executors and delivery boundaries
```

The plausible distinction is not “the first joint physical design” or “the first transition-aware optimizer.” It is that representation transformations change both network bytes and the set of materializable downstream objects; a parent may serve several jobs; and replicas of the same representation may span nodes. These properties create a distributed transformation-path coupling different from index/compression design.

This is a gap assessment based on the current literature sample, not yet a defensible “the first” claim.

### 2.2 General Autoresearch capabilities have strong precedents

[iTuned](https://www.vldb.org/pvldb/vol2/vldb09-193.pdf) plans online experiments; [Ernest](https://pages.cs.wisc.edu/~shivaram/publications/ernest-nsdi.pdf) uses optimal experiment design; [CherryPick](https://www.usenix.org/conference/nsdi17/technical-sessions/presentation/alipourfard/) uses Bayesian optimization; [MLOS](https://www.vldb.org/pvldb/vol17/p4269-kroth.pdf) supplies experiment infrastructure; [KEA](https://arxiv.org/abs/2106.11445) combines observation with production flighting; and [OnlineTune](https://arxiv.org/abs/2203.14473), [SelfTune](https://www.usenix.org/conference/nsdi23/presentation/karthikeyan), and [OPPerTune](https://www.usenix.org/conference/nsdi24/presentation/somashekar) cover dynamic, safe, low-disruption tuning.

Automatically selecting an experiment, running a canary, managing evidence, stopping, or rolling back cannot be independent novelty claims.

### 2.3 Continuous physical design and transition costs are also established

[Online Physical Design Tuning](https://www.microsoft.com/en-us/research/publication/an-online-approach-to-physical-design-tuning/) balances future benefit, creation cost, and thrashing. [To Tune or Not to Tune](https://www.microsoft.com/en-us/research/publication/to-tune-or-not-to-tune-a-lightweight-physical-design-alerter/) decides whether expensive tuning is worthwhile. [UDO](https://www.vldb.org/pvldb/vol14/p3402-wang.pdf) orders heavy and light trials to reuse expensive structures. [Automatic Indexing in Oracle](https://www.vldb.org/pvldb/vol18/p4924-chakkappen.pdf) implements isolation, validation, incremental deployment, regression protection, and accountability.

The transition contribution must therefore concern a partially reusable state graph of representation artifacts, not a generic scalar `transition_cost(P_old, P_new)`.

### 2.4 The most plausible intersection gap is also the highest risk

> In a versioned `M/L/E` plan space, the system uses plan differences and the representation DAG to select a component, sample, or canary experiment capable of changing the plan ranking. A valid representation shard produced by a trial may become a reusable physical artifact for a later plan, so experiment selection jointly values information, artifact reuse, disruption, and non-reusable transition cost.

This claim must outperform both physical-design and automated-experiment baselines, not only LRU or random search.

## 3. Physical-Planning Capability Matrix

Legend: **Primary** means a core capability, **Partial** means limited coverage or a different data object, and **No** means it is not a primary decision.

| Work | `M` Materialization | `L` Tier/Node | `E` Transform/Delivery | Version/Semantic Reuse | Transition/Reconfiguration | Distributed Multi-job |
|---|---|---|---|---|---|---|
| [KeystoneML](https://shivaram.org/publications/keystoneml-icde17.pdf) | Primary | Partial | Primary | Partial | Partial | Partial |
| [Pecan](https://www.usenix.org/conference/atc24/presentation/graur) | No | Primary | Primary | No | No | Partial |
| [HyCache](https://www.usenix.org/conference/atc25/presentation/jha) | Primary | Partial | Partial | Partial | Partial | Partial |
| [Seneca](https://www.usenix.org/conference/fast26/presentation/desai) | Primary | Partial | Partial | Partial | Partial | Primary |
| [NoPFS](https://arxiv.org/abs/2101.08734) | No | Primary | Partial | Partial | No | Primary |
| [Blaze](https://doi.org/10.1145/3627703.3629558) | Primary | Primary | Partial | Partial | Primary | Primary |
| [JellyBean](https://users.cs.duke.edu/~ml579/papers/jellybean_vldb23.pdf) | No | Primary | Primary | No | No | Partial |
| [Nectar](https://www.microsoft.com/en-us/research/publication/nectar-automatic-management-of-data-and-computation-in-data-centers/) / [HELIX](https://www.vldb.org/pvldb/vol12/p446-xin.pdf) | Primary | Partial | Partial | Primary | Partial | Partial |
| [Compression-Aware Physical Design](https://www.vldb.org/pvldb/vol4/p657-kimura.pdf) | Primary | Partial | No | No | Partial | No |
| [Budget-Conscious Physical Design](https://www.vldb.org/pvldb/vol15/p4079-richly.pdf) | Primary | Primary | No | No | Primary | No |
| [Online Physical Design](https://www.microsoft.com/en-us/research/publication/an-online-approach-to-physical-design-tuning/) | Primary | No | No | No | Primary | Partial |
| [Oracle Automatic Indexing](https://www.vldb.org/pvldb/vol18/p4924-chakkappen.pdf) | Primary | No | No | Partial | Primary | Primary |
| **Proposed project** | **Primary** | **Primary** | **Primary** | **Primary** | **Primary** | **Primary** |

The matrix does not imply that prior systems optimize only one layer. It shows that strong pairwise combinations and joint physical designs already exist. An interaction study must demonstrate that representation-transform/network coupling causes strong independent or sequential optimizers to choose systematically inferior plans.

## 4. Autoresearch Capability Matrix

| Work | Active Experiment Selection | Structural Priors | Online Safety | Transition/Trial Ordering | Persistent Artifact Reuse | Semantic Plan Validation |
|---|---|---|---|---|---|---|
| [iTuned](https://www.vldb.org/pvldb/vol2/vldb09-193.pdf) | Primary | Partial | Primary | Partial | No | No |
| [Ernest](https://pages.cs.wisc.edu/~shivaram/publications/ernest-nsdi.pdf) | Primary | Primary | No | No | No | No |
| [CherryPick](https://www.usenix.org/conference/nsdi17/technical-sessions/presentation/alipourfard/) | Primary | Partial | No | Partial | No | No |
| [MLOS](https://www.vldb.org/pvldb/vol17/p4269-kroth.pdf) | Partial | Partial | Partial | Partial | No | No |
| [KEA](https://arxiv.org/abs/2106.11445) | Partial | Partial | Primary | Partial | No | Partial |
| [UDO](https://www.vldb.org/pvldb/vol14/p3402-wang.pdf) | Primary | Primary | No | Primary | Partial | Partial |
| [OnlineTune](https://arxiv.org/abs/2203.14473) | Primary | Primary | Primary | Partial | No | Partial |
| [SelfTune](https://www.usenix.org/conference/nsdi23/presentation/karthikeyan) | Primary | Partial | Primary | Partial | No | Partial |
| [OPPerTune](https://www.usenix.org/conference/nsdi24/presentation/somashekar) | Primary | Primary | Primary | Partial | No | Partial |
| [Oracle Automatic Indexing](https://www.vldb.org/pvldb/vol18/p4924-chakkappen.pdf) | Primary | Primary | Primary | Primary | Partial | Primary |
| [Seneca](https://www.usenix.org/conference/fast26/presentation/desai) | Partial | Primary | Primary | Partial | Partial | Partial |
| **Proposed project** | **Primary** | **Primary** | **Primary** | **Primary** | **Primary** | **Primary** |

Having all columns is not itself novel. The distinction must reside in the controlled object and experiment structure: versioned multimodal shards, cross-node replicas, transform/network boundaries, and partial adoption of artifacts produced by losing candidates.

## 5. Mature Capabilities to Reuse

- tensor and columnar multimodal formats with versions and random/sequential access;
- operator profiling, parallelism, prefetching, and remote offload;
- multistage cache selection among raw, decoded, and augmented forms;
- caching, prefetching, and replica management across DRAM, NVMe, shared storage, and peers;
- workflow intermediate identity, lineage, equivalence, and invalidation;
- cost-based operator, materialization, and placement planning;
- joint compression, indexing, sorting, and tiering design;
- continuous physical design, creation-cost accounting, and anti-thrashing;
- adaptive sampling, optimal experiment design, and Bayesian or RL tuning;
- experiment infrastructure, provenance, and production flighting; and
- candidate isolation, canaries, regression protection, and rollback.

These are mechanism sources, infrastructure patterns, and strong baselines, not standalone contributions.

The controller plus per-node Data Agent architecture is consequently a substrate:

- the catalog and plan registry implement identity, plan epochs, and replica validity;
- Data Agents execute transform, replicate, serve, pin, evict, and telemetry operations;
- child plan epochs isolate candidates;
- origin-read and parent-plan fallback provide safe recovery; and
- the Evidence Store records interventions, traces, artifact adoption, and decisions.

Research value comes from executing `M/L/E` interventions that existing abstractions cannot express and exposing artifact-aware experiment state.

## 6. Updated Research Gaps

### G1. Distributed representation-path physical planning — core

Jointly select:

```text
M: which representation shards exist and for how long
L: which nodes and tiers hold their replicas
E: which parent is served, where transforms run, and where bytes cross links
```

The mechanism differs from established joint physical design because:

- transformations may first expand and then contract data;
- `M` changes which representation and how many bytes cross the network;
- `E` changes which nodes become reusable materialization points;
- `L` changes transform resources and cross-job reuse value; and
- a common parent may branch to several consumers or versions.

**Required counterfactual:** global `M/L/E` planning must significantly outperform a strong independent or sequential optimizer using the same mechanisms in at least one reproducible regime.

### G2. Artifact-aware transition graph — planner/Autoresearch bridge

Represent transitions as a partially shared artifact DAG:

```text
trial or plan operation
  -> creates or moves a representation shard
  -> the shard may be adopted by several later plans
  -> later transition cost depends on current valid artifacts and leases
```

The planner must distinguish reusable, stranded, invalid, and disruptive work while deciding which artifacts from failed candidates to retain, whether to copy or recompute, how adjacent experiments share shards, and which common ancestors survive version churn.

**Required counterfactual:** artifact-aware planning must beat both a scalar switch-cost model and UDO-style grouping based only on heavy configurations.

### G3. Plan-difference-directed Autoresearch — high-risk candidate core

Compare legal plans, identify unknown model terms that determine their ranking, and choose among:

- operator microbenchmarks;
- transfer or storage benchmarks;
- sampled representation materialization; and
- child-plan workload canaries.

```text
experiment_value =
  expected decision improvement
  + expected reusable-artifact value
  - measurement cost
  - foreground disruption
  - non-reusable transition cost
```

This extends iTuned, Ernest, Bayesian optimization, and UDO to a new plan structure. It is a contribution only if it outperforms plan-level Bayesian optimization, generic optimal design, random component sampling, and UDO-style ordering under equal budgets.

### G4. Multi-job, multi-version representation coordination — core regime

Determine which parent representations are shared globally, which final forms are job- or node-private, whether a candidate harms other jobs, what survives version updates, and how to separate external contention from candidate performance.

G4 should expose G1–G3 interactions, not expand into a general fairness scheduler.

### G5. Unified compatibility contract — correctness substrate

Input, code, parameters, randomness, model, tokenizer, and quality versions should consistently drive:

- representation IDs;
- cache and replica keys;
- invalidation;
- child-plan legality; and
- trial-artifact adoption.

Lineage and equivalence are mature topics, so G5 is essential infrastructure rather than a headline contribution.

### G6. Interaction and experiment-value evaluation — evidence

Run a factorial interaction study varying:

- expansion and contraction ratios;
- transform CPU intensity;
- network and storage bandwidth;
- NVMe and RAM capacity;
- reuse horizon and version churn; and
- job concurrency and skew.

Then evaluate Autoresearch using plan quality versus cumulative experiment cost, artifact-adoption ratio, stranded and invalid bytes, foreground disruption, stopping/refusal accuracy, and adaptation regret.

## 7. Priorities

| Priority | Item | Role |
|---|---|---|
| P0 | G1 `M/L/E` interaction | Core physical-planning question |
| P0 | G6/G0 interaction study | Go/no-go gate before building a planner |
| P1 | G2 artifact-aware transition graph | Distinguishes the work from generic transition-aware design |
| P1 | G4 multi-job and multi-version regime | Exposes sharing and distributed coupling |
| P1 | G5 semantic contract | Correctness for planning and artifact adoption |
| P1/P2 | G3 Structured Autoresearch | Pursue only after G1/G2 hold and analytical models remain insufficient |
| P2 | Online drift adaptation | Extension after the core result is established |

Recommended implementation order:

```text
static A/B/C paths
  -> G0 interaction study
  -> analytical joint planner
  -> child-plan experiment manager
  -> plan-difference experiment selection
  -> drift adaptation
```

If the analytical planner reliably selects the correct plan, G3 does not hold and a learned autonomous loop should not be forced into the system.

## 8. Required Strong Baselines

### 8.1 Physical data-path baselines

1. origin streaming;
2. local caching and prefetching;
3. HyCache/Seneca-style multistage tier caching;
4. NoPFS/Tectonic-Shift-style access-aware placement;
5. Pecan/FastFlow/SOPHON-style transform offload;
6. KeystoneML/Blaze/Budget-Conscious-style structured cost-based planning;
7. independent `M`, `L`, and `E` optimization in fixed orders using the same primitives; and
8. exhaustive search over a reduced plan space.

### 8.2 Autoresearch baselines

1. analytical-only planning;
2. passive traces;
3. random plan trials;
4. a CherryPick/Bayesian plan-level tuner;
5. Ernest-style component optimal design without plan differences;
6. UDO-style heavy/light ordering;
7. operator traces with random experiment selection;
8. full plan-difference and artifact-aware Autoresearch; and
9. an oracle with all experiment outcomes on a reduced instance.

Trial count alone is insufficient. All methods must begin from the same current plan, evidence, and materialized artifacts, and use the same legal space, budget, workload slice, adoption rules, and foreground contention.

## 9. Minimum Research Slice

- **Workload:** a recurring batch video pipeline plus one image, audio, or embedding pipeline with different expansion behavior.
- **Representations:** raw compressed data, a decoded or sampled parent, and a final tensor or embedding.
- **Unit:** immutable representation shard.
- **Tiers:** durable object storage, node NVMe, and host RAM.
- **Executors:** storage-near or data-node CPU and consumer-local CPU.
- **Topology:** one central controller plus per-node Data Agents.
- **Sharing:** at least two consumers or jobs with partially overlapping representation paths.
- **Plans:** fixed A/B/C paths, a strong independent planner, and a global `M/L/E` planner.
- **Autoresearch:** add only after the interaction gate, starting with component, materialization-sample, and canary experiments.

## 10. Updated Narrative

The defensible thesis is:

> Existing formats, caches, workflow systems, physical-design tools, and automated tuners each solve substantial parts of the problem. This project studies distributed physical planning over versioned multimodal representation paths, where materialization, cross-node placement, and transformation boundaries interact through data expansion, contraction, and cross-job reuse. When analytical evidence cannot rank close plans, structured Autoresearch selects plan-difference-directed experiments and treats valid artifacts produced by those experiments as reusable physical state.

The project should not claim to be the first joint physical optimizer, the first transition-aware system, the first automatic experiment manager, or the first safe canary tuner. Its viability depends on demonstrating the narrower `M/L/E` interaction and artifact-aware experiment value against strong baselines.
