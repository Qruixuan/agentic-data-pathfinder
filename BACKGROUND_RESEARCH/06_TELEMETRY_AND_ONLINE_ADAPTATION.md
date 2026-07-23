# Subdirection D6: Structured Autoresearch, Automated Experiments, and Safe Adaptation

## 1. Updated Research Object

The revised direction defines Autoresearch as an evidence-acquisition loop:

```text
S_t = (W, R, G, P_t, D_t)

observe
  -> formulate a structured plan hypothesis
  -> validate
  -> select a decision-relevant experiment
  -> execute and measure
  -> update evidence
  -> deploy, continue, refuse, or stop
```

The central question is not generic self-tuning. Given a versioned representation DAG, the current `M/L/E` plan, and physical state, can the system select a low-cost experiment that changes an important planning decision? Candidate interventions include operator microbenchmarks, sampled representation materialization, transfer tests, and child-plan canaries.

Active experiment selection, automated benchmarks, safe online exploration, canaries, transition-aware trial ordering, stopping, and rollback all have strong precedents. Autoresearch can be a distinct contribution only if representation-plan structure materially lowers total experimentation cost.

## 2. Representative Work

| Work | What It Achieves | Constraint and Lesson for This Project |
|---|---|---|
| [iTuned, VLDB 2009](https://www.vldb.org/pvldb/vol2/vldb09-193.pdf) | Uses adaptive sampling to plan low-overhead database experiments and discover influential parameters and strong configurations | Active evidence acquisition and online experiment planning are not new |
| [Ernest, NSDI 2016](https://pages.cs.wisc.edu/~shivaram/publications/ernest-nsdi.pdf) | Uses workload compute/communication structure and optimal experiment design to minimize training runs for performance models | Structural, information-efficient experiment selection is established; differentiation must come from plan decisions over representation DAGs |
| [CherryPick, NSDI 2017](https://www.usenix.org/conference/nsdi17/technical-sessions/presentation/alipourfard/) | Uses Bayesian optimization to find near-optimal cloud configurations with few trials | A strong plan-level black-box baseline and evidence that learning a complete response surface is unnecessary |
| [Online Physical Design Tuning, ICDE 2007](https://www.microsoft.com/en-us/research/publication/an-online-approach-to-physical-design-tuning/) | Continuously creates and drops indexes while accounting for creation cost and suppressing oscillation | Continuous physical design, future-benefit reasoning, and do-no-harm principles are established |
| [To Tune or Not to Tune, VLDB 2006](https://www.microsoft.com/en-us/research/publication/to-tune-or-not-to-tune-a-lightweight-physical-design-alerter/) | Uses low-cost bounds to decide whether expensive tuning is worthwhile | Refusing low-value tuning is not a new system objective; it must be tied specifically to plan-ranking uncertainty |
| [UDO, PVLDB 2021](https://www.vldb.org/pvldb/vol14/p3402-wang.pdf) | Orders heavy and light parameter trials to reuse expensive structures and reduce reconfiguration overhead | Strong precedent for persistent experiment state and transition-aware ordering |
| [MLOS, DEEM 2020](https://www.microsoft.com/en-us/research/publication/mlos-an-infrastructure-for-automated-software-performance-engineering/) / [MLOS in Action, PVLDB 2024](https://www.vldb.org/pvldb/vol17/p4269-kroth.pdf) | Provides reusable infrastructure for automated benchmarks, cross-VM experiments, metrics, result management, and optimizers | Experiment managers, evidence stores, and provenance are infrastructure patterns rather than contributions |
| [KEA, 2021](https://arxiv.org/abs/2106.11445) | Combines observational tuning with cautious production flighting in large-scale data infrastructure | Passive evidence plus conservative online trials is already a production pattern |
| [OnlineTune, 2022](https://arxiv.org/abs/2203.14473) | Applies contextual Bayesian optimization and black-/white-box safety constraints to dynamic cloud databases | Workload drift, context-aware tuning, and safe subspace exploration are mature |
| [SelfTune, NSDI 2023](https://www.usenix.org/conference/nsdi23/presentation/karthikeyan) | Uses simple interfaces and online learning to tune production cluster-manager parameters | General self-tuning has been deployed at scale, though representation semantics are outside its scope |
| [OPPerTune, NSDI 2024](https://www.usenix.org/conference/nsdi24/presentation/somashekar) | Automatically selects what and at what scope to tune while reducing post-deployment disruption | Automatic tuning-scope selection and low-disruption adaptation are strong baselines |
| [Automatic Indexing in Oracle, PVLDB 2025](https://www.vldb.org/pvldb/vol18/p4924-chakkappen.pdf) | Incrementally discovers, isolates, validates, deploys, and reclaims index candidates with regression protection | Candidate isolation, safe deployment, rollback, and a complete physical-structure lifecycle are production capabilities |
| [Seneca, FAST 2026](https://www.usenix.org/conference/fast26/presentation/desai) | Opportunistically samples concurrent jobs to calibrate allocation among encoded, decoded, and augmented caches | The closest empirical-adaptation precedent for representations, though not cross-node active `M/L/E` experimentation |

## 3. State of the Art

### 3.1 Active experiment selection is not a new capability

iTuned plans samples, Ernest uses optimal experiment design, and CherryPick and many Bayesian tuners choose subsequent configurations from prior results. The project cannot claim novelty from automatically selecting the next experiment.

The more precise question is whether structural differences between valid physical plans identify a shared unknown and permit a cheaper experiment than a complete plan trial. If several candidates differ mainly in storage-near decode throughput under current concurrency, the system should measure that component instead of running every full plan.

### 3.2 Experiment infrastructure and provenance are mature

MLOS orchestrates benchmarks, collects metrics, and manages results; KEA combines observational models with production flighting. An Experiment Manager, Evidence Store, trial namespace, and versioned results are sensible architecture, but not contributions by themselves.

The project can extend a generic `(configuration, score)` record to:

```text
(workload version,
 representation DAG,
 parent/child plan epoch,
 experiment intervention,
 reusable transition artifacts,
 operator/resource traces,
 decision outcome)
```

Evaluation must show that these structured fields improve experiment selection.

### 3.3 Safe continuous physical design has complete precedents

Online Physical Design, Oracle Automatic Indexing, OnlineTune, and OPPerTune already cover continuous adaptation, candidate isolation, protected rollout, drift, and low-disruption exploration. The following should be described as adopted principles:

- child-plan or candidate isolation;
- promotion after a canary;
- fallback to the origin or parent plan;
- rollback by plan epoch;
- stopping, refusal, and anti-thrashing; and
- reopening the loop after workload drift.

### 3.4 Persistent experiment state and ordering are established

UDO orders expensive trials so related configurations share transition work. The new opportunity is narrower: a sampled-frame shard created by a losing candidate may still be a legal parent for a later plan. Experiment outcomes are both measurements and changes to physical state.

The system should distinguish:

- **reusable work:** compatible, validated artifacts that future plans will consume;
- **stranded work:** legal artifacts with no expected future demand;
- **invalid work:** artifacts that fail compatibility or validation; and
- **disruption:** cost imposed on foreground jobs without producing reusable state.

## 4. Defensible Autoresearch Boundary

### 4.1 Capabilities that cannot be claimed alone

- automatically selecting the next experiment;
- optimal experiment design, Bayesian optimization, or active learning;
- finding a good configuration with few trials;
- online tuning and drift adaptation;
- safe exploration, canaries, and rollback;
- stopping or deciding whether tuning is worthwhile;
- accounting for configuration-switching cost;
- automated benchmarking, trace stores, and experiment provenance; or
- using an LLM to generate hypotheses or summarize traces.

### 4.2 Specific research questions that may remain open

#### A. Plan-difference-directed experiment selection

The input is a set of semantically valid `M/L/E` plans, not a flat configuration. The system identifies which shared representation, operator, replica, or network-path terms can change their ranking, then selects a component benchmark, materialization sample, or child-plan canary.

This is a domain-structured extension of iTuned, Ernest, and Bayesian optimization, not an entirely new experiment-design paradigm.

#### B. Experiment as a physical-state transition

An experiment may create an adoptable immutable representation shard, NVMe replica, or validated transform output. Selection must consider both information value and the effect on later state:

```text
experiment_value =
  expected plan-decision improvement
  + expected reusable-artifact value
  - measurement cost
  - disruption
  - non-reusable transition cost
```

Comparison with UDO-style heavy-parameter scheduling must show that partial artifact reuse in the DAG leads to different ordering or measurable gains.

#### C. Semantic intervention space

Lineage, randomness, model or tokenizer version, and quality contracts first determine which experiments are legal. Learned models or LLMs may rank valid child plans but cannot bypass correctness by observing good performance.

#### D. Multi-job resource externalities

A candidate materialization consumes shared CPU, NVMe, and network resources and may evict another job's valuable representation. Telemetry must separate candidate benefit, background contention, and negative externalities imposed on other jobs.

## 5. Implication for the Physical System

A central controller with per-node Data Agents is a sensible implementation, but not an independent research contribution:

- the controller, catalog, plan registry, and evidence store form a familiar automated-experiment control plane;
- per-node agents, candidate namespaces, plan epochs, and fallback implement safe execution;
- immutable shards, fingerprints, and replica state connect D4 semantics to D3 placement; and
- child plans differ from ordinary canaries only if they support plan-difference experiments and reusable artifact adoption.

Implementation should first establish repeatable Plans A/B/C and complete the G0 interaction study. If the physical planner has no close decisions that an analytical model cannot resolve, an Autoresearch layer is unnecessary.

## 6. Required Evaluation Baselines

Compare from the same initial plan and evidence state:

1. analytical-only planning with no active experiments;
2. passive trace collection;
3. random plan trials;
4. plan-level Bayesian optimization or a CherryPick-style method;
5. optimal-design/component sampling without `M/L/E` plan differences;
6. UDO-style heavy/light trial ordering;
7. structured plan search with traces but random experiment selection;
8. full structured Autoresearch; and
9. an oracle with all trial outcomes on a reduced instance.

All methods must share the same legal plan space, measurement and disruption budget, initial artifacts, transition-cost accounting, artifact-adoption rules, workload slice, and background concurrency.

Key metrics are:

- validated plan quality versus cumulative total experiment cost;
- wall-clock time and foreground disruption to reach the target plan;
- useless, stranded, and invalid materialization bytes;
- fraction of trial artifacts adopted by later plans;
- stopping and refusal accuracy; and
- plan-ranking accuracy and consequences of wrong decisions.

## 7. Implication for This Project

Adding explicit Autoresearch makes the thesis clearer but harder. Related work already covers active sampling, optimal experiment design, automated infrastructure, safe production flighting, continuous physical design, expensive-trial scheduling, and rollback.

The most credible current statement is:

> We study structured Autoresearch for versioned multimodal `M/L/E` physical planning. The system uses plan differences and a representation DAG to select decision-relevant experiments, while treating reusable representation artifacts created by trials as part of subsequent physical state.

This remains a gap assessment, not a formal first-of-its-kind result. If plan-difference selection and artifact-aware scheduling do not beat Bayesian optimization, generic optimal design, and UDO-style baselines at equal budget, Autoresearch should become an implementation mechanism and the paper should center on representation-aware joint physical planning.
