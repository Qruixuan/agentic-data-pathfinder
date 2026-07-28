# D5. Cost Models, Physical Planning, and Search

## 1. Scope

This subdirection represents system choices as physical designs and selects
executable designs under capacity, correctness, and service constraints.
Pathfinder uses:

```text
D = (M, L, E)
```

where a representation DAG constrains materialization `M`, topology and
capacity constrain layout `L`, and operator dependencies constrain execution
and delivery `E`.

Conventional physical design normally evaluates candidates against an observed
or forecast workload. Pathfinder adds a second layer: the agentic portion of
the workload is induced by the design, `W(D)`. D5 supplies the physical state
and transition model; D8 supplies the endogenous-workload model.

## 2. Representative Work

| Work | Established capability | Boundary for Pathfinder |
|---|---|---|
| [KeystoneML, ICDE 2017](https://shivaram.org/publications/keystoneml-icde17.pdf) | Cost-based ML-pipeline operators and intermediate materialization | Does not model distributed, persistent representations that change agent access |
| [JellyBean, PVLDB 2023](https://users.cs.duke.edu/~ml579/papers/jellybean_vldb23.pdf) | Heterogeneous model/operator placement under compute, network, cost, and quality constraints | No long-lived representation DAG or endogenous task response |
| [HyCache, ATC 2025](https://www.usenix.org/conference/atc25/presentation/jha) | Chooses cached preprocessing stage and DRAM/SSD allocation | Strong representation-cache baseline; topology and demand response are narrower |
| [Seneca, FAST 2026](https://www.usenix.org/conference/fast26/presentation/desai) | Allocates cache among encoded, decoded, and augmented forms using models and samples | Close representation-aware adaptation without the same cross-node access feedback |
| [Blaze, EuroSys 2024](https://doi.org/10.1145/3627703.3629558) | Unifies cache, eviction, tier placement, and recovery for iterative computation | Targets iterative compute state rather than agent-affordable multimodal paths |
| [Compression-Aware Physical Design, PVLDB 2011](https://www.vldb.org/pvldb/vol4/p657-kimura.pdf) | Shows coupled compression/index decisions can defeat staged optimization | Strong precedent: multiple interacting physical decisions are not themselves new |
| [Budget-Conscious Physical Design, PVLDB 2022](https://www.vldb.org/pvldb/vol15/p4079-richly.pdf) | Joint compression, sorting, indexing, tiering, and reconfiguration cost | Strong transition-aware joint-design baseline for relational partitions |
| [Online Physical Design Tuning, ICDE 2007](https://www.microsoft.com/en-us/research/publication/an-online-approach-to-physical-design-tuning/) | Continuous index create/drop with benefit, creation cost, and anti-thrashing | Continuous and transition-aware physical design are established |
| [UDO, PVLDB 2021](https://www.vldb.org/pvldb/vol14/p3402-wang.pdf) | Orders expensive and cheap trials to reuse transition work | Direct baseline for stateful experiment ordering |
| [Oracle Automatic Indexing, PVLDB 2025](https://www.vldb.org/pvldb/vol18/p4924-chakkappen.pdf) | Candidate discovery, isolation, validation, deployment, regression protection, and cleanup | Production-safe physical-structure lifecycle is established |
| [DBA Bandits, ICDE 2021](https://renata.borovica-gajic.com/data/2021_icde.pdf) | Learns online index value through direct exploration with safety guarantees | Workload evolves but is not modeled as caused by representation affordability |
| [HMAB, PVLDB 2022](https://www.vldb.org/pvldb/vol16/p216-perera.pdf) | Hierarchical bandits for integrated index/materialized-view design | Strong combinatorial exploration baseline |

## 3. State of the Art

### 3.1 Joint physical design is established

Prior work already jointly selects operators, intermediate results, caches,
compression, indexes, tiering, and placement. Pathfinder cannot claim novelty
from the tuple `D=(M,L,E)` alone.

The narrower physical distinction is a distributed representation path:
transformations may expand then contract bytes, one parent can serve several
tasks or workload modes, and layout changes both transform resources and the
access profile offered to an agent.

### 3.2 Transition-aware continuous control is established

Creation cost, switching cost, trial ordering, protected rollout, rollback, and
anti-thrashing all have strong precedents. Pathfinder should reuse them.

The additional state is that a Reveal can create a representation shard that
remains useful after restoration. Transition cost is therefore a function of
the current valid artifact graph, not only a scalar assigned to a pair of
configurations.

### 3.3 Bandits and generic tuners are necessary baselines

Flattening the problem into booleans and categories risks illegal encodings and
poor transfer, but black-box, Bayesian, contextual-bandit, and combinatorial-
bandit approaches remain strong equal-budget baselines. Pathfinder needs to
show that representation/task structure improves identification or reduces
exploration cost.

### 3.4 Conventional planners assume an exogenous objective

Even adaptive physical-design systems typically optimize performance for
queries that arrive independently of the selected physical structures. They
may handle drift in the observed workload without modeling the design as the
cause of that workload.

Pathfinder's objective instead separates:

```text
Phi(D) = session value under W(D)

Gain(D_t -> D') =
  Phi(D') - Phi(D_t) - Transition(D_t, D')
```

The first term is not reliably estimable from an incumbent trace when access
to a candidate representation is censored.

## 4. Recommended Planning Decomposition

```text
semantic and governance feasibility
  -> structured candidate generation
  -> physical cost and transition model
  -> AWM lower/upper session-value bounds
  -> OED Commit / Reveal / Hold
  -> deployment, restoration, or escalation
```

The physical model should include:

- materialization and reuse;
- read, transform, transfer, and queueing cost;
- placement and capacity;
- transition, foreground disruption, and restoration;
- valid reusable state left by previous probes; and
- fixed training/analytics contributions.

The access model must expose a class-specific quoted access-price matrix
`p_qv(D)`: the same representation can be local to one consumer and remote to
another. It must also keep felt latency and realized resource cost separate
from that quote. The former may change behavior; the latter is used for
resource accounting and must not be silently substituted for the signal that
caused the access decision.

Before a run, the candidate generator must induce and freeze the executable
price-level universe:

```text
P_qv = { p_qv(D) : D in D_gov and p_qv(D) is within the access gate }
```

This is a property of the declared design domain, not a list reconstructed
from prices encountered during exploration.

## 5. Remaining Gaps

### G5.1 Performative representation-path planning

Jointly choose `M/L/E` when the design changes agent access and hence the
workload used to value the design.

### G5.2 Coupled physical and behavioral uncertainty

The same design changes availability, quote, latency, contention, and task
success. Bounds must respect shared task and substitution constraints rather
than combine independent worst-case endpoints.

The current scalar-price response model additionally assumes quoted-price
sufficiency: conditional on `p(D)`, changing felt latency does not materially
change access or success. This assumption requires a factorial quote/latency
test. If it fails, the physical layer must tightly reserve latency for each
quote or AWM must expand to a response such as `eta(p, latency)` and
`rho(p, latency)`; the scalar-price Reveal-count result then no longer applies.

### G5.3 Artifact-aware exploration transitions

A losing probe can leave reusable state. The controller must decide what to
retain, promote, restore, or delete and compare against UDO-style heavy/light
ordering and scalar switch-cost models.

### G5.4 Candidate-relative certification

Candidate generation is part of the guarantee. The system must distinguish
certified candidates, probeable candidates, and `G_other`, and quantify the
opportunity excluded by the latter. Reveal resolution is price-level indexed:
the general bound is `sum_{q,v} |P_qv|`; it reduces to `|Q||V|` only when every
Reveal is pair-canonical, and to `|V|` only when every Reveal is simultaneously
canonical for all classes that can afford the representation. Uniform access
gates alone do not imply either reduced bound.

### G5.5 Robust switching under physical-cost uncertainty

Commit and anti-thrashing tests must handle time-uniform confidence sets for
candidate value, incumbent value, and transition cost. The stability margin
therefore includes all three widths:

```text
delta_t =
  candidate-value width
  + incumbent-value width
  + transition-cost width
```

Termination should follow from a finite legal design domain and the fixed
finite `P_qv` sets. A finite exploration purse limits exposure, but is not a
proof of termination; neither is an assumed positive minimum excursion cost.

## 6. Required Counterfactuals

The physical-planning part is justified only if:

1. joint `M/L/E` beats strong independent and sequential methods using the same
   primitives;
2. fixed-workload planning selects an inferior design because it misses
   design-induced access;
3. AWM/OED improves on bandit and black-box exploration at equal full cost;
4. artifact-aware transition reasoning changes useful decisions; and
5. quote-only and physical interventions agree after matching on `p_qv`, and
   quoted-price sufficiency survives a direct felt-latency intervention; and
6. the result survives materialization, probe, restoration, bounded
   cost-confidence, and telemetry accounting.

## 7. Main Takeaway

Pathfinder is not the first joint, online, transition-aware, or empirically
calibrated physical designer. The possible gap is the optimization of a
distributed representation path whose physical access changes the agentic
workload itself, combined with partial identification and stateful probes.
