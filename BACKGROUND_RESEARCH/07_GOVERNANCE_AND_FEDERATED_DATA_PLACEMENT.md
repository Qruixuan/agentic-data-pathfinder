# D7. Governance-Constrained and Geo-Distributed Physical Planning

## 1. Scope and Boundary

This subdirection studies how externally supplied governance rules constrain
the physical data path:

```text
D = (M, L, E)

M: which representation shards may be materialized
L: which zones, nodes, and tiers may hold their replicas
E: where transformations may execute and which consumers may receive outputs
```

The project does not interpret laws, contracts, or institutional policies.
Instead, it consumes a versioned, machine-checkable constraint set produced by
an external policy authority. The optimizer must generate and rank only plans
that satisfy those constraints, and the Data Agents must enforce the selected
plan locally.

The initial research scope includes:

- residency and permitted-zone constraints;
- artifact ownership and permitted consumers;
- storage, processing, and transfer restrictions;
- retention, revocation, and erasure events;
- declared derivation or de-identification attestations; and
- audit records that bind a plan epoch to the policy version it was checked
  against.

It excludes automatic legal interpretation, anonymization certification,
cross-organization identity federation, trusted-execution design, and
federated-learning protocols.

## 2. Why Governance Is a Physical-Planning Constraint

Governance is not merely a filter applied after performance optimization.
Different policy-valid plans can require different representation paths:

- raw data may be restricted to one zone while an externally attested derived
  representation may be transferable;
- a representation may be materialized only on approved nodes or storage
  tiers;
- execution may need to move to the data rather than moving data to the
  consumer;
- an ownership or retention change may invalidate replicas and cached plan
  validations; and
- a representation shared by several jobs may be valid for one consumer but
  not another.

Therefore, governance can change all three decisions in `D=(M,L,E)`. It is
still modeled as a feasibility constraint, not as an independent fourth
optimization variable.

## 3. Regulatory and Policy-Language Context

The legal texts motivate requirements but do not provide an executable
physical plan. For example, the
[GDPR](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32016R0679)
contains rules concerning erasure, copies or replications, and transfers to
third countries. The
[EU AI Act](https://eur-lex.europa.eu/eli/reg/2024/1689/)
contains record-keeping and logging obligations for specified classes of
high-risk systems. The exact applicability and interpretation of these rules
must remain outside the optimizer.

Machine-readable policy models offer reusable representation ideas.
[ODRL](https://www.w3.org/TR/odrl-model/) models permissions, prohibitions,
duties, constraints, and policy inheritance. The
[Data Privacy Vocabulary](https://www.w3.org/community/reports/dpvcg/CG-FINAL-dpv-20240801/)
provides terms for purposes, processing, locations, jurisdictions, and other
privacy concepts. These can inform the policy schema, but neither standard
selects a distributed representation, placement, and execution plan.

The safe system boundary is:

```text
external authority
  -> versioned policy and attestation facts
  -> optimizer feasibility checks
  -> signed or authenticated plan capability
  -> local Data Agent enforcement and audit
```

## 4. Representative Research

| Work | Main object and decision | Reusable result | Limit relative to this project |
|---|---|---|---|
| [Volley](https://www.usenix.org/legacy/events/nsdi10/tech/full_papers/agarwal.pdf) | Automated geo-distributed data placement under capacity, bandwidth, latency, and external constraints | Constraint-aware placement and migration planning | Places data objects; does not jointly choose a versioned multimodal transform path |
| [Geode](https://www.usenix.org/conference/nsdi15/technical-sessions/presentation/vulimiri) | Wide-area analytics under bandwidth and regulatory constraints | Geo-distributed execution and data-movement planning | Focuses on analytical query execution rather than reusable representation DAGs |
| [Gaia](https://www.usenix.org/conference/nsdi17/technical-sessions/presentation/hsieh) | Geo-distributed machine learning over constrained WANs | Communication-aware coordination across data centers | Optimizes model-training communication, not derived-data materialization and placement |
| [Compliant Geo-Distributed Data Processing in Action](https://vldb.org/pvldb/vol14/p2843-beedkar.pdf) | Declarative dataflow constraints and compliant distributed query plans | Separate legality from cost ranking; prune non-compliant placements | Closest direct prior, but its planning unit is relational dataflow rather than versioned multimodal representation shards |
| [Data Station](https://www.vldb.org/pvldb/vol15/p3172-xia.pdf) | Delegated, auditable computation over governed data | Policy mediation, auditing, and controlled computation | Does not jointly optimize physical representations, replicas, and transform boundaries |
| [PASS](https://www.usenix.org/legacy/event/usenix06/tech/full_papers/muniswamy-reddy/muniswamy-reddy_html/index.html) | Automatic storage provenance | Lineage capture and provenance maintenance | Provenance substrate rather than a policy-aware physical optimizer |
| [DELF](https://www.usenix.org/conference/usenixsecurity20/presentation/cohn-gordon) | Reliable deletion in asynchronous storage systems | Explicit deletion semantics and propagation | Does not optimize representation paths or reuse across jobs |
| [Concurrent Deletion in a Distributed Content-Addressable Storage System](https://www.usenix.org/conference/fast13/technical-sessions/presentation/strzelczak) | Deletion under global deduplication and multiple ownership | Reference and ownership-aware deletion mechanisms | Storage mechanism without joint planning or derivation semantics |
| [Text Embeddings Reveal (Almost) As Much As Text](https://aclanthology.org/2023.emnlp-main.765/) | Inversion attacks against text embeddings | Evidence that embeddings can retain sensitive source information | Security analysis, not a planning mechanism; cautions against treating derived forms as automatically transferable |

## 5. What the State of the Art Already Provides

### 5.1 Geo-distributed placement and execution are mature foundations

Volley, Geode, Gaia, and subsequent geo-distributed systems show how to model
sites, WAN costs, capacities, and external placement restrictions. This
project should reuse those abstractions and must not claim that
constraint-aware geo placement is new.

### 5.2 Compliance-aware query planning is a strong direct precedent

Compliant geo-distributed query processing already demonstrates the key
architecture of declarative constraints, compliant-plan generation, and
cost-based selection. Consequently, "put policy constraints into an
optimizer" is not a sufficient contribution.

The remaining distinction must be tested at a different planning object:
versioned multimodal representation paths whose transformations can expand or
contract bytes, create reusable artifacts, and serve multiple consumers.

### 5.3 Governed and auditable computation is established

Data Station and related trusted-data systems show that policy mediation,
delegated computation, and auditability can be designed as first-class system
properties. The controller, policy authority, and local enforcement boundary
should reuse these ideas rather than treating audit logging as novel.

### 5.4 Provenance and deletion mechanisms are established

PASS, DELF, and distributed-deletion work provide mechanisms for lineage,
ownership, references, and reliable deletion propagation. The proposed system
still needs these mechanisms, but its research question is whether their state
changes the optimal `M/L/E` plan and its transitions.

### 5.5 Derived representations are not automatically safe

An embedding, tensor, feature, or compressed object cannot be presumed
non-sensitive merely because it differs from the source encoding. Privacy and
inversion results make such a default unsafe. A broader placement permission
must therefore come from an explicit external attestation associated with the
representation version, not from the optimizer's inference.

## 6. Mechanisms to Reuse

### 6.1 Separate policy version from representation identity

The content and semantic identity of a representation should remain stable
when only a policy changes. Each plan validation and replica authorization
instead records the applicable policy version. A policy update can then
invalidate authorization without falsely changing the representation's
semantic identity.

### 6.2 Check legality before ranking performance

Candidate generation should attach explicit rejection reasons:

```text
candidate plan
  -> semantic validation
  -> governance validation
  -> capacity and safety validation
  -> cost or experiment-value ranking
```

An unconstrained optimum remains useful as an infeasible lower bound, but it
must never be deployed.

### 6.3 Use conservative inheritance and explicit attestations

By default, a child representation inherits the restrictions of its parents.
The system may broaden permissions only when the external authority provides
an attestation type accepted by the active policy. The optimizer verifies the
presence and scope of that attestation; it does not certify the transformation
itself.

### 6.4 Enforce capabilities locally

The controller should issue an authenticated plan capability containing the
plan epoch, operation, object or representation shard, source and destination,
consumer, policy version, and expiry. A Data Agent rejects an operation that
does not match the capability even if the central controller is stale or
misconfigured.

### 6.5 Treat revocation and erasure as state transitions

Policy changes should generate explicit transition work:

- block new reads, transforms, transfers, or materializations;
- expire or revoke plan capabilities;
- locate affected replicas through lineage and the replica registry;
- delete or quarantine artifacts according to the declared rule;
- record completion or failure; and
- replan from the remaining legal state.

These actions have cost and availability effects that can be measured without
claiming legal completeness.

## 7. Research Gaps Relevant to This Project

### G7.1 Governance over versioned multimodal representation paths

Prior compliant planners primarily reason about relational data, operators,
and sites. The open question here is how policy attaches to a representation
DAG, propagates across derivations, and changes the selection of raw,
decoded, sampled, tensor, or embedding paths.

### G7.2 Joint policy-aware `M/L/E` planning

A permitted-zone constraint may simultaneously remove a replica location,
move a transform boundary, and make a different materialized representation
valuable. Sequential "optimize first, filter later" approaches can therefore
lose feasible high-performance plans or incur expensive repair.

**Required counterfactual:** governance-aware joint planning must outperform a
performance-first planner followed by post-hoc filtering or repair in at least
one reproducible regime.

### G7.3 Governed experimental artifacts

A Reveal probe can produce a valid shard with reuse value, but only for
permitted consumers, locations, retention windows, and policy versions.
Experiment value must therefore include both reusable-artifact value and the
probability that the artifact remains adoptable under the active constraints.

### G7.4 Policy-change transitions over derived state

Existing planning work commonly treats a policy as static, while deletion work
focuses on correct removal rather than the resulting optimal plan. A useful
gap is the combination: revoke or erase affected derived state, account for
the transition, and choose the next legal `M/L/E` plan.

### G7.5 Multi-job ownership and consumer-specific reuse

The same representation shard may have several owners or consumers with
different permissions. The planner must decide whether to share, duplicate,
recompute, or avoid materialization while keeping policy validity explicit.

## 8. Recommended Role in the First Paper

Governance should initially be a **supported constraint and falsifiable
planning regime**, not an independent claim of a complete compliance system.
The minimum experiment is one emulated multi-zone scenario in which a declared
residency or transfer constraint changes the best legal representation path.

The paper should report:

- the performance gap between the unconstrained lower bound and best legal
  plan;
- invalid candidate and operation rejection rates;
- overhead of validation, local enforcement, and auditing;
- correctness and completion time under policy revocation or erasure; and
- the difference between joint governance-aware planning and post-hoc
  filtering or repair.

Governance can become a headline contribution only if experiments show that
it introduces a material and recurring `M/L/E` interaction beyond existing
compliant geo-distributed planning.

## 9. Main Takeaway

Geo placement, compliant query planning, policy mediation, provenance,
auditing, and reliable deletion all have strong precedents. The plausible
intersection is narrower: a policy version constrains a versioned multimodal
representation DAG, affects materialization, replica placement, transform
execution, and the access choices offered to agent tasks, and continues to
govern artifacts created by Reveal probes. The project should reuse mature
governance mechanisms and claim a separate contribution only if this changes
the performative design in a measurable way.
