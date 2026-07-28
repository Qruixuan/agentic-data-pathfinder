# Working System Design: Pathfinder Performative Physical Design

## Status and Boundary

This document contains a working system design for the research direction in
[AUTORESEARCH_MULTIMODAL_DATALAKE.md](AUTORESEARCH_MULTIMODAL_DATALAKE.md).
Its components and interfaces are provisional. The research claim should not
depend on implementing every mechanism described here.

The initial system is an optimization and management layer over durable object
storage and an existing workflow runtime. It is not itself a new object store.
It accepts declarative ownership, residency, retention, and consumer
constraints from an external policy authority; it does not interpret laws or
certify de-identification.

The design now targets an endogenous workload `W(D)`. The system must observe
which task classes and representations become reachable under each deployed
design, preserve those observations after rollback, and distinguish a
certified safe design from a temporary information-gathering probe.

## Design Goals

The prototype should:

- identify logical objects independently from their physical representations;
- validate reuse using lineage, determinism, and version compatibility;
- validate every materialization, placement, transfer, and experiment against
  versioned artifact-level governance constraints;
- express representation, placement, and transformation/delivery choices in a
  structured physical plan;
- expose one access-path resolver through which agents receive quoted prices,
  enforce affordability budgets, and emit class-indexed access observations;
- maintain a history-indexed demand and task-success envelope;
- distinguish certified Commit transitions from budgeted Reveal excursions;
- preserve probe observations after restoring the last safe plan;
- escalate from analytical estimates to microbenchmarks, partial
  materialization, and full deployment only as needed;
- charge plans for materialization and migration work; and
- expose enough control to test joint planning against strong layer-local
  baselines.

## System in One Sentence

The physical system is a **central Pathfinder planning service, one
access-path resolver and Session Manager, plus a data agent on every
participating worker**. The planning service compiles conceptual
`D=(M,L,E)` designs into replica, transformation, and routing commands; the
resolver exposes design-dependent access paths to agent sessions; and the
agents execute the commands over an existing object store, node-local
NVMe/RAM, CPU preprocessing workers, and AI consumers.

The controller never enters the bulk-data path. It stores metadata and issues
plans. Data moves directly between the object store, data agents, transform
workers, and consumers.

## Concrete MVP Boundary

The first prototype should make the following choices explicit:

- the physical optimization unit is a **representation shard**, identified by
  `(dataset version, shard, transformation fingerprint)`;
- representations and published shards are immutable;
- the durable object store is always the source of truth;
- optimized tiers are origin object storage, node-local NVMe, and host RAM;
- transformations execute on ordinary CPU workers close to the object store or
  on CPU resources attached to consumer nodes;
- consumers include queued agent sessions plus recurring training and
  analytics tasks; only the agentic stream must be endogenous in the MVP;
- every agent tool call is assigned a task class, session ID, affordability
  budget, and terminal representation where applicable;
- nodes carry administrative/geographic zone and trust-domain labels;
- the MVP policy model is limited to allowed regions/nodes/tiers, processing
  regions, consumer scope, retention, erasure epochs, and external
  attestations;
- the plan controls representation materialization, NVMe/RAM replicas, CPU
  transformation placement, and the selected delivery source;
- cache eviction uses a fixed local policy constrained by planner-issued
  reservations and pins; and
- interactive user-arrival elasticity, arbitrary continuous price menus, rack
  relays, in-network processing, GPU-memory caching, new storage formats,
  provider-internal object-store compute, automatic legal interpretation,
  cross-organization identity federation, and trusted execution remain outside
  the MVP.

A "storage-side worker" in the prototype means a controllable CPU worker in the
same cluster or network zone as the object store. It does not assume the
ability to run code inside S3 or another storage service.

The minimum distributed testbed contains one logical control service, an
existing object store, at least two CPU/data nodes with local NVMe, and at least
two consumer nodes. Components may share machines during early development,
but the evaluation must include multiple data and consumer nodes to exercise
placement and cross-node transfer.

## Physical Deployment Topology

```text
                             CONTROL NODE
  +---------------------------------------------------------------------+
  | Workload / task-class API                                            |
  |        |                                                             |
  |        v                                                             |
  | Catalog + Plan registry ---> Candidate Generator + Validator         |
  |        ^                         |                                   |
  |        |                         v                                   |
  | Observation history ---> AWM envelopes ---> OED Commit/Reveal        |
  |        ^                                     |                       |
  | Session Manager + Access Resolver <--- Escalation Manager            |
  +--------------------------|------------------------------------------+
                             | control RPCs, plan epochs, trial specs
              +--------------+------------------+
              |                                 |
              v                                 v
        CPU / DATA NODE                    GPU / CONSUMER NODE
  +-------------------------+        +------------------------------+
  | Data agent              |        | Data agent                   |
  | - replica manager       |<------>| - replica manager            |
  | - transfer server       |  data  | - transfer client/server     |
  | - transform executor    |        | - local transform executor   |
  | - telemetry exporter    |        | - telemetry exporter         |
  | NVMe cache + RAM buffers|        | NVMe/RAM -> AI consumer/GPU  |
  +------------^------------+        +------------------------------+
               |
               | object reads / derived writes
               v
        DURABLE OBJECT STORE
     raw immutable source objects
```

Bulk transfers use direct agent-to-agent or object-store-to-agent data
connections. Control RPCs carry identifiers, leases, plan epochs, checksums,
and commands rather than multimodal payloads.

## Component Placement and Responsibilities

| Component | Placement | Persistent state | Responsibility |
|---|---|---|---|
| Workload and task-class adapter | Control node, integrated with FlowMesh | Workflow versions, task classes, session budgets, success criteria | Converts queued agent, training, and analytics work into planner input |
| Representation catalog | Control node; one logical authority in the MVP | Logical objects, representation DAG, shards, replicas, compatibility | Defines identity and validates reuse |
| Plan registry and validator | Control node | Versioned plans, constraint versions, plan epochs, validation results | Stores executable realizations of `D=(M,L,E)` and rejects semantically, operationally, or policy-invalid plans |
| Candidate generator | Control node | Plan grammar, finite price profiles, dominance rules | Produces the finite generated design set used by each OED round |
| Adaptive Workload Model | Control node | Demand/success envelope, substitution groups, cost-confidence sets | Computes `Phi^-` and `Phi^+` over the complete deployment history |
| OED controller | Control node | Safe design, candidate pools, margins, Reveal resolution state | Selects Commit, Reveal, Hold, certificate-limited stop, or budget-limited stop |
| Escalation and experiment manager | Control node | Probe leases, per-excursion cap, exploration purse, rollback status | Runs model checks, microbenchmarks, partial materializations, and full Reveal excursions |
| Observation store | Control node plus durable trace files | Class-indexed access logs, task outcomes, profiles, costs, provenance | Retains every safe/probe observation after rollback |
| Session Manager and access resolver | Control path or local sidecar | Session identity, task class, quoted paths/prices, terminal representation | Makes agent access design-dependent and attributes induced demand to a deployed design |
| Data agent | Every data/consumer node | Local replica index and active leases | Executes replica, transform, serve, pin, and eviction commands |
| Transform executor | CPU/data node or consumer node | No authoritative metadata | Runs a fingerprinted representation-changing operator |
| Consumer adapter | FlowMesh worker process or sidecar | Active plan binding | Resolves each logical shard through the selected physical path |
| External policy authority | Existing governance or administrative system | Source policy, consumer identity, and attestation records | Supplies declarative constraints; remains authoritative for legal and organizational meaning |
| Durable object store | Existing external service | Raw source and optionally durable derived artifacts | Remains the recoverable source of truth |

The MVP can store catalog, plan, and experiment metadata in one transactional
database and keep large traces in immutable files. Splitting them into several
services is an operational optimization, not a research requirement.

## Representation Catalog

The catalog provides the metadata needed to reason about logical identity and
safe reuse. It records:

- stable logical object identifiers;
- modality and schema information;
- chunks and physical encodings;
- derived-representation lineage;
- transformation code, parameters, and version fingerprints;
- object locations, tier, and replica state;
- size and observed access statistics;
- freshness and compatibility constraints; and
- determinism, cacheability, and reuse scope;
- owner, administrative/geographic zone, and sensitivity labels;
- allowed storage, processing, transfer, and consumer scopes;
- retention deadlines and erasure epochs; and
- externally supplied de-identification or release attestations;
- task classes, terminal representations, affordability gates, and success
  criteria;
- declared representation substitution groups; and
- safe-plan and probe observations indexed by quoted price, realized cost,
  class, session, representation, and plan epoch.

The representation model is a directed graph. Nodes are immutable or versioned
representations; edges are transformations. A materialized node refers to its
parents and the exact transformation fingerprint that created it.

The minimum catalog schema is:

```text
LogicalDataset {
  dataset_id
  dataset_version
  shard_ids[]
}

RepresentationVersion {
  representation_id
  parent_representation_ids[]
  schema_and_encoding
  transformation_fingerprint
  determinism_and_randomness_contract
  model_or_tokenizer_version
  quality_contract
  governance_policy_id
  derivation_attestation_ids[]
}

RepresentationShard {
  representation_id
  shard_id
  logical_range
  byte_size
  checksum
}

Replica {
  representation_id
  shard_id
  node_id
  tier
  state
  lease_or_pin_expiry
  checksum
  policy_version
  authorization_expiry
}

GovernancePolicy {
  policy_id
  policy_version
  owner
  sensitivity_labels[]
  allowed_storage_regions[]
  allowed_processing_regions[]
  allowed_tiers[]
  allowed_consumers[]
  retention_deadline
  erasure_epoch
  required_attestation_types[]
}

TaskClass {
  task_class_id
  mode                         # agent, training, analytics
  arrival_contract             # queued/exogenous in the guaranteed scope
  max_accesses_per_session
  affordability_budget
  terminal_representation_id
  task_value
  success_criterion_version
}

SessionArtifact {
  artifact_id
  session_id
  task_class_id
  transformation_fingerprint
  parent_representation_ids[]
  ttl
  promotion_state
  policy_version
}

DesignObservation {
  design_id
  plan_epoch
  observation_window
  task_class_id
  session_count
  representation_access_counts[]
  task_success_count
  quoted_prices[]
  realized_costs[]
  background_state
}
```

A recommended exact identifier is:

```text
representation_id =
  hash(dataset_version,
       parent_representation_ids,
       operator_code_version,
       parameters,
       randomness_contract,
       model_or_tokenizer_version,
       output_schema_and_quality)
```

The physical path or node is not part of `representation_id`; it belongs to a
`Replica`. This allows identical valid output to be discovered across jobs and
locations without treating a copied shard as a new logical representation.

Policy identity is intentionally separate from representation identity. The
same bytes may remain semantically identical while a policy update narrows
where they may be stored or served. A cached compatibility decision therefore
binds both `representation_id` and `policy_version`.

By default, a derived representation inherits the intersection of its parents'
constraints. A transformation may broaden eligibility only when the external
policy authority supplies a recognized attestation for that exact
transformation fingerprint and source-policy version. The optimizer never
infers that an embedding, token sequence, or anonymized tensor is safe merely
because it is smaller or appears de-identified.

A `SessionArtifact` is initially TTL-scoped. It may be promoted to a shared
representation shard only when its transformation fingerprint, semantic
contract, policy version, checksum, and ownership scope match a cataloged
representation. Promotion can trigger candidate regeneration because agent
behavior has changed the physical state available to later designs.

Substitution groups are versioned catalog declarations, not automatically
inferred truths. They determine which demand floors the AWM may propagate.
Changing a group definition invalidates cached envelope and certification
results.

Replica state follows a small state machine:

```text
ABSENT -> RESERVED -> BUILDING/TRANSFERRING -> VERIFYING -> VALID
                                      \                     |
                                       -> FAILED            -> EVICTING -> ABSENT
```

Only `VALID` replicas may be returned to consumers. Data is first written to a
temporary name, verified using its manifest/checksum, and then published by an
atomic metadata transition. The catalog remains the authority; a local file
without a valid catalog record is an orphan, not a reusable replica.

Before reuse, the planner must check at least:

1. The logical input and required output schema match.
2. The transformation code and parameters are compatible.
3. The producing model version is compatible for model-derived artifacts such
   as tokens or embeddings.
4. The representation is fresh enough for the workload.
5. Any stochastic transformation was either reproduced with a compatible seed
   and semantics or marked non-reusable.
6. The replica's policy version is current and permits the requesting consumer.
7. Its node, tier, processing location, and transfer path satisfy residency and
   trust-boundary constraints.
8. The artifact has not passed its retention deadline or erasure epoch, and
   every required release or de-identification attestation is present.

## Distributed Data Plane

The MVP data plane spans remote object storage, node-local NVMe, and host
memory. A node-local agent reports contents, capacity, observed transfer rates,
transformation throughput, zone/trust labels, and supported attestation types.
Accelerator-memory caching can be considered later but is not a first-version
plan variable.

Each data agent contains four required subsystems:

1. **Replica manager:** reserves capacity, maintains the local shard index,
   applies pins/leases, and performs safe eviction.
2. **Transfer server/client:** reads from the object store or peers and streams
   a selected representation shard to the next hop.
3. **Transform executor:** runs a declared, versioned transformation and
   produces a manifest for its output shard.
4. **Telemetry exporter:** attributes bytes, queueing, service time, cache
   state, and failures to a plan, trial, representation, shard, and operator.

Every state-changing or serving command carries an authenticated plan
capability containing the plan epoch, constraint version, allowed operation,
representation shard, source and destination, target node or tier, consumer,
and expiry. The agent enforces this capability locally before it reads,
transforms, stores, transfers, or serves a shard. The MVP may use one
administrative signing authority; designing cross-organization identity
federation is outside scope.

The initial control interface should support explicit operations:

- `ensure_replica(representation_shard, node, tier, lease, plan_capability)`;
- `run_transform(parent_shard, output_representation, executor_node, plan_capability)`;
- `replicate(representation_shard, source, destination, tier, plan_capability)`;
- `serve(representation_shard, source, consumer, route, plan_capability)`;
- `stage(representation_shard, consumer, deadline, plan_capability)`;
- `pin(representation_shard, node, tier, lifetime, plan_capability)`;
- `evict(representation_shard, node, tier, plan_capability)`; and
- `invalidate(representation, reason, plan_capability)`.

These operations must be idempotent where possible and produce provenance and
policy-audit events. Failed or partial replicas must not be advertised as valid
inputs.

Control commands use an idempotency key derived from `(plan epoch, operation
ID)`. Repeated delivery of the same command returns the existing operation
state rather than generating another materialization or replica.

The data plane may implement several delivery strategies without changing the
logical workflow:

- direct reads from object storage;
- reads through node-local or rack-local caches;
- peer-assisted reads from an existing replica;
- relay distribution when several consumers require the same object; and
- decode or transform once, followed by distribution of the derived output.

The MVP should initially implement only direct object-store reads,
node-local/peer reads, and transform-then-transfer or
transfer-then-transform. Relay trees and rack-local caches remain fixed or
disabled until the central `M/L/E` interaction is established.

## Physical Plan

The research abstraction uses a focused design `D = (M, L, E)`:

- `M`: reusable representations to materialize;
- `L`: tier placement and replication of those representations; and
- `E`: transformation placement and delivery path.

A concrete execution schema can expand this abstraction:

```text
PhysicalPlan {
  plan_id
  parent_plan_id
  plan_epoch
  workload_version
  constraint_set_id
  constraint_version
  consumer_identity_and_zone
  reuse_horizon
  materializations[]           # M
  replica_placements[]         # L
  transformation_assignments[] # E
  delivery_bindings[]          # E
  capacity_reservations[]
  pin_and_eviction_directives[]
  staging_schedule[]
  validity_constraints[]
  governance_constraints[]
  required_attestations[]
  audit_specification
  task_class_bindings[]
  quoted_access_price_matrix     # p_qv(D), indexed by task class and representation
  affordability_gate_version
  price_universe_version         # ex ante finite P_qv sets
  observation_specification
  expected_transition_cost
  fallback_plan_id
}
```

The low-level fields are not intended to become independent knobs. For
example, a cache directive realizes a placement decision, and a staging
schedule realizes the selected delivery path.

Governance constraints are feasibility conditions rather than a fourth
optimization variable. They may remove materialization, placement, transform,
or route candidates, after which the planner ranks only the remaining legal
`M/L/E` designs.

The quoted access-price matrix belongs to the exact physical design. For each
`(task class q, representation v)`, `p_qv(D)` is computed from the cheapest
governance-legal path between that class's consumer and `v`. The same replica
can therefore receive different quotes for training, agentic, and analytics
consumers. A probe must execute the same design and quote the same matrix that
was scored; a catalog-only price override would observe a different
intervention and must be recorded as such.

The first guaranteed implementation uses a finite, predeclared price universe
`P_qv` for each class-representation pair, derived from actual tier, route, and
coverage configurations across the allowed design domain. This keeps
Reveal-resolution state finite and makes
`canonical_price_qv = max(P_qv)` testable before the run. A continuously
parameterized throttle or routing weight is outside the Reveal-count theorem
unless it is discretized into this menu.

The three research decisions map to concrete system effects as follows:

| Decision | Plan record | Agent operations | Observable physical effect |
|---|---|---|---|
| `M` materialization | target representation shards and lifetime | `run_transform`, `ensure_replica`, `pin` | A reusable derived shard is created and published |
| `L` placement/replication | node, tier, replica count, lease | `replicate`, `ensure_replica`, `evict` | Bytes and capacity move among origin, NVMe, RAM, and peers |
| `E` transform/delivery | executor node, parent representation, source replica, consumer route | `serve`, `run_transform`, `stage` | Conversion occurs before or after a network hop, and the consumer reads from a selected source |

### Plan Compilation

The planner emits a declarative `PhysicalPlan`; a plan compiler turns it into
four executable artifacts:

1. **Transition plan:** ordered `ensure_replica`, transform, copy, pin, and
   eviction operations needed to move from `D_old` to `D_new`.
2. **Runtime bindings:** for every `(consumer stage, logical shard)`, an ordered
   source, representation, transform chain, and fallback.
3. **Telemetry and audit specification:** the counters, spans, policy
   decisions, and artifact events required to evaluate the plan or experiment
   hypothesis and reconstruct its governed data movement.
4. **Rollback plan:** bindings to the last active plan plus cleanup rules for
   artifacts created only by the candidate.

Preparation and activation are separate. The system may construct and verify
new replicas while the old plan remains active. Only after required shards and
fallbacks are ready does the controller atomically advance the active
`plan_epoch`.

### Plan Lifecycle

```text
DRAFT
  -> VALIDATED
  -> PREPARING
  -> CANARY
  -> ACTIVE
  -> DRAINING
  -> RETIRED

PREPARING/CANARY -> FAILED or ROLLED_BACK
```

An active FlowMesh task is bound to one immutable plan epoch. A new epoch
affects newly admitted tasks or explicitly marked canary shards; it must not
silently change the physical semantics in the middle of a task. Old bindings
remain valid until their readers drain, after which unneeded replica leases may
expire.

The planner should expose structured transformations over legal plans:

- materialize or remove a reusable intermediate representation;
- move a representation between storage tiers;
- change the replication factor or replica location of a hot shard;
- move a transformation across a network boundary;
- change the representation from which a consumer is served;
- replace origin reads with an existing peer or relay; and
- co-locate a transformation with its input or consumer.

Chunk size, prefetch depth, cache eviction, and delivery topology should begin
with a small set of profiles. They can be promoted to first-class choices only
after measurements show that fixed profiles hide an important interaction.

## Consumer Request and Data Resolution

The workflow continues to request logical data rather than physical file
paths:

```text
DataRequest {
  session_id
  task_class_id
  workload_version
  plan_epoch
  consumer_stage
  logical_dataset
  shard_id
  required_representation
  affordability_budget
  terminal_representation_if_any
}
```

The consumer adapter resolves this request from the immutable runtime binding:

```text
RuntimeBinding {
  preferred_source_replica
  source_representation
  transformation_chain[]
  executor_assignments[]
  destination_tier
  quoted_access_price
  resolver_decision              # admit, reject, or fallback
  fallback_sources[]
}
```

Resolution follows a predictable sequence:

1. verify the task class, session, plan epoch, and representation requirement;
2. obtain the governance-valid path and quoted price from the immutable
   binding;
3. reject the request before issuing physical accesses when the quoted path
   exceeds the declared affordability budget;
4. use a valid local replica if it is the selected source;
5. otherwise fetch from the selected peer or object store;
6. run any assigned local transformation and verify its output;
7. deliver the required representation and record terminal-task success
   separately from access count;
8. emit class-indexed quoted price, realized cost, path, and outcome telemetry;
   and
9. use a fallback only if it remains affordable, semantically compatible, and
   governance-valid.

The planning service is not contacted for every data chunk. Runtime bindings
and price profiles are distributed and cached when a task starts. A lightweight
resolver or sidecar mediates agent tool calls and emits observations, while
bulk data still moves directly among agents, transforms, and storage.

## End-to-End Physical Data-Path Example

Assume one video corpus is used by training, queued agent sessions, and
analytics. The representation graph is:

```text
raw H.264
  -> decoded/sample frames
      -> embeddings
      -> structured multimodal digest
```

The system can execute four reference paths:

```text
Plan A: serve or recompute every requested form from the origin

Plan B: pre-stage all selected derived forms near consumers

Plan C: materialize a shallow sampled parent near the source and recompute
        deep forms at each consumer

Plan D: build the expensive digest near the governed source, materialize it,
        and serve its fields to training, agent, and analytics consumers
```

The digest is intentionally different from an embedding index that every mode
already justifies alone. Its full-corpus construction is expensive, while its
fields support content questions, weak labels, and scans across several modes.
Observed demand can be near zero before the digest exists because the
equivalent per-request vision path exceeds agent and analytics budgets.

For a Reveal of Plan D, execution is:

1. the compiler reserves NVMe on a data node;
2. cheaper rungs first measure decode/VLM throughput and optionally build a
   policy-valid partial digest;
3. if demand remains censored and the optimistic value covers the entire
   excursion, the agent builds and verifies the full digest;
4. the probe epoch makes the scored digest path genuinely affordable and
   collects class-indexed agent/task outcomes;
5. the controller restores the previous `D_safe` after the observation window
   while retaining the Plan D observation;
6. the next OED round may certify Plan D and pay a new transition, or hold it;
   and
7. valid microbenchmark, partial, and session artifacts are adopted when their
   fingerprint and policy allow reuse.

This example is the physical manifestation of PPD: the system must value an
artifact using demand that may not exist in the incumbent log because the
artifact is currently unaffordable. Whether the three modes jointly justify
the transition is measured rather than assumed.

## Plan Validation

Validation occurs before a trial allocates resources or creates derived data.
The validator rejects plans that:

- reuse stale or incompatible representations;
- treat a non-deterministic output as generally reusable;
- materialize an artifact that the active policy version does not permit;
- store, process, or transfer a representation outside its allowed regions,
  nodes, tiers, trust domains, or consumer scope;
- rely on a missing, expired, or mismatched de-identification/release
  attestation;
- retain or serve an artifact beyond its retention deadline or erasure epoch;
- exceed hard storage, memory, or monetary budgets;
- assign an operator to incompatible hardware;
- request an unavailable delivery mechanism; or
- violate workload ordering or data-dependency constraints.

The validator should also estimate the materialization and migration work of a
candidate. A plan that is semantically legal may still be pruned when its
transition cost cannot be amortized within the declared reuse horizon.

Validation has two explicit outputs:

1. a deterministic legality result with machine-readable rejection reasons;
2. a costed legal plan for ranking or experimentation.

Performance evidence can change the second output but never override the first.
A policy update invalidates cached validation results even when the underlying
representation bytes and fingerprint are unchanged.

## Structural Cost Model

The first-order model should use directly measurable quantities:

- representation and chunk sizes;
- transformation service rates by hardware type;
- storage-tier bandwidth and latency;
- link bandwidth and nominal contention state;
- cache and staging capacity;
- quoted access prices `p_qv(D)` and realized access costs
  `realized_cost_qv(D)` by task class and representation;
- task-success values, access budgets, and reuse horizon; and
- materialization, replication, and eviction costs.

It should model the critical path of the workload DAG rather than simply sum
all operator times. The initial model can be deliberately approximate, because
its primary jobs are to reject dominated plans, provide interpretable priors,
and identify uncertain comparisons worth measuring.

Quoted price and realized cost are distinct class-by-representation matrices.
The resolver gates demand using `p_qv(D)`; `Phi` charges
`realized_cost_qv(D)`. The deployable Commit certificate widens both `Phi^-`
and `Phi^+` over a time-uniform, componentwise cost-confidence box and uses an
upper transition-cost bound. A coupled cost-confidence set would make the
robust problem bilinear or otherwise require a different solver and guarantee.
Changing the quote without changing the scored physical design is a separate
causal intervention, not a self-observation of that design.

## PPD Control Loop: Adaptive Workload Model and OED

```text
D_safe + complete observation history
  -> generate and validate finite candidate set
  -> build E_t(D) and cost-confidence sets
  -> compute Phi_t^-(D), Phi_t^+(D)
  -> Commit, Reveal, Hold, or Stop
  -> retain every observation and repeat
```

The controller does not point-estimate the workload and then run an ordinary
optimizer. It brackets demand and task success under every generated design,
keeps a separate certified safe design, and treats an information-gathering
deployment as a paid excursion. Every iteration emits:

```text
PPDIteration {
  trigger
  safe_design
  observation_history_version
  candidate_generator_version
  certifiable_candidates[]
  probe_candidates[]
  other_candidates[]
  demand_envelope_version
  cost_confidence_version
  pessimistic_and_optimistic_values[]
  selected_action                  # commit, reveal, hold, stop
  per_excursion_cap
  remaining_exploration_purse
  resolution_levels[]
  reveal_selection_tier          # simultaneous-canonical, pair-canonical, fallback
  stability_radius_breakdown     # candidate, incumbent, transition widths
  stopping_reason
}
```

### Observation History and Assumption State

An iteration starts from a deployed and observed `D_safe`. The observation
store retains all safe and probe deployments, including:

- per-class session and task-success counts;
- per-class/per-representation access counts;
- the complete class-by-representation quoted-price matrix, affordability
  rejections, and felt latency;
- realized service, transfer, and transition costs;
- the exact task, agent, prompt, model, policy, graph, and plan versions; and
- the observation window and background load.

Every AWM certificate binds a versioned substitution-group declaration,
success-monotonicity contract, session-arrival contract, access cap,
quoted-price-sufficiency contract, and graph or resource closure. A failed
empirical gate disables the corresponding certificate instead of being
absorbed into a larger safety margin.

### Candidate Proposal and Validation

Each round obtains a finite `Gen_t` from a versioned candidate generator plus
every still-legal design that has already been observed. The first
implementation uses the ex ante finite `P_qv` universe known before the run,
not merely the candidates emitted in the current round. This avoids defining a
canonical Reveal price using a future adaptive generator output.

Candidates are partitioned into:

1. `G_cert`: observed designs or structural changes covered by the sound AWM
   envelope relative to `D_safe`;
2. `G_probe`: designs that make at least one unresolved
   class-representation-price state affordable; and
3. `G_other`: generated designs that are neither certifiable nor uncensoring.

The initial OED loop cannot safely reach `G_other`. The observation store and
evaluation must report how much reduced-oracle value falls into that pool.

An LLM may summarize traces or propose a structured transformation, but the
system must validate the resulting plan. Correctness and the core search
procedure must not depend on unconstrained natural-language output.

An experiment may only create artifacts and transfers permitted by the active
constraint version. Trial artifacts inherit their parent policies unless an
exact transformation attestation permits a different policy. Promotion binds
the resulting plan to the same validated constraint version; a policy change
during a trial forces revalidation before adoption.

### Commit, Reveal, and Escalation

For each certifiable candidate:

```text
PesGain(D') =
    Phi_t^-(D')
  - Phi_t^+(D_safe)
  - upper_transition_cost(D_safe -> D')
```

Commit only when `PesGain(D')` exceeds the declared margin.

When no Commit is available, a Reveal candidate must have positive optimistic
gain after the complete excursion:

```text
OptGain(D') =
    Phi_t^+(D')
  - Phi_t^-(D_safe)
  - lower_forward_transition
  - lower_restore_transition
  - lower_probe_window_loss
```

Possibility and authorization are separate. A probe is authorized only when
the upper-bound excursion fits both a per-excursion cap and the remaining
exploration purse.

Among authorized probes, selection is tiered:

1. prefer candidates simultaneously canonical for every affording class at a
   representation they leave unresolved;
2. otherwise prefer candidates canonical for at least one unresolved
   class-representation pair; and
3. otherwise allow any budget-feasible probe and record it as a non-canonical
   fallback.

A pair is resolved only up to the highest affordable price at which it has
been observed. The general Reveal-count budget is `sum_qv |P_qv|`.
Pair-canonical probes permit the `|Q||V|` specialization, and the `|V|`
specialization requires every Reveal actually taken to be simultaneously
canonical. Uniform gates alone do not imply this.

Before a full Reveal, the escalation manager tries:

1. analytical bounds;
2. operator, transfer, or storage microbenchmarks;
3. workload slices using existing replicas;
4. partial materialization or lazy population; and
5. full design deployment.

The cheaper rungs update cost-confidence sets and may create reusable shards.
They do not claim to identify demand that requires complete coverage or a
genuinely affordable physical path.

### Reveal Execution and Provenance

A full Reveal is implemented as a probe plan epoch, not an ad hoc mutation of
the safe plan:

1. begin the round deployed on `D_safe`;
2. compile the exact scored physical delta and reserve its full excursion
   budget;
3. prepare and verify candidate replicas while safe workloads continue;
4. activate the probe only after the materialization cost has been incurred;
5. quote the prices attached to the scored design and collect a
   pre-registered observation window;
6. append the class-indexed access, success, and cost observation to the
   permanent history;
7. restore `D_safe` and charge the return transition plus foreground loss;
8. retain the observation even though the physical probe is rolled back; and
9. adopt semantically and policy-valid artifacts or expire probe-only leases.

A canary validates correctness and realized service cost after preparation. It
does not make demand revelation cheap: if the representation must exist at
full coverage before a task becomes affordable, the expensive preparation is
already sunk before any canary traffic can reveal demand.

The probe record distinguishes reusable transition work from discarded work.
For example, a sampled-frame shard generated by an unsuccessful placement
experiment may remain useful to another legal plan, while a shard produced
under an incompatible transform fingerprint must not be adopted.

Governance validity is independent of semantic validity. An artifact may be
byte-correct yet ineligible for adoption because it was produced, stored, or
transferred under the wrong policy version or location. Such work is classified
as invalid rather than reusable or merely stranded.

Parallel probes are not isolated merely because they run in different
containers. They may still share object-store bandwidth, links, and disks. The
experiment manager must choose one of three explicit policies:

1. allocate isolated resources;
2. co-schedule only candidates whose resource footprints do not conflict; or
3. record concurrent load and model interference.

The first prototype should prefer sequential or resource-isolated trials until
the ranking is reproducible.

### AWM Update and Safe-Design Selection

The AWM intersects the constraints contributed by every compatible historical
deployment. For an exactly observed design, the demand/success envelope
collapses to the observation region; its cost-confidence set may still require
more measurement.

The envelope stores coupled constraints:

- non-negative class-by-representation demand;
- per-class total access caps;
- own-representation floors where monotonicity applies;
- substitution-group total floors;
- task-success floors and terminal-access upper bounds; and
- graph/resource closure restrictions.

`Phi^-` and `Phi^+` are the minimum and maximum of the session-value objective
over this feasible set and the realized-cost confidence set. Independent box
endpoints must not be substituted into the objective.

For each certifiable candidate, the controller also records:

```text
candidate_width  = Phi_t^+(D')     - Phi_t^-(D')
incumbent_width  = Phi_t^+(D_safe) - Phi_t^-(D_safe)
transition_width = upper_kappa(D_safe -> D')
                 - lower_kappa(D_safe -> D')

delta_t = max over G_cert of
          candidate_width + incumbent_width + transition_width
```

The latter two terms may vanish under exact observation, but cannot be omitted
from the deployed certificate while cost remains interval-valued.

Operator-level traces still update the structural cost model. A learned
residual may predict service-cost error, but it does not point-predict censored
demand and cannot decide semantic or governance validity.

### Stopping, Refusal, and Reopening

The controller records one of:

1. **Commit:** a certifiable candidate clears pessimistic gain plus margin;
2. **Reveal:** no Commit exists and an optimistic, uncensoring excursion fits
   both budgets;
3. **Certificate-limited stop:** no generated Commit or Reveal candidate
   clears its corresponding value test;
4. **Budget-limited stop:** a potentially valuable Reveal exists but its
   conservative excursion cost is unaffordable; or
5. **Structural hold:** value may remain in `G_other`, outside the current
   certificate and Reveal mechanisms.

Only Commit changes `D_safe`. Certificate-limited stop is relative to the
generated certifiable and probe pools, not a global optimality statement.
Budget-limited and structural-hold results certify nothing about the untested
frontier.

Termination uses the fixed finite `P_qv` universes for Reveal progress and the
finite design domain plus the positive Commit margin for safe-design progress.
It does not assume a strictly positive minimum excursion cost. The purse and
per-excursion cap bound financial exposure, not logical termination.

If quoted-price sufficiency fails, ordinary OED certificates are disabled.
The first repair is an enforceable latency reservation that pads or otherwise
holds felt latency nearly constant for each quote. The broader repair models
response as `eta(p, latency)` and `rho(p, latency)`; the controller may still
operate over that state, but the scalar-price Reveal-count theorem no longer
applies without a new partially ordered resolution frontier.

## Telemetry and Provenance

Each trial should be linked to the exact:

- task class, session ID, arrival contract, access cap, and success criterion;
- workload and DAG version;
- dataset and representation versions;
- physical plan;
- constraint and policy versions;
- requesting consumer identity and admitted zone;
- transformation code and parameters;
- cluster allocation and observed background load; and
- start/end time and random seeds.

Useful measurements include:

- per-class session count, access intensity, affordability rejection, and
  graded success;
- quoted price and realized cost for every resolved representation path;
- per-operator service and queueing time;
- bytes transferred by source, destination, tier, and representation;
- link and object-store utilization;
- cache hits by representation rather than only by file;
- staging lead time and unused staged bytes;
- CPU/GPU utilization and accelerator starvation;
- request count and throttling at object storage; and
- end-to-end valid-sample goodput or workflow makespan.

The audit stream additionally records validation outcomes, capability issuance,
materialization, transfer, serve, invalidation, erasure, and policy-revocation
events. It should contain identifiers and decisions rather than raw sensitive
payloads. Audit completeness and policy-enforcement overhead are evaluation
metrics, not assumptions.

Instrumentation overhead must be measurable and configurable. A minimal
production mode may retain aggregate counters while research trials collect
fine-grained traces.

## FlowMesh Integration

FlowMesh can serve as the control and execution substrate. Capabilities that
may be reused include:

- workflow DAG parsing and task lifecycle management;
- distributed worker registration and dispatch;
- S3, SQL, and external data retrieval;
- artifact references between stages;
- cached model or dataset locality hints;
- stage stickiness and task merging;
- heterogeneous inference, training, retrieval, and multimodal executors; and
- task-level runtime and resource telemetry.

The concrete integration contract should be narrow:

1. **Workflow registration:** FlowMesh submits a versioned DAG, dataset
   references, target representations, expected repetitions, and resource
   requirements, plus task classes, terminal representations, success criteria,
   access budgets, consumer identity, and administrative zone.
2. **Plan admission:** the planner returns a `plan_id`, `plan_epoch`, task
   placement constraints, and any preparation barrier.
3. **Task dispatch:** FlowMesh passes the immutable plan epoch and local data
   agent endpoint to each task.
4. **Data access:** the task's consumer adapter asks the access-path resolver
   for logical representation shards and receives an admitted/rejected binding
   plus quoted price; it does not embed physical object paths in the DAG.
5. **Lifecycle reporting:** FlowMesh reports task start, finish, retry, and
   cancellation so leases, makespan, and transition cost can be attributed
   correctly.
6. **Fallback:** if the optimizer or catalog is temporarily unavailable, an
   already admitted task continues using its cached binding; a new task may use
   a conservative origin-read plan only when that fallback is valid under the
   active constraint version.

The project expects to add:

- a representation and lineage catalog;
- chunk and replica metadata;
- multi-tier cache agents;
- explicit materialization and staging operations;
- network and data-path monitoring;
- a physical plan schema and compiler;
- fine-grained data-path telemetry; and
- an experiment manager and optimizer.
- a Session Manager, access-path resolver, class-indexed observation store,
  AWM envelope solver, and OED controller.

The intended responsibility split is:

```text
multimodal data-plane extensions
        +
FlowMesh control and execution plane
        +
Pathfinder PPD planner, AWM, and OED controller
```

Before implementation, FlowMesh's actual interfaces and current capabilities
must be verified. This document lists an intended integration, not an assertion
that all hooks already exist.

FlowMesh remains responsible for workflow dependencies, worker allocation, and
task lifecycle. The new system owns representation identity, physical replica
state, data-path bindings, trial provenance, and optimization decisions. This
boundary prevents the research prototype from becoming a replacement workflow
scheduler.

## Minimum Implementation Slice

The smallest implementation that can test the research claim contains:

1. a controller process with a transactional catalog, plan registry, static
   candidate generator, observation store, and experiment API;
2. one data-agent daemon on every CPU/data and GPU/consumer node;
3. a Session Manager and access-path resolver with task classes, finite price
  matrices, access budgets, felt-latency logging, and graded success;
4. immutable representation-shard manifests and one deterministic video
   transform chain containing a cheap representation and one expensive
   full-coverage artifact;
5. origin, local NVMe, host RAM, and peer-transfer paths;
6. reference Plans A/B/C/D plus reduced-instance exhaustive deployments;
7. probe epochs and restore to the last policy-valid `D_safe`, using direct
   origin reads only where permitted;
8. one emulated multi-region policy scenario in which raw and derived
   representations have different allowed placements; and
9. class-indexed demand/success, per-plan/per-representation operator, storage,
   network, transition, and policy-audit telemetry.

The initial implementation does not require a learned model. First validate
elasticity, group-total monotonicity, success monotonicity, and quoted-price
sufficiency through the resolver's causal harness. Then exhaustively deploy a
reduced design space to measure envelope coverage, decision power, and lock-in
before implementing the complete OED loop.

## Recovery and Safety Requirements

The prototype must preserve correctness when a trial or worker fails:

- materializations become visible only after an atomic validity record;
- catalog state distinguishes planned, transferring, valid, and failed replicas;
- every command and runtime binding carries a plan epoch;
- replica leases and active-reader references prevent premature eviction;
- eviction never removes the durable source of truth;
- plan deployment can fall back to a version-compatible direct origin path;
- a failed canary rebinds only its canary tasks and leaves the parent plan
  active;
- controller restart recovers operations from the catalog rather than assuming
  that a requested copy or transform completed; and
- invalidation propagates to derived nodes or prevents their future reuse.
- policy revocation or erasure advances a monotonic epoch, blocks new reads and
  transfers immediately, and schedules all affected derived replicas for
  deletion after active readers drain according to the declared policy.

These mechanisms need not form the research contribution, but weak failure
handling would undermine measurements and reproducibility.

Correctness-critical metadata updates require transactional or compare-and-swap
semantics in the MVP. Replica inventory and telemetry may be eventually
consistent because stale observations affect performance, not the identity of
the durable source or validity of a published representation.

## Open Design Decisions

The following questions should remain open until measurement narrows them:

1. The fixed shard size/range policy and whether a later optimizer should be
   allowed to change it.
2. Whether one common plan schema can cover training and offline inference
   without adding workload-specific semantics.
3. Whether both peer pull and proactive stage are necessary after direct origin
   and peer delivery work reliably.
4. Whether a learned residual is necessary, and which model class is simple
   enough to train from a small number of trials.
5. How much of cache eviction should be planned explicitly versus delegated to
   a fixed tier-local policy.
6. Whether the control metadata should remain one service or be separated for
   scale after the research result is established.
7. Whether governance materially changes plan choice often enough to be a main
   contribution or should remain a supported constraint.
8. Which policy language and attestation interface are sufficient for the
   first prototype without turning the work into a general compliance system.
9. Which physically realizable finite `P_qv` menu is expressive enough for
   planning while still supporting canonical Reveal and resolution claims.
10. Whether training and analytics remain fixed-demand contributors while only
    the queued agentic stream is modeled as endogenous.
11. Whether partial materialization preserves enough value to replace a full
    Reveal, and for which representation classes.
12. How much reduced-oracle value lies in `G_other`, outside the first OED
    certificate and probe mechanisms.

These choices should be resolved by the staged work in
[Research Roadmap and Risks](RESEARCH_ROADMAP_MULTIMODAL_DATALAKE.md), not by
expanding the first prototype preemptively.
