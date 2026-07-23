# Working System Design: Autoresearch-Driven Multimodal Physical Data Path Optimizer

## Status and Boundary

This document contains a working system design for the research direction in
[AUTORESEARCH_MULTIMODAL_DATALAKE.md](AUTORESEARCH_MULTIMODAL_DATALAKE.md).
Its components and interfaces are provisional. The research claim should not
depend on implementing every mechanism described here.

The initial system is an optimization and management layer over durable object
storage and an existing workflow runtime. It is not itself a new object store.

## Design Goals

The prototype should:

- identify logical objects independently from their physical representations;
- validate reuse using lineage, determinism, and version compatibility;
- express representation, placement, and transformation/delivery choices in a
  structured physical plan;
- turn uncertain plan choices into explicit, testable hypotheses;
- choose informative trials under measurement, disruption, and transition
  budgets;
- execute and measure alternative plans without changing workload semantics;
- charge plans for materialization and migration work; and
- expose enough control to test joint planning against strong layer-local
  baselines.

## System in One Sentence

The physical system is a **central planning and experiment-control service plus
a data agent on every participating worker**. The controller compiles `M/L/E`
plans into replica, transformation, and routing commands; the agents execute
those commands over an existing object store, node-local NVMe/RAM, CPU
preprocessing workers, and AI consumers.

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
- consumers are recurring FlowMesh batch tasks, initially on GPU worker nodes;
- the plan controls representation materialization, NVMe/RAM replicas, CPU
  transformation placement, and the selected delivery source;
- cache eviction uses a fixed local policy constrained by planner-issued
  reservations and pins; and
- rack relays, in-network processing, GPU-memory caching, new storage formats,
  and provider-internal object-store compute remain outside the MVP.

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
  | Workload adapter / API                                               |
  |        |                                                             |
  |        v                                                             |
  | Catalog + Plan registry ---> Planner + Validator                     |
  |        ^                         |                                   |
  |        |                         v                                   |
  | Evidence store <------ Autoresearch controller                       |
  |        ^                         |                                   |
  |        +--------------- Experiment manager                           |
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
| Workload adapter | Control node, integrated with FlowMesh | Workload/DAG versions | Converts a recurring workflow and target representation into planner input |
| Representation catalog | Control node; one logical authority in the MVP | Logical objects, representation DAG, shards, replicas, compatibility | Defines identity and validates reuse |
| Plan registry and validator | Control node | Versioned plans, plan epochs, validation results | Stores `P=(M,L,E)` and rejects illegal or infeasible plans |
| Physical planner | Control node | Cost-model parameters and candidate rankings | Produces transition-aware candidate plans |
| Autoresearch controller | Control node | Hypotheses, uncertainty, experiment decisions | Selects the next useful experiment or stops |
| Experiment manager | Control node | Trial leases, budgets, results, rollback status | Creates child plans and runs microbenchmarks/canaries |
| Evidence store | Control node plus durable trace files | Profiles, traces, provenance, model versions | Supplies reproducible evidence for planning |
| Data agent | Every data/consumer node | Local replica index and active leases | Executes replica, transform, serve, pin, and eviction commands |
| Transform executor | CPU/data node or consumer node | No authoritative metadata | Runs a fingerprinted representation-changing operator |
| Consumer adapter | FlowMesh worker process or sidecar | Active plan binding | Resolves each logical shard through the selected physical path |
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
- determinism, cacheability, and reuse scope.

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

## Distributed Data Plane

The MVP data plane spans remote object storage, node-local NVMe, and host
memory. A node-local agent reports contents, capacity, observed transfer rates,
and transformation throughput. Accelerator-memory caching can be considered
later but is not a first-version plan variable.

Each data agent contains four required subsystems:

1. **Replica manager:** reserves capacity, maintains the local shard index,
   applies pins/leases, and performs safe eviction.
2. **Transfer server/client:** reads from the object store or peers and streams
   a selected representation shard to the next hop.
3. **Transform executor:** runs a declared, versioned transformation and
   produces a manifest for its output shard.
4. **Telemetry exporter:** attributes bytes, queueing, service time, cache
   state, and failures to a plan, trial, representation, shard, and operator.

The initial control interface should support explicit operations:

- `ensure_replica(representation_shard, node, tier, lease)`;
- `run_transform(parent_shard, output_representation, executor_node)`;
- `replicate(representation_shard, source, destination, tier)`;
- `serve(representation_shard, source, consumer, route)`;
- `stage(representation_shard, consumer, deadline)`;
- `pin(representation_shard, node, tier, lifetime)`;
- `evict(representation_shard, node, tier)`; and
- `invalidate(representation, reason)`.

These operations must be idempotent where possible and produce provenance
events. Failed or partial replicas must not be advertised as valid inputs.

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

The research abstraction uses a focused plan `P = (M, L, E)`:

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
  reuse_horizon
  materializations[]           # M
  replica_placements[]         # L
  transformation_assignments[] # E
  delivery_bindings[]          # E
  capacity_reservations[]
  pin_and_eviction_directives[]
  staging_schedule[]
  validity_constraints[]
  expected_transition_cost
  fallback_plan_id
}
```

The low-level fields are not intended to become independent knobs. For
example, a cache directive realizes a placement decision, and a staging
schedule realizes the selected delivery path.

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
   eviction operations needed to move from `P_old` to `P_new`.
2. **Runtime bindings:** for every `(consumer stage, logical shard)`, an ordered
   source, representation, transform chain, and fallback.
3. **Telemetry specification:** the counters and spans required to evaluate the
   plan or experiment hypothesis.
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
  workload_version
  plan_epoch
  consumer_stage
  logical_dataset
  shard_id
  required_representation
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
  fallback_sources[]
}
```

Resolution follows a predictable sequence:

1. verify that the task's plan epoch and representation requirement match;
2. use a valid local replica if it is the selected source;
3. otherwise fetch from the selected peer or object store;
4. run any assigned local transformation and verify its output;
5. deliver the required representation to the consumer;
6. emit telemetry under the same plan/trial/request identifiers; and
7. fall back to the durable origin path if the selected replica or transform
   worker fails.

The control service is not contacted for every data chunk. Runtime bindings are
distributed and cached when a task starts; agents report asynchronous state and
telemetry. This avoids placing the planner or catalog on the per-sample critical
path.

## End-to-End Physical Data-Path Example

Assume a recurring video workload requires resized frame tensors. The
representation graph is:

```text
raw MP4
  -> decoded frames
  -> sampled frames
  -> resized tensor
```

The system can execute three materially different paths:

```text
Plan A: object store --raw MP4--> GPU node CPU --decode/sample/resize--> GPU

Plan B: object store --raw MP4--> data-node CPU --decode/sample/resize-->
        data-node NVMe --tensor shard--> GPU node

Plan C: object store --raw MP4--> data-node CPU --decode/sample-->
        sampled-frame shard on data-node NVMe --sampled frames-->
        GPU node CPU --resize--> GPU
```

In Plan A, `M` selects no reusable intermediate, `L` selects only the durable
origin, and `E` places all transformations at the consumer. In Plan B, `M`
materializes resized tensors, `L` stores them on selected data-node NVMe
replicas, and `E` transforms before the network hop. Plan C materializes a
shared parent representation and places the last transformation near each
consumer.

For a selected Plan C, execution is:

1. the compiler reserves NVMe on a data node;
2. the data agent reads raw MP4 shards from the object store;
3. the transform executor decodes and samples them into temporary output;
4. the agent verifies the manifest and publishes `VALID` sampled-frame shards;
5. the controller activates the new plan epoch after the canary succeeds;
6. GPU tasks receive bindings that select those shards from the data-node
   transfer server;
7. consumer-local CPU performs resize and feeds the GPU; and
8. traces record the one-time materialization, network bytes, local resize
   time, consumer stalls, and later reuse.

This example is the physical manifestation of the research question: the
system must decide whether avoiding repeated decode is worth storing and
transferring an expanded representation, and that answer changes with reuse
horizon, network contention, and CPU availability.

## Plan Validation

Validation occurs before a trial allocates resources or creates derived data.
The validator rejects plans that:

- reuse stale or incompatible representations;
- treat a non-deterministic output as generally reusable;
- exceed hard storage, memory, or monetary budgets;
- assign an operator to incompatible hardware;
- request an unavailable delivery mechanism; or
- violate workload ordering or data-dependency constraints.

The validator should also estimate the materialization and migration work of a
candidate. A plan that is semantically legal may still be pruned when its
transition cost cannot be amortized within the declared reuse horizon.

## Structural Cost Model

The first-order model should use directly measurable quantities:

- representation and chunk sizes;
- transformation service rates by hardware type;
- storage-tier bandwidth and latency;
- link bandwidth and nominal contention state;
- cache and staging capacity;
- expected access frequency and reuse horizon; and
- materialization, replication, and eviction costs.

It should model the critical path of the workload DAG rather than simply sum
all operator times. The initial model can be deliberately approximate, because
its primary jobs are to reject dominated plans, provide interpretable priors,
and identify uncertain comparisons worth measuring.

## Autoresearch Loop: Trace-Guided Experimentation

```text
observe
  -> formulate a plan hypothesis
  -> validate candidates
  -> select a decision-relevant experiment
  -> execute and measure
  -> update evidence and uncertainty
  -> deploy, continue, or stop
```

The controller does not ask only which plan has the lowest estimated cost. It
also asks which safe experiment, if any, is worth running because its result
could change a consequential plan decision. Every iteration emits a structured
research record:

```text
ResearchIteration {
  trigger
  current_plan
  hypothesis
  candidate_plans
  uncertain_model_terms
  selected_experiment
  expected_decision_value
  trial_and_transition_budget
  observations
  model_update
  decision
  stopping_reason
}
```

### Observation and Hypothesis Formation

An iteration starts only when the controller observes a decision-relevant
trigger: close or unstable candidate rankings, persistent model error, workload
or representation-version change, resource drift, or a violated objective.

The hypothesis must name a structured cause and an expected consequence. For
example:

```text
Because decode expands video by 18x and the shared link is saturated,
materializing sampled frames near storage will reduce horizon-wide makespan
despite its one-time materialization cost.
```

This form makes the experiment and its success criterion inspectable. A vague
hypothesis such as "try more cache" is not sufficient.

### Candidate Proposal and Validation

Each iteration proposes a small batch of candidates by applying legal plan
transformations to the current plan and selected frontier plans. Candidate
selection should balance estimated improvement, uncertainty, trial cost, and
coverage of poorly modeled interactions.

An LLM may summarize traces or propose a structured transformation, but the
system must validate the resulting plan. Correctness and the core search
procedure must not depend on unconstrained natural-language output.

### Experiment Selection

The experiment selector chooses among operator microbenchmarks, representation
materialization samples, workload slices, and plan canaries. Its objective is
to reduce uncertainty that can change the deployed-plan decision, not to
maximize trace volume.

A first implementation can rank experiments by:

```text
experiment_value =
    probability_the_result_changes_the_plan
  * consequence_of_choosing_the_wrong_plan
  - trial_cost
  - disruption_cost
  - non_reusable_transition_cost
```

Experiments that only refine an insensitive model parameter should be skipped.
When several plans share an uncertain operator or link, the controller should
measure that shared component instead of benchmarking every complete plan.

### Trial Execution and Provenance

Candidates can be evaluated using representative workload slices, pilot runs,
or canary executions. Every result must include transition cost or explicitly
state which previously materialized artifacts it reuses.

A candidate trial is implemented as a **child plan epoch**, not an ad hoc
mutation of the active plan:

1. choose a deterministic shard subset and consumer subset;
2. compile only the physical delta from the active parent plan;
3. reserve trial capacity and assign a separate trial namespace;
4. prepare and verify candidate replicas without changing normal task bindings;
5. bind only canary tasks to the child epoch;
6. collect the hypothesis-specific telemetry plus end-to-end correctness;
7. either promote the child plan, request another experiment, or roll it back;
   and
8. adopt semantically valid artifacts into the catalog or let trial-only leases
   expire.

The trial record distinguishes reusable transition work from discarded work.
For example, a sampled-frame shard generated by an unsuccessful placement
experiment may remain useful to another legal plan, while a shard produced
under an incompatible transform fingerprint must not be adopted.

Parallel trials are not isolated merely because they run in different
containers. They may still share object-store bandwidth, links, and disks. The
experiment manager must choose one of three explicit policies:

1. allocate isolated resources;
2. co-schedule only candidates whose resource footprints do not conflict; or
3. record concurrent load and model interference.

The first prototype should prefer sequential or resource-isolated trials until
the ranking is reproducible.

### Evidence Update and Plan Selection

A promising formulation is:

```text
estimated_cost = structural_model(plan, state)
               + learned_residual(plan, trace, state)
```

Operator-level traces provide supervision for transformation, queueing, cache,
and transfer effects. The learned residual should remain secondary to plan
semantics: it predicts model error but does not decide whether reuse is legal.

The optimizer may maintain a Pareto frontier over completion time, resource
usage, storage footprint, and monetary cost. A deployed plan is replaced only
when expected benefit exceeds migration cost and uncertainty. Periodic probes
or state changes can trigger re-optimization when workload or infrastructure
drifts.

### Stopping, Refusal, and Reopening

An autoresearch iteration stops when one of the following holds:

1. one legal plan is robustly preferred after including horizon-wide transition
   cost;
2. the expected decision value of every remaining experiment is below its
   total cost;
3. the measurement or disruption budget is exhausted;
4. no candidate can be tested safely; or
5. current evidence shows that the existing plan should be retained.

Stopping without changing the plan is a valid result. The evidence record is
reopened only after a material workload, representation, infrastructure, or
prediction-error trigger; short-lived noise should not repeatedly materialize
or migrate large representations.

## Telemetry and Provenance

Each trial should be linked to the exact:

- workload and DAG version;
- dataset and representation versions;
- physical plan;
- transformation code and parameters;
- cluster allocation and observed background load; and
- start/end time and random seeds.

Useful measurements include:

- per-operator service and queueing time;
- bytes transferred by source, destination, tier, and representation;
- link and object-store utilization;
- cache hits by representation rather than only by file;
- staging lead time and unused staged bytes;
- CPU/GPU utilization and accelerator starvation;
- request count and throttling at object storage; and
- end-to-end valid-sample goodput or workflow makespan.

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
   requirements.
2. **Plan admission:** the planner returns a `plan_id`, `plan_epoch`, task
   placement constraints, and any preparation barrier.
3. **Task dispatch:** FlowMesh passes the immutable plan epoch and local data
   agent endpoint to each task.
4. **Data access:** the task's consumer adapter asks the local agent for logical
   representation shards; it does not embed physical object paths in the DAG.
5. **Lifecycle reporting:** FlowMesh reports task start, finish, retry, and
   cancellation so leases, makespan, and transition cost can be attributed
   correctly.
6. **Fallback:** if the optimizer or catalog is temporarily unavailable, an
   already admitted task continues using its cached binding; a new task may use
   a conservative origin-read plan.

The project expects to add:

- a representation and lineage catalog;
- chunk and replica metadata;
- multi-tier cache agents;
- explicit materialization and staging operations;
- network and data-path monitoring;
- a physical plan schema and compiler;
- fine-grained data-path telemetry; and
- an experiment manager and optimizer.

The intended responsibility split is:

```text
multimodal data-plane extensions
        +
FlowMesh control and execution plane
        +
physical data path planner and autoresearch controller
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
   planner, and experiment API;
2. one data-agent daemon on every CPU/data and GPU/consumer node;
3. immutable representation-shard manifests and one deterministic video
   transform chain;
4. origin, local NVMe, host RAM, and peer-transfer paths;
5. the three physical plans in the video example;
6. canary child epochs and rollback to direct origin reads; and
7. per-plan/per-representation operator, storage, network, and transition
   telemetry.

The initial implementation does not require a learned model. First, manually
enumerate the small plan space and verify that Plans A, B, and C win in
different controlled regimes. Add autoresearch experiment selection only after
the physical system produces reproducible cost and trace differences.

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

These choices should be resolved by the staged work in
[Research Roadmap and Risks](RESEARCH_ROADMAP_MULTIMODAL_DATALAKE.md), not by
expanding the first prototype preemptively.
