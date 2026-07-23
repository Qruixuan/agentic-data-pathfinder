# Development Checklist for the Multimodal Data-Lake Optimizer

## 1. Purpose and Order

This checklist turns the research direction into an implementation sequence. Each stage has a gate: later components should not be built until the earlier evidence or system invariant holds.

```text
D0 environment and interfaces
  -> D1 reproducible baseline and telemetry
  -> D2 representation catalog and compatibility
  -> D3 per-node data plane
  -> D4 declarative physical plans A/B/C
  -> G0 interaction study
  -> D5 analytical planner
  -> D6 experiment manager
  -> D7 structured Autoresearch
  -> D8 online adaptation and hardening
  -> D9 full evaluation and reproducibility
```

The critical principle is to prove the physical-planning problem before implementing a sophisticated optimizer or learned Autoresearch loop.

---

## D0. Environment, FlowMesh Interfaces, and Repository Boundaries

### Goal

Replace architectural assumptions with verified interfaces and establish a minimal controller/agent skeleton.

### Tasks

- [ ] Inventory cluster nodes, CPU/GPU resources, RAM, local NVMe, network links, and durable object storage.
- [ ] Measure or document the permissions available to the controller and workers.
- [ ] Verify whether FlowMesh can pass `plan_id`, `plan_epoch`, and a local agent endpoint to each task.
- [ ] Verify whether FlowMesh reports task start, finish, retry, and cancellation.
- [ ] Verify whether CPU preprocessing tasks can be placed on selected nodes.
- [ ] Verify whether GPU tasks can call a local Data Agent or sidecar.
- [ ] Select the first control RPC, agent-to-agent data-transfer, and telemetry protocols.
- [ ] Select a metadata store with transactions or compare-and-swap support.
- [ ] Fix the first representation-shard policy instead of making shard size an optimization variable.
- [ ] Define a uniform `run_id / plan_epoch / trial_id / shard_id` naming convention.
- [ ] Record unverified assumptions and a fallback implementation for each one.

### Recommended Code Boundaries

```text
controller/
  catalog/
  planner/
  experiment_manager/
  autoresearch/

agent/
  replica_manager/
  transfer/
  transform_executor/
  telemetry/

common/
  representation/
  physical_plan/
  protocol/

adapters/
  flowmesh/
  object_store/

workloads/
  video/

experiments/
  configs/
  runners/
  analysis/
```

Names may change, but the controller, agent, shared plan protocol, adapters, and workload harness should remain separate.

### Deliverables

- [ ] `ENVIRONMENT_AND_INTERFACES.md` describing verified interfaces and constraints.
- [ ] Minimal controller and agent processes that register and exchange health checks.
- [ ] A versioned experiment configuration describing nodes, tiers, bandwidth limits, and workload.

### Gate D0

Proceed only when:

- at least two nodes can participate in data transfer;
- FlowMesh tasks receive stable plan and trial identities;
- object storage and local NVMe are accessible through the test harness; and
- every critical assumption is either verified or has an explicit fallback.

---

## D1. Reproducible Origin Data Path and Telemetry

### Goal

Establish a correct baseline without an optimizer:

```text
object store -> consumer-node CPU -> transform -> GPU or dummy consumer
```

### Tasks

- [ ] Select one video dataset or controlled generator.
- [ ] Fix one transform chain: `raw -> decode -> sample -> resize/tensor`.
- [ ] Define a fixed shard list and deterministic workload slice.
- [ ] Implement the origin-streaming baseline.
- [ ] Separate cold-cache and warm-cache runs.
- [ ] Record operator input/output bytes, service time, and queueing time.
- [ ] Record object-store requests, bytes, latency, and throughput.
- [ ] Record cross-node transfer bytes and time.
- [ ] Record consumer stalls, effective sample goodput, and makespan.
- [ ] Record CPU, GPU, NVMe, memory, and network utilization.
- [ ] Bind all measurements to workload, dataset, code version, run, and random seed.
- [ ] Repeat runs and quantify variance.

### First Required Measurements

- [ ] Mean and tail raw-video shard size.
- [ ] Data expansion ratio after decode.
- [ ] Data contraction ratio after sampling or filtering.
- [ ] Transform throughput on data-node and consumer-local CPUs.
- [ ] Effective read throughput from origin, peer, and local NVMe.
- [ ] Consumer data demand per second.
- [ ] Whether the pipeline is data-, CPU-, or GPU-bound.

### Deliverables

- [ ] A one-command reproducible baseline.
- [ ] An end-to-end bottleneck decomposition.
- [ ] A trace schema and trace analysis/viewer.
- [ ] Confidence intervals or distributions from at least three repeated runs.

### Gate D1

- A reproducible data-path bottleneck must exist.
- Operator and transfer measurements should approximately explain makespan.
- If the workload is always GPU-bound, change the workload or parameters before building a planner.

---

## D2. Representation Catalog, Identity, and Compatibility

### Goal

Answer precisely:

> What representation is this shard, how was it produced, where does it exist, and may the current consumer reuse it?

### Tasks

- [ ] Implement `LogicalDataset` and `dataset_version`.
- [ ] Implement `RepresentationVersion` and the parent representation DAG.
- [ ] Implement a `RepresentationShard` manifest.
- [ ] Implement `Replica(node, tier, state, checksum, lease)`.
- [ ] Implement a transformation fingerprint containing:
  - [ ] input versions;
  - [ ] operator code version;
  - [ ] parameters;
  - [ ] randomness contract;
  - [ ] model or tokenizer version; and
  - [ ] output schema and quality contract.
- [ ] Implement replica state transitions:
  `ABSENT -> RESERVED -> BUILDING/TRANSFERRING -> VERIFYING -> VALID`.
- [ ] Implement temporary writes, checksum/manifest validation, and atomic publication.
- [ ] Implement an independent compatibility validator.
- [ ] Propagate invalidation after upstream version changes.
- [ ] Recover the catalog after restart and scan for orphan replicas.

### Tests

- [ ] Two workflows with the same fingerprint discover and reuse one representation.
- [ ] Reuse is rejected for incompatible parameters, code, or model versions.
- [ ] A random transformation cannot be reused across runs without a compatible seed contract.
- [ ] A partial write or failed verification never becomes `VALID`.
- [ ] Controller restart neither loses valid replicas nor accepts temporary files as valid.

### Gate D2

Two workflows safely share one deterministic derived shard, and every constructed incompatibility case is rejected by the validator.

---

## D3. Per-Node Data Agent and Minimal Data Plane

### Goal

Implement the minimum primitives required to execute `M/L/E`.

### Agent Infrastructure

- [ ] Agent registration, heartbeat, and capability reports.
- [ ] Local replica index.
- [ ] Capacity reservation, leases, pins, and reader references.
- [ ] Idempotency keys: `(plan_epoch, operation_id)`.
- [ ] Operation-status queries and retries.

### Data Operations

- [ ] `ensure_replica`
- [ ] `run_transform`
- [ ] `replicate`
- [ ] `serve`
- [ ] `stage`
- [ ] `pin`
- [ ] `evict`
- [ ] `invalidate`

### Required Paths

- [ ] Object store to agent.
- [ ] Local NVMe to local consumer.
- [ ] Peer agent to consumer agent.
- [ ] Object store to data-node transform to NVMe.
- [ ] Peer transfer to consumer-local transform.
- [ ] Origin-read fallback.

### Safety

- [ ] Failed transfers or transforms never publish partial artifacts.
- [ ] A shard with active readers cannot be evicted.
- [ ] Agents recover local state from files and catalog records after restart.
- [ ] Eviction never affects the durable source.

### Gate D3

The same logical request is served correctly from origin, local NVMe, and a peer replica. Transform outputs are validated, reusable, and recoverable, and each path emits attributable telemetry.

---

## D4. Physical Plan, Compiler, and Three Static Paths

### Goal

Control the physical system correctly with a declarative `P = (M, L, E)` before adding an automatic optimizer.

### Tasks

- [ ] Implement a `PhysicalPlan` schema.
- [ ] Implement plan validation.
- [ ] Implement a plan registry and monotonically increasing `plan_epoch`.
- [ ] Compile a Transition Plan from `P_old` to `P_new`.
- [ ] Implement runtime bindings.
- [ ] Implement a telemetry specification per plan.
- [ ] Implement an explicit rollback plan.
- [ ] Bind admitted tasks immutably to one epoch.
- [ ] Implement `PREPARING -> CANARY -> ACTIVE -> DRAINING`.
- [ ] Ensure the controller is not on the per-sample critical path.

### Mandatory Static Plans

```text
Plan A
origin raw -> consumer CPU transform -> GPU

Plan B
origin raw -> data-node transform -> materialized tensor on NVMe
           -> consumer/GPU

Plan C
origin raw -> data-node decode/sample -> materialized parent on NVMe
           -> consumer-local final transform -> GPU
```

### Tests

- [ ] Manually submitted A/B/C plans produce their intended byte paths.
- [ ] Switching A to B includes all materialization work.
- [ ] Switching B to C reuses compatible parents or explicitly records why reuse is illegal.
- [ ] Plan activation is atomic.
- [ ] Failures fall back to Plan A's origin-read path.
- [ ] Old replicas are released only after old-epoch readers drain.

### Gate D4

Plans A/B/C produce semantically equivalent outputs from the same logical inputs. Traces distinguish representation, node, tier, transform location, and transition cost.

---

## G0. Interaction Study: Go/No-Go Gate Before the Optimizer

### Goal

Determine whether joint `M/L/E` decisions genuinely outperform independent policies. This is the project's most important early research gate.

### Tasks

- [ ] Enumerate or manually construct a small legal plan space.
- [ ] Vary:
  - [ ] network bandwidth;
  - [ ] storage bandwidth;
  - [ ] data-node CPU;
  - [ ] consumer-local CPU;
  - [ ] NVMe and RAM capacity;
  - [ ] reuse horizon;
  - [ ] concurrent consumers or jobs; and
  - [ ] representation expansion and contraction ratios.
- [ ] Find a regime where early transformation wins.
- [ ] Find a regime where late transformation wins.
- [ ] Find a regime where a shared materialized parent wins.
- [ ] Implement strong independent planners in at least two fixed orders, such as `M -> L -> E` and `E -> L -> M`.
- [ ] Compare independent plans with the global optimum.
- [ ] Compute break-even reuse counts by regime.
- [ ] Include transition costs in every comparison.

### Gate G0

Continue to planner development only if:

1. different plans win in at least two regimes;
2. a strong independent optimizer deviates materially from the global best in at least one realistic, reproducible regime;
3. the gap comes from representation, placement, and transformation-boundary interaction rather than weak LRU or thread-count settings; and
4. the conclusion survives transition-cost accounting.

If condition 2 fails, narrow or redefine the joint-planning claim. If condition 1 fails, stop expanding the system.

---

## D5. Transition-Aware Analytical Planner

### Goal

After G0 proves the problem exists, implement the first automatic planner without a learned model.

### Tasks

- [ ] Build representation, operator, and resource profiles from the catalog and telemetry.
- [ ] Generate legal candidates.
- [ ] Apply semantic pruning.
- [ ] Apply dominated-plan pruning.
- [ ] Model transforms, reads, transfers, queues, and the critical path.
- [ ] Model materialization, replication, migration, warm-up, adoption, and eviction.
- [ ] Accept an explicit reuse horizon.
- [ ] Compute break-even reuse counts.
- [ ] Rank plans and expose uncertain terms and confidence intervals.
- [ ] Implement exhaustive search on reduced instances.
- [ ] Explain each selected plan using dominant benefits, bottlenecks, and transition amortization.

### Gate D5

- The planner approaches the exhaustive oracle on reduced instances.
- It predicts the main regime changes observed in G0.
- It gives interpretable pruning reasons for clearly inferior plans.
- When close plans are ranked incorrectly, it identifies the uncertain model term that D6/D7 should measure.

---

## D6. Experiment Manager and Canary Child Plans

### Goal

Measure candidate plans safely and reproducibly without allowing Autoresearch to mutate the active plan directly.

### Tasks

- [ ] Implement child `plan_epoch` creation.
- [ ] Select deterministic shard and consumer subsets.
- [ ] Implement a trial namespace, capacity reservation, and budget.
- [ ] Compile the physical delta between parent and child.
- [ ] Support sequential or resource-isolated trials.
- [ ] Record shared-resource background load.
- [ ] Validate trial correctness.
- [ ] Implement promote, rollback, and cleanup.
- [ ] Classify reusable, stranded, invalid, and disruptive trial work.
- [ ] Store a complete `ResearchIteration` provenance record.
- [ ] Implement passive, random, and exhaustive-small-instance experiment baselines.

### Gate D6

A candidate completes a canary without corrupting normal jobs, produces replayable evidence, and can be promoted or rolled back safely. A failed trial cannot damage the active plan or publish an invalid representation.

---

## D7. Structured Autoresearch

### Goal

Choose the next most valuable experiment, rather than merely selecting the currently cheapest predicted plan.

### Minimum Version

- [ ] Define triggers:
  - [ ] overlapping confidence intervals among top plans;
  - [ ] persistent prediction errors;
  - [ ] workload or version changes; and
  - [ ] substantial resource-state drift.
- [ ] Generate structured hypotheses from plan differences.
- [ ] Map uncertain terms to executable interventions:
  - [ ] operator microbenchmark;
  - [ ] sampled representation materialization;
  - [ ] transfer benchmark; and
  - [ ] workload-slice canary.
- [ ] Implement an initial experiment-value score.
- [ ] Prefer shared measurements that discriminate among several candidates.
- [ ] Update the relevant cost-model component and plan confidence.
- [ ] Implement `deploy / continue / refuse / stop`.
- [ ] Record a structured stopping reason.
- [ ] Model reusable artifacts created by each trial.

### Defer Until Evidence Requires It

- [ ] `[optional]` Learned residual model, only for systematic contention or skew errors.
- [ ] `[optional]` LLM hypothesis proposals, only as suggestions within the valid plan space.
- [ ] `[optional]` Parallel experiments, only after sequential ranking is stable.

### Required Baselines

- [ ] Analytical-only planner.
- [ ] Passive traces.
- [ ] Random experiment selection.
- [ ] Generic plan-level black-box tuner.
- [ ] Generic component optimal design without plan differences.
- [ ] UDO-style heavy/light trial ordering.
- [ ] Structured search with end-to-end feedback only.
- [ ] Exhaustive oracle on reduced instances.

### Gate D7

Under equal measurement, disruption, and transition budgets, Autoresearch finds a better plan or reaches the same plan at materially lower total cost, while correctly refusing low-value experiments. Otherwise retain the analytical planner and remove Autoresearch from the main contribution.

---

## D8. Online Adaptation, Migration, and Hardening

### Goal

Replan after workload or resource changes without oscillation or damage to running tasks.

### Tasks

- [ ] Detect sustained prediction error and resource drift.
- [ ] Trigger on workload, version, or reuse-horizon changes.
- [ ] Add hysteresis and a minimum stable window.
- [ ] Implement migration-aware replanning.
- [ ] Recover from controller crashes.
- [ ] Handle agent disconnect and rejoin.
- [ ] Reject stale plan epochs.
- [ ] Reconcile replicas and clean orphan artifacts.
- [ ] Provide a reduced-overhead telemetry mode.
- [ ] Back up and restore the catalog and Evidence Store.
- [ ] Stress-test multi-job capacity contention and externalities.

### Gate D8

After controlled bandwidth, CPU, capacity, or reuse changes, the system returns to an appropriate plan within budget. Short-lived noise does not repeatedly create and migrate large representations.

---

## D9. Full Evaluation, Reproducibility, and Research Delivery

### Goal

Turn the implementation into evidence that supports or rejects the research claims.

### Tasks

- [ ] Complete one primary video workload.
- [ ] Add one image, audio, or embedding workload with different expansion behavior.
- [ ] Implement all required data-path baselines.
- [ ] Implement the strong independent `M/L/E` baseline.
- [ ] Implement the optimizer and Autoresearch baselines.
- [ ] Complete required ablations.
- [ ] Report cold-start, steady-state, and horizon-amortized results.
- [ ] Report trial, disruption, transition, and telemetry overheads.
- [ ] Report negative regimes where joint planning or Autoresearch is unnecessary.
- [ ] Freeze dataset versions, configuration, seeds, cluster allocation, and background load.
- [ ] Provide one-command runners and analysis notebooks or scripts.
- [ ] Validate selected plans with held-out long runs.
- [ ] Produce architecture, interaction, convergence, and breakdown figures.

### Final Success Conditions

- [ ] Different physical plans have stable, explainable winning regimes.
- [ ] Joint `M/L/E` beats a strong independent optimizer in target regimes.
- [ ] Transition-aware objectives change the plan and improve horizon-wide cost.
- [ ] Structured Autoresearch beats passive, random, generic optimal-design, and black-box baselines at equal budget.
- [ ] Benefits remain after charging materialization, migration, trials, disruption, and telemetry.

---

## 2. Priority Summary

### P0: Prove the research problem

- [ ] D0 environment and interfaces.
- [ ] D1 baseline and telemetry.
- [ ] D2 catalog.
- [ ] D3 Data Agent.
- [ ] D4 static Plans A/B/C.
- [ ] G0 interaction study.

Do not implement a complex planner or Autoresearch loop before P0 is complete.

### P1: Build the paper's core system

- [ ] D5 transition-aware analytical planner.
- [ ] D6 experiment manager.
- [ ] D7 Structured Autoresearch, only if required by evidence.

### P2: Extend and harden

- [ ] D8 online adaptation.
- [ ] D9 workload breadth, reproducibility, and engineering hardening.

## 3. Recommended Immediate Next Steps

If implementation has not begun:

1. Complete the D0 FlowMesh, object-store, and cluster interface table.
2. Build the D1 single-path video baseline.
3. Measure raw, decoded, sampled, and tensor sizes and transformation costs before implementing the catalog or optimizer.
4. Confirm that a meaningful data-path bottleneck exists, then start D2.

The first research-critical code is not the Autoresearch controller. It is a physical system that can replay Plans A/B/C on the same workload and account completely for materialization, transfer, and transition costs.
