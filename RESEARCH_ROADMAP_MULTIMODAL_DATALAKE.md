# Research Roadmap and Risks

## Purpose

This document turns the direction in
[AUTORESEARCH_MULTIMODAL_DATALAKE.md](AUTORESEARCH_MULTIMODAL_DATALAKE.md)
into staged research work. The stages are evidence gates, not a commitment to
build the full system regardless of early results.

Concrete engineering tasks, dependencies, and per-stage acceptance checks are
tracked in the
[Ordered Development Checklist](DEVELOPMENT_CHECKLIST_MULTIMODAL_DATALAKE.md).

## Scope Gates Before Full Implementation

Four questions should be answered with small experiments:

1. Which recurring batch workload shows the clearest interaction between
   representation choice and network, storage, or preprocessing cost?
2. Which two or three physical decisions explain most of the performance
   variation on the available cluster?
3. Do operator-level measurements predict larger-run plan performance better
   than end-to-end throughput alone?
4. Is logical delivery topology controllable and beneficial, or should the
   first paper focus only on representation, placement, and transformation
   placement?

Negative answers should narrow the project before substantial infrastructure
is built.

## Stage 1: Measurement and Reproducible Baselines

Build one video pipeline and one complementary image/audio/text pipeline on
FlowMesh. Add end-to-end, operator, storage, network, and accelerator telemetry.

Deliverables:

- a reproducible workload harness;
- cold- and warm-cache baselines;
- a bottleneck breakdown; and
- a controlled study showing whether the best static representation boundary
  changes across cluster conditions.

Exit criterion: at least one workload exhibits a material, reproducible data
path bottleneck and a non-trivial plan trade-off.

## Stage 2: Representation Catalog and Minimal Data-Plane Control

Implement stable logical identities, representation lineage, version checks,
and the minimum operations needed to materialize, stage, replicate, and evict
data across object storage, NVMe, and RAM.

Deliverables:

- catalog schema and compatibility rules;
- a provenance-preserving representation graph;
- explicit data movement operations; and
- failure-safe validity and fallback behavior.

Exit criterion: two workflows can safely reuse a compatible derived
representation, and an incompatible or stochastic representation is rejected.

## Stage 3: Physical Plan and Analytical Model

Define the focused plan `P = (M, L, E)` and a small set of legal plan
transformations. Build a first-order model from sizes, transformation service
rates, capacities, and bandwidth.

Deliverables:

- physical plan schema and validator;
- structured plan transformations;
- an analytical critical-path model; and
- exhaustive enumeration on reduced problem instances.

Exit criterion: the model prunes invalid or dominated plans and predicts broad
regime changes, even if it does not rank close candidates reliably.

## Stage 4: Structured Autoresearch

Add explicit hypothesis records, decision-relevant experiment selection,
representative workload slices, learned cost correction, experiment
provenance, transition-cost accounting, and stop/refusal rules.

Deliverables:

- equal-budget comparisons against passive, random, and generic black-box
  search;
- ablations for traces, structure, experiment selection, stopping, and
  transition cost;
- convergence and uncertainty analysis; and
- validation of selected plans on longer held-out runs.

Exit criterion: structured autoresearch finds better plans or reaches the same
plan at materially lower total trial, disruption, and transition cost than the
analytical, passive, random, and generic-search baselines, while correctly
refusing experiments whose expected decision value is too low.

If this criterion is not met, retain the physical planner but remove
autoresearch from the main claim.

## Stage 5: Adaptation and Optional Scope Expansion

Add drift detection and migration-aware replanning for recurring workloads.
Only then consider richer delivery topology, concurrent mixed workloads, or
online serving.

Deliverables:

- controlled environment-change experiments;
- recovery-time and migration-cost results; and
- a decision on whether the additional workload class strengthens one coherent
  story or creates a separate project.

## Major Research Risks

### Risk 1: The Search Space Is Too Broad

Searching every storage, cache, network, and scheduling knob would create an
unfocused tuner. Keep the main plan centered on representation materialization,
placement/replication, and transformation/delivery placement. Treat secondary
knobs as fixed profiles until a measurement proves their importance.

### Risk 2: The Work Appears to Be Generic Tuning

A flat parameter vector plus final throughput would be difficult to distinguish
from database configuration tuning. Use a structured plan grammar, semantic
constraints, operator-level traces, and a generic tuner over the same plan
parameters as a baseline.

### Risk 3: The Contribution Collapses into Intermediate Caching

Recent systems already coordinate caching across preprocessing stages and
storage tiers. The project must demonstrate a distributed interaction involving
placement or replication and a transformation/network boundary. If it cannot,
the novelty claim needs substantial revision.

### Risk 4: The Work Is Not Clearly a Data-Systems Contribution

A distributed cache in front of object storage may be reviewed as an ML or
storage optimization without a new data-management abstraction. Make logical
identity, representation lineage, declarative workload, physical planning,
cost modeling, and correctness constraints first-class and evaluate them.

### Risk 5: Trial and Transition Cost Dominates Benefit

Large representations may take hours to create, and full workload runs can be
expensive. Model the reuse horizon and transition cost, use workload slices and
prior measurements, and reject plans that cannot amortize their preparation.

### Risk 6: Empirical Trials Learn Interference Instead of Plan Quality

Containers do not isolate shared storage and network resources. Begin with
sequential or resource-isolated trials, record background load, randomize trial
order, and enable parallel candidates only after ranking stability is measured.

### Risk 7: Evaluation Covers One Convenient Workload

A single model and dataset cannot support a general multimodal claim. Establish
the mechanism deeply on one workload, then validate it on at least one pipeline
with different modality and data-expansion behavior. Vary reuse, data size,
transformation cost, network, and capacity.

### Risk 8: FlowMesh Integration Becomes the Project

Runtime integration work can consume the schedule without testing the research
hypothesis. Build the smallest interfaces required for controlled experiments,
and mock or simplify non-critical operational features until the main
interaction is demonstrated.

## Decision Log to Maintain

As measurements arrive, record:

- the primary workload and why it exposes the problem;
- the selected plan variables and variables held fixed;
- the reuse horizon and cost objective;
- evidence for or against a learned residual;
- the exact novelty boundary against the closest caching and storage systems;
- which FlowMesh capabilities are reused versus newly implemented; and
- changes to claims caused by negative results.

Keeping this log separate from the direction statement prevents provisional
implementation choices from silently becoming research claims.
