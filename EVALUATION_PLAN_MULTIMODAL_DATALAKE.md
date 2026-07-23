# Evaluation Plan: Autoresearch-Driven Multimodal Physical Data Path Optimizer

## Purpose

This document describes how to test the hypotheses in
[AUTORESEARCH_MULTIMODAL_DATALAKE.md](AUTORESEARCH_MULTIMODAL_DATALAKE.md).
It is a working evaluation plan rather than part of the direction statement.

The central experimental obligation is not simply to show that caching helps.
It is to show when jointly choosing representation, distributed placement, and
transformation/delivery placement improves an end-to-end workload over strong
systems that optimize those dimensions separately.

## Evaluation Principles

1. Establish the data-path bottleneck before evaluating an optimizer.
2. Compare methods with the same legal plan space and trial budget wherever
   possible.
3. Include materialization and migration in end-to-end cost.
4. Separate steady-state benefit from amortized benefit over a declared reuse
   horizon.
5. Measure candidate interference rather than assuming container isolation.
6. Use the smallest workload matrix that isolates the claimed interactions;
   add breadth only after the core causal result is clear.

## Research Questions

### RQ0: Is There a Material Optimization Opportunity?

Do representative recurring multimodal pipelines stall on storage, network, or
preprocessing, and does the best physical plan change as transformation cost,
network bandwidth, cache capacity, placement, or reuse horizon changes?

This is a prerequisite. Without a regime change or a meaningful performance
gap between plans, a self-driving joint optimizer is not justified.

### RQ1: End-to-End Benefit

Does joint physical data path optimization improve amortized workflow
makespan, valid-sample goodput, accelerator utilization, and resource cost over
strong streaming, caching, and locality-aware baselines?

### RQ2: Value of Joint Planning

How much is lost when representation materialization, placement, and
transformation/delivery are optimized independently? Which pairs of decisions
create the most important interactions?

### RQ3: Autoresearch Experiment Efficiency

How many trials, processed samples, and wall-clock minutes are needed to reach
a strong plan? Does structured autoresearch select more decision-relevant
experiments and reach the same or a better plan at lower total trial,
disruption, and transition cost than analytical-only, passive, random, or
generic black-box methods? Does it stop when more evidence is not worth its
cost?

### RQ4: Autoresearch Adaptation and Evidence Transfer

Can measurements be reused across dataset scales, modality pipelines, hardware
types, and cluster conditions? How quickly does the optimizer recover after a
change in workload mix, network bandwidth, cache capacity, transformation
cost, or reuse horizon?

### RQ5: Overhead and Robustness

What are the costs of catalog operations, telemetry, trials, materialization,
plan transitions, and drift detection? How stable are plan rankings in the
presence of shared-resource contention?

## Candidate Workloads

Start with two pipelines selected for complementary data-expansion behavior:

1. **Video-text pipeline:** compressed video -> decode -> sample -> resize ->
   tensor or embedding. This is likely to expose a strong network-versus-CPU
   trade-off because decoding expands data.
2. **Image-text or audio-text pipeline:** raw objects -> deterministic
   preprocessing -> tensor or embedding. Choose the pipeline whose early
   measurements show a reusable intermediate with a different size/cost ratio
   from video.

A third pipeline and a concurrent mixed workload should be added only after the
first two reproduce the central interaction. Candidate additions are
image-text contrastive training and audio-text preprocessing/training.

Each pipeline should have:

- a deterministic reusable prefix and, where realistic, a stochastic suffix;
- several epochs or repeated invocations;
- a dataset larger than aggregate cache capacity;
- at least two useful representation boundaries; and
- a configuration in which data delivery measurably stalls the consumer.

## Testbed

The initial testbed should include:

- S3-compatible remote object storage;
- multiple FlowMesh compute workers;
- heterogeneous CPU/GPU resources where available;
- RAM and node-local NVMe tiers;
- controllable or emulated link bandwidth and latency; and
- datasets that exceed aggregate fast-tier capacity.

The evaluation matrix should vary one factor at a time around a representative
default:

- network bandwidth or contention;
- preprocessing service rate;
- fast-tier capacity;
- number and placement of consumers;
- representation reuse frequency; and
- number of repeated workload invocations.

The final evaluation should contain at least one heterogeneous cluster
configuration. A small homogeneous cluster can validate implementation, but by
itself it does not exercise placement or transformation-boundary decisions.

## Baselines

### Data-Path Baselines

- remote streaming with no application-managed cache;
- node-local LRU or LFU caching;
- static full-dataset staging when capacity permits;
- locality-aware task placement;
- a strong intermediate-representation caching policy inspired by systems such
  as Cachew or HyCache;
- independent per-layer optimization with the same individual decision
  mechanisms as the joint optimizer; and
- a hand-tuned plan using expert knowledge of the testbed.

If source code or a faithful reimplementation of an adjacent system is not
available, state that limitation and compare against the strongest reproducible
policy rather than claiming a direct system-to-system result.

### Optimizer Baselines

- analytical model without empirical correction;
- passive trace collection without active experiment selection;
- random search over the same legal plan space;
- a generic black-box tuner over the same encoded variables;
- structured search using only end-to-end outcomes;
- structured search using operator-level traces but random experiment
  selection;
- structured autoresearch with decision-relevant experiment selection and
  stopping; and
- exhaustive enumeration on reduced instances as a small-scale oracle.

All methods should begin from the same plan and evidence state, receive the same
measurement and disruption budget, and observe the same trial, materialization,
and transition-cost accounting.

## Metrics

### Primary Metrics

- amortized workflow makespan over a declared reuse horizon;
- valid samples consumed per second;
- accelerator starvation time and utilization;
- total bytes transferred and cross-node or cross-rack traffic;
- storage footprint by representation and tier; and
- monetary or normalized resource cost where meaningful.

### Optimizer Metrics

- best achieved plan quality versus trial count;
- wall-clock convergence time;
- cumulative trial and materialization cost;
- cumulative disruption and non-reusable transition cost;
- regret relative to a small-instance oracle or best observed plan;
- plan-ranking accuracy;
- number of experiments selected, refused, and reused across decisions;
- accuracy of stopping decisions; and
- recovery time after an environment change.

### Diagnostic Metrics

- per-operator service and queueing time;
- object-store request count and throttling;
- cache hit rate by representation;
- preprocessing CPU/GPU time;
- link and disk utilization;
- bytes staged but never consumed;
- transition and invalidation cost; and
- telemetry overhead.

Raw cache hit rate is not a primary success metric: a system can improve hits
by caching large, cheap-to-recompute objects while making end-to-end performance
worse.

## Experimental Methodology

### Phase A: Bottleneck and Interaction Study

Profile static plans that choose different representation and transformation
boundaries. Sweep network bandwidth, CPU availability, capacity, and reuse
horizon. The output should be a phase diagram or equivalent result showing
which plan wins in which regime.

This phase establishes the core observation independently from the optimizer.

### Phase B: End-to-End System Comparison

Compare the selected joint optimizer against the data-path baselines. Report
both cold-start results and repeated-run results. For each experiment, specify:

- whether caches begin cold or warm;
- which derived data already exists;
- how materialization cost is charged;
- the workload horizon used for amortization; and
- whether other cluster workloads are present.

### Phase C: Autoresearch Policy Comparison

Run all methods from the same starting plan, legal plan space, evidence state,
trial mechanisms, and budget. Repeat with different random seeds. Plot best
validated end-to-end cost against cumulative trial, disruption, and transition
cost, not only iteration count. Record which uncertainty each experiment was
intended to resolve and whether its result changed the selected plan.

### Phase D: Adaptation

After the optimizer selects a plan, change one controlled factor such as link
bandwidth, CPU throughput, reuse frequency, or cache capacity. Measure drift
detection delay, trials taken to recover, transition cost, and performance
during recovery.

## Required Ablations

At minimum, ablate:

- no representation choice;
- no distributed placement or replication choice;
- fixed transformation placement;
- analytical model without learned correction;
- learned selection without structural model;
- only end-to-end feedback instead of operator-level traces;
- random experiments instead of decision-relevant experiment selection;
- no explicit stop/refusal rule;
- no transition-cost accounting; and
- flat knob search instead of structured plan transformations.

Pairwise decision ablations are especially important because they directly
support or refute the joint-planning thesis.

## Reproducibility and Statistical Treatment

- Repeat noisy trials and report confidence intervals or distributions, not
  only the best run.
- Record workload, code, dataset, plan, cluster allocation, cache state, and
  background-load identifiers.
- Randomize or counterbalance plan execution order to reduce warm-cache and
  time-of-day bias.
- Run sensitivity analysis for the workload-slice size used during trials.
- Verify selected plans with longer runs that were not used to train the cost
  model.
- Keep a held-out workload or environment configuration for generalization
  claims.

Parallel candidate evaluation should be enabled only after isolated repeats
show that plan rankings remain stable or after interference is explicitly
modeled.

## Success and Stop Criteria

The direction has a strong core result if:

1. there are reproducible regimes in which different physical plans win;
2. the joint method materially beats both intermediate caching and independent
   per-layer optimization on end-to-end cost;
3. autoresearch reaches a strong plan with fewer or cheaper experiments than
   passive, random, and generic search, and avoids low-value experiments; and
4. improvements remain after transition cost and telemetry overhead are
   included.

The scope should be reduced if the observed gain comes almost entirely from a
single conventional cache choice. Autoresearch should be removed from the
headline if the analytical model ranks plans just as well or if active
experiment selection does not beat equal-budget passive/random/generic
baselines. Delivery topology should remain outside the first paper if it is not
controllable or has no repeatable benefit on the available testbed.
