# Multi-Candidate Controlled PPD Experiment v1: Initial Results

## Status

This document records the first analysis of the frozen four-design controlled
Pathfinder experiment. It is a results companion to
[`MULTI_CANDIDATE_FORMAL_V1_PREREGISTRATION.md`](MULTI_CANDIDATE_FORMAL_V1_PREREGISTRATION.md),
not a replacement for the preregistration.

The analyzed run is the clean rerun performed after recovery from an external
LLM-gateway outage. The earlier outage-affected output is excluded rather than
mixed with the clean run. The frozen evidence consists of:

- `oracle-v2`: the exhaustive real-FlowMesh Reduced Oracle;
- `awm-v1`: the offline AWM evaluation; and
- `oed-v1`: the offline OED policy replay.

The run used a real FlowMesh Agent, a pinned dedicated worker, Pathfinder MCP
tools, the Data Agent, physical file materialization, artifact download, and
fail-closed telemetry reconciliation. Origin/local labels, minimum service
delays, unit costs, and both tiers' files remained controlled interventions on
one host. The results therefore support controlled-testbed PPD claims, but not
cross-node network, distributed placement, or provider-cost claims.

## Experimental Domain

The experiment exhaustively evaluated:

```text
4 physical designs
  x 8 NExT-QA workload-object blocks
  x 4 paired repetitions
= 128 FlowMesh sessions
```

The physical-design domain was:

| Design | Sampled frames | Multimodal digest |
|---|---|---|
| `D_origin_remote` | origin path, quote 4 | origin path, quote 6 |
| `D_local_frames` | local copy, quote 1 | origin path, quote 6 |
| `D_local_digest` | origin path, quote 4 | local copy, quote 2 |
| `D_local_pair` | local copy, quote 1 | local copy, quote 2 |

Repetitions 0--1 formed the AWM training partition. Repetitions 2--3
were hidden from fitting and policy decisions and used for paired holdout
evaluation. The horizon objective used 1,000 sessions over 24 hours. Its cost
coefficients are preregistered controlled engineering units rather than real
distributed-resource prices.

## Gate M1: Execution Integrity

Gate M1 passed completely.

| Check | Result |
|---|---:|
| Planned and recorded sessions | 128 / 128 |
| Completed sessions | 128 / 128 |
| Sessions per design | 32 / 32 |
| Telemetry-incomplete sessions | 0 |
| Sampled-frame artifact-delivery failures | 0 |
| Raw artifact handles in frozen records | 0 |
| Missing artifact-handle fingerprints | 0 |
| Safe design restored | Yes |
| Remaining materialized files owned by the run | 0 |

The experiment therefore demonstrates a reliable end-to-end path through
FlowMesh scheduling, Agent tool use, Pathfinder access control, Data Agent
execution, artifact delivery, result upload, reconciliation, bounded physical
mutation, and restoration.

## Gate M2: Multi-Design Performative Response

### Aggregate outcomes

| Design | Task success | Mean service cost | Digest selection | Frame selection | `Phi` | Transition-adjusted `Phi` |
|---|---:|---:|---:|---:|---:|---:|
| `D_origin_remote` | 0.87500 | 1.337500 | 0.62500 | 0.37500 | 7412.500000 | 7412.500000 |
| `D_local_frames` | 0.84375 | 0.875000 | 0.50000 | 0.50000 | 7562.499992 | 7562.499469 |
| `D_local_digest` | 0.84375 | 0.221875 | 0.96875 | 0.03125 | 8215.624999 | 8215.624494 |
| `D_local_pair` | 0.84375 | 0.190625 | 0.81250 | 0.18750 | **8246.874990** | **8246.874791** |

Both preregistered directional mechanism checks were observed:

- making sampled frames local and cheaper increased frame selection from
  37.5% to 50.0%, a gain of 12.5 percentage points; and
- making the digest local and cheaper increased digest selection from 62.5%
  to 96.875%, a gain of 34.375 percentage points.

Relative to `D_origin_remote`, mean realized service cost fell by approximately
34.6% under `D_local_frames`, 83.4% under `D_local_digest`, and 85.7% under
`D_local_pair`. The corresponding steady-state objective improvements were
approximately 2.0%, 10.8%, and 11.3%. These objective improvements were driven
by service-cost reduction and induced representation choice, despite the local
designs having an aggregate task-success rate 3.125 percentage points below
the origin design.

Both the steady-state and transition-aware exhaustive Oracle selected
`D_local_pair`. Controlled transition costs were too small to change the
ranking on this single-node testbed.

### Paired holdout stability

The design ranking was identical in both hidden holdout repetitions:

| Holdout repetition | Rank 1 | Rank 2 | Rank 3 | Rank 4 |
|---|---|---|---|---|
| 2 | `D_local_pair` (8556.25) | `D_local_digest` (8550.00) | `D_local_frames` (7693.75) | `D_origin_remote` (7500.00) |
| 3 | `D_local_pair` (8556.25) | `D_local_digest` (7300.00) | `D_local_frames` (6625.00) | `D_origin_remote` (6250.00) |

All three candidate designs outperformed the origin design in both holdout
repetitions. `D_local_pair` ranked first twice, but its advantage over
`D_local_digest` varied from 6.25 to 1256.25 objective units. The current
evidence supports a stable ordering in these two repetitions, but not a strong
claim about the magnitude or statistical significance of the pair-versus-
digest advantage.

### Naive-policy comparison

The naive incumbent-demand policy selected `D_local_digest`, while the
exhaustive Oracle selected `D_local_pair`. On the paired holdout, the naive
policy retained 631.25 objective units of regret relative to the Oracle.

The preregistered self-confirming-lock-in indicator was false. The result
supports a claim that incumbent-only read-react optimization was suboptimal in
this run; it does not support a claim that the naive policy remained locked in
the origin design.

## Gate M3: AWM Safety and Informativeness

The initial AWM observed 16 training sessions from `D_origin_remote`. The
other designs were hidden from initial fitting. Each design contributed 16
paired holdout sessions. No training or holdout session was excluded.

### Safety result

All three reported model variants achieved:

- joint complete-response-vector containment;
- joint `Phi` containment;
- per-design response and `Phi` containment; and
- zero false-safe Commit decisions.

This is a successful engineering safety check. One complete-vector containment
event is not, by itself, frequentist calibration evidence for nominal 90%
coverage.

### Informativeness result

The AWM did not certify any Commit. Initial candidate `Phi` widths remained
between 10,220 and 11,760 objective units. Mean width was 11,182.5 for the
assumption-free box and 9,298.7 for both the independent and nominally coupled
models.

The coupled and independent models were identical on unobserved candidates
because all empirical cross-design assumptions remained disabled. The AWM was
therefore safe but insufficiently informative to prove that any observed
candidate was better than the incumbent.

## Gate M4: Offline OED Policy Replay

### Policy outcomes

| Policy | Final design | Reveals | Commits | Control cost | Net holdout value | Regret | Reached Oracle |
|---|---|---:|---:|---:|---:|---:|---|
| Exhaustive Oracle | `D_local_pair` | 0 | 1 | 0.000200 | 8556.249791 | 0.000000 | Yes |
| Naive read-react | `D_local_digest` | 0 | 1 | 0.000504 | 7924.999494 | 631.250297 | No |
| Passive AWM | `D_origin_remote` | 0 | 0 | 0.000000 | 6875.000000 | 1681.249791 | No |
| Full OED | `D_origin_remote` | 3 | 0 | 0.081550 | 6874.918450 | 1681.331341 | No |
| Black-box Reveal | `D_origin_remote` | 3 | 0 | 0.081550 | 6874.918450 | 1681.331341 | No |
| Random Reveal | `D_origin_remote` | 3 | 0 | 0.081550 | 6874.918450 | 1681.331341 | No |

Full OED revealed candidates in the order:

```text
D_local_digest -> D_local_pair -> D_local_frames -> STOP
```

It then stopped with `certificate_limited_stop`. After every candidate had
been observed, all pessimistic Commit gains remained negative:

| Candidate | Final pessimistic gain | Final optimistic gain | Final width |
|---|---:|---:|---:|
| `D_local_digest` | -4560.48 | 5446.61 | 4956.09 |
| `D_local_frames` | -5378.27 | 5275.64 | 5602.91 |
| `D_local_pair` | -4862.39 | 5248.04 | 5059.45 |

No policy produced a safe-sequence regression. The safety behavior therefore
worked as intended, but the registered OED effectiveness checks failed:

- Full OED did not reach the exhaustive-Oracle design.
- Full OED did not use less control cost than equal-budget Reveal baselines.
- Full OED had the same aggregate terminal outcome and control cost as the
  black-box and random Reveal baselines after all cross-design assumptions had
  been disabled.

The result should be interpreted as certificate-limited conservatism, not an
execution failure.

## Diagnostic Finding: Confidence Width Can Increase After Reveal

The width for an already revealed `D_local_digest` increased as more designs
were added to the observed set:

```text
4591.50 -> 4809.80 -> 4956.09
```

The current AWM divides its joint error probability by a metric family whose
size depends on the number of observed designs. Revealing another design
therefore reduces the per-metric error budget and may widen existing Wilson
intervals. This behavior is conservative for an individual snapshot but works
against the intended value of information. In addition, repeatedly consulting
ordinary snapshot intervals in an adaptive OED loop is not yet a time-uniform
sequential safety guarantee.

This diagnostic motivates a v2 AWM based on a fixed preregistered confidence
family, paired design-gain observations, and an explicit sequential-validity
mechanism such as fixed looks with alpha spending or an anytime-valid
confidence sequence.

The repository now contains the first implementation of that diagnostic as
`pathfinder.awm/v2alpha1`. It uses a fixed full-domain family and a fixed-look
paired empirical Bernstein certificate. The frozen v1 numbers above have not
been recomputed or overwritten. The v2 configs are explicitly suffixed
`_diagnostic`; their results remain to be run and must be interpreted as
post-hoc method development.

The completed v2alpha1 replay showed that this first correction was not
informative at 16 training pairs. Although all six diagnostic holdout checks
were covered, the three final origin-to-candidate paired intervals were about
21,980--23,520 objective units wide and were effectively clipped to the full
declared utility support. Full OED again revealed all three candidates and
stopped without a Commit. The result isolates the finite-support range term,
the broad six-pair/four-look family, and the small paired sample count as the
dominant limitations; it does not indicate a telemetry or execution failure.

`pathfinder.awm/v2alpha2` is the next post-hoc method diagnostic. It fixes the
comparison family to the three origin-to-candidate decisions, treats repeated
reads of one frozen snapshot as one simultaneous look, decomposes every
certificate radius, and emits plug-in sample-size planning. The all-observed
power config and origin-only OED config are deliberately separate. Their
outputs must not replace the frozen v1 result, and any selected sample size or
decision rule must be preregistered and tested on independent data.

The v2alpha2 diagnostic has now been run. It preserved the same
Reveal-all-then-Stop behavior. At 16 raw training pairs the three direct
paired-utility radii remained approximately 21.4--22.3 units per session, and
the bounded-range term contributed roughly 87--90% of each radius. The
plug-in calculation estimated about 31 pairs merely to avoid support clipping;
much larger counts were projected for narrow intervals or a positive Commit
lower bound. This identifies the broad direct-utility support and the treatment
of repeated runs as independent units as the next statistical bottlenecks.

The repository therefore also contains `pathfinder.awm/v3alpha1` as an
isolated post-hoc diagnostic. It reduces the frozen 16 training pairs to eight
independent workload clusters, takes one deterministic paired observation per
cluster, constructs success uncertainty from paired discordance Bernoulli-KL
intervals, and constructs service-cost uncertainty separately with empirical
Bernstein. OED is required to use the combined component certificate.

The v3alpha1 replay has now been run against the frozen Oracle. The component
construction narrowed the three primary intervals to approximately 55--58%
of their declared utility support, but no candidate obtained a positive
Commit lower bound. Full OED revealed local digest, local pair, then local
frames and stopped with `certificate_limited_stop`. This is an uncertainty
result, not a system failure.

A repetition-sensitivity check then exposed a limitation of v3alpha1's
lowest-repetition rule. For local frames, the mean paired utility difference
was `+0.64375` in training repetition 0 and `-0.6125` in repetition 1; for
local pair it was `+1.14375` and `-1.16875`. Local digest remained positive
but changed from `+1.05` to `+0.0625`. The pooled two-repetition means were
`+0.015625`, `-0.0125`, and `+0.55625`, respectively. Thus, selecting the
first repetition can materially change both the point estimate and post-hoc
power projection.

`pathfinder.awm/v3alpha2` was then run as a separate post-hoc diagnostic. It
averaged the complete fixed repetition block inside each workload cluster,
counted eight workloads rather than 16 runs as independent, and reproduced
the pooled point estimates above. Local frames and local pair retained their
repetition sign-flip warnings. All three primary intervals remained wide:
approximately `14.86`, `14.09`, and `14.05` per session for local frames,
local digest, and local pair. OED revealed all three candidates and stopped
with `certificate_limited_stop`; it made no unsafe Commit. The service-cost
empirical-Bernstein component reached its declared support on this small
sample, while success uncertainty accounted for most of the combined width.

`pathfinder.awm/v3alpha3` was then run as another isolated post-hoc diagnostic.
It kept the same eight workload means and complete repetition snapshot but
certified the normalized bounded paired utility directly. Its intervals were
wider, not narrower: approximately `19.65`, `19.00`, and `18.33` per session
for local frames, local digest, and local pair. The projected independent
workload counts for 50% support width were 34 for all three candidates; the
25% targets were 149, 149, and 148. OED again revealed all candidates and
stopped with `certificate_limited_stop`, with no false-safe Commit. Thus,
v3alpha3 is a useful negative ablation: removing the decomposition alone does
not solve the small-independent-sample problem.

`pathfinder.awm/v3alpha4` is now implemented but has not yet been run on the
server Oracle. It retains the same workload means while predeclaring the joint
finite states formed by success difference, selected representation, and
bounded action cost. It orders and bins those states, applies simultaneous
Bernoulli-KL bounds to cumulative bin probabilities, and exposes the complete
support/bin audit trail to OED. This tests whether preserving action/outcome
dependence can tighten the safe certificate without reusing repetitions as
independent evidence. It remains post-hoc method development.

## Supported Claims

The current evidence supports the following controlled-testbed claims:

1. Pathfinder executed and restored a bounded multi-candidate physical-design
   experiment without telemetry or artifact-delivery loss.
2. Physical design changed the Agent's representation-selection response
   `W(D)` in both preregistered directions.
3. Exhaustive evaluation identified designs that improved the horizon
   objective relative to the safe origin design.
4. The best design was stable across the two paired holdout repetitions.
5. The initial AWM avoided false-safe Commit decisions.
6. The offline OED controller preserved the safe design throughout Reveal and
   restoration.

## Claims Not Yet Supported

The current evidence does not support the following claims:

- real distributed or cross-node placement improvement;
- real provider, network, storage-tier, or transition cost savings;
- a self-confirming lock-in event;
- nominal 90% AWM calibration across repeated experiments;
- an informative coupled-AWM advantage over an independent model;
- OED reaching the Oracle or outperforming equal-budget baselines; or
- a complete self-driving physical optimizer ready for deployment.

## Next Experiment and Method Steps

The frozen v1 results should not be overwritten or retuned into confirmatory
evidence. The next phase is:

1. replay v3alpha4 on the same frozen Oracle as explicitly post-hoc method
   development;
2. compare v3alpha2, v3alpha3, and v3alpha4 interval widths, held-out
   containment, power projections, and OED actions without changing the
   frozen snapshot;
3. choose and freeze the method and independent-workload sample size, then run
   it on independent NExT-QA objects rather than adding more within-object
   repetitions; and
4. move to a multi-node storage and network testbed only after the safe
   AWM/OED loop becomes informative enough to Commit.

## Initial Takeaway

The first full experiment validates the central performative premise: changing
the physical data path changed Agent demand and changed the best system design.
It also isolates the next research problem. The current uncertainty model is
safe but so conservative that exhaustive Reveal still cannot certify a better
design. The immediate research target is therefore a paired, sequentially
valid AWM that preserves fail-closed safety without making OED operationally
inert.
