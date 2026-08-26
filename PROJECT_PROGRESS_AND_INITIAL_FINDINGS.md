# Pathfinder: Project Progress and Initial Findings

**Report date:** 26 August 2026

**Code revision:** `4a08b45b46cf9731f01cebfad970de85f1fbc188`

**Status:** Verified engineering progress and post-hoc initial findings; not a
confirmatory scientific result

## 1. Executive Summary

I have built and exercised the first end-to-end version of Pathfinder, a
research system for **Performative Physical Design (PPD)** in multimodal data
lakes. The central premise is that changing a physical data path changes what
an Agent chooses to access; this behavioral response can in turn change which
physical design is best. The system therefore measures the complete chain:

```text
physical design
  -> offered representations, prices, and service paths
  -> Agent access response W(D)
  -> task success and realized resource cost
  -> design value Phi(D)
  -> Adaptive Workload Model (AWM) certificate
  -> OED Commit / Reveal / Stop decision
  -> safe design retention or restoration
```

The implemented system now includes a FlowMesh execution adapter, a Data
Agent service, MCP access tools, durable experiment and recovery runners, a
Reduced Oracle, a risk-constrained workload-aware AWM, and a
certificate-gated OED replay. The main controlled experiment ran real
FlowMesh Agents and real artifact transfers. It showed that physical-design
interventions changed representation demand and system value. However, the
latest safety certificate correctly found the available independent-workload
sample too small to authorize a design Commit. The current result is therefore
promising but deliberately conservative.

The present evidence is from a controlled single-host testbed. It does not yet
establish cross-node placement benefits or real network, storage, and provider
cost savings. A distributed pilot with new independent workloads is the next
research stage.

## 2. Research Objective

Conventional physical design treats workload demand as fixed. Pathfinder
instead studies a setting in which an Agent observes the representations and
access terms exposed by a design and then decides what to read. A design may
therefore change both physical cost and future demand.

The research questions addressed so far are:

1. Can a controlled physical intervention change an Agent's representation
   selection while preserving an auditable execution path?
2. Can an exhaustive small design domain provide a Reduced Oracle against
   which adaptive models and controllers can be evaluated?
3. Can an AWM refuse unsafe design changes when evidence is weak?
4. Can OED reduce exploration while preserving a safe fallback?

The current work answers these questions at the controlled-testbed and
post-hoc method-development level. It does not yet answer them for a real
distributed deployment.

## 3. What I Implemented

### 3.1 FlowMesh execution layer

I integrated Pathfinder with FlowMesh `v0.1.8-rc.1` without modifying the
FlowMesh source tree. The integration includes:

- public-SDK workflow submission and result retrieval;
- a one-node Agent workflow whose worker pin is preserved by the deployed
  parser;
- exact worker ID or stable-alias resolution that fails closed rather than
  submitting an unpinned workflow;
- workflow validation before submission;
- HTTP result delivery from the worker to the FlowMesh Root Server;
- redacted terminal failure details and workflow/task identifiers; and
- a read-only control-plane preflight.

Worker, service, and credential lifecycles remain operator-controlled. The
research runner does not silently create workers or move work to a different
worker.

### 3.2 Data Agent and access-control layer

I implemented a versioned HTTP Data Agent and a persistent MCP Access Gateway.
Together they provide:

- manifest-backed representation discovery and access;
- session budgets and access-count limits;
- idempotent access records;
- opaque, session-bound artifact handles;
- a `fetch_artifact` MCP tool for sampled-frame and other file-backed
  representations;
- digest, size, origin, redirect, and response-size validation;
- storage of artifact-handle fingerprints rather than reusable handles; and
- separate quoted price, realized cost, service latency, fetch latency,
  controlled delay, transferred bytes, and task outcome fields.

Artifact telemetry is reconciled fail closed. A selected artifact is counted
as delivered only after a completed full download is durably recorded. Missing
or unstable telemetry cannot be silently converted to zero bytes or a complete
research observation.

### 3.3 Experiment runner and audit recovery

I implemented reproducible trial planning, paired seeds, JSONL records, CSV
summaries, configuration hashes, worker pinning, bounded retries, and resume
support. Infrastructure failures, telemetry failures, artifact-delivery
failures, and task failures remain separate outcomes.

After a server/LLM-service incident interrupted the larger v2 experiment, I
added an audited recovery mechanism. It preserved an immutable incident
snapshot, assigned new recovery identities, retried only missing or
infrastructure-failed trials, retained every recovery attempt, and generated a
separately fingerprinted canonical Oracle. In the completed recovery:

- 104 trials required recovery;
- 107 recovery attempts were recorded;
- 104 attempts completed and 3 were infrastructure failures;
- no recoverable trial remained; and
- the declared safe design was restored.

### 3.4 Reduced Oracle

The first full controlled experiment exhaustively evaluated:

```text
4 physical designs
  x 8 independent NExT-QA workload objects
  x 4 paired repetitions
= 128 real FlowMesh sessions
```

The design domain contained an origin design, local sampled frames, a local
multimodal digest, and a local pair. The experiment used a pinned worker, a
real FlowMesh Agent, Pathfinder MCP tools, Data Agent accesses, artifact
downloads, result upload, physical file materialization, and restoration.

A prospective workload expansion then added eight new NExT-QA videos using a
frozen deterministic selection rule:

```text
4 designs
  x 16 independent workload objects
  x 4 paired repetitions
= 256 canonical FlowMesh sessions
```

The v2 expansion is an engineering power and scaling check. Because v1
informed later model development, pooling v1 and v2 is not an independent
confirmation of the selected AWM.

### 3.5 Adaptive Workload Model

I developed the AWM through several isolated diagnostics, each retained rather
than rewriting the earlier frozen evidence. The main lessons were that Agent
repetitions must not be counted as independent workloads, direct utility
bounds remain too wide at small sample sizes, and workload heterogeneity is
important.

The current AWM `v3alpha5` is a risk-constrained, workload-aware safety
certificate. For a candidate design it separately checks:

```text
success non-inferiority:
  LCB(delta_success) >= -delta_success_margin

cost improvement:
  LCB(origin_cost - candidate_cost) >= minimum_cost_saving
```

It emits exactly one of:

- `SAFE_TO_COMMIT`;
- `UNSAFE`; or
- `INSUFFICIENT_EVIDENCE`.

Every non-safe outcome falls back to `D_origin_remote`. Workload objects are
the independent units; complete repetition blocks are averaged within each
workload. The implementation uses one-sided bounded-mean KL inversions and a
declared Bonferroni family over every stratum, gate, and tail.

The currently evaluated policy restriction is deliberately marked post-hoc:

| Workload stratum | Candidate compared with origin |
|---|---|
| Causal | `D_local_frames` |
| Descriptive | `D_local_frames` |
| Temporal | `D_local_digest` |

`D_local_pair` is excluded from this restricted policy. This restriction is a
hypothesis derived from the inspected Oracle, not a validated policy.

### 3.6 Certificate-gated OED

I implemented an additive OED replay that consumes the three-state AWM
certificate:

- `SAFE_TO_COMMIT` may be considered for Commit subject to value and budget;
- `UNSAFE` can never be committed;
- `INSUFFICIENT_EVIDENCE` may justify a Reveal when it is useful and
  budget-feasible; and
- otherwise the controller stops and retains the safe origin design.

The replay uses a hidden-Oracle view. Candidate outcomes are not opened before
the corresponding Reveal, and leakage tests verify that changing unrevealed
outcomes cannot change earlier actions.

## 4. Experiments and Verification Completed

### 4.1 Engineering smoke tests

The smoke tests established the complete online path:

- FlowMesh dispatched a pinned Agent task;
- the Agent called Pathfinder MCP tools;
- a digest representation was returned inline;
- a sampled-frame representation was delivered through an opaque artifact
  handle and `fetch_artifact`;
- FlowMesh received the task result; and
- Pathfinder reconciled complete byte and latency telemetry.

These tests validated the mechanism, but a single fixture was not treated as
scientific evidence.

### 4.2 Four-design controlled experiment v1

All 128 planned sessions completed. There were no telemetry-incomplete
sessions or sampled-frame artifact-delivery failures, no reusable artifact
handles were frozen, and the safe design was restored.

### 4.3 Sixteen-workload expansion v2

The recovered canonical v2 Oracle contains all 256 planned design/workload/
repetition cells. It doubles the independent workload count while preserving
the original design domain and paired repetition contract.

### 4.4 Software and statistical calibration

At revision `4a08b45`, the server-side Python 3.12 verification ran 415 tests
successfully in two fresh processes. The v3alpha5 Monte Carlo calibration used
seed `90210`, 2,000 simulations in each of eight scenario families, for 16,000
family simulations in total.

The frozen calibration recorded:

| Metric | Result |
|---|---:|
| Family-wise false-safe decisions | 0 / 16,000 |
| 99% upper bound on false-safe rate | 0.000331 |
| Simultaneous coverage | 1.000000 |
| 99% lower bound on coverage | 0.999669 |
| Negative-control false-safe rate, pooled | 0.087938 |
| Negative-control coverage | 0.000000 |

The negative control deliberately reports a sample mean as though it were a
confidence bound. Its failure demonstrates that the calibration harness can
detect an anti-conservative rule rather than merely returning a vacuous pass.

## 5. Initial Findings

### 5.1 Physical design changed Agent demand

The v1 controlled experiment produced the following aggregate outcomes:

| Design | Task success | Mean service cost | Digest selection | Frame selection | `Phi` |
|---|---:|---:|---:|---:|---:|
| `D_origin_remote` | 0.87500 | 1.337500 | 0.62500 | 0.37500 | 7412.500000 |
| `D_local_frames` | 0.84375 | 0.875000 | 0.50000 | 0.50000 | 7562.499992 |
| `D_local_digest` | 0.84375 | 0.221875 | 0.96875 | 0.03125 | 8215.624999 |
| `D_local_pair` | 0.84375 | 0.190625 | 0.81250 | 0.18750 | **8246.874990** |

The two preregistered directional mechanism checks were observed:

- making sampled frames local and cheaper increased their selection from
  37.5% to 50.0%; and
- making the digest local and cheaper increased its selection from 62.5% to
  96.875%.

Relative to the origin design, the controlled mean service cost fell by about
34.6% for local frames, 83.4% for the local digest, and 85.7% for the local
pair. The local designs also had an aggregate task-success rate 3.125
percentage points below the origin. This trade-off is why the later AWM uses
separate success and cost gates rather than allowing cost savings to
automatically compensate for lower success.

Both the steady-state and transition-aware exhaustive Oracle ranked
`D_local_pair` first in v1. That result is for controlled engineering costs on
one host, not real distributed costs.

### 5.2 A single global policy masked workload heterogeneity

The post-hoc leave-one-workload-out audit found that a global policy had mean
evaluation utility gain `-0.2484` relative to origin, whereas a policy grouped
by causal, descriptive, and temporal strata had mean gain `+0.3234`. The
grouped policy nevertheless reduced mean task success by `0.0625`, and it
matched the exhaustive per-workload best design for only 8 of 16 workloads.

Causal and descriptive workloads showed the most promising frame-locality
pattern. Temporal workloads were unstable across designs and repetitions.
These observations motivated the restricted v3alpha5 policy, but they remain
post-hoc findings from the same inspected Oracle.

### 5.3 The safety certificate did not authorize a Commit

On the frozen sixteen-workload Oracle, all three v3alpha5 certificates returned
`INSUFFICIENT_EVIDENCE`:

| Stratum | Candidate | Independent workloads | Success difference: point / LCB / UCB | Cost saving: point / LCB / UCB | Decision |
|---|---|---:|---:|---:|---|
| Causal | `D_local_frames` | 6 | +0.0417 / -0.9041 / +0.9270 | +0.8875 / -1.4747 / +1.9819 | Insufficient evidence |
| Descriptive | `D_local_frames` | 4 | 0.0000 / -0.9672 / +0.9672 | +0.8375 / -1.7454 / +1.9955 | Insufficient evidence |
| Temporal | `D_local_digest` | 6 | -0.0833 / -0.9371 / +0.8913 | +1.1667 / -1.3049 / +1.9957 | Insufficient evidence |

All three decisions retained `D_origin_remote`. This is not an execution
failure: the point estimates suggest cost savings, but 4--6 independent
workloads per stratum leave the simultaneous bounded intervals too wide to
establish either safety or harm.

The evaluated post-hoc certificate used engineering placeholders
`delta_success_margin = 0.05` and `minimum_cost_saving = 0.0`. They are not
scientifically justified operating thresholds. A future pilot value of 0.25
for minimum cost saving has been proposed, but it has not yet been frozen or
validated in a new experiment.

### 5.4 Workload-aware OED reduced exploration, not uncertainty enough to Commit

The certificate-gated v3alpha5 OED replay performed:

```text
REVEAL D_local_digest
  -> REVEAL D_local_frames
  -> STOP (certificate_limited_stop)
  -> retain D_origin_remote
```

It used two Reveals with total controlled reveal cost `0.041513578`. The prior
v3alpha4 OED revealed local digest, local pair, and local frames at cost
`0.082137466`, then reached the same stopping decision. The workload-aware
restriction therefore removed one unneeded Reveal and reduced controlled
exploration cost by about 49%, but it did not change the final design. Neither
policy made a false-safe Commit.

This result shows that OED can act on a workload-aware certificate and avoid
some irrelevant exploration. It does not show that current evidence is
informative enough to improve the safe design.

## 6. What the Current Evidence Supports

The current evidence supports the following claims:

1. Pathfinder executes the complete FlowMesh--MCP--Data Agent path and records
   fail-closed research telemetry.
2. It can run, recover, canonicalize, and restore a bounded multi-candidate
   physical-design experiment.
3. Controlled physical design changed the Agent's representation-selection
   response in both preregistered directions.
4. Exhaustive controlled evaluation found local designs with higher horizon
   value than the safe origin design under the declared engineering costs.
5. Workload heterogeneity is material enough that a single global policy can
   hide conditional effects.
6. The v3alpha5 decision rule is calibrated on its declared synthetic data-
   generating processes and avoids false-safe decisions in those simulations.
7. The AWM and OED correctly retain the safe origin when real controlled-
   testbed evidence is insufficient.
8. The restricted OED replay reduced exploration cost relative to the earlier
   three-candidate replay without introducing a Commit.

## 7. What the Current Evidence Does Not Support

The current evidence does **not** establish:

- performance improvements from real cross-node data placement;
- real network, storage-tier, provider, or materialization cost savings;
- that `D_local_pair`, `D_local_frames`, or `D_local_digest` should be deployed
  as a general policy;
- that the post-hoc workload restriction generalizes to future workloads;
- confirmatory frequentist coverage for the observed NExT-QA workload
  population;
- an OED Commit supported by the current Oracle;
- an autonomous optimizer that is ready to mutate production physical design;
  or
- a complete distributed PPD result.

The origin/local paths, service delays, and unit costs in the completed Oracle
remain controlled single-host interventions. Repetitions reduce within-
workload noise but do not create additional independent workloads.

## 8. Reproducibility and Evidence Freeze

The latest method implementation is committed on `main` at:

```text
4a08b45b46cf9731f01cebfad970de85f1fbc188
Add risk-constrained AWM certificate and OED replay
```

The external post-hoc method freeze is identified as:

```text
awm-v3alpha5-posthoc-20260826-v1
```

Its `SHA256SUMS` file has SHA-256:

```text
e722b20a32d7c33078504894fc8646b76ccecae787a3554a6dbf1c3fab57082e
```

The freeze contains the source archive, exact AWM configurations, certificate
and calibration results, v3alpha5 and v3alpha4 OED comparisons, environment
versions, a manifest, and per-file checksums. All 26 recorded checksums were
verified after the freeze was made read-only.

The Oracle snapshot identifiers are:

| Scope | SHA-256 |
|---|---|
| Full declared four-design set | `2d206301a5315bbbb57eb3d79bf3f82a192ae6190a6e8530046e5ffa6ceba8d1` |
| Analysed restricted-design subset | `58cddfd360e1d8b2bfcdc21a36cdb68c24fc1fbb576a389075deb46361b0f254` |

Both use `pathfinder.reduced-oracle-snapshot-sha256/v1`; their different values
reflect different declared design scopes rather than different source data.

No FlowMesh credential, LLM key, reusable artifact handle, worker deployment
configuration, service log, or runtime database is included in the Git commit
or method freeze.

## 9. Next Steps

The next stage should move from controlled single-host interventions to a
distributed pilot rather than developing another AWM version on the same
Oracle.

The planned work is:

1. freeze a pilot preregistration and cost contract before observing new
   outcomes;
2. use `delta_success_margin = 0.05` and, subject to review, a proposed minimum
   cost saving of `0.25` in the current normalized engineering units;
3. include service, network, storage, amortized materialization, and transition
   cost in one auditable total-cost ledger;
4. support multiple Data Agent endpoints with explicit object/representation
   placement and no silent endpoint fallback;
5. select approximately 30--50 new independent workloads across causal,
   descriptive, and temporal strata using a frozen rule;
6. run a restricted distributed Reduced Oracle pilot comparing origin with the
   predeclared stratum-specific candidate;
7. apply the frozen v3alpha5 certificate and OED replay without changing the
   thresholds after outcomes are observed; and
8. use the pilot to estimate variance and determine the sample size for a
   separate confirmatory experiment.

A 30--50 workload run should be described as a distributed engineering and
power pilot, not as confirmatory evidence. The current calibration suggests
that substantially more independent workloads may be required for a safe
Commit under the conservative simultaneous family.

## 10. Verification Note for the Author

This report was generated from and cross-checked against:

- the repository at commit `4a08b45`;
- [`PPD_VALIDATION_PROTOCOL.md`](PPD_VALIDATION_PROTOCOL.md);
- [`MULTI_CANDIDATE_FORMAL_V1_PREREGISTRATION.md`](MULTI_CANDIDATE_FORMAL_V1_PREREGISTRATION.md);
- [`MULTI_CANDIDATE_FORMAL_V1_RESULTS.md`](MULTI_CANDIDATE_FORMAL_V1_RESULTS.md);
- [`MULTI_CANDIDATE_FORMAL_V2_PREREGISTRATION.md`](MULTI_CANDIDATE_FORMAL_V2_PREREGISTRATION.md);
- [`README.md`](README.md);
- the committed v3alpha5 certificate and calibration configurations; and
- the server-side canonical recovery, AWM calibration, certificate, OED, and
  freeze reports supplied for this project.
