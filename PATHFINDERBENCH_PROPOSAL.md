# PathfinderBench: Public Dataset and Benchmark Proposal

**Status:** Draft v0.1 for advisor and licensing review

**Date:** 2026-08-30

**Scope:** Benchmark specification only; no new scientific evidence

## 1. Summary

PathfinderBench is a proposed public benchmark for **Performative Physical
Design (PPD)** in multimodal data systems. It asks a systems question that is
not answered by a conventional VideoQA leaderboard:

> When a system changes where and how multimodal representations are stored,
> priced, transformed, and served, can an optimizer safely choose a physical
> design after accounting for the Agent's resulting access behavior?

The benchmark unit is therefore not just a video and a question. It combines:

1. a versioned multimodal workload;
2. several representations of the same source object;
3. a finite set of physical-design candidates;
4. complete task, access, transfer, latency, and cost observations;
5. a candidate-relative Reduced Oracle; and
6. auditable AWM/OED decisions over those observations.

PathfinderBench is **one PPD benchmark with two evaluation modes that have a
clear evidentiary hierarchy**:

- the **primary live end-to-end mode** tests the paper's causal and systems
  claims using real physical-design interventions, Agent responses, transfers,
  and restoration; and
- the **supporting offline trace-replay mode** makes the resulting finite-domain
  evidence reusable for public AWM/OED comparison without private services,
  credentials, or a cluster.

The offline mode improves reproducibility and method comparison, but it cannot
replace live evidence that a physical intervention changed a real data path.
The benchmark is a release artifact that packages the paper's existing PPD,
AWM/OED, and Pathfinder contributions; it is not a seventh independent
research contribution.

The current 16-workload controlled experiment is a development seed, not the
public benchmark. The first public evaluation cohort should use fresh,
video-disjoint workloads selected before their Pathfinder outcomes are read.

## 2. Research target

For a physical design `D`, Pathfinder observes the chain:

```text
physical design D
  -> offered representations, quotes, and service paths
  -> Agent access response W(D)
  -> task success and realized resource use
  -> session value Phi(D)
  -> AWM confidence certificate
  -> OED Commit / Reveal / Stop decision
  -> safe-design retention or restoration
```

The benchmark evaluates every link in this chain. Its primary object is the
**data-path decision policy**, not the underlying vision-language model.
Model quality remains important because a representation that cannot support
the task should not be treated as valuable, but a higher VideoQA score alone
does not constitute a better PPD policy.

### 2.1 Alignment with the first-paper contribution

The benchmark follows the contribution structure already declared by the
project rather than introducing a separate benchmark research agenda:

| Paper contribution | Benchmark evidence |
|---|---|
| PPD formulation and endogenous demand | A live intervention changes offers or placement, followed by a measured Agent access response |
| Failure of static/reactive optimization | Prespecified static and reactive baselines are compared under the same workload and cost contract |
| Agent Workload Model (AWM) | Public traces support held-out prediction and simultaneous confidence-certificate evaluation |
| Online Experimental Design (OED) | Commit / Reveal / Stop decisions are replayed under frozen budgets and checked against the Reduced Oracle |
| Pathfinder system | The live mode measures end-to-end execution, transfer, failure handling, and safe restoration |
| Reproducible and falsifiable evidence | Content-addressed offline artifacts reproduce analysis without access to private infrastructure |

FlowMesh remains the reference execution substrate. Its deployment and worker
management are necessary engineering, but performance of FlowMesh itself is
not the paper's scientific target.

### 2.2 Questions the benchmark should answer

1. Does a physical-design intervention change what the Agent accesses?
2. Does the change preserve task quality within a declared margin?
3. What service, network, storage, materialization, and transition resources
   are consumed?
4. How much value does a design achieve relative to the safe incumbent and
   the finite-domain Oracle?
5. How much experimentation is needed before a policy can Commit safely?
6. Does the policy fail closed under incomplete telemetry, failed artifact
   delivery, endpoint faults, and process interruption?
7. Does a learned policy generalize across held-out workload objects and
   across a declared deployment environment?

### 2.3 Non-goals for v0.1

PathfinderBench v0.1 will not claim:

- a universal optimum over every possible physical design;
- production-safe autonomous mutation of a data lake;
- a generic benchmark of distributed executors, schedulers, or FlowMesh;
- comparability of raw latency or cost across arbitrary hardware;
- generalization to all videos or all multimodal workloads;
- additional modalities before the primary video PPD result is stable;
- two independent, equally weighted benchmark contributions for the paper;
- that repeated runs of one object are independent samples;
- that a single utility weighting is correct for every deployment; or
- that the present single-host Oracle establishes distributed-placement
  benefits.

All Oracle and regret statements are relative to the benchmark's declared
candidate set and deployment contract.

## 3. Evaluation modes and evidence hierarchy

### 3.1 Primary mode: Live end-to-end PPD

The primary mode evaluates the complete causal chain. At least two Data Agent
nodes have distinct node identities and serve physically distinct
representation placements. A workflow worker reaches them through the same
versioned endpoint registry used by the experiment runner and MCP Gateway.

The live mode measures:

- end-to-end task outcome;
- Agent representation selection;
- Data Agent service and fetch latency;
- completed transfer bytes;
- storage and materialization quantities;
- transition and restoration work;
- infrastructure and policy failures; and
- resume behavior after interruption.

PathfinderBench v0.1 uses the tested FlowMesh stack as its reference executor.
Executor portability may be explored after the first-paper evidence is stable;
supporting arbitrary executors is not a v0.1 acceptance criterion.

Raw measurements from unrelated hardware are not directly rankable. An
official live leaderboard must either use a standard reference deployment or
report improvement relative to the safe design on the same deployment.
Bring-your-own-deployment results may be reported as diagnostic replication,
but they do not enter the primary reference result.

### 3.2 Supporting mode: Public offline trace replay

The supporting mode releases frozen, de-identified observations produced by
the live and Reduced Oracle protocols. It asks participants to make design or
exploration decisions without contacting a live service. It evaluates AWM/OED
reasoning over fixed evidence, not whether a physical intervention happened.

Inputs:

- frozen workload and representation manifests;
- candidate-design definitions;
- training observations and their completeness flags;
- a declared safe design;
- transition and restoration costs;
- a fixed exploration budget; and
- an evaluator-owned holdout or Oracle table.

Outputs:

- Commit, Reveal, or Stop at every iteration;
- the selected candidate, if any;
- confidence or uncertainty information;
- cumulative exploration and transition cost;
- safe-design state after each action; and
- a machine-readable audit trace.

The offline mode must run on a clean machine with public files only. A
participant must not need a Lumid PAT, LLM API key, FlowMesh deployment, or
Pathfinder runtime database. The current AWM/OED implementations become
reference baselines rather than privileged infrastructure. An offline score
must never be presented as evidence of live placement or transfer savings.

## 4. Data model

Every released object should be content-addressed and connected through
versioned manifests. Missing information is never represented as zero.

### 4.1 Source object

Required fields:

| Field | Meaning |
|---|---|
| `object_id` | Stable benchmark identifier, not a local path |
| `source_dataset` | Upstream dataset and version |
| `source_object_id` | Upstream video/object identifier |
| `source_revision` | Commit, release, or archive revision |
| `source_sha256` | Hash when redistribution or deterministic download permits it |
| `duration_seconds` | Validated media duration |
| `media_type` | MIME type or declared modality |
| `license_status` | Cleared, metadata-only, restricted, or pending |
| `provenance` | Selection and acquisition record |

Server-specific absolute paths must never be part of this identity.

### 4.2 Workload

Each independent workload uses one source object and one primary question.
The manifest should contain at least:

| Field | Meaning |
|---|---|
| `workload_id` | Stable unique identifier |
| `object_id` | Source-object reference |
| `stratum_id` | For example causal, temporal, or descriptive |
| `task_class_id` | Versioned task class |
| `question` | Frozen task text |
| `answer_options` | Ordered options for primary deterministic scoring |
| `correct_answer_id` | Ground-truth option identifier |
| `split` | Development, validation, or test |
| `selection_rule_id` | Frozen selection-procedure reference |
| `source_row_id` | Upstream annotation reference |

Primary v0.1 scoring should use multiple-choice exact match. The current
accepted-substring rule may remain as a compatibility diagnostic, but it is
too permissive to define the public leaderboard. Open-ended answers may be
released as a secondary task only with a frozen grader, grader version, and
known uncertainty.

The complete workload definitions, not merely their IDs, are bound into the
frozen plan through a canonical content hash.

### 4.3 Representation

Each `(object, representation)` record should declare:

- `representation_id` and schema version;
- content SHA-256 and byte size;
- media or serialization type;
- generator name and version;
- prompt/configuration hash, seed, and sampling rule when generated;
- source-object hash;
- validation status; and
- redistribution status.

The initial representation domain is:

- source/origin video or origin service response;
- `sampled_frames`;
- `multimodal_digest`; and
- any structured paired view required by a declared design.

Video is the only primary modality in v0.1. A second modality is a later
external-validity extension and should not delay or dilute the first complete
PPD result.

Generated representations must be frozen before Agent outcomes are observed.
Manual review may reject a corrupt or mismatched representation, but must not
rewrite it after seeing whether an Agent answered correctly.

### 4.4 Physical design

The extended finite design domain is:

| Design | Intended placement |
|---|---|
| `D_origin_remote` | Safe incumbent; representations served from the origin endpoint |
| `D_local_frames` | Sampled frames materialized at the execution-local endpoint |
| `D_local_digest` | Structured digest materialized at the execution-local endpoint |
| `D_local_pair` | Frames and digest materialized locally; exploratory in v0.1 |

These labels are not evidence that a path is physically local or remote. A
live run must bind them to distinct endpoint and node identities and verify
the resulting transfer path. Serving all designs from one Data Agent is a
controlled emulator or conformance fixture, not a distributed result.

The first distributed pilot retains the repository's restricted,
stratum-specific comparison:

| Stratum | Safe design | Pilot candidate |
|---|---|---|
| causal | `D_origin_remote` | `D_local_frames` |
| descriptive | `D_origin_remote` | `D_local_frames` |
| temporal | `D_origin_remote` | `D_local_digest` |

`D_local_pair` remains available in the extended offline Oracle but is not a
live Commit candidate in the first pilot.

### 4.5 Observation

A canonical observation must retain:

- frozen trial identity: experiment, trial, session, order, stratum,
  workload, design, repetition, and seed;
- exact source, representation, plan, endpoint-registry, and code digests;
- Agent final answer and deterministic task score;
- all access events and selected representations;
- endpoint identity for every access;
- complete service, fetch, transfer, and felt-latency telemetry;
- artifact-selection and completed-delivery evidence;
- the five-component cost ledger and raw units;
- infrastructure, policy, telemetry, and artifact-failure classification;
- worker/executor and model-version metadata; and
- timestamps sufficient to audit ordering without exposing credentials.

A record is canonical only when:

```text
telemetry_complete is literal true
artifact_delivery_complete is literal true
outcome_type == "completed"
```

Failed attempts belong in a separate durable attempt ledger and are never
scored as completed task observations.

## 5. Cost contract

The live benchmark uses the existing additive contract:

```text
total_cost = service
           + network
           + storage
           + amortized_materialization
           + transition
```

Every component keeps its raw quantity, unit, conversion rule, provenance,
and value kind. A component is one of `measured`, `configured`, `derived`,
`not_applicable`, or `unavailable`.

- `not_applicable` may produce a justified zero.
- `unavailable` makes `total_cost` unavailable; it must not make a design
  appear free.
- Artifact transfer is charged to either service or network, never both.
- Materialization cost is amortized over a declared session horizon.
- All rates and the horizon are frozen before outcome collection.

The benchmark release should publish raw resource vectors in addition to any
converted cost. This allows later users to apply a different, declared price
schedule without altering the original measurements.

## 6. Corpus construction and splits

### 6.1 Upstream source

NExT-QA is the initial proposed workload source because it includes causal,
temporal, and descriptive VideoQA over independent videos. The official
project reports 5,440 videos and approximately 52,000 manually annotated QA
pairs. The annotations, paper, and source links are available from the
[official NExT-QA project](https://doc-doc.github.io/docs/nextqa.html) and
[official repository](https://github.com/doc-doc/NExT-QA).

NExT-QA is a source corpus, not the contribution being claimed. PathfinderBench
adds versioned representations, placement interventions, complete system
telemetry, finite-domain Oracle observations, and optimizer evaluation.

### 6.2 Existing 16-workload seed

The current v1/v2 workloads were used during method and system development.
They must be labeled `development_seed` and excluded from the headline public
test result. They remain useful for:

- schema examples;
- offline unit and integration tests;
- baseline debugging;
- representation-generation validation; and
- live deployment preflight.

Their outcomes must not influence selection of the fresh evaluation cohort.

### 6.3 Proposed v0.1 evaluation cohort

The proposed target is **64 fresh, independent video workloads**, with one
primary QA per video. A suggested prespecified allocation is:

| Stratum | Workloads |
|---|---:|
| causal | 24 |
| temporal | 24 |
| descriptive | 16 |
| **Total** | **64** |

The exact quota may change after an annotation and licensing audit, but must
be frozen before any Pathfinder execution on the selected cohort.

All splits are disjoint by source video. A proposed split is:

| Split | Workloads | Use |
|---|---:|---|
| public development | 32 | Baseline development and format inspection |
| public validation | 16 | Hyperparameter selection and ablation |
| held-out test | 16 | Final evaluation; answers and Oracle decisions withheld from submissions |

With only 64 independent objects, v0.1 is a research benchmark pilot rather
than a population-level confirmatory study. Repetitions estimate within-
workload execution variability and never increase the independent sample
count.

### 6.4 Collection sizes

Collection proceeds through increasingly expensive gates:

1. **Conformance slice:** 8 development workloads x the safe design and one
   prespecified stratum-specific candidate x 1 repetition = 16 sessions.
2. **Restricted distributed pilot:** 30--50 fresh workloads x 2
   stratum-specific designs x 2 repetitions = 120--200 sessions.
3. **Extended v0.1 reference:** 64 fresh workloads x 4 designs x 2
   repetitions = 512 sessions.

The 512-session target is executed only after the conformance slice and
restricted pilot establish that live telemetry and cost accounting are
credible. It must not be launched merely to produce a larger number.

## 7. Evaluation protocol

### 7.1 Frozen inputs

Before the first observation, freeze and hash:

- source selection rule and exclusion list;
- workload manifest, including complete content;
- representation manifests;
- train/validation/test assignment;
- physical-design definitions;
- endpoint registry and execution-node identity;
- cost rates and provenance;
- task-scoring rule;
- utility profiles and safety margins;
- candidate set and safe design;
- repetitions, seeds, trial ordering, and retry budget;
- code revision and container image digests; and
- model and Agent configuration identifiers.

Credentials, reusable artifact handles, runtime databases, and worker config
files are never included in a frozen public plan.

### 7.2 Live-run admission gate

A live run begins only when:

1. every required endpoint is healthy and reports its expected node and
   catalog identities;
2. the worker is visible to the Root Server and receives a throwaway task;
3. the worker can reach every declared Data Agent endpoint;
4. endpoint placement has no fallback or ambiguity;
5. all cost conversions have non-placeholder provenance;
6. complete artifact downloads produce nonzero, reconciled transfer evidence
   when a payload is expected;
7. the MCP Gateway and runner use the same registry, system configuration,
   Gateway state, and telemetry timeout; and
8. canonical recovery passes interruption and duplicate-prevention tests.

The benchmark code never starts or stops shared services or workers. Their
lifecycle remains an explicit operator action.

### 7.3 Failure handling

- Infrastructure failures are recorded separately and retried only within a
  frozen budget.
- Policy violations and cross-endpoint handle use fail closed.
- Telemetry timeout never produces a complete observation.
- Artifact selection without completed download is an artifact-delivery
  failure, not a task failure.
- A crash after canonical fsync but before journal completion replays the
  exact durable record and does not execute the cell twice.
- Missing measurements are unavailable, not zero.

### 7.4 Oracle construction

The Reduced Oracle exhaustively evaluates the declared finite design set.
For each workload/design block it records task success, Agent demand, complete
resource cost, transition cost, restoration cost, and safe-design state.

The Oracle is:

- finite-domain, not globally optimal;
- deployment-specific for live raw measurements;
- versioned by every input and environment digest; and
- unavailable for a cell whose telemetry or delivery is incomplete.

Public test Oracle labels may be held back for leaderboard evaluation, while
development and validation Oracle data remain available for reproducibility.

## 8. Metrics and reporting

### 8.1 System and task metrics

Report at least:

- task accuracy, overall and by stratum;
- access rate and representation-selection distribution;
- completion rate;
- infrastructure-, telemetry-, and artifact-failure rates;
- Data Agent service and fetch latency;
- end-to-end felt latency;
- completed transfer bytes;
- stored and materialized bytes;
- transition and restoration time; and
- every raw and converted cost component.

Latency should include median and tail statistics. Means remain available for
cost aggregation but must not hide tail behavior.

### 8.2 Optimizer metrics

Report at least:

- value or utility relative to `D_origin_remote`;
- regret to the declared Reduced Oracle;
- cumulative exploration cost;
- number and tier of Reveals;
- time and observations to Commit or Stop;
- safe-design violations;
- unsafe/false-safe Commit rate;
- abstention or certificate-limited Stop rate;
- confidence-set simultaneous coverage; and
- result by workload stratum.

### 8.3 Reporting hierarchy

PathfinderBench should not hide all outcomes in one score. The release should
report results in the following order:

1. **Primary live PPD result:** baseline-normalized task quality, demand shift,
   resource use, value, and reliability on the reference deployment.
2. **Supporting offline optimizer result:** regret, exploration cost,
   confidence coverage, and safe decisions under frozen traces.
3. **Diagnostic task-resource frontier:** quality, latency, bytes, and all
   component costs, reported by workload stratum and physical design.

For users who need a scalar ranking, publish a small set of prespecified
utility profiles, such as balanced, cost-sensitive, and latency-sensitive.
Their weights and units must be frozen before test evaluation, and the raw
metric vector remains authoritative. The three reports are not independent,
equally weighted paper contributions.

### 8.4 Evidence and claim ladder

Every result must state the highest evidence level it reaches:

1. **Artifact reproducibility:** public files reproduce an offline score.
2. **Live conformance:** the distributed path executes with complete,
   fail-closed telemetry and auditable recovery.
3. **Causal PPD evidence:** prespecified within-workload physical-design
   interventions change demand or resource use while task quality and all cost
   components are measured under the same deployment contract.
4. **Optimizer evidence:** held-out AWM certificates and OED decisions are
   evaluated against the declared Reduced Oracle without post-hoc retuning.

Reaching a lower level never implies a higher one. The first paper's headline
result requires Levels 3 and 4; the offline mode alone reaches Level 1.

## 9. Required baselines

The initial release should include:

1. always retain `D_origin_remote`;
2. choose the lowest advertised price;
3. choose the lowest measured latency from training observations;
4. random feasible Reveal under the same budget;
5. naive read-react-materialize;
6. passive AWM without active probes;
7. an equal-budget independent-box or bandit baseline;
8. current certificate-gated AWM + OED; and
9. the exhaustive Reduced Oracle upper reference.

Every baseline receives the same training split, candidate set, safe design,
budget, and failure information. A baseline may not inspect held-out Oracle
outcomes or reusable artifact handles.

## 10. Licensing, provenance, and responsible release

Licensing is a release gate, not an afterthought.

The official NExT-QA repository identifies its code license as MIT and states
that dataset use and distribution should cite the paper and source. It also
states that the raw videos originate from NExTVideo/VidOR. These facts do not,
by themselves, prove that Pathfinder may re-host every MP4 or generated
derivative under the repository's software license.

Before release, separately audit:

1. QA annotation redistribution;
2. raw-video redistribution;
3. generated frame descriptions and digests;
4. model-provider terms for generated representations; and
5. benchmark code and documentation licensing.

Until raw-media redistribution is clearly permitted, publish:

- upstream IDs and revision-pinned download instructions;
- checksums where permitted;
- selection and exclusion manifests;
- workload and scoring metadata permitted by the source terms;
- representation generation code and hashes; and
- telemetry and benchmark traces that contain no copyrighted media payload.

The dataset card should also document content characteristics, potential
privacy concerns, known demographic/domain limitations, a correction and
takedown contact, and a procedure for removing an upstream object without
silently changing a released version.

## 11. Public release package

A v0.1 release should contain:

```text
pathfinderbench-v0.1/
  README.md                         benchmark card
  DATASET_CARD.md                   source, license, limits, governance
  CHANGELOG.md
  CITATION.cff
  LICENSES/
  manifests/
    sources.jsonl
    workloads.jsonl
    representations.jsonl
    designs.json
    splits.json
    content-hashes.json
  offline/
    training-observations.jsonl
    validation-oracle.csv
    transition-costs.jsonl
    failure-scenarios.jsonl
  live/
    reference-environment.json
    sanitized-observations.jsonl
    placement-and-path-evidence.jsonl
    attempt-summary.json
  schemas/
  baselines/
  evaluator/
  examples/
  SHA256SUMS
```

Subject to licensing, media and large representations may live in a separate
versioned dataset repository. The code repository should contain download and
verification tooling rather than generated runtime output.

The public package must exclude:

- LLM or FlowMesh credentials;
- inline endpoint tokens or private URLs;
- reusable artifact handles;
- worker deployment configuration;
- raw runtime databases;
- service logs containing request data;
- server-specific absolute paths; and
- unreviewed generated caches or build products.

## 12. Reproducibility and integrity

Every official result should identify:

- benchmark version;
- workload-content, representation, endpoint-registry, and plan SHA-256;
- source Git revision;
- Python, SDK, MCP, Agent, and model versions;
- container image digest where applicable;
- cost model and rate provenance;
- execution-node and Data Agent node identities in sanitized form;
- random seeds and trial ordering;
- retry and timeout configuration; and
- canonical and attempt-ledger checksums.

The evaluator must reject:

- workload content that does not match the frozen plan;
- a canonical record that does not match its exact frozen trial;
- incomplete telemetry or artifact delivery;
- duplicate or conflicting canonical records;
- unknown or missing cost components;
- a stale or ambiguous worker pin; and
- a result produced under a different benchmark version.

Official releases are immutable. Corrections produce a new semantic version
with a migration note and retained checksums for prior versions.

## 13. Implementation plan and gates

### Phase 0: Scope, specification, and licensing

- Review this proposal with the advisor.
- Confirm that live PPD evidence is primary and offline replay is supporting.
- Confirm that the benchmark packages the paper contributions rather than
  becoming a separate generic systems-benchmark contribution.
- Complete the NExT-QA/VidOR and derivative-data licensing audit.
- Freeze workload, representation, observation, and submission schemas.

**Exit gate:** the release boundary is legally and scientifically explicit.

### Phase 1: Schema and evaluator scaffold

- Package the existing 16 workloads as a clearly labeled development seed.
- Export sanitized, content-hashed traces from the frozen controlled Oracle.
- Implement a clean-machine offline evaluator.
- Publish reference baselines and golden expected outputs.

**Exit gate:** a user without private services can reproduce all development
scores from public files, and the same schemas can accept later live records.
This scaffold is not the primary benchmark result.

### Phase 2: Eight-workload distributed conformance slice

- Deploy two Data Agent nodes with distinct node and placement identities.
- Run eight development workloads over the safe design and their
  stratum-specific candidate once each; keep `D_local_pair` excluded.
- Exercise artifact transfer, component-cost accounting, failures, and resume.

**Exit gate:** every canonical record is complete, physical paths are
verified, failures are correctly separated, and interruption never duplicates
an observation.

### Phase 3: Restricted 30--50 workload distributed pilot

- Select fresh video-disjoint workloads with a frozen rule.
- Run only the predeclared safe and stratum-specific candidate designs.
- Estimate live variance, failure rate, and realistic collection cost.
- Apply the frozen certificate and OED replay without retuning.

**Exit gate:** the system is operationally credible and a justified v0.1
sample-size and cost plan can be frozen. This remains a pilot, not
confirmatory evidence.

### Phase 4: PathfinderBench v0.1 collection and release

- Freeze the fresh 64-workload cohort and splits.
- Collect the extended finite-domain reference data if the pilot gates pass.
- Validate, redact, checksum, and freeze the release package.
- Publish the primary live reference result together with the supporting
  offline replay package, dataset/benchmark card, evaluator, and baselines.

**Exit gate:** the public artifact is independently runnable, auditable,
license-cleared, and accurately labeled with its claim boundary.

### Phase 5: Later confirmatory expansion

Use v0.1 only for power and operational planning. A later confirmatory study
should use a wholly fresh workload cohort and a sample size justified by the
pilot. Current calibration suggests that conservative safe Commit claims may
require on the order of 100 independent workloads per stratum; the exact
number must come from the frozen pilot analysis rather than this proposal.

## 14. Acceptance criteria for PathfinderBench v0.1

The first public release is ready only when:

- the source and derivative-data release rights are documented;
- the workload selection rule and video-disjoint splits are frozen;
- all public artifacts have schemas and checksums;
- the offline evaluator runs without private infrastructure;
- at least the required baselines reproduce from a clean environment;
- the primary live data comes from verified distinct placements;
- every scored observation has complete telemetry and artifact delivery;
- cost components have measured or justified provenance;
- attempt failures are separated from canonical scientific observations;
- no secret, private endpoint, reusable handle, or server path is present;
- the safe design is retained for every uncertified decision; and
- the dataset card states what the benchmark does and does not establish.

## 15. Decisions requested from the advisor

The following decisions should be made before implementation expands:

1. Does the proposed hierarchy -- live end-to-end PPD as primary evidence and
   offline trace replay as the supporting reproducibility/method mode -- match
   the intended positioning of the first paper?
2. Should PathfinderBench be described as the public artifact packaging the
   paper's PPD, AWM/OED, and system contributions, rather than as an additional
   independent contribution?
3. Is NExT-QA acceptable as the initial source corpus, subject to license
   clearance, or should the project prioritize media with a simpler explicit
   redistribution license?
4. Is a 64-workload v0.1 appropriate, or should the first public version be a
   smaller pilot release followed by a larger numbered release?
5. Should the held-out test use a hosted evaluator, or should v0.1 prioritize
   fully public reproducibility and defer a hidden leaderboard?
6. Which deployment should define the official live reference environment?
7. Which cost and success margins correspond to an operationally meaningful
   decision rather than an engineering placeholder?

## 16. Relationship to current repository evidence

This proposal is the public release layer for the original contribution stack;
it does not add an unrelated benchmark objective or alter the current
evidence:

- the 128-session v1 and 256-session v2 controlled Oracles are single-host
  engineering evidence;
- the current AWM/OED result correctly retains the safe design because the
  independent-workload evidence is insufficient for Commit;
- the distributed runner, endpoint registry, total-cost ledger, FlowMesh
  seam, preflight, and audited recovery path are implemented and tested
  offline; and
- no live multi-node Pathfinder pilot has yet established real distributed
  placement savings.

The immediate next experimental step remains the eight-workload distributed
conformance slice. Benchmark specification and licensing proceed in parallel
so that the live system records every field the eventual public artifact
requires. Offline packaging can be implemented concurrently, but the primary
benchmark claim remains conditional on the live PPD evidence.

## References

- [Original research design and intended contributions](AUTORESEARCH_MULTIMODAL_DATALAKE.md)
- [Original evaluation plan](EVALUATION_PLAN_MULTIMODAL_DATALAKE.md)
- [First-paper research roadmap](RESEARCH_ROADMAP_MULTIMODAL_DATALAKE.md)
- [Pathfinder system design](SYSTEM_DESIGN_MULTIMODAL_DATALAKE.md)
- [Development and release checklist](DEVELOPMENT_CHECKLIST_MULTIMODAL_DATALAKE.md)
- [NExT-QA project page](https://doc-doc.github.io/docs/nextqa.html)
- [NExT-QA official repository](https://github.com/doc-doc/NExT-QA)
- [NExT-QA CVPR 2021 paper](https://openaccess.thecvf.com/content/CVPR2021/html/Xiao_NExT-QA_Next_Phase_of_Question-Answering_to_Explaining_Temporal_Actions_CVPR_2021_paper.html)
- [Pathfinder PPD validation protocol](PPD_VALIDATION_PROTOCOL.md)
- [Pathfinder distributed pilot plan](DISTRIBUTED_PILOT_PLAN.md)
- [Current project progress and initial findings](PROJECT_PROGRESS_AND_INITIAL_FINDINGS.md)
