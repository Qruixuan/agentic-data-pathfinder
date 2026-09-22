# Pathfinder RSI-Exam Offline Replay

This module turns checksum-verified Pathfinder execution accounting into a
self-contained policy task.  It replays measured physical-path outcomes; it
does not run a simulator, contact FlowMesh, call a model, or read source video.

The first package is deliberately a one-case conformance fixture.  It proves
the observation/action/outcome boundary and state accounting, but it is not a
scientific policy benchmark.  A formal RSI-Exam dataset still requires an
outcome-blind, video-disjoint multi-case collection.

## What the RSI agent may change

The agent implements only a policy:

```text
public observation + current replay state -> physical action ID
```

The observation contains the workload stratum, available physical actions,
remaining query horizon, index availability, and cache state.  It does not
contain `task_success` or any other outcome.  The evaluator owns the frozen
outcomes and reveals one only after an action has been selected.

The agent cannot change task answers, hidden labels, scoring, trace contents,
representations, index contents, or the evaluator.  Unknown action/state cells
return `unsupported_action`; no interpolation or synthetic counterfactual is
allowed.

## Package layout

An immutable package contains exactly:

- `replay-manifest.json`: version, source commitments, splits, objective, and
  claim boundaries;
- `cases.jsonl`: public case observations, initial state, and index-build
  accounting;
- `actions.jsonl`: public physical action descriptors;
- `outcomes.jsonl`: measured post-action results;
- `README.md`; and
- `SHA256SUMS` using canonical LF bytes.

The source video, prompts, answers, hidden labels, credentials, signed URLs,
container environments, and private host paths are not part of the package.
`task_success` is a public boolean outcome; neither the predicted answer nor
the correct answer is included.

## State and cost semantics

The replay supports two modes.

### `independent-query`

Every query begins from the case's declared initial state.  A missing temporal
index is built and charged again whenever an indexed action is selected.  A
read-through cache begins cold.  This mode exposes the concern that an index
may not be economical for a single video and a single question.

### `shared-dataset-sequence`

Index and cache state persist between questions.  The first indexed query pays
the recorded one-time build bytes; subsequent indexed queries pay only the
recorded query-time projection bytes.  A cache miss warms the selected node,
so the next cache access uses the separately measured hit outcome.

Index construction and querying remain separate in every result.  The pilot
measured build and query bytes but did not measure index-build latency, so
build latency is `null`, not zero.  It also did not perform a partial MP4 read:
the complete object was read once to build the projection.  The package makes
no source-storage-I/O reduction claim.

## Objective and included baselines

The default objective is lexicographic:

1. meet the case's quality constraint; then
2. minimize total source bytes; then
3. minimize model-input bytes.

No implicit weights are used.  Included deterministic baselines are
always-direct-video, always-indexed, always-derived, myopic-cost-first, and an
amortization-aware heuristic.  A seeded random baseline is included as a
reproducibility check.  The myopic policy is intentionally weak: it prefers a
small derived representation and ignores future reuse and observed quality
risk.

## Build and verify the one-case fixture

Use the exact Git commit whose clean archive supplies the command.  The
accounting directory must verify before it is consumed.

```powershell
$BuilderCommit = (git rev-parse HEAD).Trim()
$ExperimentCommit = "59cf0ce8ff2bbee21dfc3f79bd08c3647384f657"
$Accounting = "artifacts/temporal-index-v2-6fb1a99/temporal-index-cost-accounting-v2c"
$Output = "artifacts/temporal-index-v2-6fb1a99/offline-replay-v1-$($BuilderCommit.Substring(0,8))"

python -P -m pathfinder build-rsi-exam-offline-replay `
  --accounting-dir $Accounting `
  --source-commit $ExperimentCommit `
  --builder-commit $BuilderCommit `
  --package-id "pathfinder-rsi-exam-one-case-v1" `
  --output-dir $Output

python -P -m pathfinder verify-rsi-exam-offline-replay `
  --package-dir $Output `
  --accounting-dir $Accounting
```

The output directory must not already exist.  A correction is written to a
new immutable directory rather than overwriting a package.

## Run offline

These commands require no network or service credentials:

```powershell
python -P -m pathfinder run-rsi-exam-offline-replay `
  --package-dir $Output `
  --policy always-indexed `
  --mode independent-query `
  --queries 1

python -P -m pathfinder run-rsi-exam-offline-replay `
  --package-dir $Output `
  --policy always-indexed `
  --mode shared-dataset-sequence `
  --queries 10

python -P -m pathfinder compare-rsi-exam-offline-replay-baselines `
  --package-dir $Output `
  --mode shared-dataset-sequence `
  --queries 10 `
  --seed 7
```

Policy and outcome sampling use explicit seeds.  Repeating the same command
with the same package and seed produces the same result.

## Extending to the proposal dataset

Future collection should select videos without looking at task outcomes,
retain temporal, causal, and descriptive strata within one VideoQA workload,
and execute the same frozen action set for every eligible case.  Model-backed
actions need repeated runs so the package can store empirical distributions
rather than a single outcome.

Objects, not individual questions, form the train/development/test split.  The
verifier rejects an object appearing in more than one named split.  Traces are
frozen before policy development, test outcomes remain evaluator-only, and
missing action cells stay explicit instead of being filled by a model.

The resulting RSI-Exam task can then test whether a policy generalizes across
held-out videos while deciding when to build an index, when to reuse it, when
to warm or read a cache, and which representation and placement satisfy a
quality constraint at the lowest measured cost.  Users need only the replay
package and this repository; they do not need Pathfinder's live deployment.

## Freeze the multi-case collection before running it

The proposal cohort is defined in
`configs/rsi_exam_proposal_collection_v1.json`. It requests 12 distinct
videos: causal, temporal, and descriptive each contribute two train, one
development, and one test case. Every selected case is collected three times
over the same ten measured action/state cells (D0--D7, with separate D3/D7
cache miss and hit observations).

Selection is deterministic from public task metadata and a frozen seed. A
video can appear in only one split. The planner does not accept task outcomes,
predictions, hidden labels, runtime evidence, or credentials as inputs.

Audit a candidate pool before freezing anything:

```powershell
python -P -m pathfinder audit-rsi-exam-trace-collection-candidates `
  --public-task-set PUBLIC_TASKS.json `
  --cohort-spec configs/rsi_exam_proposal_collection_v1.json
```

The formal plan fails closed until all three strata have four distinct public
video candidates. Once the audit is ready, use the exact commit and a clean
Git archive as required by `EXPERIMENT_OPERATIONS_RUNBOOK.md`:

```powershell
$Commit = (git rev-parse HEAD).Trim()

python -P -m pathfinder freeze-rsi-exam-trace-collection-plan `
  --public-task-set PUBLIC_TASKS.json `
  --cohort-spec configs/rsi_exam_proposal_collection_v1.json `
  --builder-commit $Commit `
  --output-dir NEW_IMMUTABLE_PLAN_DIR

python -P -m pathfinder verify-rsi-exam-trace-collection-plan `
  --plan-dir NEW_IMMUTABLE_PLAN_DIR `
  --public-task-set PUBLIC_TASKS.json `
  --cohort-spec configs/rsi_exam_proposal_collection_v1.json `
  --builder-commit $Commit
```

The resulting plan is still marked `execution_authorized: false`. Before
each live collection, the operator must complete the runbook gates, create a
fresh run identity and cache state, and bind the case-specific index and
representations. The plan never authorizes a FlowMesh submission by itself.

`configs/rsi_exam_three_case_collection_fixture_v1.json` is a smaller
conformance-only specification for the three currently available proposal
strata. It checks the planner but is not a substitute for the 12-video
proposal cohort.
