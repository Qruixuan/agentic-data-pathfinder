# Fresh video-disjoint multi-question experiment

Status: PAID_PREPARATION_RUNNING; no route workflow submitted yet.

## Current resume state

- Bounded N6 preparation is running under source `a00bb13`, SSH session
  47820. Do not start a duplicate. Remote root:
  `/home/pathfinder/h48-paid-20260924-v1`, output journal under
  `output/provider-journal/`. The launcher runs captions, video-index, query
  serially and stops on failure; cached paid responses survive interruptions.
- N1 private 12-question oracle was built and verified on N1 only, at
  `/opt/pathfinder/formal/private/h48-oracle-20260924-v1/oracle/n1-oracle-package`.
  Public commitment archive is `.codex_build/h48-n1-public-commitment.tar`,
  SHA-256 `a4e39dd73aa2e116f82806bccd2a21b02a948ab4dab19c66db612d9baf371945`.
  Hidden values were not returned or copied off N1.
- The complete plan/build input archive is `.codex_build/h48-paid-input.tar`,
  SHA-256 `79d5994138a6f0cc9f880dd1b509cee2d5598e197d78535dfd167573e18b1c99`.
  Use this verified archive; `.codex_build/h48-plan.tar` is an incomplete
  failed Windows-ACL export and must not be used.
- Next public finalizer: `experiments/finalize_multiq_inputs.py`, no network
  calls. Captures per-question projection timings separately from verifier
  re-decodes; N4 digest and frames reuse the caption-preparation products.

- Active selection: `artifacts/multiq-fresh-holdout-20260924-v3-public-selection`.
- Exposure inventory: 52 prior object IDs; zero unreadable public manifests.
- Active videos: 8811725760, 10607095936, 6143391925, 2400715506.
- V1/v2 selection attempts are preserved, unexecuted. Two original candidates
  exceeded N6's 7,000,000-byte limit. V3 uses the same seed, adds only this
  runtime eligibility check against the pinned archive inventory, and does
  not use outcomes. All final MP4s passed archive CRC and SHA-256 checks.
- Active plan: `artifacts/h48-inputs-v2/plan`, plan SHA-256
  `7f70d9b02252f0944c26ea097c1dd1795892bbd48963c1b141ad565c3f0ffeec`.
  Twelve questions, 48 routes, canonical verifier passed.
- Rate snapshot: `artifacts/h48-cloud-rate-snapshot-v1`, retrieved before
  materialization from UpCloud; nine VMs in sg-sin1 are started.
- Offline build completed on N5, isolated read-only/no-network container,
  clean source revision 28b5ede. Remote parent:
  `/home/pathfinder/h48-20260924-v1/output/build`; local verified copy:
  `artifacts/h48-inputs-v2/build`. Raw import and 36-window frame preparation
  passed canonical verification. Actual decode wall time 4.450813 s, process
  CPU 4.341070 s; event journal retained beside, not inside, packages.
- Source archive SHA-256:
  `a145c8f830422caac16e405d9758a55d9f0f4f0eea71a843d4d243c3fa8dec06`.
- Public N5 output archive SHA-256:
  `ab8bf28d0b8405a9ab5ec4f0f7c08f1c998a2677d98f5a0fddd2075bb4b4b386`.
- Provider limits and baseline rules are in `execution-protocol.json`:
  36 windows, at most two caption attempts/window; four video embeddings;
  twelve query embeddings; 48 workflows, up to three attempts/inference
  under the unchanged N6 transport. Max 232 provider attempts including all
  existing transport retries, not a claim that 232 calls are needed.
- N6 SSH timeout was transient; a repeat read-only probe passed. No container
  was restarted/recreated; N5 only ran two disposable offline containers.
- Next: freeze this implementation/protocol; isolated bounded caption/index/
  query build with immediate usage persistence, then N3/N4/N2/N1 inputs and
  source-bound admission, deployment gates, one 48-route run, cost/replay.

## Frozen design constraints

- Existing 24-route and 28-route observations are development/exploratory
  data. Preserve them; do not relabel them as the new held-out test.
- Select four previously unused videos, one causal, one temporal and one
  descriptive question per video, outcome-blind with a fixed seed.
- Twelve questions, four arms R/D/DC/I, 48 planned route submissions.
- Use `experiments.batch`, not a new dated runner. Freeze a unique batch
  identity and one shared DC cache episode before execution; run serially.
- Exclude every identifiable prior video exposure, including the original
  four-video pilot and the entire twelve-video formal collection.
- Baselines: always-R, always-D, always-DC, always-I, first-R-then-I.
- No test-outcome-driven replacement, tuning, retries or answer inspection.
- Freeze exact provider request limits and price/allocation basis before any
  paid preparation. Forty-eight routes is not a total API-request budget.
- Record one-time derived/index build, per-question selection/projection,
  per-route N6 tokens, VM/Root experiment-time allocation and applicable
  storage/network units. Unknown components remain null, never zero.
- Reuse the historical 2026-09-23 Singapore model list-price snapshot for
  comparability, explicitly not as a claim about today's price or invoice.
- Preserve all previous evidence, packages, images, volumes and user changes.
  No changes to the FlowMesh repository.

## Entry state

- Source HEAD: 28b5ede8e6aa66e0512d5173c4ee8dfd6359db90.
- Pre-existing tracked edits: experiment runbook, RSI CLI and its test.
- Entire experiment runbook read before experiment actions.
- Shared runner supports configurable 48-route counts. Historical cost and
  replay recipes still contain 28-route/7-question assumptions and must not
  be reused unchanged.

## Attempts and immediate next actions

### Current resume point (supersedes the initial entry steps below)

- Fresh outcome-blind v3 selection: 4 new videos, 12 questions, 48 routes.
  Public packages: `artifacts/h48-runtime-v1`, 169 checked files; remote
  `/home/pathfinder/h48-public-20260924-v1` on N2/N3/N4/N6/N7.
- Preparation complete: 36 caption calls + 4 video embedding calls + 12
  query embedding calls; all responses/usage and machine time persisted.
  Local paid journals: `artifacts/h48-paid-evidence-v1/output`.
- N1 oracle was built/verified on N1 only; no label exported. N1/N2/N3/N4/N7
  isolated services deployed and healthy. N6 and shared DC cache unchanged;
  new video identities and frozen episode isolate this experiment.
- Both source-bound gates and 12 dependency/auth probes passed. Worker is
  wkr-2 under the pinned alias. Original route output on N7:
  `/home/pathfinder/h48-runner-20260924-v1/output/routes`.
- 13 routes COMPLETE, ordinal 13 terminal provider rejection, not a bug:
  `400 data_inspection_failed`. Exact binding and sanitized diagnostic are
  in `provider-rejection-13.json`. No accuracy outcomes inspected to choose
  a repair or sample. No retry or input modification is permitted.
- Shared runner now validates/imports a terminal prefix into a NEW output
  and continues only untouched slots. Submission ceiling remains 48.
  Next: stage the runner-only update; repeat no-call readiness; execute the
  remaining 34 slots, verify terminal observations separately from scores.
- Afterward export numeric N6 usage/attempts; join by request/result digests;
  run `experiments.interleaved_cost_replay` and five frozen baselines.
  Failed request usage remains unknown, not zero. Cost tables and full
  baseline results are not yet generated. No source-bound module changed.

1. Read-only SSH to the documented Root was denied by the local sandbox
   before authentication (`connect ... port 22: Permission denied`). No
   inference about server health or credentials follows. Retry through the
   approved network permission mechanism, then inspect public source/media
   availability and current service health.
2. Freeze an exposure inventory and outcome-blind selection protocol before
   exporting public candidates from the official CSV on N1.
3. Resolve new media availability before paid preparation. Do not substitute
   previously used videos if fresh media are unavailable.
