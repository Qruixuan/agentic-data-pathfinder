# Next Distributed Pilot: Prerequisites and Admission Gates

## Purpose and claim boundary

The next stage is a **restricted prospective distributed pilot**, not the
final public benchmark and not a confirmatory `SAFE_TO_COMMIT` study. Its
purpose is to test the current PPD workload on fresh, video-disjoint objects
with a deterministic primary task score and the real two-node data path.

The proposed manageable design is:

- 36 fresh NExT-QA videos: 14 causal, 14 temporal, and 8 descriptive;
- one primary multiple-choice question per video;
- the safe `D_origin_remote` design and one prespecified candidate per
  stratum;
- two repetitions per design; and
- 36 x 2 x 2 = 144 canonical sessions.

The existing 16 development videos and every smoke workload are excluded from
the prospective cohort. Repetitions measure execution variability but do not
increase the independent sample size above 36.

## Decisions requiring approval before cohort freeze

The proposed defaults are recorded in
`configs/pathfinderbench_v0_1_scoring.json`. Before selecting or executing the
cohort, confirm with the advisor:

1. primary task scoring is option-ID exact match;
2. the restricted pilot reports a metric vector rather than a single
   leaderboard score;
3. engineering thresholds remain `delta_success_margin = 0.05`,
   `minimum_cost_saving = 0.25`, and `alpha = 0.05`;
4. the 14/14/8 stratum allocation is acceptable; and
5. the run remains pilot evidence and is not described as confirmatory.

If any item changes, update the draft contracts before cohort selection and
give the run a new pilot ID.

## Gate 1: deterministic workload selection

Use
`configs/pathfinderbench_restricted_pilot_v0_1_selection.json` as the draft
selection protocol. The final protocol must:

- pin the upstream repository revision and annotation-file SHA-256;
- exclude all 16 videos used in v1/v2 development;
- select exactly one row per previously unseen source video;
- fill the prespecified stratum quotas using only official row order;
- avoid all Pathfinder outcomes, AWM/OED results, costs, and latency during
  selection; and
- record the selected upstream row ID and selection-rule ID.

After selection, independently verify that the 36 `object_id` values are
unique and do not overlap the exclusion list.

Once the pinned `val.csv` is present, construct the cohort without reading
any Pathfinder result:

```bash
python -m pathfinder prepare-benchmark-cohort \
  --selection-config configs/pathfinderbench_restricted_pilot_v0_1_selection.json \
  --annotation-csv "$PF_BENCHMARK_SOURCE/val.csv" \
  --output-dir "$PF_BENCHMARK_ROOT/cohort-v0.1"
```

The command verifies the source annotation hash, applies the frozen quotas in
official row order, rejects repeated videos, and refuses an existing output
directory. It writes `workloads.json`, a video-preparation-compatible
`selection.json`, `video_ids.txt`, `cohort_manifest.json`, and `SHA256SUMS`.
Run `sha256sum -c SHA256SUMS` from the output directory before proceeding.

## Gate 2: exact-match workload contract

Every entry in `workloads.json` must have this shape:

```json
{
  "causal-nextqa-val-EXAMPLE-q1": {
    "object_id": "nextqa-val-EXAMPLE",
    "question": "Why does the person open the door?",
    "answer_options": [
      {"option_id": "A", "text": "to leave the room"},
      {"option_id": "B", "text": "to let another person enter"},
      {"option_id": "C", "text": "to look outside"},
      {"option_id": "D", "text": "to close a window"},
      {"option_id": "E", "text": "to turn off a light"}
    ],
    "correct_answer_id": "B",
    "task_class_id": "video_qa",
    "quote_profile_id": "as_designed",
    "latency_multiplier": 1,
    "source_video_id": "EXAMPLE",
    "source_row_id": "official-val-row-EXAMPLE",
    "selection_rule_id": "pathfinderbench-restricted-pilot-v0.1-36",
    "split": "restricted_pilot"
  }
}
```

Option IDs use uppercase ASCII tokens. The scorer removes leading and trailing
whitespace only. `B` is correct when `B` is the declared answer; `b`, `[B]`,
`B.`, and `The answer is B` are incorrect. No heuristic extraction or partial
credit is used. The old `accepted_answer_substrings` field must not appear in
an exact-match workload.

The exact scorer and evaluator can be exercised without servers or empirical
data:

```bash
python -m pathfinder create-workload-evaluation-example \
  --success-scoring-rule multiple-choice-option-id-exact-match-v1 \
  --output-dir "$PF_BENCHMARK_ROOT/exact-scoring-example"
```

## Gate 3: representations and licensing

Before running Pathfinder:

- acquire every selected video under the upstream terms;
- validate that every video decodes;
- generate `sampled_frames.json` and `multimodal_digest.txt` with a frozen
  model, prompt, sampling rule, retry protocol, and generator revision;
- record source-video and representation SHA-256 values and byte sizes;
- verify both Data Agent catalogs report the expected object/catalog
  versions; and
- decide what can be redistributed. Public metadata and hashes do not by
  themselves grant permission to redistribute source video.

The complete representation manifest must be hashed before the first cohort
execution.

If local preparation is interrupted after complete per-object files have been
written, `pathfinder.video_prep --resume-from <checkpoint>` may reuse only an
operator-frozen checkpoint containing `INTERRUPTION.json` and a complete
`INTERRUPTED_SHA256SUMS`. Recovery verifies every checkpoint hash, the source
video hashes, model, prompt, sampling contract, and representation structure
before making an inference request. It calls the model only for missing
objects, leaves the checkpoint unchanged, and records the recovery in the
final generation manifest. Historical protocol-attempt counts that were not
persisted before interruption remain explicitly null; they are never guessed.
Run the same command with `--audit-resume-only` first to report the reusable
and missing objects without loading an API key or making an inference request.

## Gate 4: physical deployment and measured cost contract

The endpoint registry must identify the origin node, execution node, every
Data Agent endpoint, and the exact placement rule for each design and
representation. URLs and tokens remain environment variables and must never
enter the frozen package.

The restricted pilot uses mixed placement, not a design-wide wildcard for
candidate designs. `D_local_frames/sampled_frames` and
`D_local_digest/multimodal_digest` route to the execution-node Data Agent;
the other representation in each candidate design remains routed to the
origin Data Agent. System-path locations and Data Agent plan-binding locations
must use the endpoint registry labels `origin-remote` and
`local-materialized` exactly. Audit this full design-by-representation matrix
before starting either service.

The measurement manifest must bind the same pilot, preregistration, endpoint
registry, and execution node. Every storage, materialization, transition,
network, and elapsed-time conversion rate needs a deployment-specific value
and provenance. A placeholder rate blocks `live_pilot` admission.

Before the formal cohort, confirm:

- both Data Agents are healthy and expose the expected catalogs;
- the FlowMesh Root sees exactly one current worker for the requested alias;
- the worker uses the same Root and PAT as the client;
- the worker can reach both Data Agent endpoints and the MCP Gateway; and
- one excluded throwaway workload completes with full telemetry and artifact
  delivery.

## Gate 5: immutable preregistration

The exact-match preregistration must include:

```json
{
  "success_scoring_rule": "multiple-choice-option-id-exact-match-v1",
  "benchmark_bindings": {
    "selection_protocol_sha256": "<64 lowercase hex characters>",
    "scoring_contract_sha256": "<64 lowercase hex characters>",
    "representation_manifest_sha256": "<64 lowercase hex characters>"
  }
}
```

It must also freeze the pilot ID, source Git revision, safe and candidate
designs, exclusions, workload IDs by stratum, repetitions, thresholds, total
cost contract, fallback rule, seeds, order, and retry budget. The plan command
binds the full workload content and endpoint registry. Editing any bound input
after the plan exists must require a new pilot ID and output directory.

## Gate 6: admission sequence

Use separate output directories for smoke and formal execution. Do not put an
excluded smoke session into the prospective cohort ledger.

1. Compute and record hashes for the selection, scoring, representation,
   workload, endpoint, measurement, code, Agent config, and worker image
   artifacts.
2. Load the preregistration and build the frozen plan with
   `plan-distributed-pilot`. This validates exact-match workload labels before
   any FlowMesh submission.
3. Run `preflight-distributed-pilot --mode live_pilot` with the endpoint and
   measurement manifests.
4. Run one separate excluded smoke workload and inspect worker receipt,
   routes, telemetry completeness, and artifact delivery.
5. Start or resume the 144-session formal run. Never delete or edit its
   ledgers to recover an interruption.
6. Evaluate the frozen snapshot on the execution node, then reproduce the
   evaluator outputs on the second node and compare `SHA256SUMS`.

## Stop conditions

Do not start, or stop before the next canonical cell, if any of the following
holds:

- an advisor decision above is unresolved;
- the cohort overlaps a development or smoke video;
- scoring, selection, representation, endpoint, or measurement hashes are
  missing;
- an endpoint has the wrong node or catalog identity;
- the worker is absent, stale, duplicated, or connected to another Root;
- any cost provenance is still a placeholder;
- artifact delivery or telemetry is incomplete; or
- a frozen input changed after the output directory was created.

Infrastructure and recovery attempts remain separate from canonical
observations. A failed attempt must never be converted into task failure or
silently promoted into the scientific ledger.
