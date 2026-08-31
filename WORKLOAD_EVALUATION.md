# Reproducible evaluation for the existing Pathfinder workload

Status: first evaluation-script package. Offline, descriptive, standard-library
Python 3.12+. No new workload collection, public leaderboard, AWM/OED method
change, or live execution is introduced.

The immediate goal is simple: another person with the same frozen input should
obtain the same validated tables without our FlowMesh deployment, credentials,
videos, or LLM endpoint. This implements the small starting deliverable of
[the benchmark proposal](PATHFINDERBENCH_PROPOSAL.md), not its full roadmap.

## 1. What is included

- `evaluate-distributed-pilot`: read-only validation and descriptive evaluation
  of a **complete** distributed run using the production record/cost contracts.
- `create-workload-evaluation-example`: a deterministic **synthetic** snapshot
  with three fictional workload objects, two repetitions, twelve canonical
  executions, one failed infrastructure attempt, and one incorrect answer.
- `tests/test_workload_evaluation.py` and the independently hand-calculated
  `tests/fixtures/workload_evaluation/expected.json` arithmetic expectations.
- Existing offline Reduced Oracle / AWM / OED commands remain available, with
  their original evidence boundaries; see section 6.

No real server records are bundled or reconstructed from chat summaries. The
example is format/arithmetic test data, not an empirical benchmark result.

## 2. Quick start without servers or credentials

From the repository root, using Python 3.12 or newer:

```bash
python -m pip install -e .
python -m unittest tests.test_workload_evaluation -q

python -m pathfinder create-workload-evaluation-example \
  --output-dir outputs/workload-evaluation-example-v1

python -m pathfinder evaluate-distributed-pilot \
  --run-dir outputs/workload-evaluation-example-v1/run \
  --preregistration outputs/workload-evaluation-example-v1/config/preregistration.json \
  --endpoint-registry outputs/workload-evaluation-example-v1/config/endpoint-registry.json \
  --workload-manifest outputs/workload-evaluation-example-v1/config/workloads.json \
  --measurement-manifest outputs/workload-evaluation-example-v1/config/measurements.json \
  --output-dir outputs/workload-evaluation-report-v1
```

The base installation has no runtime dependencies. Neither optional `flowmesh`
nor `data-prep` dependencies are required. No environment file is sourced.
After installation, evaluation works without network access. The command blocks
above are Bash; the same CLI flags work on Windows with shell-appropriate line
continuations.

Both commands refuse an existing output directory. To repeat an evaluation,
choose a new report directory; do not delete or modify the frozen input.

Hand-calculated example checks:

| Quantity | Expected |
|---|---:|
| Independent workload objects / canonical sessions | 3 / 12 |
| Attempts: canonical / infrastructure | 12 / 1 |
| Safe task successes | 6 / 6 |
| Restricted-candidate task successes | 5 / 6 |
| Safe mean total cost | 2.1953125 invented units |
| Candidate mean total cost | 1.19921875 invented units |
| Paired success difference, candidate minus safe | -1/6 |
| Paired total-cost difference, candidate minus safe | -0.99609375 |

Arithmetic: network costs one invented unit per 1,024 bytes. Each safe session
costs 2 service units plus 100/1,024 bytes for causal/temporal workloads or
400/1,024 for descriptive. Candidate service costs are 2, 0.5, and 0.25 by
stratum. Only the causal candidate accesses the remote endpoint; every
candidate also incurs 0.1 storage + 0.1 amortized materialization + 0.05
transition. These are deliberately invented, not plausible deployment rates.

## 3. Evaluate the completed two-node conformance snapshot

Required input layout (files are read, never repaired or written):

```text
snapshot/
  config/
    preregistration.json
    endpoint-registry.json
    workloads.json
    measurements.json
  run-v1/
    distributed_pilot_plan.json
    canonical_records.jsonl
    attempt_ledger.jsonl
    cell_journal.jsonl
    run_summary.jsonl
```

Use an existing frozen copy if available. Otherwise ensure the run has finished
and nobody is modifying its records. For the current operator directory:

```bash
cd "$HOME/agentic-data-pathfinder"
source "$HOME/.venvs/pf312/bin/activate"

export PF_EVAL_INPUT="$HOME/pathfinder-distributed-conformance-v1"
export PF_EVAL_OUTPUT="$HOME/pathfinder-evaluations/distributed-conformance-v1-eval-01"

python -m pathfinder evaluate-distributed-pilot \
  --run-dir "$PF_EVAL_INPUT/run-v1" \
  --preregistration "$PF_EVAL_INPUT/config/preregistration.json" \
  --endpoint-registry "$PF_EVAL_INPUT/config/endpoint-registry.json" \
  --workload-manifest "$PF_EVAL_INPUT/config/workloads.json" \
  --measurement-manifest "$PF_EVAL_INPUT/config/measurements.json" \
  --output-dir "$PF_EVAL_OUTPUT"

cat "$PF_EVAL_OUTPUT/report.md"
(cd "$PF_EVAL_OUTPUT" && sha256sum -c SHA256SUMS)
```

For a freeze containing `experiment/config` and `experiment/run-v1`, change
only `PF_EVAL_INPUT` to that `experiment` directory. Do not substitute the
current repository's example configs for the configs frozen with a real run.

No worker, Data Agent, MCP, SSH connection to a second node, or FlowMesh token
is needed. This command will not rerun any of the sixteen conformance sessions.
On success it returns exit code 0. Invalid or incomplete input returns a
nonzero code (normally 2), with no report published. Recover an unfinished run
using the existing audited runtime workflow separately; the evaluator never
fills gaps or removes failed cells to manufacture a complete dataset.

## 4. Output and scoring contract

| File | Contents |
|---|---|
| `evaluation.json` | Validated counts, by-stratum/design and safe/candidate summaries, pair aggregates, provenance, limitations |
| `summary_by_design.csv` | Task results, representation choices, endpoint routes, all five cost components, bytes and latency summaries |
| `paired_effects.csv` | One candidate-minus-safe row per independent workload object |
| `report.md` | Short human-readable summary |
| `evaluation_manifest.json` | SHA-256 of all nine inputs, evaluator source fingerprint, Python version |
| `SHA256SUMS` | Checksums of the five report files |

The frozen plan must exactly match its preregistration, workload content, and
endpoint registry. Every planned cell must have one canonical record with the
exact frozen trial identity, literal `True` completeness flags, and outcome
`completed`. All cells must end in journal state `COMPLETED`; the attempt
ledger must have a final canonical success for each. Successful attempts are
normal ledger entries, not infrastructure failures. Prior failed attempts are
counted separately and are never included in the task/cost averages.
The final runtime summary must agree with the audited counts and with the
measurement manifest's hash: a later measurement/provenance substitution cannot
pass merely because it happens to produce the same numeric costs.

Task scores reproduce the existing frozen accepted-substring matcher. The
evaluator verifies the stored score against the answer and labels; it does not
silently introduce an exact-match, option-letter, or LLM-based grader. Missing
scoring labels produce an unevaluable task, not an automatic success/failure.

For each workload, average each design's complete repetition block first, then
subtract safe from candidate. Aggregate those differences with equal weight
per workload object. A repeated session is not another independent video.
This initial evaluator rejects multiple workload IDs sharing one object rather
than inflate the independent sample count. The overall candidate is the frozen
**stratum-restricted policy**, not a single design evaluated on all strata.

Route summaries use the actual selected representation and its frozen placement
rule, not the design's name. For example, `D_local_frames` may still access a
remote digest. The destination field is `destination_execution_node_id`, **not**
`destination_node_id`. If absent in an older record, the registry destination
is labeled `inferred-from-frozen-registry`, not presented as recorded evidence.
An explicitly conflicting endpoint/source/destination is rejected.

Costs are recomputed from accepted access events, the bound measurement manifest,
and the original cost model. All five components must be available and agree
with the stored ledger. `not_applicable` requires the production contract's
justification; unavailable quantities are not assumed zero. A selected artifact
requires positive download requests, full downloads, and delivered bytes.
Remote inline payload uses `bytes_read`; remote artifact payload uses
`artifact_bytes_sent` without adding `bytes_read` again. Local payload is
reported but contributes zero **cross-node** traffic under the registry contract.

Latency summaries report mean, median and interpolated p95 **per accepted access**,
with observed and missing counts. They distinguish felt, service, fetch,
controlled-delay and artifact-transfer timings where recorded. No missing value
becomes zero. Felt latency is not full Agent runtime. Transfer bytes exclude
protocol overhead; the report is not a bandwidth or wire-level measurement.

## 5. Evidence and publication boundaries

- Output remains descriptive pilot evidence: `confirmatory=false` and
  `eligible_for_scientific_claims=false`. This command issues no AWM certificate
  and no OED Commit decision. A positive observed saving is not a safety proof.
- Real cross-node serving does not turn configured Data Agent service costs,
  injected delays or conversion rates into real economic measurements.
  Declared measurement kinds and cost-rate provenance remain visible.
- The reports reproduce complete canonical observations. Failed-attempt time,
  cost and bytes are not a fully measured reliability-adjusted deployment cost.
  Infrastructure counts are reported separately; do not hide them.
- Hashes detect input/code changes, not authenticity, temporal preregistration,
  or whether a request physically traversed the advertised node. Identical
  inputs and evaluator code on the same Python version yield identical report
  bytes; changing code/version changes the provenance manifest.
- Answers, access-handle values, raw traces and input filesystem paths are not
  copied into reports. Nevertheless IDs and operator-supplied provenance text
  require human privacy review. This is **not** a general secret-redaction or
  dataset-licensing tool. Do not publish raw environment files, databases,
  worker configurations, runtime logs, or source videos by default.

The existing workload definitions are in
`configs/multi_candidate_formal_v2_pilot.json` (sixteen video objects), with
expansion provenance in `configs/multi_candidate_formal_v2_workload_selection.json`.
The current conformance run uses a frozen eight-object subset. Prepared
`sampled_frames` are timestamped **textual frame descriptions**, not raw images
passed to the online Agent; `multimodal_digest` is a prepared textual summary.
Source videos and generated representations are not included in this release.

Before a public empirical release: verify the server report, choose which
workload/label/representation files may be shared, review upstream permissions
and attribution, and publish a reviewed input snapshot with its hashes.
The existing development workload is sufficient for the initial script release;
it must not be relabeled an unseen public test set.

## 6. Existing controlled Oracle, AWM and OED reproduction

The 128/256-session controlled Oracles use a different on-disk contract from
the distributed canonical ledger. Do not feed one format to the other or pool
their service-cost and distributed total-cost results. No format conversion or
new statistical method is added here.

For the sixteen-workload controlled v2 Oracle, keep its frozen config and
canonical directory together. These existing commands remain offline:

```bash
# Set this to an existing canonical-oracle directory in your controlled v2 freeze.
: "${PF_FROZEN_ORACLE:?Set the path to your frozen controlled v2 Oracle}"

python -m pathfinder certify-awm-restricted-policy \
  --certificate-config configs/multi_candidate_formal_v2_awm_v3alpha5_certificate.json \
  --oracle-config configs/multi_candidate_formal_v2_oracle.json \
  --oracle-output-dir "$PF_FROZEN_ORACLE" \
  --output-dir outputs/controlled-v2-certificate-reproduction-01

python -m pathfinder run-oed-certificate-replay \
  --oed-config configs/multi_candidate_formal_v2_oed_v3alpha4.json \
  --certificate-config configs/multi_candidate_formal_v2_awm_v3alpha5_certificate.json \
  --oracle-config configs/multi_candidate_formal_v2_oracle.json \
  --oracle-output-dir "$PF_FROZEN_ORACLE" \
  --output-dir outputs/controlled-v2-certificate-oed-reproduction-01
```

Use the exact configs associated with the desired frozen analysis if they
differ from the current checked-in versions, and always use fresh outputs.
These commands retain their existing post-hoc restrictions and conservative
certificate semantics. The new evaluation package neither retunes thresholds
nor upgrades those historical findings into confirmatory evidence.
