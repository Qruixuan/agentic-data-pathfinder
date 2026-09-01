# Distributed Conformance and Reproducible Evaluation: Initial Results

- **Status:** completed engineering pilot and cross-machine offline reproduction
- **Evaluation date:** 2026-09-02
- **Collection source revision:** `8642d87a289d4c20e206aab90041aa6496870ffb`
- **Evaluator revision:** `c29499d1652201a089779481bd0c565a4c785283`

## Summary

Pathfinder completed its first two-node distributed conformance slice and then
reproduced the resulting evaluation byte-for-byte on a second machine. The run
contained eight independent video workloads and sixteen canonical sessions:
each workload was evaluated once under the safe origin design and once under
its preregistered stratum-specific candidate.

All sixteen sessions completed with full telemetry and artifact delivery. The
safe and candidate policies each answered 7/8 workloads correctly under the
frozen accepted-substring scorer. The candidate policy reduced mean normalized
total cost from approximately `1.250003` to `0.712506`, a reduction of about
`43.0%`, with no observed paired task-success loss.

These are promising engineering results, but they are not a statistical safety
certificate or a confirmatory PPD result. The sample contains only eight
independent workloads and one repetition per design.

## Experimental setup

- **Origin node:** `luyao2`, serving origin representations.
- **Execution node:** `luyao3`, hosting the execution-local Data Agent and the
  FlowMesh worker/MCP path.
- **Independent unit:** one video workload object.
- **Safe design:** `D_origin_remote` for every workload.
- **Restricted candidate policy:**
  - causal: `D_local_frames`;
  - descriptive: `D_local_frames`;
  - temporal: `D_local_digest`.
- **Workloads:** three causal, two descriptive, and three temporal.
- **Canonical sessions:** 16.
- **Attempts:** 16 canonical attempts, with no infrastructure or recovery
  attempts.
- **Cost basis:** five-component normalized conformance cost:
  service, network, storage, amortized materialization, and transition.

The endpoint registry recorded every access destination explicitly. No route
inference was needed in the evaluation.

## Initial findings

| Stratum | Workloads | Safe successes | Candidate successes | Safe mean cost | Candidate mean cost | Candidate minus safe |
|---|---:|---:|---:|---:|---:|---:|
| Causal | 3 | 2/3 | 2/3 | 1.366669 | 1.600009 | +0.233341 |
| Descriptive | 2 | 2/2 | 2/2 | 0.900004 | 0.150009 | -0.749995 |
| Temporal | 3 | 3/3 | 3/3 | 1.366669 | 0.200002 | -1.166667 |
| **Overall** | **8** | **7/8** | **7/8** | **1.250003** | **0.712506** | **-0.537496** |

Candidate-minus-safe task-success difference was `0` overall and within every
stratum.

The result is heterogeneous rather than uniformly favorable:

- **Descriptive workloads:** local sampled frames preserved observed task
  success and reduced normalized mean cost by about `83.3%`.
- **Temporal workloads:** local digests preserved observed task success and
  reduced normalized mean cost by about `85.4%`.
- **Causal workloads:** the Agent selected `multimodal_digest` in all three
  candidate sessions, so those accesses still followed the remote-origin
  route. The candidate therefore did not realize the intended local-frame
  benefit and had higher mean cost. This is direct engineering evidence for
  the PPD premise that changing a physical design does not deterministically
  change Agent demand.

Across the eight sessions per role, the descriptive summaries also give:

- mean felt access latency of approximately `210.62 ms` for the safe policy
  and `111.40 ms` for the candidate policy, about `47.1%` lower; and
- mean cross-node payload of approximately `2777.75` bytes per safe session
  and `272.75` bytes per candidate session, about `90.2%` lower.

The latency figure is per accepted access, not end-to-end Agent runtime. The
cross-node byte figure excludes protocol overhead.

## Reproducibility result

The frozen input consisted of the preregistration, endpoint registry, workload
manifest, measurement manifest, trial plan, canonical ledger, attempt ledger,
cell journal, and final run summary. The offline evaluator produced:

- `evaluation.json`;
- `evaluation_manifest.json`;
- `summary_by_design.csv`;
- `paired_effects.csv`;
- `report.md`; and
- checksums in `SHA256SUMS`.

The evaluation was first run on `luyao3`. The complete frozen snapshot was
copied to `luyao2`, where the repository was updated to the same evaluator
revision and the evaluation was repeated without starting a worker, Data
Agent, MCP Gateway, FlowMesh service, or LLM request. All output checksums
passed on both machines, and the two `SHA256SUMS` files were byte-identical.

This establishes reproducibility of the current evaluation scripts and frozen
workload evidence across the two machines. It does not independently prove the
historical live data path; that claim rests on the separately recorded live
conformance execution and route telemetry.

## Claim boundary

This result supports the following statements:

1. the two-node execution path can complete the current workload with complete,
   fail-closed records;
2. the current evaluator detects and summarizes the full frozen run without
   private services or credentials;
3. a second machine reproduces the evaluation outputs byte-for-byte; and
4. the restricted candidate policy shows an encouraging task-cost trade-off on
   this small development slice.

It does **not** establish statistical significance, generalization to unseen
videos, realistic monetary savings, a `SAFE_TO_COMMIT` certificate, or a
confirmatory comparison. Service prices and conversion rates remain normalized
accounting values, controlled delays remain part of the latency observations,
and the scorer is the frozen accepted-substring compatibility rule.

## Next step

The immediate benchmark objective requested by the advisor—prove the evaluation
scripts for the current workload—is now satisfied at engineering-pilot level.
The next experimental step is to use the frozen result for planning, then
preregister a fresh, video-disjoint distributed pilot of approximately 30–50
workloads. Its workload selection, scoring rule, physical paths, cost rates,
success/cost margins, and analysis must be frozen before any new outcomes are
read. The present eight-workload result should remain a development and power-
planning input rather than be reused as confirmatory evidence.

Detailed evaluator usage and limitations are documented in
[`WORKLOAD_EVALUATION.md`](WORKLOAD_EVALUATION.md).
