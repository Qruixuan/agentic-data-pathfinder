# Route-action cost completion ledger — 2026-09-27

Status: `BLOCKED_COMPLETE_EPISODE_EVIDENCE_NO_SUBMISSION`. This is a
development-cost preparation record, not a source-bound admission or a live
experiment receipt. No workflow or provider request has been submitted.

## Immutable and source inputs

- Local route-action preview branch: `codex/route-action-quote-preview`,
  pushed commit `6b2af2559c26b6cbacfe4cf6dcccc7b2a45ee1c5`.
- Development quote package:
  `artifacts/route-action-quotes-t60-dev-20260927-v2-draft/` (three payload
  checksums verified; 10 route/cache-state quotes).
- Quote scope: `prebuilt-route-execution-list-price-allocation`;
  `full_episode_cost_complete=false`.
- Historical public plan is `VERIFIED_TEN_ROUTE_MULTIQ_PLAN_NOT_ADMITTED`.
  Its trial/run identities are not available for a new submission.
- Clean LF archive of the code commit preceding the audit-only addendum was
  verified in the prior local preview step. A fresh archive of the final
  deployment commit is still required before any source-bound work.

## Cost coverage

| Component | Present evidence | Next requirement |
| --- | --- | --- |
| N6 route inference | t60 exact result/request joins, 60 completed attempts | Preserve one-to-one usage and attempt joins in a new run |
| Caption/index/query API | t60 provider-token build receipts | Bind new build requests to each object/question; share captions once |
| Route VM allocation | t60 elapsed time and dated nine-VM development rate card | New run's exact active interval and stage attribution |
| Build VM time | Historical journal identifies `local-windows`, not UpCloud | Time each new UpCloud build on its actual host; do not backfill old cost |
| Agent model call | Exact prior public task had `num_requests=1` but no input/cache/output token units in either FlowMesh API or dedicated worker result | Future admitted Agent run must return numeric usage and be reconciled to provider attempts |
| Storage | Nine attached standard disks, all API `part_of_plan=yes` | Record artifact bytes and retention; verify any new disk's billing status |
| Design transition | No forward/restore event | Record actual start/end, host, bytes and artifact identity |

Read-only UpCloud API recheck: ROOT and N1–N8 all `started` in `sg-sin1`.
The exact four deployed plan prices matched the 2026-09-27 development rate
card; the nine-plan sum is USD 0.166662/hour under its declared list-price
conversion. This is a fleet allocation rate, not an invoice or an additional
per-route charge. Each VM has one plan-included standard disk (20/30/40 GB).

## Gate ledger

| Gate | Result |
| --- | --- |
| Existing quote payload checksums | PASS |
| Existing quote recomputation / no-model preview | PASS (development only) |
| Current VM roster and price API | PASS (read-only) |
| Current attached-storage inclusion | PASS (read-only) |
| Root strict SSH hostname probe | PASS on second bounded attempt; first attempt timed out at banner, cause unknown |
| Dedicated worker exact-result numeric usage | INCOMPLETE: one request, no token units; read-only inspection |
| Route-action Agent usage configuration | PREPARED: `include_usage: true` in its Chat Completions model settings; backend not yet tested |
| New host-timed build and Agent usage capture | NOT RUN |
| Complete episode accounting | NOT RUN |
| Fresh source-bound plan and admission | NOT RUN |
| New Gateway/cache deployment and health/auth preflight | NOT RUN |
| FlowMesh worker preflight and public smoke | NOT RUN |

Next safe action is to collect new, host-timed build and numeric Agent usage
events under a fresh development identity, then reconcile every provider
attempt. Only after that may a complete episode-cost package and fresh
source-bound admission be frozen. Do not reuse the old plan as live admission
or submit a workflow to diagnose an earlier gate.

The numeric-only diagnostic read the existing public task
`tsk-a8ba83fa-8e12-45d3-8937-41a3df363f05` from both Root's task result
API and the dedicated worker's exact `results.json` path. Both returned only
`num_requests=1`; no prompt, answer, request body, credential or raw result
was displayed. The deployed worker has `openai-agents` 0.7.0, whose
`ModelSettings` includes `include_usage`. The new route-action Agent YAML
sets this flag, but the current worker image is unchanged. Whether Qwen
emits the final streamed usage chunk still requires a fresh admitted run;
until then Agent model cost is unknown, not zero. No FlowMesh or provider
request was made for this diagnosis.
