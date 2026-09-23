# 24-route interleaved pilot — live submission gate

Status: **OFFLINE_REAL_INPUTS_VERIFIED; LIVE_SUBMISSION_NOT_AUTHORIZED**.
This is a plumbing gate on two previously inspected videos, not held-out
quality evidence. Follow `EXPERIMENT_OPERATIONS_RUNBOOK.md` before any
deployment or workflow submission.

## Frozen and checked

| Input | Immutable identity / result |
| --- | --- |
| Source revision | `754760e` from an LF-clean Git archive; both new modules match Git blobs |
| Public schedule | `artifacts/interleaved-multiq-24route-2561a1f-v1`; 2 videos, 6 questions, 24 unique R/D/DC/I route slots |
| N1 public commitment | `artifacts/interleaved-multiq-n1-public-1c9ad84-v1`; 6 labels committed, no answer values exported |
| Video index / six query embeddings | `artifacts/interleaved-multiq-index-2561a1f-v1/{video-index-v1,query-batch-v1}`; source-bound verifiers passed |
| N3 source and question-specific bundles | `artifacts/interleaved-multiq-n3-1c9ad84-v1`; 12 raw objects and 6 frozen query-bound projections |
| N4 derived package | `artifacts/rsi-exam-formal-runtime-foundation-321f33a-v2-public/n4-package`; both target videos have the required digest/frame artifacts |
| Exact route inputs | `artifacts/interleaved-multiq-route-bindings-74114d7-v1`; 24 routes, 42 Data Agent plan bindings; SHA256SUMS passed |
| Four-arm DAGs | `artifacts/interleaved-multiq-trial-dags-754760e-v1`; 24 trials, 276 stages; each passes the actual route DAG validator; SHA256SUMS passed |

The N1 private oracle was built and verified on N1 only. Its labels and CSV
were not copied to the workstation. Only the label-free public commitment
was exported. The current production N1 scorer has **not** been switched to
this new oracle.

The local Data Agent manifest resolution test opened the actual artifacts
named by all 42 bindings and independently checked each size and SHA-256:
N3 12 accesses, N4 30 accesses, zero mismatch. All six N3 selected bundles
have distinct task-bound plan IDs and payloads. A plan ID for a *different*
question on the same video is valid for that other question, so the Data
Agent alone cannot reject it; the route admission must enforce the exact
`(trial, question, plan ID, artifact)` binding. The frozen input package does.

Focused tests: 18 simulator multi-question tests, 6 public-schedule tests,
and 4 cache-episode tests passed. These are not a full test-suite claim.

## Mandatory gates still failing

1. The 24 DAGs intentionally have `flowmesh_submission_authorized=false`
   and `required_runtime_adapter_ids=[multiq-runtime-admission-pending]`.
   There is no source-bound *runtime admission* for these six question IDs.
2. The deployed N7/N8 service factory still rejects `indexed-derived` and
   consumes the legacy one-question admission. It has not mounted the new
   question-specific N3 plan catalog or the signed DC cache episode map.
3. The new N3 package, N1 oracle, and affected N6/N7 runtime source have
   not been deployed. New services/state and rollback must be verified
   before replacing any currently healthy production container.
4. The new admission must bind N4 provisioning references, N2 query plans,
   N1 public commitment, exact N3 selections, and all 276 stages. It must
   then pass the canonical verifier and source-digest gates.
5. Runbook deployment, endpoint, auth, worker and fresh-ID checks remain
   pending. No FlowMesh workflow or answer-generation LLM call is allowed
   while any of them fails.

**No 24-route FlowMesh run has happened.** The completed work proves real
input availability and route DAG shape, not end-to-end execution, answer
quality, performance, or cost. All older frozen evidence is unchanged.
