# 24-route interleaved pilot — live submission gate

Status: **OFFLINE_RUNTIME_ASSEMBLED; LIVE_SUBMISSION_NOT_AUTHORIZED**.
This is a plumbing gate on two previously inspected videos, not held-out
quality evidence. Follow `EXPERIMENT_OPERATIONS_RUNBOOK.md` before any
deployment or workflow submission.

## Frozen and checked

| Input | Immutable identity / result |
| --- | --- |
| Source revision | Input DAGs at `754760e`; admission at `f7837d0`. Deployable runtime source must be committed and checked from a new LF-clean Git archive. |
| Public schedule | `artifacts/interleaved-multiq-24route-2561a1f-v1`; 2 videos, 6 questions, 24 unique R/D/DC/I route slots |
| N1 public commitment | `artifacts/interleaved-multiq-n1-public-1c9ad84-v1`; 6 labels committed, no answer values exported |
| Video index / six query embeddings | `artifacts/interleaved-multiq-index-2561a1f-v1/{video-index-v1,query-batch-v1}`; source-bound verifiers passed |
| N3 source and question-specific bundles | `artifacts/interleaved-multiq-n3-1c9ad84-v1`; 12 raw objects and 6 frozen query-bound projections |
| N4 derived package | `artifacts/rsi-exam-formal-runtime-foundation-321f33a-v2-public/n4-package`; both target videos have the required digest/frame artifacts |
| Exact route inputs | `artifacts/interleaved-multiq-route-bindings-74114d7-v1`; 24 routes, 42 Data Agent plan bindings; SHA256SUMS passed |
| Four-arm DAGs | `artifacts/interleaved-multiq-trial-dags-754760e-v1`; 24 trials, 276 stages; each passes the actual route DAG validator; SHA256SUMS passed |
| Runtime admission | `artifacts/interleaved-multiq-runtime-admission-f7837d0-v1`; 24 admitted trials, 276 stages, 6 index plans, 42 Data Agent plans and 6 DC cache episodes; canonical verifier and SHA256SUMS passed |

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

The interleaved N7 service factory assembles offline with these real frozen
inputs and synthetic runtime-only endpoint/credential placeholders:
`READY_NOT_PROBED`, 24 trials, 276 stages and zero source gaps. This makes no
claim about live endpoint or worker readiness and made no network/LLM call.
Focused factory/admission/DAG/policy tests passed (23); this is not a
full-suite claim.

## Mandatory gates still failing

1. The new service-factory branch is assembled only on the workstation.
   It must be committed, built from LF-clean source, deployed with the nine
   new immutable inputs, and reassembled inside the intended N7 container.
2. The N1 scorer still mounts the older 12-label private oracle; the new
   six-label oracle is verified on N1 but not served. Do not submit until a
   separately state-bound scorer and verifier present the new public oracle
   identity without exposing labels or breaking the existing scorer.
3. Read-only Docker mount checks show production N2/N4/N7 use the later
   `bd2da19-v3` packages, while this admission binds `321f33a-v2` N2/N4.
   Therefore the current services cannot satisfy the frozen identity gates.
   Choose verified additive pilot services or regenerate all source-bound
   inputs against v3; do not point the admission at mismatched live servers.
4. New N3 package and N6/N7 runtime source are not deployed. New isolated
   state/cache volumes, health, exact mounts, rollback and image digests must
   be checked before changing the running production containers.
5. Runbook deployment, endpoint, auth, worker and fresh-ID checks remain
   pending. No FlowMesh workflow or answer-generation LLM call is allowed
   while any of them fails.

**No 24-route FlowMesh run has happened.** The completed work proves real
input availability and route DAG shape, not end-to-end execution, answer
quality, performance, or cost. All older frozen evidence is unchanged.
