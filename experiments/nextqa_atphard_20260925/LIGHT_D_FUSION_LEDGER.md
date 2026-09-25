# ATP-Hard lightweight-D supplement ledger (2026-09-25)

This is a supplemental matched experiment over the frozen 8-video/40-question
cohort. Do not relabel or overwrite the prior 400-route caption-fusion result.

## Representation decision

The historical D/DC action fused `multimodal_digest` text with a
`sampled_frame_bundle`. Its text was assembled from multiple temporal-window
captions, so it is not a low-build-cost D baseline. A frame-only package was
built as an exploratory control, but **no route used it**. The intended primary
supplement retains text and frames while reducing text materialization to one
question-independent, full-video summary request per video. Eight uniformly
spaced frames from the already frozen 24-frame bundle are sent to the summary
model; N6's derived-fusion profile later selects four frames and the text.
The summary prompt, model, exact source bundle, response digest and provider
usage are bound per video. No public question, option or outcome is an input.

## Verified state

- Source commit: `8ec5e431f5131e1180c88f237a78cc1701e9cb90` on the dedicated
  `codex/lightweight-d-atphard` branch. Clean LF archive:
  `.codex_build/ld-8ec5e43-src.tar`, SHA-256
  `0372baa447ccd844650171b114ac8a6edcd460bd8ca82d819b1f754aa575756d`.
- N5 built a new eight-object **frame-only source** in an isolated no-network
  container. The immutable package remains on N5 at
  `/home/pathfinder/atphard-light-d-build-20260925-v1/output/frozen-v1` and
  a checksum-verified copy is under
  `artifacts/nextqa-atphard-light-d-cloud-20260925-v1/frozen-v1`.
  N4 package SHA-256: `4c5ea3bba568feefb4981070d0bc0a191da5010dbcc41562ea06d9e48468fa14`.
  Its receipt records real per-video N5 decode CPU and wall time; provider
  requests for this phase: zero.
- Primary text+frame public plan:
  `artifacts/nextqa-atphard-light-fusion-8ec5e43-plan-v1`, plan SHA-256
  `1aa8691ee355a6e727f4d44827e05898a609cf9139d8883d9e1ca6ba5bf67649`.
  It covers exactly 40 questions and six D/DC observations per question.
- An explicitly **test-only fake-response** dry run under
  `.codex_build/ld-fake-fusion-20260925-v1` proved the canonical freezer and
  verifier accept 240 trials, 480 N4 plan bindings and zero index plans.
  It is not experiment evidence and must never be deployed or priced.
- Focused tests: 22 passed (plan, bindings, DAG, admission, batch, light frame,
  light fusion). No full test suite was run.
- The earlier `54897f7` pure-frame route image was built on N7 and loaded on
  N8, but **never deployed**. It is not the image for the text+frame run.

## Next actions / fail-closed gates

1. Resolve the Root SSH banner timeout without touching running services. The
   last three attempts failed before authentication. The source and frame
   archive transfers to N6 had succeeded before the outage, but extraction
   and checksum verification did not run.
2. Verify N6's transferred clean archive and N4 source package. The healthy
   N6 service and exact `qwen3.8-27b` model were checked read-only before the
   outage; all three provider config keys were present, with values never
   printed. Recheck health and the provider endpoint before requesting work.
3. Materialize at most one new summary call for each of eight videos, with
   per-object response/usage persistence. If a response fails validation,
   stop and diagnose its retained raw response offline rather than blindly
   retrying. Keep secrets out of artifacts and logs.
4. Freeze the real two-representation N4 package and rerun the canonical
   240-trial/480-binding admission against it. The fake dry run is not a
   substitute. Reverify all `SHA256SUMS` from their own directories.
5. Build a new image from the final source commit, digest-pin and selectively
   deploy only the new N4, N7/N8 cache and N7/N8 route services in a separate
   project/ports, preserving the prior 400-route deployment and all volumes.
   Check both coordinators' actual N6 origin/image/epoch, N4 advertised origin,
   dependency DNS, auth boundaries, fresh cache state and worker preflight.
6. Freeze a new config, run one pre-submit canary, then the 240-route supplement
   with fresh run IDs. Record response usage, route costs and build-time VM
   allocation separately. Do not claim complete cost if a required component
   remains unmeasured.

No FlowMesh workflow or new model request for this supplement had been sent at
the time of this ledger. The old 400-route evidence and containers are intact.
