# Formal physical-path PPD pre-submit gate audit

Status: **BLOCKED — no formal FlowMesh submission authorized**.
This is a preparation audit, not an experiment admission or result. The
mandatory checklist in `EXPERIMENT_OPERATIONS_RUNBOOK.md` remains fail-closed.

## Completed without a provider request

- Pathfinder branch `codex/route-action-quote-preview` reached `eb0092a`.
  The upstream FlowMesh repository was not modified. Other pre-existing
  workstation edits remain unstaged and were not imported into the build.
- The Agent SDK numeric-usage and HTTP-attempt overlay passed 28 focused
  local tests. It records attempt time, HTTP status and a SHA-256 of a
  validated provider Request ID, never the request body, credential or raw
  Request ID. Exact provider usage is still conditional on an independent
  audit-log join.
- A clean LF Git archive containing only the four committed overlay files
  was transferred to N7. Archive SHA-256:
  `e2ea73daba0173f8438d816204b897feea5a1d53b5c5fa6636c730cbe152215a`.
  The original FlowMesh worker base image was verified by its executor source
  digest before a `--network=none` derived-image build.
- Derived image on N7: `sha256:26cdc470f736f20bd7fa3028162bcee9549122636ee52cfc2536b2523e27f599`.
  A network-disabled synthetic probe in that image passed both SDK usage and
  HTTP-attempt capture. This image is **built but not deployed**.
- Existing PPD worker `pathfinder_ppd_visual_20260927h` and representation
  Gateway `pathfinder-ppd-gateway-v1` retained their container IDs and were
  running/healthy with restart count zero after the build.

The first image build deliberately failed its pinned-source check because an
already-patched image was supplied as the base; no image was produced from
that attempt. A probe revision then failed because the probe crossed an async
`ContextVar` boundary; its corrected version passed. Both issues are recorded
in the operations runbook. Neither attempted a FlowMesh workflow or model
request.

## Gates still closed

| Gate | Current evidence | Required before formal submission |
| --- | --- | --- |
| Physical action plan and admission | Historical 40-question ten-route plan is `NOT_ADMITTED`; development quote package is `DEVELOPMENT_QUOTE_NOT_SUBMISSION_ADMISSION` | Fresh outcome-blind plan, exact source-bound runtime admission and checksums; verify every action, cache episode and new run identity |
| Agent-facing route Gateway | Implementation and loopback preview exist; live route handoff is not deployed | Admission-gated live bootstrap, independently verified cache artifact catalog/status reader, durable choice and handoff; no preview flag in production |
| Worker | Existing representation Agent worker is healthy; new attempt-capture image is built only | Route-action YAML in a pinned derived image, isolated worker registration, exact config digest, unique current alias and no-submit FlowMesh validation |
| Cost basis | Ten development quotes cover prebuilt route execution; Agent sample has SDK token units but no provider-attempt join | Freeze measured marginal quote protocol plus host-timed build, storage/retention and transition allocation rules; deploy capture before the next admitted run; reconcile Agent provider attempts before monetary claims |
| Runtime endpoints | Existing N7/N8 route-to-N6 origin was checked read-only | Recheck all participating new image IDs, mounts, epochs, N3/N4 advertised origins, N6 identity, DNS, auth boundaries, cache state and dependency health after the formal deployment |
| Research design | Prior public sample proves an engineering representation Agent/N1 score path only | Freeze development/holdout split and baseline policy before outcomes; record claim class and counterbalanced design/cache schedule |

The previously supplied Model Studio workbook predates the successful
15:59 UTC Agent sample. It cannot reconcile that sample's requests. A fresh
request-audit export for that interval can validate the *old* sample; a new
route-action run will require its own matching log. The old sample is not a
D0–D7 Agent route choice and cannot itself satisfy this admission.

No formal workflow, LLM request, N1 score or new cache episode was started
by this audit. No running container or volume was removed or recreated.
