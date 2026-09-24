# Ten-route multi-question run: pre-submit ledger

Frozen at 2026-09-24 18:45 UTC, before the first FlowMesh workflow.

## Scope and claim

- One outcome-blind, previously unexposed three-video/six-question pilot;
  60 serial route observations (N7/N8 × D0–D7, with D3/D7 miss/hit).
- This is a descriptive path/quality/cost pilot, not a scientific accuracy
  estimate or a final held-out RSI-Exam score. The six questions are not
  presented as a separate development/test split.
- New full-route limit: 60 planned observations; frozen paid-preparation
  protocol capped 27 caption windows, 3 video-index embeddings, 6 query
  embeddings, and at most 180 N6 attempts. No monetary ceiling was requested.
- Output directory reserved for `/home/pathfinder/t60-run-20260925-v1/output/routes-v1`;
  it has not been used. Frozen plan identities are in the 60-route plan.
- The two cache volumes and IDs are fresh, capacity 2,038,602 bytes, health
  reports zero entries/bytes. D3/D7 miss/hit pairs use isolated namespaces;
  these 60 routes alone do not prove cross-question eviction.

## Source and frozen input gates

- Git source revision: `52a4aeabf319bad9014cfab163008c16d0862ea5`.
  The LF-clean Git archive is the N7/N8 image build and runner import root;
  SHA-256 `e707fca04bf263e83d589559533d57fa98ab5cf67bf790cb4346ddf1e47d7c9f`.
  The three route/admission module SHA-256 values matched the image on both
  hosts. No Windows working-tree file is mounted into the runner.
- Public deployment tar SHA-256:
  `ec28a444b65d8eecd7425b867bdb304b5f789ed5ed4e600074e096e7878054c8`.
  All six hosts matched it; all 17 nested `SHA256SUMS` manifests passed from
  their own directories on every host. N1's private oracle manifest passed
  on N1 only; no hidden-label value was displayed or exported.
- Canonical source-bound batch check: `SOURCE_BOUND_INPUTS_VERIFIED`, six
  questions/60 routes; config SHA-256
  `07e485dc807e826c6ca11e2eab0606a3fe5c64cb357c55645925328d3ae605c2`,
  admission SHA-256
  `21a8c73bbba0eb9ae9fce5eaaf7a6046856218f9bf833e3cda25782a463c6bd0`,
  plan SHA-256
  `c7fd85adfbc47bec4ef722655f7bf6e5beb28e6e6fe7f5badac64264f9556530`.
  N1 oracle, N2 index, N3 raw/indexed, and N4 derived packages were each
  canonically verified. Baseline spec and route order were frozen before
  route outcomes.
- Batch timeout: 900 s, the documented shared-runner floor; the prior
  576.03 s shaped-network floor is from a different profile. This pilot
  retains the 900 s bound and stops on the first terminal failure.

## Runtime gates

- New route image on both N7/N8:
  `sha256:3a5e32f633763ecf32c999effb507304064008ab390064b716f380316e1c97ed`.
  The proven SDK-equipped runner image is separately pinned to
  `sha256:9cc1202c88d14665ffdce135421092449d2172f01f4087ed5455bb88eab713b1`;
  it imports the exact clean source archive, not its baked-in Pathfinder code.
- New N1 scorer/verifier reuse image `sha256:42eacecb28a9fc34d05c666ceb05bb92f176ec7cad3e17e241da6fdf01c9ed99`.
  N2 reuses `sha256:56cdfb0f4ce442309d4c63c9b54bf14aea050446812d530c9248578444a3017e`;
  N3 `sha256:619a3ea43f3e1089f0d5d65a6eb884804e56548cf1dd7e16498c13f293179049`;
  N4 `sha256:555ae4d00304c93a7ed9b0e443ad57e7577bc0b9729c51ac8e0415cc1302b549`.
  N6 at port 18886 reuses `sha256:002415af005a0cd0159a71b367f2eca3d5044fa238d73cd1b8a5ed8972bd3f05`.
  N7/N8 local indexes are unchanged. Existing frozen old services remain
  running and intact.
- Nine new services are healthy with restart count zero. Compose render
  checked image, non-root user, read-only root, dropped capabilities,
  no-new-privileges, read-only package binds, inherited credentials, fresh
  volumes and empty target ports before startup. N7 and N8 each passed
  12/12 dependency health from inside the coordinator.
- N3/N4 advertised origins exactly equal the new route client origins.
  From both participating nodes, all seven authenticated boundaries rejected
  invalid bearer tokens with 401 and accepted a valid token before rejecting
  a deliberately invalid body/query with 400. No task, artifact, score or
  LLM request was made by these probes.
- No SSH tunnel is used for submission. FlowMesh preflight resolved one
  current worker for alias `pathfinder_costaware_20260815a` on the expected
  node, observed ID `wkr-2`. The ID is an observation, not a contract.
- N6 health at the actual routed origin `10.70.0.16:18886` reports
  `semantic_llm_configured=true`, `semantic_quality_enabled=true`,
  `semantic_usage_journal_error_count=0` before submission. Its `/state`
  volume is writable and durable; N6 was not recreated.

## Cost and time treatment

- The frozen Alibaba Cloud Singapore rate card uses USD per million tokens:
  qwen3.8-27b input 0.50, cached input 0.10, output 3.00;
  text-embedding-v4 input 0.07. This is list-price cost, not billed cash.
- Provider preparation persisted 27 caption responses, 3 video-index
  embedding responses and 6 query embedding responses. Per-request token
  usage must be extracted from those frozen receipts. N6 per-request usage
  and request IDs will be read from the durable journal after the run.
- Shared VM time will be attributed by observed experiment duration.
  Historical build-period machine time is unknown and will not be invented;
  cold-build API charges and their amortized per-query versions will be
  reported separately. The new run time boundary starts with the first
  workflow submission and ends with its verified summary or terminal stop.
- The real exit status is captured directly from the launcher (no pipeline).
  A failure leaves a durable prefix and stops; no blind retry or altered
  frozen identity is authorized.
