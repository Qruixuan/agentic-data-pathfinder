# Route-action prelaunch audit — 2026-09-27

Status: **BLOCKED_FULL_EPISODE_COST_AND_DEPLOYMENT**. This is an
engineering audit, not a frozen experiment receipt or a claim of runtime
readiness. No Agent workflow, N6 inference or N1 scoring was submitted.

## Local work completed

- Read `EXPERIMENT_OPERATIONS_RUNBOOK.md` completely and applied the
  pre-submit gates as fail-closed.
- Added an authenticated, content-checking cache status endpoint and client.
  It returns only metadata, does not transfer the cached artifact, and does
  not mutate hit/miss events or LRU state. Runtime lookup still performs its
  independent payload and durable-lineage verification.
- Added a reader that probes every catalog-bound representation needed by a
  cache action. A partial cache is a miss; status changes invalidate an Agent's
  offer set before commitment.
- Removed historical run and per-question cache episode identities from new
  route choices. An explicit fresh execution namespace now determines new run
  IDs and per-video/per-node cache episodes. This permits multi-question reuse
  without cross-video or cross-experiment collisions.
- Added a frozen quote-package loader that rejects incomplete accounting,
  held-out/outcome-sourced pricing, stale source bindings, missing actions and
  checksum failures. The initial audit had no package to load; the bounded
  development-only package added later is described below.
- At the initial audit, focused tests: 91 passed, 0 failed across the new
  route/cache/quote tests and directly affected existing route-runtime/MCP
  modules. `py_compile` and `git diff --check` passed. No full-suite or paid
  test was run.
- The canonical loader re-verified the historical public plan as
  `VERIFIED_TEN_ROUTE_MULTIQ_PLAN_NOT_ADMITTED` (40 questions). The explicit
  `NOT_ADMITTED` suffix is respected: it is a regression fixture, not a live
  submission authorization.

## Read-only cloud observations

- UpCloud API `GET /1.3/server` returned **started** for ROOT and N1–N8,
  all in `sg-sin1`. This proves VM state only, not Docker/FlowMesh health.
- API `GET /1.3/server/{uuid}` confirmed the Root public address still equals
  the recorded address. A strict-host-key, non-interactive SSH attempt
  established TCP and sent the local SSH version string, but received no
  remote banner before timeout. No remote command was executed. Root's
  UpCloud firewall is on, with an inbound TCP/22 accept rule covering all
  IPv4 sources; the cloud firewall rule therefore does not explain the
  observed timeout. Host sshd or an intermediate network path remains to be
  diagnosed through a trusted out-of-band console or another known-good
  client. No firewall rule was changed.
- API `GET /1.3/price?zone=sg-sin1` returned USD price-list values for the
  four deployed DEV plan types. The list is current as of this read-only
  observation, but it is **not** a complete route quote or a frozen rate-card
  artifact. UpCloud documents hourly billing and the price endpoint at
  https://developers.upcloud.com/api/1.3/price .

## Gates that remain closed

1. **Complete episode cost:** the historical 400-route accounting says
   `full_path_cost_complete=false`, includes unpriced N6 retries and omits
   historical build-machine time. The later t60 development quote covers
   prebuilt route execution only, not video-level build and occupancy,
   design transitions or the Agent selection call. It cannot be renamed as a
   complete live price or used to admit submission.
2. **Artifact catalog binding:** the live status reader must receive exact
   content digests from a canonically verified public artifact catalog;
   a caller-provided mapping alone is not proof.
3. **Source and deployment:** the working tree is dirty (HEAD at
   `b04a568722a4a8850a7f9cfda5e5b44f585547d4`, many pre-existing user
   changes). No LF-clean source archive, image build, source-bound admission
   refreeze, N7 Gateway wiring, cache-service deploy or container-health
   preflight has been performed for this change.
4. **Live runtime readiness:** Root SSH is recovered (see addendum), but the
   intended N7/N8 deployment, N3/N4 origins, N6 readiness, mounts and
   authentication boundaries still need their no-inference preflight.
5. **Fresh formal binding:** the historical plan may be used as a test
   fixture, but a new live source-bound plan/admission and verified worker
   pinning are required before dispatch. A fresh run ID alone does not turn
   old route evidence into a new experiment.

At the initial audit, the existing encrypted UpCloud token was loaded only in
memory for authenticated read-only API calls; its value was not displayed,
hashed or persisted. No hidden label, prompt, answer or model request body
was read or recorded. No container, VM or volume was modified at that stage.
No FlowMesh or LLM API was called. Existing frozen artifacts are unchanged.

## Authorized Root recovery addendum — 2026-09-27

The operator explicitly authorized restarting Root after the SSH banner
timeout. The UpCloud API request targeted only the verified Root UUID
`00201264-edbc-42e7-967f-af27ce00c049`: a soft restart with 180 seconds
allowed and `timeout_action=ignore` (no forced power-off). Its state passed
through `maintenance` and returned to `started` at 07:37:31 UTC. SSH then
authenticated non-interactively with the existing strict host-key check;
`hostname` returned `pathfinder-root` and `uptime` showed a fresh boot.

Read-only checks found the Root Node Server and both Redis containers healthy,
and `http://127.0.0.1:8000/healthz` returned `{"ok":true}`. The registry
shows a current Root node (`nde-12`) and N7 node (`nde-11`), and two current,
non-stale N7 workers: `pathfinder_costaware_20260815a` (`wkr-5`) and
`pathfinder_ppd_visual_20260926e` (`wkr-8`). The older Root registration
`nde-7` remains in the listing with a pre-restart `last_seen`; it is not the
current Root node. IDs are observations, not pins. No workflow was submitted,
no LLM or scorer called, and no N1–N8 VM or container was modified. This
recovery clears the Root-SSH blocker only; it does not satisfy the quote,
source-binding, deployment or full pre-submit gates above.

The Root jump path also reaches N7, N8 and N6. Their existing containers
listed as healthy in read-only Docker checks (non-interactive sudo was needed
on N8/N6). This is a host/container-health observation, not verification of
the proposed new route-action image, mounted packages, full dependency graph,
credential boundaries or worker dispatch.

## Development quote addendum — 2026-09-27

The old 400-route accounting remains **incomplete** and has not been used as
a live quote. A narrow offline adapter projected only cost fields from the
already sealed t60 engineering pilot: 60 matched N6 results, zero unpriced
provider retries, ten route/cache states, six observations per state. It
rejects unknown fields in the cost-only projection, including outcomes, and
recomputes the source N6 list prices from measured token units. The new
loader independently recomputes each quoted amount and latency from the
sealed projection and rate card; a JSON `complete` flag alone cannot make an
incorrect amount pass.

The draft rate card at `route-rate-card-20260927.draft.json` reflects the
current UpCloud nine-VM plan roster, including N7's expanded
`DEV-2xCPU-8GB`; its observed source timestamp is 08:17:26 UTC. The
current prebuilt-route quote package is
`artifacts/route-action-quotes-t60-dev-20260927-v2-draft/`, with three payload
checksums independently OK and ten calculated offers. It allocates the
measured batch-control overhead to routes in proportion to their elapsed
time. Its basis digest is
`8e7425ec5787ed9bd56cff3d38bb654b811975ac573e4ec8d8f99430a7c2cb99`.
The v1 draft remains intact as an earlier attempt that omitted this overhead.
It explicitly records `full_episode_cost_complete=false` and the bound
40-question plan remains `VERIFIED_TEN_ROUTE_MULTIQ_PLAN_NOT_ADMITTED`.
At the development-quote audit, the code was in a dirty workstation tree.
The package has not been deployed in an LF-clean image; it is a development
artifact, **not** submission admission.

An opt-in loopback-only preview bootstrap subsequently bound this exact
quote package to the public plan and exercised list/commit with no model or
FlowMesh call. It does not add a verified live admission or enable cache
actions without a separately verified reader.

Still required for a full PPD cost objective: measured or explicitly scoped
video-level build/occupancy, forward/restore transition and Agent selection
call costs, plus a fresh admitted plan and deployment gates. No new FlowMesh
workflow, N6 inference, N1 score, or source-bound experiment was run to
produce these quotes.

## Local preview integration addendum — 2026-09-27

The bounded route-action preview and its required Gateway dependencies were
committed on `codex/route-action-quote-preview` as `15f3538`. Unrelated dirty
worktree changes were left unstaged. The opt-in CLI bootstrap enforces a
loopback listener and `preview_only=true`; its no-model regression exercised
offer listing and one durable choice commit against the development quote and
historical public plan. This does not perform the admitted handoff or submit a
workflow. The preview requires independently pinned plan, source-trace, and
rate-card digests and refuses to write state into frozen packages.

Relevant working-tree tests passed (62 across the focused route/cache/Gateway
and visual-artifact suites). A clean LF Git archive of `15f3538` passed its
available package-contained tests (11 run, 2 fixture-dependent skips), and
all three checksum-bound quote payloads verified again inside that archive.
The skipped tests require the separate historical public-plan fixture; they
passed in the working tree where that frozen fixture is available. The branch
has not been deployed or used to produce a new source-bound admission.
