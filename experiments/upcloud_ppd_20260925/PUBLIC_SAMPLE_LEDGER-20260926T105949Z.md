# Public PPD Agent closure retry: pre-submit ledger

Claim class: engineering conformance only; no quality, performance or cost
claim. The one public case is previously exposed and is not a holdout.

- Fresh session/trial: `ppd-qwen-first-20260926t105949z-ff725c9e`.
  The no-submit preflight confirmed that its output directory and Gateway
  session did not exist.
- Frozen object/question/design are unchanged from the prior public sample:
  `nextqa-val-11584566583`, `engineering-q5.txt`, `PPD_REMOTE_DIGEST`.
  Question SHA-256 is
  `570b67ec01c1a78c65952657de2f95db58b27dd07595211529e4b035513d7841`.
  No answer or hidden label is part of this check.
- Workstation HEAD is `d1e431a952ccbd477ce00a693b827b262e013c42`.
  The working tree is dirty and is not used as the runtime source. The
  existing Gateway image
  `sha256:e41ad955c9f97a89ddfc358367b08054c3e2dd20541847fab2530067993fed69`
  is the direct parent of the traced derivative
  `sha256:7e86efd423bc332b6908f6041f39aa5a834fc34f192a24b6abd88ed787f1ae52`.
  The only overlaid source file is `mcp_server.py` at LF SHA-256
  `06b81ce42526a531bdacf18128510387bd3c310d229a23dfbc975708cfee3a56`.
  Three focused no-network trace tests pass. Trace defaults off and emits
  only tool name, session SHA-256, boundary status and exception class.
- The original Gateway container ID `bf332869e95611cff5913705577d6fec6caadfc08347e49b706313d3697ab101`
  is stopped and preserved under
  `pathfinder-ppd-gateway-v1-before-tool-trace-20260926`. The new Gateway
  ID `b07c717118401015a0239652c7263dc747d4e67c74b3d1aa336caa2a30812ca6`
  is healthy on the exact diagnostic image. Its frozen package read-only
  mount, state volume, loopback host port 18765, bridge networking, private
  aliases, non-root user and security settings were preflighted against the
  old service. No active session existed at cutover; the state volume and
  all other services are preserved. Rollback requires stopping/renaming the
  new Gateway and restoring the old name, then starting the old container.
- The dedicated worker image remains
  `sha256:f8f977fe69cea83a950c60f50b40c1787f332e3084d81f142c181336c9e299eb`.
  No FlowMesh repository or worker deployment was changed. Alias
  `pathfinder_ppd_visual_20260926e` resolved uniquely to observed `wkr-8`
  in `pathfinder/upcloud-sg-sin1/pathfinder-n7`.
- The hash-verified new supervisor is staged as
  `/home/pathfinder/ppd-qwen-first-20260926-v1/run_qwen_trace_probe.py`
  with SHA-256
  `b84207b6a3f1c6f4cfb344d72e5ee20426ad97923623922fc39e49e09cdd64a1`.
  It pins the new Gateway image explicitly and reuses the unchanged inner
  one-case runner SHA-256
  `78e9a97fc4ccee804cffba02af1da12084b43faeb56150dbf36c3f9fb4dfd5c4`.
- Its no-submit preflight exited 0 with `PREFLIGHT_OK`, validated the frozen
  top-level/N3/N4 checksums, loaded the system config and endpoint registry,
  checked N3/N4/N7-replica/N6 identity and health, probed both valid-token
  validation and invalid-token rejection at each Data Agent endpoint, checked
  Gateway TCP reachability, resolved the unique current worker and validated
  the FlowMesh workflow. It recorded `workflow_submitted=false` and
  `llm_called=false`.
- Budget: exactly one new FlowMesh Agent workflow, with the current
  `max_turns=10` and task timeout 600 seconds; no blind retry. No cache
  episode or N7/N8 comparison is involved. Success requires at least one
  accepted Gateway representation access and a single-option final answer;
  a FlowMesh `DONE` status alone is insufficient. Native process exit status
  and sanitized receipt must be retained. Model billing is not claimed.

At the pre-submit checkpoint, the next operation was the one authorized
execute call. This ledger contains no credential value, hidden label,
prompt, answer, signed URL or environment dump.

## Post-submit result

- Exactly one workflow was submitted: `wfl-b14dfaf3-d964-4f2b-92ee-9fb7b4e320be`,
  task `tsk-ad3ed289-f36e-4e80-ac61-a38ddc635245`. It ended FAILED, with
  sanitized receipt at
  `/home/pathfinder/ppd-runs/ppd-qwen-first-20260926t105949z-ff725c9e/receipt.json`.
  Its SHA-256 is
  `9069851d3eda353ba292617f4e967b12de8a51b75623d97a4f88ed1e97ad06a4`.
  The outer SSH command exited 1 because the supervisor raised after the
  inner runner's failure; this is not a successful sample.
- The worker's first execution error was again exactly `Max turns (10)
  exceeded`. Gateway trace bound by the session SHA-256 recorded one
  successful `list_offers`, eight `access_representation` calls that each
  raised `DataAgentHTTPError`, and one successful `get_session_state` after
  the third failed access. No artifact-fetch or visual-inspection tool ran.
  The Gateway wrote zero access events, so there was no accepted access or
  Agent closure.
- The failure is now localized to the Data Agent HTTP access boundary; it
  does not support a Qwen-vendor switch. The trace intentionally did not
  record representation arguments or HTTP status codes, and the available
  N4 container logger had no access lines during the window. Those details
  are unknown rather than guessed. No second workflow, model call or blind
  retry was submitted. The diagnostic Gateway remains healthy and its old
  container, image and state volume remain available for rollback.
- Post-run read-only inspection found the Gateway and dedicated worker both
  `running:healthy` with restart count zero. The 3 new trace tests and 14
  existing visual bridge tests pass; `git diff --check` exits zero. No full
  suite, ten-route matrix or hidden scoring was run.

## Offline root-cause diagnosis (no new submission)

- The frozen `system.json` and `endpoint-registry.json` agree with each other,
  but neither is location-compatible with the frozen N3/N4 Data Agent
  manifests. Across all six design/representation placements, Gateway sends
  one of `n3-remote-origin`, `n4-remote-origin`, `n7-local-replica`; the
  receiving manifest expects `origin-cold` or `origin-warm`. All six equality
  checks fail. The N7 replica mounts the N4 package and inherits its
  `origin-warm` binding.
- Offline `DataAgentManifest.resolve` calls reproduced
  `DataAgentBindingMismatchError` for raw video, frame bundle, remote digest
  and local-replica digest. The server maps this class to HTTP 409
  `binding_mismatch`. The historical HTTP status was not logged, so 409 is
  the deterministic source-code result for these bindings, not a recovered
  response code.
- The preflight's valid-token request was `{}` with no protocol-version
  header; it correctly tested authentication, but returned 400 before a
  plan/location lookup. Health and package checks also do not compare the
  Gateway path locations with Data Agent manifest locations.
- No source or deployed service was changed, no FlowMesh call or LLM request
  was made, and no frozen package was edited during this diagnosis. The next
  correction must create new immutable bindings and verify all six pairs
  before any new paid run.

## Binding repair and no-LLM deployment verification

- The freezer now uses the Data Agent logical binding locations
  `origin-cold`/`origin-warm` in `system.json`, while the endpoint registry
  retains the distinct physical placements. It validates all six routed
  design/representation requests against the N3/N4 manifests before sealing.
  Four focused tests pass, including a regression that rejects the old
  mismatch.
- New immutable package:
  `/home/pathfinder/upcloud-ppd-engineering-20260926-v4` on N7. Archive
  SHA-256 `8156635d147f2d2cccb4ec19c7cda226a7ff71625c08cb98306806950cc65618`.
  The root, N3 and N4 checksum manifests verify (3, 4 and 5 entries). N3/N4
  package manifests and endpoint registry are byte-identical to v3; the
  top-level system configuration and freeze receipt changed. The N3, N4 and N7-replica live
  manifest digests match the corresponding v4 files.
- The isolated Gateway moved from container ID
  `b07c717118401015a0239652c7263dc747d4e67c74b3d1aa336caa2a30812ca6`
  to `407d9e4f6f2edcb466e922c15f32585db558eb6e4b1c8421e0029b1980afcb39`,
  with the same pinned image, state volume, credential file, port and private
  aliases. The old container remains stopped and preserved as
  `pathfinder-ppd-gateway-v1-before-binding-fix-20260926`. The new Gateway
  reached healthy; no other service was recreated.
- One bounded public access per endpoint returned `ACCESS_OK` and a content
  identity: N3 raw video, N4 remote frame bundle, N7 local digest. These
  created three Data Agent operation records but did not download artifacts.
  The first probe attempt failed locally before access because `python -P`
  lacked `PYTHONPATH=/app`; a network-disabled replay proved the import-root
  cause before the corrected live check.
- The updated supervisor at
  `/home/pathfinder/ppd-qwen-first-20260926-v1/run_qwen_binding_v4.py`
  requires the v4 Gateway mount. Its no-submit preflight returned
  `PREFLIGHT_OK`, `workflow_validated=true`, and the unique alias resolved to
  observed worker `wkr-8`. No FlowMesh workflow was submitted and no model
  request was made during the repair. Agent closure itself has not yet been
  rerun or claimed.
