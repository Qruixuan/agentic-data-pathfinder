# Pathfinder Experiment Operations Runbook

Last reviewed: **2026-09-24**, against repository revision **28b5ede**.
This is a source/contract review, not a live cluster readiness certificate.
Ports, worker IDs, image IDs, prices and successful runs mentioned as historical
examples are not defaults for the next experiment. Re-observe live state before
submission; do not assume uncommitted workstation code is deployed.

This runbook records recurring failures seen while freezing, deploying, and
running Pathfinder experiments on Windows, `luyao3`, and the UpCloud
multi-host environment. It is intended to prevent repeated failed submissions,
container rebuilds, and LLM calls.

Use it for every smoke, canary, one-case run, and formal matrix run. A run is
not ready for submission until every applicable pre-submit gate below passes.

## Current scope and sources of truth

| Concern | Current source of truth |
| --- | --- |
| Supported batch contracts and command flags | `experiments.batch --help`, `experiments/README.md`, `experiments/interleaved_batch.py`, `experiments/ten_route_batch.py` |
| Interleaved plan, source binding and cache schedule | `pathfinder/rsi_exam/interleaved_multiq_plan.py`, `pathfinder/simulator/interleaved_multiq_runtime_admission.py` and their canonical verifiers |
| Credential precedence | `pathfinder/cli_commands/_common.py` and the deployed service's declared credential contract |
| N6 usage and provider-ID capture | `pathfinder/simulator/container_node.py`; cost exports must match the deployed journal schema |
| SDK dependency | `pyproject.toml` (`flowmesh-sdk==0.1.9` at this review); record separately observed Root/Node/worker versions |
| Actual endpoints, mounts, images and state | The selected deployment's desired-state record and sanitized live inspection, not an old report or this document's examples |

Use one of two supported batch families: interleaved **R/D/DC/I** or the
existing **ten-case D0--D7** smoke. The former currently admits the approved
N7 pilot origins; it is not an arbitrary-host scheduler. The latter can use
local or verified multi-host bindings. Multi-question x ten-case scheduling
is still a separate extension, not enabled by changing a route count.

The UpCloud single-worker route-coordinator profile does not imply a worker
on every data VM: the pinned worker dispatches route API tasks, N7/N8 execute
their bound paths, and N6 performs inference. A healthy N8 coordinator alone
does not establish an N8 FlowMesh worker. Check the chosen profile rather
than inferring topology from node numbers.

## Operating rule

Treat the experiment as six separate phases:

1. bind immutable source and inputs;
2. freeze and verify all derived artifacts;
3. stage and deploy only the affected services;
4. prove network, service, worker, and credential readiness without inference;
5. submit the authorized fresh batch in its frozen order and within its budget;
6. verify and preserve its evidence before interpreting the result.

Do not use a later phase to diagnose an earlier one. In particular, do not
submit a workflow to test source binding, container startup, SSH tunnelling,
worker registration, or endpoint authentication.

A failed gate blocks **submission**, not further safe diagnosis or an
already-authorized repair. Record the incident, fix the proven cause, rerun
only affected gates and continue within scope. Do not stop merely at a phase
boundary. Stop for exhausted budget, missing authority, an unresolved gate,
or a change to frozen scientific choices that needs the operator's decision.

## Mandatory pre-submit checklist

Record every value in a small run ledger before submitting:

- [ ] Exact source revision is recorded for the runner, freezers/verifiers,
      and each affected runtime image. Every source-bound module matches its
      artifact contract; any intentionally retained older service image is
      recorded and verified, not silently normalized by rebuilding everything.
- [ ] A clean Git archive, not a mutable Windows working tree, is the Python
      import root for all source-bound commands.
- [ ] Source commitments match the clean archive under the artifact format's
      canonical digest scheme; raw-byte and normalized-source hashes are not
      silently substituted for one another.
- [ ] Every required frozen directory exists and its canonical verifier passes
      with flags checked against that revision's `--help` (argparse exit 2 is
      an invocation error, not a source-binding verdict).
- [ ] Every `SHA256SUMS` file passes from inside its own directory.
- [ ] Planning succeeded before plan verification is attempted.
- [ ] The frozen timeout is greater than every operation lower bound plus a
      documented allowance for storage, compute, queueing, and control-plane
      overhead.
- [ ] Endpoints fit the chosen local or multi-host profile. Cross-host edges
      contain no stale loopback/Docker-only names; allowed service aliases
      resolve to the intended private hosts, and allowlists match their names.
- [ ] Only services affected by the change were rebuilt or recreated.
- [ ] Participating containers are healthy, not restart-looping, and match
      desired state. Record runtime epochs and restart counts before/after;
      investigate new unexplained restarts. Do not recreate a healthy reused
      service solely to reset a historical restart count to zero.
- [ ] Runtime source/admission/catalog/gate mounts are the intended immutable
      versions and are read-only where required.
- [ ] N3/N4 advertised origins exactly match the origins used by N7/N8.
- [ ] Dependency health succeeds from every participating coordinator (both
      N7 and N8 for ten-case runs; N7 for the current interleaved pilot).
- [ ] The valid-credential boundary probe reaches the known validation error;
      the negative-credential control is rejected. A generic HTTP 400 without
      that endpoint's auth-before-validation contract is not a pass.
- [ ] If operator access uses an SSH tunnel, its listener belongs to the
      intended tunnel and reaches the intended Root, before any FlowMesh call.
- [ ] FlowMesh preflight resolves exactly one current worker for the pinned
      alias. The worker ID is recorded as an observation, not a durable pin.
- [ ] The run ID, smoke ID, output directory, and operation identities have
      never been used before.
- [ ] The run budget, trial order, cache episode/namespace and cold/warm
      initial state are frozen; no other batch can write into that episode.
- [ ] Public development/test splits and baseline policy are frozen before
      held-out outcomes. Previously inspected videos are not called unseen.
- [ ] The command's real exit status will be captured without a pipeline
      masking it.
- [ ] The expected claim class is written down: infrastructure conformance,
      semantic correctness, performance, or cost. Passing one class must not be
      reported as passing another.
- [ ] For a cost run, N6 has durable writable state and deployed usage/trace
      capture; `semantic_usage_journal_error_count` has a recorded baseline;
      the official model price snapshot (model, region, currency, effective
      date, input/cache/output rates) and experiment time boundaries are
      frozen before submission. The ledger specifies which cold-build and
      shared-VM costs will be measured, amortized, or left unknown.

If any applicable item fails, stop before submission; record why an item is
not applicable rather than silently skipping it. `check` and `preflight`
do not automate this entire checklist.

## 1. Immutable source and cross-platform byte identity

### Recurring failure

Source-bound artifacts have repeatedly been frozen from a Windows working
tree containing CRLF bytes and then deployed into Linux containers containing
LF bytes. The Python source is logically identical, but its SHA-256 is not.
The result is a source-binding or adapter-inventory failure during container
startup or runtime verification.

A related failure occurs when `PYTHONPATH` points at a clean extracted tree,
but Python is launched from the repository working directory. The working
directory is still first on the import path unless isolated, so Python imports
the CRLF working-tree module instead of the intended clean copy.

### Required practice

Use the exact Git object bytes as the canonical source. On Windows, create one
new immutable extraction directory per commit:

```powershell
$Commit = (git rev-parse HEAD).Trim()
$ShortCommit = $Commit.Substring(0, 12)
$StageRoot = "D:\pf-src" # Choose an authorized short, local staging root.
$BuildRoot = Join-Path $StageRoot $ShortCommit
$Archive = "${BuildRoot}.tar"

if ((Test-Path -LiteralPath $BuildRoot) -or
    (Test-Path -LiteralPath $Archive)) {
    throw "Clean source/archive already exists; verify it or choose a new path"
}
New-Item -ItemType Directory -Force -Path $StageRoot | Out-Null

git -c core.autocrlf=false archive --format=tar --output=$Archive $Commit
if ($LASTEXITCODE -ne 0) { throw "git archive failed" }

New-Item -ItemType Directory -Path $BuildRoot | Out-Null
tar -xf $Archive -C $BuildRoot
if ($LASTEXITCODE -ne 0) { throw "archive extraction failed" }
```

Capture the interpreter path before changing directory, then run source-bound
commands with both the clean source as `PYTHONPATH` and Python's `-P` safe-path
option:

```powershell
$Python = (Resolve-Path ".venv\Scripts\python.exe").Path # Or the verified venv.
$PreviousLocation = Get-Location
$PreviousPythonPath = $env:PYTHONPATH

try {
    Set-Location -LiteralPath $BuildRoot
    $env:PYTHONPATH = $BuildRoot
    & $Python -P -m pathfinder --help *> $null
    if ($LASTEXITCODE -ne 0) {
        throw "Pathfinder did not import from the clean source"
    }
} finally {
    $env:PYTHONPATH = $PreviousPythonPath
    Set-Location -LiteralPath $PreviousLocation
}
```

On Linux use the same rule:

```bash
(
  set -euo pipefail
  PF_SOURCE_COMMIT="$(git rev-parse HEAD)"
  PF_CLEAN_SOURCE="$(mktemp -d)"
  git -c core.autocrlf=false archive "$PF_SOURCE_COMMIT" |
    tar -x -C "$PF_CLEAN_SOURCE"
  cd "$PF_CLEAN_SOURCE"
  PYTHONPATH="$PF_CLEAN_SOURCE" python -P -m pathfinder --help >/dev/null
  printf 'Clean source: %s\n' "$PF_CLEAN_SOURCE"
)
```

Retain the extraction while an artifact or rollback procedure depends on it.
Only remove an exact, validated staging path after its required contents have
been preserved; never recursively clean a workspace or staging parent. Never
overwrite an extraction and continue using it under the same name.

Inspect line-ending drift before freezing:

```powershell
git ls-files --eol -- `
  pathfinder/simulator/*.py `
  pathfinder/cli_commands/*.py
```

Do not rely on `core.autocrlf` settings being identical across machines: set it
explicitly for the archive operation. Current local-semantic and interleaved
admission code normalizes CRLF to LF when hashing some implementation sources;
older revisions and other artifact formats may bind raw bytes. That fix does
not normalize all payloads or make an arbitrary working tree canonical.

### Source digest gate

Before deployment, compare every recorded source SHA-256 with the corresponding
file in the clean extraction. A generic helper for a known field is:

```powershell
$InventoryPath = "ABSOLUTE_PATH_TO_ADAPTER_INVENTORY.json"
$ModulePath = Join-Path $BuildRoot `
  "pathfinder\simulator\full_flow_semantic_input_profiles.py"

$Inventory = Get-Content -Raw -LiteralPath $InventoryPath |
    ConvertFrom-Json
$Expected = $Inventory.semantic_input_profile_source_sha256
$Actual = (Get-FileHash -Algorithm SHA256 -LiteralPath $ModulePath).Hash.ToLower()

if ($Expected -ne $Actual) {
    throw "Source binding mismatch: frozen=$Expected clean-source=$Actual"
}
```

Adjust the field, module and digest semantics to the artifact being checked.
The raw-file comparison above applies to a verified LF extraction. A canonical
verifier using normalized source bytes or a canonical-JSON commitment is
authoritative for that scheme. Do not substitute a Git blob ID or raw manifest
file SHA-256 for a semantic commitment; these are different digest schemes.

## 2. Freeze, verify, and change-impact discipline

### Never verify a failed plan

A failed planner normally creates no output directory. Gate every verifier on
both the planner's exit status and directory existence:

```powershell
# Set the command names and argument arrays from each subcommand's --help.
& $Python -P -m pathfinder $PlanCommand @PlanArgs
$PlanStatus = $LASTEXITCODE

if ($PlanStatus -ne 0 -or -not (Test-Path -LiteralPath $PlanDir)) {
    throw "Planning failed; verification and submission are prohibited"
}

& $Python -P -m pathfinder $VerifyCommand @VerifyArgs
if ($LASTEXITCODE -ne 0) { throw "Plan verification failed" }
```

On Linux, verify checksums from inside each artifact directory:

```bash
(
  cd "$PF_ARTIFACT_DIR" || exit 1
  sha256sum -c SHA256SUMS
)
```

Write new checksum-bound text as UTF-8/LF bytes. An old CRLF checksum manifest
must remain immutable: distinguish a checksum-reader line-ending error from a
payload mismatch, then use a documented reader that accepts CRLF terminators
without changing payload bytes or the frozen manifest. Do not rewrite it in
place or treat an unsuccessful checksum command as a pass.

Some packages enforce an **exact file set**, including N3 indexed packages.
Keep supplemental runtime-frame manifests, diagnostics and ledgers in sibling
directories and bind them through supported source commitments; do not drop
extra files into a verified package. A sibling is not automatically bound.

### Regenerate only what the source binding requires

Use the canonical verifier to determine whether a change is admission-bound.
Do not infer this from the filename or from how small the patch looks.

| Changed input | Minimum expected impact |
| --- | --- |
| Semantic input/profile module | Semantic matrix, admission, artifact preflight, promotion, index-query catalog, one-case package, and gates that bind them; deploy N6/N7/N8 as applicable |
| Route adapter or semantic runtime | Regenerate any admission/inventory that records its source digest; deploy N7/N8 |
| N6 adapter | Regenerate its source-bound admission/inventory; deploy N6 and any bound route runtime |
| N3 raw package/catalog | Re-freeze N3 and all downstream bindings that commit to its content identities |
| Query-aware N3 selection / runtime-frame manifest | Verify N3 package, artifact bindings, exact-range/provisioning catalogs, matrix/admission, live preflight and downstream catalog/one-case/N4 gates; follow the actual dependency chain |
| N4 derived generation | Re-freeze publication snapshot, N4 package, artifact bindings, and the serve gate |
| CLI-only, non-bound orchestration code | Run the canonical verifier; do not regenerate admission automatically if it proves the source set is unchanged |
| Documentation only | No runtime artifact regeneration |

Never edit a frozen artifact to make verification pass. Generate a new
timestamped directory and retain the old one as evidence.

Check a candidate verifier invocation against an unchanged control package
where possible. A bad flag, absent optional dependency or missing path is not
evidence that a package needs regeneration. Use each subcommand's real
`--help`; `--output-dir`, `--package-dir`, `--catalog-dir` and source arguments
are not interchangeable. Keep one ledger of passed gates and reuse it unless
their committed inputs change; do not rediscover and rebuild the entire chain
at every phase.

### Timeout gate

Distinguish a plan's operation deadline from the FlowMesh API-task timeout and
the client's polling/wait deadline. The shared batch adapters currently require
`task_timeout_seconds >= 900` in the frozen **batch configuration** and pass it
to the executor. This is a floor, not a guarantee that every workload fits.
The 576.03-second D4 transfer floor belonged to an earlier shaped-network
profile; it is not the measured runtime of every raw route.

Derive deadlines from the selected plan's operation bounds plus storage,
inference, queueing and control-plane allowance. If a plan-bound deadline is
wrong, freeze a new plan and its dependencies; if only the batch timeout needs
changing, freeze a new batch config and verify that it still satisfies the
plan. Do not hot-patch a running or frozen experiment's timeout to rescue it.

### Reusable experiment runner

Do **not** copy and rename the 24-route or 28-route Python runner/verifier for
each cohort. Use `experiments.batch`; the frozen configuration schema selects
either the R/D/DC/I interleaved contract or the existing ten-route contract.
`experiments.interleaved_batch` remains a compatible entry point.
See `experiments/README.md` for the active/historical tool inventory. The dated
24/28-route entry points now delegate to that shared implementation. Their
offline verifier APIs remain available to historical cost audits; executing
through a dated runner requires an explicit frozen `--config-dir` and writes
the common output format. Do not pass that new output to a legacy verifier.
For interleaved batches, a new batch changes a small public draft
configuration: immutable artifact directory names, worker alias/node,
coordinator origin, task timeout, expected plan digest, question/route counts,
and optional baseline directory. The draft contains no credential or label.
The existing `experiments/multiq_pilot_20260924/configs/dev-24.draft.json`
and `experiments/multiq_pilot_20260924/configs/sealed-28.draft.json`
show the two verified shapes; they are examples, not the next cohort's frozen
inputs.

Freeze that complete draft **once**, before any submission, to a fresh
directory with `freeze-config`. The tool writes `batch-config.json` and
`SHA256SUMS` and refuses to overwrite an existing directory. Then use the
same frozen configuration for `freeze-inputs`, `check`, `preflight`, `execute`,
and `verify`. `freeze-inputs` reuses the canonical three-stage route-binding,
DAG, and admission freezers after public plan, N1 commitment, N3 selections,
query embeddings, and other upstream packages are ready:

The following are command shapes with placeholders, not a ready-to-submit
script. Substitute verified paths and run each step only after its gates pass.
Use `python -P` with the clean import root prepared above; single lines work in
both PowerShell and Bash without mixing their continuation syntax.

```text
python -P -m experiments.batch freeze-config --draft DRAFT.json --output-dir NEW_CONFIG_DIR
python -P -m experiments.batch freeze-inputs --config-dir NEW_CONFIG_DIR --artifact-root ARTIFACT_ROOT
python -P -m experiments.batch check --config-dir NEW_CONFIG_DIR --artifact-root ARTIFACT_ROOT
python -P -m experiments.batch preflight --config-dir NEW_CONFIG_DIR --artifact-root ARTIFACT_ROOT
python -P -m experiments.batch execute --config-dir NEW_CONFIG_DIR --artifact-root ARTIFACT_ROOT --output-dir FRESH_RUN_DIR
python -P -m experiments.batch verify --config-dir NEW_CONFIG_DIR --artifact-root ARTIFACT_ROOT --output-dir FRESH_RUN_DIR --seal
```

Run these from a clean, dependency-complete source environment; `check` uses
the canonical source-bound verifier and may need the video-preparation
dependencies. It must not be replaced by a checksum-only check. `preflight`
reads the FlowMesh worker registry but makes no route or LLM request;
`execute` alone submits workflows. `verify --seal` applies only to an
unsealed, completed, fresh output directory; use plain `verify` afterward.
If the configuration names a baseline specification, freeze and verify that
specification after `freeze-inputs` and before `check`; it must bind the new
admission and plan digests.
Skip `freeze-inputs` when its downstream targets already exist and verify;
it refuses existing binding/DAG/admission targets and is not a resume command.
It also does not regenerate N1 private inputs, captions, query embeddings or
N3 selections. Those upstream packages have their own canonical tools.
All existing frozen receipts remain untouched. The reusable verifier accepts
the historical 24-route and 28-route formats for read-only regression, while
new runs use one format with per-route timing and configuration digest.

This replaces per-cohort runner/verifier code, **not** the source-bound
preparation pipeline. A new question may still require new public tasks,
private N1 commitments, query embeddings, N3 selections, bindings, DAGs,
admission and deployment checks. Reuse any video-level package only when its
canonical verifier confirms the binding. A new cohort **within the supported
contract** should not require copying Python code. A new route family, host
policy or evidence schema can require an explicit extension with focused
tests; do not bypass a gate or label every unsupported design a pipeline bug.

Current automation boundaries:

- `check` verifies configured immutable inputs; it does not inspect live
  deployment health, mount contents, auth or cache state.
- `preflight` adds read-only worker-registry checks; it does not test dispatch,
  result upload or N6 inference. Validate the chosen Root/node/namespace/
  cluster separately without a paid smoke.
- `execute` serially invokes the frozen routes and persists progress. It does
  not provision upstream assets, reconcile the provider bill, or guarantee a
  complete cost ledger. Cost journals and exports require separate checks.
- New interleaved run identities are **inside the frozen plan**, not supplied
  through `--run-id`; a fresh output path alone does not create a fresh run.

#### Ten-route compatibility (D0--D7, N7 and N8)

Use schema `pathfinder.ten-route-batch-config/v1`; start from
`experiments/configs/ten-route.draft.example.json`. Its placeholders must be
replaced before freezing. The ten executions retain the original order: raw,
indexed, derived, cache miss, cache hit on N7, then the same five on N8.
These are eight designs with two extra cache-hit observations, not ten
independent policy actions. The adapter delegates to the original runner and
verifier, preserving their case IDs, prerequisites, idempotency and evidence.

The same `freeze-config`, `check`, `preflight`, `execute`, and `verify`
commands apply, with two differences:

- `execute` requires `--run-id FRESH_RUN_ID` as well as a new output directory.
- Use plain `verify`, without `--seal`: the canonical ten-route runner already
  writes `SHA256SUMS` with its receipt and JSONL results. Progress and timings
  go in a separate `FRESH_RUN_DIR.attempt/` sibling, leaving the three-file
  canonical receipt format intact. A failed attempt cannot be blindly resumed.

`runtime_environment` selects `local` or `multi-host-private-network`.
`one_case_selection` and `one_case_plan_dir` select a single public workload;
set both to null for the admission's original representative smokes. For a new
one-case plan, `freeze-inputs` delegates to the existing one-case freezer;
it does not rebuild an admission or N4 packages. Skip it for an existing plan.

All configured source paths are relative to `--artifact-root`. For live N4,
provide the existing public live-gate descriptor and pin its file SHA-256;
paths inside it resolve relative to that descriptor and must stay under the
artifact root. For preprovisioned N4, set the descriptor fields to null and
supply the compose/bootstrap/provisioning/N4-package paths. Private oracle
packages and credentials never belong in this configuration.

Contract compatibility does not authorize changing historical semantics.
For example, the September 18 sampled-raw profile predates direct-video raw
input. Its old evidence must be fully verified with its matching source
revision; the current verifier must not reinterpret it as direct video.
`check` still enforces the canonical admission, N4 gate, deployment origins,
worker pin and one-case plan before any submission. If a historical source
binding fails, use the matching verifier revision for archival verification,
or prepare new immutable inputs for a new experiment.

### Multi-question index and cache state

Freeze the video-disjoint development/test split, question IDs, seed, ordering,
baseline policy and budget before outcomes. A split disjoint from only the
immediately preceding pilot is not necessarily unseen across earlier runs.
Do not select test videos or change question order to obtain better results.

Video captions/embeddings are video-level reusable builds. Query embedding and
temporal selection are question-level work. A query-selected N3 frame bundle
is bound to that question/selection, not a universal video index or a partial
MP4 range. Record source read bytes, frame payload/bundle bytes, network
handoff bytes and N6 input bytes separately. Source-side projection may read
the complete MP4 even when it transfers fewer bytes.

For the interleaved family, keep a distinct `run_id` per route while **DC**
routes share the one frozen `cache_episode_id`. R/D/I must not acquire that
episode. The admission binds episode, run and trial; declaring the same string
in an environment file does not create valid sharing. Use a fresh episode for
an independent batch and verify the intended initial state. Do not clear the
cache after each question (that destroys reuse) or run parallel batches into
the same episode. No volume deletion is needed merely to obtain a fresh
namespace; preserve old state and establish isolation through the verified
cache contract, or provision an additional volume if that contract requires it.

The current batch is serial by design; do not submit every question together
to try to create cache hits. A hit reuses the committed **artifact**, not an
answer, and still normally calls N6. Artifact cache hits and the LLM provider's
cached-input tokens are different events. Verify real lookup/insert lineage,
artifact identity and eviction effects instead of assuming every later access
hits. Ten-case miss/hit pairs use their original canonical smoke ordering;
do not replace it with interleaved episode semantics.

## 3. Staging files safely on Windows and Linux

### Windows path length and ACL failures

Deeply nested transferred evidence has produced `Filename too long`, partial
copies, and `Access denied` errors. Use a short staging root such as
`D:\pf-stage` or `D:\pf-run-<commit>`.

Prefer one archive plus a separately recorded SHA-256 over recursive
`Copy-Item` of a deep tree:

```powershell
$StageRoot = "D:\pf-stage"
New-Item -ItemType Directory -Force -Path $StageRoot | Out-Null

$Archive = Join-Path $StageRoot "pathfinder-inputs.tar.gz"
$ExpectedSha256 = "PASTE_RECORDED_SHA256"
$ActualSha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $Archive).Hash.ToLower()

if ($ActualSha256 -ne $ExpectedSha256.ToLower()) {
    throw "Transferred archive checksum mismatch"
}
```

After extraction, compare expected file counts and run every packaged checksum
manifest. A partially copied directory is not a valid fallback.

### Shell text corruption

Copy commands only from fenced code blocks or plain text. Rendered chat text
can turn `http://...` into a Markdown link and can show options as `\--flag`.
The shell command must contain plain URLs and `--flag`, never brackets,
parenthesized link targets, HTML entities, or a leading backslash.

Bash uses `\` for line continuation; PowerShell uses a backtick. Do not paste
Bash brace-expanded paths into PowerShell. Prefer short single-line commands
or checked-in scripts for long operations. Scan generated Python/shell text
for stray control bytes (especially a literal 0x08 in place of regex `\b`)
before running it. Fix the generator, not just one generated copy.

For `luyao3`, use the configured SSH alias so ProxyJump/identity settings apply;
do not replace it with its LAN IP. For UpCloud, use the selected Root/jump
configuration. Neither requires opening an unrelated public service port.

For exploratory server diagnostics, avoid global `set -e`; it can terminate
the SSH session after an expected failed probe. Capture and inspect individual
statuses instead. Production scripts may use `set -euo pipefail` only after
their inputs have been validated.

## 4. Deployment gates

### Endpoint and origin correctness

The historical local simulator publishes ports 19081 through 19088. A plan
once froze 29083, 29086, and 29087, for which no listeners existed. Healthy
containers could not repair a bad plan. Freeze a new plan against the actual
endpoints; do not mutate the old plan and do not restart healthy containers.

For a same-host Docker-network profile, N3/N4 may advertise origins such as:

```text
http://pathfinder-full-flow-n3-raw-data-agent:8780
http://pathfinder-full-flow-n4-derived-data-agent:8780
```

These are **not** automatically cross-host DNS names. For UpCloud, preserve
the deployed private alias-to-IP mappings (`extra_hosts` or managed private
DNS), actual service ports, and client allowlists. N3/N4 advertised origins
must match what the route clients use (scheme, hostname, effective port), not
the workstation's tunneled URL. `compose.route-state`/network overlays can
carry essential mappings; record the complete Compose file set, project,
service, env-file sources and mounts before recreating a service. Do not
reconstruct it from a fragment or a matching tag alone.

Inside a normal bridge container, `127.0.0.1` refers to that container; under
host networking it refers to the host. Neither reaches another VM. Operator
loopback tunnels must not leak into cross-host frozen dependencies.

#### Private IP is not a universal HTTP allowlist entry

Different boundaries deliberately have different contracts:

- `DataAgentClientSettings.simulator_private_http_hosts` accepts unique frozen
  names matching `pathfinder-sim-*` or `pathfinder-full-flow-*`, not numeric
  `10.x` IPs. The N6 HTTP client likewise restricts plain HTTP to loopback or
  allowlisted simulator names. Use the approved service alias mapped to its
  private IP; do not broaden the regex or disable same-origin/HTTPS checks.
- The current interleaved admission explicitly permits the approved N7
  coordinator origins `http://10.70.0.17:8780` and
  `http://10.70.0.17:18780`. That does **not** authorize substituting those IPs
  into Data Agent/N6 allowlists or inventing another coordinator port.
- The ten-case multi-host adapter verifies coordinator origins against its
  selected deployment binding. Do not invoke the local-only verifier against
  a multi-host artifact and interpret its rejection as broken infrastructure.

Check the consumer's constructor/CLI contract offline, then resolve the
approved alias and health-check the exact origin from each participating
coordinator. A workstation-only health check is insufficient.

### Selective deployment

Use independently renderable per-service Compose fragments. A former unified
overlay required all 117 variables even when starting one service, causing
unrelated credential and configuration failures.

For an unchanged image, reuse its verified digest. When a rebuild is required,
build once per distinct image/build host, then use `up --no-build`; do not let
parallel Compose builds export the same mutable tag. Pin deployments to the
**new** verified image ID/digest, with the old digest retained only for rollback.
An image built on one VM is not present on another until distributed or built
there. Do not use `latest` or an old rollback digest as the new deployment.

Service names and network aliases must be single DNS labels of at most 63
characters. A 69-character N1 verifier name passed container startup but could
not resolve from N7/N8.

Preserve these deployment invariants:

- state volumes, host ports, aliases, non-root user, read-only root filesystem,
  `cap_drop`, and `no-new-privileges`;
- hidden labels mounted only into N1 scorer/verifier services;
- immutable packages mounted read-only;
- no `down`, `--remove-orphans`, prune, or volume removal;
- automatic rollback when a recreated service fails health or dependency
  checks.

Independent builds may produce different image IDs for multiple reasons;
do not attribute that solely to metadata without inspecting the build inputs.
Record base image/dependency identities and embedded source digests as well
as per-host image IDs. Matching Python files alone does not prove identical
runtime dependencies. Shared locked registry images are preferable where
available; cross-host digest equality is required only when the deployment
contract specifies that same image manifest.

### Permissions and hidden labels

Do not solve hidden-label read failures with world-readable permissions.
Respect the selected deployment's ownership and isolation contract: either
owner-only access for its runtime UID or an explicitly authorized minimal ACL.
The earlier `luyao3` ACL recipe is not a mandatory step on UpCloud. Grant only
traverse/read access where needed, never write, and keep mounts read-only.
Verify effective access with metadata/`test -r`, not by printing labels.

When promotion needs the real private oracle, run the canonical tool **on N1**
with read-only private/public inputs, a writable new output and no unnecessary
network. Export only public commitments. A commitment directory is not an
oracle package. Only the scorer/verifier and an authorized one-shot N1
promotion process may mount it. ACLs are UID-scoped, not container-scoped;
mount isolation is essential.

### Credential families are different

Do not apply one token-precedence rule to every service:

- N3/N4 Data Agents select node-specific credentials first, with a shared
  value only as a compatibility fallback.
- N2/N7/N8 regular indexes use their frozen shared index credential contract.
- N7/N8 persistent caches use their frozen shared cache credential contract.
- W4 candidate caches keep their dedicated credential.
- N1's hidden labels and evidence-signing secret remain on N1 scorer/verifier.
  The oracle bearer token is also needed by authorized N7/N8 scoring clients;
  their remote-verification client uses the separate verification token.
  Do not distribute the evidence-signing secret to route coordinators.

An authentication probe should use the deployed client's resolver and a known
endpoint with a harmless invalid body. Confirm in that endpoint's handler
that auth precedes validation: expected evidence is invalid token -> 401/403,
valid token + invalid body -> the known validation error. **An arbitrary 400
alone is not proof** of authentication. 404/501 says nothing if routing occurs
first. Probes must not infer, materialize, score a real answer or populate cache.

Never print, hash, copy into evidence, or include credential values on a
command line. Inspect key presence and equality only through booleans or
in-process comparison.

Source credential env files silently with tracing disabled, check only required
key presence, and never combine N3/N4 per-service values into one shared map
that shadows one node's token. Runtime secrets and deployment settings can
reside in different files: do not require every key to exist in a single file
or `source` an empty search result. Record non-secret filenames/roles, not
contents. Rotation is a separate authorized operation: N1 evidence-key
rotation can invalidate its state binding and require a fresh preserved-volume
branch. Old evidence checksums remain valid, but a retired/destroyed HMAC key
cannot be re-authenticated by the new runtime; report those separately.

## 5. FlowMesh and SSH readiness

### Tunnel readiness on Windows

Only create a tunnel when the configured Root is accessed through SSH. Prefer
the existing reviewed connection helper/alias; the example below assumes
non-interactive SSH authentication is already configured. Password/passphrase
setup belongs in an operator-visible session, not a hidden process. Keep
background windows hidden and refuse an occupied port rather than mistaking a
stale tunnel's listener for the new one:

```powershell
$LocalPort = 18010 # Example; choose a verified-unused port.
$RemotePort = 8010 # Verify against this Root's deployment.
$RootAlias = "pathfinder-upcloud-root" # Verify the configured SSH alias.
if (Get-NetTCPConnection -State Listen -LocalPort $LocalPort -ErrorAction SilentlyContinue) {
    throw "Port occupied; identify its owner or choose a different port"
}
$Arguments = "-N -o BatchMode=yes -o ExitOnForwardFailure=yes -L 127.0.0.1:${LocalPort}:127.0.0.1:${RemotePort} $RootAlias"

$Tunnel = Start-Process `
    -FilePath "ssh" `
    -ArgumentList $Arguments `
    -PassThru `
    -WindowStyle Hidden

Start-Sleep -Seconds 2
$Tunnel.Refresh()
$Listener = Get-NetTCPConnection `
    -State Listen `
    -LocalPort $LocalPort `
    -ErrorAction SilentlyContinue

if ($Tunnel.HasExited -or -not $Listener -or
    @($Listener | Where-Object OwningProcess -eq $Tunnel.Id).Count -eq 0) {
    if (-not $Tunnel.HasExited) { Stop-Process -Id $Tunnel.Id }
    throw "SSH tunnel is not listening; do not submit"
}
```

After listener ownership is established, confirm the intended Root through
read-only API identity/worker checks. A listening TCP socket alone proves
neither remote reachability nor Root identity. Stop only a tunnel this task
owns; do not kill an unrelated listener. Linux diagnostic form for environment
assignment is `timeout 30s env PYTHONPATH=. python -m pathfinder ...`, not
`timeout 30s PYTHONPATH=. python ...`.

### Root authentication mode

The self-hosted test Root may intentionally run without API-key
authentication, while the SDK still requires a non-empty constructor value.
Use a clearly non-secret local placeholder only after a read-only Root endpoint
returns HTTP 200 without a key and the intended Root/worker identity is
confirmed. This exception must never be generalized to a protected Root.

### Worker pinning

Pin by worker alias, not a historical worker ID. IDs change when a worker is
re-registered. Before every submission, require:

- exactly one current worker for the alias;
- stale registrations excluded;
- expected node/cluster/namespace;
- worker reachable through the intended Node Server;
- a read-only preflight exit status of zero.

At this review, repository SDK/API adapters target FlowMesh **0.1.9**. This is
a pinned compatibility version, not a claim about the latest release or live
installation. Check `pyproject.toml`, installed SDK and deployed Root/Node/
worker versions before changing any of them. Do not modify the FlowMesh
repository; upgrade only through authorized released versions and deployment
procedures. Keep prior image/config identities for rollback.

`preflight-flowmesh` proves Root visibility and unique current registration;
its explicit `not_verified` fields include dispatch, result upload and worker
agent configuration. The common batch additionally checks alias, node alias
and readiness status, not the complete network/mount/auth checklist. Verify
namespace/cluster and worker-to-coordinator reachability separately. The
historical alias `pathfinder_costaware_20260815a` can exist on a different Root;
the alias alone never proves this is the UpCloud worker.

`worker up` returning "already exists" while preflight finds no current worker
is a lifecycle/registry problem, not evidence that the worker is usable.

Do not attribute nearby background workflows to the experiment by timestamp.
Bind lineage using the run ID and trial key in task metadata. This previously
showed that a `wkr-91` vLLM failure belonged to unrelated traffic, while the
Pathfinder smoke was correctly pinned to its own worker.

### Intermittent control-plane failures

Identity-provider HTTP 503 and result-upload timeouts are FlowMesh
control-plane failures. Do not rebuild Pathfinder simulator containers for
them. First determine:

1. whether Root recorded dispatch;
2. whether the pinned worker received the task;
3. whether the route container received the request;
4. whether result upload reached Root.

If no Pathfinder container received a request, restarting N1-N8 cannot fix the
problem. Preserve the failed workflow identifiers, wait for service recovery
or contact the operator, and then use a new run identity. Avoid chained blind
recovery attempts.

## 6. Running exactly once and capturing the real status

Always generate a fresh identity:

```powershell
$Stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ").ToLower()
$Nonce = [guid]::NewGuid().ToString("N").Substring(0, 8)
$RunId = "pathfinder-$Stamp-$Nonce"
```

The generated value is for a new plan/batch identity or the ten-case
`--run-id`, as appropriate. For interleaved runs, freeze it into the plan and
derive the per-route identities there; do not override them at execution time.

Do not reuse a run ID, request ID, output directory, or operation key for a
new experimental submission after a partial or failed execution. Distinguish
a documented read-only/idempotent result-recovery operation from resubmitting
the workload. Inspect durable state first: a client timeout does not prove
inference failed, and a second submission could pay again or break the frozen
cache order. The shared batch CLI has no general resume command.

Do not pipe the runner directly to `Tee-Object` and then read
`$LASTEXITCODE`; a later pipeline command can mask the runner's status. Capture
output first, save the native status immediately, and display it afterward:

```powershell
# RunArgs contains this family's frozen config, artifact root and fresh output.
& $Python -P -m experiments.batch execute @RunArgs *> $ConsoleLog
$RunStatus = $LASTEXITCODE
Get-Content -LiteralPath $ConsoleLog

if ($RunStatus -ne 0) {
    throw "Experiment failed with native status $RunStatus"
}
```

On Bash with `tee`, enable `set -o pipefail` and capture the entire status
array immediately, before `echo` or any other command resets it:

```bash
command_to_run | tee "$PF_CONSOLE_LOG"
PF_PIPE_STATUS=("${PIPESTATUS[@]}")
printf 'Runner=%s tee=%s\n' "${PF_PIPE_STATUS[0]}" "${PF_PIPE_STATUS[1]}"
```

Prefer redirection and immediate `$?` capture when possible. Neither a zero
shell status nor an error-free log substitutes for the canonical receipt
verifier; treat a reported error as failure even if the printed shell status
was accidentally zero.

No **final** output directory does not mean no work was paid for. New ten-case
runs persist progress in `FRESH_RUN_DIR.attempt/`; interleaved runs write each
route/timing and `failure.json` into their output directory. Inspect those,
the console, route/N6 durable state and the exact bound workflow IDs. Do not
seal a partial attempt or fabricate a successful summary, and do not discard
completed rows while diagnosing the first failed boundary.

## 7. Evidence verification and claim boundaries

After a successful run:

1. run the source-bound run verifier;
2. run `sha256sum -c SHA256SUMS`;
3. compare planned, executed, inactive, and completed counts;
4. record workflow IDs and selected/assigned worker lineage;
5. copy the immutable evidence to its long-term directory;
6. verify the copy again;
7. only then compute summary tables.

Keep these outcomes separate:

- **Infrastructure complete:** FlowMesh transport and all route stages ran.
- **Evidence verified:** source binding, commitments, telemetry, and hidden
  scoring authenticity passed.
- **Task success:** the model answer matched the hidden label.
- **Performance evidence:** repeated real measurements support a latency or
  throughput comparison.
- **Cost evidence:** measured resource units were priced under an explicit
  frozen list-price/allocation rule; this is distinct from an observed invoice.

A route can be infrastructure-complete and semantically wrong. The earlier
placeholder-semantic runs were conformance evidence even with every
`task_success=false`. Other ten-case runs used real scoring; read that run's
frozen task/scoring contract instead of copying success counts from an old
report. A wrong answer is not a reason to retry until it becomes correct.

LLM latency is variable and often dominates the total. Do not claim a cache,
placement, or representation advantage from one end-to-end observation. Use
multiple repetitions and report component service time, transferred bytes,
and model time separately.

### Cost accounting for multi-question experiments

Use **provider list-price cost**, not invoice payment, as the comparable model
cost: multiply recorded input, cached-input, and output tokens by the frozen
official rates for the exact model, region, and date. A discount, free credit,
or account balance changes the invoice, not this experimental price. Keep the
rate snapshot and units beside the result; never describe list price as an
observed charge. For N6, use its per-request provider-usage journal and join
each row to a verified route by both request and result SHA-256. Do not infer
tokens from payload bytes or match requests by timestamp alone. Check that the
join is one-to-one and covers every completed inference before reporting a
complete N6 total. Keep one-to-one inference joins separate from **one-to-many
provider attempts**: retries can add billable calls beyond the final result.
The journal's presence does not recover older runs without matching identities.

Current N6 capture (`container_node.py`) uses two state databases:

| Database under the configured `state_dir` (normally `/state`) | Contents / limitation |
| --- | --- |
| `n6-provider-usage-v1.sqlite3`, table `n6_provider_usage` | Completed-result/request SHA-256 and input/cached/output/total token units; no prompt or answer |
| `n6-provider-trace-v1.sqlite3`, table `n6_provider_attempts` | Trace/attempt index, request/result binding, outcome/HTTP status and hashed provider IDs; failed attempts may have no result binding or recoverable usage |

Provider request IDs are stored as SHA-256 of validated IDs, not plaintext.
To reconcile a provider export, hash its corresponding ID consistently and
require exact matches; do not guess by time, row order or a nearby token count.
Completion IDs and HTTP request IDs are distinct columns, not interchangeable.
The numeric-only exporter
`experiments/multiq_pilot_20260924/export_n6_usage_numeric.py` deliberately
omits those IDs and is not by itself a provider-log reconciliation tool.

Before a cost run, confirm the deployed capture code, writable durable state,
and health counter baseline without calling the model. Afterward, export
read-only, reconcile every completed inference, check attempt counts and
compare `semantic_usage_journal_error_count` before/after. The counter covers
journal write failures, **not** missing/invalid provider usage. Journaling is
best-effort so a paid result is not retried merely because its cost write failed;
`COMPLETE`, zero counter growth or a present DB alone cannot prove cost coverage.

**Historical fixed-price basis, not a live price quote:** the Singapore
snapshot in `pathfinder/rsi_exam/offline_replay_costing.py::PRICE_SNAPSHOT`
is dated 2026-09-23. Reproduce those results using its recorded USD calculation:
`((input_units - cached_input_units) * 0.50 + cached_input_units * 0.10 +
output_units * 3.00) / 1,000,000`. Count implicit-cache input only once, at
the cached rate. Use the provider's total output-token field; do not add its
reasoning-token detail a second time when already included.
For `text-embedding-v4`, use `prompt_tokens * 0.07 / 1,000,000`. Freeze a new
official rate snapshot if model, region, date, billing tier, or cache mode
changes. A new experiment must either explicitly reuse this fixed historical
basis for comparability or verify and freeze an updated official rate card;
do not silently call a hard-coded rate "today's price". Verify whether that
provider/model reports cached-input detail: the current extractor defaults an
absent detail to zero, so document that assumption or reconcile the provider
export rather than claiming cache-hit evidence. An unobserved failed provider
attempt may still be billable, so mark its amount unknown rather than zero.

Keep these buckets distinct for each object, question, and route:

1. One-time, video-level preparation: frame decoding, captions, embeddings,
   and N4 publication. Record per-request model usage immediately and the
   actual CPU/wall interval on the host that builds or publishes it. If the
   artifact already exists, record its frozen build receipt and charge it only
   under the declared cold-start or amortization scenario.
2. Per-question preparation: query embedding, query-dependent frame selection
   or materialization, and their measured usage/time. A video-level index may
   be reused across questions; a question-dependent selection is not a single
   reusable index build.
3. Per-route execution: N6 input/cache/output token usage, original-object
   bytes read, handoff bytes, cache hit/miss, component service times, and
   FlowMesh start/finish times. A cache hit must be proven from the state and
   key, not assumed from route order.
4. Infrastructure: freeze the cloud rate card and timestamped active VM,
   storage, and network intervals. Allocate shared VM charges by the declared
   experiment-time rule, recording the numerator and denominator. Provider
   model list prices and cloud VM prices are separate components.

Report cold-start, warm-reuse, and amortized-at-Q costs separately. For an
interleaved multi-question run, preserve the exact question order and cache
state so replay can charge required build costs once, at their first use and
under the declared policy. A state transition in replay must have matching
measured evidence; a DC cold miss cannot be replaced by a warm-hit observation.
Count shared builds once per declared scope, not once per route, and do not
add model-API costs twice when comparing N6 versus build buckets.

Missing build compute/publication timing, required resource rates, or unknown
provider usage makes the affected total **partial**. Mark the component and
full total `unknown`/`null`, not zero; do not substitute a new run's timing for
the old run. **Missing invoice/payment fields do not invalidate token x
list-price cost**: discounts and credits are intentionally outside that basis.
Record cloud plan cost over the actual interval separately from the chosen
per-query allocation; time-prorated allocation is not necessarily the
provider's rounded-hour invoice or the incremental cost of one request.

Use currently deployed per-call persistence and verify its coverage. New
materializers must cache allowed response content and validated usage after
each paid call, including diagnosable failed responses, before moving on.
Do not promise that the existing N6 trace contains usage for every failed
attempt or raw response content. Keep credentials, raw prompts/answers,
hidden labels and model reasoning out of public cost artifacts. Do not
repeat a successful model call merely to fill a missing billing field.

For RSI replay, declare complete/partial cost coverage in the package and
report the known subtotal separately. Do not declare `always-derived` the
full-cost winner while D/DC build or infrastructure buckets remain unknown.
Freeze baseline policies before held-out results; run the baseline on the
measured action/state, without inventing unobserved counterfactual answers.

## Recurring failure catalogue

| Symptom | Usual cause | Prevention | Correct response |
| --- | --- | --- | --- |
| `source ... does not match`, adapter inventory mismatch | Wrong import root/revision, or CRLF under a raw-byte contract | `git -c core.autocrlf=false archive`, clean import root and the format's canonical verifier | Diagnose the exact commitment scheme first; regenerate only affected artifacts, never patch frozen JSON |
| `simulator private HTTP hosts are invalid` | Numeric private IPs supplied to a service-name-only allowlist | Distinguish coordinator origins from Data Agent/N6 client contracts; verify private alias mapping | Use the approved alias/allowlist, not a looser security check |
| Route HTTP 401 although a token exists | Wrong credential precedence or wrong service-family token | Probe with deployed resolver and valid route; preserve family-specific contracts | Fix selection/configuration; do not rotate unrelated credentials |
| Route HTTP 401 with signed semantic body | Canonical JSON/HMAC mismatch such as `0.0` versus `0` across Pydantic/wire serialization | Freeze integral values as integers; compare sender and receiver canonical byte length/digest before submit | Fix canonical representation and focused tests; create a fresh run |
| Data Agent range client requires 206 but receives 200 | Full-span `Range` request treated as a non-partial interval | Preserve whether a Range header was requested; test full-span range | Fix server range semantics; keep client fail-closed |
| `container operation was replayed` | Reused run/request/operation identity | Timestamp plus nonce; new output directory every time | Do not bypass idempotency; start a fresh run |
| Plan verifier says directory missing | Planner already failed | Gate verifier on planner status and directory existence | Fix planner input first |
| Timeout rejected at freeze time | Frozen timeout below derived network floor | Inspect maximum lower bound before freezing | Freeze a new plan with adequate timeout |
| Connection refused on 29xxx | Frozen port does not match actual 19xxx runtime | Probe ports before freeze; bind actual endpoints | New plan or authorized forwarder; no container churn |
| Data Agent security/origin error | Agent advertises host loopback while route uses Docker DNS/private DNS | Compare configured and advertised normalized origins from N7 and N8 | Recreate only N3/N4 with correct advertised origins |
| Container healthy but dependency name does not resolve | Oversized DNS label, missing alias, or omitted cross-host overlay | Check names plus the full Compose/extra-host mapping set | Restore the approved mapping; regenerate names only if the name itself is invalid |
| Single-service Compose launch asks for unrelated secrets | Whole overlay interpolation requires all variables | Use selective per-service fragments | Regenerate overlay; never fabricate credentials |
| Parallel build says image already exists | Multiple services export the same image tag | Build shared image once | Continue with verified image and `up --no-build` |
| Preflight finds no worker; `worker up` says already exists | Stale lifecycle state or missing current registration | Check alias registry and container status separately | Repair worker registration; do not submit unpinned |
| Workflow fails before any container request | Root/identity-provider/dispatch issue | Check Root task state and route access logs | Preserve IDs and wait/escalate; do not rebuild N1-N8 |
| Result upload times out | Worker-to-Root result path issue | Verify callback/result endpoint reachability | Repair control plane; do not replay the same operation |
| Error printed but reported status is zero | Pipeline masked native exit code | Redirect output or capture `PIPESTATUS[0]` | Treat the run as failed and inspect durable state |
| PowerShell SSH process exists but no usable tunnel | Quoting, authentication prompt, remote bind failure or stale listener | Batch authentication, `ExitOnForwardFailure`, free port and listener ownership checks | Repair only the owned tunnel before preflight |
| Files apparently missing after transfer | Windows MAX_PATH, ACL, or partial recursive copy | Short staging root, archive transfer, checksum and count gates | Restage from the verified archive |
| N4 current generation loses earlier objects | Atomic publication replaces the snapshot rather than merging ad hoc runs | Build and bind one complete generation such as the exact-six store | Re-publish a complete generation and re-freeze its gate |
| Evidence says model input does not bind routed artifacts | Verifier and route disagree about the actual inference frontier | Bind exactly the artifacts used for inference while preserving the full routed set | Fix evidence semantics; do not weaken equality checks blindly |
| Transport cannot bind a runtime value | New value type lacks an explicit canonical handoff | Add a fail-closed, commitment-complete transport branch and focused tests | Deploy only affected route services and use a new run |
| N3 indexed package file set changed | Extra audit/manifest written inside an exact-file-set package | Put supplements in a sibling and bind them explicitly | Re-freeze to a new directory; do not delete files from frozen evidence |
| Query-aware bundle rejected at N6 | Legacy sampling-method name used instead of the actual bound policy | Validate source/policy digests and the declared profile end-to-end offline | Fix the consumption gate with rejection tests; do not accept arbitrary bundles |
| DC misses on every question or sees another run's hit | Cache tied only to per-route ID, or an episode reused across experiments | Signed admission-bound cross-question DC episode with unique batch scope | Preserve existing evidence; correct the frozen schedule/namespace |
| Paid captions lost after a later window failed | Partial results only saved at the end | Persist allowed response, usage and validation outcome per attempt before advancing | Reuse verified cached results; count failed/lost calls against the budget |
| Inference COMPLETE but cost rows absent | Missing provider usage or best-effort journal failure | Deployed capture, durable state, health-counter delta and per-result join checks | Mark partial cost; do not repeat successful inference merely for accounting |
| Workflow DONE but `task_success=false` | Wrong model answer or placeholder task semantics | Inspect hidden-score receipt and semantics mode | Report semantic failure separately from infrastructure success |

## Failure classification and stop conditions

Classify the first failing boundary before changing anything:

```text
freeze/verify failed
  -> local source or immutable-input problem; no deployment or submission

container failed health
  -> deployment, mount, source-binding, permission, or configuration problem
     rollback affected services; no submission

preflight/tunnel/worker failed
  -> control-plane readiness problem; no submission

workflow has no dispatch and route saw no request
  -> FlowMesh Root/Node/identity path

route durable state = FAILED, no evidence
  -> route adapter, Data Agent, index, cache, N6, or N1 runtime boundary

route durable state = COMPLETE, public verifier rejects evidence
  -> evidence schema, source binding, or commitment semantics

receipt VERIFIED, task_success = false
  -> semantic/task-quality outcome, not an infrastructure retry condition
```

Once a workflow has been submitted, do not perform a blind retry. Record the
run ID, trial key, workflow ID, task ID, selected and assigned worker, durable
execution ID, failure hash, exact boundary, and whether evidence exists. Apply
one minimal correction within authorization, run focused tests, deploy only
affected services if needed, and use fresh identities for a new submission.
Account for all provider **attempts**, including response-validation failures
and results lost before persistence; successful-caption count is not the call
budget. Do not extend an exhausted budget, alter frozen segmentation/ranking/
scoring, or discard unfavorable outcomes to force a pass.

Use the smallest relevant validation: docs-only -> contract/command review;
runner -> offline runner/verifier tests; a source-bound runtime change -> its
focused regressions plus affected canonical artifacts and deployment gates.
Do not rerun the full suite or a paid ten-case experiment for every small
patch. A ledger and durable checkpoints are the resume source of truth; lack
of session context is not permission to repeat already completed paid work.

## Minimal run ledger template

Copy this block into the experiment notes before starting:

```text
date_utc:
operator:
claim_class:
experiment_family_and_schema:
authorized_workflow_and_provider_attempt_budget:
runner_git_commit:
freezer_and_runtime_revision_by_component:
clean_source_path:
clean_source_archive_sha256:
batch_config_path_and_sha256:
frozen_plan_or_admission:
frozen_plan_or_admission_sha256:
catalog_and_gate_paths:
image_digest_by_node:
worker_alias:
observed_worker_id:
root_endpoint_identity:
flowmesh_sdk_root_node_worker_versions:
worker_node_cluster_namespace:
runtime_endpoint_map:
compose_project_files_and_desired_state_manifest:
runtime_epochs_and_restart_count_delta:
development_test_split_and_prior_exposure:
question_order_and_baseline_spec_sha256:
cache_episode_and_initial_state:
run_id:
output_dir:
attempt_journal_path:
native_exit_status:
workflow_ids:
verification_status:
checksum_status:
task_success_summary:
model_price_snapshot:
n6_usage_join_coverage:
n6_trace_attempt_count_and_provider_id_join:
semantic_usage_journal_error_count_before_after:
build_cost_coverage:
experiment_time_allocation:
unmeasured_cost_components:
known_limitations:
failed_attempts_and_recovery_links:
```

2026-09-24 — CONFIRMED, fresh holdout ordinal 13: the exact FlowMesh task
and durable route hash resolve to `N6AdapterError` wrapping
`N6 semantic executor failed: FullFlowRouteAdapterError`. The N6 diagnostic
inside that route's time interval records HTTP 400, provider code/type
`data_inspection_failed`. This is a provider refusal, not a source-binding,
network or scoring defect. Never alter the input to circumvent inspection,
retry the rejected request, label it incorrect, or replace the sample.
Keep the original failed batch immutable. A continuation may execute only
the still-unsubmitted suffix in frozen order, after ordinary readiness
gates; validate and copy the completed prefix, preserve a distinct terminal
failure observation and its diagnosis. Total submissions must remain the
original 48. Report answer accuracy and service availability separately;
missing usage on a rejected attempt is unknown, not zero.

The next frozen route (ordinal 14, R for the same question) independently
returned the same provider code in its own exact N6 log interval. It is
also terminal, with no retry/input alteration. Preserve each continuation
point so operator pauses can be subtracted from path-time allocation while
remaining visible in the full observed fleet-window cost.

The ledger may contain identities and non-secret metadata. It must never
contain API keys, bearer tokens, HMAC secrets, signed URLs, private prompts,
hidden labels, or environment dumps.

## Record failures as they happen

During every experiment, record **each new failure in this runbook before a
retry or a move to the next phase**. Do not wait until the experiment ends or
the final report is written. Add a short dated entry under the incident log
below with the first failing boundary, observable symptom, sanitized evidence
path, and status. If the root cause is not yet proven, mark it `INVESTIGATING`
and state what is unknown; do not present a guess as a fact.

Once the cause is proven, update the same entry to `CONFIRMED` or `RESOLVED`:
record the exact root cause, the minimal fix, and a preventive preflight or
test that would have stopped the failure before a workflow or paid model call.
If the same failure recurs, link and strengthen the existing preventive gate
instead of merely adding another anecdote. Automate the gate in the relevant
planner, verifier, or deployment preflight when safe; documentation alone is
not a substitute. At experiment handoff, check that **every** failed attempt
has an incident entry and that unresolved entries remain visibly unresolved.

Incident entries must not contain credentials, hidden labels, raw prompts,
answers, signed URLs, or unredacted container environments. A fix after a
submitted workflow still requires a fresh run identity.

### Incident log

2026-09-24 — RESOLVED: Windows `tar` could not read a mode-restricted public
plan and returned nonzero, but an initial PowerShell wrapper continued to SCP.
N1 refused the missing plan bind before the builder ran; no labels or model
calls were processed. Gate every native archive command on its immediate
exit status. Recover the public plan from the already checksummed, complete
input archive, extract only the named plan directory on N1, and verify before
building; do not loosen private-package permissions or re-freeze the plan.

2026-09-24 — RESOLVED: N5 h48 offline preparation completed and canonically
verified (4 videos, 36 windows), but packaging it with the login user failed
on mode-700 package directories owned by runtime uid 10001. Keep the partial
archive and export a new public-only archive with `sudo tar`; do not rebuild
the verified packages or relax their modes. Verify the exported archive and
inner package checksums. No provider request was made.

2026-09-24 — CONFIRMED BEFORE INFERENCE: a fresh cohort selected only from
question strata included two original MP4s above the deployed 7,000,000-byte
direct-video bound (9,375,758 and 7,865,377 bytes). Freeze media eligibility
from the pinned archive's uncompressed sizes before seeded selection, and
check both coordinator/N6 limits before any caption build. Preserve the
unexecuted original cohort; do not relax runtime limits, transcode raw inputs,
or use answer outcomes to replace videos. No provider request was spent.

2026-09-24 — INVESTIGATING: N6 read-only SSH readiness probe via Root timed
out during SSH banner exchange, before any remote command ran. Root, N1 and
N3 had passed their own read-only checks. No inference can be drawn about N6
application health from the banner timeout. Check the Root-to-N6 private SSH
path and retry the read-only health probe only; no paid readiness probe.

2026-09-24 — RESOLVED: fresh multi-question holdout preparation.
The first read-only SSH probe to the documented UpCloud Root failed locally
with `connect ... port 22: Permission denied` under network sandboxing,
before authentication. This is not evidence of a remote credential or service
failure. Use the approved network escalation for the same read-only probe;
do not rotate credentials or recreate containers. The approved read-only
probe succeeded; Root, N1 and N3 containers were healthy. No workflow or
provider request was made. Ledger:
`experiments/multiq_holdout_20260924/EXECUTION_LEDGER.md`.

2026-09-24 — RESOLVED BEFORE INFERENCE: exposure inventory must support
variable-length NExT-QA video IDs, not assume ten digits. V1 used a ten-digit
regex and recorded three public manifests denied by the sandbox. The
selector itself accepts variable-length IDs; its first selection exposed
the inventory assumption. Preserve v1 as an unexecuted attempt. V2 scans
8–12 digit IDs, fails on unreadable public sources and is frozen with
approved read access. Do not inspect private oracle files to fill this gap.
Only public object IDs are exported; no task outcomes drive the correction.

2026-09-24 — DOCUMENTATION REVIEW (no live experiment): the manual was checked
against `28b5ede` and the current canonical entry points. Corrected the archive
flag/import-root procedure, overly broad same-revision/zero-restart rules,
single-host versus multi-host origin assumptions, N1 secret-consumer boundary,
timeout configuration scope, stale tunnel detection, preflight guarantees and
the invoice-versus-list-price distinction. Added current cache-episode and N6
usage/trace accounting requirements. Historical rate snapshots and evidence
remain unchanged. This review did not revalidate the live cluster, update
FlowMesh, rotate credentials, submit workflows or call an LLM. Future source
changes must update the applicable section and the review revision, rather
than treating this date as a permanent readiness claim.

2026-09-24 — CONFIRMED: historical ten-route verifier version mismatch.
Offline regression on
`artifacts/minimum-real-retrieval-e1351cf/upcloud-minimum-real-retrieval-20260918t192435z`
hit `semantic input profile differs from its frozen route frontier` for D0/D4.
Those observations used sampled raw frames; the current raw profile uses
direct video. The receipt/checksums remain intact and the other eight route
evidence records pass current semantic evidence validation. No experiment was
submitted. Preserve the version boundary and use the historical verifier for
full archival verification; do not rewrite the old profile. Preventive test:
`HistoricalTenRouteEvidenceTests` checks all ten stored records and requires
the incompatible raw profiles to be rejected. This is not a new runtime fault.

Use this compact format for each new failure:

2026-09-24 — INVESTIGATING, fresh holdout route 13: after thirteen COMPLETE
routes, `fresh-multiq-holdout-20260924-v2|nextqa-val-2400715506-q3|I`
failed in 4525 ms with `flowmesh-workflow-terminal-failure`. All coordinator,
worker and dependency services remain healthy. Preserve the completed route
files and failure receipt under `h48-runner-20260924-v1/output/routes`.
Read the exact durable execution record before any repeat or continuation;
do not classify this infrastructure error as an incorrect model answer.

2026-09-24 — RESOLVED, runner-only dependency gate: source-bound inputs
validated, but using the route service image as a batch-client image raised
`FlowMeshDependencyError` before worker resolution. Service images need not
include the optional FlowMesh SDK. Inspect installed distributions first;
use a dedicated client runtime with the supported SDK rather than modifying
FlowMesh or rebuilding the healthy services. No workflow was submitted.
Existing dedicated runner image `sha256:9cc1202c88d14665ffdce135421092449d2172f01f4087ed5455bb88eab713b1`
already contains flowmesh-sdk 0.1.9, av 17.0.1 and Pillow 12.3.0. Use it
with the clean experiment source mounted read-only. No dependency install,
FlowMesh repository modification or service-image rebuild is necessary.

2026-09-24 — RESOLVED, before FlowMesh preflight: the new runner launcher
passed all seven valid/invalid auth boundaries and the N6 usage-journal gate,
then stopped at an assertion while acquiring its client configuration. No
workflow was submitted. Diagnose with explicit health booleans and missing
key names only; never print the captured container environment or keys.
Both services were healthy; only `FLOWMESH_API_KEY` was empty, which is valid
for this private Root's default configuration and is already supported by
`FlowMeshSettings.from_environment` (empty becomes None). Remove the helper's
extra all-values assertion, not any server authentication. The real Root
preflight remains mandatory and is the authority on access readiness.

2026-09-24 — CONFIRMED before holdout submissions: the interleaved DAG keeps
legacy fixed R/D/DC/I `order_index` values, while the new plan explicitly
rotates `route_slots` per question. A runner sorted only by DAG order would
silently ignore the frozen counterbalancing. Preserve admitted requests and
their identities; have the shared runner execute/verify them in the exact
frozen schedule order, with bijective question/arm coverage checks. No
outcomes or paid route calls existed when this discrepancy was discovered.

2026-09-24 — CONFIRMED, N7 helper import boundary: an unrelated pre-existing
`/tmp/re.py` shadowed Python's standard-library `re` when a helper was run
from `/tmp`. No deployment code executed. Invoke standalone remote helpers
with `python3 -I /tmp/helper.py` to exclude script/current-directory imports;
do not delete the unrelated file or treat this as a runtime service defect.

2026-09-24 — CONFIRMED, shell transport: PowerShell piping an LF script to
`ssh ... bash -s` appended a CR-only final line. All four public package
installations had already printed `PUBLIC_INPUT_CHECKSUMS_VERIFIED 169`,
then bash exited 1 at the trailing line. This was not a checksum failure.
Do not repeat installation or rebuild packages: inspect the completed state,
then proceed to rendering. Transfer LF scripts by scp and execute the remote
file (or use a binary subprocess stdin) instead of a PowerShell text pipe.

2026-09-24 — CONFIRMED, pre-deploy rendering only: combining both independently
generated N1 fragments in one Compose invocation failed with
`volumes.full-flow-n1-hidden-score-state.labels must be a mapping`. Existing
N1 services were created using one fragment at a time under one project.
Keep that selective-instantiation contract: render/start scorer and verifier
separately with the same explicitly named state volume. The failed rendering
stopped before any old container was stopped or new one was created.

2026-09-24 — RESOLVED (local invocation, no experiment submitted): bare
`python` resolved to the inaccessible WindowsApps execution alias. Use the
explicit verified `.venv-prep/Scripts/python.exe` interpreter for the fresh
holdout tooling. The replacement command passed all 13 focused accounting
and reusable-runner tests; do not infer any cloud or package failure from
the Windows process-start error.

```text
date_utc:
status: INVESTIGATING | CONFIRMED | RESOLVED
experiment_or_run_id:
first_failing_boundary:
observable_symptom_and_exit_status:
sanitized_evidence_path:
proven_root_cause_or_unknown:
minimal_fix:
preventive_gate_or_test:
verification_result:
```
