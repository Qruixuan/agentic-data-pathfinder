# Pathfinder Experiment Operations Runbook

This runbook records recurring failures seen while freezing, deploying, and
running Pathfinder experiments on Windows, `luyao3`, and the UpCloud
multi-host environment. It is intended to prevent repeated failed submissions,
container rebuilds, and LLM calls.

Use it for every smoke, canary, one-case run, and formal matrix run. A run is
not ready for submission until every applicable pre-submit gate below passes.

## Operating rule

Treat the experiment as six separate phases:

1. bind immutable source and inputs;
2. freeze and verify all derived artifacts;
3. stage and deploy only the affected services;
4. prove network, service, worker, and credential readiness without inference;
5. submit exactly one fresh run;
6. verify and preserve its evidence before interpreting the result.

Do not use a later phase to diagnose an earlier one. In particular, do not
submit a workflow to test source binding, container startup, SSH tunnelling,
worker registration, or endpoint authentication.

## Mandatory pre-submit checklist

Record every value in a small run ledger before submitting:

- [ ] Exact Git commit is recorded and is the same source revision used for
      freezing, verification, image builds, and the runner.
- [ ] A clean Git archive, not a mutable Windows working tree, is the Python
      import root for all source-bound commands.
- [ ] Source-bound SHA-256 values match the clean archive byte for byte.
- [ ] Every frozen directory exists and its verifier passes.
- [ ] Every `SHA256SUMS` file passes from inside its own directory.
- [ ] Planning succeeded before plan verification is attempted.
- [ ] The frozen timeout is greater than every operation lower bound plus a
      documented allowance for storage, compute, queueing, and control-plane
      overhead.
- [ ] Runtime endpoints use the actual host/private-network ports. Frozen
      inputs contain no stale loopback ports or single-host Docker DNS names.
- [ ] Only services affected by the change were rebuilt or recreated.
- [ ] All required containers are healthy and have restart count zero.
- [ ] Runtime source/admission/catalog/gate mounts are the intended immutable
      versions and are read-only where required.
- [ ] N3/N4 advertised origins exactly match the origins used by N7/N8.
- [ ] Dependency health succeeds from both route coordinators.
- [ ] Authentication boundary probes return an authenticated validation error
      such as HTTP 400, never HTTP 401 or 403.
- [ ] The SSH tunnel is listening locally before any FlowMesh command runs.
- [ ] FlowMesh preflight resolves exactly one current worker for the pinned
      alias. The worker ID is recorded as an observation, not a durable pin.
- [ ] The run ID, smoke ID, output directory, and operation identities have
      never been used before.
- [ ] The command's real exit status will be captured without a pipeline
      masking it.
- [ ] The expected claim class is written down: infrastructure conformance,
      semantic correctness, performance, or cost. Passing one class must not be
      reported as passing another.
- [ ] For a cost run, the N6 per-request usage journal is enabled and writable;
      the official model price snapshot (model, region, currency, effective
      date, input/cache/output rates) and experiment time boundaries are
      frozen before submission. The ledger specifies which cold-build and
      shared-VM costs will be measured, amortized, or left unknown.

If any item fails, stop before submission.

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
$BuildRoot = Join-Path $env:TEMP "pathfinder-source-$ShortCommit"
$Archive = "${BuildRoot}.tar"

if (Test-Path -LiteralPath $BuildRoot) {
    throw "Clean source already exists: $BuildRoot"
}

git archive --format=tar --output=$Archive $Commit
if ($LASTEXITCODE -ne 0) { throw "git archive failed" }

New-Item -ItemType Directory -Path $BuildRoot | Out-Null
tar -xf $Archive -C $BuildRoot
if ($LASTEXITCODE -ne 0) { throw "archive extraction failed" }
```

Capture the interpreter path before changing directory, then run source-bound
commands with both the clean source as `PYTHONPATH` and Python's `-P` safe-path
option:

```powershell
$Python = (Resolve-Path ".venv\Scripts\python.exe").Path
$PreviousLocation = Get-Location

try {
    Set-Location -LiteralPath $BuildRoot
    $env:PYTHONPATH = $BuildRoot
    & $Python -P -m pathfinder --help *> $null
    if ($LASTEXITCODE -ne 0) {
        throw "Pathfinder did not import from the clean source"
    }
} finally {
    Set-Location -LiteralPath $PreviousLocation
}
```

On Linux use the same rule:

```bash
export PF_SOURCE_COMMIT="$(git rev-parse HEAD)"
export PF_CLEAN_SOURCE="$(mktemp -d)/pathfinder-${PF_SOURCE_COMMIT:0:12}"
mkdir -p "$PF_CLEAN_SOURCE"
git archive "$PF_SOURCE_COMMIT" | tar -x -C "$PF_CLEAN_SOURCE"
cd "$PF_CLEAN_SOURCE"
PYTHONPATH="$PF_CLEAN_SOURCE" python -P -m pathfinder --help >/dev/null
```

The temporary directory can be removed after all source-bound artifacts and
evidence are safely copied elsewhere. Never overwrite a clean extraction and
continue using it under the same name.

Inspect line-ending drift before freezing:

```powershell
git ls-files --eol -- `
  pathfinder/simulator/*.py `
  pathfinder/cli_commands/*.py
```

`i/lf w/crlf` is sufficient reason not to use the current working-tree file as
a source-bound input. Do not rely on `core.autocrlf` settings being identical
across machines.

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

Adjust the field and module to the artifact being checked. Do not substitute a
Git blob ID for a recorded SHA-256; they are different digest schemes.

## 2. Freeze, verify, and change-impact discipline

### Never verify a failed plan

A failed planner normally creates no output directory. Gate every verifier on
both the planner's exit status and directory existence:

```powershell
& $Python -P -m pathfinder <plan-command> <arguments>
$PlanStatus = $LASTEXITCODE

if ($PlanStatus -ne 0 -or -not (Test-Path -LiteralPath $PlanDir)) {
    throw "Planning failed; verification and submission are prohibited"
}

& $Python -P -m pathfinder <verify-command> --plan-dir $PlanDir
if ($LASTEXITCODE -ne 0) { throw "Plan verification failed" }
```

On Linux, verify checksums from inside each artifact directory:

```bash
(
  cd "$PF_ARTIFACT_DIR" || exit 1
  sha256sum -c SHA256SUMS
)
```

### Regenerate only what the source binding requires

Use the canonical verifier to determine whether a change is admission-bound.
Do not infer this from the filename or from how small the patch looks.

| Changed input | Minimum expected impact |
| --- | --- |
| Semantic input/profile module | Semantic matrix, admission, artifact preflight, promotion, index-query catalog, one-case package, and gates that bind them; deploy N6/N7/N8 as applicable |
| Route adapter or semantic runtime | Regenerate any admission/inventory that records its source digest; deploy N7/N8 |
| N6 adapter | Regenerate its source-bound admission/inventory; deploy N6 and any bound route runtime |
| N3 raw package/catalog | Re-freeze N3 and all downstream bindings that commit to its content identities |
| N4 derived generation | Re-freeze publication snapshot, N4 package, artifact bindings, and the serve gate |
| CLI-only, non-bound orchestration code | Run the canonical verifier; do not regenerate admission automatically if it proves the source set is unchanged |
| Documentation only | No runtime artifact regeneration |

Never edit a frozen artifact to make verification pass. Generate a new
timestamped directory and retain the old one as evidence.

### Timeout gate

The API task timeout is a frozen execution parameter. It must exceed the
largest derived operation floor. For example, the slow D4 transfer has a
known lower bound of 576.03 seconds, so 300 seconds is invalid; the established
formal matrix used 900 seconds. A lower bound excludes storage, compute,
queueing, and control-plane overhead and is not an execution estimate.

There is deliberately no runtime timeout override. If the frozen timeout is
wrong, create a new plan.

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

Inside a container, `127.0.0.1` refers to that container. N3 and N4 Data Agents
must advertise the same internal DNS origins N7/N8 use, for example:

```text
http://pathfinder-full-flow-n3-raw-data-agent:8780
http://pathfinder-full-flow-n4-derived-data-agent:8780
```

Use host loopback URLs only for host-side operator access.

### Selective deployment

Use independently renderable per-service Compose fragments. A former unified
overlay required all 117 variables even when starting one service, causing
unrelated credential and configuration failures.

Build a shared image once, then use `up --no-build` for services that reference
the same tag. Parallel Compose builds exporting the same tag have raced with
`image already exists`.

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

Different hosts may produce different image digests because their base image
resolution differs. Record both the image digest and the embedded source
commit/module digests. Do not require cross-host image equality unless images
come from one locked registry manifest.

### Permissions and hidden labels

Do not solve hidden-label read failures with world-readable permissions. Use a
named-user ACL for runtime UID 10001, traverse-only access on private parent
directories, read-only access on required files, and read-only container
mounts. Verify that an unrelated UID cannot read the label file and that UID
10001 cannot write it.

### Credential families are different

Do not apply one token-precedence rule to every service:

- N3/N4 Data Agents select node-specific credentials first, with a shared
  value only as a compatibility fallback.
- N2/N7/N8 regular indexes use their frozen shared index credential contract.
- N7/N8 persistent caches use their frozen shared cache credential contract.
- W4 candidate caches keep their dedicated credential.
- N1 scoring credentials remain isolated to N1.

An authentication probe should use the deployed client's own resolver and a
valid endpoint path with an intentionally invalid body. HTTP 400 proves that
authentication passed and validation rejected the body. HTTP 404 or 501 says
nothing about authentication because routing may occur first.

Never print, hash, copy into evidence, or include credential values on a
command line. Inspect key presence and equality only through booleans or
in-process comparison.

## 5. FlowMesh and SSH readiness

### Tunnel readiness on Windows

Create the tunnel with one argument string; an argument array has previously
failed to create a listener under `Start-Process`. Keep the window hidden and
test the listener before calling FlowMesh:

```powershell
$LocalPort = 18010
$RemotePort = 8010
$RootAlias = "pathfinder-upcloud-root"
$Arguments = "-N -L ${LocalPort}:127.0.0.1:${RemotePort} $RootAlias"

$Tunnel = Start-Process `
    -FilePath "ssh" `
    -ArgumentList $Arguments `
    -PassThru `
    -WindowStyle Hidden

Start-Sleep -Seconds 2
$Listener = Get-NetTCPConnection `
    -State Listen `
    -LocalPort $LocalPort `
    -ErrorAction SilentlyContinue

if (-not $Listener) {
    if (-not $Tunnel.HasExited) { Stop-Process -Id $Tunnel.Id }
    throw "SSH tunnel is not listening; do not submit"
}
```

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

Do not reuse a run ID, request ID, output directory, or operation key after a
partial or failed execution. Durable idempotency correctly rejects reuse as
`container operation was replayed`.

Do not pipe the runner directly to `Tee-Object` and then read
`$LASTEXITCODE`; a later pipeline command can mask the runner's status. Capture
output first, save the native status immediately, and display it afterward:

```powershell
& $Python -P -m pathfinder <run-command> <arguments> *> $ConsoleLog
$RunStatus = $LASTEXITCODE
Get-Content -LiteralPath $ConsoleLog

if ($RunStatus -ne 0) {
    throw "Experiment failed with native status $RunStatus"
}
```

On Bash with `tee`, enable `set -o pipefail` and capture
`${PIPESTATUS[0]}` immediately. Prefer redirection when possible.

No output directory after a failure means there is nothing to checksum or
verify. Diagnose the console error and durable runtime state instead.

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
- **Cost evidence:** actual priced resource use was measured.

A route can be infrastructure-complete and semantically wrong. For example,
the minimum-real causal ten-case run completed all paths but achieved 9/10
task success. The D6 answer error is not an infrastructure failure. Likewise,
the earlier placeholder-semantic run was valid conformance evidence even
though all `task_success` values were false.

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
complete N6 total. The N6 journal records numeric usage and request IDs; its
presence does not by itself recover older runs without matching identities.

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
state so replay can charge build costs once, at the first use. A historical
receipt with missing build CPU time, publication time, provider cache units,
or invoice fields is **partially measured**: mark that component `unknown`,
not zero, and do not silently substitute a new run's timing. Before the next
experiment, make raw response/usage and timing receipts durable after every
paid call, including failed calls, so a later interruption does not lose cost
evidence. Keep raw prompts, answers, credentials, and hidden labels out of
public cost artifacts.

## Recurring failure catalogue

| Symptom | Usual cause | Prevention | Correct response |
| --- | --- | --- | --- |
| `source ... does not match`, adapter inventory mismatch | Windows CRLF bytes or wrong import root | Clean Git archive, clean working directory, `PYTHONPATH`, and `python -P`; compare SHA-256 before deploy | Regenerate from clean source; do not edit frozen JSON |
| Route HTTP 401 although a token exists | Wrong credential precedence or wrong service-family token | Probe with deployed resolver and valid route; preserve family-specific contracts | Fix selection/configuration; do not rotate unrelated credentials |
| Route HTTP 401 with signed semantic body | Canonical JSON/HMAC mismatch such as `0.0` versus `0` across Pydantic/wire serialization | Freeze integral values as integers; compare sender and receiver canonical byte length/digest before submit | Fix canonical representation and focused tests; create a fresh run |
| Data Agent range client requires 206 but receives 200 | Full-span `Range` request treated as a non-partial interval | Preserve whether a Range header was requested; test full-span range | Fix server range semantics; keep client fail-closed |
| `container operation was replayed` | Reused run/request/operation identity | Timestamp plus nonce; new output directory every time | Do not bypass idempotency; start a fresh run |
| Plan verifier says directory missing | Planner already failed | Gate verifier on planner status and directory existence | Fix planner input first |
| Timeout rejected at freeze time | Frozen timeout below derived network floor | Inspect maximum lower bound before freezing | Freeze a new plan with adequate timeout |
| Connection refused on 29xxx | Frozen port does not match actual 19xxx runtime | Probe ports before freeze; bind actual endpoints | New plan or authorized forwarder; no container churn |
| Data Agent security/origin error | Agent advertises host loopback while route uses Docker DNS/private DNS | Compare configured and advertised normalized origins from N7 and N8 | Recreate only N3/N4 with correct advertised origins |
| Container healthy but dependency name does not resolve | DNS label exceeds 63 characters or alias missing | Validate every generated service name and alias offline | Regenerate DNS-safe overlay |
| Single-service Compose launch asks for unrelated secrets | Whole overlay interpolation requires all variables | Use selective per-service fragments | Regenerate overlay; never fabricate credentials |
| Parallel build says image already exists | Multiple services export the same image tag | Build shared image once | Continue with verified image and `up --no-build` |
| Preflight finds no worker; `worker up` says already exists | Stale lifecycle state or missing current registration | Check alias registry and container status separately | Repair worker registration; do not submit unpinned |
| Workflow fails before any container request | Root/identity-provider/dispatch issue | Check Root task state and route access logs | Preserve IDs and wait/escalate; do not rebuild N1-N8 |
| Result upload times out | Worker-to-Root result path issue | Verify callback/result endpoint reachability | Repair control plane; do not replay the same operation |
| Error printed but reported status is zero | Pipeline masked native exit code | Redirect output or capture `PIPESTATUS[0]` | Treat the run as failed and inspect durable state |
| PowerShell SSH process exists but no local listener | `Start-Process -ArgumentList` quoting/array issue | Use one argument string and verify listening port | Stop failed process and recreate tunnel before preflight |
| Files apparently missing after transfer | Windows MAX_PATH, ACL, or partial recursive copy | Short staging root, archive transfer, checksum and count gates | Restage from the verified archive |
| N4 current generation loses earlier objects | Atomic publication replaces the snapshot rather than merging ad hoc runs | Build and bind one complete generation such as the exact-six store | Re-publish a complete generation and re-freeze its gate |
| Evidence says model input does not bind routed artifacts | Verifier and route disagree about the actual inference frontier | Bind exactly the artifacts used for inference while preserving the full routed set | Fix evidence semantics; do not weaken equality checks blindly |
| Transport cannot bind a runtime value | New value type lacks an explicit canonical handoff | Add a fail-closed, commitment-complete transport branch and focused tests | Deploy only affected route services and use a new run |
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
one minimal correction, run focused tests, deploy only affected services, and
use one new run identity.

## Minimal run ledger template

Copy this block into the experiment notes before starting:

```text
date_utc:
operator:
claim_class:
git_commit:
clean_source_path:
clean_source_archive_sha256:
frozen_plan_or_admission:
frozen_plan_or_admission_sha256:
catalog_and_gate_paths:
image_digest_by_node:
worker_alias:
observed_worker_id:
root_endpoint_identity:
runtime_endpoint_map:
run_id:
output_dir:
native_exit_status:
workflow_ids:
verification_status:
checksum_status:
task_success_summary:
model_price_snapshot:
n6_usage_join_coverage:
build_cost_coverage:
experiment_time_allocation:
unmeasured_cost_components:
known_limitations:
```

The ledger may contain identities and non-secret metadata. It must never
contain API keys, bearer tokens, HMAC secrets, signed URLs, private prompts,
hidden labels, or environment dumps.

## Before changing this runbook

When a new failure occurs, add it only after the exact failing boundary and
root cause are proven. Record a preventive gate, not merely the repair that
worked once. If the prevention can be automated safely, add it to the relevant
planner/verifier or deployment preflight; this document is the fallback, not a
substitute for executable checks.
