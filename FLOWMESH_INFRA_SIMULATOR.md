# FlowMesh Physical-Layout and Infrastructure Simulator

Status: **offline MVP; development evidence only**

## Purpose

This package is a deterministic, single-process discrete-event simulator for
FlowMesh-shaped workloads. It compares physical data layouts, storage and
network paths, resource queues, indexes, and bounded caches without starting a
FlowMesh Root Server, worker, Data Agent, Docker container, or LLM.

The simulator is a design-space and counterfactual tool. It does not replace
container emulation or real multi-node validation, and its outputs are always
labelled ineligible for scientific claims.

## Current reference scenario

`configs/flowmesh_infra_simulator_4x8_smoke.json` declares:

- eight logical nodes (`N1`–`N8`);
- four FlowMesh-like workload classes (`W1`–`W4`);
- eight physical designs (`D0`–`D7`);
- two repetitions, producing 64 deterministic trials;
- cold HDD, warm NVMe, local NVMe, index, GPU, core-network, and edge-network
  resource queues;
- cache hit/miss branches with LRU insertion and eviction;
- frozen synthetic quality outcomes, separate from physical simulation; and
- a clearly labelled provisional rate card and calibration profile.

The configuration is a reference scenario, not simulator source code. Other
node counts, tasks, layouts, and DAGs can use the same engine.

## Run locally

```powershell
python -m pathfinder simulate-flowmesh-infra `
  --scenario configs/flowmesh_infra_simulator_4x8_smoke.json `
  --output-dir outputs/flowmesh-infra-4x8-smoke-v1
```

Verify the immutable output:

```powershell
python -m pathfinder verify-flowmesh-infra-simulation `
  --output-dir outputs/flowmesh-infra-4x8-smoke-v1
```

An existing output directory is refused rather than overwritten. Use a new
run directory for a changed scenario or a new attempt.

## Execution model

Each configured FlowMesh-like task becomes a deterministic trial with stable
workflow, task, session, and trial identifiers. Its selected design resolves
one generic operation DAG. Supported operations are:

- control-plane work;
- index queries;
- storage reads;
- network transfers;
- CPU/GPU compute;
- cache lookup, local read, insertion, and eviction; and
- dependency barriers.

Admission has two deliberately separate layers. First, whole trials enter a
scenario-level FIFO queue ordered by `(arrival_time_ms, order_index)` and no
more than the frozen `trial_admission_slots` may be active. A slot is released
only after every operation in that trial completes. Second, resources and
links retain their independent slot queues. An operation begins only after its
dependencies and one resource/link slot are available. Its virtual duration comes
from configured base latency, bytes/throughput, optional deterministic jitter,
and operation service time. No wall-clock sleep or large artifact transfer is
performed.

Both the discrete-event and container backends report
`trial_admission_queue_ms`, `active_execution_latency_ms`, and `latency_ms`.
The last value is measured from the planned arrival and must equal the first
two values added together. Resource `queue_time_ms` excludes the whole-trial
admission wait, so runner overload is not confused with disk, network, or
compute contention.

Cache state is isolated by design and repetition. Conditional operations make
remote misses and local hits explicit; skipped branches remain in the event
trace with `executed=false`.

## Output contract

Each atomic run publishes:

| File | Meaning |
| --- | --- |
| `simulator_plan.json` | content-bound deterministic trial plan |
| `events.jsonl` | operation-level virtual event trace |
| `canonical_records.jsonl` | one complete simulated observation per trial |
| `summary.json` | per-design latency, cost, quality, bytes and cache aggregates |
| `run_manifest.json` | provenance and output digests |
| `SHA256SUMS` | integrity binding for every other output file |

Records explicitly state:

```text
simulated = true
flowmesh_deployed = false
llm_called = false
eligible_for_scientific_claims = false
```

The engine does not infer quality from a design name. Every workload must
supply an outcome for every design and identify its provenance. The smoke uses
synthetic outcomes only to exercise the full pipeline.

## Cost semantics

The simulator first records a resource vector: service and queue time by
resource class, logical/physical/network bytes, cache events, and operation
counts. Cost is then derived through a separately identified rate card.

The bundled rate card is deliberately named
`provisional-local-simulator-units-v1`. Its numbers are not prices or measured
hardware performance. Later scenarios must replace the development profile
with distributions fitted from storage/network/model calibration or real
FlowMesh traces while retaining the same scenario schema.

## Evidence-backed partial calibration

The repository includes a conservative calibration profile for the existing
36-workload PathfinderBench package. It calibrates only parameters directly
identified by those files: raw-video, sampled-frame-bundle, and digest sizes
for the descriptive, temporal, and causal workload classes. Within each
stratum it selects the object whose raw-video size is nearest the stratum
median. Every selected byte count is checked against its frozen source hash,
generation manifest, and frame-bundle binding before publication.

```powershell
python -m pathfinder calibrate-flowmesh-infra-scenario `
  --scenario configs/flowmesh_infra_simulator_4x8_smoke.json `
  --calibration-config configs/flowmesh_infra_simulator_4x8_existing_evidence_calibration.json `
  --workload-manifest D:/pathfinder-data/pathfinderbench-restricted-pilot-v0.1/cohort-v0.1/workloads.json `
  --representation-manifest D:/pathfinder-data/pathfinderbench-restricted-pilot-v0.1/representations-v0.1/generation-manifest.json `
  --frame-bundle-root D:/pathfinder-data/pathfinderbench-restricted-pilot-v0.1/frame-bundles-v0.1 `
  --video-root D:/pathfinder-data/pathfinderbench-restricted-pilot-v0.1/videos `
  --output-dir outputs/flowmesh-infra-4x8-existing-evidence-calibration-v1

python -m pathfinder verify-flowmesh-infra-calibration `
  --output-dir outputs/flowmesh-infra-4x8-existing-evidence-calibration-v1
```

The calibrated scenario can then be passed directly to
`simulate-flowmesh-infra`. The calibration report records the selected object,
old and new sizes, source digests, and all parameters deliberately left
unchanged.

This is intentionally a **partial** calibration. The original 36-workload
package has no independently designed retrieval cohort, so the first profile
retains a provisional W4 object. The development-only W4 fixture below can
instead supply a post-hoc W4 byte-size representative. Neither profile can
identify disk throughput, network bandwidth/latency, GPU/CPU service rates,
quality outcomes, or monetary rates. Those parameters remain labelled
provisional until supplied by `fio`, `iperf3`, model-timing evidence, or frozen
real FlowMesh traces. A partially calibrated run remains ineligible for
scientific claims.

## W4 retrieval development path

The W4 tool builds a real deterministic BM25 index over verified frozen
multimodal digests. Relevance labels remain an explicit input: the tool never
asks an LLM to infer ground truth. It validates source-object split isolation,
candidate-ID leakage, digest hashes, answer/ranking bindings, and immutable
outputs.

The repository contains an eight-query, 36-candidate development fixture:

```powershell
python -m pathfinder build-simulator-retrieval-cohort `
  --config configs/flowmesh_simulator_w4_existing_36_development.json `
  --representation-manifest D:/pathfinder-data/pathfinderbench-restricted-pilot-v0.1/representations-v0.1/generation-manifest.json `
  --output-dir outputs/flowmesh-simulator-w4-existing-36-development-v1

python -m pathfinder verify-simulator-retrieval-cohort `
  --output-dir outputs/flowmesh-simulator-w4-existing-36-development-v1
```

It publishes the cohort, term-frequency index, full rankings, per-query
Recall@k/Hit@k/reciprocal rank, aggregate MRR, manifest, and checksums. If an
answer-observation manifest is supplied, the evaluator additionally computes
answer accuracy and joint retrieval-and-answer success after verifying that
each answer used the exact recorded top-k ranking.

The bundled queries were drafted from already inspected digests and are
labelled `ai-drafted-requires-operator-verification`. Their observed perfect
Recall@1 only demonstrates pipeline conformance; they are too easy and too
post-hoc for a retrieval-quality claim. A human-reviewed, disjoint cohort with
hard negatives is required before confirmation.

The W4 output can also calibrate the retrieval object's byte vector:

```powershell
python -m pathfinder calibrate-flowmesh-infra-scenario `
  --scenario configs/flowmesh_infra_simulator_4x8_smoke.json `
  --calibration-config configs/flowmesh_infra_simulator_4x8_existing_evidence_with_w4_calibration.json `
  --workload-manifest D:/pathfinder-data/pathfinderbench-restricted-pilot-v0.1/cohort-v0.1/workloads.json `
  --representation-manifest D:/pathfinder-data/pathfinderbench-restricted-pilot-v0.1/representations-v0.1/generation-manifest.json `
  --frame-bundle-root D:/pathfinder-data/pathfinderbench-restricted-pilot-v0.1/frame-bundles-v0.1 `
  --video-root D:/pathfinder-data/pathfinderbench-restricted-pilot-v0.1/videos `
  --retrieval-output-dir outputs/flowmesh-simulator-w4-existing-36-development-v1 `
  --output-dir outputs/flowmesh-infra-4x8-existing-evidence-with-w4-calibration-v1
```

This calibrates all twelve W1-W4 representation-size parameters. It does not
copy the lexical index result into synthetic task-success outcomes.

## Unified infrastructure evidence and fitting

`build-flowmesh-infra-evidence` normalizes four source classes into one
content-bound bundle:

- fio JSON: read/write latency and byte throughput;
- iperf3 JSON plus explicitly supplied ping RTT samples: bandwidth and RTT;
- model-timing JSONL: service time for one named template operation; and
- a verified FlowMesh trace import: route-level end-to-end validation only.

Start from `configs/flowmesh_infra_evidence_spec.template.json`; keep every
referenced path relative to the spec directory. A model-timing row has this
minimal contract:

```json
{
  "schema_version": "pathfinder.model-timing-observation/v1alpha1",
  "observation_id": "timing-0001",
  "node_id": "N6",
  "resource_id": "N6.gpu",
  "template_id": "remote-digest",
  "op_id": "infer",
  "service_time_ms": 52.1,
  "outcome_type": "completed",
  "telemetry_complete": true,
  "real_measurement": true,
  "credentials_recorded": false
}
```

Build, verify, and fit the evidence:

```powershell
python -m pathfinder build-flowmesh-infra-evidence `
  --spec path/to/evidence-spec.json `
  --output-dir outputs/infra-evidence-v1

python -m pathfinder verify-flowmesh-infra-evidence `
  --output-dir outputs/infra-evidence-v1

python -m pathfinder fit-flowmesh-infra-scenario `
  --scenario path/to/object-size-calibrated-scenario.json `
  --evidence-dir outputs/infra-evidence-v1 `
  --output-scenario-id measured-infra-partial-v1 `
  --output-dir outputs/measured-infra-fit-v1

python -m pathfinder verify-flowmesh-infra-fit `
  --output-dir outputs/measured-infra-fit-v1
```

The fitter uses sample medians for point parameters and retains p95 evidence
in its report. It changes only named storage latency/throughput, link
RTT/bandwidth, and compute-operation service time. Slots, prices, cache state,
arrivals, quality, and unmeasured resources remain provisional. FlowMesh
end-to-end latency is never decomposed into unobserved internal components.

## Container-emulation handoff contract

The simulator scenario is now compiled once into a backend-neutral plan rather
than being independently interpreted by each execution environment. Every
trial operation records its dependencies, condition, object, representation,
exact logical byte count, resource, cache, link, and source/destination nodes.
Configured service time is retained only as a simulation hint; measured
backends are explicitly forbidden from treating it as an instruction to
sleep. Synthetic `task_success_by_design` values are not copied into the
portable plan.

Build and verify the shared plan:

```powershell
python -m pathfinder build-portable-execution-plan `
  --scenario configs/flowmesh_infra_simulator_4x8_smoke.json `
  --output-dir outputs/flowmesh-infra-portable-plan-v1

python -m pathfinder verify-portable-execution-plan `
  --output-dir outputs/flowmesh-infra-portable-plan-v1
```

The development container contract then requires exact coverage of all eight
nodes, twelve resources, two caches, eight directed links, nine operation
kinds, and four task types. Missing, extra, or incorrectly located bindings
are rejected before an output directory is published.

```powershell
python -m pathfinder plan-container-simulation `
  --scenario configs/flowmesh_infra_simulator_4x8_smoke.json `
  --portable-plan-dir outputs/flowmesh-infra-portable-plan-v1 `
  --container-spec configs/flowmesh_infra_container_4x8_contract.json `
  --output-dir outputs/flowmesh-infra-container-contract-plan-v4

python -m pathfinder verify-container-simulation-plan `
  --output-dir outputs/flowmesh-infra-container-contract-plan-v4
```

This command is non-launching. The current development contract deliberately
returns `CONTRACT_READY_LAUNCH_UNVERIFIED`. It lists two launch-time blockers:
the development container image still needs an immutable digest, and the host
still needs a live Docker Compose preflight. The local-smoke contract uses an
application-level byte-rate/RTT shaper and therefore does not require
`NET_ADMIN`; a later Linux `tc` profile must declare and preflight that
capability explicitly. It also warns that the
first payload mode preserves byte sizes but not modality semantics, so it may
support infrastructure conformance but not task-quality claims.

### Local eight-container package

The local package contains eight bounded node services. Storage operations use
real files in per-node named volumes, and network operations send real bytes
over HTTP before applying the configured application-level rate/RTT floor.
Payloads are deterministic size-preserving fixtures. No LLM, FlowMesh Root,
worker, or benchmark video is required, and semantic task quality remains
disabled.

Generate and verify the Compose project without contacting Docker:

```powershell
python -m pathfinder build-local-container-compose `
  --container-plan-dir outputs/flowmesh-infra-container-contract-plan-v4 `
  --output-dir outputs/flowmesh-infra-local-compose-v3

python -m pathfinder verify-local-container-compose `
  --output-dir outputs/flowmesh-infra-local-compose-v3

python -m pathfinder preflight-local-container-host
```

Image pinning is enforced by the generated Compose package. For a node with an
`image_digest`, the service uses `image_ref@sha256:...` and contains no
`build:` block, so starting the package cannot silently rebuild a mutable tag.
An unpinned development node retains its build block and is reported as
unpinned in `local_container_manifest.json`.

`preflight-local-container-host` is read-only. `BLOCKED` means that the Docker
CLI, Docker engine, or Compose v2 is unavailable; it does not install anything
or start a service. An unpinned `compose.yaml` requires
`PATHFINDER_REPO_ROOT` to be set to the repository root before a later explicit
launch; a fully digest-pinned package has no build context and does not require
that variable. The generated project is labelled `GENERATED_NOT_LAUNCHED`, and
generation never authorizes a run.

After an operator explicitly starts the generated Compose project and all
eight health checks pass, the first execution should be a single small frozen
trial. The serial driver does not start or stop services:

```powershell
python -m pathfinder run-local-container-simulation `
  --compose-package-dir outputs/flowmesh-infra-local-compose-v3 `
  --portable-plan-dir outputs/flowmesh-infra-portable-plan-v1 `
  --trial-key "flowmesh-infra-4x8-local-smoke-v1|smoke-descriptive|D2|r0000" `
  --output-dir outputs/flowmesh-infra-container-execution-smoke-v1

python -m pathfinder verify-local-container-simulation `
  --output-dir outputs/flowmesh-infra-container-execution-smoke-v1
```

This produces `PARTIAL_SMOKE` infrastructure records. It measures real file
reads, exact-byte HTTP transfer, and host/node timing. It intentionally does
not report task success, packet-level shaping, or monetary cost.

After the serial smoke passes, a bounded concurrent scan can enforce the
resource and link slot counts already frozen in the portable plan:

```powershell
python -m pathfinder run-local-container-simulation `
  --compose-package-dir outputs/flowmesh-infra-local-compose-v3 `
  --portable-plan-dir outputs/flowmesh-infra-portable-plan-v1 `
  --trial-limit 8 `
  --max-concurrency 4 `
  --output-dir outputs/flowmesh-infra-container-concurrent-smoke-v1
```

The portable plan freezes the global FIFO trial-admission width. Omitting
`--max-concurrency` uses that width; a complete run rejects any different
value, while a smoke subset may use fewer slots but never more. Concurrent
output records both whole-trial admission wait and the real wait between
dependency readiness and admission to a frozen resource/link slot. The latter
is an orchestrator resource queue: node-internal, operating-system, and
Docker-network queues are not separately decomposed. Cache metadata uses one
explicit resource-admission slot because the portable cache contract specifies
byte capacity but not parallel slots.
Preregistered parity tolerances are still required before a full backend-parity
study.

Container execution is auditably resumable. The runner creates
`container_run_checkpoint.json` before the first operation, then adds one
digest-bound row to `trial_checkpoint.jsonl` only after a whole trial and all
of its operation events are complete. Each new complete ledger prefix is
written, flushed with `fsync`, and atomically replaces the previous prefix, so
an interrupted write cannot expose half a JSON row. Re-running the exact
same command with the same output directory verifies and reuses those complete
trials. An interrupted partial trial is absent from the ledger and is executed
again; it is never promoted into the final infrastructure records.

Each node exposes a per-process runtime epoch and retains the original result
for every operation key. An exact retry within that epoch returns the stored
result without repeating cache mutations. Resume fails closed if the portable
plan, endpoint binding, selected trials, concurrency, timeout, or any runtime
epoch changed. Therefore a container restart requires a new output directory;
the current protocol deliberately does not claim crash-safe node idempotency
across process restarts. Once `SHA256SUMS` exists, the completed output can be
verified and returned again without contacting live nodes.

On Windows, the atomic checkpoint replacement retries only transient
access-denied and sharing-violation errors with a bounded backoff. Persistent
locks and unrelated I/O errors still fail, leaving only the previously durable
ledger prefix for an auditable resume.

### Formal 4x8 execution through FlowMesh

The formal FlowMesh runner consumes all three frozen inputs together: the
64-trial matrix, the serial execution profile, and the coordinator dry-run.
It does not start or stop the eight container nodes or the FlowMesh worker.
Those services must already be healthy, and the worker alias in the command
must match the alias frozen into the matrix.

Bind the four paths to the immutable artifacts produced by the matrix,
profile, and coordinator planning commands. Use a new run directory; the
runner creates it and refuses unrelated pre-existing contents.

```bash
export PF_MATRIX_PLAN=/path/to/matrix-plan
export PF_FORMAL_PROFILE=/path/to/formal-profile
export PF_COORDINATOR_PLAN=/path/to/coordinator-dry-run
export PF_MATRIX_RUN=/path/to/new/formal-matrix-run
export PF_FLOWMESH_WORKER_ALIAS=the-alias-frozen-in-the-matrix
```

These directories are produced and verified in this order:

1. `plan-flowmesh-container-matrix`, then
   `verify-flowmesh-container-matrix-plan`;
2. `freeze-flowmesh-container-formal-execution-profile`, then
   `verify-flowmesh-container-formal-execution-profile`; and
3. `plan-flowmesh-container-matrix-coordinator-dry-run`, then
   `verify-flowmesh-container-matrix-coordinator-dry-run`.

Use each command's `--help` output for the complete arguments. The profile
must bind the matrix plan, and the coordinator must bind both the matrix and
profile; the formal runner re-verifies all three links before submission.

Smoke runs use the same operation keys as the formal matrix. Because a node
correctly rejects a repeated key as an idempotent replay, recreate the eight
simulator containers once after the last smoke and before starting the formal
run. Verify that all eight new runtime epochs are distinct and stable before
submission. Do not recreate a node after the formal run has begun: a changed
epoch makes the existing run directory ineligible for resume.

```bash
PYTHONPATH=. python -m pathfinder run-flowmesh-container-matrix \
  --matrix-plan-dir "$PF_MATRIX_PLAN" \
  --formal-execution-profile-dir "$PF_FORMAL_PROFILE" \
  --coordinator-plan-dir "$PF_COORDINATOR_PLAN" \
  --output-dir "$PF_MATRIX_RUN" \
  --run-id "flowmesh-infra-4x8-formal-run-v1" \
  --worker-alias "$PF_FLOWMESH_WORKER_ALIAS" \
  --flowmesh-base-url "$FLOWMESH_BASE_URL" \
  --poll-interval 2
```

The per-operation API timeout is the value already frozen in the matrix; the
run command deliberately has no override that could change that envelope.

The first profile executes trial wrappers globally in their frozen order with
one wrapper active at a time. An unconditional trial is one arbitrary DAG.
A D3 or D7 trial is two workflows: phase A observes the cache, and phase B is
submitted only when that literal observation matches the frozen branch. A
successful run of the current matrix therefore has exactly 80 workflows, 472
executed operations, and 28 explicitly inactive branch operations. The final
operation ledger must still account for all 500 frozen operations; an
inactive operation has no invented zero latency or cost.

The run directory is a durable checkpoint. Re-running the exact command with
the same `run-id` and output directory resumes only a verified completed
prefix. A submission intent is persisted before every side effect and the
returned workflow and task IDs are persisted immediately afterward. If a
process stops in the narrow interval where submission may have occurred but
the IDs were not made durable, resume fails closed instead of silently
submitting a duplicate. The first ordinary failure also stops the matrix;
later trials are never skipped over.

One operating-system advisory lock covers the whole invocation. A second
process targeting the same output directory fails before reading or changing
its checkpoint. The sibling lock file is intentionally retained after exit;
the operating system releases the lock itself if the runner crashes.

After completion, verification is offline. Supplying all three source
directories additionally rechecks the run-to-input bindings; supplying only a
subset is refused.

```bash
PYTHONPATH=. python -m pathfinder verify-flowmesh-container-matrix-run \
  --run-dir "$PF_MATRIX_RUN" \
  --matrix-plan-dir "$PF_MATRIX_PLAN" \
  --formal-execution-profile-dir "$PF_FORMAL_PROFILE" \
  --coordinator-plan-dir "$PF_COORDINATOR_PLAN"
```

This runner produces infrastructure-conformance evidence only. It preserves
container service time, exact logical/physical bytes, cache outcomes, worker
identity, and runtime epochs. It does not infer FlowMesh queue time or
end-to-end latency, call an LLM, evaluate semantic answer quality, fit cost
parameters, or make a physical-money or scientific-performance claim.

The portable metric contract freezes the comparison fields but leaves parity
thresholds unset. Once a container backend emits a complete canonical record
ledger, descriptive comparison uses the exact same trial identities:

```powershell
python -m pathfinder evaluate-backend-parity `
  --portable-plan-dir outputs/flowmesh-infra-portable-plan-v1 `
  --reference-records path/to/discrete-event/canonical_records.jsonl `
  --candidate-records path/to/container/canonical_records.jsonl `
  --reference-label discrete-event `
  --candidate-label container `
  --comparison-scope infrastructure-only `
  --output-dir outputs/backend-parity-v1

python -m pathfinder verify-backend-parity `
  --output-dir outputs/backend-parity-v1
```

The explicit `infrastructure-only` scope compares paired end-to-end latency,
trial-admission wait, active execution latency, logical, physical and network
bytes, resource service/queue time, and design latency order. It permits
`task_success=null` only when the record also states
`semantic_task_quality_evaluated=false`; semantic quality is then excluded,
not interpreted as failure. The default `full` scope remains fail-closed and
requires literal boolean task success from both backends. Until tolerances are
preregistered either scope returns `DESCRIPTIVE_ONLY_THRESHOLDS_UNSET` and
never declares parity. Scenario rate-card cost is excluded because those
configured units are not physical money.

## Post-hoc container calibration

`calibrate-container-backend` binds the scenario, portable plan, discrete
event run, and complete container run before pairing every operation. It only
fits parameters that the container backend actually measures: effective
latency and throughput for deterministic fixture file reads with enough byte
size variation. Network observations are validation-only because their
elapsed-time floor is calculated from the input plan; fitting that value back
into the plan would be circular. Control, index, CPU, and GPU operations are
also excluded because the local container backend deliberately implements
them as no-ops. New plans use the same FIFO whole-trial admission contract in
both backends, so admission wait can be compared directly. Historical plans
that predate the contract remain diagnostic-only for queue calibration.

```powershell
python -m pathfinder calibrate-container-backend `
  --scenario configs/flowmesh_infra_simulator_4x8_smoke.json `
  --portable-plan-dir outputs/flowmesh-infra-portable-plan-v1 `
  --reference-run-dir outputs/flowmesh-infra-4x8-smoke-v1 `
  --container-run-dir outputs/flowmesh-infra-container-full-64-v1 `
  --output-scenario-id flowmesh-infra-container-storage-calibrated-v1 `
  --output-dir outputs/flowmesh-infra-container-calibration-v1

python -m pathfinder verify-container-backend-calibration `
  --output-dir outputs/flowmesh-infra-container-calibration-v1
```

The fit is explicitly post-hoc and applies only to the local cached fixture-I/O
environment. It is not a calibration of representative HDD/NVMe hardware and
cannot be validated against the observations used to fit it. Compile the new
scenario and use a fresh container run (or, preferably, held-out physical
measurements) for the next validation step.

## FlowMesh relationship

The MVP models the data-plane and task-execution semantics relevant to
FlowMesh, but it does not reproduce the whole FlowMesh control plane. Stable
workflow/task identities, worker placement, resource queues, artifact paths,
task types, outcomes, and telemetry form the compatibility seam.

The trace-import integration layer accepts a frozen real FlowMesh canonical
record ledger and maps:

```text
workflow/task      -> simulated workload/trial
worker             -> compute node
artifact access    -> storage and network operations
representation     -> physical data object
retry/failure      -> explicit outcome event
FlowMesh telemetry -> calibration and validation data
```

Run it without starting FlowMesh or any external service:

```powershell
python -m pathfinder import-flowmesh-infra-trace `
  --records path/to/canonical_records.jsonl `
  --output-dir outputs/flowmesh-trace-import-v1

python -m pathfinder verify-flowmesh-infra-trace-import `
  --output-dir outputs/flowmesh-trace-import-v1
```

The importer publishes trial observations, accepted access observations,
route-level latency/byte summaries, a manifest, and checksums. Failed trials
remain explicit but are excluded from calibration. Raw questions, answers,
artifact handles, and service credentials are never copied. Artifact-handle
fingerprints are validated for delivery completeness but are not republished.
Workflow, task, session, and trial identifiers are represented only by
SHA-256 fingerprints.

The importer does not pretend that an end-to-end measurement identifies all
of its internal causes. In particular, missing disk, network-queue, CPU, GPU,
or model-service time is listed as unobserved rather than estimated. Runtime
``realized_cost`` is retained under the name
``runtime_reported_service_cost`` and is explicitly not used to calibrate a
physical cost rate.

Parameter fitting against named resources and container emulation follow trace
import. They should validate
real HTTP, authentication, artifact delivery, failure recovery, and telemetry;
it should not be used to claim that eight containers on one host reproduce
eight independent machines.

## Immediate acceptance boundary

The offline MVP is complete when:

- all 64 reference trials produce complete records;
- every operation is deterministic for a fixed scenario;
- resource contention creates observable queue time;
- cache hit and miss paths are both exercised;
- network-byte totals reconcile with their events;
- two independent output directories are byte-identical; and
- checksum verification succeeds without any external service.

The implementation and focused tests enforce the offline properties.
Operator-observed smoke evidence outside this repository has exercised all
eight health endpoints, serial and concurrent cache paths, and representative
full physical chains submitted through FlowMesh; those observations are not
packaged as repository-verifiable evidence here. The formal runner can now
execute and checkpoint the complete
64-trial matrix, but that live formal run has not yet been performed. Failure
models, AWM/OED adapters, preregistered parity tolerances, and real
eight-machine validation remain later phases.

Container nodes treat `SIGTERM` and `SIGINT` as graceful shutdown requests.
The HTTP serving loop is stopped from a coordinator thread (the standard
library server would deadlock if `shutdown()` ran in its serving thread),
request threads that finish within Docker's stop timeout are drained by
`server_close()`, and Docker Compose can therefore stop an idle stack with
exit code 0 instead of timing out and forcing exit code 137. Operators should
still choose a Compose stop timeout long enough for any deliberately shaped,
in-flight transfer they intend to drain.
