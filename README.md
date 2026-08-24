# Pathfinder Minimal System

This repository now includes a minimal, runnable Python implementation of the
causal access-response harness described in the research documents. It is the
first vertical slice of Pathfinder. It now also includes offline Reduced
Oracle, AWM, and OED components, but not the complete deployed control plane.

The harness executes:

```text
physical design
  -> class-specific offered set and p_qv
  -> reproducible agent choice
  -> local Data Agent execution
  -> felt latency and realized cost
  -> terminal success and session value
  -> JSONL telemetry and CSV summaries
```

The simulated harness uses only the Python standard library. The optional
FlowMesh coupling layer uses the FlowMesh SDK and MCP package.

The staged experiment and acceptance criteria for moving from the completed
FlowMesh smoke tests to full Performative Physical Design validation are in
[PPD_VALIDATION_PROTOCOL.md](PPD_VALIDATION_PROTOCOL.md).

The first preregistered four-design controlled testbed run is defined in
[MULTI_CANDIDATE_FORMAL_V1_PREREGISTRATION.md](MULTI_CANDIDATE_FORMAL_V1_PREREGISTRATION.md).
It contains 128 real FlowMesh sessions, paired AWM train/holdout partitions,
and a three-candidate OED comparison. Its origin/local topology and unit costs
remain controlled single-node interventions; distributed placement is a later
gate.

The initial frozen results and their claim boundaries are summarized in
[MULTI_CANDIDATE_FORMAL_V1_RESULTS.md](MULTI_CANDIDATE_FORMAL_V1_RESULTS.md).
AWM v2 is now available as an opt-in post-hoc diagnostic with a fixed
full-domain confidence family and fixed-look paired gain certificates; it does
not alter the frozen v1 evidence.
AWM v3 adds a separate cluster-first diagnostic: one paired observation per
workload object, Bernoulli-KL bounds for paired success discordance, and an
independent empirical-Bernstein cost component. It is post-hoc method
development and must not be presented as a confirmatory rerun.
AWM v3alpha2 adds a non-destructive correction for repetition sensitivity: it
averages the complete fixed repetition block within each workload, continues
to count workloads—not repeated Agent runs—as independent units, and records
repetition sign flips in AWM and OED audit output. The v3alpha1 configuration
and outputs remain reproducible and unchanged.
AWM v3alpha3 is an isolated post-hoc certificate diagnostic over the same
fixed snapshot. It normalizes each workload-level paired utility to its
declared finite support and applies one direct bounded-mean KL interval as the
primary certificate. Success-discordance and service-cost decompositions are
retained only as explanatory diagnostics and are explicitly excluded from the
primary confidence family. No workload split or deployed system is changed.
AWM v3alpha4 adds an isolated joint structured diagnostic. It predeclares the
finite workload-level states induced jointly by success difference, selected
representation, and bounded action cost; groups them into five ordered utility
bins; and certifies their cumulative probabilities with simultaneous
Bernoulli-KL intervals. The support hash, effective bins, counts, and allocated
alpha are emitted into both AWM and OED outputs. This remains post-hoc method
development on the frozen Oracle, not new confirmatory evidence.

## Current Scope

The included experiment domain has:

- four video representations;
- two queued agent task classes;
- four static physical designs, A through D;
- class-specific, predeclared `P_qv` price universes;
- low and high digest quote interventions;
- independent felt-latency multipliers; and
- deterministic, seed-controlled agent and execution simulation.

Pilot repetitions use paired seeds across design, quote, and latency cells.
This implements common-random-number comparisons instead of introducing a
different synthetic workload draw for every intervention.

The `LocalDataAgent` currently emulates latency, bytes, and resource cost. Its
interface is intentionally small so it can later be replaced with FlowMesh,
object storage, NVMe, and real transformation adapters without changing the
session and telemetry contracts.

## Quick Start

Python 3.12 or newer is required.

Validate the configuration:

```powershell
python -m pathfinder validate-config
```

Run one session:

```powershell
python -m pathfinder run-session `
  --design D_structured_digest `
  --task-class video_qa `
  --quote-profile digest_low `
  --seed 42
```

The output keeps these quantities separate:

- `quoted_price` in every offered representation;
- `felt_latency_ms` after physical execution;
- `realized_cost` charged by the objective; and
- `terminal_success` and `session_value`.

Run the complete default pilot:

```powershell
python -m pathfinder run-pilot --output-dir outputs/pilot
```

The default grid runs:

```text
4 designs
x 2 task classes
x 3 quote profiles
x 2 latency multipliers
x 20 trials
= 960 sessions
```

It creates:

```text
outputs/pilot/
  sessions.jsonl
  summary.csv
  manifest.json
```

The manifest records the `P_qv` version and a SHA-256 hash of the complete
configuration used for the run.

Run a smaller pilot while developing:

```powershell
python -m pathfinder run-pilot `
  --output-dir outputs/smoke `
  --design D_structured_digest `
  --task-class video_qa `
  --quote-profile digest_low `
  --quote-profile digest_high `
  --latency-multiplier 1 `
  --latency-multiplier 2 `
  --trials-per-cell 10
```

Run the tests:

```powershell
python -m unittest discover -s tests -v
```

An editable installation is optional:

```powershell
python -m pip install -e .
pathfinder validate-config
```

## FlowMesh Integration

Pathfinder can now replace the deterministic simulated agent with a real
FlowMesh agent without modifying the FlowMesh source tree:

```text
Pathfinder runner
  -> FlowMesh SDK and AgentExecutor
  -> Pathfinder MCP AccessGateway
  -> physical representation backend
```

FlowMesh owns agent execution and orchestration. Pathfinder retains control of
the offered representations, quoted prices, budget enforcement, physical
access, and hidden realized-cost telemetry.

The coupling layer includes:

- a public-SDK client and one-agent workflow adapter;
- an MCP access gateway with persistent session state;
- atomic budget and access-count checks for the local MVP;
- worker pinning by exact ID or stable alias, so a session cannot run on an
  unrelated worker of a shared deployment; both pin kinds are verified against
  the configured Root before a Gateway session is registered or a workflow is
  submitted, so an exact ID never bypasses Root visibility;
- a read-only `preflight-flowmesh` control-plane check that reports sanitized
  endpoint identity and non-secret worker metadata, and states plainly that
  Node Server / worker alignment cannot be proven through that API;
- terminal workflow failures enriched with the workflow ID, task ID, terminal
  status, and redacted FlowMesh-reported detail — or an explicit statement
  that FlowMesh reported none;
- workflow validation that does not submit or execute a task;
- a frozen, block-randomized real-FlowMesh pilot runner with durable per-trial
  records, fail-closed telemetry classification, and resume support;
- a read-only pilot analyzer that reclassifies missing artifact downloads and
  regenerates workload-stratified and paired-contrast summaries;
- versioned feasibility-control and cost-aware quote-response pilot
  configurations that separate budget enforcement from behavioral response;
- a FlowMesh agent configuration and thin worker-image overlay; and
- fake-client integration tests that do not require a running FlowMesh stack,
  plus compatibility tests run against a real FlowMesh source tree when one is
  available.

The integration targets FlowMesh **v0.1.8-rc.1**. Two constraints of that
version shape the generated workflow: Pathfinder provenance must be nested
under `metadata.annotations.custom`, and the Agent task must be wrapped in a
one-node `spec.graph` for worker pinning to take effect at all.

See
[`integrations/flowmesh/README.md`](integrations/flowmesh/README.md)
for installation, deployment, and smoke-test instructions.
The dedicated batch configurations and Phase A runbook are in
[`integrations/flowmesh/PILOT.md`](integrations/flowmesh/PILOT.md).
The two-design Reduced Oracle MVP, safe materialization contract, output
schema, and manual server runbook are in
[`integrations/flowmesh/REDUCED_ORACLE.md`](integrations/flowmesh/REDUCED_ORACLE.md).
The offline Adaptive Workload Model, configuration-gated assumptions,
confidence sets, baselines, and held-out evaluation are documented in
[`integrations/flowmesh/AWM.md`](integrations/flowmesh/AWM.md).
The offline Commit/Reveal/Hold/Stop controller, equal-budget baselines, replay
contract, and Gate B4 boundary are documented in
[`integrations/flowmesh/OED.md`](integrations/flowmesh/OED.md).

`generate-synthetic-oracle` builds a deterministic five-design engineering
fixture in the Reduced Oracle output shape, so `evaluate-awm` and
`run-oed-replay` can be exercised over a genuinely multi-candidate Reveal
domain with no FlowMesh, Data Agent, MCP, or LLM dependency. Every generated
record, table row, and manifest carries `synthetic=true` and
`eligible_for_scientific_claims=false`, and the fixture schema refuses to load
a config that claims otherwise. Nothing it produces is evidence about a real
system. See
[`integrations/flowmesh/REDUCED_ORACLE.md`](integrations/flowmesh/REDUCED_ORACLE.md#offline-multi-candidate-synthetic-fixture).

The Gateway can also use the versioned HTTP `DataAgentClient` and included
single-node Data Agent server instead of the emulated backend. The server
executes idempotent, manifest-backed local-file accesses, supports typed
payload descriptors, object-aware catalogs, signed range downloads, and
post-download byte/time attribution while keeping realized cost hidden from
the FlowMesh agent. See
[`integrations/data_agent/README.md`](integrations/data_agent/README.md)
for the server-side protocol contract.

Remote `artifact_uri` payloads are exposed to the FlowMesh Agent through a
separate `fetch_artifact` tool. `access_representation` returns a random opaque
handle rather than the signed Data Agent URL; the handle is bound to the
originating session and access event. Only a SHA-256 fingerprint is persisted;
the reusable handle is not written to Gateway state, pilot records, or logs.
Pathfinder refreshes and downloads the artifact internally, rejects redirects
and unexpected origins, enforces a response-size bound, verifies the digest,
and returns only JSON or UTF-8 text. A selected artifact without a completed
full download is classified as `artifact_delivery_failure`, not as a completed
task.

Post-workflow reconciliation is fail-closed: byte and latency figures are
accepted only when the Data Agent reports `telemetry_complete=true`, and an
unstable or unanswerable snapshot fails the session rather than recording
under-counted values. That flag certifies a stable, per-process, point-in-time
snapshot. It does **not** prevent downloads after reconciliation and does not
coordinate multiple Data Agent processes; access sealing and multi-process
coordination are explicit follow-up requirements, not current guarantees.

All credentials — LLM keys, the FlowMesh PAT, and the Data Agent bearer token
and artifact signing secret — come from a repository-external
permission-restricted configuration file or the deployment's approved secret
manager. Committed examples in this repository contain placeholders only; see
[Credential Handling](integrations/flowmesh/README.md#credential-handling).

## Configuration Contract

The experiment contract is in
[`configs/minimal_system.json`](configs/minimal_system.json). It declares:

- representation sizes and task-specific quality;
- task-class budgets and behavioral parameters;
- the ex ante finite `P_qv` sets;
- design-specific path latency, location, realized cost, and class quote;
- quote-only interventions; and
- the default pilot grid.

Every configured design quote and intervention quote is checked against its
predeclared `P_qv`. The minimal implementation rejects invalid configuration
before running a session.

## Package Layout

```text
pathfinder/
  config.py       configuration parsing and contract validation
  models.py       versioned experiment and telemetry records
  resolver.py     class-specific offers and simulated agent choice
  data_agent.py   replaceable local physical-path emulator
  data_agent_client.py
                  versioned HTTP client for remote Data Agents
  data_agent_manifest.py
                  immutable local representation and plan bindings
  data_agent_server.py
                  idempotent HTTP Data Agent service
  experiment.py   single-session and factorial pilot runners
  telemetry.py    JSONL persistence and CSV aggregation
  cli.py          command-line interface
  synthetic_oracle/
                  deterministic engineering fixtures for the offline
                  Oracle/AWM/OED pipeline; never physical evidence
  integrations/
    flowmesh/      FlowMesh SDK, workflow, gateway, MCP adapters, read-only
                   control-plane preflight, and redaction helpers

configs/
  minimal_system.json
  data_agent_manifest.json
  data_object_catalog.example.json
  synthetic_oracle_fixture.json
  synthetic_multi_candidate_*.json

integrations/
  data_agent/      Data Agent HTTP protocol
  flowmesh/        worker overlay and FlowMesh agent configuration

tests/
  test_data_agent_client.py
  test_minimal_system.py
  test_flowmesh_integration.py
  test_synthetic_oracle.py
```

## Scientific Boundary

The default workload is synthetic. Passing this harness only demonstrates that
the implementation can express and measure the proposed causal chain. It does
not establish that a real LLM agent has access elasticity or that quoted price
is sufficient for real physical latency.

The `generate-synthetic-oracle` fixture sits further outside that boundary
again: it is generated from declared parameters rather than measured at all.
Its designs, response rates, storage costs, and transition costs are stipulated
constants, so no `Phi(D)`, regret, gate check, or policy ranking computed from
it says anything about a real system. It exists solely so the offline AWM and
OED code paths can be exercised over a multi-candidate domain while real Oracle
execution is unavailable.

The next research step is to replace `SimulatedAgent` with a fixed real-agent
adapter and replace selected `LocalDataAgent` paths with real representations,
while preserving exactly the same quote and observation schemas. AWM and OED
should be implemented only after that causal pilot passes its go/no-go gates.
