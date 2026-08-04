# Pathfinder Minimal System

This repository now includes a minimal, runnable Python implementation of the
causal access-response harness described in the research documents. It is the
first vertical slice of Pathfinder, not the complete AWM/OED controller.

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
- a FlowMesh agent configuration and thin worker-image overlay; and
- fake-client integration tests that do not require a running FlowMesh stack.

See
[`integrations/flowmesh/README.md`](integrations/flowmesh/README.md)
for installation, deployment, and smoke-test instructions.

The Gateway can also use the versioned HTTP `DataAgentClient` and included
single-node Data Agent server instead of the emulated backend. The server
executes idempotent, manifest-backed local-file accesses, supports typed
payload descriptors, object-aware catalogs, signed range downloads, and
post-download byte/time attribution while keeping realized cost hidden from
the FlowMesh agent. See
[`integrations/data_agent/README.md`](integrations/data_agent/README.md)
for the server-side protocol contract.

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
  integrations/
    flowmesh/      FlowMesh SDK, workflow, gateway, and MCP adapters

configs/
  minimal_system.json
  data_agent_manifest.json
  data_object_catalog.example.json

integrations/
  data_agent/      Data Agent HTTP protocol
  flowmesh/        worker overlay and FlowMesh agent configuration

tests/
  test_data_agent_client.py
  test_minimal_system.py
  test_flowmesh_integration.py
```

## Scientific Boundary

The default workload is synthetic. Passing this harness only demonstrates that
the implementation can express and measure the proposed causal chain. It does
not establish that a real LLM agent has access elasticity or that quoted price
is sufficient for real physical latency.

The next research step is to replace `SimulatedAgent` with a fixed real-agent
adapter and replace selected `LocalDataAgent` paths with real representations,
while preserving exactly the same quote and observation schemas. AWM and OED
should be implemented only after that causal pilot passes its go/no-go gates.
