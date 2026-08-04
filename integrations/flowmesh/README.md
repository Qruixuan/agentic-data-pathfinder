# FlowMesh Coupling Layer

This directory contains the deployment-side pieces for running a real
FlowMesh agent inside a Pathfinder experiment. The integration is deliberately
an adapter, not a fork: it does not require changes to the FlowMesh source
tree.

## Responsibility Boundary

```text
Pathfinder experiment runner
  -> FlowMesh public Python SDK
  -> FlowMesh AgentExecutor
  -> Pathfinder MCP tools
  -> Pathfinder AccessGateway
  -> representation backend
```

FlowMesh owns workflow scheduling, agent turns, model calls, and tool
invocation. Pathfinder owns the research contract:

- the class-specific offered representation set;
- the quoted price shown to the agent;
- budget and access-count enforcement;
- execution of the selected physical representation;
- felt latency returned to the agent; and
- hidden realized-cost and access telemetry.

The agent never receives the physical location, realized resource cost,
ground-truth quality, or expected latency from the gateway.

## Files

- `agent_configs/pathfinder_video.yaml` is the FlowMesh/UTU agent definition.
- `Dockerfile.worker-overlay` copies that definition into an existing
  agent-capable FlowMesh worker image.
- `pathfinder/integrations/flowmesh/` contains the SDK client, workflow
  builder, access gateway, SQLite session store, adapter, and MCP server.

## 1. Install the Local Integration

Python 3.12 or newer is required because the current FlowMesh SDK requires it.
From the Pathfinder repository:

```powershell
python -m pip install -e ".[flowmesh]"
```

When developing against the adjacent local FlowMesh checkout instead of the
published SDK:

```powershell
python -m pip install -e .
python -m pip install -e D:\Code\FlowMesh\sdk
python -m pip install "mcp>=1.23.0"
```

## 2. Start the Pathfinder Tool Gateway

The gateway is a separate Pathfinder-owned process. It shares experiment
sessions with the runner through an MVP SQLite store.

```powershell
python -m pathfinder serve-flowmesh-tools `
  --state-db outputs/flowmesh/gateway.sqlite3 `
  --host 0.0.0.0 `
  --port 8765
```

Its Streamable HTTP MCP endpoint is:

```text
http://<gateway-host>:8765/mcp
```

By default, the gateway uses the local emulated representation backend. To
route admitted accesses to a remote Data Agent, configure:

```powershell
$env:PATHFINDER_DATA_AGENT_URL = "http://127.0.0.1:8780"
$env:PATHFINDER_DATA_AGENT_TOKEN = "<optional-bearer-token>"
```

The Gateway-to-Data-Agent protocol is documented in
[`../data_agent/README.md`](../data_agent/README.md).

Use `http://host.docker.internal:8765/mcp` when a Docker Desktop FlowMesh
worker needs to reach a gateway on the host. For a remote or native-Linux
deployment, use a routable host or service address.

## 3. Make the Agent Config Available to FlowMesh

FlowMesh resolves agent configurations inside its worker image. Build a thin
overlay image so the Pathfinder config is added without editing the FlowMesh
repository:

```powershell
docker build `
  -f integrations/flowmesh/Dockerfile.worker-overlay `
  --build-arg FLOWMESH_WORKER_IMAGE=<existing-agent-capable-worker-image> `
  -t pathfinder-flowmesh-worker:dev `
  integrations/flowmesh
```

Configure the FlowMesh deployment to use the resulting worker image and pass:

```text
PATHFINDER_MCP_URL=http://<gateway-host>:8765/mcp
```

The worker still needs the model credentials expected by its UTU/FlowMesh
configuration. Start FlowMesh using its normal Linux deployment procedure.

## 4. Run One Real-Agent Session

In another terminal:

```powershell
$env:FLOWMESH_BASE_URL = "http://127.0.0.1:8000"
$env:FLOWMESH_API_KEY = "<only-if-enabled>"

python -m pathfinder run-flowmesh-session `
  --state-db outputs/flowmesh/gateway.sqlite3 `
  --design D_structured_digest `
  --task-class video_qa `
  --quote-profile digest_low `
  --seed 42 `
  --trial-id real-agent-smoke-001 `
  --object-id fixture-video-001 `
  --question "Summarize the important events in the video."
```

The runner registers the session before submitting the FlowMesh workflow. The
agent receives the opaque session ID in its task prompt and uses it when
calling `list_offers`, `access_representation`, and `get_session_state`.
When a remote Data Agent is configured, the runner also queries completed
artifact-transfer telemetry and attributes it to the matching Gateway access
event before returning the session result.

## Current Limitations

- The default `EmulatedRepresentationBackend` returns synthetic placeholder
  content and sleeps for the configured felt latency. It is suitable for
  integration tests and timing interventions, not scientific claims.
- Replace that backend with real video, embedding, digest, or object-storage
  adapters before collecting research results.
- The HTTP `DataAgentClient`, remote backend adapter, and single-node
  local-filesystem Data Agent server are implemented. Object storage,
  transforms, and multi-node routing remain future work.
- SQLite is appropriate for the single-machine MVP. A multi-worker experiment
  should move session state and atomic budget accounting to a service such as
  PostgreSQL.
- Keep one gateway process for the MVP. Its in-process lock and SQLite state
  are not a distributed transaction protocol.
- FlowMesh itself currently runs on Linux; the Pathfinder runner and gateway
  can run on Windows if the Linux FlowMesh service can reach the MCP endpoint.

## Why the Coupling Stays Outside FlowMesh

The integration uses two existing extension points: the public FlowMesh SDK
for workflow submission and an MCP toolkit for agent data access. Consequently
FlowMesh can evolve independently, and Pathfinder can replace the SDK client,
agent config, or physical backend without changing the experimental contract.
