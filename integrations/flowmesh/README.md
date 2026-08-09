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
python -m pip install -e /path/to/FlowMesh/sdk
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
  --build-arg PATHFINDER_MCP_URL=http://<gateway-host>:8765/mcp `
  -t <registry>/flowmesh_worker:<version>-cpu `
  integrations/flowmesh
```

### `PATHFINDER_MCP_URL` cannot be passed through the worker config

On FlowMesh v0.1.8-rc.1 the supervisor builds each worker container's
environment from a fixed, closed dictionary of `DockerWorkerConfig` fields
(`src/server/supervisor/adapters/base.py::_base_environment`). There is no
arbitrary-environment or passthrough field, so **`PATHFINDER_MCP_URL` cannot be
delivered through `DockerWorkerConfig`, the worker init config, or the
`flowmesh stack worker up --config` file.** Setting it there has no effect and
the agent fails when Hydra resolves `${oc.env:PATHFINDER_MCP_URL}`.

The URL must therefore come from a Pathfinder-owned mechanism. The supported
one is the overlay image above, which bakes it in as an `ENV`. Changing the
Gateway address means rebuilding the overlay with a new
`--build-arg PATHFINDER_MCP_URL`; it is a build-time, not run-time, parameter.

Also note the image tag: the supervisor derives the image name from the
worker config's `docker_registry` and `version` fields as
`<registry>/flowmesh_worker:<version>-cpu|gpu`. The overlay must be tagged to
match that convention, or the worker config cannot select it.

The worker still needs the model credentials expected by its UTU/FlowMesh
configuration (`UTU_LLM_TYPE`, `UTU_LLM_MODEL`, `UTU_LLM_BASE_URL`,
`UTU_LLM_API_KEY`). Those *are* first-class worker-config fields, so they must
never be baked into the overlay image. See
[Credential Handling](#credential-handling) for where the values themselves
belong. Start FlowMesh using its normal Linux deployment procedure.

## 4. Pin the Session to One Worker

A shared FlowMesh deployment has no worker-tenant isolation, so a Pathfinder
workflow must be pinned to the Pathfinder-owned worker. Pass either an exact
worker ID or a stable alias — they are mutually exclusive:

```powershell
python -m pathfinder run-flowmesh-session --worker-id wkr-16 ...
python -m pathfinder run-flowmesh-session --worker-alias pathfinder_cpu_0 ...
```

Equivalent environment variables are `PATHFINDER_FLOWMESH_WORKER_ID` and
`PATHFINDER_FLOWMESH_WORKER_ALIAS`.

Prefer the alias. FlowMesh reassigns worker IDs when a worker restarts, so the
alias is the durable name; Pathfinder resolves it to the current ID through
`workers.list(alias=...)` immediately before submission. Resolution must match
**exactly one** worker — zero or several matches raise
`FlowMeshPinningError` and nothing is submitted. Pathfinder never falls back to
an unpinned run, because that would place an experiment task on an arbitrary
shared worker and silently invalidate the session.

### Why every workflow is a one-node graph

FlowMesh v0.1.8-rc.1 only reads `metadata.annotations.schedule_hint` while
expanding `spec.graph` or `spec.stages`
(`src/server/task/parser.py::_parse_schedule_hint`). A bare single-task spec
**parses successfully but silently discards the hint**, producing
`selected_worker=None` and an unpinned task. That failure is invisible: the
submission succeeds and the task simply runs somewhere else.

Pathfinder therefore always emits the Agent task inside a one-node graph:

```yaml
metadata:
  annotations:
    custom:                       # all Pathfinder provenance lives here
      pathfinder_session_id: ...
    schedule_hint:                # sibling of custom, only when pinning
      selected_worker: wkr-16
spec:
  graph:
    nodes:
      - name: pathfinder-agent
        spec:
          taskType: agent
          ...
```

The same shape is used pinned and unpinned so both travel an identical parsing
and scheduling path.

### Declared resources are scheduling requests, not enforced limits

The generated Agent task declares `cpu: 1` and `memory: 2Gi` under
`spec.resources.hardware`
([`workflow.py`](../../pathfinder/integrations/flowmesh/workflow.py)). On
FlowMesh v0.1.8-rc.1 these are **task scheduling requests used for placement**.
They are not translated into Docker worker-container limits: the supervisor
does not set `--cpus`, `--memory`, or equivalent cgroup constraints from them.

Do not read them as a guarantee that the worker is confined to one core or
2 GiB. Nothing stops the task from using more, and nothing stops a co-scheduled
task from consuming the same worker's resources — v0.1.8-rc.1 has no
worker-tenant isolation.

Expected usage is nonetheless low, because model inference happens in an
external LLM service rather than on the worker; the worker mostly orchestrates
turns and MCP calls. That is an expectation, not an enforced bound, so **CPU
and memory must be monitored during a run** on the shared deployment. Treat a
sustained departure from the low-usage expectation as a signal that the
session's timing measurements are contaminated.

`metadata.annotations` is validated with `extra="forbid"` and permits only
`schedule_hint`, `description`, and `custom`, so Pathfinder provenance fields
must be nested under `custom`. Placing them directly under `annotations` is
rejected at submission with `extra_forbidden` errors.

### Validating before submission

```powershell
python -m pathfinder run-flowmesh-session --validate-workflow ...
```

This calls FlowMesh's `workflows.validate` endpoint, which parses and checks
the payload without enqueuing a workflow or dispatching a task. Useful for
confirming schema compatibility against a deployment before a live run.

## 5. Run One Real-Agent Session

In another terminal:

Load the FlowMesh endpoint and credential from your out-of-repository
configuration first (see [Credential Handling](#credential-handling)); the
placeholder below is a placeholder, not a value to paste a token into.

```powershell
$env:FLOWMESH_BASE_URL = "http://127.0.0.1:8000"
$env:FLOWMESH_API_KEY = "<from-secret-store-do-not-inline>"

python -m pathfinder run-flowmesh-session `
  --state-db outputs/flowmesh/gateway.sqlite3 `
  --design D_structured_digest `
  --task-class video_qa `
  --quote-profile digest_low `
  --seed 42 `
  --trial-id real-agent-smoke-001 `
  --object-id fixture-video-001 `
  --worker-alias pathfinder_cpu_0 `
  --question "Summarize the important events in the video."
```

The runner registers the session before submitting the FlowMesh workflow. The
agent receives the opaque session ID in its task prompt and uses it when
calling `list_offers`, `access_representation`, and `get_session_state`.
When a remote Data Agent is configured, the runner also queries completed
artifact-transfer telemetry and attributes it to the matching Gateway access
event before returning the session result.

## Credential Handling

This integration needs two classes of secret: the **LLM credentials** the
worker uses to reach the external inference service (`UTU_LLM_API_KEY`, and
`UTU_LLM_BASE_URL` where the endpoint is itself sensitive), and the **FlowMesh
PAT / API key** the Pathfinder runner uses to submit workflows
(`FLOWMESH_API_KEY`).

Both must be supplied from **a repository-external configuration file with
restricted filesystem permissions, or the deployment's approved secret
manager**. Nothing else is a supported source.

Concretely:

- **Committed files contain placeholders only.** Every credential-shaped value
  in this repository — in this README, in
  [`agent_configs/pathfinder_video.yaml`](agent_configs/pathfinder_video.yaml),
  in `Dockerfile.worker-overlay`, and in `configs/` — is a placeholder. Real
  values must never replace them in a tracked file, even temporarily.
- **Never in Git.** Not in a config, a worker config, a Compose file, a test
  fixture, or a commit message. A secret committed and then removed is still in
  history and must be treated as disclosed.
- **Never in an image.** Do not bake LLM credentials into the overlay image;
  supply them through the first-class worker-config fields, which read from the
  deployment's secret source.
- **Never in command arguments.** Arguments are visible in `ps` output to every
  user on a shared host, and are captured by shell history. Read secrets from a
  file or the secret manager inside the process instead of passing
  `--token <value>`.
- **Never in shell history.** Prefer sourcing a permission-restricted file over
  typing an assignment; the `$env:...` lines above are illustrative shapes, not
  a recommended entry method.
- **Never in logs or AI output.** Do not echo, print, or paste a credential
  into logs, issue text, or an assistant transcript, including for debugging.

Rotate through the secret manager, not by editing files in this repository.

## Current Limitations

- The default `EmulatedRepresentationBackend` returns synthetic placeholder
  content and sleeps for the configured felt latency. It is suitable for
  integration tests and timing interventions, not scientific claims.
- Replace that backend with real video, embedding, digest, or object-storage
  adapters before collecting research results.
- The HTTP `DataAgentClient`, remote backend adapter, and single-node
  local-filesystem Data Agent server are implemented. Object storage,
  transforms, and multi-node routing remain future work.
- The integration is pinned to FlowMesh `v0.1.8-rc.1` (`flowmesh-sdk==0.1.8rc1`).
  The `annotations.custom` nesting, the one-node graph requirement, and the
  absence of worker-environment passthrough were all verified against that
  exact revision and may change in later versions.
- Worker pinning constrains scheduling only. FlowMesh v0.1.8-rc.1 has no
  worker-tenant isolation, so pinning a Pathfinder task to a worker does not
  stop unrelated tasks being dispatched to that same worker. Monitor the
  worker during a run rather than assuming exclusivity.
- The task's declared `cpu` and `memory` are scheduling requests, not enforced
  container limits on v0.1.8-rc.1. See
  [Declared resources are scheduling requests, not enforced limits](#declared-resources-are-scheduling-requests-not-enforced-limits).
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
