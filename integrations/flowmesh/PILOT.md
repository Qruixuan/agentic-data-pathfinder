# Real FlowMesh Pilot Runner

`run-flowmesh-pilot` executes a frozen, randomized set of real FlowMesh Agent
sessions. It is deliberately narrower than the simulated `run-pilot` command:
it tests the Agent's response to declared interventions while collecting real
Gateway and Data Agent telemetry.

The runner only submits sessions. It never creates, starts, stops, or removes a
FlowMesh worker, and it never starts or stops the Data Agent or MCP Gateway.
Those processes remain operator-controlled.

## Included Phase A Configurations

- `configs/phase_a_quote_pilot_system.json` fixes the physical design at
  `D_structured_digest` and exposes only the three representations the current
  MCP path can safely deliver: `sampled_frames`, `embeddings`, and
  `multimodal_digest`. `compressed_video` is excluded because
  `fetch_artifact` intentionally rejects binary media.
- `configs/phase_a_quote_pilot_dry_run.json` contains one repetition of each
  quote condition. Use it to check the batch path after the Level-2 smoke test.
- `configs/phase_a_quote_pilot.json` contains ten repetitions of each quote
  condition for the engineering pilot.

Both plans hold the physical design and latency multiplier fixed. The treatment
changes only the digest quote from 6 (`as_designed`) to 2 (`digest_low`). The
question does not name or require a representation, so representation choice is
an observed Agent behavior rather than an instruction.

These included plans reuse one fixture object and one fixture question. They
can validate experiment mechanics and estimate whether the intervention has a
visible directional effect, but repeated fixture answers are not independent
scientific samples. Replace `workloads` with a frozen real evaluation set before
making a causal research claim.

## Before Running

The operator must already have:

1. the Data Agent running and healthy;
2. the Pathfinder worker created and started manually;
3. repository-external FlowMesh and Data Agent environment files loaded; and
4. the MCP Gateway running with the pilot system configuration and the same
   SQLite state path that will be passed to the batch runner.

If the current MCP Gateway was started with `configs/minimal_system.json`, stop
that process manually and restart it with the dedicated pilot system config.
The Data Agent does not need to be rebuilt or restarted merely because the
Gateway configuration changed.

For example, in the MCP terminal:

```bash
cd "$HOME/agentic-data-pathfinder"
source "$HOME/.venvs/pf312/bin/activate"

export PF_PILOT_ROOT="$HOME/pathfinder-pilot"
export PF_PILOT_DB="$PF_PILOT_ROOT/gateway.sqlite3"
mkdir -p "$PF_PILOT_ROOT"

set -a
. "$HOME/.config/pathfinder/data-agent.env"
set +a
unset PATHFINDER_DATA_AGENT_ARTIFACT_SECRET

PYTHONPATH=. python -m pathfinder serve-flowmesh-tools \
  --config configs/phase_a_quote_pilot_system.json \
  --state-db "$PF_PILOT_DB" \
  --host 127.0.0.1 \
  --port 8765 \
  --data-agent-url http://127.0.0.1:8780
```

The worker overlay must still point to this MCP endpoint. Starting the worker
remains a separate manual action.

## Two-Session Dry Run

In another terminal, load credentials from the permission-restricted files;
do not place a PAT or LLM key in these commands.

```bash
cd "$HOME/agentic-data-pathfinder"
source "$HOME/.venvs/pf312/bin/activate"

export PF_PILOT_ROOT="$HOME/pathfinder-pilot"
export PF_PILOT_DB="$PF_PILOT_ROOT/gateway.sqlite3"

set -a
. "$HOME/.config/pathfinder/flowmesh-client.env"
. "$HOME/.config/pathfinder/data-agent.env"
set +a
unset PATHFINDER_DATA_AGENT_ARTIFACT_SECRET

# Restore this from the existing operator-owned smoke environment file.
: "${PF_WORKER_ALIAS:?PF_WORKER_ALIAS must name the manually created worker}"

PYTHONPATH=. python -m pathfinder run-flowmesh-pilot \
  --pilot-config configs/phase_a_quote_pilot_dry_run.json \
  --output-dir "$PF_PILOT_ROOT/phase-a-dry-run-v1" \
  --state-db "$PF_PILOT_DB" \
  --worker-alias "$PF_WORKER_ALIAS" \
  --flowmesh-base-url "$FLOWMESH_BASE_URL" \
  --agent-config pathfinder_video \
  --data-agent-url http://127.0.0.1:8780 \
  --validate-workflow
```

The command runs exactly two sessions in a randomized order: one `digest_low`
and one `as_designed`. Re-running the identical command resumes the frozen plan
and does not resubmit recorded successes or failures.

## Ten-Per-Condition Engineering Pilot

After reviewing the dry-run output, use the full plan and a different output
directory:

```bash
PYTHONPATH=. python -m pathfinder run-flowmesh-pilot \
  --pilot-config configs/phase_a_quote_pilot.json \
  --output-dir "$PF_PILOT_ROOT/phase-a-quote-pilot-fixture-v1" \
  --state-db "$PF_PILOT_DB" \
  --worker-alias "$PF_WORKER_ALIAS" \
  --flowmesh-base-url "$FLOWMESH_BASE_URL" \
  --agent-config pathfinder_video \
  --data-agent-url http://127.0.0.1:8780 \
  --validate-workflow
```

The plan uses blocked randomization. Every workload/repetition block contains
one copy of every intervention cell; cell order and block order are determined
by `randomization_seed`. The same repetition registers the same Pathfinder
seed across the two quote conditions, but the current FlowMesh/UTU workflow
does not control the external LLM's sampling seed. Treat blocking and order
randomization, not deterministic model replay, as the protection against drift.

## Output and Resume Contract

Each output directory contains:

- `trial_plan.json`: the realized randomized order, deterministic session IDs,
  configuration hashes, and the complete frozen trial list;
- `runs.jsonl`: one fsynced record per attempted session, including intended
  quote matrix, selected representation, physical telemetry, final answer, and
  error classification;
- `summary.csv`: per-condition completion, access, task-success, selection,
  cost, byte, and latency summaries;
- `manifest.json`: progress, code/config versions, non-secret runtime metadata,
  and artifact locations.

The runner skips every trial already present in `runs.jsonl`, including an
infrastructure or telemetry failure. It never silently retries a failed outcome
under the same trial identity. For a missing JSONL record, it first checks the
Gateway state: a completed session is reconstructed, and a session already
bound to FlowMesh is waited and reconciled without resubmission. A missing
Gateway session is submitted normally. A session created but not bound to a
FlowMesh task is ambiguous, so it is marked as an infrastructure failure rather
than risking duplicate work.

The runner refuses to mix a changed repetition count, randomization seed, or
configuration into an existing output directory. Use a new pilot config with a
new `experiment_id` and a new output directory for a deliberate retry or a new
experiment version. An OS-level lock also rejects a second runner process for
the same output directory, preventing concurrent duplicate submissions.

Failure interpretation is explicit:

- `completed`: a valid behavioral outcome, including no access or a wrong
  answer;
- `telemetry_failure`: the physical byte/latency observation could not be
  proven complete and must not enter PPD analysis;
- `infrastructure_failure`: FlowMesh, worker, model, or another execution
  dependency failed.

The summary never converts either failure class into an Agent decision.
