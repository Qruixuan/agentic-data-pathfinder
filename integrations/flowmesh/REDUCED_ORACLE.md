# Reduced Oracle MVP

## Purpose

The Reduced Oracle exhaustively deploys a small, frozen physical-design set,
measures the induced Agent response `W(D)`, computes the horizon objective
`Phi(D)`, and compares that result with a naive policy that reacts only to
accesses observed under the incumbent design.

The first implementation deliberately contains two designs:

- `D_remote_digest`: the certified safe incumbent; and
- `D_local_digest`: a digest copy materialized under a Pathfinder-owned root.

It reuses the Phase B eight-object workload and runs three paired repetitions
per design under `as_designed`, for 48 FlowMesh sessions. The runner never
creates, starts, stops, or removes a FlowMesh worker and never manages the Data
Agent or MCP process lifecycle.

## What the Runner Mutates

The only physical mutation is the set of files declared by
`configs/reduced_oracle_mvp.json` under:

```text
data/reduced_oracle_mvp/materialized/
```

Targets are copied through a temporary file, fingerprint-verified, and
atomically published. Pre-existing matching files may be reused but are never
claimed or removed. Files created by the runner are deleted during restoration
only if their fingerprints are unchanged. A changed file fails restoration
closed and is left in place for an operator to inspect. No recursive deletion
is used.

The frozen execution plan, transition journal, ownership state, and active
design record live in the experiment output directory. Reusing an output
directory resumes recorded work; changing the config requires a new output
directory.

## Manual Prerequisites

Before running the Oracle, the operator must already have:

1. a current, dedicated FlowMesh worker with the cost-aware Pathfinder Agent
   config and MCP URL;
2. repository-external FlowMesh and Data Agent environment files with mode
   `600`;
3. all eight generated representations under
   `data/phase_b_confirmatory_small/`; and
4. free local ports `8780` and `8765`.

Start the Data Agent in its own terminal:

```bash
cd "$HOME/agentic-data-pathfinder"
source "$HOME/.venvs/pf312/bin/activate"

export PF_ORACLE_ROOT="$HOME/pathfinder-reduced-oracle-mvp"
mkdir -p "$PF_ORACLE_ROOT/logs" "$PF_ORACLE_ROOT/runtime"

set -a
. "$HOME/.config/pathfinder/data-agent.env"
set +a

PYTHONPATH=. python -m pathfinder serve-data-agent \
  --manifest configs/reduced_oracle_mvp_data_agent_manifest.json \
  --operation-db "$PF_ORACLE_ROOT/runtime/data-agent.sqlite3" \
  --host 127.0.0.1 \
  --port 8780 \
  --public-base-url http://127.0.0.1:8780 \
  2>&1 | tee -a "$PF_ORACLE_ROOT/logs/data-agent.log"
```

Start the MCP Gateway in a second terminal:

```bash
cd "$HOME/agentic-data-pathfinder"
source "$HOME/.venvs/pf312/bin/activate"

export PF_ORACLE_ROOT="$HOME/pathfinder-reduced-oracle-mvp"
mkdir -p "$PF_ORACLE_ROOT/logs" "$PF_ORACLE_ROOT/runtime"

set -a
. "$HOME/.config/pathfinder/data-agent.env"
set +a
unset PATHFINDER_DATA_AGENT_ARTIFACT_SECRET

PYTHONPATH=. python -m pathfinder serve-flowmesh-tools \
  --config configs/phase_b_confirmatory_small_system.json \
  --state-db "$PF_ORACLE_ROOT/runtime/gateway.sqlite3" \
  --host 127.0.0.1 \
  --port 8765 \
  --data-agent-url http://127.0.0.1:8780 \
  --telemetry-quiescence-timeout 15 \
  2>&1 | tee -a "$PF_ORACLE_ROOT/logs/gateway.log"
```

Verify both services and the manually managed worker before submitting any
session. Then run the Oracle from a third terminal:

```bash
cd "$HOME/agentic-data-pathfinder"
source "$HOME/.venvs/pf312/bin/activate"

export PF_ORACLE_ROOT="$HOME/pathfinder-reduced-oracle-mvp"
export PF_ORACLE_OUT="$PF_ORACLE_ROOT/run-v1"
: "${PF_CONFIRM_WORKER_ALIAS:?set this to the current dedicated worker alias}"

set -a
. "$HOME/.config/pathfinder/flowmesh-client.env"
. "$HOME/.config/pathfinder/data-agent.env"
set +a
unset PATHFINDER_DATA_AGENT_ARTIFACT_SECRET

PYTHONPATH=. python -m pathfinder run-reduced-oracle \
  --oracle-config configs/reduced_oracle_mvp.json \
  --output-dir "$PF_ORACLE_OUT" \
  --state-db "$PF_ORACLE_ROOT/runtime/gateway.sqlite3" \
  --worker-alias "$PF_CONFIRM_WORKER_ALIAS" \
  --flowmesh-base-url "$FLOWMESH_BASE_URL" \
  --agent-config pathfinder_video_cost_aware \
  --data-agent-url http://127.0.0.1:8780 \
  --telemetry-quiescence-timeout 15 \
  --validate-workflow \
  2>&1 | tee "$PF_ORACLE_ROOT/logs/reduced-oracle-v1.console.log"
```

The command is resumable. Run the same command with the same output directory
after an interruption. It restores the safe design before continuing and does
not resubmit a completed design batch.

Recompute the analysis without submitting work:

```bash
PYTHONPATH=. python -m pathfinder analyze-reduced-oracle \
  --oracle-config configs/reduced_oracle_mvp.json \
  --output-dir "$PF_ORACLE_OUT"
```

## Output Contract

The output directory contains:

- `oracle_plan.json`: frozen designs, paired trials, hashes, horizon, and cost
  model;
- `designs/<design-id>/`: the normal pilot plan, records, summaries, and
  manifest for each exhaustively deployed design;
- `transitions.jsonl`: forward and restoration time, bytes, foreground loss,
  and realized transition cost;
- `runtime/transition_state.json`: fingerprints of files owned by the runner;
- `runtime/active_plan.json`: the last durably activated design;
- `oracle_table.csv`: empirical `W(D)`, success, service cost, storage cost,
  `Phi(D)`, and transition-adjusted `Phi(D)` per design;
- `lock_in_trace.json`: naive incumbent-only estimate versus the exhaustive
  Oracle; and
- `oracle_summary.json` and `oracle_manifest.json`: final decision and audit
  metadata.

An Oracle row is evaluable only if its completion rate reaches the configured
threshold, all completed answers are evaluable, and telemetry is complete.
Artifact-delivery, telemetry, and infrastructure failures are not silently
converted into Agent choices.

## Scientific Boundary

The committed configuration is an **engineering MVP**, not the complete Gate
B2 result. Both source and target paths may reside on the same physical disk,
and service-path latency is still controlled by the Data Agent binding. This
validates safe materialization, exhaustive scheduling, restoration, transition
accounting, objective computation, resume behavior, and the lock-in analysis
pipeline.

Before making a physical-placement claim:

- place the source on the actual remote/shared tier and the target on actual
  node-local NVMe;
- measure rather than inject service latency, copy time, storage cost, and
  foreground loss;
- pre-register the horizon and normalized cost coefficients instead of using
  the committed `engineering-placeholder-requires-preregistration` values;
- add the remaining governance-feasible designs needed to distinguish `M`,
  `L`, and `E`; and
- repeat the exhaustive table with enough independent objects and uncertainty
  reporting.

Only that calibrated exhaustive table should become the frozen dataset used by
AWM and OED.

The offline AWM consumer and its held-out evaluation contract are documented
in [`AWM.md`](AWM.md).
