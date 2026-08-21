# Multi-Candidate Controlled PPD Experiment v1

## Status and Claim Boundary

This document freezes Pathfinder's first full multi-candidate experiment on
the existing single-node FlowMesh testbed. It is a real Agent, FlowMesh,
Data Agent, file-materialization, and artifact-delivery experiment. The
origin and local paths use different physical files selected through
the Data Agent object catalog. Their topology labels, minimum service delays,
and unit service costs remain controlled interventions on one host.

The experiment is eligible for the controlled-testbed PPD, Reduced Oracle,
AWM, and offline OED claims below. It is not evidence of cross-node network,
storage-tier, or distributed placement cost. Those claims require a later
multi-node testbed.

Do not modify the committed v1 inputs after the first workflow is submitted.
Any change requires a new version suffix and a new output directory.

## Frozen Domain

The experiment uses the same eight question-independent NExT-QA
representations and workload-object blocks as the small Phase B confirmatory
run. It exhaustively evaluates four complete physical designs:

| Design | Sampled frames | Multimodal digest |
|---|---|---|
| `D_origin_remote` | origin path, quote 4 | origin path, quote 6 |
| `D_local_frames` | local copy, quote 1 | origin path, quote 6 |
| `D_local_digest` | origin path, quote 4 | local copy, quote 2 |
| `D_local_pair` | local copy, quote 1 | local copy, quote 2 |

`D_origin_remote` is the certified safe design. Candidate transitions copy
only declared files below `data/multi_candidate_formal_v1/materialized/`.
Restoration removes only fingerprint-matching files owned by the transition
executor and returns to `D_origin_remote` after every candidate batch.

The complete Oracle contains:

```text
4 designs
  x 8 workload-object blocks
  x 4 paired repetitions
= 128 FlowMesh sessions
```

Repetitions 0--1 form the AWM training history. Repetitions 2--3 are hidden
from model fitting and policy decisions and are used only for paired holdout
evaluation. Pathfinder seeds are paired across designs. The runner executes
one design batch at a time and is resumable at completed-design boundaries.

## Fixed Objective and Costs

The horizon is 1,000 sessions over 24 hours. Task value, service cost, and
quotes are read from the committed system config. Transition and storage
costs are computed using the committed controlled single-node coefficients:

- copy cost: `0.05` per GiB;
- elapsed transition time: `0.001` per second;
- foreground transition loss: `0` because v1 runs no concurrent foreground
  workload; and
- storage cost: `0.01` per GiB-hour.

OED uses a `0.5` exploration purse, a `0.2` per-excursion cap, and the fixed
probe-window losses in `multi_candidate_formal_v1_oed.json`. These numbers
are preregistered engineering units, not provider billing or distributed
resource prices. Report copy bytes and elapsed time separately from the
derived cost.

## Questions and Acceptance Gates

### Gate M1: execution integrity

- exactly 128 planned and recorded trials;
- completion rate `1.0` for every design batch;
- complete Data Agent telemetry for every completed access;
- every accepted sampled-frame artifact is downloaded in full;
- no secret or raw artifact handle in frozen research records; and
- `safe_design_restored=true` with zero owned materialized bytes after the
  run.

An infrastructure, telemetry, or artifact-delivery failure is not a task
failure. Diagnose it and resume with the same command and frozen output. Do
not replace a completed Agent result.

### Gate M2: multi-design performative response

Report per design and workload type:

- representation-selection rates;
- exact task success;
- realized service cost;
- controlled service latency and its fetch/delay decomposition;
- artifact bytes and transfer latency; and
- steady-state and transition-adjusted `Phi`.

The registered directional mechanism checks are:

- `D_local_digest` increases digest access relative to `D_origin_remote`;
- `D_local_frames` increases frame access relative to
  `D_origin_remote`.

`D_local_pair` is not assigned a directional representation-choice claim:
both alternatives become cheaper, so its chosen representation is an
empirical result.

### Gate M3: AWM safety and informativeness

The first AWM fit observes training records from `D_origin_remote` only. All
empirical cross-design assumptions remain disabled. Report:

- complete held-out response-vector and `Phi` containment;
- false-safe Commit count;
- interval width for every design/model pair; and
- width reduction after each offline Reveal.

One successful containment event is an engineering check, not calibration of
90% frequentist coverage. The two paired holdout repetitions must also be
reported separately so a rank reversal is not hidden by aggregation.

### Gate M4: offline OED policy comparison

Compare full OED, passive AWM, random feasible Reveal, equal-budget
independent black-box Reveal, naive read-react materialization, and the
holdout exhaustive Oracle. Report:

- Reveal order and cost;
- final safe design;
- Commit and false-safe regression counts;
- net holdout value and regret; and
- whether full OED reaches the holdout Oracle at lower control cost than both
  equal-budget exploration baselines.

Because structural AWM assumptions remain disabled, full OED is not expected
to outperform the independent black-box model through transfer learning in
v1. Any advantage must come from the preregistered Reveal tiers and
value-per-cost ordering. A scientific coupled-AWM claim requires a later
validated config.

## Inputs

- System: `configs/multi_candidate_formal_v1_system.json`
- Workloads: `configs/multi_candidate_formal_v1_pilot.json`
- Data Agent: `configs/multi_candidate_formal_v1_data_agent_manifest.json`
- Object paths: `configs/multi_candidate_formal_v1_object_catalog.json`
- Oracle: `configs/multi_candidate_formal_v1_oracle.json`
- AWM: `configs/multi_candidate_formal_v1_awm.json`
- OED: `configs/multi_candidate_formal_v1_oed.json`

## Operator Sequence

Use a new external root, database files, and output directory. Never reuse the
two-design `run-office-v1` directory.

### 1. Validate committed inputs

```bash
cd "$HOME/agentic-data-pathfinder"
source "$HOME/.venvs/pf312/bin/activate"

PYTHONPATH=. python -m unittest tests.test_multi_candidate_formal -v
PYTHONPATH=. python -m pathfinder validate-config \
  --config configs/multi_candidate_formal_v1_system.json
```

### 2. Start the Data Agent in terminal A

```bash
export PF_MULTI_ROOT="$HOME/pathfinder-multi-candidate-formal-v1"
mkdir -p "$PF_MULTI_ROOT/logs" "$PF_MULTI_ROOT/runtime"

set -a
. "$HOME/.config/pathfinder/data-agent.env"
set +a

PYTHONPATH=. python -m pathfinder serve-data-agent \
  --manifest configs/multi_candidate_formal_v1_data_agent_manifest.json \
  --operation-db "$PF_MULTI_ROOT/runtime/data-agent.sqlite3" \
  --host 127.0.0.1 \
  --port 8780 \
  --public-base-url http://127.0.0.1:8780 \
  2>&1 | tee -a "$PF_MULTI_ROOT/logs/data-agent.log"
```

### 3. Start the MCP Gateway in terminal B

```bash
export PF_MULTI_ROOT="$HOME/pathfinder-multi-candidate-formal-v1"

set -a
. "$HOME/.config/pathfinder/data-agent.env"
set +a
unset PATHFINDER_DATA_AGENT_ARTIFACT_SECRET

PYTHONPATH=. python -m pathfinder serve-flowmesh-tools \
  --config configs/multi_candidate_formal_v1_system.json \
  --state-db "$PF_MULTI_ROOT/runtime/gateway.sqlite3" \
  --host 127.0.0.1 \
  --port 8765 \
  --data-agent-url http://127.0.0.1:8780 \
  --telemetry-quiescence-timeout 15 \
  2>&1 | tee -a "$PF_MULTI_ROOT/logs/gateway.log"
```

### 4. Run read-only FlowMesh preflight in terminal C

```bash
set -a
. "$HOME/.config/pathfinder/flowmesh-client.env"
set +a

: "${PF_CONFIRM_WORKER_ALIAS:?set the current dedicated worker alias}"

PYTHONPATH=. python -m pathfinder preflight-flowmesh \
  --worker-alias "$PF_CONFIRM_WORKER_ALIAS" \
  --flowmesh-base-url "$FLOWMESH_BASE_URL"
```

Proceed only if exactly one current worker is visible and both local services
are healthy. Do not create, start, stop, or remove a worker from the Oracle
command.

### 5. Run or resume the full Oracle

```bash
export PF_MULTI_ROOT="$HOME/pathfinder-multi-candidate-formal-v1"
export PF_MULTI_ORACLE_OUT="$PF_MULTI_ROOT/oracle-v1"

set -a
. "$HOME/.config/pathfinder/flowmesh-client.env"
. "$HOME/.config/pathfinder/data-agent.env"
set +a
unset PATHFINDER_DATA_AGENT_ARTIFACT_SECRET

PYTHONPATH=. python -m pathfinder run-reduced-oracle \
  --oracle-config configs/multi_candidate_formal_v1_oracle.json \
  --output-dir "$PF_MULTI_ORACLE_OUT" \
  --state-db "$PF_MULTI_ROOT/runtime/gateway.sqlite3" \
  --worker-alias "$PF_CONFIRM_WORKER_ALIAS" \
  --flowmesh-base-url "$FLOWMESH_BASE_URL" \
  --agent-config pathfinder_video_cost_aware \
  --data-agent-url http://127.0.0.1:8780 \
  --telemetry-quiescence-timeout 15 \
  --validate-workflow \
  2>&1 | tee "$PF_MULTI_ROOT/logs/oracle-v1.console.log"
```

The same command and output directory resume an interrupted run. Do not use a
new output directory to hide a failure.

### 6. Run AWM and OED only after Gate M1 passes

```bash
export PF_MULTI_AWM_OUT="$PF_MULTI_ROOT/awm-v1"
export PF_MULTI_OED_OUT="$PF_MULTI_ROOT/oed-v1"

PYTHONPATH=. python -m pathfinder evaluate-awm \
  --awm-config configs/multi_candidate_formal_v1_awm.json \
  --oracle-config configs/multi_candidate_formal_v1_oracle.json \
  --oracle-output-dir "$PF_MULTI_ORACLE_OUT" \
  --output-dir "$PF_MULTI_AWM_OUT"

PYTHONPATH=. python -m pathfinder run-oed-replay \
  --oed-config configs/multi_candidate_formal_v1_oed.json \
  --awm-config configs/multi_candidate_formal_v1_awm.json \
  --oracle-config configs/multi_candidate_formal_v1_oracle.json \
  --oracle-output-dir "$PF_MULTI_ORACLE_OUT" \
  --output-dir "$PF_MULTI_OED_OUT"
```

Freeze checksums outside each output directory before interpreting results.
