# Distributed policy-level AWM audit

This stage connects a frozen distributed-pilot evaluation to the AWM safety
gate without pretending that the restricted pilot is a complete design
Oracle.

## Evidence boundary

The restricted pilot contains, for every workload, repeated observations from:

1. the safe origin design; and
2. the one candidate design assigned to that workload's stratum.

It does not contain all physical designs for every workload. The audit accepts
only policies that choose between the two observed outcomes in each workload.
It refuses missing pairs, mismatched strata/designs, incomplete evaluations,
tampered evaluator files, and unmarked post-hoc policies. It never imputes an
unobserved design outcome.

The checked-in audit compares:

- `executed_restricted_policy`: the policy actually evaluated in the pilot
  (`D_local_frames` for causal/descriptive workloads and `D_local_digest` for
  temporal workloads); and
- `temporal_origin_fallback`: a post-hoc diagnostic that retains
  `D_local_frames` for causal/descriptive workloads but uses the safe
  `D_origin_remote` design for temporal workloads.

These names identify physical designs, not a claim that the agent always chose
the locally materialized representation. The canonical access route remains
authoritative for the representation actually selected in each session.

Both results remain development evidence. The second policy was selected after
inspecting the pilot, and the pilot also has a documented runtime-model
deviation. Neither result is eligible for scientific claims or a production
commit decision.

## Run against the frozen 36-workload snapshot

Run this offline on the execution server after checking out the revision that
contains `audit-distributed-policy-awm`:

```bash
cd "$HOME/agentic-data-pathfinder"
source "$HOME/.venvs/pf312/bin/activate"

export PF_DISTRIBUTED_SNAPSHOT="$HOME/pathfinder-freezes/pathfinderbench-restricted-pilot-v0.1-36-20260903-v2-qwen38-evaluated-v1"
export PF_POLICY_AWM_OUT="$HOME/pathfinder-awm/pathfinderbench-restricted-pilot-v0.1-36-policy-posthoc-v1"

test -d "$PF_DISTRIBUTED_SNAPSHOT/evaluation"
test -f "$PF_DISTRIBUTED_SNAPSHOT/input-freeze/config/preregistration.json"
test ! -e "$PF_POLICY_AWM_OUT"

PYTHONPATH=. python -m pathfinder audit-distributed-policy-awm \
  --evaluation-dir "$PF_DISTRIBUTED_SNAPSHOT/evaluation" \
  --preregistration \
    "$PF_DISTRIBUTED_SNAPSHOT/input-freeze/config/preregistration.json" \
  --audit-config \
    configs/pathfinderbench_restricted_pilot_v0_1_distributed_policy_awm.json \
  --output-dir "$PF_POLICY_AWM_OUT"
```

This command is read-only with respect to the snapshot, performs no network or
deployment operation, and atomically creates a new output directory.

## Verify and inspect

```bash
(
  cd "$PF_POLICY_AWM_OUT"
  sha256sum -c SHA256SUMS
)

python - <<'PY'
import json
import os
from pathlib import Path

root = Path(os.environ["PF_POLICY_AWM_OUT"])
report = json.loads((root / "policy_evaluation.json").read_text())

print("status:", report["status"])
print("pilot_id:", report["pilot_id"])
print("independent_workloads:", report["independent_workloads"])
print("complete_design_oracle:", report["complete_design_oracle"])
print("posthoc:", report["posthoc"])
print("eligible_for_scientific_claims:", report["eligible_for_scientific_claims"])

for row in report["policy_summaries"]:
    print("=" * 72)
    print("policy:", row["policy_id"])
    print("success_delta:", row["mean_task_success_delta"])
    print("cost_saving:", row["mean_cost_saving"])
    print("point_thresholds:", row["meets_point_thresholds"])
    print("certificate_state:", row["certificate_state"])
    print(
        "success_bounds:",
        row["success_lower_bound"],
        row["success_upper_bound"],
    )
    print(
        "cost_bounds:",
        row["cost_saving_lower_bound"],
        row["cost_saving_upper_bound"],
    )
PY
```

The expected descriptive point estimates from the frozen evaluation are:

| Policy | Mean success delta vs safe | Mean cost saving vs safe |
|---|---:|---:|
| Executed restricted policy | -0.0138889 | 0.7188124 |
| Temporal-origin fallback | 0.0416667 | approximately 0.2522194 |

The fallback meets the two engineering thresholds only at the point-estimate
level and only narrowly on cost. With 36 independent workloads and conservative
bounded, family-adjusted intervals, the expected certificate state is
`INSUFFICIENT_EVIDENCE`, not `SAFE_TO_COMMIT`.

## Decision after the audit

Use the output to select and document the next hypothesis. Do not rerun or
reinterpret this cohort as confirmatory. If the temporal-origin fallback
remains the preferred policy, freeze it before observing outcomes from a new,
disjoint workload holdout and run the same safe-versus-policy paired protocol
there. OED may consume the certificate only after that fresh policy test; it
must not treat this post-hoc audit as a commit authorization.
