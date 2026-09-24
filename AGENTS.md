# Pathfinder repository instructions

Before freezing, verifying, staging, deploying, or running any Pathfinder,
FlowMesh, simulator, smoke, canary, one-case, or matrix experiment, read
`EXPERIMENT_OPERATIONS_RUNBOOK.md` completely.

Treat its applicable pre-submit checklist as fail-closed. Do not submit a
FlowMesh workflow or make an LLM request to diagnose a failed source-binding,
artifact-verification, deployment-health, endpoint, tunnel, authentication, or
worker-readiness gate.

Preserve existing frozen artifacts and evidence. Write corrected artifacts to
new immutable directories, use fresh run identities, and never expose
credentials or hidden labels in commands, logs, or reports.
