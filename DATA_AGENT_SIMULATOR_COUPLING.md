# Data Agent–Simulator Semantic Coupling

## Scope

This package adds one deliberately narrow semantic integration slice to the
eight-node simulator.  It binds a real benchmark object to one exact frozen
matrix trial and performs:

```text
frozen matrix trial association
  -> endpoint-registry route
  -> Data Agent access (bearer-authenticated when configured)
  -> bounded sampled-frame-bundle download
  -> canonical tar/JPEG/hash validation
  -> ordered frames sent to the N6 semantic endpoint
  -> vision-model answer
  -> frozen multiple-choice scoring
  -> checksummed semantic evidence
  -> offline association with the existing FlowMesh matrix evidence
```

The Data Agent artifact is never written into the result package.  Neither
the frame bytes or their base64 transport encoding, rendered question, prompt,
endpoint URL, bearer token, nor API key is recorded.  The semantic-trial
evidence contains identifiers, hashes, bounded telemetry, the model identity,
the short validated option marker, ordered option IDs without option text, and
the derived score.  Prose, an undeclared option, or an oversized model response
fails before an output directory is created.  The later cross-layer bundle
omits even that option marker and retains only its digest and score.

This is a **cross-layer integration slice**, not yet a unified FlowMesh data
path.  The host coordinator fetches the Data Agent artifact and then invokes
the N6 container directly.  The existing FlowMesh matrix run used synthetic
objects.  Its infrastructure operation DAG and this semantic request are
associated through an explicit trial binding, but they are not claimed to be
the same physical route.  The posthoc cross-layer bundle therefore fixes:

```text
execution_and_semantic_route_unified = false
flowmesh_semantic_execution_verified = false
host_to_container_ownership_mapping_verified = false
cost_basis = unavailable
monetary_cost_measured = false
simulated_cost_computed = false
eligible_for_awm_oed = false
eligible_for_scientific_claims = false
```

## Contracts

### Semantic trial specification

Create a new, immutable JSON document for every real semantic observation.
The matrix identity and the real artifact identity remain separate on
purpose.  A minimal example is:

```json
{
  "schema_version": "pathfinder.data-agent-frame-bundle-semantic-spec/v1alpha1",
  "semantic_run_id": "pathfinder-semantic-visible-001",
  "trial_key": "flowmesh-infra-4x8-local-smoke-v1|smoke-descriptive|D2|r0000",
  "semantic_executor_node_id": "N6",
  "representation_id": "sampled_frame_bundle",
  "data_agent_route_design_id": "D_origin_remote",
  "data_agent_plan_id": "D_origin_remote",
  "data_agent_plan_epoch": 0,
  "workload_id": "smoke-descriptive",
  "task_class_id": "video_qa",
  "artifact_object_id": "nextqa-val-4010069381",
  "artifact_sha256": "REPLACE_WITH_64_LOWERCASE_HEX_DIGEST",
  "artifact_size_bytes": 481280,
  "object_catalog_version": "REPLACE_WITH_DATA_AGENT_CATALOG_VERSION",
  "question": "Which option best describes the main action?",
  "success_scoring_rule": "multiple-choice-option-id-canonical-match-v1",
  "answer_options": [
    {"option_id": "A", "text": "..."},
    {"option_id": "B", "text": "..."},
    {"option_id": "C", "text": "..."},
    {"option_id": "D", "text": "..."}
  ],
  "correct_answer_id": "B",
  "expected_model": "qwen3.8-27b",
  "credentials_recorded": false
}
```

The file is fail-closed: it must use exactly this field set, its artifact
size/hash/catalog must match the Data Agent response, its `workload_id` must
match the selected matrix trial, and the container must report the frozen
model identity exactly.

`matrix_object_id` is taken from the existing synthetic matrix plan;
`artifact_object_id` is taken from this specification.  They are never
silently equated.

### Semantic container protocol

The N6 endpoint now accepts semantic request schema `v1alpha2`.  It verifies
an ordered sequence of bounded JPEG frames, their canonical base64, byte
sizes, SHA-256 values, JPEG structure, dimensions, timestamp ordering, and a
whole-sequence digest before making an OpenAI-compatible multimodal request.
The base64 exists only in the bounded in-memory request and is rejected from
generated evidence.  The older text request schema remains supported
unchanged.

The health gate must report `semantic_quality_enabled: true`,
`semantic_llm_configured: true`, and
`semantic_vision_request_adapter_supported: true`, together with the exact
vision request schema, no recorded credentials, the correct node ID, and a
stable runtime epoch before and after the call.  A changed epoch, model,
payload, digest, or response schema fails the trial.

Credential-bearing Data Agent JSON requests and the model request reject all
HTTP redirects.  Model output is bounded at the container boundary and may not
contain the configured API key.  A Data Agent on a non-loopback host must use
HTTPS; a plain-HTTP lab deployment must be reached through a loopback tunnel.
Before evidence is written, the vertical runner
also requires the output to be exactly one declared option under the frozen
exact/canonical marker grammar; a declared but incorrect option is safely
recorded with `task_success: false`.

A successful trial sets
`container_semantic_response_consistency_verified: true`: the response is
bound to the request, frame sequence, model, answer digest, and stable runtime
epoch.  It still sets `container_runtime_code_provenance_verified: false`
because the current mutable container image is not bound to a frozen image
digest or source revision.  Response consistency must not be described as
verified runtime code provenance.  N6 receives validated JPEG frames from the
host, not the Data Agent artifact through a container data-plane fetch, so the
trial also fixes `container_data_plane_artifact_delivery_verified: false`.

## Running one semantic trial

Use a semantic-enabled Compose package built from the current source.  The
Data Agent URL and optional bearer token are resolved only through the
environment variable names declared in the endpoint registry.  The model
endpoint and key are likewise passed to the N6 container through environment
variables; do not put secret values in a command, spec, Compose file, or
result folder.

```bash
cd "$HOME/agentic-data-pathfinder"
source "$HOME/.venvs/pf312/bin/activate"

export PF_MATRIX_PLAN="/path/to/frozen/matrix-plan"
export PF_MATRIX_RUN="/path/to/verified/matrix-run"
export PF_ENDPOINT_REGISTRY="/path/to/endpoint-registry.json"
export PF_COMPOSE_PACKAGE="/path/to/semantic-enabled/compose-package"
export PF_SEMANTIC_SPEC="/path/to/semantic-trial-spec.json"
export PF_SEMANTIC_OUT="/new/path/semantic-trial-output"

# Source the existing operator-local Data Agent and model environment files.
# Never print them and never copy them into the output directory.

PYTHONPATH=. python -m pathfinder \
  run-data-agent-frame-bundle-semantic-trial \
  --matrix-plan-dir "$PF_MATRIX_PLAN" \
  --semantic-spec "$PF_SEMANTIC_SPEC" \
  --endpoint-registry "$PF_ENDPOINT_REGISTRY" \
  --compose-package-dir "$PF_COMPOSE_PACKAGE" \
  --output-dir "$PF_SEMANTIC_OUT" \
  --event-index 0
```

This command makes one Data Agent download and one container/LLM request.  The
host process coordinates both steps and calls N6 directly; N6 then makes the
model request.  The host does not submit a FlowMesh workflow, use FlowMesh
semantic scheduling, or mutate the frozen matrix plan or run.

If the Data Agent download completed but the semantic call failed, retry into
a **new output directory** and increment `--event-index`.  The new event gets
a distinct Data Agent access ID, so its exact-download telemetry cannot be
mixed with the prior attempt.  The semantic request identity remains stable,
allowing the N6 idempotency cache to return an already completed model result
when appropriate.

Verify the output without a network, container, Data Agent, or LLM call:

```bash
PYTHONPATH=. python -m pathfinder \
  verify-data-agent-frame-bundle-semantic-trial \
  --output-dir "$PF_SEMANTIC_OUT" \
  --matrix-plan-dir "$PF_MATRIX_PLAN" \
  --semantic-spec "$PF_SEMANTIC_SPEC" \
  --endpoint-registry "$PF_ENDPOINT_REGISTRY"

( cd "$PF_SEMANTIC_OUT" && sha256sum -c SHA256SUMS )
```

## Associating semantic and infrastructure evidence

After one or more semantic trials verify, create a separate binding spec.
Every association is explicit and includes the semantic-spec digest:

```json
{
  "schema_version": "pathfinder.flowmesh-container-pathfinder-evidence-spec/v1alpha1",
  "evidence_bundle_id": "pathfinder-simulator-cross-layer-visible-v1",
  "matrix_id": "REPLACE_WITH_MATRIX_ID",
  "matrix_plan_sha256": "REPLACE_WITH_MATRIX_PLAN_SHA256",
  "matrix_run_id": "REPLACE_WITH_MATRIX_RUN_ID",
  "bindings": [
    {
      "semantic_run_id": "pathfinder-semantic-visible-001",
      "semantic_spec_sha256": "REPLACE_WITH_SEMANTIC_SPEC_SHA256",
      "event_index": 0,
      "expected_model": "qwen3.8-27b",
      "trial_key": "flowmesh-infra-4x8-local-smoke-v1|smoke-descriptive|D2|r0000",
      "workload_id": "smoke-descriptive",
      "workload_class": "W1",
      "matrix_design_id": "D2",
      "data_agent_route_design_id": "D_origin_remote",
      "repetition": 0,
      "matrix_object_id": "REPLACE_WITH_MATRIX_OBJECT_ID",
      "artifact_object_id": "nextqa-val-4010069381",
      "representation_id": "sampled_frame_bundle"
    }
  ],
  "execution_and_semantic_route_unified": false,
  "cost_basis": "unavailable",
  "credentials_recorded": false,
  "eligible_for_awm_oed": false,
  "eligible_for_scientific_claims": false
}
```

Build and independently reproduce the association:

```bash
export PF_BINDING_SPEC="/path/to/evidence-binding-spec.json"
export PF_EVIDENCE_OUT="/new/path/pathfinder-cross-layer-evidence"

PYTHONPATH=. python -m pathfinder \
  build-flowmesh-pathfinder-evidence \
  --binding-spec "$PF_BINDING_SPEC" \
  --matrix-plan-dir "$PF_MATRIX_PLAN" \
  --matrix-run-dir "$PF_MATRIX_RUN" \
  --endpoint-registry "$PF_ENDPOINT_REGISTRY" \
  --data-agent-semantic-dir "$PF_SEMANTIC_OUT" \
  --data-agent-semantic-spec "$PF_SEMANTIC_SPEC" \
  --output-dir "$PF_EVIDENCE_OUT"

PYTHONPATH=. python -m pathfinder \
  verify-flowmesh-pathfinder-evidence \
  --evidence-dir "$PF_EVIDENCE_OUT" \
  --binding-spec "$PF_BINDING_SPEC" \
  --matrix-plan-dir "$PF_MATRIX_PLAN" \
  --matrix-run-dir "$PF_MATRIX_RUN" \
  --endpoint-registry "$PF_ENDPOINT_REGISTRY" \
  --data-agent-semantic-dir "$PF_SEMANTIC_OUT" \
  --data-agent-semantic-spec "$PF_SEMANTIC_SPEC"
```

Repeat both `--data-agent-semantic-dir` and
`--data-agent-semantic-spec` once per binding when building a multi-record
bundle.  Their counts and ordering must match; duplicates are refused.  This
is only a posthoc association of independently generated evidence, never a
claim that FlowMesh scheduled the semantic call.

## What remains before policy evaluation

This slice proves that a frozen real representation can be routed through the
Data Agent, validated, consumed by the simulator's vision-capable executor,
and scored, while being associated unambiguously after the fact with existing
infrastructure evidence.  It does not prove a FlowMesh-scheduled semantic
route, establish container runtime code provenance, provide a physical or
simulated monetary cost basis, qualify as an AWM/OED input, or make the old
64-trial matrix a scientific semantic or cost experiment.

Before AWM/OED can consume simulator evidence, a later version must:

1. make Data Agent access and semantic inference operations inside the same
   FlowMesh-submitted physical DAG rather than a host-side follow-up;
2. use real object/representation identities consistently across that DAG;
3. run comparable candidate designs on a frozen multi-workload cohort;
4. measure a defensible cost basis instead of simulator rate-card values; and
5. freeze the candidate policy, baseline, success margin, and cost threshold
   before collecting confirmatory outcomes.
