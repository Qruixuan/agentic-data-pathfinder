# Local Container Semantic Execution

This is the first semantic vertical slice for the eight-node local Pathfinder
environment.  It is intentionally separate from the infrastructure-only
runner: the latter remains the source of physical I/O, network, cache, and
queue measurements, while this command validates that a real stored text
representation can produce a scored task answer through a container-side,
OpenAI-compatible remote LLM call.

## What is exercised

1. A selected source container reads a bounded, UTF-8 representation from a
   read-only mount and delivers it to the selected executor over the Docker
   network.
2. The selected container executor (normally `N6`) builds the prompt from that
   representation and a frozen multiple-choice question.
3. That container calls the configured remote LLM API.
4. Pathfinder independently reads the same frozen file only to bind its hash,
   then records the answer, hashes of the prompt and representation, and
   a frozen single-option score.  It never writes the prompt, representation body, or
   API key to the result package.

The Compose file contains only the *names* of four runtime environment
variables; it does not contain their values.  Only the chosen semantic
executor inherits them.

## Explicit limitations

This is a semantic vertical slice with real source-container →
executor-container data delivery.  It validates representation → container
network route → LLM → scoring, but does not yet prove that this semantic
source route is equivalent to every storage/cache/network operation in the
separately frozen infrastructure plan.  The generated output labels this
limitation literally and is not eligible for scientific claims.

It supports the current textual Pathfinder representations:

- `multimodal_digest.txt`;
- UTF-8 `sampled_frames.json` containing sampled-frame descriptions.

Frame-bundle image inputs and artifact-backed, route-coupled transfer are
deliberately future work.

## Build a semantic-enabled Compose package

Use a new output path: the existing package cannot be modified in place.

```powershell
$env:PYTHONPATH = "."
& .\.venv\Scripts\python.exe -m pathfinder `
  build-local-container-compose `
  --container-plan-dir <container-plan-dir> `
  --output-dir <new-semantic-compose-dir> `
  --host-port-base 19080 `
  --semantic-executor-node N6 `
  --semantic-artifact-source-node N3 `
  --semantic-artifact-source-node N4
```

Set the remote API variables in the same terminal only before starting the
new Compose project.  Do not add them to a checked-in file or the generated
package.

```powershell
$env:PATHFINDER_SEMANTIC_LLM_BASE_URL = "https://<provider>/compatible-mode/v1"
$env:PATHFINDER_SEMANTIC_LLM_MODEL = "<text-capable-model>"
$env:PATHFINDER_SEMANTIC_LLM_API_KEY = Read-Host "API key"
$env:PATHFINDER_SEMANTIC_LLM_TIMEOUT_SECONDS = "180"
$env:PATHFINDER_SEMANTIC_ARTIFACT_ROOT = "D:\pathfinder-data\frozen-representations"

$env:PATHFINDER_REPO_ROOT = (Get-Location).Path
docker compose -f <new-semantic-compose-dir>\compose.yaml up -d --build
```

`N6` is semantic-enabled only when its `/healthz` response contains both
`"semantic_quality_enabled": true` and `"semantic_llm_configured": true`.
Each declared source node must report `"semantic_artifact_serving": true`.

## Semantic workload manifest

Create a separate operator-local JSON file.  All representation paths are
relative to `--representation-root`; absolute paths and `..` are refused.

```json
{
  "schema_version": "pathfinder.local-container-semantic-workloads/v1alpha1",
  "semantic_run_id": "local-semantic-smoke-v1",
  "success_scoring_rule": "multiple-choice-option-id-canonical-match-v1",
  "semantic_executor_node_id": "N6",
  "workloads": [
    {
      "semantic_trial_key": "local-semantic-smoke-v1|video-1|D2|r0000",
      "workload_id": "video-1-question",
      "object_id": "video-1",
      "design_id": "D2",
      "representation_id": "multimodal_digest",
      "representation_path": "nextqa-val-123/multimodal_digest.txt",
      "source_node_id": "N3",
      "question": "Which option best describes the main action?",
      "answer_options": [
        {"option_id": "A", "text": "..."},
        {"option_id": "B", "text": "..."}
      ],
      "correct_answer_id": "B"
    }
  ]
}
```

The canonical marker rule is deliberately narrow: `C`, `[C]`, `(C)`, and their
full-width bracket equivalents all mean the one option `C`.  An explanation
such as `"The answer is C"`, trailing punctuation such as `C.`, or multiple
options still scores false.  The legacy exact-match rule remains available for
older frozen pilots and is not changed retroactively.

## Run and verify

The command can incur remote-provider usage.  Start with one or two rows and
always use a fresh output directory; semantic calls are not automatically
replayed or resumed.

```powershell
& .\.venv\Scripts\python.exe -m pathfinder `
  run-local-container-semantic-execution `
  --compose-package-dir <new-semantic-compose-dir> `
  --semantic-workload-manifest <operator-local-manifest.json> `
  --representation-root <frozen-representation-root> `
  --output-dir <fresh-semantic-output-dir>

& .\.venv\Scripts\python.exe -m pathfinder `
  verify-local-container-semantic-execution `
  --output-dir <fresh-semantic-output-dir>
```

The output contains `semantic_records.jsonl`, a manifest, and checksums.  It
records answer/score/model/prompt hash/representation hash, source node, and
exact source-to-executor delivery bytes; it excludes the prompt text,
representation text, and API key.

## Aligning a legacy strict score without another LLM call

Do not modify an already-generated exact-match output.  If a development smoke
returned a single displayed marker such as `[C]`, create a separate canonical
workload manifest with `multiple-choice-option-id-canonical-match-v1`, then
derive an immutable score-alignment artifact:

```powershell
& .\.venv\Scripts\python.exe -m pathfinder `
  align-local-container-semantic-scores `
  --semantic-output-dir <legacy-exact-output-dir> `
  --canonical-workload-manifest <canonical-workload-manifest.json> `
  --output-dir <fresh-score-alignment-dir>

& .\.venv\Scripts\python.exe -m pathfinder `
  verify-local-container-semantic-score-alignment `
  --output-dir <fresh-score-alignment-dir>
```

The alignment references the source output hashes, preserves its raw-answer
hash, records both old and new scores, and makes no LLM or container request.
It is a protocol correction for development evidence, not a retroactive change
to any previously frozen confirmatory pilot.
