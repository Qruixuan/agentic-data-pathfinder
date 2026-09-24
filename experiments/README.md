# Experiment tools

Read [the operations runbook](../EXPERIMENT_OPERATIONS_RUNBOOK.md) before
freezing or verifying inputs, deploying services, or submitting experiments.
These tools run from the repository checkout with its optional dependencies;
they are not installed by the `pathfinder` Python package.

## Current entry point

Use `python -m experiments.batch --help`. One entry point dispatches by the
frozen configuration schema; do not create a new runner for each date/cohort.

| Family | Implementation | Configuration example |
| --- | --- | --- |
| R/D/DC/I interleaved multi-question | `interleaved_batch.py` | `multiq_pilot_20260924/configs/dev-24.draft.json`, `sealed-28.draft.json` |
| Existing ten-case D0--D7 smoke | `ten_route_batch.py`, delegating to canonical smoke APIs | `configs/ten-route.draft.example.json` |

The interleaved examples bind **historical** plans and run identities. Copy
the draft and supply fresh, canonically verified input packages for a new
experiment. The ten-route example contains placeholders and is not runnable
until completed. Both adapters keep the source-bound and deployment gates.

The workflow is `freeze-config`, optionally `freeze-inputs` for not-yet-frozen
downstream inputs, then `check`, `preflight`, `execute`, `verify`. See the
runbook for flags, dependencies, cache isolation, budget and cost recording.
`check` is offline; `preflight` contacts the registry; `execute` submits paid
work. A new output directory alone does **not** make an old plan's run IDs new.
Ten-route output is sealed by its canonical runner; interleaved output uses
`verify --seal` after completion.

Multi-question x ten-route scheduling is not implemented by this cleanup.
The two supported contracts stay separate rather than silently changing the
meaning of cache misses/hits or the evidence format.

## Historical compatibility

The dated `run_24route_pilot.py` / `run_28route_sealed.py` and
`verify_24route_pilot.py` / `verify_28route_sealed.py` are small adapters, not
four independent execution/verification implementations. Their old module
names remain available. Offline verification signatures and historical
result fields remain stable for the cost-audit and replay scripts.

Legacy runner flags still work for offline checks and worker preflight.
Execution now requires `--config-dir` in addition to the old flags; its output
uses the common format and must be verified with `experiments.batch verify`.
With a frozen config, put the baseline path in the config, not in a CLI
override. The historical verifiers continue to accept historical output only.

The remaining dated selectors, preparation/freezing recipes, deployment
records, cost audits and reports are retained as provenance. They encode
different sampling, source-binding or cost-evidence contracts and are not
interchangeable copies. New three-stage binding/DAG/admission freezes should
use `experiments.batch freeze-inputs`; the original staged freezer remains
available to explain/reproduce its historical recipe.

## Local material and cleanup policy

Build snapshots (`.codex_build`, `.qruix_smoke_stage`), local dependency caches,
`artifacts/`, short-path staging `a7/`, and the transferred exact-six receipts
and store (`x6r-20260917`, `x6store-20260917`) are ignored, **not deleted**.
Existing tracked manifests stay tracked. Local result directories under the
dated pilot are also ignored; reviewed public reports/configs can still be
committed explicitly. Never `git add -f` an evidence directory without a
content review for credentials and hidden labels.

Do not bulk-delete dated scripts or frozen directories just because their
names look alike. First check imports, canonical bindings, deployment labels
and rollback references. Preserve old evidence byte-for-byte. Never use a
new verifier to relabel an incompatible historical profile as a current run.

## Focused offline regressions

```text
python -m unittest tests.test_reusable_interleaved_batch tests.test_reusable_ten_route_batch tests.test_legacy_experiment_entrypoints tests.test_simulator_full_flow_local_semantic_smoke tests.test_simulator_full_flow_one_case -q
```

No workflow or LLM is called. Core tests use synthetic fixtures; extra
historical-evidence regressions skip if the operator-only artifact packages
are absent. Those regressions do not replace the canonical source-bound
pre-submit checks in the deployment environment.
