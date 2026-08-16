# Phase B Small Confirmatory Experiment

## Status and Scope

This is a small confirmatory run, not the paper's final causal evaluation. It
uses 48 sessions to test whether the Phase B fixture signals survive across
independent public video objects:

```text
8 workload-object blocks
  x 2 physical designs
  x 3 digest quote levels
  x 1 nominal latency multiplier
= 48 sessions
```

Fixing the latency multiplier at `1.0` spends the limited session budget on
eight independent objects instead of repeating three objects across additional
latency cells. The completed 54-session Phase B fixture remains the engineering
manipulation check for the `0.5x`, `1x`, and `2x` latency implementation. This
small run cannot by itself establish quoted-price sufficiency (P4).

## Frozen Dataset Slice

The eight workloads are selected from the NExT-QA validation split. Each uses a
different video and one published multiple-choice question:

- three temporal or response questions;
- three causal questions; and
- two descriptive questions.

The workload IDs, video IDs, questions, options, accepted answer strings, seeds,
and randomized trial order are declared in
`configs/phase_b_confirmatory_small.json`. Do not change them after the first
trial is submitted. Cite the NExT-QA paper and repository in any report that
uses these results.

## Representation Freeze Requirement

Before running, obtain the eight source videos through the official NExT-QA /
NExTVideo mapping and create these files for every object under the external,
git-ignored directory `data/phase_b_confirmatory_small/<object-id>/`:

- `sampled_frames.json`: a fixed-rate, time-indexed set of frame observations;
- `multimodal_digest.txt`: a question-independent temporal digest.

The representation generator must receive the video but not the selected
question, answer, answer index, or options. Generation settings must be the
same for all eight objects. Human quality control may reject a corrupt object,
but the replacement rule and replacement must be recorded before any
Pathfinder condition is run.

Freeze a SHA-256 inventory of all 16 representation files, the object catalog,
the pilot configuration, and the system configuration. Store that inventory
with the experiment output. Do not commit source videos, credentials, or model
tokens.

The repository generator is intentionally unable to read the pilot or QA
configuration. It accepts only the frozen video directory, an empty output
directory, sampling parameters, and an OpenAI-compatible vision endpoint. Keep
its credentials in an external mode-`600` environment file:

```text
PATHFINDER_PREP_LLM_BASE_URL=https://lum.id/llm/v1
PATHFINDER_PREP_LLM_MODEL=<vision-capable-model>
PATHFINDER_PREP_LLM_API_KEY=<from-approved-secret-source>
```

After installing the `data-prep` optional dependencies, generate the corpus
with:

```bash
set -a
. "$HOME/.config/pathfinder/representation-prep.env"
set +a

PYTHONPATH=. python -m pathfinder.video_prep \
  --video-dir "$PF_NEXTQA_ROOT/videos" \
  --output-dir data/phase_b_confirmatory_small \
  --probe-only

PYTHONPATH=. python -m pathfinder.video_prep \
  --video-dir "$PF_NEXTQA_ROOT/videos" \
  --output-dir data/phase_b_confirmatory_small
```

The generator refuses an existing output directory and builds into a temporary
staging directory before publishing the complete 16-file corpus and
`generation-manifest.json` atomically.

## Pre-Registered Estimands

The independent analysis unit is the workload-object block, not an individual
FlowMesh session.

### Primary bundled-design contrast

Compare the deployed-design cells `D_remote_digest/digest_high` and
`D_local_digest/digest_low`, paired within object. These cells represent the
remote incumbent at digest price `6` and the materialized local design at
digest price `2`. The primary outcome is whether `multimodal_digest` was
selected; task success and realized service cost are co-primary descriptive
outcomes. Report the paired access-rate difference, discordant-pair counts, and
the exact two-sided sign test.

This bundled contrast estimates the response to the complete deployed design;
it does not identify price and placement effects separately.

### Affordability mechanism check

Compare `digest_low` with `digest_unaffordable`, paired within object and
physical design. The outcome is whether `multimodal_digest` was selected. The
registered direction is lower digest access under `digest_unaffordable`.
Report the paired difference, discordant-pair counts, and exact two-sided sign
test. With eight independent blocks, only a large and consistent effect can be
detected; eight discordant pairs all in the registered direction give a
minimum two-sided sign-test p-value of `0.0078125`.

### Secondary within-budget quote contrast

Compare `digest_low` with `digest_high` in the same paired manner. Phase B did
not show an aggregate difference between these affordable quotes, so this is a
registered secondary test, not a result to tune after observing this run.

### Quote-fixed physical negative control and construct check

At a fixed quote, compare `D_remote_digest` with `D_local_digest`. Whenever the
digest is accessed, verify:

- location is `remote_digest_service` versus `local_nvme`;
- Data Agent service latency changes in the registered direction;
- realized digest cost is `1.6` versus `0.2`; and
- telemetry is complete.

Agent round-trip `felt_latency_ms` is reported separately and is not the
physical-path manipulation metric.

The current `list_offers` tool deliberately does not expose location or
expected latency to the Agent. Therefore quote-fixed physical cells are a
negative control for representation choice: they should not exhibit a stable
access difference merely because the hidden service path changed. A systematic
choice difference would indicate uncontrolled run-order, model, or prompt
variation rather than identified latency sensitivity. The service metrics must
still show that the physical intervention occurred.

### Task outcome

Report exact task accuracy by workload type, design, and quote. Because this
run is small, task accuracy and within-budget quote response are descriptive
unless their matched effects are large enough for the registered exact test.

## Validity and Exclusion Rules

- Keep every planned trial in `runs.jsonl`.
- Infrastructure and telemetry failures are not task failures; report them in
  separate counts and rerun only through the pilot runner's resume mechanism.
- A record with incomplete telemetry cannot contribute to cost, byte, or
  service-latency conclusions.
- Do not replace a completed model response, change accepted answers, or
  regenerate one representation after seeing a condition result.
- Require 48 recorded trials, at least 46 completed trials, and complete
  telemetry for every completed access before interpreting scientific effects.
- Treat results as a small confirmatory sample. They do not replace a later
  power-based, larger multi-object evaluation or the reduced-oracle, AWM, and
  OED gates.

## Repository Inputs

- Pilot: `configs/phase_b_confirmatory_small.json`
- System: `configs/phase_b_confirmatory_small_system.json`
- Data Agent manifest:
  `configs/phase_b_confirmatory_small_data_agent_manifest.json`
- Object catalog:
  `configs/phase_b_confirmatory_small_object_catalog.json`
