# Multi-Candidate Controlled PPD Experiment v2

## Status and purpose

This document freezes the workload expansion used to test whether the
joint structured AWM v3alpha4 contracts as predicted when the number of
independent workload objects increases from eight to sixteen. Version 1
remains immutable. Version 2 uses a new experiment ID, seeds, object catalog,
materialization root, runtime databases, and output directory.

The v1 observations informed the choice of AWM v3alpha4. Consequently, an
analysis that pools the old and new workload objects is an engineering power
and scaling check, not an independent validation of the model-selection
process. A later paper-grade confirmation should use a wholly fresh workload
set. This experiment still provides real FlowMesh, Agent, Data Agent,
materialization, and artifact-delivery measurements on the controlled
single-node testbed. It does not establish distributed placement cost.

## Frozen selection rule

The annotation source is the official NExT-QA validation CSV at repository
commit `2432e9724f88ed9f40010e2989f104570a91de4e`. Its SHA-256 is
`43198bdef8436b8d64a9b75d846b0987c10cbf94ebf4be325c4a4e54634d66b8`.
The machine-readable selection record is
`configs/multi_candidate_formal_v2_workload_selection.json`.

After excluding all eight v1 video IDs, the selector scans the pinned CSV in
row order and takes the first unique videos satisfying these quotas:

| Group | NExT-QA subtype quota | Added video IDs |
|---|---|---|
| temporal | 2 TN, 1 TC | 6356067859, 5296635780, 5735711594 |
| causal | 3 CW | 8132842161, 3462517143, 5026660202 |
| descriptive | 1 DC, 1 DO | 4942054721, 5840177726 |

This mirrors the subtype structure of v1 and prevents outcome-driven
question selection. Each video contributes exactly one independent workload.
Questions, answer options, accepted answers, row metadata, and the pinned
video archive revision are recorded before any v2 workflow is submitted.

## Frozen experiment

The design domain, quotes, controlled latencies, service costs, task value,
transition-cost coefficients, and four repetitions are unchanged. Only the
workload domain and seeds change:

```text
4 designs x 16 workload-object blocks x 4 paired repetitions
= 256 FlowMesh sessions
```

Repetitions 0--1 form the AWM training block. Repetitions 2--3 remain hidden
holdout observations. AWM v3alpha4 uses sixteen independent workload-cluster
means, five preregistered joint utility bins, and the same complete paired
repetition reduction as v1. The main diagnostic is interval contraction;
a Commit is not required and must never be forced by changing the inputs.

## Data preparation boundary

Raw videos and generated representations remain repository-external or under
the ignored directory `data/multi_candidate_formal_v2/`. The Data Agent
expects every one of the sixteen objects below:

```text
data/multi_candidate_formal_v2/source/<object-id>/sampled_frames.json
data/multi_candidate_formal_v2/source/<object-id>/multimodal_digest.txt
```

Copy the already validated v1 representations for the original eight
objects into this source root. Download the eight added videos from the
pinned `rhymes-ai/NeXTVideo` revision in the selection record and generate
their representations with exactly the v1 procedure: the same frame count,
sampling rule, vision model, prompts, and digest construction. Record video
and representation hashes plus the generation environment outside Git.
Manual review may reject corruption or a mismatched video, but it must not
rewrite a representation after observing an Agent answer.

Do not run the Oracle until all thirty-two representation files exist, are
non-empty, and have been hashed. Never commit raw videos, credentials,
worker configuration, runtime databases, or generated experiment outputs.

## Frozen inputs

- Selection: `configs/multi_candidate_formal_v2_workload_selection.json`
- Workloads: `configs/multi_candidate_formal_v2_pilot.json`
- System: `configs/multi_candidate_formal_v2_system.json`
- Data Agent: `configs/multi_candidate_formal_v2_data_agent_manifest.json`
- Object catalog: `configs/multi_candidate_formal_v2_object_catalog.json`
- Reduced Oracle: `configs/multi_candidate_formal_v2_oracle.json`
- Full-observation AWM: `configs/multi_candidate_formal_v2_awm_v3alpha4_power_diagnostic.json`
- OED-view AWM: `configs/multi_candidate_formal_v2_awm_v3alpha4_oed.json`
- OED: `configs/multi_candidate_formal_v2_oed_v3alpha4.json`

## Evaluation gates

1. All 256 planned sessions are recorded, with infrastructure failures kept
   separate from task failures.
2. Research telemetry is complete and every selected frame artifact is
   actually downloaded.
3. The safe design is restored and no owned materialization remains.
4. Holdout containment holds and the false-safe Commit count stays zero.
5. Report v3alpha4 width for each candidate and compare the ratio with v1.
   The preregistered power diagnostic predicts roughly half-width near
   fourteen independent workloads; disagreement is a result, not grounds to
   retune the bins.
6. Run OED only after the Oracle integrity gate passes. Report Reveal order,
   final action, certificate-limited stopping, and holdout regret even when
   no candidate can be safely committed.
