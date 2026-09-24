# ATP-Hard 8-video, 5-question optimization-space experiment

This protocol is frozen before any route result for the selected holdout
questions is inspected. It is a proposal-development experiment, not a
population-level accuracy estimate.

## Public cohort

- Eight distinct validation videos and five ATP-Hard questions per video:
  40 questions total. Each question has an NExT-GQA grounding annotation;
  annotations are used only for eligibility, not as route inputs.
- Development: `7288107396` plus `7059877301`. The former is previously
  exposed and is never described as unseen. The latter is the lowest SHA-256
  rank under `seed|development-video|video` among the eligible unexposed
  five-question videos, excluding the already selected test video.
- Sealed test: the first six eligible videos under the existing
  `seed|video|video` rank, excluding development. The first is the existing
  outcome-blind test selection `4013751996`; no test outcome is used.
- Five questions per video: at least one causal and two temporal by the
  existing question hash rank, then two further eligible questions by that
  same rank. Both development and test use the same rule except that the
  two previously observed development questions remain required.
- Complete original MP4 size must be 1.5--7 MB and match the pinned archive
  size and CRC-32. Video IDs, questions, source digests, and media SHA-256s
  are recorded by the public cohort builder. Hidden answer values are never
  exported into the public selection.

## Collection and evaluation

- Each question receives the existing ten route observations: N7 and N8
  each execute raw, indexed, derived, cache miss, and cache hit. There are
  400 planned observations but only 40 questions and eight independent
  video units. Cache miss/hit observations are controls, not ten independent
  policy actions.
- Use the source-bound multi-question ten-route plan in its deterministic
  interleaved order. Retain the original per-question cache-pair semantics;
  do not label an offline shared-cache episode as a live cross-question hit.
- Run and verify development first. Freeze any public-feature baseline rule
  and thresholds from development only, before executing or inspecting test
  outcomes. Evaluate it alongside fixed always-R, always-D, always-I, and
  always-DC policies on both nodes. The per-question best action is a
  diagnostic upper bound, never an implementable policy baseline.
- Report verified task correctness, unavailable/provider-refused outcomes,
  complete or partial list-price cost, route latency, and N7/N8 identity
  separately. A policy comparison uses exact measured action/state rows;
  unsupported cache states remain unsupported rather than synthesized.
- Build/API/VM costs are separated into video-level one-time construction,
  question-level query embedding/projection, per-route N6 usage, and shared
  infrastructure time. Charge a shared caption build once per video. Show
  cold-start and warm-reuse totals, plus observed per-video prefixes at
  Q=1, Q=3, and Q=5 in the frozen within-video order. Q>5 is extrapolation,
  not an observed quality or cache result.
- A path-choice opportunity is reported only when verified actions differ
  in correctness and/or non-dominated quality--cost trade-offs under the
  full declared cost boundary. If always-derived dominates the sealed set,
  report that result without replacing videos or questions.

Pre-submit gates, failure handling, source-byte discipline, and evidence
sealing follow `EXPERIMENT_OPERATIONS_RUNBOOK.md`. No route, FlowMesh, or
answer-generation request is part of freezing this protocol.
