# Ordered Development Checklist for Pathfinder

## 1. Implementation Order

This checklist follows the revised performative-physical-design direction.
Every gate can narrow or stop later work.

```text
F0 formal-model audit
  -> D0 task/access contract and environment
  -> D1 causal access-response harness
  -> G0 performative-premise gate
  -> D2 representation catalog and session state
  -> D3 Data Agents and physical operations
  -> D4 static Designs A/B/C/D and restoration
  -> G1 reduced oracle and lock-in gate
  -> D5 Adaptive Workload Model
  -> D6 OED Commit/Reveal/Hold
  -> D7 escalation and candidate expansion
  -> D8 adaptation, governance, and hardening
  -> D9 full evaluation and artifact release
```

The critical path is to prove that physical design causally changes agent
access before building a sophisticated controller.

## F0. Formal-Model Audit

### Goal

Turn the paper skeleton's formal claims into precise, implementation-aligned
definitions.

### Tasks

- [ ] Define `D = (M, L, E)`, `W(D)`, `Phi(D)`, transition cost, and
  `Gain(D_t -> D')`.
- [ ] Separate performative optimum, performative stability, and OED's
  candidate-relative certificate.
- [ ] State the history, ambiguity set, and exact AWM assumption families.
- [ ] Repair the non-identifiability construction so every residual demand
  function respects the declared access budget over the full price domain.
- [ ] Declare ex ante finite `P_qv` universes before defining Reveal bounds.
- [ ] Check the general `sum_qv |P_qv|`, pair-canonical `|Q||V|`, and
  simultaneously canonical `|V|` cases separately.
- [ ] State tightness relative to the ambiguity set.
- [ ] Separate loss along the certified safe sequence from exploration loss.
- [ ] Define the initial design and transition-counting unit for lower bounds.
- [ ] Complete the task, access, and success mapping for the hardness reduction.
- [ ] Mark each theorem as draft, checked, or experimentally motivated.

### Gate F0

Each theorem has a definition/assumption/claim sheet. Unresolved proof items are
explicitly excluded from the current guarantee rather than silently assumed.

## D0. Task/Access Contract and Environment

### Goal

Verify the execution environment and define the observable interface between a
physical design and an agent session.

### Tasks

- [ ] Inventory object storage, nodes, CPU/GPU, RAM, NVMe, and network links.
- [ ] Verify FlowMesh task placement, lifecycle events, and propagation of
  `design_id`, `design_epoch`, `session_id`, `task_class_id`, and `trial_id`.
- [ ] Select one video corpus and deterministic representation graph.
- [ ] Define the first two or three task classes and terminal success metrics.
- [ ] Define representation substitution groups.
- [ ] Define per-task access budgets and affordability semantics.
- [ ] Declare the finite access-price universe for each task-class and
  representation pair.
- [ ] Keep `p_qv`, felt latency, and `realized_cost_qv` separate in every
  schema.
- [ ] Define queued agentic sessions as endogenous.
- [ ] Define training and analytics as fixed contributors for the first pilot.
- [ ] Define externally supplied governance policies and attestations.
- [ ] Select catalog, RPC, transfer, and telemetry interfaces.
- [ ] Record every unverified infrastructure assumption and fallback.

### Recommended Code Boundaries

```text
controller/
  catalog/
  candidate_generator/
  awm/
  oed/
  escalation/

agent/
  replica_manager/
  transform_executor/
  access_resolver/
  session_manager/
  telemetry/

common/
  representation/
  physical_design/
  task_access/
  governance/
  protocol/

adapters/
  flowmesh/
  object_store/
  policy_authority/

workloads/
  video_agent/

experiments/
  causal_harness/
  reduced_oracle/
  runners/
  analysis/
```

### Gate D0

The harness can offer a versioned set of representation choices and prices to a
task class, and every session reaches a logged terminal outcome.

## D1. Causal Access-Response Harness

### Goal

Estimate whether agent behavior changes when physical access changes.

### Tasks

- [ ] Hold model, prompt, task distribution, decoding policy, and seeds fixed.
- [ ] Implement class-specific quote intervention within each declared
  `P_qv`.
- [ ] Log every offered representation, `p_qv`, budget, selection, and
  rejection.
- [ ] Log realized latency, bytes, compute, cost, success, quality, and session
  value.
- [ ] Counterbalance intervention order and cold/warm state.
- [ ] Implement matched real physical interventions.
- [ ] Compare quote-induced and physically induced responses.
- [ ] Match real and injected interventions on `p_qv`, not realized cost.
- [ ] Run an advertised-price × felt-latency factorial experiment.
- [ ] Test quoted-price sufficiency with a pre-registered equivalence margin.
- [ ] Estimate access probability and `epsilon_qv` by task class,
  representation, and quote.
- [ ] Test group-total monotonicity.
- [ ] Test representation-success monotonicity.
- [ ] Check whether training/analytics remain stable under design changes.
- [ ] Deliberately allow a fraction of sessions to re-arrive under cheaper
  access and measure exogeneity-violation tolerance.
- [ ] Repeat trials and quantify uncertainty.

### Gate G0: Performative Premise

Proceed with Pathfinder's headline only if:

1. at least one important task class has repeatable access elasticity;
2. the response changes session value, not merely an internal read count;
3. real design interventions broadly reproduce the quote response; and
4. quoted price is a sufficient mediator over the declared operating range;
   and
5. a defensible subset of AWM assumptions survives falsification tests.

If not, revert to fixed-workload physical design or narrow the task/model scope.

## D2. Representation Catalog, Task Classes, and Session State

### Goal

Answer which representation exists, how it was produced, who can access it,
what alternatives were offered, and what outcome followed.

### Tasks

- [ ] Implement logical dataset and version.
- [ ] Implement representation DAG, shard manifests, and transformation
  fingerprints.
- [ ] Implement replicas with node, tier, state, checksum, lease, and policy
  version.
- [ ] Implement `TaskClass` and versioned success contract.
- [ ] Implement versioned substitution groups.
- [ ] Implement `SessionArtifact` with temporary, reusable, and promoted states.
- [ ] Implement `DesignObservation` linked to history, task class, choices,
  quote profile, outcome, and realized cost.
- [ ] Keep semantic identity, access profile, and governance policy versions
  separate.
- [ ] Implement atomic publication after checksum/manifest validation.
- [ ] Implement compatibility, affordability, and governance validation.
- [ ] Implement lineage invalidation, revocation, and erasure propagation.
- [ ] Recover catalog state and detect orphan artifacts after restart.

### Tests

- [ ] Compatible sessions reuse one deterministic representation.
- [ ] Code/model/parameter/randomness incompatibilities are rejected.
- [ ] Unaffordable representations are never returned by the resolver.
- [ ] Policy-ineligible access is rejected even for a byte-compatible local
  replica.
- [ ] Failed or partial writes never become valid.
- [ ] A restored probe retains its observation and correctly classifies its
  artifacts.

### Gate D2

The complete causal chain from offered access to terminal session value is
replayable from catalog and observation records.

## D3. Data Agents and Physical Operations

### Goal

Execute the materialization, layout, and execution components of
`D = (M, L, E)`.

### Tasks

- [ ] Agent registration, heartbeat, capabilities, zone, and trust labels.
- [ ] Authenticated plan capabilities and idempotent operations.
- [ ] Capacity reservations, leases, pins, and active-reader references.
- [ ] `ensure_replica`, `run_transform`, `replicate`, `serve`, `stage`,
  `pin`, `evict`, and `invalidate`.
- [ ] Origin-to-agent, peer-to-agent, local-NVMe, and transform paths.
- [ ] Resolve every logical request through the access-path resolver.
- [ ] Enforce affordability and governance at serve time.
- [ ] Emit task-class, quote, path, realized-cost, and outcome telemetry.
- [ ] Prevent partial publication and eviction with active readers.
- [ ] Recover local replica state after restart.

### Gate D3

The same logical request is served correctly through origin, local, peer, and
transformed paths, with complete access and cost attribution. Constructed
illegal and unaffordable requests are rejected at the agent boundary.

## D4. Static Designs, Transitions, and Restoration

### Goal

Deploy declarative designs and restore a certified incumbent before adding AWM
or OED.

### Tasks

- [ ] Implement the `PhysicalPlan` schema corresponding to `D = (M, L, E)`.
- [ ] Bind task classes, quote profiles, governance version, and observation
  specification.
- [ ] Implement plan validation and machine-readable rejection reasons.
- [ ] Compile transitions and restoration plans.
- [ ] Implement `PREPARING -> CANARY -> ACTIVE -> DRAINING`.
- [ ] Bind admitted sessions immutably to one epoch.
- [ ] Define a certified `D_safe`.
- [ ] Charge forward, foreground-disruption, and restoration costs.
- [ ] Retain observations after probe restoration.
- [ ] Promote probe artifacts only through an explicit compatibility and value
  decision.

### Mandatory Static Designs

```text
Design A: compressed video -> local decode/sample -> cheap embedding access
Design B: shared sampled frames/embeddings on NVMe -> agent sessions
Design C: consumer-local transform from a shared parent representation
Design D: expensive structured multimodal digest -> agent sessions
```

Design D must initially expose the censored/expensive access path needed to
study performative lock-in.

### Gate D4

Designs A–D produce valid task inputs, distinct access profiles, and
explainable session outcomes. Any probe can restore `D_safe` without losing the
observation or corrupting active sessions.

## G1. Reduced Oracle and Lock-In

### Goal

Establish the central failure mode on a fully enumerable instance.

### Tasks

- [ ] Enumerate every feasible reduced design under `D_gov`.
- [ ] Deploy every design and measure `W(D)` and `Phi(D)`.
- [ ] Implement naive read-react-materialize.
- [ ] Construct the predicted censoring loop.
- [ ] Show whether an unobserved expensive representation is truly valuable.
- [ ] Include all transition and restoration costs.
- [ ] Declare candidate-generation output and `G_other`.
- [ ] Freeze an oracle dataset for AWM/OED validation.

### Gate G1

Continue to the full controller only if the lock-in is reproducible with a real
agent and physical access path, and a superior design remains after complete
cost accounting.

## D5. Adaptive Workload Model

### Goal

Maintain a history-indexed, coupled feasible set of design-dependent workload
responses.

### Tasks

- [ ] Define observation-history and assumption-version schemas.
- [ ] Version `P_qv`, quoted-price sufficiency, and latency-reservation
  contracts.
- [ ] Encode affordability gates.
- [ ] Encode allowed own-price, group-total, substitution, and success
  constraints.
- [ ] Solve lower/upper access, value, and gain queries over the full coupled
  feasible set.
- [ ] Avoid independent endpoint substitution unless proved equivalent.
- [ ] Validate coverage against the reduced oracle and held-out sessions.
- [ ] Report width and active constraints for every bound.
- [ ] Maintain time-uniform confidence events for demand, realized-cost boxes,
  transition costs, and probe-window loss.
- [ ] Detect assumption violations and invalidate affected certificates.
- [ ] Implement assumption-family and trivial-box ablations.

### Gate D5

AWM achieves declared coverage and produces decision-relevant bounds that are
tighter than assumption-free and independent-box alternatives.

## D6. Optimistic Elastic Design

### Goal

Make explicit Commit, Reveal, or Hold decisions while preserving `D_safe`.

### Tasks

- [ ] Classify candidates into `G_cert`, `G_probe`, and `G_other`.
- [ ] Track the highest affordable observed price for every `(q,v)`.
- [ ] Select simultaneously canonical probes first, pair-canonical probes
  second, and other budget-feasible probes last.
- [ ] Record which Reveal tier was taken and charge non-canonical probes
  against `sum_qv |P_qv|`.
- [ ] Compute conservative Commit gain including transition cost.
- [ ] Compute optimistic Reveal value including forward, foreground, and
  restoration cost.
- [ ] Commit only when the candidate is certified to improve over `D_safe`.
- [ ] Reveal only when the probe has positive decision value under the declared
  ambiguity set.
- [ ] Restore `D_safe` after Reveal unless a separate Commit certificate exists.
- [ ] Retain the new observation and re-run AWM.
- [ ] Implement explicit Hold/certificate-limited/budget-limited reasons.
- [ ] Track the safe sequence separately from probe executions.
- [ ] Compute `delta_t` from candidate, incumbent, and transition-cost widths.
- [ ] Prove termination from finite `P_qv` and the finite design domain rather
  than assuming a positive minimum excursion cost.
- [ ] Compare against passive, random, bandit, Bayesian, and black-box policies.

### Gate D6

On the reduced oracle and held-out runs, OED improves safe design quality or
reaches the oracle decision at lower total exploration cost than equal-budget
baselines. Claims remain candidate-relative when `G_other` is nonempty.

## D7. Escalation and Candidate Expansion

### Goal

Respond to structural ambiguity that ordinary probes cannot resolve.

### Tasks

- [ ] Escalate from microbenchmarks to sampled materialization.
- [ ] Escalate to broader task slices or representation coverage.
- [ ] Escalate to alternative quote profiles from the declared alphabet.
- [ ] Expand/refine task classes or substitution groups when assumptions fail.
- [ ] Expand candidate generation when `G_other` risk is high.
- [ ] Permit an external review request for an unsupported policy/semantic
  choice.
- [ ] Record why escalation was chosen and what uncertainty it can resolve.
- [ ] Refuse escalation whose robust value does not cover its cost.

### Gate D7

Every escalation either tightens a named decision bound, discovers an
assumption violation, expands the candidate certificate, or is explicitly
refused.

## D8. Adaptation, Governance, and Hardening

### Tasks

- [ ] Detect sustained task-mix, resource, and prediction drift.
- [ ] Add hysteresis and minimum stable windows.
- [ ] Revalidate on policy, attestation, retention, and erasure changes.
- [ ] Block stale epochs and revoked access.
- [ ] Recover controller and agents and reconcile orphan artifacts.
- [ ] Stress-test shared-resource interference.
- [ ] Provide reduced-overhead telemetry.
- [ ] Back up/restore catalog and observations.
- [ ] Run revocation and erasure correctness tests separately from performance
  adaptation.

### Gate D8

The system adapts within budget without oscillating and never serves a newly
unauthorized request after a policy change.

## D9. Full Evaluation and Research Artifact

### Tasks

- [ ] Complete the primary video workload and all falsification gates.
- [ ] Add one secondary modality only after the primary result is stable.
- [ ] Add fixed training and analytics consumers.
- [ ] Implement all physical-design and performative-controller baselines.
- [ ] Complete all AWM/OED and `M/L/E` ablations.
- [ ] Report negative regimes and assumption violations.
- [ ] Report candidate-pool coverage and oracle decomposition.
- [ ] Report quoted versus realized cost agreement.
- [ ] Report transition, probe, restoration, and telemetry overhead.
- [ ] Freeze code, data, prompts, models, seeds, policies, and configurations.
- [ ] Provide one-command reduced-oracle and end-to-end runners.
- [ ] Release trace schemas, oracle traces, and analysis scripts.

### Final Success Conditions

- [ ] Design causally changes agent access and session value.
- [ ] Read-react-materialize exhibits real lock-in.
- [ ] AWM is both empirically sound and more informative than trivial bounds.
- [ ] OED beats equal-budget exploration baselines after complete cost
  accounting.
- [ ] Joint `M/L/E` matters beyond a conventional cache choice.
- [ ] Certificate scope and `G_other` are reported honestly.
- [ ] Every constructed illegal operation is rejected at planning and agent
  boundaries.

## 2. Immediate Next Steps

If implementation has not begun:

1. Complete the F0 definition and theorem audit.
2. Write the D0 task-class, access-budget, substitution-group, and finite-price
   contracts.
3. Build the D1 resolver harness around one video corpus and two task classes.
4. Run the G0 causal pilot before implementing the catalog or OED.

The first research-critical code is the causal access-path resolver and logging
harness. The optimizer becomes justified only after that harness shows that the
workload actually depends on the physical design.
