# Full-Flow Simulator and UpCloud Migration Contract

Status: the integrated local full-flow path is **code-ready and test-covered**.
It now includes authenticated N3/N4 exact-artifact HTTP preflight,
independently startable N7/N8 semantic route services, ten representative
semantic smokes, a smoke-gated and source-bound 64-trial runner, N5-to-N4
frame-bundle and digest provisioning conformance, a durable 36-object / 72-
operation bulk provisioning coordinator, a live-publication N4 serve gate, a
generic semantic evidence-to-AWM/OED adapter with privileged N1 score replay,
an offline neutral AWM/OED consumer that refuses to invent monetary cost, and
a candidate-wide W4 route-conformance coordinator with a strict local
component execution boundary plus a globally serial, worker-pinned 16-task
FlowMesh wrapper.

That readiness is not a live-execution claim. The previously recorded
64-trial run remains the deterministic infrastructure matrix. The new ten
semantic smokes and semantic 64-trial matrix have not yet been executed as an
operator-controlled Compose/FlowMesh run. W4 in that matrix remains the
historical multiple-choice local-conformance placeholder; its separate public
retrieval runtime is code-ready only.

## Scientific boundary

There are two separate evidence layers:

| Evidence layer | Current result | Valid conclusion |
| --- | --- | --- |
| FlowMesh infrastructure matrix: recorded live evidence | W1-W4 x D0-D7 x 2 repetitions: 64 trials, 80 workflows, 500 planned operations, 472 executed and 28 inactive conditional operations | FlowMesh orchestration, container routing, dependencies, byte accounting, and configured cache/network branches worked for the deterministic fixture run |
| Integrated local semantic runtime: code and test readiness | N3/N4 authenticated artifact preflight, N7/N8 services, ten representative route smokes, a source-bound 64-trial runner, N1 hidden scoring, and neutral AWM/OED conversion are implemented and focused-tested | The declared local interfaces and fail-closed bindings are implementable; this is not proof of an operator-controlled semantic matrix run |
| N5-to-N4 provisioning: protocol-conformance tests | Frame-bundle and digest paths exercise authenticated N5 execution, exact binding, authenticated compare-and-swap N4 publication, atomic visibility, replay handling, and receipt verification; a durable bulk coordinator covers the exact 36 x 2 operation set | The N5/N4 protocols and recovery rules compose under the tested local conditions; no deployed-container, external-model, latency, cost, or source-authenticity conclusion follows |
| W4 retrieval path: local component and FlowMesh code readiness | A public retrieval task, source-verified N2 lexical ranker and crosswalk, candidate-wide routes, strict 16-trial coordinator, concrete N2/N3/N4/N6/N7/N8 local bindings, complete rankings, component receipt, hidden-relevance evaluator, and a globally serial 16-task FlowMesh wrapper are implemented without exposing labels | Operator-run component/FlowMesh receipts and a source-bound N1 evaluation of that exact run are still required before reporting measured retrieval quality; the historical semantic-matrix W4 cells remain multiple-choice placeholders |

The recorded infrastructure layer used deterministic size-preserving fixtures.
It is infrastructure-conformance evidence, not a measurement of real data
quality, cloud performance, or monetary cost. The other rows describe code and
test readiness, not newly completed operator experiments. None of these layers
is currently eligible for scientific claims.

### Hidden-score guarantees

The N1 boundary provides three different guarantees which must not be
conflated:

- **Structural isolation:** strict schemas and recursive field checks keep the
  hidden reference label out of the public task, logical plan, FlowMesh
  payload, N6 request, and returned evidence. The model's public prediction is
  retained for authenticated N1 replay and may of course happen to be correct.
- **Service authentication:** N1 signs content-bound score evidence with a
  runtime-only HMAC secret, allowing an authorized verifier to detect a
  modified request or result.
- **Archived-score replay:** generic semantic route evidence retains the
  model's public prediction and the public N1 score result, but never the
  hidden answer. Before AWM/OED consumes `task_success`, the bridge requires
  the frozen N1 package plus the runtime-only evidence secret and replays the
  package/HMAC verification. A restamped Boolean is therefore insufficient.
- **One-shot evaluation identity:** the v1alpha2 score request derives an
  `evaluation_unit_id` from the frozen `oracle_id`, `run_id`, and `trial_id`.
  The durable N1 ledger allows an exact request replay but rejects a different
  request ID or prediction for an already-consumed evaluation unit, including
  after restart. This prevents adaptive rescoring of one declared trial.

N7/N8 do not need the hidden package or the score-evidence HMAC secret to
authenticate a score. The optional N1 verification companion accepts the
exact public score request/result pair at `POST /v1/oracle/verify-score`,
checks the package and HMAC inside N1, and returns only a challenge-bound
attestation plus public digests and score claims. Its bearer credential and
endpoint are runtime inputs. Verification challenges are random, recorded in
a bounded durable ledger, and rejected on reuse; malformed, identity-mismatched,
or modified evidence receives only a generic error. Remote deployments require
HTTPS. Plain HTTP is limited to loopback or an explicitly allowlisted simulator
hostname.

Use `N1RemoteScoreEvidenceVerifier` from
`pathfinder.simulator.full_flow_n1_remote_verification` as the `verifier=`
argument to the existing `VerifiedN1HTTPScoringAdapter`. The resulting scorer
is passed unchanged as `SemanticRouteAdapters.scorer`, so
`GenericSemanticRouteCoordinator` and all current local-package defaults remain
unchanged. Only the N1 companion is constructed with `package_dir` and
`evidence_secret`; the remote verifier receives the verification URL, pinned
oracle identity, pinned public-task-set digest, and its runtime bearer token.

The public pre-selection commitment hashes the frozen N1 package and hidden
label set without publishing label values. It can later be opened against the
private package. When published before AWM/OED route selection, it identifies
the selected oracle hash, but it is not an independent timestamp, third-party
approval, or proof that the operator never saw the labels. A confirmatory
benchmark still needs an independently controlled evaluator or equivalent
external protocol.

### Runtime request authentication

Runtime credential values never enter a frozen plan, Compose file, checksum
package, or durable FlowMesh result:

- N6 semantic and artifact endpoints require the bearer token named by
  `PATHFINDER_CONTAINER_NODE_TOKEN`;
- N7 full-flow ingress requires
  `PATHFINDER_FULL_FLOW_INGRESS_HMAC_SECRET`; and
- the FlowMesh submitter computes
  `X-Pathfinder-Full-Flow-HMAC-SHA256` over the canonical request at runtime.

The N7 signature is request-bound, so it is not a reusable bearer credential.
The generated Compose overlay contains environment-variable names and
placeholders only. It renders 12 primary services plus seven required
companion services: N1 verification, N4 publication, N5 digest
materialization, and an independent W4 coordinator/cache pair on each of N7
and N8. Alongside the unified file it freezes one checksum-bound Compose
fragment per root service. A fragment contains only that service and its
transitive health dependencies, so selecting N3 or N4 does not require values
for unrelated N1/N2/N5/N6/N7/N8 variables. Its manifest records the exact
environment-variable names required by each fragment; values remain
operator-local. Compose v1alpha4 also guarantees that every service key used
as an internal Docker DNS name is one lowercase DNS label of at most 63
characters. Existing short names are preserved; an overlong companion name
first drops a redundant `full-flow-` implementation prefix and otherwise uses
a deterministic SHA-256-suffixed truncation. The two W4 caches use dedicated
identities and state namespaces; they cannot silently reuse the historical
semantic-route cache
state. Each W4 coordinator depends on healthy N2/N7/N8 indexes, N3/N4 Data
Agents, N6 inference, and its own cache. Its health endpoint becomes
unavailable if the cache identity or occupancy diverges. These controls
authenticate transport between components; they do not turn local execution
into independent scientific evidence.

N3 and N4 Data Agent fragments mount the complete frozen package root
read-only, not only `config/data-agent-manifest.json`. Their commands use a
fixed container-local manifest path, preserving resolution of the sibling
object catalog and its relative `../artifacts/` references. The effective N4
serve gate therefore rebinds `PATHFINDER_N4_PACKAGE_DIR`; the original
manifest-variable name is retained only as source-contract provenance. Bind
mounts disable automatic host-path creation, each service clears the image's
default entrypoint before executing the bootstrap's complete argv, and the N3
or N4 node-specific bearer is projected into the generic Data Agent server
variable inside that service only.

## Architecture

```text
                  prospective/offline route choice input
                    AWM / OED observation bridge
                                |
                                v
public task -------> N1 control/admission -------> N2 index (when required)
     |                                                   |
     |                                                   v
     |        N3 raw/cold Data Agent -----------> N7 or N8 executor/cache
     |                    |                              |
     |                    v                              |
     |             N5 materializer                      |
     |          frames and/or digest                    |
     |                    |                              |
     |                    v                              |
     +----------> N4 derived Data Agent ----------------+
                                                        |
                                                        v
                                              N6 semantic inference
                                                        |
                                                        v
                                             N1 hidden-label scoring
                                                        |
                                                        v
                                        redacted, content-bound evidence
```

The logical plan contains no URL, credential, host path, container name, or
cloud resource. A separate deployment source binds each service contract to a
single-host Compose endpoint or a private multi-host endpoint. This separation
is what allows the same experiment contract to move to UpCloud.

## Node responsibilities

| Node | Implemented responsibility | Persistent or hidden state |
| --- | --- | --- |
| N1 | Logical trial-control contract plus an implemented authenticated hidden-label scoring service | Hidden oracle package and idempotent SQLite score ledger; labels never enter the public task or N6 request |
| N2 | Deterministic lexical retrieval over public metadata | Frozen index snapshot; no hidden labels |
| N3 | Authoritative raw/cold object access through the standard authenticated Data Agent contract | Exact content-addressed MP4 bytes and provenance |
| N4 | Authenticated derived-representation reads through the Data Agent contract plus an authenticated atomic-publication companion API | Atomic, content-addressed publication of frame bundles and multimodal digests |
| N5 | Deterministic raw-video-to-frame-bundle materialization plus an authenticated vision-digest companion API | Frozen input/output bindings, software/model provenance, and durable idempotent output handling |
| N6 | Semantic inference | Runtime-bound vision model; only public task data and selected representation bytes are visible |
| N7 | Independently startable core-side semantic route service, input preparation, local index, durable derived-artifact cache, and W4 coordinator with a dedicated W4 cache | Explicit cache scopes, SQLite route/W4 lineage, and content-addressed bytes |
| N8 | Independently startable edge-side semantic route service, input preparation, local index, durable derived-artifact cache, and W4 coordinator with a dedicated W4 cache | Explicit cache scopes, SQLite route/W4 lineage, and content-addressed bytes |

These are real component behaviours, not role labels on otherwise identical
containers. Before semantic execution, the HTTP preflight performs one full,
authenticated fetch for every distinct N3/N4 artifact, verifies the frozen
digest and size, and verifies that an invalid bearer credential is rejected.
The N7/N8 service factory then binds only verified public packages to runtime
URLs and credentials and refuses startup if a required client, identity, or
route remains unresolved.

The direct local harness reads exact N3 bytes, queries N2, executes N5,
publishes and resolves N4, verifies N7/N8 miss-store-reopen-hit behaviour,
invokes an injected N6 adapter, and sends only its prediction to N1. Separate
N5-to-N4 HTTP integration tests cover live frame-bundle and digest publication
protocols. They use the real local service implementations, but the digest
test uses an offline fake vision adapter and therefore is not evidence of a
real model call or external network path. N1 trial admission remains a logical
contract rather than a separate service in the direct harness.

## 4 x 8 logical experiment

The route compiler preserves the existing frozen matrix while adding the
materialization and hidden-score stages needed by the full architecture.

- W1: descriptive video question answering;
- W2: temporal video question answering;
- W3: causal video question answering; and
- W4: video retrieval.

The eight designs are four route families at two execution placements:

| Designs | Executor | Route family |
| --- | --- | --- |
| D0 / D4 | N7 / N8 | raw video, with W4 using a raw scan |
| D1 / D5 | N7 / N8 | indexed raw access where the workload requires it |
| D2 / D6 | N7 / N8 | remotely stored derived representations |
| D3 / D7 | N7 / N8 | execution-side cached derived representations with explicit hit/miss branches |

For the reference scenario, compilation produces:

- 64 trials and 32 workload/design cells;
- 500 stages inherited exactly from the infrastructure operation plan;
- six shared derived-artifact provisioning chains; and
- 658 logical stages after provisioning and N1 hidden scoring are made
  explicit.

The resulting route package covers N1-N8 and is endpoint-free. It is a plan,
not proof that a semantic 64-trial execution has occurred.

Before the local semantic matrix runner is allowed to submit anything, ten
representative cases must complete and verify:

| Executor | Mandatory route cases |
| --- | --- |
| N7 | raw, indexed raw, remote derived, cache miss, cache hit |
| N8 | raw, indexed raw, remote derived, cache miss, cache hit |

The cache-hit case is ordered after its corresponding miss case. The smoke
receipt, promoted public runtime package, original semantic matrix, deployment
binding, N4 serve gate, and source hashes are bound into an outer run contract.
Only then can the durable inner runner execute the exact 64 promoted trials.
This gate prevents a compatible-looking smoke or matrix from a different
source package from authorizing the run.

W4 requires a separate interpretation. The current local 64-trial semantic
matrix retains the historical multiple-choice W4 placeholder so that it can
verify orchestration shape; it must not be reported as retrieval quality. A
separate public W4 runtime binds a hidden-label-safe retrieval task to all 16
D0-D7-by-two-repetition coordinates. Its N2 lexical ranker verifies every
shard response by replay against the frozen index package rather than trusting
server-reported digests. Candidate-wide route packages bind exact N3 raw,
N4 digest/frame, index, and exact-range packages. The strict coordinator then
resolves conditional operations, enforces independent N7/N8 miss-to-hit
lifecycle, preserves prefix/tail ranking semantics, emits one complete public
ranking per trial, and feeds the existing N1 hidden-relevance evaluator.

The deterministic coordinator adapter remains an offline conformance fixture.
A separate local component factory now binds the same routes to verified
N2/N7/N8 index services, authenticated N3/N4 Data Agents, independent N7/N8
caches, and the N6 text/vision semantic endpoint. N1 admission/return and
inter-stage byte movement remain in-process in this adapter, so its live
source-bound receipt is local component evidence, not FlowMesh scheduling,
network, cloud-performance, monetary-cost, or scientific evidence. A
separate W4 FlowMesh layer now freezes one globally serial, worker-pinned API
task per retrieval trial and verifies the resulting 16-task run against the
same routes, index package, and crosswalk. The N7/N8 coordinator endpoints and
credentials remain runtime-only; no live W4 FlowMesh run is claimed until that
operator receipt exists.

### One-for-one logical migration readiness

All eight logical roles can be exercised before UpCloud. What moves to the
cloud is the deployment binding, not a different experiment protocol:

| Role | Local implementation available now | UpCloud replacement later |
| --- | --- | --- |
| N1 control and scoring | Public admission contract, private authenticated hidden scorer, replay ledger | Private control/scoring service and durable database |
| N2 index | Verified lexical index package and authenticated HTTP query client/server | Index VM or managed index using the same query/result contract |
| N3 raw/cold source | Authenticated Data Agent over exact content-addressed raw objects | Data Agent in front of object storage or a cold-data VM |
| N4 derived source | Authenticated Data Agent plus atomic compare-and-swap publication | Derived-object service and durable cloud volume/object store |
| N5 materialization | Frame-bundle and vision-digest services plus a resumable 36-object coordinator | Materialization worker/VM using the same plans and receipts |
| N6 inference | Authenticated text/vision semantic endpoint with bounded public requests | Model-serving VM or external model endpoint |
| N7/N8 execution | Independent route coordinators, indexes, caches, and persistence | Core and edge execution VMs with separately measured paths |

This is a one-for-one replacement of logical responsibilities and evidence
contracts, not necessarily eight identical VMs. Local endpoints, in-process
handoffs, shaped links, and shared-host resources are replaced by private
cloud endpoints, TLS, real disks/object stores, and measured inter-host links.

## What is complete without UpCloud

The following work can be built and verified on one machine. Here, "complete"
means an implemented contract with deterministic verification; it does not
mean that every contract has already run together as a network service. The
verbs below describe implemented capabilities, not recorded live results:

1. Freeze and verify public tasks separately from the N1-private oracle, and
   publish a label-free pre-selection oracle commitment.
2. Freeze and verify N2, N3, and N4 packages from operator-supplied manifests.
3. Materialize canonical frame bundles and freeze/run/verify vision digests on
   N5. Model credentials remain runtime-only.
4. Join the public task plane to verified N3/N4 packages in an endpoint-free
   artifact-binding package. Each logical object is mapped to a distinct real
   artifact object with exact representation digest, byte size, and catalog
   version; no artifact bytes, endpoints, labels, or credentials are copied.
5. Compile and byte-reproduce 64 semantic trial envelopes (32 W1-W4 x D0-D7
   cells, two repetitions) from the logical routes, public tasks, and exact
   artifact bindings. Synthetic `task_success_by_design` values are explicitly
   not consumed.
6. Freeze an endpoint-free startup package covering every service action. It
   maps N3/N4 reads to the standard Data Agent, N4 publication and both N5
   materializers to authenticated companion processes, N6 to the semantic
   container, and N7/N8 to independently startable semantic route and W4
   coordinators, local indexes, and separate durable cache namespaces, while
   trial control, branch joins, and transport remain embedded workflow/runtime
   actions. N7/N8 receive only the public promoted runtime package and
   label-free N1 commitment; hidden labels and the N1 evidence secret remain on
   N1.
7. Generate one operator-editable deployment template, complete it for either
   Compose or a private multi-host deployment, and reject any missing action,
   representation, persistence, endpoint, or credential-name binding. The
   current v1alpha2 deployment schema also binds the independently startable
   N7 and N8 W4 coordinators as exact runtime services, including their own
   endpoint, health-schema identity, persistent-state declaration, and
   credential names. Legacy v1alpha1 bindings remain readable for historical
   verification but cannot authorize the current W4-ready Compose overlay.
8. Reproducibly render a unified local Compose overlay and one independently
   instantiable, checksum-bound fragment per service from the verified startup
   and deployment contracts. It maps N1-N8 to 12 primary services, seven
   companion services, and ten persistent volumes without invoking Docker.
   N3/N4 fragments bind their complete frozen Data Agent package roots
   read-only, while unrelated runtime variables are absent. The additional
   companions are the N7/N8 W4 coordinators and their exclusive cache and
   coordinator state volumes. Raw-video sampling uses a separate bounded
   ephemeral mount. A separate
   operator gate requires derived-artifact provisioning to finish, all
   published digests to verify, and an immutable N4 manifest to be frozen
   before the serving profile may start. The verified preprovisioned N4
   serve-gate receipt belongs at the execution coordinator's profile-selection
   boundary: it must be verified before selecting `serve-frozen`, and the
   `provision-derived` publication companion must remain excluded throughout
   semantic trials. Historical Compose v1alpha2/v1alpha3 overlays and their
   bound N4 serve-gate receipts remain verifiable, but they do not authorize a
   newly rendered v1alpha4 overlay. Freeze a new N4 serve-gate v1alpha2 receipt
   in a new directory after rendering the new overlay; never rewrite the old
   artifacts.
9. Provide a preflight for every distinct semantic artifact through
   authenticated N3/N4 Data Agent HTTP reads. It performs a complete fetch,
   checks the exact frozen digest and size, and challenges each service with
   an invalid bearer credential to verify that authentication is enforced.
   The receipt stays bound to the admission and source packages and makes no
   latency or throughput claim.
10. Freeze a prospective AWM workload-class assignment or an OED-selected
   ordered trial subset without reading outcomes.
11. Convert either legacy full-flow evidence or generic semantic route
   evidence from the promoted 64-trial runtime into neutral success, latency,
   and byte observations. Generic evidence is rebound to the exact trial,
   logical route, stage identities and hashes, public task, artifact, and
   executor/cache branch. Hidden labels remain excluded. Monetary cost stays
   absent unless supplied by a separately frozen external real-cost manifest.
12. Exercise all eight components in the one-task direct local harness. This
   harness deliberately uses direct Python interfaces, an offline N6 adapter,
   and no FlowMesh or Docker; it verifies composition, not deployment or
   performance.
13. Assemble independently startable N7 and N8 route services from verified
    source packages and runtime-only authenticated clients. Construction is
    fail-closed; endpoint health is checked on first use rather than treated as
    a property of the frozen package.
14. Provide runners to execute and verify ten mandatory representative
    semantic smokes, then use their exact receipt as the gate for a
    source-bound 64-trial runner. The runners and gate are implemented and
    focused-tested; no operator-controlled ten-smoke or semantic matrix
    receipt is currently recorded.
15. Exercise N5-to-N4 frame-bundle and digest provisioning through local
    in-process HTTP integration tests, including authenticated execution,
    exact binding, compare-and-swap publication, atomic visibility,
    replay-aware recovery, and checksummed receipts. This is protocol
    conformance, not proof that the deployed Compose services or a real vision
    model have completed the path.
16. Freeze a post-materialization N4 serve gate from exact live receipts,
    verify the complete immutable publication-generation chain, and require
    rebuilt artifact bindings, semantic matrix, and execution admission to
    resolve the final N4 generation. This authorizes only `serve-frozen` and
    explicitly excludes the publication companion during semantic execution.
17. Interpret and validate all 16 W4 candidate-wide route blueprints with an
    injected public operation adapter. The deterministic adapter validates
    branch, artifact, exact-range, index, cache, and complete-ranking
    semantics. The local component factory additionally binds verified
    N2/N7/N8 indexes, N3/N4 Data Agents, N7/N8 caches, and N6 semantic ranking
    without serializing endpoints or credentials. N1 control and byte
    transport remain in-process in the direct adapter. A separate implemented
    wrapper places all 16 N7/N8 coordinator calls in one globally serial,
    worker-pinned FlowMesh graph and freezes source-bound run evidence.

The hidden-oracle FlowMesh v2 path separately freezes and verifies a
worker-pinned N4 -> N7 -> N6 -> N1 trial without placing an answer in the task
sent to the worker. The new matrix path generalizes route execution, but its
code readiness does not substitute for a live ten-smoke receipt followed by a
live 64-trial semantic receipt.

## AWM and OED boundary

The bridge is deliberately narrower than an online optimizer:

- A prospective AWM policy digest can be bound to exact W1-W4 design choices.
- A prospective OED request digest can be bound to an exact ordered subset of
  trial keys.
- Both outputs are endpoint-free and immutable before execution.
- Completed evidence can be projected to neutral success, latency, and byte
  fields without importing simulator rate cards as real cost.

The bridge does not run either optimizer online or prove a policy certificate.
Its neutral-observation adapter accepts
the original legacy evidence without weakening its existing validation, and
also accepts the generic semantic route evidence emitted by the promoted
64-trial runtime. The generic branch covers raw, indexed raw, remote-derived,
cache-miss, and cache-hit routes on both N7 and N8. It re-verifies the runtime
evidence, then requires exact source, trial, logical-route, public-task,
artifact, executor, cache-branch, stage-identity, and stage-hash agreement. It
also requires privileged offline verification of the retained public N1
request and result against the frozen oracle package and runtime-only HMAC
secret.

The resulting success, component-latency, and byte values are neutral
observations. The adapter rejects hidden labels, embedded simulator cost hints,
and scientific eligibility claims. It performs no performance analysis and
does not reinterpret component timings as end-to-end or cloud performance.
The direct local-harness schema remains test-only and is not accepted as a
policy observation. Monetary cost is available only when an independently
supplied, separately frozen external real-cost manifest covers the selected
trials exactly; the adapter does not verify that external attestation's
authenticity. A separate deterministic consumer now validates the exact
W1-W4 x D0-D7 x two-repetition observation set. Without that external-cost
manifest it emits only a descriptive quality/infrastructure policy and a
prospective request for fresh independent workload pairs. It records
`NOT_EVALUATED_MISSING_EXTERNAL_REAL_COST`, never manufactures a cost scalar,
and never authorizes commit. Only a fully bound external-cost package may
invoke the existing weighted-certificate core; even then the result remains
post-hoc simulator analysis which requires prospective multi-node
confirmation.

## Local readiness versus live local evidence

### Code and focused-test readiness

The repository now contains the complete local-conformance control path for
W1-W3 and the historical multiple-choice W4 placeholder:

- authenticated, content-level N3/N4 artifact preflight;
- authenticated N5 frame-bundle and digest execution plus atomic N4
  publication conformance, including a durable 36-object / 72-operation
  coordinator with hash-chain journal, checkpoints, receipt adoption, and
  infrastructure-failure resume;
- fail-closed N7 and N8 semantic route-service construction;
- ten mandatory representative smokes covering five route families on both
  executors;
- an outer smoke/source gate around the durable 64-trial runner; and
- generic semantic route evidence conversion into neutral AWM/OED
  observations only after privileged N1 score replay;
- a live N5-to-N4 publication/immutable-serve gate with downstream rebinding;
  and
- a strict W4 multi-candidate coordinator, source-verified N2-to-artifact
  crosswalk, component execution receipt, and hidden-relevance evaluator
  path; and
- an offline neutral-observation consumer which produces a descriptive
  policy and fresh-workload OED priorities without treating simulator timing
  or byte counts as money.

These capabilities are covered by deterministic unit and local integration
tests. They do not establish that the operator's current containers,
credentials, FlowMesh worker, Data Agents, N6 model, or N1 service have
completed the same run.

### Live local evidence still to collect

The following work does not require UpCloud and should be completed before a
cloud deployment is described as a one-for-one migration:

1. Complete a deployment source with runtime endpoints and credential names,
   render and launch the verified 19-service Compose topology, and record the
   runtime identities. Confirm that the N7/N8 W4 coordinators depend on their
   dedicated healthy W4 cache services rather than the semantic-route caches.
   The two coordinator endpoints must use the exact v1alpha2 runtime-service
   bindings; inheriting only the parent N7/N8 execution-service endpoint is
   intentionally rejected.
2. Run the authenticated N3/N4 full-content artifact preflight against those
   deployed services and freeze its receipt.
3. Freeze the 36-object operator source manifest, run the bulk N5-to-N4
   coordinator against the deployed services, and verify all 72 frame/digest
   publications. Then rebuild the N4 package bindings and freeze and enforce
   the N4 `serve-frozen` gate. The current integration tests alone do not
   satisfy this step.
4. Start the N7 and N8 route services, run and verify all ten representative
   semantic smokes, and preserve the smoke receipt.
5. Use that receipt to authorize the exact source-bound 64-trial semantic run;
   verify the matrix output before producing neutral AWM/OED observations.
6. Run the source-bound W4 path over all 16 retrieval routes against
   N2/N3/N4/N6/N7/N8. The direct component command intentionally records
   `flowmesh_workflow_submitted=false`; the separate W4 FlowMesh command must
   submit the frozen 16-task graph through the pinned worker and retain its
   nested candidate-run and component-receipt directories. Score that exact
   candidate run through independently controlled N1 hidden relevance. Until
   the FlowMesh, component, and N1 receipts all bind to the same execution,
   keep W4 labelled as code/test readiness rather than measured retrieval
   quality.

The direct harness and the single-route FlowMesh v2 path reduce the risk of
these steps, but do not replace their live receipts.

The older `full_flow_experiment_freeze` module is retained only to verify
historical blocked artifacts. Its schema is deprecated and must not be used
as the current readiness decision; the aggregate command in this document is
the normative pre-UpCloud gate.

## Evidence that specifically requires UpCloud

UpCloud is not required to finish the local full-flow conformance run. It is
required for evidence about physical separation and cloud behaviour that a
single host cannot create. The cloud phase must independently establish:

- **placement reality:** N1-N8 roles really reside on the declared VMs,
  volumes, object stores, or managed services, rather than sharing one host
  kernel and disk cache;
- **network reality:** inter-host bandwidth, RTT, loss, congestion, egress,
  and cross-zone or cross-region behaviour measured on the actual paths;
- **storage reality:** HDD, SSD, NVMe, and object-store throughput, latency,
  cold-start effects, caching, and contention on the selected resources;
- **queueing and concurrency:** FlowMesh, service, GPU, network, and storage
  queueing under the frozen arrival and concurrency profile;
- **distributed operations:** private discovery, TLS or mTLS, clock alignment,
  credential delivery, restart, retry, partition, and independently failing
  host behaviour;
- **model-service reality:** inference latency, availability, and rate limits
  of the deployed model path under the experiment load; and
- **monetary reality:** VM, volume, object-store, egress, FlowMesh, API, and
  model charges from a frozen provider rate card and auditable usage records.

Only after those measurements exist can repeated multi-host semantic quality,
performance, and cost be evaluated for the complete workload/design matrix.
Local shaped delays, fixture bytes, in-process HTTP tests, and component
service times cannot be promoted into those claims.

UpCloud work should therefore start with deployment and calibration, not a new
logical experiment design:

1. Assign N1-N8 contracts to VMs or managed services.
2. Bind private DNS/IPs, TLS or mTLS, runtime secrets, disks, and the FlowMesh
   worker in a completed deployment source.
3. Measure network with iperf3, storage with fio, inference time, and queueing
   under the declared concurrency profile.
4. Freeze those measurements and the cloud rate card as external calibration
   evidence.
5. Run conformance canaries, then repeated semantic trials and failure tests.
6. Generate a real-cost manifest only from the frozen external measurements
   and provider prices.

No local configured service-cost value should be relabelled as a real cost.

## Migration contract

Moving to UpCloud is a one-for-one replacement of **logical service roles**,
not a literal replacement of each container by an identical VM.

The following must stay fixed:

- public/private task split and hidden scoring rule;
- object IDs, representation IDs, content digests, and materialization plans;
- N1-N8 service actions, state semantics, and logical dependencies;
- AWM/OED prospective selections and trial ordering;
- FlowMesh worker pinning and request schemas; and
- evidence schemas, checksums, and verifiers.

The following are deployment-specific and may change:

- service URLs, private DNS, ports, and transport security;
- credential environment-variable names and secret provisioning;
- container volumes versus real disks or object storage;
- application shaping versus physical or kernel-shaped links; and
- container limits versus VM, disk, network, and GPU allocations.

If changing deployment also changes a logical action, representation, trial,
or score contract, it is a new experiment rather than an UpCloud re-binding.

## Concise offline-first runbook

The stages deliberately separate three kinds of readiness:

| Stage | Output | What it establishes |
| --- | --- | --- |
| A. Code and package readiness | Checksummed task/oracle, N2-N5, artifact-binding, logical/semantic-plan, deployment, and Compose packages | Inputs and contracts are complete and reproducible; no service or workflow need run |
| B. Local execution evidence | Authenticated N3/N4 preflight, deployed N5-to-N4 provisioning, N7/N8 ten-route smokes, and the source-bound 64-trial semantic run | The declared components interoperate locally; local timing and configured shaping are not cloud measurements |
| C. UpCloud-only evidence | Frozen network, storage, queueing, inference, failure, and provider-price measurements | Real multi-host performance and monetary cost; this cannot be inferred from Stage A or B |

The commands below are Stage A only. They write new immutable directories and
do not launch Docker, FlowMesh, an LLM, or UpCloud resources.

```bash
export SCENARIO=configs/flowmesh_infra_simulator_4x8_smoke.json
export CONTAINER_SPEC=configs/flowmesh_infra_container_4x8_contract.json
export WORK_ROOT=/path/to/new/full-flow-work

PYTHONPATH=. python -m pathfinder build-portable-execution-plan \
  --scenario "$SCENARIO" \
  --output-dir "$WORK_ROOT/portable"

PYTHONPATH=. python -m pathfinder plan-container-simulation \
  --scenario "$SCENARIO" \
  --portable-plan-dir "$WORK_ROOT/portable" \
  --container-spec "$CONTAINER_SPEC" \
  --output-dir "$WORK_ROOT/container-plan"

PYTHONPATH=. python -m pathfinder compile-simulator-full-flow-logical-routes \
  --scenario "$SCENARIO" \
  --container-plan-dir "$WORK_ROOT/container-plan" \
  --output-dir "$WORK_ROOT/logical-routes"

PYTHONPATH=. python -m pathfinder verify-simulator-full-flow-logical-routes \
  --plan-dir "$WORK_ROOT/logical-routes" \
  --scenario "$SCENARIO" \
  --container-plan-dir "$WORK_ROOT/container-plan"

PYTHONPATH=. python -m pathfinder \
  freeze-simulator-full-flow-service-bootstrap \
  --logical-plan-dir "$WORK_ROOT/logical-routes" \
  --scenario "$SCENARIO" \
  --container-plan-dir "$WORK_ROOT/container-plan" \
  --bootstrap-id local-eight-node-services-v1 \
  --output-dir "$WORK_ROOT/service-bootstrap"

PYTHONPATH=. python -m pathfinder \
  verify-simulator-full-flow-service-bootstrap \
  --bootstrap-dir "$WORK_ROOT/service-bootstrap" \
  --logical-plan-dir "$WORK_ROOT/logical-routes" \
  --scenario "$SCENARIO" \
  --container-plan-dir "$WORK_ROOT/container-plan"

PYTHONPATH=. python -m pathfinder \
  generate-simulator-full-flow-deployment-template \
  --logical-plan-dir "$WORK_ROOT/logical-routes" \
  --scenario "$SCENARIO" \
  --container-plan-dir "$WORK_ROOT/container-plan" \
  --template-id local-eight-node-v1 \
  --output-dir "$WORK_ROOT/deployment-template"

PYTHONPATH=. python -m pathfinder \
  verify-simulator-full-flow-deployment-template \
  --template-dir "$WORK_ROOT/deployment-template" \
  --logical-plan-dir "$WORK_ROOT/logical-routes" \
  --scenario "$SCENARIO" \
  --container-plan-dir "$WORK_ROOT/container-plan"
```

The component package commands are:

```text
build-simulator-full-flow-task-plane
verify-simulator-full-flow-task-plane
build-simulator-n1-hidden-oracle
verify-simulator-n1-hidden-oracle
freeze-simulator-n1-oracle-preselection-commitment
verify-simulator-n1-oracle-preselection-commitment
build-simulator-n2-index
verify-simulator-n2-index
build-simulator-n3-raw-data-plane
verify-simulator-n3-raw-data-plane
build-simulator-n4-derived-data-plane
verify-simulator-n4-derived-data-plane
freeze-simulator-n5-digest-plan
verify-simulator-n5-digest-plan
run-simulator-n5-digest-materialization
verify-simulator-n5-digest-materialization
```

Use `python -m pathfinder COMMAND --help` for each input manifest. The build
commands refuse an existing output directory so a frozen artifact is not
silently overwritten.

Freeze the N1 commitment before any outcome-informed AWM/OED selection. The
first verification is safe for the public side; the optional second form opens
the commitment against the private N1 package without printing labels:

```bash
PYTHONPATH=. python -m pathfinder \
  freeze-simulator-n1-oracle-preselection-commitment \
  --oracle-package-dir "$WORK_ROOT/task-plane/n1-private/oracle-package" \
  --commitment-id local-oracle-preselection-v1 \
  --output-dir "$WORK_ROOT/oracle-commitment"

PYTHONPATH=. python -m pathfinder \
  verify-simulator-n1-oracle-preselection-commitment \
  --commitment-dir "$WORK_ROOT/oracle-commitment"
```

Use `--oracle-package-dir` on the verification command only inside the N1
private boundary when opening the commitment.

After all source packages verify, bind exact artifacts and compile the 64
endpoint-free semantic envelopes:

```bash
PYTHONPATH=. python -m pathfinder \
  build-simulator-full-flow-artifact-bindings \
  --logical-plan-dir "$WORK_ROOT/logical-routes" \
  --scenario "$SCENARIO" \
  --container-plan-dir "$WORK_ROOT/container-plan" \
  --task-plane-dir "$WORK_ROOT/task-plane" \
  --n3-package-dir "$WORK_ROOT/n3-package" \
  --n4-package-dir "$WORK_ROOT/n4-package" \
  --binding-set-id local-real-artifacts-v1 \
  --output-dir "$WORK_ROOT/artifact-bindings"

PYTHONPATH=. python -m pathfinder \
  compile-simulator-full-flow-semantic-matrix \
  --logical-plan-dir "$WORK_ROOT/logical-routes" \
  --scenario "$SCENARIO" \
  --container-plan-dir "$WORK_ROOT/container-plan" \
  --public-task-set "$WORK_ROOT/task-plane/public/public-tasks.json" \
  --artifact-bindings "$WORK_ROOT/artifact-bindings/artifact-bindings.json" \
  --output-dir "$WORK_ROOT/semantic-matrix"
```

Run the matching `verify-simulator-full-flow-artifact-bindings` and
`verify-simulator-full-flow-semantic-matrix` commands with the same source
arguments before deployment. Compilation records that semantic execution,
quality evaluation, performance measurement, and cost measurement are all
false.

Copy the generated source template to a new file, resolve every placeholder,
and validate it before publishing a binding:

```bash
PYTHONPATH=. python -m pathfinder \
  validate-simulator-full-flow-deployment-source \
  --deployment-source "$WORK_ROOT/deployment-source.json" \
  --template-dir "$WORK_ROOT/deployment-template" \
  --logical-plan-dir "$WORK_ROOT/logical-routes" \
  --scenario "$SCENARIO" \
  --container-plan-dir "$WORK_ROOT/container-plan"
```

Then bind, verify, and probe it:

```bash
PYTHONPATH=. python -m pathfinder build-simulator-full-flow-deployment \
  --logical-plan-dir "$WORK_ROOT/logical-routes" \
  --scenario "$SCENARIO" \
  --container-plan-dir "$WORK_ROOT/container-plan" \
  --deployment-source "$WORK_ROOT/deployment-source.json" \
  --output-dir "$WORK_ROOT/deployment-binding"

PYTHONPATH=. python -m pathfinder verify-simulator-full-flow-deployment \
  --binding-dir "$WORK_ROOT/deployment-binding" \
  --logical-plan-dir "$WORK_ROOT/logical-routes" \
  --scenario "$SCENARIO" \
  --container-plan-dir "$WORK_ROOT/container-plan"

PYTHONPATH=. python -m pathfinder preflight-simulator-full-flow-deployment \
  --binding-dir "$WORK_ROOT/deployment-binding" \
  --logical-plan-dir "$WORK_ROOT/logical-routes" \
  --scenario "$SCENARIO" \
  --container-plan-dir "$WORK_ROOT/container-plan"
```

Finally, render and reproduce the local overlay without starting it:

```bash
PYTHONPATH=. python -m pathfinder \
  render-simulator-full-flow-compose-overlay \
  --service-bootstrap-dir "$WORK_ROOT/service-bootstrap" \
  --deployment-binding-dir "$WORK_ROOT/deployment-binding" \
  --logical-plan-dir "$WORK_ROOT/logical-routes" \
  --scenario "$SCENARIO" \
  --container-plan-dir "$WORK_ROOT/container-plan" \
  --overlay-id local-eight-node-full-flow-v1 \
  --output-dir "$WORK_ROOT/compose-overlay"

PYTHONPATH=. python -m pathfinder \
  verify-simulator-full-flow-compose-overlay \
  --overlay-dir "$WORK_ROOT/compose-overlay" \
  --service-bootstrap-dir "$WORK_ROOT/service-bootstrap" \
  --deployment-binding-dir "$WORK_ROOT/deployment-binding" \
  --logical-plan-dir "$WORK_ROOT/logical-routes" \
  --scenario "$SCENARIO" \
  --container-plan-dir "$WORK_ROOT/container-plan"
```

Do not select the generated serving profile until the overlay's N4 stage gate
has been satisfied. Runtime secrets must be injected under the recorded
environment-variable names; they must not be written into the generated
files.

Prospective policy and OED artifacts use
`freeze-simulator-policy-routes`, `verify-simulator-policy-routes`,
`freeze-simulator-oed-routes`, and `verify-simulator-oed-routes`. Neutral
observation artifacts use `freeze-simulator-full-flow-observations` and
`verify-simulator-full-flow-observations`.

After Stage A packages are frozen, the local Stage B command families are:

```text
preflight-simulator-full-flow-semantic-artifacts
verify-simulator-full-flow-semantic-artifact-preflight
run-simulator-n5-n4-live-frame-bundle-smoke
verify-simulator-n5-n4-live-frame-bundle-smoke
run-simulator-n5-n4-live-digest-smoke
verify-simulator-n5-n4-live-digest-smoke
freeze-simulator-full-flow-bulk-provisioning-source-manifest
run-simulator-full-flow-bulk-live-provisioning
verify-simulator-full-flow-bulk-live-provisioning
freeze-simulator-full-flow-n4-live-serve-gate
verify-simulator-full-flow-n4-live-serve-gate
serve-simulator-full-flow-semantic-route        # start once for N7 and N8
run-simulator-full-flow-local-semantic-smokes
verify-simulator-full-flow-local-semantic-smokes
run-simulator-full-flow-local-semantic-matrix
verify-simulator-full-flow-local-semantic-matrix
freeze-simulator-full-flow-observations
verify-simulator-full-flow-observations
freeze-simulator-neutral-awm-oed-analysis
verify-simulator-neutral-awm-oed-analysis
freeze-simulator-full-flow-w4-index-artifact-crosswalk
verify-simulator-full-flow-w4-index-artifact-crosswalk
run-simulator-full-flow-w4-local-component-execution
freeze-simulator-full-flow-w4-component-execution-receipt
verify-simulator-full-flow-w4-component-execution-receipt
freeze-simulator-full-flow-w4-flowmesh-plan
verify-simulator-full-flow-w4-flowmesh-plan
serve-simulator-full-flow-w4-flowmesh-coordinator  # start for N7 and N8
run-simulator-full-flow-w4-flowmesh-matrix
verify-simulator-full-flow-w4-flowmesh-matrix
freeze-simulator-full-flow-pre-upcloud-readiness
verify-simulator-full-flow-pre-upcloud-readiness
```

The N3/N4 preflight and both N5/N4 smokes require runtime credentials but must
not persist them. The observation commands require
`--semantic-execution-admission-dir` when consuming generic semantic route
evidence. Each run command writes a new receipt directory; code availability
or a successful `--help` invocation is not a substitute for that receipt.

When the derived representations were produced by live N5-to-N4 provisioning,
freeze the live serve gate and pass the same operator-local descriptor to both
the smoke and matrix run/verify commands with
`--n4-live-gate-sources PATH`. The descriptor contains only local source paths
and exact frame/digest receipt bindings; relative paths resolve from the
descriptor's own directory. Its verifier requires the final immutable N4
generation and the rebuilt artifact bindings, semantic matrix, and admission
package to agree before any semantic run is authorized.

The bulk source-manifest command accepts a short operator mapping containing
only `object_id`, `source_video_path`, `frame_plan_path`, and
`digest_plan_dir`. It computes and verifies the source and plan commitments;
operators must not hand-copy the 36 sizes or hashes. The bulk run then invokes
the existing one-object frame and digest protocols in a deterministic
72-operation order. Only a classified infrastructure failure is resumable;
semantic or data-integrity failures stop for investigation. Its final
`live-receipt-bindings.json` is the input to the N4 live serve gate.

The pre-UpCloud readiness audit is offline and fail-closed. Its local-live
status is present only when it can re-verify the complete 36-by-two
provisioning run, a live 16-trial W4 component receipt, and the exact W4
coordinator observations against the private N1 retrieval contract and
evaluation. Its FlowMesh status is present only when both evidence groups
agree: the frozen profile/coordinator/source-bound completed 64-trial
infrastructure run, and the source-bound one-workflow/16-task W4 run. It still
reports every real multi-host storage, network, contention, failure,
performance, and monetary-cost item as an UpCloud-only gap.

After the semantic matrix has been converted to neutral observations, the
neutral AWM/OED command is the only supported local consumer for this new
evidence shape. Its default no-cost mode is intentional: it reports a
descriptive policy and fresh-workload collection priorities, not a policy
commitment. `--require-real-cost` must fail unless the observation package is
bound to a complete external real-cost manifest.

For the hidden-label worker-pinned single trial, the v2 command sequence is
`build-flowmesh-full-flow-public-request-v2`,
`plan-flowmesh-full-flow-trial-v2`,
`verify-flowmesh-full-flow-trial-v2-plan`,
`run-flowmesh-full-flow-trial-v2`, and
`verify-flowmesh-full-flow-trial-v2-run`. This remains a single-route semantic
check, not the full 4 x 8 semantic matrix.

## Claim checklist

- **May claim:** the prior 64-trial infrastructure matrix completed and its
  routing, dependencies, configured branches, and byte accounting verified.
- **May claim:** the repository implements portable N1-N8 contracts, a complete
  endpoint-free 4 x 8 logical and artifact-bound semantic plan, strict
  deployment validation, and a unified non-launching Compose overlay.
- **May claim:** authenticated N3/N4 exact-artifact preflight, independent
  N7/N8 route services, a ten-smoke runtime gate, and a source-bound 64-trial
  semantic runner are code-ready and focused-tested.
- **May claim:** local in-process HTTP integration tests verified N5-to-N4
  frame and digest publication protocol conformance, including replay-aware
  recovery. This must be described as test evidence, not a deployed run.
- **May claim:** generic semantic route evidence can be converted into neutral
  AWM/OED observations with exact binding and without consuming hidden labels
  or simulator cost hints.
- **May not claim:** the ten representative semantic smokes or the full
  semantic 4 x 8 experiment completed in the operator's live environment.
- **May claim:** the W4 public runtime, complete-candidate coordinator, exact
  index-to-artifact crosswalk, and concrete local N2/N3/N4/N6/N7/N8 component
  bindings, plus a globally serial worker-pinned 16-task FlowMesh wrapper, are
  implemented and tested without exposing relevance labels.
- **May not claim:** the historical W4 matrix cells measure retrieval quality,
  or that a W4 FlowMesh/component run has completed until its independently
  verified live receipts exist.
- **May not claim:** single-host timings predict UpCloud performance.
- **May not claim:** simulator rate cards or shaped delays are real monetary
  costs.
- **May not claim:** AWM/OED has produced a confirmatory policy certificate for
  this new full-flow evidence.

The earlier infrastructure and coupling layers remain documented in
[`FLOWMESH_INFRA_SIMULATOR.md`](FLOWMESH_INFRA_SIMULATOR.md) and
[`DATA_AGENT_SIMULATOR_COUPLING.md`](DATA_AGENT_SIMULATOR_COUPLING.md).
