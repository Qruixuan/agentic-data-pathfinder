# Distributed Pilot Infrastructure

This document describes the minimum infrastructure required to run a
Pathfinder pilot in which origin and local representations live on **different
Data Agent nodes**. The code exists; the pilot has not been run.

Nothing in `pathfinder/distributed/` starts a worker, a Data Agent, or an MCP
service, and nothing submits a workflow. Service and worker lifecycle is
manual by design.

---

## 1. What is different from the current experiment

The frozen v2 Reduced Oracle and every AWM/OED result derived from it come
from a **single-node, locally simulated** setting. The table below is the
whole gap.

| | Current (frozen v2) | Real distributed pilot |
|---|---|---|
| Representation placement | One node; "origin" and "local" differ by path and simulated latency | Two or more Data Agent nodes with distinct node identities |
| Network cost | Not measured; folded into a simulated service cost | Measured transferred bytes converted at a declared rate |
| Storage cost | Fixed per-design constant from the Oracle table | Measured bytes x hours at a declared rate |
| Materialization cost | Counted once, not amortized | Amortized over a **declared** session horizon |
| Transition cost | Modelled from copied bytes and elapsed seconds | Measured on the real copy path |
| Cost basis | `service_cost` only | `total_cost` (five additive components) |
| Failure modes | Local I/O errors | Endpoint-unreachable, cross-endpoint handle misuse, partial artifact delivery |
| Candidate set | Four designs | Stratum-restricted: three designs, `D_local_pair` excluded |

**Consequence.** The frozen results are a *controlled single-node
intervention*. They cannot be read as evidence about distributed placement.
The pilot exists to find out whether the measurement machinery survives a real
two-node deployment at all.

---

## 2. Cost equation and units

```
total_cost = service
           + network
           + storage
           + amortized_materialization
           + transition
```

All five terms are expressed in one declared `accounting_unit`. Every raw
quantity is preserved next to its converted value, its unit, the conversion
rule, and whether it was `measured`, `configured`, or `derived`.

| Component | Raw quantity | Unit | Conversion |
|---|---|---|---|
| `service` | Data Agent realized cost | accounting units | identity |
| `network` | transferred bytes | bytes | `network_cost_per_gib * bytes / 2**30` |
| `storage` | stored bytes x hours | GiB-hours | `storage_cost_per_gib_hour * gib_hours` |
| `amortized_materialization` | materialized bytes | bytes | `materialization_cost_per_gib * bytes / 2**30 / horizon` |
| `transition` | copied bytes + elapsed seconds | bytes+seconds | `transition_cost_per_gib * bytes / 2**30 + elapsed_time_cost_per_second * seconds` |

### No double counting of artifact transfer

Artifact bytes are the largest quantity in the ledger and are visible to both
the service cost and the network cost. The cost model therefore **must**
declare `artifact_transfer_accounted_in` as exactly one of `service_cost` or
`network_cost`:

* `network_cost` — the service term excludes transfer; the network term prices
  the measured bytes.
* `service_cost` — the network term is recorded as `0.0` with the rule
  `"excluded to avoid double counting"`, and the raw byte count is still kept
  for audit.

### Unavailable is not zero

A component that could not be measured is recorded with
`value_kind = "unavailable"` and a reason. It does **not** contribute zero:
`total_cost` becomes `None` and `record_total_cost` raises. A design must
never look cheaper because its network term was never measured.

### The rates are not scientific

`configs/distributed_pilot_example.json` ships every conversion rate as `0.0`
with `rate_provenance` beginning `PLACEHOLDER`. Preflight raises an advisory
warning while that is true. **Replace every rate with a value measured on the
actual deployment before interpreting any cost comparison.**

---

## 3. Endpoint registry

`endpoint_id -> connection settings + node identity`. See
`configs/distributed_endpoints_example.json`.

```
placement: (design_id, representation_id) -> endpoint_id
```

Resolution order: exact `(design, representation)` rule, then
`(design, "*")`, then the registry default. A registry with exactly one
endpoint and no placement rules routes everything there, which is how existing
single-Data-Agent behaviour is preserved.

**No credentials, ever.** An endpoint declares `base_url_env` and `token_env`
— the *names* of environment variables. Declaring an inline `base_url`,
`token`, `api_key`, or `password` is rejected by the loader. Public output
carries only a sanitized `scheme://host:port`, a fingerprint, and
`token_configured: true|false`.

**No silent fallback.** An unroutable `(design, representation)` pair raises.
Serving origin bytes from the local node would invert the exact contrast the
pilot measures, so guessing is never preferable to failing.

**Failure classification.** `EndpointUnreachableError` carries
`failure_class = "infrastructure"`; `CrossEndpointHandleError` carries
`"policy"`. Infrastructure failures are retried within a bounded budget and
recorded in a separate ledger. They are never counted as a design performing
badly.

**Endpoint-scoped artifact handles.** An `EndpointScopedHandle` redeems only
at the endpoint that issued it; the same opaque value bound to two endpoints
fingerprints differently. Persistent Gateway records store only
`artifact_handle_sha256`, never the bearer value.

---

## 4. Manual service and worker lifecycle

This tooling **never** starts, stops, or creates anything. Before a pilot the
operator must, by hand:

1. Start the Data Agent process on each node and confirm each serves its
   representations.
2. Export `PATHFINDER_DATA_AGENT_*_URL` and `..._TOKEN` for every declared
   endpoint in the shell that will run the pilot.
3. Start the FlowMesh worker and confirm it is registered with the **Root**,
   not merely visible to the local Node Server.
4. Confirm the worker can reach every Data Agent endpoint over the network.
5. Run `preflight-distributed-pilot` and resolve every failed check.
6. Run one throwaway workload end to end and confirm the worker logs a
   received task.

Steps 4 and 6 are the ones preflight cannot perform for you.

### Preflight modes

`preflight-distributed-pilot --mode offline_validation` (default) checks
structural consistency and tolerates placeholders, so a draft plan can be
validated long before deployment identifiers exist.

`--mode live_pilot` **fails** rather than warns while any of these remain a
placeholder: the Git revision, an endpoint URL variable, an endpoint or
execution node identity, the rate provenance, a required cost conversion, the
amortization horizon, a workload or exclusion manifest digest, or an endpoint
health / capability / catalog result.

A zero conversion rate is legitimate only when its provenance explicitly
justifies it as measured — for example, a cluster that bills no egress between
the two nodes. A `PLACEHOLDER` provenance always fails a live preflight.

---

## 5. Restricted design domain

| Stratum | Safe design | Candidate |
|---|---|---|
| causal | `D_origin_remote` | `D_local_frames` |
| descriptive | `D_origin_remote` | `D_local_frames` |
| temporal | `D_origin_remote` | `D_local_digest` |

`D_local_pair` is excluded and never appears in the trial plan.

Each workload gets one **complete repetition block for both** of its designs.
A workload is the independent statistical unit; repetitions are within-workload
repeated measurements and never increase the independent count.

This restriction was chosen **post-hoc** from the workload heterogeneity audit
of the frozen v2 Oracle. It is a hypothesis being carried into a pilot, not a
validated policy.

---

## 6. Why the next 30-50 workload run is a pilot, not confirmatory evidence

The preregistration fixes thresholds, the candidate restriction, and the
workload manifest before any new outcome is read. That is necessary for a
confirmatory study but nowhere near sufficient. Four things block it:

1. **The candidate restriction is post-hoc.** It was selected by inspecting
   the frozen Oracle. Confirming a hypothesis requires workloads that did not
   generate it.
2. **The thresholds are engineering values.** `delta_success_margin = 0.05`
   and `minimum_cost_saving = 0.25` are provenance-tagged
   `pilot-engineering-threshold-fixed-before-new-pilot-outcomes`. They are not
   derived from any decision-theoretic or operational argument.
3. **The cost rates are placeholders.** Until every conversion rate is
   measured on the real deployment, the cost gate compares numbers whose units
   are declared but whose magnitudes are arbitrary.
4. **The sample size is far too small.** Monte Carlo calibration of the
   v3alpha5 certificate showed that reliable commitment needs on the order of
   **100 workloads per stratum** at plausible effect sizes. 30-50 workloads
   spread over three strata will almost certainly return
   `INSUFFICIENT_EVIDENCE` everywhere. That is an expected and acceptable
   pilot outcome.

The pilot's job is to establish that the *plumbing* works: that two Data
Agents can be routed between, that a five-component cost ledger can be
assembled from real telemetry, that incomplete telemetry fails closed, and
that a run can be resumed without corrupting its dataset.

Accordingly the preregistration loader **refuses** to load a document
declaring `confirmatory: true` or `eligible_for_scientific_claims: true`.
There is no flag that turns this pilot into evidence.

---

## 7. Live COMMIT remains disabled

Nothing in this package commits a design. The pilot produces observations; the
AWM v3alpha5 certificate and the certificate-gated OED replay consume them
**offline**. Every non-`SAFE_TO_COMMIT` certificate result retains
`D_origin_remote`, and the preregistration requires
`fallback_rule.design_id = D_origin_remote`.

Applying a design to a live system remains a separate, manual, currently
unimplemented step.

---

## 8. The executable vertical slice

The following path runs end to end **offline**, against in-process fake Data
Agents, using the real routing, Gateway, artifact, cost and runner code:

```
distributed plan
  -> FlowMesh session adapter
  -> Gateway endpoint resolution
  -> endpoint-specific Data Agent access
  -> endpoint-scoped artifact delivery
  -> reconciled telemetry
  -> total-cost ledger
  -> canonical Reduced Oracle records
  -> AWM v3alpha5 input load
```

`run-distributed-pilot` executes the frozen plan cell by cell. It requires a
passing preflight, appends every attempt to a durable ledger before advancing,
classifies task / telemetry / artifact-delivery / infrastructure failures into
separate ledgers, retries within a bounded budget, refuses to resume when any
frozen input digest changes, and marks the Oracle complete only when every
workload has complete repetition blocks for origin *and* its declared
candidate.

### Measurement provider

Service cost and transferred bytes come from the reconciled access events.
Storage, materialization and transition quantities are not observable from the
access path, so they arrive through a content-hashed operator manifest bound
to the run by `pilot_id`, preregistration digest, endpoint-registry digest and
execution node. A manifest measured for a different run is refused, and a
`(design, object, node)` the manifest does not cover raises rather than
defaulting to zero.

Each component declares one of `measured`, `configured`, `derived`,
`unavailable`, or `not_applicable`. The last two are deliberately distinct:
an origin design **materializes nothing**, so its materialization term is a
justified zero, whereas a storage figure the operator could not measure
suppresses `total_cost` entirely.

### The production FlowMesh seam

`FlowMeshDistributedSessionExecutor` is the whole seam between the
distributed runner and FlowMesh. It builds the request, calls
`adapter.recover(session_id)` **before** `adapter.run(request)`, and
translates the result. `FlowMeshAgentAdapter` remains the only owner of
worker verification, Gateway registration, workflow construction and
validation, submit, wait, result retrieval, telemetry reconciliation and
session completion. Nothing is reimplemented.

Session ids are deterministic per `(cell, attempt)`. The attempt number is
part of the id because the adapter marks a session FAILED when submission
fails and then refuses to retry it -- correctly, since a silently reused
session would hide a real fault. A fresh id per attempt keeps the bounded
retry budget usable while leaving each attempt individually recoverable.

### Crash recovery

Each cell advances through a durable journal:

```
PLANNED -> STARTED -> FLOWMESH_BOUND -> RESULT_OBTAINED
        -> CANONICAL_WRITTEN -> COMPLETED
```

Each transition is fsynced before the action it authorises, so a crash is
observed at a known state rather than inferred from which of two files
happened to be written. On resume:

* a cell at `CANONICAL_WRITTEN` has its durable record **replayed**, not
  re-executed;
* a cell whose session FlowMesh already completed is **recovered**, never
  resubmitted;
* a session registered but never bound to a task is **ambiguous** -- whether
  FlowMesh accepted work is unknowable -- so it is non-retryable and the cell
  is left incomplete for an operator to resolve, rather than risking the same
  workload running twice on the worker.

### Byte accounting

Network quantity covers every representation that crosses an endpoint, not
only artifact downloads:

* inline representations are charged their reconciled `bytes_read`;
* artifact representations are charged their completed `artifact_bytes_sent`
  and *not* their `bytes_read`, because the latter is the small
  handle-metadata response and adding both would count one payload twice;
* an endpoint declared `network_transport: local` contributes a justified
  zero, and the declaration requires a `network_zero_justification`;
* missing or incomplete transfer telemetry makes the network component
  unavailable, which suppresses `total_cost`.

Charging only artifact downloads would have made origin -- which returns its
digest inline -- look as though it used no network at all.

### Telemetry reconciliation

The Gateway persists `endpoint_id` on every access event and asks that exact
endpoint for its telemetry. Polling every endpoint would work only until two
of them minted the same access id, at which point the first answer would
silently belong to the wrong node. A legacy record without endpoint identity
resolves against a single-endpoint registry and fails closed against a
multi-endpoint one.

### MCP Gateway routing

`serve-flowmesh-tools --endpoint-registry` builds the same
`EndpointRegistry -> per-endpoint HttpDataAgentClient -> RoutedDataAgentBackend
-> AccessGateway` stack the runner uses. The MCP Gateway performs every Data
Agent access and artifact download; the worker only calls MCP. Supplying no
registry leaves the existing single-`--data-agent-url` behaviour unchanged.

The MCP process and the distributed runner must be given the same endpoint
registry, the same system config, the same Gateway SQLite state DB and the
same telemetry timeout.

### What is still not proven

Running this offline slice does **not** mean a distributed experiment has run.
It means the software path is exercised end to end against fakes. A live
pilot still requires the manual deployment steps in section 4.

## 9. Compatibility with the existing pipeline

* Canonical pilot output keeps the existing record shape, so the Reduced
  Oracle and AWM loaders read it unchanged.
* `cost_basis` defaults to `service_cost`, so every existing config and the
  frozen v2 Oracle behave exactly as before.
* Selecting `cost_basis: "total_cost"` changes **which measured scalar** the
  cost gate compares. It does not change the statistical decision rule: the
  same workload-cluster bounded-mean bound, the same Bonferroni family, and
  the same three-state gate logic apply either way.
* `total_cost` on service-cost-only data fails closed rather than silently
  degrading.
