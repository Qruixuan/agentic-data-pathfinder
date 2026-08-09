# Data Agent Service and Client Protocol

Pathfinder's `DataAgentClient` is the control/data-plane boundary between the
Access Gateway and a node-local or remote Data Agent. The first client
implementation is synchronous HTTP and uses only the Python standard library.

The repository now includes a single-node, manifest-backed server with a
standard-library HTTP implementation, persistent SQLite idempotency, bearer
authentication, controlled minimum latency, and signed artifact downloads.

## Start the Server

The included manifest and data files are connectivity fixtures only. They are
not real multimodal research data.

The two values below are **placeholders**. Like the LLM credentials and the
FlowMesh PAT, the bearer token and artifact signing secret must come from a
repository-external permission-restricted configuration file or the
deployment's approved secret manager, and must not appear in Git, command
arguments, logs, shell history, or AI output — see
[Credential Handling](../flowmesh/README.md#credential-handling).

```powershell
$env:PATHFINDER_DATA_AGENT_TOKEN = "<gateway-to-data-agent-token>"
$env:PATHFINDER_DATA_AGENT_ARTIFACT_SECRET = "<artifact-signing-secret>"

python -m pathfinder serve-data-agent `
  --manifest configs/data_agent_manifest.json `
  --operation-db outputs/data_agent/operations.sqlite3 `
  --host 0.0.0.0 `
  --port 8780 `
  --public-base-url http://127.0.0.1:8780
```

Set `--public-base-url` to an address reachable by the artifact consumer. The
server exposes:

```text
GET  /healthz
POST /v1/access
GET  /v1/artifacts/<access_id>
GET  /v1/accesses/<access_id>/telemetry
```

Artifact downloads support one HTTP byte range per request. When
`PATHFINDER_DATA_AGENT_ARTIFACT_SECRET` is set, returned artifact URLs have an
expiry and HMAC signature. Without a signing secret, artifact downloads use
the same bearer authentication as the control endpoint.

## Endpoint

```text
POST <data-agent-base-url>/v1/access
```

Required headers include:

```text
Content-Type: application/json
Idempotency-Key: <access_id>
X-Pathfinder-Protocol-Version: pathfinder.data-agent/v1alpha1
Authorization: Bearer <token>  # when configured
```

The server must treat repeated requests with the same `access_id` as the same
operation. It must return the previous operation state or result rather than
executing another transfer or transformation.

## Request

```json
{
  "api_version": "pathfinder.data-agent/v1alpha1",
  "access_id": "b244a8c2-...",
  "session_id": "3e8e85ec-...",
  "trial_id": "real-agent-smoke-001",
  "plan_id": "D_structured_digest",
  "plan_epoch": 0,
  "task_class_id": "video_qa",
  "object_id": "video-001",
  "representation_id": "multimodal_digest",
  "event_index": 0,
  "latency_multiplier": 1.0,
  "binding": {
    "location": "remote_digest_service",
    "representation_size_bytes": 32768
  }
}
```

For the current MVP, `plan_id` is the static design ID and `plan_epoch` is
zero. A later plan registry will replace these placeholders with an immutable
compiled plan and authenticated `plan_capability`.

The request deliberately excludes the quoted price, expected quality, and
configured realized-cost estimate. Economic admission stays in the Access
Gateway; the Data Agent executes only the admitted physical binding.
`object_id` is optional for the legacy single-file manifest and required when
the manifest declares an object catalog.

## Successful Response

```json
{
  "api_version": "pathfinder.data-agent/v1alpha1",
  "status": "succeeded",
  "access_id": "b244a8c2-...",
  "object_id": "video-001",
  "object_catalog_version": "nextgqa-ppd-v1",
  "payload": {
    "kind": "inline_text",
    "media_type": "text/plain",
    "value": "A structured description of the video.",
    "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  },
  "telemetry": {
    "service_latency_ms": 82.5,
    "realized_cost": 0.71,
    "bytes_read": 32768,
    "location": "data-node-2/nvme",
    "cache_hit": true,
    "timings_ms": {
      "queue": 2.5,
      "fetch": 20.0,
      "transform": 50.0,
      "serve": 10.0
    }
  }
}
```

The HTTP client measures the complete round-trip time separately. The Access
Gateway uses that observed latency as `felt_latency_ms`, while the Data
Agent's internal service and stage timings remain system telemetry.

For `artifact_uri`, this first response necessarily precedes the actual
download. After the consumer finishes, the authenticated telemetry endpoint
returns measurements joined by `access_id`:

```json
{
  "api_version": "pathfinder.data-agent/v1alpha1",
  "status": "succeeded",
  "access_id": "b244a8c2-...",
  "object_id": "video-001",
  "representation_id": "compressed_video",
  "object_catalog_version": "nextgqa-ppd-v1",
  "artifact_download": {
    "download_request_count": 2,
    "completed_request_count": 2,
    "full_download_count": 1,
    "bytes_sent": 1048580,
    "transfer_latency_ms": 21.4,
    "latest_completed_at": 1770000000.0,
    "in_flight_request_count": 0,
    "telemetry_complete": true
  }
}
```

`bytes_sent` and `transfer_latency_ms` include all full and range requests for
that access. `full_download_count` counts completed requests that covered the
entire artifact in one response. The FlowMesh runner reconciles this summary
into the corresponding Gateway event after the workflow terminates.

### Reading a final summary

A transfer's bytes and duration are only known once its response body has been
written, so the Data Agent records the row *after* the last byte reaches the
client. A consumer that reads telemetry the instant its download finishes can
therefore observe a summary that does not yet include that transfer — the
counts silently read low rather than raising.

`in_flight_request_count` reports transfers that have started but are not yet
durably recorded. **A summary read while it is above zero is not final.**

A zero count alone is not sufficient either. A transfer that both starts and
finishes while the server is reading its own durable summary leaves the counter
at zero on both sides of that read, yet the summary may not include it. The
server therefore brackets the read with a monotonic per-access *transfer
generation*: it samples `(in_flight, generation)` atomically, reads the
summary, samples again, and reports `telemetry_complete: true` only when both
samples show zero in flight **and** an unchanged generation. Otherwise the
summary is published as-is with `telemetry_complete: false` so the caller keeps
polling. `in_flight_request_count` stays truthful and independent of that
verdict; consumers must gate on `telemetry_complete`, not on the counter.

`HttpDataAgentClient.get_access_telemetry` accepts `wait_for_quiescence=True`
to poll until the snapshot is stable:

```python
telemetry = client.get_access_telemetry(
    access_id,
    wait_for_quiescence=True,          # bounded; default 5s
    quiescence_timeout_seconds=5.0,
)
```

The wait is bounded, and it **fails closed**: on timeout the client raises
`DataAgentTelemetryQuiescenceError` rather than returning the provisional
summary. The provisional figures are attached to the exception for diagnostics
only. Failing the Pathfinder session is preferred over silently recording
under-counted bytes and latency as if they were complete observations.
`AccessGateway` applies the same rule and raises `TelemetryIncompleteError`; a
backend that cannot answer the question at all is treated as incomplete, since
unverifiable completeness is not completeness.

`RemoteDataAgentBackend` requests quiescence unconditionally, because Gateway
reconciliation writes the realized byte and latency figures used for analysis
and must not under-count.

Data Agents predating this contract omit `in_flight_request_count`,
`telemetry_complete`, or both. Absence is kept distinct from an explicit `0` or
`true` all the way through: the payload still parses, but the missing field
reads as `None` and is **never** inferred from the other one. Such a summary can
look perfectly settled while hiding a transfer in progress, so it is rejected
rather than trusted:

- `get_access_telemetry(wait_for_quiescence=True)` raises
  `DataAgentTelemetryUnsupportedError` on the **first** response, without
  waiting — polling cannot make a field appear that the server never sends.
- `AccessGateway` checks for the same missing fields itself, before any write,
  and raises `TelemetryIncompleteError` with `missing_fields` populated.

Reading without `wait_for_quiescence` still returns the parsed summary, with
`telemetry_supported` false and `telemetry_complete` false. An explicit
`in_flight_request_count: 0` with `telemetry_complete: true` remains a valid
result: a session that downloaded nothing reconciles to zeros normally.

#### What `telemetry_complete: true` does and does not certify

It certifies a **stable, per-process, point-in-time snapshot**. Because the
generation is monotonic and is bumped under the same lock that reserves an
in-flight slot, two matching samples prove that no transfer was reserved during
the window and none was outstanding at its edges; because the durable row is
committed *before* the counter is decremented, every transfer this process
began had already been committed before the window opened. The summary is
therefore valid at every instant in the window — a linearizable point-in-time
snapshot.

It does **not**:

- **Seal the access.** It certifies the past, not the future. Artifact URLs are
  signed with a TTL and validated on signature and expiry alone; the Data Agent
  has no knowledge of workflow state, so a late, retried, or duplicated `GET`
  can start a new transfer immediately after a complete snapshot is returned.
  Bytes transferred after reconciliation are simply not counted.
- **Coordinate multiple Data Agent processes.** The counter and generation live
  in one process's memory. A second server process sharing the same operation
  DB has its own state, and neither can certify the other's transfers.

Both gaps are **explicit follow-up requirements, not current guarantees**:
access sealing (revoking or refusing artifact URLs once a session is
reconciled) and cross-process coordination (distributed leases over the shared
operation DB). Until they exist, a run must be interpreted as reconciled with
respect to one Data Agent process at one instant.

The Gateway persists `realized_cost` but removes it from the tool response
shown to the FlowMesh agent.

## Payload Kinds

The client accepts extensible typed payloads. The current server produces:

- `inline_text` for small captions or structured digests;
- `artifact_uri` for a compressed video or other large artifact;
- `frame_uris` for sampled-frame manifests; and
- `embedding_ref` for an embedding or query-service reference.

Only `inline_text` and `artifact_uri` are currently accepted in the server
manifest. The other kinds are reserved for the next backend version.

Large video, image sets, and vectors should not be embedded directly in the
JSON or MCP response. The `value` should instead contain an authorized,
short-lived artifact descriptor.

## Manifest

[`configs/data_agent_manifest.json`](../../configs/data_agent_manifest.json)
maps a logical representation to an immutable local file and optional
plan-specific bindings. Each binding may declare:

- the expected logical location used to reject stale or inconsistent plans;
- a controlled minimum latency before applying `latency_multiplier`;
- realized cost retained as hidden Pathfinder telemetry; and
- whether the binding represents a cache hit.

A binding may override the representation file path, allowing two static
plans to point to different physical replicas while keeping the logical
representation ID stable. Relative paths are resolved from the manifest
directory. By default, `require_plan_binding` is true, so an unlisted
`plan_id` is rejected rather than silently using a fallback. Files must remain
immutable for the lifetime of an operation DB.

For a multi-object dataset, add an `object_catalog_path` to the manifest:

```json
{
  "schema_version": "pathfinder.data-agent-manifest/v1alpha1",
  "node_id": "data-node-1",
  "object_catalog_path": "data_object_catalog.json",
  "representations": {
    "compressed_video": {
      "kind": "artifact_uri",
      "media_type": "video/mp4",
      "plan_bindings": {
        "D_origin": {"location": "object_store"}
      }
    }
  }
}
```

The catalog maps stable `(object_id, representation_id)` keys to immutable
files and may override a file for a particular physical plan:

```json
{
  "schema_version": "pathfinder.data-object-catalog/v1alpha1",
  "catalog_version": "nextgqa-ppd-v1",
  "objects": {
    "video-001": {
      "representations": {
        "compressed_video": {
          "path": "../source/videos/video-001.mp4",
          "plan_paths": {
            "D_consumer_nvme": "../consumer-nvme/video-001.mp4"
          }
        }
      }
    }
  }
}
```

See [`configs/data_object_catalog.example.json`](../../configs/data_object_catalog.example.json)
for a complete fixture-sized example. A plan-specific path takes precedence
over the object's default path. Representation kind, media type, location,
controlled delay, realized cost, and cache state remain in the Data Agent
manifest rather than being duplicated per object.

## Gateway Configuration

Set the URL and optional bearer token on the Pathfinder MCP Gateway process:

```powershell
$env:PATHFINDER_DATA_AGENT_URL = "http://127.0.0.1:8780"
$env:PATHFINDER_DATA_AGENT_TOKEN = "<gateway-to-data-agent-token>"

python -m pathfinder serve-flowmesh-tools `
  --state-db outputs/flowmesh/gateway.sqlite3 `
  --data-agent-timeout 30 `
  --data-agent-max-retries 1
```

The URL can instead be passed explicitly:

```powershell
python -m pathfinder serve-flowmesh-tools `
  --data-agent-url http://127.0.0.1:8780
```

When neither the option nor environment variable is present, the Gateway uses
`EmulatedRepresentationBackend`. This preserves the existing local tests.

## Current Boundary

The present implementation is deliberately a single-process, local-filesystem
Data Agent. It persists completed operations, serializes concurrent requests
for the same access ID, and allows unrelated accesses to run concurrently.

It does not yet provide:

- object-storage or peer-transfer backends;
- transform execution, replica admission, pinning, or eviction;
- distributed leases for several server processes sharing one operation DB;
- cryptographic validation of the request's `plan_capability` (the bearer
  token currently authenticates the Gateway, while explicit plan IDs and
  locations are checked against the manifest);
- conversion of measured download bytes and time into monetary or normalized
  realized cost; or
- endpoint selection per representation shard.

Because artifact payloads return before the consumer downloads the URI, the
Gateway round-trip latency still covers only access admission, file hashing,
and descriptor generation. Artifact transfer bytes and duration are now
recorded separately and reconciled by `access_id`; analyses must use those
fields rather than interpreting the initial round-trip as full-video latency.
Reconciliation must also read a quiescent summary (see
[Reading a final summary](#reading-a-final-summary)), because transfers are
recorded after their response body is written.

The in-flight counter and transfer generation are per server process and held
in memory. They are sufficient for the current single-process Data Agent but do
not coordinate several server processes sharing one operation DB; that needs
the same distributed leases already listed above.

Two named follow-up requirements remain open, and neither is a guarantee today:

- **Access sealing.** `telemetry_complete: true` certifies a point-in-time
  snapshot; it does not stop a further download. Signed artifact URLs stay
  valid for their TTL regardless of session or workflow state, so a transfer
  arriving after reconciliation is uncounted. Sealing an access at the end of
  reconciliation is future work.
- **Multi-process telemetry coordination.** Completeness is certified per
  process. Running several Data Agent processes over one operation DB is not
  supported for telemetry purposes until the distributed-lease work lands.

See [Reading a final summary](#reading-a-final-summary) for the exact
guarantee.

The distributed version will let the compiled physical binding select a Data
Agent endpoint per representation shard. That routing belongs in the
Pathfinder resolver, not in the FlowMesh agent.
