# The `sampled_frame_bundle` representation

## Why this exists

The pilot's `sampled_frames` representation is a JSON *description* of sampled
frames — a few kilobytes of text a vision model wrote after looking at images.
The cost reality audit found that the whole experiment moved about 0.26 MiB
across the node boundary, because text descriptions are all that ever crossed
it.

`sampled_frame_bundle` is a **new, additional** representation carrying the
actual JPEG bytes. It does not replace or redefine `sampled_frames`, and the
builder never writes into the existing representation tree.

## What this artifact can and cannot claim

The JPEG bytes supplied to the historical description model were **not
retained**. The builder can therefore establish only that regenerated frames
share the same source video, sampling algorithm, frame count, frame indices,
timestamps, widths, heights, and declared encoder settings.

The manifests say exactly this:

> aligned with the frozen sampling metadata and algorithm

and never this:

> identical to the historical visual input

Both manifests carry `historical_visual_bytes_retained: false` and
`claims_byte_identity_with_historical_visual_input: false`.

## Reused sampling

Frames come from the existing `sample_video()` in `pathfinder/video_prep.py`:
uniform-midpoint temporal targets, RGB conversion, `thumbnail()` to
`jpeg_max_dimension`, JPEG `quality=82`, `optimize=True`. No second decoder or
sampling algorithm was written, and `sample_video()` was not modified. The
decoder is injectable so packaging can be tested without codecs or media.

## Input validation

Every object is validated against the frozen inputs before anything is
written; there are no defaults for missing values:

- source video filename is a bare name, present, and matches the generation
  manifest's size and SHA-256;
- `sampled_frames.json` matches its recorded SHA-256 and size;
- `frame_count`, `jpeg_max_dimension`, and `method` are read from the frozen
  representation and must agree with the generation manifest;
- the sampling method must be `uniform-midpoint`, the only one this builder
  reuses;
- `source_video_id` must match the filename, and any recorded
  `source_video_sha256` must match the file;
- after resampling, every frame's index, timestamp, width, and height must
  equal the frozen value exactly.

Duplicate, missing, unexpected, or malformed objects, and any path that is
absolute or contains `..`, are refused. Paths are additionally resolved and
required to stay inside their declared root.

## Output layout

```
<output-dir>/
├── frame-bundle-generation-manifest.json
├── SHA256SUMS
└── nextqa-val-<video-id>/
    ├── frames/000.jpg … NNN.jpg
    ├── frame_bundle_manifest.json
    └── sampled_frame_bundle.tar
```

The JPEGs are the exact bytes `sample_video()` returned. The tar contains
`frame_bundle_manifest.json` and `frames/NNN.jpg`, and its embedded manifest
is byte-identical to the external copy — both are written from one byte
string, so they cannot drift.

Schemas: `pathfinder.sampled-frame-bundle/v0.1` (per object) and
`pathfinder.frame-bundle-generation/v0.1` (top level).

The tar's own size and SHA-256 live in the **top-level** manifest, never in
the manifest embedded inside that tar, which would be self-referential.

## Determinism

Identical inputs and software produce byte-identical output trees.

- JSON is canonical: sorted keys, two-space indent, `ensure_ascii=False`,
  trailing newline.
- Tar members carry explicit `TarInfo`: `mtime=0`, `uid=0`, `gid=0`, empty
  `uname`/`gname`, mode `0644`, `REGTYPE`, USTAR format, canonical POSIX
  names, lexicographic order. Nothing is read from the filesystem, and the
  contract is Python's `tarfile`, not a host `tar` binary.
- No generation timestamp appears in any checksum-covered file.
- No absolute path appears in any output.

## Atomicity

The output directory must not already exist. Generation proceeds in a sibling
temporary staging directory and is renamed into place only after every object
completes, all hashes validate, the top-level manifest is written, and
`SHA256SUMS` verifies. A failed staging tree is removed — inputs are immutable
and regeneration is cheap. An operator-owned existing directory is never
deleted or overwritten.

## CLI

```bash
PYTHONPATH=. python -m pathfinder build-frame-bundles \
  --video-dir           "$VIDEO_DIR" \
  --representation-dir  "$REPRESENTATION_DIR" \
  --generation-manifest "$GENERATION_MANIFEST" \
  --output-dir          "$OUTPUT_DIR"
```

`--generation-manifest` may be omitted when exactly one
`generation-manifest.json` sits in `--representation-dir`; ambiguity is
refused rather than guessed. `--object-id` is repeatable for a subset.

The command performs no LLM call, no network call, and starts no service.

## Native transfer and ingestion

Generation writes bundles; `pathfinder/frame_bundle_ingest.py` and
`pathfinder/frame_bundle_transfer.py` read them back after a cross-node
download. The two layers are deliberately separate from the Agent-facing
artifact path.

### Why a second download API exists

`HttpDataAgentClient.fetch_artifact()` is the *Agent-facing* contract: it
returns only content an Agent may read as text, and it still refuses
`application/x-tar`. Widening it would turn "support frame bundles" into "an
Agent tool can return arbitrary bytes". Instead there is a separate internal
method:

```python
client.fetch_binary_artifact(request, allowed_media_types=frozenset({"application/x-tar"}))
```

`allowed_media_types` is mandatory, must be non-empty, and admits no wildcard:
a caller that has not decided which binary format it can parse has no business
downloading one. The declared type is checked before the download and the
response type after it, so a disallowed artifact costs no bytes and a lying
`Content-Type` is still caught.

Both methods share one verified-download helper, so the binary path inherits
the same boundary: exact Data Agent origin, exact artifact route, no
redirects, bearer or signed-artifact authorization, bounded `Content-Length`,
a bounded read of `max_artifact_bytes + 1`, media-type agreement with the
access response, SHA-256 agreement, and no signed URL in any exception, log,
report, or Agent-visible output. There is no unbounded `response.read()`.

The result is `DataAgentBinaryArtifact`: access ID, media type, verified
bytes, size, SHA-256, object ID, object-catalog version, served location, and
the separated latencies. It is internal — never rendered into an Agent
response.

### Bundle validation contract

`validate_frame_bundle_bytes()` parses the tar **in memory**. It never calls
`extractall()` and never writes an untrusted member path. It requires an
uncompressed tar (`mode="r:"`, so a compressed stream cannot open), regular
files only — no directories, links, devices, FIFOs, or sparse members — and
member names with no absolute prefix, no `..`, no `.`, no empty component, no
backslash, no drive letter, no control character, no duplicate, and strict
lexicographic order. Metadata must be exactly `mtime=0`, `uid=0`, `gid=0`,
empty `uname`/`gname`, mode `0644`.

#### Raw bytes, not just logical members

`tarfile` presents a *logical* view: it follows PAX and GNU extension headers,
normalizes header fields, stops at the first end-of-archive marker, and
silently ignores whatever follows. Two archives with an identical logical
member list can therefore have very different bytes, and only one of them is
what the generator produces.

So the final gate re-serializes the verified members through
**`deterministic_frame_bundle_tar()` — the generator's own serializer** — and
requires the downloaded bytes to be exactly that. There is one definition of a
canonical bundle archive and no second reader-side notion of it. A mismatch
raises `FrameBundleCanonicalizationError` (failure class
`bundle_not_canonical`), even when the logical members look equivalent.

This rejects PAX extended and global headers, GNU long-name/long-link
extensions, any non-USTAR variant, concatenated archives, appended bytes,
altered end-of-archive blocks, noncanonical record padding, and raw header
fields that `tarfile` normalized away — for example content smuggled after the
NUL terminator of `uname`, or `devmajor`/`devminor` written as octal zero
rather than NUL.

One honest boundary: `PAX_FORMAT` with no field needing an extension emits
exactly the canonical USTAR bytes, and that archive is accepted. The rule is
about bytes, not about the writer's declared format constant.

Bounds are configurable through `FrameBundleLimits`: artifact bytes, member
count, frame count, single-frame bytes, total contained bytes, manifest bytes,
and frame dimension.

The embedded manifest must be `pathfinder.sampled-frame-bundle/v0.1` with
exactly the v0.1 key set — a missing *or* unexpected field is refused, so
adding a field requires bumping the schema. It must declare
`representation_id: sampled_frame_bundle`, the requested object ID, a frame
count matching the actual members, unique canonical frame indices `0..n-1` in
ascending order, member paths matching `frames/NNN.jpg` exactly, per-frame
JPEG sizes and SHA-256 values matching the bytes carried, a total matching
both the entries and the members, finite non-negative timestamps, positive
bounded dimensions, and structurally valid source metadata.

`historical_visual_bytes_retained`,
`claims_byte_identity_with_historical_visual_input`, `llm_called`,
`credentials_recorded`, and `network_calls_performed` must each be the
**literal boolean `false`**. `0`, `""`, `"false"`, and `None` are all refused:
a falsey substitute is a malformed manifest, not a no.

The alignment statement is checked by *positive* evidence — it must assert
both that the frames are aligned with the frozen sampling metadata and that
the artifact does not claim byte identity with the historical visual input. A
forbidden-substring blocklist would be wrong here, because the correct
disclaimer contains the very phrase it negates.

### Pixel decoding is deferred

Validation is structural and cryptographic, not perceptual. Each frame is
checked for JPEG SOI/EOI markers and for the width and height declared in its
own start-of-frame segment, parsed by walking segment headers only. **No pixel
is reconstructed**, because decoding would add a mandatory runtime image
dependency to ingestion. `DEFERRED_DECODE_REQUIREMENTS` records what a
decoding adapter must therefore enforce itself: bounded pixels per frame and
per bundle, a decompression-bomb guard, a per-frame decode timeout, agreement
between decoded and manifest dimensions, and refusal of any frame whose
decoder emits an unclassifiable warning.

### Delivery and telemetry reconciliation

`fetch_validated_frame_bundle()` performs access, bounded binary download,
digest and bundle validation, a quiescent telemetry read, and strict delivery
checks. Success requires `telemetry_complete`, `in_flight_request_count == 0`,
at least one download request, one completed request, one full download, and
`bytes_sent >= artifact size`; the report also states whether the counters
were *exactly* one full download and *exactly* the artifact size.

#### A refused bundle is still a transfer

Once an artifact download has *started*, the bytes have crossed the network
and consumed remote resources — whether or not the bundle is later accepted.
Stopping at the validation failure would keep the observation out of the
record correctly but lose the audit evidence for a transfer that really
happened.

So after a post-download failure, `fetch_validated_frame_bundle()` makes a
**best-effort** quiescent telemetry read and attaches the result to the
original exception, which is re-raised unchanged. The bundle error stays the
primary classification; telemetry is secondary evidence that can never
displace it, and a failed reconciliation is recorded rather than raised.

`transfer_audit_of(error)` returns the attached `FrameBundleTransferAudit`:

```json
{
  "primary_failure_class": "bundle_not_canonical",
  "primary_error_class": "FrameBundleCanonicalizationError",
  "primary_message": "...",
  "execution_phase": {
    "access_completed": true,
    "artifact_download_started": true,
    "artifact_download_completed": true,
    "bundle_validation_completed": false,
    "telemetry_reconciliation_attempted": true
  },
  "telemetry_reconciliation": {
    "status": "complete",
    "telemetry_complete": true,
    "download_request_count": 1,
    "full_download_count": 1,
    "bytes_sent": 481280
  }
}
```

`status` is `complete`, `incomplete`, `unavailable` (reconciliation itself
failed — `error_class`/`error_message` say why), or `not_attempted`. Counters
are `null` unless the Data Agent actually reported them: **absence is silence,
never a measured zero**. Telemetry is not attempted at all when no transfer
started — an access failure, a rejected artifact URL, or a declared media type
refused before download. Messages have any query string stripped before they
are written.

None of this softens the success path: a completed observation still requires
full fail-closed reconciliation, and a failed validation still writes only
`smoke-failure.json`.

Every one of these fails closed with a stable failure class —
`telemetry_unsupported`, `telemetry_not_quiescent`, `telemetry_incomplete`,
`no_download_recorded`, `no_completed_request`, `partial_download_only`,
`bytes_sent_below_artifact_size`, `object_id_mismatch`,
`catalog_version_mismatch`, `representation_id_mismatch`, plus the artifact
and bundle classes (including `bundle_not_canonical`). A delivery or telemetry failure never becomes a completed
observation.

### Vision-adapter seam

`ValidatedFrameBundle` implements `FrameBundleVisionSource`, whose entire
surface is:

```python
bundle.vision_frames() -> tuple[VisionFrame, ...]
```

Each `VisionFrame` carries frame index, timestamp, width, height, media type,
and verified JPEG bytes — nothing else. The adapter never receives the tar,
the manifest, or a Data Agent URL. There is no `to_agent_content()`; the only
text projection, `agent_visible_summary()`, is metadata only and has no
opt-out. No provider is implemented here.

## Transfer smoke CLI

```bash
PYTHONPATH=. python -m pathfinder run-frame-bundle-transfer-smoke \
  --data-agent-url            "$DATA_AGENT_URL" \
  --object-id                 "$OBJECT_ID" \
  --plan-id                   "$PLAN_ID" \
  --location                  "$REQUESTED_LOCATION" \
  --output-dir                "$OUTPUT_DIR" \
  --expected-sha256           "$EXPECTED_SHA256" \
  --expected-size-bytes       "$EXPECTED_SIZE" \
  --expected-catalog-version  "$CATALOG_VERSION" \
  --retain-artifact
```

The bearer token comes from `PATHFINDER_DATA_AGENT_TOKEN` and is never a
command-line argument — a token on the command line lands in shell history and
in the process table. The command refuses an existing `--output-dir`, writes
atomically, and returns non-zero on every validation or telemetry failure.

On success it writes a canonical `smoke-result.json`; on failure it writes
`smoke-failure.json` and no success report. Reports record
`credentials_recorded: false`, `llm_called: false`,
`eligible_for_scientific_claims: false`, the verified byte counts and SHA-256,
and four separated durations: Data Agent service latency, client access round
trip, artifact download elapsed, and server-reported transfer latency.

The report describes itself as **transfer and conformance evidence**. The
durations are one unreplicated transfer under uncontrolled conditions: not a
performance measurement, not a cost measurement, and not confirmatory
scientific evidence.

## Not in scope here

Vision-model integration is not implemented: no provider adapter, no LLM call,
and no prompt construction. Pixel decoding, Cost Contract v2, matched
interventions, and any AWM/OED change remain separate later steps.
