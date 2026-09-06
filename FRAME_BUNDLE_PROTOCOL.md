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

## Not in scope here

This task covers artifact generation and validation only. Vision-model
integration, Data Agent transfer integration, and any cross-node transfer are
separate later steps.
