# RSI-Exam materialization-aware offline replay (v2)

This is an additive replay format. The v1 package and its measured warm-path
outcomes remain unchanged. A v2 policy still selects among frozen physical
actions; no UpCloud, FlowMesh, video, model call, or hidden label is needed to
run the evaluator.

## What changed

Each case starts with no materialization, no temporal index, and cold caches.
The action's frozen semantic-input profile selects the required build
components:

| Input profile | One-time components |
| --- | --- |
| Direct video | none |
| Indexed raw | sampling, captions, embedding, index projection |
| Derived frames | sampling, N4 frame bundle |
| Derived digest | sampling, captions, N4 digest |
| Derived fusion | sampling, N4 frame bundle, captions, N4 digest |

One-time work is charged when first needed for an object. In
`shared-dataset-sequence`, later queries reuse the same components; in
`independent-query`, every query starts cold. Shared sampling and captions
are charged once even if an index and a derived representation both need
them. Cache hit/miss outcomes still come only from measured v1 cells.

The sampling source-byte charge equals the frozen source video's size. It is
a **logical complete-object-size proxy**, not a claim that actual disk I/O
was measured. Frame/digest output sizes come from a verified N4 package.
Optional preparation and caption packages strengthen the source binding and
provide intermediate output-byte sizes. They must be supplied together and
must cover exactly the v1 objects.

Historical caption/embedding provider usage, cold-build latency, CPU time,
and monetary cost are `null` when unavailable. `null` is never treated as
zero. The built-in baseline ranking is explicitly limited to task quality
and known byte proxies; it is not a dollar-cost or cold-latency ranking.
Measured cold materialization and inference usage must be collected before
making priced claims.

## Build from a pinned, clean source revision

First follow `EXPERIMENT_OPERATIONS_RUNBOOK.md`: use the exact committed Git
archive as the Python import root, run with `-P`, and verify every input and
checksum. Do not use an uncommitted Windows working tree to freeze a formal
package. The command below uses symbolic paths deliberately; never paste
credentials or hidden-label paths into an offline package.

```text
python -P -m pathfinder build-rsi-exam-offline-replay-v2
  --v1-package-dir <verified-v1-package>
  --n4-package-dir <verified-public-n4-package>
  --builder-commit <full-commit-of-clean-source>
  --package-id <new-immutable-id>
  --output-dir <new-nonexistent-directory>
```

When the exact matching source packages are available, also provide
`--preparation-dir <verified-preparation>` and
`--caption-dir <verified-captions>`. If their object sets or derivation
commitments differ, the builder fails closed rather than importing costs
from another cohort.

Verify the new package with `verify-rsi-exam-offline-replay-v2`, supplying
`--v1-package-dir` and `--n4-package-dir` for source checks. Then use
`run-rsi-exam-offline-replay-v2` or
`compare-rsi-exam-offline-replay-v2-baselines`. Both are offline operations.

## Measurement gap for a priced proposal

For a future cohort, record source reads, frame decode/encode CPU time,
caption and embedding request usage, N4 publication/retention, and online N6
inference usage in source-bound per-object receipts. Those receipts must
distinguish shared preparation from representation-specific work and be
frozen before replay evaluation. The 2026-09 formal run does not contain all
of these historical measurements, so its monetary generation cost cannot be
reconstructed exactly or reported as zero.
