# ATP-Hard 8x5 lightweight-D supplement

This is a new experiment, not a correction to the sealed 400-route run. Keep
the original evidence and its caption-coupled D cost intact.

- Cohort: the same 8 videos and 40 public questions, in the frozen seeded
  interleaved question order. They are **already observed**; the six test
  videos are no longer unseen for policy tuning.
- New observations: 40 questions × (N7/N8 × D, DC miss, DC hit) = 240. Use
  fresh plan, run, cache-episode and output identities. Never reuse the old
  cache namespaces or state volumes.
- D construction: decode 24 uniform-midpoint JPEG frames from each verified
  original MP4, once per video. The N4 package contains only
  `sampled_frame_bundle`; no caption, digest, embedding, question or answer is
  an input to this build. The N6 frame-only profile selects four frames.
- DC: the same exact frame-bundle artifact as D; only cache access differs.
  The cold/hot pair must show real MISS/STORE/HIT events for the one artifact.
- Account separately for per-video decode/assembly CPU and wall time,
  publication/storage, per-route N6 request-bound token × frozen list price,
  actual VM experiment-time allocation, and any unknown provider attempts.
  A missing cost remains unknown, not zero.
- Old R/I results are historical controls only. The new D/DC observations
  may be compared descriptively by matched public question and N6 model, but
  cross-day model drift, deployment state and cache state remain confounders.
- The supplemented replay must not change baseline rules after inspecting
  these outcomes. Do not call this a new held-out generalization test.

Before paid submission, complete the operations runbook's full fail-closed
checklist, including canonical source-bound verification of the new N4 package,
route bindings, DAG, admission and batch config; N4 origin/catalog; both
coordinator health/auth/DNS paths; common N6 identity; worker pin; fresh cache
state; N6 usage/attempt journal baseline; and a small non-outcome-tuned canary.
