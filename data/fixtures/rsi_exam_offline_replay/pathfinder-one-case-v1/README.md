# Pathfinder RSI-Exam offline replay: `pathfinder-rsi-exam-one-case-v1`

This immutable package replays measured Pathfinder physical-path outcomes without UpCloud, FlowMesh, Docker, an LLM, source videos, credentials, or hidden labels.

A policy sees only `cases.jsonl`, `actions.jsonl`, and current replay state before choosing. The evaluator reveals the matching row from `outcomes.jsonl` afterward. Missing action/state cells fail closed; the evaluator never interpolates a counterfactual.

`independent-query` charges index construction whenever the initial state lacks an index. `shared-dataset-sequence` preserves index and cache state so one-time work can amortize over later queries. Null means unmeasured; it never means zero.

This one-case package is a conformance fixture. It is not eligible for scientific accuracy, latency, GPU-performance, or causal claims.
