# RSI-Exam materialization cost evidence

The v2 replay package charges logical source-byte proxies, not measured money.
This additive receipt reconstructs **actual provider usage** for the frozen
12-video formal cohort without changing its 360 outcomes or calling a model.

Inputs are the verified v2 replay package, the matching temporal preparation,
caption and index packages, and the original durable caption-response cache.
For each frozen caption, the builder accepts only cache records with the same
window descriptor and exact request digest; the selected response digest must
also match. Earlier responses to a different request are excluded. Matching
failed validation attempts are counted because the provider processed them.
Only digests and numeric usage leave the raw cache; caption or reasoning text
is never written to the receipt.

The index package records provider usage for 12 embedding batches over 120
inputs. Because a batch can cross object boundaries, the receipt reports
embedding usage at cohort level and does **not** invent per-video allocations.

Run from a clean Git archive as required by
`EXPERIMENT_OPERATIONS_RUNBOOK.md`:

```text
python -P -m pathfinder.rsi_exam.materialization_cost_evidence
  --replay-dir <verified-v2-replay>
  --preparation-dir <matching-temporal-preparation>
  --caption-dir <matching-temporal-captions>
  --index-dir <matching-temporal-index>
  --raw-cache-dir <durable-caption-cache>
  --builder-commit <full-clean-source-commit>
  --package-id <new-immutable-id>
  --output-dir <new-nonexistent-directory>
```

This is a provider-usage receipt, not an invoice or complete path-cost model.
It intentionally leaves frame decode CPU time, N4 publication/storage,
end-to-end cold-build latency, N6 inference usage, and actual billed money
unknown. The subset of caption service durations recorded in the source cost
receipt is not cold-build wall time (caption requests ran concurrently).
Prices, discounts, billing region, and currency conversion are not frozen into
this receipt. No monetary ranking should use it until those inputs and the
missing resource measurements are independently source-bound.
