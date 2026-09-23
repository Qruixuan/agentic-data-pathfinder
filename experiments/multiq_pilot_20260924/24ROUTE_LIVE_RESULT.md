# Interleaved multi-question pilot: 24-route live integration

Status: **VERIFIED_24_ROUTE_OUTPUT**. Run directory:
`artifacts/multiq-24route-20260923t2250z`. This is a plumbing pilot on two
previously inspected videos and six public questions, not held-out evaluation.

All 24 routes followed the frozen interleaved order (one R, D, DC and I route
for each question). All FlowMesh workflows completed on the current N7 worker
alias, all route evidence re-verified offline, N1 score authenticity passed,
and `SHA256SUMS` verified all 26 files. There was no workflow retry.

| Route | Correct / 6 | Mean N6 input bytes | Mean stage bytes read | Mean component time, ms |
| --- | ---: | ---: | ---: | ---: |
| R: complete video | 5 | 3,324,243 | 8,310,706 | 14,022 |
| D: derived digest + frames | 3 | 280,315 | 1,513,890 | 45,358 |
| DC: derived through shared cache | 3 | 280,315 | 2,335,402 | 14,927 |
| I: question-aware temporal index | 5 | 281,825 | 511,666 | 30,380 |

These are observed component counters from one run, **not** end-to-end wall
latency, cloud bill or amortized full-path cost. The large D/I time variation
is dominated by individual N6 inference calls; no statistical speed claim is
supported. Stage bytes read count reads at all participating components, so
DC cache hits can still show more aggregate reads than misses. They are not a
measure of N4 origin storage I/O.

| Question | R | D | DC | I | DC cache lookups |
| --- | :---: | :---: | :---: | :---: | --- |
| `4260763967-q5` | ✓ | ✓ | ✓ | ✓ | miss / miss |
| `4130504920-q6` | ✓ | ✓ | ✓ | ✓ | miss / miss |
| `4260763967-q7` | ✓ | ✗ | ✗ | ✓ | hit / hit |
| `4130504920-q4` | ✓ | ✓ | ✓ | ✓ | hit / hit |
| `4260763967-q6` | ✓ | ✗ | ✗ | ✓ | hit / hit |
| `4130504920-q2` | ✗ | ✗ | ✗ | ✗ | hit / hit |

Thus the frozen DC episode produced four cold lookups (two representations
on each video's first question) and eight genuine subsequent hits. The I
path and complete-video R path were correct on the same five questions; D/DC
lost two of those. This is useful evidence that multiple questions per video
can expose representation and cache effects, but it does **not** establish an
optimal policy. In particular, index build/API cost and derived build cost
must be included before comparing full-path monetary cost.

The isolated pilot used an N7 route at `10.70.0.17:18780`; production N7
services remained running and healthy. Pilot N7 route and cache were healthy
after the run with zero restarts. Source-bound admission SHA-256:
`64662cd8e0e3460bc926cdfad39ebacd9de5f46065ded26d700b7c7dfc9ddc18`.
The public output contains no credentials or hidden-label values and is not
eligible for scientific claims. The pilot has two videos, six questions and
one execution per route; a held-out cohort, repeated measurements and a
complete cost model remain necessary for the RSI-Exam proposal evaluation.
