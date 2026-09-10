# Eight-Node, 4 x 8 Pathfinder Simulator Experiment Specification

Status: **draft for adviser review; not preregistered or executed**

## 1. Goal

Build an eight-node multimodal data-lake simulator and test whether Pathfinder
can choose an appropriate physical data path from workload and system state.
The experiment crosses:

- four workload classes; and
- eight fully specified physical designs.

This produces 32 experimental cells. The design records physical measurements
first—bytes, I/O, network, CPU/GPU time, cache and latency—and derives cost from
a separate rate card. It does not assign an arbitrary fixed service cost to
each design.

The earlier eight-row abstract factor table is replaced by four complete data
paths evaluated under two network profiles. This gives clearer comparisons and
an implementable topology.

### 1.1 Representative-system scan

The configuration is grounded in the following representative systems and
public infrastructure profiles. The scan supports the *classes* of design; it
does not justify copying a vendor's peak number into the simulator as measured
performance.

| Source | Representative property | Consequence for this experiment |
| --- | --- | --- |
| [VStore](https://arxiv.org/abs/1810.01794) | chooses among many physical video formats while trading quality and multiple resources | include raw and workload-matched derived representations |
| [VSS](https://arxiv.org/abs/2103.16604) | uses temporal/physical constraints, materialized views and bounded caches | include indexed ranges, materialization and execution-local caching |
| [TASM](https://db.cs.washington.edu/projects/visualworld/tasm.pdf) | changes physical video layout to avoid reading/decoding irrelevant regions | include scan versus selective indexed access |
| [Ceph hardware guidance](https://docs.ceph.com/en/latest/start/hardware-recommendations/) | separates bulk HDD data from SSD metadata/WAL and recommends bonded 25+ Gb/s for OSD nodes | model the cold tier as HDD OSDs with SSD metadata and a 25 Gb/s uplink |
| [AWS C6gn](https://aws.amazon.com/ec2/instance-types/c6g/) and [storage-optimized instances](https://aws.amazon.com/ec2/instance-types/storage-optimized/) | expose 75–100 Gb/s networking and multi-device local NVMe configurations | retain a 100 Gb/s high-throughput core profile and local-NVMe designs |
| [UpCloud GPU configurations](https://upcloud.com/docs/products/gpu-servers/configurations/) and [storage pricing](https://upcloud.com/global/pricing/) | provide an 80 GB H100-class plan and service-defined block-storage tiers | use a deployable inference-node shape, but benchmark storage instead of claiming an undisclosed physical medium |
| [Amazon S3 performance guidance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-guidelines.html) | recommends parallel requests and byte-range GETs for large objects | count request parallelism and raw range reads rather than treating an object read as one opaque cost |
| [CloudSeg](https://www.usenix.org/system/files/hotcloud19-paper-wang.pdf) and [REACT](https://www.microsoft.com/en-us/research/wp-content/uploads/2023/04/REACT-IoTDI2023.pdf) | show that video form, bandwidth and 30/50 ms edge-cloud latency materially affect analytics | use a separately labelled edge stress profile and report representation-dependent bytes |
| [Milvus HNSW documentation](https://milvus.io/docs/hnsw.md) | exposes explicit build/search recall-latency parameters | tune HNSW on validation data, then freeze all parameters and measure build cost |
| [NExT-QA](https://openaccess.thecvf.com/content/CVPR2021/papers/Xiao_NExT-QA_Next_Phase_of_Question-Answering_to_Explaining_Temporal_Actions_CVPR_2021_paper.pdf) | contains 5,440 videos with causal, temporal and descriptive questions | ground `W1`–`W3` in an existing task taxonomy |
| [MSR-VTT](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/06/cvpr16.msr-vtt.tmei_.pdf) | contains 10,000 diverse web clips with video-language annotations | ground the 10,000-object retrieval track instead of inventing a synthetic lake size |

The scan yields three representative physical archetypes: a cold object tier,
a warm derived-data tier, and an execution-local NVMe tier. They are exercised
under a high-throughput core network and a constrained edge-cloud profile. A
long-video sensitivity track may later use [Ego4D](https://ego4d-data.org/docs/),
but it is not part of the first 4 x 8 experiment.

## 2. Decisions required before freezing

The values below are proposed simulator defaults, not measurements of existing
hardware. Before preregistration, approve or revise:

1. the four workload classes;
2. the node capacities and link profiles;
3. the NExT-QA reasoning cohort and MSR-VTT retrieval cohort;
4. the exact definitions of designs `D0`–`D7`;
5. the index, representation and cache rules;
6. the quality non-inferiority margin; and
7. whether an eight-node cloud reproduction is required for the first paper
   result.

After approval, these fields become immutable versioned manifests.

## 3. Eight-node topology

The nodes are logical simulator resources. Later, each role can map to one
container, VM or physical host.

| Node | Role | CPU and memory | Storage | Contents | Data-plane link |
| --- | --- | --- | --- | --- | --- |
| `N1 control-0` | Pathfinder, scheduler and ledger | 8 vCPU, 32 GiB | 2 x 480 GB enterprise SATA SSD, mirrored | plans, task queue, telemetry | 10 Gb/s; control only |
| `N2 index-0` | catalogue and indexes | 16 vCPU, 64 GiB | 2 x 1.875 TB local NVMe, mirrored | object, temporal, event and ANN indexes | 100 Gb/s core |
| `N3 origin-cold-0` | Ceph-like cold object tier | 32 vCPU, 128 GiB | 8 x 12 TB 7,200 RPM HDD, one OSD per disk; 2 x 960 GB enterprise SSD for DB/WAL | authoritative raw videos | bonded 25 Gb/s |
| `N4 origin-warm-0` | warm derived-object tier | 16 vCPU, 128 GiB | 4 x 3.75 TB local NVMe, one data volume per device | frame bundles and digests | 100 Gb/s core |
| `N5 materializer-0` | representation construction | 32 vCPU, 128 GiB | 2 x 3.75 TB local NVMe scratch | temporary decode/build state | 100 Gb/s core |
| `N6 inference-0` | fixed multimodal model | 12 vCPU, 240 GiB, one 80 GB H100-class GPU | 1.875 TB NVMe | frozen model and tokenizer | 100 Gb/s core |
| `N7 execution-core-0` | core executor | 16 vCPU, 64 GiB | 1.875 TB local NVMe; cache budget defined below | executor and optional local data/index | 100 Gb/s core |
| `N8 execution-edge-0` | edge executor | same CPU, RAM and storage as `N7` | same cache budget as `N7` | same software as `N7` | 10 Mb/s, 30 ms primary edge stress profile |

`N7` and `N8` must remain identical except for the data-plane network. This
makes the network contrasts interpretable.

In a cloud deployment, node roles map to measured service classes, not assumed
physical devices. For example, an UpCloud block volume must be reported as its
declared storage tier plus calibration results; it must not be relabelled
"HDD" or "NVMe" unless the provider actually guarantees that medium.

### 3.1 Storage calibration contract

The scan supports the media and tier choices, but not the earlier invented
throughput/IOPS table. Storage performance will therefore be generated by an
exact calibration protocol rather than copied from a product peak:

| Tier | Calibration before freeze | Simulator input |
| --- | --- | --- |
| cold HDD + SSD metadata | `fio` sequential 1 MiB and random 4 KiB reads at queue depths 1, 8 and 32; three runs after cache drop | empirical service-time distributions by request class |
| warm and local NVMe | the same reads plus writes at queue depths 1, 8, 32 and 128; three runs | empirical read/write distributions and saturation point |
| materializer scratch | sequential read/write plus the real frame-bundle generation workload | measured scratch I/O and build time |

The frozen calibration records device model, filesystem/object-store version,
mount options, raw command, concurrency, achieved throughput, IOPS, latency
percentiles and temperature. Container-only development may use a clearly
labelled provisional profile, but it is not eligible for measured-performance
claims.

### 3.2 Network profiles

| Profile | Capacity | RTT | Jitter | Loss | Queue |
| --- | ---: | ---: | ---: | ---: | ---: |
| control | 10 Gb/s | measured; provisional 0.5 ms | 0 | 0 | 8 MiB |
| core | 100 Gb/s | measured; provisional 0.2 ms | 0 | 0 | 32 MiB |
| cold-origin uplink | 25 Gb/s | measured; provisional 2.0 ms | 0 | 0 | 16 MiB |
| edge-primary | 10 Mb/s | 30 ms | 2 ms, seeded | 0 | 1 MiB |
| edge-sensitivity | 10 Mb/s | 50 ms | 2 ms, seeded | 0 | 1 MiB |

The 100 Gb/s core is a high-throughput representative profile supported by
current cloud instance classes, while 30/50 ms follows published edge-cloud
video evaluation conditions. The 10 Mb/s edge cap is a composite stress
profile aligned with the bandwidth consumed by an unmodified high-resolution
video stream in CloudSeg; it is not presented as one vendor's network product.
The primary 4 x 8 experiment uses `edge-primary` with no loss or node failure;
`edge-sensitivity` and failures are separate analyses.

## 4. Data lake and representations

### 4.1 Scale

Use two source-object-disjoint tracks rather than an invented size
distribution:

| Track | Frozen catalogue | Primary cohort | Workloads |
| --- | ---: | ---: | --- |
| reasoning | NExT-QA, 5,440 source videos | 300 labelled objects: 100 descriptive, 100 temporal and 100 causal | `W1`–`W3` |
| retrieval | MSR-VTT, 10,000 clips | 100 text-query/target-video units | `W4` |

Selection is outcome-blind, seeded and recorded with the upstream dataset
version and licence. The balanced NExT-QA cohort estimates workload-class
effects; a separately reported weighted aggregate may restore the published
23% descriptive, 29% temporal and 48% causal composition. Validation objects
used to choose index parameters are disjoint from the 400 evaluation units.

Raw, derived and index sizes come from frozen file manifests—there is no
synthetic 10 MiB/100 MiB/1 GiB size bucket. An emulator may use reduced or
sparse payloads, but must record `logical_bytes` and `physical_bytes`
separately; scaled traffic cannot be reported as measured real traffic. The
existing 36-object Pathfinder package remains a development/conformance set,
not the main evaluation cohort. Ego4D is reserved for a later long-video
sensitivity study.

### 4.2 Data forms

| ID | Format and location | Purpose |
| --- | --- | --- |
| `raw_video` | MP4 plus codec, duration, size and SHA-256 manifest; authoritative copy on `N3` HDD | full-fidelity safe path |
| `sampled_frame_bundle` | deterministic USTAR containing 16 JPEGs and a frame manifest; stored on `N4` after construction on `N5` | temporal and detailed visual evidence |
| `multimodal_digest` | UTF-8 text plus source hash, generator-model ID and event-order metadata; stored on `N4` | compact semantic evidence |
| `object_metadata` | canonical JSON containing ID, time range, representation locations, sizes and hashes; stored on `N2` | lookup and routing |

The existing deterministic Pathfinder JPEG frame-bundle format should be
reused. Historical textual `sampled_frames.json` is not treated as image data.

### 4.3 Indexes

Indexed designs use one frozen snapshot:

- B-tree exact object catalogue;
- sorted temporal/event intervals;
- HNSW cross-object retrieval index with cosine distance and `top_k=10`;
- validation grid `M in {16, 32, 64}`, `efConstruction in {100, 200, 400}`
  and query `ef in {10, 50, 100}`;
- embedding dimensionality and numeric type native to the frozen embedding
  model.

Choose the lowest-latency validation configuration that meets the frozen
Recall@10 target, then lock it before evaluation. `M=32`,
`efConstruction=200`, `ef=100` is only the initial smoke candidate, not a
claimed universal optimum. The embedding-model ID and checksum remain a
preregistration decision. Every index records its source hashes, build seed,
build resources, size and checksum.

### 4.4 Local cache

`N7` and `N8` each receive an LRU byte budget equal to 10% of the frozen
derived-object working set. The exact integer byte limit is computed from the
representation manifest and frozen; 5% and 20% are preregistered sensitivity
levels. Designs `D3` and `D7` use identical initial cache snapshots and request
traces. The complete index replica is accounted for separately and does not
consume this budget.

Each data access is labelled `local_hit`, `remote_miss` or `bypass`. Silent
remote fallback is prohibited.

## 5. Four workload classes

| ID | Workload | Input and action | Primary quality metric | Main physical pressure |
| --- | --- | --- | --- | --- |
| `W1` | descriptive single-object QA | known object ID; identify the dominant action | normalized MCQ accuracy | digest versus raw transfer/decode |
| `W2` | temporal reasoning | known object ID; determine event order or timing | normalized MCQ accuracy | temporal index and ordered frames |
| `W3` | causal/multi-evidence reasoning | known object ID; combine multiple visual events | normalized MCQ accuracy | multiple representations and evidence volume |
| `W4` | cross-object video retrieval | MSR-VTT text query with hidden target ID; search and rerank the 10,000-clip catalogue | Recall@10 and rank of the labelled target | ANN index, evidence reads, global scan and cache placement |

The independent unit is the source video, not a repetition. `W4` is essential:
without unknown-object retrieval, “indexing” would mostly be exact ID lookup
and would not represent a realistic data-lake decision.

## 6. Eight designs

The first four use the core executor; the latter four repeat the same data paths
through the edge executor.

| Design | Executor | Data path | Storage and data form | Index | Remote link |
| --- | --- | --- | --- | --- | --- |
| `D0` | `N7` | cold raw scan | `N3` HDD, raw MP4 | exact catalogue only | 25 Gb/s cold uplink |
| `D1` | `N7` | cold raw indexed | `N2` index + `N3` HDD raw ranges | temporal/event + ANN | 100 Gb/s index; 25 Gb/s data |
| `D2` | `N7` | warm remote derived | `N2` index + `N4` NVMe digest/frame bundle | temporal/event + ANN | 100 Gb/s core |
| `D3` | `N7` | execution-local derived | local index and NVMe cache; `N4` on miss | local frozen replica | local on hit; 100 Gb/s on miss |
| `D4` | `N8` | cold raw scan | same data contract as `D0` | exact catalogue only | 10 Mb/s, 30 ms edge |
| `D5` | `N8` | cold raw indexed | same data contract as `D1` | temporal/event + ANN | 10 Mb/s, 30 ms edge |
| `D6` | `N8` | warm remote derived | same data contract as `D2` | temporal/event + ANN | 10 Mb/s, 30 ms edge |
| `D7` | `N8` | execution-local derived | local index and NVMe cache; `N4` on miss | local frozen replica | local on hit; 10 Mb/s on miss |

### 6.1 Shared workload routing

The following rules are fixed across all relevant designs:

- raw designs: `W1`–`W3` read raw evidence; `W4` scans or retrieves candidates
  and then reads raw evidence;
- derived designs: `W1` reads a digest, `W2` reads a frame bundle, `W3` reads a
  digest and frame bundle, and `W4` reads up to ten candidate digests followed
  by the selected candidate's frame bundle;
- the model, prompt, answer normalization and inference node are identical;
- no design may silently access a representation or endpoint outside its
  contract.

### 6.2 D0 — Core, cold raw scan

- `N7` executes; `N3` serves complete raw MP4 objects from HDD.
- `W1`–`W3` perform full-object transfer and decode.
- `W4` scans the 10,000-clip MSR-VTT catalogue in deterministic ID order.
- Secondary indexes and local data cache are disabled.
- Measure HDD work, raw/wire bytes, decode CPU, network, inference and quality.
- Primary role: full-fidelity minimally optimized core baseline.

### 6.3 D1 — Core, cold raw indexed

- `N2` returns object IDs or time/event ranges; raw bytes remain on `N3`.
- `W1` still fetches the full object; `W2` and `W3` fetch indexed ranges.
- `W4` uses ANN `top_k=10` before reading candidate raw evidence.
- Index query and build resources are charged separately.
- An index miss is explicit and cannot silently fall back to `D0`.
- `D1-D0` estimates index pruning with storage, executor and raw form fixed.

### 6.4 D2 — Core, warm remote derived

- `N2` serves indexes and `N4` serves precomputed derived objects over 100 Gb/s.
- Workload-to-representation routing follows Section 6.1.
- Every access reaches `N4`; the local cache is disabled.
- Materialization on `N5` is measured and amortized over a frozen horizon.
- Parent raw hashes must bind every derived artifact to its source.
- `D2-D1` is a bundled contrast—derived/NVMe versus raw/HDD—not a pure
  single-factor causal estimate.

### 6.5 D3 — Core, execution-local derived

- `N7` holds a full index replica and the frozen 10%-working-set cache.
- A hit reads local NVMe; a miss reads `N4`, inserts the object and performs LRU
  eviction; oversize objects are labelled `bypass`.
- Index replication and initial cache population are measured transitions.
- Cold-start, warm-up and steady-state results are separated.
- A local-hit record requires a matching cached-object checksum.
- `D3-D2` estimates execution-side placement and caching on the core network.

### 6.6 D4 — Edge, cold raw scan

- Same data and scan contract as `D0`, but execution moves to `N8`.
- All artifact and inference traffic crosses the 10 Mb/s, 30 ms edge link.
- Streaming uses a fixed 4 MiB application chunk size.
- No derived data, secondary index or local cache is allowed.
- Timeouts are reported as censoring/infrastructure outcomes, not wrong answers.
- `D4-D0` estimates network sensitivity for raw scanning.

### 6.7 D5 — Edge, cold raw indexed

- Same index and raw-range contract as `D1`, but execution occurs on `N8`.
- Index calls and each raw range request pay edge RTT; batching is frozen.
- Record index RPC count, range count, redundant container bytes and wire bytes.
- An index failure cannot silently become a full scan.
- `D5-D4` estimates index value at the edge.
- `D5-D1` estimates network sensitivity for indexed raw access.

### 6.8 D6 — Edge, warm remote derived

- Same immutable derived artifacts and materialization allocation as `D2`.
- Index and data requests cross the 10 Mb/s, 30 ms edge link.
- The cache is disabled and raw fallback is prohibited.
- Record RPC count, payload/wire bytes, link delay, NVMe work and inference work.
- Network and GPU savings remain separate in the resource vector.
- `D6-D2` estimates network sensitivity for remote derived evidence.

### 6.9 D7 — Edge, execution-local derived

- Same local index, cache capacity, initial state and trace as `D3`.
- Hits avoid edge artifact traffic; misses explicitly fetch from `N4`.
- Reset the cache from its signed snapshot for every independent repetition.
- Record replication, hits/misses, evictions, local/remote bytes and phase.
- Corrupt cached data is an infrastructure failure, not a scientific outcome.
- `D7-D6` estimates edge locality; `D7-D3` estimates network interaction.

## 7. Interpretable comparisons

| Question | Contrasts | Limitation |
| --- | --- | --- |
| index pruning with cold raw data | `D1-D0`, `D5-D4` | `W1` may only expose lookup overhead |
| warm derived versus cold indexed raw | `D2-D1`, `D6-D5` | changes both representation and storage tier |
| execution-local placement/cache | `D3-D2`, `D7-D6` | includes replication and cache lifecycle |
| core versus edge network | `D4-D0`, `D5-D1`, `D6-D2`, `D7-D3` | valid only if `N7` and `N8` remain otherwise identical |

If the paper requires causal attribution to storage tier and data form
separately, add a matched-byte microbenchmark or later full factorial. The
4 x 8 experiment should not overclaim bundled contrasts.

## 8. Simulator and calibration

The seeded discrete-event simulator represents index lookup, storage queues,
network transfer, decode/materialization, cache activity, inference and
canonical-record commit. Each event records trial key, parent event,
node/resource ID, virtual start/end time, logical bytes and physical bytes.

The simulator must not infer quality from a design name. Task success comes
from a frozen empirical replay table or a separately calibrated seeded response
model.

Before scientific use, calibrate HDD/NVMe operations, achieved bandwidth and
RTT, decode/materialization, artifact transfer, index latency/recall and
fixed-model inference. Calibration produces distributions and uncertainty,
rather than one hand-entered service-cost number.

## 9. Experimental phases

| Phase | Workloads | Designs | Repetitions | Sessions | Purpose |
| --- | ---: | ---: | ---: | ---: | --- |
| smoke | 4, one per class | 8 | 2 | 64 | prove every cell and audit path |
| simulator pilot | 40, ten per class | 8 | 2 | 640 | estimate variance and validate analysis |
| simulator main | 400, 100 per class | 8 | 2 | 6,400 | evaluate the frozen policy on the held-out cohort |
| eight-node emulation | 40 | 8 | 2 | 640 | compare simulator and measured rankings |
| confirmation | power-analysis result | safe + selected policies | preregistered | TBD | fresh confirmatory evidence |

Trials use seeded randomized complete blocks. Repetitions are clustered by
source object. Cache designs replay identical traces. Concurrency 1 is primary;
concurrency 4 and 16 are separate load strata.

## 10. Measurements and cost

Canonical observations record task/retrieval quality, component latency,
raw/derived/index/cache/wire bytes, disk operations and queue time, CPU and GPU
time, index work, cache events, materialization/replication, route identities,
content hashes, retries and completeness.

The primary evidence is this resource vector. A separate versioned rate card
derives cost:

```text
total_cost =
    cpu_core_seconds       * cpu_rate
  + gpu_seconds            * gpu_rate
  + storage and I/O terms
  + network and egress terms
  + amortized materialization
  + amortized index build
  + amortized replication
```

Changing the rate card is a sensitivity analysis, not a physical rerun.
Break-even results across plausible rate cards must be reported.

## 11. Policies and evaluation

Held-out evaluation compares raw scan, raw indexed, remote derived, local
derived LRU, a static workload-class policy, a cost-only policy, Pathfinder's
workload/state-conditioned AWM policy, and a complete oracle upper bound.

Candidate Pathfinder features include workload class, object size, evidence
type, storage queue, bandwidth/RTT, cache occupancy and hit probability, and
index selectivity. Test labels and outcomes are prohibited.

Proposed criteria, subject to adviser approval:

- accuracy loss no worse than two percentage points relative to safe raw;
- a separate `W4` Recall@10 non-inferiority gate;
- positive lower confidence bound for cost saving;
- lower held-out regret than static baselines; and
- byte-identical evaluation reproduction on a second machine.

Confidence intervals cluster by source object. The experiment is not judged
solely by whether OED emits `COMMIT`.

## 12. Admission checks and next steps

The main run starts only after all 32 cells complete a smoke; hashes, byte
conservation and resource equations pass; cache and index behavior replay
exactly; hidden fallback is impossible; incomplete delivery/telemetry cannot
enter canonical data; and a second evaluator reproduces the smoke.

Freeze the topology, object/representation/index manifests, workload split,
design contracts, cache snapshots, rate cards, preregistration, trial plan,
source revision and checksums.

Recommended sequence:

1. obtain adviser approval for Sections 2–6;
2. implement strict topology, data, index and design schemas;
3. implement the deterministic event engine and conservation tests;
4. add the workload generators and `W4` retrieval ground truth;
5. connect the existing AWM/OED interfaces without changing their rules;
6. run and reproduce the 64-session smoke;
7. run the 640-session pilot and power analysis;
8. freeze and run the 6,400-session simulator experiment; and
9. validate a selected subset on eight real nodes.

Simulator, emulator and cloud results remain separate evidence classes.
