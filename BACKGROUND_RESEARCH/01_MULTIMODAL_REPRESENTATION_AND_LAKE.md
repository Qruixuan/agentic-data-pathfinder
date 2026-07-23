# Subdirection D1: Multimodal Representation and Physical Formats for AI Data Lakes

## 1. Scope

This subdirection asks how images, audio, video, documents, tensors, and embeddings should be physically organized to support versioning, random access, scans, streaming reads, and model training.

It addresses the storage and representation substrate, not the complete optimization problem of this project. Even an efficient format for compressed video, decoded frames, and tensors does not normally decide which representation to materialize for future jobs, where to place its replicas, or on which side of a network boundary decoding should occur.

## 2. Representative Work

| Work | What It Achieves | Boundary Relative to This Project |
|---|---|---|
| [Deep Lake, CIDR 2023](https://vldb.org/cidrdb/2023/deep-lake-a-lakehouse-for-deep-learning.html) | Organizes complex and variable-shape multimodal data as tensors, with versioning, lineage, queries, and streaming access | Focuses on data organization and access APIs rather than jointly deciding cross-tier materialization, replication, and operator placement |
| [The Tensor Data Platform, CIDR 2023](https://mail.vldb.org/cidrdb/2023/the-tensor-data-platform-towards-an-ai-centric-database-system.html) | Proposes a tensor-centered abstraction for storage, query, and management of AI workloads | Presents an AI-centric platform vision rather than an online joint physical planner for versioned representation paths |
| [Progressive Compressed Records, PVLDB 2021](https://www.vldb.org/pvldb/vol14/p2627-kuchnik.pdf) | Stores multiple progressively readable fidelity levels within one record so consumers read only what a task needs | Optimizes encoding and bandwidth; the format supplies candidates but does not plan dynamic placement and conversion across a cluster |
| [Lance, 2025](https://arxiv.org/abs/2504.15247) | Provides a columnar format for multimodal AI data with both scans and random access, including adaptive structural encoding | Optimizes access inside a format rather than the lifecycle of derived representations across storage, network, and compute |
| [Bullion, CIDR 2025](https://www.vldb.org/cidrdb/2025/bullion-a-column-store-for-machine-learning.html) | Designs a column store for wide, sparse, and multimodal ML data, including quantization and quality-aware reads | Improves internal organization and reads but generally separates layout choices from distributed transform placement |

## 3. State of the Art

### 3.1 Multimodal data can now be managed as structured physical objects

Deep Lake, Lance, and Bullion show that nested, variable-length, sparse, high-dimensional, and multimodal data no longer has to be reduced to an unstructured collection of JPEG, MP4, and JSON files. Random sample access, column projection, sequential scans, versioning, and schema evolution are becoming standard capabilities of AI-oriented formats.

Therefore, designing another multimodal storage format should not be the primary contribution of this project. A better position is to reuse these formats, or ordinary object storage, and expose their chunks, columns, tensors, and versions as candidate physical representations to a planner.

### 3.2 One logical object can expose multiple granularities or fidelity levels

Progressive Compressed Records shows that one physical layout can support multiple fidelity levels and let a workload fetch only the necessary bytes. Bullion likewise incorporates quantization and quality requirements into the read path. Representation selection may therefore choose not only among separate artifacts, but also among layers, column subsets, resolutions, or fidelity levels inside one encoding.

The project's representation graph should support two edge types:

- **Materialized derivation:** compressed video is decoded into frames, or frames are transformed into embeddings.
- **Projection or quality selection:** a consumer reads a lower resolution, a column subset, or reduced precision from the same progressively encoded object.

The first prototype does not need to implement a new progressive encoding. It only needs to model these capabilities as optional representation properties.

### 3.3 Versioning and lineage are becoming infrastructure

Systems such as Deep Lake treat versioning and lineage as part of AI data management. This helps the project define stable logical identities and record which code, parameters, and upstream versions produced a representation.

Recording lineage is not sufficient to decide whether a representation is reusable across workflows. Reuse also depends on determinism, random seeds, model versions, approximate-quality requirements, and consumer compatibility. D4 studies these semantic conditions.

## 4. Designs to Reuse

1. **Separate logical identity from physical representation.** One logical sample may map to several versioned tensors, chunks, or derived artifacts; location must not be part of its identity.
2. **Use explicit representation descriptors.** Record schema, shape or length distribution, encoding, precision, average and tail sizes, access granularity, producer version, and quality level.
3. **Maintain separate random-access and sequential-scan statistics.** Training shuffle and offline embedding scans favor different layouts, so one nominal throughput value is insufficient.
4. **Prefer immutable data with version pointers.** This makes cache validation and lineage substantially easier than in-place mutation.
5. **Treat format capabilities as planner constraints.** For example, a whole-object-only format cannot support a column-pruning plan, while progressive formats can expose quality-versus-byte tradeoffs.

## 5. Remaining Research Gaps

### 5.1 Workload-driven representation lifecycle

Format research usually explains how to store and read a representation, but not whether it is worth converting a compressed object to a tensor and retaining it for the next ten jobs or three hours. That decision jointly depends on the reuse horizon, production cost, capacity contention, and expiration cost.

### 5.2 Joint representation and network-boundary decisions

Decoding may expand data by orders of magnitude, while filtering, tokenization, or embedding may shrink it. Whether to transform before or after transfer depends on CPU availability at both endpoints, link contention, batching efficiency, and future reuse. Formats generally do not make this cross-resource decision.

### 5.3 Cross-job and cross-node coordination of derived representations

A format can identify versions, but it does not naturally decide which nodes should retain which replicas or whether several jobs should share, migrate, or recompute a derived artifact. This gap must be addressed jointly with D3, D4, and D5.

## 6. Implication for This Project

D1 has made substantial progress on structuring and accessing multimodal data. This project should not compete by introducing another format. Its contribution should be workload-driven planning of physical data paths above existing formats.

The most stable interface is to treat formats, object stores, and catalogs as replaceable substrates that expose representation properties, versions, access granularity, and locations. This keeps the first paper focused while leaving room to support additional data-lake formats later.
