# Subdirection D4: Workflow Materialization, Reuse, Lineage, and Semantic Correctness

## 1. Scope

This subdirection studies when workflow intermediates should be retained, how future runs discover and reuse them, which results remain valid after upstream changes, and when results should be recomputed or reclaimed. It spans database materialized views, dataflow systems, ML workflow management, and model diagnostics.

The project's materialization decision `M` is a workflow-materialization problem with physical resource constraints. Versions, lineage, determinism, and compatibility are not optimization preferences; they are correctness conditions that determine whether a candidate plan is legal.

## 2. Representative Work

| Work | What It Achieves | Boundary Relative to This Project |
|---|---|---|
| [Nectar, SOSP 2010](https://www.microsoft.com/en-us/research/publication/nectar-automatic-management-of-data-and-computation-in-data-centers/) | Identifies derived data by the program that produced it and unifies caching, reuse, regeneration, and garbage collection across a data center | Establishes cross-job derived-data reuse but does not specifically address multimodal expansion, heterogeneous tiers, or delivery split points |
| [ReStore, 2012](https://arxiv.org/abs/1203.0061) | Automatically discovers and reuses materialized outputs of MapReduce jobs or operators | Targets MapReduce DAGs and existing results; distributed replicas and representation semantics are not central variables |
| [KeystoneML, ICDE 2017](https://shivaram.org/publications/keystoneml-icde17.pdf) | Performs cost-driven end-to-end ML pipeline optimization, including physical operator selection and automatic intermediate materialization | A close physical-planning precedent, but does not manage long-lived multi-version representation replicas in a data lake |
| [MISTIQUE, SIGMOD 2018](https://cs.stanford.edu/~matei/papers/2018/sigmod_mistique.pdf) | Stores and queries model-training intermediates using compression, selective reruns, and reuse for diagnostics | Optimizes model diagnosis and state queries rather than normal data-delivery paths |
| [HELIX, PVLDB 2019](https://www.vldb.org/pvldb/vol12/p446-xin.pdf) | Reuses historical execution and selects materializations during iterative ML workflow development, accounting for operator and ancestor changes | Focuses on workflow evolution within a logical pipeline rather than cross-node replicas and execution split points |

## 3. State of the Art

### 3.1 Automatic derived-data reuse is not new

Nectar treats “program plus input” as the identity of derived data and unifies caching, reuse, recomputation, and garbage collection. ReStore and later workflow systems identify common subcomputations automatically. Hashing a preprocessing DAG to find duplicate results would therefore be insufficient as a contribution.

This project should inherit content- and computation-addressed identity: a representation is identified by logical input version, transformation code, parameters, randomness, and environment dependencies, not by filename or path.

### 3.2 Cost-driven ML materialization already exists

KeystoneML jointly chooses physical operators and intermediates, while HELIX decides between reuse and re-execution as ML workflows evolve. Automatic intermediate materialization within a workflow is established prior art.

The project's possible increment lies in spatial and semantic dimensions: the same derived result may have replicas across tiers and nodes, change size dramatically across transforms, and serve multiple future workflows or model versions.

### 3.3 Correct reuse requires structural equivalence

HELIX checks whether an operator or its ancestors changed, and Nectar relies on producer-program identity. At minimum, this project must enforce comparable structural equivalence:

- changes to resize dimensions, interpolation, or color space invalidate image results;
- tokenizer, vocabulary, prompt-template, or model-weight changes affect tokens and embeddings;
- random crop or mixup results are exactly reusable only when inputs, seeds, and execution semantics agree; and
- approximate or reduced-precision forms may be consumed only by jobs that explicitly accept their quality contract.

### 3.4 “Reusable” and “worth retaining” are separate decisions

A deterministic result may be semantically reusable yet uneconomical to materialize. Benefit depends on production cost, expected reuse, read cost, size, capacity contention, and invalidation probability. Checking legality first and physical benefit second both reduces the search space and prevents learned search from exploring incorrect plans.

## 4. Designs to Reuse

### 4.1 Layered identity and compatibility

```text
LogicalObjectID
  + SourceVersion
  + OperatorCodeVersion
  + Parameters
  + RandomnessContract
  + Model/VocabularyVersion
  + QualityContract
  -> RepresentationVersionID
```

Byte identity, exact semantic equivalence, and quality-acceptable compatibility should be distinct levels. The first prototype should automate only byte-identical or exactly equivalent reuse and leave approximate reuse as an extension.

### 4.2 HELIX-style change propagation

When an upstream input or operator changes, propagate invalidation through the representation DAG while preserving unaffected common ancestors. This is more precise than clearing an entire dataset cache.

### 4.3 Nectar-style recomputation and reclamation

The catalog should record both existence and regeneration paths with estimated costs. Under capacity pressure, reclaim objects that are cheap to recompute, rarely reused, or easily reconstructed from a nearby parent.

### 4.4 KeystoneML-style joint physical operator and materialization choices

A physical operator can change output layout, size, and reusability, so implementation and materialization choices cannot be fully separated. The first prototype can restrict each transformation edge to a few implementations, such as local, remote, and storage-side.

### 4.5 Provenance as input to an independent plan validator

The optimizer proposes a plan; an independent validator checks that every consumer version can legally be produced from the selected representations and transformations. Correctness should reside in this validator rather than in a learned ranking model.

## 5. Remaining Research Gaps

### 5.1 Semantic reuse and distributed physical location are usually separated

Workflow systems decide whether a result is reusable, while cache systems decide where an object should reside. They rarely compare recomputing at the source, reading remotely, replicating a parent then transforming locally, or directly replicating the final representation within one representation graph.

### 5.2 A unified randomness and compatibility contract is missing

Prior systems account for nondeterminism, so the claim cannot be that randomness was ignored. The narrower gap is one contract shared by the catalog, distributed cache keys, and physical planner, validated against realistic augmentation workflows.

### 5.3 Version explosion in model-derived representations

Tokens, embeddings, and features branch as models, weights, prompts, and preprocessing versions change. A cache object per version can quickly exhaust capacity. Retention must account for future demand, parent representations, and recomputation cost instead of relying only on TTL or LRU.

### 5.4 End-to-end lifecycle cost

Creating, validating, migrating, replicating, and reclaiming representations all consume resources. Existing workflow materialization provides the benefit-model foundation, but this setting must add cross-tier and cross-node transition costs plus shared benefits across jobs.

## 6. Implication for This Project

D4 is both precedent and constraint: common-subcomputation discovery, lineage-based reuse, and cost-driven materialization already exist. The project should not claim to invent them.

A more promising contribution is to extend these ideas into **distributed physical planning over a multimodal representation graph**. For each legal representation version, the planner compares retention, replication, remote reads, local recomputation from a parent, and transforms at different locations. The semantic layer proves legality; the physical layer decides which legal path is worth executing.
