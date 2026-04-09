# Hierarchical VaR Benchmark — RAG Knowledge Base

## Context

This document captures the findings, architectural decisions, and realism analysis of the ONLAB benchmark project.
The production target is a KDB+ database running on a 1 TB RAM distributed grid.
The prototype is implemented in Java 17 with PostgreSQL as a stand-in for KDB+.

---

## System Architecture

### Production system
- **Database:** KDB+ (q language), columnar, in-memory, arrays are native first-class types with no boxing or serialisation overhead.
- **Compute:** Distributed grid with approximately 1 TB of RAM across many nodes.
- **Pattern:** Map step — each grid node independently computes leaf groups for its record subset. Reduce step — node results are merged element-wise across the network.
- **Vector length:** 20 000 – 200 000 doubles per record (P&L scenario vector).
- **Loading:** In KDB+, loading a table is effectively a memory-mapped pointer dereference, not a copy.

### Prototype (ONLAB)
- **Database:** PostgreSQL via JDBC, used to simulate the load-from-database step.
- **Schema:** Attributes stored as individual SQL columns (`attr1`, `attr2`, …) rather than `TEXT[]`, enabling `ORDER BY` on attributes for streaming reads.
- **Compute:** Single JVM, single machine.
- **Scale:** 3 000 – 5 000 records, up to 40 000 vector length.

---

## Data Model

A `RiskRecord` carries:
- `datasetId` — string identifier for the dataset.
- `attributes[]` — categorical group keys (e.g. desk, book, instrument type).
- `numbers[]` — the P&L scenario vector as `double[]`.

A `GroupKey` is an immutable wrapper around `List<String>` with correct `equals`/`hashCode`.

---

## Aggregation Strategies

### Baseline (HashMap)
Groups records by computing a `GroupKey` for every record and performing element-wise accumulation into a `Map<GroupKey, double[]>`.
- Time complexity: O(N) amortised.
- All running sums are live in memory simultaneously.
- `GroupKey` hash is computed from `List<String>` on every lookup — expensive vs KDB+ symbol interning.

### Streaming (sort-merge)
Sorts records by composite group key, then accumulates sequentially. Only one running sum is live at a time.
- Time complexity: O(N log N) for the sort, O(N) for the scan.
- Comparator operates attribute-by-attribute without string concatenation.

### Prefix Rollup (sort-scan)
Sorts records once by all interesting attributes. Scans linearly and detects prefix changes to simultaneously accumulate at every hierarchy level.
- O(N log N) sort + O(N × L) scan where L = number of hierarchy levels.
- Rollup across levels is free: no extra passes over group maps.
- Implements `AllLevelsAggregationStrategy`; the engine bypasses its own rollup loop automatically.

### Prefix Streaming (DB ordered)
Expects records pre-ordered by the database (`ORDER BY attr1, attr2, …`). No Java-side sort.
Single pass, one accumulator per level, closes and emits VaR immediately when a group boundary is detected.
- Most production-realistic strategy.
- Implements `StreamingVaRAggregationStrategy` and delegates to `StreamingHierarchicalVaREngine`.

---

## Selection (Percentile) Algorithms

### Sort-based (baseline)
Clones the array, calls `Arrays.sort`, indexes the result.
- O(N log N), exact, one clone per percentile call.

### Floyd-Rivest SELECT
Partition-based selection, O(N) expected. Faster than sort for a single percentile.
- For multiple percentiles: performs one independent selection per percentile, each requiring an `arr.clone()`.
- At 200 000 elements and 3 percentiles: 6 clones × 1.6 MB = 9.6 MB of heap allocation per group.
- Not the best choice when more than one percentile is needed.

### OSILA (randomised order-statistic)
Based on Cerasa (2023), DOI 10.1007/s00180-023-01381-1.
Samples without replacement, derives bounds around the target order statistic, sorts only a filtered interval.
- Designed for very large arrays where even O(N) memory access is expensive.
- At 20 000 – 200 000 elements the array fits largely in L3 cache; the sampling overhead may outweigh the benefit.

### Recommendation for multiple percentiles on large vectors
Sort once, index multiple times:
- Sort: O(N log N), one clone, then O(1) per percentile.
- For 3 percentiles on 200 000 elements this is asymptotically more expensive than Floyd-Rivest × 3 but avoids the clone multiplication and keeps the array hot in cache.
- In KDB+, `asc vector` is SIMD-accelerated and this pattern (`s: asc x; s[i1]; s[i2]`) is the standard idiom.

---

## Hierarchical VaR Engine

### Map-based engine (`HierarchicalVaREngine`)
1. Leaf aggregation: delegate to `AggregationStrategy`.
2. Extract VaR at leaf level.
3. Rollup: drop last dimension, merge sibling groups element-wise into parents.
4. Extract VaR at each parent level.
5. Repeat until the interesting dimension list is empty (root).

Timing: `aggregationMs` = leaf aggregation + all rollup aggregations. `percentileMs` = percentile extraction across all levels.

### Streaming engine (`StreamingHierarchicalVaREngine`)
Never materialises `Map<GroupKey, double[]>` for all levels.
Maintains one `double[]` accumulator per level. When a prefix boundary is detected:
- The completed accumulator is passed to the `PercentileCalculator` immediately (vector is hot in cache).
- The result is emitted to a `VaROutputSink`.
- The accumulator is reset to the incoming record.

At stream end, all open accumulators are closed in order.

---

## Benchmark Modes

### Full benchmark
Generate → insert into PostgreSQL → load from PostgreSQL → warmup → aggregate + percentile.
Measures the entire pipeline. Dominated by insert and load for small datasets.

### Query benchmark (production-realistic)
Assumes data already exists in the database.
Measures: DB read → (optional ordering) → aggregation → percentile selection.
This isolates the actual compute cost. Recommended for algorithm comparison.

---

## Critical Findings

### Bottleneck is aggregation, not selection or I/O

At 3 000 records × 40 000 vector length × 12 hierarchy levels:

```
3 000 × 40 000 × 12 = 1.44 billion element-wise double additions
```

- DB read with streaming ordered cursor: ~12 ms.
- Aggregation: ~50 seconds.
- Percentile selection: < 1 second.

The system is **memory-bandwidth bound**, not CPU bound. Optimising the selection algorithm has negligible effect on total runtime.

### Scaling law

```
runtime ∝ record_count × vector_length × hierarchy_depth
```

All three factors are multiplicative. Increasing vector length from 40 000 to 200 000 multiplies aggregation time by 5×.

---

## Realism Analysis vs KDB+ Grid

### What transfers to production

| Finding | Transfers? | Notes |
|---|---|---|
| Aggregation is the bottleneck | Yes | Memory-bandwidth bound on any hardware |
| DB load is negligible with streaming | Partially | KDB+ load is even cheaper (memory-mapped) |
| Selection algorithm is not the bottleneck | Yes | KDB+ `asc` is faster still |
| Prefix rollup has lower memory footprint | Yes | Structural argument, hardware-independent |
| Relative ordering of strategies | Yes | Bottleneck structure is preserved |

### Where the Java benchmark is pessimistic (KDB+ would be faster)

**`addInPlace` is not SIMD-vectorised in Java.**
The inner loop `for (int i = 0; i < len; i++) target[i] += source[i]` is not guaranteed to auto-vectorise due to JVM bounds checks and object headers.
KDB+ executes the equivalent `+` operation with AVX-512, processing 8 doubles per clock cycle.
Expected difference: 4–8× for the aggregation step.

**`GroupKey` hashing on every record (HashMap strategy).**
Java `List<String>` hashCode re-hashes each character string on every map lookup.
KDB+ stores categorical attributes as `symbol` (interned integer), hash is a single integer comparison.
This makes the HashMap strategy unrealistically slow in the Java benchmark relative to KDB+.

**Multiple `arr.clone()` calls per group for percentile computation.**
Floyd-Rivest and OSILA both clone the accumulator array for each percentile call.
In KDB+, `asc` sorts once and indexing is O(1); no cloning.

### Where the Java benchmark is optimistic (KDB+ grid would face additional cost)

**The grid reduce step is not modelled.**
In production each grid node computes leaf groups for its record subset independently.
A reduce step then merges node results element-wise over the network.
This adds latency proportional to the number of distinct groups × vector length × network round-trip time.
The Java benchmark does not measure this at all.

**PostgreSQL load is not representative of KDB+ in-memory load.**
Even the query benchmark reads from PostgreSQL, which involves JDBC deserialisation, `Double[]` unboxing, and Java heap allocation.
KDB+ table access is a pointer into a memory-mapped region.
The `dbLoadMs` figures in the benchmark are not transferable to KDB+.

---

## Is the Custom Engine Necessary?

### In KDB+/q the entire pipeline is native

```q
/ Leaf level aggregation
leaf: select numbers: sum numbers by attr1, attr2, attr3 from t

/ First rollup (drop attr3)
l2: select numbers: sum numbers by attr1, attr2 from t

/ VaR extraction at leaf
var_leaf: select
    var01:  {(asc x)[floor .01 * count x]} each numbers,
    var99:  {(asc x)[floor .99 * count x]} each numbers
  by attr1, attr2, attr3 from t
```

`select … by` in KDB+ is SIMD-accelerated, symbol-interned, and operates on contiguous columnar memory.
The `HierarchicalVaREngine`, `StreamingHierarchicalVaREngine`, and all aggregation strategies are equivalent to a few lines of q.

### Ready-made alternatives for the Java layer

| Tool | Capability | Notes |
|---|---|---|
| DuckDB | `GROUP BY ROLLUP` + `PERCENTILE_CONT` in SQL | SIMD C++, embeddable, replaces the entire pipeline |
| Apache Arrow + Acero | Columnar groupby + quantile, Java API | No full ROLLUP support out of the box |
| Apache Datasketches KLL | Streaming approximate quantile, mergeable | Eliminates materialisation bottleneck; use if exact VaR is not required |
| DDSketch | Relative-error streaming quantile, mergeable | Correct for grid reduce step |

### When approximate quantile is acceptable

If the use case is internal risk monitoring (not regulatory Basel VaR), streaming quantile sketches (KLL, DDSketch) eliminate the aggregation bottleneck entirely:
- No full `double[]` accumulator needs to be materialised.
- Sketches merge across grid nodes in the reduce step with bounded memory.
- KLL sketch per group: O(log N) memory vs O(N) for exact.

If exact quantile is required (Basel III/IV regulatory capital), the full accumulation approach is mandatory.

---

## Floyd-Rivest — When It Is and Is Not the Best Choice

Floyd-Rivest SELECT is the best single-percentile exact selection algorithm in practice for large arrays where O(N log N) sort is too slow.

It is **not** the best choice when:
1. More than one percentile is needed from the same vector — sort-once then index is better.
2. The array is small enough to fit in L1/L2 cache — insertion sort dominates.
3. A C++/SIMD-optimised sort (KDB+ `asc`, `std::sort` with PDQS) is available — these often beat Floyd-Rivest due to instruction-level parallelism even at O(N log N).
4. Approximate quantile is acceptable — sketches are asymptotically and practically faster.

For the ONLAB prototype, Floyd-Rivest is 4–5× faster than the sort-based baseline. However, since selection is not the bottleneck, this speedup does not materially affect total runtime.

---

## Recommendations

1. **Do not reimplement the aggregation engine in Java for production.** KDB+ already provides it natively via `select … by`.
2. **Use the Java prototype to understand bottleneck structure**, not to produce production numbers.
3. **Optimise the q-code** for SIMD utilisation and cache-aware rollup ordering, not the Java engine.
4. **Profile the grid reduce step** — this is the latency component that the benchmark does not capture.
5. **Consider DDSketch or KLL** if regulatory constraints permit approximate VaR; this eliminates the aggregation bottleneck.
6. **For exact multi-percentile selection in Java**, prefer sort-once + multi-index over Floyd-Rivest × N.
