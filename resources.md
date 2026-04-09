# Summary of Benchmarking and System Design Exploration

## 1. Goal

The primary goal of this work is to evaluate different approaches for computing hierarchical VaR-style aggregations over large datasets, focusing on:

* Aggregation strategies (HashMap vs Prefix Rollup)
* Selection algorithms (Sort-based vs Floyd–Rivest / OSILA)
* Realistic, production-like execution pipelines
* Scalability toward large datasets (up to TB-scale memory environments)

---

## 2. Initial Benchmark Issue

The original benchmark included:

* Data generation
* Database insertion
* Database loading
* Aggregation
* Percentile computation

This resulted in misleading conclusions because:

* Most time was spent in **data preparation (insert + load)**
* Actual computation (aggregation + selection) was a small fraction

### Key insight:

> This benchmark measured a **pipeline**, not a **query path**.

---

## 3. Refactoring to Query Benchmark

A new benchmark mode was introduced:

### Query Benchmark (production-like)

Execution flow:

1. Data already exists in DB
2. Ordered read / streaming from DB
3. Aggregation
4. Percentile computation

Measured stages:

* DB read
* Aggregation
* Percentile
* Total

### Key benefit:

> This isolates the **actual compute cost**, making results meaningful.

---

## 4. Selection Algorithms

### Compared:

* Sort-based percentile (baseline)
* Floyd–Rivest SELECT
* OSILA variants (earlier experiments)

### Result:

* Floyd–Rivest is **~4–5× faster** than sort-based selection
* However, **selection is not the bottleneck** in the full query path

### Insight:

> Optimizing selection alone does not significantly reduce total runtime.

---

## 5. Aggregation Strategies

### A) HashMap Aggregation

* Very fast in benchmark (~150 ms)
* Requires full materialization
* Higher memory overhead

### B) Prefix Rollup (initial version)

* Required full in-memory sort
* Not production-realistic
* Did not show clear advantage

### C) Prefix Streaming Rollup (final version)

* Uses DB-ordered input
* No Java-side sorting
* Streaming processing

---

## 6. Critical Result (Most Important Finding)

Latest benchmark:

* DB read: **~12 ms**
* Aggregation: **~50 seconds**
* Percentile: **<1 second**

### Interpretation:

> The bottleneck shifted completely to **aggregation**.

Previously:

* DB read dominated (due to materialization / inefficiencies)

Now:

* With proper streaming, DB cost is negligible
* **Aggregation dominates runtime**

---

## 7. Why Aggregation Is Expensive

Given parameters:

* 3000 records
* Vector length = 40,000
* 12 hierarchy levels

Estimated operations:

```
3000 × 12 × 40000 ≈ 1.44 billion operations
```

Main costs:

* Repeated element-wise vector addition
* Memory bandwidth limits
* Frequent array cloning
* Multi-level accumulator updates

### Insight:

> The system is likely **memory-bound**, not CPU-bound.

---

## 8. Prefix Rollup vs HashMap

### Time perspective:

* Prefix rollup is **not clearly faster**
* Both approaches dominated by vector operations

### System perspective:

Prefix rollup advantages:

* Lower memory footprint
* Streaming-friendly
* No need to store all groups simultaneously
* Scales better for large datasets

### Key conclusion:

> Prefix rollup is valuable for **scalability**, not necessarily raw speed.

---

## 9. Database Schema Evolution

Original schema:

* Attributes stored as `TEXT[]`
* Not suitable for sorting/indexing

Refactored schema:

* Attributes stored as separate columns (`attr1 ... attrN`)
* Enables:

    * Efficient `ORDER BY`
    * Indexing
    * Prefix streaming

### Insight:

> Schema design is critical for enabling efficient aggregation.

---

## 10. PostgreSQL vs kdb+ Reality

Observation:

* PostgreSQL + JDBC + Java pipeline shows high read cost

Key understanding:

* kdb+ would likely be **much faster** because:

    * Columnar storage
    * In-memory processing
    * Reduced data movement
    * Vectorized execution

### Important distinction:

> Current benchmark ≠ actual kdb+ performance
> It models a **less optimized production pipeline**

---

## 11. Scaling Considerations

Estimated scaling behavior:

* Runtime ∝ records × vector length × hierarchy depth

Key observations:

* Increasing vector length dominates cost
* Increasing number of attributes has minor impact
* Increasing hierarchy depth multiplies work

### Example:

* 3000 → 5000 records
* 40k → 70k vector

Estimated runtime:
→ ~2.5 minutes on local machine
→ ~30–60 seconds on high-end server / grid

---

## 12. Main Conclusions

### What matters most:

1. Aggregation cost dominates
2. Data movement (DB read) can dominate if not optimized
3. Selection algorithm is secondary

### What does NOT matter as much:

* Choice between good selection algorithms (once optimized)
* Minor schema differences (after normalization)

---

## 13. Final Key Insights

* Moving from pipeline benchmark → query benchmark was essential
* Streaming + ordered DB input is the correct production model
* Prefix rollup is structurally better, but not automatically faster
* The real optimization target is:

  > **vector aggregation efficiency and memory access patterns**

---

## 14. Next Steps (Suggested)

To continue improving:

* Profile aggregation:

    * array allocations
    * addInPlace cost
    * boundary flush frequency
* Reduce copying (`double[]`)
* Explore:

    * SIMD / vectorization
    * off-heap memory
    * parallel aggregation
* Compare:

    * Java vs native / columnar engine performance

---

## Final Summary (One Sentence)

> The system has evolved from a misleading pipeline benchmark to a realistic query benchmark, revealing that the true bottleneck is large-scale multi-level vector aggregation, not database access or percentile selection.
