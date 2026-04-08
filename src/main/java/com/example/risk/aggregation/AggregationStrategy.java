package com.example.risk.aggregation;

import com.example.risk.model.GroupKey;
import com.example.risk.model.RiskRecord;

import java.util.List;
import java.util.Map;

/**
 * Strategy interface for grouping and elementwise-summing records.
 *
 * <p>Implementations differ in how they traverse the record list and manage
 * intermediate state. The {@link com.example.risk.aggregation.strategy.HierarchicalVaREngine}
 * calls this once per hierarchy level.
 *
 * <p>Rollup (merging one hierarchy level into the next) is handled by the engine,
 * not by this interface, so implementations focus on the flat grouping step only.
 */
public interface AggregationStrategy {

    /**
     * Group {@code records} by the attribute indices in {@code interestingIndices}
     * and return the elementwise sum of each group's numbers vector.
     *
     * @param records           source records (not mutated)
     * @param interestingIndices attribute indices to group by (in order)
     * @return map from group key (attribute values at interesting indices) to summed vector
     */
    Map<GroupKey, double[]> aggregate(List<RiskRecord> records, List<Integer> interestingIndices);

    /** Human-readable name shown in the UI. */
    String name();
}
