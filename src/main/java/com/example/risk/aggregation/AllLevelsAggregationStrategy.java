package com.example.risk.aggregation;

import com.example.risk.model.GroupKey;
import com.example.risk.model.RiskRecord;

import java.util.List;
import java.util.Map;

/**
 * Extension of {@link AggregationStrategy} for strategies that can produce
 * all hierarchy levels in a single pass rather than level-by-level rollup.
 *
 * <p>Implementors sort records once and detect prefix changes on the fly,
 * accumulating partial sums for every level simultaneously.
 * The engine skips its own rollup loop when this interface is detected.
 */
public interface AllLevelsAggregationStrategy extends AggregationStrategy {

    /**
     * Aggregate records at every hierarchy level simultaneously.
     *
     * @param records           the full record set for one dataset
     * @param interestingIndices attribute indices ordered from broadest to most specific
     *                           (same contract as {@link AggregationStrategy#aggregate})
     * @return list of size {@code interestingIndices.size() + 1}:
     *         index 0 = leaf groups (all dims), index n = root (empty key, single group)
     */
    List<Map<GroupKey, double[]>> aggregateAllLevels(List<RiskRecord> records,
                                                     List<Integer> interestingIndices);
}
