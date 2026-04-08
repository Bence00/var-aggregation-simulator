package com.example.risk.aggregation.impl;

import com.example.risk.aggregation.AggregationStrategy;
import com.example.risk.model.GroupKey;
import com.example.risk.model.RiskRecord;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Sort-merge aggregation: sort records by group key first, then accumulate sequentially.
 *
 * <p>Advantages over the baseline:
 * <ul>
 *   <li>At any point only one running sum is in scope (current group).</li>
 *   <li>Spatial locality: consecutive records belong to the same group → better cache use.</li>
 *   <li>Avoids HashMap overhead for very large group counts.</li>
 * </ul>
 *
 * <p>Trade-off: O(N log N) sort of the record list before processing.
 * Baseline is O(N) but hash-collision-prone for many groups.
 *
 * <p>In a production system, the "streaming" variant would consume a pre-sorted
 * cursor from the database (ORDER BY attribute columns) and never materialise the
 * full record list.
 */
public class StreamingAggregationStrategy implements AggregationStrategy {

    @Override
    public Map<GroupKey, double[]> aggregate(List<RiskRecord> records,
                                             List<Integer> interestingIndices) {
        if (records.isEmpty()) return new LinkedHashMap<>();

        // Sort records by the composite group key so equal groups are adjacent
        Comparator<RiskRecord> byKey = Comparator.comparing(
                r -> BaselineAggregationStrategy.extractKey(r, interestingIndices).toString());

        List<RiskRecord> sorted = new ArrayList<>(records);
        sorted.sort(byKey);

        Map<GroupKey, double[]> result = new LinkedHashMap<>();
        GroupKey currentKey = null;
        double[] currentSum = null;

        for (RiskRecord record : sorted) {
            GroupKey key = BaselineAggregationStrategy.extractKey(record, interestingIndices);
            if (!key.equals(currentKey)) {
                if (currentKey != null) result.put(currentKey, currentSum);
                currentKey = key;
                currentSum = record.getNumbers().clone();
            } else {
                BaselineAggregationStrategy.addInPlace(currentSum, record.getNumbers());
            }
        }
        if (currentKey != null) result.put(currentKey, currentSum);

        return result;
    }

    @Override
    public String name() { return "Streaming (sort-merge)"; }
}
