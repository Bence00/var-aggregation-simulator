package com.example.risk.aggregation.impl;

import com.example.risk.aggregation.AggregationStrategy;
import com.example.risk.model.GroupKey;
import com.example.risk.model.RiskRecord;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Baseline aggregation: HashMap grouping with elementwise accumulation.
 *
 * <p>For each record, extract its group key (attribute values at the
 * interesting indices), look up or create the running sum, and add the
 * record's numbers vector elementwise.
 *
 * <p>Simple and correct. Performance degrades for very large record sets
 * because all running sums are live in memory simultaneously.
 */
public class BaselineAggregationStrategy implements AggregationStrategy {

    @Override
    public Map<GroupKey, double[]> aggregate(List<RiskRecord> records,
                                             List<Integer> interestingIndices) {
        Map<GroupKey, double[]> result = new LinkedHashMap<>();

        for (RiskRecord record : records) {
            GroupKey key = extractKey(record, interestingIndices);
            double[] sum = result.get(key);

            if (sum == null) {
                // First record in this group: clone its numbers as the initial sum
                result.put(key, record.getNumbers().clone());
            } else {
                // Accumulate elementwise
                addInPlace(sum, record.getNumbers());
            }
        }
        return result;
    }

    @Override
    public String name() { return "Baseline (HashMap)"; }

    // ── Utilities ──────────────────────────────────────────────────────

    static GroupKey extractKey(RiskRecord record, List<Integer> indices) {
        List<String> values = new ArrayList<>(indices.size());
        for (int idx : indices) values.add(record.getAttributes().get(idx));
        return new GroupKey(values);
    }

    /** Elementwise in-place sum: target[i] += source[i]. */
    static void addInPlace(double[] target, double[] source) {
        int len = Math.min(target.length, source.length);
        for (int i = 0; i < len; i++) target[i] += source[i];
    }
}
