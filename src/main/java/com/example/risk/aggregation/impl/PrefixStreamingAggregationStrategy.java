package com.example.risk.aggregation.impl;

import com.example.risk.aggregation.AggregationStrategy;
import com.example.risk.aggregation.streaming.OrderedRiskRecordSource;
import com.example.risk.aggregation.streaming.StreamingAggregationResult;
import com.example.risk.aggregation.streaming.StreamingHierarchicalVaREngine;
import com.example.risk.aggregation.streaming.StreamingVaRAggregationStrategy;
import com.example.risk.aggregation.streaming.VaROutputSink;
import com.example.risk.model.GroupKey;
import com.example.risk.model.RiskRecord;
import com.example.risk.percentile.PercentileCalculator;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Production-like prefix-rollup strategy.
 *
 * <p>This strategy expects records to arrive already ordered by the interesting
 * dimensions. It performs a single pass over the stream and never sorts or
 * materializes the full dataset itself.
 *
 * <p>The {@link #aggregate(List, List)} method remains only for interface
 * compatibility and assumes the input is already ordered. The intended entry
 * point is {@link #computeOrdered(OrderedRiskRecordSource, List, List, PercentileCalculator, VaROutputSink, Consumer)}.
 */
public class PrefixStreamingAggregationStrategy implements AggregationStrategy, StreamingVaRAggregationStrategy {

    @Override
    public Map<GroupKey, double[]> aggregate(List<RiskRecord> records, List<Integer> interestingIndices) {
        Map<GroupKey, double[]> groups = new LinkedHashMap<>();
        GroupAccumulator acc = new GroupAccumulator();
        for (RiskRecord record : records) {
            GroupKey key = extractKey(record, interestingIndices);
            if (!key.equals(acc.currentKey)) {
                if (acc.currentKey != null) {
                    groups.put(acc.currentKey, acc.currentSum);
                }
                acc.currentKey = key;
                acc.currentSum = record.getNumbers().clone();
            } else {
                addInPlace(acc.currentSum, record.getNumbers());
            }
        }
        if (acc.currentKey != null) {
            groups.put(acc.currentKey, acc.currentSum);
        }
        return groups;
    }

    @Override
    public String name() {
        return "Prefix streaming (DB ordered)";
    }

    @Override
    public StreamingAggregationResult computeOrdered(OrderedRiskRecordSource source,
                                                     List<Integer> interesting,
                                                     List<Double> percentiles,
                                                     PercentileCalculator calculator,
                                                     VaROutputSink sink,
                                                     Consumer<String> progress) {
        StreamingHierarchicalVaREngine engine = new StreamingHierarchicalVaREngine(calculator, progress);
        StreamingHierarchicalVaREngine.StreamComputeResult result =
                engine.processOrderedSource(source, interesting, percentiles, sink);
        return new StreamingAggregationResult(result.orderingMs, result.aggregationMs, result.percentileMs);
    }

    private static GroupKey extractKey(RiskRecord record, List<Integer> indices) {
        java.util.ArrayList<String> values = new java.util.ArrayList<>(indices.size());
        for (int idx : indices) {
            values.add(record.getAttributes().get(idx));
        }
        return new GroupKey(values);
    }

    private static void addInPlace(double[] target, double[] source) {
        int len = Math.min(target.length, source.length);
        for (int i = 0; i < len; i++) {
            target[i] += source[i];
        }
    }

    private static final class GroupAccumulator {
        private GroupKey currentKey;
        private double[] currentSum;
    }
}
