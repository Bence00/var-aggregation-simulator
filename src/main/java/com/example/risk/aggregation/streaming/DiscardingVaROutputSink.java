package com.example.risk.aggregation.streaming;

import com.example.risk.model.GroupKey;

import java.util.List;
import java.util.Map;

/**
 * Sink used by benchmarks when the percentile values do not need to be retained.
 */
public class DiscardingVaROutputSink implements VaROutputSink {

    @Override
    public void acceptGroup(int levelIndex, List<Integer> dimensions, GroupKey groupKey, Map<Double, Double> varValues) {
        // Intentionally discard completed outputs.
    }

    @Override
    public void finishLevel(int levelIndex, List<Integer> dimensions, int groupCount) {
        // Nothing to record.
    }
}
