package com.example.risk.aggregation.streaming;

import com.example.risk.model.LevelResult;
import com.example.risk.percentile.PercentileCalculator;

import java.util.List;
import java.util.function.Consumer;

/**
 * Strategy contract for aggregation paths that operate on an already ordered
 * record stream instead of a materialized in-memory list.
 */
public interface StreamingVaRAggregationStrategy {

    StreamingAggregationResult computeOrdered(
            OrderedRiskRecordSource source,
            List<Integer> interesting,
            List<Double> percentiles,
            PercentileCalculator calculator,
            VaROutputSink sink,
            Consumer<String> progress
    );

    default List<LevelResult> computeOrderedLevels(
            OrderedRiskRecordSource source,
            List<Integer> interesting,
            List<Double> percentiles,
            PercentileCalculator calculator,
            Consumer<String> progress
    ) {
        CollectingVaROutputSink sink = new CollectingVaROutputSink(levelDimensions(interesting));
        computeOrdered(source, interesting, percentiles, calculator, sink, progress);
        return sink.toLevelResults();
    }

    private static List<List<Integer>> levelDimensions(List<Integer> interesting) {
        List<List<Integer>> levels = new java.util.ArrayList<>(interesting.size() + 1);
        for (int i = 0; i <= interesting.size(); i++) {
            levels.add(List.copyOf(interesting.subList(0, interesting.size() - i)));
        }
        return levels;
    }
}
