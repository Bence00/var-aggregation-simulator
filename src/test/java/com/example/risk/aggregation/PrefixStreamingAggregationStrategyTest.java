package com.example.risk.aggregation;

import com.example.risk.aggregation.impl.PrefixRollupAggregationStrategy;
import com.example.risk.aggregation.impl.PrefixStreamingAggregationStrategy;
import com.example.risk.aggregation.streaming.OrderedRiskRecordSource;
import com.example.risk.aggregation.strategy.HierarchicalVaREngine;
import com.example.risk.model.GroupKey;
import com.example.risk.model.LevelResult;
import com.example.risk.model.RiskRecord;
import com.example.risk.percentile.SortBasedPercentileCalculator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PrefixStreamingAggregationStrategyTest {

    @Test
    void streamingPrefixMatchesInMemoryPrefixRollup() {
        List<RiskRecord> records = sampleRecords();
        List<Integer> interesting = List.of(0, 1, 2);
        List<Double> percentiles = List.of(1.0, 50.0, 95.0);

        HierarchicalVaREngine inMemoryEngine = new HierarchicalVaREngine(
                new PrefixRollupAggregationStrategy(),
                new SortBasedPercentileCalculator()
        );
        List<LevelResult> expected = inMemoryEngine.compute(records, interesting, percentiles);

        PrefixStreamingAggregationStrategy streamingStrategy = new PrefixStreamingAggregationStrategy();
        List<RiskRecord> ordered = new ArrayList<>(records);
        ordered.sort(Comparator
                .comparing((RiskRecord r) -> r.getAttributes().get(0))
                .thenComparing(r -> r.getAttributes().get(1))
                .thenComparing(r -> r.getAttributes().get(2)));

        List<LevelResult> actual = streamingStrategy.computeOrderedLevels(
                orderedSource(ordered),
                interesting,
                percentiles,
                new SortBasedPercentileCalculator(),
                ignored -> {}
        );

        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).getDimensions(), actual.get(i).getDimensions());
            assertEquals(expected.get(i).getGroupCount(), actual.get(i).getGroupCount());
            assertVarMapsEqual(expected.get(i).getVarValues(), actual.get(i).getVarValues());
        }
    }

    private static OrderedRiskRecordSource orderedSource(List<RiskRecord> ordered) {
        return consumer -> {
            for (RiskRecord record : ordered) {
                consumer.accept(record);
            }
        };
    }

    private static void assertVarMapsEqual(Map<GroupKey, Map<Double, Double>> expected,
                                           Map<GroupKey, Map<Double, Double>> actual) {
        assertEquals(expected.keySet(), actual.keySet());
        for (GroupKey key : expected.keySet()) {
            Map<Double, Double> expectedValues = expected.get(key);
            Map<Double, Double> actualValues = actual.get(key);
            assertEquals(expectedValues.keySet(), actualValues.keySet());
            for (Double percentile : expectedValues.keySet()) {
                assertEquals(expectedValues.get(percentile), actualValues.get(percentile), 1e-9);
            }
        }
    }

    private static List<RiskRecord> sampleRecords() {
        return List.of(
                record("A", "X", "K", new double[]{1, 5, 9, 13}),
                record("A", "X", "K", new double[]{2, 6, 10, 14}),
                record("A", "X", "L", new double[]{3, 7, 11, 15}),
                record("A", "Y", "K", new double[]{4, 8, 12, 16}),
                record("B", "X", "K", new double[]{5, 9, 13, 17}),
                record("B", "X", "L", new double[]{6, 10, 14, 18}),
                record("B", "Y", "K", new double[]{7, 11, 15, 19}),
                record("B", "Y", "K", new double[]{8, 12, 16, 20})
        );
    }

    private static RiskRecord record(String a0, String a1, String a2, double[] numbers) {
        return new RiskRecord("TEST", List.of(a0, a1, a2), numbers);
    }
}
