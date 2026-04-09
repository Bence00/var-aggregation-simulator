package com.example.risk.aggregation.strategy;

import com.example.risk.aggregation.AggregationStrategy;
import com.example.risk.aggregation.AllLevelsAggregationStrategy;
import com.example.risk.aggregation.impl.PrefixRollupAggregationStrategy;
import com.example.risk.aggregation.impl.PrefixStreamingAggregationStrategy;
import com.example.risk.aggregation.streaming.CollectingVaROutputSink;
import com.example.risk.aggregation.streaming.StreamingHierarchicalVaREngine;
import com.example.risk.model.GroupKey;
import com.example.risk.model.LevelResult;
import com.example.risk.model.RiskRecord;
import com.example.risk.percentile.PercentileCalculator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Drives the hierarchical VaR computation.
 *
 * <p>For prefix-rollup aggregation this class now uses a true streaming engine:
 * one live accumulator per level, exact percentile extraction when a group closes,
 * and no materialization of full {@code Map<GroupKey, double[]>} hierarchies.
 *
 * <p>The legacy map-based path remains for the other aggregation strategies.
 */
public class HierarchicalVaREngine {

    private final AggregationStrategy strategy;
    private final PercentileCalculator calculator;
    private final Consumer<String> progress;

    public HierarchicalVaREngine(AggregationStrategy strategy, PercentileCalculator calculator) {
        this(strategy, calculator, ignored -> {});
    }

    public HierarchicalVaREngine(AggregationStrategy strategy,
                                 PercentileCalculator calculator,
                                 Consumer<String> progress) {
        this.strategy = strategy;
        this.calculator = calculator;
        this.progress = progress == null ? ignored -> {} : progress;
    }

    public List<LevelResult> compute(List<RiskRecord> records,
                                     List<Integer> interesting,
                                     List<Double> percentiles) {
        List<Integer> dims = new ArrayList<>(interesting);

        if (strategy instanceof PrefixRollupAggregationStrategy
                || strategy instanceof PrefixStreamingAggregationStrategy) {
            progress.accept(String.format(
                    "Streaming prefix-rollup: aggregating and computing VaR across %d levels", dims.size() + 1));
            CollectingVaROutputSink sink = new CollectingVaROutputSink(levelDimensions(dims));
            StreamingHierarchicalVaREngine streamEngine =
                    new StreamingHierarchicalVaREngine(calculator, progress);
            streamEngine.processInMemory(records, dims, percentiles, sink);
            return sink.toLevelResults();
        }

        if (strategy instanceof AllLevelsAggregationStrategy allLevels) {
            progress.accept(String.format(
                    "Prefix-rollup: aggregating all %d levels in one pass", dims.size() + 1));
            List<Map<GroupKey, double[]>> allMaps = allLevels.aggregateAllLevels(records, dims);
            return computeTimedAllLevels(allMaps, dims, percentiles).levels;
        }

        progress.accept(String.format("Aggregating leaf level 1/%d: dims %s",
                interesting.size() + 1, dims));
        Map<GroupKey, double[]> groups = strategy.aggregate(records, dims);
        return computeFromGroups(groups, dims, percentiles);
    }

    public List<LevelResult> computeFromGroups(Map<GroupKey, double[]> groups,
                                               List<Integer> interesting,
                                               List<Double> percentiles) {
        return computeTimedFromGroups(groups, interesting, percentiles).levels;
    }

    public ComputeResult computeTimedFromGroups(Map<GroupKey, double[]> groups,
                                                List<Integer> interesting,
                                                List<Double> percentiles) {
        List<LevelResult> levels = new ArrayList<>();
        List<Integer> dims = new ArrayList<>(interesting);
        int totalLevels = interesting.size() + 1;
        int level = 1;
        long rollupAggNs = 0L;
        long percentileNs = 0L;

        progress.accept(String.format("Computing VaR level %d/%d: %s (%d groups)",
                level, totalLevels, dimensionsLabel(dims), groups.size()));
        long t0 = System.nanoTime();
        levels.add(buildLevel(dims, groups, percentiles));
        percentileNs += System.nanoTime() - t0;

        while (!dims.isEmpty()) {
            level++;
            progress.accept(String.format("Rolling up to level %d/%d: dropping dimension %d",
                    level, totalLevels, dims.get(dims.size() - 1)));
            dims = new ArrayList<>(dims.subList(0, dims.size() - 1));

            long t1 = System.nanoTime();
            groups = rollup(groups);
            rollupAggNs += System.nanoTime() - t1;

            progress.accept(String.format("Computing VaR level %d/%d: %s (%d groups)",
                    level, totalLevels, dimensionsLabel(dims), groups.size()));
            long t2 = System.nanoTime();
            levels.add(buildLevel(dims, groups, percentiles));
            percentileNs += System.nanoTime() - t2;
        }

        progress.accept(String.format("VaR aggregation complete: %d/%d levels", totalLevels, totalLevels));
        return new ComputeResult(levels, rollupAggNs / 1_000_000L, percentileNs / 1_000_000L);
    }

    public ComputeResult computeTimedAllLevels(List<Map<GroupKey, double[]>> allMaps,
                                               List<Integer> interesting,
                                               List<Double> percentiles) {
        List<LevelResult> levels = new ArrayList<>();
        int n = interesting.size();
        int totalLevels = n + 1;
        long percentileNs = 0L;

        for (int i = 0; i < allMaps.size(); i++) {
            List<Integer> dimsAtLevel = new ArrayList<>(interesting.subList(0, n - i));
            Map<GroupKey, double[]> map = allMaps.get(i);
            progress.accept(String.format("Computing VaR level %d/%d: %s (%d groups)",
                    i + 1, totalLevels, dimensionsLabel(dimsAtLevel), map.size()));
            long t = System.nanoTime();
            levels.add(buildLevel(dimsAtLevel, map, percentiles));
            percentileNs += System.nanoTime() - t;
        }

        progress.accept(String.format("VaR aggregation complete: %d/%d levels", totalLevels, totalLevels));
        return new ComputeResult(levels, 0L, percentileNs / 1_000_000L);
    }

    public static final class ComputeResult {
        public final List<LevelResult> levels;
        public final long rollupAggMs;
        public final long percentileMs;

        ComputeResult(List<LevelResult> levels, long rollupAggMs, long percentileMs) {
            this.levels = levels;
            this.rollupAggMs = rollupAggMs;
            this.percentileMs = percentileMs;
        }
    }

    private static Map<GroupKey, double[]> rollup(Map<GroupKey, double[]> current) {
        Map<GroupKey, double[]> parent = new LinkedHashMap<>();
        for (Map.Entry<GroupKey, double[]> entry : current.entrySet()) {
            GroupKey parentKey = entry.getKey().withoutLast();
            double[] existing = parent.get(parentKey);
            if (existing == null) {
                parent.put(parentKey, entry.getValue().clone());
            } else {
                addInPlace(existing, entry.getValue());
            }
        }
        return parent;
    }

    private LevelResult buildLevel(List<Integer> dims,
                                   Map<GroupKey, double[]> groups,
                                   List<Double> percentiles) {
        Map<GroupKey, Map<Double, Double>> varMap = new LinkedHashMap<>();
        for (Map.Entry<GroupKey, double[]> e : groups.entrySet()) {
            Map<Double, Double> pMap = new LinkedHashMap<>();
            for (double p : percentiles) {
                pMap.put(p, calculator.compute(e.getValue(), p));
            }
            varMap.put(e.getKey(), pMap);
        }
        return new LevelResult(List.copyOf(dims), null, varMap);
    }

    private static void addInPlace(double[] target, double[] source) {
        for (int i = 0; i < Math.min(target.length, source.length); i++) {
            target[i] += source[i];
        }
    }

    private static String dimensionsLabel(List<Integer> dims) {
        return dims.isEmpty() ? "root" : "dims " + dims;
    }

    private static List<List<Integer>> levelDimensions(List<Integer> interesting) {
        List<List<Integer>> levels = new ArrayList<>(interesting.size() + 1);
        for (int i = 0; i <= interesting.size(); i++) {
            levels.add(List.copyOf(interesting.subList(0, interesting.size() - i)));
        }
        return levels;
    }
}
