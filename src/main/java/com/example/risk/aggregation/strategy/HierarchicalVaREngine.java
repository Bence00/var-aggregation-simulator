package com.example.risk.aggregation.strategy;

import com.example.risk.aggregation.AggregationStrategy;
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
 * Drives the full hierarchical VaR computation:
 *
 * <ol>
 *   <li>Leaf level: delegate to {@link AggregationStrategy} to group records
 *       by the full interesting vector and produce summed numbers vectors.</li>
 *   <li>Extract VaR percentiles at the leaf level.</li>
 *   <li>Roll up: remove the last dimension → merge sibling groups into parents
 *       by elementwise summation.</li>
 *   <li>Extract VaR at the parent level.</li>
 *   <li>Repeat until the interesting vector is empty (root).</li>
 * </ol>
 *
 * <p>Space: at each step only the current level's groups are live; the previous
 * level is discarded after rollup (space-conservative variant — Part a of the problem).
 */
public class HierarchicalVaREngine {

    private final AggregationStrategy  strategy;
    private final PercentileCalculator calculator;
    private final Consumer<String> progress;

    public HierarchicalVaREngine(AggregationStrategy strategy, PercentileCalculator calculator) {
        this(strategy, calculator, ignored -> {});
    }

    public HierarchicalVaREngine(AggregationStrategy strategy,
                                 PercentileCalculator calculator,
                                 Consumer<String> progress) {
        this.strategy   = strategy;
        this.calculator = calculator;
        this.progress   = progress == null ? ignored -> {} : progress;
    }

    /**
     * Compute VaR at every hierarchy level.
     *
     * @param records     loaded records for a single datasetId
     * @param interesting attribute indices to group by (ordered; last is removed first)
     * @param percentiles target percentile values in [0, 100], e.g. [1.0, 95.0, 99.0]
     * @return one {@link LevelResult} per level (leaf first, root last)
     */
    public List<LevelResult> compute(List<RiskRecord>  records,
                                     List<Integer>     interesting,
                                     List<Double>      percentiles) {

        // ── Leaf level: initial grouping from raw records ──────────────
        List<Integer> dims = new ArrayList<>(interesting);
        progress.accept(String.format("Aggregating leaf level 1/%d: dims %s",
                interesting.size() + 1, dims));
        Map<GroupKey, double[]> groups = strategy.aggregate(records, dims);
        return computeFromGroups(groups, dims, percentiles);
    }

    public List<LevelResult> computeFromGroups(Map<GroupKey, double[]> groups,
                                               List<Integer> interesting,
                                               List<Double> percentiles) {
        List<LevelResult> levels = new ArrayList<>();
        List<Integer> dims = new ArrayList<>(interesting);
        int totalLevels = interesting.size() + 1;
        int level = 1;
        progress.accept(String.format("Computing VaR level %d/%d: %s (%d groups)",
                level, totalLevels, dimensionsLabel(dims), groups.size()));
        levels.add(buildLevel(dims, groups, percentiles));

        // ── Rollup levels: iteratively remove last dimension ───────────
        while (!dims.isEmpty()) {
            level++;
            progress.accept(String.format("Rolling up to level %d/%d: dropping dimension %d",
                    level, totalLevels, dims.get(dims.size() - 1)));
            dims = new ArrayList<>(dims.subList(0, dims.size() - 1));
            groups = rollup(groups);                             // merge into parents
            progress.accept(String.format("Computing VaR level %d/%d: %s (%d groups)",
                    level, totalLevels, dimensionsLabel(dims), groups.size()));
            levels.add(buildLevel(dims, groups, percentiles));  // extract VaR
            // previous groups map is now unreferenced → eligible for GC
        }

        progress.accept(String.format("VaR aggregation complete: %d/%d levels", totalLevels, totalLevels));
        return levels;
    }

    // ── Private helpers ────────────────────────────────────────────────

    /**
     * Merge current-level groups into parent-level groups by dropping the last
     * element of each group key and summing the numbers vectors.
     */
    private static Map<GroupKey, double[]> rollup(Map<GroupKey, double[]> current) {
        Map<GroupKey, double[]> parent = new LinkedHashMap<>();
        for (Map.Entry<GroupKey, double[]> entry : current.entrySet()) {
            GroupKey parentKey = entry.getKey().withoutLast();
            double[] existing  = parent.get(parentKey);
            if (existing == null) {
                parent.put(parentKey, entry.getValue().clone());
            } else {
                addInPlace(existing, entry.getValue());
            }
        }
        return parent;
    }

    /** Compute VaR for every group and assemble a LevelResult. */
    private LevelResult buildLevel(List<Integer>          dims,
                                   Map<GroupKey, double[]> groups,
                                   List<Double>           percentiles) {
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
        for (int i = 0; i < Math.min(target.length, source.length); i++)
            target[i] += source[i];
    }

    private static String dimensionsLabel(List<Integer> dims) {
        return dims.isEmpty() ? "root" : "dims " + dims;
    }
}
