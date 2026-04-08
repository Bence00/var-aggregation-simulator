package com.example.risk.aggregation.impl;

import com.example.risk.aggregation.AllLevelsAggregationStrategy;
import com.example.risk.model.GroupKey;
import com.example.risk.model.RiskRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Prefix-rollup aggregation: sort once by all interesting attributes, then scan
 * linearly and accumulate partial sums at every hierarchy level simultaneously.
 *
 * <h2>Algorithm</h2>
 * <ol>
 *   <li>Sort records lexicographically by the interesting attribute values.</li>
 *   <li>Maintain one running-sum accumulator per level (leaf through root).</li>
 *   <li>For each record, find the first attribute index that differs from the
 *       previous record — call it {@code j}.</li>
 *   <li>Every level whose key uses attribute {@code j} or later (i.e. levels
 *       with {@code numDims > j}) has a completed group: emit it and reset the
 *       accumulator to the current record's numbers.</li>
 *   <li>Every shallower level (numDims ≤ j) just adds the current record.</li>
 *   <li>After the last record, emit all remaining accumulators.</li>
 * </ol>
 *
 * <h2>Complexity</h2>
 * <ul>
 *   <li>O(N log N) sort (same as {@link StreamingAggregationStrategy}).</li>
 *   <li>O(N · L) accumulation, where L = number of hierarchy levels.
 *       In practice L is small (typically 2–6), so this is essentially O(N).</li>
 *   <li>Rollup is free: no extra passes over the group maps are needed.</li>
 * </ul>
 *
 * <p>The {@link #aggregate} method (required by the base interface) delegates to
 * {@link #aggregateAllLevels} and returns only the leaf-level map.  The
 * {@link com.example.risk.aggregation.strategy.HierarchicalVaREngine} detects
 * that this strategy implements {@link AllLevelsAggregationStrategy} and uses
 * the optimised path automatically.
 */
public class PrefixRollupAggregationStrategy implements AllLevelsAggregationStrategy {

    // ── AggregationStrategy (leaf only) ────────────────────────────────

    @Override
    public Map<GroupKey, double[]> aggregate(List<RiskRecord> records,
                                             List<Integer> interestingIndices) {
        List<Map<GroupKey, double[]>> all = aggregateAllLevels(records, interestingIndices);
        return all.isEmpty() ? new LinkedHashMap<>() : all.get(0);
    }

    @Override
    public String name() { return "Prefix rollup (sort-scan)"; }

    // ── AllLevelsAggregationStrategy ───────────────────────────────────

    /**
     * {@inheritDoc}
     *
     * <p>Returns a list of {@code n+1} maps (n = {@code interestingIndices.size()}):
     * <ul>
     *   <li>index 0 — leaf: groups keyed by all n interesting attributes</li>
     *   <li>index 1 — first rollup: groups keyed by the first n−1 attributes</li>
     *   <li>…</li>
     *   <li>index n — root: one group with the empty key</li>
     * </ul>
     */
    @Override
    public List<Map<GroupKey, double[]>> aggregateAllLevels(List<RiskRecord> records,
                                                            List<Integer> interestingIndices) {
        int n = interestingIndices.size();   // number of interesting dimensions
        int numLevels = n + 1;               // leaf … root

        // Initialise one output map per level (index 0 = leaf, index n = root)
        @SuppressWarnings("unchecked")
        Map<GroupKey, double[]>[] maps = new LinkedHashMap[numLevels];
        for (int i = 0; i < numLevels; i++) maps[i] = new LinkedHashMap<>();

        if (records.isEmpty()) return Arrays.asList(maps);

        // Sort once by all interesting attrs (lexicographic, attribute-by-attribute)
        List<RiskRecord> sorted = new ArrayList<>(records);
        sorted.sort(comparator(interestingIndices));

        // Accumulators indexed by numDims (0 = root, n = leaf)
        double[][] acc = new double[numLevels][];
        String[] prevAttrs = null;  // interesting-attr values of the previous record

        for (RiskRecord record : sorted) {
            String[] cur  = extractAttrs(record, interestingIndices);
            double[] nums = record.getNumbers();

            if (prevAttrs == null) {
                // First record: initialise every accumulator with a fresh clone
                for (int numDims = 0; numDims <= n; numDims++) {
                    acc[numDims] = nums.clone();
                }
            } else {
                // Find the first attribute index that changed (0 = shallowest dimension)
                int firstChanged = firstChangedIndex(prevAttrs, cur, n);

                if (firstChanged < 0) {
                    // Exact same group key at every level: just accumulate
                    for (int numDims = 0; numDims <= n; numDims++) {
                        addInPlace(acc[numDims], nums);
                    }
                } else {
                    // Levels that use attribute `firstChanged` or later have a new group.
                    // numDims > firstChanged  → emit completed group, reset to current record
                    // numDims ≤ firstChanged  → continue accumulating
                    for (int numDims = n; numDims > firstChanged; numDims--) {
                        maps[n - numDims].put(makeKey(prevAttrs, numDims), acc[numDims]);
                        acc[numDims] = nums.clone();
                    }
                    for (int numDims = firstChanged; numDims >= 0; numDims--) {
                        addInPlace(acc[numDims], nums);
                    }
                }
            }
            prevAttrs = cur;
        }

        // Emit all remaining accumulators
        if (prevAttrs != null) {
            for (int numDims = n; numDims >= 0; numDims--) {
                maps[n - numDims].put(makeKey(prevAttrs, numDims), acc[numDims]);
            }
        }

        return Arrays.asList(maps);
    }

    // ── Private helpers ────────────────────────────────────────────────

    /**
     * Return the 0-based index of the first attribute (in interesting-index order)
     * where {@code cur} differs from {@code prev}, or −1 if all are equal.
     */
    private static int firstChangedIndex(String[] prev, String[] cur, int n) {
        for (int j = 0; j < n; j++) {
            if (!cur[j].equals(prev[j])) return j;
        }
        return -1;
    }

    /**
     * Extract the interesting attribute values from a record into a plain array,
     * ordered by {@code interestingIndices}.
     */
    private static String[] extractAttrs(RiskRecord record, List<Integer> interesting) {
        List<String> all = record.getAttributes();
        String[] attrs = new String[interesting.size()];
        for (int j = 0; j < interesting.size(); j++) {
            attrs[j] = all.get(interesting.get(j));
        }
        return attrs;
    }

    /**
     * Build a {@link GroupKey} from the first {@code numDims} entries of {@code attrs}.
     * {@code numDims == 0} produces the root (empty) key.
     */
    private static GroupKey makeKey(String[] attrs, int numDims) {
        if (numDims == 0) return new GroupKey(List.of());
        List<String> values = new ArrayList<>(numDims);
        for (int i = 0; i < numDims; i++) values.add(attrs[i]);
        return new GroupKey(values);
    }

    /**
     * Comparator that orders records lexicographically by their interesting
     * attribute values.  Compares attribute by attribute so no concatenated
     * string is ever allocated.
     */
    private static Comparator<RiskRecord> comparator(List<Integer> interesting) {
        return (r1, r2) -> {
            List<String> a1 = r1.getAttributes();
            List<String> a2 = r2.getAttributes();
            for (int idx : interesting) {
                int cmp = a1.get(idx).compareTo(a2.get(idx));
                if (cmp != 0) return cmp;
            }
            return 0;
        };
    }

    /** Elementwise in-place addition: target[i] += source[i]. */
    private static void addInPlace(double[] target, double[] source) {
        int len = Math.min(target.length, source.length);
        for (int i = 0; i < len; i++) target[i] += source[i];
    }
}
