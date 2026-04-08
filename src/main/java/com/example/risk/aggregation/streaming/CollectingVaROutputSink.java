package com.example.risk.aggregation.streaming;

import com.example.risk.model.GroupKey;
import com.example.risk.model.LevelResult;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Collects streaming VaR outputs into {@link LevelResult} objects.
 *
 * <p>This stores only final percentile values per group, not the full aggregated
 * {@code double[]} vectors that produced them.
 */
public class CollectingVaROutputSink implements VaROutputSink {

    private final List<LevelBuffer> levels;

    public CollectingVaROutputSink(List<List<Integer>> levelDimensions) {
        this.levels = new ArrayList<>(levelDimensions.size());
        for (List<Integer> dims : levelDimensions) {
            levels.add(new LevelBuffer(List.copyOf(dims)));
        }
    }

    @Override
    public void acceptGroup(int levelIndex, List<Integer> dimensions, GroupKey groupKey, Map<Double, Double> varValues) {
        levels.get(levelIndex).values.put(groupKey, new LinkedHashMap<>(varValues));
    }

    @Override
    public void finishLevel(int levelIndex, List<Integer> dimensions, int groupCount) {
        levels.get(levelIndex).groupCount = groupCount;
    }

    public List<LevelResult> toLevelResults() {
        List<LevelResult> out = new ArrayList<>(levels.size());
        for (LevelBuffer level : levels) {
            out.add(new LevelResult(level.dimensions, null, level.values, level.groupCount));
        }
        return out;
    }

    private static final class LevelBuffer {
        private final List<Integer> dimensions;
        private final Map<GroupKey, Map<Double, Double>> values = new LinkedHashMap<>();
        private int groupCount;

        private LevelBuffer(List<Integer> dimensions) {
            this.dimensions = dimensions;
        }
    }
}
