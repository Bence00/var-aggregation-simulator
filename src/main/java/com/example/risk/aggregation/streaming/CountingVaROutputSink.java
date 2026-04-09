package com.example.risk.aggregation.streaming;

import com.example.risk.model.GroupKey;

import java.util.List;
import java.util.Map;

/**
 * Streaming sink that retains no VaR outputs and only counts emitted groups per level.
 */
public class CountingVaROutputSink implements VaROutputSink {

    private final int[] levelCounts;

    public CountingVaROutputSink(int levelCount) {
        this.levelCounts = new int[levelCount];
    }

    @Override
    public void acceptGroup(int levelIndex, List<Integer> dimensions, GroupKey groupKey, Map<Double, Double> varValues) {
        levelCounts[levelIndex]++;
    }

    @Override
    public void finishLevel(int levelIndex, List<Integer> dimensions, int groupCount) {
        levelCounts[levelIndex] = groupCount;
    }

    public int[] levelCounts() {
        return levelCounts.clone();
    }

    public int totalGroups() {
        int total = 0;
        for (int count : levelCounts) {
            total += count;
        }
        return total;
    }
}
