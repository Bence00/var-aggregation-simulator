package com.example.risk.aggregation.streaming;

import com.example.risk.model.GroupKey;

import java.util.List;
import java.util.Map;

/**
 * Receives completed VaR outputs from the streaming hierarchical engine.
 *
 * <p>The engine calls {@link #acceptGroup} as soon as a group closes and its
 * exact percentile values have been computed. Implementations may collect,
 * persist, or discard those results.
 */
public interface VaROutputSink {

    /**
     * Consume one completed group result.
     *
     * @param levelIndex zero-based level index, where 0 is the leaf and the last level is the root
     * @param dimensions active dimensions for the level
     * @param groupKey   closed group key
     * @param varValues  exact percentile values for the closed aggregate
     */
    void acceptGroup(int levelIndex, List<Integer> dimensions, GroupKey groupKey, Map<Double, Double> varValues);

    /**
     * Notification that a level has been fully emitted.
     *
     * @param levelIndex zero-based level index
     * @param dimensions active dimensions for the level
     * @param groupCount number of groups emitted for the level
     */
    void finishLevel(int levelIndex, List<Integer> dimensions, int groupCount);
}
