package com.example.risk.model;

import java.util.List;
import java.util.Map;

/**
 * VaR results for one level of the hierarchy.
 * Each level has a set of groups; each group has a summed numbers vector
 * and the extracted VaR values per requested percentile.
 */
public final class LevelResult {

    /** Attribute indices active at this level. Empty = root. */
    private final List<Integer> dimensions;

    /** Group key → elementwise-summed numbers vector. */
    private final Map<GroupKey, double[]> groups;
    private final int groupCount;

    /** Group key → (percentile% → computed VaR value). */
    private final Map<GroupKey, Map<Double, Double>> varValues;

    public LevelResult(List<Integer> dimensions,
                       Map<GroupKey, double[]> groups,
                       Map<GroupKey, Map<Double, Double>> varValues) {
        this.dimensions = dimensions;
        this.groups     = groups;
        this.groupCount = groups == null ? varValues.size() : groups.size();
        this.varValues  = varValues;
    }

    public List<Integer>                          getDimensions() { return dimensions; }
    public Map<GroupKey, double[]>                getGroups()     { return groups == null ? Map.of() : groups; }
    public Map<GroupKey, Map<Double, Double>>     getVarValues()  { return varValues; }
    public int                                    getGroupCount() { return groupCount; }

    public String dimensionsLabel() {
        return dimensions.isEmpty() ? "(root — global)" : "dims " + dimensions;
    }
}
