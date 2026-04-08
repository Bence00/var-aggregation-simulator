package com.example.risk.percentile;

import java.util.Arrays;

/**
 * Baseline percentile implementation: sort the array, then index.
 *
 * <p>NaN and ±Inf are handled using {@link Double#compare} semantics:
 * NaN sorts to the end (excluded from VaR rank), ±Inf is valid and included.
 *
 * <p>Complexity: O(n log n) per call. Suitable as reference implementation.
 * For production, replace with OSILA single-scan (see OSILA project).
 */
public class SortBasedPercentileCalculator implements PercentileCalculator {

    @Override
    public double compute(double[] data, double percentile) {
        // Filter NaN; keep ±Inf as valid P&L extremes
        double[] finite = Arrays.stream(data)
                .filter(v -> !Double.isNaN(v))
                .toArray();

        if (finite.length == 0) return Double.NaN;

        Arrays.sort(finite); // sorts ascending; ±Inf land at correct positions

        // Linear interpolation index: k = p/100 * (n-1)
        double pos    = (percentile / 100.0) * (finite.length - 1);
        int    lo     = (int) Math.floor(pos);
        int    hi     = Math.min(lo + 1, finite.length - 1);
        double frac   = pos - lo;

        return finite[lo] * (1.0 - frac) + finite[hi] * frac;
    }

    @Override
    public String name() { return "Sort-based (baseline)"; }
}
