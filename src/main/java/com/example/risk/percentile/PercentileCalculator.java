package com.example.risk.percentile;

/**
 * Strategy interface for percentile computation.
 * Implementations can be swapped without changing the aggregation pipeline.
 *
 * <p>Contract:
 * <ul>
 *   <li>{@code data} must not be null; may contain NaN / Inf.</li>
 *   <li>{@code percentile} is in [0, 100].</li>
 *   <li>NaN values in data should be excluded from ranking (implementation-defined).</li>
 * </ul>
 */
public interface PercentileCalculator {

    /**
     * Compute the value at the given percentile in {@code data}.
     *
     * @param data       input scenario vector
     * @param percentile target percentile in [0, 100]
     * @return computed value, or NaN if all data is NaN
     */
    double compute(double[] data, double percentile);

    /** Human-readable name for this implementation. */
    String name();
}
