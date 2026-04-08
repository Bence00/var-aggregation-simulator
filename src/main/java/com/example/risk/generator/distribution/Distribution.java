package com.example.risk.generator.distribution;

/**
 * Pluggable distribution interface.
 * Each implementation produces a scenario P&L vector of the requested length.
 */
public interface Distribution {

    /** Generate {@code size} random doubles drawn from this distribution. */
    double[] generate(int size);

    /** Human-readable name shown in the UI. */
    String name();
}
