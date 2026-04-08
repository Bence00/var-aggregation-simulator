package com.example.risk.generator.distribution;

import java.util.Random;

/** Uniform distribution on [low, high]. */
public class UniformDistribution implements Distribution {

    private final double low;
    private final double high;
    private final Random rng;

    public UniformDistribution(double low, double high, long seed) {
        this.low  = low;
        this.high = high;
        this.rng  = new Random(seed);
    }

    public UniformDistribution(long seed) { this(-1.0, 1.0, seed); }

    @Override
    public double[] generate(int size) {
        double range = high - low;
        double[] out = new double[size];
        for (int i = 0; i < size; i++) out[i] = low + rng.nextDouble() * range;
        return out;
    }

    @Override
    public String name() { return "Uniform[" + low + ", " + high + "]"; }
}
