package com.example.risk.generator.distribution;

import java.util.Random;

/** Standard Normal N(mean, stddev). */
public class NormalDistribution implements Distribution {

    private final double mean;
    private final double stddev;
    private final Random rng;

    public NormalDistribution(double mean, double stddev, long seed) {
        this.mean   = mean;
        this.stddev = stddev;
        this.rng    = new Random(seed);
    }

    public NormalDistribution(long seed) { this(0.0, 1.0, seed); }

    @Override
    public double[] generate(int size) {
        double[] out = new double[size];
        for (int i = 0; i < size; i++) out[i] = mean + stddev * rng.nextGaussian();
        return out;
    }

    @Override
    public String name() { return "Normal(μ=" + mean + ", σ=" + stddev + ")"; }
}
