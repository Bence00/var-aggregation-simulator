package com.example.risk.generator.distribution;

import java.util.Random;

/**
 * Exponential distribution: X = -ln(U) / lambda, U ~ Uniform(0,1).
 * Mean = 1/lambda. Values are negated to model losses (left tail).
 */
public class ExponentialDistribution implements Distribution {

    private final double lambda;
    private final Random rng;

    public ExponentialDistribution(double lambda, long seed) {
        this.lambda = lambda;
        this.rng    = new Random(seed);
    }

    public ExponentialDistribution(long seed) { this(1.0, seed); }

    @Override
    public double[] generate(int size) {
        double[] out = new double[size];
        for (int i = 0; i < size; i++) {
            double u = rng.nextDouble();
            if (u == 0.0) u = Double.MIN_VALUE;
            out[i] = -Math.log(u) / lambda; // positive right-tail; negate for loss
        }
        return out;
    }

    @Override
    public String name() { return "Exponential(λ=" + lambda + ")"; }
}
