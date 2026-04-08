package com.example.risk.generator.distribution;

import java.util.Random;

/**
 * Log-Normal distribution: X = exp(μ + σ·Z), Z ~ N(0,1).
 * Useful for modelling asset prices (always positive).
 * Values are shifted to be centred around zero for P&L representation.
 */
public class LogNormalDistribution implements Distribution {

    private final double mu;
    private final double sigma;
    private final Random rng;

    public LogNormalDistribution(double mu, double sigma, long seed) {
        this.mu    = mu;
        this.sigma = sigma;
        this.rng   = new Random(seed);
    }

    public LogNormalDistribution(long seed) { this(0.0, 0.5, seed); }

    @Override
    public double[] generate(int size) {
        double[] out = new double[size];
        double median = Math.exp(mu); // centre around zero
        for (int i = 0; i < size; i++) {
            out[i] = Math.exp(mu + sigma * rng.nextGaussian()) - median;
        }
        return out;
    }

    @Override
    public String name() { return "LogNormal(μ=" + mu + ", σ=" + sigma + ")"; }
}
