package com.example.risk.generator.distribution;

import java.util.List;
import java.util.Random;

/**
 * Mixture distribution: picks one component per element according to weights.
 * Useful for regime-switching models (calm + stressed market scenarios).
 *
 * Example: 90% Normal(0,1) + 10% Normal(0,5) — models occasional vol spikes.
 */
public class MixtureDistribution implements Distribution {

    private final List<Distribution> components;
    private final double[]           weights;    // must sum to 1.0
    private final Random             rng;

    public MixtureDistribution(List<Distribution> components, double[] weights, long seed) {
        if (components.size() != weights.length)
            throw new IllegalArgumentException("components and weights must have the same length");
        this.components = components;
        this.weights    = weights;
        this.rng        = new Random(seed);
    }

    /** Convenience: 90% normal(0,1) + 10% normal(0,5). */
    public static MixtureDistribution calmsStress(long seed) {
        return new MixtureDistribution(
            List.of(new NormalDistribution(0, 1, seed),
                    new NormalDistribution(0, 5, seed + 1)),
            new double[]{0.9, 0.1},
            seed + 2);
    }

    @Override
    public double[] generate(int size) {
        double[] out = new double[size];
        for (int i = 0; i < size; i++) {
            double r = rng.nextDouble();
            double cumulative = 0;
            Distribution chosen = components.get(components.size() - 1);
            for (int c = 0; c < components.size(); c++) {
                cumulative += weights[c];
                if (r <= cumulative) { chosen = components.get(c); break; }
            }
            out[i] = chosen.generate(1)[0];
        }
        return out;
    }

    @Override
    public String name() { return "Mixture(" + components.size() + " components)"; }
}
