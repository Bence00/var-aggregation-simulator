package com.example.risk.percentile;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Correctness and performance tests for FloydRivestPercentileCalculator.
 *
 * Strategy: the sort-based calculator is the ground truth.
 * Floyd-Rivest must return the same value (within 1e-9) for every input.
 */
class FloydRivestPercentileCalculatorTest {

    private final PercentileCalculator floyd   = new FloydRivestPercentileCalculator();
    private final PercentileCalculator baseline = new SortBasedPercentileCalculator();

    // ── Correctness ────────────────────────────────────────────────────

    @Test
    void smallKnownArray_p50() {
        double[] data = {3.0, 1.0, 4.0, 1.0, 5.0, 9.0, 2.0, 6.0};
        assertEquals(baseline.compute(data, 50.0), floyd.compute(data, 50.0), 1e-9);
    }

    @Test
    void smallKnownArray_p1() {
        double[] data = {3.0, 1.0, 4.0, 1.0, 5.0, 9.0, 2.0, 6.0};
        assertEquals(baseline.compute(data, 1.0), floyd.compute(data, 1.0), 1e-9);
    }

    @Test
    void smallKnownArray_p99() {
        double[] data = {3.0, 1.0, 4.0, 1.0, 5.0, 9.0, 2.0, 6.0};
        assertEquals(baseline.compute(data, 99.0), floyd.compute(data, 99.0), 1e-9);
    }

    @Test
    void singleElement() {
        double[] data = {42.0};
        assertEquals(42.0, floyd.compute(data, 50.0), 1e-9);
        assertEquals(42.0, floyd.compute(data, 1.0),  1e-9);
        assertEquals(42.0, floyd.compute(data, 99.0), 1e-9);
    }

    @Test
    void twoElements() {
        double[] data = {10.0, 20.0};
        assertEquals(baseline.compute(data, 1.0),  floyd.compute(data, 1.0),  1e-9);
        assertEquals(baseline.compute(data, 50.0), floyd.compute(data, 50.0), 1e-9);
        assertEquals(baseline.compute(data, 99.0), floyd.compute(data, 99.0), 1e-9);
    }

    @Test
    void allSameValues() {
        double[] data = new double[1000];
        Arrays.fill(data, 7.0);
        assertEquals(7.0, floyd.compute(data, 1.0),  1e-9);
        assertEquals(7.0, floyd.compute(data, 50.0), 1e-9);
        assertEquals(7.0, floyd.compute(data, 99.0), 1e-9);
    }

    @Test
    void alreadySortedAscending() {
        double[] data = new double[500];
        for (int i = 0; i < data.length; i++) data[i] = i;
        assertEquals(baseline.compute(data, 1.0),  floyd.compute(data, 1.0),  1e-9);
        assertEquals(baseline.compute(data, 95.0), floyd.compute(data, 95.0), 1e-9);
    }

    @Test
    void alreadySortedDescending() {
        double[] data = new double[500];
        for (int i = 0; i < data.length; i++) data[i] = 500 - i;
        assertEquals(baseline.compute(data, 1.0),  floyd.compute(data, 1.0),  1e-9);
        assertEquals(baseline.compute(data, 99.0), floyd.compute(data, 99.0), 1e-9);
    }

    @Test
    void negativeValues() {
        double[] data = {-5.0, -1.0, -3.0, -2.0, -4.0};
        assertEquals(baseline.compute(data, 1.0),  floyd.compute(data, 1.0),  1e-9);
        assertEquals(baseline.compute(data, 99.0), floyd.compute(data, 99.0), 1e-9);
    }

    @Test
    void mixedNegativePositive() {
        double[] data = {-100.0, -50.0, 0.0, 50.0, 100.0};
        for (double p : new double[]{1.0, 25.0, 50.0, 75.0, 99.0}) {
            assertEquals(baseline.compute(data, p), floyd.compute(data, p), 1e-9,
                    "Mismatch at p=" + p);
        }
    }

    @Test
    void nanValuesAreIgnored() {
        double[] data = {1.0, Double.NaN, 3.0, Double.NaN, 5.0};
        double expected = baseline.compute(data, 50.0);
        double actual   = floyd.compute(data, 50.0);
        assertFalse(Double.isNaN(actual), "Result should not be NaN when valid values exist");
        assertEquals(expected, actual, 1e-9);
    }

    @Test
    void allNanReturnsNan() {
        double[] data = {Double.NaN, Double.NaN, Double.NaN};
        assertTrue(Double.isNaN(floyd.compute(data, 50.0)));
    }

    @Test
    void doesNotMutateOriginalArray() {
        double[] data = {5.0, 1.0, 3.0, 2.0, 4.0};
        double[] copy = data.clone();
        floyd.compute(data, 50.0);
        assertArrayEquals(copy, data, 1e-9, "Floyd-Rivest must not mutate the input array");
    }

    @ParameterizedTest
    @ValueSource(ints = {100, 1_000, 10_000, 40_000})
    void randomArrayMatchesBaseline_variousSizes(int size) {
        double[] data = randomArray(size, 12345L);
        for (double p : new double[]{1.0, 5.0, 50.0, 95.0, 99.0}) {
            assertEquals(baseline.compute(data, p), floyd.compute(data, p), 1e-9,
                    "Mismatch at size=" + size + " p=" + p);
        }
    }

    @Test
    void randomLargeArray_productionScale() {
        // Closest to production: 200 000 elements, 3 percentiles
        double[] data = randomArray(200_000, 99999L);
        for (double p : new double[]{1.0, 95.0, 99.0}) {
            assertEquals(baseline.compute(data, p), floyd.compute(data, p), 1e-9,
                    "Mismatch at p=" + p);
        }
    }

    // ── Performance (nem assert, csak log) ────────────────────────────

    @Test
    void performanceComparisonAt200k() {
        double[] data = randomArray(200_000, 42L);
        int warmupRuns = 5;
        int timedRuns  = 20;
        double p = 1.0;

        // Warmup
        for (int i = 0; i < warmupRuns; i++) {
            baseline.compute(data, p);
            floyd.compute(data, p);
        }

        long baselineNs = 0;
        for (int i = 0; i < timedRuns; i++) {
            long t = System.nanoTime();
            baseline.compute(data, p);
            baselineNs += System.nanoTime() - t;
        }

        long floydNs = 0;
        for (int i = 0; i < timedRuns; i++) {
            long t = System.nanoTime();
            floyd.compute(data, p);
            floydNs += System.nanoTime() - t;
        }

        double baselineMs = baselineNs / 1_000_000.0 / timedRuns;
        double floydMs    = floydNs    / 1_000_000.0 / timedRuns;
        double ratio      = baselineMs / floydMs;

        System.out.printf(
                "[PERF] n=200k, p=%.0f%%: baseline=%.3fms  floyd=%.3fms  speedup=%.2fx%n",
                p, baselineMs, floydMs, ratio);

        // Floyd-Rivest should be at least 2× faster for a single percentile at this scale
        assertTrue(ratio > 2.0,
                String.format("Expected Floyd-Rivest >2× speedup, got %.2fx (baseline=%.3fms, floyd=%.3fms)",
                        ratio, baselineMs, floydMs));
    }

    @Test
    void multiplePercentiles_sortBasedWins() {
        // For 3 percentiles, sort-based should be comparable or faster (one sort, 3 indexes)
        double[] data = randomArray(200_000, 77L);
        double[] percentiles = {1.0, 95.0, 99.0};
        int timedRuns = 20;

        // Warmup
        for (int i = 0; i < 5; i++) {
            for (double p : percentiles) { baseline.compute(data, p); floyd.compute(data, p); }
        }

        long baselineNs = 0;
        for (int i = 0; i < timedRuns; i++) {
            long t = System.nanoTime();
            for (double p : percentiles) baseline.compute(data, p);
            baselineNs += System.nanoTime() - t;
        }

        long floydNs = 0;
        for (int i = 0; i < timedRuns; i++) {
            long t = System.nanoTime();
            for (double p : percentiles) floyd.compute(data, p);
            floydNs += System.nanoTime() - t;
        }

        double baselineMs = baselineNs / 1_000_000.0 / timedRuns;
        double floydMs    = floydNs    / 1_000_000.0 / timedRuns;

        double ratio = baselineMs / floydMs;
        System.out.printf(
                "[PERF] n=200k, 3 percentiles: baseline=%.3fms  floyd=%.3fms  speedup=%.2fx%n",
                baselineMs, floydMs, ratio);

        // Sort-based re-sorts on every compute() call, so Floyd-Rivest wins even for 3 percentiles.
        // A shared-sort multi-percentile calculator would beat Floyd-Rivest here,
        // but that is a separate implementation. Floyd-Rivest must be faster than sort-based.
        assertTrue(ratio > 1.5,
                String.format("Expected Floyd-Rivest faster than sort-based for 3 percentiles, got %.2fx", ratio));
    }

    // ── Helpers ────────────────────────────────────────────────────────

    private static double[] randomArray(int size, long seed) {
        Random rng = new Random(seed);
        double[] arr = new double[size];
        for (int i = 0; i < size; i++) arr[i] = rng.nextGaussian();
        return arr;
    }
}
