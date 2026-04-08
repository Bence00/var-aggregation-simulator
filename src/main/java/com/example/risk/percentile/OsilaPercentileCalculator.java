package com.example.risk.percentile;

import java.util.Arrays;
import java.util.Random;

/**
 * OSILA (Order Statistics In Large Arrays) percentile calculator.
 *
 * <p>This implementation follows Cerasa's two-step OSILA procedure:
 * sample without replacement, derive paper-defined bounds around the target
 * order statistic, then sort only the filtered interval.
 */
public class OsilaPercentileCalculator implements PercentileCalculator {

    private static final int SORT_THRESHOLD = 64;
    private static final double Z_ALPHA = 2.576; // alpha = 0.01 in the paper
    private static final long RNG_SEED = 0x4F53494C41L;

    @Override
    public double compute(double[] data, double percentile) {
        double[] arr = Arrays.stream(data)
                .filter(v -> !Double.isNaN(v))
                .toArray();

        if (arr.length == 0) {
            return Double.NaN;
        }

        int n = arr.length;
        double pos = (percentile / 100.0) * (n - 1);
        int lo = (int) Math.floor(pos);
        int hi = Math.min(lo + 1, n - 1);
        double frac = pos - lo;

        // osilaSelect is non-destructive for large arrays (mutates only internal copies).
        // For arrays <= SORT_THRESHOLD, osilaSelect clones internally before sorting,
        // so calling it twice on the same arr is safe — no external clone needed.
        double vLo = osilaSelect(arr, lo + 1);
        if (lo == hi || frac == 0.0) {
            return vLo;
        }

        double vHi = osilaSelect(arr, hi + 1);
        return vLo * (1.0 - frac) + vHi * frac;
    }

    @Override
    public String name() {
        return "OSILA (randomised order-statistic)";
    }

    private double osilaSelect(double[] values, int targetRank) {
        if (values.length <= SORT_THRESHOLD) {
            // Clone here so `values` (= the caller's arr) is not mutated,
            // allowing a second call on the same array for interpolation.
            double[] copy = values.clone();
            Arrays.sort(copy);
            return copy[targetRank - 1];
        }

        Random rng = new Random(RNG_SEED ^ values.length ^ ((long) targetRank << 32));
        double[] current = values;
        int k = targetRank;

        while (current.length > SORT_THRESHOLD) {
            int n = current.length;
            int sampleSize = sampleSize(n, k);
            if (sampleSize >= n) {
                break;
            }

            double[] sample = sampleWithoutReplacement(current, sampleSize, rng);
            Arrays.sort(sample);

            int j0 = clamp((int) Math.round(((double) (sampleSize + 1) * k) / (n + 1.0)), 1, sampleSize);
            double x0 = sample[j0 - 1];
            RankStats rank0 = rankStats(current, x0);

            if (rank0.contains(k)) {
                return x0;
            }

            Bounds bounds;
            if (rank0.isAbove(k)) {
                bounds = resolveLowerBound(current, sample, n, k, j0, rank0.lessOrEqual);
                bounds.upperValue = x0;
                bounds.upperInclusiveCount = rank0.lessOrEqual;
            } else {
                bounds = resolveUpperBound(current, sample, n, k, j0, rank0.lessOrEqual);
                bounds.lowerValue = x0;
                bounds.lowerExclusiveCount = rank0.less;
            }

            if (!Double.isFinite(bounds.lowerValue) && !Double.isFinite(bounds.upperValue)) {
                break;
            }

            double[] reduced = filterBetween(current, bounds.lowerValue, bounds.upperValue);
            int nextK = k - bounds.lowerExclusiveCount;
            if (reduced.length == 0 || nextK < 1 || nextK > reduced.length || reduced.length >= current.length) {
                break;
            }

            current = reduced;
            k = nextK;
        }

        Arrays.sort(current);
        return current[k - 1];
    }

    private Bounds resolveLowerBound(double[] values, double[] sample, int n, int k, int j0, int k0) {
        Bounds bounds = new Bounds();
        int currentJ0 = j0;
        int currentK0 = k0;

        while (true) {
            int jAlpha = lowerBoundSampleRank(currentJ0, currentK0, k);
            if (jAlpha <= 0) {
                bounds.lowerValue = Double.NEGATIVE_INFINITY;
                bounds.lowerExclusiveCount = 0;
                return bounds;
            }

            double xAlpha = sample[jAlpha - 1];
            RankStats stats = rankStats(values, xAlpha);
            if (stats.contains(k)) {
                bounds.lowerValue = xAlpha;
                bounds.upperValue = xAlpha;
                bounds.lowerExclusiveCount = stats.less;
                bounds.upperInclusiveCount = stats.lessOrEqual;
                return bounds;
            }

            if (stats.isBelow(k)) {
                bounds.lowerValue = xAlpha;
                bounds.lowerExclusiveCount = stats.less;
                return bounds;
            }

            if (jAlpha == 1) {
                bounds.lowerValue = Double.NEGATIVE_INFINITY;
                bounds.lowerExclusiveCount = 0;
                return bounds;
            }

            if (jAlpha >= currentJ0) {
                bounds.lowerValue = Double.NEGATIVE_INFINITY;
                bounds.lowerExclusiveCount = 0;
                return bounds;
            }

            currentJ0 = jAlpha;
            currentK0 = stats.lessOrEqual;
        }
    }

    private Bounds resolveUpperBound(double[] values, double[] sample, int n, int k, int j0, int k0) {
        Bounds bounds = new Bounds();
        int currentJ0 = j0;
        int currentK0 = k0;
        int sampleSize = sample.length;

        while (true) {
            int localRank = upperBoundSampleRank(sampleSize, n, currentJ0, currentK0, k);
            int suffixSize = sampleSize - currentJ0;
            if (localRank > suffixSize) {
                bounds.upperValue = Double.POSITIVE_INFINITY;
                bounds.upperInclusiveCount = n;
                return bounds;
            }

            int jAlpha = currentJ0 + localRank;
            double xAlpha = sample[jAlpha - 1];
            RankStats stats = rankStats(values, xAlpha);
            if (stats.contains(k)) {
                bounds.lowerValue = xAlpha;
                bounds.upperValue = xAlpha;
                bounds.lowerExclusiveCount = stats.less;
                bounds.upperInclusiveCount = stats.lessOrEqual;
                return bounds;
            }

            if (stats.isAbove(k)) {
                bounds.upperValue = xAlpha;
                bounds.upperInclusiveCount = stats.lessOrEqual;
                return bounds;
            }

            if (jAlpha == sampleSize) {
                bounds.upperValue = Double.POSITIVE_INFINITY;
                bounds.upperInclusiveCount = n;
                return bounds;
            }

            if (localRank <= 0 || jAlpha <= currentJ0) {
                bounds.upperValue = Double.POSITIVE_INFINITY;
                bounds.upperInclusiveCount = n;
                return bounds;
            }

            currentJ0 = jAlpha;
            currentK0 = stats.lessOrEqual;
        }
    }

    private int lowerBoundSampleRank(int j0, int k0, int k) {
        double aRatio = (double) k0 / j0;
        double varianceTerm = Math.max(0.0, ((double) k0 * (k0 - j0)) / (j0 * j0 * (j0 + 1.0)));
        double bScale = Z_ALPHA * Math.sqrt(varianceTerm);
        double qa = -(aRatio * aRatio) - (bScale * bScale);
        double qb = (bScale * bScale * j0) + (2.0 * aRatio * k);
        double qc = -(double) k * k;
        double disc = qb * qb - 4.0 * qa * qc;
        if (disc < 0.0) {
            return 0;
        }

        double root = (-qb + Math.sqrt(disc)) / (2.0 * qa);
        return (int) Math.floor(root);
    }

    private int upperBoundSampleRank(int sampleSize, int n, int j0, int k0, int k) {
        double cRatio = (double) (n - k0 + 1) / (sampleSize - j0 + 1.0);
        double varianceTerm = Math.max(
                0.0,
                ((double) (n - k0 + 1) * (n - k0 - sampleSize + j0))
                        / ((sampleSize - j0 + 1.0) * (sampleSize - j0 + 1.0) * (sampleSize - j0 + 2.0))
        );
        double dScale = Z_ALPHA * Math.sqrt(varianceTerm);
        double qa = (cRatio * cRatio) + (dScale * dScale);
        double qb = (2.0 * cRatio * (k0 - k)) - (sampleSize * dScale * dScale) + (j0 * dScale * dScale) - (dScale * dScale);
        double qc = (double) (k0 - k) * (k0 - k);
        double disc = qb * qb - 4.0 * qa * qc;
        if (disc < 0.0) {
            return sampleSize + 1;
        }

        double root = (-qb + Math.sqrt(disc)) / (2.0 * qa);
        return (int) Math.ceil(root);
    }

    private static double[] sampleWithoutReplacement(double[] values, int sampleSize, Random rng) {
        // boolean[] avoids Integer boxing that HashSet<Integer> would incur.
        boolean[] used = new boolean[values.length];
        double[] sample = new double[sampleSize];
        int idx = 0;
        while (idx < sampleSize) {
            int pick = rng.nextInt(values.length);
            if (!used[pick]) {
                used[pick] = true;
                sample[idx++] = values[pick];
            }
        }
        return sample;
    }

    private static double[] filterBetween(double[] values, double lower, double upper) {
        int count = 0;
        for (double value : values) {
            if (value >= lower && value <= upper) {
                count++;
            }
        }

        double[] filtered = new double[count];
        int idx = 0;
        for (double value : values) {
            if (value >= lower && value <= upper) {
                filtered[idx++] = value;
            }
        }
        return filtered;
    }

    private static RankStats rankStats(double[] values, double pivot) {
        int less = 0;
        int lessOrEqual = 0;
        for (double value : values) {
            if (value < pivot) {
                less++;
            }
            if (value <= pivot) {
                lessOrEqual++;
            }
        }
        return new RankStats(less, lessOrEqual);
    }

    private static int sampleSize(int n, int k) {
        if (n <= SORT_THRESHOLD) {
            return n;
        }

        if (k <= 1 || k >= n) {
            return clamp((int) Math.ceil(Math.sqrt(n)), 2, n - 1);
        }

        int left = Math.max(k, 1);
        int right = Math.max(n - k, 1);
        double q = Math.sqrt((2.0 * k / Math.PI) * ((n - k + 1.0) / (n + 1.0)));
        double g = Z_ALPHA / 4.0;
        double a = 2.0 * q * g * ((1.0 / Math.sqrt(left)) + (1.0 / Math.sqrt(right)));
        double b = q + (g * Math.sqrt(left)) - (g / Math.sqrt(left)) + (g * Math.sqrt(right));

        double lo = 0.0;
        double hi = Math.max(1.0, Math.sqrt(n));
        while ((a * hi * hi * hi * hi) + (b * hi * hi * hi) - (2.0 * n) < 0.0) {
            hi *= 2.0;
        }

        for (int i = 0; i < 80; i++) {
            double mid = (lo + hi) * 0.5;
            double value = (a * mid * mid * mid * mid) + (b * mid * mid * mid) - (2.0 * n);
            if (value >= 0.0) {
                hi = mid;
            } else {
                lo = mid;
            }
        }

        double r = hi;
        int sampleSize = (int) Math.round(n / (r * r));
        return clamp(sampleSize, 2, n - 1);
    }

    private static int clamp(int value, int min, int max) {
        return Math.max(min, Math.min(max, value));
    }

    private static final class RankStats {
        private final int less;
        private final int lessOrEqual;

        private RankStats(int less, int lessOrEqual) {
            this.less = less;
            this.lessOrEqual = lessOrEqual;
        }

        private boolean contains(int rank) {
            return rank > less && rank <= lessOrEqual;
        }

        private boolean isAbove(int rank) {
            return rank <= less;
        }

        private boolean isBelow(int rank) {
            return rank > lessOrEqual;
        }
    }

    private static final class Bounds {
        private double lowerValue = Double.NEGATIVE_INFINITY;
        private double upperValue = Double.POSITIVE_INFINITY;
        private int lowerExclusiveCount;
        private int upperInclusiveCount;
    }
}
