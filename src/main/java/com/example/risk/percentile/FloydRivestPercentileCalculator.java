package com.example.risk.percentile;

import java.util.Arrays;

/**
 * Percentile calculator backed by the Floyd-Rivest SELECT algorithm.
 *
 * <p>For interpolated percentiles this performs two independent exact selections,
 * one for the lower rank and one for the upper rank, then interpolates between them.
 */
public class FloydRivestPercentileCalculator implements PercentileCalculator {

    private static final int INSERTION_SORT_THRESHOLD = 32;
    private static final int SAMPLE_THRESHOLD = 600;

    @Override
    public double compute(double[] data, double percentile) {
        double[] arr = Arrays.stream(data)
                .filter(v -> !Double.isNaN(v))
                .toArray();

        if (arr.length == 0) {
            return Double.NaN;
        }

        double pos = (percentile / 100.0) * (arr.length - 1);
        int lo = (int) Math.floor(pos);
        int hi = Math.min(lo + 1, arr.length - 1);
        double frac = pos - lo;

        double vLo = selectAt(arr.clone(), lo);
        if (lo == hi || frac == 0.0) {
            return vLo;
        }

        double vHi = selectAt(arr.clone(), hi);
        return vLo * (1.0 - frac) + vHi * frac;
    }

    @Override
    public String name() {
        return "Floyd-Rivest SELECT";
    }

    private static double selectAt(double[] arr, int k) {
        int left = 0;
        int right = arr.length - 1;

        while (right > left) {
            if (right - left <= INSERTION_SORT_THRESHOLD) {
                insertionSort(arr, left, right);
                return arr[k];
            }

            if (right - left > SAMPLE_THRESHOLD) {
                int n = right - left + 1;
                int i = k - left + 1;
                double z = Math.log(n);
                double s = 0.5 * Math.exp(2.0 * z / 3.0);
                double sd = 0.5 * Math.sqrt(z * s * (n - s) / n) * sign(i - n / 2.0);
                int newLeft = Math.max(left, (int) Math.floor(k - i * s / n + sd));
                int newRight = Math.min(right, (int) Math.floor(k + (n - i) * s / n + sd));
                selectRange(arr, newLeft, newRight, k);
            }

            double pivot = arr[k];
            int i = left;
            int j = right;
            swap(arr, left, k);
            if (arr[right] > pivot) {
                swap(arr, left, right);
            }

            while (i < j) {
                swap(arr, i, j);
                i++;
                j--;
                while (arr[i] < pivot) {
                    i++;
                }
                while (arr[j] > pivot) {
                    j--;
                }
            }

            if (arr[left] == pivot) {
                swap(arr, left, j);
            } else {
                j++;
                swap(arr, j, right);
            }

            if (j <= k) {
                left = j + 1;
            }
            if (k <= j) {
                right = j - 1;
            }
        }

        return arr[k];
    }

    private static void selectRange(double[] arr, int left, int right, int k) {
        while (right > left) {
            if (right - left <= INSERTION_SORT_THRESHOLD) {
                insertionSort(arr, left, right);
                return;
            }

            double pivot = arr[k];
            int i = left;
            int j = right;
            swap(arr, left, k);
            if (arr[right] > pivot) {
                swap(arr, left, right);
            }

            while (i < j) {
                swap(arr, i, j);
                i++;
                j--;
                while (arr[i] < pivot) {
                    i++;
                }
                while (arr[j] > pivot) {
                    j--;
                }
            }

            if (arr[left] == pivot) {
                swap(arr, left, j);
            } else {
                j++;
                swap(arr, j, right);
            }

            if (j <= k) {
                left = j + 1;
            }
            if (k <= j) {
                right = j - 1;
            }
        }
    }

    private static void insertionSort(double[] arr, int left, int right) {
        for (int i = left + 1; i <= right; i++) {
            double value = arr[i];
            int j = i - 1;
            while (j >= left && arr[j] > value) {
                arr[j + 1] = arr[j];
                j--;
            }
            arr[j + 1] = value;
        }
    }

    private static double sign(double value) {
        if (value < 0) {
            return -1.0;
        }
        if (value > 0) {
            return 1.0;
        }
        return 0.0;
    }

    private static void swap(double[] arr, int a, int b) {
        double tmp = arr[a];
        arr[a] = arr[b];
        arr[b] = tmp;
    }
}
