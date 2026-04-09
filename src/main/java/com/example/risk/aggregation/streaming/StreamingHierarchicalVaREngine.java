package com.example.risk.aggregation.streaming;

import com.example.risk.model.GroupKey;
import com.example.risk.model.RiskRecord;
import com.example.risk.percentile.PercentileCalculator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * True streaming hierarchical VaR engine for prefix-rollup processing.
 *
 * <p>Unlike the map-based rollup path, this engine never materializes
 * {@code Map<GroupKey, double[]>} for all levels. It keeps only one live
 * accumulator per level, computes exact percentile values when a group closes,
 * emits the result to a sink, and releases the aggregate immediately.
 */
public class StreamingHierarchicalVaREngine {

    private final PercentileCalculator calculator;
    private final Consumer<String> progress;

    public StreamingHierarchicalVaREngine(PercentileCalculator calculator, Consumer<String> progress) {
        this.calculator = calculator;
        this.progress = progress == null ? ignored -> {} : progress;
    }

    public StreamComputeResult processInMemory(List<RiskRecord> records,
                                               List<Integer> interesting,
                                               List<Double> percentiles,
                                               VaROutputSink sink) {
        long orderingStart = System.nanoTime();
        List<RiskRecord> sorted = new ArrayList<>(records);
        sorted.sort(comparator(interesting));
        long orderingMs = (System.nanoTime() - orderingStart) / 1_000_000L;
        StreamComputeResult orderedResult = processOrderedSource(consumer -> {
            for (RiskRecord record : sorted) {
                consumer.accept(record);
            }
        }, interesting, percentiles, sink);
        return new StreamComputeResult(orderingMs, orderedResult.aggregationMs, orderedResult.percentileMs);
    }

    public StreamComputeResult processOrderedSource(OrderedRiskRecordSource source,
                                                    List<Integer> interesting,
                                                    List<Double> percentiles,
                                                    VaROutputSink sink) {
        int n = interesting.size();
        int totalLevels = n + 1;
        double[][] acc = new double[totalLevels][];
        final String[][] prevAttrsRef = new String[1][];
        AtomicInteger[] groupCounts = new AtomicInteger[totalLevels];
        for (int i = 0; i < totalLevels; i++) {
            groupCounts[i] = new AtomicInteger();
        }

        long totalStart = System.nanoTime();
        long[] percentileNs = {0L};

        try {
            source.forEach(record -> {
                String[] cur = extractAttrs(record, interesting);
                double[] nums = record.getNumbers();

                if (prevAttrsRef[0] == null) {
                    for (int numDims = 0; numDims <= n; numDims++) {
                        acc[numDims] = nums.clone();
                    }
                    prevAttrsRef[0] = cur;
                    return;
                }

                int firstChanged = firstChangedIndex(prevAttrsRef[0], cur, n);
                if (firstChanged < 0) {
                    for (int numDims = 0; numDims <= n; numDims++) {
                        addInPlace(acc[numDims], nums);
                    }
                } else {
                    for (int numDims = n; numDims > firstChanged; numDims--) {
                        closeGroup(n, numDims, prevAttrsRef[0], acc[numDims], interesting, percentiles, sink,
                                groupCounts, percentileNs);
                        acc[numDims] = nums.clone();
                    }
                    for (int numDims = firstChanged; numDims >= 0; numDims--) {
                        addInPlace(acc[numDims], nums);
                    }
                }

                prevAttrsRef[0] = cur;
            });
        } catch (Exception e) {
            throw new RuntimeException("Streaming hierarchical VaR failed", e);
        }

        if (prevAttrsRef[0] != null) {
            for (int numDims = n; numDims >= 0; numDims--) {
                closeGroup(n, numDims, prevAttrsRef[0], acc[numDims], interesting, percentiles, sink,
                        groupCounts, percentileNs);
                acc[numDims] = null;
            }
        }

        for (int levelIndex = 0; levelIndex < totalLevels; levelIndex++) {
            List<Integer> dims = dimsAtLevel(interesting, levelIndex);
            sink.finishLevel(levelIndex, dims, groupCounts[levelIndex].get());
        }

        long totalNs = System.nanoTime() - totalStart;
        long percentileMs = percentileNs[0] / 1_000_000L;
        long aggregationMs = Math.max(0L, (totalNs - percentileNs[0]) / 1_000_000L);
        progress.accept(String.format("Streaming prefix-rollup complete: %d levels", totalLevels));
        return new StreamComputeResult(0L, aggregationMs, percentileMs);
    }

    private void closeGroup(int totalDims,
                            int numDims,
                            String[] attrs,
                            double[] aggregate,
                            List<Integer> interesting,
                            List<Double> percentiles,
                            VaROutputSink sink,
                            AtomicInteger[] groupCounts,
                            long[] percentileNs) {
        int levelIndex = totalDims - numDims;
        List<Integer> dims = dimsAtLevel(interesting, levelIndex);
        GroupKey key = makeKey(attrs, numDims);
        long t0 = System.nanoTime();
        Map<Double, Double> varValues = computePercentiles(aggregate, percentiles);
        percentileNs[0] += System.nanoTime() - t0;
        sink.acceptGroup(levelIndex, dims, key, varValues);
        groupCounts[levelIndex].incrementAndGet();
    }

    private Map<Double, Double> computePercentiles(double[] aggregate, List<Double> percentiles) {
        Map<Double, Double> out = new LinkedHashMap<>();
        for (double p : percentiles) {
            out.put(p, calculator.compute(aggregate, p));
        }
        return out;
    }

    private static List<Integer> dimsAtLevel(List<Integer> interesting, int levelIndex) {
        int size = interesting.size() - levelIndex;
        return List.copyOf(interesting.subList(0, size));
    }

    private static Comparator<RiskRecord> comparator(List<Integer> interesting) {
        return (r1, r2) -> {
            List<String> a1 = r1.getAttributes();
            List<String> a2 = r2.getAttributes();
            for (int idx : interesting) {
                int cmp = a1.get(idx).compareTo(a2.get(idx));
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        };
    }

    private static String[] extractAttrs(RiskRecord record, List<Integer> interesting) {
        List<String> all = record.getAttributes();
        String[] attrs = new String[interesting.size()];
        for (int j = 0; j < interesting.size(); j++) {
            attrs[j] = all.get(interesting.get(j));
        }
        return attrs;
    }

    private static int firstChangedIndex(String[] prev, String[] cur, int n) {
        for (int j = 0; j < n; j++) {
            if (!cur[j].equals(prev[j])) {
                return j;
            }
        }
        return -1;
    }

    private static GroupKey makeKey(String[] attrs, int numDims) {
        if (numDims == 0) {
            return new GroupKey(List.of());
        }
        List<String> values = new ArrayList<>(numDims);
        for (int i = 0; i < numDims; i++) {
            values.add(attrs[i]);
        }
        return new GroupKey(values);
    }

    private static void addInPlace(double[] target, double[] source) {
        for (int i = 0; i < Math.min(target.length, source.length); i++) {
            target[i] += source[i];
        }
    }

    public static final class StreamComputeResult {
        public final long orderingMs;
        public final long aggregationMs;
        public final long percentileMs;

        public StreamComputeResult(long orderingMs, long aggregationMs, long percentileMs) {
            this.orderingMs = orderingMs;
            this.aggregationMs = aggregationMs;
            this.percentileMs = percentileMs;
        }
    }

}
