package com.example.risk.service;

import com.example.risk.aggregation.AggregationStrategy;
import com.example.risk.aggregation.impl.BaselineAggregationStrategy;
import com.example.risk.aggregation.impl.PrefixRollupAggregationStrategy;
import com.example.risk.aggregation.impl.PrefixStreamingAggregationStrategy;
import com.example.risk.aggregation.impl.StreamingAggregationStrategy;
import com.example.risk.aggregation.streaming.CollectingVaROutputSink;
import com.example.risk.aggregation.streaming.CountingVaROutputSink;
import com.example.risk.aggregation.streaming.OrderedRiskRecordSource;
import com.example.risk.aggregation.streaming.StreamingAggregationResult;
import com.example.risk.aggregation.streaming.StreamingHierarchicalVaREngine;
import com.example.risk.aggregation.strategy.HierarchicalVaREngine;
import com.example.risk.benchmark.BenchmarkRunner;
import com.example.risk.config.AppConfig;
import com.example.risk.generator.GeneratorConfig;
import com.example.risk.generator.SyntheticDataGenerator;
import com.example.risk.generator.distribution.*;
import com.example.risk.model.BenchmarkResult;
import com.example.risk.model.GroupKey;
import com.example.risk.model.LevelResult;
import com.example.risk.model.RiskRecord;
import com.example.risk.percentile.FloydRivestPercentileCalculator;
import com.example.risk.percentile.OsilaPercentileCalculator;
import com.example.risk.percentile.PercentileCalculator;
import com.example.risk.percentile.SortBasedPercentileCalculator;
import com.example.risk.repository.BenchmarkRepository;
import com.example.risk.repository.RecordRepository;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Consumer;

/**
 * Central service that wires together all subsystems.
 * The UI interacts exclusively through this class.
 */
public class VaRService {

    private final AppConfig            config;
    private RecordRepository           recordRepo;
    private final BenchmarkRepository  benchmarkRepo;
    private final SyntheticDataGenerator generator;
    private final PercentileCalculator calculator;

    /** Available aggregation strategies by display name. */
    private final Map<String, AggregationStrategy> strategies;

    /** Available percentile calculators (selection algorithms) by display name. */
    private final Map<String, PercentileCalculator> calculators;

    /** Records currently loaded in memory (from last generate or load call). */
    private List<RiskRecord> loadedRecords = new ArrayList<>();
    private String           loadedDatasetId = "";

    public VaRService(AppConfig config) {
        this.config        = config;
        this.recordRepo    = new RecordRepository(config);
        this.benchmarkRepo = new BenchmarkRepository(config);
        this.generator     = new SyntheticDataGenerator();
        this.calculator    = new SortBasedPercentileCalculator(); // default

        strategies = new LinkedHashMap<>();
        strategies.put("Baseline (HashMap)",        new BaselineAggregationStrategy());
        strategies.put("Streaming (sort-merge)",    new StreamingAggregationStrategy());
        strategies.put("Prefix rollup (sort-scan)", new PrefixRollupAggregationStrategy());
        strategies.put("Prefix streaming (DB ordered)", new PrefixStreamingAggregationStrategy());

        calculators = new LinkedHashMap<>();
        calculators.put("Sort-based (baseline)",           new SortBasedPercentileCalculator());
        calculators.put("Floyd-Rivest SELECT",             new FloydRivestPercentileCalculator());
        calculators.put("OSILA (randomised order-statistic)", new OsilaPercentileCalculator());
    }

    // ── Schema ─────────────────────────────────────────────────────────

    public void initDatabase() throws SQLException {
        recordRepo.ensureSchema();
        benchmarkRepo.ensureSchema();
    }

    public int getSchemaMaxAttributes() {
        return config.getSchemaMaxAttributes();
    }

    public void updateSchemaMaxAttributes(int value) throws SQLException {
        config.setSchemaMaxAttributes(value);
        this.recordRepo = new RecordRepository(config);
        initDatabase();
    }

    // ── Queries ────────────────────────────────────────────────────────

    public Set<String>         getStrategyNames()    { return strategies.keySet(); }
    public Set<String>         getCalculatorNames()  { return calculators.keySet(); }
    public List<String>        getDistributionNames() {
        return List.of("Normal", "LogNormal", "Uniform", "Exponential", "Mixture");
    }
    public List<RiskRecord>    getLoadedRecords()    { return loadedRecords; }
    public String              getLoadedDatasetId()  { return loadedDatasetId; }

    public List<String> listDatasetIds() throws SQLException {
        return recordRepo.listDatasetIds();
    }

    // ── Data generation ────────────────────────────────────────────────

    /**
     * Generate synthetic records, persist them to PostgreSQL, and keep them in memory.
     *
     * @return BenchmarkResult with generation + insert timings
     */
    public BenchmarkResult generateAndSave(GeneratorConfig genConfig) throws SQLException {
        BenchmarkResult result = new BenchmarkResult();
        result.setRunLabel(timestamp() + " generate");
        result.setDatasetId(genConfig.getDatasetId());
        result.setRecordCount(genConfig.getNumRecords());
        result.setNumbersLength(genConfig.getNumbersLength());
        result.setStrategyName("N/A");

        long totalStart = System.nanoTime();

        // Generate
        var genTimed = BenchmarkRunner.time(() -> generator.generate(genConfig));
        result.setGenerationMs(genTimed.elapsedMs);
        List<RiskRecord> records = genTimed.unwrap();

        // Delete old records for this dataset, then insert
        recordRepo.deleteByDatasetId(genConfig.getDatasetId());
        var insertTimed = BenchmarkRunner.time(() -> recordRepo.insertBatch(records));
        result.setDbInsertMs(insertTimed.elapsedMs);
        insertTimed.unwrap();

        loadedRecords   = records;
        loadedDatasetId = genConfig.getDatasetId();

        result.setTotalMs((System.nanoTime() - totalStart) / 1_000_000L);
        saveBenchmark(result);
        return result;
    }

    // ── DB load ────────────────────────────────────────────────────────

    /** Load records from the database into memory. Returns load time ms. */
    public long loadFromDb(String datasetId) throws SQLException {
        var timed = BenchmarkRunner.time(() -> recordRepo.findByDatasetId(datasetId));
        loadedRecords   = timed.unwrap();
        loadedDatasetId = datasetId;
        return timed.elapsedMs;
    }

    // ── VaR computation ────────────────────────────────────────────────

    /**
     * Run hierarchical VaR on the currently loaded records.
     *
     * @param strategyName    key from {@link #getStrategyNames()}
     * @param interestingDims attribute indices for grouping (ordered)
     * @param percentiles     percentile levels in [0,100]
     * @return one LevelResult per hierarchy level (leaf first)
     */
    public List<LevelResult> runVaR(String         strategyName,
                                    String         calculatorName,
                                    List<Integer>  interestingDims,
                                    List<Double>   percentiles) {
        return runVaR(strategyName, calculatorName, interestingDims, percentiles, ignored -> {});
    }

    public List<LevelResult> runVaR(String           strategyName,
                                    String           calculatorName,
                                    List<Integer>    interestingDims,
                                    List<Double>     percentiles,
                                    Consumer<String> progress) {
        AggregationStrategy strategy = strategies.getOrDefault(strategyName,
                new BaselineAggregationStrategy());
        PercentileCalculator calc = calculators.getOrDefault(calculatorName, calculator);
        HierarchicalVaREngine engine = new HierarchicalVaREngine(strategy, calc, progress);
        return engine.compute(loadedRecords, interestingDims, percentiles);
    }

    // ── Full benchmark ─────────────────────────────────────────────────

    /**
     * Generate → save → load → aggregate → compute percentiles.
     * Records all stage timings and saves them to the DB.
     */
    public BenchmarkResult runFullBenchmark(GeneratorConfig genConfig,
                                            String          strategyName,
                                            String          calculatorName,
                                            List<Integer>   interestingDims,
                                            List<Double>    percentiles) throws SQLException {
        return runFullBenchmark(genConfig, strategyName, calculatorName, interestingDims, percentiles, ignored -> {});
    }

    public BenchmarkResult runQueryBenchmark(String datasetId,
                                             String strategyName,
                                             String calculatorName,
                                             List<Integer> interestingDims,
                                             List<Double> percentiles) throws SQLException {
        return runQueryBenchmark(datasetId, strategyName, calculatorName, interestingDims, percentiles, ignored -> {});
    }

    public BenchmarkResult runFullBenchmark(GeneratorConfig  genConfig,
                                            String           strategyName,
                                            String           calculatorName,
                                            List<Integer>    interestingDims,
                                            List<Double>     percentiles,
                                            Consumer<String> progress) throws SQLException {
        BenchmarkResult result = new BenchmarkResult();
        result.setRunLabel(timestamp() + " benchmark");
        result.setDatasetId(genConfig.getDatasetId());
        result.setRecordCount(genConfig.getNumRecords());
        result.setNumbersLength(genConfig.getNumbersLength());
        result.setStrategyName(strategyName + " / " + calculatorName);

        String datasetId = genConfig.getDatasetId();
        AggregationStrategy strategy = freshStrategy(strategyName);
        PercentileCalculator calc = freshCalculator(calculatorName);

        long generationNs;
        long insertNs;
        long loadNs;
        long orderingNs;
        long aggregationNs;
        long percentileNs;

        progress.accept("Benchmark: generating records");
        long t0 = System.nanoTime();
        List<RiskRecord> generated = generator.generate(genConfig);
        generationNs = System.nanoTime() - t0;

        progress.accept("Benchmark: saving records to database");
        recordRepo.deleteByDatasetId(datasetId);
        long t1 = System.nanoTime();
        recordRepo.insertBatch(generated);
        insertNs = System.nanoTime() - t1;

        // Ensure measured load is a fresh database read, not reused in-memory state.
        generated = null;
        loadedRecords = new ArrayList<>();
        loadedDatasetId = "";
        System.gc();

        List<RiskRecord> loaded = List.of();
        if (strategy instanceof PrefixStreamingAggregationStrategy) {
            progress.accept("Benchmark: warming up JIT...");
            List<RiskRecord> warmupSample = recordRepo.findSampleByDatasetId(datasetId, 200);
            warmup(new PrefixRollupAggregationStrategy(), freshCalculator(calculatorName), warmupSample, interestingDims, percentiles);
            loadNs = 0L;
        } else {
            progress.accept("Benchmark: loading records from database");
            long t2 = System.nanoTime();
            loaded = recordRepo.findByDatasetId(datasetId);
            loadNs = System.nanoTime() - t2;

            progress.accept("Benchmark: warming up JIT...");
            warmup(freshStrategy(strategyName), freshCalculator(calculatorName), loaded, interestingDims, percentiles);
        }
        System.gc();

        HierarchicalVaREngine engine = new HierarchicalVaREngine(strategy, calc, progress);
        if (strategy instanceof PrefixRollupAggregationStrategy) {
            progress.accept("Benchmark: ordering by interesting dimensions, then prefix-rollup");
            StreamingHierarchicalVaREngine streamingEngine =
                    new StreamingHierarchicalVaREngine(calc, progress);
            CollectingVaROutputSink sink = new CollectingVaROutputSink(levelDimensions(interestingDims));
            StreamingHierarchicalVaREngine.StreamComputeResult computed =
                    streamingEngine.processInMemory(loaded, interestingDims, percentiles, sink);
            orderingNs = computed.orderingMs * 1_000_000L;
            aggregationNs = computed.aggregationMs * 1_000_000L;
            percentileNs = computed.percentileMs * 1_000_000L;
            List<LevelResult> levels = sink.toLevelResults();
            emitBenchmarkDiagnostics(datasetId, loaded.size(), levels, generationNs, insertNs, loadNs, orderingNs, aggregationNs, percentileNs);
        } else if (strategy instanceof PrefixStreamingAggregationStrategy streamingStrategy) {
            progress.accept("Benchmark: ordered DB stream + prefix streaming rollup");
            CountingVaROutputSink sink = new CountingVaROutputSink(interestingDims.size() + 1);
            long t3 = System.nanoTime();
            StreamingAggregationResult computed = streamingStrategy.computeOrdered(
                    orderedDbSource(datasetId, interestingDims),
                    interestingDims,
                    percentiles,
                    calc,
                    sink,
                    progress
            );
            orderingNs = computed.orderingMs() * 1_000_000L;
            aggregationNs = computed.aggregationMs() * 1_000_000L;
            percentileNs = computed.percentileMs() * 1_000_000L;
            long streamTotalNs = System.nanoTime() - t3;
            loadNs = Math.max(0L, streamTotalNs - orderingNs - aggregationNs - percentileNs);
            emitBenchmarkDiagnostics(
                    datasetId,
                    genConfig.getNumRecords(),
                    streamingLevels(interestingDims, sink.levelCounts()),
                    generationNs,
                    insertNs,
                    loadNs,
                    orderingNs,
                    aggregationNs,
                    percentileNs
            );
        } else {
            orderingNs = 0L;
            progress.accept(String.format("Benchmark: aggregating leaf level for %s", strategyName));
            long t3 = System.nanoTime();
            Map<GroupKey, double[]> groups = strategy.aggregate(loaded, interestingDims);
            aggregationNs = System.nanoTime() - t3;

            progress.accept("Benchmark: computing percentiles");
            HierarchicalVaREngine.ComputeResult computed =
                    engine.computeTimedFromGroups(groups, interestingDims, percentiles);
            percentileNs = computed.percentileMs * 1_000_000L;
            emitBenchmarkDiagnostics(datasetId, loaded.size(), computed.levels, generationNs, insertNs, loadNs, orderingNs, aggregationNs, percentileNs);
        }

        result.setGenerationMs(nanosToMs(generationNs));
        result.setDbInsertMs(nanosToMs(insertNs));
        result.setDbLoadMs(nanosToMs(loadNs));
        result.setOrderingMs(nanosToMs(orderingNs));
        result.setAggregationMs(nanosToMs(aggregationNs));
        result.setPercentileMs(nanosToMs(percentileNs));

        long stageTotalMs = result.getGenerationMs()
                + result.getDbInsertMs()
                + result.getDbLoadMs()
                + result.getOrderingMs()
                + result.getAggregationMs()
                + result.getPercentileMs();
        result.setTotalMs(stageTotalMs);
        validateBenchmarkTotals(result);
        saveBenchmark(result);
        return result;
    }

    public BenchmarkResult runQueryBenchmark(String datasetId,
                                             String strategyName,
                                             String calculatorName,
                                             List<Integer> interestingDims,
                                             List<Double> percentiles,
                                             Consumer<String> progress) throws SQLException {
        BenchmarkResult result = new BenchmarkResult();
        result.setRunLabel(timestamp() + " query benchmark");
        result.setDatasetId(datasetId);
        result.setStrategyName(strategyName + " / " + calculatorName + " [query]");

        AggregationStrategy strategy = freshStrategy(strategyName);
        PercentileCalculator calc = freshCalculator(calculatorName);

        RecordRepository.DatasetInfo info = recordRepo.getDatasetInfo(datasetId);
        if (info.recordCount() == 0) {
            throw new IllegalStateException("Dataset not found or empty: " + datasetId);
        }

        result.setRecordCount(info.recordCount());
        result.setNumbersLength(info.numbersLength());

        progress.accept("Query benchmark: warming up JIT...");
        List<RiskRecord> warmupSample = recordRepo.findSampleByDatasetId(datasetId, 200);
        warmup(freshStrategy(strategyName), freshCalculator(calculatorName), warmupSample, interestingDims, percentiles);
        System.gc();

        HierarchicalVaREngine engine = new HierarchicalVaREngine(strategy, calc, progress);

        long loadNs;
        long orderingNs = 0L;
        long aggregationNs;
        long percentileNs;

        progress.accept("Query benchmark: loading records from DB");
        long t1 = System.nanoTime();
        List<RiskRecord> loaded = loadRecordsForQueryBenchmark(datasetId, strategy, interestingDims);
        loadNs = System.nanoTime() - t1;
        emitQueryStageDebug(
                datasetId,
                strategyName,
                "db-load",
                loadNs,
                "loadedRecords=" + loaded.size() + ", orderByInteresting=" + usesOrderedRead(strategy)
        );

        progress.accept("Query benchmark: leaf aggregation");
        long t2 = System.nanoTime();
        Map<GroupKey, double[]> groups = strategy.aggregate(loaded, interestingDims);
        aggregationNs = System.nanoTime() - t2;
        emitQueryStageDebug(
                datasetId,
                strategyName,
                "aggregation",
                aggregationNs,
                "leafGroups=" + groups.size() + ", loadedRecords=" + loaded.size()
        );

        progress.accept("Query benchmark: percentile selection");
        HierarchicalVaREngine.ComputeResult computed =
                engine.computeTimedFromGroups(groups, interestingDims, percentiles);
        percentileNs = computed.percentileMs * 1_000_000L;
        int totalLevelGroups = (int) computed.levels.stream().mapToLong(LevelResult::getGroupCount).sum();
        emitQueryStageDebug(
                datasetId,
                strategyName,
                "percentile",
                percentileNs,
                "levels=" + computed.levels.size() + ", totalLevelGroups=" + totalLevelGroups
        );
        emitQueryDiagnostics(
                datasetId,
                info.recordCount(),
                totalLevelGroups,
                loadNs,
                orderingNs,
                aggregationNs,
                percentileNs
        );

        result.setGenerationMs(0L);
        result.setDbInsertMs(0L);
        result.setDbLoadMs(nanosToMs(loadNs));
        result.setOrderingMs(nanosToMs(orderingNs));
        result.setAggregationMs(nanosToMs(aggregationNs));
        result.setPercentileMs(nanosToMs(percentileNs));
        result.setTotalMs(result.getDbLoadMs() + result.getOrderingMs() + result.getAggregationMs() + result.getPercentileMs());

        validateBenchmarkTotals(result);
        saveBenchmark(result);
        return result;
    }

    // ── Benchmark utilities ────────────────────────────────────────────

    /**
     * Run the computation once on a small subset to trigger JIT compilation
     * of the hot paths before the timed measurement begins.
     */
    private void warmup(AggregationStrategy  strategy,
                        PercentileCalculator calc,
                        List<RiskRecord>     records,
                        List<Integer>        dims,
                        List<Double>         percentiles) {
        int size = Math.max(50, Math.min(records.size() / 10, 500));
        List<RiskRecord> subset = records.subList(0, Math.min(size, records.size()));
        HierarchicalVaREngine engine = new HierarchicalVaREngine(strategy, calc);
        engine.compute(subset, dims, percentiles);
    }

    private AggregationStrategy freshStrategy(String strategyName) {
        if ("Streaming (sort-merge)".equals(strategyName)) {
            return new StreamingAggregationStrategy();
        }
        if ("Prefix rollup (sort-scan)".equals(strategyName)) {
            return new PrefixRollupAggregationStrategy();
        }
        if ("Prefix streaming (DB ordered)".equals(strategyName)) {
            return new PrefixStreamingAggregationStrategy();
        }
        return new BaselineAggregationStrategy();
    }

    private PercentileCalculator freshCalculator(String calculatorName) {
        if ("Floyd-Rivest SELECT".equals(calculatorName)) {
            return new FloydRivestPercentileCalculator();
        }
        if ("OSILA (randomised order-statistic)".equals(calculatorName)) {
            return new OsilaPercentileCalculator();
        }
        return new SortBasedPercentileCalculator();
    }

    private List<RiskRecord> loadRecordsForQueryBenchmark(String datasetId,
                                                          AggregationStrategy strategy,
                                                          List<Integer> dims) throws SQLException {
        if (strategy instanceof StreamingAggregationStrategy
                || strategy instanceof PrefixStreamingAggregationStrategy) {
            return recordRepo.findByDatasetId(datasetId, dims);
        }
        return recordRepo.findByDatasetId(datasetId);
    }

    private OrderedRiskRecordSource orderedDbSource(String datasetId, List<Integer> interestingDims) {
        return consumer -> recordRepo.forEachByDatasetId(datasetId, interestingDims, record -> {
            try {
                consumer.accept(record);
            } catch (Exception e) {
                throw new SQLException("Streaming record consumer failed", e);
            }
        });
    }

    private Map<GroupKey, double[]> aggregateLeafFromDb(String datasetId,
                                                        AggregationStrategy strategy,
                                                        List<Integer> dims) throws SQLException {
        if (strategy instanceof StreamingAggregationStrategy) {
            return aggregateOrderedLeafFromDb(datasetId, dims);
        }
        return aggregateHashLeafFromDb(datasetId, dims);
    }

    private Map<GroupKey, double[]> aggregateHashLeafFromDb(String datasetId,
                                                            List<Integer> dims) throws SQLException {
        Map<GroupKey, double[]> groups = new LinkedHashMap<>();
        recordRepo.forEachByDatasetId(datasetId, List.of(), record -> {
            GroupKey key = extractKey(record, dims);
            double[] existing = groups.get(key);
            if (existing == null) {
                groups.put(key, record.getNumbers().clone());
            } else {
                addInPlace(existing, record.getNumbers());
            }
        });
        return groups;
    }

    private Map<GroupKey, double[]> aggregateOrderedLeafFromDb(String datasetId,
                                                               List<Integer> dims) throws SQLException {
        Map<GroupKey, double[]> result = new LinkedHashMap<>();
        GroupAccumulator acc = new GroupAccumulator();
        recordRepo.forEachByDatasetId(datasetId, dims, record -> {
            GroupKey key = extractKey(record, dims);
            if (!key.equals(acc.currentKey)) {
                if (acc.currentKey != null) {
                    result.put(acc.currentKey, acc.currentSum);
                }
                acc.currentKey = key;
                acc.currentSum = record.getNumbers().clone();
            } else {
                addInPlace(acc.currentSum, record.getNumbers());
            }
        });
        if (acc.currentKey != null) {
            result.put(acc.currentKey, acc.currentSum);
        }
        return result;
    }

    private static GroupKey extractKey(RiskRecord record, List<Integer> indices) {
        List<String> values = new ArrayList<>(indices.size());
        for (int idx : indices) {
            values.add(record.getAttributes().get(idx));
        }
        return new GroupKey(values);
    }

    private static void addInPlace(double[] target, double[] source) {
        int len = Math.min(target.length, source.length);
        for (int i = 0; i < len; i++) {
            target[i] += source[i];
        }
    }

    private static List<List<Integer>> levelDimensions(List<Integer> interesting) {
        List<List<Integer>> levels = new ArrayList<>(interesting.size() + 1);
        for (int i = 0; i <= interesting.size(); i++) {
            levels.add(List.copyOf(interesting.subList(0, interesting.size() - i)));
        }
        return levels;
    }

    private static boolean usesOrderedRead(AggregationStrategy strategy) {
        return strategy instanceof StreamingAggregationStrategy
                || strategy instanceof PrefixStreamingAggregationStrategy;
    }

    private static List<LevelResult> streamingLevels(List<Integer> interesting, int[] groupCounts) {
        List<LevelResult> levels = new ArrayList<>(groupCounts.length);
        for (int i = 0; i < groupCounts.length; i++) {
            List<Integer> dims = List.copyOf(interesting.subList(0, interesting.size() - i));
            levels.add(new LevelResult(dims, null, Map.of(), groupCounts[i]));
        }
        return levels;
    }

    private void emitBenchmarkDiagnostics(String datasetId,
                                          int processedRecords,
                                          List<LevelResult> levels,
                                          long generationNs,
                                          long insertNs,
                                          long loadNs,
                                          long orderingNs,
                                          long aggregationNs,
                                          long percentileNs) {
        long totalGroups = levels.stream().mapToLong(LevelResult::getGroupCount).sum();
        System.out.printf(
                "[BENCH] dataset=%s processedRecords=%d levels=%d groups=%d gen=%.3fms insert=%.3fms load=%.3fms order=%.3fms agg=%.3fms pct=%.3fms%n",
                datasetId,
                processedRecords,
                levels.size(),
                totalGroups,
                generationNs / 1_000_000.0,
                insertNs / 1_000_000.0,
                loadNs / 1_000_000.0,
                orderingNs / 1_000_000.0,
                aggregationNs / 1_000_000.0,
                percentileNs / 1_000_000.0
        );
    }

    private void emitQueryDiagnostics(String datasetId,
                                      int processedRecords,
                                      int totalGroups,
                                      long loadNs,
                                      long orderingNs,
                                      long aggregationNs,
                                      long percentileNs) {
        System.out.printf(
                "[QUERY] dataset=%s processedRecords=%d groups=%d read=%.3fms order=%.3fms agg=%.3fms pct=%.3fms total=%.3fms%n",
                datasetId,
                processedRecords,
                totalGroups,
                loadNs / 1_000_000.0,
                orderingNs / 1_000_000.0,
                aggregationNs / 1_000_000.0,
                percentileNs / 1_000_000.0,
                (loadNs + orderingNs + aggregationNs + percentileNs) / 1_000_000.0
        );
    }

    private void emitQueryStageDebug(String datasetId,
                                     String strategyName,
                                     String stage,
                                     long elapsedNs,
                                     String details) {
        System.out.printf(
                "[QUERY-STAGE] dataset=%s strategy=%s stage=%s elapsed=%.3fms %s%n",
                datasetId,
                strategyName,
                stage,
                elapsedNs / 1_000_000.0,
                details
        );
    }

    private void validateBenchmarkTotals(BenchmarkResult result) {
        long stageSum = result.getGenerationMs()
                + result.getDbInsertMs()
                + result.getDbLoadMs()
                + result.getOrderingMs()
                + result.getAggregationMs()
                + result.getPercentileMs();
        if (result.getTotalMs() != stageSum) {
            System.err.printf(
                    "[WARN] Benchmark total mismatch for %s: total=%dms stageSum=%dms%n",
                    result.getStrategyName(),
                    result.getTotalMs(),
                    stageSum
            );
        }
    }

    private static long nanosToMs(long nanos) {
        return nanos / 1_000_000L;
    }

    private static final class GroupAccumulator {
        private GroupKey currentKey;
        private double[] currentSum;
    }

    // ── Distribution factory ───────────────────────────────────────────

    /** Create a Distribution from its display name (used by the UI). */
    public static Distribution createDistribution(String name, long seed) {
        return switch (name) {
            case "LogNormal"   -> new LogNormalDistribution(seed);
            case "Uniform"     -> new UniformDistribution(seed);
            case "Exponential" -> new ExponentialDistribution(seed);
            case "Mixture"     -> MixtureDistribution.calmsStress(seed);
            default            -> new NormalDistribution(seed); // "Normal"
        };
    }

    // ── Utilities ──────────────────────────────────────────────────────

    private void saveBenchmark(BenchmarkResult r) {
        try { benchmarkRepo.insert(r); }
        catch (SQLException e) { /* non-fatal: just log */ System.err.println("Benchmark save failed: " + e.getMessage()); }
    }

    private static String timestamp() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
    }
}
