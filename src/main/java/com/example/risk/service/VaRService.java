package com.example.risk.service;

import com.example.risk.aggregation.AggregationStrategy;
import com.example.risk.aggregation.impl.BaselineAggregationStrategy;
import com.example.risk.aggregation.impl.PrefixRollupAggregationStrategy;
import com.example.risk.aggregation.impl.StreamingAggregationStrategy;
import com.example.risk.aggregation.streaming.DiscardingVaROutputSink;
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
    private final RecordRepository     recordRepo;
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

        long totalStart = System.nanoTime();

        List<RiskRecord> loaded;
        String datasetId = genConfig.getDatasetId();
        if (matchesLoadedDataset(genConfig)) {
            progress.accept("Benchmark: reusing already loaded dataset");
            loaded = loadedRecords;
            result.setGenerationMs(0L);
            result.setDbInsertMs(0L);
            result.setDbLoadMs(0L);
        } else if (recordRepo.countByDatasetId(datasetId) == genConfig.getNumRecords()) {
            progress.accept("Benchmark: loading existing dataset from database");
            var loadT = BenchmarkRunner.time(() -> recordRepo.findByDatasetId(datasetId));
            result.setDbLoadMs(loadT.elapsedMs);
            loaded = loadT.unwrap();
            loadedRecords = loaded;
            loadedDatasetId = datasetId;
            result.setGenerationMs(0L);
            result.setDbInsertMs(0L);
        } else {
            // 1. Generate
            progress.accept("Benchmark: generating records");
            var genT = BenchmarkRunner.time(() -> generator.generate(genConfig));
            result.setGenerationMs(genT.elapsedMs);
            List<RiskRecord> records = genT.unwrap();

            // 2. Insert
            progress.accept("Benchmark: saving records to database");
            recordRepo.deleteByDatasetId(datasetId);
            var insT = BenchmarkRunner.time(() -> recordRepo.insertBatch(records));
            result.setDbInsertMs(insT.elapsedMs);
            insT.unwrap();

            // 3. Load from DB (simulates production: load from kdb)
            progress.accept("Benchmark: loading records from database");
            var loadT = BenchmarkRunner.time(() -> recordRepo.findByDatasetId(datasetId));
            result.setDbLoadMs(loadT.elapsedMs);
            loaded = loadT.unwrap();
            loadedRecords   = loaded;
            loadedDatasetId = datasetId;
        }

        // 4+5. Aggregate + percentile with properly split timings
        AggregationStrategy  strategy = strategies.getOrDefault(strategyName,
                new BaselineAggregationStrategy());
        PercentileCalculator calc     = calculators.getOrDefault(calculatorName, calculator);

        // Warmup on a small in-memory subset so the JIT compiles hot paths before
        // the timed DB-backed measurement begins.
        progress.accept("Benchmark: warming up JIT...");
        warmup(strategy, calc, loaded, interestingDims, percentiles);

        // Release the full dataset before the realistic DB-backed benchmark path.
        loaded = null;
        loadedRecords = new ArrayList<>();
        loadedDatasetId = "";
        System.gc();

        HierarchicalVaREngine engine = new HierarchicalVaREngine(strategy, calc, progress);
        if (strategy instanceof PrefixRollupAggregationStrategy) {
            progress.accept("Benchmark: true streaming prefix aggregation from database");
            StreamingHierarchicalVaREngine streamingEngine =
                    new StreamingHierarchicalVaREngine(calc, progress);
            BenchmarkRunner.TimedResult<StreamingHierarchicalVaREngine.StreamComputeResult> streamT =
                    BenchmarkRunner.time(() -> streamingEngine.processOrderedSource(
                            consumer -> recordRepo.forEachByDatasetId(datasetId, interestingDims, record -> {
                                try {
                                    consumer.accept(record);
                                } catch (Exception e) {
                                    throw new SQLException("Streaming record consumer failed", e);
                                }
                            }),
                            interestingDims,
                            percentiles,
                            new DiscardingVaROutputSink()));
            result.setDbLoadMs(0L);
            StreamingHierarchicalVaREngine.StreamComputeResult computed = streamT.unwrap();
            result.setAggregationMs(computed.aggregationMs);
            result.setPercentileMs(computed.percentileMs);
        } else {
            progress.accept("Benchmark: streaming leaf aggregation from database");
            BenchmarkRunner.TimedResult<Map<GroupKey, double[]>> aggT =
                    BenchmarkRunner.time(() -> aggregateLeafFromDb(datasetId, strategy, interestingDims));
            result.setDbLoadMs(0L);
            var computed = engine.computeTimedFromGroups(aggT.unwrap(), interestingDims, percentiles);
            result.setAggregationMs(aggT.elapsedMs + computed.rollupAggMs);
            result.setPercentileMs(computed.percentileMs);
        }

        result.setTotalMs((System.nanoTime() - totalStart) / 1_000_000L);
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

    private boolean matchesLoadedDataset(GeneratorConfig config) {
        if (!config.getDatasetId().equals(loadedDatasetId) || loadedRecords.isEmpty()) {
            return false;
        }
        if (loadedRecords.size() != config.getNumRecords()) {
            return false;
        }

        RiskRecord first = loadedRecords.get(0);
        return first.getAttributes() != null
                && first.getAttributes().size() == config.getNumAttributes()
                && first.getNumbers() != null
                && first.getNumbers().length == config.getNumbersLength();
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
