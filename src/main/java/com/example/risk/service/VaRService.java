package com.example.risk.service;

import com.example.risk.aggregation.AggregationStrategy;
import com.example.risk.aggregation.impl.BaselineAggregationStrategy;
import com.example.risk.aggregation.impl.StreamingAggregationStrategy;
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

    /** Available strategies by display name. */
    private final Map<String, AggregationStrategy> strategies;

    /** Records currently loaded in memory (from last generate or load call). */
    private List<RiskRecord> loadedRecords = new ArrayList<>();
    private String           loadedDatasetId = "";

    public VaRService(AppConfig config) {
        this.config        = config;
        this.recordRepo    = new RecordRepository(config);
        this.benchmarkRepo = new BenchmarkRepository(config);
        this.generator     = new SyntheticDataGenerator();
        this.calculator    = new SortBasedPercentileCalculator();

        strategies = new LinkedHashMap<>();
        strategies.put("Baseline (HashMap)",      new BaselineAggregationStrategy());
        strategies.put("Streaming (sort-merge)",  new StreamingAggregationStrategy());
    }

    // ── Schema ─────────────────────────────────────────────────────────

    public void initDatabase() throws SQLException {
        recordRepo.ensureSchema();
        benchmarkRepo.ensureSchema();
    }

    // ── Queries ────────────────────────────────────────────────────────

    public Set<String>         getStrategyNames()    { return strategies.keySet(); }
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

        long total = System.currentTimeMillis();

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

        result.setTotalMs(System.currentTimeMillis() - total);
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
                                    List<Integer>  interestingDims,
                                    List<Double>   percentiles) {
        return runVaR(strategyName, interestingDims, percentiles, ignored -> {});
    }

    public List<LevelResult> runVaR(String         strategyName,
                                    List<Integer>  interestingDims,
                                    List<Double>   percentiles,
                                    Consumer<String> progress) {
        AggregationStrategy strategy = strategies.getOrDefault(strategyName,
                new BaselineAggregationStrategy());
        HierarchicalVaREngine engine = new HierarchicalVaREngine(strategy, calculator, progress);
        return engine.compute(loadedRecords, interestingDims, percentiles);
    }

    // ── Full benchmark ─────────────────────────────────────────────────

    /**
     * Generate → save → load → aggregate → compute percentiles.
     * Records all stage timings and saves them to the DB.
     */
    public BenchmarkResult runFullBenchmark(GeneratorConfig genConfig,
                                            String          strategyName,
                                            List<Integer>   interestingDims,
                                            List<Double>    percentiles) throws SQLException {
        return runFullBenchmark(genConfig, strategyName, interestingDims, percentiles, ignored -> {});
    }

    public BenchmarkResult runFullBenchmark(GeneratorConfig genConfig,
                                            String          strategyName,
                                            List<Integer>   interestingDims,
                                            List<Double>    percentiles,
                                            Consumer<String> progress) throws SQLException {
        BenchmarkResult result = new BenchmarkResult();
        result.setRunLabel(timestamp() + " benchmark");
        result.setDatasetId(genConfig.getDatasetId());
        result.setRecordCount(genConfig.getNumRecords());
        result.setNumbersLength(genConfig.getNumbersLength());
        result.setStrategyName(strategyName);

        long total = System.currentTimeMillis();

        // 1. Generate
        progress.accept("Benchmark: generating records");
        var genT = BenchmarkRunner.time(() -> generator.generate(genConfig));
        result.setGenerationMs(genT.elapsedMs);
        List<RiskRecord> records = genT.unwrap();

        // 2. Insert
        progress.accept("Benchmark: saving records to database");
        recordRepo.deleteByDatasetId(genConfig.getDatasetId());
        var insT = BenchmarkRunner.time(() -> recordRepo.insertBatch(records));
        result.setDbInsertMs(insT.elapsedMs);
        insT.unwrap();

        // 3. Load from DB (simulates production: load from kdb)
        progress.accept("Benchmark: loading records from database");
        var loadT = BenchmarkRunner.time(() -> recordRepo.findByDatasetId(genConfig.getDatasetId()));
        result.setDbLoadMs(loadT.elapsedMs);
        List<RiskRecord> loaded = loadT.unwrap();
        loadedRecords   = loaded;
        loadedDatasetId = genConfig.getDatasetId();

        // 4+5. Aggregate + percentile (interleaved inside engine, split here for timing)
        AggregationStrategy strategy = strategies.getOrDefault(strategyName,
                new BaselineAggregationStrategy());

        progress.accept(String.format("Aggregating leaf level 1/%d: dims %s",
                interestingDims.size() + 1, interestingDims));
        var aggT = BenchmarkRunner.time(() -> strategy.aggregate(loaded, interestingDims));
        result.setAggregationMs(aggT.elapsedMs);
        Map<GroupKey, double[]> leafGroups = aggT.unwrap();

        // Full hierarchical including percentile extraction
        long pctStart = System.currentTimeMillis();
        HierarchicalVaREngine engine = new HierarchicalVaREngine(strategy, calculator, progress);
        engine.computeFromGroups(leafGroups, interestingDims, percentiles);
        result.setPercentileMs(System.currentTimeMillis() - pctStart);

        result.setTotalMs(System.currentTimeMillis() - total);
        saveBenchmark(result);
        return result;
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
