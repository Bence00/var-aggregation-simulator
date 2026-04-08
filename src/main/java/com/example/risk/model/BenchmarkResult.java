package com.example.risk.model;

/**
 * Timing record stored in the benchmark_results table.
 */
public class BenchmarkResult {

    private Long   id;
    private String runLabel;
    private String datasetId;
    private int    recordCount;
    private int    numbersLength;
    private String strategyName;

    // Stage timings in milliseconds
    private long generationMs;
    private long dbInsertMs;
    private long dbLoadMs;
    private long aggregationMs;
    private long percentileMs;
    private long totalMs;

    // ── Getters / Setters ──────────────────────────────────────────────

    public Long   getId()                    { return id; }
    public void   setId(Long id)             { this.id = id; }

    public String getRunLabel()              { return runLabel; }
    public void   setRunLabel(String v)      { this.runLabel = v; }

    public String getDatasetId()             { return datasetId; }
    public void   setDatasetId(String v)     { this.datasetId = v; }

    public int    getRecordCount()           { return recordCount; }
    public void   setRecordCount(int v)      { this.recordCount = v; }

    public int    getNumbersLength()         { return numbersLength; }
    public void   setNumbersLength(int v)    { this.numbersLength = v; }

    public String getStrategyName()          { return strategyName; }
    public void   setStrategyName(String v)  { this.strategyName = v; }

    public long   getGenerationMs()          { return generationMs; }
    public void   setGenerationMs(long v)    { this.generationMs = v; }

    public long   getDbInsertMs()            { return dbInsertMs; }
    public void   setDbInsertMs(long v)      { this.dbInsertMs = v; }

    public long   getDbLoadMs()              { return dbLoadMs; }
    public void   setDbLoadMs(long v)        { this.dbLoadMs = v; }

    public long   getAggregationMs()         { return aggregationMs; }
    public void   setAggregationMs(long v)   { this.aggregationMs = v; }

    public long   getPercentileMs()          { return percentileMs; }
    public void   setPercentileMs(long v)    { this.percentileMs = v; }

    public long   getTotalMs()               { return totalMs; }
    public void   setTotalMs(long v)         { this.totalMs = v; }

    @Override
    public String toString() {
        return String.format(
            "BenchmarkResult{label=%s, records=%d, n=%d, gen=%dms, insert=%dms, load=%dms, agg=%dms, pct=%dms, total=%dms}",
            runLabel, recordCount, numbersLength,
            generationMs, dbInsertMs, dbLoadMs, aggregationMs, percentileMs, totalMs);
    }
}
