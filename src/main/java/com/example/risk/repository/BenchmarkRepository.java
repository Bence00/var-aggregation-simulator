package com.example.risk.repository;

import com.example.risk.config.AppConfig;
import com.example.risk.model.BenchmarkResult;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/** Stores and retrieves benchmark timing records. */
public class BenchmarkRepository {

    private final AppConfig config;

    public BenchmarkRepository(AppConfig config) {
        this.config = config;
    }

    public void ensureSchema() throws SQLException {
        String sql = """
                CREATE TABLE IF NOT EXISTS benchmark_results (
                    id              BIGSERIAL    PRIMARY KEY,
                    run_label       VARCHAR(255),
                    dataset_id      VARCHAR(255),
                    record_count    INTEGER,
                    numbers_length  INTEGER,
                    strategy_name   VARCHAR(100),
                    generation_ms   BIGINT,
                    db_insert_ms    BIGINT,
                    db_load_ms      BIGINT,
                    aggregation_ms  BIGINT,
                    percentile_ms   BIGINT,
                    total_ms        BIGINT,
                    created_at      TIMESTAMP    NOT NULL DEFAULT NOW()
                );
                """;
        try (Connection conn = config.openConnection();
             Statement stmt  = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    public void insert(BenchmarkResult r) throws SQLException {
        String sql = """
                INSERT INTO benchmark_results
                (run_label, dataset_id, record_count, numbers_length, strategy_name,
                 generation_ms, db_insert_ms, db_load_ms, aggregation_ms, percentile_ms, total_ms)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """;
        try (Connection conn = config.openConnection();
             PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            stmt.setString(1, r.getRunLabel());
            stmt.setString(2, r.getDatasetId());
            stmt.setInt   (3, r.getRecordCount());
            stmt.setInt   (4, r.getNumbersLength());
            stmt.setString(5, r.getStrategyName());
            stmt.setLong  (6, r.getGenerationMs());
            stmt.setLong  (7, r.getDbInsertMs());
            stmt.setLong  (8, r.getDbLoadMs());
            stmt.setLong  (9, r.getAggregationMs());
            stmt.setLong  (10, r.getPercentileMs());
            stmt.setLong  (11, r.getTotalMs());
            stmt.executeUpdate();
        }
    }

    public List<BenchmarkResult> findRecent(int limit) throws SQLException {
        String sql = "SELECT * FROM benchmark_results ORDER BY created_at DESC LIMIT ?";
        List<BenchmarkResult> list = new ArrayList<>();
        try (Connection conn = config.openConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, limit);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) list.add(mapRow(rs));
            }
        }
        return list;
    }

    private static BenchmarkResult mapRow(ResultSet rs) throws SQLException {
        BenchmarkResult r = new BenchmarkResult();
        r.setId(rs.getLong("id"));
        r.setRunLabel(rs.getString("run_label"));
        r.setDatasetId(rs.getString("dataset_id"));
        r.setRecordCount(rs.getInt("record_count"));
        r.setNumbersLength(rs.getInt("numbers_length"));
        r.setStrategyName(rs.getString("strategy_name"));
        r.setGenerationMs(rs.getLong("generation_ms"));
        r.setDbInsertMs(rs.getLong("db_insert_ms"));
        r.setDbLoadMs(rs.getLong("db_load_ms"));
        r.setAggregationMs(rs.getLong("aggregation_ms"));
        r.setPercentileMs(rs.getLong("percentile_ms"));
        r.setTotalMs(rs.getLong("total_ms"));
        return r;
    }
}
