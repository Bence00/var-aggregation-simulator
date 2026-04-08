package com.example.risk.repository;

import com.example.risk.config.AppConfig;
import com.example.risk.model.RiskRecord;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * JDBC repository for risk_records table.
 *
 * <p>Uses PostgreSQL array types (TEXT[], FLOAT8[]) directly.
 * No ORM; all SQL is explicit so query plans are visible.
 */
public class RecordRepository {

    private final AppConfig config;

    public RecordRepository(AppConfig config) {
        this.config = config;
    }

    // ── Schema ─────────────────────────────────────────────────────────

    /** Create the table if it doesn't exist (idempotent). */
    public void ensureSchema() throws SQLException {
        String sql = """
                CREATE TABLE IF NOT EXISTS risk_records (
                    id          BIGSERIAL           PRIMARY KEY,
                    dataset_id  VARCHAR(255)        NOT NULL,
                    attributes  TEXT[]              NOT NULL,
                    numbers     DOUBLE PRECISION[]  NOT NULL,
                    created_at  TIMESTAMP           NOT NULL DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_risk_records_dataset
                    ON risk_records (dataset_id);
                """;
        try (Connection conn = config.openConnection();
             Statement stmt  = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    // ── Write ──────────────────────────────────────────────────────────

    /**
     * Batch-insert records. Uses a single transaction for performance.
     * @return number of records inserted
     */
    public int insertBatch(List<RiskRecord> records) throws SQLException {
        String sql = "INSERT INTO risk_records (dataset_id, attributes, numbers) VALUES (?,?,?)";
        try (Connection conn = config.openConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            conn.setAutoCommit(false);
            for (RiskRecord r : records) {
                stmt.setString(1, r.getDatasetId());
                stmt.setArray(2, conn.createArrayOf("text",
                        r.getAttributes().toArray(new String[0])));
                stmt.setArray(3, conn.createArrayOf("float8",
                        boxed(r.getNumbers())));
                stmt.addBatch();
            }
            int[] counts = stmt.executeBatch();
            conn.commit();

            int total = 0;
            for (int c : counts) if (c >= 0) total += c;
            return total;
        }
    }

    /** Delete all records for a dataset (used before re-generating). */
    public void deleteByDatasetId(String datasetId) throws SQLException {
        String sql = "DELETE FROM risk_records WHERE dataset_id = ?";
        try (Connection conn = config.openConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, datasetId);
            stmt.executeUpdate();
        }
    }

    // ── Read ───────────────────────────────────────────────────────────

    /** Load all records for a dataset into memory. */
    public List<RiskRecord> findByDatasetId(String datasetId) throws SQLException {
        String sql = "SELECT id, dataset_id, attributes, numbers " +
                     "FROM risk_records WHERE dataset_id = ? ORDER BY id";
        List<RiskRecord> records = new ArrayList<>();
        try (Connection conn = config.openConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, datasetId);
            stmt.setFetchSize(1000); // stream from server in pages

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) records.add(mapRow(rs));
            }
        }
        return records;
    }

    /** Count records for a dataset without loading them. */
    public long countByDatasetId(String datasetId) throws SQLException {
        String sql = "SELECT COUNT(*) FROM risk_records WHERE dataset_id = ?";
        try (Connection conn = config.openConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, datasetId);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0;
            }
        }
    }

    /** List all distinct dataset IDs in the table. */
    public List<String> listDatasetIds() throws SQLException {
        String sql = "SELECT DISTINCT dataset_id FROM risk_records ORDER BY dataset_id";
        List<String> ids = new ArrayList<>();
        try (Connection conn = config.openConnection();
             Statement  stmt = conn.createStatement();
             ResultSet  rs   = stmt.executeQuery(sql)) {
            while (rs.next()) ids.add(rs.getString(1));
        }
        return ids;
    }

    // ── Mapping ────────────────────────────────────────────────────────

    private static RiskRecord mapRow(ResultSet rs) throws SQLException {
        RiskRecord r = new RiskRecord();
        r.setId(rs.getLong("id"));
        r.setDatasetId(rs.getString("dataset_id"));

        String[] attrs = (String[]) rs.getArray("attributes").getArray();
        r.setAttributes(Arrays.asList(attrs));

        Double[] nums = (Double[]) rs.getArray("numbers").getArray();
        r.setNumbers(unboxed(nums));
        return r;
    }

    private static Double[] boxed(double[] arr) {
        Double[] out = new Double[arr.length];
        for (int i = 0; i < arr.length; i++) out[i] = arr[i];
        return out;
    }

    private static double[] unboxed(Double[] arr) {
        double[] out = new double[arr.length];
        for (int i = 0; i < arr.length; i++) out[i] = arr[i];
        return out;
    }
}
