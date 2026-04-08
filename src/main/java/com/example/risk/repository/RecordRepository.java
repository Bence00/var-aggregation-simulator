package com.example.risk.repository;

import com.example.risk.config.AppConfig;
import com.example.risk.model.RiskRecord;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

/**
 * JDBC repository for risk_records table.
 *
 * <p>Uses PostgreSQL array types (TEXT[], FLOAT8[]) directly.
 * No ORM; all SQL is explicit so query plans are visible.
 */
public class RecordRepository {

    private static final int COPY_CHUNK_SIZE = 500;

    private final AppConfig config;

    public RecordRepository(AppConfig config) {
        this.config = config;
    }

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
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    /**
     * Bulk-insert records using the PostgreSQL COPY protocol.
     *
     * <p>The COPY payload is streamed through a lazy reader, so heap usage stays bounded
     * by the current record text instead of a full chunk-sized String.
     *
     * @return number of records inserted
     */
    public int insertBatch(List<RiskRecord> records) throws SQLException {
        if (records.isEmpty()) {
            return 0;
        }

        String copySql = "COPY risk_records (dataset_id, attributes, numbers) FROM STDIN";

        try (Connection conn = config.openConnection()) {
            CopyManager mgr = new CopyManager(conn.unwrap(BaseConnection.class));
            conn.setAutoCommit(false);

            int total = 0;
            int offset = 0;
            while (offset < records.size()) {
                int end = Math.min(offset + COPY_CHUNK_SIZE, records.size());
                try (Reader reader = new CopyTextReader(records, offset, end)) {
                    total += (int) mgr.copyIn(copySql, reader);
                } catch (IOException e) {
                    throw new SQLException("COPY stream error", e);
                }
                offset = end;
            }

            conn.commit();
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

    /** Load all records for a dataset into memory. */
    public List<RiskRecord> findByDatasetId(String datasetId) throws SQLException {
        String sql = "SELECT id, dataset_id, attributes, numbers " +
                "FROM risk_records WHERE dataset_id = ? ORDER BY id";
        List<RiskRecord> records = new ArrayList<>();
        try (Connection conn = config.openConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, datasetId);
            stmt.setFetchSize(1000);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    records.add(mapRow(rs));
                }
            }
        }
        return records;
    }

    /**
     * Stream records for a dataset through a forward-only JDBC cursor.
     *
     * <p>If {@code orderByAttributeIndices} is non-empty, rows are ordered by those
     * attribute positions using PostgreSQL's 1-based array indexing.
     */
    public void forEachByDatasetId(String datasetId,
                                   List<Integer> orderByAttributeIndices,
                                   RecordConsumer consumer) throws SQLException {
        StringBuilder sql = new StringBuilder(
                "SELECT id, dataset_id, attributes, numbers FROM risk_records WHERE dataset_id = ?"
        );
        if (orderByAttributeIndices != null && !orderByAttributeIndices.isEmpty()) {
            StringJoiner joiner = new StringJoiner(", ");
            for (int idx : orderByAttributeIndices) {
                joiner.add("attributes[" + (idx + 1) + "]");
            }
            sql.append(" ORDER BY ").append(joiner);
        } else {
            sql.append(" ORDER BY id");
        }

        try (Connection conn = config.openConnection();
             PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            conn.setAutoCommit(false);
            stmt.setString(1, datasetId);
            stmt.setFetchSize(1000);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    consumer.accept(mapRow(rs));
                }
            }
        }
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
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                ids.add(rs.getString(1));
            }
        }
        return ids;
    }

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

    private static double[] unboxed(Double[] arr) {
        double[] out = new double[arr.length];
        for (int i = 0; i < arr.length; i++) {
            out[i] = arr[i];
        }
        return out;
    }

    private static String serializeRecord(RiskRecord record) {
        StringBuilder sb = new StringBuilder();
        sb.append(record.getDatasetId()).append('\t');

        sb.append('{');
        List<String> attrs = record.getAttributes();
        for (int i = 0; i < attrs.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append('"');
            appendEscapedText(sb, attrs.get(i));
            sb.append('"');
        }

        sb.append("}\t{");
        double[] nums = record.getNumbers();
        for (int i = 0; i < nums.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(nums[i]);
        }
        sb.append("}\n");
        return sb.toString();
    }

    private static void appendEscapedText(StringBuilder sb, String value) {
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch == '\\' || ch == '"') {
                sb.append('\\');
            }
            sb.append(ch);
        }
    }

    /**
     * Lazy COPY reader that emits one serialized record at a time.
     */
    private static final class CopyTextReader extends Reader {
        private final List<RiskRecord> records;
        private final int to;

        private int index;
        private String current = "";
        private int currentPos;

        private CopyTextReader(List<RiskRecord> records, int from, int to) {
            this.records = records;
            this.index = from;
            this.to = to;
        }

        @Override
        public int read(char[] cbuf, int off, int len) {
            if (len == 0) {
                return 0;
            }

            int written = 0;
            while (written < len) {
                if (currentPos >= current.length()) {
                    if (index >= to) {
                        return written == 0 ? -1 : written;
                    }
                    current = serializeRecord(records.get(index++));
                    currentPos = 0;
                }

                int copyLen = Math.min(len - written, current.length() - currentPos);
                current.getChars(currentPos, currentPos + copyLen, cbuf, off + written);
                currentPos += copyLen;
                written += copyLen;
            }

            return written;
        }

        @Override
        public void close() {
            current = "";
            currentPos = 0;
        }
    }

    @FunctionalInterface
    public interface RecordConsumer {
        void accept(RiskRecord record) throws SQLException;
    }
}
