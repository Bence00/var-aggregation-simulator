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
 * JDBC repository for {@code risk_records}.
 *
 * <p>Attributes are stored as separate SQL columns ({@code attr1 .. attrN}) so ordered
 * cursor reads can use plain {@code ORDER BY attrX, attrY, ...}. The Java model still
 * reconstructs those columns as {@code List<String>} for the aggregation pipeline.
 */
public class RecordRepository {

    private static final int COPY_CHUNK_SIZE = 500;

    private final AppConfig config;
    private final int maxAttributes;

    public RecordRepository(AppConfig config) {
        this.config = config;
        this.maxAttributes = config.getSchemaMaxAttributes();
    }

    /** Create the table if it doesn't exist (non-destructive for the configured max attribute count). */
    public void ensureSchema() throws SQLException {
        try (Connection conn = config.openConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(schemaSql());
        }
    }

    /**
     * Bulk-insert records using PostgreSQL COPY.
     *
     * <p>The payload is streamed lazily so heap usage is bounded by the current row text.
     */
    public int insertBatch(List<RiskRecord> records) throws SQLException {
        if (records.isEmpty()) {
            return 0;
        }

        String copySql = "COPY risk_records (" + insertColumnList() + ") FROM STDIN";

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
        return findByDatasetId(datasetId, List.of());
    }

    /** Load all records for a dataset into memory with optional attribute ordering. */
    public List<RiskRecord> findByDatasetId(String datasetId,
                                            List<Integer> orderByAttributeIndices) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT id, ").append(selectColumnList())
                .append(" FROM risk_records WHERE dataset_id = ?");
        if (orderByAttributeIndices != null && !orderByAttributeIndices.isEmpty()) {
            StringJoiner joiner = new StringJoiner(", ");
            for (int idx : orderByAttributeIndices) {
                joiner.add(attributeColumn(idx));
            }
            sql.append(" ORDER BY ").append(joiner);
        } else {
            sql.append(" ORDER BY id");
        }

        List<RiskRecord> records = new ArrayList<>();
        try (Connection conn = config.openConnection();
             PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
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

    /** Load only a small sample of records for warmup or inspection. */
    public List<RiskRecord> findSampleByDatasetId(String datasetId, int limit) throws SQLException {
        String sql = "SELECT id, " + selectColumnList() +
                " FROM risk_records WHERE dataset_id = ? ORDER BY id LIMIT ?";
        List<RiskRecord> records = new ArrayList<>();
        try (Connection conn = config.openConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, datasetId);
            stmt.setInt(2, limit);
            stmt.setFetchSize(Math.max(1, limit));

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
     * attribute columns directly ({@code attr1}, {@code attr2}, ...).
     */
    public void forEachByDatasetId(String datasetId,
                                   List<Integer> orderByAttributeIndices,
                                   RecordConsumer consumer) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT id, ").append(selectColumnList())
                .append(" FROM risk_records WHERE dataset_id = ?");
        if (orderByAttributeIndices != null && !orderByAttributeIndices.isEmpty()) {
            StringJoiner joiner = new StringJoiner(", ");
            for (int idx : orderByAttributeIndices) {
                joiner.add(attributeColumn(idx));
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

    public DatasetInfo getDatasetInfo(String datasetId) throws SQLException {
        String sql = """
                SELECT COUNT(*) AS record_count,
                       COALESCE(MAX(array_length(numbers, 1)), 0) AS numbers_length
                FROM risk_records
                WHERE dataset_id = ?
                """;
        try (Connection conn = config.openConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, datasetId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return new DatasetInfo(0, 0);
                }
                return new DatasetInfo(rs.getInt("record_count"), rs.getInt("numbers_length"));
            }
        }
    }

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

    private RiskRecord mapRow(ResultSet rs) throws SQLException {
        RiskRecord r = new RiskRecord();
        r.setId(rs.getLong("id"));
        r.setDatasetId(rs.getString("dataset_id"));

        List<String> attrs = new ArrayList<>(maxAttributes);
        for (int i = 0; i < maxAttributes; i++) {
            String value = rs.getString(attributeColumn(i));
            if (value == null) {
                break;
            }
            attrs.add(value);
        }
        r.setAttributes(attrs);

        Double[] nums = (Double[]) rs.getArray("numbers").getArray();
        r.setNumbers(unboxed(nums));
        return r;
    }

    private String schemaSql() {
        StringBuilder sql = new StringBuilder("""
                CREATE TABLE IF NOT EXISTS risk_records (
                    id          BIGSERIAL           PRIMARY KEY,
                    dataset_id  VARCHAR(255)        NOT NULL,
                    numbers     DOUBLE PRECISION[]  NOT NULL,
                    created_at  TIMESTAMP           NOT NULL DEFAULT NOW()
                );
                """);
        for (int i = 0; i < maxAttributes; i++) {
            sql.append("ALTER TABLE risk_records ADD COLUMN IF NOT EXISTS ")
                    .append(attributeColumn(i))
                    .append(" VARCHAR(255);\n");
        }
        sql.append("""
                CREATE INDEX IF NOT EXISTS idx_risk_records_dataset
                    ON risk_records (dataset_id);
                CREATE INDEX IF NOT EXISTS idx_risk_records_prefix
                    ON risk_records (
                        dataset_id
                """);
        for (int i = 0; i < maxAttributes; i++) {
            sql.append(", ").append(attributeColumn(i));
        }
        sql.append("\n    );");
        return sql.toString();
    }

    private String insertColumnList() {
        StringJoiner joiner = new StringJoiner(", ");
        joiner.add("dataset_id");
        for (int i = 0; i < maxAttributes; i++) {
            joiner.add(attributeColumn(i));
        }
        joiner.add("numbers");
        return joiner.toString();
    }

    private String selectColumnList() {
        StringJoiner joiner = new StringJoiner(", ");
        joiner.add("dataset_id");
        for (int i = 0; i < maxAttributes; i++) {
            joiner.add(attributeColumn(i));
        }
        joiner.add("numbers");
        return joiner.toString();
    }

    private static String attributeColumn(int zeroBasedIndex) {
        return "attr" + (zeroBasedIndex + 1);
    }

    private static double[] unboxed(Double[] arr) {
        double[] out = new double[arr.length];
        for (int i = 0; i < arr.length; i++) {
            out[i] = arr[i];
        }
        return out;
    }

    private String serializeRecord(RiskRecord record) {
        validateAttributeCount(record);

        StringBuilder sb = new StringBuilder();
        appendCopyValue(sb, record.getDatasetId());
        List<String> attrs = record.getAttributes();
        for (int i = 0; i < maxAttributes; i++) {
            sb.append('\t');
            if (i < attrs.size()) {
                appendCopyValue(sb, attrs.get(i));
            } else {
                sb.append("\\N");
            }
        }

        sb.append('\t').append('{');
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

    private void validateAttributeCount(RiskRecord record) {
        if (record.getAttributes().size() > maxAttributes) {
            throw new IllegalArgumentException(
                    "Record has " + record.getAttributes().size()
                            + " attributes but schema supports at most " + maxAttributes
            );
        }
    }

    private static void appendCopyValue(StringBuilder sb, String value) {
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                default:
                    sb.append(ch);
            }
        }
    }

    private final class CopyTextReader extends Reader {
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

    public static final class DatasetInfo {
        private final int recordCount;
        private final int numbersLength;

        public DatasetInfo(int recordCount, int numbersLength) {
            this.recordCount = recordCount;
            this.numbersLength = numbersLength;
        }

        public int recordCount() {
            return recordCount;
        }

        public int numbersLength() {
            return numbersLength;
        }
    }
}
