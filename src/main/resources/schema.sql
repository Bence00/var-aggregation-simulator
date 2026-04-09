-- Historical VaR System - PostgreSQL schema
-- This static file mirrors the runtime default of schema.max.attributes = 16.

DROP TABLE IF EXISTS risk_records;

CREATE TABLE risk_records (
    id          BIGSERIAL           PRIMARY KEY,
    dataset_id  VARCHAR(255)        NOT NULL,
    attr1       VARCHAR(255),
    attr2       VARCHAR(255),
    attr3       VARCHAR(255),
    attr4       VARCHAR(255),
    attr5       VARCHAR(255),
    attr6       VARCHAR(255),
    attr7       VARCHAR(255),
    attr8       VARCHAR(255),
    attr9       VARCHAR(255),
    attr10      VARCHAR(255),
    attr11      VARCHAR(255),
    attr12      VARCHAR(255),
    attr13      VARCHAR(255),
    attr14      VARCHAR(255),
    attr15      VARCHAR(255),
    attr16      VARCHAR(255),
    numbers     DOUBLE PRECISION[]  NOT NULL,
    created_at  TIMESTAMP           NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_risk_records_dataset
    ON risk_records (dataset_id);

CREATE INDEX IF NOT EXISTS idx_risk_records_prefix
    ON risk_records (
        dataset_id,
        attr1, attr2, attr3, attr4,
        attr5, attr6, attr7, attr8,
        attr9, attr10, attr11, attr12,
        attr13, attr14, attr15, attr16
    );

CREATE TABLE IF NOT EXISTS benchmark_results (
    id              BIGSERIAL       PRIMARY KEY,
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
    created_at      TIMESTAMP       NOT NULL DEFAULT NOW()
);
