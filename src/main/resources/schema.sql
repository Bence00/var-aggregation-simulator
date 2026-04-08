-- Historical VaR System — PostgreSQL schema
-- Run once: psql -U postgres -d risk_var -f schema.sql

CREATE TABLE IF NOT EXISTS risk_records (
    id          BIGSERIAL           PRIMARY KEY,
    dataset_id  VARCHAR(255)        NOT NULL,
    attributes  TEXT[]              NOT NULL,
    numbers     DOUBLE PRECISION[]  NOT NULL,
    created_at  TIMESTAMP           NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_risk_records_dataset ON risk_records (dataset_id);

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
