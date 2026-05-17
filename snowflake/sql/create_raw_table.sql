USE DATABASE FRAUD_DETECTION;
USE SCHEMA RAW;

-- IF NOT EXISTS protects against accidental data wipes on re-runs.
-- Use CREATE OR REPLACE only during intentional schema migrations.
CREATE TABLE IF NOT EXISTS RAW_TRANSACTIONS (

    -- Operational Metadata
    event_id            STRING          NOT NULL,
    event_created_at    TIMESTAMP_NTZ   NOT NULL,   -- UTC wall-clock, no tz stored
    event_timestamp     TIMESTAMP_NTZ   NOT NULL,   -- logical event time (PaySim step)
    pipeline_source     STRING          NOT NULL,
    ingestion_mode      STRING          NOT NULL,

    -- Transaction Core
    transaction_id      STRING          NOT NULL,
    step                INTEGER         NOT NULL,
    transaction_type    STRING          NOT NULL,
    amount              FLOAT           NOT NULL,
    customer_id         STRING          NOT NULL,
    merchant_id         STRING          NOT NULL,

    -- Balance Snapshot
    old_balance_orig    FLOAT           NOT NULL,
    new_balance_orig    FLOAT           NOT NULL,
    old_balance_dest    FLOAT           NOT NULL,
    new_balance_dest    FLOAT           NOT NULL,

    -- Labels
    is_fraud            INTEGER         NOT NULL,   -- 0 | 1

    -- Enrichment / Anomaly Fields
    synthetic_anomaly       STRING,                 -- NULL when no anomaly injected
    is_synthetic_anomaly    INTEGER     NOT NULL,   -- 0 | 1
    transaction_velocity    INTEGER     NOT NULL
);