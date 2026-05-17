USE DATABASE FRAUD_DETECTION;
USE SCHEMA RAW;

COPY INTO RAW_TRANSACTIONS (
    -- Operational Metadata
    event_id,
    event_created_at,
    event_timestamp,
    pipeline_source,
    ingestion_mode,

    -- Transaction Core
    transaction_id,
    step,
    transaction_type,
    amount,
    customer_id,
    merchant_id,

    -- Balance Snapshot
    old_balance_orig,
    new_balance_orig,
    old_balance_dest,
    new_balance_dest,

    -- Labels
    is_fraud,

    -- Enrichment / Anomaly Fields
    synthetic_anomaly,
    is_synthetic_anomaly,
    transaction_velocity
)
FROM (
    SELECT
        $1:event_id::STRING,
        $1:event_created_at::TIMESTAMP_NTZ,
        $1:event_timestamp::TIMESTAMP_NTZ,
        $1:pipeline_source::STRING,
        $1:ingestion_mode::STRING,

        $1:transaction_id::STRING,
        $1:step::INTEGER,
        $1:transaction_type::STRING,
        $1:amount::FLOAT,
        $1:customer_id::STRING,
        $1:merchant_id::STRING,

        $1:old_balance_orig::FLOAT,
        $1:new_balance_orig::FLOAT,
        $1:old_balance_dest::FLOAT,
        $1:new_balance_dest::FLOAT,

        $1:is_fraud::INTEGER,

        $1:synthetic_anomaly::STRING,
        $1:is_synthetic_anomaly::INTEGER,
        $1:transaction_velocity::INTEGER

    FROM @TRANSACTION_STAGE
)
FILE_FORMAT = (
    TYPE = JSON
    STRIP_OUTER_ARRAY = FALSE   -- NDJSON: one JSON object per line, no wrapping array
);