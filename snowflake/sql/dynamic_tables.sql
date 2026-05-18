USE DATABASE FRAUD_DETECTION;
USE SCHEMA RAW;

-- Dynamic tables self-refresh automatically based on TARGET_LAG.
-- Snowflake's engine monitors the upstream source and triggers incremental
-- refreshes to keep this table within the specified lag of the base data.

CREATE OR REPLACE DYNAMIC TABLE DYNAMIC_ANOMALY_FEATURES
    TARGET_LAG = '5 minutes'
    WAREHOUSE  = COMPUTE_WH
AS

SELECT

    -- Identity
    event_id,
    transaction_id,
    customer_id,
    merchant_id,

    -- Timing
    event_timestamp,
    transaction_date,
    transaction_hour,

    -- Transaction Detail
    amount,
    transaction_type,

    -- Labels
    is_fraud,
    is_synthetic_anomaly,
    synthetic_anomaly,

    -- Rolling User-level Features
    rolling_avg_amount,
    rolling_transaction_count,
    merchant_diversity_score,

    -- Engineered Flags
    statistical_amount_spike_flag

-- Schema-qualify the source so this works regardless of the session's current schema 
FROM FRAUD_DETECTION.ANALYTICS.MART_ANOMALY_FEATURES;


COMMENT ON DYNAMIC TABLE DYNAMIC_ANOMALY_FEATURES IS
'Near-real-time ML feature store refreshed from mart_anomaly_features. Lag target: 5 minutes. Self-refreshing — no manual Task required.';