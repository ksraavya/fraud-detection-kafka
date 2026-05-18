WITH source AS (

    SELECT *
    FROM {{ source('raw', 'raw_transactions') }}

)

SELECT

    -- Operational Metadata
    event_id,
    event_created_at,
    event_timestamp,
    pipeline_source,
    ingestion_mode,

    -- Transaction Core
    transaction_id,
    customer_id,
    merchant_id,
    transaction_type,
    amount,
    step,

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
    transaction_velocity,

    -- Derived Time Fields
    DATE(event_timestamp)               AS transaction_date,
    EXTRACT(HOUR FROM event_timestamp)  AS transaction_hour

FROM source