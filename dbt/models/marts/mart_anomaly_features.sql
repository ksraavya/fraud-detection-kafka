WITH base AS (

    SELECT *
    FROM {{ ref('stg_transactions') }}

),

aggregates AS (

    SELECT *
    FROM {{ ref('int_user_aggregates') }}

)

SELECT

    -- Event & Transaction Identity
    base.event_id,
    base.event_timestamp,
    base.transaction_date,
    base.transaction_hour,
    base.transaction_id,

    -- Parties
    base.customer_id,
    base.merchant_id,

    -- Transaction Detail
    base.transaction_type,
    base.amount,
    base.step,

    -- Balance Snapshot (useful features for tree-based models)
    base.old_balance_orig,
    base.new_balance_orig,
    base.old_balance_dest,
    base.new_balance_dest,

    -- Ground-truth & Anomaly Labels
    base.is_fraud,
    base.synthetic_anomaly,
    base.is_synthetic_anomaly,

    -- Rolling User-level Features (from intermediate)
    aggregates.rolling_avg_amount,
    aggregates.rolling_transaction_count,
    aggregates.merchant_diversity_score,

    -- Engineered Flags
    -- rolling_avg_amount excludes the current row (see int_user_aggregates),
    -- so this comparison is clean: current amount vs prior baseline.
    -- NULL guard: rolling_avg_amount is NULL for a customer's very first
    -- transaction (no preceding rows), so the flag defaults to 0.
    CASE
        WHEN aggregates.rolling_avg_amount IS NOT NULL
             AND base.amount > aggregates.rolling_avg_amount * 3
        THEN 1
        ELSE 0
    END AS statistical_amount_spike_flag

FROM base

LEFT JOIN aggregates
    ON base.transaction_id = aggregates.transaction_id