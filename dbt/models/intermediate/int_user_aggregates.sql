WITH base AS (

    SELECT
        customer_id,
        event_timestamp,
        transaction_id,
        merchant_id,
        amount
    FROM {{ ref('stg_transactions') }}

),

user_metrics AS (

    SELECT

        customer_id,
        event_timestamp,
        transaction_id,

        -- Rolling average over the 10 preceding transactions, deliberately
        -- excluding the current row so the flag in the mart compares this
        -- transaction's amount against a baseline that it didn't influence.
        AVG(amount)
            OVER (
                PARTITION BY customer_id
                ORDER BY event_timestamp
                ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
            ) AS rolling_avg_amount,

        -- Count of transactions in the same 10-transaction lookback window
        COUNT(transaction_id)
            OVER (
                PARTITION BY customer_id
                ORDER BY event_timestamp
                ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
            ) AS rolling_transaction_count,

        -- Number of distinct merchants seen in the last 20 transactions;
        -- high diversity can signal account takeover or card testing behaviour
        COUNT(DISTINCT merchant_id)
            OVER (
                PARTITION BY customer_id
                ORDER BY event_timestamp
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS merchant_diversity_score

    FROM base

)

SELECT
    customer_id,
    event_timestamp,
    transaction_id,
    rolling_avg_amount,
    rolling_transaction_count,
    merchant_diversity_score
FROM user_metrics