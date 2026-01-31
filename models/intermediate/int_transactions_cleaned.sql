{{
  config(
    materialized='incremental',
    unique_key='trans_num',
    tags=['intermediate', 'transactions', 'cleaning'],
    cluster_by=['trans_ts']
  )
}}

/*
 * Intermediate model: Cleaned and validated transactions
 * Original: dt_transactions with TARGET_LAG = '1 minute'
 * 
 * Business Context:
 * - Transaction data (especially fraud data) is time-sensitive
 * - High cost due to frequent warehouse wake-ups (practically continuous)
 * - Justified by business context - real-time fraud detection
 * 
 * Recommended Schedule: Every 1 minute
 * 
 * Logic:
 * - Type conversion with error handling using TRY_TO_* functions
 * - Data validation to identify invalid records
 * - Preserves metadata for lineage tracking and debugging
 */

SELECT
    trans_num_raw AS trans_num,
    TRY_TO_TIMESTAMP(trans_date_raw, 'YYYY-MM-DD HH24:MI:SS') AS trans_ts,
    card_num_raw AS card_num,
    TRY_TO_DECIMAL(amount_raw, 18, 2) AS amount,
    merchant_raw AS merchant,
    category_raw AS category,
    TRY_TO_BOOLEAN(is_fraud_raw) AS is_fraud,
    meta_load_ts, -- preserving metadata for later analysis and debugging
    
    -- Quick validation to identify data quality issues
    CASE 
        WHEN TRY_TO_TIMESTAMP(trans_date_raw, 'YYYY-MM-DD HH24:MI:SS') IS NULL THEN 'INVALID_DATE'
        WHEN TRY_TO_DECIMAL(amount_raw, 18, 2) IS NULL THEN 'INVALID_AMOUNT'
        WHEN TRY_TO_BOOLEAN(is_fraud_raw) IS NULL THEN 'INVALID_IS_FRAUD'
        ELSE 'VALID'
    END AS validation_status

FROM {{ ref('stg_raw_transactions') }}

{% if is_incremental() %}
    -- Only process new records since last run
    WHERE meta_load_ts > (SELECT MAX(meta_load_ts) FROM {{ this }})
{% endif %}
