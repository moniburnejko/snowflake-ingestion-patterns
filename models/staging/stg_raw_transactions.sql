{{
  config(
    materialized='view',
    tags=['staging', 'transactions']
  )
}}

/*
 * Staging model for raw transaction data
 * Source: raw_trans table with CHANGE_TRACKING enabled
 * 
 * This view provides a clean interface to raw transaction data
 * No transformations applied at this stage - raw data passthrough
 */

SELECT
    trans_num_raw,
    trans_date_raw,
    card_num_raw,
    amount_raw,
    merchant_raw,
    category_raw,
    is_fraud_raw,
    -- meta columns for lineage and debugging
    meta_filename,
    meta_file_row_number,
    meta_load_ts
FROM {{ source('poc2_dynamic', 'raw_trans') }}
