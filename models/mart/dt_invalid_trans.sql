{{
  config(
    materialized='table',
    tags=['mart', 'data_quality', 'invalid_transactions']
  )
}}

/*
 * Mart model: Invalid Transactions
 * Original: dt_invalid_trans with TARGET_LAG = 'DOWNSTREAM'
 * 
 * Purpose:
 * - Capture invalid transactions for further analysis
 * - The dt_fraud_full mart will only contain valid transactions
 * - Don't want to completely lose information about invalid transactions
 * 
 * Recommended Schedule: Every 1 minute (DOWNSTREAM - runs after upstream models)
 * 
 * Data Quality Checks:
 * - Filters transactions with validation_status != 'VALID'
 * - Possible validation statuses:
 *   - INVALID_DATE: timestamp conversion failed
 *   - INVALID_AMOUNT: decimal conversion failed
 *   - INVALID_IS_FRAUD: boolean conversion failed
 */

SELECT 
    trans_num,
    validation_status,
    meta_load_ts,
    trans_ts,
    card_num,
    amount,
    merchant,
    category,
    is_fraud

FROM {{ ref('int_transactions_cleaned') }}

WHERE validation_status != 'VALID'
