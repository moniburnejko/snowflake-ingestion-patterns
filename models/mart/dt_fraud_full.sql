{{
  config(
    materialized='table',
    tags=['mart', 'fraud', 'enriched'],
    cluster_by=['trans_ts', 'card_num']
  )
}}

/*
 * Mart model: Enriched Fraud Dataset
 * Original: dt_fraud_full with TARGET_LAG = 'DOWNSTREAM'
 * 
 * Purpose:
 * - Join transactions with clients and merchants
 * - Apply SCD logic and validation filters
 * - Create comprehensive fraud analysis dataset
 * 
 * Recommended Schedule: Every 1 minute (DOWNSTREAM - runs after upstream models)
 * - DOWNSTREAM is the best option for the final table
 * - Table refreshes whenever ANY of the source tables refreshes
 * 
 * Key Features:
 * - Point-in-time joins with SCD Type 2 client data
 * - Enriched with merchant location data
 * - Only includes validated transactions
 * - Preserves lineage metadata (when transaction entered the system)
 * 
 * Point-in-Time Join Logic:
 * - Selects the client record that was valid at the time of transaction
 * - Conditions: t.trans_ts >= c.valid_from AND (t.trans_ts < c.valid_to OR c.valid_to IS NULL)
 * - This enables accurate historical analysis even when client data changes
 */

SELECT
    -- Transaction Data
    t.trans_num,
    t.trans_ts,
    t.card_num,
    t.amount,
    t.category,
    t.is_fraud,
    
    -- Client data (historical state at moment of transaction)
    c.first_name,
    c.last_name,
    c.gender,
    c.job,
    c.city AS client_city,
    c.state AS client_state,
    
    -- Merchant data
    m.merchant,
    m.merchant_lat,
    m.merchant_long,
    
    -- Lineage: when transaction entered the system
    t.meta_load_ts

FROM {{ ref('int_transactions_cleaned') }} t

-- Join with merchants (left join, as we can have transactions with unknown merchants)
LEFT JOIN {{ ref('int_merchants_scd1') }} m 
    ON t.merchant = m.merchant
    
-- Join with clients with point-in-time conditions
LEFT JOIN {{ ref('int_clients_scd2') }} c 
    ON t.card_num = c.card_num
    
    -- Point-in-time conditions
    -- Select the client record that was valid at the time of transaction
    AND t.trans_ts >= c.valid_from 
    AND (t.trans_ts < c.valid_to OR c.valid_to IS NULL)

-- Filter only valid transactions
WHERE t.validation_status = 'VALID'
