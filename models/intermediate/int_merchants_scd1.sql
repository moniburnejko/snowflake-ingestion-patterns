{{
  config(
    materialized='table',
    tags=['intermediate', 'merchants', 'scd_type_1'],
    cluster_by=['merchant']
  )
}}

/*
 * Intermediate model: Merchants with SCD Type 1 logic
 * Original: dt_merchants with TARGET_LAG = '15 minutes'
 * 
 * Business Context:
 * - Merchant location data is more statistical in nature
 * - Need reasonably fresh data for new merchants
 * - Warehouse wakes up more frequently but merchant table is relatively small
 * - Cost doesn't increase significantly while data consistency improves greatly
 * - TODO: Test and compare costs with TARGET_LAG 15 min vs 1h
 * 
 * Recommended Schedule: Every 15 minutes
 * 
 * SCD Type 1 Logic:
 * - Deduplication: Keep only the most recent record per merchant
 * - Uses QUALIFY with ROW_NUMBER() to select latest record based on:
 *   1. meta_load_ts (primary sort - most recent load time)
 *   2. meta_file_row_number (tiebreaker - last row in file)
 * - Always have current data for each merchant
 * - No historical tracking (only location data, not sensitive to changes)
 * 
 * Materialization Note:
 * - Uses table materialization (not incremental) for simplicity
 * - Full refresh on each run ensures clean SCD Type 1 state
 * - Merchant table is small, so full refresh is efficient
 */

SELECT
    merchant_raw AS merchant,
    TRY_TO_DECIMAL(merchant_lat_raw, 11, 8) AS merchant_lat,
    TRY_TO_DECIMAL(merchant_long_raw, 11, 8) AS merchant_long,
    meta_load_ts

FROM {{ ref('stg_raw_merchants') }}

-- Deduplication - SCD Type 1
-- Select the newest record for each merchant_raw and load time
-- Filter to keep only that record (qualify row_number = 1)
-- This way we always have the latest data for each merchant
-- (Due to the nature of the data (only location), we don't use SCD Type 2)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY merchant_raw 
    ORDER BY meta_load_ts DESC, meta_file_row_number DESC
) = 1
