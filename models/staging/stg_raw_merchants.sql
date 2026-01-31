{{
  config(
    materialized='view',
    tags=['staging', 'merchants']
  )
}}

/*
 * Staging model for raw merchant data
 * Source: raw_merch table with CHANGE_TRACKING enabled
 * 
 * This view provides a clean interface to raw merchant data
 * No transformations applied at this stage - raw data passthrough
 */

SELECT
    merchant_raw,
    merchant_lat_raw,
    merchant_long_raw,
    -- meta columns for lineage and debugging
    meta_filename,
    meta_file_row_number,
    meta_load_ts
FROM {{ source('poc2_dynamic', 'raw_merch') }}
