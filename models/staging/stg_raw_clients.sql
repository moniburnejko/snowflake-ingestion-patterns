{{
  config(
    materialized='view',
    tags=['staging', 'clients']
  )
}}

/*
 * Staging model for raw client data
 * Source: raw_clients table with CHANGE_TRACKING enabled
 * 
 * This view provides a clean interface to raw client data
 * No transformations applied at this stage - raw data passthrough
 */

SELECT
    card_num_raw,
    first_name_raw,
    last_name_raw,
    gender_raw,
    date_birth_raw,
    job_raw,
    street_raw,
    city_raw,
    state_raw,
    -- meta columns for lineage and debugging
    meta_filename,
    meta_file_row_number,
    meta_load_ts
FROM {{ source('poc2_dynamic', 'raw_clients') }}
