{{
  config(
    materialized='table',
    tags=['intermediate', 'clients', 'scd_type_2'],
    cluster_by=['card_num', 'valid_from']
  )
}}

/*
 * Intermediate model: Clients with SCD Type 2 logic
 * Original: dt_clients with TARGET_LAG = '5 minutes'
 * 
 * Business Context:
 * - Client data may not change very frequently
 * - Larger lag (several/a dozen minutes) wouldn't be a problem
 * - BUT important to have data about new clients
 * - 5-minute lag is better for new client -> immediate transactions scenario
 * - This is a compromise between cost and data consistency
 * 
 * Recommended Schedule: Every 5 minutes
 * 
 * SCD Type 2 Logic:
 * - Maintains full historical record of changes
 * - Each change creates a new record with valid_from and valid_to timestamps
 * - Uses LEAD window function to set valid_to from next record's valid_from
 * - Current records have valid_to = NULL and is_current = TRUE
 * - Enables point-in-time queries for historical analysis
 * 
 * Implementation Steps:
 * 1. Deduplicate: Remove duplicates within same load batch
 * 2. SCD Type 2: Add temporal columns (valid_from, valid_to, is_current)
 * 3. LEAD function creates continuous timeline where end of one state
 *    is the beginning of the next
 */

WITH deduped_clients AS (
    SELECT
        card_num_raw AS card_num,
        first_name_raw AS first_name,
        last_name_raw AS last_name,
        gender_raw AS gender,
        TRY_TO_DATE(date_birth_raw) AS date_birth,
        job_raw AS job,
        street_raw AS street,
        city_raw AS city,
        state_raw AS state,
        
        -- valid_from is the load time of the record
        meta_load_ts AS valid_from,
        meta_load_ts -- preserve original column name for consistency in control queries
        
    FROM {{ ref('stg_raw_clients') }}
    
    -- Deduplication within same load batch
    -- Select the newest record for each card_num and load time
    -- Filter to keep only that record (qualify row_number = 1)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY card_num_raw, meta_load_ts 
        ORDER BY meta_file_row_number DESC
    ) = 1
),

scd_clients AS (
    SELECT
        *,
        -- LEAD window function gets the valid_from date of the next record
        -- and sets it as the end date (valid_to) for the current record
        -- -> continuous timeline where end of one state is the start of the next
        -- For the most recent record, function returns NULL, meaning it's the current record
        -- This works ONLY if the pair (card_num, valid_from) is unique in the table
        -- That's why we work with the deduped_clients CTE
        LEAD(valid_from) OVER (
            PARTITION BY card_num 
            ORDER BY valid_from
        ) AS valid_to,
        
        -- Flag is_current indicating if record is current or historical
        CASE 
            WHEN LEAD(valid_from) OVER (
                PARTITION BY card_num 
                ORDER BY valid_from
            ) IS NULL THEN TRUE 
            ELSE FALSE 
        END AS is_current

    FROM deduped_clients
)

SELECT * FROM scd_clients
