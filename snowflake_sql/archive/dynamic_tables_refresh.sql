--- WAREHOUSE ---
-- create separate warehouse for dynamic tables for easier cost tracking
USE ROLE accountadmin;

 CREATE WAREHOUSE IF NOT EXISTS dynamic_wh
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

GRANT USAGE ON WAREHOUSE dynamic_wh TO ROLE sysadmin;

--- CONTEXT SETUP ---
USE ROLE sysadmin;
USE WAREHOUSE dynamic_wh;
USE DATABASE POC_DB;
CREATE SCHEMA IF NOT EXISTS POC_DB.POC2_DYNAMIC;
USE SCHEMA POC2_DYNAMIC;
ALTER SESSION SET TIMEZONE = 'Europe/Warsaw';


--- CREATE RAW LANDING TABLES ---
-- transactions
CREATE OR REPLACE TABLE raw_trans (
    trans_num_raw VARCHAR,
    trans_date_raw VARCHAR,
    card_num_raw VARCHAR,
    amount_raw VARCHAR,
    merchant_raw VARCHAR,
    category_raw VARCHAR,
    is_fraud_raw VARCHAR,
    -- meta columns
    meta_filename VARCHAR,
    meta_file_row_number INT,
    meta_load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CHANGE_TRACKING = TRUE;

-- clients
CREATE OR REPLACE TABLE raw_clients (
    card_num_raw VARCHAR,
    first_name_raw VARCHAR,
    last_name_raw VARCHAR,
    gender_raw VARCHAR,
    date_birth_raw VARCHAR,
    job_raw VARCHAR,
    street_raw VARCHAR,
    city_raw VARCHAR,
    state_raw VARCHAR,
    -- meta columns
    meta_filename VARCHAR,
    meta_file_row_number INT,
    meta_load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CHANGE_TRACKING = TRUE;

-- merchants
CREATE OR REPLACE TABLE raw_merch (
    merchant_raw VARCHAR,
    merchant_lat_raw VARCHAR,
    merchant_long_raw VARCHAR,
    -- meta columns
    meta_filename VARCHAR,
    meta_file_row_number INT,
    meta_load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CHANGE_TRACKING = TRUE;

--- CREATE FILE FORMAT ---
CREATE OR REPLACE FILE FORMAT ff_poc_csv
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  NULL_IF = ('', 'NULL', 'null')
  REPLACE_INVALID_CHARACTERS = TRUE;


--- CREATE EXTERAL STAGES: 1 stage per folder in S3 ---
CREATE OR REPLACE STAGE dynamic_stage_trans
  URL = 's3://s3-poc2-mb/poc2_dynamic/fraud_transactions/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = ff_poc_csv;

CREATE OR REPLACE STAGE dynamic_stage_client
  URL = 's3://s3-poc2-mb/poc2_dynamic/fraud_clients/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = ff_poc_csv;

CREATE OR REPLACE STAGE dynamic_stage_merch
  URL = 's3://s3-poc2-mb/poc2_dynamic/fraud_merchant/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = ff_poc_csv;

SHOW STAGES;

LIST @dynamic_stage_trans;
LIST @dynamic_stage_client;
LIST @dynamic_stage_merch;

--- CREATE PIPES: 1 pipe per stage ---
USE ROLE pipeadmin;

-- transactions pipe
CREATE OR REPLACE PIPE pipe_trans
  AUTO_INGEST = TRUE
AS
  COPY INTO raw_trans (
    trans_num_raw, 
    trans_date_raw,
    card_num_raw, 
    amount_raw,
    merchant_raw,
    category_raw, 
    is_fraud_raw, 
    meta_filename, 
    meta_file_row_number
  )
  FROM (
    SELECT 
      $1, $2, $3, $4, $5, $6, $7,
      METADATA$FILENAME,
      METADATA$FILE_ROW_NUMBER
    FROM @dynamic_stage_trans
  );

-- clients pipe
CREATE OR REPLACE PIPE pipe_client
  AUTO_INGEST = TRUE
AS
  COPY INTO raw_clients (
    card_num_raw,
    first_name_raw,
    last_name_raw,
    gender_raw,
    street_raw,
    city_raw,
    state_raw,
    job_raw,
    date_birth_raw,
    meta_filename,
    meta_file_row_number
  )
  FROM (
    SELECT 
      $1, $2, $3, $4, $5, $6, $7, $9, $10,
      METADATA$FILENAME,
      METADATA$FILE_ROW_NUMBER
    FROM @dynamic_stage_client
  );

-- merchants pipe
CREATE OR REPLACE PIPE pipe_merch
  AUTO_INGEST = TRUE
AS
  COPY INTO raw_merch (
    merchant_raw,
    merchant_lat_raw,
    merchant_long_raw,
    meta_filename,
    meta_file_row_number
  )
  FROM (
    SELECT 
      $2, $3, $4,
      METADATA$FILENAME,
      METADATA$FILE_ROW_NUMBER
    FROM @dynamic_stage_merch
  );

SHOW PIPES;
DESC PIPE pipe_trans;
DESC PIPE pipe_client; 
DESC PIPE pipe_merch;

-- refresh pipes
ALTER PIPE pipe_trans REFRESH;
ALTER PIPE pipe_client REFRESH;
ALTER PIPE pipe_merch REFRESH;

SELECT SYSTEM$PIPE_STATUS('PIPE_TRANS');
SELECT SYSTEM$PIPE_STATUS('PIPE_CLIENT');
SELECT SYSTEM$PIPE_STATUS('PIPE_MERCH');

-- quick check data landed
SELECT COUNT(*) FROM raw_trans;
SELECT COUNT(*) FROM raw_clients;
SELECT COUNT(*) FROM raw_merch;
SELECT * FROM raw_trans
ORDER BY meta_load_ts DESC LIMIT 10;
SELECT * FROM raw_clients
ORDER BY meta_load_ts DESC LIMIT 10;
SELECT * FROM raw_merch
ORDER BY meta_load_ts DESC LIMIT 10;




--- DYNAMIC TABLES ---

-- DYNAMIC TABLE STAGE 1: CLEAN-UP AND LOGIC --

-- DYNAMIC TABLE TRANSACTIONS 
-- cleaning types and basic validation
USE ROLE sysadmin;
CREATE OR REPLACE DYNAMIC TABLE dt_transactions
  TARGET_LAG = '1 minute'
  WAREHOUSE = dynamic_wh
AS
SELECT
  trans_num_raw AS trans_num,
  COALESCE(
    TRY_TO_TIMESTAMP(trans_date_raw, 'DD-MM-YYYY HH24:MI:SS'), 
    TRY_TO_TIMESTAMP(trans_date_raw, 'DD-MM-YYYY HH24:MI')
  ) AS trans_ts,
  card_num_raw AS card_num,
  TRY_TO_DECIMAL(amount_raw, 18, 2) AS amount,
  merchant_raw AS merchant,
  category_raw AS category,
  TRY_TO_BOOLEAN(is_fraud_raw) AS is_fraud,
  meta_load_ts,
  -- quick validation
  CASE 
    WHEN COALESCE(
      TRY_TO_TIMESTAMP(trans_date_raw, 'DD-MM-YYYY HH24:MI:SS'),
      TRY_TO_TIMESTAMP(trans_date_raw, 'DD-MM-YYYY HH24:MI')
    ) IS NULL THEN 'INVALID_DATE'
    WHEN TRY_TO_DECIMAL(amount_raw, 18, 2) IS NULL THEN 'INVALID_AMOUNT'
    WHEN TRY_TO_BOOLEAN(is_fraud_raw) IS NULL THEN 'INVALID_IS_FRAUD'
    ELSE 'VALID'
  END AS validation_status
FROM raw_trans;



-- DYNAMIC TABLE MERCHANTS
-- cleaning and SCD TYPE 1 logic
CREATE OR REPLACE DYNAMIC TABLE dt_merchants
  TARGET_LAG = '1 minute'
  WAREHOUSE = dynamic_wh
AS
SELECT
  merchant_raw AS merchant,
  TRY_TO_DECIMAL(merchant_lat_raw, 11, 8) AS merchant_lat,
  TRY_TO_DECIMAL(merchant_long_raw, 11, 8) AS merchant_long,
  meta_load_ts
FROM raw_merch
-- deduplicate based on latest load timestamp and file row number (scd type 1)
QUALIFY ROW_NUMBER() OVER (PARTITION BY merchant_raw ORDER BY meta_load_ts DESC, meta_file_row_number DESC) = 1;



--- DYNAMIC TABLES CLIENTS
-- DT_CLIENT_DEDUPED - deduplication based on load timestamp and file row number
CREATE OR REPLACE DYNAMIC TABLE dt_clients_deduped
  TARGET_LAG = '1 minute'
  WAREHOUSE = dynamic_wh
AS
SELECT
  card_num_raw AS card_num,
  first_name_raw AS first_name,
  last_name_raw AS last_name,
  gender_raw AS gender,
  TRY_TO_DATE(date_birth_raw, 'DD-MM-YYYY') AS date_birth,
  job_raw AS job,
  street_raw AS street,
  city_raw AS city,
  state_raw AS state,
  -- valid_from = time when this record was loaded
  meta_load_ts AS valid_from,
  meta_load_ts -- i left meta_load_ts for lineage
FROM raw_clients
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY card_num_raw, meta_load_ts ORDER BY meta_file_row_number DESC) = 1;

-- DT_CLIENTS_SCD2 - SCD TYPE 2 implementation
CREATE OR REPLACE DYNAMIC TABLE dt_clients_scd2
  TARGET_LAG = 'DOWNSTREAM'
  WAREHOUSE = dynamic_wh
AS
  SELECT
    *,
    -- scd type 2 - valid_to = time when next record for same card_num was loaded
    -- we need deduplication in case of multiple records for same card_num and same valid_from (load timestamp)
    LEAD(valid_from) OVER (
      PARTITION BY card_num ORDER BY valid_from) AS valid_to,
    -- is_current flag for easier filtering of current records
    CASE WHEN LEAD(valid_from) OVER (
      PARTITION BY card_num ORDER BY valid_from) IS NULL THEN TRUE 
      ELSE FALSE 
    END AS is_current
  FROM dt_clients_deduped;



-- DYNAMIC TABLE - INVALID TRANSACTIONS
-- to catch invalid transactions for further analysis
CREATE OR REPLACE DYNAMIC TABLE dt_invalid_trans
  TARGET_LAG = 'DOWNSTREAM'
  WAREHOUSE = dynamic_wh
AS
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
FROM dt_transactions
WHERE validation_status != 'VALID';

-- DYNAMIC TABLES INTERMEDIATE
-- join transactions with merchants
CREATE OR REPLACE DYNAMIC TABLE dt_trans_merch
  TARGET_LAG = 'DOWNSTREAM'
  WAREHOUSE = dynamic_wh
AS
SELECT
  -- transaction data
  t.trans_num,
  t.trans_ts,
  t.card_num,
  t.amount,
  t.category,
  t.is_fraud,
  t.validation_status,

  -- all merchant data
  m.merchant,
  m.merchant_lat,
  m.merchant_long,

  t.meta_load_ts -- lineage: when the transaction was loaded
FROM dt_transactions t
LEFT JOIN dt_merchants m ON t.merchant = m.merchant;


-- DYNAMIC TABLE WITH ALL CLIENT DATA
CREATE OR REPLACE DYNAMIC TABLE dt_trans_all_clients
  TARGET_LAG = 'DOWNSTREAM'
  WAREHOUSE = dynamic_wh
AS
SELECT
  -- transaction data
  tm.trans_num,
  tm.trans_ts,
  tm.card_num,
  tm.amount,
  tm.category,
  tm.is_fraud,
  tm.validation_status,

  -- all merchant data
  tm.merchant,
  tm.merchant_lat,
  tm.merchant_long,

  tm.meta_load_ts, -- lineage: when the transaction was loaded

  -- all client data (no scd logic applied)
  c.first_name,
  c.last_name,
  c.gender,
  c.job,
  c.city AS client_city,
  c.state AS client_state
FROM dt_trans_merch tm
LEFT JOIN dt_clients_scd2 c ON tm.card_num = c.card_num;

--- DYNAMIC TABLE FRAUD FULL
-- final enriched dataset
-- with scd logic applied and quick validation
CREATE OR REPLACE DYNAMIC TABLE dt_fraud_full
-- downstream is the best option here as we depend on multiple upstream tables
  TARGET_LAG = 'DOWNSTREAM'
  WAREHOUSE = dynamic_wh
AS
SELECT *
FROM dt_trans_all_clients
-- filter only valid transactions
WHERE validation_status = 'VALID';
  -- scd type 2 logic:
  -- we chose the client record which was valid at the time of transaction
  AND trans_ts >= valid_from 
  AND (trans_ts < valid_to OR valid_to IS NULL);




-- quick check data landed
SELECT COUNT(*) FROM dt_transactions;
SELECT COUNT(*) FROM dt_merchants;
SELECT COUNT(*) FROM dt_clients_deduped;
SELECT COUNT(*) FROM dt_clients_scd2;
SELECT COUNT(*) FROM dt_invalid_trans;
SELECT COUNT(*) FROM dt_trans_merch;  
SELECT COUNT(*) FROM dt_trans_all_clients;
SELECT COUNT(*) FROM dt_fraud_full;


SELECT * FROM dt_transactions
ORDER BY meta_load_ts DESC LIMIT 10;
SELECT * FROM dt_merchants
ORDER BY meta_load_ts DESC LIMIT 10;
SELECT * FROM dt_clients_deduped
ORDER BY meta_load_ts DESC LIMIT 10;
SELECT * FROM dt_clients_scd2
ORDER BY meta_load_ts DESC LIMIT 10;
SELECT * FROM dt_invalid_trans
ORDER BY meta_load_ts DESC LIMIT 10;
SELECT * FROM dt_trans_merch
ORDER BY meta_load_ts DESC LIMIT 10;
SELECT * FROM dt_trans_all_clients
ORDER BY meta_load_ts DESC LIMIT 10;  
SELECT * FROM dt_fraud_full
ORDER BY meta_load_ts DESC LIMIT 10;  


-- check if tables are incremental or full refresh
USE ROLE accountadmin;
ALTER SESSION SET TIMEZONE = 'Europe/Warsaw';
SHOW DYNAMIC TABLES;

SELECT
    name,
    state,
    refresh_action,
    refresh_start_time,
    refresh_end_time,
    statistics
FROM snowflake.account_usage.dynamic_table_refresh_history
WHERE name LIKE 'DT_%'
  AND database_name = 'POC_DB'
  AND schema_name = 'POC2_DYNAMIC'
ORDER BY refresh_start_time;
