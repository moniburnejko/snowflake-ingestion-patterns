USE ROLE ACCOUNTADMIN;
USE WAREHOUSE POC_WH;
USE DATABASE POC_DB;
USE SCHEMA POC2_SCHEMA;

-- CREATE RAW LANDING TABLE
CREATE OR REPLACE TABLE poc2_raw_landing (
  C1 VARCHAR,
  C2 VARCHAR,
  C5 VARCHAR,
  C23 VARCHAR,
  meta_filename VARCHAR,
  meta_file_row_number INT,
  meta_load_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- CREATE SEPARATE STAGE AND PIPELINE FOR TEST
CREATE OR REPLACE STAGE poc2_stage_dynamic
  STORAGE_INTEGRATION = s3_int
  URL = 's3://s3-poc2-mb/poc2_transactions/'
  FILE_FORMAT = FF_POC_CSV;

CREATE OR REPLACE PIPE poc2_pipe_dynamic
  AUTO_INGEST = TRUE
AS
COPY INTO poc2_raw_landing (C1, C2, C5, C23, meta_filename, meta_file_row_number)
  FROM (
    SELECT 
      $1,
      $2,
      $5,
      $23,
      METADATA$FILENAME,
      METADATA$FILE_ROW_NUMBER
    FROM @poc2_stage_dynamic
  );


SELECT COUNT(*) AS cnt, max(meta_load_ts) AS last_load
FROM poc2_raw_landing;


-- CREATE DYNAMIC TABLE AS 'LANDING TABLE'
// dynamic table musi miec target_lag i warehouse, i definicje jako select
// why choose dynamic table over pipe + landing table?
// - simpler architecture
// - less objects to manage
// - automatic data retention and time travel management
// - built-in data validation and error handling
// - easier to set up and maintain
// - potentially lower costs due to reduced object count
// - better suited for real-time data ingestion scenarios
// - automatic schema evolution handling
// - integrated monitoring and alerting features
// - seamless integration with Snowflake's data ecosystem
// disadvantages:
// - less control over ingestion process
// - limited customization options
// - may not support all data sources or formats
// - potential performance trade-offs for complex transformations
// - may not fit all use cases, especially complex ETL scenarios
// - requires understanding of dynamic table concepts and configurations
CREATE OR REPLACE DYNAMIC TABLE poc2_dynamic_landing
  WAREHOUSE = POC_WH
  TARGET_LAG = '1 MINUTE'
AS
SELECT
    TRY_TO_NUMBER(C1) AS transaction_id,
    TRY_TO_TIMESTAMP(C2) AS trans_date,
    TRIM(C5) AS category,
    TRY_TO_BOOLEAN(C23) AS is_fraud,
    meta_filename,
    meta_file_row_number,
    meta_load_ts
FROM poc2_raw_landing;

-- VERIFY DYNAMIC TABLE
SHOW DYNAMIC TABLES LIKE 'POC2_DYNAMIC_LANDING';
DESCRIBE DYNAMIC TABLE poc2_dynamic_landing;

SELECT COUNT(*) FROM poc2_dynamic_landing;
SELECT * FROM poc2_dynamic_landing LIMIT 10;


-- REFRESH BEHAVIOR TESTING
-- refresh history table functions (shows refreshes from last 7 days)
SELECT
  data_timestamp,
  state,
  refresh_trigger,
  target_lag_sec,
  total_refresh_duration_ms,
  rows_inserted,
  rows_updated,
  rows_deleted,
  query_id
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
  NAME => 'POC_DB.POC2_SCHEMA.POC2_DYNAMIC_LANDING'
))
ORDER BY data_timestamp DESC;

-- table functions dynamic_table shows status and lag metrics
SELECT *
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES(
  NAME => 'POC_DB.POC2_SCHEMA.POC2_DYNAMIC_LANDING'
));


-- account usage (cost raport) - ma duze opoznienia, nawer kilka godzin
SELECT
  data_timestamp,
  database_name,
  schema_name,
  name,
  state,
  refresh_trigger,
  total_refresh_duration_ms,
  rows_inserted,
  rows_updated,
  rows_deleted,
  query_id
FROM snowflake.account_usage.dynamic_table_refresh_history
WHERE data_timestamp >= DATEADD(DAY, -1, CURRENT_TIMESTAMP())
  AND name ILIKE '%POC2_DYNAMIC_LANDING%'
ORDER BY data_timestamp DESC;


-- how compare delays and cost between dynamic table vs stram + task?
-- ingest to landing typed delay
// wrzuc 1 plok, potem sprawdz max timestampy i pierwsze pojawienie sie danych
// np
// SELECT MAX(meta_load_ts) AS raw_max_ts FROM poc2_raw_landing;
// SELECT MAX(meta_load_ts) AS typed_max_ts FROM poc2_landing;
// + total_refresh_duration_ms i data_timestamp z refresh history jako dowod zachowania odswiezen 


-- cost 
-- w dynamic table koszty licza sie przez zuzycie warehouse i cloud services
-- do testu moge uzyc osobnego wh, przpiac do niego dynamic table i mierzyc zuzycie tego wh w oknie testu
-- -> izolacja kosztow

