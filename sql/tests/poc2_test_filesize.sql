USE ROLE accountadmin;
USE WAREHOUSE poc_wh;
USE DATABASE poc_db;
USE SCHEMA poc2_schema;

-- 3 EXTERNAL STAGES
-- 1 one big file ~137 MB
CREATE OR REPLACE STAGE poc2_stage_1file
  URL = 's3://s3-poc2-mb/poc2_transactions/one_big/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = ff_poc_csv;

-- 10 files ~13.7 MB each
CREATE OR REPLACE STAGE poc2_stage_10files
  URL = 's3://s3-poc2-mb/poc2_transactions/micro_10/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = ff_poc_csv;

-- 50 files ~2.7 MB each
CREATE OR REPLACE STAGE poc2_stage_50files
  URL = 's3://s3-poc2-mb/poc2_transactions/micro_50/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = ff_poc_csv;


LIST @poc2_stage_1file;
LIST @poc2_stage_10files;
LIST @poc2_stage_50files;

-- add column for scenario name
ALTER TABLE poc_db.poc2_schema.poc2_landing 
ADD COLUMN IF NOT EXISTS 
  meta_scenario VARCHAR;


-- 3 PIPES for AUTO INGESTION from each stage
-- 1 file
CREATE PIPE IF NOT EXISTS  poc2_pipe_1file
  AUTO_INGEST = TRUE
AS
  COPY INTO poc_db.poc2_schema.poc2_landing (
    transaction_id, trans_date, category, is_fraud, 
    meta_filename, meta_file_row_number, meta_scenario
  )
  FROM (
    SELECT 
      TRY_TO_NUMBER($1),
      TRY_TO_TIMESTAMP($2),
      TRIM($5), 
      TRY_TO_BOOLEAN($23),
      METADATA$FILENAME,
      METADATA$FILE_ROW_NUMBER,
      'one_big_file' AS meta_scenario
    FROM @poc2_stage_1file
  );


-- 10 files
CREATE PIPE IF NOT EXISTS  poc2_pipe_10files
  AUTO_INGEST = TRUE
AS
  COPY INTO poc_db.poc2_schema.poc2_landing (
    transaction_id, trans_date, category, is_fraud, 
    meta_filename, meta_file_row_number, meta_scenario
  )
  FROM (
    SELECT 
      TRY_TO_NUMBER($1),
      TRY_TO_TIMESTAMP($2),
      TRIM($5), 
      TRY_TO_BOOLEAN($23),
      METADATA$FILENAME,
      METADATA$FILE_ROW_NUMBER,
      '10_files' AS meta_scenario
    FROM @poc2_stage_10files
  );


-- 50 files
CREATE PIPE IF NOT EXISTS poc2_pipe_50files
  AUTO_INGEST = TRUE
AS
  COPY INTO poc_db.poc2_schema.poc2_landing (
    transaction_id, trans_date, category, is_fraud, 
    meta_filename, meta_file_row_number, meta_scenario
  )
  FROM (
    SELECT 
      TRY_TO_NUMBER($1),
      TRY_TO_TIMESTAMP($2),
      TRIM($5), 
      TRY_TO_BOOLEAN($23),
      METADATA$FILENAME,
      METADATA$FILE_ROW_NUMBER,
      '50_files' AS meta_scenario
    FROM @poc2_stage_50files
  );

SHOW PIPES;

-- record test start time
SET test_start = CURRENT_TIMESTAMP();
SELECT $test_start;
//ALTER PIPE poc2_pipe_1file REFRESH;
//ALTER PIPE poc2_pipe_10files REFRESH;
//ALTER PIPE poc2_pipe_50files REFRESH;   


-- load time per file, number of files, and status per scenario
WITH load_per_file_scenario AS (
SELECT
  pipe_name,
  file_name,
  file_size,
  DATEDIFF('second', pipe_received_time, last_load_time) AS load_duration,
  bytes_billed
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME=>'POC2_LANDING',
  START_TIME=> DATEADD(hours, -24, CURRENT_TIMESTAMP())))
  )
SELECT
  pipe_name,
  COUNT(*) AS total_files,
  AVG(load_duration) AS avg_load_duration_seconds,
  SUM(bytes_billed) AS total_bytes_billed 
FROM load_per_file_scenario
WHERE pipe_name IN (
    'POC2_PIPE_1FILE',
    'POC2_PIPE_10FILES',
    'POC2_PIPE_50FILES'
  )
GROUP BY 1
ORDER BY 1;


-- pipe_usage_history: costs per scenario
SELECT 
    PIPE_NAME,
    TO_DATE(start_time) AS date,
    SUM(credits_used) AS credits_used,
    SUM(bytes_billed) AS bytes_billed_total,
    SUM(files_inserted) AS files_inserted
  FROM snowflake.account_usage.pipe_usage_history
  WHERE start_time >= DATEADD(hour,-24,CURRENT_TIMESTAMP())
  AND PIPE_NAME IN (
    'POC2_PIPE_1FILE',
    'POC2_PIPE_10FILES',
    'POC2_PIPE_50FILES'
  )
  GROUP BY 1, 2;