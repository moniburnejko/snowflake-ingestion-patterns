-- SNOWFLAKE: integration with S3 via snowpipe and serverless task

--- CONTEXT SETUP ---
USE ROLE sysadmin;
USE WAREHOUSE poc_wh;
USE DATABASE poc_db;
CREATE SCHEMA IF NOT EXISTS poc_db.poc2_schema;
USE SCHEMA poc2_schema;
ALTER SESSION SET TIMEZONE = 'Europe/Warsaw';


--- CSV FILE FORMAT ---
CREATE FILE FORMAT IF NOT EXISTS ff_poc2_csv
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  NULL_IF = ('', 'NULL', 'null')
  REPLACE_INVALID_CHARACTERS = TRUE;

-- EXTERNAL STAGE --- 
CREATE STAGE IF NOT EXISTS poc2_stage
  URL = 's3://s3-poc2-mb/poc2_transactions/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = ff_poc2_csv;

LIST @poc2_stage;


--- LANDING TABLE ---
CREATE TABLE IF NOT EXISTS poc2_landing (
    trans_id VARCHAR,
    trans_ts TIMESTAMP,
    category VARCHAR,
    is_fraud BOOLEAN,
    meta_filename VARCHAR,
    meta_file_row_number INT,
    meta_load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
-- enable change tracking for stream creation later
-- landing table was created by sysadmin, but streams will be created by pipeadmin
CHANGE_TRACKING = TRUE;


--- SNOWPIPE (AUTO INGEST) ---
USE ROLE pipeadmin;
CREATE or replace pipe poc2_pipe
  AUTO_INGEST = TRUE
AS
  COPY INTO poc2_landing (
    trans_id, 
    trans_ts, 
    category, 
    is_fraud, 
    meta_filename, 
    meta_file_row_number
  )
  FROM (
    SELECT 
      $19,
      TRY_TO_TIMESTAMP($2, 'DD-MM-YYYY HH24:MI'),
      $5, 
      TRY_TO_BOOLEAN($23),
      METADATA$FILENAME,
      METADATA$FILE_ROW_NUMBER
    FROM @poc2_stage
  );

-- check definition and status
SHOW PIPES;
DESC PIPE poc2_pipe;
SELECT SYSTEM$PIPE_STATUS('POC2_PIPE');

-- refresh pipe if files already existed in s3 prior to pipe creation
ALTER PIPE poc2_pipe REFRESH;


--- STREAM ON LANDING TABLE ---
CREATE STREAM IF NOT EXISTS poc2_landing_stream
  ON TABLE poc2_landing
  APPEND_ONLY = TRUE;

SHOW STREAMS;


--- CONFORMED TABLE ---
USE ROLE sysadmin;
CREATE TABLE IF NOT EXISTS poc2_conformed (
  trans_id VARCHAR,
  trans_ts TIMESTAMP,
  category VARCHAR,
  is_fraud BOOLEAN,
  source_filename VARCHAR,
  processed_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

--- SERVERLESS TASK ---
-- to merge stream data into conformed table
-- scheduled to check every 15 mins, but executes only when stream has data (saves costs)
USE ROLE taskadmin;
CREATE TASK IF NOT EXISTS poc2_serverless_task
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  SCHEDULE = '15 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('POC2_LANDING_STREAM')
AS
MERGE INTO poc2_conformed AS target
USING (
  SELECT
    trans_id,
    trans_ts,
    category,
    is_fraud,
    meta_filename
  FROM poc2_landing_stream
  -- deduplikacja
  -- najpierw szukamy najnowszych rekordow wg timestampu ladowania i numeru wiersza w pliku
  -- potem filtrujemy tylko ten rekord (qualify row_number = 1)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY trans_id 
    ORDER BY meta_load_ts DESC, meta_file_row_number DESC
  ) = 1
) AS source
ON target.trans_id = source.trans_id
-- optymalizacja - aktualizuj tylko jesli dane faktycznie sie zmienily
WHEN MATCHED AND (
  target.trans_ts IS DISTINCT FROM source.trans_ts OR
  target.category IS DISTINCT FROM source.category OR
  target.is_fraud IS DISTINCT FROM source.is_fraud OR
  target.source_filename IS DISTINCT FROM source.meta_filename
) THEN
  UPDATE SET
  target.trans_ts = source.trans_ts,
  target.category = source.category,
  target.is_fraud = source.is_fraud,
  target.source_filename = source.meta_filename,
  target.processed_ts = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN 
  INSERT (trans_id, trans_ts, category, is_fraud, source_filename, processed_ts)
  VALUES (source.trans_id, source.trans_ts, source.category, 
            source.is_fraud, source.meta_filename, CURRENT_TIMESTAMP());


-- when created, tasks are in SUSPENDED state, resume them to enable monitoring
ALTER TASK poc2_serverless_task RESUME;
ALTER TASK poc2_serverless_task SUSPEND;
SHOW TASKS;
EXECUTE TASK poc2_serverless_task;