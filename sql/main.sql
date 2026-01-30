-- SNOWFLAKE POC#2: integration with S3 via snowpipe and serverless task


--- CONTEXT SETUP ---
USE ROLE sysadmin;
USE WAREHOUSE poc_wh;
USE DATABASE poc_db;
CREATE SCHEMA IF NOT EXISTS poc_db.poc2_schema;
USE SCHEMA poc2_schema;


--- CSV FILE FORMAT ---
CREATE FILE FORMAT IF NOT EXISTS ff_poc_csv
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE
  TRIM_SPACE = TRUE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"';

-- EXTERNAL STAGE --- 
CREATE STAGE IF NOT EXISTS poc2_stage
  URL = 's3://s3-poc2-mb/poc2_transactions/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = ff_poc_csv;

LIST @poc2_stage;


--- LANDING TABLE ---
CREATE TABLE IF NOT EXISTS poc2_landing (
    transaction_id INT,
    trans_date TIMESTAMP,
    category VARCHAR,
    is_fraud BOOLEAN,
    meta_filename VARCHAR,
    meta_file_row_number INT,
    meta_load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CHANGE_TRACKING = TRUE;


--- SNOWPIPE (AUTO INGEST) ---
USE ROLE pipeadmin;
CREATE PIPE IF NOT EXISTS poc2_pipe
  AUTO_INGEST = TRUE
AS
  COPY INTO poc2_landing (
    transaction_id, 
    trans_date, 
    category, 
    is_fraud, 
    meta_filename, 
    meta_file_row_number
  )
  FROM (
    SELECT 
      TRY_TO_NUMBER($1),
      TRY_TO_TIMESTAMP($2),
      TRIM($5), 
      TRY_TO_BOOLEAN($23),
      METADATA$FILENAME,
      METADATA$FILE_ROW_NUMBER
    FROM @poc2_stage
  )
FILE_FORMAT = (FORMAT_NAME = ff_poc_csv);

-- check definition and status
DESC PIPE poc2_pipe;
SELECT SYSTEM$PIPE_STATUS('POC2_PIPE');

-- refresh pipe if files already existed in s3 prior to pipe creation
ALTER PIPE poc2_pipe REFRESH;


--- STREAM ON LANDING TABLE ---
CREATE STREAM IF NOT EXISTS POC2_LANDING_STREAM
  ON TABLE poc2_landing
  APPEND_ONLY = TRUE;

SHOW STREAMS;


--- CONFORMED TABLE ---
USE ROLE sysadmin;
CREATE TABLE IF NOT EXISTS poc2_conformed (
  transaction_id INT,
  trans_date TIMESTAMP,
  category VARCHAR,
  is_fraud BOOLEAN,
  source_filename VARCHAR,
  processed_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

--- SERVERLESS TASK ---
-- to merge stream data into conformed table
-- triggered, runs only when the stream has data
USE ROLE taskadmin;
CREATE TASK IF NOT EXISTS POC2_SERVERLESS_TASK
  TARGET_COMPLETION_INTERVAL = '1 MINUTE'
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  SCHEDULE = '15 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('POC2_LANDING_STREAM')
AS
MERGE INTO poc2_conformed AS target
USING (
  SELECT
    transaction_id,
    trans_date,
    category,
    is_fraud,
    meta_filename
  FROM (
    SELECT
      transaction_id,
      trans_date,
      category,
      is_fraud,
      meta_filename,
      ROW_NUMBER() OVER (
        PARTITION BY transaction_id 
        ORDER BY meta_load_ts DESC, meta_file_row_number DESC
      ) AS rn
    FROM POC2_LANDING_STREAM
  )
    WHERE rn = 1
) AS source
ON target.transaction_id = source.transaction_id
WHEN MATCHED THEN
  UPDATE SET
  target.trans_date = source.trans_date,
  target.category = source.category,
  target.is_fraud = source.is_fraud,
  target.source_filename = source.meta_filename,
  target.processed_ts = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN 
  INSERT (transaction_id, trans_date, category, is_fraud, source_filename, processed_ts)
  VALUES (source.transaction_id, source.trans_date, source.category, 
            source.is_fraud, source.meta_filename, CURRENT_TIMESTAMP());


-- when created, tasks are in SUSPENDED state, resume them to enable monitoring
ALTER TASK POC2_SERVERLESS_TASK RESUME;
//ALTER TASK POC2_SERVERLESS_TASK SUSPEND;
SHOW TASKS;
EXECUTE TASK POC2_SERVERLESS_TASK;