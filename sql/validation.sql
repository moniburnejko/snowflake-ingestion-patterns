USE ROLE accountadmin;
USE DATABASE poc_db;
USE SCHEMA poc_db.poc2_schema;

--- QUICK DATA QUALITY ---

-- landing table check
SELECT COUNT(*) FROM poc2_landing;
SELECT * FROM poc2_landing 
ORDER BY meta_load_ts DESC LIMIT 10;

-- stream check (if task is running, then this query can be empty)
SELECT * FROM poc2_landing_stream LIMIT 10;

-- conformed table check
SELECT COUNT(*) FROM poc2_conformed;
SELECT * FROM poc2_conformed
ORDER BY processed_ts DESC LIMIT 10;


-- STATUS OF TASK AND ALERTS --
-- task history check
SELECT * FROM TABLE(INFORMATION_SCHEMA.SERVERLESS_TASK_HISTORY(
  DATE_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
  TASK_NAME => 'POC2_SERVERLESS_TASK'
))
ORDER BY END_TIME DESC;


-- alert history check
SELECT *
FROM TABLE(INFORMATION_SCHEMA.ALERT_HISTORY(
  SCHEDULED_TIME_RANGE_START=> DATEADD('hour', -24, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;


--- PIPE PERFORMANCE ANALYSIS ---
WITH load_stats AS (
  SELECT
    pipe_name,
    file_name,
    file_size,
    row_count,
    last_load_time,
    DATEDIFF('second', pipe_received_time, last_load_time) AS latency_seconds, 
  FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME=>'POC2_LANDING',
    START_TIME=> DATEADD('hour', -24, CURRENT_TIMESTAMP())
    ))
  )
SELECT
  pipe_name,
  AVG(latency_seconds) AS avg_latency_sec,
  MAX(latency_seconds) AS max_latency_sec,
  SUM(row_count) AS total_rows_loaded,
  COUNT(*) AS files_processed
FROM load_stats
WHERE pipe_name = 'POC2_PIPE'
GROUP BY 1;


--- PIPE COST ANALYSIS ---
// latency: snowflake.account_usage views have a latency of up to 2 hours
// information_schema.pipe_usage_history shows costs almost in real-time
SELECT
  START_TIME,
  END_TIME,
  PIPE_NAME,
  CREDITS_USED,
  BYTES_INSERTED,
  FILES_INSERTED
FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
  DATE_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
  PIPE_NAME => 'POC2_PIPE'
))
ORDER BY START_TIME DESC;


--- SERVERLESS TASK COST ANALYSIS ---
SELECT
  START_TIME,
  END_TIME,
  TASK_NAME,
  CREDITS_USED 
FROM TABLE(INFORMATION_SCHEMA.SERVERLESS_TASK_HISTORY(
  DATE_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
  TASK_NAME => 'POC2_SERVERLESS_TASK'
))
ORDER BY START_TIME DESC;