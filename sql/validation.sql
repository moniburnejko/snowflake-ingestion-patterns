--- VALIDATION QUERIES ---

-- CONTEXT SETUP --
USE ROLE accountadmin;
USE WAREHOUSE poc_wh;
USE DATABASE poc_db;
USE SCHEMA poc2_schema;
ALTER SESSION SET TIMEZONE = 'Europe/Warsaw';


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
  date_range_start => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
  task_name => 'POC2_SERVERLESS_TASK'
));

-- alert history check
SELECT *
FROM TABLE(INFORMATION_SCHEMA.ALERT_HISTORY(
  scheduled_time_range_start=> DATEADD('hour', -24, CURRENT_TIMESTAMP())))
ORDER BY scheduled_time DESC;


--- PIPE PERFORMANCE ANALYSIS ---
WITH load_stats AS (
  SELECT
    status,
    pipe_name,
    file_name,
    file_size,
    row_count,
    last_load_time,
    DATEDIFF('second', pipe_received_time, last_load_time) AS latency_seconds
  FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME=>'POC2_LANDING',
    START_TIME=> DATEADD('hour', -24, CURRENT_TIMESTAMP())
    ))
  )
SELECT
  status,
  pipe_name,
  last_load_time,
  AVG(latency_seconds) AS avg_latency_sec,
  MAX(latency_seconds) AS max_latency_sec,
  SUM(row_count) AS total_rows_loaded,
  COUNT(*) AS files_processed
FROM load_stats
WHERE pipe_name = 'POC2_PIPE'
GROUP BY status, pipe_name, last_load_time
ORDER BY last_load_time DESC;

--- PIPE COST ANALYSIS ---
// latency: snowflake.account_usage views have a latency of up to 2 hours
// information_schema.pipe_usage_history shows costs almost in real-time
SELECT
  start_time,
  end_time,
  pipe_name,
  credits_used,
  bytes_inserted,
  files_inserted
FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
  date_range_start => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
  pipe_name => 'POC2_PIPE'
));

--- SERVERLESS TASK COST ANALYSIS ---
SELECT
  task_name,
  start_time,
  end_time,
  credits_used
FROM TABLE(INFORMATION_SCHEMA.SERVERLESS_TASK_HISTORY(
  date_range_start => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
  task_name => 'POC2_SERVERLESS_TASK'
));