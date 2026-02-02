--- ALERTS SETUP ---
-- archive -> now in terraform/alerts.tf

-- context setup for alerts
USE ROLE alertadmin;
USE WAREHOUSE poc_wh;
USE DATABASE poc_db;
USE SCHEMA poc2_schema; 
ALTER SESSION SET TIMEZONE = 'Europe/Warsaw';

-- SNOWPIPE ERROR ALERT --
CREATE ALERT IF NOT EXISTS alert_snowpipe_errors
  WAREHOUSE = poc_wh
  SCHEDULE = '15 MINUTE'
IF (EXISTS (
  SELECT 1
  FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'POC2_LANDING',
    -- check only records since last successful alert run
    START_TIME => SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME()
  ))
  WHERE 
    pipe_name = 'POC2_PIPE' 
    AND error_count > 0
    AND table_name = 'POC2_LANDING'
    AND (last_load_time BETWEEN SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME() 
      AND SNOWFLAKE.ALERT.SCHEDULED_TIME())
)) 
THEN
  CALL SYSTEM$SEND_EMAIL(
    'poc2_email_int',
    'Email Alert: Snowpipe Errors',
    'Snowpipe POC2_PIPE had load errors. Check COPY_HISTORY for details.'
);


-- TASK ERROR ALERT --
CREATE ALERT IF NOT EXISTS alert_task_errors
  WAREHOUSE = poc_wh
  SCHEDULE = '15 MINUTE'
IF (EXISTS (
  SELECT 1
  FROM TABLE(INFORMATION_SCHEMA.SERVERLESS_TASK_HISTORY(
    START_TIME => SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME(),
    END_TIME => SNOWFLAKE.ALERT.SCHEDULED_TIME(),
    TASK_NAME => 'POC2_SERVERLESS_TASK'
  ))
  WHERE STATE = 'FAILED'
))
THEN
  CALL SYSTEM$SEND_EMAIL(
    'poc2_email_int',
    'Email Alert: Task Failed',
    'Task POC2_SERVERLESS_TASK has failed. Check SERVERLESS_TASK_HISTORY for details.'
);

-- when created, alerts are in SUSPENDED state, resume them to enable monitoring
//ALTER ALERT alert_task_errors RESUME;
//ALTER ALERT alert_snowpipe_errors RESUME;
ALTER ALERT alert_task_errors SUSPEND;
ALTER ALERT alert_snowpipe_errors SUSPEND;

-- check created alerts details
SHOW ALERTS;

-- execute alerts manually for testing
//EXECUTE ALERT alert_snowpipe_errors;
//EXECUTE ALERT alert_task_errors;
