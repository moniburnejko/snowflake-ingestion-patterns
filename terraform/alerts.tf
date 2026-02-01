resource "snowflake_alert" "snowpipe_alert" {
  provider  = snowflake.sys
  name      = "snowpipe_errors_alert"
  warehouse = var.warehouses[0]
  database  = var.database
  schema    = var.schemas[0]
  alert_schedule {
    interval = 15
  }
  condition = <<-EOT
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
  EOT

  action = <<-EOT
    CALL SYSTEM$SEND_EMAIL(
      'poc2_email_int',
      'Email Alert: Snowpipe Errors',
      'Snowpipe POC2_PIPE had load errors. Check COPY_HISTORY for details.'
    )
  EOT
  enabled = false # so it's suspended after creation
}

resource "snowflake_alert" "task_alert" {
  provider = snowflake.sys
  name      = "task_error_alert"
  warehouse = var.warehouses[0]
  database  = var.database
  schema    = var.schemas[0]
  alert_schedule {
    interval = 15
  }
  condition = <<-EOT
    SELECT 1
    FROM TABLE(INFORMATION_SCHEMA.SERVERLESS_TASK_HISTORY(
      START_TIME => SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME(),
      END_TIME => SNOWFLAKE.ALERT.SCHEDULED_TIME(),
      TASK_NAME => 'POC2_SERVERLESS_TASK'
    ))
    WHERE STATE = 'FAILED'
  EOT

  action = <<-EOT
    CALL SYSTEM$SEND_EMAIL(
      'poc2_email_int',
      'Email Alert: Task Failed',
      'Task POC2_SERVERLESS_TASK has failed. Check SERVERLESS_TASK_HISTORY for details.'
    )
  EOT

  enabled = false
}