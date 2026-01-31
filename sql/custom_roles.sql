--- CREATE CUSTOM ROLES AND GRANT NECESSARY PRIVILEGES ---
-- TODO: zautomatyzowac terraformem tworzenie rol i granty

USE ROLE securityadmin;

-- PIPE ADMIN --
-- stage, file formats, pipes, loading to landing table
CREATE ROLE IF NOT EXISTS pipeadmin;

GRANT USAGE ON WAREHOUSE poc_wh TO ROLE pipeadmin;
GRANT USAGE ON WAREHOUSE dynamic_wh TO ROLE pipeadmin;
GRANT USAGE ON DATABASE poc_db TO ROLE pipeadmin;
GRANT USAGE ON SCHEMA poc_db.poc2_schema TO ROLE pipeadmin;
GRANT USAGE ON SCHEMA poc_db.poc2_dynamic TO ROLE pipeadmin;

GRANT USAGE ON ALL STAGES IN SCHEMA poc_db.poc2_schema TO ROLE pipeadmin;     
GRANT USAGE ON FUTURE STAGES IN SCHEMA poc_db.poc2_schema TO ROLE pipeadmin;   
GRANT USAGE ON ALL STAGES IN SCHEMA poc_db.poc2_dynamic TO ROLE pipeadmin;     
GRANT USAGE ON FUTURE STAGES IN SCHEMA poc_db.poc2_dynamic TO ROLE pipeadmin; 

GRANT USAGE ON ALL FILE FORMATS IN SCHEMA poc_db.poc2_schema TO ROLE pipeadmin;  
GRANT USAGE ON FUTURE FILE FORMATS IN SCHEMA poc_db.poc2_schema TO ROLE pipeadmin;
GRANT USAGE ON ALL FILE FORMATS IN SCHEMA poc_db.poc2_dynamic TO ROLE pipeadmin;  
GRANT USAGE ON FUTURE FILE FORMATS IN SCHEMA poc_db.poc2_dynamic TO ROLE pipeadmin;

GRANT INSERT, SELECT ON ALL TABLES IN SCHEMA poc_db.poc2_schema TO ROLE pipeadmin;
GRANT INSERT, SELECT ON FUTURE TABLES IN SCHEMA poc_db.poc2_schema TO ROLE pipeadmin;
GRANT INSERT, SELECT ON ALL TABLES IN SCHEMA poc_db.poc2_dynamic TO ROLE pipeadmin;
GRANT INSERT, SELECT ON FUTURE TABLES IN SCHEMA poc_db.poc2_dynamic TO ROLE pipeadmin;

GRANT CREATE PIPE ON SCHEMA poc_db.poc2_schema TO ROLE pipeadmin;
GRANT CREATE STREAM ON SCHEMA poc_db.poc2_schema TO ROLE pipeadmin;
GRANT CREATE PIPE ON SCHEMA poc_db.poc2_dynamic TO ROLE pipeadmin;

-- to see history and status of pipes
GRANT MONITOR ON FUTURE PIPES IN SCHEMA poc_db.poc2_schema TO ROLE pipeadmin;
GRANT MONITOR ON FUTURE PIPES IN SCHEMA poc_db.poc2_dynamic TO ROLE pipeadmin;
  
GRANT ROLE pipeadmin TO ROLE sysadmin;


-- TASK ADMIN --
-- streams, tasks, conformed table
CREATE ROLE IF NOT EXISTS taskadmin;

GRANT USAGE ON WAREHOUSE poc_wh TO ROLE taskadmin;
GRANT USAGE ON WAREHOUSE dynamic_wh TO ROLE taskadmin;
GRANT USAGE ON DATABASE poc_db TO ROLE taskadmin;
GRANT USAGE ON SCHEMA poc_db.poc2_schema TO ROLE taskadmin;
GRANT USAGE ON SCHEMA poc_db.poc2_dynamic TO ROLE taskadmin;

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA poc_db.poc2_schema TO ROLE taskadmin;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA poc_db.poc2_schema TO ROLE taskadmin;

GRANT SELECT ON ALL TABLES IN SCHEMA poc_db.poc2_dynamic TO ROLE taskadmin;
GRANT SELECT ON FUTURE TABLES IN SCHEMA poc_db.poc2_dynamic TO ROLE taskadmin;
-- operate on dynamic tables pozwala na odswiezanie i pauzowanie
GRANT OPERATE ON ALL DYNAMIC TABLES IN SCHEMA poc_db.poc2_dynamic TO ROLE taskadmin;
GRANT OPERATE ON FUTURE DYNAMIC TABLES IN SCHEMA poc_db.poc2_dynamic TO ROLE taskadmin;

GRANT SELECT ON ALL STREAMS IN SCHEMA poc_db.poc2_schema TO ROLE taskadmin;
GRANT SELECT ON FUTURE STREAMS IN SCHEMA poc_db.poc2_schema TO ROLE taskadmin;

GRANT CREATE TASK ON SCHEMA poc_db.poc2_schema TO ROLE taskadmin;
-- to see history and status of tasks
GRANT MONITOR ON FUTURE TASKS IN SCHEMA poc_db.poc2_schema TO ROLE taskadmin;

GRANT ROLE taskadmin TO ROLE sysadmin;

-- global grant to execute SERVERLESS tasks on account level 
USE ROLE accountadmin;
GRANT EXECUTE TASK, EXECUTE MANAGED TASK ON ACCOUNT TO ROLE taskadmin;


-- ALERT ADMIN --
-- create and monitor alerts
CREATE ROLE IF NOT EXISTS alertadmin;

-- global grants to execute alerts on account level
GRANT EXECUTE ALERT, EXECUTE MANAGED ALERT ON ACCOUNT TO ROLE alertadmin;

USE ROLE securityadmin;
GRANT USAGE ON WAREHOUSE poc_wh TO ROLE alertadmin;
GRANT USAGE ON WAREHOUSE dynamic_wh TO ROLE alertadmin;
GRANT USAGE ON DATABASE poc_db TO ROLE alertadmin;
GRANT USAGE ON SCHEMA poc_db.poc2_schema TO ROLE alertadmin;
GRANT USAGE ON SCHEMA poc_db.poc2_dynamic TO ROLE alertadmin;

GRANT CREATE ALERT ON SCHEMA poc_db.poc2_schema TO ROLE alertadmin;
GRANT CREATE ALERT ON SCHEMA poc_db.poc2_dynamic TO ROLE alertadmin;
-- to see history and status of objects (copy_history, task_history etc) so alerts can query them
GRANT MONITOR ON DATABASE poc_db TO ROLE alertadmin;

GRANT ROLE alertadmin TO ROLE sysadmin;