# RBAC
# TODO: try modules for better reusability!!

# CREATE CUSTOM ROLES 
# create all roles defined in variable custom_role
resource "snowflake_account_role" "roles" {
  for_each = toset(var.custom_role)
  name     = each.value
}

# ROLE HIERARCHY (grant to SYSADMIN)
resource "snowflake_grant_account_role" "sysadmin_grants" {
  provider         = snowflake.sys
  for_each         = toset(var.custom_role)
  role_name        = snowflake_account_role.roles[each.value].name
  parent_role_name = "SYSADMIN"
}

# CONFIG MAPS AND HELPERS
locals {
  # helpers for common grants loops
  warehouse_grants = setproduct(var.warehouses, var.custom_role)
  schema_grants    = setproduct(var.schemas, var.custom_role)

  # SCHEMA CREATION PRIVILEGES (role -> list of privileges)
  schema_creation_map = {
    PIPEADMIN  = ["CREATE PIPE", "CREATE STREAM"]
    TASKADMIN  = ["CREATE TASK"]
    ALERTADMIN = ["CREATE ALERT"]
  }

  # PIPEADMIN OBJECT PRIVILEGES (object type -> list of privileges)
  pipeadmin_objects = {
    STAGES         = ["USAGE"]
    "FILE FORMATS" = ["USAGE"]
    TABLES         = ["INSERT", "SELECT"]
    PIPES          = ["MONITOR", "OPERATE"]
  }

  # ALERTADMIN OBJECT PRIVILEGES
  alertadmin_objects = {
    ALERTS = ["MONITOR", "OPERATE"]
  }

  # TASKADMIN OBJECT PRIVILEGES
  taskadmin_objects = {
    TABLES           = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"]
    STREAMS          = ["SELECT"]
    TASKS            = ["MONITOR"]
    "DYNAMIC TABLES" = ["SELECT", "OPERATE"]
  }
}


# COMMON GRANTS (USAGE)

# WAREHOUSE USAGE
resource "snowflake_grant_privileges_to_account_role" "warehouse_usage" {
  for_each          = { for pair in local.warehouse_grants : "${pair[0]}_${pair[1]}" => { wh = pair[0], role = pair[1] } }
  account_role_name = snowflake_account_role.roles[each.value.role].name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = each.value.wh
  }
}

# DATABASE USAGE
resource "snowflake_grant_privileges_to_account_role" "database_usage" {
  for_each          = toset(var.custom_role)
  account_role_name = snowflake_account_role.roles[each.value].name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = var.database
  }
}

# SCHEMA USAGE
resource "snowflake_grant_privileges_to_account_role" "schema_usage" {
  for_each          = { for pair in local.schema_grants : "${pair[0]}_${pair[1]}" => { schema = pair[0], role = pair[1] } }
  account_role_name = snowflake_account_role.roles[each.value.role].name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = "\"${var.database}\".\"${each.value.schema}\""
  }
}

# SCHEMA CREATION (for all roles defined in map)
resource "snowflake_grant_privileges_to_account_role" "schema_creation" {
  for_each = {
    for pair in setproduct(var.schemas, keys(local.schema_creation_map)) :
    "${pair[0]}_${pair[1]}" => { schema = pair[0], role = pair[1] }
  }
  account_role_name = snowflake_account_role.roles[each.value.role].name
  privileges        = local.schema_creation_map[each.value.role]
  on_schema {
    schema_name = "\"${var.database}\".\"${each.value.schema}\""
  }
}


# PIPEADMIN OBJECT GRANTS

resource "snowflake_grant_privileges_to_account_role" "pipeadmin_objects_future" {
  for_each = {
    for pair in setproduct(var.schemas, keys(local.pipeadmin_objects)) :
    "${pair[0]}_${pair[1]}" => { schema = pair[0], type = pair[1] }
  }
  account_role_name = snowflake_account_role.roles["PIPEADMIN"].name
  privileges        = local.pipeadmin_objects[each.value.type]
  on_schema_object {
    future {
      object_type_plural = each.value.type
      in_schema          = "\"${var.database}\".\"${each.value.schema}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "pipeadmin_objects_all" {
  for_each = {
    for pair in setproduct(var.schemas, keys(local.pipeadmin_objects)) :
    "${pair[0]}_${pair[1]}" => { schema = pair[0], type = pair[1] }
  }
  account_role_name = snowflake_account_role.roles["PIPEADMIN"].name
  privileges        = local.pipeadmin_objects[each.value.type]
  on_schema_object {
    all {
      object_type_plural = each.value.type
      in_schema          = "\"${var.database}\".\"${each.value.schema}\""
    }
  }
}

# ALERTADMIN OBJECT GRANTS (future and all)

resource "snowflake_grant_privileges_to_account_role" "alertadmin_objects_future" {
  for_each = {
    for pair in setproduct(var.schemas, keys(local.alertadmin_objects)) :
    "${pair[0]}_${pair[1]}" => { schema = pair[0], type = pair[1] }
  }
  account_role_name = snowflake_account_role.roles["ALERTADMIN"].name
  privileges        = local.alertadmin_objects[each.value.type]
  on_schema_object {
    future {
      object_type_plural = each.value.type
      in_schema          = "\"${var.database}\".\"${each.value.schema}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "alertadmin_objects_all" {
  for_each = {
    for pair in setproduct(var.schemas, keys(local.alertadmin_objects)) :
    "${pair[0]}_${pair[1]}" => { schema = pair[0], type = pair[1] }
  }
  account_role_name = snowflake_account_role.roles["ALERTADMIN"].name
  privileges        = local.alertadmin_objects[each.value.type]
  on_schema_object {
    all {
      object_type_plural = each.value.type
      in_schema          = "\"${var.database}\".\"${each.value.schema}\""
    }
  }
}


# TASKADMIN GRANTS

resource "snowflake_grant_privileges_to_account_role" "taskadmin_account_grants" {
  account_role_name = snowflake_account_role.roles["TASKADMIN"].name
  privileges        = ["EXECUTE TASK", "EXECUTE MANAGED TASK"]
  on_account        = true
  provider          = snowflake.account
}

# helper resource for taskadmin object grants
# create separate resources for future and all grants

resource "snowflake_grant_privileges_to_account_role" "taskadmin_objects_future" {
  for_each = {
    for pair in setproduct(var.schemas, keys(local.taskadmin_objects)) :
    "${pair[0]}_${pair[1]}" => { schema = pair[0], type = pair[1] }
  }
  account_role_name = snowflake_account_role.roles["TASKADMIN"].name
  privileges        = local.taskadmin_objects[each.value.type]
  on_schema_object {
    future {
      object_type_plural = each.value.type
      in_schema          = "\"${var.database}\".\"${each.value.schema}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "taskadmin_objects_all" {
  for_each = {
    for pair in setproduct(var.schemas, keys(local.taskadmin_objects)) :
    "${pair[0]}_${pair[1]}" => { schema = pair[0], type = pair[1] }
  }
  account_role_name = snowflake_account_role.roles["TASKADMIN"].name
  privileges        = local.taskadmin_objects[each.value.type]
  on_schema_object {
    all {
      object_type_plural = each.value.type
      in_schema          = "\"${var.database}\".\"${each.value.schema}\""
    }
  }
}


# ALERTADMIN GRANTS

resource "snowflake_grant_privileges_to_account_role" "alertadmin_account_grants" {
  account_role_name = snowflake_account_role.roles["ALERTADMIN"].name
  privileges        = ["EXECUTE ALERT", "EXECUTE MANAGED ALERT"]
  on_account        = true
  provider          = snowflake.account
}

# database monitor
resource "snowflake_grant_privileges_to_account_role" "alertadmin_db_monitor" {
  account_role_name = snowflake_account_role.roles["ALERTADMIN"].name
  privileges        = ["MONITOR"]
  on_account_object {
    object_type = "DATABASE"
    object_name = var.database
  }
}