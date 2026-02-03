variable "aws_access_key" {
  type      = string
  sensitive = true
}

variable "aws_secret_key" {
  type      = string
  sensitive = true
}

variable "snowflake_organization" {
  type = string
}

variable "snowflake_account" {
  type = string
}

variable "snowflake_user" {
  type = string
}

variable "snowflake_private_key" {
  type      = string
  sensitive = true
}

variable "snowflake_private_key_passphrase" {
  type      = string
  sensitive = true
  default   = null
}

variable "warehouses" {
  type    = list(string)
  default = ["POC_WH", "DYNAMIC_WH", "DBT_WH"]
}

variable "database" {
  type    = string
  default = "POC_DB"
}

variable "schemas" {
  type    = list(string)
  default = ["POC2_SCHEMA", "POC2_DYNAMIC", "POC2_DBT"]
}

variable "custom_role" {
  type    = list(string)
  default = ["PIPEADMIN", "TASKADMIN", "ALERTADMIN", "DBTADMIN"]
}
