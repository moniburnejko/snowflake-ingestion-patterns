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

variable "snowflake_pat" {
  type      = string
  sensitive = true
}

variable "warehouses" {
  type    = list(string)
  default = ["POC_WH", "DYNAMIC_WH"]
}

variable "database" {
  type    = string
  default = "POC_DB"
}

variable "schemas" {
  type    = list(string)
  default = ["POC2_SCHEMA", "POC2_DYNAMIC"]
}

variable "schemas_rw" {
  type    = list(string)
  description = "schemas where TASKADMIN has full privileges (read and write)"
  default = ["POC2_SCHEMA"]
}

variable "schemas_ro" {
  type    = list(string)
  description = "schemas where TASKADMIN has read-only privileges"
  default = ["POC2_DYNAMIC"]
}

variable "custom_role" {
  type    = list(string)
  default = ["PIPEADMIN", "TASKADMIN", "ALERTADMIN"]
}