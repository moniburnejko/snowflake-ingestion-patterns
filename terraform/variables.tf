variable "snowflake_organization" {
  type = string
}

variable "snowflake_account" {
  type = string
}

variable "snowflake_username" {
  type = string
}

variable "snowflake_password" {
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

variable "custom_role" {
  type    = list(string)
  default = ["PIPEADMIN", "TASKADMIN", "ALERTADMIN"]
}