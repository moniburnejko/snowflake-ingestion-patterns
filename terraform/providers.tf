terraform {
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "2.12.0"
    }
  }
}

provider "snowflake" {
    alias             = "account"
    organization_name = var.snowflake_organization
    account_name      = var.snowflake_account
    user              = var.snowflake_username
    password          = var.snowflake_password
    role              = "ACCOUNTADMIN"
    }

provider "snowflake" {
    alias            = "sys"
    organization_name = var.snowflake_organization
    account_name      = var.snowflake_account
    user              = var.snowflake_username
    password          = var.snowflake_password
    role              = "SYSADMIN"
    }

provider "snowflake" {
    alias             = "security"
    organization_name = var.snowflake_organization
    account_name      = var.snowflake_account
    user              = var.snowflake_username
    password          = var.snowflake_password
    role              = "SECURITYADMIN"
    }