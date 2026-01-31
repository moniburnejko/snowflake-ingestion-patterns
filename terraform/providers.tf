terraform {
  required_providers {
    snowflake = {
      source = "snowflakedb/snowflake"
      version = "2.12.0"
    }
  }
}

# default provider = securityadmin
provider "snowflake" {
    organization_name = var.snowflake_organization
    account_name      = var.snowflake_account
    user              = var.snowflake_user
    token             = var.snowflake_pat
    authenticator     = "PROGRAMMATIC_ACCESS_TOKEN"
    role              = "SECURITYADMIN"
}

provider "snowflake" {
    alias             = "account"
    organization_name = var.snowflake_organization
    account_name      = var.snowflake_account
    user              = var.snowflake_user
    token             = var.snowflake_pat
    authenticator     = "PROGRAMMATIC_ACCESS_TOKEN"
    role              = "ACCOUNTADMIN"
    }

provider "snowflake" {
    alias            = "sys"
    organization_name = var.snowflake_organization
    account_name      = var.snowflake_account
    user              = var.snowflake_user
    token             = var.snowflake_pat
    authenticator     = "PROGRAMMATIC_ACCESS_TOKEN"
    role              = "SYSADMIN"
    }

provider "snowflake" {
    alias            = "security"
    organization_name = var.snowflake_organization
    account_name      = var.snowflake_account
    user              = var.snowflake_user
    token             = var.snowflake_pat
    authenticator     = "PROGRAMMATIC_ACCESS_TOKEN"
    role              = "SECURITYADMIN"
}
