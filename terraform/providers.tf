terraform {
  required_providers {
    
        aws = {
      source = "hashicorp/aws"
      version = "6.30.0"
    }
    snowflake = {
      source = "snowflakedb/snowflake"
      version = "2.12.0"
    }
  }
}

provider "aws" {
  region     = "eu-north-1"
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
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