variable "project_name" {
  description = "Project name used for tagging"
  type        = string
  default     = "supplychain360"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "eu-central-1"
}

variable "source_bucket" {
  description = "S3 bucket for supply chain data"
  type        = string
  default     = "supplychain360-bucket-t3"
}

variable "statefile_bucket" {
  description = "S3 bucket for Terraform state"
  type        = string
  default     = "supch360-tfstate-file"
}

variable "bucket_path" {
  description = "Base path inside the source bucket"
  type        = string
  default     = "source_data"
}

variable "gsheet" {
  description = "Google Sheets subfolder name"
  type        = string
  default     = "gsheet"
}

variable "s3_source" {
  description = "S3 source subfolder name"
  type        = string
  default     = "s3"
}

variable "postgres" {
  description = "Transactional DB subfolder name"
  type        = string
  default     = "postgres"
}

# Snowflake 
variable "snowflake_organization" {
  description = "Snowflake organization name"
  type        = string
}

variable "snowflake_account" {
  description = "Snowflake account name"
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake username"
  type        = string
  sensitive   = true
}

variable "snowflake_password" {
  description = "Snowflake password"
  type        = string
  sensitive   = true
}

variable "image_name" {
  type = string
  default = "supplychain360/current-image"
}