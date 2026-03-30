resource "snowflake_storage_integration_aws" "s3_integration" {
  name                      = "supplychain360_aws_storage_integration"
  enabled                   = true
  storage_provider          = "S3"
  storage_allowed_locations = ["s3://${var.source_bucket}/${var.bucket_path}/", "s3://${var.source_bucket}/${var.bucket_path}/${var.gsheet}/*", "s3://${var.source_bucket}/${var.bucket_path}/${var.s3_source}/*", "s3://${var.source_bucket}/${var.bucket_path}/${var.postgres}/*"]
  storage_blocked_locations = ["s3://${var.statefile_bucket}/${var.environment}/"]
  use_privatelink_endpoint  = false
  comment                   = "Storage integration for supplychain360"

  storage_aws_role_arn = aws_iam_role.snowflake_role.arn
}

# resource "snowflake_storage_integration" "s3_integration" {
#   name                      = "SUPPLYCHAIN360_AWS_STORAGE_INTEGRATION"
#   type                      = "EXTERNAL_STAGE"
#   enabled                   = true
#   storage_provider          = "S3"
#   storage_allowed_locations = ["s3://${var.source_bucket}/${var.bucket_path}/${var.gsheet}/*", "s3://${var.source_bucket}/${var.bucket_path}/${var.s3_source}/*", "s3://${var.source_bucket}/${var.bucket_path}/${var.postgres}/*"]
#   storage_blocked_locations = ["s3://${var.statefile_bucket}/${var.environment}/"]
#   comment                   = "Storage integration for supplychain360"

#   storage_aws_role_arn = aws_iam_role.snowflake_role.arn
# }



# Resource with AWS IAM role credentials
resource "snowflake_stage_external_s3" "complete" {
  name                = "complete_s3_stage"
  database            = snowflake_database.supplychain360_db.name
  schema              = snowflake_schema.schema.name
  url                 = "s3://${var.source_bucket}/${var.bucket_path}/"
  storage_integration = snowflake_storage_integration_aws.s3_integration.name
}

resource "snowflake_file_format" "parquet" {
  name        = "PARQUET_FORMAT"
  database    = snowflake_database.supplychain360_db.name
  schema      = snowflake_schema.schema.name
  format_type = "PARQUET"
}


resource "snowflake_schema" "schema" {
  name     = "RAW"
  database = snowflake_database.supplychain360_db.name
  comment  = "Schema for supplychain360 raw layer"
}


resource "snowflake_database" "supplychain360_db" {
  name    = "SUPPLYCHAIN360_DB"
  comment = "Supplychain360 database"

}

resource "snowflake_warehouse" "warehouse" {
  name                = "SC_WAREHOUSE"
  warehouse_type      = "STANDARD"
  warehouse_size      = "X-SMALL"
  max_cluster_count   = 3
  min_cluster_count   = 1
  scaling_policy      = "ECONOMY"
  auto_suspend        = 1200
  auto_resume         = true
  initially_suspended = true
  comment             = "Supplychain360 warehouse."

}
