# # ---------------------------------------------------------------------------
# # AWS Secrets Manager — secret shells only.
# #
# # Terraform creates the secret resource (name, description, tags).
# # Secret VALUES are populated manually via AWS CLI or console — never
# # committed to Terraform state or version control.
# #
# # After terraform apply, populate each secret once:
# #
# #   # Supabase — plain SQLAlchemy URL string (NOT JSON):
# #   aws secretsmanager put-secret-value \
# #     --secret-id supplychain360/supabase-db-cred \
# #     --secret-string "postgresql+psycopg2://user:pass@host:5432/postgres"
# #
# #   # Google service account — full contents of service_account.json:
# #   aws secretsmanager put-secret-value \
# #     --secret-id supplychain360/google-service-account \
# #     --secret-string file://src/service_account.json
# #
# #   # Cross-account S3 keys — JSON with two keys:
# #   aws secretsmanager put-secret-value \
# #     --secret-id supplychain360/s3-source-keys \
# #     --secret-string '{"aws_access_key_id":"...","aws_secret_access_key":"..."}'
# #
# # Secret names must match the defaults in src/utils/config.py.
# # ---------------------------------------------------------------------------

# resource "aws_secretsmanager_secret" "supabase_db_cred" {
#   name        = "supplychain360/supabase-db-cred"
#   description = "Supabase PostgreSQL SQLAlchemy connection URL"
#   tags        = local.common_tags
# }

# resource "aws_secretsmanager_secret" "google_service_account" {
#   name        = "supplychain360/google-service-account"
#   description = "Google Sheets service account JSON"
#   tags        = local.common_tags
# }

# resource "aws_secretsmanager_secret" "s3_source_keys" {
#   name        = "supplychain360/s3-source-keys"
#   description = "Cross-account S3 static access keys"
#   tags        = local.common_tags
# }

resource "aws_secretsmanager_secret" "latest_image" {
  name        = "supplychain360/current-image"
  description = "Latest Airflow Docker image URI pushed by CD pipeline"
  tags        = local.common_tags
}
