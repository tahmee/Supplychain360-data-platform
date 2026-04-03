resource "aws_secretsmanager_secret" "latest_image" {
  name        = var.image_name
  description = "Latest Airflow Docker image URI pushed by CD pipeline"
  tags        = local.common_tags
}
