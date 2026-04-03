resource "aws_s3_bucket" "supplychain360_bucket" {
  bucket = var.source_bucket

  tags = local.common_tags

  lifecycle {
    prevent_destroy = false
  }
}

resource "aws_s3_bucket_public_access_block" "suppychain360_access" {
  bucket = aws_s3_bucket.supplychain360_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

