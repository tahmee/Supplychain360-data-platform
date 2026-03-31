# 1. The S3 Bucket for State
resource "aws_s3_bucket" "terraform_statefile" {
  bucket = var.statefile_bucket # Globally unique

  force_destroy = true

  tags = local.common_tags

  lifecycle {
    prevent_destroy = false
  }

}

resource "aws_s3_bucket_public_access_block" "s3_access_block" {
  bucket = aws_s3_bucket.terraform_statefile.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# 2. Enable Versioning (Crucial for recovering from state corruption)
resource "aws_s3_bucket_versioning" "statefile_versioning" {
  bucket = aws_s3_bucket.terraform_statefile.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "bucket_lifecycle" {
  bucket = aws_s3_bucket.terraform_statefile.id

  depends_on = [aws_s3_bucket_versioning.statefile_versioning]

  rule {
    id     = "enable-old-versioning"
    status = "Enabled"

    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }
}


# 3. Encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "statefile_encryption" {
  bucket = aws_s3_bucket.terraform_statefile.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}