data "aws_caller_identity" "current" {}


data "aws_iam_policy_document" "snowflake_assume_role_placeholder" {
  statement {
    sid     = "SnowflakePlaceholder"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [data.aws_caller_identity.current.arn]
    }
  }
}

resource "aws_iam_role" "snowflake_role" {
  name               = "snowflake-s3-role"
  assume_role_policy = data.aws_iam_policy_document.snowflake_assume_role_placeholder.json
  tags               = local.common_tags

  lifecycle {
    # Allow the trust policy to be updated in-place without destroying the role
    ignore_changes = [assume_role_policy]
  }
}

resource "aws_iam_role_policy" "snowflake_s3" {
  name = "snowflake-s3-access"
  role = aws_iam_role.snowflake_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ]
      Resource = [
        aws_s3_bucket.supplychain360_bucket.arn,
        "${aws_s3_bucket.supplychain360_bucket.arn}/*"
      ]
    }]
  })
}


data "aws_iam_policy_document" "snowflake_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = [snowflake_storage_integration_aws.s3_integration.describe_output[0].iam_user_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [snowflake_storage_integration_aws.s3_integration.describe_output[0].external_id]
    }
  }
}

resource "null_resource" "patch_snowflake_trust_policy" {
  depends_on = [
    aws_iam_role.snowflake_role,
    snowflake_storage_integration_aws.s3_integration
  ]

  triggers = {
    iam_user_arn = snowflake_storage_integration_aws.s3_integration.describe_output[0].iam_user_arn
    external_id  = snowflake_storage_integration_aws.s3_integration.describe_output[0].external_id
    role_name    = aws_iam_role.snowflake_role.name
  }

  provisioner "local-exec" {
    command = <<-EOT
      aws iam update-assume-role-policy \
        --role-name "${aws_iam_role.snowflake_role.name}" \
        --policy-document '${data.aws_iam_policy_document.snowflake_assume_role.json}'
    EOT
  }
}

