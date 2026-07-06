resource "aws_glue_connection" "bigquery" {
  name            = "${var.project-name}-bigquery"
  connection_type = "BIGQUERY"

  connection_properties = {
    SparkProperties = jsonencode({
      secretId = "gcp/service-account"
    })
  }
}

resource "aws_iam_role_policy" "glue_secretsmanager" {
  name = "glue-secretsmanager-gcp"
  role = aws_iam_role.glue_job_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "secretsmanager:GetSecretValue"
        Resource = "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:gcp/service-account*"
      }
    ]
  })
}

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}
