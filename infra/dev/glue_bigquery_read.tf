resource "aws_glue_job" "bigquery_read" {
  name              = "bigquery-read-dicionario"
  description       = "Reads dicionario table from BigQuery and writes to S3 test path"
  role_arn          = aws_iam_role.glue_job_role.arn
  glue_version      = "5.0"
  max_retries       = 0
  timeout           = 60
  number_of_workers = 2
  worker_type       = "G.1X"
  execution_class   = "STANDARD"

  connections = [aws_glue_connection.bigquery.name]

  command {
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/jobs/bigquery_read.py"
    name            = "glueetl"
    python_version  = "3"
  }

  notification_property {
    notify_delay_after = 3
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--continuous-log-logGroup"          = "/aws-glue/jobs"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-metrics"                   = ""
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = {
    "ManagedBy" = "Terraform"
  }
}

resource "aws_s3_object" "bigquery_read_script" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "jobs/bigquery_read.py"
  source = "../../glue/bigquery_read.py"
  etag   = filemd5("../../glue/bigquery_read.py")
}
