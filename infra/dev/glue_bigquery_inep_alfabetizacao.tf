locals {
  inep_alfabetizacao_tables = toset([
    "alunos",
    "dicionario",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_municipio",
    "meta_alfabetizacao_uf",
    "municipio",
    "uf",
  ])
}

resource "aws_s3_object" "bq_inep_alfabetizacao_script" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "jobs/bronze/bigquery_inep_alfabetizacao.py"
  source = "../../glue/bronze/bigquery_inep_alfabetizacao.py"
  etag   = filemd5("../../glue/bronze/bigquery_inep_alfabetizacao.py")
}

resource "aws_glue_job" "bq_inep_alfabetizacao" {
  for_each = local.inep_alfabetizacao_tables

  name              = "bq-bronze-inep-alfabetizacao-${each.key}"
  description       = "Reads ${each.key} from BigQuery br_inep_avaliacao_alfabetizacao into S3 bronze layer"
  role_arn          = "arn:aws:iam::161582022021:role/glue-role"
  glue_version      = "5.0"
  max_retries       = 0
  timeout           = 60
  number_of_workers = 2
  worker_type       = "G.1X"
  execution_class   = "STANDARD"

  connections = [aws_glue_connection.bigquery.name]

  command {
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/jobs/bronze/bigquery_inep_alfabetizacao.py"
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
    "--table"                            = each.key
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = {
    "ManagedBy" = "Terraform"
    "Dataset"   = "inep_avaliacao_alfabetizacao"
  }
}
