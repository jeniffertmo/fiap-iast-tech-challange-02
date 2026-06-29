resource "aws_s3_bucket" "datalake" {
  bucket = local.datalake_bukcet

  tags = {
    Name        = local.datalake_bukcet
    Environment = var.environment
  }

  force_destroy = true
}

resource "aws_s3_object" "layers" {
  for_each     = toset(local.layers)
  bucket       = aws_s3_bucket.datalake.id
  key          = each.value
  content_type = "application/x-directory"
}

resource "aws_s3_bucket" "glue_scripts" {
  bucket = local.glue_scripts_bucket

  tags = {
    Name        = local.glue_scripts_bucket
    Environment = var.environment
  }

  force_destroy = true
}
