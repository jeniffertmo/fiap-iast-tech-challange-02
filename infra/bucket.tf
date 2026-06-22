resource "aws_s3_bucket" "medallion-bucket" {
  bucket = local.datalake_bukcet

  tags = {
    Name        = local.datalake_bukcet
    Environment = "Dev"
  }

  force_destroy = true
}

resource "aws_s3_object" "layers" {
  for_each = toset(local.layers)
  bucket       = aws_s3_bucket.medallion-bucket.id
  key          = each.value
  content_type = "application/x-directory"

}

resource "aws_s3_bucket" "glue-scripts-bucket" {
  bucket = local.glue_scripts_bucket

  tags = {
    Name        = local.glue_scripts_bucket
    Environment = "Dev"
  }

  force_destroy = true
}
