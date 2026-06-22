resource "aws_s3_bucket" "medallion-bucket" {
  bucket = "fiap-datalake-tech"

  tags = {
    Name        = var.medallion-bucket
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
    Name        = "fiap-datalake-tech-glue-scripts"
    Environment = "Dev"
  }

  force_destroy = true
}
