terraform {
  backend "s3" {
    bucket = "fiap-datalake-tech-terraform"
    region = "us-east-1"
    key    = "dev/datalake.tfstate"
  }
}
