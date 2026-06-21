import pandas as pd
import os
import bigquery_client

# AWS imports import boto3
import boto3
from botocore.exceptions import ClientError
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

aws_access_key = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_key = os.environ['AWS_SECRET_ACCESS_KEY']
aws_region = 'us-east-1'


config = {
    'paths': {
        'bronze': 'bronze/',
        'silver': 'silver/',
        'gold': 'gold/',
        'quality_reports': 'quality_reports/',
    },
    'bucket': {
        'name': 'fiap-datalake-tech',
        'region': 'us-east-1',
    }
}


if __name__ == '__main__':
    tables = ['alunos',
              'dicionario',
              'meta_alfabetizacao_brasil',
              'meta_alfabetizacao_municipio',
              'meta_alfabetizacao_uf',
              'municipio',
              'uf']

    dfs = bigquery_client.ingest_from_bigquery(tables, config)
