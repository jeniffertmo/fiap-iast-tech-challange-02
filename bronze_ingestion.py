import pandas as pd
import os
import bigquery_client

# AWS imports
import boto3
from botocore.exceptions import ClientError
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

aws_access_key = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_key = os.environ['AWS_SECRET_ACCESS_KEY']
aws_region = 'us-east-1'


def upload_to_s3(dfs: dict) -> None:
    for df_name in dfs:
        logger.info(f"[+] Saving {df_name} dataframe to file")
        df = dfs[df_name]
        df.to_parquet(f'{df_name}.parquet', index=False)

        parquet_file = f"{df_name}.parquet"
        logger.info(f"Upload {parquet_file} to s3 bronze")
        s3_client.upload_file(str(parquet_file), bucket, f'{config["paths"]["bronze"]}{parquet_file}')


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
    #tables = ['alunos', 'dicionario', 'meta_alfabetizacao_brasil', 'meta_alfabetizacao_municipio', 'meta_alfabetizacao_uf', 'municipio', 'uf']
    tables = ['alunos']

    dfs = bigquery_client.ingest_from_bigquery(tables)

    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )

    bucket = config['bucket']['name']

    for path in config['paths'].values():
        try:
            # Check if the folder placeholder object exists}
            s3_client.head_object(Bucket=bucket, Key=path)
            logger.info(f"{path} folder already exists.")
        except ClientError as e:
            # A 404 error means the folder does not exist
            if e.response['Error']['Code'] == '404':
                s3_client.put_object(Bucket=bucket, Key=path)
                logger.info(f"{path} folder created successfully.")
            else:
                # Something else went wrong (e.g., permissions issues)
                logger.info(f"An error occurred: {e}")

    upload_to_s3(dfs)
