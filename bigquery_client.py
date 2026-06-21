import logging
import hashlib
import os
import datetime
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import boto3
from botocore.exceptions import ClientError

aws_access_key = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_key = os.environ['AWS_SECRET_ACCESS_KEY']
aws_region = 'us-east-1'


def upload_to_s3(df, table, config) -> None:
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )

    logger.info(f"[+] Saving {table} dataframe to file")
    df.to_parquet(f'{table}.parquet', index=False)

    parquet_file = f"{table}.parquet"
    logger.info(f"Upload {parquet_file} to s3 bronze")
    s3_client.upload_file(str(parquet_file), config['bucket']['name'], f'{config["paths"]["bronze"]}{parquet_file}')


def ingest_from_bigquery(tables, config) -> None:
    INGESTION_TIMESTAMP = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    client = bigquery.Client()

    for table in tables:
        query = f"""
    SELECT * FROM `basedosdados.br_inep_avaliacao_alfabetizacao.{table}` LIMIT 1000
    """
        logger.info(f"[+] Querying table {table}")
        try:
            query_job = client.query(query)
        except GoogleAPIError as e:
            logger.error("Failed to query table %s: %s", table, e)
            raise
        logger.info("[+] Appending to ingestion dataframes")
        df = query_job.to_dataframe()
        df['_ingestion_timestamp_'] = INGESTION_TIMESTAMP
        df['_source_dataset_'] = 'basedosdados'
        df['_source_table_'] = table
        df['_record_hash'] = df.drop(
            columns=['_ingestion_timestamp', '_source_url', '_source_system'],
            errors='ignore'
        ).apply(lambda row: hashlib.md5(str(row.values).encode()).hexdigest(), axis=1)

        logger.info("Registros inseridos: %s", len(df))
        logger.info("   Colunes: %s", list(df.columns))

        upload_to_s3(df, table, config)
