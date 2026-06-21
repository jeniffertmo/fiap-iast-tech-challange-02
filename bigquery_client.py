import logging
import hashlib
from datetime import datetime, timezone
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
from s3_client import upload_to_s3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
        df['_ingestion_timestamp'] = INGESTION_TIMESTAMP
        df['_source_dataset'] = 'basedosdados'
        df['_source_table'] = table
        # We remove the metadata table, as to only hash on the content
        # TODO: implement a function to hash tables, make it so it accepts
        # generic metadata as long as it follows a pattern
        df['_record_hash'] = df.drop(
            columns=['_ingestion_timestamp', '_source_dataset', '_source_table'],
            errors='ignore'
        ).apply(lambda row: hashlib.md5(str(row.values).encode()).hexdigest(), axis=1)

        logger.info("Itens added: %s", len(df))
        logger.info("   Columns: %s", list(df.columns))

        upload_to_s3(df, table, config)
