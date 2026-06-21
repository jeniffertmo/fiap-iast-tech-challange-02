import bigquery_client

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
