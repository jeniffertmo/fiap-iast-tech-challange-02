import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

bucket = 's3://fiap-datalake-tech'
table = 'municipio'

df = spark.read.parquet(f"{bucket}/bronze/{table}.parquet")
print('Raw schema')
df.printSchema()
# deduplicate
df = df.dropDuplicates(['_record_hash'])
# drop bronze metadata
df = df.drop('_source_dataset', '_source_table', '_bronze_ingestion_timestamp')
# load dicionario
print('Dicionario Silver schema')
df_dicionario = spark.read.parquet(f'{bucket}/silver/dicionario/')
df_dicionario.printSchema()

lookup_rede = (
    df_dicionario
    .filter((col('id_tabela') == 'municipio') & (col('nome_coluna') == 'rede'))
    .select(
        col('chave').alias('rede'),
        col('valor').alias('rede_label')
    )
)
df = df.join(lookup_rede, on='rede', how='left')

df.show(20)

print('Silver schema')
df.printSchema()
df.write.mode("overwrite").parquet(f"{bucket}/silver/municipio/")
