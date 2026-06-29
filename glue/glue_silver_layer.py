import logging
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

bucket = 's3://fiap-datalake-tech'
table = 'alunos'

df = spark.read.parquet(f"{bucket}/bronze/{table}.parquet")
df.printSchema()
print(type(df))
df = df.dropDuplicates(['_record_hash'])
df = df.drop('_source_dataset', '_source_table', '_bronze_ingestion_timestamp')
