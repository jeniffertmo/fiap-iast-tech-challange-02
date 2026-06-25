import logging
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

bucket = 's3://fiap-datalake-tech'
table = 'dicionario'

df = spark.read.parquet(f"{bucket}/bronze/{table}.parquet")
print('Raw schema')
df.printSchema()
# deduplicate
df = df.dropDuplicates(['_record_hash'])
# drop bronze metadata
df = df.drop('_source_dataset', '_source_table', '_bronze_ingestion_timestamp')
#
df = df.drop('cobertura_temporal')
print('Silver schema')
df.printSchema()
df.write.mode("overwrite").parquet(f"{bucket}/silver/dicionario/")
