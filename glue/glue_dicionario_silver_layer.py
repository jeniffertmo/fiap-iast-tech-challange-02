from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

BUCKET = "s3://fiap-datalake-tech"
ENTITY = "dicionario"

BRONZE_METADATA = [
    "_ingestion_timestamp",
    "_ingestion_date",
    "_source_path",
    "_source_entity",
    "_environment",
]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(f"silver-{ENTITY}")

df = spark.read.parquet(f"{BUCKET}/bronze/{ENTITY}/")

print("Bronze schema:")
df.printSchema()

df_silver = df.drop(*BRONZE_METADATA).dropDuplicates()

print("Silver schema:")
df_silver.printSchema()

df_silver.write.mode("overwrite").parquet(f"{BUCKET}/silver/{ENTITY}/")

job.commit()
