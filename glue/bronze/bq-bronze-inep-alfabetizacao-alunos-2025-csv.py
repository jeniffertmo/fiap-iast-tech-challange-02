from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from datetime import datetime, timezone
import sys

args   = getResolvedOptions(sys.argv, ["JOB_NAME", "table"])
ENTITY = args["table"]

BUCKET   = "fiap-datalake-tech"
S3_RAW_PATH = f"s3://{BUCKET}/raw/alunos_2025/"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"])

now = datetime.now(timezone.utc)
INGESTION_TS   = now.strftime("%Y-%m-%d %H:%M:%S")
INGESTION_DATE = now.strftime("%Y-%m-%d")
ANOMESDIA      = now.strftime("%Y%m%d")

df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ";") \
    .option("encoding", "ISO-8859-1") \
    .load(S3_RAW_PATH)


df_bronze = (df_raw
    .withColumn("_ingestion_timestamp", F.lit(INGESTION_TS))
    .withColumn("_ingestion_date",      F.lit(INGESTION_DATE))
    .withColumn("_source_path",         F.lit(S3_RAW_PATH))
    .withColumn("_source_entity",       F.lit(ENTITY))
    .withColumn("_environment",         F.lit("dev"))
)

df_bronze.write.mode("overwrite").parquet(
    f"s3://{BUCKET}/bronze/{ENTITY}/anomesdia={ANOMESDIA}"
)

job.commit()
