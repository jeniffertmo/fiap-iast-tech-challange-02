from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from datetime import datetime, timezone

JOB_NAME = "bigquery-read-dicionario"
BUCKET   = "fiap-datalake-tech"
ENTITY   = "dicionario"
BQ_TABLE = "basedosdados.br_inep_avaliacao_alfabetizacao.dicionario"

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(JOB_NAME)

now = datetime.now(timezone.utc)
INGESTION_TS   = now.strftime("%Y-%m-%d %H:%M:%S")
INGESTION_DATE = now.strftime("%Y-%m-%d")
ANOMESDIA      = now.strftime("%Y%m%d")

dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="bigquery",
    connection_options={
        "connectionName": "fiap-datalake-tech-bigquery",
        "parentProject":  "harness-ci-480918",
        "sourceType":     "table",
        "table":          BQ_TABLE,
    }
)

df_bronze = (dyf.toDF()
    .withColumn("_ingestion_timestamp", F.lit(INGESTION_TS))
    .withColumn("_ingestion_date",      F.lit(INGESTION_DATE))
    .withColumn("_source_path",         F.lit(f"bigquery://{BQ_TABLE}"))
    .withColumn("_source_entity",       F.lit(ENTITY))
    .withColumn("_environment",         F.lit("dev"))
)

df_bronze.write.mode("overwrite").parquet(
    f"s3://{BUCKET}/bronze/{ENTITY}/anomesdia={ANOMESDIA}"
)

job.commit()
