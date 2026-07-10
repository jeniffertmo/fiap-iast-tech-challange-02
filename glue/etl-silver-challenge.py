import sys
import logging
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

# ============================================================
# PARÂMETROS DO JOB (AJUSTADO PARA AMBIENTE LOCAL/DOCKER)
# ============================================================
# Verifica se os argumentos esperados pelo Glue estão presentes no terminal.
if not any(arg.startswith('--JOB_NAME') for arg in sys.argv):
    log.info("⚠️ Ambiente Local/Docker detectado! Ignorando getResolvedOptions.")
    JOB_NAME = "job-silver-local"
    ENTIDADE = "" 
    # Criamos um dicionário fictício apenas para não quebrar o job.init abaixo
    args = {'JOB_NAME': JOB_NAME, 'ENTIDADE': ENTIDADE}
else:
    # Se estiver rodando dentro da AWS Glue real:
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'ENTIDADE'])
    JOB_NAME = args['JOB_NAME']
    ENTIDADE = args['ENTIDADE']

# ============================================================
# CONTEXTO GLUE E SPARK
# ============================================================
sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)

# Agora o 'args' sempre existirá, seja real ou simulado!
job.init(JOB_NAME, args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.sparkContext.setLogLevel("WARN")

# ============================================================
# VARIÁVEIS DE AMBIENTE E MAPEAMENTOS
# ============================================================
JOB_NAME          = args['JOB_NAME']
ENTIDADE          = args['ENTIDADE']

# Balde e pastas padronizados do projeto
BUCKET_PRINCIPAL = "fiap-datalake-fase2"
PASTA_BRONZE      = "bronze"
PASTA_SILVER      = "silver"

# Formatação da partição igual ao do notebook para leitura
ANOMESDIA         = datetime.now(timezone.utc).strftime("%Y%m%d")

# Extração de componentes de data atuais para criar as novas partições da Silver
ano_corrente      = ANOMESDIA[0:4]
mes_corrente      = ANOMESDIA[4:6]
dia_corrente      = ANOMESDIA[6:8]

# Mapeamento dos caminhos S3
BRONZE_PATH       = f"s3://{BUCKET_PRINCIPAL}/{PASTA_BRONZE}/{ENTIDADE}/anomesdia={ANOMESDIA}/"
SILVER_PATH       = f"s3://{BUCKET_PRINCIPAL}/{PASTA_SILVER}/{ENTIDADE}/"

# Mapeamento de chaves de negócio (Padronizadas em MAIÚSCULO)
CHAVES_NEGOCIO_MAP = {
    "alunos_2023":     ["NU_ANO_AVALIACAO", "ID_ALUNO"],
    "alunos_2024":     ["NU_ANO_AVALIACAO", "ID_ALUNO"],
    "alunos_2025":     ["NU_ANO_AVALIACAO", "ID_ALUNO"],
    "meta_brasil":     ["ANO", "REDE"],
    "meta_uf":         ["ANO", "SG_UF"],
    "meta_municipio":  ["ANO", "ID_MUNICIPIO"],
    "municipio":       ["ANO", "ID_MUNICIPIO"],
    "uf":              ["ANO", "SG_UF"]
}

log.info("=" * 60)
log.info(f"JOB       : {JOB_NAME}")
log.info(f"ENTIDADE  : {ENTIDADE}")
log.info(f"LENDO DE  : {BRONZE_PATH}")
log.info(f"GRAVANDO  : {SILVER_PATH} (Particionado por ano, mes, dia)")
log.info("=" * 60)


# ============================================================
# FUNÇÕES POR TABELA
# ============================================================

def transformar_alunos_2023(df):
    log.info("[SILVER] Aplicando regras específicas: alunos_2023")
    df = df.select(*[F.col(c).alias("SG_UF" if c.upper() == "SIGLA_UF" else c.upper()) for c in df.columns])
    df = df.select(*[F.col(c.name).cast(StringType()).alias(c.name) if isinstance(c.dataType, StringType) else F.col(c.name) for c in df.schema])
    
    colunas_string = [c.name for c in df.schema.fields if isinstance(c.dataType, StringType)]
    for col_name in colunas_string:
        df = df.withColumn(col_name, F.when(F.trim(F.col(col_name)) == "", F.lit(None)).otherwise(F.col(col_name)))
    df = df.fillna("N/A", subset=colunas_string)
    
    colunas_numericas = [c.name for c in df.schema.fields if any(t in c.dataType.simpleString() for t in ["double", "float", "int"])]
    for col_num in colunas_numericas:
        df = df.withColumn(col_num, F.when(F.col(col_num).isNull() | (F.trim(F.col(col_num).cast("string")) == ""), F.lit(None)).otherwise(F.round(F.col(col_num), 2)))
    return df


def transformar_alunos_2024(df):
    log.info("[SILVER] Aplicando regras específicas: alunos_2024")
    df = df.select(*[F.col(c).alias("SG_UF" if c.upper() == "SIGLA_UF" else c.upper()) for c in df.columns])
    df = df.select(*[F.col(c.name).cast(StringType()).alias(c.name) if isinstance(c.dataType, StringType) else F.col(c.name) for c in df.schema])
    
    colunas_string = [c.name for c in df.schema.fields if isinstance(c.dataType, StringType)]
    for col_name in colunas_string:
        df = df.withColumn(col_name, F.when(F.trim(F.col(col_name)) == "", F.lit(None)).otherwise(F.col(col_name)))
    df = df.fillna("N/A", subset=colunas_string)
    
    colunas_numericas = [c.name for c in df.schema.fields if any(t in c.dataType.simpleString() for t in ["double", "float", "int"])]
    for col_num in colunas_numericas:
        df = df.withColumn(col_num, F.when(F.col(col_num).isNull() | (F.trim(F.col(col_num).cast("string")) == ""), F.lit(None)).otherwise(F.round(F.col(col_num), 2)))
    return df


def transformar_alunos_2025(df):
    log.info("[SILVER] Aplicando regras específicas: alunos_2025")
    df = df.select(*[F.col(c).alias("SG_UF" if c.upper() == "SIGLA_UF" else c.upper()) for c in df.columns])
    df = df.select(*[F.col(c.name).cast(StringType()).alias(c.name) if isinstance(c.dataType, StringType) else F.col(c.name) for c in df.schema])
    
    colunas_para_dropar = [
        "CO_BLOCO_1", "TX_RESPOSTA_BLOCO_1", "TX_GABARITO_BLOCO_1",
        "CO_BLOCO_2", "TX_RESPOSTA_BLOCO_2", "TX_GABARITO_BLOCO_2",
        "CO_BLOCO_3", "TX_RESPOSTA_BLOCO_3", "TX_GABARITO_BLOCO_3",
        "CO_BLOCO_4", "TX_RESPOSTA_BLOCO_4", "TX_GABARITO_BLOCO_4"
    ]
    colunas_existentes = [c for c in colunas_para_dropar if c in df.columns]
    if colunas_existentes:
        df = df.drop(*colunas_existentes)

    colunas_para_inteiro = ["ID_ESCOLA", "TP_DEPENDENCIA", "CO_MUNICIPIO"]
    for col_num in colunas_para_inteiro:
        if col_num in df.columns:
            df = df.withColumn(col_num, F.col(col_num).cast("int"))

    colunas_string = [c.name for c in df.schema.fields if isinstance(c.dataType, StringType)]
    for col_name in colunas_string:
        df = df.withColumn(col_name, F.when(F.trim(F.col(col_name)) == "", F.lit(None)).otherwise(F.col(col_name)))
    df = df.fillna("N/A", subset=colunas_string)
    
    colunas_numericas = [c.name for c in df.schema.fields if any(t in c.dataType.simpleString() for t in ["double", "float", "int"])]
    for col_num in colunas_numericas:
        df = df.withColumn(col_num, F.when(F.col(col_num).isNull() | (F.trim(F.col(col_num).cast("string")) == ""), F.lit(None)).otherwise(F.round(F.col(col_num), 2)))
    return df


def transformar_meta_brasil(df):
    log.info("[SILVER] Aplicando regras específicas: meta_brasil")
    # Seleciona TODAS as colunas existentes de forma dinâmica
    df = df.select(*df.columns)
    
    df = df.select(*[F.col(c).alias("SG_UF" if c.upper() == "SIGLA_UF" else c.upper()) for c in df.columns])
    df = df.select(*[F.col(c.name).cast(StringType()).alias(c.name) if isinstance(c.dataType, StringType) else F.col(c.name) for c in df.schema])
    
    if "REDE" in df.columns:
        df = df.withColumn("REDE", F.col("REDE").cast("string"))

    colunas_string = [c.name for c in df.schema.fields if isinstance(c.dataType, StringType)]
    for col_name in colunas_string:
        df = df.withColumn(col_name, F.when(F.trim(F.col(col_name)) == "", F.lit(None)).otherwise(F.col(col_name)))
    df = df.fillna("N/A", subset=colunas_string)
    
    colunas_numericas = [c.name for c in df.schema.fields if any(t in c.dataType.simpleString() for t in ["double", "float", "int"])]
    for col_num in colunas_numericas:
        df = df.withColumn(col_num, F.when(F.col(col_num).isNull() | (F.trim(F.col(col_num).cast("string")) == ""), F.lit(None)).otherwise(F.round(F.col(col_num), 2)))
    return df


def transformar_meta_uf(df):
    log.info("[SILVER] Aplicando regras específicas: meta_uf")
    df = df.select(*df.columns)
    
    df = df.select(*[F.col(c).alias("SG_UF" if c.upper() == "SIGLA_UF" else c.upper()) for c in df.columns])
    df = df.select(*[F.col(c.name).cast(StringType()).alias(c.name) if isinstance(c.dataType, StringType) else F.col(c.name) for c in df.schema])
    
    if "SG_UF" in df.columns:
        df = df.withColumn("SG_UF", F.col("SG_UF").cast("string"))

    colunas_string = [c.name for c in df.schema.fields if isinstance(c.dataType, StringType)]
    for col_name in colunas_string:
        df = df.withColumn(col_name, F.when(F.trim(F.col(col_name)) == "", F.lit(None)).otherwise(F.col(col_name)))
    df = df.fillna("N/A", subset=colunas_string)
    
    colunas_numericas = [c.name for c in df.schema.fields if any(t in c.dataType.simpleString() for t in ["double", "float", "int"])]
    for col_num in colunas_numericas:
        df = df.withColumn(col_num, F.when(F.col(col_num).isNull() | (F.trim(F.col(col_num).cast("string")) == ""), F.lit(None)).otherwise(F.round(F.col(col_num), 2)))
    return df


def transformar_meta_municipio(df):
    log.info("[SILVER] Aplicando regras específicas: meta_municipio")
    df = df.select(*df.columns)
    
    df = df.select(*[F.col(c).alias("SG_UF" if c.upper() == "SIGLA_UF" else c.upper()) for c in df.columns])
    df = df.select(*[F.col(c.name).cast(StringType()).alias(c.name) if isinstance(c.dataType, StringType) else F.col(c.name) for c in df.schema])
    
    if "NO_MUNICIPIO" in df.columns:
        df = df.withColumn("NO_MUNICIPIO", F.col("NO_MUNICIPIO").cast("string"))

    colunas_string = [c.name for c in df.schema.fields if isinstance(c.dataType, StringType)]
    for col_name in colunas_string:
        df = df.withColumn(col_name, F.when(F.trim(F.col(col_name)) == "", F.lit(None)).otherwise(F.col(col_name)))
    df = df.fillna("N/A", subset=colunas_string)
    
    colunas_numericas = [c.name for c in df.schema.fields if any(t in c.dataType.simpleString() for t in ["double", "float", "int"])]
    for col_num in colunas_numericas:
        df = df.withColumn(col_num, F.when(F.col(col_num).isNull() | (F.trim(F.col(col_num).cast("string")) == ""), F.lit(None)).otherwise(F.round(F.col(col_num), 2)))
    return df


def transformar_municipio(df):
    log.info("[SILVER] Aplicando regras específicas: municipio")
    df = df.select(*df.columns)
    
    df = df.select(*[F.col(c).alias("SG_UF" if c.upper() == "SIGLA_UF" else c.upper()) for c in df.columns])
    df = df.select(*[F.col(c.name).cast(StringType()).alias(c.name) if isinstance(c.dataType, StringType) else F.col(c.name) for c in df.schema])
    
    if "NO_MUNICIPIO" in df.columns:
        df = df.withColumn("NO_MUNICIPIO", F.col("NO_MUNICIPIO").cast("string"))

    colunas_string = [c.name for c in df.schema.fields if isinstance(c.dataType, StringType)]
    for col_name in colunas_string:
        df = df.withColumn(col_name, F.when(F.trim(F.col(col_name)) == "", F.lit(None)).otherwise(F.col(col_name)))
    df = df.fillna("N/A", subset=colunas_string)
    
    colunas_numericas = [c.name for c in df.schema.fields if any(t in c.dataType.simpleString() for t in ["double", "float", "int"])]
    for col_num in colunas_numericas:
        df = df.withColumn(col_num, F.when(F.col(col_num).isNull() | (F.trim(F.col(col_num).cast("string")) == ""), F.lit(None)).otherwise(F.round(F.col(col_num), 2)))
    return df


def transformar_uf(df):
    log.info("[SILVER] Aplicando regras específicas: uf")
    df = df.select(*df.columns)
    
    df = df.select(*[F.col(c).alias("SG_UF" if c.upper() == "SIGLA_UF" else c.upper()) for c in df.columns])
    df = df.select(*[F.col(c.name).cast(StringType()).alias(c.name) if isinstance(c.dataType, StringType) else F.col(c.name) for c in df.schema])
    
    if "SG_UF" in df.columns:
        df = df.withColumn("SG_UF", F.col("SG_UF").cast("string"))

    colunas_string = [c.name for c in df.schema.fields if isinstance(c.dataType, StringType)]
    for col_name in colunas_string:
        df = df.withColumn(col_name, F.when(F.trim(F.col(col_name)) == "", F.lit(None)).otherwise(F.col(col_name)))
    df = df.fillna("N/A", subset=colunas_string)
    
    colunas_numericas = [c.name for c in df.schema.fields if any(t in c.dataType.simpleString() for t in ["double", "float", "int"])]
    for col_num in colunas_numericas:
        df = df.withColumn(col_num, F.when(F.col(col_num).isNull() | (F.trim(F.col(col_num).cast("string")) == ""), F.lit(None)).otherwise(F.round(F.col(col_num), 2)))
    return df


TRANSFORMACOES = {
    "alunos_2023":    transformar_alunos_2023,
    "alunos_2024":    transformar_alunos_2024,
    "alunos_2025":    transformar_alunos_2025,
    "meta_brasil":    transformar_meta_brasil,
    "meta_uf":        transformar_meta_uf,
    "meta_municipio": transformar_meta_municipio,
    "municipio":      transformar_municipio,
    "uf":             transformar_uf,
}

# ============================================================
# ORQUESTRAÇÃO DA TRANSFORMAÇÃO
# ============================================================

def construir_silver(df_bronze, entidade_atual):
    #Remove  partições e metadados de carga
    colunas_para_remover = [c for c in ["mes", "dia", "ANOMESDIA", "anomesdia"] if c in df_bronze.columns]
    df = df_bronze.drop(*colunas_para_remover)
    
    # Executa dinamicamente a função correspondente à tabela atual
    transformar = TRANSFORMACOES.get(entidade_atual, lambda d: d)
    df = transformar(df)
    
    # Deduplicação baseada no mapeamento de chaves
    chaves = CHAVES_NEGOCIO_MAP.get(entidade_atual)
    if chaves:
        antes = df.count()
        df = df.dropna(subset=chaves)
        df = df.dropDuplicates(chaves)
        log.info(f"[SILVER] Deduplicação: {antes - df.count()} linhas nulas/duplicadas removidas (Chaves: {chaves})")
    else:
        log.warning(f"[SILVER] Nenhuma chave cadastrada para '{ENTIDADE}' — pulando deduplicação")

    # Ingestão de Metadados e criação explicativa das partições separadas para a gravação final no S3
    # Usamos os nomes 'ano_particao', 'mes_particao' e 'dia_particao' para nunca colidir com o seu campo "ANO" de dados.
    return (df
        .withColumn("_SILVER_PROCESSED_AT", F.current_timestamp())
        .withColumn("_PIPELINE_VERSION",     F.lit("v1.2_silver_uppercase"))
        .withColumn("ano_particao",          F.lit(ano_corrente))
        .withColumn("mes_particao",          F.lit(mes_corrente))
        .withColumn("dia_particao",          F.lit(dia_corrente))
    )

# ============================================================
# EXECUÇÃO DO PIPELINE (ADAPTADO PARA LOOP LOCAL DE 8 TABELAS)
# ============================================================

# Se a ENTIDADE global inicializada for vazia, corre todas as 8 tabelas do dicionário
if ENTIDADE == "":
    tabelas_para_processar = list(TRANSFORMACOES.keys())
else:
    tabelas_para_processar = [ENTIDADE]

for entidade_atual in tabelas_para_processar:
    
    # IMPORTANTE: Construção explícita do caminho com a entidade da iteração corrente
    BRONZE_PATH = f"s3://{BUCKET_PRINCIPAL}/{PASTA_BRONZE}/{entidade_atual}/anomesdia={ANOMESDIA}/"
    SILVER_PATH = f"s3://{BUCKET_PRINCIPAL}/{PASTA_SILVER}/{entidade_atual}/"
    
    log.info("-" * 60)
    log.info(f"🚀 [LOOP] Processando Entidade: {entidade_atual}")
    log.info(f"Lendo de: {BRONZE_PATH}")
    
    try:
        df_bronze = spark.read.parquet(BRONZE_PATH)
        registros_bronze = df_bronze.count()
        log.info(f"📊 Registros encontrados na Bronze: {registros_bronze}")
                      
        if registros_bronze == 0:
            log.warning(f"⚠️ Lote vazio para {entidade_atual}. Pulando...")
            continue
            
        # Passamos a entidade correta para aplicar as regras isoladas
        df_silver = construir_silver(df_bronze, entidade_atual)
        
        log.info(f"💾 Gravando {entidade_atual} na Silver S3: {SILVER_PATH}")
        df_silver.write \
            .partitionBy("ano_particao", "mes_particao", "dia_particao") \
            .mode("overwrite") \
            .parquet(SILVER_PATH)
            
        log.info(f"✅ Concluído com sucesso: {entidade_atual}")

    except Exception as e:
        log.error(f"❌ Erro ao processar a tabela {entidade_atual}: {str(e)}")

# Finaliza de forma segura
try:
    job.commit()
except NameError:
    pass