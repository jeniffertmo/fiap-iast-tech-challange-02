import sys
import logging
from datetime import datetime, timezone
import boto3

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
# CONTEXTO GLUE E SPARK
# ============================================================
sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)


# ============================================================
# PARÂMETROS DO JOB (AWS GLUE)
# ============================================================
args_disponiveis = {'JOB_NAME': "", 'ENTIDADE': ""}

argumentos_reais = []
if any(arg.startswith('--JOB_NAME') for arg in sys.argv):
    argumentos_reais.append('JOB_NAME')
if any(arg.startswith('--ENTIDADE') for arg in sys.argv):
    argumentos_reais.append('ENTIDADE')

if argumentos_reais:
    try:
        valores_capturados = getResolvedOptions(sys.argv, argumentos_reais)
        for k, v in valores_capturados.items():
            args_disponiveis[k] = v
    except Exception as e:
        log.warning(f"⚠️ Falha ao resolver parâmetros do Glue: {str(e)}. Usando padrões.")

JOB_NAME = args_disponiveis['JOB_NAME'] if args_disponiveis['JOB_NAME'] else "bq-silver-inep-alfabetizacao-meta_alfabetizacao_uf"
ENTIDADE = args_disponiveis['ENTIDADE']

job.init(JOB_NAME, args_disponiveis)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.sparkContext.setLogLevel("WARN")

# ============================================================
# VARIÁVEIS DE AMBIENTE E MAPEAMENTOS
# ============================================================
BUCKET_PRINCIPAL  = "fiap-datalake-tech"  
PASTA_BRONZE      = "bronze"
PASTA_SILVER      = "silver"

# 1. DATA DE LEITURA DA BRONZE
ANOMESDIA = "20260630" 

# 2. DATA REAL DA INGESTÃO (Utilizada para as partições do S3)
agora_utc = datetime.now(timezone.utc)
ano_corrente = agora_utc.strftime("%Y")
mes_corrente = agora_utc.strftime("%m")
dia_corrente = agora_utc.strftime("%d")

# Chaves de negócio originais baseadas no esquema da Bronze
CHAVES_NEGOCIO_MAP = {
    "alunos":                        ["ano", "id_aluno"],
    "meta_alfabetizacao_brasil":    ["ano", "rede"],
    "meta_alfabetizacao_uf":        ["ano", "sigla_uf"],
    "meta_alfabetizacao_municipio": ["ano", "id_municipio"],
    "municipio":                    ["ano", "id_municipio"],
    "uf":                           ["ano", "sigla_uf"]
}

# ============================================================
# REGRAS DE QUALIDADE (DATA QUALITY - DQ)
# ============================================================
CHECKS = {
    "alunos": [
        {"tipo": "min_count", "valor": 1,               "critico": True},
        {"tipo": "not_null",  "coluna": "id_aluno",     "critico": True},
        {"tipo": "not_null",  "coluna": "ano",          "critico": True},
    ],
    "meta_alfabetizacao_brasil": [
        {"tipo": "min_count", "valor": 1,               "critico": True},
        {"tipo": "not_null",  "coluna": "ano",          "critico": True},
        {"tipo": "not_null",  "coluna": "rede",         "critico": True},
    ],
    "meta_alfabetizacao_uf": [
        {"tipo": "min_count", "valor": 1,               "critico": True},
        {"tipo": "not_null",  "coluna": "ano",          "critico": True},
        {"tipo": "not_null",  "coluna": "sigla_uf",     "critico": True},
        {"tipo": "regex",     "coluna": "sigla_uf",     "valor": r"^[A-Z]{2}$", "critico": False},
    ],
    "meta_alfabetizacao_municipio": [
        {"tipo": "min_count", "valor": 1,               "critico": True},
        {"tipo": "not_null",  "coluna": "ano",          "critico": True},
        {"tipo": "not_null",  "coluna": "id_municipio", "critico": True},
    ],
    "municipio": [
        {"tipo": "min_count", "valor": 1,               "critico": True},
        {"tipo": "not_null",  "coluna": "id_municipio", "critico": True},
        {"tipo": "not_null",  "coluna": "ano",          "critico": True},
    ],
    "uf": [
        {"tipo": "min_count", "valor": 1,               "critico": True},
        {"tipo": "not_null",  "coluna": "sigla_uf",     "critico": True},
        {"tipo": "not_null",  "coluna": "ano",          "critico": True},
        {"tipo": "regex",     "coluna": "sigla_uf",     "valor": r"^[A-Z]{2}$", "critico": False},
    ],
}

log.info("=" * 60)
log.info(f"JOB       : {JOB_NAME}")
log.info(f"ENTIDADE  : {ENTIDADE}")
log.info(f"DATA LOTE : {ANOMESDIA}")
log.info("=" * 60)

# ============================================================
# FUNÇÃO DE VALIDAÇÃO DE QUALIDADE
# ============================================================
def checar_qualidade(df, checks):
    log.info(f"[DQ:SILVER] Iniciando verificacoes | checks={len(checks)}")
    passou = falhou = criticos = 0

    for check in checks:
        tipo    = check["tipo"]
        coluna  = check.get("coluna")
        valor   = check.get("valor")
        critico = check.get("critico", True)
        ok      = False
        detalhe = ""

        if coluna and coluna not in df.columns:
            log.warning(f"[DQ:SILVER] Coluna '{coluna}' nao encontrada no schema para o check '{tipo}'. Pulando...")
            continue

        try:
            if tipo == "not_null":
                nulos = df.filter(F.col(coluna).isNull() | (F.col(coluna) == "N/A")).count()
                ok      = nulos == 0
                detalhe = f"{nulos} nulos/NA encontrados"
            elif tipo == "min_count":
                contagem = df.count()
                ok       = contagem >= valor
                detalhe  = f"contagem={contagem} | minimo={valor}"
            elif tipo == "unique":
                dups    = df.count() - df.select(coluna).distinct().count()
                ok      = dups == 0
                detalhe = f"{dups} duplicatas encontradas"
            elif tipo == "regex":
                invalidos = df.filter(
                    F.col(coluna).isNotNull() & (F.col(coluna) != "N/A") & ~F.col(coluna).rlike(valor)
                ).count()
                ok      = invalidos == 0
                detalhe = f"{invalidos} com formato invalido"
            elif tipo == "range":
                mn, mx = valor
                fora   = df.filter((F.col(coluna) < mn) | (F.col(coluna) > mx)).count()
                ok      = fora == 0
                detalhe = f"{fora} fora do intervalo [{mn},{mx}]"
        except Exception as e:
            ok      = False
            detalhe = f"Erro ao executar check: {e}"

        status = "PASS" if ok else ("FAIL" if critico else "WARN")
        if ok:
            passou += 1
            log.info(f"[DQ:SILVER] {status} | {tipo} | coluna={coluna} | {detalhe}")
        else:
            falhou += 1
            if critico:
                criticos += 1
                log.error(f"[DQ:SILVER] {status} | {tipo} | coluna={coluna} | {detalhe}")
            else:
                log.warning(f"[DQ:SILVER] {status} | {tipo} | coluna={coluna} | {detalhe}")

    score = round(passou / len(checks) * 100, 1) if checks else 100.0
    log.info(f"[DQ:SILVER] Score={score}% | PASS={passou} FAIL={falhou}")

    if criticos > 0:
        raise Exception(f"[DQ:SILVER] {criticos} check(s) critico(s) falharam. Escrita abortada para esta entidade.")

# ============================================================
# FUNÇÕES DE TRANSFORMAÇÃO
# ============================================================

def transformar_entidade_unificado(df, entidade_atual):
    log.info(f"[SILVER] Aplicando regras de tratamento para: {entidade_atual}")
    
    # --- 1. REGRA COMUM: Padronização inicial de tipos String ---
    df = df.select(*[F.col(c.name).cast(StringType()).alias(c.name) if isinstance(c.dataType, StringType) else F.col(c.name) for c in df.schema])
    
    # --- 2. REGRAS ESPECÍFICAS (Por exceção) ---
    if entidade_atual == "alunos":
        colunas_para_dropar = [
            "co_bloco_1", "tx_resposta_bloco_1", "tx_gabarito_bloco_1",
            "co_bloco_2", "tx_resposta_bloco_2", "tx_gabarito_bloco_2",
            "co_bloco_3", "tx_resposta_bloco_3", "tx_gabarito_bloco_3",
            "co_bloco_4", "tx_resposta_bloco_4", "tx_gabarito_bloco_4"
        ]
        colunas_existentes_drop = [c for c in colunas_para_dropar if c in df.columns]
        if colunas_existentes_drop:
            df = df.drop(*colunas_existentes_drop)

        colunas_para_inteiro = ["id_escola", "tp_dependencia", "co_municipio", "ano"]
        for col_num in colunas_para_inteiro:
            if col_num in df.columns:
                df = df.withColumn(col_num, F.col(col_num).cast("int"))
                
    elif entidade_atual in ["meta_alfabetizacao_brasil", "meta_alfabetizacao_uf", "meta_alfabetizacao_municipio", "municipio", "uf"]:
        # Garante casts de campos chave específicos
        for col_cast, col_tipo in [("rede", "string"), ("sigla_uf", "string"), ("no_municipio", "string")]:
            if col_cast in df.columns:
                df = df.withColumn(col_cast, F.col(col_cast).cast(col_tipo))

    # --- 3. REGRA COMUM: Tratamento de Strings Vazias e Nulos ---
    colunas_string = [c.name for c in df.schema.fields if isinstance(c.dataType, StringType)]
    for col_name in colunas_string:
        df = df.withColumn(col_name, F.when(F.trim(F.col(col_name)) == "", F.lit(None)).otherwise(F.col(col_name)))
    df = df.fillna("N/A", subset=colunas_string)
    
    # --- 4. REGRA COMUM: Tratamento e arredondamento numérico geral ---
    colunas_numericas = [c.name for c in df.schema.fields if any(t in c.dataType.simpleString() for t in ["double", "float", "int"])]
    for col_num in colunas_numericas:
        df = df.withColumn(col_num, F.when(F.col(col_num).isNull() | (F.trim(F.col(col_num).cast("string")) == ""), F.lit(None)).otherwise(F.round(F.col(col_num), 2)))
        
    return df

# ============================================================
# ORQUESTRAÇÃO DA TRANSFORMAÇÃO
# ============================================================
def construir_silver(df_bronze, entity_atual):
    colunas_para_remover = [c for c in ["mes", "dia", "ANOMESDIA", "anomesdia"] if c in df_bronze.columns]
    df = df_bronze.drop(*colunas_para_remover)
    
    df = transformar_entidade_unificado(df, entidade_atual)
    
    # Deduplicação baseada no mapeamento de chaves
    chaves = CHAVES_NEGOCIO_MAP.get(entidade_atual)
    if chaves:
        antes = df.count()
        chaves_existentes = [c for c in chaves if c in df.columns]
        
        if chaves_existentes:
            df = df.dropna(subset=chaves_existentes)
            df = df.dropDuplicates(chaves_existentes)
            log.info(f"[SILVER] Deduplicação: {antes - df.count()} linhas nulas/duplicadas removidas (Chaves utilizadas: {chaves_existentes})")
    else:
        log.warning(f"[SILVER] Nenhuma chave cadastrada para '{entidade_atual}' — pulando deduplicação")

    # Ingestão de partições limpas que vão gerar a estrutura física de pastas no S3
    return (df
        .withColumn("_SILVER_PROCESSED_AT", F.current_timestamp())
        .withColumn("_PIPELINE_VERSION",     F.lit("v1.8_partitioned_by_date"))
        .withColumn("ano_ingestao",                    F.lit(ano_corrente))
        .withColumn("mes_ingestao",                    F.lit(mes_corrente))
        .withColumn("dia_ingestao",                    F.lit(dia_corrente))
    )

# ============================================================
# EXECUÇÃO DO PIPELINE
# ============================================================
if ENTIDADE == "":
    tabelas_para_processar = ["alunos", "meta_alfabetizacao_brasil", "meta_alfabetizacao_uf", "meta_alfabetizacao_municipio", "municipio", "uf"]
else:
    tabelas_para_processar = [ENTIDADE]

for entidade_atual in tabelas_para_processar:
    
    BRONZE_PATH = f"s3://{BUCKET_PRINCIPAL}/{PASTA_BRONZE}/{entidade_atual}/anomesdia={ANOMESDIA}/"
    SILVER_PATH = f"s3://{BUCKET_PRINCIPAL}/{PASTA_SILVER}/{entidade_atual}/"
    
    log.info("-" * 60)
    log.info(f"🚀 [LOOP] Processando Entidade: {entidade_atual}")
    log.info(f"Lendo de: {BRONZE_PATH}")
    log.info(f"Gravando em: {SILVER_PATH}")
    
    try:
        df_bronze = spark.read.parquet(BRONZE_PATH)
        registros_bronze = df_bronze.count()
        log.info(f"📊 Registros encontrados na Bronze: {registros_bronze}")
                    
        if registros_bronze == 0:
            log.warning(f"⚠️ Lote vazio para {entidade_atual}. Pulando...")
            continue
            
        # 1. Processamento Unificado 
        df_silver = construir_silver(df_bronze, entidade_atual)
        
        # 2. Persistir em cache temporário (Otimiza os múltiplos testes de qualidade subsequentes)
        df_silver.persist()

        # 3. Executar Validação de Qualidade [INCLUÍDO]
        checks = CHECKS.get(entidade_atual, [])
        if checks:
            checar_qualidade(df_silver, checks)
        else:
            log.warning(f"[DQ:SILVER] Nenhuma regra definida para '{entidade_atual}' — pulando verificacao")

        # 4. Gravação unificada criando partições no seu bucket S3
        df_silver.write \
            .partitionBy("ano_ingestao", "mes_ingestao", "dia_ingestao") \
            .mode("overwrite") \
            .parquet(SILVER_PATH)
            
        log.info(f"✅ Concluído com sucesso: {entidade_atual}")

    except Exception as e:
        log.error(f"❌ Erro ao processar a tabela {entidade_atual}: {str(e)}")
        
    finally:
        # Liberação obrigatória da memória cache do DataFrame para não causar vazamento de memória
        if 'df_silver' in locals():
            df_silver.unpersist()

# ============================================================
# FINALIZAÇÃO E ACIONAMENTO DO CRAWLER
# ============================================================
try:
    job.commit()
    log.info("🚀 Disparando o Glue Crawler para atualizar o Catálogo...")
    glue_client = boto3.client('glue', region_name='us-east-1')
    glue_client.start_crawler(Name='silver-crawler')
    log.info("✅ Crawler iniciado com sucesso!")
except Exception as e:
    log.error(f"⚠️ Falha ao disparar o Crawler: {str(e)}")