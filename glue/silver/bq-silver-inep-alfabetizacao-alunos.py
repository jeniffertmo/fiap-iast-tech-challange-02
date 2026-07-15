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

JOB_NAME = args_disponiveis['JOB_NAME'] if args_disponiveis['JOB_NAME'] else "bq-silver-inep-alfabetizacao-alunos"
ENTIDADE = args_disponiveis['ENTIDADE']

job.init(JOB_NAME, args_disponiveis)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
spark.sparkContext.setLogLevel("WARN")


# ============================================================
# VARIÁVEIS DE AMBIENTE E MAPEAMENTOS
# ============================================================
BUCKET_PRINCIPAL  = "fiap-datalake-tech"  
PASTA_BRONZE      = "bronze"
PASTA_SILVER      = "silver"

ANOMESDIA = "20260713" 

agora_utc = datetime.now(timezone.utc)
ano_corrente = agora_utc.strftime("%Y")
mes_corrente = agora_utc.strftime("%m")
dia_corrente = agora_utc.strftime("%d")

CHAVES_NEGOCIO_SILVER = {
    "alunos":                    ["ano", "id_aluno"],
    "meta_alfabetizacao_brasil":    ["ano", "rede"],
    "meta_alfabetizacao_uf":        ["ano", "sigla_uf"],
    "meta_alfabetizacao_municipio": ["ano", "id_municipio"],
    "municipio":                    ["ano", "id_municipio"],
    "uf":                            ["ano", "sigla_uf"]
}

log.info("=" * 60)
log.info(f"JOB       : {JOB_NAME}")
log.info(f"ENTIDADE  : {ENTIDADE}")
log.info(f"DATA LOTE : {ANOMESDIA}")
log.info("=" * 60)


# ============================================================
# REGRAS DE QUALIDADE (DATA QUALITY)
# ============================================================
CHECKS = {
    "alunos": [
        {"tipo": "min_count", "valor": 1,              "critico": True},
        {"tipo": "not_null",  "coluna": "id_aluno",     "critico": True},
        {"tipo": "not_null",  "coluna": "ano",    "critico": True}
    ],
    "meta_alfabetizacao_brasil": [
        {"tipo": "min_count", "valor": 1,              "critico": True},
        {"tipo": "not_null",  "coluna": "ano",          "critico": True},
        {"tipo": "not_null",  "coluna": "rede",         "critico": True}
    ],
    "meta_alfabetizacao_uf": [
        {"tipo": "min_count", "valor": 1,              "critico": True},
        {"tipo": "not_null",  "coluna": "sigla_uf",     "critico": True},
        {"tipo": "regex",     "coluna": "sigla_uf", "valor": r"^[A-Z]{2}$", "critico": False}
    ],
    "meta_alfabetizacao_municipio": [
        {"tipo": "min_count", "valor": 1,              "critico": True},
        {"tipo": "not_null",  "coluna": "id_municipio", "critico": True}
    ],
    "municipio": [
        {"tipo": "min_count", "valor": 1,              "critico": True},
        {"tipo": "not_null",  "coluna": "id_municipio", "critico": True},
        {"tipo": "unique",    "coluna": "id_municipio", "critico": True}
    ],
    "uf": [
        {"tipo": "min_count", "valor": 1,              "critico": True},
        {"tipo": "not_null",  "coluna": "sigla_uf",     "critico": True},
        {"tipo": "regex",     "coluna": "sigla_uf", "valor": r"^[A-Z]{2}$", "critico": True}
    ]
}


# ============================================================
# FUNÇÕES DE QUALIDADE E TRATAMENTO
# ============================================================

def checar_qualidade(df, checks, entidade_atual):
    log.info(f"[DQ:SILVER] Iniciando verificacoes para '{entidade_atual}' | checks={len(checks)}")
    passou = falhou = criticos = 0
    total_linhas = None

    for check in checks:
        tipo    = check["tipo"]
        coluna  = check.get("coluna")
        valor   = check.get("valor")
        critico = check.get("critico", True)
        ok      = False
        detalhe = ""

        try:
            if tipo == "not_null":
                nulos   = df.filter(F.col(coluna).isNull() | (F.col(coluna) == "N/A")).count()
                ok      = nulos == 0
                detalhe = f"{nulos} nulos/NA encontrados"
            elif tipo == "min_count":
                if total_linhas is None:
                    total_linhas = df.count()
                ok       = total_linhas >= valor
                detalhe  = f"contagem={total_linhas} | minimo={valor}"
            elif tipo == "unique":
                duplicados_df = df.groupBy(coluna).count().filter("count > 1")
                has_dups      = not duplicados_df.isEmpty()
                ok            = not has_dups
                detalhe       = "Duplicatas encontradas na chave informada" if has_dups else "Nenhuma duplicata"
            elif tipo == "regex":
                invalidos = df.filter(
                    F.col(coluna).isNotNull() & (F.col(coluna) != "N/A") & ~F.col(coluna).rlike(valor)
                ).count()
                ok      = invalidos == 0
                detalhe = f"{invalidos} registros fora do padrao regex esperado"
            elif tipo == "range":
                mn, mx = valor
                fora   = df.filter((F.col(coluna) < mn) | (F.col(coluna) > mx)).count()
                ok      = fora == 0
                detalhe = f"{fora} fora do intervalo [{mn},{mx}]"
        except Exception as e:
            ok      = False
            detalhe = f"Erro ao executar o check: {e}"

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
    log.info(f"[DQ:SILVER] Finalizado '{entidade_atual}' -> Score={score}% | PASS={passou} FAIL={falhou}")

    if criticos > 0:
        raise Exception(f"[DQ:SILVER] {criticos} check(s) critico(s) falharam na entidade '{entidade_atual}'. Job abortado.")


def colunas_para_minusculo(df):
    """Converte o nome de todas as colunas do DataFrame para letras minúsculas."""
    log.info("[TRANSFORMAÇÃO] Convertendo todas as colunas para minúsculo.")
    for col_name in df.columns:
        if col_name != col_name.lower():
            df = df.withColumnRenamed(col_name, col_name.lower())
    return df


def transformar_entidade_unificado(df, entidade_atual):
    log.info(f"[SILVER] Aplicando regras de tratamento para: {entidade_atual}")
    
    # --- 1. Tratamento específico utilizando os nomes já padronizados (Silver) ---
    if entidade_atual == "alunos":
        colunas_para_dropar = [
            "co_bloco_1", "tx_resposta_bloco_1", "tx_gabarito_bloco_1",
            "co_bloco_2", "tx_resposta_bloco_2", "tx_gabarito_bloco_2",
            "co_bloco_3", "tx_resposta_bloco_3", "tx_gabarito_bloco_3",
            "co_bloco_4", "tx_resposta_bloco_4", "tx_gabarito_bloco_4"
        ]
        colunas_existentes_drop = [c for c in colunas_para_dropar if c in df.columns]
        if colunas_existentes_drop:
            log.info(f"[TRATAMENTO] Removendo colunas irrelevantes para alunos: {colunas_existentes_drop}")
            df = df.drop(*colunas_existentes_drop)

        colunas_para_inteiro = ["id_escola", "tp_dependencia", "id_municipio", "ano"]
        log.info(f"[TRATAMENTO] Aplicando cast para Inteiro nas colunas de alunos: {colunas_para_inteiro}")
        for col_num in colunas_para_inteiro:
            if col_num in df.columns:
                df = df.withColumn(col_num, F.col(col_num).cast("int"))
                
    elif entidade_atual in ["meta_alfabetizacao_brasil", "meta_alfabetizacao_uf", "meta_alfabetizacao_municipio", "municipio", "uf"]:
        log.info(f"[TRATAMENTO] Aplicando cast para String nas colunas específicas da entidade {entidade_atual}")
        for col_cast, col_tipo in [("rede", "string"), ("sigla_uf", "string"), ("nome_municipio", "string")]:
            if col_cast in df.columns:
                df = df.withColumn(col_cast, F.col(col_cast).cast(col_tipo))

     # --- 2. Padronização de tipos String ---
    log.info("[TRATAMENTO] Padronizando tipos de dados String na schema.")
    df = df.select(*[F.col(c.name).cast(StringType()).alias(c.name) if isinstance(c.dataType, StringType) else F.col(c.name) for c in df.schema])

    # --- 3. Tratamento de Strings Vazias e Nulos ---
    colunas_string = [c.name for c in df.schema.fields if isinstance(c.dataType, StringType)]
    log.info(f"[TRATAMENTO] Convertendo strings vazias para None e aplicando preenchimento 'N/A' em {len(colunas_string)} colunas textuais.")
    for col_name in colunas_string:
        df = df.withColumn(col_name, F.when(F.trim(F.col(col_name)) == "", F.lit(None)).otherwise(F.col(col_name)))
    df = df.fillna("N/A", subset=colunas_string)
    
    # --- 4. Tratamento Numérico Geral ---
    colunas_numericas = [c.name for c in df.schema.fields if any(t in c.dataType.simpleString() for t in ["double", "float", "int"])]
    log.info(f"[TRATAMENTO] Tratando valores nulos/vazios e aplicando arredondamento em {len(colunas_numericas)} colunas numéricas.")
    for col_num in colunas_numericas:
        df = df.withColumn(col_num, F.when(F.col(col_num).isNull() | (F.trim(F.col(col_num).cast("string")) == ""), F.lit(None)).otherwise(F.round(F.col(col_num), 2)))
        
    return df


# ============================================================
# ORQUESTRAÇÃO DA TRANSFORMAÇÃO
# ============================================================
def construir_silver(df_bronze, entidade_atual):
    log.info(f"===> [DIAGNÓSTICO: {entidade_atual}] Iniciando construção da Silver.")
    
    colunas_para_remover = [c for c in ["mes", "dia", "ANOMESDIA", "anomesdia"] if c in df_bronze.columns]
    if colunas_para_remover:
        log.info(f"[ETAPA 1] Removendo colunas de partição antigas: {colunas_para_remover}")
        df = df_bronze.drop(*colunas_para_remover)
    else:
        df = df_bronze
    
    # --- ETAPA 1: RENOMEIO  ---
    de_para_nomes = {
        "nu_ano_avaliacao": "ano",
        "sg_uf":            "sigla_uf",
        "co_uf":            "id_uf",
        "co_municipio":     "id_municipio",
        "no_municipio":     "nome_municipio"
    }
    
    colunas_encontradas_para_renomear = [antigo for antigo in de_para_nomes.keys() if antigo in df.columns]
    if colunas_encontradas_para_renomear:
        log.info(f"[ETAPA 1] Renomeando colunas de negócio para o padrão Silver: {colunas_encontradas_para_renomear}")
        for nome_antigo, nome_novo in de_para_nomes.items():
            if nome_antigo in df.columns:
                df = df.withColumnRenamed(nome_antigo, nome_novo)

    # Executa a limpeza utilizando as colunas já renomeadas para o padrão Silver
    df = transformar_entidade_unificado(df, entidade_atual)
    
    # --- ETAPA 2: DEDUPLICAÇÃO ---
    chaves = CHAVES_NEGOCIO_SILVER.get(entidade_atual)
    if chaves:
        chaves_existentes = [c for c in chaves if c in df.columns]
        if chaves_existentes:
            log.info(f"[ETAPA 2] Iniciando processo de deduplicação utilizando as chaves: {chaves_existentes}")
            
            # Persiste temporariamente para evitar reprocessamento no Spark ao rodar os dois counts
            df.cache()
            antes = df.count()
            
            # Aplica a limpeza de chaves nulas e duplicadas
            df = df.dropna(subset=chaves_existentes)
            df = df.dropDuplicates(chaves_existentes)
            
            depois = df.count()
            linhas_removidas = antes - depois
            log.info(f"[ETAPA 2] Deduplicação concluída para '{entidade_atual}'. Linhas antes: {antes} | Linhas depois: {depois} | Removidas: {linhas_removidas}")
            
            # Libera a memória após a contagem
            df.unpersist()
        else:
            log.warning(f"[ETAPA 2] As chaves de negócio {chaves} não foram encontradas no DataFrame atual.")
    else:
        log.warning(f"[ETAPA 2] Nenhuma chave de negócio cadastrada para a entidade '{entidade_atual}'. Ignorando deduplicação.")

    # Ingestão de partições limpas que vão gerar a estrutura física de pastas no S3
    log.info("[ETAPA 3] Inserindo metadados de controle e colunas de partição temporal da Silver.")
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
    
    if entidade_atual == "alunos":
        BRONZE_PATH = f"s3://{BUCKET_PRINCIPAL}/{PASTA_BRONZE}/alunos_*/*/"
    else:
        BRONZE_PATH = f"s3://{BUCKET_PRINCIPAL}/{PASTA_BRONZE}/{entidade_atual}/anomesdia={ANOMESDIA}/"
        
    SILVER_PATH = f"s3://{BUCKET_PRINCIPAL}/{PASTA_SILVER}/{entidade_atual}/"
    
    log.info("-" * 60)
    log.info(f"🚀 [LOOP] Processando Entidade: {entidade_atual}")
    
    try:
        log.info(f"[LEITURA] Lendo dados parquet da origem Bronze: {BRONZE_PATH}")
        df_bronze = spark.read.parquet(BRONZE_PATH)
        
        # Padroniza todas as colunas de origem para minúsculo antes de iniciar qualquer lógica
        df_bronze = colunas_para_minusculo(df_bronze)
        
        registros_bronze = df_bronze.count()
        log.info(f"📊 Total de registros lidos da Bronze: {registros_bronze}")
              
        if registros_bronze == 0:
            log.warning(f"⚠️ Entidade '{entidade_atual}' está vazia no lote atual. Pulando para a próxima.")
            continue
            
        # Processamento Unificado 
        df_silver = construir_silver(df_bronze, entidade_atual)
        
        # --- ETAPA ADICIONADA: PERFORMANCE & DATA QUALITY ---
        # Cacheamos o DataFrame antes de rodar os testes de DQ para evitar ler do S3 várias vezes
        df_silver.cache()
        
        checks = CHECKS.get(entidade_atual, [])
        if checks:
            checar_qualidade(df_silver, checks, entidade_atual)
        else:
            log.warning(f"[DQ:SILVER] Nenhuma regra de DQ encontrada para '{entidade_atual}'. Pulando verificacao.")

        if entidade_atual == "alunos":
            log.info("[PARTICIONAMENTO] Aplicando repartition para otimizar escrita da tabela alunos.")
            df_silver = df_silver.repartition("ano_ingestao", "mes_ingestao", "dia_ingestao")
            
        # Gravação limpa com as novas colunas utilizando sobregravação nativa
        log.info(f"[ESCRITA] Gravando dados limpos no S3 (Silver): {SILVER_PATH}")
        df_silver.write \
            .partitionBy("ano_ingestao", "mes_ingestao", "dia_ingestao") \
            .mode("overwrite") \
            .parquet(SILVER_PATH)
            
        # Removemos o dataframe da memória após a persistência bem-sucedida no S3
        df_silver.unpersist()
            
        log.info(f"✅ Concluído com sucesso: {entidade_atual}")

    except Exception as e:
        log.error(f"❌ Erro crítico ao processar a tabela {entidade_atual}: {str(e)}", exc_info=True)

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