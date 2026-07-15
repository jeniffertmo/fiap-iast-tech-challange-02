import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# ==============================================================================
# PARÂMETROS COM CAPTURA SEGURA
# ==============================================================================
# Lista com todos os parâmetros que você espera que o Job possa receber
parametros_esperados = ["JOB_NAME", "ALUNOS", "BUCKET_PRINCIPAL", "PASTA_SILVER", "PASTA_GOLD"]

# Filtra apenas os parâmetros que de fato foram passados na linha de comando/execução
argumentos_reais = [p for p in parametros_esperados if any(arg.startswith(f"--{p}") for arg in sys.argv)]

# Valores padrão de fallback caso o script seja rodado fora do Glue (ex: Notebook local)
args_resolvidos = {
    "JOB_NAME": "bq-gold-inep-alfabetizacao",
    "ALUNOS": "alunos",
    "BUCKET_PRINCIPAL": "fiap-datalake-tech",
    "PASTA_SILVER": "silver",
    "PASTA_GOLD": "gold"
}

# Se houver parâmetros reais passados, resolvemos eles dinamicamente
if argumentos_reais:
    try:
        valores_capturados = getResolvedOptions(sys.argv, argumentos_reais)
        for k, v in valores_capturados.items():
            args_resolvidos[k] = v
    except Exception as e:
        print(f"⚠️ Falha ao resolver parâmetros do Glue: {str(e)}. Usando padrões.")

# Atribuição das variáveis baseada no dicionário resolvido com segurança
JOB_NAME         = args_resolvidos["JOB_NAME"]
ALUNOS           = args_resolvidos["ALUNOS"]
BUCKET_PRINCIPAL = args_resolvidos["BUCKET_PRINCIPAL"]
PASTA_SILVER     = args_resolvidos["PASTA_SILVER"]
PASTA_GOLD       = args_resolvidos["PASTA_GOLD"]

S3_SILVER = f"s3://{BUCKET_PRINCIPAL}/{PASTA_SILVER}"
S3_GOLD   = f"s3://{BUCKET_PRINCIPAL}/{PASTA_GOLD}"

# Inicialização do Contexto Spark e Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Inicialização oficial do Job do Glue utilizando o JOB_NAME resolvido com segurança
job = Job(glueContext)
job.init(JOB_NAME, args_resolvidos)

# ============================================================
# PASSO 1: REGRAS DE QUALIDADE PARA AS VISÕES GOLD (DQ)
# ============================================================
CHECKS_GOLD = {
    "indicador_alfabetizacao_municipio": [
        {"tipo": "min_count", "valor": 1,                               "critico": True},
        {"tipo": "range",     "coluna": "taxa_alfabetizacao_real", "valor": (0.0, 100.0), "critico": True},
    ],
    "comparativo_metas_resultados": [
        {"tipo": "min_count", "valor": 1,                               "critico": True},
        {"tipo": "not_null",  "coluna": "status_meta",                  "critico": True},
    ],
    "evolucao_temporal_estado": [
        {"tipo": "min_count", "valor": 1,                               "critico": True},
        {"tipo": "regex",     "coluna": "sigla_uf", "valor": r"^[A-Z]{2}$", "critico": False},
        {"tipo": "range",     "coluna": "taxa_alfabetizacao_estado", "valor": (0.0, 100.0), "critico": True},
    ],
}

# ============================================================
# PASSO 2: FUNÇÃO EXECUTORA DE DATA QUALITY
# ============================================================
def checar_qualidade(df, checks, nome_visao):
    print(f"🔍 [DQ:GOLD] Verificando qualidade da visão '{nome_visao}' | {len(checks)} teste(s)")
    passou = falhou = criticos = 0

    for check in checks:
        tipo    = check["tipo"]
        coluna  = check.get("coluna")
        valor   = check.get("valor")
        critico = check.get("critico", True)
        ok      = False
        detalhe = ""

        if coluna and coluna not in df.columns:
            print(f"⚠️ [DQ:GOLD] Coluna '{coluna}' não encontrada no schema. Pulando check '{tipo}'.")
            continue

        try:
            if tipo == "not_null":
                nulos   = df.filter(F.col(coluna).isNull() | (F.col(coluna) == "N/A")).count()
                ok      = nulos == 0
                detalhe = f"{nulos} nulos/NA encontrados"
            elif tipo == "min_count":
                contagem = df.count()
                ok       = contagem >= valor
                detalhe  = f"contagem={contagem} | mínimo esperado={valor}"
            elif tipo == "unique":
                dups    = df.count() - df.select(coluna).distinct().count()
                ok      = dups == 0
                detalhe = f"{dups} duplicatas encontradas"
            elif tipo == "regex":
                invalidos = df.filter(
                    F.col(coluna).isNotNull() & (F.col(coluna) != "N/A") & ~F.col(coluna).rlike(valor)
                ).count()
                ok      = invalidos == 0
                detalhe = f"{invalidos} registros fora do padrão regex"
            elif tipo == "range":
                mn, mx = valor
                fora   = df.filter((F.col(coluna) < mn) | (F.col(coluna) > mx)).count()
                ok      = fora == 0
                detalhe = f"{fora} registros fora do intervalo [{mn}, {mx}]"
        except Exception as e:
            ok      = False
            detalhe = f"Erro ao processar validação: {e}"

        status = "PASS" if ok else ("FAIL" if critico else "WARN")
        if ok:
            passou += 1
            print(f"  ✅ {status} | check={tipo} | coluna={coluna if coluna else 'N/A'} | {detalhe}")
        else:
            falhou += 1
            if critico:
                criticos += 1
                print(f"  ❌ {status} (CRÍTICO) | check={tipo} | coluna={coluna if coluna else 'N/A'} | {detalhe}")
            else:
                print(f"  ⚠️ {status} (AVISO) | check={tipo} | coluna={coluna if coluna else 'N/A'} | {detalhe}")

    score = round(passou / len(checks) * 100, 1) if checks else 100.0
    print(f"📊 [DQ:GOLD] Placar final para {nome_visao}: Score={score}% | PASS={passou} | FAIL={falhou}")

    if criticos > 0:
        raise Exception(f"🚨 [DQ:GOLD] {criticos} check(s) crítico(s) falharam para a visão '{nome_visao}'. Escrita abortada!")

# ==============================================================================
# LEITURA DIRETA DO S3 (SILVER) E PROCESSAMENTO DAS VISÕES GOLD
# ==============================================================================
# Correções aplicadas:
#   ✅ Removeu anomesdia=... dos paths (causava PATH_NOT_FOUND)
#   ✅ Leitura única de silver/alunos/ → filtro por NU_ANO_AVALIACAO
#   ✅ Nome correto da meta: meta_alfabetizacao_municipio
# ==============================================================================

print("📡 Lendo camada Silver diretamente do S3...")

# Leitura CORRIGIDA — sem partição anomesdia no path
df_alunos_all = spark.read.parquet(f"{S3_SILVER}/alunos")
df_meta_muni  = spark.read.parquet(f"{S3_SILVER}/meta_alfabetizacao_municipio")

# Filtro de alunos presentes
df_base = df_alunos_all.filter(F.col("in_presenca_lp") == 1)

# Filtro seguro: Remove registros sem município válido antes de agregar
# Remove nulos estruturais do Spark e eventuais strings "N/A"
df_base = df_base.filter(
    F.col("id_municipio").isNotNull() & 
    (~F.col("id_municipio").cast("string").isin("N/A", "", "NaN"))
)

# ─── VISÃO 1: Indicador de Alfabetização por Município ────────────
print("📊 Criando visão: indicador_alfabetizacao_municipio...")
df_indicador = (
    df_base
    .groupBy("ano", "sigla_uf", "id_municipio", "nome_municipio")
    .agg(
        F.count("id_aluno").alias("total_avaliados"),
        F.sum(
            F.when(
                (F.col("in_alfabetizado") == 1) | (F.col("in_alfabetizado") == "1"), 1
            ).otherwise(0)
        ).alias("total_alfabetizados")
    )
    .withColumn(
        "taxa_alfabetizacao_real",
        F.round((F.col("total_alfabetizados") / F.col("total_avaliados")) * 100, 2)
    )
)

# ─── VISÃO 2: Comparativo Metas vs Resultados ─────────────────────
print("🎯 Criando visão: comparativo_metas_resultados...")
df_metas_ajustadas = df_meta_muni.select(
    F.col("id_municipio").cast("string").alias("id_municipio"),
    F.col("meta_alfabetizacao_2024").alias("meta_2024"),
    F.col("meta_alfabetizacao_2025").alias("meta_2025")
).distinct()

df_comparativo = (
    df_indicador
    .join(df_metas_ajustadas, "id_municipio", "left")
    .withColumn(
        "meta_ano",
        F.when(F.col("ano") == 2024, F.col("meta_2024"))
         .when(F.col("ano") == 2025, F.col("meta_2025"))
    )
    .withColumn(
        "desvio_meta",
        F.round(F.col("taxa_alfabetizacao_real") - F.col("meta_ano"), 2)
    )
    .withColumn(
        "status_meta",
        F.when(F.col("meta_ano").isNull(), "Sem Meta Definida")
         .when(F.col("desvio_meta") >= 0, "Atingiu a Meta")
         .otherwise("Abaixo da Meta")
    )
    .select(
        "ano", "sigla_uf", "id_municipio", "nome_municipio",
        "total_avaliados", "taxa_alfabetizacao_real",
        "meta_ano", "desvio_meta", "status_meta"
    )
)

# ─── VISÃO 3: Evolução Temporal por Estado ─────────────────────────
print("📈 Criando visão: evolucao_temporal_estado...")
df_evolucao = (
    df_base
    .groupBy("ano", "sigla_uf")
    .agg(
        F.count("id_aluno").alias("total_avaliados_estado"),
        F.sum(
            F.when(
                (F.col("in_alfabetizado") == 1) | (F.col("in_alfabetizado") == "1"), 1
            ).otherwise(0)
        ).alias("total_alfabetizados_estado")
    )
    .withColumn(
        "taxa_alfabetizacao_estado",
        F.round((F.col("total_alfabetizados_estado") / F.col("total_avaliados_estado")) * 100, 2)
    )
    .orderBy("sigla_uf", "ano")
)

print("✅ Três visões Gold processadas em memória, prontas para escrita!")


# ==============================================================================
# VALIDAÇÃO DE QUALIDADE E ESCRITA DIRETA NO S3 (OTIMIZADA COM CACHE)
# ==============================================================================

# Dicionário com as 3 visões processadas
VISOES_GOLD = {
    "indicador_alfabetizacao_municipio": df_indicador,
    "comparativo_metas_resultados":    df_comparativo,
    "evolucao_temporal_estado":        df_evolucao,
}

print("💾 Iniciando validação de qualidade e escrita das visões Gold no S3...\n")

for nome_visao, df in VISOES_GOLD.items():
    print("-" * 60)
    
    # 1. Adiciona metadados de auditoria
    df_final = df \
        .withColumn("_gold_processed_at", F.current_timestamp()) \
        .withColumn("_analytics_version",  F.lit("v3.0_spark_native_s3"))

    try:
        # 2. Persiste em memória para evitar reprocessamento da árvore de execução (DAG)
        df_final.persist()

        # 3. Executa as validações específicas da visão analítica
        regras = CHECKS_GOLD.get(nome_visao, [])
        if regras:
            checar_qualidade(df_final, regras, nome_visao)
        else:
            print(f"⚠️ Nenhuma regra de qualidade cadastrada para '{nome_visao}'.")

        # 4. Gravação física direta no S3 se passou na qualidade
        s3_destino = f"{S3_GOLD}/{nome_visao}/"
        print(f"📤 Gravando dados aprovados em: {s3_destino}")
        
        df_final.write \
            .mode("overwrite") \
            .parquet(s3_destino)
            
        print(f"✅ Gravação de '{nome_visao}' finalizada com sucesso!")

    except Exception as e:
        print(f"❌ Falha crítica no processamento/qualidade de '{nome_visao}': {str(e)}")
        # Propaga o erro para parar o Glue Job caso um teste crítico falhe
        raise e
        
    finally:
        # 5. Liberação obrigatória do cache para liberar memória do cluster
        df_final.unpersist()

print(f"\n🏆 Camada Gold validada e gravada com sucesso em: {S3_GOLD}")


# ==============================================================================
# CÉLULA 4: VALIDAÇÃO DAS VISÕES GOLD (LEITURA DIRETA DO S3)
# ==============================================================================
# O que esta célula faz:
#   - Lê cada visão Gold DIRETO do S3 (confirma que a escrita e a persistência física funcionaram)
#   - Exibe schema, contagem de registros e amostra das primeiras linhas
# ==============================================================================

print("=" * 70)
print("🔍 VALIDAÇÃO DAS TABELAS GOLD GRAVADAS NO S3")
print("=" * 70)

VISOES_PARA_VALIDAR = [
    "indicador_alfabetizacao_municipio",
    "comparativo_metas_resultados",
    "evolucao_temporal_estado",
]

for nome_visao in VISOES_PARA_VALIDAR:
    s3_path = f"{S3_GOLD}/{nome_visao}/"

    print(f"\n📊 {nome_visao}")
    print(f"    Path: {s3_path}")

    try:
        # Lê direto do S3 os dados que foram aprovados pelo Data Quality
        df_val = spark.read.parquet(s3_path)
        total = df_val.count()

        print(f"    ✅ Total de registros físicos gravados: {total}")
        print(f"    📋 Schema físico final:")
        df_val.printSchema()
        print(f"    👀 Amostra física (3 linhas):")
        df_val.show(3, truncate=False)

    except Exception as e:
        # Importante: Como o Quality Check interrompe o pipeline em caso de erro crítico na Célula 3,
        # se chegar aqui com erro, indica falha de leitura ou que o bucket estava inacessível.
        print(f"    ❌ Erro ao validar persistência no S3: {str(e)}")

print("\n" + "=" * 70)
print("✅ Validação pós-escrita concluída!")
print("=" * 70)

job.commit()
