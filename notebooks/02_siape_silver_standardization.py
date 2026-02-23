# Databricks notebook source
# MAGIC %run ./00_siape_config_and_utils

# COMMAND ----------

# 02_siape_silver_standardization
# SILVER: padronizar dados pra facilitar join no gold

cpf_candidates = ["cpf", "cpf_servidor", "cpf_beneficiario"]
id_candidates  = ["id_servidor_portal", "id_servidor", "id_servidor_portal_transparencia"]


# REMUNERAÇÃO
def build_silver_remuneracao(posicao: str):
    # pega só o snapshot daquele mês
    b = spark.table(f"{SCHEMA}.bronze_siape_remuneracao") \
             .where(F.col("posicao") == posicao)

    df = b

    # normaliza cpf pra usar como chave no gold
    cpf_col = pick_col(b, cpf_candidates)
    if cpf_col:
        df = df.withColumn("cpf_norm", normalize_cpf(F.col(cpf_col)))

    # converte valores monetários (se existirem no arquivo)
    money_candidates = [
        "remuneracao_basica_bruta_r",
        "remuneracao_apos_deducoes_obrigatorias_r",
        "irrf_r",
        "demais_deducoes_r",
        "total_de_verbas_indenizatorias_r",
    ]

    for c in money_candidates:
        if c in df.columns:
            df = df.withColumn(c + "_num", to_decimal_ptbr(F.col(c)))

    # garante um registro por servidor por mês
    if "cpf_norm" in df.columns:
        df = df.dropDuplicates(["cpf_norm", "posicao"])

    (
        df.write.format("delta")
        .mode("append")
        .partitionBy("posicao")
        .saveAsTable(f"{SCHEMA}.silver_siape_remuneracao")
    )


# CADASTRO
def build_silver_cadastro(posicao: str):
    b = spark.table(f"{SCHEMA}.bronze_siape_cadastro") \
             .where(F.col("posicao") == posicao)

    df = b

    # cria chave padronizada
    cpf_col = pick_col(b, cpf_candidates)
    if cpf_col:
        df = df.withColumn("cpf_norm", normalize_cpf(F.col(cpf_col)))

    # tenta identificar grupo ocupacional
    cargo_candidates = ["cargo", "descricao_cargo", "funcao", "descricao_funcao", "ocupacao"]
    cargo_col = pick_col(df, cargo_candidates)

    if cargo_col:
        df = df.withColumn("grupo_ocupacao", F.upper(F.col(cargo_col)))

    if "cpf_norm" in df.columns:
        df = df.dropDuplicates(["cpf_norm", "posicao"])

    (
        df.write.format("delta")
        .mode("append")
        .partitionBy("posicao")
        .saveAsTable(f"{SCHEMA}.silver_siape_cadastro")
    )


# AFASTAMENTO
def build_silver_afastamento(posicao: str):
    b = spark.table(f"{SCHEMA}.bronze_siape_afastamento") \
             .where(F.col("posicao") == posicao)

    df = b

    cpf_col = pick_col(b, cpf_candidates)
    if cpf_col:
        df = df.withColumn("cpf_norm", normalize_cpf(F.col(cpf_col)))

    # marca que o servidor teve afastamento naquele mês
    df = df.withColumn("tem_afastamento", F.lit(1))

    (
        df.write.format("delta")
        .mode("append")
        .partitionBy("posicao")
        .saveAsTable(f"{SCHEMA}.silver_siape_afastamento")
    )


# OBSERVAÇÕES
def build_silver_observacoes(posicao: str):
    b = spark.table(f"{SCHEMA}.bronze_siape_observacoes") \
             .where(F.col("posicao") == posicao)

    df = b

    cpf_col = pick_col(b, cpf_candidates)
    if cpf_col:
        df = df.withColumn("cpf_norm", normalize_cpf(F.col(cpf_col)))

    (
        df.write.format("delta")
        .mode("append")
        .partitionBy("posicao")
        .saveAsTable(f"{SCHEMA}.silver_siape_observacoes")
    )


# execução
for pos in POSITIONS:
    print(f"SILVER | posicao={pos}")

    build_silver_remuneracao(pos)
    print("OK silver remuneracao")

    build_silver_cadastro(pos)
    print("OK silver cadastro")

    build_silver_afastamento(pos)
    print("OK silver afastamento")

    build_silver_observacoes(pos)
    print("OK silver observacoes")

print("\nSilver finalizado para todas as posições.")