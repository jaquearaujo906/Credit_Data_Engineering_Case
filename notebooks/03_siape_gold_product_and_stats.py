# Databricks notebook source
# MAGIC %run ./00_siape_config_and_utils

# COMMAND ----------

# 03_siape_gold_product_and_stats
# GOLD: consolidar SIAPE no grão cpf + posicao e gerar estatísticas descritivas
from pyspark.sql import functions as F


rem = spark.table(f"{SCHEMA}.silver_siape_remuneracao")
cad = spark.table(f"{SCHEMA}.silver_siape_cadastro")
afa = spark.table(f"{SCHEMA}.silver_siape_afastamento")
obs = spark.table(f"{SCHEMA}.silver_siape_observacoes")


# Define quais posições entram no recorte do case
# pega a maior posicao disponível e volta 3 meses
max_pos = rem.agg(F.max("posicao").alias("max_pos")).collect()[0]["max_pos"]

# se por algum motivo nao tiver nada, melhor falhar cedo
if not max_pos:
    raise Exception("Nao encontrei posicao na silver_siape_remuneracao. Verifique se o notebook 02 rodou.")

pos_min = (
    spark.sql(f"SELECT date_format(add_months(to_date('{max_pos}-01'), -3), 'yyyy-MM') AS p")
         .collect()[0]["p"]
)

print(f"Recorte do case: {pos_min} até {max_pos} (M-2 + 3 meses anteriores)")

# filtra tudo nesse intervalo
rem = rem.where((F.col("posicao") >= pos_min) & (F.col("posicao") <= max_pos))
cad = cad.where((F.col("posicao") >= pos_min) & (F.col("posicao") <= max_pos))
afa = afa.where((F.col("posicao") >= pos_min) & (F.col("posicao") <= max_pos))
obs = obs.where((F.col("posicao") >= pos_min) & (F.col("posicao") <= max_pos))

# Agregações antes do join pra não duplicar linha na gold
afa_agg = (
    afa.groupBy("cpf_norm", "posicao")
       .agg(
           F.max("tem_afastamento").alias("tem_afastamento"),
           F.count("*").alias("qtd_registros_afastamento")
       )
)

# observações idem -> vira contagem
obs_agg = (
    obs.groupBy("cpf_norm", "posicao")
       .agg(F.count("*").alias("qtd_observacoes"))
)

# Seleciona colunas essenciais 
# cadastro: só preciso do grupo ocupacional pro case
cad_keep = [c for c in ["cpf_norm", "posicao", "grupo_ocupacao"] if c in cad.columns]
cad_sel = cad.select(*cad_keep).dropDuplicates(["cpf_norm", "posicao"])

# remuneração: mantenho só cpf, posicao e campos monetários já convertidos (*_num)
rem_keep = [c for c in rem.columns if c in ["cpf_norm", "posicao"] or c.endswith("_num")]
rem_sel = rem.select(*rem_keep).dropDuplicates(["cpf_norm", "posicao"])


# Join final 
gold = (
    rem_sel
    .join(cad_sel, ["cpf_norm", "posicao"], "left")
    .join(afa_agg, ["cpf_norm", "posicao"], "left")
    .join(obs_agg, ["cpf_norm", "posicao"], "left")
)

# valores faltantes -> 0 (facilita consumo)
gold = (
    gold
    .withColumn("tem_afastamento", F.coalesce(F.col("tem_afastamento"), F.lit(0)))
    .withColumn("qtd_registros_afastamento", F.coalesce(F.col("qtd_registros_afastamento"), F.lit(0)))
    .withColumn("qtd_observacoes", F.coalesce(F.col("qtd_observacoes"), F.lit(0)))
)

gold = (
    gold
    .withColumn("source_remuneracao", F.lit("siape/remuneracao"))
    .withColumn("source_cadastro", F.lit("siape/cadastro"))
    .withColumn("source_afastamento", F.lit("siape/afastamento"))
    .withColumn("source_observacoes", F.lit("siape/observacoes"))
    .withColumn("gold_created_ts", F.current_timestamp())
)


# Salva a tabela final
(
    gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("posicao")
    .saveAsTable(f"{SCHEMA}.gold_siape_servidor_mensal")
)

print("Gold salva:", f"{SCHEMA}.gold_siape_servidor_mensal")
display(gold.limit(10))

# Estatísticas descritivas
g = spark.table(f"{SCHEMA}.gold_siape_servidor_mensal")

# estatísticas básicas pra validar estrutura e qualidade
stats = g.agg(
    F.count("*").alias("rows"),
    F.countDistinct("cpf_norm").alias("cpfs_unicos"),
    F.sum(F.when(F.col("grupo_ocupacao").isNull(), 1).otherwise(0)).alias("null_grupo_ocupacao"),
    F.sum(F.when(F.col("tem_afastamento").isNull(), 1).otherwise(0)).alias("null_tem_afastamento"),
    F.sum(F.when(F.col("qtd_observacoes").isNull(), 1).otherwise(0)).alias("null_qtd_observacoes"),
)
display(stats)

if "grupo_ocupacao" in g.columns:
    display(
        g.groupBy("grupo_ocupacao").count().orderBy(F.desc("count")).limit(15)
    )