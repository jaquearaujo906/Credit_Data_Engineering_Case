# Databricks notebook source
# MAGIC %run ./00_siape_config_and_utils

# COMMAND ----------

# 01_siape_bronze_ingestion
# Bronze: pegar os CSV do RAW e salvar em Delta particionado
def write_bronze(domain: str, posicao: str, df):
    df_out = (
        df
        .withColumn("posicao", F.lit(posicao))
        .withColumn("source_domain", F.lit(domain))
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("ingestion_ts", F.current_timestamp())
    )

    table = f"{SCHEMA}.bronze_siape_{domain}"

    (
        df_out.write
        .format("delta")
        .mode("append")
        .partitionBy("posicao")
        .saveAsTable(table)
    )
    return table

for pos in POSITIONS:
    print(f"BRONZE | posicao={pos}")

    for domain in RAW_FILES.keys():
        path = raw_path(pos, domain)
        print("Reading:", path)

        df = read_raw_csv(path)
        print(domain, "cols:", len(df.columns))
        display(df.limit(2))

        t = write_bronze(domain, pos, df)
        print("Saved:", t)

print("\n Bronze finalizado para todas as posições.")
spark.sql(f"SHOW TABLES IN {SCHEMA}").show(truncate=False)


# COMMAND ----------

spark.sql("SHOW TABLES IN public_informations").show()

# COMMAND ----------

spark.table("public_informations.bronze_siape_cadastro").select("posicao","source_file").limit(5).show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM public_informations.bronze_siape_remuneracao
# MAGIC WHERE posicao = '2025-12';