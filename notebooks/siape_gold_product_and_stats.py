# 03_siape_gold_product_and_stats.py
from pyspark.sql import functions as F
from notebooks.siape_config_and_utils import spark, SCHEMA


def run_gold():
    rem = spark.table(f"{SCHEMA}.silver_siape_remuneracao")
    cad = spark.table(f"{SCHEMA}.silver_siape_cadastro")
    afa = spark.table(f"{SCHEMA}.silver_siape_afastamento")
    obs = spark.table(f"{SCHEMA}.silver_siape_observacoes")

    max_pos = rem.agg(F.max("posicao").alias("max_pos")).collect()[0]["max_pos"]

    if not max_pos:
        raise Exception("Nao encontrei posicao na silver_siape_remuneracao. Verifique se o notebook 02 rodou.")

    pos_min = (
        spark.sql(f"SELECT date_format(add_months(to_date('{max_pos}-01'), -3), 'yyyy-MM') AS p")
             .collect()[0]["p"]
    )

    print(f"Recorte do case: {pos_min} até {max_pos} (M-2 + 3 meses anteriores)")

    rem = rem.where((F.col("posicao") >= pos_min) & (F.col("posicao") <= max_pos))
    cad = cad.where((F.col("posicao") >= pos_min) & (F.col("posicao") <= max_pos))
    afa = afa.where((F.col("posicao") >= pos_min) & (F.col("posicao") <= max_pos))
    obs = obs.where((F.col("posicao") >= pos_min) & (F.col("posicao") <= max_pos))

    afa_agg = (
        afa.groupBy("cpf_norm", "posicao")
           .agg(
               F.max("tem_afastamento").alias("tem_afastamento"),
               F.count("*").alias("qtd_registros_afastamento")
           )
    )

    obs_agg = (
        obs.groupBy("cpf_norm", "posicao")
           .agg(F.count("*").alias("qtd_observacoes"))
    )

    cad_keep = [c for c in ["cpf_norm", "posicao", "grupo_ocupacao"] if c in cad.columns]
    cad_sel = cad.select(*cad_keep).dropDuplicates(["cpf_norm", "posicao"])

    rem_keep = [c for c in rem.columns if c in ["cpf_norm", "posicao"] or c.endswith("_num")]
    rem_sel = rem.select(*rem_keep).dropDuplicates(["cpf_norm", "posicao"])

    gold = (
        rem_sel
        .join(cad_sel, ["cpf_norm", "posicao"], "left")
        .join(afa_agg, ["cpf_norm", "posicao"], "left")
        .join(obs_agg, ["cpf_norm", "posicao"], "left")
    )

    gold = (
        gold
        .withColumn("tem_afastamento", F.coalesce(F.col("tem_afastamento"), F.lit(0)))
        .withColumn("qtd_registros_afastamento", F.coalesce(F.col("qtd_registros_afastamento"), F.lit(0)))
        .withColumn("qtd_observacoes", F.coalesce(F.col("qtd_observacoes"), F.lit(0)))
        .withColumn("source_remuneracao", F.lit("siape/remuneracao"))
        .withColumn("source_cadastro", F.lit("siape/cadastro"))
        .withColumn("source_afastamento", F.lit("siape/afastamento"))
        .withColumn("source_observacoes", F.lit("siape/observacoes"))
        .withColumn("gold_created_ts", F.current_timestamp())
    )

    (
        gold.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("posicao")
        .saveAsTable(f"{SCHEMA}.gold_siape_servidor_mensal")
    )

    print("Gold salva:", f"{SCHEMA}.gold_siape_servidor_mensal")

    # Estatísticas descritivas — substituímos display() por .show()
    g = spark.table(f"{SCHEMA}.gold_siape_servidor_mensal")

    print("\n--- Estatísticas gerais ---")
    g.agg(
        F.count("*").alias("rows"),
        F.countDistinct("cpf_norm").alias("cpfs_unicos"),
        F.sum(F.when(F.col("grupo_ocupacao").isNull(), 1).otherwise(0)).alias("null_grupo_ocupacao"),
        F.sum(F.when(F.col("tem_afastamento").isNull(), 1).otherwise(0)).alias("null_tem_afastamento"),
        F.sum(F.when(F.col("qtd_observacoes").isNull(), 1).otherwise(0)).alias("null_qtd_observacoes"),
    ).show()

    if "grupo_ocupacao" in g.columns:
        print("\n--- Top 15 grupos de ocupação ---")
        g.groupBy("grupo_ocupacao").count().orderBy(F.desc("count")).limit(15).show(truncate=False)


if __name__ == "__main__":
    run_gold()
