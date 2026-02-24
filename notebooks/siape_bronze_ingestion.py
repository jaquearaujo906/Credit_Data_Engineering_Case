# 01_siape_bronze_ingestion.py
from pyspark.sql import functions as F
from notebooks.siape_config_and_utils import (
    spark, SCHEMA, POSITIONS, RAW_FILES, raw_path, read_raw_csv
)


def write_bronze(domain: str, posicao: str, df, path: str):
    df_out = (
        df
        .withColumn("posicao", F.lit(posicao))
        .withColumn("source_domain", F.lit(domain))
        .withColumn("source_file", F.lit(path))   # substituímos _metadata.file_path pelo path local
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


def run_bronze():
    for pos in POSITIONS:
        print(f"BRONZE | posicao={pos}")

        for domain in RAW_FILES.keys():
            path = raw_path(pos, domain)
            print("  Reading:", path)

            df = read_raw_csv(path)
            print(f"  {domain} cols:", len(df.columns))

            t = write_bronze(domain, pos, df, path)
            print("  Saved:", t)

    print("\n Bronze finalizado para todas as posições.")
    spark.sql(f"SHOW TABLES IN {SCHEMA}").show(truncate=False)


if __name__ == "__main__":
    run_bronze()
