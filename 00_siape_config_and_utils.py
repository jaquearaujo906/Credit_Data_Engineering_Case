# Databricks notebook source
# 00_siape_config_and_utils
# Configurações e funções utilitárias para o pipeline de dados do SIAPE
from pyspark.sql import functions as F
import re
import unicodedata


SCHEMA = "public_informations"
RAW_ROOT = "/Volumes/workspace/default/siape_raw"

#posicoes M-2 + 3 meses anteriores
POSITIONS = ["2025-12", "2025-11", "2025-10", "2025-09"]

RAW_FILES = {
    "remuneracao": "remuneracao.csv",
    "cadastro": "cadastro.csv",
    "afastamento": "afastamentos.csv",
    "observacoes": "observacoes.csv",
}

# SCHEMA
spark.sql(f"CREATE DATABASE IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE {SCHEMA}")

print("Schema:", SCHEMA)
print("RAW_ROOT:", RAW_ROOT)
print("POSITIONS:", POSITIONS)


# delta não aceita acento e alguns símbolos padronizo aqui
def clean_colname(c: str) -> str:
    c = c.replace('"', '').strip().lower()
    c = unicodedata.normalize("NFKD", c).encode("ascii", "ignore").decode("ascii")
    c = re.sub(r"[^\w]+", "_", c)
    c = re.sub(r"__+", "_", c).strip("_")
    if re.match(r"^\d", c):
        c = f"col_{c}"
    return c

#leitura pros csv do portal
def read_raw_csv(path: str):
    df = (
        spark.read
        .option("header", "true")
        .option("sep", ";")
        .option("encoding", "ISO-8859-1")
        .option("inferSchema", "false")
        .option("includeMetadata", "true")  # necessário no UC pra usar _metadata.file_path
        .csv(path)
    )
    #padroniza os nomes pra nao dar erro no delta
    df = df.toDF(*[clean_colname(c) for c in df.columns])
    return df

def normalize_cpf(col):
    return F.lpad(F.regexp_replace(col.cast("string"), r"[^0-9]", ""), 11, "0")

def to_decimal_ptbr(col):
    s = F.regexp_replace(col.cast("string"), r"\.", "")
    s = F.regexp_replace(s, ",", ".")
    return s.cast("double")

#em alguns arquivos o nome do campo pode variar
def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

#cria o caminho final do arquivo dentro da pasta posicao=YYYY-MM
def raw_path(posicao: str, domain: str) -> str:
    return f"{RAW_ROOT}/posicao={posicao}/{RAW_FILES[domain]}"

#verifica se os arquivos existem em todas as posicoes
def validate_raw_inputs():
    missing = []
    for pos in POSITIONS:
        for domain in RAW_FILES.keys():
            p = raw_path(pos, domain)
            try:
                dbutils.fs.ls(p)
            except Exception:
                missing.append(p)
    return missing


missing = validate_raw_inputs()
if missing:
    print("\n FALTANDO arquivos RAW:")
    for m in missing:
        print(" -", m)
else:
    print("\n RAW OK: todos os arquivos existem para todas as posições.")