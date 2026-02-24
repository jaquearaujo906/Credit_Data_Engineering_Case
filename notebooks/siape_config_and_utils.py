# 00_siape_config_and_utils.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip
import re, unicodedata, os, shutil, requests, zipfile

# Spark local com Delta
builder = (
    SparkSession.builder
    .appName("siape_local")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", os.path.abspath("datalake/warehouse"))
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")


class FakeDBUtils:
    class fs:
        @staticmethod
        def ls(path):
            if not os.path.exists(path):
                return []
            result = []
            for f in os.listdir(path):
                full = os.path.join(path, f)
                obj = type("F", (), {
                    "path": full,
                    "name": f,
                    "isDir": os.path.isdir(full)
                })()
                result.append(obj)
            return result

        @staticmethod
        def mkdirs(path):
            os.makedirs(path, exist_ok=True)

        @staticmethod
        def cp(src, dst):
            src = src.replace("file://", "")
            shutil.copy(src, dst)

        @staticmethod
        def mv(src, dst):
            shutil.move(src, dst)

dbutils = FakeDBUtils()


SCHEMA = "public_informations"
RAW_ROOT = os.path.abspath("data/raw")
WAREHOUSE = os.path.abspath("datalake/warehouse")

POSITIONS = ["2025-12", "2025-11", "2025-10", "2025-09"]

RAW_FILES = {
    "remuneracao": "remuneracao.csv",
    "cadastro": "cadastro.csv",
    "afastamento": "afastamentos.csv",
    "observacoes": "observacoes.csv",
}

spark.sql(f"CREATE DATABASE IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE {SCHEMA}")

print("Schema:", SCHEMA)
print("RAW_ROOT:", RAW_ROOT)
print("POSITIONS:", POSITIONS)


def clean_colname(c: str) -> str:
    c = c.replace('"', '').strip().lower()
    c = unicodedata.normalize("NFKD", c).encode("ascii", "ignore").decode("ascii")
    c = re.sub(r"[^\w]+", "_", c)
    c = re.sub(r"__+", "_", c).strip("_")
    if re.match(r"^\d", c):
        c = f"col_{c}"
    return c


def read_raw_csv(path: str):
    df = (
        spark.read
        .option("header", "true")
        .option("sep", ";")
        .option("encoding", "ISO-8859-1")
        .option("inferSchema", "false")
        .csv(path)
    )
    df = df.toDF(*[clean_colname(c) for c in df.columns])
    return df


def normalize_cpf(col):
    return F.lpad(F.regexp_replace(col.cast("string"), r"[^0-9]", ""), 11, "0")


def to_decimal_ptbr(col):
    s = F.regexp_replace(col.cast("string"), r"\.", "")
    s = F.regexp_replace(s, ",", ".")
    return s.cast("double")


def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None


def raw_path(posicao: str, domain: str) -> str:
    return os.path.join(RAW_ROOT, f"posicao={posicao}", RAW_FILES[domain])


def validate_raw_inputs():
    missing = []
    for pos in POSITIONS:
        for domain in RAW_FILES.keys():
            p = raw_path(pos, domain)
            if not os.path.exists(p):
                missing.append(p)
    return missing


if __name__ == "__main__":
    missing = validate_raw_inputs()
    if missing:
        print("\n FALTANDO arquivos RAW:")
        for m in missing:
            print(" -", m)
    else:
        print("\n RAW OK: todos os arquivos existem para todas as posições.")
