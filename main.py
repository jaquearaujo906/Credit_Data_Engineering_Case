# main.py
# from notebooks.siape_download import download_siape
from notebooks.siape_fix_file_names import fix_file_names
from notebooks.siape_bronze_ingestion import run_bronze
from notebooks.siape_silver_standardization import run_silver
from notebooks.siape_gold_product_and_stats import run_gold
from notebooks.siape_config_and_utils import POSITIONS, validate_raw_inputs

# ── 1. Download ──────────────────────────────────────────────
# print("=" * 50)
# print("ETAPA 1: Download dos arquivos")
# print("=" * 50)
# for pos in POSITIONS:
#     download_siape(pos)

# ── 2. Renomeia arquivos ─────────────────────────────────────
print("\n" + "=" * 50)
print("ETAPA 2: Padronização dos nomes dos arquivos")
print("=" * 50)
fix_file_names()

# ── 3. Valida se tudo chegou ─────────────────────────────────
print("\n" + "=" * 50)
print("ETAPA 3: Validação dos arquivos RAW")
print("=" * 50)
missing = validate_raw_inputs()
if missing:
    print("ERRO - Arquivos faltando:")
    for m in missing:
        print(" -", m)
    raise SystemExit("Corrija os arquivos antes de continuar.")
else:
    print("OK - Todos os arquivos encontrados.")

# ── 4. Pipeline ───────────────────────────────────────────────
print("\n" + "=" * 50)
print("ETAPA 4: Bronze")
print("=" * 50)
run_bronze()

print("\n" + "=" * 50)
print("ETAPA 5: Silver")
print("=" * 50)
run_silver()

print("\n" + "=" * 50)
print("ETAPA 6: Gold + Estatísticas")
print("=" * 50)
run_gold()

print("\n Pipeline completo!")
