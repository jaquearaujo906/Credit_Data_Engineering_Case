# Databricks notebook source
BASE_PATH = "/Volumes/workspace/default/siape_raw"

# Lista todas as pastas posicao=
folders = [f.path for f in dbutils.fs.ls(BASE_PATH) if f.isDir()]

for folder in folders:
    print(f"\n Ajustando pasta: {folder}")
    
    files = dbutils.fs.ls(folder)
    
    for file in files:
        old_path = file.path
        old_name = file.name.lower()
        
        # Detecta tipo pelo nome
        if "remuneracao" in old_name:
            new_name = "remuneracao.csv"
        elif "cadastro" in old_name:
            new_name = "cadastro.csv"
        elif "afastamento" in old_name:
            new_name = "afastamentos.csv"
        elif "observacao" in old_name:
            new_name = "observacoes.csv"
        else:
            continue
        
        new_path = folder + new_name
        
        print(f"{old_name}  →  {new_name}")
        
        dbutils.fs.mv(old_path, new_path)
        
print("\n Todos os arquivos foram renomeados!")

# COMMAND ----------

BASE_PATH = "/Volumes/workspace/default/siape_raw"

folders = [f.path for f in dbutils.fs.ls(BASE_PATH) if f.isDir()]

for folder in folders:
    files = dbutils.fs.ls(folder)
    for file in files:
        old_path = file.path
        old_name = file.name.lower()

        # pega qualquer variação que contenha "observ"
        if "observ" in old_name and old_name.endswith(".csv"):
            new_path = folder + "observacoes.csv"
            print(f"{file.name}  →  observacoes.csv  |  {folder}")
            dbutils.fs.mv(old_path, new_path)

print("\n Observações renomeadas (onde existia arquivo correspondente).")