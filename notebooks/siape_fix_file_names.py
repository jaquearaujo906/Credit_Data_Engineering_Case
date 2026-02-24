# 99_fix_file_names.py
import os
from notebooks.siape_config_and_utils import dbutils, RAW_ROOT

def fix_folder_names():
    BASE_PATH = RAW_ROOT

    for folder in os.listdir(BASE_PATH):
        full_path = os.path.join(BASE_PATH, folder)
        
        if not os.path.isdir(full_path):
            continue
        
        # detecta padrão YYYYMM_Servidores_SIAPE
        if len(folder) >= 6 and folder[:6].isdigit() and "siape" in folder.lower():
            ano = folder[:4]
            mes = folder[4:6]
            new_name = f"posicao={ano}-{mes}"
            new_path = os.path.join(BASE_PATH, new_name)
            
            if full_path != new_path:
                os.rename(full_path, new_path)
                print(f"  {folder}  →  {new_name}")

def fix_file_names():
    fix_folder_names()
    BASE_PATH = RAW_ROOT

    folders = [f.path for f in dbutils.fs.ls(BASE_PATH) if f.isDir]

    for folder in folders:
        print(f"\n Ajustando pasta: {folder}")
        files = dbutils.fs.ls(folder)

        for file in files:
            old_path = file.path
            old_name = file.name.lower()

            if "remuneracao" in old_name:
                new_name = "remuneracao.csv"
            elif "cadastro" in old_name:
                new_name = "cadastro.csv"
            elif "afastamento" in old_name:
                new_name = "afastamentos.csv"
            elif "observacao" in old_name or "observ" in old_name:
                new_name = "observacoes.csv"
            else:
                continue

            new_path = os.path.join(folder, new_name)

            if old_path == new_path:
                print(f"  (já correto) {old_name}")
                continue

            print(f"  {old_name}  →  {new_name}")
            dbutils.fs.mv(old_path, new_path)

    print("\n Todos os arquivos foram renomeados!")


if __name__ == "__main__":
    fix_file_names()
