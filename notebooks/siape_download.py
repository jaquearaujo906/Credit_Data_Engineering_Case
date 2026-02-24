def download_siape(posicao: str):
    ano_mes = posicao.replace("-", "")
    
    # URL correta sem .zip duplicado
    url = f"https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/servidores/{ano_mes}_Servidores_SIAPE.zip"

    dest_dir = os.path.join(RAW_ROOT, f"posicao={posicao}")
    os.makedirs(dest_dir, exist_ok=True)

    local_zip = os.path.join(dest_dir, f"siape_{ano_mes}.zip")  # Windows não tem /tmp
    extract_dir = os.path.join(dest_dir, f"extract_{ano_mes}")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Referer": "https://portaldatransparencia.gov.br/",
    }

    print(f"Baixando {url}...")
    r = requests.get(url, stream=True, timeout=300, headers=headers, allow_redirects=True)
    r.raise_for_status()

    with open(local_zip, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)

    print("Extraindo...")
    with zipfile.ZipFile(local_zip, "r") as z:
        z.extractall(extract_dir)

    for fname in os.listdir(extract_dir):
        if fname.endswith(".csv"):
            shutil.copy2(
                os.path.join(extract_dir, fname),
                os.path.join(dest_dir, fname)
            )
            print(f"  {fname} → {dest_dir}")

    os.remove(local_zip)
    shutil.rmtree(extract_dir)
    print(f"  Concluído: posicao={posicao}")