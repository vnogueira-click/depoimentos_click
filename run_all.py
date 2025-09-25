# run_all.py
import subprocess
import sys
import os
import pandas as pd

RAW_CSV = "reviews_clickcannabis.csv"          # saída do baixar_reviews.py (bruto)
FINAL_CSV = "reviews_clickcannabis_ia.csv"     # saída final que o app lê

def run(cmd: list[str]):
    print(">>>", " ".join(cmd), flush=True)
    subprocess.check_call(cmd)

def main():
    # 1) Baixar tudo de novo (ou só “newest”: você pode trocar a flag dentro do script)
    run([sys.executable, "baixar_reviews.py"])

    # 2) Classificar com IA (usa OPENAI_API_KEY)
    run([sys.executable, "classificar_ia.py"])

    # 3) Sanitizar/garantir dedupe
    if os.path.exists(FINAL_CSV):
        df = pd.read_csv(FINAL_CSV)
        # Ordena por data se existir e remove nulos estranhos
        if "data_iso" in df.columns:
            df["data_iso_sort"] = pd.to_datetime(df["data_iso"], errors="coerce", utc=True)
            df = df.sort_values("data_iso_sort", ascending=False).drop(columns=["data_iso_sort"])
        # Dedupe por review_id + texto
        keys = [c for c in ["review_id", "texto"] if c in df.columns]
        if keys:
            df = df.drop_duplicates(subset=keys, keep="first")
        df.to_csv(FINAL_CSV, index=False, encoding="utf-8-sig")

    print("✅ Pipeline concluído.")

if __name__ == "__main__":
    main()
