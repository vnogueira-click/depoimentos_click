# run_all.py
import subprocess
import sys
import os
import pandas as pd

RAW_CSV = "reviews_clickcannabis.csv"          # saída do baixar_reviews.py (bruto)
FINAL_CSV = "reviews_clickcannabis_ia.csv"     # base lida pelo app / classificador

def run(cmd: list[str]):
    print(">>>", " ".join(cmd), flush=True)
    subprocess.check_call(cmd)

def read_csv_safe(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path)

def sort_and_dedupe(df: pd.DataFrame) -> pd.DataFrame:
    # Ordena por data se existir
    if "data_iso" in df.columns:
        df["data_iso_sort"] = pd.to_datetime(df["data_iso"], errors="coerce", utc=True)
        df = df.sort_values("data_iso_sort", ascending=False).drop(columns=["data_iso_sort"])
    # Dedupe por review_id + texto (fallback se faltar review_id)
    keys = [c for c in ["review_id", "texto"] if c in df.columns]
    if keys:
        df = df.drop_duplicates(subset=keys, keep="first")
    else:
        df = df.drop_duplicates(keep="first")
    return df

def main():
    # 1) Baixar novos reviews (o script já está otimizado para parar ao encontrar review_id já existente)
    run([sys.executable, "baixar_reviews.py"])

    # 2) Mesclar bruto -> final (para que a IA processe APENAS o que ainda não tem categorias_ia)
    df_raw = read_csv_safe(RAW_CSV)
    df_final_before = read_csv_safe(FINAL_CSV)

    if df_raw.empty and df_final_before.empty:
        print("⚠️ Nenhum dado encontrado em RAW nem FINAL; nada a fazer.")
        return

    if df_final_before.empty:
        df_merged = df_raw.copy()
    elif df_raw.empty:
        df_merged = df_final_before.copy()
    else:
        df_merged = pd.concat([df_final_before, df_raw], ignore_index=True)

    df_merged = sort_and_dedupe(df_merged)
    # Salva a base FINAL já com os novos reviews incorporados (ainda sem categorias_ia para os novos)
    df_merged.to_csv(FINAL_CSV, index=False, encoding="utf-8-sig")
    print(f"📦 FINAL pronto para classificar: {len(df_merged)} linhas")

    # 3) Classificar com IA (o classificar_ia.py agora só trata linhas sem categorias_ia)
    run([sys.executable, "classificar_ia.py"])

    # 4) Sanitizar/garantir dedupe após a classificação
    df_final_after = read_csv_safe(FINAL_CSV)
    if not df_final_after.empty:
        df_final_after = sort_and_dedupe(df_final_after)
        df_final_after.to_csv(FINAL_CSV, index=False, encoding="utf-8-sig")
        print(f"✅ Pipeline concluído. Linhas finais: {len(df_final_after)}")
    else:
        print("⚠️ FINAL ficou vazio após classificação? Verifique os logs.")

if __name__ == "__main__":
    main()
