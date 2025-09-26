# run_all.py
import subprocess
import sys
import os
import pandas as pd

RAW_CSV   = "reviews_clickcannabis.csv"          # saída do baixar_reviews.py (bruto)
FINAL_CSV = "reviews_clickcannabis_ia.csv"       # base lida pelo app / classificador

def run(cmd):
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

def count_unclassified(df: pd.DataFrame) -> int:
    if "categorias_ia" not in df.columns:
        return len(df)
    s = df["categorias_ia"].astype(str).str.strip().str.lower()
    return (s.eq("") | s.eq("nan") | s.eq("none")).sum()

def unique_ids_count(df: pd.DataFrame) -> int:
    if "review_id" not in df.columns:
        return len(df)
    return df["review_id"].astype(str).nunique()

def main():
    # 1) Baixar novos reviews
    run([sys.executable, "baixar_reviews.py"])

    # 2) Mesclar bruto -> final (para que a IA processe APENAS o que ainda não tem categorias_ia)
    df_raw = read_csv_safe(RAW_CSV)
    df_final_before = read_csv_safe(FINAL_CSV)

    ids_before = unique_ids_count(df_final_before)
    to_classify_before = count_unclassified(df_final_before)

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

    # 3) Classificar com IA (só linhas sem categorias_ia)
    run([sys.executable, "classificar_ia.py"])

    # 4) Sanitizar/garantir dedupe após a classificação
    df_final_after = read_csv_safe(FINAL_CSV)
    if df_final_after.empty:
        print("⚠️ FINAL ficou vazio após classificação? Verifique os logs.")
        return

    df_final_after = sort_and_dedupe(df_final_after)
    df_final_after.to_csv(FINAL_CSV, index=False, encoding="utf-8-sig")

    # ======= RESUMO =======
    ids_after = unique_ids_count(df_final_after)
    new_reviews = max(ids_after - ids_before, 0)

    to_classify_after = count_unclassified(df_final_after)
    classified_now = max((to_classify_before + new_reviews) - to_classify_after, 0)

    print("✅ Pipeline concluído.")
    print(f"📊 Resumo:")
    print(f"   • Novos reviews adicionados: {new_reviews}")
    print(f"   • Classificados nesta rodada: {classified_now}")
    print(f"   • Total de reviews (únicos): {ids_after}")

if __name__ == "__main__":
    main()
