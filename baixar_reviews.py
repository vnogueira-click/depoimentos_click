#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import json
import sys
import requests
import pandas as pd
from dateutil import parser as dateparser

# ================== CONFIG ==================
API_KEY   = os.getenv("SERPAPI_KEY") or ""
# Use o data_id abaixo (estável) OU comente-o e use PLACE_ID
DATA_ID   = "0x9bdb4de1ad551d:0x2222b9defd4b9462"
PLACE_ID  = None  # exemplo: "ChIJHVWt4U3bmwARYpRL_d65IiI"
LANG      = "pt-BR"

URL        = "https://serpapi.com/search.json"
OUT_CSV    = "reviews_clickcannabis.csv"        # bruto (saida deste script)
OUT_JSONL  = "reviews_clickcannabis.jsonl"      # bruto (linha a linha)
FINAL_CSV  = "reviews_clickcannabis_ia.csv"     # base final (usada p/ ver IDs existentes)

# Controle de paginação/robustez
PAGE_SLEEP   = float(os.getenv("PAGE_SLEEP", "0.8"))
RETRY_SLEEP  = float(os.getenv("RETRY_SLEEP", "2.0"))
MAX_RETRIES  = int(os.getenv("MAX_RETRIES", "5"))

# Otimização de custo: limite de novos por execução e “streak” de antigos
MAX_NEW            = int(os.getenv("MAX_NEW", "80"))  # 0 = sem limite
OLD_STREAK_STOP_AT = int(os.getenv("OLD_STREAK_STOP_AT", "5"))

# ================== HELPERS ==================
def normalize_date(d: str) -> str:
    if not d:
        return ""
    try:
        return dateparser.parse(d).isoformat()
    except Exception:
        return d

def load_existing_ids() -> set:
    """Lê review_id já presentes no CSV final para parar cedo."""
    if not os.path.exists(FINAL_CSV):
        return set()
    try:
        df = pd.read_csv(FINAL_CSV, usecols=["review_id"])
        return set(df["review_id"].astype(str).dropna().unique())
    except Exception:
        return set()

# ================== CORE =====================
def fetch_all_reviews() -> list[dict]:
    existing_ids = load_existing_ids()
    if existing_ids:
        print(f"📚 IDs existentes carregados do FINAL: {len(existing_ids)}")

    params_base = {
        "engine": "google_maps_reviews",
        "hl": LANG,
        "api_key": API_KEY,
        "sort_by": "newest",         # pega do mais novo p/ o mais antigo
        # "no_cache": "true",        # evite isso; aumenta custo
    }
    if DATA_ID:
        params_base["data_id"] = DATA_ID
    elif PLACE_ID:
        params_base["place_id"] = PLACE_ID
    else:
        raise SystemExit("Defina DATA_ID ou PLACE_ID.")

    all_rows = []
    seen_ids = set()
    next_token = None
    page = 0
    new_count = 0
    old_streak = 0  # quando começa a ver IDs antigos seguidos, paramos

    # garante arquivo limpo a cada rodada
    try:
        if os.path.exists(OUT_JSONL):
            os.remove(OUT_JSONL)
    except Exception:
        pass

    with open(OUT_JSONL, "w", encoding="utf-8") as jf:
        while True:
            if MAX_NEW and new_count >= MAX_NEW:
                print(f"🛑 Atingiu MAX_NEW={MAX_NEW}. Parando.")
                break

            page += 1
            params = dict(params_base)
            if next_token:
                params["next_page_token"] = next_token

            # retries simples
            data = None
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    resp = requests.get(URL, params=params, timeout=60)
                    resp.raise_for_status()
                    data = resp.json()
                    break
                except Exception as e:
                    if attempt == MAX_RETRIES:
                        print(f"❌ Falhou na página {page}: {e}")
                        raise
                    time.sleep(RETRY_SLEEP)

            reviews = data.get("reviews", []) or []
            if not reviews:
                print("⛔ Sem mais reviews nesta paginação.")
                break

            page_new = 0
            for r in reviews:
                rid = str(r.get("review_id") or r.get("id") or r.get("reviewId") or "").strip()
                if not rid:
                    continue

                # evita duplicar dentro da MESMA execução
                if rid in seen_ids:
                    continue
                seen_ids.add(rid)

                # se já existe no FINAL, conta “antigo”
                if rid in existing_ids:
                    old_streak += 1
                    # se acumulou vários antigos na sequência, chegamos na fronteira
                    if old_streak >= OLD_STREAK_STOP_AT:
                        print(f"✅ Encontrou {OLD_STREAK_STOP_AT} antigos seguidos. Parando.")
                        reviews = []  # força sair da paginação
                        break
                    continue
                else:
                    old_streak = 0  # reset, achou um novo

                # imagens (SerpAPI pode vir str ou dict)
                imgs = r.get("images") or []
                image_urls = []
                for img in imgs:
                    if isinstance(img, dict):
                        image_urls.append(img.get("original") or img.get("src"))
                    elif isinstance(img, str):
                        image_urls.append(img)

                row = {
                    "autor_nome":      r.get("user") or r.get("user_name"),
                    "autor_perfil_link": r.get("user_link"),
                    "autor_foto":      r.get("user_photo"),
                    "rating":          r.get("rating"),
                    "data_original":   r.get("date"),
                    "data_iso":        normalize_date(r.get("date")),
                    "texto":           r.get("snippet") or r.get("content") or r.get("comment"),
                    "review_link":     r.get("link") or "",
                    "review_id":       rid,
                    "helpful_votes":   r.get("thumbs_up_count") or r.get("likes_count") or 0,
                    "imagens_do_review": "|".join([u for u in image_urls if u]),
                }

                all_rows.append(row)
                jf.write(json.dumps(r, ensure_ascii=False) + "\n")
                page_new += 1
                new_count += 1

                if MAX_NEW and new_count >= MAX_NEW:
                    break

            print(f"📄 Página {page}: +{page_new} novos (acumulado: {new_count})")

            if MAX_NEW and new_count >= MAX_NEW:
                break

            # paginação
            next_token = (data.get("serpapi_pagination") or {}).get("next_page_token")
            if not next_token:
                print("⛔ Fim da paginação (sem next_page_token).")
                break

            time.sleep(PAGE_SLEEP)

    return all_rows

# ================== MAIN =====================
def main():
    if not API_KEY or len(API_KEY) < 20:
        print("❌ SERPAPI_KEY não definido. Ex.: export SERPAPI_KEY='sua_chave'")
        sys.exit(1)

    print("🔑 API key (masc):", API_KEY[:6] + "..." + API_KEY[-4:])

    rows = fetch_all_reviews()
    if not rows:
        print("✔️ Sem novos reviews desta vez.")
        # ainda assim, garanta que OUT_CSV exista (idempotência do pipeline)
        if not os.path.exists(OUT_CSV) and os.path.exists(FINAL_CSV):
            pd.read_csv(FINAL_CSV).to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
        return

    df = pd.DataFrame(rows)

    # Dedupe por segurança
    if "review_id" in df.columns:
        df = df.drop_duplicates(subset=["review_id", "texto"], keep="first")
    else:
        df = df.drop_duplicates(keep="first")

    # Ordenar por data se possível
    if "data_iso" in df.columns:
        df["__sort"] = pd.to_datetime(df["data_iso"], errors="coerce", utc=True)
        df = df.sort_values("__sort", ascending=False).drop(columns=["__sort"])

    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ Gerado {OUT_CSV} com {len(df)} linha(s).")
    print(f"🗂  Bruto JSONL: {OUT_JSONL}")

if __name__ == "__main__":
    main()
