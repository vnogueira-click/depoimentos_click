# baixar_reviews.py
import os, time, json, sys, requests
import pandas as pd
from dateutil import parser as dateparser

API_KEY = os.getenv("SERPAPI_KEY", "").strip()
DATA_ID = "0x9bdb4de1ad551d:0x2222b9defd4b9462"  # Click Cannabis
LANG = "pt-BR"

URL = "https://serpapi.com/search.json"
OUT_RAW = "reviews_clickcannabis.csv"
OUT_JSONL = "reviews_clickcannabis.jsonl"  # dump bruto por página

# Limites e backoff via env (com defaults seguros)
MAX_NEW = int(os.getenv("MAX_NEW", "60"))                      # teto de reviews novos por execução
OLD_STREAK_STOP_AT = int(os.getenv("OLD_STREAK_STOP_AT", "5")) # para após N seguidos já existentes
PAGE_SLEEP = float(os.getenv("PAGE_SLEEP", "0.8"))
RETRY_SLEEP = float(os.getenv("RETRY_SLEEP", "2.0"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))

def normalize_date(d):
    if not d:
        return ""
    try:
        return dateparser.parse(d).isoformat()
    except Exception:
        return d

def load_known_ids() -> set:
    """Lê review_id já existentes a partir do CSV de saída do app (preferência) ou do bruto."""
    ids = set()
    for path in ("reviews_clickcannabis_ia.csv", OUT_RAW):
        if os.path.exists(path):
            try:
                df = pd.read_csv(path, usecols=["review_id"])
                ids |= set(df["review_id"].astype(str).fillna(""))
            except Exception:
                pass
    return {i for i in ids if i and i != "nan"}

def fetch_all_reviews():
    params_base = {
        "engine": "google_maps_reviews",
        "data_id": DATA_ID,
        "hl": LANG,
        "api_key": API_KEY,
        "sort_by": "newest",     # muito importante p/ parar cedo
        # "no_cache": "true",    # habilite se quiser forçar bypass de cache do SerpAPI
    }

    known_ids = load_known_ids()
    print(f"✔ ids conhecidos: {len(known_ids)}")
    new_rows = []
    seen_ids = set()
    next_token = None
    page = 0
    old_streak = 0

    # para controle de custo
    max_new = MAX_NEW
    old_streak_cutoff = OLD_STREAK_STOP_AT

    # dump bruto (útil p/ debugging)
    jf = open(OUT_JSONL, "w", encoding="utf-8")

    try:
        while True:
            if max_new is not None and len(new_rows) >= max_new:
                print(f"▶ Atingiu MAX_NEW={max_new}. Encerrando.")
                break
            if old_streak >= old_streak_cutoff:
                print(f"▶ Encontrou {old_streak} antigos seguidos. Encerrando cedo para poupar créditos.")
                break

            page += 1
            params = dict(params_base)
            if next_token:
                params["next_page_token"] = next_token

            # retries com backoff
            delay = RETRY_SLEEP
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    resp = requests.get(URL, params=params, timeout=60)
                    if resp.status_code == 429 or 500 <= resp.status_code < 600:
                        # backoff exponencial com jitter leve
                        print(f"⚠ {resp.status_code} na página {page}. Tentativa {attempt}/{MAX_RETRIES}. Aguardando {delay:.1f}s…")
                        time.sleep(delay)
                        delay = min(delay * 1.8, 20.0)
                        continue
                    resp.raise_for_status()
                    data = resp.json()
                    break
                except Exception as e:
                    if attempt == MAX_RETRIES:
                        print(f"✖ Falhou na página {page}: {e}")
                        raise
                    print(f"⚠ Erro na página {page}: {e} (tentativa {attempt}/{MAX_RETRIES})")
                    time.sleep(delay)
                    delay = min(delay * 1.8, 20.0)

            reviews = data.get("reviews", []) or []
            jf.write(json.dumps({"page": page, "count": len(reviews), "raw": data}, ensure_ascii=False) + "\n")

            # processa
            added_this_page = 0
            for r in reviews:
                rid = str(r.get("review_id") or r.get("id") or r.get("reviewId") or "").strip()
                if not rid:
                    continue
                if rid in seen_ids:
                    continue
                seen_ids.add(rid)

                # checa se já conhecemos
                if rid in known_ids:
                    old_streak += 1
                    continue
                else:
                    old_streak = 0  # zerar a sequência ao encontrar novo

                images = r.get("images") or []
                image_urls = []
                for img in images:
                    if isinstance(img, dict):
                        image_urls.append(img.get("original") or img.get("src"))
                    elif isinstance(img, str):
                        image_urls.append(img)

                row = {
                    "autor_nome": r.get("user") or r.get("user_name"),
                    "autor_perfil_link": r.get("user_link"),
                    "autor_foto": r.get("user_photo"),
                    "rating": r.get("rating"),
                    "data_original": r.get("date"),
                    "data_iso": normalize_date(r.get("date")),
                    "texto": r.get("snippet") or r.get("content") or r.get("comment"),
                    "review_link": r.get("link") or "",
                    "review_id": rid,
                    "helpful_votes": r.get("thumbs_up_count") or r.get("likes_count") or 0,
                    "imagens_do_review": "|".join([u for u in image_urls if u]),
                }

                new_rows.append(row)
                added_this_page += 1

                if max_new is not None and len(new_rows) >= max_new:
                    break  # respeita teto

            total = len(new_rows)
            print(f"Página {page}: +{added_this_page} novos (acumulado: {total}).")
            time.sleep(PAGE_SLEEP)

            # paginação
            next_token = (data.get("serpapi_pagination") or {}).get("next_page_token")
            if not next_token:
                print("▶ Não há próximo token. Fim.")
                break

            # heurística extra: se esta página trouxe 0 novos, aumente chance de encerrar nas próximas
            if added_this_page == 0:
                old_streak += 1

    finally:
        jf.close()

    return new_rows

def save_merge(rows):
    """Salva/mescla no OUT_RAW (CSV) sem duplicar."""
    if not rows:
        print("Nenhum review novo para salvar.")
        return

    df_new = pd.DataFrame(rows)
    if os.path.exists(OUT_RAW):
        df_old = pd.read_csv(OUT_RAW)
        df = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df = df_new

    # dedupe por review_id + texto
    keys = [c for c in ["review_id", "texto"] if c in df.columns]
    if keys:
        df = df.drop_duplicates(subset=keys, keep="first")

    # ordenar por data se possível
    if "data_iso" in df.columns:
        df["data_iso_sort"] = pd.to_datetime(df["data_iso"], errors="coerce", utc=True)
        df = df.sort_values("data_iso_sort", ascending=False).drop(columns=["data_iso_sort"])

    df.to_csv(OUT_RAW, index=False, encoding="utf-8-sig")
    print(f"💾 Salvo/mesclado: {OUT_RAW} | linhas: {len(df)}")

def main():
    if not API_KEY:
        print("Erro: defina SERPAPI_KEY no ambiente.")
        sys.exit(1)

    print(f"Rodando com limites: MAX_NEW={MAX_NEW}, OLD_STREAK_STOP_AT={OLD_STREAK_STOP_AT}")
    rows = fetch_all_reviews()
    save_merge(rows)
    print("✅ download concluído.")

if __name__ == "__main__":
    main()
