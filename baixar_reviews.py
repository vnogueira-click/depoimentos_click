# baixar_reviews.py
import os, sys, time, json, requests
import pandas as pd
from dateutil import parser as dateparser

# ===================== CONFIG ===================== #
SERPAPI_KEY = os.getenv("SERPAPI_KEY") or ""
# Use o data_id (mais estável). Se preferir, pode trocar por place_id=...
DATA_ID     = "0x9bdb4de1ad551d:0x2222b9defd4b9462"
LANG        = "pt-BR"

URL         = "https://serpapi.com/search.json"
OUT_CSV     = "reviews_clickcannabis.csv"
OUT_JSONL   = "reviews_clickcannabis.jsonl"

PAGE_SLEEP  = 1.0            # pausa entre páginas
RETRY_SLEEP = 3.0            # pausa entre tentativas
MAX_RETRIES = 5              # tentativas por request
MAX_PAGES   = 2000           # guarda-chuva
OLD_STREAK_STOP = 8          # para quando encontrar N páginas seguidas só com ids já conhecidos

# ===================== UTILS ====================== #
def normalize_date(d):
    if not d:
        return ""
    try:
        return dateparser.parse(d).isoformat()
    except Exception:
        return d

def read_known_ids(csv_path: str) -> set[str]:
    """Carrega review_id já existentes (para parar cedo e evitar custo)."""
    if not os.path.exists(csv_path):
        return set()
    try:
        df = pd.read_csv(csv_path, usecols=["review_id"])
        return set(df["review_id"].astype(str).dropna().str.strip().tolist())
    except Exception:
        return set()

def robust_get(params: dict) -> dict:
    """GET com retries/backoff simples."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(URL, params=params, timeout=60)
            if r.status_code == 429:
                # Too Many Requests — aguarda mais e tenta de novo
                time.sleep(RETRY_SLEEP * attempt)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_SLEEP * attempt)
    return {}

# ===================== CORE ======================= #
def fetch_all_reviews() -> list[dict]:
    if not SERPAPI_KEY:
        print("ERRO: defina SERPAPI_KEY no ambiente.", file=sys.stderr)
        sys.exit(1)

    known_ids = read_known_ids(OUT_CSV)
    print(f"🔎 IDs conhecidos no CSV atual: {len(known_ids)}")

    params_base = {
        "engine": "google_maps_reviews",
        "data_id": DATA_ID,
        "hl": LANG,
        "api_key": SERPAPI_KEY,
        "sort_by": "newest",     # pega mais recentes primeiro
        # "no_cache": "true",
    }

    new_rows: list[dict] = []
    seen_ids: set[str] = set()     # segurança intra-execução
    next_token = None
    page = 0
    old_streak = 0                 # páginas sem nada novo

    # abre JSONL para depuração/backup bruto
    jf = open(OUT_JSONL, "w", encoding="utf-8")

    try:
        while True:
            page += 1
            if page > MAX_PAGES:
                print("⛔ MAX_PAGES atingido; encerrando.")
                break

            params = dict(params_base)
            if next_token:
                params["next_page_token"] = next_token

            data = robust_get(params)
            reviews = data.get("reviews") or []
            next_token = (data.get("serpapi_pagination") or {}).get("next_page_token")

            added_this_page = 0

            for r in reviews:
                rid = str(r.get("review_id") or r.get("id") or r.get("reviewId") or "").strip()
                if not rid or rid in seen_ids:
                    continue
                seen_ids.add(rid)

                # se já conhecemos, não é “novo”
                if rid in known_ids:
                    continue

                # >>> IGNORAR reviews sem comentário <<<
                texto = r.get("snippet") or r.get("content") or r.get("comment")
                if not (texto and str(texto).strip()):
                    # pular quem só tem estrelas/foto
                    continue

                # imagens (se houver)
                image_urls = []
                for img in (r.get("images") or []):
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
                    "texto": texto,
                    "review_link": r.get("link") or "",
                    "review_id": rid,
                    "helpful_votes": r.get("thumbs_up_count") or r.get("likes_count") or 0,
                    "imagens_do_review": "|".join([u for u in image_urls if u]),
                }

                new_rows.append(row)
                jf.write(json.dumps(r, ensure_ascii=False) + "\n")
                added_this_page += 1

            # logs
            print(f"🟩 Página {page:>3}: +{added_this_page} novos (acumulado: {len(new_rows)})")

            # controle de parada:
            if added_this_page == 0:
                old_streak += 1
            else:
                old_streak = 0

            # se já passamos OLD_STREAK_STOP páginas sem nada novo: parar cedo
            if old_streak >= OLD_STREAK_STOP:
                print(f"✅ {OLD_STREAK_STOP} páginas seguidas sem novos. Encerrando cedo para poupar créditos.")
                break

            if not next_token:
                print("🔚 Sem next_page_token. Fim.")
                break

            time.sleep(PAGE_SLEEP)

    finally:
        jf.close()

    return new_rows

def main():
    novos = fetch_all_reviews()

    if os.path.exists(OUT_CSV):
        df_old = pd.read_csv(OUT_CSV)
    else:
        df_old = pd.DataFrame()

    df_new = pd.DataFrame(novos)
    df_all = pd.concat([df_new, df_old], ignore_index=True)

    # dedupe por (review_id, texto) para segurança
    keys = [c for c in ["review_id", "texto"] if c in df_all.columns]
    if keys:
        df_all = df_all.drop_duplicates(subset=keys, keep="first")

    # ordenar por data se possível
    if "data_iso" in df_all.columns:
        df_all["data_iso_sort"] = pd.to_datetime(df_all["data_iso"], errors="coerce", utc=True)
        df_all = df_all.sort_values("data_iso_sort", ascending=False).drop(columns=["data_iso_sort"])

    df_all.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    print(f"\n📥 Novos salvos nesta execução: {len(novos)}")
    print(f"🗂  Total no CSV: {len(df_all)}")
    print(f"💾 Arquivos gerados: {OUT_CSV}  |  {OUT_JSONL}")

if __name__ == "__main__":
    main()
