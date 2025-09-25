import os, time, json, sys, requests
import pandas as pd
from dateutil import parser as dateparser

# ... mantenha suas constantes (API_KEY, DATA_ID/PLACE_ID, LANG, URL, OUT_CSV, OUT_JSONL etc)

def load_existing_ids(csv_path: str) -> set[str]:
    """Lê o CSV final e devolve um set com review_id existentes (como string)."""
    if not os.path.exists(csv_path):
        return set()
    try:
        df_old = pd.read_csv(csv_path)
        if "review_id" in df_old.columns:
            return set(df_old["review_id"].astype(str))
    except Exception:
        pass
    return set()

def normalize_date(d):
    if not d: 
        return ""
    try:
        return dateparser.parse(d).isoformat()
    except Exception:
        return d


# ===== CONFIG =====
# Use variável de ambiente para a chave (mais seguro):
#   macOS/Linux: export SERPAPI_KEY="SUA_CHAVE_AQUI"
#   Windows PS:  $Env:SERPAPI_KEY="SUA_CHAVE_AQUI"
API_KEY = os.getenv("SERPAPI_KEY")  # não deixe chave fixa no código

LANG = "pt-BR"
URL = "https://serpapi.com/search.json"
OUT_CSV = "reviews_clickcannabis.csv"
OUT_JSONL = "reviews_clickcannabis.jsonl"

# Prefira data_id (mais estável que place_id)
DATA_ID = "0x9bdb4de1ad551d:0x2222b9defd4b9462"
# (Se quiser usar place_id, troque no params_base)

PAGE_SLEEP = 1.0
RETRY_SLEEP = 3.0
MAX_RETRIES = 5

def normalize_date(d):
    if not d:
        return ""
    try:
        return dateparser.parse(d).isoformat()
    except Exception:
        return d

def fetch_all_reviews(existing_ids: set[str]) -> list[dict]:
    """
    Busca reviews mais recentes primeiro (sort_by=newest) e para assim que
    encontrar um review_id já existente no CSV.
    Retorna a lista de NOVOS reviews (dicionários normalizados).
    """
    params_base = {
        "engine": "google_maps_reviews",
        # Use APENAS UM: se você usa data_id, deixe data_id; se usa place_id, comente o outro.
        "data_id": DATA_ID,           # ou comente e use: "place_id": PLACE_ID,
        "hl": LANG,
        "api_key": API_KEY,
        "sort_by": "newest",
        # "no_cache": "true",          # opcional (pode aumentar custo)
    }

    all_rows = []
    seen_page_ids = set()
    next_token = None
    page = 0
    found_old = False

    # abre JSONL bruto para auditoria
    with open(OUT_JSONL, "w", encoding="utf-8") as jf:
        while True:
            page += 1
            params = dict(params_base)
            if next_token:
                params["next_page_token"] = next_token

            # requisição com simples retry
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    resp = requests.get(URL, params=params, timeout=60)
                    resp.raise_for_status()
                    data = resp.json()
                    break
                except Exception as e:
                    if attempt == MAX_RETRIES:
                        print(f"Falhou na página {page}: {e}")
                        raise
                    time.sleep(RETRY_SLEEP)

            reviews = data.get("reviews", []) or []

            # processa cada review
            page_new_count = 0
            for r in reviews:
                rid = str(r.get("review_id") or r.get("id") or r.get("reviewId") or "")
                if rid:  # se tem id
                    # se já existe no CSV anterior, podemos parar toda a coleta
                    if rid in existing_ids:
                        found_old = True
                        break
                    # evita duplicar dentro da mesma execução por segurança
                    if rid in seen_page_ids:
                        continue
                    seen_page_ids.add(rid)

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

                all_rows.append(row)
                page_new_count += 1
                jf.write(json.dumps(r, ensure_ascii=False) + "\n")

            print(f"Página {page}: +{page_new_count} novo(s) (total novos: {len(all_rows)})")

            # critério de parada:
            # - encontramos review antigo -> paramos
            # - não há paginação seguinte -> paramos
            serp_pagi = data.get("serpapi_pagination") or {}
            next_token = serp_pagi.get("next_page_token")

            if found_old:
                print("⏹️ Encontrado review_id já existente. Coleta interrompida para economizar créditos.")
                break
            if not next_token or not reviews:
                break

            time.sleep(PAGE_SLEEP)

    return all_rows


def main():
    if not API_KEY or API_KEY in ("SUA_CHAVE_AQUI", "SUA_CHAVE_DA_SERPAPI"):
        print("Erro: defina sua chave em API_KEY ou SERPAPI_KEY.")
        sys.exit(1)

    # 1) IDs que já temos no CSV final
    existing_ids = load_existing_ids(OUT_CSV)

    # 2) Buscar APENAS os novos
    novos = fetch_all_reviews(existing_ids)

    # 3) Juntar com os antigos e salvar
    df_new = pd.DataFrame(novos)

    if os.path.exists(OUT_CSV):
        df_old = pd.read_csv(OUT_CSV)
        df_all = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_all = df_new

    # Dedup (segurança extra)
    if "review_id" in df_all.columns:
        df_all = df_all.drop_duplicates(subset=["review_id", "texto"], keep="first")
    else:
        df_all = df_all.drop_duplicates(subset=["autor_nome", "data_original", "texto"], keep="first")

    # Ordena por data se existir
    if "data_iso" in df_all.columns:
        df_all["data_iso_sort"] = pd.to_datetime(df_all["data_iso"], errors="coerce", utc=True)
        df_all = df_all.sort_values("data_iso_sort", ascending=False).drop(columns=["data_iso_sort"])

    # salva
    df_all.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    print(f"\n✅ CSV atualizado: {OUT_CSV}")
    print(f"   Antigos: {len(existing_ids)} | Novos: {len(df_new)} | Total: {len(df_all)}")


if __name__ == "__main__":
    # Logs úteis de ambiente
    print("Python em uso:", sys.executable)
    try:
        import urllib3
        print("urllib3 em:", urllib3.__file__)
    except Exception:
        pass
    key = os.getenv("SERPAPI_KEY")
    print("SERPAPI_KEY presente?", bool(key))
    main()
