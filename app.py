import os
import ast
import base64
import warnings
import html
import json
from io import StringIO
from datetime import datetime

import pandas as pd
import streamlit as st
import requests

# ================== CONFIG ==================
CSV_PATH    = "reviews_clickcannabis_ia.csv"   # base que o app lê
BACKUP_DIR  = "_backups"
BRAND_GREEN = "#006f19"
PAGE_BG     = "#f1f2f2"

# ---- Persistência remota do "Já usei" ----
STATE_FILE = "usados_state.csv"                              # arquivo salvo no repo
GH_TOKEN   = st.secrets.get("GH_TOKEN")                      # obrigatório para salvar
GH_REPO    = st.secrets.get("GH_REPO", "vnogueira-click/depoimentos_click")
GH_BRANCH  = st.secrets.get("GH_BRANCH", "main")

warnings.filterwarnings("ignore", message="Could not infer format")

# ================== UTILS ===================
def b64img(path: str) -> str:
    try:
        with open(path, "rb") as f:
            return "data:image/png;base64," + base64.b64encode(f.read()).decode()
    except Exception:
        return ""

def save_df(df: pd.DataFrame, path: str) -> None:
    """Backup local (opcional) + salvar CSV principal (não o de estado)."""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    bak = os.path.join(BACKUP_DIR, f"{os.path.basename(path)}.{ts}.bak.csv")
    df.to_csv(bak, index=False, encoding="utf-8-sig")
    df.to_csv(path, index=False, encoding="utf-8-sig")

@st.cache_data(ttl=30)
def load_df(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    needed = [
        "categorias_ia","confianca_ia","justificativa_ia","imagens_do_review",
        "review_link","autor_nome","texto","usado","usado_em","rating",
        "data_original","data_iso","review_id"
    ]
    for col in needed:
        if col not in df.columns:
            df[col] = "" if col not in ("confianca_ia", "usado") else (0.0 if col=="confianca_ia" else False)
    df["usado"] = df["usado"].astype(str).str.lower().isin(["1","true","sim","yes","y","t"])
    return df

def split_imgs(s: str):
    if not isinstance(s, str):
        return []
    return [p.strip() for p in s.split("|") if p.strip()]

def parse_author(val):
    if isinstance(val, str) and val.strip().startswith("{") and "'name':" in val:
        try:
            d = ast.literal_eval(val)
            name = d.get("name") or d.get("user") or ""
            link = d.get("link") or ""
            thumb = d.get("thumbnail") or d.get("user_photo") or ""
            return name or "(sem nome)", link, thumb
        except Exception:
            pass
    return (val or "(sem nome)"), "", ""

# ---------- GitHub helpers (persistência) ----------
def gh_headers():
    return {
        "Authorization": f"Bearer {GH_TOKEN}",
        "Accept": "application/vnd.github+json"
    }

def gh_get_file(path):
    """Lê arquivo no GitHub. Retorna (texto, sha) ou (None, None) se não existir."""
    if not GH_TOKEN:
        return None, None
    url = f"https://api.github.com/repos/{GH_REPO}/contents/{path}?ref={GH_BRANCH}"
    r = requests.get(url, headers=gh_headers(), timeout=30)
    if r.status_code == 200:
        data = r.json()
        content = base64.b64decode(data["content"]).decode("utf-8")
        sha = data["sha"]
        return content, sha
    if r.status_code == 404:
        return None, None
    st.error(f"GitHub GET falhou: {r.status_code} - {r.text}")
    return None, None

def gh_put_file(path, text, sha=None, message="feat(app): update usados_state"):
    """Grava/atualiza arquivo no GitHub."""
    if not GH_TOKEN:
        return False
    url = f"https://api.github.com/repos/{GH_REPO}/contents/{path}"
    body = {
        "message": message,
        "content": base64.b64encode(text.encode("utf-8")).decode(),
        "branch": GH_BRANCH
    }
    if sha:
        body["sha"] = sha
    r = requests.put(url, headers=gh_headers(), data=json.dumps(body), timeout=30)
    if r.status_code not in (200, 201):
        st.error(f"GitHub PUT falhou: {r.status_code} - {r.text}")
        return False
    return True

@st.cache_data(ttl=15)
def load_state_df() -> pd.DataFrame:
    """Lê o CSV de estado (review_id, usado, usado_em) salvo no GitHub."""
    txt, _ = gh_get_file(STATE_FILE)
    if not txt:
        return pd.DataFrame(columns=["review_id","usado","usado_em"])
    return pd.read_csv(StringIO(txt))

def save_state_df(df_state: pd.DataFrame) -> bool:
    """Salva o CSV de estado no GitHub e limpa caches para refletir no app."""
    if not GH_TOKEN:
        st.warning("GH_TOKEN não configurado; estado não será persistido no GitHub.")
        return False
    _, sha = gh_get_file(STATE_FILE)
    csv_txt = df_state.to_csv(index=False, encoding="utf-8-sig")
    ok = gh_put_file(STATE_FILE, csv_txt, sha=sha)
    if ok:
        st.cache_data.clear()
    return ok

# ================== PAGE SETUP ===============
st.set_page_config(page_title="Reviews ClickCannabis", layout="wide")

st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    body, .stApp {{ font-family: 'Inter', sans-serif; background-color: {PAGE_BG}; }}
    .block-container {{ padding: 1.5rem 2rem; max-width: 1200px; }}
    .content-box {{
        background-color: #FFFFFF; border: 1px solid #e5e7eb; border-radius: 12px;
        padding: 20px; height: 100%; margin-bottom: 1.5rem;
    }}
    div[data-testid="stHorizontalBlock"] {{ align-items: center; }}
    [data-testid="stSidebarCollapseButton"] {{ display: none; }}
    [data-testid="stSidebar"] [data-testid="stImage"] {{ margin-top: -14px; }}
    div[data-testid="stSelectbox"] div[data-baseweb="select"],
    div[data-testid="stTextInput"] input {{ border: 1px solid #ced4da; border-radius: 0.5rem; }}
    div[data-testid="stSelectbox"] div[data-baseweb="select"]:focus-within,
    div[data-testid="stTextInput"] input:focus {{ border-color: transparent; }}
    .review-header {{ display: flex; align-items: center; gap: 12px; margin-bottom: 12px; flex-wrap: wrap; }}
    .author-name {{ font-weight: 600; font-size: 15px; color: #111827; }}
    .author-name a {{ color: #6b7280 !important; text-decoration: none; margin-left: 4px; }}
    .rating-stars {{ font-size: 14px; color: #f59e0b; }}
    .confidence-text {{ font-size: 13px; color: #6b7280; }}
    .date-text {{ font-size: 12px; color: #9ca3af; }}
    .review-text {{ font-size: 14px; line-height: 1.6; color: #374151; margin-bottom: 12px; }}
    .photo-links a {{ color: {BRAND_GREEN}; font-size: 13px; margin-right: 12px; text-decoration: none; font-weight: 500; }}
    .photo-links a:hover {{ text-decoration: underline; }}
    .categories-text {{ font-size: 13px; color: #6b7280; margin: 12px 0 0 0; }}
    .categories-text strong {{ color: #374151; font-weight: 500; }}
    .no-photo-text {{ font-size: 13px; color: #9ca3af; font-style: italic; }}
    div[data-testid="stToggle"] {{ display: flex; align-items: center; flex-direction: row-reverse; justify-content: flex-end; gap: 8px; }}
    h1 {{ font-size: 24px !important; font-weight: 700 !important; }}
    h3 {{ font-size: 16px !important; font-weight: 500 !important; color: #4b7280; margin-bottom: 24px !important; }}
    [data-testid="stSidebar"] {{ background-color: #ffffff; border-right: 1px solid #e5e7eb; }}
    #MainMenu, footer, header {{ visibility: hidden; }}
</style>
""", unsafe_allow_html=True)

# ================== SESSION STATE PARA PÁGINA ==================
if 'page' not in st.session_state:
    st.session_state.page = 1

st.title("Reviews ClickCannabis")

# ================== LOAD DATA E SIDEBAR ==================
if not os.path.exists(CSV_PATH):
    st.error(f"Arquivo não encontrado: {CSV_PATH}")
    st.stop()
df = load_df(CSV_PATH)

# ---- Mescla estado remoto do GitHub (se existir) ----
state_df = load_state_df()
if "review_id" in df.columns and not state_df.empty:
    df["review_id"] = df["review_id"].astype(str)
    state_df["review_id"] = state_df["review_id"].astype(str)
    df = df.merge(state_df, on="review_id", how="left", suffixes=("","_state"))
    # prioridade para o que veio do estado remoto
    df["usado"] = df["usado_state"].fillna(df.get("usado", False)).fillna(False).astype(bool)
    df["usado_em"] = df["usado_em_state"].fillna(df.get("usado_em", ""))
    df.drop(columns=[c for c in ["usado_state","usado_em_state"] if c in df.columns], inplace=True)
else:
    if "usado" not in df.columns: df["usado"] = False
    if "usado_em" not in df.columns: df["usado_em"] = ""

with st.sidebar:
    st.image("logo_click_cannabis.png", width=220)
    st.divider()
    st.header("Filtros")
    categorias_unicas = sorted(set(
        c.strip() for row in df["categorias_ia"].fillna("")
        for c in row.split(",") if c.strip()
    ))
    escolha = st.selectbox("Categoria", options=["TODOS"] + categorias_unicas, index=0)
    mostrar_nao_usados = st.checkbox("Mostrar só não usados", value=True)
    busca = st.text_input("Buscar (nome ou texto)", value="").strip()
    conf_min = 0.0
    page_size = st.selectbox("Itens por página", options=[10, 20, 50, 100], index=1)

# ================== FILTERING ==================
f = df.copy()
if escolha != "TODOS":
    f = f[f["categorias_ia"].fillna("").str.contains(rf"\b{escolha}\b", regex=True)]
if mostrar_nao_usados:
    f = f[~f["usado"]]
if busca:
    b = busca.lower()
    f = f[ f["autor_nome"].fillna("").str.lower().str.contains(b) | f["texto"].fillna("").str.lower().str.contains(b) ]
f = f[f["confianca_ia"].fillna(0.0).astype(float) >= conf_min]
if "data_iso" in f.columns:
    f["_ord"] = pd.to_datetime(f["data_iso"], errors="coerce", utc=True)
    f = f.sort_values("_ord", ascending=False).drop(columns=["_ord"])

st.subheader(f"Resultados: {len(f)} review(s)" + (f" — Categoria **{escolha}**" if escolha!="TODOS" else ""))

# ================== PAGINATION COM BOTÕES ==================
total_pages = (len(f) + page_size - 1) // page_size if len(f) > 0 else 1
if st.session_state.page > total_pages:
    st.session_state.page = 1
st.write("Página")
col1, col2, col3 = st.columns([2, 1, 2])
with col1:
    if st.button("⬅️ Anterior", use_container_width=True, disabled=(st.session_state.page <= 1)):
        st.session_state.page -= 1; st.rerun()
with col3:
    if st.button("Próxima ➡️", use_container_width=True, disabled=(st.session_state.page >= total_pages)):
        st.session_state.page += 1; st.rerun()
with col2:
    st.write(f"{st.session_state.page} de {total_pages}")

start, end = (st.session_state.page - 1) * page_size, (st.session_state.page - 1) * page_size + page_size
sub = f.iloc[start:end].copy()

# ================== RENDER CARDS ============
for idx, row in sub.iterrows():
    rid = str(row.get("review_id", ""))
    autor, autor_profile, _ = parse_author(row.get("autor_nome", ""))
    data = row.get("data_original", "") or "um mês atrás"
    rating = row.get("rating", "")
    rating_f = float(rating) if rating else None
    conf = float(row.get("confianca_ia", 0.0) or 0.0)
    cats = row.get("categorias_ia", "") or "—"
    texto = (row.get("texto", "") or "").strip() or "_(sem texto)_"
    imgs = split_imgs(row.get("imagens_do_review", ""))
    review_link = row.get("review_link", "")
    justificativa = row.get("justificativa_ia", "")

    safe_autor = html.escape(autor)
    safe_texto = html.escape(texto).replace("\n", "<br>")
    safe_cats = html.escape(cats)
    safe_justificativa = html.escape(justificativa)

    if imgs:
        links_content = ''.join([f'<a href="{u}" target="_blank">Foto {i}</a>' for i, u in enumerate(imgs, 1)])
    else:
        links_content = '<span class="no-photo-text">Sem Foto</span>'
    links_html = f'<div class="photo-links">{links_content}</div>'

    content_html = f"""
        <div class="review-header">
            <span class="author-name">{safe_autor} {f'<a href="{review_link}" target="_blank">↗</a>' if review_link else ''}</span>
            {'<span class="rating-stars">' + "⭐" * int(round(rating_f)) + f' {rating_f:.1f}</span>' if rating_f else ''}
            <span class="confidence-text">Confiança IA categorizar: {conf:.2f}</span>
            <span class="date-text">{data}</span>
        </div>
        <div class="review-text">{safe_texto}</div>
        {links_html}
        <div class="categories-text"><strong>Categorias IA:</strong> {safe_cats}</div>
        {'<div class="categories-text"><strong>Justificativa da IA:</strong> ' + safe_justificativa + '</div>' if justificativa else ''}
    """

    col1, col2 = st.columns([15, 2])
    with col1:
        st.markdown(f'<div class="content-box">{content_html}</div>', unsafe_allow_html=True)

    with col2:
        marcado = bool(row.get("usado", False))
        novo = st.toggle("Já usei", value=marcado, key=f"usado_{rid}_{idx}")
        if novo != marcado:
            # 1) Atualiza base em memória (apenas esta linha exibida)
            df.loc[df["review_id"].astype(str) == rid, "usado"] = novo
            df.loc[df["review_id"].astype(str) == rid, "usado_em"] = (
                datetime.now().strftime("%Y-%m-%d %H:%M:%S") if novo else ""
            )
            # 2) Persiste estado no GitHub
            sdf = load_state_df()
            if sdf.empty:
                sdf = pd.DataFrame(columns=["review_id","usado","usado_em"])
            sdf["review_id"] = sdf["review_id"].astype(str)
            if rid in set(sdf["review_id"]):
                sdf.loc[sdf["review_id"] == rid, ["usado","usado_em"]] = [
                    novo,
                    (datetime.now().strftime("%Y-%m-%d %H:%M:%S") if novo else "")
                ]
            else:
                sdf = pd.concat([
                    sdf,
                    pd.DataFrame([{
                        "review_id": rid,
                        "usado": novo,
                        "usado_em": (datetime.now().strftime("%Y-%m-%d %H:%M:%S") if novo else "")
                    }])
                ], ignore_index=True)

            ok_remote = save_state_df(sdf)
            # 3) (opcional) salva CSV principal localmente também
            save_df(df, CSV_PATH)

            if ok_remote:
                st.toast("Salvo no GitHub!", icon="✅")
            else:
                st.toast("Falha ao salvar no GitHub", icon="⚠️")
