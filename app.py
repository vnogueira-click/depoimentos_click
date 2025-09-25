import os
import ast
import base64
import warnings
from datetime import datetime
import html

import pandas as pd
import streamlit as st

# ================== CONFIG ==================
CSV_PATH    = "reviews_clickcannabis_ia.csv"
BACKUP_DIR  = "_backups"
BRAND_GREEN = "#006f19"
PAGE_BG     = "#f1f2f2"

warnings.filterwarnings("ignore", message="Could not infer format")

# ================== UTILS ===================
def b64img(path: str) -> str:
    try:
        with open(path, "rb") as f:
            return "data:image/png;base64," + base64.b64encode(f.read()).decode()
    except Exception:
        return ""

def save_df(df: pd.DataFrame, path: str) -> None:
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

# ================== PAGE SETUP ===============
st.set_page_config(page_title="Reviews ClickCannabis", layout="wide")

st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    body, .stApp {{
        font-family: 'Inter', sans-serif;
        background-color: {PAGE_BG};
    }}
    
    .block-container {{
        padding: 1.5rem 2rem;
        max-width: 1200px;
    }}
    
    .content-box {{
        background-color: #FFFFFF;
        border: 1px solid #e5e7eb;
        border-radius: 12px;
        padding: 20px;
        height: 100%;
        margin-bottom: 1.5rem;
    }}

    div[data-testid="stHorizontalBlock"] {{
        align-items: center;
    }}
    
    [data-testid="stSidebarCollapseButton"] {{
        display: none;
    }}

    /* --- AJUSTE FINO PARA POSIÇÃO DO LOGO --- */
    [data-testid="stSidebar"] [data-testid="stImage"] {{
        margin-top: -14px;
    }}
    /* --- FIM DO AJUSTE --- */

    div[data-testid="stSelectbox"] div[data-baseweb="select"],
    div[data-testid="stTextInput"] input {{
        border: 1px solid #ced4da;
        border-radius: 0.5rem;
    }}

    div[data-testid="stSelectbox"] div[data-baseweb="select"]:focus-within,
    div[data-testid="stTextInput"] input:focus {{
        border-color: transparent;
    }}
    
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

with st.sidebar:
    st.image("logo_click_cannabis.png", width=220)
    st.divider()
    st.header("Filtros")
    
    categorias_unicas = sorted(set( c.strip() for row in df["categorias_ia"].fillna("") for c in row.split(",") if c.strip() ))
    
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
            df.loc[df.index == idx, "usado"] = novo
            df.loc[df.index == idx, "usado_em"] = (datetime.now().strftime("%Y-%m-%d %H:%M:%S") if novo else "")
            save_df(df, CSV_PATH)
            st.toast("Salvo!", icon="✅")
