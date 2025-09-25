#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
from datetime import datetime

import pandas as pd
from tqdm import tqdm
from tenacity import retry, stop_after_attempt, wait_exponential

from openai import OpenAI

CSV_PATH = "reviews_clickcannabis_ia.csv"   # base “final” que o app lê
BACKUP_DIR = "_backups"
MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")  # pode trocar por outro

CATEGORIAS = [
    "Atendimento", "É Golpe?", "Preço", "Click", "Sono", "Bem estar geral",
    "Ansiedade", "Dores", "Alzheimer", "Enxaqueca", "Autismo", "TDAH",
    "Fibromialgia", "Epilepsia", "Tabagismo", "Estresse",
    "Produto: óleo", "Produto: gummy", "Produto: tópico"
]

SYSTEM_PROMPT = f"""
Você é um assistente que classifica depoimentos de clientes em múltiplas categorias.
Regras:
- Leia o texto do review (português).
- Devolva JSON STRICTO (application/json), com as chaves:
  - categorias: lista de strings escolhidas de {CATEGORIAS}
  - justificativa: string curta (1–2 frases) explicando a escolha
  - confianca: número entre 0 e 1 (float)
- Se não houver texto útil, devolva categorias=[], justificativa="Sem texto", confianca=0.0
- Não invente fatos; use apenas o que está no texto do review.
"""

def backup_csv(path: str):
    """Salva cópia de segurança do CSV antes de atualizar."""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    bak = os.path.join(BACKUP_DIR, f"{os.path.basename(path)}.{ts}.bak.csv")
    try:
        df = pd.read_csv(path)
        df.to_csv(bak, index=False, encoding="utf-8-sig")
    except Exception:
        pass

def need_mask(df: pd.DataFrame):
    """Linhas que ainda precisam de IA (categorias_ia vazia/NaN)."""
    if "categorias_ia" not in df.columns:
        return pd.Series([True]*len(df), index=df.index)
    s = df["categorias_ia"].astype(str).str.strip()
    return (s.eq("")) | (s.eq("nan")) | (s.eq("None"))

@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=1, max=15))
def classify_text(client: OpenAI, texto: str) -> dict:
    """Chama a API para classificar um único review."""
    user_prompt = f"Texto do review:\n\"\"\"\n{texto}\n\"\"\"\n\nResponda SOMENTE o JSON pedido."
    resp = client.chat.completions.create(
        model=MODEL,
        temperature=0.1,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        response_format={ "type": "json_object" },  # força JSON
    )
    raw = resp.choices[0].message.content
    return json.loads(raw)

def main():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise SystemExit("Defina OPENAI_API_KEY no ambiente.")

    # Carregar CSV
    if not os.path.exists(CSV_PATH):
        raise SystemExit(f"Arquivo não encontrado: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)

    # Garantir colunas destino
    for col in ["categorias_ia", "justificativa_ia", "confianca_ia"]:
        if col not in df.columns:
            df[col] = ""

    mask = need_mask(df)
    idxs = df.index[mask].tolist()

    if not idxs:
        print("✅ Nada a classificar. Todas as linhas já possuem categorias_ia.")
        return

    backup_csv(CSV_PATH)
    client = OpenAI(api_key=api_key)

    print(f"Classificando apenas as linhas novas: {len(idxs)} item(ns). Modelo: {MODEL}")

    # Loop item-a-item (mais robusto p/ quedas de rede / rate limit)
    atualizados = 0
    for i in tqdm(idxs, desc="Classificando"):
        texto = str(df.at[i, "texto"] or "").strip()
        if not texto:
            df.at[i, "categorias_ia"] = ""
            df.at[i, "justificativa_ia"] = "Sem texto"
            df.at[i, "confianca_ia"] = 0.0
            atualizados += 1
            continue

        try:
            out = classify_text(client, texto)
            cats = out.get("categorias", [])
            just = out.get("justificativa", "")
            conf = out.get("confianca", 0)

            # Normalizações leves
            if isinstance(cats, list):
                cats = ", ".join([c.strip() for c in cats if c and isinstance(c, str)])
            else:
                cats = str(cats)

            try:
                conf = float(conf)
            except Exception:
                conf = 0.0

            df.at[i, "categorias_ia"] = cats
            df.at[i, "justificativa_ia"] = just
            df.at[i, "confianca_ia"] = conf
            atualizados += 1

        except Exception as e:
            # Em caso de erro, mantém vazio e segue
            df.at[i, "categorias_ia"] = ""
            df.at[i, "justificativa_ia"] = f"Erro: {e}"
            df.at[i, "confianca_ia"] = 0.0

        # checkpoint a cada 50
        if atualizados % 50 == 0:
            df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

        # Respiro leve para evitar rate-limit agressivo
        time.sleep(0.05)

    # Salvar final
    df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    print(f"✅ Classificação concluída. Atualizadas {atualizados} linha(s).")

if __name__ == "__main__":
    main()
