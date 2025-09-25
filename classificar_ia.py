import os, json, time
import pandas as pd
from typing import List, Dict, Any
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from tqdm import tqdm
from openai import OpenAI

# ===== CONFIG =====
INPUT_CSV  = "reviews_clickcannabis_COM_TEXTO.csv"   # use o CSV filtrado
OUTPUT_CSV = "reviews_clickcannabis_ia.csv"
CHECKPOINT = "reviews_clickcannabis_ia.jsonl"
MODEL = "gpt-4o-mini"
BATCH_SIZE = 25          # ajuste se quiser
MAX_REVIEWS = None       # ex.: 300 para testar em amostra
TIMEOUT_S = 120

LABELS = [
 "Atendimento","É Golpe?","Preço","Click",
 "Sono","Bem estar geral","Ansiedade","Dores","Alzheimer",
 "Enxaqueca","Autismo","TDAH","Fibromialgia","Epilepsia",
 "Tabagismo","Estresse","Produto: Óleo","Produto: Gummy","Produto: Tópico"
]

LABEL_GUIDE = {
 "Atendimento": "Atendimento/suporte/funcionários/pós-venda.",
 "É Golpe?": "Confiabilidade/segurança, menções a golpe/fraude/scam.",
 "Preço": "Preço, caro/barato, custo-benefício, frete.",
 "Click": "Menções à Click Cannabis (marca/loja/site).",
 "Sono": "Dormir melhor/pior, insônia.",
 "Bem estar geral": "Bem-estar geral, qualidade de vida, relaxamento.",
 "Ansiedade": "Controle/redução da ansiedade, crises.",
 "Dores": "Dores e alívio de dor não específico.",
 "Alzheimer": "Citações a Alzheimer/demência.",
 "Enxaqueca": "Enxaqueca/cefaleia/migrânea.",
 "Autismo": "Autismo/TEA.",
 "TDAH": "Déficit de atenção/hiperatividade (TDAH).",
 "Fibromialgia": "Fibromialgia.",
 "Epilepsia": "Epilepsia/convulsões.",
 "Tabagismo": "Parar de fumar/cessação do tabaco.",
 "Estresse": "Estresse/tensão/sobrecarga.",
 "Produto: Óleo": "Óleo sublingual (‘óleo’, ‘oleo’, ‘olio’).",
 "Produto: Gummy": "Gomas/balas/jelly/gummy.",
 "Produto: Tópico": "Pomada/creme/gel/tópico na pele."
}

SYSTEM_PROMPT = f"""
Você é um classificador multilabel de depoimentos PT-BR. Regras:
- Use apenas estes rótulos: {LABELS}.
- Aplique 0..N rótulos por depoimento, com bom senso (não dependa só de palavras exatas).
- Retorne JSON válido no formato pedido. Dê uma justificativa curta (1–2 frases).
- Siga estas definições:
{json.dumps(LABEL_GUIDE, ensure_ascii=False, indent=2)}
"""

client = OpenAI(timeout=TIMEOUT_S)

def build_user_payload(items: List[Dict[str, Any]]) -> str:
    bloco = []
    for it in items:
        t = (it.get("texto") or "").strip()
        bloco.append({"id": it["idx"], "texto": t[:4000]})
    task = {
        "tarefa": "Classificar multilabel cada depoimento nos rótulos predefinidos.",
        "rotulos": LABELS,
        "formato_resposta": {
            "type": "object",
            "properties": {
                "itens": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "id": {"type":"integer"},
                            "categorias": {"type":"array","items":{"type":"string"}},
                            "confianca": {"type":"number"},
                            "justificativa": {"type":"string"}
                        },
                        "required": ["id","categorias","confianca","justificativa"]
                    }
                }
            },
            "required": ["itens"]
        },
        "itens": bloco
    }
    return json.dumps(task, ensure_ascii=False)

@retry(reraise=True, stop=stop_after_attempt(5),
       wait=wait_exponential(min=2, max=20),
       retry=retry_if_exception_type(Exception))
def classify_batch(batch_rows: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    user_payload = build_user_payload(batch_rows)
    resp = client.chat.completions.create(
        model=MODEL, temperature=0.0,
        response_format={"type": "json_object"},
        messages=[{"role":"system","content": SYSTEM_PROMPT},
                  {"role":"user","content": user_payload}]
    )
    data = json.loads(resp.choices[0].message.content)
    out = {}
    for item in data.get("itens", []):
        idx = int(item["id"])
        out[idx] = {
            "categorias": [c for c in item.get("categorias", []) if c in LABELS],
            "confianca": float(item.get("confianca", 0.0)),
            "justificativa": item.get("justificativa", "")
        }
    return out

def main():
    if not os.getenv("OPENAI_API_KEY"):
        raise SystemExit("Defina OPENAI_API_KEY (export OPENAI_API_KEY='...').")

    if not os.path.exists(INPUT_CSV):
        raise SystemExit(f"Arquivo não encontrado: {INPUT_CSV}")

    base = pd.read_csv(INPUT_CSV)
    if "texto" not in base.columns:
        raise SystemExit("CSV precisa ter coluna 'texto'.")

    # montar lista a classificar
    rows = [{"idx": i, "texto": row["texto"]} for i, row in base.iterrows()]
    if MAX_REVIEWS is not None:
        rows = rows[:MAX_REVIEWS]

    # se já existe OUTPUT_CSV, retoma sem duplicar
    done_keys = set()
    if os.path.exists(OUTPUT_CSV):
        done = pd.read_csv(OUTPUT_CSV)
        done_keys = set(done.index.tolist())

    results_map = {}
    for start in tqdm(range(0, len(rows), BATCH_SIZE), desc="Classificando"):
        batch = rows[start:start+BATCH_SIZE]
        # pular itens já existentes (retomada simples)
        batch = [b for b in batch if b["idx"] not in done_keys]
        if not batch:
            continue

        out = classify_batch(batch)

        # checkpoint bruto
        with open(CHECKPOINT, "a", encoding="utf-8") as f:
            for it in batch:
                idx = it["idx"]
                res = out.get(idx, {"categorias":[],"confianca":0.0,"justificativa":""})
                f.write(json.dumps({"idx": idx, **res}, ensure_ascii=False) + "\n")

        # salvar/atualizar CSV incrementalmente
        part = []
        for it in batch:
            idx = it["idx"]
            row = base.loc[idx].to_dict()
            res = out.get(idx, {"categorias":[],"confianca":0.0,"justificativa":""})
            row["categorias_ia"] = ", ".join(res["categorias"])
            row["confianca_ia"] = res["confianca"]
            row["justificativa_ia"] = res["justificativa"]
            part.append(row)
        part_df = pd.DataFrame(part)

        if os.path.exists(OUTPUT_CSV):
            existing = pd.read_csv(OUTPUT_CSV)
            merged = pd.concat([existing, part_df], ignore_index=True)
            merged = merged.drop_duplicates(subset=["review_id","texto"], keep="last") \
                     if "review_id" in merged.columns \
                     else merged.drop_duplicates(subset=["autor_nome","data_original","texto"], keep="last")
            merged.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
        else:
            part_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

        time.sleep(0.5)

    print(f"\n✅ Classificação concluída. Arquivo salvo: {OUTPUT_CSV}")
    # resumo rápido
    final = pd.read_csv(OUTPUT_CSV)
    counts = {}
    for lab in LABELS:
        counts[lab] = int(final["categorias_ia"].fillna("").str.contains(rf"\b{lab}\b", regex=True).sum())
    pd.DataFrame(sorted(counts.items(), key=lambda x:(-x[1], x[0])), columns=["tema","qtd"]) \
      .to_csv("resumo_categorias_ia.csv", index=False, encoding="utf-8-sig")
    print("📊 Resumo salvo em: resumo_categorias_ia.csv")

if __name__ == "__main__":
    main()
