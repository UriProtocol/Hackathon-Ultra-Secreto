import json
import os
import time
import requests

from core.database import fetch_pending_web_metadata, upsert_web_metadata

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:7b")

SYSTEM = (
    "Eres un extractor de información. Devuelve SOLO JSON válido, sin markdown. "
    "NO inventes datos: si no está en el texto, usa null o []."
)

def call_ollama_json(prompt: str) -> dict:
    url = f"{OLLAMA_HOST}/api/generate"
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.1,
            "num_ctx": 4096,
        },
    }

    r = requests.post(url, json=payload, timeout=180)
    r.raise_for_status()
    out = r.json()

    text = (out.get("response") or "").strip()

    # intentar parseo directo
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # fallback: recorta al primer { ... último }
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(text[start:end + 1])
        raise


def build_prompt(url: str, title: str, raw_text: str) -> str:
    # recorta para no saturar el contexto
    raw_text = (raw_text or "").strip()
    if len(raw_text) > 20000:
        raw_text = raw_text[:20000]

    return f"""
{SYSTEM}

URL: {url}
TITLE: {title}

TEXTO (MARKDOWN):
{raw_text}

Devuelve un JSON con este esquema EXACTO (puedes dejar campos vacíos, pero NO cambies los nombres):

{{
  "summary": string,
  "topics": [string],
  "entity_type": string,
  "entities": [{{"type": string, "name": string}}],
  "key_fields": {{
    "emails": [string],
    "phones": [string],
    "locations": [string],
    "dates": [string],
    "links": [string]
  }},
  "structured": object,
  "evidence": [{{"claim": string, "snippet": string, "source_url": string}}]
}}

Reglas:
- summary máximo 600 caracteres.
- topics máximo 10.
- evidence máximo 5; "snippet" debe ser literal del texto.
- Si no hay evidencia para un claim, NO lo incluyas.
""".strip()


def run(limit=2, sleep_s=0.2):
    docs = fetch_pending_web_metadata(limit=limit)
    if not docs:
        print("[normalize_ollama] No hay pendientes.")
        return

    for d in docs:
        doc_id = d["id"]
        url = d["url"]
        title = d["title"]
        raw_text = d["raw_text"]

        try:
            prompt = build_prompt(url, title, raw_text)
            data = call_ollama_json(prompt)

            upsert_web_metadata(
                document_id=doc_id,
                url=url,
                content_type="text/markdown",
                data=data,
            )

            print(f"[normalize_ollama] ✅ upsert web_metadata doc_id={doc_id} url={url}")

        except Exception as e:
            print(f"[normalize_ollama] ❌ error doc_id={doc_id} url={url} err={e}")

        time.sleep(sleep_s)


if __name__ == "__main__":
    run(limit=2)