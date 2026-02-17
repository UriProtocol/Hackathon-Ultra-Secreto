import os
import re
import time
from typing import List, Dict, Any

import psycopg2
import requests
import chromadb

from core.config import settings


# -------------------------
# Config
# -------------------------
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434").rstrip("/")
OLLAMA_EMBED_MODEL = os.getenv("OLLAMA_EMBED_MODEL", "nomic-embed-text")
CHROMA_COLLECTION = os.getenv("CHROMA_COLLECTION", "documents")

# nomic-embed-text suele ir bien con chunks moderados
MAX_EMBED_CHARS = int(os.getenv("OLLAMA_MAX_CHARS", "3000"))

# Chunking
CHUNK_MAX_CHARS = int(os.getenv("CHUNK_MAX_CHARS", "1200"))
CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", "150"))


# -------------------------
# Supabase connection
# -------------------------
def get_connection():
    return psycopg2.connect(
        host=settings.SUPABASE_HOST,
        database=settings.SUPABASE_DB,
        user=settings.SUPABASE_USER,
        password=settings.SUPABASE_PASSWORD,
        port=int(settings.SUPABASE_PORT or 5432),
        sslmode="require",
    )


def fetch_pending_web_metadata_for_embedding(limit: int = 10) -> List[Dict[str, Any]]:
    """
    Trae filas con cleaned_text y embedded_at NULL
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
          wm.document_id,
          wm.url,
          COALESCE(d.title, '') as title,
          COALESCE(wm.data->>'cleaned_text', '') as cleaned_text
        FROM web_metadata wm
        LEFT JOIN documents d ON d.id = wm.document_id
        WHERE COALESCE(wm.data->>'cleaned_text', '') <> ''
          AND wm.embedded_at IS NULL
        ORDER BY wm.created_at DESC
        LIMIT %s
        """,
        (limit,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {"document_id": r[0], "url": r[1], "title": r[2], "cleaned_text": r[3]}
        for r in rows
    ]


def mark_embedded(document_id: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE web_metadata
        SET embedded_at = NOW()
        WHERE document_id = %s
        """,
        (document_id,),
    )
    conn.commit()
    cur.close()
    conn.close()


# -------------------------
# Chroma
# -------------------------
def get_chroma_collection():
    client = chromadb.CloudClient(
        api_key=settings.CHROMA_API_KEY,
        tenant=settings.CHROMA_TENANT,
        database=settings.CHROMA_DATABASE,
    )
    return client.get_or_create_collection(CHROMA_COLLECTION)


# -------------------------
# Cleaning (extra, antes de chunking/embedding)
# -------------------------
def clean_text_for_embedding(text: str) -> str:
    if not text:
        return ""

    # quitar URLs visibles (ya están en metadata)
    text = re.sub(r"https?://\S+|www\.\S+", " ", text)

    # normalizar espacios
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)

    # unir palabras cortadas por guión en saltos
    text = re.sub(r"([A-Za-zÁÉÍÓÚÜÑáéíóúüñ])-\s+([A-Za-zÁÉÍÓÚÜÑáéíóúüñ])", r"\1\2", text)

    return text.strip()


# -------------------------
# Chunking (evita cortar palabras)
# -------------------------
def chunk_text(text: str, max_chars: int = CHUNK_MAX_CHARS, overlap: int = CHUNK_OVERLAP) -> List[str]:
    text = (text or "").strip()
    if not text:
        return []

    chunks = []
    i = 0
    n = len(text)

    while i < n:
        end = min(n, i + max_chars)

        # evita cortar palabra: retrocede hasta un separador razonable
        if end < n:
            back = end
            while back > i and text[back - 1] not in [" ", "\n", ".", "!", "?", ",", ";", ":"]:
                back -= 1
            # si encontramos separador y no nos quedamos con un chunk tiny, úsalo
            if back > i + int(max_chars * 0.6):
                end = back

        chunk = text[i:end].strip()
        if chunk:
            chunks.append(chunk)

        if end >= n:
            break
        i = max(0, end - overlap)

    return chunks


# -------------------------
# Ollama embeddings (remote)
# -------------------------
def ollama_embed(texts: List[str]) -> List[List[float]]:
    embeddings: List[List[float]] = []

    for t in texts:
        t = (t or "")[:MAX_EMBED_CHARS]

        r = requests.post(
            f"{OLLAMA_URL}/api/embeddings",
            json={"model": OLLAMA_EMBED_MODEL, "prompt": t},
            timeout=90,
        )
        if r.status_code != 200:
            raise RuntimeError(f"Ollama error {r.status_code}: {r.text}")

        data = r.json()
        emb = data.get("embedding")
        if not emb:
            raise RuntimeError(f"Unexpected Ollama response: {data}")

        embeddings.append(emb)

    return embeddings


# -------------------------
# Worker
# -------------------------
def run(limit_docs: int = 10, sleep_s: float = 0.1):
    col = get_chroma_collection()

    pending = fetch_pending_web_metadata_for_embedding(limit=limit_docs)
    if not pending:
        print("[embed_ollama] No hay pendientes (embedded_at IS NULL).")
        return

    print(f"[embed_ollama] Procesando {len(pending)} docs | ollama={OLLAMA_URL} model={OLLAMA_EMBED_MODEL} | collection={CHROMA_COLLECTION}")

    for d in pending:
        doc_id = str(d["document_id"])
        url = d["url"]
        title = d["title"] or ""

        cleaned = clean_text_for_embedding(d["cleaned_text"] or "")

        try:
            chunks = chunk_text(cleaned)
            if not chunks:
                print(f"[embed_ollama] ⚠️ sin chunks doc_id={doc_id}")
                mark_embedded(doc_id)
                continue

            # IDs deterministas
            ids = [f"web:{doc_id}:{i}" for i in range(len(chunks))]

            # Embeddings uno-por-uno (como tu función). Si luego quieres, lo optimizamos con batch.
            embs = ollama_embed(chunks)

            metadatas = [
                {"source": "web", "document_id": doc_id, "chunk_index": i, "url": url, "title": title}
                for i in range(len(chunks))
            ]

            col.upsert(ids=ids, documents=chunks, embeddings=embs, metadatas=metadatas)

            mark_embedded(doc_id)
            print(f"[embed_ollama] ✅ embedded doc_id={doc_id} chunks={len(chunks)} url={url}")

        except Exception as e:
            print(f"[embed_ollama] ❌ error doc_id={doc_id} url={url} err={e}")

        time.sleep(sleep_s)


if __name__ == "__main__":
    run(limit_docs=10, sleep_s=0.1)