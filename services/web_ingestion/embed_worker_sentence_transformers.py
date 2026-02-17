import os
import time
from typing import List, Dict, Any

import psycopg2
from sentence_transformers import SentenceTransformer

import chromadb
from core.config import settings


# -------------------------
# Chroma client + collection
# -------------------------
def get_chroma_collection():
    client = chromadb.CloudClient(
        api_key=settings.CHROMA_API_KEY,
        tenant=settings.CHROMA_TENANT,
        database=settings.CHROMA_DATABASE,
    )
    # Colección donde guardaremos chunks web
    return client.get_or_create_collection("documents")


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


# -------------------------
# Fetch pending web_metadata (not embedded)
# -------------------------
def fetch_pending_web_metadata_for_embedding(limit: int = 10) -> List[Dict[str, Any]]:
    """
    Trae filas de web_metadata cuyo JSONB data tiene cleaned_text,
    y que aún no se han embebido (embedded_at IS NULL).
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
# Chunking
# -------------------------
def chunk_text(text: str, max_chars: int = 1200, overlap: int = 150) -> List[str]:
    """
    Chunk simple por caracteres (suficiente para prototipo).
    - max_chars: tamaño aprox del chunk
    - overlap: traslape para no cortar contexto duro
    """
    text = (text or "").strip()
    if not text:
        return []

    chunks = []
    i = 0
    n = len(text)

    while i < n:
        end = min(n, i + max_chars)
        chunk = text[i:end].strip()
        if chunk:
            chunks.append(chunk)
        if end == n:
            break
        i = max(0, end - overlap)

    return chunks


# -------------------------
# Main worker
# -------------------------
def run(limit_docs: int = 5, sleep_s: float = 0.1):
    """
    1) Trae docs pendientes de web_metadata
    2) Parte en chunks
    3) Genera embeddings localmente (Sentence Transformers)
    4) Upsert a Chroma
    5) Marca embedded_at
    """
    # Modelo liviano y muy usado (rápido en CPU)
    model_name = os.getenv("ST_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
    model = SentenceTransformer(model_name)

    col = get_chroma_collection()

    pending = fetch_pending_web_metadata_for_embedding(limit=limit_docs)
    if not pending:
        print("[embed_st] No hay pendientes (embedded_at IS NULL).")
        return

    print(f"[embed_st] Procesando {len(pending)} documentos... model={model_name}")

    for d in pending:
        doc_id = str(d["document_id"])
        url = d["url"]
        title = d["title"] or ""
        cleaned = d["cleaned_text"] or ""

        try:
            chunks = chunk_text(cleaned, max_chars=1200, overlap=150)
            if not chunks:
                print(f"[embed_st] ⚠️ sin chunks doc_id={doc_id}")
                mark_embedded(doc_id)
                continue

            # IDs deterministas => rerun idempotente
            ids = [f"web:{doc_id}:{i}" for i in range(len(chunks))]

            # Embeddings en batch
            embeddings = model.encode(
                chunks,
                normalize_embeddings=True,  # mejora similarity en muchos casos
                show_progress_bar=False,
            ).tolist()

            metadatas = [
                {
                    "source": "web",
                    "document_id": doc_id,
                    "chunk_index": i,
                    "url": url,
                    "title": title,
                }
                for i in range(len(chunks))
            ]

            # Upsert a Chroma
            col.upsert(
                ids=ids,
                documents=chunks,
                embeddings=embeddings,
                metadatas=metadatas,
            )

            mark_embedded(doc_id)
            print(f"[embed_st] ✅ embedded doc_id={doc_id} chunks={len(chunks)} url={url}")

        except Exception as e:
            print(f"[embed_st] ❌ error doc_id={doc_id} url={url} err={e}")

        time.sleep(sleep_s)


if __name__ == "__main__":
    run(limit_docs=5, sleep_s=0.1)