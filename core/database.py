import psycopg2
from core.config import settings


# -------------------------
# Connection
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
# Insert / Upsert documents
# -------------------------
def insert_document(source_type, identifier, title, raw_text=None):
    """
    Inserta o actualiza un documento.
    - Deduplicación por canonical_identifier
    - raw_text normalmente es None en fase discovery
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO documents (source_type, canonical_identifier, title, raw_text)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (canonical_identifier) DO UPDATE SET
            title = EXCLUDED.title
        RETURNING id
        """,
        (source_type, identifier, title, raw_text),
    )

    result = cur.fetchone()

    conn.commit()
    cur.close()
    conn.close()

    return result[0] if result else None


# -------------------------
# Update raw_text
# -------------------------
def update_document_raw_text_by_identifier(identifier, raw_text):
    """
    Usado por Crawl4AI cuando procesa una URL
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE documents
        SET raw_text = %s
        WHERE canonical_identifier = %s
        RETURNING id
        """,
        (raw_text, identifier),
    )

    result = cur.fetchone()

    conn.commit()
    cur.close()
    conn.close()

    return result[0] if result else None


def update_document_raw_text_by_id(doc_id, raw_text):
    """
    Variante más segura cuando ya tienes el id
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE documents
        SET raw_text = %s
        WHERE id = %s
        """,
        (raw_text, doc_id),
    )

    conn.commit()
    cur.close()
    conn.close()


# -------------------------
# Fetch pending documents
# -------------------------
def fetch_pending_web_documents(limit=10):
    """
    Devuelve documentos descubiertos por SerpAPI
    que aún no han sido crawleados.
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id, canonical_identifier
        FROM documents
        WHERE source_type = 'serpapi'
          AND raw_text IS NULL
        ORDER BY created_at ASC
        LIMIT %s
        """,
        (limit,),
    )

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return [{"id": r[0], "url": r[1]} for r in rows]

    import json

def fetch_pending_web_metadata(limit=5):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT d.id, d.canonical_identifier, d.title, d.raw_text
        FROM documents d
        LEFT JOIN web_metadata wm ON wm.document_id = d.id
        WHERE d.source_type = 'serpapi'
          AND d.raw_text IS NOT NULL
          AND wm.document_id IS NULL
        ORDER BY d.created_at ASC
        LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {"id": r[0], "url": r[1], "title": r[2] or "", "raw_text": r[3] or ""}
        for r in rows
    ]

def upsert_web_metadata(document_id, url, content_type, data: dict):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO web_metadata (document_id, url, content_type, data)
        VALUES (%s, %s, %s, %s::jsonb)
        ON CONFLICT (document_id) DO UPDATE SET
          url = EXCLUDED.url,
          content_type = EXCLUDED.content_type,
          data = EXCLUDED.data
    """, (document_id, url, content_type, json.dumps(data)))
    conn.commit()
    cur.close()
    conn.close()