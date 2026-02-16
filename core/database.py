import psycopg2
from core.config import settings

def get_connection():
    return psycopg2.connect(
        host=settings.SUPABASE_HOST,
        database=settings.SUPABASE_DB,
        user=settings.SUPABASE_USER,
        password=settings.SUPABASE_PASSWORD,
        port=settings.SUPABASE_PORT,
        sslmode="require"
    )

def insert_document(source_type, identifier, title, raw_text=None):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO documents (source_type, canonical_identifier, title, raw_text)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (canonical_identifier) DO NOTHING
        RETURNING id
    """, (source_type, identifier, title, raw_text))

    result = cur.fetchone()

    conn.commit()
    cur.close()
    conn.close()

    return result[0] if result else None
