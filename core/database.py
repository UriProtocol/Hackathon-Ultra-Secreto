import psycopg2
from core.config import settings
import json
from psycopg2.extras import execute_values

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


def insert_openalex_full(work):
    """
    Inserta documento + academic_metadata
    en una sola transacción.
    """

    if not work.get("doi"):
        return None

    conn = get_connection()
    cur = conn.cursor()

    try:
        # 1️⃣ Insertar documento
        cur.execute("""
            INSERT INTO documents (
                source_type,
                canonical_identifier,
                title,
                raw_text
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (canonical_identifier) DO UPDATE
            SET title = EXCLUDED.title
            RETURNING id
        """, (
            "openalex",
            work.get("openalex_id"),
            work.get("title"),
            work.get("abstract")
        ))

        document_id = cur.fetchone()[0]

        # 2️⃣ Insertar metadata
        cur.execute("""
            INSERT INTO academic_metadata (
                document_id,
                doi,
                journal_name,
                publisher,
                issn,
                publication_year,
                citation_count,
                is_open_access,
                open_access_url,
                authors,
                institutions,
                concepts,
                raw_source
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (doi) DO UPDATE
            SET
                citation_count = EXCLUDED.citation_count,
                updated_at = NOW()
        """, (
            document_id,
            work.get("doi"),
            work.get("journal_name"),
            work.get("publisher"),
            work.get("issn"),
            work.get("publication_year"),
            work.get("citation_count"),
            work.get("is_open_access"),
            work.get("open_access_url"),
            json.dumps(work.get("authors")),
            json.dumps(work.get("institutions")),
            json.dumps(work.get("concepts")),
            json.dumps(work.get("raw_source"))
        ))

        conn.commit()

        return document_id

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        cur.close()
        conn.close()

def bulk_insert_institutions(institutions_generator):

    conn = get_connection()
    cur = conn.cursor()

    for inst in institutions_generator:
        cur.execute("""
            INSERT INTO institutions_catalog (
                openalex_id,
                display_name,
                city,
                type,
                works_count
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (openalex_id)
            DO UPDATE SET
                display_name = EXCLUDED.display_name,
                city = EXCLUDED.city,
                type = EXCLUDED.type,
                works_count = EXCLUDED.works_count,
                updated_at = NOW()
        """, (
            inst.get("id"),
            inst.get("display_name"),
            inst.get("city"),
            inst.get("type"),
            inst.get("works_count")
        ))

    conn.commit()
    cur.close()
    conn.close()


def get_nuevo_leon_institution_ids():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT openalex_id FROM institutions_catalog")
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return [row[0] for row in rows]


def _flush_batch(cur, documents_batch, metadata_batch):

    # ---------------------------------
    # 1️⃣ Insert documents en bulk
    # ---------------------------------
    insert_documents_sql = """
        INSERT INTO documents (
            source_type,
            canonical_identifier,
            title,
            raw_text
        )
        VALUES %s
        ON CONFLICT (canonical_identifier)
        DO UPDATE SET title = EXCLUDED.title
        RETURNING id, canonical_identifier
    """

    result = execute_values(
        cur,
        insert_documents_sql,
        documents_batch,
        fetch=True
    )

    # Map canonical_identifier → document_id
    doc_id_map = {row[1]: row[0] for row in result}

    # ---------------------------------
    # 2️⃣ Preparar academic_metadata
    # ---------------------------------
    metadata_values = []

    for w in metadata_batch:
        identifier = w["canonical_identifier"]

        if identifier not in doc_id_map:
            continue

        metadata_values.append((
            doc_id_map[identifier],
            w["doi"],
            w["journal_name"],
            w["publisher"],
            w["issn"],
            w["publication_year"],
            w["citation_count"],
            w["is_open_access"],
            w["open_access_url"],
            json.dumps(w["authors"]),
            json.dumps(w["institutions"]),
            json.dumps(w["concepts"]),
            json.dumps(w["raw_source"])
        ))

    if not metadata_values:
        return

    insert_metadata_sql = """
        INSERT INTO academic_metadata (
            document_id,
            doi,
            journal_name,
            publisher,
            issn,
            publication_year,
            citation_count,
            is_open_access,
            open_access_url,
            authors,
            institutions,
            concepts,
            raw_source
        )
        VALUES %s
        ON CONFLICT (doi)
        DO UPDATE SET
            citation_count = EXCLUDED.citation_count,
            updated_at = NOW()
    """

    execute_values(
        cur,
        insert_metadata_sql,
        metadata_values
    )
