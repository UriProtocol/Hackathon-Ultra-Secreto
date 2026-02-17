import json
import psycopg2
from core.config import settings
import json
from psycopg2.extras import execute_values


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

# -------------------------
# Fetch pending documents (crawl)
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
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (limit,),
    )

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [{"id": r[0], "url": r[1]} for r in rows]


# -------------------------
# Fetch pending web_metadata (create row if missing)
# -------------------------
def fetch_pending_web_metadata(limit=5):
    """
    Docs con raw_text pero sin fila en web_metadata.
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT d.id, d.canonical_identifier, d.title, d.raw_text
        FROM documents d
        LEFT JOIN web_metadata wm ON wm.document_id = d.id
        WHERE d.source_type = 'serpapi'
          AND d.raw_text IS NOT NULL
          AND wm.document_id IS NULL
        ORDER BY d.created_at DESC
        LIMIT %s
        """,
        (limit,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {"id": r[0], "url": r[1], "title": r[2] or "", "raw_text": r[3] or ""}
        for r in rows
    ]


# -------------------------
# Fetch web_metadata that needs refresh (cleaned_text empty or wrong version)
# -------------------------
def fetch_web_metadata_needing_refresh(limit=50):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT d.id, d.canonical_identifier, d.title, d.raw_text
        FROM documents d
        JOIN web_metadata wm ON wm.document_id = d.id
        WHERE d.source_type = 'serpapi'
          AND d.raw_text IS NOT NULL
          AND (
            COALESCE(wm.data->>'cleaned_text','') = ''
            OR COALESCE(wm.data->>'version','') <> 'det_v1'
          )
        ORDER BY d.created_at DESC
        LIMIT %s
        """,
        (limit,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {"id": r[0], "url": r[1], "title": r[2] or "", "raw_text": r[3] or ""}
        for r in rows
    ]


# -------------------------
# Upsert web_metadata
# -------------------------
def upsert_web_metadata(document_id, url, content_type, data: dict):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO web_metadata (document_id, url, content_type, data)
        VALUES (%s, %s, %s, %s::jsonb)
        ON CONFLICT (document_id) DO UPDATE SET
          url = EXCLUDED.url,
          content_type = EXCLUDED.content_type,
          data = EXCLUDED.data
        """,
        (document_id, url, content_type, json.dumps(data)),
    )
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
    """Insert documents, metadata, authors and their relationships"""
    
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
    # 2️⃣ Insert/Update authors
    # ---------------------------------
    all_authors = []
    for w in metadata_batch:
        all_authors.extend(w.get("authors_list", []))
    
    # Deduplicate authors by ID
    unique_authors = {a["openalex_id"]: a for a in all_authors if a.get("openalex_id")}.values()
    
    if unique_authors:
        insert_authors_sql = """
            INSERT INTO authors 
            (openalex_id, display_name, orcid, last_known_institution_id, works_count, cited_by_count)
            VALUES %s
            ON CONFLICT (openalex_id) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                orcid = EXCLUDED.orcid,
                updated_at = CURRENT_TIMESTAMP
        """
        
        author_values = [
            (
                a["openalex_id"],
                a.get("display_name"),
                a.get("orcid"),
                a.get("last_known_institution_id"),
                a.get("works_count", 0),
                a.get("cited_by_count", 0)
            )
            for a in unique_authors
        ]
        
        execute_values(cur, insert_authors_sql, author_values)

    # ---------------------------------
    # 3️⃣ Insert academic_metadata
    # ---------------------------------
    metadata_values = []

    for w in metadata_batch:
        identifier = w["canonical_identifier"]

        if identifier not in doc_id_map:
            continue

        metadata_values.append((
            doc_id_map[identifier],  # document_id
            w.get("doi"),
            w.get("journal_name"),
            w.get("publisher"),
            w.get("issn"),
            w.get("publication_year"),
            w.get("citation_count"),
            w.get("is_open_access"),
            w.get("open_access_url"),
            json.dumps(w.get("authors", [])),
            json.dumps(w.get("institutions", [])),
            json.dumps(w.get("concepts", [])),
            json.dumps(w.get("raw_source", {}))
        ))

    if metadata_values:
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
        
        execute_values(cur, insert_metadata_sql, metadata_values)

    # ---------------------------------
    # 4️⃣ Insert academic_metadata_institutions pivot
    # ---------------------------------
    inst_pivot_values = []

    for w in metadata_batch:
        identifier = w["canonical_identifier"]

        if identifier not in doc_id_map:
            continue

        document_id = doc_id_map[identifier]

        for inst in w.get("institutions", []):
            openalex_id = inst.get("id")

            if not openalex_id:
                continue

            inst_pivot_values.append((
                document_id,
                openalex_id
            ))

    if inst_pivot_values:
        insert_inst_pivot_sql = """
            INSERT INTO academic_metadata_institutions (
                document_id,
                institution_openalex_id
            )
            VALUES %s
            ON CONFLICT DO NOTHING
        """

        execute_values(cur, insert_inst_pivot_sql, inst_pivot_values)

 # ---------------------------------
    # 5️⃣ Insert academic_metadata_authors pivot
    # ---------------------------------
    author_pivot_values = []
    
    for w in metadata_batch:
        identifier = w["canonical_identifier"]
        
        if identifier not in doc_id_map:
            continue
            
        document_id = doc_id_map[identifier]
        
        # Get the academic_metadata id (you might need to retrieve this if you need it)
        # For now, we'll use document_id as the academic_metadata_id if they're 1:1
        for pivot in w.get("pivot_authors", []):
            author_pivot_values.append((
                document_id,  # academic_metadata_id (assuming 1:1 with document)
                pivot.get("author_openalex_id"),
                pivot.get("author_position"),
                pivot.get("raw_affiliation")
            ))
    
    if author_pivot_values:
        # 🔥 FIX: Deduplicate pivot values to avoid ON CONFLICT errors
        unique_pivot_values = {}
        for value in author_pivot_values:
            key = (value[0], value[1])  # (academic_metadata_id, author_openalex_id)
            if key not in unique_pivot_values:
                unique_pivot_values[key] = value
        
        deduplicated_values = list(unique_pivot_values.values())
        
        # print(f"   📊 Pivot authors: {len(author_pivot_values)} total, {len(deduplicated_values)} unique after deduplication")
        
        insert_author_pivot_sql = """
            INSERT INTO academic_metadata_authors 
            (academic_metadata_id, author_openalex_id, author_position, raw_affiliation)
            VALUES %s
            ON CONFLICT (academic_metadata_id, author_openalex_id) 
            DO UPDATE SET
                author_position = EXCLUDED.author_position,
                raw_affiliation = EXCLUDED.raw_affiliation
        """
        
        execute_values(cur, insert_author_pivot_sql, deduplicated_values)
