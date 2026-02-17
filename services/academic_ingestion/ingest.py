from core.database import _flush_batch
from services.academic_ingestion.extractor import fetch_works
from services.academic_ingestion.transformer import normalize_work
from core.database import get_connection

BATCH_SIZE = 500

def bulk_insert_works(year):

    conn = get_connection()
    cur = conn.cursor()

    documents_batch = []
    metadata_batch = []

    try:
        for work in fetch_works(year):
            w = normalize_work(work)

            # -------------------
            # documents
            # -------------------
            documents_batch.append((
                w["source_type"],
                w["canonical_identifier"],
                w["title"],
                w["raw_text"]
            ))

            # metadata (se insertará después)
            metadata_batch.append(w)

            if len(documents_batch) >= BATCH_SIZE:
                _flush_batch(cur, documents_batch, metadata_batch)
                documents_batch = []
                metadata_batch = []

        if documents_batch:
            _flush_batch(cur, documents_batch, metadata_batch)

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()