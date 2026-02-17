from core.database import _flush_batch
from services.academic_ingestion.extractor import fetch_works
from services.academic_ingestion.transformer import normalize_work
from core.database import get_connection
import time
from psycopg2.extras import execute_values

BATCH_SIZE = 500

def get_existing_entities(cur, inst_ids=None, author_ids=None):
    """Return sets of existing institution and author IDs"""
    result = {
        "institutions": set(),
        "authors": set()
    }
    
    # Check institutions
    if inst_ids:
        cur.execute("CREATE TEMP TABLE temp_inst_ids (id TEXT) ON COMMIT DROP")
        execute_values(
            cur,
            "INSERT INTO temp_inst_ids (id) VALUES %s",
            [(inst_id,) for inst_id in inst_ids]
        )
        
        cur.execute("""
            SELECT t.id 
            FROM temp_inst_ids t
            INNER JOIN institutions_catalog ic ON ic.openalex_id = t.id
        """)
        result["institutions"] = {row[0] for row in cur.fetchall()}
    
    # Check authors
    if author_ids:
        cur.execute("CREATE TEMP TABLE temp_author_ids (id TEXT) ON COMMIT DROP")
        execute_values(
            cur,
            "INSERT INTO temp_author_ids (id) VALUES %s",
            [(author_id,) for author_id in author_ids]
        )
        
        cur.execute("""
            SELECT t.id 
            FROM temp_author_ids t
            INNER JOIN authors a ON a.openalex_id = t.id
        """)
        result["authors"] = {row[0] for row in cur.fetchall()}
    
    return result

def bulk_insert_works(year):
    print("=" * 60)
    print(f"🚀 Iniciando inserción en bulk por el año {year}")
    print("=" * 60)

    start_time = time.perf_counter()
    total_processed = 0
    batch_number = 0

    conn = get_connection()
    cur = conn.cursor()

    try:
        # Process in batches from the generator
        current_batch = []
        
        for work in fetch_works(year):
            current_batch.append(work)
            
            if len(current_batch) >= BATCH_SIZE:
                batch_number += 1
                print(f"📦 Procesando batch #{batch_number} ({len(current_batch)} documentos)...")
                
                # Step 1: Extract all institution IDs and author IDs from this batch
                all_inst_ids = set()
                all_author_ids = set()
                
                for w in current_batch:
                    # Extract institution IDs
                    for authorship in w.get("authorships", []):
                        for inst in authorship.get("institutions", []):
                            inst_id = inst.get("id")
                            if inst_id:
                                all_inst_ids.add(inst_id)
                        
                        # Extract author IDs
                        author = authorship.get("author", {})
                        author_id = author.get("id")
                        if author_id:
                            all_author_ids.add(author_id)
                
                # Step 2: Check which institutions AND authors exist in catalog (single function)
                print(f"   🔍 Verificando {len(all_inst_ids)} instituciones y {len(all_author_ids)} autores en catálogo...")
                
                existing = get_existing_entities(
                    cur, 
                    inst_ids=all_inst_ids if all_inst_ids else None,
                    author_ids=all_author_ids if all_author_ids else None
                )
                
                existing_inst_ids = existing["institutions"]
                existing_author_ids = existing["authors"]
                
                print(f"   ✅ {len(existing_inst_ids)} instituciones encontradas")
                print(f"   ✅ {len(existing_author_ids)} autores encontrados")
                
                # Step 3: Process the batch with filtered institutions and author checking
                documents_batch = []
                metadata_batch = []
                
                for w in current_batch:
                    # Pass both existing institution and author IDs to normalize_work
                    normalized = normalize_work(
                        w, 
                        existing_inst_ids,
                        existing_author_ids  # Now passing author IDs too
                    )
                    
                    documents_batch.append((
                        normalized["source_type"],
                        normalized["canonical_identifier"],
                        normalized["title"],
                        normalized["raw_text"]
                    ))
                    
                    metadata_batch.append(normalized)
                    total_processed += 1
                
                # Step 4: Flush to database (now handles authors and pivots)
                _flush_batch(cur, documents_batch, metadata_batch)
                conn.commit()
                print(f"   💾 Batch #{batch_number} insertado (incluyendo {len(all_author_ids)} autores)")
                
                # Clear the batch
                current_batch = []
        
        # Process remaining works
        if current_batch:
            batch_number += 1
            print(f"📦 Procesando último batch #{batch_number} ({len(current_batch)} documentos)...")
            
            # Same process for the final batch
            all_inst_ids = set()
            all_author_ids = set()
            
            for w in current_batch:
                for authorship in w.get("authorships", []):
                    for inst in authorship.get("institutions", []):
                        inst_id = inst.get("id")
                        if inst_id:
                            all_inst_ids.add(inst_id)
                    
                    author = authorship.get("author", {})
                    author_id = author.get("id")
                    if author_id:
                        all_author_ids.add(author_id)
            
            existing = get_existing_entities(
                cur,
                inst_ids=all_inst_ids if all_inst_ids else None,
                author_ids=all_author_ids if all_author_ids else None
            )
            
            existing_inst_ids = existing["institutions"]
            existing_author_ids = existing["authors"]
            
            documents_batch = []
            metadata_batch = []
            
            for w in current_batch:
                normalized = normalize_work(w, existing_inst_ids, existing_author_ids)
                
                documents_batch.append((
                    normalized["source_type"],
                    normalized["canonical_identifier"],
                    normalized["title"],
                    normalized["raw_text"]
                ))
                
                metadata_batch.append(normalized)
                total_processed += 1
            
            _flush_batch(cur, documents_batch, metadata_batch)
            conn.commit()
            print(f"   💾 Batch #{batch_number} insertado (incluyendo {len(all_author_ids)} autores)")

        end_time = time.perf_counter()
        total_time = end_time - start_time

        print("=" * 60)
        print("✅ Ingestión académica completada exitosamente.")
        print(f"📄 Total de documentos procesados: {total_processed}")
        print(f"📦 Total de batches: {batch_number}")
        print(f"⏱️ Duración: {total_time:.2f} segundos")
        print("=" * 60)

    except Exception as e:
        conn.rollback()
        print("❌ Error durante la ingestión. Reversando transacción.")
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise e
    finally:
        cur.close()
        conn.close()