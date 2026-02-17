from core.database import insert_document
from services.web_ingestion.serpapi_client import serpapi_search_urls

def ingest_web_seeds_from_serpapi(query: str, num_results: int = 10, page_limit: int = 1):
    results = serpapi_search_urls(query, num_results=num_results, page_limit=page_limit)

    inserted_ids = []
    for r in results:
        doc_id = insert_document(
            source_type="serpapi",
            identifier=r.url,
            title=r.title or "",
            raw_text=None
        )
        inserted_ids.append(doc_id)

    print(f"[serpapi] query='{query}' discovered={len(results)} upserted={len(inserted_ids)}")
    return inserted_ids
