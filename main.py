from services.academic_ingestion.ingest import ingest_openalex
from services.web_ingestion.ingest import ingest_web_seeds_from_serpapi

if __name__ == "__main__":
    #ingest_openalex()
    ingest_web_seeds_from_serpapi("investigadores cinestav", num_results=10, page_limit=1)