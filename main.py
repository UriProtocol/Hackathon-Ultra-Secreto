from services.academic_ingestion.ingest import ingest_openalex
from services.web_ingestion.ingest import ingest_web_example

if __name__ == "__main__":
    ingest_openalex()
    ingest_web_example()
