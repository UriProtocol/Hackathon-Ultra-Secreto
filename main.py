from services.web_ingestion.ingest import ingest_web_example
from services.academic_ingestion.ingest import bulk_insert_works
from core.database import bulk_insert_institutions
from services.academic_ingestion.institutions import fetch_nuevo_leon_institutions
import json

from services.academic_ingestion.extractor import fetch_works
if __name__ == "__main__":
    #bulk_insert_institutions(fetch_nuevo_leon_institutions())
    bulk_insert_works(2025)
    #fetch_works(2025)
    #for work in fetch_works(2026):
    ## Convertir a JSON con formato legible
    #    print(json.dumps(work, indent=2, ensure_ascii=False))
    #    print("-" * 80)
    
