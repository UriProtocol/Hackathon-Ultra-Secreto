from services.academic_ingestion.institutions import fetch_nuevo_leon_institutions
from core.database import bulk_insert_institutions

def populate_institutions():
    print("=" * 60)
    print("🚀 Iniciando población de instituciones")
    print("=" * 60)
    bulk_insert_institutions(fetch_nuevo_leon_institutions())