from core.database import insert_document

def ingest_web_example():
    # Aquí iría SERP + Crawl4AI
    url = "https://example.gov.mx/convocatoria-ia"
    title = "Convocatoria Nacional de IA 2026"
    raw_text = "Texto limpio extraído del sitio..."

    doc_id = insert_document(
        source_type="web",
        identifier=url,
        title=title,
        raw_text=raw_text
    )

    print("Inserted web:", doc_id)
