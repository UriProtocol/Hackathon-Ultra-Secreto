import requests
from core.database import insert_document

OPENALEX_URL = "https://api.openalex.org/works"

def ingest_openalex(query="mexico"):
    response = requests.get(OPENALEX_URL, params={
        "filter": "authorships.institutions.country_code:MX",
        "per-page": 5
    })

    data = response.json()

    for work in data.get("results", []):
        doi = work.get("doi")
        title = work.get("title")

        if doi:
            doc_id = insert_document(
                source_type="academic",
                identifier=doi,
                title=title
            )

            print("Inserted academic:", doc_id)
