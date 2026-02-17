import pyalex
from pyalex import Works
from core.config import settings
from core.database import get_nuevo_leon_institution_ids

pyalex.config.api_key = settings.OPENALEX_API_KEY

institution_ids = get_nuevo_leon_institution_ids()

def fetch_works(year):
    works = Works().filter_or(
        institutions={"id": institution_ids},
        publication_year=year
    ).paginate(per_page=200)

    for page in works:
        for work in page:
            yield work
