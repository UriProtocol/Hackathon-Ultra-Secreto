import pyalex
from pyalex import Works
from core.config import settings
from core.database import get_nuevo_leon_institution_ids

pyalex.config.api_key = settings.OPENALEX_API_KEY

institution_ids = get_nuevo_leon_institution_ids()

def fetch_works(year):
    works = Works().filter_or(
        institutions={"id": institution_ids},
    ).filter(publication_year=year).paginate(per_page=200)

    for page in works:
        for work in page:
            if is_cti(work):
                yield work

def is_cti(work):

    conceptos_cyti = [
        # Ciencia
        'physics', 'chemistry', 'biology', 'mathematics', 'biochemistry', 
        'molecular biology', 'genetics', 'neuroscience', 'medicine',
        
        # Tecnología
        'computer science', 'artificial intelligence', 'machine learning',
        'data science', 'deep learning', 'robotics', 'computer vision',
        'blockchain', 'cloud computing', 'big data', 'software engineering',
        'materials science', 'nanotechnology', 'biotechnology',
        'engineering', 'bioengineering', 'software', 'development', 'cybersecurity', 
        'web', 'web development', 'app', 'iot', 'website', 'web site', 'application'
        
        # Innovación
        'innovation', 'research and development', 'technology transfer',
        'intellectual property', 'entrepreneurship', 'startup',
        'business model', 'technology management', 'innovation management',
        'knowledge management', 'disruptive technology',
        
        # Términos relacionados
        'computational', 'digital', 'automated', 'advanced', 'novel',
        'development', 'application', 'system'
    ]

    concepts = []
    if isinstance(work.get("concepts"), list):
        concepts = [
            c.get("display_name").lower()
            for c in work["concepts"]
            if isinstance(c, dict) and c.get("display_name")
        ]
    
    if any(keyword in concepts for keyword in conceptos_cyti):
        return True

    return False
