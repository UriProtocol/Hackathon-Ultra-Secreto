import pyalex
from pyalex import Institutions
from core.config import settings

pyalex.config.api_key = settings.OPENALEX_API_KEY

NUEVO_LEON_CITIES = [
    "Monterrey","Ciudad Apodaca", "García", "Ciudad General Escobedo", "Guadalupe", 
    "Ciudad Benito Juárez", "Santa Catarina", "San Nicolás de los Garza", 
    "San Pedro Garza García", "Santiago", "Cadereyta Jiménez", "Salinas Victoria", 
    "Abasolo", "Ciénega de Flores", "Doctor González", "El Carmen", "General Zuazua", 
    "Hidalgo", "Higueras", "Marín", "Mina", "Pesquería", "Allende", "General Terán", 
    "Hualahuises", "Linares", "Montemorelos", "Rayones", "Agualeguas", "Anáhuac", 
    "Bustamante", "Cerralvo", "China", "Doctor Coss", "General Bravo", 
    "General Treviño", "Lampazos de Naranjo", "Los Aldamas", "Los Herreras", 
    "Los Ramones", "Melchor Ocampo", "Parás", "Sabinas Hidalgo", "Vallecillo", 
    "Villaldama", "Aramberri", "Doctor Arroyo", "Galeana", "General Zaragoza", 
    "Iturbide", "Mier y Noriega"
]

def fetch_nuevo_leon_institutions(limit=None):

    count = 0

    institutions = Institutions().filter(
        country_code="MX"
    ).paginate(per_page=200)

    for page in institutions:
        for inst in page:

            city = inst.get("geo", {}).get("city")

            if city in NUEVO_LEON_CITIES:

                yield {
                    "id": inst.get("id"),
                    "display_name": inst.get("display_name"),
                    "city": city,
                    "type": inst.get("type"),
                    "works_count": inst.get("works_count")
                }

                count += 1
                if limit and count >= limit:
                    return

