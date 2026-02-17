import os
from dotenv import load_dotenv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

load_dotenv()

class Settings:
    SUPABASE_HOST = os.getenv("SUPABASE_HOST")
    SUPABASE_DB = os.getenv("SUPABASE_DB")
    SUPABASE_USER = os.getenv("SUPABASE_USER")
    SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")
    SUPABASE_PORT = os.getenv("SUPABASE_PORT")

    CHROMA_API_KEY = os.getenv("CHROMA_API_KEY")
    CHROMA_TENANT = os.getenv("CHROMA_TENANT")
    CHROMA_DATABASE = os.getenv("CHROMA_DATABASE")

    SERPAPI_API_KEY = os.getenv("SERPAPI_API_KEY")
    SERPAPI_ENGINE = os.getenv("SERPAPI_ENGINE", "google")
    SERPAPI_GL = os.getenv("SERPAPI_GL", "mx")
    SERPAPI_HL = os.getenv("SERPAPI_HL", "es")
    OPENALEX_API_KEY = os.getenv("OPENALEX_API_KEY")

settings = Settings()
