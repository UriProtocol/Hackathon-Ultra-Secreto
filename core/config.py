import os
from dotenv import load_dotenv

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

settings = Settings()
