import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

try:
    conn = psycopg2.connect(
        host=os.getenv("SUPABASE_HOST"),
        database=os.getenv("SUPABASE_DB"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD"),
        port=os.getenv("SUPABASE_PORT"),
        sslmode="require"  # MUY IMPORTANTE
    )

    cur = conn.cursor()

    # Prueba simple
    cur.execute("SELECT version();")
    db_version = cur.fetchone()

    print("✅ Conexión exitosa!")
    print("PostgreSQL version:")
    print(db_version)

    cur.close()
    conn.close()

except Exception as e:
    print("❌ Error de conexión:")
    print(e)
