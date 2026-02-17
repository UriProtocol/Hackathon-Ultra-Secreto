import os
import sys
from pathlib import Path

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent.parent))

from core.database import get_connection
from services.vector_db.chroma_service import ChromaService
import time
from dotenv import load_dotenv

load_dotenv()

def populate_chromadb(limit_works=None, limit_authors=None, limit_institutions=None, reset=False):
    """
    Pobla ChromaDB Cloud con datos de PostgreSQL
    """
    print("=" * 60)
    print("🚀 Iniciando población de ChromaDB Cloud")
    print("=" * 60)
    print(f"📊 Configuración:")
    print(f"   - Tenant: {os.getenv('CHROMA_TENANT')}")
    print(f"   - Database: {os.getenv('CHROMA_DATABASE')}")
    print(f"   - Reset: {reset}")
    print("=" * 60)
    
    start_time = time.perf_counter()
    
    # Conectar a PostgreSQL
    conn = get_connection()
    
    # Inicializar servicio ChromaDB Cloud
    chroma = ChromaService(collection_prefix="academic_")
    
    try:
        # Opcional: resetear bases de datos
        if reset:
            print("\n🔄 Reseteando colecciones...")
            chroma.reset_database()
        
        # Indexar trabajos
        works_count = chroma.index_works(conn, limit=limit_works)
        
        # # Indexar autores
        # authors_count = chroma.index_authors(conn, limit=limit_authors)
        
        # # Indexar instituciones
        # institutions_count = chroma.index_institutions(conn, limit=limit_institutions)
        
        # Estadísticas finales
        end_time = time.perf_counter()
        total_time = end_time - start_time
        
        print("\n" + "=" * 60)
        print("✅ Población de ChromaDB Cloud completada exitosamente")
        print(f"📊 Estadísticas finales:")
        print(f"   - academic_works: {works_count} documentos")
        # print(f"   - authors: {authors_count} autores")
        # print(f"   - institutions: {institutions_count} instituciones")
        print(f"⏱️ Duración total: {total_time:.2f} segundos")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Error durante la población: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Poblar ChromaDB Cloud')
    parser.add_argument('--limit-works', type=int, help='Límite de trabajos a indexar')
    parser.add_argument('--limit-authors', type=int, help='Límite de autores a indexar')
    parser.add_argument('--limit-institutions', type=int, help='Límite de instituciones a indexar')
    parser.add_argument('--reset', action='store_true', help='Resetear colecciones antes de indexar')
    
    args = parser.parse_args()
    
    populate_chromadb(
        limit_works=args.limit_works,
        limit_authors=args.limit_authors,
        limit_institutions=args.limit_institutions,
        reset=args.reset
    )