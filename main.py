#!/usr/bin/env python
"""
Script principal para el Hackathon Ultra Secreto
Uso: python main.py [comando] [opciones]

Comandos disponibles:
  ingest-academic [año]    - Ingestar datos académicos para un año específico
  populate-vector-db [n]    - Poblar ChromaDB (n = límite opcional de trabajos)
  populate-institutions     - Poblar tabla de instituciones
  all [año] [n]            - Ejecutar todo el pipeline (ingest + instituciones + vectores)

Ejemplos:
  python main.py ingest-academic 2026
  python main.py populate-vector-db 1000
  python main.py populate-institutions
  python main.py all 2026 500
"""

from services.web_ingestion.ingest import ingest_web_example
from services.academic_ingestion.ingest import bulk_insert_works
from core.database import bulk_insert_institutions
from scripts.populate_chromadb import populate_chromadb
from scripts.populate_institutions import populate_institutions
from services.academic_ingestion.extractor import fetch_works
import sys
import argparse
import time

def print_header(title):
    """Imprime un encabezado formateado"""
    print("\n" + "=" * 60)
    print(f"🚀 {title}")
    print("=" * 60)

def print_success(message):
    """Imprime un mensaje de éxito"""
    print(f"✅ {message}")

def print_error(message):
    """Imprime un mensaje de error"""
    print(f"❌ {message}")

def ingest_academic(year):
    """Ingesta datos académicos para un año específico"""
    print_header(f"Ingestando datos académicos para el año {year}")
    
    start_time = time.perf_counter()
    try:
        bulk_insert_works(year)
        end_time = time.perf_counter()
        print_success(f"Ingesta académica completada en {end_time - start_time:.2f} segundos")
        return True
    except Exception as e:
        print_error(f"Error en ingesta académica: {e}")
        return False

def run_populate_vector_db(limit=None):
    """Pobla ChromaDB con datos de la base de datos"""
    print_header("Poblando ChromaDB")
    
    if limit:
        print(f"📊 Límite: {limit} trabajos")
    
    start_time = time.perf_counter()
    try:
        populate_chromadb(limit_works=limit)
        end_time = time.perf_counter()
        print_success(f"Población de ChromaDB completada en {end_time - start_time:.2f} segundos")
        return True
    except Exception as e:
        print_error(f"Error poblando ChromaDB: {e}")
        return False

def run_populate_institutions():
    """Pobla la tabla de instituciones"""
    print_header("Poblando tabla de instituciones")
    
    start_time = time.perf_counter()
    try:
        populate_institutions()
        end_time = time.perf_counter()
        print_success(f"Población de instituciones completada en {end_time - start_time:.2f} segundos")
        return True
    except Exception as e:
        print_error(f"Error poblando instituciones: {e}")
        return False

def run_all_pipeline(year, vector_limit=None):
    """
    Ejecuta todo el pipeline:
    1. Ingesta académica
    2. Población de instituciones
    3. Población de ChromaDB
    """
    print_header(f"Ejecutando pipeline completo para el año {year}")
    
    steps = [
        ("📚 Ingesta académica", lambda: ingest_academic(year)),
        ("🏛️ Población de instituciones", run_populate_institutions),
        ("🗄️ Población de ChromaDB", lambda: run_populate_vector_db(vector_limit))
    ]
    
    successful_steps = 0
    failed_steps = []
    
    for step_name, step_func in steps:
        print(f"\n{step_name}...")
        if step_func():
            successful_steps += 1
        else:
            failed_steps.append(step_name)
    
    # Resumen final
    print("\n" + "=" * 60)
    print("📊 RESUMEN DEL PIPELINE")
    print("=" * 60)
    print(f"✅ Pasos exitosos: {successful_steps}/{len(steps)}")
    
    if failed_steps:
        print("❌ Pasos fallidos:")
        for step in failed_steps:
            print(f"   - {step}")
        return False
    else:
        print("🎉 Todos los pasos completados exitosamente!")
        return True

def show_help():
    """Muestra la ayuda del script"""
    help_text = """
📚 SISTEMA DE INGESTA ACADÉMICA - HACKATHON ULTRA SECRETO

USO:
  python main.py COMANDO [OPCIONES]

COMANDOS DISPONIBLES:

  ingest-academic [AÑO]      Ingestar datos académicos para un año específico
      Ejemplo: python main.py ingest-academic 2026

  populate-vector-db [N]      Poblar ChromaDB (N = límite opcional de trabajos)
      Ejemplo: python main.py populate-vector-db 1000

  populate-institutions       Poblar la tabla de instituciones
      Ejemplo: python main.py populate-institutions

  all [AÑO] [N]               Ejecutar todo el pipeline completo
      Ejemplo: python main.py all 2026 500

  help                        Mostrar esta ayuda

EJEMPLOS RÁPIDOS:
  # Ingestar datos de 2026
  python main.py ingest-academic 2026

  # Poblar ChromaDB con todos los datos
  python main.py populate-vector-db

  # Poblar ChromaDB con solo 100 trabajos (para pruebas)
  python main.py populate-vector-db 100

  # Pipeline completo para 2025
  python main.py all 2025

NOTAS:
  - El año en ingest-academic y all es obligatorio
  - El límite en populate-vector-db es opcional
  - Los comandos ejecutan transacciones que pueden revertirse en caso de error
"""
    print(help_text)

def main():
    """Función principal que procesa los argumentos de línea de comandos"""
    
    # Si no hay argumentos, mostrar ayuda
    if len(sys.argv) == 1:
        show_help()
        return
    
    # Obtener el comando
    command = sys.argv[1].lower()
    
    # Procesar comandos
    if command == "help" or command == "--help" or command == "-h":
        show_help()
    
    elif command == "ingest-academic":
        if len(sys.argv) < 3:
            print_error("Debes especificar un año")
            print("Ejemplo: python main.py ingest-academic 2026")
            return
        
        try:
            year = int(sys.argv[2])
            ingest_academic(year)
        except ValueError:
            print_error("El año debe ser un número válido")
    
    elif command == "populate-vector-db":
        limit = None
        if len(sys.argv) > 2:
            try:
                limit = int(sys.argv[2])
            except ValueError:
                print_error("El límite debe ser un número válido")
                return
        
        run_populate_vector_db(limit)
    
    elif command == "populate-institutions":
        run_populate_institutions()
    
    elif command == "all":
        if len(sys.argv) < 3:
            print_error("Debes especificar un año para el pipeline")
            print("Ejemplo: python main.py all 2026")
            return
        
        try:
            year = int(sys.argv[2])
            vector_limit = None
            if len(sys.argv) > 3:
                vector_limit = int(sys.argv[3])
            
            run_all_pipeline(year, vector_limit)
        except ValueError:
            print_error("El año y el límite deben ser números válidos")
    
    else:
        print_error(f"Comando desconocido: '{command}'")
        show_help()

if __name__ == "__main__":
    main()