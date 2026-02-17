import chromadb
import os
from dotenv import load_dotenv

load_dotenv()

try:
    client = chromadb.CloudClient(
        api_key=os.getenv("CHROMA_API_KEY"),
        tenant=os.getenv("CHROMA_TENANT"),
        database=os.getenv("CHROMA_DATABASE")
    )

    print("✅ Conectado a Chroma Cloud")

    # Crear o recuperar colección
    collection = client.get_or_create_collection(
        name="test_collection"
    )

    print("✅ Colección lista")

    # Insertar documento de prueba
    collection.add(
        documents=["Esto es una prueba de embeddings"],
        metadatas=[{"source": "test"}],
        ids=["doc1"]
    )

    print("✅ Documento insertado")

    # Hacer query
    results = collection.query(
        query_texts=["prueba embeddings"],
        n_results=1
    )

    print("✅ Query ejecutado")
    print("Resultados:", results)

except Exception as e:
    print("❌ Error:", e)
