import chromadb
from core.config import settings

client = chromadb.CloudClient(
    api_key=settings.CHROMA_API_KEY,
    tenant=settings.CHROMA_TENANT,
    database=settings.CHROMA_DATABASE
)

collection = client.get_or_create_collection(
    name="documents"
)

def store_embedding(document_id, embedding, document_text, metadata=None):
    collection.upsert(
        ids=[str(document_id)],
        embeddings=[embedding],
        documents=[document_text],   # 👈 MUY IMPORTANTE
        metadatas=[metadata or {}]
    )
