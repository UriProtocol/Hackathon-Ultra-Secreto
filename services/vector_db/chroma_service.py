import chromadb
import os
import json
from typing import List, Dict, Any, Optional
import hashlib
from dotenv import load_dotenv
import requests

load_dotenv()

class ChromaService:
    def __init__(self, collection_prefix: str = ""):
        """
        Inicializa el cliente de ChromaDB en la nube
        """
        # Conectar a Chroma Cloud
        self.client = chromadb.CloudClient(
            api_key=os.getenv("CHROMA_API_KEY"),
            tenant=os.getenv("CHROMA_TENANT"),
            database=os.getenv("CHROMA_DATABASE")
        )
        
        self.collection_prefix = collection_prefix
        
        # Crear o obtener colecciones
        self.works_collection = self._get_or_create_collection(f"{collection_prefix}academic_works")
        self.authors_collection = self._get_or_create_collection(f"{collection_prefix}authors")
        self.institutions_collection = self._get_or_create_collection(f"{collection_prefix}institutions")
        
        print(f"✅ Conectado a Chroma Cloud - Tenant: {os.getenv('CHROMA_TENANT')}, Database: {os.getenv('CHROMA_DATABASE')}")

    def get_ollama_embeddings(self, texts):
        embeddings = []

        MAX_CHARS = 3000

        for text in texts:
            text = text[:MAX_CHARS]

            response = requests.post(
                "http://localhost:11434/api/embeddings",
                json={
                    "model": "nomic-embed-text",
                    "prompt": text
                }
            )

            if response.status_code != 200:
                print("❌ Ollama error:", response.text)
                raise Exception(response.text)

            data = response.json()

            if "embedding" not in data:
                print("❌ Unexpected Ollama response:", data)
                raise Exception("No embedding returned")

            embeddings.append(data["embedding"])

        return embeddings

    def _get_or_create_collection(self, name):
        """Obtiene o crea una colección"""
        try:
            # Intentar obtener la colección existente
            collection = self.client.get_collection(name)
            print(f"   📂 Colección '{name}' encontrada ({collection.count()} documentos)")
            return collection
        except Exception as e:
            # Si no existe, crearla
            print(f"   🆕 Creando colección '{name}'...")
            return self.client.create_collection(
                name=name,
                metadata={"hnsw:space": "cosine"}  # Usar cosine similarity
            )
    
    def _generate_id(self, prefix: str, identifier: str) -> str:
        """Genera un ID único para ChromaDB"""
        return f"{prefix}_{hashlib.md5(identifier.encode()).hexdigest()[:16]}"
    
    def delete_collection(self, collection_name: str):
        """Elimina una colección específica"""
        try:
            self.client.delete_collection(collection_name)
            print(f"🗑️ Colección '{collection_name}' eliminada")
        except Exception as e:
            print(f"⚠️ Error al eliminar colección: {e}")
    
    def reset_database(self):
        """Elimina todas las colecciones (para reiniciar)"""
        collections = [self.works_collection, self.authors_collection, self.institutions_collection]
        for collection in collections:
            try:
                self.client.delete_collection(collection.name)
                print(f"🗑️ Colección '{collection.name}' eliminada")
            except:
                pass
        
        # Recrear colecciones
        self.works_collection = self._get_or_create_collection(f"{self.collection_prefix}academic_works")
        self.authors_collection = self._get_or_create_collection(f"{self.collection_prefix}authors")
        self.institutions_collection = self._get_or_create_collection(f"{self.collection_prefix}institutions")
    
    def index_works(self, conn, limit=None, batch_size=100):
        """
        Indexa trabajos académicos desde PostgreSQL a ChromaDB Cloud
        """
        cur = conn.cursor()

        # Query para obtener trabajos con toda su información
        query = """
        SELECT 
            d.id as document_id,
            d.canonical_identifier,
            d.title,
            d.raw_text,
            am.doi,
            am.journal_name,
            am.publication_year,
            am.citation_count,
            am.is_open_access,
            am.authors as authors_json,
            am.concepts as concepts_json,
            (
                SELECT json_agg(json_build_object(
                    'author_id', a.openalex_id,
                    'author_name', a.display_name,
                    'position', ama.author_position,
                    'affiliation', ama.raw_affiliation
                ))
                FROM academic_metadata_authors ama
                JOIN authors a ON a.openalex_id = ama.author_openalex_id
                WHERE ama.academic_metadata_id = am.document_id
            ) as detailed_authors,
            (
                SELECT json_agg(json_build_object(
                    'institution_id', i.openalex_id,
                    'institution_name', i.display_name,
                    'city', i.city
                ))
                FROM academic_metadata_institutions ami
                JOIN institutions_catalog i ON i.openalex_id = ami.institution_openalex_id
                WHERE ami.document_id = am.document_id
            ) as detailed_institutions
        FROM documents d
        JOIN academic_metadata am ON am.document_id = d.id
        ORDER BY d.id
        """

        if limit:
            query += f" LIMIT {limit}"

        cur.execute(query)
        works = cur.fetchall()

        print(f"\n📚 Indexando {len(works)} trabajos en Chroma Cloud...")

        total_batches = (len(works) + batch_size - 1) // batch_size

        for i in range(0, len(works), batch_size):
            batch = works[i:i+batch_size]
            batch_num = i//batch_size + 1

            ids = []
            documents = []
            metadatas = []

            for work in batch:
                (
                    document_id, canonical_id, title, raw_text, doi, 
                    journal_name, pub_year, citations, is_oa, 
                    authors_json, concepts_json, detailed_authors, detailed_institutions
                ) = work

                  # 🔥 CORREGIDO: Manejar títulos y raw_text que pueden ser None
                title_str = str(title) if title is not None else ""
                raw_text_str = str(raw_text) if raw_text is not None else ""
                
                # Crear el texto para embedding (combinar título + abstract)
                if raw_text_str:
                    text_content = f"{title_str}\n\n{raw_text_str}"
                else:
                    text_content = title_str     

                # Si aún así está vacío, usar un placeholder
                if not text_content or text_content.isspace():
                    text_content = f"Document {canonical_id}"  

                # Limitar tamaño del texto si es muy largo
                if len(text_content) > 10000:
                    text_content = text_content[:10000]
                
                # 🔥 CORREGIDO: Manejar concepts_json que ya viene como lista
                if concepts_json is None:
                    concepts_list = []
                elif isinstance(concepts_json, str):
                    # Si es string, parsear JSON
                    try:
                        concepts_list = json.loads(concepts_json)
                    except:
                        concepts_list = []
                elif isinstance(concepts_json, list):
                    # Si ya es lista, usarla directamente
                    concepts_list = concepts_json
                else:
                    concepts_list = []
                
                concept_names = [c.get("display_name", "") for c in concepts_list if isinstance(c, dict)][:10]
                
                # 🔥 CORREGIDO: Manejar authors_json que ya viene como lista
                if authors_json is None:
                    author_count = 0
                elif isinstance(authors_json, str):
                    try:
                        author_count = len(json.loads(authors_json))
                    except:
                        author_count = 0
                elif isinstance(authors_json, list):
                    author_count = len(authors_json)
                else:
                    author_count = 0
                
                # Metadata estructurada para filtrado
                metadata = {
                    "document_id": str(document_id),
                    "canonical_id": str(canonical_id),
                    "title": str(title)[:500] if title else "",
                    "doi": str(doi)[:200] if doi else "",
                    "journal": str(journal_name)[:200] if journal_name else "",
                    "year": int(pub_year) if pub_year else 0,
                    "citations": int(citations) if citations else 0,
                    "is_open_access": 1 if is_oa else 0,
                    "author_count": author_count,
                    "concepts": ", ".join(concept_names)[:1000],
                    "source": "openalex"
                }
                
                # Limpiar valores None
                cleaned_metadata = {}
                for k, v in metadata.items():
                    if v is None:
                        cleaned_metadata[k] = "" if k not in ["year", "citations", "author_count", "is_open_access"] else 0
                    else:
                        cleaned_metadata[k] = v
                
                ids.append(self._generate_id("work", canonical_id))
                documents.append(text_content)
                metadatas.append(cleaned_metadata)

            # Agregar a ChromaDB Cloud
            try:
                embeddings = self.get_ollama_embeddings(documents)
                self.works_collection.upsert(
                    ids=ids,
                    documents=documents,
                    embeddings=embeddings,
                    metadatas=metadatas
                )

                print(f"   ✅ Batch {batch_num}/{total_batches} completado ({len(batch)} trabajos)")
            except Exception as e:
                print(f"   ❌ Error en batch {batch_num}: {e}")
                # Intentar de a uno para identificar el problema
                for j, (doc_id, doc_text, metadata) in enumerate(zip(ids, documents, metadatas)):
                    try:
                        doc_embeddings = self.get_ollama_embeddings([doc_text])
                        self.works_collection.upsert(
                            ids=[doc_id],
                            documents=[doc_text],
                            embeddings=doc_embeddings,
                            metadatas=[metadata]
                        )
                        print(f"      ✅ Documento {j+1} insertado correctamente")
                    except Exception as e2:
                        print(f"      ⚠️ Error con documento {doc_id}: {e2}")

        final_count = self.works_collection.count()
        print(f"\n✅ Indexación de trabajos completada. Total en colección: {final_count}")
        return final_count
    
    def index_authors(self, conn, limit=None, batch_size=100):
        """
        Indexa autores desde PostgreSQL a ChromaDB Cloud
        """
        cur = conn.cursor()
        
        # Query para autores
        query = """
            SELECT 
                a.openalex_id,
                a.display_name,
                a.orcid,
                ic.display_name as institution_name,
                a.works_count,
                a.cited_by_count,
                (
                    SELECT COUNT(DISTINCT ama.academic_metadata_id)
                    FROM academic_metadata_authors ama
                    WHERE ama.author_openalex_id = a.openalex_id
                ) as actual_works_count
            FROM authors a
            LEFT JOIN institutions_catalog ic ON ic.openalex_id = a.last_known_institution_id
            WHERE a.display_name IS NOT NULL
            ORDER BY a.works_count DESC NULLS LAST
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        cur.execute(query)
        authors = cur.fetchall()
        
        print(f"\n👥 Indexando {len(authors)} autores en Chroma Cloud...")
        
        total_batches = (len(authors) + batch_size - 1) // batch_size
        
        for i in range(0, len(authors), batch_size):
            batch = authors[i:i+batch_size]
            batch_num = i//batch_size + 1
            
            ids = []
            documents = []
            metadatas = []
            
            for author in batch:
                (author_id, name, orcid, institution, works_count, cited_count, actual_works) = author
                
                # Texto para embedding
                text_content = str(name) if name else ""
                if institution:
                    text_content += f" - {institution}"
                
                # Asegurar tipos correctos
                metadata = {
                    "author_id": str(author_id) if author_id else "",
                    "name": str(name)[:500] if name else "",
                    "orcid": str(orcid)[:100] if orcid else "",
                    "institution": str(institution)[:200] if institution else "",
                    "works_count": int(works_count) if works_count is not None else 0,
                    "cited_by_count": int(cited_count) if cited_count is not None else 0,
                    "source": "openalex"
                }
                
                # Limpiar valores None
                cleaned_metadata = {}
                for k, v in metadata.items():
                    if v is None:
                        cleaned_metadata[k] = "" if k not in ["works_count", "cited_by_count"] else 0
                    else:
                        cleaned_metadata[k] = v
                
                ids.append(self._generate_id("author", str(author_id) if author_id else f"unknown_{i}"))
                documents.append(text_content)
                metadatas.append(cleaned_metadata)
            
            try:
                embeddings = self.get_ollama_embeddings(documents)
                self.authors_collection.upsert(
                    ids=ids,
                    documents=documents,
                    embeddings=embeddings,
                    metadatas=metadatas
                )
                print(f"   ✅ Batch {batch_num}/{total_batches} completado ({len(batch)} autores)")
            except Exception as e:
                print(f"   ❌ Error en batch {batch_num}: {e}")
        
        final_count = self.authors_collection.count()
        print(f"\n✅ Indexación de autores completada. Total en colección: {final_count}")
        return final_count
    
    def index_institutions(self, conn, limit=None, batch_size=100):
        """
        Indexa instituciones desde PostgreSQL a ChromaDB Cloud
        Adaptado para tu estructura actual de institutions_catalog
        """
        cur = conn.cursor()
        
        # Query adaptada a tu estructura exacta
        query = """
            SELECT 
                i.openalex_id,
                i.display_name,
                i.city,
                i.type,
                i.works_count,
                (
                    SELECT COUNT(DISTINCT ami.document_id)
                    FROM academic_metadata_institutions ami
                    WHERE ami.institution_openalex_id = i.openalex_id
                ) as documents_count,
                (
                    SELECT COUNT(DISTINCT a.openalex_id)
                    FROM authors a
                    WHERE a.last_known_institution_id = i.openalex_id
                ) as associated_authors
            FROM institutions_catalog i
            WHERE i.display_name IS NOT NULL
            ORDER BY i.works_count DESC NULLS LAST
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        cur.execute(query)
        institutions = cur.fetchall()
        
        print(f"\n🏛️ Indexando {len(institutions)} instituciones en Chroma Cloud...")
        
        total_batches = (len(institutions) + batch_size - 1) // batch_size
        
        for i in range(0, len(institutions), batch_size):
            batch = institutions[i:i+batch_size]
            batch_num = i//batch_size + 1
            
            ids = []
            documents = []
            metadatas = []
            
            for inst in batch:
                inst_id = inst[0]
                name = inst[1]
                city = inst[2]
                inst_type = inst[3]
                works_count = inst[4]
                documents_count = inst[5] if len(inst) > 5 else 0
                author_count = inst[6] if len(inst) > 6 else 0
                
                # Texto para embedding (nombre + ciudad para mejor búsqueda semántica)
                text_content = str(name) if name else ""
                if city:
                    text_content += f", {city}"
                if inst_type:
                    text_content += f" - {inst_type}"
                
                metadata = {
                    "institution_id": str(inst_id) if inst_id else "",
                    "name": str(name)[:500] if name else "",
                    "city": str(city)[:100] if city else "",
                    "type": str(inst_type)[:50] if inst_type else "",
                    "works_count": int(works_count) if works_count else 0,
                    "documents_count": int(documents_count) if documents_count else 0,
                    "associated_authors": int(author_count) if author_count else 0,
                    "source": "openalex"
                }
                
                # Limpiar valores None
                cleaned_metadata = {}
                for k, v in metadata.items():
                    if v is None:
                        cleaned_metadata[k] = "" if k not in ["works_count", "documents_count", "associated_authors"] else 0
                    else:
                        cleaned_metadata[k] = v
                
                ids.append(self._generate_id("inst", str(inst_id) if inst_id else f"unknown_{i}"))
                documents.append(text_content)
                metadatas.append(cleaned_metadata)
            
            try:
                embeddings = self.get_ollama_embeddings(documents)
                self.institutions_collection.upsert(
                    ids=ids,
                    documents=documents,
                    metadatas=metadatas,
                    embeddings=embeddings
                )
                print(f"   ✅ Batch {batch_num}/{total_batches} completado ({len(batch)} instituciones)")
            except Exception as e:
                print(f"   ❌ Error en batch {batch_num}: {e}")
        
        final_count = self.institutions_collection.count()
        print(f"\n✅ Indexación de instituciones completada. Total en colección: {final_count}")
        return final_count
    
    # Métodos de búsqueda
    def search_similar_works(self, query_text, n_results=10, filter_dict=None):
        """Busca trabajos similares por texto"""
        results = self.works_collection.query(
            query_texts=[query_text],
            n_results=n_results,
            where=filter_dict
        )
        return results
    
    def get_author_recommendations(self, author_name, n_results=5):
        """Recomienda autores similares"""
        results = self.authors_collection.query(
            query_texts=[author_name],
            n_results=n_results
        )
        return results
    
    def get_institution_recommendations(self, institution_name, n_results=5):
        """Recomienda instituciones similares"""
        results = self.institutions_collection.query(
            query_texts=[institution_name],
            n_results=n_results
        )
        return results
    
    def search_institutions_by_city(self, city_name, n_results=20):
        """Busca instituciones por ciudad"""
        results = self.institutions_collection.query(
            query_texts=[f"institutions in {city_name}"],
            n_results=n_results
        )
        return results
    
    def hybrid_search(self, query_text, collection="works", n_results=10, **filters):
        """Búsqueda híbrida con filtros"""
        collection_map = {
            "works": self.works_collection,
            "authors": self.authors_collection,
            "institutions": self.institutions_collection
        }
        
        col = collection_map.get(collection, self.works_collection)
        
        results = col.query(
            query_texts=[query_text],
            n_results=n_results,
            where=filters if filters else None
        )
        
        return results
    
    def get_collection_stats(self):
        """Obtiene estadísticas de las colecciones"""
        stats = {
            "works": self.works_collection.count(),
            "authors": self.authors_collection.count(),
            "institutions": self.institutions_collection.count()
        }
        return stats