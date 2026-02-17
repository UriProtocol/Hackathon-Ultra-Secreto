import asyncio
from core.database import fetch_pending_web_documents, update_document_raw_text_by_id

from crawl4ai import AsyncWebCrawler


async def process_batch(batch_size: int = 5, delay_s: float = 0.5):
    pending = fetch_pending_web_documents(limit=batch_size)
    if not pending:
        print("[crawl_worker] No hay documentos pendientes (raw_text IS NULL).")
        return

    print(f"[crawl_worker] Procesando batch de {len(pending)} URLs...")

    async with AsyncWebCrawler(verbose=True) as crawler:
        for item in pending:
            doc_id = item["id"]
            url = item["url"]

            try:
                result = await crawler.arun(url=url)

                # Crawl4AI 0.8.0 normalmente expone markdown
                markdown = getattr(result, "markdown", None)

                # fallback por si cambia el atributo
                if not markdown:
                    markdown = getattr(result, "markdown_v2", None)

                if not markdown:
                    print(f"[crawl_worker] ❌ Sin markdown doc_id={doc_id} url={url}")
                    continue

                update_document_raw_text_by_id(doc_id, markdown)
                print(f"[crawl_worker] ✅ Guardado doc_id={doc_id} chars={len(markdown)} url={url}")

            except Exception as e:
                print(f"[crawl_worker] ❌ Error doc_id={doc_id} url={url} err={e}")

            await asyncio.sleep(delay_s)


if __name__ == "__main__":
    asyncio.run(process_batch(batch_size=5, delay_s=0.5))