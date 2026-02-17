# services/web_ingestion/serpapi_client.py
from __future__ import annotations
import re
import time
from dataclasses import dataclass
from typing import List, Optional
import requests

from core.config import settings


@dataclass(frozen=True)
class SerpResult:
    url: str
    title: Optional[str] = None
    snippet: Optional[str] = None
    source: str = "serpapi"


def _is_probably_bad_url(url: str) -> bool:
    # filtra cosas que no quieres meter al pipeline
    bad_patterns = [
        r"^mailto:",
        r"^tel:",
        r"^javascript:",
        r"\.pdf($|\?)",          # PDFs los manejas luego por otra ruta si quieres
        r"accounts\.google\.com",
    ]
    return any(re.search(p, url, re.IGNORECASE) for p in bad_patterns)


def serpapi_search_urls(query: str, *, num_results: int = 10, page_limit: int = 1, sleep_s: float = 0.2) -> List[SerpResult]:
    """
    Descubre URLs usando SerpAPI.
    - page_limit: cuántas páginas de resultados consumir (cada una suele traer ~10 orgánicos)
    """
    results: List[SerpResult] = []
    seen = set()

    for page in range(page_limit):
        params = {
            "api_key": settings.SERPAPI_API_KEY,
            "engine": settings.SERPAPI_ENGINE,
            "q": query,
            "hl": settings.SERPAPI_HL,
            "gl": settings.SERPAPI_GL,
            "num": min(10, max(1, num_results)),
            "start": page * 10,
        }

        r = requests.get("https://serpapi.com/search.json", params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        organic = data.get("organic_results", []) or []
        for item in organic:
            link = item.get("link")
            if not link:
                continue
            if _is_probably_bad_url(link):
                continue
            if link in seen:
                continue
            seen.add(link)
            results.append(
                SerpResult(
                    url=link,
                    title=item.get("title"),
                    snippet=item.get("snippet"),
                )
            )
            if len(results) >= num_results:
                return results

        time.sleep(sleep_s)

    return results
