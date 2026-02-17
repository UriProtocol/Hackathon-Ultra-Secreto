
import json
import re
import time
from urllib.parse import urlparse

from core.database import fetch_pending_web_metadata, upsert_web_metadata


# -------------------------
# Regex
# -------------------------
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)

# Simple MX-ish phone patterns (prototipo; mejorable)
PHONE_RE = re.compile(r"(?:\+?52\s*)?(?:\(?\d{2,3}\)?[\s-]*)?\d{3}[\s-]*\d{4}")

MD_LINK_RE = re.compile(r"\[([^\]]+)\]\((https?://[^)]+)\)")
URL_RE = re.compile(r"https?://[^\s\)]+", re.I)

# Para extraer “personas” tipo:
# ## [Nombre](url)
NAME_BLOCK_RE = re.compile(r"^##\s+\[(.+?)\]\((https?://[^)]+)\)", re.M)

NOISE_LINE_PATTERNS = [
    re.compile(r"^\[?\s*skip to content\s*\]?$", re.I),
    re.compile(r"^menu$", re.I),
    re.compile(r"^enlaces rápidos$", re.I),
    re.compile(r"^contacto$", re.I),
    re.compile(r"^síguenos$", re.I),
    re.compile(r"^©\s*\d{4}", re.I),
    re.compile(r"all rights reserved", re.I),
]


# -------------------------
# Helpers
# -------------------------
def is_noise_link(u: str) -> bool:
    u2 = (u or "").lower().strip()
    if not u2:
        return True

    # archivos / assets típicos
    if any(u2.endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".svg", ".webp", ".gif", ".pdf"]):
        return True

    # redes / translate / etc
    if any(x in u2 for x in ["facebook.com", "instagram.com", "twitter.com", "x.com", "youtube.com", "translate.goog"]):
        return True

    # login / auth
    if any(x in u2 for x in ["login", "signin", "sign-in", "auth", "account"]):
        return True

    return False


def dedupe_long_lines(lines: list[str], min_len: int = 40) -> list[str]:
    """
    Quita repetidos no consecutivos (muy común: menú/headers duplicados).
    Solo aplica para líneas "largas" para no eliminar headings o bullets cortos.
    """
    seen = set()
    out = []
    for l in lines:
        key = l
        if len(l) >= min_len:
            if key in seen:
                continue
            seen.add(key)
        out.append(l)
    return out


# -------------------------
# Cleaning
# -------------------------
def clean_markdown(md: str) -> str:
    """Limpieza determinista (sin IA). Mantiene headings y párrafos útiles."""
    md = (md or "").strip()
    if not md:
        return ""

    lines = [l.strip() for l in md.splitlines()]
    cleaned = []
    prev = ""

    for l in lines:
        if not l:
            continue

        # elimina líneas de ruido
        if any(p.search(l) for p in NOISE_LINE_PATTERNS):
            continue

        # elimina “solo imagen” tipo ![...](...)
        if l.startswith("![](") or l.startswith("!["):
            continue

        # evita duplicados consecutivos
        if l == prev:
            continue
        prev = l

        if l.count("](") >= 3:   # 3+ links en una sola línea suele ser navegación
            continue

        # si es bullet de navegación con link y texto corto
        if l.startswith("* [") and len(l) < 120:
            continue

            # Si hay demasiadas líneas de bullets, probablemente es navegación
    bullet_lines = [x for x in cleaned if x.startswith("* ")]
    if len(bullet_lines) > 40:
        # deja bullets solo si son pocos; si son muchos, quítalos todos
        cleaned = [x for x in cleaned if not x.startswith("* ")]

        cleaned.append(l)

    # quita duplicados comunes (menú repetido, etc.)
    cleaned = dedupe_long_lines(cleaned, min_len=40)

    text = "\n".join(cleaned)

    # recorte por tamaño para DB/procesamiento
    if len(text) > 50000:
        text = text[:50000]

    return text


# -------------------------
# Signals
# -------------------------
def extract_signals(text: str) -> dict:
    emails = sorted(set(EMAIL_RE.findall(text)))
    phones = sorted(set([p.strip() for p in PHONE_RE.findall(text)]))

    md_links = MD_LINK_RE.findall(text)
    links = sorted(set([url for _, url in md_links] + URL_RE.findall(text)))
    links = [u for u in links if not is_noise_link(u)]

    # headings (Markdown)
    headings = [l.lstrip("#").strip() for l in text.splitlines() if l.startswith("#")]

    excerpt = ""
    for l in text.splitlines():
        if len(l) > 80 and not l.startswith("#"):
            excerpt = l[:300]
            break

    return {
        "emails": emails[:20],
        "phones": phones[:20],
        "links": links[:200],
        "headings": headings[:50],
        "excerpt": excerpt,
        "word_count": len(text.split()),
        "char_count": len(text),
    }


# -------------------------
# Entity type
# -------------------------
def guess_entity_type(headings: list[str], url: str) -> str:
    """Heurística simple (mejorable)."""
    u = (url or "").lower()
    h = " ".join(headings).lower()

    # por URL primero (suele ser más confiable)
    if "directorio" in u or "directory" in u:
        return "directory"
    if "researcher" in u or "investigador" in u:
        return "research_profile"
    if "publication" in u or "publicacion" in u:
        return "publications_page"

    # por headings
    if "investigadores" in h or "researchers" in h:
        return "research_group"
    if "publicaciones" in h or "publications" in h:
        return "publications_page"

    # dominio (institución)
    host = urlparse(url).netloc.lower()
    if any(x in host for x in ["unam", "uadec", "tec", ".edu", ".gob", "ipn", "cinvestav"]):
        return "institution_page"

    return "web_page"


# -------------------------
# Structured extraction (no IA)
# -------------------------
def extract_people_from_markdown(cleaned_md: str) -> list[dict]:
    """
    Extrae personas de un patrón común:
      ## [Nombre](url)
    """
    people = []
    for name, profile_url in NAME_BLOCK_RE.findall(cleaned_md or ""):
        n = (name or "").strip()
        u = (profile_url or "").strip()
        if not n or not u:
            continue
        people.append({"name": n, "profile_url": u})
    # dedupe por URL
    seen = set()
    uniq = []
    for p in people:
        if p["profile_url"] in seen:
            continue
        seen.add(p["profile_url"])
        uniq.append(p)
    return uniq[:50]


# -------------------------
# Runner
# -------------------------
def run(limit: int = 5, sleep_s: float = 0.2):
    docs = fetch_pending_web_metadata(limit=limit)
    if not docs:
        print("[normalize_det] No hay pendientes.")
        return

    for d in docs:
        doc_id = d["id"]
        url = d["url"]
        title = d["title"]
        raw_text = d["raw_text"]

        try:
            cleaned = clean_markdown(raw_text)
            signals = extract_signals(cleaned)

            people = extract_people_from_markdown(cleaned)

            data = {
                "version": "det_v1",
                "url": url,
                "title": title,
                "entity_type": guess_entity_type(signals["headings"], url),
                "signals": signals,
                "structured": {
                    "people": people,  # útil para directorios tipo Cinvestav/CIMA
                },
                "cleaned_text": cleaned,  # opcional: puedes quitarlo si pesa mucho
            }

            upsert_web_metadata(
                document_id=doc_id,
                url=url,
                content_type="text/markdown",
                data=data,
            )
            print(f"[normalize_det] ✅ upsert web_metadata doc_id={doc_id} url={url}")

        except Exception as e:
            print(f"[normalize_det] ❌ error doc_id={doc_id} url={url} err={e}")

        time.sleep(sleep_s)


if __name__ == "__main__":
    run(limit=5)