import re
import time
from urllib.parse import urlparse

from core.database import fetch_web_metadata_needing_refresh, upsert_web_metadata


# -------------------------
# Regex
# -------------------------
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
PHONE_RE = re.compile(r"(?:\+?52\s*)?(?:\(?\d{2,3}\)?[\s-]*)?\d{3}[\s-]*\d{4}")

MD_LINK_RE = re.compile(r"\[([^\]]+)\]\((https?://[^)]+)\)")
URL_RE = re.compile(r"https?://[^\s\)]+", re.I)

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

    if any(u2.endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".svg", ".webp", ".gif", ".pdf"]):
        return True

    if any(x in u2 for x in ["facebook.com", "instagram.com", "twitter.com", "x.com", "youtube.com", "translate.goog"]):
        return True

    if any(x in u2 for x in ["login", "signin", "sign-in", "auth", "account"]):
        return True

    return False


def dedupe_long_lines(lines: list[str], min_len: int = 40) -> list[str]:
    seen = set()
    out = []
    for l in lines:
        if len(l) >= min_len:
            if l in seen:
                continue
            seen.add(l)
        out.append(l)
    return out


# -------------------------
# Cleaning
# -------------------------
def clean_markdown(md: str) -> str:
    md = (md or "").strip()
    if not md:
        return ""

    lines = [l.strip() for l in md.splitlines()]
    cleaned: list[str] = []
    prev = ""

    for l in lines:
        if not l:
            continue

        if any(p.search(l) for p in NOISE_LINE_PATTERNS):
            continue

        if l.startswith("![](") or l.startswith("!["):
            continue

        if l == prev:
            continue
        prev = l

        if l.count("](") >= 3:
            continue

        if l.startswith("* [") and len(l) < 120:
            continue

        cleaned.append(l)

    bullet_lines = [x for x in cleaned if x.startswith("* ")]
    if len(bullet_lines) > 40:
        cleaned = [x for x in cleaned if not x.startswith("* ")]

    cleaned = dedupe_long_lines(cleaned, min_len=40)

    text = "\n".join(cleaned)
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
    u = (url or "").lower()
    h = " ".join(headings).lower()

    if "directorio" in u or "directory" in u:
        return "directory"
    if "researcher" in u or "investigador" in u:
        return "research_profile"
    if "publication" in u or "publicacion" in u:
        return "publications_page"

    if "investigadores" in h or "researchers" in h:
        return "research_group"
    if "publicaciones" in h or "publications" in h:
        return "publications_page"

    host = urlparse(url).netloc.lower()
    if any(x in host for x in ["unam", "uadec", "tec", ".edu", ".gob", "ipn", "cinvestav"]):
        return "institution_page"

    return "web_page"


# -------------------------
# Structured extraction (no IA)
# -------------------------
def extract_people_from_markdown(cleaned_md: str) -> list[dict]:
    people = []
    for name, profile_url in NAME_BLOCK_RE.findall(cleaned_md or ""):
        n = (name or "").strip()
        u = (profile_url or "").strip()
        if not n or not u:
            continue
        people.append({"name": n, "profile_url": u})

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
def run(limit: int = 50, sleep_s: float = 0.1, max_rounds: int = 50):
    for round_i in range(1, max_rounds + 1):
        docs = fetch_web_metadata_needing_refresh(limit=limit)
        if not docs:
            print("[normalize_det] No hay pendientes.")
            return

        print(f"[normalize_det] Round {round_i}: procesando {len(docs)} docs...")

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
                    "structured": {"people": people},
                    "cleaned_text": cleaned,
                }

                upsert_web_metadata(
                    document_id=doc_id,
                    url=url,
                    content_type="text/markdown",
                    data=data,
                )
                print(f"[normalize_det] ✅ upsert doc_id={doc_id} url={url}")

            except Exception as e:
                print(f"[normalize_det] ❌ error doc_id={doc_id} url={url} err={e}")

            time.sleep(sleep_s)

    print(f"[normalize_det] ⚠️ Llegó a max_rounds={max_rounds}. Aún podrían quedar pendientes.")


if __name__ == "__main__":
    run(limit=50, sleep_s=0.1, max_rounds=50)