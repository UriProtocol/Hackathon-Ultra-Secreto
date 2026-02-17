
import sys
import os
import re
import json
import time
import spacy
from typing import List, Dict, Any

# Fix path to allow importing from core
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core.database import fetch_pending_web_metadata, upsert_web_metadata

# Load Spacy model
try:
    nlp = spacy.load("es_core_news_sm")
    print("[normalize_nlp] Spacy model 'es_core_news_sm' loaded.")
except OSError:
    print("[normalize_nlp] Spacy model 'es_core_news_sm' not found. Please run: python -m spacy download es_core_news_sm")
    sys.exit(1)

# Regex patterns
EMAIL_PATTERN = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
PHONE_PATTERN = r'(?:\+?\d{1,3}[ -]?)?\(?\d{3}\)?[ -]?\d{3}[ -]?\d{4}'
URL_PATTERN = r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+'
DATE_PATTERN = r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}[/-]\d{1,2}[/-]\d{1,2}'

def extract_entities(doc) -> List[Dict[str, str]]:
    """Extract named entities using Spacy."""
    entities = []
    seen = set()
    for ent in doc.ents:
        if ent.text not in seen:
            entities.append({"type": ent.label_, "name": ent.text})
            seen.add(ent.text)
    return entities

def extract_topics(doc) -> List[str]:
    """Extract potential topics using noun chunks and frequent nouns."""
    # Get frequent nouns/proper nouns
    nouns = [token.text.lower() for token in doc if token.pos_ in ["NOUN", "PROPN"] and not token.is_stop and len(token.text) > 3]
    
    # Simple frequency count
    from collections import Counter
    counts = Counter(nouns)
    return [word for word, count in counts.most_common(10)]

def extract_key_fields(text: str) -> Dict[str, List[str]]:
    """Extract emails, phones, links, dates using regex."""
    return {
        "emails": list(set(re.findall(EMAIL_PATTERN, text))),
        "phones": list(set(re.findall(PHONE_PATTERN, text))),
        "links": list(set(re.findall(URL_PATTERN, text))),
        "dates": list(set(re.findall(DATE_PATTERN, text))),
        "locations": [] # Populated via NER later if needed, but Spacy LOC covers it
    }

def generate_summary(doc, max_chars=600) -> str:
    """Generate a simple extractive summary (first few sentences)."""
    sentences = list(doc.sents)
    summary = ""
    for sent in sentences:
        if len(summary) + len(sent.text) <= max_chars:
            summary += sent.text + " "
        else:
            break
    return summary.strip()

def process_document(url: str, title: str, raw_text: str) -> Dict[str, Any]:
    """Process a single document with Spacy and Regex."""
    if not raw_text:
        return {}

    # Truncate text for Spacy to avoid memory issues with huge docs
    # Spacy max length is usually 1,000,000 characters
    text_to_process = raw_text[:900000]
    
    doc = nlp(text_to_process)
    
    entities = extract_entities(doc)
    
    # Separate locations from general entities for the specific schema field
    locations = [e["name"] for e in entities if e["type"] in ["LOC", "GPE"]]
    
    key_fields = extract_key_fields(raw_text)
    key_fields["locations"] = locations

    return {
        "summary": generate_summary(doc),
        "topics": extract_topics(doc),
        "entity_type": "unknown", # Hard to determine without LLM
        "entities": entities,
        "key_fields": key_fields,
        "structured": {},
        "evidence": [] # Cannot extract reliable evidence/claims without LLM
    }

def run(limit=10, sleep_s=0.1):
    print(f"[normalize_nlp] Starting local normalization (Spacy + Regex)...")
    
    while True:
        docs = fetch_pending_web_metadata(limit=limit)
        if not docs:
            print("[normalize_nlp] No hay pendientes. Terminando.")
            break

        for d in docs:
            doc_id = d["id"]
            url = d["url"]
            title = d["title"]
            raw_text = d["raw_text"]

            try:
                data = process_document(url, title, raw_text)

                upsert_web_metadata(
                    document_id=doc_id,
                    url=url,
                    content_type="text/markdown",
                    data=data,
                )

                print(f"[normalize_nlp] ✅ upsert web_metadata doc_id={doc_id}")

            except Exception as e:
                print(f"[normalize_nlp] ❌ error doc_id={doc_id} err={e}")

            time.sleep(sleep_s)

if __name__ == "__main__":
    run(limit=20)
