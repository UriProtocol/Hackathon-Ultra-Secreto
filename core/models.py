from dataclasses import dataclass

@dataclass
class Document:
    source_type: str
    canonical_identifier: str
    title: str
    raw_text: str | None = None
