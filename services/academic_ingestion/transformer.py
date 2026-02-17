def reconstruct_abstract(inv_index):
    if not inv_index:
        return None

    words = []
    for word, positions in inv_index.items():
        for pos in positions:
            words.append((pos, word))

    words.sort()
    return " ".join([w for _, w in words])


def normalize_work(work):
    return {
        # documents
        "canonical_identifier": work["id"],
        "source_type": "openalex",
        "title": work.get("title"),
        "raw_text": reconstruct_abstract(work.get("abstract_inverted_index")),

        # academic_metadata
        "doi": work.get("doi"),
        "journal_name": work.get("host_venue", {}).get("display_name"),
        "publisher": work.get("host_venue", {}).get("publisher"),
        "issn": (
            work.get("host_venue", {}).get("issn_l")
            or None
        ),
        "publication_year": work.get("publication_year"),
        "citation_count": work.get("cited_by_count"),
        "is_open_access": work.get("open_access", {}).get("is_oa"),
        "open_access_url": work.get("open_access", {}).get("oa_url"),
        "authors": work.get("authorships", []),
        "institutions": [
            inst
            for authorship in work.get("authorships", [])
            for inst in authorship.get("institutions", [])
        ],
        "concepts": work.get("concepts", []),
        "raw_source": work
    }