def reconstruct_abstract(inv_index):
    if not inv_index:
        return None

    words = []
    for word, positions in inv_index.items():
        for pos in positions:
            words.append((pos, word))

    words.sort()
    return " ".join([w for _, w in words])


def safe_get(data, *keys):
    """Safely get nested dictionary values without AttributeError"""
    for key in keys:
        if data is None:
            return None
        if isinstance(data, dict):
            data = data.get(key)
        else:
            return None
    return data

def normalize_work(work, existing_inst_ids, existing_author_ids=None):
    """
    Normalize work data including authors
    existing_author_ids: optional set of author IDs that already exist in DB
    """
    existing_institutions = []
    authors_list = []
    
    for authorship in work.get("authorships", []):
        # Process author
        author = authorship.get("author", {})
        author_id = author.get("id")
        
        if author_id:
            author_data = {
                "openalex_id": author_id,
                "display_name": author.get("display_name"),
                "orcid": author.get("orcid"),
                # You might get these from other parts of the API
                "last_known_institution_id": None,  # Could be derived
                "works_count": 0,  # Will update later
                "cited_by_count": 0  # Will update later
            }
            authors_list.append(author_data)
        
        # Process institutions (existing logic)
        for inst in authorship.get("institutions", []):
            inst_id = inst.get("id")
            if inst_id and inst_id in existing_inst_ids:
                existing_institutions.append(inst)
    
    # Prepare pivot data
    pivot_authors = []
    for authorship in work.get("authorships", []):
        author = authorship.get("author", {})
        author_id = author.get("id")
        if author_id:
            pivot_authors.append({
                "academic_metadata_id": work["id"],
                "author_openalex_id": author_id,
                "author_position": authorship.get("author_position", "middle"),
                "raw_affiliation": authorship.get("raw_affiliation_string")
            })

    return {
        # documents
        "canonical_identifier": work["id"],
        "source_type": "openalex",
        "title": work.get("title"),
        "raw_text": reconstruct_abstract(work.get("abstract_inverted_index")),

        # academic_metadata
        "doi": work.get("doi"),
        "journal_name": safe_get(work, "primary_location", "source", "display_name"),
        "publisher": safe_get(work, "primary_location", "source", "host_organization_name"),
        "issn": safe_get(work, "primary_location", "source", "issn_l"),
        "publication_year": work.get("publication_year"),
        "citation_count": work.get("cited_by_count"),
        "is_open_access": work.get("open_access", {}).get("is_oa"),
        "open_access_url": work.get("open_access", {}).get("oa_url"),
        "authors": work.get("authorships", []),  # Keep original for backward compatibility
        "institutions": existing_institutions,
        "concepts": work.get("concepts", []),
        "raw_source": work,
        
        # New fields for authors
        "authors_list": authors_list,  # Deduplicated list of authors in this work
        "pivot_authors": pivot_authors  # Relationship data
    }