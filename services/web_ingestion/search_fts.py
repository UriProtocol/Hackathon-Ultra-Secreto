from core.database import get_connection

def search_fts(query: str, limit: int = 10):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
          d.id,
          d.canonical_identifier as url,
          wm.data->>'title' as title,
          ts_rank_cd(
            to_tsvector('spanish', wm.data->>'cleaned_text'),
            plainto_tsquery('spanish', %s)
          ) AS rank
        FROM web_metadata wm
        JOIN documents d ON d.id = wm.document_id
        WHERE to_tsvector('spanish', wm.data->>'cleaned_text')
              @@ plainto_tsquery('spanish', %s)
        ORDER BY rank DESC
        LIMIT %s
        """,
        (query, query, limit),
    )

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return rows


def main():
    query = "directorio de investigadores del cinvestav"
    results = search_fts(query)

    print(f"\nResultados para: '{query}'\n")
    for r in results:
        doc_id, url, title, rank = r
        print(f"- ({rank:.3f}) {title or '[sin título]'}")
        print(f"  {url}\n")


if __name__ == "__main__":
    main()