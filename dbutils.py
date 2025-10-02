

# db_utils.py or scraper_functions.py
def insert_search_table_results(results_list, conn):
    """
    Insert/Upsert scraped results into:
      - clerk.documents_header
      - clerk.party_names
      - clerk.document_parties

    Args:
        results_list: list of dictionaries from scraper (DB-ready)
        conn: psycopg connection object
    """
    doc_cols = [
        "search_term",
        "doc_type",
        "recorded_date",
        "doc_number",
        "book_vol_page",
        "legal_description",
        "source_county",
        "doc_link"
    ]

    with conn.cursor() as cur:
        for row in results_list:
            # --------------------
            # 1. Upsert document header
            # --------------------
            cur.execute("""
                INSERT INTO clerk.documents_header
                (search_term, doc_type, recorded_date, doc_number,
                 book_vol_page, legal_description, source_county, doc_link)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (doc_number, source_county)
                DO UPDATE SET
                    search_term = EXCLUDED.search_term,
                    doc_type = EXCLUDED.doc_type,
                    recorded_date = EXCLUDED.recorded_date,
                    book_vol_page = EXCLUDED.book_vol_page,
                    legal_description = EXCLUDED.legal_description,
                    doc_link = EXCLUDED.doc_link,
                    scraped_at = now()
                RETURNING id;
            """, tuple(row[c] for c in doc_cols))

            document_id = cur.fetchone()[0]

            # --------------------
            # 2. Insert parties (grantor, grantee)
            # --------------------
            for role in ["grantor", "grantee"]:
                name = row.get(role)
                if not name or name.strip() == "":
                    continue

                # Upsert into party_names
                cur.execute("""
                    INSERT INTO clerk.party_names (name)
                    VALUES (%s)
                    ON CONFLICT (name) DO NOTHING
                    RETURNING id;
                """, (name.strip(),))

                result = cur.fetchone()
                if result:
                    party_id = result[0]
                else:
                    cur.execute("SELECT id FROM clerk.party_names WHERE name = %s", (name.strip(),))
                    party_id = cur.fetchone()[0]

                # --------------------
                # 3. Link in join table
                # --------------------
                cur.execute("""
                    INSERT INTO clerk.document_parties (document_id, party_id, role)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (document_id, party_id, role))

    conn.commit()
    print(f"Upserted {len(results_list)} documents and linked parties into clerk schema")
