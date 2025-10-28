

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
                (search_term, doc_type, recorded_date, doc_number, book_vol_page, legal_description, source_county, doc_link)
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

def check_search_term_exists(search_term, county, conn):
    """
    Check if a search term already exists in the documents_header table for a given county code.

    Args:
        search_term: The search term to check.
        county: The county to check against.
        conn: psycopg connection object.
    Returns:
        bool: True if the search term exists, False otherwise.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS(
                SELECT 1
                FROM clerk.documents_header
                WHERE search_term = %s AND source_county = %s
            );
        """, (search_term, county))
        return cur.fetchone()[0]

def load_doc_paths_from_db_to_search_table(search_table, county, conn):
    """
    Given a search_table (list of dicts) and a county,
    add 'doc_path' from clerk.documents_header if it exists in the DB.
    Keeps all original keys in each row.
    """
    # Extract doc_numbers from the scraped data
    doc_numbers = [row["doc_number"] for row in search_table if row.get("doc_number")]
    if not doc_numbers:
        print("⚠️ No document numbers found in search_table.")
        return search_table

    # Query database for existing paths
    with conn.cursor() as cur:
        cur.execute("""
            SELECT doc_number, doc_path
            FROM clerk.documents_header
            WHERE source_county = %s
              AND doc_number = ANY(%s);
        """, (county, doc_numbers))
        db_rows = cur.fetchall()

    # Build lookup dictionary {doc_number: doc_path}
    db_lookup = {doc_number: doc_path for doc_number, doc_path in db_rows}

    # Merge doc_path into the existing search_table
    updated = []
    for row in search_table:
        doc_number = row.get("doc_number")
        if not doc_number:
            row["doc_path"] = None
        else:
            row["doc_path"] = db_lookup.get(doc_number)
        updated.append(row)

    print(f"✅ Added 'doc_path' to {len(db_lookup)} matching records (total rows: {len(updated)}).")
    return updated
