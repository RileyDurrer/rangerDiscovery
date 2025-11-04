

# db_utils.py or scraper_functions.py
def insert_search_table_results(results_list, conn):
    doc_cols = [
        "search_term", "doc_type", "recorded_date", "doc_number",
        "book_vol_page", "legal_description", "source_county", "doc_link",
        "doc_path", "abstract_num", "county", "survey_name",
        "acres", "subdivision", "case_number", "misc_legal"
    ]

    insert_doc_sql = """
        INSERT INTO clerk.document_header (
            search_term, doc_type, recorded_date, doc_number,
            book_vol_page, legal_description, source_county, doc_link,
            doc_path, abstract_num, county, survey_name,
            acres, subdivision, case_number, misc_legal
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (doc_number, source_county)
        DO UPDATE SET
            search_term     = EXCLUDED.search_term,
            doc_type        = EXCLUDED.doc_type,
            recorded_date   = EXCLUDED.recorded_date,
            book_vol_page   = EXCLUDED.book_vol_page,
            legal_description = EXCLUDED.legal_description,
            doc_link        = EXCLUDED.doc_link,
            doc_path        = EXCLUDED.doc_path,
            abstract_num    = EXCLUDED.abstract_num,
            county          = EXCLUDED.county,
            survey_name     = EXCLUDED.survey_name,
            acres           = EXCLUDED.acres,
            subdivision     = EXCLUDED.subdivision,
            case_number     = EXCLUDED.case_number,
            misc_legal      = EXCLUDED.misc_legal,
            scraped_at      = NOW()
        RETURNING id;
    """

    insert_party_sql = """
        INSERT INTO clerk.party (party_name)
        VALUES (%s)
        RETURNING id;
    """

    select_party_sql = """
        SELECT id FROM clerk.party WHERE party_name = %s LIMIT 1;
    """

    insert_role_sql = """
        INSERT INTO clerk.document_party_role (document_header_id, party_id, role, original_name)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """

    with conn.cursor() as cur:
        for row in results_list:

            # 1) UPSERT document
            values = tuple(row.get(c) for c in doc_cols)

            cur.execute(insert_doc_sql, values)
            document_id = cur.fetchone()[0]

            # 2) Party loading
            for role in ("grantor", "grantee"):
                name = row.get(role)
                if not name or not name.strip():
                    continue

                name = name.strip()

                # Try insert
                cur.execute(insert_party_sql, (name,))
                r = cur.fetchone()
                if r:
                    party_id = r[0]
                else:
                    # fallback lookup
                    cur.execute(select_party_sql, (name,))
                    party_id = cur.fetchone()[0]

                # 3) Link party + doc
                cur.execute(
                    insert_role_sql,
                    (document_id, party_id, role, name)
                )

        conn.commit()


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
    Used to determine if a document needs to be scraped or not.
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
