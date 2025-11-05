def in_range(r, prev, nxt):
    d = r.get("recorded_date")

    # ✅ Keep rows with no date
    if not d:
        return True

    # ✅ If prev is given, enforce lower bound
    if prev is not None and d < prev:
        return False

    # ✅ If nxt is given, enforce upper bound
    if nxt is not None and d > nxt:
        return False

    return True

def order_documents(search_table, target_abstract_number, target_survey_name, previous_ownership_date, next_ownership_date):
    """
    Orders and filters search results PRIOR to document-level scraping.

    Purpose
    -------
    Prioritize and organize search hits returned from the county clerk lookups
    before attempting to download (scrape) the full document files.  
    This improves scraping efficiency and helps ensure the highest-confidence
    documents are processed first.

    Pipeline Overview
    -----------------
    1. Initial Ordering & Filtering
        • Sort rows from newest → oldest using `recorded_date`.
        • Optionally sort to prioritize *deed document types 
        • Optionally remove documents falling outside known ownership timeline.

    2. Priority Classification
        Each row receives a priority score based on data strength:
            Priority 1 — Exact abstract number 
            Priority 2 - exact survey name match
            Priority 3-5 — Fuzzy / similar survey name (handles typos + inconsistencies)
            Priority 6 — Inconclusive or vague legal descriptions 
                (e.g. “SEE INSTRUMENT”, “MULTIPLE TRACTS”, “N/A”)
            Priority 7 — Judgement / Affidavit / Heirship / CC JUDGMT types
            Priority 8 — Remaining rows (these should trigger a warning or error)

    3. Final Ordering
        Rows are sorted by:
            ( priority ASC, recorded_date DESC )


    4. OCR Fallback
        If a document cannot be confidently categorized into Priority 1–4,
        an OCR extraction phase may be triggered after it is downloaded to
        search the PDF for:
            • abstract number
            • survey name
            • subdivision
            • acreage
            • case number
        If still unresolved after OCR, raise an error or flag for manual review.

    When This Runs
    --------------
    • Runs AFTER scraping the header/search result table
    • Runs BEFORE the document-grab step (PDF download + parsing)
    • Does NOT affect DB insert order; DB storage remains unordered

    Returns
    -------
    • A list of row dictionaries with assigned `priority` values
    • Ordered so that the most useful files are scraped first
    • Lower-confidence rows are scraped later or flagged

    Notes
    -----
    • Assumes the presence of fields such as `recorded_date`, `doc_type`,
        `abstract_num`, `survey_name`, and `misc_legal`.
    • The purpose is to maximize scraping value + efficiency, NOT to preserve
        chronological order in the database.
    """

# Step 1: Initial Ordering & Filtering
# Sort by recorded_date (newest first)
rows.sort(key=lambda r: r.get("recorded_date") or date.min, reverse=True)
# Filter to ownership-relevant document dates
rows = [r for r in rows if in_range(r, previous_ownership_date, next_ownership_date)]

#Sort to prioritize "*DEED" document types
def deed_priority_key(r):
    doc_type = r.get("doc_type", "").upper()
    return 0 if "DEED" in doc_type else 1

rows.sort(key=deed_priority_key)

# Step 2: Priority Classification
for r in rows:
    abstract_num = r.get("abstract_num", "").strip().upper()
    survey_name = r.get("survey_name", "").strip().upper()
    misc_legal = r.get("misc_legal", "").strip().upper()
    doc_type = r.get("doc_type", "").strip().upper()

    # Priority 1: Exact abstract number 
    if abstract_num and abstract_num == target_abstract_number:
        r["priority"] = 1
        continue

    # Priority 2: Exact survey name match
    if survey_name and target_survey_name and (target_survey_name in survey_name or survey_name in target_survey_name):
        r["priority"] = 2
        continue
    
    # Priority 3-5: Fuzzy / similar survey name
    if survey_name and target_survey_name:
        ratio = fuzz.token_set_ratio(survey_name, target_survey_name)
        if ratio >= 90:
            r["priority"] = 3
        elif ratio >= 75:
            r["priority"] = 4
        elif ratio >= 50:
            r["priority"] = 5
        continue
    # Priority 6: Inconclusive or vague legal descriptions
    #if legal description = misc_legal 
    
    

    





