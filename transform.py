def order_documents(search_table, target_abstract_number):
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
        • Optionally filter to ownership-relevant document types 
            (e.g. DEED, MINERAL DEED, WARRANTY DEED, O&G LEASE).
        • Optionally remove documents falling outside known ownership timeline.

    2. Priority Classification
        Each row receives a priority score based on data strength:
            Priority 1 — Exact abstract number or exact survey match
            Priority 2 — Fuzzy / similar survey name (handles typos + inconsistencies)
            Priority 3 — Inconclusive or vague legal descriptions 
                (e.g. “SEE INSTRUMENT”, “MULTIPLE TRACTS”, “N/A”)
            Priority 4 — Judgement / Affidavit / Heirship / CC JUDGMT types
            Priority 5 — Remaining rows (these should trigger a warning or error)

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
