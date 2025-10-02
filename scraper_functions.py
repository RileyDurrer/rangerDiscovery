# Standard library
import csv
import os
import random
import sys
import time
from datetime import datetime

# Third-party packages
import pandas as pd
import psycopg
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

#Set Variables
#county_link = "https://freestone.tx.publicsearch.us/"  # Example county link, replace with actual
#county="Freestone"

#counties = {
#    "Freestone": {"id": "081", "link": "https://freestone.tx.publicsearch.us/", "host": "GovOS"},
#    "Anderson": {"id": "001", "link": "https://anderson.tx.publicsearch.us/", "host": "GovOS"},
#}

#Load environment variables
#DB_NAME, DB_USER, DB_PASSWORD
load_dotenv(dotenv_path=r"C:\Users\milom\Documents\landman\.env")

conn = psycopg.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
)

#Scrapes file metadata related to a search term from a county public records website 
def get_search_results_table(search, county, county_link, page):
    # Define selectors
    search_box_selector = '[data-testid="searchInputBox"]'
    search_button_selector = '[data-testid="searchSubmitButton"]'
    table_row_selector = "#main-content div.search-results__results-wrap div.a11y-table table tbody tr"  # Selector for table rows in results
    nxt_button_selector = "#main-content div.search-results__results-wrap div.search-results__pagination nav div button:last-child"  # Selector for the "Next" button in pagination
    

    print(f"Searching for related files for: {search}")
    results_list = []
    page.goto(county_link)

    #fill the search input box with the search term
    page.fill(search_box_selector, search)

    #click the search button
    page.click(search_button_selector)
    print("Search submitted, waiting for results...")

    # Wait for results to appear
    page.wait_for_selector(table_row_selector)

    page_num = 1

    #---- Page loop ----
    while True:

        rows = page.query_selector_all(table_row_selector)
        # ---- Row loop --------------------------------------------------------------------
        
        for row in rows:
            text = row.inner_text().strip().split("\t")

            # extract columns from row text
            grantor, grantee, doc_type, recorded_date, doc_number, book_vol_page, legal_description = (
                text + [None] * (7 - len(text))  # pad if short
            )

            # Get document link from checkbox id
            checkbox = row.query_selector("input[data-testid='searchResultCheckbox']")
            doc_link = None
            if checkbox:
                checkbox_id = checkbox.get_attribute("id")  # e.g. "table-checkbox-94560668"
                if checkbox_id and "table-checkbox-" in checkbox_id:
                    doc_id = checkbox_id.replace("table-checkbox-", "")
                    doc_link = f"{county_link.rstrip('/')}/doc/{doc_id}"

            results_list.append({
                "grantor": grantor,
                "grantee": grantee,
                "doc_type": doc_type,
                "recorded_date": recorded_date,
                "doc_number": doc_number,
                "book_vol_page": book_vol_page,
                "legal_description": legal_description,
                "doc_link": doc_link
            })

        
        #---- End Row loop --------------------------------------------------------------------
        print(f"Processed page {page_num}, total results so far: {len(results_list)}")
        
        # Grab the "Next" button
        next_btn = page.query_selector(nxt_button_selector)
        if not next_btn:
            print("No next button found, stopping")
            break

        if next_btn.is_disabled() or "disabled" in (next_btn.get_attribute("class") or ""):
            print("Reached last page")
            break

        #save first row text to detect page change
        first_row_text = rows[0].inner_text().strip()

        next_btn.click()

        # Wait for page to load new results by checking that the first row has changed
        page.wait_for_function(
            """first => {
                const rows = document.querySelectorAll(
                "#main-content div.search-results__results-wrap div.a11y-table table tbody tr"
                );
                if (rows.length === 0) return false;
                return rows[0].innerText.trim() !== first;
            }""",
            arg=first_row_text
        )

        page_num += 1
        print("Navigated to next page")


        time.sleep(random.uniform(0.1, 0.3))  # tiny jitter

    print(f"Found {len(results_list)} results for {search}")

    # Post-process results
    for row in results_list:
        # Fix recorded_date
        date_str = row.get("recorded_date")
        if date_str:
            try:
                row["recorded_date"] = datetime.strptime(date_str, "%m/%d/%Y").date()
            except Exception:
                row["recorded_date"] = None
        else:
            row["recorded_date"] = None

        # Add metadata
        row["search_term"] = search
        row["source_county"] = county

    return results_list
         







def scrape_documents_from_row(row, page, output_dir=r"C:\Users\milom\Documents\landman\county_clerk_docs\Freestone"):
    """
    Clicks a search result row, saves all document images, then returns to results.
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    print("➡️ Opening row…")
    row.click()
    page.wait_for_timeout(2000)

    multi_btn = page.query_selector("button.css-1vdp520")
    if multi_btn:
        multi_btn.click()
        print("🖼️ Switched to Multi-View")

    # Wait for at least one doc image
    page.wait_for_selector("img[src*='/files/documents/']", timeout=20000)

    # Scroll to load more pages
    for _ in range(5):
        page.mouse.wheel(0, 2000)
        page.wait_for_timeout(1000)

    # Collect only doc images
    images = page.query_selector_all("img[src*='/files/documents/']")
    print(f"  Found {len(images)} document page images")

    for j, img in enumerate(images, start=1):
        src = img.get_attribute("src")
        alt = img.get_attribute("alt")
        print(f"    Page {j}: alt={alt}, src={src}")



       

                         
#df=scrape_related_files('Alford John', county)
#print(df.head())
#print("Scraping complete, inserting results into database...")
#insert_results(df, conn)
#print("Data insertion complete.")

conn.close()










