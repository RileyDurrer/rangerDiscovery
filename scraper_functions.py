# Standard library
import csv
import os
import random
import sys
import time
from datetime import datetime
import requests

# Third-party packages
import pandas as pd
import psycopg
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright


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



def get_document(link, county_name, page):
    base_dir = r"C:\\Users\\milom\\Documents\\landman\\county_clerk_docs"
    output_dir = os.path.join(base_dir, county_name)
    os.makedirs(output_dir, exist_ok=True)

    print(f"➡️ Accessing {link}…")
    page.goto(link)
    page.wait_for_timeout(1500)

    # Hide the metadata panel
    hide_button = page.query_selector("button.css-okyhgk")
    if hide_button:
        hide_button.click()
        page.wait_for_timeout(500)

    # Prepare viewport and zoom for better resolution
    page.set_viewport_size({"width": 4000, "height": 3200})
    page.evaluate("document.body.style.zoom = '200%'")
    page.evaluate("document.body.style.background = 'white'")

    # Select all SVG <image> elements (actual document scans)
    images = page.query_selector_all("svg image")
    downloaded_files = []
    page_index = 1

    for img in images:
        href = img.get_attribute("xlink:href")
        if not href or "/images/" not in href:
            continue

        img.scroll_into_view_if_needed()

        filename = f"page_{page_index}.png"
        filepath = os.path.join(output_dir, filename)
        img.screenshot(path=filepath, scale="device", omit_background=False)

        downloaded_files.append(filepath)
        print(f"✅ Saved page {page_index}")
        page_index += 1

    print(f"🎉 Completed document. Saved {len(downloaded_files)} pages to {output_dir}")
    return downloaded_files

    




def scrape_documents_from_row(link, page, output_dir=r"C:\Users\milom\Documents\landman\county_clerk_docs\Freestone"):
    """
    Clicks a search result row, saves all document images, then returns to results.
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    print("➡️ Opening row…")
    link.click()
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


if __name__ == "__main__":
    # Example usage
    county = "Freestone"
    county_link = "https://freestone.tx.publicsearch.us/"
    search_term = "Alford John"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        
        files=get_document('https://freestone.tx.publicsearch.us/doc/94458756', county, page)
        print(files)

        print("Document scraping complete.")

        browser.close()











