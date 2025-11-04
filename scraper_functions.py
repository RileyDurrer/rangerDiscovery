# Standard library
import csv
import os
import random
import sys
import time
from datetime import datetime
import requests
from PIL import Image
import glob
import re

# Third-party packages
import pandas as pd
import psycopg
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

#Helpers
def acres_string_to_float(acreage_str):
    """Convert a string like '1 1/2', '1/5', '40', or '.5' into a float."""
    if not acreage_str or not isinstance(acreage_str, str):
        return None

    # Remove all characters except digits, dot, slash, and space
    cleaned = re.sub(r'[^0-9./\s]', '', acreage_str).strip()

    # Mixed number (e.g. '1 1/2')
    if re.match(r'^\d+\s+\d+/\d+$', cleaned):
        whole, num, den = map(float, re.findall(r'\d+', cleaned))
        return round(whole + num / den, 6)

    # Pure fraction (e.g. '1/5')
    if re.match(r'^\d+/\d+$', cleaned):
        num, den = map(float, re.findall(r'\d+', cleaned))
        return round(num / den, 6)

    # Decimal (e.g. '.5', '5.44')
    if re.match(r'^\d*\.\d+$', cleaned):
        return round(float(cleaned), 6)

    # Whole number (e.g. '40')
    if re.match(r'^\d+$', cleaned):
        return round(float(cleaned), 6)

    # If none of the above matched, return None
    print('failed to convert string')
    return None

def parse_legal_description(legal_desc, doc_type):
    """Extracts survey name and abs number from a single legal description string."""
    if not isinstance(legal_desc, str):
        return pd.Series([None, None, None, None, None, None])
    
    abs_patterns = [
        r'AB#\d+',
        r'A-\d+',
        r'Survey: \d+'
    ]
    survey_patterns = [
        r'.*LEAGUE',
        r'.*SUR',
        r'.*GRANT',
        r'Survey- Name: .*'
    
    ]
    unwanted_survey_phrases = [
        'Survey- Name: ',
        'Survey Name: ',
        'Survey-',
        'Survey -',
        'GRANT',
        'SURVEY',
        'SUR',
        'LEAGUE',


    ]


    acreage_patterns = [
        # --- Decimals (with parentheses) ---
        r'\(\d+\.\d+\s*ACRES\)',             # (5.44 ACRES), 5.44 ACS
        r'\(\.\d+\s*ACRES\)',                # (.5 ACRES), (.5 ACS)
        r'\(\d+\.\d+\s*ACS\)',             # (5.44 ACRES), 5.44 ACS
        r'\(\.\d+\s*ACS\)',                # (.5 ACRES), (.5 ACS)

        # --- Decimals (without parentheses) ---
        r'\d*\.\d+\s*ACRES',             # (5.44 ACRES), 5.44 ACS
        r'\.\d+\s*ACRES',                # (.5 ACRES), (.5 ACS)
        r'\d*\.\d+\s*ACS',             # (5.44 ACRES), 5.44 ACS
        r'\.\d+\s*ACS',                # (.5 ACRES), (.5 ACS)


        # --- Mixed numbers (whole + fraction) ---
        r'\(\d+\s+\d+/\d+\s*ACRES\)',        # (1 1/2 ACRES), 1 1/2 ACS
        r'\d+\s+\d+/\d+\s*ACRES',

        # --- Pure fractions ---
        r'\(\d+/\d+\s*ACRES\)',              # (1/5 ACRES), 1/5 ACS
        r'\d+/\d+\s*ACRES',

        # --- Whole numbers ---
        r'\(PT \d*\.?\d+ ACRES\)', # (PT 5.44 ACRES)
        r'\(\d+\s*ACRES\)', 
        r'\d+\s*ACRES',                  # (5 ACRES), 5 ACS
        r'\d+ ACS',

        # --- Labelled or unusual formatting ---
       r'Acres?:\s*\d*\.?\d+', # Acres: 54.2 Acres: .96
       r'Acres: \d'

        


    ]
    subdivision_patterns = [   
        r'L-\w+ (?:\b\w+\b\s+)*ADDN',   
        r'ADDN \d \d{5} \d{5} \d{3}[A-Z]',
        r'ADDN \d{5} \d{5} \d{3}[A-Z]',
        r'ADDN \d+',
        r'SUBD LOT \d+',
        r'SUBD \d+',
        r'\d{5} \d{4} \d{4}[A-Z] \d{4}',
        r'\d{5} \d{5} \d{4}',
        r'\d{5} \d{5} \d{3}[A-Z]',
        r'L-\w+ (?:\b\w+\b\s+)*',
        r'\w+ \d{5}',
        r'Subdivision\s?-',
    

        


    ]
    misc_legal_patterns = [
        r'MULTIPLE TRACTS SEE INSTRUMENT',
        r'N/A',

    ]


    
    abs_num = None
    survey_name = None
    acreage = None
    subdivision = None
    case_number = None
    misc_legal = None

    if doc_type == 'ABSTRACT JUDGEMT':
        case_number = legal_desc.strip()
        return pd.Series([abs_num, survey_name, acreage, subdivision, case_number, misc_legal])

    #Checks for undescriptive legal descriptions to return as misc_legal
    for pattern in misc_legal_patterns:
        match = re.search(pattern, legal_desc, re.IGNORECASE)
        if match:
            misc_legal = legal_desc.strip()
            return pd.Series([abs_num, survey_name, acreage, subdivision, case_number, misc_legal]) 

    #Extract Columns from legal description
    # ABS Number
    for pattern in abs_patterns:
        match = re.search(pattern, legal_desc)
        if match:
            abs_num = match.group(0)
            legal_desc = legal_desc.replace(abs_num, '').strip()
            # keep only digits from the match
            abs_num = re.sub(r'\D', '', abs_num)
            break
    
    #Numeric value of Acreage
    for pattern in acreage_patterns:
        match = re.search(pattern, legal_desc, re.IGNORECASE)
        if match:
            acreage_str=match.group(0)
            legal_desc=legal_desc.replace(acreage_str,'').strip()
            acreage=acres_string_to_float(acreage_str)
            break



    
    for pattern in subdivision_patterns:
        match = re.search(pattern, legal_desc, re.IGNORECASE)
        if match:
            subdivision = legal_desc 
            return pd.Series([abs_num, survey_name, acreage, subdivision, case_number, misc_legal])

    # Survey Name

    for pattern in survey_patterns:
        match = re.search(pattern, legal_desc)
        if match:
            survey_name = match.group(0)
            legal_desc = legal_desc.replace(survey_name, '').strip()
            break
        if not match:
            if abs_num or acreage:
                survey_name = legal_desc.strip()
                legal_desc = legal_desc.replace(survey_name, '').strip()
                break


    misc_legal = legal_desc.strip() 
    if survey_name:
        for phrase in unwanted_survey_phrases:
            if phrase in survey_name:
                survey_name = survey_name.replace(phrase, "")
                misc_legal += f" st: {phrase}"
        survey_name = survey_name.strip()

    if survey_name=='':
        survey_name = None

    

    if misc_legal=='':
        misc_legal = None
        
    
    

    
    return pd.Series([abs_num, survey_name, acreage, subdivision, case_number, misc_legal])




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

            parsed = parse_legal_description(legal_description, doc_type)   ### ⬅ ADDED
            abs_num, survey_name, acreage, subdivision, case_number, misc_legal = parsed  ### ⬅ ADDED

            results_list.append({
                "grantor": grantor,
                "grantee": grantee,
                "doc_type": doc_type,
                "recorded_date": recorded_date,
                "doc_number": doc_number,
                "book_vol_page": book_vol_page,
                "legal_description": legal_description,
                "doc_link": doc_link,

                # ✅ parsed fields
                "abs_num":      abs_num,
                "survey_name":  survey_name,
                "acreage":      acreage,
                "subdivision":  subdivision,
                "case_number":  case_number,
                "misc_legal":   misc_legal,
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




def get_document(link, doc_id, county_name, page):
    base_dir = r"C:\Users\milom\Documents\landman\county_clerk_docs"
    output_dir = os.path.join(base_dir, county_name)
    os.makedirs(output_dir, exist_ok=True)

    print(f"➡️ Accessing {link}…")
    page.goto(link)
    page.wait_for_timeout(1500)

    # Hide panel if visible
    hide_button = page.query_selector("button.css-okyhgk")
    if hide_button:
        hide_button.click()
        page.wait_for_timeout(500)

    # Prepare environment for clarity
    page.set_viewport_size({"width": 4000, "height": 3200})
    page.evaluate("document.body.style.zoom = '200%'")
    page.evaluate("document.body.style.background = 'white'")

    # Detect total pages from input box
    page_input = page.query_selector("input[aria-label='Page Number']")
    if not page_input:
        print("⚠️ Page input box not found.")
        return []
    total_pages = int(page_input.get_attribute("max"))
    print(f"📄 Total pages: {total_pages}")

    downloaded_files = []

    # Capture all pages
    for page_index in range(1, total_pages + 1):
        page_input.fill(str(page_index))
        page.evaluate("el => el.dispatchEvent(new Event('change', { bubbles: true }))", page_input)
        page.wait_for_timeout(3500)  # give each page time to render

        img = page.query_selector("svg image")
        if not img:
            print(f"⚠️ No image found on page {page_index}")
            continue

        img.scroll_into_view_if_needed()
        filename = f"page_{page_index}.png"
        filepath = os.path.join(output_dir, filename)
        img.screenshot(path=filepath, scale="device", omit_background=False)
        downloaded_files.append(filepath)
        print(f"✅ Saved page {page_index}")

    # Combine PNGs into one PDF
    if downloaded_files:
        image_files = sorted(glob.glob(os.path.join(output_dir, "page_*.png")))
        first_image = Image.open(image_files[0]).convert("RGB")
        others = [Image.open(img).convert("RGB") for img in image_files[1:]]
        pdf_path = os.path.join(output_dir, f"{doc_id}.pdf")
        first_image.save(pdf_path, save_all=True, append_images=others)
        print(f"📄 Combined {len(image_files)} pages into {pdf_path}")

        # Delete PNGs after creating PDF
        for img_file in image_files:
            os.remove(img_file)
        print("🧹 Deleted temporary PNG files.")

        #remove base_dir from pdf_path for return
        relative_pdf_path = os.path.relpath(pdf_path, base_dir)
        return [relative_pdf_path]
    else:
        print("⚠️ No images captured — skipping PDF creation.")
        return None




if __name__ == "__main__":
    # Example usage
    county = "Freestone"
    county_link = "https://freestone.tx.publicsearch.us/"
    search_term = "Alford John"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        
        files=get_document('https://freestone.tx.publicsearch.us/doc/94458756', "tester", county, page)
        print(files)

        print("Document scraping complete.")

        browser.close()











