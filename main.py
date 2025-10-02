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

# Local imports
import dbutils
import scraper_functions

def main():
    #Set Variables
    counties = {
        "Freestone": {"code": "081", "link": "https://freestone.tx.publicsearch.us/", "host": "GovOS"},
        "Anderson": {"code": "001", "link": "https://anderson.tx.publicsearch.us/", "host": "GovOS"},
    }

    county_name="Freestone"
    search_term="Emma Stone"

    county_code=counties[county_name]["code"]
    county_link=counties[county_name]["link"]

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

        # Launch Playwright once
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()

        # Each function gets its own page for isolation
        page1 = context.new_page()
        search_table=scraper_functions.get_search_results_table(search_term, county_name, county_link, page1)
        dbutils.insert_search_table_results(search_table, conn)

        #page2 = context.new_page()
        

        browser.close()

    conn.close()

if __name__ == "__main__":
    main()
