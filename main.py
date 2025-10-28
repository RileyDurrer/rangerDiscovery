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
    #options 

    refresh_searches=True  #whether to use old searches from db or not
    #True ubtil you build get_search_term_headers function

    save_searches=False     #whether to save new searches to db

    grab_documents=False   #whether to grab documents or not

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

        #get all associated headders for search term from DB or scrape new
        #look if search term already exists in DB
        if not dbutils.check_search_term_exists(search_term, county_name, conn) or refresh_searches:
            search_table=scraper_functions.get_search_results_table(search_term, county_name, county_link, page1)
            if save_searches:
                dbutils.insert_search_table_results(search_table, conn)
        else:
            search_table=dbutils.get_search_term_headers(search_term, county_name, conn)

        #display search_table
        df_search_table=pd.DataFrame(search_table)
        print(df_search_table)
        #save to csv for review
        df_search_table.to_csv(r"C:\Users\milom\Documents\landman\search_table.csv", index=False)


        if grab_documents:
            #Scrape document for each header in search_table if not already in DB
            search_table = dbutils.load_doc_paths_from_db_to_search_table(search_table, county_name, conn)
            #Get documents from links
            page2 = context.new_page()

            #for each row find doc_path if none
            for row in search_table:
                if not row.get("doc_path"):
                    doc_path = scraper_functions.get_document({"doc_link": row["doc_link"]}, county_name, page2)
                    row["doc_path"]=doc_path





        

        browser.close()

    conn.close()

if __name__ == "__main__":
    main()
