from openai import OpenAI
import os
from dotenv import load_dotenv
load_dotenv(dotenv_path=r"C:\Users\milom\Documents\landman\.env")

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def analyze_document_direct(pdf_path, prompt):
    # 1. Upload the file
    file_obj = client.files.create(
        file=open(pdf_path, "rb"),
        purpose="assistants"
    )

    # 2. Reference the uploaded file in your request
    response = client.responses.create(
        model="gpt-4.1",  # or "gpt-4o", "gpt-5"
        input=[
            {"role": "system", "content": prompt},
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": "Please extract all required fields from the attached document."},
                    {"type": "input_file", "file_id": file_obj.id},
                ],
            },
        ],
    )

    return response.output[0].content[0].text


prompt = """General Prompt	You are acting as a senior title examiner preparing a runsheet. For each document, extract and return all required fields with exactness. Maintain precision, consistency, and uniform formatting across every entry. All responses must be factual, structured identically, and free of paraphrasing or omission. If a field is not applicable, return ‘N/A.’ Do not infer beyond what is written in the document.
    Doc Type	State the full document type or title exactly as written on the face of the instrument (examples: Warranty Deed, Mineral Deed, Oil & Gas Lease, Deed of Trust, Release, Affidavit of Heirship, Assignment of Oil & Gas Lease, etc.). Do not abbreviate, paraphrase, or shorten.
    Book Type	Identify the recording book type abbreviation exactly as shown (examples: DR, DT, PA, OPR, DCM, OR, etc.). If spelled out (e.g., ‘Deed Records’), convert to the standard abbreviation.
    Doc #	Provide the document number exactly as recorded (instrument number, clerk file number, etc.). Do not add extra words, prefixes, or notes.
    B (Book)	Provide the book number exactly as recorded. If no book number is listed, return ‘N/A.’
    P (Page)	Provide the page number exactly as recorded. If no page number is listed, return ‘N/A.’
    Eff Date	Provide the effective date if one is stated. If none is shown, return ‘Not stated.’ If the instrument relates to probate or heirship, use the date of death (DOD) as the effective date if specified.
    Inst Date	Provide the execution (instrument/signature) date of the document. If multiple execution dates appear, return the latest. If none are shown, return ‘Not stated.’
    File Date	Provide the clerk’s official file/recording date as stamped in the margin or header. If not present, return ‘Not stated.’
    Grantor	List all grantor(s) exactly as written in the document, including punctuation, capitalization, marital status, trustee/executor/administrator language, or other role designations. If multiple grantors, list in the same order shown.
    Grantee	List all grantee(s) exactly as written in the document, including punctuation, capitalization, marital status, trustee/executor/administrator language, or other role designations. If multiple grantees, list in the same order shown.
    Comments/Description	Provide a runsheet remark in 2–3 sentences (up to 3 paragraphs max if complex). Summarize what the document accomplishes (e.g., conveyance, lease, assignment, release, probate, affidavit, lien). State whether interests are conveyed, reserved, assigned, or released, and identify burdens created or removed. Do not restate doc type, book type, doc #, book, page, or dates. Do not provide a full metes-and-bounds legal. Focus on substance.
    Land Description	Provide the land description verbatim as it appears (survey name, abstract number, subdivision, block/lot, or short metes-and-bounds). If the document references multiple tracts, list each tract separately in the order given. If no land description is present, state ‘No land description—blanket instrument.’ If the instrument clearly does not apply to real property (e.g., lien release against a person only), return ‘Not applicable.’"""
pdf_path = r"C:\Users\milom\Documents\landman\county_clerk_docs\Freestone\tester.pdf"
print(analyze_document_direct(pdf_path, prompt))