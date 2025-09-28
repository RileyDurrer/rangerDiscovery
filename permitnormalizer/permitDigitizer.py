# This script extracts data from a folder of PDF files containing permit information and processes it into a structured format.

import logging
import os
import re
import time

import fitz  # PyMuPDF for PDF manipulation
import pandas as pd
import pdfplumber
import pytesseract
from PIL import Image
from pdf2image import convert_from_path

# Suppress verbose pdfplumber warnings
logging.getLogger("pdfminer").setLevel(logging.ERROR)
start = time.time()


Replace=True

#Paths
input_folder_path = r"C:\Users\milom\Documents\landman\permitnormalizer\PDFinput"
permit_csv_path = r"C:\Users\milom\Documents\landman\permitnormalizer\output/permit_data.csv"
field_csv_path = r"C:\Users\milom\Documents\landman\permitnormalizer\output\field_data.csv"
plat_csv_path = r"C:\Users\milom\Documents\landman\permitnormalizer\output/plat_data.csv"

#setting up data frames and thier columns
columns = [
    "API Number",#
    "File Path",#
    "Page Count",#
    "Drilling Permit Number",#
    "SWR Exception",#
    "Form Type",#
    "Permit Status",#
    "RRC Operator No.",#
    "Operator's Name",#
    "Operator Address",#
    "Lease Name",#
    "Well No.",#
    "Purpose of filing: New Drill",#
    "Purpose of filing: Recompletion",#
    "Purpose of filing: Reclass",#
    "Purpose of filing: Field Transfer",#
    "Purpose of filing: Re-Enter",#
    "Purpose of filing: Amended",#
    "Purpose of filing: Amended as Drilled (BHL)",#
    "Wellbore Profile: Vertical",#
    "Wellbore Profile: Horizontal",#
    "Wellbore Profile: Directional",#
    "Wellbore Profile: Sidetrack",#
    "Total Depth",#
    "Right to develop minerals",# 
    "Hydrogen sulfide area",#
    "RRC District No.",#
    "County",#
    "Surface Location",#
    "Distance to nearest town",#
    "Direction to nearest town",#
    "Nearest Town",#
    "Section",#
    "Block",#
    "Survey",#
    "Abstract No.",#
    "Distance to nearest lease line",#
    "Number of contiguous acres in lease",#
    "Lease Perpendiculars Distance 1",#
    "Lease Perpendiculars Direction 1",#
    "Lease Perpendiculars Distance 2",#
    "Lease Perpendiculars Direction 2",#
    "Survey Perpendiculars Distance 1",#
    "Survey Perpendiculars Direction 1",#
    "Survey Perpendiculars Distance 2",#
    "Survey Perpendiculars Direction 2",#
    "Is this a pooled unit?",#
    "Unitization Docket No",#
    "Substandard Acreage Field",  #
    "Field ID's",#
    "Remarks",#
    "Name of filer",#
    "Date submitted",#
    "Phone",#
    "E-mail Address (OPTIONAL)",#
    "RRC Use Only Data Validation Time Stamp"#
]

fieldcolumns = [
    "RRC District No.",#
    "Field No.",#
    "Field Name",#
    "Well Type",#
    "Completion Depth",#
    "Distance to Nearest Well in this Reservoir",#
    "Number of Wells in this Reservoir"#
    ]

platcolumns = [
    "API Number",
    "File Path",
    "Page Count",
    "name",
    "contents"
]

# Create empty DataFrames with the specified columns
permit_df= pd.DataFrame(columns=columns)
field_df = pd.DataFrame(columns=fieldcolumns)
plat_df = pd.DataFrame(columns=platcolumns)




def extract_data_from_pdf(file_path):
    # Initialize a dictionary to hold the extracted data
    data = {col: "" for col in columns}
    with pdfplumber.open(file_path) as pdf:
        data["File Path"] = file_path  # Store the file path
        page_count = len(pdf.pages)
        data["Page Count"] = page_count  # Store the page count in the data dictionary``
        page = pdf.pages[0]  # only first page
        table = page.extract_table()
        if table:
            #---Header Information---
            #API Cell
            api_cell = table[0][0]
            api_parts = api_cell.split("\n")
            data["API Number"] = api_parts[1]
            data["Drilling Permit Number"] = api_parts[3]
            data["SWR Exception"] = api_parts[5] if len(api_parts) > 5 else "None"
            data["Form Type"] = table[0][16].replace('\n', ' ').strip()
            data["Permit Status"] = (table[1][16])[14:].strip()  # Extracting the status from the cell
            data["RRC Operator No."] = table[2][0].split("\n")[1].strip()
            data["Operator's Name"] = table[2][6].split("\n")[1].strip()
            data["Operator Address"] = ' '.join(table[2][13].split("\n")[1:]).strip()
            data["Lease Name"] = table[3][0].split("\n")[1].strip()
            data["Well No."] = table[3][9].split("\n")[1].strip()

            #---General Information---
            #Purpose of filing
            purpose_cell = table[5][0]
            data["Purpose of filing: New Drill"] = True if "X New Drill" in purpose_cell else False
            data["Purpose of filing: Recompletion"] = True if "X Recompletion" in purpose_cell else False
            data["Purpose of filing: Reclass"] = True if "X Reclass" in purpose_cell else False
            data["Purpose of filing: Field Transfer"] = True if "X Field Transfer" in purpose_cell else False
            data["Purpose of filing: Re-Enter"] = True if "X Re-Enter" in purpose_cell else False
            data["Purpose of filing: Amended"] = True if "X Amended" in purpose_cell else False
            data["Purpose of filing: Amended as Drilled (BHL)"] = True if "X Amended as Drilled (BHL)" in purpose_cell else False

            # Wellbore Profile
            wellbore_profile_cell = table[6][0]
            data["Wellbore Profile: Vertical"] = True if "X Vertical" in wellbore_profile_cell else False
            data["Wellbore Profile: Horizontal"] = True if "X Horizontal" in wellbore_profile_cell else False
            data["Wellbore Profile: Directional"] = True if "X Directional" in wellbore_profile_cell else False
            data["Wellbore Profile: Sidetrack"] = True if "X Sidetrack" in wellbore_profile_cell else False
            
            # Total Depth
            data["Total Depth"] = table[7][0].split("\n")[1].strip()

            # Hydrogen sulfide area
            data["Hydrogen sulfide area"] = True if "X Yes" in table[7][9] else (False if "X No" in table[7][9]else None)

            #---Surface Location and Acreage Information---
            data["RRC District No."] = table[9][0].split("\n")[1].strip()
            data["County"] = table[9][3].split("\n")[1].strip()

            # Surface Location
            surface_location_cell = table[9][8]
            if "X Land" in surface_location_cell:
                data["Surface Location"] = "Land"
            elif "X Bay/Estuary" in surface_location_cell:
                data["Surface Location"] = "Bay/Estuary"
            elif "X Inland Waterway" in surface_location_cell:
                data["Surface Location"] = "Inland Waterway"
            elif "X Offshore" in surface_location_cell:
                data["Surface Location"] = "Offshore"
            else:
                data["Surface Location"] = None

            # Distance to nearest town
            town_cell = table[10][0]
            town_cell=town_cell.split("\n")[0]
            parts = town_cell.split(maxsplit=2)
            data["Distance to nearest town"] = parts[0].strip()
            data["Direction to nearest town"] = parts[1].strip()
            data["Nearest Town"] = parts[2].rstrip() if len(parts) > 2 else ""

            # Section, Block, Survey, Abstract No.
            section = table[11][0].split("\n")
            if len(section) > 1:
                data["Section"] = section[1].strip()
            else:
                data["Section"] = None

            block = table[11][2].split("\n")
            if len(block) > 1:
                data["Block"] = block[1].strip()    
            else:
                data["Block"] = None
            
            data["Survey"] = table[11][5].split("\n")[1].strip()
            data["Abstract No."] = table[11][10].split("\n")[1].strip()
            data["Distance to nearest lease line"] = table[11][12].split("\n")[1].strip()

            # Number of contiguous acres in lease
            cont_acre=table[11][15].split("\n")[1].strip()
            data["Number of contiguous acres in lease"] = re.sub(r'\D', '', cont_acre)  # Remove non-digit characters

            #Perpendiculars
            lease_cell = table[12][0]

            lease_match = re.search(r"Lease Perpendiculars: (\d+) ft from the (\w+) line and (\d+) ft from the (\w+) line", lease_cell)
            survey_match = re.search(r"Survey Perpendiculars: (\d+) ft from the (\w+) line and (\d+) ft from the (\w+) line", lease_cell)

            if lease_cell:
                # Lease
                lease_match = re.search(
                    r"Lease Perpendiculars: (\d+) ft from the (\w+) line and (\d+) ft from the (\w+) line",
                    lease_cell
                )
                if lease_match:
                    data["Lease Perpendiculars Distance 1"] = lease_match.group(1)
                    data["Lease Perpendiculars Direction 1"] = lease_match.group(2)
                    data["Lease Perpendiculars Distance 2"] = lease_match.group(3)
                    data["Lease Perpendiculars Direction 2"] = lease_match.group(4)

                # Survey
                survey_match = re.search(
                    r"Survey Perpendiculars: (\d+) ft from the (\w+) line and (\d+) ft from the (\w+) line",
                    lease_cell
                )
                if survey_match:
                    data["Survey Perpendiculars Distance 1"] = survey_match.group(1)
                    data["Survey Perpendiculars Direction 1"] = survey_match.group(2)
                    data["Survey Perpendiculars Distance 2"] = survey_match.group(3)
                    data["Survey Perpendiculars Direction 2"] = survey_match.group(4)
            
            data["Is this a pooled unit?"] = True if "X Yes" in table[13][0] else (False if "X No" in table[13][0]else None )

            #Unitization Docket No
            docket = table[13][7]
            data["Unitization Docket No"] = docket.split(":", 1)[1].strip() 
            if data["Unitization Docket No"] == "":
                data["Unitization Docket No"] = None

            #---Field information---
            fields = []
            bottomhole_row = None  # will record the row index
            permit_fields_df = pd.DataFrame(columns=fieldcolumns)

            y = 16
            while y < len(table):
                row = table[y]
                cell0 = row[0] if row and len(row) > 0 else None
                
                # stop condition
                if cell0 and cell0.strip().startswith("BOTTOMHOLE LOCATION INFORMATION"):
                    bottomhole_row = y
                    break
                
                if row[0] not in ["", None]:
                    #add_field(row)
                    field_data = {col: "" for col in fieldcolumns}
                    field_data['RRC District No.'] = row[0].strip() 
                    field_data['Field No.'] = row[1].strip()
                    field_data['Field Name'] = row[4].strip() 
                    field_data['Well Type'] = row[11].strip()  
                    field_data['Completion Depth'] = row[14].strip() 
                    field_data['Distance to Nearest Well in this Reservoir'] = row[15].strip()
                    field_data['Number of Wells in this Reservoir'] = row[17].strip()
                    permit_fields_df = pd.concat([permit_fields_df, pd.DataFrame([field_data])], ignore_index=True)

                # process only non-empty rows
                if cell0:
                    col1 = row[1] if len(row) > 1 and row[1] else ""
                    fields.append(f"{cell0.strip()}-{col1.strip()}")
                
                y += 1

            #Join the fields into a single string
            data["Field ID's"] = "|".join(fields) 

            current_row = bottomhole_row + 1 

            #---Remarks and Certificate Information---
            #Remarks
            lines = table[current_row][0].split("\n")[1:]
            data["Remarks"] = " ".join(line.strip() for line in lines if line)

            # Certificate Information)
            certificate = table[current_row][12].split("\n")
            name_date = certificate[3].strip() 
            parts = name_date.split(" ")
            name = " ".join(parts[:-3])           # all but last 3 parts
            date = " ".join(parts[-3:])           # last 3 parts

            data["Name of filer"] = name
            data["Date submitted"] = date

            phone_email = certificate[5].strip() .split(" ")
            data["Phone"] = phone_email[0] 
            data["E-mail Address (OPTIONAL)"] = phone_email[1] if len(phone_email) > 1 else None

            current_row += 1
            timestamp = table[current_row][0].strip() 
            data["RRC Use Only Data Validation Time Stamp"] = timestamp.split(":", 1)[1].strip()

            #Alternative method for Right to develop minerals & Substandard Acreage Field
            text= page.extract_text()

            if '9 m . i n D e o ra y ls o u u n h d a e v r e a t n h y e r r i i g g h h t t - o to f- d w e a v y e l ? op the Yes X No' in text:
                data["Right to develop minerals"] = False    
            elif '9 m . i n D e o ra y ls o u u n h d a e v r e a t n h y e r r i i g g h h t t - o to f- d w e a v y e l ? op the X Yes No' in text:
                data["Right to develop minerals"] = True
            else:
                data["Right to develop minerals"] = None

            if '25. Are you applying for Substandard Acreage Field? Yes (attach Form W-1A) X No' in text:
                data["Substandard Acreage Field"] = False
            elif '25. Are you applying for Substandard Acreage Field? X Yes (attach Form W-1A) No' in text:
                data["Substandard Acreage Field"] = True
            else:
                data["Substandard Acreage Field"] = None
            
            return data, permit_fields_df



# Get paths from the selected folder
all_files = []
for root_dir, _, files in os.walk(input_folder_path):
    for file in files:
        full_path = os.path.join(root_dir, file)
        all_files.append(full_path)

#filter between permit and plat files
permit_files = [f for f in all_files if f.endswith('.pdf') and 'W1_AsApprovedW1' in f]
plat_files = [f for f in all_files if '_Plat_' in f]

for file_path in permit_files:
    data, permit_fields_df = extract_data_from_pdf(file_path)
    permit_df = pd.concat([permit_df, pd.DataFrame([data])], ignore_index=True)
    field_df = pd.concat([field_df, permit_fields_df], ignore_index=True)

for file_path in plat_files:
    #Normalize file type
    if file_path.endswith('.tif'):
        #convert tif to pdf
        try:
            with Image.open(file_path) as img:
                pdf_file_path = file_path.replace('.tif', '.pdf')
                img.save(pdf_file_path, "PDF")
                os.remove(file_path)  # Remove the original TIF file after conversion
                file_path = pdf_file_path  # Update file path to the new PDF file
        except FileNotFoundError:
            print(f"Error: The file {file_path} was not found.")
            continue
        except IOError:
            print(f"Error: There was an issue with the image file {file_path}.")
            continue
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            continue
    
    if not file_path.lower().endswith('.pdf'):
        print(f"Skipping non-PDF file: {file_path}")
        continue  

    # Extract data from the PDF file
    data = {col: "" for col in platcolumns}
    data["API Number"] = file_path.split("\\")[-1].split("_")[0]  # Extract API Number from file name
    data["File Path"] = file_path 
    file_name=os.path.basename(file_path)
    docName = re.sub(r'^\d+_Plat_', '', file_name) # Remove the trailing number and .pdf extension
    data["name"] = os.path.splitext(docName)[0]

    with pdfplumber.open(file_path) as pdf:
        data["Page Count"] = len(pdf.pages)
        #get all text
        all_text = ''
        for page in pdf.pages:
            #get page rotation

            all_text += page.extract_text() + " , "
        data["contents"] = all_text.strip()  # Store the extracted text

    if not data["contents"]:
        all_text = ''
        # Convert PDF to images and use OCR if no text is extracted
        images = convert_from_path(file_path)
        for i, image in enumerate(images):
            # Use OCR to extract text from the image
            text = pytesseract.image_to_string(image)
            if text:
                print(f"OCR extracted text from page {i + 1} of {file_path}")
            all_text += text + " , "

    data["contents"] = data["contents"].replace('\n', ' ').strip()  # Clean up the text``

    plat_df = pd.concat([plat_df, pd.DataFrame([data])], ignore_index=True)


# Check if file exists
permit_file_exists = os.path.isfile(permit_csv_path)
field_file_exists = os.path.isfile(field_csv_path)
plat_file_exists = os.path.isfile(plat_csv_path)

# Append with or without header based on existence
# If Replace is True, overwrite the files; otherwise, append to them
if Replace:
    print("Replacing existing files with new data.")
    permit_df.to_csv(permit_csv_path, mode='w', index=False, header=True)
    field_df.to_csv(field_csv_path, mode='w', index=False, header=True) 
    plat_df.to_csv(plat_csv_path, mode='w', index=False, header=True)
else:
    print("Appending new data to existing files.")
    permit_df.to_csv(permit_csv_path, mode='a', index=False, header=not permit_file_exists)
    field_df.to_csv(field_csv_path, mode='a', index=False, header=not field_file_exists)
    plat_df.to_csv(plat_csv_path, mode='a', index=False, header=not plat_file_exists)

end = time.time()
print(f"Data extraction completed in {end - start:.2f} seconds.")