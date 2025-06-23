# Data Lake Pipeline Project – File Ingestion to Star Schema

This project simulates a complete **data lake architecture**, starting from raw file ingestion to the creation of a **star schema** in a data warehouse. It demonstrates how unstructured and semi-structured files are processed, analyzed, and transformed into structured analytical datasets using Python-based ETL scripts.

## Project Structure
data-lake-project/

- sample/ # Source folder with dummy files (CSV, TXT, PDF)
-  Data Co2.csv
-  dokumentasi interaksi pengguna di media sosial.txt
-  PT PRIMA BERSAMA.pdf

- raw/ # Output from ingest.py (organized by file type)
-  processed/ # Cleaned CSV files from processed.py

- ingest.py # Sorts files by type into 'raw/' folder
-  processed.py # Converts TXT and PDF files to CSV format
-  analyze.py # Analyzes and uploads data to staging database
-  stagtodw.py # Builds star schema in data warehouse (fact + dimensions)

## Pipeline Steps

### 1. `ingest.py` – File Ingestion
Sorts files from the `sample/` folder into the `raw/` folder based on file extension (`.csv`, `.txt`, `.pdf`).

### 2. `processed.py` – Format Standardization
Converts all non-CSV files (TXT, PDF) into CSV format using parsing and text extraction logic. Output is saved into the `processed/` folder.

### 3. `analyze.py` – Data Analysis & Upload
Performs basic analysis (e.g., aggregations, summarization) on the processed CSV files and uploads the results to the **staging database** (`datalake_staging`).

### 4. `stagtodw.py` – Star Schema Generation
Pulls data from the staging database and loads it into the **data warehouse** (`datalake_warehouse`). This step includes:
- Creating **fact tables**
- Creating **dimension tables**
- Defining relationships (foreign keys)
- Result: A functional **star schema** model for analytics

---

## Dummy Dataset Description

Initial files placed in the `sample/` folder:
- `Data Co2.csv` → structured transaction data
- `dokumentasi interaksi pengguna di media sosial.txt` → semi-structured log or interaction records
- `PT PRIMA BERSAMA.pdf` → unstructured notes or textual reports

These represent common real-world data lake inputs that need to be unified for analysis.

---

## Technologies Used

- **Python 3.x**
- **pandas**, **PyMuPDF**, **sqlalchemy**, **psycopg2**
- **PostgreSQL** for staging and warehouse layers
- Modular scripts for ETL, transformation, and schema modeling

---

## Output

By the end of this pipeline:
- All data is converted into structured CSV format
- Uploaded to a staging database for preprocessing
- Migrated to a star schema model in the data warehouse
- Ready for use in business intelligence tools or dashboarding

---

## Author

**Sultan Alamsyah Lintang Mubarok**  
Information Systems Student at Institut Teknologi Sepuluh Nopember  
Focus area: Data Engineering • Data Lakes • ETL Automation • Analytics Architecture



