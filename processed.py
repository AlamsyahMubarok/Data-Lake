import os
import pandas as pd
from PyPDF2 import PdfReader

# Folder sumber dan tujuan
raw_folder = "raw"
processed_folder = "processed"
os.makedirs(processed_folder, exist_ok=True)

# Fungsi proses CSV (cukup copy)
def process_csv(file_path, output_path):
    df = pd.read_csv(file_path)
    df.to_csv(output_path, index=False)
    print(f"Processed CSV: {output_path}")

# Fungsi proses PDF ke CSV (contoh: ekstrak tabel rasio)
def process_pdf(file_path, output_path):
    reader = PdfReader(file_path)
    full_text = ""
    for page in reader.pages:
        full_text += page.extract_text() + "\n"

    import re
    pattern = re.compile(
        r"(?P<category>[A-Za-z ]+)\s*\|\s*(?P<total_sales>[\d\.]+)\s*\|\s*(?P<units_sold>[\d\.]+)\s*\|\s*(?P<avg_price>[\d\.]+)",
        re.MULTILINE
    )

    data = []
    for match in pattern.finditer(full_text):
        cat = match.group("category").strip()
        sales = int(match.group("total_sales").replace(".", ""))
        units = int(match.group("units_sold").replace(".", ""))
        avg = int(match.group("avg_price").replace(".", ""))
        data.append((cat, sales, units, avg))

    if data:
        df = pd.DataFrame(data, columns=["Category", "Total Sales", "Units Sold", "Avg Unit Price"])
        df.to_csv(output_path, index=False)
        print(f"Processed PDF: {output_path}")
    else:
        print("No data extracted from PDF.")

# Fungsi proses TXT ke CSV (misal split per kalimat)
def process_txt(file_path, output_path):
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    
    cleaned_lines = [line.strip() for line in lines if line.strip()]
    df = pd.DataFrame(cleaned_lines, columns=["Content"])
    df.to_csv(output_path, index=False)
    print(f"Processed TXT: {output_path}")

# Proses semua file di raw/
for subfolder in os.listdir(raw_folder):
    subfolder_path = os.path.join(raw_folder, subfolder)
    if os.path.isdir(subfolder_path):
        for filename in os.listdir(subfolder_path):
            file_path = os.path.join(subfolder_path, filename)
            base_name, ext = os.path.splitext(filename)
            output_csv = os.path.join(processed_folder, f"{base_name}.csv")
            
            if ext.lower() == ".csv":
                process_csv(file_path, output_csv)
            elif ext.lower() == ".pdf":
                process_pdf(file_path, output_csv)
            elif ext.lower() == ".txt":
                process_txt(file_path, output_csv)
