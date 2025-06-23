import os
import shutil

# Folder sumber dan tujuan
source_folder = "sample"  # ganti dengan nama folder sample jika ada
raw_folder = "raw"

# Subfolder berdasarkan tipe file
subfolders = {
    "csv": os.path.join(raw_folder, "csv"),
    "pdf": os.path.join(raw_folder, "pdf"),
    "txt": os.path.join(raw_folder, "txt")
}

# Membuat subfolder jika belum ada
for subfolder in subfolders.values():
    os.makedirs(subfolder, exist_ok=True)

# Daftar file dan tujuan penyalinan
files_to_copy = {
    "Data Co2.csv": subfolders["csv"],
    "PT PRIMA BERSAMA.pdf": subfolders["pdf"],
    "dokumentasi interaksi pengguna di media sosial.txt": subfolders["txt"]
}

# Menyalin file
for filename, dest_folder in files_to_copy.items():
    src_path = os.path.join(source_folder, filename)
    dst_path = os.path.join(dest_folder, filename)
    if os.path.exists(src_path):
        shutil.copy2(src_path, dst_path)
        print(f"Copied {filename} to {dest_folder}")
    else:
        print(f"{filename} not found in {source_folder}")
