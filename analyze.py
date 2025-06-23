import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import os
import re

# Koneksi ke database stagging
engine = create_engine("postgresql+psycopg2://postgres:alamsyah25@localhost:5432/datalake_stagging")

def analyze_co2(csv_path):
    print("=== Analisis Statistik Deskriptif CO2 Harian ===")
    df = pd.read_csv(csv_path)
    print("Kolom yang tersedia:", df.columns.tolist())

    co2_stats = {
        "Maksimum": df["CO2_ppm"].max(),
        "Minimum": df["CO2_ppm"].min(),
        "Rata-rata": round(df["CO2_ppm"].mean(), 2),
        "Median": df["CO2_ppm"].median(),
        "Std Dev": round(df["CO2_ppm"].std(), 2),
    }

    for k, v in co2_stats.items():
        print(f"  {k:<15}: {v}")

    # Push ke DB
    df.to_sql("co2_daily", engine, if_exists="replace", index=False)
    print("✔ Data CO2 dipush ke database.")

def analyze_sales():
    print("\n=== Analisis Rasio Kinerja Produk ===")
    data = {
        "Category": ["Bikes", "Components", "Clothing", "Safety", "Accessories"],
        "Total Sales": [1_200_000_000, 800_000_000, 600_000_000, 450_000_000, 300_000_000],
        "Units Sold": [3000, 4000, 6000, 3000, 5000],
    }
    df = pd.DataFrame(data)
    df["Avg Unit Price"] = df["Total Sales"] / df["Units Sold"]
    print(df)

    max_cat = df.loc[df["Total Sales"].idxmax()]
    min_price_cat = df.loc[df["Avg Unit Price"].idxmin()]
    print(f"\nKategori dengan Total Penjualan Tertinggi: {max_cat['Category']} (Rp {int(max_cat['Total Sales'])})")
    print(f"Kategori dengan Harga Jual Rata-rata Terendah: {min_price_cat['Category']} (Rp {int(min_price_cat['Avg Unit Price'])})")

    # Push ke DB
    df.to_sql("sales_category", engine, if_exists="replace", index=False)
    print("✔ Data sales dipush ke database.")  

def analyze_engagement_txt(txt_path):
    print("\n=== Analisis Engagement Komentar dari CSV (hasil konversi TXT) ===")
    try:
        with open(txt_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        engagements = []
        current_postid = None

        for line in lines:
            line = line.strip()

            if line.startswith("PostID:"):
                current_postid = line.split(":", 1)[1].strip()

            elif "@" in line and current_postid:
                match = re.search(r"@(\w+)", line)
                if match:
                    username = match.group(1)
                    engagements.append({"PostID": current_postid, "Username": username})


        if not engagements:
            print("Tidak ada data engagement yang ditemukan.")
            return

        df = pd.DataFrame(engagements)
        engagement_summary = df.groupby("Username").size().reset_index(name="Jumlah Komentar")
        print(engagement_summary)

        # Push ke DB
        engagement_summary.to_sql("txt_engagement", engine, if_exists="replace", index=False)
        print("✔ Data engagement berhasil dipush ke database.")

        # Visualisasi
        plt.figure(figsize=(10, 5))
        plt.bar(engagement_summary["Username"], engagement_summary["Jumlah Komentar"])
        plt.xticks(rotation=45)
        plt.title("Jumlah Komentar per Pengguna")
        plt.xlabel("Username")
        plt.ylabel("Jumlah Komentar")
        plt.tight_layout()
        plt.show()

    except Exception as e:
        print(f"Gagal membaca file: {txt_path}. Error: {e}")

if __name__ == "__main__":
    co2_path = "processed/Data Co2.csv"  # Memperbarui nama file di path
    engagement_txt_path = "processed/dokumentasi interaksi pengguna di media sosial.csv"

    if os.path.exists(co2_path):
        analyze_co2(co2_path)
    else:
        print(f"File {co2_path} tidak ditemukan.")

    analyze_sales()

    if os.path.exists(engagement_txt_path):
        analyze_engagement_txt(engagement_txt_path)
    else:
        print(f"File {engagement_txt_path} tidak ditemukan.")

