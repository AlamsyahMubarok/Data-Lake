import psycopg2

# Koneksi ke database staging
conn_stg = psycopg2.connect(
    host="localhost",
    database="datalake_stagging",
    user="postgres",
    password="alamsyah25"
)
cur_stg = conn_stg.cursor()

# Koneksi ke data warehouse
conn_dw = psycopg2.connect(
    host="localhost",
    database="datalake_warehouse",
    user="postgres",
    password="alamsyah25"
)
cur_dw = conn_dw.cursor()

# ===========================
# Drop Tabel di Staging
# ===========================
print("=== Menghapus tabel di database staging ===")

tables_stg = [
    "txt_engagement",  # Tabel Engagement
    "csv_statistik_co2",  # Tabel CO2
    "sales_category",  # Tabel Sales
    "pdf_rasio_kinerja",  # Tabel laporan kinerja produk
    "co2_daily",  # Tabel data CO2 harian
]

for table in tables_stg:
    cur_stg.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
    print(f"Tabel {table} berhasil dihapus dari staging.")

# ===========================
# Drop Tabel di Data Warehouse
# ===========================
print("=== Menghapus tabel di Data Warehouse ===")

tables_dw = [
    "dim_engagement_user",  # Tabel dimensi pengguna
    "fact_engagement",  # Tabel fakta keterlibatan
    "dim_date",  # Tabel dimensi tanggal
    "fact_co2",  # Tabel fakta CO2
    "dim_product_category",  # Tabel dimensi kategori produk
    "fact_sales",  # Tabel fakta penjualan
]

for table in tables_dw:
    cur_dw.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
    print(f"Tabel {table} berhasil dihapus dari Data Warehouse.")

# Commit dan tutup koneksi
conn_stg.commit()
conn_dw.commit()

cur_stg.close()
conn_stg.close()
cur_dw.close()
conn_dw.close()

print("✅ Semua tabel berhasil dihapus dari staging dan Data Warehouse.")
        