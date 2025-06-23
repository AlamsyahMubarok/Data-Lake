import psycopg2
from datetime import datetime

def get_or_create_date(cur, date_value):
    try:
        # Memasukkan data tanggal ke dalam dim_date jika belum ada
        cur.execute("""
            INSERT INTO dim_date (date_id, year, month, day, day_of_week)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (date_id) DO NOTHING;
        """, (
            date_value,
            date_value.year,
            date_value.month,
            date_value.day,
            date_value.strftime("%A")
        ))
        print(f"Data untuk {date_value} berhasil dimasukkan ke dim_date.")
    except Exception as e:
        print(f"Error saat memasukkan tanggal {date_value} ke dim_date: {e}")

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

# ========== 1. Engagement ==========
print("=== Mulai migrasi engagement ===")

cur_stg.execute("""
    SELECT DISTINCT "Username", "Jumlah Komentar"
    FROM txt_engagement
    WHERE "Username" IS NOT NULL AND "Jumlah Komentar" IS NOT NULL
""")
engagement_data = cur_stg.fetchall()

# Membuat tabel dim_engagement_user dan fact_engagement di Data Warehouse
cur_dw.execute("""
    CREATE TABLE IF NOT EXISTS dim_engagement_user (
        usernameid SERIAL PRIMARY KEY,
        username TEXT UNIQUE
    );
""")
cur_dw.execute("""
    CREATE TABLE IF NOT EXISTS fact_engagement (
        factid SERIAL PRIMARY KEY,
        usernameid INTEGER REFERENCES dim_engagement_user(usernameid),
        jumlah_komentar INTEGER
    );
""")

# Truncate fact_engagement
cur_dw.execute("TRUNCATE TABLE fact_engagement RESTART IDENTITY CASCADE;")

# Memasukkan data engagement ke dalam Data Warehouse
for username, jumlah_komentar in engagement_data:
    cur_dw.execute("""
        INSERT INTO dim_engagement_user (username)
        VALUES (%s)
        ON CONFLICT (username) DO NOTHING
        RETURNING usernameid;
    """, (username,))
    result = cur_dw.fetchone()
    if result:
        usernameid = result[0]
    else:
        cur_dw.execute("SELECT usernameid FROM dim_engagement_user WHERE username = %s", (username,))
        usernameid = cur_dw.fetchone()[0]

    cur_dw.execute("""
        INSERT INTO fact_engagement (usernameid, jumlah_komentar)
        VALUES (%s, %s);
    """, (usernameid, jumlah_komentar))

print("✔ Engagement data berhasil dipindahkan")

# ========== 2. CO2 ==========
print("=== Mulai migrasi CO2 ===")
cur_stg.execute("SELECT * FROM co2_daily")
co2_data = cur_stg.fetchall()

# Membuat tabel dim_date dan fact_co2 di Data Warehouse jika belum ada
cur_dw.execute("""
    CREATE TABLE IF NOT EXISTS dim_date (
        date_id DATE PRIMARY KEY,
        year INT,
        month INT,
        day INT,
        day_of_week TEXT
    );
""")
cur_dw.execute("""
    CREATE TABLE IF NOT EXISTS fact_co2 (
        co2id SERIAL PRIMARY KEY,
        date_id DATE REFERENCES dim_date(date_id),
        sensor_id TEXT,
        co2_ppm FLOAT
    );
""")

# Truncate fact_co2
cur_dw.execute("TRUNCATE TABLE fact_co2 RESTART IDENTITY CASCADE;")

# Memasukkan data CO2 ke dalam Data Warehouse
for row in co2_data:
    record_id, sensor_id, timestamp, co2_ppm = row
    try:
        print(f"Parsing timestamp: {timestamp}")  # Debugging line
        # Parsing timestamp dengan format yang sesuai
        dt = datetime.strptime(timestamp, "%m/%d/%Y %H:%M")  # Perubahan format
        date_only = dt.date()
    except Exception as e:
        print(f"Error parsing timestamp {timestamp}: {e}")
        continue  # Skip jika timestamp tidak valid

    # Memasukkan tanggal ke dalam dim_date jika belum ada
    get_or_create_date(cur_dw, date_only)

    # Menyisipkan data ke dalam fact_co2 setelah memastikan dim_date ada
    cur_dw.execute("""
        INSERT INTO fact_co2 (date_id, sensor_id, co2_ppm)
        VALUES (%s, %s, %s);
    """, (date_only, sensor_id, co2_ppm))

print("✔ CO2 data berhasil dipindahkan")

# ========== 3. Sales ==========
print("=== Mulai migrasi Sales ===")
cur_stg.execute("SELECT * FROM sales_category")
sales_data = cur_stg.fetchall()

# Membuat tabel dim_product_category dan fact_sales di Data Warehouse jika belum ada
cur_dw.execute("""
    CREATE TABLE IF NOT EXISTS dim_product_category (
        category_id SERIAL PRIMARY KEY,
        category_name TEXT UNIQUE
    );
""")
cur_dw.execute("""
    CREATE TABLE IF NOT EXISTS fact_sales (
        sales_id SERIAL PRIMARY KEY,
        category_id INT REFERENCES dim_product_category(category_id),
        total_sales BIGINT,
        units_sold INT,
        avg_unit_price FLOAT
    );
""")

# Truncate fact_sales
cur_dw.execute("TRUNCATE TABLE fact_sales RESTART IDENTITY CASCADE;")

# Memasukkan data penjualan ke dalam Data Warehouse
for row in sales_data:
    category, total_sales, units_sold, avg_unit_price = row

    cur_dw.execute("""
        INSERT INTO dim_product_category (category_name)
        VALUES (%s)
        ON CONFLICT (category_name) DO NOTHING
        RETURNING category_id;
    """, (category,))
    result = cur_dw.fetchone()
    if result:
        category_id = result[0]
    else:
        cur_dw.execute("SELECT category_id FROM dim_product_category WHERE category_name = %s", (category,))
        category_id = cur_dw.fetchone()[0]

    cur_dw.execute("""
        INSERT INTO fact_sales (category_id, total_sales, units_sold, avg_unit_price)
        VALUES (%s, %s, %s, %s);
    """, (category_id, total_sales, units_sold, avg_unit_price))

print("✔ Sales data berhasil dipindahkan")

# Commit dan tutup koneksi
conn_dw.commit()
cur_stg.close()
conn_stg.close()
cur_dw.close()
conn_dw.close()

print("✅ Semua data berhasil dimigrasikan ke Data Warehouse.")
