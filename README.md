# MCI2026_Task2_Kelompok18

## Arsitektur Sistem

```
API Orders
     ↓
Apache Airflow (Orchestrator)
     ↓
Fetch & Flatten Data (Python)
     ↓
Raw Data Layer (ClickHouse: orders_db.orders)
     ↓
Data Lake (Parquet)
     ↓
Apache Spark Processing
     ↓
Analytics Layer (ClickHouse: analytics.orders_top_products)
     ↓
Metabase Visualization & Dashboard
```

## Struktur Proyek
```
MCI2026_Task2_Kelompok18/
├── dags/
│   ├── scripts/
│   │   ├── fetch_orders.py
│   │   └── process_orders_spark.py
│   └── orders_pipeline.py
├── sql/
│   ├── create_database.sql
│   ├── create_table.sql
│   └── metabase_queries.sql
├── screenshots/
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Step by Step

---

### Step 1 : Membuat Struktur Proyek

Membuat folder utama

```
mkdir MCI2026_Task2_Kelompok18
cd MCI2026_Task2_Kelompok18
```

Membuat sub-folder dag dan sql (DDL)

```
mkdir -p dags/scripts sql
```

Membuat file di dags/scripts/

```
touch fetch_orders.py
touch process_orders_spark.py
```

Membuat file di sql/

```
touch create_database.sql
touch create_table.sql
touch metabase_queries.sql
```

---

### Step 2 : Isi File dan Konfigurasi Kode

|File|Fungsi|
|---|---|
|order_pipeline_dag.py| • ```schedule_interval="*/10 * * * *":``` Berfungsi untuk memicu (trigger) pipeline data agar berjalan otomatis setiap 10 menit sekali non-stop selama 24 jam.<br>• ```catchup=False:``` Memastikan Airflow hanya memproses data waktu saat ini (real-time) dan tidak memproses ulang jadwal masa lalu yang tertinggal sejak tahun 2024.<br>• ```max_active_runs=1:``` Membatasi agar hanya ada 1 proses pipeline yang berjalan dalam satu waktu. Jika proses menit ke-10 belum selesai, proses menit ke-20 akan mengantre (mencegah tabrakan data).<br>• ```python /opt/airflow/dags/scripts/fetch_orders.py``` Skrip ini bertugas menembak API <http://96.9.212.102:8000/orders>, melakukan flattening (membongkar array produk dari format nested JSON), lalu mengonversinya menjadi file biner Parquet untuk disimpan ke dalam folder penyimpanan lokal (Data Lake) di direktori /opt/airflow/data_lake/orders/.<br>• ```process_top_products_spark(BashOperator)``` Fungsinya: Memerintahkan sistem untuk mengeksekusi skrip pemrosesan berat<br>• ```fetch_step >> process_step``` Fungsinya: Menegaskan ketergantungan antar-proses (dependency). Task Spark (process_step) hanya boleh berjalan jika proses penarikan data API (fetch_step) berstatus sukses (Success). Jika internet terputus atau API down, seluruh alur akan otomatis berhenti demi menjaga akurasi data di ClickHouse. |
|fetch_orders.py| mengambil data mentah yang rumit dan bertingkat (nested) dari API, membongkarnya menjadi tabel datar yang rapi, lalu mengamankannya ke dalam media penyimpanan sementara (Data Lake) dalam format Parquet. 
|clickhouse-queries.sql| • Membuktikan bahwa mesin komputasi Spark berhasil melakukan kalkulasi analitik (agregasi total pesanan, jumlah beli kembali, dan jumlah pengguna unik), lalu memuat ringkasannya ke dalam database ClickHouse.<br>• Membuktikan bahwa skrip penarik data (fetch_orders.py) berhasil melakukan ekstraksi dari API dan meratakan struktur data yang tadinya bertingkat (nested JSON) menjadi tabel data mentah yang komprehensif.|
|process_orders_spark.py| memasak data tersebut menggunakan engine Apache Spark, menghitung metrik analitiknya secara menyeluruh, menyimpannya ke database analitik ClickHouse, dan melakukan pembersihan (housekeeping) terhadap Data Lake. |
|create_database.sql| • Mendefinisikan struktur penyimpanan tabel data mentah komprehensif (orders_db.orders) hasil proses flattening.<br>• Menyediakan 15 kolom analitik lengkap untuk menangkap granularitas data hingga level 1 row = 1 product line item.<br>• Dioptimalkan menggunakan ```ENGINE = MergeTree()``` dan diindeks berdasarkan ```ORDER BY (order_id, product_id)``` untuk menjamin kecepatan query multi-dimensi di Metabase. |
|create_table.sql| menyediakan cetak biru (blueprint) tempat penyimpanan data mentah yang sudah diratakan (flattened raw data) di ClickHouse. Jika dianalogikan, file ini bertugas untuk membangun sebuah "lemari arsip raksasa berkecepatan tinggi" bernama orders_db.orders yang memiliki 15 laci spesifik (kolom). |
|metabase_queries.sql| Inti fungsi dari file kumpulan query SQL di atas adalah sebagai mesin penggerak analitik (analisis data bisnis) yang akan digunakan di Metabase untuk menghasilkan metrik dan grafik pada Dashboard |
|docker-compose.yml| sebagai blueprint infrastruktur pintar (Infrastructure as Code) yang bertugas otomatis menyediakan, mengonfigurasi, dan menyalakan seluruh ekosistem database analitik (ClickHouse) beserta visualisasinya (Metabase) secara instan dan terisolasi dalam satu komputer. |
|requirements.txt| sebagai daftar manifes belanjaan library (dependencies) Python beserta versi spesifiknya yang wajib diinstal agar seluruh skrip pipeline kelompokmu `fetch_orders.py` dan `process_orders_spark.py` bisa berjalan tanpa error di dalam container Docker Airflow |
|Dockerfile| sebagai instruksi kustom untuk merakit dan memasak sebuah sistem operasi kontainer (Docker Image) khusus Airflow yang sudah dimodifikasi. |

---

### Step 3 : Aktifkan Pipeline di Airflow

<img width="2559" height="1307" alt="image" src="https://github.com/user-attachments/assets/ff202a59-7918-42f2-a536-bc89a97522ae" />

<img width="1704" height="424" alt="image" src="https://github.com/user-attachments/assets/a04d6d6c-faf9-4a62-acd7-2f4d7082a019" />

<img width="2559" height="693" alt="image" src="https://github.com/user-attachments/assets/22f9cc26-ed74-4c6a-802e-083dd9a90f91" />

---

### Step 4 : Validasi Data via Click House

- Masuk ke Database di ClickHouse

```
docker exec -it projects-clickhouse-server-1 clickhouse-client
```

- Kita menggunakan 2 database, yaitu analytics (hasil agregasi spark) dan juga orders_db (raw dari dataset)

```
SHOW DATABASES;
```

<img width="642" height="489" alt="image" src="https://github.com/user-attachments/assets/99e32b29-7b45-4b3a-9dfd-193ac48dd75a" />

- Melihat data dari `analytics.orders_top_products`

```
USE analytics;
DESCRIBE analytics.orders_top_products;
SELECT COUNT(*) FROM analytics.orders_top_products;
SELECT * FROM analytics.orders_top_products LIMIT 5;
```

<img width="1049" height="931" alt="image" src="https://github.com/user-attachments/assets/6aa0b417-b223-44d0-be3f-071f9b8e00bd" />

- Melihat data dari `orders_db.orders`
  
```
USE orders_db;
DESCRIBE orders_db.orders;
SELECT COUNT(*) FROM orders_db.orders;
SELECT * FROM orders_db.orders LIMIT 5;
```

<img width="1121" height="504" alt="image" src="https://github.com/user-attachments/assets/d948847b-f0e1-498d-8fb3-149646f8937e" />

<img width="1970" height="748" alt="image" src="https://github.com/user-attachments/assets/592f348f-8262-4af0-826c-b467367c7855" />

Jika seluruh data dari masing-masing database tidak kosong, maka fetch data berhasil 

---

### Step 5 : Visualisasi dan Question dalam Dashboard Metabase

<img width="2559" height="1306" alt="image" src="https://github.com/user-attachments/assets/0ddac175-89bf-446a-8d04-fdc48b17b79d" />

<img width="2559" height="1313" alt="image" src="https://github.com/user-attachments/assets/c9f98889-a019-4eeb-a854-6c4f7f9cd83a" />

penjelasan


