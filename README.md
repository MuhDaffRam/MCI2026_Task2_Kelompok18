# MCI2026_Task2_Kelompok18

## Arsitektur Sistem

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
|order_pipeline_dag.py| ```schedule_interval="*/10 * * * *":``` Berfungsi untuk memicu (trigger) pipeline data agar berjalan otomatis setiap 10 menit sekali non-stop selama 24 jam.<br>```catchup=False:``` Memastikan Airflow hanya memproses data waktu saat ini (real-time) dan tidak memproses ulang jadwal masa lalu yang tertinggal sejak tahun 2024.<br>```max_active_runs=1:```Membatasi agar hanya ada 1 proses pipeline yang berjalan dalam satu waktu. Jika proses menit ke-10 belum selesai, proses menit ke-20 akan mengantre (mencegah tabrakan data).<br>```python /opt/airflow/dags/scripts/fetch_orders.py``` Skrip ini bertugas menembak API <http://96.9.212.102:8000/orders>, melakukan flattening (membongkar array produk dari format nested JSON), lalu mengonversinya menjadi file biner Parquet untuk disimpan ke dalam folder penyimpanan lokal (Data Lake) di direktori /opt/airflow/data_lake/orders/.<br>```process_top_products_spark (BashOperator)``` Fungsinya: Memerintahkan sistem untuk mengeksekusi skrip pemrosesan berat |
|fetch_orders.py| ... |
|process_orders_spark.py| ... |
|create_database.sql|---|
|create_database.sql| ... | 
|create_table.sql| ... |
|metabase_queries| ... |
|docker-compose.yml| ... |

### Step 3 : Pemodelan Data via Click House

- screenshot

penjelasan

### Step 4 : Membuat Visualisasi dan Question di Metabase

-- screenshot
penjelasan

### Step 5 : Membangun Dashboard Metabase

-- screenshot
penjelasan


