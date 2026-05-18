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
|order_pipeline_dag.py| • ```schedule_interval="*/10 * * * *":``` Berfungsi untuk memicu (trigger) pipeline data agar berjalan otomatis setiap 10 menit sekali non-stop selama 24 jam.<br>• ```catchup=False:``` Memastikan Airflow hanya memproses data waktu saat ini (real-time) dan tidak memproses ulang jadwal masa lalu yang tertinggal sejak tahun 2024.<br>• ```max_active_runs=1:``` Membatasi agar hanya ada 1 proses pipeline yang berjalan dalam satu waktu. Jika proses menit ke-10 belum selesai, proses menit ke-20 akan mengantre (mencegah tabrakan data).<br>• ```python /opt/airflow/dags/scripts/fetch_orders.py``` Skrip ini bertugas menembak API <http://96.9.212.102:8000/orders>, melakukan flattening (membongkar array produk dari format nested JSON), lalu mengonversinya menjadi file biner Parquet untuk disimpan ke dalam folder penyimpanan lokal (Data Lake) di direktori /opt/airflow/data_lake/orders/.<br>• ```process_top_products_spark(BashOperator)``` Fungsinya: Memerintahkan sistem untuk mengeksekusi skrip pemrosesan berat<br>• ```fetch_step >> process_step``` Fungsinya: Menegaskan ketergantungan antar-proses (dependency). Task Spark (process_step) hanya boleh berjalan jika proses penarikan data API (fetch_step) berstatus sukses (Success). Jika internet terputus atau API down, seluruh alur akan otomatis berhenti demi menjaga akurasi data di ClickHouse. |
|fetch_orders.py| mengambil data mentah yang rumit dan bertingkat (nested) dari API, membongkarnya menjadi tabel datar yang rapi, lalu mengamankannya ke dalam media penyimpanan sementara (Data Lake) dalam format Parquet. |
|process_orders_spark.py| memasak data tersebut menggunakan engine Apache Spark, menghitung metrik analitiknya secara menyeluruh, menyimpannya ke database analitik ClickHouse, dan melakukan pembersihan (housekeeping) terhadap Data Lake. |
|create_database.sql| • Mendefinisikan struktur penyimpanan tabel data mentah komprehensif (orders_db.orders) hasil proses flattening.<br>• Menyediakan 15 kolom analitik lengkap untuk menangkap granularitas data hingga level 1 row = 1 product line item.<br>• Dioptimalkan menggunakan ```ENGINE = MergeTree()``` dan diindeks berdasarkan ```ORDER BY (order_id, product_id)``` untuk menjamin kecepatan query multi-dimensi di Metabase. |
|create_table.sql| menyediakan cetak biru (blueprint) tempat penyimpanan data mentah yang sudah diratakan (flattened raw data) di ClickHouse. Jika dianalogikan, file ini bertugas untuk membangun sebuah "lemari arsip raksasa berkecepatan tinggi" bernama orders_db.orders yang memiliki 15 laci spesifik (kolom). |
|metabase_queries| Inti fungsi dari file kumpulan query SQL di atas adalah sebagai mesin penggerak analitik (analisis data bisnis) yang akan digunakan di Metabase untuk menghasilkan metrik dan grafik pada Dashboard |
|docker-compose.yml| sebagai blueprint infrastruktur pintar (Infrastructure as Code) yang bertugas otomatis menyediakan, mengonfigurasi, dan menyalakan seluruh ekosistem database analitik (ClickHouse) beserta visualisasinya (Metabase) secara instan dan terisolasi dalam satu komputer. |

---

### Step 3 : Pemodelan Data via Click House

<img width="2559" height="1316" alt="image" src="https://github.com/user-attachments/assets/f53be258-cfd5-4c37-bf99-241c04e126f4" />

penjelasan

---

### Step 4 : Visualisasi dan Question di Metabase

-- screenshot

penjelasan

---

### Step 5 : Dashboard Metabase

-- screenshot

penjelasan


