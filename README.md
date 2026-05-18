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
|order_pipeline_dag.py| ... |
|| ... |
