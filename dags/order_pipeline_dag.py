from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from clickhouse_driver import Client
import os

DATASET_URL = "http://96.9.212.102:8000/orders"

LOCAL_DATA_LAKE = "/opt/airflow/data_lake/orders/"

CLICKHOUSE_CONFIG = {
    'host': 'localhost',
    'port': 9000,
    'user': 'default',
    'password': '',
    'database': 'mci_analytics'
}

default_args = {
    'owner': 'MCI_Kelompok_XX',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_api_to_parquet():
    """
    TAHAPAN 1: INGESTION (API -> DATA LAKE)
    Mengambil data JSON dari endpoint orders dan menyimpannya ke format Parquet.
    """
    if not os.path.exists(LOCAL_DATA_LAKE):
        os.makedirs(LOCAL_DATA_LAKE)

    print(f"Memulai pengambilan data dari: {DATASET_URL}")
    response = requests.get(DATASET_URL, timeout=30)
    response.raise_for_status() 
    
    data = response.json()
    df = pd.DataFrame(data)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"orders_{timestamp}.parquet"
    filepath = os.path.join(LOCAL_DATA_LAKE, filename)
    
    df.to_parquet(filepath, index=False)
    print(f"Berhasil mengunduh {len(df)} data. File disimpan di: {filepath}")
    
    return filepath


def load_parquet_to_clickhouse(**kwargs):
    """
    TAHAPAN 2: TRANSFORMATION & LOADING (DATA LAKE -> CLICKHOUSE)
    Membaca file Parquet terakhir, merapikan tipe data, dan mengirimkannya ke ClickHouse.
    """
    
    ti = kwargs['ti']
    filepath = ti.xcom_pull(task_ids='ingest_api_data')
    
    if not filepath:
        raise ValueError("Gagal menerima path file dari task sebelumnya.")
        
    client = Client(
        host=CLICKHOUSE_CONFIG['host'],
        port=CLICKHOUSE_CONFIG['port'], 
        user=CLICKHOUSE_CONFIG['user'], 
        password=CLICKHOUSE_CONFIG['password']
    )

    client.execute(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_CONFIG['database']}")
    
    client.execute(f"USE {CLICKHOUSE_CONFIG['database']}")

    client.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            order_id UInt64,
            customer_name String,
            product_name String,
            price Float64,
            quantity UInt32,
            order_date DateTime
        ) ENGINE = MergeTree() 
        ORDER BY (order_date, order_id)
    ''')

    print(f"Membaca file Parquet dari Data Lake: {filepath}")
    df = pd.read_parquet(filepath)

    df['order_date'] = pd.to_datetime(df['order_date'])
    
    data_to_insert = df.to_dict('records')
    
    print("Mentransfer data ke database ClickHouse...")
    client.execute(
        "INSERT INTO orders (order_id, customer_name, product_name, price, quantity, order_date) VALUES", 
        data_to_insert
    )
    
    print(f"Pipeline Sukses! Sebanyak {len(df)} baris data berhasil dimuat ke ClickHouse.")

with DAG(
    'MCI2026_Task2_Pipeline_Orders',
    default_args=default_args,
    description='Pipeline ETL Data Orders dari API ke ClickHouse berbasis Parquet',
    schedule_interval=timedelta(minutes=10), 
    catchup=False
) as dag:
    
    task_ingest = PythonOperator(
        task_id='ingest_api_data',
        python_callable=fetch_api_to_parquet
    )

    task_load = PythonOperator(
        task_id='load_to_clickhouse',
        python_callable=load_parquet_to_clickhouse
    )
    task_ingest >> task_load
