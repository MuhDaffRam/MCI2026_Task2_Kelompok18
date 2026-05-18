from datetime import datetime, timedelta
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from clickhouse_connect import get_client

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 1),
    'email_failures': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_flatten_and_load():
    url = "http://96.9.212.102:8000/orders"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Gagal mengambil data dari API: {response.status_code}")
    
    data = response.json()
    
    all_rows = []
    orders_list = data.get("orders", [])
    
    for order in orders_list:
        order_metadata = {
            "order_id": order.get("order_id"),
            "user_id": order.get("user_id"),
            "order_number": order.get("order_number"),
            "order_dow": order.get("order_dow"),
            "order_hour_of_day": order.get("order_hour_of_day"),
            "days_since_prior_order": order.get("days_since_prior_order") if order.get("days_since_prior_order") is not None else 0.0,
            "eval_set": order.get("eval_set", "")
        }
        
        products_list = order.get("products", [])
        
        for product in products_list:
            row = {
                **order_metadata, 
                "product_id": product.get("product_id"),
                "product_name": product.get("product_name", ""),
                "aisle_id": product.get("aisle_id"),
                "aisle": product.get("aisle", ""),
                "department_id": product.get("department_id"),
                "department": product.get("department", ""),
                "add_to_cart_order": product.get("add_to_cart_order"),
                "reordered": product.get("reordered")
            }
            all_rows.append(row)
            
    df = pd.DataFrame(all_rows)
    
    df = df.fillna({
        'days_since_prior_order': 0.0,
        'product_name': '',
        'aisle': '',
        'department': ''
    })
    

    client = get_client(
        host='clickhouse', 
        port=8123, 
        username='default', 
        password='your_password',
        database='orders_db'
    )
    
    client.insert_df(table='orders', df=df)
    print(f"Berhasil memproses & memindahkan {len(df)} baris produk ke ClickHouse.")

with DAG(
    'mci_flattened_orders_pipeline',
    default_args=default_args,
    description='Pipeline ETL Flattening untuk Data Orders Instacart',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    task_transform_and_load = PythonOperator(
        task_id='flatten_and_load_orders',
        python_callable=fetch_flatten_and_load,
    )

    task_transform_and_load
