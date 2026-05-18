from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "mmds_engineer",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "orders_realtime_pipeline",
    default_args=default_args,
    # Setiap 10 menit narik snapshot baru dari API
    schedule_interval="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    description="Micro-batching Orders API → Spark → ClickHouse",
) as dag:

    fetch_step = BashOperator(
        task_id="fetch_orders",
        bash_command="python /opt/airflow/dags/scripts/fetch_orders.py",
    )

    process_step = BashOperator(
        task_id="process_top_products_spark",
        bash_command="python /opt/airflow/dags/scripts/process_orders_spark.py",
    )

    # Process baru jalan setelah fetch sukses.
    # Kalau fetch gagal, Spark ga akan baca file yang ga ada.
    fetch_step >> process_step
