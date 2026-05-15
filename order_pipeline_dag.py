from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'MCI_Kelompok_XX',
    'start_date': datetime(2026, 5, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'orders_realtime_pipeline',
    default_args=default_args,
    schedule_interval=timedelta(minutes=10), # Micro-batch tiap 10 menit
    catchup=False
) as dag:

    # Task 1: API -> Parquet
    fetch_task = BashOperator(
        task_id='fetch_orders_to_parquet',
        bash_command='python3 /opt/airflow/dags/scripts/fetch_wikipedia_stream.py',
    )

    # Task 2: Parquet -> ClickHouse
    process_task = BashOperator(
        task_id='process_to_clickhouse',
        bash_command='python3 /opt/airflow/dags/scripts/process_wikipedia_spark.py',
    )

    fetch_task >> process_task
