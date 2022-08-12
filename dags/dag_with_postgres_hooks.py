import csv
import logging
from datetime import datetime, timedelta
from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {
    'owner': 'jaykay',
    'retry': 5,
    'retry_delay': timedelta(minutes=10)
}

def postgres_to_s3():
    # Step 1: query data from postgres db and save as a text file.
    hook = PostgresHook(postgres_conn_id = "postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("select * from orders where date <= '20220501'")
    with open("dags/get_orders.txt", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerow(cursor)

    cursor.close()
    conn.close()
    logging.info("Saved Orders Data in Text file get_orders.txt.")
    # Step 2: upload text file to s3

with DAG(
    default_args = default_args,
    dag_id = "dag_with_postgres_hooks",
    start_date = datetime(2021, 11, 1),
    schedule_interval = '0 0 * * *'
) as dag:
    task1 = PythonOperator(
        task_id = "postgres_to_s3"
        python_callable = postgres_to_s3
    )