from datetime import datetime, timedelta
from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor


default_args = {
    'owner': 'jaykay',
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args = default_args,
    dag_id = "dag_with_bucket",
    start_date = datetime(2021, 11, 1),
    schedule_interval = '0 0 * * *'
) as dag:
    task1 = S3KeySensor(
        task_id = 'sensor_minio_s3',
        bucket_name = 'airflow',
        bucket_key = 'data.csv',
        aws_conn_id = 'minio_conn',
        mode = 'poke',
        poke_interval = 30
    )
    task1