from datetime import datetime, timedelta

from airflow import DAG 
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'jaykay',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


with DAG(
    dag_id = 'our_first_dag_v3',
    default_args = default_args,
    description = 'This is the first dag we write',
    start_date = datetime(2022, 7, 29, 2),
    schedule_interval = "@daily"
) as dag:
    task1 = BashOperator(
        task_id = 'first_task',
        bash_command = "echo Hello World, this is the first task "
    )

    task2 = BashOperator(
        task_id = 'second_task',
        bash_command = "echo hey, I am task2 and will run after task 1"
    )

    task3 =BashOperator(
        task_id = 'third_task',
        bash_command = "echo hey, i will be running after task1 at the same time with task2"
    )

# Task Dependency methods
    # Method 1
    task1.set_downstream(task2)
    task1.set_downstream(task3)
    
    # Method 2
    task1 >> task2
    task1 >> task3 

    # Method 3
    task1 >> [task2, task3]