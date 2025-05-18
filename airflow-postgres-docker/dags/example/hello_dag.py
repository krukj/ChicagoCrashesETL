from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def say_hello():
    print("Hello, Airflow!")
    date = datetime.now()
    print(f"Current date and time: {date}")
    print("This is a simple DAG example.")

with DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2025, 5, 17),
    schedule_interval=None,
    catchup=False,
    tags=['example']
) as dag:


    hello_task = PythonOperator(
        task_id='say_hello_task',
        python_callable=say_hello
    )