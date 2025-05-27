from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import sys
import os
sys.path.insert(0, "/opt/airflow")

import logging

def extract_wrapper():
    logging.info(f"Current working dir: {os.getcwd()}")
    logging.info(f"sys.path = {sys.path}")
    logging.info(f"List /opt/airflow/data/weather_data: {os.listdir('/opt/airflow/data/weather_data')}")
    from etl.extract.extract import main
    main()



def transform_all_wrapper():
    from etl.transform.transform_all import main

    main()


def load_all_wrapper():
    from etl.load.load_all import main

    main()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

with DAG(
    "crash_etl_dag",
    default_args=default_args,
    description="ETL: Extract, Transoform and Load data to POSTGRESQL",
    schedule_interval="@daily",
    catchup=False,
    tags=["crash_data", "etl"],
) as dag:

    extract_task = PythonOperator(task_id="extract", python_callable=extract_wrapper)

    transform_task = PythonOperator(
        task_id="transform", python_callable=transform_all_wrapper
    )

    load_task = PythonOperator(task_id="load", python_callable=load_all_wrapper)

    logging.info("DAG - start")
    extract_task >> transform_task >> load_task
