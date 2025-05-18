from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import os
import requests
from datetime import datetime, timedelta 
import pandas as pd


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1) 
}

BASE_PATH = os.path.dirname(__file__)
EXTRACT_PATH = os.path.join(BASE_PATH, 'api_users.pkl')

def extract_api():
    url = 'https://jsonplaceholder.typicode.com/users'
    response = requests.get(url)
    response.raise_for_status()

    data = response.json()
    df = pd.DataFrame(data)
    df.to_pickle(EXTRACT_PATH)

    print(f"Extracted {len(df)} users from API.")

def load_to_postgres():
    df = pd.read_pickle(EXTRACT_PATH)
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE SCHEMA IF NOT EXISTS staging;
                   """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS staging.api_users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            username TEXT,
            email TEXT,
            website TEXT
        )
                   """)

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO staging.api_users (id, name, username, email, website)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
                       """, (row['id'], row['name'], row['username'], row['email'], row['website']))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Loaded {len(df)} uses to staging.api_users")


with DAG(
    'api_to_postgres_etl',
    default_args=default_args,
    description='Fetch users data from example API and load to Postgres',
    schedule_interval='@daily',
    catchup=False,
    tags=['example', 'api', 'etl']
) as dag:

    extract_task = PythonOperator(
        task_id='extract_api',
        python_callable=extract_api
    )

    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres
    )

    extract_task >> load_task