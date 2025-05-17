from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
import os
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

BASE_PATH = os.path.dirname(__file__)
EXTRACT_PATH = os.path.join(BASE_PATH, 'extracted.pkl')
TRANSFORMED_PATH = os.path.join(BASE_PATH, 'transformed.pkl')
CSV_PATH = os.path.join(BASE_PATH, 'sample_data.csv')

def extract():
    df = pd.read_csv(CSV_PATH)
    df.to_pickle(EXTRACT_PATH)
    print(f"Extracted {len(df)} rows")

def transform():
    df = pd.read_pickle(EXTRACT_PATH)
    df.drop_duplicates(subset='id', inplace=True)
    df['email'] = df['email'].str.lower()
    df.to_pickle(TRANSFORMED_PATH)
    print(f"Transformed to {len(df)} unique rows.")

def load():
    df = pd.read_pickle(TRANSFORMED_PATH)
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER,
            email TEXT
        )
                   """)

    for _, row in df.iterrows():
        cursor.execute(
            "INSERT INTO users (id, name, age, email) VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
            (row['id'], row['name'], row['age'], row['email'])
        )

    conn.commit()
    cursor.close()
    conn.close()
    print(f"-----------------------------------")
    print(f"Loaded {len(df)} rows into database")
    print(f"At time {datetime.now()}")
    print(f"-----------------------------------")


with DAG(
    'csv_to_postgres_etl',
    default_args=default_args,
    description='Load CSV data to PostgreSQL every 5 minutes',
    schedule_interval='@daily',
    catchup=False,
    tags=['example', 'etl'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load
    )


    extract_task >> transform_task >> load_task
    