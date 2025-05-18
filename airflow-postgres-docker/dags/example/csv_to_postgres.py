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
        CREATE TABLE IF NOT EXISTS staging.users (
            id INTEGER,
            name TEXT,
            age INTEGER,
            email TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("TRUNCATE staging.users")

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO staging.users (id, name, age, email, created_at)
            VALUES (%s, %s, %s, %s, %s)
        """, (row['id'], row['name'], row['age'], row['email'], row['created_at']))
    
    conn.commit()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS core.users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER,
            email TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("TRUNCATE core.users")

    cursor.execute("""
        INSERT INTO core.users (id, name, age, email, created_at)
        SELECT id, name, age, email, created_at FROM staging.users
    """)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Loaded {len(df)} rows from staging to core.")

def aggregate_to_analytics():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS analytics.daily_user_count (
                       date DATE PRIMARY KEY,
                       user_count INTEGER
                   )
    """)

    # To średnio optymalne, ale dla tego przykładu wystarczy 😉🤠
    cursor.execute("TRUNCATE analytics.daily_user_count")

    cursor.execute("""
        INSERT INTO analytics.daily_user_count (date, user_count)
        SELECT created_at::date, COUNT(*) FROM core.users
        GROUP BY created_at::date
        """)

    conn.commit()
    cursor.close()
    conn.close()

    print("Aggregated daily user count.")


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

    aggregate_task = PythonOperator(
        task_id='aggregate_to_analytics',
        python_callable=aggregate_to_analytics
    )


    extract_task >> transform_task >> load_task >> aggregate_task
    