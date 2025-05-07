from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def load_csv_to_postgres():
    # Ścieżka do pliku CSV
    csv_path = os.path.join(os.path.dirname(__file__), 'sample_data.csv')
    
    # Wczytanie danych z CSV
    df = pd.read_csv(csv_path)
    
    # Połączenie z PostgreSQL
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    # Utworzenie tabeli jeśli nie istnieje
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER,
            email TEXT
        )
    """)
    
    # Wstawienie danych
    for _, row in df.iterrows():
        cursor.execute(
            "INSERT INTO users (id, name, age, email) VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
            (row['id'], row['name'], row['age'], row['email'])
        )
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Successfully loaded {len(df)} records to PostgreSQL")

with DAG(
    'csv_to_postgres',
    default_args=default_args,
    description='Load CSV data to PostgreSQL every 5 minutes',
    schedule_interval='*/5 * * * *',
    catchup=False,
    tags=['example'],
) as dag:
    
    load_task = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres,
    )

    load_task