import pandas as pd
import os

from airflow.providers.postgres.hooks.postgres import PostgresHook
from etl.logging_config import setup_logger

logger = setup_logger(__name__)
module_tag = "[dim_date]"

def load_dim_date(filepath_in) -> None:
    try:
        dim_date = pd.read_pickle(filepath_in)

        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS staging.dim_date (
                date_id INT PRIMARY KEY,
                full_date DATE NOT NULL,
                year SMALLINT NOT NULL,
                quarter SMALLINT NOT NULL,
                month SMALLINT NOT NULL,
                month_name VARCHAR(10) NOT NULL,
                day_of_month SMALLINT NOT NULL,
                day_of_week SMALLINT NOT NULL,
                day_name VARCHAR(10) NOT NULL,
                is_weekend BOOLEAN NOT NULL,
                week_of_year SMALLINT NOT NULL,
                is_holiday BOOLEAN NOT NULL DEFAULT FALSE,
                holiday_name VARCHAR(50)
            );
            """)

        cursor.execute("TRUNCATE staging.dim_date") 

        for _, row in dim_date.iterrows():
            cursor.execute("""
                INSERT INTO staging.dim_date 
                (date_id, full_date, year, quarter, month, month_name, day_of_month, day_of_week,
                day_name, is_weekend, week_of_year, is_holiday, holiday_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, 
                (row['date_id'], row['full_date'], row['year'], row['quarter'], row['month'], 
                row['month_name'], row['day_of_month'], row['day_of_week'], row['day_name'],
                row['is_weekend'], row['week_of_year'], row['is_holiday'], row['holiday_name']))

        conn.commit()

        # Teraz staging -> core
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS core.dim_date (
                date_id INT PRIMARY KEY,
                full_date DATE NOT NULL,
                year SMALLINT NOT NULL,
                quarter SMALLINT NOT NULL,
                month SMALLINT NOT NULL,
                month_name VARCHAR(10) NOT NULL,
                day_of_month SMALLINT NOT NULL,
                day_of_week SMALLINT NOT NULL,
                day_name VARCHAR(10) NOT NULL,
                is_weekend BOOLEAN NOT NULL,
                week_of_year SMALLINT NOT NULL,
                is_holiday BOOLEAN NOT NULL DEFAULT FALSE,
                holiday_name VARCHAR(50)
            );
            """) 

        cursor.execute("TRUNCATE core.dim_date") # Changed from core.users to core.dim_date

        cursor.execute("""
            INSERT INTO core.dim_date
            (date_id, full_date, year, quarter, month, month_name, day_of_month, day_of_week,
            day_name, is_weekend, week_of_year, is_holiday, holiday_name)
            SELECT 
            date_id, full_date, year, quarter, month, month_name, day_of_month, day_of_week,
            day_name, is_weekend, week_of_year, is_holiday, holiday_name
            FROM staging.dim_date
                    """)

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"{module_tag} Successfully inserted dim_date into database")
    
    except Exception as e:
        logger.error(f"{module_tag} Error in load_dim_date: {str(e)}")
        raise

    # Tutaj z tym loggerem to możesz mnie poprawić, na razie tak dałem po prostu
    # może można jakoś lepiej

    # A no i wgl może można by było dać jakieś 
    # try catch
    # może miało by to trochę sensu


def main():
    filepath_in = os.path.join(
        os.path.dirname(__file__), 
        '..', '..', '..', 'data', 'tmp', 'transformed', 'dim_date.pkl'
    )
    print(f"Loading dim_date from: {filepath_in}")
    load_dim_date(filepath_in)

    # nie działa to jak coś z tym python -m
    # nie wiem na razie ide spac


if __name__ == "__main__":
    main()