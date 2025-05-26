import pandas as pd
import os
import psycopg2.extras
from airflow.providers.postgres.hooks.postgres import PostgresHook
from etl.logging_config import setup_logger

logger = setup_logger(__name__)
module_tag = "[dim_location]"


def load_dim_location(filepath_in) -> None:
    try:
        logger.info(f"{module_tag} Loading dim_location from pickle: {filepath_in}")
        dim_location = pd.read_pickle(filepath_in)

        logger.info(f"{module_tag} Connecting to Postgres.")
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Ensure schemas exist
        cursor.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS core;")

        # Create & truncate staging table
        logger.info(f"{module_tag} Creating staging.dim_location table.")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS staging.dim_location (
                location_key NUMERIC PRIMARY KEY,
                street_direction VARCHAR(10) NOT NULL,
                street_name VARCHAR(100) NOT NULL,
                street_no INT NOT NULL,
                latitude FLOAT NOT NULL,
                longitude FLOAT NOT NULL
            );
        """
        )
        cursor.execute("TRUNCATE staging.dim_location CASCADE;")

        # Bulk insert into staging
        logger.info(f"{module_tag} Bulk inserting into staging.dim_location.")
        records = (
            dim_location[
                [
                    "LOCATION_KEY",
                    "STREET_DIRECTION",
                    "STREET_NAME",
                    "STREET_NO",
                    "LATITUDE",
                    "LONGITUDE",
                ]
            ]
            .to_records(index=False)
            .tolist()
        )

        insert_sql = """
            INSERT INTO staging.dim_location (
                location_key, street_direction, street_name, street_no, latitude, longitude
            ) VALUES %s
        """
        psycopg2.extras.execute_values(cursor, insert_sql, records, page_size=1000)
        conn.commit()

        # Create & truncate core table
        logger.info(f"{module_tag} Creating core.dim_location table.")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS core.dim_location (
                location_key NUMERIC PRIMARY KEY,
                street_direction VARCHAR(10) NOT NULL,
                street_name VARCHAR(100) NOT NULL,
                street_no INT NOT NULL,
                latitude FLOAT NOT NULL,
                longitude FLOAT NOT NULL
            );
        """
        )
        cursor.execute("TRUNCATE core.dim_location CASCADE;")

        # Copy from staging to core
        logger.info(f"{module_tag} Copying data from staging -> core.")
        cursor.execute(
            "INSERT INTO core.dim_location SELECT * FROM staging.dim_location;"
        )
        conn.commit()

        cursor.close()
        conn.close()
        logger.info(f"{module_tag} Successfully inserted dim_location into database.")
    except Exception as e:
        logger.error(f"{module_tag} Error in load_dim_location: {e}")
        raise


def main():
    base_dir = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    filepath_in = os.path.join(
        base_dir, "data", "tmp", "transformed", "dim_location.pkl"
    )
    logger.info(f"{module_tag} Starting load_dim_location main().")
    load_dim_location(filepath_in)


if __name__ == "__main__":
    main()
