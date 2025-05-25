import pandas as pd
import os

from airflow.providers.postgres.hooks.postgres import PostgresHook
from etl.logging_config import setup_logger

logger = setup_logger(__name__)
module_tag = "[dim_location]"


def load_dim_location(filepath_in) -> None:
    try:
        logger.info(f"{module_tag} Starting to load dim_location from pickle.")
        dim_location = pd.read_pickle(filepath_in)

        logger.info(f"{module_tag} Creating postgres connection.")
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS core;")

        logger.info(f"{module_tag} Dropping old tables (if exist).")
        # cursor.execute("DROP TABLE IF EXISTS staging.dim_location;")
        # cursor.execute("DROP TABLE IF EXISTS core.dim_location;")

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

        cursor.execute("TRUNCATE staging.dim_location;")

        logger.info(f"{module_tag} Inserting data into staging.dim_location.")
        for _, row in dim_location.iterrows():
            cursor.execute(
                """
                INSERT INTO staging.dim_location (
                    location_key, street_direction, street_name, street_no, latitude, longitude
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    int(row["LOCATION_KEY"]),
                    row["STREET_DIRECTION"],
                    row["STREET_NAME"],
                    int(row["STREET_NO"]),
                    float(row["LATITUDE"]),
                    float(row["LONGITUDE"]),
                ),
            )

        conn.commit()

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

        cursor.execute("TRUNCATE core.dim_location;")

        logger.info(f"{module_tag} Copying data from staging.dim_location to core.dim_location.")
        cursor.execute(
            """
            INSERT INTO core.dim_location
            SELECT * FROM staging.dim_location;
            """
        )

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"{module_tag} Successfully inserted dim_location into database")

    except Exception as e:
        logger.error(f"{module_tag} Error in load_dim_location: {str(e)}")
        raise
