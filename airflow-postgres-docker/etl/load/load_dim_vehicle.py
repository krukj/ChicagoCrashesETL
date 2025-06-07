import os
import pandas as pd
import numpy as np
import psycopg2.extras
from airflow.providers.postgres.hooks.postgres import PostgresHook
from etl.logging_config import setup_logger

logger = setup_logger(__name__)
module_tag = "[dim_vehicle]"


def load_dim_vehicle(filepath_in) -> None:
    try:
        logger.info(f"{module_tag} Loading dim_vehicle from pickle: {filepath_in}")
        dim_vehicle = pd.read_pickle(filepath_in)

        logger.info(f"{module_tag} Connecting to Postgres.")
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cursor = conn.cursor()

        # schemas
        cursor.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS core;")
        
        # Check if tables exist and rename column if needed
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'core' 
            AND table_name = 'dim_vehicle' 
            AND column_name = 'vehicle_code'
        """)
        if cursor.fetchone():
            logger.info(f"{module_tag} Renaming vehicle_code to vehicle_id in existing tables")
            cursor.execute("ALTER TABLE core.dim_vehicle RENAME COLUMN vehicle_code TO vehicle_id;")
            conn.commit()
            
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'staging' 
            AND table_name = 'dim_vehicle' 
            AND column_name = 'vehicle_code'
        """)
        if cursor.fetchone():
            cursor.execute("ALTER TABLE staging.dim_vehicle RENAME COLUMN vehicle_code TO vehicle_id;")
            conn.commit()

        # staging table
        logger.info(f"{module_tag} Creating staging.dim_vehicle table.")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS staging.dim_vehicle (
                vehicle_key NUMERIC PRIMARY KEY,
                crash_unit_id INT NOT NULL,
                crash_record_id VARCHAR(150) NOT NULL,
                unit_type VARCHAR(150),
                num_passengers INT,
                vehicle_id NUMERIC UNIQUE,
                make VARCHAR(150),
                model VARCHAR(150),
                vehicle_year INT,
                vehicle_defect VARCHAR(150),
                vehicle_type VARCHAR(150),
                vehicle_use VARCHAR(150),
                travel_direction VARCHAR(150),
                maneuver VARCHAR(150),
                occupant_cnt INT,
                first_contact_point VARCHAR(150)
            );
        """
        )
        cursor.execute("TRUNCATE staging.dim_vehicle CASCADE;")

        # bulk insert staging
        logger.info(f"{module_tag} Bulk inserting into staging.dim_vehicle.")
        records = (
            dim_vehicle[
                [
                    "VEHICLE_KEY",
                    "CRASH_UNIT_ID",
                    "CRASH_RECORD_ID",
                    "UNIT_TYPE",
                    "NUM_PASSENGERS",
                    "VEHICLE_ID",
                    "MAKE",
                    "MODEL",
                    "VEHICLE_YEAR",
                    "VEHICLE_DEFECT",
                    "VEHICLE_TYPE",
                    "VEHICLE_USE",
                    "TRAVEL_DIRECTION",
                    "MANEUVER",
                    "OCCUPANT_CNT",
                    "FIRST_CONTACT_POINT",
                ]
            ]
            .to_records(index=False)
            .tolist()
        )

        insert_sql = """
            INSERT INTO staging.dim_vehicle (
                vehicle_key, crash_unit_id, crash_record_id, unit_type,
                num_passengers, vehicle_id, make, model, vehicle_year,
                vehicle_defect, vehicle_type, vehicle_use, travel_direction,
                maneuver, occupant_cnt, first_contact_point
            ) VALUES %s
        """
        psycopg2.extras.execute_values(cursor, insert_sql, records, page_size=100_000)
        conn.commit()

        # core table
        logger.info(f"{module_tag} Creating core.dim_vehicle table.")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS core.dim_vehicle (
                vehicle_key NUMERIC PRIMARY KEY,
                crash_unit_id INT NOT NULL,
                crash_record_id VARCHAR(150) NOT NULL,
                unit_type VARCHAR(150),
                num_passengers INT,
                vehicle_id NUMERIC UNIQUE,
                make VARCHAR(150),
                model VARCHAR(150),
                vehicle_year INT,
                vehicle_defect VARCHAR(150),
                vehicle_type VARCHAR(150),
                vehicle_use VARCHAR(150),
                travel_direction VARCHAR(150),
                maneuver VARCHAR(150),
                occupant_cnt INT,
                first_contact_point VARCHAR(150)
            );
        """
        )
        cursor.execute("TRUNCATE core.dim_vehicle CASCADE;")

        # copy staging → core
        logger.info(f"{module_tag} Copying data staging → core.")
        cursor.execute(
            "INSERT INTO core.dim_vehicle SELECT * FROM staging.dim_vehicle;"
        )
        conn.commit()

        cursor.close()
        conn.close()
        logger.info(f"{module_tag} Successfully loaded dim_vehicle into database.")

    except Exception as e:
        logger.error(f"{module_tag} Error in load_dim_vehicle: {e}")
        raise