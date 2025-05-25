import os
import pandas as pd
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

        # Ensure schemas exist
        cursor.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS core;")

        # Create & truncate staging table
        logger.info(f"{module_tag} Creating staging.dim_vehicle table.")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS staging.dim_vehicle (
                vehicle_key NUMERIC PRIMARY KEY,
                crash_unit_id INT NOT NULL,
                crash_record_id VARCHAR(150) NOT NULL,
                unit_type VARCHAR(150) NOT NULL,
                num_passengers INT,
                vehicle_code FLOAT,
                make VARCHAR(150) NOT NULL,
                model VARCHAR(150) NOT NULL,
                vehicle_year INT,
                vehicle_defect VARCHAR(150) NOT NULL,
                vehicle_type VARCHAR(150) NOT NULL,
                vehicle_use VARCHAR(150) NOT NULL,
                travel_direction VARCHAR(150) NOT NULL,
                maneuver VARCHAR(150) NOT NULL,
                occupant_cnt INT,
                first_contact_point VARCHAR(150) NOT NULL
            );
        """)
        cursor.execute("TRUNCATE staging.dim_vehicle CASCADE;")

        # Bulk insert into staging
        logger.info(f"{module_tag} Bulk inserting into staging.dim_vehicle.")
        records = dim_vehicle[[
            "VEHICLE_KEY", "CRASH_UNIT_ID", "CRASH_RECORD_ID", "UNIT_TYPE",
            "NUM_PASSENGERS", "VEHICLE_ID", "MAKE", "MODEL", "VEHICLE_YEAR",
            "VEHICLE_DEFECT", "VEHICLE_TYPE", "VEHICLE_USE", "TRAVEL_DIRECTION",
            "MANEUVER", "OCCUPANT_CNT", "FIRST_CONTACT_POINT"
        ]].to_records(index=False).tolist()

        insert_sql = """
            INSERT INTO staging.dim_vehicle (
                vehicle_key, crash_unit_id, crash_record_id, unit_type,
                num_passengers, vehicle_code, make, model, vehicle_year,
                vehicle_defect, vehicle_type, vehicle_use, travel_direction,
                maneuver, occupant_cnt, first_contact_point
            ) VALUES %s
        """
        psycopg2.extras.execute_values(
            cursor, insert_sql, records, page_size=1000
        )
        conn.commit()

        # Create & truncate core table
        logger.info(f"{module_tag} Creating core.dim_vehicle table.")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS core.dim_vehicle (
                vehicle_key NUMERIC PRIMARY KEY,
                crash_unit_id INT NOT NULL,
                crash_record_id VARCHAR(150) NOT NULL,
                unit_type VARCHAR(150) NOT NULL,
                num_passengers INT,
                vehicle_code FLOAT,
                make VARCHAR(150) NOT NULL,
                model VARCHAR(150) NOT NULL,
                vehicle_year INT,
                vehicle_defect VARCHAR(150) NOT NULL,
                vehicle_type VARCHAR(150) NOT NULL,
                vehicle_use VARCHAR(150) NOT NULL,
                travel_direction VARCHAR(150) NOT NULL,
                maneuver VARCHAR(150) NOT NULL,
                occupant_cnt INT,
                first_contact_point VARCHAR(150) NOT NULL
            );
        """)
        cursor.execute("TRUNCATE core.dim_vehicle CASCADE;")

        # Copy from staging to core
        logger.info(f"{module_tag} Copying data staging -> core.")
        cursor.execute("""
            INSERT INTO core.dim_vehicle
            SELECT * FROM staging.dim_vehicle;
        """)
        conn.commit()

        cursor.close()
        conn.close()
        logger.info(f"{module_tag} Successfully loaded dim_vehicle into database.")

    except Exception as e:
        logger.error(f"{module_tag} Error in load_dim_vehicle: {e}")
        raise


def main():
    base_dir = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    filepath_in = os.path.join(
        base_dir, "data", "tmp", "transformed", "dim_vehicle.pkl"
    )
    logger.info(f"{module_tag} Starting load_dim_vehicle main().")
    load_dim_vehicle(filepath_in)


if __name__ == "__main__":
    main()