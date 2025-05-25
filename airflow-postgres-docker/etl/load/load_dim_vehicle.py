import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from etl.logging_config import setup_logger

logger = setup_logger(__name__)
module_tag = "[dim_vehicle]"


def load_dim_vehicle(filepath_in) -> None:
    try:
        logger.info(f"{module_tag} Loading dim_vehicle from pickle.")
        dim_vehicle = pd.read_pickle(filepath_in)

        logger.info(f"{module_tag} Connecting to Postgres.")
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS core;")

        logger.info(f"{module_tag} Dropping old tables.")
        # cursor.execute("DROP TABLE IF EXISTS staging.dim_vehicle;")
        # cursor.execute("DROP TABLE IF EXISTS core.dim_vehicle;")

        logger.info(f"{module_tag} Creating staging table.")
        cursor.execute(
            """
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
            """
        )

        cursor.execute("TRUNCATE staging.dim_vehicle;")

        logger.info(f"{module_tag} Inserting data into staging.")
        for _, row in dim_vehicle.iterrows():
            cursor.execute(
                """
                INSERT INTO staging.dim_vehicle (
                    vehicle_key, crash_unit_id, crash_record_id, unit_type, num_passengers, vehicle_code,
                    make, model, vehicle_year, vehicle_defect, vehicle_type, vehicle_use,
                    travel_direction, maneuver, occupant_cnt, first_contact_point
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row["VEHICLE_KEY"],
                    row["CRASH_UNIT_ID"],
                    row["CRASH_RECORD_ID"],
                    row["UNIT_TYPE"],
                    row["NUM_PASSENGERS"],
                    row["VEHICLE_ID"],
                    row["MAKE"],
                    row["MODEL"],
                    row["VEHICLE_YEAR"],
                    row["VEHICLE_DEFECT"],
                    row["VEHICLE_TYPE"],
                    row["VEHICLE_USE"],
                    row["TRAVEL_DIRECTION"],
                    row["MANEUVER"],
                    row["OCCUPANT_CNT"],
                    row["FIRST_CONTACT_POINT"],
                ),
            )

        conn.commit()

        logger.info(f"{module_tag} Creating core table.")
        cursor.execute(
            """
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
            """
        )

        cursor.execute("TRUNCATE core.dim_vehicle;")

        logger.info(f"{module_tag} Copying data from staging to core.")
        cursor.execute(
            """
            INSERT INTO core.dim_vehicle (
                vehicle_key, crash_unit_id, crash_record_id, unit_type, num_passengers, vehicle_code,
                make, model, vehicle_year, vehicle_defect, vehicle_type, vehicle_use,
                travel_direction, maneuver, occupant_cnt, first_contact_point
            )
            SELECT 
                vehicle_key, crash_unit_id, crash_record_id, unit_type, num_passengers, vehicle_code,
                make, model, vehicle_year, vehicle_defect, vehicle_type, vehicle_use,
                travel_direction, maneuver, occupant_cnt, first_contact_point
            FROM staging.dim_vehicle;
            """
        )

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"{module_tag} Successfully inserted dim_vehicle into database.")

    except Exception as e:
        logger.error(f"{module_tag} Error in load_dim_vehicle: {str(e)}")
        raise
