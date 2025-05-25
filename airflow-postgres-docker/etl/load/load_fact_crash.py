import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from etl.logging_config import setup_logger

logger = setup_logger(__name__)
module_tag = "[fact_crash]"

def load_fact_crash(filepath_in) -> None:
    try:
        logger.info(f"{module_tag} Starting to load fact_crash from pickle.")
        fact_crash = pd.read_pickle(filepath_in)

        logger.info(f"{module_tag} Creating postgres connection.")
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS core;")

        logger.info(f"{module_tag} Dropping old tables.")
        # cursor.execute("DROP TABLE IF EXISTS staging.fact_crash;")
        # cursor.execute("DROP TABLE IF EXISTS core.fact_crash;")

        logger.info(f"{module_tag} Creating table (staging).")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS staging.fact_crash (
                fact_crash_key NUMERIC PRIMARY KEY,
                crash_record_id VARCHAR(150) NOT NULL,
                num_units INT NOT NULL,
                injuries_total INT NOT NULL,
                injuries_fatal INT NOT NULL,
                injuries_incapacitating INT NOT NULL,
                injuries_non_incapacitating INT NOT NULL,
                injuries_reported_not_evident INT NOT NULL,
                injuries_no_indication INT NOT NULL,
                injuries_unknown INT NOT NULL,
                date_id INT NOT NULL,
                crash_info_key NUMERIC NOT NULL,
                location_key NUMERIC NOT NULL,
                person_id VARCHAR(150) NOT NULL,
                vehicle_key INT NOT NULL,

                CONSTRAINT fk_date FOREIGN KEY (date_id) REFERENCES staging.dim_date(date_id),
                CONSTRAINT fk_crash_info FOREIGN KEY (crash_info_key) REFERENCES staging.dim_crash_info(crash_info_key),
                CONSTRAINT fk_location FOREIGN KEY (location_key) REFERENCES staging.dim_location(location_key),
                CONSTRAINT fk_person FOREIGN KEY (person_id) REFERENCES staging.dim_person(person_id),
                CONSTRAINT fk_vehicle FOREIGN KEY (vehicle_key) REFERENCES staging.dim_vehicle(vehicle_key)
            );
            """
        )

        cursor.execute("TRUNCATE staging.fact_crash;")

        logger.info(f"{module_tag} Inserting data into staging.")
        for _, row in fact_crash.iterrows():
            cursor.execute(
                """
                INSERT INTO staging.fact_crash (
                    fact_crash_key, crash_record_id, num_units, injuries_total, injuries_fatal,
                    injuries_incapacitating, injuries_non_incapacitating, injuries_reported_not_evident,
                    injuries_no_indication, injuries_unknown, date_id, crash_info_key,
                    location_key, person_id, vehicle_key
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row["FACT_CRASH_KEY"],
                    row["CRASH_RECORD_ID"],
                    row["NUM_UNITS"],
                    row["INJURIES_TOTAL"],
                    row["INJURIES_FATAL"],
                    row["INJURIES_INCAPACITATING"],
                    row["INJURIES_NON_INCAPACITATING"],
                    row["INJURIES_REPORTED_NOT_EVIDENT"],
                    row["INJURIES_NO_INDICATION"],
                    row["INJURIES_UNKNOWN"],
                    row["date_id"],
                    row["CRASH_INFO_KEY"],
                    row["LOCATION_KEY"],
                    row["PERSON_ID"],
                    row["VEHICLE_ID"],
                ),
            )

        conn.commit()

        logger.info(f"{module_tag} Creating table (core).")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS core.fact_crash (
                fact_crash_key BIGINT PRIMARY KEY,
                crash_record_id VARCHAR(50) NOT NULL,
                num_units INT NOT NULL,
                injuries_total INT NOT NULL,
                injuries_fatal INT NOT NULL,
                injuries_incapacitating INT NOT NULL,
                injuries_non_incapacitating INT NOT NULL,
                injuries_reported_not_evident INT NOT NULL,
                injuries_no_indication INT NOT NULL,
                injuries_unknown INT NOT NULL,
                date_id INT NOT NULL,
                crash_info_key BIGINT NOT NULL,
                location_key BIGINT NOT NULL,
                person_id VARCHAR(50) NOT NULL,
                vehicle_id INT NOT NULL,

                CONSTRAINT fk_date FOREIGN KEY (date_id) REFERENCES core.dim_date(date_id),
                CONSTRAINT fk_crash_info FOREIGN KEY (crash_info_key) REFERENCES core.dim_crash_info(crash_info_key),
                CONSTRAINT fk_location FOREIGN KEY (location_key) REFERENCES core.dim_location(location_key),
                CONSTRAINT fk_person FOREIGN KEY (person_id) REFERENCES core.dim_person(person_id),
                CONSTRAINT fk_vehicle FOREIGN KEY (vehicle_id) REFERENCES core.dim_vehicle(vehicle_id)
            );
            """
        )

        cursor.execute("TRUNCATE core.fact_crash;")

        logger.info(f"{module_tag} Copying data to core.")
        cursor.execute(
            """
            INSERT INTO core.fact_crash (
                fact_crash_key, crash_record_id, num_units, injuries_total, injuries_fatal,
                injuries_incapacitating, injuries_non_incapacitating, injuries_reported_not_evident,
                injuries_no_indication, injuries_unknown, date_id, crash_info_key,
                location_key, person_id, vehicle_id
            )
            SELECT 
                fact_crash_key, crash_record_id, num_units, injuries_total, injuries_fatal,
                injuries_incapacitating, injuries_non_incapacitating, injuries_reported_not_evident,
                injuries_no_indication, injuries_unknown, date_id, crash_info_key,
                location_key, person_id, vehicle_id
            FROM staging.fact_crash;
            """
        )

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"{module_tag} Successfully inserted fact_crash into database.")

    except Exception as e:
        logger.error(f"{module_tag} Error in load_fact_crash: {str(e)}")
        raise
