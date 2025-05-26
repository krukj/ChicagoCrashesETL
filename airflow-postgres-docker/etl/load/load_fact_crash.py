import os
import pandas as pd
import psycopg2.extras
from airflow.providers.postgres.hooks.postgres import PostgresHook
from etl.logging_config import setup_logger

logger = setup_logger(__name__)
module_tag = "[fact_crash]"


def load_fact_crash(filepath_in) -> None:
    try:
        logger.info(f"{module_tag} Loading fact_crash from pickle: {filepath_in}")
        fact_crash = pd.read_pickle(filepath_in)

        # Remove duplicates dla pewności
        dups = fact_crash.duplicated(subset=["FACT_CRASH_KEY"]).sum()
        if dups != 0:
            logger.info(
                f"{module_tag} Found {dups} duplicate FACT_CRASH_KEY records. Removing duplicates."
            )
            fact_crash = fact_crash.drop_duplicates(subset=["FACT_CRASH_KEY"])

        ################
        float_columns = [
            "VEHICLE_ID",
            "CRASH_INFO_KEY",
            "LOCATION_KEY",
            "FACT_CRASH_KEY",
        ]
        for col in float_columns:
            if col in fact_crash.columns:
                print(f"Column {col}:")
                print(f"  dtype: {fact_crash[col].dtype}")
                print(f"  NaN count: {fact_crash[col].isna().sum()}")
                print(
                    f"  Non-integer values: {fact_crash[col][fact_crash[col] != fact_crash[col].round()].head()}"
                )
                print(f"  Sample values: {fact_crash[col].head()}")
        ########################

        logger.info(f"{module_tag} Connecting to Postgres.")
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS core;")

        # Validate foreign keys before creating table
        # fact_crash = validate_foreign_keys(fact_crash, cursor)

        # if len(fact_crash) == 0:
        #     logger.error(f"{module_tag} No valid records left after foreign key validation!")
        #     raise ValueError("No valid records to insert")

        logger.info(f"{module_tag} Creating staging.fact_crash table.")
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
                vehicle_id NUMERIC NOT NULL,
                FOREIGN KEY (date_id) REFERENCES staging.dim_date(date_id),
                FOREIGN KEY (crash_info_key) REFERENCES staging.dim_crash_info(crash_info_key),
                FOREIGN KEY (location_key) REFERENCES staging.dim_location(location_key),
                FOREIGN KEY (person_id) REFERENCES staging.dim_person(person_id),
                FOREIGN KEY (vehicle_id) REFERENCES staging.dim_vehicle(vehicle_id)
            );
        """
        )
        cursor.execute("TRUNCATE staging.fact_crash CASCADE;")

        # Bulk insert into staging
        logger.info(
            f"{module_tag} Bulk inserting {len(fact_crash)} records into staging.fact_crash."
        )

        # Check what columns actually exist in the DataFrame
        available_columns = fact_crash.columns.tolist()
        logger.info(f"{module_tag} Available columns: {available_columns}")

        # Use the correct column names based on what's actually in the DataFrame
        vehicle_col = "VEHICLE_ID"
        person_col = "PERSON_ID"

        records = (
            fact_crash[
                [
                    "FACT_CRASH_KEY",
                    "CRASH_RECORD_ID",
                    "NUM_UNITS",
                    "INJURIES_TOTAL",
                    "INJURIES_FATAL",
                    "INJURIES_INCAPACITATING",
                    "INJURIES_NON_INCAPACITATING",
                    "INJURIES_REPORTED_NOT_EVIDENT",
                    "INJURIES_NO_INDICATION",
                    "INJURIES_UNKNOWN",
                    "date_id",
                    "CRASH_INFO_KEY",
                    "LOCATION_KEY",
                    person_col,
                    vehicle_col,
                ]
            ]
            .to_records(index=False)
            .tolist()
        )

        insert_sql = """
            INSERT INTO staging.fact_crash (
                fact_crash_key, crash_record_id, num_units, injuries_total, injuries_fatal,
                injuries_incapacitating, injuries_non_incapacitating, injuries_reported_not_evident,
                injuries_no_indication, injuries_unknown, date_id, crash_info_key,
                location_key, person_id, vehicle_id
            ) VALUES %s
        """
        psycopg2.extras.execute_values(cursor, insert_sql, records, page_size=100_000)
        conn.commit()

        # Create & truncate core.fact_crash
        logger.info(f"{module_tag} Creating core.fact_crash table.")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS core.fact_crash (
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
                vehicle_id NUMERIC NOT NULL,
                FOREIGN KEY (date_id) REFERENCES core.dim_date(date_id),
                FOREIGN KEY (crash_info_key) REFERENCES core.dim_crash_info(crash_info_key),
                FOREIGN KEY (location_key) REFERENCES core.dim_location(location_key),
                FOREIGN KEY (person_id) REFERENCES core.dim_person(person_id),
                FOREIGN KEY (vehicle_id) REFERENCES core.dim_vehicle(vehicle_id)
            );
        """
        )
        cursor.execute("TRUNCATE core.fact_crash CASCADE;")

        # Copy staging -> core
        logger.info(f"{module_tag} Copying data from staging to core.")
        cursor.execute(
            """
            INSERT INTO core.fact_crash
            SELECT * FROM staging.fact_crash;
        """
        )
        conn.commit()

        cursor.close()
        conn.close()
        logger.info(
            f"{module_tag} Successfully loaded {len(fact_crash)} records into fact_crash tables."
        )

    except Exception as e:
        logger.error(f"{module_tag} Error in load_fact_crash: {e}")
        raise


def main():
    base_dir = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    filepath_in = os.path.join(base_dir, "data", "tmp", "transformed", "fact_crash.pkl")
    logger.info(f"{module_tag} Starting load_fact_crash main().")
    load_fact_crash(filepath_in)


if __name__ == "__main__":
    main()
