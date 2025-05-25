import os
import pandas as pd
import psycopg2.extras
from airflow.providers.postgres.hooks.postgres import PostgresHook
from etl.logging_config import setup_logger

logger = setup_logger(__name__)
module_tag = "[dim_person]"


def load_dim_person(filepath_in) -> None:
    try:
        logger.info(f"{module_tag} Loading dim_person from pickle: {filepath_in}")
        dim_person = pd.read_pickle(filepath_in)

        logger.info(f"{module_tag} Connecting to Postgres.")
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cursor = conn.cursor()

        # ensure schemas
        cursor.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS core;")

        # create & truncate staging table
        logger.info(f"{module_tag} Creating staging.dim_person table.")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS staging.dim_person (
                person_id VARCHAR(150) PRIMARY KEY,
                person_type VARCHAR(150) NOT NULL,
                date_id NUMERIC NOT NULL,
                crash_record_id VARCHAR(150) NOT NULL,
                vehicle_id NUMERIC,
                crash_date DATE NOT NULL,
                sex VARCHAR(150) NOT NULL,
                age INT,
                safety_equipment VARCHAR(150) NOT NULL,
                airbag_deployed VARCHAR(150) NOT NULL,
                ejection VARCHAR(150) NOT NULL,
                injury_classification VARCHAR(150) NOT NULL,
                driver_action VARCHAR(150) NOT NULL,
                driver_vision VARCHAR(150) NOT NULL,
                physical_condition VARCHAR(150) NOT NULL,
                bac_result VARCHAR(150) NOT NULL,
                bac_result_value FLOAT,
                valid_from NUMERIC NOT NULL,
                valid_to NUMERIC NOT NULL,
                is_current BOOLEAN NOT NULL
            );
        """)
        cursor.execute("TRUNCATE staging.dim_person CASCADE;")
    

        # bulk insert via execute_values
        logger.info(f"{module_tag} Bulk inserting into staging.dim_person.")
        # replace NaNs with None
        records = (
            dim_person[[
                "PERSON_ID","PERSON_TYPE","date_id","CRASH_RECORD_ID","VEHICLE_ID",
                "CRASH_DATE","SEX","AGE","SAFETY_EQUIPMENT","AIRBAG_DEPLOYED","EJECTION",
                "INJURY_CLASSIFICATION","DRIVER_ACTION","DRIVER_VISION","PHYSICAL_CONDITION",
                "BAC_RESULT","BAC_RESULT VALUE","VALID_FROM","VALID_TO","IS_CURRENT"
            ]]
            .where(pd.notnull(dim_person), None)
            .to_records(index=False)
            .tolist()
        )

        insert_sql = """
            INSERT INTO staging.dim_person (
                person_id, person_type, date_id, crash_record_id, vehicle_id, crash_date,
                sex, age, safety_equipment, airbag_deployed, ejection, injury_classification,
                driver_action, driver_vision, physical_condition, bac_result, bac_result_value,
                valid_from, valid_to, is_current
            ) VALUES %s
        """
        psycopg2.extras.execute_values(
            cursor, insert_sql, records, page_size=1000
        )
        conn.commit()

        # create & truncate core table
        logger.info(f"{module_tag} Creating core.dim_person table.")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS core.dim_person (
                person_id VARCHAR(150) PRIMARY KEY,
                person_type VARCHAR(150) NOT NULL,
                date_id NUMERIC NOT NULL,
                crash_record_id VARCHAR(150) NOT NULL,
                vehicle_id NUMERIC,
                crash_date DATE NOT NULL,
                sex VARCHAR(150) NOT NULL,
                age INT,
                safety_equipment VARCHAR(150) NOT NULL,
                airbag_deployed VARCHAR(150) NOT NULL,
                ejection VARCHAR(150) NOT NULL,
                injury_classification VARCHAR(150) NOT NULL,
                driver_action VARCHAR(150) NOT NULL,
                driver_vision VARCHAR(150) NOT NULL,
                physical_condition VARCHAR(150) NOT NULL,
                bac_result VARCHAR(150) NOT NULL,
                bac_result_value FLOAT,
                valid_from NUMERIC NOT NULL,
                valid_to NUMERIC NOT NULL,
                is_current BOOLEAN NOT NULL
            );
        """)
        cursor.execute("TRUNCATE core.dim_person CASCADE;")

        logger.info(f"{module_tag} Copying data staging -> core.")
        cursor.execute("""
            INSERT INTO core.dim_person
            SELECT * FROM staging.dim_person;
        """)
        conn.commit()

        cursor.close()
        conn.close()
        logger.info(f"{module_tag} Successfully inserted dim_person into database.")

    except Exception as e:
        logger.error(f"{module_tag} Error in load_dim_person: {e}")
        raise


def main():
    base_dir = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    filepath_in = os.path.join(base_dir, "data", "tmp", "transformed", "dim_people.pkl")
    logger.info(f"{module_tag} Starting load_dim_person main().")
    load_dim_person(filepath_in)


if __name__ == "__main__":
    main()