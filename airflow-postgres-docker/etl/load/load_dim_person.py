import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from etl.logging_config import setup_logger

logger = setup_logger(__name__)
module_tag = "[dim_person]"


def load_dim_person(filepath_in) -> None:
    try:
        logger.info(f"{module_tag} Starting to load dim_person from pickle.")
        dim_person = pd.read_pickle(filepath_in)

        logger.info(f"{module_tag} Creating postgres connection.")
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS core;")

        logger.info(f"{module_tag} Dropping old tables if exist.")
        # cursor.execute("DROP TABLE IF EXISTS staging.dim_person;")
        # cursor.execute("DROP TABLE IF EXISTS core.dim_person;")

        logger.info(f"{module_tag} Creating staging.dim_person table.")
        cursor.execute(
            """
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
            """
        )

        cursor.execute("TRUNCATE staging.dim_person;")

        logger.info(f"{module_tag} Inserting data into staging.dim_person.")
        for _, row in dim_person.iterrows():
            cursor.execute(
                """
                INSERT INTO staging.dim_person (
                    person_id, person_type, date_id, crash_record_id, vehicle_id, crash_date, sex, age,
                    safety_equipment, airbag_deployed, ejection, injury_classification, driver_action,
                    driver_vision, physical_condition, bac_result, bac_result_value, valid_from, valid_to, is_current
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row["PERSON_ID"],
                    row["PERSON_TYPE"],
                    int(row["date_id"]),
                    row["CRASH_RECORD_ID"],
                    int(row["VEHICLE_ID"]) if pd.notnull(row["VEHICLE_ID"]) else None,
                    pd.to_datetime(row["CRASH_DATE"]).date(),
                    row["SEX"],
                    int(row["AGE"]) if pd.notnull(row["AGE"]) else None,
                    row["SAFETY_EQUIPMENT"],
                    row["AIRBAG_DEPLOYED"],
                    row["EJECTION"],
                    row["INJURY_CLASSIFICATION"],
                    row["DRIVER_ACTION"],
                    row["DRIVER_VISION"],
                    row["PHYSICAL_CONDITION"],
                    row["BAC_RESULT"],
                    (
                        float(row["BAC_RESULT VALUE"])
                        if pd.notnull(row["BAC_RESULT VALUE"])
                        else None
                    ),
                    int(row["VALID_FROM"]),
                    int(row["VALID_TO"]),
                    bool(row["IS_CURRENT"]),
                ),
            )

        conn.commit()

        logger.info(f"{module_tag} Creating core.dim_person table.")
        cursor.execute(
            """
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
            """
        )

        cursor.execute("TRUNCATE core.dim_person;")

        logger.info(
            f"{module_tag} Copying data from staging.dim_person to core.dim_person."
        )
        cursor.execute(
            """
            INSERT INTO core.dim_person
            SELECT * FROM staging.dim_person;
            """
        )

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"{module_tag} Successfully inserted dim_person into database")

    except Exception as e:
        logger.error(f"{module_tag} Error in load_dim_person: {str(e)}")
        raise
