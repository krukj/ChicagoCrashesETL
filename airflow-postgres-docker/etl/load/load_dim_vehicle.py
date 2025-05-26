import os
import pandas as pd
import numpy as np
import psycopg2.extras
from airflow.providers.postgres.hooks.postgres import PostgresHook
from etl.logging_config import setup_logger
from etl.transform.utils import generate_surrogate_key

logger = setup_logger(__name__)
module_tag = "[dim_vehicle]"


def load_dim_vehicle(filepath_in) -> None:
    try:
        logger.info(f"{module_tag} Loading dim_vehicle from pickle: {filepath_in}")
        dim_vehicle = pd.read_pickle(filepath_in)

        # --- dołączamy brakujące VEHICLE_ID z dim_people.pkl ---
        person_path = os.path.join(os.path.dirname(filepath_in), "dim_people.pkl")
        if os.path.exists(person_path):
            df_person = pd.read_pickle(person_path)
            existing = dim_vehicle["VEHICLE_ID"].dropna().unique()
            missing = df_person.loc[~df_person["VEHICLE_ID"].isin(existing)]
            if not missing.empty:
                # Upewnij się, że CRASH_UNIT_ID będzie -1 (nie NULL/NA)
                missing_veh = missing.assign(
                    VEHICLE_KEY=missing.apply(
                        lambda r: generate_surrogate_key(
                            r["CRASH_RECORD_ID"], r["PERSON_ID"]
                        ),
                        axis=1
                    ),
                    CRASH_UNIT_ID=-1,
                    CRASH_RECORD_ID="None",
                    UNIT_TYPE="None",
                    NUM_PASSENGERS=0,
                    MAKE="None",
                    MODEL="None",
                    VEHICLE_YEAR=0,
                    VEHICLE_DEFECT="None",
                    VEHICLE_TYPE="None",
                    VEHICLE_USE="None",
                    TRAVEL_DIRECTION="None",
                    MANEUVER="None",
                    OCCUPANT_CNT=0,
                    FIRST_CONTACT_POINT="None",
                )[
                    [
                        "VEHICLE_KEY", "CRASH_UNIT_ID", "CRASH_RECORD_ID",
                        "UNIT_TYPE", "NUM_PASSENGERS", "VEHICLE_ID", "MAKE",
                        "MODEL", "VEHICLE_YEAR", "VEHICLE_DEFECT",
                        "VEHICLE_TYPE", "VEHICLE_USE", "TRAVEL_DIRECTION",
                        "MANEUVER", "OCCUPANT_CNT", "FIRST_CONTACT_POINT"
                    ]
                ]
                dim_vehicle = pd.concat([dim_vehicle, missing_veh], ignore_index=True)
                logger.info(f"{module_tag} Appended {len(missing_veh)} missing VEHICLE_ID rows.")
        else:
            logger.warning(f"{module_tag} {person_path} not found, skip missing-vehicle logic.")

        logger.info(f"{module_tag} Connecting to Postgres.")
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cursor = conn.cursor()

        # schemas
        cursor.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS core;")

        # staging table
        logger.info(f"{module_tag} Creating staging.dim_vehicle table.")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS staging.dim_vehicle (
                vehicle_key NUMERIC PRIMARY KEY,
                crash_unit_id INT NOT NULL,
                crash_record_id VARCHAR(150) NOT NULL,
                unit_type VARCHAR(150),
                num_passengers INT,
                vehicle_code FLOAT,
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
        """)
        cursor.execute("TRUNCATE staging.dim_vehicle CASCADE;")

        # bulk insert staging - radykalna konwersja typów
        logger.info(f"{module_tag} Bulk inserting into staging.dim_vehicle.")
        cols = [
            "VEHICLE_KEY", "CRASH_UNIT_ID", "CRASH_RECORD_ID", "UNIT_TYPE",
            "NUM_PASSENGERS", "VEHICLE_ID", "MAKE", "MODEL", "VEHICLE_YEAR",
            "VEHICLE_DEFECT", "VEHICLE_TYPE", "VEHICLE_USE", "TRAVEL_DIRECTION",
            "MANEUVER", "OCCUPANT_CNT", "FIRST_CONTACT_POINT"
        ]
        
        # Konwersja przez to_dict żeby mieć pewność konwersji typów
        data_dict = dim_vehicle[cols].to_dict('records')
        records = []
        for row_dict in data_dict:
            converted_row = []
            for col in cols:
                val = row_dict[col]
                # Obsługa NAType, NaN, None, itp.
                if pd.isna(val) or (hasattr(val, 'dtype') and pd.api.types.is_dtype_equal(val.dtype, 'object') and val is pd.NA):
                    converted_row.append(None)
                # Konwersja typów NumPy
                elif isinstance(val, np.integer):
                    converted_row.append(int(val))
                elif isinstance(val, np.floating):
                    converted_row.append(float(val))
                # Pozostałe typy
                else:
                    converted_row.append(val)
            records.append(tuple(converted_row))

        insert_sql = """
            INSERT INTO staging.dim_vehicle (
                vehicle_key, crash_unit_id, crash_record_id, unit_type,
                num_passengers, vehicle_code, make, model, vehicle_year,
                vehicle_defect, vehicle_type, vehicle_use, travel_direction,
                maneuver, occupant_cnt, first_contact_point
            ) VALUES %s
        """
        psycopg2.extras.execute_values(cursor, insert_sql, records, page_size=1000)
        conn.commit()

        # core table
        logger.info(f"{module_tag} Creating core.dim_vehicle table.")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS core.dim_vehicle (
                vehicle_key NUMERIC PRIMARY KEY,
                crash_unit_id INT NOT NULL,
                crash_record_id VARCHAR(150) NOT NULL,
                unit_type VARCHAR(150),
                num_passengers INT,
                vehicle_code FLOAT,
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
        """)
        cursor.execute("TRUNCATE core.dim_vehicle CASCADE;")

        # copy staging → core
        logger.info(f"{module_tag} Copying data staging → core.")
        cursor.execute("INSERT INTO core.dim_vehicle SELECT * FROM staging.dim_vehicle;")
        conn.commit()

        cursor.close()
        conn.close()
        logger.info(f"{module_tag} Successfully loaded dim_vehicle into database.")

    except Exception as e:
        logger.error(f"{module_tag} Error in load_dim_vehicle: {e}")
        raise