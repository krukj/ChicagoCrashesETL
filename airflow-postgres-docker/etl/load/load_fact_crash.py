import os
import pandas as pd
import psycopg2.extras
from airflow.providers.postgres.hooks.postgres import PostgresHook
from etl.logging_config import setup_logger

logger = setup_logger(__name__)
module_tag = "[fact_crash]"

def validate_foreign_keys(fact_crash_df, cursor):
    """
    Validates foreign key constraints before insertion
    """
    logger.info(f"{module_tag} Validating foreign key constraints...")
    
    # Check vehicle_key
    cursor.execute("SELECT vehicle_code FROM staging.dim_vehicle")
    valid_vehicle_keys = set(row[0] for row in cursor.fetchall())
    
    invalid_vehicle_keys = set(fact_crash_df['VEHICLE_ID'].unique()) - valid_vehicle_keys
    if invalid_vehicle_keys:
        logger.warning(f"{module_tag} Found {len(invalid_vehicle_keys)} invalid vehicle keys")
        logger.warning(f"{module_tag} Sample invalid keys: {list(invalid_vehicle_keys)[:10]}")
        
        # Remove or replace invalid keys
        logger.info(f"{module_tag} Removing rows with invalid vehicle keys...")
        initial_count = len(fact_crash_df)
        fact_crash_df = fact_crash_df[fact_crash_df['VEHICLE_ID'].isin(valid_vehicle_keys)]
        final_count = len(fact_crash_df)
        logger.info(f"{module_tag} Removed {initial_count - final_count} rows with invalid vehicle keys")
    
    # Check person_id
    cursor.execute("SELECT person_id FROM staging.dim_person")
    valid_person_ids = set(row[0] for row in cursor.fetchall())
    
    invalid_person_ids = set(fact_crash_df['PERSON_ID'].unique()) - valid_person_ids
    if invalid_person_ids:
        logger.warning(f"{module_tag} Found {len(invalid_person_ids)} invalid person IDs")
        logger.warning(f"{module_tag} Sample invalid IDs: {list(invalid_person_ids)[:10]}")
        
        # Remove rows with invalid person IDs
        initial_count = len(fact_crash_df)
        fact_crash_df = fact_crash_df[fact_crash_df['PERSON_ID'].isin(valid_person_ids)]
        final_count = len(fact_crash_df)
        logger.info(f"{module_tag} Removed {initial_count - final_count} rows with invalid person IDs")
    
    # Check crash_info_key
    cursor.execute("SELECT crash_info_key FROM staging.dim_crash_info")
    valid_crash_info_keys = set(row[0] for row in cursor.fetchall())
    
    invalid_crash_info_keys = set(fact_crash_df['CRASH_INFO_KEY'].unique()) - valid_crash_info_keys
    if invalid_crash_info_keys:
        logger.warning(f"{module_tag} Found {len(invalid_crash_info_keys)} invalid crash info keys")
        
        initial_count = len(fact_crash_df)
        fact_crash_df = fact_crash_df[fact_crash_df['CRASH_INFO_KEY'].isin(valid_crash_info_keys)]
        final_count = len(fact_crash_df)
        logger.info(f"{module_tag} Removed {initial_count - final_count} rows with invalid crash info keys")
    
    # Check location_key  
    cursor.execute("SELECT location_key FROM staging.dim_location")
    valid_location_keys = set(row[0] for row in cursor.fetchall())
    
    invalid_location_keys = set(fact_crash_df['LOCATION_KEY'].unique()) - valid_location_keys
    if invalid_location_keys:
        logger.warning(f"{module_tag} Found {len(invalid_location_keys)} invalid location keys")
        
        initial_count = len(fact_crash_df)
        fact_crash_df = fact_crash_df[fact_crash_df['LOCATION_KEY'].isin(valid_location_keys)]
        final_count = len(fact_crash_df)
        logger.info(f"{module_tag} Removed {initial_count - final_count} rows with invalid location keys")
    
    # Check date_id
    cursor.execute("SELECT date_id FROM staging.dim_date")
    valid_date_ids = set(row[0] for row in cursor.fetchall())
    
    invalid_date_ids = set(fact_crash_df['date_id'].unique()) - valid_date_ids
    if invalid_date_ids:
        logger.warning(f"{module_tag} Found {len(invalid_date_ids)} invalid date IDs")
        
        initial_count = len(fact_crash_df)
        fact_crash_df = fact_crash_df[fact_crash_df['date_id'].isin(valid_date_ids)]
        final_count = len(fact_crash_df)
        logger.info(f"{module_tag} Removed {initial_count - final_count} rows with invalid date IDs")
    
    return fact_crash_df

def load_fact_crash(filepath_in) -> None:
    try:
        logger.info(f"{module_tag} Loading fact_crash from pickle: {filepath_in}")
        fact_crash = pd.read_pickle(filepath_in)

        # Remove duplicates
        dups = fact_crash.duplicated(subset=["FACT_CRASH_KEY"]).sum()
        logger.info(f"{module_tag} Found {dups} duplicate FACT_CRASH_KEY records. Removing duplicates.")
        fact_crash = fact_crash.drop_duplicates(subset=["FACT_CRASH_KEY"])

        ################3
        float_columns = ['VEHICLE_ID', 'CRASH_INFO_KEY', 'LOCATION_KEY', 'FACT_CRASH_KEY']
        for col in float_columns:
            if col in fact_crash.columns:
                print(f"Column {col}:")
                print(f"  dtype: {fact_crash[col].dtype}")
                print(f"  NaN count: {fact_crash[col].isna().sum()}")
                print(f"  Non-integer values: {fact_crash[col][fact_crash[col] != fact_crash[col].round()].head()}")
                print(f"  Sample values: {fact_crash[col].head()}")
        ########################3
        
        # Convert float keys to int to avoid .0 in the data
        float_columns = ['VEHICLE_ID']
        for col in float_columns:
            if col in fact_crash.columns:
                fact_crash[col] = fact_crash[col].astype('int64')
        
        logger.info(f"{module_tag} Connecting to Postgres.")
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS core;")

        # Validate foreign keys before creating table
        # fact_crash = validate_foreign_keys(fact_crash, cursor)
        
        if len(fact_crash) == 0:
            logger.error(f"{module_tag} No valid records left after foreign key validation!")
            raise ValueError("No valid records to insert")

        logger.info(f"{module_tag} Creating staging.fact_crash table.")
        cursor.execute("""
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
                -- vehicle_key NUMERIC NOT NULL,
                FOREIGN KEY (date_id) REFERENCES staging.dim_date(date_id),
                FOREIGN KEY (crash_info_key) REFERENCES staging.dim_crash_info(crash_info_key),
                FOREIGN KEY (location_key) REFERENCES staging.dim_location(location_key),
                FOREIGN KEY (person_id) REFERENCES staging.dim_person(person_id),
                -- FOREIGN KEY (vehicle_key) REFERENCES staging.dim_vehicle(vehicle_key)
            );
        """)
        cursor.execute("TRUNCATE staging.fact_crash CASCADE;")

        # Bulk insert into staging
        logger.info(f"{module_tag} Bulk inserting {len(fact_crash)} records into staging.fact_crash.")
        
        # Check what columns actually exist in the DataFrame
        available_columns = fact_crash.columns.tolist()
        logger.info(f"{module_tag} Available columns: {available_columns}")
        
        # Use the correct column names based on what's actually in the DataFrame
        vehicle_col = "VEHICLE_KEY" if "VEHICLE_KEY" in available_columns else "VEHICLE_ID"
        person_col = "PERSON_ID" if "PERSON_ID" in available_columns else "PERSON_KEY"
        
        records = fact_crash[[
            "FACT_CRASH_KEY", "CRASH_RECORD_ID", "NUM_UNITS", "INJURIES_TOTAL",
            "INJURIES_FATAL", "INJURIES_INCAPACITATING", "INJURIES_NON_INCAPACITATING",
            "INJURIES_REPORTED_NOT_EVIDENT", "INJURIES_NO_INDICATION", "INJURIES_UNKNOWN",
            "date_id", "CRASH_INFO_KEY", "LOCATION_KEY", person_col, vehicle_col
        ]].to_records(index=False).tolist()

        insert_sql = """
            INSERT INTO staging.fact_crash (
                fact_crash_key, crash_record_id, num_units, injuries_total, injuries_fatal,
                injuries_incapacitating, injuries_non_incapacitating, injuries_reported_not_evident,
                injuries_no_indication, injuries_unknown, date_id, crash_info_key,
                location_key, person_id, vehicle_key
            ) VALUES %s
        """
        psycopg2.extras.execute_values(
            cursor, insert_sql, records, page_size=1000
        )
        conn.commit()

        # Create & truncate core.fact_crash
        logger.info(f"{module_tag} Creating core.fact_crash table.")
        cursor.execute("""
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
                vehicle_key NUMERIC NOT NULL,
                FOREIGN KEY (date_id) REFERENCES core.dim_date(date_id),
                FOREIGN KEY (crash_info_key) REFERENCES core.dim_crash_info(crash_info_key),
                FOREIGN KEY (location_key) REFERENCES core.dim_location(location_key),
                FOREIGN KEY (person_id) REFERENCES core.dim_person(person_id),
                FOREIGN KEY (vehicle_key) REFERENCES core.dim_vehicle(vehicle_key)
            );
        """)
        cursor.execute("TRUNCATE core.fact_crash CASCADE;")

        # Copy staging -> core
        logger.info(f"{module_tag} Copying data from staging to core.")
        cursor.execute("""
            INSERT INTO core.fact_crash
            SELECT * FROM staging.fact_crash;
        """)
        conn.commit()

        cursor.close()
        conn.close()
        logger.info(f"{module_tag} Successfully loaded {len(fact_crash)} records into fact_crash tables.")

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