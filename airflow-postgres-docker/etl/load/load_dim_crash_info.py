import pandas as pd
import psycopg2.extras
from airflow.providers.postgres.hooks.postgres import PostgresHook
from etl.logging_config import setup_logger

logger = setup_logger(__name__)
module_tag = "[dim_crash_info]"


def load_dim_crash_info(filepath_in) -> None:
    try:
        logger.info(f"{module_tag} Loading dim_crash_info from pickle.")
        dim_crash_info = pd.read_pickle(filepath_in)

        logger.info(f"{module_tag} Connecting to Postgres.")
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Ensure schemas exist
        cursor.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS core;")

        # Drop old tables if they exist
        logger.info(f"{module_tag} Dropping old tables.")
        cursor.execute("DROP TABLE IF EXISTS staging.dim_crash_info;")
        cursor.execute("DROP TABLE IF EXISTS core.dim_crash_info;")

        # Create staging table
        logger.info(f"{module_tag} Creating staging table.")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS staging.dim_crash_info (
                crash_info_key NUMERIC PRIMARY KEY,
                traffic_control_device VARCHAR(50) NOT NULL,
                device_condition VARCHAR(50) NOT NULL,
                first_crash_type VARCHAR(50) NOT NULL,
                trafficway_type VARCHAR(50) NOT NULL,
                alignment VARCHAR(50) NOT NULL,
                roadway_surface_cond VARCHAR(50) NOT NULL,
                road_defect VARCHAR(50) NOT NULL,
                report_type VARCHAR(50) NOT NULL,
                crash_type VARCHAR(50) NOT NULL,
                damage VARCHAR(50) NOT NULL,
                prim_contributory_cause VARCHAR(100) NOT NULL,
                sec_contributory_cause VARCHAR(100) NOT NULL,
                most_severe_injury VARCHAR(50) NOT NULL
            );
            """
        )
        cursor.execute("TRUNCATE staging.dim_crash_info CASCADE;")

        # Bulk insert into staging using execute_values
        logger.info(f"{module_tag} Inserting data into staging.")
        records = dim_crash_info[
            [
                "CRASH_INFO_KEY",
                "TRAFFIC_CONTROL_DEVICE",
                "DEVICE_CONDITION",
                "FIRST_CRASH_TYPE",
                "TRAFFICWAY_TYPE",
                "ALIGNMENT",
                "ROADWAY_SURFACE_COND",
                "ROAD_DEFECT",
                "REPORT_TYPE",
                "CRASH_TYPE",
                "DAMAGE",
                "PRIM_CONTRIBUTORY_CAUSE",
                "SEC_CONTRIBUTORY_CAUSE",
                "MOST_SEVERE_INJURY",
            ]
        ].to_records(index=False).tolist()

        insert_sql = """
            INSERT INTO staging.dim_crash_info (
                crash_info_key, traffic_control_device, device_condition, first_crash_type,
                trafficway_type, alignment, roadway_surface_cond, road_defect, report_type,
                crash_type, damage, prim_contributory_cause, sec_contributory_cause, most_severe_injury
            ) VALUES %s
        """

        psycopg2.extras.execute_values(
            cursor,
            insert_sql,
            records,
            page_size=1000
        )
        conn.commit()

        # Create core table and copy data from staging
        logger.info(f"{module_tag} Creating core table.")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS core.dim_crash_info (
                crash_info_key NUMERIC PRIMARY KEY,
                traffic_control_device VARCHAR(50) NOT NULL,
                device_condition VARCHAR(50) NOT NULL,
                first_crash_type VARCHAR(50) NOT NULL,
                trafficway_type VARCHAR(50) NOT NULL,
                alignment VARCHAR(50) NOT NULL,
                roadway_surface_cond VARCHAR(50) NOT NULL,
                road_defect VARCHAR(50) NOT NULL,
                report_type VARCHAR(50) NOT NULL,
                crash_type VARCHAR(50) NOT NULL,
                damage VARCHAR(50) NOT NULL,
                prim_contributory_cause VARCHAR(100) NOT NULL,
                sec_contributory_cause VARCHAR(100) NOT NULL,
                most_severe_injury VARCHAR(50) NOT NULL
            );
            """
        )
        cursor.execute("TRUNCATE core.dim_crash_info CASCADE;")

        logger.info(f"{module_tag} Copying data from staging to core.")
        cursor.execute(
            """
            INSERT INTO core.dim_crash_info (
                crash_info_key, traffic_control_device, device_condition, first_crash_type,
                trafficway_type, alignment, roadway_surface_cond, road_defect, report_type,
                crash_type, damage, prim_contributory_cause, sec_contributory_cause, most_severe_injury
            )
            SELECT
                crash_info_key, traffic_control_device, device_condition, first_crash_type,
                trafficway_type, alignment, roadway_surface_cond, road_defect, report_type,
                crash_type, damage, prim_contributory_cause, sec_contributory_cause, most_severe_injury
            FROM staging.dim_crash_info;
            """
        )
        conn.commit()

        cursor.close()
        conn.close()
        logger.info(f"{module_tag} Successfully inserted dim_crash_info into database.")

    except Exception as e:
        logger.error(f"{module_tag} Error in load_dim_crash_info: {e}")
        raise