import pandas as pd
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

        cursor.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS core;")

        logger.info(f"{module_tag} Dropping old tables.")
        cursor.execute("DROP TABLE IF EXISTS staging.dim_crash_info;")
        cursor.execute("DROP TABLE IF EXISTS core.dim_crash_info;")

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

        cursor.execute("TRUNCATE staging.dim_crash_info;")

        logger.info(f"{module_tag} Inserting data into staging.")
        for _, row in dim_crash_info.iterrows():
            cursor.execute(
                """
                INSERT INTO staging.dim_crash_info (
                    crash_info_key, traffic_control_device, device_condition, first_crash_type,
                    trafficway_type, alignment, roadway_surface_cond, road_defect, report_type,
                    crash_type, damage, prim_contributory_cause, sec_contributory_cause, most_severe_injury
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row["CRASH_INFO_KEY"],
                    row["TRAFFIC_CONTROL_DEVICE"],
                    row["DEVICE_CONDITION"],
                    row["FIRST_CRASH_TYPE"],
                    row["TRAFFICWAY_TYPE"],
                    row["ALIGNMENT"],
                    row["ROADWAY_SURFACE_COND"],
                    row["ROAD_DEFECT"],
                    row["REPORT_TYPE"],
                    row["CRASH_TYPE"],
                    row["DAMAGE"],
                    row["PRIM_CONTRIBUTORY_CAUSE"],
                    row["SEC_CONTRIBUTORY_CAUSE"],
                    row["MOST_SEVERE_INJURY"],
                ),
            )

        conn.commit()

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

        cursor.execute("TRUNCATE core.dim_crash_info;")

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
        logger.error(f"{module_tag} Error in load_dim_crash_info: {str(e)}")
        raise
