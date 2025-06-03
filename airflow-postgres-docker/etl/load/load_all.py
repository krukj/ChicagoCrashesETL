from .load_dim_date import load_dim_date
from .load_dim_location import load_dim_location
from .load_dim_vehicle import load_dim_vehicle
from .load_dim_person import load_dim_person
from .load_dim_crash_info import load_dim_crash_info
from .load_fact_weather import load_fact_weather
from .load_fact_crash import load_fact_crash
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook
from etl.logging_config import setup_logger

logger = setup_logger(__name__)


def load_all(
    dim_date_path: str,
    dim_location_path: str,
    dim_vehicle_path: str,
    dim_person_path: str,
    dim_crash_info_path: str,
    fact_weather_path: str,
    fact_crash_path: str,
):
    """
    Load all data in correct order with proper dependency management
    """
    try:
        # Disable foreign key checks temporarily for truncation
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Truncate fact tables first (they have foreign keys)
        fact_tables = [
            "staging.fact_crash",
            "staging.fact_weather",
            "core.fact_crash",
            "core.fact_weather",
        ]

        logger.info("[LOAD] Truncating fact tables...")
        for table in fact_tables:
            schema, name = table.split(".")
            cursor.execute(
                f"""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = '{schema}' AND table_name = '{name}'
                ) THEN
                    EXECUTE 'TRUNCATE TABLE {schema}.{name} CASCADE';
                END IF;
            END$$;
            """
            )

        conn.commit()
        cursor.close()
        conn.close()

        logger.info("[LOAD] Loading dimension tables...")
        load_dim_date(dim_date_path)
        load_dim_location(dim_location_path)
        load_dim_person(dim_person_path)
        load_dim_vehicle(dim_vehicle_path)
        load_dim_crash_info(dim_crash_info_path)

        # # Load fact tables last
        logger.info("[LOAD] Loading fact tables...")
        load_fact_crash(fact_crash_path)
        load_fact_weather(fact_weather_path)

        logger.info("[LOAD] All data loaded successfully!")

    except Exception as e:
        logger.error(f"[LOAD] Error in load_all: {e}")
        raise


def main():
    base_dir = "/opt/airflow"

    # Define all file paths
    paths = {
        "dim_date": os.path.join(
            base_dir, "data", "tmp", "transformed", "dim_date.pkl"
        ),
        "dim_location": os.path.join(
            base_dir, "data", "tmp", "transformed", "dim_location.pkl"
        ),
        "dim_vehicle": os.path.join(
            base_dir, "data", "tmp", "transformed", "dim_vehicle.pkl"
        ),
        "dim_person": os.path.join(
            base_dir, "data", "tmp", "transformed", "dim_people.pkl"
        ),
        "dim_crash_info": os.path.join(
            base_dir, "data", "tmp", "transformed", "dim_crash_info.pkl"
        ),
        "fact_weather": os.path.join(
            base_dir, "data", "tmp", "transformed", "fact_weather.pkl"
        ),
        "fact_crash": os.path.join(
            base_dir, "data", "tmp", "transformed", "fact_crash.pkl"
        ),
    }

    # Check if all files exist
    for name, path in paths.items():
        if not os.path.exists(path):
            logger.error(f"[LOAD] File not found: {path}")
            raise FileNotFoundError(f"Required file not found: {path}")

    logger.info("[LOAD] Starting to load data.")
    logger.info(f"[LOAD] All required files found: {list(paths.keys())}")

    load_all(
        dim_date_path=paths["dim_date"],
        dim_location_path=paths["dim_location"],
        dim_vehicle_path=paths["dim_vehicle"],
        dim_person_path=paths["dim_person"],
        dim_crash_info_path=paths["dim_crash_info"],
        fact_weather_path=paths["fact_weather"],
        fact_crash_path=paths["fact_crash"],
    )


if __name__ == "__main__":
    main()
