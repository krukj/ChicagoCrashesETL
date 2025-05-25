from .load_dim_date import load_dim_date
from .load_dim_location import load_dim_location
from .load_dim_vehicle import load_dim_vehicle
from .load_dim_person import load_dim_person
from .load_dim_crash_info import load_dim_crash_info
from .load_fact_weather import load_fact_weather
from .load_fact_crash import load_fact_crash

import os
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
    load_dim_date(dim_date_path)
    load_dim_location(dim_location_path)
    load_dim_vehicle(dim_vehicle_path)
    load_dim_person(dim_person_path)
    load_dim_crash_info(dim_crash_info_path)
    load_fact_weather(fact_weather_path)
    load_fact_crash(fact_crash_path)


def main():
    base_dir = "/opt/airflow"
    fact_crash_path = os.path.join(
        base_dir, "data", "tmp", "transformed", "fact_crash.pkl"
    )
    dim_crash_info_path = os.path.join(
        base_dir, "data", "tmp", "transformed", "dim_crash_info.pkl"
    )
    dim_location_path = os.path.join(
        base_dir, "data", "tmp", "transformed", "dim_location.pkl"
    )
    fact_weather_path = os.path.join(
        base_dir, "data", "tmp", "transformed", "fact_weather.pkl"
    )
    dim_vehicle_path = os.path.join(
        base_dir, "data", "tmp", "transformed", "dim_vehicle.pkl"
    )
    dim_person_path = os.path.join(
        base_dir, "data", "tmp", "transformed", "dim_people.pkl"
    )
    dim_date_path = os.path.join(base_dir, "data", "tmp", "transformed", "dim_date.pkl")

    logger.info("[LOAD] Starting to load data.")
    load_all(
        dim_date_path=dim_date_path,
        dim_location_path=dim_location_path,
        dim_vehicle_path=dim_vehicle_path,
        dim_person_path=dim_person_path,
        dim_crash_info_path=dim_crash_info_path,
        fact_weather_path=fact_weather_path,
        fact_crash_path=fact_crash_path,
    )


if __name__ == "__main__":
    main()
