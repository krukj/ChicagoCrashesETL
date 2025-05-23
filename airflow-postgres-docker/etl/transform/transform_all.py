from .transform_crash import transform_crash, split_crash
from .transform_weather import trasform_weather
from .transform_vehicle import transform_vehicle
from .utils import generate_surrogate_key

import os
import pandas as pd
from etl.logging_config import setup_logger

logger = setup_logger(__name__)


def perform_transformation_crash(
    filepath_in: str, fact_filepath_out: str, dim_filepath_out: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    module_tag = "[CRASH]"
    logger.info(f"{module_tag} Starting crash data transformation.")

    crash_df = transform_crash(filepath_in)

    logger.info(
        f"{module_tag} Splitting transformed crash data into fact and dimension tables."
    )
    fact_crash, dim_crash_info = split_crash(crash_df)

    logger.info(f"{module_tag} Crash data transformation completed successfully.")

    fact_crash.to_pickle(fact_filepath_out)
    dim_crash_info.to_pickle(dim_filepath_out)
    logger.info(
        f"{module_tag} Saved as a pickle in {fact_filepath_out} and {dim_filepath_out}."
    )

    return fact_crash, dim_crash_info


def perform_transformation_people(filepath_in: str, filepath_out: str):
    # module_tag = "[PEOPLE]"
    # logger.info(f"{module_tag} Starting people data transformation.")

    # logger.info(f"{module_tag} People data transformation completed successfully.")

    # logger.info(f"{module_tag} Saved as a pickle in {filepath_out}")

    pass


def perform_transformation_vehicles(
    filepath_in: str, filepath_out: str, dim_crash_info_path: str
):
    module_tag = "[VEHICLES]"
    logger.info(f"{module_tag} Starting vehicle data transformation.")

    vehicle_df = transform_vehicle(filepath_in)

    dim_crash_info = pd.read_pickle(dim_crash_info_path)
    crash_key_map = dim_crash_info.set_index("CRASH_RECORD_ID")["CRASH_INFO_KEY"]
    vehicle_df["CRASH_INFO_KEY"] = vehicle_df["CRASH_RECORD_ID"].map(crash_key_map)

    vehicle_df["VEHICLE_KEY"] = vehicle_df.apply(
        lambda row: generate_surrogate_key(*[row[col] for col in vehicle_df.columns]),
        axis=1,
    )
    logger.info(f"{module_tag} Vehicle data transformation completed successfully.")

    vehicle_df.to_pickle(filepath_out)
    logger.info(f"{module_tag} Saved as a pickle in {filepath_out}")


def perform_transformation_weather(filepath_in: str, filepath_out: str):
    module_tag = "[WEATHER]"
    logger.info(f"{module_tag} Starting weather data transformation.")

    weather_df = trasform_weather(filepath_in)
    logger.info(f"{module_tag} Weather data transformation completed successfully.")

    weather_df.to_pickle(filepath_out)
    logger.info(f"{module_tag} Saved as a pickle in {filepath_out}")
    return weather_df


def main():
    # source paths
    crash_path_in = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "data",
        "tmp",
        "extracted",
        "crashes.pkl",
    )
    weather_path_in = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "data",
        "tmp",
        "extracted",
        "weather.pkl",
    )

    vehicles_path_in = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "data",
        "tmp",
        "extracted",
        "vehicles.pkl",
    )

    # out paths
    fact_crash_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "data",
        "tmp",
        "transformed",
        "fact_crash.pkl",
    )
    dim_crash_info_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "data",
        "tmp",
        "transformed",
        "dim_crash_info.pkl",
    )
    fact_weather_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "data",
        "tmp",
        "transformed",
        "fact_weather.pkl",
    )
    dim_vehicle_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "data",
        "tmp",
        "transformed",
        "dim_vehicle.pkl",
    )
    fact_crash, dim_crash_info = perform_transformation_crash(
        crash_path_in, fact_crash_path, dim_crash_info_path
    )
    fact_weather = perform_transformation_weather(weather_path_in, fact_weather_path)
    dim_vehicle = perform_transformation_vehicles(
        vehicles_path_in, dim_vehicle_path, dim_crash_info_path
    )
    # pass


if __name__ == "__main__":
    main()
