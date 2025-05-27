from .schemas import (
    EXPECTED_CRASHES_COLUMNS,
    EXPECTED_PEOPLE_COLUMNS,
    EXPECTED_VEHICLES_COLUMNS,
    EXPECTED_WEATHER_COLUMNS,
)
from .utils import validate_columns, print_null_summary
from etl.logging_config import setup_logger


import pandas as pd
from pathlib import Path
import os

logger = setup_logger(__name__)


def extract_crashes_csv(filepath_in: str, filepath_out: str) -> pd.DataFrame:
    module_tag = "[CRASH]"
    logger.info(f"{module_tag} Starting crash data extraction.")

    chunksize = 100_000
    chunks = []
    total_records = 0

    for chunk in pd.read_csv(filepath_in, chunksize=chunksize):
        validate_columns(chunk, EXPECTED_CRASHES_COLUMNS, "extract_crashes")
        print_null_summary(chunk, EXPECTED_CRASHES_COLUMNS, "extract_crashes")
        chunks.append(chunk)
        total_records += len(chunk)
        logger.info(
            f"{module_tag} Processed chunk with {len(chunk)} records. Total so far: {total_records}."
        )

    df = pd.concat(chunks)
    logger.info(
        f"{module_tag} Successfully extracted {len(df)} total records from {os.path.basename(filepath_in)}."
    )

    df.to_pickle(filepath_out)
    logger.info(f"{module_tag} Saved as a pickle in {filepath_out}.")

    return df


def extract_people_csv(filepath_in: str, filepath_out: str) -> pd.DataFrame:
    module_tag = "PERSON"
    logger.info(f"{module_tag} Starting people data extraction.")

    chunksize = 100_000
    chunks = []
    total_records = 0

    for chunk in pd.read_csv(filepath_in, low_memory=False, chunksize=chunksize):
        validate_columns(chunk, EXPECTED_PEOPLE_COLUMNS, "extract_people")
        print_null_summary(chunk, EXPECTED_PEOPLE_COLUMNS, "extract_people")
        chunks.append(chunk)
        total_records += len(chunk)
        logger.info(
            f"{module_tag} Processed chunk with {len(chunk)} records. Total so far: {total_records}."
        )

    df = pd.concat(chunks)
    logger.info(
        f"{module_tag} Successfully extracted {len(df)} total records from {os.path.basename(filepath_in)}."
    )

    df.to_pickle(filepath_out)
    logger.info(f"{module_tag} Saved as a pickle in {filepath_out}.")

    return df


def extract_vehicles_csv(filepath_in: str, filepath_out: str) -> pd.DataFrame:
    module_tag = "[VEHICLE]"
    logger.info(f"{module_tag} Starting vehicles data extraction.")

    chunksize = 100_000
    chunks = []
    total_records = 0

    for chunk in pd.read_csv(filepath_in, low_memory=False, chunksize=chunksize):
        validate_columns(chunk, EXPECTED_VEHICLES_COLUMNS, "extract_vehicles")
        print_null_summary(chunk, EXPECTED_VEHICLES_COLUMNS, "extract_vehicles")
        chunks.append(chunk)
        total_records += len(chunk)
        logger.info(
            f"{module_tag} Processed chunk with {len(chunk)} records. Total so far: {total_records}."
        )

    df = pd.concat(chunks)
    logger.info(
        f"{module_tag} Successfully extracted {len(df)} total records from {os.path.basename(filepath_in)}."
    )

    df.to_pickle(filepath_out)
    logger.info(f"{module_tag} Saved as a pickle in {filepath_out}.")

    return df


def extract_weather_csv(dirpath: str, filepath_out: str) -> pd.DataFrame:
    """
    Tutaj zakładamy że mamy katalog dirpath gdzie znajdują się pliki csv z pogodą.
    """
    logger.info(f"Reading weather data from path: {dir}")

    module_tag = "[WEATHER]"
    logger.info(f"{module_tag} Starting vehicles data extraction.")
    df = pd.DataFrame()

    directory = Path(dirpath)
    logger.info(
        f"{module_tag} Successfully extracted {len(list(directory.iterdir()))} files from {directory}."
    )
    for file in directory.iterdir():
        df_file = pd.read_csv(file)

        logger.info(
            f"{module_tag} Successfully extracted {len(df_file)} records from {os.path.basename(file)}."
        )

        df = pd.concat([df, df_file], ignore_index=True)
        logger.info(
            f"{module_tag} Successfully concatenated {len(df_file)} records from {os.path.basename(file)} to df."
        )

    logger.info(
        f"{module_tag} Successfully connected {len(df)} records from directory {directory}"
    )
    validate_columns(df, EXPECTED_WEATHER_COLUMNS, "extract_weather")
    print_null_summary(df, EXPECTED_WEATHER_COLUMNS, "extract_weather")

    df.to_pickle(filepath_out)

    logger.info(f"{module_tag} Saved as a pickle in {filepath_out}.")

    return df


def main():

    base_dir = "/opt/airflow"
    weather_path = os.path.join(base_dir, "data", "weather_data")
    crashes_path = os.path.join(
        base_dir,
        "data",
        "crashes_data",
        "Traffic_Crashes_Crashes.csv",
    )
    people_path = os.path.join(
        base_dir,
        "data",
        "crashes_data",
        "Traffic_Crashes_People.csv",
    )
    vehicles_path = os.path.join(
        base_dir,
        "data",
        "crashes_data",
        "Traffic_Crashes_Vehicles.csv",
    )

    weather_path_out = os.path.join(base_dir, "data", "tmp", "extracted", "weather.pkl")
    crashes_path_out = os.path.join(base_dir, "data", "tmp", "extracted", "crashes.pkl")
    people_path_out = os.path.join(base_dir, "data", "tmp", "extracted", "people.pkl")
    vehicles_path_out = os.path.join(
        base_dir, "data", "tmp", "extracted", "vehicles.pkl"
    )

    weather_df = extract_weather_csv(weather_path, weather_path_out)
    crashes_df = extract_crashes_csv(crashes_path, crashes_path_out)
    people_df = extract_people_csv(people_path, people_path_out)
    vehicles_df = extract_vehicles_csv(vehicles_path, vehicles_path_out)


if __name__ == "__main__":
    main()
