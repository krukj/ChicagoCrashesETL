from typing import Tuple
from .transform_crash import transform_crash, split_crash
from .transform_weather import trasform_weather
from .transform_vehicle import transform_vehicle
from .transform_person import transform_person
from .make_dim_date import make_dim_date
from .utils import generate_surrogate_key

import os
import datetime
import pandas as pd
from etl.logging_config import setup_logger

logger = setup_logger(__name__)


def perform_transformation_crash(filepath_in: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    module_tag = "[CRASH]"
    logger.info(f"{module_tag} Starting crash data transformation.")

    crash_df = transform_crash(filepath_in)

    logger.info(
        f"{module_tag} Splitting transformed crash data into fact and dimension tables."
    )
    fact_crash, dim_crash_info, dim_location = split_crash(crash_df)

    logger.info(f"{module_tag} Crash data transformation completed successfully.")

    return fact_crash, dim_crash_info, dim_location


def perform_transformation_person(filepath_in: str) -> pd.DataFrame:
    module_tag = "[PERSON]"
    logger.info(f"{module_tag} Starting people data transformation.")

    person_df = transform_person(filepath_in)

    logger.info(f"{module_tag} People data transformation completed successfully.")

    return person_df


def perform_transformation_vehicles(filepath_in: str) -> pd.DataFrame:
    module_tag = "[VEHICLE]"
    logger.info(f"{module_tag} Starting vehicle data transformation.")

    vehicle_df = transform_vehicle(filepath_in)

    vehicle_df["VEHICLE_KEY"] = vehicle_df.apply(
        lambda row: generate_surrogate_key(*[row[col] for col in vehicle_df.columns]),
        axis=1,
    )
    logger.info(f"{module_tag} Vehicle data transformation completed successfully.")

    return vehicle_df


def perform_transformation_weather(filepath_in: str) -> pd.DataFrame:
    module_tag = "[WEATHER]"
    logger.info(f"{module_tag} Starting weather data transformation.")

    weather_df = trasform_weather(filepath_in)
    logger.info(f"{module_tag} Weather data transformation completed successfully.")

    return weather_df


def perform_make_dim_date(
    start_date: datetime.datetime = datetime.datetime(2013, 1, 1, 0, 0),
    end_date: datetime.datetime = datetime.datetime(2025, 12, 31, 23, 0),
) -> pd.DataFrame:
    module_tag = "[DATE]"
    logger.info(f"{module_tag} Starting dim_date making.")

    dim_date = make_dim_date(start_date, end_date)

    logger.info(f"{module_tag} Dim_date successfully made")

    return dim_date


# To jest już do transformowanie pomiędzy nimi, nazywa się transform_all ale te funkcje z góry też trzeba będzie wykonywać
def transform_all(
    df_fact_crash: pd.DataFrame,
    df_fact_weather: pd.DataFrame,
    df_dim_crash_info: pd.DataFrame,
    df_dim_person: pd.DataFrame,
    df_dim_vehicle: pd.DataFrame,
    df_dim_location: pd.DataFrame,
    df_dim_date: pd.DataFrame,
    fact_crash_path_out: str,
    fact_weather_path_out: str,
    dim_crash_info_path_out: str,
    dim_person_path_out: str,
    dim_vehicle_path_out: str,
    dim_location_path_out: str,
    dim_date_path_out: str,
    removed_records_path: str,
):

    # Dołączyć klucze do fact_crash [person_id, vehicle_id, location_id]
    df_fact_crash = df_fact_crash.merge(
        df_dim_person[["CRASH_RECORD_ID", "PERSON_ID", "VEHICLE_ID"]],
        on="CRASH_RECORD_ID",
        how="inner",
    )

    # Obsługa NULL w VEHICLE_ID - wygeneruj unikalne wartości
    null_vehicle_mask = df_fact_crash["VEHICLE_ID"].isna()
    if null_vehicle_mask.any():
        max_vehicle_id = df_fact_crash["VEHICLE_ID"].max()
        if pd.isna(max_vehicle_id):
            max_vehicle_id = 0

        # Generuj unikalne ID dla NULL vehicle_id
        null_count = null_vehicle_mask.sum()
        new_vehicle_ids = range(
            int(max_vehicle_id) + 1, int(max_vehicle_id) + 1 + null_count
        )
        df_fact_crash.loc[null_vehicle_mask, "VEHICLE_ID"] = new_vehicle_ids
        logger.info(f"[VEHICLE] Assigned {null_count} new vehicle IDs for NULL values")

    # NAJPIERW generuj klucze wymiarów PRZED usuwaniem duplikatów
    df_dim_crash_info.insert(
        0,
        "CRASH_INFO_KEY",
        df_dim_crash_info.apply(
            lambda row: generate_surrogate_key(
                *[row[col] for col in df_dim_crash_info.columns.to_list()]
            ),
            axis=1,
        ),
    )

    df_dim_location.insert(
        0,
        "LOCATION_KEY",
        df_dim_location.apply(
            lambda row: generate_surrogate_key(
                *[row[col] for col in df_dim_location.columns.to_list()]
            ),
            axis=1,
        ),
    )

    # Usuń duplikaty z wymiarów PRZED łączeniem z faktami
    logger.info(f"[LOCATION] Location records before dedup: {len(df_dim_location)}")
    df_dim_crash_info = df_dim_crash_info.drop_duplicates(subset=["CRASH_INFO_KEY"])
    df_dim_location = df_dim_location.drop_duplicates(subset=["LOCATION_KEY"])

    df_fact_crash_remove = None
    # Usuń problematyczny klucz 3707804392 i odpowiednie rekordy z faktów
    problematic_key = 3707804392
    if problematic_key in df_dim_location["LOCATION_KEY"].values:
        # Znajdź CRASH_RECORD_ID powiązane z tym kluczem
        problematic_crash_ids = df_dim_location[
            df_dim_location["LOCATION_KEY"] == problematic_key
        ]["CRASH_RECORD_ID"].unique()

        # Usuń z wymiarów
        df_dim_location = df_dim_location[
            df_dim_location["LOCATION_KEY"] != problematic_key
        ]

        # Usuń odpowiednie rekordy z faktów
        original_fact_count = len(df_fact_crash)
        df_fact_crash_remove = df_fact_crash["CRASH_RECORD_ID"].isin(
            problematic_crash_ids
        )

        df_fact_crash = df_fact_crash[
            ~df_fact_crash["CRASH_RECORD_ID"].isin(problematic_crash_ids)
        ]
        removed_fact_count = original_fact_count - len(df_fact_crash)

        logger.warning(
            f"[LOCATION] Removed problematic key {problematic_key} and {removed_fact_count} related fact records"
        )

    logger.info(f"[LOCATION] Location records after dedup: {len(df_dim_location)}")

    valid_vehicle_ids = df_dim_vehicle["VEHICLE_ID"]
    original_fact_count = len(df_fact_crash)

    if df_fact_crash_remove is not None:
        df_fact_crash_remove = pd.concat(
            [
                df_fact_crash_remove,
                df_fact_crash[~df_fact_crash["VEHICLE_ID"].isin(valid_vehicle_ids)],
            ]
        )
    else:
        df_fact_crash_remove = df_fact_crash[
            ~df_fact_crash["VEHICLE_ID"].isin(valid_vehicle_ids)
        ]

    df_fact_crash_remove.to_pickle(
        os.path.join(removed_records_path, "removed_fact_crash.pkl")
    )
    df_fact_crash = df_fact_crash[df_fact_crash["VEHICLE_ID"].isin(valid_vehicle_ids)]
    removed_fact_count = original_fact_count - len(df_fact_crash)
    logger.info(
        f"[VEHICLE] Removed {removed_fact_count} fact records with invalid vehicle IDs"
    )
    logger.info(
        f"[VEHICLE] & [LOCATION] Removed records saved to {removed_records_path}"
    )

    # Sprawdź, czy wszystkie CRASH_RECORD_ID z faktów mają odpowiedniki w wymiarach
    valid_crash_info_ids = set(df_dim_crash_info["CRASH_RECORD_ID"].unique())
    valid_location_ids = set(df_dim_location["CRASH_RECORD_ID"].unique())

    # Usuń fakty bez odpowiedników w wymiarach
    df_fact_crash = df_fact_crash[
        df_fact_crash["CRASH_RECORD_ID"].isin(valid_crash_info_ids)
        & df_fact_crash["CRASH_RECORD_ID"].isin(valid_location_ids)
    ]

    # DOPIERO TERAZ generuj klucz dla faktów
    df_fact_crash.insert(
        0,
        "FACT_CRASH_KEY",
        df_fact_crash.apply(
            lambda row: generate_surrogate_key(
                *[row[col] for col in df_fact_crash.columns.to_list()]
            ),
            axis=1,
        ),
    )

    # Usuń duplikaty z faktów
    df_fact_crash = df_fact_crash.drop_duplicates(subset=["FACT_CRASH_KEY"])

    # Łącz z wymiarami
    df_fact_crash = df_fact_crash.merge(
        df_dim_crash_info[["CRASH_RECORD_ID", "CRASH_INFO_KEY"]],
        on="CRASH_RECORD_ID",
        how="inner",
    )
    df_fact_crash = df_fact_crash.merge(
        df_dim_location[["CRASH_RECORD_ID", "LOCATION_KEY"]],
        on="CRASH_RECORD_ID",
        how="inner",
    )

    # Sprawdź końcowe statystyki
    logger.info(f"[FINAL] Fact crash records: {len(df_fact_crash)}")
    logger.info(f"[FINAL] Dim crash info records: {len(df_dim_crash_info)}")
    logger.info(f"[FINAL] Dim location records: {len(df_dim_location)}")

    # Zapisz do pickli
    df_fact_crash.to_pickle(fact_crash_path_out)
    df_dim_crash_info.to_pickle(dim_crash_info_path_out)
    df_dim_location.to_pickle(dim_location_path_out)
    logger.info(
        f"[CRASH] Saved as a pickle in {fact_crash_path_out}, {dim_crash_info_path_out} and {dim_location_path_out}."
    )

    df_dim_person.to_pickle(dim_person_path_out)
    logger.info(f"[PERSON] Saved as a pickle in {dim_person_path_out}")

    df_dim_vehicle.to_pickle(dim_vehicle_path_out)
    logger.info(f"[VEHICLE] Saved as a pickle in {dim_vehicle_path_out}")

    df_fact_weather.to_pickle(fact_weather_path_out)
    logger.info(f"[WEATHER] Saved as a pickle in {fact_weather_path_out}")

    df_dim_date.to_pickle(dim_date_path_out)
    logger.info(f"[DATE] Saves as a pickle in {dim_date_path_out}")

    return (
        df_fact_crash,
        df_fact_weather,
        df_dim_crash_info,
        df_dim_person,
        df_dim_vehicle,
        df_dim_location,
        df_dim_date,
    )


def main():
    base_dir = "/opt/airflow"

    # source paths
    crash_path_in = os.path.join(base_dir, "data", "tmp", "extracted", "crashes.pkl")
    weather_path_in = os.path.join(base_dir, "data", "tmp", "extracted", "weather.pkl")
    vehicles_path_in = os.path.join(
        base_dir, "data", "tmp", "extracted", "vehicles.pkl"
    )
    person_path_in = os.path.join(base_dir, "data", "tmp", "extracted", "people.pkl")

    # out paths
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

    fact_crash, dim_crash_info, dim_location = perform_transformation_crash(
        crash_path_in
    )

    fact_weather = perform_transformation_weather(weather_path_in)
    dim_vehicle = perform_transformation_vehicles(vehicles_path_in)

    dim_person = perform_transformation_person(person_path_in)

    dim_date = perform_make_dim_date()

    ### Teraz wszystko:
    removed_path = os.path.join(base_dir, "data", "tmp", "transformed", "removed")
    tables = transform_all(
        df_fact_crash=fact_crash,
        df_fact_weather=fact_weather,
        df_dim_crash_info=dim_crash_info,
        df_dim_person=dim_person,
        df_dim_vehicle=dim_vehicle,
        df_dim_location=dim_location,
        df_dim_date=dim_date,
        fact_crash_path_out=fact_crash_path,
        fact_weather_path_out=fact_weather_path,
        dim_crash_info_path_out=dim_crash_info_path,
        dim_person_path_out=dim_person_path,
        dim_vehicle_path_out=dim_vehicle_path,
        dim_location_path_out=dim_location_path,
        dim_date_path_out=dim_date_path,
        removed_records_path=removed_path,
    )


if __name__ == "__main__":
    main()
