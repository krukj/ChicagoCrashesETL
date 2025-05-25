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


def perform_transformation_crash(
    filepath_in: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    module_tag = "[CRASH]"
    logger.info(f"{module_tag} Starting crash data transformation.")

    crash_df = transform_crash(filepath_in)

    logger.info(
        f"{module_tag} Splitting transformed crash data into fact and dimension tables."
    )
    fact_crash, dim_crash_info, dim_location = split_crash(crash_df)

    logger.info(f"{module_tag} Crash data transformation completed successfully.")

    return fact_crash, dim_crash_info, dim_location


def perform_transformation_person(
    filepath_in: str
    ) -> pd.DataFrame:
    module_tag = "[PERSON]"
    logger.info(f"{module_tag} Starting people data transformation.")

    person_df = transform_person(filepath_in)

    logger.info(f"{module_tag} People data transformation completed successfully.")

    return person_df

def perform_transformation_vehicles(
    filepath_in: str
    ) -> pd.DataFrame:
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
    start_date: datetime.datetime = datetime.datetime(2016, 1, 1, 0, 0),
    end_date: datetime.datetime = datetime.datetime(2021, 12, 31, 23, 0)
    ) -> pd.DataFrame:
    module_tag = "[DATE]"
    logger.info(f"{module_tag} Starting dim_date making.")

    dim_date = make_dim_date(start_date, end_date)

    logger.info(f"{module_tag} Dim_date successfully made")

    return dim_date

# To jest już do transformowanie pomiędzy nimi, nazywa się transform_all ale te funkcje z góry też trzeba będzie wykonywać
def transform_all(df_fact_crash:   pd.DataFrame,
                  df_fact_weather: pd.DataFrame,
                  df_dim_crash_info: pd.DataFrame,
                  df_dim_person    : pd.DataFrame,
                  df_dim_vehicle   : pd.DataFrame,
                  df_dim_location  : pd.DataFrame,
                  df_dim_date      : pd.DataFrame,

                  fact_crash_path_out    : str,
                  fact_weather_path_out  : str,
                  dim_crash_info_path_out: str,
                  dim_person_path_out    : str,
                  dim_vehicle_path_out   : str,
                  dim_location_path_out  : str,
                  dim_date_path_out      : str):

    # Dołączyć klucze do fact_crash [person_id, vehicle_id, location_id] (crash_info_id już jest z transform poprzedniego)
    df_fact_crash = df_fact_crash.merge(df_dim_person[["CRASH_RECORD_ID", "PERSON_ID", "VEHICLE_ID"]], on='CRASH_RECORD_ID', how='inner')

    # Wyrzucić niepotrzebne kolumny z innych wymiarów ewentualnie ale może nie trzeba


    # I dopiero teraz zapisanie do pickli wszystkiego

    df_fact_crash.to_pickle(fact_crash_path_out)
    df_dim_crash_info.to_pickle(dim_crash_info_path_out)
    df_dim_location.to_pickle(dim_location_path_out)
    logger.info(
        f"[CRASH] Saved as a pickle in {fact_crash_path_out}, {dim_crash_info_path_out} and {dim_location_path_out}."
    )

    df_dim_person.to_pickle(dim_person_path_out)
    logger.info(
        f"[PERSON] Saved as a pickle in {dim_person_path_out}"
    )

    df_dim_vehicle.to_pickle(dim_vehicle_path_out)
    logger.info(
        f"[VEHICLE] Saved as a pickle in {dim_vehicle_path_out}"
    )

    df_dim_location.to_pickle(dim_location_path_out)
    logger.info(
        f"[LOCATION] Saved as a pickle in {dim_location_path_out}"
    )

    df_fact_weather.to_pickle(fact_weather_path_out)
    logger.info(
        f"[WEATHER] Saved as a pickle in {fact_weather_path_out}"
    )

    df_dim_date.to_pickle(dim_date_path_out)
    logger.info(
        f"[DATE] Saves as a pickle in {dim_date_path_out}"
    )

    return (
        df_fact_crash,
        df_fact_weather,
        df_dim_crash_info,
        df_dim_person,
        df_dim_vehicle,
        df_dim_location,
        df_dim_date
    )

def main():
    base_dir = "/opt/airflow"

    # source paths
    crash_path_in    = os.path.join(base_dir, "data","tmp","extracted","crashes.pkl")
    weather_path_in  = os.path.join(base_dir, "data","tmp","extracted", "weather.pkl")
    vehicles_path_in = os.path.join(base_dir, "data","tmp","extracted","vehicles.pkl")
    person_path_in   = os.path.join(base_dir, "data","tmp","extracted","people.pkl")

    # out paths
    fact_crash_path     = os.path.join(base_dir, "data","tmp","transformed","fact_crash.pkl")
    dim_crash_info_path = os.path.join(base_dir, "data","tmp","transformed","dim_crash_info.pkl")
    dim_location_path   = os.path.join(base_dir, "data","tmp","transformed","dim_location.pkl")
    fact_weather_path   = os.path.join(base_dir, "data","tmp","transformed","fact_weather.pkl")
    dim_vehicle_path    = os.path.join(base_dir, "data","tmp","transformed","dim_vehicle.pkl")
    dim_person_path     = os.path.join(base_dir, "data","tmp","transformed","dim_people.pkl")
    dim_date_path       = os.path.join(base_dir, "data","tmp","transformed","dim_date.pkl")

    fact_crash, dim_crash_info, dim_location = perform_transformation_crash(
        crash_path_in
    )

    fact_weather = perform_transformation_weather(
        weather_path_in
    )
    dim_vehicle = perform_transformation_vehicles(
        vehicles_path_in
    )

    dim_person = perform_transformation_person(
        person_path_in
    )

    dim_date = perform_make_dim_date(
    )

    ### Teraz wszystko:

    tables = transform_all(
        df_fact_crash    =fact_crash,
        df_fact_weather  =fact_weather,
        df_dim_crash_info=dim_crash_info,
        df_dim_person    =dim_person,
        df_dim_vehicle   =dim_vehicle,
        df_dim_location  =dim_location,
        df_dim_date      =dim_date,

        fact_crash_path_out=fact_crash_path,
        fact_weather_path_out=fact_weather_path,
        dim_crash_info_path_out=dim_crash_info_path,
        dim_person_path_out=dim_person_path,
        dim_vehicle_path_out=dim_vehicle_path,
        dim_location_path_out=dim_location_path,
        dim_date_path_out=dim_date_path
    )
    

if __name__ == "__main__":
    main()
