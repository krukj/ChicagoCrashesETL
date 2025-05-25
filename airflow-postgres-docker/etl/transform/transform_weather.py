# fact_weather_id: surrogate key
# date_id: dodac klucz łączący z dim_date
# name: wywalic
# datetime: na jej podstawie date_id
# temp: type: float, nazwa: temp
# feelslike: type: float, nazwa: feels_like
# dew: type: float, nazwa: dew
# humidity: type: float, nazwa: humidity
# precip: type: float, nazwa: precip, nulle -> 0
# precipprob: type: int, nazwa: precip_prob, chyba ma tylko distinct 0 i 100
# preciptype: type: str, nazwa: precip_type, nulle jako NONE moze zeby bylo ze nie bylo opadu
# snow: type: float, nazwa: snow, nulle -> 0
# snowdepth: type: float, nazwa: snow_depth, nulle -> 0
# windgust: type: float, nazwa: wind_gust, nulle -> 0
# windspeed: type: float, nazwa: wind_speed, nulle -> 0
# winddir: jest w katach, zamienic na kierunek typu N, S, type: str, nazwa: wind_dir
# sealevelpressure: type: float, nazwa: sea_level_pressure
# cloudcover: type: float, nazwa: cloud_cover, nulle -> 0
# visibility: type: float, nazwa: visibility
# solarradiation - type: float, nazwa: solar_radiation, nulle -> 0
# solarenergy -  type: float, nazwa: solar_energy, nulle -> 0
# uvindex - type: int, nazwa: uv_index, nulle -> 0
# severerisk: wywalic
# conditions: type: str, nazwa: conditions, nulle -> UNKNOWN
# icon - wywalic
# stations - wywalic

from .schemas import (
    COLUMNS_TO_DROP_WEATHER,
    COLUMNS_NULL_UNKNOWN_WEATHER,
    COLUMNS_NULL_NONE_WEATHER,
    COLUMNS_TO_FLOAT_WEATHER,
    COLUMNS_TO_INT_WEATHER,
)
from .utils import fill_na, change_type, replace_value, generate_surrogate_key
import pandas as pd


def trasform_weather(filepath_in: str) -> pd.DataFrame:
    df = pd.read_pickle(filepath_in)

    df = df.drop(columns=COLUMNS_TO_DROP_WEATHER)

    df = fill_na(df, ["winddir"], 0)

    # windir -> to str
    df["winddir"] = pd.cut(
        df["winddir"],
        bins=[0, 45, 90, 135, 180, 225, 270, 315, 360],
        labels=["N", "NE", "E", "SE", "S", "SW", "W", "NW"],
        include_lowest=True,
    )

    # String handling
    df = fill_na(df, COLUMNS_NULL_NONE_WEATHER, "NONE")
    df = fill_na(df, COLUMNS_NULL_UNKNOWN_WEATHER, "UNKNOWN")
    df = replace_value(df, COLUMNS_NULL_NONE_WEATHER, "", "NONE")
    df = replace_value(df, COLUMNS_NULL_UNKNOWN_WEATHER, "", "UNKNOWN")
    df = change_type(
        df, COLUMNS_NULL_NONE_WEATHER + COLUMNS_NULL_UNKNOWN_WEATHER, "string"
    )
    # Int handling
    df = fill_na(df, COLUMNS_TO_INT_WEATHER, -1)
    df = change_type(df, COLUMNS_TO_INT_WEATHER, "Int64")

    # Float handling
    df = fill_na(df, COLUMNS_TO_FLOAT_WEATHER, 0)
    df = change_type(df, COLUMNS_TO_FLOAT_WEATHER, "float32")

    # Date handling
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["datetime"] = df["datetime"].dt.round("H")
    df["date_id"] = df["datetime"].dt.strftime('%Y%m%d%H').astype(int)
    df.insert(1, 'date_id', df.pop('date_id'))

    df.insert(0, "WEATHER_KEY", df.apply(
        lambda row: generate_surrogate_key(row['date_id']), axis=1
    ))

    return df
