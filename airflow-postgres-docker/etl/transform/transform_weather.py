from etl.logging_config import setup_logger

logger = setup_logger(__name__)
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
    df["date_id"] = df["datetime"].dt.strftime("%Y%m%d%H").astype(int)
    df.insert(1, "date_id", df.pop("date_id"))

    COLS_TO_KEY = ["date_id", "winddir"]
    df.insert(
        0,
        "WEATHER_KEY",
        df.apply(
            lambda row: generate_surrogate_key(*[row[col] for col in df.columns]),
            axis=1,
        ),
    )
    # df.insert(0, "WEATHER_KEY", df.apply(
    #     lambda row: generate_surrogate_key(row["date_id"])), axis=1
    # )

    return df
