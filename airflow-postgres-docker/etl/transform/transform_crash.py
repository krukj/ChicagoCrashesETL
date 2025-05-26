from .schemas import (
    COLUMNS_TO_DROP_CRASHES,
    COLUMNS_TO_STRING_CRASHES,
    COLUMNS_TO_INT_CRASHES,
    COLUMNS_TO_FLOAT_CRASHES,
    COLUMNS_TO_DATE,
    COLUMNS_TO_FACT_CRASH,
    COLUMNS_TO_DIM_CRASH_INFO,
    COLUMNS_TO_DIM_LOCATION,
)
from .utils import fill_na, change_type, replace_value, generate_surrogate_key

import pandas as pd
from typing import Tuple


def transform_crash(filepath_in: str) -> pd.DataFrame:
    df = pd.read_pickle(filepath_in)

    df = df.drop(columns=COLUMNS_TO_DROP_CRASHES)

    # String handling
    df = fill_na(df, COLUMNS_TO_STRING_CRASHES, "UNKNOWN")
    df = replace_value(df, COLUMNS_TO_STRING_CRASHES, "", "UNKNOWN")
    df = change_type(df, COLUMNS_TO_STRING_CRASHES, "string")

    # Int handling
    df = fill_na(df, COLUMNS_TO_INT_CRASHES, -1)
    # df = change_type(df, COLUMNS_TO_INT_CRASHES, "int")

    # Float handling
    df = fill_na(df, COLUMNS_TO_FLOAT_CRASHES, -999)
    df = change_type(df, COLUMNS_TO_FLOAT_CRASHES, "float32")

    # Date handling
    df["CRASH_DATETIME"] = pd.to_datetime(
        df["CRASH_DATE"], format="%m/%d/%Y %I:%M:%S %p"
    )

    df["CRASH_DATETIME_ROUNDED"] = df["CRASH_DATETIME"].dt.round("H")

    df["date_id"] = df["CRASH_DATETIME_ROUNDED"].dt.strftime("%Y%m%d%H").astype(int)
    df = df.drop(columns=["CRASH_DATETIME_ROUNDED"])
    df.insert(2, "date_id", df.pop("date_id"))
    return df


def split_crash(df) -> Tuple[pd.DataFrame, pd.DataFrame]:

    dim_crash_info = df[COLUMNS_TO_DIM_CRASH_INFO].drop_duplicates()
    dim_location = df[COLUMNS_TO_DIM_LOCATION].drop_duplicates()
    fact_crash = df[COLUMNS_TO_FACT_CRASH + ["date_id"]].drop_duplicates()

    return fact_crash, dim_crash_info, dim_location
