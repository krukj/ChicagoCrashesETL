from .schemas import (
    COLUMNS_TO_DROP_VEHICLES,
    COLUMNS_TO_STRING_VEHICLES,
    COLUMNS_TO_INT_VEHICLES,
)
from .utils import fill_na, change_type, replace_value, generate_surrogate_key
import pandas as pd


def transform_vehicle(filepath_in: str) -> pd.DataFrame:
    df = pd.read_pickle(filepath_in)

    df = df.drop(columns=COLUMNS_TO_DROP_VEHICLES)

    # String handling
    df = fill_na(df, COLUMNS_TO_STRING_VEHICLES, "UKNOWN")
    df = replace_value(df, COLUMNS_TO_STRING_VEHICLES, "", "UNKNOWN")
    df = change_type(df, COLUMNS_TO_STRING_VEHICLES, "string")

    # Int handling
    df = fill_na(df, COLUMNS_TO_INT_VEHICLES, -1)
    df["OCCUPANT_CNT"] = df["OCCUPANT_CNT"] + 1
    df = change_type(df, COLUMNS_TO_INT_VEHICLES, "int64")

    df["VEHICLE_ID"] = df["CRASH_UNIT_ID"].astype("int64")

    vehicle_key_cols = df.columns.to_list()

    df["VEHICLE_KEY"] = df.apply(
        lambda row: generate_surrogate_key(*[row[col] for col in vehicle_key_cols]),
        axis=1,
    )
    return df
