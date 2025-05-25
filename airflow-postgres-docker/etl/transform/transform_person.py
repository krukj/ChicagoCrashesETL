# person_key - sztuczny klucz główny (jeszcze nie wiem czy tak to rozwiązujemy)
# PERSON_ID - mamy to w danych jakieś stringi to są typu O749947
# PERSON_TYPE - str (jeśli null UNKNOWN ale nie mo)
# mamy CRASH_RECORD_ID to trzeba będzie jakoś połączyć z CrashFact (jakiś hash)
# mamy też VEHICLE_ID (można to jakoś uwzględnić idk jeszcze) (to floaty ale chyba w praktyce inty większość albo wszystkie)
# SEX - M/F/X -> X zmieniamy na UNKNOWN
# AGE - floaty -> zamieniamy na inty ofc (tylko sprawdzić czy git wszystko) oraz 29% nulli -> no to jakieś -1 się da
# SAFETY_EQUIPMENT - str (jak null to "USAGE UNKNOWN")
# AIRBAG_DEPLOYED - str (jak null to "DEPLOYMENT UNKNOWN")
# EJECTION - str (jak null to "UNKNOWN")
# INJURY_CLASSIFICATION - str (jak null to "UNKNOWN")
# DRIVER_ACTION - str (jak null to "UNKNOWN")
# DRIVER_VISION - str (jak null to "UNKNOWN")
# PHYSICAL_CONDITION - str (jak null to "UNKNOWN")
# BAC_RESULT - str (jak null to "UNKNOWN")
# BAC_RESULT VALUE - float -> nulli jest 99% (wtedy damy albo -1 albo 0.0)

from .schemas import (
    COLUMNS_TO_DROP_PEOPLE,
    COLUMNS_TO_INT_PEOPLE,
    COLUMNS_TO_FLOAT_PEOPLE,
    COLUMNS_TO_STRING_PEOPLE,
)
from .utils import fill_na, change_type, replace_value, generate_surrogate_key
import datetime
import pandas as pd


def transform_person(filepath_in: str) -> pd.DataFrame:
    df = pd.read_pickle(filepath_in)

    df = df.drop(columns=COLUMNS_TO_DROP_PEOPLE)

    # String handling
    df = fill_na(df, COLUMNS_TO_STRING_PEOPLE, "UNKNOWN")
    df = replace_value(df, COLUMNS_TO_STRING_PEOPLE, "", "UNKNOWN")

    df = replace_value(df, ["SEX"], "X", "UNKNOWN")  # 🗿🗿🗿
    df = replace_value(df, ["SAFETY_EQUIPMENT"], "USAGE UNKNOWN", "UNKNOWN")
    df = replace_value(df, ["AIRBAG_DEPLOYED"], "DEPLOYMENT UNKNOWN", "UNKNOWN")

    df = change_type(df, COLUMNS_TO_STRING_PEOPLE, type="string")

    # Int handling
    df = fill_na(df, COLUMNS_TO_INT_PEOPLE, -1)
    df.loc[df["AGE"] < 0, "AGE"] = -1
    df = change_type(df, COLUMNS_TO_INT_PEOPLE, "Int64")

    # Float handling
    df = fill_na(df, COLUMNS_TO_FLOAT_PEOPLE, -999)
    df = change_type(df, COLUMNS_TO_FLOAT_PEOPLE, "float32")

    # Date handling
    df["CRASH_DATETIME"] = pd.to_datetime(
        df["CRASH_DATE"], format="%m/%d/%Y %I:%M:%S %p"
    )

    # Surogate key
    cols_to_surrogate = ["PERSON_ID", "CRASH_RECORD_ID"]
    df.insert(0, "PERSON_KEY", df.apply(
        lambda row: generate_surrogate_key(
            *[row[col] for col in cols_to_surrogate]
        ),
        axis=1
    ))

    df["CRASH_DATETIME_ROUNDED"] = df["CRASH_DATETIME"].dt.round("H")

    df["date_id"] = df["CRASH_DATETIME_ROUNDED"].dt.strftime("%Y%m%d%H").astype(int)
    df = df.drop(columns=["CRASH_DATETIME_ROUNDED", "CRASH_DATETIME"])
    df.insert(2, "date_id", df.pop("date_id"))

    # SCD2
    df["VALID_FROM"] = df["date_id"]
    df["VALID_TO"] = 9999123123
    df["IS_CURRENT"] = True

    return df
