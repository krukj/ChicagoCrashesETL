from .schemas import (
    COLUMNS_TO_DROP_CRASHES,
    COLUMNS_TO_STRING_CRASHES,
    COLUMNS_TO_INT_CRASHES,
    COLUMNS_TO_FLOAT_CRASHES,
    COLUMNS_TO_DATE,
    COLUMNS_TO_FACT_CRASH,
    COLUMNS_TO_DIM_CRASH_INFO,
)
from .utils import fill_na, change_type, replace_value, generate_surrogate_key

import pandas as pd
from pathlib import Path

# nazwy no to po prostu zmieniamy na snake_case małe litery
# jak str są '' to UNKNOWN

# CRASH_RECORD_ID
# CRASH_DATE_EST_I       skip
# CRASH_DATE             format git -> na klucz obcy do dim_date będzie zamianka
# POSTED_SPEED_LIMIT     na int, jak null to -1
# TRAFFIC_CONTROL_DEVICE str, jak null to UNKNOWN
# DEVICE_CONDITION       str, jak null to UNKNOWN
# WEATHER_CONDITION      str, jak null to UNKNOWN
# LIGHTING_CONDITION     str, jak null to UNKNOWN
# FIRST_CRASH_TYPE       str, jak null to UNKNOWN
# TRAFFICWAY_TYPE        str, jak null to UNKNOWN
# LANE_CNT               wyjebać
# ALIGNMENT              str, jak null to UNKNOWN
# ROADWAY_SURFACE_COND   str, jak null to UNKNOWN
# ROAD_DEFECT            str, jak null to UNKNOWN
# REPORT_TYPE           str, jak null to UNKNOWN
# CRASH_TYPE             str, jak null to UNKNOWN
# INTERSECTION_RELATED_I  wyjebać
# NOT_RIGHT_OF_WAY_I     wyjebać
# HIT_AND_RUN_I          wyjebać
# DAMAGE                 str, jak null to UNKNOWN
# DATE_POLICE_NOTIFIED   format git -> na klucz obcy do dim_date będzie zamiana
# PRIM_CONTRIBUTORY_CAUSE str, jak null to UNKNOWN
# SEC_CONTRIBUTORY_CAUSE str, jak null to UNKNOWN
# STREET_NO              int, jak null to -1
# STREET_DIRECTION       str, jak null to UNKNOWN
# STREET_NAME            str, jak null to UNKNOWN
# BEAT_OF_OCCURRENCE     int jak null to -1  (nie wiem co to nie wiem czy tu wgl dajemy)
# PHOTOS_TAKEN_I         wyjebać
# STATEMENTS_TAKEN_I     wyjebać
# DOORING_I             wyjebać
# WORK_ZONE_I           wyjebać
# WORK_ZONE_TYPE        wyjebać
# WORKERS_PRESENT_I     wyjebać
# NUM_UNITS             int, jak null to -1
# MOST_SEVERE_INJURY    str jak null to UNKNOWN
# INJURIES_TOTAL        int jak null to -1
# INJURIES_FATAL        int jak null to -1
# INJURIES_INCAPACITATING        int jak null to -1
# INJURIES_NON_INCAPACITATING    int jak null to -1
# INJURIES_REPORTED_NOT_EVIDENT  int jak null to -1
# INJURIES_NO_INDICATION         int jak null to -1
# INJURIES_UNKNOWN              int jak null to -1
# CRASH_HOUR                    wyjebać
# CRASH_DAY_OF_WEEK             wyjebać
# CRASH_MONTH                   wyjebać
# LATITUDE                  float, jak null to -1
# LONGITUDE                 float, jak null to -1
# LOCATION                  wyjebać


def transform_crash(filepath_in: str) -> pd.DataFrame:
    df = pd.read_pickle(filepath_in)

    # df.columns = df.columns.str.strip().str.lower()

    df = df.drop(columns=COLUMNS_TO_DROP_CRASHES)

    # String handling
    df = fill_na(df, COLUMNS_TO_STRING_CRASHES, "UNKNOWN")
    df = replace_value(df, COLUMNS_TO_STRING_CRASHES, "", "UNKNOWN")
    df = change_type(df, COLUMNS_TO_STRING_CRASHES, "string")

    # Int handling
    df = fill_na(df, COLUMNS_TO_INT_CRASHES, -1)
    df = change_type(df, COLUMNS_TO_INT_CRASHES, "Int64")

    # Float handling
    df = fill_na(df, COLUMNS_TO_FLOAT_CRASHES, -999)
    df = change_type(df, COLUMNS_TO_FLOAT_CRASHES, "float32")

    return df
    # TODO coś tam z datą pokminić jak najsensowniej


def split_crash(df) -> tuple[pd.DataFrame, pd.DataFrame]:

    dim_crash_info = df[COLUMNS_TO_DIM_CRASH_INFO].drop_duplicates()
    fact_crash = df[COLUMNS_TO_FACT_CRASH].drop_duplicates()

    # generating surrogate keys
    fact_crash["FACT_CRASH_KEY"] = fact_crash.apply(
        lambda row: generate_surrogate_key(row["CRASH_RECORD_ID"]), axis=1
    )
    dim_crash_info["CRASH_INFO_KEY"] = dim_crash_info.apply(
        lambda row: generate_surrogate_key(
            *[row[col] for col in COLUMNS_TO_DIM_CRASH_INFO]
        ),
        axis=1,
    )

    return fact_crash, dim_crash_info
