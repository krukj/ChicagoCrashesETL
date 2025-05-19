import pandas as pd
from typing import List

def change_type(df: pd.DataFrame, cols: List[str], type: str) -> pd.DataFrame:
    return df[cols].astype(type)

def fill_na(df: pd.DataFrame, cols: List[str], value) -> pd.DataFrame:
    return df[cols].fillna(value)

def replace_value(df: pd.DataFrame, cols: List[str], value_in, value_out) -> pd.DataFrame:
    return df[cols].replace(value_in, value_out)