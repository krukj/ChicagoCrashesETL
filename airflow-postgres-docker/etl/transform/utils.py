import hashlib
import pandas as pd
from typing import List


def change_type(df: pd.DataFrame, cols: List[str], type: str) -> pd.DataFrame:
    """
    Changes the data type of specified columns in a DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame.
        cols (List[str]): List of column names to change the data type.
        type (str): Desired data type to convert the columns to.

    Returns:
        pd.DataFrame: DataFrame with specified columns converted to the desired data type.
    """
    df_copy = df.copy()
    for col in cols:
        if col in df_copy.columns:
            df_copy[col] = df_copy[col].astype(type)
    return df_copy


def fill_na(df: pd.DataFrame, cols: List[str], value) -> pd.DataFrame:
    """Fill missing values in specified columns with a given value.

    Args:
        df (pd.DataFrame): Input DataFrame.
        cols (List[str]): A list of column names where NA values should be filled.
        value (_type_): The value used to replace NA values.

    Returns:
        pd.DataFrame: A DataFrame with NA values in the specified columns filled.
    """
    df_copy = df.copy()
    for col in cols:
        if col in df_copy.columns:
            df_copy[col] = df_copy[col].fillna(value)
    return df_copy


def replace_value(
    df: pd.DataFrame, cols: List[str], value_in, value_out
) -> pd.DataFrame:
    """Replaces specified values with a given value_out.

    Args:
        df (pd.DataFrame): Input DataFrame.
        cols (List[str]): A list of column names where values should be changed.
        value_in (_type_): The value to be changed.
        value_out (_type_): The value used to replace value_in.

    Returns:
        pd.DataFrame: A DataFrame with changed value_in to value_out.
    """
    df_copy = df.copy()
    for col in cols:
        if col in df_copy.columns:
            df_copy[col] = df_copy[col].replace(value_in, value_out)
    return df_copy


def generate_surrogate_key(*args) -> int:
    """
    Generates a surrogate key based on the input arguments.

    Args:
        *args: Variable number of arguments to generate the key from.

    Returns:
        int: The generated surrogate key.
    """
    # example use:
    # dim_crash_info_df["crash_info_id"] = dim_crash_info_df.apply(lambda row: generate_surrogate_key(*[row[col] for col in crash_info_cols]), axis=1)

    key_string = "|".join(str(arg).lower().strip() for arg in args)
    hash_bytes = hashlib.sha256(key_string.encode("utf-8")).digest()

    surrogate_key = int.from_bytes(hash_bytes[:8], byteorder="big", signed=False)

    return surrogate_key
