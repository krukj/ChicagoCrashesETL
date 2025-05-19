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
    return df[cols].astype(type)


def fill_na(df: pd.DataFrame, cols: List[str], value) -> pd.DataFrame:
    """Fill missing values in specified columns with a given value.

    Args:
        df (pd.DataFrame): Input DataFrame.
        cols (List[str]): A list of column names where NA values should be filled.
        value (_type_): The value used to replace NA values.

    Returns:
        pd.DataFrame: A DataFrame with NA values in the specified columns filled.
    """
    return df[cols].fillna(value)


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
    return df[cols].replace(value_in, value_out)
