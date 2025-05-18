import pandas as pd

def validate_columns(df: pd.DataFrame, expected_cols: set, name: str):
    missing = expected_cols - set(df.columns)
    if missing:
        raise ValueError(f"[{name}] Missing expected columns: {missing}")

def print_null_summary(df: pd.DataFrame, columns: set, name: str):
    print(f"[{name}] Null summary:")
    summary = df[list(columns)].isnull().sum()
    print(summary[summary > 0].sort_values(ascending=False))