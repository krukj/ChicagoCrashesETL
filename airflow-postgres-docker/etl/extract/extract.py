from schemas import EXPECTED_CRASHES_COLUMNS, EXPECTED_PEOPLE_COLUMNS, EXPECTED_VEHICLES_COLUMNS, EXPECTED_WEATHER_COLUMNS
from utils import validate_columns, print_null_summary

import pandas as pd
from pathlib import Path
import os


def extract_crashes_csv(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath)

    print("--------------------------------------------------------------------------------")
    validate_columns(df, EXPECTED_CRASHES_COLUMNS, "extract_crashes")
    print(f"[extract_crashes] Wczytano {len(df)} rekordów z {os.path.basename(filepath)}")
    print_null_summary(df, EXPECTED_CRASHES_COLUMNS, "extract_crashes")
    print("--------------------------------------------------------------------------------")

    return df


def extract_people_csv(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath, low_memory=False) # low_memory=False to avoid DtypeWarning

    print("--------------------------------------------------------------------------------")
    validate_columns(df, EXPECTED_PEOPLE_COLUMNS, "extract_people")
    print(f"[extract_people] Wczytano {len(df)} rekordów z {os.path.basename(filepath)}")
    print_null_summary(df, EXPECTED_PEOPLE_COLUMNS, "extract_people")
    print("--------------------------------------------------------------------------------")

    return df

def extract_vehicles_csv(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath, low_memory=False) # low_memory=False to avoid DtypeWarning
    print("--------------------------------------------------------------------------------")
    validate_columns(df, EXPECTED_VEHICLES_COLUMNS, "extract_vehicles")
    print(f"[extract_vehicles] Wczytano {len(df)} rekordów z {os.path.basename(filepath)}")
    print_null_summary(df, EXPECTED_VEHICLES_COLUMNS, "extract_vehicles")
    print("--------------------------------------------------------------------------------")

    return df

def extract_weather_csv(dirpath: str) -> pd.DataFrame:
    """
    Tutaj zakładamy że mamy katalog dirpath gdzie znajdują się pliki csv z pogodą.
    """

    df = pd.DataFrame()

    directory = Path(dirpath)
    print("--------------------------------------------------------------------------------")
    print(f"[extract_weather] Wczytano {len(list(directory.iterdir()))} plików z katalogu {directory}")
    for file in directory.iterdir():
        df_file = pd.read_csv(file)

        print(f"[extract_weather] Wczytano {len(df_file)} rekordów z {os.path.basename(file)}")

        df = pd.concat([df, df_file], ignore_index=True)
        print(f"[extract_weather] Połączono {len(df_file)} rekordów z {os.path.basename(file)} do df")
        
    print(f"[extract_weather] Połączono {len(df)} rekordów z katalogu {directory}")
    validate_columns(df, EXPECTED_WEATHER_COLUMNS, "extract_weather")
    print_null_summary(df, EXPECTED_WEATHER_COLUMNS, "extract_weather")
    print("--------------------------------------------------------------------------------")

    return df  
        

def main():
    # Example usage
    print(os.getcwd())
    print(__file__)

    weather_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "weather_data")
    crashes_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "crashes_data", "Traffic_Crashes_Crashes.csv")
    people_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "crashes_data", "Traffic_Crashes_People.csv")
    vehicles_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "crashes_data", "Traffic_Crashes_Vehicles.csv")

    # weather_df = extract_weather_csv(weather_path)
    # crashes_df = extract_crashes_csv(crashes_path)
    people_df = extract_people_csv(people_path)
    vehicles_df = extract_vehicles_csv(vehicles_path)


if __name__ == "__main__":
    main()
    
        
        