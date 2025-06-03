from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import os
import sys
import pandas as pd

##### SETUP PATHS #####
sys.path.insert(0, "/opt/airflow")
BASE_DIR = "/opt/airflow"
EXTRACTED_DIR = os.path.join(BASE_DIR, "data", "tmp", "extracted")
TRANSFORMED_DIR = os.path.join(BASE_DIR, "data", "tmp", "transformed")
REMOVED_DIR = os.path.join(TRANSFORMED_DIR, "removed")


##### EXTRACT #####
def extract_crashes():
    from etl.extract.extract import extract_crashes_csv

    input_path = os.path.join(
        BASE_DIR, "data", "crashes_data", "Traffic_Crashes_Crashes.csv"
    )
    output_path = os.path.join(EXTRACTED_DIR, "crashes.pkl")
    extract_crashes_csv(input_path, output_path)


def extract_people():
    from etl.extract.extract import extract_people_csv

    input_path = os.path.join(
        BASE_DIR, "data", "crashes_data", "Traffic_Crashes_People.csv"
    )
    output_path = os.path.join(EXTRACTED_DIR, "people.pkl")
    extract_people_csv(input_path, output_path)


def extract_vehicles():
    from etl.extract.extract import extract_vehicles_csv

    input_path = os.path.join(
        BASE_DIR, "data", "crashes_data", "Traffic_Crashes_Vehicles.csv"
    )
    output_path = os.path.join(EXTRACTED_DIR, "vehicles.pkl")
    extract_vehicles_csv(input_path, output_path)


def extract_weather():
    from etl.extract.extract import extract_weather_csv

    input_path = os.path.join(BASE_DIR, "data", "weather_data")
    output_path = os.path.join(EXTRACTED_DIR, "weather.pkl")
    extract_weather_csv(input_path, output_path)


##### TRANSFORM #####
def transform_crash_task():
    from etl.transform.transform_all import perform_transformation_crash

    fact, dim_info, dim_loc = perform_transformation_crash(
        os.path.join(EXTRACTED_DIR, "crashes.pkl")
    )
    fact.to_pickle(os.path.join(TRANSFORMED_DIR, "fact_crash.pkl"))
    dim_info.to_pickle(os.path.join(TRANSFORMED_DIR, "dim_crash_info.pkl"))
    dim_loc.to_pickle(os.path.join(TRANSFORMED_DIR, "dim_location.pkl"))


def transform_weather_task():
    from etl.transform.transform_all import perform_transformation_weather

    df = perform_transformation_weather(os.path.join(EXTRACTED_DIR, "weather.pkl"))
    df.to_pickle(os.path.join(TRANSFORMED_DIR, "fact_weather.pkl"))


def transform_vehicle_task():
    from etl.transform.transform_all import perform_transformation_vehicles

    df = perform_transformation_vehicles(os.path.join(EXTRACTED_DIR, "vehicles.pkl"))
    df.to_pickle(os.path.join(TRANSFORMED_DIR, "dim_vehicle.pkl"))


def transform_person_task():
    from etl.transform.transform_all import perform_transformation_person

    df = perform_transformation_person(os.path.join(EXTRACTED_DIR, "people.pkl"))
    df.to_pickle(os.path.join(TRANSFORMED_DIR, "dim_people.pkl"))


def transform_date_task():
    from etl.transform.transform_all import perform_make_dim_date

    df = perform_make_dim_date()
    df.to_pickle(os.path.join(TRANSFORMED_DIR, "dim_date.pkl"))


def transform_all_task():
    from etl.transform.transform_all import transform_all

    transform_all(
        df_fact_crash=pd.read_pickle(os.path.join(TRANSFORMED_DIR, "fact_crash.pkl")),
        df_fact_weather=pd.read_pickle(
            os.path.join(TRANSFORMED_DIR, "fact_weather.pkl")
        ),
        df_dim_crash_info=pd.read_pickle(
            os.path.join(TRANSFORMED_DIR, "dim_crash_info.pkl")
        ),
        df_dim_person=pd.read_pickle(os.path.join(TRANSFORMED_DIR, "dim_people.pkl")),
        df_dim_vehicle=pd.read_pickle(os.path.join(TRANSFORMED_DIR, "dim_vehicle.pkl")),
        df_dim_location=pd.read_pickle(
            os.path.join(TRANSFORMED_DIR, "dim_location.pkl")
        ),
        df_dim_date=pd.read_pickle(os.path.join(TRANSFORMED_DIR, "dim_date.pkl")),
        fact_crash_path_out=os.path.join(TRANSFORMED_DIR, "fact_crash.pkl"),
        fact_weather_path_out=os.path.join(TRANSFORMED_DIR, "fact_weather.pkl"),
        dim_crash_info_path_out=os.path.join(TRANSFORMED_DIR, "dim_crash_info.pkl"),
        dim_person_path_out=os.path.join(TRANSFORMED_DIR, "dim_people.pkl"),
        dim_vehicle_path_out=os.path.join(TRANSFORMED_DIR, "dim_vehicle.pkl"),
        dim_location_path_out=os.path.join(TRANSFORMED_DIR, "dim_location.pkl"),
        dim_date_path_out=os.path.join(TRANSFORMED_DIR, "dim_date.pkl"),
        removed_records_path=REMOVED_DIR,
    )


##### LOAD #####
def load_all_wrapper():
    from etl.load.load_all import main

    main()


##### DAG ARGS #####
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}


##### DAG #####
with DAG(
    "crash_etl_dag",
    default_args=default_args,
    description="ETL: Extract, Transform and Load data to POSTGRESQL",
    schedule_interval="@daily",
    catchup=False,
    tags=["crash_data", "etl"],
) as dag:

    ##### EXTRACT TASK GROUP #####
    with TaskGroup("extract_tasks", tooltip="Extract subtasks") as extract_tasks:
        extract_crashes_task = PythonOperator(
            task_id="extract_crashes", python_callable=extract_crashes
        )
        extract_people_task = PythonOperator(
            task_id="extract_people", python_callable=extract_people
        )
        extract_vehicles_task = PythonOperator(
            task_id="extract_vehicles", python_callable=extract_vehicles
        )
        extract_weather_task = PythonOperator(
            task_id="extract_weather", python_callable=extract_weather
        )

    ##### TRANSFORM TASK GROUP #####
    with TaskGroup("transform_tasks", tooltip="Transform subtasks") as transform_tasks:
        t_crash = PythonOperator(
            task_id="transform_crash", python_callable=transform_crash_task
        )
        t_weather = PythonOperator(
            task_id="transform_weather", python_callable=transform_weather_task
        )
        t_vehicle = PythonOperator(
            task_id="transform_vehicle", python_callable=transform_vehicle_task
        )
        t_person = PythonOperator(
            task_id="transform_person", python_callable=transform_person_task
        )
        t_date = PythonOperator(
            task_id="transform_date", python_callable=transform_date_task
        )
        t_all = PythonOperator(
            task_id="transform_all", python_callable=transform_all_task
        )

        [t_crash, t_weather, t_vehicle, t_person, t_date] >> t_all

    ##### LOAD #####
    load_task = PythonOperator(task_id="load_all", python_callable=load_all_wrapper)

    ##### DAG PIPELINE #####
    extract_tasks >> transform_tasks >> load_task
