# ChicagoCrashesETL
**Traffic crashes in Chicago — ETL and BI project.**  <br>
*By: [Julia Kruk](https://github.com/krukj) & [Tomasz Żywicki](https://github.com/tomaszzywicki)*

---

## 📊 Data sources

- [Traffic Crashes - Crashes](https://data.cityofchicago.org/Transportation/Traffic-Crashes-Crashes/85ca-t3if/about_data)  
- [Traffic Crashes - People](https://data.cityofchicago.org/Transportation/Traffic-Crashes-People/u6pd-qa9d/about_data)  
- [Traffic Crashes - Vehicles](https://data.cityofchicago.org/Transportation/Traffic-Crashes-Vehicles/68nd-jvt3/about_data)  
- [Visual Crossing Weather Data](https://www.visualcrossing.com/weather-history/Chicago,%20IL,%20United%20States/us/last15days/)

---

## 🗂️ Project structure



```
ChicagoCrashesETL
├── README.md                   # Project documentation
├── airflow-postgres-docker/    # Main ETL app with Airflow and Postgres
│   ├── Dockerfile              # Airflow Docker image definition
│   ├── docker-compose.yml      # Services configuration for Airflow and Postgres
│   ├── dags/                   # DAGs definitions for Airflow
│   │   ├── crash_etl_dag.py    # Main ETL DAG
│   │   └── example/            # Example/test DAGs and mock data
│   ├── data/                   # Raw and intermediate datasets
│   │   ├── crashes_data/       # Raw crash reports
│   │   ├── weather_data/       # Raw weather data
│   │   ├── tmp/                # Temporary data
│   │   │   ├── extracted/      # Data after extraction step
│   │   │   └── transformed/    # Data after transformation step
│   │   │       └── removed/    # Filtered-out records
│   ├── etl/                    # ETL pipeline code
│   │   ├── extract/            # Extraction logic and helpers
│   │   ├── transform/          # Data transformation functions
│   │   ├── load/               # Loading logic to DB or storage
│   │   ├── logging_config.py   # Logging setup
│   │   └── utils.py            # Utility functions shared across ETL
│   ├── logs/                   # Airflow and custom logs
├── data/                       # Global data directory 
├── docker_setup/               # Setup/config files for Docker environment
├── schemas/                    # SQL scripts for data warehouse schema
├── split_data.ipynb            # Notebook for testing data splitting
└── test.ipynb                  # General-purpose testing notebook

```


