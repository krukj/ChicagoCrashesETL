import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from etl.logging_config import setup_logger

logger = setup_logger(__name__)
module_tag = "[fact_weather]"

def load_fact_weather(filepath_in) -> None:
    try:
        logger.info(f"{module_tag} Starting to load fact_weather from pickle.")
        fact_weather = pd.read_pickle(filepath_in)

        logger.info(f"{module_tag} Creating postgres connection.")
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS core;")

        logger.info(f"{module_tag} Dropping old tables.")
        # cursor.execute("DROP TABLE IF EXISTS staging.fact_weather;")
        # cursor.execute("DROP TABLE IF EXISTS core.fact_weather;")

        logger.info(f"{module_tag} Creating table (staging).")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS staging.fact_weather (
                weather_key NUMERIC PRIMARY KEY,
                datetime TIMESTAMP NOT NULL,
                date_id INT NOT NULL,
                temp FLOAT NOT NULL,
                feels_like FLOAT NOT NULL,
                dew FLOAT NOT NULL,
                humidity FLOAT NOT NULL,
                precip FLOAT NOT NULL,
                precip_prob INT NOT NULL,
                precip_type VARCHAR(100) NOT NULL,
                snow FLOAT NOT NULL,
                snow_depth FLOAT NOT NULL,
                wind_gust FLOAT NOT NULL,
                wind_speed FLOAT NOT NULL,
                wind_dir VARCHAR(10) NOT NULL,
                sea_level_pressure FLOAT NOT NULL,
                cloud_cover FLOAT NOT NULL,
                visibility FLOAT NOT NULL,
                solar_radiation FLOAT NOT NULL,
                solar_energy FLOAT NOT NULL,
                uv_index INT NOT NULL,
                conditions VARCHAR(100) NOT NULL,

                CONSTRAINT fk_date_id FOREIGN KEY (date_id) REFERENCES staging.dim_date (date_id)
            );
            """
        )

        cursor.execute("TRUNCATE staging.fact_weather;")

        logger.info(f"{module_tag} Inserting data into staging.")
        for _, row in fact_weather.iterrows():
            cursor.execute(
                """
                INSERT INTO staging.fact_weather (
                    weather_key, datetime, date_id, temp, feels_like, dew, humidity, precip, precip_prob,
                    precip_type, snow, snow_depth, wind_gust, wind_speed, wind_dir,
                    sea_level_pressure, cloud_cover, visibility, solar_radiation,
                    solar_energy, uv_index, conditions
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row["WEATHER_KEY"],
                    row["datetime"],
                    row["date_id"],
                    row["temp"],
                    row["feelslike"],
                    row["dew"],
                    row["humidity"],
                    row["precip"],
                    row["precipprob"],
                    row.get("preciptype", None),
                    row["snow"],
                    row["snowdepth"],
                    row["windgust"],
                    row["windspeed"],
                    row["winddir"],
                    row["sealevelpressure"],
                    row["cloudcover"],
                    row["visibility"],
                    row["solarradiation"],
                    row["solarenergy"],
                    row["uvindex"],
                    row["conditions"],
                ),
            )

        conn.commit()

        logger.info(f"{module_tag} Creating table (core).")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS core.fact_weather (
                weather_key NUMERIC PRIMARY KEY,
                datetime TIMESTAMP NOT NULL,
                date_id INT NOT NULL,
                temp FLOAT NOT NULL,
                feels_like FLOAT NOT NULL,
                dew FLOAT NOT NULL,
                humidity FLOAT NOT NULL,
                precip FLOAT NOT NULL,
                precip_prob INT NOT NULL,
                precip_type VARCHAR(100) NOT NULL,
                snow FLOAT NOT NULL,
                snow_depth FLOAT NOT NULL,
                wind_gust FLOAT NOT NULL,
                wind_speed FLOAT NOT NULL,
                wind_dir VARCHAR(10) NOT NULL,
                sea_level_pressure FLOAT NOT NULL,
                cloud_cover FLOAT NOT NULL,
                visibility FLOAT NOT NULL,
                solar_radiation FLOAT NOT NULL,
                solar_energy FLOAT NOT NULL,
                uv_index INT NOT NULL,
                conditions VARCHAR(100) NOT NULL,

                CONSTRAINT fk_date_id FOREIGN KEY (date_id) REFERENCES core.dim_date (date_id)
            );
            """
        )

        cursor.execute("TRUNCATE core.fact_weather;")

        logger.info(f"{module_tag} Copying data to core.")
        cursor.execute(
            """
            INSERT INTO core.fact_weather (
                weather_key, datetime, date_id, temp, feels_like, dew, humidity, precip, precip_prob,
                precip_type, snow, snow_depth, wind_gust, wind_speed, wind_dir,
                sea_level_pressure, cloud_cover, visibility, solar_radiation,
                solar_energy, uv_index, conditions
            )
            SELECT 
                weather_key, datetime, date_id, temp, feels_like, dew, humidity, precip, precip_prob,
                precip_type, snow, snow_depth, wind_gust, wind_speed, wind_dir,
                sea_level_pressure, cloud_cover, visibility, solar_radiation,
                solar_energy, uv_index, conditions
            FROM staging.fact_weather;
            """
        )

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"{module_tag} Successfully inserted fact_weather into database.")

    except Exception as e:
        logger.error(f"{module_tag} Error in load_fact_weather: {str(e)}")
        raise
