# fact_weather_id: surrogate key
# date_id: dodac klucz łączący z dim_date
# name: wywalic
# datetime: na jej podstawie date_id
# temp: type: float, nazwa: temp
# feelslike: type: float, nazwa: feels_like
# dew: type: float, nazwa: dew
# humidity: type: float, nazwa: humidity
# precip: type: float, nazwa: precip, nulle -> 0
# precipprob: type: int, nazwa: precip_prob, chyba ma tylko distinct 0 i 100
# preciptype: type: str, nazwa: precip_type, nulle jako NONE moze zeby bylo ze nie bylo opadu 
# snow: type: float, nazwa: snow, nulle -> 0
# snowdepth: type: float, nazwa: snow_depth, nulle -> 0
# windgust: type: float, nazwa: wind_gust, nulle -> 0
# windspeed: type: float, nazwa: wind_speed, nulle -> 0
# winddir: jest w katach, zamienic na kierunek typu N, S, type: str, nazwa: wind_dir
# sealevelpressure: type: float, nazwa: sea_level_pressure
# cloudcover: type: float, nazwa: cloud_cover, nulle -> 0
# visibility: type: float, nazwa: visibility
# solarradiation - type: float, nazwa: solar_radiation, nulle -> 0
# solarenergy -  type: float, nazwa: solar_energy, nulle -> 0
# uvindex - type: int, nazwa: uv_index, nulle -> 0
# severerisk: wywalic
# conditions: type: str, nazwa: conditions, nulle -> UNKNOWN
# icon - wywalic
# stations - wywalic