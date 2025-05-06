// fact_crash
CREATE TABLE fact_crash (
    fact_crash_id NVARCHAR(150) NOT NULL PRIMARY KEY, // id maja dlugosc ponad 120 
    crash_date_id NVARCHAR(150) NOT NULL,
    date_police_notified_id NVARCHAR(150) NOT NULL,
    location_id NVARCHAR(150) NOT NULL, // trzeba stworzyc surrogate key, moze zmniejszyc wtedy dlugosc id? 
    vehicle_id NVARCHAR(150) NOT NULL,
    person_id NVARCHAR(150) NOT NULL,
    crash_info_id NVARCHAR(150) NOT NULL,

    num_units INTEGER NOT NULL,  
    injuries_total INTEGER NOT NULL, 
    injuries_fatal INTEGER NOT NULL, 
    injuries_incapacitating INTEGER NOT NULL, 
    injuries_non_incapacitating INTEGER NOT NULL, 
    injuries_reported_not_evident INTEGER NOT NULL, 
    injuries_no_indication INTEGER NOT NULL, 
    injuries_unknown INTEGER NOT NULL, 

    CONSTRAINT fk_crash_info
        FOREIGN KEY (crash_info_id)
        REFERENCES dim_crash_info(crash_info_id)

    CONSTRAINT fk_crash_date_id
        FOREIGN KEY (crash_date_id)
        REFERENCES dim_date(date_id)

    CONSTRAINT fk_date_police_notified_id
        FOREIGN KEY (date_police_notified_id)
        REFERENCES dim_date(date_id)

    CONSTRAINT fk_location_id
        FOREIGN KEY (location_id)
        REFERENCES dim_location(location_id)

    CONSTRAINT fk_vehicle_id
        FOREIGN KEY (vehicle_id)
        REFERENCES dim_vehicle(vehicle_id)
    

);

// dim_crash_info
CREATE TABLE dim_crash_info (
    crash_info_id NVARCHAR(150) NOT NULL PRIMARY KEY,
    traffic_control_device NVARCHAR(50) NOT NULL,
    device_condition NVARCHAR(50) NOT NULL,
    first_crash_type NVARCHAR(50) NOT NULL,
    trafficway_type NVARCHAR(50) NOT NULL,
    alignment NVARCHAR(50) NOT NULL,
    roadway_surface_condition NVARCHAR(50) NOT NULL,
    road_defect NVARCHAR(50) NOT NULL,
    report_type NVARCHAR(50) NULL, // w sensie jest troche missing to null ?
    crash_type NVARCHAR(50) NOT NULL,
    damage NVARCHAR(50) NOT NULL,
    prime_contributory_cause NVARCHAR(50) NOT NULL,
    second_contributory_cause NVARCHAR(50) NOT NULL,
    most_severe_injury NVARCHAR(50) NOT NULL,
    speedlimit INTEGER NOT NULL,
    intersection_related NVARCHAR(50) NULL, // bardzo duzo missing, moze wywalic ?
    hit_and_run NVARCHAR(50) NULL, // to samo
    photos_taken NVARCHAR(50) NULL, // to samo
    statements_taken NVARCHAR(50) NULL // to samo


);


// dim_location
CREATE TABLE dim_location (
  location_id NVARCHAR(150) NOT NULL PRIMARY KEY, 
  street_no INTEGER NOT NULL,
  street_direction VARCHAR(1) NOT NULL,
  street_name NVARCHAR(50) NOT NULL,
  latitude FLOAT NULL,
  longitude FLOAT NULL
);

// dim_person
CREATE TABLE dim_person (
  person_id NVARCHAR(150) NOT NULL PRIMARY KEY,
  person_type NVARCHAR(50) NOT NULL,
  city NVARCHAR(50) NULL, // city of residence
  state NVARCHAR(50) NULL, 
  zip_code  NVARCHAR(50) NULL, 
  sex VARCHAR(1) NULL,
  age INTEGER NULL,
  safety_equipment NVARCHAR(50) NULL,
  airbag_deployed BIT NULL,
  ejection NVARCHAR(50) NULL,
  injury_classification NVARCHAR(50) NULL,
  driver_action NVARCHAR(50) NULL,
  driver_vision NVARCHAR(50) NULL,
  physical_condition NVARCHAR(50) NULL,
  bac_result NVARCHAR(50) NULL,
  bac_result_value FLOAT NULL
)

// dim_vehicle
CREATE TABLE dim_vehicle ()

// fact_weather
CREATE TABLE fact_weather ()
// moze tutaj dodac kolumne lighting_condition z crashes ?


// dim_date
CREATE TABLE dim_date ()


// KOMENTARZE
// - moze zastanowic sie czy by nie wywalic czesci kolumn ktore beda bezuzyteczne
// - wywalic kolumny gdzie jest duzo missingow? np intersection_related
// - ew zmniejszyc liczbe w NVARCHAR(50) na mniejsza ale idk 
