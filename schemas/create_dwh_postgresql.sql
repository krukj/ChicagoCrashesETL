-- dim_date
CREATE TABLE
    dim_date (
        date_id INT PRIMARY KEY, -- Surrogate key (np. 20240507)
        full_date DATE NOT NULL,
        year SMALLINT NOT NULL,
        quarter SMALLINT NOT NULL,
        month SMALLINT NOT NULL,
        month_name VARCHAR(10) NOT NULL,
        day_of_month SMALLINT NOT NULL,
        day_of_week SMALLINT NOT NULL,
        day_name VARCHAR(10) NOT NULL,
        is_weekend BOOLEAN NOT NULL,
        week_of_year SMALLINT NOT NULL,
        is_holiday BOOLEAN NOT NULL DEFAULT FALSE,
        holiday_name VARCHAR(50)
    );

-- dim_crash_info
CREATE TABLE
    dim_crash_info (
        crash_info_id BIGINT PRIMARY KEY,
        traffic_control_device VARCHAR(50) NOT NULL,
        device_condition VARCHAR(50) NOT NULL,
        first_crash_type VARCHAR(50) NOT NULL,
        trafficway_type VARCHAR(50) NOT NULL,
        alignment VARCHAR(50) NOT NULL,
        roadway_surface_condition VARCHAR(50) NOT NULL,
        road_defect VARCHAR(50) NOT NULL,
        report_type VARCHAR(50),
        crash_type VARCHAR(50) NOT NULL,
        damage VARCHAR(50) NOT NULL,
        prime_contributory_cause VARCHAR(50) NOT NULL,
        second_contributory_cause VARCHAR(50) NOT NULL,
        most_severe_injury VARCHAR(50) NOT NULL,
        speedlimit INTEGER NOT NULL,
        travel_direction VARCHAR(10),
        maneuver VARCHAR(50),
        first_contact_point VARCHAR(50)
    );

-- dim_location
CREATE TABLE
    dim_location (
        location_id BIGINT PRIMARY KEY,
        street_no INTEGER NOT NULL,
        street_direction VARCHAR(1) NOT NULL,
        street_name VARCHAR(50) NOT NULL,
        latitude FLOAT,
        longitude FLOAT
    );

-- dim_person
CREATE TABLE
    dim_person (
        person_id BIGINT PRIMARY KEY,
        person_type VARCHAR(50) NOT NULL,
        zip_code VARCHAR(50),
        sex VARCHAR(1),
        age INTEGER,
        safety_equipment VARCHAR(50),
        airbag_deployed BOOLEAN,
        ejection VARCHAR(50),
        injury_classification VARCHAR(50),
        driver_action VARCHAR(50),
        driver_vision VARCHAR(50),
        physical_condition VARCHAR(50),
        bac_result VARCHAR(50),
        bac_result_value FLOAT,
        start_date_id INT NOT NULL,
        end_date_id INT NOT NULL,
        isCurrent BOOLEAN NOT NULL,
        CONSTRAINT fk_start_date FOREIGN KEY (start_date_id) REFERENCES dim_date (date_id),
        CONSTRAINT fk_end_date FOREIGN KEY (end_date_id) REFERENCES dim_date (date_id)
    );

-- dim_vehicle
CREATE TABLE
    dim_vehicle (
        vehicle_id BIGINT PRIMARY KEY,
        make VARCHAR(50) NOT NULL,
        model VARCHAR(50) NOT NULL,
        year INT NOT NULL,
        defect VARCHAR(50) NOT NULL,
        type VARCHAR(50) NOT NULL
    );

-- fact_crash
CREATE TABLE
    fact_crash (
        fact_crash_id BIGINT PRIMARY KEY,
        crash_date_id INT NOT NULL,
        date_police_notified_id INT NOT NULL,
        location_id BIGINT NOT NULL,
        vehicle_id BIGINT NOT NULL,
        person_id BIGINT NOT NULL,
        crash_info_id BIGINT NOT NULL,
        num_units INT NOT NULL,
        injuries_total INT NOT NULL,
        injuries_fatal INT NOT NULL,
        injuries_incapacitating INT NOT NULL,
        injuries_non_incapacitating INT NOT NULL,
        injuries_reported_not_evident INT NOT NULL,
        injuries_no_indication INT NOT NULL,
        injuries_unknown INT NOT NULL,
        CONSTRAINT fk_crash_info FOREIGN KEY (crash_info_id) REFERENCES dim_crash_info (crash_info_id),
        CONSTRAINT fk_crash_date_id FOREIGN KEY (crash_date_id) REFERENCES dim_date (date_id),
        CONSTRAINT fk_date_police_notified_id FOREIGN KEY (date_police_notified_id) REFERENCES dim_date (date_id),
        CONSTRAINT fk_location_id FOREIGN KEY (location_id) REFERENCES dim_location (location_id),
        CONSTRAINT fk_vehicle_id FOREIGN KEY (vehicle_id) REFERENCES dim_vehicle (vehicle_id),
        CONSTRAINT fk_person_id FOREIGN KEY (person_id) REFERENCES dim_person (person_id)
    );

-- fact_weather
CREATE TABLE
    fact_weather (
        weather_id BIGINT PRIMARY KEY,
        date_id INT NOT NULL,
        temp DECIMAL(5, 2) NOT NULL,
        feelslike DECIMAL(5, 2) NOT NULL,
        dew DECIMAL(5, 2) NOT NULL,
        humidity DECIMAL(5, 2) NOT NULL,
        precip DECIMAL(5, 2) NOT NULL,
        precipprob DECIMAL(5, 2) NOT NULL,
        preciptype VARCHAR(50),
        snow DECIMAL(5, 2) NOT NULL,
        snowdepth DECIMAL(5, 2) NOT NULL,
        windgust DECIMAL(5, 2),
        windspeed DECIMAL(5, 2),
        winddir INT NOT NULL,
        sealevelpressure DECIMAL(5, 2) NOT NULL,
        cloudcover DECIMAL(5, 2) NOT NULL,
        visibility DECIMAL(5, 2) NOT NULL,
        solarradiation INT NOT NULL,
        solarenergy DECIMAL(5, 2) NOT NULL,
        uvindex INT NOT NULL,
        conditions VARCHAR(50) NOT NULL,
        CONSTRAINT fk_date_id FOREIGN KEY (date_id) REFERENCES dim_date (date_id)
    );