-- dim_crash_info
CREATE TABLE
    dim_crash_info (
        crash_info_id BIGINT NOT NULL PRIMARY KEY,
        traffic_control_device NVARCHAR (50) NOT NULL,
        device_condition NVARCHAR (50) NOT NULL,
        first_crash_type NVARCHAR (50) NOT NULL,
        trafficway_type NVARCHAR (50) NOT NULL,
        alignment NVARCHAR (50) NOT NULL,
        roadway_surface_condition NVARCHAR (50) NOT NULL,
        road_defect NVARCHAR (50) NOT NULL,
        report_type NVARCHAR (50) NULL,
        -- w sensie jest troche missing to null ?
        crash_type NVARCHAR (50) NOT NULL,
        damage NVARCHAR (50) NOT NULL,
        prime_contributory_cause NVARCHAR (50) NOT NULL,
        second_contributory_cause NVARCHAR (50) NOT NULL,
        most_severe_injury NVARCHAR (50) NOT NULL,
        speedlimit INTEGER NOT NULL,
        -- te dalej to z vehicle
        travel_direction NVARCHAR (10),
        maneuver NVARCHAR (50),
        first_contact_point NVARCHAR (50)
    );

-- dim_location
CREATE TABLE
    dim_location (
        location_id BIGINT NOT NULL PRIMARY KEY,
        street_no INTEGER NOT NULL,
        street_direction VARCHAR(1) NOT NULL,
        street_name NVARCHAR (50) NOT NULL,
        latitude FLOAT NULL,
        longitude FLOAT NULL
    );

-- dim_person
CREATE TABLE
    dim_person (
        person_id BIGINT NOT NULL PRIMARY KEY,
        person_type NVARCHAR (50) NOT NULL,
        zip_code NVARCHAR (50) NULL,
        sex VARCHAR(1) NULL,
        age INTEGER NULL,
        safety_equipment NVARCHAR (50) NULL,
        airbag_deployed BIT NULL,
        ejection NVARCHAR (50) NULL,
        injury_classification NVARCHAR (50) NULL,
        driver_action NVARCHAR (50) NULL,
        driver_vision NVARCHAR (50) NULL,
        physical_condition NVARCHAR (50) NULL,
        bac_result NVARCHAR (50) NULL,
        bac_result_value FLOAT NULL,
        -- tutaj wlatuje SCD2
        start_date_id INT NOT NULL,
        end_date_id INT NOT NULL,
        isCurrent bit NOT NULL,
        CONSTRAINT fk_start_date FOREIGN KEY (start_date_id) REFERENCES dim_date (date_id),
        CONSTRAINT fk_end_date FOREIGN KEY (end_date_id) REFERENCES dim_date (date_id)
    )
    --dim_vehicle
CREATE TABLE
    dim_vehicle (
        vehicle_id BIGINT NOT NULL PRIMARY KEY,
        make NVARCHAR (50) NOT NULL,
        model NVARCHAR (50) NOT NULL,
        year INT NOT NULL,
        defect NVARCHAR (50) NOT NULL,
        type NVARCHAR (50) NOT NULL
    )
CREATE TABLE
    dim_date (
        date_id INT NOT NULL PRIMARY KEY, -- Surrogate key (np. 20240507)
        full_date DATE NOT NULL, -- Pełna data (YYYY-MM-DD)
        year SMALLINT NOT NULL, -- Rok (np. 2024)
        quarter TINYINT NOT NULL, -- Kwartał (1-4)
        month TINYINT NOT NULL, -- Miesiąc (1-12)
        month_name NVARCHAR (10) NOT NULL, -- Nazwa miesiąca (np. 'May')
        day_of_month TINYINT NOT NULL, -- Dzień miesiąca (1-31)
        day_of_week TINYINT NOT NULL, -- Dzień tygodnia (1=Monday, 7=Sunday)
        day_name NVARCHAR (10) NOT NULL, -- Nazwa dnia tygodnia (np. 'Tuesday')
        is_weekend BIT NOT NULL, -- Czy to weekend (1/0)
        week_of_year TINYINT NOT NULL, -- Numer tygodnia w roku (1-53)
        is_holiday BIT NOT NULL DEFAULT 0, -- Czy to święto (0 domyślnie)
        holiday_name NVARCHAR (50) NULL -- Nazwa święta jeśli występuje
    );

-- fact_crash
CREATE TABLE
    fact_crash (
        fact_crash_id BIGINT NOT NULL PRIMARY KEY, -- id maja dlugosc ponad 120 
        crash_date_id INT NOT NULL,
        date_police_notified_id INT NOT NULL,
        location_id BIGINT NOT NULL,
        -- trzeba stworzyc surrogate key,moze zmniejszyc wtedy dlugosc id ? 
        -- co? ~ Tomek
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

--fact_weather
CREATE TABLE
    fact_weather (
        weather_id BIGINT NOT NULL PRIMARY KEY,
        date_id INT NOT NULL,
        temp decimal(3, 2) NOT NULL,
        feelslike decimal(3, 2) NOT NULL,
        dew decimal(3, 2) NOT NULL,
        humidity decimal(3, 2) NOT NULL,
        precip decimal(3, 2) NOT NULL,
        precipprob decimal(3, 2) NOT NULL,
        preciptype NVARCHAR (50) NULL,
        snow decimal(3, 2) NOT NULL,
        snowdepth decimal(3, 2) NOT NULL,
        windgust decimal(3, 2) NULL,
        windspeed decimal(3, 2) NULL,
        winddir INT NOT NULL,
        sealevelpressure decimal(5, 2) NOT NULL,
        cloudcover decimal(5, 2) NOT NULL,
        visibility decimal(5, 2) NOT NULL,
        solarradiation int NOT NULL,
        solarenergy decimal(3, 2) NOT NULL,
        uvindex int NOT NULL,
        conditions NVARCHAR (50) NOT NULL,
        CONSTRAINT fk_date_id FOREIGN KEY (date_id) REFERENCES dim_date (date_id)
    )
    -- KOMENTARZE 
    -- moze zastanowic sie czy by nie wywalic czesci kolumn ktore beda bezuzyteczne 
    -- - wywalic kolumny gdzie jest duzo missingow ? np intersection_related 
    -- - ew zmniejszyc liczbe w NVARCHAR (50) na mniejsza ale idk