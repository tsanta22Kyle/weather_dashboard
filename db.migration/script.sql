CREATE DATABASE meteo_db;
\c meteo_db;
-- ================================
-- 🌍 DIMENSION : Ville
-- ================================
CREATE TABLE IF NOT EXISTS dim_city (
    city_id SERIAL PRIMARY KEY,
    city_name TEXT NOT NULL,
    country TEXT,
    continent TEXT,
    latitude FLOAT,
    longitude FLOAT
);

-- ================================
-- 📅 DIMENSION : Date
-- ================================
CREATE TABLE IF NOT EXISTS dim_date (
    date_id INTEGER PRIMARY KEY, -- format YYYYMMDD
    full_date DATE,
    day INTEGER,
    month INTEGER,
    month_name TEXT,
    year INTEGER,
    season TEXT
);

-- ================================
-- 📊 TABLE DE FAITS : météo journalière
-- ================================
CREATE TABLE IF NOT EXISTS weather_fact (
    fact_id SERIAL PRIMARY KEY,
    date_id INTEGER REFERENCES dim_date(date_id),
    city_id INTEGER REFERENCES dim_city(city_id),
    temp_max FLOAT,
    precipitation FLOAT,
    wind_speed FLOAT,
    humidity FLOAT,
    sun_hours FLOAT,
    uv_index FLOAT,
    weather_score FLOAT,
    source TEXT , -- 'historical' ou 'realtime'
    unique(date_id, city_id, source)
);


CREATE TABLE IF NOT EXISTS city_weather_summary (
    id SERIAL PRIMARY KEY,
    city_id INTEGER REFERENCES dim_city(city_id),
    month INTEGER,
    month_name TEXT,
    season TEXT,
    year INTEGER,
    avg_temp FLOAT,
    avg_precipitation FLOAT,
    avg_humidity FLOAT,
    avg_wind FLOAT,
    avg_sun_hours FLOAT,
    avg_score FLOAT,
    source TEXT,
    UNIQUE (city_id, month, year, source)
);
