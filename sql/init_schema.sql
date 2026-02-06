-- Creates the Velib data warehouse schema in PostgreSQL

-- Stations master data
CREATE TABLE IF NOT EXISTS stations (
    station_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(255),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    capacity INTEGER,
    arrondissement VARCHAR(100),
    code_insee VARCHAR(10),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Station availability time-series
CREATE TABLE IF NOT EXISTS station_availability (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(20) REFERENCES stations(station_id),
    num_bikes_available INTEGER,
    num_bikes_mechanical INTEGER,
    num_bikes_ebike INTEGER,
    num_docks_available INTEGER,
    is_installed BOOLEAN,
    is_renting BOOLEAN,
    is_returning BOOLEAN,
    last_reported TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT NOW()
);

-- Index for time-series queries
CREATE INDEX IF NOT EXISTS idx_availability_station_time
ON station_availability(station_id, ingested_at);
