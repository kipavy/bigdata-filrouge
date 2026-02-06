-- Insert or update station master data

INSERT INTO stations (station_id, name, latitude, longitude, capacity, arrondissement, code_insee, updated_at)
VALUES %s
ON CONFLICT (station_id) DO UPDATE SET
    name = EXCLUDED.name,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    capacity = EXCLUDED.capacity,
    arrondissement = EXCLUDED.arrondissement,
    code_insee = EXCLUDED.code_insee,
    updated_at = NOW()
