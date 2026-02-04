"""
traitement.py - Transform data from MongoDB and load into PostgreSQL
"""
import os
from datetime import datetime
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values


def get_mongo_client():
    """Create MongoDB connection."""
    host = os.getenv("MONGO_HOST", "mongodb")
    port = os.getenv("MONGO_PORT", "27017")
    user = os.getenv("MONGO_USER", "mongo")
    password = os.getenv("MONGO_PASSWORD", "mongo")

    uri = f"mongodb://{user}:{password}@{host}:{port}/"
    return MongoClient(uri)


def get_postgres_connection():
    """Create PostgreSQL connection."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        database=os.getenv("DB_NAME", "airflow"),
        user=os.getenv("DB_USER", "airflow"),
        password=os.getenv("DB_PASSWORD", "airflow"),
        port=5432
    )


def init_postgres_tables():
    """Initialize PostgreSQL tables if they don't exist."""
    conn = get_postgres_connection()
    cur = conn.cursor()

    # Create stations table
    cur.execute("""
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
    """)

    # Create station_availability table
    cur.execute("""
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
    """)

    # Create index for time-series queries
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_availability_station_time
        ON station_availability(station_id, ingested_at);
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("PostgreSQL tables initialized.")


def get_latest_from_mongodb():
    """Retrieve the most recent document from MongoDB collection."""
    client = get_mongo_client()
    db = client[os.getenv("MONGO_DB", "velib_datalake")]
    collection = db["velib_raw"]

    # Get the most recent document
    document = collection.find_one(
        sort=[("ingested_at", -1)]
    )
    client.close()
    return document


def transform_data(raw_data: dict) -> tuple:
    """Transform OpenDataSoft Velib data into stations and availability records."""
    records = raw_data.get("data", {}).get("records", [])

    stations = []
    availability = []

    for record in records:
        fields = record.get("fields", {})

        station_id = fields.get("stationcode", "")
        if not station_id:
            continue

        # Extract coordinates
        coords = fields.get("coordonnees_geo", [0, 0])
        lat = coords[0] if len(coords) > 0 else 0
        lon = coords[1] if len(coords) > 1 else 0

        # Station info
        stations.append({
            "station_id": station_id,
            "name": fields.get("name", ""),
            "latitude": lat,
            "longitude": lon,
            "capacity": fields.get("capacity", 0),
            "arrondissement": fields.get("nom_arrondissement_communes", ""),
            "code_insee": fields.get("code_insee_commune", "")
        })

        # Parse last_reported timestamp
        duedate = fields.get("duedate")
        if duedate:
            try:
                last_reported = datetime.fromisoformat(duedate.replace("+00:00", ""))
            except:
                last_reported = datetime.utcnow()
        else:
            last_reported = datetime.utcnow()

        # Availability info
        availability.append({
            "station_id": station_id,
            "num_bikes_available": fields.get("numbikesavailable", 0),
            "num_bikes_mechanical": fields.get("mechanical", 0),
            "num_bikes_ebike": fields.get("ebike", 0),
            "num_docks_available": fields.get("numdocksavailable", 0),
            "is_installed": fields.get("is_installed", "NON") == "OUI",
            "is_renting": fields.get("is_renting", "NON") == "OUI",
            "is_returning": fields.get("is_returning", "NON") == "OUI",
            "last_reported": last_reported
        })

    return stations, availability


def load_stations_to_postgres(stations: list):
    """Upsert station information to PostgreSQL."""
    if not stations:
        print("No stations to load.")
        return 0

    conn = get_postgres_connection()
    cur = conn.cursor()

    # Upsert stations (insert or update on conflict)
    query = """
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
    """

    values = [
        (s["station_id"], s["name"], s["latitude"], s["longitude"],
         s["capacity"], s["arrondissement"], s["code_insee"], datetime.utcnow())
        for s in stations
    ]

    execute_values(cur, query, values)
    conn.commit()

    count = len(values)
    cur.close()
    conn.close()
    print(f"Loaded {count} stations to PostgreSQL.")
    return count


def load_availability_to_postgres(availability: list):
    """Insert station availability records to PostgreSQL."""
    if not availability:
        print("No availability data to load.")
        return 0

    conn = get_postgres_connection()
    cur = conn.cursor()

    query = """
        INSERT INTO station_availability
        (station_id, num_bikes_available, num_bikes_mechanical, num_bikes_ebike,
         num_docks_available, is_installed, is_renting, is_returning, last_reported)
        VALUES %s
    """

    values = [
        (
            a["station_id"],
            a["num_bikes_available"],
            a["num_bikes_mechanical"],
            a["num_bikes_ebike"],
            a["num_docks_available"],
            a["is_installed"],
            a["is_renting"],
            a["is_returning"],
            a["last_reported"]
        )
        for a in availability
    ]

    execute_values(cur, query, values)
    conn.commit()

    count = len(values)
    cur.close()
    conn.close()
    print(f"Loaded {count} availability records to PostgreSQL.")
    return count


def transform_and_load():
    """Main transformation and loading function."""
    print(f"[{datetime.utcnow()}] Starting data transformation and loading...")

    # Initialize tables
    init_postgres_tables()

    # Get latest data from MongoDB
    print("Retrieving data from MongoDB...")
    raw_doc = get_latest_from_mongodb()

    if not raw_doc:
        print("No data found in MongoDB. Skipping transformation.")
        return {"error": "No data in MongoDB"}

    # Transform data
    print("Transforming data...")
    stations, availability = transform_data(raw_doc)

    if not stations:
        print("No stations data to process.")
        return {"error": "No stations in data"}

    # Load to PostgreSQL
    print("Loading to PostgreSQL...")
    stations_count = load_stations_to_postgres(stations)
    availability_count = load_availability_to_postgres(availability)

    result = {
        "transformation_time": datetime.utcnow().isoformat(),
        "stations_loaded": stations_count,
        "availability_records_loaded": availability_count
    }

    print(f"Transformation and loading complete: {result}")
    return result


if __name__ == "__main__":
    result = transform_and_load()
    print(f"Result: {result}")
