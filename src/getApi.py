"""
getApi.py - Extract data from Velib OpenDataSoft API and store in MongoDB (Data Lake)
"""

import os
from datetime import datetime

import requests
from pymongo import MongoClient

# API OpenDataSoft (plus fiable que smoove.pro)
VELIB_API_URL = "https://data.opendatasoft.com/api/records/1.0/search/"
DATASET = "velib-disponibilite-en-temps-reel@parisdata"


def get_mongo_client():
    """Create MongoDB connection."""
    host = os.getenv("MONGO_HOST", "mongodb")
    port = os.getenv("MONGO_PORT", "27017")
    user = os.getenv("MONGO_USER", "mongo")
    password = os.getenv("MONGO_PASSWORD", "mongo")

    uri = f"mongodb://{user}:{password}@{host}:{port}/"
    return MongoClient(uri)


def fetch_velib_data(rows=10000):
    """Fetch real-time Velib data from OpenDataSoft API."""
    params = {"dataset": DATASET, "rows": rows, "format": "json"}

    response = requests.get(VELIB_API_URL, params=params, timeout=60)
    response.raise_for_status()
    return response.json()


def save_to_mongodb(data: dict, collection_name: str):
    """Save raw data to MongoDB with ingestion timestamp."""
    client = get_mongo_client()
    db = client[os.getenv("MONGO_DB", "velib_datalake")]
    collection = db[collection_name]

    # Add ingestion metadata
    document = {
        "data": data,
        "ingested_at": datetime.utcnow(),
        "source": "velib_opendatasoft_api",
        "records_count": data.get("nhits", 0),
    }

    result = collection.insert_one(document)
    client.close()

    return str(result.inserted_id)


def extract_velib_data():
    """Main extraction function - fetches and stores Velib data."""
    print(f"[{datetime.utcnow()}] Starting Velib data extraction...")

    # Fetch all station data (real-time availability)
    print("Fetching Velib data from OpenDataSoft...")
    velib_data = fetch_velib_data()

    records_count = velib_data.get("nhits", 0)
    print(f"Retrieved {records_count} stations")

    # Save to MongoDB
    doc_id = save_to_mongodb(velib_data, "velib_raw")
    print(f"Data saved to MongoDB with ID: {doc_id}")

    # Return metadata for Airflow XCom
    return {
        "document_id": doc_id,
        "extraction_time": datetime.utcnow().isoformat(),
        "stations_count": records_count,
    }


if __name__ == "__main__":
    result = extract_velib_data()
    print(f"Extraction complete: {result}")
