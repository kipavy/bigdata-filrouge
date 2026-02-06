"""
Pytest configuration and fixtures for Velib ETL tests.
"""

import os
import sys
from datetime import datetime
from unittest.mock import MagicMock

import pytest

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow", "dags"))


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set up test environment variables."""
    monkeypatch.setenv("MONGO_HOST", "localhost")
    monkeypatch.setenv("MONGO_PORT", "27017")
    monkeypatch.setenv("MONGO_USER", "test_user")
    monkeypatch.setenv("MONGO_PASSWORD", "test_password")
    monkeypatch.setenv("MONGO_DB", "test_velib_datalake")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_NAME", "test_airflow")
    monkeypatch.setenv("DB_USER", "test_user")
    monkeypatch.setenv("DB_PASSWORD", "test_password")


@pytest.fixture
def sample_velib_api_response():
    """Sample response from the OpenDataSoft Velib API."""
    return {
        "nhits": 2,
        "records": [
            {
                "fields": {
                    "stationcode": "16107",
                    "name": "Benjamin Godard - Victor Hugo",
                    "coordonnees_geo": [48.865983, 2.275725],
                    "capacity": 35,
                    "nom_arrondissement_communes": "Paris 16ème",
                    "code_insee_commune": "75116",
                    "numbikesavailable": 12,
                    "mechanical": 8,
                    "ebike": 4,
                    "numdocksavailable": 23,
                    "is_installed": "OUI",
                    "is_renting": "OUI",
                    "is_returning": "OUI",
                    "duedate": "2024-01-15T10:30:00+00:00",
                }
            },
            {
                "fields": {
                    "stationcode": "10042",
                    "name": "Charonne - Robert et Sonia Delaunay",
                    "coordonnees_geo": [48.855, 2.397],
                    "capacity": 20,
                    "nom_arrondissement_communes": "Paris 10ème",
                    "code_insee_commune": "75110",
                    "numbikesavailable": 5,
                    "mechanical": 3,
                    "ebike": 2,
                    "numdocksavailable": 15,
                    "is_installed": "OUI",
                    "is_renting": "OUI",
                    "is_returning": "NON",
                    "duedate": "2024-01-15T10:25:00+00:00",
                }
            },
        ],
    }


@pytest.fixture
def sample_mongodb_document(sample_velib_api_response):
    """Sample MongoDB document as stored by getApi.py."""
    return {
        "_id": "507f1f77bcf86cd799439011",
        "data": sample_velib_api_response,
        "ingested_at": datetime(2024, 1, 15, 10, 35, 0),
        "source": "velib_opendatasoft_api",
        "records_count": 2,
    }


@pytest.fixture
def mock_mongo_client():
    """Mock MongoDB client."""
    mock_client = MagicMock()
    mock_db = MagicMock()
    mock_collection = MagicMock()

    mock_client.__getitem__ = MagicMock(return_value=mock_db)
    mock_db.__getitem__ = MagicMock(return_value=mock_collection)

    return mock_client, mock_db, mock_collection


@pytest.fixture
def mock_postgres_connection():
    """Mock PostgreSQL connection."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn, mock_cursor
