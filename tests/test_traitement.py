"""
Tests for traitement.py - Data transformation module.
"""

import os
import sys
from datetime import datetime
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from traitement import (
    get_postgres_connection,
    load_availability_to_postgres,
    load_stations_to_postgres,
    transform_data,
)


class TestTransformData:
    """Tests for data transformation logic."""

    def test_transform_data_extracts_stations(self, sample_mongodb_document):
        """Test that transform_data correctly extracts station information."""
        stations, availability = transform_data(sample_mongodb_document)

        assert len(stations) == 2

        station_1 = next(s for s in stations if s["station_id"] == "16107")
        assert station_1["name"] == "Benjamin Godard - Victor Hugo"
        assert station_1["latitude"] == 48.865983
        assert station_1["longitude"] == 2.275725
        assert station_1["capacity"] == 35
        assert station_1["arrondissement"] == "Paris 16ème"

    def test_transform_data_extracts_availability(self, sample_mongodb_document):
        """Test that transform_data correctly extracts availability information."""
        stations, availability = transform_data(sample_mongodb_document)

        assert len(availability) == 2

        avail_1 = next(a for a in availability if a["station_id"] == "16107")
        assert avail_1["num_bikes_available"] == 12
        assert avail_1["num_bikes_mechanical"] == 8
        assert avail_1["num_bikes_ebike"] == 4
        assert avail_1["num_docks_available"] == 23
        assert avail_1["is_installed"] is True
        assert avail_1["is_renting"] is True
        assert avail_1["is_returning"] is True

    def test_transform_data_handles_missing_station_code(self):
        """Test that records without station code are skipped."""
        raw_data = {
            "data": {
                "records": [
                    {"fields": {"name": "No Station Code"}},
                    {"fields": {"stationcode": "12345", "name": "Valid Station"}},
                ]
            }
        }

        stations, availability = transform_data(raw_data)

        assert len(stations) == 1
        assert stations[0]["station_id"] == "12345"

    def test_transform_data_handles_missing_coordinates(self):
        """Test handling of missing coordinates."""
        raw_data = {
            "data": {
                "records": [
                    {"fields": {"stationcode": "12345", "name": "No Coords"}},
                ]
            }
        }

        stations, availability = transform_data(raw_data)

        assert stations[0]["latitude"] == 0
        assert stations[0]["longitude"] == 0

    def test_transform_data_handles_empty_records(self):
        """Test handling of empty records list."""
        raw_data = {"data": {"records": []}}

        stations, availability = transform_data(raw_data)

        assert len(stations) == 0
        assert len(availability) == 0

    def test_transform_data_parses_boolean_fields(self, sample_mongodb_document):
        """Test that OUI/NON fields are correctly parsed to booleans."""
        stations, availability = transform_data(sample_mongodb_document)

        avail_2 = next(a for a in availability if a["station_id"] == "10042")
        assert avail_2["is_installed"] is True
        assert avail_2["is_renting"] is True
        assert avail_2["is_returning"] is False  # This one was "NON"


class TestLoadStationsToPostgres:
    """Tests for PostgreSQL station loading."""

    def test_load_stations_empty_list(self, mock_env_vars):
        """Test that empty station list returns 0."""
        result = load_stations_to_postgres([])
        assert result == 0

    def test_load_stations_success(self, mock_env_vars):
        """Test successful station loading."""
        stations = [
            {
                "station_id": "12345",
                "name": "Test Station",
                "latitude": 48.85,
                "longitude": 2.35,
                "capacity": 20,
                "arrondissement": "Paris 1er",
                "code_insee": "75101",
            }
        ]

        with (
            patch("traitement.get_postgres_connection") as mock_conn,
            patch("traitement.execute_values") as mock_exec,
            patch("traitement.load_sql", return_value="INSERT INTO stations VALUES %s"),
        ):
            mock_connection = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.return_value = mock_connection
            mock_connection.cursor.return_value = mock_cursor

            result = load_stations_to_postgres(stations)

            assert result == 1
            mock_exec.assert_called_once()
            mock_connection.commit.assert_called_once()
            mock_cursor.close.assert_called_once()
            mock_connection.close.assert_called_once()


class TestLoadAvailabilityToPostgres:
    """Tests for PostgreSQL availability loading."""

    def test_load_availability_empty_list(self, mock_env_vars):
        """Test that empty availability list returns 0."""
        result = load_availability_to_postgres([])
        assert result == 0

    def test_load_availability_success(self, mock_env_vars):
        """Test successful availability loading."""
        availability = [
            {
                "station_id": "12345",
                "num_bikes_available": 10,
                "num_bikes_mechanical": 6,
                "num_bikes_ebike": 4,
                "num_docks_available": 10,
                "is_installed": True,
                "is_renting": True,
                "is_returning": True,
                "last_reported": datetime.now(),
            }
        ]

        with (
            patch("traitement.get_postgres_connection") as mock_conn,
            patch("traitement.execute_values") as mock_exec,
            patch(
                "traitement.load_sql",
                return_value="INSERT INTO station_availability VALUES %s",
            ),
        ):
            mock_connection = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.return_value = mock_connection
            mock_connection.cursor.return_value = mock_cursor

            result = load_availability_to_postgres(availability)

            assert result == 1
            mock_exec.assert_called_once()
            mock_connection.commit.assert_called_once()


class TestGetPostgresConnection:
    """Tests for PostgreSQL connection creation."""

    def test_get_postgres_connection_uses_env_vars(self, mock_env_vars):
        """Test that get_postgres_connection uses environment variables."""
        with patch("traitement.psycopg2.connect") as mock_connect:
            get_postgres_connection()

            mock_connect.assert_called_once_with(
                host="localhost",
                database="test_airflow",
                user="test_user",
                password="test_password",
                port=5432,
            )
