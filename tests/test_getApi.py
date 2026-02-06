"""
Tests for getApi.py - Data extraction module.
"""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest
import requests
import responses

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from getApi import (
    VELIB_API_URL,
    fetch_velib_data,
    get_mongo_client,
    save_to_mongodb,
)


class TestGetMongoClient:
    """Tests for MongoDB client creation."""

    def test_get_mongo_client_uses_env_vars(self, mock_env_vars):
        """Test that get_mongo_client uses environment variables."""
        with patch("getApi.MongoClient") as mock_client:
            get_mongo_client()
            mock_client.assert_called_once()
            call_args = mock_client.call_args[0][0]
            assert "localhost" in call_args
            assert "27017" in call_args
            assert "test_user" in call_args
            assert "test_password" in call_args

    def test_get_mongo_client_default_values(self):
        """Test that get_mongo_client uses default values when env vars not set."""
        with patch("getApi.MongoClient") as mock_client:
            with patch.dict(os.environ, {}, clear=True):
                get_mongo_client()
                call_args = mock_client.call_args[0][0]
                assert "mongodb" in call_args  # default host


class TestFetchVelibData:
    """Tests for API data fetching."""

    @responses.activate
    def test_fetch_velib_data_success(self, sample_velib_api_response):
        """Test successful API call."""
        responses.add(
            responses.GET,
            VELIB_API_URL,
            json=sample_velib_api_response,
            status=200,
        )

        result = fetch_velib_data(rows=100)

        assert result == sample_velib_api_response
        assert len(responses.calls) == 1

    @responses.activate
    def test_fetch_velib_data_with_custom_rows(self):
        """Test API call with custom row count."""
        responses.add(
            responses.GET,
            VELIB_API_URL,
            json={"nhits": 0, "records": []},
            status=200,
        )

        fetch_velib_data(rows=500)

        assert "rows=500" in responses.calls[0].request.url

    @responses.activate
    def test_fetch_velib_data_api_error(self):
        """Test handling of API errors."""
        responses.add(
            responses.GET,
            VELIB_API_URL,
            json={"error": "Service unavailable"},
            status=503,
        )

        with pytest.raises(requests.exceptions.HTTPError):
            fetch_velib_data()


class TestSaveToMongoDB:
    """Tests for MongoDB data saving."""

    def test_save_to_mongodb_success(self, mock_env_vars, sample_velib_api_response):
        """Test successful save to MongoDB."""
        with patch("getApi.MongoClient") as mock_mongo:
            mock_client = MagicMock()
            mock_db = MagicMock()
            mock_collection = MagicMock()
            mock_result = MagicMock()
            mock_result.inserted_id = "test_id_123"

            mock_mongo.return_value = mock_client
            mock_client.__getitem__.return_value = mock_db
            mock_db.__getitem__.return_value = mock_collection
            mock_collection.insert_one.return_value = mock_result

            result = save_to_mongodb(sample_velib_api_response, "velib_raw")

            assert result == "test_id_123"
            mock_collection.insert_one.assert_called_once()
            mock_client.close.assert_called_once()

    def test_save_to_mongodb_document_structure(self, mock_env_vars, sample_velib_api_response):
        """Test that saved document has correct structure."""
        with patch("getApi.MongoClient") as mock_mongo:
            mock_client = MagicMock()
            mock_db = MagicMock()
            mock_collection = MagicMock()
            mock_result = MagicMock()
            mock_result.inserted_id = "test_id"

            mock_mongo.return_value = mock_client
            mock_client.__getitem__.return_value = mock_db
            mock_db.__getitem__.return_value = mock_collection
            mock_collection.insert_one.return_value = mock_result

            save_to_mongodb(sample_velib_api_response, "velib_raw")

            # Check the document structure
            call_args = mock_collection.insert_one.call_args[0][0]
            assert "data" in call_args
            assert "ingested_at" in call_args
            assert "source" in call_args
            assert "records_count" in call_args
            assert call_args["source"] == "velib_opendatasoft_api"
