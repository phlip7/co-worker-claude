"""Unit tests for API client."""

import pytest
from unittest.mock import Mock, patch, MagicMock

from analytique.msb_ingestion.api.client import APIClient, APIClientError
from analytique.msb_ingestion.config.settings import APIConfig, TableConfig


class TestAPIClient:
    """Tests for APIClient class."""

    @pytest.fixture
    def api_config(self):
        return APIConfig(
            base_url="https://api.example.com",
            timeout_seconds=30,
            max_retries=3,
            page_size=100,
        )

    @pytest.fixture
    def table_config(self):
        return TableConfig(
            name="customers",
            endpoint="/api/customers",
            primary_key="id",
            pagination_type="offset",
            pagination_param="offset",
            page_size_param="limit",
        )

    @pytest.fixture
    def client(self, api_config):
        return APIClient(api_config)

    def test_build_url(self, client):
        url = client._build_url("/api/customers")
        assert url == "https://api.example.com/api/customers"

    def test_build_url_no_leading_slash(self, client):
        url = client._build_url("api/customers")
        assert url == "https://api.example.com/api/customers"

    def test_build_url_trailing_slash_in_base(self):
        config = APIConfig(base_url="https://api.example.com/")
        client = APIClient(config)
        url = client._build_url("/api/customers")
        assert url == "https://api.example.com/api/customers"

    def test_extract_records_list_response(self, client):
        response = [{"id": 1}, {"id": 2}]
        records = client._extract_records(response)
        assert records == [{"id": 1}, {"id": 2}]

    def test_extract_records_data_key(self, client):
        response = {"data": [{"id": 1}, {"id": 2}], "meta": {}}
        records = client._extract_records(response)
        assert records == [{"id": 1}, {"id": 2}]

    def test_extract_records_results_key(self, client):
        response = {"results": [{"id": 1}], "count": 1}
        records = client._extract_records(response)
        assert records == [{"id": 1}]

    def test_extract_records_items_key(self, client):
        response = {"items": [{"id": 1}]}
        records = client._extract_records(response)
        assert records == [{"id": 1}]

    def test_extract_records_empty(self, client):
        response = {"metadata": {}}
        records = client._extract_records(response)
        assert records == []

    @patch.object(APIClient, 'fetch')
    def test_fetch_with_offset_pagination(self, mock_fetch, client, table_config):
        mock_fetch.side_effect = [
            {"data": [{"id": i} for i in range(100)]},
            {"data": [{"id": i} for i in range(100, 150)]},
        ]

        batches = list(client.fetch_with_pagination(table_config))

        assert len(batches) == 2
        assert len(batches[0]) == 100
        assert len(batches[1]) == 50

    @patch.object(APIClient, 'fetch')
    def test_fetch_with_offset_pagination_empty_response(self, mock_fetch, client, table_config):
        mock_fetch.return_value = {"data": []}

        batches = list(client.fetch_with_pagination(table_config))

        assert len(batches) == 0

    @patch.object(APIClient, 'fetch')
    def test_fetch_with_cursor_pagination(self, mock_fetch, client, table_config):
        table_config.pagination_type = "cursor"

        mock_fetch.side_effect = [
            {"data": [{"id": 1}], "next_cursor": "abc123"},
            {"data": [{"id": 2}], "next_cursor": None},
        ]

        batches = list(client.fetch_with_pagination(table_config))

        assert len(batches) == 2

    @patch.object(APIClient, 'fetch')
    def test_fetch_with_page_pagination(self, mock_fetch, client, table_config):
        table_config.pagination_type = "page"

        mock_fetch.side_effect = [
            {"data": [{"id": 1}], "total_pages": 2},
            {"data": [{"id": 2}], "total_pages": 2},
        ]

        batches = list(client.fetch_with_pagination(table_config))

        assert len(batches) == 2

    def test_fetch_with_unsupported_pagination(self, client, table_config):
        table_config.pagination_type = "unknown"

        with pytest.raises(APIClientError, match="Unsupported pagination type"):
            list(client.fetch_with_pagination(table_config))

    def test_context_manager(self, api_config):
        with APIClient(api_config) as client:
            assert client is not None
        assert client._session is None

    def test_build_date_params(self, client):
        from datetime import datetime

        params = client._build_date_params(
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 31),
            watermark_value=None,
            change_tracking_column=None,
        )

        assert params["start_date"] == "2024-01-01"
        assert params["end_date"] == "2024-01-31"

    def test_build_date_params_with_watermark(self, client):
        from datetime import datetime

        watermark_dt = datetime(2024, 1, 15, 10, 30, 0)
        params = client._build_date_params(
            start_date=None,
            end_date=None,
            watermark_value=watermark_dt,
            change_tracking_column="updated_at",
        )

        assert "updated_at_after" in params
