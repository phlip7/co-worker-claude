"""Unit tests for configuration settings."""

import os
import pytest
import tempfile
import yaml

from analytique.msb_ingestion.config.settings import (
    APIConfig,
    GCSConfig,
    TableConfig,
    Settings,
)


class TestAPIConfig:
    """Tests for APIConfig dataclass."""

    def test_default_values(self):
        config = APIConfig(base_url="https://api.example.com")
        assert config.auth_type == "bearer"
        assert config.timeout_seconds == 30
        assert config.max_retries == 3
        assert config.page_size == 1000

    def test_custom_values(self):
        config = APIConfig(
            base_url="https://api.example.com",
            auth_type="api_key",
            timeout_seconds=60,
            max_retries=5,
        )
        assert config.auth_type == "api_key"
        assert config.timeout_seconds == 60
        assert config.max_retries == 5

    def test_get_auth_token_from_env(self):
        os.environ["TEST_API_TOKEN"] = "test-token-123"
        config = APIConfig(
            base_url="https://api.example.com",
            auth_token_env_var="TEST_API_TOKEN",
        )
        assert config.get_auth_token() == "test-token-123"
        del os.environ["TEST_API_TOKEN"]

    def test_get_auth_token_missing_env(self):
        config = APIConfig(
            base_url="https://api.example.com",
            auth_token_env_var="NONEXISTENT_TOKEN",
        )
        assert config.get_auth_token() is None


class TestGCSConfig:
    """Tests for GCSConfig dataclass."""

    def test_default_values(self):
        config = GCSConfig(
            project_id="my-project",
            bucket_name="my-bucket",
        )
        assert config.bronze_layer_prefix == "bronze"
        assert config.file_format == "parquet"

    def test_custom_values(self):
        config = GCSConfig(
            project_id="my-project",
            bucket_name="my-bucket",
            bronze_layer_prefix="raw",
            file_format="json",
        )
        assert config.bronze_layer_prefix == "raw"
        assert config.file_format == "json"


class TestTableConfig:
    """Tests for TableConfig dataclass."""

    def test_default_values(self):
        config = TableConfig(
            name="customers",
            endpoint="/api/customers",
            primary_key="id",
        )
        assert config.table_type == "transactional"
        assert config.pagination_type == "offset"
        assert config.change_tracking_column is None

    def test_reference_table(self):
        config = TableConfig(
            name="countries",
            endpoint="/api/countries",
            primary_key="code",
            table_type="reference",
        )
        assert config.table_type == "reference"


class TestSettings:
    """Tests for Settings class."""

    @pytest.fixture
    def sample_config_dict(self):
        return {
            "api": {
                "base_url": "https://api.example.com",
                "timeout_seconds": 30,
            },
            "gcs": {
                "project_id": "my-project",
                "bucket_name": "my-bucket",
            },
            "tables": [
                {
                    "name": "customers",
                    "endpoint": "/api/customers",
                    "primary_key": "id",
                    "table_type": "transactional",
                    "change_tracking_column": "updated_at",
                },
                {
                    "name": "countries",
                    "endpoint": "/api/countries",
                    "primary_key": "code",
                    "table_type": "reference",
                },
            ],
            "log_level": "DEBUG",
        }

    @pytest.fixture
    def sample_config_file(self, sample_config_dict):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            yaml.dump(sample_config_dict, f)
            return f.name

    def test_from_yaml(self, sample_config_file):
        settings = Settings.from_yaml(sample_config_file)
        assert settings.api.base_url == "https://api.example.com"
        assert settings.gcs.bucket_name == "my-bucket"
        assert len(settings.tables) == 2
        assert settings.log_level == "DEBUG"
        os.unlink(sample_config_file)

    def test_from_yaml_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            Settings.from_yaml("/nonexistent/path/config.yaml")

    def test_parse_config_missing_api(self):
        config = {"gcs": {}, "tables": []}
        with pytest.raises(ValueError, match="Missing required 'api'"):
            Settings._parse_config(config)

    def test_parse_config_missing_gcs(self):
        config = {"api": {"base_url": "http://test"}, "tables": []}
        with pytest.raises(ValueError, match="Missing required 'gcs'"):
            Settings._parse_config(config)

    def test_get_table(self, sample_config_dict):
        settings = Settings._parse_config(sample_config_dict)
        table = settings.get_table("customers")
        assert table.name == "customers"
        assert table.primary_key == "id"

    def test_get_table_not_found(self, sample_config_dict):
        settings = Settings._parse_config(sample_config_dict)
        with pytest.raises(KeyError, match="not found"):
            settings.get_table("nonexistent")

    def test_get_all_tables(self, sample_config_dict):
        settings = Settings._parse_config(sample_config_dict)
        tables = settings.get_all_tables()
        assert len(tables) == 2

    def test_get_tables_by_type(self, sample_config_dict):
        settings = Settings._parse_config(sample_config_dict)

        transactional = settings.get_tables_by_type("transactional")
        assert len(transactional) == 1
        assert transactional[0].name == "customers"

        reference = settings.get_tables_by_type("reference")
        assert len(reference) == 1
        assert reference[0].name == "countries"
