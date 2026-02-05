"""MSB-specific configuration management for REST API ingestion.

This module provides configuration loading and validation for the MSB ingestion pipeline.
Supports YAML configuration files and environment variable overrides.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml

from analytique.common.api.client import APIClientConfig
from analytique.common.storage.gcs_writer import GCSWriterConfig


@dataclass
class APIConfig:
    """MSB API connection configuration."""

    base_url: str
    auth_type: str = "bearer"
    auth_token_env_var: str = "API_AUTH_TOKEN"
    timeout_seconds: int = 30
    max_retries: int = 3
    retry_backoff_factor: float = 2.0
    page_size: int = 1000

    def get_auth_token(self) -> Optional[str]:
        """Retrieve authentication token from environment variable."""
        return os.environ.get(self.auth_token_env_var)

    def to_client_config(self) -> APIClientConfig:
        """Convert to generic APIClientConfig."""
        return APIClientConfig(
            base_url=self.base_url,
            auth_type=self.auth_type,
            auth_token=self.get_auth_token(),
            timeout_seconds=self.timeout_seconds,
            max_retries=self.max_retries,
            retry_backoff_factor=self.retry_backoff_factor,
            page_size=self.page_size,
        )


@dataclass
class GCSConfig:
    """MSB GCS storage configuration."""

    project_id: str
    bucket_name: str
    bronze_layer_prefix: str = "bronze"
    partition_format: str = "ingestion_date={date}"
    file_format: str = "parquet"

    def to_writer_config(self) -> GCSWriterConfig:
        """Convert to generic GCSWriterConfig."""
        return GCSWriterConfig(
            bucket_name=self.bucket_name,
            prefix=self.bronze_layer_prefix,
            partition_format=self.partition_format,
            file_format=self.file_format,
        )


@dataclass
class TableConfig:
    """MSB-specific configuration for a single table.

    Implements the PaginationConfig protocol for use with generic API client.
    """

    name: str
    endpoint: str
    primary_key: str
    table_type: str = "transactional"
    change_tracking_column: Optional[str] = None
    schema_fields: list[dict[str, Any]] = field(default_factory=list)
    pagination_type: str = "offset"
    pagination_param: str = "offset"
    page_size_param: str = "limit"


@dataclass
class Settings:
    """Main settings container for the ingestion pipeline."""

    api: APIConfig
    gcs: GCSConfig
    tables: dict[str, TableConfig]
    checkpoint_table: str = "ingestion_checkpoints"
    log_level: str = "INFO"

    @classmethod
    def from_yaml(cls, config_path: str | Path) -> "Settings":
        """Load settings from a YAML configuration file.

        Args:
            config_path: Path to the YAML configuration file.

        Returns:
            Settings instance populated from the configuration file.

        Raises:
            FileNotFoundError: If the configuration file doesn't exist.
            ValueError: If the configuration is invalid.
        """
        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_path, "r") as f:
            config_data = yaml.safe_load(f)

        return cls._parse_config(config_data)

    @classmethod
    def _parse_config(cls, config_data: dict[str, Any]) -> "Settings":
        """Parse configuration dictionary into Settings object.

        Args:
            config_data: Dictionary containing configuration data.

        Returns:
            Settings instance.

        Raises:
            ValueError: If required configuration fields are missing.
        """
        if "api" not in config_data:
            raise ValueError("Missing required 'api' configuration section")
        if "gcs" not in config_data:
            raise ValueError("Missing required 'gcs' configuration section")
        if "tables" not in config_data:
            raise ValueError("Missing required 'tables' configuration section")

        api_config = APIConfig(**config_data["api"])
        gcs_config = GCSConfig(**config_data["gcs"])

        tables = {}
        for table_data in config_data["tables"]:
            table = TableConfig(**table_data)
            tables[table.name] = table

        return cls(
            api=api_config,
            gcs=gcs_config,
            tables=tables,
            checkpoint_table=config_data.get("checkpoint_table", "ingestion_checkpoints"),
            log_level=config_data.get("log_level", "INFO"),
        )

    def get_table(self, table_name: str) -> TableConfig:
        """Get configuration for a specific table.

        Args:
            table_name: Name of the table.

        Returns:
            TableConfig for the specified table.

        Raises:
            KeyError: If the table is not found in configuration.
        """
        if table_name not in self.tables:
            raise KeyError(f"Table '{table_name}' not found in configuration")
        return self.tables[table_name]

    def get_all_tables(self) -> list[TableConfig]:
        """Get all configured tables.

        Returns:
            List of all TableConfig objects.
        """
        return list(self.tables.values())

    def get_tables_by_type(self, table_type: str) -> list[TableConfig]:
        """Get tables filtered by type.

        Args:
            table_type: Type of tables to filter ('transactional' or 'reference').

        Returns:
            List of TableConfig objects matching the specified type.
        """
        return [t for t in self.tables.values() if t.table_type == table_type]
