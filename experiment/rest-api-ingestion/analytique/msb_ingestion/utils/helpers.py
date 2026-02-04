"""Common utility functions for REST API ingestion pipeline.

Provides helper functions for date handling, data validation,
and other common operations.
"""

from datetime import datetime, timedelta
from typing import Any, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, BooleanType, TimestampType, DateType


def parse_date(date_str: str, date_format: str = "%Y-%m-%d") -> datetime:
    """Parse a date string into a datetime object.

    Args:
        date_str: Date string to parse.
        date_format: Expected format of the date string.

    Returns:
        Parsed datetime object.

    Raises:
        ValueError: If the date string doesn't match the expected format.
    """
    return datetime.strptime(date_str, date_format)


def format_date(dt: datetime, date_format: str = "%Y-%m-%d") -> str:
    """Format a datetime object as a string.

    Args:
        dt: Datetime object to format.
        date_format: Desired output format.

    Returns:
        Formatted date string.
    """
    return dt.strftime(date_format)


def get_date_range(
    start_date: datetime,
    end_date: datetime,
    include_end: bool = True,
) -> list[datetime]:
    """Generate a list of dates between start and end dates.

    Args:
        start_date: Start of the date range.
        end_date: End of the date range.
        include_end: Whether to include the end date.

    Returns:
        List of datetime objects for each day in the range.
    """
    dates = []
    current = start_date
    end = end_date if include_end else end_date - timedelta(days=1)

    while current <= end:
        dates.append(current)
        current += timedelta(days=1)

    return dates


def build_gcs_path(
    bucket_name: str,
    prefix: str,
    table_name: str,
    ingestion_date: datetime,
    partition_format: str = "ingestion_date={date}",
) -> str:
    """Build the GCS path for storing data.

    Args:
        bucket_name: Name of the GCS bucket.
        prefix: Path prefix (e.g., 'bronze').
        table_name: Name of the table.
        ingestion_date: Date of ingestion for partitioning.
        partition_format: Format string for the partition.

    Returns:
        Full GCS path.
    """
    partition = partition_format.format(date=format_date(ingestion_date))
    return f"gs://{bucket_name}/{prefix}/{table_name}/{partition}"


def generate_unique_id(table_name: str, primary_key_value: Any, timestamp: datetime) -> str:
    """Generate a unique identifier for idempotency.

    Args:
        table_name: Name of the table.
        primary_key_value: Value of the primary key.
        timestamp: Timestamp of the record.

    Returns:
        Unique identifier string.
    """
    ts_str = timestamp.strftime("%Y%m%d%H%M%S") if timestamp else "none"
    return f"{table_name}_{primary_key_value}_{ts_str}"


def map_json_type_to_spark(json_type: str) -> Any:
    """Map a JSON schema type to a PySpark type.

    Args:
        json_type: JSON type string (string, integer, number, boolean, etc.)

    Returns:
        Corresponding PySpark type.
    """
    type_mapping = {
        "string": StringType(),
        "integer": IntegerType(),
        "long": LongType(),
        "number": DoubleType(),
        "double": DoubleType(),
        "boolean": BooleanType(),
        "timestamp": TimestampType(),
        "date": DateType(),
    }
    return type_mapping.get(json_type.lower(), StringType())


def build_spark_schema(schema_fields: list[dict[str, Any]]) -> StructType:
    """Build a PySpark StructType from a list of field definitions.

    Args:
        schema_fields: List of dictionaries with 'name', 'type', and optional 'nullable'.

    Returns:
        PySpark StructType schema.
    """
    fields = []
    for field_def in schema_fields:
        field_name = field_def["name"]
        field_type = map_json_type_to_spark(field_def.get("type", "string"))
        nullable = field_def.get("nullable", True)
        fields.append(StructField(field_name, field_type, nullable))

    return StructType(fields)


def add_ingestion_metadata(df: DataFrame, ingestion_date: datetime, mode: str) -> DataFrame:
    """Add standard ingestion metadata columns to a DataFrame.

    Args:
        df: Input DataFrame.
        ingestion_date: Date of ingestion.
        mode: Ingestion mode (initial, incremental, backfill, reference).

    Returns:
        DataFrame with added metadata columns.
    """
    return df.withColumn(
        "_ingestion_timestamp", F.lit(datetime.utcnow())
    ).withColumn(
        "_ingestion_date", F.lit(format_date(ingestion_date))
    ).withColumn(
        "_ingestion_mode", F.lit(mode)
    )


def chunk_list(lst: list, chunk_size: int) -> list[list]:
    """Split a list into chunks of specified size.

    Args:
        lst: List to split.
        chunk_size: Maximum size of each chunk.

    Returns:
        List of lists, each with at most chunk_size elements.
    """
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def safe_get(data: dict, *keys: str, default: Any = None) -> Any:
    """Safely get a nested value from a dictionary.

    Args:
        data: Dictionary to traverse.
        *keys: Keys to follow in sequence.
        default: Default value if any key is missing.

    Returns:
        Value at the nested key path, or default if not found.
    """
    result = data
    for key in keys:
        if isinstance(result, dict):
            result = result.get(key, default)
        else:
            return default
    return result


def validate_required_fields(
    data: dict,
    required_fields: list[str],
) -> tuple[bool, list[str]]:
    """Validate that all required fields are present in data.

    Args:
        data: Dictionary to validate.
        required_fields: List of required field names.

    Returns:
        Tuple of (is_valid, list of missing fields).
    """
    missing = [field for field in required_fields if field not in data or data[field] is None]
    return len(missing) == 0, missing


def sanitize_column_name(name: str) -> str:
    """Sanitize a column name for use in Spark/Parquet.

    Args:
        name: Original column name.

    Returns:
        Sanitized column name (lowercase, spaces replaced with underscores).
    """
    sanitized = name.lower().strip()
    sanitized = sanitized.replace(" ", "_")
    sanitized = sanitized.replace("-", "_")
    sanitized = "".join(c if c.isalnum() or c == "_" else "" for c in sanitized)
    return sanitized
