"""Unit tests for helper utilities."""

import pytest
from datetime import datetime

from analytique.msb_ingestion.utils.helpers import (
    parse_date,
    format_date,
    get_date_range,
    build_gcs_path,
    generate_unique_id,
    chunk_list,
    safe_get,
    validate_required_fields,
    sanitize_column_name,
)


class TestParseDate:
    """Tests for parse_date function."""

    def test_parse_valid_date(self):
        result = parse_date("2024-01-15")
        assert result == datetime(2024, 1, 15)

    def test_parse_date_custom_format(self):
        result = parse_date("15/01/2024", "%d/%m/%Y")
        assert result == datetime(2024, 1, 15)

    def test_parse_invalid_date_raises(self):
        with pytest.raises(ValueError):
            parse_date("invalid-date")


class TestFormatDate:
    """Tests for format_date function."""

    def test_format_date_default(self):
        dt = datetime(2024, 1, 15)
        result = format_date(dt)
        assert result == "2024-01-15"

    def test_format_date_custom_format(self):
        dt = datetime(2024, 1, 15)
        result = format_date(dt, "%d/%m/%Y")
        assert result == "15/01/2024"


class TestGetDateRange:
    """Tests for get_date_range function."""

    def test_date_range_inclusive(self):
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 3)
        result = get_date_range(start, end)
        assert len(result) == 3
        assert result[0] == datetime(2024, 1, 1)
        assert result[2] == datetime(2024, 1, 3)

    def test_date_range_exclusive(self):
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 3)
        result = get_date_range(start, end, include_end=False)
        assert len(result) == 2
        assert result[-1] == datetime(2024, 1, 2)

    def test_single_day_range(self):
        start = datetime(2024, 1, 1)
        result = get_date_range(start, start)
        assert len(result) == 1


class TestBuildGcsPath:
    """Tests for build_gcs_path function."""

    def test_build_path_default_format(self):
        result = build_gcs_path(
            bucket_name="my-bucket",
            prefix="bronze",
            table_name="customers",
            ingestion_date=datetime(2024, 1, 15),
        )
        assert result == "gs://my-bucket/bronze/customers/ingestion_date=2024-01-15"

    def test_build_path_custom_partition_format(self):
        result = build_gcs_path(
            bucket_name="my-bucket",
            prefix="bronze",
            table_name="orders",
            ingestion_date=datetime(2024, 1, 15),
            partition_format="dt={date}",
        )
        assert result == "gs://my-bucket/bronze/orders/dt=2024-01-15"


class TestGenerateUniqueId:
    """Tests for generate_unique_id function."""

    def test_generate_id_with_timestamp(self):
        result = generate_unique_id(
            table_name="customers",
            primary_key_value=123,
            timestamp=datetime(2024, 1, 15, 10, 30, 0),
        )
        assert result == "customers_123_20240115103000"

    def test_generate_id_none_timestamp(self):
        result = generate_unique_id(
            table_name="customers",
            primary_key_value=456,
            timestamp=None,
        )
        assert result == "customers_456_none"


class TestChunkList:
    """Tests for chunk_list function."""

    def test_chunk_even_split(self):
        lst = [1, 2, 3, 4, 5, 6]
        result = chunk_list(lst, 2)
        assert result == [[1, 2], [3, 4], [5, 6]]

    def test_chunk_uneven_split(self):
        lst = [1, 2, 3, 4, 5]
        result = chunk_list(lst, 2)
        assert result == [[1, 2], [3, 4], [5]]

    def test_chunk_larger_than_list(self):
        lst = [1, 2]
        result = chunk_list(lst, 5)
        assert result == [[1, 2]]

    def test_chunk_empty_list(self):
        result = chunk_list([], 3)
        assert result == []


class TestSafeGet:
    """Tests for safe_get function."""

    def test_safe_get_single_key(self):
        data = {"name": "John"}
        result = safe_get(data, "name")
        assert result == "John"

    def test_safe_get_nested_keys(self):
        data = {"user": {"profile": {"name": "John"}}}
        result = safe_get(data, "user", "profile", "name")
        assert result == "John"

    def test_safe_get_missing_key(self):
        data = {"name": "John"}
        result = safe_get(data, "age")
        assert result is None

    def test_safe_get_missing_key_with_default(self):
        data = {"name": "John"}
        result = safe_get(data, "age", default=30)
        assert result == 30

    def test_safe_get_missing_nested_key(self):
        data = {"user": {"name": "John"}}
        result = safe_get(data, "user", "profile", "name", default="Unknown")
        assert result == "Unknown"


class TestValidateRequiredFields:
    """Tests for validate_required_fields function."""

    def test_all_fields_present(self):
        data = {"name": "John", "age": 30}
        is_valid, missing = validate_required_fields(data, ["name", "age"])
        assert is_valid is True
        assert missing == []

    def test_missing_fields(self):
        data = {"name": "John"}
        is_valid, missing = validate_required_fields(data, ["name", "age", "email"])
        assert is_valid is False
        assert missing == ["age", "email"]

    def test_null_field_value(self):
        data = {"name": "John", "age": None}
        is_valid, missing = validate_required_fields(data, ["name", "age"])
        assert is_valid is False
        assert missing == ["age"]


class TestSanitizeColumnName:
    """Tests for sanitize_column_name function."""

    def test_lowercase_conversion(self):
        result = sanitize_column_name("CustomerName")
        assert result == "customername"

    def test_space_replacement(self):
        result = sanitize_column_name("customer name")
        assert result == "customer_name"

    def test_hyphen_replacement(self):
        result = sanitize_column_name("customer-name")
        assert result == "customer_name"

    def test_special_char_removal(self):
        result = sanitize_column_name("customer@name!")
        assert result == "customername"

    def test_trim_whitespace(self):
        result = sanitize_column_name("  customer_name  ")
        assert result == "customer_name"
